from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import zipfile
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TypedDict


LOG = logging.getLogger(__name__)

PITCH_TYPES = {
    "1": "PickoffAttemptFirst",
    "2": "PickoffAttemptSecond",
    "3": "PickoffAttemptThird",
    "B": "Ball",
    "C": "CalledStrike",
    "F": "Foul",
    "H": "HitBatter",
    "I": "IntentionalBall",
    "K": "StrikeUnknownType",
    "L": "FoulBunt",
    "M": "MissedBunt",
    "N": "NoPitch",
    "O": "FoulTipBunt",
    "P": "Pitchout",
    "Q": "SwingingOnPitchout",
    "R": "FoulOnPitchout",
    "S": "SwingingStrike",
    "T": "FoulTip",
    "U": "Unknown",
    "?": "Unknown",
    "V": "AutomaticBall",
    "A": "AutomaticStrike",
    "X": "InPlay",
    "Y": "InPlayOnPitchout",
}
BASES = {"1": "First", "2": "Second", "3": "Third", "H": "Home"}


@dataclass(frozen=True)
class Pitch:
    sequence_item: str
    runners_going_flag: bool
    blocked_by_catcher_flag: bool
    catcher_pickoff_attempt_at_base: str


@dataclass(frozen=True)
class ExportedPitch:
    sequence_id: int
    pitch: Pitch


@dataclass(frozen=True)
class SourcePlay:
    game_id: str
    filename: str
    line_number: int
    event_id: int
    inning: str
    batting_side: str
    batter_id: str
    count: str
    raw_pitch_sequence: str
    raw_play: str


@dataclass
class AppearanceHistory:
    raw: str = ""
    pitches: tuple[Pitch, ...] = ()


class ValidationReport(TypedDict):
    counters: dict[str, int]
    cases: dict[str, list[dict[str, object]]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archives", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--max-games", type=int, default=0)
    return parser.parse_args()


def pitch_tokens(raw: str) -> tuple[Pitch, ...]:
    tokens: list[Pitch] = []
    index = 0
    pending_pickoff = ""
    if len(raw) >= 2 and raw[0] == "+" and raw[1] in BASES:
        pending_pickoff = BASES[raw[1]]
        index = 2
    sequence_item = "Unrecognized"
    runners_going = False
    blocked_by_catcher = False
    while index < len(raw):
        char = raw[index]
        if char == ".":
            if (
                index + 2 < len(raw)
                and raw[index + 1] == "+"
                and raw[index + 2] in BASES
            ):
                pending_pickoff = BASES[raw[index + 2]]
                index += 3
            else:
                index += 1
            continue
        if char == "*":
            blocked_by_catcher = True
            index += 1
            continue
        if char == ">":
            if (
                index + 2 < len(raw)
                and raw[index + 1] == "+"
                and raw[index + 2] in BASES
            ):
                index += 3
                continue
            runners_going = True
            index += 1
            continue
        if char == "+" and index + 1 < len(raw) and raw[index + 1] in BASES:
            index += 2
            continue
        sequence_item = PITCH_TYPES.get(char, "Unrecognized")
        pickoff = pending_pickoff
        pending_pickoff = ""
        if (
            index + 3 < len(raw)
            and raw[index + 1] == ">"
            and raw[index + 2] == "+"
            and raw[index + 3] in BASES
        ):
            pickoff = BASES[raw[index + 3]]
        elif index + 2 < len(raw) and raw[index + 1] == "+" and raw[index + 2] in BASES:
            pickoff = BASES[raw[index + 2]]
        tokens.append(Pitch(sequence_item, runners_going, blocked_by_catcher, pickoff))
        sequence_item = "Unrecognized"
        runners_going = False
        blocked_by_catcher = False
        index += 1
    return tuple(tokens)


def archive_plays(archives: Path) -> dict[str, list[SourcePlay]]:
    plays: dict[str, list[SourcePlay]] = {}
    for archive in sorted(archives.glob("*eve.zip")):
        with zipfile.ZipFile(archive) as bundle:
            for filename in sorted(bundle.namelist()):
                if not filename.endswith((".EVA", ".EVN", ".EVE", ".EVR")):
                    continue
                game_id = ""
                event_id = 0
                content = bundle.read(filename).decode("latin1")
                for line_number, line in enumerate(content.splitlines(), start=1):
                    fields = line.split(",")
                    if fields[0] == "id":
                        game_id = fields[1]
                        event_id = 0
                    elif fields[0] == "play" and len(fields) >= 7 and game_id:
                        event_id += 1
                        plays.setdefault(game_id, []).append(
                            SourcePlay(
                                game_id,
                                filename,
                                line_number,
                                event_id,
                                fields[1],
                                fields[2],
                                fields[3],
                                fields[4],
                                fields[5],
                                fields[6],
                            )
                        )
    return plays


def output_rows(
    output: Path, selected_game_ids: set[str]
) -> tuple[
    dict[tuple[str, int], dict[str, str]],
    dict[tuple[str, int], dict[str, str]],
    dict[int, list[ExportedPitch]],
    int,
]:
    events: dict[tuple[str, int], dict[str, str]] = {}
    audits: dict[tuple[str, int], dict[str, str]] = {}
    skipped_events = 0
    with (output / "events.csv").open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row["game_id"] in selected_game_ids:
                events[(row["game_id"], int(row["event_id"]))] = row
            else:
                skipped_events += 1
    with (output / "event_audit.csv").open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row["game_id"] in selected_game_ids:
                audits[(row["game_id"], int(row["event_id"]))] = row
    selected_event_keys = {int(row["event_key"]) for row in events.values()}
    pitches: dict[int, list[ExportedPitch]] = defaultdict(list)
    with (output / "event_pitch_sequences.csv").open(newline="") as handle:
        for row in csv.DictReader(handle):
            event_key = int(row["event_key"])
            if event_key in selected_event_keys:
                pitches[event_key].append(
                    ExportedPitch(
                        int(row["sequence_id"]),
                        Pitch(
                            row["sequence_item"],
                            row["runners_going_flag"] == "true",
                            row["blocked_by_catcher_flag"] == "true",
                            row["catcher_pickoff_attempt_at_base"],
                        ),
                    )
                )
    return events, audits, pitches, skipped_events


def validate_resolution_rows(
    output: Path,
    source: dict[str, list[SourcePlay]],
    game_ids: set[str],
    events: dict[tuple[str, int], dict[str, str]],
    counters: Counter[str],
) -> None:
    source_rows = {
        (game_id, play.event_id): play.raw_pitch_sequence
        for game_id in game_ids
        for play in source[game_id]
    }
    expected_starts: dict[tuple[str, int], int] = {}
    for game_id in game_ids:
        start = 1
        for play in source[game_id]:
            key = (game_id, play.event_id)
            expected_starts[key] = start
            if key in events and reset_reason(events[key]):
                start = play.event_id + 1
    seen: set[tuple[str, int]] = set()
    with (output / "event_pitch_sequence_status.csv").open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row["game_id"] not in game_ids:
                continue
            key = (row["game_id"], int(row["event_id"]))
            if key in seen or source_rows.get(key) != row["raw_pitch_sequence"]:
                counters["resolution_provenance_mismatches"] += 1
            if int(row["appearance_start_event_id"]) != expected_starts.get(key):
                counters["appearance_boundary_mismatches"] += 1
            if key not in events or row["event_key"] != events[key]["event_key"]:
                counters["resolution_event_key_mismatches"] += 1
            seen.add(key)
            if row["status"] != "Resolved":
                counters["modern_unresolved_or_unavailable_events"] += 1
            counters["resolution_rows_checked"] += 1
    counters["missing_resolution_rows"] = len(source_rows.keys() - seen)
    with (output / "event_pitch_sequence_issues.csv").open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row["game_id"] in game_ids:
                counters["modern_conflict_issues"] += 1


def remove_delimiters(raw: str) -> str:
    return raw.replace(".", "")


def extract(
    history: AppearanceHistory, source: SourcePlay
) -> tuple[tuple[Pitch, ...] | None, str | None]:
    current = pitch_tokens(source.raw_pitch_sequence)
    if not source.raw_pitch_sequence:
        return current, None
    current_raw = remove_delimiters(source.raw_pitch_sequence)
    history_raw = remove_delimiters(history.raw)
    if current_raw.startswith(history_raw):
        prefix = history.pitches
    elif current_raw.startswith(history_raw.rstrip("N")):
        raw_prefix = history_raw.rstrip("N")
        remainder = current_raw[len(raw_prefix) :]
        retained = len(remainder) - len(remainder.lstrip("N"))
        removed = len(history_raw) - len(raw_prefix) - retained
        prefix = history.pitches[: len(history.pitches) - removed]
    else:
        return None, "source_cumulative_conflict"
    if current[: len(prefix)] != prefix:
        return None, "parsed_prefix_conflict"
    return current[len(prefix) :], None


def reset_reason(event: dict[str, str]) -> str:
    if event["plate_appearance_result"]:
        return "plate_appearance"
    if int(event["outs"]) + int(event["outs_on_play"]) >= 3:
        return "third_out"
    return ""


def count_before_final_pitch(tokens: tuple[Pitch, ...]) -> tuple[int, int]:
    return count_through_pitches(tokens[:-1])


def count_through_pitches(tokens: tuple[Pitch, ...]) -> tuple[int, int]:
    balls = 0
    strikes = 0
    for pitch in tokens:
        if pitch.sequence_item in {
            "Ball",
            "IntentionalBall",
            "AutomaticBall",
            "Pitchout",
        }:
            balls += 1
        elif pitch.sequence_item in {
            "CalledStrike",
            "SwingingStrike",
            "StrikeUnknownType",
            "FoulBunt",
            "MissedBunt",
            "FoulTipBunt",
            "FoulTip",
            "SwingingOnPitchout",
            "AutomaticStrike",
        }:
            strikes += 1
        elif pitch.sequence_item in {"Foul", "FoulOnPitchout"} and strikes < 2:
            strikes += 1
    return balls, strikes


def count_includes_final_pitch(source: SourcePlay, is_plate_appearance: bool) -> bool:
    return source.raw_play == "NP" or (
        not is_plate_appearance
        and (
            source.raw_pitch_sequence.endswith(("+1", "+2", "+3", "+H"))
            or (
                source.raw_play.startswith("BK")
                and source.raw_pitch_sequence.endswith(".")
            )
        )
    )


def terminal_issue(
    event: dict[str, str], source: SourcePlay, tokens: tuple[Pitch, ...]
) -> str:
    outcome = event["plate_appearance_result"]
    if not outcome:
        return ""
    if not tokens:
        return "terminal_without_pitch_sequence"
    final = tokens[-1].sequence_item
    if outcome == "StrikeOut":
        return (
            ""
            if final
            in {
                "CalledStrike",
                "SwingingStrike",
                "StrikeUnknownType",
                "FoulBunt",
                "MissedBunt",
                "FoulTipBunt",
                "FoulTip",
                "SwingingOnPitchout",
                "AutomaticStrike",
            }
            else "terminal_pitch_mismatch"
        )
    if outcome in {"Walk", "IntentionalWalk"}:
        return (
            ""
            if final in {"Ball", "IntentionalBall", "AutomaticBall", "Pitchout"}
            else "terminal_pitch_mismatch"
        )
    if outcome == "HitByPitch":
        return "" if final == "HitBatter" else "terminal_pitch_mismatch"
    if outcome == "Interference":
        return ""
    if final in {"InPlay", "InPlayOnPitchout"}:
        return ""
    if "BINT" in source.raw_play:
        return "terminal_special_outcome"
    return "terminal_pitch_mismatch"


def append_case(
    cases: dict[str, list[dict[str, object]]], name: str, value: dict[str, object]
) -> None:
    if len(cases[name]) < 20:
        cases[name].append(value)


def validate(archives: Path, output: Path, max_games: int) -> ValidationReport:
    source = archive_plays(archives)
    game_ids = sorted(source)
    if max_games:
        game_ids = game_ids[:max_games]
    selected_game_ids = set(game_ids)
    events, audits, exported_pitches, skipped_events = output_rows(
        output, selected_game_ids
    )
    counters: Counter[str] = Counter()
    cases: dict[str, list[dict[str, object]]] = defaultdict(list)
    validate_resolution_rows(output, source, selected_game_ids, events, counters)
    for game_id in game_ids:
        source_plays = source[game_id]
        counters["games_checked"] += 1
        history = AppearanceHistory()
        for play in source_plays:
            counters["source_events"] += 1
            key = (game_id, play.event_id)
            event = events.get(key)
            audit = audits.get(key)
            if event is None:
                counters["missing_events"] += 1
                append_case(cases, "missing_events", asdict(play))
                history = AppearanceHistory()
                continue
            if audit is None:
                counters["missing_audits"] += 1
                append_case(cases, "missing_audits", asdict(play))
                history = AppearanceHistory()
                continue
            if (
                audit["filename"] != play.filename
                or int(audit["line_number"]) != play.line_number
                or audit["raw_play"] != play.raw_play
            ):
                counters["source_identity_mismatches"] += 1
                append_case(
                    cases,
                    "source_identity_mismatches",
                    {"source": asdict(play), "audit": audit},
                )
                history = AppearanceHistory()
                continue
            expected_side = "Away" if play.batting_side == "0" else "Home"
            if (
                event["inning"] != play.inning
                or event["batting_side"] != expected_side
                or event["batter_id"] != play.batter_id
            ):
                counters["source_context_mismatches"] += 1
                append_case(
                    cases,
                    "source_context_mismatches",
                    {"source": asdict(play), "event": event},
                )
                history = AppearanceHistory()
                continue
            expected, anomaly = extract(history, play)
            tokens = pitch_tokens(play.raw_pitch_sequence)
            issue = terminal_issue(event, play, tokens)
            if issue:
                counters[issue] += 1
                append_case(cases, issue, {"source": asdict(play), "event": event})
            if len(play.count) == 2 and play.count.isdigit():
                counters["count_checked"] += 1
                expected_count = (
                    count_through_pitches(tokens)
                    if count_includes_final_pitch(
                        play, bool(event["plate_appearance_result"])
                    )
                    else count_before_final_pitch(tokens)
                )
                actual_count = (int(play.count[0]), int(play.count[1]))
                if expected_count[0] > 3 or expected_count[1] > 2:
                    counters["source_count_overflows"] += 1
                    append_case(
                        cases,
                        "source_count_overflows",
                        {
                            "source": asdict(play),
                            "expected_count": expected_count,
                        },
                    )
                if actual_count != expected_count:
                    counters["source_count_anomalies"] += 1
                    append_case(
                        cases,
                        "source_count_anomalies",
                        {
                            "source": asdict(play),
                            "expected_count": expected_count,
                            "actual_count": actual_count,
                        },
                    )
            else:
                counters["count_uncheckable"] += 1
            if anomaly is not None:
                counters[anomaly] += 1
                append_case(
                    cases, anomaly, {"source": asdict(play), "history_raw": history.raw}
                )
            else:
                actual = tuple(exported_pitches.get(int(event["event_key"]), []))
                expected_export = tuple(
                    ExportedPitch(sequence_id, pitch)
                    for sequence_id, pitch in enumerate(expected or (), start=1)
                )
                if actual != expected_export:
                    counters["extraction_mismatches"] += 1
                    append_case(
                        cases,
                        "extraction_mismatches",
                        {
                            "source": asdict(play),
                            "expected": [asdict(pitch) for pitch in expected_export],
                            "actual": [asdict(pitch) for pitch in actual],
                        },
                    )
                else:
                    counters["events_validated"] += 1
            if play.raw_pitch_sequence:
                history = AppearanceHistory(
                    play.raw_pitch_sequence, pitch_tokens(play.raw_pitch_sequence)
                )
            reason = reset_reason(event)
            if reason:
                history = AppearanceHistory()
                counters[f"appearance_resets_{reason}"] += 1
    counters["output_events_not_selected"] = skipped_events
    return {"counters": dict(sorted(counters.items())), "cases": dict(cases)}


def main() -> int:
    args = parse_args()
    if args.max_games < 0:
        raise ValueError("--max-games must be zero or positive")
    result = validate(args.archives, args.output, args.max_games)
    rendered = json.dumps(result, indent=2, sort_keys=True)
    if args.report:
        args.report.write_text(f"{rendered}\n")
        LOG.info("wrote report to %s", args.report)
    print(rendered)
    bad = sum(
        result["counters"].get(name, 0)
        for name in (
            "appearance_boundary_mismatches",
            "resolution_event_key_mismatches",
            "resolution_provenance_mismatches",
            "missing_resolution_rows",
            "modern_conflict_issues",
            "missing_events",
            "missing_audits",
            "source_identity_mismatches",
            "source_context_mismatches",
            "source_cumulative_conflict",
            "parsed_prefix_conflict",
            "extraction_mismatches",
        )
    )
    return 1 if bad else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sys.exit(main())
