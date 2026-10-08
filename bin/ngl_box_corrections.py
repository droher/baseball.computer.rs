from __future__ import annotations

import csv
import logging
import os
import re
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

UNKNOWN = frozenset({"NA", "?", ""})
VALID_FIELD_POSITIONS = frozenset(str(p) for p in range(1, 13))
STARTING_FIELD_POSITIONS = frozenset(str(p) for p in range(1, 11))
SIDES = frozenset({"0", "1"})

DIAGNOSTIC_RE = re.compile(r"NUMBER OF .+ DISAGREE BETWEEN .+")
MISTYPED_ID_RE = re.compile(r"Yid,([A-Z0-9]{12})")
MISFILED_SUB_LINE_RE = re.compile(r"event,(phline|prline),")

CSV_FIELDS = ("file", "line", "game_id", "rule", "before", "after")


@dataclass(frozen=True)
class Correction:
    file: str
    line: int
    game_id: str
    rule: str
    before: str
    after: str | None


@dataclass(frozen=True)
class Unresolved:
    file: str
    line: int
    game_id: str
    reason: str
    record: str


@dataclass(frozen=True)
class FileResult:
    corrections: list[Correction]
    unresolved: list[Unresolved]
    text: str


class _Edits:
    def __init__(self, lines: Sequence[str], file: str) -> None:
        self.lines: Sequence[str] = lines
        self.file: str = file
        self.changes: dict[int, str | None] = {}
        self.rules: dict[int, str] = {}
        self.games: dict[int, str] = {}
        self.unresolved: list[Unresolved] = []

    def text(self, i: int) -> str | None:
        return self.changes.get(i, self.lines[i])

    def fields(self, i: int) -> list[str]:
        current = self.text(i)
        return [] if current is None else next(csv.reader([current]))

    def set(self, i: int, rule: str, after: str | None) -> None:
        self.changes[i] = after
        self.rules[i] = f"{self.rules[i]}+{rule}" if i in self.rules else rule

    def flag(self, i: int, game_id: str, reason: str) -> None:
        self.unresolved.append(
            Unresolved(self.file, i + 1, game_id, reason, self.lines[i])
        )


def _apply_line_rules(edits: _Edits) -> None:
    for i, line in enumerate(edits.lines):
        if DIAGNOSTIC_RE.fullmatch(line):
            edits.set(i, "drop_validator_message", None)
        elif m := MISTYPED_ID_RE.fullmatch(line):
            edits.set(i, "mistyped_id_record", f"id,{m.group(1)}")
        elif MISFILED_SUB_LINE_RE.match(line):
            edits.set(i, "misfiled_sub_line", "stat," + line.removeprefix("event,"))


def _game_blocks(edits: _Edits) -> Iterable[tuple[str, list[int]]]:
    game_id = ""
    block: list[int] = []
    for i in range(len(edits.lines)):
        current = edits.text(i)
        if current is not None and current.startswith("id,"):
            if block:
                yield game_id, block
            game_id = current.removeprefix("id,")
            block = []
        block.append(i)
    if block:
        yield game_id, block


def _record_type(fields: list[str]) -> str:
    if not fields:
        return ""
    if fields[0] in ("stat", "event") and len(fields) > 1:
        return f"{fields[0]},{fields[1]}"
    return fields[0]


def _player_sides(edits: _Edits, block: list[int]) -> dict[str, set[str]]:
    sides: dict[str, set[str]] = defaultdict(set)
    for i in block:
        f = edits.fields(i)
        kind = _record_type(f)
        if kind in ("start", "sub") and len(f) >= 6 and f[3] in SIDES:
            sides[f[1]].add(f[3])
        elif kind in ("stat,bline", "stat,dline", "stat,pline") and len(f) > 3:
            if f[3] in SIDES:
                sides[f[2]].add(f[3])
    return sides


def _known_side(sides: dict[str, set[str]], player: str) -> str | None:
    found = sides.get(player, set())
    return next(iter(found)) if len(found) == 1 else None


def _fix_line_score(edits: _Edits, game_id: str, block: list[int]) -> None:
    runs_by_side: dict[str, int] = defaultdict(int)
    runs_known: dict[str, bool] = defaultdict(lambda: True)
    for i in block:
        f = edits.fields(i)
        if _record_type(f) == "stat,bline" and len(f) > 7:
            if f[7].isdigit():
                runs_by_side[f[3]] += int(f[7])
            else:
                runs_known[f[3]] = False
    for i in block:
        f = edits.fields(i)
        if _record_type(f) != "line" or len(f) < 3:
            continue
        innings = f[2:]
        trimmed = list(innings)
        while trimmed and trimmed[-1] in ("", "x"):
            _ = trimmed.pop()
        if trimmed == innings:
            continue
        if not trimmed or not all(v.isdigit() for v in trimmed):
            edits.flag(i, game_id, "line score has non-numeric innings")
            continue
        side = f[1]
        if side in runs_by_side and runs_known[side]:
            if sum(int(v) for v in trimmed) != runs_by_side[side]:
                edits.flag(i, game_id, "line score total disagrees with bline runs")
                continue
        edits.set(i, "trailing_line_score_placeholder", ",".join([*f[:2], *trimmed]))


def _first_defensive_position(
    edits: _Edits, block: list[int], player: str, side: str
) -> str | None:
    found = [
        f[5]
        for i in block
        if _record_type(f := edits.fields(i)) == "stat,dline"
        and len(f) > 5
        and f[2] == player
        and f[3] == side
        and f[4] == "1"
    ]
    if len(found) != 1 or found[0] not in STARTING_FIELD_POSITIONS:
        return None
    return found[0]


def _fix_start_positions(edits: _Edits, game_id: str, block: list[int]) -> None:
    for i in block:
        f = edits.fields(i)
        if _record_type(f) != "start" or len(f) < 6:
            continue
        position = f[-1]
        if position in VALID_FIELD_POSITIONS:
            continue
        derived = _first_defensive_position(edits, block, f[1], f[3])
        if derived is None:
            edits.flag(i, game_id, "starter position not derivable from dline")
            continue
        if position.isdigit() and derived not in position:
            edits.flag(i, game_id, "multi-position starter omits first dline position")
            continue
        rule = "blank_starter_position" if position == "" else "multi_position_starter"
        current = edits.text(i)
        assert current is not None
        edits.set(i, rule, f"{current.rsplit(',', 1)[0]},{derived}")


def _fix_unknown_dline_positions(edits: _Edits, game_id: str, block: list[int]) -> None:
    starters = {
        (f[1], f[3]): f[-1]
        for i in block
        if _record_type(f := edits.fields(i)) == "start" and len(f) >= 6
    }
    for i in block:
        f = edits.fields(i)
        if _record_type(f) != "stat,dline" or len(f) < 6 or f[5] not in UNKNOWN:
            continue
        start_position = starters.get((f[2], f[3]))
        if f[4] != "1" or start_position not in STARTING_FIELD_POSITIONS:
            edits.flag(i, game_id, "dline position not derivable from start record")
            continue
        edits.set(
            i,
            "unknown_dline_position",
            ",".join([*f[:5], start_position, *f[6:]]),
        )


def _fix_sequence_numbers(
    edits: _Edits,
    game_id: str,
    block: list[int],
    kind: str,
    seq_index: int,
    group_key: tuple[int, ...],
    rule: str,
) -> None:
    groups: dict[tuple[str, ...], list[int]] = defaultdict(list)
    for i in block:
        f = edits.fields(i)
        if _record_type(f) == kind and len(f) > max(seq_index, *group_key):
            groups[tuple(f[k] for k in group_key)].append(i)
    for members in groups.values():
        unknown = [i for i in members if edits.fields(i)[seq_index] in UNKNOWN]
        if not unknown:
            continue
        consistent = all(
            edits.fields(i)[seq_index] in UNKNOWN
            or edits.fields(i)[seq_index] == str(n)
            for n, i in enumerate(members, start=1)
        )
        if not consistent:
            for i in unknown:
                edits.flag(i, game_id, f"{kind} sequence not derivable from order")
            continue
        for n, i in enumerate(members, start=1):
            f = edits.fields(i)
            if f[seq_index] in UNKNOWN:
                f[seq_index] = str(n)
                edits.set(i, rule, ",".join(f))


def _fix_event_lines(edits: _Edits, game_id: str, block: list[int]) -> None:
    sides = _player_sides(edits, block)
    for i in block:
        f = edits.fields(i)
        kind = _record_type(f)
        if kind == "event,hpline" and len(f) >= 5 and all(v in UNKNOWN for v in f[2:]):
            edits.set(i, "placeholder_hpline", None)
        elif kind == "event,hpline" and len(f) >= 5 and f[2] not in SIDES:
            edits.flag(i, game_id, "hpline pitching side not derivable")
        elif kind == "event,hrline" and len(f) >= 8 and f[2] not in SIDES:
            if f[2] in UNKNOWN and all(v in UNKNOWN or v == "-1" for v in f[3:]):
                edits.set(i, "placeholder_hrline", None)
                continue
            side = _known_side(sides, f[3]) if f[2] in UNKNOWN else None
            if side is None:
                edits.flag(i, game_id, "hrline batting side not derivable")
                continue
            edits.set(i, "unknown_hrline_side", ",".join([*f[:2], side, *f[3:]]))
        elif (
            kind in ("event,dpline", "event,tpline") and len(f) >= 4 and f[2] in UNKNOWN
        ):
            fielder_sides = {_known_side(sides, p) for p in f[3:]}
            if len(fielder_sides) != 1 or None in fielder_sides:
                edits.flag(i, game_id, "fielding-play side not derivable")
                continue
            side = next(iter(fielder_sides))
            assert side is not None
            edits.set(i, "unknown_fielding_play_side", ",".join([*f[:2], side, *f[3:]]))
        elif kind == "stat,phline" and len(f) >= 5 and f[4] not in SIDES:
            if f[3] not in SIDES or not f[4].isdigit():
                edits.flag(i, game_id, "phline side not derivable")
                continue
            known = _known_side(sides, f[2])
            if known is not None and known != f[3]:
                edits.flag(i, game_id, "phline swapped side disagrees with bline")
                continue
            edits.set(
                i, "swapped_phline_inning_side", ",".join([*f[:3], f[4], f[3], *f[5:]])
            )


def correct_text(text: str, file: str) -> FileResult:
    raw_lines = text.splitlines(keepends=True)
    stripped = [line.rstrip("\r\n") for line in raw_lines]
    edits = _Edits(stripped, file)
    _apply_line_rules(edits)
    for game_id, block in _game_blocks(edits):
        for i in block:
            edits.games[i] = game_id
        _fix_line_score(edits, game_id, block)
        _fix_start_positions(edits, game_id, block)
        _fix_unknown_dline_positions(edits, game_id, block)
        _fix_sequence_numbers(
            edits, game_id, block, "stat,bline", 5, (3, 4), "unknown_bline_sequence"
        )
        _fix_sequence_numbers(
            edits, game_id, block, "stat,pline", 4, (3,), "unknown_pline_sequence"
        )
        _fix_sequence_numbers(
            edits, game_id, block, "stat,dline", 4, (2, 3), "unknown_dline_sequence"
        )
        _fix_event_lines(edits, game_id, block)

    corrections = [
        Correction(
            file, i + 1, edits.games.get(i, ""), edits.rules[i], stripped[i], after
        )
        for i, after in sorted(edits.changes.items())
    ]
    out: list[str] = []
    for i, raw in enumerate(raw_lines):
        after = edits.text(i)
        if after is None:
            continue
        out.append(after + raw[len(stripped[i]) :])
    return FileResult(corrections, edits.unresolved, "".join(out))


def correct_directory(
    root: Path, subdir: str = "ngl_b"
) -> tuple[list[Correction], list[Unresolved]]:
    corrections: list[Correction] = []
    unresolved: list[Unresolved] = []
    for path in sorted((root / subdir).glob("*.EB*")):
        relative = path.relative_to(root).as_posix()
        text = path.read_bytes().decode("utf-8", errors="surrogateescape")
        result = correct_text(text, relative)
        corrections.extend(result.corrections)
        unresolved.extend(result.unresolved)
        for c in result.corrections:
            logger.debug(
                "%s:%d %s %s %r -> %r",
                c.file,
                c.line,
                c.game_id,
                c.rule,
                c.before,
                c.after,
            )
        for u in result.unresolved:
            logger.warning(
                "UNRESOLVED %s:%d %s %s: %s",
                u.file,
                u.line,
                u.game_id,
                u.reason,
                u.record,
            )
        if result.corrections:
            tmp = path.with_suffix(path.suffix + ".tmp")
            _ = tmp.write_bytes(result.text.encode("utf-8", errors="surrogateescape"))
            os.replace(tmp, path)
            logger.info("CORRECT %s: %d record(s)", relative, len(result.corrections))
    return corrections, unresolved


def write_corrections_csv(corrections: Sequence[Correction], path: Path) -> None:
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(CSV_FIELDS)
        for c in corrections:
            writer.writerow(
                (
                    c.file,
                    c.line,
                    c.game_id,
                    c.rule,
                    c.before,
                    "" if c.after is None else c.after,
                )
            )
