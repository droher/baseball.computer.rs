"""
Audit raw Retrosheet corpus for codes/values that the Rust parser does not
recognize. Detects cases where the parser silently drops a value (mapping it to
`Unrecognized`/`Unknown` defaults) — like the `A` pitch-clock-violation pitch
char that landed unhandled in the 2023 corpus.

Run:
    uv run python bin/audit_codes.py <retrosheet_dir>

Output groups codes by source field. For each field:
  - missing_from_enum: present in corpus, absent from Rust enum  (FIX NEEDED)
  - unused_in_corpus:  in Rust enum, never observed  (informational)

The expected-value sets are hardcoded to mirror the strum-derived enums in
src/event_file/{info.rs,pitch_sequence.rs,box_score.rs,parser.rs}. When a new
variant is added to the Rust source, mirror it here.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from collections.abc import Iterator
from collections import Counter, defaultdict
from pathlib import Path

logger = logging.getLogger(__name__)

# --- Expected value sets (mirror Rust enums) -----------------------------

# src/event_file/parser.rs MappedRecord::try_from
EXPECTED_RECORD_TYPES = {
    "id",
    "version",
    "info",
    "start",
    "sub",
    "play",
    "badj",
    "padj",
    "ladj",
    "radj",
    "presadj",
    "com",
    "data",
    "stat",
    "line",
    "event",
}

# src/event_file/info.rs InfoRecord::try_from
EXPECTED_INFO_KEYS = {
    "visteam",
    "hometeam",
    "site",
    "umphome",
    "ump1b",
    "ump2b",
    "ump3b",
    "umplf",
    "umprf",
    "number",
    "daynight",
    "pitches",
    "fieldcond",
    "fieldcon",
    "precip",
    "sky",
    "winddir",
    "howscored",
    "gametype",
    "howentered",
    "windspeed",
    "timeofgame",
    "attendance",
    "temp",
    "innings",
    "usedh",
    "htbf",
    "date",
    "starttime",
    "wp",
    "lp",
    "save",
    "gwrbi",
    "scorer",
    "oscorer",
    "inputter",
    "translator",
    "inputtime",
    "edittime",
    "tiebreaker",
    "inputprogvers",
    "umpchange",
}

# src/event_file/info.rs categorical enums (lowercase)
EXPECTED_INFO_VALUES: dict[str, set[str]] = {
    "howscored": {"park", "tv", "radio", "unknown"},
    "fieldcond": {"dry", "soaked", "wet", "damp", "unknown"},
    "fieldcon": {"dry", "soaked", "wet", "damp", "unknown"},
    "precip": {"rain", "drizzle", "showers", "snow", "none", "unknown"},
    "sky": {"cloudy", "dome", "night", "overcast", "sunny", "unknown"},
    "winddir": {
        "fromcf",
        "fromlf",
        "fromrf",
        "ltor",
        "rtol",
        "tocf",
        "tolf",
        "torf",
        "unknown",
    },
    "daynight": {"day", "night", "unknown", ""},
    "pitches": {"pitches", "count", "none", "unknown"},
    "number": {"0", "1", "2", "3", "4"},
    # GameType variants come from traits.rs and use PascalCase serialization
    # in output, but raw values are lowercase per Retrosheet docs.
    # See src/event_file/traits.rs GameType.
    "gametype": {
        "exhibition",
        "preseason",
        "regular",
        "allstar",
        "playoff",
        "wildcard",
        "divisionseries",
        "lcs",
        "worldseries",
        "negroleagues",
        "championship",
        "unknown",
    },
}

# src/event_file/pitch_sequence.rs PitchType + control chars
EXPECTED_PITCH_CHARS = set("123.BCFHIKLMNOPQRSTUVXYA?") | set("*+>")

# src/event_file/box_score.rs BoxScoreLine
EXPECTED_STAT_TAGS = {
    "bline",
    "phline",
    "prline",
    "pline",
    "dline",
    "tline",
    "btline",
    "dtline",
}
# src/event_file/box_score.rs BoxScoreEvent
EXPECTED_EVENT_TAGS = {"dpline", "tpline", "hpline", "hrline", "sbline", "csline"}


# --- Corpus walkers ------------------------------------------------------


def iter_event_files(root: Path) -> Iterator[Path]:
    for ext_glob in ("**/*.EV*", "**/*.ED*", "**/*.EB*"):
        yield from root.glob(ext_glob)


def read_records(path: Path) -> Iterator[list[str]]:
    # csv.reader handles quoted commas in comments; some files are latin-1.
    with path.open("r", encoding="latin-1", newline="") as fh:
        reader = csv.reader(fh)
        for row in reader:
            if row:
                yield row


class Observed:
    def __init__(self) -> None:
        self.record_types: Counter[str] = Counter()
        self.info_keys: Counter[str] = Counter()
        self.info_values: dict[str, Counter[str]] = defaultdict(Counter)
        self.pitch_chars: Counter[str] = Counter()
        self.stat_tags: Counter[str] = Counter()
        self.event_tags: Counter[str] = Counter()


def collect_observed(root: Path) -> Observed:
    obs = Observed()
    for path in iter_event_files(root):
        for row in read_records(path):
            kind = row[0].strip()
            obs.record_types[kind] += 1
            if kind == "info" and len(row) >= 2:
                key = row[1].strip()
                obs.info_keys[key] += 1
                if len(row) >= 3:
                    obs.info_values[key][row[2].strip()] += 1
            elif kind == "play" and len(row) >= 6:
                # Strip whitespace; the Rust parser feeds the raw string into
                # a char iterator and silently drops stray whitespace into
                # `Unrecognized`. We don't want the audit to flag those as
                # missing codes since the data, not the parser, is at fault.
                for c in row[5].strip():
                    obs.pitch_chars[c] += 1
            elif kind == "stat" and len(row) >= 2:
                obs.stat_tags[row[1].strip()] += 1
            elif kind == "event" and len(row) >= 2:
                obs.event_tags[row[1].strip()] += 1
    return obs


# --- Reporting -----------------------------------------------------------


def report(name: str, expected: set[str], observed: Counter[str]) -> bool:
    missing = sorted(set(observed) - expected)
    unused = sorted(expected - set(observed))
    has_missing = bool(missing)
    if has_missing:
        rows = ", ".join(f"{v!r}={observed[v]}" for v in missing)
        logger.error("[FAIL] %s — MISSING FROM ENUM: %s", name, rows)
    else:
        logger.info("[ ok ] %s", name)
    if unused:
        logger.info("        unused in corpus : %s", unused)
    return has_missing


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    ap = argparse.ArgumentParser(description=__doc__)
    _ = ap.add_argument("input", type=Path, help="Retrosheet root dir")
    args = ap.parse_args()
    root: Path = args.input

    if not root.is_dir():
        sys.exit(f"not a directory: {root}")

    obs = collect_observed(root)

    any_fail = False
    any_fail |= report("record_types (col 1)", EXPECTED_RECORD_TYPES, obs.record_types)
    any_fail |= report("info_keys (col 2 of info,)", EXPECTED_INFO_KEYS, obs.info_keys)
    any_fail |= report(
        "pitch_chars (col 6 of play,)", EXPECTED_PITCH_CHARS, obs.pitch_chars
    )
    any_fail |= report("stat_tags (col 2 of stat,)", EXPECTED_STAT_TAGS, obs.stat_tags)
    any_fail |= report(
        "event_tags (col 2 of event,)", EXPECTED_EVENT_TAGS, obs.event_tags
    )

    logger.info("")
    logger.info("--- info values per categorical key ---")
    for key, expected in sorted(EXPECTED_INFO_VALUES.items()):
        observed = obs.info_values.get(key, Counter())
        any_fail |= report(f"info,{key}", expected, observed)

    sys.exit(1 if any_fail else 0)


if __name__ == "__main__":
    main()
