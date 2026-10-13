import argparse
import fnmatch
import logging
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import cast

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class Covered:
    pattern: str
    published_as: str | None
    reason: str = ""

    def matches(self, path: str) -> bool:
        parts, wanted = PurePosixPath(path).parts, PurePosixPath(self.pattern).parts
        return len(parts) == len(wanted) and all(
            fnmatch.fnmatchcase(part, want) for part, want in zip(parts, wanted)
        )


PARSER = "event, game, box_score (the Rust parser)"
REVISIT = "overlaps a published table; revisit for the information only it holds"

COVERAGE: tuple[Covered, ...] = (
    *(
        Covered(f"retrosheet/{folder}/{name}", PARSER)
        for folder in ("events", "boxes", "ebe", "allstar", "postseason", "ngl_e", "ngl_b")
        for name in ("*.EV*", "*.ED*", "*.EB*")
    ),
    *(
        Covered(f"retrosheet/{folder}/TEAM*", None, f"{REVISIT}: biodata.teams")
        for folder in ("events", "boxes", "allstar", "postseason")
    ),
    Covered("retrosheet/rosters/*.ROS", "misc.roster"),
    Covered("retrosheet/rosters/UMPIRES*.txt", "misc.umpire_roster"),
    Covered("retrosheet/gamelogs/gl*.txt", "misc.gamelog"),
    Covered("retrosheet/gamelogs_post/*.txt", None, "every game also has an event file"),
    Covered("retrosheet/schedules/*.csv", "misc.schedule"),
    Covered("retrosheet/discrepancies/*.csv", "misc.discrepancy"),
    Covered("retrosheet/ngl_stats/*.csv", "ngl_stats"),
    Covered("retrosheet/ngl_all/*.csv", None, f"{REVISIT}: ngl_stats, which lacks 10 of its games"),
    Covered("retrosheet/ngl_all/biofiles/*.csv", None, f"{REVISIT}: misc.bio and biodata"),
    Covered("retrosheet/ngl_all/rosters/*.ROS", None, f"{REVISIT}: misc.roster, 27 of which differ"),
    Covered("retrosheet/ballparks.csv", "misc.park"),
    Covered("retrosheet/biofile.csv", "misc.bio"),
    Covered("retrosheet/CurrentNames.csv", "misc.franchise"),
    *(
        Covered(f"retrosheet/{name}.csv", f"biodata.{name}")
        for name in ("teams", "coaches", "relatives", "ejections", "managers0", "umpires0")
    ),
    *(
        Covered(f"retrosheet/{name}0.csv", None, f"{REVISIT}: {table}")
        for name, table in (
            ("biofile", "misc.bio"),
            ("ballparks", "misc.park"),
            ("teams", "biodata.teams"),
            ("coaches", "biodata.coaches"),
        )
    ),
    Covered("lahman/*.csv", "baseballdatabank"),
    Covered("lahman/readme*.txt", None, "documentation"),
    Covered("CORRECTIONS.csv", None, "the mirror's record of applied Corrections"),
    Covered("REVISION.json", None, "the mirror's revision metadata"),
)


def uncovered(paths: list[str]) -> tuple[list[str], list[str]]:
    missing: list[str] = []
    ambiguous: list[str] = []
    for path in paths:
        hits = [c.pattern for c in COVERAGE if c.matches(path)]
        if not hits:
            missing.append(path)
        elif len(hits) > 1:
            ambiguous.append(f"{path} ({', '.join(hits)})")
    return missing, ambiguous


def corpus_paths(corpus: Path) -> list[str]:
    return sorted(p.relative_to(corpus).as_posix() for p in corpus.rglob("*") if p.is_file())


def main() -> int:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--corpus", type=Path, required=True, help="Corpus revision root")
    corpus = cast(Path, parser.parse_args().corpus)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    paths = corpus_paths(corpus)
    missing, ambiguous = uncovered(paths)
    for path in missing:
        LOG.error("No published table or reason covers %s", path)
    for entry in ambiguous:
        LOG.error("More than one coverage entry matches %s", entry)
    unpublished = sum(
        1 for path in paths for c in COVERAGE if c.published_as is None and c.matches(path)
    )
    LOG.info("%d Corpus files: %d unpublished with a reason", len(paths), unpublished)
    return 1 if missing or ambiguous else 0


if __name__ == "__main__":
    sys.exit(main())
