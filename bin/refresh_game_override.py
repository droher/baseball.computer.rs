# pyright: reportAny=false

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import patch_known_corpus_bugs

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Rewrite the diff in each <GAMEID>.diff from a corpus that has had "
            "PATCHES and NLB corrections applied but not game overrides, "
            "keeping the explanation."
        )
    )
    _ = parser.add_argument("corpus_dir", type=Path)
    _ = parser.add_argument("records", type=Path, nargs="+")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    corpus_dir: Path = args.corpus_dir
    records: list[Path] = args.records
    rc = 0
    for record in records:
        try:
            logger.info(
                "%s", patch_known_corpus_bugs.refresh_game_diff(record, corpus_dir)
            )
        except (OSError, ValueError):
            logger.exception("Failed refreshing %s", record)
            rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
