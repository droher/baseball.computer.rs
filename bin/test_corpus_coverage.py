from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType


def load_corpus_coverage() -> ModuleType:
    path = Path(__file__).with_name("corpus_coverage.py")
    spec = importlib.util.spec_from_file_location("baseball_corpus_coverage", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


corpus_coverage = load_corpus_coverage()


def test_every_known_kind_is_covered_once() -> None:
    paths = [
        "retrosheet/events/2025NYA.EVA",
        "retrosheet/events/TEAM2025",
        "retrosheet/boxes/1897.EBN",
        "retrosheet/ebe/1912.EBE",
        "retrosheet/rosters/NYA2025.ROS",
        "retrosheet/rosters/UMPIRES2025.txt",
        "retrosheet/ngl_all/batting.csv",
        "retrosheet/ngl_all/biofiles/teams.csv",
        "retrosheet/ngl_all/rosters/CAG1920.ROS",
        "retrosheet/teams0.csv",
        "lahman/Batting.csv",
        "REVISION.json",
    ]

    assert corpus_coverage.uncovered(paths) == ([], [])


def test_a_new_file_kind_is_reported() -> None:
    missing, _ = corpus_coverage.uncovered(["retrosheet/newfolder/x.csv", "retrosheet/x.csv"])

    assert missing == ["retrosheet/newfolder/x.csv", "retrosheet/x.csv"]


def test_a_wildcard_does_not_reach_into_subfolders() -> None:
    covered = corpus_coverage.Covered("retrosheet/ngl_all/*.csv", None)

    assert covered.matches("retrosheet/ngl_all/batting.csv")
    assert not covered.matches("retrosheet/ngl_all/biofiles/teams.csv")


def test_corpus_paths_reach_every_file_under_the_root(tmp_path: Path) -> None:
    (tmp_path / "retrosheet/discrepancies").mkdir(parents=True)
    (tmp_path / "retrosheet/discrepancies/1897nldiscs.csv").write_text("ID\n")
    (tmp_path / "retrosheet/unknown.dat").write_text("")

    missing, _ = corpus_coverage.uncovered(corpus_coverage.corpus_paths(tmp_path))

    assert missing == ["retrosheet/unknown.dat"]
