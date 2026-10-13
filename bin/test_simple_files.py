from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pyarrow.parquet as pq
import pytest


def load_simple_files() -> ModuleType:
    path = Path(__file__).with_name("simple_files.py")
    spec = importlib.util.spec_from_file_location("baseball_simple_files", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


simple_files = load_simple_files()


def test_lahman_keeps_na_league_ids_and_nulls_only_empty_cells(tmp_path: Path) -> None:
    source = tmp_path / "lahman"
    source.mkdir()
    (source / "Batting.csv").write_bytes(
        "﻿playerID,yearID,lgID,SB\naddybo01,1871,NA,\nabercda01,1872,NA,3\n".encode()
    )

    written = simple_files.convert_csv_dir(source, tmp_path / "out", required=True)

    assert [p.name for p in written] == ["batting.parquet"]
    table = pq.read_table(written[0])
    rows = table.to_pylist()
    assert [r["lg_id"] for r in rows] == ["NA", "NA"]
    assert rows[0]["player_id"] == "addybo01"
    assert rows[0]["sb"] is None
    assert rows[1]["sb"] == 3
    assert table.schema.field("sb").type == "int64"


@pytest.mark.parametrize(
    ("header", "column"),
    [
        ("playerID", "player_id"),
        ("G_batting", "g_batting"),
        ("InnOuts", "inn_outs"),
        ("WCWin", "wc_win"),
        ("BAOpp", "ba_opp"),
        ("IPouts", "ip_outs"),
        ("2B", "_2b"),
        ("Glf", "g_lf"),
        ("HRA", "hr_a"),
        ("teamIDretro", "team_id_retro"),
        ("finalGame", "final_game"),
        ("ERA", "era"),
        ("G_1b", "g_1b"),
    ],
)
def test_lahman_headers_take_the_warehouse_column_names(header: str, column: str) -> None:
    assert simple_files.snake_case(header) == column


@pytest.mark.parametrize(
    ("stem", "table"),
    [("FieldingOF", "fielding_of"), ("FieldingOFsplit", "fielding_of_split"),
     ("AllstarFull", "allstar_full"), ("People", "people"), ("teamstats", "teamstats")],
)
def test_csv_stems_take_the_warehouse_table_names(stem: str, table: str) -> None:
    assert simple_files.table_name(stem) == table


def test_people_dates_and_heights_keep_their_types(tmp_path: Path) -> None:
    source = tmp_path / "lahman"
    source.mkdir()
    (source / "People.csv").write_text(
        "playerID,height,debut,finalGame\naardsda01,75,2004-04-06,2015-08-23\nzz01,,,\n"
    )

    written = simple_files.convert_csv_dir(source, tmp_path / "out", required=True)

    schema = pq.read_table(written[0]).schema
    assert str(schema.field("height").type) == "double"
    assert str(schema.field("debut").type).startswith("timestamp")
    assert str(schema.field("final_game").type).startswith("timestamp")


def test_every_csv_in_the_folder_becomes_a_parquet(tmp_path: Path) -> None:
    source = tmp_path / "ngl_stats"
    source.mkdir()
    for name in ("Batting.csv", "teamstats.csv"):
        (source / name).write_text("a,b\n1,x\n")
    (source / "readme.txt").write_text("not a table")

    written = simple_files.convert_csv_dir(source, tmp_path / "out", required=False)

    assert sorted(p.name for p in written) == ["batting.parquet", "teamstats.parquet"]


def test_a_missing_optional_folder_is_skipped_and_a_required_one_fails(tmp_path: Path) -> None:
    assert simple_files.convert_csv_dir(tmp_path / "absent", tmp_path / "out", required=False) == []
    with pytest.raises(FileNotFoundError):
        simple_files.convert_csv_dir(tmp_path / "absent", tmp_path / "out", required=True)


def test_franchises_come_from_the_corpus_and_are_skipped_when_missing(tmp_path: Path) -> None:
    retrosheet = tmp_path / "retrosheet"
    retrosheet.mkdir()
    out = tmp_path / "simple"
    out.mkdir()
    assert simple_files.get_prebuilt_csvs(retrosheet, out) is False
    assert not (out / "franchise.parquet").exists()

    (retrosheet / "CurrentNames.csv").write_text(
        "ANA,ANA,AL,W,Los Angeles,Angels,,2005-04-05,,Anaheim,CA\n"
    )
    assert simple_files.get_prebuilt_csvs(retrosheet, out) is True

    rows = pq.read_table(out / "franchise.parquet").to_pylist()
    assert rows[0]["current_franchise_id"] == "ANA"
    assert rows[0]["nickname"] == "Angels"


DISCREPANCY_HEADER = "ID,Player,Year,Team,Type,Pos,Cat,Game,Retro,Official,Cross,Code,,Notes,Accepted\n"


def test_discrepancies_take_their_league_from_the_file_and_tolerate_ragged_rows(
    tmp_path: Path,
) -> None:
    source = tmp_path / "retrosheet" / "discrepancies"
    source.mkdir(parents=True)
    (source / "1944nldiscs.csv").write_text(
        DISCREPANCY_HEADER
        + "1944N0069,schub102,1944,CHN,O,,GDP,CHN194404231,1,2,1944N0055,SWAP,,Officially, 0-2,\n"
    )
    (source / "1901aldiscs.csv").write_text(
        DISCREPANCY_HEADER + "1901A0001,collj103,1901,BOS,O,,AB,WS1190105070,3,4,,,,SHs not shown\n"
    )

    written = simple_files.write_discrepancies(tmp_path / "retrosheet", tmp_path)

    assert written is not None
    rows = {row["discrepancy_id"]: row for row in pq.read_table(written).to_pylist()}
    assert rows["1944N0069"]["notes"] == "Officially, 0-2"
    assert rows["1944N0069"]["accepted"] is None
    assert rows["1944N0069"]["league"] == "NL"
    assert rows["1901A0001"]["notes"] == "SHs not shown"
    assert rows["1901A0001"]["league"] == "AL"
    assert rows["1901A0001"]["year"] == 1901


def test_umpire_rosters_take_their_year_from_the_file_name(tmp_path: Path) -> None:
    rosters = tmp_path / "retrosheet" / "rosters"
    rosters.mkdir(parents=True)
    (rosters / "UMPIRES1901.txt").write_text("ID,last,first\nallia101,Allison,Art\n")
    (rosters / "UMPIRES1902.txt").write_text("alstd901,Alston,Dave\n")
    (rosters / "NYA1901.ROS").write_text("x\n")

    written = simple_files.write_umpire_rosters(tmp_path / "retrosheet", tmp_path)

    assert written is not None
    assert pq.read_table(written).to_pylist() == [
        {"year": 1901, "umpire_id": "allia101", "last_name": "Allison", "first_name": "Art"},
        {"year": 1902, "umpire_id": "alstd901", "last_name": "Alston", "first_name": "Dave"},
    ]


def test_missing_discrepancies_and_umpire_rosters_are_skipped(tmp_path: Path) -> None:
    assert simple_files.write_discrepancies(tmp_path, tmp_path) is None
    assert simple_files.write_umpire_rosters(tmp_path, tmp_path) is None
