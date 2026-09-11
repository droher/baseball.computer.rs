from __future__ import annotations

import importlib.util
from importlib import import_module
from pathlib import Path
from typing import Protocol, cast

import pyarrow as pa


class ParquetReader(Protocol):
    def read_table(self, source: Path) -> pa.Table: ...


pyarrow_parquet = cast(ParquetReader, import_module("pyarrow.parquet"))


class ParquetConverter(Protocol):
    def file_to_data_frame_to_parquet(
        self, local_file: str, parquet_file: str
    ) -> None: ...

    def write_empty_parquet(self, stem: str, parquet_file: str) -> bool: ...


def load_converter() -> ParquetConverter:
    path = Path(__file__).with_name("parquet.py")
    spec = importlib.util.spec_from_file_location("baseball_parquet_converter", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load converter from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return cast(ParquetConverter, module)


parquet_converter = load_converter()


def test_status_preserves_empty_and_nonempty_raw_sequences(tmp_path: Path) -> None:
    source = tmp_path / "event_pitch_sequence_status.csv"
    destination = tmp_path / "event_pitch_sequence_status.parquet"
    source.write_text(
        "game_id,event_id,event_key,appearance_start_event_id,status,raw_pitch_sequence\n"
        "ABC202404010,1,2,1,Unavailable,\n"
        "ABC202404010,2,1,1,Resolved,B*BBC+1.+1F>X\n"
    )

    parquet_converter.file_to_data_frame_to_parquet(str(source), str(destination))

    table = pyarrow_parquet.read_table(destination)
    assert table.column_names == [
        "game_id",
        "event_id",
        "event_key",
        "appearance_start_event_id",
        "status",
        "raw_pitch_sequence",
    ]
    assert table.to_pylist() == [
        {
            "game_id": "ABC202404010",
            "event_id": 2,
            "event_key": 1,
            "appearance_start_event_id": 1,
            "status": "Resolved",
            "raw_pitch_sequence": "B*BBC+1.+1F>X",
        },
        {
            "game_id": "ABC202404010",
            "event_id": 1,
            "event_key": 2,
            "appearance_start_event_id": 1,
            "status": "Unavailable",
            "raw_pitch_sequence": "",
        },
    ]


def test_issues_preserve_strings_and_null_empty_numeric_ids(tmp_path: Path) -> None:
    source = tmp_path / "event_pitch_sequence_issues.csv"
    destination = tmp_path / "event_pitch_sequence_issues.parquet"
    source.write_text(
        "game_id,event_id,event_key,appearance_start_event_id,sequence_id,reason,prior_event_id,prior_raw_pitch_sequence,current_raw_pitch_sequence\n"
        'ABC202404010,3,9,1,1,"metadata changed, runner flag",,,'
        ">B.*B*SC3.X\n"
        "ABC202404010,4,10,1,1,current sequence disappeared,3,B*B.C,\n"
    )

    parquet_converter.file_to_data_frame_to_parquet(str(source), str(destination))

    rows = pyarrow_parquet.read_table(destination).to_pylist()
    assert rows[0]["reason"] == "metadata changed, runner flag"
    assert rows[0]["prior_event_id"] is None
    assert rows[0]["prior_raw_pitch_sequence"] == ""
    assert rows[0]["current_raw_pitch_sequence"] == ">B.*B*SC3.X"
    assert rows[1]["prior_event_id"] == 3
    assert rows[1]["prior_raw_pitch_sequence"] == "B*B.C"
    assert rows[1]["current_raw_pitch_sequence"] == ""


def test_empty_issues_csv_emits_typed_parquet(tmp_path: Path) -> None:
    destination = tmp_path / "event_pitch_sequence_issues.parquet"

    emitted = parquet_converter.write_empty_parquet(
        "event_pitch_sequence_issues", str(destination)
    )

    table = pyarrow_parquet.read_table(destination)
    assert emitted
    assert table.num_rows == 0
    assert table.column_names == [
        "game_id",
        "event_id",
        "event_key",
        "appearance_start_event_id",
        "sequence_id",
        "reason",
        "prior_event_id",
        "prior_raw_pitch_sequence",
        "current_raw_pitch_sequence",
    ]
    assert table.schema.types[table.column_names.index("prior_event_id")] == pa.uint8()


def test_scorer_columns_preserve_numeric_strings_and_all_empty_nulls(
    tmp_path: Path,
) -> None:
    source = tmp_path / "games.csv"
    destination = tmp_path / "games.parquet"
    source.write_text(
        "game_id,event_key,official_scorer,source_scorer\n"
        "ABC202404010,2,00123,\n"
        "ABC202404020,1,00007,\n"
    )

    parquet_converter.file_to_data_frame_to_parquet(str(source), str(destination))

    table = pyarrow_parquet.read_table(destination)
    assert table.schema.field("official_scorer").type == pa.string()
    assert table.schema.field("source_scorer").type == pa.string()
    assert table.to_pylist() == [
        {
            "game_id": "ABC202404010",
            "event_key": 2,
            "official_scorer": "00123",
            "source_scorer": None,
        },
        {
            "game_id": "ABC202404020",
            "event_key": 1,
            "official_scorer": "00007",
            "source_scorer": None,
        },
    ]


def test_scorer_columns_preserve_literal_null_words_and_free_text(
    tmp_path: Path,
) -> None:
    source = tmp_path / "box_score_games.csv"
    destination = tmp_path / "box_score_games.parquet"
    source.write_text(
        "game_id,event_key,official_scorer,source_scorer,legacy_text\n"
        'ABC202404010,1,NA,"Press box, section 14 and a long source label",NA\n'
        "ABC202404020,2,NULL,unknown,NULL\n"
        "ABC202404030,3,unknown,N/A,N/A\n"
    )

    parquet_converter.file_to_data_frame_to_parquet(str(source), str(destination))

    table = pyarrow_parquet.read_table(destination)
    assert table.column("official_scorer").to_pylist() == ["NA", "NULL", "unknown"]
    assert table.column("source_scorer").to_pylist() == [
        "Press box, section 14 and a long source label",
        "unknown",
        "N/A",
    ]
    assert table.column("legacy_text").to_pylist() == [None, None, None]


def test_old_game_csv_does_not_gain_provenance_columns(tmp_path: Path) -> None:
    source = tmp_path / "games.csv"
    destination = tmp_path / "games.parquet"
    source.write_text(
        "game_id,event_key,scorer,legacy_text\nABC202404010,1,scogc701,NA\n"
    )

    parquet_converter.file_to_data_frame_to_parquet(str(source), str(destination))

    table = pyarrow_parquet.read_table(destination)
    assert "official_scorer" not in table.column_names
    assert "source_scorer" not in table.column_names
    assert table.column("scorer").to_pylist() == ["scogc701"]
    assert table.column("legacy_text").to_pylist() == [None]
