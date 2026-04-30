import glob
import os

import pyarrow as pa
from pyarrow import csv, parquet

# pyarrow.csv.read_csv raises on a zero-byte CSV. The Rust parser writes one
# CSV per EventFileSchema variant, but two box-score line variants
# (`box_score_team_batting_lines`, `box_score_team_fielding_lines`) come from
# Retrosheet `btline`/`dtline` records that the corpus does not currently
# contain — only `tline` (-> miscellaneous) is emitted.
#
# dbt sources expect the parquet files regardless. Provide an empty-schema
# fallback for these tables so an empty source still parses. Keep this in
# sync with the row structs in src/event_file/box_score.rs:
#   - TeamBattingLine = (side, BattingLineStats)
#   - TeamDefenseLine = (side, DefenseLineStats)
# wrapped by BoxScoreWritableRecord which prepends game_id.
EMPTY_SCHEMA_FALLBACK: dict[str, pa.Schema] = {
    "box_score_team_batting_lines": pa.schema(
        [
            ("game_id", pa.string()),
            ("side", pa.string()),
            ("at_bats", pa.int8()),
            ("runs", pa.int8()),
            ("hits", pa.int8()),
            ("doubles", pa.int8()),
            ("triples", pa.int8()),
            ("home_runs", pa.int8()),
            ("rbi", pa.int8()),
            ("sacrifice_hits", pa.int8()),
            ("sacrifice_flies", pa.int8()),
            ("hit_by_pitch", pa.int8()),
            ("walks", pa.int8()),
            ("intentional_walks", pa.int8()),
            ("strikeouts", pa.int8()),
            ("stolen_bases", pa.int8()),
            ("caught_stealing", pa.int8()),
            ("grounded_into_double_plays", pa.int8()),
            ("reached_on_interference", pa.int8()),
        ]
    ),
    "box_score_team_fielding_lines": pa.schema(
        [
            ("game_id", pa.string()),
            ("side", pa.string()),
            ("outs_played", pa.int8()),
            ("putouts", pa.int8()),
            ("assists", pa.int8()),
            ("errors", pa.int8()),
            ("double_plays", pa.int8()),
            ("triple_plays", pa.int8()),
            ("passed_balls", pa.int8()),
        ]
    ),
}


def file_to_data_frame_to_parquet(local_file: str, parquet_file: str) -> None:
    explicit_types = {
        "event_key": "int32",
    }
    table = csv.read_csv(local_file,
                         convert_options=csv.ConvertOptions(strings_can_be_null=True,
                                                            column_types=explicit_types))
    if "event" in local_file:
        table = table.sort_by("event_key")
    parquet.write_table(table,
                        parquet_file,
                        compression='zstd',
                        use_dictionary = [c for c in table.column_names if c != "event_key"],
                        column_encoding={'event_key': 'DELTA_BINARY_PACKED'})


def write_empty_parquet(stem: str, parquet_file: str) -> bool:
    """Emit an empty parquet using EMPTY_SCHEMA_FALLBACK if available."""
    schema = EMPTY_SCHEMA_FALLBACK.get(stem)
    if schema is None:
        return False
    table = pa.Table.from_pylist([], schema=schema)
    parquet.write_table(table, parquet_file, compression="zstd")
    print(f"  -> emitted empty parquet for {stem} (no rows in corpus)")
    return True


if __name__ == "__main__":
    for f in glob.glob("csv/*.csv"):
        print(f)
        fname = f.split("/")[-1].split(".")[0]
        parquet_path = f"parquet/{fname}.parquet"
        if os.path.getsize(f) == 0:
            if not write_empty_parquet(fname, parquet_path):
                print(f"  -> skipped empty CSV (no fallback schema): {fname}")
            continue
        try:
            file_to_data_frame_to_parquet(f, parquet_path)
        except Exception as e:
            print(e)
