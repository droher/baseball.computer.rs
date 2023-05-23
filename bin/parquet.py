import glob

from pyarrow import csv, parquet


def file_to_data_frame_to_parquet(local_file: str, parquet_file: str) -> None:
    explicit_types = {
        "event_id": "uint8",
        "inning": "uint8",
        "outs": "uint8",
        "count_balls": "uint8",
        "count_strikes": "uint8",
        "fielding_position": "uint8",
        "lineup_position": "uint8",
        "at_bat": "uint8",
        "event_key": "uint32",
        "game_key": "uint32",
    }
    table = csv.read_csv(local_file, convert_options=csv.ConvertOptions(strings_can_be_null=True, column_types=explicit_types))
    parquet.write_table(table, parquet_file, compression='zstd', row_group_size=1000000000, write_batch_size=1000000000)


if __name__ == "__main__":
    for f in glob.glob("csv/*.csv"):
        print(f)
        try:
            fname = f.split("/")[-1].split(".")[0]
            file_to_data_frame_to_parquet(f, f"parquet/{fname}.parquet")
        except Exception as e:
            print(e)
