import glob

from pyarrow import csv, parquet


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

if __name__ == "__main__":
    for f in glob.glob("csv/*.csv"):
        print(f)
        try:
            fname = f.split("/")[-1].split(".")[0]
            file_to_data_frame_to_parquet(f, f"parquet/{fname}.parquet")
        except Exception as e:
            print(e)
