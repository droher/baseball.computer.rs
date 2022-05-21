import glob

from pyarrow import csv, parquet


def file_to_data_frame_to_parquet(local_file: str, parquet_file: str) -> None:
    table = csv.read_csv(local_file)
    parquet.write_table(table, parquet_file, compression='zstd')


if __name__ == "__main__":
    for f in glob.glob("csv/*.csv"):
        print(f)
        try:
            fname = f.split("/")[-1].split(".")[0]
            file_to_data_frame_to_parquet(f, f"parquet/{fname}.parquet")
        except Exception as e:
            print(e)
