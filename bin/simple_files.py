import fileinput
from pathlib import Path
from typing import Dict, Type, Iterator, List, Tuple

import pyarrow as pa
import pandas
from boxball_schemas import retrosheet
from pyarrow import csv as pcsv
from pyarrow import parquet as pq
from sqlalchemy import Integer, SmallInteger, Float, String, CHAR, Text, Boolean, Date, DateTime
from sqlalchemy import Table as AlchemyTable
from sqlalchemy.sql.type_api import TypeEngine

RETROSHEET_PATH = Path("retrosheet")
OUTPUT_PATH = Path("retrosheet_simple")
DATABANK_PATH = Path("baseballdatabank")

RETROSHEET_SUBDIRS = "gamelog", "schedule", "misc", "rosters"
FILES = "gamelog", "schedule", "park", "roster"

# MS-DOS eof character that needs to be specially handled in some files
DOS_EOF = chr(26)


def get_prebuilt_csvs() -> None:
    franchise_header = ["current_franchise_id", "team_id", "league", "division", "location", "nickname",
                        "alternate_nicknames", "date_start", "date_end", "city", "state"]
    (pandas
     .read_csv("https://www.retrosheet.org/CurrentNames.csv", names=franchise_header, parse_dates=["date_start", "date_end"])
     .to_parquet("retrosheet_simple/franchise.parquet", index=False)
     )
    databank_files = ["Appearances", "Batting", "Fielding", "Pitching", "People"]
    DATABANK_PATH.mkdir(exist_ok=True)
    for f in databank_files:
        github_url = f"https://raw.githubusercontent.com/chadwickbureau/baseballdatabank/master/core/{f}.csv"
        df = pandas.read_csv(github_url)
        if "yearID" in df.columns:
            # Filter out all years after 1900
            df = df[df["yearID"] <= 1900]
        df.to_parquet(f"baseballdatabank/{f.lower()}.parquet", index=False)


def parse_simple_files() -> None:
    def concat_files(input_path: Path, output_file: Path, glob: str = "*",
                     prepend_filename: bool = False,
                     strip_header: bool = False,
                     check_dupes: bool = True):
        files = [f for f in input_path.glob(glob) if f.is_file()]

        with open(output_file, 'wt') as fout, fileinput.input(files) as fin:
            lines = set()
            for line in fin:
                year = Path(fin.filename()).stem[-4:]
                # Remove DOS EOF character (CRTL+Z)
                new_line = line.strip(DOS_EOF)
                if not new_line or new_line.isspace():
                    continue
                if fin.isfirstline() and strip_header:
                    continue
                # Only grab gamelogs without event/boxscores
                if "gamelog" in output_file.name and new_line[-2] != "N":
                    continue
                if prepend_filename:
                    new_line = f"{year},{new_line}"
                if new_line in lines:
                    print(f"Duplicate row in {fin.filename()}: {new_line.strip()}")
                    continue
                if check_dupes:
                    lines.add(new_line)
                fout.write(new_line)

    retrosheet_base = Path(RETROSHEET_PATH)
    output_base = Path(OUTPUT_PATH)
    output_base.mkdir(exist_ok=True)
    subdirs = {subdir: retrosheet_base / subdir for subdir in RETROSHEET_SUBDIRS}

    print("Writing simple files...")
    concat_files(subdirs["gamelog"], output_base / "gamelog.csv", glob="*.TXT", check_dupes=False)
    concat_files(subdirs["schedule"], output_base / "schedule.csv", glob="*.TXT")
    concat_files(subdirs["misc"], output_base / "park.csv", glob="parkcode.txt", strip_header=True)
    concat_files(subdirs["rosters"], output_base / "roster.csv", glob="*.ROS", prepend_filename=True)


sql_type_lookup: Dict[Type[TypeEngine], str] = {
    Integer: 'int32',
    SmallInteger: 'int16',
    Float: 'float64',
    String: 'str',
    CHAR: 'str',
    Text: 'str',
    Boolean: 'bool',
    # Some Parquet targets can't handle Parquet dates, so we need to parse and pass timestamps
    Date: 'timestamp[ms]',
    DateTime: 'timestamp[ms]'
}


def get_fields(table: AlchemyTable) -> List[Tuple[str, str]]:
    cols = [(c.name, c.type) for c in table.columns.values() if c.autoincrement is not True]
    return [(name, sql_type_lookup[type(dtype)]) for name, dtype in cols]


def write_files() -> None:
    """
    Creates a Parquet file for each table in the schema.
    """
    tables: Iterator[AlchemyTable] = [t for t in retrosheet.metadata.tables.values()
                                      if t.name in FILES]
    for table in tables:
        name = table.name
        print(name)

        extract_file = OUTPUT_PATH / f"{name}.csv"
        parquet_file = OUTPUT_PATH / f"{name}.parquet"

        arrow_schema = pa.schema(get_fields(table))
        column_names = [name for name, dtype in get_fields(table)]

        read_options = pcsv.ReadOptions(column_names=column_names, block_size=1000000000)
        parse_options = pcsv.ParseOptions(newlines_in_values=True)
        convert_options = pcsv.ConvertOptions(column_types=arrow_schema, timestamp_parsers=["%Y%m%d", "%Y-%m-%d"],
                                              true_values=["1", "T"], false_values=["0", "F"], strings_can_be_null=True)

        table = pcsv.read_csv(extract_file, read_options=read_options, parse_options=parse_options,
                              convert_options=convert_options)

        pq.write_table(table, parquet_file, compression="zstd")


if __name__ == "__main__":
    parse_simple_files()
    write_files()
    get_prebuilt_csvs()
