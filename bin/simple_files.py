import argparse
import csv as stdcsv
import fileinput
import logging
import re
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

# Retrosheet added a `park_id` column to schedule files starting in 2024
# (e.g. SEO01 for the 2024 Seoul series). The published `boxball-schemas`
# 0.0.8 still describes the legacy 12-column shape. Override here so the
# output Parquet matches the modern shape; pad legacy rows with an empty
# park_id so all rows have 13 columns.
SCHEDULE_COLUMNS_13 = [
    ("date", "timestamp[ms]"),
    ("double_header", "int16"),
    ("day_of_week", "str"),
    ("visiting_team", "str"),
    ("visiting_team_league", "str"),
    ("visiting_team_game_number", "int16"),
    ("home_team", "str"),
    ("home_team_league", "str"),
    ("home_team_game_number", "int32"),
    ("day_night", "str"),
    ("park_id", "str"),
    ("postponement_indicator", "str"),
    ("makeup_dates", "str"),
]
SCHEDULE_PARK_INSERT_AT = 10  # index of park_id, between day_night and postponement_indicator

OUTPUT_PATH = Path("retrosheet_simple")
LAHMAN_OUTPUT_PATH = Path("baseballdatabank")
NGL_STATS_OUTPUT_PATH = Path("ngl_stats")
FRANCHISE_FILE = "CurrentNames.csv"
DISCREPANCY_FILE = re.compile(r"(\d{4})([afn])ldiscs\.csv")
DISCREPANCY_LEAGUES = {"a": "AL", "f": "FL", "n": "NL"}
DISCREPANCY_COLUMNS = (
    "discrepancy_id", "player_id", "year", "team_id", "stat_group", "position", "category",
    "game_id", "retrosheet_value", "official_value", "cross_reference", "code", "extra",
    "notes", "accepted",
)
DISCREPANCY_NOTES_AT = DISCREPANCY_COLUMNS.index("notes")
UMPIRE_ROSTER_FILE = re.compile(r"UMPIRES(\d{4})\.txt")
TABLE_RENAMES = {"FieldingOFsplit": "fielding_of_split"}
COLUMN_RENAMES = {
    "2B": "_2b", "3B": "_3b", "IPouts": "ip_outs", "Ghome": "g_home",
    "HA": "h_a", "HRA": "hr_a", "BBA": "bb_a", "SOA": "so_a",
    "Glf": "g_lf", "Gcf": "g_cf", "Grf": "g_rf",
    "teamIDBR": "team_id_br", "teamIDlahman45": "team_id_lahman45", "teamIDretro": "team_id_retro",
    "teamIDwinner": "team_id_winner", "lgIDwinner": "lg_id_winner",
    "teamIDloser": "team_id_loser", "lgIDloser": "lg_id_loser",
    "NAassoc": "na_assoc", "yearid": "year_id",
    "yearkey": "year_id", "leaguekey": "lg_id", "teamkey": "team_id", "parkkey": "park_id",
    "spanfirst": "first_game", "spanlast": "last_game", "parkalias": "park_alias", "parkname": "park_name",
}
DATE_COLUMNS = "debut", "final_game"
FLOAT_COLUMNS = "zr", "height"
CAMEL_BOUNDARY = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")

LOG = logging.getLogger(__name__)

RETROSHEET_SUBDIRS = "gamelogs", "schedules", "rosters"
FILES = "gamelog", "schedule", "park", "roster", "bio"

# MS-DOS eof character that needs to be specially handled in some files
DOS_EOF = chr(26)


def get_prebuilt_csvs(retrosheet_base: Path, output_base: Path = OUTPUT_PATH) -> bool:
    franchise_header = ["current_franchise_id", "team_id", "league", "division", "location", "nickname",
                        "alternate_nicknames", "date_start", "date_end", "city", "state"]
    source = retrosheet_base / FRANCHISE_FILE
    if not source.is_file():
        LOG.warning("%s is missing from the Corpus revision; skipping franchise.parquet", source)
        return False
    (pandas
     .read_csv(source, names=franchise_header, parse_dates=["date_start", "date_end"])
     .to_parquet(output_base / "franchise.parquet", index=False)
     )
    return True


def discrepancy_fields(row: list[str], source: str) -> list[str]:
    width = len(DISCREPANCY_COLUMNS)
    if len(row) > width:
        LOG.warning("%s: joining %d unquoted commas into notes for %s", source, len(row) - width, row[0])
        overflow = len(row) - width
        notes = ",".join(row[DISCREPANCY_NOTES_AT:DISCREPANCY_NOTES_AT + overflow + 1])
        row = [*row[:DISCREPANCY_NOTES_AT], notes, *row[DISCREPANCY_NOTES_AT + overflow + 1:]]
    return [field.strip() for field in row] + [""] * (width - len(row))


def write_discrepancies(retrosheet_base: Path, output_base: Path = OUTPUT_PATH) -> Path | None:
    source = retrosheet_base / "discrepancies"
    if not source.is_dir():
        LOG.warning("%s is missing from the Corpus revision; skipping discrepancy.parquet", source)
        return None
    columns: dict[str, list[str]] = {name: [] for name in (*DISCREPANCY_COLUMNS, "league")}
    short = 0
    for path in sorted(source.glob("*.csv")):
        match = DISCREPANCY_FILE.fullmatch(path.name)
        if match is None:
            raise ValueError(f"Unexpected discrepancy file name {path.name}")
        league = DISCREPANCY_LEAGUES[match.group(2)]
        with path.open(encoding="utf-8-sig", newline="") as stream:
            rows = [row for row in stdcsv.reader(stream) if row][1:]
        for row in rows:
            short += len(row) < len(DISCREPANCY_COLUMNS)
            for name, value in zip(DISCREPANCY_COLUMNS, discrepancy_fields(row, path.name)):
                columns[name].append(value)
            columns["league"].append(league)
    if short:
        LOG.info("Padded %d discrepancy rows that end before the accepted column", short)
    table = pa.table({name: pa.array([v or None for v in values], pa.string()) for name, values in columns.items()})
    table = table.set_column(
        table.schema.get_field_index("year"), "year", table.column("year").cast(pa.int16())
    )
    target = output_base / "discrepancy.parquet"
    pq.write_table(table, target, compression="zstd")
    LOG.info("Wrote %d discrepancy rows -> %s", table.num_rows, target)
    return target


def write_umpire_rosters(retrosheet_base: Path, output_base: Path = OUTPUT_PATH) -> Path | None:
    files = sorted((retrosheet_base / "rosters").glob("UMPIRES*.txt"))
    if not files:
        LOG.warning("No umpire rosters in the Corpus revision; skipping umpire_roster.parquet")
        return None
    columns: dict[str, list[object]] = {"year": [], "umpire_id": [], "last_name": [], "first_name": []}
    for path in files:
        match = UMPIRE_ROSTER_FILE.fullmatch(path.name)
        if match is None:
            raise ValueError(f"Unexpected umpire roster name {path.name}")
        with path.open(encoding="utf-8-sig", newline="") as stream:
            rows = [row for row in stdcsv.reader(stream) if row]
        if rows and rows[0][0].upper() == "ID":
            rows = rows[1:]
        for row in rows:
            if len(row) != 3:
                raise ValueError(f"{path.name}: expected 3 fields, got {row!r}")
            columns["year"].append(int(match.group(1)))
            for name, value in zip(("umpire_id", "last_name", "first_name"), row):
                columns[name].append(value.strip() or None)
    schema = pa.schema([("year", pa.int16()), ("umpire_id", pa.string()),
                        ("last_name", pa.string()), ("first_name", pa.string())])
    table = pa.table(columns, schema=schema)
    target = output_base / "umpire_roster.parquet"
    pq.write_table(table, target, compression="zstd")
    LOG.info("Wrote %d umpire roster rows from %d files -> %s", table.num_rows, len(files), target)
    return target


def snake_case(name: str) -> str:
    if name in COLUMN_RENAMES:
        return COLUMN_RENAMES[name]
    return CAMEL_BOUNDARY.sub("_", name).lower()


def table_name(stem: str) -> str:
    return TABLE_RENAMES.get(stem, snake_case(stem))


def read_verbatim_csv(path: Path) -> pandas.DataFrame:
    frame = pandas.read_csv(path, encoding="utf-8-sig", keep_default_na=False, na_values=[""],
                            low_memory=False, dtype_backend="numpy_nullable")
    frame = frame.rename(columns=snake_case)
    for column in DATE_COLUMNS:
        if column in frame.columns:
            frame[column] = pandas.to_datetime(frame[column], format="%Y-%m-%d")
    for column in FLOAT_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].astype("Float64")
    return frame


def convert_csv_dir(source: Path, output: Path, *, required: bool) -> list[Path]:
    if not source.is_dir():
        if required:
            raise FileNotFoundError(f"{source} is missing from the Corpus revision")
        LOG.warning("%s is missing from the Corpus revision; skipping it", source)
        return []
    output.mkdir(parents=True, exist_ok=True)
    written = []
    for csv_path in sorted(source.glob("*.csv")):
        target = output / f"{table_name(csv_path.stem)}.parquet"
        frame = read_verbatim_csv(csv_path)
        frame.to_parquet(target, index=False, compression="zstd")
        LOG.info("Wrote %s rows=%d -> %s", csv_path.name, len(frame), target)
        written.append(target)
    return written


def parse_simple_files(retrosheet_base: Path, output_base: Path = OUTPUT_PATH) -> None:
    def concat_files(input_path: Path, output_file: Path, glob: str = "*",
                     prepend_filename: bool = False,
                     strip_header: bool = False,
                     check_dupes: bool = True):
        files = sorted([f for f in input_path.glob(glob) if f.is_file()])
        print(f"Found {len(files)} files in {input_path}")
        with open(output_file, 'wt') as fout, fileinput.input(files) as fin:
            lines = set()
            for line in fin:
                year = Path(fin.filename()).stem[-4:]
                # Remove DOS EOF character (CRTL+Z)
                new_line = line.strip(DOS_EOF)
                original_line = new_line
                if not new_line or new_line.isspace():
                    continue
                if fin.isfirstline() and strip_header:
                    continue
                # Only grab gamelogs without event/boxscores or prior to 1920
                keep_line = "N" in new_line.rpartition(",")[-1] or new_line.partition(",")[0][:4] < "1920"
                if "gamelog" in output_file.name and not keep_line:
                    continue
                if prepend_filename:
                    new_line = f"{year},{new_line}"
                if new_line in lines:
                    print(f"Duplicate row in {fin.filename()}: {original_line.strip()}")
                    continue
                # TODO: Fix NLB roster file shape in raw data
                if "roster" in output_file.name and len(new_line.split(",")) == 7:
                    print(f"Fixing row in file {fin.filename()} with missing data: " + original_line.strip())
                    new_line = new_line.strip() + ","
                elif "roster" in output_file.name and len(new_line.split(",")) < 7:
                    print(f"Skipping row in file {fin.filename()} with missing data: " + original_line.strip())
                    continue
                if check_dupes:
                    lines.add(new_line)
                fout.write(new_line.strip() + "\n")

    output_base.mkdir(exist_ok=True)
    subdirs = {subdir: retrosheet_base / subdir for subdir in RETROSHEET_SUBDIRS}

    print("Writing simple files...")
    concat_files(subdirs["gamelogs"], output_base / "gamelog.csv", glob="*.txt", check_dupes=True)
    concat_files(subdirs["schedules"], output_base / "schedule.csv", glob="*.csv", check_dupes=True, strip_header=True)
    concat_files(retrosheet_base, output_base / "park.csv", glob="ballparks.csv", strip_header=True)
    concat_files(retrosheet_base, output_base / "bio.csv", glob="biofile.csv", strip_header=True)
    concat_files(subdirs["rosters"], output_base / "roster.csv", glob="*.ROS", prepend_filename=True)

    # Pad legacy 12-col schedule rows so the file is uniformly 13 cols.
    pad_schedule_csv(output_base / "schedule.csv")


def pad_schedule_csv(path: Path) -> None:
    """Insert an empty park_id column for any legacy 12-col schedule row."""
    tmp = path.with_suffix(".csv.tmp")
    padded = 0
    with path.open(newline="") as fin, tmp.open("w", newline="") as fout:
        reader = stdcsv.reader(fin)
        writer = stdcsv.writer(fout, quoting=stdcsv.QUOTE_MINIMAL)
        for row in reader:
            if len(row) == 12:
                row.insert(SCHEDULE_PARK_INSERT_AT, "")
                padded += 1
            elif len(row) != 13:
                raise ValueError(f"Unexpected schedule row width {len(row)}: {row!r}")
            writer.writerow(row)
    tmp.replace(path)
    print(f"Padded {padded} legacy schedule rows to 13 columns")


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


def fields_for(name: str, table: AlchemyTable) -> List[Tuple[str, str]]:
    if name == "schedule":
        return list(SCHEDULE_COLUMNS_13)
    return get_fields(table)


def write_files(output_base: Path = OUTPUT_PATH) -> None:
    """
    Creates a Parquet file for each table in the schema.
    """
    tables: Iterator[AlchemyTable] = [t for t in retrosheet.metadata.tables.values()
                                      if t.name in FILES]
    for table in tables:
        name = table.name
        print(name)

        extract_file = output_base / f"{name}.csv"
        parquet_file = output_base / f"{name}.parquet"

        fields = fields_for(name, table)
        arrow_schema = pa.schema(fields)
        column_names = [col_name for col_name, _ in fields]

        read_options = pcsv.ReadOptions(column_names=column_names, block_size=1000000000)
        parse_options = pcsv.ParseOptions(newlines_in_values=True)
        convert_options = pcsv.ConvertOptions(column_types=arrow_schema, timestamp_parsers=["%Y%m%d", "%Y-%m-%d", "%m/%d/%Y"],
                                              true_values=["1", "T"], false_values=["0", "F"], strings_can_be_null=True)

        table = pcsv.read_csv(extract_file, read_options=read_options, parse_options=parse_options,
                              convert_options=convert_options)

        pq.write_table(table, parquet_file, compression="zstd")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", type=Path, required=True, help="Corpus revision root")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    retrosheet_base = args.corpus / "retrosheet"
    parse_simple_files(retrosheet_base)
    write_files()
    get_prebuilt_csvs(retrosheet_base)
    write_discrepancies(retrosheet_base)
    write_umpire_rosters(retrosheet_base)
    convert_csv_dir(args.corpus / "lahman", LAHMAN_OUTPUT_PATH, required=True)
    convert_csv_dir(retrosheet_base / "ngl_stats", NGL_STATS_OUTPUT_PATH, required=False)


if __name__ == "__main__":
    main()
