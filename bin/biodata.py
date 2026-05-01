"""Convert Retrosheet biodata CSVs to Parquet for downstream publishing.

Inputs live at the root of the fetched retrosheet/ directory and don't go
through the Rust event parser. Outputs are written to biodata/ alongside
retrosheet_simple/, then synced to s3://timeball/biodata for the
baseball.computer dbt project.

Tables (with renames where the upstream header is uppercase / inconsistent):
  - teams.csv        -> biodata/teams.parquet
  - coaches.csv      -> biodata/coaches.parquet
  - relatives.csv    -> biodata/relatives.parquet
  - ejections.csv    -> biodata/ejections.parquet
  - managers0.csv    -> biodata/managers0.parquet
  - umpires0.csv     -> biodata/umpires0.parquet

Schemas are explicit (pyarrow), dates parsed strictly. Two well-known
malformed ejections rows for game NY1191108192 carry an extra empty field
between EJECTEENAME and TEAM; we drop the empty field on read.
"""

from __future__ import annotations

import csv
import datetime as dt
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
from pyarrow import parquet as pq

logger = logging.getLogger(__name__)

RETROSHEET_PATH = Path("retrosheet")
OUTPUT_PATH = Path("biodata")


@dataclass(frozen=True)
class TableSpec:
    name: str
    source: str
    schema: pa.Schema
    date_columns: tuple[str, ...] = ()
    rename: dict[str, str] | None = None
    drop_extra_empty_at: int | None = None


def _date_mdy(value: str) -> dt.date | None:
    if not value:
        return None
    parts = value.split("/")
    if len(parts) != 3 or len(parts[2]) != 4:
        raise ValueError(f"unexpected m/d/Y date {value!r}")
    m, d, y = parts
    return dt.date(int(y), int(m), int(d))


def _date_ymd(value: str) -> dt.date | None:
    if not value:
        return None
    if len(value) != 8 or not value.isdigit():
        raise ValueError(f"unexpected YYYYMMDD date {value!r}")
    return dt.date(int(value[0:4]), int(value[4:6]), int(value[6:8]))


SPECS: tuple[TableSpec, ...] = (
    TableSpec(
        name="teams",
        source="teams.csv",
        rename={
            "TEAM": "team",
            "LEAGUE": "league",
            "CITY": "city",
            "NICKNAME": "nickname",
            "FIRST": "first_year",
            "LAST": "last_year",
        },
        schema=pa.schema(
            [
                ("team", pa.string()),
                ("league", pa.string()),
                ("city", pa.string()),
                ("nickname", pa.string()),
                ("first_year", pa.int16()),
                ("last_year", pa.int16()),
            ]
        ),
    ),
    TableSpec(
        name="coaches",
        source="coaches.csv",
        rename={"start": "start_date", "end": "end_date"},
        schema=pa.schema(
            [
                ("id", pa.string()),
                ("year", pa.int16()),
                ("team", pa.string()),
                ("role", pa.string()),
                ("start_date", pa.date32()),
                ("end_date", pa.date32()),
            ]
        ),
        date_columns=("start_date", "end_date"),
    ),
    TableSpec(
        name="relatives",
        source="relatives.csv",
        schema=pa.schema(
            [
                ("id1", pa.string()),
                ("relation", pa.string()),
                ("id2", pa.string()),
            ]
        ),
    ),
    TableSpec(
        name="ejections",
        source="ejections.csv",
        rename={
            "GAMEID": "game_id",
            "DATE": "date",
            "DH": "double_header",
            "EJECTEE": "ejectee",
            "EJECTEENAME": "ejectee_name",
            "TEAM": "team",
            "JOB": "job",
            "UMPIRE": "umpire",
            "UMPIRENAME": "umpire_name",
            "INNING": "inning",
            "REASON": "reason",
        },
        schema=pa.schema(
            [
                ("game_id", pa.string()),
                ("date", pa.date32()),
                ("double_header", pa.int8()),
                ("ejectee", pa.string()),
                ("ejectee_name", pa.string()),
                ("team", pa.string()),
                ("job", pa.string()),
                ("umpire", pa.string()),
                ("umpire_name", pa.string()),
                ("inning", pa.int16()),
                ("reason", pa.string()),
            ]
        ),
        date_columns=("date",),
        drop_extra_empty_at=5,
    ),
    TableSpec(
        name="managers0",
        source="managers0.csv",
        schema=pa.schema(
            [
                ("id", pa.string()),
                ("lastname", pa.string()),
                ("firstname", pa.string()),
                ("first_g", pa.date32()),
                ("last_g", pa.date32()),
            ]
        ),
        date_columns=("first_g", "last_g"),
    ),
    TableSpec(
        name="umpires0",
        source="umpires0.csv",
        schema=pa.schema(
            [
                ("id", pa.string()),
                ("lastname", pa.string()),
                ("firstname", pa.string()),
                ("first_g", pa.date32()),
                ("last_g", pa.date32()),
            ]
        ),
        date_columns=("first_g", "last_g"),
    ),
)


def _read_rows(path: Path, spec: TableSpec) -> Iterable[dict[str, str]]:
    with path.open(newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        target_cols = [
            (spec.rename or {}).get(h, h.lower().replace(" ", "_")) for h in header
        ]
        n = len(header)
        for line_no, row in enumerate(reader, start=2):
            if spec.drop_extra_empty_at is not None and len(row) == n + 1:
                idx = spec.drop_extra_empty_at
                if row[idx] != "":
                    raise ValueError(
                        f"{path}:{line_no} expected empty field at {idx}, got {row[idx]!r}"
                    )
                row = row[:idx] + row[idx + 1 :]
                logger.warning(
                    "Patched malformed row in %s line %d (dropped extra empty col)",
                    path.name,
                    line_no,
                )
            if len(row) != n:
                raise ValueError(
                    f"{path}:{line_no} field count {len(row)} != expected {n}: {row!r}"
                )
            yield dict(zip(target_cols, row, strict=True))


DATE_FORMATS_MDY = frozenset({"ejections", "coaches"})


def _coerce(spec: TableSpec, rows: list[dict[str, str]]) -> dict[str, list[object]]:
    field_types = {f.name: f.type for f in spec.schema}
    cols: dict[str, list[object]] = {name: [] for name in field_types}
    parse_date = _date_mdy if spec.name in DATE_FORMATS_MDY else _date_ymd
    for row in rows:
        for name, dtype in field_types.items():
            raw = row.get(name, "")
            if name in spec.date_columns:
                cols[name].append(parse_date(raw))
            elif pa.types.is_integer(dtype):
                cols[name].append(int(raw) if raw not in ("", None) else None)
            else:
                cols[name].append(raw if raw != "" else None)
    return cols


def write_biodata() -> None:
    OUTPUT_PATH.mkdir(exist_ok=True)
    for spec in SPECS:
        src = RETROSHEET_PATH / spec.source
        if not src.exists():
            raise FileNotFoundError(
                f"Required biodata source missing: {src}. "
                "Run bin/fetch_retrosheet.py first."
            )
        rows = list(_read_rows(src, spec))
        cols = _coerce(spec, rows)
        table = pa.table(cols, schema=spec.schema)
        out = OUTPUT_PATH / f"{spec.name}.parquet"
        pq.write_table(table, out, compression="zstd")
        logger.info("Wrote %s rows=%d -> %s", spec.name, len(rows), out)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    write_biodata()


if __name__ == "__main__":
    main()
