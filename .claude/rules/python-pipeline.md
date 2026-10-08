# Downstream Python pipeline (`bin/`)

After the Rust parser writes CSVs, `bin/parquet.py` and `bin/simple_files.py` produce the Parquet files actually consumed downstream. Per global rules, run Python with `uv`:

```bash
uv sync                                                     # default deps (pyarrow/pandas/sqlalchemy/boxball-schemas)
uv sync --extra ci                                          # adds awscli for `uv run aws s3 sync ...`
uv run python bin/parquet.py                                # csv/*.csv -> parquet/*.parquet (zstd, dictionary, DELTA_BINARY_PACKED on event_key)
uv run python bin/simple_files.py                           # gamelog/schedule/park/roster/bio CSV concat + parquet
uv run python bin/biodata.py                                # retrosheet/{teams,coaches,relatives,ejections,managers0,umpires0}.csv -> biodata/*.parquet
uv run python bin/fetch_retrosheet.py -o retrosheet         # assembles a fresh corpus from retrosheet.org per-year + bundle URLs
uv run python bin/patch_known_corpus_bugs.py <retrosheet> [--corrections-csv docs/ngl_box_corrections.csv]   # idempotent in-place fixes for game records the parser cannot resolve (single-game patches plus NLB box-score normalization; see docs/corpus_corrections.md)
uv run python -m unittest discover -s bin -p 'test_*.py'   # Python helper tests
```

`bin/biodata.py` writes to `biodata/*.parquet` and is synced to `s3://timeball/biodata` alongside the existing `event/` and `misc/` paths. It applies one well-known fixup: two malformed `ejections.csv` rows for game `NY1191108192` carry an extra empty field between `EJECTEENAME` and `TEAM`; the script drops the empty field on read and warns. All date columns are strict-parsed (`m/d/Y` for ejections/coaches, `YYYYMMDD` for managers0/umpires0) — bad dates fail loudly.

Deps live in `pyproject.toml` + `uv.lock`. CI installs with `uv sync --extra ci --frozen`. The `ci` extra (awscli + a `pyyaml>=6.0.1` override — awscli's lower bound otherwise resolves to 5.4.1, whose sdist no longer builds on Cython 3+) is gated off the default deps so local `uv sync` stays minimal.
