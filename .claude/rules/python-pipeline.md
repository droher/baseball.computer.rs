# Downstream Python pipeline (`bin/`)

After the Rust parser writes CSVs, `bin/parquet.py` and `bin/simple_files.py` produce the Parquet files actually consumed downstream. Per global rules, run Python with `uv`:

```bash
uv sync                                                     # default deps (pyarrow/pandas/sqlalchemy/boxball-schemas)
uv sync --extra ci                                          # adds awscli for `uv run aws s3 sync ...`
uv run python bin/parquet.py                                # csv/*.csv -> parquet/*.parquet (zstd, dictionary, DELTA_BINARY_PACKED on event_key); skips findings.csv and exits nonzero if any table fails to convert
uv run python bin/corpus_coverage.py --corpus <corpus>     # fails if any Corpus file has neither a published table nor a reason it is unpublished
uv run python bin/simple_files.py --corpus <corpus>         # gamelog/schedule/park/roster/bio/franchise/discrepancy/umpire_roster -> retrosheet_simple/, lahman/ -> baseballdatabank/, retrosheet/ngl_stats/ -> ngl_stats/
uv run python bin/biodata.py --corpus <corpus>              # retrosheet/{teams,coaches,relatives,ejections,managers0,umpires0}.csv -> biodata/*.parquet
uv run --group dev python -m pytest -q bin                  # Python helper tests
```

`<corpus>` is a Corpus revision root from the mirror (it holds `retrosheet/` and `lahman/`). Nothing in `bin/` downloads anything: `CurrentNames.csv` comes from the corpus; when it is missing (as in `2026.08-c1`), `franchise.parquet` is skipped with a warning. The warehouse reads franchises from its own `seed_franchises` instead.

The Lahman conversion keeps the names the warehouse reads (`SELECT * FROM read_parquet`): snake_case tables and columns as in boxball's schema (`FieldingOF.csv` -> `fielding_of`, `playerID` -> `player_id`, `IPouts` -> `ip_outs`, `2B` -> `_2b`), nullable integers, `debut`/`final_game` as timestamps, and `zr`/`height` as doubles. CSVs are read with `keep_default_na=False` so the National Association's `lgID` stays the string `NA`. Irregular headers live in `COLUMN_RENAMES`; a new Lahman header that the camelCase rule gets wrong needs an entry there.

`bin/corpus_coverage.py` holds `COVERAGE`: for every file kind in a Corpus revision, the table it is published as, or why it is not. CI runs it before the conversions, so a file kind the mirror adds fails the build until it gets an entry. Files whose information a published table already holds (the `*0.csv` older formats, `TEAM{YYYY}` lists, `ngl_all/`, `gamelogs_post/`) are unpublished for now.

`discrepancy.parquet` joins every `discrepancies/{YYYY}{a,f,n}ldiscs.csv` with the league from the file name. Rows that stop before the `Accepted` column are padded, and a row with unquoted commas in its notes has them joined back. `umpire_roster.parquet` joins `rosters/UMPIRES{YYYY}.txt` with the year from the file name.

`bin/biodata.py` writes to `biodata/*.parquet`. It applies one well-known fixup: two malformed `ejections.csv` rows for game `NY1191108192` carry an extra empty field between `EJECTEENAME` and `TEAM`; the script drops the empty field on read and warns. All date columns are strict-parsed (`m/d/Y` for ejections/coaches, `YYYYMMDD` for managers0/umpires0) — bad dates fail loudly.

Deps live in `pyproject.toml` + `uv.lock`. CI installs with `uv sync --extra ci --frozen`. The `ci` extra (awscli + a `pyyaml>=6.0.1` override — awscli's lower bound otherwise resolves to 5.4.1, whose sdist no longer builds on Cython 3+) is gated off the default deps so local `uv sync` stays minimal. The R2 sync steps set `AWS_REQUEST_CHECKSUM_CALCULATION` and `AWS_RESPONSE_CHECKSUM_VALIDATION` to `when_required`: R2 rejects the default integrity checksums that awscli 1.36+ sends. `[tool.uv] constraint-dependencies` keeps `urllib3` and `pyasn1` at patched versions. pytest is in the `dev` group.
