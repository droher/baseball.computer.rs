# Conventions specific to this repo

- Adding a new output table = add an `EventFileSchema` enum variant in `main.rs`, a row struct + (usually) `ContextToVec` impl in `schemas.rs`, and a write call in `write_play_by_play_files` or `write_box_score_files`. The `WriterMap`, header logic, and CSV path follow automatically from the enum.
- Output filenames are `{schema}.csv` where `schema` is the `snake_case` strum display of the variant — `bin/parquet.py` reads from `csv/*.csv` and writes `parquet/*.parquet` by stem, so renaming a variant renames the downstream Parquet file and breaks dbt sources.
- Schema changes must stay aligned with `baseball.computer`'s staging models. Coordinate before renaming columns. See [`docs/schema.md`](docs/schema.md) for the full variant→struct→filename map.
- `event/` and `data/` directories are generated artifacts (gitignored). `alldata.zip` at the repo root is a local Retrosheet snapshot (also gitignored content-wise).
