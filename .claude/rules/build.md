# Build / run

```bash
cargo build --release
# Input dir contains Retrosheet files (.EVA/.EVN/.EVE/.EBA/.EBN, etc.); output dir is created if missing.
./target/release/baseball-computer -i <retrosheet_dir> -o <csv_out_dir>
# JSON mode (single games.jsonl, no per-schema CSVs):
./target/release/baseball-computer -i <retrosheet_dir> -o <out_dir> --json
```

CI builds with PGO via `.cargo/config.toml` (`-Cprofile-use=/tmp/pgo-data/merged.profdata`); local debug/release builds without that profdata work fine.

Every push fetches a fresh corpus from retrosheet.org unless a corpus cache for the current UTC date exists. To republish from an already cached corpus without any retrosheet requests, push with `[skip ci]` and run `gh workflow run build_parser -f retrosheet_key=YYYY-MM-DD` (see `gh cache list` for `retrosheet-*` keys); the run fails instead of fetching if that cache is gone.
