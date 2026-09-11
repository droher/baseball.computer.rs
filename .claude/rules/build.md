# Build / run

```bash
cargo build --release
# Input dir contains Retrosheet files (.EVA/.EVN/.EVE/.EBA/.EBN, etc.); output dir is created if missing.
./target/release/baseball-computer -i <retrosheet_dir> -o <csv_out_dir>
# JSON mode (single games.jsonl, no per-schema CSVs):
./target/release/baseball-computer -i <retrosheet_dir> -o <out_dir> --json
```

CI builds with PGO via `.cargo/config.toml` (`-Cprofile-use=/tmp/pgo-data/merged.profdata`); local debug/release builds without that profdata work fine.
