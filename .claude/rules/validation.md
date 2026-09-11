# Tests / lint

```bash
cargo test                   # unit + integration; ~1s for the suite
cargo clippy --all-targets   # lint level is strict — see main.rs
cargo fmt
```

Unit tests live as inline `#[cfg(test)] mod tests` blocks at the bottom of each `src/event_file/*.rs` module — they cover pitch sequence parsing, play parsing (`ParsedPlay::try_from`, `PlayStats` invariants), info records, box-score lines, and the `MappedRecord` dispatch. Integration tests in `tests/integration.rs` invoke the compiled binary against `tests/fixtures/events/` (fixtures spanning All-Star, postseason wild card, deduced PBP, single-game box score, dead-ball regression cases `1914_BSN.EVN` for bogus `presadj` and `1948_BIR.EBR` for trailing-space `info` values, plus `2023ALW1.EVE` for a deep substitution chain and `2023NLW1.EVE` for comments interleaved with subs/plays) and assert schema-level invariants on the resulting CSV/JSONL — row counts, `event_key` uniqueness, `events → games` referential integrity, and rerun determinism. `csv_snapshots_are_stable` additionally pins canonicalized (sort body, keep header) snapshots of every CSV output under `tests/snapshots/all_fixtures/`. Run `BLESS=1 cargo test csv_snapshots_are_stable` to regenerate after intentional output changes. JSONL is not snapshotted because some rows preserve `Vec` field ordering (e.g. `out_on_play`) which currently varies across runs (HashMap iteration upstream). The fixtures are checked into the repo.

Full-corpus validation remains the final gate: run the binary against the full Retrosheet corpus and diff Parquet output downstream. Before validating, run `uv run python bin/audit_codes.py <retrosheet_dir>` to surface any codes the parser silently maps to `Unrecognized`/`Unknown` (pitch chars, info keys/values, stat/event tags, record types). Exits non-zero if any code in the corpus is missing from the corresponding Rust enum — the audit caught the `A` pitch-clock-violation strike code that was silently dropped on the 2023+ corpus, and is the canonical way to detect new Retrosheet additions before they hit downstream.

`main.rs` declares `#![forbid(unsafe_code)]` and `#![deny(clippy::all, clippy::cargo)]` plus warn-level `nursery`/`pedantic`/`unwrap_used`/`expect_used`. Don't relax these globally — prefer per-call `#[allow(...)]` only at justified `expect()` sites (mirroring existing code). The pitch-history fix passes `cargo clippy --all-targets`; keep that gate clean.
