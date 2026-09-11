# Workspace

This is a Cargo workspace whose root package is the legacy Retrosheet parser. The `crates/retrosheet-grammar/` member contains parser grammar helpers. The parser implementation and pitch-history extraction live under `src/event_file/`.

The parser output feeds the `baseball.computer` dbt pipeline. Per-crate guidance must preserve the root parser's CSV, Parquet, and event-key contracts.
