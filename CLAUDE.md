# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project context

Rust parser that turns raw [Retrosheet](https://www.retrosheet.org/) event/box-score files into structured CSVs (and optionally JSONL). Downstream, those CSVs are converted to Parquet and consumed by the `baseball.computer` dbt pipeline. Retrosheet's event format is a stateful text format — the parser maintains full per-game state (lineup, runners, outs, score) to derive every output column.

**Status (2026-05-01):** Active. Legacy parser runs cleanly on current Retrosheet corpus (assembled by `bin/fetch_retrosheet.py` from retrosheet.org per-year + bundle URLs; the older `alldata.zip` workflow is deprecated since the published bundle lags by months). Full corpus = 205,886 games / 18.14M events in ~12s, with 11 known WARNs (six internal NLB box-file dupes in `1913.EBR`/`1926.EBR`; four pitcher-responsibility-for-non-existent-runner cases — `riege101`, `willv101`, `steeb102`, `oescj101` (BSN191409102 ER-attribution loss); one `info,hometeam,BIR ` whitespace strip in BIR194806210). Eleven games require the idempotent fix-up applied by `uv run python bin/patch_known_corpus_bugs.py <retrosheet_dir>` before parsing (ATN193807032 batter swap; CIN191007090 stray-space `data,er`; WS1191105040 `5-3` → `53` fielders; eight NLB box-score `info,site,...` park-ID typos in `ngl_b/{1921,1941,1946,1947,1948}.EBR` — `PHi17`→`PHI17`, `DEC01`→`DCT01`, 5×`DEC02`→`DCT02`, `CHESTER PA`→`CHE01`) — re-run it after every fresh fetch. The `info`-value trim is scoped to known string-typed fields (team/site/scorer/inputter/translator/wp/lp/save/gwrbi/umps); strict-parsed fields (date, numbers, bool, enum lookups) keep their original strict behavior so corrupt structured values still fail loudly. A `pest`-grammar rewrite remains an in-flight option but is not blocking. Confirm current direction with user before doing significant parser work.

Upstream raw data lives in the sibling `retrosheet-mirror` repo (or `droher/retrosheet-mirror` on GitHub). Output Parquet files are published to Cloudflare R2 (`s3://timeball/event`, `s3://timeball/misc`) and feed the dbt project at `droher/baseball.computer`.

## Build / run

```bash
cargo build --release
# Input dir contains Retrosheet files (.EVA/.EVN/.EVE/.EBA/.EBN, etc.); output dir is created if missing.
./target/release/baseball-computer -i <retrosheet_dir> -o <csv_out_dir>
# JSON mode (single games.jsonl, no per-schema CSVs):
./target/release/baseball-computer -i <retrosheet_dir> -o <out_dir> --json
```

CI builds with PGO via `.cargo/config.toml` (`-Cprofile-use=/tmp/pgo-data/merged.profdata`); local debug/release builds without that profdata work fine.

## Tests / lint

```bash
cargo test                   # unit + integration; ~1s for the suite
cargo clippy --all-targets   # lint level is strict — see main.rs
cargo fmt
```

Unit tests live as inline `#[cfg(test)] mod tests` blocks at the bottom of each `src/event_file/*.rs` module — they cover pitch sequence parsing, play parsing (`ParsedPlay::try_from`, `PlayStats` invariants), info records, box-score lines, and the `MappedRecord` dispatch. Integration tests in `tests/integration.rs` invoke the compiled binary against `tests/fixtures/events/` (small fixtures spanning All-Star, postseason wild card, deduced PBP, single-game box score, plus dead-ball regression cases `1914_BSN.EVN` for bogus `presadj` and `1948_BIR.EBR` for trailing-space `info` values) and assert schema-level invariants on the resulting CSV/JSONL — row counts, `event_key` uniqueness, `events → games` referential integrity, and rerun determinism. The fixtures are checked into the repo. Don't pin byte-for-byte snapshots — output ordering is non-deterministic via `rayon`, and per project rules tests should verify invariants, not hardcoded values.

Full-corpus validation remains the final gate: run the binary against the full Retrosheet corpus and diff Parquet output downstream. Before validating, run `uv run python bin/audit_codes.py <retrosheet_dir>` to surface any codes the parser silently maps to `Unrecognized`/`Unknown` (pitch chars, info keys/values, stat/event tags, record types). Exits non-zero if any code in the corpus is missing from the corresponding Rust enum — the audit caught the `A` pitch-clock-violation strike code that was silently dropped on the 2023+ corpus, and is the canonical way to detect new Retrosheet additions before they hit downstream.

`main.rs` declares `#![forbid(unsafe_code)]` and `#![deny(clippy::all, clippy::cargo)]` plus warn-level `nursery`/`pedantic`/`unwrap_used`/`expect_used`. Don't relax these globally — prefer per-call `#[allow(...)]` only at justified `expect()` sites (mirroring existing code). The current baseline has 56 pre-existing `clippy::all` errors in `src/event_file/{game_state,play,parser,box_score}.rs`; treat them as a known cleanup task, not a blocker.

## Downstream Python pipeline (`bin/`)

After the Rust parser writes CSVs, `bin/parquet.py` and `bin/simple_files.py` produce the Parquet files actually consumed downstream. Per global rules, run Python with `uv`:

```bash
uv sync                                                     # default deps (pyarrow/pandas/sqlalchemy/boxball-schemas)
uv sync --extra ci                                          # adds awscli for `uv run aws s3 sync ...`
uv run python bin/parquet.py                                # csv/*.csv -> parquet/*.parquet (zstd, dictionary, DELTA_BINARY_PACKED on event_key)
uv run python bin/simple_files.py                           # gamelog/schedule/park/roster/bio CSV concat + parquet
uv run python bin/biodata.py                                # retrosheet/{teams,coaches,relatives,ejections,managers0,umpires0}.csv -> biodata/*.parquet
uv run python bin/fetch_retrosheet.py -o retrosheet         # assembles a fresh corpus from retrosheet.org per-year + bundle URLs
uv run python bin/patch_known_corpus_bugs.py <retrosheet>   # idempotent in-place fixes for game records the parser cannot resolve (ATN193807032 / CIN191007090 / WS1191105040)
```

`bin/biodata.py` writes to `biodata/*.parquet` and is synced to `s3://timeball/biodata` alongside the existing `event/` and `misc/` paths. It applies one well-known fixup: two malformed `ejections.csv` rows for game `NY1191108192` carry an extra empty field between `EJECTEENAME` and `TEAM`; the script drops the empty field on read and warns. All date columns are strict-parsed (`m/d/Y` for ejections/coaches, `YYYYMMDD` for managers0/umpires0) — bad dates fail loudly.

Deps live in `pyproject.toml` + `uv.lock`. CI installs with `uv sync --extra ci --frozen`. Python is pinned to `>=3.10,<3.11` because `awscli==1.24.5` pulls `pyyaml==5.4.1`, which doesn't build on platforms without a prebuilt wheel; the `ci` extra is gated off the default deps so local `uv sync` doesn't trip on it.

## Architecture

Entry point: `src/main.rs`. Module tree: `src/event_file/` (declared via `src/event_file.rs`).

**Three-pass file processing.** `FileProcessor::process_files` runs three passes in sequence: conventional play-by-play, deduced play-by-play, then box scores. Each pass globs the input dir via `AccountType::glob`, sorts paths, then drives them through `rayon::par_iter`. Game IDs from the first two passes are accumulated into `self.game_ids`; subsequent passes skip already-seen IDs (warn log). Box-score files are expected to duplicate PBP and aren't checked. NLB All-Star/postseason `.EVR` dupes are filtered via `contains_nlb_dupes` (TODO to remove once raw data is fixed).

**Per-file pipeline.** `RetrosheetReader` (in `event_file/parser.rs`) is a stateful iterator that yields one `RecordVec` per game (a slice of `MappedRecord`s plus a line offset). For each game, `GameContext::new` (in `event_file/game_state.rs`) replays the records to produce a fully resolved `GameContext` containing events, lineup/fielding appearances, baserunner movements, etc. `EventFileSchema::write` then dispatches: JSON mode serializes `GameContext` straight to `games.jsonl`; box-score files go through `write_box_score_files`; play-by-play goes through `write_play_by_play_files`.

**Writer fan-out.** `EventFileSchema` is a `strum::EnumIter` enum (one variant per output table — see ~28 variants in `main.rs`). At startup, `WriterMap` constructs one `ThreadSafeCsvWriter` per variant under `OUTPUT_ROOT`. Writers use `Mutex<csv::Writer<File>>` for thread-safe append; box-score line variants set `uses_custom_header()` so headers are derived dynamically from the first row via `BoxScoreWritableRecord::generate_header` (gated by `has_header_written: AtomicBool`). Globals (`OUTPUT_ROOT`, `WRITER_MAP`, `JSON_WRITER`) are `lazy_static` — `OUTPUT_ROOT` calls `Opt::parse()` on first access, so CLI parsing happens implicitly on the first lazy_static touch.

**Schema serialization.** `event_file/schemas.rs` defines the row structs and the `ContextToVec` trait (`from_game_context(&GameContext) -> Vec<Self>`). The generic `WriterMap::write_csv::<C: ContextToVec>` is the standard path; non-`ContextToVec` outputs (`Games`, `BoxScoreLineScores`, comment streams, `EventFlags`, lineup/fielding appearances) write inline in `write_play_by_play_files` / `write_box_score_files`.

**Event key namespacing.** Each file's events are assigned globally unique `event_key`s by offsetting `(self.index + i) * EVENT_KEY_BUFFER` (constant in `event_file/traits.rs`). Don't reorder file processing without preserving this contract — the Parquet step in `bin/parquet.py` sorts by `event_key` and uses `DELTA_BINARY_PACKED` encoding for it.

**Play parsing cache.** `event_file/play.rs` uses `quick_cache` with stats; `print_cache_info()` runs at end of `main()`. Cache hit rate matters for full-corpus runs. Don't disable stats casually.

### Module map (`src/event_file/`)

- `parser.rs` — `RetrosheetReader`, `AccountType` (PlayByPlay / Deduced / BoxScore), `MappedRecord`, `RecordSlice`. The legacy hand-rolled parser; targeted by the in-flight `pest` rewrite.
- `game_state.rs` — `GameContext` and the state-machine logic that replays records.
- `play.rs` — play-string parsing + `quick_cache`.
- `pitch_sequence.rs` — pitch-sequence parsing.
- `box_score.rs` — `BoxScoreLine` / `BoxScoreEvent` enums.
- `info.rs` — `info,` field handling.
- `misc.rs` — `GameId` and small helpers.
- `traits.rs` — `GameType`, `EVENT_KEY_BUFFER`.
- `schemas.rs` — every output row struct + `ContextToVec`.

## Conventions specific to this repo

- Adding a new output table = add an `EventFileSchema` enum variant in `main.rs`, a row struct + (usually) `ContextToVec` impl in `schemas.rs`, and a write call in `write_play_by_play_files` or `write_box_score_files`. The `WriterMap`, header logic, and CSV path follow automatically from the enum.
- Output filenames are `{schema}.csv` where `schema` is the `snake_case` strum display of the variant — `bin/parquet.py` reads from `csv/*.csv` and writes `parquet/*.parquet` by stem, so renaming a variant renames the downstream Parquet file and breaks dbt sources.
- Schema changes must stay aligned with `baseball.computer`'s staging models. Coordinate before renaming columns. See [`docs/schema.md`](docs/schema.md) for the full variant→struct→filename map.
- `event/` and `data/` directories are generated artifacts (gitignored). `alldata.zip` at the repo root is a local Retrosheet snapshot (also gitignored content-wise).
