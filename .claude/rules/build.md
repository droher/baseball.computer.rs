# Build / run

```bash
cargo build --release
# Input dir is a Corpus revision's retrosheet/ subtree; output dir is created if missing.
./target/release/baseball-computer -i <corpus>/retrosheet -o <csv_out_dir>
# JSON mode (single games.jsonl, no per-schema CSVs):
./target/release/baseball-computer -i <corpus>/retrosheet -o <out_dir> --json
```

Locally, `<corpus>` is the mirror's `work/corrected` worktree (`uv run mirror worktrees` in `retrosheet-mirror`). The output root also gets `findings.csv`.

CI builds with PGO via `.cargo/config.toml` (`-Cprofile-use=/tmp/pgo-data/merged.profdata`); local debug/release builds without that profdata work fine.

CI (`.github/workflows/actions.yml`) checks out the mirror at the tag in `CORPUS_REVISION`, shallow, with the `MIRROR_DEPLOY_KEY` secret. Every push builds, tests, parses, and uploads `findings.csv` as an artifact; pushes never publish. Publishing to R2 and purging the Cloudflare cache happen only on a manual run on `master` for a promoted revision:

```bash
gh workflow run build_parser -f revision=2026.08-c1 -f publish=true
```

A `revision` input without `publish` builds that revision instead of `CORPUS_REVISION`.
