#!/usr/bin/env bash
# Wall-clock benchmark with hyperfine. Use for before/after comparisons of
# perf changes. Default 3 warmups + 5 runs is enough to spot >5% deltas
# on the full corpus.
#
#   bin/bench.sh                              # full corpus -> retrosheet/
#   bin/bench.sh tests/fixtures/events        # any input dir
#   BENCH_RUNS=10 BENCH_WARMUP=2 bin/bench.sh # override run counts
#   bin/bench.sh retrosheet --export-json target/profiling/bench.json

set -euo pipefail

if ! command -v hyperfine >/dev/null 2>&1; then
    echo "hyperfine not found. Install with: brew install hyperfine" >&2
    exit 1
fi

INPUT="${1:-retrosheet}"
shift || true
RUNS="${BENCH_RUNS:-10}"
WARMUP="${BENCH_WARMUP:-5}"

if [[ ! -d "$INPUT" ]]; then
    echo "Input dir $INPUT does not exist" >&2
    exit 1
fi

cargo build --profile profiling --quiet
BIN="target/profiling/baseball-computer"

OUT_CSV="$(mktemp -d)"
trap 'rm -rf "$OUT_CSV"' EXIT

# `caffeinate -i` prevents idle sleep during the run. A short sleep between
# iterations lets the CPU cool off so back-to-back runs are not throttled.
# We don't pass `--prepare`; the parser truncates output files on each run,
# so old data is overwritten and `rm -rf` between runs adds its own variance.
exec caffeinate -i hyperfine \
    --warmup "$WARMUP" \
    --runs "$RUNS" \
    --prepare "sleep 2" \
    "$@" \
    "$BIN -i $INPUT -o $OUT_CSV"
