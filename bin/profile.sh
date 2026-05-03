#!/usr/bin/env bash
# Sample the parser with samply. View the resulting profile in
# https://profiler.firefox.com (samply opens it automatically).
#
#   bin/profile.sh                       # full corpus -> retrosheet/
#   bin/profile.sh tests/fixtures/events # any input dir
#   SAMPLY_RATE=4000 bin/profile.sh ...  # override sampling rate (Hz)

set -euo pipefail

if ! command -v samply >/dev/null 2>&1; then
    echo "samply not found. Install with: brew install samply" >&2
    exit 1
fi

INPUT="${1:-retrosheet}"
OUT_CSV="${2:-target/profiling/csv}"
PROFILE_OUT="${PROFILE_OUT:-target/profiling/profile.json.gz}"
RATE="${SAMPLY_RATE:-2000}"

if [[ ! -d "$INPUT" ]]; then
    echo "Input dir $INPUT does not exist" >&2
    exit 1
fi

mkdir -p "$OUT_CSV" "$(dirname "$PROFILE_OUT")"

cargo build --profile profiling --quiet
BIN="target/profiling/baseball-computer"

echo "samply rate=${RATE}Hz  input=${INPUT}  csv=${OUT_CSV}  profile=${PROFILE_OUT}"
exec samply record \
    --rate "$RATE" \
    --save-only \
    --unstable-presymbolicate \
    --output "$PROFILE_OUT" \
    -- "$BIN" -i "$INPUT" -o "$OUT_CSV"
