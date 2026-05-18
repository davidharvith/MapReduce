#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BENCH="${ROOT}/build/bench"
OUT="${ROOT}/benchmarks/results.csv"
INPUTS="${INPUTS:-500000}"
THREADS="${THREADS:-1 2 4 8}"

if [[ ! -x "$BENCH" ]]; then
  echo "Build bench first: cmake --build build --target bench" >&2
  exit 1
fi

echo "threads,inputs,time_s,inputs_per_sec" >"$OUT"
for t in $THREADS; do
  "$BENCH" --threads "$t" --inputs "$INPUTS" --csv --repeat 3
done >>"$OUT"

echo "Wrote $OUT"
cat "$OUT"
