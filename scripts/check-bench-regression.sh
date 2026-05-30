#!/usr/bin/env bash
# Compile and run oxide-arb-bench hot_paths; optional baseline compare via critcmp.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "Running hot_paths benchmarks (release)..."
RESULT="$(mktemp)"
cargo bench -p oxide-arb-bench --bench hot_paths -- --output-format bencher | tee "$RESULT"

BASELINE="${BENCH_BASELINE:-$ROOT/benches/baseline/hot_paths.txt}"
mkdir -p "$(dirname "$BASELINE")"

if command -v critcmp >/dev/null 2>&1 && [[ -f "$BASELINE" ]]; then
  echo "Comparing against baseline $BASELINE (5% regression budget)..."
  critcmp "$BASELINE" "$RESULT" --threshold 5
elif [[ "${CI:-}" == "true" ]]; then
  echo "Regression gate cannot run in CI: install critcmp and provide $BASELINE." >&2
  exit 1
else
  echo "Saving snapshot to $BASELINE (install critcmp + re-run to enforce regression gate)."
  cp "$RESULT" "$BASELINE"
fi
