#!/usr/bin/env bash
# Enforce absolute micro-benchmark SLO ceilings (nanoseconds, release build).
# Portable bash (macOS / Linux) — no associative arrays required.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# name:limit pairs (nanoseconds).
readonly SLO_SPECS="
walk_micro_5:100
walk_micro_50:1000
walk_micro_200:5000
detect_with_direction:2000
pipeline_process:100000
pre_trade_pass:5000
pre_trade_fail_short:1000
book_apply_snapshot_50:5000
book_apply_delta_50:500
dual_book_assemble_50_levels:10000
coalescer_pair_flush:50000
funnel_immediate_dispatch:50000
ws_normalize_book_50:2000
scanner_scan_market:2000
execution_pipeline_paper_sync:10000
"

DELTA_SNAPSHOT_RATIO_NUM=6
DELTA_SNAPSHOT_RATIO_DEN=10

bench_output="$(mktemp)"
trap 'rm -f "$bench_output"' EXIT

cargo bench -p oxide-arb-bench --bench hot_paths -- --output-format bencher >"$bench_output" 2>&1

lookup_ns() {
  local name="$1"
  awk -v target="$name" '
    $1 == "test" && $2 == target && /bench:/ {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^[0-9]+$/ && $(i+1) == "ns/iter") {
          print $i
          exit
        }
      }
    }
  ' "$bench_output"
}

failed=0
snap=""
delta=""

while IFS= read -r spec; do
  [[ -z "$spec" ]] && continue
  name="${spec%%:*}"
  limit="${spec#*:}"
  ns="$(lookup_ns "$name")"
  if [[ -z "$ns" ]]; then
    echo "SLO FAIL: $name not found in bench output" >&2
    failed=1
    continue
  fi
  if (( ns > limit )); then
    echo "SLO FAIL: $name ${ns}ns > ${limit}ns" >&2
    failed=1
  else
    echo "SLO OK: $name ${ns}ns <= ${limit}ns"
  fi
  case "$name" in
    book_apply_snapshot_50) snap="$ns" ;;
    book_apply_delta_50) delta="$ns" ;;
  esac
done <<<"$SLO_SPECS"

if [[ -n "$snap" && -n "$delta" ]]; then
  ceiling=$(( snap * DELTA_SNAPSHOT_RATIO_NUM / DELTA_SNAPSHOT_RATIO_DEN ))
  if (( delta * DELTA_SNAPSHOT_RATIO_DEN >= snap * DELTA_SNAPSHOT_RATIO_NUM )); then
    echo "SLO FAIL: book_apply_delta_50 ${delta}ns >= book_apply_snapshot_50 * 0.6 (${ceiling}ns)" >&2
    failed=1
  else
    echo "SLO OK: book_apply_delta_50 ${delta}ns < book_apply_snapshot_50 * 0.6 (${ceiling}ns)"
  fi
fi

exit "$failed"
