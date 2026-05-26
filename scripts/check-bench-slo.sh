#!/usr/bin/env bash
# Enforce absolute micro-benchmark SLO ceilings (nanoseconds, release build).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RESULT="$(mktemp)"
cargo bench -p oxide-arb-bench --bench hot_paths -- --output-format bencher | tee "$RESULT"

declare -A SLO_NS=(
  ["pipeline_process"]=100000
  ["pre_trade_pass"]=50000
  ["book_apply_snapshot_50"]=5000
  ["book_apply_delta_50"]=2000
  ["dual_book_assemble_50_levels"]=10000
  ["coalescer_pair_flush"]=50000
  ["funnel_immediate_dispatch"]=50000
)

failed=0
while IFS= read -r line; do
  for name in "${!SLO_NS[@]}"; do
    if [[ "$line" == *"test $name"* ]]; then
      ns="$(echo "$line" | awk '{print $(NF-1)}' | tr -d 'ns')"
      limit="${SLO_NS[$name]}"
      if (( ns > limit )); then
        echo "SLO FAIL: $name ${ns}ns > ${limit}ns"
        failed=1
      else
        echo "SLO OK: $name ${ns}ns <= ${limit}ns"
      fi
    fi
  done
done < "$RESULT"

rm -f "$RESULT"
exit "$failed"
