#!/usr/bin/env bash
# Enforce absolute micro-benchmark SLO ceilings (nanoseconds, release build).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

readonly SLO_SPECS="
book_store_apply_snapshot:5000
book_store_load_empty:500
"

bench_output="$(mktemp)"
trap 'rm -f "$bench_output"' EXIT

cargo bench -p quant-pivot-bench --bench hot_paths -- --output-format bencher >"$bench_output" 2>&1
cargo bench -p quant-pivot-bench --bench e2e_paths -- --output-format bencher >>"$bench_output" 2>&1

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
done <<<"$SLO_SPECS"

exit "$failed"
