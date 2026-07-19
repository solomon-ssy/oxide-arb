#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ERRORS=0
REPLACING_TABLES=(
  quant_domain_event
  quant_entry_condition_evaluation_event
  quant_feature_parity_event
  quant_report_recommendation_fact
  quant_report_market_funnel
)

echo "=== Checking ReplacingMergeTree correctness reads ==="
for table in "${REPLACING_TABLES[@]}"; do
  if rg -n "FROM[[:space:]]+${table}([[:space:]]|\\\\)*($|WHERE|PREWHERE|GROUP|ORDER|LIMIT)" \
    crates --glob '**/src/**/*.rs' 2>/dev/null; then
    echo "ERROR: production reads from ${table} must declare FINAL (or be rewritten as an audited argMax query)"
    ERRORS=$((ERRORS + 1))
  fi
done

echo "=== Checking production code never forces OPTIMIZE FINAL ==="
if rg -n -i 'OPTIMIZE([[:space:]]+TABLE)?[^;"\n]*FINAL' \
  crates --glob '**/src/**/*.rs' --glob '**/src/**/*.sql' 2>/dev/null; then
  echo "ERROR: OPTIMIZE FINAL is an operational anti-pattern and cannot be a correctness dependency"
  ERRORS=$((ERRORS + 1))
fi

if [ "$ERRORS" -ne 0 ]; then
  echo "$ERRORS ClickHouse correctness violation(s) found"
  exit 1
fi

echo "All ClickHouse correctness checks passed!"
