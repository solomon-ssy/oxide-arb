#!/usr/bin/env bash
# Enforce that business crates never import SeaORM entities directly.
# Only quant-pivot-repository and quant-pivot-storage (via feature gate) may access them.
set -euo pipefail

FORBIDDEN_CRATES=(
  "crates/quant-pivot-core"
  "crates/quant-pivot-risk"
  "crates/quant-pivot-algorithm"
  "crates/quant-pivot-api"
)

EXIT_CODE=0

for crate_dir in "${FORBIDDEN_CRATES[@]}"; do
  if [ ! -d "$crate_dir" ]; then
    continue
  fi

  matches=$(rg --no-heading 'quant_pivot_models::entities' "$crate_dir" || true)
  if [ -n "$matches" ]; then
    echo "ERROR: $crate_dir imports quant_pivot_models::entities directly:"
    echo "$matches"
    EXIT_CODE=1
  fi
done

if [ "$EXIT_CODE" -eq 0 ]; then
  echo "OK: No business crate imports entities directly."
fi

exit "$EXIT_CODE"
