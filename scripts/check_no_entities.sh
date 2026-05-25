#!/usr/bin/env bash
# Enforce that business crates never import SeaORM entities directly.
# Only oxide-arb-repository and oxide-arb-storage (via feature gate) may access them.
set -euo pipefail

FORBIDDEN_CRATES=(
  "crates/oxide-arb-core"
  "crates/oxide-arb-risk"
  "crates/oxide-arb-algorithm"
  "crates/oxide-arb-api"
)

EXIT_CODE=0

for crate_dir in "${FORBIDDEN_CRATES[@]}"; do
  if [ ! -d "$crate_dir" ]; then
    continue
  fi

  matches=$(rg --no-heading 'oxide_arb_models::entities' "$crate_dir" || true)
  if [ -n "$matches" ]; then
    echo "ERROR: $crate_dir imports oxide_arb_models::entities directly:"
    echo "$matches"
    EXIT_CODE=1
  fi
done

if [ "$EXIT_CODE" -eq 0 ]; then
  echo "OK: No business crate imports entities directly."
fi

exit "$EXIT_CODE"
