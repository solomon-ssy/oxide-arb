#!/usr/bin/env bash
set -euo pipefail

# Error-layer hygiene — typed sub-errors via From/?; no Internal abuse in production src.

fail=0

if rg -n 'QuantError::Internal\(' crates/*/src --glob '!**/tests/**' --glob '!crates/quant-pivot-error/**' 2>/dev/null; then
  echo "ERROR: QuantError::Internal must not appear in production src/ (use typed sub-errors)"
  fail=1
fi

if rg -n 'fn \w+_error\([^)]*\) -> QuantError' crates/*/src --glob '!**/tests/**' 2>/dev/null; then
  echo "ERROR: manual *_error() -> QuantError mappers forbidden — use From + ? propagation"
  fail=1
fi

FORBIDDEN_TYPES='MarketRegistryError|RuntimeControlError|WindowQueryError'
if rg -n "$FORBIDDEN_TYPES" crates/ 2>/dev/null; then
  echo "ERROR: deleted legacy error types detected (see lint-quant-pivot-errors.sh)"
  fail=1
fi

if rg -n 'StorageError::Conflict|is_duplicate_entity' crates/*/src --glob '!**/tests/**' 2>/dev/null; then
  echo "ERROR: StorageError::Conflict and is_duplicate_entity are removed — use typed storage variants"
  fail=1
fi

if [[ "$fail" -ne 0 ]]; then
  exit 1
fi

echo "Quant pivot error checks passed."
