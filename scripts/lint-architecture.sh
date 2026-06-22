#!/usr/bin/env bash
set -euo pipefail

# Architecture lint: enforce domain DTO boundaries
ERRORS=0

echo "=== Checking for entities/sea_orm imports in core/research ==="
if rg 'use (quant_pivot_models::entities|sea_orm)' crates/quant-pivot-{core,research}/src/ 2>/dev/null; then
    echo "ERROR: core/research must not import entities or sea_orm"
    ERRORS=$((ERRORS + 1))
fi

echo "=== Checking write paths don't accept RegistryInfo ==="
if rg 'fn persist_.*RegistryInfo|fn save_.*&.*Info[^r]' crates/quant-pivot-{core,research}/src/ 2>/dev/null; then
    echo "ERROR: Write paths must not accept *Info or *RegistryInfo"
    ERRORS=$((ERRORS + 1))
fi

echo "=== Checking for cross-crate re-exports ==="
if rg 'pub use quant_pivot_(models|error)::' crates/quant-pivot-{api,core,storage}/src/lib.rs crates/quant-pivot-{api,core,storage}/src/**/mod.rs 2>/dev/null; then
    echo "WARNING: Cross-crate re-exports found"
    ERRORS=$((ERRORS + 1))
fi

echo "=== Checking for from_decimal_unchecked in production ingest paths ==="
if rg 'from_decimal_unchecked' crates/quant-pivot-api/src/ws/ crates/quant-pivot-api/src/clob/ 2>/dev/null; then
    echo "ERROR: from_decimal_unchecked must not appear in WS/REST ingest paths"
    ERRORS=$((ERRORS + 1))
fi

echo "=== Checking DataPipeline try_send success path does not clone events ==="
if rg 'pipeline_event\.clone\(\)' crates/quant-pivot-core/src/pipeline/data_pipeline.rs 2>/dev/null; then
    echo "ERROR: data_pipeline try_send success path must not clone PipelineEvent"
    ERRORS=$((ERRORS + 1))
fi

if [ $ERRORS -eq 0 ]; then
    echo "All architecture checks passed!"
else
    echo "$ERRORS architecture violation(s) found"
    exit 1
fi
