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
if rg 'pipeline_event\.clone\(\)' crates/quant-pivot-core/src/ingest/data_pipeline.rs 2>/dev/null; then
    echo "ERROR: data_pipeline try_send success path must not clone PipelineEvent"
    ERRORS=$((ERRORS + 1))
fi

echo "=== Checking for legacy Endgame / execution-mode symbols in production code ==="
if rg 'EndgameBook|EndgameDetector|ScoredOpportunity|OpportunityPipeline|ExecutionMode::(DryRun|Paper|Live)' crates/ --glob '!**/tests/**' 2>/dev/null; then
    echo "ERROR: Legacy Endgame / execution-mode symbols found in production code"
    ERRORS=$((ERRORS + 1))
fi

echo "=== Checking for retired 'Universe' / 'hotset' vocabulary ==="
# The report market set is 'selection'; the WS subscription set is 'subscription'.
if rg -i 'universe|hotset' crates/ --glob '*.rs' 2>/dev/null; then
    echo "ERROR: retired 'universe'/'hotset' vocabulary found (use 'selection' / 'subscription')"
    ERRORS=$((ERRORS + 1))
fi

echo "=== Checking for legacy Opportunity/Trade compatibility re-exports ==="
# Word boundaries keep legitimate Polymarket SDK types (e.g. ClobTrade) allowed
# while forbidding re-export shims for the deleted domain Opportunity/Trade types.
if rg 'pub use .*\b(Opportunity|Trade)\b' crates/ --glob '!**/tests/**' 2>/dev/null; then
    echo "ERROR: compatibility re-export of legacy Opportunity/Trade found"
    ERRORS=$((ERRORS + 1))
fi

echo "=== Checking for manual IntoActiveModel in domain DTOs ==="
if rg 'impl IntoActiveModel' crates/quant-pivot-models/src/domain/ 2>/dev/null; then
    echo "ERROR: domain DTOs must use DeriveIntoActiveModel"
    ERRORS=$((ERRORS + 1))
fi

echo "=== Checking postgres/quant monolith ==="
# The threshold tracks "one `mod` + one `pub use` line per repository", not a
# hard cap on the number of repositories — it grows as the schema grows.
# What it actually guards against is real logic creeping into this file: any
# `fn`/`struct`/`impl` here (as opposed to `mod`/`pub use` declarations) is
# the actual monolith signal.
if [ -f crates/quant-pivot-repository/src/postgres/quant/mod.rs ] && \
   grep -qE '^\s*(fn|struct|impl|enum|trait)\b' crates/quant-pivot-repository/src/postgres/quant/mod.rs; then
    echo "ERROR: postgres/quant/mod.rs must be a thin re-export module (found a definition, not just mod/pub use)"
    ERRORS=$((ERRORS + 1))
fi

if [ $ERRORS -eq 0 ]; then
    echo "All architecture checks passed!"
else
    echo "$ERRORS architecture violation(s) found"
    exit 1
fi
