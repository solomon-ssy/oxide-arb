#!/usr/bin/env bash
set -euo pipefail

# Training-serving parity gate (Phase 11.6).
#
# This is deliberately a narrow symbol/idiom gate, not a ban on numeric zero.
# Legitimate zero values (empty counts, mathematical identities, test data) stay
# valid. The checks below cover only the named production escape hatches that
# previously fabricated evidence or let train/serve preprocessing diverge.

ERRORS=0

check_forbidden() {
    local title=$1
    local message=$2
    local pattern=$3
    shift 3

    echo "=== ${title} ==="
    if rg -n -P "$pattern" "$@" \
        --glob '!**/tests/**' \
        --glob '!**/fixtures/**' \
        2>/dev/null; then
        echo "ERROR: ${message}"
        ERRORS=$((ERRORS + 1))
    fi
}

# Inline unit-test modules share a source file with production validation code.
# This helper scans only the prefix before the first `#[cfg(test)]`, keeping the
# gate precise without exempting the production half of those files.
check_production_prefix_forbidden() {
    local title=$1
    local message=$2
    local pattern=$3
    shift 3
    local found=0

    echo "=== ${title} ==="
    for file in "$@"; do
        local matches
        matches=$(awk '/^[[:space:]]*#\[cfg\(test\)\]/{exit} {print}' "$file" \
            | rg -n -P "$pattern" - 2>/dev/null || true)
        if [ -n "$matches" ]; then
            echo "${file}:"
            echo "$matches"
            found=1
        fi
    done
    if [ "$found" -ne 0 ]; then
        echo "ERROR: ${message}"
        ERRORS=$((ERRORS + 1))
    fi
}

# Discover every PIT-source implementation, including integration-test fakes,
# then inspect only those files plus the canonical trait. Repository APIs such
# such as CatalogLedgerRepository::market_at are intentionally outside this set.
check_retired_pit_methods() {
    local title=$1
    local message=$2
    local found=0
    local files

    echo "=== ${title} ==="
    files="crates/quant-pivot-research/src/pit/mod.rs
$(rg -l '\bPointInTimeSnapshotSource for\b' crates --glob '*.rs' 2>/dev/null || true)"
    for file in $files; do
        local matches
        matches=$(rg -n -P \
            '^[[:space:]]*async[[:space:]]+fn[[:space:]]+(?:book_at|market_at|market_at_from_info)[[:space:]]*\(' \
            "$file" 2>/dev/null || true)
        if [ -n "$matches" ]; then
            echo "${file}:"
            echo "$matches"
            found=1
        fi
    done
    if [ "$found" -ne 0 ]; then
        echo "ERROR: ${message}"
        ERRORS=$((ERRORS + 1))
    fi
}

check_forbidden \
    "Checking for duplicate feature/PIT contracts" \
    "extend FeatureSpec/FeatureSchema and PointInTimeSnapshotSource; do not create a parallel contract" \
    '\b(?:FeatureContract|HistoricalRegistryResolver)\b' \
    crates/*/src

check_forbidden \
    "Checking for retired global feature requiredness" \
    "FeatureSpec has no global critical flag and runtime config cannot override model input requiredness" \
    '\b(?:CriticalFeatureCoverage|min_critical_feature_coverage|critical_feature_coverage)\b|features\.required_features' \
    crates/quant-pivot-core/src \
    crates/quant-pivot-models/src \
    crates/quant-pivot-research/src \
    config

check_forbidden \
    "Checking for retired dual PIT interfaces" \
    "serving and replay must use PointInTimeSnapshotSource; live/current-state PIT adapters are forbidden" \
    '\b(?:PitView|PointInTimeDataSource|LiveBookDataSource|TradeTapePitParams)\b' \
    crates/*/src

check_retired_pit_methods \
    "Checking for retired as-of PIT methods" \
    "PointInTimeSnapshotSource implementations must accept DecisionBoundary; book_at/market_at/market_at_from_info cannot return"

check_forbidden \
    "Checking for retired parallel decision-capture schema" \
    "DecisionCaptureEvidence on quant_feature_vector is the sole decision capture; the unwired book_decision_context design must not return" \
    '\b(?:BookDecisionContextRow|BookDecisionContextWriter|ChBookDecisionStage|ChBookEvidenceTier|ChBookQuality)\b|\bbook_decision_contexts\b' \
    crates/*/src \
    crates/quant-pivot-storage/src/clickhouse/sql

check_forbidden \
    "Checking for retired dataset replacement paths" \
    "frozen dataset bytes are the only train/backtest input; rematerialization must be an independent parity job" \
    '\b(?:rematerialize_training_examples|rematerialize_exit_decision_examples|DatasetReplayService|ModelRuntimeWarning)\b' \
    crates/*/src

check_forbidden \
    "Checking for retired dataset lifecycle/manual promotion" \
    "dataset integrity transitions directly to Ready; Built and manual promotion must not return" \
    '\b(?:promote_dataset_ready|DatasetStatus::Built|TrainingDatasetStatus::Built)\b' \
    crates/*/src

check_forbidden \
    "Checking for rollback gate bypasses" \
    "rollback must use the atomic subject/latch/parity/gate-bound commit; a generic retired-to-published restore is forbidden" \
    '\brestore_model_version\b' \
    crates/*/src

check_forbidden \
    "Checking for mutable serving normalization" \
    "HistoricalQuantile and runtime recent-value normalization are retired; use FrozenReferenceQuantile from the artifact" \
    '\bHistoricalQuantile\b|\.recent_values\s*\(' \
    crates/quant-pivot-research/src/model \
    crates/quant-pivot-research/src/factors \
    crates/quant-pivot-core/src/service/model_runner.rs \
    crates/quant-pivot-core/src/service/factor_pipeline.rs

check_production_prefix_forbidden \
    "Checking canonical business-prediction hashing" \
    "online, shadow, and replay must share canonical_business_prediction_hash; raw candidates include non-deterministic execution ids" \
    'ResearchHasher::canonical\(&(?:[A-Za-z_][A-Za-z0-9_]*\.)?candidates\)' \
    crates/quant-pivot-core/src/service/model_runner.rs \
    crates/quant-pivot-core/src/service/durable_feature_parity.rs

check_forbidden \
    "Checking for fabricated feature evidence" \
    "stub/empty/default market evidence is forbidden; return typed Resolution/FeatureCell state" \
    '\b(?:stub_market_context|empty_book|resolve_registry)\s*\(' \
    crates/quant-pivot-research/src/features

check_production_prefix_forbidden \
    "Checking critical upstream evidence is not defaulted" \
    "venue positions and Gamma settlement clocks/outcomes are decision inputs; missing fields must fail closed, never become zero/false/now" \
    '#\[serde\(default(?:,[^]]*)?\)\]|\bfn\s+yes_outcome_won\b|resolved_at\.unwrap_or\(updated\)|missing_outcome_defaults_to_false' \
    crates/quant-pivot-api/src/data_api.rs \
    crates/quant-pivot-api/src/gamma/mapper.rs \
    crates/quant-pivot-core/src/service/gamma.rs

check_forbidden \
    "Checking for fabricated domain applicability" \
    "a missing domain-resolution row is Unknown/Missing, never silently NotMapped" \
    'unwrap_or(?:_else)?\([^;\n]*DomainAvailability::NotMapped' \
    crates/quant-pivot-core/src/pit \
    crates/quant-pivot-core/src/prefetch \
    crates/quant-pivot-core/src/service/pit_selection.rs \
    crates/quant-pivot-research/src/pit \
    crates/quant-pivot-research/src/features

check_production_prefix_forbidden \
    "Checking domain-ingest cursor integrity" \
    "persisted cursor status/time must be validated; an unknown status or failed initial read cannot invent a bootstrap checkpoint" \
    'DomainCursorStatus::parse\([^;\n]*\)\.unwrap_or|\.find\([^;\n]*\)\s*\.await\s*\.ok\(\)\s*\.flatten\(\)|map_or_else\(Utc::now,[^;\n]*last_event_time' \
    crates/quant-pivot-core/src/service/domain_ingest.rs

check_forbidden \
    "Checking for the retired classical missing-value protocol" \
    "fill_missing and the .__available synthetic-name protocol are removed; use FittedInputTransform" \
    '\bfill_missing\b|\bAVAILABILITY_SUFFIX\b|\.__available\b' \
    crates/quant-pivot-research/src/training \
    crates/quant-pivot-research/src/features \
    crates/quant-pivot-research/src/model \
    crates/quant-pivot-core/src/projection/inference_batch.rs

check_forbidden \
    "Checking model-artifact input-contract ownership" \
    "every model family must freeze typed ModelInputContract; required features are derived from requiredness" \
    '\bpub\s+required_features\s*:\s*Vec<FeatureName>|\bfn\s+contract_hash\s*\(|input_contract_hash\s*:\s*[^,\n]*factor_schema_hash' \
    crates/quant-pivot-research/src/model/artifact.rs \
    crates/quant-pivot-research/src/model/classical.rs \
    crates/quant-pivot-research/src/model/trainer.rs \
    crates/quant-pivot-research/src/model/sell_scorer/trainer.rs

# A DecisionBoundary implementation is the sole owner of decision-time minus
# knowledge-lag arithmetic. The path exclusion is intentional: downstream code
# may consume cutoffs but must never derive them again.
check_forbidden \
    "Checking for downstream delay subtraction" \
    "derive knowledge/source cutoffs only in decision_boundary.rs; downstream subtraction can double-apply lag" \
    'let\s+\w+\s*=\s*(?:\w+\.)?(?:as_of|trigger_time|decision_at)\s*-\s*(?:checked_knowledge_lag\s*\([^;]*\)|knowledge_lag\b|knowledge_lag\b)|\bchecked_knowledge_lag\s*\(' \
    crates/quant-pivot-core/src/report \
    crates/quant-pivot-core/src/service \
    crates/quant-pivot-research/src/domain \
    crates/quant-pivot-research/src/features \
    crates/quant-pivot-research/src/training \
    --glob '!**/decision_boundary.rs'

check_forbidden \
    "Checking for hidden downstream lag derivation" \
    "downstream PIT code may subtract lookback from a resolved cutoff, but must never subtract knowledge/source lag from decision time" \
    '^(?!\s*//).*\b(?:as_of|decision_at|trigger_time)\s*\.checked_sub_signed\s*\([^;\n]*(?:knowledge_lag|source_delay)' \
    crates/quant-pivot-core/src/pit \
    crates/quant-pivot-core/src/prefetch \
    crates/quant-pivot-core/src/report \
    crates/quant-pivot-core/src/service \
    crates/quant-pivot-research/src/domain \
    crates/quant-pivot-research/src/features \
    crates/quant-pivot-research/src/pit \
    crates/quant-pivot-research/src/training \
    --glob '!**/decision_boundary.rs' \
    --glob '!**/acceptance.rs'

check_forbidden \
    "Checking for retired source-delay vocabulary" \
    "runtime/research contracts use knowledge_lag_secs and per-source availability_lag; source_delay must remain migration-only" \
    '\bsource_delay(?:_secs)?\b' \
    crates/quant-pivot-core/src \
    crates/quant-pivot-research/src \
    crates/quant-pivot-models/src/domain \
    crates/quant-pivot-models/src/runtime_config

# Conversion failure is not zero/one. Restrict this check to PIT, feature and
# classical-transform boundaries where fallback changes model semantics.
check_forbidden \
    "Checking for silent numeric/time conversion defaults" \
    "PIT/feature/model-input conversion errors must be typed failures, never fallback 0/1" \
    'to_(?:f64|u64|i64|usize)\(\)\.unwrap_or\(\s*(?:0(?:\.0)?|1(?:\.0)?)\s*\)|(?:means|stds)\.get\([^)]*\)[^;\n]*unwrap_or\(\s*(?:0(?:\.0)?|1(?:\.0)?)\s*\)|u64::try_from\([^;\n]*\)\.unwrap_or\(\s*0\s*\)|u32::try_from\([^;\n]*\)\.unwrap_or\(\s*0\s*\)|ChronoDuration::from_std\([^)]*\)\.unwrap_or_else\([^;\n]*ChronoDuration::zero\(\)' \
    crates/quant-pivot-research/src/features \
    crates/quant-pivot-research/src/model/classical.rs \
    crates/quant-pivot-research/src/model/classical_runtime.rs \
    crates/quant-pivot-research/src/model/weighted \
    crates/quant-pivot-research/src/pit \
    crates/quant-pivot-research/src/selection \
    crates/quant-pivot-core/src/pit \
    crates/quant-pivot-core/src/prefetch/feature_window.rs \
    crates/quant-pivot-core/src/prefetch/historical_window.rs \
    crates/quant-pivot-core/src/projection/inference_batch.rs \
    --glob '!**/acceptance.rs'

check_forbidden \
    "Checking for silent saturation at semantic boundaries" \
    "PIT/feature/factor/training/model conversions must reject overflow; MAX/0/1 saturation changes business semantics" \
    '(?:(?:i64|u64|u32|usize)::try_from\([^;\n]*\)|to_(?:f64|u64|i64|usize)\(\))\.unwrap_or\(\s*(?:i64::MAX|u64::MAX|u32::MAX|usize::MAX|0(?:\.0)?|1(?:\.0)?)\s*\)|Decimal::from_f64\([^;\n]*\)\.unwrap_or(?:_else)?\([^;\n]*(?:Decimal::ZERO|Decimal::ONE)' \
    crates/quant-pivot-core/src/pit \
    crates/quant-pivot-core/src/prefetch \
    crates/quant-pivot-core/src/projection/inference_batch.rs \
    crates/quant-pivot-core/src/report \
    crates/quant-pivot-core/src/service/feature_pipeline.rs \
    crates/quant-pivot-core/src/service/model_calibration_fit.rs \
    crates/quant-pivot-core/src/service/model_training.rs \
    crates/quant-pivot-core/src/service/training_dataset.rs \
    crates/quant-pivot-core/src/governance/model_governance.rs \
    crates/quant-pivot-research/src/factors/computer.rs \
    crates/quant-pivot-research/src/factors/structural.rs \
    crates/quant-pivot-research/src/features \
    crates/quant-pivot-research/src/model/calibrator \
    crates/quant-pivot-research/src/model/classical.rs \
    crates/quant-pivot-research/src/model/classical_runtime.rs \
    crates/quant-pivot-research/src/model/sell_scorer \
    crates/quant-pivot-research/src/pit \
    crates/quant-pivot-research/src/selection \
    crates/quant-pivot-research/src/training \
    --glob '!**/acceptance.rs'

VALIDATION_NUMERIC_BOUNDARIES=(
    crates/quant-pivot-research/src/validation/dsr.rs
    crates/quant-pivot-research/src/validation/pbo.rs
    crates/quant-pivot-research/src/validation/cpcv.rs
    crates/quant-pivot-research/src/validation/purge.rs
    crates/quant-pivot-research/src/validation/trials.rs
    crates/quant-pivot-core/src/service/cpcv_backtest.rs
)

check_production_prefix_forbidden \
    "Checking validation/CPCV numeric fallback defaults" \
    "training validation conversions must return typed errors; 0/1/MAX/MIN are not conversion results" \
    'unwrap_or(?:_else)?\([^)]*(?:[uf](?:32|64)::(?:MAX|MIN)|i(?:32|64)::(?:MAX|MIN)|usize::(?:MAX|MIN)|0(?:\.0)?|1(?:\.0)?|Decimal::(?:ZERO|ONE))|map_or\(\s*(?:f64::MAX|u(?:32|64)::MAX|usize::MAX)' \
    "${VALIDATION_NUMERIC_BOUNDARIES[@]}"

check_production_prefix_forbidden \
    "Checking validation/CPCV lossy casts and saturation" \
    "validation counts, indices and durations must use checked/try conversions, never lossy casts or saturation" \
    '\bsaturating_(?:add|sub|mul)\s*\(|(?:\)|\]|\b(?:n_groups|k_test|block_count|expanded|trial_count|path_index|group_index|period_count|count|n))\s+as\s+(?:u32|u64|i32|i64|usize|f64)\b' \
    "${VALIDATION_NUMERIC_BOUNDARIES[@]}"

check_forbidden \
    "Checking portfolio-cap fail-closed projection" \
    "PortfolioConfig decimal parsing must use PortfolioCaps::try_from and propagate typed errors" \
    'impl\s+From<&PortfolioConfig>\s+for\s+PortfolioCaps|PortfolioCaps::from\s*\(|parse::<Decimal>\(\)\.unwrap_or' \
    crates/quant-pivot-research/src/backtest/mod.rs \
    crates/quant-pivot-core/src/service/backtest.rs \
    crates/quant-pivot-core/src/service/cpcv_backtest.rs

CHECKED_NUMERIC_BOUNDARIES=(
    crates/quant-pivot-research/src/backtest/metrics.rs
    crates/quant-pivot-research/src/factors/normalize/stats.rs
    crates/quant-pivot-research/src/model/objective.rs
    crates/quant-pivot-research/src/model/reliability.rs
    crates/quant-pivot-research/src/portfolio/correlation.rs
    crates/quant-pivot-research/src/portfolio/lp.rs
    crates/quant-pivot-core/src/service/bias_table_fit.rs
)

check_production_prefix_forbidden \
    "Checking checked statistical/portfolio numeric boundaries" \
    "factor, calibration, ranking, backtest and portfolio conversions must fail closed instead of fabricating 0/1/MAX" \
    '(?:to_(?:f64|u64|i64|usize)\(\)|Decimal::from_f64(?:_retain)?\([^;\n]*\)|parse::<Decimal>\(\)|(?:i64|u64|u32|usize)::try_from\([^;\n]*\))\.unwrap_or(?:_else)?\([^;\n]*(?:Decimal::(?:ZERO|ONE)|0(?:\.0)?|0\.5|1(?:\.0)?|[ui](?:32|64)::MAX|usize::MAX)|map_or\(\s*(?:f64::MAX|[ui](?:32|64)::MAX|usize::MAX)' \
    "${CHECKED_NUMERIC_BOUNDARIES[@]}"

check_production_prefix_forbidden \
    "Checking bias-fit PIT lag binding" \
    "bias-table fitting must consume the frozen runtime PIT knowledge lag; a hardcoded zero creates look-ahead skew" \
    'DecisionClock::new\(\s*0\s*\)|knowledge_lag:\s*(?:StdDuration|Duration)::ZERO' \
    crates/quant-pivot-core/src/service/bias_table_fit.rs

check_forbidden \
    "Checking for factor missing-to-zero coercion" \
    "missing feature cells must keep their state/reason; factor reads may not coerce absence to Decimal zero" \
    'read\([^;\n]*\)\.unwrap_or\(\s*Decimal::ZERO\s*\)' \
    crates/quant-pivot-research/src/factors

check_production_prefix_forbidden \
    "Checking deterministic Sell position-state identity" \
    "position_state is a dedicated frozen input; do not duplicate it as a random governed FactorValue revision" \
    '\bposition_state_factor_values\b|FactorDefinitionId::from_v7\(\)' \
    crates/quant-pivot-research/src/model/sell_scorer/position_state.rs

check_forbidden \
    "Checking for report monetary parse fallbacks" \
    "invalid report budget, constraint, or entry-depth decimals must fail closed, never become zero" \
    'parse::<Decimal>\(\)\.unwrap_or(?:_else)?\([^;\n]*Decimal::ZERO|\bparse_decimal_lossless\b' \
    crates/quant-pivot-core/src/report

check_forbidden \
    "Checking for timestamp/duration fallback freshness" \
    "invalid PIT timestamps and durations must return typed errors, never become the cutoff, epoch, or zero duration" \
    'unwrap_or\(\s*default(?:_time)?\s*\)|unwrap_or_else\(\s*(?:epoch|Utc::now)\s*\)|ChronoDuration::zero\(\)|saturating_sub\([^;\n]*timestamp' \
    crates/quant-pivot-research/src/pit \
    crates/quant-pivot-research/src/features \
    crates/quant-pivot-core/src/pit \
    crates/quant-pivot-core/src/prefetch

check_forbidden \
    "Checking category model failure semantics" \
    "category load/scope/inference failure must fail the report; generic fallback is forbidden" \
    'falls? back to generic|fallback to the generic|fallback_to_generic' \
    crates/quant-pivot-core/src/service/model_runner.rs \
    crates/quant-pivot-core/src/governance/category_pointer_guard.rs \
    crates/quant-pivot-models/src/runtime_config/sections/config.rs

check_forbidden \
    "Checking category pointer scope parity" \
    "config activation and serving must both require an exact category_scope; unscoped artifacts are generic-only" \
    'category_scope\.is_none_or|generic_unscoped_artifact_is_accepted' \
    crates/quant-pivot-core/src/governance/category_pointer_guard.rs

check_forbidden \
    "Checking for a calibration publish escape hatch" \
    "uncalibrated buy/classical models are shadow-only; the publish gate cannot be disabled by config" \
    '\brequire_for_publish\b|\bcalibration_gate_enabled\b' \
    crates/quant-pivot-core/src \
    crates/quant-pivot-models/src \
    crates/quant-pivot-research/src \
    config

check_forbidden \
    "Checking for synthetic model-run identity" \
    "serving evidence must bind the persisted model_run_id created for the real inference round" \
    '\b(?:synthetic_model_run_id|synthetic_run_id|placeholder_model_run_id)\b' \
    crates/quant-pivot-core/src \
    crates/quant-pivot-research/src

check_forbidden \
    "Checking serving evidence durability" \
    "feature/model-input serving evidence must await a durable sink; telemetry AsyncWriter buffering is forbidden" \
    'Arc\s*<\s*AsyncWriter|AsyncWriter\s*<' \
    crates/quant-pivot-core/src/observability/feature_fact_writer.rs \
    crates/quant-pivot-core/src/observability/model_input_fact_writer.rs

if [ "$ERRORS" -eq 0 ]; then
    echo "Training-serving parity checks passed."
else
    echo "$ERRORS training-serving parity violation group(s) found"
    exit 1
fi
