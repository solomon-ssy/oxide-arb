#!/usr/bin/env bash
set -euo pipefail

# Phase 0 symbol gate — forbid Endgame / deleted-crate references from re-entering.

FORBIDDEN='EndgameDetector|ScoredOpportunity(?!Snapshot)|OpportunityPipeline|\bExecutionMode::(DryRun|Paper|Live)|oxide_arb_algorithm|oxide_arb_risk|oxide_arb_control'

if rg -n -P "$FORBIDDEN" crates/ config/ --glob '!**/lint-quant-pivot-boundary.sh' 2>/dev/null; then
  echo "ERROR: forbidden legacy Endgame symbols detected (see lint-quant-pivot-boundary.sh)"
  exit 1
fi

if rg -n 'quant_pivot_(algorithm|risk|control)::|use quant_pivot_(algorithm|risk|control)\b' crates/ config/ 2>/dev/null; then
  echo "ERROR: forbidden deleted-crate import detected"
  exit 1
fi

# Phase 11.7.2 breaking-contract gate. These semantics were deleted rather
# than deprecated; tests and UI fixtures are included so no shadow contract can
# silently keep them alive.
PHASE_1172_DELETED='max_selection_size|SelectionCapExceeded|FeeSource::CategoryDefault|allow_category_fallback|archive_manifest_set_hash|notional_tier|min_universe_coverage|\buniverse_coverage\b|\bfee_category\b|\bfee_rank\b|FeeCalculator|FeeQuote(Error)?|execution\.exit_monitor\.opportunistic_sell\.max_cumulative_exit_pct|X-API-Version|x-api-version'
if rg -n "$PHASE_1172_DELETED" crates/ config/ ui/packages/types/src ui/apps/web-antdv-next/src 2>/dev/null; then
  echo "ERROR: deleted Phase 11.7.2 compatibility/archive/selection/fee semantic detected"
  exit 1
fi

if rg -n 'fee_details: Option<ClobFeeDetails>|builder_(maker|taker)_fee_rate_bps: Option<u32>' \
  crates/quant-pivot-models/src/types/clob_market_info.rs \
  crates/quant-pivot-models/src/entities/clob_market_info_version.rs 2>/dev/null; then
  echo "ERROR: CLOB V2 fee facts are mandatory; zero/optional fee fallback is forbidden"
  exit 1
fi

# Active Phase 11.7 documents split stable contracts from one mutable completion
# ledger. Fail CI if superseded version, archive, sizing, canary, or drawer
# semantics drift back into that active chain.
PHASE_117_ACTIVE_DOCS=(
  docs/plans/quant-pivot/phase-11/README.md
  docs/plans/quant-pivot/phase-11/11.7-labeling-entry-exit-closed-loop.md
  docs/plans/quant-pivot/phase-11/11.7.1-composable-entry-event-triggers.md
  docs/plans/quant-pivot/phase-11/11.7.2-executable-l2-policy-validation.md
  docs/plans/quant-pivot/phase-11/11.7.2-closed-loop-completion-plan-2026-07-14.md
  docs/plans/quant-pivot/phase-11/11.9-attribution-feedback-and-auto-retraining.md
)
PHASE_117_STALE_DOC='Runtime v1[34]|runtime config \*\*v1[34]\*\*|runtime config v1[34]|v1[34] ?→ ?v1[45]|dataset/model artifact (v4|`format_version = 4`)|Policy v[345]|Trade Policy v4|Evidence( Bundle)? v2|TradePolicy artifact (只接受 )?v[34]|exact-(notional|tier)|notional tier|WORM 日分区 Parquet|archive seal|archive worker 待|archive worker 驱动|30 ?天 canary|窄屏 drawer'
if rg -n "$PHASE_117_STALE_DOC" "${PHASE_117_ACTIVE_DOCS[@]}" 2>/dev/null; then
  echo "ERROR: stale Phase 11.7 contract detected in an active design document"
  exit 1
fi

# Gamma may retain provider wire payloads for source audit, but normalized,
# persisted, registry, and Web DTOs must never reintroduce fee truth.
if rg -n '\b(fee_exponent|fee_rate_bps|maker_base_fee|taker_base_fee)\b' \
  crates/quant-pivot-api/src/gamma/{catalog.rs,mapper.rs} \
  crates/quant-pivot-models/src/{entities/market.rs,domain/api/market.rs,domain/market/registry.rs} \
  crates/quant-pivot-core/src/ingest/market_registry.rs 2>/dev/null; then
  echo "ERROR: Gamma-derived canonical fee field detected outside raw provider audit payload"
  exit 1
fi

if rg -n 'pub use .*::Opportunity;|pub use .*ScoredOpportunity;' \
  crates/**/lib.rs crates/**/mod.rs 2>/dev/null; then
  echo "ERROR: forbidden re-export of legacy symbols"
  exit 1
fi

# tokio-cron-scheduler is the report-plane scheduling backend and MUST stay
# behind the ReportScheduleRunner facade in core/src/infra/schedule/. Code paths
# (underscore form) must not appear outside that module; the crate dependency
# (hyphen form) is allowed only in quant-pivot-core's manifest.
if rg -n 'tokio_cron_scheduler' crates/ \
  --glob '!crates/quant-pivot-core/src/infra/schedule/**' 2>/dev/null; then
  echo "ERROR: tokio-cron-scheduler must stay behind core/src/infra/schedule/ (ReportScheduleRunner facade)"
  exit 1
fi
if rg -n 'tokio-cron-scheduler' crates/ \
  --glob '**/Cargo.toml' \
  --glob '!crates/quant-pivot-core/Cargo.toml' 2>/dev/null; then
  echo "ERROR: only quant-pivot-core may declare the tokio-cron-scheduler dependency"
  exit 1
fi

# Phase 10.7 config-UI regression gate — deleted concepts must not return.
# money_critical / x-money-critical / FieldSemantics::Money were collapsed into
# `widget` (rendering) + FieldSemantics::GovernanceCritical (danger confirmation).
if rg -n 'money_critical|x-money-critical|FieldSemantics::Money' crates/ config/ 2>/dev/null; then
  echo "ERROR: money_critical concept removed in 10.7 — use widget + FieldSemantics::GovernanceCritical"
  exit 1
fi

if [ -d ui ]; then
  # The runtime-config editor lives in the app (views/runtime-config/modules/editor),
  # never inside the generic Vben preferences package.
  if rg -n 'preferences/blocks/runtime-config|RuntimeConfigGovernedKey|RuntimeConfigRequestClientKey|RuntimeConfigRevisionKey' \
    ui/packages ui/apps 2>/dev/null; then
    echo "ERROR: runtime-config preferences block / injection keys removed in 10.7 — edit via /runtime-config page"
    exit 1
  fi
  # money_critical flag and the legacy { en, zh_cn } / { kind } UiText shapes are gone.
  if rg -n 'money_critical|moneyCritical' ui/packages ui/apps 2>/dev/null; then
    echo "ERROR: money_critical removed in 10.7 — use semantics === 'governance_critical'"
    exit 1
  fi
  if rg -n "kind: 'localized'|kind: 'simple'|kind === 'simple'" ui/packages ui/apps 2>/dev/null; then
    echo "ERROR: legacy UiText union removed in 10.7 — UiText is { locales: Record<string,string> }"
    exit 1
  fi
  if rg -n 'RuntimeConfigDocument\.risk|max_daily_loss' ui/packages/types 2>/dev/null; then
    echo "ERROR: legacy runtime-config risk hint removed in 10.7"
    exit 1
  fi
fi

echo "Quant pivot boundary checks passed."
