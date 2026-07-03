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
