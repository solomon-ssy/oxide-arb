#!/usr/bin/env bash
set -euo pipefail

# Dead-semantics gate (Phase 11) — enforce live/pending/deleted semantics.
#
# Two checks:
#   A. Regression guard — symbols deleted in Phase 11.0 must never re-enter
#      production code (like scripts/lint-quant-pivot-boundary.sh).
#   B. Allowlist honesty — every registered-pending variant in
#      docs/plans/quant-pivot/phase-11/dead-semantics-allowlist.txt must (1) carry
#      a phase-11.x owner and (2) still be un-emitted in production src. Once a
#      variant is constructed, it is no longer dead and MUST be removed from the
#      allowlist.

ALLOWLIST="docs/plans/quant-pivot/phase-11/dead-semantics-allowlist.txt"
SRC_GLOBS=(crates/*/src)
ERRORS=0

echo "=== Checking deleted dead-semantic symbols do not reappear ==="
DELETED='BindingConstraint::ManualCap|CapitalAllocationState::Planned|ChCapitalAllocationState::Planned|EmptyReportReason::ModelQualityGateFailed|EmptyReportReason::RuntimeModeDisabled|EmptyReason\b|SizingBetStructure|HeuristicTpSl|EntryConditionPlanKind|PartialExitNode|SignalInvalidationRule|ExecutedPartialExitNodes|OpportunisticExitState|ExitTriggerKind|override_shares|override_limit_price|max_allowed_usd|target_reward_multiple|order_retry_policy|set_order_retry|\bmeta_label\b|MetaLabel'
if rg -n -P "$DELETED" "${SRC_GLOBS[@]}" --glob '!**/tests/**' 2>/dev/null; then
    echo "ERROR: a symbol deleted in Phase 11.0 reappeared in production src (see lint-dead-semantics.sh)"
    ERRORS=$((ERRORS + 1))
fi

echo "=== Checking Phase 11.7 live semantics have production producers and consumers ==="
require_live() {
    local symbol="$1"
    local producer_pattern="$2"
    local consumer_pattern="$3"
    if ! rg -q -P "$producer_pattern" "${SRC_GLOBS[@]}" --glob '!**/tests/**'; then
        echo "ERROR: live semantic '$symbol' has no production producer"
        ERRORS=$((ERRORS + 1))
    fi
    if ! rg -q -P "$consumer_pattern" "${SRC_GLOBS[@]}" --glob '!**/tests/**'; then
        echo "ERROR: live semantic '$symbol' has no production consumer"
        ERRORS=$((ERRORS + 1))
    fi
}

require_live 'EntryConditionState::Expired' \
    'EntryConditionState::Expired' \
    'entry_condition_state'
require_live 'FAK' \
    'OrderType::Fak' \
    'OrderType::Fak|OrderTypeKind::Fak'
require_live 'policy_net_positive' \
    'PolicySimulationLabelKind::NetPositive' \
    'policy_net_positive'
require_live 'RecommendationTradePlan' \
    'RecommendationTradePlan::Unavailable' \
    'RecommendationTradePlan::Frozen'

echo "=== Checking deleted Phase 11.7 wire fields do not remain in the active UI ==="
if rg -n -P 'recommendation\.(entry_plan|sizing_plan|exit_plan|risk_envelope)|record\.sizing_plan|override_shares|override_limit_price|max_allowed_usd|\bmeta_label\b' \
    ui/packages/types/src ui/apps/web-antdv-next/src --glob '!**/locales/**' 2>/dev/null; then
    echo "ERROR: a deleted Phase 11.7 UI/wire semantic reappeared"
    ERRORS=$((ERRORS + 1))
fi

echo "=== Auditing registered-pending dead-semantics allowlist ==="
if [ ! -f "$ALLOWLIST" ]; then
    echo "ERROR: allowlist file missing: $ALLOWLIST"
    exit 1
fi

while IFS= read -r line; do
    # Skip comments and blank lines.
    [[ -z "${line// }" ]] && continue
    [[ "$line" == \#* ]] && continue

    symbol=$(awk '{print $1}' <<<"$line")
    owner=$(awk '{print $2}' <<<"$line")

    if [[ ! "$owner" =~ ^phase-11\.[0-9]+$ ]]; then
        echo "ERROR: allowlist entry '$symbol' is missing a phase-11.x owner (got '$owner')"
        ERRORS=$((ERRORS + 1))
        continue
    fi

    # The entry must still be un-emitted. A fully-qualified Type::Variant appears
    # only at construction / match sites, never at the enum definition, so any hit
    # in production src means it is now live and must leave the allowlist.
    if rg -n --fixed-strings "$symbol" "${SRC_GLOBS[@]}" --glob '!**/tests/**' 2>/dev/null; then
        echo "ERROR: allowlisted symbol '$symbol' is now emitted in production src — remove it from $ALLOWLIST ($owner delivered it)"
        ERRORS=$((ERRORS + 1))
    fi
done <"$ALLOWLIST"

if [ $ERRORS -eq 0 ]; then
    echo "Dead-semantics checks passed."
else
    echo "$ERRORS dead-semantics violation(s) found"
    exit 1
fi
