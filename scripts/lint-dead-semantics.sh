#!/usr/bin/env bash
set -euo pipefail

# Dead-semantics gate (Phase 11.0) — enforce "zero dead semantics" on enum values.
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
DELETED='BindingConstraint::ManualCap|CapitalAllocationState::Planned|ChCapitalAllocationState::Planned|EmptyReportReason::ModelQualityGateFailed|EmptyReportReason::RuntimeModeDisabled|EmptyReason\b'
if rg -n -P "$DELETED" "${SRC_GLOBS[@]}" --glob '!**/tests/**' 2>/dev/null; then
    echo "ERROR: a symbol deleted in Phase 11.0 reappeared in production src (see lint-dead-semantics.sh)"
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
