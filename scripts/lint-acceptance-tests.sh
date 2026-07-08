#!/usr/bin/env bash
set -euo pipefail

# Phase 11.2.2 remediation R11 — acceptance-test-existence gate.
#
# `docs/plans/quant-pivot/phase-11/11.2.2-crypto-external-vertical.md` §9
# names every acceptance test the crypto external vertical must pass. A test
# function silently renamed, deleted, or marked `#[ignore]` must never let
# this class of gap recur — this script enforces that every §9-named test
# genuinely exists, is discovered by `cargo test`, and is not skipped.

DESIGN_DOC="docs/plans/quant-pivot/phase-11/11.2.2-crypto-external-vertical.md"
TEST_FILE="crates/quant-pivot-research/tests/domain_vertical_acceptance.rs"
ERRORS=0

if [ ! -f "$DESIGN_DOC" ]; then
    echo "ERROR: design doc missing: $DESIGN_DOC"
    exit 1
fi
if [ ! -f "$TEST_FILE" ]; then
    echo "ERROR: acceptance test file missing: $TEST_FILE"
    exit 1
fi

echo "=== Extracting §9 acceptance test names from $DESIGN_DOC ==="
# §9 lines are markdown bullets of the form `- \`test_name\`(optional Chinese
# annotation)`. Extract the backtick-quoted identifier from every such line
# between the "## 9. 验收测试" heading and the next "## " heading.
NAMES=()
while IFS= read -r name; do
    NAMES+=("$name")
done < <(
    awk '/^## 9\. 验收测试/{flag=1; next} /^## /{flag=0} flag' "$DESIGN_DOC" \
        | rg -o '^- `([a-z_0-9]+)`' -r '$1' \
        | sort -u
)

if [ "${#NAMES[@]}" -eq 0 ]; then
    echo "ERROR: no acceptance test names extracted from $DESIGN_DOC §9 — parser or doc drifted"
    exit 1
fi

echo "Found ${#NAMES[@]} named acceptance test(s) in §9."

echo "=== Verifying every §9 test exists, is a real test, and is not ignored ==="
for name in "${NAMES[@]}"; do
    # Must exist as a function definition somewhere in the acceptance suite.
    if ! rg -q "fn ${name}\\b" "$TEST_FILE"; then
        echo "ERROR: §9 test '$name' has no matching function in $TEST_FILE"
        ERRORS=$((ERRORS + 1))
        continue
    fi

    # Must be attributed as a test (#[test] or #[tokio::test]) within a few
    # lines above its definition — a plain helper fn of the same name would
    # otherwise satisfy the existence check without ever running.
    context=$(rg -B6 "fn ${name}\\b" "$TEST_FILE")
    if ! rg -q '#\[(tokio::)?test\]' <<<"$context"; then
        echo "ERROR: §9 test '$name' exists but is not attributed #[test] / #[tokio::test]"
        ERRORS=$((ERRORS + 1))
        continue
    fi

    # Must not be #[ignore]'d — a skipped test is not an executed acceptance
    # guarantee.
    if rg -q '#\[ignore' <<<"$context"; then
        echo "ERROR: §9 test '$name' is marked #[ignore] — it must actually run"
        ERRORS=$((ERRORS + 1))
    fi
done

if [ $ERRORS -eq 0 ]; then
    echo "All ${#NAMES[@]} §9 acceptance tests exist and run."
else
    echo "$ERRORS acceptance-test-coverage violation(s) found"
    exit 1
fi
