#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

errors=0
active_docs=(
  docs/operations/architecture-and-design.md
  docs/operations/runbook.md
  docs/plans/quant-pivot/06-config-deploy-and-ops.md
  docs/plans/quant-pivot/08-cold-start-production-closeout.md
  docs/plans/quant-pivot/10-frontend-refactor.md
  docs/plans/quant-pivot/phase-10/README.md
)

for path in "${active_docs[@]}"; do
  if [[ ! -f "${path}" ]]; then
    echo "ERROR: active contract document is missing: ${path}" >&2
    errors=$((errors + 1))
  fi
done

echo "=== Checking active docs do not restore deleted config/credential contracts ==="
legacy_contract='DeploySecret|SystemdCredentialRef|CREDENTIALS_DIRECTORY|credential reference|typed credential|runtime identity|migration identity|deploy identity|/api/runtime-config|/runtime-config'
if hits="$(rg -n -P "${legacy_contract}" "${active_docs[@]}" || true)" && [[ -n "${hits}" ]]; then
  echo "ERROR: an active document describes a deleted config or credential contract" >&2
  echo "${hits}" >&2
  errors=$((errors + 1))
fi

echo "=== Checking active docs do not claim unledgered completion ==="
if hits="$(rg -n -P '\|[^|]+\|\s*Verified\s*\|' "${active_docs[@]}" || true)" && [[ -n "${hits}" ]]; then
  echo "ERROR: Verified task status belongs only in the closure Execution Ledger" >&2
  echo "${hits}" >&2
  errors=$((errors + 1))
fi
false_visual_claim='(视觉回归|visual regression|视觉快照|visual snapshots?).*(通过|完成|全绿|passed|complete)'
if hits="$(rg -ni -P "${false_visual_claim}" "${active_docs[@]}" || true)" && [[ -n "${hits}" ]]; then
  echo "ERROR: active design docs must reference ledger evidence instead of claiming visual completion" >&2
  echo "${hits}" >&2
  errors=$((errors + 1))
fi

echo "=== Checking dead placeholder UI and snapshot designs stay deleted ==="
if hits="$(rg -n 'RoadmapPlaceholder|roadmap-placeholder' ui/apps/web-antdv-next/src || true)" && [[ -n "${hits}" ]]; then
  echo "ERROR: mock-only production placeholder UI reappeared" >&2
  echo "${hits}" >&2
  errors=$((errors + 1))
fi
if hits="$(find ui/apps/web-antdv-next/tests -type f -name '*-darwin.png' -print)" && [[ -n "${hits}" ]]; then
  echo "ERROR: Darwin-suffixed Playwright snapshots are not canonical CI evidence" >&2
  echo "${hits}" >&2
  errors=$((errors + 1))
fi
linux_snapshot_count="$({ find ui/apps/web-antdv-next/tests/e2e -type f -name '*-linux.png' -print || true; } | wc -l | tr -d ' ')"
if [[ "${linux_snapshot_count}" != "37" ]]; then
  echo "ERROR: expected 37 reviewed Linux Playwright baselines, found ${linux_snapshot_count}" >&2
  errors=$((errors + 1))
fi
ignored_linux_snapshots=""
while IFS= read -r snapshot; do
  relative_snapshot="${snapshot#ui/}"
  if git -C ui check-ignore -q "${relative_snapshot}"; then
    ignored_linux_snapshots+="${relative_snapshot}"$'\n'
  fi
done < <(find ui/apps/web-antdv-next/tests/e2e -type f -name '*-linux.png' -print)
if [[ -n "${ignored_linux_snapshots}" ]]; then
  echo "ERROR: Linux Playwright baselines must be tracked review assets, not ignored artifacts" >&2
  printf '%s' "${ignored_linux_snapshots}" >&2
  errors=$((errors + 1))
fi

echo "=== Checking Config acceptance scenarios are executable and CI-consumed ==="
config_spec='ui/apps/web-antdv-next/tests/e2e/config-governance.spec.ts'
for number in $(seq -w 1 22); do
  if ! rg -q -F "test('[CFG-${number}]" "${config_spec}"; then
    echo "ERROR: executable Config scenario CFG-${number} is missing" >&2
    errors=$((errors + 1))
  fi
done
for contract in X-01 X-02; do
  if ! rg -q -F "test('[${contract}]" "${config_spec}"; then
    echo "ERROR: executable cross-cutting scenario ${contract} is missing" >&2
    errors=$((errors + 1))
  fi
done
if ! rg -q -F 'data-screenshot-volatile="true"' \
  ui/apps/web-antdv-next/src/views/config/modules/report-schedule-preview.vue || \
  ! rg -q -F 'fireTimes.every(' "${config_spec}"; then
  echo "ERROR: schedule preview snapshots must mask timestamps and assert their real semantics" >&2
  errors=$((errors + 1))
fi
for spec in \
  'apps/web-antdv-next/tests/e2e/config-governance.spec.ts' \
  'apps/web-antdv-next/tests/e2e/phase-11-7-protected-flow.spec.ts'; do
  if ! rg -q -F "pnpm exec playwright test ${spec}" scripts/check-production-gates.sh; then
    echo "ERROR: protected-e2e gate does not execute ${spec}" >&2
    errors=$((errors + 1))
  fi
done

echo "=== Checking canonical docs state the current deploy boundaries ==="
if ! rg -q -F 'zeroizing、non-serializable、redacted-debug' \
  docs/plans/quant-pivot/phase-10/10.7-deploy-config-and-preferences.md; then
  echo "ERROR: Config console contract does not state the direct SecretText boundary" >&2
  errors=$((errors + 1))
fi
if ! rg -q -F '各自只有一组 `user + password`' \
  docs/plans/quant-pivot/06-config-deploy-and-ops.md \
  docs/plans/quant-pivot/phase-10/10.7-deploy-config-and-preferences.md; then
  echo "ERROR: canonical docs do not state the single PostgreSQL/ClickHouse identity contract" >&2
  errors=$((errors + 1))
fi

if (( errors > 0 )); then
  echo "${errors} active-document contract violation(s) found" >&2
  exit 1
fi

echo "Active-document contracts passed"
