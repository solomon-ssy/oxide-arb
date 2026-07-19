#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
plan_root="${repo_root}/docs/plans/quant-pivot"
errors=0

required_keys=(
  lifecycle_assumption
  schema_data_version_impact
  pre_production_behavior
  production_frozen_behavior
  rollback_and_data_verification
)

while IFS= read -r file; do
  marker_count="$(rg -c 'quant-pivot-lifecycle-contract:v1' "${file}" || true)"
  if [[ "${marker_count}" != "1" ]]; then
    echo "ERROR: ${file#"${repo_root}/"} must contain exactly one lifecycle contract marker" >&2
    errors=$((errors + 1))
  fi
  for key in "${required_keys[@]}"; do
    if ! rg -q "${key}" "${file}"; then
      echo "ERROR: ${file#"${repo_root}/"} is missing lifecycle field ${key}" >&2
      errors=$((errors + 1))
    fi
  done
done < <(find "${plan_root}" -type f -name '*.md' | LC_ALL=C sort)

if legacy_directives="$(rg -n --glob '*.md' \
  '目标版本.*v[0-9]|RUNTIME_CONFIG_SCHEMA_VERSION.*(→|=[[:space:]]*[2-9])|schema_version.*必须为[[:space:]]*[2-9]|当前版本组合固定为.*v[0-9]|当前权威.*Runtime v[0-9]|破坏式 bump.*v[0-9]|单调 \+1 bump|schema_version[:=][[:space:]]*[2-9]' \
  "${plan_root}" || true)" && [[ -n "${legacy_directives}" ]]; then
  echo "ERROR: executable legacy version directives remain after the boot rebaseline:" >&2
  echo "${legacy_directives}" >&2
  errors=$((errors + 1))
fi

if ! rg -q '^state = "pre_production_resettable"$' "${repo_root}/project-lifecycle.toml" ||
   ! rg -q '^baseline = "boot"$' "${repo_root}/project-lifecycle.toml"; then
  echo "ERROR: project-lifecycle.toml must declare the pre-production boot baseline" >&2
  errors=$((errors + 1))
fi

if (( errors > 0 )); then
  echo "${errors} phase lifecycle violation(s) found" >&2
  exit 1
fi

echo "Phase lifecycle contracts passed"
