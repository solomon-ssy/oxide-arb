#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_path="${repo_root}/docs/plans/quant-pivot/inventory/code-constant-inventory.tsv"
check_only=false
if [[ "${1:-}" == "--check" ]]; then
  check_only=true
elif [[ -n "${1:-}" ]]; then
  echo "usage: $0 [--check]" >&2
  exit 2
fi

inventory_tmp="$(mktemp "${TMPDIR:-/tmp}/quant-pivot-constant-inventory.XXXXXX")"
trap 'rm -f "${inventory_tmp}"' EXIT

printf '%s\n' $'location\tsymbol\tcurrent_owner\tdisposition\tnew_owner\tconsumer\treason' \
  >"${inventory_tmp}"

classify() {
  local file="$1"
  local symbol="$2"
  local disposition new_owner consumer reason

  if [[ "${symbol}" == "MAX_STALE_RATIO_BPS" ]]; then
    disposition="delete"
    new_owner="RecommendationPolicy.data_quality.max_stale_ratio_bps"
    consumer="data-quality gate"
    reason="Duplicate decision threshold; the governed policy is the only source of truth"
  elif [[ "${symbol}" =~ (CHAIN_ID|EIP712|EIP_712|POLYMARKET_PROTOCOL|CLOB_SIGNATURE|EXTERNAL_PROTOCOL) ]]; then
    disposition="keep_external_protocol"
    new_owner="typed provider protocol catalog"
    consumer="external protocol adapter"
    reason="Externally assigned protocol values cannot follow the project lifecycle baseline"
  elif [[ "${symbol}" =~ (SCHEMA_VERSION|FORMAT_VERSION|MANIFEST_VERSION|EVALUATOR_VERSION|CONTRACT_VERSION|CATALOG_VERSION|METHODOLOGY_VERSION) ]]; then
    disposition="reset_boot_1"
    new_owner="owning typed contract module"
    consumer="serializer, validator and lineage reader"
    reason="System-owned contract version is reset to the first boot baseline because no production lineage exists"
  elif [[ "${symbol}" =~ (_ENV|_ENVIRONMENT|_HEADER|_CONTENT_TYPE|_MIME|_USER_AGENT|_TABLE|_COLUMN|_INDEX|_KEY_PREFIX|_NAMESPACE|_DOMAIN|_TAG|_LABEL|_KIND)$ ]]; then
    disposition="keep_code_constant"
    new_owner="owning adapter contract module"
    consumer="$(basename "${file}" .rs)"
    reason="Stable identifier or protocol spelling is not an operator-tunable value"
  elif [[ "${symbol}" == "MAX_PREVIEW_OCCURRENCES" || "${symbol}" =~ (MAX_REQUEST|MAX_BODY|MAX_PAYLOAD|MAX_PAGE|MAX_DEPTH|MAX_RECURSION|MAX_FRAME|MAX_MESSAGE|MAX_QUERY) ]]; then
    disposition="keep_safety_limit"
    new_owner="centralized defensive-limit module"
    consumer="boundary validator"
    reason="Defensive upper bound prevents resource amplification and must not be raised through Runtime Config"
  elif [[ "${file}" == *"/runtime_config/"* && "${symbol}" =~ (MIN_|MAX_|DEFAULT_|_BPS|_SECS|_MS|_USD|_RATIO|_COUNT|_LIMIT) ]]; then
    disposition="derive_from_typed_schema"
    new_owner="typed policy field validator"
    consumer="Runtime Config validation and generated UI schema"
    reason="Editable field bounds must come from the same typed validator that governs the API and UI"
  elif [[ "${file}" == *"/execution/"* || "${file}" == *"/report/"* ]]; then
    if [[ "${symbol}" =~ (_USD|_BPS|_RATIO|_CONFIDENCE|_SCORE|_EXPOSURE|_LOSS|_SLIPPAGE) ]]; then
      disposition="migrate_runtime_policy"
      new_owner="RecommendationPolicy or ExecutionRiskPolicy"
      consumer="money-impacting decision path"
      reason="Business decision threshold requires audited hot activation and decision snapshot lineage"
    else
      disposition="keep_algorithm_invariant"
      new_owner="owning execution or report contract module"
      consumer="$(basename "${file}" .rs)"
      reason="Internal algorithm invariant is centralized and is not an operator preference"
    fi
  elif [[ "${file}" == *"/config/"* && "${symbol}" =~ (TIMEOUT|POLL|LEASE|HEARTBEAT|BATCH|CONCURRENCY|POOL|CAPACITY|INTERVAL) ]]; then
    disposition="migrate_deploy_budget"
    new_owner="typed Deploy resource budget"
    consumer="infrastructure or worker constructor"
    reason="Host-sensitive capacity and timing belong to one validated deployment budget"
  elif [[ "${file}" == *"/research/"* || "${file}" == *"quant-pivot-research/"* ]]; then
    if [[ "${symbol}" =~ (MIN_|MAX_|DEFAULT_|_BPS|_RATIO|_CONFIDENCE|_SAMPLES|_TRIALS|_FOLDS) ]]; then
      disposition="migrate_immutable_profile"
      new_owner="ResearchProfile, FeatureProfile or ScoringProfile artifact"
      consumer="training, replay or evaluation job"
      reason="Research methodology must be frozen and content-addressed with each job"
    else
      disposition="keep_algorithm_invariant"
      new_owner="owning research contract module"
      consumer="$(basename "${file}" .rs)"
      reason="Mathematical or serialization invariant is intentionally not operator editable"
    fi
  elif [[ "${symbol}" =~ (TIMEOUT|POLL|LEASE|HEARTBEAT|BATCH|CONCURRENCY|POOL|CAPACITY|INTERVAL) ]]; then
    disposition="centralize_operational_constant"
    new_owner="owning worker contract or Deploy resource budget"
    consumer="$(basename "${file}" .rs)"
    reason="Operational constant must have one named owner; environment-sensitive values move to Deploy Config"
  else
    disposition="keep_code_constant"
    new_owner="owning typed contract module"
    consumer="$(basename "${file}" .rs)"
    reason="Stable mathematical, lexical or defensive invariant is not a business configuration"
  fi

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${file}:${line}" "${symbol}" "code" "${disposition}" "${new_owner}" "${consumer}" "${reason}"
}

while IFS=: read -r file line declaration; do
  symbol="$(sed -E 's/^[[:space:]]*(pub([[:space:]]*\([^)]*\))?[[:space:]]+)?(const|static)[[:space:]]+([A-Z][A-Z0-9_]*).*/\4/' <<<"${declaration}")"
  classify "${file}" "${symbol}"
done < <(
  cd "${repo_root}"
  rg -n --no-heading \
    '^[[:space:]]*(pub([[:space:]]*\([^)]*\))?[[:space:]]+)?(const|static)[[:space:]]+[A-Z][A-Z0-9_]*[[:space:]]*:' \
    crates/*/src --glob '*.rs' | LC_ALL=C sort
) >>"${inventory_tmp}"

if ${check_only}; then
  if ! cmp -s "${inventory_tmp}" "${output_path}"; then
    echo "code constant inventory is stale; run scripts/generate-constant-inventory.sh" >&2
    diff -u "${output_path}" "${inventory_tmp}" || true
    exit 1
  fi
  exit 0
fi

mkdir -p "$(dirname "${output_path}")"
cp "${inventory_tmp}" "${output_path}"
echo "generated ${output_path}"
