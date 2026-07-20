#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_path="${repo_root}/docs/plans/quant-pivot/inventory/config-leaf-inventory.tsv"
check_only=false
if [[ "${1:-}" == "--check" ]]; then
  check_only=true
elif [[ -n "${1:-}" ]]; then
  echo "usage: $0 [--check]" >&2
  exit 2
fi

for command in cargo jq yq; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "required command not found: ${command}" >&2
    exit 1
  fi
done

inventory_tmp="$(mktemp -d "${TMPDIR:-/tmp}/quant-pivot-config-inventory.XXXXXX")"
trap 'rm -rf "${inventory_tmp}"' EXIT

(
  cd "${repo_root}"
  CARGO_INCREMENTAL=0 cargo run --quiet -p quant-pivot-xtask -- render-boot-policy \
    >"${inventory_tmp}/runtime.json"
)
yq -p toml -o json '.' "${repo_root}/config/quant-pivot.toml" \
  >"${inventory_tmp}/deploy-base.json"
yq -p toml -o json '.' "${repo_root}/config/quant-pivot.production.example.toml" \
  >"${inventory_tmp}/deploy-production.json"
jq -s '.[0] * .[1]' \
  "${inventory_tmp}/deploy-base.json" \
  "${inventory_tmp}/deploy-production.json" \
  >"${inventory_tmp}/deploy.json"

printf '%s\n' $'path\tcurrent_owner\tdisposition\tnew_owner\tconsumer\tapply_boundary\tvalidator\tsecret_classification\treason' \
  >"${inventory_tmp}/inventory.tsv"

jq -r '
  def canonical_path:
    map(if type == "number" then "[]" else tostring end)
    | join(".")
    | gsub("\\.\\[\\]"; "[]");
  def metadata($path):
    if ($path | contains("hold_to_resolution")) then
      ["delete", "none", "none", "not_applicable", "none", "none",
       "Field encoded a no-op path and falsely implied governed exit behavior"]
    elif ($path | startswith("recommendation.")) then
      ["keep_runtime", "RecommendationPolicy", "report coordinator, market selector, data-quality gate",
       "new ReportRun claim", "typed policy validator and dependency preflight", "none",
       "Changes report eligibility and recommendation output and therefore requires audited hot activation"]
    elif ($path | startswith("execution_risk.")) then
      ["keep_runtime", "ExecutionRiskPolicy", "portfolio planner, admission gate, execution workers",
       "new OrderIntent or admission decision", "typed risk validator and execution preflight", "none",
       "Changes capital or execution decisions and must be frozen at a money-impacting decision boundary"]
    elif ($path | startswith("model_routing.")) then
      ["keep_runtime", "ModelRouting", "model router and category pointer guard",
       "new report or model evaluation run", "artifact existence, status, scope and content-hash preflight", "none",
       "Serving artifact selection is operational routing, not research methodology"]
    elif ($path | startswith("report_schedule.")) then
      ["keep_runtime", "ReportSchedule", "durable report scheduler",
       "future unclaimed runs after reconcile", "typed cadence, timezone and preview validator", "none",
       "Operators need audited schedule changes without process restart"]
    elif ($path | startswith("operational_control.")) then
      ["keep_runtime", "OperationalControl", "admission gates and notification router",
       "next admission check", "typed operational-control validator and consumer prepare", "none",
       "Immediate pause, halt and routing controls require atomic hot application"]
    elif ($path | startswith("execution_authorization.")) then
      ["keep_runtime", "ExecutionAuthorization", "runtime-mode admission and authorization gate",
       "next admission after mode preflight", "credential, capability and funds-impact preflight", "none",
       "Execution authority is a governed operator decision rather than a deployment knob"]
    elif ($path | startswith("profile_artifacts.features.")) then
      ["migrate", "FeatureProfile artifact", "feature pipeline, training and replay",
       "new job or artifact lineage", "FeatureSchema builder and content-hash validation", "none",
       "Feature methodology must be immutable and reproducible rather than hot edited"]
    elif ($path | startswith("profile_artifacts.scoring.")) then
      ["migrate", "ScoringProfile artifact", "factor pipeline, trainer and backtest",
       "new job or artifact lineage", "factor-contract validator and content-hash validation", "none",
       "Factor construction and normalization define research methodology"]
    elif ($path | startswith("profile_artifacts.domain.")) then
      ["migrate", "DomainProfile artifact", "domain feature and evidence pipelines",
       "new job or artifact lineage", "domain capability and evidence-contract validator", "none",
       "Domain semantics must be frozen with the run that consumed them"]
    elif ($path | startswith("profile_artifacts.research_method.training.")) then
      ["migrate", "TrainingRunSpec", "dataset builder and model trainer",
       "job enqueue", "training-spec validator", "none",
       "Training inputs belong to an immutable job specification"]
    elif ($path | startswith("profile_artifacts.research_method.research.")) then
      ["migrate", "ResearchMethodProfile artifact", "CPCV, PBO, backtest and policy-fit jobs",
       "job enqueue", "research-methodology validator and content hash", "none",
       "Research methodology must be reproducible and cannot change under a running experiment"]
    elif ($path | startswith("profile_artifacts.research_method.model_promotion.")) then
      ["migrate", "ModelEvaluationSpec artifact", "model quality-gate workflow",
       "promotion review creation", "quality-gate evidence validator", "none",
       "Evaluation thresholds must be frozen with promotion evidence until ModelPromotionPolicy is implemented"]
    elif ($path | startswith("profile_artifacts.research_method.schema_version")) then
      ["migrate", "ResearchMethodProfile artifact", "research and training lineage readers",
       "new job or artifact lineage", "boot schema and content-hash validation", "none",
       "The immutable research-method contract is versioned independently from hot policy resources"]
    elif ($path | startswith("revisions.")) then
      ["system_generated", "DecisionPolicySnapshot revision bundle", "lineage and audit readers",
       "policy activation transaction", "repository foreign-key and bundle completeness invariant", "none",
       "Revision identifiers are generated by governance and are never editable configuration"]
    else
      ["delete", "none", "none", "not_applicable", "none", "none",
       "Leaf is outside the six governed resources and immutable profile contracts"]
    end;
  paths((type != "object" and type != "array") or (type == "array" and length == 0)) as $parts
  | (if (getpath($parts) | type) == "array" then $parts + ["[]"] else $parts end
     | canonical_path) as $path
  | metadata($path) as $m
  | ([$path, "runtime_config"] + $m | @tsv)
' "${inventory_tmp}/runtime.json" | LC_ALL=C sort -u >>"${inventory_tmp}/inventory.tsv"

jq -r '
  def canonical_path:
    map(if type == "number" then "[]" else tostring end)
    | join(".")
    | gsub("\\.\\[\\]"; "[]");
  def is_secret($path):
    ($path | test("(^|\\.)(password|private_key|signing_key|previous_signing_keys\\[\\]|token|secret|api_key|api_secret|bot_token|authorization)$"))
      or $path == "polymarket.onchain.rpc_endpoint.url";
  def metadata($path):
    if $path == "polymarket.chain_id" then
      ["migrate", "Polymarket protocol catalog constant", "EIP-712 and transaction signing",
       "application build", "compile-time protocol contract", "public_protocol_value",
       "The chain id is an external protocol invariant and must not be operator editable"]
    elif is_secret($path) then
      ["keep_deploy", "SecretText", "owning infrastructure or provider adapter",
       "process start", "typed format validation, Debug redaction and tracked-placeholder lint", "plaintext_deploy_secret",
       "The permission-restricted deploy TOML is the single secret source; values are zeroized and never serialized"]
    elif ($path | startswith("db.")) then
      ["keep_deploy", "database resource budget", "PostgreSQL or ClickHouse adapter",
       "process start", "typed DeployConfig validation and connectivity preflight", "none",
       "Connection locations and host capacity cannot be hot swapped safely"]
    elif ($path | startswith("cache.")) then
      ["keep_deploy", "cache resource budget", "Redis and in-process cache adapters",
       "process start", "typed DeployConfig validation and connectivity preflight", "none",
       "Cache capacity, namespace and connection policy are deployment properties"]
    elif ($path | startswith("market_data.")) then
      ["keep_deploy", "market_data_ingest resource budget", "Gamma, Data API and CLOB ingest adapters",
       "process start", "provider binding and capacity validator", "none",
       "Provider endpoints, connection limits and ingest capacity are deployment-scoped"]
    elif ($path | startswith("domain_sources.")) then
      ["keep_deploy", "provider binding catalog", "typed domain-source adapters",
       "process start", "typed source/binding validator and provider preflight", "none",
       "External source endpoints and physical station bindings are environment-specific adapter inputs"]
    elif ($path | startswith("quant.research_jobs.")) then
      ["merge", "research_jobs resource budget", "research worker scheduler",
       "process start", "derived lease, heartbeat and concurrency invariants", "none",
       "Independent worker timings and capacities must be derived from one coherent host budget"]
    elif ($path | startswith("quant.workers.")) then
      ["merge", "report_execution resource budget", "report and execution worker supervisors",
       "process start", "derived queue, lease and poll invariants", "none",
       "Worker-level knobs are consolidated to prevent contradictory timing relationships"]
    elif ($path | startswith("quant.account.")) then
      ["keep_deploy", "execution identity binding", "account and signer adapters",
       "process start", "typed wallet-kind and credential preflight", "credential_metadata",
       "Wallet implementation is a deployment identity choice, not a hot policy"]
    elif ($path | startswith("polymarket.")) then
      ["keep_deploy", "Polymarket provider binding", "CLOB, on-chain and relayer clients",
       "process start", "typed endpoint and timeout validation", "none",
       "External service locations and transport timeouts vary by deployment"]
    elif ($path | startswith("research.artifact_store.")) then
      ["keep_deploy", "research_jobs resource budget", "artifact-store adapter",
       "process start", "typed artifact-store binding validation", "none",
       "Artifact storage location and adapter type are infrastructure choices"]
    elif ($path | startswith("web.")) then
      ["keep_deploy", "web resource budget and identity", "HTTP server and authentication layer",
       "process start", "typed bind, JWT and path validation", "none",
       "Listener, deployment identity and authentication issuer are startup contracts"]
    elif ($path | startswith("observability.")) then
      ["keep_deploy", "observability deployment policy", "tracing and alert adapters",
       "process start", "typed log-level and channel binding validator", "none",
       "Telemetry sinks and process logging are deployment concerns"]
    else
      ["keep_deploy", "typed DeployConfig", "owning infrastructure adapter",
       "process start", "typed DeployConfig validator", "none",
       "Leaf is required to construct an immutable process dependency"]
    end;
  paths((type != "object" and type != "array") or (type == "array" and length == 0)) as $parts
  | (if (getpath($parts) | type) == "array" then $parts + ["[]"] else $parts end
     | canonical_path) as $path
  | metadata($path) as $m
  | ([$path, "deploy_config"] + $m | @tsv)
' "${inventory_tmp}/deploy.json" | LC_ALL=C sort -u >>"${inventory_tmp}/inventory.tsv"

if ${check_only}; then
  if ! cmp -s "${inventory_tmp}/inventory.tsv" "${output_path}"; then
    echo "config leaf inventory is stale; run scripts/generate-config-leaf-inventory.sh" >&2
    diff -u "${output_path}" "${inventory_tmp}/inventory.tsv" || true
    exit 1
  fi
  exit 0
fi

mkdir -p "$(dirname "${output_path}")"
cp "${inventory_tmp}/inventory.tsv" "${output_path}"
echo "generated ${output_path}"
