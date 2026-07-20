#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

errors=0
scan_paths=(
  config
  crates
  docs/operations
  docs/plans/quant-pivot
  .cursor
  AGENTS.md
)

echo "=== Checking secret values are never sourced from environment variables ==="
secret_env_pattern='QUANT_PIVOT(__|_TEST_)[A-Z0-9_]*(PRIVATE_KEY(?!_FILE)|PASSWORD(?!_FILE)|SIGNING_KEY(?!_FILE)|API_KEY(?!_ADDRESS|_FILE)|API_SECRET(?!_FILE)|BOT_TOKEN(?!_FILE))'
if hits="$(rg -l -P "${secret_env_pattern}" "${scan_paths[@]}" \
  --glob '*.rs' --glob '*.toml' --glob '*.md' --glob '*.mdc' || true)" &&
  [[ -n "${hits}" ]]; then
  echo "ERROR: secret environment-variable contract found; use a typed credential-file reference" >&2
  echo "Files: ${hits}" >&2
  errors=$((errors + 1))
fi

echo "=== Checking TOML examples never carry plaintext credential values ==="
plaintext_toml_pattern='^[[:space:]]*(private_key|password|signing_key|api_key|api_secret|bot_token_credential|authorization_credential)[[:space:]]*=[[:space:]]*"'
if hits="$(rg -l -P "${plaintext_toml_pattern}" config docs/operations docs/plans/quant-pivot \
  --glob '*.toml' --glob '*.md' || true)" && [[ -n "${hits}" ]]; then
  echo "ERROR: plaintext credential assignment found; TOML may store only { name = \"...\" }" >&2
  echo "Files: ${hits}" >&2
  errors=$((errors + 1))
fi

if hits="$(rg -l -P '^[[:space:]]*rpc_url[[:space:]]*=' config docs/operations docs/plans/quant-pivot \
  --glob '*.toml' --glob '*.md' || true)" && [[ -n "${hits}" ]]; then
  echo "ERROR: untyped RPC URL assignment found; use PolygonRpcEndpoint" >&2
  echo "Files: ${hits}" >&2
  errors=$((errors + 1))
fi

echo "=== Checking credential fields remain typed references ==="
credential_types=(
  'pub private_key: Option<SystemdCredentialRef>'
  'pub signing_key: SystemdCredentialRef'
  'pub password: SystemdCredentialRef'
  'pub api_key: Option<SystemdCredentialRef>'
  'pub rpc_endpoint: PolygonRpcEndpoint'
)
for contract in "${credential_types[@]}"; do
  if ! rg -q --fixed-strings "${contract}" crates/quant-pivot-models/src/config; then
    echo "ERROR: typed credential contract missing: ${contract}" >&2
    errors=$((errors + 1))
  fi
done

if (( errors > 0 )); then
  echo "${errors} secret-boundary violation(s) found" >&2
  exit 1
fi

echo "Secret-boundary contracts passed"
