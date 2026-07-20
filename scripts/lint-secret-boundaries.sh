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
  echo "ERROR: secret environment-variable contract found; use the permission-restricted deploy TOML" >&2
  echo "Files: ${hits}" >&2
  errors=$((errors + 1))
fi

echo "=== Checking tracked deploy TOML carries only explicit secret placeholders ==="
plaintext_toml_pattern='^[[:space:]]*(private_key|password|signing_key|api_key|api_secret|bot_token|authorization)[[:space:]]*=[[:space:]]*"[^"[:space:]][^"]*"'
if rg -n -P "${plaintext_toml_pattern}" config/quant-pivot.toml; then
  echo "ERROR: the tracked base config must not contain a non-empty secret" >&2
  errors=$((errors + 1))
fi
if hits="$(rg -n -P "${plaintext_toml_pattern}" config/quant-pivot.production.example.toml \
  | rg -v '"(REPLACE_WITH_[A-Z0-9_]+|Bearer REPLACE_WITH_[A-Z0-9_]+)"$' || true)" &&
  [[ -n "${hits}" ]]; then
  echo "ERROR: production example contains a non-placeholder secret value" >&2
  echo "${hits}" >&2
  errors=$((errors + 1))
fi
if hits="$(rg -n 'rpc_endpoint[[:space:]]*=.*source[[:space:]]*=[[:space:]]*"protected"' \
  config/quant-pivot.production.example.toml \
  | rg -v 'REPLACE_WITH_' || true)" && [[ -n "${hits}" ]]; then
  echo "ERROR: protected RPC example must contain only REPLACE_WITH placeholders" >&2
  echo "${hits}" >&2
  errors=$((errors + 1))
fi

if hits="$(rg -l -P '^[[:space:]]*rpc_url[[:space:]]*=' config docs/operations docs/plans/quant-pivot \
  --glob '*.toml' --glob '*.md' || true)" && [[ -n "${hits}" ]]; then
  echo "ERROR: untyped RPC URL assignment found; use PolygonRpcEndpoint" >&2
  echo "Files: ${hits}" >&2
  errors=$((errors + 1))
fi

echo "=== Checking the direct SecretText contract and deleted designs ==="
credential_types=(
  'pub private_key: Option<SecretText>'
  'pub signing_key: SecretText'
  'pub password: SecretText'
  'pub api_key: Option<SecretText>'
  'pub rpc_endpoint: PolygonRpcEndpoint'
)
for contract in "${credential_types[@]}"; do
  if ! rg -q --fixed-strings "${contract}" crates/quant-pivot-models/src/config; then
    echo "ERROR: typed credential contract missing: ${contract}" >&2
    errors=$((errors + 1))
  fi
done

if rg -n 'DeploySecret|SystemdCredentialRef|SchemaMigrationConfig|resolve_(runtime|migration)_credentials|CREDENTIALS_DIRECTORY' \
  crates/quant-pivot-models/src/config config --glob '*.rs' --glob '*.toml'; then
  echo "ERROR: deleted credential-source or dual-identity design reappeared" >&2
  errors=$((errors + 1))
fi

secret_source='crates/quant-pivot-models/src/config/secret.rs'
for contract in \
  'pub struct SecretText(Zeroizing<String>)' \
  'impl fmt::Debug for SecretText' \
  "impl<'de> Deserialize<'de> for SecretText"; do
  if ! rg -q -F "${contract}" "${secret_source}"; then
    echo "ERROR: SecretText safety contract missing: ${contract}" >&2
    errors=$((errors + 1))
  fi
done
if rg -n 'impl (fmt::)?Display for SecretText|impl Serialize for SecretText|derive\([^)]*Serialize[^)]*\)' \
  "${secret_source}"; then
  echo "ERROR: SecretText must never implement Display or Serialize" >&2
  errors=$((errors + 1))
fi

if (( errors > 0 )); then
  echo "${errors} secret-boundary violation(s) found" >&2
  exit 1
fi

echo "Secret-boundary contracts passed"
