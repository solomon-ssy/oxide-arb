#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"${repo_root}/scripts/generate-config-leaf-inventory.sh" --check

inventory="${repo_root}/docs/plans/quant-pivot/inventory/config-leaf-inventory.tsv"
expected_columns=9
awk -F '\t' -v expected="${expected_columns}" '
  NF != expected {
    printf "invalid inventory row %d: expected %d columns, found %d\n", NR, expected, NF > "/dev/stderr"
    invalid = 1
  }
  NR > 1 && ($1 == "" || $3 == "" || $4 == "" || $5 == "" || $6 == "" || $7 == "" || $8 == "" || $9 == "") {
    printf "incomplete inventory row %d\n", NR > "/dev/stderr"
    invalid = 1
  }
  END { exit invalid }
' "${inventory}"

config_sources="${repo_root}/crates/quant-pivot-models/src/config"
if hits="$(rg -n 'pub (password|private_key|signing_key|previous_signing_keys|api_key|api_secret|bot_token|authorization):' \
  "${config_sources}" --glob '*.rs' --glob '!secret.rs' \
  | rg -v 'SecretText' || true)" && [[ -n "${hits}" ]]; then
  echo "ERROR: every Deploy Config secret field must use SecretText directly" >&2
  echo "${hits}" >&2
  exit 1
fi

if rg -n 'DeploySecret|SystemdCredentialRef|SchemaMigrationConfig|resolve_(runtime|migration)_credentials|CREDENTIALS_DIRECTORY' \
  "${config_sources}" "${repo_root}/config" --glob '*.rs' --glob '*.toml'; then
  echo "ERROR: deleted credential-source or database migration-identity design reappeared" >&2
  exit 1
fi

if rg -n 'config::Environment|with_prefix\("QUANT_PIVOT"\)' \
  "${config_sources}" --glob '*.rs'; then
  echo "ERROR: arbitrary QUANT_PIVOT__* environment overlays are forbidden" >&2
  exit 1
fi

if rg -n '^(password|private_key|signing_key|api_key|api_secret|bot_token|authorization)[[:space:]]*=[[:space:]]*"[^"[:space:]][^"]*"' \
  "${repo_root}/config/quant-pivot.toml"; then
  echo "ERROR: tracked base Deploy TOML must not contain non-empty secrets" >&2
  exit 1
fi

for contract in \
  'pub struct SecretText(Zeroizing<String>)' \
  'impl fmt::Debug for SecretText' \
  "impl<'de> Deserialize<'de> for SecretText"; do
  if ! rg -q -F "${contract}" "${repo_root}/crates/quant-pivot-models/src/config/secret.rs"; then
    echo "ERROR: direct SecretText contract missing: ${contract}" >&2
    exit 1
  fi
done

if rg -n '\[db\.(postgres|clickhouse)\.migration\]|migration[[:space:]]*=' \
  "${repo_root}/config" --glob '*.toml'; then
  echo "ERROR: PostgreSQL and ClickHouse each permit only one configured identity" >&2
  exit 1
fi
