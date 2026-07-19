#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"${repo_root}/scripts/generate-config-leaf-inventory.sh" --check
"${repo_root}/scripts/generate-constant-inventory.sh" --check

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

constant_inventory="${repo_root}/docs/plans/quant-pivot/inventory/code-constant-inventory.tsv"
awk -F '\t' '
  NF != 7 {
    printf "invalid constant inventory row %d: expected 7 columns, found %d\n", NR, NF > "/dev/stderr"
    invalid = 1
  }
  NR > 1 && ($1 == "" || $2 == "" || $4 == "" || $5 == "" || $6 == "" || $7 == "") {
    printf "incomplete constant inventory row %d\n", NR > "/dev/stderr"
    invalid = 1
  }
  END { exit invalid }
' "${constant_inventory}"

config_sources="${repo_root}/crates/quant-pivot-models/src/config"
if rg -n 'pub (password|private_key|signing_key|previous_signing_keys|api_key|api_secret|bot_token|authorization):.*(SecretText|SecretString)' \
  "${config_sources}" --glob '*.rs' --glob '!secret.rs'; then
  echo "ERROR: Deploy Config secret fields must be typed SystemdCredentialRef values" >&2
  exit 1
fi

if rg -n 'config::Environment|with_prefix\("QUANT_PIVOT"\)' \
  "${config_sources}" --glob '*.rs'; then
  echo "ERROR: arbitrary QUANT_PIVOT__* environment overlays are forbidden" >&2
  exit 1
fi

if rg -n '^(password|private_key|signing_key|api_key|api_secret|bot_token_credential|authorization_credential)[[:space:]]*=[[:space:]]*"' \
  "${repo_root}/config/quant-pivot.toml" \
  "${repo_root}/config/quant-pivot.production.example.toml"; then
  echo "ERROR: Deploy TOML must contain credential references, never secret-shaped scalar values" >&2
  exit 1
fi

for contract in \
  'resolve_runtime_credentials' \
  'resolve_migration_credentials' \
  'CREDENTIALS_DIRECTORY' \
  'symlink_metadata' \
  'metadata.permissions().mode() & 0o077'; do
  if ! rg -q -F "${contract}" \
    "${repo_root}/crates/quant-pivot-models/src/config/mod.rs" \
    "${repo_root}/crates/quant-pivot-models/src/config/secret.rs"; then
    echo "ERROR: systemd credential bootstrap contract missing: ${contract}" >&2
    exit 1
  fi
done
