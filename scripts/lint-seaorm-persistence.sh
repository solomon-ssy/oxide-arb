#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"
errors=0

echo "=== Checking semantic persistence-field decisions ==="
cargo run -q -p quant-pivot-xtask -- persistence-field-audit

echo "=== Checking JSONB field decisions ==="
cargo run -q -p quant-pivot-xtask -- jsonb-field-audit

echo "=== Checking handwritten SQL boundaries ==="
cargo run -q -p quant-pivot-xtask -- sql-contract-audit

echo "=== Checking Config persistence is strongly typed ==="
config_entities=(
  crates/quant-pivot-models/src/entities/policy_revision.rs
  crates/quant-pivot-models/src/entities/policy_approval.rs
  crates/quant-pivot-models/src/entities/policy_activation.rs
  crates/quant-pivot-models/src/entities/decision_policy_snapshot.rs
  crates/quant-pivot-models/src/entities/system_production_baseline.rs
)
if rg -n 'pub (document|validation_evidence|snapshot|evidence): (Json|serde_json::Value)|pub [a-z_]+: Json' \
  "${config_entities[@]}"; then
  echo "ERROR: Config governance JSONB columns must decode into typed FromJsonQueryResult structs/enums" >&2
  errors=$((errors + 1))
fi
if ! rg -q 'type_name = "qp_config_resource_kind"' \
     crates/quant-pivot-models/src/enums/runtime_config.rs ||
   ! rg -q 'sea_orm::DeriveActiveEnum' crates/quant-pivot-models/src/enums/mod.rs; then
  echo "ERROR: Config finite states must use SeaORM ActiveEnum" >&2
  errors=$((errors + 1))
fi
if ! rg -q '#\[sea_orm\(rs_type = "Enum", db_type = "Enum", enum_name = \$type_name\)\]' \
     crates/quant-pivot-models/src/enums/mod.rs ||
   rg -n 'rs_type = "String", db_type = "Enum"' \
     crates/quant-pivot-models/src crates/quant-pivot-migration/src/snapshots/v1; then
  echo "ERROR: PostgreSQL ActiveEnum must preserve native type identity with rs_type = \"Enum\"" >&2
  errors=$((errors + 1))
fi

echo "=== Checking governed queries stay batched ==="
config_repo="crates/quant-pivot-repository/src/postgres/governance/runtime_config.rs"
for contract in \
  'find_also_related\(RevisionEntity\)' \
  'distinct_on\(\[\(ActivationEntity, ActivationColumn::ResourceKind\)\]\)' \
  'on_conflict_do_nothing_on\(\[SnapshotColumn::DecisionPolicySnapshotId\]\)' \
  'TryInsertResult::Conflicted'; do
  if ! rg -q "${contract}" "${config_repo}"; then
    echo "ERROR: Config repository lost required batched/idempotent SeaORM contract: ${contract}" >&2
    errors=$((errors + 1))
  fi
done

echo "=== Checking model picker catalog stays a single typed projection ==="
model_repo="crates/quant-pivot-repository/src/postgres/quant/model_registry.rs"
catalog_port="crates/quant-pivot-core/src/app/ports/research_catalog.rs"
for contract in \
  'async fn list_published_catalog' \
  'select_only\(\)' \
  'column_as\(quant_model_spec::Column::Name, "spec_name"\)' \
  'into_model::<PublishedModelCatalogInfo>\(\)'; do
  if ! rg -q "${contract}" "${model_repo}"; then
    echo "ERROR: model picker lost its typed single-query projection: ${contract}" >&2
    errors=$((errors + 1))
  fi
done
if rg -n 'load_hash_verified_artifact|find_model_spec_by_id' "${catalog_port}"; then
  echo "ERROR: model picker catalog regressed to per-row spec/artifact loading" >&2
  errors=$((errors + 1))
fi

if loop_awaits="$(rg -n -U --pcre2 \
  'for\s+[^\{]+\{(?:(?!\n\s*\}).)*?\.(one|all|count|exec|save|insert|update|delete)\([^;]*\)\.await' \
  crates/quant-pivot-repository/src --glob '*.rs' || true)" && [[ -n "${loop_awaits}" ]]; then
  echo "ERROR: possible repository N+1 query inside a loop; use eager join, Loader, IN query, or batch write" >&2
  echo "${loop_awaits}" >&2
  errors=$((errors + 1))
fi

if direct_batch_writes="$(rg -l '::insert_many\(' \
  crates/quant-pivot-repository/src --glob '*.rs' | \
  rg -v 'postgres/(write|rbac/casbin/adapter)\.rs$' || true)" && \
  [[ -n "${direct_batch_writes}" ]]; then
  echo "ERROR: direct insert_many bypasses the bind-budgeted batch-write boundary" >&2
  echo "${direct_batch_writes}" >&2
  errors=$((errors + 1))
fi

if (( errors > 0 )); then
  echo "${errors} SeaORM persistence violation(s) found" >&2
  exit 1
fi

echo "SeaORM persistence contracts passed"
