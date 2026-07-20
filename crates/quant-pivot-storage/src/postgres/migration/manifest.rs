//! Normalized `PostgreSQL` schema manifest sourced from `pg_catalog`.

use std::collections::BTreeSet;

use quant_pivot_error::storage::StorageError;
use serde_json::Value;

use crate::sql_contract_registry::POSTGRES_SCHEMA_VERIFY;

const MANIFEST_JSON: &str = include_str!("../../../../../schema/postgres/manifest.json");

const INSPECT_SQL: &str = r#"
SELECT jsonb_build_object(
  'format_version', 1,
  'tables', COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'name', c.relname,
      'kind', c.relkind,
      'owner', pg_get_userbyid(c.relowner),
      'rls_enabled', c.relrowsecurity,
      'rls_forced', c.relforcerowsecurity
    ) ORDER BY c.relname)
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
  ), '[]'::jsonb),
  'columns', COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'table', c.relname,
      'ordinal', a.attnum - (
        SELECT COUNT(*)::integer
        FROM pg_attribute dropped
        WHERE dropped.attrelid = a.attrelid
          AND dropped.attnum > 0
          AND dropped.attnum <= a.attnum
          AND dropped.attisdropped
      ),
      'name', a.attname,
      'type', pg_catalog.format_type(a.atttypid, a.atttypmod),
      'nullable', NOT a.attnotnull,
      'default', pg_get_expr(d.adbin, d.adrelid),
      'identity', a.attidentity,
      'generated', a.attgenerated,
      'collation', CASE WHEN a.attcollation = 0 THEN NULL ELSE coll.collname END,
      'storage', a.attstorage,
      'compression', a.attcompression
    ) ORDER BY c.relname, a.attnum)
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
    LEFT JOIN pg_collation coll ON coll.oid = a.attcollation
    WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
      AND a.attnum > 0 AND NOT a.attisdropped
  ), '[]'::jsonb),
  'constraints', COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'table', c.relname,
      'name', con.conname,
      'type', con.contype,
      'deferrable', con.condeferrable,
      'initially_deferred', con.condeferred,
      'validated', con.convalidated,
      'definition', pg_get_constraintdef(con.oid, false)
    ) ORDER BY c.relname, con.conname)
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
  ), '[]'::jsonb),
  'indexes', COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'table', table_class.relname,
      'name', index_class.relname,
      'method', access_method.amname,
      'unique', idx.indisunique,
      'primary', idx.indisprimary,
      'valid', idx.indisvalid,
      'ready', idx.indisready,
      'definition', pg_get_indexdef(idx.indexrelid, 0, false)
    ) ORDER BY table_class.relname, index_class.relname)
    FROM pg_index idx
    JOIN pg_class table_class ON table_class.oid = idx.indrelid
    JOIN pg_namespace n ON n.oid = table_class.relnamespace
    JOIN pg_class index_class ON index_class.oid = idx.indexrelid
    JOIN pg_am access_method ON access_method.oid = index_class.relam
    WHERE n.nspname = 'public'
  ), '[]'::jsonb),
  'enums', COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'name', enum_type.typname,
      'labels', labels.values
    ) ORDER BY enum_type.typname)
    FROM pg_type enum_type
    JOIN pg_namespace n ON n.oid = enum_type.typnamespace
    CROSS JOIN LATERAL (
      SELECT jsonb_agg(value.enumlabel ORDER BY value.enumsortorder) AS values
      FROM pg_enum value WHERE value.enumtypid = enum_type.oid
    ) labels
    WHERE n.nspname = 'public' AND enum_type.typtype = 'e'
  ), '[]'::jsonb),
  'triggers', COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'table', c.relname,
      'name', t.tgname,
      'enabled', t.tgenabled,
      'definition', pg_get_triggerdef(t.oid, false)
    ) ORDER BY c.relname, t.tgname)
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND NOT t.tgisinternal
  ), '[]'::jsonb),
  'functions', COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'name', p.proname,
      'identity_arguments', pg_get_function_identity_arguments(p.oid),
      'result', pg_get_function_result(p.oid),
      'language', lang.lanname,
      'volatility', p.provolatile,
      'security_definer', p.prosecdef,
      'owner', pg_get_userbyid(p.proowner),
      'definition', pg_get_functiondef(p.oid)
    ) ORDER BY p.proname, pg_get_function_identity_arguments(p.oid))
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    JOIN pg_language lang ON lang.oid = p.prolang
    WHERE n.nspname = 'public'
  ), '[]'::jsonb),
  'sequences', COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'name', c.relname,
      'owner', pg_get_userbyid(c.relowner),
      'type', pg_catalog.format_type(seq.seqtypid, NULL),
      'start', seq.seqstart,
      'increment', seq.seqincrement,
      'minimum', seq.seqmin,
      'maximum', seq.seqmax,
      'cache', seq.seqcache,
      'cycle', seq.seqcycle
    ) ORDER BY c.relname)
    FROM pg_sequence seq
    JOIN pg_class c ON c.oid = seq.seqrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
  ), '[]'::jsonb),
  'extensions', COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'name', ext.extname,
      'version', ext.extversion,
      'schema', n.nspname
    ) ORDER BY ext.extname)
    FROM pg_extension ext JOIN pg_namespace n ON n.oid = ext.extnamespace
  ), '[]'::jsonb),
  'policies', COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'table', c.relname,
      'name', policy.polname,
      'permissive', policy.polpermissive,
      'roles', ARRAY(SELECT pg_get_userbyid(role_id) FROM unnest(policy.polroles) role_id ORDER BY 1),
      'command', policy.polcmd,
      'using', pg_get_expr(policy.polqual, policy.polrelid),
      'check', pg_get_expr(policy.polwithcheck, policy.polrelid)
    ) ORDER BY c.relname, policy.polname)
    FROM pg_policy policy
    JOIN pg_class c ON c.oid = policy.polrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
  ), '[]'::jsonb),
  'grants', COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'object_kind', grants.object_kind,
      'object_name', grants.object_name,
      'grantee', grants.grantee,
      'privilege', grants.privilege,
      'grantable', grants.grantable
    ) ORDER BY grants.object_kind, grants.object_name, grants.grantee, grants.privilege)
    FROM (
      SELECT CASE WHEN object.relkind = 'S' THEN 'sequence' ELSE 'table' END AS object_kind,
             object.relname AS object_name,
             CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(acl.grantee) END AS grantee,
             acl.privilege_type AS privilege,
             CASE WHEN acl.is_grantable THEN 'YES' ELSE 'NO' END AS grantable
      FROM pg_class object
      JOIN pg_namespace namespace ON namespace.oid = object.relnamespace
      CROSS JOIN LATERAL aclexplode(COALESCE(
        object.relacl,
        acldefault(CASE WHEN object.relkind = 'S' THEN 'S'::"char" ELSE 'r'::"char" END, object.relowner)
      )) acl
      WHERE namespace.nspname = 'public'
        AND object.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
      UNION ALL
      SELECT 'function', routine.proname || '(' || pg_get_function_identity_arguments(routine.oid) || ')',
             CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(acl.grantee) END,
             acl.privilege_type,
             CASE WHEN acl.is_grantable THEN 'YES' ELSE 'NO' END
      FROM pg_proc routine
      JOIN pg_namespace namespace ON namespace.oid = routine.pronamespace
      CROSS JOIN LATERAL aclexplode(COALESCE(
        routine.proacl,
        acldefault('f', routine.proowner)
      )) acl
      WHERE namespace.nspname = 'public'
      UNION ALL
      SELECT 'schema', namespace.nspname,
             CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(acl.grantee) END,
             acl.privilege_type,
             CASE WHEN acl.is_grantable THEN 'YES' ELSE 'NO' END
      FROM pg_namespace namespace
      CROSS JOIN LATERAL aclexplode(
        COALESCE(namespace.nspacl, acldefault('n', namespace.nspowner))
      ) acl
      WHERE namespace.nspname = 'public'
    ) grants
  ), '[]'::jsonb)
) AS manifest
"#;

pub(super) fn expected() -> Result<Value, StorageError> {
    serde_json::from_str(MANIFEST_JSON)
        .map_err(|error| StorageError::Migration(format!("parse PostgreSQL manifest: {error}")))
}

pub async fn inspect(pool: &sqlx::PgPool) -> Result<Value, StorageError> {
    let mut value =
        sqlx::query_scalar::<_, Value>(POSTGRES_SCHEMA_VERIFY.postgres_query(INSPECT_SQL))
            .fetch_one(pool)
            .await
            .map_err(|error| {
                StorageError::Migration(format!("inspect PostgreSQL manifest: {error}"))
            })?;
    normalize_roles(&mut value);
    Ok(value)
}

pub fn render(value: &Value) -> Result<String, StorageError> {
    serde_json::to_string_pretty(value)
        .map(|json| format!("{json}\n"))
        .map_err(|error| StorageError::Migration(format!("render PostgreSQL manifest: {error}")))
}

pub(super) fn section_counts(value: &Value) -> (usize, usize) {
    (array_len(value, "tables"), array_len(value, "indexes"))
}

pub(super) fn drift_sections(expected: &Value, actual: &Value) -> Vec<String> {
    let Some(expected) = expected.as_object() else {
        return vec!["manifest_root".to_owned()];
    };
    let Some(actual) = actual.as_object() else {
        return vec!["manifest_root".to_owned()];
    };
    expected
        .keys()
        .chain(actual.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| expected.get(*key) != actual.get(*key))
        .cloned()
        .collect()
}

fn array_len(value: &Value, key: &str) -> usize {
    value.get(key).and_then(Value::as_array).map_or(0, Vec::len)
}

fn normalize_roles(value: &mut Value) {
    let owners = ["tables", "functions", "sequences"]
        .into_iter()
        .filter_map(|section| value.get(section).and_then(Value::as_array))
        .flat_map(|rows| rows.iter())
        .filter_map(|row| row.get("owner").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();

    for section in ["tables", "functions", "sequences"] {
        if let Some(rows) = value.get_mut(section).and_then(Value::as_array_mut) {
            for row in rows {
                if let Some(owner) = row.get_mut("owner") {
                    *owner = Value::String("$schema_owner".to_owned());
                }
            }
        }
    }
    if let Some(rows) = value.get_mut("grants").and_then(Value::as_array_mut) {
        for row in rows {
            let Some(grantee) = row.get_mut("grantee") else {
                continue;
            };
            let normalized = match grantee.as_str() {
                Some("PUBLIC") => "$public",
                Some("pg_database_owner") => "$schema_owner",
                Some(role) if owners.contains(role) => "$schema_owner",
                Some(_) => "$unexpected_grantee",
                None => continue,
            };
            *grantee = Value::String(normalized.to_owned());
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::normalize_roles;

    #[test]
    fn normalizes_only_schema_owner_and_public_grantees() {
        let mut manifest = json!({
            "tables": [{ "owner": "quant_pivot" }],
            "functions": [],
            "sequences": [],
            "grants": [
                { "grantee": "PUBLIC" },
                { "grantee": "pg_database_owner" },
                { "grantee": "quant_pivot" },
                { "grantee": "unmanaged_role" }
            ]
        });

        normalize_roles(&mut manifest);

        assert_eq!(manifest["tables"][0]["owner"], "$schema_owner");
        assert_eq!(manifest["grants"][0]["grantee"], "$public");
        assert_eq!(manifest["grants"][1]["grantee"], "$schema_owner");
        assert_eq!(manifest["grants"][2]["grantee"], "$schema_owner");
        assert_eq!(manifest["grants"][3]["grantee"], "$unexpected_grantee");
    }
}
