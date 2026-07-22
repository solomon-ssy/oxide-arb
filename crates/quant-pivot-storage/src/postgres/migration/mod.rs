//! `PostgreSQL` seed deployment and read-only runtime schema verification.

mod helpers;
mod manifest;

use std::collections::BTreeMap;

use helpers::{run_catalog_seeds, verify_catalog_seeds};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{hashing::CanonicalDigest, types::ContentHash};
use sea_orm::DatabaseConnection;
use serde::Deserialize;
use serde_json::Value;
use sqlx::{AssertSqlSafe, Error, PgPool, Row};

const MIGRATION_MANIFEST_JSON: &str =
    include_str!("../../../../../schema/postgres/migrations.json");

#[derive(Debug, Deserialize)]
struct MigrationManifest {
    format_version: u32,
    checksum_algorithm: String,
    migration_engine: String,
    migrations: Vec<ExpectedMigration>,
}

#[derive(Debug, Deserialize)]
struct ExpectedMigration {
    version: String,
    checksum: String,
    artifact_length: i64,
}

/// Verified `PostgreSQL` schema metadata exposed by startup and deploy tooling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresSchemaStatus {
    pub current_version: String,
    pub migration_count: usize,
    pub required_table_count: usize,
    pub required_index_count: usize,
    pub schema_fingerprint: ContentHash,
}

/// Apply transactional catalog seeds and public-surface hardening after migration.
pub async fn finalize_schema_deployment(
    db: &DatabaseConnection,
    bootstrap_admin_password_hash: &str,
) -> Result<PostgresSchemaStatus, StorageError> {
    run_catalog_seeds(db, bootstrap_admin_password_hash)
        .await
        .map_err(|error| StorageError::Migration(format!("apply catalog seeds: {error}")))?;
    apply_public_privilege_hardening(db.get_postgres_connection_pool()).await?;
    verify_contract(db).await
}

async fn apply_public_privilege_hardening(pool: &PgPool) -> Result<(), StorageError> {
    let database = sqlx::query_scalar::<_, String>("SELECT current_database()")
        .fetch_one(pool)
        .await
        .map_err(migration_error("read PostgreSQL database name"))?;
    let database = quote_identifier(&database);
    let statements = [
        "REVOKE CREATE ON SCHEMA public FROM PUBLIC".to_owned(),
        format!("REVOKE TEMPORARY ON DATABASE {database} FROM PUBLIC"),
        "REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC".to_owned(),
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC"
            .to_owned(),
    ];
    for statement in statements {
        sqlx::raw_sql(AssertSqlSafe(statement))
            .execute(pool)
            .await
            .map_err(migration_error(
                "apply PostgreSQL public privilege hardening",
            ))?;
    }
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Verify checksums, semantic schema, and seed data.
pub async fn verify_schema(db: &DatabaseConnection) -> Result<PostgresSchemaStatus, StorageError> {
    verify_contract(db).await
}

/// Inspect the normalized semantic manifest for deploy tooling.
pub async fn inspect_schema_manifest(db: &DatabaseConnection) -> Result<Value, StorageError> {
    manifest::inspect(db.get_postgres_connection_pool()).await
}

/// Render a normalized manifest deterministically for source control.
pub fn render_schema_manifest(value: &Value) -> Result<String, StorageError> {
    manifest::render(value)
}

/// Apply post-migration seeds and grants in an owned disposable database, then
/// inspect the semantic manifest used for source-controlled schema generation.
///
/// This deliberately refuses ordinary database names so manifest generation
/// cannot become an unchecked production deployment path.
pub async fn generate_disposable_schema_manifest(
    db: &DatabaseConnection,
    bootstrap_admin_password_hash: &str,
) -> Result<Value, StorageError> {
    let database = sqlx::query_scalar::<_, String>("SELECT current_database()")
        .fetch_one(db.get_postgres_connection_pool())
        .await
        .map_err(migration_error("read disposable manifest database name"))?;
    if !database.starts_with("quant_pivot_manifest_") {
        return Err(StorageError::Migration(format!(
            "refusing unchecked manifest generation outside an owned disposable database: {database}"
        )));
    }
    run_catalog_seeds(db, bootstrap_admin_password_hash)
        .await
        .map_err(|error| StorageError::Migration(format!("apply catalog seeds: {error}")))?;
    apply_public_privilege_hardening(db.get_postgres_connection_pool()).await?;
    manifest::inspect(db.get_postgres_connection_pool()).await
}

async fn verify_contract(db: &DatabaseConnection) -> Result<PostgresSchemaStatus, StorageError> {
    let pool = db.get_postgres_connection_pool();
    let migration_manifest = expected_migration_manifest()?;
    verify_migration_ledger(pool, &migration_manifest).await?;

    let expected = manifest::expected()?;
    let actual = manifest::inspect(pool).await?;
    if actual != expected {
        let sections = manifest::drift_sections(&expected, &actual);
        return Err(StorageError::Migration(format!(
            "PostgreSQL semantic schema manifest drift in sections: {}",
            sections.join(", ")
        )));
    }
    verify_catalog_seeds(db)
        .await
        .map_err(|error| StorageError::Migration(format!("verify catalog seeds: {error}")))?;

    let manifest_bytes = manifest::render(&expected)?;
    let (required_table_count, required_index_count) = manifest::section_counts(&expected);
    let current_version = migration_manifest
        .migrations
        .last()
        .map(|migration| migration.version.clone())
        .ok_or_else(|| StorageError::Migration("migration manifest is empty".to_owned()))?;
    Ok(PostgresSchemaStatus {
        current_version,
        migration_count: migration_manifest.migrations.len(),
        required_table_count,
        required_index_count,
        schema_fingerprint: CanonicalDigest::content_hash_bytes(manifest_bytes.as_bytes()),
    })
}

fn expected_migration_manifest() -> Result<MigrationManifest, StorageError> {
    let manifest: MigrationManifest = serde_json::from_str(MIGRATION_MANIFEST_JSON)
        .map_err(|error| StorageError::Migration(format!("parse migration manifest: {error}")))?;
    if manifest.format_version != 1 {
        return Err(StorageError::Migration(format!(
            "unsupported migration manifest format {}",
            manifest.format_version
        )));
    }
    if manifest.migrations.is_empty() {
        return Err(StorageError::Migration(
            "migration manifest must contain at least one migration".to_owned(),
        ));
    }
    Ok(manifest)
}

async fn verify_migration_ledger(
    pool: &PgPool,
    expected: &MigrationManifest,
) -> Result<(), StorageError> {
    let (seaorm_exists, audit_exists, legacy_sqlx_exists) =
        sqlx::query_as::<_, (bool, bool, bool)>(
            "SELECT to_regclass('public.seaql_migrations') IS NOT NULL, \
                    to_regclass('public.schema_migration_audit') IS NOT NULL, \
                    to_regclass('public._sqlx_migrations') IS NOT NULL",
        )
        .fetch_one(pool)
        .await
        .map_err(migration_error("inspect PostgreSQL migration ledgers"))?;
    if legacy_sqlx_exists {
        return Err(StorageError::Migration(
            "forbidden legacy migration ledger `_sqlx_migrations` exists".to_owned(),
        ));
    }
    if !seaorm_exists || !audit_exists {
        return Err(StorageError::Migration(format!(
            "required migration ledgers are missing: seaql_migrations={seaorm_exists} \
             schema_migration_audit={audit_exists}"
        )));
    }

    let applied =
        sqlx::query_scalar::<_, String>("SELECT version FROM seaql_migrations ORDER BY version")
            .fetch_all(pool)
            .await
            .map_err(migration_error("read SeaORM migration ledger"))?;
    let expected_versions = expected
        .migrations
        .iter()
        .map(|migration| migration.version.clone())
        .collect::<Vec<_>>();
    if applied != expected_versions {
        return Err(StorageError::Migration(format!(
            "SeaORM migration ledger differs from deploy manifest: \
             applied={applied:?} expected={expected_versions:?}"
        )));
    }

    let audit = sqlx::query(
        "SELECT version, checksum_algorithm, checksum, artifact_length, migration_engine \
         FROM schema_migration_audit ORDER BY version",
    )
    .fetch_all(pool)
    .await
    .map_err(migration_error("read migration checksum ledger"))?
    .into_iter()
    .map(|row| {
        let version = row.try_get::<String, _>("version")?;
        Ok((
            version,
            (
                row.try_get::<String, _>("checksum_algorithm")?,
                row.try_get::<String, _>("checksum")?,
                row.try_get::<i64, _>("artifact_length")?,
                row.try_get::<String, _>("migration_engine")?,
            ),
        ))
    })
    .collect::<Result<BTreeMap<_, _>, Error>>()
    .map_err(migration_error("decode migration checksum ledger"))?;
    if audit.len() != expected.migrations.len() {
        return Err(StorageError::Migration(format!(
            "migration checksum ledger cardinality differs: actual={} expected={}",
            audit.len(),
            expected.migrations.len()
        )));
    }
    for migration in &expected.migrations {
        let Some((algorithm, checksum, artifact_length, engine)) = audit.get(&migration.version)
        else {
            return Err(StorageError::Migration(format!(
                "migration `{}` has no checksum audit record",
                migration.version
            )));
        };
        if algorithm != &expected.checksum_algorithm
            || checksum != &migration.checksum
            || artifact_length != &migration.artifact_length
            || engine != &expected.migration_engine
        {
            return Err(StorageError::Migration(format!(
                "migration `{}` differs from the immutable deploy manifest",
                migration.version
            )));
        }
    }
    Ok(())
}

fn migration_error(context: &'static str) -> impl FnOnce(Error) -> StorageError {
    move |error| StorageError::Migration(format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::expected_migration_manifest;

    #[test]
    fn migration_manifest_is_valid() {
        let manifest = expected_migration_manifest().expect("valid migration manifest");
        assert_eq!(manifest.migrations.len(), 1);
        assert_eq!(manifest.migrations[0].checksum.len(), 64);
    }
}
