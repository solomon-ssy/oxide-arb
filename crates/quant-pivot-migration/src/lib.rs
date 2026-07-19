//! Deploy-only `SeaORM` `PostgreSQL` migrator with immutable artifact checksums.

mod audit;
pub mod migrations;
#[expect(
    clippy::derive_partial_eq_without_eq,
    clippy::enum_variant_names,
    clippy::struct_field_names,
    reason = "the immutable SeaORM CLI snapshot preserves generated model and enum names; SeaORM rc.43 intentionally removes Eq from ModelEx"
)]
mod snapshots;

use std::collections::BTreeMap;

use quant_pivot_error::storage::StorageError;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, FromQueryResult, Statement};
use sea_orm_migration::MigratorTrait;
use serde::Serialize;

use migrations::Migrator;

const CHECKSUM_DOMAIN: &str = "quant-pivot/postgres-migration/v1";
const CHECKSUM_ALGORITHM: &str = "blake3-256";
const MIGRATION_ENGINE: &str = "sea-orm-migration/2.0.0-rc.43";
const DEPLOYMENT_LOCK_KEY: i64 = 7_293_770_821_154_641_591;
#[cfg(test)]
const CHECKED_MIGRATION_MANIFEST: &str = include_str!("../../../schema/postgres/migrations.json");

/// Immutable identity of one compiled migration artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MigrationSpec {
    pub version: String,
    pub checksum: String,
    pub artifact_length: i64,
}

#[derive(Serialize)]
struct MigrationManifest<'a> {
    format_version: u32,
    checksum_algorithm: &'a str,
    migration_engine: &'a str,
    migrations: &'a [MigrationSpec],
}

/// Read-only comparison between a target database and this deploy artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationPlan {
    pub migration_ledger_exists: bool,
    pub applied_versions: Vec<String>,
    pub pending_migrations: Vec<MigrationSpec>,
}

#[derive(Debug, FromQueryResult)]
struct AuditRow {
    version: String,
    checksum_algorithm: String,
    checksum: String,
    artifact_length: i64,
    migration_engine: String,
}

/// Inspect migration state without creating the `SeaORM` ledger or applying DDL.
pub async fn plan(db: &DatabaseConnection) -> Result<MigrationPlan, StorageError> {
    if !relation_exists(db, "seaql_migrations").await? {
        return Ok(MigrationPlan {
            migration_ledger_exists: false,
            applied_versions: Vec::new(),
            pending_migrations: migrations::specs(),
        });
    }

    let applied_versions = read_native_versions(db).await?;
    validate_known_versions(&applied_versions)?;
    if !applied_versions.is_empty() {
        verify_audit_rows(db, &applied_versions).await?;
    }
    let pending_migrations = migrations::specs()
        .into_iter()
        .filter(|spec| !applied_versions.contains(&spec.version))
        .collect();
    Ok(MigrationPlan {
        migration_ledger_exists: true,
        applied_versions,
        pending_migrations,
    })
}

/// Apply pending migrations one at a time under a deployment-wide advisory lock.
pub async fn apply(db: &DatabaseConnection) -> Result<(), StorageError> {
    let pool = db.get_postgres_connection_pool();
    let mut lock_connection = pool.acquire().await.map_err(migration_error(
        "acquire PostgreSQL deployment lock connection",
    ))?;
    let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
        .bind(DEPLOYMENT_LOCK_KEY)
        .fetch_one(&mut *lock_connection)
        .await
        .map_err(migration_error("acquire PostgreSQL deployment lock"))?;
    if !acquired {
        return Err(StorageError::Migration(
            "another PostgreSQL schema deployment holds the migration lock".to_owned(),
        ));
    }

    let apply_result = apply_locked(db).await;
    let unlock_result = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(DEPLOYMENT_LOCK_KEY)
        .fetch_one(&mut *lock_connection)
        .await
        .map_err(migration_error("release PostgreSQL deployment lock"));
    match (apply_result, unlock_result) {
        (Ok(()), Ok(true)) => Ok(()),
        (Ok(()), Ok(false)) => Err(StorageError::Migration(
            "PostgreSQL deployment lock was not held at release".to_owned(),
        )),
        (Err(error), _) | (Ok(()), Err(error)) => Err(error),
    }
}

async fn apply_locked(db: &DatabaseConnection) -> Result<(), StorageError> {
    if relation_exists(db, "seaql_migrations").await? {
        let applied_versions = read_native_versions(db).await?;
        validate_known_versions(&applied_versions)?;
        if !applied_versions.is_empty() {
            verify_audit_rows(db, &applied_versions).await?;
        }
    }

    Migrator::up(db, None)
        .await
        .map_err(|error| StorageError::Migration(error.to_string()))?;
    verify(db).await
}

/// Verify exact `SeaORM` ledger membership and project checksum audit rows.
pub async fn verify(db: &DatabaseConnection) -> Result<(), StorageError> {
    if !relation_exists(db, "seaql_migrations").await? {
        return Err(StorageError::Migration(
            "SeaORM migration ledger `seaql_migrations` does not exist".to_owned(),
        ));
    }
    if relation_exists(db, "_sqlx_migrations").await? {
        return Err(StorageError::Migration(
            "forbidden legacy migration ledger `_sqlx_migrations` exists".to_owned(),
        ));
    }
    let applied_versions = read_native_versions(db).await?;
    validate_complete_versions(&applied_versions)?;
    verify_audit_rows(db, &applied_versions).await
}

/// Return compiled migration specs for manifest generation and CI assertions.
#[must_use]
pub fn expected_migrations() -> Vec<MigrationSpec> {
    migrations::specs()
}

/// Render the runtime migration contract deterministically for source control.
pub fn render_manifest() -> Result<String, StorageError> {
    let migrations = expected_migrations();
    let manifest = MigrationManifest {
        format_version: 1,
        checksum_algorithm: CHECKSUM_ALGORITHM,
        migration_engine: MIGRATION_ENGINE,
        migrations: &migrations,
    };
    serde_json::to_string_pretty(&manifest)
        .map(|json| format!("{json}\n"))
        .map_err(|error| StorageError::Migration(format!("render migration manifest: {error}")))
}

fn migration_spec(version: &str, artifacts: &[&[u8]]) -> MigrationSpec {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CHECKSUM_DOMAIN.as_bytes());
    hasher.update(&[0]);
    hasher.update(version.as_bytes());
    hasher.update(&[0]);
    let mut artifact_length = 0_i64;
    for artifact in artifacts {
        let fragment_length = u64::try_from(artifact.len()).unwrap_or(u64::MAX);
        hasher.update(&fragment_length.to_be_bytes());
        hasher.update(artifact);
        artifact_length =
            artifact_length.saturating_add(i64::try_from(artifact.len()).unwrap_or(i64::MAX));
    }
    MigrationSpec {
        version: version.to_owned(),
        checksum: hasher.finalize().to_hex().to_string(),
        artifact_length,
    }
}

async fn relation_exists(db: &DatabaseConnection, relation: &str) -> Result<bool, StorageError> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT to_regclass($1) IS NOT NULL AS exists",
            [format!("public.{relation}").into()],
        ))
        .await
        .map_err(|error| StorageError::Migration(error.to_string()))?
        .ok_or_else(|| StorageError::Migration("PostgreSQL returned no catalog row".to_owned()))?;
    row.try_get::<bool>("", "exists")
        .map_err(|error| StorageError::Migration(error.to_string()))
}

async fn read_native_versions(db: &DatabaseConnection) -> Result<Vec<String>, StorageError> {
    let rows = db
        .query_all_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT version FROM seaql_migrations ORDER BY version",
        ))
        .await
        .map_err(|error| StorageError::Migration(error.to_string()))?;
    rows.into_iter()
        .map(|row| {
            row.try_get::<String>("", "version")
                .map_err(|error| StorageError::Migration(error.to_string()))
        })
        .collect()
}

async fn verify_audit_rows(
    db: &DatabaseConnection,
    applied_versions: &[String],
) -> Result<(), StorageError> {
    if !relation_exists(db, "schema_migration_audit").await? {
        return Err(StorageError::Migration(
            "migration checksum ledger `schema_migration_audit` does not exist".to_owned(),
        ));
    }
    let rows = AuditRow::find_by_statement(Statement::from_string(
        DbBackend::Postgres,
        "SELECT version, checksum_algorithm, checksum, artifact_length, migration_engine \
         FROM schema_migration_audit ORDER BY version",
    ))
    .all(db)
    .await
    .map_err(|error| StorageError::Migration(error.to_string()))?;
    let actual = rows
        .into_iter()
        .map(|row| (row.version.clone(), row))
        .collect::<BTreeMap<_, _>>();
    let expected = migrations::specs()
        .into_iter()
        .map(|spec| (spec.version.clone(), spec))
        .collect::<BTreeMap<_, _>>();

    if actual.len() != applied_versions.len() {
        return Err(StorageError::Migration(format!(
            "migration ledger cardinality mismatch: seaql={} audit={}",
            applied_versions.len(),
            actual.len()
        )));
    }
    for version in applied_versions {
        let row = actual.get(version).ok_or_else(|| {
            StorageError::Migration(format!(
                "migration `{version}` has no checksum audit record"
            ))
        })?;
        let spec = expected.get(version).ok_or_else(|| {
            StorageError::Migration(format!("unknown applied migration `{version}`"))
        })?;
        if row.checksum_algorithm != CHECKSUM_ALGORITHM
            || row.checksum != spec.checksum
            || row.artifact_length != spec.artifact_length
            || row.migration_engine != MIGRATION_ENGINE
        {
            return Err(StorageError::Migration(format!(
                "migration `{version}` differs from its immutable artifact"
            )));
        }
    }
    Ok(())
}

fn validate_known_versions(applied_versions: &[String]) -> Result<(), StorageError> {
    let expected = migrations::specs()
        .into_iter()
        .map(|spec| spec.version)
        .collect::<Vec<_>>();
    for version in applied_versions {
        if !expected.contains(version) {
            return Err(StorageError::Migration(format!(
                "applied PostgreSQL migration `{version}` is unknown to this deploy artifact"
            )));
        }
    }
    Ok(())
}

fn validate_complete_versions(applied_versions: &[String]) -> Result<(), StorageError> {
    validate_known_versions(applied_versions)?;
    let expected = migrations::specs()
        .into_iter()
        .map(|spec| spec.version)
        .collect::<Vec<_>>();
    if applied_versions != expected {
        return Err(StorageError::Migration(format!(
            "PostgreSQL migration ledger is incomplete: applied={applied_versions:?} expected={expected:?}"
        )));
    }
    Ok(())
}

fn migration_error(context: &'static str) -> impl FnOnce(sqlx::Error) -> StorageError {
    move |error| StorageError::Migration(format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{
        CHECKED_MIGRATION_MANIFEST, CHECKSUM_ALGORITHM, expected_migrations, render_manifest,
    };

    #[test]
    fn compiled_migration_artifacts_are_ordered_and_checksummed() {
        let migrations = expected_migrations();
        assert_eq!(migrations.len(), 1);
        assert_eq!(
            migrations
                .iter()
                .map(|migration| migration.version.as_str())
                .collect::<Vec<_>>(),
            ["m00000000_000001_bootstrap"]
        );
        assert!(
            migrations
                .iter()
                .all(|migration| migration.checksum.len() == 64)
        );
        assert!(
            migrations
                .iter()
                .all(|migration| migration.artifact_length > 0)
        );
        assert_eq!(CHECKSUM_ALGORITHM, "blake3-256");
    }

    #[test]
    fn checked_migration_manifest_matches_compiled_artifacts() {
        let rendered = render_manifest().expect("migration manifest must render");

        assert_eq!(rendered, CHECKED_MIGRATION_MANIFEST);
    }
}
