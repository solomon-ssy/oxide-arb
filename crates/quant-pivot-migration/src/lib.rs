//! Deploy-only `SeaORM` `PostgreSQL` migrator with immutable artifact checksums.

use quant_pivot_allocator as _;

mod audit;
pub mod migrations;
#[expect(
    clippy::derive_partial_eq_without_eq,
    clippy::enum_variant_names,
    clippy::struct_field_names,
    reason = "the immutable SeaORM CLI snapshot preserves generated model and enum names; SeaORM 2.0 intentionally removes Eq from ModelEx"
)]
mod snapshots;

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use blake3::Hasher;
use migrations::Migrator;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{config::PostgresConfig, types::SCHEMA_MUTATION_ADVISORY_LOCK_KEY};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, FromQueryResult, Statement};
use sea_orm_migration::MigratorTrait;
use serde::Serialize;
use sqlx::{Connection, Error, PgConnection, PgPool};
use tokio::{
    sync::Mutex,
    task::JoinHandle,
    time::{MissedTickBehavior, interval, timeout},
};
use tokio_util::sync::CancellationToken;

const CHECKSUM_DOMAIN: &str = "quant-pivot/postgres-migration/v1";
const CHECKSUM_ALGORITHM: &str = "blake3-256";
const MIGRATION_ENGINE: &str = "sea-orm-migration/2.0.0";
const PREPRODUCTION_DATABASE: &str = "quant_pivot";
const PREPRODUCTION_DATABASE_USER: &str = "quant_pivot";
const SCHEMA_MUTATION_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
const SCHEMA_MUTATION_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(2);
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

/// Read-only inventory used by the guarded preproduction reset plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PreproductionPostgresInventory {
    pub database_exists: bool,
    pub object_count: i64,
    pub connection_count: i64,
}

/// Inspect only the exact project-owned preproduction database.
pub async fn inspect_preproduction_postgres(
    config: &PostgresConfig,
) -> Result<PreproductionPostgresInventory, StorageError> {
    validate_preproduction_postgres_target(config)?;
    let maintenance_url = config
        .try_database_url("postgres")
        .map_err(|error| StorageError::Migration(format!("build PG maintenance URL: {error}")))?;
    let maintenance = PgPool::connect(&maintenance_url)
        .await
        .map_err(migration_error("connect PostgreSQL maintenance database"))?;
    let database_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
    )
    .bind(PREPRODUCTION_DATABASE)
    .fetch_one(&maintenance)
    .await
    .map_err(migration_error("inspect PostgreSQL target database"))?;
    if !database_exists {
        maintenance.close().await;
        return Ok(PreproductionPostgresInventory {
            database_exists: false,
            object_count: 0,
            connection_count: 0,
        });
    }
    let connection_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint FROM pg_stat_activity \
             WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(PREPRODUCTION_DATABASE)
    .fetch_one(&maintenance)
    .await
    .map_err(migration_error("count PostgreSQL target connections"))?;
    let target_url = config
        .try_connection_url()
        .map_err(|error| StorageError::Migration(format!("build PG target URL: {error}")))?;
    let target = PgPool::connect(&target_url)
        .await
        .map_err(migration_error("connect PostgreSQL target database"))?;
    let object_count = sqlx::query_scalar::<_, i64>(
        "WITH objects AS (\
             SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f') \
             UNION ALL SELECT t.oid FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace \
             WHERE n.nspname = 'public' AND t.typtype IN ('e', 'd', 'r') \
             UNION ALL SELECT p.oid FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = 'public' \
             UNION ALL SELECT g.oid FROM pg_trigger g JOIN pg_class c ON c.oid = g.tgrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' AND NOT g.tgisinternal) \
             SELECT COUNT(*)::bigint FROM objects",
    )
    .fetch_one(&target)
    .await
    .map_err(migration_error("count PostgreSQL target objects"))?;
    target.close().await;
    maintenance.close().await;
    Ok(PreproductionPostgresInventory {
        database_exists,
        object_count,
        connection_count,
    })
}

fn validate_preproduction_postgres_target(config: &PostgresConfig) -> Result<(), StorageError> {
    if config.database != PREPRODUCTION_DATABASE
        || config.user != PREPRODUCTION_DATABASE_USER
        || config.schema != "public"
    {
        return Err(StorageError::Migration(format!(
            "preproduction reset only permits database `{PREPRODUCTION_DATABASE}`, user `{PREPRODUCTION_DATABASE_USER}`, schema `public`"
        )));
    }
    Ok(())
}

/// Dedicated session-scoped lease shared by every schema mutation and reset.
pub struct SchemaMutationLease {
    connection: Option<Arc<Mutex<PgConnection>>>,
    backend_pid: i32,
    lost: CancellationToken,
    shutdown: CancellationToken,
    heartbeat: Option<JoinHandle<()>>,
}

impl SchemaMutationLease {
    /// Wait until the canonical coordination session is lost.
    pub async fn cancelled(&self) {
        self.lost.cancelled().await;
    }

    /// Fail closed when the heartbeat can no longer prove lease ownership.
    pub fn ensure_active(&self) -> Result<(), StorageError> {
        if self.lost.is_cancelled() {
            Err(StorageError::Migration(
                "canonical PostgreSQL schema mutation lease was lost".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    /// `PostgreSQL` session identifier exposed for lease diagnostics and
    /// deterministic lease-loss integration tests.
    #[must_use]
    pub const fn backend_pid(&self) -> i32 {
        self.backend_pid
    }

    /// Destructively recreate only the exact project-owned preproduction database.
    ///
    /// The same maintenance session owns the advisory lease, performs the final
    /// active-session check, and executes the DDL. The inspection therefore does
    /// not create a target-database connection that can race with the subsequent
    /// `DROP DATABASE`. Unknown sessions are never terminated; any such session
    /// keeps the operation fail-closed.
    pub async fn reset_preproduction_postgres(
        &self,
        config: &PostgresConfig,
    ) -> Result<(), StorageError> {
        validate_preproduction_postgres_target(config)?;
        self.ensure_active()?;
        let connection = self.connection.as_ref().ok_or_else(|| {
            StorageError::Migration(
                "PostgreSQL schema mutation lease connection is absent".to_owned(),
            )
        })?;
        let mut connection = connection.lock().await;
        self.ensure_active()?;
        let connection_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::bigint FROM pg_stat_activity WHERE datname = $1",
        )
        .bind(PREPRODUCTION_DATABASE)
        .fetch_one(&mut *connection)
        .await
        .map_err(migration_error(
            "count PostgreSQL target connections before reset",
        ))?;
        if connection_count != 0 {
            return Err(StorageError::Migration(format!(
                "{connection_count} PostgreSQL target connections remain; stop their owners before reset"
            )));
        }
        sqlx::query("DROP DATABASE IF EXISTS quant_pivot")
            .execute(&mut *connection)
            .await
            .map_err(migration_error("drop PostgreSQL preproduction database"))?;
        sqlx::query("CREATE DATABASE quant_pivot OWNER quant_pivot")
            .execute(&mut *connection)
            .await
            .map_err(migration_error("create PostgreSQL preproduction database"))?;
        drop(connection);
        self.ensure_active()
    }
}

impl Drop for SchemaMutationLease {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Acquire the cross-system schema mutation lease on the fixed `postgres`
/// coordination database using the single configured `PostgreSQL` identity.
pub async fn acquire_schema_mutation_lease(
    config: &PostgresConfig,
) -> Result<SchemaMutationLease, StorageError> {
    let maintenance_url = config.try_database_url("postgres").map_err(|error| {
        StorageError::Migration(format!("build PostgreSQL coordination URL: {error}"))
    })?;
    let mut connection = PgConnection::connect(&maintenance_url).await.map_err(|_| {
        StorageError::Connection(
            "connect canonical PostgreSQL schema mutation coordination database failed".to_owned(),
        )
    })?;
    let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
        .bind(SCHEMA_MUTATION_ADVISORY_LOCK_KEY)
        .fetch_one(&mut connection)
        .await
        .map_err(|_| {
            StorageError::Connection(
                "acquire canonical PostgreSQL schema mutation lease failed".to_owned(),
            )
        })?;
    if !acquired {
        return Err(StorageError::Migration(
            "schema mutation or pre-production reset already holds the canonical schema mutation lease".to_owned(),
        ));
    }
    let backend_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut connection)
        .await
        .map_err(|_| {
            StorageError::Connection(
                "inspect PostgreSQL schema mutation lease session failed".to_owned(),
            )
        })?;

    let connection = Arc::new(Mutex::new(connection));
    let lost = CancellationToken::new();
    let shutdown = CancellationToken::new();
    let heartbeat_connection = Arc::clone(&connection);
    let heartbeat_lost = lost.clone();
    let heartbeat_shutdown = shutdown.clone();
    let heartbeat = tokio::spawn(async move {
        let mut ticker = interval(SCHEMA_MUTATION_HEARTBEAT_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            tokio::select! {
                () = heartbeat_shutdown.cancelled() => break,
                _ = ticker.tick() => {
                    let mut connection = heartbeat_connection.lock().await;
                    let heartbeat_result = timeout(
                        SCHEMA_MUTATION_HEARTBEAT_TIMEOUT,
                        sqlx::query_scalar::<_, i32>("SELECT 1")
                        .fetch_one(&mut *connection),
                    )
                    .await;
                    drop(connection);
                    if !matches!(heartbeat_result, Ok(Ok(_))) {
                        heartbeat_lost.cancel();
                        break;
                    }
                }
            }
        }
    });

    Ok(SchemaMutationLease {
        connection: Some(connection),
        backend_pid,
        lost,
        shutdown,
        heartbeat: Some(heartbeat),
    })
}

impl SchemaMutationLease {
    /// Release the canonical schema mutation lease after guarded work completes.
    pub async fn release_schema_mutation_lease(mut self) -> Result<(), StorageError> {
        self.shutdown.cancel();
        if let Some(heartbeat) = self.heartbeat.take() {
            heartbeat.await.map_err(|_| {
                StorageError::Connection(
                    "PostgreSQL schema mutation heartbeat task failed".to_owned(),
                )
            })?;
        }
        self.ensure_active()?;
        let connection = self.connection.take().ok_or_else(|| {
            StorageError::Migration(
                "PostgreSQL schema mutation lease connection is absent".to_owned(),
            )
        })?;
        let connection = Arc::try_unwrap(connection).map_err(|_| {
            StorageError::Migration("PostgreSQL schema mutation lease is still borrowed".to_owned())
        })?;
        let mut connection = connection.into_inner();
        let released = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
            .bind(SCHEMA_MUTATION_ADVISORY_LOCK_KEY)
            .fetch_one(&mut connection)
            .await
            .map_err(|_| {
                StorageError::Connection(
                    "release canonical PostgreSQL schema mutation lease failed".to_owned(),
                )
            })?;
        connection.close().await.map_err(|_| {
            StorageError::Connection(
                "close PostgreSQL schema mutation lease session failed".to_owned(),
            )
        })?;
        if released {
            Ok(())
        } else {
            Err(StorageError::Migration(
                "canonical PostgreSQL schema mutation lease was not held at release".to_owned(),
            ))
        }
    }
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

/// Apply pending migrations while holding the canonical cross-system lease.
pub async fn apply(config: &PostgresConfig, db: &DatabaseConnection) -> Result<(), StorageError> {
    let lease = acquire_schema_mutation_lease(config).await?;
    let apply_result = tokio::select! {
        result = apply_under_lease(db, &lease) => result,
        () = lease.cancelled() => Err(StorageError::Migration(
            "canonical PostgreSQL schema mutation lease was lost during migration".to_owned(),
        )),
    };
    let release_result = lease.release_schema_mutation_lease().await;
    match (apply_result, release_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), _) | (Ok(()), Err(error)) => Err(error),
    }
}

/// Apply `PostgreSQL` migrations while a caller-owned canonical lease is held.
pub async fn apply_under_lease(
    db: &DatabaseConnection,
    lease: &SchemaMutationLease,
) -> Result<(), StorageError> {
    lease.ensure_active()?;
    apply_locked(db).await?;
    lease.ensure_active()
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
    let mut hasher = Hasher::new();
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

async fn relation_exists(db: &impl ConnectionTrait, relation: &str) -> Result<bool, StorageError> {
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

fn migration_error(context: &'static str) -> impl FnOnce(Error) -> StorageError {
    move |error| StorageError::Migration(format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{
        CHECKED_MIGRATION_MANIFEST, CHECKSUM_ALGORITHM, expected_migrations, render_manifest,
    };

    #[test]
    fn compiled_migration_artifacts_checksummed() {
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
    fn checked_migration_matches_artifacts() {
        let rendered = render_manifest().expect("migration manifest must render");

        assert_eq!(rendered, CHECKED_MIGRATION_MANIFEST);
    }
}
