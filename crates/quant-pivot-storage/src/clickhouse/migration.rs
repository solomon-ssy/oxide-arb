//! Versioned `ClickHouse` startup migrations and read-only runtime verification.

use std::collections::{BTreeMap, BTreeSet};

use clickhouse::Row;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{config::ClickHouseConfig, hashing::CanonicalDigest};
use serde::Deserialize;
use tracing::info;

use crate::clickhouse::{ensure, schema};

const MIGRATION_TABLE: &str = "quant_pivot_schema_migration";
const MIGRATION_CLAIM_PREFIX: &str = "quant_pivot_schema_migration_claim_";
const MIGRATION_TABLE_DDL: &str = "CREATE TABLE IF NOT EXISTS quant_pivot_schema_migration (
    version UInt32,
    name String,
    checksum String,
    applied_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(applied_at)
ORDER BY version";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseSchemaMigrationInfo {
    pub version: u32,
    pub name: &'static str,
    pub checksum: String,
    pub safety: ClickHouseMigrationSafety,
}

/// Operational class assigned to every immutable schema migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClickHouseMigrationSafety {
    /// Idempotent metadata DDL that is safe to resume and run concurrently.
    OnlineSafe,
    /// Data rewrites, backfills, key changes, or destructive lifecycle DDL.
    OfflineRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseSchemaPlan {
    pub database_exists: bool,
    pub migration_ledger_exists: bool,
    pub applied_versions: Vec<u32>,
    pub pending_migrations: Vec<ClickHouseSchemaMigrationInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseSchemaStatus {
    pub current_version: u32,
    pub required_object_count: usize,
}

#[derive(Debug, Clone, Copy)]
struct MigrationSpec {
    version: u32,
    name: &'static str,
    safety: ClickHouseMigrationSafety,
    sources: &'static [&'static str],
    expected_checksum: &'static str,
    destructive_empty_tables: &'static [&'static str],
}

impl MigrationSpec {
    fn info(self) -> ClickHouseSchemaMigrationInfo {
        ClickHouseSchemaMigrationInfo {
            version: self.version,
            name: self.name,
            checksum: self.expected_checksum.to_owned(),
            safety: self.safety,
        }
    }

    fn computed_checksum(self) -> String {
        let mut framed = Vec::new();
        for source in self.sources {
            framed.extend_from_slice(&(source.len() as u64).to_le_bytes());
            framed.extend_from_slice(source.as_bytes());
        }
        CanonicalDigest::prefixed_bytes(&framed)
    }

    fn statements(self) -> Vec<String> {
        self.sources
            .iter()
            .flat_map(|source| schema::split_statements(source))
            .collect()
    }
}

const fn migrations() -> [MigrationSpec; 2] {
    [
        MigrationSpec {
            version: 1,
            name: "cloud_baseline",
            safety: ClickHouseMigrationSafety::OnlineSafe,
            sources: schema::BASELINE_SOURCES,
            expected_checksum: "blake3:4dcfdaaa484b6d4997d9f60007e1c24d8c5c649e8718e5cf12c757a4555cf682",
            destructive_empty_tables: &[],
        },
        MigrationSpec {
            version: 2,
            name: "report_lifecycle_v2",
            safety: ClickHouseMigrationSafety::OfflineRequired,
            sources: schema::REPORT_LIFECYCLE_V2_SOURCES,
            expected_checksum: "blake3:c831fbb3fb7719dc19138baa4dfa396f2edfe86f4bc17889e9b01e82aa2bbe57",
            destructive_empty_tables: &[
                "quant_recommendation_event",
                "quant_recommendation_attribution_event",
            ],
        },
    ]
}

/// Build a read-only deployment plan. The target database is never created.
pub async fn plan_schema(config: &ClickHouseConfig) -> Result<ClickHouseSchemaPlan, StorageError> {
    validate_migration_registry()?;
    if !ensure::database_exists(config).await? {
        return Ok(ClickHouseSchemaPlan {
            database_exists: false,
            migration_ledger_exists: false,
            applied_versions: Vec::new(),
            pending_migrations: migrations().into_iter().map(MigrationSpec::info).collect(),
        });
    }

    let client = client(config);
    let names = schema_object_names(&client).await?;
    let migration_ledger_exists = names.contains(MIGRATION_TABLE);
    if !migration_ledger_exists {
        reject_unmanaged_schema(&names)?;
        return Ok(ClickHouseSchemaPlan {
            database_exists: true,
            migration_ledger_exists: false,
            applied_versions: Vec::new(),
            pending_migrations: migrations().into_iter().map(MigrationSpec::info).collect(),
        });
    }

    build_plan(&client, true).await
}

/// Apply pending online-safe schema migrations with deploy-only credentials.
///
/// Startup migration deliberately accepts concurrent application instances:
/// each statement in this class must be idempotent, a per-version atomic DDL
/// claim rejects checksum conflicts before migration DDL runs, and an
/// interrupted instance can resume from the same immutable source.
pub async fn apply_online_schema_migrations(
    config: &ClickHouseConfig,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    apply_schema_migrations(config, false).await
}

/// Apply all pending migrations during an explicit offline maintenance window.
///
/// Destructive migrations additionally prove every declared source table is
/// empty before executing. This command never backfills or preserves legacy
/// rows; a non-empty table blocks the clean-slate rollout.
pub async fn apply_offline_schema_migrations(
    config: &ClickHouseConfig,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    apply_schema_migrations(config, true).await
}

async fn apply_schema_migrations(
    config: &ClickHouseConfig,
    allow_offline: bool,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    validate_migration_registry()?;
    ensure::ensure_database(config).await?;
    let client = client(config);
    let names = schema_object_names(&client).await?;
    if !names.contains(MIGRATION_TABLE) {
        reject_unmanaged_schema(&names)?;
        client.query(MIGRATION_TABLE_DDL).execute().await?;
    }

    let plan = build_plan(&client, true).await?;
    for migration in &plan.pending_migrations {
        let spec = migrations()
            .into_iter()
            .find(|candidate| candidate.version == migration.version)
            .ok_or_else(|| {
                StorageError::Migration(format!(
                    "ClickHouse migration {} is not compiled into this binary",
                    migration.version
                ))
            })?;
        if migration.safety == ClickHouseMigrationSafety::OfflineRequired {
            if !allow_offline {
                return Err(StorageError::Migration(format!(
                    "ClickHouse migration {} ({}) requires an explicit offline maintenance rollout",
                    migration.version, migration.name
                )));
            }
            verify_destructive_empty_tables(&client, spec).await?;
        }
        claim_migration(&client, migration).await?;
        for statement in spec.statements() {
            client.query(&statement).execute().await?;
        }
        client
            .query(
                "INSERT INTO quant_pivot_schema_migration (version, name, checksum) \
                 VALUES (?, ?, ?)",
            )
            .bind(spec.version)
            .bind(spec.name)
            .bind(&migration.checksum)
            .execute()
            .await?;
        info!(
            version = spec.version,
            migration = spec.name,
            checksum = %migration.checksum,
            "ClickHouse schema migration applied"
        );
    }

    verify_schema_client(&client).await
}

async fn verify_destructive_empty_tables(
    client: &clickhouse::Client,
    migration: MigrationSpec,
) -> Result<(), StorageError> {
    if migration.destructive_empty_tables.is_empty() {
        return Err(StorageError::Migration(format!(
            "offline migration {} ({}) has no explicit destructive-table preconditions",
            migration.version, migration.name
        )));
    }
    for table in migration.destructive_empty_tables {
        if table_exists(client, table).await?
            && client
                .query(&format!("SELECT 1 FROM {table} LIMIT 1"))
                .fetch_optional::<u8>()
                .await?
                .is_some()
        {
            return Err(StorageError::Migration(format!(
                "offline migration {} ({}) requires empty table `{table}`; clean-slate activation refuses to discard persisted rows",
                migration.version, migration.name
            )));
        }
    }
    Ok(())
}

/// Verify the deployed migration ledger and structural runtime contract.
pub async fn verify_schema(
    config: &ClickHouseConfig,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    validate_migration_registry()?;
    if !ensure::database_exists(config).await? {
        return Err(StorageError::Migration(format!(
            "ClickHouse database `{}` does not exist and automatic online migration did not initialize it",
            config.database
        )));
    }
    verify_schema_client(&client(config)).await
}

fn validate_migration_registry() -> Result<(), StorageError> {
    let specs = migrations();
    let mut names = BTreeSet::new();
    for (index, spec) in specs.into_iter().enumerate() {
        let expected_version = u32::try_from(index + 1).map_err(|error| {
            StorageError::Migration(format!("ClickHouse migration registry overflow: {error}"))
        })?;
        if spec.version != expected_version {
            return Err(StorageError::Migration(format!(
                "ClickHouse migration registry must be contiguous from version 1; found version {} at position {expected_version}",
                spec.version
            )));
        }
        if !names.insert(spec.name) {
            return Err(StorageError::Migration(format!(
                "ClickHouse migration name `{}` is duplicated",
                spec.name
            )));
        }
        let computed_checksum = spec.computed_checksum();
        if computed_checksum != spec.expected_checksum {
            return Err(StorageError::Migration(format!(
                "ClickHouse migration {} ({}) source was modified after publication: computed checksum `{computed_checksum}`, immutable checksum `{}`; restore this version and add a new migration",
                spec.version, spec.name, spec.expected_checksum
            )));
        }
        let statements = spec.statements();
        if statements.is_empty() {
            return Err(StorageError::Migration(format!(
                "ClickHouse migration {} has no executable statements",
                spec.version
            )));
        }
        if spec.safety == ClickHouseMigrationSafety::OnlineSafe {
            for statement in statements {
                validate_online_safe_statement(spec, &statement)?;
            }
        }
    }
    Ok(())
}

fn validate_online_safe_statement(
    migration: MigrationSpec,
    statement: &str,
) -> Result<(), StorageError> {
    let normalized = statement
        .split_ascii_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase();
    let resumable_create = normalized.starts_with("CREATE TABLE IF NOT EXISTS ")
        || normalized.starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS ")
        || normalized.starts_with("CREATE VIEW IF NOT EXISTS ");
    let resumable_add_column =
        normalized.starts_with("ALTER TABLE ") && normalized.contains(" ADD COLUMN IF NOT EXISTS ");
    let forbidden_alter = [
        " DROP ",
        " DELETE ",
        " UPDATE ",
        " MODIFY ",
        " MATERIALIZE ",
        " MOVE ",
        " RENAME ",
        " EXCHANGE ",
        " DETACH ",
        " TRUNCATE ",
        " OPTIMIZE ",
        " POPULATE ",
    ]
    .into_iter()
    .find(|token| normalized.contains(token));
    let unsafe_create = resumable_create && normalized.contains(" POPULATE ");
    let unsafe_alter = resumable_add_column && forbidden_alter.is_some();
    if !(resumable_create || resumable_add_column) || unsafe_create || unsafe_alter {
        return Err(StorageError::Migration(format!(
            "ClickHouse migration {} ({}) is classified OnlineSafe but contains non-resumable or potentially destructive DDL: `{}`",
            migration.version,
            migration.name,
            normalized.chars().take(160).collect::<String>()
        )));
    }
    Ok(())
}

pub(super) async fn verify_schema_client(
    client: &clickhouse::Client,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    if !table_exists(client, MIGRATION_TABLE).await? {
        return Err(StorageError::Migration(
            "ClickHouse migration ledger is absent and automatic online migration did not initialize it"
                .to_owned(),
        ));
    }
    let plan = build_plan(client, true).await?;
    if !plan.pending_migrations.is_empty() {
        let versions = plan
            .pending_migrations
            .iter()
            .map(|migration| migration.version.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(StorageError::Migration(format!(
            "ClickHouse schema is behind; pending migration versions: {versions}"
        )));
    }
    verify_structure(client).await?;
    let current_version = plan.applied_versions.last().copied().ok_or_else(|| {
        StorageError::Migration("ClickHouse migration ledger is empty".to_owned())
    })?;
    Ok(ClickHouseSchemaStatus {
        current_version,
        required_object_count: schema::REQUIRED_SCHEMA_OBJECTS.len(),
    })
}

async fn claim_migration(
    client: &clickhouse::Client,
    migration: &ClickHouseSchemaMigrationInfo,
) -> Result<(), StorageError> {
    let table = format!("{MIGRATION_CLAIM_PREFIX}{:06}", migration.version);
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {table} (claimed UInt8) \
         ENGINE = MergeTree ORDER BY tuple() COMMENT '{}'",
        migration.checksum
    );
    client.query(&ddl).execute().await?;
    let observed = client
        .query(
            "SELECT comment FROM system.tables \
             WHERE database = currentDatabase() AND name = ?",
        )
        .bind(&table)
        .fetch_one::<String>()
        .await?;
    if observed != migration.checksum {
        return Err(StorageError::Migration(format!(
            "ClickHouse migration {} was concurrently claimed with checksum `{observed}`, expected `{}`; refuse to execute conflicting DDL",
            migration.version, migration.checksum
        )));
    }
    Ok(())
}

async fn verify_migration_claim(
    client: &clickhouse::Client,
    migration: &ClickHouseSchemaMigrationInfo,
) -> Result<(), StorageError> {
    let table = format!("{MIGRATION_CLAIM_PREFIX}{:06}", migration.version);
    let observed = client
        .query(
            "SELECT comment FROM system.tables \
             WHERE database = currentDatabase() AND name = ?",
        )
        .bind(&table)
        .fetch_optional::<String>()
        .await?
        .ok_or_else(|| {
            StorageError::Migration(format!(
                "ClickHouse migration {} has no immutable DDL claim",
                migration.version
            ))
        })?;
    if observed != migration.checksum {
        return Err(StorageError::Migration(format!(
            "ClickHouse migration {} claim checksum is `{observed}`, expected `{}`",
            migration.version, migration.checksum
        )));
    }
    Ok(())
}

async fn build_plan(
    client: &clickhouse::Client,
    migration_ledger_exists: bool,
) -> Result<ClickHouseSchemaPlan, StorageError> {
    let applied = applied_migrations(client).await?;
    let specs = migrations();
    let known_versions = specs
        .iter()
        .map(|migration| migration.version)
        .collect::<BTreeSet<_>>();
    for migration in &applied {
        if migration.variants != 1 {
            return Err(StorageError::Migration(format!(
                "ClickHouse migration ledger contains {} distinct definitions for version {}",
                migration.variants, migration.version
            )));
        }
        if !known_versions.contains(&migration.version) {
            return Err(StorageError::Migration(format!(
                "ClickHouse schema version {} is newer than or unknown to this binary",
                migration.version
            )));
        }
        let expected = specs
            .iter()
            .find(|spec| spec.version == migration.version)
            .map(|spec| spec.info())
            .ok_or_else(|| {
                StorageError::Migration(format!(
                    "ClickHouse migration {} has no compiled specification",
                    migration.version
                ))
            })?;
        if migration.migration_name != expected.name
            || migration.migration_checksum != expected.checksum
        {
            return Err(StorageError::Migration(format!(
                "ClickHouse migration {} checksum/name differs from the immutable schema source",
                migration.version
            )));
        }
        verify_migration_claim(client, &expected).await?;
    }

    let applied_versions = applied
        .iter()
        .map(|migration| migration.version)
        .collect::<Vec<_>>();
    if let Some(highest_applied) = applied_versions.last().copied() {
        let missing_predecessors = specs
            .iter()
            .filter(|spec| {
                spec.version <= highest_applied && !applied_versions.contains(&spec.version)
            })
            .map(|spec| spec.version.to_string())
            .collect::<Vec<_>>();
        if !missing_predecessors.is_empty() {
            return Err(StorageError::Migration(format!(
                "ClickHouse migration ledger has a version gap before {highest_applied}; missing versions: {}",
                missing_predecessors.join(", ")
            )));
        }
    }
    let pending_migrations = specs
        .into_iter()
        .filter(|spec| !applied_versions.contains(&spec.version))
        .map(MigrationSpec::info)
        .collect();
    Ok(ClickHouseSchemaPlan {
        database_exists: true,
        migration_ledger_exists,
        applied_versions,
        pending_migrations,
    })
}

async fn applied_migrations(
    client: &clickhouse::Client,
) -> Result<Vec<AppliedMigrationRow>, StorageError> {
    client
        .query(
            "SELECT version, argMax(name, applied_at) AS migration_name, \
             argMax(checksum, applied_at) AS migration_checksum, \
             uniqExact(tuple(name, checksum)) AS variants \
             FROM quant_pivot_schema_migration \
             GROUP BY version ORDER BY version",
        )
        .fetch_all::<AppliedMigrationRow>()
        .await
        .map_err(Into::into)
}

async fn verify_structure(client: &clickhouse::Client) -> Result<(), StorageError> {
    let rows = client
        .query(
            "SELECT name, engine, partition_key, create_table_query \
             FROM system.tables WHERE database = currentDatabase()",
        )
        .fetch_all::<TableMetadataRow>()
        .await?;
    let by_name = rows
        .into_iter()
        .map(|row| (row.name.clone(), row))
        .collect::<BTreeMap<_, _>>();

    let missing = schema::REQUIRED_SCHEMA_OBJECTS
        .iter()
        .filter(|name| !by_name.contains_key(**name))
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(StorageError::Migration(format!(
            "ClickHouse schema is missing required objects: {}",
            missing.join(", ")
        )));
    }
    for name in schema::FORBIDDEN_SCHEMA_OBJECTS {
        if by_name.contains_key(name) {
            return Err(StorageError::Migration(format!(
                "ClickHouse contains forbidden compatibility object `{name}`; recreate the pre-production database"
            )));
        }
    }
    let materialized_view = by_name.get("book_microstructure_1m_mv").ok_or_else(|| {
        StorageError::Migration(
            "ClickHouse canonical microstructure materialized view is absent".to_owned(),
        )
    })?;
    if materialized_view.engine != "MaterializedView" {
        return Err(StorageError::Migration(
            "ClickHouse object `book_microstructure_1m_mv` is not a MaterializedView".to_owned(),
        ));
    }
    for spec in schema::RAW_HISTORY_TABLES {
        let table = by_name.get(spec.table).ok_or_else(|| {
            StorageError::Migration(format!(
                "ClickHouse raw-history table `{}` is absent",
                spec.table
            ))
        })?;
        if table.partition_key != spec.partition_key {
            return Err(StorageError::Migration(format!(
                "ClickHouse table `{}` partition key is `{}`, expected `{}`",
                spec.table, table.partition_key, spec.partition_key
            )));
        }
        if schema::extract_table_ttl(&table.create_table_query).is_some() {
            return Err(StorageError::Migration(format!(
                "ClickHouse raw-history table `{}` has an unmanaged table TTL",
                spec.table
            )));
        }
    }
    Ok(())
}

fn reject_unmanaged_schema(names: &BTreeSet<String>) -> Result<(), StorageError> {
    let managed = schema::REQUIRED_SCHEMA_OBJECTS
        .iter()
        .chain(schema::FORBIDDEN_SCHEMA_OBJECTS.iter())
        .filter(|name| names.contains(**name))
        .copied()
        .collect::<Vec<_>>();
    if managed.is_empty() {
        return Ok(());
    }
    Err(StorageError::Migration(format!(
        "ClickHouse database contains unmanaged pre-baseline objects ({}); automatic adoption is intentionally unsupported, so recreate this pre-production database once and restart the application",
        managed.join(", ")
    )))
}

async fn schema_object_names(
    client: &clickhouse::Client,
) -> Result<BTreeSet<String>, StorageError> {
    client
        .query("SELECT name FROM system.tables WHERE database = currentDatabase()")
        .fetch_all::<TableNameRow>()
        .await
        .map(|rows| rows.into_iter().map(|row| row.name).collect())
        .map_err(Into::into)
}

async fn table_exists(client: &clickhouse::Client, table: &str) -> Result<bool, StorageError> {
    let count = client
        .query(
            "SELECT count() FROM system.tables \
             WHERE database = currentDatabase() AND name = ?",
        )
        .bind(table)
        .fetch_one::<u64>()
        .await?;
    Ok(count == 1)
}

fn client(config: &ClickHouseConfig) -> clickhouse::Client {
    clickhouse::Client::default()
        .with_url(&config.url)
        .with_database(&config.database)
        .with_user(&config.user)
        .with_password(&config.password)
}

#[derive(Debug, Row, Deserialize)]
struct AppliedMigrationRow {
    version: u32,
    migration_name: String,
    migration_checksum: String,
    variants: u64,
}

#[derive(Debug, Row, Deserialize)]
struct TableMetadataRow {
    name: String,
    engine: String,
    partition_key: String,
    create_table_query: String,
}

#[derive(Debug, Row, Deserialize)]
struct TableNameRow {
    name: String,
}

#[cfg(test)]
mod tests {
    use super::{
        ClickHouseMigrationSafety, MigrationSpec, validate_migration_registry,
        validate_online_safe_statement,
    };

    const SAFE_ADD_COLUMN: &[&str] = &["ALTER TABLE events ADD COLUMN IF NOT EXISTS source String"];
    const UNSAFE_TTL: &[&str] =
        &["ALTER TABLE events MODIFY TTL event_time + INTERVAL 30 DAY DELETE"];

    #[test]
    fn compiled_migration_registry_is_contiguous_and_valid() {
        validate_migration_registry().expect("compiled migration registry should be valid");
    }

    #[test]
    fn online_safe_accepts_resumable_add_column() {
        let migration = MigrationSpec {
            version: 2,
            name: "add_source",
            safety: ClickHouseMigrationSafety::OnlineSafe,
            sources: SAFE_ADD_COLUMN,
            expected_checksum: "test-only",
            destructive_empty_tables: &[],
        };
        validate_online_safe_statement(migration, SAFE_ADD_COLUMN[0])
            .expect("ADD COLUMN IF NOT EXISTS is resumable metadata DDL");
    }

    #[test]
    fn online_safe_rejects_ttl_and_destructive_ddl() {
        let migration = MigrationSpec {
            version: 2,
            name: "ttl_delete",
            safety: ClickHouseMigrationSafety::OnlineSafe,
            sources: UNSAFE_TTL,
            expected_checksum: "test-only",
            destructive_empty_tables: &[],
        };
        let error = validate_online_safe_statement(migration, UNSAFE_TTL[0])
            .expect_err("TTL migration must require an offline rollout");
        assert!(error.to_string().contains("potentially destructive DDL"));
    }
}
