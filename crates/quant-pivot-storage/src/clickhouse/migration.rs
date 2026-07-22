//! Versioned `ClickHouse` startup migrations and read-only runtime verification.

use std::collections::{BTreeMap, BTreeSet};

use clickhouse::{Client, Row};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    config::ClickHouseConfig,
    hashing::CanonicalDigest,
    types::{ContentHash, ResearchSourceStorageKind, research_source_registry},
};
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

use crate::clickhouse::{
    ensure,
    query_limits::{CLICKHOUSE_SCHEMA_APPLY, CLICKHOUSE_SCHEMA_VERIFY},
    schema,
    schema::{BOOTSTRAP_SOURCES, REQUIRED_SCHEMA_OBJECTS},
};

const MIGRATION_TABLE: &str = "quant_pivot_schema_migration";
const MIGRATION_CLAIM_PREFIX: &str = "quant_pivot_schema_migration_claim_";
const DEPLOYMENT_LOCK_TABLE: &str = "quant_pivot_schema_deployment_lock";
const SCHEMA_MANIFEST_FORMAT_VERSION: u32 = 1;
const MINIMUM_CLICKHOUSE_MAJOR: u32 = 26;
const MINIMUM_CLICKHOUSE_MINOR: u32 = 1;
const EXPECTED_SCHEMA_MANIFEST: &str = include_str!("../../../../schema/clickhouse/manifest.json");
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
    OnlineMetadata,
    /// Offline metadata cleanup or additive work that does not destroy facts.
    OfflineNonDestructive,
    /// Data rewrites, key changes, or destructive lifecycle DDL.
    OfflineDestructive,
    /// Resumable offline rebuild that preserves validated rows and explicitly
    /// excludes one reproducible source with a known corrupt representation.
    OfflineRebuildableDataRepair,
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
    pub schema_fingerprint: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ClickHouseSchemaManifest {
    format_version: u32,
    objects: Vec<ClickHouseSchemaObjectManifest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ClickHouseSchemaObjectManifest {
    name: String,
    engine: String,
    engine_full: String,
    partition_key: String,
    sorting_key: String,
    primary_key: String,
    sampling_key: String,
    create_table_query: String,
    columns: Vec<ClickHouseColumnManifest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ClickHouseColumnManifest {
    position: u64,
    name: String,
    column_type: String,
    default_kind: String,
    default_expression: String,
    compression_codec: String,
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

const fn migrations() -> [MigrationSpec; 1] {
    [MigrationSpec {
        version: 1,
        name: "bootstrap",
        safety: ClickHouseMigrationSafety::OnlineMetadata,
        sources: BOOTSTRAP_SOURCES,
        expected_checksum: "blake3:60663c9ffc458f923d00297d2d4b37bad65948687147005077e238f553f11d8a",
        destructive_empty_tables: &[],
    }]
}

/// Content hash of the complete immutable `ClickHouse` schema migration contract.
#[must_use]
pub fn schema_contract_hash() -> String {
    let mut framed = Vec::new();
    for migration in migrations() {
        framed.extend_from_slice(&migration.version.to_le_bytes());
        framed.extend_from_slice(&(migration.expected_checksum.len() as u64).to_le_bytes());
        framed.extend_from_slice(migration.expected_checksum.as_bytes());
    }
    framed.extend_from_slice(&(EXPECTED_SCHEMA_MANIFEST.len() as u64).to_le_bytes());
    framed.extend_from_slice(EXPECTED_SCHEMA_MANIFEST.as_bytes());
    CanonicalDigest::prefixed_bytes(&framed)
}

/// Inspect the deployed managed objects and render the normalized semantic
/// manifest committed alongside the immutable migrations.
pub async fn render_schema_manifest(config: &ClickHouseConfig) -> Result<String, StorageError> {
    let client = client(config);
    verify_server_version(&client).await?;
    let manifest = inspect_schema_manifest(&client).await?;
    let mut rendered = serde_json::to_string_pretty(&manifest)
        .map_err(|error| StorageError::Migration(format!("render ClickHouse manifest: {error}")))?;
    rendered.push('\n');
    Ok(rendered)
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
    verify_server_version(&client).await?;
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

/// Apply pending online-safe schema migrations with the configured `ClickHouse` identity.
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
    verify_server_version(&client).await?;
    let lock_owner = Uuid::now_v7().to_string();
    acquire_deployment_lock(&client, &lock_owner).await?;
    let result = apply_schema_migrations_locked(&client, allow_offline).await;
    let release = release_deployment_lock(&client, &lock_owner).await;
    match (result, release) {
        (Ok(status), Ok(())) => Ok(status),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(StorageError::Migration(format!(
            "ClickHouse schema applied but deployment lock release failed: {error}"
        ))),
        (Err(apply_error), Err(release_error)) => Err(StorageError::Migration(format!(
            "ClickHouse schema apply failed: {apply_error}; deployment lock release also failed: {release_error}"
        ))),
    }
}

async fn apply_schema_migrations_locked(
    client: &Client,
    allow_offline: bool,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    let names = schema_object_names(client).await?;
    if !names.contains(MIGRATION_TABLE) {
        reject_unmanaged_schema(&names)?;
        CLICKHOUSE_SCHEMA_APPLY
            .query(client, MIGRATION_TABLE_DDL)
            .execute()
            .await?;
    }

    let plan = build_plan(client, true).await?;
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
        if migration.safety != ClickHouseMigrationSafety::OnlineMetadata {
            if !allow_offline {
                return Err(StorageError::Migration(format!(
                    "ClickHouse migration {} ({}) requires an explicit offline maintenance rollout",
                    migration.version, migration.name
                )));
            }
            if migration.safety == ClickHouseMigrationSafety::OfflineDestructive {
                verify_destructive_empty_tables(client, spec).await?;
            }
        }
        claim_migration(client, migration).await?;
        for statement in spec.statements() {
            CLICKHOUSE_SCHEMA_APPLY
                .query(client, &statement)
                .execute()
                .await?;
        }
        CLICKHOUSE_SCHEMA_APPLY
            .query(
                client,
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

    verify_schema_client_during_deployment(client).await
}

async fn acquire_deployment_lock(client: &Client, owner: &str) -> Result<(), StorageError> {
    let ddl = format!(
        "CREATE TABLE {DEPLOYMENT_LOCK_TABLE} (owner String) \
         ENGINE = TinyLog COMMENT '{owner}'"
    );
    CLICKHOUSE_SCHEMA_APPLY
        .query(client, &ddl)
        .execute()
        .await
        .map_err(|error| {
        StorageError::Migration(format!(
            "ClickHouse schema deployment lock is already held or could not be acquired; inspect `{DEPLOYMENT_LOCK_TABLE}` before retrying: {error}"
        ))
    })?;
    Ok(())
}

async fn release_deployment_lock(client: &Client, owner: &str) -> Result<(), StorageError> {
    let observed = CLICKHOUSE_SCHEMA_APPLY
        .query(
            client,
            "SELECT comment FROM system.tables \
             WHERE database = currentDatabase() AND name = ?",
        )
        .bind(DEPLOYMENT_LOCK_TABLE)
        .fetch_optional::<String>()
        .await?;
    if observed.as_deref() != Some(owner) {
        return Err(StorageError::Migration(format!(
            "ClickHouse schema deployment lock ownership changed before release; expected `{owner}`, observed `{}`",
            observed.as_deref().unwrap_or("missing")
        )));
    }
    CLICKHOUSE_SCHEMA_APPLY
        .query(client, &format!("DROP TABLE {DEPLOYMENT_LOCK_TABLE}"))
        .execute()
        .await?;
    Ok(())
}

async fn verify_destructive_empty_tables(
    client: &Client,
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
            && CLICKHOUSE_SCHEMA_APPLY
                .query(client, &format!("SELECT 1 FROM {table} LIMIT 1"))
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
        if spec.safety == ClickHouseMigrationSafety::OnlineMetadata {
            for statement in statements {
                validate_online_safe_statement(spec, &statement)?;
            }
        }
        if spec.safety == ClickHouseMigrationSafety::OfflineDestructive
            && spec.destructive_empty_tables.is_empty()
        {
            return Err(StorageError::Migration(format!(
                "destructive ClickHouse migration {} must declare every table requiring an emptiness proof",
                spec.version
            )));
        }
        if spec.safety != ClickHouseMigrationSafety::OfflineDestructive
            && !spec.destructive_empty_tables.is_empty()
        {
            return Err(StorageError::Migration(format!(
                "non-destructive ClickHouse migration {} cannot declare destructive table proofs",
                spec.version
            )));
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
            "ClickHouse migration {} ({}) is classified OnlineMetadata but contains non-resumable or potentially destructive DDL: `{}`",
            migration.version,
            migration.name,
            normalized.chars().take(160).collect::<String>()
        )));
    }
    Ok(())
}

pub(super) async fn verify_schema_client(
    client: &Client,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    verify_schema_client_with_lock_policy(client, false).await
}

async fn verify_schema_client_during_deployment(
    client: &Client,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    verify_schema_client_with_lock_policy(client, true).await
}

async fn verify_schema_client_with_lock_policy(
    client: &Client,
    deployment_lock_owned: bool,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    verify_server_version(client).await?;
    if !deployment_lock_owned && table_exists(client, DEPLOYMENT_LOCK_TABLE).await? {
        return Err(StorageError::Migration(format!(
            "ClickHouse schema deployment lock `{DEPLOYMENT_LOCK_TABLE}` is present; runtime startup is blocked until the deploy owner completes or an operator proves and clears a stale lock"
        )));
    }
    if !table_exists(client, MIGRATION_TABLE).await? {
        return Err(StorageError::Migration(
            "ClickHouse migration ledger is absent; run the deploy-only clickhouse-schema apply command"
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
        required_object_count: REQUIRED_SCHEMA_OBJECTS.len(),
        schema_fingerprint: ContentHash::parse(&schema_contract_hash()).map_err(|error| {
            StorageError::Migration(format!("construct ClickHouse schema fingerprint: {error}"))
        })?,
    })
}

async fn verify_server_version(client: &Client) -> Result<(), StorageError> {
    let version = CLICKHOUSE_SCHEMA_VERIFY
        .query(client, "SELECT version()")
        .fetch_one::<String>()
        .await?;
    ensure_supported_clickhouse_version(&version)
}

fn ensure_supported_clickhouse_version(version: &str) -> Result<(), StorageError> {
    let (major, minor) = parse_clickhouse_version(version)?;
    if (major, minor) < (MINIMUM_CLICKHOUSE_MAJOR, MINIMUM_CLICKHOUSE_MINOR) {
        return Err(StorageError::Migration(format!(
            "ClickHouse {version} is unsupported; version {MINIMUM_CLICKHOUSE_MAJOR}.{MINIMUM_CLICKHOUSE_MINOR} or newer is required for acknowledged async-insert deduplication across dependent materialized views"
        )));
    }
    Ok(())
}

fn parse_clickhouse_version(version: &str) -> Result<(u32, u32), StorageError> {
    let mut components = version.split('.');
    let major = components
        .next()
        .and_then(|value| value.parse::<u32>().ok());
    let minor = components
        .next()
        .and_then(|value| value.parse::<u32>().ok());
    match (major, minor) {
        (Some(major), Some(minor)) => Ok((major, minor)),
        _ => Err(StorageError::Migration(format!(
            "ClickHouse server returned an invalid version string `{version}`"
        ))),
    }
}

async fn claim_migration(
    client: &Client,
    migration: &ClickHouseSchemaMigrationInfo,
) -> Result<(), StorageError> {
    let table = format!("{MIGRATION_CLAIM_PREFIX}{:06}", migration.version);
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {table} (claimed UInt8) \
         ENGINE = MergeTree ORDER BY tuple() COMMENT '{}'",
        migration.checksum
    );
    CLICKHOUSE_SCHEMA_APPLY
        .query(client, &ddl)
        .execute()
        .await?;
    let observed = CLICKHOUSE_SCHEMA_APPLY
        .query(
            client,
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
    client: &Client,
    migration: &ClickHouseSchemaMigrationInfo,
) -> Result<(), StorageError> {
    let table = format!("{MIGRATION_CLAIM_PREFIX}{:06}", migration.version);
    let observed = CLICKHOUSE_SCHEMA_VERIFY
        .query(
            client,
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
    client: &Client,
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

async fn applied_migrations(client: &Client) -> Result<Vec<AppliedMigrationRow>, StorageError> {
    CLICKHOUSE_SCHEMA_VERIFY
        .query(
            client,
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

async fn verify_structure(client: &Client) -> Result<(), StorageError> {
    let rows = CLICKHOUSE_SCHEMA_VERIFY
        .query(
            client,
            "SELECT name, engine, engine_full, partition_key, sorting_key, \
             primary_key, sampling_key, create_table_query \
             FROM system.tables WHERE database = currentDatabase()",
        )
        .fetch_all::<TableMetadataRow>()
        .await?;
    let by_name = rows
        .into_iter()
        .map(|row| (row.name.clone(), row))
        .collect::<BTreeMap<_, _>>();
    let mut allowed = REQUIRED_SCHEMA_OBJECTS
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    allowed.insert(MIGRATION_TABLE.to_owned());
    allowed.insert(DEPLOYMENT_LOCK_TABLE.to_owned());
    allowed.extend(
        migrations()
            .iter()
            .map(|migration| format!("{MIGRATION_CLAIM_PREFIX}{:06}", migration.version)),
    );
    let unknown = by_name
        .keys()
        .filter(|name| !allowed.contains(*name))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        return Err(StorageError::Migration(format!(
            "ClickHouse database contains objects outside the boot manifest: {}",
            unknown.join(", ")
        )));
    }

    let missing = REQUIRED_SCHEMA_OBJECTS
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
    for name in ["book_microstructure_1m_mv", "quant_book_l2_trade_tape_mv"] {
        let materialized_view = by_name.get(name).ok_or_else(|| {
            StorageError::Migration(format!("ClickHouse materialized view `{name}` is absent"))
        })?;
        if materialized_view.engine != "MaterializedView" {
            return Err(StorageError::Migration(format!(
                "ClickHouse object `{name}` is not a MaterializedView"
            )));
        }
    }
    for name in REQUIRED_SCHEMA_OBJECTS {
        let object = by_name.get(name).ok_or_else(|| {
            StorageError::Migration(format!("ClickHouse managed object `{name}` is absent"))
        })?;
        if schema::extract_table_ttl(&object.create_table_query).is_some() {
            return Err(StorageError::Migration(format!(
                "ClickHouse managed object `{name}` has an unmanaged table TTL"
            )));
        }
    }
    for spec in research_source_registry()
        .map_err(StorageError::Migration)?
        .bindings
        .into_iter()
        .filter(|binding| binding.storage == ResearchSourceStorageKind::ClickHouseTable)
    {
        let table = by_name.get(spec.object.as_str()).ok_or_else(|| {
            StorageError::Migration(format!(
                "ClickHouse raw-history table `{}` is absent",
                spec.object
            ))
        })?;
        let expected_partition = spec.partition_key.as_deref().ok_or_else(|| {
            StorageError::Migration(format!(
                "ClickHouse source binding `{}` has no partition contract",
                spec.object
            ))
        })?;
        if table.partition_key != expected_partition {
            return Err(StorageError::Migration(format!(
                "ClickHouse table `{}` partition key is `{}`, expected `{}`",
                spec.object, table.partition_key, expected_partition
            )));
        }
    }
    verify_schema_manifest(client).await?;
    Ok(())
}

async fn verify_schema_manifest(client: &Client) -> Result<(), StorageError> {
    let expected: ClickHouseSchemaManifest = serde_json::from_str(EXPECTED_SCHEMA_MANIFEST)
        .map_err(|error| {
            StorageError::Migration(format!(
                "committed ClickHouse schema manifest is invalid: {error}"
            ))
        })?;
    if expected.format_version != SCHEMA_MANIFEST_FORMAT_VERSION {
        return Err(StorageError::Migration(format!(
            "ClickHouse schema manifest format {} is unsupported; expected {SCHEMA_MANIFEST_FORMAT_VERSION}",
            expected.format_version
        )));
    }
    let observed = inspect_schema_manifest(client).await?;
    if expected == observed {
        return Ok(());
    }
    let expected_hash = CanonicalDigest::blake3_json(&expected)
        .map_err(|error| StorageError::Migration(error.to_string()))?;
    let observed_hash = CanonicalDigest::blake3_json(&observed)
        .map_err(|error| StorageError::Migration(error.to_string()))?;
    let object = expected
        .objects
        .iter()
        .zip(&observed.objects)
        .find(|(left, right)| left != right)
        .map_or_else(
            || "managed object inventory".to_owned(),
            |(left, right)| format!("`{}` / `{}`", left.name, right.name),
        );
    Err(StorageError::Migration(format!(
        "ClickHouse semantic schema drift detected at {object}; expected manifest {expected_hash}, observed {observed_hash}; regenerate only from an intentionally migrated clean database"
    )))
}

async fn inspect_schema_manifest(
    client: &Client,
) -> Result<ClickHouseSchemaManifest, StorageError> {
    let database = CLICKHOUSE_SCHEMA_VERIFY
        .query(client, "SELECT currentDatabase()")
        .fetch_one::<String>()
        .await?;
    let required = REQUIRED_SCHEMA_OBJECTS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut columns_by_table = BTreeMap::<String, Vec<ClickHouseColumnManifest>>::new();
    for column in CLICKHOUSE_SCHEMA_VERIFY
        .query(
            client,
            "SELECT table, position, name, type AS column_type, default_kind, \
             default_expression, compression_codec \
             FROM system.columns WHERE database = currentDatabase() \
             ORDER BY table, position",
        )
        .fetch_all::<ColumnMetadataRow>()
        .await?
    {
        if required.contains(column.table.as_str()) {
            columns_by_table
                .entry(column.table)
                .or_default()
                .push(ClickHouseColumnManifest {
                    position: column.position,
                    name: column.name,
                    column_type: column.column_type,
                    default_kind: column.default_kind,
                    default_expression: column.default_expression,
                    compression_codec: column.compression_codec,
                });
        }
    }

    let rows = CLICKHOUSE_SCHEMA_VERIFY
        .query(
            client,
            "SELECT name, engine, engine_full, partition_key, sorting_key, \
             primary_key, sampling_key, create_table_query \
             FROM system.tables WHERE database = currentDatabase() ORDER BY name",
        )
        .fetch_all::<TableMetadataRow>()
        .await?;
    let mut objects = Vec::with_capacity(REQUIRED_SCHEMA_OBJECTS.len());
    for row in rows {
        if !required.contains(row.name.as_str()) {
            continue;
        }
        let columns = columns_by_table.remove(&row.name).ok_or_else(|| {
            StorageError::Migration(format!(
                "ClickHouse managed object `{}` has no system.columns contract",
                row.name
            ))
        })?;
        objects.push(ClickHouseSchemaObjectManifest {
            name: row.name,
            engine: row.engine,
            engine_full: row.engine_full,
            partition_key: row.partition_key,
            sorting_key: row.sorting_key,
            primary_key: row.primary_key,
            sampling_key: row.sampling_key,
            create_table_query: normalize_create_table_query(&database, &row.create_table_query),
            columns,
        });
    }
    if objects.len() != REQUIRED_SCHEMA_OBJECTS.len() {
        return Err(StorageError::Migration(format!(
            "ClickHouse semantic manifest observed {} managed objects, expected {}",
            objects.len(),
            schema::REQUIRED_SCHEMA_OBJECTS.len()
        )));
    }
    Ok(ClickHouseSchemaManifest {
        format_version: SCHEMA_MANIFEST_FORMAT_VERSION,
        objects,
    })
}

fn normalize_create_table_query(database: &str, query: &str) -> String {
    let mut normalized = query
        .replace(&format!("`{database}`."), "")
        .replace(&format!("{database}."), "");
    while let Some(start) = normalized.find(" UUID '") {
        let value_start = start + " UUID '".len();
        let Some(value_end) = normalized[value_start..].find('\'') else {
            break;
        };
        normalized.replace_range(start..=(value_start + value_end), "");
    }
    normalized
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn reject_unmanaged_schema(names: &BTreeSet<String>) -> Result<(), StorageError> {
    let unmanaged = names
        .iter()
        .filter(|name| name.as_str() != DEPLOYMENT_LOCK_TABLE)
        .cloned()
        .collect::<Vec<_>>();
    if unmanaged.is_empty() {
        return Ok(());
    }
    Err(StorageError::Migration(format!(
        "ClickHouse database contains unmanaged pre-baseline objects ({}); automatic adoption is intentionally unsupported, so recreate this pre-production database once and restart the application",
        unmanaged.join(", ")
    )))
}

async fn schema_object_names(client: &Client) -> Result<BTreeSet<String>, StorageError> {
    CLICKHOUSE_SCHEMA_VERIFY
        .query(
            client,
            "SELECT name FROM system.tables WHERE database = currentDatabase()",
        )
        .fetch_all::<TableNameRow>()
        .await
        .map(|rows| rows.into_iter().map(|row| row.name).collect())
        .map_err(Into::into)
}

async fn table_exists(client: &Client, table: &str) -> Result<bool, StorageError> {
    let count = CLICKHOUSE_SCHEMA_VERIFY
        .query(
            client,
            "SELECT count() FROM system.tables \
             WHERE database = currentDatabase() AND name = ?",
        )
        .bind(table)
        .fetch_one::<u64>()
        .await?;
    Ok(count == 1)
}

fn client(config: &ClickHouseConfig) -> Client {
    Client::default()
        .with_url(&config.url)
        .with_database(&config.database)
        .with_user(&config.user)
        .with_password(config.password.expose_secret())
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
    engine_full: String,
    partition_key: String,
    sorting_key: String,
    primary_key: String,
    sampling_key: String,
    create_table_query: String,
}

#[derive(Debug, Row, Deserialize)]
struct ColumnMetadataRow {
    table: String,
    position: u64,
    name: String,
    column_type: String,
    default_kind: String,
    default_expression: String,
    compression_codec: String,
}

#[derive(Debug, Row, Deserialize)]
struct TableNameRow {
    name: String,
}

#[cfg(test)]
mod tests {
    use super::{
        ClickHouseMigrationSafety, MigrationSpec, ensure_supported_clickhouse_version,
        parse_clickhouse_version, validate_migration_registry, validate_online_safe_statement,
    };

    const SAFE_ADD_COLUMN: &[&str] = &["ALTER TABLE events ADD COLUMN IF NOT EXISTS source String"];
    const UNSAFE_TTL: &[&str] =
        &["ALTER TABLE events MODIFY TTL event_time + INTERVAL 30 DAY DELETE"];

    #[test]
    fn compiled_migration_registry_is_contiguous_and_valid() {
        validate_migration_registry().expect("compiled migration registry should be valid");
    }

    #[test]
    fn clickhouse_version_parser_accepts_release_build_components() {
        assert_eq!(
            parse_clickhouse_version("26.5.1.882").expect("valid ClickHouse release"),
            (26, 5)
        );
    }

    #[test]
    fn clickhouse_version_parser_rejects_incomplete_versions() {
        let error = parse_clickhouse_version("26")
            .expect_err("minor version is required for the deduplication contract");
        assert!(error.to_string().contains("invalid version string"));
    }

    #[test]
    fn clickhouse_version_contract_rejects_pre_26_1_servers() {
        let error = ensure_supported_clickhouse_version("25.12.9.1")
            .expect_err("dependent materialized-view deduplication is not end-to-end");
        assert!(error.to_string().contains("26.1 or newer"));
        ensure_supported_clickhouse_version("26.1.0.0").expect("minimum supported release");
        ensure_supported_clickhouse_version("27.0.0.0").expect("future major release");
    }

    #[test]
    fn online_safe_accepts_resumable_add_column() {
        let migration = MigrationSpec {
            version: 2,
            name: "add_source",
            safety: ClickHouseMigrationSafety::OnlineMetadata,
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
            safety: ClickHouseMigrationSafety::OnlineMetadata,
            sources: UNSAFE_TTL,
            expected_checksum: "test-only",
            destructive_empty_tables: &[],
        };
        let error = validate_online_safe_statement(migration, UNSAFE_TTL[0])
            .expect_err("TTL migration must require an offline rollout");
        assert!(error.to_string().contains("potentially destructive DDL"));
    }
}
