//! Versioned `ClickHouse` startup migrations and read-only runtime verification.

use std::collections::{BTreeMap, BTreeSet};

use clickhouse::Row;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    config::ClickHouseConfig,
    hashing::CanonicalDigest,
    types::{ResearchSourceStorageKind, research_source_registry},
};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::clickhouse::{ensure, schema};

const MIGRATION_TABLE: &str = "quant_pivot_schema_migration";
const MIGRATION_CLAIM_PREFIX: &str = "quant_pivot_schema_migration_claim_";
const DEPLOYMENT_LOCK_TABLE: &str = "quant_pivot_schema_deployment_lock";
const SCHEMA_MANIFEST_FORMAT_VERSION: u32 = 1;
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

const fn migrations() -> [MigrationSpec; 9] {
    [
        MigrationSpec {
            version: 1,
            name: "cloud_baseline",
            safety: ClickHouseMigrationSafety::OnlineMetadata,
            sources: schema::BASELINE_SOURCES,
            expected_checksum: "blake3:4dcfdaaa484b6d4997d9f60007e1c24d8c5c649e8718e5cf12c757a4555cf682",
            destructive_empty_tables: &[],
        },
        MigrationSpec {
            version: 2,
            name: "report_lifecycle_v2",
            safety: ClickHouseMigrationSafety::OfflineDestructive,
            sources: schema::REPORT_LIFECYCLE_V2_SOURCES,
            expected_checksum: "blake3:c831fbb3fb7719dc19138baa4dfa396f2edfe86f4bc17889e9b01e82aa2bbe57",
            destructive_empty_tables: &[
                "quant_recommendation_event",
                "quant_recommendation_attribution_event",
            ],
        },
        MigrationSpec {
            version: 3,
            name: "remove_unmanaged_rollup_ttl",
            safety: ClickHouseMigrationSafety::OfflineNonDestructive,
            sources: schema::REMOVE_UNMANAGED_ROLLUP_TTL_SOURCES,
            expected_checksum: "blake3:c31b51579cfb505ba0da215325277c3bbd132f7a254b1747f32f55b734d40837",
            destructive_empty_tables: &[],
        },
        MigrationSpec {
            version: 4,
            name: "schema_version_uint32",
            safety: ClickHouseMigrationSafety::OfflineNonDestructive,
            sources: schema::SCHEMA_VERSION_UINT32_SOURCES,
            expected_checksum: "blake3:21a550a792226c7edbea627b1b3fe3eca269177de7d4cda31c1661dbc1b19a04",
            destructive_empty_tables: &[],
        },
        MigrationSpec {
            version: 5,
            name: "weather_long_form_v2",
            safety: ClickHouseMigrationSafety::OfflineDestructive,
            sources: schema::WEATHER_LONG_FORM_V2_SOURCES,
            expected_checksum: "blake3:793e84a9ffc5a65f69c6989d0d06f5148670bf8d8524e4d2e5f48ea1e97e1fc7",
            destructive_empty_tables: &[
                "quant_weather_observation_report",
                "quant_weather_forecast_point",
            ],
        },
        MigrationSpec {
            version: 6,
            name: "weather_historical_date32",
            safety: ClickHouseMigrationSafety::OfflineNonDestructive,
            sources: schema::WEATHER_HISTORICAL_DATE32_SOURCES,
            expected_checksum: "blake3:b226177fa5f82a07f7ae3aac24cfcffe811e29cc68aec0bfd1417b2b6b066b50",
            destructive_empty_tables: &[],
        },
        MigrationSpec {
            version: 7,
            name: "weather_epoch_day",
            safety: ClickHouseMigrationSafety::OfflineNonDestructive,
            sources: schema::WEATHER_EPOCH_DAY_SOURCES,
            expected_checksum: "blake3:6df662b111f277011f3fd13e25e4b300f98a5e9b605608f82adc00ac56a2d4b5",
            destructive_empty_tables: &[],
        },
        MigrationSpec {
            version: 8,
            name: "weather_observation_epoch_time",
            safety: ClickHouseMigrationSafety::OfflineRebuildableDataRepair,
            sources: schema::WEATHER_OBSERVATION_EPOCH_TIME_SOURCES,
            expected_checksum: "blake3:49d3d7e1107275b598806bf927f03e249972f3598e813b97b5dcfe3173b49226",
            destructive_empty_tables: &[],
        },
        MigrationSpec {
            version: 9,
            name: "immutable_domain_fact_idempotency",
            safety: ClickHouseMigrationSafety::OfflineRebuildableDataRepair,
            sources: schema::IMMUTABLE_DOMAIN_FACT_IDEMPOTENCY_SOURCES,
            expected_checksum: "blake3:e3141e29ffb93b9c60839cb2da925bf8dfb1abb4890e51131b175e503a8158e5",
            destructive_empty_tables: &[],
        },
    ]
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
    let manifest = inspect_schema_manifest(&client(config)).await?;
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
    let lock_owner = uuid::Uuid::now_v7().to_string();
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
    client: &clickhouse::Client,
    allow_offline: bool,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    let names = schema_object_names(client).await?;
    if !names.contains(MIGRATION_TABLE) {
        reject_unmanaged_schema(&names)?;
        client.query(MIGRATION_TABLE_DDL).execute().await?;
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
        if migration.safety == ClickHouseMigrationSafety::OfflineRebuildableDataRepair {
            match spec.name {
                "weather_observation_epoch_time" => {
                    rebuild_weather_observation_epoch_time(client, spec).await?;
                }
                "immutable_domain_fact_idempotency" => {
                    rebuild_immutable_domain_facts(client, spec).await?;
                }
                name => {
                    return Err(StorageError::Migration(format!(
                        "ClickHouse rebuildable migration {} has no typed executor for `{name}`",
                        spec.version
                    )));
                }
            }
        } else {
            for statement in spec.statements() {
                client.query(&statement).execute().await?;
            }
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

    verify_schema_client_during_deployment(client).await
}

async fn acquire_deployment_lock(
    client: &clickhouse::Client,
    owner: &str,
) -> Result<(), StorageError> {
    let ddl = format!(
        "CREATE TABLE {DEPLOYMENT_LOCK_TABLE} (owner String) \
         ENGINE = TinyLog COMMENT '{owner}'"
    );
    client.query(&ddl).execute().await.map_err(|error| {
        StorageError::Migration(format!(
            "ClickHouse schema deployment lock is already held or could not be acquired; inspect `{DEPLOYMENT_LOCK_TABLE}` before retrying: {error}"
        ))
    })?;
    Ok(())
}

async fn release_deployment_lock(
    client: &clickhouse::Client,
    owner: &str,
) -> Result<(), StorageError> {
    let observed = client
        .query(
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
    client
        .query(&format!("DROP TABLE {DEPLOYMENT_LOCK_TABLE}"))
        .execute()
        .await?;
    Ok(())
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

struct ImmutableFactTableRepair {
    table: &'static str,
    stage: &'static str,
    backup: &'static str,
    insert_sql: &'static str,
    source_key_count_sql: &'static str,
    repaired_key_count_sql: &'static str,
    repaired_revision_conflict_sql: Option<&'static str>,
}

const DOMAIN_OBSERVATION_REPAIR: ImmutableFactTableRepair = ImmutableFactTableRepair {
    table: "quant_domain_observation",
    stage: "quant_domain_observation_idempotent_stage",
    backup: "quant_domain_observation_pre_idempotency_backup",
    insert_sql: "INSERT INTO __STAGE__ \
        SELECT argMin(family, ingestion_time), source_id, instrument_key, metric, \
               argMin(value, ingestion_time), event_time, \
               argMin(publish_time, ingestion_time), min(ingestion_time), \
               argMin(schema_version, ingestion_time) \
        FROM __SOURCE__ \
        GROUP BY source_id, instrument_key, metric, event_time",
    source_key_count_sql: "SELECT uniqExact(tuple(source_id, instrument_key, metric, event_time)) FROM __SOURCE__",
    repaired_key_count_sql: "SELECT toUInt64(count() - uniqExact(tuple(source_id, instrument_key, metric, event_time))) FROM __REPAIRED__",
    repaired_revision_conflict_sql: None,
};

const CRYPTO_PRICE_REPORT_REPAIR: ImmutableFactTableRepair = ImmutableFactTableRepair {
    table: "quant_crypto_price_report",
    stage: "quant_crypto_price_report_idempotent_stage",
    backup: "quant_crypto_price_report_pre_idempotency_backup",
    insert_sql: "INSERT INTO __STAGE__ \
        SELECT source_id, instrument_key, source_sequence, \
               argMin(price, available_at), argMin(quantity, available_at), event_time, \
               argMin(published_at, available_at), min(available_at), \
               argMin(valid_from, available_at), argMin(observations_timestamp, available_at), \
               argMin(expires_at, available_at), report_hash, \
               argMin(raw_report, available_at), argMin(schema_version, available_at) \
        FROM __SOURCE__ \
        GROUP BY source_id, instrument_key, source_sequence, event_time, report_hash",
    source_key_count_sql: "SELECT uniqExact(tuple(source_id, instrument_key, source_sequence, event_time, report_hash)) FROM __SOURCE__",
    repaired_key_count_sql: "SELECT toUInt64(count() - uniqExact(tuple(source_id, instrument_key, source_sequence, event_time, report_hash))) FROM __REPAIRED__",
    repaired_revision_conflict_sql: None,
};

const WEATHER_OBSERVATION_REPAIR: ImmutableFactTableRepair = ImmutableFactTableRepair {
    table: "quant_weather_observation_fact",
    stage: "quant_weather_observation_fact_idempotent_stage",
    backup: "quant_weather_observation_fact_pre_idempotency_backup",
    insert_sql: "INSERT INTO __STAGE__ \
        SELECT source_id, instrument_key, tupleElement(chosen, 1), tupleElement(chosen, 2), \
               tupleElement(chosen, 3), variable, tupleElement(chosen, 4), \
               tupleElement(chosen, 5), tupleElement(chosen, 6), observed_at, \
               tupleElement(chosen, 7), tupleElement(chosen, 8), tupleElement(chosen, 9), \
               earliest_available_at, \
               toUInt32(row_number() OVER identity_window - 1), report_hash, \
               lagInFrame(toNullable(report_hash), 1, CAST(NULL, 'Nullable(String)')) \
                   OVER identity_window, \
               tupleElement(chosen, 10), tupleElement(chosen, 11) \
        FROM (\
            SELECT source_id, instrument_key, variable, observed_at, report_hash, \
                   min(available_at) AS earliest_available_at, \
                   argMin(tuple(subject_key, local_date, report_kind, value, unit, precision, \
                                valid_from, valid_to, published_at, raw_report, schema_version), \
                          tuple(available_at, cityHash64(tuple(subject_key, local_date, report_kind, \
                               value, unit, precision, valid_from, valid_to, published_at, raw_report, schema_version)))) AS chosen \
            FROM __SOURCE__ \
            GROUP BY source_id, instrument_key, variable, observed_at, report_hash\
        ) \
        WINDOW identity_window AS (\
            PARTITION BY source_id, instrument_key, variable, observed_at \
            ORDER BY earliest_available_at, report_hash \
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        )",
    source_key_count_sql: "SELECT uniqExact(tuple(source_id, instrument_key, variable, observed_at, report_hash)) FROM __SOURCE__",
    repaired_key_count_sql: "SELECT toUInt64(count() - uniqExact(tuple(source_id, instrument_key, variable, observed_at, report_hash))) FROM __REPAIRED__",
    repaired_revision_conflict_sql: Some(
        "SELECT toUInt64(count() - uniqExact(tuple(source_id, instrument_key, variable, observed_at, revision))) FROM __REPAIRED__",
    ),
};

const WEATHER_FORECAST_REPAIR: ImmutableFactTableRepair = ImmutableFactTableRepair {
    table: "quant_weather_forecast_fact",
    stage: "quant_weather_forecast_fact_idempotent_stage",
    backup: "quant_weather_forecast_fact_pre_idempotency_backup",
    insert_sql: "INSERT INTO __STAGE__ \
        SELECT source_id, instrument_key, tupleElement(chosen, 1), variable, \
               tupleElement(chosen, 2), tupleElement(chosen, 3), tupleElement(chosen, 4), \
               reference_time, valid_time, tupleElement(chosen, 5), earliest_available_at, \
               tupleElement(chosen, 6), member, \
               toUInt32(row_number() OVER identity_window - 1), \
               tupleElement(chosen, 7), tupleElement(chosen, 8), report_hash, \
               tupleElement(chosen, 9) \
        FROM (\
            SELECT source_id, instrument_key, variable, reference_time, valid_time, member, report_hash, \
                   min(available_at) AS earliest_available_at, \
                   argMin(tuple(subject_key, value, unit, precision, published_at, lead_hours, \
                                grid_binding_hash, run_manifest_hash, schema_version), \
                          tuple(available_at, cityHash64(tuple(subject_key, value, unit, precision, \
                               published_at, lead_hours, grid_binding_hash, run_manifest_hash, schema_version)))) AS chosen \
            FROM __SOURCE__ \
            GROUP BY source_id, instrument_key, variable, reference_time, valid_time, member, report_hash\
        ) \
        WINDOW identity_window AS (\
            PARTITION BY source_id, instrument_key, variable, reference_time, valid_time, member \
            ORDER BY earliest_available_at, report_hash \
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        )",
    source_key_count_sql: "SELECT uniqExact(tuple(source_id, instrument_key, variable, reference_time, valid_time, ifNull(member, 65535), report_hash)) FROM __SOURCE__",
    repaired_key_count_sql: "SELECT toUInt64(count() - uniqExact(tuple(source_id, instrument_key, variable, reference_time, valid_time, ifNull(member, 65535), report_hash))) FROM __REPAIRED__",
    repaired_revision_conflict_sql: Some(
        "SELECT toUInt64(count() - uniqExact(tuple(source_id, instrument_key, variable, reference_time, valid_time, ifNull(member, 65535), revision))) FROM __REPAIRED__",
    ),
};

async fn rebuild_immutable_domain_facts(
    client: &clickhouse::Client,
    migration: MigrationSpec,
) -> Result<(), StorageError> {
    for repair in [
        &DOMAIN_OBSERVATION_REPAIR,
        &CRYPTO_PRICE_REPORT_REPAIR,
        &WEATHER_OBSERVATION_REPAIR,
        &WEATHER_FORECAST_REPAIR,
    ] {
        rebuild_immutable_fact_table(client, migration, repair).await?;
    }
    Ok(())
}

async fn rebuild_immutable_fact_table(
    client: &clickhouse::Client,
    migration: MigrationSpec,
    repair: &ImmutableFactTableRepair,
) -> Result<(), StorageError> {
    if immutable_fact_schema_is_current(client, repair.table).await? {
        if table_exists(client, repair.backup).await? {
            verify_immutable_fact_repair(client, repair, repair.table, repair.backup).await?;
            client
                .query(&format!("DROP TABLE {}", repair.backup))
                .execute()
                .await?;
        }
        if table_exists(client, repair.stage).await? {
            client
                .query(&format!("DROP TABLE {}", repair.stage))
                .execute()
                .await?;
        }
        return Ok(());
    }
    if table_exists(client, repair.backup).await? {
        return Err(StorageError::Migration(format!(
            "ClickHouse data repair {} found backup `{}` while `{}` is still legacy; operator inspection is required",
            migration.version, repair.backup, repair.table
        )));
    }
    if table_exists(client, repair.stage).await? {
        client
            .query(&format!("DROP TABLE {}", repair.stage))
            .execute()
            .await?;
    }
    let stage_marker = format!("CREATE TABLE IF NOT EXISTS {}", repair.stage);
    let create_stage = migration
        .statements()
        .into_iter()
        .find(|statement| statement.starts_with(&stage_marker))
        .ok_or_else(|| {
            StorageError::Migration(format!(
                "ClickHouse data repair {} has no stage DDL for `{}`",
                migration.version, repair.stage
            ))
        })?;
    client.query(&create_stage).execute().await?;
    let insert = render_repair_sql(repair.insert_sql, repair.stage, repair.table, repair.stage);
    client.query(&insert).execute().await?;
    verify_immutable_fact_repair(client, repair, repair.stage, repair.table).await?;
    client
        .query(&format!(
            "RENAME TABLE {} TO {}, {} TO {}",
            repair.table, repair.backup, repair.stage, repair.table
        ))
        .execute()
        .await?;
    verify_immutable_fact_repair(client, repair, repair.table, repair.backup).await?;
    client
        .query(&format!("DROP TABLE {}", repair.backup))
        .execute()
        .await?;
    Ok(())
}

async fn immutable_fact_schema_is_current(
    client: &clickhouse::Client,
    table: &str,
) -> Result<bool, StorageError> {
    let query = client
        .query(
            "SELECT engine, create_table_query FROM system.tables \
             WHERE database = currentDatabase() AND name = ?",
        )
        .bind(table)
        .fetch_optional::<(String, String)>()
        .await?;
    let Some((engine, create_table_query)) = query else {
        return Err(StorageError::Migration(format!(
            "ClickHouse immutable fact table `{table}` is absent"
        )));
    };
    Ok(engine == "MergeTree"
        && create_table_query.contains("non_replicated_deduplication_window = 10000"))
}

async fn verify_immutable_fact_repair(
    client: &clickhouse::Client,
    repair: &ImmutableFactTableRepair,
    repaired: &str,
    source: &str,
) -> Result<(), StorageError> {
    let expected = client
        .query(&render_repair_sql(
            repair.source_key_count_sql,
            repair.stage,
            source,
            repaired,
        ))
        .fetch_one::<u64>()
        .await?;
    let actual = table_row_count(client, repaired).await?;
    let duplicate_keys = client
        .query(&render_repair_sql(
            repair.repaired_key_count_sql,
            repair.stage,
            source,
            repaired,
        ))
        .fetch_one::<u64>()
        .await?;
    let revision_conflicts = if let Some(sql) = repair.repaired_revision_conflict_sql {
        client
            .query(&render_repair_sql(sql, repair.stage, source, repaired))
            .fetch_one::<u64>()
            .await?
    } else {
        0
    };
    if actual != expected || duplicate_keys != 0 || revision_conflicts != 0 {
        return Err(StorageError::Migration(format!(
            "ClickHouse immutable fact repair failed for `{}`: expected_keys={expected}, actual_rows={actual}, duplicate_keys={duplicate_keys}, revision_conflicts={revision_conflicts}",
            repair.table
        )));
    }
    Ok(())
}

fn render_repair_sql(template: &str, stage: &str, source: &str, repaired: &str) -> String {
    template
        .replace("__STAGE__", stage)
        .replace("__SOURCE__", source)
        .replace("__REPAIRED__", repaired)
}

async fn rebuild_weather_observation_epoch_time(
    client: &clickhouse::Client,
    migration: MigrationSpec,
) -> Result<(), StorageError> {
    const TABLE: &str = "quant_weather_observation_fact";
    const STAGE: &str = "quant_weather_observation_fact_epoch_stage";
    const BACKUP: &str = "quant_weather_observation_fact_date32_backup";
    const REBUILDABLE_SOURCE: &str = "nasa_gistemp";

    let observed_type = column_type(client, TABLE, "observed_at")
        .await?
        .ok_or_else(|| {
            StorageError::Migration(format!(
                "ClickHouse data repair {} requires `{TABLE}.observed_at`",
                migration.version
            ))
        })?;
    if observed_type == "Int64" {
        if table_exists(client, BACKUP).await? {
            verify_rebuild_row_counts(client, TABLE, BACKUP, REBUILDABLE_SOURCE).await?;
            client
                .query(&format!("DROP TABLE {BACKUP}"))
                .execute()
                .await?;
        }
        if table_exists(client, STAGE).await? {
            client
                .query(&format!("DROP TABLE {STAGE}"))
                .execute()
                .await?;
        }
        return Ok(());
    }
    if observed_type != "DateTime64(3, 'UTC')" {
        return Err(StorageError::Migration(format!(
            "ClickHouse data repair {} expected `{TABLE}.observed_at` to be DateTime64(3, 'UTC') or Int64, observed `{observed_type}`",
            migration.version
        )));
    }
    if table_exists(client, BACKUP).await? {
        return Err(StorageError::Migration(format!(
            "ClickHouse data repair {} found unexpected backup `{BACKUP}` while `{TABLE}` is still legacy; operator inspection is required",
            migration.version
        )));
    }
    if table_exists(client, STAGE).await? {
        client
            .query(&format!("DROP TABLE {STAGE}"))
            .execute()
            .await?;
    }
    for statement in migration.statements() {
        client.query(&statement).execute().await?;
    }
    client
        .query(&format!(
            "INSERT INTO {STAGE} SELECT \
             source_id, instrument_key, subject_key, local_date, report_kind, variable, value, unit, precision, \
             toUnixTimestamp64Milli(observed_at), toUnixTimestamp64Milli(valid_from), toUnixTimestamp64Milli(valid_to), \
             published_at, available_at, revision, report_hash, supersedes_report_hash, raw_report, schema_version \
             FROM {TABLE} WHERE source_id != ?"
        ))
        .bind(REBUILDABLE_SOURCE)
        .execute()
        .await?;
    verify_rebuild_row_counts(client, STAGE, TABLE, REBUILDABLE_SOURCE).await?;
    client
        .query(&format!(
            "RENAME TABLE {TABLE} TO {BACKUP}, {STAGE} TO {TABLE}"
        ))
        .execute()
        .await?;
    verify_rebuild_row_counts(client, TABLE, BACKUP, REBUILDABLE_SOURCE).await?;
    client
        .query(&format!("DROP TABLE {BACKUP}"))
        .execute()
        .await?;
    Ok(())
}

async fn verify_rebuild_row_counts(
    client: &clickhouse::Client,
    repaired_table: &str,
    source_table: &str,
    rebuildable_source: &str,
) -> Result<(), StorageError> {
    let source_rows = table_row_count(client, source_table).await?;
    let excluded_rows = client
        .query(&format!(
            "SELECT count() FROM {source_table} FINAL WHERE source_id = ?"
        ))
        .bind(rebuildable_source)
        .fetch_one::<u64>()
        .await?;
    let expected_rows = source_rows.checked_sub(excluded_rows).ok_or_else(|| {
        StorageError::Migration("Weather observation repair row-count underflow".to_owned())
    })?;
    let repaired_rows = table_row_count(client, repaired_table).await?;
    let unexpected_rebuildable = client
        .query(&format!(
            "SELECT count() FROM {repaired_table} FINAL WHERE source_id = ?"
        ))
        .bind(rebuildable_source)
        .fetch_one::<u64>()
        .await?;
    if repaired_rows != expected_rows || unexpected_rebuildable != 0 {
        return Err(StorageError::Migration(format!(
            "Weather observation repair invariant failed: source_rows={source_rows}, explicitly_rebuildable_rows={excluded_rows}, expected_preserved={expected_rows}, observed_preserved={repaired_rows}, unexpected_rebuildable={unexpected_rebuildable}"
        )));
    }
    Ok(())
}

async fn table_row_count(client: &clickhouse::Client, table: &str) -> Result<u64, StorageError> {
    let engine = client
        .query(
            "SELECT engine FROM system.tables \
             WHERE database = currentDatabase() AND name = ?",
        )
        .bind(table)
        .fetch_optional::<String>()
        .await?
        .ok_or_else(|| {
            StorageError::Migration(format!(
                "ClickHouse row-count verification table `{table}` is absent"
            ))
        })?;
    let final_clause = if engine.starts_with("ReplacingMergeTree") {
        " FINAL"
    } else {
        ""
    };
    client
        .query(&format!("SELECT count() FROM {table}{final_clause}"))
        .fetch_one::<u64>()
        .await
        .map_err(Into::into)
}

async fn column_type(
    client: &clickhouse::Client,
    table: &str,
    column: &str,
) -> Result<Option<String>, StorageError> {
    client
        .query(
            "SELECT type FROM system.columns \
             WHERE database = currentDatabase() AND table = ? AND name = ?",
        )
        .bind(table)
        .bind(column)
        .fetch_optional::<String>()
        .await
        .map_err(Into::into)
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
    client: &clickhouse::Client,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    verify_schema_client_with_lock_policy(client, false).await
}

async fn verify_schema_client_during_deployment(
    client: &clickhouse::Client,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    verify_schema_client_with_lock_policy(client, true).await
}

async fn verify_schema_client_with_lock_policy(
    client: &clickhouse::Client,
    deployment_lock_owned: bool,
) -> Result<ClickHouseSchemaStatus, StorageError> {
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
    for name in schema::REQUIRED_SCHEMA_OBJECTS {
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

async fn verify_schema_manifest(client: &clickhouse::Client) -> Result<(), StorageError> {
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
    client: &clickhouse::Client,
) -> Result<ClickHouseSchemaManifest, StorageError> {
    let database = client
        .query("SELECT currentDatabase()")
        .fetch_one::<String>()
        .await?;
    let required = schema::REQUIRED_SCHEMA_OBJECTS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut columns_by_table = BTreeMap::<String, Vec<ClickHouseColumnManifest>>::new();
    for column in client
        .query(
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

    let rows = client
        .query(
            "SELECT name, engine, engine_full, partition_key, sorting_key, \
             primary_key, sampling_key, create_table_query \
             FROM system.tables WHERE database = currentDatabase() ORDER BY name",
        )
        .fetch_all::<TableMetadataRow>()
        .await?;
    let mut objects = Vec::with_capacity(schema::REQUIRED_SCHEMA_OBJECTS.len());
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
    if objects.len() != schema::REQUIRED_SCHEMA_OBJECTS.len() {
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
