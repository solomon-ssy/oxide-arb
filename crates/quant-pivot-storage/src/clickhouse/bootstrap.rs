//! Fresh `ClickHouse` schema bootstrap and read-only runtime verification.

use std::collections::{BTreeMap, BTreeSet};

use clickhouse::{Client, Row};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    config::ClickHouseConfig,
    hashing::CanonicalDigest,
    types::{ContentHash, ResearchSourceStorageKind, research_source_registry},
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use uuid::Uuid;

use crate::clickhouse::{
    deadline::ClickHouseIoDeadlines,
    ensure,
    query::ClickHouseMaintenanceClient,
    query_limits::{CLICKHOUSE_SCHEMA_BOOTSTRAP, CLICKHOUSE_SCHEMA_VERIFY},
    schema,
    schema::{BOOTSTRAP_SQL, REQUIRED_SCHEMA_OBJECTS},
};

const DEPLOYMENT_LOCK_TABLE: &str = "quant_pivot_schema_deployment_lock";
const SCHEMA_MANIFEST_FORMAT_VERSION: u32 = 1;
const SUPPORTED_CLICKHOUSE_MAJOR: u32 = 26;
const SUPPORTED_CLICKHOUSE_MINOR: u32 = 5;
const EXPECTED_SCHEMA_MANIFEST: &str = include_str!("../../../../schema/clickhouse/manifest.json");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseSchemaStatus {
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

/// Content hash of the complete fresh `ClickHouse` schema contract.
#[must_use]
pub fn schema_contract_hash() -> String {
    let mut framed = Vec::new();
    framed.extend_from_slice(&(BOOTSTRAP_SQL.len() as u64).to_le_bytes());
    framed.extend_from_slice(BOOTSTRAP_SQL.as_bytes());
    framed.extend_from_slice(&(EXPECTED_SCHEMA_MANIFEST.len() as u64).to_le_bytes());
    framed.extend_from_slice(EXPECTED_SCHEMA_MANIFEST.as_bytes());
    CanonicalDigest::prefixed_bytes(&framed)
}

/// Apply the compiled bootstrap to a provably empty database and render the
/// resulting semantic manifest without comparing it to the checked artifact.
///
/// This is the sole code-generation path for replacing the checked manifest
/// before a first deployment. Ordinary bootstrap and verify paths always compare
/// against the checked artifact and therefore remain fail closed.
pub async fn generate_clean_schema_manifest(
    config: &ClickHouseConfig,
) -> Result<String, StorageError> {
    validate_bootstrap_source()?;
    if ensure::database_exists(config).await? {
        return Err(StorageError::Schema(format!(
            "clean ClickHouse manifest generation requires absent database `{}`",
            config.database
        )));
    }
    ensure::ensure_database(config).await?;
    let client = client(config);
    client.verify_server_version().await?;
    let lock_owner = Uuid::now_v7().to_string();
    acquire_deployment_lock(&client, &lock_owner).await?;
    let result = async {
        bootstrap_schema_locked(&client, false).await?;
        client.render_observed_schema_manifest().await
    }
    .await;
    let release = release_deployment_lock(&client, &lock_owner).await;
    match (result, release) {
        (Ok(rendered), Ok(())) => Ok(rendered),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(generate_error), Err(release_error)) => {
            error!(%release_error, "ClickHouse deployment lock release also failed after manifest generation failure");
            Err(generate_error)
        }
    }
}

/// Bootstrap the one compiled schema into an absent or object-empty database.
///
/// Existing, partially initialized, and already bootstrapped schemas are all
/// rejected. This path never adopts, resumes, upgrades, or rewrites data.
pub async fn bootstrap_schema(
    config: &ClickHouseConfig,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    validate_bootstrap_source()?;
    ensure::ensure_database(config).await?;
    let client = client(config);
    client.verify_server_version().await?;
    let lock_owner = Uuid::now_v7().to_string();
    acquire_deployment_lock(&client, &lock_owner).await?;
    let result = bootstrap_schema_locked(&client, true).await;
    let release = release_deployment_lock(&client, &lock_owner).await;
    match (result, release) {
        (Ok(status), Ok(())) => Ok(status),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(bootstrap_error), Err(release_error)) => {
            error!(%release_error, "ClickHouse deployment lock release also failed after bootstrap failure");
            Err(bootstrap_error)
        }
    }
}

async fn bootstrap_schema_locked(
    client: &ClickHouseMaintenanceClient,
    verify_expected_manifest: bool,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    let names = client.schema_object_names().await?;
    reject_nonempty_schema(&names)?;
    for statement in schema::split_statements(BOOTSTRAP_SQL) {
        CLICKHOUSE_SCHEMA_BOOTSTRAP
            .maintenance_query(client, &statement)
            .execute()
            .await?;
    }
    info!(
        objects = REQUIRED_SCHEMA_OBJECTS.len(),
        "ClickHouse fresh schema bootstrapped"
    );

    verify_deploy_schema(client, verify_expected_manifest).await
}

async fn acquire_deployment_lock(
    client: &ClickHouseMaintenanceClient,
    owner: &str,
) -> Result<(), StorageError> {
    let ddl = format!(
        "CREATE TABLE {DEPLOYMENT_LOCK_TABLE} (owner String) \
         ENGINE = TinyLog COMMENT '{owner}'"
    );
    match CLICKHOUSE_SCHEMA_BOOTSTRAP
        .maintenance_query(client, &ddl)
        .execute()
        .await
    {
        Ok(()) => {}
        Err(error @ StorageError::ClickHouseTimeout { .. }) => return Err(error),
        Err(error) => {
            return Err(StorageError::Schema(format!(
                "ClickHouse schema deployment lock is already held or could not be acquired; inspect `{DEPLOYMENT_LOCK_TABLE}` before retrying: {error}"
            )));
        }
    }
    Ok(())
}

async fn release_deployment_lock(
    client: &ClickHouseMaintenanceClient,
    owner: &str,
) -> Result<(), StorageError> {
    let observed = CLICKHOUSE_SCHEMA_BOOTSTRAP
        .maintenance_query(
            client,
            "SELECT comment FROM system.tables \
             WHERE database = currentDatabase() AND name = ?",
        )
        .bind(DEPLOYMENT_LOCK_TABLE)
        .fetch_optional::<String>()
        .await?;
    if observed.as_deref() != Some(owner) {
        return Err(StorageError::Schema(format!(
            "ClickHouse schema deployment lock ownership changed before release; expected `{owner}`, observed `{}`",
            observed.as_deref().unwrap_or("missing")
        )));
    }
    CLICKHOUSE_SCHEMA_BOOTSTRAP
        .maintenance_query(client, &format!("DROP TABLE {DEPLOYMENT_LOCK_TABLE}"))
        .execute()
        .await?;
    Ok(())
}

/// Verify the deployed structural runtime contract without performing DDL.
pub async fn verify_schema(
    config: &ClickHouseConfig,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    validate_bootstrap_source()?;
    if !ensure::database_exists(config).await? {
        return Err(StorageError::Schema(format!(
            "ClickHouse database `{}` does not exist; run the deploy-only fresh bootstrap",
            config.database
        )));
    }
    client(config).verify_schema().await
}

fn validate_bootstrap_source() -> Result<(), StorageError> {
    let statements = schema::split_statements(BOOTSTRAP_SQL);
    if statements.len() != REQUIRED_SCHEMA_OBJECTS.len() {
        return Err(StorageError::Schema(format!(
            "ClickHouse fresh bootstrap contains {} statements, expected {} managed objects",
            statements.len(),
            REQUIRED_SCHEMA_OBJECTS.len()
        )));
    }
    for statement in statements {
        let normalized = statement
            .split_ascii_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_uppercase();
        let create_only = normalized.starts_with("CREATE TABLE ")
            || normalized.starts_with("CREATE MATERIALIZED VIEW ");
        if !create_only || normalized.contains(" IF NOT EXISTS ") {
            return Err(StorageError::Schema(format!(
                "ClickHouse fresh bootstrap accepts only unconditional CREATE TABLE or CREATE MATERIALIZED VIEW statements: `{}`",
                normalized.chars().take(160).collect::<String>()
            )));
        }
    }
    Ok(())
}

impl ClickHouseMaintenanceClient {
    pub(crate) async fn verify_schema(&self) -> Result<ClickHouseSchemaStatus, StorageError> {
        verify_locked_schema(self, false, true).await
    }

    async fn verify_server_version(&self) -> Result<(), StorageError> {
        let version = CLICKHOUSE_SCHEMA_VERIFY
            .maintenance_query(self, "SELECT version()")
            .fetch_one::<String>()
            .await?;
        ensure_supported_clickhouse_version(&version)
    }

    async fn render_observed_schema_manifest(&self) -> Result<String, StorageError> {
        let manifest = self.inspect_schema_manifest().await?;
        let mut rendered = serde_json::to_string_pretty(&manifest).map_err(|error| {
            StorageError::Schema(format!("render ClickHouse manifest: {error}"))
        })?;
        rendered.push('\n');
        Ok(rendered)
    }

    async fn verify_schema_manifest(&self) -> Result<(), StorageError> {
        let expected: ClickHouseSchemaManifest = serde_json::from_str(EXPECTED_SCHEMA_MANIFEST)
            .map_err(|error| {
                StorageError::Schema(format!(
                    "committed ClickHouse schema manifest is invalid: {error}"
                ))
            })?;
        if expected.format_version != SCHEMA_MANIFEST_FORMAT_VERSION {
            return Err(StorageError::Schema(format!(
                "ClickHouse schema manifest format {} is unsupported; expected {SCHEMA_MANIFEST_FORMAT_VERSION}",
                expected.format_version
            )));
        }
        let observed = self.inspect_schema_manifest().await?;
        if expected == observed {
            return Ok(());
        }
        let expected_hash = CanonicalDigest::blake3_json(&expected)
            .map_err(|error| StorageError::Schema(error.to_string()))?;
        let observed_hash = CanonicalDigest::blake3_json(&observed)
            .map_err(|error| StorageError::Schema(error.to_string()))?;
        let object = expected
            .objects
            .iter()
            .zip(&observed.objects)
            .find(|(left, right)| left != right)
            .map_or_else(
                || "managed object inventory".to_owned(),
                |(left, right)| format!("`{}` / `{}`", left.name, right.name),
            );
        Err(StorageError::Schema(format!(
            "ClickHouse semantic schema drift detected at {object}; expected manifest {expected_hash}, observed {observed_hash}; regenerate only from a fresh disposable database"
        )))
    }

    async fn inspect_schema_manifest(&self) -> Result<ClickHouseSchemaManifest, StorageError> {
        let database = CLICKHOUSE_SCHEMA_VERIFY
            .maintenance_query(self, "SELECT currentDatabase()")
            .fetch_one::<String>()
            .await?;
        let required = REQUIRED_SCHEMA_OBJECTS
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut columns_by_table = BTreeMap::<String, Vec<ClickHouseColumnManifest>>::new();
        for column in CLICKHOUSE_SCHEMA_VERIFY
            .maintenance_query(
                self,
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
            .maintenance_query(
                self,
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
                StorageError::Schema(format!(
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
                create_table_query: normalize_create_table_query(
                    &database,
                    &row.create_table_query,
                ),
                columns,
            });
        }
        if objects.len() != REQUIRED_SCHEMA_OBJECTS.len() {
            return Err(StorageError::Schema(format!(
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

    async fn schema_object_names(&self) -> Result<BTreeSet<String>, StorageError> {
        CLICKHOUSE_SCHEMA_VERIFY
            .maintenance_query(
                self,
                "SELECT name FROM system.tables WHERE database = currentDatabase()",
            )
            .fetch_all::<TableNameRow>()
            .await
            .map(|rows| rows.into_iter().map(|row| row.name).collect())
    }
}

async fn verify_deploy_schema(
    client: &ClickHouseMaintenanceClient,
    verify_expected_manifest: bool,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    verify_locked_schema(client, true, verify_expected_manifest).await
}

async fn verify_locked_schema(
    client: &ClickHouseMaintenanceClient,
    deployment_lock_owned: bool,
    verify_expected_manifest: bool,
) -> Result<ClickHouseSchemaStatus, StorageError> {
    client.verify_server_version().await?;
    if !deployment_lock_owned && table_exists(client, DEPLOYMENT_LOCK_TABLE).await? {
        return Err(StorageError::Schema(format!(
            "ClickHouse schema deployment lock `{DEPLOYMENT_LOCK_TABLE}` is present; runtime startup is blocked until the deploy owner completes or an operator proves and clears a stale lock"
        )));
    }
    verify_structure(client, deployment_lock_owned, verify_expected_manifest).await?;
    Ok(ClickHouseSchemaStatus {
        required_object_count: REQUIRED_SCHEMA_OBJECTS.len(),
        schema_fingerprint: ContentHash::parse(&schema_contract_hash()).map_err(|error| {
            StorageError::Schema(format!("construct ClickHouse schema fingerprint: {error}"))
        })?,
    })
}

fn ensure_supported_clickhouse_version(version: &str) -> Result<(), StorageError> {
    let (major, minor) = parse_clickhouse_version(version)?;
    if (major, minor) != (SUPPORTED_CLICKHOUSE_MAJOR, SUPPORTED_CLICKHOUSE_MINOR) {
        return Err(StorageError::Schema(format!(
            "ClickHouse {version} is unsupported; this schema contract requires {SUPPORTED_CLICKHOUSE_MAJOR}.{SUPPORTED_CLICKHOUSE_MINOR}.x exactly"
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
        _ => Err(StorageError::Schema(format!(
            "ClickHouse server returned an invalid version string `{version}`"
        ))),
    }
}

async fn verify_structure(
    client: &ClickHouseMaintenanceClient,
    deployment_lock_owned: bool,
    verify_expected_manifest: bool,
) -> Result<(), StorageError> {
    let rows = CLICKHOUSE_SCHEMA_VERIFY
        .maintenance_query(
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
    if deployment_lock_owned {
        allowed.insert(DEPLOYMENT_LOCK_TABLE.to_owned());
    }
    let unknown = by_name
        .keys()
        .filter(|name| !allowed.contains(*name))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        return Err(StorageError::Schema(format!(
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
        return Err(StorageError::Schema(format!(
            "ClickHouse schema is missing required objects: {}",
            missing.join(", ")
        )));
    }
    {
        let name = "book_microstructure_1m_mv";
        let materialized_view = by_name.get(name).ok_or_else(|| {
            StorageError::Schema(format!("ClickHouse materialized view `{name}` is absent"))
        })?;
        if materialized_view.engine != "MaterializedView" {
            return Err(StorageError::Schema(format!(
                "ClickHouse object `{name}` is not a MaterializedView"
            )));
        }
    }
    for name in REQUIRED_SCHEMA_OBJECTS {
        let object = by_name.get(name).ok_or_else(|| {
            StorageError::Schema(format!("ClickHouse managed object `{name}` is absent"))
        })?;
        if schema::extract_table_ttl(&object.create_table_query).is_some() {
            return Err(StorageError::Schema(format!(
                "ClickHouse managed object `{name}` has an unmanaged table TTL"
            )));
        }
    }
    for spec in research_source_registry()
        .map_err(StorageError::Schema)?
        .bindings
        .into_iter()
        .filter(|binding| binding.storage == ResearchSourceStorageKind::ClickHouseTable)
    {
        let table = by_name.get(spec.object.as_str()).ok_or_else(|| {
            StorageError::Schema(format!(
                "ClickHouse raw-history table `{}` is absent",
                spec.object
            ))
        })?;
        let expected_partition = spec.partition_key.as_deref().ok_or_else(|| {
            StorageError::Schema(format!(
                "ClickHouse source binding `{}` has no partition contract",
                spec.object
            ))
        })?;
        if table.partition_key != expected_partition {
            return Err(StorageError::Schema(format!(
                "ClickHouse table `{}` partition key is `{}`, expected `{}`",
                spec.object, table.partition_key, expected_partition
            )));
        }
    }
    if verify_expected_manifest {
        client.verify_schema_manifest().await?;
    }
    Ok(())
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

fn reject_nonempty_schema(names: &BTreeSet<String>) -> Result<(), StorageError> {
    let existing = names
        .iter()
        .filter(|name| name.as_str() != DEPLOYMENT_LOCK_TABLE)
        .cloned()
        .collect::<Vec<_>>();
    if existing.is_empty() {
        return Ok(());
    }
    Err(StorageError::Schema(format!(
        "ClickHouse fresh bootstrap requires an object-empty database; found ({}). Existing and partial schemas are never adopted or resumed; recreate the disposable database before retrying",
        existing.join(", ")
    )))
}

async fn table_exists(
    client: &ClickHouseMaintenanceClient,
    table: &str,
) -> Result<bool, StorageError> {
    let count = CLICKHOUSE_SCHEMA_VERIFY
        .maintenance_query(
            client,
            "SELECT count() FROM system.tables \
             WHERE database = currentDatabase() AND name = ?",
        )
        .bind(table)
        .fetch_one::<u64>()
        .await?;
    Ok(count == 1)
}

fn client(config: &ClickHouseConfig) -> ClickHouseMaintenanceClient {
    let client = Client::default()
        .with_url(&config.url)
        .with_database(&config.database)
        .with_user(&config.user)
        .with_password(config.password.expose_secret())
        .with_setting("max_threads", config.max_threads_per_query.to_string());
    ClickHouseMaintenanceClient::new(
        client,
        ClickHouseIoDeadlines::from(&config.io).maintenance(),
    )
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
    use std::time::Duration;

    use quant_pivot_error::storage::StorageError;
    use quant_pivot_models::config::ClickHouseConfig;

    use super::{
        ensure_supported_clickhouse_version, parse_clickhouse_version, validate_bootstrap_source,
        verify_schema,
    };
    use crate::clickhouse::test_support::NeverResponseServer;

    #[test]
    fn bootstrap_source_is_clean() {
        validate_bootstrap_source().expect("fresh bootstrap must be unconditional create-only DDL");
    }

    #[test]
    fn clickhouse_version_accepts_components() {
        assert_eq!(
            parse_clickhouse_version("26.5.1.882").expect("valid ClickHouse release"),
            (26, 5)
        );
    }

    #[test]
    fn clickhouse_version_rejects_versions() {
        let error = parse_clickhouse_version("26")
            .expect_err("minor version is required for the deduplication contract");
        assert!(error.to_string().contains("invalid version string"));
    }

    #[test]
    fn clickhouse_version_rejects_servers() {
        let error = ensure_supported_clickhouse_version("25.12.9.1")
            .expect_err("older server contract must be rejected");
        assert!(error.to_string().contains("requires 26.5.x exactly"));
        ensure_supported_clickhouse_version("26.5.1.882").expect("supported release line");
        ensure_supported_clickhouse_version("26.6.0.0")
            .expect_err("unverified minor release must be rejected");
        ensure_supported_clickhouse_version("27.0.0.0")
            .expect_err("unverified major release must be rejected");
    }

    #[tokio::test(start_paused = true)]
    async fn maintenance_deadline_is_bounded() {
        let server = NeverResponseServer::start().await;
        let mut config = ClickHouseConfig::default();
        server.url().clone_into(&mut config.url);
        config.io.maintenance_timeout_ms = 50;

        let error = verify_schema(&config)
            .await
            .expect_err("never-response schema verification must reach its deadline");

        assert!(matches!(
            error,
            StorageError::ClickHouseTimeout {
                operation: "ch.storage.database_bootstrap.v1",
                duration
            } if duration == Duration::from_millis(50)
        ));
    }
}
