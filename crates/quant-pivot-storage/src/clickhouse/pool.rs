//! `ClickHouse` client wrapper.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use clickhouse::Client;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::config::{ClickHouseConfig, ClickHouseResourceGovernance};
use serde::Deserialize;
use tokio::sync::Semaphore;
use tracing::info;

use super::{
    bootstrap::ClickHouseSchemaStatus,
    deadline::ClickHouseIoDeadlines,
    query::{ChReadMetrics, ClickHouseMaintenanceClient},
    query_limits::{CLICKHOUSE_HEALTH, CLICKHOUSE_RESOURCE_GOVERNANCE},
};

const REQUIRED_SERVER_SETTINGS: &[(&str, &str)] = &[
    ("background_merges_mutations_concurrency_ratio", "1"),
    ("background_pool_size", "4"),
    ("background_schedule_pool_size", "8"),
    ("max_concurrent_queries", "64"),
    ("max_thread_pool_size", "256"),
];
const REQUIRED_MERGE_TREE_SETTINGS: &[(&str, &str)] = &[
    ("number_of_free_entries_in_pool_to_execute_mutation", "1"),
    (
        "number_of_free_entries_in_pool_to_execute_optimize_entire_partition",
        "1",
    ),
    (
        "number_of_free_entries_in_pool_to_lower_max_size_of_merge",
        "1",
    ),
];

#[derive(clickhouse::Row, Deserialize)]
struct ServerSettingRow {
    name: String,
    value: String,
}

#[derive(clickhouse::Row, Deserialize)]
struct SystemLogInventoryRow {
    query_log_count: u64,
    forbidden_log_count: u64,
}

pub struct ClickHousePool {
    client: Client,
    maintenance_client: ClickHouseMaintenanceClient,
    read_permits: Arc<Semaphore>,
    read_metrics: Arc<ChReadMetrics>,
    deadlines: ClickHouseIoDeadlines,
}

impl ClickHousePool {
    /// Connect to a schema-managed database without performing DDL.
    pub async fn connect(config: &ClickHouseConfig) -> Result<Self, StorageError> {
        let pool = Self::from_config(config);
        if config.resource_governance == ClickHouseResourceGovernance::SelfManaged {
            pool.verify_resource_governance().await?;
        }
        pool.health_check().await?;
        info!(
            url = %config.url,
            database = %config.database,
            "ClickHouse client initialized"
        );
        Ok(pool)
    }

    /// Build a pool from config without opening a connection or ensuring the database.
    ///
    /// Used by [`Self::connect`] after bootstrap checks. Callers that only need
    /// the handle (and never invoke [`Self::health_check`]) may construct directly.
    #[must_use]
    pub fn from_config(config: &ClickHouseConfig) -> Self {
        let deadlines = ClickHouseIoDeadlines::from(&config.io);
        let maintenance = Client::default()
            .with_url(&config.url)
            .with_database(&config.database)
            .with_user(&config.user)
            .with_password(config.password.expose_secret());
        Self {
            client: maintenance
                .clone()
                .with_setting("max_threads", config.max_threads_per_query.to_string())
                .with_setting("priority", "8")
                .with_setting("use_concurrency_control", "1")
                .with_setting("log_queries", "1")
                .with_setting("log_queries_probability", "0.01")
                .with_setting("log_query_threads", "0")
                .with_setting("log_processors_profiles", "0"),
            maintenance_client: ClickHouseMaintenanceClient::new(
                maintenance,
                deadlines.maintenance(),
            ),
            read_permits: Arc::new(Semaphore::new(config.max_concurrent_reads)),
            read_metrics: Arc::new(ChReadMetrics::new()),
            deadlines,
        }
    }

    pub const fn client(&self) -> &Client {
        &self.client
    }

    pub(super) fn read_permits(&self) -> Arc<Semaphore> {
        Arc::clone(&self.read_permits)
    }

    pub(super) const fn query_deadline(&self) -> Duration {
        self.deadlines.query()
    }

    pub(super) fn query_server_seconds(&self) -> u64 {
        self.deadlines.query_seconds_ceil()
    }

    const fn maintenance_client(&self) -> &ClickHouseMaintenanceClient {
        &self.maintenance_client
    }

    pub const fn read_metrics(&self) -> &Arc<ChReadMetrics> {
        &self.read_metrics
    }

    pub async fn health_check(&self) -> Result<(), StorageError> {
        CLICKHOUSE_HEALTH
            .query(self, "SELECT 1")
            .fetch_one::<u8>()
            .await?;
        Ok(())
    }

    async fn verify_resource_governance(&self) -> Result<(), StorageError> {
        let client = self.maintenance_client();
        CLICKHOUSE_RESOURCE_GOVERNANCE
            .maintenance_query(client, "SELECT 1")
            .with_setting("log_queries", "1")
            .with_setting("log_queries_probability", "1")
            .fetch_one::<u8>()
            .await?;
        CLICKHOUSE_RESOURCE_GOVERNANCE
            .maintenance_query(client, "SYSTEM FLUSH LOGS")
            .with_setting("log_queries", "1")
            .with_setting("log_queries_probability", "1")
            .execute()
            .await?;
        let settings = CLICKHOUSE_RESOURCE_GOVERNANCE
            .maintenance_query(
                client,
                "SELECT name, value FROM system.server_settings \
                 WHERE name IN ('background_merges_mutations_concurrency_ratio', \
                 'background_pool_size', 'background_schedule_pool_size', \
                 'max_concurrent_queries', 'max_thread_pool_size') ORDER BY name",
            )
            .fetch_all::<ServerSettingRow>()
            .await?
            .into_iter()
            .map(|row| (row.name, row.value))
            .collect::<BTreeMap<_, _>>();
        for &(name, expected) in REQUIRED_SERVER_SETTINGS {
            if settings.get(name).map(String::as_str) != Some(expected) {
                return Err(StorageError::Connection(format!(
                    "ClickHouse resource governance requires server setting {name}={expected}, observed {:?}",
                    settings.get(name)
                )));
            }
        }

        let merge_tree = CLICKHOUSE_RESOURCE_GOVERNANCE
            .maintenance_query(
                client,
                "SELECT name, value FROM system.merge_tree_settings \
                 WHERE name IN ('number_of_free_entries_in_pool_to_execute_mutation', \
                 'number_of_free_entries_in_pool_to_execute_optimize_entire_partition', \
                 'number_of_free_entries_in_pool_to_lower_max_size_of_merge') ORDER BY name",
            )
            .fetch_all::<ServerSettingRow>()
            .await?
            .into_iter()
            .map(|row| (row.name, row.value))
            .collect::<BTreeMap<_, _>>();
        for &(name, expected) in REQUIRED_MERGE_TREE_SETTINGS {
            if merge_tree.get(name).map(String::as_str) != Some(expected) {
                return Err(StorageError::Connection(format!(
                    "ClickHouse resource governance requires MergeTree setting {name}={expected}, observed {:?}",
                    merge_tree.get(name)
                )));
            }
        }

        let logs = CLICKHOUSE_RESOURCE_GOVERNANCE
            .maintenance_query(
                client,
                "SELECT countIf(name = 'query_log') AS query_log_count, \
                 countIf(name IN ('metric_log', 'asynchronous_metric_log', 'text_log', \
                 'trace_log', 'processors_profile_log', 'query_thread_log', 'query_views_log', \
                 'query_metric_log', 'part_log', 'background_schedule_pool_log', \
                 'asynchronous_insert_log')) AS forbidden_log_count \
                 FROM system.tables WHERE database = 'system'",
            )
            .fetch_one::<SystemLogInventoryRow>()
            .await?;
        if logs.query_log_count != 1 || logs.forbidden_log_count != 0 {
            return Err(StorageError::Connection(format!(
                "ClickHouse system-log governance requires query_log only; query_log_count={}, forbidden_log_count={}",
                logs.query_log_count, logs.forbidden_log_count
            )));
        }
        Ok(())
    }

    /// Verify the immutable schema contract using metadata reads only.
    pub async fn verify_schema(&self) -> Result<ClickHouseSchemaStatus, StorageError> {
        self.maintenance_client().verify_schema().await
    }
}
