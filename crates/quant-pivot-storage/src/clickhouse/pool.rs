//! `ClickHouse` client wrapper.

use crate::clickhouse::{ensure, schema};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::config::{ClickHouseConfig, ClickHouseRawLifecycleConfig};
use tracing::info;

pub struct ClickHousePool {
    client: clickhouse::Client,
    raw_lifecycle: ClickHouseRawLifecycleConfig,
}

impl ClickHousePool {
    /// Connect to the configured database, creating it on first boot when absent.
    pub async fn connect(config: &ClickHouseConfig) -> Result<Self, StorageError> {
        ensure::ensure_database(config).await?;
        info!(
            url = %config.url,
            database = %config.database,
            "ClickHouse client initialized"
        );
        Ok(Self::from_config(config))
    }

    /// Build a pool from config without opening a connection or ensuring the database.
    ///
    /// Used by [`Self::connect`] after bootstrap checks. Callers that only need
    /// the handle (and never invoke [`Self::health_check`]) may construct directly.
    #[must_use]
    pub fn from_config(config: &ClickHouseConfig) -> Self {
        Self {
            client: clickhouse::Client::default()
                .with_url(&config.url)
                .with_database(&config.database)
                .with_user(&config.user)
                .with_password(&config.password),
            raw_lifecycle: config.raw_lifecycle.clone(),
        }
    }

    pub const fn client(&self) -> &clickhouse::Client {
        &self.client
    }

    pub async fn health_check(&self) -> Result<(), StorageError> {
        self.client
            .query("SELECT 1")
            .fetch_one::<u8>()
            .await
            .map_err(|e| {
                StorageError::Connection(format!("ClickHouse health check failed: {e}"))
            })?;
        Ok(())
    }

    pub async fn ensure_schema(&self) -> Result<(), StorageError> {
        for ddl in schema::all_ddl() {
            self.client.query(&ddl).execute().await?;
        }
        self.apply_raw_lifecycle().await?;
        info!("ClickHouse schema ensured");
        Ok(())
    }

    async fn apply_raw_lifecycle(&self) -> Result<(), StorageError> {
        let lifecycle = &self.raw_lifecycle;
        for spec in schema::RAW_LIFECYCLE_TABLES {
            let query = match &lifecycle.cold_volume {
                Some(volume) if lifecycle.delete_enabled => format!(
                    "ALTER TABLE {} MODIFY TTL {} + INTERVAL {} DAY TO VOLUME '{}', {} + INTERVAL {} DAY DELETE",
                    spec.table,
                    spec.time_column,
                    lifecycle.hot_days,
                    volume,
                    spec.time_column,
                    lifecycle.retention_days,
                ),
                Some(volume) => format!(
                    "ALTER TABLE {} MODIFY TTL {} + INTERVAL {} DAY TO VOLUME '{}'",
                    spec.table, spec.time_column, lifecycle.hot_days, volume,
                ),
                None => format!("ALTER TABLE {} REMOVE TTL", spec.table),
            };
            self.client.query(&query).execute().await?;
        }
        info!(
            hot_days = lifecycle.hot_days,
            retention_days = lifecycle.retention_days,
            cold_volume = lifecycle.cold_volume.as_deref().unwrap_or("disabled"),
            delete_enabled = lifecycle.delete_enabled,
            retention_plan_bound = lifecycle.signed_retention_plan_hash.is_some(),
            "ClickHouse native raw lifecycle applied"
        );
        Ok(())
    }
}
