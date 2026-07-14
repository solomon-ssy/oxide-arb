//! `ClickHouse` client wrapper.

use crate::clickhouse::{ensure, schema};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::config::ClickHouseConfig;
use tracing::info;

pub struct ClickHousePool {
    client: clickhouse::Client,
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
        info!("ClickHouse schema ensured");
        Ok(())
    }
}
