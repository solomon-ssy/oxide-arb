//! `ClickHouse` client wrapper.

use crate::clickhouse::migration;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::config::ClickHouseConfig;
use tracing::info;

pub struct ClickHousePool {
    client: clickhouse::Client,
}

impl ClickHousePool {
    /// Connect to a schema-managed database without performing DDL.
    pub async fn connect(config: &ClickHouseConfig) -> Result<Self, StorageError> {
        let pool = Self::from_config(config);
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

    /// Verify the immutable schema contract using metadata reads only.
    pub async fn verify_schema(&self) -> Result<migration::ClickHouseSchemaStatus, StorageError> {
        migration::verify_schema_client(&self.client).await
    }
}
