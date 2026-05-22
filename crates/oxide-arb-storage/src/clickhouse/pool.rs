//! `ClickHouse` client wrapper.

use crate::{clickhouse::schema, error::StorageError};
use oxide_arb_models::config::AnalyticsConfig;
use tracing::info;

pub struct ClickHousePool {
    client: clickhouse::Client,
    database: String,
}

impl ClickHousePool {
    #[allow(clippy::unused_async)]
    pub async fn connect(config: &AnalyticsConfig) -> Result<Self, StorageError> {
        let client = clickhouse::Client::default()
            .with_url(&config.clickhouse_url)
            .with_database(&config.clickhouse_database)
            .with_user(&config.clickhouse_user)
            .with_password(&config.clickhouse_password);

        info!(
            url = %config.clickhouse_url,
            database = %config.clickhouse_database,
            "ClickHouse client initialized"
        );

        Ok(Self {
            client,
            database: config.clickhouse_database.clone(),
        })
    }

    pub const fn client(&self) -> &clickhouse::Client {
        &self.client
    }

    pub fn database(&self) -> &str {
        &self.database
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
            self.client.query(ddl).execute().await?;
        }
        info!("ClickHouse schema ensured");
        Ok(())
    }
}
