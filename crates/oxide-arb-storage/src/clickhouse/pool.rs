//! `ClickHouse` client wrapper.

use crate::clickhouse::schema;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::config::ClickHouseConfig;
use tracing::info;

pub struct ClickHousePool {
    client: clickhouse::Client,
    database: String,
}

impl ClickHousePool {
    pub fn connect(config: &ClickHouseConfig) -> Result<Self, StorageError> {
        let client = clickhouse::Client::default()
            .with_url(&config.url)
            .with_database(&config.database)
            .with_user(&config.user)
            .with_password(&config.password);

        info!(
            url = %config.url,
            database = %config.database,
            "ClickHouse client initialized"
        );

        Ok(Self {
            client,
            database: config.database.clone(),
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
