use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{RuntimeConfigInfo, UpsertRuntimeConfig},
    enums::runtime_config::RuntimeConfigKey,
};

#[async_trait::async_trait]
pub trait RuntimeConfigRepository: Send + Sync {
    async fn get(&self, key: RuntimeConfigKey) -> Result<Option<RuntimeConfigInfo>, StorageError>;

    async fn upsert(&self, config: UpsertRuntimeConfig) -> Result<RuntimeConfigInfo, StorageError>;

    async fn get_all(&self) -> Result<Vec<RuntimeConfigInfo>, StorageError>;

    async fn delete(&self, key: RuntimeConfigKey) -> Result<bool, StorageError>;
}
