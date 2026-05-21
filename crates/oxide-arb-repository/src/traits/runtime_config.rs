use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::runtime_config::{self, RuntimeConfigKey};

pub trait RuntimeConfigRepository: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<runtime_config::Model>, StorageError>;

    async fn get_typed(
        &self,
        key: RuntimeConfigKey,
    ) -> Result<Option<runtime_config::Model>, StorageError>;

    async fn set(
        &self,
        key: &str,
        value: &serde_json::Value,
        updated_by: &str,
    ) -> Result<runtime_config::Model, StorageError>;

    async fn set_typed(
        &self,
        key: RuntimeConfigKey,
        value: &serde_json::Value,
        updated_by: &str,
    ) -> Result<runtime_config::Model, StorageError>;

    async fn get_all(&self) -> Result<Vec<runtime_config::Model>, StorageError>;

    async fn delete(&self, key: &str) -> Result<bool, StorageError>;
}
