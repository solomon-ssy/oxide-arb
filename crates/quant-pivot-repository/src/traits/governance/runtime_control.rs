//! Atomic runtime-control singleton repository.

use async_trait::async_trait;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::governance::{RuntimeControlInfo, RuntimeControlUpdate};

#[async_trait]
pub trait RuntimeControlRepository: Send + Sync {
    /// Load the required fresh-boot singleton.
    async fn load(&self) -> Result<RuntimeControlInfo, StorageError>;

    /// Apply an expected-revision transition and append its audit row in one transaction.
    async fn compare_and_set(
        &self,
        update: RuntimeControlUpdate,
    ) -> Result<RuntimeControlInfo, StorageError>;
}
