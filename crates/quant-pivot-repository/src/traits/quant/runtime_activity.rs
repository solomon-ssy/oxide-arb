//! Read-only cross-ledger projection for the operator Activity Center.

use async_trait::async_trait;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::api::{RuntimeActivityPageView, RuntimeActivityReadQuery};

#[async_trait]
pub trait RuntimeActivityRepository: Send + Sync {
    /// Read one permission-scoped keyset page from existing durable fact tables.
    async fn page(
        &self,
        query: RuntimeActivityReadQuery,
    ) -> Result<RuntimeActivityPageView, StorageError>;
}
