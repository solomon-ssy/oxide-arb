//! Append-only operation-log repository contract.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::{
    api::OperationLogQuery,
    governance::{NewOperationLog, OperationLogInfo},
    pagination::Paginated,
};

/// Persistence for the immutable operation-log activity trail.
///
/// Only INSERT and SELECT are exposed: rows are never updated or deleted (the
/// DB layer enforces append-only). `detail` must arrive already redacted.
#[async_trait::async_trait]
pub trait OperationLogRepository: Send + Sync {
    /// Append a single row.
    async fn append(&self, log: NewOperationLog) -> Result<(), StorageError>;

    /// Append many rows in one transaction — used by the async buffered writer.
    async fn append_batch(&self, logs: Vec<NewOperationLog>) -> Result<(), StorageError>;

    /// Paginated, filtered query ordered by `occurred_at desc`.
    async fn page(
        &self,
        query: OperationLogQuery,
    ) -> Result<Paginated<OperationLogInfo>, StorageError>;
}
