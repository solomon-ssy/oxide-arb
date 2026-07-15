//! Server-owned source-slice materialization ledger.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{BeginSourceSliceOutcome, CompleteSourceSlice, NewSourceSlice, SourceSliceInfo},
    types::{ContentHash, SourceSliceId},
};

#[async_trait::async_trait]
pub trait SourceSliceRepository: Send + Sync {
    /// Atomically claim a canonical identity or return the concurrent winner.
    async fn begin_or_get(
        &self,
        source_slice: NewSourceSlice,
    ) -> Result<BeginSourceSliceOutcome, StorageError>;

    async fn find_by_id(
        &self,
        source_slice_id: &SourceSliceId,
    ) -> Result<Option<SourceSliceInfo>, StorageError>;

    async fn find_by_identity(
        &self,
        identity_hash: &ContentHash,
    ) -> Result<Option<SourceSliceInfo>, StorageError>;

    /// CAS `Materializing → Ready` after all objects and manifest are verified.
    async fn complete(
        &self,
        source_slice_id: &SourceSliceId,
        completion: CompleteSourceSlice,
    ) -> Result<SourceSliceInfo, StorageError>;

    /// CAS `Materializing → Failed` while retaining the immutable identity.
    async fn fail(
        &self,
        source_slice_id: &SourceSliceId,
        detail: String,
    ) -> Result<SourceSliceInfo, StorageError>;
}
