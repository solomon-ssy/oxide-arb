//! CPCV path-set ledger repository trait.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::BacktestPathSetListQuery,
        pagination::Paginated,
        quant::{BacktestPathSetInfo, NewBacktestPathSet},
    },
    types::{BacktestPathSetId, ContentHash, ModelVersionId},
};

/// One atomic CPCV path-set append and exact `ModelRun` terminal transition.
pub struct CpcvPathSetCommit {
    pub path_set: NewBacktestPathSet,
    pub input_hash: ContentHash,
}

/// Persistence port for the append-only CPCV + trial-grid validation ledger.
#[async_trait::async_trait]
pub trait BacktestPathSetRepository: Send + Sync {
    /// Insert a new path-set row, returning the persisted projection.
    async fn create(
        &self,
        path_set: NewBacktestPathSet,
    ) -> Result<BacktestPathSetInfo, StorageError>;

    /// Atomically append one path set and succeed its exact pre-assigned run.
    /// Exact retries return the stored path set; every other collision fails closed.
    async fn commit_cpcv(
        &self,
        commit: CpcvPathSetCommit,
    ) -> Result<BacktestPathSetInfo, StorageError>;

    /// Look up a path set by id.
    async fn find_by_id(
        &self,
        path_set_id: &BacktestPathSetId,
    ) -> Result<Option<BacktestPathSetInfo>, StorageError>;

    /// List path sets for a model version, most recent first.
    async fn list_by_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Vec<BacktestPathSetInfo>, StorageError>;

    /// Page the ledger for the operator catalog, newest (`created_at`) first.
    async fn page(
        &self,
        query: BacktestPathSetListQuery,
    ) -> Result<Paginated<BacktestPathSetInfo>, StorageError>;
}
