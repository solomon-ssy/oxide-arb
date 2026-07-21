use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::FactorDefinitionListQuery,
        pagination::Paginated,
        quant::{
            FactorDefinitionInfo, FactorValueInfo, LatestFactorSnapshotBundleInfo,
            LatestFactorSnapshotInfo, NewFactorDefinition, NewFactorValue,
        },
    },
    types::{FactorDefinitionId, MarketId, ModelRunId, ModelVersionId},
};

/// Factor definition and value persistence port.
#[async_trait::async_trait]
pub trait FactorRepository: Send + Sync {
    async fn create_definition(
        &self,
        definition: NewFactorDefinition,
    ) -> Result<FactorDefinitionInfo, StorageError>;

    async fn create_values(
        &self,
        values: Vec<NewFactorValue>,
    ) -> Result<Vec<FactorValueInfo>, StorageError>;

    async fn find_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> Result<Option<FactorDefinitionInfo>, StorageError>;

    async fn find_definitions_by_ids(
        &self,
        factor_definition_ids: &[FactorDefinitionId],
    ) -> Result<Vec<FactorDefinitionInfo>, StorageError>;

    /// Page the factor-definition governance catalog, newest (`created_at`) first.
    async fn page_definitions(
        &self,
        query: FactorDefinitionListQuery,
    ) -> Result<Paginated<FactorDefinitionInfo>, StorageError>;

    async fn publish_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> Result<FactorDefinitionInfo, StorageError>;

    /// Atomically publish all requested revisions, returning only definitions
    /// changed by this call and retiring any previously published revision with
    /// the same logical name in the same transaction.
    async fn publish_definitions(
        &self,
        factor_definition_ids: &[FactorDefinitionId],
    ) -> Result<Vec<FactorDefinitionInfo>, StorageError>;

    async fn retire_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> Result<FactorDefinitionInfo, StorageError>;

    async fn list_values_for_run(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<Vec<FactorValueInfo>, StorageError>;

    /// Factor values for the given definitions within `[from, until)`, ascending
    /// by `as_of`. Research-only input for factor-collinearity analysis; serving
    /// normalization must never call this mutable-history query.
    async fn recent_values(
        &self,
        factor_definition_ids: &[FactorDefinitionId],
        from: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<FactorValueInfo>, StorageError>;

    /// Latest scored value for the exact online PIT binding.
    async fn latest_snapshot(
        &self,
        factor_definition_id: &FactorDefinitionId,
        market_id: &MarketId,
        model_version_id: &ModelVersionId,
    ) -> Result<Option<LatestFactorSnapshotInfo>, StorageError>;

    /// Latest coherent factor plane for one exact market/model binding.
    ///
    /// Implementations must select a single serving run containing every
    /// requested definition; values from different runs must never be mixed.
    async fn latest_snapshot_bundle(
        &self,
        _factor_definition_ids: &[FactorDefinitionId],
        _market_id: &MarketId,
        _model_version_id: &ModelVersionId,
    ) -> Result<Option<LatestFactorSnapshotBundleInfo>, StorageError> {
        Ok(None)
    }
}
