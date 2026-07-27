use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::FactorDefinitionListQuery,
        pagination::Paginated,
        quant::{
            FactorDefinitionInfo, FactorRegistrationOutcome, FactorValueInfo,
            LatestFactorSnapshotBundleInfo, LatestFactorSnapshotInfo, NewFactorDefinition,
            NewFactorValue,
        },
    },
    types::{FactorDefinitionId, FeatureVectorId, MarketId, ModelRunId, ModelVersionId},
};

/// Factor definition and value persistence port.
#[async_trait::async_trait]
pub trait FactorRepository: Send + Sync {
    /// Atomically register one canonical batch of immutable factor revisions.
    ///
    /// Implementations reject duplicate names, IDs, or hashes before writing
    /// and return outcomes in canonical factor-name order. Exact retries are
    /// idempotent; any persisted identity collision rolls back the entire batch.
    async fn register_definitions(
        &self,
        definitions: Vec<NewFactorDefinition>,
    ) -> Result<Vec<FactorRegistrationOutcome>, StorageError>;

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

    /// Page the immutable factor-definition catalog, newest (`created_at`) first.
    async fn page_definitions(
        &self,
        query: FactorDefinitionListQuery,
    ) -> Result<Paginated<FactorDefinitionInfo>, StorageError>;

    async fn list_values_for_run(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<Vec<FactorValueInfo>, StorageError>;

    /// Batch-load exact factor rows by their immutable source feature vectors.
    async fn find_values_by_vectors(
        &self,
        feature_vector_ids: &[FeatureVectorId],
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

    /// Latest scored value visible by `available_by` for the exact online PIT
    /// binding. Only succeeded live-inference runs are serving-visible.
    async fn latest_snapshot(
        &self,
        factor_definition_id: &FactorDefinitionId,
        market_id: &MarketId,
        model_version_id: &ModelVersionId,
        available_by: DateTime<Utc>,
    ) -> Result<Option<LatestFactorSnapshotInfo>, StorageError>;

    /// Latest coherent factor plane visible by `available_by` for one exact
    /// market/model binding.
    ///
    /// Implementations must select one exact feature vector in a single
    /// serving run containing every requested definition. Values from
    /// different vectors or runs must never be mixed. A newer incomplete plane
    /// must not mask an older complete plane.
    async fn latest_snapshot_bundle(
        &self,
        _factor_definition_ids: &[FactorDefinitionId],
        _market_id: &MarketId,
        _model_version_id: &ModelVersionId,
        _available_by: DateTime<Utc>,
    ) -> Result<Option<LatestFactorSnapshotBundleInfo>, StorageError> {
        Ok(None)
    }
}
