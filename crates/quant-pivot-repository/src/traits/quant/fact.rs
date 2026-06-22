use quant_pivot_error::storage::StorageError;
use quant_pivot_models::clickhouse::{
    QuantExecutionEventRow, QuantFactorEventRow, QuantFeatureEventRow, QuantRecommendationEventRow,
    QuantSignalCandidateEventRow,
};

#[async_trait::async_trait]
pub trait QuantFactRepository: Send + Sync {
    async fn insert_feature_events(
        &self,
        rows: Vec<QuantFeatureEventRow>,
    ) -> Result<(), StorageError>;

    async fn insert_factor_events(
        &self,
        rows: Vec<QuantFactorEventRow>,
    ) -> Result<(), StorageError>;

    async fn insert_signal_candidate_events(
        &self,
        rows: Vec<QuantSignalCandidateEventRow>,
    ) -> Result<(), StorageError>;

    async fn insert_recommendation_events(
        &self,
        rows: Vec<QuantRecommendationEventRow>,
    ) -> Result<(), StorageError>;

    async fn insert_execution_events(
        &self,
        rows: Vec<QuantExecutionEventRow>,
    ) -> Result<(), StorageError>;
}
