use quant_pivot_error::storage::StorageError;
use quant_pivot_models::clickhouse::{
    QuantExecutionEventRow, QuantFactorEventRow, QuantFeatureEventRow, QuantRecommendationEventRow,
    QuantSignalCandidateEventRow,
};

/// Generic batch sink for one `ClickHouse` fact stream.
///
/// This is the single telemetry write abstraction: book ingest facts and quant
/// pipeline facts are both persisted through `FactWriter<Row>` implementations,
/// all funnelling into the storage `ChWriteManager` (permit + retry + metrics).
#[async_trait::async_trait]
pub trait FactWriter<T: Send>: Send + Sync {
    /// Persist a batch of fact rows. Best-effort callers (e.g. the hot-path
    /// `AsyncWriter` flush) log and drop on error; durable callers propagate it.
    async fn write_batch(&self, rows: Vec<T>) -> Result<(), StorageError>;
}

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
