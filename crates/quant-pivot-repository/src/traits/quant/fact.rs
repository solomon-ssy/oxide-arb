use quant_pivot_error::storage::StorageError;
use quant_pivot_models::clickhouse::{
    QuantCapitalAllocationEventRow, QuantExecutionEventRow, QuantExitSignalEvaluationEventRow,
    QuantFactorEventRow, QuantFeatureEventRow, QuantFeatureParityEventRow, QuantModelInputEventRow,
    QuantPositionEventRow, QuantRecommendationAttributionEventRow,
    QuantReportRecommendationFactRow, QuantServingEvidenceCompletionRow,
    QuantSignalCandidateEventRow, ReportMarketFunnelRow,
};

/// Generic batch sink for one `ClickHouse` fact stream.
///
/// Book/telemetry callers may place this sink behind an `AsyncWriter`. Serving
/// evidence callers await it directly and must not declare completion until
/// the insert acknowledgement is returned.
#[async_trait::async_trait]
pub trait FactWriter<T: Send>: Send + Sync {
    /// Persist a batch of fact rows. Best-effort callers may log a failure;
    /// durable callers propagate it and fail closed.
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

    async fn insert_model_input_events(
        &self,
        rows: Vec<QuantModelInputEventRow>,
    ) -> Result<(), StorageError>;

    async fn insert_serving_evidence_completions(
        &self,
        rows: Vec<QuantServingEvidenceCompletionRow>,
    ) -> Result<(), StorageError>;

    async fn insert_feature_parity_events(
        &self,
        rows: Vec<QuantFeatureParityEventRow>,
    ) -> Result<(), StorageError>;

    async fn insert_signal_candidate_events(
        &self,
        rows: Vec<QuantSignalCandidateEventRow>,
    ) -> Result<(), StorageError>;

    async fn insert_report_recommendation_facts(
        &self,
        rows: Vec<QuantReportRecommendationFactRow>,
    ) -> Result<(), StorageError>;

    async fn insert_report_market_funnel(
        &self,
        rows: Vec<ReportMarketFunnelRow>,
    ) -> Result<(), StorageError>;

    async fn insert_execution_events(
        &self,
        rows: Vec<QuantExecutionEventRow>,
    ) -> Result<(), StorageError>;

    async fn insert_capital_allocation_events(
        &self,
        rows: Vec<QuantCapitalAllocationEventRow>,
    ) -> Result<(), StorageError>;

    async fn insert_position_events(
        &self,
        rows: Vec<QuantPositionEventRow>,
    ) -> Result<(), StorageError>;

    async fn insert_recommendation_attribution_events(
        &self,
        rows: Vec<QuantRecommendationAttributionEventRow>,
    ) -> Result<(), StorageError>;

    async fn insert_exit_signal_evaluation_events(
        &self,
        rows: Vec<QuantExitSignalEvaluationEventRow>,
    ) -> Result<(), StorageError>;
}
