//! Durable run-scoped barrier for exact serving model-input evidence.

use std::sync::Arc;

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    clickhouse::{QuantModelInputEventRow, QuantServingEvidenceCompletionRow},
    domain::DecisionBoundary,
    types::ModelRunId,
};
use quant_pivot_repository::traits::FactWriter;

use super::serving_evidence::{FeatureEvidenceCommitment, completion_marker};

/// Commits model-input rows and a completion marker as an ordered barrier.
pub struct ModelInputEventWriter {
    input_sink: Arc<dyn FactWriter<QuantModelInputEventRow>>,
    completion_sink: Arc<dyn FactWriter<QuantServingEvidenceCompletionRow>>,
}

impl ModelInputEventWriter {
    #[must_use]
    pub const fn new(
        input_sink: Arc<dyn FactWriter<QuantModelInputEventRow>>,
        completion_sink: Arc<dyn FactWriter<QuantServingEvidenceCompletionRow>>,
    ) -> Self {
        Self {
            input_sink,
            completion_sink,
        }
    }

    /// Persist input rows, then persist the run completion marker. Returning
    /// success is the barrier that permits the caller to finalize its model run
    /// as `Succeeded`.
    pub async fn commit_run(
        &self,
        model_run_id: &ModelRunId,
        boundary: &DecisionBoundary,
        features: &FeatureEvidenceCommitment,
        rows: Vec<QuantModelInputEventRow>,
    ) -> QuantResult<QuantServingEvidenceCompletionRow> {
        let marker = completion_marker(
            model_run_id,
            boundary,
            features,
            &rows,
            chrono::Utc::now().timestamp_millis(),
        )?;
        self.input_sink.write_batch(rows).await?;
        self.completion_sink
            .write_batch(vec![marker.clone()])
            .await?;
        Ok(marker)
    }
}
