//! Durable `ClickHouse` sink for long-format serving feature evidence.
//!
//! Serving evidence is business-critical audit state: callers await the
//! `ClickHouse` insert acknowledgement and receive a canonical commitment for
//! the exact batch. It must never pass through the telemetry-class
//! `AsyncWriter`, whose full-channel and terminal flush semantics permit drops.

use std::sync::Arc;

use quant_pivot_error::QuantResult;
use quant_pivot_models::clickhouse::QuantFeatureEventRow;
use quant_pivot_repository::traits::FactWriter;

use super::serving_evidence::{FeatureEvidenceCommitment, feature_commitment};

/// Durably persists already-projected feature-event rows.
pub struct FeatureEventWriter {
    sink: Arc<dyn FactWriter<QuantFeatureEventRow>>,
}

impl FeatureEventWriter {
    /// Build a writer over an acknowledged fact sink.
    #[must_use]
    pub const fn new(sink: Arc<dyn FactWriter<QuantFeatureEventRow>>) -> Self {
        Self { sink }
    }

    /// Persist a complete projected batch and return its producer-side
    /// commitment only after `ClickHouse` acknowledges every row.
    pub async fn write_batch(
        &self,
        rows: Vec<QuantFeatureEventRow>,
    ) -> QuantResult<FeatureEvidenceCommitment> {
        let commitment = feature_commitment(&rows)?;
        self.sink.write_batch(rows).await?;
        Ok(commitment)
    }
}
