//! Typed execution boundary for deterministic feature-parity research jobs.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use tokio_util::sync::CancellationToken;

use crate::domain::{
    api::{FeatureParityJobParams, FeatureParityRunView},
    quant::JobProgressSink,
};

/// Executes the full comparison ladder (selection through business prediction).
///
/// The durable worker depends on this explicit port so parity can never fall
/// through to another research job kind or report success without evidence.
#[async_trait]
pub trait FeatureParityExecutionPort: Send + Sync {
    async fn execute(
        &self,
        params: FeatureParityJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeatureParityRunView>;
}
