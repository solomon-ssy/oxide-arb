//! Exit-monitor contract.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::PositionInfo,
    enums::execution::{ExitReason, ExitState},
    types::TokenId,
};

/// Exit-monitor evaluation input.
#[derive(Debug, Clone)]
pub struct ExitMonitorInput {
    pub token_id: TokenId,
    pub evaluated_at: DateTime<Utc>,
}

/// Exit-monitor decision for one position.
#[derive(Debug, Clone)]
pub struct ExitMonitorDecision {
    pub position: PositionInfo,
    pub state: ExitState,
    pub reason: Option<ExitReason>,
    pub detail: String,
}

/// Position exit monitor boundary.
#[async_trait]
pub trait ExitMonitor: Send + Sync {
    async fn evaluate(&self, input: ExitMonitorInput) -> QuantResult<ExitMonitorDecision>;
}
