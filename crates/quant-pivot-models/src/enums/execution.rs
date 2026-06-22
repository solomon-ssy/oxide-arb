//! Execution pipeline enums.

use crate::{
    enums::common::ExecutionMode,
    types::{OrderId, Price, Shares, Usd},
};
use serde::Serialize;

/// Outcome of a single execution attempt.
#[derive(Debug, Clone, Serialize)]
pub enum ExecutionOutcome {
    Filled {
        order_id: OrderId,
        filled_shares: Shares,
        avg_fill_price: Option<Price>,
        fee_paid: Usd,
        tx_hash: Option<String>,
        execution_mode: ExecutionMode,
        latency_ms: u64,
    },
    Miss {
        reason: String,
        execution_mode: ExecutionMode,
    },
    Failed {
        error: String,
        execution_mode: ExecutionMode,
    },
    Unknown {
        reason: String,
        execution_mode: ExecutionMode,
    },
}

/// Lightweight execution outcome for pipeline results — no clone of full [`ExecutionOutcome`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ExecutionOutcomeSummary {
    Filled { order_id: OrderId },
    Miss,
    Failed,
    Unknown,
}

impl ExecutionOutcomeSummary {
    #[must_use]
    pub fn from_outcome(outcome: &ExecutionOutcome) -> Self {
        match outcome {
            ExecutionOutcome::Filled { order_id, .. } => Self::Filled {
                order_id: order_id.clone(),
            },
            ExecutionOutcome::Miss { .. } => Self::Miss,
            ExecutionOutcome::Failed { .. } => Self::Failed,
            ExecutionOutcome::Unknown { .. } => Self::Unknown,
        }
    }
}
