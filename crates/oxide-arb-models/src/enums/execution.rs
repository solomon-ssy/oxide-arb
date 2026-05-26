//! Execution pipeline enums.

use crate::enums::common::ExecutionMode;
use crate::types::{OrderId, Price, Shares, Usd};
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
}
