//! Execution pipeline enums.

use crate::enums::common::ExecutionMode;
use crate::types::{OrderId, Price, Shares, Usd};
use serde::{Deserialize, Serialize};

/// Execution state machine states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExecState {
    Idle,
    Validate,
    Exec,
    Emergency,
}

impl ExecState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Validate => "validate",
            Self::Exec => "exec",
            Self::Emergency => "emergency",
        }
    }
}

impl std::fmt::Display for ExecState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

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
