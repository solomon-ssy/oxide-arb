//! Endgame opportunity enums.

use crate::enums::common::Side;
use crate::types::Usd;
use serde::{Deserialize, Serialize};

/// Settlement payout model for endgame strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PayoutModel {
    DirectionalSettlement {
        /// Payout if our prediction is correct: `shares * $1.00`.
        projected_payout_if_correct: Usd,
        /// Expected payout: `fused_p * projected_payout_if_correct`.
        expected_payout: Usd,
        predicted_side: Side,
    },
}

impl PayoutModel {
    /// Single source of truth for expected `PnL` computation.
    pub fn compute_pnl(&self, total_cost: Usd, total_fees: Usd) -> Usd {
        match self {
            Self::DirectionalSettlement {
                expected_payout, ..
            } => *expected_payout - total_cost - total_fees,
        }
    }
}
