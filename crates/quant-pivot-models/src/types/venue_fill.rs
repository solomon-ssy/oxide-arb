//! Venue fill and fee provenance shared by execution and reconciliation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    enums::fee::FeeLiquidityRole,
    types::{Bps, ContentHash, OrderId, Price, Shares, Usd},
};

/// Strength of the evidence supporting a venue fee observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeEvidencePriority {
    PreparedScheduleExpected,
    AuthenticatedTradeReconstructed,
    OnChainExact,
}

/// Fee evidence with explicit provenance. Expected fees never masquerade as
/// venue-observed facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FeeEvidence {
    PreparedScheduleExpected {
        schedule_hash: ContentHash,
        expected_fee: Usd,
    },
    AuthenticatedTradeReconstructed {
        trade_id: String,
        order_id: OrderId,
        liquidity_role: FeeLiquidityRole,
        fee_rate_bps: Bps,
        reconstructed_fee: Usd,
        transaction_hash: String,
        matched_at: DateTime<Utc>,
        maker_order_ids: Vec<OrderId>,
    },
    OnChainExact {
        order_id: OrderId,
        liquidity_role: FeeLiquidityRole,
        transaction_hash: String,
        log_index: u64,
        matched_at: DateTime<Utc>,
        actual_fee: Usd,
        builder_code: Option<String>,
    },
}

impl FeeEvidence {
    #[must_use]
    pub const fn priority(&self) -> FeeEvidencePriority {
        match self {
            Self::PreparedScheduleExpected { .. } => FeeEvidencePriority::PreparedScheduleExpected,
            Self::AuthenticatedTradeReconstructed { .. } => {
                FeeEvidencePriority::AuthenticatedTradeReconstructed
            }
            Self::OnChainExact { .. } => FeeEvidencePriority::OnChainExact,
        }
    }

    #[must_use]
    pub const fn fee(&self) -> Usd {
        match self {
            Self::PreparedScheduleExpected { expected_fee, .. } => *expected_fee,
            Self::AuthenticatedTradeReconstructed {
                reconstructed_fee, ..
            } => *reconstructed_fee,
            Self::OnChainExact { actual_fee, .. } => *actual_fee,
        }
    }

    #[must_use]
    pub const fn is_observed(&self) -> bool {
        !matches!(self, Self::PreparedScheduleExpected { .. })
    }
}

/// One order-side fill observation. `maker_order_ids` preserves authenticated
/// CLOB trade structure instead of flattening all legs into the taker order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VenueFillObservation {
    pub order_id: OrderId,
    pub liquidity_role: FeeLiquidityRole,
    pub filled_shares: Shares,
    pub average_price: Price,
    pub matched_at: DateTime<Utc>,
    pub maker_order_ids: Vec<OrderId>,
    pub builder_code: Option<String>,
    pub fee_evidence: FeeEvidence,
}

#[cfg(test)]
mod tests {
    use super::{FeeEvidence, FeeEvidencePriority};
    use crate::{
        enums::fee::FeeLiquidityRole,
        types::{OrderId, Usd},
    };

    #[test]
    fn fee_evidence_priority_is_strictly_monotonic() {
        let prepared = FeeEvidencePriority::PreparedScheduleExpected;
        let authenticated = FeeEvidencePriority::AuthenticatedTradeReconstructed;
        let on_chain = FeeEvidencePriority::OnChainExact;

        assert!(prepared < authenticated);
        assert!(authenticated < on_chain);
        assert!(
            !FeeEvidence::OnChainExact {
                order_id: OrderId::new("0x01"),
                liquidity_role: FeeLiquidityRole::Taker,
                transaction_hash: "0x01".to_owned(),
                log_index: 1,
                matched_at: chrono::Utc::now(),
                actual_fee: Usd::ZERO,
                builder_code: None,
            }
            .fee()
            .is_positive()
        );
    }
}
