//! Venue fill and fee provenance shared by execution and reconciliation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    domain::market::fee::DeferredVenueIncentive,
    enums::fee::FeeLiquidityRole,
    types::{
        Bps, ContentHash, EvmAddress, EvmTransactionHash, OrderId, Price, Shares, Usd, VenueTradeId,
    },
};

/// Strength of the evidence supporting a venue fee observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeMeasurementPriority {
    PreparedExpected,
    AuthenticatedTradeDerived,
    OnChainSettled,
}

/// Fee measurement with explicit provenance. Expected and authenticated
/// derived values never masquerade as chain-settled facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FeeMeasurement {
    PreparedExpected {
        schedule_hash: ContentHash,
        expected_fee: Usd,
    },
    AuthenticatedTradeDerived {
        trade_id: VenueTradeId,
        bucket_index: u32,
        order_id: OrderId,
        liquidity_role: FeeLiquidityRole,
        fee_rate_bps: Bps,
        expected_fee: Usd,
        derived_fee: Usd,
        /// Present only for an authenticated maker fill covered by the frozen
        /// decision-time Gamma schedule.
        expected_maker_rebate: Option<DeferredVenueIncentive>,
        transaction_hash: Option<EvmTransactionHash>,
        matched_at: DateTime<Utc>,
        maker_order_ids: Vec<OrderId>,
    },
    OnChainSettled {
        venue_trade_id: VenueTradeId,
        chain_id: u64,
        protocol_version: u16,
        exchange_address: EvmAddress,
        order_id: OrderId,
        liquidity_role: FeeLiquidityRole,
        transaction_hash: EvmTransactionHash,
        log_index: u64,
        matched_at: DateTime<Utc>,
        available_at: DateTime<Utc>,
        settled_fee: Usd,
        builder_code: Option<String>,
    },
}

impl FeeMeasurement {
    #[must_use]
    pub const fn priority(&self) -> FeeMeasurementPriority {
        match self {
            Self::PreparedExpected { .. } => FeeMeasurementPriority::PreparedExpected,
            Self::AuthenticatedTradeDerived { .. } => {
                FeeMeasurementPriority::AuthenticatedTradeDerived
            }
            Self::OnChainSettled { .. } => FeeMeasurementPriority::OnChainSettled,
        }
    }

    #[must_use]
    pub const fn fee(&self) -> Usd {
        match self {
            Self::PreparedExpected { expected_fee, .. } => *expected_fee,
            Self::AuthenticatedTradeDerived { derived_fee, .. } => *derived_fee,
            Self::OnChainSettled { settled_fee, .. } => *settled_fee,
        }
    }

    #[must_use]
    pub const fn is_settled(&self) -> bool {
        matches!(self, Self::OnChainSettled { .. })
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
    pub fee_evidence: FeeMeasurement,
}

#[cfg(test)]
mod tests {
    use super::{FeeMeasurement, FeeMeasurementPriority};
    use crate::{
        enums::fee::FeeLiquidityRole,
        types::{EvmAddress, EvmTransactionHash, OrderId, Usd, VenueTradeId},
    };

    #[test]
    fn fee_evidence_priority_monotonic() {
        let prepared = FeeMeasurementPriority::PreparedExpected;
        let authenticated = FeeMeasurementPriority::AuthenticatedTradeDerived;
        let on_chain = FeeMeasurementPriority::OnChainSettled;

        assert!(prepared < authenticated);
        assert!(authenticated < on_chain);
        assert!(
            !FeeMeasurement::OnChainSettled {
                venue_trade_id: VenueTradeId::new("trade-1"),
                chain_id: 137,
                protocol_version: 2,
                exchange_address: EvmAddress::parse(format!("0x{}", "a".repeat(40)))
                    .expect("exchange address"),
                order_id: OrderId::new("0x01"),
                liquidity_role: FeeLiquidityRole::Taker,
                transaction_hash: EvmTransactionHash::parse(format!("0x{}", "b".repeat(64)))
                    .expect("transaction hash"),
                log_index: 1,
                matched_at: chrono::Utc::now(),
                available_at: chrono::Utc::now(),
                settled_fee: Usd::ZERO,
                builder_code: None,
            }
            .fee()
            .is_positive()
        );
    }
}
