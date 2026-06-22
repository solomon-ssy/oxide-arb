//! Execution pipeline domain types.

use crate::{
    domain::trade::TradeObservation,
    enums::{
        common::{MarketCategory, Side, StalenessLevel, TradeBusinessOutcome, TradeState},
        execution::{ExecutionOutcome, ExecutionOutcomeSummary},
    },
    types::{
        Bps, EventId, ExecutionId, MarketId, OpportunityId, OrderId, Price, ReservationId, Shares,
        TokenId, Usd,
    },
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use std::fmt::Display;

/// Fill-time expected net profit: `fused_p * shares - cost - fee`.
///
/// Mirrors the endgame detector's EV formula (`expected_payout - cost - fees`)
/// but substitutes execution-time economics (actual fill price, actual fee)
/// for the detection-time estimates.
///
/// This is **not** realized `PnL` (which requires market settlement), but the
/// best EV estimate at the moment the order is filled.
#[must_use]
pub fn fill_expected_net_profit(
    fused_p: Decimal,
    filled_shares: Shares,
    cost_usd: Usd,
    fee_usd: Usd,
) -> Usd {
    let expected_payout = Usd::new(filled_shares.inner() * fused_p);
    expected_payout - cost_usd - fee_usd
}

/// All information needed to place a single order.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionPlan {
    pub execution_id: ExecutionId,
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub side: Side,
    pub shares: Shares,
    pub limit_price: Price,
    pub estimated_cost: Usd,
    pub estimated_fee: Usd,
    /// Market category for fee calculation at dispatch time (may differ from detection if registry updated).
    pub category: MarketCategory,
    pub neg_risk: bool,
    pub reservation_id: ReservationId,
    pub detected_at: DateTime<Utc>,
    pub planned_at: DateTime<Utc>,
}

/// Result of the full execution pipeline.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionResult {
    pub outcome_summary: Option<ExecutionOutcomeSummary>,
    pub rejection_reason: Option<String>,
    pub rejection_stage: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
}

impl ExecutionResult {
    #[must_use]
    pub fn completed(summary: ExecutionOutcomeSummary) -> Self {
        let now = Utc::now();
        Self {
            outcome_summary: Some(summary),
            rejection_reason: None,
            rejection_stage: None,
            started_at: now,
            completed_at: now,
        }
    }

    #[must_use]
    pub fn rejected(stage: &str, reason: impl Display) -> Self {
        let now = Utc::now();
        Self {
            outcome_summary: None,
            rejection_reason: Some(reason.to_string()),
            rejection_stage: Some(stage.into()),
            started_at: now,
            completed_at: now,
        }
    }

    #[must_use]
    pub const fn is_filled(&self) -> bool {
        matches!(
            self.outcome_summary,
            Some(ExecutionOutcomeSummary::Filled { .. })
        )
    }

    #[must_use]
    pub const fn is_miss(&self) -> bool {
        matches!(self.outcome_summary, Some(ExecutionOutcomeSummary::Miss))
    }

    #[must_use]
    pub const fn is_rejected(&self) -> bool {
        self.rejection_reason.is_some()
    }
}

/// Validation result snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct ValidationResult {
    pub current_price: Price,
    pub staleness: StalenessLevel,
    pub slippage_bps: Bps,
    pub validated_at: DateTime<Utc>,
}

/// Handle to an active capital reservation.
#[derive(Debug, Clone)]
pub struct ReservationHandle {
    pub id: ReservationId,
    pub amount: Usd,
    pub market_id: MarketId,
}

// ── Post-trade runtime types ─────────────────────────────────────────

/// Flattened, resolved fields from an [`ExecutionOutcome`] — computed once,
/// shared by all post-trade consumers (PG trade UPDATE, risk accounting, CH audit).
///
/// `net_profit_usd` is the **fill-time EV** (`fused_p * shares - cost - fee`),
/// not realized `PnL` (which requires market settlement — see phase 4.3g).
/// Miss and Failed outcomes produce `None`.
#[derive(Debug, Clone)]
pub struct ResolvedOutcome {
    pub business_outcome: TradeBusinessOutcome,
    pub filled_shares: Shares,
    pub avg_fill_price: Price,
    pub cost_usd: Usd,
    pub fee_usd: Usd,
    pub net_profit_usd: Option<Usd>,
    pub order_id: Option<OrderId>,
    pub tx_hash: Option<String>,
    pub latency_ms: Option<u64>,
    pub error_message: Option<String>,
}

impl ResolvedOutcome {
    /// Single resolution point for known [`ExecutionOutcome`] values.
    ///
    /// For fills, `net_profit_usd` is computed from actual execution economics
    /// and the frozen `resolution_prob` via [`fill_expected_net_profit`].
    ///
    /// `entry_price` is the planned entry (fallback when the venue omits an
    /// average fill price); `resolution_prob` is the frozen scored probability.
    /// Returns `None` for [`ExecutionOutcome::Unknown`], which must enter the
    /// reconciliation queue instead of being flattened into a business outcome.
    #[must_use]
    pub fn try_resolve(
        outcome: &ExecutionOutcome,
        entry_price: Price,
        resolution_prob: f64,
    ) -> Option<Self> {
        match outcome {
            ExecutionOutcome::Filled {
                order_id,
                filled_shares,
                avg_fill_price,
                fee_paid,
                tx_hash,
                latency_ms,
                ..
            } => {
                let price = avg_fill_price.unwrap_or(entry_price);
                let cost = *filled_shares * price;
                let fused_p = Decimal::try_from(resolution_prob).unwrap_or(Decimal::ZERO);
                let ev = fill_expected_net_profit(fused_p, *filled_shares, cost, *fee_paid);
                Some(Self {
                    business_outcome: TradeBusinessOutcome::Success,
                    filled_shares: *filled_shares,
                    avg_fill_price: price,
                    cost_usd: cost,
                    fee_usd: *fee_paid,
                    net_profit_usd: Some(ev),
                    order_id: Some(order_id.clone()),
                    tx_hash: tx_hash.clone(),
                    latency_ms: Some(*latency_ms),
                    error_message: None,
                })
            }
            ExecutionOutcome::Miss { reason, .. } => Some(Self {
                business_outcome: TradeBusinessOutcome::Miss,
                filled_shares: Shares::ZERO,
                avg_fill_price: entry_price,
                cost_usd: Usd::ZERO,
                fee_usd: Usd::ZERO,
                net_profit_usd: None,
                order_id: None,
                tx_hash: None,
                latency_ms: None,
                error_message: Some(reason.clone()),
            }),
            ExecutionOutcome::Failed { error, .. } => Some(Self {
                business_outcome: TradeBusinessOutcome::Failed,
                filled_shares: Shares::ZERO,
                avg_fill_price: entry_price,
                cost_usd: Usd::ZERO,
                fee_usd: Usd::ZERO,
                net_profit_usd: None,
                order_id: None,
                tx_hash: None,
                latency_ms: None,
                error_message: Some(error.clone()),
            }),
            ExecutionOutcome::Unknown { .. } => None,
        }
    }

    /// The `*_observed` state this outcome transitions the trade row into.
    #[must_use]
    pub const fn observed_state(&self) -> TradeState {
        match self.business_outcome {
            TradeBusinessOutcome::Success => TradeState::FillObserved,
            TradeBusinessOutcome::Miss => TradeState::MissObserved,
            TradeBusinessOutcome::Failed => TradeState::FailObserved,
        }
    }

    /// Build the venue-result observation written when the outcome becomes known.
    #[must_use]
    pub fn to_observation(&self) -> TradeObservation {
        TradeObservation {
            state: self.observed_state(),
            shares: self.filled_shares,
            price: self.avg_fill_price,
            cost_usd: self.cost_usd,
            fee_usd: self.fee_usd,
            order_id: self.order_id.clone(),
            tx_hash: self.tx_hash.clone(),
            net_profit_usd: self.net_profit_usd,
            latency_ms: self
                .latency_ms
                .map(|ms| i32::try_from(ms).unwrap_or(i32::MAX)),
            error_message: self.error_message.clone(),
            confirmed_at: Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enums::legacy::LegacyExecutionMode, types::OrderId};
    use rust_decimal_macros::dec;

    /// Planned entry price + frozen resolution probability used across resolve tests.
    fn resolve(outcome: &ExecutionOutcome) -> ResolvedOutcome {
        ResolvedOutcome::try_resolve(outcome, Price::new(dec!(0.92)), 0.95)
            .expect("test outcome is known")
    }

    #[test]
    fn fill_expected_net_profit_matches_detector_formula() {
        // fused_p=0.99, 100 shares @ 0.92, fee=0.40
        // expected_payout = 100 * 0.99 = 99
        // ev = 99 - 92 - 0.40 = 6.60
        let ev = fill_expected_net_profit(
            dec!(0.99),
            Shares::new(dec!(100)),
            Usd::new(dec!(92)),
            Usd::new(dec!(0.40)),
        );
        assert_eq!(ev, Usd::new(dec!(6.60)));
    }

    #[test]
    fn resolve_filled_computes_cost_from_fill() {
        let resolved = resolve(&ExecutionOutcome::Filled {
            order_id: OrderId::new("ord1"),
            filled_shares: Shares::new(dec!(80)),
            avg_fill_price: Some(Price::new(dec!(0.93))),
            fee_paid: Usd::new(dec!(0.50)),
            tx_hash: None,
            execution_mode: LegacyExecutionMode::Paper,
            latency_ms: 42,
        });
        // cost = 80 * 0.93 = 74.40
        assert_eq!(resolved.cost_usd, Usd::new(dec!(74.40)));
        assert_eq!(resolved.fee_usd, Usd::new(dec!(0.50)));
        assert_eq!(resolved.filled_shares, Shares::new(dec!(80)));
        assert_eq!(resolved.observed_state(), TradeState::FillObserved);
    }

    #[test]
    fn resolve_filled_net_profit_uses_fill_ev() {
        let resolved = resolve(&ExecutionOutcome::Filled {
            order_id: OrderId::new("ord1"),
            filled_shares: Shares::new(dec!(100)),
            avg_fill_price: Some(Price::new(dec!(0.92))),
            fee_paid: Usd::new(dec!(0.40)),
            tx_hash: None,
            execution_mode: LegacyExecutionMode::Paper,
            latency_ms: 10,
        });
        // resolution_prob = 0.95 → fused_p = 0.95
        // expected_payout = 100 * 0.95 = 95
        // cost = 100 * 0.92 = 92
        // ev = 95 - 92 - 0.40 = 2.60
        assert_eq!(resolved.net_profit_usd, Some(Usd::new(dec!(2.60))));
    }

    #[test]
    fn resolve_miss_zeros_cost_and_fee() {
        let resolved = resolve(&ExecutionOutcome::Miss {
            reason: "no fill".into(),
            execution_mode: LegacyExecutionMode::Paper,
        });
        assert_eq!(resolved.business_outcome, TradeBusinessOutcome::Miss);
        assert_eq!(resolved.observed_state(), TradeState::MissObserved);
        assert_eq!(resolved.cost_usd, Usd::ZERO);
        assert_eq!(resolved.fee_usd, Usd::ZERO);
        assert_eq!(resolved.net_profit_usd, None);
        assert_eq!(resolved.filled_shares, Shares::ZERO);
    }

    #[test]
    fn resolve_failed_zeros_cost_and_fee() {
        let resolved = resolve(&ExecutionOutcome::Failed {
            error: "timeout".into(),
            execution_mode: LegacyExecutionMode::Paper,
        });
        assert_eq!(resolved.business_outcome, TradeBusinessOutcome::Failed);
        assert_eq!(resolved.observed_state(), TradeState::FailObserved);
        assert_eq!(resolved.cost_usd, Usd::ZERO);
        assert_eq!(resolved.fee_usd, Usd::ZERO);
        assert_eq!(resolved.net_profit_usd, None);
    }

    #[test]
    fn to_observation_clamps_latency_and_sets_observed_state() {
        let resolved = resolve(&ExecutionOutcome::Filled {
            order_id: OrderId::new("ord1"),
            filled_shares: Shares::new(dec!(10)),
            avg_fill_price: Some(Price::new(dec!(0.5))),
            fee_paid: Usd::ZERO,
            tx_hash: None,
            execution_mode: LegacyExecutionMode::Paper,
            latency_ms: u64::MAX,
        });
        let observation = resolved.to_observation();
        assert_eq!(observation.latency_ms, Some(i32::MAX));
        assert_eq!(observation.state, TradeState::FillObserved);
    }
}
