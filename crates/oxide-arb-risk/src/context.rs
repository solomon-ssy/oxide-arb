//! Immutable decision context for pre-trade risk evaluation.
//!
//! [`PreTradeContext`] borrows a frozen [`RiskSnapshot`] and a point-in-time
//! [`RiskMetricsSnapshot`]. Checks read only from this context — no I/O or
//! subsystem locks on the hot path.

use chrono::{DateTime, Utc};
use oxide_arb_models::domain::opportunity::Opportunity;
use oxide_arb_models::domain::risk::ProbabilityInput;
use oxide_arb_models::types::Usd;
use rust_decimal::Decimal;

use crate::snapshot::RiskSnapshot;
use crate::traits::RiskMetricsSnapshot;
use crate::types::DrawdownAction;

/// Circuit breaker gate snapshot for pre-trade evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CircuitBreakerGate {
    pub allows_trading: bool,
    pub is_probe: bool,
}

/// Manual halt gate state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualHaltGate {
    Clear,
    Halted { reason: String },
}

impl ManualHaltGate {
    #[must_use]
    #[inline]
    pub const fn allows_trading(&self) -> bool {
        matches!(self, Self::Clear)
    }

    #[must_use]
    pub fn denial_detail(&self) -> Option<String> {
        match self {
            Self::Clear => None,
            Self::Halted { reason } => Some(reason.clone()),
        }
    }
}

/// Blacklist gate state for the trading path (legacy tests / audit only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlacklistGate {
    Clear,
    Blocked { detail: String },
}

impl BlacklistGate {
    #[must_use]
    #[inline]
    pub const fn allows_trading(&self) -> bool {
        matches!(self, Self::Clear)
    }

    #[must_use]
    pub fn denial_detail(&self) -> Option<String> {
        match self {
            Self::Clear => None,
            Self::Blocked { detail } => Some(detail.clone()),
        }
    }
}

/// Borrowed view of all state for one pre-trade decision.
#[derive(Debug, Clone, Copy)]
pub struct PreTradeContext<'a> {
    pub opportunity: &'a Opportunity,
    pub probability: ProbabilityInput,
    pub snap: &'a RiskSnapshot,
    pub metrics: RiskMetricsSnapshot,
    pub now: DateTime<Utc>,
}

impl PreTradeContext<'_> {
    #[inline]
    pub const fn manual_halt(&self) -> &ManualHaltGate {
        &self.snap.circuit_breaker.manual_halt
    }

    #[inline]
    pub const fn circuit_breaker(&self) -> CircuitBreakerGate {
        self.snap.circuit_breaker.circuit_breaker
    }

    #[inline]
    pub const fn market_exposure_before(&self) -> Usd {
        self.metrics.market_exposure
    }

    #[inline]
    pub const fn total_exposure_before(&self) -> Usd {
        self.metrics.total_exposure
    }

    #[inline]
    pub const fn total_potential_loss(&self) -> Usd {
        self.snap.total_potential_loss
    }

    #[inline]
    pub const fn active_reservation_count(&self) -> usize {
        self.metrics.active_reservation_count
    }

    #[inline]
    pub const fn reserved_usd(&self) -> Usd {
        self.metrics.reserved_usd
    }

    #[inline]
    pub const fn open_position_count(&self) -> usize {
        self.metrics.open_position_count
    }

    #[inline]
    pub const fn cached_balance(&self) -> Usd {
        self.metrics.cached_balance
    }

    #[inline]
    pub const fn ws_disconnect_secs(&self) -> u64 {
        self.metrics.ws_disconnect_secs
    }

    #[inline]
    pub const fn open_directional_count_same_side(&self) -> usize {
        self.metrics.open_directional_count(self.opportunity.side)
    }

    #[inline]
    pub const fn daily_directional_trades_same_side(&self) -> u32 {
        self.metrics.daily_directional_trades(self.opportunity.side)
    }

    #[inline]
    pub const fn consecutive_market_misses(&self) -> u32 {
        self.metrics.consecutive_market_misses
    }

    #[inline]
    pub const fn hourly_loss(&self) -> Usd {
        self.snap.hourly.hourly_loss
    }

    #[inline]
    pub const fn daily_loss(&self) -> Usd {
        self.snap.daily.daily_loss
    }

    #[inline]
    pub const fn daily_budget_remaining(&self) -> Usd {
        self.snap.daily.daily_budget_remaining
    }

    #[inline]
    pub const fn weekly_loss(&self) -> Usd {
        self.snap.weekly.weekly_loss
    }

    #[inline]
    pub const fn daily_pnl(&self) -> Usd {
        self.snap.daily.daily_pnl
    }

    #[inline]
    pub const fn api_error_count(&self) -> u64 {
        self.metrics.api_error_count
    }

    #[inline]
    pub const fn api_request_count(&self) -> u64 {
        self.metrics.api_request_count
    }

    #[must_use]
    pub fn drawdown_factor(&self) -> Decimal {
        self.snap
            .drawdown
            .sizing_factor(self.metrics.cached_balance)
    }

    #[must_use]
    pub fn drawdown_action(&self) -> DrawdownAction {
        self.snap.drawdown.evaluate(self.metrics.cached_balance).1
    }

    /// Bloom fast-path negative, then exact confirm from snapshot (no live blacklist read).
    #[must_use]
    pub fn is_market_blacklisted_trading_path(&self) -> Option<String> {
        self.snap
            .blacklist
            .trading_path_block_detail(&self.opportunity.market_id)
    }

    #[must_use]
    pub fn is_token_blacklisted(&self) -> bool {
        self.snap
            .blacklist
            .is_token_blacklisted(&self.opportunity.token_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{BlacklistSnapshot, RiskSnapshot};
    use oxide_arb_models::types::MarketId;

    #[test]
    fn bloom_negative_skips_confirm() {
        let snap = RiskSnapshot {
            blacklist: BlacklistSnapshot::empty(),
            ..RiskSnapshot::zeroed()
        };
        let opp = make_test_opportunity();
        let ctx = PreTradeContext {
            opportunity: &opp,
            probability: make_test_probability(),
            snap: &snap,
            metrics: RiskMetricsSnapshot::zeroed(),
            now: Utc::now(),
        };
        assert!(ctx.is_market_blacklisted_trading_path().is_none());
        assert!(!ctx.is_token_blacklisted());
    }

    fn make_test_probability() -> ProbabilityInput {
        use rust_decimal_macros::dec;
        ProbabilityInput {
            calibrated_win_prob: dec!(0.9),
            fill_prob: dec!(0.9),
            calibration_confidence: dec!(0.9),
            sample_size: 10,
            model_staleness_secs: 0,
            expected_slippage_pct: dec!(0),
            expected_failure_cost_pct: dec!(0),
        }
    }

    fn make_test_opportunity() -> Opportunity {
        use chrono::Utc;
        use oxide_arb_models::domain::calibration::{BucketKey, CalibrationSnapshot};
        use oxide_arb_models::domain::opportunity::{EndgameMeta, Opportunity};
        use oxide_arb_models::enums::calibration::{DurationBucket, PriceZone};
        use oxide_arb_models::enums::common::{MarketCategory, Side, StalenessLevel};
        use oxide_arb_models::enums::opportunity::PayoutModel;
        use oxide_arb_models::types::{Bps, EventId, OpportunityId, Price, Shares, TokenId, Usd};
        use rust_decimal_macros::dec;

        Opportunity {
            opportunity_id: OpportunityId::new_v7(),
            market_id: MarketId::new("m"),
            event_id: EventId::new("e"),
            token_id: TokenId::new("t"),
            side: Side::Buy,
            payout_model: PayoutModel::DirectionalSettlement {
                projected_payout_if_correct: Usd::ZERO,
                expected_payout: Usd::ZERO,
                predicted_side: Side::Buy,
            },
            shares: Shares::ZERO,
            entry_price: Price::new(dec!(0.5)),
            total_cost: Usd::ZERO,
            total_fees: Usd::ZERO,
            net_profit: Usd::ZERO,
            expected_net_profit: Usd::ZERO,
            edge_bps: Bps::ZERO,
            resolution_adjust: dec!(1),
            depth_used_pct: dec!(1),
            staleness: StalenessLevel::Fresh,
            category: MarketCategory::Politics,
            meta: EndgameMeta {
                predicted_yes: true,
                confidence: dec!(0.9),
                convergence_duration_secs: 0,
                price_zone: PriceZone::Z97,
                duration_bucket: DurationBucket::Medium,
                settlement_deadline: None,
            },
            calibration: CalibrationSnapshot {
                bucket_key: BucketKey {
                    category: MarketCategory::Politics,
                    price_zone: PriceZone::Z97,
                    duration_bucket: DurationBucket::Medium,
                },
                posterior_mean: dec!(0.9),
                sample_size: 10,
                alpha_prior: dec!(1),
                beta_prior: dec!(1),
                fallback_tier: 1,
                fused_probability: dec!(0.9),
            },
            detected_at: Utc::now(),
        }
    }
}
