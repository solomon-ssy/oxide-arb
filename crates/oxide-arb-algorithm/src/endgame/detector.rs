//! 8-step endgame detection pipeline.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use oxide_arb_models::{
    config::{CalibrationConfig, EndgameDetectionConfig},
    domain::{
        book::{EndgameBookPair, EndgameBookView},
        calibration::BucketKey,
        opportunity::{EndgameMeta, Opportunity},
    },
    enums::calibration::{DurationBucket, PriceZone},
    enums::common::{MarketCategory, Side, StalenessLevel},
    enums::opportunity::PayoutModel,
    types::{
        Bps, EventId, MarketId, MicroPrice, MicroUsd, OpportunityId, Price, Shares, TokenId, Usd,
    },
};

use crate::calibration::ResolutionCalibrator;
use crate::endgame::confidence::{ConfidenceFusion, compute_realtime_confidence};
use crate::endgame::convergence::{ConvergenceDirection, InMemoryConvergenceTracker};
use crate::fee::FeeEstimator;
use crate::walker::OrderbookWalker;

pub struct EndgameDetector<F: FeeEstimator> {
    enabled: bool,
    settlement_window_hours: u64,
    min_convergence_duration_secs: u64,
    high_threshold: MicroPrice,
    max_investment_usd: MicroUsd,
    min_profit_per_share: Decimal,
    convergence: InMemoryConvergenceTracker,
    calibrator: Arc<ResolutionCalibrator>,
    fusion: ConfidenceFusion,
    fee_estimator: F,
}

impl<F: FeeEstimator> EndgameDetector<F> {
    #[must_use]
    pub fn new(
        config: &EndgameDetectionConfig,
        calibration_config: &CalibrationConfig,
        calibrator: Arc<ResolutionCalibrator>,
        fee_estimator: F,
    ) -> Self {
        Self {
            enabled: config.enabled,
            settlement_window_hours: config.settlement_window_hours,
            min_convergence_duration_secs: config.min_convergence_duration_secs,
            high_threshold: MicroPrice::try_from_decimal(config.high_threshold)
                .unwrap_or(MicroPrice::ZERO),
            max_investment_usd: MicroUsd::try_from_decimal(config.max_investment_usd)
                .unwrap_or(MicroUsd::ZERO),
            min_profit_per_share: config.min_profit_per_share,
            convergence: InMemoryConvergenceTracker::new(&config.convergence_tracker),
            fusion: ConfidenceFusion::new(calibration_config),
            calibrator,
            fee_estimator,
        }
    }

    #[must_use]
    #[inline]
    pub fn detect_direction(&self, book: EndgameBookView<'_>) -> Option<ConvergenceDirection> {
        if book.yes_asks.best_price()?.inner() >= self.high_threshold.to_decimal() {
            return Some(ConvergenceDirection::YesLikely);
        }
        if book.no_asks.best_price()?.inner() >= self.high_threshold.to_decimal() {
            return Some(ConvergenceDirection::NoLikely);
        }
        None
    }

    #[must_use]
    pub fn should_reset_market_state(
        &self,
        direction: Option<ConvergenceDirection>,
        settlement_deadline: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> bool {
        if !self.enabled {
            return true;
        }
        let Some(deadline) = settlement_deadline else {
            return true;
        };
        let hours_remaining = (deadline - now).num_hours();
        if hours_remaining < 0
            || hours_remaining
                > ToPrimitive::to_i64(&self.settlement_window_hours).unwrap_or(i64::MAX)
        {
            return true;
        }
        direction.is_none()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn detect_with_direction(
        &self,
        market_id: &MarketId,
        event_id: &EventId,
        token_yes: &TokenId,
        token_no: &TokenId,
        book: &EndgameBookPair,
        direction: ConvergenceDirection,
        category: MarketCategory,
        staleness: StalenessLevel,
        settlement_deadline: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<Opportunity> {
        if !self.enabled {
            return None;
        }
        let deadline = settlement_deadline?;
        let hours_remaining = (deadline - now).num_hours();
        if hours_remaining < 0
            || hours_remaining
                > ToPrimitive::to_i64(&self.settlement_window_hours).unwrap_or(i64::MAX)
        {
            self.convergence.remove(market_id);
            return None;
        }

        let view = book.view();
        let convergence_secs = self.convergence.update_and_get(market_id, direction, now);
        if convergence_secs < self.min_convergence_duration_secs {
            return None;
        }

        let (target_asks, target_token) = match direction {
            ConvergenceDirection::YesLikely => (view.yes_asks, token_yes),
            ConvergenceDirection::NoLikely => (view.no_asks, token_no),
        };

        let walk = OrderbookWalker::walk_asks_by_cost(
            target_asks.levels,
            self.max_investment_usd,
            self.high_threshold,
            target_asks.total_depth_usd,
        )?;

        let entry_price = Price::new(walk.vwap.to_decimal());
        let shares = Shares::new(walk.shares.to_decimal());
        let total_cost = Usd::new(walk.total_cost.to_decimal());

        let raw_profit_per_share = Decimal::ONE - entry_price.inner();
        if raw_profit_per_share < self.min_profit_per_share {
            return None;
        }

        let bucket_key = BucketKey {
            category,
            price_zone: PriceZone::from_price(entry_price),
            duration_bucket: DurationBucket::from_secs(convergence_secs),
        };
        let cal_entry = self.calibrator.lookup(&bucket_key);

        let realtime_conf = compute_realtime_confidence(
            entry_price.inner(),
            convergence_secs,
            self.high_threshold.to_decimal(),
        );
        let fused_p = self.fusion.fuse(
            cal_entry.posterior_mean(),
            realtime_conf,
            cal_entry.sample_count(),
        );

        let fees = self
            .fee_estimator
            .estimate_fee(shares, entry_price, category, target_token);

        let shares_usd = Usd::new(shares.inner());
        let net_profit = shares_usd - total_cost - fees;
        let expected_payout = shares_usd * fused_p;
        let expected_net_profit = expected_payout - total_cost - fees;

        let cost_plus_fees = total_cost + fees;
        let edge_bps = if cost_plus_fees.is_zero() {
            Bps::ZERO
        } else {
            Bps::new((expected_net_profit.inner() / cost_plus_fees.inner() * dec!(10000)).round())
        };

        let predicted_yes = matches!(direction, ConvergenceDirection::YesLikely);
        let depth_used_pct = Decimal::from(walk.depth_used_pct);

        Some(Opportunity {
            opportunity_id: OpportunityId::pending(),
            market_id: market_id.clone(),
            event_id: event_id.clone(),
            token_id: target_token.clone(),
            side: Side::Buy,
            payout_model: PayoutModel::DirectionalSettlement {
                projected_payout_if_correct: shares_usd,
                expected_payout,
                predicted_side: Side::Buy,
            },
            shares,
            entry_price,
            total_cost,
            total_fees: fees,
            net_profit,
            expected_net_profit,
            edge_bps,
            resolution_adjust: fused_p,
            depth_used_pct,
            staleness,
            category,
            meta: EndgameMeta {
                predicted_yes,
                confidence: fused_p,
                convergence_duration_secs: convergence_secs,
                price_zone: bucket_key.price_zone,
                duration_bucket: bucket_key.duration_bucket,
                settlement_deadline: Some(deadline),
            },
            calibration: cal_entry.to_snapshot(fused_p),
            detected_at: now,
        })
    }

    #[must_use]
    pub const fn convergence_tracker(&self) -> &InMemoryConvergenceTracker {
        &self.convergence
    }
}
