//! 8-step endgame detection pipeline.
//!
//! Endgame-only by design (ADR-001): there is no enable/disable switch.
//! Detection parameters are hot-reloadable through [`EndgameDetector::reload`]
//! (lock-free `ArcSwap` parameter snapshot); the convergence tracker state is
//! preserved across reloads so accumulated convergence durations survive a
//! runtime-config activation.

use crate::{
    calibration::ResolutionCalibrator,
    endgame::{
        confidence::{ConfidenceFusion, compute_realtime_confidence},
        convergence::{ConvergenceDirection, InMemoryConvergenceTracker},
    },
    fee::FeeEstimator,
    walker::{OrderbookWalker, WalkResult},
};
use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_models::{
    domain::{
        book::{EndgameBookPair, EndgameBookView},
        calibration::BucketKey,
        opportunity::{EndgameMeta, Opportunity},
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::{MarketCategory, Side, StalenessLevel},
        opportunity::PayoutModel,
    },
    runtime_config::{CalibrationConfig, EndgameDetectionConfig},
    types::{
        Bps, EventId, MarketId, MicroPrice, MicroProb, MicroUsd, OpportunityId, Price, Shares,
        TokenId, Usd,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;

struct OpportunityBuildCtx<'a> {
    input: &'a EndgameDetectInput<'a>,
    walk: WalkResult,
    target_token: &'a TokenId,
    entry_price: Price,
    convergence_secs: u64,
    deadline: DateTime<Utc>,
    now: DateTime<Utc>,
    fusion: &'a ConfidenceFusion,
    high_threshold: MicroPrice,
}

/// Inputs for a single directional endgame detection pass.
pub struct EndgameDetectInput<'a> {
    pub market_id: &'a MarketId,
    pub event_id: &'a EventId,
    pub token_yes: &'a TokenId,
    pub token_no: &'a TokenId,
    pub book: &'a EndgameBookPair,
    pub direction: ConvergenceDirection,
    pub category: MarketCategory,
    pub staleness: StalenessLevel,
    pub settlement_deadline: Option<DateTime<Utc>>,
}

/// Hot-swappable detection parameter snapshot (one allocation per reload).
struct DetectorParams {
    settlement_window_hours: u64,
    min_convergence_duration_secs: u64,
    high_threshold: MicroPrice,
    max_investment_usd: MicroUsd,
    min_profit_per_share: MicroPrice,
    fusion: ConfidenceFusion,
}

impl DetectorParams {
    fn from_config(config: &EndgameDetectionConfig, calibration: &CalibrationConfig) -> Self {
        Self {
            settlement_window_hours: config.settlement_window_hours,
            min_convergence_duration_secs: config.min_convergence_duration_secs,
            high_threshold: MicroPrice::try_from_decimal(config.high_threshold)
                .unwrap_or(MicroPrice::ZERO),
            max_investment_usd: MicroUsd::try_from_decimal(config.max_investment_usd)
                .unwrap_or(MicroUsd::ZERO),
            min_profit_per_share: MicroPrice::try_from_decimal(config.min_profit_per_share)
                .unwrap_or(MicroPrice::ZERO),
            fusion: ConfidenceFusion::new(calibration),
        }
    }
}

pub struct EndgameDetector<F: FeeEstimator> {
    params: ArcSwap<DetectorParams>,
    convergence: InMemoryConvergenceTracker,
    calibrator: Arc<ResolutionCalibrator>,
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
            params: ArcSwap::from_pointee(DetectorParams::from_config(config, calibration_config)),
            convergence: InMemoryConvergenceTracker::new(&config.convergence_tracker),
            calibrator,
            fee_estimator,
        }
    }

    /// Hot-reload detection parameters (runtime-config activation).
    ///
    /// Scalar thresholds and the confidence fusion are swapped atomically. The
    /// convergence tracker keeps its state: the idle eviction bound is updated
    /// in place, while capacity stays at its construction value (restart-bound)
    /// so accumulated convergence durations survive the activation.
    pub fn reload(&self, config: &EndgameDetectionConfig, calibration: &CalibrationConfig) {
        self.params
            .store(Arc::new(DetectorParams::from_config(config, calibration)));
        self.convergence
            .set_max_idle_secs(config.convergence_tracker.max_idle_secs);
    }

    #[must_use]
    #[inline]
    pub fn detect_direction(&self, book: EndgameBookView<'_>) -> Option<ConvergenceDirection> {
        let high_threshold = self.params.load().high_threshold;
        if book
            .yes_asks
            .best_price_micro()
            .is_some_and(|p| p.micro() >= high_threshold.micro())
        {
            return Some(ConvergenceDirection::YesLikely);
        }
        if book
            .no_asks
            .best_price_micro()
            .is_some_and(|p| p.micro() >= high_threshold.micro())
        {
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
        let Some(deadline) = settlement_deadline else {
            return true;
        };
        let settlement_window_hours = self.params.load().settlement_window_hours;
        let hours_remaining = (deadline - now).num_hours();
        if hours_remaining < 0
            || hours_remaining > ToPrimitive::to_i64(&settlement_window_hours).unwrap_or(i64::MAX)
        {
            return true;
        }
        direction.is_none()
    }

    #[inline]
    pub fn detect_with_direction(
        &self,
        input: &EndgameDetectInput<'_>,
        now: DateTime<Utc>,
    ) -> Option<Opportunity> {
        let params = self.params.load();
        let deadline = input.settlement_deadline?;
        let hours_remaining = (deadline - now).num_hours();
        if hours_remaining < 0
            || hours_remaining
                > ToPrimitive::to_i64(&params.settlement_window_hours).unwrap_or(i64::MAX)
        {
            self.convergence.remove(input.market_id);
            return None;
        }

        let view = input.book.view();
        let convergence_secs =
            self.convergence
                .update_and_get(input.market_id, input.direction, now);
        if convergence_secs < params.min_convergence_duration_secs {
            return None;
        }

        let (target_asks, target_token) = match input.direction {
            ConvergenceDirection::YesLikely => (view.yes_asks, input.token_yes),
            ConvergenceDirection::NoLikely => (view.no_asks, input.token_no),
        };

        let walk = OrderbookWalker::walk_asks_by_cost(
            target_asks.levels,
            params.max_investment_usd,
            params.high_threshold,
            target_asks.total_depth_usd,
        )?;

        let entry_price_micro = walk.vwap;
        let entry_price = Price::new(entry_price_micro.to_decimal());
        let min_edge = MicroPrice::ONE
            .micro()
            .saturating_sub(entry_price_micro.micro());
        if min_edge < params.min_profit_per_share.micro() {
            return None;
        }

        let ctx = OpportunityBuildCtx {
            input,
            walk,
            target_token,
            entry_price,
            convergence_secs,
            deadline,
            now,
            fusion: &params.fusion,
            high_threshold: params.high_threshold,
        };
        Some(self.build_opportunity(&ctx))
    }

    fn build_opportunity(&self, ctx: &OpportunityBuildCtx<'_>) -> Opportunity {
        let input = ctx.input;
        let walk = ctx.walk;
        let target_token = ctx.target_token;
        let entry_price = ctx.entry_price;
        let convergence_secs = ctx.convergence_secs;
        let deadline = ctx.deadline;
        let now = ctx.now;
        let shares = Shares::new(walk.shares.to_decimal());
        let total_cost = Usd::new(walk.total_cost.to_decimal());

        let bucket_key = BucketKey {
            category: input.category,
            price_zone: PriceZone::from_price(entry_price),
            duration_bucket: DurationBucket::from_secs(convergence_secs),
        };
        let cal_entry = self.calibrator.lookup(&bucket_key);

        let entry_price_micro =
            MicroPrice::try_from_decimal(entry_price.inner()).unwrap_or(MicroPrice::ZERO);
        let realtime_conf =
            compute_realtime_confidence(entry_price_micro, convergence_secs, ctx.high_threshold);
        let fused_p = ctx.fusion.fuse(
            MicroProb::try_from_decimal(cal_entry.posterior_mean()).unwrap_or(MicroProb::ZERO),
            realtime_conf,
            cal_entry.sample_count(),
        );
        let fused_p_dec = fused_p.to_decimal();

        let fees =
            self.fee_estimator
                .estimate_fee(shares, entry_price, input.category, target_token);

        let shares_usd = Usd::new(shares.inner());
        let net_profit = shares_usd - total_cost - fees;
        let expected_payout = shares_usd * fused_p_dec;
        let expected_net_profit = expected_payout - total_cost - fees;

        let cost_plus_fees = total_cost + fees;
        let edge_bps = if cost_plus_fees.is_zero() {
            Bps::ZERO
        } else {
            Bps::new((expected_net_profit.inner() / cost_plus_fees.inner() * dec!(10000)).round())
        };

        let predicted_yes = matches!(input.direction, ConvergenceDirection::YesLikely);

        Opportunity {
            opportunity_id: OpportunityId::from_v7(),
            market_id: input.market_id.clone(),
            event_id: input.event_id.clone(),
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
            resolution_adjust: fused_p_dec,
            depth_used_pct: Decimal::from(walk.depth_used_pct),
            staleness: input.staleness,
            category: input.category,
            meta: EndgameMeta {
                predicted_yes,
                confidence: fused_p_dec,
                convergence_duration_secs: convergence_secs,
                price_zone: bucket_key.price_zone,
                duration_bucket: bucket_key.duration_bucket,
                settlement_deadline: Some(deadline),
            },
            calibration: cal_entry.to_snapshot(fused_p_dec),
            detected_at: now,
        }
    }

    #[must_use]
    pub const fn convergence_tracker(&self) -> &InMemoryConvergenceTracker {
        &self.convergence
    }
}
