//! 8-step endgame detection pipeline.
//!
//! Pure computation: all I/O dependencies (fees, calibration data) are
//! injected via traits or pre-loaded in-memory structures.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use oxide_arb_models::{
    config::{CalibrationConfig, EndgameDetectionConfig},
    domain::{
        book::EndgameBookSnapshot,
        calibration::{BucketKey, DurationBucket, PriceZone},
        opportunity::{EndgameMeta, Opportunity, PayoutModel},
    },
    enums::common::{MarketCategory, Side, StalenessLevel},
    types::{Bps, EventId, MarketId, OpportunityId, TokenId, Usd},
};

use crate::calibration::ResolutionCalibrator;
use crate::endgame::confidence::{ConfidenceFusion, compute_realtime_confidence};
use crate::endgame::convergence::{ConvergenceDirection, InMemoryConvergenceTracker};
use crate::fee::FeeEstimator;
use crate::walker::OrderbookWalker;

/// Core endgame detection engine.
///
/// Holds all stateful components (convergence tracker, calibrator) and
/// configuration. Designed to be constructed once and reused across scans.
pub struct EndgameDetector {
    config: EndgameDetectionConfig,
    // TODO(redis-backend): switch this field to `RedisConvergenceTracker`
    // before running multiple scanner instances against the same markets.
    convergence: InMemoryConvergenceTracker,
    calibrator: Arc<ResolutionCalibrator>,
    fusion: ConfidenceFusion,
    fee_estimator: Arc<dyn FeeEstimator>,
}

impl EndgameDetector {
    /// Construct a new detector with all dependencies.
    #[must_use]
    pub fn new(
        config: EndgameDetectionConfig,
        calibration_config: &CalibrationConfig,
        calibrator: Arc<ResolutionCalibrator>,
        fee_estimator: Arc<dyn FeeEstimator>,
    ) -> Self {
        Self {
            convergence: InMemoryConvergenceTracker::new(&config.convergence_tracker),
            fusion: ConfidenceFusion::new(calibration_config),
            config,
            calibrator,
            fee_estimator,
        }
    }

    /// 8-step detection pipeline for a single market.
    ///
    /// Returns `None` when any gate rejects the opportunity.
    #[allow(clippy::too_many_arguments)]
    pub fn detect(
        &self,
        market_id: &MarketId,
        event_id: &EventId,
        token_yes: &TokenId,
        token_no: &TokenId,
        book: &EndgameBookSnapshot,
        category: MarketCategory,
        staleness: StalenessLevel,
        settlement_deadline: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<Opportunity> {
        // Step 1: enabled + settlement window
        if !self.config.enabled {
            return None;
        }
        let deadline = settlement_deadline?;
        let hours_remaining = (deadline - now).num_hours();
        if hours_remaining < 0
            || hours_remaining
                > i64::try_from(self.config.settlement_window_hours).unwrap_or(i64::MAX)
        {
            self.convergence.remove(market_id);
            return None;
        }

        // Step 2: convergence direction
        let direction = self.detect_direction(book)?;

        // Step 3: convergence duration
        let convergence_secs = self.convergence.update_and_get(market_id, direction, now);
        if convergence_secs < self.config.min_convergence_duration_secs {
            return None;
        }

        // Step 4: walk the target orderbook
        let (target_asks, target_token) = match direction {
            ConvergenceDirection::YesLikely => (&book.yes_asks, token_yes),
            ConvergenceDirection::NoLikely => (&book.no_asks, token_no),
        };

        let walk = OrderbookWalker::walk_asks_by_cost(
            &target_asks.levels,
            self.config.max_investment_usd,
            Some(self.config.high_threshold),
        )?;

        // Step 4b: raw profit per share gate
        let raw_profit_per_share = Decimal::ONE - walk.vwap.inner();
        if raw_profit_per_share < self.config.min_profit_per_share {
            return None;
        }

        // Step 5: calibration lookup
        let bucket_key = BucketKey {
            category,
            price_zone: PriceZone::from_price(walk.vwap),
            duration_bucket: DurationBucket::from_secs(convergence_secs),
        };
        let cal_entry = self.calibrator.lookup(&bucket_key);

        // Step 6: confidence fusion
        let realtime_conf = compute_realtime_confidence(
            walk.vwap.inner(),
            convergence_secs,
            self.config.high_threshold,
        );
        let fused_p = self.fusion.fuse(
            cal_entry.posterior_mean(),
            realtime_conf,
            cal_entry.sample_count(),
        );

        // Step 7: costs and E[PnL]
        let fees = self
            .fee_estimator
            .estimate_fee(walk.shares, walk.vwap, category, target_token);

        let shares_usd = Usd::new(walk.shares.inner());
        let total_cost = walk.total_cost;
        let net_profit = shares_usd - total_cost - fees;

        let expected_payout = shares_usd * fused_p;
        let expected_net_profit = expected_payout - total_cost - fees;

        // Step 8: edge_bps + build
        let cost_plus_fees = total_cost + fees;
        let edge_bps = if cost_plus_fees.is_zero() {
            Bps::ZERO
        } else {
            Bps::new((expected_net_profit.inner() / cost_plus_fees.inner() * dec!(10000)).round())
        };

        let predicted_yes = matches!(direction, ConvergenceDirection::YesLikely);
        let calibration_snapshot = cal_entry.to_snapshot(fused_p);

        Some(Opportunity {
            opportunity_id: OpportunityId::new_v7(),
            market_id: market_id.clone(),
            event_id: event_id.clone(),
            token_id: target_token.clone(),
            side: Side::Buy,
            payout_model: PayoutModel::DirectionalSettlement {
                projected_payout_if_correct: shares_usd,
                expected_payout,
                predicted_side: Side::Buy,
            },
            shares: walk.shares,
            entry_price: walk.vwap,
            total_cost,
            total_fees: fees,
            net_profit,
            expected_net_profit,
            edge_bps,
            resolution_adjust: fused_p,
            depth_used_pct: walk.depth_used_pct,
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
            calibration: calibration_snapshot,
            detected_at: now,
        })
    }

    /// Detect convergence direction from the market book.
    ///
    /// Checks YES asks first (`YesLikely` if best ask >= threshold), then NO
    /// asks (`NoLikely` if NO best ask >= threshold).
    fn detect_direction(&self, book: &EndgameBookSnapshot) -> Option<ConvergenceDirection> {
        if let Some(yes_best_ask) = book.yes_asks.best_price() {
            if yes_best_ask.inner() >= self.config.high_threshold {
                return Some(ConvergenceDirection::YesLikely);
            }
        }

        if let Some(no_best_ask) = book.no_asks.best_price() {
            if no_best_ask.inner() >= self.config.high_threshold {
                return Some(ConvergenceDirection::NoLikely);
            }
        }

        None
    }

    /// Return `true` when per-market state should be cleared.
    ///
    /// The pipeline uses this signal to reset emission cooldown when a market
    /// leaves the active settlement window or no longer has converged prices.
    #[must_use]
    pub fn should_reset_market_state(
        &self,
        book: &EndgameBookSnapshot,
        settlement_deadline: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> bool {
        if !self.config.enabled {
            return true;
        }
        let Some(deadline) = settlement_deadline else {
            return true;
        };
        let hours_remaining = (deadline - now).num_hours();
        if hours_remaining < 0
            || hours_remaining
                > i64::try_from(self.config.settlement_window_hours).unwrap_or(i64::MAX)
        {
            return true;
        }
        self.detect_direction(book).is_none()
    }

    /// Access the underlying convergence tracker (for metrics/diagnostics).
    #[must_use]
    pub const fn convergence_tracker(&self) -> &InMemoryConvergenceTracker {
        &self.convergence
    }

    /// Access the current detection config.
    #[must_use]
    pub const fn config(&self) -> &EndgameDetectionConfig {
        &self.config
    }
}
