//! End-to-end opportunity pipeline: detect → filter → score → cooldown → emit.
//!
//! [`OpportunityPipeline::reload`] is the single hot-reload entry point for
//! the whole detection chain: it swaps the pipeline's own emit gates and
//! propagates the new configuration to the detector, scorer, and emission
//! cooldown atomically with respect to each component.

use crate::{
    cooldown::InMemoryEmissionCooldown,
    detection_reject::{DetectionProcessOutcome, DetectionRejectReason},
    endgame::{EndgameDetectInput, EndgameDetector},
    fee::FeeEstimator,
    scorer::{EmitContext, EndgameScorer, ScoredOpportunity},
    staleness::StalenessPolicy,
};
use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use oxide_arb_models::{
    domain::{
        book::{BookSnapshot, EndgameBookPair},
        control_factor::{
            AppliedControlFactor, BucketRiskDimensions, ControlFactorProvider,
            ControlFactorSnapshot, bucket_resolution_trace, effective_fill_probability,
            effective_resolution_prob, execution_quality_dimensions, execution_quality_fill_trace,
            expected_net_profit,
        },
        latency::LatencyTrace,
        opportunity::Opportunity,
    },
    enums::common::{MarketCategory, StalenessLevel},
    runtime_config::DetectionConfig,
    types::{EventId, MarketId, MicroPct, MicroProb, MicroScore, MicroUsd, TokenId, Usd},
};
use std::sync::Arc;

pub struct MarketScanInput {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_yes: TokenId,
    pub token_no: TokenId,
    pub book: EndgameBookPair,
    pub category: MarketCategory,
    pub staleness: StalenessLevel,
    pub settlement_deadline: Option<DateTime<Utc>>,
    pub latency: Arc<LatencyTrace>,
}

/// Borrowed scan input — avoids cloning `Arc<str>` IDs on the hot path.
pub struct MarketScanInputRef<'a> {
    pub market_id: &'a MarketId,
    pub event_id: &'a EventId,
    pub token_yes: &'a TokenId,
    pub token_no: &'a TokenId,
    pub book: &'a EndgameBookPair,
    pub category: MarketCategory,
    pub staleness: StalenessLevel,
    pub settlement_deadline: Option<DateTime<Utc>>,
    pub latency: Arc<LatencyTrace>,
}

impl MarketScanInput {
    #[inline]
    pub fn as_ref(&self) -> MarketScanInputRef<'_> {
        MarketScanInputRef {
            market_id: &self.market_id,
            event_id: &self.event_id,
            token_yes: &self.token_yes,
            token_no: &self.token_no,
            book: &self.book,
            category: self.category,
            staleness: self.staleness,
            settlement_deadline: self.settlement_deadline,
            latency: Arc::clone(&self.latency),
        }
    }
}

/// Hot-swappable emit-gate parameters.
struct PipelineParams {
    min_profit_threshold_usd: MicroUsd,
    max_depth_usage_pct: MicroPct,
    min_score: MicroScore,
}

impl PipelineParams {
    fn from_config(config: &DetectionConfig) -> Self {
        Self {
            min_profit_threshold_usd: MicroUsd::try_from_decimal(config.min_profit_threshold_usd)
                .unwrap_or(MicroUsd::ZERO),
            max_depth_usage_pct: MicroPct::try_from_pct_decimal(
                config.endgame.scorer.max_depth_usage_pct,
            )
            .unwrap_or(MicroPct::ZERO),
            min_score: MicroScore::try_from_decimal(config.endgame.scorer.min_score)
                .unwrap_or(MicroScore::ZERO),
        }
    }
}

pub struct OpportunityPipeline<F: FeeEstimator> {
    detector: EndgameDetector<F>,
    scorer: EndgameScorer,
    cooldown: InMemoryEmissionCooldown,
    factors: Arc<dyn ControlFactorProvider>,
    params: ArcSwap<PipelineParams>,
}

impl<F: FeeEstimator> OpportunityPipeline<F> {
    #[must_use]
    pub fn new(
        detector: EndgameDetector<F>,
        scorer: EndgameScorer,
        cooldown: InMemoryEmissionCooldown,
        factors: Arc<dyn ControlFactorProvider>,
        config: &DetectionConfig,
    ) -> Self {
        Self {
            detector,
            scorer,
            cooldown,
            factors,
            params: ArcSwap::from_pointee(PipelineParams::from_config(config)),
        }
    }

    /// Hot-reload the full detection chain (runtime-config activation).
    ///
    /// Order: detector (thresholds + fusion) → scorer (weights + fill model) →
    /// emission cooldown → own emit gates. Each component swap is atomic; the
    /// chain converges to the new configuration within one scan iteration.
    pub fn reload(&self, config: &DetectionConfig) {
        self.detector.reload(&config.endgame, &config.calibration);
        self.scorer.reload(
            &config.endgame.scorer,
            &config.endgame.fill_probability,
            config.endgame.settlement_window_hours,
        );
        self.cooldown.reload(&config.endgame.emission_cooldown);
        self.params
            .store(Arc::new(PipelineParams::from_config(config)));
    }

    #[inline]
    pub fn process_ref(
        &self,
        input: &MarketScanInputRef<'_>,
        now: DateTime<Utc>,
    ) -> DetectionProcessOutcome {
        let params = self.params.load();
        let snapshot = self.factors.snapshot();

        // Market-anomaly pre-gate: block/cooldown before any detection work.
        if snapshot
            .market_anomalies
            .market_block(input.market_id)
            .is_some()
            || snapshot
                .market_anomalies
                .event_block(input.event_id)
                .is_some()
            || snapshot
                .market_anomalies
                .category_cooldown_secs(input.category)
                .is_some()
        {
            return DetectionProcessOutcome::rejected(DetectionRejectReason::MarketAnomaly);
        }

        if !self.cooldown.may_emit(input.market_id) {
            return DetectionProcessOutcome::rejected(DetectionRejectReason::EmissionCooldown);
        }

        if !StalenessPolicy::is_tradeable(input.staleness) {
            return DetectionProcessOutcome::rejected(DetectionRejectReason::StalenessExpired);
        }

        let direction = self.detector.detect_direction(input.book.view());
        if self
            .detector
            .should_reset_market_state(direction, input.settlement_deadline, now)
        {
            self.cooldown.reset(input.market_id);
        }

        let Some(direction) = direction else {
            return DetectionProcessOutcome::rejected(
                DetectionRejectReason::NoConvergenceDirection,
            );
        };

        let detect_input = EndgameDetectInput {
            market_id: input.market_id,
            event_id: input.event_id,
            token_yes: input.token_yes,
            token_no: input.token_no,
            book: input.book,
            direction,
            category: input.category,
            staleness: input.staleness,
            settlement_deadline: input.settlement_deadline,
        };
        let mut opp = match self
            .detector
            .detect_with_direction(&detect_input, now)
        {
            Ok(opp) => opp,
            Err(reason) => return DetectionProcessOutcome::rejected(reason),
        };

        let mut applied: Vec<AppliedControlFactor> = Vec::new();

        // BucketRisk: recompute expected net profit under a haircut resolution
        // probability and re-gate on the tightened economics.
        if let Some(reason) = Self::apply_bucket_risk(&snapshot, &mut opp, &mut applied) {
            return DetectionProcessOutcome::rejected(reason);
        }

        let profit =
            MicroUsd::try_from_decimal(opp.expected_net_profit.inner()).unwrap_or(MicroUsd::ZERO);
        if profit < params.min_profit_threshold_usd {
            return DetectionProcessOutcome::rejected(DetectionRejectReason::MinProfitThreshold);
        }

        let depth_pct =
            MicroPct::try_from_pct_decimal(opp.depth_used_pct).unwrap_or(MicroPct::ZERO);
        if depth_pct > params.max_depth_usage_pct {
            return DetectionProcessOutcome::rejected(DetectionRejectReason::MaxDepthUsage);
        }

        // ExecutionQuality: discount the base fill probability before scoring so
        // the effect propagates into the score and downstream risk sizing.
        let fill_override =
            self.apply_execution_quality(&snapshot, &opp, input.book, now, &mut applied);

        let draft = self.scorer.score(&opp, now, fill_override);
        if draft.score < params.min_score {
            return DetectionProcessOutcome::rejected(DetectionRejectReason::MinScore);
        }

        self.cooldown.record_emission(input.market_id);
        DetectionProcessOutcome {
            opportunity: Some(EndgameScorer::finalize(
                opp,
                draft,
                EmitContext {
                    token_yes: input.token_yes.clone(),
                    token_no: input.token_no.clone(),
                    book_yes_version: input.book.yes.version,
                    book_no_version: input.book.no.version,
                    applied_factors: Arc::from(applied),
                    trace: input.latency.clone(),
                },
            )),
            reject: None,
        }
    }

    /// Apply the matching bucket-risk factor to `opp` in place. Returns `Some`
    /// when the factor blocks new entries (caller must skip the opportunity).
    fn apply_bucket_risk(
        snapshot: &ControlFactorSnapshot,
        opp: &mut Opportunity,
        applied: &mut Vec<AppliedControlFactor>,
    ) -> Option<DetectionRejectReason> {
        let publication_id = snapshot.publication_id.clone()?;
        let dims = BucketRiskDimensions::coarse(
            opp.category,
            opp.meta.price_zone,
            opp.meta.duration_bucket,
        );
        let found = snapshot.bucket_risk.lookup(&dims)?;
        let payload = &found.payload;
        if payload.block_new_entries {
            return Some(DetectionRejectReason::BucketRiskBlocked);
        }
        // The bucket may demand an absolute minimum edge floor (bps).
        if opp.edge_bps.inner() < payload.min_edge_bps_addon {
            return Some(DetectionRejectReason::BucketRiskEdgeFloor);
        }
        let base_prob = opp.resolution_adjust;
        let effective_prob =
            effective_resolution_prob(base_prob, payload.resolution_haircut_factor);
        // Raw payout assuming the predicted outcome wins.
        let payout_if_correct =
            Usd::new(opp.net_profit.inner() + opp.total_cost.inner() + opp.total_fees.inner());
        opp.expected_net_profit = expected_net_profit(
            payout_if_correct,
            opp.total_cost,
            opp.total_fees,
            effective_prob,
        );
        opp.resolution_adjust = effective_prob;
        applied.push(bucket_resolution_trace(
            found.factor_id.clone(),
            publication_id,
            base_prob,
            effective_prob,
        ));
        None
    }

    /// Resolve the execution-quality fill multiplier for the traded token and
    /// return the factor-adjusted fill probability override (if any matched).
    fn apply_execution_quality(
        &self,
        snapshot: &ControlFactorSnapshot,
        opp: &Opportunity,
        book: &EndgameBookPair,
        now: DateTime<Utc>,
        applied: &mut Vec<AppliedControlFactor>,
    ) -> Option<MicroProb> {
        let publication_id = snapshot.publication_id.clone()?;
        let traded_book: &BookSnapshot = if opp.meta.predicted_yes {
            &book.yes
        } else {
            &book.no
        };
        let now_ms = u64::try_from(now.timestamp_millis()).unwrap_or(0);
        let dims = execution_quality_dimensions(
            opp.category,
            opp.meta.price_zone,
            opp.staleness,
            traded_book,
            now_ms,
        );
        let found = snapshot.execution_quality.lookup(&dims)?;
        let base_fill = self.scorer.estimate_fill(opp, now);
        let effective = effective_fill_probability(
            base_fill.to_decimal(),
            found.payload.fill_probability_multiplier,
        );
        let effective_fill = MicroProb::try_from_decimal(effective).unwrap_or(base_fill);
        applied.push(execution_quality_fill_trace(
            found.factor_id.clone(),
            publication_id,
            base_fill.to_decimal(),
            effective_fill.to_decimal(),
        ));
        Some(effective_fill)
    }

    pub fn scan_batch(
        &self,
        inputs: &[MarketScanInput],
        now: DateTime<Utc>,
    ) -> Vec<Arc<ScoredOpportunity>> {
        let mut results: Vec<Arc<ScoredOpportunity>> = inputs
            .iter()
            .filter_map(|input| self.process_ref(&input.as_ref(), now).opportunity)
            .collect();

        results.sort_by(|a, b| a.score.cmp_desc(b.score));
        results
    }

    #[must_use]
    pub const fn cooldown(&self) -> &InMemoryEmissionCooldown {
        &self.cooldown
    }

    #[must_use]
    pub const fn detector(&self) -> &EndgameDetector<F> {
        &self.detector
    }
}
