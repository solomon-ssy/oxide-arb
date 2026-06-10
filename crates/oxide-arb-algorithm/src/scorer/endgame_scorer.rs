//! Endgame opportunity scorer — ranks opportunities by risk-adjusted expected `PnL`.
//!
//! Scoring parameters are hot-reloadable through [`EndgameScorer::reload`]
//! (lock-free `ArcSwap` parameter snapshot). Config values arrive as decimals
//! (the runtime-config wire format) and are converted to fixed-point `Micro*`
//! once per reload, never on the hot path.

use crate::{
    fill_probability::FillProbabilityEstimator, staleness::StalenessPolicy, urgency::UrgencyFactor,
};
use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use oxide_arb_models::{
    domain::{
        control_factor::AppliedControlFactor, latency::LatencyTrace, opportunity::Opportunity,
    },
    runtime_config::{FillProbabilityConfig, ScorerConfig},
    types::{MicroPct, MicroProb, MicroScore, MicroUsd, TokenId},
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ScoredOpportunity {
    pub opportunity: Arc<Opportunity>,
    pub token_yes: TokenId,
    pub token_no: TokenId,
    pub book_yes_version: u64,
    pub book_no_version: u64,
    pub score: MicroScore,
    pub fill_probability: MicroProb,
    pub urgency_factor: MicroProb,
    pub category_weight: MicroProb,
    pub staleness_discount: MicroProb,
    /// Auditable control factors applied at detection/scoring time (bucket
    /// resolution haircut, execution-quality fill discount). Empty when no
    /// publication is active or no factor matched.
    pub applied_factors: Arc<[AppliedControlFactor]>,
    pub trace: Arc<LatencyTrace>,
}

/// Score components computed before final emit gates.
#[derive(Debug, Clone, Copy)]
pub struct ScoreDraft {
    pub score: MicroScore,
    pub fill_probability: MicroProb,
    pub urgency_factor: MicroProb,
    pub category_weight: MicroProb,
    pub staleness_discount: MicroProb,
}

/// Emit-time context bundled to keep [`EndgameScorer::finalize`] cohesive.
pub struct EmitContext {
    pub token_yes: TokenId,
    pub token_no: TokenId,
    pub book_yes_version: u64,
    pub book_no_version: u64,
    pub applied_factors: Arc<[AppliedControlFactor]>,
    pub trace: Arc<LatencyTrace>,
}

/// Hot-swappable scoring parameter snapshot.
struct ScorerParams {
    category_weights: [MicroProb; 10],
    fill_estimator: FillProbabilityEstimator,
    settlement_window_hours: u64,
}

impl ScorerParams {
    fn from_config(
        config: &ScorerConfig,
        fill_config: &FillProbabilityConfig,
        settlement_window_hours: u64,
    ) -> Self {
        let mut category_weights = [MicroProb::ONE; 10];
        for (cat, weight) in &config.category_weights {
            category_weights[cat.table_index()] = MicroProb::try_from_decimal(*weight)
                .map_or(MicroProb::ONE, |w| MicroProb::from_factor_micro(w.micro()));
        }
        Self {
            category_weights,
            fill_estimator: FillProbabilityEstimator::new(fill_config),
            settlement_window_hours,
        }
    }
}

pub struct EndgameScorer {
    params: ArcSwap<ScorerParams>,
}

impl EndgameScorer {
    #[must_use]
    pub fn new(
        config: &ScorerConfig,
        fill_config: &FillProbabilityConfig,
        settlement_window_hours: u64,
    ) -> Self {
        Self {
            params: ArcSwap::from_pointee(ScorerParams::from_config(
                config,
                fill_config,
                settlement_window_hours,
            )),
        }
    }

    /// Hot-reload scoring parameters (runtime-config activation).
    pub fn reload(
        &self,
        config: &ScorerConfig,
        fill_config: &FillProbabilityConfig,
        settlement_window_hours: u64,
    ) {
        self.params.store(Arc::new(ScorerParams::from_config(
            config,
            fill_config,
            settlement_window_hours,
        )));
    }

    /// Baseline fill-probability estimate for an opportunity (pre-factor).
    ///
    /// Exposed so the pipeline can apply an `ExecutionQualityFactor` multiplier
    /// to the base fill probability *before* the score is computed, keeping the
    /// factor effect visible to risk sizing downstream.
    #[must_use]
    #[inline]
    pub fn estimate_fill(&self, opp: &Opportunity, now: DateTime<Utc>) -> MicroProb {
        let hours_to_settlement = opp
            .meta
            .settlement_deadline
            .map_or(i64::MAX, |d| (d - now).num_hours());
        let depth_pct =
            MicroPct::try_from_pct_decimal(opp.depth_used_pct).unwrap_or(MicroPct::ZERO);
        self.params
            .load()
            .fill_estimator
            .estimate(depth_pct, opp.staleness, hours_to_settlement)
    }

    /// Compute score from an opportunity reference — no heap allocation.
    ///
    /// `fill_override` supplies the execution-quality-adjusted fill probability;
    /// when `None` the baseline estimate is used.
    #[must_use]
    #[inline]
    pub fn score(
        &self,
        opp: &Opportunity,
        now: DateTime<Utc>,
        fill_override: Option<MicroProb>,
    ) -> ScoreDraft {
        let params = self.params.load();
        let category_weight = params.category_weights[opp.category.table_index()];

        let hours_to_settlement = opp
            .meta
            .settlement_deadline
            .map_or(i64::MAX, |d| (d - now).num_hours());

        let fill_prob = fill_override.unwrap_or_else(|| {
            let depth_pct =
                MicroPct::try_from_pct_decimal(opp.depth_used_pct).unwrap_or(MicroPct::ZERO);
            params
                .fill_estimator
                .estimate(depth_pct, opp.staleness, hours_to_settlement)
        });

        let urgency =
            UrgencyFactor::compute(hours_to_settlement.max(0), params.settlement_window_hours);

        let staleness_discount = StalenessPolicy::confidence_discount(opp.staleness);

        let profit =
            MicroUsd::try_from_decimal(opp.expected_net_profit.inner()).unwrap_or(MicroUsd::ZERO);
        let score = MicroScore::from_profit_prob(profit, fill_prob)
            .scale_by_factor(urgency)
            .scale_by_factor(category_weight)
            .scale_by_factor(staleness_discount);

        ScoreDraft {
            score,
            fill_probability: fill_prob,
            urgency_factor: urgency,
            category_weight,
            staleness_discount,
        }
    }

    /// Wrap a scored opportunity for emission (allocates the `Arc` once).
    ///
    /// The opportunity already carries its identity (assigned at detection), so
    /// finalization only attaches scoring outputs and emission context.
    #[must_use]
    pub fn finalize(
        opp: Opportunity,
        draft: ScoreDraft,
        ctx: EmitContext,
    ) -> Arc<ScoredOpportunity> {
        let mut trace = Arc::unwrap_or_clone(ctx.trace);
        trace.mark_scan_emitted();
        Arc::new(ScoredOpportunity {
            opportunity: Arc::new(opp),
            token_yes: ctx.token_yes,
            token_no: ctx.token_no,
            book_yes_version: ctx.book_yes_version,
            book_no_version: ctx.book_no_version,
            score: draft.score,
            fill_probability: draft.fill_probability,
            urgency_factor: draft.urgency_factor,
            category_weight: draft.category_weight,
            staleness_discount: draft.staleness_discount,
            applied_factors: ctx.applied_factors,
            trace: Arc::new(trace),
        })
    }
}
