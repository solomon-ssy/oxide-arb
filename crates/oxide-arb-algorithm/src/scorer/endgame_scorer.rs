//! Endgame opportunity scorer — ranks opportunities by risk-adjusted expected `PnL`.

use crate::{
    fill_probability::FillProbabilityEstimator, staleness::StalenessPolicy, urgency::UrgencyFactor,
};
use chrono::{DateTime, Utc};
use oxide_arb_models::{
    config::{FillProbabilityConfig, ScorerConfig},
    domain::{latency::LatencyTrace, opportunity::Opportunity},
    types::{MicroPct, MicroProb, MicroScore, MicroUsd, OpportunityId, TokenId},
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

pub struct EndgameScorer {
    category_weights: [MicroProb; 10],
    min_score: MicroScore,
    max_depth_usage_pct: MicroPct,
    fill_estimator: FillProbabilityEstimator,
    settlement_window_hours: u64,
}

impl EndgameScorer {
    #[must_use]
    pub fn new(
        config: ScorerConfig,
        fill_config: &FillProbabilityConfig,
        settlement_window_hours: u64,
    ) -> Self {
        let mut category_weights = [MicroProb::ONE; 10];
        for (cat, weight) in config.category_weights {
            category_weights[cat.table_index()] = weight;
        }
        Self {
            category_weights,
            min_score: config.min_score,
            max_depth_usage_pct: config.max_depth_usage_pct,
            fill_estimator: FillProbabilityEstimator::new(fill_config),
            settlement_window_hours,
        }
    }

    /// Compute score from an opportunity reference — no heap allocation.
    #[must_use]
    #[inline]
    pub fn score(&self, opp: &Opportunity, now: DateTime<Utc>) -> ScoreDraft {
        let category_weight = self.category_weights[opp.category.table_index()];

        let hours_to_settlement = opp
            .meta
            .settlement_deadline
            .map_or(i64::MAX, |d| (d - now).num_hours());

        let depth_pct =
            MicroPct::try_from_pct_decimal(opp.depth_used_pct).unwrap_or(MicroPct::ZERO);
        let fill_prob = self
            .fill_estimator
            .estimate(depth_pct, opp.staleness, hours_to_settlement);

        let urgency =
            UrgencyFactor::compute(hours_to_settlement.max(0), self.settlement_window_hours);

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

    /// Wrap a scored opportunity for emission (assigns ID, allocates Arc once).
    #[must_use]
    pub fn finalize(
        mut opp: Opportunity,
        draft: ScoreDraft,
        token_yes: TokenId,
        token_no: TokenId,
        book_yes_version: u64,
        book_no_version: u64,
        trace: Arc<LatencyTrace>,
    ) -> Arc<ScoredOpportunity> {
        if opp.opportunity_id.is_pending() {
            opp.opportunity_id = OpportunityId::new_v7();
        }
        let mut trace = Arc::unwrap_or_clone(trace);
        trace.mark_scan_emitted();
        Arc::new(ScoredOpportunity {
            opportunity: Arc::new(opp),
            token_yes,
            token_no,
            book_yes_version,
            book_no_version,
            score: draft.score,
            fill_probability: draft.fill_probability,
            urgency_factor: draft.urgency_factor,
            category_weight: draft.category_weight,
            staleness_discount: draft.staleness_discount,
            trace: Arc::new(trace),
        })
    }

    #[must_use]
    pub const fn min_score(&self) -> MicroScore {
        self.min_score
    }

    #[must_use]
    pub const fn max_depth_usage_pct(&self) -> MicroPct {
        self.max_depth_usage_pct
    }
}
