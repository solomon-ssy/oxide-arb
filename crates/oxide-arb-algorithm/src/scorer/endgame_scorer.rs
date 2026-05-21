//! Endgame opportunity scorer — ranks opportunities by risk-adjusted expected `PnL`.
//!
//! `score = expected_net_profit × fill_probability × urgency × category_weight`

use oxide_arb_models::{
    config::{FillProbabilityConfig, ScorerConfig},
    domain::opportunity::Opportunity,
};
use rust_decimal::Decimal;

use crate::fill_probability::FillProbabilityEstimator;
use crate::urgency::UrgencyFactor;

/// A scored opportunity ready for ranking and emission.
#[derive(Debug, Clone)]
pub struct ScoredOpportunity {
    pub opportunity: Opportunity,
    /// Composite score used for ranking (higher = better).
    pub score: Decimal,
    /// Estimated probability that the FOK order fills at the target price.
    pub fill_probability: Decimal,
    /// Time-to-settlement urgency multiplier applied.
    pub urgency_factor: Decimal,
    /// Category-based weight multiplier applied.
    pub category_weight: Decimal,
}

/// Endgame opportunity scorer.
pub struct EndgameScorer {
    config: ScorerConfig,
    fill_estimator: FillProbabilityEstimator,
    settlement_window_hours: u64,
}

impl EndgameScorer {
    /// Create a new scorer.
    #[must_use]
    pub fn new(
        config: ScorerConfig,
        fill_config: &FillProbabilityConfig,
        settlement_window_hours: u64,
    ) -> Self {
        Self {
            config,
            fill_estimator: FillProbabilityEstimator::new(fill_config),
            settlement_window_hours,
        }
    }

    /// Score an opportunity.
    ///
    /// `score = expected_net_profit × fill_probability × urgency × category_weight`
    #[must_use]
    pub fn score(&self, opp: &Opportunity) -> ScoredOpportunity {
        let category_weight = self
            .config
            .category_weights
            .get(&opp.category)
            .copied()
            .unwrap_or(Decimal::ONE);

        let hours_to_settlement = opp
            .meta
            .settlement_deadline
            .map_or(i64::MAX, |d| (d - chrono::Utc::now()).num_hours());

        let fill_prob =
            self.fill_estimator
                .estimate(opp.depth_used_pct, opp.staleness, hours_to_settlement);

        let urgency = UrgencyFactor::compute(
            Decimal::from(hours_to_settlement.max(0)),
            Decimal::from(self.settlement_window_hours),
        );

        let score = opp.expected_net_profit.inner() * fill_prob * urgency * category_weight;

        ScoredOpportunity {
            opportunity: opp.clone(),
            score,
            fill_probability: fill_prob,
            urgency_factor: urgency,
            category_weight,
        }
    }

    /// Access the scorer configuration.
    #[must_use]
    pub const fn config(&self) -> &ScorerConfig {
        &self.config
    }
}
