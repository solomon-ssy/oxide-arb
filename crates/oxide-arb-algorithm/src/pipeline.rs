//! End-to-end opportunity pipeline: detect → filter → score → cooldown → emit.
//!
//! Called by `oxide-arb-core` on each market data update or periodic scan.

use chrono::{DateTime, Utc};
use oxide_arb_models::{
    config::ScorerConfig,
    domain::book::MarketBookSnapshot,
    enums::common::{MarketCategory, StalenessLevel},
    types::{EventId, MarketId, TokenId},
};
use rust_decimal::Decimal;

use crate::cooldown::InMemoryEmissionCooldown;
use crate::endgame::EndgameDetector;
use crate::scorer::{EndgameScorer, ScoredOpportunity};
use crate::staleness::StalenessPolicy;

/// Input bundle for batch scanning.
pub struct MarketScanInput {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_yes: TokenId,
    pub token_no: TokenId,
    pub book: MarketBookSnapshot,
    pub category: MarketCategory,
    pub staleness: StalenessLevel,
    pub settlement_deadline: Option<DateTime<Utc>>,
}

/// End-to-end opportunity pipeline.
///
/// Owns the detector, scorer, and cooldown — the three stateful components
/// of the algorithm layer. Constructed once by `oxide-arb-core` and reused
/// across all scan ticks.
pub struct OpportunityPipeline {
    detector: EndgameDetector,
    scorer: EndgameScorer,
    // TODO(redis-backend): switch this field to a distributed
    // `RedisEmissionCooldown` before running multiple scanner instances.
    cooldown: InMemoryEmissionCooldown,
    min_profit_threshold_usd: Decimal,
    max_depth_usage_pct: Decimal,
    min_score: Decimal,
}

impl OpportunityPipeline {
    /// Create the pipeline from its constituent parts.
    #[must_use]
    pub const fn new(
        detector: EndgameDetector,
        scorer: EndgameScorer,
        cooldown: InMemoryEmissionCooldown,
        min_profit_threshold_usd: Decimal,
        scorer_config: &ScorerConfig,
    ) -> Self {
        Self {
            detector,
            scorer,
            cooldown,
            min_profit_threshold_usd,
            max_depth_usage_pct: scorer_config.max_depth_usage_pct,
            min_score: scorer_config.min_score,
        }
    }

    /// Process a single market: detect → filter → score → cooldown → emit.
    #[allow(clippy::too_many_arguments)]
    pub fn process(
        &self,
        market_id: &MarketId,
        event_id: &EventId,
        token_yes: &TokenId,
        token_no: &TokenId,
        book: &MarketBookSnapshot,
        category: MarketCategory,
        staleness: StalenessLevel,
        settlement_deadline: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<ScoredOpportunity> {
        // 1. Cooldown check (cheapest gate first)
        if !self.cooldown.may_emit(market_id) {
            return None;
        }

        // 2. Staleness guard
        if !StalenessPolicy::is_tradeable(staleness) {
            return None;
        }

        if self
            .detector
            .should_reset_market_state(book, settlement_deadline, now)
        {
            self.cooldown.reset(market_id);
        }

        // 3. Detect
        let opp = self.detector.detect(
            market_id,
            event_id,
            token_yes,
            token_no,
            book,
            category,
            staleness,
            settlement_deadline,
            now,
        )?;

        // 4. Minimum expected profit
        if opp.expected_net_profit.inner() < self.min_profit_threshold_usd {
            tracing::debug!(
                market_id = %opp.market_id,
                enp = %opp.expected_net_profit,
                "Expected net profit below threshold"
            );
            return None;
        }

        // 5. Depth usage limit
        if opp.depth_used_pct > self.max_depth_usage_pct {
            tracing::debug!(
                market_id = %opp.market_id,
                depth_pct = %opp.depth_used_pct,
                "Depth usage exceeds limit"
            );
            return None;
        }

        // 6. Score
        let scored = self.scorer.score(&opp);

        // 7. Minimum score
        if scored.score < self.min_score {
            tracing::debug!(
                market_id = %opp.market_id,
                score = %scored.score,
                "Score below threshold"
            );
            return None;
        }

        // 8. Record emission + cooldown
        self.cooldown.record_emission(market_id);

        Some(scored)
    }

    /// Batch process multiple markets. Returns results sorted by score descending.
    pub fn scan_batch(
        &self,
        inputs: &[MarketScanInput],
        now: DateTime<Utc>,
    ) -> Vec<ScoredOpportunity> {
        let mut results: Vec<ScoredOpportunity> = inputs
            .iter()
            .filter_map(|input| {
                self.process(
                    &input.market_id,
                    &input.event_id,
                    &input.token_yes,
                    &input.token_no,
                    &input.book,
                    input.category,
                    input.staleness,
                    input.settlement_deadline,
                    now,
                )
            })
            .collect();

        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results
    }

    /// Access the emission cooldown (for metrics).
    #[must_use]
    pub const fn cooldown(&self) -> &InMemoryEmissionCooldown {
        &self.cooldown
    }

    /// Access the detector (for convergence tracker metrics).
    #[must_use]
    pub const fn detector(&self) -> &EndgameDetector {
        &self.detector
    }
}
