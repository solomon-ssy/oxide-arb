//! End-to-end opportunity pipeline: detect → filter → score → cooldown → emit.

use chrono::{DateTime, Utc};
use oxide_arb_models::{
    config::ScorerConfig,
    domain::book::EndgameBookPair,
    enums::common::{MarketCategory, StalenessLevel},
    types::{EventId, MarketId, TokenId},
};
use rust_decimal::Decimal;

use crate::cooldown::InMemoryEmissionCooldown;
use crate::endgame::EndgameDetector;
use crate::fee::FeeEstimator;
use crate::scorer::{EndgameScorer, ScoredOpportunity};
use crate::staleness::StalenessPolicy;

pub struct MarketScanInput {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_yes: TokenId,
    pub token_no: TokenId,
    pub book: EndgameBookPair,
    pub category: MarketCategory,
    pub staleness: StalenessLevel,
    pub settlement_deadline: Option<DateTime<Utc>>,
}

pub struct OpportunityPipeline<F: FeeEstimator> {
    detector: EndgameDetector<F>,
    scorer: EndgameScorer,
    cooldown: InMemoryEmissionCooldown,
    min_profit_threshold_usd: Decimal,
    max_depth_usage_pct: Decimal,
    min_score: Decimal,
}

impl<F: FeeEstimator> OpportunityPipeline<F> {
    #[must_use]
    pub const fn new(
        detector: EndgameDetector<F>,
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

    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn process(
        &self,
        market_id: &MarketId,
        event_id: &EventId,
        token_yes: &TokenId,
        token_no: &TokenId,
        book: &EndgameBookPair,
        category: MarketCategory,
        staleness: StalenessLevel,
        settlement_deadline: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<ScoredOpportunity> {
        if !self.cooldown.may_emit(market_id) {
            return None;
        }

        if !StalenessPolicy::is_tradeable(staleness) {
            return None;
        }

        let direction = self.detector.detect_direction(book.view());
        if self
            .detector
            .should_reset_market_state(direction, settlement_deadline, now)
        {
            self.cooldown.reset(market_id);
        }

        let direction = direction?;

        let opp = self.detector.detect_with_direction(
            market_id,
            event_id,
            token_yes,
            token_no,
            book,
            direction,
            category,
            staleness,
            settlement_deadline,
            now,
        )?;

        if opp.expected_net_profit.inner() < self.min_profit_threshold_usd {
            return None;
        }

        if opp.depth_used_pct > self.max_depth_usage_pct {
            return None;
        }

        let draft = self.scorer.score(&opp, now);
        if draft.score < self.min_score {
            return None;
        }

        self.cooldown.record_emission(market_id);
        Some(EndgameScorer::finalize(opp, draft))
    }

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

    #[must_use]
    pub const fn cooldown(&self) -> &InMemoryEmissionCooldown {
        &self.cooldown
    }

    #[must_use]
    pub const fn detector(&self) -> &EndgameDetector<F> {
        &self.detector
    }
}
