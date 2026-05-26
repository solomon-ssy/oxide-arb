//! End-to-end opportunity pipeline: detect → filter → score → cooldown → emit.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use oxide_arb_models::{
    config::ScorerConfig,
    domain::book::EndgameBookPair,
    domain::latency::LatencyTrace,
    enums::common::{MarketCategory, StalenessLevel},
    types::{EventId, MarketId, MicroPct, MicroScore, MicroUsd, TokenId},
};

use crate::cooldown::InMemoryEmissionCooldown;
use crate::endgame::{EndgameDetectInput, EndgameDetector};
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
    pub latency: LatencyTrace,
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
    pub latency: LatencyTrace,
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
            latency: self.latency.clone(),
        }
    }
}

pub struct OpportunityPipeline<F: FeeEstimator> {
    detector: EndgameDetector<F>,
    scorer: EndgameScorer,
    cooldown: InMemoryEmissionCooldown,
    min_profit_threshold_usd: MicroUsd,
    max_depth_usage_pct: MicroPct,
    min_score: MicroScore,
}

impl<F: FeeEstimator> OpportunityPipeline<F> {
    #[must_use]
    pub const fn new(
        detector: EndgameDetector<F>,
        scorer: EndgameScorer,
        cooldown: InMemoryEmissionCooldown,
        min_profit_threshold_usd: MicroUsd,
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

    #[inline]
    pub fn process(
        &self,
        input: &MarketScanInput,
        now: DateTime<Utc>,
    ) -> Option<Arc<ScoredOpportunity>> {
        self.process_ref(&input.as_ref(), now)
    }

    #[inline]
    pub fn process_ref(
        &self,
        input: &MarketScanInputRef<'_>,
        now: DateTime<Utc>,
    ) -> Option<Arc<ScoredOpportunity>> {
        if !self.cooldown.may_emit(input.market_id) {
            return None;
        }

        if !StalenessPolicy::is_tradeable(input.staleness) {
            return None;
        }

        let direction = self.detector.detect_direction(input.book.view());
        if self
            .detector
            .should_reset_market_state(direction, input.settlement_deadline, now)
        {
            self.cooldown.reset(input.market_id);
        }

        let direction = direction?;

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
        let opp = self.detector.detect_with_direction(&detect_input, now)?;

        let profit =
            MicroUsd::try_from_decimal(opp.expected_net_profit.inner()).unwrap_or(MicroUsd::ZERO);
        if profit < self.min_profit_threshold_usd {
            return None;
        }

        let depth_pct =
            MicroPct::try_from_pct_decimal(opp.depth_used_pct).unwrap_or(MicroPct::ZERO);
        if depth_pct > self.max_depth_usage_pct {
            return None;
        }

        let draft = self.scorer.score(&opp, now);
        if draft.score < self.min_score {
            return None;
        }

        self.cooldown.record_emission(input.market_id);
        Some(EndgameScorer::finalize(
            opp,
            draft,
            input.token_yes.clone(),
            input.token_no.clone(),
            input.book.yes.version,
            input.book.no.version,
            input.latency.clone(),
        ))
    }

    pub fn scan_batch(
        &self,
        inputs: &[MarketScanInput],
        now: DateTime<Utc>,
    ) -> Vec<Arc<ScoredOpportunity>> {
        let mut results: Vec<Arc<ScoredOpportunity>> = inputs
            .iter()
            .filter_map(|input| self.process(input, now))
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
