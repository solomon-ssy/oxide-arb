//! Keeps CLOB websocket subscriptions aligned with the trading hotset.
//!
//! The full Gamma catalog is too large to mirror blindly into Polymarket WS
//! connections. The engine baseline is therefore selected by
//! [`MarketDataSubscriptionPolicy`] and registered under
//! [`SubscriptionSource::Engine`]; the web plane remains an overlay and can
//! never unsubscribe an engine-owned token.

use crate::{
    observability::metrics_hub::MetricsHub,
    pipeline::{market_registry::MarketRegistry, universe_filter::MarketUniverseFilter},
};
use chrono::{DateTime, Duration, Utc};
use oxide_arb_api::ws::{ClobWsManager, SubscriptionSource};
use oxide_arb_models::{
    domain::market::MarketRegistryInfo, enums::market::MarketStatus, types::TokenId,
};
use std::sync::Arc;

/// Fraction of the token budget reserved for the detection-window tier (Tier1).
const TIER1_TOKEN_BUDGET_PCT: usize = 80;

/// Candidate bucket used for hotset metrics and tiered selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HotsetCandidateKind {
    PastDeadline,
    FutureDetect,
    FuturePrewarm,
    NoEndDate,
}

impl HotsetCandidateKind {
    const fn metric_label(self) -> &'static str {
        match self {
            Self::PastDeadline => "past_deadline",
            Self::FutureDetect => "future_detect",
            Self::FuturePrewarm => "future_prewarm",
            Self::NoEndDate => "no_end_date",
        }
    }
}

/// Outcome of a single hotset reconciliation pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HotsetSelectionStats {
    pub candidates_past_deadline: u64,
    pub candidates_future_detect: u64,
    pub candidates_future_prewarm: u64,
    pub candidates_no_end_date: u64,
    pub selected_future_detect: u64,
    pub selected_future_prewarm: u64,
    pub selected_tokens: u64,
}

impl HotsetSelectionStats {
    const fn record_candidate(&mut self, kind: HotsetCandidateKind) {
        match kind {
            HotsetCandidateKind::PastDeadline => self.candidates_past_deadline += 1,
            HotsetCandidateKind::FutureDetect => self.candidates_future_detect += 1,
            HotsetCandidateKind::FuturePrewarm => self.candidates_future_prewarm += 1,
            HotsetCandidateKind::NoEndDate => self.candidates_no_end_date += 1,
        }
    }

    const fn record_selected(&mut self, kind: HotsetCandidateKind, token_pairs: u64) {
        match kind {
            HotsetCandidateKind::FutureDetect => self.selected_future_detect += token_pairs,
            HotsetCandidateKind::FuturePrewarm => self.selected_future_prewarm += token_pairs,
            HotsetCandidateKind::PastDeadline | HotsetCandidateKind::NoEndDate => {}
        }
        self.selected_tokens += token_pairs.saturating_mul(2);
    }

    /// Ratio of selected detection-window markets to eligible detection-window candidates.
    #[must_use]
    pub fn detection_window_coverage_ratio(self) -> f64 {
        if self.candidates_future_detect == 0 {
            return 0.0;
        }
        let selected_markets = self
            .selected_future_detect
            .min(self.candidates_future_detect);
        f64::from(u32::try_from(selected_markets).unwrap_or(u32::MAX))
            / f64::from(u32::try_from(self.candidates_future_detect).unwrap_or(u32::MAX))
    }

    /// Markets selected into Tier1 (detection-window bucket).
    #[must_use]
    pub const fn tier1_markets(self) -> u64 {
        self.selected_future_detect
    }

    /// CLOB tokens subscribed for Tier1 markets (two per binary market).
    #[must_use]
    pub const fn tier1_tokens(self) -> u64 {
        self.selected_future_detect.saturating_mul(2)
    }

    /// Markets selected into Tier2 (prewarm bucket).
    #[must_use]
    pub const fn tier2_markets(self) -> u64 {
        self.selected_future_prewarm
    }

    /// CLOB tokens subscribed for Tier2 markets (two per binary market).
    #[must_use]
    pub const fn tier2_tokens(self) -> u64 {
        self.selected_future_prewarm.saturating_mul(2)
    }

    pub(crate) fn publish(&self, metrics: &MetricsHub) {
        metrics
            .hotset_candidates_total
            .with_label_values(&[HotsetCandidateKind::PastDeadline.metric_label()])
            .inc_by(self.candidates_past_deadline);
        metrics
            .hotset_candidates_total
            .with_label_values(&[HotsetCandidateKind::FutureDetect.metric_label()])
            .inc_by(self.candidates_future_detect);
        metrics
            .hotset_candidates_total
            .with_label_values(&[HotsetCandidateKind::FuturePrewarm.metric_label()])
            .inc_by(self.candidates_future_prewarm);
        metrics
            .hotset_candidates_total
            .with_label_values(&[HotsetCandidateKind::NoEndDate.metric_label()])
            .inc_by(self.candidates_no_end_date);
        metrics
            .hotset_selected_total
            .with_label_values(&[HotsetCandidateKind::FutureDetect.metric_label()])
            .inc_by(self.selected_future_detect);
        metrics
            .hotset_selected_total
            .with_label_values(&[HotsetCandidateKind::FuturePrewarm.metric_label()])
            .inc_by(self.selected_future_prewarm);
        metrics
            .hotset_detection_window_coverage_ratio
            .set(self.detection_window_coverage_ratio());
    }
}

/// Bounded selector for the trading engine's market-data hotset.
#[derive(Debug, Clone, Copy)]
pub struct MarketDataSubscriptionPolicy {
    max_subscription_tokens: usize,
    endgame_window_hours: u64,
}

pub struct WsSubscriptionCoordinator {
    ws_manager: Arc<ClobWsManager>,
    policy: MarketDataSubscriptionPolicy,
}

struct Candidate {
    market: Arc<MarketRegistryInfo>,
    rank_ms: i64,
}

impl WsSubscriptionCoordinator {
    pub const fn new(ws_manager: Arc<ClobWsManager>, policy: MarketDataSubscriptionPolicy) -> Self {
        Self { ws_manager, policy }
    }

    /// Reconcile the engine baseline to the policy-selected trading hotset.
    pub fn sync_engine_hotset(
        &self,
        registry: &MarketRegistry,
        universe: &MarketUniverseFilter,
        detection_window_hours: u64,
        metrics: &MetricsHub,
    ) -> HotsetSelectionStats {
        let (desired, stats) =
            self.policy
                .select_tokens_with_stats(registry, universe, detection_window_hours);
        self.ws_manager
            .sync_tokens(SubscriptionSource::Engine, &desired);
        stats.publish(metrics);
        stats
    }

    #[must_use]
    pub fn subscribed_count(&self) -> usize {
        self.ws_manager
            .source_subscription_count(SubscriptionSource::Engine)
    }
}

impl MarketDataSubscriptionPolicy {
    #[must_use]
    pub const fn new(max_subscription_tokens: usize, endgame_window_hours: u64) -> Self {
        Self {
            max_subscription_tokens,
            endgame_window_hours,
        }
    }

    /// Return a stable, bounded token list for the engine WS baseline.
    #[must_use]
    pub fn select_tokens(
        &self,
        registry: &MarketRegistry,
        universe: &MarketUniverseFilter,
        detection_window_hours: u64,
    ) -> Vec<TokenId> {
        self.select_tokens_with_stats(registry, universe, detection_window_hours)
            .0
    }

    /// Select tokens and return hotset statistics for metrics/logging.
    #[must_use]
    pub fn select_tokens_with_stats(
        &self,
        registry: &MarketRegistry,
        universe: &MarketUniverseFilter,
        detection_window_hours: u64,
    ) -> (Vec<TokenId>, HotsetSelectionStats) {
        let now = Utc::now();
        let (tier1, tier2, mut stats) =
            self.partition_candidates(registry, universe, now, detection_window_hours);

        let tier1_budget = self
            .max_subscription_tokens
            .saturating_mul(TIER1_TOKEN_BUDGET_PCT)
            / 100;
        let mut tokens = Vec::with_capacity(self.max_subscription_tokens);

        Self::append_tier(
            &mut tokens,
            tier1,
            tier1_budget,
            HotsetCandidateKind::FutureDetect,
            &mut stats,
        );
        let remaining = self.max_subscription_tokens.saturating_sub(tokens.len());
        Self::append_tier(
            &mut tokens,
            tier2,
            remaining,
            HotsetCandidateKind::FuturePrewarm,
            &mut stats,
        );

        tokens.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        tokens.dedup_by(|a, b| a.as_str() == b.as_str());
        (tokens, stats)
    }

    fn append_tier(
        tokens: &mut Vec<TokenId>,
        mut candidates: Vec<Candidate>,
        token_budget: usize,
        kind: HotsetCandidateKind,
        stats: &mut HotsetSelectionStats,
    ) {
        if token_budget == 0 {
            return;
        }
        candidates.sort_by(|left, right| {
            left.rank_ms.cmp(&right.rank_ms).then(
                left.market
                    .market_id
                    .as_str()
                    .cmp(right.market.market_id.as_str()),
            )
        });

        for candidate in candidates {
            if tokens.len().saturating_add(2) > token_budget {
                break;
            }
            tokens.push(candidate.market.token_yes.clone());
            tokens.push(candidate.market.token_no.clone());
            stats.record_selected(kind, 1);
        }
    }

    fn partition_candidates(
        &self,
        registry: &MarketRegistry,
        universe: &MarketUniverseFilter,
        now: DateTime<Utc>,
        detection_window_hours: u64,
    ) -> (Vec<Candidate>, Vec<Candidate>, HotsetSelectionStats) {
        let mut stats = HotsetSelectionStats::default();
        let mut tier1 = Vec::new();
        let mut tier2 = Vec::new();
        let hours = i64::try_from(self.endgame_window_hours).unwrap_or(i64::MAX);
        let prewarm_cutoff = now + Duration::hours(hours);
        let detect_cutoff =
            now + Duration::hours(i64::try_from(detection_window_hours).unwrap_or(i64::MAX));

        for market_id in registry.active_markets().iter() {
            let Some(market) = registry.get_market(market_id) else {
                continue;
            };
            if market.status != MarketStatus::Active || !universe.is_enabled(market.categories) {
                continue;
            }

            let Some(end_date) = market.end_date else {
                stats.record_candidate(HotsetCandidateKind::NoEndDate);
                continue;
            };

            if end_date <= now {
                stats.record_candidate(HotsetCandidateKind::PastDeadline);
                continue;
            }
            if end_date > prewarm_cutoff {
                continue;
            }

            let rank_ms = end_date.signed_duration_since(now).num_milliseconds();
            if end_date <= detect_cutoff {
                stats.record_candidate(HotsetCandidateKind::FutureDetect);
                tier1.push(Candidate { market, rank_ms });
            } else {
                stats.record_candidate(HotsetCandidateKind::FuturePrewarm);
                tier2.push(Candidate { market, rank_ms });
            }
        }

        (tier1, tier2, stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::market_registry::MarketRegistry;
    use chrono::Duration;
    use oxide_arb_models::{
        domain::market::{MarketRegistryInfo, TokenInfo},
        enums::common::MarketCategory,
        types::{EventId, MarketId, Usd},
    };
    use rust_decimal_macros::dec;

    fn sample_market(id: &str, end_date: Option<DateTime<Utc>>) -> MarketRegistryInfo {
        MarketRegistryInfo {
            market_id: MarketId::new(id),
            event_id: EventId::new("evt-1"),
            token_yes: TokenId::new(format!("{id}-yes")),
            token_no: TokenId::new(format!("{id}-no")),
            question: "Q?".into(),
            slug: "q".into(),
            categories: MarketCategory::Politics.into(),
            status: MarketStatus::Active,
            outcome: None,
            neg_risk: false,
            tick_size: oxide_arb_models::enums::common::TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: TokenId::new(format!("{id}-yes")),
                    outcome: "Yes".into(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: TokenId::new(format!("{id}-no")),
                    outcome: "No".into(),
                    neg_risk: false,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: dec!(5),
            volume_24h: Usd::ZERO,
            fee_schedule: None,
            end_date,
            resolved_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn excludes_past_deadline_and_selects_nearest_future_first() {
        let now = Utc::now();
        let reg = MarketRegistry::new();
        reg.register_market(sample_market("past", Some(now - Duration::hours(1))));
        reg.register_market(sample_market("soon", Some(now + Duration::hours(2))));
        reg.register_market(sample_market("later", Some(now + Duration::hours(10))));

        let policy = MarketDataSubscriptionPolicy::new(2000, 72);
        let universe = MarketUniverseFilter::default();
        let (tokens, stats) = policy.select_tokens_with_stats(&reg, &universe, 24);

        assert_eq!(stats.candidates_past_deadline, 1);
        assert_eq!(stats.candidates_future_detect, 2);
        assert!(tokens.contains(&TokenId::new("soon-yes")));
        assert!(tokens.contains(&TokenId::new("later-yes")));
        assert!(!tokens.iter().any(|t| t.as_str().contains("past")));
    }

    #[test]
    fn no_end_date_markets_are_not_subscribed() {
        let reg = MarketRegistry::new();
        reg.register_market(sample_market("no-date", None));

        let policy = MarketDataSubscriptionPolicy::new(2000, 72);
        let (tokens, stats) =
            policy.select_tokens_with_stats(&reg, &MarketUniverseFilter::default(), 24);

        assert!(tokens.is_empty());
        assert_eq!(stats.candidates_no_end_date, 1);
    }

    #[test]
    fn token_cap_truncates_to_nearest_deadlines() {
        let now = Utc::now();
        let reg = MarketRegistry::new();
        for hour in [1_i64, 2, 3, 4, 5] {
            reg.register_market(sample_market(
                &format!("m{hour}"),
                Some(now + Duration::hours(hour)),
            ));
        }

        let policy = MarketDataSubscriptionPolicy::new(5, 72);
        let (tokens, _) =
            policy.select_tokens_with_stats(&reg, &MarketUniverseFilter::default(), 24);

        // Tier1 budget is 80% of 5 → 4 tokens → two nearest markets.
        assert_eq!(tokens.len(), 4);
        assert!(tokens.iter().any(|t| t.as_str() == "m1-yes"));
        assert!(tokens.iter().any(|t| t.as_str() == "m2-yes"));
        assert!(!tokens.iter().any(|t| t.as_str().contains("m5")));
    }
}
