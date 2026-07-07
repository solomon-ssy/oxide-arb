//! [`ConfiguredMarketSelector`]: the pure selection function.
//!
//! Given a frozen [`MarketSelectionBuildRequest`] and a frozen
//! `Vec<MarketCandidate>`, it runs the [`FilterChain`], stably caps the survivors
//! at `max_selection_size`, computes the canonical [`selector_hash`], and returns
//! a [`MarketSelectionSnapshot`]. It performs no I/O and reads no clock — an
//! empty result is a normal snapshot, never an error.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::MarketCandidate,
    types::{MarketId, MarketSelectionId, SelectionExclusionSummary},
};

use crate::{
    features::FeatureSchema,
    hashing::ResearchHasher,
    selection::{
        ExcludedMarket, ExclusionReason, FilterChain, FilterOutcome, MarketCandidateCtx,
        MarketSelectionBuildRequest, MarketSelectionSnapshot, MarketSelector, SelectedMarket,
        SelectionResult, SelectionThresholds, SelectorHashInput, accumulate_exclusion,
    },
};

/// The default, config-driven market selector.
pub struct ConfiguredMarketSelector {
    chain: FilterChain,
}

impl ConfiguredMarketSelector {
    /// Build a selector backed by the canonical 7-stage filter chain.
    #[must_use]
    pub fn new() -> Self {
        Self {
            chain: FilterChain::standard(),
        }
    }

    /// Run the filter chain + stable cap over `candidates`, returning the
    /// included / excluded partition — the pure selection core, with no snapshot
    /// id or canonical hash.
    ///
    /// Both the online snapshot builder ([`Self::build_snapshot`]) and the
    /// offline point-in-time dataset selector share this one code path, so the
    /// training funnel is byte-for-byte the same filter policy as production.
    pub fn select_markets(
        &self,
        request: &MarketSelectionBuildRequest,
        candidates: &[MarketCandidate],
    ) -> QuantResult<SelectionResult> {
        let thresholds = SelectionThresholds::resolve(&request.selection, &request.data_quality)?;
        let feature_schema = FeatureSchema::build(&request.features);

        let mut included = Vec::new();
        let mut excluded = Vec::new();
        let mut exclusion_summary = SelectionExclusionSummary::default();

        for candidate in candidates {
            let ctx = MarketCandidateCtx {
                candidate,
                thresholds: &thresholds,
                as_of: request.as_of,
                model_requirements: &request.model_requirements,
                feature_schema: &feature_schema,
            };
            match self.chain.evaluate(&ctx) {
                FilterOutcome::Keep => included.push(SelectedMarket::from(candidate)),
                FilterOutcome::Exclude(reason) => {
                    accumulate_exclusion(&mut exclusion_summary, &reason);
                    excluded.push(ExcludedMarket {
                        market_id: candidate.market_id.clone(),
                        reason,
                    });
                }
            }
        }

        // Stable cap: highest liquidity first, then market id ascending, so the
        // same candidate slice always yields the same truncated selection.
        included.sort_by(|left, right| {
            right
                .liquidity_usd
                .cmp(&left.liquidity_usd)
                .then_with(|| left.market_id.as_str().cmp(right.market_id.as_str()))
        });
        let cap = request.selection.max_selection_size as usize;
        if included.len() > cap {
            for market in included.drain(cap..) {
                accumulate_exclusion(
                    &mut exclusion_summary,
                    &ExclusionReason::SelectionCapExceeded,
                );
                excluded.push(ExcludedMarket {
                    market_id: market.market_id,
                    reason: ExclusionReason::SelectionCapExceeded,
                });
            }
        }

        Ok(SelectionResult {
            included,
            excluded,
            exclusion_summary,
        })
    }
}

impl Default for ConfiguredMarketSelector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MarketSelector for ConfiguredMarketSelector {
    async fn build_snapshot(
        &self,
        request: MarketSelectionBuildRequest,
        candidates: Vec<MarketCandidate>,
    ) -> QuantResult<MarketSelectionSnapshot> {
        let SelectionResult {
            included,
            excluded,
            exclusion_summary,
        } = self.select_markets(&request, &candidates)?;

        let included_ids = included
            .iter()
            .map(|market| market.market_id.clone())
            .collect::<Vec<MarketId>>();
        let selector_hash =
            ResearchHasher::canonical(&SelectorHashInput::new(&request, &included_ids))?;

        Ok(MarketSelectionSnapshot {
            market_selection_id: MarketSelectionId::from_v7(),
            as_of: request.as_of,
            runtime_config_version_id: request.runtime_config_version_id,
            selector_hash,
            included,
            excluded,
            exclusion_summary,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ConfiguredMarketSelector;
    use crate::{
        features::names,
        selection::{
            ExclusionReason, MarketSelectionBuildRequest, MarketSelectionSnapshot, MarketSelector,
            ModelFeatureRequirements,
        },
    };
    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_models::{
        domain::{DomainAvailability, MarketCandidate},
        enums::{common::MarketCategory, market::MarketStatus},
        runtime_config::{DataQualityConfig, DecimalString, FeaturesConfig, SelectionConfig},
        types::{EventId, MarketId, Price, RuntimeConfigVersionId, TokenId, Usd},
    };
    use rust_decimal::Decimal;

    fn as_of() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap()
    }

    /// A candidate that passes every default-threshold filter.
    fn healthy_candidate(id: &str) -> MarketCandidate {
        MarketCandidate {
            market_id: MarketId::new(id),
            event_id: EventId::new("evt-1"),
            category: MarketCategory::Sports,
            status: MarketStatus::Active,
            primary_token_id: TokenId::new("token-yes"),
            secondary_token_id: Some(TokenId::new("token-no")),
            end_date: Some(as_of() + chrono::Duration::days(7)),
            liquidity_usd: Some(Usd::new(Decimal::from(10_000))),
            volume_24h_usd: Some(Usd::new(Decimal::from(5_000))),
            best_bid: Some(Price::new(Decimal::new(49, 2))),
            best_ask: Some(Price::new(Decimal::new(51, 2))),
            depth_usd: Some(Usd::new(Decimal::from(2_000))),
            book_age_ms: Some(500),
            crossed: false,
            empty: false,
            connection_healthy: true,
            ingest_lag_ms: 1_000,
            domain_availability: DomainAvailability::NotMapped,
            observed_at: as_of(),
        }
    }

    fn selection_config() -> SelectionConfig {
        SelectionConfig {
            enabled_categories: vec![MarketCategory::Sports],
            min_liquidity_usd: DecimalString::new("1000"),
            min_volume_24h_usd: DecimalString::new("1000"),
            ..SelectionConfig::default()
        }
    }

    fn request_with(selection: SelectionConfig) -> MarketSelectionBuildRequest {
        request_with_model(selection, ModelFeatureRequirements::default())
    }

    fn request_with_model(
        selection: SelectionConfig,
        model_requirements: ModelFeatureRequirements,
    ) -> MarketSelectionBuildRequest {
        MarketSelectionBuildRequest {
            as_of: as_of(),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            selection,
            data_quality: DataQualityConfig::default(),
            features: FeaturesConfig::default(),
            model_requirements,
            source_delay_secs: 10,
        }
    }

    async fn build(
        request: MarketSelectionBuildRequest,
        candidates: Vec<MarketCandidate>,
    ) -> MarketSelectionSnapshot {
        ConfiguredMarketSelector::new()
            .build_snapshot(request, candidates)
            .await
            .expect("snapshot")
    }

    #[tokio::test]
    async fn selector_keeps_only_open_markets() {
        let mut paused = healthy_candidate("0xpaused");
        paused.status = MarketStatus::Paused;
        let snapshot = build(
            request_with(selection_config()),
            vec![healthy_candidate("0xopen"), paused],
        )
        .await;

        assert_eq!(snapshot.included.len(), 1);
        assert_eq!(snapshot.included[0].market_id.as_str(), "0xopen");
        assert_eq!(snapshot.excluded.len(), 1);
        assert_eq!(snapshot.excluded[0].reason, ExclusionReason::NotOpen);
    }

    #[tokio::test]
    async fn category_filter_respects_enabled_categories() {
        let mut politics = healthy_candidate("0xpolitics");
        politics.category = MarketCategory::Politics;

        let snapshot = build(
            request_with(selection_config()),
            vec![healthy_candidate("0xsports"), politics],
        )
        .await;

        let kept = snapshot
            .included
            .iter()
            .map(|market| market.market_id.as_str().to_owned())
            .collect::<Vec<_>>();
        assert!(kept.contains(&"0xsports".to_owned()));
        assert!(!kept.contains(&"0xpolitics".to_owned()));
        assert_eq!(
            snapshot.excluded[0].reason,
            ExclusionReason::CategoryDisabled
        );
    }

    #[tokio::test]
    async fn liquidity_filter_excludes_below_thresholds() {
        let mut thin = healthy_candidate("0xthin");
        thin.liquidity_usd = Some(Usd::new(Decimal::from(10)));
        let mut wide = healthy_candidate("0xwide");
        wide.best_bid = Some(Price::new(Decimal::new(10, 2)));
        wide.best_ask = Some(Price::new(Decimal::new(90, 2)));

        let snapshot = build(
            request_with(selection_config()),
            vec![healthy_candidate("0xok"), thin, wide],
        )
        .await;

        assert_eq!(snapshot.included.len(), 1);
        let reasons = snapshot
            .excluded
            .iter()
            .map(|market| market.reason.clone())
            .collect::<Vec<_>>();
        assert!(reasons.contains(&ExclusionReason::InsufficientLiquidity));
        assert!(reasons.contains(&ExclusionReason::SpreadTooWide));
    }

    #[tokio::test]
    async fn data_quality_filter_is_connection_aware_for_book_age() {
        // Aged book while the connection is UNHEALTHY → stale (may be missing
        // updates).
        let mut stale = healthy_candidate("0xstale");
        stale.book_age_ms = Some(60_000);
        stale.connection_healthy = false;
        // Aged book while the connection is HEALTHY → still the venue truth,
        // admitted (quiet ≠ stale on Polymarket).
        let mut quiet = healthy_candidate("0xquiet");
        quiet.book_age_ms = Some(60_000);
        quiet.connection_healthy = true;
        // Ingest pipeline backpressure is independent of book age.
        let mut lagging = healthy_candidate("0xlag");
        lagging.ingest_lag_ms = 120_000;

        let snapshot = build(
            request_with(selection_config()),
            vec![healthy_candidate("0xfresh"), stale, quiet, lagging],
        )
        .await;

        assert_eq!(snapshot.included.len(), 2);
        let reasons = snapshot
            .excluded
            .iter()
            .map(|market| market.reason.clone())
            .collect::<Vec<_>>();
        assert!(reasons.contains(&ExclusionReason::StaleBook));
        assert!(reasons.contains(&ExclusionReason::IngestLagExceeded));
        assert_eq!(snapshot.exclusion_summary.stale_book_count, 1);
    }

    #[tokio::test]
    async fn manually_blocked_status_excludes_market() {
        let mut blocked = healthy_candidate("0xblocked");
        blocked.status = MarketStatus::ManuallyBlocked;
        let snapshot = build(
            request_with(selection_config()),
            vec![healthy_candidate("0xok"), blocked],
        )
        .await;

        assert_eq!(snapshot.included.len(), 1);
        assert_eq!(snapshot.included[0].market_id.as_str(), "0xok");
        assert_eq!(
            snapshot.excluded[0].reason,
            ExclusionReason::ManuallyBlocked
        );
        assert_eq!(snapshot.exclusion_summary.excluded_by_operator_count, 1);
    }

    #[tokio::test]
    async fn model_eligibility_keeps_market_with_available_required_feature() {
        // A healthy candidate has a two-sided book, so a book-derived feature is
        // available — the market is kept (no blanket fail-closed).
        let model_requirements = ModelFeatureRequirements {
            required_features: vec![names::book::SPREAD_BPS],
        };
        let snapshot = build(
            request_with_model(selection_config(), model_requirements),
            vec![healthy_candidate("0xok")],
        )
        .await;

        assert_eq!(snapshot.included.len(), 1);
        assert_eq!(snapshot.included[0].market_id.as_str(), "0xok");
    }

    #[tokio::test]
    async fn model_eligibility_excludes_unavailable_feature() {
        // A model requiring a feature the schema does not define makes the market
        // ineligible — and only that feature is reported missing (the oracle never
        // claims to provide a feature it does not declare).
        let model_requirements = ModelFeatureRequirements {
            required_features: vec![crate::features::FeatureName::from_static(
                "nonexistent.feature",
            )],
        };
        let snapshot = build(
            request_with_model(selection_config(), model_requirements),
            vec![healthy_candidate("0xok")],
        )
        .await;

        assert!(snapshot.included.is_empty());
        match &snapshot.excluded[0].reason {
            ExclusionReason::ModelFeatureUnavailable { missing } => {
                assert_eq!(missing.len(), 1);
                assert_eq!(missing[0].as_str(), "nonexistent.feature");
            }
            other => panic!("expected ModelFeatureUnavailable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn selector_hash_is_deterministic() {
        let request = request_with(selection_config());
        let candidates = vec![healthy_candidate("0xa"), healthy_candidate("0xb")];

        let first = ConfiguredMarketSelector::new()
            .build_snapshot(request.clone(), candidates.clone())
            .await
            .expect("snapshot");
        // Reorder candidates: the hash must not change.
        let mut reordered = candidates;
        reordered.reverse();
        let second = ConfiguredMarketSelector::new()
            .build_snapshot(request, reordered)
            .await
            .expect("snapshot");

        assert_eq!(first.selector_hash, second.selector_hash);
        assert_ne!(
            first.market_selection_id, second.market_selection_id,
            "snapshot id is fresh per build"
        );
    }

    #[tokio::test]
    async fn selector_hash_changes_when_data_quality_changes() {
        let candidates = vec![healthy_candidate("0xa"), healthy_candidate("0xb")];
        let mut request_a = request_with(selection_config());
        request_a.data_quality.max_book_age_ms = 1_000;
        let mut request_b = request_with(selection_config());
        request_b.data_quality.max_book_age_ms = 2_000;

        let hash_a = ConfiguredMarketSelector::new()
            .build_snapshot(request_a, candidates.clone())
            .await
            .expect("snapshot")
            .selector_hash;
        let hash_b = ConfiguredMarketSelector::new()
            .build_snapshot(request_b, candidates)
            .await
            .expect("snapshot")
            .selector_hash;

        assert_ne!(hash_a, hash_b);
    }

    #[tokio::test]
    async fn empty_selection_persists_with_reason() {
        let mut closed = healthy_candidate("0xclosed");
        closed.status = MarketStatus::Settled;
        let snapshot = build(request_with(selection_config()), vec![closed]).await;

        assert!(snapshot.included.is_empty());
        assert_eq!(snapshot.excluded.len(), 1);
        assert_eq!(snapshot.excluded[0].reason, ExclusionReason::NotOpen);
        // Empty selection is still a valid, hashable snapshot.
        assert!(!snapshot.selector_hash.as_str().is_empty());
    }

    #[tokio::test]
    async fn max_selection_size_truncation_is_stable() {
        let mut high = healthy_candidate("0xhigh");
        high.liquidity_usd = Some(Usd::new(Decimal::from(90_000)));
        let mut mid = healthy_candidate("0xmid");
        mid.liquidity_usd = Some(Usd::new(Decimal::from(50_000)));
        let mut low = healthy_candidate("0xlow");
        low.liquidity_usd = Some(Usd::new(Decimal::from(10_000)));

        let mut selection = selection_config();
        selection.max_selection_size = 2;

        let snapshot = build(
            request_with(selection),
            vec![low, high.clone(), mid.clone()],
        )
        .await;

        assert_eq!(snapshot.included.len(), 2);
        assert_eq!(snapshot.included[0].market_id.as_str(), "0xhigh");
        assert_eq!(snapshot.included[1].market_id.as_str(), "0xmid");
        assert_eq!(snapshot.excluded.len(), 1);
        assert_eq!(
            snapshot.excluded[0].market_id.as_str(),
            "0xlow",
            "cap-truncated market must appear in excluded"
        );
        assert_eq!(
            snapshot.excluded[0].reason,
            ExclusionReason::SelectionCapExceeded
        );
    }

    #[tokio::test]
    async fn liquidity_filter_excludes_missing_volume() {
        let mut missing = healthy_candidate("0xmissing");
        missing.volume_24h_usd = None;

        let snapshot = build(
            request_with(selection_config()),
            vec![healthy_candidate("0xok"), missing],
        )
        .await;

        assert_eq!(snapshot.included.len(), 1);
        assert_eq!(snapshot.included[0].market_id.as_str(), "0xok");
        assert_eq!(
            snapshot.excluded[0].reason,
            ExclusionReason::InsufficientLiquidity
        );
    }
}
