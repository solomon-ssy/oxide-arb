//! Offline point-in-time market selection.
//!
//! Reconstructs, per historical `as_of`, the same market-selection funnel the
//! online report pipeline runs — by projecting a [`MarketCandidate`] from
//! point-in-time facts and evaluating it through the **identical**
//! [`ConfiguredMarketSelector`] / `FilterChain::standard()` code. Only markets
//! that survive the funnel at that instant enter the training spine, so the
//! offline dataset carries no train/serve selection skew.
//!
//! # Honest approximations (data availability)
//!
//! Gamma `liquidity_usd` / `volume_24h` are not historized in the offline plane,
//! so the funnel is replayed with principled substitutions:
//!
//! - **liquidity** → the book's combined visible USD depth ([`ResolvedBook::visible_liquidity_usd`]),
//!   gated by the frozen `training.min_selection_depth_usd` floor (a book-depth
//!   quantity, distinct from the Gamma-calibrated online `min_liquidity_usd`);
//! - **24h volume** → the volume floor is skipped offline (`min_volume_24h_usd = 0`),
//!   as trade-print volume is not the same measure as the Gamma figure;
//! - **feed health** (`connection_healthy` / `ingest_lag_ms`) → treated as
//!   healthy / zero: a stored historical book is the venue truth by construction,
//!   with no live-feed staleness to guard against.
//!
//! Every other gate (status, category, spread, book freshness, resolution window,
//! model eligibility) runs against exact point-in-time facts with the frozen
//! config's own thresholds.
//!
//! # Model-feature gating (11.2.2 remediation R7)
//!
//! `DatasetPlanRequest.model_spec_id` names the `ModelSpec` this dataset is
//! built **for** — the target model's requirements are therefore knowable at
//! plan/build time, not a hypothetical future unknown. The caller resolves
//! the target spec's governed `feature_requirements` and passes it into
//! [`OfflinePitSelector::new`], so `ModelFeatureUnavailable` genuinely
//! excludes a market whose domain features that spec's model would need are
//! unavailable — mirroring exactly what the online funnel enforces once a
//! version trained under this spec is routed, rather than a permissive
//! `ModelFeatureRequirements::default()` placeholder.
//!
//! # Domain availability (11.2.2 §3.8 train-serve parity)
//!
//! [`select_at`](OfflinePitSelector::select_at) takes a
//! [`DomainAvailabilitySource`] —
//! [`PrefetchedDomainAvailabilitySource`](crate::prefetch::domain_availability::PrefetchedDomainAvailabilitySource)
//! for the real dataset build (zero I/O, replayed from the batch-prefetched
//! linkage + observation window) or
//! [`LiveDomainAvailabilitySource`](crate::prefetch::domain_availability::LiveDomainAvailabilitySource)
//! for the keep-rate dry-run estimate (bounded live reads) — so
//! `domain_availability` genuinely reflects whether the market's linkage is
//! `Resolved` and its bound instrument has a visible observation, exactly
//! mirroring what
//! [`MarketCandidateProvider`](crate::prefetch::market_candidates::MarketCandidateProvider)
//! would compute online for the same evidence. A crypto (or any
//! domain-mapped) market can therefore genuinely reach `Available` offline —
//! it is never hardcoded to `Unresolved`, which would otherwise make any
//! model requiring a `domain.*` feature exclude 100% of that category
//! regardless of real evidence. See `crate::prefetch::domain_availability`
//! for the shared decision rule and its one documented honest approximation
//! (the prefetched observation window has a lookback-bounded lower edge,
//! unlike the online plane's unbounded "most recent observation before
//! cutoff" read).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{DomainAvailability, MarketCandidate, MarketInfo},
    runtime_config::{DataQualityConfig, DecimalString, FeaturesConfig, SelectionConfig},
    types::{MarketId, RuntimeConfigVersionId, Usd},
};
use quant_pivot_research::{
    features::ResolvedBook,
    pit::{MarketContextAt, PitQueryEngine},
    selection::{
        ConfiguredMarketSelector, MarketSelectionBuildRequest, ModelFeatureRequirements,
        SelectionResult,
    },
};

use crate::prefetch::domain_availability::DomainAvailabilitySource;

/// Replays the online selection funnel over point-in-time facts, per `as_of`.
pub struct OfflinePitSelector {
    selector: ConfiguredMarketSelector,
    /// Frozen selection policy, with the two non-historized floors overridden for
    /// offline replay (`min_liquidity_usd` → book-depth floor, `min_volume` → 0).
    selection: SelectionConfig,
    data_quality: DataQualityConfig,
    features: FeaturesConfig,
    runtime_config_version_id: RuntimeConfigVersionId,
    source_delay_secs: u64,
    /// The target `ModelSpec`'s declared feature requirements (11.2.2
    /// remediation R7) — genuinely gates `ModelFeatureUnavailable`, mirroring
    /// exactly what the online funnel would enforce once this spec's model is
    /// trained and routed, rather than a permissive placeholder.
    model_requirements: ModelFeatureRequirements,
}

impl OfflinePitSelector {
    /// Wire the selector from a build's frozen config snapshot.
    #[must_use]
    pub fn new(
        selection: &SelectionConfig,
        data_quality: &DataQualityConfig,
        features: &FeaturesConfig,
        min_selection_depth_usd: &DecimalString,
        runtime_config_version_id: RuntimeConfigVersionId,
        source_delay_secs: u64,
        model_requirements: ModelFeatureRequirements,
    ) -> Self {
        // Override only the two Gamma-sourced floors the offline plane cannot
        // reproduce; every other threshold stays at its frozen value.
        let mut offline_selection = selection.clone();
        offline_selection.min_liquidity_usd = min_selection_depth_usd.clone();
        offline_selection.min_volume_24h_usd = DecimalString::new("0");
        Self {
            selector: ConfiguredMarketSelector::new(),
            selection: offline_selection,
            data_quality: data_quality.clone(),
            features: features.clone(),
            runtime_config_version_id,
            source_delay_secs,
            model_requirements,
        }
    }

    /// Run the funnel over `markets` at `as_of`, returning the kept/excluded
    /// partition. Book + market context are resolved point-in-time from
    /// `pit`; domain-plane availability is resolved once, batched, from
    /// `domain_availability` (see the module-level "Domain availability"
    /// section for which backend the caller should pass).
    pub async fn select_at(
        &self,
        as_of: DateTime<Utc>,
        markets: &[&MarketInfo],
        pit: &dyn PitQueryEngine,
        domain_availability: &dyn DomainAvailabilitySource,
    ) -> QuantResult<SelectionResult> {
        let availability = domain_availability.resolve(as_of, markets).await?;
        let mut candidates = Vec::with_capacity(markets.len());
        for market in markets {
            let context = pit.market_at_from_info(market, as_of).await?;
            let book = pit
                .book_at(&market.yes_token_id, as_of)
                .await?
                .map(ResolvedBook::from);
            let domain = availability_for(&availability, &market.market_id);
            candidates.push(project_candidate(
                market,
                context.as_ref(),
                book.as_ref(),
                as_of,
                domain,
            ));
        }
        let request = self.request(as_of);
        self.selector.select_markets(&request, &candidates)
    }

    fn request(&self, as_of: DateTime<Utc>) -> MarketSelectionBuildRequest {
        MarketSelectionBuildRequest {
            as_of,
            runtime_config_version_id: self.runtime_config_version_id.clone(),
            selection: self.selection.clone(),
            data_quality: self.data_quality.clone(),
            features: self.features.clone(),
            model_requirements: self.model_requirements.clone(),
            source_delay_secs: self.source_delay_secs,
        }
    }
}

/// Look up one market's resolved domain availability, defaulting to
/// `NotMapped` for a market the source's batch omitted (mirrors the online
/// projector's own `unwrap_or(NotMapped)` fallback).
fn availability_for(
    availability: &HashMap<MarketId, DomainAvailability>,
    market_id: &MarketId,
) -> DomainAvailability {
    availability
        .get(market_id)
        .copied()
        .unwrap_or(DomainAvailability::NotMapped)
}

/// Project a point-in-time [`MarketCandidate`] from registry metadata + PIT book.
fn project_candidate(
    market: &MarketInfo,
    context: Option<&MarketContextAt>,
    book: Option<&ResolvedBook>,
    as_of: DateTime<Utc>,
    domain_availability: DomainAvailability,
) -> MarketCandidate {
    let depth_usd = book.map(ResolvedBook::visible_liquidity_usd);
    MarketCandidate {
        market_id: market.market_id.clone(),
        event_id: market.event_id.clone(),
        category: market.fee_category(),
        // Prefer the point-in-time lifecycle status (resolution-aware) over the
        // current registry status, so a since-resolved market is `Active` at an
        // `as_of` that predates its resolution.
        status: context.map_or(market.status, |ctx| ctx.status),
        primary_token_id: market.yes_token_id.clone(),
        secondary_token_id: Some(market.no_token_id.clone()),
        end_date: context.and_then(|ctx| ctx.end_date).or(market.end_date),
        // Gamma liquidity/volume are not historized: book depth is the liquidity
        // proxy; the volume floor is skipped via the offline threshold override.
        liquidity_usd: depth_usd,
        volume_24h_usd: Some(Usd::ZERO),
        best_bid: book.and_then(ResolvedBook::best_bid),
        best_ask: book.and_then(ResolvedBook::best_ask),
        depth_usd,
        book_age_ms: book.map(|resolved| book_age_ms(resolved, as_of)),
        crossed: book.is_some_and(ResolvedBook::is_crossed),
        // No book at `as_of` ⇒ treat as empty (fail-closed, like the online plane).
        empty: book.is_none_or(ResolvedBook::is_empty),
        // Offline replay: a stored book is the venue truth (no live-feed staleness).
        connection_healthy: true,
        ingest_lag_ms: 0,
        // Resolved by the caller's `DomainAvailabilitySource` (see the
        // module-level "Domain availability" section) — genuinely mirrors
        // the online plane's linkage + observation evidence, never a
        // hardcoded placeholder.
        domain_availability,
        observed_at: as_of,
    }
}

/// Book age in milliseconds at `as_of` (clamped non-negative).
fn book_age_ms(book: &ResolvedBook, as_of: DateTime<Utc>) -> u64 {
    let published = i64::try_from(book.timestamp_ms).unwrap_or(i64::MAX);
    u64::try_from((as_of.timestamp_millis() - published).max(0)).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{OfflinePitSelector, project_candidate};
    use crate::prefetch::{
        domain_availability::PrefetchedDomainAvailabilitySource, historical_window::Prefetched,
    };
    use async_trait::async_trait;
    use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::{
        domain::{
            CryptoSubject, DomainAvailability, DomainObservation, GroundingProof, LinkageOutcome,
            MarketInfo, MarketLinkage, MarketSubject, PriceComparator, ResolutionOracle,
            ResolvedBinding, market::book::BookLevel,
        },
        enums::{
            common::{MarketCategory, TickSize},
            domain::{DomainFamily, DomainMetric, KlineInterval, ResolverTier},
            market::MarketStatus,
        },
        runtime_config::{
            DataQualityConfig, DecimalString, DomainConfig, FeaturesConfig, SelectionConfig,
        },
        types::{
            BinanceSymbol, ContentHash, CryptoAsset, CryptoQuote, DomainInstrumentKey,
            DomainSourceId, EventId, MarketId, MarketLinkageId, Price, Probability,
            ResolverVersion, RuntimeConfigVersionId, Shares, TokenId, Usd,
        },
    };
    use quant_pivot_research::{
        features::{FeatureName, ResolvedBook},
        pit::{BookSnapshotAt, MarketContextAt, PitQueryEngine},
        selection::{ExclusionReason, ModelFeatureRequirements},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::{collections::HashMap, sync::Arc};

    fn market() -> MarketInfo {
        let now = Utc.timestamp_millis_opt(1_000_000).single().expect("ts");
        MarketInfo {
            market_id: MarketId::new("m"),
            event_id: EventId::new("e"),
            question: "q".to_owned(),
            slug: "s".to_owned(),
            description: None,
            categories: vec![MarketCategory::Sports],
            status: MarketStatus::Active,
            outcome: None,
            yes_token_id: TokenId::new("yes"),
            no_token_id: TokenId::new("no"),
            tick_size: TickSize::Hundredth,
            neg_risk: false,
            end_date: Some(now),
            resolved_at: None,
            fees_enabled: false,
            fee_rate: None,
            fee_exponent: None,
            fee_taker_only: None,
            fee_rebate_rate: None,
            fee_source: None,
            fee_observed_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn level(price: &str, size: u64) -> BookLevel {
        BookLevel::from_decimal_unchecked(
            Price::new(Decimal::from_str_exact(price).expect("price")),
            Shares::new(Decimal::from(size)),
        )
    }

    #[test]
    fn projects_book_depth_as_liquidity_with_volume_and_feed_sentinels() {
        let as_of = Utc.timestamp_millis_opt(2_000_000).single().expect("ts");
        let book = ResolvedBook::from(BookSnapshotAt {
            token_id: TokenId::new("yes"),
            as_of,
            bids: Arc::from([level("0.48", 100)]),
            asks: Arc::from([level("0.52", 100)]),
            timestamp_ms: 1_995_000,
            version: 1,
        });
        let candidate = project_candidate(
            &market(),
            None,
            Some(&book),
            as_of,
            DomainAvailability::NotMapped,
        );

        // Book depth is the liquidity proxy; volume floor is skipped (sentinel 0).
        assert_eq!(candidate.liquidity_usd, Some(book.visible_liquidity_usd()));
        assert_eq!(candidate.depth_usd, Some(book.visible_liquidity_usd()));
        assert_eq!(candidate.volume_24h_usd, Some(Usd::ZERO));
        // Offline replay: the stored book is venue truth (no live-feed staleness).
        assert!(candidate.connection_healthy);
        assert_eq!(candidate.ingest_lag_ms, 0);
        assert_eq!(candidate.book_age_ms, Some(5_000));
        assert!(!candidate.crossed);
        assert!(!candidate.empty);
        assert_eq!(candidate.category, MarketCategory::Sports);
        // The projector never computes availability itself — it trusts whatever
        // the caller's `DomainAvailabilitySource` resolved (see `select_at`).
        assert_eq!(candidate.domain_availability, DomainAvailability::NotMapped);
    }

    #[test]
    fn missing_book_projects_empty_fail_closed() {
        let as_of = Utc.timestamp_millis_opt(2_000_000).single().expect("ts");
        let candidate =
            project_candidate(&market(), None, None, as_of, DomainAvailability::Unresolved);
        assert!(candidate.empty, "no book at as_of ⇒ empty (fail-closed)");
        assert_eq!(candidate.liquidity_usd, None);
        assert_eq!(candidate.best_bid, None);
        // Passed straight through, independent of the book/liquidity projection.
        assert_eq!(
            candidate.domain_availability,
            DomainAvailability::Unresolved
        );
    }

    // ── select_at(): domain availability drives ModelEligibilityFilter ─────

    fn crypto_market(market_id: &str, end_date: DateTime<Utc>) -> MarketInfo {
        let mut info = market();
        info.market_id = MarketId::new(market_id);
        info.categories = vec![MarketCategory::Crypto];
        info.end_date = Some(end_date);
        info
    }

    /// A `PitQueryEngine` returning a fixed, healthy two-sided book and no
    /// market-context override (so the candidate falls back to the
    /// `MarketInfo`'s own `status`/`end_date`, mirroring a market with no
    /// resolution event yet).
    struct FixedBookPitEngine {
        book: BookSnapshotAt,
    }

    #[async_trait]
    impl PitQueryEngine for FixedBookPitEngine {
        async fn book_at(
            &self,
            _token_id: &TokenId,
            _as_of: DateTime<Utc>,
        ) -> QuantResult<Option<BookSnapshotAt>> {
            Ok(Some(self.book.clone()))
        }

        async fn market_at(
            &self,
            _market_id: &MarketId,
            _as_of: DateTime<Utc>,
        ) -> QuantResult<Option<MarketContextAt>> {
            Ok(None)
        }
    }

    fn healthy_book(token: &str, as_of: DateTime<Utc>) -> BookSnapshotAt {
        BookSnapshotAt {
            token_id: TokenId::new(token),
            as_of,
            bids: Arc::from([level("0.48", 100)]),
            asks: Arc::from([level("0.52", 100)]),
            timestamp_ms: u64::try_from(as_of.timestamp_millis()).unwrap_or(0),
            version: 1,
        }
    }

    fn instrument() -> DomainInstrumentKey {
        DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            KlineInterval::OneMinute,
        )
    }

    fn resolved_linkage(market_id: &str, derived_at: DateTime<Utc>) -> MarketLinkage {
        let market_id = MarketId::new(market_id);
        let now = derived_at;
        let outcome = LinkageOutcome::Resolved(ResolvedBinding {
            subject: MarketSubject::Crypto(CryptoSubject {
                asset: CryptoAsset::parse("BTC").expect("asset"),
                quote: CryptoQuote::parse("USD").expect("quote"),
                comparator: PriceComparator::UpVsReference,
                strike: None,
                reference_at: Some(now - ChronoDuration::minutes(5)),
                observation_at: now + ChronoDuration::days(1),
                resolution_oracle: ResolutionOracle::BinanceKline {
                    symbol: BinanceSymbol::parse("BTCUSDT").expect("symbol"),
                    interval: KlineInterval::OneMinute,
                },
            }),
            instrument_key: instrument(),
            grounding: GroundingProof { spans: Vec::new() },
            override_context: None,
        });
        let metadata_hash = ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("hash");
        let content_hash = MarketLinkage::compute_content_hash(
            &market_id,
            DomainFamily::Crypto,
            &outcome,
            ResolverTier::Tier0Slug,
            ResolverVersion::FIRST,
            &metadata_hash,
        )
        .expect("hash");
        MarketLinkage {
            linkage_id: MarketLinkageId::from_v7(),
            market_id,
            domain_family: DomainFamily::Crypto,
            outcome,
            confidence: Probability::ONE,
            resolver_tier: ResolverTier::Tier0Slug,
            resolver_version: ResolverVersion::FIRST,
            metadata_hash,
            content_hash,
            derived_at,
            created_at: derived_at,
        }
    }

    fn empty_prefetched() -> Prefetched {
        Prefetched {
            books: HashMap::new(),
            micro: HashMap::new(),
            trade_tape: HashMap::new(),
            resolutions: HashMap::new(),
            markets_by_id: HashMap::new(),
            neg_risk_leg_sets: HashMap::new(),
            domain_observations: HashMap::new(),
            linkages: HashMap::new(),
        }
    }

    fn crypto_model_requirements() -> ModelFeatureRequirements {
        let mut requirements = ModelFeatureRequirements::default();
        requirements.by_category.insert(
            MarketCategory::Crypto,
            vec![FeatureName::from_static("domain.crypto.distance_to_strike")],
        );
        requirements
    }

    #[tokio::test]
    async fn select_at_includes_crypto_market_when_linkage_resolved_and_observed() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let market = crypto_market("0xcrypto", as_of + ChronoDuration::days(7));

        let mut prefetched = empty_prefetched();
        prefetched.linkages.insert(
            market.market_id.clone(),
            vec![resolved_linkage(
                "0xcrypto",
                as_of - ChronoDuration::hours(1),
            )],
        );
        prefetched.domain_observations.insert(
            instrument(),
            vec![DomainObservation {
                family: DomainFamily::Crypto,
                source_id: DomainSourceId::binance(),
                instrument_key: instrument(),
                metric: DomainMetric::Close,
                value: dec!(100000),
                observed_at: as_of - ChronoDuration::seconds(10),
                publish_time: as_of - ChronoDuration::seconds(10),
            }],
        );
        let domain_config = DomainConfig::default();
        let domain_source = PrefetchedDomainAvailabilitySource::new(&prefetched, &domain_config);

        let selector = OfflinePitSelector::new(
            &SelectionConfig {
                enabled_categories: vec![MarketCategory::Crypto],
                ..SelectionConfig::default()
            },
            &DataQualityConfig::default(),
            &FeaturesConfig::default(),
            &DecimalString::new("0"),
            RuntimeConfigVersionId::from_v7(),
            domain_config.crypto.source_delay_secs,
            crypto_model_requirements(),
        );
        let pit = FixedBookPitEngine {
            book: healthy_book("yes", as_of),
        };

        let result = selector
            .select_at(as_of, &[&market], &pit, &domain_source)
            .await
            .expect("selection");

        assert_eq!(
            result.included.len(),
            1,
            "resolved linkage + visible observation must yield Available, not a hardcoded \
             Unresolved that fails ModelEligibilityFilter: excluded = {:?}",
            result.excluded
        );
        assert!(
            result.excluded.is_empty(),
            "expected no exclusions, got {:?}",
            result.excluded
        );
    }

    #[tokio::test]
    async fn select_at_excludes_crypto_market_when_linkage_unresolved() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let market = crypto_market("0xunresolved", as_of + ChronoDuration::days(7));

        // No linkage at all for this market ⇒ Unresolved ⇒ the required domain
        // feature is unavailable ⇒ ModelFeatureUnavailable.
        let prefetched = empty_prefetched();
        let domain_config = DomainConfig::default();
        let domain_source = PrefetchedDomainAvailabilitySource::new(&prefetched, &domain_config);

        let selector = OfflinePitSelector::new(
            &SelectionConfig {
                enabled_categories: vec![MarketCategory::Crypto],
                ..SelectionConfig::default()
            },
            &DataQualityConfig::default(),
            &FeaturesConfig::default(),
            &DecimalString::new("0"),
            RuntimeConfigVersionId::from_v7(),
            domain_config.crypto.source_delay_secs,
            crypto_model_requirements(),
        );
        let pit = FixedBookPitEngine {
            book: healthy_book("yes", as_of),
        };

        let result = selector
            .select_at(as_of, &[&market], &pit, &domain_source)
            .await
            .expect("selection");

        assert!(result.included.is_empty());
        assert_eq!(result.excluded.len(), 1);
        match &result.excluded[0].reason {
            ExclusionReason::ModelFeatureUnavailable { missing } => {
                assert!(
                    missing
                        .iter()
                        .any(|name| name.as_str() == "domain.crypto.distance_to_strike")
                );
            }
            other => panic!("expected ModelFeatureUnavailable, got {other:?}"),
        }
    }
}
