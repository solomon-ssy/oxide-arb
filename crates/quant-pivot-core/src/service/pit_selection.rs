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
//! The append-only catalog ledger historizes Gamma liquidity and 24h volume, so
//! offline replay applies the exact governed selection thresholds to the PIT
//! catalog version. Live feed health / ingest lag is explicitly
//! `NotApplicable` for durable replay; book freshness remains enforced from the
//! persisted event time.
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
//! the target spec's governed typed input contract and passes its required raw
//! features into
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
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        DecisionBoundary, DomainAvailability, MarketCandidate, MarketDataHealth, MarketRegistryInfo,
    },
    runtime_config::{DataQualityConfig, FeaturesConfig, SelectionConfig},
    types::{MarketId, RuntimeConfigVersionId},
};
use quant_pivot_research::{
    features::ResolvedBook,
    pit::{MarketContextAt, PointInTimeSnapshotSource},
    selection::{
        ConfiguredMarketSelector, MarketSelectionBuildRequest, ModelFeatureRequirements,
        SelectionResult,
    },
};

use crate::prefetch::domain_availability::DomainAvailabilitySource;
/// Replays the online selection funnel over point-in-time facts, per `as_of`.
pub struct OfflinePitSelector {
    selector: ConfiguredMarketSelector,
    /// Frozen selection policy applied to the historical catalog version.
    selection: SelectionConfig,
    data_quality: DataQualityConfig,
    features: FeaturesConfig,
    runtime_config_version_id: RuntimeConfigVersionId,
    knowledge_lag_secs: u64,
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
        runtime_config_version_id: RuntimeConfigVersionId,
        knowledge_lag_secs: u64,
        model_requirements: ModelFeatureRequirements,
    ) -> Self {
        Self {
            selector: ConfiguredMarketSelector::new(),
            selection: selection.clone(),
            data_quality: data_quality.clone(),
            features: features.clone(),
            runtime_config_version_id,
            knowledge_lag_secs,
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
        boundary: &DecisionBoundary,
        market_ids: &[MarketId],
        pit: &dyn PointInTimeSnapshotSource,
        domain_availability: &dyn DomainAvailabilitySource,
    ) -> QuantResult<SelectionResult> {
        let mut snapshots = Vec::with_capacity(market_ids.len());
        for market_id in market_ids {
            if let Some(snapshot) = pit.market_snapshot_at(market_id, boundary).await? {
                snapshots.push(snapshot);
            }
        }
        let domain_markets: Vec<_> = snapshots
            .iter()
            .map(|snapshot| {
                (
                    snapshot.market.market_id.clone(),
                    snapshot.market.primary_category(),
                )
            })
            .collect();
        let availability = domain_availability
            .resolve(boundary, &domain_markets)
            .await?;
        let mut candidates = Vec::with_capacity(snapshots.len());
        for snapshot in snapshots {
            let market = snapshot.market;
            let book = pit
                .book_at_boundary(&market.token_yes, boundary)
                .await?
                .map(ResolvedBook::try_from)
                .transpose()?;
            let domain = availability_for(&availability, &market.market_id)?;
            candidates.push(project_candidate(
                market.as_ref(),
                &snapshot.context,
                book.as_ref(),
                boundary.decision_at(),
                domain,
            )?);
        }
        let request = self.request(boundary.decision_at());
        self.selector.select_markets(&request, &candidates)
    }

    fn request(&self, decision_at: DateTime<Utc>) -> MarketSelectionBuildRequest {
        MarketSelectionBuildRequest {
            decision_at,
            runtime_config_version_id: self.runtime_config_version_id.clone(),
            selection: self.selection.clone(),
            data_quality: self.data_quality.clone(),
            features: self.features.clone(),
            model_requirements: self.model_requirements.clone(),
            knowledge_lag_secs: self.knowledge_lag_secs,
        }
    }
}

/// Look up one market's resolved domain availability.
///
/// The batch source must return an explicit result for every requested market;
/// absence is an invariant violation, not evidence that the market is
/// structurally unmapped.
fn availability_for(
    availability: &HashMap<MarketId, DomainAvailability>,
    market_id: &MarketId,
) -> QuantResult<DomainAvailability> {
    availability.get(market_id).copied().ok_or_else(|| {
        ResearchError::PitResolution {
            detail: format!("domain availability batch omitted requested market {market_id}"),
        }
        .into()
    })
}

/// Project a point-in-time [`MarketCandidate`] from registry metadata + PIT book.
fn project_candidate(
    market: &MarketRegistryInfo,
    context: &MarketContextAt,
    book: Option<&ResolvedBook>,
    decision_at: DateTime<Utc>,
    domain_availability: DomainAvailability,
) -> QuantResult<MarketCandidate> {
    let depth_usd = book.map(ResolvedBook::visible_liquidity_usd);
    Ok(MarketCandidate {
        market_id: market.market_id.clone(),
        event_id: market.event_id.clone(),
        category: market.primary_category(),
        // Prefer the point-in-time lifecycle status (resolution-aware) over the
        // current registry status, so a since-resolved market is `Active` at an
        // `as_of` that predates its resolution.
        status: context.status,
        primary_token_id: market.token_yes.clone(),
        secondary_token_id: Some(market.token_no.clone()),
        end_date: context.end_date,
        liquidity_usd: market.liquidity_usd,
        volume_24h_usd: market.volume_24h,
        best_bid: book.and_then(ResolvedBook::best_bid),
        best_ask: book.and_then(ResolvedBook::best_ask),
        depth_usd,
        book_age_ms: book
            .map(|resolved| book_age_ms(resolved, decision_at))
            .transpose()?,
        crossed: book.map(ResolvedBook::is_crossed),
        empty: book.map(ResolvedBook::is_empty),
        market_data_health: MarketDataHealth::NotApplicable,
        ingest_lag_ms: None,
        // Resolved by the caller's `DomainAvailabilitySource` (see the
        // module-level "Domain availability" section) — genuinely mirrors
        // the online plane's linkage + observation evidence, never a
        // hardcoded placeholder.
        domain_availability,
        decision_at,
    })
}

/// Book age in milliseconds at `as_of`.
fn book_age_ms(book: &ResolvedBook, decision_at: DateTime<Utc>) -> QuantResult<u64> {
    u64::try_from((decision_at - book.effective_at).num_milliseconds()).map_err(|_| {
        ResearchError::PitResolution {
            detail: format!(
                "book {} observation {} is after decision time {decision_at}",
                book.token_id, book.effective_at
            ),
        }
        .into()
    })
}

#[cfg(test)]
mod tests {
    use super::{OfflinePitSelector, availability_for, project_candidate};
    use crate::prefetch::{
        domain_availability::PrefetchedDomainAvailabilitySource, historical_window::Prefetched,
    };
    use async_trait::async_trait;
    use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::{
        domain::{
            CatalogWindowInfo, CryptoSubject, DecisionBoundary, DecisionClock, DecisionSource,
            DomainAvailability, DomainObservation, EventRegistryInfo, GroundingProof,
            LinkageOutcome, MarketDataHealth, MarketLinkage, MarketSubject, PriceComparator,
            ResolutionOracle, ResolvedBinding, ResolvedSourceBinding,
            market::{
                book::BookLevel,
                registry::{MarketRegistryInfo, NegRiskLegSet, TokenInfo},
            },
        },
        enums::{
            common::{CategorySet, MarketCategory, TickSize},
            domain::{DomainFamily, DomainMetric, KlineInterval, LinkageSourceRole, ResolverTier},
            market::{EventStatus, MarketStatus},
        },
        runtime_config::{DataQualityConfig, DomainConfig, FeaturesConfig, SelectionConfig},
        types::{
            BinanceSymbol, CatalogSyncBatchId, ContentHash, CryptoAsset, CryptoQuote,
            DomainInstrumentKey, DomainSourceId, EventCatalogVersionId, EventId,
            MarketCatalogVersionId, MarketId, MarketLinkageId, Price, Probability, ResolverVersion,
            RuntimeConfigVersionId, Shares, TokenId, Usd,
        },
    };
    use quant_pivot_research::{
        features::{FeatureName, ResolvedBook},
        pit::{BookSnapshotAt, MarketContextAt, PointInTimeSnapshotSource, ResolvedMarketSnapshot},
        selection::{ExclusionReason, ModelFeatureRequirements},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::{collections::HashMap, slice, sync::Arc};

    fn market() -> MarketRegistryInfo {
        let now = Utc.timestamp_millis_opt(1_000_000).single().expect("ts");
        MarketRegistryInfo {
            market_id: MarketId::new("m"),
            event_id: EventId::new("e"),
            token_yes: TokenId::new("yes"),
            token_no: TokenId::new("no"),
            question: "q".to_owned(),
            slug: "s".to_owned(),
            description: None,
            categories: CategorySet::from(MarketCategory::Sports),
            status: MarketStatus::Active,
            outcome: None,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: TokenId::new("yes"),
                    outcome: "Yes".to_owned(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: TokenId::new("no"),
                    outcome: "No".to_owned(),
                    neg_risk: false,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: Decimal::ONE,
            liquidity_usd: None,
            volume_24h: None,
            start_date: Some(now),
            end_date: Some(now),
            resolved_at: None,
            created_at: Some(now),
            updated_at: now,
        }
    }

    fn context(market: &MarketRegistryInfo) -> MarketContextAt {
        MarketContextAt {
            market_id: market.market_id.clone(),
            effective_at: market.updated_at,
            available_at: market.updated_at,
            status: market.status,
            neg_risk: market.neg_risk,
            start_date: market.start_date,
            end_date: market.end_date,
            created_at: market.created_at,
            fee_schedule: None,
        }
    }

    fn level(price: &str, size: u64) -> BookLevel {
        BookLevel::from_decimal_unchecked(
            Price::new(Decimal::from_str_exact(price).expect("price")),
            Shares::new(Decimal::from(size)),
        )
    }

    #[test]
    fn projects_book_depth_separately_from_catalog_liquidity() {
        let as_of = Utc.timestamp_millis_opt(2_000_000).single().expect("ts");
        let book = ResolvedBook::try_from(BookSnapshotAt {
            token_id: TokenId::new("yes"),
            source_cutoff: as_of,
            decision_at: as_of,
            bids: Arc::from([level("0.48", 100)]),
            asks: Arc::from([level("0.52", 100)]),
            timestamp_ms: 1_995_000,
            version: 1,
            sequence: 1,
            source_event: None,
            available_at: as_of,
        })
        .expect("resolved book");
        let market = market();
        let candidate = project_candidate(
            &market,
            &context(&market),
            Some(&book),
            as_of,
            DomainAvailability::NotMapped,
        )
        .expect("candidate");

        // Catalog liquidity/volume stay absent; book depth remains a distinct
        // microstructure fact and never impersonates historical metadata.
        assert_eq!(candidate.liquidity_usd, None);
        assert_eq!(candidate.depth_usd, Some(book.visible_liquidity_usd()));
        assert_eq!(candidate.volume_24h_usd, None);
        assert_eq!(
            candidate.market_data_health,
            MarketDataHealth::NotApplicable
        );
        assert_eq!(candidate.ingest_lag_ms, None);
        assert_eq!(candidate.book_age_ms, Some(5_000));
        assert_eq!(candidate.decision_at, as_of);
        assert_eq!(candidate.crossed, Some(false));
        assert_eq!(candidate.empty, Some(false));
        assert_eq!(candidate.category, MarketCategory::Sports);
        // The projector never computes availability itself — it trusts whatever
        // the caller's `DomainAvailabilitySource` resolved (see `select_at`).
        assert_eq!(candidate.domain_availability, DomainAvailability::NotMapped);
    }

    #[test]
    fn missing_domain_batch_entry_is_not_reclassified_as_not_mapped() {
        let availability = HashMap::new();
        assert!(availability_for(&availability, &MarketId::new("missing")).is_err());
    }

    #[test]
    fn missing_book_projects_empty_fail_closed() {
        let as_of = Utc.timestamp_millis_opt(2_000_000).single().expect("ts");
        let market = market();
        let candidate = project_candidate(
            &market,
            &context(&market),
            None,
            as_of,
            DomainAvailability::Unresolved,
        )
        .expect("candidate");
        assert_eq!(
            candidate.empty, None,
            "no book has no fabricated empty flag"
        );
        assert_eq!(candidate.liquidity_usd, None);
        assert_eq!(candidate.best_bid, None);
        // Passed straight through, independent of the book/liquidity projection.
        assert_eq!(
            candidate.domain_availability,
            DomainAvailability::Unresolved
        );
    }

    // ── select_at(): domain availability drives ModelEligibilityFilter ─────

    fn crypto_market(market_id: &str, end_date: DateTime<Utc>) -> MarketRegistryInfo {
        let mut info = market();
        info.market_id = MarketId::new(market_id);
        info.categories = CategorySet::from(MarketCategory::Crypto);
        info.end_date = Some(end_date);
        info.liquidity_usd = Some(Usd::new(dec!(1000)));
        info.volume_24h = Some(Usd::new(dec!(1000)));
        info
    }

    /// A `PointInTimeSnapshotSource` returning a fixed, healthy two-sided book and no
    /// market-context override (so the candidate falls back to the
    /// `MarketInfo`'s own `status`/`end_date`, mirroring a market with no
    /// resolution event yet).
    struct FixedBookPitEngine {
        book: BookSnapshotAt,
        snapshot: ResolvedMarketSnapshot,
    }

    #[async_trait]
    impl PointInTimeSnapshotSource for FixedBookPitEngine {
        async fn book_at_boundary(
            &self,
            _token_id: &TokenId,
            _boundary: &DecisionBoundary,
        ) -> QuantResult<Option<BookSnapshotAt>> {
            Ok(Some(self.book.clone()))
        }

        async fn market_snapshot_at(
            &self,
            _market_id: &MarketId,
            _boundary: &DecisionBoundary,
        ) -> QuantResult<Option<ResolvedMarketSnapshot>> {
            Ok(Some(self.snapshot.clone()))
        }
    }

    fn snapshot(market: MarketRegistryInfo, boundary: &DecisionBoundary) -> ResolvedMarketSnapshot {
        let event = EventRegistryInfo {
            event_id: market.event_id.clone(),
            title: "event".to_owned(),
            slug: "event".to_owned(),
            series_slug: None,
            status: EventStatus::Active,
            market_ids: vec![market.market_id.clone()],
            categories: market.categories,
            tags: Vec::new(),
            neg_risk: false,
            end_date: market.end_date,
            created_at: market.created_at.expect("created at"),
            updated_at: market.updated_at,
        };
        ResolvedMarketSnapshot {
            boundary: boundary.clone(),
            context: context(&market),
            market: Arc::new(market),
            event: Arc::new(event),
            neg_risk_leg_set: NegRiskLegSet::empty(),
            catalog_sync_batch_id: CatalogSyncBatchId::from_v7(),
            market_catalog_version_id: MarketCatalogVersionId::from_v7(),
            event_catalog_version_id: EventCatalogVersionId::from_v7(),
            market_content_hash: ContentHash::parse(format!("blake3:{}", "a".repeat(64)))
                .expect("hash"),
            event_content_hash: ContentHash::parse(format!("blake3:{}", "b".repeat(64)))
                .expect("hash"),
            membership_hash: ContentHash::parse(format!("blake3:{}", "c".repeat(64)))
                .expect("hash"),
            market_timestamp_quality: "source".to_owned(),
            event_timestamp_quality: "source".to_owned(),
            market_effective_at: boundary.cutoff_for(DecisionSource::Catalog),
            market_available_at: boundary.decision_at(),
            event_effective_at: boundary.cutoff_for(DecisionSource::Catalog),
            event_available_at: boundary.decision_at(),
        }
    }

    fn healthy_book(token: &str, as_of: DateTime<Utc>) -> BookSnapshotAt {
        BookSnapshotAt {
            token_id: TokenId::new(token),
            source_cutoff: as_of,
            decision_at: as_of,
            bids: Arc::from([level("0.48", 100)]),
            asks: Arc::from([level("0.52", 100)]),
            timestamp_ms: u64::try_from(as_of.timestamp_millis()).unwrap_or(0),
            version: 1,
            sequence: 1,
            source_event: None,
            available_at: as_of,
        }
    }

    fn instrument() -> DomainInstrumentKey {
        DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            KlineInterval::OneMinute,
        )
    }

    fn resolved_linkage(market_id: &str, effective_at: DateTime<Utc>) -> MarketLinkage {
        let market_id = MarketId::new(market_id);
        let now = effective_at;
        let outcome = LinkageOutcome::Resolved(Box::new(ResolvedBinding {
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
            source_bindings: vec![ResolvedSourceBinding {
                role: LinkageSourceRole::Feature,
                source_id: DomainSourceId::binance(),
                instrument_key: instrument(),
                available_at: now,
                binding_hash: ContentHash::parse(format!("blake3:{}", "d".repeat(64)))
                    .expect("binding hash"),
            }],
            grounding: GroundingProof { spans: Vec::new() },
            override_context: None,
        }));
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
            effective_at,
            available_at: effective_at + ChronoDuration::milliseconds(1),
        }
    }

    fn empty_prefetched() -> Prefetched {
        Prefetched {
            books: HashMap::new(),
            micro: HashMap::new(),
            trade_tape: HashMap::new(),
            resolutions: HashMap::new(),
            catalog: CatalogWindowInfo {
                market_versions: Vec::new(),
                event_versions: Vec::new(),
            },
            domain_observations: HashMap::new(),
            crypto_reports: HashMap::new(),
            weather_observations: HashMap::new(),
            weather_forecasts: HashMap::new(),
            weather_calibrations: Vec::new(),
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
                available_at: Some(as_of - ChronoDuration::seconds(9)),
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
            RuntimeConfigVersionId::from_v7(),
            domain_config.crypto.availability_lag_secs,
            crypto_model_requirements(),
        );
        let boundary = DecisionClock::new(domain_config.crypto.availability_lag_secs)
            .boundary(as_of)
            .expect("boundary");
        let pit = FixedBookPitEngine {
            book: healthy_book("yes", as_of),
            snapshot: snapshot(market.clone(), &boundary),
        };

        let result = selector
            .select_at(
                &boundary,
                slice::from_ref(&market.market_id),
                &pit,
                &domain_source,
            )
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
            RuntimeConfigVersionId::from_v7(),
            domain_config.crypto.availability_lag_secs,
            crypto_model_requirements(),
        );
        let boundary = DecisionClock::new(domain_config.crypto.availability_lag_secs)
            .boundary(as_of)
            .expect("boundary");
        let pit = FixedBookPitEngine {
            book: healthy_book("yes", as_of),
            snapshot: snapshot(market.clone(), &boundary),
        };

        let result = selector
            .select_at(
                &boundary,
                slice::from_ref(&market.market_id),
                &pit,
                &domain_source,
            )
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
