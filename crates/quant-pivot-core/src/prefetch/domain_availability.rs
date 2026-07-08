//! Shared domain-plane availability projector (Phase 11.2.2 §3.8).
//!
//! One canonical decision — mapped ∧ enabled ∧ `Resolved` linkage ∧ a visible
//! PIT `Close` observation at the source-delayed cutoff — computed
//! identically whether the caller is the live report pipeline
//! ([`crate::prefetch::market_candidates::MarketCandidateProvider`], reading
//! the linkage ledger + fact store directly) or the offline replay
//! (`OfflinePitSelector`, either reading the batch-prefetched linkage /
//! observation window during a real dataset build, or issuing bounded live
//! reads during the dry-run keep-rate estimate).
//!
//! [`resolve_domain_availability`] is the DB-backed batch projector shared by
//! the live pipeline and the offline keep-rate estimator (both need a
//! point-in-time-correct read against the live repositories, just at
//! different `as_of` instants — "now" online, historical offline).
//! [`PrefetchedDomainAvailabilitySource`] is the zero-I/O offline
//! build-time counterpart, backed by a prefetched window, so the historical
//! spine issues no DB queries per cross-section.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{DomainAvailability, LinkageOutcome, MarketInfo},
    enums::{
        common::MarketCategory,
        domain::{DomainFamily, DomainMetric, LinkageStatus},
    },
    runtime_config::DomainConfig,
    types::{DomainInstrumentKey, MarketId},
};
use quant_pivot_repository::traits::{MarketLinkageRepository, QuantFactReadRepository};
use quant_pivot_research::domain::domain_availability_at;

use crate::prefetch::historical_window::Prefetched;

/// Resolves per-market domain-plane availability for one selection round.
///
/// `as_of` is "now" for the live pipeline, historical for the offline
/// dataset build / keep-rate estimator. Implementations must never silently
/// drop a mapped market from the returned map; a market with no evidence yet
/// is `Unresolved`, not absent.
#[async_trait]
pub trait DomainAvailabilitySource: Send + Sync {
    /// Resolve availability for every entry in `markets` as of `as_of`.
    async fn resolve(
        &self,
        as_of: DateTime<Utc>,
        markets: &[&MarketInfo],
    ) -> QuantResult<HashMap<MarketId, DomainAvailability>>;
}

/// Batched linkage-ledger + domain-fact read.
///
/// Shared by the live report pipeline
/// ([`MarketCandidateProvider`](crate::prefetch::market_candidates::MarketCandidateProvider))
/// and the offline dry-run keep-rate estimator
/// (`TrainingDatasetService::estimate_keep_rate`) — both need a
/// point-in-time-correct read against the live repositories, just at
/// different `as_of` instants (mapped ∧ enabled ∧ `Resolved` linkage ∧ the
/// linked instrument has a visible PIT observation at `as_of`, Phase 11.2.2
/// §3.8).
///
/// # Errors
///
/// Propagates linkage-ledger / domain-fact read failures (the domain plane
/// fails closed as a whole rather than serving guessed availability).
pub async fn resolve_domain_availability(
    linkage_repo: &dyn MarketLinkageRepository,
    fact_read: &dyn QuantFactReadRepository,
    domain: &DomainConfig,
    as_of: DateTime<Utc>,
    markets: &[(MarketId, MarketCategory)],
) -> QuantResult<HashMap<MarketId, DomainAvailability>> {
    let mapped: Vec<MarketId> = markets
        .iter()
        .filter(|(_, category)| {
            DomainFamily::for_category(*category)
                .is_some_and(|family| domain.family_enabled(family))
        })
        .map(|(market_id, _)| market_id.clone())
        .collect();
    if mapped.is_empty() {
        return Ok(HashMap::new());
    }

    // PIT-correct: the record that was actually valid at `as_of`, never the
    // ledger's latest-ever row for the market — a metadata revision resolved
    // *after* `as_of` must never leak into this decision (`latest_for_markets`
    // is reserved for resolver idempotence only).
    let valid_at = linkage_repo
        .valid_at_for_markets(&mapped, as_of)
        .await
        .map_err(QuantError::from)?;
    let mut by_market: HashMap<MarketId, DomainAvailability> = mapped
        .iter()
        .map(|market_id| (market_id.clone(), DomainAvailability::Unresolved))
        .collect();

    // Resolve source presence once per distinct instrument.
    let mut instrument_by_market: HashMap<MarketId, DomainInstrumentKey> = HashMap::new();
    let mut instruments: HashSet<DomainInstrumentKey> = HashSet::new();
    for info in valid_at {
        if info.status == LinkageStatus::Unresolved {
            continue;
        }
        let market_id = info.market_id.clone();
        let outcome: LinkageOutcome = serde_json::from_value(info.outcome).map_err(|error| {
            QuantError::config(format!(
                "linkage ledger row for market {market_id} has an undecodable outcome \
                 payload: {error}"
            ))
        })?;
        if let LinkageOutcome::Resolved(binding) = outcome {
            instruments.insert(binding.instrument_key.clone());
            instrument_by_market.insert(market_id, binding.instrument_key);
        }
    }

    let cutoff_ms = (as_of
        - ChronoDuration::seconds(i64::try_from(domain.crypto.source_delay_secs).unwrap_or(0)))
    .timestamp_millis();
    let mut instrument_has_data: HashMap<DomainInstrumentKey, bool> = HashMap::new();
    for instrument in instruments {
        let observation = fact_read
            .domain_observation_at(&instrument, DomainMetric::Close.as_str(), cutoff_ms)
            .await
            .map_err(QuantError::from)?;
        instrument_has_data.insert(instrument, observation.is_some());
    }

    for (market_id, instrument) in instrument_by_market {
        let availability = if instrument_has_data
            .get(&instrument)
            .copied()
            .unwrap_or(false)
        {
            DomainAvailability::Available
        } else {
            DomainAvailability::SourceEmpty
        };
        by_market.insert(market_id, availability);
    }
    Ok(by_market)
}

/// Live-repository [`DomainAvailabilitySource`] backend.
///
/// Used by the offline keep-rate dry-run estimator (bounded `slices ×
/// markets` trials — a live read per trial round is acceptable and mirrors
/// the existing per-market `PitQueryEngine` reads that estimator already
/// performs). The historical spine build itself never takes this path — see
/// [`PrefetchedDomainAvailabilitySource`].
pub struct LiveDomainAvailabilitySource {
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    fact_read: Arc<dyn QuantFactReadRepository>,
    domain: DomainConfig,
}

impl LiveDomainAvailabilitySource {
    /// Wire the source from the linkage ledger, fact reader, and frozen
    /// domain-plane config.
    #[must_use]
    pub const fn new(
        linkage_repo: Arc<dyn MarketLinkageRepository>,
        fact_read: Arc<dyn QuantFactReadRepository>,
        domain: DomainConfig,
    ) -> Self {
        Self {
            linkage_repo,
            fact_read,
            domain,
        }
    }
}

#[async_trait]
impl DomainAvailabilitySource for LiveDomainAvailabilitySource {
    async fn resolve(
        &self,
        as_of: DateTime<Utc>,
        markets: &[&MarketInfo],
    ) -> QuantResult<HashMap<MarketId, DomainAvailability>> {
        let pairs: Vec<(MarketId, MarketCategory)> = markets
            .iter()
            .map(|info| (info.market_id.clone(), info.fee_category()))
            .collect();
        resolve_domain_availability(
            self.linkage_repo.as_ref(),
            self.fact_read.as_ref(),
            &self.domain,
            as_of,
            &pairs,
        )
        .await
    }
}

/// Zero-I/O [`DomainAvailabilitySource`] backend for the historical dataset build.
///
/// Backed by the batch-prefetched linkage + domain-observation window
/// ([`Prefetched`]) — the build loop issues no DB queries per cross-section.
pub struct PrefetchedDomainAvailabilitySource<'a> {
    prefetched: &'a Prefetched,
    domain: &'a DomainConfig,
}

impl<'a> PrefetchedDomainAvailabilitySource<'a> {
    /// Wire the source from the build's prefetched window and frozen
    /// domain-plane config.
    #[must_use]
    pub const fn new(prefetched: &'a Prefetched, domain: &'a DomainConfig) -> Self {
        Self { prefetched, domain }
    }
}

#[async_trait]
impl DomainAvailabilitySource for PrefetchedDomainAvailabilitySource<'_> {
    async fn resolve(
        &self,
        as_of: DateTime<Utc>,
        markets: &[&MarketInfo],
    ) -> QuantResult<HashMap<MarketId, DomainAvailability>> {
        Ok(markets
            .iter()
            .map(|info| {
                let linkages = self
                    .prefetched
                    .linkages
                    .get(&info.market_id)
                    .map_or(&[][..], Vec::as_slice);
                let availability = domain_availability_at(
                    info.fee_category(),
                    linkages,
                    as_of,
                    self.domain,
                    &self.prefetched.domain_observations,
                );
                (info.market_id.clone(), availability)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DomainAvailabilitySource, LiveDomainAvailabilitySource, PrefetchedDomainAvailabilitySource,
        resolve_domain_availability,
    };
    use crate::prefetch::historical_window::Prefetched;
    use async_trait::async_trait;
    use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
    use quant_pivot_error::storage::StorageError;
    use quant_pivot_models::{
        clickhouse::{
            BookMicrostructureRow, BookSnapshotRow, ChSchemaVersion, DomainObservationRow,
            MarketResolutionRow, MidPriceBucketRow, TickEventRow, TradeTapeRow,
        },
        domain::{
            CryptoSubject, DomainAvailability, DomainObservation, GroundingProof, LinkageOutcome,
            MarketInfo, MarketLinkage, MarketLinkageInfo, MarketLinkageListQuery, MarketSubject,
            NewMarketLinkage, Paginated, PriceComparator, ResolutionOracle, ResolvedBinding,
        },
        enums::{
            common::{MarketCategory, TickSize},
            domain::{DomainFamily, DomainMetric, KlineInterval, LinkageStatus, ResolverTier},
            market::MarketStatus,
        },
        runtime_config::DomainConfig,
        types::{
            BinanceSymbol, ContentHash, CryptoAsset, CryptoQuote, DomainInstrumentKey,
            DomainSourceId, EventId, MarketId, MarketLinkageId, Probability, ResolverVersion,
            TokenId,
        },
    };
    use quant_pivot_repository::traits::{MarketLinkageRepository, QuantFactReadRepository};
    use rust_decimal_macros::dec;
    use std::{collections::HashMap, sync::Arc};

    fn instrument_for(symbol: &str) -> DomainInstrumentKey {
        DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse(symbol).expect("symbol"),
            KlineInterval::OneMinute,
        )
    }

    fn instrument() -> DomainInstrumentKey {
        instrument_for("BTCUSDT")
    }

    fn market_info(market_id: &str, category: MarketCategory) -> MarketInfo {
        let now = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        MarketInfo {
            market_id: MarketId::new(market_id),
            event_id: EventId::new("evt"),
            question: "q".to_owned(),
            slug: "s".to_owned(),
            description: None,
            categories: vec![category],
            status: MarketStatus::Active,
            outcome: None,
            yes_token_id: TokenId::new("yes"),
            no_token_id: TokenId::new("no"),
            tick_size: TickSize::Hundredth,
            neg_risk: false,
            end_date: None,
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

    fn binding_for(symbol: &str) -> ResolvedBinding {
        let now = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        ResolvedBinding {
            subject: MarketSubject::Crypto(CryptoSubject {
                asset: CryptoAsset::parse("BTC").expect("asset"),
                quote: CryptoQuote::parse("USD").expect("quote"),
                comparator: PriceComparator::UpVsReference,
                strike: None,
                reference_at: Some(now - ChronoDuration::minutes(5)),
                observation_at: now,
                resolution_oracle: ResolutionOracle::BinanceKline {
                    symbol: BinanceSymbol::parse(symbol).expect("symbol"),
                    interval: KlineInterval::OneMinute,
                },
            }),
            instrument_key: instrument_for(symbol),
            grounding: GroundingProof { spans: Vec::new() },
            override_context: None,
        }
    }

    fn binding() -> ResolvedBinding {
        binding_for("BTCUSDT")
    }

    fn linkage(market_id: &str, outcome: LinkageOutcome) -> MarketLinkage {
        let market_id = MarketId::new(market_id);
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
        let derived_at = Utc.with_ymd_and_hms(2026, 7, 1, 11, 0, 0).unwrap();
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

    // ── PrefetchedDomainAvailabilitySource (zero I/O) ───────────────────────

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

    #[tokio::test]
    async fn prefetched_source_resolves_from_in_memory_linkage_and_observations() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let domain = DomainConfig::default();
        let mut prefetched = empty_prefetched();
        prefetched.linkages.insert(
            MarketId::new("resolved-available"),
            vec![linkage(
                "resolved-available",
                LinkageOutcome::Resolved(binding()),
            )],
        );
        prefetched.linkages.insert(
            MarketId::new("resolved-empty"),
            vec![linkage(
                "resolved-empty",
                LinkageOutcome::Resolved(binding_for("ETHUSDT")),
            )],
        );
        prefetched.linkages.insert(
            MarketId::new("unresolved"),
            vec![linkage(
                "unresolved",
                LinkageOutcome::Unresolved {
                    reason: "no template".to_owned(),
                },
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
                observed_at: as_of - ChronoDuration::seconds(5),
                publish_time: as_of - ChronoDuration::seconds(5),
            }],
        );

        let markets = [
            market_info("resolved-available", MarketCategory::Crypto),
            market_info("resolved-empty", MarketCategory::Crypto),
            market_info("unresolved", MarketCategory::Crypto),
            market_info("sports", MarketCategory::Sports),
        ];
        let refs: Vec<&MarketInfo> = markets.iter().collect();

        let source = PrefetchedDomainAvailabilitySource::new(&prefetched, &domain);
        let result = source.resolve(as_of, &refs).await.expect("resolve");

        assert_eq!(
            result.get(&MarketId::new("resolved-available")).copied(),
            Some(DomainAvailability::Available)
        );
        assert_eq!(
            result.get(&MarketId::new("resolved-empty")).copied(),
            Some(DomainAvailability::SourceEmpty)
        );
        assert_eq!(
            result.get(&MarketId::new("unresolved")).copied(),
            Some(DomainAvailability::Unresolved)
        );
        assert_eq!(
            result.get(&MarketId::new("sports")).copied(),
            Some(DomainAvailability::NotMapped)
        );
    }

    // ── LiveDomainAvailabilitySource / resolve_domain_availability (DB) ─────

    struct FakeLinkageRepo {
        rows: Vec<MarketLinkageInfo>,
    }

    #[async_trait]
    impl MarketLinkageRepository for FakeLinkageRepo {
        async fn append(
            &self,
            _linkage: NewMarketLinkage,
        ) -> Result<MarketLinkageInfo, StorageError> {
            unimplemented!("read-only fake")
        }

        async fn valid_at(
            &self,
            _market_id: &MarketId,
            _as_of: DateTime<Utc>,
        ) -> Result<Option<MarketLinkageInfo>, StorageError> {
            unimplemented!("unused by this test")
        }

        async fn valid_at_for_markets(
            &self,
            market_ids: &[MarketId],
            _as_of: DateTime<Utc>,
        ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
            Ok(self
                .rows
                .iter()
                .filter(|row| market_ids.contains(&row.market_id))
                .cloned()
                .collect())
        }

        async fn latest_for_markets(
            &self,
            _market_ids: &[MarketId],
        ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
            unimplemented!("unused by this test")
        }

        async fn ledger_for_markets(
            &self,
            _market_ids: &[MarketId],
            _derived_before: DateTime<Utc>,
        ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
            unimplemented!("unused by this test")
        }

        async fn find_by_id(
            &self,
            _linkage_id: &MarketLinkageId,
        ) -> Result<Option<MarketLinkageInfo>, StorageError> {
            unimplemented!("unused by this test")
        }

        async fn page(
            &self,
            query: MarketLinkageListQuery,
        ) -> Result<Paginated<MarketLinkageInfo>, StorageError> {
            Ok(Paginated::empty_for(&query))
        }
    }

    struct FakeFactRead {
        has_observation: bool,
    }

    #[async_trait]
    impl QuantFactReadRepository for FakeFactRead {
        async fn microstructure_window(
            &self,
            _token_ids: Vec<TokenId>,
            _from_ms: i64,
            _to_ms: i64,
        ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn microstructure_series(
            &self,
            _token_ids: Vec<TokenId>,
            _from_ms: i64,
            _to_ms: i64,
            _minute: bool,
        ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn last_trades(
            &self,
            _token_ids: Vec<TokenId>,
            _from_ms: i64,
            _to_ms: i64,
            _limit: u64,
        ) -> Result<Vec<TickEventRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn trade_tape_window_by_market(
            &self,
            _market_ids: Vec<MarketId>,
            _from_ms: i64,
            _to_ms: i64,
        ) -> Result<Vec<TradeTapeRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn mid_price_series(
            &self,
            _token_ids: Vec<TokenId>,
            _from_ms: i64,
            _to_ms: i64,
            _bucket_secs: u32,
        ) -> Result<Vec<MidPriceBucketRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn book_snapshot_at(
            &self,
            _token_id: &TokenId,
            _as_of_ms: i64,
        ) -> Result<Option<BookSnapshotRow>, StorageError> {
            Ok(None)
        }

        async fn book_snapshots_between(
            &self,
            _token_ids: Vec<TokenId>,
            _from_ms: i64,
            _to_ms: i64,
        ) -> Result<Vec<BookSnapshotRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn resolution_at(
            &self,
            _market_id: &MarketId,
            _as_of_ms: i64,
        ) -> Result<Option<MarketResolutionRow>, StorageError> {
            Ok(None)
        }

        async fn resolutions_between(
            &self,
            _market_ids: Vec<MarketId>,
            _from_ms: i64,
            _to_ms: i64,
        ) -> Result<Vec<MarketResolutionRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn observed_markets_between(
            &self,
            _from_ms: i64,
            _to_ms: i64,
        ) -> Result<Vec<MarketId>, StorageError> {
            Ok(Vec::new())
        }

        async fn domain_observations_between(
            &self,
            _instrument_keys: Vec<DomainInstrumentKey>,
            _from_ms: i64,
            _to_ms: i64,
        ) -> Result<Vec<DomainObservationRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn domain_observation_at(
            &self,
            _instrument_key: &DomainInstrumentKey,
            _metric: &str,
            _as_of_ms: i64,
        ) -> Result<Option<DomainObservationRow>, StorageError> {
            if self.has_observation {
                Ok(Some(DomainObservationRow {
                    family: DomainFamily::Crypto.as_str().to_owned(),
                    source_id: DomainSourceId::binance(),
                    instrument_key: instrument(),
                    metric: DomainMetric::Close.as_str().to_owned(),
                    value: dec!(100000).into(),
                    event_time: 0,
                    publish_time: 0,
                    ingestion_time: 0,
                    schema_version: ChSchemaVersion::FIRST,
                }))
            } else {
                Ok(None)
            }
        }
    }

    fn linkage_row(
        market_id: &str,
        status: LinkageStatus,
        outcome: LinkageOutcome,
    ) -> MarketLinkageInfo {
        let linkage = linkage(market_id, outcome);
        MarketLinkageInfo {
            linkage_id: linkage.linkage_id,
            market_id: linkage.market_id,
            domain_family: linkage.domain_family,
            status,
            resolver_tier: linkage.resolver_tier,
            resolver_version: linkage.resolver_version,
            confidence: linkage.confidence,
            outcome: serde_json::to_value(&linkage.outcome).expect("serialize outcome"),
            instrument_key: Some(instrument()),
            metadata_hash: linkage.metadata_hash,
            content_hash: linkage.content_hash,
            derived_at: linkage.derived_at,
            override_reason: None,
            override_actor: None,
            created_at: linkage.created_at,
        }
    }

    #[tokio::test]
    async fn live_source_batches_the_ledger_and_fact_reads() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let domain = DomainConfig::default();
        let repo = FakeLinkageRepo {
            rows: vec![linkage_row(
                "0xcrypto",
                LinkageStatus::Resolved,
                LinkageOutcome::Resolved(binding()),
            )],
        };
        let fact_read = FakeFactRead {
            has_observation: true,
        };
        let source = LiveDomainAvailabilitySource::new(Arc::new(repo), Arc::new(fact_read), domain);
        let market = market_info("0xcrypto", MarketCategory::Crypto);
        let result = source.resolve(as_of, &[&market]).await.expect("resolve");
        assert_eq!(
            result.get(&MarketId::new("0xcrypto")).copied(),
            Some(DomainAvailability::Available)
        );
    }

    #[tokio::test]
    async fn resolve_domain_availability_defaults_absent_markets_to_unresolved() {
        let as_of = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let domain = DomainConfig::default();
        let repo = FakeLinkageRepo { rows: Vec::new() };
        let fact_read = FakeFactRead {
            has_observation: false,
        };
        let result = resolve_domain_availability(
            &repo,
            &fact_read,
            &domain,
            as_of,
            &[(MarketId::new("0xnoledger"), MarketCategory::Crypto)],
        )
        .await
        .expect("resolve");
        assert_eq!(
            result.get(&MarketId::new("0xnoledger")).copied(),
            Some(DomainAvailability::Unresolved)
        );
    }
}
