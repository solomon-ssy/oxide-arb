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
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        DecisionBoundary, DecisionSource, DomainAvailability, LinkageOutcome, MarketLinkageInfo,
        MarketSubject, ResolvedBinding, WeatherForecastPoint, WeatherObservationFact,
        WeatherObservationReportKind, WeatherSubject,
    },
    enums::{
        common::MarketCategory,
        domain::{DomainFamily, DomainMetric, LinkageSourceRole, LinkageStatus},
    },
    runtime_config::DomainConfig,
    types::{DomainInstrumentKey, DomainSourceId, IcaoStation, MarketId},
};
use quant_pivot_repository::traits::{MarketLinkageRepository, QuantFactReadRepository};
use quant_pivot_research::domain::{
    DomainAvailabilityFacts, domain_availability_at, source_binding,
};

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
        boundary: &DecisionBoundary,
        markets: &[(MarketId, MarketCategory)],
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
    boundary: &DecisionBoundary,
    markets: &[(MarketId, MarketCategory)],
) -> QuantResult<HashMap<MarketId, DomainAvailability>> {
    let source_cutoff = boundary.cutoff_for(DecisionSource::DomainCrypto);
    let decision_at = boundary.decision_at();
    let mut by_market: HashMap<MarketId, DomainAvailability> = markets
        .iter()
        .map(|(market_id, _)| (market_id.clone(), DomainAvailability::NotMapped))
        .collect();
    let mapped: Vec<MarketId> = markets
        .iter()
        .filter(|(_, category)| {
            DomainFamily::for_category(*category)
                .is_some_and(|family| domain.family_enabled(family))
        })
        .map(|(market_id, _)| market_id.clone())
        .collect();
    if mapped.is_empty() {
        return Ok(by_market);
    }

    // PIT-correct: the record that was actually valid at `as_of`, never the
    // ledger's latest-ever row for the market — a metadata revision resolved
    // *after* `as_of` must never leak into this decision (`latest_for_markets`
    // is reserved for resolver idempotence only).
    let valid_at = linkage_repo
        .valid_at_for_markets(&mapped, boundary)
        .await
        .map_err(QuantError::from)?;
    for market_id in &mapped {
        by_market.insert(market_id.clone(), DomainAvailability::Unresolved);
    }

    let resolved = resolve_availability_bindings(valid_at)?;
    let instrument_has_data =
        load_crypto_availability(fact_read, &resolved.instruments, source_cutoff, decision_at)
            .await?;
    let weather = load_weather_availability(fact_read, boundary, &resolved).await?;
    project_availability(
        &mut by_market,
        resolved.binding_by_market,
        &instrument_has_data,
        &weather,
    )?;
    Ok(by_market)
}

enum AvailabilityBinding {
    Crypto(DomainInstrumentKey),
    Weather {
        station: IcaoStation,
        local_date: chrono::NaiveDate,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
}

struct AvailabilityResolution {
    binding_by_market: HashMap<MarketId, AvailabilityBinding>,
    instruments: HashSet<DomainInstrumentKey>,
    weather_stations: HashSet<IcaoStation>,
    weather_from: Option<DateTime<Utc>>,
    weather_to: Option<DateTime<Utc>>,
}

struct WeatherAvailability {
    observations: HashMap<IcaoStation, Vec<WeatherObservationFact>>,
    forecasts: HashMap<IcaoStation, Vec<WeatherForecastPoint>>,
}

fn resolve_availability_bindings(
    rows: Vec<MarketLinkageInfo>,
) -> QuantResult<AvailabilityResolution> {
    let mut resolved = AvailabilityResolution {
        binding_by_market: HashMap::new(),
        instruments: HashSet::new(),
        weather_stations: HashSet::new(),
        weather_from: None,
        weather_to: None,
    };
    for info in rows {
        if info.status == LinkageStatus::Unresolved {
            continue;
        }
        let market_id = info.market_id.clone();
        let outcome: LinkageOutcome = serde_json::from_value(info.outcome).map_err(|error| {
            QuantError::config(format!(
                "linkage ledger row for market {market_id} has an undecodable outcome: {error}"
            ))
        })?;
        let LinkageOutcome::Resolved(binding) = outcome else {
            continue;
        };
        match &binding.subject {
            MarketSubject::Crypto(_) => {
                let Some(feature) = source_binding(&binding, LinkageSourceRole::Feature) else {
                    continue;
                };
                resolved.instruments.insert(feature.instrument_key.clone());
                resolved.binding_by_market.insert(
                    market_id,
                    AvailabilityBinding::Crypto(feature.instrument_key.clone()),
                );
            }
            MarketSubject::Weather(subject) => {
                let Some(binding) = weather_availability_binding(&binding, subject)? else {
                    continue;
                };
                let AvailabilityBinding::Weather {
                    ref station,
                    from,
                    to,
                    ..
                } = binding
                else {
                    continue;
                };
                resolved.weather_from = Some(
                    resolved
                        .weather_from
                        .map_or(from, |current| current.min(from)),
                );
                resolved.weather_to =
                    Some(resolved.weather_to.map_or(to, |current| current.max(to)));
                resolved.weather_stations.insert(station.clone());
                resolved.binding_by_market.insert(market_id, binding);
            }
        }
    }
    Ok(resolved)
}

fn weather_availability_binding(
    binding: &ResolvedBinding,
    subject: &WeatherSubject,
) -> QuantResult<Option<AvailabilityBinding>> {
    let Some(live) = source_binding(binding, LinkageSourceRole::LiveEvent) else {
        return Ok(None);
    };
    let Some(forecast) = source_binding(binding, LinkageSourceRole::Forecast) else {
        return Ok(None);
    };
    let Some(calibration) = source_binding(binding, LinkageSourceRole::HistoricalCalibration)
    else {
        return Ok(None);
    };
    let sources_match = live.source_id == DomainSourceId::aviation_weather()
        && live.instrument_key
            == DomainInstrumentKey::aviation_weather(&subject.decision_group.station)
        && forecast.source_id == DomainSourceId::gefs()
        && forecast.instrument_key == DomainInstrumentKey::gefs(&subject.decision_group.station)
        && calibration.source_id == DomainSourceId::ghcnh()
        && calibration.instrument_key
            == DomainInstrumentKey::ghcnh(&subject.decision_group.station);
    if !sources_match {
        return Ok(None);
    }
    let (from, to) = weather_day_bounds(subject)?;
    Ok(Some(AvailabilityBinding::Weather {
        station: subject.decision_group.station.clone(),
        local_date: subject.decision_group.local_date,
        from,
        to,
    }))
}

async fn load_crypto_availability(
    fact_read: &dyn QuantFactReadRepository,
    instruments: &HashSet<DomainInstrumentKey>,
    source_cutoff: DateTime<Utc>,
    decision_at: DateTime<Utc>,
) -> QuantResult<HashMap<DomainInstrumentKey, bool>> {
    let mut available = HashMap::new();
    for instrument in instruments {
        let observation = fact_read
            .domain_observation_at(
                instrument,
                DomainMetric::Close.as_str(),
                source_cutoff.timestamp_millis(),
                decision_at.timestamp_millis(),
            )
            .await
            .map_err(QuantError::from)?;
        available.insert(instrument.clone(), observation.is_some());
    }
    Ok(available)
}

async fn load_weather_availability(
    fact_read: &dyn QuantFactReadRepository,
    boundary: &DecisionBoundary,
    resolution: &AvailabilityResolution,
) -> QuantResult<WeatherAvailability> {
    let mut available = WeatherAvailability {
        observations: HashMap::new(),
        forecasts: HashMap::new(),
    };
    let (Some(from), Some(to)) = (resolution.weather_from, resolution.weather_to) else {
        return Ok(available);
    };
    let stations = resolution
        .weather_stations
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let source_cutoff = boundary
        .cutoff_for(DecisionSource::DomainWeather)
        .timestamp_millis();
    for row in fact_read
        .weather_observation_facts_between(
            stations.clone(),
            from.timestamp_millis(),
            to.timestamp_millis(),
            source_cutoff,
            boundary.decision_at().timestamp_millis(),
        )
        .await
        .map_err(QuantError::from)?
    {
        let fact = WeatherObservationFact::from_clickhouse_row(row).ok_or_else(|| {
            QuantError::config("invalid Weather observation in availability projection")
        })?;
        let station = fact.station().ok_or_else(|| {
            QuantError::config(format!(
                "Weather observation subject `{}` is not an ICAO station",
                fact.subject_key
            ))
        })?;
        available
            .observations
            .entry(station)
            .or_default()
            .push(fact);
    }
    for row in fact_read
        .weather_forecast_facts_between(
            stations,
            from.timestamp_millis(),
            to.timestamp_millis(),
            source_cutoff,
            boundary.decision_at().timestamp_millis(),
        )
        .await
        .map_err(QuantError::from)?
    {
        let fact = WeatherForecastPoint::from_clickhouse_row(row)
            .ok_or_else(|| QuantError::config("invalid GEFS point in availability projection"))?;
        let station = fact.station().ok_or_else(|| {
            QuantError::config(format!(
                "Weather forecast subject `{}` is not an ICAO station",
                fact.subject_key
            ))
        })?;
        available.forecasts.entry(station).or_default().push(fact);
    }
    Ok(available)
}

fn project_availability(
    by_market: &mut HashMap<MarketId, DomainAvailability>,
    bindings: HashMap<MarketId, AvailabilityBinding>,
    crypto: &HashMap<DomainInstrumentKey, bool>,
    weather: &WeatherAvailability,
) -> QuantResult<()> {
    for (market_id, binding) in bindings {
        let has_data = match binding {
            AvailabilityBinding::Crypto(instrument) => {
                crypto.get(&instrument).copied().ok_or_else(|| {
                    QuantError::config(format!(
                        "domain availability batch omitted instrument {instrument}"
                    ))
                })?
            }
            AvailabilityBinding::Weather {
                station,
                local_date,
                from,
                to,
            } => {
                let has_observation = weather.observations.get(&station).is_some_and(|facts| {
                    facts.iter().any(|fact| {
                        fact.local_date == local_date
                            && fact.report_kind != WeatherObservationReportKind::HistoricalGhcnh
                    })
                });
                let has_forecast = weather.forecasts.get(&station).is_some_and(|facts| {
                    facts
                        .iter()
                        .any(|fact| fact.valid_time >= from && fact.valid_time < to)
                });
                has_observation || has_forecast
            }
        };
        by_market.insert(
            market_id,
            if has_data {
                DomainAvailability::Available
            } else {
                DomainAvailability::SourceEmpty
            },
        );
    }
    Ok(())
}

fn weather_day_bounds(subject: &WeatherSubject) -> QuantResult<(DateTime<Utc>, DateTime<Utc>)> {
    let timezone = subject
        .decision_group
        .timezone
        .parse::<Tz>()
        .map_err(|error| {
            QuantError::config(format!("invalid Weather linkage timezone: {error}"))
        })?;
    let next_date = subject
        .decision_group
        .local_date
        .succ_opt()
        .ok_or_else(|| QuantError::config("Weather local date overflow"))?;
    let local_start = subject
        .decision_group
        .local_date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| QuantError::config("invalid Weather local day start"))?;
    let local_end = next_date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| QuantError::config("invalid Weather local day end"))?;
    let resolve = |local| {
        timezone
            .from_local_datetime(&local)
            .single()
            .map(|value| value.with_timezone(&Utc))
            .ok_or_else(|| QuantError::config("Weather local midnight is ambiguous or missing"))
    };
    Ok((resolve(local_start)?, resolve(local_end)?))
}

/// Live-repository [`DomainAvailabilitySource`] backend.
///
/// Used by the offline keep-rate dry-run estimator (bounded `slices ×
/// markets` trials — a live read per trial round is acceptable and mirrors
/// the existing per-market `PointInTimeSnapshotSource` reads that estimator already
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
        boundary: &DecisionBoundary,
        markets: &[(MarketId, MarketCategory)],
    ) -> QuantResult<HashMap<MarketId, DomainAvailability>> {
        resolve_domain_availability(
            self.linkage_repo.as_ref(),
            self.fact_read.as_ref(),
            &self.domain,
            boundary,
            markets,
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
        boundary: &DecisionBoundary,
        markets: &[(MarketId, MarketCategory)],
    ) -> QuantResult<HashMap<MarketId, DomainAvailability>> {
        Ok(markets
            .iter()
            .map(|(market_id, category)| {
                let linkages = self
                    .prefetched
                    .linkages
                    .get(market_id)
                    .map_or(&[][..], Vec::as_slice);
                let availability = domain_availability_at(
                    *category,
                    linkages,
                    boundary,
                    self.domain,
                    DomainAvailabilityFacts {
                        observations: &self.prefetched.domain_observations,
                        weather_observations: &self.prefetched.weather_observations,
                    },
                );
                (market_id.clone(), availability)
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
            BookL2CheckpointRow, BookMicrostructureRow, ChSchemaVersion, DomainObservationRow,
            MarketResolutionRow, MidPriceBucketRow, TradeTapeRow,
        },
        domain::{
            CatalogWindowInfo, CryptoSubject, DecisionBoundary, DecisionClock, DecisionSource,
            DomainAvailability, DomainObservation, GroundingProof, LinkageOutcome, MarketInfo,
            MarketLinkage, MarketLinkageInfo, MarketLinkageListQuery, MarketSubject,
            NewMarketLinkage, Paginated, PriceComparator, ResolutionOracle, ResolvedBinding,
            ResolvedSourceBinding,
        },
        enums::{
            common::{MarketCategory, TickSize},
            domain::{
                BinanceMarketSegment, DomainFamily, DomainMetric, KlineInterval, LinkageSourceRole,
                LinkageStatus, ResolverTier,
            },
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

    fn boundary(as_of: DateTime<Utc>, domain: &DomainConfig) -> DecisionBoundary {
        DecisionClock::new(0)
            .boundary(as_of)
            .expect("boundary")
            .with_source_cutoff(
                DecisionSource::DomainCrypto,
                domain.crypto.availability_lag_secs,
            )
            .expect("domain cutoff")
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
            filter_reasons: Vec::new(),
            outcome: None,
            yes_token_id: TokenId::new("yes"),
            no_token_id: TokenId::new("no"),
            tick_size: TickSize::Hundredth,
            neg_risk: false,
            start_date: None,
            end_date: None,
            resolved_at: None,
            content_hash: ContentHash::parse(format!("blake3:{}", "d".repeat(64)))
                .expect("market hash"),
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
                    market: BinanceMarketSegment::Spot,
                    symbol: BinanceSymbol::parse(symbol).expect("symbol"),
                    interval: KlineInterval::OneMinute,
                },
            }),
            source_bindings: vec![ResolvedSourceBinding {
                role: LinkageSourceRole::Feature,
                source_id: DomainSourceId::binance(),
                instrument_key: instrument_for(symbol),
                available_at: now,
                binding_hash: ContentHash::parse(format!("blake3:{}", "c".repeat(64)))
                    .expect("binding hash"),
            }],
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
        let capability_registry_hash =
            ContentHash::parse(format!("blake3:{}", "f".repeat(64))).expect("hash");
        let content_hash = MarketLinkage::compute_content_hash(
            &market_id,
            DomainFamily::Crypto,
            &outcome,
            ResolverTier::Tier0Slug,
            ResolverVersion::FIRST,
            &metadata_hash,
            &capability_registry_hash,
        )
        .expect("hash");
        let effective_at = Utc.with_ymd_and_hms(2026, 7, 1, 11, 0, 0).unwrap();
        MarketLinkage {
            linkage_id: MarketLinkageId::from_v7(),
            market_id,
            domain_family: DomainFamily::Crypto,
            outcome,
            confidence: Probability::ONE,
            resolver_tier: ResolverTier::Tier0Slug,
            resolver_version: ResolverVersion::FIRST,
            metadata_hash,
            capability_registry_hash: Some(capability_registry_hash),
            content_hash,
            effective_at,
            available_at: effective_at + ChronoDuration::milliseconds(1),
        }
    }

    // ── PrefetchedDomainAvailabilitySource (zero I/O) ───────────────────────

    fn empty_prefetched() -> Prefetched {
        Prefetched {
            books: HashMap::new(),
            micro: HashMap::new(),
            trade_tape: HashMap::new(),
            resolutions: HashMap::new(),
            catalog: CatalogWindowInfo {
                market_changes: Vec::new(),
                event_changes: Vec::new(),
            },
            domain_observations: HashMap::new(),
            crypto_reports: HashMap::new(),
            weather_observations: HashMap::new(),
            weather_forecasts: HashMap::new(),
            weather_calibrations: Vec::new(),
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
                LinkageOutcome::Resolved(Box::new(binding())),
            )],
        );
        prefetched.linkages.insert(
            MarketId::new("resolved-empty"),
            vec![linkage(
                "resolved-empty",
                LinkageOutcome::Resolved(Box::new(binding_for("ETHUSDT"))),
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
                available_at: Some(as_of - ChronoDuration::seconds(4)),
            }],
        );

        let markets = [
            market_info("resolved-available", MarketCategory::Crypto),
            market_info("resolved-empty", MarketCategory::Crypto),
            market_info("unresolved", MarketCategory::Crypto),
            market_info("sports", MarketCategory::Sports),
        ];
        let refs: Vec<_> = markets
            .iter()
            .map(|market| (market.market_id.clone(), market.primary_category()))
            .collect();

        let source = PrefetchedDomainAvailabilitySource::new(&prefetched, &domain);
        let boundary = boundary(as_of, &domain);
        let result = source.resolve(&boundary, &refs).await.expect("resolve");

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

        async fn append_batch(
            &self,
            _linkages: Vec<NewMarketLinkage>,
        ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
            unimplemented!("read-only fake")
        }

        async fn valid_at(
            &self,
            _market_id: &MarketId,
            _boundary: &DecisionBoundary,
        ) -> Result<Option<MarketLinkageInfo>, StorageError> {
            unimplemented!("unused by this test")
        }

        async fn valid_at_for_markets(
            &self,
            market_ids: &[MarketId],
            boundary: &DecisionBoundary,
        ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
            Ok(self
                .rows
                .iter()
                .filter(|row| {
                    market_ids.contains(&row.market_id)
                        && row.derived_at <= boundary.cutoff_for(DecisionSource::Linkage)
                        && row.created_at <= boundary.decision_at()
                })
                .cloned()
                .collect())
        }

        async fn latest_for_markets(
            &self,
            _market_ids: &[MarketId],
        ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
            unimplemented!("unused by this test")
        }

        async fn latest_for_active_markets(&self) -> Result<Vec<MarketLinkageInfo>, StorageError> {
            Ok(self.rows.clone())
        }

        async fn ledger_for_markets(
            &self,
            _market_ids: &[MarketId],
            _end_boundary: &DecisionBoundary,
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
            _decision_at_ms: i64,
        ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn microstructure_series(
            &self,
            _token_ids: Vec<TokenId>,
            _from_ms: i64,
            _to_ms: i64,
            _available_by_ms: i64,
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
        ) -> Result<Vec<TradeTapeRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn trade_tape_window_by_market(
            &self,
            _market_ids: Vec<MarketId>,
            _from_ms: i64,
            _to_ms: i64,
            _decision_at_ms: i64,
        ) -> Result<Vec<TradeTapeRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn mid_price_series(
            &self,
            _token_ids: Vec<TokenId>,
            _from_ms: i64,
            _to_ms: i64,
            _decision_at_ms: i64,
            _bucket_secs: u32,
        ) -> Result<Vec<MidPriceBucketRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn book_checkpoint_at(
            &self,
            _token_id: &TokenId,
            _as_of_ms: i64,
            _decision_at_ms: i64,
        ) -> Result<Option<BookL2CheckpointRow>, StorageError> {
            Ok(None)
        }

        async fn book_checkpoints_between(
            &self,
            _token_ids: Vec<TokenId>,
            _from_ms: i64,
            _to_ms: i64,
            _available_by_ms: i64,
        ) -> Result<Vec<BookL2CheckpointRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn resolution_at(
            &self,
            _market_id: &MarketId,
            _source_cutoff_ms: i64,
            _decision_at_ms: i64,
        ) -> Result<Option<MarketResolutionRow>, StorageError> {
            Ok(None)
        }

        async fn resolutions_between(
            &self,
            _market_ids: Vec<MarketId>,
            _from_ms: i64,
            _to_ms: i64,
            _decision_at_ms: i64,
        ) -> Result<Vec<MarketResolutionRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn observed_markets_between(
            &self,
            _from_ms: i64,
            _to_ms: i64,
            _decision_at_ms: i64,
        ) -> Result<Vec<MarketId>, StorageError> {
            Ok(Vec::new())
        }

        async fn domain_observations_between(
            &self,
            _instrument_keys: Vec<DomainInstrumentKey>,
            _from_ms: i64,
            _to_ms: i64,
            _publish_cutoff_ms: i64,
            _decision_at_ms: i64,
        ) -> Result<Vec<DomainObservationRow>, StorageError> {
            Ok(Vec::new())
        }

        async fn domain_observation_at(
            &self,
            _instrument_key: &DomainInstrumentKey,
            _metric: &str,
            _as_of_ms: i64,
            _decision_at_ms: i64,
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
            metadata_hash: linkage.metadata_hash,
            capability_registry_hash: linkage.capability_registry_hash,
            content_hash: linkage.content_hash,
            derived_at: linkage.effective_at,
            override_reason: None,
            override_actor: None,
            created_at: linkage.available_at,
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
                LinkageOutcome::Resolved(Box::new(binding())),
            )],
        };
        let fact_read = FakeFactRead {
            has_observation: true,
        };
        let source = LiveDomainAvailabilitySource::new(Arc::new(repo), Arc::new(fact_read), domain);
        let market = market_info("0xcrypto", MarketCategory::Crypto);
        let boundary = DecisionClock::new(0).boundary(as_of).expect("boundary");
        let result = source
            .resolve(
                &boundary,
                &[(market.market_id.clone(), market.primary_category())],
            )
            .await
            .expect("resolve");
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
        let boundary = DecisionClock::new(0).boundary(as_of).expect("boundary");
        let result = resolve_domain_availability(
            &repo,
            &fact_read,
            &domain,
            &boundary,
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
