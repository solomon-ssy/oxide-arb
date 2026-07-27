//! Shared historical-window prefetch + point-in-time materialization.
//!
//! The offline closure (training-dataset build **and** backtest replay) resolves
//! every feature/factor point-in-time from a single batch-prefetched window served
//! by an in-memory [`MaterializedPitEngine`], so the replay loop issues zero DB
//! queries and is byte-identical regardless of caller. This module owns that
//! prefetch + materialization + the PIT window/forward slicing helpers; both
//! [`TrainingDatasetService`](crate::service::training_dataset::TrainingDatasetService)
//! and [`BacktestService`](crate::service::backtest::BacktestService) consume it,
//! so their PIT semantics can never drift.
//!
//! Features are bounded by the frozen [`DecisionBoundary`] source cutoffs;
//! forward label/settlement
//! windows look strictly after `as_of`. There is **no** live `BookStore` here —
//! the offline plane is structurally barred from the live source.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use chrono_tz::Tz;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{BookL2LedgerRow, BookMicrostructureRow, MarketResolutionRow, TradeTapeRow},
    domain::{
        data_plane::{
            CryptoPriceReport, DecisionBoundary, DecisionClock, DecisionSource, DomainObservation,
            TradeTapePrint, WeatherForecastPoint, WeatherObservationFact,
        },
        market::{CatalogWindowInfo, MarketRegistryInfo},
        quant::{MarketLinkage, MarketSubject, WeatherSubject},
    },
    enums::domain::DomainFamily,
    runtime_config::DomainConfig,
    types::{
        Bps, DomainInstrumentKey, IcaoStation, MarketId, PayoutRatio, Price, TokenId, Usd,
        calibration::PublishedWeatherStationLeadBias,
    },
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, CatalogLedgerRepository, MarketLinkageRepository,
    QuantFactReadRepository,
};
use quant_pivot_research::{
    domain::{crypto_lookback_secs, oracle_instrument},
    features::{MarketWindowSnapshot, MicrostructureBucket, TradeTapeWindowSnapshot},
    pit::{BookSnapshotAt, MaterializedPitEngine},
    selection::SelectedMarket,
    training::{ForwardSample, ForwardWindow, MarketResolution as ResearchMarketResolution},
};
use rust_decimal::Decimal;

use crate::pit::platform::ch_historical::snapshot_from_row;

/// One `(market, primary token)` instant the replay will resolve point-in-time.
///
/// The token may be either catalog YES or catalog NO. Materialization validates
/// the exact pair and orients the resolved book/feature row to this token.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplaySample {
    /// Market the sample scores.
    pub market_id: MarketId,
    /// Primary outcome token for this sample.
    pub token_id: TokenId,
}

/// The window + sample set a prefetch must cover.
pub struct WindowSpec {
    /// Inclusive window start (first `as_of`).
    pub window_start: DateTime<Utc>,
    /// Exclusive window end (last `as_of` is strictly before).
    pub window_end: DateTime<Utc>,
    /// Frozen information-availability cutoff for all materialized facts.
    pub available_by: DateTime<Utc>,
    /// Distinct `(market, token)` samples to prefetch facts for.
    pub samples: Vec<ReplaySample>,
    /// Maximum trailing feature lookback.
    pub lookback: Duration,
    /// Source visibility delay applied to features.
    pub knowledge_lag: Duration,
    /// Maximum forward label horizon (forward facts are read this far past `window_end`).
    pub max_horizon_secs: u64,
    /// Frozen domain-plane configuration (drives linkage + observation prefetch).
    pub domain: DomainConfig,
}

/// Batch-prefetched historical facts for an offline replay window.
pub struct Prefetched {
    /// Book snapshots per token (ascending by observation time).
    pub books: HashMap<TokenId, Vec<BookL2LedgerRow>>,
    /// Microstructure buckets per token (trailing + forward).
    pub micro: HashMap<TokenId, Vec<BookMicrostructureRow>>,
    /// Trade-tape participant rows per market.
    pub trade_tape: HashMap<MarketId, Vec<TradeTapeRow>>,
    /// Settlement resolutions per market.
    pub resolutions: HashMap<MarketId, Vec<MarketResolutionRow>>,
    /// Every immutable market/event revision required by the replay window.
    pub catalog: CatalogWindowInfo,
    /// External domain observations per instrument, ascending.
    pub domain_observations: HashMap<DomainInstrumentKey, Vec<DomainObservation>>,
    /// Source-native Crypto reports per settlement instrument, ascending.
    pub crypto_reports: HashMap<DomainInstrumentKey, Vec<CryptoPriceReport>>,
    /// Typed AviationWeather/GHCNh facts per station.
    pub weather_observations: HashMap<IcaoStation, Vec<WeatherObservationFact>>,
    /// Raw GEFS points per station, including calibration history and target days.
    pub weather_forecasts: HashMap<IcaoStation, Vec<WeatherForecastPoint>>,
    /// Immutable Weather calibration publication ledger visible through the window end.
    pub weather_calibrations: Vec<PublishedWeatherStationLeadBias>,
    /// Frozen linkage ledger records per market covering the window (any
    /// derivation order; PIT selection happens per `as_of`).
    pub linkages: HashMap<MarketId, Vec<MarketLinkage>>,
}

/// A fully prefetched + materialized historical window, ready for zero-DB replay.
pub struct HistoricalWindow {
    /// The raw prefetched facts (forward windows, labels, settlement derive from these).
    pub prefetched: Prefetched,
    /// In-memory point-in-time engine over the prefetched books + market contexts.
    pub pit: MaterializedPitEngine,
    /// Count of book snapshot rows that failed to decode (data-quality signal).
    pub book_decode_failures: u64,
}

/// Build the zero-I/O replay engine from an already verified Source Slice.
/// This is the only Dataset/Fit/Validate entry after materialization; it cannot
/// reach `ClickHouse` or `Postgres`.
pub(crate) fn historical_window_from_prefetched(
    prefetched: Prefetched,
    max_book_staleness: Duration,
) -> QuantResult<HistoricalWindow> {
    let (pit, book_decode_failures) = build_materialized_pit(&prefetched, max_book_staleness)?;
    Ok(HistoricalWindow {
        prefetched,
        pit,
        book_decode_failures,
    })
}

/// Loads + materializes a historical window from `ClickHouse` facts + the PG catalog.
///
/// Holds no live source; the resulting [`HistoricalWindow`] serves every
/// point-in-time lookup from memory.
pub struct HistoricalWindowLoader {
    fact_read: Arc<dyn QuantFactReadRepository>,
    catalog_repo: Arc<dyn CatalogLedgerRepository>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    calibration_repo: Arc<dyn CalibrationArtifactRepository>,
    max_book_staleness: Duration,
}

impl HistoricalWindowLoader {
    /// Wire the loader from the fact reader, market catalog, linkage ledger,
    /// and staleness bound.
    #[must_use]
    pub fn new(
        fact_read: Arc<dyn QuantFactReadRepository>,
        catalog_repo: Arc<dyn CatalogLedgerRepository>,
        linkage_repo: Arc<dyn MarketLinkageRepository>,
        calibration_repo: Arc<dyn CalibrationArtifactRepository>,
        max_book_staleness: Duration,
    ) -> Self {
        Self {
            fact_read,
            catalog_repo,
            linkage_repo,
            calibration_repo,
            max_book_staleness,
        }
    }

    /// Batch-read every fact the window needs, then materialize the PIT engine.
    pub async fn load(&self, spec: &WindowSpec) -> QuantResult<HistoricalWindow> {
        let prefetched = self.prefetch(spec).await?;
        let (pit, book_decode_failures) =
            build_materialized_pit(&prefetched, self.max_book_staleness)?;
        Ok(HistoricalWindow {
            prefetched,
            pit,
            book_decode_failures,
        })
    }

    /// Batch-read every historical fact the window will consume, without
    /// materializing the in-memory PIT engine (used when a caller supplies its
    /// own point-in-time source, e.g. leakage-probe integration tests).
    pub async fn prefetch(&self, spec: &WindowSpec) -> QuantResult<Prefetched> {
        let WindowSubjects {
            tokens,
            markets,
            seen_tokens,
        } = window_subjects(&spec.samples);

        let end_boundary = (spec).window_end_boundary()?;
        let catalog = self
            .catalog_repo
            .window_through(&markets, &end_boundary)
            .await
            .map_err(QuantError::from)?;
        let catalog_markets = decode_catalog_markets(&catalog)?;

        // Expand books to both binary outcome tokens named by every catalog
        // revision. The model context uses the exact PIT best ask for whichever
        // side it recommends; a NO quote is never synthesized as `1 - YES`.
        let book_tokens = expand_book_tokens(tokens.clone(), seen_tokens, &catalog_markets);

        // The earliest sample resolves its book at
        // `window_start - knowledge_lag`. Prefetch one full staleness window
        // before that already-governed cutoff; starting at merely
        // `window_start - max_book_staleness` drops valid rows whenever lag is
        // non-zero and creates an offline-only missing book.
        let book_from = book_prefetch_start(
            spec.window_start,
            spec.knowledge_lag,
            self.max_book_staleness,
        )?
        .timestamp_millis();
        let book_to = spec.window_end.timestamp_millis();
        let lookback_start = checked_sub_duration(
            spec.window_start,
            spec.lookback,
            "historical feature lookback",
        )?;
        let micro_from = checked_sub_duration(
            lookback_start,
            spec.knowledge_lag,
            "historical knowledge lag",
        )?
        .timestamp_millis();
        let horizon_secs = i64::try_from(spec.max_horizon_secs).map_err(|error| {
            QuantError::config(format!("historical max horizon does not fit i64: {error}"))
        })?;
        let micro_to = spec
            .window_end
            .checked_add_signed(ChronoDuration::seconds(horizon_secs))
            .ok_or_else(|| {
                QuantError::config("historical forward window end is outside chrono range")
            })?
            .timestamp_millis();
        let resolution_to = micro_to;

        let available_by_ms = spec.available_by.timestamp_millis();
        let book_rows = self
            .fact_read
            .book_ledger_snapshots_between(book_tokens, book_from, book_to, available_by_ms)
            .await
            .map_err(QuantError::from)?;
        let micro_rows = self
            .fact_read
            .microstructure_window(tokens.clone(), micro_from, micro_to, available_by_ms)
            .await
            .map_err(QuantError::from)?;
        let trade_rows = self
            .fact_read
            .market_tape_window(markets.clone(), micro_from, micro_to, available_by_ms)
            .await
            .map_err(QuantError::from)?;
        let resolution_rows = self
            .fact_read
            .resolutions_between(markets.clone(), 0, resolution_to, available_by_ms)
            .await
            .map_err(QuantError::from)?;

        let (books, resolutions) = group_book_resolution_rows(book_rows, resolution_rows);
        let micro = group_micro_rows(micro_rows);
        let trade_tape = group_trade_tape_rows(trade_rows);

        let (
            linkages,
            domain_observations,
            crypto_reports,
            weather_observations,
            weather_forecasts,
        ) = self
            .prefetch_domain(spec, &end_boundary, &markets, &catalog_markets)
            .await?;
        let weather_calibrations = self
            .calibration_repo
            .published_weather_through(end_boundary.decision_at())
            .await?;

        Ok(Prefetched {
            books,
            micro,
            trade_tape,
            resolutions,
            catalog,
            domain_observations,
            crypto_reports,
            weather_observations,
            weather_forecasts,
            weather_calibrations,
            linkages,
        })
    }

    /// Prefetch the frozen linkage ledger and PIT domain observations for every
    /// category-mapped sample market.
    ///
    /// The observation range covers the widest domain lookback before
    /// `window_start` through `window_end` (domain features never look
    /// forward); the linkage ledger is bounded by the same end boundary used
    /// for the catalog snapshot, on both effective and availability axes, so
    /// each sample can apply its own bitemporal boundary in memory.
    async fn prefetch_domain(
        &self,
        spec: &WindowSpec,
        end_boundary: &DecisionBoundary,
        sample_markets: &[MarketId],
        catalog_markets: &[MarketRegistryInfo],
    ) -> QuantResult<(
        HashMap<MarketId, Vec<MarketLinkage>>,
        HashMap<DomainInstrumentKey, Vec<DomainObservation>>,
        HashMap<DomainInstrumentKey, Vec<CryptoPriceReport>>,
        HashMap<IcaoStation, Vec<WeatherObservationFact>>,
        HashMap<IcaoStation, Vec<WeatherForecastPoint>>,
    )> {
        let mapped_markets = mapped_domain_markets(spec, sample_markets, catalog_markets);
        if mapped_markets.is_empty() {
            return Ok((
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            ));
        }

        let linkage_window =
            load_domain_linkages(self.linkage_repo.as_ref(), &mapped_markets, end_boundary).await?;
        if linkage_window.instruments.is_empty() {
            return Ok((
                linkage_window.linkages,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            ));
        }

        let start_boundary = replay_boundary(
            spec.window_start,
            spec.knowledge_lag.as_secs(),
            spec.domain.crypto.availability_lag_secs,
            spec.domain.weather.availability_lag_secs,
        )?;
        let domain_from = checked_sub_duration(
            start_boundary.cutoff_for(DecisionSource::DomainCrypto),
            Duration::from_secs(crypto_lookback_secs(&spec.domain)),
            "domain observation lookback",
        )?;
        let domain_cutoff = end_boundary.cutoff_for(DecisionSource::DomainCrypto);
        let from_ms = domain_from.timestamp_millis();
        let to_ms = domain_cutoff
            .timestamp_millis()
            .checked_add(1)
            .ok_or_else(|| QuantError::config("domain observation cutoff overflowed i64"))?;
        let domain_observations = load_domain_observations(
            self.fact_read.as_ref(),
            &linkage_window.instruments,
            domain_from,
            domain_cutoff,
            end_boundary.decision_at(),
        )
        .await?;
        let crypto_reports = load_crypto_reports(
            self.fact_read.as_ref(),
            &linkage_window.oracle_instruments,
            from_ms,
            to_ms,
            domain_cutoff,
            end_boundary.decision_at(),
        )
        .await?;
        let weather_from = checked_sub_duration(
            start_boundary.cutoff_for(DecisionSource::DomainWeather),
            Duration::from_secs(
                u64::from(spec.domain.weather.calibration_lookback_days)
                    .checked_mul(86_400)
                    .ok_or_else(|| QuantError::config("Weather calibration lookback overflowed"))?,
            ),
            "Weather calibration lookback",
        )?;
        let weather_cutoff = end_boundary.cutoff_for(DecisionSource::DomainWeather);
        let (weather_observations, weather_forecasts) = load_weather_facts(
            self.fact_read.as_ref(),
            &linkage_window.weather_stations,
            weather_from,
            linkage_window.weather_valid_to,
            weather_cutoff,
            end_boundary.decision_at(),
        )
        .await?;
        Ok((
            linkage_window.linkages,
            domain_observations,
            crypto_reports,
            weather_observations,
            weather_forecasts,
        ))
    }
}

fn group_book_resolution_rows(
    book_rows: Vec<BookL2LedgerRow>,
    resolution_rows: Vec<MarketResolutionRow>,
) -> (
    HashMap<TokenId, Vec<BookL2LedgerRow>>,
    HashMap<MarketId, Vec<MarketResolutionRow>>,
) {
    let mut books: HashMap<TokenId, Vec<BookL2LedgerRow>> = HashMap::new();
    for row in book_rows {
        books.entry(row.token_id.clone()).or_default().push(row);
    }
    let mut resolutions: HashMap<MarketId, Vec<MarketResolutionRow>> = HashMap::new();
    for row in resolution_rows {
        resolutions
            .entry(row.market_id.clone())
            .or_default()
            .push(row);
    }
    (books, resolutions)
}

struct DomainLinkageWindow {
    linkages: HashMap<MarketId, Vec<MarketLinkage>>,
    instruments: HashSet<DomainInstrumentKey>,
    oracle_instruments: HashSet<DomainInstrumentKey>,
    weather_stations: HashSet<IcaoStation>,
    weather_valid_to: DateTime<Utc>,
}

async fn load_domain_linkages(
    repository: &dyn MarketLinkageRepository,
    markets: &[MarketId],
    boundary: &DecisionBoundary,
) -> QuantResult<DomainLinkageWindow> {
    let rows = repository
        .ledger_for_markets(markets, boundary)
        .await
        .map_err(QuantError::from)?;
    let mut window = DomainLinkageWindow {
        linkages: HashMap::new(),
        instruments: HashSet::new(),
        oracle_instruments: HashSet::new(),
        weather_stations: HashSet::new(),
        weather_valid_to: boundary.decision_at(),
    };
    for info in rows {
        let market_id = info.market_id.clone();
        let linkage = MarketLinkage::from(info);
        if let Some(binding) = linkage.binding() {
            window.instruments.extend(
                binding
                    .source_bindings
                    .iter()
                    .map(|source| source.instrument_key.clone()),
            );
            if let Some(oracle_key) = oracle_instrument(binding) {
                window.instruments.insert(oracle_key.clone());
                window.oracle_instruments.insert(oracle_key);
            }
            if let MarketSubject::Weather(subject) = &binding.subject {
                window
                    .weather_stations
                    .insert(subject.decision_group.station.clone());
                window.weather_valid_to =
                    window.weather_valid_to.max(weather_local_day_end(subject)?);
            }
        }
        window.linkages.entry(market_id).or_default().push(linkage);
    }
    Ok(window)
}

async fn load_domain_observations(
    fact_read: &dyn QuantFactReadRepository,
    instruments: &HashSet<DomainInstrumentKey>,
    from: DateTime<Utc>,
    cutoff: DateTime<Utc>,
    decision_at: DateTime<Utc>,
) -> QuantResult<HashMap<DomainInstrumentKey, Vec<DomainObservation>>> {
    let to_ms = cutoff
        .timestamp_millis()
        .checked_add(1)
        .ok_or_else(|| QuantError::config("domain observation cutoff overflowed i64"))?;
    let rows = fact_read
        .domain_observations_between(
            instruments.iter().cloned().collect(),
            from.timestamp_millis(),
            to_ms,
            cutoff.timestamp_millis(),
            decision_at.timestamp_millis(),
        )
        .await
        .map_err(QuantError::from)?;
    let mut observations: HashMap<DomainInstrumentKey, Vec<DomainObservation>> = HashMap::new();
    for row in rows {
        let observation = DomainObservation::from_clickhouse_row(&row).ok_or_else(|| {
            ResearchError::PitResolution {
                detail: format!(
                    "domain observation {} / {} at {} cannot be decoded",
                    row.instrument_key, row.metric, row.event_time
                ),
            }
        })?;
        let outside_pit = observation.observed_at < from
            || observation.observed_at > cutoff
            || observation.publish_time > cutoff
            || observation
                .available_at
                .is_none_or(|available_at| available_at > decision_at);
        if outside_pit {
            return Err(ResearchError::PitResolution {
                detail: format!(
                    "domain observation {} at {} is outside PIT window [{from}, {cutoff}]",
                    observation.instrument_key, observation.observed_at
                ),
            }
            .into());
        }
        observations
            .entry(observation.instrument_key.clone())
            .or_default()
            .push(observation);
    }
    for series in observations.values_mut() {
        series.sort_by_key(|observation| observation.observed_at);
    }
    Ok(observations)
}

async fn load_crypto_reports(
    fact_read: &dyn QuantFactReadRepository,
    instruments: &HashSet<DomainInstrumentKey>,
    from_ms: i64,
    to_ms: i64,
    cutoff: DateTime<Utc>,
    decision_at: DateTime<Utc>,
) -> QuantResult<HashMap<DomainInstrumentKey, Vec<CryptoPriceReport>>> {
    let rows = fact_read
        .crypto_price_reports_between(
            instruments.iter().cloned().collect(),
            from_ms,
            to_ms,
            cutoff.timestamp_millis(),
            decision_at.timestamp_millis(),
        )
        .await
        .map_err(QuantError::from)?;
    let mut reports: HashMap<DomainInstrumentKey, Vec<CryptoPriceReport>> = HashMap::new();
    for row in rows {
        let report = CryptoPriceReport::from_clickhouse_row(row).ok_or_else(|| {
            ResearchError::PitResolution {
                detail: "crypto report contains an invalid persisted timestamp".to_owned(),
            }
        })?;
        reports
            .entry(report.instrument_key.clone())
            .or_default()
            .push(report);
    }
    for series in reports.values_mut() {
        series.sort_by_key(|report| {
            (
                report.event_time,
                report.available_at,
                report.source_sequence,
            )
        });
    }
    Ok(reports)
}

async fn load_weather_facts(
    fact_read: &dyn QuantFactReadRepository,
    stations: &HashSet<IcaoStation>,
    from: DateTime<Utc>,
    valid_to: DateTime<Utc>,
    cutoff: DateTime<Utc>,
    decision_at: DateTime<Utc>,
) -> QuantResult<(
    HashMap<IcaoStation, Vec<WeatherObservationFact>>,
    HashMap<IcaoStation, Vec<WeatherForecastPoint>>,
)> {
    let stations = stations.iter().map(ToString::to_string).collect::<Vec<_>>();
    let observation_to = cutoff
        .timestamp_millis()
        .checked_add(1)
        .ok_or_else(|| QuantError::config("Weather observation cutoff overflowed i64"))?;
    let rows = fact_read
        .weather_observation_facts_between(
            stations.clone(),
            from.timestamp_millis(),
            observation_to,
            cutoff.timestamp_millis(),
            decision_at.timestamp_millis(),
        )
        .await
        .map_err(QuantError::from)?;
    let mut observations = HashMap::<IcaoStation, Vec<WeatherObservationFact>>::new();
    for row in rows {
        let fact = WeatherObservationFact::from_clickhouse_row(row).ok_or_else(|| {
            ResearchError::PitResolution {
                detail: "Weather observation contains an invalid persisted value".to_owned(),
            }
        })?;
        let station = fact.station().ok_or_else(|| ResearchError::PitResolution {
            detail: format!(
                "Weather observation subject `{}` is not an ICAO station",
                fact.subject_key
            ),
        })?;
        observations.entry(station).or_default().push(fact);
    }
    for series in observations.values_mut() {
        series.sort_by(|left, right| {
            (left.observed_at, left.revision, left.report_hash).cmp(&(
                right.observed_at,
                right.revision,
                right.report_hash,
            ))
        });
    }
    let forecast_to = valid_to
        .timestamp_millis()
        .checked_add(1)
        .ok_or_else(|| QuantError::config("GEFS valid-time cutoff overflowed i64"))?;
    let rows = fact_read
        .weather_forecast_facts_between(
            stations,
            from.timestamp_millis(),
            forecast_to,
            cutoff.timestamp_millis(),
            decision_at.timestamp_millis(),
        )
        .await
        .map_err(QuantError::from)?;
    let mut forecasts = HashMap::<IcaoStation, Vec<WeatherForecastPoint>>::new();
    for row in rows {
        let fact = WeatherForecastPoint::from_clickhouse_row(row).ok_or_else(|| {
            ResearchError::PitResolution {
                detail: "GEFS point contains an invalid persisted value".to_owned(),
            }
        })?;
        let station = fact.station().ok_or_else(|| ResearchError::PitResolution {
            detail: format!(
                "Weather forecast subject `{}` is not an ICAO station",
                fact.subject_key
            ),
        })?;
        forecasts.entry(station).or_default().push(fact);
    }
    for series in forecasts.values_mut() {
        series.sort_by_key(|point| {
            (
                point.reference_time,
                point.valid_time,
                point.lead_hours,
                point.member,
            )
        });
    }
    Ok((observations, forecasts))
}

fn weather_local_day_end(subject: &WeatherSubject) -> QuantResult<DateTime<Utc>> {
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
    let midnight = next_date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| QuantError::config("invalid Weather local midnight"))?;
    timezone
        .from_local_datetime(&midnight)
        .single()
        .map(|value| value.with_timezone(&Utc))
        .ok_or_else(|| QuantError::config("Weather local midnight is ambiguous or missing"))
}

fn mapped_domain_markets(
    spec: &WindowSpec,
    sample_markets: &[MarketId],
    catalog_markets: &[MarketRegistryInfo],
) -> Vec<MarketId> {
    sample_markets
        .iter()
        .filter(|market_id| {
            catalog_markets.iter().any(|market| {
                &market.market_id == *market_id
                    && DomainFamily::for_category(market.primary_category())
                        .is_some_and(|family| spec.domain.family_enabled(family))
            })
        })
        .cloned()
        .collect()
}

fn group_micro_rows(
    rows: Vec<BookMicrostructureRow>,
) -> HashMap<TokenId, Vec<BookMicrostructureRow>> {
    let mut grouped: HashMap<TokenId, Vec<BookMicrostructureRow>> = HashMap::new();
    for row in rows {
        grouped.entry(row.token_id.clone()).or_default().push(row);
    }
    grouped
}

fn group_trade_tape_rows(rows: Vec<TradeTapeRow>) -> HashMap<MarketId, Vec<TradeTapeRow>> {
    let mut grouped: HashMap<MarketId, Vec<TradeTapeRow>> = HashMap::new();
    for row in rows {
        grouped.entry(row.market_id.clone()).or_default().push(row);
    }
    grouped
}

impl WindowSpec {
    fn window_end_boundary(&self) -> QuantResult<DecisionBoundary> {
        if self.knowledge_lag.subsec_nanos() != 0 {
            return Err(QuantError::config(
                "historical knowledge lag must be expressed in whole seconds",
            ));
        }
        let availability_delay = self
            .available_by
            .signed_duration_since(self.window_end)
            .to_std()
            .map_err(|error| {
                QuantError::config(format!(
                    "historical availability cutoff precedes the replay window: {error}"
                ))
            })?;
        let global_lag = availability_delay
            .as_secs()
            .checked_add(self.knowledge_lag.as_secs())
            .ok_or_else(|| QuantError::config("historical availability lag overflow"))?;
        let crypto_lag = global_lag
            .checked_add(self.domain.crypto.availability_lag_secs)
            .ok_or_else(|| QuantError::config("historical Crypto availability lag overflow"))?;
        let weather_lag = global_lag
            .checked_add(self.domain.weather.availability_lag_secs)
            .ok_or_else(|| QuantError::config("historical Weather availability lag overflow"))?;
        replay_boundary(self.available_by, global_lag, crypto_lag, weather_lag)
    }
}

/// Construct the sole frozen boundary for one offline replay decision.
///
/// Every reader consumes one of these recorded cutoffs. No downstream window
/// helper is permitted to subtract lag again.
pub fn replay_boundary(
    decision_at: DateTime<Utc>,
    knowledge_lag_secs: u64,
    domain_crypto_lag_secs: u64,
    domain_weather_lag_secs: u64,
) -> QuantResult<DecisionBoundary> {
    DecisionClock::new(knowledge_lag_secs).serving_boundary(
        decision_at,
        domain_crypto_lag_secs,
        domain_weather_lag_secs,
    )
}

fn decode_catalog_markets(catalog: &CatalogWindowInfo) -> QuantResult<Vec<MarketRegistryInfo>> {
    catalog
        .market_changes
        .iter()
        .map(|version| {
            serde_json::from_value(version.payload.clone().into_inner()).map_err(|error| {
                ResearchError::PitResolution {
                    detail: format!(
                        "market catalog change {} payload is invalid: {error}",
                        version.market_change_id
                    ),
                }
                .into()
            })
        })
        .collect()
}

/// Build the in-memory PIT engine from the prefetched window, returning the
/// engine and the number of book rows that failed to decode.
fn build_materialized_pit(
    prefetched: &Prefetched,
    max_staleness: Duration,
) -> QuantResult<(MaterializedPitEngine, u64)> {
    let mut book_decode_failures = 0_u64;
    let mut books: HashMap<TokenId, Vec<BookSnapshotAt>> = HashMap::new();
    for (token, rows) in &prefetched.books {
        let mut series = Vec::with_capacity(rows.len());
        for row in rows {
            let source_cutoff = timestamp_millis(row.venue_event_time, "book venue_event_time")?;
            let decision_at = timestamp_millis(row.persisted_time, "book persisted_time")?;
            let (snapshot, status) = snapshot_from_row(row.clone(), source_cutoff, decision_at);
            if status.counts_as_failure() {
                book_decode_failures += 1;
            }
            if let Some(snapshot) = snapshot {
                series.push(snapshot);
            }
        }
        books.insert(token.clone(), series);
    }

    Ok((
        MaterializedPitEngine::new(
            books,
            prefetched.catalog.clone(),
            chrono_duration(max_staleness, "training.max_book_staleness_ms")?,
        )?,
        book_decode_failures,
    ))
}

struct WindowSubjects {
    tokens: Vec<TokenId>,
    markets: Vec<MarketId>,
    seen_tokens: HashSet<TokenId>,
}

fn expand_book_tokens(
    mut tokens: Vec<TokenId>,
    mut seen: HashSet<TokenId>,
    markets: &[MarketRegistryInfo],
) -> Vec<TokenId> {
    for market in markets {
        if seen.insert(market.token_yes.clone()) {
            tokens.push(market.token_yes.clone());
        }
        if seen.insert(market.token_no.clone()) {
            tokens.push(market.token_no.clone());
        }
    }
    tokens
}

fn window_subjects(samples: &[ReplaySample]) -> WindowSubjects {
    let mut tokens = Vec::new();
    let mut markets = Vec::new();
    let mut seen_tokens = HashSet::new();
    let mut seen_markets = HashSet::new();
    for sample in samples {
        if seen_tokens.insert(sample.token_id.clone()) {
            tokens.push(sample.token_id.clone());
        }
        if seen_markets.insert(sample.market_id.clone()) {
            markets.push(sample.market_id.clone());
        }
    }
    WindowSubjects {
        tokens,
        markets,
        seen_tokens,
    }
}

/// Project a market catalog row into a selection entry oriented to `primary_token_id`.
///
/// Liquidity and volume come from the exact PIT catalog change. The offline
/// plane never substitutes a live selection snapshot or re-derives these fields
/// from a different evidence source.
pub fn selected_market(
    info: &MarketRegistryInfo,
    primary_token_id: &TokenId,
) -> QuantResult<SelectedMarket> {
    let (primary_token_id, secondary_token_id) = if *primary_token_id == info.token_yes {
        (info.token_yes.clone(), info.token_no.clone())
    } else if *primary_token_id == info.token_no {
        (info.token_no.clone(), info.token_yes.clone())
    } else {
        return Err(ResearchError::PitResolution {
            detail: format!(
                "replay token {primary_token_id} matches neither catalog token {} nor {} for market {}",
                info.token_yes, info.token_no, info.market_id
            ),
        }
        .into());
    };
    Ok(SelectedMarket {
        market_id: info.market_id.clone(),
        event_id: info.event_id.clone(),
        category: info.primary_category(),
        primary_token_id,
        secondary_token_id: Some(secondary_token_id),
        liquidity_usd: info.liquidity_usd,
        volume_24h_usd: info.volume_24h,
        source_refs: Vec::new(),
    })
}

/// Build the trailing PIT feature window for one `(token, as_of)`.
pub fn feature_window(
    token_id: TokenId,
    boundary: &DecisionBoundary,
    lookback: Duration,
    rows: &[BookMicrostructureRow],
) -> QuantResult<MarketWindowSnapshot> {
    let cutoff = boundary.cutoff_for(DecisionSource::Microstructure);
    let start = checked_sub_duration(cutoff, lookback, "feature window lookback")?;
    let mut buckets = Vec::new();
    for row in rows {
        let at = timestamp_millis(row.bucket_time, "microstructure bucket_time")?;
        let available_at = timestamp_millis(row.available_at, "microstructure available_at")?;
        if at >= start && at <= cutoff && available_at <= boundary.decision_at() {
            buckets.push(bucket_from_row(row, at, available_at));
        }
    }
    Ok(MarketWindowSnapshot {
        token_id,
        decision_at: boundary.decision_at(),
        knowledge_cutoff: cutoff,
        buckets,
    })
}

/// Build the trailing PIT trade-tape window for one `(market, as_of)`.
pub fn trade_tape_window(
    market_id: MarketId,
    boundary: &DecisionBoundary,
    lookback: Duration,
    rows: &[TradeTapeRow],
) -> QuantResult<TradeTapeWindowSnapshot> {
    let cutoff = boundary.cutoff_for(DecisionSource::TradeTape);
    let start = checked_sub_duration(cutoff, lookback, "trade-tape window lookback")?;
    let mut prints = Vec::new();
    for row in rows {
        let at = timestamp_millis(row.event_time, "trade-tape event_time")?;
        let available_at = timestamp_millis(row.ingestion_time, "trade-tape ingestion_time")?;
        if at >= start && at < cutoff && available_at <= boundary.decision_at() {
            prints.push(TradeTapePrint::from_clickhouse_row_at(
                row,
                at,
                available_at,
            ));
        }
    }
    Ok(TradeTapeWindowSnapshot::available(
        market_id,
        boundary.decision_at(),
        cutoff,
        prints,
    ))
}

/// Build the strictly-forward label/settlement window for one `(token, as_of)`.
pub fn forward_window(
    as_of: DateTime<Utc>,
    max_horizon_secs: u64,
    rows: &[BookMicrostructureRow],
    resolutions: &[MarketResolutionRow],
) -> QuantResult<ForwardWindow> {
    let data_available_until = rows
        .last()
        .map(|row| timestamp_millis(row.bucket_time, "forward bucket_time"))
        .transpose()?
        .unwrap_or(as_of);
    let horizon_secs = i64::try_from(max_horizon_secs).map_err(|error| {
        QuantError::config(format!("forward horizon does not fit i64 seconds: {error}"))
    })?;
    let cap = as_of
        .checked_add_signed(ChronoDuration::seconds(horizon_secs))
        .ok_or_else(|| QuantError::config("forward horizon end is outside chrono range"))?;
    let mut samples = Vec::new();
    for row in rows {
        let at = timestamp_millis(row.bucket_time, "forward bucket_time")?;
        if at > as_of && at <= cap {
            samples.push(forward_sample(row, at));
        }
    }
    // Settlement is independent of microstructure maturity: any resolution strictly
    // after `as_of` is visible to the settlement labeler.
    let mut decoded_resolutions = Vec::with_capacity(resolutions.len());
    for row in resolutions {
        let resolved_at = timestamp_millis(row.resolved_at, "resolution resolved_at")?;
        let observed_at = timestamp_millis(row.observed_at, "resolution observed_at")?;
        if resolved_at > as_of {
            row.validate().map_err(|error| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: format!(
                        "market resolution `{}` violates the canonical payout contract: {error}",
                        row.market_id
                    ),
                })
            })?;
            let payout_ratios = row
                .payout_ratios
                .iter()
                .copied()
                .map(|payout| {
                    PayoutRatio::try_from(payout).map_err(|error| {
                        QuantError::from(ResearchError::DatasetBuild {
                            detail: format!(
                                "market resolution `{}` contains an invalid payout: {error}",
                                row.market_id
                            ),
                        })
                    })
                })
                .collect::<QuantResult<Vec<_>>>()?;
            decoded_resolutions.push(ResearchMarketResolution {
                token_ids: row.token_ids.clone(),
                payout_ratios,
                resolved_at,
                observed_at,
            });
        }
    }
    let resolution = decoded_resolutions
        .into_iter()
        .max_by_key(|resolution| (resolution.resolved_at, resolution.observed_at));
    Ok(ForwardWindow {
        anchor: as_of,
        data_available_until,
        samples,
        resolution,
    })
}

/// Decode a microstructure row into a compute-domain bucket.
fn bucket_from_row(
    row: &BookMicrostructureRow,
    at: DateTime<Utc>,
    available_at: DateTime<Utc>,
) -> MicrostructureBucket {
    MicrostructureBucket {
        bucket_time: at,
        available_at,
        mid_close: row.mid_price_close.map(Price::from),
        spread_bps_avg: row.spread_bps_avg.map(Bps::from),
        top1_depth_usd_avg: row.top1_depth_usd_avg.map(Usd::from),
        top5_depth_usd_avg: row.top5_depth_usd_avg.map(Usd::from),
        imbalance_avg: row.imbalance_avg.map(Decimal::from),
        update_count: row.update_count,
        snapshot_count: row.snapshot_count,
        delta_count: row.delta_count,
        crossed_count: row.crossed_count,
        gap_count: row.gap_count,
        max_book_age_ms: row.max_book_age_ms,
    }
}

/// Decode a microstructure row into a forward label observation.
fn forward_sample(row: &BookMicrostructureRow, at: DateTime<Utc>) -> ForwardSample {
    ForwardSample {
        at,
        mid_close: row.mid_price_close.map(Price::from),
        best_bid_high: row.best_bid_high.map(Price::from),
        best_bid_low: row.best_bid_low.map(Price::from),
        best_bid_close: row.best_bid_close.map(Price::from),
        top1_depth_usd: row.top1_depth_usd_avg.map(Usd::from),
    }
}

fn chrono_duration(duration: Duration, field: &'static str) -> QuantResult<ChronoDuration> {
    ChronoDuration::from_std(duration)
        .map_err(|error| QuantError::config(format!("{field} is outside chrono range: {error}")))
}

fn checked_sub_duration(
    value: DateTime<Utc>,
    duration: Duration,
    field: &'static str,
) -> QuantResult<DateTime<Utc>> {
    value
        .checked_sub_signed(chrono_duration(duration, field)?)
        .ok_or_else(|| QuantError::config(format!("{field} start is outside chrono range")))
}

fn book_prefetch_start(
    window_start: DateTime<Utc>,
    knowledge_lag: Duration,
    max_book_staleness: Duration,
) -> QuantResult<DateTime<Utc>> {
    let coverage = knowledge_lag
        .checked_add(max_book_staleness)
        .ok_or_else(|| QuantError::config("historical book prefetch duration overflow"))?;
    checked_sub_duration(
        window_start,
        coverage,
        "historical book knowledge-lag and staleness coverage",
    )
}

fn timestamp_millis(timestamp_ms: i64, field: &'static str) -> QuantResult<DateTime<Utc>> {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .ok_or_else(|| {
            ResearchError::PitResolution {
                detail: format!("{field} {timestamp_ms} is outside chrono range"),
            }
            .into()
        })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{Duration as ChronoDuration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::market::{MarketRegistryInfo, TokenInfo},
        enums::{
            catalog::CatalogFilterReasonSet,
            common::{CategorySet, MarketCategory, TickSize},
            market::MarketStatus,
        },
        types::{EventId, MarketId, TokenId},
    };
    use rust_decimal_macros::dec;

    use super::{book_prefetch_start, selected_market};

    fn market() -> MarketRegistryInfo {
        MarketRegistryInfo {
            market_id: MarketId::from("market"),
            event_id: EventId::from("event"),
            token_yes: TokenId::from("yes"),
            token_no: TokenId::from("no"),
            question: "test?".to_owned(),
            slug: "test".to_owned(),
            description: None,
            categories: CategorySet::from(MarketCategory::Other),
            status: MarketStatus::Active,
            filter_reasons: CatalogFilterReasonSet::default(),
            outcome: None,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: TokenId::from("yes"),
                    outcome: "Yes".to_owned(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: TokenId::from("no"),
                    outcome: "No".to_owned(),
                    neg_risk: false,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: dec!(5),
            liquidity_usd: None,
            volume_24h: None,
            start_date: None,
            end_date: None,
            resolved_at: None,
            created_at: None,
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn book_non_zero_before() {
        let window_start = Utc
            .with_ymd_and_hms(2026, 7, 12, 12, 0, 0)
            .single()
            .expect("timestamp");

        let from = book_prefetch_start(
            window_start,
            Duration::from_mins(2),
            Duration::from_secs(10),
        )
        .expect("prefetch start");

        assert_eq!(from, window_start - ChronoDuration::seconds(130));
    }

    #[test]
    fn book_zero_preserves_window() {
        let window_start = Utc
            .with_ymd_and_hms(2026, 7, 12, 12, 0, 0)
            .single()
            .expect("timestamp");

        let from = book_prefetch_start(window_start, Duration::ZERO, Duration::from_secs(10))
            .expect("prefetch start");

        assert_eq!(from, window_start - ChronoDuration::seconds(10));
    }

    #[test]
    fn selected_market_orients_no() {
        let selected = selected_market(&market(), &TokenId::from("no")).expect("NO market");

        assert_eq!(selected.primary_token_id, TokenId::from("no"));
        assert_eq!(selected.secondary_token_id, Some(TokenId::from("yes")));
    }

    #[test]
    fn selected_rejects_unknown_token() {
        assert!(selected_market(&market(), &TokenId::from("other")).is_err());
    }
}
