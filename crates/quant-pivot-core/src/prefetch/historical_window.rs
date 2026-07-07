//! Shared historical-window prefetch + point-in-time materialization (Phase 3.6).
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
//! Features are bounded by `as_of - source_delay`; forward label/settlement
//! windows look strictly after `as_of`. There is **no** live `BookStore` here —
//! the offline plane is structurally barred from the live source.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, ChBps, ChDecimal64, ChPrice, ChUsd,
        MarketResolutionRow, TradeTapeRow,
    },
    domain::market::registry::{CatalogMarketLeg, MarketInfo, NegRiskLegSet},
    domain::{DomainObservation, MarketLinkage, TradeTapePrint},
    enums::{domain::DomainFamily, market::MarketStatus},
    runtime_config::DomainConfig,
    types::{DomainInstrumentKey, EventId, MarketId, TokenId},
};
use quant_pivot_repository::traits::{
    EventRepository, MarketLinkageRepository, MarketRepository, QuantFactReadRepository,
};
use quant_pivot_research::{
    features::{MarketWindowSnapshot, MicrostructureBucket, TradeTapeWindowSnapshot},
    pit::{BookSnapshotAt, MarketContextAt, MaterializedPitEngine},
    selection::SelectedMarket,
    training::{ForwardSample, ForwardWindow, MarketResolution as ResearchMarketResolution},
};

use crate::pit::platform::ch_historical::snapshot_from_row;
use quant_pivot_research::{
    domain::{crypto_lookback_secs, oracle_instrument},
    pit::TradeTapePitParams,
};

/// One `(market, token)` instant the replay will resolve point-in-time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaySample {
    /// Market the sample scores.
    pub market_id: MarketId,
    /// Primary (YES) outcome token.
    pub token_id: TokenId,
}

/// The window + sample set a prefetch must cover.
pub struct WindowSpec {
    /// Inclusive window start (first `as_of`).
    pub window_start: DateTime<Utc>,
    /// Exclusive window end (last `as_of` is strictly before).
    pub window_end: DateTime<Utc>,
    /// Distinct `(market, token)` samples to prefetch facts for.
    pub samples: Vec<ReplaySample>,
    /// Maximum trailing feature lookback.
    pub lookback: Duration,
    /// Source visibility delay applied to features.
    pub source_delay: Duration,
    /// Maximum forward label horizon (forward facts are read this far past `window_end`).
    pub max_horizon_secs: u64,
    /// Frozen domain-plane configuration (drives linkage + observation prefetch).
    pub domain: DomainConfig,
}

/// Batch-prefetched historical facts for an offline replay window.
pub struct Prefetched {
    /// Book snapshots per token (ascending by observation time).
    pub books: HashMap<TokenId, Vec<BookSnapshotRow>>,
    /// Microstructure buckets per token (trailing + forward).
    pub micro: HashMap<TokenId, Vec<BookMicrostructureRow>>,
    /// Trade-tape participant rows per market.
    pub trade_tape: HashMap<MarketId, Vec<TradeTapeRow>>,
    /// Settlement resolutions per market.
    pub resolutions: HashMap<MarketId, Vec<MarketResolutionRow>>,
    /// Market catalog metadata.
    pub markets_by_id: HashMap<MarketId, Arc<MarketInfo>>,
    /// Per-event neg-risk leg enumeration (expected vs resolved), keyed by event.
    pub neg_risk_leg_sets: HashMap<EventId, NegRiskLegSet>,
    /// External domain observations per instrument, ascending (Phase 11.2.2).
    pub domain_observations: HashMap<DomainInstrumentKey, Vec<DomainObservation>>,
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

/// Loads + materializes a historical window from `ClickHouse` facts + the PG catalog.
///
/// Holds no live source; the resulting [`HistoricalWindow`] serves every
/// point-in-time lookup from memory.
pub struct HistoricalWindowLoader {
    fact_read: Arc<dyn QuantFactReadRepository>,
    market_repo: Arc<dyn MarketRepository>,
    event_repo: Arc<dyn EventRepository>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    max_book_staleness: Duration,
}

impl HistoricalWindowLoader {
    /// Wire the loader from the fact reader, market catalog, linkage ledger,
    /// and staleness bound.
    #[must_use]
    pub fn new(
        fact_read: Arc<dyn QuantFactReadRepository>,
        market_repo: Arc<dyn MarketRepository>,
        event_repo: Arc<dyn EventRepository>,
        linkage_repo: Arc<dyn MarketLinkageRepository>,
        max_book_staleness: Duration,
    ) -> Self {
        Self {
            fact_read,
            market_repo,
            event_repo,
            linkage_repo,
            max_book_staleness,
        }
    }

    /// Batch-read every fact the window needs, then materialize the PIT engine.
    pub async fn load(&self, spec: &WindowSpec) -> QuantResult<HistoricalWindow> {
        let prefetched = self.prefetch(spec).await?;
        let (pit, book_decode_failures) =
            build_materialized_pit(&prefetched, self.max_book_staleness);
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
        let mut tokens: Vec<TokenId> = Vec::new();
        let mut markets: Vec<MarketId> = Vec::new();
        let mut seen_tokens: HashSet<TokenId> = HashSet::new();
        let mut seen_markets: HashSet<MarketId> = HashSet::new();
        for sample in &spec.samples {
            if seen_tokens.insert(sample.token_id.clone()) {
                tokens.push(sample.token_id.clone());
            }
            if seen_markets.insert(sample.market_id.clone()) {
                markets.push(sample.market_id.clone());
            }
        }

        // Load sample market infos first, then — for neg-risk markets — expand to
        // the full set of event YES legs so the offline structural full-leg
        // aggregates resolve from the same books the online plane sees (Phase
        // 11.2.1 train-serve parity). Sibling leg BOOKS are prefetched; sibling
        // micro/resolutions are not needed by the structural plane.
        let sample_infos = self
            .market_repo
            .find_by_ids(&markets)
            .await
            .map_err(QuantError::from)?;
        let mut markets_by_id: HashMap<MarketId, Arc<MarketInfo>> = sample_infos
            .iter()
            .map(|info| (info.market_id.clone(), Arc::clone(info)))
            .collect();
        let mut book_tokens: Vec<TokenId> = tokens.clone();
        let mut seen_book_tokens: HashSet<TokenId> = seen_tokens.clone();
        for info in &sample_infos {
            if !info.neg_risk {
                continue;
            }
            let siblings = self
                .market_repo
                .find_by_event(info.event_id.as_str())
                .await
                .map_err(QuantError::from)?;
            for sibling in siblings {
                if seen_book_tokens.insert(sibling.yes_token_id.clone()) {
                    book_tokens.push(sibling.yes_token_id.clone());
                }
                markets_by_id
                    .entry(sibling.market_id.clone())
                    .or_insert(sibling);
            }
        }

        let book_from = (spec.window_start - to_chrono(self.max_book_staleness)).timestamp_millis();
        let book_to = spec.window_end.timestamp_millis();
        let micro_from =
            (spec.window_start - to_chrono(spec.lookback) - to_chrono(spec.source_delay))
                .timestamp_millis();
        let micro_to = (spec.window_end
            + ChronoDuration::seconds(i64::try_from(spec.max_horizon_secs).unwrap_or(i64::MAX)))
        .timestamp_millis();
        let resolution_to = micro_to;

        let book_rows = self
            .fact_read
            .book_snapshots_between(book_tokens, book_from, book_to)
            .await
            .map_err(QuantError::from)?;
        let micro_rows = self
            .fact_read
            .microstructure_window(tokens.clone(), micro_from, micro_to)
            .await
            .map_err(QuantError::from)?;
        let trade_rows = self
            .fact_read
            .trade_tape_window_by_market(markets.clone(), micro_from, micro_to)
            .await
            .map_err(QuantError::from)?;
        let resolution_rows = self
            .fact_read
            .resolutions_between(markets.clone(), 0, resolution_to)
            .await
            .map_err(QuantError::from)?;

        let mut books: HashMap<TokenId, Vec<BookSnapshotRow>> = HashMap::new();
        for row in book_rows {
            books.entry(row.token_id.clone()).or_default().push(row);
        }
        let micro = group_micro_rows(micro_rows);
        let trade_tape = group_trade_tape_rows(trade_rows);
        let mut resolutions: HashMap<MarketId, Vec<MarketResolutionRow>> = HashMap::new();
        for row in resolution_rows {
            resolutions
                .entry(row.market_id.clone())
                .or_default()
                .push(row);
        }

        let neg_risk_leg_sets =
            build_neg_risk_leg_sets(self.event_repo.as_ref(), &sample_infos, &markets_by_id)
                .await?;
        let (linkages, domain_observations) =
            self.prefetch_domain(spec, &markets, &markets_by_id).await?;

        Ok(Prefetched {
            books,
            micro,
            trade_tape,
            resolutions,
            markets_by_id,
            neg_risk_leg_sets,
            domain_observations,
            linkages,
        })
    }

    /// Prefetch the frozen linkage ledger and PIT domain observations for every
    /// category-mapped sample market (Phase 11.2.2).
    ///
    /// The observation range covers the widest domain lookback before
    /// `window_start` through `window_end` (domain features never look
    /// forward); the linkage ledger is bounded by `derived_at <= window_end`
    /// so per-`as_of` bitemporal selection happens in memory.
    async fn prefetch_domain(
        &self,
        spec: &WindowSpec,
        sample_markets: &[MarketId],
        markets_by_id: &HashMap<MarketId, Arc<MarketInfo>>,
    ) -> QuantResult<(
        HashMap<MarketId, Vec<MarketLinkage>>,
        HashMap<DomainInstrumentKey, Vec<DomainObservation>>,
    )> {
        let mapped_markets: Vec<MarketId> = sample_markets
            .iter()
            .filter(|market_id| {
                markets_by_id.get(*market_id).is_some_and(|info| {
                    DomainFamily::for_category(info.fee_category())
                        .is_some_and(|family| spec.domain.family_enabled(family))
                })
            })
            .cloned()
            .collect();
        if mapped_markets.is_empty() {
            return Ok((HashMap::new(), HashMap::new()));
        }

        let ledger_rows = self
            .linkage_repo
            .ledger_for_markets(&mapped_markets, spec.window_end)
            .await
            .map_err(QuantError::from)?;
        let mut linkages: HashMap<MarketId, Vec<MarketLinkage>> = HashMap::new();
        let mut instruments: HashSet<DomainInstrumentKey> = HashSet::new();
        for info in ledger_rows {
            let market_id = info.market_id.clone();
            let linkage = info.into_domain().map_err(|error| {
                QuantError::config(format!(
                    "linkage ledger row for market {market_id} has an undecodable outcome \
                     payload: {error}"
                ))
            })?;
            if let Some(binding) = linkage.binding() {
                instruments.insert(binding.instrument_key.clone());
                if let Some(oracle_key) = oracle_instrument(binding) {
                    instruments.insert(oracle_key);
                }
            }
            linkages.entry(market_id).or_default().push(linkage);
        }
        if instruments.is_empty() {
            return Ok((linkages, HashMap::new()));
        }

        let lookback_secs = i64::try_from(crypto_lookback_secs(&spec.domain)).unwrap_or(i64::MAX);
        let delay_secs = i64::try_from(spec.domain.crypto.source_delay_secs).unwrap_or(i64::MAX);
        let from_ms = (spec.window_start
            - ChronoDuration::seconds(lookback_secs)
            - ChronoDuration::seconds(delay_secs))
        .timestamp_millis();
        let to_ms = spec.window_end.timestamp_millis();
        let rows = self
            .fact_read
            .domain_observations_between(instruments.into_iter().collect(), from_ms, to_ms)
            .await
            .map_err(QuantError::from)?;
        let mut domain_observations: HashMap<DomainInstrumentKey, Vec<DomainObservation>> =
            HashMap::new();
        for row in rows {
            // An unreadable persisted label means this build cannot interpret
            // the row — fail closed by skipping (the feature side then reports
            // DomainSourceUnavailable, never a fabricated value).
            if let Some(observation) = DomainObservation::from_clickhouse_row(&row) {
                domain_observations
                    .entry(observation.instrument_key.clone())
                    .or_default()
                    .push(observation);
            }
        }
        for series in domain_observations.values_mut() {
            series.sort_by_key(|observation| observation.observed_at);
        }
        Ok((linkages, domain_observations))
    }
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

/// Enumerate neg-risk leg sets for every event touched by the sample window.
///
/// Uses the persisted Gamma catalog snapshot (`EventInfo.catalog_market_ids`) —
/// the same source as online [`MarketRegistry::neg_risk_leg_set`] — so
/// `expected_legs` semantics match the live plane (Phase 11.2.1 train-serve parity).
async fn build_neg_risk_leg_sets(
    event_repo: &dyn EventRepository,
    sample_infos: &[Arc<MarketInfo>],
    markets_by_id: &HashMap<MarketId, Arc<MarketInfo>>,
) -> QuantResult<HashMap<EventId, NegRiskLegSet>> {
    let mut neg_risk_events: HashSet<EventId> = sample_infos
        .iter()
        .filter(|info| info.neg_risk)
        .map(|info| info.event_id.clone())
        .collect();
    for info in markets_by_id.values().filter(|info| info.neg_risk) {
        neg_risk_events.insert(info.event_id.clone());
    }
    let event_ids: Vec<EventId> = neg_risk_events.into_iter().collect();
    let events = event_repo
        .find_by_ids(&event_ids)
        .await
        .map_err(QuantError::from)?;
    let events_by_id: HashMap<EventId, _> = events
        .into_iter()
        .map(|event| (event.event_id.clone(), event))
        .collect();

    let mut neg_risk_leg_sets = HashMap::new();
    for event_id in event_ids {
        let Some(event) = events_by_id.get(&event_id) else {
            neg_risk_leg_sets.insert(event_id, NegRiskLegSet::empty());
            continue;
        };
        if !event.neg_risk {
            continue;
        }
        let catalog = event.catalog_market_ids.as_slice();
        let leg_set = NegRiskLegSet::from_catalog(catalog, |market_id| {
            markets_by_id.get(market_id).map(|info| {
                if info.neg_risk {
                    CatalogMarketLeg::NegRisk {
                        yes_token_id: info.yes_token_id.clone(),
                    }
                } else {
                    CatalogMarketLeg::NonNegRisk
                }
            })
        });
        neg_risk_leg_sets.insert(event_id, leg_set);
    }
    Ok(neg_risk_leg_sets)
}

/// Build the in-memory PIT engine from the prefetched window, returning the
/// engine and the number of book rows that failed to decode.
fn build_materialized_pit(
    prefetched: &Prefetched,
    max_staleness: Duration,
) -> (MaterializedPitEngine, u64) {
    let placeholder = epoch();
    let mut book_decode_failures = 0_u64;
    let mut books: HashMap<TokenId, Vec<BookSnapshotAt>> = HashMap::new();
    for (token, rows) in &prefetched.books {
        let series: Vec<BookSnapshotAt> = rows
            .iter()
            .filter_map(|row| {
                let (snapshot, status) = snapshot_from_row(row.clone(), placeholder);
                if status.counts_as_failure() {
                    book_decode_failures += 1;
                }
                snapshot
            })
            .collect();
        books.insert(token.clone(), series);
    }

    let mut markets: HashMap<MarketId, Vec<MarketContextAt>> = HashMap::new();
    for (market_id, info) in &prefetched.markets_by_id {
        let mut series = vec![market_context_entry(
            info,
            info.created_at,
            MarketStatus::Active,
        )];
        if let Some(latest) = prefetched.resolutions.get(market_id).and_then(|rows| {
            rows.iter()
                .max_by_key(|row| (row.resolved_at, row.observed_at))
        }) {
            series.push(market_context_entry(
                info,
                ms(latest.resolved_at),
                MarketStatus::Settled,
            ));
        }
        markets.insert(market_id.clone(), series);
    }

    (
        MaterializedPitEngine::new(books, markets, to_chrono(max_staleness)),
        book_decode_failures,
    )
}

/// One market-context series entry observed at `observed_at` with `status`.
fn market_context_entry(
    info: &MarketInfo,
    observed_at: DateTime<Utc>,
    status: MarketStatus,
) -> MarketContextAt {
    MarketContextAt {
        market_id: info.market_id.clone(),
        as_of: observed_at,
        observed_at,
        status,
        neg_risk: info.neg_risk,
        end_date: info.end_date,
        created_at: info.created_at,
        outcome_count: 2,
    }
}

/// Project a market catalog row into a selection entry (primary = YES token).
///
/// `liquidity_usd` is left `None`: the offline plane has no live selection
/// snapshot, so liquidity is sourced from the resolved feature vector instead
/// (mirroring the online context projection's fallback).
#[must_use]
pub fn selected_market(info: &MarketInfo) -> SelectedMarket {
    SelectedMarket {
        market_id: info.market_id.clone(),
        event_id: info.event_id.clone(),
        category: info.fee_category(),
        primary_token_id: info.yes_token_id.clone(),
        secondary_token_id: Some(info.no_token_id.clone()),
        liquidity_usd: None,
        volume_24h_usd: None,
        source_refs: Vec::new(),
    }
}

/// Build the trailing PIT feature window for one `(token, as_of)`.
#[must_use]
pub fn feature_window(
    token_id: TokenId,
    as_of: DateTime<Utc>,
    source_delay: Duration,
    lookback: Duration,
    rows: &[BookMicrostructureRow],
) -> MarketWindowSnapshot {
    let cutoff = as_of - to_chrono(source_delay);
    let start = cutoff - to_chrono(lookback);
    let buckets = rows
        .iter()
        .filter_map(|row| {
            let at = ms(row.bucket_time);
            (at >= start && at <= cutoff).then(|| bucket_from_row(row, at))
        })
        .collect();
    MarketWindowSnapshot {
        token_id,
        as_of,
        source_delay,
        buckets,
    }
}

/// Build the trailing PIT trade-tape window for one `(market, as_of)`.
#[must_use]
pub fn trade_tape_window(
    market_id: MarketId,
    as_of: DateTime<Utc>,
    source_delay: Duration,
    lookback: Duration,
    rows: &[TradeTapeRow],
) -> TradeTapeWindowSnapshot {
    let pit = TradeTapePitParams {
        trigger_time: as_of,
        source_delay,
        lookback,
    };
    let start = pit.cutoff() - to_chrono(lookback);
    let cutoff = pit.cutoff();
    let prints = rows
        .iter()
        .filter_map(|row| {
            let at = ms(row.event_time);
            (at >= start && at < cutoff).then(|| TradeTapePrint::from_clickhouse_row(row, at))
        })
        .collect();
    TradeTapeWindowSnapshot::available(market_id, as_of, source_delay, prints)
}

/// Build the strictly-forward label/settlement window for one `(token, as_of)`.
#[must_use]
pub fn forward_window(
    as_of: DateTime<Utc>,
    max_horizon_secs: u64,
    rows: &[BookMicrostructureRow],
    resolutions: &[MarketResolutionRow],
) -> ForwardWindow {
    let data_available_until = rows.last().map_or(as_of, |row| ms(row.bucket_time));
    let cap = as_of + ChronoDuration::seconds(i64::try_from(max_horizon_secs).unwrap_or(i64::MAX));
    let samples = rows
        .iter()
        .filter_map(|row| {
            let at = ms(row.bucket_time);
            (at > as_of && at <= cap).then(|| forward_sample(row, at))
        })
        .collect();
    // Settlement is independent of microstructure maturity: any resolution strictly
    // after `as_of` is visible to the settlement labeler.
    let resolution = resolutions
        .iter()
        .filter(|row| ms(row.resolved_at) > as_of)
        .max_by_key(|row| (row.resolved_at, row.observed_at))
        .map(|row| ResearchMarketResolution {
            winning_token_id: row.winning_token_id.clone(),
            resolved_at: ms(row.resolved_at),
            observed_at: ms(row.observed_at),
        });
    ForwardWindow {
        anchor: as_of,
        data_available_until,
        samples,
        resolution,
    }
}

/// Decode a microstructure row into a compute-domain bucket.
fn bucket_from_row(row: &BookMicrostructureRow, at: DateTime<Utc>) -> MicrostructureBucket {
    MicrostructureBucket {
        bucket_time: at,
        mid_close: row.mid_price_close.map(ChPrice::to_price),
        spread_bps_avg: row.spread_bps_avg.map(ChBps::to_bps),
        top1_depth_usd_avg: row.top1_depth_usd_avg.map(ChUsd::to_usd),
        top5_depth_usd_avg: row.top5_depth_usd_avg.map(ChUsd::to_usd),
        imbalance_avg: row.imbalance_avg.map(ChDecimal64::to_decimal),
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
        mid_close: row.mid_price_close.map(ChPrice::to_price),
        best_bid_high: row.best_bid_high.map(ChPrice::to_price),
        best_bid_low: row.best_bid_low.map(ChPrice::to_price),
        top1_depth_usd: row.top1_depth_usd_avg.map(ChUsd::to_usd),
    }
}

/// Convert a `std::time::Duration` into a saturating `chrono::Duration`.
#[must_use]
pub fn to_chrono(duration: Duration) -> ChronoDuration {
    ChronoDuration::from_std(duration).unwrap_or_else(|_| ChronoDuration::zero())
}

/// Convert epoch milliseconds to a UTC instant (epoch fallback on overflow).
#[must_use]
pub fn ms(timestamp_ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .unwrap_or_else(epoch)
}

/// The Unix epoch instant, used as an overflow/placeholder fallback.
#[must_use]
pub fn epoch() -> DateTime<Utc> {
    DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now)
}
