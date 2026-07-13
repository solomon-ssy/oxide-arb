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
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, ChBps, ChDecimal64, ChPrice, ChUsd,
        MarketResolutionRow, TradeTapeRow,
    },
    domain::{
        CatalogWindowInfo, DecisionBoundary, DecisionClock, DecisionSource, DomainObservation,
        MarketLinkage, MarketRegistryInfo, TradeTapePrint,
    },
    enums::domain::DomainFamily,
    runtime_config::DomainConfig,
    types::{DomainInstrumentKey, MarketId, TokenId},
};
use quant_pivot_repository::traits::{
    CatalogVersionRepository, MarketLinkageRepository, QuantFactReadRepository,
};
use quant_pivot_research::{
    domain::{crypto_lookback_secs, oracle_instrument},
    features::{MarketWindowSnapshot, MicrostructureBucket, TradeTapeWindowSnapshot},
    pit::{BookSnapshotAt, MaterializedPitEngine},
    selection::SelectedMarket,
    training::{ForwardSample, ForwardWindow, MarketResolution as ResearchMarketResolution},
};

use crate::pit::platform::ch_historical::snapshot_from_row;

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
    pub knowledge_lag: Duration,
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
    /// Every immutable market/event revision required by the replay window.
    pub catalog: CatalogWindowInfo,
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
    catalog_repo: Arc<dyn CatalogVersionRepository>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    max_book_staleness: Duration,
}

impl HistoricalWindowLoader {
    /// Wire the loader from the fact reader, market catalog, linkage ledger,
    /// and staleness bound.
    #[must_use]
    pub fn new(
        fact_read: Arc<dyn QuantFactReadRepository>,
        catalog_repo: Arc<dyn CatalogVersionRepository>,
        linkage_repo: Arc<dyn MarketLinkageRepository>,
        max_book_staleness: Duration,
    ) -> Self {
        Self {
            fact_read,
            catalog_repo,
            linkage_repo,
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

        let end_boundary = window_end_boundary(spec)?;
        let catalog = self
            .catalog_repo
            .window_through(&markets, &end_boundary)
            .await
            .map_err(QuantError::from)?;
        let catalog_markets = decode_catalog_markets(&catalog)?;

        // Expand books to both binary outcome tokens named by every catalog
        // revision. The model context uses the exact PIT best ask for whichever
        // side it recommends; a NO quote is never synthesized as `1 - YES`.
        let mut book_tokens: Vec<TokenId> = tokens.clone();
        let mut seen_book_tokens: HashSet<TokenId> = seen_tokens.clone();
        for market in &catalog_markets {
            if seen_book_tokens.insert(market.token_yes.clone()) {
                book_tokens.push(market.token_yes.clone());
            }
            if seen_book_tokens.insert(market.token_no.clone()) {
                book_tokens.push(market.token_no.clone());
            }
        }

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

        let book_rows = self
            .fact_read
            .book_snapshots_between(book_tokens, book_from, book_to, book_to)
            .await
            .map_err(QuantError::from)?;
        let micro_rows = self
            .fact_read
            .microstructure_window(tokens.clone(), micro_from, micro_to, micro_to)
            .await
            .map_err(QuantError::from)?;
        let trade_rows = self
            .fact_read
            .trade_tape_window_by_market(markets.clone(), micro_from, micro_to, micro_to)
            .await
            .map_err(QuantError::from)?;
        let resolution_rows = self
            .fact_read
            .resolutions_between(markets.clone(), 0, resolution_to, resolution_to)
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

        let (linkages, domain_observations) = self
            .prefetch_domain(spec, &end_boundary, &markets, &catalog_markets)
            .await?;

        Ok(Prefetched {
            books,
            micro,
            trade_tape,
            resolutions,
            catalog,
            domain_observations,
            linkages,
        })
    }

    /// Prefetch the frozen linkage ledger and PIT domain observations for every
    /// category-mapped sample market (Phase 11.2.2).
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
    )> {
        let mapped_markets = mapped_domain_markets(spec, sample_markets, catalog_markets);
        if mapped_markets.is_empty() {
            return Ok((HashMap::new(), HashMap::new()));
        }

        let ledger_rows = self
            .linkage_repo
            .ledger_for_markets(&mapped_markets, end_boundary)
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

        let start_boundary = replay_boundary(
            spec.window_start,
            spec.knowledge_lag.as_secs(),
            spec.domain.crypto.availability_lag_secs,
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
        let rows = self
            .fact_read
            .domain_observations_between(
                instruments.into_iter().collect(),
                from_ms,
                to_ms,
                domain_cutoff.timestamp_millis(),
                end_boundary.decision_at().timestamp_millis(),
            )
            .await
            .map_err(QuantError::from)?;
        let mut domain_observations: HashMap<DomainInstrumentKey, Vec<DomainObservation>> =
            HashMap::new();
        for row in rows {
            let observation = DomainObservation::from_clickhouse_row(&row).ok_or_else(|| {
                ResearchError::PitResolution {
                    detail: format!(
                        "domain observation {} / {} at {} cannot be decoded",
                        row.instrument_key, row.metric, row.event_time
                    ),
                }
            })?;
            if observation.observed_at < domain_from
                || observation.observed_at > domain_cutoff
                || observation.publish_time > domain_cutoff
                || observation
                    .available_at
                    .is_none_or(|available_at| available_at > end_boundary.decision_at())
            {
                return Err(ResearchError::PitResolution {
                    detail: format!(
                        "domain observation {} at {} (published {}) is outside PIT prefetch window [{domain_from}, {domain_cutoff}]",
                        observation.instrument_key,
                        observation.observed_at,
                        observation.publish_time
                    ),
                }
                .into());
            }
            domain_observations
                .entry(observation.instrument_key.clone())
                .or_default()
                .push(observation);
        }
        for series in domain_observations.values_mut() {
            series.sort_by_key(|observation| observation.observed_at);
        }
        Ok((linkages, domain_observations))
    }
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
                    && DomainFamily::for_category(market.fee_category())
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

fn window_end_boundary(spec: &WindowSpec) -> QuantResult<DecisionBoundary> {
    if spec.knowledge_lag.subsec_nanos() != 0 {
        return Err(QuantError::config(
            "historical knowledge lag must be expressed in whole seconds",
        ));
    }
    replay_boundary(
        spec.window_end,
        spec.knowledge_lag.as_secs(),
        spec.domain.crypto.availability_lag_secs,
    )
}

/// Construct the sole frozen boundary for one offline replay decision.
///
/// Every reader consumes one of these recorded cutoffs. No downstream window
/// helper is permitted to subtract lag again.
pub fn replay_boundary(
    decision_at: DateTime<Utc>,
    knowledge_lag_secs: u64,
    domain_crypto_lag_secs: u64,
) -> QuantResult<DecisionBoundary> {
    DecisionClock::new(knowledge_lag_secs).serving_boundary(decision_at, domain_crypto_lag_secs)
}

fn decode_catalog_markets(catalog: &CatalogWindowInfo) -> QuantResult<Vec<MarketRegistryInfo>> {
    catalog
        .market_versions
        .iter()
        .map(|version| {
            serde_json::from_value(version.payload.clone()).map_err(|error| {
                ResearchError::PitResolution {
                    detail: format!(
                        "market catalog version {} payload is invalid: {error}",
                        version.market_catalog_version_id
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
            let source_cutoff = timestamp_millis(row.event_time, "book event_time")?;
            let decision_at = timestamp_millis(row.ingestion_time, "book ingestion_time")?;
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

/// Project a market catalog row into a selection entry (primary = YES token).
///
/// Liquidity and volume come from the exact PIT catalog version. The offline
/// plane never substitutes a live selection snapshot or re-derives these fields
/// from a different evidence source.
#[must_use]
pub fn selected_market(info: &MarketRegistryInfo) -> SelectedMarket {
    SelectedMarket {
        market_id: info.market_id.clone(),
        event_id: info.event_id.clone(),
        category: info.fee_category(),
        primary_token_id: info.token_yes.clone(),
        secondary_token_id: Some(info.token_no.clone()),
        liquidity_usd: info.liquidity_usd,
        volume_24h_usd: info.volume_24h,
        source_refs: Vec::new(),
    }
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
            decoded_resolutions.push((row, resolved_at, observed_at));
        }
    }
    let resolution = decoded_resolutions
        .into_iter()
        .max_by_key(|(_, resolved_at, observed_at)| (*resolved_at, *observed_at))
        .map(|(row, resolved_at, observed_at)| ResearchMarketResolution {
            winning_token_id: row.winning_token_id.clone(),
            resolved_at,
            observed_at,
        });
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
        best_bid_close: row.best_bid_close.map(ChPrice::to_price),
        top1_depth_usd: row.top1_depth_usd_avg.map(ChUsd::to_usd),
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

    use super::book_prefetch_start;

    #[test]
    fn book_prefetch_covers_non_zero_lag_before_the_earliest_cutoff() {
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
    fn book_prefetch_zero_lag_preserves_the_staleness_window() {
        let window_start = Utc
            .with_ymd_and_hms(2026, 7, 12, 12, 0, 0)
            .single()
            .expect("timestamp");

        let from = book_prefetch_start(window_start, Duration::ZERO, Duration::from_secs(10))
            .expect("prefetch start");

        assert_eq!(from, window_start - ChronoDuration::seconds(10));
    }
}
