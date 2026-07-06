//! Fire-and-forget CH writer for token-level book facts.

use chrono::Utc;
use dashmap::{DashMap, mapref::entry::Entry};
use quant_pivot_models::{
    clickhouse::{
        BookL2ReplayRow, BookMicrostructureRow, BookSnapshotRow, ChBps, ChDecimal64, ChPrice,
        ChSchemaVersion, ChShares, ChUsd, MarketResolutionRow, TickEventRow,
    },
    domain::{
        book::{BookLevel, BookSnapshot, IMBALANCE_DEPTH_LEVELS, top_n_share_depth},
        pipeline::{BookSnapshotCmd, PriceDeltaCmd},
    },
    enums::clickhouse::{ChBookEventType, ChFactSource, ChSnapshotReason},
    enums::common::{Side, TickSize},
    enums::system::ShardConnectionStatus,
    hashing::CanonicalDigest,
    types::{ContentHash, MarketId, Price, TokenId, Usd},
};
use quant_pivot_storage::write::AsyncWriter;
use rust_decimal::Decimal;
use serde::Serialize;
use std::{mem, sync::Arc};

pub struct BookFactWriter {
    ticks: Arc<AsyncWriter<TickEventRow>>,
    l2: Arc<AsyncWriter<BookL2ReplayRow>>,
    snapshots: Arc<AsyncWriter<BookSnapshotRow>>,
    microstructure_1s: Arc<AsyncWriter<BookMicrostructureRow>>,
    resolutions: Arc<AsyncWriter<MarketResolutionRow>>,
    microstructure_pending: DashMap<TokenId, BookMicrostructureRow>,
}

struct SnapshotLevels<'a> {
    token_id: &'a TokenId,
    market_id: Option<MarketId>,
    reason: ChSnapshotReason,
    bids: &'a [BookLevel],
    asks: &'a [BookLevel],
    event_time: i64,
    ingestion_time: i64,
    book_version: u64,
    source: ChFactSource,
}

#[derive(Serialize)]
struct SnapshotHashPayload {
    event_type: &'static str,
    token_id: String,
    event_time: u64,
    bids: Vec<HashLevel>,
    asks: Vec<HashLevel>,
}

#[derive(Serialize)]
struct DeltaHashPayload {
    event_type: &'static str,
    token_id: String,
    event_time: u64,
    changes: Vec<HashChange>,
}

#[derive(Serialize)]
struct HashLevel {
    price: String,
    size: String,
}

#[derive(Serialize)]
struct HashChange {
    side: &'static str,
    price: String,
    size: String,
}

impl BookFactWriter {
    pub fn new(
        tick_writer: Arc<AsyncWriter<TickEventRow>>,
        l2_writer: Arc<AsyncWriter<BookL2ReplayRow>>,
        snapshot_writer: Arc<AsyncWriter<BookSnapshotRow>>,
        microstructure_1s_writer: Arc<AsyncWriter<BookMicrostructureRow>>,
        resolution_writer: Arc<AsyncWriter<MarketResolutionRow>>,
    ) -> Self {
        Self {
            ticks: tick_writer,
            l2: l2_writer,
            snapshots: snapshot_writer,
            microstructure_1s: microstructure_1s_writer,
            resolutions: resolution_writer,
            microstructure_pending: DashMap::new(),
        }
    }

    /// Enqueue all open microstructure second-buckets before analytics writers drain.
    pub fn flush_pending_microstructure(&self) {
        let keys: Vec<TokenId> = self
            .microstructure_pending
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        for key in keys {
            if let Some((_, row)) = self.microstructure_pending.remove(&key) {
                self.microstructure_1s.write(row);
            }
        }
    }

    pub fn write_snapshot(
        &self,
        cmd: &BookSnapshotCmd,
        market_id: Option<MarketId>,
        book_version: u64,
    ) {
        let ingestion_time = Utc::now().timestamp_millis();
        let levels_count =
            u16::try_from(cmd.bids.levels.len() + cmd.asks.levels.len()).unwrap_or(u16::MAX);
        let l2 = BookL2ReplayRow {
            token_id: cmd.asset_id.clone(),
            market_id: market_id.clone(),
            event_type: ChBookEventType::Snapshot,
            bid_prices: cmd
                .bids
                .levels
                .iter()
                .map(|level| ChPrice::from(level.price_decimal()))
                .collect(),
            bid_sizes: cmd
                .bids
                .levels
                .iter()
                .map(|level| ChShares::from(level.size_decimal()))
                .collect(),
            ask_prices: cmd
                .asks
                .levels
                .iter()
                .map(|level| ChPrice::from(level.price_decimal()))
                .collect(),
            ask_sizes: cmd
                .asks
                .levels
                .iter()
                .map(|level| ChShares::from(level.size_decimal()))
                .collect(),
            book_version,
            levels_count,
            is_full_snapshot: true,
            event_time: i64::try_from(cmd.timestamp_ms).unwrap_or(i64::MAX),
            ingestion_time,
            sequence: book_version,
            source: ChFactSource::WsSnapshot,
            feed_event_hash: snapshot_feed_event_hash(cmd),
            schema_version: ChSchemaVersion::FIRST,
        };
        self.l2.write(l2);
        let snapshot = BookSnapshot::new(
            Arc::clone(&cmd.bids.levels),
            Arc::clone(&cmd.asks.levels),
            cmd.timestamp_ms,
            book_version,
        );
        self.write_microstructure_observation(
            &cmd.asset_id,
            market_id.clone(),
            &snapshot,
            ChBookEventType::Snapshot,
            0,
        );
        self.write_snapshot_levels(SnapshotLevels {
            token_id: &cmd.asset_id,
            market_id,
            reason: ChSnapshotReason::WsSnapshot,
            bids: &cmd.bids.levels,
            asks: &cmd.asks.levels,
            event_time: i64::try_from(cmd.timestamp_ms).unwrap_or(i64::MAX),
            ingestion_time,
            book_version,
            source: ChFactSource::WsSnapshot,
        });
    }

    pub fn write_published_snapshot(
        &self,
        token_id: &TokenId,
        market_id: Option<MarketId>,
        reason: ChSnapshotReason,
        snapshot: &BookSnapshot,
    ) {
        let now_ms = Utc::now().timestamp_millis();
        self.write_snapshot_levels(SnapshotLevels {
            token_id,
            market_id,
            reason,
            bids: &snapshot.bids,
            asks: &snapshot.asks,
            event_time: i64::try_from(snapshot.timestamp_ms).unwrap_or(i64::MAX),
            ingestion_time: now_ms,
            book_version: snapshot.version,
            source: ChFactSource::QuantPipeline,
        });
    }

    pub fn write_delta(&self, cmd: &PriceDeltaCmd, market_id: Option<MarketId>, book_version: u64) {
        let ingestion_time = Utc::now().timestamp_millis();
        let mut bid_prices = Vec::new();
        let mut bid_sizes = Vec::new();
        let mut ask_prices = Vec::new();
        let mut ask_sizes = Vec::new();
        for change in cmd.changes.iter() {
            if change.side == Side::Buy {
                bid_prices.push(ChPrice::from(change.price));
                bid_sizes.push(ChShares::from(change.size));
            } else {
                ask_prices.push(ChPrice::from(change.price));
                ask_sizes.push(ChShares::from(change.size));
            }
        }
        let row = BookL2ReplayRow {
            token_id: cmd.asset_id.clone(),
            market_id,
            event_type: ChBookEventType::Delta,
            bid_prices,
            bid_sizes,
            ask_prices,
            ask_sizes,
            book_version,
            levels_count: u16::try_from(cmd.changes.len()).unwrap_or(u16::MAX),
            is_full_snapshot: false,
            event_time: i64::try_from(cmd.timestamp_ms).unwrap_or(i64::MAX),
            ingestion_time,
            sequence: book_version,
            source: ChFactSource::WsDelta,
            feed_event_hash: delta_feed_event_hash(cmd),
            schema_version: ChSchemaVersion::FIRST,
        };
        self.l2.write(row);
        // Full-book microstructure must be generated from the published
        // snapshot after deltas are applied; callers pass it via
        // `write_microstructure_snapshot`.
    }

    pub fn write_microstructure_snapshot(
        &self,
        token_id: &TokenId,
        market_id: Option<MarketId>,
        snapshot: &BookSnapshot,
        event_type: ChBookEventType,
        delete_count: u64,
    ) {
        self.write_microstructure_observation(
            token_id,
            market_id,
            snapshot,
            event_type,
            delete_count,
        );
    }

    pub fn write_bbo(
        &self,
        token_id: &TokenId,
        market_id: Option<MarketId>,
        best_bid: Price,
        best_ask: Price,
        timestamp_ms: u64,
    ) {
        self.ticks.write(TickEventRow {
            token_id: token_id.clone(),
            market_id,
            event_type: ChBookEventType::Bbo,
            best_bid: Some(ChPrice::from(best_bid)),
            best_ask: Some(ChPrice::from(best_ask)),
            last_trade_price: None,
            bid_depth_usd: None,
            ask_depth_usd: None,
            spread_bps: spread_bps(Some(best_bid), Some(best_ask)).map(ChBps::from),
            book_version: 0,
            raw_payload_json: None,
            event_time: i64::try_from(timestamp_ms).unwrap_or(i64::MAX),
            ingestion_time: Utc::now().timestamp_millis(),
            sequence: timestamp_ms,
            source: ChFactSource::WsBbo,
            schema_version: ChSchemaVersion::FIRST,
        });
    }

    pub fn write_tick_size_change(
        &self,
        token_id: &TokenId,
        market_id: Option<MarketId>,
        old_tick: TickSize,
        new_tick: TickSize,
    ) {
        let now_ms = Utc::now().timestamp_millis();
        self.ticks.write(TickEventRow {
            token_id: token_id.clone(),
            market_id,
            event_type: ChBookEventType::TickSizeChange,
            best_bid: None,
            best_ask: None,
            last_trade_price: None,
            bid_depth_usd: None,
            ask_depth_usd: None,
            spread_bps: None,
            book_version: 0,
            raw_payload_json: Some(
                serde_json::json!({
                    "old_tick": old_tick,
                    "new_tick": new_tick,
                })
                .to_string(),
            ),
            event_time: now_ms,
            ingestion_time: now_ms,
            sequence: u64::try_from(now_ms).unwrap_or(u64::MAX),
            source: ChFactSource::WsTickSize,
            schema_version: ChSchemaVersion::FIRST,
        });
    }

    pub fn write_last_trade(
        &self,
        token_id: &TokenId,
        market_id: Option<MarketId>,
        price: Price,
        timestamp_ms: u64,
    ) {
        self.ticks.write(TickEventRow {
            token_id: token_id.clone(),
            market_id,
            event_type: ChBookEventType::LastTrade,
            best_bid: None,
            best_ask: None,
            last_trade_price: Some(ChPrice::from(price)),
            bid_depth_usd: None,
            ask_depth_usd: None,
            spread_bps: None,
            book_version: 0,
            raw_payload_json: Some(serde_json::json!({ "price": price }).to_string()),
            event_time: i64::try_from(timestamp_ms).unwrap_or(i64::MAX),
            ingestion_time: Utc::now().timestamp_millis(),
            sequence: timestamp_ms,
            source: ChFactSource::WsLastTrade,
            schema_version: ChSchemaVersion::FIRST,
        });
    }

    /// Append the authoritative settlement event to the typed
    /// `market_resolution_event` fact — the single point-in-time settlement truth
    /// source consumed by training labels, backtest realized `PnL`, and historical
    /// market-status resolution. The settlement key is `winning_token_id`
    /// (label-agnostic); `winning_outcome` is informational only.
    pub fn write_market_resolved(
        &self,
        market_id: &MarketId,
        winning_token_id: &TokenId,
        winning_outcome: &str,
        asset_ids: &[TokenId],
        timestamp_ms: u64,
    ) {
        let resolved_at = i64::try_from(timestamp_ms).unwrap_or(i64::MAX);
        let observed_at = Utc::now().timestamp_millis();
        self.resolutions.write(MarketResolutionRow {
            market_id: market_id.clone(),
            winning_token_id: winning_token_id.clone(),
            winning_outcome: winning_outcome.to_owned(),
            asset_token_ids: asset_ids.to_vec(),
            resolved_at,
            observed_at,
            sequence: timestamp_ms,
            source: ChFactSource::WsMarketResolved,
            schema_version: ChSchemaVersion::FIRST,
        });
    }

    pub fn write_shard_status(&self, shard_id: usize, status: ShardConnectionStatus) {
        let now_ms = Utc::now().timestamp_millis();
        self.ticks.write(TickEventRow {
            token_id: TokenId::new(format!("__ws_shard_{shard_id}")),
            market_id: None,
            event_type: ChBookEventType::ShardStatus,
            best_bid: None,
            best_ask: None,
            last_trade_price: None,
            bid_depth_usd: None,
            ask_depth_usd: None,
            spread_bps: None,
            book_version: 0,
            raw_payload_json: Some(
                serde_json::json!({
                    "shard_id": shard_id,
                    "status": status,
                })
                .to_string(),
            ),
            event_time: now_ms,
            ingestion_time: now_ms,
            sequence: u64::try_from(now_ms).unwrap_or(u64::MAX),
            source: ChFactSource::WsShardStatus,
            schema_version: ChSchemaVersion::FIRST,
        });
    }

    fn write_snapshot_levels(&self, input: SnapshotLevels<'_>) {
        let bids_json = levels_to_pairs(input.bids);
        let asks_json = levels_to_pairs(input.asks);
        let bid_depth_usd = depth_usd(input.bids);
        let ask_depth_usd = depth_usd(input.asks);
        let best_bid = input.bids.first().map(|level| level.price_decimal());
        let best_ask = input.asks.first().map(|level| level.price_decimal());
        let levels_count = u16::try_from(input.bids.len() + input.asks.len()).unwrap_or(u16::MAX);
        self.snapshots.write(BookSnapshotRow {
            token_id: input.token_id.clone(),
            market_id: input.market_id,
            snapshot_reason: input.reason,
            top_n: levels_count,
            bids_json: serde_json::to_string(&bids_json).unwrap_or_else(|_| "[]".to_owned()),
            asks_json: serde_json::to_string(&asks_json).unwrap_or_else(|_| "[]".to_owned()),
            bid_depth_usd: Some(ChUsd::from(bid_depth_usd)),
            ask_depth_usd: Some(ChUsd::from(ask_depth_usd)),
            mid_price: mid_price(best_bid, best_ask).map(ChPrice::from),
            spread_bps: spread_bps(best_bid, best_ask).map(ChBps::from),
            book_version: input.book_version,
            levels_count,
            event_time: input.event_time,
            ingestion_time: input.ingestion_time,
            sequence: input.book_version,
            source: input.source,
            schema_version: ChSchemaVersion::FIRST,
        });
    }

    fn write_microstructure_observation(
        &self,
        token_id: &TokenId,
        market_id: Option<MarketId>,
        snapshot: &BookSnapshot,
        event_type: ChBookEventType,
        delete_count: u64,
    ) {
        let now_ms = Utc::now().timestamp_millis();
        let second_observation = microstructure_row(
            token_id,
            market_id,
            snapshot,
            event_type,
            delete_count,
            bucket_ms(now_ms, 1_000),
        );
        if event_type == ChBookEventType::Snapshot {
            self.microstructure_1s.write(second_observation);
            return;
        }

        let mut stale_bucket = None;
        match self.microstructure_pending.entry(token_id.clone()) {
            Entry::Occupied(mut entry) => {
                if entry.get().bucket_time == second_observation.bucket_time {
                    merge_microstructure_row(entry.get_mut(), second_observation);
                } else {
                    stale_bucket = Some(mem::replace(entry.get_mut(), second_observation));
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(second_observation);
            }
        }
        if let Some(row) = stale_bucket {
            self.microstructure_1s.write(row);
        }
    }
}

/// Incremental weighted mean of an optional `Decimal64` bucket field.
///
/// `None` contributes nothing (empty-book observations do not skew the mean);
/// otherwise each side's running mean is weighted by its observation count so
/// folding single-observation rows yields a correct arithmetic mean.
/// Smaller of two optional samples (`None` is the identity).
fn opt_min(a: Option<Decimal>, b: Option<Decimal>) -> Option<Decimal> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

/// Larger of two optional samples (`None` is the identity).
fn opt_max(a: Option<Decimal>, b: Option<Decimal>) -> Option<Decimal> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

/// Observation-count-weighted mean of two optional samples so that folding
/// single-observation rows yields a correct arithmetic mean (`None` = no data,
/// contributes nothing).
fn opt_weighted_mean(
    a: Option<Decimal>,
    a_count: u64,
    b: Option<Decimal>,
    b_count: u64,
) -> Option<Decimal> {
    match (a, b) {
        (Some(x), Some(y)) => {
            let xw = Decimal::from(a_count.max(1));
            let yw = Decimal::from(b_count.max(1));
            Some((x * xw + y * yw) / (xw + yw))
        }
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

fn merge_mean_decimal(
    current: Option<ChDecimal64>,
    current_count: u64,
    next: Option<ChDecimal64>,
    next_count: u64,
) -> Option<ChDecimal64> {
    opt_weighted_mean(
        current.map(ChDecimal64::to_decimal),
        current_count,
        next.map(ChDecimal64::to_decimal),
        next_count,
    )
    .map(ChDecimal64::from)
}

/// Merge a pending 1s bucket with a later same-second observation, producing an
/// honest OHLC + mean aggregation: `open` stays first, `close` takes the latest,
/// `high`/`low`/`min`/`max` track real extrema, and every `_avg` is a true
/// observation-weighted mean (never "last value wins").
fn merge_microstructure_row(current: &mut BookMicrostructureRow, next: BookMicrostructureRow) {
    let prior_count = current.update_count;
    let next_count = next.update_count;
    current.market_id = next.market_id;

    // Best-bid / best-ask OHLC: open untouched (first), close = latest.
    current.best_bid_high = opt_max(
        current.best_bid_high.map(|v| v.to_price().inner()),
        next.best_bid_high.map(|v| v.to_price().inner()),
    )
    .map(|d| ChPrice::from(Price::new(d)));
    current.best_bid_low = opt_min(
        current.best_bid_low.map(|v| v.to_price().inner()),
        next.best_bid_low.map(|v| v.to_price().inner()),
    )
    .map(|d| ChPrice::from(Price::new(d)));
    current.best_bid_close = next.best_bid_close;
    current.best_ask_high = opt_max(
        current.best_ask_high.map(|v| v.to_price().inner()),
        next.best_ask_high.map(|v| v.to_price().inner()),
    )
    .map(|d| ChPrice::from(Price::new(d)));
    current.best_ask_low = opt_min(
        current.best_ask_low.map(|v| v.to_price().inner()),
        next.best_ask_low.map(|v| v.to_price().inner()),
    )
    .map(|d| ChPrice::from(Price::new(d)));
    current.best_ask_close = next.best_ask_close;
    current.mid_price_close = next.mid_price_close;

    // Spread band: real min / max, true mean.
    current.spread_bps_min = opt_min(
        current.spread_bps_min.map(|v| v.to_bps().inner()),
        next.spread_bps_min.map(|v| v.to_bps().inner()),
    )
    .map(ChBps::from);
    current.spread_bps_max = opt_max(
        current.spread_bps_max.map(|v| v.to_bps().inner()),
        next.spread_bps_max.map(|v| v.to_bps().inner()),
    )
    .map(ChBps::from);
    current.spread_bps_avg = opt_weighted_mean(
        current.spread_bps_avg.map(|v| v.to_bps().inner()),
        prior_count,
        next.spread_bps_avg.map(|v| v.to_bps().inner()),
        next_count,
    )
    .map(ChBps::from);

    // Depth (`_avg`) and imbalance: true observation-weighted means, so the
    // `_avg` column name is honest instead of "last value wins".
    current.top1_depth_usd_avg = opt_weighted_mean(
        current.top1_depth_usd_avg.map(|v| v.to_usd().inner()),
        prior_count,
        next.top1_depth_usd_avg.map(|v| v.to_usd().inner()),
        next_count,
    )
    .map(ChUsd::from);
    current.top5_depth_usd_avg = opt_weighted_mean(
        current.top5_depth_usd_avg.map(|v| v.to_usd().inner()),
        prior_count,
        next.top5_depth_usd_avg.map(|v| v.to_usd().inner()),
        next_count,
    )
    .map(ChUsd::from);
    current.top20_depth_usd_avg = opt_weighted_mean(
        current.top20_depth_usd_avg.map(|v| v.to_usd().inner()),
        prior_count,
        next.top20_depth_usd_avg.map(|v| v.to_usd().inner()),
        next_count,
    )
    .map(ChUsd::from);
    current.imbalance_avg = merge_mean_decimal(
        current.imbalance_avg,
        prior_count,
        next.imbalance_avg,
        next_count,
    );
    current.update_count = current.update_count.saturating_add(next.update_count);
    current.snapshot_count = current.snapshot_count.saturating_add(next.snapshot_count);
    current.delta_count = current.delta_count.saturating_add(next.delta_count);
    current.delete_count = current.delete_count.saturating_add(next.delete_count);
    current.crossed_count = current.crossed_count.saturating_add(next.crossed_count);
    current.invalid_level_count = current
        .invalid_level_count
        .saturating_add(next.invalid_level_count);
    current.gap_count = current.gap_count.saturating_add(next.gap_count);
    current.last_trade_count = current
        .last_trade_count
        .saturating_add(next.last_trade_count);
    current.max_book_age_ms = current.max_book_age_ms.max(next.max_book_age_ms);
}

fn levels_to_pairs(levels: &[BookLevel]) -> Vec<(String, String)> {
    levels
        .iter()
        .map(|level| {
            let price = level.price_decimal().inner().to_string();
            let size = level.size_decimal().inner().to_string();
            (price, size)
        })
        .collect()
}

fn depth_usd(levels: &[BookLevel]) -> Decimal {
    levels.iter().fold(Decimal::ZERO, |acc, level| {
        acc + level.depth_usd().to_decimal()
    })
}

fn top_depth_usd(levels: &[BookLevel], top_n: usize) -> Decimal {
    levels.iter().take(top_n).fold(Decimal::ZERO, |acc, level| {
        acc + level.depth_usd().to_decimal()
    })
}

fn both_side_top_depth(snapshot: &BookSnapshot, top_n: usize) -> Decimal {
    top_depth_usd(&snapshot.bids, top_n) + top_depth_usd(&snapshot.asks, top_n)
}

fn microstructure_row(
    token_id: &TokenId,
    market_id: Option<MarketId>,
    snapshot: &BookSnapshot,
    event_type: ChBookEventType,
    delete_count: u64,
    bucket_time: i64,
) -> BookMicrostructureRow {
    let best_bid = snapshot.best_bid();
    let best_ask = snapshot.best_ask();
    let spread = spread_bps(best_bid, best_ask);
    let mid = mid_price(best_bid, best_ask);
    let crossed = matches!((best_bid, best_ask), (Some(bid), Some(ask)) if bid >= ask);
    BookMicrostructureRow {
        token_id: token_id.clone(),
        market_id,
        bucket_time,
        best_bid_open: best_bid.map(ChPrice::from),
        best_bid_high: best_bid.map(ChPrice::from),
        best_bid_low: best_bid.map(ChPrice::from),
        best_bid_close: best_bid.map(ChPrice::from),
        best_ask_open: best_ask.map(ChPrice::from),
        best_ask_high: best_ask.map(ChPrice::from),
        best_ask_low: best_ask.map(ChPrice::from),
        best_ask_close: best_ask.map(ChPrice::from),
        spread_bps_min: spread.map(ChBps::from),
        spread_bps_avg: spread.map(ChBps::from),
        spread_bps_max: spread.map(ChBps::from),
        mid_price_open: mid.map(ChPrice::from),
        mid_price_close: mid.map(ChPrice::from),
        top1_depth_usd_avg: Some(ChUsd::from(Usd::new(both_side_top_depth(snapshot, 1)))),
        top5_depth_usd_avg: Some(ChUsd::from(Usd::new(both_side_top_depth(snapshot, 5)))),
        top20_depth_usd_avg: Some(ChUsd::from(Usd::new(both_side_top_depth(snapshot, 20)))),
        imbalance_avg: imbalance(snapshot).map(ChDecimal64::from),
        update_count: 1,
        snapshot_count: u64::from(event_type == ChBookEventType::Snapshot),
        delta_count: u64::from(event_type == ChBookEventType::Delta),
        delete_count,
        crossed_count: u64::from(crossed),
        invalid_level_count: 0,
        gap_count: 0,
        last_trade_count: 0,
        max_book_age_ms: u64::try_from(Utc::now().timestamp_millis())
            .unwrap_or(0)
            .saturating_sub(snapshot.timestamp_ms),
        schema_version: ChSchemaVersion::FIRST,
    }
}

/// Top-N share-weighted queue imbalance `(bid - ask) / (bid + ask)` in `[-1, 1]`.
///
/// Uses near-touch share depth (best [`IMBALANCE_DEPTH_LEVELS`] levels per side),
/// not full-book USD notional: USD weighting is structurally ask-biased (ask
/// prices > bid prices) and full-book summation is dominated by deep resting
/// liquidity, both of which destroy the signal's meaning. Positive = bid-heavy.
fn imbalance(snapshot: &BookSnapshot) -> Option<Decimal> {
    let bid = top_n_share_depth(&snapshot.bids, IMBALANCE_DEPTH_LEVELS).inner();
    let ask = top_n_share_depth(&snapshot.asks, IMBALANCE_DEPTH_LEVELS).inner();
    let total = bid + ask;
    if total.is_zero() {
        return None;
    }
    Some((bid - ask) / total)
}

const fn bucket_ms(timestamp_ms: i64, interval_ms: i64) -> i64 {
    timestamp_ms - timestamp_ms.rem_euclid(interval_ms)
}

fn mid_price(best_bid: Option<Price>, best_ask: Option<Price>) -> Option<Price> {
    let bid = best_bid?;
    let ask = best_ask?;
    Some(Price::new((bid.inner() + ask.inner()) / Decimal::from(2)))
}

fn snapshot_feed_event_hash(cmd: &BookSnapshotCmd) -> Option<ContentHash> {
    let payload = SnapshotHashPayload {
        event_type: "book_snapshot",
        token_id: cmd.asset_id.to_string(),
        event_time: cmd.timestamp_ms,
        bids: hash_levels(&cmd.bids.levels),
        asks: hash_levels(&cmd.asks.levels),
    };
    CanonicalDigest::content_hash_json(&payload).ok()
}

fn delta_feed_event_hash(cmd: &PriceDeltaCmd) -> Option<ContentHash> {
    let changes = cmd
        .changes
        .iter()
        .map(|change| HashChange {
            side: change.side.as_str(),
            price: change.price.to_string(),
            size: change.size.to_string(),
        })
        .collect();
    let payload = DeltaHashPayload {
        event_type: "price_delta",
        token_id: cmd.asset_id.to_string(),
        event_time: cmd.timestamp_ms,
        changes,
    };
    CanonicalDigest::content_hash_json(&payload).ok()
}

fn hash_levels(levels: &[BookLevel]) -> Vec<HashLevel> {
    levels
        .iter()
        .map(|level| HashLevel {
            price: level.price_decimal().to_string(),
            size: level.size_decimal().to_string(),
        })
        .collect()
}

fn spread_bps(best_bid: Option<Price>, best_ask: Option<Price>) -> Option<Decimal> {
    let bid = best_bid?;
    let ask = best_ask?;
    let mid = mid_price(Some(bid), Some(ask))?;
    if mid.is_zero() {
        return None;
    }
    Some((ask.inner() - bid.inner()) / mid.inner() * Decimal::from(10_000))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_pivot_models::types::Shares;

    fn level(price: i64, size: i64) -> BookLevel {
        BookLevel::from_decimal_unchecked(
            Price::new(Decimal::new(price, 2)),
            Shares::new(Decimal::from(size)),
        )
    }

    fn snapshot(bids: &[BookLevel], asks: &[BookLevel]) -> BookSnapshot {
        BookSnapshot::new(Arc::from(bids), Arc::from(asks), 0, 0)
    }

    #[test]
    fn imbalance_is_share_weighted_not_usd_biased() {
        // Low-mid book with EQUAL shares per side. The old full-book USD formula
        // returned a large negative value (ask price ≫ bid price); the top-N
        // share formula is zero — the correct "balanced" reading.
        let snap = snapshot(&[level(5, 10)], &[level(95, 10)]);
        assert_eq!(imbalance(&snap), Some(Decimal::ZERO));
    }

    #[test]
    fn imbalance_is_minus_one_when_bids_empty() {
        let snap = snapshot(&[], &[level(60, 10)]);
        assert_eq!(imbalance(&snap), Some(Decimal::NEGATIVE_ONE));
    }

    #[test]
    fn imbalance_is_plus_one_when_asks_empty() {
        let snap = snapshot(&[level(40, 10)], &[]);
        assert_eq!(imbalance(&snap), Some(Decimal::ONE));
    }

    #[test]
    fn imbalance_none_when_both_sides_empty() {
        let snap = snapshot(&[], &[]);
        assert_eq!(imbalance(&snap), None);
    }

    #[test]
    fn opt_weighted_mean_respects_observation_counts() {
        // 0.2 over 3 obs merged with 0.6 over 1 obs → (0.2*3 + 0.6*1)/4 = 0.3.
        assert_eq!(
            opt_weighted_mean(Some(Decimal::new(2, 1)), 3, Some(Decimal::new(6, 1)), 1),
            Some(Decimal::new(3, 1)),
        );
        assert_eq!(
            opt_weighted_mean(Some(Decimal::new(5, 1)), 9, None, 1),
            Some(Decimal::new(5, 1)),
        );
        assert_eq!(opt_weighted_mean(None, 1, None, 1), None);
    }

    #[test]
    fn opt_min_max_track_extrema_over_none() {
        assert_eq!(
            opt_min(Some(Decimal::TEN), Some(Decimal::ONE)),
            Some(Decimal::ONE)
        );
        assert_eq!(
            opt_max(Some(Decimal::TEN), Some(Decimal::ONE)),
            Some(Decimal::TEN)
        );
        assert_eq!(opt_min(None, Some(Decimal::ONE)), Some(Decimal::ONE));
        assert_eq!(opt_max(Some(Decimal::TEN), None), Some(Decimal::TEN));
    }

    #[test]
    fn merge_mean_decimal_is_a_true_running_mean() {
        let current = Some(ChDecimal64::from(Decimal::new(2, 1))); // 0.2, 1 obs
        let next = Some(ChDecimal64::from(Decimal::new(6, 1))); // 0.6, 1 obs
        let merged = merge_mean_decimal(current, 1, next, 1).expect("some");
        assert_eq!(merged.to_decimal(), Decimal::new(4, 1)); // 0.4
    }

    #[test]
    fn merge_mean_decimal_ignores_none_contributions() {
        let value = Some(ChDecimal64::from(Decimal::new(3, 1)));
        assert_eq!(
            merge_mean_decimal(value, 5, None, 1).map(ChDecimal64::to_decimal),
            Some(Decimal::new(3, 1)),
        );
        assert_eq!(
            merge_mean_decimal(None, 5, value, 1).map(ChDecimal64::to_decimal),
            Some(Decimal::new(3, 1)),
        );
    }
}
