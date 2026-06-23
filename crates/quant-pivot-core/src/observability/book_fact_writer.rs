//! Fire-and-forget CH writer for token-level book facts.

use chrono::Utc;
use dashmap::{DashMap, mapref::entry::Entry};
use quant_pivot_models::{
    clickhouse::{
        BookL2ReplayRow, BookMicrostructureRow, BookSnapshotRow, ChBps, ChDecimal64, ChPrice,
        ChSchemaVersion, ChShares, ChUsd, TickEventRow,
    },
    domain::{
        book::{BookLevel, BookSnapshot},
        pipeline::{BookSnapshotCmd, PriceDeltaCmd},
    },
    enums::clickhouse::{ChBookEventType, ChFactSource, ChSnapshotReason},
    enums::common::{Side, TickSize},
    enums::system::ShardConnectionStatus,
    hashing::CanonicalDigest,
    types::{MarketId, Price, TokenId, Usd},
};
use quant_pivot_storage::write::AsyncWriter;
use rust_decimal::Decimal;
use serde::Serialize;
use std::{mem, sync::Arc};

use super::{fact_lag::FactLagTracker, metrics_hub::MetricsHub};

pub struct BookFactWriter {
    ticks: Arc<AsyncWriter<TickEventRow>>,
    l2: Arc<AsyncWriter<BookL2ReplayRow>>,
    snapshots: Arc<AsyncWriter<BookSnapshotRow>>,
    microstructure_1s: Arc<AsyncWriter<BookMicrostructureRow>>,
    microstructure_pending: DashMap<TokenId, BookMicrostructureRow>,
    fact_lag: Arc<FactLagTracker>,
    metrics: Arc<MetricsHub>,
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
        fact_lag: Arc<FactLagTracker>,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            ticks: tick_writer,
            l2: l2_writer,
            snapshots: snapshot_writer,
            microstructure_1s: microstructure_1s_writer,
            microstructure_pending: DashMap::new(),
            fact_lag,
            metrics,
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

    fn record_fact_lag(&self, stream: &'static str, event_time_ms: i64, ingestion_time_ms: i64) {
        let event_ms = u64::try_from(event_time_ms.max(0)).unwrap_or(0);
        let ingestion_ms = u64::try_from(ingestion_time_ms.max(0)).unwrap_or(0);
        let lag_ms = ingestion_ms.saturating_sub(event_ms);
        self.fact_lag.record_ms(lag_ms);
        self.metrics.observe_fact_lag_ms(stream, lag_ms);
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
            schema_version: ChSchemaVersion(2),
        };
        self.record_fact_lag("book_l2_replay_hot", l2.event_time, ingestion_time);
        self.l2.write(l2);
        let snapshot = BookSnapshot::new(
            Arc::clone(&cmd.bids.levels),
            Arc::clone(&cmd.asks.levels),
            cmd.timestamp_ms,
            book_version,
        );
        self.write_microstructure_observation(
            &cmd.asset_id,
            market_id,
            &snapshot,
            ChBookEventType::Snapshot,
            0,
        );
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
            schema_version: ChSchemaVersion(2),
        };
        self.record_fact_lag("book_l2_replay_hot", row.event_time, ingestion_time);
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
        self.record_fact_lag(
            "tick_events",
            i64::try_from(timestamp_ms).unwrap_or(i64::MAX),
            Utc::now().timestamp_millis(),
        );
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
            schema_version: ChSchemaVersion(2),
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
            schema_version: ChSchemaVersion(2),
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
            schema_version: ChSchemaVersion(2),
        });
    }

    pub fn write_market_resolved(
        &self,
        market_id: &MarketId,
        winning_token_id: &TokenId,
        winning_outcome: &str,
        asset_ids: &[TokenId],
        timestamp_ms: u64,
    ) {
        let ingestion_time = Utc::now().timestamp_millis();
        for token_id in asset_ids {
            self.ticks.write(TickEventRow {
                token_id: token_id.clone(),
                market_id: Some(market_id.clone()),
                event_type: ChBookEventType::MarketResolved,
                best_bid: None,
                best_ask: None,
                last_trade_price: None,
                bid_depth_usd: None,
                ask_depth_usd: None,
                spread_bps: None,
                book_version: 0,
                raw_payload_json: Some(
                    serde_json::json!({
                        "market_id": market_id,
                        "winning_token_id": winning_token_id,
                        "winning_outcome": winning_outcome,
                    })
                    .to_string(),
                ),
                event_time: i64::try_from(timestamp_ms).unwrap_or(i64::MAX),
                ingestion_time,
                sequence: timestamp_ms,
                source: ChFactSource::WsMarketResolved,
                schema_version: ChSchemaVersion(2),
            });
        }
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
            schema_version: ChSchemaVersion(2),
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
        self.record_fact_lag("book_snapshots", input.event_time, input.ingestion_time);
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
            schema_version: ChSchemaVersion(2),
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
            self.record_fact_lag(
                "book_microstructure_1s",
                second_observation.bucket_time,
                now_ms,
            );
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
            self.record_fact_lag("book_microstructure_1s", row.bucket_time, now_ms);
            self.microstructure_1s.write(row);
        }
    }
}

fn merge_microstructure_row(current: &mut BookMicrostructureRow, next: BookMicrostructureRow) {
    current.market_id = next.market_id;
    current.best_bid_close = next.best_bid_close;
    current.best_ask_close = next.best_ask_close;
    current.spread_bps_avg = next.spread_bps_avg;
    current.mid_price_close = next.mid_price_close;
    current.top1_depth_usd_avg = next.top1_depth_usd_avg;
    current.top5_depth_usd_avg = next.top5_depth_usd_avg;
    current.top20_depth_usd_avg = next.top20_depth_usd_avg;
    current.imbalance_avg = next.imbalance_avg;
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
        schema_version: ChSchemaVersion(1),
    }
}

fn imbalance(snapshot: &BookSnapshot) -> Option<Decimal> {
    let bid = snapshot.total_bid_depth_usd.to_decimal();
    let ask = snapshot.total_ask_depth_usd.to_decimal();
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

fn snapshot_feed_event_hash(cmd: &BookSnapshotCmd) -> Option<String> {
    let payload = SnapshotHashPayload {
        event_type: "book_snapshot",
        token_id: cmd.asset_id.to_string(),
        event_time: cmd.timestamp_ms,
        bids: hash_levels(&cmd.bids.levels),
        asks: hash_levels(&cmd.asks.levels),
    };
    CanonicalDigest::blake3_json(&payload).ok()
}

fn delta_feed_event_hash(cmd: &PriceDeltaCmd) -> Option<String> {
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
    CanonicalDigest::blake3_json(&payload).ok()
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
