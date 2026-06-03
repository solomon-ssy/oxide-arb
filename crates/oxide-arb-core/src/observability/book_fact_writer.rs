//! Fire-and-forget CH writer for token-level book facts.

use crate::infra::async_writer::AsyncWriter;
use chrono::Utc;
use oxide_arb_models::{
    clickhouse::{
        BookSnapshotRow, ChBps, ChPrice, ChSchemaVersion, ChShares, ChUsd, TickEventL2Row,
        TickEventRow,
    },
    domain::{
        book::{BookLevel, BookSnapshot},
        pipeline::{BookSnapshotCmd, PriceDeltaCmd},
    },
    enums::clickhouse::{ChBookEventType, ChFactSource, ChSnapshotReason},
    enums::common::{Side, TickSize},
    enums::pipeline::ShardConnectionStatus,
    types::{MarketId, Price, TokenId},
};
use rust_decimal::Decimal;
use std::sync::Arc;

pub struct BookFactWriter {
    ticks: Arc<AsyncWriter<TickEventRow>>,
    l2: Arc<AsyncWriter<TickEventL2Row>>,
    snapshots: Arc<AsyncWriter<BookSnapshotRow>>,
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

impl BookFactWriter {
    pub const fn new(
        tick_writer: Arc<AsyncWriter<TickEventRow>>,
        l2_writer: Arc<AsyncWriter<TickEventL2Row>>,
        snapshot_writer: Arc<AsyncWriter<BookSnapshotRow>>,
    ) -> Self {
        Self {
            ticks: tick_writer,
            l2: l2_writer,
            snapshots: snapshot_writer,
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
        let l2 = TickEventL2Row {
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
            changed_levels_json: None,
            book_version,
            levels_count,
            is_full_snapshot: true,
            event_time: i64::try_from(cmd.timestamp_ms).unwrap_or(i64::MAX),
            ingestion_time,
            sequence: book_version,
            source: ChFactSource::WsSnapshot,
            schema_version: ChSchemaVersion(2),
        };
        self.l2.write(l2);
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
            source: ChFactSource::Scanner,
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
        let row = TickEventL2Row {
            token_id: cmd.asset_id.clone(),
            market_id,
            event_type: ChBookEventType::Delta,
            bid_prices,
            bid_sizes,
            ask_prices,
            ask_sizes,
            changed_levels_json: Some(
                serde_json::json!({ "changed_level_count": cmd.changes.len() }).to_string(),
            ),
            book_version,
            levels_count: u16::try_from(cmd.changes.len()).unwrap_or(u16::MAX),
            is_full_snapshot: false,
            event_time: i64::try_from(cmd.timestamp_ms).unwrap_or(i64::MAX),
            ingestion_time,
            sequence: book_version,
            source: ChFactSource::WsDelta,
            schema_version: ChSchemaVersion(2),
        };
        self.l2.write(row);
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

fn mid_price(best_bid: Option<Price>, best_ask: Option<Price>) -> Option<Price> {
    let bid = best_bid?;
    let ask = best_ask?;
    Some(Price::new((bid.inner() + ask.inner()) / Decimal::from(2)))
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
