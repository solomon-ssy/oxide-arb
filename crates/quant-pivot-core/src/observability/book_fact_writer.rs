//! Fire-and-forget CH writer for token-level book facts.

use chrono::Utc;
use dashmap::{DashMap, mapref::entry::Entry};
use quant_pivot_models::{
    clickhouse::{
        BookL2CheckpointRow, BookL2EventRow, BookMicrostructureRow, BookStreamSessionRow, ChBps,
        ChDecimal64, ChPrice, ChSchemaVersion, ChShares, ChUsd, MarketResolutionRow, TradeTapeRow,
    },
    domain::{
        book::{BookLevel, BookSnapshot, IMBALANCE_DEPTH_LEVELS, top_n_share_depth},
        pipeline::{BookSnapshotCmd, IngressTrace, PriceDeltaCmd},
        trade_tape::trade_tape_coverage,
    },
    enums::clickhouse::{
        ChBookEventType, ChCanonicalBookEventType, ChFactSource, ChStreamSessionEndReason,
        ChStreamSessionState, ChTradeParticipantRole, ChTradeReconciliationStatus, ChTradeSide,
        ChTradeTapeSource,
    },
    enums::common::{Side, TickSize},
    hashing::CanonicalDigest,
    types::{ContentHash, MarketId, Price, Shares, TokenId, Usd},
};
use quant_pivot_storage::write::{AsyncWriter, DurableWriter};
use rust_decimal::Decimal;
use serde::Serialize;
use std::{mem, sync::Arc, time::Duration};
use uuid::Uuid;

const CANONICAL_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const L2_SCHEMA_VERSION: ChSchemaVersion = ChSchemaVersion(2);

pub struct BookFactWriter {
    l2: Arc<DurableWriter<BookL2EventRow>>,
    checkpoints: Arc<DurableWriter<BookL2CheckpointRow>>,
    sessions: Arc<DurableWriter<BookStreamSessionRow>>,
    trades: Arc<DurableWriter<TradeTapeRow>>,
    microstructure_1s: Arc<AsyncWriter<BookMicrostructureRow>>,
    resolutions: Arc<AsyncWriter<MarketResolutionRow>>,
    microstructure_pending: DashMap<TokenId, BookMicrostructureRow>,
}

pub(crate) struct MarketWsTradeFact<'a> {
    pub token_id: &'a TokenId,
    pub market_id: MarketId,
    pub price: Price,
    pub side: Option<Side>,
    pub trade_size: Option<Shares>,
    pub fee_rate_bps: Option<Decimal>,
    pub timestamp_ms: u64,
    pub trace: IngressTrace,
}

struct SnapshotLevels<'a> {
    token_id: &'a TokenId,
    market_id: Option<MarketId>,
    bids: &'a [BookLevel],
    asks: &'a [BookLevel],
    event_time: i64,
    stream_session_id: Uuid,
    token_sequence: u64,
    book_version: u64,
    source_event_hash: ContentHash,
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
        l2_writer: Arc<DurableWriter<BookL2EventRow>>,
        checkpoint_writer: Arc<DurableWriter<BookL2CheckpointRow>>,
        session_writer: Arc<DurableWriter<BookStreamSessionRow>>,
        trade_writer: Arc<DurableWriter<TradeTapeRow>>,
        microstructure_1s_writer: Arc<AsyncWriter<BookMicrostructureRow>>,
        resolution_writer: Arc<AsyncWriter<MarketResolutionRow>>,
    ) -> Self {
        Self {
            l2: l2_writer,
            checkpoints: checkpoint_writer,
            sessions: session_writer,
            trades: trade_writer,
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

    pub fn write_snapshot_event(
        &self,
        cmd: &BookSnapshotCmd,
        market_id: Option<MarketId>,
    ) -> Option<ContentHash> {
        let payload_hash = snapshot_feed_event_hash(cmd)?;
        let persisted_time = Utc::now().timestamp_millis();
        let l2 = BookL2EventRow {
            stream_session_id: cmd.trace.stream_session_id,
            shard_id: cmd.trace.shard_id,
            token_id: cmd.asset_id.clone(),
            market_id,
            token_sequence: cmd.trace.token_sequence,
            event_type: ChCanonicalBookEventType::Snapshot,
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
            book_version: cmd.trace.token_sequence,
            old_tick_size: None,
            new_tick_size: None,
            venue_event_time: i64::try_from(cmd.timestamp_ms).unwrap_or(i64::MAX),
            ingress_time: cmd.trace.ingress_time_ms,
            persisted_time,
            payload_hash: payload_hash.clone(),
            schema_version: L2_SCHEMA_VERSION,
        };
        self.l2
            .write_timeout(l2, CANONICAL_WRITE_TIMEOUT)
            .ok()
            .map(|()| payload_hash)
    }

    pub fn write_checkpoint(
        &self,
        token_id: &TokenId,
        market_id: Option<MarketId>,
        snapshot: &BookSnapshot,
        stream_session_id: Uuid,
        token_sequence: u64,
        source_event_hash: ContentHash,
    ) -> bool {
        self.write_snapshot_levels(SnapshotLevels {
            token_id,
            market_id,
            bids: &snapshot.bids,
            asks: &snapshot.asks,
            event_time: i64::try_from(snapshot.timestamp_ms).unwrap_or(i64::MAX),
            stream_session_id,
            token_sequence,
            book_version: snapshot.version,
            source_event_hash,
        })
    }

    pub fn write_delta_event(
        &self,
        cmd: &PriceDeltaCmd,
        market_id: Option<MarketId>,
    ) -> Option<ContentHash> {
        let payload_hash = delta_feed_event_hash(cmd)?;
        let persisted_time = Utc::now().timestamp_millis();
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
        let row = BookL2EventRow {
            stream_session_id: cmd.trace.stream_session_id,
            shard_id: cmd.trace.shard_id,
            token_id: cmd.asset_id.clone(),
            market_id,
            token_sequence: cmd.trace.token_sequence,
            event_type: ChCanonicalBookEventType::Delta,
            bid_prices,
            bid_sizes,
            ask_prices,
            ask_sizes,
            book_version: cmd.trace.token_sequence,
            old_tick_size: None,
            new_tick_size: None,
            venue_event_time: i64::try_from(cmd.timestamp_ms).unwrap_or(i64::MAX),
            ingress_time: cmd.trace.ingress_time_ms,
            persisted_time,
            payload_hash: payload_hash.clone(),
            schema_version: L2_SCHEMA_VERSION,
        };
        self.l2
            .write_timeout(row, CANONICAL_WRITE_TIMEOUT)
            .ok()
            .map(|()| payload_hash)
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

    pub fn write_tick_size_change(
        &self,
        token_id: &TokenId,
        market_id: Option<MarketId>,
        old_tick: TickSize,
        new_tick: TickSize,
        trace: quant_pivot_models::domain::pipeline::IngressTrace,
    ) -> bool {
        let payload_hash = CanonicalDigest::content_hash_json(&serde_json::json!({
            "event_type": "tick_size_change",
            "token_id": token_id,
            "old_tick": old_tick,
            "new_tick": new_tick,
            "token_sequence": trace.token_sequence,
        }));
        let Ok(payload_hash) = payload_hash else {
            return false;
        };
        let now_ms = Utc::now().timestamp_millis();
        self.l2
            .write_timeout(
                BookL2EventRow {
                    stream_session_id: trace.stream_session_id,
                    shard_id: trace.shard_id,
                    token_id: token_id.clone(),
                    market_id,
                    token_sequence: trace.token_sequence,
                    event_type: ChCanonicalBookEventType::TickSizeChange,
                    bid_prices: Vec::new(),
                    bid_sizes: Vec::new(),
                    ask_prices: Vec::new(),
                    ask_sizes: Vec::new(),
                    book_version: trace.token_sequence,
                    old_tick_size: Some(ChPrice::from(Price::new(old_tick.as_decimal()))),
                    new_tick_size: Some(ChPrice::from(Price::new(new_tick.as_decimal()))),
                    venue_event_time: i64::try_from(trace.ws_timestamp_ms).unwrap_or(i64::MAX),
                    ingress_time: trace.ingress_time_ms,
                    persisted_time: now_ms,
                    payload_hash,
                    schema_version: L2_SCHEMA_VERSION,
                },
                CANONICAL_WRITE_TIMEOUT,
            )
            .is_ok()
    }

    pub(crate) fn write_last_trade(&self, fact: MarketWsTradeFact<'_>) -> bool {
        let MarketWsTradeFact {
            token_id,
            market_id,
            price,
            side,
            trade_size,
            fee_rate_bps,
            timestamp_ms,
            trace,
        } = fact;
        let payload = serde_json::json!({
            "event_type": "last_trade",
            "market_id": market_id,
            "token_id": token_id,
            "price": price,
            "side": side,
            "size": trade_size,
            "fee_rate_bps": fee_rate_bps,
            "timestamp_ms": timestamp_ms,
            "stream_session_id": trace.stream_session_id,
            "token_sequence": trace.token_sequence,
        });
        let Ok(source_event_id) = CanonicalDigest::content_hash_json(&payload) else {
            return false;
        };
        let size_shares = trade_size.unwrap_or(Shares::ZERO);
        let mut observed_field_flags = trade_tape_coverage::MARKET_ID
            | trade_tape_coverage::TOKEN_ID
            | trade_tape_coverage::PRICE
            | trade_tape_coverage::TRADE_ID;
        if side.is_some() {
            observed_field_flags |= trade_tape_coverage::SIDE;
        }
        if trade_size.is_some() {
            observed_field_flags |= trade_tape_coverage::SIZE;
        }
        if fee_rate_bps.is_some() {
            observed_field_flags |= trade_tape_coverage::FEE_RATE;
        }
        self.trades
            .write_timeout(
                TradeTapeRow {
                    market_id,
                    token_id: token_id.clone(),
                    event_time: i64::try_from(timestamp_ms).unwrap_or(i64::MAX),
                    ingestion_time: Utc::now().timestamp_millis(),
                    stream_session_id: Some(trace.stream_session_id),
                    token_sequence: Some(trace.token_sequence),
                    participant_address: String::new(),
                    participant_role: ChTradeParticipantRole::Unknown,
                    side: match side {
                        Some(Side::Buy) => ChTradeSide::Buy,
                        Some(Side::Sell) => ChTradeSide::Sell,
                        None => ChTradeSide::Unknown,
                    },
                    price: ChPrice::from(price),
                    size_shares: ChShares::from(size_shares),
                    notional_usd: ChUsd::from(Usd::new(price.inner() * size_shares.inner())),
                    tx_hash: None,
                    source_event_id: source_event_id.to_string(),
                    source: ChTradeTapeSource::MarketWs,
                    observed_field_flags,
                    fee_rate_bps: fee_rate_bps.map(ChBps::from),
                    reconciliation_status: if side.is_some() && trade_size.is_some() {
                        ChTradeReconciliationStatus::Pending
                    } else {
                        ChTradeReconciliationStatus::Unavailable
                    },
                    matched_source_event_id: None,
                    revision: 1,
                    reconciled_at: None,
                    raw_payload_json: Some(payload.to_string()),
                    schema_version: TradeTapeRow::SCHEMA_VERSION,
                },
                CANONICAL_WRITE_TIMEOUT,
            )
            .is_ok()
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

    pub fn write_stream_session_open(
        &self,
        stream_session_id: Uuid,
        shard_id: u32,
        subscription_token_hash: ContentHash,
        subscription_token_count: u32,
        opened_at_ms: i64,
    ) -> bool {
        self.write_stream_session(BookStreamSessionRow {
            stream_session_id,
            shard_id,
            ledger_sequence: 1,
            state: ChStreamSessionState::Open,
            end_reason: ChStreamSessionEndReason::None,
            subscription_token_hash,
            subscription_token_count,
            received_sequence_json: "{}".to_owned(),
            persisted_sequence_json: "{}".to_owned(),
            opened_at: opened_at_ms,
            recorded_at: Utc::now().timestamp_millis(),
            schema_version: L2_SCHEMA_VERSION,
        })
    }

    pub fn write_stream_session_close(&self, row: BookStreamSessionRow) -> bool {
        self.write_stream_session(row)
    }

    fn write_stream_session(&self, row: BookStreamSessionRow) -> bool {
        self.sessions
            .write_timeout(row, CANONICAL_WRITE_TIMEOUT)
            .is_ok()
    }

    pub fn write_gap(
        &self,
        token_id: &TokenId,
        market_id: Option<MarketId>,
        stream_session_id: Uuid,
        shard_id: u32,
        token_sequence: u64,
        timestamp_ms: u64,
    ) -> bool {
        let payload = serde_json::json!({
            "event_type": "gap",
            "token_id": token_id,
            "stream_session_id": stream_session_id,
            "token_sequence": token_sequence,
        });
        let Ok(payload_hash) = CanonicalDigest::content_hash_json(&payload) else {
            return false;
        };
        self.l2
            .write_timeout(
                BookL2EventRow {
                    stream_session_id,
                    shard_id,
                    token_id: token_id.clone(),
                    market_id,
                    token_sequence,
                    event_type: ChCanonicalBookEventType::Gap,
                    bid_prices: Vec::new(),
                    bid_sizes: Vec::new(),
                    ask_prices: Vec::new(),
                    ask_sizes: Vec::new(),
                    book_version: token_sequence,
                    old_tick_size: None,
                    new_tick_size: None,
                    venue_event_time: i64::try_from(timestamp_ms).unwrap_or(i64::MAX),
                    ingress_time: i64::try_from(timestamp_ms).unwrap_or(i64::MAX),
                    persisted_time: Utc::now().timestamp_millis(),
                    payload_hash,
                    schema_version: L2_SCHEMA_VERSION,
                },
                CANONICAL_WRITE_TIMEOUT,
            )
            .is_ok()
    }

    fn write_snapshot_levels(&self, input: SnapshotLevels<'_>) -> bool {
        let bids_json = levels_to_pairs(input.bids);
        let asks_json = levels_to_pairs(input.asks);
        let Ok(bids_json) = serde_json::to_string(&bids_json) else {
            return false;
        };
        let Ok(asks_json) = serde_json::to_string(&asks_json) else {
            return false;
        };
        let checkpoint_payload = serde_json::json!({
            "token_id": input.token_id,
            "stream_session_id": input.stream_session_id,
            "token_sequence": input.token_sequence,
            "bids": bids_json,
            "asks": asks_json,
            "source_event_hash": input.source_event_hash,
        });
        let Ok(checkpoint_hash) = CanonicalDigest::content_hash_json(&checkpoint_payload) else {
            return false;
        };
        self.checkpoints
            .write_timeout(
                BookL2CheckpointRow {
                    token_id: input.token_id.clone(),
                    market_id: input.market_id,
                    stream_session_id: input.stream_session_id,
                    token_sequence: input.token_sequence,
                    bids_json,
                    asks_json,
                    book_version: input.book_version,
                    source_event_hash: input.source_event_hash,
                    checkpoint_hash,
                    event_time: input.event_time,
                    created_at: Utc::now().timestamp_millis(),
                    schema_version: L2_SCHEMA_VERSION,
                },
                CANONICAL_WRITE_TIMEOUT,
            )
            .is_ok()
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
            now_ms,
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
    current.available_at = current.available_at.max(next.available_at);

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
    available_at: i64,
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
        max_book_age_ms: u64::try_from(available_at).map_or(u64::MAX, |available_at| {
            available_at
                .checked_sub(snapshot.timestamp_ms)
                .unwrap_or(u64::MAX)
        }),
        schema_version: ChSchemaVersion::FIRST,
        available_at,
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
