//! Fire-and-forget CH writer for token-level book facts.

use std::{sync::Arc, time::Duration};

use chrono::Utc;
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, BookStreamSessionRow, ChBps, ChDecimal64, ChDigest,
        ChPrice, ChSchemaVersion, ChShares, ChUsd, MarketResolutionRow,
    },
    domain::{
        data_plane::pipeline::{BookSnapshotCmd, IngressTrace, PriceDeltaCmd},
        market::book::BookSnapshot,
    },
    enums::{
        clickhouse::{
            ChBookEventType, ChCanonicalBookEventType, ChFactSource, ChLedgerTradeSide,
            ChStreamSessionEndReason, ChStreamSessionState,
        },
        common::{Side, TickSize},
    },
    types::{ContentHash, MarketId, PartitionId, Price, Shares, TokenId, Usd},
};
use quant_pivot_storage::write::{AsyncWriter, DurableWriter};
use rust_decimal::Decimal;
use uuid::Uuid;

use super::ledger_persistence::{LedgerPersistenceHandle, PartitionLedgerClient};

const CANONICAL_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const L2_SCHEMA_VERSION: ChSchemaVersion = ChSchemaVersion(1);

pub struct BookFactWriter {
    ledger: LedgerPersistenceHandle,
    sessions: Arc<DurableWriter<BookStreamSessionRow>>,
    microstructure_1s: Arc<AsyncWriter<BookMicrostructureRow>>,
    resolutions: Arc<AsyncWriter<MarketResolutionRow>>,
}

/// Partition-owned one-second telemetry accumulator for one token.
#[derive(Default)]
pub(crate) struct MicrostructureAccumulator {
    pending: Option<BookMicrostructureRow>,
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

impl BookFactWriter {
    pub const fn new(
        ledger: LedgerPersistenceHandle,
        session_writer: Arc<DurableWriter<BookStreamSessionRow>>,
        microstructure_1s_writer: Arc<AsyncWriter<BookMicrostructureRow>>,
        resolution_writer: Arc<AsyncWriter<MarketResolutionRow>>,
    ) -> Self {
        Self {
            ledger,
            sessions: session_writer,
            microstructure_1s: microstructure_1s_writer,
            resolutions: resolution_writer,
        }
    }

    #[must_use]
    pub fn ledger_client(&self, partition_id: PartitionId) -> Option<PartitionLedgerClient> {
        self.ledger.partition(partition_id)
    }

    /// Build one snapshot row that is both canonical event and replay anchor.
    pub(crate) fn snapshot_ledger_row(
        cmd: &BookSnapshotCmd,
        token_id: &TokenId,
        market_id: Option<MarketId>,
    ) -> Option<BookL2LedgerRow> {
        let mut row = base_ledger_row(
            cmd.trace,
            token_id,
            market_id,
            ChCanonicalBookEventType::Snapshot,
            cmd.timestamp_ms,
        );
        row.bid_prices = cmd
            .bids
            .levels
            .iter()
            .map(|level| ChPrice::from(level.price_decimal()))
            .collect();
        row.bid_sizes = cmd
            .bids
            .levels
            .iter()
            .map(|level| ChShares::from(level.size_decimal()))
            .collect();
        row.ask_prices = cmd
            .asks
            .levels
            .iter()
            .map(|level| ChPrice::from(level.price_decimal()))
            .collect();
        row.ask_sizes = cmd
            .asks
            .levels
            .iter()
            .map(|level| ChShares::from(level.size_decimal()))
            .collect();
        seal_ledger_row(row)
    }

    pub(crate) fn delta_ledger_row(
        cmd: &PriceDeltaCmd,
        token_id: &TokenId,
        market_id: Option<MarketId>,
    ) -> Option<BookL2LedgerRow> {
        let mut row = base_ledger_row(
            cmd.trace,
            token_id,
            market_id,
            ChCanonicalBookEventType::Delta,
            cmd.timestamp_ms,
        );
        for change in cmd.changes.iter() {
            if change.side == Side::Buy {
                row.bid_prices.push(ChPrice::from(change.price));
                row.bid_sizes.push(ChShares::from(change.size));
            } else {
                row.ask_prices.push(ChPrice::from(change.price));
                row.ask_sizes.push(ChShares::from(change.size));
            }
        }
        seal_ledger_row(row)
    }

    pub(crate) fn write_microstructure_row(&self, row: BookMicrostructureRow) {
        self.microstructure_1s.write(row);
    }

    pub(crate) fn tick_size_ledger_row(
        token_id: &TokenId,
        market_id: Option<MarketId>,
        old_tick: TickSize,
        new_tick: TickSize,
        trace: IngressTrace,
    ) -> Option<BookL2LedgerRow> {
        let mut row = base_ledger_row(
            trace,
            token_id,
            market_id,
            ChCanonicalBookEventType::TickSizeChange,
            trace.ws_timestamp_ms,
        );
        row.old_tick_size = Some(ChPrice::from(Price::new(old_tick.as_decimal())));
        row.new_tick_size = Some(ChPrice::from(Price::new(new_tick.as_decimal())));
        seal_ledger_row(row)
    }

    pub(crate) fn last_trade_ledger_row(fact: &MarketWsTradeFact<'_>) -> Option<BookL2LedgerRow> {
        let mut ledger_row = base_ledger_row(
            fact.trace,
            fact.token_id,
            Some(fact.market_id.clone()),
            ChCanonicalBookEventType::LastTrade,
            fact.timestamp_ms,
        );
        ledger_row.trade_price = Some(ChPrice::from(fact.price));
        ledger_row.trade_side = fact.side.map(|side| match side {
            Side::Buy => ChLedgerTradeSide::Buy,
            Side::Sell => ChLedgerTradeSide::Sell,
        });
        ledger_row.trade_size = fact.trade_size.map(ChShares::from);
        ledger_row.fee_rate_bps = fact.fee_rate_bps.map(ChBps::from);
        seal_ledger_row(ledger_row)
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

    pub async fn write_stream_session_open(
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
        .await
    }

    pub async fn write_stream_session_close(&self, row: BookStreamSessionRow) -> bool {
        self.write_stream_session(row).await
    }

    async fn write_stream_session(&self, row: BookStreamSessionRow) -> bool {
        self.sessions
            .write_async_timeout(row, CANONICAL_WRITE_TIMEOUT)
            .await
            .is_ok()
    }

    pub(crate) fn gap_ledger_row(
        token_id: &TokenId,
        market_id: Option<MarketId>,
        stream_session_id: Uuid,
        shard_id: u32,
        token_sequence: u64,
        timestamp_ms: u64,
    ) -> Option<BookL2LedgerRow> {
        let row = ledger_row(LedgerIdentity {
            stream_session_id,
            shard_id,
            token_sequence,
            token_id,
            market_id,
            event_type: ChCanonicalBookEventType::Gap,
            venue_event_time: timestamp_ms,
            ingress_time: i64::try_from(timestamp_ms).unwrap_or(i64::MAX),
        });
        seal_ledger_row(row)
    }
}

impl MicrostructureAccumulator {
    pub(crate) fn observe(
        &mut self,
        token_id: &TokenId,
        market_id: Option<MarketId>,
        snapshot: &BookSnapshot,
        event_type: ChBookEventType,
        delete_count: u64,
    ) -> Option<BookMicrostructureRow> {
        let now_ms = Utc::now().timestamp_millis();
        self.observe_at(
            token_id,
            market_id,
            snapshot,
            event_type,
            delete_count,
            now_ms,
        )
    }

    fn observe_at(
        &mut self,
        token_id: &TokenId,
        market_id: Option<MarketId>,
        snapshot: &BookSnapshot,
        event_type: ChBookEventType,
        delete_count: u64,
        now_ms: i64,
    ) -> Option<BookMicrostructureRow> {
        let second_observation = microstructure_row(
            token_id,
            market_id,
            snapshot,
            event_type,
            delete_count,
            bucket_ms(now_ms, 1_000),
            now_ms,
        );
        match self.pending.as_mut() {
            Some(current) if current.bucket_time == second_observation.bucket_time => {
                merge_microstructure_row(current, second_observation);
                None
            }
            Some(_) => self.pending.replace(second_observation),
            None => {
                self.pending = Some(second_observation);
                None
            }
        }
    }

    pub(crate) const fn flush(&mut self) -> Option<BookMicrostructureRow> {
        self.pending.take()
    }

    pub(crate) fn flush_elapsed(&mut self, now_ms: i64) -> Option<BookMicrostructureRow> {
        if self
            .pending
            .as_ref()
            .is_some_and(|row| row.bucket_time.saturating_add(1_000) <= now_ms)
        {
            self.pending.take()
        } else {
            None
        }
    }
}

fn base_ledger_row(
    trace: IngressTrace,
    token_id: &TokenId,
    market_id: Option<MarketId>,
    event_type: ChCanonicalBookEventType,
    venue_event_time: u64,
) -> BookL2LedgerRow {
    ledger_row(LedgerIdentity {
        stream_session_id: trace.session.stream_session_id,
        shard_id: trace.shard_id,
        token_sequence: trace.token_sequence,
        token_id,
        market_id,
        event_type,
        venue_event_time,
        ingress_time: trace.ingress_time_ms,
    })
}

struct LedgerIdentity<'a> {
    stream_session_id: Uuid,
    shard_id: u32,
    token_sequence: u64,
    token_id: &'a TokenId,
    market_id: Option<MarketId>,
    event_type: ChCanonicalBookEventType,
    venue_event_time: u64,
    ingress_time: i64,
}

fn ledger_row(identity: LedgerIdentity<'_>) -> BookL2LedgerRow {
    let LedgerIdentity {
        stream_session_id,
        shard_id,
        token_sequence,
        token_id,
        market_id,
        event_type,
        venue_event_time,
        ingress_time,
    } = identity;
    BookL2LedgerRow {
        stream_session_id,
        shard_id,
        token_id: token_id.clone(),
        market_id,
        token_sequence,
        event_type,
        bid_prices: Vec::new(),
        bid_sizes: Vec::new(),
        ask_prices: Vec::new(),
        ask_sizes: Vec::new(),
        old_tick_size: None,
        new_tick_size: None,
        trade_price: None,
        trade_side: None,
        trade_size: None,
        fee_rate_bps: None,
        venue_event_time: i64::try_from(venue_event_time).unwrap_or(i64::MAX),
        ingress_time,
        persisted_time: Utc::now().timestamp_millis(),
        event_hash: ChDigest::new([0; 32]),
        schema_version: BookL2LedgerRow::SCHEMA_VERSION,
    }
}

fn seal_ledger_row(row: BookL2LedgerRow) -> Option<BookL2LedgerRow> {
    row.seal().ok()
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

fn microstructure_row(
    token_id: &TokenId,
    market_id: Option<MarketId>,
    snapshot: &BookSnapshot,
    event_type: ChBookEventType,
    delete_count: u64,
    bucket_time: i64,
    available_at: i64,
) -> BookMicrostructureRow {
    let summary = snapshot.summary;
    let best_bid = summary.best_bid;
    let best_ask = summary.best_ask;
    let spread = spread_bps(summary.spread, summary.mid);
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
        mid_price_open: summary.mid.map(ChPrice::from),
        mid_price_close: summary.mid.map(ChPrice::from),
        top1_depth_usd_avg: Some(ChUsd::from(Usd::new(summary.top1_depth_usd.to_decimal()))),
        top5_depth_usd_avg: Some(ChUsd::from(Usd::new(summary.top5_depth_usd.to_decimal()))),
        top20_depth_usd_avg: Some(ChUsd::from(Usd::new(summary.top20_depth_usd.to_decimal()))),
        imbalance_avg: summary.imbalance.map(ChDecimal64::from),
        update_count: 1,
        snapshot_count: u64::from(event_type == ChBookEventType::Snapshot),
        delta_count: u64::from(event_type == ChBookEventType::Delta),
        delete_count,
        crossed_count: u64::from(summary.crossed),
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

const fn bucket_ms(timestamp_ms: i64, interval_ms: i64) -> i64 {
    timestamp_ms - timestamp_ms.rem_euclid(interval_ms)
}

fn spread_bps(spread: Option<Price>, mid: Option<Price>) -> Option<Decimal> {
    let spread = spread?;
    let mid = mid?;
    if mid.is_zero() {
        return None;
    }
    Some(spread.inner() / mid.inner() * Decimal::from(10_000))
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{domain::market::BookLevel, types::Shares};

    use super::*;

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
        assert_eq!(snap.summary.imbalance, Some(Decimal::ZERO));
    }

    #[test]
    fn imbalance_is_minus_one_when_bids_empty() {
        let snap = snapshot(&[], &[level(60, 10)]);
        assert_eq!(snap.summary.imbalance, Some(Decimal::NEGATIVE_ONE));
    }

    #[test]
    fn imbalance_is_plus_one_when_asks_empty() {
        let snap = snapshot(&[level(40, 10)], &[]);
        assert_eq!(snap.summary.imbalance, Some(Decimal::ONE));
    }

    #[test]
    fn imbalance_none_when_both_sides_empty() {
        let snap = snapshot(&[], &[]);
        assert_eq!(snap.summary.imbalance, None);
    }

    #[test]
    fn partition_accumulator_rolls_and_flushes_one_bucket() {
        let token_id = TokenId::new("token");
        let snap = snapshot(&[level(40, 10)], &[level(60, 10)]);
        let mut accumulator = MicrostructureAccumulator::default();

        assert!(
            accumulator
                .observe_at(&token_id, None, &snap, ChBookEventType::Snapshot, 0, 1_000)
                .is_none()
        );
        assert!(
            accumulator
                .observe_at(&token_id, None, &snap, ChBookEventType::Delta, 2, 1_999)
                .is_none()
        );
        let rolled = accumulator
            .observe_at(&token_id, None, &snap, ChBookEventType::Delta, 1, 2_000)
            .expect("completed one-second bucket");
        assert_eq!(rolled.bucket_time, 1_000);
        assert_eq!(rolled.update_count, 2);
        assert_eq!(rolled.snapshot_count, 1);
        assert_eq!(rolled.delta_count, 1);
        assert_eq!(rolled.delete_count, 2);

        let pending = accumulator.flush().expect("current bucket");
        assert_eq!(pending.bucket_time, 2_000);
        assert_eq!(pending.update_count, 1);
        assert!(accumulator.flush().is_none());
    }

    #[test]
    fn partition_accumulator_flushes_quiet_completed_bucket() {
        let token_id = TokenId::new("token");
        let snap = snapshot(&[level(40, 10)], &[level(60, 10)]);
        let mut accumulator = MicrostructureAccumulator::default();
        assert!(
            accumulator
                .observe_at(&token_id, None, &snap, ChBookEventType::Snapshot, 0, 1_500)
                .is_none()
        );
        assert!(accumulator.flush_elapsed(1_999).is_none());
        assert_eq!(
            accumulator
                .flush_elapsed(2_000)
                .expect("elapsed bucket")
                .bucket_time,
            1_000
        );
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
