//! Durable point-in-time source backed by `ClickHouse` facts and the append-only
//! Postgres catalog ledger.
//!
//! This is the single production resolver for report serving, exit re-scoring,
//! dataset planning, and replay. It returns the freshest fact visible at the
//! already-frozen source cutoff; feature-specific staleness policy is evaluated
//! by the shared feature builder, never hidden inside this storage adapter.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{
    QuantResult,
    research::{PitResolutionStorageError, ResearchError},
};
use quant_pivot_models::{
    clickhouse::{BookL2LedgerRow, BookStreamSessionRow, ChPrice, ChShares},
    domain::{
        data_plane::{DecisionBoundary, DecisionSource},
        market::book::BookLevel,
    },
    enums::{
        clickhouse::{ChCanonicalBookEventType, ChStreamSessionState},
        common::Side,
    },
    types::{ContentHash, MarketId, Price, Shares, TokenId},
};
use quant_pivot_repository::traits::{
    CatalogLedgerRepository, ClobMarketInfoRepository, QuantFactReadRepository,
};
use quant_pivot_research::pit::{
    BookSnapshotAt, CanonicalBookEventRef, PointInTimeSnapshotSource, ResolvedMarketSnapshot,
    resolve_catalog_snapshot,
};

use crate::ingest::order_book::OrderBook;

/// Outcome of decoding persisted book level JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookDecodeStatus {
    /// At least one valid level was decoded (possibly partial).
    Ok,
    /// Valid JSON but the level array is empty.
    Empty,
    /// Typed arrays have unequal lengths or contain an invalid level.
    InvalidLevel,
    /// Persisted event time cannot be represented as a non-negative epoch.
    InvalidTimestamp,
}

impl BookDecodeStatus {
    /// Whether this status should increment dataset `book_decode_failures`.
    #[must_use]
    pub const fn counts_as_failure(self) -> bool {
        matches!(self, Self::InvalidLevel | Self::InvalidTimestamp)
    }

    /// Merge two decode statuses, preferring the more severe failure.
    #[must_use]
    pub const fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::InvalidTimestamp, _) | (_, Self::InvalidTimestamp) => Self::InvalidTimestamp,
            (Self::InvalidLevel, _) | (_, Self::InvalidLevel) => Self::InvalidLevel,
            (Self::Empty, Self::Empty) => Self::Empty,
            _ => Self::Ok,
        }
    }
}

/// Resolves durable book and catalog state with no look-ahead.
pub struct DurablePitSource {
    fact_read: Arc<dyn QuantFactReadRepository>,
    catalog_repo: Arc<dyn CatalogLedgerRepository>,
    clob_market_info_repo: Arc<dyn ClobMarketInfoRepository>,
}

impl DurablePitSource {
    /// Build the production PIT source.
    #[must_use]
    pub fn new(
        fact_read: Arc<dyn QuantFactReadRepository>,
        catalog_repo: Arc<dyn CatalogLedgerRepository>,
        clob_market_info_repo: Arc<dyn ClobMarketInfoRepository>,
    ) -> Self {
        Self {
            fact_read,
            catalog_repo,
            clob_market_info_repo,
        }
    }
}

#[async_trait]
impl PointInTimeSnapshotSource for DurablePitSource {
    async fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        let source_cutoff = boundary.cutoff_for(DecisionSource::Book);
        let Some(row) = self
            .fact_read
            .book_ledger_snapshot_at(
                token_id,
                source_cutoff.timestamp_millis(),
                boundary.decision_at().timestamp_millis(),
            )
            .await
            .map_err(PitResolutionStorageError::from)?
        else {
            return Ok(None);
        };
        self.reconstruct_book_row(row, source_cutoff, boundary.decision_at())
            .await
    }

    async fn books_at_boundary(
        &self,
        token_ids: &[TokenId],
        boundary: &DecisionBoundary,
    ) -> QuantResult<HashMap<TokenId, BookSnapshotAt>> {
        let cutoff = boundary.cutoff_for(DecisionSource::Book);
        let rows = self
            .fact_read
            .book_ledger_snapshots_at(
                token_ids.to_vec(),
                cutoff.timestamp_millis(),
                boundary.decision_at().timestamp_millis(),
            )
            .await
            .map_err(PitResolutionStorageError::from)?;
        let mut books = HashMap::with_capacity(rows.len());
        for row in rows {
            if let Some(book) = self
                .reconstruct_book_row(row, cutoff, boundary.decision_at())
                .await?
            {
                books.insert(book.token_id.clone(), book);
            }
        }
        Ok(books)
    }

    async fn market_snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<ResolvedMarketSnapshot>> {
        let snapshot = self
            .catalog_repo
            .snapshot_at(market_id, boundary)
            .await
            .map_err(PitResolutionStorageError::from)?;
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        let mut resolved = resolve_catalog_snapshot(&snapshot, boundary)?;
        resolved.context.fee_schedule = self
            .clob_market_info_repo
            .at(
                market_id,
                boundary.knowledge_cutoff(),
                boundary.decision_at(),
            )
            .await
            .map_err(PitResolutionStorageError::from)?
            .map(|version| version.fee_schedule());
        Ok(Some(resolved))
    }

    async fn market_snapshots_at_boundary(
        &self,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Vec<ResolvedMarketSnapshot>> {
        let snapshots = self
            .catalog_repo
            .snapshots_at_boundary(boundary)
            .await
            .map_err(PitResolutionStorageError::from)?;
        let market_ids = snapshots
            .iter()
            .map(|snapshot| snapshot.market.market_id.clone())
            .collect::<Vec<_>>();
        let fee_schedules = self
            .clob_market_info_repo
            .at_many(
                &market_ids,
                boundary.knowledge_cutoff(),
                boundary.decision_at(),
            )
            .await
            .map_err(PitResolutionStorageError::from)?
            .into_iter()
            .map(|version| (version.market_id.clone(), version.fee_schedule()))
            .collect::<HashMap<_, _>>();
        snapshots
            .into_iter()
            .map(|snapshot| {
                let mut resolved = resolve_catalog_snapshot(&snapshot, boundary)?;
                resolved.context.fee_schedule =
                    fee_schedules.get(&resolved.context.market_id).cloned();
                Ok(resolved)
            })
            .collect()
    }
}

impl DurablePitSource {
    async fn reconstruct_book_row(
        &self,
        snapshot: BookL2LedgerRow,
        source_cutoff: DateTime<Utc>,
        decision_at: DateTime<Utc>,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        let token_id = snapshot.token_id.clone();
        let session = self
            .fact_read
            .book_stream_session_at(snapshot.stream_session_id, decision_at.timestamp_millis())
            .await
            .map_err(PitResolutionStorageError::from)?
            .ok_or_else(|| replay_error(&token_id, "stream session ledger is unavailable"))?;
        if session.state == ChStreamSessionState::Invalidated {
            return Err(replay_error(&token_id, "stream session is invalidated").into());
        }
        let events = self
            .fact_read
            .book_l2_ledger_from(
                &token_id,
                snapshot.stream_session_id,
                snapshot.token_sequence,
                source_cutoff.timestamp_millis(),
                decision_at.timestamp_millis(),
            )
            .await
            .map_err(PitResolutionStorageError::from)?;
        reconstruct_snapshot(snapshot, &session, &events, source_cutoff, decision_at)
    }
}

fn replay_error(token_id: &TokenId, detail: &str) -> ResearchError {
    ResearchError::PitResolution {
        detail: format!("canonical book replay for token {token_id} failed: {detail}"),
    }
}

pub(crate) fn reconstruct_snapshot(
    snapshot: BookL2LedgerRow,
    session: &BookStreamSessionRow,
    events: &[BookL2LedgerRow],
    source_cutoff: DateTime<Utc>,
    decision_at: DateTime<Utc>,
) -> QuantResult<Option<BookSnapshotAt>> {
    let token_id = snapshot.token_id.clone();
    let anchor_sequence = snapshot.token_sequence;
    validate_replay_identity(&snapshot, session, events)?;
    let (snapshot, status) = snapshot_from_row(snapshot, source_cutoff, decision_at);
    if status.counts_as_failure() {
        return Err(replay_error(&token_id, &format!("snapshot decode status {status:?}")).into());
    }
    let Some(anchor) = snapshot else {
        return Ok(None);
    };
    let mut book = OrderBook::new(token_id.clone());
    book.apply_snapshot_validated(anchor.bids, anchor.asks, anchor.timestamp_ms);
    let mut latest_event_time = anchor.timestamp_ms;
    let mut latest_available_at = anchor.available_at;
    let mut latest_version = anchor.version;
    let mut latest_sequence = anchor.sequence;
    let mut latest_source_event = anchor.source_event;
    for event in events
        .iter()
        .filter(|event| event.token_sequence > anchor_sequence)
    {
        apply_replay_event(&mut book, event, &token_id)?;
        latest_event_time = u64::try_from(event.venue_event_time)
            .map_err(|_| replay_error(&token_id, "event timestamp is negative"))?;
        latest_available_at = DateTime::from_timestamp_millis(event.persisted_time)
            .ok_or_else(|| replay_error(&token_id, "persisted timestamp is invalid"))?;
        latest_version = event.token_sequence;
        latest_sequence = event.token_sequence;
        latest_source_event = Some(CanonicalBookEventRef {
            stream_session_id: event.stream_session_id,
            token_sequence: event.token_sequence,
            source_event_hash: ContentHash::from(event.event_hash),
        });
    }
    Ok(Some(BookSnapshotAt {
        token_id,
        source_cutoff,
        decision_at,
        bids: Arc::from(book.bids()),
        asks: Arc::from(book.asks()),
        timestamp_ms: latest_event_time,
        version: latest_version,
        sequence: latest_sequence,
        source_event: latest_source_event,
        available_at: latest_available_at,
    }))
}

struct ReplaySequence<'a> {
    effective_ms: i64,
    available_ms: i64,
    event: &'a BookL2LedgerRow,
}

/// Reconstruct a monotonic series of boundaries from one session anchor in a
/// single sequence walk. The query count and event scan are independent of the
/// candidate count; only the requested output snapshots are cloned.
pub(crate) fn reconstruct_snapshot_series(
    snapshot: BookL2LedgerRow,
    session: &BookStreamSessionRow,
    events: &[BookL2LedgerRow],
    boundaries: &[DecisionBoundary],
) -> QuantResult<Vec<BookSnapshotAt>> {
    if boundaries.is_empty() {
        return Ok(Vec::new());
    }
    if boundaries.windows(2).any(|pair| {
        pair[0].decision_at() >= pair[1].decision_at()
            || pair[0].cutoff_for(DecisionSource::Book) >= pair[1].cutoff_for(DecisionSource::Book)
    }) {
        return Err(replay_error(
            &snapshot.token_id,
            "series boundaries are not strictly monotonic",
        )
        .into());
    }
    validate_replay_identity(&snapshot, session, events)?;
    let token_id = snapshot.token_id.clone();
    let anchor_sequence = snapshot.token_sequence;
    let first = boundaries
        .first()
        .ok_or_else(|| replay_error(&token_id, "series has no first boundary"))?;
    let (anchor, status) = snapshot_from_row(
        snapshot,
        first.cutoff_for(DecisionSource::Book),
        first.decision_at(),
    );
    if status.counts_as_failure() {
        return Err(replay_error(&token_id, &format!("snapshot decode status {status:?}")).into());
    }
    let anchor = anchor.ok_or_else(|| replay_error(&token_id, "snapshot is empty"))?;
    let mut book = OrderBook::new(token_id.clone());
    book.apply_snapshot_validated(anchor.bids, anchor.asks, anchor.timestamp_ms);
    let mut latest_event_time = anchor.timestamp_ms;
    let mut latest_available_at = anchor.available_at;
    let mut latest_version = anchor.version;
    let mut latest_sequence = anchor.sequence;
    let mut latest_source_event = anchor.source_event;

    let mut sequence_rows = BTreeMap::<u64, ReplaySequence<'_>>::new();
    for event in events
        .iter()
        .filter(|event| event.token_sequence > anchor_sequence)
    {
        sequence_rows.insert(
            event.token_sequence,
            ReplaySequence {
                effective_ms: event.venue_event_time,
                available_ms: event.persisted_time,
                event,
            },
        );
    }
    let sequence_rows = sequence_rows.into_iter().collect::<Vec<_>>();
    let mut cursor = 0_usize;
    let mut snapshots = Vec::with_capacity(boundaries.len());
    for boundary in boundaries {
        let cutoff_ms = boundary.cutoff_for(DecisionSource::Book).timestamp_millis();
        let decision_ms = boundary.decision_at().timestamp_millis();
        while let Some((_, row)) = sequence_rows.get(cursor) {
            if row.effective_ms > cutoff_ms || row.available_ms > decision_ms {
                break;
            }
            let event = row.event;
            apply_replay_event(&mut book, event, &token_id)?;
            latest_event_time = u64::try_from(event.venue_event_time)
                .map_err(|_| replay_error(&token_id, "event timestamp is negative"))?;
            latest_available_at = DateTime::from_timestamp_millis(event.persisted_time)
                .ok_or_else(|| replay_error(&token_id, "persisted timestamp is invalid"))?;
            latest_version = event.token_sequence;
            latest_sequence = event.token_sequence;
            latest_source_event = Some(CanonicalBookEventRef {
                stream_session_id: event.stream_session_id,
                token_sequence: event.token_sequence,
                source_event_hash: ContentHash::from(event.event_hash),
            });
            cursor = cursor
                .checked_add(1)
                .ok_or_else(|| replay_error(&token_id, "series cursor overflow"))?;
        }
        snapshots.push(BookSnapshotAt {
            token_id: token_id.clone(),
            source_cutoff: boundary.cutoff_for(DecisionSource::Book),
            decision_at: boundary.decision_at(),
            bids: Arc::from(book.bids()),
            asks: Arc::from(book.asks()),
            timestamp_ms: latest_event_time,
            version: latest_version,
            sequence: latest_sequence,
            source_event: latest_source_event.clone(),
            available_at: latest_available_at,
        });
    }
    Ok(snapshots)
}

fn validate_replay_identity(
    snapshot: &BookL2LedgerRow,
    session: &BookStreamSessionRow,
    events: &[BookL2LedgerRow],
) -> QuantResult<()> {
    let token_id = &snapshot.token_id;
    if session.stream_session_id != snapshot.stream_session_id {
        return Err(replay_error(token_id, "snapshot/session identity mismatch").into());
    }
    let Some(anchor) = events.first() else {
        return Err(replay_error(token_id, "snapshot source event is unavailable").into());
    };
    if anchor.token_sequence != snapshot.token_sequence
        || anchor.event_type != ChCanonicalBookEventType::Snapshot
        || anchor.event_hash != snapshot.event_hash
    {
        return Err(
            replay_error(token_id, "snapshot source event hash or sequence mismatch").into(),
        );
    }
    let mut sequence_identities = BTreeMap::<u64, String>::new();
    for event in events {
        let payload_hash = ContentHash::from(event.event_hash).to_string();
        insert_sequence_identity(
            &mut sequence_identities,
            event.token_sequence,
            &payload_hash,
            token_id,
        )?;
    }
    if let Some(last) = sequence_identities
        .last_key_value()
        .map(|(sequence, _)| *sequence)
    {
        for sequence in snapshot.token_sequence..=last {
            if !sequence_identities.contains_key(&sequence) {
                return Err(
                    replay_error(token_id, &format!("missing token sequence {sequence}")).into(),
                );
            }
        }
        validate_sealed_session_boundaries(session, token_id, last)?;
    }
    Ok(())
}

fn insert_sequence_identity(
    identities: &mut BTreeMap<u64, String>,
    sequence: u64,
    identity: &str,
    token_id: &TokenId,
) -> QuantResult<()> {
    if let Some(existing) = identities.insert(sequence, identity.to_owned())
        && existing != identity
    {
        return Err(
            replay_error(token_id, &format!("conflicting token sequence {sequence}")).into(),
        );
    }
    Ok(())
}

fn validate_sealed_session_boundaries(
    session: &BookStreamSessionRow,
    token_id: &TokenId,
    visible_last_sequence: u64,
) -> QuantResult<()> {
    if session.state != ChStreamSessionState::Sealed {
        return Ok(());
    }
    let received: BTreeMap<String, u64> = serde_json::from_str(&session.received_sequence_json)
        .map_err(|_| replay_error(token_id, "received sequence ledger is malformed"))?;
    let persisted: BTreeMap<String, u64> =
        serde_json::from_str(&session.persisted_sequence_json)
            .map_err(|_| replay_error(token_id, "persisted sequence ledger is malformed"))?;
    if received != persisted {
        return Err(replay_error(token_id, "received/persisted sequence boundaries differ").into());
    }
    if persisted
        .get(token_id.as_str())
        .is_none_or(|last| *last < visible_last_sequence)
    {
        return Err(replay_error(token_id, "sealed session boundary is behind replay").into());
    }
    Ok(())
}

fn apply_replay_event(
    book: &mut OrderBook,
    event: &BookL2LedgerRow,
    token_id: &TokenId,
) -> QuantResult<()> {
    let timestamp_ms = u64::try_from(event.venue_event_time)
        .map_err(|_| replay_error(token_id, "event timestamp is negative"))?;
    match event.event_type {
        ChCanonicalBookEventType::Snapshot => {
            let (bids, asks) = decode_event_levels(event, token_id)?;
            book.apply_snapshot(bids, asks, timestamp_ms);
        }
        ChCanonicalBookEventType::Delta => {
            if event.bid_prices.len() != event.bid_sizes.len()
                || event.ask_prices.len() != event.ask_sizes.len()
            {
                return Err(
                    replay_error(token_id, "delta price/size vector lengths differ").into(),
                );
            }
            let bids = event
                .bid_prices
                .iter()
                .zip(&event.bid_sizes)
                .map(|(price, size)| (Side::Buy, Price::from(*price), Shares::from(*size)));
            let asks = event
                .ask_prices
                .iter()
                .zip(&event.ask_sizes)
                .map(|(price, size)| (Side::Sell, Price::from(*price), Shares::from(*size)));
            book.apply_delta(bids.chain(asks), timestamp_ms);
        }
        ChCanonicalBookEventType::TickSizeChange | ChCanonicalBookEventType::LastTrade => {}
        ChCanonicalBookEventType::Gap => {
            return Err(replay_error(token_id, "gap event encountered").into());
        }
    }
    Ok(())
}

fn decode_event_levels(
    event: &BookL2LedgerRow,
    token_id: &TokenId,
) -> QuantResult<(Vec<BookLevel>, Vec<BookLevel>)> {
    if event.bid_prices.len() != event.bid_sizes.len()
        || event.ask_prices.len() != event.ask_sizes.len()
    {
        return Err(replay_error(token_id, "snapshot price/size vector lengths differ").into());
    }
    let decode = |price: &ChPrice, size: &ChShares| {
        BookLevel::from_decimal(Price::from(*price), Shares::from(*size))
            .map_err(|_| replay_error(token_id, "snapshot contains invalid level"))
    };
    let bids = event
        .bid_prices
        .iter()
        .zip(&event.bid_sizes)
        .map(|(price, size)| decode(price, size))
        .collect::<Result<Vec<_>, _>>()?;
    let asks = event
        .ask_prices
        .iter()
        .zip(&event.ask_sizes)
        .map(|(price, size)| decode(price, size))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((bids, asks))
}

#[cfg(test)]
fn decode_book_row(
    row: BookL2LedgerRow,
    source_cutoff: DateTime<Utc>,
    decision_at: DateTime<Utc>,
) -> QuantResult<Option<BookSnapshotAt>> {
    let token_id = row.token_id.clone();
    let (snapshot, status) = snapshot_from_row(row, source_cutoff, decision_at);
    if status.counts_as_failure() {
        return Err(ResearchError::PitResolution {
            detail: format!("book snapshot for token {token_id} failed to decode: {status:?}"),
        }
        .into());
    }
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    let effective_at =
        DateTime::from_timestamp_millis(i64::try_from(snapshot.timestamp_ms).map_err(|_| {
            ResearchError::PitResolution {
                detail: format!(
                    "book snapshot for token {token_id} has an unrepresentable effective timestamp"
                ),
            }
        })?)
        .ok_or_else(|| ResearchError::PitResolution {
            detail: format!(
                "book snapshot for token {token_id} has an unrepresentable effective timestamp"
            ),
        })?;
    if effective_at > source_cutoff || snapshot.available_at > decision_at {
        return Err(ResearchError::PitResolution {
            detail: format!(
                "book snapshot for token {token_id} is outside boundary: effective {effective_at}, available {}, source cutoff {source_cutoff}, decision {decision_at}",
                snapshot.available_at,
            ),
        }
        .into());
    }
    Ok(Some(snapshot))
}

/// Build a [`BookSnapshotAt`] from a persisted snapshot row, decoding the level
/// JSON written by the book-fact writer (`[(price, size)]` decimal-string pairs).
///
/// Returns `(None, status)` when JSON is malformed, every level is invalid, or
/// both sides decode to empty — treated as a missing book for PIT resolution.
#[must_use]
pub fn snapshot_from_row(
    row: BookL2LedgerRow,
    source_cutoff: DateTime<Utc>,
    decision_at: DateTime<Utc>,
) -> (Option<BookSnapshotAt>, BookDecodeStatus) {
    let Ok(timestamp_ms) = u64::try_from(row.venue_event_time) else {
        return (None, BookDecodeStatus::InvalidTimestamp);
    };
    if row.event_type != ChCanonicalBookEventType::Snapshot {
        return (None, BookDecodeStatus::InvalidLevel);
    }
    let (bids, bid_status) = decode_levels(&row.bid_prices, &row.bid_sizes);
    let (asks, ask_status) = decode_levels(&row.ask_prices, &row.ask_sizes);
    let status = bid_status.merge(ask_status);
    if status.counts_as_failure() || (bids.is_empty() && asks.is_empty()) {
        return (None, status);
    }
    let source_event = CanonicalBookEventRef {
        stream_session_id: row.stream_session_id,
        token_sequence: row.token_sequence,
        source_event_hash: ContentHash::from(row.event_hash),
    };
    (
        Some(BookSnapshotAt {
            token_id: row.token_id,
            source_cutoff,
            decision_at,
            bids,
            asks,
            timestamp_ms,
            version: row.token_sequence,
            sequence: row.token_sequence,
            source_event: Some(source_event),
            available_at: match DateTime::from_timestamp_millis(row.persisted_time) {
                Some(value) => value,
                None => return (None, BookDecodeStatus::InvalidTimestamp),
            },
        }),
        status,
    )
}

/// Decode typed price/size arrays into best-first book levels.
#[must_use]
pub fn decode_levels(
    prices: &[ChPrice],
    sizes: &[ChShares],
) -> (Arc<[BookLevel]>, BookDecodeStatus) {
    if prices.len() != sizes.len() {
        return (Arc::from([]), BookDecodeStatus::InvalidLevel);
    }
    if prices.is_empty() {
        return (Arc::from([]), BookDecodeStatus::Empty);
    }
    let mut levels = Vec::with_capacity(prices.len());
    for (price, size) in prices.iter().zip(sizes) {
        let Some(level) = BookLevel::try_from_decimal(Price::from(*price), Shares::from(*size))
        else {
            return (Arc::from([]), BookDecodeStatus::InvalidLevel);
        };
        levels.push(level);
    }
    (Arc::from(levels), BookDecodeStatus::Ok)
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        clickhouse::{BookL2LedgerRow, ChDigest, ChPrice, ChShares},
        enums::clickhouse::ChCanonicalBookEventType,
        types::{Price, Shares, TokenId},
    };
    use rust_decimal::Decimal;
    use uuid::Uuid;

    use super::{BookDecodeStatus, decode_book_row, decode_levels, snapshot_from_row};

    fn sample_row() -> BookL2LedgerRow {
        BookL2LedgerRow {
            stream_session_id: Uuid::nil(),
            shard_id: 0,
            token_id: TokenId::new("tok"),
            market_id: None,
            token_sequence: 1,
            event_type: ChCanonicalBookEventType::Snapshot,
            bid_prices: vec![ChPrice::from(Price::new(Decimal::new(48, 2)))],
            bid_sizes: vec![ChShares::from(Shares::new(Decimal::from(100)))],
            ask_prices: vec![ChPrice::from(Price::new(Decimal::new(52, 2)))],
            ask_sizes: vec![ChShares::from(Shares::new(Decimal::from(100)))],
            old_tick_size: None,
            new_tick_size: None,
            trade_price: None,
            trade_side: None,
            trade_size: None,
            fee_rate_bps: None,
            venue_event_time: 1_000_000,
            ingress_time: 1_000_000,
            persisted_time: 1_000_000,
            event_hash: ChDigest::new([1; 32]),
            schema_version: BookL2LedgerRow::SCHEMA_VERSION,
        }
    }

    #[test]
    fn decode_levels_valid_arrays() {
        let row = sample_row();
        let (levels, status) = decode_levels(&row.bid_prices, &row.bid_sizes);
        assert_eq!(status, BookDecodeStatus::Ok);
        assert_eq!(levels.len(), 1);
    }

    #[test]
    fn decode_levels_rejects_lengths() {
        let row = sample_row();
        let (levels, status) = decode_levels(&row.bid_prices, &[]);
        assert!(levels.is_empty());
        assert_eq!(status, BookDecodeStatus::InvalidLevel);
        assert!(status.counts_as_failure());
    }

    #[test]
    fn snapshot_rejects_invalid_level() {
        let mut row = sample_row();
        row.bid_sizes[0] = ChShares::from(Decimal::NEGATIVE_ONE);
        let decision_at = Utc.timestamp_millis_opt(1_000_000).single().expect("ts");
        let (snapshot, status) = snapshot_from_row(row.clone(), decision_at, decision_at);

        assert!(snapshot.is_none());
        assert_eq!(status, BookDecodeStatus::InvalidLevel);
        let error = decode_book_row(row, decision_at, decision_at)
            .expect_err("durable PIT reads must surface a typed decode error");
        assert!(error.to_string().contains("InvalidLevel"));
    }

    #[test]
    fn decoded_book_effective_boundary() {
        let decision_at = Utc
            .timestamp_millis_opt(1_000_000)
            .single()
            .expect("decision time");

        let mut future_effective = sample_row();
        future_effective.venue_event_time += 1;
        assert!(decode_book_row(future_effective, decision_at, decision_at).is_err());

        let mut future_available = sample_row();
        future_available.persisted_time += 1;
        assert!(decode_book_row(future_available, decision_at, decision_at).is_err());
    }
}
