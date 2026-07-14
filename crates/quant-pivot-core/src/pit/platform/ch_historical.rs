//! Durable point-in-time source backed by `ClickHouse` facts and the append-only
//! Postgres catalog ledger.
//!
//! This is the single production resolver for report serving, exit re-scoring,
//! dataset planning, and replay. It returns the freshest fact visible at the
//! already-frozen source cutoff; feature-specific staleness policy is evaluated
//! by the shared feature builder, never hidden inside this storage adapter.

use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{
    QuantResult,
    research::{PitResolutionStorageError, ResearchError},
};
use quant_pivot_models::{
    clickhouse::{BookL2CheckpointRow, BookL2EventRow, BookStreamSessionRow, TradeTapeRow},
    domain::{DecisionBoundary, DecisionSource, market::book::BookLevel},
    enums::{
        clickhouse::{ChCanonicalBookEventType, ChStreamSessionState},
        common::Side,
    },
    types::{MarketId, Price, Shares, TokenId},
};
use quant_pivot_repository::traits::{CatalogVersionRepository, QuantFactReadRepository};
use quant_pivot_research::pit::{
    BookSnapshotAt, CanonicalBookEventRef, PointInTimeSnapshotSource, ResolvedMarketSnapshot,
    resolve_catalog_snapshot,
};
use rust_decimal::Decimal;

use crate::ingest::order_book::OrderBook;

/// Outcome of decoding persisted book level JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookDecodeStatus {
    /// At least one valid level was decoded (possibly partial).
    Ok,
    /// Valid JSON but the level array is empty.
    Empty,
    /// JSON could not be parsed as `[(price, size)]` pairs.
    MalformedJson,
    /// JSON parsed but every level pair failed validation.
    InvalidLevel,
    /// Persisted event time cannot be represented as a non-negative epoch.
    InvalidTimestamp,
}

impl BookDecodeStatus {
    /// Whether this status should increment dataset `book_decode_failures`.
    #[must_use]
    pub const fn counts_as_failure(self) -> bool {
        matches!(
            self,
            Self::MalformedJson | Self::InvalidLevel | Self::InvalidTimestamp
        )
    }

    /// Merge two decode statuses, preferring the more severe failure.
    #[must_use]
    pub const fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::MalformedJson, _) | (_, Self::MalformedJson) => Self::MalformedJson,
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
    catalog_repo: Arc<dyn CatalogVersionRepository>,
}

impl DurablePitSource {
    /// Build the production PIT source.
    #[must_use]
    pub fn new(
        fact_read: Arc<dyn QuantFactReadRepository>,
        catalog_repo: Arc<dyn CatalogVersionRepository>,
    ) -> Self {
        Self {
            fact_read,
            catalog_repo,
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
            .book_checkpoint_at(
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
            .book_checkpoints_at(
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
        self.catalog_repo
            .snapshot_at(market_id, boundary)
            .await
            .map_err(PitResolutionStorageError::from)?
            .map(|snapshot| resolve_catalog_snapshot(snapshot, boundary))
            .transpose()
    }

    async fn market_snapshots_at_boundary(
        &self,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Vec<ResolvedMarketSnapshot>> {
        self.catalog_repo
            .snapshots_at_boundary(boundary)
            .await
            .map_err(PitResolutionStorageError::from)?
            .into_iter()
            .map(|snapshot| resolve_catalog_snapshot(snapshot, boundary))
            .collect()
    }
}

impl DurablePitSource {
    async fn reconstruct_book_row(
        &self,
        checkpoint: BookL2CheckpointRow,
        source_cutoff: DateTime<Utc>,
        decision_at: DateTime<Utc>,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        let token_id = checkpoint.token_id.clone();
        let session = self
            .fact_read
            .book_stream_session_at(checkpoint.stream_session_id, decision_at.timestamp_millis())
            .await
            .map_err(PitResolutionStorageError::from)?
            .ok_or_else(|| replay_error(&token_id, "stream session ledger is unavailable"))?;
        if session.state == ChStreamSessionState::Invalidated {
            return Err(replay_error(&token_id, "stream session is invalidated").into());
        }
        let events = self
            .fact_read
            .book_l2_events_from(
                &token_id,
                checkpoint.stream_session_id,
                checkpoint.token_sequence,
                source_cutoff.timestamp_millis(),
                decision_at.timestamp_millis(),
            )
            .await
            .map_err(PitResolutionStorageError::from)?;
        let trades = self
            .fact_read
            .market_ws_trades_from(
                &token_id,
                checkpoint.stream_session_id,
                checkpoint.token_sequence,
                source_cutoff.timestamp_millis(),
                decision_at.timestamp_millis(),
            )
            .await
            .map_err(PitResolutionStorageError::from)?;
        reconstruct_checkpoint(
            checkpoint,
            &session,
            &events,
            &trades,
            source_cutoff,
            decision_at,
        )
    }
}

fn replay_error(token_id: &TokenId, detail: &str) -> ResearchError {
    ResearchError::PitResolution {
        detail: format!("canonical book replay for token {token_id} failed: {detail}"),
    }
}

fn reconstruct_checkpoint(
    checkpoint: BookL2CheckpointRow,
    session: &BookStreamSessionRow,
    events: &[BookL2EventRow],
    trades: &[TradeTapeRow],
    source_cutoff: DateTime<Utc>,
    decision_at: DateTime<Utc>,
) -> QuantResult<Option<BookSnapshotAt>> {
    let token_id = checkpoint.token_id.clone();
    let anchor_sequence = checkpoint.token_sequence;
    validate_replay_identity(&checkpoint, session, events, trades)?;
    let (snapshot, status) = snapshot_from_row(checkpoint, source_cutoff, decision_at);
    if status.counts_as_failure() {
        return Err(
            replay_error(&token_id, &format!("checkpoint decode status {status:?}")).into(),
        );
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
        latest_version = event.book_version;
        latest_sequence = event.token_sequence;
        latest_source_event = Some(CanonicalBookEventRef {
            stream_session_id: event.stream_session_id,
            token_sequence: event.token_sequence,
            source_event_hash: event.payload_hash.clone(),
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

fn validate_replay_identity(
    checkpoint: &BookL2CheckpointRow,
    session: &BookStreamSessionRow,
    events: &[BookL2EventRow],
    trades: &[TradeTapeRow],
) -> QuantResult<()> {
    let token_id = &checkpoint.token_id;
    if session.stream_session_id != checkpoint.stream_session_id {
        return Err(replay_error(token_id, "checkpoint/session identity mismatch").into());
    }
    let Some(anchor) = events.first() else {
        return Err(replay_error(token_id, "checkpoint source event is unavailable").into());
    };
    if anchor.token_sequence != checkpoint.token_sequence
        || anchor.event_type != ChCanonicalBookEventType::Snapshot
        || anchor.payload_hash != checkpoint.source_event_hash
    {
        return Err(replay_error(
            token_id,
            "checkpoint source event hash or sequence mismatch",
        )
        .into());
    }
    let mut sequence_identities = BTreeMap::<u64, String>::new();
    for event in events {
        insert_sequence_identity(
            &mut sequence_identities,
            event.token_sequence,
            event.payload_hash.as_str(),
            token_id,
        )?;
    }
    for trade in trades {
        let sequence = trade
            .token_sequence
            .ok_or_else(|| replay_error(token_id, "Market WS trade has no token sequence"))?;
        insert_sequence_identity(
            &mut sequence_identities,
            sequence,
            &trade.source_event_id,
            token_id,
        )?;
    }
    if let Some(last) = sequence_identities
        .last_key_value()
        .map(|(sequence, _)| *sequence)
    {
        for sequence in checkpoint.token_sequence..=last {
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
    event: &BookL2EventRow,
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
                .map(|(price, size)| (Side::Buy, price.to_price(), size.to_shares()));
            let asks = event
                .ask_prices
                .iter()
                .zip(&event.ask_sizes)
                .map(|(price, size)| (Side::Sell, price.to_price(), size.to_shares()));
            book.apply_delta(bids.chain(asks), timestamp_ms);
        }
        ChCanonicalBookEventType::TickSizeChange => {}
        ChCanonicalBookEventType::Gap => {
            return Err(replay_error(token_id, "gap event encountered").into());
        }
    }
    Ok(())
}

fn decode_event_levels(
    event: &BookL2EventRow,
    token_id: &TokenId,
) -> QuantResult<(Vec<BookLevel>, Vec<BookLevel>)> {
    if event.bid_prices.len() != event.bid_sizes.len()
        || event.ask_prices.len() != event.ask_sizes.len()
    {
        return Err(replay_error(token_id, "snapshot price/size vector lengths differ").into());
    }
    let decode = |price: &quant_pivot_models::clickhouse::ChPrice,
                  size: &quant_pivot_models::clickhouse::ChShares| {
        BookLevel::from_decimal(price.to_price(), size.to_shares())
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
    row: BookL2CheckpointRow,
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
    row: BookL2CheckpointRow,
    source_cutoff: DateTime<Utc>,
    decision_at: DateTime<Utc>,
) -> (Option<BookSnapshotAt>, BookDecodeStatus) {
    let Ok(timestamp_ms) = u64::try_from(row.event_time) else {
        return (None, BookDecodeStatus::InvalidTimestamp);
    };
    let (bids, bid_status) = decode_levels(&row.bids_json);
    let (asks, ask_status) = decode_levels(&row.asks_json);
    let status = bid_status.merge(ask_status);
    if status.counts_as_failure() || (bids.is_empty() && asks.is_empty()) {
        return (None, status);
    }
    let source_event = CanonicalBookEventRef {
        stream_session_id: row.stream_session_id,
        token_sequence: row.token_sequence,
        source_event_hash: row.source_event_hash.clone(),
    };
    (
        Some(BookSnapshotAt {
            token_id: row.token_id,
            source_cutoff,
            decision_at,
            bids,
            asks,
            timestamp_ms,
            version: row.book_version,
            sequence: row.token_sequence,
            source_event: Some(source_event),
            available_at: match DateTime::from_timestamp_millis(row.created_at) {
                Some(value) => value,
                None => return (None, BookDecodeStatus::InvalidTimestamp),
            },
        }),
        status,
    )
}

/// Decode the `[(price, size)]` decimal-string pairs into best-first book levels.
#[must_use]
pub fn decode_levels(json: &str) -> (Arc<[BookLevel]>, BookDecodeStatus) {
    let pairs: Vec<(String, String)> = match serde_json::from_str(json) {
        Ok(parsed) => parsed,
        Err(_) => return (Arc::from([]), BookDecodeStatus::MalformedJson),
    };
    if pairs.is_empty() {
        return (Arc::from([]), BookDecodeStatus::Empty);
    }
    let mut levels = Vec::with_capacity(pairs.len());
    for (price, size) in pairs {
        let (Ok(price), Ok(size)) = (Decimal::from_str(&price), Decimal::from_str(&size)) else {
            return (Arc::from([]), BookDecodeStatus::InvalidLevel);
        };
        let Some(level) = BookLevel::try_from_decimal(Price::new(price), Shares::new(size)) else {
            return (Arc::from([]), BookDecodeStatus::InvalidLevel);
        };
        levels.push(level);
    }
    (Arc::from(levels), BookDecodeStatus::Ok)
}

#[cfg(test)]
mod tests {
    use super::{BookDecodeStatus, decode_book_row, decode_levels, snapshot_from_row};
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        clickhouse::{BookL2CheckpointRow, ChSchemaVersion},
        types::{ContentHash, TokenId},
    };
    use uuid::Uuid;

    fn sample_row(bids_json: &str, asks_json: &str) -> BookL2CheckpointRow {
        BookL2CheckpointRow {
            token_id: TokenId::new("tok"),
            market_id: None,
            stream_session_id: Uuid::nil(),
            token_sequence: 1,
            bids_json: bids_json.to_owned(),
            asks_json: asks_json.to_owned(),
            book_version: 1,
            source_event_hash: ContentHash::parse(format!("blake3:{}", "1".repeat(64)))
                .expect("source hash"),
            checkpoint_hash: ContentHash::parse(format!("blake3:{}", "2".repeat(64)))
                .expect("checkpoint hash"),
            event_time: 1_000_000,
            created_at: 1_000_000,
            schema_version: ChSchemaVersion(2),
        }
    }

    #[test]
    fn decode_levels_valid_pairs() {
        let (levels, status) = decode_levels(r#"[["0.48","100"],["0.47","50"]]"#);
        assert_eq!(status, BookDecodeStatus::Ok);
        assert_eq!(levels.len(), 2);
    }

    #[test]
    fn decode_levels_malformed_json() {
        let (levels, status) = decode_levels("not-json");
        assert!(levels.is_empty());
        assert_eq!(status, BookDecodeStatus::MalformedJson);
        assert!(status.counts_as_failure());
    }

    #[test]
    fn decode_levels_rejects_a_partially_invalid_book() {
        let (levels, status) = decode_levels(r#"[["0.48","100"],["not-a-price","50"]]"#);

        assert!(levels.is_empty());
        assert_eq!(status, BookDecodeStatus::InvalidLevel);
        assert!(status.counts_as_failure());
    }

    #[test]
    fn snapshot_from_row_malformed_bids_yields_none() {
        let row = sample_row("not-json", r#"[["0.52","100"]]"#);
        let decision_at = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        let (snapshot, status) = snapshot_from_row(row, decision_at, decision_at);
        assert!(snapshot.is_none());
        assert_eq!(status, BookDecodeStatus::MalformedJson);
    }

    #[test]
    fn snapshot_from_row_rejects_one_invalid_level_in_an_otherwise_valid_side() {
        let row = sample_row(
            r#"[["0.48","100"],["0.47","invalid-size"]]"#,
            r#"[["0.52","100"]]"#,
        );
        let decision_at = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        let (snapshot, status) = snapshot_from_row(row.clone(), decision_at, decision_at);

        assert!(snapshot.is_none());
        assert_eq!(status, BookDecodeStatus::InvalidLevel);
        let error = decode_book_row(row, decision_at, decision_at)
            .expect_err("durable PIT reads must surface a typed decode error");
        assert!(error.to_string().contains("InvalidLevel"));
    }

    #[test]
    fn decoded_book_must_be_effective_and_available_within_boundary() {
        let decision_at = Utc
            .timestamp_millis_opt(1_000_000)
            .single()
            .expect("decision time");

        let mut future_effective = sample_row(r#"[["0.48","100"]]"#, r#"[["0.52","100"]]"#);
        future_effective.event_time += 1;
        assert!(decode_book_row(future_effective, decision_at, decision_at).is_err());

        let mut future_available = sample_row(r#"[["0.48","100"]]"#, r#"[["0.52","100"]]"#);
        future_available.created_at += 1;
        assert!(decode_book_row(future_available, decision_at, decision_at).is_err());
    }
}
