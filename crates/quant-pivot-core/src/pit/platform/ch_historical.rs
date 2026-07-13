//! Durable point-in-time source backed by `ClickHouse` facts and the append-only
//! Postgres catalog ledger.
//!
//! This is the single production resolver for report serving, exit re-scoring,
//! dataset planning, and replay. It returns the freshest fact visible at the
//! already-frozen source cutoff; feature-specific staleness policy is evaluated
//! by the shared feature builder, never hidden inside this storage adapter.

use std::{collections::HashMap, str::FromStr, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{
    QuantResult,
    research::{PitResolutionStorageError, ResearchError},
};
use quant_pivot_models::{
    clickhouse::BookSnapshotRow,
    domain::{DecisionBoundary, DecisionSource, market::book::BookLevel},
    types::{MarketId, Price, Shares, TokenId},
};
use quant_pivot_repository::traits::{CatalogVersionRepository, QuantFactReadRepository};
use quant_pivot_research::pit::{
    BookSnapshotAt, PointInTimeSnapshotSource, ResolvedMarketSnapshot, resolve_catalog_snapshot,
};
use rust_decimal::Decimal;

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
            .book_snapshot_at(
                token_id,
                source_cutoff.timestamp_millis(),
                boundary.decision_at().timestamp_millis(),
            )
            .await
            .map_err(PitResolutionStorageError::from)?
        else {
            return Ok(None);
        };
        decode_book_row(row, source_cutoff, boundary.decision_at())
    }

    async fn books_at_boundary(
        &self,
        token_ids: &[TokenId],
        boundary: &DecisionBoundary,
    ) -> QuantResult<HashMap<TokenId, BookSnapshotAt>> {
        let cutoff = boundary.cutoff_for(DecisionSource::Book);
        let rows = self
            .fact_read
            .book_snapshots_at(
                token_ids.to_vec(),
                cutoff.timestamp_millis(),
                boundary.decision_at().timestamp_millis(),
            )
            .await
            .map_err(PitResolutionStorageError::from)?;
        let mut books = HashMap::with_capacity(rows.len());
        for row in rows {
            if let Some(book) = decode_book_row(row, cutoff, boundary.decision_at())? {
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

fn decode_book_row(
    row: BookSnapshotRow,
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
    row: BookSnapshotRow,
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
    (
        Some(BookSnapshotAt {
            token_id: row.token_id,
            source_cutoff,
            decision_at,
            bids,
            asks,
            timestamp_ms,
            version: row.book_version,
            sequence: row.sequence,
            available_at: match DateTime::from_timestamp_millis(row.ingestion_time) {
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
        clickhouse::{BookSnapshotRow, ChSchemaVersion},
        enums::clickhouse::{ChFactSource, ChSnapshotReason},
        types::TokenId,
    };

    fn sample_row(bids_json: &str, asks_json: &str) -> BookSnapshotRow {
        BookSnapshotRow {
            token_id: TokenId::new("tok"),
            market_id: None,
            snapshot_reason: ChSnapshotReason::Startup,
            top_n: 5,
            bids_json: bids_json.to_owned(),
            asks_json: asks_json.to_owned(),
            bid_depth_usd: None,
            ask_depth_usd: None,
            mid_price: None,
            spread_bps: None,
            book_version: 1,
            levels_count: 1,
            event_time: 1_000_000,
            ingestion_time: 1_000_000,
            sequence: 1,
            source: ChFactSource::WsSnapshot,
            schema_version: ChSchemaVersion::FIRST,
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
        future_available.ingestion_time += 1;
        assert!(decode_book_row(future_available, decision_at, decision_at).is_err());
    }
}
