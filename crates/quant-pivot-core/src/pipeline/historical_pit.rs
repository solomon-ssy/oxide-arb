//! Streaming historical point-in-time source backed by `ClickHouse` facts +
//! `Postgres` market metadata.
//!
//! The `as_of`-bounded counterpart to [`LiveBookDataSource`](super::point_in_time::LiveBookDataSource):
//! it resolves the freshest book snapshot published at or before `as_of` (within
//! `max_book_staleness`) and derives the market's point-in-time status from the
//! authoritative settlement ledger. Used by the 3.6 backtester for streaming
//! replay; the offline dataset builder batch-prefetches and serves from
//! [`MaterializedPitEngine`](quant_pivot_research::pit::MaterializedPitEngine)
//! instead, so its build loop never queries a database.

use std::{str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use quant_pivot_error::{QuantResult, research::PitResolutionStorageError};
use quant_pivot_models::{
    clickhouse::BookSnapshotRow,
    domain::{MarketInfo, market::book::BookLevel},
    enums::market::MarketStatus,
    types::{MarketId, Price, Shares, TokenId},
};
use quant_pivot_repository::traits::{MarketRepository, QuantFactReadRepository};
use quant_pivot_research::pit::{BookSnapshotAt, MarketContextAt, PitQueryEngine};
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
}

impl BookDecodeStatus {
    /// Whether this status should increment dataset `book_decode_failures`.
    #[must_use]
    pub const fn counts_as_failure(self) -> bool {
        matches!(self, Self::MalformedJson | Self::InvalidLevel)
    }

    /// Merge two decode statuses, preferring the more severe failure.
    #[must_use]
    pub const fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::MalformedJson, _) | (_, Self::MalformedJson) => Self::MalformedJson,
            (Self::InvalidLevel, _) | (_, Self::InvalidLevel) => Self::InvalidLevel,
            (Self::Empty, Self::Empty) => Self::Empty,
            _ => Self::Ok,
        }
    }
}

/// Resolves historical book / market state with no look-ahead.
pub struct ChHistoricalPitSource {
    fact_read: Arc<dyn QuantFactReadRepository>,
    market_repo: Arc<dyn MarketRepository>,
    max_book_staleness: Duration,
}

impl ChHistoricalPitSource {
    /// Build a historical PIT source.
    #[must_use]
    pub fn new(
        fact_read: Arc<dyn QuantFactReadRepository>,
        market_repo: Arc<dyn MarketRepository>,
        max_book_staleness: Duration,
    ) -> Self {
        Self {
            fact_read,
            market_repo,
            max_book_staleness,
        }
    }

    /// Maximum book staleness, in milliseconds, as a saturating `i64`.
    fn max_staleness_ms(&self) -> i64 {
        i64::try_from(self.max_book_staleness.as_millis()).unwrap_or(i64::MAX)
    }
}

#[async_trait]
impl PitQueryEngine for ChHistoricalPitSource {
    async fn book_at(
        &self,
        token_id: &TokenId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        let cutoff_ms = as_of.timestamp_millis();
        let Some(row) = self
            .fact_read
            .book_snapshot_at(token_id, cutoff_ms)
            .await
            .map_err(PitResolutionStorageError::from)?
        else {
            return Ok(None);
        };
        if cutoff_ms.saturating_sub(row.event_time) > self.max_staleness_ms() {
            return Ok(None);
        }
        Ok(snapshot_from_row(row, as_of).0)
    }

    async fn market_at(
        &self,
        market_id: &MarketId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<MarketContextAt>> {
        let Some(info) = self
            .market_repo
            .find_by_id(market_id)
            .await
            .map_err(PitResolutionStorageError::from)?
        else {
            return Ok(None);
        };
        if info.created_at > as_of {
            // The market did not exist at the decision time.
            return Ok(None);
        }
        let resolved = self
            .fact_read
            .resolution_at(market_id, as_of.timestamp_millis())
            .await
            .map_err(PitResolutionStorageError::from)?
            .is_some();
        Ok(Some(market_context(&info, as_of, resolved)))
    }
}

/// Derive a point-in-time market context from static metadata + settled status.
///
/// Only immutable fields are read from the (mutable) market row; the lifecycle
/// status comes from the append-only settlement ledger.
#[must_use]
pub fn market_context(info: &MarketInfo, as_of: DateTime<Utc>, resolved: bool) -> MarketContextAt {
    MarketContextAt {
        market_id: info.market_id.clone(),
        as_of,
        observed_at: as_of,
        status: if resolved {
            MarketStatus::Settled
        } else {
            MarketStatus::Active
        },
        neg_risk: info.neg_risk,
        end_date: info.end_date,
        created_at: info.created_at,
        // Polymarket binary markets carry a YES and a NO outcome token.
        outcome_count: 2,
    }
}

/// Build a [`BookSnapshotAt`] from a persisted snapshot row, decoding the level
/// JSON written by the book-fact writer (`[(price, size)]` decimal-string pairs).
///
/// Returns `(None, status)` when JSON is malformed, every level is invalid, or
/// both sides decode to empty — treated as a missing book for PIT resolution.
#[must_use]
pub fn snapshot_from_row(
    row: BookSnapshotRow,
    as_of: DateTime<Utc>,
) -> (Option<BookSnapshotAt>, BookDecodeStatus) {
    let (bids, bid_status) = decode_levels(&row.bids_json);
    let (asks, ask_status) = decode_levels(&row.asks_json);
    let status = bid_status.merge(ask_status);
    if status.counts_as_failure() || (bids.is_empty() && asks.is_empty()) {
        return (None, status);
    }
    (
        Some(BookSnapshotAt {
            token_id: row.token_id,
            as_of,
            bids,
            asks,
            timestamp_ms: u64::try_from(row.event_time).unwrap_or(0),
            version: row.book_version,
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
    let levels: Vec<BookLevel> = pairs
        .iter()
        .filter_map(|(price, size)| {
            let price = Decimal::from_str(price).ok()?;
            let size = Decimal::from_str(size).ok()?;
            BookLevel::try_from_decimal(Price::new(price), Shares::new(size))
        })
        .collect();
    if levels.is_empty() {
        return (Arc::from([]), BookDecodeStatus::InvalidLevel);
    }
    (Arc::from(levels), BookDecodeStatus::Ok)
}

/// Convert epoch milliseconds to a UTC instant, falling back to `default`.
#[must_use]
pub fn millis_to_utc(timestamp_ms: i64, default: DateTime<Utc>) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::{BookDecodeStatus, decode_levels, snapshot_from_row};
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
    fn snapshot_from_row_malformed_bids_yields_none() {
        let row = sample_row("not-json", r#"[["0.52","100"]]"#);
        let as_of = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        let (snapshot, status) = snapshot_from_row(row, as_of);
        assert!(snapshot.is_none());
        assert_eq!(status, BookDecodeStatus::MalformedJson);
    }
}
