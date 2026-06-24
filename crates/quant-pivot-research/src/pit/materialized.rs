//! In-memory point-in-time engine over a pre-fetched window.
//!
//! The offline dataset builder batch-reads every book snapshot / market context
//! it will need once, then serves per-sample point-in-time lookups from memory
//! through this engine — so the build loop issues **zero** database queries and
//! produces byte-identical features to the online path.
//!
//! **Immutability:** this type is a frozen snapshot for one dataset build. It is
//! not updated when live books or catalog rows change at runtime; online /
//! streaming replay uses `ChHistoricalPitSource` (`quant-pivot-core`) instead.
//! That separation keeps offline builds reproducible (prefetch window + plan
//! hash fully determine the materialized state) while the streaming source can
//! observe fresh facts.

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::types::{MarketId, TokenId};

use super::{BookSnapshotAt, MarketContextAt, PitQueryEngine};

/// Serves point-in-time book / market lookups from a pre-fetched window.
///
/// Each per-key series is kept ascending by observed time; a lookup returns the
/// freshest entry at or before `as_of` (never look-ahead). A book older than
/// `max_book_staleness` relative to `as_of` is treated as missing, mirroring the
/// streaming `ChHistoricalPitSource` so online and offline resolution agree.
pub struct MaterializedPitEngine {
    books: HashMap<TokenId, Vec<BookSnapshotAt>>,
    markets: HashMap<MarketId, Vec<MarketContextAt>>,
    max_book_staleness: Duration,
}

impl MaterializedPitEngine {
    /// Build from pre-fetched book / market series.
    ///
    /// Each series is sorted defensively: books by `timestamp_ms`, markets by
    /// `observed_at`, both ascending, so the point-in-time lookup is a reverse
    /// scan for the first entry within the cutoff.
    #[must_use]
    pub fn new(
        mut books: HashMap<TokenId, Vec<BookSnapshotAt>>,
        mut markets: HashMap<MarketId, Vec<MarketContextAt>>,
        max_book_staleness: Duration,
    ) -> Self {
        for series in books.values_mut() {
            series.sort_by_key(|snapshot| snapshot.timestamp_ms);
        }
        for series in markets.values_mut() {
            series.sort_by_key(|context| context.observed_at);
        }
        Self {
            books,
            markets,
            max_book_staleness,
        }
    }
}

#[async_trait]
impl PitQueryEngine for MaterializedPitEngine {
    async fn book_at(
        &self,
        token_id: &TokenId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        let cutoff_ms = as_of.timestamp_millis();
        let min_ms = (as_of - self.max_book_staleness).timestamp_millis();
        let resolved = self.books.get(token_id).and_then(|series| {
            series
                .iter()
                .rev()
                .find(|snapshot| ms_le(snapshot.timestamp_ms, cutoff_ms))
                .filter(|snapshot| ms_ge(snapshot.timestamp_ms, min_ms))
                .map(|snapshot| BookSnapshotAt {
                    as_of,
                    ..snapshot.clone()
                })
        });
        Ok(resolved)
    }

    async fn market_at(
        &self,
        market_id: &MarketId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<MarketContextAt>> {
        let resolved = self.markets.get(market_id).and_then(|series| {
            series
                .iter()
                .rev()
                .find(|context| context.observed_at <= as_of)
                .map(|context| MarketContextAt {
                    as_of,
                    ..context.clone()
                })
        });
        Ok(resolved)
    }
}

/// Whether an epoch-millisecond `timestamp_ms` is at or before `cutoff_ms`.
fn ms_le(timestamp_ms: u64, cutoff_ms: i64) -> bool {
    i64::try_from(timestamp_ms).is_ok_and(|ms| ms <= cutoff_ms)
}

/// Whether an epoch-millisecond `timestamp_ms` is at or after `min_ms`.
fn ms_ge(timestamp_ms: u64, min_ms: i64) -> bool {
    i64::try_from(timestamp_ms).map_or(true, |ms| ms >= min_ms)
}
