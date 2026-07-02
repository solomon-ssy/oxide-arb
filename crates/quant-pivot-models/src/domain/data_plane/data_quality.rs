//! Point-in-time data-quality classification of the live book plane.
//!
//! The dominant data-quality signals are book **staleness** (age against the
//! runtime staleness ladder) and structural validity (**empty** / **crossed**
//! books). Depth-in-USD gating against `min_book_depth_usd` is a Phase 3
//! refinement (TODO) once feature builders consume per-level notionals.

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{
    enums::{common::StalenessLevel, quant::DataQualityStatus},
    types::TokenId,
};

/// Classifier input for one token's current book.
#[derive(Debug, Clone)]
pub struct DataQualityInput {
    pub token_id: TokenId,
    /// Milliseconds since the token's last observed book update (local receipt
    /// clock). On Polymarket a quiet book is not resent, so age reflects trading
    /// activity, not data health — hence it only downgrades when the connection
    /// is unhealthy (see `connection_healthy`).
    pub book_age_ms: u64,
    /// Best bid is at or above best ask (inconsistent book).
    pub crossed: bool,
    /// One or both sides have no levels.
    pub empty: bool,
    /// Whether the market-data connection feeding this token is healthy (traffic
    /// fresh within the WS threshold AND all shards connected). When healthy, a
    /// merely-quiet book stays usable; when unhealthy, an aged book is stale.
    pub connection_healthy: bool,
}

/// Per-token data-quality classification.
#[derive(Debug, Clone, Serialize)]
pub struct DataQualityReport {
    pub token_id: TokenId,
    pub status: DataQualityStatus,
    pub staleness: StalenessLevel,
    pub book_age_ms: u64,
    pub crossed: bool,
    pub empty: bool,
}

/// Aggregate data-quality snapshot across the live book plane (operator/API view).
#[derive(Debug, Clone, Serialize)]
pub struct DataQualitySnapshot {
    pub as_of: DateTime<Utc>,
    pub total_tokens: u64,
    pub fresh: u64,
    pub acceptable: u64,
    pub degraded: u64,
    pub stale: u64,
    pub insufficient: u64,
    /// Active acceptable book-age threshold (ms) from runtime config.
    pub max_book_age_ms: u64,
    /// Worst book age (ms) actually observed across the live plane at snapshot
    /// time (max over tokens) — the true "worst book latency", not a threshold.
    pub worst_book_age_ms: u64,
    /// Active max acceptable ingest pipeline lag (ms) from runtime config.
    pub max_ingest_lag_ms: u64,
    /// Peak ingest pipeline lag (enqueue→flush, ms) observed in the current
    /// book plane. Measures `ClickHouse` persistence backpressure, not book age.
    pub worst_ingest_lag_ms: u64,
    /// True when `worst_ingest_lag_ms` exceeds `max_ingest_lag_ms`.
    pub ingest_lag_exceeded: bool,
}

impl DataQualitySnapshot {
    /// Tally one report's status into the aggregate counters.
    pub const fn tally(&mut self, status: DataQualityStatus) {
        self.total_tokens += 1;
        match status {
            DataQualityStatus::Fresh => self.fresh += 1,
            DataQualityStatus::Acceptable => self.acceptable += 1,
            DataQualityStatus::Degraded => self.degraded += 1,
            DataQualityStatus::Stale => self.stale += 1,
            DataQualityStatus::Insufficient => self.insufficient += 1,
        }
    }

    /// Empty snapshot anchored at `as_of` with the active data-quality thresholds.
    #[must_use]
    pub const fn empty(as_of: DateTime<Utc>, max_book_age_ms: u64, max_ingest_lag_ms: u64) -> Self {
        Self {
            as_of,
            total_tokens: 0,
            fresh: 0,
            acceptable: 0,
            degraded: 0,
            stale: 0,
            insufficient: 0,
            max_book_age_ms,
            worst_book_age_ms: 0,
            max_ingest_lag_ms,
            worst_ingest_lag_ms: 0,
            ingest_lag_exceeded: false,
        }
    }
}
