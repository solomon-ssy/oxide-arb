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
    /// Milliseconds since the book snapshot's event time.
    pub book_age_ms: u64,
    /// Best bid is at or above best ask (inconsistent book).
    pub crossed: bool,
    /// One or both sides have no levels.
    pub empty: bool,
    /// Optional ingest-side lag (`ingestion_time - event_time`) for this token.
    pub fact_lag_ms: Option<u64>,
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
    /// Active maximum acceptable fact lag (ms) from runtime config.
    pub max_fact_lag_ms: u64,
    /// Peak ingest-side fact lag (ms) observed in the current book plane.
    pub worst_fact_lag_ms: u64,
    /// True when `worst_fact_lag_ms` exceeds `max_fact_lag_ms`.
    pub fact_lag_exceeded: bool,
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
    pub const fn empty(as_of: DateTime<Utc>, max_book_age_ms: u64, max_fact_lag_ms: u64) -> Self {
        Self {
            as_of,
            total_tokens: 0,
            fresh: 0,
            acceptable: 0,
            degraded: 0,
            stale: 0,
            insufficient: 0,
            max_book_age_ms,
            max_fact_lag_ms,
            worst_fact_lag_ms: 0,
            fact_lag_exceeded: false,
        }
    }
}
