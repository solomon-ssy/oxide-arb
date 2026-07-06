//! Cross-resource read-filter domain types.
//!
//! These are the canonical read-query primitives shared by the API contract
//! layer (`domain::api` `*WindowQuery::resolve()` produces them) and the
//! repository read methods (evidence timeseries, analytics) that consume them.
//! They live in `quant-pivot-models` — the lowest layer — so the API contract can
//! resolve into them without depending on `quant-pivot-repository`.

use crate::{
    enums::{common::MarketCategory, quant::QuantRuntimeMode},
    types::{EventId, MarketId, TokenId},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Windowed trade aggregation scope for analytics endpoints.
#[derive(Debug, Clone, Copy)]
pub struct TradeAnalyticsFilter {
    /// Half-open UTC execution window `[from, to)`.
    pub window: TimeWindow,
    /// When `None`, all execution modes are included.
    pub runtime_mode: Option<QuantRuntimeMode>,
}

/// A time window for domain reads and offline materialization.
///
/// **Query / analytics** endpoints resolve optional bounds via
/// [`TimeWindowQuery`](crate::domain::api::TimeWindowQuery) and allow
/// zero-width inclusive spans (`from == to`).
///
/// **Mutation bodies** (dataset build, bias-table fit, …) use half-open
/// `[from, to)` semantics: samples satisfy `from <= t < to`. Construct those
/// with [`Self::try_half_open`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeWindow {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

/// Why explicit half-open window bounds failed validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowBoundsError {
    /// `end <= start` — empty or inverted half-open span.
    EmptyOrInverted,
}

impl WindowBoundsError {
    /// Wire + validator message for HTTP 400 responses.
    pub const MESSAGE: &'static str = "window_end must be after window_start";
}

impl TimeWindow {
    /// Construct a window over an explicit `[from, to]` range (caller-validated).
    #[must_use]
    pub const fn new(from: DateTime<Utc>, to: DateTime<Utc>) -> Self {
        Self { from, to }
    }

    /// Construct a half-open `[from, to)` window; rejects empty/inverted spans.
    pub fn try_half_open(
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Self, WindowBoundsError> {
        if to <= from {
            return Err(WindowBoundsError::EmptyOrInverted);
        }
        Ok(Self::new(from, to))
    }
}

/// AND-combined market scoping filter for windowed reads. An empty vector means
/// "no constraint on that dimension"; a fully-default filter matches everything.
#[derive(Debug, Clone, Default, Serialize)]
pub struct MarketFilter {
    pub market_ids: Vec<MarketId>,
    pub event_ids: Vec<EventId>,
    pub token_ids: Vec<TokenId>,
    pub categories: Vec<MarketCategory>,
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{TimeWindow, WindowBoundsError};

    #[test]
    fn try_half_open_rejects_empty_and_inverted() {
        let start = Utc.timestamp_opt(100, 0).unwrap();
        assert_eq!(
            TimeWindow::try_half_open(start, start),
            Err(WindowBoundsError::EmptyOrInverted)
        );
        assert_eq!(
            TimeWindow::try_half_open(start, Utc.timestamp_opt(50, 0).unwrap()),
            Err(WindowBoundsError::EmptyOrInverted)
        );
    }

    #[test]
    fn try_half_open_accepts_positive_span() {
        let start = Utc.timestamp_opt(100, 0).unwrap();
        let end = Utc.timestamp_opt(200, 0).unwrap();
        assert_eq!(
            TimeWindow::try_half_open(start, end),
            Ok(TimeWindow::new(start, end))
        );
    }
}
