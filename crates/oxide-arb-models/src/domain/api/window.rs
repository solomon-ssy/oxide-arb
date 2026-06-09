//! Shared inbound time-window query contract for windowed read endpoints.
//!
//! A single [`TimeWindowQuery`] replaces the per-route window/filter structs.
//! [`TimeWindowQuery::resolve`] hardens the window (default look-back, inverted
//! and over-wide rejection) and returns a domain [`WindowQueryError`] — never a
//! web error — so the web layer maps it via `From<WindowQueryError> for WebError`.

use crate::{
    domain::{MarketFilter, TimeWindow},
    types::MarketId,
};
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use std::{error::Error, fmt};

/// Optional time-window + market filter for windowed read endpoints.
///
/// All fields are optional: `to` defaults to now, `from` to `to - default
/// look-back`, and `market_id` (when present) narrows the [`MarketFilter`].
#[derive(Debug, Default, Deserialize)]
pub struct TimeWindowQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub market_id: Option<MarketId>,
}

/// Why a [`TimeWindowQuery`] failed to resolve into a valid [`TimeWindow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowQueryError {
    /// `to` precedes `from`.
    Inverted,
    /// The requested span exceeds the endpoint's maximum.
    TooWide { max_days: i64 },
}

impl fmt::Display for WindowQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inverted => write!(f, "`to` must be >= `from`"),
            Self::TooWide { max_days } => write!(f, "window too wide (max {max_days} days)"),
        }
    }
}

impl Error for WindowQueryError {}

impl TimeWindowQuery {
    /// Resolve to a validated [`TimeWindow`]: `to` defaults to now, `from` to
    /// `to - default_lookback`. Rejects inverted or over-`max_days` windows.
    pub fn resolve(
        &self,
        default_lookback: Duration,
        max_days: i64,
    ) -> Result<TimeWindow, WindowQueryError> {
        let to = self.to.unwrap_or_else(Utc::now);
        let from = self.from.unwrap_or(to - default_lookback);
        if to < from {
            return Err(WindowQueryError::Inverted);
        }
        if to - from > Duration::days(max_days) {
            return Err(WindowQueryError::TooWide { max_days });
        }
        Ok(TimeWindow::new(from, to))
    }

    /// Project the optional `market_id` into a [`MarketFilter`].
    #[must_use]
    pub fn market_filter(&self) -> MarketFilter {
        MarketFilter {
            market_ids: self.market_id.clone().into_iter().collect(),
            ..MarketFilter::default()
        }
    }
}
