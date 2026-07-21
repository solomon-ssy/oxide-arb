//! Shared inbound time-window query contract for windowed read endpoints.
//!
//! A single [`TimeWindowQuery`] replaces the per-route window/filter structs.
//! [`TimeWindowQuery::resolve`] hardens the window (default look-back, inverted
//! and over-wide rejection) and returns a domain [`QueryError`] — never a
//! web error — so the web layer maps it via `From<QueryError> for WebError`.

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::query::QueryError;
use serde::Deserialize;

use crate::{
    domain::query::{MarketFilter, TimeWindow},
    types::MarketId,
};

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

impl TimeWindowQuery {
    /// Resolve to a validated [`TimeWindow`]: `to` defaults to now, `from` to
    /// `to - default_lookback`. Rejects inverted or over-`max_days` windows.
    pub fn resolve(
        &self,
        default_lookback: Duration,
        max_days: i64,
    ) -> Result<TimeWindow, QueryError> {
        let to = self.to.unwrap_or_else(Utc::now);
        let from = self.from.unwrap_or(to - default_lookback);
        if to < from {
            return Err(QueryError::Inverted);
        }
        if to - from > Duration::days(max_days) {
            return Err(QueryError::TooWide { max_days });
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
