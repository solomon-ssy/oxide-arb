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
use serde::Serialize;

/// Windowed trade aggregation scope for analytics endpoints.
#[derive(Debug, Clone, Copy)]
pub struct TradeAnalyticsFilter {
    /// Half-open UTC execution window `[from, to)`.
    pub window: TimeWindow,
    /// When `None`, all execution modes are included.
    pub runtime_mode: Option<QuantRuntimeMode>,
}

/// A closed `[from, to]` time window for windowed reads.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct TimeWindow {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

impl TimeWindow {
    /// Construct a window over an explicit `[from, to]` range.
    #[must_use]
    pub const fn new(from: DateTime<Utc>, to: DateTime<Utc>) -> Self {
        Self { from, to }
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
