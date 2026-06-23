//! Frozen market-candidate projection consumed by the research selection plane.
//!
//! [`MarketCandidate`] is the decision-time (`as_of`) freeze of every fact a
//! market selector needs: registry metadata, Gamma liquidity/volume, live
//! top-of-book prices and depth, and per-market data-quality measurements. It
//! is a neutral, serializable value owned by `quant-pivot-models` so that the
//! producer (`quant-pivot-core`, which projects [`MarketRegistry`], `BookStore`,
//! and the fact-lag tracker) and the consumer (`quant-pivot-research`'s
//! `MarketSelector`) share one contract without a layering cycle.
//!
//! # Raw facts, not decisions
//!
//! A candidate carries only *measurements*. Every threshold comparison (book
//! staleness, spread width, liquidity floor, resolution window) is the research
//! plane's policy decision, applied by the selection filters. This split keeps
//! selection deterministic and replayable: the same candidate slice plus the
//! same frozen config always yields the same snapshot.
//!
//! [`MarketRegistry`]: https://docs.rs/quant-pivot-core

use crate::{
    enums::{common::MarketCategory, market::MarketStatus},
    types::{EventId, MarketId, Price, TokenId, Usd},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A decision-time freeze of one market's selection-relevant facts.
///
/// Produced once per selection round by the core-side projector; the selector
/// never reads mutable runtime state, only this immutable slice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketCandidate {
    /// Polymarket `condition_id`.
    pub market_id: MarketId,
    /// Owning event (always present in the registry projection).
    pub event_id: EventId,
    /// Deterministic fee/selection category for this market.
    pub category: MarketCategory,
    /// Registry lifecycle status at `observed_at`.
    pub status: MarketStatus,
    /// Primary (YES) outcome token.
    pub primary_token_id: TokenId,
    /// Secondary (NO) outcome token, when the pair is known.
    pub secondary_token_id: Option<TokenId>,
    /// Scheduled resolution time, when published by the upstream source.
    pub end_date: Option<DateTime<Utc>>,
    /// Gamma-reported liquidity, when available.
    pub liquidity_usd: Option<Usd>,
    /// Gamma-reported trailing 24h volume, when available.
    pub volume_24h_usd: Option<Usd>,
    /// Live best bid on the primary token, from the published book.
    pub best_bid: Option<Price>,
    /// Live best ask on the primary token, from the published book.
    pub best_ask: Option<Price>,
    /// Combined bid+ask USD depth of the primary token's published book.
    pub depth_usd: Option<Usd>,
    /// Age of the primary token's published book, in milliseconds.
    ///
    /// `None` when no book has ever been published for the token.
    pub book_age_ms: Option<u64>,
    /// Whether the primary token's book is crossed (`best_bid >= best_ask`).
    pub crossed: bool,
    /// Whether the primary token's book is empty (no bid or no ask).
    pub empty: bool,
    /// Worst observed fact-write lag at `observed_at`, in milliseconds.
    ///
    /// This is a process-global measurement shared by all candidates in a round.
    pub fact_lag_ms: u64,
    /// The instant this candidate was frozen (equals the selection `as_of`).
    pub observed_at: DateTime<Utc>,
}

impl MarketCandidate {
    /// Seconds from `as_of` until scheduled resolution.
    ///
    /// Returns `None` when no resolution time is published or when the market
    /// has already passed its resolution time at `as_of` (non-positive window).
    #[must_use]
    pub fn seconds_to_resolution(&self, as_of: DateTime<Utc>) -> Option<u64> {
        let end = self.end_date?;
        let secs = (end - as_of).num_seconds();
        u64::try_from(secs).ok().filter(|_| secs > 0)
    }
}
