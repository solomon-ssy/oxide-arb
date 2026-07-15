//! Frozen market-candidate projection consumed by the research selection plane.
//!
//! [`MarketCandidate`] is the decision-boundary freeze of every fact a
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

/// Frozen domain-plane availability for one candidate at selection time.
///
/// Produced by the core-side projector from the category → family routing
/// table, the linkage ledger, and domain ingest health, so the selection
/// filters stay pure functions over the frozen candidate (Phase 11.2.2 §3.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainAvailability {
    /// The market's category maps to no vertical: domain features are
    /// structurally not applicable (this is never an exclusion reason).
    NotMapped,
    /// Category-mapped, but no `Resolved` linkage existed at `as_of`
    /// (fail-closed — a model requiring domain features excludes the market).
    Unresolved,
    /// Linkage resolved, but the linked instrument had no visible PIT
    /// observation at `as_of` (source gap).
    SourceEmpty,
    /// Linkage resolved and the source had PIT data at `as_of`.
    Available,
}

/// Availability semantics of the live market-data connection at decision time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataHealth {
    /// All required live shards were connected and traffic was current.
    Healthy,
    /// The live connection was degraded, so book age must be enforced.
    Unhealthy,
    /// The candidate came from durable replay; live connection health and
    /// process-local ingest lag do not apply.
    NotApplicable,
}

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
    /// Deterministic primary category for single-cohort selection and scoring.
    pub category: MarketCategory,
    /// Registry lifecycle status at `decision_at`.
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
    /// Age of the primary token's published book, in milliseconds, on the local
    /// WS receipt clock (venue clock skew / reconnect re-writes excluded).
    ///
    /// `None` when no book has ever been published for the token.
    pub book_age_ms: Option<u64>,
    /// Whether the primary token's book is crossed (`best_bid >= best_ask`), or
    /// `None` when no book was resolved.
    pub crossed: Option<bool>,
    /// Whether the resolved primary-token book is one-sided/empty, or `None`
    /// when no book was resolved.
    pub empty: Option<bool>,
    /// Live connection state at `decision_at`, or `NotApplicable` for durable
    /// replay. This must never be fabricated as a healthy boolean.
    pub market_data_health: MarketDataHealth,
    /// Worst observed ingest pipeline lag (enqueue→flush) at `decision_at`, ms.
    ///
    /// This is a process-global measurement shared by all candidates in a round.
    /// `None` only when [`MarketDataHealth::NotApplicable`].
    pub ingest_lag_ms: Option<u64>,
    /// Domain-plane availability at `decision_at` (Phase 11.2.2 §3.8).
    pub domain_availability: DomainAvailability,
    /// The decision instant at which this candidate world was frozen.
    ///
    /// Source-effective and system-availability clocks belong to the durable
    /// decision capture; this field must never be repurposed as metadata
    /// freshness or populated from a source timestamp.
    pub decision_at: DateTime<Utc>,
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
