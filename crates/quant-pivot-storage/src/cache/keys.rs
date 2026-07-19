//! Type-safe cache key builder with embedded TTL and domain labels.
//!
//! Each variant encodes a unique namespace prefix, a per-domain TTL for L2
//! (Redis), and a domain label for metrics partitioning. L1 (Moka) TTL is
//! derived as `ttl / 4` by the [`TieredCache`] layer.

use quant_pivot_models::types::{EventId, MarketId};
use std::time::Duration;

/// Strongly-typed cache key encompassing all cacheable domains.
///
/// Key format: `"{domain_prefix}:{discriminator}"`.
/// TTL semantics: represents the L2 (Redis) expiry; L1 uses `ttl / 4`.
pub enum CacheKey {
    // ── Market data ─────────────────────────────────────────────────────
    /// Cached market metadata (category, tokens, settlement info).
    MarketInfo { market_id: MarketId },

    /// Cached event entry (event-level metadata aggregate).
    EventInfo { event_id: EventId },

    /// Market metadata used by detection and scoring (category, deadline).
    MarketMetadata { market_id: MarketId },
}

impl CacheKey {
    /// Render the cache key as a string suitable for both L1 and L2 backends.
    pub fn as_str(&self) -> String {
        match self {
            Self::MarketInfo { market_id } => format!("mkt:{market_id}"),
            Self::EventInfo { event_id } => format!("evt:{event_id}"),
            Self::MarketMetadata { market_id } => format!("mkt_meta:{market_id}"),
        }
    }

    /// L2 (Redis) TTL for this key variant.
    ///
    /// L1 (Moka) TTL is derived as `ttl / 4` by the tiered cache layer.
    pub const fn ttl(&self) -> Duration {
        match self {
            Self::MarketInfo { .. } | Self::EventInfo { .. } => Duration::from_mins(5),
            Self::MarketMetadata { .. } => Duration::from_mins(30),
        }
    }

    /// Domain label used for metrics partitioning (cache hit/miss counters).
    pub const fn domain(&self) -> &'static str {
        match self {
            Self::MarketInfo { .. } => "market",
            Self::EventInfo { .. } => "event",
            Self::MarketMetadata { .. } => "market_metadata",
        }
    }
}
