//! External-vertical (domain) taxonomy: families, linkage lifecycle, resolver
//! tiers, and domain-observation metrics (Phase 11.2.2).
//!
//! The domain plane models category-routed alpha built on **external** data
//! sources (crypto underlying prices today; sports / politics / weather /
//! geopolitics additively later). Everything here is deliberately open along
//! the four extension axes: new vertical = new [`DomainFamily`] variant, new
//! asset = resolver-ruleset entry, new oracle = [`ResolutionOracle`] variant
//! (in `domain::quant::linkage`), new feed = new `DomainSourceId` — the
//! long-format `quant_domain_observation` fact never changes shape.

use schemars::JsonSchema;

use crate::enums::common::MarketCategory;

crate::pg_enum! {
    type_name = "qp_domain_family",
    /// An external vertical served by the domain plane.
    ///
    /// Families are routed by market category (never config-enabled the way
    /// generic factor families are) and each family owns its external feature
    /// sources and domain factors.
    @derive(JsonSchema, PartialOrd, Ord)
    pub enum DomainFamily {
        /// Crypto underlying-price vertical (Binance klines + Chainlink oracle).
        Crypto => "crypto",
    }
}

impl DomainFamily {
    /// Every domain family in declaration order.
    pub const ALL: [Self; 1] = [Self::Crypto];

    /// The vertical a market category maps to, if any.
    ///
    /// The single authoritative category → family routing table. Categories
    /// without a vertical return `None` — their markets carry no domain slice
    /// at all (structurally absent, never a row of missing values).
    #[must_use]
    pub const fn for_category(category: MarketCategory) -> Option<Self> {
        match category {
            MarketCategory::Crypto => Some(Self::Crypto),
            _ => None,
        }
    }
}

crate::pg_enum! {
    type_name = "qp_resolver_tier",
    /// Which layer of the market-linkage resolver produced a linkage record.
    ///
    /// Deterministic tiers (`Tier0Slug` / `Tier1Template`) are pure functions
    /// of frozen market metadata. `Tier2Llm` is the offline-only LLM fallback
    /// (interface reserved by 11.2.2, designed in 11.2.3). `Override` is an
    /// audited operator decision.
    @derive(JsonSchema)
    pub enum ResolverTier {
        /// Deterministic series-slug direct read (`{asset}-updown-{tf}-{epoch}`).
        Tier0Slug => "tier0_slug",
        /// Deterministic template parser over slug / question / description.
        Tier1Template => "tier1_template",
        /// Offline LLM structured extraction (11.2.3; never on the PIT hot path).
        Tier2Llm => "tier2_llm",
        /// Audited operator override.
        Override => "override",
    }
}

crate::pg_enum! {
    type_name = "qp_linkage_status",
    /// Lifecycle state of a frozen market → external-subject linkage record.
    @derive(JsonSchema)
    pub enum LinkageStatus {
        /// A validated subject binding exists; the domain plane may serve it.
        Resolved => "resolved",
        /// No tier produced a validated subject — fail-closed (`DomainMissing`).
        Unresolved => "unresolved",
        /// An operator override supersedes the resolver outcome (audited).
        Overridden => "overridden",
    }
}

crate::wire_enum! {
    /// The metric dimension of one long-format domain observation.
    ///
    /// Only metrics that are actually persisted and consumed exist here (no
    /// dead semantics): candle close + volume from the Binance kline source,
    /// and the oracle spot price from the Chainlink aggregator source.
    @derive(JsonSchema, PartialOrd, Ord)
    pub enum DomainMetric {
        /// Candle close price (Binance klines; the feature-source price).
        Close => "close",
        /// Candle base-asset volume (Binance klines).
        Volume => "volume",
        /// Oracle spot price (Chainlink aggregator; basis cross-check source).
        OraclePrice => "oracle_price",
    }
}

crate::wire_enum! {
    /// Kline interval of an ingested candle series.
    ///
    /// Only intervals the ingest plane actually pulls exist here; expansion is
    /// additive (the instrument key embeds the interval label, so new intervals
    /// never change any schema).
    @derive(JsonSchema, PartialOrd, Ord)
    pub enum KlineInterval {
        /// One-minute candles (the crypto feature-source resolution).
        OneMinute => "1m",
    }
}

impl KlineInterval {
    /// Interval duration in seconds.
    #[must_use]
    pub const fn secs(self) -> u64 {
        match self {
            Self::OneMinute => 60,
        }
    }
}
