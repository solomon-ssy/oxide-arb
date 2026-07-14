//! External-vertical (domain) taxonomy: families, linkage lifecycle, resolver
//! tiers, and domain-observation metrics (Phase 11.2.2).
//!
//! The domain plane models category-routed alpha built on **external** data
//! sources (crypto underlying prices and airport daily-high weather). Everything
//! here is deliberately open along the four extension axes: new vertical = new [`DomainFamily`] variant, new
//! asset = resolver-ruleset entry, new oracle = [`ResolutionOracle`] variant
//! (in `domain::quant::linkage`), new feed = new `DomainSourceId` — the
//! long-format `quant_domain_observation` fact never changes shape.

use schemars::JsonSchema;

use crate::enums::common::MarketCategory;

pg_enum! {
    type_name = "qp_domain_family",
    /// An external vertical served by the domain plane.
    ///
    /// Families are routed by market category (never config-enabled the way
    /// generic factor families are) and each family owns its external feature
    /// sources and domain factors.
    @derive(JsonSchema, PartialOrd, Ord)
    pub enum DomainFamily {
        /// Crypto underlying-price vertical (Binance + Chainlink Data Streams).
        Crypto => "crypto",
        /// Airport local-calendar-day maximum-temperature vertical.
        Weather => "weather",
    }
}

pg_enum! {
    type_name = "qp_linkage_source_role",
    /// Exact role an external source plays for a resolved market linkage.
    @derive(PartialOrd, Ord)
    pub enum LinkageSourceRole {
        Feature => "feature",
        LiveEvent => "live_event",
        Resolution => "resolution",
        HistoricalCalibration => "historical_calibration",
        Forecast => "forecast",
    }
}

impl DomainFamily {
    /// Every domain family in declaration order.
    pub const ALL: [Self; 2] = [Self::Crypto, Self::Weather];

    /// The vertical a market category maps to, if any.
    ///
    /// The single authoritative category → family routing table. Categories
    /// without a vertical return `None` — their markets carry no domain slice
    /// at all (structurally absent, never a row of missing values).
    #[must_use]
    pub const fn for_category(category: MarketCategory) -> Option<Self> {
        match category {
            MarketCategory::Crypto => Some(Self::Crypto),
            MarketCategory::Weather => Some(Self::Weather),
            _ => None,
        }
    }
}

pg_enum! {
    type_name = "qp_resolver_tier",
    /// Which layer of the market-linkage resolver produced a linkage record.
    ///
    /// Deterministic tiers (`Tier0Slug` / `Tier1Template`) are pure functions
    /// of frozen market metadata. `Override` is an audited operator decision.
    ///
    /// A `Tier2Llm` variant (offline structured-extraction fallback, designed
    /// in `phase-11/11.2.3`) is deliberately **not** modeled here yet — it
    /// lands only alongside its real implementation, per the zero-dead-
    /// semantics policy (an unemitted, unmatched enum variant is a
    /// remediation blocker, not a reserved placeholder).
    @derive(JsonSchema)
    pub enum ResolverTier {
        /// Deterministic series-slug direct read (`{asset}-updown-{tf}-{epoch}`).
        Tier0Slug => "tier0_slug",
        /// Deterministic template parser over slug / question / description.
        Tier1Template => "tier1_template",
        /// Audited operator override.
        Override => "override",
    }
}

pg_enum! {
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

wire_enum! {
    /// The metric dimension of one long-format domain observation.
    ///
    /// Only metrics that are persisted **and consumed by the legacy long-form
    /// feature plane** exist here. Live crypto reports and all weather facts use
    /// dedicated typed fact tables. Candle volume is deliberately
    /// **not** modeled here — Binance klines carry it on the wire, but no
    /// domain feature consumes it; re-add it only alongside a real consumer.
    @derive(JsonSchema, PartialOrd, Ord)
    pub enum DomainMetric {
        /// Candle close price (Binance klines; the feature-source price).
        Close => "close",
    }
}

wire_enum! {
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
