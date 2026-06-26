//! Vertical (domain) taxonomy shared by the feature and factor planes.
//!
//! A single [`DomainFamily`] enumerates the verticals quant-pivot models. Both
//! the feature plane (domain feature builders) and the factor plane (domain
//! factor families) key off this type so the two planes never drift into
//! parallel vertical enums.

use crate::enums::common::MarketCategory;

crate::wire_enum! {
    /// A vertical/domain category whose markets share specialized signals.
    @derive(PartialOrd, Ord, schemars::JsonSchema)
    pub enum DomainFamily {
        /// Sports markets (pre-match moves, live score shocks).
        Sports => "sports",
        /// Political / election markets (poll momentum, event deadlines).
        Politics => "politics",
        /// Crypto-price markets (underlying beta, risk-on proxies).
        Crypto => "crypto",
        /// Weather markets (forecast revisions).
        Weather => "weather",
        /// Geopolitics markets (news-shock decay).
        Geopolitics => "geopolitics",
    }
}

impl DomainFamily {
    /// Every vertical in declaration order.
    pub const ALL: [Self; 5] = [
        Self::Sports,
        Self::Politics,
        Self::Crypto,
        Self::Weather,
        Self::Geopolitics,
    ];

    /// The vertical a market category maps to, when one is modeled.
    ///
    /// Categories without a dedicated vertical (finance, tech, culture,
    /// economics, other) return `None` and are served by generic features only.
    #[must_use]
    pub const fn for_category(category: MarketCategory) -> Option<Self> {
        match category {
            MarketCategory::Sports => Some(Self::Sports),
            MarketCategory::Politics => Some(Self::Politics),
            MarketCategory::Crypto => Some(Self::Crypto),
            MarketCategory::Weather => Some(Self::Weather),
            MarketCategory::Geopolitics => Some(Self::Geopolitics),
            MarketCategory::Finance
            | MarketCategory::Tech
            | MarketCategory::Culture
            | MarketCategory::Economics
            | MarketCategory::Other => None,
        }
    }
}
