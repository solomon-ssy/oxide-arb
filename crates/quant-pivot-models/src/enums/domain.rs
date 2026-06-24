//! Vertical (domain) taxonomy shared by the feature and factor planes.
//!
//! A single [`DomainFamily`] enumerates the verticals quant-pivot models. Both
//! the feature plane (domain feature builders) and the factor plane (domain
//! factor families) key off this type so the two planes never drift into
//! parallel vertical enums.

use std::fmt::{self, Display, Formatter};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::enums::common::MarketCategory;

/// A vertical/domain category whose markets share specialized signals.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DomainFamily {
    /// Sports markets (pre-match moves, live score shocks).
    Sports,
    /// Political / election markets (poll momentum, event deadlines).
    Politics,
    /// Crypto-price markets (underlying beta, risk-on proxies).
    Crypto,
    /// Weather markets (forecast revisions).
    Weather,
    /// Geopolitics markets (news-shock decay).
    Geopolitics,
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

    /// The stable `snake_case` identifier (matches the serde representation).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sports => "sports",
            Self::Politics => "politics",
            Self::Crypto => "crypto",
            Self::Weather => "weather",
            Self::Geopolitics => "geopolitics",
        }
    }

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

impl Display for DomainFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
