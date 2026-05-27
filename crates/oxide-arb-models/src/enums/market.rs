//! Market lifecycle enums for the data pipeline.

use oxide_arb_macros::IntoActiveValue;
use sea_orm::{DeriveActiveEnum, EnumIter};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

/// Lifecycle state of a market in the registry.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum MarketStatus {
    #[sea_orm(string_value = "discovered")]
    Discovered,
    #[sea_orm(string_value = "active")]
    Active,
    #[sea_orm(string_value = "filtered")]
    Filtered,
    #[sea_orm(string_value = "paused")]
    Paused,
    #[sea_orm(string_value = "settled")]
    Settled,
    #[sea_orm(string_value = "delisted")]
    Delisted,
}

impl Display for MarketStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Discovered => write!(f, "discovered"),
            Self::Active => write!(f, "active"),
            Self::Filtered => write!(f, "filtered"),
            Self::Paused => write!(f, "paused"),
            Self::Settled => write!(f, "settled"),
            Self::Delisted => write!(f, "delisted"),
        }
    }
}

/// Lifecycle state of an external Polymarket event.
///
/// Event status is intentionally separate from [`MarketStatus`]: events model
/// the upstream Gamma lifecycle, while markets additionally carry local
/// registry states such as discovered, filtered, and delisted.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum EventStatus {
    /// Event is open for active market discovery and scanning.
    #[sea_orm(string_value = "active")]
    Active,
    /// Event has closed to new trading.
    #[sea_orm(string_value = "closed")]
    Closed,
    /// Event has been archived by the upstream source.
    #[sea_orm(string_value = "archived")]
    Archived,
    /// Event status was not recognized by the ingestion layer.
    #[sea_orm(string_value = "unknown")]
    Unknown,
}

impl EventStatus {
    /// Canonical database and cache representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Closed => "closed",
            Self::Archived => "archived",
            Self::Unknown => "unknown",
        }
    }
}

impl Display for EventStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
