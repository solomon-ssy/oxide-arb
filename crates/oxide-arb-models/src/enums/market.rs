//! Market lifecycle enums for the data pipeline.

use oxide_arb_macros::IntoActiveValue;
use sea_orm::{DeriveActiveEnum, EnumIter};
use serde::{Deserialize, Serialize};

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

impl std::fmt::Display for MarketStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
