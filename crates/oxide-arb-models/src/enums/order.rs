//! Order enums for Polymarket CLOB interaction.

use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

/// Status of an order on the CLOB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// Order accepted and fully filled.
    Filled,
    /// Order partially filled (FOK would have been killed).
    PartiallyFilled,
    /// Order rejected by the exchange.
    Rejected,
    /// Order cancelled (e.g. FAK remainder).
    Cancelled,
    /// Order is resting on the book (GTC/GTD).
    Open,
    /// Order expired (GTD past deadline).
    Expired,
}

impl Display for OrderStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Filled => write!(f, "filled"),
            Self::PartiallyFilled => write!(f, "partially_filled"),
            Self::Rejected => write!(f, "rejected"),
            Self::Cancelled => write!(f, "cancelled"),
            Self::Open => write!(f, "open"),
            Self::Expired => write!(f, "expired"),
        }
    }
}
