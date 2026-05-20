//! Position tracking domain models.

use crate::enums::common::Side;
use crate::enums::risk::ReservationStatus;
use crate::types::{MarketId, Price, ReservationId, Shares, TokenId, TradeId, Usd};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Full serializable view of a token-level position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionInfo {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub size: Shares,
    pub avg_entry_price: Price,
    pub cost_basis: Usd,
    pub updated_at: DateTime<Utc>,
}

/// Capital reservation for a pending trade execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExposureReservation {
    pub reservation_id: ReservationId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub trade_id: TradeId,
    pub reserved_usd: Usd,
    pub status: ReservationStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
