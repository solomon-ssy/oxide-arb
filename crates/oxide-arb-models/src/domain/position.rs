//! Position tracking domain models.
//!
//! `NewPosition` derives `DeriveIntoActiveModel` for clean DTO→ActiveModel
//! conversion. System fields (`position_id`, status, timestamps, `PnL` defaults)
//! are populated by `ActiveModelBehavior::before_save`.

use crate::enums::common::{PositionStatus, Side};
use crate::enums::risk::ReservationStatus;
use crate::types::{MarketId, Price, ReservationId, Shares, TokenId, TradeId, Usd};
use chrono::{DateTime, Utc};
use sea_orm::DeriveIntoActiveModel;
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

// ── Repository Write DTOs ────────────────────────────────────────────

/// All fields required to open a new position.
///
/// Derives `DeriveIntoActiveModel` — system fields (`position_id`, status,
/// `opened_at`, unrealized/realized `PnL` defaults) are filled by the entity's
/// `ActiveModelBehavior::before_save`.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::position::ActiveModel")]
pub struct NewPosition {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub shares: Shares,
    pub avg_entry_price: Price,
    pub total_cost_usd: Usd,
    pub total_fees_usd: Usd,
}

/// Fields that can change when a position is updated (add/reduce/close/settle).
#[derive(Debug, Clone, Default)]
pub struct UpdatePosition {
    pub shares: Option<Shares>,
    pub avg_entry_price: Option<Price>,
    pub total_cost_usd: Option<Usd>,
    pub total_fees_usd: Option<Usd>,
    pub unrealized_pnl: Option<Usd>,
    pub realized_pnl: Option<Usd>,
    pub status: Option<PositionStatus>,
    pub closed_at: Option<DateTime<Utc>>,
    pub settled_at: Option<DateTime<Utc>>,
}
