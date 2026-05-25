//! Position tracking domain models.
//!
//! `NewPosition` derives `DeriveIntoActiveModel` for clean DTO→ActiveModel
//! conversion. System fields (`position_id`, status, timestamps, `PnL` defaults)
//! are populated by `ActiveModelBehavior::before_save`.

use crate::enums::common::{PositionStatus, Side};
use crate::enums::risk::ReservationStatus;
use crate::types::{MarketId, PositionId, Price, ReservationId, Shares, TokenId, TradeId, Usd};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

// ── Read model ──────────────────────────────────────────────────────

/// DB row projection for the `position` table.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::position::Entity")]
pub struct PositionInfo {
    pub position_id: PositionId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub shares: Shares,
    pub avg_entry_price: Price,
    pub total_cost_usd: Usd,
    pub total_fees_usd: Usd,
    pub unrealized_pnl: Usd,
    pub realized_pnl: Usd,
    pub status: PositionStatus,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub settled_at: Option<DateTime<Utc>>,
}

info_from_model!(PositionInfo, crate::entities::position::Model, {
    position_id, market_id, token_id, side, shares, avg_entry_price,
    total_cost_usd, total_fees_usd, unrealized_pnl, realized_pnl,
    status, opened_at, closed_at, settled_at,
});

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
