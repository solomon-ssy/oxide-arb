//! Append-only authenticated execution-fill persistence contracts.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_execution_fill,
    enums::{common::Side, execution::ExecutionOrderPhase, fee::FeeLiquidityRole},
    types::{
        ContentHash, EvmTransactionHash, ExecutionAccountId, ExecutionFillId, ExecutionOrderId,
        MarketId, OrderId, OrderIntentId, Price, Shares, TokenId, Usd, VenueTradeId,
    },
};

/// Immutable authenticated fill fact.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_execution_fill::Entity")]
pub struct ExecutionFillInfo {
    pub execution_fill_id: ExecutionFillId,
    pub execution_order_id: ExecutionOrderId,
    pub order_intent_id: OrderIntentId,
    pub execution_account_id: ExecutionAccountId,
    pub venue_trade_id: VenueTradeId,
    pub venue_bucket_index: i32,
    pub venue_order_id: OrderId,
    pub order_phase: ExecutionOrderPhase,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub liquidity_role: FeeLiquidityRole,
    pub shares: Shares,
    pub price: Price,
    pub principal_usd: Usd,
    pub matched_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub evidence_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

info_from_model!(ExecutionFillInfo, quant_execution_fill::Model, {
    execution_fill_id,
    execution_order_id,
    order_intent_id,
    execution_account_id,
    venue_trade_id,
    venue_bucket_index,
    venue_order_id,
    order_phase,
    market_id,
    token_id,
    side,
    liquidity_role,
    shares,
    price,
    principal_usd,
    matched_at,
    available_at,
    evidence_hash,
    created_at,
});

/// Insert payload for one immutable authenticated fill fact.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_execution_fill::ActiveModel")]
pub struct NewExecutionFill {
    pub execution_fill_id: ExecutionFillId,
    pub execution_order_id: ExecutionOrderId,
    pub order_intent_id: OrderIntentId,
    pub execution_account_id: ExecutionAccountId,
    pub venue_trade_id: VenueTradeId,
    pub venue_bucket_index: i32,
    pub venue_order_id: OrderId,
    pub order_phase: ExecutionOrderPhase,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub liquidity_role: FeeLiquidityRole,
    pub shares: Shares,
    pub price: Price,
    pub principal_usd: Usd,
    pub matched_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub evidence_hash: ContentHash,
}

/// Authenticated fill paired with the later, independently observed chain
/// transaction identity required for exact fee settlement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingExecutionFeeSettlement {
    pub fill: ExecutionFillInfo,
    pub transaction_hash: EvmTransactionHash,
}
