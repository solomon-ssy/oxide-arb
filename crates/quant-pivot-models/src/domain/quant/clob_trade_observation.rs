//! Append-only authenticated CLOB trade observation contracts.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_clob_trade_observation,
    enums::{common::Side, execution::ExecutionOrderPhase, fee::FeeLiquidityRole},
    types::{
        Bps, ClobTradeObservationId, ContentHash, ExecutionAccountId, ExecutionOrderId, MarketId,
        OrderId, OrderIntentId, Price, Shares, TokenId, Usd, VenueTradeId,
    },
};

/// Immutable authenticated fill fact.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_clob_trade_observation::Entity")]
pub struct ClobTradeObservationInfo {
    pub clob_trade_observation_id: ClobTradeObservationId,
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
    pub provisional_fee_usd: Usd,
    pub provisional_fee_rate_bps: Bps,
    pub matched_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub evidence_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

info_from_model!(ClobTradeObservationInfo, quant_clob_trade_observation::Model, {
    clob_trade_observation_id,
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
    provisional_fee_usd,
    provisional_fee_rate_bps,
    matched_at,
    available_at,
    evidence_hash,
    created_at,
});

/// Insert payload for one immutable authenticated fill fact.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_clob_trade_observation::ActiveModel")]
pub struct NewClobTradeObservation {
    pub clob_trade_observation_id: ClobTradeObservationId,
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
    pub provisional_fee_usd: Usd,
    pub provisional_fee_rate_bps: Bps,
    pub matched_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub evidence_hash: ContentHash,
}
