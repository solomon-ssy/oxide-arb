//! Trade-tape participant facts normalized from on-chain `OrderFilled` logs.
//!
//! These types model fill-side participant observations, not execution orders.
//! They intentionally do not reuse execution/reconciliation trade structs.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChPrice, ChShares, ChUsd, TradeTapeRow},
    entities::quant_trade_tape_block_cursor,
    enums::{
        clickhouse::{
            ChTradeParticipantRole, ChTradeReconciliationStatus, ChTradeSide, ChTradeTapeSource,
        },
        common::Side,
    },
    types::{EvmAddress, MarketId, Price, Shares, TokenId, Usd},
};

/// The upstream role directly observed for a participant row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeParticipantRole {
    Maker,
    Taker,
    Unknown,
}

impl From<TradeParticipantRole> for ChTradeParticipantRole {
    fn from(value: TradeParticipantRole) -> Self {
        match value {
            TradeParticipantRole::Maker => Self::Maker,
            TradeParticipantRole::Taker => Self::Taker,
            TradeParticipantRole::Unknown => Self::Unknown,
        }
    }
}

impl From<ChTradeParticipantRole> for TradeParticipantRole {
    fn from(value: ChTradeParticipantRole) -> Self {
        match value {
            ChTradeParticipantRole::Maker => Self::Maker,
            ChTradeParticipantRole::Taker => Self::Taker,
            ChTradeParticipantRole::Unknown => Self::Unknown,
        }
    }
}

crate::pg_enum! {
    type_name = "qp_trade_tape_source_kind",
    /// Source that produced a normalized trade-tape row.
    pub enum TradeTapeSourceKind {
        MarketWs => "market_ws",
        OnChain => "on_chain",
    }
}

impl From<TradeTapeSourceKind> for ChTradeTapeSource {
    fn from(value: TradeTapeSourceKind) -> Self {
        match value {
            TradeTapeSourceKind::MarketWs => Self::MarketWs,
            TradeTapeSourceKind::OnChain => Self::OnChainOrderFilled,
        }
    }
}

impl From<ChTradeTapeSource> for TradeTapeSourceKind {
    fn from(value: ChTradeTapeSource) -> Self {
        match value {
            ChTradeTapeSource::MarketWs => Self::MarketWs,
            ChTradeTapeSource::OnChainOrderFilled => Self::OnChain,
        }
    }
}

/// Bit flags recording which upstream fields were directly observed.
pub mod trade_tape_coverage {
    pub const TRADE_ID: u16 = 1 << 0;
    pub const MARKET_ID: u16 = 1 << 1;
    pub const TOKEN_ID: u16 = 1 << 2;
    pub const PARTICIPANT_ADDRESS: u16 = 1 << 3;
    pub const PARTICIPANT_ROLE: u16 = 1 << 4;
    pub const SIDE: u16 = 1 << 5;
    pub const TX_HASH: u16 = 1 << 6;
    pub const PRICE: u16 = 1 << 7;
    pub const SIZE: u16 = 1 << 8;
    pub const FEE_RATE: u16 = 1 << 9;
}

/// One normalized participant observation in the market trade tape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeTapePrint {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub event_time: DateTime<Utc>,
    /// Time at which this revision became visible to the system.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available_at: Option<DateTime<Utc>>,
    pub participant_address: String,
    pub participant_role: TradeParticipantRole,
    pub side: Option<Side>,
    pub price: Price,
    pub size_shares: Shares,
    pub notional_usd: Usd,
    pub tx_hash: Option<String>,
    pub trade_id: String,
    pub source: TradeTapeSourceKind,
    pub coverage_flags: u16,
    pub raw_payload_json: Option<String>,
}

crate::pg_enum! {
    type_name = "qp_trade_tape_block_cursor_status",
    /// Typed lifecycle status for an on-chain trade-tape block cursor.
    pub enum TradeTapeBlockCursorStatus {
        Bootstrap => "bootstrap",
        CatchingUp => "catching_up",
        Live => "live",
        Faulted => "error",
    }
}

/// Persisted block cursor for one `(source, contract_address)` exchange stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_trade_tape_block_cursor::Entity")]
pub struct TradeTapeBlockCursorInfo {
    pub source: TradeTapeSourceKind,
    pub contract_address: EvmAddress,
    pub last_finalized_block: i64,
    pub last_log_index: i32,
    pub head_lag_blocks: i64,
    pub status: TradeTapeBlockCursorStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    TradeTapeBlockCursorInfo,
    quant_trade_tape_block_cursor::Model,
    {
        source,
        contract_address,
        last_finalized_block,
        last_log_index,
        head_lag_blocks,
        status,
        created_at,
        updated_at,
    }
);

/// Upsert payload for the durable on-chain block cursor.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_trade_tape_block_cursor::ActiveModel")]
pub struct UpsertTradeTapeBlockCursor {
    pub source: TradeTapeSourceKind,
    pub contract_address: EvmAddress,
    pub last_finalized_block: i64,
    pub last_log_index: i32,
    pub head_lag_blocks: i64,
    pub status: TradeTapeBlockCursorStatus,
    pub updated_at: DateTime<Utc>,
}

impl TradeTapePrint {
    /// Decode a `ClickHouse` trade-tape row into the domain print shape.
    #[must_use]
    pub fn from_clickhouse_row_at(
        row: &TradeTapeRow,
        event_time: DateTime<Utc>,
        available_at: DateTime<Utc>,
    ) -> Self {
        Self {
            market_id: row.market_id.clone(),
            token_id: row.token_id.clone(),
            event_time,
            available_at: Some(available_at),
            participant_address: row.participant_address.clone(),
            participant_role: row.participant_role.into(),
            side: ch_trade_side_to_domain(row.side),
            price: row.price.to_price(),
            size_shares: row.size_shares.to_shares(),
            notional_usd: row.notional_usd.to_usd(),
            tx_hash: row.tx_hash.clone(),
            trade_id: row.source_event_id.clone(),
            source: TradeTapeSourceKind::from(row.source),
            coverage_flags: row.observed_field_flags,
            raw_payload_json: row.raw_payload_json.clone(),
        }
    }

    /// Convert to the `ClickHouse` fact row shape at write time.
    #[must_use]
    pub fn into_clickhouse_row(self, ingestion_time: DateTime<Utc>) -> TradeTapeRow {
        TradeTapeRow {
            market_id: self.market_id,
            token_id: self.token_id,
            event_time: self.event_time.timestamp_millis(),
            ingestion_time: ingestion_time.timestamp_millis(),
            stream_session_id: None,
            token_sequence: None,
            participant_address: self.participant_address,
            participant_role: self.participant_role.into(),
            side: ch_trade_side(self.side),
            price: ChPrice::from(self.price),
            size_shares: ChShares::from(self.size_shares),
            notional_usd: ChUsd::from(self.notional_usd),
            tx_hash: self.tx_hash,
            source_event_id: self.trade_id,
            source: self.source.into(),
            observed_field_flags: self.coverage_flags,
            fee_rate_bps: None,
            reconciliation_status: ChTradeReconciliationStatus::OnChainOnly,
            matched_source_event_id: None,
            revision: 1,
            reconciled_at: None,
            raw_payload_json: self.raw_payload_json,
            schema_version: TradeTapeRow::SCHEMA_VERSION,
        }
    }

    /// Participant notional used by concentration estimators.
    #[must_use]
    pub const fn participant_notional(&self) -> Decimal {
        self.notional_usd.inner()
    }
}

#[must_use]
pub const fn ch_trade_side(side: Option<Side>) -> ChTradeSide {
    match side {
        Some(Side::Buy) => ChTradeSide::Buy,
        Some(Side::Sell) => ChTradeSide::Sell,
        None => ChTradeSide::Unknown,
    }
}

#[must_use]
pub const fn ch_trade_side_to_domain(side: ChTradeSide) -> Option<Side> {
    match side {
        ChTradeSide::Buy => Some(Side::Buy),
        ChTradeSide::Sell => Some(Side::Sell),
        ChTradeSide::Unknown => None,
    }
}
