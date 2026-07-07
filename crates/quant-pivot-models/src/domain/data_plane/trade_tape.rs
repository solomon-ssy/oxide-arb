//! Trade-tape participant facts normalized from on-chain `OrderFilled` logs.
//!
//! These types model fill-side participant observations, not execution orders.
//! They intentionally do not reuse execution/reconciliation trade structs.

use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChPrice, ChSchemaVersion, ChShares, ChUsd, TradeTapeRow},
    enums::{
        clickhouse::{ChTradeParticipantRole, ChTradeSide, ChTradeTapeSource},
        common::Side,
    },
    types::{MarketId, Price, Shares, TokenId, Usd},
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

/// Source that produced a normalized trade-tape row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeTapeSourceKind {
    OnChain,
}

impl From<TradeTapeSourceKind> for ChTradeTapeSource {
    fn from(value: TradeTapeSourceKind) -> Self {
        match value {
            TradeTapeSourceKind::OnChain => Self::OnChain,
        }
    }
}

impl From<ChTradeTapeSource> for TradeTapeSourceKind {
    fn from(value: ChTradeTapeSource) -> Self {
        match value {
            ChTradeTapeSource::OnChain => Self::OnChain,
        }
    }
}

impl TradeTapeSourceKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OnChain => "on_chain",
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
}

/// One normalized participant observation in the market trade tape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeTapePrint {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub event_time: DateTime<Utc>,
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

/// Typed lifecycle status for an on-chain trade-tape block cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeTapeBlockCursorStatus {
    Bootstrap,
    CatchingUp,
    Live,
    Error,
}

impl TradeTapeBlockCursorStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bootstrap => "bootstrap",
            Self::CatchingUp => "catching_up",
            Self::Live => "live",
            Self::Error => "error",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "bootstrap" => Some(Self::Bootstrap),
            "catching_up" => Some(Self::CatchingUp),
            "live" => Some(Self::Live),
            "error" => Some(Self::Error),
            _ => None,
        }
    }
}

/// Persisted block cursor for one `(source, contract_address)` exchange stream.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel, FromQueryResult,
)]
#[sea_orm(entity = "crate::entities::quant_trade_tape_block_cursor::Entity")]
pub struct TradeTapeBlockCursorInfo {
    pub source: String,
    pub contract_address: String,
    pub last_finalized_block: i64,
    pub last_log_index: i32,
    pub head_lag_blocks: i64,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    TradeTapeBlockCursorInfo,
    crate::entities::quant_trade_tape_block_cursor::Model,
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
    pub source: String,
    pub contract_address: String,
    pub last_finalized_block: i64,
    pub last_log_index: i32,
    pub head_lag_blocks: i64,
    pub status: String,
    pub updated_at: DateTime<Utc>,
}

impl TradeTapePrint {
    /// Decode a `ClickHouse` trade-tape row into the domain print shape.
    #[must_use]
    pub fn from_clickhouse_row(row: &TradeTapeRow, default_time: DateTime<Utc>) -> Self {
        Self {
            market_id: row.market_id.clone(),
            token_id: row.token_id.clone(),
            event_time: millis_to_utc(row.event_time, default_time),
            participant_address: row.participant_address.clone(),
            participant_role: row.participant_role.into(),
            side: ch_trade_side_to_domain(row.side),
            price: row.price.to_price(),
            size_shares: row.size_shares.to_shares(),
            notional_usd: row.notional_usd.to_usd(),
            tx_hash: row.tx_hash.clone(),
            trade_id: row.trade_id.clone(),
            source: TradeTapeSourceKind::from(row.source),
            coverage_flags: row.coverage_flags,
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
            participant_address: self.participant_address,
            participant_role: self.participant_role.into(),
            side: ch_trade_side(self.side),
            price: ChPrice::from(self.price),
            size_shares: ChShares::from(self.size_shares),
            notional_usd: ChUsd::from(self.notional_usd),
            tx_hash: self.tx_hash,
            trade_id: self.trade_id,
            source: self.source.into(),
            coverage_flags: self.coverage_flags,
            raw_payload_json: self.raw_payload_json,
            schema_version: ChSchemaVersion::FIRST,
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

fn millis_to_utc(timestamp_ms: i64, default: DateTime<Utc>) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .unwrap_or(default)
}
