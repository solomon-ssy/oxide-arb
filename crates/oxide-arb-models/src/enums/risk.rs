//! Risk engine enums — circuit breakers, blacklists, exposure reservations.

use oxide_arb_macros::IntoActiveValue;
use sea_orm::{DeriveActiveEnum, EnumIter};
use serde::{Deserialize, Serialize};

/// Persisted top-level state of the 4-state circuit breaker FSM.
///
/// ```text
/// Closed ──trip──▶ Open ──cooldown expires──▶ HalfOpen ──probes pass──▶ Recovered
///   ▲                ▲                           │                         │
///   │                └───── probe fails ──────────┘                         │
///   └──────────────── observation period expires ──────────────────────────┘
/// ```
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
pub enum BreakerStateName {
    /// Normal operation — execution permitted.
    #[sea_orm(string_value = "closed")]
    Closed,
    /// Tripped — execution blocked, cooldown timer running.
    #[sea_orm(string_value = "open")]
    Open,
    /// Cooldown expired — allowing probe trades to test recovery.
    #[sea_orm(string_value = "half_open")]
    HalfOpen,
    /// Probes succeeded — observation period before returning to Closed.
    #[sea_orm(string_value = "recovered")]
    Recovered,
}

impl std::fmt::Display for BreakerStateName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => f.write_str("closed"),
            Self::Open => f.write_str("open"),
            Self::HalfOpen => f.write_str("half_open"),
            Self::Recovered => f.write_str("recovered"),
        }
    }
}

/// Severity level of a circuit-breaker trip (1–4).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum CircuitBreakerLevel {
    #[sea_orm(string_value = "trade")]
    Trade = 1,
    #[sea_orm(string_value = "session")]
    Session = 2,
    #[sea_orm(string_value = "daily")]
    Daily = 3,
    #[sea_orm(string_value = "system")]
    System = 4,
}

impl CircuitBreakerLevel {
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Trade => 1,
            Self::Session => 2,
            Self::Daily => 3,
            Self::System => 4,
        }
    }
}

impl std::fmt::Display for CircuitBreakerLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Trade => f.write_str("trade"),
            Self::Session => f.write_str("session"),
            Self::Daily => f.write_str("daily"),
            Self::System => f.write_str("system"),
        }
    }
}

/// Severity scope for a blacklist entry.
///
/// Ordered so that higher scopes include all lower ones via `>=` comparison.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
    bitcode::Encode,
    bitcode::Decode,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum BlacklistScope {
    #[sea_orm(string_value = "data_path")]
    DataPath = 0,
    #[sea_orm(string_value = "trading_path")]
    TradingPath = 1,
    #[sea_orm(string_value = "full")]
    Full = 2,
}

impl std::fmt::Display for BlacklistScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DataPath => f.write_str("data_path"),
            Self::TradingPath => f.write_str("trading_path"),
            Self::Full => f.write_str("full"),
        }
    }
}

/// Why a market or token was added to the blacklist.
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
    bitcode::Encode,
    bitcode::Decode,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum BlacklistReason {
    #[sea_orm(string_value = "consecutive_fok_failures")]
    ConsecutiveFokFailures,
    #[sea_orm(string_value = "trade_failed_after_matched")]
    TradeFailedAfterMatched,
    #[sea_orm(string_value = "depth_drop")]
    DepthDrop,
    #[sea_orm(string_value = "tick_change")]
    TickChange,
    #[sea_orm(string_value = "manual")]
    Manual,
    #[sea_orm(string_value = "data_not_found")]
    DataNotFound,
}

impl std::fmt::Display for BlacklistReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConsecutiveFokFailures => f.write_str("consecutive_fok_failures"),
            Self::TradeFailedAfterMatched => f.write_str("trade_failed_after_matched"),
            Self::DepthDrop => f.write_str("depth_drop"),
            Self::TickChange => f.write_str("tick_change"),
            Self::Manual => f.write_str("manual"),
            Self::DataNotFound => f.write_str("data_not_found"),
        }
    }
}

/// Distinguishes fill-time vs. settlement-time trade accounting.
///
/// Endgame convergence trades have two accounting phases:
/// - `Fill`: cost/volume recorded, potential loss entry created, but no
///   realized profit flows into daily/weekly loss caps (settlement hasn't
///   happened yet).
/// - `Settlement`: realized profit recorded, potential loss resolved,
///   breaker checks triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeAccountingPhase {
    /// Trade was filled — record cost, counts, potential loss. No realized profit.
    Fill,
    /// Market settled — record realized profit, resolve potential loss, check caps.
    Settlement,
}

impl std::fmt::Display for TradeAccountingPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fill => f.write_str("fill"),
            Self::Settlement => f.write_str("settlement"),
        }
    }
}

/// Overall outcome of a balance/exposure reconciliation run.
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
pub enum ReconciliationStatus {
    #[sea_orm(string_value = "ok")]
    Ok,
    #[sea_orm(string_value = "warning")]
    Warning,
    #[sea_orm(string_value = "critical")]
    Critical,
}

impl std::fmt::Display for ReconciliationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ok => f.write_str("ok"),
            Self::Warning => f.write_str("warning"),
            Self::Critical => f.write_str("critical"),
        }
    }
}

/// Type of risk audit event persisted for post-mortem analysis.
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
pub enum RiskAuditEventType {
    #[sea_orm(string_value = "trade_allowed")]
    TradeAllowed,
    #[sea_orm(string_value = "trade_denied")]
    TradeDenied,
    #[sea_orm(string_value = "breaker_tripped")]
    BreakerTripped,
    #[sea_orm(string_value = "breaker_recovered")]
    BreakerRecovered,
    #[sea_orm(string_value = "breaker_reset")]
    BreakerReset,
    #[sea_orm(string_value = "blacklist_added")]
    BlacklistAdded,
    #[sea_orm(string_value = "blacklist_removed")]
    BlacklistRemoved,
    #[sea_orm(string_value = "accounting_rollover")]
    AccountingRollover,
    #[sea_orm(string_value = "reconciliation_completed")]
    ReconciliationCompleted,
    #[sea_orm(string_value = "engine_halted")]
    EngineHalted,
    #[sea_orm(string_value = "engine_resumed")]
    EngineResumed,
    #[sea_orm(string_value = "post_trade_update")]
    PostTradeUpdate,
}

impl std::fmt::Display for RiskAuditEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TradeAllowed => f.write_str("trade_allowed"),
            Self::TradeDenied => f.write_str("trade_denied"),
            Self::BreakerTripped => f.write_str("breaker_tripped"),
            Self::BreakerRecovered => f.write_str("breaker_recovered"),
            Self::BreakerReset => f.write_str("breaker_reset"),
            Self::BlacklistAdded => f.write_str("blacklist_added"),
            Self::BlacklistRemoved => f.write_str("blacklist_removed"),
            Self::AccountingRollover => f.write_str("accounting_rollover"),
            Self::ReconciliationCompleted => f.write_str("reconciliation_completed"),
            Self::EngineHalted => f.write_str("engine_halted"),
            Self::EngineResumed => f.write_str("engine_resumed"),
            Self::PostTradeUpdate => f.write_str("post_trade_update"),
        }
    }
}

/// Lifecycle state of an exposure reservation.
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
pub enum ReservationStatus {
    #[sea_orm(string_value = "pending")]
    Pending,
    #[sea_orm(string_value = "confirmed")]
    Confirmed,
    #[sea_orm(string_value = "released")]
    Released,
}

impl std::fmt::Display for ReservationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Confirmed => f.write_str("confirmed"),
            Self::Released => f.write_str("released"),
        }
    }
}

/// Granularity of a time-windowed risk accumulator.
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
pub enum WindowType {
    #[sea_orm(string_value = "hourly")]
    Hourly,
    #[sea_orm(string_value = "daily")]
    Daily,
    #[sea_orm(string_value = "weekly")]
    Weekly,
}

impl WindowType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hourly => "hourly",
            Self::Daily => "daily",
            Self::Weekly => "weekly",
        }
    }
}

impl std::fmt::Display for WindowType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
