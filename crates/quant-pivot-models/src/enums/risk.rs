//! Risk engine enums — circuit breakers, blacklists, exposure reservations.

use std::fmt::{self, Display, Formatter};

active_string_enum! {
    /// Persisted top-level state of the 5-state circuit breaker FSM.
    ///
    /// ```text
    ///                     ┌── Halted (L3 Daily / L4 System) ──┐
    ///                     │   NO tick transition               │
    ///                     │   manual ack only                  │
    ///                     └──────────▲─────────────────────────┘
    ///                                │ halt()
    /// Closed ──trip_session──▶ Open ──cooldown──▶ HalfOpen ──probes──▶ Recovered ──obs──▶ Closed
    ///   ▲                         ▲                    │                                    │
    ///   │                         └── probe fail ──────┘                                    │
    ///   └───────────────────────── acknowledge_and_resume (from Halted) ────────────────────┘
    /// ```
    pub enum BreakerStateName {
        /// Normal operation — execution permitted.
        Closed => "closed",
        /// Tripped (L2 Session) — execution blocked, cooldown timer running.
        Open => "open",
        /// Cooldown expired — allowing probe trades to test recovery.
        HalfOpen => "half_open",
        /// Probes succeeded — observation period before returning to Closed.
        Recovered => "recovered",
        /// Hard halt (L3 Daily / L4 System) — requires operator `acknowledge_and_resume`.
        Halted => "halted",
    }
}

active_string_enum! {
    /// Severity level of a circuit-breaker trip (1–4).
    @derive(PartialOrd, Ord)
    pub enum CircuitBreakerLevel {
        Trade = 1 => "trade",
        Session = 2 => "session",
        Daily = 3 => "daily",
        System = 4 => "system",
    }
}

impl CircuitBreakerLevel {
    #[must_use]
    #[inline]
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Trade => 1,
            Self::Session => 2,
            Self::Daily => 3,
            Self::System => 4,
        }
    }
}

active_string_enum! {
    /// Severity scope for a blacklist entry.
    ///
    /// Ordered so that higher scopes include all lower ones via `>=` comparison.
    @derive(PartialOrd, Ord, bitcode::Encode, bitcode::Decode)
    pub enum BlacklistScope {
        DataPath = 0 => "data_path",
        TradingPath = 1 => "trading_path",
        Full = 2 => "full",
    }
}

active_string_enum! {
    /// Why a market or token was added to the blacklist.
    @derive(bitcode::Encode, bitcode::Decode)
    pub enum BlacklistReason {
        ConsecutiveFokFailures => "consecutive_fok_failures",
        TradeFailedAfterMatched => "trade_failed_after_matched",
        DepthDrop => "depth_drop",
        TickChange => "tick_change",
        Manual => "manual",
        DataNotFound => "data_not_found",
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeAccountingPhase {
    /// Trade was filled — record cost, counts, potential loss. No realized profit.
    Fill,
    /// Market settled — record realized profit, resolve potential loss, check caps.
    Settlement,
}

impl Display for TradeAccountingPhase {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fill => f.write_str("fill"),
            Self::Settlement => f.write_str("settlement"),
        }
    }
}

active_string_enum! {
    /// Overall outcome of a balance/exposure reconciliation run.
    pub enum ReconciliationStatus {
        Ok => "ok",
        Warning => "warning",
        Critical => "critical",
    }
}

active_string_enum! {
    /// Type of risk audit event persisted for post-mortem analysis.
    pub enum RiskAuditEventType {
        TradeAllowed => "trade_allowed",
        TradeDenied => "trade_denied",
        BreakerTripped => "breaker_tripped",
        BreakerRecovered => "breaker_recovered",
        BreakerReset => "breaker_reset",
        BlacklistAdded => "blacklist_added",
        BlacklistRemoved => "blacklist_removed",
        AccountingRollover => "accounting_rollover",
        ReconciliationCompleted => "reconciliation_completed",
        EngineHalted => "engine_halted",
        EngineResumed => "engine_resumed",
        PostTradeUpdate => "post_trade_update",
    }
}

active_string_enum! {
    /// Lifecycle state of an exposure reservation.
    pub enum ReservationStatus {
        Pending => "pending",
        Confirmed => "confirmed",
        Released => "released",
    }
}

active_string_enum! {
    /// Granularity of a time-windowed risk accumulator.
    pub enum WindowType {
        Hourly => "hourly",
        Daily => "daily",
        Weekly => "weekly",
    }
}
