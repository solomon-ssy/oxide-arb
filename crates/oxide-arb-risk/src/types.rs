//! Crate-local types for the risk engine.
//!
//! These types are owned by `oxide-arb-risk` and not shared via `oxide-arb-models`.
//! Consumers (e.g. `oxide-arb-core`) depend on this crate to access them.

use crate::audit::RiskDecisionTrace;
use chrono::{DateTime, Utc};
use oxide_arb_models::{
    domain::risk::RiskEngineState,
    enums::risk::{BreakerStateName, CircuitBreakerLevel, ReconciliationStatus},
    types::{MarketId, ReservationId, TokenId, Usd},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

// ── State Version ───────────────────────────────────────────────────────────

/// Monotonically increasing version number for risk engine state.
///
/// Incremented on every mutation (post-trade update, breaker transition,
/// blacklist change). All checks within a single `pre_trade_check()` share
/// the same version, guaranteeing snapshot consistency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StateVersion(u64);

impl StateVersion {
    pub const ZERO: Self = Self(0);

    #[must_use]
    #[inline]
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    #[must_use]
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for StateVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// Atomic version counter used inside `RiskEngine`.
pub struct AtomicStateVersion(AtomicU64);

impl AtomicStateVersion {
    pub const fn new(initial: u64) -> Self {
        Self(AtomicU64::new(initial))
    }

    #[must_use]
    #[inline]
    pub fn load(&self) -> StateVersion {
        StateVersion(self.0.load(Ordering::Acquire))
    }

    pub fn increment(&self) -> StateVersion {
        StateVersion(self.0.fetch_add(1, Ordering::AcqRel) + 1)
    }

    pub fn store(&self, version: StateVersion) {
        self.0.store(version.get(), Ordering::Release);
    }
}

impl fmt::Debug for AtomicStateVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AtomicStateVersion({})", self.0.load(Ordering::Relaxed))
    }
}

// ── Report Mode ─────────────────────────────────────────────────────────────

/// Controls how `RiskPipeline` handles check failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportMode {
    /// Return immediately on the first failed hard gate (production fast path).
    ShortCircuit,
    /// Evaluate all checks regardless of failures (diagnostics / full report).
    FullReport,
}

// ── Breaker Runtime State ───────────────────────────────────────────────────

/// Runtime circuit breaker state with embedded transition metadata.
///
/// Richer than the persisted `BreakerStateName` — carries timing info
/// needed for FSM transitions without re-querying the database.
///
/// Five states:
/// - `Closed`: normal trading
/// - `Open`: L2 Session trip with cooldown (auto-recovery via `HalfOpen`)
/// - `HalfOpen`: probe trades after cooldown expires
/// - `Recovered`: observation period before returning to Closed
/// - `Halted`: L3 Daily / L4 System — **no** automatic recovery, requires
///   operator `acknowledge_and_resume()`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BreakerState {
    Closed,
    Open {
        level: CircuitBreakerLevel,
        reason: String,
        tripped_at: DateTime<Utc>,
        cooldown_until: DateTime<Utc>,
    },
    HalfOpen {
        level: CircuitBreakerLevel,
        entered_at: DateTime<Utc>,
        successful_probes: u32,
        required_probes: u32,
    },
    Recovered {
        entered_at: DateTime<Utc>,
        observation_until: DateTime<Utc>,
    },
    Halted {
        level: CircuitBreakerLevel,
        reason: String,
        halted_at: DateTime<Utc>,
    },
}

impl BreakerState {
    #[must_use]
    #[inline]
    pub const fn to_name(&self) -> BreakerStateName {
        match self {
            Self::Closed => BreakerStateName::Closed,
            Self::Open { .. } => BreakerStateName::Open,
            Self::HalfOpen { .. } => BreakerStateName::HalfOpen,
            Self::Recovered { .. } => BreakerStateName::Recovered,
            Self::Halted { .. } => BreakerStateName::Halted,
        }
    }

    #[must_use]
    #[inline]
    pub const fn allows_trading(&self) -> bool {
        matches!(self, Self::Closed | Self::HalfOpen { .. })
    }

    #[must_use]
    #[inline]
    pub const fn is_probe_mode(&self) -> bool {
        matches!(self, Self::HalfOpen { .. })
    }

    #[must_use]
    #[inline]
    pub const fn is_halted(&self) -> bool {
        matches!(self, Self::Halted { .. })
    }
}

// ── Check Types ─────────────────────────────────────────────────────────────

/// Stable identifier for each risk check in the pipeline.
///
/// The ordering here defines the default pipeline execution order.
/// Tests lock this order via golden tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RiskCheckId {
    ManualHalt,
    CircuitBreaker,
    BlacklistTradingPath,
    TokenBlacklist,
    /// Control-factor `MarketAnomalyFactor` block on this market/event.
    MarketAnomalyBlock,
    /// Control-factor `ReconciliationHealthFactor` forced maintenance mode.
    ReconciliationMaintenance,
    /// Control-factor requires operator acknowledgement before new entries.
    ControlFactorManualAckRequired,
    /// Control-factor publication TTL elapsed under fail-closed policy.
    ControlFactorSnapshotExpired,
    /// Live settlement redeem route must resolve for the target market class.
    RedeemRouteResolvable,
    MetricsFreshness,
    MinDepth,
    MaxDepthUsage,
    Staleness,
    DailyBudget,
    DailyLossCap,
    WeeklyLossCap,
    HourlyLossCap,
    FeeSpend,
    MaxSingleBet,
    MarketExposure,
    TotalExposure,
    ExposurePct,
    PotentialLossCap,
    MaxPositions,
    WsConnectivity,
    MinBalance,
    DirectionalConcentration,
    DailyDirectionalBudget,
    DuplicateMarket,
    DrawdownGuard,
    ApiErrorRate,
}

impl fmt::Display for RiskCheckId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// Classification of risk checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskCheckKind {
    /// Hard gate: failure blocks the trade.
    Gate,
    /// Sizing constraint: reduces position size but does not block.
    SizingConstraint,
}

/// Result of a single risk check evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskCheckResult {
    pub check_id: RiskCheckId,
    pub passed: bool,
    pub detail: Option<String>,
    pub threshold: Option<String>,
    pub actual: Option<String>,
    pub elapsed_us: u64,
}

impl RiskCheckResult {
    /// Zero-allocation pass result; pipeline sets `elapsed_us` after evaluation.
    #[must_use]
    #[inline]
    pub const fn passed(check_id: RiskCheckId) -> Self {
        Self {
            check_id,
            passed: true,
            detail: None,
            threshold: None,
            actual: None,
            elapsed_us: 0,
        }
    }

    #[must_use]
    #[inline]
    pub const fn failed(
        check_id: RiskCheckId,
        detail: String,
        threshold: String,
        actual: String,
    ) -> Self {
        Self {
            check_id,
            passed: false,
            detail: Some(detail),
            threshold: Some(threshold),
            actual: Some(actual),
            elapsed_us: 0,
        }
    }
}

// ── Decision ────────────────────────────────────────────────────────────────

/// Result of a pre-trade risk evaluation.
///
/// This is the single decision type — there is no `PreTradeDecision` alias.
#[derive(Debug, Clone, Serialize)]
pub struct RiskDecision {
    pub allowed: bool,
    pub denial_reason: Option<String>,
    pub recommended_size: Option<SizeResult>,
    pub drawdown_factor: Decimal,
    pub evaluated_at: DateTime<Utc>,
    pub state_version: StateVersion,
    pub trace: RiskDecisionTrace,
}

impl RiskDecision {
    #[must_use]
    #[inline]
    pub fn checks(&self) -> &[RiskCheckResult] {
        &self.trace.check_results
    }
}

// `RiskDecisionTrace` is defined in `crate::audit` — the single source of truth.

// ── Sizing ──────────────────────────────────────────────────────────────────

/// Output of Kelly calculation.
#[derive(Debug, Clone, Serialize)]
pub struct KellyResult {
    pub bet_usd: Usd,
    pub kelly_raw: Decimal,
    pub kelly_fractional: Decimal,
    pub edge_bps: Decimal,
    pub effective_win_prob: Decimal,
    pub net_odds: Decimal,
    pub binding_reason: &'static str,
}

/// A single sizing constraint with its computed upper bound.
#[derive(Debug, Clone, Serialize)]
pub struct SizeConstraint {
    pub name: &'static str,
    pub max_usd: Usd,
}

/// Itemized breakdown of all constraint ceilings.
#[derive(Debug, Clone, Serialize)]
pub struct SizeBreakdown {
    pub constraints: Vec<SizeConstraint>,
}

/// Complete sizing output.
#[derive(Debug, Clone, Serialize)]
pub struct SizeResult {
    pub bet_usd: Usd,
    pub kelly_result: KellyResult,
    pub binding_constraint: &'static str,
    pub breakdown: SizeBreakdown,
}

impl SizeResult {
    #[must_use]
    pub const fn zero() -> Self {
        Self {
            bet_usd: Usd::ZERO,
            kelly_result: KellyResult {
                bet_usd: Usd::ZERO,
                kelly_raw: Decimal::ZERO,
                kelly_fractional: Decimal::ZERO,
                edge_bps: Decimal::ZERO,
                effective_win_prob: Decimal::ZERO,
                net_odds: Decimal::ZERO,
                binding_reason: "zero",
            },
            binding_constraint: "zero",
            breakdown: SizeBreakdown {
                constraints: Vec::new(),
            },
        }
    }
}

/// Action recommended by the drawdown guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DrawdownAction {
    Normal,
    Reduce,
    Halt,
}

// ── Accounting ──────────────────────────────────────────────────────────────

/// Accumulator for a single accounting period (day/week).
///
/// All fields are monotonically increasing within a period. On rollover
/// they reset to zero.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeriodStats {
    pub loss: Usd,
    pub fees: Usd,
    pub pnl: Usd,
    pub trade_count: u32,
    pub success_count: u32,
    pub miss_count: u32,
    pub max_single_loss: Usd,
    pub max_single_profit: Usd,
}

// ── Reconciliation ──────────────────────────────────────────────────────────

/// A single mismatch detected during reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReconciliationMismatch {
    BalanceDrift {
        internal: Usd,
        external: Usd,
        drift: Usd,
    },
    PositionDrift {
        market_id: MarketId,
        internal: Usd,
        external: Usd,
        drift: Usd,
    },
    OrphanedReservation {
        reservation_id: ReservationId,
        amount: Usd,
    },
}

impl ReconciliationMismatch {
    #[must_use]
    #[inline]
    pub fn drift_abs(&self) -> Usd {
        match self {
            Self::BalanceDrift { drift, .. } | Self::PositionDrift { drift, .. } => drift.abs(),
            Self::OrphanedReservation { amount, .. } => *amount,
        }
    }
}

/// Full report from a reconciliation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationReport {
    pub status: ReconciliationStatus,
    pub mismatches: Vec<ReconciliationMismatch>,
    pub internal_balance: Usd,
    pub external_balance: Usd,
    pub internal_exposure: Usd,
    pub external_exposure: Usd,
    pub reserved: Usd,
    pub tolerance: Usd,
    pub checked_at: DateTime<Utc>,
    pub duration_ms: u64,
}

// ── Execution Risk Events ───────────────────────────────────────────────────

/// Events from the execution layer that the risk engine consumes for
/// blacklist management and system health tracking.
#[derive(Debug, Clone)]
pub enum ExecutionRiskEvent {
    FokFailure {
        market_id: MarketId,
        token_id: TokenId,
        consecutive: u32,
    },
    TradeFailed {
        market_id: MarketId,
        token_id: TokenId,
    },
    DepthDrop {
        market_id: MarketId,
        pct_drop: Decimal,
    },
    HeartbeatFailure,
    HeartbeatSuccess,
}

// ── Blacklist Key ───────────────────────────────────────────────────────────

/// Lookup key for the in-memory blacklist projection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BlacklistKey {
    Market(MarketId),
    Token(TokenId),
}

// ── Post-Trade Report ───────────────────────────────────────────────────────

/// Summary of all state changes produced by `on_trade_result`.
#[derive(Debug, Clone, Serialize)]
pub struct PostTradeReport {
    pub snapshot: RiskEngineState,
    pub daily_rolled: bool,
    pub weekly_rolled: bool,
    pub hourly_rolled: bool,
    pub breaker_tripped: Option<CircuitBreakerLevel>,
    pub auto_blacklisted: Option<MarketId>,
}

// ── Pipeline Report ─────────────────────────────────────────────────────────

/// Output of `RiskPipeline::evaluate`.
#[derive(Debug, Clone)]
pub struct PipelineReport {
    pub results: Vec<RiskCheckResult>,
    pub has_failed_hard_gate: bool,
    pub first_failure: Option<RiskCheckId>,
    pub total_elapsed_us: u64,
}

impl PipelineReport {
    /// Append results from a subsequent pipeline segment (e.g. phase-2 gates).
    pub fn merge(&mut self, other: Self) {
        self.results.extend(other.results);
        self.has_failed_hard_gate |= other.has_failed_hard_gate;
        if self.first_failure.is_none() {
            self.first_failure = other.first_failure;
        }
        self.total_elapsed_us = self.total_elapsed_us.saturating_add(other.total_elapsed_us);
    }
}
