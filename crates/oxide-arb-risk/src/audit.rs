//! Risk audit events and decision traces.
//!
//! Every critical state mutation and trade decision produces an immutable
//! audit event that must be durably persisted before the mutation is
//! acknowledged.

use crate::types::{
    PeriodStats, ReconciliationStatus, RiskCheckResult, SizeBreakdown, StateVersion,
};
use chrono::{DateTime, NaiveDate, Utc};
use oxide_arb_models::domain::blacklist::BlacklistEntry;
use oxide_arb_models::enums::common::TradeOutcome;
use oxide_arb_models::enums::risk::{BreakerStateName, CircuitBreakerLevel, WindowType};
use oxide_arb_models::types::{MarketId, OpportunityId, TradeId, Usd};
use serde::Serialize;

/// Immutable audit event produced by the risk engine.
///
/// Persisted via `RiskPersistence::append_audit_event`. Critical events
/// (breaker trips, trade decisions, blacklist changes) must be durably
/// committed before the engine acknowledges the state change.
#[derive(Debug, Clone, Serialize)]
pub enum RiskAuditEvent {
    TradeAllowed {
        trace: RiskDecisionTrace,
        opportunity_id: OpportunityId,
    },
    TradeDenied {
        trace: RiskDecisionTrace,
        opportunity_id: OpportunityId,
    },
    BreakerTripped {
        level: CircuitBreakerLevel,
        reason: String,
        previous_state: BreakerStateName,
    },
    BreakerRecovered {
        from: BreakerStateName,
    },
    BreakerReset {
        operator_reason: String,
    },
    BlacklistAdded {
        entry: BlacklistEntry,
    },
    BlacklistRemoved {
        market_id: MarketId,
        operator_reason: String,
    },
    AccountingRollover {
        window_type: WindowType,
        old_start: NaiveDate,
        new_start: NaiveDate,
        final_stats: PeriodStats,
    },
    ReconciliationCompleted {
        status: ReconciliationStatus,
        mismatch_count: usize,
    },
    EngineHalted {
        reason: String,
    },
    EngineResumed,
    PostTradeUpdate {
        trade_id: TradeId,
        outcome: TradeOutcome,
        daily_loss_after: Usd,
        weekly_loss_after: Usd,
    },
}

/// Full audit trace for a single pre-trade decision.
///
/// Stored inside `RiskAuditEvent::TradeAllowed` and `TradeDenied`.
/// Contains enough information to reconstruct exactly why a trade was
/// allowed or denied, including all check results, sizing breakdown,
/// the state version, and wall-clock timing.
#[derive(Debug, Clone, Serialize)]
pub struct RiskDecisionTrace {
    pub check_results: Vec<RiskCheckResult>,
    pub sizing_breakdown: Option<SizeBreakdown>,
    pub state_version: StateVersion,
    pub total_elapsed_us: u64,
    pub evaluated_at: DateTime<Utc>,
}
