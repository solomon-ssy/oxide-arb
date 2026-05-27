//! Risk audit events and decision traces.
//!
//! Every critical state mutation and trade decision produces an immutable
//! audit event that must be durably persisted before the mutation is
//! acknowledged.

use crate::types::{PeriodStats, RiskCheckResult, SizeBreakdown, StateVersion};
use chrono::{DateTime, NaiveDate, Utc};
use oxide_arb_models::{
    domain::{blacklist::BlacklistInfo, risk::NewRiskAuditEvent},
    enums::{
        ReconciliationStatus,
        common::TradeOutcome,
        risk::{
            BreakerStateName, CircuitBreakerLevel, RiskAuditEventType, TradeAccountingPhase,
            WindowType,
        },
    },
    types::{MarketId, OpportunityId, TradeId, Usd},
};
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
    /// Compact audit for allowed trades on the short-circuit hot path.
    TradeAllowedSummary {
        opportunity_id: OpportunityId,
        state_version: StateVersion,
        check_count: usize,
        total_elapsed_us: u64,
        evaluated_at: DateTime<Utc>,
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
        entry: BlacklistInfo,
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
        phase: TradeAccountingPhase,
        daily_loss_after: Usd,
        weekly_loss_after: Usd,
        hourly_loss_after: Usd,
        breaker_tripped: Option<CircuitBreakerLevel>,
        auto_blacklisted: Option<MarketId>,
        daily_rolled: bool,
        weekly_rolled: bool,
        hourly_rolled: bool,
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

impl From<RiskAuditEvent> for NewRiskAuditEvent {
    fn from(event: RiskAuditEvent) -> Self {
        let (event_type, opportunity_id, trade_id) = audit_event_metadata(&event);
        let payload = serde_json::to_value(&event).unwrap_or(serde_json::Value::Null);

        Self {
            event_type,
            opportunity_id,
            trade_id,
            payload,
            created_at: Utc::now(),
        }
    }
}

fn audit_event_metadata(
    event: &RiskAuditEvent,
) -> (RiskAuditEventType, Option<OpportunityId>, Option<TradeId>) {
    match event {
        RiskAuditEvent::TradeAllowed { opportunity_id, .. }
        | RiskAuditEvent::TradeAllowedSummary { opportunity_id, .. } => (
            RiskAuditEventType::TradeAllowed,
            Some(opportunity_id.clone()),
            None,
        ),
        RiskAuditEvent::TradeDenied { opportunity_id, .. } => (
            RiskAuditEventType::TradeDenied,
            Some(opportunity_id.clone()),
            None,
        ),
        RiskAuditEvent::BreakerTripped { .. } => (RiskAuditEventType::BreakerTripped, None, None),
        RiskAuditEvent::BreakerRecovered { .. } => {
            (RiskAuditEventType::BreakerRecovered, None, None)
        }
        RiskAuditEvent::BreakerReset { .. } => (RiskAuditEventType::BreakerReset, None, None),
        RiskAuditEvent::BlacklistAdded { .. } => (RiskAuditEventType::BlacklistAdded, None, None),
        RiskAuditEvent::BlacklistRemoved { .. } => {
            (RiskAuditEventType::BlacklistRemoved, None, None)
        }
        RiskAuditEvent::AccountingRollover { .. } => {
            (RiskAuditEventType::AccountingRollover, None, None)
        }
        RiskAuditEvent::ReconciliationCompleted { .. } => {
            (RiskAuditEventType::ReconciliationCompleted, None, None)
        }
        RiskAuditEvent::EngineHalted { .. } => (RiskAuditEventType::EngineHalted, None, None),
        RiskAuditEvent::EngineResumed => (RiskAuditEventType::EngineResumed, None, None),
        RiskAuditEvent::PostTradeUpdate { trade_id, .. } => (
            RiskAuditEventType::PostTradeUpdate,
            None,
            Some(trade_id.clone()),
        ),
    }
}
