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
        common::TradeBusinessOutcome,
        risk::{
            BreakerStateName, CircuitBreakerLevel, ReconciliationStatus, RiskAuditEventType,
            TradeAccountingPhase, WindowType,
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
        market_id: MarketId,
    },
    /// Compact audit for allowed trades on the short-circuit hot path.
    TradeAllowedSummary {
        opportunity_id: OpportunityId,
        market_id: MarketId,
        state_version: StateVersion,
        check_count: usize,
        total_elapsed_us: u64,
        evaluated_at: DateTime<Utc>,
    },
    TradeDenied {
        trace: RiskDecisionTrace,
        opportunity_id: OpportunityId,
        market_id: MarketId,
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
        outcome: TradeBusinessOutcome,
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
        let columns = audit_event_columns(&event);
        let payload = serde_json::to_value(&event).unwrap_or(serde_json::Value::Null);

        Self {
            event_type: columns.event_type,
            market_id: columns.market_id,
            opportunity_id: columns.opportunity_id,
            trade_id: columns.trade_id,
            rejection_reason: columns.rejection_reason,
            payload,
        }
    }
}

/// Queryable columns lifted out of a [`RiskAuditEvent`] at persist time.
///
/// Everything else stays inside the JSON `payload`; these fields back the
/// decisions dashboard's filterable / renderable grid columns.
struct AuditEventColumns {
    event_type: RiskAuditEventType,
    market_id: Option<MarketId>,
    opportunity_id: Option<OpportunityId>,
    trade_id: Option<TradeId>,
    rejection_reason: Option<String>,
}

impl AuditEventColumns {
    const fn bare(event_type: RiskAuditEventType) -> Self {
        Self {
            event_type,
            market_id: None,
            opportunity_id: None,
            trade_id: None,
            rejection_reason: None,
        }
    }
}

/// Human-readable denial summary: every failed check joined as
/// `check_id: detail`, in pipeline order.
fn denial_reason(trace: &RiskDecisionTrace) -> Option<String> {
    let parts: Vec<String> = trace
        .check_results
        .iter()
        .filter(|check| !check.passed)
        .map(|check| {
            check.detail.as_ref().map_or_else(
                || check.check_id.to_string(),
                |detail| format!("{}: {detail}", check.check_id),
            )
        })
        .collect();
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("; "))
    }
}

fn audit_event_columns(event: &RiskAuditEvent) -> AuditEventColumns {
    match event {
        RiskAuditEvent::TradeAllowed {
            opportunity_id,
            market_id,
            ..
        }
        | RiskAuditEvent::TradeAllowedSummary {
            opportunity_id,
            market_id,
            ..
        } => AuditEventColumns {
            event_type: RiskAuditEventType::TradeAllowed,
            market_id: Some(market_id.clone()),
            opportunity_id: Some(opportunity_id.clone()),
            trade_id: None,
            rejection_reason: None,
        },
        RiskAuditEvent::TradeDenied {
            trace,
            opportunity_id,
            market_id,
        } => AuditEventColumns {
            event_type: RiskAuditEventType::TradeDenied,
            market_id: Some(market_id.clone()),
            opportunity_id: Some(opportunity_id.clone()),
            trade_id: None,
            rejection_reason: denial_reason(trace),
        },
        RiskAuditEvent::BreakerTripped { reason, .. } => AuditEventColumns {
            rejection_reason: Some(reason.clone()),
            ..AuditEventColumns::bare(RiskAuditEventType::BreakerTripped)
        },
        RiskAuditEvent::BreakerRecovered { .. } => {
            AuditEventColumns::bare(RiskAuditEventType::BreakerRecovered)
        }
        RiskAuditEvent::BreakerReset { .. } => {
            AuditEventColumns::bare(RiskAuditEventType::BreakerReset)
        }
        RiskAuditEvent::BlacklistAdded { entry } => AuditEventColumns {
            market_id: Some(entry.market_id.clone()),
            ..AuditEventColumns::bare(RiskAuditEventType::BlacklistAdded)
        },
        RiskAuditEvent::BlacklistRemoved { market_id, .. } => AuditEventColumns {
            market_id: Some(market_id.clone()),
            ..AuditEventColumns::bare(RiskAuditEventType::BlacklistRemoved)
        },
        RiskAuditEvent::AccountingRollover { .. } => {
            AuditEventColumns::bare(RiskAuditEventType::AccountingRollover)
        }
        RiskAuditEvent::ReconciliationCompleted { .. } => {
            AuditEventColumns::bare(RiskAuditEventType::ReconciliationCompleted)
        }
        RiskAuditEvent::EngineHalted { reason } => AuditEventColumns {
            rejection_reason: Some(reason.clone()),
            ..AuditEventColumns::bare(RiskAuditEventType::EngineHalted)
        },
        RiskAuditEvent::EngineResumed => AuditEventColumns::bare(RiskAuditEventType::EngineResumed),
        RiskAuditEvent::PostTradeUpdate { trade_id, .. } => AuditEventColumns {
            trade_id: Some(trade_id.clone()),
            ..AuditEventColumns::bare(RiskAuditEventType::PostTradeUpdate)
        },
    }
}
