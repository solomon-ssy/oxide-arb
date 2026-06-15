//! Risk API contract: outbound views for the risk dashboard.
//!
//! [`RiskEngineStateView`] is the wire projection of the live risk-engine
//! snapshot; it strips the engine's internal recovery mechanics (rolling-window
//! anchor timestamps and the cooldown back-off multiplier) that drive state
//! restoration but carry no operator-facing meaning. [`RiskAuditEventView`]
//! projects a persisted risk-decision audit row, dropping the raw database
//! primary key.

use crate::{
    domain::{BlacklistInfo, RiskAuditEventInfo, RiskEngineState},
    enums::risk::{
        BlacklistReason, BlacklistScope, BreakerStateName, CircuitBreakerLevel, RiskAuditEventType,
    },
    types::{MarketId, OpportunityId, TokenId, TradeId, Usd},
};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Outbound projection of the live risk-engine snapshot for the dashboard.
///
/// Omits the internal rolling-window anchors (`*_window_start`) and the
/// `cooldown_multiplier` back-off counter — both are state-restoration
/// machinery the dashboard never renders.
#[derive(Debug, Clone, Serialize)]
pub struct RiskEngineStateView {
    pub breaker_state: BreakerStateName,
    pub breaker_level: Option<CircuitBreakerLevel>,
    pub is_halted: bool,
    pub halt_reason: Option<String>,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub total_exposure: Usd,
    pub hourly_loss_usd: Usd,
    pub hourly_fee_usd: Usd,
    pub hourly_trade_count: i32,
    pub hourly_success_count: i32,
    pub hourly_miss_count: i32,
    pub daily_pnl: Usd,
    pub daily_loss_usd: Usd,
    pub daily_fee_usd: Usd,
    pub daily_budget_spent: Usd,
    pub daily_trade_count: i32,
    pub daily_success_count: i32,
    pub daily_miss_count: i32,
    pub daily_window_start: NaiveDate,
    pub weekly_loss_usd: Usd,
    pub weekly_trade_count: i32,
    pub consecutive_misses: i32,
    pub hwm_equity: Usd,
    pub last_emergency_at: Option<DateTime<Utc>>,
    pub last_emergency_reason: Option<String>,
    pub snapshot_at: DateTime<Utc>,
}

impl From<RiskEngineState> for RiskEngineStateView {
    fn from(s: RiskEngineState) -> Self {
        Self {
            breaker_state: s.breaker_state,
            breaker_level: s.breaker_level,
            is_halted: s.is_halted,
            halt_reason: s.halt_reason,
            cooldown_until: s.cooldown_until,
            total_exposure: s.total_exposure,
            hourly_loss_usd: s.hourly_loss_usd,
            hourly_fee_usd: s.hourly_fee_usd,
            hourly_trade_count: s.hourly_trade_count,
            hourly_success_count: s.hourly_success_count,
            hourly_miss_count: s.hourly_miss_count,
            daily_pnl: s.daily_pnl,
            daily_loss_usd: s.daily_loss_usd,
            daily_fee_usd: s.daily_fee_usd,
            daily_budget_spent: s.daily_budget_spent,
            daily_trade_count: s.daily_trade_count,
            daily_success_count: s.daily_success_count,
            daily_miss_count: s.daily_miss_count,
            daily_window_start: s.daily_window_start,
            weekly_loss_usd: s.weekly_loss_usd,
            weekly_trade_count: s.weekly_trade_count,
            consecutive_misses: s.consecutive_misses,
            hwm_equity: s.hwm_equity,
            last_emergency_at: s.last_emergency_at,
            last_emergency_reason: s.last_emergency_reason,
            snapshot_at: s.snapshot_at,
        }
    }
}

/// Add a market to the runtime blacklist.
#[derive(Debug, Deserialize, Validate)]
pub struct BlacklistCreateRequest {
    /// Polymarket `condition_id` to exclude from trading.
    pub market_id: MarketId,
    /// Business classification of why the market is excluded.
    pub blacklist_reason: BlacklistReason,
    /// Operator justification, recorded on the operation log and risk audit.
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Remove a market from the runtime blacklist (governed mutation).
#[derive(Debug, Deserialize, Validate)]
pub struct BlacklistRemoveRequest {
    /// Operator justification, recorded on the operation log and risk audit.
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Operator-facing blacklist entry view.
///
/// Uses `blacklist_reason` on the wire so the business classification cannot be
/// confused with governed operator `reason` fields on mutating requests.
#[derive(Debug, Clone, Serialize)]
pub struct BlacklistEntryView {
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub scope: BlacklistScope,
    pub blacklist_reason: BlacklistReason,
    pub expires_at: Option<DateTime<Utc>>,
    pub miss_count: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<BlacklistInfo> for BlacklistEntryView {
    fn from(info: BlacklistInfo) -> Self {
        Self {
            market_id: info.market_id,
            token_id: info.token_id,
            scope: info.scope,
            blacklist_reason: info.reason,
            expires_at: info.expires_at,
            miss_count: info.miss_count,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

/// Outbound projection of a persisted risk-decision audit event.
///
/// Drops the raw database primary key (`id`); the dashboard keys off the
/// event's domain identifiers (`opportunity_id` / `trade_id`) and timestamp.
/// `market_id` / `rejection_reason` are the queryable columns lifted at
/// persist time — rows written before those columns existed surface `null`.
#[derive(Debug, Clone, Serialize)]
pub struct RiskAuditEventView {
    pub event_type: RiskAuditEventType,
    pub market_id: Option<MarketId>,
    pub opportunity_id: Option<OpportunityId>,
    pub trade_id: Option<TradeId>,
    pub rejection_reason: Option<String>,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

impl From<RiskAuditEventInfo> for RiskAuditEventView {
    fn from(info: RiskAuditEventInfo) -> Self {
        Self {
            event_type: info.event_type,
            market_id: info.market_id,
            opportunity_id: info.opportunity_id,
            trade_id: info.trade_id,
            rejection_reason: info.rejection_reason,
            payload: info.payload,
            created_at: info.created_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RiskAuditEventInfo, RiskAuditEventView};
    use crate::enums::risk::RiskAuditEventType;
    use chrono::Utc;

    #[test]
    fn audit_event_view_strips_db_primary_key() {
        let info = RiskAuditEventInfo {
            id: 42,
            event_type: RiskAuditEventType::TradeAllowed,
            market_id: None,
            opportunity_id: None,
            trade_id: None,
            rejection_reason: None,
            payload: serde_json::json!({ "decision": "allowed" }),
            created_at: Utc::now(),
        };
        let json = serde_json::to_value(RiskAuditEventView::from(info)).expect("serialize");
        assert!(
            json.get("id").is_none(),
            "DB primary key must never cross the wire"
        );
        assert!(json.get("event_type").is_some());
        assert!(json.get("payload").is_some());
        assert!(
            json.get("market_id").is_some() && json.get("rejection_reason").is_some(),
            "lifted decision columns must be part of the wire shape (null for legacy rows)"
        );
    }
}
