//! In-memory [`RiskPersistence`] for integration tests and runtime harnesses.

use std::sync::Mutex;

use oxide_arb_error::OxideResult;
use oxide_arb_models::domain::blacklist::{BlacklistInfo, UpsertBlacklistEntry};
use oxide_arb_models::domain::risk::{
    NewEmergencySnapshot, NewReconciliationReport, NewRiskAuditEvent, RiskStateInfo,
    UpsertRiskEngineState,
};
use oxide_arb_models::enums::risk::BreakerStateName;
use oxide_arb_models::types::{MarketId, Usd};
use oxide_arb_risk::traits::RiskPersistence;

/// Captures all persistence writes in memory for assertions (no I/O, no PG).
pub struct TestRiskPersistence {
    state: Mutex<Option<RiskStateInfo>>,
    blacklist: Mutex<Vec<BlacklistInfo>>,
    audits: Mutex<Vec<NewRiskAuditEvent>>,
    emergencies: Mutex<Vec<NewEmergencySnapshot>>,
    reconciliations: Mutex<Vec<NewReconciliationReport>>,
}

impl TestRiskPersistence {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: Mutex::new(None),
            blacklist: Mutex::new(Vec::new()),
            audits: Mutex::new(Vec::new()),
            emergencies: Mutex::new(Vec::new()),
            reconciliations: Mutex::new(Vec::new()),
        }
    }

    /// Drain captured audit events (most recent batch since last take).
    pub fn take_audits(&self) -> Vec<NewRiskAuditEvent> {
        std::mem::take(&mut *self.audits.lock().expect("audit lock"))
    }

    fn default_state_info() -> RiskStateInfo {
        let now = chrono::Utc::now();
        RiskStateInfo {
            id: 1,
            breaker_state: BreakerStateName::Closed,
            breaker_level: None,
            is_halted: false,
            halt_reason: None,
            consecutive_misses: 0,
            cooldown_until: None,
            cooldown_multiplier: 0,
            total_exposure: Usd::ZERO,
            hourly_loss_usd: Usd::ZERO,
            hourly_fee_usd: Usd::ZERO,
            hourly_trade_count: 0,
            hourly_success_count: 0,
            hourly_miss_count: 0,
            hourly_window_start: now,
            daily_loss_usd: Usd::ZERO,
            daily_fee_usd: Usd::ZERO,
            daily_pnl: Usd::ZERO,
            daily_budget_spent: Usd::ZERO,
            daily_trade_count: 0,
            daily_success_count: 0,
            daily_miss_count: 0,
            daily_window_start: now.date_naive(),
            weekly_loss_usd: Usd::ZERO,
            weekly_trade_count: 0,
            weekly_window_start: now.date_naive(),
            hwm_equity: Usd::ZERO,
            last_emergency_at: None,
            last_emergency_reason: None,
            updated_at: now,
        }
    }
}

impl Default for TestRiskPersistence {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl RiskPersistence for TestRiskPersistence {
    async fn upsert_state(&self, state: UpsertRiskEngineState) -> OxideResult<()> {
        let mut slot = self.state.lock().expect("state lock");
        let mut info = slot.clone().unwrap_or_else(Self::default_state_info);
        info.breaker_state = state.breaker_state;
        info.breaker_level = state.breaker_level;
        info.is_halted = state.is_halted;
        info.halt_reason = state.halt_reason;
        info.consecutive_misses = state.consecutive_misses;
        info.cooldown_until = state.cooldown_until;
        info.cooldown_multiplier = state.cooldown_multiplier;
        info.total_exposure = state.total_exposure;
        info.hourly_loss_usd = state.hourly_loss_usd;
        info.hourly_fee_usd = state.hourly_fee_usd;
        info.hourly_trade_count = state.hourly_trade_count;
        info.hourly_success_count = state.hourly_success_count;
        info.hourly_miss_count = state.hourly_miss_count;
        info.hourly_window_start = state.hourly_window_start;
        info.daily_loss_usd = state.daily_loss_usd;
        info.daily_fee_usd = state.daily_fee_usd;
        info.daily_pnl = state.daily_pnl;
        info.daily_budget_spent = state.daily_budget_spent;
        info.daily_trade_count = state.daily_trade_count;
        info.daily_success_count = state.daily_success_count;
        info.daily_miss_count = state.daily_miss_count;
        info.daily_window_start = state.daily_window_start;
        info.weekly_loss_usd = state.weekly_loss_usd;
        info.weekly_trade_count = state.weekly_trade_count;
        info.weekly_window_start = state.weekly_window_start;
        info.hwm_equity = state.hwm_equity;
        info.last_emergency_at = state.last_emergency_at;
        info.last_emergency_reason = state.last_emergency_reason;
        info.updated_at = chrono::Utc::now();
        *slot = Some(info);
        drop(slot);
        Ok(())
    }

    async fn load_state(&self) -> OxideResult<RiskStateInfo> {
        Ok(self
            .state
            .lock()
            .expect("state lock")
            .clone()
            .unwrap_or_else(Self::default_state_info))
    }

    async fn upsert_blacklist(&self, entry: UpsertBlacklistEntry) -> OxideResult<()> {
        let mut list = self.blacklist.lock().expect("blacklist lock");
        if let Some(existing) = list.iter_mut().find(|e| e.market_id == entry.market_id) {
            existing.token_id = entry.token_id;
            existing.scope = entry.scope;
            existing.reason = entry.reason;
            existing.expires_at = entry.expires_at;
            existing.miss_count = entry.miss_count;
            existing.updated_at = chrono::Utc::now();
        } else {
            let now = chrono::Utc::now();
            list.push(BlacklistInfo {
                market_id: entry.market_id,
                token_id: entry.token_id,
                scope: entry.scope,
                reason: entry.reason,
                expires_at: entry.expires_at,
                miss_count: entry.miss_count,
                created_at: now,
                updated_at: now,
            });
        }
        drop(list);
        Ok(())
    }

    async fn remove_blacklist(&self, market_id: &MarketId) -> OxideResult<()> {
        self.blacklist
            .lock()
            .expect("blacklist lock")
            .retain(|e| &e.market_id != market_id);
        Ok(())
    }

    async fn load_blacklist(&self) -> OxideResult<Vec<BlacklistInfo>> {
        Ok(self.blacklist.lock().expect("blacklist lock").clone())
    }

    async fn create_emergency(&self, emergency: NewEmergencySnapshot) -> OxideResult<()> {
        self.emergencies
            .lock()
            .expect("emergency lock")
            .push(emergency);
        Ok(())
    }

    async fn create_reconciliation(&self, report: NewReconciliationReport) -> OxideResult<()> {
        self.reconciliations
            .lock()
            .expect("reconciliation lock")
            .push(report);
        Ok(())
    }

    async fn create_audit(&self, audit: NewRiskAuditEvent) -> OxideResult<()> {
        self.audits.lock().expect("audit lock").push(audit);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use oxide_arb_models::domain::risk::NewRiskAuditEvent;
    use oxide_arb_models::enums::risk::RiskAuditEventType;
    use oxide_arb_risk::traits::RiskPersistence;

    use super::TestRiskPersistence;

    #[tokio::test]
    async fn captures_audit_events() {
        let persistence = TestRiskPersistence::new();
        persistence
            .create_audit(NewRiskAuditEvent {
                event_type: RiskAuditEventType::EngineHalted,
                opportunity_id: None,
                trade_id: None,
                payload: serde_json::json!({}),
                created_at: chrono::Utc::now(),
            })
            .await
            .expect("audit write");
        assert_eq!(persistence.take_audits().len(), 1);
    }

    #[tokio::test]
    async fn load_state_defaults_when_empty() {
        let persistence = TestRiskPersistence::new();
        let state = persistence.load_state().await.expect("load");
        assert!(!state.is_halted);
    }
}
