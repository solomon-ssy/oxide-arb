use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::{
    domain::{
        blacklist::{BlacklistInfo, UpsertBlacklistEntry},
        risk::{
            NewEmergencySnapshot, NewReconciliationReport, NewRiskAuditEvent, RiskStateInfo,
            UpsertRiskEngineState,
        },
    },
    types::MarketId,
};
use oxide_arb_repository::{
    postgres::{
        PgBlacklistPersistenceRepository, PgEmergencyRepository, PgReconciliationRepository,
        PgRiskAuditRepository, PgRiskStateRepository,
    },
    traits::{
        BlacklistPersistenceRepository, EmergencyRepository, ReconciliationRepository,
        RiskAuditRepository, RiskStateRepository,
    },
};
use oxide_arb_risk::traits::RiskPersistence;
use std::sync::Arc;

pub struct CoreRiskPersistence {
    risk_state: Arc<PgRiskStateRepository>,
    blacklist: Arc<PgBlacklistPersistenceRepository>,
    audit: Arc<PgRiskAuditRepository>,
    emergency: Arc<PgEmergencyRepository>,
    reconciliation: Arc<PgReconciliationRepository>,
}

impl CoreRiskPersistence {
    pub const fn new(
        risk_state: Arc<PgRiskStateRepository>,
        blacklist: Arc<PgBlacklistPersistenceRepository>,
        audit: Arc<PgRiskAuditRepository>,
        emergency: Arc<PgEmergencyRepository>,
        reconciliation: Arc<PgReconciliationRepository>,
    ) -> Self {
        Self {
            risk_state,
            blacklist,
            audit,
            emergency,
            reconciliation,
        }
    }
}

#[async_trait::async_trait]
impl RiskPersistence for CoreRiskPersistence {
    async fn upsert_state(&self, state: UpsertRiskEngineState) -> OxideResult<()> {
        self.risk_state
            .upsert(state)
            .await
            .map_err(OxideError::from)
    }

    async fn load_state(&self) -> OxideResult<RiskStateInfo> {
        self.risk_state.load().await.map_err(OxideError::from)
    }

    async fn upsert_blacklist(&self, entry: UpsertBlacklistEntry) -> OxideResult<()> {
        self.blacklist
            .upsert(entry)
            .await
            .map_err(OxideError::from)?;
        Ok(())
    }

    async fn remove_blacklist(&self, market_id: &MarketId) -> OxideResult<()> {
        self.blacklist
            .remove(market_id)
            .await
            .map_err(OxideError::from)
    }

    async fn load_blacklist(&self) -> OxideResult<Vec<BlacklistInfo>> {
        self.blacklist.load_active().await.map_err(OxideError::from)
    }

    async fn create_emergency(&self, emergency: NewEmergencySnapshot) -> OxideResult<()> {
        self.emergency
            .create(emergency)
            .await
            .map_err(OxideError::from)?;
        Ok(())
    }

    async fn create_reconciliation(&self, report: NewReconciliationReport) -> OxideResult<()> {
        self.reconciliation
            .create(report)
            .await
            .map_err(OxideError::from)?;
        Ok(())
    }

    async fn create_audit(&self, audit: NewRiskAuditEvent) -> OxideResult<()> {
        self.audit.create(audit).await.map_err(OxideError::from)
    }
}
