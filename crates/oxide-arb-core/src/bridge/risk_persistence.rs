use std::sync::Arc;

use chrono::{DateTime, Utc};
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::{
    domain::{
        blacklist::{BlacklistInfo, UpsertBlacklistEntry},
        risk::{
            FillCommit, NewEmergencySnapshot, NewReconciliationReport, NewRiskAuditEvent,
            RiskStateInfo, UpsertRiskEngineState,
        },
    },
    types::{MarketId, TradeId},
};
use oxide_arb_repository::{
    postgres::{
        PgBlacklistPersistenceRepository, PgEmergencyRepository, PgReconciliationRepository,
        PgRiskAuditRepository, PgRiskStateRepository,
        risk_fill::{PgRiskFillClaim, PgRiskFillCommitGuard, PgRiskFillRepository},
    },
    traits::{
        BlacklistPersistenceRepository, EmergencyRepository, ReconciliationRepository,
        RiskAuditRepository, RiskStateRepository,
    },
};
use oxide_arb_risk::traits::{FillClaim, RiskFillCommitGuard, RiskPersistence};

pub struct CoreRiskPersistence {
    risk_state: Arc<PgRiskStateRepository>,
    blacklist: Arc<PgBlacklistPersistenceRepository>,
    audit: Arc<PgRiskAuditRepository>,
    risk_fill: Arc<PgRiskFillRepository>,
    emergency: Arc<PgEmergencyRepository>,
    reconciliation: Arc<PgReconciliationRepository>,
}

struct CoreRiskFillCommitGuard {
    inner: PgRiskFillCommitGuard,
}

impl CoreRiskPersistence {
    pub const fn new(
        risk_state: Arc<PgRiskStateRepository>,
        blacklist: Arc<PgBlacklistPersistenceRepository>,
        audit: Arc<PgRiskAuditRepository>,
        risk_fill: Arc<PgRiskFillRepository>,
        emergency: Arc<PgEmergencyRepository>,
        reconciliation: Arc<PgReconciliationRepository>,
    ) -> Self {
        Self {
            risk_state,
            blacklist,
            audit,
            risk_fill,
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

    async fn begin_fill<'a>(
        &'a self,
        trade_id: &TradeId,
        applied_at: DateTime<Utc>,
    ) -> OxideResult<FillClaim<'a>> {
        match self
            .risk_fill
            .begin_fill(trade_id, applied_at)
            .await
            .map_err(OxideError::from)?
        {
            PgRiskFillClaim::AlreadyApplied => Ok(FillClaim::AlreadyApplied),
            PgRiskFillClaim::Claimed(inner) => {
                Ok(FillClaim::Claimed(Box::new(CoreRiskFillCommitGuard {
                    inner,
                })))
            }
        }
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

#[async_trait::async_trait]
impl RiskFillCommitGuard for CoreRiskFillCommitGuard {
    async fn commit(self: Box<Self>, commit: FillCommit) -> OxideResult<()> {
        self.inner.commit(commit).await.map_err(OxideError::from)
    }
}
