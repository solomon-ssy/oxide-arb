//! Governed strict break-glass recovery port.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{api::AccountRecoveryIncidentView, quant::AccountRecoverySellAllocation},
    types::{AccountRecoveryIncidentId, AccountRecoveryManifestId, UserId},
};

#[async_trait]
pub trait AccountRecoveryControlPort: Send + Sync {
    async fn active_incident(&self) -> QuantResult<Option<AccountRecoveryIncidentView>>;

    async fn incident(
        &self,
        incident_id: &AccountRecoveryIncidentId,
    ) -> QuantResult<Option<AccountRecoveryIncidentView>>;

    async fn pause_and_reconcile(
        &self,
        incident_id: &AccountRecoveryIncidentId,
        expected_revision: i64,
        allocations: Vec<AccountRecoverySellAllocation>,
    ) -> QuantResult<AccountRecoveryIncidentView>;

    async fn seal(
        &self,
        incident_id: &AccountRecoveryIncidentId,
        manifest_id: AccountRecoveryManifestId,
        expected_revision: i64,
        actor_id: UserId,
    ) -> QuantResult<AccountRecoveryIncidentView>;

    async fn unpause_and_finalize(
        &self,
        incident_id: &AccountRecoveryIncidentId,
        expected_revision: i64,
    ) -> QuantResult<AccountRecoveryIncidentView>;
}
