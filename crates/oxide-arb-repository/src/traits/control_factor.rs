use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::control_factor::{
        ControlFactorAuditEventInfo, ControlFactorMaterializationRunInfo,
        ControlFactorPublicationInfo, ControlFactorStageReportInfo, ControlFactorValueInfo,
        NewControlFactorAuditEvent, NewControlFactorMaterializationRun,
        NewControlFactorPublication, NewControlFactorStageReport, NewControlFactorValue,
    },
    enums::control_factor::{FactorStatus, PublicationMode},
    types::{ControlFactorId, FactorPublicationId},
};

#[async_trait::async_trait]
pub trait ControlFactorRepository: Send + Sync {
    async fn create_materialization_run(
        &self,
        run: NewControlFactorMaterializationRun,
    ) -> Result<ControlFactorMaterializationRunInfo, StorageError>;

    async fn create_stage_report(
        &self,
        report: NewControlFactorStageReport,
    ) -> Result<ControlFactorStageReportInfo, StorageError>;

    async fn create_factor(
        &self,
        factor: NewControlFactorValue,
    ) -> Result<ControlFactorValueInfo, StorageError>;

    async fn transition_factor(
        &self,
        factor_id: &ControlFactorId,
        status: FactorStatus,
    ) -> Result<Option<ControlFactorValueInfo>, StorageError>;

    async fn create_publication(
        &self,
        publication: NewControlFactorPublication,
    ) -> Result<ControlFactorPublicationInfo, StorageError>;

    async fn activate_publication(
        &self,
        publication_id: &FactorPublicationId,
        actor: &str,
        reason: &str,
    ) -> Result<ControlFactorPublicationInfo, StorageError>;

    async fn rollback_publication(
        &self,
        active_publication_id: &FactorPublicationId,
        target_publication_id: &FactorPublicationId,
        actor: &str,
        reason: &str,
    ) -> Result<ControlFactorPublicationInfo, StorageError>;

    async fn expire_factors(&self, now: DateTime<Utc>) -> Result<u64, StorageError>;

    async fn append_audit_event(
        &self,
        event: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorAuditEventInfo, StorageError>;

    async fn load_active_publication(
        &self,
        mode: PublicationMode,
    ) -> Result<Option<ControlFactorPublicationInfo>, StorageError>;
}
