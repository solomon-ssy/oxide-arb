use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::control_factor::{
        AcquireMaterializationRunOutcome, CancelMaterializationRunOutcome,
        ControlFactorAuditEventInfo, ControlFactorMaterializationRunInfo,
        ControlFactorPublicationInfo, ControlFactorStageReportInfo, ControlFactorValueInfo,
        EnqueueMaterializationRunOptions, EnqueueMaterializationRunOutcome,
        MaterializationRunStatusPatch, NewControlFactorAuditEvent,
        NewControlFactorMaterializationRun, NewControlFactorPublication,
        NewControlFactorStageReport, NewControlFactorValue, RunTransitionOutcome,
    },
    enums::control_factor::{
        FactorStatus, MaterializationRunStatus, MaterializationStageName, PublicationMode,
    },
    types::{ControlFactorId, FactorPublicationId, MaterializationRunId},
};

#[async_trait::async_trait]
pub trait ControlFactorRepository: Send + Sync {
    async fn enqueue_materialization_run(
        &self,
        run: NewControlFactorMaterializationRun,
        options: EnqueueMaterializationRunOptions,
    ) -> Result<EnqueueMaterializationRunOutcome, StorageError>;

    async fn load_materialization_run(
        &self,
        run_id: &MaterializationRunId,
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError>;

    async fn find_materialization_run_by_dedupe_key(
        &self,
        dedupe_key: &str,
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError>;

    async fn try_acquire_materialization_run(
        &self,
        run_id: &MaterializationRunId,
        started_at: DateTime<Utc>,
    ) -> Result<AcquireMaterializationRunOutcome, StorageError>;

    async fn retry_materialization_run(
        &self,
        run_id: &MaterializationRunId,
    ) -> Result<RunTransitionOutcome, StorageError>;

    async fn transition_materialization_run(
        &self,
        run_id: &MaterializationRunId,
        expected_from: MaterializationRunStatus,
        target: MaterializationRunStatus,
        patch: MaterializationRunStatusPatch,
    ) -> Result<RunTransitionOutcome, StorageError>;

    async fn cancel_materialization_run(
        &self,
        run_id: &MaterializationRunId,
        reason: &str,
        cancelled_at: DateTime<Utc>,
    ) -> Result<CancelMaterializationRunOutcome, StorageError>;

    async fn upsert_stage_report(
        &self,
        report: NewControlFactorStageReport,
    ) -> Result<ControlFactorStageReportInfo, StorageError>;

    async fn load_stage_report(
        &self,
        run_id: &MaterializationRunId,
        stage_name: MaterializationStageName,
    ) -> Result<Option<ControlFactorStageReportInfo>, StorageError>;

    async fn list_stage_reports(
        &self,
        run_id: &MaterializationRunId,
    ) -> Result<Vec<ControlFactorStageReportInfo>, StorageError>;

    async fn create_factor(
        &self,
        factor: NewControlFactorValue,
    ) -> Result<ControlFactorValueInfo, StorageError>;

    async fn load_factor(
        &self,
        factor_id: &ControlFactorId,
    ) -> Result<Option<ControlFactorValueInfo>, StorageError>;

    async fn list_factors_by_run(
        &self,
        run_id: &MaterializationRunId,
    ) -> Result<Vec<ControlFactorValueInfo>, StorageError>;

    async fn transition_factor(
        &self,
        factor_id: &ControlFactorId,
        status: FactorStatus,
    ) -> Result<Option<ControlFactorValueInfo>, StorageError>;

    async fn create_publication(
        &self,
        publication: NewControlFactorPublication,
    ) -> Result<ControlFactorPublicationInfo, StorageError>;

    async fn load_publication(
        &self,
        publication_id: &FactorPublicationId,
    ) -> Result<Option<ControlFactorPublicationInfo>, StorageError>;

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
