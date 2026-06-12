//! Minimal [`ControlFactorRepository`] fake for governance notify wiring tests.

use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        Paginated, ReplayPageQuery,
        control_factor::{
            AcquireMaterializationRunOutcome, AuditActor, AuditedOutcome,
            CancelMaterializationRunOutcome, ControlFactorAuditEventInfo,
            ControlFactorMaterializationRunInfo, ControlFactorPublicationInfo,
            ControlFactorStageReportInfo, ControlFactorValueInfo, EnqueueMaterializationRunOptions,
            EnqueueMaterializationRunOutcome, ExpireFactorsOutcome, MaterializationRunStatusPatch,
            NewControlFactorAuditEvent, NewControlFactorMaterializationRun,
            NewControlFactorPublication, NewControlFactorStageReport, NewControlFactorValue,
            PublishPublicationOutcome, RunTransitionOutcome,
        },
    },
    enums::control_factor::{
        ControlFactorType, FactorStatus, MaterializationRunStatus, MaterializationStageName,
        PublicationMode, PublicationStatus,
    },
    types::{AuditEventId, ControlFactorId, FactorPublicationId, MaterializationRunId},
};
use oxide_arb_repository::traits::ControlFactorRepository;

/// Records governance publication calls and returns synthetic successes.
#[derive(Default)]
pub struct MockGovernanceControlFactorRepository {
    publish_calls: Mutex<u32>,
    rollback_calls: Mutex<u32>,
}

impl MockGovernanceControlFactorRepository {
    /// Number of times [`ControlFactorRepository::publish_publication`] completed.
    pub fn publish_calls(&self) -> u32 {
        *self.publish_calls.lock().unwrap()
    }

    /// Number of times [`ControlFactorRepository::rollback_publication`] completed.
    pub fn rollback_calls(&self) -> u32 {
        *self.rollback_calls.lock().unwrap()
    }
}

#[async_trait]
impl ControlFactorRepository for MockGovernanceControlFactorRepository {
    async fn enqueue_materialization_run(
        &self,
        _run: NewControlFactorMaterializationRun,
        _options: EnqueueMaterializationRunOptions,
    ) -> Result<EnqueueMaterializationRunOutcome, StorageError> {
        Err(governance_unexpected("enqueue_materialization_run"))
    }

    async fn load_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError> {
        Ok(None)
    }

    async fn find_materialization_run_by_dedupe_key(
        &self,
        _dedupe_key: &str,
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError> {
        Ok(None)
    }

    async fn latest_run_for_schedule(
        &self,
        _schedule_id: &str,
        _statuses: &[MaterializationRunStatus],
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError> {
        Ok(None)
    }

    async fn list_queued_materialization_runs(
        &self,
        _limit: u64,
    ) -> Result<Vec<MaterializationRunId>, StorageError> {
        Ok(Vec::new())
    }

    async fn list_active_materialization_runs(
        &self,
        _limit: u64,
    ) -> Result<Vec<ControlFactorMaterializationRunInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn page_materialization_runs(
        &self,
        query: &ReplayPageQuery,
    ) -> Result<Paginated<ControlFactorMaterializationRunInfo>, StorageError> {
        Ok(Paginated::from_request(
            Vec::new(),
            0,
            &query.page.normalized(),
        ))
    }

    async fn try_acquire_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
        _started_at: DateTime<Utc>,
    ) -> Result<AcquireMaterializationRunOutcome, StorageError> {
        Err(governance_unexpected("try_acquire_materialization_run"))
    }

    async fn retry_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
    ) -> Result<RunTransitionOutcome, StorageError> {
        Err(governance_unexpected("retry_materialization_run"))
    }

    async fn transition_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
        _expected_from: MaterializationRunStatus,
        _target: MaterializationRunStatus,
        _patch: MaterializationRunStatusPatch,
    ) -> Result<RunTransitionOutcome, StorageError> {
        Err(governance_unexpected("transition_materialization_run"))
    }

    async fn cancel_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
        _reason: &str,
        _cancelled_at: DateTime<Utc>,
    ) -> Result<CancelMaterializationRunOutcome, StorageError> {
        Err(governance_unexpected("cancel_materialization_run"))
    }

    async fn upsert_stage_report(
        &self,
        _report: NewControlFactorStageReport,
    ) -> Result<ControlFactorStageReportInfo, StorageError> {
        Err(governance_unexpected("upsert_stage_report"))
    }

    async fn load_stage_report(
        &self,
        _run_id: &MaterializationRunId,
        _stage_name: MaterializationStageName,
    ) -> Result<Option<ControlFactorStageReportInfo>, StorageError> {
        Ok(None)
    }

    async fn list_stage_reports(
        &self,
        _run_id: &MaterializationRunId,
    ) -> Result<Vec<ControlFactorStageReportInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn create_factor(
        &self,
        _factor: NewControlFactorValue,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorValueInfo, StorageError> {
        Err(governance_unexpected("create_factor"))
    }

    async fn load_factor(
        &self,
        _factor_id: &ControlFactorId,
    ) -> Result<Option<ControlFactorValueInfo>, StorageError> {
        Ok(None)
    }

    async fn load_factors_by_ids(
        &self,
        _factor_ids: &[ControlFactorId],
    ) -> Result<Vec<ControlFactorValueInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn list_factors_by_run(
        &self,
        _run_id: &MaterializationRunId,
    ) -> Result<Vec<ControlFactorValueInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn list_factors_by_status(
        &self,
        _status: FactorStatus,
        _factor_type: Option<ControlFactorType>,
    ) -> Result<Vec<ControlFactorValueInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn reject_factor(
        &self,
        _factor_id: &ControlFactorId,
        _status_reason: &str,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<Option<AuditedOutcome<ControlFactorValueInfo>>, StorageError> {
        Ok(None)
    }

    async fn expire_factors(
        &self,
        _now: DateTime<Utc>,
        _actor: AuditActor,
    ) -> Result<ExpireFactorsOutcome, StorageError> {
        Ok(ExpireFactorsOutcome::default())
    }

    async fn publish_publication(
        &self,
        publication: NewControlFactorPublication,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<PublishPublicationOutcome, StorageError> {
        *self.publish_calls.lock().unwrap() += 1;
        Ok(PublishPublicationOutcome::Published(AuditedOutcome::new(
            publication_info_from(&publication),
            AuditEventId::from_v7(),
        )))
    }

    async fn load_publication(
        &self,
        _publication_id: &FactorPublicationId,
    ) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
        Ok(None)
    }

    async fn load_active_publication(
        &self,
        _mode: PublicationMode,
    ) -> Result<Option<ControlFactorPublicationInfo>, StorageError> {
        // Genesis publish: no rollback target required.
        Ok(None)
    }

    async fn list_publications(
        &self,
        _mode: PublicationMode,
        _status: Option<PublicationStatus>,
        _limit: u64,
    ) -> Result<Vec<ControlFactorPublicationInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn rollback_publication(
        &self,
        _active_publication_id: &FactorPublicationId,
        target_publication_id: &FactorPublicationId,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<AuditedOutcome<ControlFactorPublicationInfo>, StorageError> {
        *self.rollback_calls.lock().unwrap() += 1;
        Ok(AuditedOutcome::new(
            rollback_target_info(target_publication_id),
            AuditEventId::from_v7(),
        ))
    }

    async fn append_audit_event(
        &self,
        _event: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorAuditEventInfo, StorageError> {
        Err(governance_unexpected("append_audit_event"))
    }

    async fn load_audit_chain(
        &self,
        _from_sequence: i64,
        _limit: u64,
    ) -> Result<Vec<ControlFactorAuditEventInfo>, StorageError> {
        Ok(Vec::new())
    }
}

fn governance_unexpected(method: &str) -> StorageError {
    StorageError::Codec(format!("governance notify mock must not call {method}"))
}

fn publication_info_from(
    publication: &NewControlFactorPublication,
) -> ControlFactorPublicationInfo {
    let now = Utc::now();
    ControlFactorPublicationInfo {
        publication_id: publication.publication_id.clone(),
        mode: publication.mode,
        factor_ids: publication.factor_ids.clone(),
        previous_publication_id: publication.previous_publication_id.clone(),
        status: PublicationStatus::Active,
        effective_from: publication.effective_from,
        expires_at: publication.expires_at,
        approved_by: publication.approved_by.clone(),
        approval_reason: publication.approval_reason.clone(),
        publication_hash: publication.publication_hash.clone(),
        created_at: now,
        updated_at: now,
    }
}

fn rollback_target_info(
    target_publication_id: &FactorPublicationId,
) -> ControlFactorPublicationInfo {
    let now = Utc::now();
    ControlFactorPublicationInfo {
        publication_id: target_publication_id.clone(),
        mode: PublicationMode::Published,
        factor_ids: vec![ControlFactorId::from_v7()],
        previous_publication_id: None,
        status: PublicationStatus::Active,
        effective_from: now - chrono::Duration::hours(1),
        expires_at: now + chrono::Duration::days(1),
        approved_by: Some("risk_owner".into()),
        approval_reason: "rollback target".into(),
        publication_hash: "blake3:rollback-target".into(),
        created_at: now,
        updated_at: now,
    }
}
