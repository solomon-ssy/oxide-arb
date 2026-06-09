//! In-memory [`ControlFactorRepository`] mock for materialization scheduler tests.
//!
//! Implements enqueue + `latest_run_for_schedule` and records publish attempts so
//! tests can assert the scheduler never publishes.

use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::control_factor::{
        AcquireMaterializationRunOutcome, AuditActor, AuditedOutcome,
        CancelMaterializationRunOutcome, ControlFactorAuditEventInfo,
        ControlFactorMaterializationRunInfo, ControlFactorPublicationInfo,
        ControlFactorStageReportInfo, ControlFactorValueInfo, EnqueueMaterializationRunOptions,
        EnqueueMaterializationRunOutcome, ExpireFactorsOutcome, MaterializationRunStatusPatch,
        NewControlFactorAuditEvent, NewControlFactorMaterializationRun,
        NewControlFactorPublication, NewControlFactorStageReport, NewControlFactorValue,
        PublishPublicationOutcome, RunTransitionOutcome,
    },
    enums::control_factor::{
        ControlFactorType, FactorStatus, MaterializationRunStatus, MaterializationStageName,
        PublicationMode, PublicationStatus,
    },
    types::{ControlFactorId, FactorPublicationId, MaterializationRunId},
};
use oxide_arb_repository::traits::ControlFactorRepository;

/// Default `trigger_ref` used by [`crate::materialization::scheduled_materialization_run_info`].
pub const EXECUTION_QUALITY_HOURLY_SCHEDULE_ID: &str = "execution-quality-hourly";

/// Records enqueues and latest-run lookups for scheduler unit tests.
#[derive(Default)]
pub struct MockSchedulerControlFactorRepository {
    runs: Mutex<Vec<ControlFactorMaterializationRunInfo>>,
    enqueued: Mutex<Vec<NewControlFactorMaterializationRun>>,
    publish_calls: Mutex<u32>,
}

impl MockSchedulerControlFactorRepository {
    /// Pre-seeds materialization runs returned by [`ControlFactorRepository::latest_run_for_schedule`].
    #[must_use]
    pub fn with_runs(runs: Vec<ControlFactorMaterializationRunInfo>) -> Self {
        Self {
            runs: Mutex::new(runs),
            ..Self::default()
        }
    }

    /// Number of runs passed to [`ControlFactorRepository::enqueue_materialization_run`].
    pub fn enqueued_count(&self) -> usize {
        self.enqueued.lock().unwrap().len()
    }

    /// Number of times [`ControlFactorRepository::publish_publication`] was invoked.
    pub fn publish_calls(&self) -> u32 {
        *self.publish_calls.lock().unwrap()
    }
}

#[async_trait]
impl ControlFactorRepository for MockSchedulerControlFactorRepository {
    async fn enqueue_materialization_run(
        &self,
        run: NewControlFactorMaterializationRun,
        _options: EnqueueMaterializationRunOptions,
    ) -> Result<EnqueueMaterializationRunOutcome, StorageError> {
        let info = materialization_run_info_from_new(&run);
        self.enqueued.lock().unwrap().push(run);
        self.runs.lock().unwrap().push(info.clone());
        Ok(EnqueueMaterializationRunOutcome::Created(info))
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
        schedule_id: &str,
        statuses: &[MaterializationRunStatus],
    ) -> Result<Option<ControlFactorMaterializationRunInfo>, StorageError> {
        let runs = self.runs.lock().unwrap();
        Ok(runs
            .iter()
            .filter(|run| run.trigger_ref.as_deref() == Some(schedule_id))
            .filter(|run| statuses.is_empty() || statuses.contains(&run.status))
            .max_by_key(|run| run.created_at)
            .cloned())
    }

    async fn list_queued_materialization_runs(
        &self,
        limit: u64,
    ) -> Result<Vec<MaterializationRunId>, StorageError> {
        let runs = self.runs.lock().unwrap();
        Ok(runs
            .iter()
            .filter(|run| run.status == MaterializationRunStatus::Queued)
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .map(|run| run.materialization_run_id.clone())
            .collect())
    }

    async fn try_acquire_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
        _started_at: DateTime<Utc>,
    ) -> Result<AcquireMaterializationRunOutcome, StorageError> {
        Err(scheduler_unexpected("try_acquire_materialization_run"))
    }

    async fn retry_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
    ) -> Result<RunTransitionOutcome, StorageError> {
        Err(scheduler_unexpected("retry_materialization_run"))
    }

    async fn transition_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
        _expected_from: MaterializationRunStatus,
        _target: MaterializationRunStatus,
        _patch: MaterializationRunStatusPatch,
    ) -> Result<RunTransitionOutcome, StorageError> {
        Err(scheduler_unexpected("transition_materialization_run"))
    }

    async fn cancel_materialization_run(
        &self,
        _run_id: &MaterializationRunId,
        _reason: &str,
        _cancelled_at: DateTime<Utc>,
    ) -> Result<CancelMaterializationRunOutcome, StorageError> {
        Err(scheduler_unexpected("cancel_materialization_run"))
    }

    async fn upsert_stage_report(
        &self,
        _report: NewControlFactorStageReport,
    ) -> Result<ControlFactorStageReportInfo, StorageError> {
        Err(scheduler_unexpected("upsert_stage_report"))
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
        Err(scheduler_unexpected("create_factor"))
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
        _publication: NewControlFactorPublication,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<PublishPublicationOutcome, StorageError> {
        *self.publish_calls.lock().unwrap() += 1;
        Err(scheduler_unexpected("publish_publication"))
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
        _target_publication_id: &FactorPublicationId,
        _audit: NewControlFactorAuditEvent,
    ) -> Result<AuditedOutcome<ControlFactorPublicationInfo>, StorageError> {
        Err(scheduler_unexpected("rollback_publication"))
    }

    async fn append_audit_event(
        &self,
        _event: NewControlFactorAuditEvent,
    ) -> Result<ControlFactorAuditEventInfo, StorageError> {
        Err(scheduler_unexpected("append_audit_event"))
    }

    async fn load_audit_chain(
        &self,
        _from_sequence: i64,
        _limit: u64,
    ) -> Result<Vec<ControlFactorAuditEventInfo>, StorageError> {
        Ok(Vec::new())
    }
}

fn scheduler_unexpected(method: &str) -> StorageError {
    StorageError::Codec(format!("scheduler must not call {method}"))
}

fn materialization_run_info_from_new(
    new: &NewControlFactorMaterializationRun,
) -> ControlFactorMaterializationRunInfo {
    ControlFactorMaterializationRunInfo {
        materialization_run_id: new.materialization_run_id.clone(),
        run_dedupe_key: new.run_dedupe_key.clone(),
        run_kind: new.run_kind,
        trigger_type: new.trigger_type,
        trigger_ref: new.trigger_ref.clone(),
        status: new.status,
        window_from: new.window_from,
        window_to: new.window_to,
        source_delay_secs: new.source_delay_secs,
        market_filter: new.market_filter.clone(),
        requested_factor_types: new.requested_factor_types.clone(),
        data_requirements: new.data_requirements.clone(),
        runtime_config_ref: new.runtime_config_ref.clone(),
        simulation_config_hash: new.simulation_config_hash.clone(),
        quality_gate_policy_hash: new.quality_gate_policy_hash.clone(),
        output_policy: new.output_policy,
        manifest: new.manifest.clone(),
        manifest_hash: new.manifest_hash.clone(),
        report: new.report.clone(),
        code_git_sha: new.code_git_sha.clone(),
        created_by: new.created_by.clone(),
        started_at: new.started_at,
        finished_at: new.finished_at,
        failure_code: new.failure_code.clone(),
        failure_detail: new.failure_detail.clone(),
        report_uri: new.report_uri.clone(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}
