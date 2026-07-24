//! Deterministic feature-parity execution and fail-closed latch transitions.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration as StdDuration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::QuantFeatureParityEventRow,
    domain::{
        api::{FeatureParityJobParams, FeatureParityRunView},
        ports::FeatureParityExecutionPort,
        quant::{CompleteFeatureParityRun, FeatureParityRunInfo, JobProgressSink},
    },
    enums::{
        common::{AlertCategory, AlertLevel, AlertSource},
        quant::{
            FeatureCellState, FeatureParityEventStatus, FeatureParityRunKind,
            FeatureParityRunStatus, FeatureParityStage, FeatureParityStateTransition,
        },
    },
    types::{
        ContentHash, DiagnosticCode, FeatureParityDetail, FeatureParityDetailSource,
        FeatureParityEventId, MarketId, ModelRunId, ModelVersionId, RecommendationReportId,
        ResearchJobProgress, TrainingDatasetId,
    },
};
use quant_pivot_repository::traits::{
    FactWriter, FeatureParityRepository, RecommendationReportRepository,
};
use quant_pivot_research::hashing::ResearchHasher;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    observability::{
        alert_dispatcher::{Alert, AlertDispatcher},
        metrics_hub::MetricsHub,
    },
    report::ReportLifecycleService,
};
const SAMPLE_FRACTION_NUMERATOR: usize = 1;
const SAMPLE_FRACTION_DENOMINATOR: usize = 10;
const SAMPLE_MINIMUM: usize = 20;

/// Stable serving decision that may be replayed from durable evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeatureParitySubject {
    /// A real persisted live-inference run. This subject has model-input,
    /// factor, and prediction evidence in addition to the earlier stages.
    ModelRun(ModelRunId),
    /// A committed report that intentionally stopped before model inference.
    /// Its replay stops at the last stage represented by the report evidence.
    PreInferenceReport(RecommendationReportId),
}

/// Stable serving row that may be replayed from durable evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureParityCandidate {
    /// Stable, globally unique key used by deterministic sampling.
    pub sampling_key: String,
    /// Strongly typed evidence owner. A report id is never placed into a model
    /// run field and no synthetic model run is created for pre-inference paths.
    pub subject: FeatureParitySubject,
    /// Report-local row sampling unit. `None` is valid only for a report whose
    /// persisted selection is empty; no fake market is introduced.
    pub market_id: Option<MarketId>,
    pub decision_at: DateTime<Utc>,
}

/// One exact side of a stage comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureParityEvidence {
    pub state: Option<FeatureCellState>,
    pub value: Option<String>,
    pub effective_at: Option<DateTime<Utc>>,
    pub available_at: Option<DateTime<Utc>>,
    pub cutoff: Option<DateTime<Utc>>,
    /// Canonical fingerprint covering value, state, provenance, cutoff, DQ,
    /// transform, and route. It must never be empty.
    pub fingerprint: String,
}

/// One replayed stage comparison for a serving decision.
#[derive(Debug, Clone)]
pub struct FeatureParityComparison {
    pub sampling_key: String,
    pub decision_at: DateTime<Utc>,
    pub stage: FeatureParityStage,
    pub report_id: Option<RecommendationReportId>,
    pub model_run_id: Option<ModelRunId>,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub market_id: Option<MarketId>,
    pub feature_name: Option<String>,
    pub reason: Option<String>,
    pub online: FeatureParityEvidence,
    pub replay: FeatureParityEvidence,
    pub transform_hash: Option<ContentHash>,
    pub detail: FeatureParityDetailSource,
}

/// Evidence whose source writer has not reached the required watermark yet.
#[derive(Debug, Clone)]
pub struct PendingFeatureParityComparison {
    pub sampling_key: String,
    pub decision_at: DateTime<Utc>,
    pub stage: FeatureParityStage,
    pub report_id: Option<RecommendationReportId>,
    pub model_run_id: Option<ModelRunId>,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub market_id: Option<MarketId>,
    pub feature_name: Option<String>,
    pub reason: String,
    /// Online evidence when the primary serving row is durable but a downstream
    /// source is behind. `None` when the serving-evidence writer itself has not
    /// reached the decision; absence is never replaced by a synthetic value.
    pub online: Option<FeatureParityEvidence>,
    pub required_watermark: DateTime<Utc>,
    pub observed_watermark: Option<DateTime<Utc>>,
}

/// One source attempt. Pending rows are retried and are never counted as a
/// deterministic mismatch before the materialization deadline.
#[derive(Debug, Clone, Default)]
pub struct FeatureParityReplayAttempt {
    pub comparisons: Vec<FeatureParityComparison>,
    pub pending: Vec<PendingFeatureParityComparison>,
}

/// Durable online-evidence reader plus exact replay implementation.
///
/// Splitting discovery from replay lets the executor own the sampling contract;
/// a source cannot accidentally choose a convenient, non-deterministic subset.
#[async_trait]
pub trait FeatureParityReplaySource: Send + Sync {
    async fn list_candidates(
        &self,
        run: &FeatureParityRunInfo,
    ) -> QuantResult<Vec<FeatureParityCandidate>>;

    async fn replay(
        &self,
        run: &FeatureParityRunInfo,
        candidates: &[FeatureParityCandidate],
    ) -> QuantResult<FeatureParityReplayAttempt>;
}

/// Containment hook invoked after the mismatch transaction opens the global
/// latch. Implementations revoke affected reports and cascade-invalidate their
/// still-active intents.
#[async_trait]
pub trait FeatureParityIncidentPort: Send + Sync {
    async fn contain(
        &self,
        run: &FeatureParityRunInfo,
        report_ids: &[RecommendationReportId],
    ) -> QuantResult<()>;
}

/// Report-lifecycle containment adapter. Exit/settlement paths stay available;
/// only affected reports and their pre-submission intents are revoked.
#[async_trait]
trait ReportContainmentPort: Send + Sync {
    async fn revoke_and_cascade(
        &self,
        report_id: &RecommendationReportId,
        reason: &str,
        revoked_at: DateTime<Utc>,
    ) -> QuantResult<()>;
}

#[async_trait]
impl ReportContainmentPort for ReportLifecycleService {
    async fn revoke_and_cascade(
        &self,
        report_id: &RecommendationReportId,
        reason: &str,
        revoked_at: DateTime<Utc>,
    ) -> QuantResult<()> {
        self.contain_parity_incident(report_id, reason, revoked_at)
            .await
    }
}

#[async_trait]
trait AffectedReportLookup: Send + Sync {
    async fn find_actionable_ids_between(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> QuantResult<Vec<RecommendationReportId>>;
}

struct RepositoryAffectedReportLookup(Arc<dyn RecommendationReportRepository>);

#[async_trait]
impl AffectedReportLookup for RepositoryAffectedReportLookup {
    async fn find_actionable_ids_between(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> QuantResult<Vec<RecommendationReportId>> {
        self.0
            .find_actionable_ids_between(window_start, window_end)
            .await
            .map_err(QuantError::from)
    }
}

pub struct ReportFeatureParityIncidentResponse {
    reports: Arc<dyn ReportContainmentPort>,
    report_lookup: Arc<dyn AffectedReportLookup>,
    alerts: Arc<AlertDispatcher>,
    metrics: Arc<MetricsHub>,
}

impl ReportFeatureParityIncidentResponse {
    #[must_use]
    pub fn new(
        reports: Arc<ReportLifecycleService>,
        report_repo: Arc<dyn RecommendationReportRepository>,
        alerts: Arc<AlertDispatcher>,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            reports,
            report_lookup: Arc::new(RepositoryAffectedReportLookup(report_repo)),
            alerts,
            metrics,
        }
    }

    #[cfg(test)]
    fn with_test_ports(
        reports: Arc<dyn ReportContainmentPort>,
        report_lookup: Arc<dyn AffectedReportLookup>,
        alerts: Arc<AlertDispatcher>,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            reports,
            report_lookup,
            alerts,
            metrics,
        }
    }
}

#[async_trait]
impl FeatureParityIncidentPort for ReportFeatureParityIncidentResponse {
    async fn contain(
        &self,
        run: &FeatureParityRunInfo,
        report_ids: &[RecommendationReportId],
    ) -> QuantResult<()> {
        self.alerts
            .dispatch_operator_notification(
                Alert::new(
                    format!("feature-parity:{}", run.run_id),
                    AlertLevel::Critical,
                    AlertCategory::TradingSafety,
                    AlertSource::System,
                    "Deterministic feature parity failure",
                    format!(
                        "run={} status={} window=[{}, {}) latch=open; revoking affected reports and invalidating entry intents",
                        run.run_id,
                        run.status.as_str(),
                        run.window_start,
                        run.window_end
                    ),
                    Utc::now(),
                )
                .with_affects_trading(true)
                .with_visible_toast(true)
                .with_dedupe_secs(0),
            )
            .await;
        let reason = format!("feature parity containment for run {}", run.run_id);
        let mut affected = report_ids.to_vec();
        if let Some(report_id) = run.report_id.as_ref() {
            affected.push(*report_id);
        }
        if affected.is_empty() {
            affected = self
                .report_lookup
                .find_actionable_ids_between(run.window_start, run.window_end)
                .await?;
        }
        affected.sort_by_key(ToString::to_string);
        affected.dedup_by(|left, right| left == right);
        let mut failures = Vec::new();
        for report_id in &affected {
            if let Err(error) = self
                .reports
                .revoke_and_cascade(report_id, &reason, Utc::now())
                .await
            {
                failures.push(format!("{report_id}: {error}"));
            }
        }
        if !failures.is_empty() {
            self.metrics.record_feature_parity_containment("failed");
            return Err(ResearchError::Determinism {
                detail: format!(
                    "parity run {} containment incomplete: {}",
                    run.run_id,
                    failures.join("; ")
                ),
            }
            .into());
        }
        self.metrics.record_feature_parity_containment("completed");
        Ok(())
    }
}

/// Production executor for full/sampled deterministic parity runs.
pub struct FeatureParityExecutor {
    parity: Arc<dyn FeatureParityRepository>,
    source: Arc<dyn FeatureParityReplaySource>,
    evidence_writer: Arc<dyn FactWriter<QuantFeatureParityEventRow>>,
    incidents: Arc<dyn FeatureParityIncidentPort>,
    metrics: Arc<MetricsHub>,
    minimum_pending_timeout: Duration,
    pending_retry_interval: StdDuration,
}

impl FeatureParityExecutor {
    #[must_use]
    pub fn new(
        parity: Arc<dyn FeatureParityRepository>,
        source: Arc<dyn FeatureParityReplaySource>,
        evidence_writer: Arc<dyn FactWriter<QuantFeatureParityEventRow>>,
        incidents: Arc<dyn FeatureParityIncidentPort>,
        metrics: Arc<MetricsHub>,
        minimum_pending_timeout: Duration,
        pending_retry_interval: StdDuration,
    ) -> Self {
        Self {
            parity,
            source,
            evidence_writer,
            incidents,
            metrics,
            minimum_pending_timeout: minimum_pending_timeout.max(Duration::minutes(10)),
            pending_retry_interval,
        }
    }

    async fn execute_inner(
        &self,
        params: &FeatureParityJobParams,
        progress: &Arc<dyn JobProgressSink>,
        cancel: &CancellationToken,
    ) -> QuantResult<FeatureParityRunView> {
        let queued = self
            .parity
            .find_run(&params.parity_run_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found("quant_feature_parity_run", params.parity_run_id)
            })?;
        validate_job_binding(&queued, params)?;
        let pending_timeout = pending_timeout(params, self.minimum_pending_timeout)?;
        if queued.status.is_terminal() {
            return self.resume_terminal(queued).await;
        }
        let run = if queued.status == FeatureParityRunStatus::Running {
            queued
        } else {
            self.parity.mark_running(&params.parity_run_id).await?
        };
        let selected = self.discover_candidates(&run, progress).await?;
        self.replay_until_terminal(run, selected, pending_timeout, progress, cancel)
            .await
    }

    async fn resume_terminal(
        &self,
        run: FeatureParityRunInfo,
    ) -> QuantResult<FeatureParityRunView> {
        match run.status {
            FeatureParityRunStatus::Passed => view(run),
            FeatureParityRunStatus::Mismatched => {
                let contained = self.ensure_containment(&run, &[]).await?;
                view(contained)
            }
            FeatureParityRunStatus::Failed => {
                let contained = self.ensure_containment(&run, &[]).await?;
                let failure_detail = contained.failure_detail.as_deref().ok_or_else(|| {
                    ResearchError::Determinism {
                        detail: format!(
                            "failed feature parity run {} has no failure detail",
                            contained.run_id
                        ),
                    }
                })?;
                Err(ResearchError::Determinism {
                    detail: format!(
                        "feature parity run {} already failed: {failure_detail}",
                        contained.run_id
                    ),
                }
                .into())
            }
            status => Err(ResearchError::Determinism {
                detail: format!(
                    "feature parity run {} reported non-terminal status {} as terminal",
                    run.run_id,
                    status.as_str()
                ),
            }
            .into()),
        }
    }

    async fn discover_candidates(
        &self,
        run: &FeatureParityRunInfo,
        progress: &Arc<dyn JobProgressSink>,
    ) -> QuantResult<Vec<FeatureParityCandidate>> {
        progress.report(ResearchJobProgress::indeterminate("discover_evidence", 0));
        let candidates = match self.source.list_candidates(run).await {
            Ok(candidates) => candidates,
            Err(error) => {
                return self
                    .fail_integrity(run, "source_unavailable", &error.to_string())
                    .await;
            }
        };
        if let Err(error) = validate_candidates(run, &candidates) {
            return self
                .fail_integrity(run, "invalid_candidates", &error.to_string())
                .await;
        }
        let selected = match deterministic_sample(run.kind, candidates) {
            Ok(selected) => selected,
            Err(error) => {
                return self
                    .fail_integrity(run, "invalid_candidates", &error.to_string())
                    .await;
            }
        };
        if selected.is_empty() {
            return self
                .fail_integrity(
                    run,
                    "empty_evidence",
                    "parity window contains no durable serving decisions",
                )
                .await;
        }
        Ok(selected)
    }

    async fn replay_until_terminal(
        &self,
        mut run: FeatureParityRunInfo,
        selected: Vec<FeatureParityCandidate>,
        pending_timeout: Duration,
        progress: &Arc<dyn JobProgressSink>,
        cancel: &CancellationToken,
    ) -> QuantResult<FeatureParityRunView> {
        loop {
            if cancel.is_cancelled() {
                return self
                    .fail_integrity(
                        &run,
                        "cancelled",
                        "feature parity execution was cancelled before a complete comparison",
                    )
                    .await;
            }
            let outcome = self.replay_once(&run, &selected, progress).await?;
            if outcome.completion.status == FeatureParityRunStatus::PendingMaterialization {
                run = self
                    .await_pending_materialization(
                        run,
                        outcome.completion,
                        pending_timeout,
                        progress,
                        cancel,
                    )
                    .await?;
                continue;
            }
            return self.complete_outcome(&run, outcome, progress).await;
        }
    }

    async fn replay_once(
        &self,
        run: &FeatureParityRunInfo,
        selected: &[FeatureParityCandidate],
        progress: &Arc<dyn JobProgressSink>,
    ) -> QuantResult<BuiltOutcome> {
        progress.report(ResearchJobProgress::with_total(
            "replay",
            0,
            count_to_u64(selected.len(), "selected parity candidates")?,
        ));
        let attempt = match self.source.replay(run, selected).await {
            Ok(attempt) => attempt,
            Err(error) => {
                return self
                    .fail_integrity(run, "replay_failed", &error.to_string())
                    .await;
            }
        };
        let outcome = match build_outcome(run, selected, attempt) {
            Ok(outcome) => outcome,
            Err(error) => {
                return self
                    .fail_integrity(run, "invalid_evidence", &error.to_string())
                    .await;
            }
        };
        for row in &outcome.rows {
            self.metrics.record_feature_parity_comparison(
                &row.stage,
                &row.status,
                controlled_metric_reason(row),
            );
        }
        let BuiltOutcome {
            rows,
            completion,
            mismatched_report_ids,
        } = outcome;
        if !rows.is_empty()
            && let Err(error) = self.evidence_writer.write_batch(rows).await
        {
            return self
                .fail_integrity(run, "evidence_write_failed", &error.to_string())
                .await;
        }
        Ok(BuiltOutcome {
            rows: Vec::new(),
            completion,
            mismatched_report_ids,
        })
    }

    async fn await_pending_materialization(
        &self,
        run: FeatureParityRunInfo,
        completion: CompleteFeatureParityRun,
        timeout: Duration,
        progress: &Arc<dyn JobProgressSink>,
        cancel: &CancellationToken,
    ) -> QuantResult<FeatureParityRunInfo> {
        let pending_since = run.pending_since.unwrap_or_else(Utc::now);
        let deadline = pending_since.checked_add_signed(timeout).ok_or_else(|| {
            ResearchError::Determinism {
                detail: format!(
                    "materialization deadline overflows for parity run {}",
                    run.run_id
                ),
            }
        })?;
        if Utc::now() >= deadline {
            return self
                .fail_integrity_with_counts(
                    &run,
                    "materialization_timeout",
                    "serving evidence completion marker did not become durable before the parity deadline",
                    completion,
                )
                .await;
        }
        let pending = self.parity.complete_run(&run.run_id, completion).await?;
        self.metrics
            .record_feature_parity_run(pending.kind.as_str(), pending.status.as_str());
        progress.report(ResearchJobProgress::indeterminate(
            "pending_materialization",
            0,
        ));
        tokio::select! {
            () = cancel.cancelled() => {
                let running = self.parity.mark_running(&pending.run_id).await?;
                return self.fail_integrity(
                    &running,
                    "cancelled",
                    "feature parity execution was cancelled while awaiting source materialization",
                ).await;
            }
            () = tokio::time::sleep(self.pending_retry_interval) => {}
        }
        self.parity
            .mark_running(&pending.run_id)
            .await
            .map_err(QuantError::from)
    }

    async fn complete_outcome(
        &self,
        run: &FeatureParityRunInfo,
        outcome: BuiltOutcome,
        progress: &Arc<dyn JobProgressSink>,
    ) -> QuantResult<FeatureParityRunView> {
        let completed = self
            .parity
            .complete_run(&run.run_id, outcome.completion)
            .await?;
        self.metrics
            .record_feature_parity_run(completed.kind.as_str(), completed.status.as_str());
        let completed = if completed.status == FeatureParityRunStatus::Mismatched {
            self.metrics.set_parity_latch_open(true);
            self.ensure_containment(&completed, &outcome.mismatched_report_ids)
                .await?
        } else {
            completed
        };
        let completed_rows = nonnegative_count(completed.total_count, "completed parity rows")?;
        progress.report(ResearchJobProgress::with_total(
            "finalize",
            completed_rows,
            completed_rows,
        ));
        view(completed)
    }

    async fn fail_integrity<T>(
        &self,
        run: &FeatureParityRunInfo,
        code: &str,
        detail: &str,
    ) -> QuantResult<T> {
        self.fail_integrity_with_counts(
            run,
            code,
            detail,
            CompleteFeatureParityRun {
                status: FeatureParityRunStatus::Failed,
                total_count: 0,
                compared_count: 0,
                matched_count: 0,
                mismatched_count: 0,
                pending_materialization_count: 0,
                feature_contract_hash: run.feature_contract_hash,
                transform_hash: run.transform_hash,
                failure_code: Some(DiagnosticCode::new(code)),
                failure_detail: Some(detail.to_owned()),
            },
        )
        .await
    }

    async fn fail_integrity_with_counts<T>(
        &self,
        run: &FeatureParityRunInfo,
        code: &str,
        detail: &str,
        mut completion: CompleteFeatureParityRun,
    ) -> QuantResult<T> {
        completion.status = FeatureParityRunStatus::Failed;
        completion.failure_code = Some(DiagnosticCode::new(code));
        completion.failure_detail = Some(detail.to_owned());
        let failed = self.parity.complete_run(&run.run_id, completion).await?;
        self.metrics
            .record_feature_parity_run(failed.kind.as_str(), failed.status.as_str());
        self.parity
            .open_latch(
                &failed.run_id,
                FeatureParityStateTransition::IntegrityFailure,
                detail.to_owned(),
            )
            .await?;
        self.metrics.set_parity_latch_open(true);
        if let Err(containment_error) = self.ensure_containment(&failed, &[]).await {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "feature parity {code}: {detail}; containment remains incomplete: {containment_error}"
                ),
            }
            .into());
        }
        Err(ResearchError::Determinism {
            detail: format!("feature parity {code}: {detail}"),
        }
        .into())
    }

    async fn ensure_containment(
        &self,
        run: &FeatureParityRunInfo,
        report_ids: &[RecommendationReportId],
    ) -> QuantResult<FeatureParityRunInfo> {
        if run.containment_completed_at.is_some() {
            return Ok(run.clone());
        }
        self.incidents.contain(run, report_ids).await?;
        self.parity
            .mark_containment_complete(&run.run_id)
            .await
            .map_err(QuantError::from)
    }
}

fn controlled_metric_reason(row: &QuantFeatureParityEventRow) -> &'static str {
    match row.status.as_str() {
        "matched" => "matched",
        "mismatched" => "audit_fingerprint_mismatch",
        "pending_materialization" => match row.reason.as_deref() {
            Some("serving_evidence_completion_missing") => "completion_missing",
            Some("model_input_completion_pending") => "model_input_pending",
            Some("feature_evidence_completion_pending") => "feature_evidence_pending",
            _ => "materialization_pending_other",
        },
        _ => "invalid_status",
    }
}

fn count_to_u64(count: usize, field: &'static str) -> QuantResult<u64> {
    u64::try_from(count).map_err(|error| {
        ResearchError::Determinism {
            detail: format!("{field} count does not fit u64: {error}"),
        }
        .into()
    })
}

fn nonnegative_count(count: i64, field: &'static str) -> QuantResult<u64> {
    u64::try_from(count).map_err(|error| {
        ResearchError::Determinism {
            detail: format!("{field} must be non-negative and fit u64: {error}"),
        }
        .into()
    })
}

fn pending_timeout(params: &FeatureParityJobParams, minimum: Duration) -> QuantResult<Duration> {
    let seconds = i64::try_from(params.materialization_timeout_secs).map_err(|error| {
        ResearchError::Determinism {
            detail: format!("materialization timeout does not fit i64 seconds: {error}"),
        }
    })?;
    if seconds <= 0 {
        return Err(ResearchError::Determinism {
            detail: "materialization timeout must be positive".to_owned(),
        }
        .into());
    }
    Ok(Duration::seconds(seconds).max(minimum))
}

#[async_trait]
impl FeatureParityExecutionPort for FeatureParityExecutor {
    async fn execute(
        &self,
        params: FeatureParityJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeatureParityRunView> {
        self.execute_inner(&params, &progress, &cancel).await
    }
}

fn validate_job_binding(
    run: &FeatureParityRunInfo,
    params: &FeatureParityJobParams,
) -> QuantResult<()> {
    let request_start = params.request.window_start.ok_or_else(|| {
        QuantError::from(ResearchError::Determinism {
            detail: "persisted feature-parity job is missing window_start".to_owned(),
        })
    })?;
    let request_end = params.request.window_end.ok_or_else(|| {
        QuantError::from(ResearchError::Determinism {
            detail: "persisted feature-parity job is missing window_end".to_owned(),
        })
    })?;
    if run.window_start != request_start
        || run.window_end != request_end
        || run.reason != params.request.reason
        || run.feature_contract_hash.is_none()
    {
        return Err(ResearchError::Determinism {
            detail: format!("feature-parity job/run binding mismatch for {}", run.run_id),
        }
        .into());
    }
    Ok(())
}

fn validate_candidates(
    run: &FeatureParityRunInfo,
    candidates: &[FeatureParityCandidate],
) -> QuantResult<()> {
    if let Some(invalid) = candidates.iter().find(|candidate| {
        matches!(&candidate.subject, FeatureParitySubject::ModelRun(_))
            && candidate.market_id.is_none()
    }) {
        return Err(ResearchError::Determinism {
            detail: format!(
                "model-run parity candidate `{}` has no real market binding",
                invalid.sampling_key
            ),
        }
        .into());
    }
    if let Some(outside) = candidates.iter().find(|candidate| {
        candidate.decision_at < run.window_start || candidate.decision_at >= run.window_end
    }) {
        return Err(ResearchError::Determinism {
            detail: format!(
                "feature-parity candidate `{}` decision_at {} is outside [{}, {})",
                outside.sampling_key, outside.decision_at, run.window_start, run.window_end
            ),
        }
        .into());
    }
    Ok(())
}

fn deterministic_sample(
    kind: FeatureParityRunKind,
    mut candidates: Vec<FeatureParityCandidate>,
) -> QuantResult<Vec<FeatureParityCandidate>> {
    let mut unique = BTreeSet::new();
    for candidate in &candidates {
        if candidate.sampling_key.trim().is_empty() || !unique.insert(&candidate.sampling_key) {
            return Err(ResearchError::Determinism {
                detail: "feature-parity candidates require unique, non-empty sampling keys"
                    .to_owned(),
            }
            .into());
        }
    }
    if kind == FeatureParityRunKind::Full {
        candidates.sort_by(|left, right| left.sampling_key.cmp(&right.sampling_key));
        return Ok(candidates);
    }
    let fraction = candidates
        .len()
        .checked_mul(SAMPLE_FRACTION_NUMERATOR)
        .ok_or_else(|| ResearchError::Determinism {
            detail: "feature-parity sample-size multiplication overflow".to_owned(),
        })?
        .div_ceil(SAMPLE_FRACTION_DENOMINATOR);
    let sample_size = candidates.len().min(fraction.max(SAMPLE_MINIMUM));
    candidates.sort_by(|left, right| {
        sampling_digest(&left.sampling_key)
            .cmp(&sampling_digest(&right.sampling_key))
            .then_with(|| left.sampling_key.cmp(&right.sampling_key))
    });
    candidates.truncate(sample_size);
    Ok(candidates)
}

fn sampling_digest(key: &str) -> [u8; 32] {
    Sha256::digest(key.as_bytes()).into()
}

struct BuiltOutcome {
    rows: Vec<QuantFeatureParityEventRow>,
    completion: CompleteFeatureParityRun,
    mismatched_report_ids: Vec<RecommendationReportId>,
}

struct OutcomeEvidence {
    rows: Vec<QuantFeatureParityEventRow>,
    seen: BTreeSet<String>,
    evidence_keys: BTreeSet<String>,
    transform_hashes: BTreeSet<String>,
    mismatched_report_ids: Vec<RecommendationReportId>,
    mismatched_report_keys: BTreeSet<String>,
    matched: i64,
    mismatched: i64,
}

impl OutcomeEvidence {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
            seen: BTreeSet::new(),
            evidence_keys: BTreeSet::new(),
            transform_hashes: BTreeSet::new(),
            mismatched_report_ids: Vec::new(),
            mismatched_report_keys: BTreeSet::new(),
            matched: 0,
            mismatched: 0,
        }
    }

    fn push_comparison(
        &mut self,
        run: &FeatureParityRunInfo,
        feature_contract_hash: &ContentHash,
        expected: &BTreeMap<String, DateTime<Utc>>,
        comparison: FeatureParityComparison,
        ingestion_time: i64,
    ) -> QuantResult<()> {
        validate_replayed_key(expected, &comparison.sampling_key, comparison.decision_at)?;
        let evidence_key = format!(
            "{}/{}/{}/{}",
            comparison.sampling_key,
            comparison.stage.as_str(),
            comparison.feature_name.as_deref().unwrap_or(""),
            comparison
                .market_id
                .as_ref()
                .map_or_else(String::new, ToString::to_string)
        );
        if !self.evidence_keys.insert(evidence_key) {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "duplicate parity evidence for candidate `{}` stage `{}` feature `{}`",
                    comparison.sampling_key,
                    comparison.stage.as_str(),
                    comparison.feature_name.as_deref().unwrap_or("")
                ),
            }
            .into());
        }
        self.seen.insert(comparison.sampling_key.clone());
        comparison.online.validate_evidence()?;
        comparison.replay.validate_evidence()?;
        let matched = comparison.online == comparison.replay;
        if matched {
            self.matched += 1;
        } else {
            self.mismatched += 1;
            if let Some(report_id) = comparison.report_id.as_ref()
                && self.mismatched_report_keys.insert(report_id.to_string())
            {
                self.mismatched_report_ids.push(*report_id);
            }
        }
        if let Some(hash) = comparison.transform_hash.as_ref() {
            self.transform_hashes.insert(hash.to_string());
        }
        let status = if matched {
            FeatureParityEventStatus::Matched
        } else {
            FeatureParityEventStatus::Mismatched
        };
        self.rows.push(comparison_row(
            run,
            feature_contract_hash,
            comparison,
            status,
            ingestion_time,
        )?);
        Ok(())
    }

    fn push_pending(
        &mut self,
        run: &FeatureParityRunInfo,
        feature_contract_hash: &ContentHash,
        expected: &BTreeMap<String, DateTime<Utc>>,
        pending: PendingFeatureParityComparison,
        ingestion_time: i64,
    ) -> QuantResult<()> {
        validate_replayed_key(expected, &pending.sampling_key, pending.decision_at)?;
        self.seen.insert(pending.sampling_key.clone());
        if let Some(online) = pending.online.as_ref() {
            (online).validate_evidence()?;
        }
        self.rows.push(pending_row(
            run,
            feature_contract_hash,
            pending,
            ingestion_time,
        )?);
        Ok(())
    }

    fn finish(
        mut self,
        expected: &BTreeMap<String, DateTime<Utc>>,
        feature_contract_hash: ContentHash,
    ) -> QuantResult<BuiltOutcome> {
        if let Some(missing) = expected.keys().find(|key| !self.seen.contains(*key)) {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "replay source omitted selected candidate `{missing}` without pending evidence"
                ),
            }
            .into());
        }
        let pending = i64::try_from(
            self.rows
                .iter()
                .filter(|row| {
                    row.status == FeatureParityEventStatus::PendingMaterialization.as_str()
                })
                .count(),
        )
        .map_err(|error| ResearchError::Determinism {
            detail: format!("pending parity count overflow: {error}"),
        })?;
        let compared = self.matched + self.mismatched;
        let total = compared + pending;
        if total == 0 {
            return Err(ResearchError::Determinism {
                detail: "replay source returned no comparisons for non-empty candidates".to_owned(),
            }
            .into());
        }
        let status = if self.mismatched > 0 {
            FeatureParityRunStatus::Mismatched
        } else if pending > 0 {
            FeatureParityRunStatus::PendingMaterialization
        } else {
            FeatureParityRunStatus::Passed
        };
        let transform_hash = if self.transform_hashes.is_empty() {
            None
        } else {
            Some(ResearchHasher::canonical(&self.transform_hashes)?)
        };
        self.mismatched_report_ids.sort_by_key(ToString::to_string);
        Ok(BuiltOutcome {
            rows: self.rows,
            mismatched_report_ids: self.mismatched_report_ids,
            completion: CompleteFeatureParityRun {
                status,
                total_count: total,
                compared_count: compared,
                matched_count: self.matched,
                mismatched_count: self.mismatched,
                pending_materialization_count: pending,
                feature_contract_hash: Some(feature_contract_hash),
                transform_hash,
                failure_code: None,
                failure_detail: None,
            },
        })
    }
}

fn build_outcome(
    run: &FeatureParityRunInfo,
    candidates: &[FeatureParityCandidate],
    attempt: FeatureParityReplayAttempt,
) -> QuantResult<BuiltOutcome> {
    let feature_contract_hash = run.feature_contract_hash.ok_or_else(|| {
        QuantError::from(ResearchError::Determinism {
            detail: "parity run has no feature contract hash".to_owned(),
        })
    })?;
    let expected: BTreeMap<_, _> = candidates
        .iter()
        .map(|candidate| (candidate.sampling_key.clone(), candidate.decision_at))
        .collect();
    let ingestion_time = Utc::now().timestamp_millis();
    let mut evidence =
        OutcomeEvidence::with_capacity(attempt.comparisons.len() + attempt.pending.len());
    for comparison in attempt.comparisons {
        evidence.push_comparison(
            run,
            &feature_contract_hash,
            &expected,
            comparison,
            ingestion_time,
        )?;
    }
    for pending in attempt.pending {
        evidence.push_pending(
            run,
            &feature_contract_hash,
            &expected,
            pending,
            ingestion_time,
        )?;
    }
    evidence.finish(&expected, feature_contract_hash)
}

fn validate_replayed_key(
    expected: &BTreeMap<String, DateTime<Utc>>,
    sampling_key: &str,
    decision_at: DateTime<Utc>,
) -> QuantResult<()> {
    let expected_decision_at = expected.get(sampling_key).ok_or_else(|| {
        QuantError::from(ResearchError::Determinism {
            detail: format!("replay source returned unselected candidate `{sampling_key}`"),
        })
    })?;
    if *expected_decision_at != decision_at {
        return Err(ResearchError::Determinism {
            detail: format!(
                "replay source changed decision_at for candidate `{sampling_key}`: expected {expected_decision_at}, got {decision_at}"
            ),
        }
        .into());
    }
    Ok(())
}

impl FeatureParityEvidence {
    fn validate_evidence(&self) -> QuantResult<()> {
        if self.fingerprint.trim().is_empty() {
            return Err(ResearchError::Determinism {
                detail: "parity evidence fingerprint must not be empty".to_owned(),
            }
            .into());
        }
        Ok(())
    }
}

fn stable_parity_event_id(
    run: &FeatureParityRunInfo,
    sampling_key: &str,
    stage: FeatureParityStage,
    status: FeatureParityEventStatus,
    market_id: Option<&MarketId>,
    feature_name: Option<&str>,
    reason: Option<&str>,
) -> QuantResult<FeatureParityEventId> {
    #[derive(Serialize)]
    struct Identity<'a> {
        parity_run_id: String,
        sampling_key: &'a str,
        stage: &'a str,
        status: &'a str,
        market_id: Option<&'a str>,
        feature_name: Option<&'a str>,
        reason: Option<&'a str>,
    }

    let hash = ResearchHasher::canonical(&Identity {
        parity_run_id: run.run_id.to_string(),
        sampling_key,
        stage: stage.as_str(),
        status: status.as_str(),
        market_id: market_id.map(MarketId::as_str),
        feature_name,
        reason,
    })?;
    Ok(FeatureParityEventId::from_evidence_hash(&hash))
}

fn comparison_row(
    run: &FeatureParityRunInfo,
    feature_contract_hash: &ContentHash,
    comparison: FeatureParityComparison,
    status: FeatureParityEventStatus,
    ingestion_time: i64,
) -> QuantResult<QuantFeatureParityEventRow> {
    let detail = FeatureParityDetail::Compared {
        sampling_key: comparison.sampling_key.clone(),
        source: Box::new(comparison.detail),
    };
    detail
        .validate_for(comparison.stage, status)
        .map_err(|detail| ResearchError::Determinism {
            detail: detail.to_owned(),
        })?;
    let detail_json = canonical_detail(&detail)?;
    let parity_event_id = stable_parity_event_id(
        run,
        &comparison.sampling_key,
        comparison.stage,
        status,
        comparison.market_id.as_ref(),
        comparison.feature_name.as_deref(),
        comparison.reason.as_deref(),
    )?;
    Ok(QuantFeatureParityEventRow {
        event_time: comparison.decision_at.timestamp_millis(),
        parity_event_id,
        parity_run_id: run.run_id,
        decision_at: comparison.decision_at.timestamp_millis(),
        stage: comparison.stage.to_string(),
        status: status.to_string(),
        report_id: comparison.report_id,
        model_run_id: comparison.model_run_id,
        model_version_id: comparison.model_version_id,
        training_dataset_id: comparison.training_dataset_id,
        market_id: comparison.market_id,
        feature_name: comparison.feature_name,
        reason: comparison.reason.or_else(|| {
            (status == FeatureParityEventStatus::Mismatched)
                .then(|| "audit_fingerprint_mismatch".to_owned())
        }),
        online_state: comparison.online.state.map(|state| state.to_string()),
        replay_state: comparison.replay.state.map(|state| state.to_string()),
        online_value: comparison.online.value,
        replay_value: comparison.replay.value,
        online_effective_at: comparison
            .online
            .effective_at
            .map(|value| value.timestamp_millis()),
        online_available_at: comparison
            .online
            .available_at
            .map(|value| value.timestamp_millis()),
        online_cutoff: comparison
            .online
            .cutoff
            .map(|value| value.timestamp_millis()),
        replay_effective_at: comparison
            .replay
            .effective_at
            .map(|value| value.timestamp_millis()),
        replay_available_at: comparison
            .replay
            .available_at
            .map(|value| value.timestamp_millis()),
        replay_cutoff: comparison
            .replay
            .cutoff
            .map(|value| value.timestamp_millis()),
        feature_contract_hash: feature_contract_hash.to_string(),
        transform_hash: comparison
            .transform_hash
            .map_or_else(String::new, |hash| hash.to_string()),
        online_fingerprint: comparison.online.fingerprint,
        replay_fingerprint: comparison.replay.fingerprint,
        detail_json,
        ingestion_time,
    })
}

fn pending_row(
    run: &FeatureParityRunInfo,
    feature_contract_hash: &ContentHash,
    pending: PendingFeatureParityComparison,
    ingestion_time: i64,
) -> QuantResult<QuantFeatureParityEventRow> {
    let detail = FeatureParityDetail::PendingMaterialization {
        sampling_key: pending.sampling_key.clone(),
        required_watermark: pending.required_watermark,
        observed_watermark: pending.observed_watermark,
    };
    detail
        .validate_for(
            pending.stage,
            FeatureParityEventStatus::PendingMaterialization,
        )
        .map_err(|detail| ResearchError::Determinism {
            detail: detail.to_owned(),
        })?;
    let status = FeatureParityEventStatus::PendingMaterialization;
    let parity_event_id = stable_parity_event_id(
        run,
        &pending.sampling_key,
        pending.stage,
        status,
        pending.market_id.as_ref(),
        pending.feature_name.as_deref(),
        Some(&pending.reason),
    )?;
    Ok(QuantFeatureParityEventRow {
        event_time: pending.decision_at.timestamp_millis(),
        parity_event_id,
        parity_run_id: run.run_id,
        decision_at: pending.decision_at.timestamp_millis(),
        stage: pending.stage.to_string(),
        status: status.to_string(),
        report_id: pending.report_id,
        model_run_id: pending.model_run_id,
        model_version_id: pending.model_version_id,
        training_dataset_id: pending.training_dataset_id,
        market_id: pending.market_id,
        feature_name: pending.feature_name,
        reason: Some(pending.reason),
        online_state: pending
            .online
            .as_ref()
            .and_then(|online| online.state)
            .map(|state| state.to_string()),
        replay_state: None,
        online_value: pending
            .online
            .as_ref()
            .and_then(|online| online.value.clone()),
        replay_value: None,
        online_effective_at: pending
            .online
            .as_ref()
            .and_then(|online| online.effective_at)
            .map(|value| value.timestamp_millis()),
        online_available_at: pending
            .online
            .as_ref()
            .and_then(|online| online.available_at)
            .map(|value| value.timestamp_millis()),
        online_cutoff: pending
            .online
            .as_ref()
            .and_then(|online| online.cutoff)
            .map(|value| value.timestamp_millis()),
        replay_effective_at: None,
        replay_available_at: pending
            .observed_watermark
            .map(|value| value.timestamp_millis()),
        replay_cutoff: Some(pending.required_watermark.timestamp_millis()),
        feature_contract_hash: feature_contract_hash.to_string(),
        transform_hash: String::new(),
        online_fingerprint: pending
            .online
            .map_or_else(String::new, |online| online.fingerprint),
        replay_fingerprint: "pending_materialization".to_owned(),
        detail_json: canonical_detail(&detail)?,
        ingestion_time,
    })
}

fn canonical_detail<T: Serialize + ?Sized>(value: &T) -> QuantResult<String> {
    serde_json::to_string(value).map_err(|error| {
        ResearchError::Serialization {
            detail: format!("parity evidence serialization failed: {error}"),
        }
        .into()
    })
}

fn view(info: FeatureParityRunInfo) -> QuantResult<FeatureParityRunView> {
    FeatureParityRunView::try_from_info(info).map_err(|detail| {
        ResearchError::Determinism {
            detail: detail.to_owned(),
        }
        .into()
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        slice,
        sync::{Mutex, MutexGuard},
    };

    use quant_pivot_models::{
        domain::{
            api::{FeatureParityRunListQuery, RunFullFeatureParityRequest},
            pagination::Paginated,
            quant::{
                FeatureParityStateInfo, FrozenFeatureParitySubject, NewFeatureParityRun,
                NewFrozenModelParitySubject, NewResearchJob, NoopProgressSink, ResearchJobInfo,
            },
        },
        enums::quant::FeatureParityLatchState,
        types::{FeatureParityRunId, FeatureParityStateId, FeatureVectorId, RoleCode},
    };
    use quant_pivot_repository::traits::{
        EnqueueFrozenFeatureParityOutcome, FeatureParityLatchActor,
    };

    use super::*;

    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex.lock().expect("test mutex")
    }

    fn unexpected_repo_call(operation: &str) -> StorageError {
        StorageError::invariant_violation(
            Some("test_feature_parity"),
            format!("unexpected repository call: {operation}"),
        )
    }

    #[derive(Default)]
    struct ParityRepoState {
        run: Option<FeatureParityRunInfo>,
        mismatch_latch_opens: usize,
        explicit_latch_opens: Vec<FeatureParityStateTransition>,
        containment_marks: usize,
    }

    #[derive(Default)]
    struct InMemoryParityRepository {
        state: Mutex<ParityRepoState>,
    }

    impl InMemoryParityRepository {
        fn with_run(run: FeatureParityRunInfo) -> Self {
            Self {
                state: Mutex::new(ParityRepoState {
                    run: Some(run),
                    ..ParityRepoState::default()
                }),
            }
        }

        fn run(&self) -> FeatureParityRunInfo {
            lock(&self.state).run.clone().expect("test parity run")
        }
    }

    #[async_trait]
    impl FeatureParityRepository for InMemoryParityRepository {
        async fn create_run(
            &self,
            _run: NewFeatureParityRun,
        ) -> Result<FeatureParityRunInfo, StorageError> {
            Err(unexpected_repo_call("create_run"))
        }

        async fn create_frozen_model_run(
            &self,
            _run: NewFeatureParityRun,
            _subject: NewFrozenModelParitySubject,
        ) -> Result<FeatureParityRunInfo, StorageError> {
            Err(unexpected_repo_call("create_frozen_model_run"))
        }

        async fn enqueue_run(
            &self,
            _run: NewFeatureParityRun,
            _job: NewResearchJob,
        ) -> Result<(FeatureParityRunInfo, ResearchJobInfo), StorageError> {
            Err(unexpected_repo_call("enqueue_run"))
        }

        async fn enqueue_frozen_full(
            &self,
            _run: NewFeatureParityRun,
            _job: NewResearchJob,
        ) -> Result<EnqueueFrozenFeatureParityOutcome, StorageError> {
            Err(unexpected_repo_call("enqueue_frozen_full"))
        }

        async fn load_frozen_subjects(
            &self,
            _run_id: &FeatureParityRunId,
        ) -> Result<Vec<FrozenFeatureParitySubject>, StorageError> {
            Err(unexpected_repo_call("load_frozen_subjects"))
        }

        async fn find_run(
            &self,
            run_id: &FeatureParityRunId,
        ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
            Ok(lock(&self.state)
                .run
                .clone()
                .filter(|run| &run.run_id == run_id))
        }

        async fn page_runs(
            &self,
            _query: FeatureParityRunListQuery,
        ) -> Result<Paginated<FeatureParityRunInfo>, StorageError> {
            Err(unexpected_repo_call("page_runs"))
        }

        async fn latest_run(
            &self,
            _kind: FeatureParityRunKind,
        ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
            Err(unexpected_repo_call("latest_run"))
        }

        async fn find_full_window(
            &self,
            _window_start: DateTime<Utc>,
            _window_end: DateTime<Utc>,
        ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
            Err(unexpected_repo_call("find_full_window"))
        }

        async fn find_sampled_report(
            &self,
            _report_id: &RecommendationReportId,
        ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
            Err(unexpected_repo_call("find_sampled_report"))
        }

        async fn latest_full_for_model(
            &self,
            _model_version_id: &ModelVersionId,
            _training_dataset_id: &TrainingDatasetId,
        ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
            Err(unexpected_repo_call("latest_full_for_model"))
        }

        async fn mark_running(
            &self,
            run_id: &FeatureParityRunId,
        ) -> Result<FeatureParityRunInfo, StorageError> {
            let updated = {
                let mut state = lock(&self.state);
                let run = state
                    .run
                    .as_mut()
                    .filter(|run| &run.run_id == run_id)
                    .ok_or_else(|| unexpected_repo_call("mark_running unknown run"))?;
                run.status = FeatureParityRunStatus::Running;
                run.started_at.get_or_insert_with(Utc::now);
                run.updated_at = Utc::now();
                let updated = run.clone();
                drop(state);
                updated
            };
            Ok(updated)
        }

        async fn complete_run(
            &self,
            run_id: &FeatureParityRunId,
            result: CompleteFeatureParityRun,
        ) -> Result<FeatureParityRunInfo, StorageError> {
            let updated = {
                let mut state = lock(&self.state);
                if result.status == FeatureParityRunStatus::Mismatched {
                    state.mismatch_latch_opens += 1;
                }
                let run = state
                    .run
                    .as_mut()
                    .filter(|run| &run.run_id == run_id)
                    .ok_or_else(|| unexpected_repo_call("complete_run unknown run"))?;
                run.status = result.status;
                run.total_count = result.total_count;
                run.compared_count = result.compared_count;
                run.matched_count = result.matched_count;
                run.mismatched_count = result.mismatched_count;
                run.pending_materialization_count = result.pending_materialization_count;
                run.feature_contract_hash = result.feature_contract_hash;
                run.transform_hash = result.transform_hash;
                run.failure_code = result.failure_code;
                run.failure_detail = result.failure_detail;
                if result.status == FeatureParityRunStatus::PendingMaterialization {
                    run.pending_since.get_or_insert_with(Utc::now);
                }
                if result.status.is_terminal() {
                    run.finished_at = Some(Utc::now());
                }
                run.updated_at = Utc::now();
                let updated = run.clone();
                drop(state);
                updated
            };
            Ok(updated)
        }

        async fn mark_containment_complete(
            &self,
            run_id: &FeatureParityRunId,
        ) -> Result<FeatureParityRunInfo, StorageError> {
            let updated = {
                let mut state = lock(&self.state);
                state.containment_marks += 1;
                let run = state
                    .run
                    .as_mut()
                    .filter(|run| &run.run_id == run_id)
                    .ok_or_else(|| unexpected_repo_call("containment unknown run"))?;
                run.containment_completed_at.get_or_insert_with(Utc::now);
                run.updated_at = Utc::now();
                let updated = run.clone();
                drop(state);
                updated
            };
            Ok(updated)
        }

        async fn current_state(&self) -> Result<Option<FeatureParityStateInfo>, StorageError> {
            Ok(None)
        }

        async fn open_latch(
            &self,
            cause_run_id: &FeatureParityRunId,
            transition: FeatureParityStateTransition,
            reason: String,
        ) -> Result<FeatureParityStateInfo, StorageError> {
            lock(&self.state).explicit_latch_opens.push(transition);
            Ok(FeatureParityStateInfo {
                state_id: FeatureParityStateId::from_v7(),
                state: FeatureParityLatchState::Open,
                transition,
                cause_run_id: Some(*cause_run_id),
                recovery_run_id: None,
                previous_state_id: None,
                actor: None,
                acting_role: None,
                reason,
                created_at: Utc::now(),
            })
        }

        async fn acknowledge_latch(
            &self,
            _recovery_run_id: &FeatureParityRunId,
            _actor: FeatureParityLatchActor,
        ) -> Result<FeatureParityStateInfo, StorageError> {
            Err(unexpected_repo_call("acknowledge_latch"))
        }
    }

    struct FixedReplaySource {
        candidates: Vec<FeatureParityCandidate>,
        attempts: Mutex<VecDeque<FeatureParityReplayAttempt>>,
    }

    #[async_trait]
    impl FeatureParityReplaySource for FixedReplaySource {
        async fn list_candidates(
            &self,
            _run: &FeatureParityRunInfo,
        ) -> QuantResult<Vec<FeatureParityCandidate>> {
            Ok(self.candidates.clone())
        }

        async fn replay(
            &self,
            _run: &FeatureParityRunInfo,
            _candidates: &[FeatureParityCandidate],
        ) -> QuantResult<FeatureParityReplayAttempt> {
            lock(&self.attempts).pop_front().ok_or_else(|| {
                ResearchError::Determinism {
                    detail: "test replay attempt queue exhausted".to_owned(),
                }
                .into()
            })
        }
    }

    #[derive(Default)]
    struct RecordingFactWriter {
        rows: Mutex<Vec<QuantFeatureParityEventRow>>,
    }

    #[async_trait]
    impl FactWriter<QuantFeatureParityEventRow> for RecordingFactWriter {
        async fn write_batch(
            &self,
            rows: Vec<QuantFeatureParityEventRow>,
        ) -> Result<(), StorageError> {
            lock(&self.rows).extend(rows);
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingIncidentPort {
        calls: Mutex<Vec<(FeatureParityRunStatus, Vec<RecommendationReportId>)>>,
    }

    #[async_trait]
    impl FeatureParityIncidentPort for RecordingIncidentPort {
        async fn contain(
            &self,
            run: &FeatureParityRunInfo,
            report_ids: &[RecommendationReportId],
        ) -> QuantResult<()> {
            lock(&self.calls).push((run.status, report_ids.to_vec()));
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingReportContainment {
        calls: Mutex<Vec<RecommendationReportId>>,
        failures: Mutex<BTreeSet<String>>,
    }

    #[async_trait]
    impl ReportContainmentPort for RecordingReportContainment {
        async fn revoke_and_cascade(
            &self,
            report_id: &RecommendationReportId,
            _reason: &str,
            _revoked_at: DateTime<Utc>,
        ) -> QuantResult<()> {
            lock(&self.calls).push(*report_id);
            if lock(&self.failures).contains(&report_id.to_string()) {
                return Err(ResearchError::Determinism {
                    detail: format!("test revoke/cascade failed for {report_id}"),
                }
                .into());
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingAffectedReportLookup {
        calls: Mutex<usize>,
        ids: Vec<RecommendationReportId>,
    }

    #[async_trait]
    impl AffectedReportLookup for RecordingAffectedReportLookup {
        async fn find_actionable_ids_between(
            &self,
            _window_start: DateTime<Utc>,
            _window_end: DateTime<Utc>,
        ) -> QuantResult<Vec<RecommendationReportId>> {
            *lock(&self.calls) += 1;
            Ok(self.ids.clone())
        }
    }

    fn candidate(index: usize) -> FeatureParityCandidate {
        FeatureParityCandidate {
            sampling_key: format!("report-a/market-{index:03}"),
            subject: FeatureParitySubject::ModelRun(ModelRunId::from_v7()),
            market_id: Some(MarketId::new(format!("market-{index:03}"))),
            decision_at: Utc::now(),
        }
    }

    fn run(now: DateTime<Utc>) -> FeatureParityRunInfo {
        FeatureParityRunInfo {
            run_id: FeatureParityRunId::from_v7(),
            kind: FeatureParityRunKind::Full,
            status: FeatureParityRunStatus::Running,
            window_start: now - Duration::minutes(1),
            window_end: now + Duration::minutes(1),
            report_id: None,
            model_version_id: None,
            training_dataset_id: None,
            triggered_by: "test".to_owned(),
            requested_by: Some("test".to_owned()),
            acting_role: RoleCode::new("risk_owner"),
            reason: "test".to_owned(),
            total_count: 0,
            compared_count: 0,
            matched_count: 0,
            mismatched_count: 0,
            pending_materialization_count: 0,
            feature_contract_hash: Some(
                ContentHash::parse(&format!("blake3:{}", "1".repeat(64))).expect("hash"),
            ),
            transform_hash: None,
            failure_code: None,
            failure_detail: None,
            started_at: Some(now),
            pending_since: None,
            containment_completed_at: None,
            finished_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn evidence(fingerprint: &str, cutoff: DateTime<Utc>) -> FeatureParityEvidence {
        FeatureParityEvidence {
            state: Some(FeatureCellState::Observed),
            value: Some("0.42".to_owned()),
            effective_at: Some(cutoff - Duration::seconds(1)),
            available_at: Some(cutoff),
            cutoff: Some(cutoff),
            fingerprint: fingerprint.to_owned(),
        }
    }

    impl FeatureParityComparison {
        fn fixture(
            now: DateTime<Utc>,
            online: FeatureParityEvidence,
            replay: FeatureParityEvidence,
        ) -> Self {
            Self {
                sampling_key: "report-a/market-001".to_owned(),
                decision_at: now,
                stage: FeatureParityStage::FeatureCell,
                report_id: None,
                model_run_id: None,
                model_version_id: None,
                training_dataset_id: None,
                market_id: None,
                feature_name: Some("book.spread_bps".to_owned()),
                reason: None,
                online,
                replay,
                transform_hash: None,
                detail: FeatureParityDetailSource::FeatureCell {
                    feature_vector_id: FeatureVectorId::from_v7(),
                },
            }
        }
    }

    fn params(run: &FeatureParityRunInfo) -> FeatureParityJobParams {
        FeatureParityJobParams {
            parity_run_id: run.run_id,
            materialization_timeout_secs: 600,
            request: RunFullFeatureParityRequest {
                window_start: Some(run.window_start),
                window_end: Some(run.window_end),
                reason: run.reason.clone(),
            },
        }
    }

    fn executor(
        parity: Arc<InMemoryParityRepository>,
        source: Arc<FixedReplaySource>,
        writer: Arc<RecordingFactWriter>,
        incidents: Arc<RecordingIncidentPort>,
    ) -> FeatureParityExecutor {
        FeatureParityExecutor::new(
            parity,
            source,
            writer,
            incidents,
            Arc::new(MetricsHub::new()),
            Duration::minutes(10),
            StdDuration::ZERO,
        )
    }

    #[test]
    fn sampled_run_uses_twenty() {
        let small = deterministic_sample(
            FeatureParityRunKind::Sampled,
            (0..12).map(candidate).collect(),
        )
        .expect("small sample");
        assert_eq!(small.len(), 12);

        let medium = deterministic_sample(
            FeatureParityRunKind::Sampled,
            (0..100).map(candidate).collect(),
        )
        .expect("medium sample");
        assert_eq!(medium.len(), 20);

        let large = deterministic_sample(
            FeatureParityRunKind::Sampled,
            (0..1_000).map(candidate).collect(),
        )
        .expect("large sample");
        assert_eq!(large.len(), 100);
    }

    #[test]
    fn sampling_stable_across_order() {
        let forward: Vec<_> = (0..100).map(candidate).collect();
        let mut reverse = forward.clone();
        reverse.reverse();
        let selected_forward = deterministic_sample(FeatureParityRunKind::Sampled, forward)
            .expect("forward sample")
            .into_iter()
            .map(|item| item.sampling_key)
            .collect::<Vec<_>>();
        let selected_reverse = deterministic_sample(FeatureParityRunKind::Sampled, reverse)
            .expect("reverse sample")
            .into_iter()
            .map(|item| item.sampling_key)
            .collect::<Vec<_>>();
        assert_eq!(selected_forward, selected_reverse);
    }

    #[test]
    fn parity_event_identity_sensitive() {
        let now = Utc::now();
        let run = run(now);
        let contract_hash = run
            .feature_contract_hash
            .as_ref()
            .expect("feature contract hash");
        let compared =
            FeatureParityComparison::fixture(now, evidence("same", now), evidence("same", now));
        let first = comparison_row(
            &run,
            contract_hash,
            compared.clone(),
            FeatureParityEventStatus::Matched,
            now.timestamp_millis(),
        )
        .expect("first row");
        let retry = comparison_row(
            &run,
            contract_hash,
            compared.clone(),
            FeatureParityEventStatus::Matched,
            (now + Duration::seconds(1)).timestamp_millis(),
        )
        .expect("retry row");
        let mismatch = comparison_row(
            &run,
            contract_hash,
            compared,
            FeatureParityEventStatus::Mismatched,
            now.timestamp_millis(),
        )
        .expect("mismatch row");

        assert_eq!(first.parity_event_id, retry.parity_event_id);
        assert_ne!(first.parity_event_id, mismatch.parity_event_id);
    }

    #[test]
    fn duplicate_sampling_keys_rejects() {
        let duplicate = candidate(1);
        let error = deterministic_sample(
            FeatureParityRunKind::Sampled,
            vec![duplicate.clone(), duplicate],
        )
        .expect_err("duplicates must fail");
        assert!(error.to_string().contains("unique"));
    }

    #[test]
    fn empty_uses_without_market() {
        let now = Utc::now();
        let run = run(now);
        let report_candidate = FeatureParityCandidate {
            sampling_key: "report/empty/selection".to_owned(),
            subject: FeatureParitySubject::PreInferenceReport(RecommendationReportId::from_v7()),
            market_id: None,
            decision_at: now,
        };
        validate_candidates(&run, slice::from_ref(&report_candidate))
            .expect("report-level empty selection is real evidence");

        let model_candidate = FeatureParityCandidate {
            sampling_key: "run/empty".to_owned(),
            subject: FeatureParitySubject::ModelRun(ModelRunId::from_v7()),
            market_id: None,
            decision_at: now,
        };
        assert!(validate_candidates(&run, &[model_candidate]).is_err());
    }

    #[test]
    fn exact_evidence_passes_mismatches() {
        let now = Utc::now();
        let run = run(now);
        let candidates = vec![FeatureParityCandidate {
            sampling_key: "report-a/market-001".to_owned(),
            subject: FeatureParitySubject::ModelRun(ModelRunId::from_v7()),
            market_id: Some(MarketId::new("market-001")),
            decision_at: now,
        }];
        let online = evidence("fingerprint-a", now - Duration::seconds(2));
        let passed = build_outcome(
            &run,
            &candidates,
            FeatureParityReplayAttempt {
                comparisons: vec![FeatureParityComparison::fixture(
                    now,
                    online.clone(),
                    online.clone(),
                )],
                pending: Vec::new(),
            },
        )
        .expect("exact evidence");
        assert_eq!(passed.completion.status, FeatureParityRunStatus::Passed);
        assert_eq!(passed.completion.matched_count, 1);

        let mut replay = online.clone();
        replay.cutoff = replay.cutoff.map(|cutoff| cutoff - Duration::seconds(1));
        let mismatched = build_outcome(
            &run,
            &candidates,
            FeatureParityReplayAttempt {
                comparisons: vec![FeatureParityComparison::fixture(now, online, replay)],
                pending: Vec::new(),
            },
        )
        .expect("mismatch evidence");
        assert_eq!(
            mismatched.completion.status,
            FeatureParityRunStatus::Mismatched
        );
        assert_eq!(mismatched.completion.mismatched_count, 1);
    }

    #[test]
    fn writer_lag_not_mismatch() {
        let now = Utc::now();
        let run = run(now);
        let candidates = vec![FeatureParityCandidate {
            sampling_key: "report-a/market-001".to_owned(),
            subject: FeatureParitySubject::ModelRun(ModelRunId::from_v7()),
            market_id: Some(MarketId::new("market-001")),
            decision_at: now,
        }];
        let outcome = build_outcome(
            &run,
            &candidates,
            FeatureParityReplayAttempt {
                comparisons: Vec::new(),
                pending: vec![PendingFeatureParityComparison {
                    sampling_key: "report-a/market-001".to_owned(),
                    decision_at: now,
                    stage: FeatureParityStage::Snapshot,
                    report_id: None,
                    model_run_id: None,
                    model_version_id: None,
                    training_dataset_id: None,
                    market_id: None,
                    feature_name: None,
                    reason: "source_writer_watermark".to_owned(),
                    online: Some(evidence("online", now)),
                    required_watermark: now,
                    observed_watermark: Some(now - Duration::seconds(30)),
                }],
            },
        )
        .expect("pending evidence");
        assert_eq!(
            outcome.completion.status,
            FeatureParityRunStatus::PendingMaterialization
        );
        assert_eq!(outcome.completion.mismatched_count, 0);
        assert_eq!(outcome.completion.pending_materialization_count, 1);
    }

    #[tokio::test]
    async fn deterministic_mismatch_contains_stamp() {
        let now = Utc::now();
        let run = run(now);
        let report_id = RecommendationReportId::from_v7();
        let candidate = FeatureParityCandidate {
            sampling_key: "report-a/market-001".to_owned(),
            subject: FeatureParitySubject::ModelRun(ModelRunId::from_v7()),
            market_id: Some(MarketId::new("market-001")),
            decision_at: now,
        };
        let online = evidence("online", now);
        let mut mismatch = FeatureParityComparison::fixture(now, online, evidence("replay", now));
        mismatch.report_id = Some(report_id);
        mismatch.model_run_id = match &candidate.subject {
            FeatureParitySubject::ModelRun(run_id) => Some(*run_id),
            FeatureParitySubject::PreInferenceReport(_) => None,
        };
        mismatch.market_id.clone_from(&candidate.market_id);

        let parity = Arc::new(InMemoryParityRepository::with_run(run.clone()));
        let source = Arc::new(FixedReplaySource {
            candidates: vec![candidate],
            attempts: Mutex::new(VecDeque::from([FeatureParityReplayAttempt {
                comparisons: vec![mismatch],
                pending: Vec::new(),
            }])),
        });
        let writer = Arc::new(RecordingFactWriter::default());
        let incidents = Arc::new(RecordingIncidentPort::default());

        let result = executor(
            Arc::clone(&parity),
            source,
            Arc::clone(&writer),
            Arc::clone(&incidents),
        )
        .execute(
            params(&run),
            Arc::new(NoopProgressSink),
            CancellationToken::new(),
        )
        .await
        .expect("mismatch completes after containment");

        assert_eq!(result.status, FeatureParityRunStatus::Mismatched);
        assert_eq!(result.mismatched_count, 1);
        assert!(result.containment_completed_at.is_some());
        let repo_state = lock(&parity.state);
        assert_eq!(repo_state.mismatch_latch_opens, 1);
        assert_eq!(repo_state.containment_marks, 1);
        drop(repo_state);
        assert_eq!(lock(&writer.rows).len(), 1);
        assert_eq!(
            lock(&incidents.calls).as_slice(),
            &[(FeatureParityRunStatus::Mismatched, vec![report_id])]
        );
    }

    #[tokio::test]
    async fn expired_pending_rejects_contains() {
        let now = Utc::now();
        let mut run = run(now);
        run.pending_since = Some(now - Duration::minutes(11));
        let candidate = FeatureParityCandidate {
            sampling_key: "report-a/market-001".to_owned(),
            subject: FeatureParitySubject::ModelRun(ModelRunId::from_v7()),
            market_id: Some(MarketId::new("market-001")),
            decision_at: now,
        };
        let candidate_run_id = match &candidate.subject {
            FeatureParitySubject::ModelRun(run_id) => *run_id,
            FeatureParitySubject::PreInferenceReport(_) => unreachable!("test model subject"),
        };
        let pending = PendingFeatureParityComparison {
            sampling_key: candidate.sampling_key.clone(),
            decision_at: now,
            stage: FeatureParityStage::ModelInput,
            report_id: None,
            model_run_id: Some(candidate_run_id),
            model_version_id: None,
            training_dataset_id: None,
            market_id: candidate.market_id.clone(),
            feature_name: None,
            reason: "serving_evidence_completion_missing".to_owned(),
            online: None,
            required_watermark: now,
            observed_watermark: None,
        };
        let parity = Arc::new(InMemoryParityRepository::with_run(run.clone()));
        let source = Arc::new(FixedReplaySource {
            candidates: vec![candidate],
            attempts: Mutex::new(VecDeque::from([FeatureParityReplayAttempt {
                comparisons: Vec::new(),
                pending: vec![pending],
            }])),
        });
        let writer = Arc::new(RecordingFactWriter::default());
        let incidents = Arc::new(RecordingIncidentPort::default());

        let error = executor(
            Arc::clone(&parity),
            source,
            Arc::clone(&writer),
            Arc::clone(&incidents),
        )
        .execute(
            params(&run),
            Arc::new(NoopProgressSink),
            CancellationToken::new(),
        )
        .await
        .expect_err("expired pending evidence must fail closed");

        assert!(error.to_string().contains("materialization_timeout"));
        let failed = parity.run();
        assert_eq!(failed.status, FeatureParityRunStatus::Failed);
        assert_eq!(
            failed.failure_code.as_ref().map(DiagnosticCode::as_str),
            Some("materialization_timeout")
        );
        assert!(failed.containment_completed_at.is_some());
        let repo_state = lock(&parity.state);
        assert_eq!(
            repo_state.explicit_latch_opens,
            vec![FeatureParityStateTransition::IntegrityFailure]
        );
        assert_eq!(repo_state.containment_marks, 1);
        drop(repo_state);
        assert_eq!(lock(&writer.rows).len(), 1);
        assert_eq!(
            lock(&incidents.calls).as_slice(),
            &[(FeatureParityRunStatus::Failed, Vec::new())]
        );
    }

    #[tokio::test]
    async fn report_incident_without_lookup() {
        let now = Utc::now();
        let mut run = run(now);
        run.status = FeatureParityRunStatus::Mismatched;
        let bound_report_id = RecommendationReportId::from_v7();
        let explicit_report_id = RecommendationReportId::from_v7();
        run.report_id = Some(bound_report_id);
        let reports = Arc::new(RecordingReportContainment::default());
        let lookup = Arc::new(RecordingAffectedReportLookup::default());
        let alerts = Arc::new(Mutex::new(Vec::new()));
        let response = ReportFeatureParityIncidentResponse::with_test_ports(
            Arc::clone(&reports) as Arc<dyn ReportContainmentPort>,
            Arc::clone(&lookup) as Arc<dyn AffectedReportLookup>,
            Arc::new(AlertDispatcher::with_recordings(Arc::clone(&alerts))),
            Arc::new(MetricsHub::new()),
        );

        response
            .contain(
                &run,
                &[explicit_report_id, bound_report_id, explicit_report_id],
            )
            .await
            .expect("all affected reports revoked and cascaded");

        assert_eq!(*lock(&lookup.calls), 0);
        let actual = lock(&reports.calls)
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        let expected = [bound_report_id, explicit_report_id]
            .into_iter()
            .map(|id| id.to_string())
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
        assert_eq!(lock(&alerts).len(), 1);
    }

    #[tokio::test]
    async fn report_incident_falls_failure() {
        let now = Utc::now();
        let mut run = run(now);
        run.status = FeatureParityRunStatus::Failed;
        let first = RecommendationReportId::from_v7();
        let second = RecommendationReportId::from_v7();
        let reports = Arc::new(RecordingReportContainment::default());
        lock(&reports.failures).insert(first.to_string());
        let lookup = Arc::new(RecordingAffectedReportLookup {
            calls: Mutex::new(0),
            ids: vec![first, second],
        });
        let response = ReportFeatureParityIncidentResponse::with_test_ports(
            Arc::clone(&reports) as Arc<dyn ReportContainmentPort>,
            Arc::clone(&lookup) as Arc<dyn AffectedReportLookup>,
            Arc::new(AlertDispatcher::with_recordings(Arc::new(Mutex::new(
                Vec::new(),
            )))),
            Arc::new(MetricsHub::new()),
        );

        let error = response
            .contain(&run, &[])
            .await
            .expect_err("partial containment must keep the latch uncleared");

        assert!(error.to_string().contains("containment incomplete"));
        assert_eq!(*lock(&lookup.calls), 1);
        assert_eq!(lock(&reports.calls).len(), 2);
    }
}
