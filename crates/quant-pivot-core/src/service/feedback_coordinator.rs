//! Durable feedback-cycle coordinator.
//!
//! `PostgreSQL` remains the authority for cycle leases, `ResearchJob` execution,
//! and append-only stage evidence. The process-local wake only shortens
//! completion latency; bounded polling recovers every lost wake and every
//! process restart.

use std::{cmp, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashSet;
use quant_pivot_error::{
    QuantError, QuantResult,
    feedback::FeedbackError,
    research::ResearchError,
    storage::{
        StorageError,
        entity::{QUANT_FEEDBACK_CYCLE, QUANT_FEEDBACK_STAGE_EVENT},
    },
};
use quant_pivot_models::{
    config::ResearchJobsConfig,
    domain::quant::{
        FeedbackCoordinatorFaultReason, FeedbackCycleInfo, FeedbackCycleTerminal,
        FeedbackSchedulerStateInfo, FeedbackStageEventInfo, FeedbackStageEventInput,
        FeedbackStageJobIdentity, NewDriftReport, NewFeedbackStageEvent, NewResearchJob,
        ResearchJobInfo,
    },
    enums::{
        common::{AlertCategory, AlertLevel, AlertSource},
        quant::{
            FeedbackCycleStatus, FeedbackDecision, FeedbackDriftMetric, FeedbackStage,
            FeedbackStageEventKind, ResearchJobErrorCode, ResearchJobStatus,
        },
    },
    types::{ArtifactUri, ContentHash, FeedbackCycleId, ResearchJobError, ResearchJobId},
};
use quant_pivot_repository::traits::{
    ExecutionAttemptOutcomeRepository, FeedbackCoordinatorFaultWriteOutcome,
    FeedbackCycleCasOutcome, FeedbackCycleClaim, FeedbackCycleClaimMode, FeedbackCycleLeaseGuard,
    FeedbackCycleRepository, FeedbackSchedulerRepository, FeedbackStageWriteOutcome,
    RecommendationExecutionRollupRepository, ResearchJobEnqueueOutcome,
    ResolutionObservationRepository,
};
use tokio::{
    sync::watch::{self, Receiver, Sender},
    task::JoinSet,
    time::{Interval, MissedTickBehavior, interval, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    app::research_job::ResearchJobEngine,
    observability::{
        alert_dispatcher::{Alert, AlertDispatcher},
        metrics_hub::MetricsHub,
    },
};

const CYCLE_RECOVERED_REASON: &str = "cycle_lease_recovered";

/// Operational cadence for one feedback coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedbackCoordinatorConfig {
    poll_interval: Duration,
    lease_heartbeat: Duration,
    lease_ttl: Duration,
    max_inflight: usize,
    stuck_after: Duration,
    alert_timeout: Duration,
    alert_dedupe_secs: u64,
    shutdown_drain: Duration,
}

/// Named scheduler resource and lifecycle budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedbackCoordinatorBudget {
    pub poll_interval: Duration,
    pub lease_heartbeat: Duration,
    pub lease_ttl: Duration,
    pub max_inflight: usize,
    pub stuck_after: Duration,
    pub alert_timeout: Duration,
    pub alert_dedupe_secs: u64,
    pub shutdown_drain: Duration,
}

impl FeedbackCoordinatorConfig {
    /// Validate the complete scheduler resource and lifecycle budget.
    pub fn try_new(budget: FeedbackCoordinatorBudget) -> Result<Self, FeedbackError> {
        if !(Duration::from_secs(1)..=Duration::from_mins(5)).contains(&budget.poll_interval) {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "poll interval must be between one and 300 seconds".to_owned(),
            });
        }
        if !(Duration::from_secs(3)..=Duration::from_hours(1)).contains(&budget.lease_ttl) {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "lease TTL must be between three and 3600 seconds".to_owned(),
            });
        }
        if budget.lease_heartbeat < Duration::from_secs(1)
            || budget.lease_heartbeat > budget.lease_ttl / 3
        {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "lease heartbeat must be positive and no greater than TTL / 3".to_owned(),
            });
        }
        if !(1..=32).contains(&budget.max_inflight) {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "feedback cycle concurrency must be between one and 32".to_owned(),
            });
        }
        if budget.stuck_after <= budget.lease_ttl || budget.stuck_after > Duration::from_hours(720)
        {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "stuck threshold must exceed lease TTL and fit within 30 days".to_owned(),
            });
        }
        if budget.alert_timeout < Duration::from_secs(1)
            || budget.alert_timeout > Duration::from_secs(30)
            || budget.alert_timeout > budget.shutdown_drain
        {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "alert timeout must fit inside the shutdown drain".to_owned(),
            });
        }
        if budget.alert_dedupe_secs < budget.alert_timeout.as_secs()
            || budget.alert_dedupe_secs > 86_400
        {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "alert dedupe must span the timeout and fit within one day".to_owned(),
            });
        }
        if !(Duration::from_secs(1)..=Duration::from_secs(3)).contains(&budget.shutdown_drain) {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "shutdown drain must be between one and three seconds".to_owned(),
            });
        }
        Ok(Self {
            poll_interval: budget.poll_interval,
            lease_heartbeat: budget.lease_heartbeat,
            lease_ttl: budget.lease_ttl,
            max_inflight: budget.max_inflight,
            stuck_after: budget.stuck_after,
            alert_timeout: budget.alert_timeout,
            alert_dedupe_secs: budget.alert_dedupe_secs,
            shutdown_drain: budget.shutdown_drain,
        })
    }

    #[must_use]
    pub const fn poll_interval(self) -> Duration {
        self.poll_interval
    }

    #[must_use]
    pub const fn lease_heartbeat(self) -> Duration {
        self.lease_heartbeat
    }

    #[must_use]
    pub const fn lease_ttl(self) -> Duration {
        self.lease_ttl
    }

    #[must_use]
    pub const fn max_inflight(self) -> usize {
        self.max_inflight
    }

    #[must_use]
    pub const fn stuck_after(self) -> Duration {
        self.stuck_after
    }

    #[must_use]
    pub const fn alert_timeout(self) -> Duration {
        self.alert_timeout
    }

    #[must_use]
    pub const fn alert_dedupe_secs(self) -> u64 {
        self.alert_dedupe_secs
    }

    #[must_use]
    pub const fn shutdown_drain(self) -> Duration {
        self.shutdown_drain
    }
}

impl TryFrom<&ResearchJobsConfig> for FeedbackCoordinatorConfig {
    type Error = FeedbackError;

    fn try_from(config: &ResearchJobsConfig) -> Result<Self, Self::Error> {
        let lease_ttl_secs = u64::try_from(config.lease_ttl_secs).map_err(|_| {
            FeedbackError::InvalidCoordinatorConfig {
                detail: "lease TTL must be positive".to_owned(),
            }
        })?;
        Self::try_new(FeedbackCoordinatorBudget {
            poll_interval: Duration::from_secs(config.poll_secs),
            lease_heartbeat: Duration::from_secs(config.heartbeat_secs),
            lease_ttl: Duration::from_secs(lease_ttl_secs),
            max_inflight: config.feedback_cycle_concurrency,
            stuck_after: Duration::from_secs(config.feedback_stuck_secs),
            alert_timeout: Duration::from_secs(config.feedback_alert_timeout_secs),
            alert_dedupe_secs: config.feedback_alert_dedupe_secs,
            shutdown_drain: Duration::from_secs(config.shutdown_drain_secs),
        })
    }
}

/// Coalesced, non-authoritative process-local feedback wake.
#[derive(Clone)]
pub struct FeedbackCoordinatorWake {
    revision: Sender<u64>,
}

impl FeedbackCoordinatorWake {
    #[must_use]
    pub fn new() -> Self {
        let (revision, _receiver) = watch::channel(0);
        Self { revision }
    }

    /// Wake the resident coordinator after a local lifecycle change.
    pub fn wake(&self) {
        self.revision
            .send_modify(|revision| *revision = revision.saturating_add(1));
    }

    /// Subscribe one independent coalesced consumer before it starts work.
    #[must_use]
    pub fn subscribe(&self) -> FeedbackWakeReceiver {
        FeedbackWakeReceiver {
            revision: self.revision.subscribe(),
        }
    }
}

impl Default for FeedbackCoordinatorWake {
    fn default() -> Self {
        Self::new()
    }
}

/// Independent latest-revision subscription for one scheduler consumer.
pub struct FeedbackWakeReceiver {
    revision: Receiver<u64>,
}

impl FeedbackWakeReceiver {
    /// Wait for the newest unseen revision, coalescing intermediate wakes.
    pub async fn wait(&mut self) {
        if self.revision.changed().await.is_ok() {
            self.revision.borrow_and_update();
        }
    }
}

/// Durable action after validating one succeeded stage job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedbackStageDirective {
    /// Enqueue the next stage in the closed DAG.
    Advance,
    /// Finish the cycle with one business decision.
    Complete(FeedbackCycleTerminal),
}

/// Content-addressed evidence and durable action for a succeeded stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackStageSuccess {
    evidence_uri: ArtifactUri,
    evidence_hash: ContentHash,
    directive: FeedbackStageDirective,
    drift_reports: Vec<NewDriftReport>,
}

/// Durable scheduler action returned before a stage job exists.
#[derive(Debug)]
pub enum FeedbackStagePreparation {
    Ready(Box<NewResearchJob>),
    Deferred {
        resume_after: DateTime<Utc>,
        reason_code: &'static str,
    },
}

impl FeedbackStageSuccess {
    /// Advance after sealing this stage's immutable evidence.
    #[must_use]
    pub const fn advance(evidence_uri: ArtifactUri, evidence_hash: ContentHash) -> Self {
        Self {
            evidence_uri,
            evidence_hash,
            directive: FeedbackStageDirective::Advance,
            drift_reports: Vec::new(),
        }
    }

    /// Finish successfully after sealing this stage's immutable evidence.
    pub fn try_complete(
        evidence_uri: ArtifactUri,
        evidence_hash: ContentHash,
        decision: FeedbackDecision,
        reason_code: String,
    ) -> Result<Self, FeedbackError> {
        Ok(Self {
            evidence_uri,
            evidence_hash,
            directive: FeedbackStageDirective::Complete(FeedbackCycleTerminal::try_succeeded(
                decision,
                reason_code,
            )?),
            drift_reports: Vec::new(),
        })
    }

    /// Attach the exact four aggregate drift headers, or no headers when
    /// overlapping windows produced typed insufficient evidence.
    pub fn attach_drift(mut self, reports: Vec<NewDriftReport>) -> Result<Self, FeedbackError> {
        let expected = [
            FeedbackDriftMetric::PopulationStabilityIndex,
            FeedbackDriftMetric::KolmogorovSmirnovPValue,
            FeedbackDriftMetric::RankIcDrop,
            FeedbackDriftMetric::JensenShannonDivergence,
        ];
        if !reports.is_empty()
            && (reports.len() != expected.len()
                || reports
                    .iter()
                    .zip(expected)
                    .any(|(report, metric)| report.metric() != metric)
                || reports.first().is_some_and(|first| {
                    reports
                        .iter()
                        .any(|report| report.feedback_cycle_id() != first.feedback_cycle_id())
                }))
        {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "drift stage must attach zero or four ordered exact metric headers"
                    .to_owned(),
            });
        }
        self.drift_reports = reports;
        Ok(self)
    }

    #[must_use]
    pub const fn evidence_uri(&self) -> &ArtifactUri {
        &self.evidence_uri
    }

    #[must_use]
    pub const fn evidence_hash(&self) -> ContentHash {
        self.evidence_hash
    }

    #[must_use]
    pub const fn directive(&self) -> &FeedbackStageDirective {
        &self.directive
    }

    #[must_use]
    pub fn drift_reports(&self) -> &[NewDriftReport] {
        &self.drift_reports
    }
}

/// Stage-specific boundary implemented by F06/F07/F09/F10/F11 adapters.
///
/// `prepare` must return the exact queued job bound to `identity`.
/// `succeeded` must be deterministic for immutable cycle/job inputs so a
/// coordinator restart can revalidate already-appended WORM evidence.
#[async_trait]
pub trait FeedbackStagePort: Send + Sync {
    async fn prepare(
        &self,
        cycle: &FeedbackCycleInfo,
        lease: FeedbackCycleLeaseGuard,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<FeedbackStagePreparation>;

    async fn succeeded(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess>;
}

/// Releases an active route-owned shadow before a cancelled cycle becomes
/// terminal. Implementations must be idempotent and converge the committed
/// policy before returning success.
#[async_trait]
pub trait FeedbackShadowCancellationPort: Send + Sync {
    async fn release_cycle(&self, cycle: &FeedbackCycleInfo, reason_code: &str) -> QuantResult<()>;
}

/// Dependencies for the one resident feedback coordinator.
pub struct FeedbackCoordinatorDeps {
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub scheduler: Arc<dyn FeedbackSchedulerRepository>,
    pub resolutions: Arc<dyn ResolutionObservationRepository>,
    pub attempts: Arc<dyn ExecutionAttemptOutcomeRepository>,
    pub rollups: Arc<dyn RecommendationExecutionRollupRepository>,
    pub jobs: ResearchJobEngine,
    pub stages: Arc<dyn FeedbackStagePort>,
    pub shadow_cancellation: Arc<dyn FeedbackShadowCancellationPort>,
    pub metrics: Arc<MetricsHub>,
    pub alerts: Arc<AlertDispatcher>,
    pub config: FeedbackCoordinatorConfig,
}

/// Single owner of the durable feedback DAG.
#[derive(Clone)]
pub struct FeedbackCoordinator {
    cycles: Arc<dyn FeedbackCycleRepository>,
    scheduler: Arc<dyn FeedbackSchedulerRepository>,
    resolutions: Arc<dyn ResolutionObservationRepository>,
    attempts: Arc<dyn ExecutionAttemptOutcomeRepository>,
    rollups: Arc<dyn RecommendationExecutionRollupRepository>,
    jobs: ResearchJobEngine,
    stages: Arc<dyn FeedbackStagePort>,
    shadow_cancellation: Arc<dyn FeedbackShadowCancellationPort>,
    wake: FeedbackCoordinatorWake,
    metrics: Arc<MetricsHub>,
    alerts: Arc<AlertDispatcher>,
    stuck_seen: Arc<DashSet<FeedbackCycleId>>,
    config: FeedbackCoordinatorConfig,
}

impl FeedbackCoordinator {
    #[must_use]
    pub fn new(deps: FeedbackCoordinatorDeps) -> Self {
        let wake = deps.jobs.feedback_wake();
        Self {
            cycles: deps.cycles,
            scheduler: deps.scheduler,
            resolutions: deps.resolutions,
            attempts: deps.attempts,
            rollups: deps.rollups,
            jobs: deps.jobs,
            stages: deps.stages,
            shadow_cancellation: deps.shadow_cancellation,
            wake,
            metrics: deps.metrics,
            alerts: deps.alerts,
            stuck_seen: Arc::new(DashSet::new()),
            config: deps.config,
        }
    }

    /// Run until the task's staged-shutdown token is cancelled.
    pub async fn run(&self, shutdown: CancellationToken) {
        let mut tasks = JoinSet::<FeedbackCycleId>::new();
        let mut scheduler_wake = self.wake.subscribe();
        while !shutdown.is_cancelled() {
            let mut queue_exhausted = false;
            while tasks.len() < self.config.max_inflight() && !shutdown.is_cancelled() {
                match self
                    .cycles
                    .claim_cycle(*self.jobs.instance_id(), self.config.lease_ttl().as_secs())
                    .await
                {
                    Ok(Some(claim)) => {
                        let cycle_id = claim.cycle.feedback_cycle_id;
                        let coordinator = self.clone();
                        let child_shutdown = shutdown.child_token();
                        let cycle_wake = self.wake.subscribe();
                        tasks.spawn(async move {
                            coordinator
                                .drive_cycle(claim, cycle_wake, &child_shutdown)
                                .await;
                            cycle_id
                        });
                        self.set_active(tasks.len());
                    }
                    Ok(None) => {
                        queue_exhausted = true;
                        break;
                    }
                    Err(error) => {
                        self.metrics
                            .record_research_heartbeat("feedback_claim", "storage_error");
                        warn!(%error, "feedback-cycle claim failed; backing off");
                        queue_exhausted = true;
                        break;
                    }
                }
            }
            self.refresh_queue().await;
            self.refresh_operations().await;
            if shutdown.is_cancelled() {
                break;
            }
            if !queue_exhausted && tasks.len() < self.config.max_inflight() {
                continue;
            }
            tokio::select! {
                () = shutdown.cancelled() => {}
                () = scheduler_wake.wait() => {}
                () = sleep(self.config.poll_interval()) => {}
                joined = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Err(error)) = joined {
                        warn!(%error, "feedback-cycle task terminated unexpectedly");
                    }
                    self.set_active(tasks.len());
                }
            }
        }

        if tokio::time::timeout(self.config.shutdown_drain(), async {
            while tasks.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            warn!(
                drain_secs = self.config.shutdown_drain().as_secs(),
                remaining = tasks.len(),
                "feedback-cycle shutdown drain timed out"
            );
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
        self.set_active(0);
        info!("feedback coordinator stopped");
    }

    async fn drive_cycle(
        &self,
        claim: FeedbackCycleClaim,
        mut wake: FeedbackWakeReceiver,
        shutdown: &CancellationToken,
    ) {
        let cycle_id = claim.cycle.feedback_cycle_id;
        let mut lease = claim.lease;
        let mut recovery = RecoveryMarker::from_claim(&claim);
        let mut heartbeat = interval(self.config.lease_heartbeat());
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        heartbeat.tick().await;
        let mut release_on_shutdown = false;

        loop {
            if shutdown.is_cancelled() {
                release_on_shutdown = true;
                break;
            }
            match self.reconcile_cycle(&mut lease, &mut recovery).await {
                Ok(ReconcileState::Progressed) => continue,
                Ok(ReconcileState::Waiting) => {}
                Ok(ReconcileState::Finished | ReconcileState::LeaseLost) => break,
                Err(error) => {
                    if let Some(detail) = Self::invalid_state_detail(&error) {
                        self.quarantine_cycle(lease, &detail).await;
                        break;
                    }
                    warn!(%cycle_id, %error, "feedback-cycle reconciliation failed closed");
                }
            }
            match self
                .wait_cycle(&mut lease, &mut heartbeat, &mut wake, shutdown)
                .await
            {
                WaitState::Continue => {}
                WaitState::Stop => {
                    release_on_shutdown = true;
                    break;
                }
                WaitState::LeaseLost => break,
            }
        }
        if release_on_shutdown {
            self.release_lease(lease).await;
        }
        self.stuck_seen.remove(&cycle_id);
    }

    fn invalid_state_detail(error: &QuantError) -> Option<String> {
        match error {
            QuantError::Feedback(
                FeedbackError::InvalidCoordinatorState { detail }
                | FeedbackError::InvalidCycleState { detail }
                | FeedbackError::InvalidStageEvent { detail }
                | FeedbackError::InvalidJobIdentity { detail }
                | FeedbackError::InvalidJobContract { detail },
            ) => Some(detail.clone()),
            QuantError::Research(
                error @ (ResearchError::Serialization { .. }
                | ResearchError::ArtifactHashMismatch { .. }
                | ResearchError::SchemaHashMismatch { .. }
                | ResearchError::Determinism { .. }
                | ResearchError::InvalidModelArtifact { .. }),
            ) => Some(format!(
                "persisted stage artifact failed validation: {error}"
            )),
            QuantError::Storage(StorageError::InvariantViolation {
                entity: Some(entity),
                detail,
            }) if matches!(*entity, QUANT_FEEDBACK_CYCLE | QUANT_FEEDBACK_STAGE_EVENT) => Some(
                format!("persisted coordinator row failed validation: {detail}"),
            ),
            _ => None,
        }
    }

    async fn quarantine_cycle(&self, lease: FeedbackCycleLeaseGuard, detail: &str) {
        let reason = FeedbackCoordinatorFaultReason::invalid_state(detail);
        match self.cycles.quarantine_cycle(lease, reason).await {
            Ok(commit) => {
                self.record_cycle(&commit.cycle);
                let fault = match &commit.fault {
                    FeedbackCoordinatorFaultWriteOutcome::Inserted(fault)
                    | FeedbackCoordinatorFaultWriteOutcome::AlreadyPresent(fault) => fault,
                };
                self.metrics
                    .record_research_heartbeat("feedback_coordinator_quarantine", "committed");
                self.dispatch_ops_alert(Alert::new(
                    format!(
                        "feedback-coordinator-quarantine:{}",
                        lease.feedback_cycle_id
                    ),
                    AlertLevel::Critical,
                    AlertCategory::SchedulerHealth,
                    AlertSource::Scheduler,
                    "Feedback cycle quarantined after coordinator-state corruption",
                    format!(
                        "cycle={} generation={} fault_id={} code={}; the cycle is immutable and must be replaced by a new cycle",
                        lease.feedback_cycle_id,
                        lease.expected_generation,
                        fault.feedback_coordinator_fault_id,
                        fault.fault_code
                    ),
                    fault.observed_at,
                ))
                .await;
            }
            Err(StorageError::StateConflict { .. } | StorageError::IllegalTransition { .. }) => {
                self.metrics
                    .record_research_heartbeat("feedback_coordinator_quarantine", "lease_lost");
            }
            Err(error) => {
                self.metrics
                    .record_research_heartbeat("feedback_coordinator_quarantine", "storage_error");
                warn!(
                    cycle_id = %lease.feedback_cycle_id,
                    %error,
                    "feedback-cycle quarantine persistence failed; lease will expire without renewal"
                );
                self.dispatch_ops_alert(Alert::new(
                    format!(
                        "feedback-coordinator-quarantine-persistence:{}",
                        lease.feedback_cycle_id
                    ),
                    AlertLevel::Critical,
                    AlertCategory::SchedulerHealth,
                    AlertSource::Scheduler,
                    "Feedback cycle corruption could not be persisted",
                    format!(
                        "cycle={} generation={}; coordinator stopped renewal and requires database recovery",
                        lease.feedback_cycle_id, lease.expected_generation
                    ),
                    Utc::now(),
                ))
                .await;
            }
        }
    }

    async fn reconcile_cycle(
        &self,
        lease: &mut FeedbackCycleLeaseGuard,
        recovery: &mut Option<RecoveryMarker>,
    ) -> QuantResult<ReconcileState> {
        let Some(cycle) = self.cycles.find_cycle(&lease.feedback_cycle_id).await? else {
            return Err(
                StorageError::not_found(QUANT_FEEDBACK_CYCLE, lease.feedback_cycle_id).into(),
            );
        };
        cycle
            .validate()
            .map_err(|error| FeedbackError::InvalidCoordinatorState {
                detail: format!("feedback cycle projection failed validation: {error}"),
            })?;
        if cycle.status.is_terminal() {
            return Ok(ReconcileState::Finished);
        }
        if cycle.status != FeedbackCycleStatus::Running
            || cycle.lease_owner.as_ref() != Some(self.jobs.instance_id())
        {
            return Ok(ReconcileState::LeaseLost);
        }
        *lease = lease.with_generation(cycle.generation);
        if let Err(error) = self.detect_stuck(&cycle).await {
            self.metrics
                .record_research_heartbeat("feedback_stuck_check", "storage_error");
            warn!(
                cycle_id = %cycle.feedback_cycle_id,
                %error,
                "feedback-cycle stuck detection could not read database time"
            );
        }

        let events = self
            .cycles
            .list_stage_events(&cycle.feedback_cycle_id)
            .await?;
        let timeline = FeedbackTimeline::parse(&events)?;
        if let Some(marker) = *recovery {
            if let Some((stage, job_id)) = timeline.audit_job() {
                let job = self.load_job(&cycle, stage, job_id).await?;
                let occurred_at = marker.occurred_at.max(job.created_at);
                if timeline.has_recovery(stage, job_id, occurred_at) {
                    *recovery = None;
                } else {
                    let event = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                        feedback_cycle_id: cycle.feedback_cycle_id,
                        event_sequence: timeline.next_sequence,
                        stage,
                        event_kind: FeedbackStageEventKind::LeaseRecovered,
                        trigger_family: None,
                        research_job_id: Some(job_id),
                        actor: None,
                        reason_code: Some(CYCLE_RECOVERED_REASON.to_owned()),
                        evidence_uri: None,
                        evidence_hash: None,
                        occurred_at,
                    })?;
                    self.cycles.append_stage(*lease, event).await?;
                    return Ok(ReconcileState::Progressed);
                }
            } else {
                *recovery = None;
            }
        }

        match timeline.position {
            TimelinePosition::Active { stage, job_id } => {
                self.reconcile_active(&cycle, *lease, &events, stage, job_id, timeline)
                    .await
            }
            TimelinePosition::Succeeded {
                stage,
                job_id,
                evidence_uri,
                evidence_hash,
            } => {
                let job = self.load_job(&cycle, stage, job_id).await?;
                let success = self.stages.succeeded(&cycle, &job).await?;
                if success.evidence_uri() != &evidence_uri
                    || success.evidence_hash() != evidence_hash
                {
                    return Err(FeedbackError::InvalidCoordinatorState {
                        detail: format!(
                            "stage {stage} success evidence differs from its WORM event"
                        ),
                    }
                    .into());
                }
                self.append_drift_reports(&cycle, *lease, &success).await?;
                if let Some(reason) = timeline.cancel_reason {
                    self.finish_cancelled(&cycle, *lease, reason).await
                } else {
                    match success.directive() {
                        FeedbackStageDirective::Advance => {
                            let next = stage.next().ok_or_else(|| {
                                FeedbackError::InvalidCoordinatorState {
                                    detail: "decision stage cannot advance beyond the closed DAG"
                                        .to_owned(),
                                }
                            })?;
                            self.enqueue_stage(&cycle, *lease, next, timeline.next_sequence)
                                .await
                        }
                        FeedbackStageDirective::Complete(terminal) => {
                            if terminal.status() != FeedbackCycleStatus::Succeeded {
                                return Err(FeedbackError::InvalidCoordinatorState {
                                    detail: "succeeded stage returned a non-success terminal"
                                        .to_owned(),
                                }
                                .into());
                            }
                            let outcome =
                                self.cycles.finalize_cycle(*lease, terminal.clone()).await?;
                            self.record_cycle(&outcome);
                            Ok(ReconcileState::Finished)
                        }
                    }
                }
            }
            TimelinePosition::Failed { reason_code, .. } => {
                if let Some(reason) = timeline.cancel_reason {
                    self.finish_cancelled(&cycle, *lease, reason).await
                } else {
                    let outcome = self
                        .cycles
                        .finalize_cycle(*lease, FeedbackCycleTerminal::try_failed(reason_code)?)
                        .await?;
                    self.record_cycle(&outcome);
                    Ok(ReconcileState::Finished)
                }
            }
            TimelinePosition::Cancelled { reason_code, .. } => {
                self.finish_cancelled(
                    &cycle,
                    *lease,
                    timeline.cancel_reason.unwrap_or(reason_code),
                )
                .await
            }
        }
    }

    async fn reconcile_active(
        &self,
        cycle: &FeedbackCycleInfo,
        lease: FeedbackCycleLeaseGuard,
        events: &[FeedbackStageEventInfo],
        stage: FeedbackStage,
        job_id: Option<ResearchJobId>,
        timeline: FeedbackTimeline,
    ) -> QuantResult<ReconcileState> {
        let Some(job_id) = job_id else {
            if let Some(reason) = timeline.cancel_reason {
                return self.finish_cancelled(cycle, lease, reason).await;
            }
            return self
                .enqueue_stage(cycle, lease, stage, timeline.next_sequence)
                .await;
        };

        let job = self.load_job(cycle, stage, job_id).await?;
        if job.started_at.is_some_and(|started_at| {
            !FeedbackTimeline::has_event(
                events,
                stage,
                job_id,
                FeedbackStageEventKind::Started,
                started_at,
            )
        }) {
            let event = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                feedback_cycle_id: cycle.feedback_cycle_id,
                event_sequence: timeline.next_sequence,
                stage,
                event_kind: FeedbackStageEventKind::Started,
                trigger_family: None,
                research_job_id: Some(job_id),
                actor: None,
                reason_code: None,
                evidence_uri: None,
                evidence_hash: None,
                occurred_at: job.started_at.ok_or_else(|| {
                    FeedbackError::InvalidCoordinatorState {
                        detail: format!("stage {stage} lost its observed start timestamp"),
                    }
                })?,
            })?;
            self.cycles.append_stage(lease, event).await?;
            return Ok(ReconcileState::Progressed);
        }

        match job.status {
            ResearchJobStatus::Queued
            | ResearchJobStatus::AwaitingEvidence
            | ResearchJobStatus::RetryScheduled => {
                if timeline.cancel_reason.is_some() {
                    let cancelled = self
                        .jobs
                        .repo()
                        .cancel_if_pending(
                            &job_id,
                            ResearchJobError::new(
                                ResearchJobErrorCode::Cancelled,
                                "feedback cycle cancelled at a stage boundary",
                            ),
                        )
                        .await?;
                    if cancelled && let Some(updated) = self.jobs.repo().find_by_id(&job_id).await?
                    {
                        self.jobs
                            .publish(&updated, Some("cancelled".to_owned()), None);
                    }
                    Ok(ReconcileState::Progressed)
                } else {
                    Ok(ReconcileState::Waiting)
                }
            }
            ResearchJobStatus::Running => Ok(ReconcileState::Waiting),
            ResearchJobStatus::Succeeded => {
                let finished_at = Self::finished_at(&job)?;
                let success = self.stages.succeeded(cycle, &job).await?;
                self.append_drift_reports(cycle, lease, &success).await?;
                let event = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                    feedback_cycle_id: cycle.feedback_cycle_id,
                    event_sequence: timeline.next_sequence,
                    stage,
                    event_kind: FeedbackStageEventKind::Succeeded,
                    trigger_family: None,
                    research_job_id: Some(job_id),
                    actor: None,
                    reason_code: None,
                    evidence_uri: Some(success.evidence_uri().clone()),
                    evidence_hash: Some(success.evidence_hash()),
                    occurred_at: finished_at,
                })?;
                let outcome = self.cycles.append_stage(lease, event).await?;
                self.record_stage(&outcome, stage, &job);
                Ok(ReconcileState::Progressed)
            }
            ResearchJobStatus::Failed | ResearchJobStatus::Cancelled => {
                let finished_at = Self::finished_at(&job)?;
                let reason_code = Self::job_reason(&job)?;
                let event_kind = if job.status == ResearchJobStatus::Failed {
                    FeedbackStageEventKind::Failed
                } else {
                    FeedbackStageEventKind::Cancelled
                };
                let event = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                    feedback_cycle_id: cycle.feedback_cycle_id,
                    event_sequence: timeline.next_sequence,
                    stage,
                    event_kind,
                    trigger_family: None,
                    research_job_id: Some(job_id),
                    actor: None,
                    reason_code: Some(reason_code),
                    evidence_uri: None,
                    evidence_hash: None,
                    occurred_at: finished_at,
                })?;
                let outcome = self.cycles.append_stage(lease, event).await?;
                self.record_stage(&outcome, stage, &job);
                Ok(ReconcileState::Progressed)
            }
        }
    }

    async fn append_drift_reports(
        &self,
        cycle: &FeedbackCycleInfo,
        lease: FeedbackCycleLeaseGuard,
        success: &FeedbackStageSuccess,
    ) -> QuantResult<()> {
        for report in success.drift_reports() {
            if report.feedback_cycle_id() != cycle.feedback_cycle_id {
                return Err(FeedbackError::InvalidCoordinatorState {
                    detail: "stage adapter returned a drift header for another cycle".to_owned(),
                }
                .into());
            }
            self.cycles.append_drift(lease, report.clone()).await?;
        }
        Ok(())
    }

    async fn enqueue_stage(
        &self,
        cycle: &FeedbackCycleInfo,
        lease: FeedbackCycleLeaseGuard,
        stage: FeedbackStage,
        event_sequence: i64,
    ) -> QuantResult<ReconcileState> {
        let identity = FeedbackStageJobIdentity::try_root(cycle.feedback_cycle_id, stage)?;
        let (job, inserted) = if let Some(info) =
            self.jobs.repo().find_by_id(&identity.job_id()).await?
        {
            info.validate_identity()?;
            if info.feedback_cycle_id != Some(cycle.feedback_cycle_id)
                || info.feedback_stage != Some(stage)
                || info.parent_job_id.is_some()
            {
                return Err(FeedbackError::InvalidCoordinatorState {
                    detail: format!(
                        "deterministic root job {} has invalid cycle/stage lineage",
                        info.job_id
                    ),
                }
                .into());
            }
            (info, false)
        } else {
            let job = match self.stages.prepare(cycle, lease, identity).await? {
                FeedbackStagePreparation::Ready(job) => *job,
                FeedbackStagePreparation::Deferred {
                    resume_after,
                    reason_code,
                } => {
                    let deferred = self.cycles.defer_cycle(lease, resume_after).await?;
                    self.metrics
                        .record_research_heartbeat("feedback_stage_deferred", reason_code);
                    info!(
                        cycle_id = %cycle.feedback_cycle_id,
                        %stage,
                        %resume_after,
                        reason_code,
                        generation = deferred.generation,
                        "feedback stage deferred to its durable resume boundary"
                    );
                    return Ok(ReconcileState::LeaseLost);
                }
            };
            job.validate_enqueue()?;
            if job.job_id != identity.job_id()
                || job.feedback_cycle_id != Some(cycle.feedback_cycle_id)
                || job.feedback_stage != Some(stage)
                || job.parent_job_id.is_some()
            {
                return Err(FeedbackError::InvalidCoordinatorState {
                        detail: format!(
                            "stage {stage} adapter returned a job outside its deterministic root identity"
                        ),
                    }
                    .into());
            }
            match self.jobs.repo().enqueue(job).await? {
                ResearchJobEnqueueOutcome::Inserted(info) => (info, true),
                ResearchJobEnqueueOutcome::AlreadyPresent(info) => (info, false),
            }
        };
        if inserted {
            self.jobs.publish(&job, None, None);
        }
        let event = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
            feedback_cycle_id: cycle.feedback_cycle_id,
            event_sequence,
            stage,
            event_kind: FeedbackStageEventKind::JobLinked,
            trigger_family: None,
            research_job_id: Some(job.job_id),
            actor: None,
            reason_code: None,
            evidence_uri: None,
            evidence_hash: None,
            occurred_at: job.created_at,
        })?;
        self.cycles.append_stage(lease, event).await?;
        Ok(ReconcileState::Progressed)
    }

    async fn load_job(
        &self,
        cycle: &FeedbackCycleInfo,
        stage: FeedbackStage,
        job_id: ResearchJobId,
    ) -> QuantResult<ResearchJobInfo> {
        let job = self.jobs.repo().find_by_id(&job_id).await?.ok_or_else(|| {
            FeedbackError::InvalidCoordinatorState {
                detail: format!("stage {stage} references missing job {job_id}"),
            }
        })?;
        job.validate_identity()?;
        if job.feedback_cycle_id != Some(cycle.feedback_cycle_id)
            || job.feedback_stage != Some(stage)
        {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: format!("job {job_id} is outside cycle/stage lineage"),
            }
            .into());
        }
        Ok(job)
    }

    async fn finish_cancelled(
        &self,
        cycle: &FeedbackCycleInfo,
        lease: FeedbackCycleLeaseGuard,
        reason_code: String,
    ) -> QuantResult<ReconcileState> {
        self.shadow_cancellation
            .release_cycle(cycle, &reason_code)
            .await?;
        let outcome = self
            .cycles
            .finalize_cycle(lease, FeedbackCycleTerminal::try_cancelled(reason_code)?)
            .await?;
        self.record_cycle(&outcome);
        Ok(ReconcileState::Finished)
    }

    async fn detect_stuck(&self, cycle: &FeedbackCycleInfo) -> QuantResult<()> {
        let Some(started_at) = cycle.started_at else {
            return Ok(());
        };
        let database_time = self.cycles.database_time().await?;
        let Ok(age) = database_time.signed_duration_since(started_at).to_std() else {
            return Ok(());
        };
        if age < self.config.stuck_after() || !self.stuck_seen.insert(cycle.feedback_cycle_id) {
            return Ok(());
        }

        self.metrics.feedback_stuck_total.inc();
        let alert = Alert::new(
            format!("feedback-cycle-stuck:{}", cycle.feedback_cycle_id),
            AlertLevel::Warning,
            AlertCategory::SchedulerHealth,
            AlertSource::Scheduler,
            "Feedback cycle exceeded its runtime threshold",
            format!(
                "cycle={} generation={} age_secs={} remains running; durable reconciliation continues",
                cycle.feedback_cycle_id,
                cycle.generation,
                age.as_secs()
            ),
            database_time,
        )
        .with_dedupe_secs(self.config.alert_dedupe_secs())
        .with_affects_trading(false)
        .with_visible_toast(false);
        if tokio::time::timeout(self.config.alert_timeout(), self.alerts.dispatch(alert))
            .await
            .is_err()
        {
            self.metrics
                .record_research_heartbeat("feedback_alert", "timeout");
            warn!(
                cycle_id = %cycle.feedback_cycle_id,
                timeout_secs = self.config.alert_timeout().as_secs(),
                "feedback-cycle stuck alert timed out"
            );
        }
        Ok(())
    }

    async fn release_lease(&self, lease: FeedbackCycleLeaseGuard) {
        match self.cycles.release_cycle_lease(lease).await {
            Ok(_) => self
                .metrics
                .record_research_heartbeat("feedback_release", "released"),
            Err(StorageError::StateConflict { .. } | StorageError::IllegalTransition { .. }) => {
                self.metrics
                    .record_research_heartbeat("feedback_release", "lease_lost");
            }
            Err(error) => {
                self.metrics
                    .record_research_heartbeat("feedback_release", "storage_error");
                warn!(
                    cycle_id = %lease.feedback_cycle_id,
                    %error,
                    "feedback-cycle shutdown lease release failed"
                );
            }
        }
    }

    async fn refresh_queue(&self) {
        match self.cycles.queue_snapshot().await {
            Ok(snapshot) => self
                .metrics
                .set_feedback_queue(snapshot.queued, snapshot.pending_outbox),
            Err(error) => {
                self.metrics
                    .record_research_heartbeat("feedback_queue", "storage_error");
                warn!(%error, "feedback-cycle queue snapshot failed");
            }
        }
    }

    async fn refresh_operations(&self) {
        let database_time = match self.cycles.database_time().await {
            Ok(database_time) => database_time,
            Err(error) => {
                self.metrics
                    .record_research_heartbeat("feedback_operations_clock", "storage_error");
                warn!(%error, "feedback operations monitor could not read database time");
                return;
            }
        };
        let snapshot = tokio::try_join!(
            self.resolutions.barrier(database_time),
            self.attempts.barrier(database_time),
            self.rollups.barrier(database_time),
            self.scheduler.list_states(),
        );
        let (resolution, attempts, rollups, scheduler) = match snapshot {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.metrics
                    .record_research_heartbeat("feedback_operations", "storage_error");
                warn!(%error, "feedback operations snapshot failed");
                return;
            }
        };
        let inbox_age_secs = resolution
            .oldest_unresolved_at
            .map_or(0, |oldest| Self::database_lag(database_time, oldest));
        self.metrics.set_feedback_truth(
            inbox_age_secs,
            resolution.quarantined_count,
            Self::database_lag(database_time, resolution.terminal_through),
            Self::database_lag(database_time, attempts.sealed_through),
            Self::database_lag(database_time, rollups.sealed_through),
        );
        let (overdue_profiles, max_overdue_secs) =
            Self::scheduler_overdue(database_time, &scheduler);
        self.metrics
            .set_scheduler_overdue(overdue_profiles, max_overdue_secs);
        if resolution.quarantined_count > 0 {
            self.dispatch_ops_alert(Alert::new(
                "feedback-resolution-quarantine",
                AlertLevel::Critical,
                AlertCategory::SchedulerHealth,
                AlertSource::Scheduler,
                "Resolution observations are quarantined",
                format!(
                    "quarantined={} unresolved={} oldest_age_secs={}; TruthFreeze remains fail-closed",
                    resolution.quarantined_count, resolution.unresolved_count, inbox_age_secs
                ),
                database_time,
            ))
            .await;
        }
        if overdue_profiles > 0 {
            self.dispatch_ops_alert(Alert::new(
                "feedback-scheduler-overdue",
                AlertLevel::Warning,
                AlertCategory::SchedulerHealth,
                AlertSource::Scheduler,
                "Automatic retraining scheduler is overdue",
                format!(
                    "overdue_profiles={overdue_profiles} max_overdue_secs={max_overdue_secs}; durable scheduler state remains authoritative"
                ),
                database_time,
            ))
            .await;
        }
    }

    fn database_lag(database_time: DateTime<Utc>, frontier: DateTime<Utc>) -> u64 {
        database_time
            .signed_duration_since(frontier)
            .to_std()
            .map_or(0, |duration| duration.as_secs())
    }

    fn scheduler_overdue(
        database_time: DateTime<Utc>,
        states: &[FeedbackSchedulerStateInfo],
    ) -> (u64, u64) {
        let mut profiles = 0_u64;
        let mut max_overdue_secs = 0_u64;
        for state in states {
            if state.paused
                || state
                    .lease_expires_at
                    .is_some_and(|expires_at| expires_at > database_time)
            {
                continue;
            }
            let effective_due = [state.cooldown_until, state.retry_at]
                .into_iter()
                .flatten()
                .fold(state.next_due_at, cmp::max);
            if effective_due >= database_time {
                continue;
            }
            profiles = profiles.saturating_add(1);
            max_overdue_secs =
                max_overdue_secs.max(Self::database_lag(database_time, effective_due));
        }
        (profiles, max_overdue_secs)
    }

    async fn dispatch_ops_alert(&self, alert: Alert) {
        let alert = alert
            .with_dedupe_secs(self.config.alert_dedupe_secs())
            .with_affects_trading(false)
            .with_visible_toast(true);
        if tokio::time::timeout(self.config.alert_timeout(), self.alerts.dispatch(alert))
            .await
            .is_err()
        {
            self.metrics
                .record_research_heartbeat("feedback_operations_alert", "timeout");
        }
    }

    fn record_cycle(&self, outcome: &FeedbackCycleCasOutcome) {
        let FeedbackCycleCasOutcome::Applied(cycle) = outcome else {
            return;
        };
        let (Some(started_at), Some(completed_at)) = (cycle.started_at, cycle.completed_at) else {
            warn!(
                cycle_id = %cycle.feedback_cycle_id,
                "terminal feedback cycle lacks duration timestamps"
            );
            return;
        };
        let Ok(duration) = completed_at.signed_duration_since(started_at).to_std() else {
            warn!(
                cycle_id = %cycle.feedback_cycle_id,
                "terminal feedback cycle has a negative duration"
            );
            return;
        };
        let decision = match cycle.decision {
            Some(FeedbackDecision::NoAction) => "no_action",
            Some(FeedbackDecision::ChallengerRejected) => "challenger_rejected",
            Some(FeedbackDecision::CandidateReady) => "candidate_ready",
            Some(FeedbackDecision::Promoted) => "promoted",
            None => "none",
        };
        self.metrics
            .record_feedback_cycle(cycle.status.as_str(), decision, duration.as_secs_f64());
    }

    fn record_stage(
        &self,
        outcome: &FeedbackStageWriteOutcome,
        stage: FeedbackStage,
        job: &ResearchJobInfo,
    ) {
        if !matches!(outcome, FeedbackStageWriteOutcome::Inserted(_)) {
            return;
        }
        let Some(finished_at) = job.finished_at else {
            return;
        };
        let started_at = job.started_at.unwrap_or(job.created_at);
        let Ok(duration) = finished_at.signed_duration_since(started_at).to_std() else {
            warn!(job_id = %job.job_id, "feedback stage has a negative duration");
            return;
        };
        self.metrics.observe_feedback_stage(
            stage.as_str(),
            job.status.as_str(),
            duration.as_secs_f64(),
        );
    }

    fn set_active(&self, active: usize) {
        self.metrics
            .feedback_cycle_active
            .set(i64::try_from(active).unwrap_or(i64::MAX));
    }

    async fn wait_cycle(
        &self,
        lease: &mut FeedbackCycleLeaseGuard,
        heartbeat: &mut Interval,
        wake: &mut FeedbackWakeReceiver,
        shutdown: &CancellationToken,
    ) -> WaitState {
        tokio::select! {
            () = shutdown.cancelled() => WaitState::Stop,
            () = wake.wait() => WaitState::Continue,
            () = sleep(self.config.poll_interval()) => WaitState::Continue,
            _ = heartbeat.tick() => {
                match self
                    .cycles
                    .renew_cycle_lease(*lease, self.config.lease_ttl().as_secs())
                    .await
                {
                    Ok(cycle) => {
                        *lease = lease.with_generation(cycle.generation);
                        WaitState::Continue
                    }
                    Err(StorageError::StateConflict { .. } | StorageError::IllegalTransition { .. }) => {
                        WaitState::LeaseLost
                    }
                    Err(error) => {
                        warn!(%error, "feedback-cycle lease renewal failed; retaining poll backoff");
                        WaitState::Continue
                    }
                }
            }
        }
    }

    fn finished_at(job: &ResearchJobInfo) -> Result<DateTime<Utc>, FeedbackError> {
        job.finished_at
            .ok_or_else(|| FeedbackError::InvalidCoordinatorState {
                detail: format!("terminal job {} has no finished_at", job.job_id),
            })
    }

    fn job_reason(job: &ResearchJobInfo) -> Result<String, FeedbackError> {
        job.error_json
            .as_ref()
            .map(|error| format!("research_job.{}", error.code.as_str()))
            .ok_or_else(|| FeedbackError::InvalidCoordinatorState {
                detail: format!(
                    "terminal unsuccessful job {} has no typed error",
                    job.job_id
                ),
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileState {
    Progressed,
    Waiting,
    Finished,
    LeaseLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitState {
    Continue,
    Stop,
    LeaseLost,
}

#[derive(Debug, Clone, Copy)]
struct RecoveryMarker {
    occurred_at: DateTime<Utc>,
}

impl RecoveryMarker {
    fn from_claim(claim: &FeedbackCycleClaim) -> Option<Self> {
        (claim.mode == FeedbackCycleClaimMode::LeaseRecovered).then_some(Self {
            occurred_at: claim.cycle.updated_at,
        })
    }
}

#[derive(Debug)]
pub(crate) struct FeedbackTimeline {
    position: TimelinePosition,
    next_sequence: i64,
    cancel_reason: Option<String>,
    events: Vec<FeedbackStageEventInfo>,
}

impl FeedbackTimeline {
    pub(crate) fn parse(events: &[FeedbackStageEventInfo]) -> Result<Self, FeedbackError> {
        let Some(trigger) = events.first() else {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "feedback cycle has no trigger evidence".to_owned(),
            });
        };
        if trigger.event_sequence != 1
            || trigger.stage != FeedbackStage::Trigger
            || trigger.event_kind != FeedbackStageEventKind::Triggered
        {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "feedback timeline must begin with sequence-1 Triggered evidence"
                    .to_owned(),
            });
        }

        let stage = FeedbackStage::Trigger.next().ok_or_else(|| {
            FeedbackError::InvalidCoordinatorState {
                detail: "trigger has no executable successor".to_owned(),
            }
        })?;
        let mut scan = TimelineScan::new(stage);

        for (index, event) in events.iter().enumerate() {
            event
                .validate()
                .map_err(|error| FeedbackError::InvalidCoordinatorState {
                    detail: format!(
                        "feedback timeline event {} failed integrity validation: {error}",
                        event.event_sequence
                    ),
                })?;
            let expected_sequence =
                i64::try_from(index + 1).map_err(|_| FeedbackError::InvalidCoordinatorState {
                    detail: "feedback timeline exceeds signed sequence capacity".to_owned(),
                })?;
            if event.event_sequence != expected_sequence {
                return Err(FeedbackError::InvalidCoordinatorState {
                    detail: format!(
                        "feedback timeline sequence gap: expected {expected_sequence}, found {}",
                        event.event_sequence
                    ),
                });
            }
            if index == 0 {
                continue;
            }
            scan.apply(event)?;
        }

        let (position, cancel_reason) = scan.finish()?;
        let next_sequence = i64::try_from(events.len() + 1).map_err(|_| {
            FeedbackError::InvalidCoordinatorState {
                detail: "feedback timeline exceeds signed sequence capacity".to_owned(),
            }
        })?;
        Ok(Self {
            position,
            next_sequence,
            cancel_reason,
            events: events.to_vec(),
        })
    }

    #[must_use]
    pub(crate) const fn next_sequence(&self) -> i64 {
        self.next_sequence
    }

    #[must_use]
    pub(crate) const fn stage(&self) -> FeedbackStage {
        match &self.position {
            TimelinePosition::Active { stage, .. }
            | TimelinePosition::Succeeded { stage, .. }
            | TimelinePosition::Failed { stage, .. }
            | TimelinePosition::Cancelled { stage, .. } => *stage,
        }
    }

    fn require_job(
        event: &FeedbackStageEventInfo,
        stage: FeedbackStage,
        job_id: Option<ResearchJobId>,
    ) -> Result<(), FeedbackError> {
        if event.stage == stage && event.research_job_id == job_id && job_id.is_some() {
            Ok(())
        } else {
            Err(FeedbackError::InvalidCoordinatorState {
                detail: format!(
                    "event {} does not match active stage/job lineage",
                    event.event_sequence
                ),
            })
        }
    }

    fn matches_success(
        succeeded: Option<&FeedbackStageEventInfo>,
        event: &FeedbackStageEventInfo,
    ) -> bool {
        succeeded.is_some_and(|success| {
            event.stage == success.stage && event.research_job_id == success.research_job_id
        })
    }

    fn matches_terminal(
        terminal: Option<&FeedbackStageEventInfo>,
        event: &FeedbackStageEventInfo,
    ) -> bool {
        terminal.is_some_and(|terminal| {
            event.stage == terminal.stage && event.research_job_id == terminal.research_job_id
        })
    }

    const fn audit_job(&self) -> Option<(FeedbackStage, ResearchJobId)> {
        match &self.position {
            TimelinePosition::Active {
                stage,
                job_id: Some(job_id),
            }
            | TimelinePosition::Succeeded { stage, job_id, .. }
            | TimelinePosition::Failed { stage, job_id, .. }
            | TimelinePosition::Cancelled { stage, job_id, .. } => Some((*stage, *job_id)),
            TimelinePosition::Active { job_id: None, .. } => None,
        }
    }

    fn has_recovery(
        &self,
        stage: FeedbackStage,
        job_id: ResearchJobId,
        occurred_at: DateTime<Utc>,
    ) -> bool {
        Self::has_event(
            &self.events,
            stage,
            job_id,
            FeedbackStageEventKind::LeaseRecovered,
            occurred_at,
        )
    }

    fn has_event(
        events: &[FeedbackStageEventInfo],
        stage: FeedbackStage,
        job_id: ResearchJobId,
        kind: FeedbackStageEventKind,
        occurred_at: DateTime<Utc>,
    ) -> bool {
        events.iter().any(|event| {
            event.stage == stage
                && event.research_job_id == Some(job_id)
                && event.event_kind == kind
                && event.occurred_at == occurred_at
        })
    }
}

#[derive(Debug)]
struct TimelineScan {
    stage: FeedbackStage,
    job_id: Option<ResearchJobId>,
    succeeded: Option<FeedbackStageEventInfo>,
    terminal: Option<FeedbackStageEventInfo>,
    cancel_reason: Option<String>,
}

impl TimelineScan {
    const fn new(stage: FeedbackStage) -> Self {
        Self {
            stage,
            job_id: None,
            succeeded: None,
            terminal: None,
            cancel_reason: None,
        }
    }

    fn apply(&mut self, event: &FeedbackStageEventInfo) -> Result<(), FeedbackError> {
        if event.event_kind == FeedbackStageEventKind::CancellationRequested {
            return self.cancel(event);
        }
        if self.terminal.is_some() {
            if event.event_kind == FeedbackStageEventKind::LeaseRecovered
                && FeedbackTimeline::matches_terminal(self.terminal.as_ref(), event)
            {
                return Ok(());
            }
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "feedback timeline contains stage work after a terminal event".to_owned(),
            });
        }
        match event.event_kind {
            FeedbackStageEventKind::JobLinked => {
                if event.stage != self.stage || self.job_id.is_some() {
                    return Err(FeedbackError::InvalidCoordinatorState {
                        detail: "job link does not begin the active stage exactly once".to_owned(),
                    });
                }
                self.job_id = event.research_job_id;
                self.succeeded = None;
            }
            FeedbackStageEventKind::Started => {
                FeedbackTimeline::require_job(event, self.stage, self.job_id)?;
            }
            FeedbackStageEventKind::LeaseRecovered => {
                if self.job_id.is_some() {
                    FeedbackTimeline::require_job(event, self.stage, self.job_id)?;
                } else if !FeedbackTimeline::matches_success(self.succeeded.as_ref(), event) {
                    return Err(FeedbackError::InvalidCoordinatorState {
                        detail: "lease recovery evidence has no active or succeeded stage job"
                            .to_owned(),
                    });
                }
            }
            FeedbackStageEventKind::Succeeded => {
                FeedbackTimeline::require_job(event, self.stage, self.job_id)?;
                self.succeeded = Some(event.clone());
                self.job_id = None;
                if let Some(next) = self.stage.next() {
                    self.stage = next;
                }
            }
            FeedbackStageEventKind::Failed | FeedbackStageEventKind::Cancelled => {
                FeedbackTimeline::require_job(event, self.stage, self.job_id)?;
                self.terminal = Some(event.clone());
                self.job_id = None;
            }
            FeedbackStageEventKind::Triggered | FeedbackStageEventKind::CancellationRequested => {
                return Err(FeedbackError::InvalidCoordinatorState {
                    detail: "trigger/cancellation event appeared in an invalid branch".to_owned(),
                });
            }
        }
        Ok(())
    }

    fn cancel(&mut self, event: &FeedbackStageEventInfo) -> Result<(), FeedbackError> {
        if event.stage != self.stage {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: format!(
                    "cancellation evidence stage {} differs from active stage {}",
                    event.stage, self.stage
                ),
            });
        }
        let reason =
            event
                .reason_code
                .clone()
                .ok_or_else(|| FeedbackError::InvalidCoordinatorState {
                    detail: "cancellation evidence lost its reason".to_owned(),
                })?;
        if self
            .cancel_reason
            .as_ref()
            .is_some_and(|stored| stored != &reason)
        {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "feedback cycle contains conflicting cancellation reasons".to_owned(),
            });
        }
        self.cancel_reason = Some(reason);
        Ok(())
    }

    fn finish(self) -> Result<(TimelinePosition, Option<String>), FeedbackError> {
        let position = if let Some(event) = self.terminal {
            let job_id =
                event
                    .research_job_id
                    .ok_or_else(|| FeedbackError::InvalidCoordinatorState {
                        detail: "terminal stage event lost its job id".to_owned(),
                    })?;
            let reason_code =
                event
                    .reason_code
                    .ok_or_else(|| FeedbackError::InvalidCoordinatorState {
                        detail: "terminal stage event lost its reason code".to_owned(),
                    })?;
            if event.event_kind == FeedbackStageEventKind::Failed {
                TimelinePosition::Failed {
                    stage: event.stage,
                    job_id,
                    reason_code,
                }
            } else {
                TimelinePosition::Cancelled {
                    stage: event.stage,
                    job_id,
                    reason_code,
                }
            }
        } else if let Some(event) = self.succeeded {
            TimelinePosition::Succeeded {
                stage: event.stage,
                job_id: event.research_job_id.ok_or_else(|| {
                    FeedbackError::InvalidCoordinatorState {
                        detail: "succeeded stage event lost its job id".to_owned(),
                    }
                })?,
                evidence_uri: event.evidence_uri.ok_or_else(|| {
                    FeedbackError::InvalidCoordinatorState {
                        detail: "succeeded stage event lost its evidence URI".to_owned(),
                    }
                })?,
                evidence_hash: event.evidence_hash.ok_or_else(|| {
                    FeedbackError::InvalidCoordinatorState {
                        detail: "succeeded stage event lost its evidence hash".to_owned(),
                    }
                })?,
            }
        } else {
            TimelinePosition::Active {
                stage: self.stage,
                job_id: self.job_id,
            }
        };
        Ok((position, self.cancel_reason))
    }
}

#[derive(Debug)]
enum TimelinePosition {
    Active {
        stage: FeedbackStage,
        job_id: Option<ResearchJobId>,
    },
    Succeeded {
        stage: FeedbackStage,
        job_id: ResearchJobId,
        evidence_uri: ArtifactUri,
        evidence_hash: ContentHash,
    },
    Failed {
        stage: FeedbackStage,
        job_id: ResearchJobId,
        reason_code: String,
    },
    Cancelled {
        stage: FeedbackStage,
        job_id: ResearchJobId,
        reason_code: String,
    },
}
