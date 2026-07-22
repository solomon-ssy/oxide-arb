//! PostgreSQL-backed report schedule coordinator and global build worker.

use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_error::{QuantError, QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::quant::{
        ClaimReportSchedule, MaterializeReportSchedule, ReconcileReportSchedule,
        ReportRunClaimConfig, ReportRunInfo,
    },
    hashing::CanonicalDigest,
    runtime_config::{
        DecisionPolicySnapshot, ReportScheduleConfig, ReportsConfig, due_schedule_window,
        preview_fire_times,
    },
    types::{DecisionPolicySnapshotId, WorkerId},
};
use quant_pivot_repository::traits::{PolicyRepository, ReportRunRepository};
use tokio::time::{Instant, interval_at};
use tokio_util::sync::CancellationToken;

use super::{ReportLifecycleService, publisher::ReportPublisher};

/// Structural settings for the durable coordinator.
#[derive(Debug, Clone, Copy)]
pub struct ReportCoordinatorConfig {
    pub poll_secs: u64,
    pub lease_secs: u64,
    pub heartbeat_secs: u64,
    pub ad_hoc_ttl_secs: u64,
}

/// Multi-replica-safe coordinator. `PostgreSQL` owns every cursor, queue row, and lease.
pub struct ReportCoordinator {
    runs: Arc<dyn ReportRunRepository>,
    runtime_configs: Arc<dyn PolicyRepository>,
    lifecycle: Arc<ReportLifecycleService>,
    publisher: Arc<ReportPublisher>,
    config: ReportCoordinatorConfig,
    worker_id: WorkerId,
}

impl ReportCoordinator {
    #[must_use]
    pub fn new(
        runs: Arc<dyn ReportRunRepository>,
        runtime_configs: Arc<dyn PolicyRepository>,
        lifecycle: Arc<ReportLifecycleService>,
        publisher: Arc<ReportPublisher>,
        config: ReportCoordinatorConfig,
    ) -> Self {
        Self {
            runs,
            runtime_configs,
            lifecycle,
            publisher,
            config,
            worker_id: WorkerId::from_v7(),
        }
    }

    /// Poll durable state until shutdown. A failed pass is isolated and retried;
    /// no failure can erase a cursor or tear down unrelated runtime tasks.
    pub async fn run(&self, shutdown: CancellationToken) -> QuantResult<()> {
        let cadence = Duration::from_secs(self.config.poll_secs);
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            if let Err(error) = Box::pin(self.run_once()).await {
                tracing::error!(%error, "durable report coordinator pass failed");
            }
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                () = tokio::time::sleep(cadence) => {}
            }
        }
    }

    async fn run_once(&self) -> QuantResult<()> {
        let (version_id, config) = self.reconcile_active(None).await?;
        let database_now = self.runs.database_time().await?;
        self.materialize_due(&version_id, &config, database_now)
            .await?;
        for abandoned in self.runs.abandon_expired_runs().await? {
            self.publisher.publish_run(&abandoned, database_now);
        }
        let claim_config = claim_config(&version_id, &config)?;
        if let Some(run) = self
            .runs
            .claim_next_run(
                self.worker_id,
                self.config.lease_secs,
                self.config.ad_hoc_ttl_secs,
                claim_config,
            )
            .await?
        {
            self.publisher.publish_run(&run, Utc::now());
            Box::pin(self.execute_with_heartbeat(run)).await?;
        }
        let health = self.runs.schedule_health().await?;
        self.publisher.record_schedule_health(&health)?;
        Ok(())
    }

    async fn reconcile_active(
        &self,
        expected: Option<&ReportsConfig>,
    ) -> QuantResult<(DecisionPolicySnapshotId, DecisionPolicySnapshot)> {
        let version = self.runtime_configs.load_current().await?.ok_or_else(|| {
            ReportError::InvariantViolation {
                stage: "report_schedule_reconcile",
                detail: "no active runtime config".to_owned(),
            }
        })?;
        let activation = self
            .runtime_configs
            .load_current_activation(None)
            .await?
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "report_schedule_reconcile",
                detail: "active runtime config has no activation row".to_owned(),
            })?;
        if activation.decision_policy_snapshot_id != version.decision_policy_snapshot_id {
            return Err(ReportError::InvariantViolation {
                stage: "report_schedule_reconcile",
                detail: "runtime config version and activation disagree".to_owned(),
            }
            .into());
        }
        let config = version.snapshot;
        if expected.is_some_and(|reports| reports != &config.recommendation.reports) {
            return Err(ReportError::ContractViolation {
                detail: "runtime apply payload differs from the already-active durable config"
                    .to_owned(),
            }
            .into());
        }
        let schedules = reconcile_commands(
            &version.decision_policy_snapshot_id,
            activation.activated_at,
            &config.report_schedule.schedules,
        )?;
        let outcome = self
            .runs
            .reconcile_schedules(&version.decision_policy_snapshot_id, schedules)
            .await?;
        for gap in &outcome.gaps {
            self.publisher.record_schedule_gap(gap)?;
        }
        for skipped in outcome.skipped_runs {
            self.publisher.publish_run(&skipped, Utc::now());
        }
        Ok((version.decision_policy_snapshot_id, config))
    }

    async fn materialize_due(
        &self,
        version_id: &DecisionPolicySnapshotId,
        config: &DecisionPolicySnapshot,
        through: DateTime<Utc>,
    ) -> QuantResult<()> {
        let schedule_by_id = config
            .report_schedule
            .schedules
            .iter()
            .map(|schedule| (schedule.schedule_id.as_str(), schedule))
            .collect::<HashMap<_, _>>();
        for state in self.runs.list_schedule_states().await? {
            if !state.enabled || state.decision_policy_snapshot_id != *version_id {
                continue;
            }
            let schedule = schedule_by_id
                .get(state.schedule_id.as_str())
                .ok_or_else(|| ReportError::InvariantViolation {
                    stage: "report_schedule_materialize",
                    detail: format!(
                        "enabled schedule state {} is absent from active config",
                        state.schedule_id
                    ),
                })?;
            let Some(window) =
                due_schedule_window(&schedule.cadence, state.next_scheduled_for, through)?
            else {
                continue;
            };
            let earlier_missed_count = i64::try_from(window.occurrence_count.saturating_sub(1))
                .map_err(|error| ReportError::NumericOverflow {
                    field: "report_schedule.earlier_missed_count",
                    detail: error.to_string(),
                })?;
            let command = MaterializeReportSchedule {
                schedule_id: state.schedule_id,
                decision_policy_snapshot_id: *version_id,
                spec_hash: state.spec_hash,
                expected_next_scheduled_for: window.first,
                latest_scheduled_for: window.latest,
                next_scheduled_for: window.next,
                earlier_first_scheduled_for: (earlier_missed_count > 0).then_some(window.first),
                earlier_last_scheduled_for: (earlier_missed_count > 0)
                    .then(|| predecessor_occurrence(schedule, window.latest, window.first))
                    .transpose()?,
                earlier_missed_count,
            };
            let outcome = self.runs.materialize_schedule(command).await?;
            for gap in &outcome.gaps {
                self.publisher.record_schedule_gap(gap)?;
            }
            if let Some(skipped) = outcome.skipped_run {
                self.publisher.publish_run(&skipped, Utc::now());
            }
            self.publisher.publish_run(&outcome.run, Utc::now());
        }
        Ok(())
    }

    async fn execute_with_heartbeat(&self, mut run: ReportRunInfo) -> QuantResult<()> {
        let heartbeat = Duration::from_secs(self.config.heartbeat_secs);
        let mut timer = interval_at(Instant::now() + heartbeat, heartbeat);
        let build_run = run.clone();
        let prepared = {
            let future = self.lifecycle.prepare_claimed(&build_run);
            tokio::pin!(future);
            loop {
                tokio::select! {
                    result = &mut future => break result,
                    _ = timer.tick() => {
                        run = self.runs.heartbeat_run(
                            &run.report_run_id,
                            self.worker_id,
                            self.config.lease_secs,
                        ).await.map_err(QuantError::from)?;
                        self.publisher.publish_run(&run, Utc::now());
                    }
                }
            }
        };
        let composed = match prepared {
            Ok(composed) => composed,
            Err(error) => {
                self.lifecycle.fail_claimed_run(&run, &error).await;
                return Err(error);
            }
        };
        if let Err(error) = self.lifecycle.commit_claimed(&run, composed).await {
            self.lifecycle.fail_claimed_run(&run, &error).await;
            return Err(error);
        }
        Ok(())
    }
}

fn reconcile_commands(
    version_id: &DecisionPolicySnapshotId,
    activated_at: DateTime<Utc>,
    schedules: &[ReportScheduleConfig],
) -> QuantResult<Vec<ReconcileReportSchedule>> {
    schedules
        .iter()
        .map(|schedule| {
            let next_scheduled_for = preview_fire_times(&schedule.cadence, activated_at, 1)?
                .into_iter()
                .next()
                .ok_or_else(|| ReportError::InvariantViolation {
                    stage: "report_schedule_reconcile",
                    detail: format!("schedule {} has no next occurrence", schedule.schedule_id),
                })?;
            Ok(ReconcileReportSchedule {
                schedule_id: schedule.schedule_id.clone(),
                decision_policy_snapshot_id: *version_id,
                spec_hash: CanonicalDigest::content_hash_json(schedule)?,
                next_scheduled_for,
                enabled: schedule.enabled,
            })
        })
        .collect()
}

fn claim_config(
    version_id: &DecisionPolicySnapshotId,
    config: &DecisionPolicySnapshot,
) -> QuantResult<ReportRunClaimConfig> {
    let ad_hoc_default_top_n = i32::try_from(config.recommendation.reports.ad_hoc_default_top_n)
        .map_err(|error| ReportError::NumericOverflow {
            field: "reports.ad_hoc_default_top_n",
            detail: error.to_string(),
        })?;
    let ad_hoc_default_knowledge_lag_secs = i64::try_from(
        config
            .recommendation
            .reports
            .ad_hoc_default_knowledge_lag_secs,
    )
    .map_err(|error| ReportError::NumericOverflow {
        field: "reports.ad_hoc_default_knowledge_lag_secs",
        detail: error.to_string(),
    })?;
    let schedules = config
        .report_schedule
        .schedules
        .iter()
        .filter(|schedule| schedule.enabled)
        .map(|schedule| {
            Ok(ClaimReportSchedule {
                schedule_id: schedule.schedule_id.clone(),
                top_n: i32::try_from(schedule.top_n).map_err(|error| {
                    ReportError::NumericOverflow {
                        field: "reports.schedules.top_n",
                        detail: error.to_string(),
                    }
                })?,
                knowledge_lag_secs: i64::try_from(schedule.knowledge_lag_secs).map_err(
                    |error| ReportError::NumericOverflow {
                        field: "reports.schedules.knowledge_lag_secs",
                        detail: error.to_string(),
                    },
                )?,
            })
        })
        .collect::<Result<Vec<_>, ReportError>>()?;
    Ok(ReportRunClaimConfig {
        decision_policy_snapshot_id: *version_id,
        ad_hoc_default_top_n,
        ad_hoc_default_knowledge_lag_secs,
        schedules,
    })
}

fn predecessor_occurrence(
    schedule: &ReportScheduleConfig,
    latest: DateTime<Utc>,
    first: DateTime<Utc>,
) -> QuantResult<DateTime<Utc>> {
    let window = due_schedule_window(
        &schedule.cadence,
        first,
        latest - ChronoDuration::nanoseconds(1),
    )?
    .ok_or_else(|| ReportError::InvariantViolation {
        stage: "report_schedule_materialize",
        detail: format!(
            "schedule {} has no predecessor occurrence",
            schedule.schedule_id
        ),
    })?;
    Ok(window.latest)
}
