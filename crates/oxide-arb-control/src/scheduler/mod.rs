//! Offline materialization scheduler — enqueue-only cadence driver.
//!
//! [`MaterializationScheduler`] turns a [`SchedulePolicy`] into queued
//! materialization runs. It deliberately depends on **only** a
//! [`ControlFactorRepository`] and the policy: it never executes runs, never
//! resolves point-in-time inputs, and — most importantly for capital safety —
//! **never publishes**. Each [`tick`](MaterializationScheduler::tick) takes an
//! injected `now`, so the whole driver is unit-testable with a mock repository.
//!
//! The downstream execute worker (which drains `Queued` runs via
//! `MaterializationRunner::execute_run`) and the periodic tick loop are Phase 6
//! process wiring; see `docs/plans/phase6-web-layer.md` §13.5.

mod policy;

pub use policy::{
    ScheduleActivation, ScheduleInactiveReason, ScheduleModeContract, SchedulePolicy,
    ScheduledMaterialization, StaleSeverity, is_due, is_overdue, staleness,
    staleness_threshold_secs,
};

use std::sync::Arc;

use chrono::{DateTime, Utc};
use oxide_arb_models::{
    domain::control_factor::{
        EnqueueMaterializationRunOptions, EnqueueMaterializationRunOutcome,
        NewControlFactorMaterializationRun, RunTrigger,
    },
    enums::control_factor::{MaterializationRunKind, MaterializationRunStatus},
    types::MaterializationRunId,
};
use oxide_arb_repository::traits::ControlFactorRepository;

use crate::{
    materialization::{
        ManifestBuilder, ManifestBuilderInput, MaterializationResult, SealedMaterializationManifest,
    },
    scheduler::policy::ScheduledMaterialization as Task,
};

/// Terminal run statuses considered "successful" for staleness evaluation.
const SUCCESS_STATUSES: &[MaterializationRunStatus] = &[
    MaterializationRunStatus::Completed,
    MaterializationRunStatus::CompletedWithRejectedFactors,
    MaterializationRunStatus::ReportOnly,
];

/// Outcome of evaluating one scheduled cadence during a tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleOutcome {
    /// A new `Queued` run was enqueued for the cadence.
    Enqueued {
        schedule_id: String,
        run_id: MaterializationRunId,
    },
    /// A run for this cadence is already active (`Queued`/`Running`); no
    /// duplicate was enqueued.
    DuplicateActive {
        schedule_id: String,
        run_id: MaterializationRunId,
    },
    /// The cadence has not elapsed since the last run.
    NotDue { schedule_id: String },
    /// The cadence is intentionally inactive for the current execution mode.
    Inactive {
        schedule_id: String,
        reason: ScheduleInactiveReason,
    },
    /// Sealing the manifest or building the insert row failed; nothing was
    /// enqueued. Carries the stable failure code for triage.
    BuildFailed { schedule_id: String, code: String },
}

/// Data-only alert emitted by the scheduler for a missed or stale cadence.
///
/// The scheduler returns alerts as data; Phase 6 process wiring maps them onto
/// the `AlertDispatcher`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleAlert {
    /// The most recent run (any status) is older than the overdue threshold.
    Overdue {
        schedule_id: String,
        last_run_at: DateTime<Utc>,
    },
    /// No successful run exists within the staleness threshold.
    Stale {
        schedule_id: String,
        last_success_at: Option<DateTime<Utc>>,
        threshold_secs: u64,
    },
}

impl ScheduleAlert {
    /// Stable suffix for scheduler alert idempotency keys.
    pub fn idempotency_suffix(&self) -> String {
        match self {
            Self::Overdue { schedule_id, .. } => format!("{schedule_id}.overdue"),
            Self::Stale { schedule_id, .. } => format!("{schedule_id}.stale"),
        }
    }

    /// Human-readable title and body for operator notifications.
    pub fn operator_message(&self) -> (String, String) {
        match self {
            Self::Overdue {
                schedule_id,
                last_run_at,
            } => (
                "Materialization cadence overdue".to_owned(),
                format!("schedule {schedule_id} last ran at {last_run_at}"),
            ),
            Self::Stale {
                schedule_id,
                last_success_at,
                threshold_secs,
            } => (
                "Materialization cadence stale".to_owned(),
                format!(
                    "schedule {schedule_id} no success within {threshold_secs}s \
                     (last success: {last_success_at:?})"
                ),
            ),
        }
    }
}

/// Result of a single scheduler tick across all policy tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerCycleReport {
    pub evaluated_at: DateTime<Utc>,
    pub outcomes: Vec<ScheduleOutcome>,
    pub alerts: Vec<ScheduleAlert>,
}

/// Enqueue-only driver over a [`SchedulePolicy`].
pub struct MaterializationScheduler {
    repo: Arc<dyn ControlFactorRepository>,
    policy: SchedulePolicy,
}

impl MaterializationScheduler {
    #[must_use]
    pub fn new(repo: Arc<dyn ControlFactorRepository>, policy: SchedulePolicy) -> Self {
        Self { repo, policy }
    }

    /// Evaluates every scheduled cadence against `now`, enqueuing due runs and
    /// collecting overdue / stale alerts. Never publishes.
    pub async fn tick(&self, now: DateTime<Utc>) -> MaterializationResult<SchedulerCycleReport> {
        let mut outcomes = Vec::with_capacity(self.policy.tasks.len());
        let mut alerts = Vec::new();
        for task in &self.policy.tasks {
            if let ScheduleActivation::Inactive { reason } = task.activation {
                outcomes.push(ScheduleOutcome::Inactive {
                    schedule_id: task.schedule_id.clone(),
                    reason,
                });
                continue;
            }
            let latest_any = self
                .repo
                .latest_run_for_schedule(&task.schedule_id, &[])
                .await?;
            let last_run_at = latest_any.as_ref().map(|run| run.created_at);

            let outcome =
                if let Some(active) = latest_any.as_ref().filter(|run| is_active(run.status)) {
                    ScheduleOutcome::DuplicateActive {
                        schedule_id: task.schedule_id.clone(),
                        run_id: active.materialization_run_id.clone(),
                    }
                } else if is_due(now, last_run_at, task.cadence) {
                    self.enqueue_due(task, now).await?
                } else {
                    ScheduleOutcome::NotDue {
                        schedule_id: task.schedule_id.clone(),
                    }
                };
            outcomes.push(outcome);

            self.collect_alerts(task, now, last_run_at, &mut alerts)
                .await?;
        }
        Ok(SchedulerCycleReport {
            evaluated_at: now,
            outcomes,
            alerts,
        })
    }

    async fn enqueue_due(
        &self,
        task: &Task,
        now: DateTime<Utc>,
    ) -> MaterializationResult<ScheduleOutcome> {
        let sealed = match self.seal_manifest(task, now) {
            Ok(sealed) => sealed,
            Err(error) => {
                return Ok(ScheduleOutcome::BuildFailed {
                    schedule_id: task.schedule_id.clone(),
                    code: error.failure_code(),
                });
            }
        };
        let new_run = match NewControlFactorMaterializationRun::try_from(&sealed) {
            Ok(run) => run,
            Err(error) => {
                return Ok(ScheduleOutcome::BuildFailed {
                    schedule_id: task.schedule_id.clone(),
                    code: error.failure_code(),
                });
            }
        };
        let outcome = self
            .repo
            .enqueue_materialization_run(
                new_run,
                EnqueueMaterializationRunOptions {
                    force_new_run: false,
                    reason: None,
                },
            )
            .await?;
        Ok(match outcome {
            EnqueueMaterializationRunOutcome::Created(run) => ScheduleOutcome::Enqueued {
                schedule_id: task.schedule_id.clone(),
                run_id: run.materialization_run_id,
            },
            EnqueueMaterializationRunOutcome::DuplicateActive(run) => {
                ScheduleOutcome::DuplicateActive {
                    schedule_id: task.schedule_id.clone(),
                    run_id: run.materialization_run_id,
                }
            }
            // A completed run already covered this window; treat as not due.
            EnqueueMaterializationRunOutcome::DuplicateCompleted(_) => ScheduleOutcome::NotDue {
                schedule_id: task.schedule_id.clone(),
            },
        })
    }

    fn seal_manifest(
        &self,
        task: &Task,
        now: DateTime<Utc>,
    ) -> MaterializationResult<SealedMaterializationManifest> {
        ManifestBuilder::new(ManifestBuilderInput {
            run_kind: MaterializationRunKind::Scheduled,
            trigger: RunTrigger::Scheduled {
                schedule_id: task.schedule_id.clone(),
            },
            trigger_time: now,
            interval: task.cadence,
            source_delay_secs: task.source_delay_secs,
            markets: task.markets.clone(),
            replay_account_scope: task.replay_account_scope.clone(),
            requested_factor_types: task.requested_factor_types.clone(),
            data_requirements: task.data_requirements.clone(),
            runtime_config_ref: task.runtime_config_ref.clone(),
            simulation_config: task.simulation_config.clone(),
            quality_gate_policy: task.quality_gate_policy.clone(),
            output_policy: task.output_policy,
            code_git_sha: self.policy.code_git_sha.clone(),
            created_by: self.policy.created_by.clone(),
            created_at: now,
        })
        .build()
    }

    async fn collect_alerts(
        &self,
        task: &Task,
        now: DateTime<Utc>,
        last_run_at: Option<DateTime<Utc>>,
        alerts: &mut Vec<ScheduleAlert>,
    ) -> MaterializationResult<()> {
        let success = self
            .repo
            .latest_run_for_schedule(&task.schedule_id, SUCCESS_STATUSES)
            .await?;
        let last_success_at = success
            .as_ref()
            .map(|run| run.finished_at.unwrap_or(run.created_at));
        if last_run_at.is_some() && staleness(now, last_success_at, task.cadence).is_some() {
            alerts.push(ScheduleAlert::Stale {
                schedule_id: task.schedule_id.clone(),
                last_success_at,
                threshold_secs: staleness_threshold_secs(task.cadence),
            });
        }
        if let Some(last) = last_run_at
            && is_overdue(now, last, task.cadence)
        {
            alerts.push(ScheduleAlert::Overdue {
                schedule_id: task.schedule_id.clone(),
                last_run_at: last,
            });
        }
        Ok(())
    }
}

const fn is_active(status: MaterializationRunStatus) -> bool {
    matches!(
        status,
        MaterializationRunStatus::Queued | MaterializationRunStatus::Running
    )
}

#[cfg(test)]
mod tests;
