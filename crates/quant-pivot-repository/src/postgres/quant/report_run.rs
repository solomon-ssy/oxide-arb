//! `PostgreSQL` durable report-run queue and lease ledger.

use crate::{
    postgres::{
        governance::runtime_config::{acquire_activation_lock, do_load_current},
        primitives,
        query::paginate_mapped,
    },
    traits::ReportRunRepository,
};
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        EnqueueReportRunOutcome, MaterializeReportSchedule, MaterializeReportScheduleOutcome,
        NewReportRun, PageWindow, Paginated, ReconcileReportSchedule,
        ReconcileReportSchedulesOutcome, ReportRunClaimConfig, ReportRunInfo, ReportRunListQuery,
        ReportScheduleGapInfo, ReportScheduleGapListQuery, ReportScheduleHealthInfo,
        ReportScheduleStateInfo,
    },
    entities::{
        quant_recommendation_report, quant_report_run, quant_report_schedule_gap,
        quant_report_schedule_state, research_profile_artifact,
    },
    enums::quant::{
        RecommendationReportStatus, ReportRunStatus, ReportRunTerminalReason,
        ReportScheduleGapReason, ReportTriggerKind,
    },
    types::{
        DecisionPolicySnapshotId, RecommendationReportId, ReportRunId, ReportScheduleGapId,
        ReportScheduleId, WorkerId,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, ExprTrait, IntoActiveModel, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait,
    sea_query::{Alias, Expr, LockBehavior, LockType},
};
use std::collections::{BTreeMap, HashSet};

const REPORT_RUN_QUEUE_LOCK_KEY: i64 = 0x_11_08_51_55;
const REPORT_RUN_CLAIM_LOCK_KEY: i64 = 0x_11_08_43_4c;
const REPORT_SCHEDULE_LOCK_KEY: i64 = 0x_11_08_53_43;
const ERROR_CODE_MAX_CHARS: usize = 128;
const ERROR_SUMMARY_MAX_CHARS: usize = 4_096;

pub struct PgReportRunRepository {
    db: DatabaseConnection,
}

impl PgReportRunRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

fn checked_duration(field: &'static str, secs: u64) -> Result<Duration, StorageError> {
    let secs = i64::try_from(secs).map_err(|error| {
        StorageError::invariant_violation(
            Some(entity::QUANT_REPORT_RUN),
            format!("{field} exceeds i64 seconds: {error}"),
        )
    })?;
    if secs <= 0 {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_REPORT_RUN),
            format!("{field} must be greater than zero"),
        ));
    }
    Ok(Duration::seconds(secs))
}

async fn expire_stale_ad_hoc(
    txn: &DatabaseTransaction,
    now: DateTime<Utc>,
    ttl: Duration,
) -> Result<(), StorageError> {
    let cutoff = now - ttl;
    let rows = quant_report_run::Entity::find()
        .filter(quant_report_run::Column::Status.eq(ReportRunStatus::Queued))
        .filter(quant_report_run::Column::TriggerKind.eq(ReportTriggerKind::AdHoc))
        .filter(quant_report_run::Column::RequestedAt.lte(cutoff))
        .order_by_asc(quant_report_run::Column::RequestedAt)
        .order_by_asc(quant_report_run::Column::ReportRunId)
        .lock_exclusive()
        .all(txn)
        .await
        .map_err(StorageError::from)?;
    for row in rows {
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(ReportRunStatus::Skipped);
        active.terminal_reason = ActiveValue::Set(Some(ReportRunTerminalReason::QueueExpired));
        active.finished_at = ActiveValue::Set(Some(now));
        active.update(txn).await.map_err(StorageError::from)?;
    }
    Ok(())
}

fn validate_new_ad_hoc(run: &NewReportRun) -> Result<(), StorageError> {
    if run.trigger_kind != ReportTriggerKind::AdHoc
        || run.status != ReportRunStatus::Queued
        || run
            .request_id
            .as_ref()
            .is_none_or(|request_id| request_id.as_str().is_empty())
        || run.schedule_id.is_some()
        || run.scheduled_for.is_some()
    {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_REPORT_RUN),
            "ad-hoc enqueue requires queued status, request id, and no schedule occurrence",
        ));
    }
    if run.top_n.is_some_and(|value| value <= 0)
        || run.knowledge_lag_secs.is_some_and(|value| value < 0)
    {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_REPORT_RUN),
            "ad-hoc report overrides must be non-negative and top_n must be positive",
        ));
    }
    Ok(())
}

fn page_condition(query: &ReportRunListQuery) -> sea_orm::Condition {
    sea_orm::Condition::all()
        .add_option(
            query
                .status
                .map(|status| quant_report_run::Column::Status.eq(status)),
        )
        .add_option(
            query
                .trigger_kind
                .map(|kind| quant_report_run::Column::TriggerKind.eq(kind)),
        )
        .add_option(
            query
                .schedule_id
                .as_ref()
                .map(|id| quant_report_run::Column::ScheduleId.eq(id.clone())),
        )
        .add_option(
            query
                .from
                .map(|from| quant_report_run::Column::RequestedAt.gte(from)),
        )
        .add_option(
            query
                .to
                .map(|to| quant_report_run::Column::RequestedAt.lt(to)),
        )
}

fn gap_page_condition(query: &ReportScheduleGapListQuery) -> sea_orm::Condition {
    sea_orm::Condition::all()
        .add_option(
            query
                .schedule_id
                .as_ref()
                .map(|id| quant_report_schedule_gap::Column::ScheduleId.eq(id.clone())),
        )
        .add_option(
            query
                .reason
                .map(|reason| quant_report_schedule_gap::Column::Reason.eq(reason)),
        )
        .add_option(
            query
                .from
                .map(|from| quant_report_schedule_gap::Column::DetectedAt.gte(from)),
        )
        .add_option(
            query
                .to
                .map(|to| quant_report_schedule_gap::Column::DetectedAt.lt(to)),
        )
}

async fn verify_active_config(
    txn: &DatabaseTransaction,
    expected: &DecisionPolicySnapshotId,
) -> Result<(), StorageError> {
    acquire_activation_lock(txn).await?;
    let current = do_load_current(txn).await?.ok_or_else(|| {
        StorageError::state_conflict(
            entity::DECISION_POLICY_SNAPSHOT,
            Option::<&DecisionPolicySnapshotId>::None,
            "no active runtime config",
        )
    })?;
    if current.decision_policy_snapshot_id != *expected {
        return Err(StorageError::state_conflict(
            entity::DECISION_POLICY_SNAPSHOT,
            Some(expected),
            "runtime config changed during report schedule operation",
        ));
    }
    Ok(())
}

async fn skip_queued_schedule(
    txn: &DatabaseTransaction,
    schedule_id: &ReportScheduleId,
    reason: ReportRunTerminalReason,
    occurred_at: DateTime<Utc>,
) -> Result<Option<quant_report_run::Model>, StorageError> {
    let row = quant_report_run::Entity::find()
        .filter(quant_report_run::Column::Status.eq(ReportRunStatus::Queued))
        .filter(quant_report_run::Column::TriggerKind.eq(ReportTriggerKind::Scheduled))
        .filter(quant_report_run::Column::ScheduleId.eq(schedule_id.clone()))
        .lock_exclusive()
        .one(txn)
        .await
        .map_err(StorageError::from)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let mut active = row.into_active_model();
    active.status = ActiveValue::Set(ReportRunStatus::Skipped);
    active.terminal_reason = ActiveValue::Set(Some(reason));
    active.finished_at = ActiveValue::Set(Some(occurred_at));
    active
        .update(txn)
        .await
        .map(Some)
        .map_err(StorageError::from)
}

struct ScheduleGapInsert<'a> {
    schedule_id: &'a ReportScheduleId,
    decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    reason: ReportScheduleGapReason,
    first: DateTime<Utc>,
    last: DateTime<Utc>,
    count: i64,
    detected_at: DateTime<Utc>,
    detail: Option<String>,
}

async fn insert_schedule_gap(
    txn: &DatabaseTransaction,
    gap: ScheduleGapInsert<'_>,
) -> Result<quant_report_schedule_gap::Model, StorageError> {
    if gap.count <= 0 || gap.first > gap.last {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_REPORT_SCHEDULE_GAP),
            "schedule gap requires a positive count and ordered occurrence range",
        ));
    }
    quant_report_schedule_gap::ActiveModel {
        gap_id: ActiveValue::Set(ReportScheduleGapId::from_v7()),
        schedule_id: ActiveValue::Set(gap.schedule_id.clone()),
        decision_policy_snapshot_id: ActiveValue::Set(gap.decision_policy_snapshot_id.clone()),
        reason: ActiveValue::Set(gap.reason),
        first_scheduled_for: ActiveValue::Set(gap.first),
        last_scheduled_for: ActiveValue::Set(gap.last),
        missed_count: ActiveValue::Set(gap.count),
        detected_at: ActiveValue::Set(gap.detected_at),
        detail: ActiveValue::Set(gap.detail),
    }
    .insert(txn)
    .await
    .map_err(StorageError::from)
}

#[async_trait::async_trait]
impl ReportRunRepository for PgReportRunRepository {
    async fn database_time(&self) -> Result<DateTime<Utc>, StorageError> {
        primitives::statement_timestamp(&self.db).await
    }

    async fn reconcile_schedules(
        &self,
        decision_policy_snapshot_id: &DecisionPolicySnapshotId,
        schedules: Vec<ReconcileReportSchedule>,
    ) -> Result<ReconcileReportSchedulesOutcome, StorageError> {
        let mut desired = BTreeMap::new();
        for schedule in schedules {
            if schedule.schedule_id.as_str().trim().is_empty()
                || schedule.decision_policy_snapshot_id != *decision_policy_snapshot_id
                || desired
                    .insert(schedule.schedule_id.clone(), schedule)
                    .is_some()
            {
                return Err(StorageError::invariant_violation(
                    Some(entity::QUANT_REPORT_SCHEDULE_STATE),
                    "schedule reconcile requires unique non-empty ids bound to the active config",
                ));
            }
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        primitives::advisory_xact_lock(&txn, REPORT_SCHEDULE_LOCK_KEY).await?;
        verify_active_config(&txn, decision_policy_snapshot_id).await?;
        let now = primitives::statement_timestamp(&txn).await?;
        let existing = quant_report_schedule_state::Entity::find()
            .order_by_asc(quant_report_schedule_state::Column::ScheduleId)
            .lock_exclusive()
            .all(&txn)
            .await
            .map_err(StorageError::from)?;
        let mut states = Vec::with_capacity(desired.len());
        let mut skipped_runs = Vec::new();
        let mut gaps = Vec::new();
        let mut visited = HashSet::new();

        for row in existing {
            let schedule_id = row.schedule_id.clone();
            let incoming = desired.get(&schedule_id);
            let spec_changed = incoming
                .is_none_or(|spec| spec.spec_hash != row.spec_hash || spec.enabled != row.enabled);
            if spec_changed
                && let Some(skipped) = skip_queued_schedule(
                    &txn,
                    &schedule_id,
                    ReportRunTerminalReason::ScheduleReconfigured,
                    now,
                )
                .await?
            {
                let scheduled_for = skipped.scheduled_for.ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some(entity::QUANT_REPORT_RUN),
                        "queued scheduled run has no scheduled_for",
                    )
                })?;
                let gap = insert_schedule_gap(
                    &txn,
                    ScheduleGapInsert {
                        schedule_id: &schedule_id,
                        decision_policy_snapshot_id,
                        reason: ReportScheduleGapReason::ScheduleReconfigured,
                        first: scheduled_for,
                        last: scheduled_for,
                        count: 1,
                        detected_at: now,
                        detail: Some(
                            "queued occurrence invalidated by active schedule spec".to_owned(),
                        ),
                    },
                )
                .await?;
                skipped_runs.push(skipped.into());
                gaps.push(gap.into());
            }

            let mut active = row.into_active_model();
            if let Some(spec) = incoming {
                visited.insert(schedule_id);
                active.decision_policy_snapshot_id =
                    ActiveValue::Set(decision_policy_snapshot_id.clone());
                active.updated_at = ActiveValue::Set(now);
                if spec_changed {
                    active.spec_hash = ActiveValue::Set(spec.spec_hash.clone());
                    active.next_scheduled_for = ActiveValue::Set(spec.next_scheduled_for);
                    active.last_materialized_for = ActiveValue::Set(None);
                    active.enabled = ActiveValue::Set(spec.enabled);
                }
            } else {
                active.decision_policy_snapshot_id =
                    ActiveValue::Set(decision_policy_snapshot_id.clone());
                active.enabled = ActiveValue::Set(false);
                active.updated_at = ActiveValue::Set(now);
            }
            let updated = active.update(&txn).await.map_err(StorageError::from)?;
            if incoming.is_some() {
                states.push(updated.into());
            }
        }

        for (schedule_id, spec) in desired {
            if visited.contains(&schedule_id) {
                continue;
            }
            let created = quant_report_schedule_state::ActiveModel {
                schedule_id: ActiveValue::Set(schedule_id),
                decision_policy_snapshot_id: ActiveValue::Set(decision_policy_snapshot_id.clone()),
                spec_hash: ActiveValue::Set(spec.spec_hash),
                next_scheduled_for: ActiveValue::Set(spec.next_scheduled_for),
                last_materialized_for: ActiveValue::Set(None),
                enabled: ActiveValue::Set(spec.enabled),
                created_at: ActiveValue::Set(now),
                updated_at: ActiveValue::Set(now),
            }
            .insert(&txn)
            .await
            .map_err(StorageError::from)?;
            states.push(created.into());
        }
        states.sort_by(|left: &ReportScheduleStateInfo, right| {
            left.schedule_id.cmp(&right.schedule_id)
        });
        txn.commit().await.map_err(StorageError::from)?;
        Ok(ReconcileReportSchedulesOutcome {
            states,
            skipped_runs,
            gaps,
        })
    }

    async fn list_schedule_states(&self) -> Result<Vec<ReportScheduleStateInfo>, StorageError> {
        quant_report_schedule_state::Entity::find()
            .order_by_asc(quant_report_schedule_state::Column::ScheduleId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn materialize_schedule(
        &self,
        command: MaterializeReportSchedule,
    ) -> Result<MaterializeReportScheduleOutcome, StorageError> {
        if command.schedule_id.as_str().trim().is_empty()
            || command.latest_scheduled_for < command.expected_next_scheduled_for
            || command.next_scheduled_for <= command.latest_scheduled_for
            || command.earlier_missed_count < 0
            || (command.earlier_missed_count == 0
                && (command.earlier_first_scheduled_for.is_some()
                    || command.earlier_last_scheduled_for.is_some()))
            || (command.earlier_missed_count > 0
                && (command.earlier_first_scheduled_for.is_none()
                    || command.earlier_last_scheduled_for.is_none()))
        {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_REPORT_SCHEDULE_STATE),
                "invalid latest-only schedule materialization window",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        primitives::advisory_xact_lock(&txn, REPORT_SCHEDULE_LOCK_KEY).await?;
        verify_active_config(&txn, &command.decision_policy_snapshot_id).await?;
        let now = primitives::statement_timestamp(&txn).await?;
        let state = quant_report_schedule_state::Entity::find_by_id(command.schedule_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(entity::QUANT_REPORT_SCHEDULE_STATE, &command.schedule_id)
            })?;
        let version_changed =
            state.decision_policy_snapshot_id != command.decision_policy_snapshot_id;
        let spec_changed = state.spec_hash != command.spec_hash;
        let cursor_changed = state.next_scheduled_for != command.expected_next_scheduled_for;
        if !state.enabled || version_changed || spec_changed || cursor_changed {
            return Err(StorageError::state_conflict(
                entity::QUANT_REPORT_SCHEDULE_STATE,
                Some(&command.schedule_id),
                "schedule cursor/spec changed before occurrence materialization",
            ));
        }

        let skipped = skip_queued_schedule(
            &txn,
            &command.schedule_id,
            ReportRunTerminalReason::CoalescedByNewerOccurrence,
            now,
        )
        .await?;
        let mut gaps = Vec::new();
        let skipped_scheduled_for = skipped.as_ref().and_then(|run| run.scheduled_for);
        let missed_count = command.earlier_missed_count + i64::from(skipped.is_some());
        if missed_count > 0 {
            let first = skipped_scheduled_for
                .or(command.earlier_first_scheduled_for)
                .ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some(entity::QUANT_REPORT_SCHEDULE_GAP),
                        "coalesced schedule window has no first occurrence",
                    )
                })?;
            let last = command
                .earlier_last_scheduled_for
                .or(skipped_scheduled_for)
                .ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some(entity::QUANT_REPORT_SCHEDULE_GAP),
                        "coalesced schedule window has no last occurrence",
                    )
                })?;
            let reason = if skipped.is_some() {
                ReportScheduleGapReason::CoalescedByNewerOccurrence
            } else {
                ReportScheduleGapReason::CoordinatorLag
            };
            gaps.push(
                insert_schedule_gap(
                    &txn,
                    ScheduleGapInsert {
                        schedule_id: &command.schedule_id,
                        decision_policy_snapshot_id: &command.decision_policy_snapshot_id,
                        reason,
                        first,
                        last,
                        count: missed_count,
                        detected_at: now,
                        detail: None,
                    },
                )
                .await?
                .into(),
            );
        }

        let new_run = NewReportRun {
            report_run_id: ReportRunId::from_v7(),
            trigger_kind: ReportTriggerKind::Scheduled,
            trigger_key: format!(
                "scheduled:{}:{}",
                command.schedule_id,
                command.latest_scheduled_for.to_rfc3339()
            ),
            schedule_id: Some(command.schedule_id.clone()),
            request_id: None,
            retry_of_run_id: None,
            scheduled_for: Some(command.latest_scheduled_for),
            requested_at: now,
            status: ReportRunStatus::Queued,
            top_n: None,
            knowledge_lag_secs: None,
        };
        let run = new_run
            .into_active_model()
            .insert(&txn)
            .await
            .map_err(StorageError::from)?;
        let mut active_state = state.into_active_model();
        active_state.last_materialized_for = ActiveValue::Set(Some(command.latest_scheduled_for));
        active_state.next_scheduled_for = ActiveValue::Set(command.next_scheduled_for);
        active_state.updated_at = ActiveValue::Set(now);
        let state = active_state
            .update(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(MaterializeReportScheduleOutcome {
            run: run.into(),
            skipped_run: skipped.map(Into::into),
            gaps,
            state: state.into(),
        })
    }

    async fn page_schedule_gaps(
        &self,
        query: ReportScheduleGapListQuery,
    ) -> Result<Paginated<ReportScheduleGapInfo>, StorageError> {
        paginate_mapped(
            quant_report_schedule_gap::Entity::find()
                .filter(gap_page_condition(&query))
                .order_by_desc(quant_report_schedule_gap::Column::DetectedAt)
                .order_by_desc(quant_report_schedule_gap::Column::GapId),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn schedule_health(&self) -> Result<ReportScheduleHealthInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let observed_at = primitives::statement_timestamp(&txn).await?;
        let since = observed_at - Duration::hours(24);
        let active_run = quant_report_run::Entity::find()
            .filter(quant_report_run::Column::Status.eq(ReportRunStatus::Running))
            .order_by_asc(quant_report_run::Column::StartedAt)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .map(Into::into);
        let queued_run_count = quant_report_run::Entity::find()
            .filter(quant_report_run::Column::Status.eq(ReportRunStatus::Queued))
            .count(&txn)
            .await
            .map_err(StorageError::from)?;
        let failed_run_count_24h = quant_report_run::Entity::find()
            .filter(
                quant_report_run::Column::Status
                    .is_in([ReportRunStatus::Failed, ReportRunStatus::Abandoned]),
            )
            .filter(quant_report_run::Column::FinishedAt.gte(since))
            .count(&txn)
            .await
            .map_err(StorageError::from)?;
        let gap_count_24h = quant_report_schedule_gap::Entity::find()
            .filter(quant_report_schedule_gap::Column::DetectedAt.gte(since))
            .count(&txn)
            .await
            .map_err(StorageError::from)?;
        let missed_occurrence_count_24h = quant_report_schedule_gap::Entity::find()
            .filter(quant_report_schedule_gap::Column::DetectedAt.gte(since))
            .select_only()
            .column_as(
                Expr::col(quant_report_schedule_gap::Column::MissedCount)
                    .sum()
                    .cast_as(Alias::new("bigint"))
                    .if_null(0_i64),
                "missed_count",
            )
            .into_tuple::<i64>()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(entity::QUANT_REPORT_SCHEDULE_GAP),
                    "schedule health aggregate returned no row",
                )
            })?;
        let prepared_report_count = quant_recommendation_report::Entity::find()
            .filter(
                quant_recommendation_report::Column::Status
                    .eq(RecommendationReportStatus::Prepared),
            )
            .count(&txn)
            .await
            .map_err(StorageError::from)?;
        let current_reports = quant_recommendation_report::Entity::find()
            .inner_join(research_profile_artifact::Entity)
            .filter(
                quant_recommendation_report::Column::Status
                    .eq(RecommendationReportStatus::Published),
            )
            .order_by_desc(quant_recommendation_report::Column::PublishedAt)
            .order_by_asc(research_profile_artifact::Column::ResearchProfileId)
            .order_by_asc(quant_recommendation_report::Column::ReportKind)
            .all(&txn)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Into::into)
            .collect();
        let schedules = quant_report_schedule_state::Entity::find()
            .order_by_asc(quant_report_schedule_state::Column::ScheduleId)
            .all(&txn)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Into::into)
            .collect();
        txn.commit().await.map_err(StorageError::from)?;
        Ok(ReportScheduleHealthInfo {
            observed_at,
            active_run,
            queued_run_count,
            failed_run_count_24h,
            gap_count_24h,
            missed_occurrence_count_24h,
            prepared_report_count,
            current_reports,
            schedules,
        })
    }

    async fn enqueue_ad_hoc(
        &self,
        run: NewReportRun,
        capacity: u64,
        ttl_secs: u64,
    ) -> Result<EnqueueReportRunOutcome, StorageError> {
        validate_new_ad_hoc(&run)?;
        if capacity == 0 {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_REPORT_RUN),
                "ad-hoc queue capacity must be greater than zero",
            ));
        }
        let ttl = checked_duration("ad_hoc_ttl_secs", ttl_secs)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        primitives::advisory_xact_lock(&txn, REPORT_RUN_QUEUE_LOCK_KEY).await?;
        if let Some(existing) = quant_report_run::Entity::find()
            .filter(quant_report_run::Column::TriggerKey.eq(run.trigger_key.clone()))
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        {
            if existing.trigger_kind != run.trigger_kind
                || existing.request_id != run.request_id
                || existing.retry_of_run_id != run.retry_of_run_id
                || existing.schedule_id != run.schedule_id
                || existing.scheduled_for != run.scheduled_for
                || existing.top_n != run.top_n
                || existing.knowledge_lag_secs != run.knowledge_lag_secs
            {
                return Err(StorageError::state_conflict(
                    entity::QUANT_REPORT_RUN,
                    Some(&existing.report_run_id),
                    "trigger key is already bound to different report request semantics",
                ));
            }
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(EnqueueReportRunOutcome::Existing(existing.into()));
        }
        let now = primitives::statement_timestamp(&txn).await?;
        expire_stale_ad_hoc(&txn, now, ttl).await?;
        let queued = quant_report_run::Entity::find()
            .filter(quant_report_run::Column::Status.eq(ReportRunStatus::Queued))
            .filter(quant_report_run::Column::TriggerKind.eq(ReportTriggerKind::AdHoc))
            .count(&txn)
            .await
            .map_err(StorageError::from)?;
        if queued >= capacity {
            return Err(StorageError::capacity_exceeded(
                entity::QUANT_REPORT_RUN,
                capacity,
            ));
        }
        let mut active = run.into_active_model();
        active.requested_at = ActiveValue::Set(now);
        let created = active.insert(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(EnqueueReportRunOutcome::Created(created.into()))
    }

    async fn find_by_id(
        &self,
        run_id: &ReportRunId,
    ) -> Result<Option<ReportRunInfo>, StorageError> {
        quant_report_run::Entity::find_by_id(run_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_trigger_key(
        &self,
        trigger_key: &str,
    ) -> Result<Option<ReportRunInfo>, StorageError> {
        quant_report_run::Entity::find()
            .filter(quant_report_run::Column::TriggerKey.eq(trigger_key))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_output_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Option<ReportRunInfo>, StorageError> {
        quant_report_run::Entity::find()
            .filter(quant_report_run::Column::OutputReportId.eq(report_id.clone()))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: ReportRunListQuery,
    ) -> Result<Paginated<ReportRunInfo>, StorageError> {
        paginate_mapped(
            quant_report_run::Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(quant_report_run::Column::RequestedAt)
                .order_by_desc(quant_report_run::Column::ReportRunId),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn claim_next_run(
        &self,
        worker_id: WorkerId,
        lease_secs: u64,
        ad_hoc_ttl_secs: u64,
        config: ReportRunClaimConfig,
    ) -> Result<Option<ReportRunInfo>, StorageError> {
        let lease = checked_duration("lease_secs", lease_secs)?;
        let ttl = checked_duration("ad_hoc_ttl_secs", ad_hoc_ttl_secs)?;
        if config.ad_hoc_default_top_n <= 0 || config.ad_hoc_default_knowledge_lag_secs < 0 {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_REPORT_RUN),
                "report-run claim defaults are invalid",
            ));
        }
        let schedules = config
            .schedules
            .iter()
            .map(|schedule| (schedule.schedule_id.as_str(), schedule))
            .collect::<BTreeMap<_, _>>();
        if schedules.len() != config.schedules.len()
            || config
                .schedules
                .iter()
                .any(|schedule| schedule.top_n <= 0 || schedule.knowledge_lag_secs < 0)
        {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_REPORT_RUN),
                "report-run claim schedule inputs are invalid or duplicated",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        primitives::advisory_xact_lock(&txn, REPORT_RUN_CLAIM_LOCK_KEY).await?;
        verify_active_config(&txn, &config.decision_policy_snapshot_id).await?;
        let now = primitives::statement_timestamp(&txn).await?;
        expire_stale_ad_hoc(&txn, now, ttl).await?;
        let running = quant_report_run::Entity::find()
            .filter(quant_report_run::Column::Status.eq(ReportRunStatus::Running))
            .count(&txn)
            .await
            .map_err(StorageError::from)?;
        if running > 0 {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        }
        let Some(row) = quant_report_run::Entity::find()
            .filter(quant_report_run::Column::Status.eq(ReportRunStatus::Queued))
            .order_by_asc(quant_report_run::Column::RequestedAt)
            .order_by_asc(quant_report_run::Column::ReportRunId)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        };
        let (effective_top_n, effective_lag) = match row.trigger_kind {
            ReportTriggerKind::AdHoc => (
                row.top_n.unwrap_or(config.ad_hoc_default_top_n),
                row.knowledge_lag_secs
                    .unwrap_or(config.ad_hoc_default_knowledge_lag_secs),
            ),
            ReportTriggerKind::Scheduled => {
                let schedule_id = row.schedule_id.as_ref().ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some(entity::QUANT_REPORT_RUN),
                        "queued scheduled run has no schedule id",
                    )
                })?;
                let schedule = schedules.get(schedule_id.as_str()).ok_or_else(|| {
                    StorageError::state_conflict(
                        entity::QUANT_REPORT_RUN,
                        Some(&row.report_run_id),
                        "queued run references a schedule absent from the active config",
                    )
                })?;
                (schedule.top_n, schedule.knowledge_lag_secs)
            }
        };
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(ReportRunStatus::Running);
        active.started_at = ActiveValue::Set(Some(now));
        active.decision_at = ActiveValue::Set(Some(now));
        active.heartbeat_at = ActiveValue::Set(Some(now));
        active.lease_expires_at = ActiveValue::Set(Some(now + lease));
        active.lease_owner = ActiveValue::Set(Some(worker_id));
        active.decision_policy_snapshot_id =
            ActiveValue::Set(Some(config.decision_policy_snapshot_id));
        active.top_n = ActiveValue::Set(Some(effective_top_n));
        active.knowledge_lag_secs = ActiveValue::Set(Some(effective_lag));
        let claimed = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(Some(claimed.into()))
    }

    async fn heartbeat_run(
        &self,
        run_id: &ReportRunId,
        worker_id: WorkerId,
        lease_secs: u64,
    ) -> Result<ReportRunInfo, StorageError> {
        let lease = checked_duration("lease_secs", lease_secs)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&txn).await?;
        let row = quant_report_run::Entity::find_by_id(run_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(entity::QUANT_REPORT_RUN, run_id))?;
        if row.status != ReportRunStatus::Running
            || row.lease_owner != Some(worker_id)
            || row.lease_expires_at.is_none_or(|expires| expires <= now)
        {
            return Err(StorageError::state_conflict(
                entity::QUANT_REPORT_RUN,
                Some(run_id),
                "report run lease is not live and owned by this worker",
            ));
        }
        let mut active = row.into_active_model();
        active.heartbeat_at = ActiveValue::Set(Some(now));
        active.lease_expires_at = ActiveValue::Set(Some(now + lease));
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn fail_run(
        &self,
        run_id: &ReportRunId,
        worker_id: WorkerId,
        error_code: &str,
        error_summary: &str,
    ) -> Result<ReportRunInfo, StorageError> {
        let code = error_code.trim();
        let summary = error_summary.trim();
        if code.is_empty() || summary.is_empty() {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_REPORT_RUN),
                "failed report run requires a non-empty error code and summary",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&txn).await?;
        let row = quant_report_run::Entity::find_by_id(run_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(entity::QUANT_REPORT_RUN, run_id))?;
        if row.status != ReportRunStatus::Running
            || row.lease_owner != Some(worker_id)
            || row.lease_expires_at.is_none_or(|expires| expires <= now)
        {
            return Err(StorageError::state_conflict(
                entity::QUANT_REPORT_RUN,
                Some(run_id),
                "report run cannot fail after lease loss",
            ));
        }
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(ReportRunStatus::Failed);
        active.terminal_reason = ActiveValue::Set(Some(ReportRunTerminalReason::BuildFailed));
        active.error_code = ActiveValue::Set(Some(
            code.chars()
                .take(ERROR_CODE_MAX_CHARS)
                .collect::<String>()
                .into(),
        ));
        active.error_summary = ActiveValue::Set(Some(
            summary.chars().take(ERROR_SUMMARY_MAX_CHARS).collect(),
        ));
        active.finished_at = ActiveValue::Set(Some(now));
        active.lease_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn abandon_expired_runs(&self) -> Result<Vec<ReportRunInfo>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&txn).await?;
        let rows = quant_report_run::Entity::find()
            .filter(quant_report_run::Column::Status.eq(ReportRunStatus::Running))
            .filter(quant_report_run::Column::LeaseExpiresAt.lte(now))
            .order_by_asc(quant_report_run::Column::LeaseExpiresAt)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .all(&txn)
            .await
            .map_err(StorageError::from)?;
        let mut abandoned = Vec::with_capacity(rows.len());
        for row in rows {
            let mut active = row.into_active_model();
            active.status = ActiveValue::Set(ReportRunStatus::Abandoned);
            active.terminal_reason = ActiveValue::Set(Some(ReportRunTerminalReason::LeaseExpired));
            active.finished_at = ActiveValue::Set(Some(now));
            active.lease_owner = ActiveValue::Set(None);
            active.lease_expires_at = ActiveValue::Set(None);
            abandoned.push(
                active
                    .update(&txn)
                    .await
                    .map_err(StorageError::from)?
                    .into(),
            );
        }
        txn.commit().await.map_err(StorageError::from)?;
        Ok(abandoned)
    }

    async fn skip_queued_run(
        &self,
        run_id: &ReportRunId,
        reason: ReportRunTerminalReason,
        occurred_at: DateTime<Utc>,
    ) -> Result<ReportRunInfo, StorageError> {
        if !matches!(
            reason,
            ReportRunTerminalReason::CoalescedByNewerOccurrence
                | ReportRunTerminalReason::ScheduleReconfigured
                | ReportRunTerminalReason::QueueExpired
        ) {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_REPORT_RUN),
                "queued report run requires a typed skip reason",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = quant_report_run::Entity::find_by_id(run_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(entity::QUANT_REPORT_RUN, run_id))?;
        if row.status != ReportRunStatus::Queued {
            return Err(StorageError::illegal_transition(
                entity::QUANT_REPORT_RUN,
                Some(run_id),
                row.status,
                ReportRunStatus::Skipped,
            ));
        }
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(ReportRunStatus::Skipped);
        active.terminal_reason = ActiveValue::Set(Some(reason));
        active.finished_at = ActiveValue::Set(Some(occurred_at));
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }
}
