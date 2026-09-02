//! `PostgreSQL` feedback-cycle orchestration and immutable evidence repository.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{
    feedback::FeedbackCycleCommandError,
    storage::{
        StorageError,
        entity::{
            QUANT_DRIFT_REPORT, QUANT_FEEDBACK_COORDINATOR_FAULT, QUANT_FEEDBACK_CYCLE,
            QUANT_FEEDBACK_EVALUATION_USE, QUANT_FEEDBACK_EVENT_OUTBOX,
            QUANT_FEEDBACK_SCHEDULER_STATE, QUANT_FEEDBACK_STAGE_EVENT,
            QUANT_FEEDBACK_TRIGGER_EVENT,
        },
    },
};
use quant_pivot_models::{
    domain::{
        api::{DriftReportListQuery, FeedbackCycleListQuery},
        pagination::{PageRequest, PageWindow, Paginated},
        quant::{
            DriftReportInfo, FeedbackCoordinatorFaultInfo, FeedbackCoordinatorFaultInput,
            FeedbackCoordinatorFaultReason, FeedbackCoordinatorTimelineHead, FeedbackCycleInfo,
            FeedbackCycleTerminal, FeedbackEvaluationUseInfo, FeedbackOutboxEntry,
            FeedbackOutboxSource, FeedbackQueueSnapshot, FeedbackSchedulerStateInfo,
            FeedbackStageEventInfo, FeedbackStageEventInput, FeedbackTriggerEventInfo,
            FeedbackTriggerEventInput, GovernedFeedbackCancellation, GovernedFeedbackTrigger,
            NewDriftReport, NewFeedbackCoordinatorFault, NewFeedbackCycle,
            NewFeedbackEvaluationUse, NewFeedbackStageEvent, NewFeedbackTriggerEvent,
            cadence_cutoff, next_cadence_after,
        },
    },
    entities::{
        quant_drift_report::{
            Column as DriftColumn, Entity as DriftEntity, Model as DriftModel,
            Relation as DriftRelation,
        },
        quant_feedback_coordinator_fault::{
            Column as CoordinatorFaultColumn, Entity as CoordinatorFaultEntity,
            Model as CoordinatorFaultModel,
        },
        quant_feedback_cycle::{
            Column as CycleColumn, Entity as CycleEntity, Model as CycleModel,
            Relation as CycleRelation,
        },
        quant_feedback_evaluation_use::{
            Column as EvaluationColumn, Entity as EvaluationEntity, Model as EvaluationModel,
        },
        quant_feedback_event_outbox::{
            ActiveModel as OutboxActiveModel, Column as OutboxColumn, Entity as OutboxEntity,
            Model as OutboxModel,
        },
        quant_feedback_scheduler_state::{Entity as SchedulerEntity, Model as SchedulerModel},
        quant_feedback_stage_event::{
            Column as StageColumn, Entity as StageEntity, Model as StageModel,
        },
        quant_feedback_trigger_event::{
            Column as TriggerColumn, Entity as TriggerEntity, Model as TriggerModel,
        },
        research_profile_artifact::Column as ProfileColumn,
    },
    enums::{
        quant::{
            FeedbackCycleStatus, FeedbackDecision, FeedbackEvaluationMode, FeedbackStage,
            FeedbackStageEventKind, FeedbackTriggerFamily,
        },
        rbac::{Operation, ResourceType},
    },
    types::{
        FeedbackCycleId, FeedbackStageEventId, FeedbackTriggerEventId, ModelVersionId,
        PolicyIdempotencyKey, ResearchProfileId, WorkerId,
    },
};
use sea_orm::{
    AccessMode, ActiveModelTrait,
    ActiveValue::{NotSet, Set},
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    ExprTrait, IntoActiveModel, IsolationLevel, JoinType, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, QueryTrait, RelationTrait, TransactionTrait, TryInsertResult,
    sea_query::{Expr, LockBehavior, LockType, OnConflict},
};

use crate::{
    postgres::{authorization, primitives, query::paginate_mapped},
    traits::{
        DriftReportWriteOutcome, FeedbackCoordinatorFaultWriteOutcome,
        FeedbackCoordinatorQuarantine, FeedbackCycleCasOutcome, FeedbackCycleClaim,
        FeedbackCycleClaimMode, FeedbackCycleGeneration, FeedbackCycleLeaseGuard,
        FeedbackCycleRepository, FeedbackCycleWriteOutcome, FeedbackEvaluationWriteOutcome,
        FeedbackOutboxRepository, FeedbackStageWriteOutcome, FeedbackTriggerCommit,
        FeedbackTriggerWriteOutcome,
    },
};

/// `PostgreSQL`-backed feedback-cycle repository.
pub struct PgFeedbackCycleRepository {
    db: DatabaseConnection,
}

#[derive(Clone, Copy)]
enum OutboxSourceId {
    Stage(FeedbackStageEventId),
    Trigger(FeedbackTriggerEventId),
}

impl PgFeedbackCycleRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn lease_duration(lease_secs: u64) -> Result<Duration, StorageError> {
        let seconds = i64::try_from(lease_secs).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_CYCLE),
                format!("lease_secs exceeds i64 seconds: {error}"),
            )
        })?;
        if seconds <= 0 {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_CYCLE),
                "lease_secs must be greater than zero",
            ));
        }
        Ok(Duration::seconds(seconds))
    }

    fn next_generation(generation: i64) -> Result<i64, StorageError> {
        generation.checked_add(1).ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_CYCLE),
                "feedback-cycle generation exhausted i64",
            )
        })
    }

    fn insert_applied(
        result: &TryInsertResult<u64>,
        entity: &'static str,
    ) -> Result<bool, StorageError> {
        match result {
            TryInsertResult::Inserted(1) => Ok(true),
            TryInsertResult::Inserted(0) | TryInsertResult::Conflicted => Ok(false),
            TryInsertResult::Inserted(rows) => Err(StorageError::invariant_violation(
                Some(entity),
                format!("single immutable insert affected {rows} rows"),
            )),
            TryInsertResult::Empty => Err(StorageError::invariant_violation(
                Some(entity),
                "non-empty immutable insert produced no statement",
            )),
        }
    }
}

impl PgFeedbackCycleRepository {
    fn cycle_info(row: CycleModel) -> Result<FeedbackCycleInfo, StorageError> {
        let cycle: FeedbackCycleInfo = row.into();
        cycle.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_CYCLE),
                format!("stored feedback cycle failed integrity validation: {error}"),
            )
        })?;
        Ok(cycle)
    }

    fn coordinator_fault_info(
        row: CoordinatorFaultModel,
    ) -> Result<FeedbackCoordinatorFaultInfo, StorageError> {
        let fault: FeedbackCoordinatorFaultInfo = row.into();
        fault.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_COORDINATOR_FAULT),
                format!("stored coordinator fault failed integrity validation: {error}"),
            )
        })?;
        Ok(fault)
    }

    fn stage_info(row: StageModel) -> Result<FeedbackStageEventInfo, StorageError> {
        let event: FeedbackStageEventInfo = row.into();
        event.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_STAGE_EVENT),
                format!("stored stage event failed integrity validation: {error}"),
            )
        })?;
        Ok(event)
    }

    fn trigger_info(row: TriggerModel) -> Result<FeedbackTriggerEventInfo, StorageError> {
        let event: FeedbackTriggerEventInfo = row.into();
        event.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_TRIGGER_EVENT),
                format!("stored trigger event failed integrity validation: {error}"),
            )
        })?;
        Ok(event)
    }

    fn scheduler_info(row: SchedulerModel) -> Result<FeedbackSchedulerStateInfo, StorageError> {
        let state: FeedbackSchedulerStateInfo = row.into();
        state.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_SCHEDULER_STATE),
                format!("stored feedback scheduler state failed integrity validation: {error}"),
            )
        })?;
        Ok(state)
    }

    fn drift_info(row: DriftModel) -> Result<DriftReportInfo, StorageError> {
        let report: DriftReportInfo = row.into();
        report.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_DRIFT_REPORT),
                format!("stored drift report failed integrity validation: {error}"),
            )
        })?;
        Ok(report)
    }

    fn evaluation_info(row: EvaluationModel) -> Result<FeedbackEvaluationUseInfo, StorageError> {
        let evaluation: FeedbackEvaluationUseInfo = row.into();
        evaluation.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_EVALUATION_USE),
                format!("stored evaluation use failed integrity validation: {error}"),
            )
        })?;
        Ok(evaluation)
    }

    fn outbox_limit(limit: u64) -> Result<u64, StorageError> {
        if (1..=1_000).contains(&limit) {
            Ok(limit)
        } else {
            Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                "feedback outbox limit must be between 1 and 1000 inclusive",
            ))
        }
    }

    async fn outbox_entries<C>(
        connection: &C,
        rows: Vec<OutboxModel>,
    ) -> Result<Vec<FeedbackOutboxEntry>, StorageError>
    where
        C: ConnectionTrait,
    {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let stage_ids = rows
            .iter()
            .filter_map(|row| row.feedback_stage_event_id)
            .collect::<Vec<_>>();
        let trigger_ids = rows
            .iter()
            .filter_map(|row| row.feedback_trigger_event_id)
            .collect::<Vec<_>>();
        let mut stages = if stage_ids.is_empty() {
            HashMap::new()
        } else {
            StageEntity::find()
                .filter(StageColumn::FeedbackStageEventId.is_in(stage_ids))
                .all(connection)
                .await
                .map_err(StorageError::from)?
                .into_iter()
                .map(|row| {
                    let event = Self::stage_info(row)?;
                    Ok((event.feedback_stage_event_id, event))
                })
                .collect::<Result<HashMap<_, _>, StorageError>>()?
        };
        let mut triggers = if trigger_ids.is_empty() {
            HashMap::new()
        } else {
            TriggerEntity::find()
                .filter(TriggerColumn::FeedbackTriggerEventId.is_in(trigger_ids))
                .all(connection)
                .await
                .map_err(StorageError::from)?
                .into_iter()
                .map(|row| {
                    let event = Self::trigger_info(row)?;
                    Ok((event.feedback_trigger_event_id, event))
                })
                .collect::<Result<HashMap<_, _>, StorageError>>()?
        };
        let cycle_ids = stages
            .values()
            .map(|event| event.feedback_cycle_id)
            .chain(triggers.values().map(|event| event.feedback_cycle_id))
            .collect::<Vec<_>>();
        let cycles = CycleEntity::find()
            .filter(CycleColumn::FeedbackCycleId.is_in(cycle_ids))
            .all(connection)
            .await
            .map_err(StorageError::from)?;
        let profiles = cycles
            .into_iter()
            .map(|row| {
                let cycle = Self::cycle_info(row)?;
                Ok((cycle.feedback_cycle_id, cycle.profile_ref.id))
            })
            .collect::<Result<HashMap<FeedbackCycleId, ResearchProfileId>, StorageError>>()?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let source = match (row.feedback_stage_event_id, row.feedback_trigger_event_id) {
                (Some(event_id), None) => {
                    let event = stages.remove(&event_id).ok_or_else(|| {
                        StorageError::invariant_violation(
                            Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                            format!(
                                "outbox revision {} references a missing stage event",
                                row.revision
                            ),
                        )
                    })?;
                    FeedbackOutboxSource::Stage(event)
                }
                (None, Some(event_id)) => {
                    let event = triggers.remove(&event_id).ok_or_else(|| {
                        StorageError::invariant_violation(
                            Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                            format!(
                                "outbox revision {} references a missing trigger event",
                                row.revision
                            ),
                        )
                    })?;
                    FeedbackOutboxSource::Trigger(event)
                }
                _ => {
                    return Err(StorageError::invariant_violation(
                        Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                        format!(
                            "outbox revision {} must reference exactly one source event",
                            row.revision
                        ),
                    ));
                }
            };
            let cycle_id = source.feedback_cycle_id();
            let profile_id = profiles.get(&cycle_id).cloned().ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                    format!(
                        "outbox revision {} references a missing feedback cycle",
                        row.revision
                    ),
                )
            })?;
            let entry = FeedbackOutboxEntry {
                revision: row.revision,
                publish_attempts: row.publish_attempts,
                profile_id,
                source,
            };
            entry.validate().map_err(|error| {
                StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                    error.to_string(),
                )
            })?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl PgFeedbackCycleRepository {
    async fn verify_forced_parent(
        transaction: &DatabaseTransaction,
        cycle: &NewFeedbackCycle,
    ) -> Result<(), StorageError> {
        if cycle.evaluation_mode() == FeedbackEvaluationMode::Conditional {
            return Ok(());
        }
        let parent_cycle_id = cycle.parent_cycle_id().ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_CYCLE),
                "forced retraining has no parent cycle",
            )
        })?;
        let parent = CycleEntity::find_by_id(parent_cycle_id)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_FEEDBACK_CYCLE, parent_cycle_id))?;
        let parent = Self::cycle_info(parent)?;
        if parent.evaluation_mode != FeedbackEvaluationMode::Conditional
            || parent.status != FeedbackCycleStatus::Succeeded
            || parent.decision != Some(FeedbackDecision::NoAction)
            || parent.profile_ref != *cycle.profile_ref()
            || parent.label_cutoff != cycle.label_cutoff()
        {
            return Err(StorageError::state_conflict(
                QUANT_FEEDBACK_CYCLE,
                Some(parent_cycle_id),
                "forced retraining parent must be the same profile/cutoff terminal Conditional NoAction cycle",
            ));
        }
        Ok(())
    }

    async fn cycle_candidate(
        transaction: &DatabaseTransaction,
        cycle: &NewFeedbackCycle,
    ) -> Result<Option<FeedbackCycleInfo>, StorageError> {
        let mut identity = Condition::any()
            .add(CycleColumn::FeedbackCycleId.eq(cycle.feedback_cycle_id()))
            .add(CycleColumn::IdempotencyHash.eq(cycle.idempotency_hash()));
        if cycle.evaluation_mode() == FeedbackEvaluationMode::Conditional {
            identity = identity.add(
                Condition::all()
                    .add(
                        CycleColumn::ResearchProfileArtifactId
                            .eq(cycle.research_profile_artifact_id().clone()),
                    )
                    .add(CycleColumn::LabelCutoff.eq(cycle.label_cutoff()))
                    .add(CycleColumn::EvaluationMode.eq(FeedbackEvaluationMode::Conditional)),
            );
        }
        CycleEntity::find()
            .filter(identity)
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .map(Self::cycle_info)
            .transpose()
    }

    async fn persist_cycle(
        transaction: &DatabaseTransaction,
        cycle: &NewFeedbackCycle,
    ) -> Result<FeedbackCycleWriteOutcome, StorageError> {
        let insert = CycleEntity::insert(cycle.clone().into_active_model())
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .try_insert()
            .exec_without_returning(transaction)
            .await
            .map_err(StorageError::from)?;
        let inserted = Self::insert_applied(&insert, QUANT_FEEDBACK_CYCLE)?;
        let stored = Self::cycle_candidate(transaction, cycle)
            .await?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_CYCLE),
                    "cycle conflict completed without an observable row",
                )
            })?;
        if !stored.has_same_identity(cycle) {
            return Err(StorageError::state_conflict(
                QUANT_FEEDBACK_CYCLE,
                Some(cycle.feedback_cycle_id()),
                "cycle id or idempotency hash is bound to different frozen content",
            ));
        }
        if inserted {
            Ok(FeedbackCycleWriteOutcome::Inserted(stored))
        } else {
            Ok(FeedbackCycleWriteOutcome::AlreadyPresent(stored))
        }
    }
}

impl PgFeedbackCycleRepository {
    async fn stage_candidate(
        transaction: &DatabaseTransaction,
        event: &NewFeedbackStageEvent,
    ) -> Result<Option<FeedbackStageEventInfo>, StorageError> {
        StageEntity::find()
            .filter(
                Condition::any()
                    .add(StageColumn::FeedbackStageEventId.eq(event.feedback_stage_event_id()))
                    .add(
                        Condition::all()
                            .add(StageColumn::FeedbackCycleId.eq(event.feedback_cycle_id()))
                            .add(StageColumn::EventSequence.eq(event.event_sequence())),
                    ),
            )
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .map(Self::stage_info)
            .transpose()
    }

    fn exact_stage(
        stored: FeedbackStageEventInfo,
        event: &NewFeedbackStageEvent,
    ) -> Result<FeedbackStageEventInfo, StorageError> {
        if stored.has_same_content(event) {
            Ok(stored)
        } else {
            Err(StorageError::state_conflict(
                QUANT_FEEDBACK_STAGE_EVENT,
                Some(event.feedback_stage_event_id()),
                "event id or cycle sequence is bound to different immutable content",
            ))
        }
    }

    async fn persist_stage_row(
        transaction: &DatabaseTransaction,
        event: &NewFeedbackStageEvent,
    ) -> Result<FeedbackStageWriteOutcome, StorageError> {
        if let Some(stored) = Self::stage_candidate(transaction, event).await? {
            let stored = Self::exact_stage(stored, event)?;
            return Ok(FeedbackStageWriteOutcome::AlreadyPresent(stored));
        }
        let insert = StageEntity::insert(event.clone().into_active_model())
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .try_insert()
            .exec_without_returning(transaction)
            .await
            .map_err(StorageError::from)?;
        let inserted = Self::insert_applied(&insert, QUANT_FEEDBACK_STAGE_EVENT)?;
        let stored = Self::stage_candidate(transaction, event)
            .await?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_STAGE_EVENT),
                    "stage-event conflict completed without an observable row",
                )
            })?;
        let stored = Self::exact_stage(stored, event)?;
        if inserted {
            Ok(FeedbackStageWriteOutcome::Inserted(stored))
        } else {
            Ok(FeedbackStageWriteOutcome::AlreadyPresent(stored))
        }
    }

    async fn persist_stage(
        transaction: &DatabaseTransaction,
        event: &NewFeedbackStageEvent,
    ) -> Result<FeedbackStageWriteOutcome, StorageError> {
        let outcome = Self::persist_stage_row(transaction, event).await?;
        let event_id = match &outcome {
            FeedbackStageWriteOutcome::Inserted(event)
            | FeedbackStageWriteOutcome::AlreadyPresent(event) => event.feedback_stage_event_id,
        };
        Self::persist_stage_outbox(transaction, event_id).await?;
        Ok(outcome)
    }

    async fn persist_initial_stage(
        transaction: &DatabaseTransaction,
        event: &NewFeedbackStageEvent,
    ) -> Result<FeedbackStageWriteOutcome, StorageError> {
        let stored = StageEntity::find()
            .filter(StageColumn::FeedbackCycleId.eq(event.feedback_cycle_id()))
            .filter(StageColumn::EventSequence.eq(1))
            .lock_exclusive()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .map(Self::stage_info)
            .transpose()?;
        if let Some(stored) = stored {
            if stored.stage != FeedbackStage::Trigger
                || stored.event_kind != FeedbackStageEventKind::Triggered
                || stored.event_sequence != 1
            {
                return Err(StorageError::state_conflict(
                    QUANT_FEEDBACK_STAGE_EVENT,
                    Some(stored.feedback_stage_event_id),
                    "cycle sequence one is not a valid lifecycle trigger",
                ));
            }
            return Ok(FeedbackStageWriteOutcome::AlreadyPresent(stored));
        }
        Self::persist_stage_row(transaction, event).await
    }

    async fn persist_trigger_event(
        transaction: &DatabaseTransaction,
        event: &NewFeedbackTriggerEvent,
    ) -> Result<FeedbackTriggerWriteOutcome, StorageError> {
        let candidate = || {
            TriggerEntity::find()
                .filter(
                    Condition::any()
                        .add(
                            TriggerColumn::FeedbackTriggerEventId
                                .eq(event.feedback_trigger_event_id),
                        )
                        .add(TriggerColumn::EventHash.eq(event.event_hash)),
                )
                .lock_exclusive()
                .into_partial_model::<FeedbackTriggerEventInfo>()
                .one(transaction)
        };
        if let Some(stored) = candidate().await.map_err(StorageError::from)? {
            stored.validate().map_err(|error| {
                StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_TRIGGER_EVENT),
                    error.to_string(),
                )
            })?;
            if stored.matches_new(event) {
                return Ok(FeedbackTriggerWriteOutcome::AlreadyPresent(stored));
            }
            return Err(StorageError::state_conflict(
                QUANT_FEEDBACK_TRIGGER_EVENT,
                Some(event.feedback_trigger_event_id),
                "trigger event id or hash is bound to different immutable content",
            ));
        }
        let insert = TriggerEntity::insert(event.clone().into_active_model())
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .try_insert()
            .exec_without_returning(transaction)
            .await
            .map_err(StorageError::from)?;
        let inserted = Self::insert_applied(&insert, QUANT_FEEDBACK_TRIGGER_EVENT)?;
        let stored = candidate()
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_TRIGGER_EVENT),
                    "trigger event insert completed without an observable row",
                )
            })?;
        stored.validate().map_err(|error| {
            StorageError::invariant_violation(Some(QUANT_FEEDBACK_TRIGGER_EVENT), error.to_string())
        })?;
        if stored.matches_new(event) {
            if inserted {
                Ok(FeedbackTriggerWriteOutcome::Inserted(stored))
            } else {
                Ok(FeedbackTriggerWriteOutcome::AlreadyPresent(stored))
            }
        } else {
            Err(StorageError::state_conflict(
                QUANT_FEEDBACK_TRIGGER_EVENT,
                Some(event.feedback_trigger_event_id),
                "concurrent trigger event insert has immutable drift",
            ))
        }
    }

    async fn persist_outbox(
        transaction: &DatabaseTransaction,
        source: OutboxSourceId,
    ) -> Result<(), StorageError> {
        let existing = match source {
            OutboxSourceId::Stage(event_id) => {
                OutboxEntity::find()
                    .filter(OutboxColumn::FeedbackStageEventId.eq(event_id))
                    .one(transaction)
                    .await
            }
            OutboxSourceId::Trigger(event_id) => {
                OutboxEntity::find()
                    .filter(OutboxColumn::FeedbackTriggerEventId.eq(event_id))
                    .one(transaction)
                    .await
            }
        }
        .map_err(StorageError::from)?;
        if existing.is_some() {
            return Ok(());
        }
        let now = primitives::statement_timestamp(transaction).await?;
        let (stage_event_id, trigger_event_id, conflict_column) = match source {
            OutboxSourceId::Stage(event_id) => {
                (Some(event_id), None, OutboxColumn::FeedbackStageEventId)
            }
            OutboxSourceId::Trigger(event_id) => {
                (None, Some(event_id), OutboxColumn::FeedbackTriggerEventId)
            }
        };
        let row = OutboxActiveModel {
            revision: NotSet,
            feedback_stage_event_id: Set(stage_event_id),
            feedback_trigger_event_id: Set(trigger_event_id),
            published_at: Set(None),
            publish_attempts: Set(0),
            claim_owner: Set(None),
            lease_expires_at: Set(None),
            last_error: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        };
        OutboxEntity::insert(row)
            .on_conflict(OnConflict::column(conflict_column).do_nothing().to_owned())
            .exec_without_returning(transaction)
            .await
            .map_err(StorageError::from)?;
        let stored = match source {
            OutboxSourceId::Stage(event_id) => {
                OutboxEntity::find()
                    .filter(OutboxColumn::FeedbackStageEventId.eq(event_id))
                    .one(transaction)
                    .await
            }
            OutboxSourceId::Trigger(event_id) => {
                OutboxEntity::find()
                    .filter(OutboxColumn::FeedbackTriggerEventId.eq(event_id))
                    .one(transaction)
                    .await
            }
        }
        .map_err(StorageError::from)?;
        if stored.is_none() {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                "feedback source event committed without an observable outbox revision",
            ));
        }
        Ok(())
    }

    async fn persist_stage_outbox(
        transaction: &DatabaseTransaction,
        event_id: FeedbackStageEventId,
    ) -> Result<(), StorageError> {
        Self::persist_outbox(transaction, OutboxSourceId::Stage(event_id)).await
    }

    async fn persist_trigger_outbox(
        transaction: &DatabaseTransaction,
        event_id: FeedbackTriggerEventId,
    ) -> Result<(), StorageError> {
        Self::persist_outbox(transaction, OutboxSourceId::Trigger(event_id)).await
    }
}

impl PgFeedbackCycleRepository {
    async fn drift_candidate(
        transaction: &DatabaseTransaction,
        report: &NewDriftReport,
    ) -> Result<Option<DriftReportInfo>, StorageError> {
        DriftEntity::find()
            .filter(
                Condition::any()
                    .add(DriftColumn::DriftReportId.eq(report.drift_report_id()))
                    .add(
                        Condition::all()
                            .add(DriftColumn::FeedbackCycleId.eq(report.feedback_cycle_id()))
                            .add(DriftColumn::Metric.eq(report.metric())),
                    ),
            )
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .map(Self::drift_info)
            .transpose()
    }

    fn exact_drift(
        stored: DriftReportInfo,
        report: &NewDriftReport,
    ) -> Result<DriftReportInfo, StorageError> {
        if stored.has_same_content(report) {
            Ok(stored)
        } else {
            Err(StorageError::state_conflict(
                QUANT_DRIFT_REPORT,
                Some(report.drift_report_id()),
                "report id or cycle metric is bound to different immutable content",
            ))
        }
    }

    async fn persist_drift(
        transaction: &DatabaseTransaction,
        report: &NewDriftReport,
    ) -> Result<DriftReportWriteOutcome, StorageError> {
        if let Some(stored) = Self::drift_candidate(transaction, report).await? {
            return Self::exact_drift(stored, report).map(DriftReportWriteOutcome::AlreadyPresent);
        }
        let insert = DriftEntity::insert(report.clone().into_active_model())
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .try_insert()
            .exec_without_returning(transaction)
            .await
            .map_err(StorageError::from)?;
        let inserted = Self::insert_applied(&insert, QUANT_DRIFT_REPORT)?;
        let stored = Self::drift_candidate(transaction, report)
            .await?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_DRIFT_REPORT),
                    "drift conflict completed without an observable row",
                )
            })?;
        let stored = Self::exact_drift(stored, report)?;
        if inserted {
            Ok(DriftReportWriteOutcome::Inserted(stored))
        } else {
            Ok(DriftReportWriteOutcome::AlreadyPresent(stored))
        }
    }
}

impl PgFeedbackCycleRepository {
    async fn evaluation_candidate(
        transaction: &DatabaseTransaction,
        evaluation: &NewFeedbackEvaluationUse,
    ) -> Result<Option<FeedbackEvaluationUseInfo>, StorageError> {
        EvaluationEntity::find()
            .filter(
                Condition::any()
                    .add(
                        EvaluationColumn::FeedbackEvaluationUseId
                            .eq(evaluation.feedback_evaluation_use_id()),
                    )
                    .add(
                        EvaluationColumn::EvaluationDatasetId
                            .eq(evaluation.evaluation_dataset_id()),
                    )
                    .add(EvaluationColumn::SemanticUseHash.eq(evaluation.semantic_use_hash()))
                    .add(
                        Condition::all()
                            .add(
                                EvaluationColumn::EvaluationDatasetHash
                                    .eq(evaluation.evaluation_dataset_hash()),
                            )
                            .add(
                                EvaluationColumn::EvaluationArtifactBytesHash
                                    .eq(evaluation.evaluation_artifact_bytes_hash()),
                            )
                            .add(
                                EvaluationColumn::CohortManifestHash
                                    .eq(evaluation.cohort_manifest_hash()),
                            ),
                    ),
            )
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .map(Self::evaluation_info)
            .transpose()
    }

    fn exact_evaluation(
        stored: FeedbackEvaluationUseInfo,
        evaluation: &NewFeedbackEvaluationUse,
    ) -> Result<FeedbackEvaluationUseInfo, StorageError> {
        if stored.has_same_content(evaluation) {
            Ok(stored)
        } else {
            Err(StorageError::state_conflict(
                QUANT_FEEDBACK_EVALUATION_USE,
                Some(evaluation.feedback_evaluation_use_id()),
                "evaluation dataset or semantic holdout is already consumed by different content",
            ))
        }
    }

    async fn persist_evaluation(
        transaction: &DatabaseTransaction,
        evaluation: &NewFeedbackEvaluationUse,
    ) -> Result<FeedbackEvaluationWriteOutcome, StorageError> {
        if let Some(stored) = Self::evaluation_candidate(transaction, evaluation).await? {
            return Self::exact_evaluation(stored, evaluation)
                .map(FeedbackEvaluationWriteOutcome::AlreadyPresent);
        }
        let insert = EvaluationEntity::insert(evaluation.clone().into_active_model())
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .try_insert()
            .exec_without_returning(transaction)
            .await
            .map_err(StorageError::from)?;
        let inserted = Self::insert_applied(&insert, QUANT_FEEDBACK_EVALUATION_USE)?;
        let stored = Self::evaluation_candidate(transaction, evaluation)
            .await?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_EVALUATION_USE),
                    "evaluation conflict completed without an observable row",
                )
            })?;
        let stored = Self::exact_evaluation(stored, evaluation)?;
        if inserted {
            Ok(FeedbackEvaluationWriteOutcome::Inserted(stored))
        } else {
            Ok(FeedbackEvaluationWriteOutcome::AlreadyPresent(stored))
        }
    }
}

impl PgFeedbackCycleRepository {
    async fn lock_cycle(
        transaction: &DatabaseTransaction,
        cycle_id: &FeedbackCycleId,
    ) -> Result<CycleModel, StorageError> {
        CycleEntity::find_by_id(*cycle_id)
            .lock_exclusive()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_FEEDBACK_CYCLE, cycle_id))
    }

    fn ensure_generation(cycle: &FeedbackCycleInfo, generation: i64) -> Result<(), StorageError> {
        if cycle.generation == generation {
            Ok(())
        } else {
            Err(StorageError::state_conflict(
                QUANT_FEEDBACK_CYCLE,
                Some(cycle.feedback_cycle_id),
                format!(
                    "generation mismatch: expected {generation}, found {}",
                    cycle.generation
                ),
            ))
        }
    }

    fn ensure_live_lease(
        cycle: &FeedbackCycleInfo,
        lease: FeedbackCycleLeaseGuard,
        now: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        Self::ensure_generation(cycle, lease.expected_generation)?;
        if cycle.status == FeedbackCycleStatus::Running
            && cycle.lease_owner == Some(lease.worker_id)
            && cycle
                .lease_expires_at
                .is_some_and(|expires_at| expires_at > now)
        {
            Ok(())
        } else {
            Err(StorageError::state_conflict(
                QUANT_FEEDBACK_CYCLE,
                Some(cycle.feedback_cycle_id),
                "cycle lease is expired, not running, or owned by another worker",
            ))
        }
    }

    async fn advance_scheduler(
        transaction: &DatabaseTransaction,
        cycle: &NewFeedbackCycle,
        scheduler_row: SchedulerModel,
        scheduler: &FeedbackSchedulerStateInfo,
        now: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        if let Some(lease_expires_at) = scheduler.lease_expires_at
            && lease_expires_at > now
        {
            return Err(StorageError::state_conflict(
                QUANT_FEEDBACK_SCHEDULER_STATE,
                Some(&cycle.profile_ref().id),
                format!(
                    "manual feedback trigger is blocked by a live scheduler lease until {lease_expires_at}"
                ),
            ));
        }
        if let Some(cooldown_until) = scheduler.cooldown_until
            && cooldown_until > now
        {
            return Err(StorageError::state_conflict(
                QUANT_FEEDBACK_SCHEDULER_STATE,
                Some(&cycle.profile_ref().id),
                format!("feedback retraining cooldown remains active until {cooldown_until}"),
            ));
        }
        let revision = scheduler.revision.checked_add(1).ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_SCHEDULER_STATE),
                "feedback scheduler revision overflowed",
            )
        })?;
        let cooldown_until = now
            .checked_add_signed(Duration::seconds(scheduler.cooldown_secs))
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_SCHEDULER_STATE),
                    "feedback scheduler cooldown time overflowed",
                )
            })?;
        let mut active = scheduler_row.into_active_model();
        if cycle.evaluation_mode() == FeedbackEvaluationMode::Conditional {
            let gap_seconds = cycle
                .label_cutoff()
                .signed_duration_since(scheduler.next_due_at)
                .num_seconds();
            if gap_seconds < 0 || gap_seconds.rem_euclid(scheduler.cadence_secs) != 0 {
                return Err(StorageError::state_conflict(
                    QUANT_FEEDBACK_SCHEDULER_STATE,
                    Some(&cycle.profile_ref().id),
                    "manual Conditional cutoff regresses or is not aligned with the due cursor",
                ));
            }
            let skipped = gap_seconds.div_euclid(scheduler.cadence_secs);
            if skipped > 0 {
                active.coalesced_gap_count = Set(scheduler
                    .coalesced_gap_count
                    .checked_add(skipped)
                    .ok_or_else(|| {
                        StorageError::invariant_violation(
                            Some(QUANT_FEEDBACK_SCHEDULER_STATE),
                            "manual scheduler coalesced gap counter overflowed",
                        )
                    })?);
                active.last_coalesced_from = Set(Some(scheduler.next_due_at));
                active.last_coalesced_to = Set(Some(
                    cycle
                        .label_cutoff()
                        .checked_sub_signed(Duration::seconds(scheduler.cadence_secs))
                        .ok_or_else(|| {
                            StorageError::invariant_violation(
                                Some(QUANT_FEEDBACK_SCHEDULER_STATE),
                                "manual scheduler coalesced gap range underflowed",
                            )
                        })?,
                ));
            }
            active.last_cycle_id = Set(Some(cycle.feedback_cycle_id()));
            active.last_cutoff = Set(Some(cycle.label_cutoff()));
            active.next_due_at = Set(next_cadence_after(now, scheduler.cadence_secs).map_err(
                |error| {
                    StorageError::invariant_violation(
                        Some(QUANT_FEEDBACK_SCHEDULER_STATE),
                        error.to_string(),
                    )
                },
            )?);
        }
        active.cooldown_until = Set(Some(cooldown_until));
        active.attempt = Set(0);
        active.retry_at = Set(None);
        active.last_error = Set(None);
        active.revision = Set(revision);
        active.updated_at = Set(now);
        let updated = active
            .update(transaction)
            .await
            .map_err(StorageError::from)?;
        Self::scheduler_info(updated)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl FeedbackCycleRepository for PgFeedbackCycleRepository {
    async fn database_time(&self) -> Result<DateTime<Utc>, StorageError> {
        primitives::statement_timestamp(&self.db).await
    }

    async fn record_trigger(
        &self,
        cycle: NewFeedbackCycle,
        trigger: NewFeedbackStageEvent,
    ) -> Result<FeedbackTriggerCommit, StorageError> {
        let trigger_family = trigger.trigger_family().ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_STAGE_EVENT),
                "record_trigger requires typed trigger provenance",
            )
        })?;
        if trigger.feedback_cycle_id() != cycle.feedback_cycle_id()
            || trigger.event_kind() != FeedbackStageEventKind::Triggered
            || cycle.evaluation_mode() != FeedbackEvaluationMode::Conditional
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_STAGE_EVENT),
                "record_trigger requires matching Conditional Triggered evidence",
            ));
        }
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&transaction).await?;
        if cycle.label_cutoff() > now || trigger.occurred_at() > now {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_CYCLE),
                "cycle cutoff and trigger occurrence cannot be in the database future",
            ));
        }
        let cycle_outcome = Self::persist_cycle(&transaction, &cycle).await?;
        let idempotency_key = format!(
            "system-{}-{}",
            trigger_family.as_str(),
            cycle.feedback_cycle_id()
        )
        .parse::<PolicyIdempotencyKey>()
        .map_err(|error| {
            StorageError::invariant_violation(Some(QUANT_FEEDBACK_TRIGGER_EVENT), error.to_string())
        })?;
        let provenance = NewFeedbackTriggerEvent::try_seal(FeedbackTriggerEventInput {
            feedback_cycle_id: cycle.feedback_cycle_id(),
            trigger_family,
            evaluation_mode: cycle.evaluation_mode(),
            idempotency_key,
            actor_user_id: None,
            actor_label: trigger
                .actor()
                .ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some(QUANT_FEEDBACK_TRIGGER_EVENT),
                        "scheduled trigger has no actor label",
                    )
                })?
                .to_owned(),
            actor_role: None,
            reason_code: trigger
                .reason_code()
                .ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some(QUANT_FEEDBACK_TRIGGER_EVENT),
                        "scheduled trigger has no reason code",
                    )
                })?
                .to_owned(),
        })
        .map_err(|error| {
            StorageError::invariant_violation(Some(QUANT_FEEDBACK_TRIGGER_EVENT), error.to_string())
        })?;
        let trigger_outcome = Self::persist_trigger_event(&transaction, &provenance).await?;
        let trigger_event_id = match &trigger_outcome {
            FeedbackTriggerWriteOutcome::Inserted(event)
            | FeedbackTriggerWriteOutcome::AlreadyPresent(event) => event.feedback_trigger_event_id,
        };
        let event_outcome = Self::persist_initial_stage(&transaction, &trigger).await?;
        Self::persist_trigger_outbox(&transaction, trigger_event_id).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(FeedbackTriggerCommit {
            cycle: cycle_outcome,
            stage: event_outcome,
            trigger: trigger_outcome,
        })
    }

    async fn record_governed_trigger(
        &self,
        command: GovernedFeedbackTrigger,
    ) -> Result<FeedbackTriggerCommit, FeedbackCycleCommandError> {
        command.validate()?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let authorized = authorization::authorize_actor::<FeedbackCycleCommandError>(
            &transaction,
            command.actor.user_id,
            &command.actor.acting_role,
            ResourceType::Materialization,
            Operation::Create,
        )
        .await?;
        let now = primitives::statement_timestamp(&transaction).await?;
        if command.cycle.label_cutoff() > now {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_CYCLE),
                "governed trigger requires a non-future cycle cutoff",
            )
            .into());
        }
        Self::verify_forced_parent(&transaction, &command.cycle).await?;
        let scheduler_row = SchedulerEntity::find_by_id(command.cycle.profile_ref().id.clone())
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_FEEDBACK_SCHEDULER_STATE,
                    &command.cycle.profile_ref().id,
                )
            })?;
        let scheduler = Self::scheduler_info(scheduler_row.clone())?;
        if scheduler.research_profile_artifact_id != *command.cycle.research_profile_artifact_id()
            || scheduler.profile_hash != command.cycle.profile_ref().content_hash
            || scheduler.feedback_policy_hash != command.cycle.feedback_policy_hash()
            || cadence_cutoff(command.cycle.label_cutoff(), scheduler.cadence_secs)?
                != command.cycle.label_cutoff()
            || now - command.cycle.label_cutoff() >= Duration::seconds(scheduler.cadence_secs)
        {
            return Err(StorageError::state_conflict(
                QUANT_FEEDBACK_SCHEDULER_STATE,
                Some(&command.cycle.profile_ref().id),
                "manual cycle differs from the current scheduler profile or cadence window",
            )
            .into());
        }
        let cycle_outcome = Self::persist_cycle(&transaction, &command.cycle).await?;
        if matches!(&cycle_outcome, FeedbackCycleWriteOutcome::Inserted(_)) {
            Self::advance_scheduler(&transaction, &command.cycle, scheduler_row, &scheduler, now)
                .await?;
        }
        let cycle_id = command.cycle.feedback_cycle_id();
        let _cycle_lock = Self::lock_cycle(&transaction, &cycle_id).await?;
        let actor = format!("{}@{}", authorized.username, authorized.role);
        let provenance = NewFeedbackTriggerEvent::try_seal(FeedbackTriggerEventInput {
            feedback_cycle_id: cycle_id,
            trigger_family: FeedbackTriggerFamily::Manual,
            evaluation_mode: command.cycle.evaluation_mode(),
            idempotency_key: command.idempotency_key.clone(),
            actor_user_id: Some(authorized.user_id),
            actor_label: authorized.username.clone(),
            actor_role: Some(authorized.role.clone()),
            reason_code: command.reason_code.clone(),
        })?;
        let trigger_outcome = Self::persist_trigger_event(&transaction, &provenance).await?;
        let trigger_event_id = match &trigger_outcome {
            FeedbackTriggerWriteOutcome::Inserted(event)
            | FeedbackTriggerWriteOutcome::AlreadyPresent(event) => event.feedback_trigger_event_id,
        };
        let stored = StageEntity::find()
            .filter(StageColumn::FeedbackCycleId.eq(cycle_id))
            .filter(StageColumn::EventSequence.eq(1))
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .map(Self::stage_info)
            .transpose()?;
        if let Some(stored) = stored {
            if stored.stage != FeedbackStage::Trigger
                || stored.event_kind != FeedbackStageEventKind::Triggered
                || stored.event_sequence != 1
            {
                return Err(StorageError::state_conflict(
                    QUANT_FEEDBACK_STAGE_EVENT,
                    Some(stored.feedback_stage_event_id),
                    "cycle sequence one is not a valid lifecycle trigger",
                )
                .into());
            }
            Self::persist_trigger_outbox(&transaction, trigger_event_id).await?;
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(FeedbackTriggerCommit {
                cycle: cycle_outcome,
                stage: FeedbackStageWriteOutcome::AlreadyPresent(stored),
                trigger: trigger_outcome,
            });
        }
        let trigger = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
            feedback_cycle_id: cycle_id,
            event_sequence: 1,
            stage: FeedbackStage::Trigger,
            event_kind: FeedbackStageEventKind::Triggered,
            trigger_family: Some(FeedbackTriggerFamily::Manual),
            research_job_id: None,
            actor: Some(actor),
            reason_code: Some(command.reason_code),
            evidence_uri: None,
            evidence_hash: None,
            occurred_at: now,
        })?;
        let event_outcome = Self::persist_stage_row(&transaction, &trigger).await?;
        Self::persist_trigger_outbox(&transaction, trigger_event_id).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(FeedbackTriggerCommit {
            cycle: cycle_outcome,
            stage: event_outcome,
            trigger: trigger_outcome,
        })
    }

    async fn find_cycle(
        &self,
        cycle_id: &FeedbackCycleId,
    ) -> Result<Option<FeedbackCycleInfo>, StorageError> {
        CycleEntity::find_by_id(*cycle_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Self::cycle_info)
            .transpose()
    }

    async fn find_cycles(
        &self,
        cycle_ids: &[FeedbackCycleId],
    ) -> Result<Vec<FeedbackCycleInfo>, StorageError> {
        if cycle_ids.is_empty() {
            return Ok(Vec::new());
        }
        CycleEntity::find()
            .filter(CycleColumn::FeedbackCycleId.is_in(cycle_ids.iter().copied()))
            .order_by_asc(CycleColumn::FeedbackCycleId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Self::cycle_info)
            .collect()
    }

    async fn list_trigger_events(
        &self,
        cycle_id: &FeedbackCycleId,
    ) -> Result<Vec<FeedbackTriggerEventInfo>, StorageError> {
        TriggerEntity::find()
            .filter(TriggerColumn::FeedbackCycleId.eq(*cycle_id))
            .order_by_asc(TriggerColumn::OccurredAt)
            .order_by_asc(TriggerColumn::FeedbackTriggerEventId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Self::trigger_info)
            .collect()
    }

    async fn page_cycles(
        &self,
        query: FeedbackCycleListQuery,
    ) -> Result<Paginated<FeedbackCycleInfo>, StorageError> {
        let window = PageWindow::from_query(&query);
        let condition =
            Condition::all().add_option(query.status.map(|status| CycleColumn::Status.eq(status)));
        let mut select = CycleEntity::find().filter(condition);
        if let Some(trigger_family) = query.trigger_family {
            select = select.filter(
                CycleColumn::FeedbackCycleId.in_subquery(
                    TriggerEntity::find()
                        .select_only()
                        .column(TriggerColumn::FeedbackCycleId)
                        .filter(TriggerColumn::TriggerFamily.eq(trigger_family))
                        .into_query(),
                ),
            );
        }
        if let Some(profile_id) = query.profile_id {
            select = select
                .join(
                    JoinType::InnerJoin,
                    CycleRelation::ResearchProfileArtifact.def(),
                )
                .filter(ProfileColumn::ResearchProfileId.eq(profile_id));
        }
        let page = paginate_mapped(
            select
                .order_by_desc(CycleColumn::CreatedAt)
                .order_by_desc(CycleColumn::FeedbackCycleId),
            &self.db,
            window,
            |row| row,
        )
        .await?;
        let items = page
            .items
            .into_iter()
            .map(Self::cycle_info)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Paginated {
            items,
            total: page.total,
            page: page.page,
            size: page.size,
            has_next: page.has_next,
        })
    }

    async fn claim_cycle(
        &self,
        worker_id: WorkerId,
        lease_secs: u64,
    ) -> Result<Option<FeedbackCycleClaim>, StorageError> {
        let lease_duration = Self::lease_duration(lease_secs)?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&transaction).await?;
        let eligible = Condition::any()
            .add(CycleColumn::Status.eq(FeedbackCycleStatus::Queued))
            .add(
                Condition::all()
                    .add(CycleColumn::Status.eq(FeedbackCycleStatus::Running))
                    .add(
                        Condition::any()
                            .add(CycleColumn::LeaseExpiresAt.lte(now))
                            .add(
                                Condition::all()
                                    .add(CycleColumn::LeaseOwner.is_null())
                                    .add(CycleColumn::StageResumeAfter.lte(now)),
                            ),
                    ),
            );
        let Some(row) = CycleEntity::find()
            .filter(eligible)
            .order_by_asc(CycleColumn::CreatedAt)
            .order_by_asc(CycleColumn::FeedbackCycleId)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
        else {
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        };
        let mode = match row.status {
            FeedbackCycleStatus::Queued => FeedbackCycleClaimMode::Started,
            FeedbackCycleStatus::Running if row.stage_resume_after.is_some() => {
                FeedbackCycleClaimMode::StageResumed
            }
            FeedbackCycleStatus::Running => FeedbackCycleClaimMode::LeaseRecovered,
            status => {
                return Err(StorageError::state_conflict(
                    QUANT_FEEDBACK_CYCLE,
                    Some(row.feedback_cycle_id),
                    format!("claim selected ineligible status {status}"),
                ));
            }
        };
        let generation = Self::next_generation(row.generation)?;
        let started_at = row.started_at.or(Some(now));
        let cycle_id = row.feedback_cycle_id;
        let mut active = row.into_active_model();
        active.status = Set(FeedbackCycleStatus::Running);
        active.generation = Set(generation);
        active.lease_owner = Set(Some(worker_id));
        active.lease_expires_at = Set(Some(now + lease_duration));
        active.stage_resume_after = Set(None);
        active.started_at = Set(started_at);
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        let cycle = Self::cycle_info(updated)?;
        let lease = FeedbackCycleLeaseGuard {
            feedback_cycle_id: cycle_id,
            expected_generation: generation,
            worker_id,
        };
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(Some(FeedbackCycleClaim { cycle, mode, lease }))
    }

    async fn renew_cycle_lease(
        &self,
        lease: FeedbackCycleLeaseGuard,
        lease_secs: u64,
    ) -> Result<FeedbackCycleInfo, StorageError> {
        let lease_duration = Self::lease_duration(lease_secs)?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let row = Self::lock_cycle(&transaction, &lease.feedback_cycle_id).await?;
        let cycle = Self::cycle_info(row.clone())?;
        let now = primitives::statement_timestamp(&transaction).await?;
        Self::ensure_live_lease(&cycle, lease, now)?;
        let generation = Self::next_generation(cycle.generation)?;
        let mut active = row.into_active_model();
        active.generation = Set(generation);
        active.lease_expires_at = Set(Some(now + lease_duration));
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        let cycle = Self::cycle_info(updated)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(cycle)
    }

    async fn release_cycle_lease(
        &self,
        lease: FeedbackCycleLeaseGuard,
    ) -> Result<FeedbackCycleInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let row = Self::lock_cycle(&transaction, &lease.feedback_cycle_id).await?;
        let cycle = Self::cycle_info(row.clone())?;
        let now = primitives::statement_timestamp(&transaction).await?;
        Self::ensure_live_lease(&cycle, lease, now)?;
        let mut active = row.into_active_model();
        active.generation = Set(Self::next_generation(cycle.generation)?);
        active.lease_expires_at = Set(Some(now));
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        let released = Self::cycle_info(updated)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(released)
    }

    async fn defer_cycle(
        &self,
        lease: FeedbackCycleLeaseGuard,
        resume_after: DateTime<Utc>,
    ) -> Result<FeedbackCycleInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let row = Self::lock_cycle(&transaction, &lease.feedback_cycle_id).await?;
        let cycle = Self::cycle_info(row.clone())?;
        let now = primitives::statement_timestamp(&transaction).await?;
        Self::ensure_live_lease(&cycle, lease, now)?;
        if resume_after <= now {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_CYCLE),
                "deferred feedback stage must resume in the database future",
            ));
        }
        let mut active = row.into_active_model();
        active.generation = Set(Self::next_generation(cycle.generation)?);
        active.lease_owner = Set(None);
        active.lease_expires_at = Set(None);
        active.stage_resume_after = Set(Some(resume_after));
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        let deferred = Self::cycle_info(updated)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(deferred)
    }

    async fn quarantine_cycle(
        &self,
        lease: FeedbackCycleLeaseGuard,
        reason: FeedbackCoordinatorFaultReason,
    ) -> Result<FeedbackCoordinatorQuarantine, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let row = Self::lock_cycle(&transaction, &lease.feedback_cycle_id).await?;
        let current = Self::cycle_info(row.clone())?;
        let stored_fault = CoordinatorFaultEntity::find()
            .filter(CoordinatorFaultColumn::FeedbackCycleId.eq(lease.feedback_cycle_id))
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .map(Self::coordinator_fault_info)
            .transpose()?;
        if current.status == FeedbackCycleStatus::Quarantined {
            let fault = stored_fault.ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_COORDINATOR_FAULT),
                    "quarantined feedback cycle has no WORM coordinator fault",
                )
            })?;
            let expected_generation = Self::next_generation(lease.expected_generation)?;
            if current.generation != expected_generation
                || fault.lease_generation != lease.expected_generation
                || fault.worker_id != lease.worker_id
                || fault.detail != reason.detail()
            {
                return Err(StorageError::state_conflict(
                    QUANT_FEEDBACK_COORDINATOR_FAULT,
                    Some(current.feedback_cycle_id),
                    "quarantine retry differs from the committed fault",
                ));
            }
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(FeedbackCoordinatorQuarantine {
                cycle: FeedbackCycleCasOutcome::AlreadyApplied(current),
                fault: FeedbackCoordinatorFaultWriteOutcome::AlreadyPresent(fault),
            });
        }
        if stored_fault.is_some() {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_COORDINATOR_FAULT),
                "non-quarantined feedback cycle already has coordinator fault evidence",
            ));
        }
        let now = primitives::statement_timestamp(&transaction).await?;
        Self::ensure_live_lease(&current, lease, now)?;
        let last_event = StageEntity::find()
            .filter(StageColumn::FeedbackCycleId.eq(lease.feedback_cycle_id))
            .order_by_desc(StageColumn::EventSequence)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?;
        let timeline_head = last_event.map_or(
            FeedbackCoordinatorTimelineHead {
                active_stage: None,
                last_event_sequence: None,
                last_stage_event_id: None,
                last_stage_event_hash: None,
            },
            |event| FeedbackCoordinatorTimelineHead {
                active_stage: Some(event.stage),
                last_event_sequence: Some(event.event_sequence),
                last_stage_event_id: Some(event.feedback_stage_event_id),
                last_stage_event_hash: Some(event.event_hash),
            },
        );
        let fault = NewFeedbackCoordinatorFault::try_seal(FeedbackCoordinatorFaultInput {
            feedback_cycle_id: lease.feedback_cycle_id,
            lease_generation: lease.expected_generation,
            worker_id: lease.worker_id,
            timeline_head,
            reason,
            observed_at: now,
        })
        .map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_COORDINATOR_FAULT),
                format!("coordinator fault could not be sealed: {error}"),
            )
        })?;
        let mut fault_active = fault.clone().into_active_model();
        fault_active.created_at = Set(now);
        let inserted = CoordinatorFaultEntity::insert(fault_active)
            .exec_with_returning(&transaction)
            .await
            .map_err(StorageError::from)?;
        let fault_info = Self::coordinator_fault_info(inserted)?;

        let mut active = row.into_active_model();
        active.status = Set(FeedbackCycleStatus::Quarantined);
        active.decision = Set(None);
        active.terminal_reason_code = Set(Some("invalid_coordinator_state".to_owned()));
        active.generation = Set(Self::next_generation(current.generation)?);
        active.lease_owner = Set(None);
        active.lease_expires_at = Set(None);
        active.stage_resume_after = Set(None);
        active.completed_at = Set(Some(now));
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        let quarantined = Self::cycle_info(updated)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(FeedbackCoordinatorQuarantine {
            cycle: FeedbackCycleCasOutcome::Applied(quarantined),
            fault: FeedbackCoordinatorFaultWriteOutcome::Inserted(fault_info),
        })
    }

    async fn find_coordinator_fault(
        &self,
        cycle_id: &FeedbackCycleId,
    ) -> Result<Option<FeedbackCoordinatorFaultInfo>, StorageError> {
        CoordinatorFaultEntity::find()
            .filter(CoordinatorFaultColumn::FeedbackCycleId.eq(*cycle_id))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Self::coordinator_fault_info)
            .transpose()
    }

    async fn request_cancel(
        &self,
        generation: FeedbackCycleGeneration,
        event: NewFeedbackStageEvent,
    ) -> Result<(FeedbackCycleCasOutcome, FeedbackStageWriteOutcome), StorageError> {
        Self::request_cancelled(&self.db, generation, event).await
    }

    async fn request_governed_cancel(
        &self,
        command: GovernedFeedbackCancellation,
    ) -> Result<(FeedbackCycleCasOutcome, FeedbackStageWriteOutcome), FeedbackCycleCommandError>
    {
        command.validate()?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let authorized = authorization::authorize_actor::<FeedbackCycleCommandError>(
            &transaction,
            command.actor.user_id,
            &command.actor.acting_role,
            ResourceType::Materialization,
            Operation::Create,
        )
        .await?;
        let actor = format!("{}@{}", authorized.username, authorized.role);
        let row = Self::lock_cycle(&transaction, &command.feedback_cycle_id).await?;
        let current = Self::cycle_info(row.clone())?;
        let cancellations = StageEntity::find()
            .filter(StageColumn::FeedbackCycleId.eq(command.feedback_cycle_id))
            .filter(StageColumn::EventKind.eq(FeedbackStageEventKind::CancellationRequested))
            .order_by_asc(StageColumn::EventSequence)
            .lock_exclusive()
            .all(&transaction)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Self::stage_info)
            .collect::<Result<Vec<_>, _>>()?;
        if cancellations.len() > 1 {
            return Err(StorageError::state_conflict(
                QUANT_FEEDBACK_STAGE_EVENT,
                Some(command.feedback_cycle_id),
                "feedback timeline contains more than one governed cancellation request",
            )
            .into());
        }
        if let Some(stored) = cancellations.into_iter().next() {
            if stored.actor.as_deref() != Some(actor.as_str())
                || stored.reason_code.as_deref() != Some(command.reason_code.as_str())
            {
                return Err(StorageError::state_conflict(
                    QUANT_FEEDBACK_STAGE_EVENT,
                    Some(stored.feedback_stage_event_id),
                    "governed cancellation retry differs in actor or reason",
                )
                .into());
            }
            if current.cancel_requested_at.is_none()
                && current.status != FeedbackCycleStatus::Cancelled
            {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_CYCLE),
                    "governed cancellation evidence exists without cycle cancellation state",
                )
                .into());
            }
            Self::persist_stage_outbox(&transaction, stored.feedback_stage_event_id).await?;
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok((
                FeedbackCycleCasOutcome::AlreadyApplied(current),
                FeedbackStageWriteOutcome::AlreadyPresent(stored),
            ));
        }

        Self::ensure_generation(&current, command.expected_generation)?;
        let events = StageEntity::find()
            .filter(StageColumn::FeedbackCycleId.eq(command.feedback_cycle_id))
            .order_by_asc(StageColumn::EventSequence)
            .lock_shared()
            .all(&transaction)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Self::stage_info)
            .collect::<Result<Vec<_>, _>>()?;
        let expected_existing =
            usize::try_from(command.expected_event_sequence - 1).map_err(|error| {
                StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_STAGE_EVENT),
                    format!("cancellation event sequence exceeds platform capacity: {error}"),
                )
            })?;
        if events.len() != expected_existing
            || events
                .iter()
                .enumerate()
                .any(|(index, event)| i64::try_from(index + 1) != Ok(event.event_sequence))
        {
            return Err(StorageError::state_conflict(
                QUANT_FEEDBACK_STAGE_EVENT,
                Some(command.feedback_cycle_id),
                "governed cancellation sequence does not match the locked WORM timeline",
            )
            .into());
        }

        let now = primitives::statement_timestamp(&transaction).await?;
        let event = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
            feedback_cycle_id: command.feedback_cycle_id,
            event_sequence: command.expected_event_sequence,
            stage: command.stage,
            event_kind: FeedbackStageEventKind::CancellationRequested,
            trigger_family: None,
            research_job_id: None,
            actor: Some(actor),
            reason_code: Some(command.reason_code.clone()),
            evidence_uri: None,
            evidence_hash: None,
            occurred_at: now,
        })?;
        let mut active = row.into_active_model();
        active.generation = Set(Self::next_generation(current.generation)?);
        active.cancel_requested_at = Set(Some(now));
        match current.status {
            FeedbackCycleStatus::Queued => {
                active.status = Set(FeedbackCycleStatus::Cancelled);
                active.terminal_reason_code = Set(Some(command.reason_code));
                active.completed_at = Set(Some(now));
            }
            FeedbackCycleStatus::Running if current.stage_resume_after.is_some() => {
                active.status = Set(FeedbackCycleStatus::Cancelled);
                active.terminal_reason_code = Set(Some(command.reason_code));
                active.stage_resume_after = Set(None);
                active.completed_at = Set(Some(now));
            }
            FeedbackCycleStatus::Running => {}
            status => {
                return Err(StorageError::illegal_transition(
                    QUANT_FEEDBACK_CYCLE,
                    Some(current.feedback_cycle_id),
                    status,
                    FeedbackCycleStatus::Cancelled,
                )
                .into());
            }
        }
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        let updated = Self::cycle_info(updated)?;
        let event_outcome = Self::persist_stage(&transaction, &event).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok((FeedbackCycleCasOutcome::Applied(updated), event_outcome))
    }

    async fn finalize_cycle(
        &self,
        lease: FeedbackCycleLeaseGuard,
        terminal: FeedbackCycleTerminal,
    ) -> Result<FeedbackCycleCasOutcome, StorageError> {
        Self::finish_cycle(&self.db, lease, terminal).await
    }

    async fn append_stage(
        &self,
        lease: FeedbackCycleLeaseGuard,
        event: NewFeedbackStageEvent,
    ) -> Result<FeedbackStageWriteOutcome, StorageError> {
        Self::write_stage(&self.db, lease, event).await
    }

    async fn append_drift(
        &self,
        lease: FeedbackCycleLeaseGuard,
        report: NewDriftReport,
    ) -> Result<DriftReportWriteOutcome, StorageError> {
        Self::write_drift(&self.db, lease, report).await
    }

    async fn append_evaluation(
        &self,
        lease: FeedbackCycleLeaseGuard,
        evaluation: NewFeedbackEvaluationUse,
    ) -> Result<FeedbackEvaluationWriteOutcome, StorageError> {
        Self::write_evaluation(&self.db, lease, evaluation).await
    }

    async fn list_stage_events(
        &self,
        cycle_id: &FeedbackCycleId,
    ) -> Result<Vec<FeedbackStageEventInfo>, StorageError> {
        StageEntity::find()
            .filter(StageColumn::FeedbackCycleId.eq(*cycle_id))
            .order_by_asc(StageColumn::EventSequence)
            .order_by_asc(StageColumn::FeedbackStageEventId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Self::stage_info)
            .collect()
    }

    async fn find_stage_events(
        &self,
        cycle_ids: &[FeedbackCycleId],
    ) -> Result<Vec<FeedbackStageEventInfo>, StorageError> {
        if cycle_ids.is_empty() {
            return Ok(Vec::new());
        }
        StageEntity::find()
            .filter(StageColumn::FeedbackCycleId.is_in(cycle_ids.iter().copied()))
            .order_by_asc(StageColumn::FeedbackCycleId)
            .order_by_asc(StageColumn::EventSequence)
            .order_by_asc(StageColumn::FeedbackStageEventId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Self::stage_info)
            .collect()
    }

    async fn list_drift_reports(
        &self,
        cycle_id: &FeedbackCycleId,
    ) -> Result<Vec<DriftReportInfo>, StorageError> {
        DriftEntity::find()
            .filter(DriftColumn::FeedbackCycleId.eq(*cycle_id))
            .order_by_asc(DriftColumn::Metric)
            .order_by_asc(DriftColumn::DriftReportId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Self::drift_info)
            .collect()
    }

    async fn page_drift_reports(
        &self,
        query: DriftReportListQuery,
    ) -> Result<Paginated<DriftReportInfo>, StorageError> {
        let window = PageWindow::from_query(&query);
        let condition = Condition::all()
            .add_option(
                query
                    .feedback_cycle_id
                    .map(|cycle_id| DriftColumn::FeedbackCycleId.eq(cycle_id)),
            )
            .add_option(query.kind.map(|kind| DriftColumn::Kind.eq(kind)))
            .add_option(query.metric.map(|metric| DriftColumn::Metric.eq(metric)));
        let mut select = DriftEntity::find().filter(condition);
        if let Some(profile_id) = query.profile_id {
            select = select
                .join(JoinType::InnerJoin, DriftRelation::FeedbackCycle.def())
                .join(
                    JoinType::InnerJoin,
                    CycleRelation::ResearchProfileArtifact.def(),
                )
                .filter(ProfileColumn::ResearchProfileId.eq(profile_id));
        }
        let page = paginate_mapped(
            select
                .order_by_desc(DriftColumn::ObservedAt)
                .order_by_desc(DriftColumn::DriftReportId),
            &self.db,
            window,
            |row| row,
        )
        .await?;
        let items = page
            .items
            .into_iter()
            .map(Self::drift_info)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Paginated::new(items, page.total, page.page, page.size))
    }

    async fn list_evaluation_uses(
        &self,
        cycle_id: &FeedbackCycleId,
    ) -> Result<Vec<FeedbackEvaluationUseInfo>, StorageError> {
        EvaluationEntity::find()
            .filter(EvaluationColumn::FeedbackCycleId.eq(*cycle_id))
            .order_by_asc(EvaluationColumn::ReservedAt)
            .order_by_asc(EvaluationColumn::FeedbackEvaluationUseId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Self::evaluation_info)
            .collect()
    }

    async fn page_model_evaluation_uses(
        &self,
        model_version_id: &ModelVersionId,
        page: PageRequest,
    ) -> Result<Paginated<FeedbackEvaluationUseInfo>, StorageError> {
        let window = PageWindow::harden(page);
        let page = paginate_mapped(
            EvaluationEntity::find()
                .filter(EvaluationColumn::ChampionModelVersionId.eq(*model_version_id))
                .order_by_desc(EvaluationColumn::ReservedAt)
                .order_by_desc(EvaluationColumn::FeedbackEvaluationUseId),
            &self.db,
            window,
            |row| row,
        )
        .await?;
        let items = page
            .items
            .into_iter()
            .map(Self::evaluation_info)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Paginated::new(items, page.total, page.page, page.size))
    }

    async fn queue_snapshot(&self) -> Result<FeedbackQueueSnapshot, StorageError> {
        let transaction = self
            .db
            .begin_with_config(
                Some(IsolationLevel::RepeatableRead),
                Some(AccessMode::ReadOnly),
            )
            .await
            .map_err(StorageError::from)?;
        let queued = CycleEntity::find()
            .filter(CycleColumn::Status.eq(FeedbackCycleStatus::Queued))
            .count(&transaction)
            .await
            .map_err(StorageError::from)?;
        let running = CycleEntity::find()
            .filter(CycleColumn::Status.eq(FeedbackCycleStatus::Running))
            .count(&transaction)
            .await
            .map_err(StorageError::from)?;
        let pending_outbox = OutboxEntity::find()
            .filter(OutboxColumn::PublishedAt.is_null())
            .count(&transaction)
            .await
            .map_err(StorageError::from)?;
        let oldest_queued_at = CycleEntity::find()
            .filter(CycleColumn::Status.eq(FeedbackCycleStatus::Queued))
            .order_by_asc(CycleColumn::CreatedAt)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .map(|row| row.created_at);
        let oldest_running_at = CycleEntity::find()
            .filter(CycleColumn::Status.eq(FeedbackCycleStatus::Running))
            .order_by_asc(CycleColumn::StartedAt)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .and_then(|row| row.started_at);
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(FeedbackQueueSnapshot {
            queued,
            running,
            pending_outbox,
            oldest_queued_at,
            oldest_running_at,
        })
    }
}

#[async_trait::async_trait]
impl FeedbackOutboxRepository for PgFeedbackCycleRepository {
    async fn latest_outbox_revision(&self) -> Result<i64, StorageError> {
        OutboxEntity::find()
            .order_by_desc(OutboxColumn::Revision)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map_or(0, |entry| entry.revision))
    }

    async fn claim_outbox(
        &self,
        worker_id: WorkerId,
        lease_secs: u64,
        limit: u64,
    ) -> Result<Vec<FeedbackOutboxEntry>, StorageError> {
        let limit = Self::outbox_limit(limit)?;
        let lease_duration = Self::lease_duration(lease_secs)?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let now = primitives::statement_timestamp(&transaction).await?;
        let mut rows = OutboxEntity::find()
            .filter(OutboxColumn::PublishedAt.is_null())
            .filter(
                Condition::any()
                    .add(OutboxColumn::LeaseExpiresAt.is_null())
                    .add(OutboxColumn::LeaseExpiresAt.lte(now)),
            )
            .order_by_asc(OutboxColumn::Revision)
            .limit(limit)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .all(&transaction)
            .await
            .map_err(StorageError::from)?;
        if rows.is_empty() {
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(Vec::new());
        }
        for row in &mut rows {
            row.publish_attempts = row.publish_attempts.checked_add(1).ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                    format!(
                        "outbox revision {} exhausted publish attempts",
                        row.revision
                    ),
                )
            })?;
        }
        let revisions = rows.iter().map(|row| row.revision).collect::<Vec<_>>();
        let updated = OutboxEntity::update_many()
            .col_expr(OutboxColumn::ClaimOwner, Expr::value(Some(worker_id)))
            .col_expr(
                OutboxColumn::LeaseExpiresAt,
                Expr::value(Some(now + lease_duration)),
            )
            .col_expr(
                OutboxColumn::PublishAttempts,
                Expr::col(OutboxColumn::PublishAttempts).add(1),
            )
            .col_expr(OutboxColumn::UpdatedAt, Expr::value(now))
            .filter(OutboxColumn::Revision.is_in(revisions))
            .exec(&transaction)
            .await
            .map_err(StorageError::from)?;
        let expected = u64::try_from(rows.len()).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                format!("claimed outbox batch length overflow: {error}"),
            )
        })?;
        if updated.rows_affected != expected {
            return Err(StorageError::state_conflict(
                QUANT_FEEDBACK_EVENT_OUTBOX,
                Option::<i64>::None,
                "outbox claim set changed while rows were locked",
            ));
        }
        let entries = Self::outbox_entries(&transaction, rows).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(entries)
    }

    async fn publish_outbox(&self, revision: i64, worker_id: WorkerId) -> Result<(), StorageError> {
        let now = primitives::statement_timestamp(&self.db).await?;
        let updated = OutboxEntity::update_many()
            .col_expr(OutboxColumn::PublishedAt, Expr::value(Some(now)))
            .col_expr(
                OutboxColumn::ClaimOwner,
                Expr::value(Option::<WorkerId>::None),
            )
            .col_expr(
                OutboxColumn::LeaseExpiresAt,
                Expr::value(Option::<DateTime<Utc>>::None),
            )
            .col_expr(OutboxColumn::LastError, Expr::value(Option::<String>::None))
            .col_expr(OutboxColumn::UpdatedAt, Expr::value(now))
            .filter(OutboxColumn::Revision.eq(revision))
            .filter(OutboxColumn::ClaimOwner.eq(worker_id))
            .filter(OutboxColumn::PublishedAt.is_null())
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        if updated.rows_affected == 1 {
            return Ok(());
        }
        let stored = OutboxEntity::find_by_id(revision)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        if stored.is_some_and(|row| row.published_at.is_some()) {
            Ok(())
        } else {
            Err(StorageError::state_conflict(
                QUANT_FEEDBACK_EVENT_OUTBOX,
                Some(revision),
                "outbox publish requires the exact live claim owner",
            ))
        }
    }

    async fn fail_outbox(
        &self,
        revision: i64,
        worker_id: WorkerId,
        detail: String,
    ) -> Result<(), StorageError> {
        let detail = detail.trim();
        if detail.is_empty() || detail.len() > 2_048 {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                "outbox failure detail must contain 1..=2048 bytes",
            ));
        }
        let now = primitives::statement_timestamp(&self.db).await?;
        let updated = OutboxEntity::update_many()
            .col_expr(OutboxColumn::LastError, Expr::value(Some(detail)))
            .col_expr(
                OutboxColumn::ClaimOwner,
                Expr::value(Option::<WorkerId>::None),
            )
            .col_expr(
                OutboxColumn::LeaseExpiresAt,
                Expr::value(Option::<DateTime<Utc>>::None),
            )
            .col_expr(OutboxColumn::UpdatedAt, Expr::value(now))
            .filter(OutboxColumn::Revision.eq(revision))
            .filter(OutboxColumn::ClaimOwner.eq(worker_id))
            .filter(OutboxColumn::PublishedAt.is_null())
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        if updated.rows_affected == 1 {
            return Ok(());
        }
        let stored = OutboxEntity::find_by_id(revision)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        if stored.is_some_and(|row| {
            row.published_at.is_none()
                && row.claim_owner.is_none()
                && row.last_error.as_deref() == Some(detail)
        }) {
            Ok(())
        } else {
            Err(StorageError::state_conflict(
                QUANT_FEEDBACK_EVENT_OUTBOX,
                Some(revision),
                "outbox failure requires the exact live claim owner",
            ))
        }
    }

    async fn list_outbox(
        &self,
        after_revision: i64,
        limit: u64,
    ) -> Result<Vec<FeedbackOutboxEntry>, StorageError> {
        let limit = Self::outbox_limit(limit)?;
        if after_revision < 0 {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                "feedback outbox cursor cannot be negative",
            ));
        }
        let rows = OutboxEntity::find()
            .filter(OutboxColumn::Revision.gt(after_revision))
            .order_by_asc(OutboxColumn::Revision)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Self::outbox_entries(&self.db, rows).await
    }
}

impl PgFeedbackCycleRepository {
    async fn request_cancelled(
        db: &DatabaseConnection,
        generation: FeedbackCycleGeneration,
        event: NewFeedbackStageEvent,
    ) -> Result<(FeedbackCycleCasOutcome, FeedbackStageWriteOutcome), StorageError> {
        if event.feedback_cycle_id() != generation.feedback_cycle_id {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_STAGE_EVENT),
                "cancellation evidence belongs to a different cycle",
            ));
        }
        let reason_code = event
            .cancellation_reason()
            .map_err(|error| {
                StorageError::invariant_violation(Some(QUANT_FEEDBACK_STAGE_EVENT), error)
            })?
            .to_owned();
        let transaction = db.begin().await.map_err(StorageError::from)?;
        let row = Self::lock_cycle(&transaction, &generation.feedback_cycle_id).await?;
        let current = Self::cycle_info(row.clone())?;
        let now = primitives::statement_timestamp(&transaction).await?;
        if event.occurred_at() < current.created_at || event.occurred_at() > now {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_STAGE_EVENT),
                "cancellation occurrence must be within the durable cycle lifetime",
            ));
        }
        let cancel_requested_at = event.occurred_at();
        if let Some(stored) = Self::stage_candidate(&transaction, &event).await? {
            let stored = Self::exact_stage(stored, &event)?;
            if current.cancel_requested_at.is_none()
                && current.status != FeedbackCycleStatus::Cancelled
            {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_CYCLE),
                    "cancellation evidence exists without cycle cancellation state",
                ));
            }
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok((
                FeedbackCycleCasOutcome::AlreadyApplied(current),
                FeedbackStageWriteOutcome::AlreadyPresent(stored),
            ));
        }
        Self::ensure_generation(&current, generation.expected_generation)?;
        let next_generation = Self::next_generation(current.generation)?;
        let mut active = row.into_active_model();
        active.generation = Set(next_generation);
        active.cancel_requested_at = Set(Some(cancel_requested_at));
        match current.status {
            FeedbackCycleStatus::Queued => {
                active.status = Set(FeedbackCycleStatus::Cancelled);
                active.terminal_reason_code = Set(Some(reason_code));
                active.completed_at = Set(Some(now));
            }
            FeedbackCycleStatus::Running if current.stage_resume_after.is_some() => {
                active.status = Set(FeedbackCycleStatus::Cancelled);
                active.terminal_reason_code = Set(Some(reason_code));
                active.stage_resume_after = Set(None);
                active.completed_at = Set(Some(now));
            }
            FeedbackCycleStatus::Running => {}
            status => {
                return Err(StorageError::illegal_transition(
                    QUANT_FEEDBACK_CYCLE,
                    Some(current.feedback_cycle_id),
                    status,
                    FeedbackCycleStatus::Cancelled,
                ));
            }
        }
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        let updated = Self::cycle_info(updated)?;
        let event_outcome = Self::persist_stage(&transaction, &event).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok((FeedbackCycleCasOutcome::Applied(updated), event_outcome))
    }
}

impl PgFeedbackCycleRepository {
    async fn finish_cycle(
        db: &DatabaseConnection,
        lease: FeedbackCycleLeaseGuard,
        terminal: FeedbackCycleTerminal,
    ) -> Result<FeedbackCycleCasOutcome, StorageError> {
        let transaction = db.begin().await.map_err(StorageError::from)?;
        let row = Self::lock_cycle(&transaction, &lease.feedback_cycle_id).await?;
        let current = Self::cycle_info(row.clone())?;
        let expected_applied = Self::next_generation(lease.expected_generation)?;
        if current.status == terminal.status()
            && current.decision == terminal.decision()
            && current.terminal_reason_code.as_deref() == Some(terminal.reason_code())
            && current.generation == expected_applied
        {
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(FeedbackCycleCasOutcome::AlreadyApplied(current));
        }
        if current.status.is_terminal() {
            return Err(StorageError::illegal_transition(
                QUANT_FEEDBACK_CYCLE,
                Some(current.feedback_cycle_id),
                current.status,
                terminal.status(),
            ));
        }
        let now = primitives::statement_timestamp(&transaction).await?;
        Self::ensure_live_lease(&current, lease, now)?;
        if terminal.status() == FeedbackCycleStatus::Cancelled
            && current.cancel_requested_at.is_none()
        {
            return Err(StorageError::state_conflict(
                QUANT_FEEDBACK_CYCLE,
                Some(current.feedback_cycle_id),
                "worker cannot cancel a cycle without a governed cancellation request",
            ));
        }
        let mut active = row.into_active_model();
        active.status = Set(terminal.status());
        active.decision = Set(terminal.decision());
        active.terminal_reason_code = Set(Some(terminal.reason_code().to_owned()));
        active.generation = Set(Self::next_generation(current.generation)?);
        active.lease_owner = Set(None);
        active.lease_expires_at = Set(None);
        active.stage_resume_after = Set(None);
        active.completed_at = Set(Some(now));
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        let updated = Self::cycle_info(updated)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(FeedbackCycleCasOutcome::Applied(updated))
    }
}

impl PgFeedbackCycleRepository {
    async fn write_stage(
        db: &DatabaseConnection,
        lease: FeedbackCycleLeaseGuard,
        event: NewFeedbackStageEvent,
    ) -> Result<FeedbackStageWriteOutcome, StorageError> {
        if event.feedback_cycle_id() != lease.feedback_cycle_id
            || matches!(
                event.event_kind(),
                FeedbackStageEventKind::Triggered | FeedbackStageEventKind::CancellationRequested
            )
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_STAGE_EVENT),
                "worker append requires matching non-trigger, non-cancellation evidence",
            ));
        }
        let transaction = db.begin().await.map_err(StorageError::from)?;
        if let Some(stored) = Self::stage_candidate(&transaction, &event).await? {
            let stored = Self::exact_stage(stored, &event)?;
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(FeedbackStageWriteOutcome::AlreadyPresent(stored));
        }
        let row = Self::lock_cycle(&transaction, &lease.feedback_cycle_id).await?;
        let cycle = Self::cycle_info(row)?;
        let now = primitives::statement_timestamp(&transaction).await?;
        Self::ensure_live_lease(&cycle, lease, now)?;
        if event.occurred_at() > now {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_STAGE_EVENT),
                "stage occurrence cannot be in the database future",
            ));
        }
        let outcome = Self::persist_stage(&transaction, &event).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(outcome)
    }

    async fn write_drift(
        db: &DatabaseConnection,
        lease: FeedbackCycleLeaseGuard,
        report: NewDriftReport,
    ) -> Result<DriftReportWriteOutcome, StorageError> {
        if report.feedback_cycle_id() != lease.feedback_cycle_id {
            return Err(StorageError::invariant_violation(
                Some(QUANT_DRIFT_REPORT),
                "drift report belongs to a different cycle",
            ));
        }
        let transaction = db.begin().await.map_err(StorageError::from)?;
        if let Some(stored) = Self::drift_candidate(&transaction, &report).await? {
            let stored = Self::exact_drift(stored, &report)?;
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(DriftReportWriteOutcome::AlreadyPresent(stored));
        }
        let row = Self::lock_cycle(&transaction, &lease.feedback_cycle_id).await?;
        let cycle = Self::cycle_info(row)?;
        let now = primitives::statement_timestamp(&transaction).await?;
        Self::ensure_live_lease(&cycle, lease, now)?;
        if !cycle.accepts_drift(&report) || report.observed_at() > now {
            return Err(StorageError::invariant_violation(
                Some(QUANT_DRIFT_REPORT),
                "drift lineage or observation time differs from the frozen cycle",
            ));
        }
        let outcome = Self::persist_drift(&transaction, &report).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(outcome)
    }

    async fn write_evaluation(
        db: &DatabaseConnection,
        lease: FeedbackCycleLeaseGuard,
        evaluation: NewFeedbackEvaluationUse,
    ) -> Result<FeedbackEvaluationWriteOutcome, StorageError> {
        if evaluation.feedback_cycle_id() != lease.feedback_cycle_id {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_EVALUATION_USE),
                "evaluation use belongs to a different cycle",
            ));
        }
        let transaction = db.begin().await.map_err(StorageError::from)?;
        if let Some(stored) = Self::evaluation_candidate(&transaction, &evaluation).await? {
            let stored = Self::exact_evaluation(stored, &evaluation)?;
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(FeedbackEvaluationWriteOutcome::AlreadyPresent(stored));
        }
        let row = Self::lock_cycle(&transaction, &lease.feedback_cycle_id).await?;
        let cycle = Self::cycle_info(row)?;
        let now = primitives::statement_timestamp(&transaction).await?;
        Self::ensure_live_lease(&cycle, lease, now)?;
        if !cycle.accepts_evaluation(&evaluation) {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_EVALUATION_USE),
                "evaluation lineage differs from the frozen cycle",
            ));
        }
        let outcome = Self::persist_evaluation(&transaction, &evaluation).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(outcome)
    }
}
