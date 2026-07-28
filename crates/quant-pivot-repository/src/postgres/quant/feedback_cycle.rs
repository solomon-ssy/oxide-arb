//! `PostgreSQL` feedback-cycle orchestration and immutable evidence repository.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{
        QUANT_DRIFT_REPORT, QUANT_FEEDBACK_CYCLE, QUANT_FEEDBACK_EVALUATION_USE,
        QUANT_FEEDBACK_EVENT_OUTBOX, QUANT_FEEDBACK_STAGE_EVENT,
    },
};
use quant_pivot_models::{
    domain::quant::{
        DriftReportInfo, FeedbackCycleInfo, FeedbackCycleTerminal, FeedbackEvaluationUseInfo,
        FeedbackOutboxEntry, FeedbackQueueSnapshot, FeedbackStageEventInfo, NewDriftReport,
        NewFeedbackCycle, NewFeedbackEvaluationUse, NewFeedbackStageEvent,
    },
    entities::{
        quant_drift_report::{Column as DriftColumn, Entity as DriftEntity, Model as DriftModel},
        quant_feedback_cycle::{Column as CycleColumn, Entity as CycleEntity, Model as CycleModel},
        quant_feedback_evaluation_use::{
            Column as EvaluationColumn, Entity as EvaluationEntity, Model as EvaluationModel,
        },
        quant_feedback_event_outbox::{
            ActiveModel as OutboxActiveModel, Column as OutboxColumn, Entity as OutboxEntity,
            Model as OutboxModel,
        },
        quant_feedback_stage_event::{
            Column as StageColumn, Entity as StageEntity, Model as StageModel,
        },
    },
    enums::quant::{FeedbackCycleStatus, FeedbackStageEventKind},
    types::{FeedbackCycleId, FeedbackStageEventId, WorkerId},
};
use sea_orm::{
    ActiveModelTrait,
    ActiveValue::{NotSet, Set},
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    ExprTrait, IntoActiveModel, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait, TryInsertResult,
    sea_query::{Expr, LockBehavior, LockType, OnConflict},
};

use crate::{
    postgres::primitives,
    traits::{
        DriftReportWriteOutcome, FeedbackCycleCasOutcome, FeedbackCycleClaim,
        FeedbackCycleClaimMode, FeedbackCycleGeneration, FeedbackCycleLeaseGuard,
        FeedbackCycleRepository, FeedbackCycleWriteOutcome, FeedbackEvaluationWriteOutcome,
        FeedbackStageWriteOutcome,
    },
};

/// `PostgreSQL`-backed feedback-cycle repository.
pub struct PgFeedbackCycleRepository {
    db: DatabaseConnection,
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
        let event_ids = rows
            .iter()
            .map(|row| row.feedback_stage_event_id)
            .collect::<Vec<_>>();
        let events = StageEntity::find()
            .filter(StageColumn::FeedbackStageEventId.is_in(event_ids))
            .all(connection)
            .await
            .map_err(StorageError::from)?;
        let mut events = events
            .into_iter()
            .map(|row| {
                let event = Self::stage_info(row)?;
                Ok((event.feedback_stage_event_id, event))
            })
            .collect::<Result<HashMap<_, _>, StorageError>>()?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let event = events.remove(&row.feedback_stage_event_id).ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                    format!(
                        "outbox revision {} references a missing stage event",
                        row.revision
                    ),
                )
            })?;
            let entry = FeedbackOutboxEntry {
                revision: row.revision,
                publish_attempts: row.publish_attempts,
                event,
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
    async fn cycle_candidate(
        transaction: &DatabaseTransaction,
        cycle: &NewFeedbackCycle,
    ) -> Result<Option<FeedbackCycleInfo>, StorageError> {
        CycleEntity::find()
            .filter(
                Condition::any()
                    .add(CycleColumn::FeedbackCycleId.eq(cycle.feedback_cycle_id()))
                    .add(CycleColumn::IdempotencyHash.eq(cycle.idempotency_hash())),
            )
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

    async fn persist_stage(
        transaction: &DatabaseTransaction,
        event: &NewFeedbackStageEvent,
    ) -> Result<FeedbackStageWriteOutcome, StorageError> {
        if let Some(stored) = Self::stage_candidate(transaction, event).await? {
            let stored = Self::exact_stage(stored, event)?;
            Self::persist_outbox(transaction, stored.feedback_stage_event_id).await?;
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
        Self::persist_outbox(transaction, stored.feedback_stage_event_id).await?;
        if inserted {
            Ok(FeedbackStageWriteOutcome::Inserted(stored))
        } else {
            Ok(FeedbackStageWriteOutcome::AlreadyPresent(stored))
        }
    }

    async fn persist_outbox(
        transaction: &DatabaseTransaction,
        event_id: FeedbackStageEventId,
    ) -> Result<(), StorageError> {
        if OutboxEntity::find()
            .filter(OutboxColumn::FeedbackStageEventId.eq(event_id))
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .is_some()
        {
            return Ok(());
        }
        let now = primitives::statement_timestamp(transaction).await?;
        let row = OutboxActiveModel {
            revision: NotSet,
            feedback_stage_event_id: Set(event_id),
            published_at: Set(None),
            publish_attempts: Set(0),
            claim_owner: Set(None),
            lease_expires_at: Set(None),
            last_error: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        };
        OutboxEntity::insert(row)
            .on_conflict(
                OnConflict::column(OutboxColumn::FeedbackStageEventId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(transaction)
            .await
            .map_err(StorageError::from)?;
        let stored = OutboxEntity::find()
            .filter(OutboxColumn::FeedbackStageEventId.eq(event_id))
            .one(transaction)
            .await
            .map_err(StorageError::from)?;
        if stored.is_none() {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                "stage event committed without an observable outbox revision",
            ));
        }
        Ok(())
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
    ) -> Result<(FeedbackCycleWriteOutcome, FeedbackStageWriteOutcome), StorageError> {
        if trigger.feedback_cycle_id() != cycle.feedback_cycle_id()
            || trigger.event_kind() != FeedbackStageEventKind::Triggered
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_STAGE_EVENT),
                "record_trigger requires matching Triggered evidence",
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
        let event_outcome = Self::persist_stage(&transaction, &trigger).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok((cycle_outcome, event_outcome))
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
                    .add(CycleColumn::LeaseExpiresAt.lte(now)),
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

    async fn request_cancel(
        &self,
        generation: FeedbackCycleGeneration,
        event: NewFeedbackStageEvent,
    ) -> Result<(FeedbackCycleCasOutcome, FeedbackStageWriteOutcome), StorageError> {
        Self::request_cancelled(&self.db, generation, event).await
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

    async fn queue_snapshot(&self) -> Result<FeedbackQueueSnapshot, StorageError> {
        let queued = CycleEntity::find()
            .filter(CycleColumn::Status.eq(FeedbackCycleStatus::Queued))
            .count(&self.db)
            .await
            .map_err(StorageError::from)?;
        let running = CycleEntity::find()
            .filter(CycleColumn::Status.eq(FeedbackCycleStatus::Running))
            .count(&self.db)
            .await
            .map_err(StorageError::from)?;
        let pending_outbox = OutboxEntity::find()
            .filter(OutboxColumn::PublishedAt.is_null())
            .count(&self.db)
            .await
            .map_err(StorageError::from)?;
        let oldest_queued_at = CycleEntity::find()
            .filter(CycleColumn::Status.eq(FeedbackCycleStatus::Queued))
            .order_by_asc(CycleColumn::CreatedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(|row| row.created_at);
        let oldest_running_at = CycleEntity::find()
            .filter(CycleColumn::Status.eq(FeedbackCycleStatus::Running))
            .order_by_asc(CycleColumn::StartedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .and_then(|row| row.started_at);
        Ok(FeedbackQueueSnapshot {
            queued,
            running,
            pending_outbox,
            oldest_queued_at,
            oldest_running_at,
        })
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
        if event.occurred_at() > now {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_STAGE_EVENT),
                "cancellation occurrence cannot be in the database future",
            ));
        }
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
        active.cancel_requested_at = Set(Some(now));
        match current.status {
            FeedbackCycleStatus::Queued => {
                active.status = Set(FeedbackCycleStatus::Cancelled);
                active.terminal_reason_code = Set(Some(reason_code));
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
