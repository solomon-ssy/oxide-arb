//! Postgres-backed durable research-job ledger repository.

use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_RESEARCH_JOB};
use quant_pivot_models::{
    domain::{
        api::ResearchJobListQuery,
        pagination::{PageWindow, Paginated},
        quant::{NewResearchJob, ResearchJobFinalization, ResearchJobInfo},
    },
    entities::quant_research_job::{Column, Entity, Model},
    enums::quant::{ResearchJobErrorCode, ResearchJobKind, ResearchJobStatus},
    types::{ResearchJobError, ResearchJobId, ResearchJobProgress, WorkerId},
};
use sea_orm::{
    ActiveValue::Set,
    ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction, EntityTrait, ExprTrait,
    FromQueryResult, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
    TryInsertResult,
    sea_query::{Expr, LockBehavior, LockType, OnConflict},
};

use crate::{
    postgres::{primitives, query::paginate_mapped},
    traits::{
        KindRunningCount, ReclaimOutcome, ResearchJobEnqueueOutcome, ResearchJobRepository,
        ResearchJobRetryOutcome,
    },
};

/// Postgres-backed durable research-job ledger repository.
pub struct PgResearchJobRepository {
    db: DatabaseConnection,
}

impl PgResearchJobRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    async fn enqueue_candidate(
        transaction: &DatabaseTransaction,
        job: &NewResearchJob,
    ) -> Result<Option<Model>, StorageError> {
        if let Some(stored) = Entity::find_by_id(job.job_id)
            .one(transaction)
            .await
            .map_err(StorageError::from)?
        {
            return Ok(Some(stored));
        }
        let query = match (job.feedback_cycle_id, job.feedback_stage, job.parent_job_id) {
            (Some(cycle_id), Some(stage), None) => Entity::find()
                .filter(Column::FeedbackCycleId.eq(cycle_id))
                .filter(Column::FeedbackStage.eq(stage))
                .filter(Column::ParentJobId.is_null()),
            (Some(cycle_id), Some(stage), Some(parent_job_id)) => Entity::find()
                .filter(Column::FeedbackCycleId.eq(cycle_id))
                .filter(Column::FeedbackStage.eq(stage))
                .filter(Column::ParentJobId.eq(parent_job_id)),
            _ => return Ok(None),
        };
        query.one(transaction).await.map_err(StorageError::from)
    }

    async fn validate_parent(
        transaction: &DatabaseTransaction,
        job: &NewResearchJob,
    ) -> Result<(), StorageError> {
        let Some(parent_job_id) = job.parent_job_id else {
            return Ok(());
        };
        let parent = Entity::find_by_id(parent_job_id)
            .lock(LockType::Update)
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RESEARCH_JOB, parent_job_id))?;
        let parent = ResearchJobInfo::from(parent);
        parent.validate_identity().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RESEARCH_JOB),
                format!("retry parent identity is invalid: {error}"),
            )
        })?;
        if !parent.status.is_terminal() {
            return Err(StorageError::state_conflict(
                QUANT_RESEARCH_JOB,
                Some(&job.job_id),
                format!(
                    "retry parent {parent_job_id} is still active as {}",
                    parent.status
                ),
            ));
        }
        if job.feedback_cycle_id != parent.feedback_cycle_id
            || job.feedback_stage != parent.feedback_stage
        {
            return Err(StorageError::state_conflict(
                QUANT_RESEARCH_JOB,
                Some(&job.job_id),
                format!(
                    "retry parent {parent_job_id} does not own the same feedback cycle and stage"
                ),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, FromQueryResult)]
struct KindCountRow {
    kind: ResearchJobKind,
    running: i64,
}

#[async_trait::async_trait]
impl ResearchJobRepository for PgResearchJobRepository {
    async fn enqueue(
        &self,
        job: NewResearchJob,
    ) -> Result<ResearchJobEnqueueOutcome, StorageError> {
        job.validate_enqueue().map_err(|error| {
            StorageError::invariant_violation(Some(QUANT_RESEARCH_JOB), error.to_string())
        })?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        Self::validate_parent(&transaction, &job).await?;
        let insert = Entity::insert(job.clone().into_active_model())
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .try_insert()
            .exec_without_returning(&transaction)
            .await
            .map_err(StorageError::from)?;
        let inserted = match insert {
            TryInsertResult::Inserted(1) => true,
            TryInsertResult::Inserted(0) | TryInsertResult::Conflicted => false,
            TryInsertResult::Inserted(rows) => {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_RESEARCH_JOB),
                    format!("single research-job insert affected {rows} rows"),
                ));
            }
            TryInsertResult::Empty => {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_RESEARCH_JOB),
                    "non-empty research-job insert produced no statement",
                ));
            }
        };
        let stored = Self::enqueue_candidate(&transaction, &job)
            .await?
            .ok_or_else(|| {
                StorageError::state_conflict(
                    QUANT_RESEARCH_JOB,
                    Some(&job.job_id),
                    "enqueue conflict did not resolve to the deterministic winning row",
                )
            })?;
        let info = ResearchJobInfo::from(stored);
        info.validate_identity().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RESEARCH_JOB),
                format!("stored research-job identity is invalid: {error}"),
            )
        })?;
        if !job.accepts(&info) {
            return Err(StorageError::state_conflict(
                QUANT_RESEARCH_JOB,
                Some(&job.job_id),
                "research-job identity already exists with different immutable enqueue content",
            ));
        }
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(if inserted {
            ResearchJobEnqueueOutcome::Inserted(info)
        } else {
            ResearchJobEnqueueOutcome::AlreadyPresent(info)
        })
    }

    async fn find_by_id(
        &self,
        job_id: &ResearchJobId,
    ) -> Result<Option<ResearchJobInfo>, StorageError> {
        Entity::find_by_id(*job_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_ids(
        &self,
        job_ids: &[ResearchJobId],
    ) -> Result<Vec<ResearchJobInfo>, StorageError> {
        if job_ids.is_empty() {
            return Ok(Vec::new());
        }
        Entity::find()
            .filter(Column::JobId.is_in(job_ids.iter().copied()))
            .order_by_asc(Column::JobId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn page(
        &self,
        query: ResearchJobListQuery,
    ) -> Result<Paginated<ResearchJobInfo>, StorageError> {
        paginate_mapped(
            Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn running_counts(&self) -> Result<Vec<KindRunningCount>, StorageError> {
        let rows = Entity::find()
            .select_only()
            .column(Column::Kind)
            .column_as(Column::JobId.count(), "running")
            .filter(Column::Status.eq(ResearchJobStatus::Running))
            .group_by(Column::Kind)
            .into_model::<KindCountRow>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(rows
            .into_iter()
            .map(|row| KindRunningCount {
                kind: row.kind,
                running: row.running,
            })
            .collect())
    }

    async fn lease_next(
        &self,
        eligible: &[ResearchJobKind],
        owner: &WorkerId,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<ResearchJobInfo>, StorageError> {
        if eligible.is_empty() {
            return Ok(None);
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        // Skip-locked so concurrent workers never contend on the same queued row.
        let candidate = Entity::find()
            .filter(
                Condition::any()
                    .add(Column::Status.eq(ResearchJobStatus::Queued))
                    .add(
                        Condition::all()
                            .add(Column::Status.eq(ResearchJobStatus::AwaitingEvidence))
                            .add(
                                Expr::col(Column::NextAttemptAt)
                                    .lte(Expr::cust("statement_timestamp()")),
                            ),
                    )
                    .add(
                        Condition::all()
                            .add(Column::Status.eq(ResearchJobStatus::RetryScheduled))
                            .add(
                                Expr::col(Column::NextAttemptAt)
                                    .lte(Expr::cust("statement_timestamp()")),
                            ),
                    ),
            )
            .filter(Column::Kind.is_in(eligible.iter().copied()))
            .order_by_asc(Column::CreatedAt)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .one(&txn)
            .await
            .map_err(StorageError::from)?;
        let Some(model) = candidate else {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        };
        let now = primitives::statement_timestamp(&txn).await?;
        let mut active = model.into_active_model();
        active.status = Set(ResearchJobStatus::Running);
        active.next_attempt_at = Set(None);
        active.progress_json = Set(None);
        active.error_json = Set(None);
        active.lease_owner = Set(Some(*owner));
        active.lease_expires_at = Set(Some(lease_expires_at));
        active.started_at = Set(Some(now));
        active.heartbeat_at = Set(Some(now));
        let leased = Entity::update(active)
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(Some(leased.into()))
    }

    async fn heartbeat(
        &self,
        job_id: &ResearchJobId,
        owner: &WorkerId,
        lease_expires_at: DateTime<Utc>,
        progress: Option<ResearchJobProgress>,
    ) -> Result<bool, StorageError> {
        let Some(model) = Entity::find_by_id(*job_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Ok(false);
        };
        // Cooperative stop signal: a job that is no longer running under this
        // owner was cancelled, reclaimed, or already finalized.
        if model.status != ResearchJobStatus::Running || model.lease_owner.as_ref() != Some(owner) {
            return Ok(false);
        }
        let mut active = model.into_active_model();
        active.heartbeat_at = Set(Some(primitives::statement_timestamp(&self.db).await?));
        active.lease_expires_at = Set(Some(lease_expires_at));
        if let Some(progress) = progress {
            active.progress_json = Set(Some(progress));
        }
        Entity::update(active)
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(true)
    }

    async fn finalize(
        &self,
        job_id: &ResearchJobId,
        owner: &WorkerId,
        finalization: ResearchJobFinalization,
    ) -> Result<ResearchJobInfo, StorageError> {
        let (status, result, artifact, error, coverage) = finalization.into_parts();
        // Single conditional UPDATE: only the lease holder may terminalize.
        let (result_kind, result_ref) =
            result.map_or((None, None), |result| (Some(result.kind), Some(result.id)));
        let (result_artifact_uri, result_artifact_hash) = artifact
            .map_or((None, None), |artifact| {
                (Some(artifact.uri), Some(artifact.content_hash))
            });
        let result = Entity::update_many()
            .col_expr(Column::Status, primitives::enum_value(&status))
            .col_expr(Column::ResultKind, Expr::value(result_kind))
            .col_expr(Column::ResultRef, Expr::value(result_ref))
            .col_expr(Column::ResultArtifactUri, Expr::value(result_artifact_uri))
            .col_expr(
                Column::ResultArtifactHash,
                Expr::value(result_artifact_hash),
            )
            .col_expr(Column::ErrorJson, Expr::value(error))
            .col_expr(Column::CoverageJson, Expr::value(coverage))
            .col_expr(Column::FinishedAt, Expr::cust("statement_timestamp()"))
            .col_expr(Column::LeaseOwner, Expr::value(None::<WorkerId>))
            .col_expr(Column::LeaseExpiresAt, Expr::value(None::<DateTime<Utc>>))
            .col_expr(Column::NextAttemptAt, Expr::value(None::<DateTime<Utc>>))
            .filter(Column::JobId.eq(*job_id))
            .filter(Column::Status.eq(ResearchJobStatus::Running))
            .filter(Column::LeaseOwner.eq(owner))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        if result.rows_affected == 0 {
            return Err(Self::finalize_guard_failure(&self.db, job_id).await);
        }
        Entity::find_by_id(*job_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Into::into)
            .ok_or_else(|| StorageError::not_found(QUANT_RESEARCH_JOB, job_id))
    }

    async fn await_evidence(
        &self,
        job_id: &ResearchJobId,
        owner: &WorkerId,
        progress: ResearchJobProgress,
        retry_after: Duration,
    ) -> Result<ResearchJobInfo, StorageError> {
        let delay = ChronoDuration::from_std(retry_after).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RESEARCH_JOB),
                format!("evidence-wait delay is not representable: {error}"),
            )
        })?;
        if delay <= ChronoDuration::zero() {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RESEARCH_JOB),
                "evidence-wait delay must be positive",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let model = Entity::find_by_id(*job_id)
            .lock(LockType::Update)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RESEARCH_JOB, job_id))?;
        if model.status != ResearchJobStatus::Running || model.lease_owner.as_ref() != Some(owner) {
            return Err(StorageError::state_conflict(
                QUANT_RESEARCH_JOB,
                Some(job_id),
                format!(
                    "cannot await evidence from {} owned by {:?}",
                    model.status, model.lease_owner
                ),
            ));
        }
        if model.kind != ResearchJobKind::FeatureParity {
            return Err(StorageError::state_conflict(
                QUANT_RESEARCH_JOB,
                Some(job_id),
                format!(
                    "only feature_parity may await external evidence, got {}",
                    model.kind
                ),
            ));
        }
        let now = primitives::statement_timestamp(&txn).await?;
        let mut active = model.into_active_model();
        active.status = Set(ResearchJobStatus::AwaitingEvidence);
        active.next_attempt_at = Set(Some(now + delay));
        active.progress_json = Set(Some(progress));
        active.error_json = Set(None);
        active.lease_owner = Set(None);
        active.lease_expires_at = Set(None);
        active.started_at = Set(None);
        active.heartbeat_at = Set(None);
        active.finished_at = Set(None);
        let updated = Entity::update(active)
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn cancel_if_pending(
        &self,
        job_id: &ResearchJobId,
        error: ResearchJobError,
    ) -> Result<bool, StorageError> {
        // Atomic guard: the `status = queued` filter makes this a single
        // conditional UPDATE — a job the worker has already leased (running) is
        // never touched here (that path is the cooperative in-memory cancel).
        let result = Entity::update_many()
            .col_expr(
                Column::Status,
                primitives::enum_value(&ResearchJobStatus::Cancelled),
            )
            .col_expr(Column::ErrorJson, Expr::value(error))
            .col_expr(Column::FinishedAt, Expr::cust("statement_timestamp()"))
            .col_expr(Column::NextAttemptAt, Expr::value(None::<DateTime<Utc>>))
            .filter(Column::JobId.eq(*job_id))
            .filter(Column::Status.is_in([
                ResearchJobStatus::Queued,
                ResearchJobStatus::AwaitingEvidence,
                ResearchJobStatus::RetryScheduled,
            ]))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(result.rows_affected > 0)
    }

    async fn retry_transient(
        &self,
        job_id: &ResearchJobId,
        owner: &WorkerId,
        detail: String,
        retry_after: Duration,
    ) -> Result<ResearchJobRetryOutcome, StorageError> {
        let delay = ChronoDuration::from_std(retry_after).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RESEARCH_JOB),
                format!("transient retry delay is not representable: {error}"),
            )
        })?;
        if delay <= ChronoDuration::zero() {
            return Err(StorageError::invariant_violation(
                Some(QUANT_RESEARCH_JOB),
                "transient retry delay must be positive",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let model = Entity::find_by_id(*job_id)
            .lock(LockType::Update)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RESEARCH_JOB, job_id))?;
        if model.status != ResearchJobStatus::Running || model.lease_owner.as_ref() != Some(owner) {
            return Err(StorageError::state_conflict(
                QUANT_RESEARCH_JOB,
                Some(job_id),
                format!(
                    "cannot schedule transient retry from {} owned by {:?}",
                    model.status, model.lease_owner
                ),
            ));
        }
        let now = primitives::statement_timestamp(&txn).await?;
        let exhausted = model.recovery_attempt >= model.max_recovery_attempts;
        let next_recovery_attempt = model.recovery_attempt + 1;
        let mut active = model.into_active_model();
        active.lease_owner = Set(None);
        active.lease_expires_at = Set(None);
        if exhausted {
            active.status = Set(ResearchJobStatus::Failed);
            active.next_attempt_at = Set(None);
            active.finished_at = Set(Some(now));
            active.error_json = Set(Some(ResearchJobError::new(
                ResearchJobErrorCode::ExecutionRetryExhausted,
                detail,
            )));
        } else {
            active.status = Set(ResearchJobStatus::RetryScheduled);
            active.recovery_attempt = Set(next_recovery_attempt);
            active.next_attempt_at = Set(Some(now + delay));
            active.progress_json = Set(None);
            active.started_at = Set(None);
            active.finished_at = Set(None);
            active.heartbeat_at = Set(None);
            active.error_json = Set(Some(ResearchJobError::new(
                ResearchJobErrorCode::ExecutionRetryScheduled,
                detail,
            )));
        }
        let updated = Entity::update(active)
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        let info = ResearchJobInfo::from(updated);
        Ok(if exhausted {
            ResearchJobRetryOutcome::Exhausted(info)
        } else {
            ResearchJobRetryOutcome::Scheduled(info)
        })
    }

    async fn reclaim_orphaned(
        &self,
        owner: &WorkerId,
        now: DateTime<Utc>,
    ) -> Result<ReclaimOutcome, StorageError> {
        // Boot recovery: reclaim rows whose lease is expired / dead-epoch.
        Self::reclaim_by_condition(&self.db, orphaned_condition(owner, now)).await
    }

    async fn requeue_inflight(&self, owner: &WorkerId) -> Result<ReclaimOutcome, StorageError> {
        // Graceful drain: reclaim this owner's own still-`running` rows (lease may
        // still be valid) so a new epoch re-leases them without a lease-expiry wait.
        Self::reclaim_by_condition(&self.db, owned_running_condition(owner)).await
    }
}

impl PgResearchJobRepository {
    /// Quarantine-then-requeue a set of `running` rows selected by `base` (a
    /// `status = running` predicate plus an ownership/lease clause).
    ///
    /// Quarantines rows already at the recovery cap to `failed` (poison-pill guard),
    /// then re-queues the rest with `recovery_attempt += 1`. Both branches clear the
    /// lease so the row is free to be re-leased.
    async fn reclaim_by_condition(
        db: &DatabaseConnection,
        base: Condition,
    ) -> Result<ReclaimOutcome, StorageError> {
        let quarantined = Entity::update_many()
            .col_expr(
                Column::Status,
                primitives::enum_value(&ResearchJobStatus::Failed),
            )
            .col_expr(Column::LeaseOwner, Expr::value(None::<WorkerId>))
            .col_expr(Column::LeaseExpiresAt, Expr::value(None::<DateTime<Utc>>))
            .col_expr(Column::FinishedAt, Expr::cust("statement_timestamp()"))
            .col_expr(
                Column::ErrorJson,
                Expr::value(ResearchJobError::new(
                    ResearchJobErrorCode::InterruptedExceededAttempts,
                    "exceeded max recovery attempts after repeated interruption",
                )),
            )
            .filter(base.clone())
            .filter(Expr::col(Column::RecoveryAttempt).gte(Expr::col(Column::MaxRecoveryAttempts)))
            .exec(db)
            .await
            .map_err(StorageError::from)?
            .rows_affected;

        let requeued = Entity::update_many()
            .col_expr(
                Column::Status,
                primitives::enum_value(&ResearchJobStatus::RetryScheduled),
            )
            .col_expr(
                Column::RecoveryAttempt,
                Expr::col(Column::RecoveryAttempt).add(1),
            )
            .col_expr(Column::LeaseOwner, Expr::value(None::<WorkerId>))
            .col_expr(Column::LeaseExpiresAt, Expr::value(None::<DateTime<Utc>>))
            .col_expr(Column::NextAttemptAt, Expr::cust("statement_timestamp()"))
            .col_expr(
                Column::ProgressJson,
                Expr::value(None::<ResearchJobProgress>),
            )
            .col_expr(Column::StartedAt, Expr::value(None::<DateTime<Utc>>))
            .col_expr(Column::HeartbeatAt, Expr::value(None::<DateTime<Utc>>))
            .col_expr(
                Column::ErrorJson,
                Expr::value(ResearchJobError::new(
                    ResearchJobErrorCode::InterruptedByRestart,
                    "re-queued after service interruption",
                )),
            )
            .filter(base)
            .filter(Expr::col(Column::RecoveryAttempt).lt(Expr::col(Column::MaxRecoveryAttempts)))
            .exec(db)
            .await
            .map_err(StorageError::from)?
            .rows_affected;

        Ok(ReclaimOutcome {
            requeued,
            quarantined,
        })
    }
}

impl PgResearchJobRepository {
    /// Diagnose why a guarded finalize touched zero rows.
    async fn finalize_guard_failure(
        db: &DatabaseConnection,
        job_id: &ResearchJobId,
    ) -> StorageError {
        let row = match Entity::find_by_id(*job_id).one(db).await {
            Ok(row) => row,
            Err(err) => return StorageError::from(err),
        };
        let Some(row) = row else {
            return StorageError::not_found(QUANT_RESEARCH_JOB, job_id);
        };
        if row.status.is_terminal() {
            return StorageError::state_conflict(
                QUANT_RESEARCH_JOB,
                Some(job_id),
                format!("already finalized as {}", row.status),
            );
        }
        StorageError::state_conflict(
            QUANT_RESEARCH_JOB,
            Some(job_id),
            format!(
                "cannot finalize from status {} (lease_owner={:?})",
                row.status, row.lease_owner
            ),
        )
    }
}

/// A `running` row is orphaned when it is owned by a different (dead) lease
/// epoch, has no owner, or its lease has expired past `now`.
fn orphaned_condition(owner: &WorkerId, now: DateTime<Utc>) -> Condition {
    Condition::all()
        .add(Column::Status.eq(ResearchJobStatus::Running))
        .add(
            Condition::any()
                .add(Column::LeaseOwner.ne(owner))
                .add(Column::LeaseOwner.is_null())
                .add(Column::LeaseExpiresAt.lt(now)),
        )
}

/// This owner's own still-`running` rows (lease validity irrelevant): the
/// graceful-drain set, requeued so a new epoch re-leases without waiting for
/// lease expiry.
fn owned_running_condition(owner: &WorkerId) -> Condition {
    Condition::all()
        .add(Column::Status.eq(ResearchJobStatus::Running))
        .add(Column::LeaseOwner.eq(owner))
}

fn page_condition(query: &ResearchJobListQuery) -> Condition {
    Condition::all()
        .add_option(query.result_kind.map(|kind| Column::ResultKind.eq(kind)))
        .add_option(query.kind.map(|kind| Column::Kind.eq(kind)))
        .add_option(query.status.map(|status| Column::Status.eq(status)))
        .add_option(query.model_spec_id.map(|id| Column::ModelSpecId.eq(id)))
        .add_option(query.result_ref.map(|id| Column::ResultRef.eq(id)))
        .add_option(query.from.map(|from| Column::CreatedAt.gte(from)))
        .add_option(query.to.map(|to| Column::CreatedAt.lt(to)))
}
