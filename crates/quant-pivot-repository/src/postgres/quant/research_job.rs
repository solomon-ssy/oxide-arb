//! Postgres-backed durable research-job ledger repository.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_RESEARCH_JOB};
use quant_pivot_models::{
    domain::{
        api::ResearchJobListQuery,
        pagination::{PageWindow, Paginated},
        quant::{NewResearchJob, ResearchJobInfo, ResearchJobResultRef},
    },
    entities::quant_research_job::{Column, Entity},
    enums::quant::{ResearchJobErrorCode, ResearchJobKind, ResearchJobStatus},
    types::{DatasetCoverage, ResearchJobError, ResearchJobId, ResearchJobProgress, WorkerId},
};
use sea_orm::{
    ActiveValue::Set,
    ColumnTrait, Condition, DatabaseConnection, EntityTrait, ExprTrait, FromQueryResult,
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
    sea_query::{Expr, LockBehavior, LockType},
};

use crate::{
    postgres::{error, primitives, query::paginate_mapped},
    traits::{KindRunningCount, ReclaimOutcome, ResearchJobRepository},
};

/// Postgres-backed durable research-job ledger repository.
pub struct PgResearchJobRepository {
    db: DatabaseConnection,
}

impl PgResearchJobRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[derive(Debug, FromQueryResult)]
struct KindCountRow {
    kind: ResearchJobKind,
    running: i64,
}

#[async_trait::async_trait]
impl ResearchJobRepository for PgResearchJobRepository {
    async fn enqueue(&self, job: NewResearchJob) -> Result<ResearchJobInfo, StorageError> {
        Entity::insert(job.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
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
            .filter(Column::Status.eq(ResearchJobStatus::Queued))
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
        let now = Utc::now();
        let mut active = model.into_active_model();
        active.status = Set(ResearchJobStatus::Running);
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
        active.heartbeat_at = Set(Some(Utc::now()));
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
        status: ResearchJobStatus,
        result: Option<ResearchJobResultRef>,
        error: Option<ResearchJobError>,
        coverage: Option<DatasetCoverage>,
    ) -> Result<ResearchJobInfo, StorageError> {
        debug_assert!(status.is_terminal(), "finalize requires a terminal status");
        // Single conditional UPDATE: only the lease holder may terminalize.
        let (result_kind, result_ref) =
            result.map_or((None, None), |result| (Some(result.kind), Some(result.id)));
        let result = Entity::update_many()
            .col_expr(Column::Status, primitives::enum_value(&status))
            .col_expr(Column::ResultKind, Expr::value(result_kind))
            .col_expr(Column::ResultRef, Expr::value(result_ref))
            .col_expr(Column::ErrorJson, Expr::value(error))
            .col_expr(Column::CoverageJson, Expr::value(coverage))
            .col_expr(Column::FinishedAt, Expr::value(Utc::now()))
            .col_expr(Column::LeaseOwner, Expr::value(None::<WorkerId>))
            .col_expr(Column::LeaseExpiresAt, Expr::value(None::<DateTime<Utc>>))
            .filter(Column::JobId.eq(*job_id))
            .filter(Column::Status.eq(ResearchJobStatus::Running))
            .filter(Column::LeaseOwner.eq(owner))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        if result.rows_affected == 0 {
            return Err(finalize_guard_failure(&self.db, job_id).await);
        }
        Entity::find_by_id(*job_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Into::into)
            .ok_or_else(|| StorageError::not_found(QUANT_RESEARCH_JOB, job_id))
    }

    async fn cancel_if_queued(
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
            .col_expr(Column::FinishedAt, Expr::value(Utc::now()))
            .filter(Column::JobId.eq(*job_id))
            .filter(Column::Status.eq(ResearchJobStatus::Queued))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(result.rows_affected > 0)
    }

    async fn reclaim_orphaned(
        &self,
        owner: &WorkerId,
        now: DateTime<Utc>,
    ) -> Result<ReclaimOutcome, StorageError> {
        // Boot recovery: reclaim rows whose lease is expired / dead-epoch.
        reclaim_by_condition(&self.db, orphaned_condition(owner, now)).await
    }

    async fn requeue_inflight(&self, owner: &WorkerId) -> Result<ReclaimOutcome, StorageError> {
        // Graceful drain: reclaim this owner's own still-`running` rows (lease may
        // still be valid) so a new epoch re-leases them without a lease-expiry wait.
        reclaim_by_condition(&self.db, owned_running_condition(owner)).await
    }
}

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
        .col_expr(Column::FinishedAt, Expr::value(Utc::now()))
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
            primitives::enum_value(&ResearchJobStatus::Queued),
        )
        .col_expr(
            Column::RecoveryAttempt,
            Expr::col(Column::RecoveryAttempt).add(1),
        )
        .col_expr(Column::LeaseOwner, Expr::value(None::<WorkerId>))
        .col_expr(Column::LeaseExpiresAt, Expr::value(None::<DateTime<Utc>>))
        .col_expr(
            Column::ProgressJson,
            Expr::value(None::<ResearchJobProgress>),
        )
        .col_expr(Column::StartedAt, Expr::value(None::<DateTime<Utc>>))
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

/// Diagnose why a guarded finalize touched zero rows.
async fn finalize_guard_failure(db: &DatabaseConnection, job_id: &ResearchJobId) -> StorageError {
    let row = match Entity::find_by_id(*job_id).one(db).await {
        Ok(row) => row,
        Err(err) => return StorageError::from(err),
    };
    let Some(row) = row else {
        return StorageError::not_found(QUANT_RESEARCH_JOB, job_id);
    };
    if row.status.is_terminal() {
        return error::state_conflict(
            QUANT_RESEARCH_JOB,
            Some(job_id),
            format!("already finalized as {}", row.status),
        );
    }
    error::state_conflict(
        QUANT_RESEARCH_JOB,
        Some(job_id),
        format!(
            "cannot finalize from status {} (lease_owner={:?})",
            row.status, row.lease_owner
        ),
    )
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
