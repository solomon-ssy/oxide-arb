//! Postgres implementation of [`OperationLogRepository`].
//!
//! The table is append-only at the database level (WORM trigger), so this
//! repository exposes only INSERT and SELECT. No method ever issues an UPDATE
//! or DELETE.

use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{NewOperationLog, OperationLogInfo, OperationLogQuery, Paginated},
    entities::operation_log::{ActiveModel, Column, Entity},
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, IntoActiveModel, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, TransactionTrait, sea_query::Condition,
};

use crate::{batch, traits::OperationLogRepository};

/// Number of columns in the `operation_log` table, for bind-variable budgeting.
const OPERATION_LOG_COLUMNS: usize = 19;

/// Operation-log repository backed by Postgres.
pub struct PgOperationLogRepository {
    db: DatabaseConnection,
}

impl PgOperationLogRepository {
    /// Create a repository over the given connection handle.
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

async fn do_append(db: &impl ConnectionTrait, log: NewOperationLog) -> Result<(), StorageError> {
    Entity::insert(log.into_active_model())
        .exec_without_returning(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

async fn do_append_batch(
    db: &DatabaseConnection,
    logs: Vec<NewOperationLog>,
) -> Result<(), StorageError> {
    if logs.is_empty() {
        return Ok(());
    }

    let chunk_size = batch::max_rows_per_insert(OPERATION_LOG_COLUMNS);
    let txn = db.begin().await.map_err(StorageError::from)?;
    let mut chunk: Vec<ActiveModel> = Vec::with_capacity(chunk_size);
    for log in logs {
        chunk.push(log.into_active_model());
        if chunk.len() < chunk_size {
            continue;
        }
        let models = std::mem::take(&mut chunk);
        Entity::insert_many(models)
            .exec_without_returning(&txn)
            .await
            .map_err(StorageError::from)?;
    }
    if !chunk.is_empty() {
        Entity::insert_many(chunk)
            .exec_without_returning(&txn)
            .await
            .map_err(StorageError::from)?;
    }
    txn.commit().await.map_err(StorageError::from)?;
    Ok(())
}

fn page_condition(query: &OperationLogQuery) -> Condition {
    let mut condition = Condition::all();
    if let Some(actor) = &query.actor_user_id {
        condition = condition.add(Column::ActorUserId.eq(actor.clone()));
    }
    if let Some(category) = query.category {
        condition = condition.add(Column::Category.eq(category));
    }
    if let Some(resource) = query.resource_type {
        condition = condition.add(Column::ResourceType.eq(resource));
    }
    if let Some(outcome) = query.outcome {
        condition = condition.add(Column::Outcome.eq(outcome));
    }
    if let Some(request_id) = query.request_id.as_deref().filter(|id| !id.is_empty()) {
        condition = condition.add(Column::RequestId.eq(request_id));
    }
    if let Some(from) = query.from {
        condition = condition.add(Column::OccurredAt.gte(from));
    }
    if let Some(to) = query.to {
        condition = condition.add(Column::OccurredAt.lt(to));
    }
    condition
}

async fn do_page(
    db: &impl ConnectionTrait,
    query: OperationLogQuery,
) -> Result<Paginated<OperationLogInfo>, StorageError> {
    let window = query.page.normalized();
    let condition = page_condition(&query);

    let total = Entity::find()
        .filter(condition.clone())
        .count(db)
        .await
        .map_err(StorageError::from)?;

    if total == 0 {
        return Ok(Paginated::from_request(Vec::new(), total, &window));
    }

    let models = Entity::find()
        .filter(condition)
        .order_by_desc(Column::OccurredAt)
        .offset(window.offset())
        .limit(window.limit())
        .all(db)
        .await
        .map_err(StorageError::from)?;
    let items = models.into_iter().map(Into::into).collect();
    Ok(Paginated::from_request(items, total, &window))
}

#[async_trait::async_trait]
impl OperationLogRepository for PgOperationLogRepository {
    async fn append(&self, log: NewOperationLog) -> Result<(), StorageError> {
        do_append(&self.db, log).await
    }

    async fn append_batch(&self, logs: Vec<NewOperationLog>) -> Result<(), StorageError> {
        do_append_batch(&self.db, logs).await
    }

    async fn page(
        &self,
        query: OperationLogQuery,
    ) -> Result<Paginated<OperationLogInfo>, StorageError> {
        do_page(&self.db, query).await
    }
}
