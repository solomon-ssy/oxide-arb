use crate::traits::OutboxRepository;
use num_traits::ToPrimitive;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::{NewOutboxEventWithId, OutboxEventInfo, UpdateOutboxEvent};
use oxide_arb_models::entities::outbox_event::{self, Column, Entity};
use oxide_arb_models::types::OutboxEventId;
use sea_orm::sea_query::{LockBehavior, LockType};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection,
    DatabaseTransaction, EntityTrait, IntoActiveModel, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect,
};

pub struct PgOutboxRepository {
    db: DatabaseConnection,
}

impl PgOutboxRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgOutboxRepositoryTxn<'_> {
        PgOutboxRepositoryTxn { txn }
    }
}

pub struct PgOutboxRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

async fn do_create(
    db: &impl ConnectionTrait,
    event: NewOutboxEventWithId,
) -> Result<OutboxEventInfo, StorageError> {
    let am: outbox_event::ActiveModel = event.into_active_model();
    let model = am.insert(db).await.map_err(StorageError::from)?;
    Ok(model.into())
}

async fn do_fetch_pending(
    db: &impl ConnectionTrait,
    limit: usize,
) -> Result<Vec<OutboxEventInfo>, StorageError> {
    Entity::find()
        .filter(Column::PublishedAt.is_null())
        .filter(Column::DeadLetterReason.is_null())
        .order_by_asc(Column::CreatedAt)
        .limit(ToPrimitive::to_u64(&limit).unwrap_or(u64::MAX))
        .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_update(
    db: &impl ConnectionTrait,
    event_id: &OutboxEventId,
    update: &UpdateOutboxEvent,
) -> Result<(), StorageError> {
    let model = Entity::find_by_id(event_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "outbox_event",
            id: event_id.to_string(),
        })?;

    let mut am: outbox_event::ActiveModel = model.into();
    if let Some(published_at) = update.published_at {
        am.published_at = ActiveValue::Set(Some(published_at));
    }
    if let Some(ref reason) = update.dead_letter_reason {
        am.dead_letter_reason = ActiveValue::Set(reason.clone());
    }
    am.update(db).await.map_err(StorageError::from)?;
    Ok(())
}

async fn do_dead_letter_count(db: &impl ConnectionTrait) -> Result<u64, StorageError> {
    Entity::find()
        .filter(Column::DeadLetterReason.is_not_null())
        .count(db)
        .await
        .map_err(StorageError::from)
}

impl OutboxRepository for PgOutboxRepository {
    async fn create(&self, event: NewOutboxEventWithId) -> Result<OutboxEventInfo, StorageError> {
        do_create(&self.db, event).await
    }

    async fn fetch_pending(&self, limit: usize) -> Result<Vec<OutboxEventInfo>, StorageError> {
        do_fetch_pending(&self.db, limit).await
    }

    async fn update(
        &self,
        event_id: &OutboxEventId,
        update: UpdateOutboxEvent,
    ) -> Result<(), StorageError> {
        do_update(&self.db, event_id, &update).await
    }

    async fn dead_letter_count(&self) -> Result<u64, StorageError> {
        do_dead_letter_count(&self.db).await
    }
}

impl OutboxRepository for PgOutboxRepositoryTxn<'_> {
    async fn create(&self, event: NewOutboxEventWithId) -> Result<OutboxEventInfo, StorageError> {
        do_create(self.txn, event).await
    }

    async fn fetch_pending(&self, limit: usize) -> Result<Vec<OutboxEventInfo>, StorageError> {
        do_fetch_pending(self.txn, limit).await
    }

    async fn update(
        &self,
        event_id: &OutboxEventId,
        update: UpdateOutboxEvent,
    ) -> Result<(), StorageError> {
        do_update(self.txn, event_id, &update).await
    }

    async fn dead_letter_count(&self) -> Result<u64, StorageError> {
        do_dead_letter_count(self.txn).await
    }
}
