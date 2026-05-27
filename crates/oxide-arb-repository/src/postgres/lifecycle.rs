use super::orm::{
    ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait, NotSet, QueryOrder,
    QuerySelect, Set,
};
use crate::traits::LifecycleRepository;
use chrono::Utc;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{LifecycleEventInfo, NewLifecycleEvent},
    entities::lifecycle_event::{ActiveModel, Column, Entity},
};

pub struct PgLifecycleRepository {
    db: DatabaseConnection,
}

impl PgLifecycleRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgLifecycleRepositoryTxn<'_> {
        PgLifecycleRepositoryTxn { txn }
    }
}

pub struct PgLifecycleRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

async fn do_create(
    db: &impl ConnectionTrait,
    event: NewLifecycleEvent,
) -> Result<LifecycleEventInfo, StorageError> {
    let model = ActiveModel {
        id: NotSet,
        phase: Set(event.phase),
        stage: Set(event.stage),
        message: Set(event.message),
        metadata: Set(event.metadata),
        created_at: Set(Utc::now()),
    };

    let result = Entity::insert(model)
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;
    Ok(result.into())
}

async fn do_get_recent(
    db: &impl ConnectionTrait,
    limit: u64,
) -> Result<Vec<LifecycleEventInfo>, StorageError> {
    Entity::find()
        .order_by_desc(Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

impl LifecycleRepository for PgLifecycleRepository {
    async fn create(&self, event: NewLifecycleEvent) -> Result<LifecycleEventInfo, StorageError> {
        do_create(&self.db, event).await
    }

    async fn get_recent(&self, limit: u64) -> Result<Vec<LifecycleEventInfo>, StorageError> {
        do_get_recent(&self.db, limit).await
    }
}

impl LifecycleRepository for PgLifecycleRepositoryTxn<'_> {
    async fn create(&self, event: NewLifecycleEvent) -> Result<LifecycleEventInfo, StorageError> {
        do_create(self.txn, event).await
    }

    async fn get_recent(&self, limit: u64) -> Result<Vec<LifecycleEventInfo>, StorageError> {
        do_get_recent(self.txn, limit).await
    }
}
