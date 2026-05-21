use crate::traits::EventRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::event::{self, ActiveModel, Column, Entity};
use oxide_arb_models::types::EventId;
#[allow(clippy::wildcard_imports)]
use sea_orm::*;
use std::collections::HashSet;

pub struct PgEventRepository {
    db: DatabaseConnection,
}

impl PgEventRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgEventRepositoryTxn<'_> {
        PgEventRepositoryTxn { txn }
    }
}

pub struct PgEventRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

async fn do_find_by_id(
    db: &impl ConnectionTrait,
    id: &EventId,
) -> Result<Option<event::Model>, StorageError> {
    Entity::find_by_id(id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)
}

async fn do_find_active(db: &impl ConnectionTrait) -> Result<Vec<event::Model>, StorageError> {
    Entity::find()
        .filter(Column::Status.eq("active"))
        .all(db)
        .await
        .map_err(StorageError::from)
}

async fn do_find_existing_ids(
    db: &impl ConnectionTrait,
    ids: &[EventId],
) -> Result<HashSet<String>, StorageError> {
    if ids.is_empty() {
        return Ok(HashSet::new());
    }
    let id_strs: Vec<&str> = ids.iter().map(EventId::as_str).collect();
    let rows = Entity::find()
        .filter(Column::EventId.is_in(id_strs))
        .select_only()
        .column(Column::EventId)
        .into_tuple::<String>()
        .all(db)
        .await?;
    Ok(rows.into_iter().collect())
}

async fn do_insert(
    db: &impl ConnectionTrait,
    model: ActiveModel,
) -> Result<event::Model, StorageError> {
    Entity::insert(model)
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)
}

async fn do_insert_batch(
    db: &impl ConnectionTrait,
    models: Vec<ActiveModel>,
) -> Result<u64, StorageError> {
    if models.is_empty() {
        return Ok(0);
    }
    let count = models.len() as u64;
    Entity::insert_many(models)
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(count)
}

async fn do_update(
    db: &impl ConnectionTrait,
    model: ActiveModel,
) -> Result<event::Model, StorageError> {
    model.update(db).await.map_err(StorageError::from)
}

impl EventRepository for PgEventRepository {
    async fn find_by_id(&self, id: &EventId) -> Result<Option<event::Model>, StorageError> {
        do_find_by_id(&self.db, id).await
    }

    async fn find_active(&self) -> Result<Vec<event::Model>, StorageError> {
        do_find_active(&self.db).await
    }

    async fn find_existing_ids(&self, ids: &[EventId]) -> Result<HashSet<String>, StorageError> {
        do_find_existing_ids(&self.db, ids).await
    }

    async fn insert(&self, model: ActiveModel) -> Result<event::Model, StorageError> {
        do_insert(&self.db, model).await
    }

    async fn insert_batch(&self, models: Vec<ActiveModel>) -> Result<u64, StorageError> {
        do_insert_batch(&self.db, models).await
    }

    async fn update(&self, model: ActiveModel) -> Result<event::Model, StorageError> {
        do_update(&self.db, model).await
    }
}

impl EventRepository for PgEventRepositoryTxn<'_> {
    async fn find_by_id(&self, id: &EventId) -> Result<Option<event::Model>, StorageError> {
        do_find_by_id(self.txn, id).await
    }

    async fn find_active(&self) -> Result<Vec<event::Model>, StorageError> {
        do_find_active(self.txn).await
    }

    async fn find_existing_ids(&self, ids: &[EventId]) -> Result<HashSet<String>, StorageError> {
        do_find_existing_ids(self.txn, ids).await
    }

    async fn insert(&self, model: ActiveModel) -> Result<event::Model, StorageError> {
        do_insert(self.txn, model).await
    }

    async fn insert_batch(&self, models: Vec<ActiveModel>) -> Result<u64, StorageError> {
        do_insert_batch(self.txn, models).await
    }

    async fn update(&self, model: ActiveModel) -> Result<event::Model, StorageError> {
        do_update(self.txn, model).await
    }
}
