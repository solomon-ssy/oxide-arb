use crate::{
    postgres::{
        catalog::ingest::{find_existing_str_id_chunks, find_models_by_str_id_chunks},
        write::upsert_many_chunked,
    },
    traits::EventRepository,
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{EventInfo, UpsertEvent},
    entities::event::{Column, Entity},
    enums::market::EventStatus,
    schema::column,
    types::EventId,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, QueryFilter, sea_query::OnConflict,
};
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
) -> Result<Option<EventInfo>, StorageError> {
    Entity::find_by_id(id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|opt| opt.map(Into::into))
}

async fn do_find_active(db: &impl ConnectionTrait) -> Result<Vec<EventInfo>, StorageError> {
    Entity::find()
        .filter(Column::Status.eq(EventStatus::Active))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_find_by_ids(
    db: &impl ConnectionTrait,
    ids: &[EventId],
) -> Result<Vec<EventInfo>, StorageError> {
    find_models_by_str_id_chunks::<Entity, _, _, _>(db, ids, Column::EventId, EventId::as_str)
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
}

async fn do_find_existing_ids(
    db: &impl ConnectionTrait,
    ids: &[EventId],
) -> Result<HashSet<String>, StorageError> {
    find_existing_str_id_chunks::<Entity, _, _, _>(db, ids, Column::EventId, EventId::as_str).await
}

/// `ON CONFLICT (event_id) DO UPDATE` clause shared by single and batch upserts.
fn event_upsert_on_conflict() -> OnConflict {
    OnConflict::column(Column::EventId)
        .update_columns([
            Column::Title,
            Column::Slug,
            Column::Tags,
            Column::NegRisk,
            Column::EndDate,
            Column::RawGamma,
        ])
        .values([(
            Column::Status,
            column::pg_enum_excluded::<EventStatus>(Column::Status),
        )])
        .to_owned()
}

async fn do_upsert(db: &impl ConnectionTrait, dto: UpsertEvent) -> Result<EventInfo, StorageError> {
    let model = Entity::insert(dto.into_active_model())
        .on_conflict(event_upsert_on_conflict())
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;
    Ok(model.into())
}

async fn do_upsert_batch(
    db: &impl ConnectionTrait,
    dtos: Vec<UpsertEvent>,
) -> Result<u64, StorageError> {
    upsert_many_chunked::<Entity, UpsertEvent>(db, dtos, event_upsert_on_conflict()).await
}

#[async_trait::async_trait]
impl EventRepository for PgEventRepository {
    async fn find_by_id(&self, id: &EventId) -> Result<Option<EventInfo>, StorageError> {
        do_find_by_id(&self.db, id).await
    }

    async fn find_by_ids(&self, ids: &[EventId]) -> Result<Vec<EventInfo>, StorageError> {
        do_find_by_ids(&self.db, ids).await
    }

    async fn find_active(&self) -> Result<Vec<EventInfo>, StorageError> {
        do_find_active(&self.db).await
    }

    async fn find_existing_ids(&self, ids: &[EventId]) -> Result<HashSet<String>, StorageError> {
        do_find_existing_ids(&self.db, ids).await
    }

    async fn upsert(&self, dto: UpsertEvent) -> Result<EventInfo, StorageError> {
        do_upsert(&self.db, dto).await
    }

    async fn upsert_batch(&self, dtos: Vec<UpsertEvent>) -> Result<u64, StorageError> {
        do_upsert_batch(&self.db, dtos).await
    }
}

#[async_trait::async_trait]
impl EventRepository for PgEventRepositoryTxn<'_> {
    async fn find_by_id(&self, id: &EventId) -> Result<Option<EventInfo>, StorageError> {
        do_find_by_id(self.txn, id).await
    }

    async fn find_by_ids(&self, ids: &[EventId]) -> Result<Vec<EventInfo>, StorageError> {
        do_find_by_ids(self.txn, ids).await
    }

    async fn find_active(&self) -> Result<Vec<EventInfo>, StorageError> {
        do_find_active(self.txn).await
    }

    async fn find_existing_ids(&self, ids: &[EventId]) -> Result<HashSet<String>, StorageError> {
        do_find_existing_ids(self.txn, ids).await
    }

    async fn upsert(&self, dto: UpsertEvent) -> Result<EventInfo, StorageError> {
        do_upsert(self.txn, dto).await
    }

    async fn upsert_batch(&self, dtos: Vec<UpsertEvent>) -> Result<u64, StorageError> {
        do_upsert_batch(self.txn, dtos).await
    }
}
