use std::collections::HashSet;

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::market::{EventInfo, UpsertEvent},
    entities::event::{Column, Entity},
    enums::market::EventStatus,
    types::EventId,
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, DatabaseTransaction, EntityTrait, IntoActiveModel,
    QueryFilter, sea_query::OnConflict,
};

use crate::{
    postgres::{
        catalog::ingest::{find_existing_chunks, find_str_id_chunks},
        connection::RepositoryConnection,
        primitives,
        write::upsert_many_chunked,
    },
    traits::EventRepository,
};

pub struct PgEventRepository<C = DatabaseConnection> {
    db: C,
}

impl PgEventRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub(crate) const fn with_txn(
        txn: &DatabaseTransaction,
    ) -> PgEventRepository<&'_ DatabaseTransaction> {
        PgEventRepository { db: txn }
    }
}

/// `ON CONFLICT (event_id) DO UPDATE` clause shared by single and batch upserts.
fn event_upsert_on_conflict() -> OnConflict {
    OnConflict::column(Column::EventId)
        .update_columns([
            Column::Title,
            Column::Slug,
            Column::Tags,
            Column::NegRisk,
            Column::CatalogMarketIds,
            Column::EndDate,
            Column::ContentHash,
        ])
        .values([(
            Column::Status,
            primitives::excluded_enum::<EventStatus>(Column::Status),
        )])
        .to_owned()
}

#[async_trait::async_trait]
impl<C> EventRepository for PgEventRepository<C>
where
    C: RepositoryConnection,
{
    async fn find_by_id(&self, id: &EventId) -> Result<Option<EventInfo>, StorageError> {
        Entity::find_by_id(id.clone())
            .one(self.db.connection())
            .await
            .map_err(StorageError::from)
            .map(|event| event.map(Into::into))
    }

    async fn find_by_ids(&self, ids: &[EventId]) -> Result<Vec<EventInfo>, StorageError> {
        find_str_id_chunks::<Entity, _, _, _>(
            self.db.connection(),
            ids,
            Column::EventId,
            EventId::as_str,
        )
        .await
        .map(|events| events.into_iter().map(Into::into).collect())
    }

    async fn find_active(&self) -> Result<Vec<EventInfo>, StorageError> {
        Entity::find()
            .filter(Column::Status.eq(EventStatus::Active))
            .all(self.db.connection())
            .await
            .map_err(StorageError::from)
            .map(|events| events.into_iter().map(Into::into).collect())
    }

    async fn find_existing_ids(&self, ids: &[EventId]) -> Result<HashSet<String>, StorageError> {
        find_existing_chunks::<Entity, _, _, _>(
            self.db.connection(),
            ids,
            Column::EventId,
            EventId::as_str,
        )
        .await
    }

    async fn upsert(&self, dto: UpsertEvent) -> Result<EventInfo, StorageError> {
        let model = Entity::insert(dto.into_active_model())
            .on_conflict(event_upsert_on_conflict())
            .exec_with_returning(self.db.connection())
            .await
            .map_err(StorageError::from)?;
        Ok(model.into())
    }

    async fn upsert_batch(&self, dtos: Vec<UpsertEvent>) -> Result<u64, StorageError> {
        upsert_many_chunked::<Entity, UpsertEvent>(
            self.db.connection(),
            dtos,
            event_upsert_on_conflict(),
        )
        .await
    }
}
