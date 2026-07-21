//! Postgres-backed domain-source ingest cursor repository.

use chrono::Utc;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::data_plane::{DomainSourceCursorInfo, UpsertDomainSourceCursor},
    entities::quant_domain_source_cursor::{Column, Entity},
    types::{DomainInstrumentKey, DomainSourceId},
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    sea_query::OnConflict,
};

use crate::traits::DomainSourceCursorRepository;

/// Postgres-backed checkpoint store for external domain-source ingestion.
pub struct PgDomainSourceCursorRepository {
    db: DatabaseConnection,
}

impl PgDomainSourceCursorRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl DomainSourceCursorRepository for PgDomainSourceCursorRepository {
    async fn find(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
    ) -> Result<Option<DomainSourceCursorInfo>, StorageError> {
        Entity::find()
            .filter(Column::SourceId.eq(source_id.clone()))
            .filter(Column::InstrumentKey.eq(instrument_key.clone()))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn upsert(
        &self,
        mut cursor: UpsertDomainSourceCursor,
    ) -> Result<DomainSourceCursorInfo, StorageError> {
        cursor.updated_at = Utc::now();
        Entity::insert(cursor.into_active_model())
            .on_conflict(
                OnConflict::columns([Column::SourceId, Column::InstrumentKey])
                    .update_columns([
                        Column::CheckpointJson,
                        Column::CheckpointHash,
                        Column::Status,
                        Column::LastError,
                        Column::UpdatedAt,
                    ])
                    .to_owned(),
            )
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn list_all(&self) -> Result<Vec<DomainSourceCursorInfo>, StorageError> {
        Entity::find()
            .order_by_asc(Column::SourceId)
            .order_by_asc(Column::InstrumentKey)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
