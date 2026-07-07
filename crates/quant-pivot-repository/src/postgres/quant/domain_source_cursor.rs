//! Postgres-backed domain-source ingest cursor repository.

use crate::traits::DomainSourceCursorRepository;
use chrono::Utc;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{DomainSourceCursorInfo, UpsertDomainSourceCursor},
    entities::quant_domain_source_cursor,
    types::{DomainInstrumentKey, DomainSourceId},
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    sea_query::OnConflict,
};

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
        quant_domain_source_cursor::Entity::find()
            .filter(quant_domain_source_cursor::Column::SourceId.eq(source_id.clone()))
            .filter(quant_domain_source_cursor::Column::InstrumentKey.eq(instrument_key.clone()))
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
        quant_domain_source_cursor::Entity::insert(cursor.into_active_model())
            .on_conflict(
                OnConflict::columns([
                    quant_domain_source_cursor::Column::SourceId,
                    quant_domain_source_cursor::Column::InstrumentKey,
                ])
                .update_columns([
                    quant_domain_source_cursor::Column::LastEventTime,
                    quant_domain_source_cursor::Column::Status,
                    quant_domain_source_cursor::Column::UpdatedAt,
                ])
                .to_owned(),
            )
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn list_all(&self) -> Result<Vec<DomainSourceCursorInfo>, StorageError> {
        quant_domain_source_cursor::Entity::find()
            .order_by_asc(quant_domain_source_cursor::Column::SourceId)
            .order_by_asc(quant_domain_source_cursor::Column::InstrumentKey)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
