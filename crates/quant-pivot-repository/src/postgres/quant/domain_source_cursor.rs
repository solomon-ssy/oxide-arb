//! Postgres-backed domain-source ingest cursor repository.

use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity::QUANT_DOMAIN_SOURCE_CURSOR};
use quant_pivot_models::{
    domain::data_plane::{
        DomainSourceCursorCasOutcome, DomainSourceCursorInfo, UpsertDomainSourceCursor,
    },
    entities::quant_domain_source_cursor::{Column, Entity, Model},
    types::{ContentHash, DomainInstrumentKey, DomainSourceId},
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, DbErr, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    sea_query::{Expr, OnConflict},
};

use crate::{postgres::primitives, traits::DomainSourceCursorRepository};

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
        let row = Entity::find()
            .filter(Column::SourceId.eq(source_id.clone()))
            .filter(Column::InstrumentKey.eq(instrument_key.clone()))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        row.map(validated_info).transpose()
    }

    async fn upsert(
        &self,
        mut cursor: UpsertDomainSourceCursor,
    ) -> Result<DomainSourceCursorInfo, StorageError> {
        validate_cursor(&cursor)?;
        cursor.updated_at = Utc::now();
        let row = Entity::insert(cursor.into_active_model())
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
            .map_err(StorageError::from)?;
        validated_info(row)
    }

    async fn compare_and_set(
        &self,
        expected_checkpoint_hash: Option<ContentHash>,
        mut cursor: UpsertDomainSourceCursor,
    ) -> Result<DomainSourceCursorCasOutcome, StorageError> {
        validate_cursor(&cursor)?;
        cursor.updated_at = primitives::statement_timestamp(&self.db).await?;
        let source_id = cursor.source_id.clone();
        let instrument_key = cursor.instrument_key.clone();
        let advanced = match expected_checkpoint_hash {
            None => match Entity::insert(cursor.into_active_model())
                .on_conflict(
                    OnConflict::columns([Column::SourceId, Column::InstrumentKey])
                        .do_nothing()
                        .to_owned(),
                )
                .exec_with_returning(&self.db)
                .await
            {
                Ok(row) => Some(row),
                Err(DbErr::RecordNotFound(_)) => None,
                Err(error) => return Err(StorageError::from(error)),
            },
            Some(expected_hash) => {
                let mut rows = Entity::update_many()
                    .col_expr(Column::CheckpointJson, Expr::value(cursor.checkpoint_json))
                    .col_expr(Column::CheckpointHash, Expr::value(cursor.checkpoint_hash))
                    .col_expr(Column::Status, primitives::enum_value(&cursor.status))
                    .col_expr(Column::LastError, Expr::value(cursor.last_error))
                    .col_expr(Column::UpdatedAt, Expr::value(cursor.updated_at))
                    .filter(Column::SourceId.eq(source_id.clone()))
                    .filter(Column::InstrumentKey.eq(instrument_key.clone()))
                    .filter(Column::CheckpointHash.eq(expected_hash))
                    .exec_with_returning(&self.db)
                    .await
                    .map_err(StorageError::from)?;
                let advanced = rows.pop();
                if !rows.is_empty() {
                    return Err(StorageError::invariant_violation(
                        Some(QUANT_DOMAIN_SOURCE_CURSOR),
                        "cursor compare-and-set updated more than one row",
                    ));
                }
                advanced
            }
        };
        if let Some(row) = advanced {
            return validated_info(row).map(DomainSourceCursorCasOutcome::Advanced);
        }
        self.find(&source_id, &instrument_key)
            .await?
            .map(DomainSourceCursorCasOutcome::Conflict)
            .ok_or_else(|| {
                StorageError::state_conflict(
                    QUANT_DOMAIN_SOURCE_CURSOR,
                    Some(format!("{source_id}/{instrument_key}")),
                    "cursor compare-and-set lost its expected row without a durable winner",
                )
            })
    }

    async fn list_all(&self) -> Result<Vec<DomainSourceCursorInfo>, StorageError> {
        let rows = Entity::find()
            .order_by_asc(Column::SourceId)
            .order_by_asc(Column::InstrumentKey)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        rows.into_iter().map(validated_info).collect()
    }
}

fn validate_cursor(cursor: &UpsertDomainSourceCursor) -> Result<(), StorageError> {
    cursor.validate().map_err(|detail| {
        StorageError::invariant_violation(Some(QUANT_DOMAIN_SOURCE_CURSOR), detail)
    })
}

fn validated_info(row: Model) -> Result<DomainSourceCursorInfo, StorageError> {
    let cursor: DomainSourceCursorInfo = row.into();
    cursor.validate().map_err(|detail| {
        StorageError::invariant_violation(
            Some(QUANT_DOMAIN_SOURCE_CURSOR),
            format!("stored cursor failed integrity validation: {detail}"),
        )
    })?;
    Ok(cursor)
}
