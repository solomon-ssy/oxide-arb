//! Postgres-backed on-chain trade-tape block cursor repository.

use chrono::Utc;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::data_plane::{
        TradeTapeBlockCursorInfo, TradeTapeSourceKind, UpsertTradeTapeBlockCursor,
    },
    entities::quant_trade_tape_block_cursor::{Column, Entity},
    types::EvmAddress,
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    sea_query::OnConflict,
};

use crate::traits::TradeTapeBlockCursorRepository;

/// Postgres-backed checkpoint store for on-chain trade-tape ingestion.
pub struct PgTradeTapeBlockCursorRepository {
    db: DatabaseConnection,
}

impl PgTradeTapeBlockCursorRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl TradeTapeBlockCursorRepository for PgTradeTapeBlockCursorRepository {
    async fn find(
        &self,
        source: TradeTapeSourceKind,
        contract_address: &EvmAddress,
    ) -> Result<Option<TradeTapeBlockCursorInfo>, StorageError> {
        Entity::find()
            .filter(Column::Source.eq(source))
            .filter(Column::ContractAddress.eq(contract_address))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn upsert(
        &self,
        mut cursor: UpsertTradeTapeBlockCursor,
    ) -> Result<TradeTapeBlockCursorInfo, StorageError> {
        cursor.updated_at = Utc::now();
        Entity::insert(cursor.into_active_model())
            .on_conflict(
                OnConflict::columns([Column::Source, Column::ContractAddress])
                    .update_columns([
                        Column::LastFinalizedBlock,
                        Column::LastLogIndex,
                        Column::HeadLagBlocks,
                        Column::Status,
                        Column::UpdatedAt,
                    ])
                    .to_owned(),
            )
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn list_by_source(
        &self,
        source: TradeTapeSourceKind,
    ) -> Result<Vec<TradeTapeBlockCursorInfo>, StorageError> {
        Entity::find()
            .filter(Column::Source.eq(source))
            .order_by_desc(Column::HeadLagBlocks)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
