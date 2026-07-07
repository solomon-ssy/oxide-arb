//! Postgres-backed on-chain trade-tape block cursor repository.

use crate::traits::TradeTapeBlockCursorRepository;
use chrono::Utc;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{TradeTapeBlockCursorInfo, UpsertTradeTapeBlockCursor},
    entities::quant_trade_tape_block_cursor,
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    sea_query::OnConflict,
};

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
        source: &str,
        contract_address: &str,
    ) -> Result<Option<TradeTapeBlockCursorInfo>, StorageError> {
        quant_trade_tape_block_cursor::Entity::find()
            .filter(quant_trade_tape_block_cursor::Column::Source.eq(source))
            .filter(quant_trade_tape_block_cursor::Column::ContractAddress.eq(contract_address))
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
        quant_trade_tape_block_cursor::Entity::insert(cursor.into_active_model())
            .on_conflict(
                OnConflict::columns([
                    quant_trade_tape_block_cursor::Column::Source,
                    quant_trade_tape_block_cursor::Column::ContractAddress,
                ])
                .update_columns([
                    quant_trade_tape_block_cursor::Column::LastFinalizedBlock,
                    quant_trade_tape_block_cursor::Column::LastLogIndex,
                    quant_trade_tape_block_cursor::Column::HeadLagBlocks,
                    quant_trade_tape_block_cursor::Column::Status,
                    quant_trade_tape_block_cursor::Column::UpdatedAt,
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
        source: &str,
    ) -> Result<Vec<TradeTapeBlockCursorInfo>, StorageError> {
        quant_trade_tape_block_cursor::Entity::find()
            .filter(quant_trade_tape_block_cursor::Column::Source.eq(source))
            .order_by_desc(quant_trade_tape_block_cursor::Column::HeadLagBlocks)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
