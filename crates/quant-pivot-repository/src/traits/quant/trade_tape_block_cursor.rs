use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::data_plane::{
        TradeTapeBlockCursorInfo, TradeTapeSourceKind, UpsertTradeTapeBlockCursor,
    },
    types::EvmAddress,
};

/// Durable checkpoint repository for on-chain trade-tape block cursors.
#[async_trait::async_trait]
pub trait TradeTapeBlockCursorRepository: Send + Sync {
    async fn find(
        &self,
        source: TradeTapeSourceKind,
        contract_address: &EvmAddress,
    ) -> Result<Option<TradeTapeBlockCursorInfo>, StorageError>;

    async fn upsert(
        &self,
        cursor: UpsertTradeTapeBlockCursor,
    ) -> Result<TradeTapeBlockCursorInfo, StorageError>;

    async fn list_by_source(
        &self,
        source: TradeTapeSourceKind,
    ) -> Result<Vec<TradeTapeBlockCursorInfo>, StorageError>;
}
