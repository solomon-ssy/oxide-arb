//! Favorite-longshot bias-table ledger repository trait (Phase 11.2.1).

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        BiasTableListQuery, FavoriteLongshotBiasTableInfo, NewFavoriteLongshotBiasTable, Paginated,
    },
    types::FavoriteLongshotBiasTableId,
};

/// Persistence port for the append-only, content-addressed favorite-longshot
/// bias-table ledger.
#[async_trait::async_trait]
pub trait FavoriteLongshotBiasTableRepository: Send + Sync {
    /// Insert a new bias-table row, returning the persisted projection.
    async fn create(
        &self,
        table: NewFavoriteLongshotBiasTable,
    ) -> Result<FavoriteLongshotBiasTableInfo, StorageError>;

    /// Look up a bias table by id.
    async fn find_by_id(
        &self,
        bias_table_id: &FavoriteLongshotBiasTableId,
    ) -> Result<Option<FavoriteLongshotBiasTableInfo>, StorageError>;

    /// Page the ledger for the operator catalog, newest (`created_at`) first.
    async fn page(
        &self,
        query: BiasTableListQuery,
    ) -> Result<Paginated<FavoriteLongshotBiasTableInfo>, StorageError>;
}
