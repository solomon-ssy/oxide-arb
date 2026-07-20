//! Postgres-backed market selection snapshot repository.

use crate::{
    postgres::{query::find_models_by_id_chunks, write::insert_many_chunked},
    traits::MarketSelectionRepository,
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        MarketSelectionInfo, MarketSelectionMemberInfo, NewMarketSelection,
        NewMarketSelectionMember,
    },
    entities::{quant_market_selection, quant_market_selection_member},
    types::MarketSelectionId,
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    TransactionTrait,
};

/// Postgres-backed market selection snapshot repository.
pub struct PgMarketSelectionRepository {
    db: DatabaseConnection,
}

impl PgMarketSelectionRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl MarketSelectionRepository for PgMarketSelectionRepository {
    async fn create_snapshot(
        &self,
        snapshot: NewMarketSelection,
        members: Vec<NewMarketSelectionMember>,
    ) -> Result<MarketSelectionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let snapshot_model = quant_market_selection::Entity::insert(snapshot.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;
        insert_many_chunked::<quant_market_selection_member::Entity, _>(&txn, members).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(snapshot_model.into())
    }

    async fn find_by_id(
        &self,
        snapshot_id: &MarketSelectionId,
    ) -> Result<Option<MarketSelectionInfo>, StorageError> {
        quant_market_selection::Entity::find_by_id(snapshot_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn list_members(
        &self,
        snapshot_id: &MarketSelectionId,
    ) -> Result<Vec<MarketSelectionMemberInfo>, StorageError> {
        quant_market_selection_member::Entity::find()
            .filter(
                quant_market_selection_member::Column::MarketSelectionId.eq(snapshot_id.clone()),
            )
            .order_by_asc(quant_market_selection_member::Column::MarketId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn list_members_by_snapshot_ids(
        &self,
        snapshot_ids: &[MarketSelectionId],
    ) -> Result<Vec<MarketSelectionMemberInfo>, StorageError> {
        let mut rows = find_models_by_id_chunks::<quant_market_selection_member::Entity, _, _>(
            &self.db,
            snapshot_ids,
            quant_market_selection_member::Column::MarketSelectionId,
        )
        .await?;
        rows.sort_by(|left, right| {
            left.market_selection_id
                .to_string()
                .cmp(&right.market_selection_id.to_string())
                .then_with(|| left.market_id.as_str().cmp(right.market_id.as_str()))
        });
        Ok(rows.into_iter().map(Into::into).collect())
    }
}
