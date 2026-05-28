use super::orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    QueryFilter, QueryOrder, Set,
};
use crate::traits::ResolutionEventRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::settlement::{NewResolutionEvent, ResolutionEventInfo},
    entities::resolution_event::{ActiveModel, Column, Entity},
    types::MarketId,
};

pub struct PgResolutionEventRepository {
    db: DatabaseConnection,
}

impl PgResolutionEventRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgResolutionEventRepositoryTxn<'_> {
        PgResolutionEventRepositoryTxn { txn }
    }
}

pub struct PgResolutionEventRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

#[async_trait::async_trait]
impl ResolutionEventRepository for PgResolutionEventRepository {
    async fn append(&self, event: NewResolutionEvent) -> Result<(), StorageError> {
        append_q(&self.db, event).await
    }

    async fn latest_for_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<ResolutionEventInfo>, StorageError> {
        latest_for_market_q(&self.db, market_id).await
    }

    async fn latest_by_source(
        &self,
        market_id: &MarketId,
        source: &str,
    ) -> Result<Option<ResolutionEventInfo>, StorageError> {
        latest_by_source_q(&self.db, market_id, source).await
    }
}

#[async_trait::async_trait]
impl ResolutionEventRepository for PgResolutionEventRepositoryTxn<'_> {
    async fn append(&self, event: NewResolutionEvent) -> Result<(), StorageError> {
        append_q(self.txn, event).await
    }

    async fn latest_for_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<ResolutionEventInfo>, StorageError> {
        latest_for_market_q(self.txn, market_id).await
    }

    async fn latest_by_source(
        &self,
        market_id: &MarketId,
        source: &str,
    ) -> Result<Option<ResolutionEventInfo>, StorageError> {
        latest_by_source_q(self.txn, market_id, source).await
    }
}

async fn append_q(
    db: &impl sea_orm::ConnectionTrait,
    event: NewResolutionEvent,
) -> Result<(), StorageError> {
    active_model(event)
        .insert(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

async fn latest_for_market_q(
    db: &impl sea_orm::ConnectionTrait,
    market_id: &MarketId,
) -> Result<Option<ResolutionEventInfo>, StorageError> {
    Entity::find()
        .filter(Column::MarketId.eq(market_id.as_str()))
        .order_by_desc(Column::CreatedAt)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(Into::into))
}

async fn latest_by_source_q(
    db: &impl sea_orm::ConnectionTrait,
    market_id: &MarketId,
    source: &str,
) -> Result<Option<ResolutionEventInfo>, StorageError> {
    Entity::find()
        .filter(Column::MarketId.eq(market_id.as_str()))
        .filter(Column::Source.eq(source))
        .order_by_desc(Column::CreatedAt)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(Into::into))
}

fn active_model(event: NewResolutionEvent) -> ActiveModel {
    ActiveModel {
        resolution_id: Set(event.resolution_id),
        market_id: Set(event.market_id),
        outcome: Set(event.outcome),
        source: Set(event.source),
        gamma_agrees: Set(event.gamma_agrees),
        ctf_agrees: Set(event.ctf_agrees),
        evidence: Set(event.evidence),
        resolved_at: Set(event.resolved_at),
        created_at: Set(event.created_at),
    }
}
