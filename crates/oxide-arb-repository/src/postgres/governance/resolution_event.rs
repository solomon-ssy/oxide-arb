use crate::traits::ResolutionEventRepository;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::settlement::{NewResolutionEvent, ResolutionEventInfo},
    entities::resolution_event::{Column, Entity},
    types::MarketId,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder,
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

    async fn latest_before(
        &self,
        market_id: &MarketId,
        before: DateTime<Utc>,
    ) -> Result<Option<ResolutionEventInfo>, StorageError> {
        latest_before_q(&self.db, market_id, before).await
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

    async fn latest_before(
        &self,
        market_id: &MarketId,
        before: DateTime<Utc>,
    ) -> Result<Option<ResolutionEventInfo>, StorageError> {
        latest_before_q(self.txn, market_id, before).await
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
    event
        .into_active_model()
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
        .filter(Column::MarketId.eq(market_id))
        .order_by_desc(Column::CreatedAt)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(Into::into))
}

async fn latest_before_q(
    db: &impl sea_orm::ConnectionTrait,
    market_id: &MarketId,
    before: DateTime<Utc>,
) -> Result<Option<ResolutionEventInfo>, StorageError> {
    Entity::find()
        .filter(Column::MarketId.eq(market_id))
        .filter(Column::ResolvedAt.lte(before))
        .order_by_desc(Column::ResolvedAt)
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
        .filter(Column::MarketId.eq(market_id))
        .filter(Column::Source.eq(source))
        .order_by_desc(Column::CreatedAt)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(Into::into))
}
