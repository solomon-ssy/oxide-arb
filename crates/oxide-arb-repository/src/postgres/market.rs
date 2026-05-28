use super::orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, QueryFilter, QuerySelect,
};
use crate::traits::MarketRepository;
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{MarketInfo, UpsertMarket},
    entities::market::{ActiveModel, Column, Entity},
    enums::market::MarketStatus,
    types::MarketId,
};
use sea_orm::sea_query::{Expr, OnConflict};
use std::collections::HashSet;

pub struct PgMarketRepository {
    db: DatabaseConnection,
}

impl PgMarketRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgMarketRepositoryTxn<'_> {
        PgMarketRepositoryTxn { txn }
    }
}

pub struct PgMarketRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

async fn do_find_by_id(
    db: &impl ConnectionTrait,
    id: &MarketId,
) -> Result<Option<MarketInfo>, StorageError> {
    Entity::find_by_id(id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|opt| opt.map(Into::into))
}

async fn do_find_active(db: &impl ConnectionTrait) -> Result<Vec<MarketInfo>, StorageError> {
    Entity::find()
        .filter(Column::Status.eq(MarketStatus::Active))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_find_by_event(
    db: &impl ConnectionTrait,
    event_id: &str,
) -> Result<Vec<MarketInfo>, StorageError> {
    Entity::find()
        .filter(Column::EventId.eq(event_id))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_find_endgame_candidates(
    db: &impl ConnectionTrait,
    before_deadline: DateTime<Utc>,
) -> Result<Vec<MarketInfo>, StorageError> {
    Entity::find()
        .filter(Column::Status.eq(MarketStatus::Active))
        .filter(Column::EndDate.is_not_null())
        .filter(Column::EndDate.lte(before_deadline))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_find_existing_ids(
    db: &impl ConnectionTrait,
    ids: &[MarketId],
) -> Result<HashSet<String>, StorageError> {
    if ids.is_empty() {
        return Ok(HashSet::new());
    }
    let id_strs: Vec<&str> = ids.iter().map(MarketId::as_str).collect();
    let rows = Entity::find()
        .filter(Column::MarketId.is_in(id_strs))
        .select_only()
        .column(Column::MarketId)
        .into_tuple::<String>()
        .all(db)
        .await?;
    Ok(rows.into_iter().collect())
}

async fn do_upsert(
    db: &impl ConnectionTrait,
    dto: UpsertMarket,
) -> Result<MarketInfo, StorageError> {
    let am: ActiveModel = dto.into_active_model();
    let model = Entity::insert(am.prepare_for_insert())
        .on_conflict(
            OnConflict::column(Column::MarketId)
                .update_columns([
                    Column::EventId,
                    Column::Question,
                    Column::Slug,
                    Column::Category,
                    Column::Status,
                    Column::YesTokenId,
                    Column::NoTokenId,
                    Column::TickSize,
                    Column::NegRisk,
                    Column::UpdatedAt,
                ])
                .to_owned(),
        )
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;
    Ok(model.into())
}

async fn do_upsert_batch(
    db: &impl ConnectionTrait,
    dtos: Vec<UpsertMarket>,
) -> Result<u64, StorageError> {
    if dtos.is_empty() {
        return Ok(0);
    }
    let count = ToPrimitive::to_u64(&dtos.len()).unwrap_or(u64::MAX);
    let models: Vec<ActiveModel> = dtos
        .into_iter()
        .map(|dto| ActiveModel::prepare_for_insert(dto.into_active_model()))
        .collect();
    Entity::insert_many(models)
        .on_conflict(
            OnConflict::column(Column::MarketId)
                .update_columns([
                    Column::EventId,
                    Column::Question,
                    Column::Slug,
                    Column::Category,
                    Column::Status,
                    Column::YesTokenId,
                    Column::NoTokenId,
                    Column::TickSize,
                    Column::NegRisk,
                    Column::UpdatedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(count)
}

async fn do_update_status(
    db: &impl ConnectionTrait,
    id: &MarketId,
    status: &str,
    outcome: Option<&str>,
) -> Result<(), StorageError> {
    let mut stmt = Entity::update_many().col_expr(Column::Status, Expr::value(status));

    if let Some(o) = outcome {
        stmt = stmt.col_expr(Column::Outcome, Expr::value(Some(o.to_string())));
    }

    let result = stmt
        .filter(Column::MarketId.eq(id.as_str()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;

    if result.rows_affected == 0 {
        return Err(StorageError::NotFound {
            entity: "market",
            id: id.to_string(),
        });
    }
    Ok(())
}

#[async_trait::async_trait]
impl MarketRepository for PgMarketRepository {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<MarketInfo>, StorageError> {
        do_find_by_id(&self.db, id).await
    }

    async fn find_active(&self) -> Result<Vec<MarketInfo>, StorageError> {
        do_find_active(&self.db).await
    }

    async fn find_by_event(&self, event_id: &str) -> Result<Vec<MarketInfo>, StorageError> {
        do_find_by_event(&self.db, event_id).await
    }

    async fn find_endgame_candidates(
        &self,
        before_deadline: DateTime<Utc>,
    ) -> Result<Vec<MarketInfo>, StorageError> {
        do_find_endgame_candidates(&self.db, before_deadline).await
    }

    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        do_find_existing_ids(&self.db, ids).await
    }

    async fn upsert(&self, dto: UpsertMarket) -> Result<MarketInfo, StorageError> {
        do_upsert(&self.db, dto).await
    }

    async fn upsert_batch(&self, dtos: Vec<UpsertMarket>) -> Result<u64, StorageError> {
        do_upsert_batch(&self.db, dtos).await
    }

    async fn update_status(
        &self,
        id: &MarketId,
        status: &str,
        outcome: Option<&str>,
    ) -> Result<(), StorageError> {
        do_update_status(&self.db, id, status, outcome).await
    }
}

#[async_trait::async_trait]
impl MarketRepository for PgMarketRepositoryTxn<'_> {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<MarketInfo>, StorageError> {
        do_find_by_id(self.txn, id).await
    }

    async fn find_active(&self) -> Result<Vec<MarketInfo>, StorageError> {
        do_find_active(self.txn).await
    }

    async fn find_by_event(&self, event_id: &str) -> Result<Vec<MarketInfo>, StorageError> {
        do_find_by_event(self.txn, event_id).await
    }

    async fn find_endgame_candidates(
        &self,
        before_deadline: DateTime<Utc>,
    ) -> Result<Vec<MarketInfo>, StorageError> {
        do_find_endgame_candidates(self.txn, before_deadline).await
    }

    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        do_find_existing_ids(self.txn, ids).await
    }

    async fn upsert(&self, dto: UpsertMarket) -> Result<MarketInfo, StorageError> {
        do_upsert(self.txn, dto).await
    }

    async fn upsert_batch(&self, dtos: Vec<UpsertMarket>) -> Result<u64, StorageError> {
        do_upsert_batch(self.txn, dtos).await
    }

    async fn update_status(
        &self,
        id: &MarketId,
        status: &str,
        outcome: Option<&str>,
    ) -> Result<(), StorageError> {
        do_update_status(self.txn, id, status, outcome).await
    }
}
