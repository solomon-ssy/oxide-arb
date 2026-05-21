use crate::traits::MarketRepository;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::market::{self, ActiveModel, Column, Entity};
use oxide_arb_models::enums::market::MarketStatus;
use oxide_arb_models::types::MarketId;
use sea_orm::sea_query::Expr;
#[allow(clippy::wildcard_imports)]
use sea_orm::*;
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
) -> Result<Option<market::Model>, StorageError> {
    Entity::find_by_id(id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)
}

async fn do_find_active(db: &impl ConnectionTrait) -> Result<Vec<market::Model>, StorageError> {
    Entity::find()
        .filter(Column::Status.eq(MarketStatus::Active))
        .all(db)
        .await
        .map_err(StorageError::from)
}

async fn do_find_by_event(
    db: &impl ConnectionTrait,
    event_id: &str,
) -> Result<Vec<market::Model>, StorageError> {
    Entity::find()
        .filter(Column::EventId.eq(event_id))
        .all(db)
        .await
        .map_err(StorageError::from)
}

async fn do_find_endgame_candidates(
    db: &impl ConnectionTrait,
    before_deadline: DateTime<Utc>,
) -> Result<Vec<market::Model>, StorageError> {
    Entity::find()
        .filter(Column::Status.eq(MarketStatus::Active))
        .filter(Column::EndDate.is_not_null())
        .filter(Column::EndDate.lte(before_deadline))
        .all(db)
        .await
        .map_err(StorageError::from)
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

async fn do_insert(
    db: &impl ConnectionTrait,
    model: ActiveModel,
) -> Result<market::Model, StorageError> {
    Entity::insert(model)
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)
}

async fn do_insert_batch(
    db: &impl ConnectionTrait,
    models: Vec<ActiveModel>,
) -> Result<u64, StorageError> {
    if models.is_empty() {
        return Ok(0);
    }
    let count = models.len() as u64;
    Entity::insert_many(models)
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(count)
}

async fn do_update(
    db: &impl ConnectionTrait,
    model: ActiveModel,
) -> Result<market::Model, StorageError> {
    model.update(db).await.map_err(StorageError::from)
}

async fn do_update_status(
    db: &impl ConnectionTrait,
    id: &MarketId,
    status: &str,
    outcome: Option<&str>,
) -> Result<(), StorageError> {
    let mut stmt = Entity::update_many()
        .col_expr(Column::Status, Expr::value(status))
        .col_expr(Column::UpdatedAt, Expr::value(Utc::now()));

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

impl MarketRepository for PgMarketRepository {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<market::Model>, StorageError> {
        do_find_by_id(&self.db, id).await
    }

    async fn find_active(&self) -> Result<Vec<market::Model>, StorageError> {
        do_find_active(&self.db).await
    }

    async fn find_by_event(&self, event_id: &str) -> Result<Vec<market::Model>, StorageError> {
        do_find_by_event(&self.db, event_id).await
    }

    async fn find_endgame_candidates(
        &self,
        before_deadline: DateTime<Utc>,
    ) -> Result<Vec<market::Model>, StorageError> {
        do_find_endgame_candidates(&self.db, before_deadline).await
    }

    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        do_find_existing_ids(&self.db, ids).await
    }

    async fn insert(&self, model: ActiveModel) -> Result<market::Model, StorageError> {
        do_insert(&self.db, model).await
    }

    async fn insert_batch(&self, models: Vec<ActiveModel>) -> Result<u64, StorageError> {
        do_insert_batch(&self.db, models).await
    }

    async fn update(&self, model: ActiveModel) -> Result<market::Model, StorageError> {
        do_update(&self.db, model).await
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

impl MarketRepository for PgMarketRepositoryTxn<'_> {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<market::Model>, StorageError> {
        do_find_by_id(self.txn, id).await
    }

    async fn find_active(&self) -> Result<Vec<market::Model>, StorageError> {
        do_find_active(self.txn).await
    }

    async fn find_by_event(&self, event_id: &str) -> Result<Vec<market::Model>, StorageError> {
        do_find_by_event(self.txn, event_id).await
    }

    async fn find_endgame_candidates(
        &self,
        before_deadline: DateTime<Utc>,
    ) -> Result<Vec<market::Model>, StorageError> {
        do_find_endgame_candidates(self.txn, before_deadline).await
    }

    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        do_find_existing_ids(self.txn, ids).await
    }

    async fn insert(&self, model: ActiveModel) -> Result<market::Model, StorageError> {
        do_insert(self.txn, model).await
    }

    async fn insert_batch(&self, models: Vec<ActiveModel>) -> Result<u64, StorageError> {
        do_insert_batch(self.txn, models).await
    }

    async fn update(&self, model: ActiveModel) -> Result<market::Model, StorageError> {
        do_update(self.txn, model).await
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
