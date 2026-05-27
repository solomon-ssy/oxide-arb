use super::orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, FromQueryResult, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, Set,
};
use crate::{batch, traits::TradeRepository};
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{NewTrade, TradeInfo, UpdateTradeOutcome},
    entities::trade::{ActiveModel, Column, Entity},
    types::{MarketId, TradeId},
};
use std::collections::HashMap;

/// Number of columns in the `trade` table used for bind-variable calculations.
const TRADE_COLUMNS: usize = 22;

pub struct PgTradeRepository {
    db: DatabaseConnection,
}

impl PgTradeRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgTradeRepositoryTxn<'_> {
        PgTradeRepositoryTxn { txn }
    }
}

pub struct PgTradeRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

#[derive(Debug, FromQueryResult)]
struct OutcomeCount {
    outcome: String,
    count: i64,
}

async fn do_create(db: &impl ConnectionTrait, new: NewTrade) -> Result<TradeInfo, StorageError> {
    let model = new
        .into_active_model()
        .insert(db)
        .await
        .map_err(StorageError::from)?;
    Ok(model.into())
}

async fn do_update(
    db: &impl ConnectionTrait,
    trade_id: &TradeId,
    update: UpdateTradeOutcome,
) -> Result<TradeInfo, StorageError> {
    let existing = Entity::find_by_id(trade_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "trade",
            id: trade_id.to_string(),
        })?;

    let mut active: ActiveModel = existing.into();
    active.outcome = Set(update.outcome);
    active.order_id = Set(update.order_id);
    active.tx_hash = Set(update.tx_hash);
    active.net_profit_usd = Set(update.net_profit_usd);
    active.latency_ms = Set(update.latency_ms);
    active.error_message = Set(update.error_message);
    active.confirmed_at = Set(update.confirmed_at);

    let model = active.update(db).await.map_err(StorageError::from)?;
    Ok(model.into())
}

async fn do_create_batch(
    db: &impl ConnectionTrait,
    trades: Vec<NewTrade>,
) -> Result<u64, StorageError> {
    if trades.is_empty() {
        return Ok(0);
    }

    let mut total = 0u64;
    for chunk in batch::chunk_for_insert(&trades, TRADE_COLUMNS) {
        let chunk_len = ToPrimitive::to_u64(&chunk.len()).unwrap_or(u64::MAX);
        let models: Vec<ActiveModel> = chunk
            .iter()
            .cloned()
            .map(|new| ActiveModel::prepare_for_insert(new.into_active_model()))
            .collect();
        Entity::insert_many(models)
            .exec(db)
            .await
            .map_err(StorageError::from)?;
        total += chunk_len;
    }

    Ok(total)
}

async fn do_find_by_id(
    db: &impl ConnectionTrait,
    trade_id: &TradeId,
) -> Result<Option<TradeInfo>, StorageError> {
    Entity::find_by_id(trade_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|opt| opt.map(Into::into))
}

async fn do_find_by_execution(
    db: &impl ConnectionTrait,
    execution_id: &str,
) -> Result<Vec<TradeInfo>, StorageError> {
    Entity::find()
        .filter(Column::ExecutionId.eq(execution_id))
        .order_by_desc(Column::CreatedAt)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_find_by_market(
    db: &impl ConnectionTrait,
    market_id: &MarketId,
    limit: u64,
) -> Result<Vec<TradeInfo>, StorageError> {
    Entity::find()
        .filter(Column::MarketId.eq(market_id.as_str()))
        .order_by_desc(Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_find_recent(
    db: &impl ConnectionTrait,
    since: DateTime<Utc>,
    limit: u64,
) -> Result<Vec<TradeInfo>, StorageError> {
    Entity::find()
        .filter(Column::CreatedAt.gte(since))
        .order_by_desc(Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_count_by_outcome(
    db: &impl ConnectionTrait,
    since: DateTime<Utc>,
) -> Result<HashMap<String, i64>, StorageError> {
    let results: Vec<OutcomeCount> = Entity::find()
        .filter(Column::CreatedAt.gte(since))
        .select_only()
        .column(Column::Outcome)
        .column_as(Column::TradeId.count(), "count")
        .group_by(Column::Outcome)
        .into_model::<OutcomeCount>()
        .all(db)
        .await
        .map_err(StorageError::from)?;

    Ok(results.into_iter().map(|r| (r.outcome, r.count)).collect())
}

impl TradeRepository for PgTradeRepository {
    async fn create(&self, trade: NewTrade) -> Result<TradeInfo, StorageError> {
        do_create(&self.db, trade).await
    }

    async fn create_batch(&self, trades: Vec<NewTrade>) -> Result<u64, StorageError> {
        do_create_batch(&self.db, trades).await
    }

    async fn update(
        &self,
        trade_id: &TradeId,
        update: UpdateTradeOutcome,
    ) -> Result<TradeInfo, StorageError> {
        do_update(&self.db, trade_id, update).await
    }

    async fn find_by_id(&self, trade_id: &TradeId) -> Result<Option<TradeInfo>, StorageError> {
        do_find_by_id(&self.db, trade_id).await
    }

    async fn find_by_execution(&self, execution_id: &str) -> Result<Vec<TradeInfo>, StorageError> {
        do_find_by_execution(&self.db, execution_id).await
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
        limit: u64,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        do_find_by_market(&self.db, market_id, limit).await
    }

    async fn find_recent(
        &self,
        since: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        do_find_recent(&self.db, since, limit).await
    }

    async fn count_by_outcome(
        &self,
        since: DateTime<Utc>,
    ) -> Result<HashMap<String, i64>, StorageError> {
        do_count_by_outcome(&self.db, since).await
    }
}

impl TradeRepository for PgTradeRepositoryTxn<'_> {
    async fn create(&self, trade: NewTrade) -> Result<TradeInfo, StorageError> {
        do_create(self.txn, trade).await
    }

    async fn create_batch(&self, trades: Vec<NewTrade>) -> Result<u64, StorageError> {
        do_create_batch(self.txn, trades).await
    }

    async fn update(
        &self,
        trade_id: &TradeId,
        update: UpdateTradeOutcome,
    ) -> Result<TradeInfo, StorageError> {
        do_update(self.txn, trade_id, update).await
    }

    async fn find_by_id(&self, trade_id: &TradeId) -> Result<Option<TradeInfo>, StorageError> {
        do_find_by_id(self.txn, trade_id).await
    }

    async fn find_by_execution(&self, execution_id: &str) -> Result<Vec<TradeInfo>, StorageError> {
        do_find_by_execution(self.txn, execution_id).await
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
        limit: u64,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        do_find_by_market(self.txn, market_id, limit).await
    }

    async fn find_recent(
        &self,
        since: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        do_find_recent(self.txn, since, limit).await
    }

    async fn count_by_outcome(
        &self,
        since: DateTime<Utc>,
    ) -> Result<HashMap<String, i64>, StorageError> {
        do_count_by_outcome(self.txn, since).await
    }
}
