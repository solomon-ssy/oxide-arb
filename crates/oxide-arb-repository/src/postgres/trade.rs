use super::orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, FromQueryResult, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait,
};
use crate::{batch, traits::TradeRepository};
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{NewTrade, ReportTradeStats, TradeInfo, TradeObservation},
    entities::trade::{ActiveModel, Column, Entity},
    enums::common::{TradeBusinessOutcome, TradeState},
    types::{MarketId, TradeId, Usd},
};
use sea_orm::sea_query::{Condition, Expr, LockBehavior, LockType};
use std::collections::HashMap;

/// Number of columns in the `trade` table used for bind-variable calculations.
const TRADE_COLUMNS: usize = 28;

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

    pub async fn successful_spend_total(&self) -> Result<Usd, StorageError> {
        let trades = Entity::find()
            .filter(Column::BusinessOutcome.eq(TradeBusinessOutcome::Success.to_string()))
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(trades
            .iter()
            .map(|trade| trade.cost_usd + trade.fee_usd)
            .sum())
    }
}

pub struct PgTradeRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

#[derive(Debug, FromQueryResult)]
struct OutcomeCount {
    business_outcome: String,
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

async fn do_mark_submitted(
    db: &impl ConnectionTrait,
    trade_id: &TradeId,
    submitted_at: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let result = Entity::update_many()
        .col_expr(
            Column::State,
            Expr::value(TradeState::Submitted.to_string()),
        )
        .col_expr(Column::SubmittedAt, Expr::value(Some(submitted_at)))
        .filter(Column::TradeId.eq(trade_id.clone()))
        .filter(Column::State.eq(TradeState::Intent.to_string()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(result.rows_affected > 0)
}

async fn do_mark_observed(
    db: &impl ConnectionTrait,
    trade_id: &TradeId,
    obs: TradeObservation,
) -> Result<(), StorageError> {
    if !obs.state.is_unprocessed() {
        return Err(StorageError::StaleData(format!(
            "trade observation state {} is not claimable",
            obs.state
        )));
    }

    let result = Entity::update_many()
        .col_expr(Column::State, Expr::value(obs.state.to_string()))
        .col_expr(Column::Shares, Expr::value(obs.shares))
        .col_expr(Column::Price, Expr::value(obs.price))
        .col_expr(Column::CostUsd, Expr::value(obs.cost_usd))
        .col_expr(Column::FeeUsd, Expr::value(obs.fee_usd))
        .col_expr(Column::OrderId, Expr::value(obs.order_id))
        .col_expr(Column::TxHash, Expr::value(obs.tx_hash))
        .col_expr(Column::NetProfitUsd, Expr::value(obs.net_profit_usd))
        .col_expr(Column::LatencyMs, Expr::value(obs.latency_ms))
        .col_expr(Column::ErrorMessage, Expr::value(obs.error_message))
        .col_expr(Column::ConfirmedAt, Expr::value(Some(obs.confirmed_at)))
        .filter(Column::TradeId.eq(trade_id.clone()))
        .filter(Column::State.eq(TradeState::Submitted.to_string()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    if result.rows_affected == 0 {
        return Err(StorageError::StaleData(format!(
            "trade {trade_id} was not in submitted state"
        )));
    }
    Ok(())
}

async fn do_claim_unprocessed(
    txn: &DatabaseTransaction,
    limit: u64,
    owner: &str,
    claimed_at: DateTime<Utc>,
    lease_expired_before: DateTime<Utc>,
) -> Result<Vec<TradeInfo>, StorageError> {
    let claimable = Entity::find()
        .filter(claimable_condition(lease_expired_before))
        .order_by_asc(Column::CreatedAt)
        .limit(limit)
        .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
        .all(txn)
        .await
        .map_err(StorageError::from)?;

    if claimable.is_empty() {
        return Ok(Vec::new());
    }

    let mut ordered_ids = Vec::with_capacity(claimable.len());
    let mut fill_ids = Vec::new();
    let mut miss_ids = Vec::new();
    let mut fail_ids = Vec::new();
    for trade in claimable {
        ordered_ids.push(trade.trade_id.clone());
        match trade.state {
            TradeState::FillObserved | TradeState::FillProcessing => fill_ids.push(trade.trade_id),
            TradeState::MissObserved | TradeState::MissProcessing => miss_ids.push(trade.trade_id),
            TradeState::FailObserved | TradeState::FailProcessing => fail_ids.push(trade.trade_id),
            _ => {}
        }
    }

    let mut claimed_by_id = HashMap::with_capacity(ordered_ids.len());
    update_claimed_group(
        txn,
        fill_ids,
        TradeState::FillProcessing,
        owner,
        claimed_at,
        &mut claimed_by_id,
    )
    .await?;
    update_claimed_group(
        txn,
        miss_ids,
        TradeState::MissProcessing,
        owner,
        claimed_at,
        &mut claimed_by_id,
    )
    .await?;
    update_claimed_group(
        txn,
        fail_ids,
        TradeState::FailProcessing,
        owner,
        claimed_at,
        &mut claimed_by_id,
    )
    .await?;

    Ok(ordered_ids
        .into_iter()
        .filter_map(|trade_id| claimed_by_id.remove(&trade_id))
        .collect())
}

fn claimable_condition(lease_expired_before: DateTime<Utc>) -> Condition {
    Condition::any()
        .add(Column::State.is_in([
            TradeState::FillObserved.to_string(),
            TradeState::MissObserved.to_string(),
            TradeState::FailObserved.to_string(),
        ]))
        .add(
            Condition::all()
                .add(Column::State.is_in([
                    TradeState::FillProcessing.to_string(),
                    TradeState::MissProcessing.to_string(),
                    TradeState::FailProcessing.to_string(),
                ]))
                .add(Column::PostTradeClaimedAt.lt(lease_expired_before)),
        )
}

async fn update_claimed_group(
    txn: &DatabaseTransaction,
    trade_ids: Vec<TradeId>,
    processing_state: TradeState,
    owner: &str,
    claimed_at: DateTime<Utc>,
    claimed_by_id: &mut HashMap<TradeId, TradeInfo>,
) -> Result<(), StorageError> {
    if trade_ids.is_empty() {
        return Ok(());
    }

    let updated = Entity::update_many()
        .col_expr(Column::State, Expr::value(processing_state.to_string()))
        .col_expr(
            Column::PostTradeClaimOwner,
            Expr::value(Some(owner.to_owned())),
        )
        .col_expr(Column::PostTradeClaimedAt, Expr::value(Some(claimed_at)))
        .col_expr(
            Column::PostTradeAttempts,
            Expr::col(Column::PostTradeAttempts).add(1),
        )
        .col_expr(Column::UpdatedAt, Expr::value(claimed_at))
        .filter(Column::TradeId.is_in(trade_ids))
        .exec_with_returning(txn)
        .await
        .map_err(StorageError::from)?;

    for model in updated {
        claimed_by_id.insert(model.trade_id.clone(), model.into());
    }
    Ok(())
}

async fn do_advance_state(
    db: &impl ConnectionTrait,
    trade_id: &TradeId,
    from: TradeState,
    to: TradeState,
) -> Result<bool, StorageError> {
    let result = Entity::update_many()
        .col_expr(Column::State, Expr::value(to.to_string()))
        .col_expr(
            Column::PostTradeClaimOwner,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            Column::PostTradeClaimedAt,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .filter(Column::TradeId.eq(trade_id.clone()))
        .filter(Column::State.eq(from.to_string()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(result.rows_affected > 0)
}

async fn do_mark_orphaned(
    db: &impl ConnectionTrait,
    trade_id: &TradeId,
) -> Result<bool, StorageError> {
    let result = Entity::update_many()
        .col_expr(Column::State, Expr::value(TradeState::Orphaned.to_string()))
        .col_expr(Column::NeedsReconcile, Expr::value(true))
        .filter(Column::TradeId.eq(trade_id.clone()))
        .filter(Column::State.eq(TradeState::Submitted.to_string()))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(result.rows_affected > 0)
}

async fn do_find_stale_submitted(
    db: &impl ConnectionTrait,
    older_than: DateTime<Utc>,
    limit: u64,
) -> Result<Vec<TradeInfo>, StorageError> {
    Entity::find()
        .filter(Column::State.eq(TradeState::Submitted.to_string()))
        .filter(Column::SubmittedAt.lt(older_than))
        .order_by_asc(Column::SubmittedAt)
        .limit(limit)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
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
        .filter(Column::BusinessOutcome.is_not_null())
        .select_only()
        .column(Column::BusinessOutcome)
        .column_as(Column::TradeId.count(), "count")
        .group_by(Column::BusinessOutcome)
        .into_model::<OutcomeCount>()
        .all(db)
        .await
        .map_err(StorageError::from)?;

    Ok(results
        .into_iter()
        .map(|r| (r.business_outcome, r.count))
        .collect())
}

async fn do_aggregate_between(
    db: &impl ConnectionTrait,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<ReportTradeStats, StorageError> {
    let trades: Vec<TradeInfo> = Entity::find()
        .filter(Column::CreatedAt.gte(start))
        .filter(Column::CreatedAt.lt(end))
        .all(db)
        .await
        .map_err(StorageError::from)?
        .into_iter()
        .map(Into::into)
        .collect();

    let mut stats = ReportTradeStats {
        trade_count: 0,
        success_count: 0,
        miss_count: 0,
        failed_count: 0,
        total_fill_cost: Usd::ZERO,
        total_fill_fees: Usd::ZERO,
        fill_expected_pnl: Usd::ZERO,
    };

    for trade in trades {
        stats.trade_count = stats.trade_count.saturating_add(1);
        stats.total_fill_cost += trade.cost_usd;
        stats.total_fill_fees += trade.fee_usd;
        if let Some(pnl) = trade.net_profit_usd {
            stats.fill_expected_pnl += pnl;
        }
        match trade.business_outcome {
            Some(TradeBusinessOutcome::Success) => {
                stats.success_count = stats.success_count.saturating_add(1);
            }
            Some(TradeBusinessOutcome::Miss) => {
                stats.miss_count = stats.miss_count.saturating_add(1);
            }
            Some(TradeBusinessOutcome::Failed) => {
                stats.failed_count = stats.failed_count.saturating_add(1);
            }
            None => {}
        }
    }

    Ok(stats)
}

#[async_trait::async_trait]
impl TradeRepository for PgTradeRepository {
    async fn create(&self, trade: NewTrade) -> Result<TradeInfo, StorageError> {
        do_create(&self.db, trade).await
    }

    async fn create_batch(&self, trades: Vec<NewTrade>) -> Result<u64, StorageError> {
        do_create_batch(&self.db, trades).await
    }

    async fn mark_submitted(
        &self,
        trade_id: &TradeId,
        submitted_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        do_mark_submitted(&self.db, trade_id, submitted_at).await
    }

    async fn mark_observed(
        &self,
        trade_id: &TradeId,
        observation: TradeObservation,
    ) -> Result<(), StorageError> {
        do_mark_observed(&self.db, trade_id, observation).await
    }

    async fn claim_unprocessed(
        &self,
        limit: u64,
        owner: &str,
        claimed_at: DateTime<Utc>,
        lease_expired_before: DateTime<Utc>,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let claimed =
            do_claim_unprocessed(&txn, limit, owner, claimed_at, lease_expired_before).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(claimed)
    }

    async fn advance_state(
        &self,
        trade_id: &TradeId,
        from: TradeState,
        to: TradeState,
    ) -> Result<bool, StorageError> {
        do_advance_state(&self.db, trade_id, from, to).await
    }

    async fn mark_orphaned(&self, trade_id: &TradeId) -> Result<bool, StorageError> {
        do_mark_orphaned(&self.db, trade_id).await
    }

    async fn find_stale_submitted(
        &self,
        older_than: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        do_find_stale_submitted(&self.db, older_than, limit).await
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

    async fn aggregate_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<ReportTradeStats, StorageError> {
        do_aggregate_between(&self.db, start, end).await
    }
}

#[async_trait::async_trait]
impl TradeRepository for PgTradeRepositoryTxn<'_> {
    async fn create(&self, trade: NewTrade) -> Result<TradeInfo, StorageError> {
        do_create(self.txn, trade).await
    }

    async fn create_batch(&self, trades: Vec<NewTrade>) -> Result<u64, StorageError> {
        do_create_batch(self.txn, trades).await
    }

    async fn mark_submitted(
        &self,
        trade_id: &TradeId,
        submitted_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        do_mark_submitted(self.txn, trade_id, submitted_at).await
    }

    async fn mark_observed(
        &self,
        trade_id: &TradeId,
        observation: TradeObservation,
    ) -> Result<(), StorageError> {
        do_mark_observed(self.txn, trade_id, observation).await
    }

    async fn claim_unprocessed(
        &self,
        limit: u64,
        owner: &str,
        claimed_at: DateTime<Utc>,
        lease_expired_before: DateTime<Utc>,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        do_claim_unprocessed(self.txn, limit, owner, claimed_at, lease_expired_before).await
    }

    async fn advance_state(
        &self,
        trade_id: &TradeId,
        from: TradeState,
        to: TradeState,
    ) -> Result<bool, StorageError> {
        do_advance_state(self.txn, trade_id, from, to).await
    }

    async fn mark_orphaned(&self, trade_id: &TradeId) -> Result<bool, StorageError> {
        do_mark_orphaned(self.txn, trade_id).await
    }

    async fn find_stale_submitted(
        &self,
        older_than: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        do_find_stale_submitted(self.txn, older_than, limit).await
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

    async fn aggregate_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<ReportTradeStats, StorageError> {
        do_aggregate_between(self.txn, start, end).await
    }
}
