use crate::{batch, traits::TradeRepository};
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        EdgeBucket, MarketPerformanceRow, NewTrade, PageRequest, Paginated, ReportTradeStats,
        TradeAnalyticsFilter, TradeInfo, TradeObservation, TradePageQuery,
    },
    entities::trade::{ActiveModel, Column, Entity},
    enums::common::{ExecutionMode, TradeBusinessOutcome, TradeReconcileResolution, TradeState},
    types::{ExecutionId, MarketId, TradeId, Usd},
};
use rust_decimal::Decimal;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, FromQueryResult, IntoActiveModel, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, TransactionTrait,
    sea_query::{Condition, Expr, Func, LockBehavior, LockType, NullOrdering, Order, SimpleExpr},
};
use std::collections::HashMap;

/// Number of `NewTrade` columns used for bind-variable calculations.
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

    /// Total capital spent (cost + fees) on successful trades for one
    /// execution mode.
    ///
    /// Mode-scoped so simulated (dry-run/paper) fills never leak into the
    /// Live internal ledger and vice versa.
    pub async fn successful_spend_total(&self, mode: ExecutionMode) -> Result<Usd, StorageError> {
        let row = Entity::find()
            .select_only()
            .column_as(
                Expr::expr(Expr::col(Column::CostUsd).add(Expr::col(Column::FeeUsd))).sum(),
                "total",
            )
            .filter(Column::BusinessOutcome.eq(TradeBusinessOutcome::Success))
            .filter(Column::ExecutionMode.eq(mode))
            .into_model::<UsdTotal>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(row.and_then(|r| r.total).unwrap_or(Usd::ZERO))
    }
}

/// SQL-side scalar projection for `SUM(...)` ledger aggregates.
#[derive(Debug, FromQueryResult)]
struct UsdTotal {
    total: Option<Usd>,
}

pub struct PgTradeRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

#[derive(Debug, FromQueryResult)]
struct OutcomeCount {
    business_outcome: TradeBusinessOutcome,
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
    let chunk_size = batch::max_rows_per_insert(TRADE_COLUMNS);
    let mut chunk = Vec::with_capacity(chunk_size);
    for trade in trades {
        chunk.push(trade);
        if chunk.len() < chunk_size {
            continue;
        }
        let chunk_len = ToPrimitive::to_u64(&chunk.len()).unwrap_or(u64::MAX);
        let models = std::mem::take(&mut chunk)
            .into_iter()
            .map(IntoActiveModel::into_active_model)
            .collect::<Vec<ActiveModel>>();
        Entity::insert_many(models)
            .exec(db)
            .await
            .map_err(StorageError::from)?;
        total += chunk_len;
    }
    if !chunk.is_empty() {
        let chunk_len = ToPrimitive::to_u64(&chunk.len()).unwrap_or(u64::MAX);
        let models = chunk
            .into_iter()
            .map(IntoActiveModel::into_active_model)
            .collect::<Vec<ActiveModel>>();
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
        .col_expr(Column::State, Expr::value(TradeState::Submitted))
        .col_expr(Column::SubmittedAt, Expr::value(Some(submitted_at)))
        .filter(Column::TradeId.eq(trade_id.clone()))
        .filter(Column::State.eq(TradeState::Intent))
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
        .col_expr(Column::State, Expr::value(obs.state))
        .col_expr(
            Column::BusinessOutcome,
            Expr::value(obs.state.business_outcome()),
        )
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
        .filter(Column::State.eq(TradeState::Submitted))
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

async fn do_mark_reconciled_observed(
    db: &impl ConnectionTrait,
    trade_id: &TradeId,
    obs: TradeObservation,
    resolution: TradeReconcileResolution,
    note: &str,
) -> Result<bool, StorageError> {
    if !obs.state.is_unprocessed() {
        return Err(StorageError::StaleData(format!(
            "reconciled observation state {} is not claimable",
            obs.state
        )));
    }

    let reconciled_at = Utc::now();
    let result = Entity::update_many()
        .col_expr(Column::State, Expr::value(obs.state))
        .col_expr(
            Column::BusinessOutcome,
            Expr::value(obs.state.business_outcome()),
        )
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
        .col_expr(Column::NeedsReconcile, Expr::value(false))
        .col_expr(Column::ReconcileResolution, Expr::value(Some(resolution)))
        .col_expr(Column::ReconciledAt, Expr::value(Some(reconciled_at)))
        .col_expr(Column::ReconcileNote, Expr::value(Some(note.to_owned())))
        .filter(Column::TradeId.eq(trade_id.clone()))
        .filter(Column::State.eq(TradeState::Orphaned))
        .filter(Column::NeedsReconcile.eq(true))
        .filter(Column::ReconcileResolution.is_null())
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(result.rows_affected > 0)
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
            TradeState::FillObserved,
            TradeState::MissObserved,
            TradeState::FailObserved,
        ]))
        .add(
            Condition::all()
                .add(Column::State.is_in([
                    TradeState::FillProcessing,
                    TradeState::MissProcessing,
                    TradeState::FailProcessing,
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
        .col_expr(Column::State, Expr::value(processing_state))
        .col_expr(
            Column::PostTradeClaimOwner,
            Expr::value(Some(owner.to_owned())),
        )
        .col_expr(Column::PostTradeClaimedAt, Expr::value(Some(claimed_at)))
        .col_expr(
            Column::PostTradeAttempts,
            Expr::col(Column::PostTradeAttempts).add(1),
        )
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
        .col_expr(Column::State, Expr::value(to))
        .col_expr(Column::BusinessOutcome, Expr::value(to.business_outcome()))
        .col_expr(
            Column::PostTradeClaimOwner,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            Column::PostTradeClaimedAt,
            Expr::value(Option::<DateTime<Utc>>::None),
        )
        .filter(Column::TradeId.eq(trade_id.clone()))
        .filter(Column::State.eq(from))
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
        .col_expr(Column::State, Expr::value(TradeState::Orphaned))
        .col_expr(
            Column::BusinessOutcome,
            Expr::value(Option::<TradeBusinessOutcome>::None),
        )
        .col_expr(Column::NeedsReconcile, Expr::value(true))
        .filter(Column::TradeId.eq(trade_id.clone()))
        .filter(Column::State.eq(TradeState::Submitted))
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
        .filter(Column::State.eq(TradeState::Submitted))
        .filter(Column::SubmittedAt.lt(older_than))
        .order_by_asc(Column::SubmittedAt)
        .limit(limit)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_find_needs_reconcile(
    db: &impl ConnectionTrait,
    limit: u64,
) -> Result<Vec<TradeInfo>, StorageError> {
    Entity::find()
        .filter(Column::NeedsReconcile.eq(true))
        .filter(Column::ReconcileResolution.is_null())
        .order_by_asc(Column::CreatedAt)
        .order_by_asc(Column::TradeId)
        .limit(limit)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_mark_reconciled(
    db: &impl ConnectionTrait,
    trade_id: &TradeId,
    resolution: TradeReconcileResolution,
    note: &str,
    reconciled_at: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let result = Entity::update_many()
        .col_expr(Column::ReconcileResolution, Expr::value(Some(resolution)))
        .col_expr(Column::ReconciledAt, Expr::value(Some(reconciled_at)))
        .col_expr(Column::ReconcileNote, Expr::value(Some(note.to_owned())))
        .filter(Column::TradeId.eq(trade_id.clone()))
        .filter(Column::NeedsReconcile.eq(true))
        .filter(Column::ReconcileResolution.is_null())
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(result.rows_affected > 0)
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
    execution_id: &ExecutionId,
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
        .filter(Column::MarketId.eq(market_id))
        .order_by_desc(Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

fn page_condition(query: &TradePageQuery) -> Condition {
    let mut condition = Condition::all();
    if let Some(market_id) = &query.market_id {
        condition = condition.add(Column::MarketId.eq(market_id));
    }
    if let Some(side) = query.side {
        condition = condition.add(Column::Side.eq(side));
    }
    if let Some(state) = query.state {
        condition = condition.add(Column::State.eq(state));
    }
    if let Some(outcome) = query.business_outcome {
        condition = condition.add(Column::BusinessOutcome.eq(outcome));
    }
    if let Some(mode) = query.execution_mode {
        condition = condition.add(Column::ExecutionMode.eq(mode));
    }
    if let Some(from) = query.from {
        condition = condition.add(Column::CreatedAt.gte(from));
    }
    if let Some(to) = query.to {
        condition = condition.add(Column::CreatedAt.lt(to));
    }
    condition
}

async fn do_page(
    db: &impl ConnectionTrait,
    query: TradePageQuery,
) -> Result<Paginated<TradeInfo>, StorageError> {
    let window = query.page.normalized();
    let condition = page_condition(&query);
    let total = Entity::find()
        .filter(condition.clone())
        .count(db)
        .await
        .map_err(StorageError::from)?;
    if total == 0 {
        return Ok(Paginated::from_request(Vec::new(), total, &window));
    }
    let models = Entity::find()
        .filter(condition)
        .order_by_desc(Column::CreatedAt)
        .order_by_desc(Column::TradeId)
        .offset(window.offset())
        .limit(window.limit())
        .all(db)
        .await
        .map_err(StorageError::from)?;
    let items = models.into_iter().map(Into::into).collect();
    Ok(Paginated::from_request(items, total, &window))
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

async fn do_find_between(
    db: &impl ConnectionTrait,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<TradeInfo>, StorageError> {
    Entity::find()
        .filter(Column::CreatedAt.gte(start))
        .filter(Column::CreatedAt.lt(end))
        .order_by_asc(Column::CreatedAt)
        .order_by_asc(Column::TradeId)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_count_by_outcome(
    db: &impl ConnectionTrait,
    since: DateTime<Utc>,
) -> Result<HashMap<TradeBusinessOutcome, i64>, StorageError> {
    let results: Vec<OutcomeCount> = Entity::find()
        .filter(Column::CreatedAt.gte(since))
        .filter(Column::NeedsReconcile.eq(false))
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

/// SQL-side scalar projection for windowed trade rollups.
///
/// Outcome counts use `SUM(CASE …)`; `PostgreSQL` returns `NULL` (not `0`) when
/// the filtered set is empty, so those fields are optional at decode time.
#[derive(Debug, FromQueryResult)]
struct AggregatedReportStats {
    trade_count: i64,
    success_count: Option<i64>,
    miss_count: Option<i64>,
    failed_count: Option<i64>,
    total_fill_cost: Option<Usd>,
    total_fill_fees: Option<Usd>,
    fill_expected_pnl: Option<Usd>,
}

async fn do_aggregate_between(
    db: &impl ConnectionTrait,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<ReportTradeStats, StorageError> {
    let outcome_count = |outcome: TradeBusinessOutcome| {
        Func::sum(Expr::case(Column::BusinessOutcome.eq(outcome), 1).finally(0))
    };

    let row = Entity::find()
        .filter(Column::CreatedAt.gte(start))
        .filter(Column::CreatedAt.lt(end))
        .filter(Column::NeedsReconcile.eq(false))
        .select_only()
        .column_as(Column::TradeId.count(), "trade_count")
        .expr_as(
            outcome_count(TradeBusinessOutcome::Success),
            "success_count",
        )
        .expr_as(outcome_count(TradeBusinessOutcome::Miss), "miss_count")
        .expr_as(outcome_count(TradeBusinessOutcome::Failed), "failed_count")
        .column_as(Column::CostUsd.sum(), "total_fill_cost")
        .column_as(Column::FeeUsd.sum(), "total_fill_fees")
        .column_as(Column::NetProfitUsd.sum(), "fill_expected_pnl")
        .into_model::<AggregatedReportStats>()
        .one(db)
        .await
        .map_err(StorageError::from)?;

    Ok(row.map_or(
        ReportTradeStats {
            trade_count: 0,
            success_count: 0,
            miss_count: 0,
            failed_count: 0,
            total_fill_cost: Usd::ZERO,
            total_fill_fees: Usd::ZERO,
            fill_expected_pnl: Usd::ZERO,
        },
        |stats| ReportTradeStats {
            trade_count: u32::try_from(stats.trade_count.max(0)).unwrap_or(0),
            success_count: u32::try_from(stats.success_count.unwrap_or(0).max(0)).unwrap_or(0),
            miss_count: u32::try_from(stats.miss_count.unwrap_or(0).max(0)).unwrap_or(0),
            failed_count: u32::try_from(stats.failed_count.unwrap_or(0).max(0)).unwrap_or(0),
            total_fill_cost: stats.total_fill_cost.unwrap_or(Usd::ZERO),
            total_fill_fees: stats.total_fill_fees.unwrap_or(Usd::ZERO),
            fill_expected_pnl: stats.fill_expected_pnl.unwrap_or(Usd::ZERO),
        },
    ))
}

/// Detected-edge histogram bucket bounds in basis points (right-open `[lo, hi)`).
/// `None` lower bound = unbounded below; `None` upper bound = unbounded above.
const EDGE_BUCKET_BOUNDS: [(&str, Option<i64>, Option<i64>); 6] = [
    ("<0", None, Some(0)),
    ("0-50", Some(0), Some(50)),
    ("50-100", Some(50), Some(100)),
    ("100-200", Some(100), Some(200)),
    ("200-500", Some(200), Some(500)),
    ("500+", Some(500), None),
];

/// Shared `trade` row predicate for analytics aggregations.
fn analytics_trade_condition(filter: TradeAnalyticsFilter) -> Condition {
    let mut condition = Condition::all()
        .add(Column::CreatedAt.gte(filter.window.from))
        .add(Column::CreatedAt.lt(filter.window.to));
    if let Some(mode) = filter.execution_mode {
        condition = condition.add(Column::ExecutionMode.eq(mode));
    }
    condition
}

async fn do_edge_histogram(
    db: &impl ConnectionTrait,
    filter: TradeAnalyticsFilter,
) -> Result<Vec<EdgeBucket>, StorageError> {
    let base = analytics_trade_condition(filter);
    let mut buckets = Vec::with_capacity(EDGE_BUCKET_BOUNDS.len());
    for (label, lo, hi) in EDGE_BUCKET_BOUNDS {
        let mut query = Entity::find()
            .filter(base.clone())
            .filter(Column::DetectedEdgeBps.is_not_null());
        if let Some(lo) = lo {
            query = query.filter(Column::DetectedEdgeBps.gte(Decimal::from(lo)));
        }
        if let Some(hi) = hi {
            query = query.filter(Column::DetectedEdgeBps.lt(Decimal::from(hi)));
        }
        let count = query.count(db).await.map_err(StorageError::from)?;
        buckets.push(EdgeBucket { label, count });
    }
    Ok(buckets)
}

/// SQL-side paginated market-performance row (one row per market).
#[derive(Debug, FromQueryResult)]
struct MarketPerfPageRow {
    market_id: MarketId,
    trade_count: i64,
    success_count: i64,
    net_profit_usd: Option<Usd>,
    total_cost_usd: Option<Usd>,
    avg_edge_bps: Option<Decimal>,
}

#[derive(Debug, FromQueryResult)]
struct MarketPerfCountRow {
    total: i64,
}

/// Conditional `SUM(CASE …)` for per-market success counts.
fn success_trade_count() -> SimpleExpr {
    Func::sum(Expr::case(Column::BusinessOutcome.eq(TradeBusinessOutcome::Success), 1).finally(0))
        .into()
}

async fn do_market_performance(
    db: &impl ConnectionTrait,
    filter: TradeAnalyticsFilter,
    page: PageRequest,
) -> Result<Paginated<MarketPerformanceRow>, StorageError> {
    let window_page = page.normalized();
    let base_condition = analytics_trade_condition(filter);

    let count_row = Entity::find()
        .filter(base_condition.clone())
        .select_only()
        .expr_as(Func::count_distinct(Expr::col(Column::MarketId)), "total")
        .into_model::<MarketPerfCountRow>()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .unwrap_or(MarketPerfCountRow { total: 0 });
    let total = u64::try_from(count_row.total.max(0)).unwrap_or(0);
    if total == 0 {
        return Ok(Paginated::from_request(Vec::new(), 0, &window_page));
    }

    let rows = Entity::find()
        .filter(base_condition)
        .select_only()
        .column(Column::MarketId)
        .column_as(Column::TradeId.count(), "trade_count")
        .expr_as(success_trade_count(), "success_count")
        .column_as(Column::NetProfitUsd.sum(), "net_profit_usd")
        .column_as(Column::CostUsd.sum(), "total_cost_usd")
        .expr_as(
            Func::avg(Expr::col(Column::DetectedEdgeBps)),
            "avg_edge_bps",
        )
        .group_by(Column::MarketId)
        .order_by_with_nulls(Column::NetProfitUsd.sum(), Order::Desc, NullOrdering::Last)
        .order_by_asc(Column::MarketId)
        .offset(window_page.offset())
        .limit(window_page.limit())
        .into_model::<MarketPerfPageRow>()
        .all(db)
        .await
        .map_err(StorageError::from)?;

    let items = rows
        .into_iter()
        .map(|row| MarketPerformanceRow {
            market_id: row.market_id,
            trade_count: u64::try_from(row.trade_count).unwrap_or(0),
            success_count: u64::try_from(row.success_count).unwrap_or(0),
            net_profit_usd: row.net_profit_usd.unwrap_or(Usd::ZERO),
            total_cost_usd: row.total_cost_usd.unwrap_or(Usd::ZERO),
            avg_edge_bps: row.avg_edge_bps,
        })
        .collect();

    Ok(Paginated::from_request(items, total, &window_page))
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

    async fn mark_reconciled_observed(
        &self,
        trade_id: &TradeId,
        observation: TradeObservation,
        resolution: TradeReconcileResolution,
        note: &str,
    ) -> Result<bool, StorageError> {
        do_mark_reconciled_observed(&self.db, trade_id, observation, resolution, note).await
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

    async fn find_needs_reconcile(&self, limit: u64) -> Result<Vec<TradeInfo>, StorageError> {
        do_find_needs_reconcile(&self.db, limit).await
    }

    async fn mark_reconciled(
        &self,
        trade_id: &TradeId,
        resolution: TradeReconcileResolution,
        note: &str,
        reconciled_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        do_mark_reconciled(&self.db, trade_id, resolution, note, reconciled_at).await
    }

    async fn find_by_id(&self, trade_id: &TradeId) -> Result<Option<TradeInfo>, StorageError> {
        do_find_by_id(&self.db, trade_id).await
    }

    async fn find_by_execution(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        do_find_by_execution(&self.db, execution_id).await
    }

    async fn page(&self, query: TradePageQuery) -> Result<Paginated<TradeInfo>, StorageError> {
        do_page(&self.db, query).await
    }

    async fn edge_histogram(
        &self,
        filter: TradeAnalyticsFilter,
    ) -> Result<Vec<EdgeBucket>, StorageError> {
        do_edge_histogram(&self.db, filter).await
    }

    async fn market_performance(
        &self,
        filter: TradeAnalyticsFilter,
        page: PageRequest,
    ) -> Result<Paginated<MarketPerformanceRow>, StorageError> {
        do_market_performance(&self.db, filter, page).await
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

    async fn find_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        do_find_between(&self.db, start, end).await
    }

    async fn count_by_outcome(
        &self,
        since: DateTime<Utc>,
    ) -> Result<HashMap<TradeBusinessOutcome, i64>, StorageError> {
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

    async fn mark_reconciled_observed(
        &self,
        trade_id: &TradeId,
        observation: TradeObservation,
        resolution: TradeReconcileResolution,
        note: &str,
    ) -> Result<bool, StorageError> {
        do_mark_reconciled_observed(self.txn, trade_id, observation, resolution, note).await
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

    async fn find_needs_reconcile(&self, limit: u64) -> Result<Vec<TradeInfo>, StorageError> {
        do_find_needs_reconcile(self.txn, limit).await
    }

    async fn mark_reconciled(
        &self,
        trade_id: &TradeId,
        resolution: TradeReconcileResolution,
        note: &str,
        reconciled_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        do_mark_reconciled(self.txn, trade_id, resolution, note, reconciled_at).await
    }

    async fn page(&self, query: TradePageQuery) -> Result<Paginated<TradeInfo>, StorageError> {
        do_page(self.txn, query).await
    }

    async fn edge_histogram(
        &self,
        filter: TradeAnalyticsFilter,
    ) -> Result<Vec<EdgeBucket>, StorageError> {
        do_edge_histogram(self.txn, filter).await
    }

    async fn market_performance(
        &self,
        filter: TradeAnalyticsFilter,
        page: PageRequest,
    ) -> Result<Paginated<MarketPerformanceRow>, StorageError> {
        do_market_performance(self.txn, filter, page).await
    }

    async fn find_by_id(&self, trade_id: &TradeId) -> Result<Option<TradeInfo>, StorageError> {
        do_find_by_id(self.txn, trade_id).await
    }

    async fn find_by_execution(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<Vec<TradeInfo>, StorageError> {
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

    async fn find_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        do_find_between(self.txn, start, end).await
    }

    async fn count_by_outcome(
        &self,
        since: DateTime<Utc>,
    ) -> Result<HashMap<TradeBusinessOutcome, i64>, StorageError> {
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
