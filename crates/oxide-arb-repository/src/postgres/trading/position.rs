use crate::traits::PositionRepository;
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        MarkRedeemedParams, NewPosition, NullablePatch, Paginated, Patch, PositionInfo,
        PositionPageQuery, PositionPatch, SettlePositionParams, SettledPositionStats,
    },
    entities::{
        market::Column as MarketColumn,
        position::{Column, Entity, Relation},
    },
    enums::{
        common::{PositionStatus, RedeemStatus, SettlementAccountingStatus, SettlementTrigger},
        market::MarketStatus,
    },
    types::{MarketId, PositionId, TokenId, TradeId, Usd},
};
use rust_decimal::Decimal;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, IntoActiveModel, JoinType, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
    RelationTrait, sea_query::Condition,
};

// ── helpers ──────────────────────────────────────────────────────────

async fn find_open_q(db: &impl ConnectionTrait) -> Result<Vec<PositionInfo>, StorageError> {
    Entity::find()
        .filter(Column::Status.eq(PositionStatus::Open))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn open_as_of_q(
    db: &impl ConnectionTrait,
    at: DateTime<Utc>,
) -> Result<Vec<PositionInfo>, StorageError> {
    Entity::find()
        .filter(Column::OpenedAt.lte(at))
        .filter(
            Condition::any()
                .add(Column::ClosedAt.is_null())
                .add(Column::ClosedAt.gt(at)),
        )
        .order_by_asc(Column::OpenedAt)
        .order_by_asc(Column::PositionId)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn changed_between_q(
    db: &impl ConnectionTrait,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<PositionInfo>, StorageError> {
    Entity::find()
        .filter(
            Condition::any()
                .add(Column::OpenedAt.between(start, end))
                .add(Column::ClosedAt.between(start, end))
                .add(Column::SettledAt.between(start, end))
                .add(Column::SettlementAccountedAt.between(start, end)),
        )
        .order_by_asc(Column::OpenedAt)
        .order_by_asc(Column::PositionId)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn find_by_id_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
) -> Result<Option<PositionInfo>, StorageError> {
    Entity::find_by_id(position_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|opt| opt.map(Into::into))
}

async fn find_by_market_q(
    db: &impl ConnectionTrait,
    market_id: &MarketId,
) -> Result<Vec<PositionInfo>, StorageError> {
    Entity::find()
        .filter(Column::MarketId.eq(market_id.as_str()))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn find_open_by_market_q(
    db: &impl ConnectionTrait,
    market_id: &MarketId,
) -> Result<Vec<PositionInfo>, StorageError> {
    Entity::find()
        .filter(Column::MarketId.eq(market_id.as_str()))
        .filter(Column::Status.eq(PositionStatus::Open))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn find_by_trade_id_q(
    db: &impl ConnectionTrait,
    trade_id: &TradeId,
) -> Result<Option<PositionInfo>, StorageError> {
    Entity::find()
        .filter(Column::TradeId.eq(trade_id.as_uuid()))
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|opt| opt.map(Into::into))
}

async fn find_redeem_retry_candidates_q(
    db: &impl ConnectionTrait,
    max_attempts: u32,
) -> Result<Vec<PositionInfo>, StorageError> {
    let max_attempts = i32::try_from(max_attempts).unwrap_or(i32::MAX);
    Entity::find()
        .filter(Column::Status.eq(PositionStatus::Open))
        .filter(Column::RedeemStatus.is_in([RedeemStatus::Pending, RedeemStatus::Failed]))
        .filter(Column::RedeemAttempts.lt(max_attempts))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn find_open_for_resolved_markets_q(
    db: &impl ConnectionTrait,
    limit: u64,
) -> Result<Vec<PositionInfo>, StorageError> {
    Entity::find()
        .join(JoinType::InnerJoin, Relation::Market.def())
        .filter(Column::Status.eq(PositionStatus::Open))
        .filter(MarketColumn::Status.ne(MarketStatus::Active))
        .filter(MarketColumn::ResolvedAt.is_not_null())
        .limit(limit)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn find_accounting_retry_candidates_q(
    db: &impl ConnectionTrait,
    max_attempts: u32,
) -> Result<Vec<PositionInfo>, StorageError> {
    let max_attempts = i32::try_from(max_attempts).unwrap_or(i32::MAX);
    Entity::find()
        .filter(Column::Status.eq(PositionStatus::Open))
        .filter(Column::SettlementAccountingStatus.eq(SettlementAccountingStatus::Failed))
        .filter(Column::RedeemAttempts.lt(max_attempts))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn create_q(
    db: &impl ConnectionTrait,
    new: NewPosition,
) -> Result<PositionInfo, StorageError> {
    let model = new
        .into_active_model()
        .insert(db)
        .await
        .map_err(StorageError::from)?;
    Ok(model.into())
}

async fn patch_position_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    patch: PositionPatch,
    entity: &'static str,
) -> Result<PositionInfo, StorageError> {
    let models = Entity::update_many()
        .set(patch.into_active_model())
        .filter(Column::PositionId.eq(position_id.clone()))
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;

    models
        .into_iter()
        .next()
        .map(Into::into)
        .ok_or_else(|| StorageError::NotFound {
            entity,
            id: position_id.to_string(),
        })
}

async fn update_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    patch: PositionPatch,
) -> Result<PositionInfo, StorageError> {
    patch_position_q(db, position_id, patch, "position").await
}

async fn close_position_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    realized_pnl: Decimal,
) -> Result<(), StorageError> {
    let patch = PositionPatch {
        status: Patch::set(PositionStatus::Closed),
        realized_pnl: Patch::set(Usd::new(realized_pnl)),
        closed_at: NullablePatch::set(Utc::now()),
        ..Default::default()
    };
    patch_position_q(db, position_id, patch, "open position").await?;
    Ok(())
}

async fn settle_position_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    params: SettlePositionParams,
) -> Result<PositionInfo, StorageError> {
    let now = Utc::now();
    let patch = PositionPatch {
        status: Patch::set(PositionStatus::Settled),
        realized_pnl: Patch::set(Usd::new(params.realized_pnl)),
        winning_token_id: NullablePatch::set(params.winning_token_id),
        settlement_payout_usd: NullablePatch::set(params.settlement_payout_usd),
        redeem_tx_hash: NullablePatch::set_nullable(params.redeem_tx_hash),
        redeem_status: Patch::set(params.redeem_status),
        settlement_accounting_status: Patch::set(SettlementAccountingStatus::Accounted),
        settlement_accounting_error: NullablePatch::clear(),
        settlement_accounted_at: NullablePatch::set(now),
        oracle_verdict: NullablePatch::set_nullable(params.oracle_verdict),
        settlement_trigger: NullablePatch::set(params.settlement_trigger),
        settled_at: NullablePatch::set(now),
        ..Default::default()
    };
    let models = Entity::update_many()
        .set(patch.into_active_model())
        .filter(Column::PositionId.eq(position_id.clone()))
        .filter(Column::Status.eq(PositionStatus::Open))
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;

    models
        .into_iter()
        .next()
        .map(Into::into)
        .ok_or_else(|| StorageError::NotFound {
            entity: "open position",
            id: position_id.to_string(),
        })
}

async fn mark_redeemed_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    params: MarkRedeemedParams,
) -> Result<PositionInfo, StorageError> {
    let patch = PositionPatch {
        winning_token_id: NullablePatch::set(params.winning_token_id),
        settlement_payout_usd: NullablePatch::set(params.settlement_payout_usd),
        realized_pnl: Patch::set(params.realized_pnl),
        redeem_tx_hash: NullablePatch::set_nullable(params.redeem_tx_hash),
        redeem_status: Patch::set(params.redeem_status),
        settlement_trigger: NullablePatch::set(params.settlement_trigger),
        redeem_terminal_reason: NullablePatch::set_nullable(params.redeem_terminal_reason),
        settlement_accounting_status: Patch::set(SettlementAccountingStatus::Redeemed),
        ..Default::default()
    };
    patch_position_q(db, position_id, patch, "position").await
}

async fn mark_accounted_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    accounted_at: chrono::DateTime<Utc>,
) -> Result<PositionInfo, StorageError> {
    let patch = PositionPatch {
        status: Patch::set(PositionStatus::Settled),
        settlement_accounting_status: Patch::set(SettlementAccountingStatus::Accounted),
        settlement_accounting_error: NullablePatch::clear(),
        settlement_accounted_at: NullablePatch::set(accounted_at),
        settled_at: NullablePatch::set(accounted_at),
        ..Default::default()
    };
    patch_position_q(db, position_id, patch, "position").await
}

async fn mark_accounting_failed_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    error: String,
) -> Result<PositionInfo, StorageError> {
    let patch = PositionPatch {
        settlement_accounting_status: Patch::set(SettlementAccountingStatus::Failed),
        settlement_accounting_error: NullablePatch::set(error),
        ..Default::default()
    };
    patch_position_q(db, position_id, patch, "position").await
}

async fn record_redeem_failure_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    attempts: u32,
    winning_token_id: &TokenId,
    settlement_trigger: SettlementTrigger,
) -> Result<PositionInfo, StorageError> {
    let patch = PositionPatch {
        redeem_status: Patch::set(RedeemStatus::Failed),
        redeem_attempts: Patch::set(i32::try_from(attempts).unwrap_or(i32::MAX)),
        winning_token_id: NullablePatch::set(winning_token_id.clone()),
        settlement_trigger: NullablePatch::set(settlement_trigger),
        ..Default::default()
    };
    patch_position_q(db, position_id, patch, "position").await
}

async fn mark_redeem_terminal_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    attempts: u32,
    winning_token_id: &TokenId,
    settlement_trigger: SettlementTrigger,
    reason: String,
) -> Result<PositionInfo, StorageError> {
    let patch = PositionPatch {
        redeem_status: Patch::set(RedeemStatus::Failed),
        redeem_attempts: Patch::set(i32::try_from(attempts).unwrap_or(i32::MAX)),
        winning_token_id: NullablePatch::set(winning_token_id.clone()),
        settlement_trigger: NullablePatch::set(settlement_trigger),
        settlement_accounting_status: Patch::set(SettlementAccountingStatus::Failed),
        settlement_accounting_error: NullablePatch::set(reason.clone()),
        redeem_terminal_reason: NullablePatch::set(reason),
        ..Default::default()
    };
    patch_position_q(db, position_id, patch, "position").await
}

async fn patch_oracle_verdict_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    verdict: serde_json::Value,
) -> Result<(), StorageError> {
    let patch = PositionPatch {
        oracle_verdict: NullablePatch::set(verdict),
        ..Default::default()
    };
    patch_position_q(db, position_id, patch, "position").await?;
    Ok(())
}

async fn total_exposure_q(db: &impl ConnectionTrait) -> Result<Usd, StorageError> {
    let positions = Entity::find()
        .filter(Column::Status.eq(PositionStatus::Open))
        .all(db)
        .await
        .map_err(StorageError::from)?;

    Ok(positions.iter().map(|p| p.total_cost_usd).sum())
}

async fn count_open_q(db: &impl ConnectionTrait) -> Result<usize, StorageError> {
    let count = Entity::find()
        .filter(Column::Status.eq(PositionStatus::Open))
        .count(db)
        .await
        .map_err(StorageError::from)?;
    Ok(ToPrimitive::to_usize(&count).unwrap_or(usize::MAX))
}

async fn aggregate_settled_between_q(
    db: &impl ConnectionTrait,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<SettledPositionStats, StorageError> {
    let settled: Vec<PositionInfo> = Entity::find()
        .filter(Column::Status.eq(PositionStatus::Settled))
        .filter(Column::SettlementAccountingStatus.eq(SettlementAccountingStatus::Accounted))
        .filter(Column::SettledAt.gte(start))
        .filter(Column::SettledAt.lt(end))
        .all(db)
        .await
        .map_err(StorageError::from)?
        .into_iter()
        .map(Into::into)
        .collect();
    let unsettled_count = Entity::find()
        .filter(Column::Status.eq(PositionStatus::Open))
        .count(db)
        .await
        .map_err(StorageError::from)?;
    let failed_count = Entity::find()
        .filter(Column::SettlementAccountingStatus.eq(SettlementAccountingStatus::Failed))
        .count(db)
        .await
        .map_err(StorageError::from)?;

    let mut stats = SettledPositionStats {
        realized_pnl: Usd::ZERO,
        total_payout: Usd::ZERO,
        total_cost: Usd::ZERO,
        total_fees: Usd::ZERO,
        settled_position_count: 0,
        winning_position_count: 0,
        losing_position_count: 0,
        unsettled_position_count: ToPrimitive::to_u32(&unsettled_count).unwrap_or(u32::MAX),
        failed_accounting_count: ToPrimitive::to_u32(&failed_count).unwrap_or(u32::MAX),
        largest_single_profit: Usd::ZERO,
        largest_single_loss: Usd::ZERO,
    };

    for position in settled {
        stats.settled_position_count = stats.settled_position_count.saturating_add(1);
        stats.realized_pnl += position.realized_pnl;
        stats.total_cost += position.total_cost_usd;
        stats.total_fees += position.total_fees_usd;
        if let Some(payout) = position.settlement_payout_usd {
            stats.total_payout += payout;
            if payout > Usd::ZERO {
                stats.winning_position_count = stats.winning_position_count.saturating_add(1);
            } else {
                stats.losing_position_count = stats.losing_position_count.saturating_add(1);
            }
        }
        if position.realized_pnl > stats.largest_single_profit {
            stats.largest_single_profit = position.realized_pnl;
        }
        if position.realized_pnl < stats.largest_single_loss {
            stats.largest_single_loss = position.realized_pnl;
        }
    }

    Ok(stats)
}

// ── connection-based impl ────────────────────────────────────────────

pub struct PgPositionRepository {
    db: DatabaseConnection,
}

impl PgPositionRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgPositionRepositoryTxn<'_> {
        PgPositionRepositoryTxn { txn }
    }

    pub async fn settlement_payout_total(&self) -> Result<Usd, StorageError> {
        let positions = Entity::find()
            .filter(Column::Status.eq(PositionStatus::Settled))
            .filter(Column::SettlementAccountingStatus.eq(SettlementAccountingStatus::Accounted))
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(positions
            .iter()
            .filter_map(|position| position.settlement_payout_usd)
            .sum())
    }
}

fn page_condition(query: &PositionPageQuery) -> Condition {
    let mut condition = Condition::all();
    if let Some(market_id) = &query.market_id {
        condition = condition.add(Column::MarketId.eq(market_id.as_str()));
    }
    if let Some(status) = query.status {
        condition = condition.add(Column::Status.eq(status));
    }
    condition
}

async fn page_q(
    db: &impl ConnectionTrait,
    query: PositionPageQuery,
) -> Result<Paginated<PositionInfo>, StorageError> {
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
        .order_by_desc(Column::OpenedAt)
        .order_by_desc(Column::PositionId)
        .offset(window.offset())
        .limit(window.limit())
        .all(db)
        .await
        .map_err(StorageError::from)?;
    let items = models.into_iter().map(Into::into).collect();
    Ok(Paginated::from_request(items, total, &window))
}

#[async_trait::async_trait]
impl PositionRepository for PgPositionRepository {
    async fn page(
        &self,
        query: PositionPageQuery,
    ) -> Result<Paginated<PositionInfo>, StorageError> {
        page_q(&self.db, query).await
    }

    async fn find_open(&self) -> Result<Vec<PositionInfo>, StorageError> {
        find_open_q(&self.db).await
    }

    async fn open_as_of(&self, at: DateTime<Utc>) -> Result<Vec<PositionInfo>, StorageError> {
        open_as_of_q(&self.db, at).await
    }

    async fn changed_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        changed_between_q(&self.db, start, end).await
    }

    async fn find_by_id(
        &self,
        position_id: &PositionId,
    ) -> Result<Option<PositionInfo>, StorageError> {
        find_by_id_q(&self.db, position_id).await
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        find_by_market_q(&self.db, market_id).await
    }

    async fn find_open_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        find_open_by_market_q(&self.db, market_id).await
    }

    async fn find_by_trade_id(
        &self,
        trade_id: &TradeId,
    ) -> Result<Option<PositionInfo>, StorageError> {
        find_by_trade_id_q(&self.db, trade_id).await
    }

    async fn find_redeem_retry_candidates(
        &self,
        max_attempts: u32,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        find_redeem_retry_candidates_q(&self.db, max_attempts).await
    }

    async fn find_open_for_resolved_markets(
        &self,
        limit: u64,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        find_open_for_resolved_markets_q(&self.db, limit).await
    }

    async fn find_accounting_retry_candidates(
        &self,
        max_attempts: u32,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        find_accounting_retry_candidates_q(&self.db, max_attempts).await
    }

    async fn create(&self, position: NewPosition) -> Result<PositionInfo, StorageError> {
        create_q(&self.db, position).await
    }

    async fn update(
        &self,
        position_id: &PositionId,
        patch: PositionPatch,
    ) -> Result<PositionInfo, StorageError> {
        update_q(&self.db, position_id, patch).await
    }

    async fn close_position(
        &self,
        position_id: &PositionId,
        realized_pnl: Decimal,
    ) -> Result<(), StorageError> {
        close_position_q(&self.db, position_id, realized_pnl).await
    }

    async fn settle_position(
        &self,
        position_id: &PositionId,
        params: SettlePositionParams,
    ) -> Result<PositionInfo, StorageError> {
        settle_position_q(&self.db, position_id, params).await
    }

    async fn mark_redeemed(
        &self,
        position_id: &PositionId,
        params: MarkRedeemedParams,
    ) -> Result<PositionInfo, StorageError> {
        mark_redeemed_q(&self.db, position_id, params).await
    }

    async fn mark_accounted(
        &self,
        position_id: &PositionId,
        accounted_at: chrono::DateTime<Utc>,
    ) -> Result<PositionInfo, StorageError> {
        mark_accounted_q(&self.db, position_id, accounted_at).await
    }

    async fn mark_accounting_failed(
        &self,
        position_id: &PositionId,
        error: String,
    ) -> Result<PositionInfo, StorageError> {
        mark_accounting_failed_q(&self.db, position_id, error).await
    }

    async fn record_redeem_failure(
        &self,
        position_id: &PositionId,
        attempts: u32,
        winning_token_id: &TokenId,
        settlement_trigger: SettlementTrigger,
    ) -> Result<PositionInfo, StorageError> {
        record_redeem_failure_q(
            &self.db,
            position_id,
            attempts,
            winning_token_id,
            settlement_trigger,
        )
        .await
    }

    async fn mark_redeem_terminal(
        &self,
        position_id: &PositionId,
        attempts: u32,
        winning_token_id: &TokenId,
        settlement_trigger: SettlementTrigger,
        reason: String,
    ) -> Result<PositionInfo, StorageError> {
        mark_redeem_terminal_q(
            &self.db,
            position_id,
            attempts,
            winning_token_id,
            settlement_trigger,
            reason,
        )
        .await
    }

    async fn patch_oracle_verdict(
        &self,
        position_id: &PositionId,
        verdict: serde_json::Value,
    ) -> Result<(), StorageError> {
        patch_oracle_verdict_q(&self.db, position_id, verdict).await
    }

    async fn total_exposure(&self) -> Result<Usd, StorageError> {
        total_exposure_q(&self.db).await
    }

    async fn count_open(&self) -> Result<usize, StorageError> {
        count_open_q(&self.db).await
    }

    async fn aggregate_settled_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<SettledPositionStats, StorageError> {
        aggregate_settled_between_q(&self.db, start, end).await
    }
}

// ── transaction-based impl ───────────────────────────────────────────

pub struct PgPositionRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

#[async_trait::async_trait]
impl PositionRepository for PgPositionRepositoryTxn<'_> {
    async fn page(
        &self,
        query: PositionPageQuery,
    ) -> Result<Paginated<PositionInfo>, StorageError> {
        page_q(self.txn, query).await
    }

    async fn find_open(&self) -> Result<Vec<PositionInfo>, StorageError> {
        find_open_q(self.txn).await
    }

    async fn open_as_of(&self, at: DateTime<Utc>) -> Result<Vec<PositionInfo>, StorageError> {
        open_as_of_q(self.txn, at).await
    }

    async fn changed_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        changed_between_q(self.txn, start, end).await
    }

    async fn find_by_id(
        &self,
        position_id: &PositionId,
    ) -> Result<Option<PositionInfo>, StorageError> {
        find_by_id_q(self.txn, position_id).await
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        find_by_market_q(self.txn, market_id).await
    }

    async fn find_open_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        find_open_by_market_q(self.txn, market_id).await
    }

    async fn find_by_trade_id(
        &self,
        trade_id: &TradeId,
    ) -> Result<Option<PositionInfo>, StorageError> {
        find_by_trade_id_q(self.txn, trade_id).await
    }

    async fn find_redeem_retry_candidates(
        &self,
        max_attempts: u32,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        find_redeem_retry_candidates_q(self.txn, max_attempts).await
    }

    async fn find_open_for_resolved_markets(
        &self,
        limit: u64,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        find_open_for_resolved_markets_q(self.txn, limit).await
    }

    async fn find_accounting_retry_candidates(
        &self,
        max_attempts: u32,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        find_accounting_retry_candidates_q(self.txn, max_attempts).await
    }

    async fn create(&self, position: NewPosition) -> Result<PositionInfo, StorageError> {
        create_q(self.txn, position).await
    }

    async fn update(
        &self,
        position_id: &PositionId,
        patch: PositionPatch,
    ) -> Result<PositionInfo, StorageError> {
        update_q(self.txn, position_id, patch).await
    }

    async fn close_position(
        &self,
        position_id: &PositionId,
        realized_pnl: Decimal,
    ) -> Result<(), StorageError> {
        close_position_q(self.txn, position_id, realized_pnl).await
    }

    async fn settle_position(
        &self,
        position_id: &PositionId,
        params: SettlePositionParams,
    ) -> Result<PositionInfo, StorageError> {
        settle_position_q(self.txn, position_id, params).await
    }

    async fn mark_redeemed(
        &self,
        position_id: &PositionId,
        params: MarkRedeemedParams,
    ) -> Result<PositionInfo, StorageError> {
        mark_redeemed_q(self.txn, position_id, params).await
    }

    async fn mark_accounted(
        &self,
        position_id: &PositionId,
        accounted_at: chrono::DateTime<Utc>,
    ) -> Result<PositionInfo, StorageError> {
        mark_accounted_q(self.txn, position_id, accounted_at).await
    }

    async fn mark_accounting_failed(
        &self,
        position_id: &PositionId,
        error: String,
    ) -> Result<PositionInfo, StorageError> {
        mark_accounting_failed_q(self.txn, position_id, error).await
    }

    async fn record_redeem_failure(
        &self,
        position_id: &PositionId,
        attempts: u32,
        winning_token_id: &TokenId,
        settlement_trigger: SettlementTrigger,
    ) -> Result<PositionInfo, StorageError> {
        record_redeem_failure_q(
            self.txn,
            position_id,
            attempts,
            winning_token_id,
            settlement_trigger,
        )
        .await
    }

    async fn mark_redeem_terminal(
        &self,
        position_id: &PositionId,
        attempts: u32,
        winning_token_id: &TokenId,
        settlement_trigger: SettlementTrigger,
        reason: String,
    ) -> Result<PositionInfo, StorageError> {
        mark_redeem_terminal_q(
            self.txn,
            position_id,
            attempts,
            winning_token_id,
            settlement_trigger,
            reason,
        )
        .await
    }

    async fn patch_oracle_verdict(
        &self,
        position_id: &PositionId,
        verdict: serde_json::Value,
    ) -> Result<(), StorageError> {
        patch_oracle_verdict_q(self.txn, position_id, verdict).await
    }

    async fn total_exposure(&self) -> Result<Usd, StorageError> {
        total_exposure_q(self.txn).await
    }

    async fn count_open(&self) -> Result<usize, StorageError> {
        count_open_q(self.txn).await
    }

    async fn aggregate_settled_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<SettledPositionStats, StorageError> {
        aggregate_settled_between_q(self.txn, start, end).await
    }
}
