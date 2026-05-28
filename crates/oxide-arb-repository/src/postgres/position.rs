use super::orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, IntoActiveModel, PaginatorTrait, QueryFilter, QuerySelect, Set,
};
use crate::traits::PositionRepository;
use chrono::Utc;
use num_traits::ToPrimitive;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{MarkRedeemedParams, NewPosition, PositionInfo, SettlePositionParams, UpdatePosition},
    entities::{
        market::Column as MarketColumn,
        position::{ActiveModel, Column, Entity, Relation},
    },
    enums::{
        common::{PositionStatus, RedeemStatus, SettlementAccountingStatus, SettlementTrigger},
        market::MarketStatus,
    },
    types::{MarketId, PositionId, TokenId, TradeId, Usd},
};
use rust_decimal::Decimal;
use sea_orm::{JoinType, RelationTrait};

// ── helpers ──────────────────────────────────────────────────────────

async fn find_open_q(db: &impl ConnectionTrait) -> Result<Vec<PositionInfo>, StorageError> {
    Entity::find()
        .filter(Column::Status.eq(PositionStatus::Open))
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
        .filter(Column::TradeId.eq(trade_id.as_str()))
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

async fn update_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    update: UpdatePosition,
) -> Result<PositionInfo, StorageError> {
    let existing = Entity::find_by_id(position_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "position",
            id: position_id.to_string(),
        })?;

    let mut active: ActiveModel = existing.into();
    if let Some(shares) = update.shares {
        active.shares = Set(shares);
    }
    if let Some(price) = update.avg_entry_price {
        active.avg_entry_price = Set(price);
    }
    if let Some(cost) = update.total_cost_usd {
        active.total_cost_usd = Set(cost);
    }
    if let Some(fees) = update.total_fees_usd {
        active.total_fees_usd = Set(fees);
    }
    if let Some(pnl) = update.unrealized_pnl {
        active.unrealized_pnl = Set(pnl);
    }
    if let Some(pnl) = update.realized_pnl {
        active.realized_pnl = Set(pnl);
    }
    if let Some(status) = update.status {
        active.status = Set(status);
    }
    if let Some(closed) = update.closed_at {
        active.closed_at = Set(Some(closed));
    }
    if let Some(settled) = update.settled_at {
        active.settled_at = Set(Some(settled));
    }
    if let Some(winning_token_id) = update.winning_token_id {
        active.winning_token_id = Set(Some(winning_token_id));
    }
    if let Some(payout) = update.settlement_payout_usd {
        active.settlement_payout_usd = Set(Some(payout));
    }
    if let Some(tx_hash) = update.redeem_tx_hash {
        active.redeem_tx_hash = Set(Some(tx_hash));
    }
    if let Some(status) = update.redeem_status {
        active.redeem_status = Set(status);
    }
    if let Some(attempts) = update.redeem_attempts {
        active.redeem_attempts = Set(attempts);
    }
    if let Some(verdict) = update.oracle_verdict {
        active.oracle_verdict = Set(Some(verdict));
    }
    if let Some(trigger) = update.settlement_trigger {
        active.settlement_trigger = Set(Some(trigger));
    }
    if let Some(status) = update.settlement_accounting_status {
        active.settlement_accounting_status = Set(status);
    }
    if let Some(error) = update.settlement_accounting_error {
        active.settlement_accounting_error = Set(Some(error));
    }
    if let Some(accounted_at) = update.settlement_accounted_at {
        active.settlement_accounted_at = Set(Some(accounted_at));
    }
    if let Some(reason) = update.redeem_terminal_reason {
        active.redeem_terminal_reason = Set(Some(reason));
    }

    let model = active.update(db).await.map_err(StorageError::from)?;
    Ok(model.into())
}

async fn close_position_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    realized_pnl: Decimal,
) -> Result<(), StorageError> {
    let existing = Entity::find_by_id(position_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "position",
            id: position_id.to_string(),
        })?;

    let mut active: ActiveModel = existing.into();
    active.status = Set(PositionStatus::Closed);
    active.realized_pnl = Set(Usd::new(realized_pnl));
    active.closed_at = Set(Some(Utc::now()));
    active.update(db).await.map_err(StorageError::from)?;
    Ok(())
}

async fn settle_position_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    params: SettlePositionParams,
) -> Result<PositionInfo, StorageError> {
    let existing = Entity::find_by_id(position_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "position",
            id: position_id.to_string(),
        })?;

    if existing.status != PositionStatus::Open {
        return Err(StorageError::NotFound {
            entity: "open position",
            id: position_id.to_string(),
        });
    }

    let mut active: ActiveModel = existing.into();
    active.status = Set(PositionStatus::Settled);
    active.realized_pnl = Set(Usd::new(params.realized_pnl));
    active.winning_token_id = Set(Some(params.winning_token_id));
    active.settlement_payout_usd = Set(Some(params.settlement_payout_usd));
    active.redeem_tx_hash = Set(params.redeem_tx_hash);
    active.redeem_status = Set(params.redeem_status);
    active.settlement_accounting_status = Set(SettlementAccountingStatus::Accounted);
    active.settlement_accounting_error = Set(None);
    active.settlement_accounted_at = Set(Some(Utc::now()));
    active.oracle_verdict = Set(params.oracle_verdict);
    active.settlement_trigger = Set(Some(params.settlement_trigger));
    active.settled_at = Set(Some(Utc::now()));
    let model = active.update(db).await.map_err(StorageError::from)?;
    Ok(model.into())
}

async fn mark_redeemed_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    params: MarkRedeemedParams,
) -> Result<PositionInfo, StorageError> {
    let existing = Entity::find_by_id(position_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "position",
            id: position_id.to_string(),
        })?;

    let mut active: ActiveModel = existing.into();
    active.winning_token_id = Set(Some(params.winning_token_id));
    active.settlement_payout_usd = Set(Some(params.settlement_payout_usd));
    active.redeem_tx_hash = Set(params.redeem_tx_hash);
    active.redeem_status = Set(params.redeem_status);
    active.settlement_trigger = Set(Some(params.settlement_trigger));
    active.redeem_terminal_reason = Set(params.redeem_terminal_reason);
    active.settlement_accounting_status = Set(SettlementAccountingStatus::Redeemed);
    let model = active.update(db).await.map_err(StorageError::from)?;
    Ok(model.into())
}

async fn mark_accounted_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    accounted_at: chrono::DateTime<Utc>,
) -> Result<PositionInfo, StorageError> {
    let existing = Entity::find_by_id(position_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "position",
            id: position_id.to_string(),
        })?;

    let mut active: ActiveModel = existing.into();
    active.status = Set(PositionStatus::Settled);
    active.settlement_accounting_status = Set(SettlementAccountingStatus::Accounted);
    active.settlement_accounting_error = Set(None);
    active.settlement_accounted_at = Set(Some(accounted_at));
    active.settled_at = Set(Some(accounted_at));
    let model = active.update(db).await.map_err(StorageError::from)?;
    Ok(model.into())
}

async fn mark_accounting_failed_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    error: String,
) -> Result<PositionInfo, StorageError> {
    let existing = Entity::find_by_id(position_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "position",
            id: position_id.to_string(),
        })?;

    let mut active: ActiveModel = existing.into();
    active.settlement_accounting_status = Set(SettlementAccountingStatus::Failed);
    active.settlement_accounting_error = Set(Some(error));
    let model = active.update(db).await.map_err(StorageError::from)?;
    Ok(model.into())
}

async fn record_redeem_failure_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    attempts: u32,
    winning_token_id: &TokenId,
    settlement_trigger: SettlementTrigger,
) -> Result<PositionInfo, StorageError> {
    let existing = Entity::find_by_id(position_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "position",
            id: position_id.to_string(),
        })?;

    let mut active: ActiveModel = existing.into();
    active.redeem_status = Set(RedeemStatus::Failed);
    active.redeem_attempts = Set(i32::try_from(attempts).unwrap_or(i32::MAX));
    active.winning_token_id = Set(Some(winning_token_id.clone()));
    active.settlement_trigger = Set(Some(settlement_trigger));
    let model = active.update(db).await.map_err(StorageError::from)?;
    Ok(model.into())
}

async fn patch_oracle_verdict_q(
    db: &impl ConnectionTrait,
    position_id: &PositionId,
    verdict: serde_json::Value,
) -> Result<(), StorageError> {
    let existing = Entity::find_by_id(position_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "position",
            id: position_id.to_string(),
        })?;

    let mut active: ActiveModel = existing.into();
    active.oracle_verdict = Set(Some(verdict));
    active.update(db).await.map_err(StorageError::from)?;
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
}

#[async_trait::async_trait]
impl PositionRepository for PgPositionRepository {
    async fn find_open(&self) -> Result<Vec<PositionInfo>, StorageError> {
        find_open_q(&self.db).await
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
        update: UpdatePosition,
    ) -> Result<PositionInfo, StorageError> {
        update_q(&self.db, position_id, update).await
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
}

// ── transaction-based impl ───────────────────────────────────────────

pub struct PgPositionRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

#[async_trait::async_trait]
impl PositionRepository for PgPositionRepositoryTxn<'_> {
    async fn find_open(&self) -> Result<Vec<PositionInfo>, StorageError> {
        find_open_q(self.txn).await
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
        update: UpdatePosition,
    ) -> Result<PositionInfo, StorageError> {
        update_q(self.txn, position_id, update).await
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
}
