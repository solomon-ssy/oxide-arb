//! Postgres-backed position ledger repository.

use crate::traits::PositionRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewPosition, PositionExit, PositionFill, PositionInfo},
    entities::quant_position,
    enums::execution::PositionLedgerState,
    types::{MarketId, Price, Shares, TokenId, Usd},
};
use rust_decimal::Decimal;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter,
};

/// Postgres-backed current-position ledger repository.
pub struct PgPositionRepository {
    db: DatabaseConnection,
}

impl PgPositionRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl PositionRepository for PgPositionRepository {
    async fn apply_fill(&self, fill: PositionFill) -> Result<PositionInfo, StorageError> {
        apply_fill(&self.db, fill).await
    }

    async fn apply_exit(
        &self,
        token_id: &TokenId,
        exit: PositionExit,
    ) -> Result<PositionInfo, StorageError> {
        if !exit.shares.is_positive() {
            return Err(StorageError::Conflict(
                "position exit must have positive shares".to_owned(),
            ));
        }

        let Some(row) = quant_position::Entity::find_by_id(token_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::Conflict(format!(
                "position not found: {token_id}"
            )));
        };

        if exit.shares > row.shares {
            return Err(StorageError::Conflict(format!(
                "exit shares exceed position shares for {token_id}: {} > {}",
                exit.shares, row.shares
            )));
        }

        let cost_reduction = row.avg_price * exit.shares;
        let shares = row.shares - exit.shares;
        let cost_usd = row.cost_usd - cost_reduction;
        let realized_pnl_usd = row.realized_pnl_usd + exit.realized_pnl_usd;
        let (state, closed_at, normalized_cost) = if shares.is_zero() {
            (PositionLedgerState::Closed, Some(exit.exited_at), Usd::ZERO)
        } else {
            (PositionLedgerState::Closing, row.closed_at, cost_usd)
        };
        let avg_price = if shares.is_zero() {
            Price::ZERO
        } else {
            price_from_cost_and_shares(normalized_cost, shares)?
        };

        let mut active = row.into_active_model();
        active.state = ActiveValue::Set(state);
        active.shares = ActiveValue::Set(shares);
        active.avg_price = ActiveValue::Set(avg_price);
        active.cost_usd = ActiveValue::Set(normalized_cost);
        active.realized_pnl_usd = ActiveValue::Set(realized_pnl_usd);
        active.updated_at = ActiveValue::Set(exit.exited_at);
        active.closed_at = ActiveValue::Set(closed_at);
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_token(
        &self,
        token_id: &TokenId,
    ) -> Result<Option<PositionInfo>, StorageError> {
        quant_position::Entity::find_by_id(token_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_open_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        quant_position::Entity::find()
            .filter(quant_position::Column::MarketId.eq(market_id.clone()))
            .filter(
                quant_position::Column::State
                    .is_in([PositionLedgerState::Open, PositionLedgerState::Closing]),
            )
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}

/// Upsert a position from a fill (weighted-average cost), on the caller's
/// connection — usable inside the execution-submission transaction so the
/// position ledger and capital settlement commit atomically.
pub async fn apply_fill(
    db: &impl ConnectionTrait,
    fill: PositionFill,
) -> Result<PositionInfo, StorageError> {
    if !fill.shares.is_positive() || fill.cost_usd.is_negative() {
        return Err(StorageError::Conflict(
            "position fill must have positive shares and non-negative cost".to_owned(),
        ));
    }

    let existing = quant_position::Entity::find_by_id(fill.token_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?;

    let Some(row) = existing else {
        return quant_position::Entity::insert(
            NewPosition {
                token_id: fill.token_id,
                market_id: fill.market_id,
                event_id: fill.event_id,
                category: fill.category,
                side: fill.side,
                state: PositionLedgerState::Open,
                shares: fill.shares,
                avg_price: fill.price,
                cost_usd: fill.cost_usd,
                realized_pnl_usd: Usd::ZERO,
                source: fill.source,
                opened_at: fill.filled_at,
                closed_at: None,
            }
            .into_active_model(),
        )
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)
        .map(Into::into);
    };

    if row.state == PositionLedgerState::Closed || row.state == PositionLedgerState::Settled {
        return Err(StorageError::Conflict(format!(
            "cannot apply fill to closed position: {}",
            row.token_id
        )));
    }

    let shares = row.shares + fill.shares;
    let cost_usd = row.cost_usd + fill.cost_usd;
    let avg_price = price_from_cost_and_shares(cost_usd, shares)?;
    let mut active = row.into_active_model();
    active.state = ActiveValue::Set(PositionLedgerState::Open);
    active.shares = ActiveValue::Set(shares);
    active.avg_price = ActiveValue::Set(avg_price);
    active.cost_usd = ActiveValue::Set(cost_usd);
    active.source = ActiveValue::Set(fill.source);
    active.updated_at = ActiveValue::Set(fill.filled_at);
    active.closed_at = ActiveValue::Set(None);
    active
        .update(db)
        .await
        .map_err(StorageError::from)
        .map(Into::into)
}

fn price_from_cost_and_shares(cost_usd: Usd, shares: Shares) -> Result<Price, StorageError> {
    if shares.is_zero() {
        return Err(StorageError::Conflict(
            "cannot compute average price from zero shares".to_owned(),
        ));
    }
    let price = cost_usd.inner() / shares.inner();
    if price < Decimal::ZERO {
        return Err(StorageError::Conflict(
            "computed average price is negative".to_owned(),
        ));
    }
    Ok(Price::new(price))
}
