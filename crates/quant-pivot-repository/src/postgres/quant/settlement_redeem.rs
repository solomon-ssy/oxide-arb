//! Postgres-backed settlement redemption ledger repository.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_ORDER_INTENT, QUANT_SETTLEMENT_REDEEM},
};
use quant_pivot_models::{
    domain::{
        api::{SettlementRedeemListQuery, SettlementRedeemSummary},
        pagination::{PageWindow, Paginated},
        quant::{
            ConfirmSettlementRedeem, NewSettlementRedeem, SettlementRedeemInfo,
            SettlementRedeemLotInfo,
        },
    },
    entities::{
        quant_order_intent::Entity as QuantOrderIntentEntity,
        quant_settlement_redeem::{Column, Entity, Model},
        quant_settlement_redeem_lot::{
            Column as QuantSettlementRedeemLotColumn, Entity as QuantSettlementRedeemLotEntity,
        },
    },
    enums::execution::{ExitReason, ExitState, SettlementRedeemState},
    types::{EvmAddress, EvmTransactionHash, MarketId, OrderIntentId, SettlementRedeemId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    EntityTrait, ExprTrait, FromQueryResult, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait, sea_query::Expr,
};

use crate::{
    postgres::{
        error,
        quant::{capital_allocation::complete_exit_capital, position},
        query::paginate_mapped,
    },
    traits::SettlementRedeemRepository,
};

pub struct PgSettlementRedeemRepository {
    db: DatabaseConnection,
}

impl PgSettlementRedeemRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl SettlementRedeemRepository for PgSettlementRedeemRepository {
    async fn find_by_id(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
    ) -> Result<Option<SettlementRedeemInfo>, StorageError> {
        Entity::find_by_id(settlement_redeem_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: SettlementRedeemListQuery,
    ) -> Result<Paginated<SettlementRedeemSummary>, StorageError> {
        let page: Paginated<SettlementRedeemInfo> = paginate_mapped(
            Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await?;
        let counts = lot_counts_for(
            &self.db,
            page.items
                .iter()
                .map(|redeem| redeem.settlement_redeem_id.clone()),
        )
        .await?;
        Ok(page.map(|redeem| {
            let lot_count = counts
                .get(&redeem.settlement_redeem_id)
                .copied()
                .unwrap_or(0);
            SettlementRedeemSummary { redeem, lot_count }
        }))
    }

    async fn list_lots_by_redeem(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
    ) -> Result<Vec<SettlementRedeemLotInfo>, StorageError> {
        QuantSettlementRedeemLotEntity::find()
            .filter(
                QuantSettlementRedeemLotColumn::SettlementRedeemId.eq(settlement_redeem_id.clone()),
            )
            .order_by_asc(QuantSettlementRedeemLotColumn::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_by_market_funder(
        &self,
        market_id: &MarketId,
        funder_address: &EvmAddress,
    ) -> Result<Option<SettlementRedeemInfo>, StorageError> {
        Entity::find()
            .filter(Column::MarketId.eq(market_id.clone()))
            .filter(Column::FunderAddress.eq(funder_address))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn upsert_pending(
        &self,
        redeem: NewSettlementRedeem,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        if let Some(row) = Entity::find()
            .filter(Column::MarketId.eq(redeem.market_id.clone()))
            .filter(Column::FunderAddress.eq(redeem.funder_address.clone()))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        {
            if matches!(
                row.state,
                SettlementRedeemState::Submitted
                    | SettlementRedeemState::Confirmed
                    | SettlementRedeemState::ManualRequired
            ) {
                return Ok(row.into());
            }
            let mut active = row.into_active_model();
            active.wallet_kind = ActiveValue::Set(redeem.wallet_kind);
            active.state = ActiveValue::Set(redeem.state);
            active.tx_hash = ActiveValue::Set(redeem.tx_hash);
            active.index_sets_json = ActiveValue::Set(redeem.index_sets_json);
            active.payout_vector_json = ActiveValue::Set(redeem.payout_vector_json);
            active.balance_before_json = ActiveValue::Set(redeem.balance_before_json);
            active.balance_after_json = ActiveValue::Set(redeem.balance_after_json);
            active.payout_usd = ActiveValue::Set(redeem.payout_usd);
            active.gas_fee_pol = ActiveValue::Set(redeem.gas_fee_pol);
            active.next_attempt_at = ActiveValue::Set(redeem.next_attempt_at);
            active.last_error = ActiveValue::Set(redeem.last_error);
            active.submitted_at = ActiveValue::Set(redeem.submitted_at);
            active.confirmed_at = ActiveValue::Set(redeem.confirmed_at);
            active.failed_at = ActiveValue::Set(redeem.failed_at);
            return active
                .update(&self.db)
                .await
                .map_err(StorageError::from)
                .map(Into::into);
        }

        Entity::insert(redeem.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn mark_submitted(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        tx_hash: EvmTransactionHash,
        submitted_at: DateTime<Utc>,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        let row = load_redeem(&self.db, settlement_redeem_id).await?;
        if row.state == SettlementRedeemState::Confirmed {
            return Ok(row.into());
        }
        let mut active = row.into_active_model();
        active.state = ActiveValue::Set(SettlementRedeemState::Submitted);
        active.tx_hash = ActiveValue::Set(Some(tx_hash));
        active.submitted_at = ActiveValue::Set(Some(submitted_at));
        active.last_error = ActiveValue::Set(None);
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn mark_failed(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        error: String,
        next_attempt_at: Option<DateTime<Utc>>,
        failed_at: DateTime<Utc>,
        manual_required: bool,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        let row = load_redeem(&self.db, settlement_redeem_id).await?;
        if row.state == SettlementRedeemState::Confirmed {
            return Ok(row.into());
        }
        let attempt_count = row.attempt_count + 1;
        let mut active = row.into_active_model();
        active.state = ActiveValue::Set(if manual_required {
            SettlementRedeemState::ManualRequired
        } else {
            SettlementRedeemState::Failed
        });
        active.attempt_count = ActiveValue::Set(attempt_count);
        active.next_attempt_at = ActiveValue::Set(next_attempt_at);
        active.last_error = ActiveValue::Set(Some(error));
        active.failed_at = ActiveValue::Set(Some(failed_at));
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn confirm(
        &self,
        write: ConfirmSettlementRedeem,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        if write.lots.is_empty() {
            return Err(error::invariant_violation(
                Some(QUANT_SETTLEMENT_REDEEM),
                "confirmed settlement redeem must close at least one lot",
            ));
        }

        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let redeem = Entity::find_by_id(write.settlement_redeem_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                error::not_found(QUANT_SETTLEMENT_REDEEM, &write.settlement_redeem_id)
            })?;

        if redeem.state == SettlementRedeemState::Confirmed {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(redeem.into());
        }

        let mut redeem_active = redeem.into_active_model();
        redeem_active.state = ActiveValue::Set(SettlementRedeemState::Confirmed);
        redeem_active.balance_after_json = ActiveValue::Set(Some(write.balance_after_json));
        redeem_active.payout_usd = ActiveValue::Set(write.payout_usd);
        redeem_active.gas_fee_pol = ActiveValue::Set(write.gas_fee_pol);
        redeem_active.confirmed_at = ActiveValue::Set(Some(write.confirmed_at));
        redeem_active.next_attempt_at = ActiveValue::Set(None);
        redeem_active.last_error = ActiveValue::Set(None);
        let confirmed = redeem_active
            .update(&txn)
            .await
            .map_err(StorageError::from)?;

        for lot_write in write.lots {
            let intent_id = lot_write.lot.order_intent_id.clone();
            QuantSettlementRedeemLotEntity::insert(lot_write.lot.into_active_model())
                .exec(&txn)
                .await
                .map_err(StorageError::from)?;
            position::apply_exit(&txn, &intent_id, lot_write.position_exit).await?;
            complete_exit_capital(&txn, &intent_id, "resolution redeem".to_owned()).await?;
            mark_intent_redeemed(&txn, &intent_id).await?;
        }

        txn.commit().await.map_err(StorageError::from)?;
        Ok(confirmed.into())
    }
}

#[derive(Debug, FromQueryResult)]
struct LotCountRow {
    settlement_redeem_id: SettlementRedeemId,
    lot_count: i64,
}

/// Count lots per redeem batch for the given ids in a single grouped query.
async fn lot_counts_for(
    db: &DatabaseConnection,
    ids: impl Iterator<Item = SettlementRedeemId>,
) -> Result<HashMap<SettlementRedeemId, i64>, StorageError> {
    let ids: Vec<SettlementRedeemId> = ids.collect();
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = QuantSettlementRedeemLotEntity::find()
        .select_only()
        .column(QuantSettlementRedeemLotColumn::SettlementRedeemId)
        .column_as(
            Expr::col(QuantSettlementRedeemLotColumn::SettlementRedeemLotId).count(),
            "lot_count",
        )
        .filter(QuantSettlementRedeemLotColumn::SettlementRedeemId.is_in(ids))
        .group_by(QuantSettlementRedeemLotColumn::SettlementRedeemId)
        .into_model::<LotCountRow>()
        .all(db)
        .await
        .map_err(StorageError::from)?;
    Ok(rows
        .into_iter()
        .map(|row| (row.settlement_redeem_id, row.lot_count))
        .collect())
}

async fn load_redeem(
    db: &DatabaseConnection,
    settlement_redeem_id: &SettlementRedeemId,
) -> Result<Model, StorageError> {
    Entity::find_by_id(settlement_redeem_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(QUANT_SETTLEMENT_REDEEM, settlement_redeem_id))
}

async fn mark_intent_redeemed(
    db: &impl ConnectionTrait,
    intent_id: &OrderIntentId,
) -> Result<(), StorageError> {
    let intent = QuantOrderIntentEntity::find_by_id(intent_id.clone())
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(QUANT_ORDER_INTENT, intent_id))?;
    let mut active = intent.into_active_model();
    active.exit_state = ActiveValue::Set(ExitState::Exited);
    active.exit_reason = ActiveValue::Set(Some(ExitReason::ResolutionRedeem));
    let mut scale_out_state = active.scale_out_state.take().unwrap_or_default();
    scale_out_state.pending_target = None;
    active.scale_out_state = ActiveValue::Set(scale_out_state);
    active
        .update(db)
        .await
        .map_err(StorageError::from)
        .map(|_| ())
}

fn page_condition(query: &SettlementRedeemListQuery) -> Condition {
    Condition::all()
        .add_option(query.state.map(|state| Column::State.eq(state)))
        .add_option(
            query
                .market_id
                .clone()
                .map(|market_id| Column::MarketId.eq(market_id)),
        )
        .add_option(query.from.map(|from| Column::CreatedAt.gte(from)))
        .add_option(query.to.map(|to| Column::CreatedAt.lte(to)))
}
