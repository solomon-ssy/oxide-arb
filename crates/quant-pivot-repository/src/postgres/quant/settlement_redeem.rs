//! Postgres-backed settlement redemption ledger repository.

use crate::{
    postgres::{
        error,
        quant::{capital_allocation::complete_exit_capital, position},
        query::paginate_mapped,
    },
    traits::SettlementRedeemRepository,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        ConfirmSettlementRedeem, NewSettlementRedeem, Paginated, SettlementRedeemInfo,
        SettlementRedeemListQuery, SettlementRedeemLotInfo,
    },
    entities::{quant_order_intent, quant_settlement_redeem, quant_settlement_redeem_lot},
    enums::execution::{ExitReason, ExitState, SettlementRedeemState},
    types::{MarketId, SettlementRedeemId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
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
        quant_settlement_redeem::Entity::find_by_id(settlement_redeem_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: SettlementRedeemListQuery,
    ) -> Result<Paginated<SettlementRedeemInfo>, StorageError> {
        let query = query.normalized();
        paginate_mapped(
            quant_settlement_redeem::Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(quant_settlement_redeem::Column::CreatedAt),
            &self.db,
            &query.page,
            Into::into,
        )
        .await
    }

    async fn list_lots_by_redeem(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
    ) -> Result<Vec<SettlementRedeemLotInfo>, StorageError> {
        quant_settlement_redeem_lot::Entity::find()
            .filter(
                quant_settlement_redeem_lot::Column::SettlementRedeemId
                    .eq(settlement_redeem_id.clone()),
            )
            .order_by_asc(quant_settlement_redeem_lot::Column::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_by_market_funder(
        &self,
        market_id: &MarketId,
        funder_address: &str,
    ) -> Result<Option<SettlementRedeemInfo>, StorageError> {
        quant_settlement_redeem::Entity::find()
            .filter(quant_settlement_redeem::Column::MarketId.eq(market_id.clone()))
            .filter(quant_settlement_redeem::Column::FunderAddress.eq(funder_address))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn upsert_pending(
        &self,
        redeem: NewSettlementRedeem,
    ) -> Result<SettlementRedeemInfo, StorageError> {
        if let Some(row) = quant_settlement_redeem::Entity::find()
            .filter(quant_settlement_redeem::Column::MarketId.eq(redeem.market_id.clone()))
            .filter(
                quant_settlement_redeem::Column::FunderAddress.eq(redeem.funder_address.clone()),
            )
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

        quant_settlement_redeem::Entity::insert(redeem.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn mark_submitted(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        tx_hash: String,
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
                Some(entity::QUANT_SETTLEMENT_REDEEM),
                "confirmed settlement redeem must close at least one lot",
            ));
        }

        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let redeem =
            quant_settlement_redeem::Entity::find_by_id(write.settlement_redeem_id.clone())
                .lock_exclusive()
                .one(&txn)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| {
                    error::not_found(entity::QUANT_SETTLEMENT_REDEEM, &write.settlement_redeem_id)
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
            quant_settlement_redeem_lot::Entity::insert(lot_write.lot.into_active_model())
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

async fn load_redeem(
    db: &DatabaseConnection,
    settlement_redeem_id: &SettlementRedeemId,
) -> Result<quant_settlement_redeem::Model, StorageError> {
    quant_settlement_redeem::Entity::find_by_id(settlement_redeem_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(entity::QUANT_SETTLEMENT_REDEEM, settlement_redeem_id))
}

async fn mark_intent_redeemed(
    db: &impl sea_orm::ConnectionTrait,
    intent_id: &quant_pivot_models::types::OrderIntentId,
) -> Result<(), StorageError> {
    let intent = quant_order_intent::Entity::find_by_id(intent_id.clone())
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(entity::QUANT_ORDER_INTENT, intent_id))?;
    let mut active = intent.into_active_model();
    active.exit_state = ActiveValue::Set(ExitState::Exited);
    active.exit_reason = ActiveValue::Set(Some(ExitReason::ResolutionRedeem));
    active.pending_partial_exit_node_id = ActiveValue::Set(None);
    active
        .update(db)
        .await
        .map_err(StorageError::from)
        .map(|_| ())
}

fn page_condition(query: &SettlementRedeemListQuery) -> Condition {
    Condition::all()
        .add_option(
            query
                .state
                .map(|state| quant_settlement_redeem::Column::State.eq(state)),
        )
        .add_option(
            query
                .market_id
                .clone()
                .map(|market_id| quant_settlement_redeem::Column::MarketId.eq(market_id)),
        )
        .add_option(
            query
                .from
                .map(|from| quant_settlement_redeem::Column::CreatedAt.gte(from)),
        )
        .add_option(
            query
                .to
                .map(|to| quant_settlement_redeem::Column::CreatedAt.lte(to)),
        )
}
