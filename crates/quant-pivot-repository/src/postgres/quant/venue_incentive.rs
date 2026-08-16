//! PostgreSQL-backed append-only venue incentive ledger.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_VENUE_INCENTIVE_EVENT};
use quant_pivot_models::{
    domain::quant::{NewVenueIncentiveEvent, VenueIncentiveReconciliation},
    entities::quant_venue_incentive_event::{Column, Entity},
    enums::fee::{VenueIncentiveKind, VenueIncentiveStage},
    types::{ExecutionAccountId, Usd},
};
use rust_decimal::Decimal;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, ExprTrait, FromQueryResult,
    IntoActiveModel, QueryFilter, TransactionTrait,
    sea_query::{Alias, Expr, Order, Query},
};

use crate::traits::VenueIncentiveRepository;

/// PostgreSQL-backed venue incentive repository.
pub struct PgVenueIncentiveRepository {
    db: DatabaseConnection,
}

#[derive(Debug, FromQueryResult)]
struct CreditSum {
    total: Option<Decimal>,
}

impl PgVenueIncentiveRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub(crate) async fn persist_on(
        db: &impl ConnectionTrait,
        event: NewVenueIncentiveEvent,
    ) -> Result<(), StorageError> {
        let existing = Entity::find()
            .filter(Column::SourceIdentity.eq(event.source_identity.clone()))
            .one(db)
            .await
            .map_err(StorageError::from)?;
        if let Some(existing) = existing {
            let exact_retry = existing.execution_account_id == event.execution_account_id
                && existing.execution_fill_id == event.execution_fill_id
                && existing.market_id == event.market_id
                && existing.kind == event.kind
                && existing.stage == event.stage
                && existing.program_date == event.program_date
                && existing.amount_usd == event.amount_usd
                && existing.source_schedule_hash == event.source_schedule_hash
                && existing.source_partition == event.source_partition
                && existing.transaction_hash == event.transaction_hash
                && existing.evidence_hash == event.evidence_hash;
            if exact_retry {
                return Ok(());
            }
            return Err(StorageError::state_conflict(
                QUANT_VENUE_INCENTIVE_EVENT,
                Some(&event.source_identity),
                "venue incentive identity was replayed with different economics or lineage",
            ));
        }
        Entity::insert(event.into_active_model())
            .exec(db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }

    async fn latest_total(
        &self,
        execution_account_id: &ExecutionAccountId,
        kind: VenueIncentiveKind,
        stage: VenueIncentiveStage,
        as_of: DateTime<Utc>,
    ) -> Result<Usd, StorageError> {
        let latest_alias = Alias::new("latest_incentive_partition");
        let latest = Query::select()
            .distinct_on([Column::SourcePartition])
            .columns([Column::SourcePartition, Column::AmountUsd])
            .from(Entity)
            .and_where(Column::ExecutionAccountId.eq(*execution_account_id))
            .and_where(Column::Kind.eq(kind))
            .and_where(Column::Stage.eq(stage))
            .and_where(Column::AvailableAt.lte(as_of))
            .order_by(Column::SourcePartition, Order::Asc)
            .order_by(Column::AvailableAt, Order::Desc)
            .order_by(Column::CreatedAt, Order::Desc)
            .to_owned();
        let statement = Query::select()
            .expr_as(
                Expr::col((latest_alias.clone(), Column::AmountUsd)).sum(),
                Alias::new("total"),
            )
            .from_subquery(latest, latest_alias)
            .to_owned();
        let row = CreditSum::find_by_statement(self.db.get_database_backend().build(&statement))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(Usd::new(
            row.and_then(|value| value.total).unwrap_or(Decimal::ZERO),
        ))
    }
}

#[async_trait::async_trait]
impl VenueIncentiveRepository for PgVenueIncentiveRepository {
    async fn record(&self, events: Vec<NewVenueIncentiveEvent>) -> Result<(), StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        for event in events {
            Self::persist_on(&txn, event).await?;
        }
        txn.commit().await.map_err(StorageError::from)
    }

    async fn credited_cumulative(
        &self,
        execution_account_id: &ExecutionAccountId,
        as_of: DateTime<Utc>,
    ) -> Result<Usd, StorageError> {
        let maker = self
            .latest_total(
                execution_account_id,
                VenueIncentiveKind::MakerRebate,
                VenueIncentiveStage::WalletCredited,
                as_of,
            )
            .await?;
        let taker = self
            .latest_total(
                execution_account_id,
                VenueIncentiveKind::TakerRebate,
                VenueIncentiveStage::WalletCredited,
                as_of,
            )
            .await?;
        Ok(maker + taker)
    }

    async fn reconciliation_cumulative(
        &self,
        execution_account_id: &ExecutionAccountId,
        as_of: DateTime<Utc>,
    ) -> Result<VenueIncentiveReconciliation, StorageError> {
        let maker_accrual = self
            .latest_total(
                execution_account_id,
                VenueIncentiveKind::MakerRebate,
                VenueIncentiveStage::EstimatedAccrual,
                as_of,
            )
            .await?;
        let maker_award = self
            .latest_total(
                execution_account_id,
                VenueIncentiveKind::MakerRebate,
                VenueIncentiveStage::VenueAwarded,
                as_of,
            )
            .await?;
        let maker_cash = self
            .latest_total(
                execution_account_id,
                VenueIncentiveKind::MakerRebate,
                VenueIncentiveStage::WalletCredited,
                as_of,
            )
            .await?;
        let taker_cash = self
            .latest_total(
                execution_account_id,
                VenueIncentiveKind::TakerRebate,
                VenueIncentiveStage::WalletCredited,
                as_of,
            )
            .await?;
        Ok(VenueIncentiveReconciliation {
            as_of,
            estimated_maker_accrual_usd: maker_accrual,
            venue_awarded_maker_usd: maker_award,
            wallet_credited_maker_usd: maker_cash,
            wallet_credited_taker_usd: taker_cash,
        })
    }
}
