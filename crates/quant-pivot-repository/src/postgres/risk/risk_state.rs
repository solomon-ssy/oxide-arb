use crate::traits::RiskStateRepository;
use chrono::Utc;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{RiskStateInfo, UpsertRiskEngineState},
    entities::risk_state::{ActiveModel, Column, Entity},
    types::Usd,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, QueryFilter,
    sea_query::{Expr, OnConflict},
};

pub struct PgRiskStateRepository {
    db: DatabaseConnection,
}

impl PgRiskStateRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgRiskStateRepositoryTxn<'_> {
        PgRiskStateRepositoryTxn { txn }
    }
}

pub struct PgRiskStateRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

async fn do_load(db: &impl ConnectionTrait) -> Result<RiskStateInfo, StorageError> {
    let model = Entity::find_by_id(1)
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "risk_engine_state",
            id: "1".to_string(),
        })?;
    Ok(model.into())
}

pub(crate) async fn do_upsert(
    db: &impl ConnectionTrait,
    state: UpsertRiskEngineState,
) -> Result<(), StorageError> {
    let am: ActiveModel = state.into_active_model();
    Entity::insert(am)
        .on_conflict(
            OnConflict::column(Column::Id)
                .update_columns([
                    Column::BreakerState,
                    Column::BreakerLevel,
                    Column::IsHalted,
                    Column::HaltReason,
                    Column::ConsecutiveMisses,
                    Column::CooldownUntil,
                    Column::CooldownMultiplier,
                    Column::TotalExposure,
                    Column::HourlyLossUsd,
                    Column::HourlyFeeUsd,
                    Column::HourlyTradeCount,
                    Column::HourlySuccessCount,
                    Column::HourlyMissCount,
                    Column::HourlyWindowStart,
                    Column::DailyLossUsd,
                    Column::DailyFeeUsd,
                    Column::DailyPnl,
                    Column::DailyBudgetSpent,
                    Column::DailyTradeCount,
                    Column::DailySuccessCount,
                    Column::DailyMissCount,
                    Column::DailyWindowStart,
                    Column::WeeklyLossUsd,
                    Column::WeeklyTradeCount,
                    Column::WeeklyWindowStart,
                    Column::HwmEquity,
                    Column::LastEmergencyAt,
                    Column::LastEmergencyReason,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

async fn do_reset_hourly_window(db: &impl ConnectionTrait) -> Result<(), StorageError> {
    Entity::update_many()
        .col_expr(Column::HourlyLossUsd, Expr::value(Usd::ZERO))
        .col_expr(Column::HourlyFeeUsd, Expr::value(Usd::ZERO))
        .col_expr(Column::HourlyWindowStart, Expr::value(Utc::now()))
        .filter(Column::Id.eq(1))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

async fn do_reset_daily_window(db: &impl ConnectionTrait) -> Result<(), StorageError> {
    Entity::update_many()
        .col_expr(Column::DailyLossUsd, Expr::value(Usd::ZERO))
        .col_expr(Column::DailyFeeUsd, Expr::value(Usd::ZERO))
        .col_expr(Column::DailyPnl, Expr::value(Usd::ZERO))
        .col_expr(
            Column::DailyWindowStart,
            Expr::value(Utc::now().date_naive()),
        )
        .filter(Column::Id.eq(1))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

async fn do_reset_weekly_window(db: &impl ConnectionTrait) -> Result<(), StorageError> {
    Entity::update_many()
        .col_expr(Column::WeeklyLossUsd, Expr::value(Usd::ZERO))
        .col_expr(
            Column::WeeklyWindowStart,
            Expr::value(Utc::now().date_naive()),
        )
        .filter(Column::Id.eq(1))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

#[async_trait::async_trait]
impl RiskStateRepository for PgRiskStateRepository {
    async fn load(&self) -> Result<RiskStateInfo, StorageError> {
        do_load(&self.db).await
    }

    async fn upsert(&self, state: UpsertRiskEngineState) -> Result<(), StorageError> {
        do_upsert(&self.db, state).await
    }

    async fn reset_hourly_window(&self) -> Result<(), StorageError> {
        do_reset_hourly_window(&self.db).await
    }

    async fn reset_daily_window(&self) -> Result<(), StorageError> {
        do_reset_daily_window(&self.db).await
    }

    async fn reset_weekly_window(&self) -> Result<(), StorageError> {
        do_reset_weekly_window(&self.db).await
    }
}

#[async_trait::async_trait]
impl RiskStateRepository for PgRiskStateRepositoryTxn<'_> {
    async fn load(&self) -> Result<RiskStateInfo, StorageError> {
        do_load(self.txn).await
    }

    async fn upsert(&self, state: UpsertRiskEngineState) -> Result<(), StorageError> {
        do_upsert(self.txn, state).await
    }

    async fn reset_hourly_window(&self) -> Result<(), StorageError> {
        do_reset_hourly_window(self.txn).await
    }

    async fn reset_daily_window(&self) -> Result<(), StorageError> {
        do_reset_daily_window(self.txn).await
    }

    async fn reset_weekly_window(&self) -> Result<(), StorageError> {
        do_reset_weekly_window(self.txn).await
    }
}
