use crate::traits::RiskStateRepository;
use chrono::Utc;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::risk_state::{self, ActiveModel, Column, Entity};
use sea_orm::sea_query::{Expr, OnConflict};
#[allow(clippy::wildcard_imports)]
use sea_orm::*;

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

async fn do_load(db: &impl ConnectionTrait) -> Result<risk_state::Model, StorageError> {
    Entity::find_by_id(1)
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "risk_engine_state",
            id: "1".to_string(),
        })
}

async fn do_save(db: &impl ConnectionTrait, state: ActiveModel) -> Result<(), StorageError> {
    let state = state.prepare_for_insert();
    Entity::insert(state)
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
                    Column::HourlyWindowStart,
                    Column::DailyLossUsd,
                    Column::DailyFeeUsd,
                    Column::DailyPnl,
                    Column::DailyWindowStart,
                    Column::WeeklyLossUsd,
                    Column::WeeklyWindowStart,
                    Column::LastEmergencyAt,
                    Column::LastEmergencyReason,
                    Column::UpdatedAt,
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
        .col_expr(Column::HourlyLossUsd, Expr::value("0"))
        .col_expr(Column::HourlyFeeUsd, Expr::value("0"))
        .col_expr(Column::HourlyWindowStart, Expr::value(Utc::now()))
        .filter(Column::Id.eq(1))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

async fn do_reset_daily_window(db: &impl ConnectionTrait) -> Result<(), StorageError> {
    Entity::update_many()
        .col_expr(Column::DailyLossUsd, Expr::value("0"))
        .col_expr(Column::DailyFeeUsd, Expr::value("0"))
        .col_expr(Column::DailyPnl, Expr::value("0"))
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
        .col_expr(Column::WeeklyLossUsd, Expr::value("0"))
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

impl RiskStateRepository for PgRiskStateRepository {
    async fn load(&self) -> Result<risk_state::Model, StorageError> {
        do_load(&self.db).await
    }

    async fn save(&self, state: ActiveModel) -> Result<(), StorageError> {
        do_save(&self.db, state).await
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

impl RiskStateRepository for PgRiskStateRepositoryTxn<'_> {
    async fn load(&self) -> Result<risk_state::Model, StorageError> {
        do_load(self.txn).await
    }

    async fn save(&self, state: ActiveModel) -> Result<(), StorageError> {
        do_save(self.txn, state).await
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
