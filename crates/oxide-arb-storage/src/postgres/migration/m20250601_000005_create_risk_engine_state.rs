use super::migrate_up;
use oxide_arb_models::{
    enums::risk::BreakerStateName, idens::risk_state::RiskEngineState, types::Usd,
};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        migrate_up(
            manager,
            create_tables(),
            create_indexes(),
            specials(manager),
            seeding_data(manager),
        )
        .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::drop_tables(manager, drop_tables()).await
    }
}

fn create_tables() -> Vec<TableCreateStatement> {
    vec![risk_engine_state_table()]
}

fn risk_engine_state_table() -> TableCreateStatement {
    let mut table = Table::create()
        .table(RiskEngineState::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(RiskEngineState::Id)
                .integer()
                .not_null()
                .primary_key(),
        )
        .to_owned();
    risk_engine_breaker_columns(&mut table);
    risk_engine_hourly_window_columns(&mut table);
    risk_engine_daily_window_columns(&mut table);
    risk_engine_weekly_window_columns(&mut table);
    risk_engine_emergency_columns(&mut table);
    table
}

fn risk_engine_breaker_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(RiskEngineState::BreakerState)
                .text()
                .not_null()
                .default(BreakerStateName::Closed),
        )
        .col(ColumnDef::new(RiskEngineState::BreakerLevel).text().null())
        .col(
            ColumnDef::new(RiskEngineState::IsHalted)
                .boolean()
                .not_null()
                .default(false),
        )
        .col(ColumnDef::new(RiskEngineState::HaltReason).text().null())
        .col(
            ColumnDef::new(RiskEngineState::ConsecutiveMisses)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(RiskEngineState::CooldownUntil)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(RiskEngineState::CooldownMultiplier)
                .integer()
                .not_null()
                .default(1),
        )
        .col(
            ColumnDef::new(RiskEngineState::TotalExposure)
                .text()
                .not_null()
                .default(Usd::ZERO),
        );
}

fn risk_engine_hourly_window_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(RiskEngineState::HourlyLossUsd)
                .text()
                .not_null()
                .default(Usd::ZERO),
        )
        .col(
            ColumnDef::new(RiskEngineState::HourlyFeeUsd)
                .text()
                .not_null()
                .default(Usd::ZERO),
        )
        .col(
            ColumnDef::new(RiskEngineState::HourlyTradeCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(RiskEngineState::HourlySuccessCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(RiskEngineState::HourlyMissCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(RiskEngineState::HourlyWindowStart)
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::current_timestamp()),
        );
}

fn risk_engine_daily_window_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(RiskEngineState::DailyLossUsd)
                .text()
                .not_null()
                .default(Usd::ZERO),
        )
        .col(
            ColumnDef::new(RiskEngineState::DailyFeeUsd)
                .text()
                .not_null()
                .default(Usd::ZERO),
        )
        .col(
            ColumnDef::new(RiskEngineState::DailyPnl)
                .text()
                .not_null()
                .default(Usd::ZERO),
        )
        .col(
            ColumnDef::new(RiskEngineState::DailyBudgetSpent)
                .text()
                .not_null()
                .default(Usd::ZERO),
        )
        .col(
            ColumnDef::new(RiskEngineState::DailyTradeCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(RiskEngineState::DailySuccessCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(RiskEngineState::DailyMissCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(RiskEngineState::DailyWindowStart)
                .date()
                .not_null()
                .default(Expr::current_date()),
        );
}

fn risk_engine_weekly_window_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(RiskEngineState::WeeklyLossUsd)
                .text()
                .not_null()
                .default(Usd::ZERO),
        )
        .col(
            ColumnDef::new(RiskEngineState::WeeklyTradeCount)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(RiskEngineState::WeeklyWindowStart)
                .date()
                .not_null()
                .default(Expr::current_date()),
        )
        .col(
            ColumnDef::new(RiskEngineState::HwmEquity)
                .text()
                .not_null()
                .default(Usd::ZERO),
        );
}

fn risk_engine_emergency_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(RiskEngineState::LastEmergencyAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(RiskEngineState::LastEmergencyReason)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(RiskEngineState::UpdatedAt)
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::current_timestamp()),
        );
}

const fn create_indexes() -> Vec<IndexCreateStatement> {
    Vec::new()
}

async fn specials(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

async fn seeding_data(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    super::noop(manager).await
}

fn drop_tables() -> Vec<TableDropStatement> {
    vec![Table::drop().table(RiskEngineState::Table).to_owned()]
}
