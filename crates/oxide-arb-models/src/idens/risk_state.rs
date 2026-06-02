use crate::{
    enums::risk::BreakerStateName,
    schema::{
        dependency::TableDependency, index::IndexSpec, seed::SeedSpec, timestamp_with_write_default,
    },
    seed::risk_engine_state,
    types::Usd,
};
use oxide_arb_macros::oxide_schema;
use sea_orm::sea_query::{ColumnDef, Expr, Table, TableCreateStatement};

#[oxide_schema(lifecycle = "core")]
pub enum RiskEngineState {
    Table,
    Id,
    BreakerState,
    BreakerLevel,
    IsHalted,
    HaltReason,
    ConsecutiveMisses,
    CooldownUntil,
    CooldownMultiplier,
    TotalExposure,
    HourlyLossUsd,
    HourlyFeeUsd,
    HourlyTradeCount,
    HourlySuccessCount,
    HourlyMissCount,
    HourlyWindowStart,
    DailyLossUsd,
    DailyFeeUsd,
    DailyPnl,
    DailyBudgetSpent,
    DailyTradeCount,
    DailySuccessCount,
    DailyMissCount,
    DailyWindowStart,
    WeeklyLossUsd,
    WeeklyTradeCount,
    WeeklyWindowStart,
    HwmEquity,
    LastEmergencyAt,
    LastEmergencyReason,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
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

pub const fn indexes() -> Vec<IndexSpec> {
    Vec::new()
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub fn seed_units() -> Vec<SeedSpec> {
    vec![risk_engine_state::RISK_ENGINE_STATE_SEED]
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
        .col(default_zero_usd(RiskEngineState::HourlyLossUsd))
        .col(default_zero_usd(RiskEngineState::HourlyFeeUsd))
        .col(default_zero_i32(RiskEngineState::HourlyTradeCount))
        .col(default_zero_i32(RiskEngineState::HourlySuccessCount))
        .col(default_zero_i32(RiskEngineState::HourlyMissCount))
        .col(timestamp_with_write_default(
            RiskEngineState::HourlyWindowStart,
        ));
}

fn risk_engine_daily_window_columns(table: &mut TableCreateStatement) {
    table
        .col(default_zero_usd(RiskEngineState::DailyLossUsd))
        .col(default_zero_usd(RiskEngineState::DailyFeeUsd))
        .col(default_zero_usd(RiskEngineState::DailyPnl))
        .col(default_zero_usd(RiskEngineState::DailyBudgetSpent))
        .col(default_zero_i32(RiskEngineState::DailyTradeCount))
        .col(default_zero_i32(RiskEngineState::DailySuccessCount))
        .col(default_zero_i32(RiskEngineState::DailyMissCount))
        .col(
            ColumnDef::new(RiskEngineState::DailyWindowStart)
                .date()
                .not_null()
                .default(Expr::current_date()),
        );
}

fn risk_engine_weekly_window_columns(table: &mut TableCreateStatement) {
    table
        .col(default_zero_usd(RiskEngineState::WeeklyLossUsd))
        .col(default_zero_i32(RiskEngineState::WeeklyTradeCount))
        .col(
            ColumnDef::new(RiskEngineState::WeeklyWindowStart)
                .date()
                .not_null()
                .default(Expr::current_date()),
        )
        .col(default_zero_usd(RiskEngineState::HwmEquity));
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
        .col(timestamp_with_write_default(RiskEngineState::UpdatedAt));
}

fn default_zero_usd(column: RiskEngineState) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.text().not_null().default(Usd::ZERO);
    col
}

fn default_zero_i32(column: RiskEngineState) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.integer().not_null().default(0);
    col
}
