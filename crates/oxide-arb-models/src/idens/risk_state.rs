use sea_orm::DeriveIden;

#[derive(DeriveIden)]
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
    HourlyWindowStart,
    DailyLossUsd,
    DailyFeeUsd,
    DailyPnl,
    DailyWindowStart,
    WeeklyLossUsd,
    WeeklyWindowStart,
    LastEmergencyAt,
    LastEmergencyReason,
    UpdatedAt,
}
