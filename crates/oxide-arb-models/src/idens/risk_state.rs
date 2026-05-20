use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum RiskEngineState {
    Table,
    Id,
    BreakerState,
    BreakerLevel,
    BreakerReason,
    CoolingUntil,
    TotalExposure,
    DailyPnl,
    ConsecutiveLosses,
    UpdatedAt,
}
