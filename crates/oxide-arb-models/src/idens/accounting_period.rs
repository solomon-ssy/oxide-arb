use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum AccountingPeriod {
    Table,
    PeriodId,
    PeriodType,
    StartDate,
    EndDate,
    RealizedPnl,
    TotalFees,
    TradeCount,
    WinCount,
    LossCount,
    MissCount,
    MaxDrawdown,
    SharpeRatio,
    Finalized,
    CreatedAt,
}
