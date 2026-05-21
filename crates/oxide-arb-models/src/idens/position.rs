use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum Position {
    Table,
    PositionId,
    MarketId,
    TokenId,
    Side,
    Shares,
    AvgEntryPrice,
    TotalCostUsd,
    TotalFeesUsd,
    UnrealizedPnl,
    RealizedPnl,
    Status,
    OpenedAt,
    ClosedAt,
    SettledAt,
}
