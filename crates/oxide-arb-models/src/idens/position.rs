use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum Position {
    Table,
    MarketId,
    TokenId,
    Side,
    Size,
    AvgEntryPrice,
    CostBasis,
    UpdatedAt,
}
