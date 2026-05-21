use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum PotentialLossLedger {
    Table,
    LedgerId,
    MarketId,
    TokenId,
    Shares,
    EntryPrice,
    MaxLossUsd,
    Status,
    CreatedAt,
    ResolvedAt,
}
