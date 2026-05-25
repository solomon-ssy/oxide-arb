use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum BlacklistEntry {
    Table,
    MarketId,
    TokenId,
    Scope,
    Reason,
    ExpiresAt,
    MissCount,
    CreatedAt,
    UpdatedAt,
}
