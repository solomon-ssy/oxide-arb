use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum Market {
    Table,
    MarketId,
    EventId,
    Question,
    Slug,
    Category,
    Status,
    Outcome,
    YesTokenId,
    NoTokenId,
    TickSize,
    NegRisk,
    EndDate,
    ResolvedAt,
    CreatedAt,
    UpdatedAt,
}
