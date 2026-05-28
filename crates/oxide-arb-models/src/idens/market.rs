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
    FeesEnabled,
    FeeRate,
    FeeExponent,
    FeeTakerOnly,
    FeeRebateRate,
    FeeSource,
    FeeObservedAt,
    CreatedAt,
    UpdatedAt,
}
