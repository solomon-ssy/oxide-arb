use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum ResolutionEvent {
    Table,
    ResolutionId,
    MarketId,
    Outcome,
    Source,
    GammaAgrees,
    CtfAgrees,
    Evidence,
    ResolvedAt,
    CreatedAt,
}
