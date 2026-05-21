use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum Event {
    Table,
    EventId,
    Title,
    Slug,
    Category,
    Status,
    NegRisk,
    EndDate,
    RawGamma,
    CreatedAt,
    UpdatedAt,
}
