use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum Event {
    Table,
    EventId,
    Title,
    Slug,
    NegRisk,
    CreatedAt,
    UpdatedAt,
}
