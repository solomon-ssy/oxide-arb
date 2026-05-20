use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum Market {
    Table,
    ConditionId,
    EventId,
    Question,
    Slug,
    Category,
    NegRisk,
    Active,
    CreatedAt,
    UpdatedAt,
}
