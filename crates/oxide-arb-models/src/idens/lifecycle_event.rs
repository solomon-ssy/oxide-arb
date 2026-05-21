use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum LifecycleEvent {
    Table,
    Id,
    Phase,
    Stage,
    Message,
    Metadata,
    CreatedAt,
}
