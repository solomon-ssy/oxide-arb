use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum LifecycleEvent {
    Table,
    Id,
    OpportunityId,
    Phase,
    Recorder,
    Detail,
    CreatedAt,
}
