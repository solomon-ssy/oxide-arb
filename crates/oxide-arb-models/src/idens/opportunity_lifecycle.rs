use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum OpportunityLifecycleEvent {
    Table,
    EventId,
    OpportunityId,
    ExecutionId,
    Phase,
    PhaseData,
    CreatedAt,
}
