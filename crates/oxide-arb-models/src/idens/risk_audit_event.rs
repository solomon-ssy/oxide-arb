use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum RiskAuditEvent {
    Table,
    Id,
    EventType,
    OpportunityId,
    TradeId,
    Payload,
    CreatedAt,
}
