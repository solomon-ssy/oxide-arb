use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum EmergencySnapshot {
    Table,
    Id,
    TriggerLevel,
    Reason,
    RiskState,
    OpenPositionsCount,
    OpenReservationsCount,
    TriggeredAt,
}
