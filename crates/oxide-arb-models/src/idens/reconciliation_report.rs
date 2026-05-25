use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum ReconciliationReport {
    Table,
    Id,
    Status,
    Mismatches,
    InternalBalance,
    ExternalBalance,
    InternalExposure,
    ExternalExposure,
    Reserved,
    Tolerance,
    CheckedAt,
    DurationMs,
}
