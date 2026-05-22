use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum Report {
    Table,
    Id,
    ReportType,
    PeriodStart,
    PeriodEnd,
    Payload,
    CreatedAt,
}
