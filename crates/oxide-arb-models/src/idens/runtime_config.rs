use sea_orm::DeriveIden;

#[derive(DeriveIden)]
pub enum RuntimeConfig {
    Table,
    Key,
    Value,
    UpdatedAt,
}
