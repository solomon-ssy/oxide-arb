use oxide_arb_macros::oxide_schema;
use sea_orm::sea_query::{ColumnDef, Table, TableCreateStatement};

use crate::schema::{column, dependency::TableDependency, index::IndexSpec, seed::SeedSpec};

#[oxide_schema(lifecycle = "audit")]
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

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(EmergencySnapshot::Table)
        .if_not_exists()
        .col(column::bigserial_pk(EmergencySnapshot::Id))
        .col(
            ColumnDef::new(EmergencySnapshot::TriggerLevel)
                .text()
                .not_null(),
        )
        .col(ColumnDef::new(EmergencySnapshot::Reason).text().not_null())
        .col(
            ColumnDef::new(EmergencySnapshot::RiskState)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(EmergencySnapshot::OpenPositionsCount)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(EmergencySnapshot::OpenReservationsCount)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(EmergencySnapshot::TriggeredAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .to_owned()
}

pub const fn indexes() -> Vec<IndexSpec> {
    Vec::new()
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}
