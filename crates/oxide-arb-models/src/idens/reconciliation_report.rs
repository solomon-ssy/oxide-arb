use oxide_arb_macros::oxide_schema;
use sea_orm::sea_query::{ColumnDef, Table, TableCreateStatement};

use crate::schema::{dependency::TableDependency, index::IndexSpec, seed::SeedSpec};

#[oxide_schema]
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

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ReconciliationReport::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(ReconciliationReport::Id)
                .big_integer()
                .not_null()
                .auto_increment()
                .primary_key(),
        )
        .col(
            ColumnDef::new(ReconciliationReport::Status)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ReconciliationReport::Mismatches)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ReconciliationReport::InternalBalance)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ReconciliationReport::ExternalBalance)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ReconciliationReport::InternalExposure)
                .text()
                .not_null()
                .default("0"),
        )
        .col(
            ColumnDef::new(ReconciliationReport::ExternalExposure)
                .text()
                .not_null()
                .default("0"),
        )
        .col(
            ColumnDef::new(ReconciliationReport::Reserved)
                .text()
                .not_null()
                .default("0"),
        )
        .col(
            ColumnDef::new(ReconciliationReport::Tolerance)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ReconciliationReport::CheckedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(ReconciliationReport::DurationMs)
                .big_integer()
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
