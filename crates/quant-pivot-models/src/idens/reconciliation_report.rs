use quant_pivot_macros::quant_schema;
use sea_orm::sea_query::{ColumnDef, Table, TableCreateStatement};

use crate::schema::{column, dependency::TableDependency, index::IndexSpec, seed::SeedSpec};

#[quant_schema(lifecycle = "report")]
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
        .col(column::bigserial_pk(ReconciliationReport::Id))
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
        .col(column::usd(ReconciliationReport::InternalBalance))
        .col(column::usd(ReconciliationReport::ExternalBalance))
        .col(column::usd_default_zero(
            ReconciliationReport::InternalExposure,
        ))
        .col(column::usd_default_zero(
            ReconciliationReport::ExternalExposure,
        ))
        .col(column::usd_default_zero(ReconciliationReport::Reserved))
        .col(column::usd(ReconciliationReport::Tolerance))
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
