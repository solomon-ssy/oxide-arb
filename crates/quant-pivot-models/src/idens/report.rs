use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::schema::{
    column,
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
    timestamp_with_write_default,
};

#[oxide_schema(lifecycle = "report")]
pub enum Report {
    Table,
    Id,
    ReportType,
    PeriodStart,
    PeriodEnd,
    Payload,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(Report::Table)
        .if_not_exists()
        .col(column::text_id_pk(Report::Id))
        .col(ColumnDef::new(Report::ReportType).text().not_null())
        .col(ColumnDef::new(Report::PeriodStart).date().not_null())
        .col(ColumnDef::new(Report::PeriodEnd).date().not_null())
        .col(ColumnDef::new(Report::Payload).json_binary().not_null())
        .col(timestamp_with_write_default(Report::CreatedAt))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_report_type_period",
        report_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_report_type_period")
            .table(Report::Table)
            .col(Report::ReportType)
            .col((Report::PeriodStart, IndexOrder::Desc))
            .to_owned(),
        "report lookup by type and period",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn report_table_name() -> String {
    Report::Table.to_string()
}
