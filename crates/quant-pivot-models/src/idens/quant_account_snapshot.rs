use quant_pivot_macros::quant_schema;
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

#[quant_schema(lifecycle = "report")]
pub enum QuantAccountSnapshot {
    Table,
    AccountSnapshotId,
    AsOf,
    Source,
    EquityUsd,
    AvailableUsd,
    ReservedUsd,
    PositionsJson,
    ExposuresJson,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantAccountSnapshot::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantAccountSnapshot::AccountSnapshotId))
        .col(
            ColumnDef::new(QuantAccountSnapshot::AsOf)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantAccountSnapshot::Source)
                .text()
                .not_null(),
        )
        .col(column::usd(QuantAccountSnapshot::EquityUsd))
        .col(column::usd(QuantAccountSnapshot::AvailableUsd))
        .col(column::usd(QuantAccountSnapshot::ReservedUsd))
        .col(
            ColumnDef::new(QuantAccountSnapshot::PositionsJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantAccountSnapshot::ExposuresJson)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantAccountSnapshot::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_account_snapshot_as_of",
        quant_account_snapshot_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_account_snapshot_as_of")
            .table(QuantAccountSnapshot::Table)
            .col((QuantAccountSnapshot::AsOf, IndexOrder::Desc))
            .to_owned(),
        "account snapshots by PIT timestamp",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_account_snapshot_table_name() -> String {
    QuantAccountSnapshot::Table.to_string()
}
