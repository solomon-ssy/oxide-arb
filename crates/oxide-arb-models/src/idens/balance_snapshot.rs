use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::schema::{
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
    timestamp_with_write_default,
};

#[oxide_schema(lifecycle = "audit")]
pub enum BalanceSnapshot {
    Table,
    BalanceSnapshotId,
    HolderAddress,
    InternalAvailableUsd,
    InternalReservedUsd,
    ExternalAvailableUsd,
    ExternalLockedUsd,
    DriftUsd,
    Source,
    BlockNumber,
    ReconciliationReportId,
    ObservedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(BalanceSnapshot::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(BalanceSnapshot::BalanceSnapshotId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(BalanceSnapshot::HolderAddress)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(BalanceSnapshot::InternalAvailableUsd)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(BalanceSnapshot::InternalReservedUsd)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(BalanceSnapshot::ExternalAvailableUsd)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(BalanceSnapshot::ExternalLockedUsd)
                .text()
                .not_null(),
        )
        .col(ColumnDef::new(BalanceSnapshot::DriftUsd).text().not_null())
        .col(ColumnDef::new(BalanceSnapshot::Source).text().not_null())
        .col(
            ColumnDef::new(BalanceSnapshot::BlockNumber)
                .big_integer()
                .null(),
        )
        .col(
            ColumnDef::new(BalanceSnapshot::ReconciliationReportId)
                .big_integer()
                .null(),
        )
        .col(
            ColumnDef::new(BalanceSnapshot::ObservedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(timestamp_with_write_default(BalanceSnapshot::CreatedAt))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_balance_snapshot_holder_observed",
        balance_snapshot_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_balance_snapshot_holder_observed")
            .table(BalanceSnapshot::Table)
            .col(BalanceSnapshot::HolderAddress)
            .col((BalanceSnapshot::ObservedAt, IndexOrder::Desc))
            .to_owned(),
        "balance snapshots by holder and observation time",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn balance_snapshot_table_name() -> String {
    BalanceSnapshot::Table.to_string()
}
