use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    enums::quant::AccountSource,
    idens::quant_account_snapshot::QuantAccountSnapshot,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantEquitySnapshot {
    Table,
    EquitySnapshotId,
    AsOf,
    Source,
    VenueNetLiquidationUsd,
    CapitalBaseUsd,
    AvailableUsd,
    ReservedUsd,
    RealizedPnlCumulativeUsd,
    UnrealizedPnlUsd,
    HighWaterMarkUsd,
    DrawdownPct,
    AccountSnapshotRef,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantEquitySnapshot::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantEquitySnapshot::EquitySnapshotId))
        .col(
            ColumnDef::new(QuantEquitySnapshot::AsOf)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(column::pg_enum::<AccountSource>(
            QuantEquitySnapshot::Source,
        ))
        .col(column::usd(QuantEquitySnapshot::VenueNetLiquidationUsd))
        .col(column::usd(QuantEquitySnapshot::CapitalBaseUsd))
        .col(column::usd(QuantEquitySnapshot::AvailableUsd))
        .col(column::usd(QuantEquitySnapshot::ReservedUsd))
        .col(column::usd(QuantEquitySnapshot::RealizedPnlCumulativeUsd))
        .col(column::usd(QuantEquitySnapshot::UnrealizedPnlUsd))
        .col(column::usd(QuantEquitySnapshot::HighWaterMarkUsd))
        .col(column::ratio(QuantEquitySnapshot::DrawdownPct))
        .col(column::uuid_null(QuantEquitySnapshot::AccountSnapshotRef))
        .col(timestamp_with_write_default(QuantEquitySnapshot::CreatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_equity_snapshot_account_snapshot")
                .from(
                    QuantEquitySnapshot::Table,
                    QuantEquitySnapshot::AccountSnapshotRef,
                )
                .to(
                    QuantAccountSnapshot::Table,
                    QuantAccountSnapshot::AccountSnapshotId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_equity_snapshot_as_of",
            quant_equity_snapshot_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_equity_snapshot_as_of")
                .table(QuantEquitySnapshot::Table)
                .col((QuantEquitySnapshot::AsOf, IndexOrder::Desc))
                .to_owned(),
            "equity snapshots by PIT timestamp",
        ),
        IndexSpec::sea_query(
            "idx_quant_equity_snapshot_created_at",
            quant_equity_snapshot_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_equity_snapshot_created_at")
                .table(QuantEquitySnapshot::Table)
                .col((QuantEquitySnapshot::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "equity snapshots by creation timestamp",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(
        quant_account_snapshot_table_name,
    )]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_equity_snapshot_table_name() -> String {
    QuantEquitySnapshot::Table.to_string()
}

fn quant_account_snapshot_table_name() -> String {
    QuantAccountSnapshot::Table.to_string()
}
