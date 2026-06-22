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

#[quant_schema(lifecycle = "control")]
pub enum QuantUniverseSnapshot {
    Table,
    UniverseSnapshotId,
    AsOf,
    RuntimeConfigVersionId,
    SelectorHash,
    MarketCount,
    IncludedMarketIds,
    ExcludedMarketIds,
    ExclusionSummary,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantUniverseSnapshot::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantUniverseSnapshot::UniverseSnapshotId))
        .col(
            ColumnDef::new(QuantUniverseSnapshot::AsOf)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(column::uuid_fk(
            QuantUniverseSnapshot::RuntimeConfigVersionId,
        ))
        .col(
            ColumnDef::new(QuantUniverseSnapshot::SelectorHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantUniverseSnapshot::MarketCount)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantUniverseSnapshot::IncludedMarketIds)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantUniverseSnapshot::ExcludedMarketIds)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantUniverseSnapshot::ExclusionSummary)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantUniverseSnapshot::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_universe_snapshot_as_of",
            quant_universe_snapshot_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_universe_snapshot_as_of")
                .table(QuantUniverseSnapshot::Table)
                .col((QuantUniverseSnapshot::AsOf, IndexOrder::Desc))
                .to_owned(),
            "universe snapshots by recency",
        ),
        IndexSpec::sea_query(
            "idx_quant_universe_snapshot_runtime_as_of",
            quant_universe_snapshot_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_universe_snapshot_runtime_as_of")
                .table(QuantUniverseSnapshot::Table)
                .col(QuantUniverseSnapshot::RuntimeConfigVersionId)
                .col((QuantUniverseSnapshot::AsOf, IndexOrder::Desc))
                .to_owned(),
            "universe snapshots by runtime config",
        ),
        IndexSpec::sea_query(
            "idx_quant_universe_snapshot_selector_hash",
            quant_universe_snapshot_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_universe_snapshot_selector_hash")
                .table(QuantUniverseSnapshot::Table)
                .col(QuantUniverseSnapshot::SelectorHash)
                .to_owned(),
            "universe snapshots by selector hash",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_universe_snapshot_table_name() -> String {
    QuantUniverseSnapshot::Table.to_string()
}
