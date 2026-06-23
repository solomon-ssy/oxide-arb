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
pub enum QuantMarketSelection {
    Table,
    MarketSelectionId,
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
        .table(QuantMarketSelection::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantMarketSelection::MarketSelectionId))
        .col(
            ColumnDef::new(QuantMarketSelection::AsOf)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(column::uuid_fk(
            QuantMarketSelection::RuntimeConfigVersionId,
        ))
        .col(
            ColumnDef::new(QuantMarketSelection::SelectorHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantMarketSelection::MarketCount)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantMarketSelection::IncludedMarketIds)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantMarketSelection::ExcludedMarketIds)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantMarketSelection::ExclusionSummary)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantMarketSelection::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_market_selection_as_of",
            quant_market_selection_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_market_selection_as_of")
                .table(QuantMarketSelection::Table)
                .col((QuantMarketSelection::AsOf, IndexOrder::Desc))
                .to_owned(),
            "selection snapshots by recency",
        ),
        IndexSpec::sea_query(
            "idx_quant_market_selection_runtime_as_of",
            quant_market_selection_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_market_selection_runtime_as_of")
                .table(QuantMarketSelection::Table)
                .col(QuantMarketSelection::RuntimeConfigVersionId)
                .col((QuantMarketSelection::AsOf, IndexOrder::Desc))
                .to_owned(),
            "selection snapshots by runtime config",
        ),
        IndexSpec::sea_query(
            "idx_quant_market_selection_selector_hash",
            quant_market_selection_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_market_selection_selector_hash")
                .table(QuantMarketSelection::Table)
                .col(QuantMarketSelection::SelectorHash)
                .to_owned(),
            "selection snapshots by selector hash",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_market_selection_table_name() -> String {
    QuantMarketSelection::Table.to_string()
}
