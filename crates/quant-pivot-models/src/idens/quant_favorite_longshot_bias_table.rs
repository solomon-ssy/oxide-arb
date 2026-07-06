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

// Append-only, content-addressed favorite-longshot bias-table artifact (Phase
// 11.2.1). One row per fitted `(category, price_bucket) → empirical-bias` table.
// The per-category curves (bins, IC, sample counts) live in a single JSONB
// payload; scalar fit provenance (window, split hash, sample totals) is typed so
// the governance catalog can filter without deserializing the payload. Immutable
// analytical output ⇒ `report` lifecycle (mirrors `quant_backtest_report`).
#[quant_schema(lifecycle = "report")]
pub enum QuantFavoriteLongshotBiasTable {
    Table,
    BiasTableId,
    ContentHash,
    FitWindowStart,
    FitWindowEnd,
    CalibrationSplitHash,
    CategoryCount,
    TotalSampleCount,
    ByCategory,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantFavoriteLongshotBiasTable::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantFavoriteLongshotBiasTable::BiasTableId))
        .col(
            ColumnDef::new(QuantFavoriteLongshotBiasTable::ContentHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFavoriteLongshotBiasTable::FitWindowStart)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFavoriteLongshotBiasTable::FitWindowEnd)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFavoriteLongshotBiasTable::CalibrationSplitHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFavoriteLongshotBiasTable::CategoryCount)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFavoriteLongshotBiasTable::TotalSampleCount)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFavoriteLongshotBiasTable::ByCategory)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantFavoriteLongshotBiasTable::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_favorite_longshot_bias_table_hash",
            quant_favorite_longshot_bias_table_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_favorite_longshot_bias_table_hash")
                .table(QuantFavoriteLongshotBiasTable::Table)
                .col(QuantFavoriteLongshotBiasTable::ContentHash)
                .unique()
                .to_owned(),
            "one row per content-addressed bias-table hash",
        ),
        IndexSpec::sea_query(
            "idx_quant_favorite_longshot_bias_table_created",
            quant_favorite_longshot_bias_table_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_favorite_longshot_bias_table_created")
                .table(QuantFavoriteLongshotBiasTable::Table)
                .col((QuantFavoriteLongshotBiasTable::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "bias tables by recency",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_favorite_longshot_bias_table_table_name() -> String {
    QuantFavoriteLongshotBiasTable::Table.to_string()
}
