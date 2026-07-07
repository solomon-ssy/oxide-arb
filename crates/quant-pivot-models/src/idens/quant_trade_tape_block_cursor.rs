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

#[quant_schema(lifecycle = "runtime")]
pub enum QuantTradeTapeBlockCursor {
    Table,
    Source,
    ContractAddress,
    LastFinalizedBlock,
    LastLogIndex,
    HeadLagBlocks,
    Status,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantTradeTapeBlockCursor::Table)
        .if_not_exists()
        .col(column::text_id(QuantTradeTapeBlockCursor::Source))
        .col(column::text_id(QuantTradeTapeBlockCursor::ContractAddress))
        .col(
            ColumnDef::new(QuantTradeTapeBlockCursor::LastFinalizedBlock)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(QuantTradeTapeBlockCursor::LastLogIndex)
                .integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(QuantTradeTapeBlockCursor::HeadLagBlocks)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(column::text_id(QuantTradeTapeBlockCursor::Status))
        .col(timestamp_with_write_default(
            QuantTradeTapeBlockCursor::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantTradeTapeBlockCursor::UpdatedAt,
        ))
        .primary_key(
            Index::create()
                .col(QuantTradeTapeBlockCursor::Source)
                .col(QuantTradeTapeBlockCursor::ContractAddress),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_trade_tape_block_cursor_status_lag",
        quant_trade_tape_block_cursor_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_trade_tape_block_cursor_status_lag")
            .table(QuantTradeTapeBlockCursor::Table)
            .col(QuantTradeTapeBlockCursor::Status)
            .col((QuantTradeTapeBlockCursor::HeadLagBlocks, IndexOrder::Desc))
            .to_owned(),
        "on-chain trade-tape block cursor health by status and lag",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_trade_tape_block_cursor_table_name() -> String {
    QuantTradeTapeBlockCursor::Table.to_string()
}
