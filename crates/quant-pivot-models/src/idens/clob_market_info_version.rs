use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    enums::common::TickSize,
    idens::market::Market,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum ClobMarketInfoVersion {
    Table,
    VersionId,
    MarketId,
    TokensJson,
    TickSize,
    MinimumOrderSize,
    NegRisk,
    TakerOrderDelayEnabled,
    MinimumOrderAgeSecs,
    BlockaidCheckEnabled,
    FeeDetailsJson,
    BuilderMakerFeeRateBps,
    BuilderTakerFeeRateBps,
    EffectiveAt,
    AvailableAt,
    PayloadHash,
    RawPayload,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ClobMarketInfoVersion::Table)
        .if_not_exists()
        .col(column::uuid_pk(ClobMarketInfoVersion::VersionId))
        .col(column::market_id(ClobMarketInfoVersion::MarketId))
        .col(
            ColumnDef::new(ClobMarketInfoVersion::TokensJson)
                .json_binary()
                .not_null(),
        )
        .col(column::pg_enum::<TickSize>(ClobMarketInfoVersion::TickSize))
        .col(
            ColumnDef::new(ClobMarketInfoVersion::MinimumOrderSize)
                .decimal_len(38, 18)
                .not_null(),
        )
        .col(
            ColumnDef::new(ClobMarketInfoVersion::NegRisk)
                .boolean()
                .not_null(),
        )
        .col(
            ColumnDef::new(ClobMarketInfoVersion::TakerOrderDelayEnabled)
                .boolean()
                .not_null(),
        )
        .col(
            ColumnDef::new(ClobMarketInfoVersion::MinimumOrderAgeSecs)
                .big_integer()
                .null(),
        )
        .col(
            ColumnDef::new(ClobMarketInfoVersion::BlockaidCheckEnabled)
                .boolean()
                .not_null(),
        )
        .col(
            ColumnDef::new(ClobMarketInfoVersion::FeeDetailsJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ClobMarketInfoVersion::BuilderMakerFeeRateBps)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(ClobMarketInfoVersion::BuilderTakerFeeRateBps)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(ClobMarketInfoVersion::EffectiveAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(ClobMarketInfoVersion::AvailableAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(ClobMarketInfoVersion::PayloadHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ClobMarketInfoVersion::RawPayload)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            ClobMarketInfoVersion::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_clob_market_info_version_market")
                .from(
                    ClobMarketInfoVersion::Table,
                    ClobMarketInfoVersion::MarketId,
                )
                .to(Market::Table, Market::MarketId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_clob_market_info_version_pit",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_clob_market_info_version_pit")
                .table(ClobMarketInfoVersion::Table)
                .col(ClobMarketInfoVersion::MarketId)
                .col((ClobMarketInfoVersion::EffectiveAt, IndexOrder::Desc))
                .col((ClobMarketInfoVersion::AvailableAt, IndexOrder::Desc))
                .to_owned(),
            "point-in-time CLOB market-info resolution",
        ),
        IndexSpec::sea_query(
            "uq_clob_market_info_version_payload",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_clob_market_info_version_payload")
                .table(ClobMarketInfoVersion::Table)
                .col(ClobMarketInfoVersion::MarketId)
                .col(ClobMarketInfoVersion::PayloadHash)
                .unique()
                .to_owned(),
            "content-addressed market-info observations",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(market_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    ClobMarketInfoVersion::Table.to_string()
}

fn market_table_name() -> String {
    Market::Table.to_string()
}
