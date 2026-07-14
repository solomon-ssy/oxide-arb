use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    enums::domain::LinkageSourceRole,
    idens::quant_market_linkage::QuantMarketLinkage,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantMarketLinkageSource {
    Table,
    LinkageId,
    Role,
    SourceId,
    InstrumentKey,
    BindingHash,
    AvailableAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantMarketLinkageSource::Table)
        .if_not_exists()
        .col(column::uuid_fk(QuantMarketLinkageSource::LinkageId))
        .col(column::pg_enum::<LinkageSourceRole>(
            QuantMarketLinkageSource::Role,
        ))
        .col(
            ColumnDef::new(QuantMarketLinkageSource::SourceId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantMarketLinkageSource::InstrumentKey)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantMarketLinkageSource::BindingHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantMarketLinkageSource::AvailableAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantMarketLinkageSource::CreatedAt,
        ))
        .primary_key(
            Index::create()
                .col(QuantMarketLinkageSource::LinkageId)
                .col(QuantMarketLinkageSource::Role)
                .col(QuantMarketLinkageSource::SourceId)
                .col(QuantMarketLinkageSource::InstrumentKey),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_market_linkage_source_linkage")
                .from(
                    QuantMarketLinkageSource::Table,
                    QuantMarketLinkageSource::LinkageId,
                )
                .to(QuantMarketLinkage::Table, QuantMarketLinkage::LinkageId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_market_linkage_source_discovery",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_market_linkage_source_discovery")
            .table(QuantMarketLinkageSource::Table)
            .col(QuantMarketLinkageSource::SourceId)
            .col(QuantMarketLinkageSource::InstrumentKey)
            .col(QuantMarketLinkageSource::Role)
            .to_owned(),
        "dynamic ingest discovery by typed source role",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(parent_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantMarketLinkageSource::Table.to_string()
}

fn parent_table_name() -> String {
    QuantMarketLinkage::Table.to_string()
}
