use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::{
    enums::quant::TradePolicyStatus,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantTradePolicyArtifact {
    Table,
    ArtifactId,
    ContentHash,
    Status,
    SourceDatasetId,
    PayloadJson,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantTradePolicyArtifact::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantTradePolicyArtifact::ArtifactId))
        .col(
            ColumnDef::new(QuantTradePolicyArtifact::ContentHash)
                .text()
                .not_null(),
        )
        .col(column::pg_enum::<TradePolicyStatus>(
            QuantTradePolicyArtifact::Status,
        ))
        .col(column::uuid_fk(QuantTradePolicyArtifact::SourceDatasetId))
        .col(
            ColumnDef::new(QuantTradePolicyArtifact::PayloadJson)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantTradePolicyArtifact::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantTradePolicyArtifact::UpdatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_trade_policy_artifact_hash",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_trade_policy_artifact_hash")
                .table(QuantTradePolicyArtifact::Table)
                .col(QuantTradePolicyArtifact::ContentHash)
                .unique()
                .to_owned(),
            "one row per immutable trade-policy payload",
        ),
        IndexSpec::sea_query(
            "idx_quant_trade_policy_status_created",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_trade_policy_status_created")
                .table(QuantTradePolicyArtifact::Table)
                .col(QuantTradePolicyArtifact::Status)
                .col((QuantTradePolicyArtifact::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "trade policies by governance state and recency",
        ),
    ]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantTradePolicyArtifact::Table.to_string()
}
