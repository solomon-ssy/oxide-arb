use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, Table, TableCreateStatement},
};

use crate::schema::{
    column,
    dependency::TableDependency,
    index::{IndexBuildMode, IndexSpec},
    seed::SeedSpec,
    timestamp_with_write_default,
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantEntryConditionArtifact {
    Table,
    ArtifactId,
    ContentHash,
    SchemaVersion,
    EvaluatorVersion,
    PayloadJson,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantEntryConditionArtifact::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantEntryConditionArtifact::ArtifactId))
        .col(
            ColumnDef::new(QuantEntryConditionArtifact::ContentHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionArtifact::SchemaVersion)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionArtifact::EvaluatorVersion)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantEntryConditionArtifact::PayloadJson)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantEntryConditionArtifact::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "uq_quant_entry_condition_artifact_hash",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("uq_quant_entry_condition_artifact_hash")
            .table(QuantEntryConditionArtifact::Table)
            .col(QuantEntryConditionArtifact::ContentHash)
            .unique()
            .to_owned(),
        "one immutable condition artifact per canonical content hash",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantEntryConditionArtifact::Table.to_string()
}
