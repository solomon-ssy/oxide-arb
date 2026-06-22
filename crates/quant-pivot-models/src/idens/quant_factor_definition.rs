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

#[quant_schema(lifecycle = "control")]
pub enum QuantFactorDefinition {
    Table,
    FactorDefinitionId,
    Name,
    FactorFamily,
    Scope,
    InputSchemaVersion,
    OutputSchemaVersion,
    DefinitionJson,
    Status,
    CreatedBy,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantFactorDefinition::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantFactorDefinition::FactorDefinitionId))
        .col(
            ColumnDef::new(QuantFactorDefinition::Name)
                .text()
                .not_null()
                .unique_key(),
        )
        .col(
            ColumnDef::new(QuantFactorDefinition::FactorFamily)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFactorDefinition::Scope)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFactorDefinition::InputSchemaVersion)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFactorDefinition::OutputSchemaVersion)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFactorDefinition::DefinitionJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFactorDefinition::Status)
                .text()
                .not_null(),
        )
        .col(column::uuid_null(QuantFactorDefinition::CreatedBy))
        .col(timestamp_with_write_default(
            QuantFactorDefinition::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantFactorDefinition::UpdatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_factor_definition_family_status",
        quant_factor_definition_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_factor_definition_family_status")
            .table(QuantFactorDefinition::Table)
            .col(QuantFactorDefinition::FactorFamily)
            .col(QuantFactorDefinition::Status)
            .to_owned(),
        "factor definitions by family and lifecycle status",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_factor_definition_table_name() -> String {
    QuantFactorDefinition::Table.to_string()
}
