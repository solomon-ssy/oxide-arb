use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Expr, Index, Table, TableCreateStatement},
};

use crate::{
    enums::{
        factor::{FactorDefinitionScope, FactorFamily},
        quant::PublicationStatus,
    },
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "control")]
pub enum QuantFactorDefinition {
    Table,
    FactorDefinitionId,
    DefinitionHash,
    FeatureContractHash,
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
            ColumnDef::new(QuantFactorDefinition::DefinitionHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFactorDefinition::FeatureContractHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFactorDefinition::Name)
                .text()
                .not_null(),
        )
        .col(column::pg_enum::<FactorFamily>(
            QuantFactorDefinition::FactorFamily,
        ))
        .col(column::pg_enum::<FactorDefinitionScope>(
            QuantFactorDefinition::Scope,
        ))
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
        .col(column::pg_enum::<PublicationStatus>(
            QuantFactorDefinition::Status,
        ))
        .col(column::uuid_null(QuantFactorDefinition::CreatedBy))
        .col(timestamp_with_write_default(
            QuantFactorDefinition::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantFactorDefinition::UpdatedAt,
        ))
        .check(Expr::cust("definition_hash ~ '^blake3:[0-9a-f]{64}$'"))
        .check(Expr::cust(
            "feature_contract_hash ~ '^blake3:[0-9a-f]{64}$'",
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
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
        ),
        IndexSpec::raw(
            "uq_quant_factor_definition_definition_hash",
            quant_factor_definition_table_name,
            IndexBuildMode::Transactional,
            "CREATE UNIQUE INDEX uq_quant_factor_definition_definition_hash \
             ON quant_factor_definition (definition_hash)",
            "one immutable factor revision per canonical definition hash",
        ),
        IndexSpec::raw(
            "uq_quant_factor_definition_published_name",
            quant_factor_definition_table_name,
            IndexBuildMode::Transactional,
            "CREATE UNIQUE INDEX uq_quant_factor_definition_published_name \
             ON quant_factor_definition (name) \
             WHERE status = 'published'::qp_publication_status",
            "at most one published revision per logical factor name",
        ),
    ]
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
