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
pub enum QuantModelSpec {
    Table,
    ModelSpecId,
    Name,
    ModelFamily,
    PredictionHorizonSecs,
    FeatureSchemaVersion,
    LabelSchemaVersion,
    SpecJson,
    Status,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantModelSpec::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantModelSpec::ModelSpecId))
        .col(
            ColumnDef::new(QuantModelSpec::Name)
                .text()
                .not_null()
                .unique_key(),
        )
        .col(
            ColumnDef::new(QuantModelSpec::ModelFamily)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantModelSpec::PredictionHorizonSecs)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantModelSpec::FeatureSchemaVersion)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantModelSpec::LabelSchemaVersion)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantModelSpec::SpecJson)
                .json_binary()
                .not_null(),
        )
        .col(ColumnDef::new(QuantModelSpec::Status).text().not_null())
        .col(timestamp_with_write_default(QuantModelSpec::CreatedAt))
        .col(timestamp_with_write_default(QuantModelSpec::UpdatedAt))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_model_spec_family_status",
        quant_model_spec_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_model_spec_family_status")
            .table(QuantModelSpec::Table)
            .col(QuantModelSpec::ModelFamily)
            .col(QuantModelSpec::Status)
            .to_owned(),
        "model specs by family and publication status",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_model_spec_table_name() -> String {
    QuantModelSpec::Table.to_string()
}
