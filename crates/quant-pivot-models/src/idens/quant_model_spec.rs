use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Expr, Index, Table, TableCreateStatement},
};

use crate::{
    enums::{model::ModelFamily, quant::PublicationStatus},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

// `feature_requirements` (11.2.2 remediation R7) is the governed contract a
// dataset build's offline point-in-time selector gates on: it is the exact
// requirement set the eventual model (trained under this spec) would impose
// online, so the offline funnel mirrors the real serving population instead
// of guessing. Structured JSON deserializing to
// `quant_pivot_research::selection::ModelFeatureRequirements`; validated at
// authoring time in `quant-pivot-core`'s `ModelSpecService`.
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
    FeatureRequirements,
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
        .col(column::pg_enum::<ModelFamily>(QuantModelSpec::ModelFamily))
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
        .col(
            ColumnDef::new(QuantModelSpec::FeatureRequirements)
                .json_binary()
                .not_null()
                .default(Expr::cust("'{}'::jsonb")),
        )
        .col(column::pg_enum::<PublicationStatus>(QuantModelSpec::Status))
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
