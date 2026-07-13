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

// `input_contract` is the ordered raw feature graph consumed by the model.
// Synthetic/encoded columns are fitted artifact outputs and never schema inputs.
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
    InputContract,
    TrainingContract,
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
            ColumnDef::new(QuantModelSpec::InputContract)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantModelSpec::TrainingContract)
                .json_binary()
                .not_null(),
        )
        .col(column::pg_enum::<PublicationStatus>(QuantModelSpec::Status))
        .col(timestamp_with_write_default(QuantModelSpec::CreatedAt))
        .col(timestamp_with_write_default(QuantModelSpec::UpdatedAt))
        .check(Expr::cust(
            "status = 'retired'::qp_publication_status OR (\
             jsonb_typeof(input_contract) = 'object' AND \
             jsonb_typeof(input_contract->'inputs') = 'array' AND \
             jsonb_array_length(input_contract->'inputs') > 0 AND \
             jsonb_typeof(training_contract) = 'object' AND \
             jsonb_typeof(training_contract->'target_label_name') = 'string' AND \
             length(training_contract->>'target_label_name') BETWEEN 1 AND 128 AND \
             (training_contract->>'validation_folds')::integer BETWEEN 2 AND 20)",
        ))
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
