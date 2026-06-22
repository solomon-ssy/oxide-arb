use oxide_arb_macros::oxide_schema;
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

#[oxide_schema(lifecycle = "control")]
pub enum ControlFactorTrainingDataset {
    Table,
    DatasetId,
    MaterializationRunId,
    FactorType,
    WindowFrom,
    WindowTo,
    EntityCount,
    ExampleCount,
    LabelCount,
    DatasetHash,
    FeatureSchemaHash,
    LabelSchemaHash,
    StorageUri,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ControlFactorTrainingDataset::Table)
        .if_not_exists()
        .col(column::uuid_pk(ControlFactorTrainingDataset::DatasetId))
        .col(column::uuid_fk(
            ControlFactorTrainingDataset::MaterializationRunId,
        ))
        .col(
            ColumnDef::new(ControlFactorTrainingDataset::FactorType)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorTrainingDataset::WindowFrom)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorTrainingDataset::WindowTo)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorTrainingDataset::EntityCount)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorTrainingDataset::ExampleCount)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorTrainingDataset::LabelCount)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorTrainingDataset::DatasetHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorTrainingDataset::FeatureSchemaHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorTrainingDataset::LabelSchemaHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorTrainingDataset::StorageUri)
                .text()
                .null(),
        )
        .col(timestamp_with_write_default(
            ControlFactorTrainingDataset::CreatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_control_factor_training_dataset_run",
        training_dataset_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_control_factor_training_dataset_run")
            .table(ControlFactorTrainingDataset::Table)
            .col(ControlFactorTrainingDataset::MaterializationRunId)
            .col((ControlFactorTrainingDataset::CreatedAt, IndexOrder::Desc))
            .to_owned(),
        "training datasets by materialization run",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn training_dataset_table_name() -> String {
    ControlFactorTrainingDataset::Table.to_string()
}
