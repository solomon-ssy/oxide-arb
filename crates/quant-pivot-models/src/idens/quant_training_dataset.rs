use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, Expr, ForeignKey, ForeignKeyAction, ForeignKeyCreateStatement, Index,
        IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    enums::quant::{DatasetPurpose, TrainingDatasetStatus},
    idens::{quant_model_spec::QuantModelSpec, runtime_config_version::RuntimeConfigVersion},
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

// Append-only frozen training-dataset ledger: each row pins the schema/dataset
// hashes + parquet artifact location for one materialized, point-in-time dataset.
// Status mutates through a bounded lifecycle (`planned → building → ready
// → expired`), so it is `ledger` (immutable identity, recorded status history via
// `quant_model_run`/artifact), not `runtime`.
#[quant_schema(lifecycle = "ledger")]
pub enum QuantTrainingDataset {
    Table,
    TrainingDatasetId,
    ModelSpecId,
    WindowStart,
    WindowEnd,
    Status,
    Purpose,
    FeatureSchemaHash,
    FactorSchemaHash,
    LabelSchemaHash,
    DatasetHash,
    ManifestHash,
    ManifestJson,
    ArtifactBytesHash,
    ParquetUri,
    SampleCount,
    KnowledgeLagSecs,
    SampleIntervalSecs,
    HorizonsSecs,
    FeatureSchemaVersion,
    SampleSources,
    CoverageJson,
    RuntimeConfigVersionId,
    FailureDetail,
    CompletedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    let mut stmt = training_dataset_columns();
    training_dataset_apply_foreign_keys(&mut stmt);
    stmt.check(Expr::cust(
        "(
            status NOT IN ('planned', 'building')
            OR (
                feature_schema_version IS NOT NULL
                AND sample_sources IS NOT NULL
                AND feature_schema_hash IS NULL
                AND factor_schema_hash IS NULL
                AND label_schema_hash IS NULL
                AND dataset_hash IS NULL
                AND manifest_hash IS NULL
                AND manifest_json IS NULL
                AND artifact_bytes_hash IS NULL
                AND parquet_uri IS NULL
                AND sample_count IS NULL
                AND coverage_json IS NULL
                AND completed_at IS NULL
            )
        )
        AND (
            status NOT IN ('ready', 'insufficient_labels')
            OR (
                feature_schema_version IS NOT NULL
                AND sample_sources IS NOT NULL
                AND feature_schema_hash IS NOT NULL
                AND factor_schema_hash IS NOT NULL
                AND label_schema_hash IS NOT NULL
                AND dataset_hash IS NOT NULL
                AND manifest_hash IS NOT NULL
                AND manifest_json IS NOT NULL
                AND artifact_bytes_hash IS NOT NULL
                AND parquet_uri IS NOT NULL
                AND sample_count IS NOT NULL
                AND sample_count >= 0
                AND coverage_json IS NOT NULL
                AND completed_at IS NOT NULL
            )
        )
        AND (status <> 'failed' OR completed_at IS NOT NULL)",
    ));
    stmt
}

fn training_dataset_columns() -> TableCreateStatement {
    Table::create()
        .table(QuantTrainingDataset::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantTrainingDataset::TrainingDatasetId))
        .col(column::uuid_fk(QuantTrainingDataset::ModelSpecId))
        .col(
            ColumnDef::new(QuantTrainingDataset::WindowStart)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantTrainingDataset::WindowEnd)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(column::pg_enum::<TrainingDatasetStatus>(
            QuantTrainingDataset::Status,
        ))
        .col(column::pg_enum::<DatasetPurpose>(
            QuantTrainingDataset::Purpose,
        ))
        .col(ColumnDef::new(QuantTrainingDataset::FeatureSchemaHash).text())
        .col(ColumnDef::new(QuantTrainingDataset::FactorSchemaHash).text())
        .col(ColumnDef::new(QuantTrainingDataset::LabelSchemaHash).text())
        .col(ColumnDef::new(QuantTrainingDataset::DatasetHash).text())
        .col(ColumnDef::new(QuantTrainingDataset::ManifestHash).text())
        .col(ColumnDef::new(QuantTrainingDataset::ManifestJson).json_binary())
        .col(ColumnDef::new(QuantTrainingDataset::ArtifactBytesHash).text())
        .col(ColumnDef::new(QuantTrainingDataset::ParquetUri).text())
        .col(ColumnDef::new(QuantTrainingDataset::SampleCount).big_integer())
        .col(
            ColumnDef::new(QuantTrainingDataset::KnowledgeLagSecs)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantTrainingDataset::SampleIntervalSecs)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantTrainingDataset::HorizonsSecs)
                .json_binary()
                .not_null(),
        )
        .col(ColumnDef::new(QuantTrainingDataset::FeatureSchemaVersion).integer())
        .col(ColumnDef::new(QuantTrainingDataset::SampleSources).json_binary())
        .col(ColumnDef::new(QuantTrainingDataset::CoverageJson).json_binary())
        .col(column::uuid_fk(
            QuantTrainingDataset::RuntimeConfigVersionId,
        ))
        .col(ColumnDef::new(QuantTrainingDataset::FailureDetail).text())
        .col(ColumnDef::new(QuantTrainingDataset::CompletedAt).timestamp_with_time_zone())
        .col(timestamp_with_write_default(
            QuantTrainingDataset::CreatedAt,
        ))
        .to_owned()
}

fn training_dataset_apply_foreign_keys(stmt: &mut TableCreateStatement) {
    let mut model_spec_fk = model_spec_fk();
    stmt.foreign_key(&mut model_spec_fk);
    let mut runtime_config_fk = runtime_config_fk();
    stmt.foreign_key(&mut runtime_config_fk);
}

fn model_spec_fk() -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("fk_quant_training_dataset_model_spec")
        .from(
            QuantTrainingDataset::Table,
            QuantTrainingDataset::ModelSpecId,
        )
        .to(QuantModelSpec::Table, QuantModelSpec::ModelSpecId)
        .on_delete(ForeignKeyAction::Restrict)
        .to_owned()
}

fn runtime_config_fk() -> ForeignKeyCreateStatement {
    ForeignKey::create()
        .name("fk_quant_training_dataset_runtime_config")
        .from(
            QuantTrainingDataset::Table,
            QuantTrainingDataset::RuntimeConfigVersionId,
        )
        .to(
            RuntimeConfigVersion::Table,
            RuntimeConfigVersion::RuntimeConfigVersionId,
        )
        .on_delete(ForeignKeyAction::Restrict)
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_training_dataset_spec_created",
            quant_training_dataset_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_training_dataset_spec_created")
                .table(QuantTrainingDataset::Table)
                .col(QuantTrainingDataset::ModelSpecId)
                .col((QuantTrainingDataset::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "training datasets by model spec and recency",
        ),
        IndexSpec::sea_query(
            "idx_quant_training_dataset_status",
            quant_training_dataset_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_training_dataset_status")
                .table(QuantTrainingDataset::Table)
                .col(QuantTrainingDataset::Status)
                .to_owned(),
            "training datasets by lifecycle status",
        ),
        IndexSpec::sea_query(
            "uq_quant_training_dataset_hash",
            quant_training_dataset_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_training_dataset_hash")
                .table(QuantTrainingDataset::Table)
                .col(QuantTrainingDataset::DatasetHash)
                .unique()
                .to_owned(),
            "one row per content-addressed dataset hash",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_model_spec_table_name),
        TableDependency::foreign_key(runtime_config_version_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_training_dataset_table_name() -> String {
    QuantTrainingDataset::Table.to_string()
}

fn quant_model_spec_table_name() -> String {
    QuantModelSpec::Table.to_string()
}

fn runtime_config_version_table_name() -> String {
    RuntimeConfigVersion::Table.to_string()
}
