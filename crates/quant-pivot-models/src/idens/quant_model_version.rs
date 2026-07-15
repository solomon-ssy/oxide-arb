use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    enums::quant::PublicationStatus,
    idens::{
        quant_model_spec::QuantModelSpec, quant_trade_policy_artifact::QuantTradePolicyArtifact,
        quant_training_dataset::QuantTrainingDataset,
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
pub enum QuantModelVersion {
    Table,
    ModelVersionId,
    ModelSpecId,
    Version,
    ArtifactHash,
    ProfileRef,
    TrainingDatasetId,
    TradePolicyArtifactId,
    TradePolicyHash,
    /// Explicit CPCV path set the publish/promote quality gate must evaluate
    /// (Phase 11.5 remediation). `NULL` means no publish candidate is bound —
    /// the gate fails `CpcvRequired` rather than silently reading "latest".
    PublishPathSetId,
    MetricsJson,
    TrainingObjectiveJson,
    QualityGateReport,
    PublicationStatus,
    PublishedAt,
    RetiredAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantModelVersion::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantModelVersion::ModelVersionId))
        .col(column::uuid_fk(QuantModelVersion::ModelSpecId))
        .col(
            ColumnDef::new(QuantModelVersion::Version)
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantModelVersion::ArtifactHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantModelVersion::ProfileRef)
                .json_binary()
                .not_null(),
        )
        .col(column::uuid_null(QuantModelVersion::TrainingDatasetId))
        .col(column::uuid_null(QuantModelVersion::TradePolicyArtifactId))
        .col(
            ColumnDef::new(QuantModelVersion::TradePolicyHash)
                .text()
                .null(),
        )
        .col(column::uuid_null(QuantModelVersion::PublishPathSetId))
        .col(
            ColumnDef::new(QuantModelVersion::MetricsJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantModelVersion::TrainingObjectiveJson)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantModelVersion::QualityGateReport)
                .json_binary()
                .not_null(),
        )
        .col(column::pg_enum::<PublicationStatus>(
            QuantModelVersion::PublicationStatus,
        ))
        .col(
            ColumnDef::new(QuantModelVersion::PublishedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantModelVersion::RetiredAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(timestamp_with_write_default(QuantModelVersion::CreatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_model_version_spec")
                .from(QuantModelVersion::Table, QuantModelVersion::ModelSpecId)
                .to(QuantModelSpec::Table, QuantModelSpec::ModelSpecId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_model_version_trade_policy")
                .from(
                    QuantModelVersion::Table,
                    QuantModelVersion::TradePolicyArtifactId,
                )
                .to(
                    QuantTradePolicyArtifact::Table,
                    QuantTradePolicyArtifact::ArtifactId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_model_version_training_dataset")
                .from(
                    QuantModelVersion::Table,
                    QuantModelVersion::TrainingDatasetId,
                )
                .to(
                    QuantTrainingDataset::Table,
                    QuantTrainingDataset::TrainingDatasetId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        // `publish_path_set_id` is intentionally **not** a DB foreign key:
        // `quant_backtest_path_set` already FKs to this table, and a reverse FK
        // would create a create-order cycle. Ownership is enforced in
        // `ModelGovernanceService::bind_publish_path_set` (path set must belong
        // to the version) before the column is written.
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_model_version_spec_version",
            quant_model_version_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_model_version_spec_version")
                .table(QuantModelVersion::Table)
                .col(QuantModelVersion::ModelSpecId)
                .col(QuantModelVersion::Version)
                .unique()
                .to_owned(),
            "one version number per model spec",
        ),
        IndexSpec::sea_query(
            "idx_quant_model_version_spec_created",
            quant_model_version_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_model_version_spec_created")
                .table(QuantModelVersion::Table)
                .col(QuantModelVersion::ModelSpecId)
                .col((QuantModelVersion::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "model versions by model spec and recency",
        ),
        IndexSpec::sea_query(
            "idx_quant_model_version_status",
            quant_model_version_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_model_version_status")
                .table(QuantModelVersion::Table)
                .col(QuantModelVersion::PublicationStatus)
                .to_owned(),
            "model versions by publication status",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_model_spec_table_name),
        TableDependency::foreign_key(quant_trade_policy_artifact_table_name),
        TableDependency::foreign_key(quant_training_dataset_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_model_version_table_name() -> String {
    QuantModelVersion::Table.to_string()
}

fn quant_model_spec_table_name() -> String {
    QuantModelSpec::Table.to_string()
}

fn quant_training_dataset_table_name() -> String {
    QuantTrainingDataset::Table.to_string()
}

fn quant_trade_policy_artifact_table_name() -> String {
    QuantTradePolicyArtifact::Table.to_string()
}
