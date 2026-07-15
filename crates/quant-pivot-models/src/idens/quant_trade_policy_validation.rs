use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, Expr, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table,
        TableCreateStatement,
    },
};

use crate::{
    enums::quant::TradePolicyValidationStatus,
    idens::{
        quant_trade_policy_artifact::QuantTradePolicyArtifact,
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

#[quant_schema(lifecycle = "ledger")]
pub enum QuantTradePolicyValidation {
    Table,
    ValidationRunId,
    ArtifactId,
    ArtifactHash,
    SourceDatasetId,
    SourceDatasetHash,
    SourceSliceManifestHash,
    EvidenceManifestHash,
    Status,
    TotalRows,
    PassedRows,
    FailedRows,
    ValidationHash,
    FailureDetail,
    ActorId,
    Reason,
    StartedAt,
    CompletedAt,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantTradePolicyValidation::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantTradePolicyValidation::ValidationRunId))
        .col(column::uuid_fk(QuantTradePolicyValidation::ArtifactId))
        .col(
            ColumnDef::new(QuantTradePolicyValidation::ArtifactHash)
                .text()
                .not_null(),
        )
        .col(column::uuid_fk(QuantTradePolicyValidation::SourceDatasetId))
        .col(
            ColumnDef::new(QuantTradePolicyValidation::SourceDatasetHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantTradePolicyValidation::SourceSliceManifestHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantTradePolicyValidation::EvidenceManifestHash)
                .text()
                .not_null(),
        )
        .col(column::pg_enum::<TradePolicyValidationStatus>(
            QuantTradePolicyValidation::Status,
        ))
        .col(
            ColumnDef::new(QuantTradePolicyValidation::TotalRows)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(QuantTradePolicyValidation::PassedRows)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(QuantTradePolicyValidation::FailedRows)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(ColumnDef::new(QuantTradePolicyValidation::ValidationHash).text())
        .col(ColumnDef::new(QuantTradePolicyValidation::FailureDetail).text())
        .col(column::uuid_fk(QuantTradePolicyValidation::ActorId))
        .col(
            ColumnDef::new(QuantTradePolicyValidation::Reason)
                .text()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantTradePolicyValidation::StartedAt,
        ))
        .col(ColumnDef::new(QuantTradePolicyValidation::CompletedAt).timestamp_with_time_zone())
        .col(timestamp_with_write_default(
            QuantTradePolicyValidation::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_trade_policy_validation_artifact")
                .from(
                    QuantTradePolicyValidation::Table,
                    QuantTradePolicyValidation::ArtifactId,
                )
                .to(
                    QuantTradePolicyArtifact::Table,
                    QuantTradePolicyArtifact::ArtifactId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_trade_policy_validation_dataset")
                .from(
                    QuantTradePolicyValidation::Table,
                    QuantTradePolicyValidation::SourceDatasetId,
                )
                .to(
                    QuantTrainingDataset::Table,
                    QuantTrainingDataset::TrainingDatasetId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .check(Expr::cust(
            "total_rows >= 0 AND passed_rows >= 0 AND failed_rows >= 0
             AND passed_rows + failed_rows <= total_rows
             AND length(btrim(reason)) BETWEEN 1 AND 512
             AND (
               (status = 'running' AND validation_hash IS NULL AND failure_detail IS NULL
                 AND completed_at IS NULL)
               OR (status = 'succeeded' AND validation_hash IS NOT NULL
                 AND failure_detail IS NULL AND completed_at IS NOT NULL
                 AND failed_rows = 0 AND passed_rows = total_rows AND total_rows > 0)
               OR (status IN ('failed', 'cancelled') AND validation_hash IS NOT NULL
                 AND failure_detail IS NOT NULL AND completed_at IS NOT NULL)
             )",
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_trade_policy_validation_artifact_created",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_trade_policy_validation_artifact_created")
                .table(QuantTradePolicyValidation::Table)
                .col(QuantTradePolicyValidation::ArtifactId)
                .col((QuantTradePolicyValidation::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "validation attempts by immutable policy and recency",
        ),
        IndexSpec::raw(
            "uq_quant_trade_policy_validation_running_artifact",
            table_name,
            IndexBuildMode::Transactional,
            "CREATE UNIQUE INDEX uq_quant_trade_policy_validation_running_artifact
             ON quant_trade_policy_validation (artifact_id)
             WHERE status = 'running'::qp_trade_policy_validation_status",
            "only one independent validation may run for an immutable Draft",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(trade_policy_table_name),
        TableDependency::foreign_key(training_dataset_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantTradePolicyValidation::Table.to_string()
}

fn trade_policy_table_name() -> String {
    QuantTradePolicyArtifact::Table.to_string()
}

fn training_dataset_table_name() -> String {
    QuantTrainingDataset::Table.to_string()
}
