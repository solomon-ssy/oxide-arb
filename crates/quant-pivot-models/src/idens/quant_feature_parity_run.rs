use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    enums::quant::{FeatureParityRunKind, FeatureParityRunStatus},
    idens::{
        quant_model_version::QuantModelVersion,
        quant_recommendation_report::QuantRecommendationReport,
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

// Mutable lifecycle ledger for deterministic replay execution. The result is
// terminal once passed/mismatched/failed/cancelled; transition guards live in
// the repository and updated_at is maintained by the canonical runtime trigger.
#[quant_schema(lifecycle = "runtime")]
pub enum QuantFeatureParityRun {
    Table,
    RunId,
    Kind,
    Status,
    WindowStart,
    WindowEnd,
    ReportId,
    ModelVersionId,
    TrainingDatasetId,
    TriggeredBy,
    RequestedBy,
    ActingRole,
    Reason,
    TotalCount,
    ComparedCount,
    MatchedCount,
    MismatchedCount,
    PendingMaterializationCount,
    FeatureContractHash,
    TransformHash,
    FailureCode,
    FailureDetail,
    StartedAt,
    PendingSince,
    ContainmentCompletedAt,
    FinishedAt,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    let mut table = Table::create();
    table.table(QuantFeatureParityRun::Table).if_not_exists();
    add_identity_columns(&mut table);
    add_result_columns(&mut table);
    add_lifecycle_columns(&mut table);
    add_foreign_keys(&mut table);
    table
}

fn add_identity_columns(table: &mut TableCreateStatement) {
    table
        .col(column::uuid_pk(QuantFeatureParityRun::RunId))
        .col(column::pg_enum::<FeatureParityRunKind>(
            QuantFeatureParityRun::Kind,
        ))
        .col(column::pg_enum_default::<FeatureParityRunStatus>(
            QuantFeatureParityRun::Status,
            &FeatureParityRunStatus::Queued,
        ))
        .col(
            ColumnDef::new(QuantFeatureParityRun::WindowStart)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::WindowEnd)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(column::uuid_null(QuantFeatureParityRun::ReportId))
        .col(column::uuid_null(QuantFeatureParityRun::ModelVersionId))
        .col(column::uuid_null(QuantFeatureParityRun::TrainingDatasetId))
        .col(
            ColumnDef::new(QuantFeatureParityRun::TriggeredBy)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::RequestedBy)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::ActingRole)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::Reason)
                .text()
                .not_null(),
        );
}

fn add_result_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(QuantFeatureParityRun::TotalCount)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::ComparedCount)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::MatchedCount)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::MismatchedCount)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::PendingMaterializationCount)
                .big_integer()
                .not_null()
                .default(0),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::FeatureContractHash)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::TransformHash)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::FailureCode)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::FailureDetail)
                .text()
                .null(),
        );
}

fn add_lifecycle_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(QuantFeatureParityRun::StartedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::PendingSince)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::ContainmentCompletedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantFeatureParityRun::FinishedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(timestamp_with_write_default(
            QuantFeatureParityRun::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantFeatureParityRun::UpdatedAt,
        ));
}

fn add_foreign_keys(table: &mut TableCreateStatement) {
    table.foreign_key(
        ForeignKey::create()
            .name("fk_quant_feature_parity_run_report")
            .from(
                QuantFeatureParityRun::Table,
                QuantFeatureParityRun::ReportId,
            )
            .to(
                QuantRecommendationReport::Table,
                QuantRecommendationReport::RecommendationReportId,
            )
            .on_delete(ForeignKeyAction::Restrict),
    );
    table.foreign_key(
        ForeignKey::create()
            .name("fk_quant_feature_parity_run_model")
            .from(
                QuantFeatureParityRun::Table,
                QuantFeatureParityRun::ModelVersionId,
            )
            .to(QuantModelVersion::Table, QuantModelVersion::ModelVersionId)
            .on_delete(ForeignKeyAction::Restrict),
    );
    table.foreign_key(
        ForeignKey::create()
            .name("fk_quant_feature_parity_run_dataset")
            .from(
                QuantFeatureParityRun::Table,
                QuantFeatureParityRun::TrainingDatasetId,
            )
            .to(
                QuantTrainingDataset::Table,
                QuantTrainingDataset::TrainingDatasetId,
            )
            .on_delete(ForeignKeyAction::Restrict),
    );
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_feature_parity_run_kind_created",
            quant_feature_parity_run_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_feature_parity_run_kind_created")
                .table(QuantFeatureParityRun::Table)
                .col(QuantFeatureParityRun::Kind)
                .col((QuantFeatureParityRun::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "parity runs by scope and recency",
        ),
        IndexSpec::sea_query(
            "idx_quant_feature_parity_run_status_created",
            quant_feature_parity_run_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_feature_parity_run_status_created")
                .table(QuantFeatureParityRun::Table)
                .col(QuantFeatureParityRun::Status)
                .col((QuantFeatureParityRun::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "parity work queue and terminal history",
        ),
        IndexSpec::raw(
            "uq_quant_feature_parity_run_sampled_report",
            quant_feature_parity_run_table_name,
            IndexBuildMode::Transactional,
            "CREATE UNIQUE INDEX uq_quant_feature_parity_run_sampled_report \
             ON quant_feature_parity_run (report_id) \
             WHERE kind = 'sampled'::qp_feature_parity_run_kind AND report_id IS NOT NULL",
            "exactly one sampled replay ledger per committed serving report",
        ),
        IndexSpec::raw(
            "uq_quant_feature_parity_run_full_window",
            quant_feature_parity_run_table_name,
            IndexBuildMode::Transactional,
            "CREATE UNIQUE INDEX uq_quant_feature_parity_run_full_window \
             ON quant_feature_parity_run (window_start, window_end) \
             WHERE kind = 'full'::qp_feature_parity_run_kind \
               AND report_id IS NULL \
               AND model_version_id IS NULL \
               AND training_dataset_id IS NULL \
               AND status IN (\
                   'queued'::qp_feature_parity_run_status, \
                   'running'::qp_feature_parity_run_status, \
                   'pending_materialization'::qp_feature_parity_run_status\
               )",
            "one active unbound runtime full replay per window across replicas; terminal windows remain replayable for governed recovery",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_recommendation_report_table_name),
        TableDependency::foreign_key(quant_model_version_table_name),
        TableDependency::foreign_key(quant_training_dataset_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_feature_parity_run_table_name() -> String {
    QuantFeatureParityRun::Table.to_string()
}

fn quant_recommendation_report_table_name() -> String {
    QuantRecommendationReport::Table.to_string()
}

fn quant_model_version_table_name() -> String {
    QuantModelVersion::Table.to_string()
}

fn quant_training_dataset_table_name() -> String {
    QuantTrainingDataset::Table.to_string()
}
