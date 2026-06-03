use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    enums::control_factor::EvidenceStageStatus,
    idens::control_factor_materialization_run::ControlFactorMaterializationRun,
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[oxide_schema(lifecycle = "control")]
pub enum ControlFactorStageReport {
    Table,
    StageReportId,
    MaterializationRunId,
    StageName,
    Status,
    StartedAt,
    FinishedAt,
    InputArtifactHashes,
    OutputArtifactHash,
    Coverage,
    Metrics,
    RecordsRead,
    RecordsWritten,
    Warnings,
    Errors,
    QueryFingerprints,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ControlFactorStageReport::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(ControlFactorStageReport::StageReportId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::MaterializationRunId)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::StageName)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::Status)
                .text()
                .not_null()
                .default(EvidenceStageStatus::Pending),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::StartedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::FinishedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::InputArtifactHashes)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::OutputArtifactHash)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::Coverage)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::Metrics)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::RecordsRead)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::RecordsWritten)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::Warnings)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::Errors)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::QueryFingerprints)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            ControlFactorStageReport::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_control_factor_stage_report_run")
                .from(
                    ControlFactorStageReport::Table,
                    ControlFactorStageReport::MaterializationRunId,
                )
                .to(
                    ControlFactorMaterializationRun::Table,
                    ControlFactorMaterializationRun::MaterializationRunId,
                )
                .on_delete(ForeignKeyAction::Cascade),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uniq_cfm_stage",
            stage_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uniq_cfm_stage")
                .table(ControlFactorStageReport::Table)
                .col(ControlFactorStageReport::MaterializationRunId)
                .col(ControlFactorStageReport::StageName)
                .unique()
                .to_owned(),
            "one retry-safe report per materialization run and stage",
        ),
        IndexSpec::sea_query(
            "idx_cfm_stage_run_created",
            stage_report_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_cfm_stage_run_created")
                .table(ControlFactorStageReport::Table)
                .col(ControlFactorStageReport::MaterializationRunId)
                .col((ControlFactorStageReport::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "evidence stage reports by materialization run",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(materialization_run_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn stage_report_table_name() -> String {
    ControlFactorStageReport::Table.to_string()
}

fn materialization_run_table_name() -> String {
    ControlFactorMaterializationRun::Table.to_string()
}
