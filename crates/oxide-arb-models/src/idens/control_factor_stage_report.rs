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
    WindowFrom,
    WindowTo,
    Coverage,
    Warnings,
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
            ColumnDef::new(ControlFactorStageReport::WindowFrom)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::WindowTo)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::Coverage)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorStageReport::Warnings)
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
    vec![IndexSpec::sea_query(
        "idx_control_factor_stage_report_run",
        stage_report_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_control_factor_stage_report_run")
            .table(ControlFactorStageReport::Table)
            .col(ControlFactorStageReport::MaterializationRunId)
            .col((ControlFactorStageReport::CreatedAt, IndexOrder::Desc))
            .to_owned(),
        "evidence stage reports by materialization run",
    )]
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
