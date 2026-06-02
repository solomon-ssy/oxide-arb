use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::{
    enums::control_factor::MaterializationRunStatus,
    schema::{
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[oxide_schema(lifecycle = "control")]
pub enum ControlFactorMaterializationRun {
    Table,
    MaterializationRunId,
    Status,
    WindowFrom,
    WindowTo,
    SourceDelaySecs,
    Manifest,
    Report,
    CodeGitSha,
    QueryFingerprint,
    StartedAt,
    FinishedAt,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ControlFactorMaterializationRun::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::MaterializationRunId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::Status)
                .text()
                .not_null()
                .default(MaterializationRunStatus::Queued),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::WindowFrom)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::WindowTo)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::SourceDelaySecs)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::Manifest)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::Report)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::CodeGitSha)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::QueryFingerprint)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::StartedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::FinishedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(timestamp_with_write_default(
            ControlFactorMaterializationRun::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            ControlFactorMaterializationRun::UpdatedAt,
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_control_factor_materialization_run_window",
        materialization_run_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_control_factor_materialization_run_window")
            .table(ControlFactorMaterializationRun::Table)
            .col((
                ControlFactorMaterializationRun::WindowFrom,
                IndexOrder::Desc,
            ))
            .col((ControlFactorMaterializationRun::WindowTo, IndexOrder::Desc))
            .to_owned(),
        "materialization runs by PIT window",
    )]
}

pub const fn dependencies() -> Vec<TableDependency> {
    Vec::new()
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn materialization_run_table_name() -> String {
    ControlFactorMaterializationRun::Table.to_string()
}
