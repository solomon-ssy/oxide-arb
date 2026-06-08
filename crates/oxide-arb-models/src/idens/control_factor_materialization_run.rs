use oxide_arb_macros::oxide_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, Index, IndexOrder, Table, TableCreateStatement},
};

use crate::{
    enums::control_factor::MaterializationRunStatus,
    schema::{
        column,
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
    RunDedupeKey,
    RunKind,
    TriggerType,
    TriggerRef,
    Status,
    WindowFrom,
    WindowTo,
    SourceDelaySecs,
    MarketFilter,
    RequestedFactorTypes,
    DataRequirements,
    RuntimeConfigRef,
    SimulationConfigHash,
    QualityGatePolicyHash,
    OutputPolicy,
    Manifest,
    ManifestHash,
    Report,
    CodeGitSha,
    CreatedBy,
    StartedAt,
    FinishedAt,
    FailureCode,
    FailureDetail,
    ReportUri,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    let mut table = Table::create();
    table
        .table(ControlFactorMaterializationRun::Table)
        .if_not_exists();
    add_identity_columns(&mut table);
    add_window_columns(&mut table);
    add_manifest_columns(&mut table);
    add_lifecycle_columns(&mut table);
    add_timestamp_columns(&mut table);
    table.clone()
}

fn add_identity_columns(table: &mut TableCreateStatement) {
    table
        .col(column::uuid_pk(
            ControlFactorMaterializationRun::MaterializationRunId,
        ))
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::RunDedupeKey)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::RunKind)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::TriggerType)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::TriggerRef)
                .text()
                .null(),
        );
}

fn add_window_columns(table: &mut TableCreateStatement) {
    table
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
        );
}

fn add_manifest_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::MarketFilter)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::RequestedFactorTypes)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::DataRequirements)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::RuntimeConfigRef)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::SimulationConfigHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::QualityGatePolicyHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::OutputPolicy)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::Manifest)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::ManifestHash)
                .text()
                .not_null(),
        );
}

fn add_lifecycle_columns(table: &mut TableCreateStatement) {
    table
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::Status)
                .text()
                .not_null()
                .default(MaterializationRunStatus::Queued),
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
            ColumnDef::new(ControlFactorMaterializationRun::CreatedBy)
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
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::FailureCode)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::FailureDetail)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(ControlFactorMaterializationRun::ReportUri)
                .text()
                .null(),
        );
}

fn add_timestamp_columns(table: &mut TableCreateStatement) {
    table
        .col(timestamp_with_write_default(
            ControlFactorMaterializationRun::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            ControlFactorMaterializationRun::UpdatedAt,
        ));
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::raw(
            "uniq_cfm_run_dedupe_key",
            materialization_run_table_name,
            IndexBuildMode::Transactional,
            "CREATE UNIQUE INDEX IF NOT EXISTS uniq_cfm_run_dedupe_key \
             ON control_factor_materialization_run (run_dedupe_key) \
             WHERE run_dedupe_key IS NOT NULL",
            "deduplicate equivalent materialization runs",
        ),
        IndexSpec::sea_query(
            "idx_cfm_run_status_created_at",
            materialization_run_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_cfm_run_status_created_at")
                .table(ControlFactorMaterializationRun::Table)
                .col(ControlFactorMaterializationRun::Status)
                .col((ControlFactorMaterializationRun::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "materialization runs by lifecycle status and recency",
        ),
        IndexSpec::sea_query(
            "idx_cfm_run_window",
            materialization_run_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_cfm_run_window")
                .table(ControlFactorMaterializationRun::Table)
                .col((
                    ControlFactorMaterializationRun::WindowFrom,
                    IndexOrder::Desc,
                ))
                .col((ControlFactorMaterializationRun::WindowTo, IndexOrder::Desc))
                .to_owned(),
            "materialization runs by PIT window",
        ),
        IndexSpec::sea_query(
            "idx_cfm_run_kind_created_at",
            materialization_run_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_cfm_run_kind_created_at")
                .table(ControlFactorMaterializationRun::Table)
                .col(ControlFactorMaterializationRun::RunKind)
                .col((ControlFactorMaterializationRun::CreatedAt, IndexOrder::Desc))
                .to_owned(),
            "materialization runs by kind and recency",
        ),
    ]
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
