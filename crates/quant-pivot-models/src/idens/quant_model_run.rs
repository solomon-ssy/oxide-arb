use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    idens::{
        quant_model_version::QuantModelVersion, quant_universe_snapshot::QuantUniverseSnapshot,
    },
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
    },
};

#[quant_schema(lifecycle = "audit")]
pub enum QuantModelRun {
    Table,
    ModelRunId,
    RunKind,
    ModelVersionId,
    RuntimeConfigVersionId,
    UniverseSnapshotId,
    WindowStart,
    WindowEnd,
    Status,
    InputHash,
    OutputHash,
    MetricsJson,
    ErrorCode,
    ErrorMessage,
    StartedAt,
    FinishedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantModelRun::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantModelRun::ModelRunId))
        .col(ColumnDef::new(QuantModelRun::RunKind).text().not_null())
        .col(column::uuid_null(QuantModelRun::ModelVersionId))
        .col(column::uuid_fk(QuantModelRun::RuntimeConfigVersionId))
        .col(column::uuid_null(QuantModelRun::UniverseSnapshotId))
        .col(
            ColumnDef::new(QuantModelRun::WindowStart)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantModelRun::WindowEnd)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(ColumnDef::new(QuantModelRun::Status).text().not_null())
        .col(ColumnDef::new(QuantModelRun::InputHash).text().not_null())
        .col(ColumnDef::new(QuantModelRun::OutputHash).text().null())
        .col(
            ColumnDef::new(QuantModelRun::MetricsJson)
                .json_binary()
                .not_null(),
        )
        .col(ColumnDef::new(QuantModelRun::ErrorCode).text().null())
        .col(ColumnDef::new(QuantModelRun::ErrorMessage).text().null())
        .col(
            ColumnDef::new(QuantModelRun::StartedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantModelRun::FinishedAt)
                .timestamp_with_time_zone()
                .null(),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_model_run_model_version")
                .from(QuantModelRun::Table, QuantModelRun::ModelVersionId)
                .to(QuantModelVersion::Table, QuantModelVersion::ModelVersionId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_model_run_universe_snapshot")
                .from(QuantModelRun::Table, QuantModelRun::UniverseSnapshotId)
                .to(
                    QuantUniverseSnapshot::Table,
                    QuantUniverseSnapshot::UniverseSnapshotId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_quant_model_run_kind_started",
            quant_model_run_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_model_run_kind_started")
                .table(QuantModelRun::Table)
                .col(QuantModelRun::RunKind)
                .col((QuantModelRun::StartedAt, IndexOrder::Desc))
                .to_owned(),
            "model runs by kind and start time",
        ),
        IndexSpec::sea_query(
            "idx_quant_model_run_status_started",
            quant_model_run_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_model_run_status_started")
                .table(QuantModelRun::Table)
                .col(QuantModelRun::Status)
                .col((QuantModelRun::StartedAt, IndexOrder::Desc))
                .to_owned(),
            "model runs by status and start time",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(quant_model_version_table_name),
        TableDependency::foreign_key(quant_universe_snapshot_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_model_run_table_name() -> String {
    QuantModelRun::Table.to_string()
}

fn quant_model_version_table_name() -> String {
    QuantModelVersion::Table.to_string()
}

fn quant_universe_snapshot_table_name() -> String {
    QuantUniverseSnapshot::Table.to_string()
}
