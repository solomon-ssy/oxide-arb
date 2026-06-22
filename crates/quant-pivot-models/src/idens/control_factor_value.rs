use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
};

use crate::{
    enums::control_factor::FactorStatus,
    idens::control_factor_materialization_run::ControlFactorMaterializationRun,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "control")]
pub enum ControlFactorValue {
    Table,
    FactorId,
    RunId,
    FactorType,
    Dimensions,
    DimensionsHash,
    Payload,
    PayloadHash,
    Evidence,
    Status,
    StatusReason,
    GeneratedAt,
    ExpiresAt,
    Owner,
    SchemaVersion,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(ControlFactorValue::Table)
        .if_not_exists()
        .col(column::uuid_pk(ControlFactorValue::FactorId))
        .col(column::uuid_fk(ControlFactorValue::RunId))
        .col(
            ColumnDef::new(ControlFactorValue::FactorType)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorValue::Dimensions)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorValue::DimensionsHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorValue::Payload)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorValue::PayloadHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorValue::Evidence)
                .json_binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorValue::Status)
                .text()
                .not_null()
                .default(FactorStatus::Draft),
        )
        .col(
            ColumnDef::new(ControlFactorValue::StatusReason)
                .text()
                .null(),
        )
        .col(
            ColumnDef::new(ControlFactorValue::GeneratedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(ControlFactorValue::ExpiresAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(ColumnDef::new(ControlFactorValue::Owner).text().not_null())
        .col(
            ColumnDef::new(ControlFactorValue::SchemaVersion)
                .integer()
                .not_null(),
        )
        .col(timestamp_with_write_default(ControlFactorValue::CreatedAt))
        .col(timestamp_with_write_default(ControlFactorValue::UpdatedAt))
        .foreign_key(
            ForeignKey::create()
                .name("fk_control_factor_value_run")
                .from(ControlFactorValue::Table, ControlFactorValue::RunId)
                .to(
                    ControlFactorMaterializationRun::Table,
                    ControlFactorMaterializationRun::MaterializationRunId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "idx_control_factor_value_type_status_expires",
            control_factor_value_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_control_factor_value_type_status_expires")
                .table(ControlFactorValue::Table)
                .col(ControlFactorValue::FactorType)
                .col(ControlFactorValue::Status)
                .col((ControlFactorValue::ExpiresAt, IndexOrder::Asc))
                .to_owned(),
            "factor lookup by type, lifecycle status, and expiry",
        ),
        IndexSpec::sea_query(
            "idx_control_factor_value_run",
            control_factor_value_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_control_factor_value_run")
                .table(ControlFactorValue::Table)
                .col(ControlFactorValue::RunId)
                .to_owned(),
            "factor lookup by materialization run",
        ),
        IndexSpec::sea_query(
            "idx_control_factor_value_dimensions_hash",
            control_factor_value_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_control_factor_value_dimensions_hash")
                .table(ControlFactorValue::Table)
                .col(ControlFactorValue::DimensionsHash)
                .to_owned(),
            "factor lookup by dimensions hash",
        ),
        IndexSpec::sea_query(
            "uniq_control_factor_value_run_payload",
            control_factor_value_table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uniq_control_factor_value_run_payload")
                .table(ControlFactorValue::Table)
                .col(ControlFactorValue::RunId)
                .col(ControlFactorValue::FactorType)
                .col(ControlFactorValue::DimensionsHash)
                .col(ControlFactorValue::PayloadHash)
                .unique()
                .to_owned(),
            "dedupe factor rows per run/type/dimensions/payload",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(materialization_run_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn control_factor_value_table_name() -> String {
    ControlFactorValue::Table.to_string()
}

fn materialization_run_table_name() -> String {
    ControlFactorMaterializationRun::Table.to_string()
}
