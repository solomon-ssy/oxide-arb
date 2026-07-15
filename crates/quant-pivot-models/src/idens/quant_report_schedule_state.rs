use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table, TableCreateStatement},
};

use crate::{
    idens::runtime_config_version::RuntimeConfigVersion,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
        timestamp_with_write_default,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantReportScheduleState {
    Table,
    ScheduleId,
    RuntimeConfigVersionId,
    SpecHash,
    NextScheduledFor,
    LastMaterializedFor,
    Enabled,
    CreatedAt,
    UpdatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantReportScheduleState::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(QuantReportScheduleState::ScheduleId)
                .text()
                .not_null()
                .primary_key(),
        )
        .col(column::uuid_fk(
            QuantReportScheduleState::RuntimeConfigVersionId,
        ))
        .col(
            ColumnDef::new(QuantReportScheduleState::SpecHash)
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantReportScheduleState::NextScheduledFor)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantReportScheduleState::LastMaterializedFor)
                .timestamp_with_time_zone()
                .null(),
        )
        .col(
            ColumnDef::new(QuantReportScheduleState::Enabled)
                .boolean()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantReportScheduleState::CreatedAt,
        ))
        .col(timestamp_with_write_default(
            QuantReportScheduleState::UpdatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_report_schedule_state_runtime_config")
                .from(
                    QuantReportScheduleState::Table,
                    QuantReportScheduleState::RuntimeConfigVersionId,
                )
                .to(
                    RuntimeConfigVersion::Table,
                    RuntimeConfigVersion::RuntimeConfigVersionId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_report_schedule_state_due",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_report_schedule_state_due")
            .table(QuantReportScheduleState::Table)
            .col(QuantReportScheduleState::Enabled)
            .col(QuantReportScheduleState::NextScheduledFor)
            .to_owned(),
        "enabled report schedules by next occurrence",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(runtime_config_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantReportScheduleState::Table.to_string()
}

fn runtime_config_table_name() -> String {
    RuntimeConfigVersion::Table.to_string()
}
