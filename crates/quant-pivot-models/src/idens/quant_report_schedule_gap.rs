use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, Expr, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table,
        TableCreateStatement,
    },
};

use crate::{
    enums::quant::ReportScheduleGapReason,
    idens::runtime_config_version::RuntimeConfigVersion,
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantReportScheduleGap {
    Table,
    GapId,
    ScheduleId,
    RuntimeConfigVersionId,
    Reason,
    FirstScheduledFor,
    LastScheduledFor,
    MissedCount,
    DetectedAt,
    Detail,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantReportScheduleGap::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantReportScheduleGap::GapId))
        .col(
            ColumnDef::new(QuantReportScheduleGap::ScheduleId)
                .text()
                .not_null(),
        )
        .col(column::uuid_fk(
            QuantReportScheduleGap::RuntimeConfigVersionId,
        ))
        .col(column::pg_enum::<ReportScheduleGapReason>(
            QuantReportScheduleGap::Reason,
        ))
        .col(
            ColumnDef::new(QuantReportScheduleGap::FirstScheduledFor)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantReportScheduleGap::LastScheduledFor)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantReportScheduleGap::MissedCount)
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(QuantReportScheduleGap::DetectedAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(ColumnDef::new(QuantReportScheduleGap::Detail).text().null())
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_report_schedule_gap_runtime_config")
                .from(
                    QuantReportScheduleGap::Table,
                    QuantReportScheduleGap::RuntimeConfigVersionId,
                )
                .to(
                    RuntimeConfigVersion::Table,
                    RuntimeConfigVersion::RuntimeConfigVersionId,
                )
                .on_delete(ForeignKeyAction::Restrict),
        )
        .check(Expr::cust("missed_count > 0"))
        .check(Expr::cust("first_scheduled_for <= last_scheduled_for"))
        .check(Expr::cust(
            "detail IS NULL OR char_length(detail) BETWEEN 1 AND 4096",
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![IndexSpec::sea_query(
        "idx_quant_report_schedule_gap_detected",
        table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_report_schedule_gap_detected")
            .table(QuantReportScheduleGap::Table)
            .col(QuantReportScheduleGap::ScheduleId)
            .col((QuantReportScheduleGap::DetectedAt, IndexOrder::Desc))
            .col(QuantReportScheduleGap::GapId)
            .to_owned(),
        "schedule-gap audit pagination",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(runtime_config_table_name)]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantReportScheduleGap::Table.to_string()
}

fn runtime_config_table_name() -> String {
    RuntimeConfigVersion::Table.to_string()
}
