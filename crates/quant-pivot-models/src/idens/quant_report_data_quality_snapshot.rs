use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table, TableCreateStatement,
    },
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

#[quant_schema(lifecycle = "report")]
pub enum QuantReportDataQualitySnapshot {
    Table,
    ReportDataQualitySnapshotId,
    DecisionAt,
    RuntimeConfigVersionId,
    TokensJson,
    CreatedAt,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantReportDataQualitySnapshot::Table)
        .if_not_exists()
        .col(column::uuid_pk(
            QuantReportDataQualitySnapshot::ReportDataQualitySnapshotId,
        ))
        .col(
            ColumnDef::new(QuantReportDataQualitySnapshot::DecisionAt)
                .timestamp_with_time_zone()
                .not_null(),
        )
        .col(column::uuid_fk(
            QuantReportDataQualitySnapshot::RuntimeConfigVersionId,
        ))
        .col(
            ColumnDef::new(QuantReportDataQualitySnapshot::TokensJson)
                .json_binary()
                .not_null(),
        )
        .col(timestamp_with_write_default(
            QuantReportDataQualitySnapshot::CreatedAt,
        ))
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_report_dq_snapshot_runtime_config")
                .from(
                    QuantReportDataQualitySnapshot::Table,
                    QuantReportDataQualitySnapshot::RuntimeConfigVersionId,
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
        "idx_quant_report_dq_snapshot_decision_at",
        quant_report_data_quality_snapshot_table_name,
        IndexBuildMode::Transactional,
        Index::create()
            .name("idx_quant_report_dq_snapshot_decision_at")
            .table(QuantReportDataQualitySnapshot::Table)
            .col((QuantReportDataQualitySnapshot::DecisionAt, IndexOrder::Desc))
            .to_owned(),
        "report DQ snapshots by PIT timestamp",
    )]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![TableDependency::foreign_key(
        runtime_config_version_table_name,
    )]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn quant_report_data_quality_snapshot_table_name() -> String {
    QuantReportDataQualitySnapshot::Table.to_string()
}

fn runtime_config_version_table_name() -> String {
    RuntimeConfigVersion::Table.to_string()
}
