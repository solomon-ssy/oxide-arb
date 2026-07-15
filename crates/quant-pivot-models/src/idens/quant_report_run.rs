use quant_pivot_macros::quant_schema;
use sea_orm::{
    Iden,
    sea_query::{
        ColumnDef, Expr, ForeignKey, ForeignKeyAction, Index, IndexOrder, Table,
        TableCreateStatement,
    },
};

use crate::{
    enums::quant::{ReportRunStatus, ReportRunTerminalReason, ReportTriggerKind},
    idens::{
        quant_recommendation_report::QuantRecommendationReport,
        runtime_config_version::RuntimeConfigVersion,
    },
    schema::{
        column,
        dependency::TableDependency,
        index::{IndexBuildMode, IndexSpec},
        seed::SeedSpec,
    },
};

#[quant_schema(lifecycle = "ledger")]
pub enum QuantReportRun {
    Table,
    ReportRunId,
    TriggerKind,
    TriggerKey,
    ScheduleId,
    RequestId,
    RetryOfRunId,
    ScheduledFor,
    RequestedAt,
    Status,
    StartedAt,
    DecisionAt,
    HeartbeatAt,
    LeaseExpiresAt,
    FinishedAt,
    LeaseOwner,
    RuntimeConfigVersionId,
    TopN,
    KnowledgeLagSecs,
    OutputReportId,
    TerminalReason,
    ErrorCode,
    ErrorSummary,
}

pub fn table() -> TableCreateStatement {
    Table::create()
        .table(QuantReportRun::Table)
        .if_not_exists()
        .col(column::uuid_pk(QuantReportRun::ReportRunId))
        .col(column::pg_enum::<ReportTriggerKind>(QuantReportRun::TriggerKind))
        .col(ColumnDef::new(QuantReportRun::TriggerKey).text().not_null())
        .col(ColumnDef::new(QuantReportRun::ScheduleId).text().null())
        .col(ColumnDef::new(QuantReportRun::RequestId).text().null())
        .col(column::uuid_null(QuantReportRun::RetryOfRunId))
        .col(ColumnDef::new(QuantReportRun::ScheduledFor).timestamp_with_time_zone().null())
        .col(ColumnDef::new(QuantReportRun::RequestedAt).timestamp_with_time_zone().not_null())
        .col(column::pg_enum::<ReportRunStatus>(QuantReportRun::Status))
        .col(ColumnDef::new(QuantReportRun::StartedAt).timestamp_with_time_zone().null())
        .col(ColumnDef::new(QuantReportRun::DecisionAt).timestamp_with_time_zone().null())
        .col(ColumnDef::new(QuantReportRun::HeartbeatAt).timestamp_with_time_zone().null())
        .col(ColumnDef::new(QuantReportRun::LeaseExpiresAt).timestamp_with_time_zone().null())
        .col(ColumnDef::new(QuantReportRun::FinishedAt).timestamp_with_time_zone().null())
        .col(column::uuid_null(QuantReportRun::LeaseOwner))
        .col(column::uuid_null(QuantReportRun::RuntimeConfigVersionId))
        .col(ColumnDef::new(QuantReportRun::TopN).integer().null())
        .col(ColumnDef::new(QuantReportRun::KnowledgeLagSecs).big_integer().null())
        .col(column::uuid_null(QuantReportRun::OutputReportId))
        .col(column::pg_enum_null::<ReportRunTerminalReason>(QuantReportRun::TerminalReason))
        .col(ColumnDef::new(QuantReportRun::ErrorCode).text().null())
        .col(ColumnDef::new(QuantReportRun::ErrorSummary).text().null())
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_report_run_retry_of")
                .from(QuantReportRun::Table, QuantReportRun::RetryOfRunId)
                .to(QuantReportRun::Table, QuantReportRun::ReportRunId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_report_run_runtime_config")
                .from(QuantReportRun::Table, QuantReportRun::RuntimeConfigVersionId)
                .to(RuntimeConfigVersion::Table, RuntimeConfigVersion::RuntimeConfigVersionId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .foreign_key(
            ForeignKey::create()
                .name("fk_quant_report_run_output_report")
                .from(QuantReportRun::Table, QuantReportRun::OutputReportId)
                .to(QuantRecommendationReport::Table, QuantRecommendationReport::RecommendationReportId)
                .on_delete(ForeignKeyAction::Restrict),
        )
        .check(Expr::cust(
            "(trigger_kind = 'scheduled'::qp_report_trigger_kind AND schedule_id IS NOT NULL AND request_id IS NULL AND scheduled_for IS NOT NULL) OR (trigger_kind = 'ad_hoc'::qp_report_trigger_kind AND schedule_id IS NULL AND request_id IS NOT NULL AND scheduled_for IS NULL)",
        ))
        .check(Expr::cust("top_n IS NULL OR top_n > 0"))
        .check(Expr::cust(
            "knowledge_lag_secs IS NULL OR knowledge_lag_secs >= 0",
        ))
        .check(Expr::cust(
            "status <> 'queued'::qp_report_run_status OR trigger_kind = 'ad_hoc'::qp_report_trigger_kind OR (top_n IS NULL AND knowledge_lag_secs IS NULL)",
        ))
        .check(Expr::cust(
            "error_code IS NULL OR char_length(error_code) BETWEEN 1 AND 128",
        ))
        .check(Expr::cust(
            "error_summary IS NULL OR char_length(error_summary) BETWEEN 1 AND 4096",
        ))
        .check(Expr::cust(
            "char_length(trigger_key) BETWEEN 1 AND 512 AND (schedule_id IS NULL OR char_length(schedule_id) BETWEEN 1 AND 128) AND (request_id IS NULL OR char_length(request_id) BETWEEN 1 AND 256)",
        ))
        .check(Expr::cust(
            "retry_of_run_id IS NULL OR trigger_kind = 'ad_hoc'::qp_report_trigger_kind",
        ))
        .check(Expr::cust(
            "scheduled_for IS NULL OR scheduled_for <= requested_at",
        ))
        .check(Expr::cust(
            "(status = 'succeeded'::qp_report_run_status AND output_report_id IS NOT NULL) OR (status <> 'succeeded'::qp_report_run_status AND output_report_id IS NULL)",
        ))
        .check(Expr::cust(
            "(status = 'running'::qp_report_run_status AND lease_owner IS NOT NULL AND started_at IS NOT NULL AND decision_at IS NOT NULL AND heartbeat_at IS NOT NULL AND lease_expires_at IS NOT NULL AND runtime_config_version_id IS NOT NULL AND top_n IS NOT NULL AND knowledge_lag_secs IS NOT NULL AND finished_at IS NULL AND terminal_reason IS NULL) OR (status <> 'running'::qp_report_run_status AND lease_owner IS NULL AND lease_expires_at IS NULL)",
        ))
        .check(Expr::cust(
            "(status IN ('succeeded'::qp_report_run_status, 'failed'::qp_report_run_status, 'skipped'::qp_report_run_status, 'abandoned'::qp_report_run_status)) = (finished_at IS NOT NULL)",
        ))
        .check(Expr::cust(
            "(status = 'queued'::qp_report_run_status AND started_at IS NULL AND decision_at IS NULL AND heartbeat_at IS NULL AND finished_at IS NULL AND runtime_config_version_id IS NULL AND output_report_id IS NULL AND terminal_reason IS NULL AND error_code IS NULL AND error_summary IS NULL) OR status <> 'queued'::qp_report_run_status",
        ))
        .check(Expr::cust(
            "(status = 'skipped'::qp_report_run_status AND started_at IS NULL AND decision_at IS NULL AND heartbeat_at IS NULL AND runtime_config_version_id IS NULL AND output_report_id IS NULL AND terminal_reason IN ('coalesced_by_newer_occurrence'::qp_report_run_terminal_reason, 'schedule_reconfigured'::qp_report_run_terminal_reason, 'queue_expired'::qp_report_run_terminal_reason) AND error_code IS NULL AND error_summary IS NULL) OR status <> 'skipped'::qp_report_run_status",
        ))
        .check(Expr::cust(
            "(status = 'succeeded'::qp_report_run_status AND started_at IS NOT NULL AND decision_at IS NOT NULL AND heartbeat_at IS NOT NULL AND runtime_config_version_id IS NOT NULL AND top_n IS NOT NULL AND knowledge_lag_secs IS NOT NULL AND terminal_reason IS NULL AND error_code IS NULL AND error_summary IS NULL) OR status <> 'succeeded'::qp_report_run_status",
        ))
        .check(Expr::cust(
            "(status = 'failed'::qp_report_run_status AND started_at IS NOT NULL AND decision_at IS NOT NULL AND heartbeat_at IS NOT NULL AND runtime_config_version_id IS NOT NULL AND top_n IS NOT NULL AND knowledge_lag_secs IS NOT NULL AND terminal_reason = 'build_failed'::qp_report_run_terminal_reason AND error_code IS NOT NULL AND error_summary IS NOT NULL) OR status <> 'failed'::qp_report_run_status",
        ))
        .check(Expr::cust(
            "(status = 'abandoned'::qp_report_run_status AND started_at IS NOT NULL AND decision_at IS NOT NULL AND heartbeat_at IS NOT NULL AND runtime_config_version_id IS NOT NULL AND top_n IS NOT NULL AND knowledge_lag_secs IS NOT NULL AND terminal_reason = 'lease_expired'::qp_report_run_terminal_reason AND error_code IS NULL AND error_summary IS NULL) OR status <> 'abandoned'::qp_report_run_status",
        ))
        .check(Expr::cust(
            "terminal_reason <> 'queue_expired'::qp_report_run_terminal_reason OR trigger_kind = 'ad_hoc'::qp_report_trigger_kind",
        ))
        .check(Expr::cust(
            "terminal_reason NOT IN ('coalesced_by_newer_occurrence'::qp_report_run_terminal_reason, 'schedule_reconfigured'::qp_report_run_terminal_reason) OR trigger_kind = 'scheduled'::qp_report_trigger_kind",
        ))
        .check(Expr::cust(
            "started_at IS NULL OR (decision_at = started_at AND requested_at <= started_at)",
        ))
        .check(Expr::cust(
            "finished_at IS NULL OR started_at IS NULL OR finished_at >= started_at",
        ))
        .check(Expr::cust(
            "lease_expires_at IS NULL OR lease_expires_at > heartbeat_at",
        ))
        .to_owned()
}

pub fn indexes() -> Vec<IndexSpec> {
    vec![
        IndexSpec::sea_query(
            "uq_quant_report_run_trigger_key",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_report_run_trigger_key")
                .table(QuantReportRun::Table)
                .col(QuantReportRun::TriggerKey)
                .unique()
                .to_owned(),
            "durable report trigger idempotency authority",
        ),
        IndexSpec::sea_query(
            "uq_quant_report_run_output_report",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("uq_quant_report_run_output_report")
                .table(QuantReportRun::Table)
                .col(QuantReportRun::OutputReportId)
                .unique()
                .to_owned(),
            "one successful run per immutable report artifact",
        ),
        IndexSpec::raw(
            "uq_quant_report_run_single_running",
            table_name,
            IndexBuildMode::Transactional,
            "CREATE UNIQUE INDEX uq_quant_report_run_single_running ON quant_report_run (status) WHERE status = 'running'::qp_report_run_status",
            "single global report build slot",
        ),
        IndexSpec::raw(
            "uq_quant_report_run_queued_schedule",
            table_name,
            IndexBuildMode::Transactional,
            "CREATE UNIQUE INDEX uq_quant_report_run_queued_schedule ON quant_report_run (schedule_id) WHERE status = 'queued'::qp_report_run_status AND trigger_kind = 'scheduled'::qp_report_trigger_kind",
            "one queued occurrence per report schedule",
        ),
        IndexSpec::sea_query(
            "idx_quant_report_run_claim",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_report_run_claim")
                .table(QuantReportRun::Table)
                .col(QuantReportRun::Status)
                .col(QuantReportRun::RequestedAt)
                .col(QuantReportRun::ReportRunId)
                .to_owned(),
            "fair FIFO report-run claim queue",
        ),
        IndexSpec::sea_query(
            "idx_quant_report_run_lease_recovery",
            table_name,
            IndexBuildMode::Transactional,
            Index::create()
                .name("idx_quant_report_run_lease_recovery")
                .table(QuantReportRun::Table)
                .col(QuantReportRun::Status)
                .col((QuantReportRun::LeaseExpiresAt, IndexOrder::Asc))
                .to_owned(),
            "expired report-run lease recovery",
        ),
    ]
}

pub fn dependencies() -> Vec<TableDependency> {
    vec![
        TableDependency::foreign_key(runtime_config_table_name),
        TableDependency::foreign_key(report_table_name),
    ]
}

pub const fn seed_units() -> Vec<SeedSpec> {
    Vec::new()
}

fn table_name() -> String {
    QuantReportRun::Table.to_string()
}

fn runtime_config_table_name() -> String {
    RuntimeConfigVersion::Table.to_string()
}

fn report_table_name() -> String {
    QuantRecommendationReport::Table.to_string()
}
