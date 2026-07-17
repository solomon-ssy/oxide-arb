//! Durable report build-attempt ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

use crate::{
    enums::quant::{ReportRunStatus, ReportRunTerminalReason, ReportTriggerKind},
    types::{RecommendationReportId, ReportRunId, RuntimeConfigVersionId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_report_run")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub report_run_id: ReportRunId,
    pub trigger_kind: ReportTriggerKind,
    pub trigger_key: String,
    pub schedule_id: Option<String>,
    pub request_id: Option<String>,
    pub retry_of_run_id: Option<ReportRunId>,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub requested_at: DateTime<Utc>,
    pub status: ReportRunStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub decision_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub lease_owner: Option<Uuid>,
    pub runtime_config_version_id: Option<RuntimeConfigVersionId>,
    pub top_n: Option<i32>,
    pub knowledge_lag_secs: Option<i64>,
    pub output_report_id: Option<RecommendationReportId>,
    pub terminal_reason: Option<ReportRunTerminalReason>,
    pub error_code: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub error_summary: Option<String>,

    #[sea_orm(
        self_ref,
        relation_enum = "RetryOf",
        from = "retry_of_run_id",
        to = "report_run_id"
    )]
    pub retry_of: BelongsTo<Option<Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "RuntimeConfigVersion",
        from = "runtime_config_version_id",
        to = "runtime_config_version_id"
    )]
    pub runtime_config_version: BelongsTo<Option<super::runtime_config_version::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "OutputReport",
        from = "output_report_id",
        to = "recommendation_report_id"
    )]
    pub output_report: BelongsTo<Option<super::quant_recommendation_report::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
