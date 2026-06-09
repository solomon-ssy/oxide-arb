//! Replay API contract: inbound enqueue request + outbound enqueue view.

use crate::{
    domain::{
        ReplayEnqueueRequest,
        control_factor::{
            ControlFactorMaterializationRunInfo, ControlFactorStageReportInfo, MarketFilterSpec,
            ReplayAccountScope,
        },
    },
    enums::{
        common::MarketCategory,
        control_factor::{
            ControlFactorType, EvidenceStageStatus, MaterializationOutputPolicy,
            MaterializationRunKind, MaterializationRunStatus, MaterializationStageName,
            RunTriggerType,
        },
    },
    types::{EventId, MarketId, MaterializationRunId, StageReportId, TokenId},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Operator request to enqueue a replay (backfill) materialization run.
#[derive(Debug, Deserialize, Validate)]
pub struct ReplayCreateRequest {
    /// Inclusive window start.
    pub from: DateTime<Utc>,
    /// Exclusive window end (must be > `from`).
    pub to: DateTime<Utc>,
    /// Optional market scope (empty = all markets).
    #[serde(default)]
    pub market_ids: Vec<MarketId>,
    #[serde(default)]
    pub event_ids: Vec<EventId>,
    #[serde(default)]
    pub token_ids: Vec<TokenId>,
    #[serde(default)]
    pub categories: Vec<MarketCategory>,
    /// Control-factor types to (re)materialize (must be non-empty).
    #[validate(length(min = 1))]
    pub requested_factor_types: Vec<ControlFactorType>,
    /// Optional account boundary for balance evidence.
    #[serde(default)]
    pub holder_address: Option<String>,
    /// Operator justification (recorded on the run + operation log).
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
    /// Force a brand-new run even if an equivalent one exists.
    #[serde(default)]
    pub force_new_run: bool,
}

impl From<ReplayCreateRequest> for ReplayEnqueueRequest {
    fn from(request: ReplayCreateRequest) -> Self {
        Self {
            from: request.from,
            to: request.to,
            markets: MarketFilterSpec {
                market_ids: request.market_ids,
                event_ids: request.event_ids,
                token_ids: request.token_ids,
                categories: request.categories,
            },
            requested_factor_types: request.requested_factor_types,
            replay_account_scope: request
                .holder_address
                .map(|holder_address| ReplayAccountScope { holder_address }),
            reason: request.reason,
            force_new_run: request.force_new_run,
        }
    }
}

/// Response for a replay enqueue.
#[derive(Debug, Serialize)]
pub struct ReplayEnqueueView {
    /// Whether a new run was created (`false` on dedupe).
    pub created: bool,
    /// The created or deduplicated run.
    pub run: ControlFactorMaterializationRunView,
}

/// Outbound view of a materialization/replay run.
///
/// Strips the run's internal forensic blobs (`manifest`, `*_hash`,
/// `code_git_sha`, `data_requirements`, `runtime_config_ref`) — those are
/// audit-chain / reproducibility internals the dashboard never renders — while
/// keeping the operator-facing window, scope, status, report, and lifecycle.
#[derive(Debug, Clone, Serialize)]
pub struct ControlFactorMaterializationRunView {
    pub materialization_run_id: MaterializationRunId,
    pub run_dedupe_key: Option<String>,
    pub run_kind: MaterializationRunKind,
    pub trigger_type: RunTriggerType,
    pub trigger_ref: Option<String>,
    pub status: MaterializationRunStatus,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    pub source_delay_secs: i64,
    pub market_filter: serde_json::Value,
    pub requested_factor_types: serde_json::Value,
    pub output_policy: MaterializationOutputPolicy,
    pub report: serde_json::Value,
    pub created_by: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub failure_code: Option<String>,
    pub failure_detail: Option<String>,
    pub report_uri: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<ControlFactorMaterializationRunInfo> for ControlFactorMaterializationRunView {
    fn from(info: ControlFactorMaterializationRunInfo) -> Self {
        Self {
            materialization_run_id: info.materialization_run_id,
            run_dedupe_key: info.run_dedupe_key,
            run_kind: info.run_kind,
            trigger_type: info.trigger_type,
            trigger_ref: info.trigger_ref,
            status: info.status,
            window_from: info.window_from,
            window_to: info.window_to,
            source_delay_secs: info.source_delay_secs,
            market_filter: info.market_filter,
            requested_factor_types: info.requested_factor_types,
            output_policy: info.output_policy,
            report: info.report,
            created_by: info.created_by,
            started_at: info.started_at,
            finished_at: info.finished_at,
            failure_code: info.failure_code,
            failure_detail: info.failure_detail,
            report_uri: info.report_uri,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

/// Outbound view of a single per-stage report for a run.
///
/// Strips the artifact-hash forensics (`input_artifact_hashes`,
/// `output_artifact_hash`, `query_fingerprints`) used for reproducibility,
/// keeping the operator-facing coverage / metrics / warnings / errors.
#[derive(Debug, Clone, Serialize)]
pub struct ControlFactorStageReportView {
    pub stage_report_id: StageReportId,
    pub materialization_run_id: MaterializationRunId,
    pub stage_name: MaterializationStageName,
    pub status: EvidenceStageStatus,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub coverage: serde_json::Value,
    pub metrics: serde_json::Value,
    pub records_read: i64,
    pub records_written: i64,
    pub warnings: serde_json::Value,
    pub errors: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

impl From<ControlFactorStageReportInfo> for ControlFactorStageReportView {
    fn from(info: ControlFactorStageReportInfo) -> Self {
        Self {
            stage_report_id: info.stage_report_id,
            materialization_run_id: info.materialization_run_id,
            stage_name: info.stage_name,
            status: info.status,
            started_at: info.started_at,
            finished_at: info.finished_at,
            coverage: info.coverage,
            metrics: info.metrics,
            records_read: info.records_read,
            records_written: info.records_written,
            warnings: info.warnings,
            errors: info.errors,
            created_at: info.created_at,
        }
    }
}
