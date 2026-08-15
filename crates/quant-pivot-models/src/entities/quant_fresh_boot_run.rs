//! `quant_fresh_boot_run` current projection for durable orchestration.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    domain::quant::{FreshBootSourceCoverageManifest, ModelRouteBootstrapPreflight},
    enums::quant::{FreshBootBlockedReason, FreshBootRetryReason, FreshBootStage, FreshBootStatus},
    runtime_config::BuyModelRoute,
    types::{
        BacktestPathSetId, CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId,
        FeatureParityRunId, FreshBootRunId, ModelSpecId, ModelVersionId, PolicyActivationId,
        PolicyIdempotencyKey, PortfolioScenarioModelArtifactId, RecommendationReportId,
        ReportRunId, ResearchJobId, ResearchProfileArtifactId, SourceSliceId, TrainingDatasetId,
        WorkerId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_fresh_boot_run")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub run_id: FreshBootRunId,
    pub supersedes_run_id: Option<FreshBootRunId>,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub profile_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub route: BuyModelRoute,
    pub stage: FreshBootStage,
    pub status: FreshBootStatus,
    #[sea_orm(column_type = "JsonBinary")]
    pub source_coverage_manifest: Option<FreshBootSourceCoverageManifest>,
    pub source_coverage_hash: Option<ContentHash>,
    pub source_slice_id: Option<SourceSliceId>,
    pub source_slice_hash: Option<ContentHash>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_spec_id: Option<ModelSpecId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub calibration_dataset_id: Option<TrainingDatasetId>,
    pub source_model_version_id: Option<ModelVersionId>,
    pub model_version_id: Option<ModelVersionId>,
    pub path_set_id: Option<BacktestPathSetId>,
    pub calibration_id: Option<CalibrationArtifactId>,
    pub parity_run_id: Option<FeatureParityRunId>,
    pub scenario_artifact_id: Option<PortfolioScenarioModelArtifactId>,
    pub scenario_artifact_hash: Option<ContentHash>,
    #[sea_orm(column_type = "JsonBinary")]
    pub bootstrap_preflight: Option<ModelRouteBootstrapPreflight>,
    pub bootstrap_preflight_hash: Option<ContentHash>,
    pub active_job_id: Option<ResearchJobId>,
    pub last_job_id: Option<ResearchJobId>,
    pub bootstrap_policy_activation_id: Option<PolicyActivationId>,
    pub manual_report_ready_at: Option<DateTime<Utc>>,
    pub first_report_run_id: Option<ReportRunId>,
    pub first_report_id: Option<RecommendationReportId>,
    pub next_scheduled_report_at: Option<DateTime<Utc>>,
    pub retry_reason: Option<FreshBootRetryReason>,
    pub retry_detail: Option<String>,
    pub retry_count: i32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub blocked_reason: Option<FreshBootBlockedReason>,
    pub blocked_detail: Option<String>,
    pub lease_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text", unique)]
    pub idempotency_key: PolicyIdempotencyKey,
    pub revision: i64,
    pub stage_entered_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
