//! Per-Route readiness, model lineage, and candidate funnel for a global report run.

use crate::{
    domain::data_plane::HistorySealChunkRef,
    entities::quant_report_route_run,
    runtime_config::BuyModelRoute,
    types::{
        CalibrationArtifactId, ContentHash, HistoryServingHeadSealId, ModelRunId, ModelVersionId,
        ReportRouteRunId, ReportRunId, ResearchProfileArtifactId, ResearchProfileRef,
        ServingAuthority, TradePolicyArtifactId,
    },
};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromJsonQueryResult};
use serde::{Deserialize, Serialize};

/// Terminal outcome of one represented Route inside a report attempt.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult,
)]
#[serde(rename_all = "snake_case")]
pub enum RouteRunOutcome {
    Ready,
    ZeroCandidates,
    Failed,
}

/// Exact finalized-execution source bound to one Route decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "source")]
pub enum RouteHistoryLineage {
    /// Live serving consumed one immutable Activation head.
    Runtime {
        serving_head_seal_id: HistoryServingHeadSealId,
        serving_head_seal_hash: ContentHash,
    },
    /// Historical materialization consumed an exact accepted chunk set.
    Materialized {
        available_by: DateTime<Utc>,
        chunks: Vec<HistorySealChunkRef>,
    },
}

/// Frozen lineage required atomically before any Route-specific model filtering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct RouteModelLineage {
    pub model_version_id: ModelVersionId,
    pub model_run_id: Option<ModelRunId>,
    pub calibration_artifact_id: CalibrationArtifactId,
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub research_profile_ref: ResearchProfileRef,
    pub prediction_horizon_secs: i64,
    pub feature_contract_digest: ContentHash,
    pub pit_lineage_digest: ContentHash,
    pub serving_contract_digest: ContentHash,
    pub recommendation_contract_hash: ContentHash,
    pub report_universe_plan_hash: ContentHash,
    pub history: RouteHistoryLineage,
    pub serving_authority: ServingAuthority,
}

/// Complete Route-local funnel counts. Zero is evidence, never an omitted stage.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult,
)]
#[serde(deny_unknown_fields)]
pub struct RouteCandidateFunnel {
    pub eligible_markets: u32,
    pub feature_complete_markets: u32,
    pub calibrated_candidates: u32,
    pub admitted_economic_tiers: u32,
    pub selected_recommendations: u32,
}

/// Durable Route row linked to one report attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReportRouteRun {
    pub report_route_run_id: ReportRouteRunId,
    pub report_run_id: ReportRunId,
    pub route: BuyModelRoute,
    pub outcome: RouteRunOutcome,
    pub lineage: Option<RouteModelLineage>,
    pub funnel: RouteCandidateFunnel,
    pub diagnostic_code: Option<String>,
    pub finished_at: DateTime<Utc>,
}

/// Read projection for one durable per-Route report outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_report_route_run::Entity")]
pub struct ReportRouteRunInfo {
    pub report_route_run_id: ReportRouteRunId,
    pub report_run_id: ReportRunId,
    pub route: BuyModelRoute,
    pub outcome: RouteRunOutcome,
    pub model_version_id: Option<ModelVersionId>,
    pub model_run_id: Option<ModelRunId>,
    pub calibration_artifact_id: Option<CalibrationArtifactId>,
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    pub research_profile_artifact_id: Option<ResearchProfileArtifactId>,
    pub lineage_json: Option<RouteModelLineage>,
    pub funnel_json: RouteCandidateFunnel,
    pub diagnostic_code: Option<String>,
    pub finished_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(ReportRouteRunInfo, quant_report_route_run::Model, {
    report_route_run_id, report_run_id, route, outcome, model_version_id, model_run_id,
    calibration_artifact_id, trade_policy_artifact_id, research_profile_artifact_id,
    lineage_json, funnel_json, diagnostic_code, finished_at, created_at,
});

/// Insert payload for `quant_report_route_run`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_report_route_run::ActiveModel")]
pub struct NewReportRouteRun {
    pub report_route_run_id: ReportRouteRunId,
    pub report_run_id: ReportRunId,
    pub route: BuyModelRoute,
    pub outcome: RouteRunOutcome,
    pub model_version_id: Option<ModelVersionId>,
    pub model_run_id: Option<ModelRunId>,
    pub calibration_artifact_id: Option<CalibrationArtifactId>,
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    pub research_profile_artifact_id: Option<ResearchProfileArtifactId>,
    pub lineage_json: Option<RouteModelLineage>,
    pub funnel_json: RouteCandidateFunnel,
    pub diagnostic_code: Option<String>,
    pub finished_at: DateTime<Utc>,
}

/// API projection attached to a recommendation detail view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RouteLineageView {
    pub report_route_run_id: ReportRouteRunId,
    pub route: BuyModelRoute,
    pub outcome: RouteRunOutcome,
    pub lineage: RouteModelLineage,
    pub funnel: RouteCandidateFunnel,
}
