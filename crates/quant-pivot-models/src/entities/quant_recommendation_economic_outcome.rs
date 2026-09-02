//! Immutable recommendation-level executable economic outcome.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    decision_policy_snapshot, quant_model_version, quant_recommendation,
    quant_recommendation_report, quant_report_route_run, quant_trade_policy_artifact,
    research_profile_artifact,
};
use crate::{
    domain::quant::RecommendationEconomicOutcomePayload,
    enums::quant::RecommendationEconomicOutcomeState,
    types::{
        ContentHash, DecisionPolicySnapshotId, EconomicTierId, ModelVersionId, RecommendationId,
        RecommendationReportId, ReportRouteRunId, ResearchProfileArtifactId, TradePolicyArtifactId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_recommendation_economic_outcome")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub report_route_run_id: ReportRouteRunId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub economic_tier_id: EconomicTierId,
    pub model_version_id: ModelVersionId,
    pub trade_policy_artifact_id: TradePolicyArtifactId,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub state: RecommendationEconomicOutcomeState,
    pub decision_at: DateTime<Utc>,
    pub horizon_at: DateTime<Utc>,
    pub source_available_until: DateTime<Utc>,
    #[sea_orm(column_type = "Text")]
    pub replay_kernel_version: String,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload_json: RecommendationEconomicOutcomePayload,
    pub evidence_hash: ContentHash,
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Recommendation",
        from = "recommendation_id",
        to = "recommendation_id"
    )]
    pub recommendation: BelongsTo<quant_recommendation::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "RecommendationReport",
        from = "recommendation_report_id",
        to = "recommendation_report_id"
    )]
    pub recommendation_report: BelongsTo<quant_recommendation_report::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ReportRouteRun",
        from = "report_route_run_id",
        to = "report_route_run_id"
    )]
    pub report_route_run: BelongsTo<quant_report_route_run::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "DecisionPolicySnapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub decision_policy_snapshot: BelongsTo<decision_policy_snapshot::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ModelVersion",
        from = "model_version_id",
        to = "model_version_id"
    )]
    pub model_version: BelongsTo<quant_model_version::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "TradePolicyArtifact",
        from = "trade_policy_artifact_id",
        to = "artifact_id"
    )]
    pub trade_policy_artifact: BelongsTo<quant_trade_policy_artifact::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ResearchProfileArtifact",
        from = "research_profile_artifact_id",
        to = "research_profile_artifact_id"
    )]
    pub research_profile_artifact: BelongsTo<research_profile_artifact::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
