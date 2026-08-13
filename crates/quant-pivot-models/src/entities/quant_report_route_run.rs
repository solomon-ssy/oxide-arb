//! `quant_report_route_run` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    quant_calibration_artifact, quant_model_run, quant_model_version, quant_report_run,
    quant_trade_policy_artifact, research_profile_artifact,
};
use crate::{
    domain::quant::{RouteCandidateFunnel, RouteModelLineage, RouteRunOutcome},
    runtime_config::BuyModelRoute,
    types::{
        CalibrationArtifactId, ModelRunId, ModelVersionId, ReportRouteRunId, ReportRunId,
        ResearchProfileArtifactId, TradePolicyArtifactId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_report_route_run")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub report_route_run_id: ReportRouteRunId,
    pub report_run_id: ReportRunId,
    #[sea_orm(column_type = "JsonBinary")]
    pub route: BuyModelRoute,
    #[sea_orm(column_type = "JsonBinary")]
    pub outcome: RouteRunOutcome,
    pub model_version_id: Option<ModelVersionId>,
    pub model_run_id: Option<ModelRunId>,
    pub calibration_artifact_id: Option<CalibrationArtifactId>,
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    pub research_profile_artifact_id: Option<ResearchProfileArtifactId>,
    #[sea_orm(column_type = "JsonBinary")]
    pub lineage_json: Option<RouteModelLineage>,
    #[sea_orm(column_type = "JsonBinary")]
    pub funnel_json: RouteCandidateFunnel,
    pub diagnostic_code: Option<String>,
    pub finished_at: DateTime<Utc>,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ReportRun",
        from = "report_run_id",
        to = "report_run_id"
    )]
    pub report_run: BelongsTo<quant_report_run::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ModelVersion",
        from = "model_version_id",
        to = "model_version_id"
    )]
    pub model_version: BelongsTo<Option<quant_model_version::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ModelRun",
        from = "model_run_id",
        to = "model_run_id"
    )]
    pub model_run: BelongsTo<Option<quant_model_run::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "CalibrationArtifact",
        from = "calibration_artifact_id",
        to = "artifact_id"
    )]
    pub calibration_artifact: BelongsTo<Option<quant_calibration_artifact::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "TradePolicyArtifact",
        from = "trade_policy_artifact_id",
        to = "artifact_id"
    )]
    pub trade_policy_artifact: BelongsTo<Option<quant_trade_policy_artifact::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ResearchProfileArtifact",
        from = "research_profile_artifact_id",
        to = "research_profile_artifact_id"
    )]
    pub research_profile_artifact: BelongsTo<Option<research_profile_artifact::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
