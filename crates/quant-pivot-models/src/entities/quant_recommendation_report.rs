//! `quant_recommendation_report` table entity.

use crate::{
    enums::quant::{QuantRuntimeMode, RecommendationReportStatus, ReportKind},
    types::{
        ModelVersionId, PortfolioPlanId, RecommendationReportId, RuntimeConfigVersionId,
        UniverseSnapshotId,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_recommendation_report")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub recommendation_report_id: RecommendationReportId,
    pub report_kind: ReportKind,
    pub as_of: DateTime<Utc>,
    pub horizon_secs: i64,
    pub runtime_mode: QuantRuntimeMode,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub model_version_id: ModelVersionId,
    pub universe_snapshot_id: UniverseSnapshotId,
    pub portfolio_plan_id: PortfolioPlanId,
    pub top_n: i32,
    pub status: RecommendationReportStatus,
    #[sea_orm(column_type = "JsonBinary")]
    pub summary_json: Json,
    pub published_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_model_version::Entity",
        from = "Column::ModelVersionId",
        to = "super::quant_model_version::Column::ModelVersionId"
    )]
    ModelVersion,
    #[sea_orm(
        belongs_to = "super::quant_universe_snapshot::Entity",
        from = "Column::UniverseSnapshotId",
        to = "super::quant_universe_snapshot::Column::UniverseSnapshotId"
    )]
    UniverseSnapshot,
    #[sea_orm(
        belongs_to = "super::quant_portfolio_plan::Entity",
        from = "Column::PortfolioPlanId",
        to = "super::quant_portfolio_plan::Column::PortfolioPlanId"
    )]
    PortfolioPlan,
    #[sea_orm(has_many = "super::quant_recommendation::Entity")]
    Recommendation,
}

impl Related<super::quant_model_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelVersion.def()
    }
}

impl Related<super::quant_universe_snapshot::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::UniverseSnapshot.def()
    }
}

impl Related<super::quant_portfolio_plan::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PortfolioPlan.def()
    }
}

impl Related<super::quant_recommendation::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Recommendation.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
