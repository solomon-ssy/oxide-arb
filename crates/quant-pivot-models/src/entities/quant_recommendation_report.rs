//! `quant_recommendation_report` table entity.

use crate::{
    enums::quant::{
        AccountSource, QuantRuntimeMode, RecommendationReportStatus, ReportKind, ReportTriggerKind,
    },
    types::{
        AccountSnapshotId, MarketSelectionId, ModelVersionId, PortfolioPlanId,
        RecommendationReportId, ReportDataQualitySnapshotId, ReportSummary, RuntimeConfigVersionId,
        Usd,
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
    pub trigger_kind: ReportTriggerKind,
    pub trigger_key: String,
    pub trigger_time: DateTime<Utc>,
    pub source_delay_secs: i64,
    pub as_of: DateTime<Utc>,
    pub horizon_secs: i64,
    pub runtime_mode: QuantRuntimeMode,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub model_version_id: ModelVersionId,
    pub market_selection_id: MarketSelectionId,
    pub portfolio_plan_id: PortfolioPlanId,
    pub top_n: i32,
    pub status: RecommendationReportStatus,
    pub account_source: AccountSource,
    pub capital_base_usd: Usd,
    pub account_snapshot_ref: AccountSnapshotId,
    pub data_quality_snapshot_ref: ReportDataQualitySnapshotId,
    #[sea_orm(column_type = "JsonBinary")]
    pub summary_json: ReportSummary,
    pub published_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub status_reason: Option<String>,
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
        belongs_to = "super::quant_market_selection::Entity",
        from = "Column::MarketSelectionId",
        to = "super::quant_market_selection::Column::MarketSelectionId"
    )]
    MarketSelection,
    #[sea_orm(
        belongs_to = "super::quant_portfolio_plan::Entity",
        from = "Column::PortfolioPlanId",
        to = "super::quant_portfolio_plan::Column::PortfolioPlanId"
    )]
    PortfolioPlan,
    #[sea_orm(
        belongs_to = "super::quant_account_snapshot::Entity",
        from = "Column::AccountSnapshotRef",
        to = "super::quant_account_snapshot::Column::AccountSnapshotId"
    )]
    AccountSnapshot,
    #[sea_orm(
        belongs_to = "super::quant_report_data_quality_snapshot::Entity",
        from = "Column::DataQualitySnapshotRef",
        to = "super::quant_report_data_quality_snapshot::Column::ReportDataQualitySnapshotId"
    )]
    DataQualitySnapshot,
    #[sea_orm(has_many = "super::quant_recommendation::Entity")]
    Recommendation,
}

impl Related<super::quant_model_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelVersion.def()
    }
}

impl Related<super::quant_market_selection::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::MarketSelection.def()
    }
}

impl Related<super::quant_portfolio_plan::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PortfolioPlan.def()
    }
}

impl Related<super::quant_account_snapshot::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::AccountSnapshot.def()
    }
}

impl Related<super::quant_report_data_quality_snapshot::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::DataQualitySnapshot.def()
    }
}

impl Related<super::quant_recommendation::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Recommendation.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
