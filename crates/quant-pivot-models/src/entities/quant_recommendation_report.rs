//! `quant_recommendation_report` table entity.

use crate::{
    enums::quant::{AccountSource, QuantRuntimeMode, RecommendationReportStatus, ReportKind},
    types::{
        AccountSnapshotId, EquitySnapshotId, MarketSelectionId, ModelRunId, ModelVersionId,
        PortfolioPlanId, RecommendationReportId, ReportDataQualitySnapshotId, ReportSummary,
        ResearchProfileRef, RuntimeConfigVersionId, Usd,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_recommendation_report")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub recommendation_report_id: RecommendationReportId,
    pub profile_id: String,
    #[sea_orm(column_type = "JsonBinary")]
    pub profile_ref: ResearchProfileRef,
    pub report_kind: ReportKind,
    pub decision_at: DateTime<Utc>,
    pub horizon_secs: i64,
    pub runtime_mode: QuantRuntimeMode,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub model_run_id: Option<ModelRunId>,
    pub model_version_id: ModelVersionId,
    pub market_selection_id: MarketSelectionId,
    pub portfolio_plan_id: PortfolioPlanId,
    pub top_n: i32,
    pub status: RecommendationReportStatus,
    pub account_source: AccountSource,
    pub capital_base_usd: Usd,
    pub account_snapshot_ref: AccountSnapshotId,
    pub equity_snapshot_ref: EquitySnapshotId,
    pub data_quality_snapshot_ref: ReportDataQualitySnapshotId,
    #[sea_orm(column_type = "JsonBinary")]
    pub summary_json: ReportSummary,
    pub published_at: Option<DateTime<Utc>>,
    pub successor_report_id: Option<RecommendationReportId>,
    pub superseded_at: Option<DateTime<Utc>>,
    pub obsoleted_at: Option<DateTime<Utc>>,
    /// Data-driven validity deadline = `max(recommendation.valid_until)` over the
    /// report's recommendations, frozen at publish (`None` for a report with no
    /// recommendations only when no fallback applies). This is the report's
    /// roll-up "actionable until" instant — distinct from `expired_at` (the event
    /// timestamp of when it was actually transitioned to `Expired`).
    pub valid_until: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub status_reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "Entity",
        from = "Column::SuccessorReportId",
        to = "Column::RecommendationReportId"
    )]
    Successor,
    #[sea_orm(
        belongs_to = "super::quant_model_version::Entity",
        from = "Column::ModelVersionId",
        to = "super::quant_model_version::Column::ModelVersionId"
    )]
    ModelVersion,
    #[sea_orm(
        belongs_to = "super::quant_model_run::Entity",
        from = "Column::ModelRunId",
        to = "super::quant_model_run::Column::ModelRunId"
    )]
    ModelRun,
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
        belongs_to = "super::quant_equity_snapshot::Entity",
        from = "Column::EquitySnapshotRef",
        to = "super::quant_equity_snapshot::Column::EquitySnapshotId"
    )]
    EquitySnapshot,
    #[sea_orm(
        belongs_to = "super::quant_report_data_quality_snapshot::Entity",
        from = "Column::DataQualitySnapshotRef",
        to = "super::quant_report_data_quality_snapshot::Column::ReportDataQualitySnapshotId"
    )]
    DataQualitySnapshot,
    #[sea_orm(has_many = "super::quant_recommendation::Entity")]
    Recommendation,
    #[sea_orm(has_one = "super::quant_report_fact_delivery::Entity")]
    FactDelivery,
}

impl Related<super::quant_model_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelVersion.def()
    }
}

impl Related<super::quant_model_run::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelRun.def()
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

impl Related<super::quant_equity_snapshot::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::EquitySnapshot.def()
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

impl Related<super::quant_report_fact_delivery::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::FactDelivery.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
