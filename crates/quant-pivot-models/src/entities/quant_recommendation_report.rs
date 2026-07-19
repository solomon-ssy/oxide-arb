//! `quant_recommendation_report` table entity.

use crate::{
    enums::quant::{AccountSource, QuantRuntimeMode, RecommendationReportStatus, ReportKind},
    types::{
        AccountSnapshotId, DecisionPolicySnapshotId, EquitySnapshotId, MarketSelectionId,
        ModelRunId, ModelVersionId, PortfolioPlanId, RecommendationReportId,
        ReportDataQualitySnapshotId, ReportSummary, ResearchProfileRef, Usd,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
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
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
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

    #[sea_orm(
        self_ref,
        relation_enum = "Successor",
        from = "successor_report_id",
        to = "recommendation_report_id"
    )]
    pub successor: BelongsTo<Option<Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ModelVersion",
        from = "model_version_id",
        to = "model_version_id"
    )]
    pub model_version: BelongsTo<super::quant_model_version::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ModelRun",
        from = "model_run_id",
        to = "model_run_id"
    )]
    pub model_run: BelongsTo<Option<super::quant_model_run::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "MarketSelection",
        from = "market_selection_id",
        to = "market_selection_id"
    )]
    pub market_selection: BelongsTo<super::quant_market_selection::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "PortfolioPlan",
        from = "portfolio_plan_id",
        to = "portfolio_plan_id"
    )]
    pub portfolio_plan: BelongsTo<super::quant_portfolio_plan::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "AccountSnapshot",
        from = "account_snapshot_ref",
        to = "account_snapshot_id"
    )]
    pub account_snapshot: BelongsTo<super::quant_account_snapshot::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "EquitySnapshot",
        from = "equity_snapshot_ref",
        to = "equity_snapshot_id"
    )]
    pub equity_snapshot: BelongsTo<super::quant_equity_snapshot::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "DataQualitySnapshot",
        from = "data_quality_snapshot_ref",
        to = "report_data_quality_snapshot_id"
    )]
    pub data_quality_snapshot: BelongsTo<super::quant_report_data_quality_snapshot::Entity>,
    #[sea_orm(has_many, relation_enum = "Recommendation")]
    pub recommendation: HasMany<super::quant_recommendation::Entity>,
    #[sea_orm(has_one, relation_enum = "FactDelivery")]
    pub fact_delivery: HasOne<super::quant_report_fact_delivery::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
