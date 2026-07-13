//! Recommendation report persistence DTOs.
//!
//! Payload columns are strong-typed value objects (`types::report_payload`)
//! serialized into the existing JSONB columns — never a bare `serde_json::Value`.

use crate::{
    entities::quant_recommendation_report,
    enums::quant::{
        AccountSource, OutcomeSide, QuantRuntimeMode, RecommendationReportStatus,
        RecommendationStatus, ReportKind, ReportTriggerKind,
    },
    types::{
        AccountSnapshotId, Bps, EquitySnapshotId, EventId, EvidenceRefs, ExecutionEligibility,
        MarketContext, MarketId, MarketSelectionId, ModelRunId, ModelVersionId, PortfolioPlanId,
        Probability, RecommendationFactorBreakdown, RecommendationId, RecommendationIdentity,
        RecommendationReportId, RecommendationTradePlan, ReportDataQualitySnapshotId,
        ReportSummary, RuntimeConfigVersionId, TokenId, Usd,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Immutable `TopN` recommendation report row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_recommendation_report::Entity")]
pub struct RecommendationReportInfo {
    pub recommendation_report_id: RecommendationReportId,
    pub report_kind: ReportKind,
    pub trigger_kind: ReportTriggerKind,
    pub trigger_key: String,
    pub trigger_time: DateTime<Utc>,
    pub knowledge_lag_secs: i64,
    pub decision_at: DateTime<Utc>,
    pub horizon_secs: i64,
    pub runtime_mode: QuantRuntimeMode,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Exact serving run that produced this report. Empty reports that stop
    /// before model inference carry `None`.
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
    pub summary_json: ReportSummary,
    pub published_at: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub status_reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    RecommendationReportInfo,
    quant_recommendation_report::Model,
    {
        recommendation_report_id,
        report_kind,
        trigger_kind,
        trigger_key,
        trigger_time,
        knowledge_lag_secs,
        decision_at,
        horizon_secs,
        runtime_mode,
        runtime_config_version_id,
        model_run_id,
        model_version_id,
        market_selection_id,
        portfolio_plan_id,
        top_n,
        status,
        account_source,
        capital_base_usd,
        account_snapshot_ref,
        equity_snapshot_ref,
        data_quality_snapshot_ref,
        summary_json,
        published_at,
        valid_until,
        revoked_at,
        expired_at,
        status_reason,
        created_at,
    }
);

/// Insert payload for `quant_recommendation_report`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_recommendation_report::ActiveModel")]
pub struct NewRecommendationReport {
    pub recommendation_report_id: RecommendationReportId,
    pub report_kind: ReportKind,
    pub trigger_kind: ReportTriggerKind,
    pub trigger_key: String,
    pub trigger_time: DateTime<Utc>,
    pub knowledge_lag_secs: i64,
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
    pub summary_json: ReportSummary,
    pub published_at: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub status_reason: Option<String>,
}

/// Single actionable recommendation row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_recommendation::Entity")]
pub struct RecommendationInfo {
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub rank: i32,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub composite_score: Probability,
    pub risk_adjusted_score: Probability,
    pub confidence: Probability,
    pub expected_return_bps: Bps,
    pub downside_bps: Bps,
    pub identity: RecommendationIdentity,
    pub market_context: MarketContext,
    pub rank_before_portfolio: i32,
    pub liquidity_score: Probability,
    pub data_quality_score: Probability,
    pub model_score_percentile: Probability,
    pub trade_plan: RecommendationTradePlan,
    pub factor_breakdown: RecommendationFactorBreakdown,
    pub evidence_refs: EvidenceRefs,
    pub execution_eligibility: ExecutionEligibility,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub status: RecommendationStatus,
    pub created_at: DateTime<Utc>,
}

info_from_model!(RecommendationInfo, crate::entities::quant_recommendation::Model, {
    recommendation_id, recommendation_report_id, rank, market_id, event_id, token_id,
    outcome_side, composite_score, risk_adjusted_score, confidence, expected_return_bps,
    downside_bps, identity, market_context, rank_before_portfolio, liquidity_score,
    data_quality_score, model_score_percentile, trade_plan,
    factor_breakdown, evidence_refs, execution_eligibility, valid_from, valid_until, status,
    created_at,
});

/// Insert payload for `quant_recommendation`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_recommendation::ActiveModel")]
pub struct NewRecommendation {
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub rank: i32,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub composite_score: Probability,
    pub risk_adjusted_score: Probability,
    pub confidence: Probability,
    pub expected_return_bps: Bps,
    pub downside_bps: Bps,
    pub identity: RecommendationIdentity,
    pub market_context: MarketContext,
    pub rank_before_portfolio: i32,
    pub liquidity_score: Probability,
    pub data_quality_score: Probability,
    pub model_score_percentile: Probability,
    pub trade_plan: RecommendationTradePlan,
    pub factor_breakdown: RecommendationFactorBreakdown,
    pub evidence_refs: EvidenceRefs,
    pub execution_eligibility: ExecutionEligibility,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub status: RecommendationStatus,
}
