//! Recommendation report persistence DTOs.
//!
//! Payload columns are strong-typed value objects (`types::report_payload`)
//! serialized into the existing JSONB columns — never a bare `serde_json::Value`.

use chrono::{DateTime, Utc};
use sea_orm::DeriveIntoActiveModel;
use serde::{Deserialize, Serialize};

use crate::{
    entities::{
        quant_recommendation, quant_recommendation::Model as RecommendationModel,
        quant_recommendation_report, quant_recommendation_report::Model,
    },
    enums::quant::{
        AccountSource, OutcomeSide, QuantRuntimeMode, RecommendationReportStatus,
        RecommendationStatus, ReportKind,
    },
    types::{
        AccountSnapshotId, Bps, DecisionPolicySnapshotId, EquitySnapshotId, EventId, EvidenceRefs,
        ExecutionEligibility, MarketContext, MarketId, MarketSelectionId, ModelRunId,
        ModelVersionId, PortfolioPlanId, Probability, RecommendationFactorBreakdown,
        RecommendationId, RecommendationIdentity, RecommendationReportId, RecommendationTradePlan,
        ReportDataQualitySnapshotId, ReportSummary, ResearchProfileArtifactId, ResearchProfileId,
        ResearchProfileRef, TokenId, Usd,
    },
};

/// Immutable `TopN` recommendation report row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecommendationReportInfo {
    pub recommendation_report_id: RecommendationReportId,
    pub profile_id: ResearchProfileId,
    pub profile_ref: ResearchProfileRef,
    pub report_kind: ReportKind,
    pub decision_at: DateTime<Utc>,
    pub horizon_secs: i64,
    pub runtime_mode: QuantRuntimeMode,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
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
    pub successor_report_id: Option<RecommendationReportId>,
    pub superseded_at: Option<DateTime<Utc>>,
    pub obsoleted_at: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub status_reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl From<Model> for RecommendationReportInfo {
    fn from(model: Model) -> Self {
        let profile_ref = model.research_profile_artifact_id.profile_ref();
        Self {
            recommendation_report_id: model.recommendation_report_id,
            profile_id: profile_ref.id.clone(),
            profile_ref,
            report_kind: model.report_kind,
            decision_at: model.decision_at,
            horizon_secs: model.horizon_secs,
            runtime_mode: model.runtime_mode,
            decision_policy_snapshot_id: model.decision_policy_snapshot_id,
            model_run_id: model.model_run_id,
            model_version_id: model.model_version_id,
            market_selection_id: model.market_selection_id,
            portfolio_plan_id: model.portfolio_plan_id,
            top_n: model.top_n,
            status: model.status,
            account_source: model.account_source,
            capital_base_usd: model.capital_base_usd,
            account_snapshot_ref: model.account_snapshot_ref,
            equity_snapshot_ref: model.equity_snapshot_ref,
            data_quality_snapshot_ref: model.data_quality_snapshot_ref,
            summary_json: model.summary_json,
            published_at: model.published_at,
            successor_report_id: model.successor_report_id,
            superseded_at: model.superseded_at,
            obsoleted_at: model.obsoleted_at,
            valid_until: model.valid_until,
            revoked_at: model.revoked_at,
            expired_at: model.expired_at,
            status_reason: model.status_reason,
            created_at: model.created_at,
        }
    }
}

/// Insert payload for `quant_recommendation_report`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_recommendation_report::ActiveModel")]
pub struct NewRecommendationReport {
    pub recommendation_report_id: RecommendationReportId,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
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
    pub summary_json: ReportSummary,
    pub published_at: Option<DateTime<Utc>>,
    pub successor_report_id: Option<RecommendationReportId>,
    pub superseded_at: Option<DateTime<Utc>>,
    pub obsoleted_at: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub status_reason: Option<String>,
}

/// Single actionable recommendation row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecommendationInfo {
    pub recommendation_id: RecommendationId,
    pub profile_ref: ResearchProfileRef,
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
    pub status_changed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl From<RecommendationModel> for RecommendationInfo {
    fn from(model: RecommendationModel) -> Self {
        Self {
            recommendation_id: model.recommendation_id,
            profile_ref: model.research_profile_artifact_id.profile_ref(),
            recommendation_report_id: model.recommendation_report_id,
            rank: model.rank,
            market_id: model.market_id,
            event_id: model.event_id,
            token_id: model.token_id,
            outcome_side: model.outcome_side,
            composite_score: model.composite_score,
            risk_adjusted_score: model.risk_adjusted_score,
            confidence: model.confidence,
            expected_return_bps: model.expected_return_bps,
            downside_bps: model.downside_bps,
            identity: model.identity,
            market_context: model.market_context,
            rank_before_portfolio: model.rank_before_portfolio,
            liquidity_score: model.liquidity_score,
            data_quality_score: model.data_quality_score,
            model_score_percentile: model.model_score_percentile,
            trade_plan: model.trade_plan,
            factor_breakdown: model.factor_breakdown,
            evidence_refs: model.evidence_refs,
            execution_eligibility: model.execution_eligibility,
            valid_from: model.valid_from,
            valid_until: model.valid_until,
            status: model.status,
            status_changed_at: model.status_changed_at,
            created_at: model.created_at,
        }
    }
}

/// Insert payload for `quant_recommendation`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_recommendation::ActiveModel")]
pub struct NewRecommendation {
    pub recommendation_id: RecommendationId,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
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
