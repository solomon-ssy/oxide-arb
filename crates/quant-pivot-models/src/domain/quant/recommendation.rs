//! Recommendation report persistence DTOs.
//!
//! Payload columns are strong-typed value objects (`types::report_payload`)
//! serialized into the existing JSONB columns — never a bare `serde_json::Value`.

use crate::{
    enums::quant::{
        AccountSource, QuantRuntimeMode, RecommendationReportStatus, RecommendationStatus,
        ReportKind, SignalSide,
    },
    types::{
        AccountSnapshotId, EntryPlan, EventId, EvidenceRefs, ExecutionEligibility, ExitPlan,
        MarketId, MarketSelectionId, ModelVersionId, PortfolioPlanId, Probability,
        RecommendationFactorBreakdown, RecommendationId, RecommendationReportId, ReportSummary,
        RiskEnvelope, RuntimeConfigVersionId, SizingPlan, TokenId, Usd,
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
    pub summary_json: ReportSummary,
    pub published_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    RecommendationReportInfo,
    crate::entities::quant_recommendation_report::Model,
    {
        recommendation_report_id,
        report_kind,
        as_of,
        horizon_secs,
        runtime_mode,
        runtime_config_version_id,
        model_version_id,
        market_selection_id,
        portfolio_plan_id,
        top_n,
        status,
        account_source,
        capital_base_usd,
        account_snapshot_ref,
        summary_json,
        published_at,
        revoked_at,
        created_at,
    }
);

/// Insert payload for `quant_recommendation_report`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_recommendation_report::ActiveModel")]
pub struct NewRecommendationReport {
    pub recommendation_report_id: RecommendationReportId,
    pub report_kind: ReportKind,
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
    pub summary_json: ReportSummary,
    pub published_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
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
    pub side: SignalSide,
    pub composite_score: Probability,
    pub risk_adjusted_score: Probability,
    pub confidence: Probability,
    pub entry_plan: EntryPlan,
    pub sizing_plan: SizingPlan,
    pub exit_plan: ExitPlan,
    pub risk_envelope: RiskEnvelope,
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
    side, composite_score, risk_adjusted_score, confidence, entry_plan, sizing_plan,
    exit_plan, risk_envelope, factor_breakdown, evidence_refs, execution_eligibility,
    valid_from, valid_until, status, created_at,
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
    pub side: SignalSide,
    pub composite_score: Probability,
    pub risk_adjusted_score: Probability,
    pub confidence: Probability,
    pub entry_plan: EntryPlan,
    pub sizing_plan: SizingPlan,
    pub exit_plan: ExitPlan,
    pub risk_envelope: RiskEnvelope,
    pub factor_breakdown: RecommendationFactorBreakdown,
    pub evidence_refs: EvidenceRefs,
    pub execution_eligibility: ExecutionEligibility,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub status: RecommendationStatus,
}
