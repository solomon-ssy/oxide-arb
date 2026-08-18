//! Recommendation report persistence DTOs.
//!
//! Payload columns are strong-typed value objects (`types::report_payload`)
//! serialized into the existing JSONB columns — never a bare `serde_json::Value`.

use chrono::{DateTime, Utc};
use sea_orm::{ActiveValue::Set, DeriveIntoActiveModel, IntoActiveModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::{
        quant_recommendation,
        quant_recommendation::Model as RecommendationModel,
        quant_recommendation_report::{ActiveModel as RecommendationReportActiveModel, Model},
    },
    enums::quant::{
        AccountSource, OutcomeSide, RecommendationReportStatus, RecommendationStatus, ReportKind,
    },
    runtime_config::BuyModelRoute,
    types::{
        AccountSnapshotId, ContentHash, DecisionPolicySnapshotId, EconomicTierId, EquitySnapshotId,
        EventId, EvidenceRefs, ExecutionEligibility, MarketContext, MarketId, MarketSelectionId,
        PortfolioPlanId, PortfolioScenarioArtifactId, RecommendationFactorBreakdown,
        RecommendationId, RecommendationIdentity, RecommendationReportId, RecommendationTradePlan,
        ReportDataQualitySnapshotId, ReportRouteRunId, ReportRunId, ReportSummary, TokenId, Usd,
    },
};

use super::{ExecutableEconomicTier, RecommendationEconomics, RepresentedRouteSet};

/// Immutable `TopN` recommendation report row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecommendationReportInfo {
    pub recommendation_report_id: RecommendationReportId,
    pub report_run_id: ReportRunId,
    pub report_kind: ReportKind,
    pub decision_at: DateTime<Utc>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub market_selection_id: MarketSelectionId,
    pub portfolio_plan_id: PortfolioPlanId,
    pub represented_routes_json: RepresentedRouteSet,
    pub scenario_artifact_id: Option<PortfolioScenarioArtifactId>,
    pub scenario_artifact_hash: Option<ContentHash>,
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
    /// Immutable availability clock committed with the report and its parity proof.
    pub created_at: DateTime<Utc>,
}

impl From<Model> for RecommendationReportInfo {
    fn from(model: Model) -> Self {
        Self {
            recommendation_report_id: model.recommendation_report_id,
            report_run_id: model.report_run_id,
            report_kind: model.report_kind,
            decision_at: model.decision_at,
            decision_policy_snapshot_id: model.decision_policy_snapshot_id,
            market_selection_id: model.market_selection_id,
            portfolio_plan_id: model.portfolio_plan_id,
            represented_routes_json: model.represented_routes_json,
            scenario_artifact_id: model.scenario_artifact_id,
            scenario_artifact_hash: model.scenario_artifact_hash,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewRecommendationReport {
    pub recommendation_report_id: RecommendationReportId,
    pub report_run_id: ReportRunId,
    pub report_kind: ReportKind,
    pub decision_at: DateTime<Utc>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub market_selection_id: MarketSelectionId,
    pub portfolio_plan_id: PortfolioPlanId,
    pub represented_routes_json: RepresentedRouteSet,
    pub scenario_artifact_id: Option<PortfolioScenarioArtifactId>,
    pub scenario_artifact_hash: Option<ContentHash>,
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
    /// Immutable availability clock committed with the report and its parity proof.
    pub created_at: DateTime<Utc>,
}

impl IntoActiveModel<RecommendationReportActiveModel> for NewRecommendationReport {
    fn into_active_model(self) -> RecommendationReportActiveModel {
        let Self {
            recommendation_report_id,
            report_run_id,
            report_kind,
            decision_at,
            decision_policy_snapshot_id,
            market_selection_id,
            portfolio_plan_id,
            represented_routes_json,
            scenario_artifact_id,
            scenario_artifact_hash,
            top_n,
            status,
            account_source,
            capital_base_usd,
            account_snapshot_ref,
            equity_snapshot_ref,
            data_quality_snapshot_ref,
            summary_json,
            published_at,
            successor_report_id,
            superseded_at,
            obsoleted_at,
            valid_until,
            revoked_at,
            expired_at,
            status_reason,
            created_at,
        } = self;
        RecommendationReportActiveModel {
            recommendation_report_id: Set(recommendation_report_id),
            report_run_id: Set(report_run_id),
            report_kind: Set(report_kind),
            decision_at: Set(decision_at),
            decision_policy_snapshot_id: Set(decision_policy_snapshot_id),
            market_selection_id: Set(market_selection_id),
            portfolio_plan_id: Set(portfolio_plan_id),
            represented_routes_json: Set(represented_routes_json),
            scenario_artifact_id: Set(scenario_artifact_id),
            scenario_artifact_hash: Set(scenario_artifact_hash),
            top_n: Set(top_n),
            status: Set(status),
            account_source: Set(account_source),
            capital_base_usd: Set(capital_base_usd),
            account_snapshot_ref: Set(account_snapshot_ref),
            equity_snapshot_ref: Set(equity_snapshot_ref),
            data_quality_snapshot_ref: Set(data_quality_snapshot_ref),
            summary_json: Set(summary_json),
            published_at: Set(published_at),
            successor_report_id: Set(successor_report_id),
            superseded_at: Set(superseded_at),
            obsoleted_at: Set(obsoleted_at),
            valid_until: Set(valid_until),
            revoked_at: Set(revoked_at),
            expired_at: Set(expired_at),
            status_reason: Set(status_reason),
            created_at: Set(created_at),
        }
    }
}

/// Single actionable recommendation row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecommendationInfo {
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub report_route_run_id: ReportRouteRunId,
    pub portfolio_plan_id: PortfolioPlanId,
    pub economic_tier_id: EconomicTierId,
    pub rank: i32,
    pub route: BuyModelRoute,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub economics_json: RecommendationEconomics,
    pub economic_tier_json: ExecutableEconomicTier,
    pub identity: RecommendationIdentity,
    pub market_context: MarketContext,
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
            recommendation_report_id: model.recommendation_report_id,
            report_route_run_id: model.report_route_run_id,
            portfolio_plan_id: model.portfolio_plan_id,
            economic_tier_id: model.economic_tier_id,
            rank: model.rank,
            route: model.route,
            market_id: model.market_id,
            event_id: model.event_id,
            token_id: model.token_id,
            outcome_side: model.outcome_side,
            economics_json: model.economics_json,
            economic_tier_json: model.economic_tier_json,
            identity: model.identity,
            market_context: model.market_context,
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
    pub recommendation_report_id: RecommendationReportId,
    pub report_route_run_id: ReportRouteRunId,
    pub portfolio_plan_id: PortfolioPlanId,
    pub economic_tier_id: EconomicTierId,
    pub rank: i32,
    pub route: BuyModelRoute,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub economics_json: RecommendationEconomics,
    pub economic_tier_json: ExecutableEconomicTier,
    pub identity: RecommendationIdentity,
    pub market_context: MarketContext,
    pub trade_plan: RecommendationTradePlan,
    pub factor_breakdown: RecommendationFactorBreakdown,
    pub evidence_refs: EvidenceRefs,
    pub execution_eligibility: ExecutionEligibility,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub status: RecommendationStatus,
    /// Immutable availability clock used by point-in-time feedback queries.
    pub created_at: DateTime<Utc>,
}
