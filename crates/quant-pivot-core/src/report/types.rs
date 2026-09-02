//! Shared report module DTOs.

use chrono::{DateTime, Utc};
use quant_pivot_error::QuantError;
use quant_pivot_models::{
    clickhouse::{QuantReportRecommendationFactRow, ReportMarketFunnelRow},
    domain::quant::ExecutableEconomicTier,
    domain::quant::NewReportTransaction,
    enums::quant::{EmptyReportReason, OutcomeSide, ReportKind, ReportTriggerKind},
    runtime_config::BuyModelRoute,
    runtime_config::ReportDeliveryPolicy,
    types::{
        Bps, ContentHash, CorrelationId, EconomicTierId, MarketId, ModelRunId, ModelVersionId,
        Price, RecommendationPolicyProvenance, RecommendationReportId, ReportFunnelDiagnostics,
        ReportFunnelReason, ReportRouteRunId, ReportRunId, ReportScheduleId, ReportTriggerKey,
        ResearchFeatureContract, ResearchProfileRef, SemanticTextError, Shares, TradePolicyCohort,
        TradePolicyCohortProvenance, Usd,
    },
};
use quant_pivot_research::{
    execution_semantics::PitMakerRebateUnavailableReason, model::SignalCandidate,
    portfolio::TierAdmissionRejectionCode,
};

/// Source that triggered one report build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReportTrigger {
    /// Scheduled generation by schedule id.
    Scheduled { schedule_id: ReportScheduleId },
    /// Operator/API requested ad-hoc generation by stable request id.
    AdHoc { request_id: CorrelationId },
}

impl ReportTrigger {
    /// Stable trigger kind persisted on the report header.
    #[must_use]
    pub const fn kind(&self) -> ReportTriggerKind {
        match self {
            Self::Scheduled { .. } => ReportTriggerKind::Scheduled,
            Self::AdHoc { .. } => ReportTriggerKind::AdHoc,
        }
    }

    /// Fixed idempotency key contract.
    pub fn key(&self, trigger_time: DateTime<Utc>) -> Result<ReportTriggerKey, SemanticTextError> {
        let value = match self {
            Self::Scheduled { schedule_id } => {
                format!("scheduled:{schedule_id}:{}", trigger_time.to_rfc3339())
            }
            Self::AdHoc { request_id } => format!("ad_hoc:{request_id}"),
        };
        ReportTriggerKey::parse(value)
    }
}

/// Builder input after lifecycle idempotency resolution.
#[derive(Debug, Clone)]
pub struct BuildReportRequest {
    /// Durable report-attempt identity used by every per-Route diagnostic row.
    pub report_run_id: ReportRunId,
    pub trigger: ReportTrigger,
    pub trigger_time: DateTime<Utc>,
    pub top_n_override: Option<u32>,
    pub knowledge_lag_secs_override: Option<u64>,
}

/// Context carried when a report is intentionally empty.
#[derive(Debug, Clone)]
pub struct EmptyReportContext {
    pub reason: EmptyReportReason,
    pub candidate_count: u32,
    pub rejected_count: u32,
    pub warnings: Vec<String>,
}

/// Exact recommendation regime behind a globally selected live-L2 tier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannedRecommendationContract {
    FullL2 {
        provenance: TradePolicyCohortProvenance,
        cohort: Box<TradePolicyCohort>,
    },
    Bootstrap {
        profile_ref: ResearchProfileRef,
        feature_contract: ResearchFeatureContract,
        recommendation_contract_hash: ContentHash,
        cash_budget_tier: Usd,
        reference_horizon_secs: u64,
        max_slippage_bps: Bps,
        min_depth_usd: Usd,
        max_book_age_ms: u64,
    },
}

impl PlannedRecommendationContract {
    #[must_use]
    pub fn provenance(&self) -> RecommendationPolicyProvenance {
        match self {
            Self::FullL2 { provenance, .. } => provenance.clone().into(),
            Self::Bootstrap {
                profile_ref,
                feature_contract,
                recommendation_contract_hash,
                ..
            } => RecommendationPolicyProvenance::BootstrapProfile {
                profile_ref: profile_ref.clone(),
                feature_contract: *feature_contract,
                recommendation_contract_hash: *recommendation_contract_hash,
            },
        }
    }

    #[must_use]
    pub fn release_secs(&self) -> u64 {
        match self {
            Self::FullL2 { cohort, .. } => cohort.vertical_barrier_secs,
            Self::Bootstrap {
                reference_horizon_secs,
                ..
            } => *reference_horizon_secs,
        }
    }
}

/// One globally selected live-L2 tier enriched with its exact Route/model lineage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedReportRecommendation {
    pub rank: u32,
    pub route: BuyModelRoute,
    pub report_route_run_id: ReportRouteRunId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: ModelRunId,
    pub candidate: SignalCandidate,
    pub tier: ExecutableEconomicTier,
    pub contract: PlannedRecommendationContract,
    pub entry_limit_price: Price,
}

/// Report-funnel projection of one tier rejected before or by the global MILP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportTierRejection {
    pub economic_tier_id: EconomicTierId,
    pub market_id: MarketId,
    pub code: TierAdmissionRejectionCode,
    pub diagnostics: Option<ReportFunnelDiagnostics>,
}

impl ReportTierRejection {
    /// Enforce the one canonical diagnostics shape owned by the rejection code.
    pub fn validate(&self) -> Result<(), &'static str> {
        match (self.code, &self.diagnostics) {
            (
                TierAdmissionRejectionCode::ProfitProbabilityFloor,
                Some(
                    diagnostics @ ReportFunnelDiagnostics::ProfitProbabilityFloor {
                        economic_tier_id,
                        ..
                    },
                ),
            ) if *economic_tier_id == self.economic_tier_id => {
                diagnostics.validate_for(ReportFunnelReason::ProfitProbabilityBelowFloor)
            }
            (TierAdmissionRejectionCode::ProfitProbabilityFloor, Some(_)) => {
                Err("probability rejection has mismatched diagnostics")
            }
            (TierAdmissionRejectionCode::ProfitProbabilityFloor, None) => {
                Err("probability rejection has no diagnostics")
            }
            (_, None) => Ok(()),
            (_, Some(_)) => Err("non-probability rejection has probability diagnostics"),
        }
    }
}

/// Typed reason why no executable economic tier could be built for a market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EconomicTierBuildRejection {
    ExecutionEconomicsUnavailable {
        market_id: MarketId,
    },
    PassiveMakerRebateUnavailable {
        market_id: MarketId,
        reason: PitMakerRebateUnavailableReason,
    },
    BelowMinimumOrderSize {
        market_id: MarketId,
        requested: Shares,
        minimum: Shares,
    },
    InsufficientLiveDepth {
        market_id: MarketId,
        visible_usd: Usd,
        required_usd: Usd,
        limit_price: Price,
    },
}

impl EconomicTierBuildRejection {
    #[must_use]
    pub const fn market_id(&self) -> &MarketId {
        match self {
            Self::ExecutionEconomicsUnavailable { market_id }
            | Self::PassiveMakerRebateUnavailable { market_id, .. }
            | Self::BelowMinimumOrderSize { market_id, .. }
            | Self::InsufficientLiveDepth { market_id, .. } => market_id,
        }
    }
}

/// One recommendation summarized for an operator notification (`TopN` preview).
#[derive(Debug, Clone)]
pub struct NotificationRecommendation {
    pub market_id: String,
    pub outcome_side: OutcomeSide,
    pub route: BuyModelRoute,
    pub profit_probability_bps: Bps,
    pub robust_expected_net_usd: Usd,
    pub marginal_portfolio_value_usd: Usd,
    pub hard_reserved_cash_usd: Usd,
}

/// Operator-facing notification payload for a committed report.
#[derive(Debug, Clone)]
pub struct ReportNotificationPayload {
    pub report_id: RecommendationReportId,
    pub kind: ReportKind,
    pub status: String,
    pub published_count: u32,
    pub total_hard_reserved_cash_usd: Usd,
    pub top3: Vec<NotificationRecommendation>,
    pub warnings: Vec<String>,
    pub empty_reason: Option<EmptyReportReason>,
}

/// Complete report artifact ready for atomic PG write and post-commit publish.
#[derive(Debug, Clone)]
pub struct ComposedReport {
    pub transaction: NewReportTransaction,
    pub ch_rows: Vec<QuantReportRecommendationFactRow>,
    pub funnel_rows: Vec<ReportMarketFunnelRow>,
    pub notification: ReportNotificationPayload,
    pub delivery_policy: ReportDeliveryPolicy,
    pub notify_operators: bool,
}

/// Stable operator-facing summary for durable/API error projections.
///
/// Raw dependency diagnostics may contain credentials, signed URLs, query
/// parameters, or host paths. They remain in correlated structured logs and
/// must never be copied into `PostgreSQL` rows returned by the report API.
pub(super) fn durable_report_error_summary(error: &QuantError) -> String {
    format!(
        "{} failure; inspect correlated structured logs",
        error.code()
    )
}

#[cfg(test)]
mod tests {
    use quant_pivot_error::{QuantError, report::ReportError};

    use super::durable_report_error_summary;

    #[test]
    fn error_never_persists_diagnostic() {
        let error = QuantError::from(ReportError::InvariantViolation {
            stage: "test",
            detail: "secret-token=must-not-persist".to_owned(),
        });

        let summary = durable_report_error_summary(&error);
        assert_eq!(
            summary,
            "report failure; inspect correlated structured logs"
        );
        assert!(!summary.contains("must-not-persist"));
    }
}
