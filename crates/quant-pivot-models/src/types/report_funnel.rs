//! Row-level conserved report-market funnel semantics.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::types::{NullReason, stable_name::FeatureName};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportFunnelStage {
    CatalogVisible,
    BusinessEligible,
    ExecutableDataEligible,
    FeatureReady,
    ModelScored,
    ModelGatePassed,
    PolicyReady,
    SizingEligible,
    PortfolioFunded,
    Published,
}

impl ReportFunnelStage {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CatalogVisible => "catalog_visible",
            Self::BusinessEligible => "business_eligible",
            Self::ExecutableDataEligible => "executable_data_eligible",
            Self::FeatureReady => "feature_ready",
            Self::ModelScored => "model_scored",
            Self::ModelGatePassed => "model_gate_passed",
            Self::PolicyReady => "policy_ready",
            Self::SizingEligible => "sizing_eligible",
            Self::PortfolioFunded => "portfolio_funded",
            Self::Published => "published",
        }
    }

    pub const ALL: [Self; 10] = [
        Self::CatalogVisible,
        Self::BusinessEligible,
        Self::ExecutableDataEligible,
        Self::FeatureReady,
        Self::ModelScored,
        Self::ModelGatePassed,
        Self::PolicyReady,
        Self::SizingEligible,
        Self::PortfolioFunded,
        Self::Published,
    ];
}

impl FromStr for ReportFunnelStage {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|stage| stage.as_str() == value)
            .ok_or_else(|| format!("unknown report funnel stage `{value}`"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportFunnelReason {
    NotOpen,
    CategoryDisabled,
    ResolutionAmbiguous,
    ManuallyBlocked,
    InsufficientLiquidity,
    SpreadTooWide,
    StaleBook,
    IngestLagExceeded,
    ModelFeatureUnavailable,
    FeatureDataQualityRejected,
    MissingModelOutput,
    ScoreBelowFloor,
    LowConfidence,
    NoPositiveSignal,
    InvalidEdgeInputs,
    ReturnModelUncalibrated,
    ExecutableEntryUnavailable,
    TradePolicyUnavailable,
    BelowMinSize,
    LiquidityInfeasible,
    BudgetExhausted,
    MarketCapExhausted,
    EventCapExhausted,
    CategoryCapExhausted,
    CorrelationCapExhausted,
    AvailableCashExhausted,
    AggregateExposureCapExhausted,
    BeyondTopN,
    SystemDegraded,
    Published,
}

impl ReportFunnelReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotOpen => "not_open",
            Self::CategoryDisabled => "category_disabled",
            Self::ResolutionAmbiguous => "resolution_ambiguous",
            Self::ManuallyBlocked => "manually_blocked",
            Self::InsufficientLiquidity => "insufficient_liquidity",
            Self::SpreadTooWide => "spread_too_wide",
            Self::StaleBook => "stale_book",
            Self::IngestLagExceeded => "ingest_lag_exceeded",
            Self::ModelFeatureUnavailable => "model_feature_unavailable",
            Self::FeatureDataQualityRejected => "feature_data_quality_rejected",
            Self::MissingModelOutput => "missing_model_output",
            Self::ScoreBelowFloor => "score_below_floor",
            Self::LowConfidence => "low_confidence",
            Self::NoPositiveSignal => "no_positive_signal",
            Self::InvalidEdgeInputs => "invalid_edge_inputs",
            Self::ReturnModelUncalibrated => "return_model_uncalibrated",
            Self::ExecutableEntryUnavailable => "executable_entry_unavailable",
            Self::TradePolicyUnavailable => "trade_policy_unavailable",
            Self::BelowMinSize => "below_min_size",
            Self::LiquidityInfeasible => "liquidity_infeasible",
            Self::BudgetExhausted => "budget_exhausted",
            Self::MarketCapExhausted => "market_cap_exhausted",
            Self::EventCapExhausted => "event_cap_exhausted",
            Self::CategoryCapExhausted => "category_cap_exhausted",
            Self::CorrelationCapExhausted => "correlation_cap_exhausted",
            Self::AvailableCashExhausted => "available_cash_exhausted",
            Self::AggregateExposureCapExhausted => "aggregate_exposure_cap_exhausted",
            Self::BeyondTopN => "beyond_top_n",
            Self::SystemDegraded => "system_degraded",
            Self::Published => "published",
        }
    }
}

impl FromStr for ReportFunnelReason {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        [
            Self::NotOpen,
            Self::CategoryDisabled,
            Self::ResolutionAmbiguous,
            Self::ManuallyBlocked,
            Self::InsufficientLiquidity,
            Self::SpreadTooWide,
            Self::StaleBook,
            Self::IngestLagExceeded,
            Self::ModelFeatureUnavailable,
            Self::FeatureDataQualityRejected,
            Self::MissingModelOutput,
            Self::ScoreBelowFloor,
            Self::LowConfidence,
            Self::NoPositiveSignal,
            Self::InvalidEdgeInputs,
            Self::ReturnModelUncalibrated,
            Self::ExecutableEntryUnavailable,
            Self::TradePolicyUnavailable,
            Self::BelowMinSize,
            Self::LiquidityInfeasible,
            Self::BudgetExhausted,
            Self::MarketCapExhausted,
            Self::EventCapExhausted,
            Self::CategoryCapExhausted,
            Self::CorrelationCapExhausted,
            Self::AvailableCashExhausted,
            Self::AggregateExposureCapExhausted,
            Self::BeyondTopN,
            Self::SystemDegraded,
            Self::Published,
        ]
        .into_iter()
        .find(|reason| reason.as_str() == value)
        .ok_or_else(|| format!("unknown report funnel reason `{value}`"))
    }
}

/// Closed, reason-specific diagnostics for one terminal report-funnel row.
///
/// This document crosses the `ClickHouse` boundary as canonical JSON text, but
/// its schema is fully owned by this system. `None` is explicit so callers do
/// not overload `{}` with several incompatible meanings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum ReportFunnelDiagnostics {
    None {},
    MissingModelFeatures {
        features: Vec<FeatureName>,
    },
    FeatureDataQuality {
        missing: Vec<MissingFeatureDiagnostic>,
    },
    PlannerRejection {
        detail: String,
    },
}

impl ReportFunnelDiagnostics {
    /// Reject a diagnostics payload whose shape disagrees with the row's
    /// authoritative primary reason.
    pub fn validate_for(&self, reason: ReportFunnelReason) -> Result<(), &'static str> {
        let valid = match self {
            Self::None {} => !matches!(
                reason,
                ReportFunnelReason::ModelFeatureUnavailable
                    | ReportFunnelReason::FeatureDataQualityRejected
                    | ReportFunnelReason::BelowMinSize
                    | ReportFunnelReason::LiquidityInfeasible
                    | ReportFunnelReason::BudgetExhausted
                    | ReportFunnelReason::MarketCapExhausted
                    | ReportFunnelReason::EventCapExhausted
                    | ReportFunnelReason::CategoryCapExhausted
                    | ReportFunnelReason::CorrelationCapExhausted
                    | ReportFunnelReason::AvailableCashExhausted
                    | ReportFunnelReason::AggregateExposureCapExhausted
                    | ReportFunnelReason::BeyondTopN
            ),
            Self::MissingModelFeatures { features } => {
                reason == ReportFunnelReason::ModelFeatureUnavailable && !features.is_empty()
            }
            Self::FeatureDataQuality { missing } => {
                reason == ReportFunnelReason::FeatureDataQualityRejected && !missing.is_empty()
            }
            Self::PlannerRejection { detail } => {
                matches!(
                    reason,
                    ReportFunnelReason::BelowMinSize
                        | ReportFunnelReason::LiquidityInfeasible
                        | ReportFunnelReason::BudgetExhausted
                        | ReportFunnelReason::MarketCapExhausted
                        | ReportFunnelReason::EventCapExhausted
                        | ReportFunnelReason::CategoryCapExhausted
                        | ReportFunnelReason::CorrelationCapExhausted
                        | ReportFunnelReason::AvailableCashExhausted
                        | ReportFunnelReason::AggregateExposureCapExhausted
                        | ReportFunnelReason::BeyondTopN
                ) && !detail.trim().is_empty()
            }
        };
        valid
            .then_some(())
            .ok_or("report funnel diagnostics do not match primary reason")
    }
}

/// One required feature rejected by the governed data-quality policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissingFeatureDiagnostic {
    pub feature_name: FeatureName,
    pub reason: NullReason,
}

#[cfg(test)]
mod tests {
    use super::{ReportFunnelDiagnostics, ReportFunnelReason};

    #[test]
    fn diagnostics_bound_primary_reason() {
        let diagnostics = ReportFunnelDiagnostics::PlannerRejection {
            detail: "portfolio cap reached".to_owned(),
        };
        assert!(
            diagnostics
                .validate_for(ReportFunnelReason::BudgetExhausted)
                .is_ok()
        );
        assert!(
            diagnostics
                .validate_for(ReportFunnelReason::Published)
                .is_err()
        );
        assert!(
            ReportFunnelDiagnostics::None {}
                .validate_for(ReportFunnelReason::FeatureDataQualityRejected)
                .is_err()
        );
    }

    #[test]
    fn diagnostics_reject_unknown_fields() {
        let result = serde_json::from_value::<ReportFunnelDiagnostics>(serde_json::json!({
            "kind": "none",
            "legacy_detail": "must not be ignored"
        }));
        assert!(result.is_err());
    }
}
