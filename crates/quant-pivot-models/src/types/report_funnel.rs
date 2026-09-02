//! Row-level conserved report-market funnel semantics.

use std::str::FromStr;

use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    enums::quant::DataQualityStatus,
    types::{
        Bps, ContentHash, EconomicTierId, NullReason, PortfolioScenarioArtifactId, Price, Usd,
        stable_name::FeatureName,
    },
};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReportFunnelReason {
    NotOpen,
    CategoryDisabled,
    RouteNotActivated,
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
    ExecutableEntryUnavailable,
    ExecutionEconomicsUnavailable,
    InsufficientLiveDepth,
    ScenarioExitCapacityInsufficient,
    NominalExpectedNetBelowFloor,
    RobustExpectedNetBelowFloor,
    ProfitProbabilityBelowFloor,
    ProbabilityIntervalTooWide,
    LiquidityBufferInsufficient,
    SingleRecommendationExposureExceeded,
    ExistingStructuralConflict,
    NotSelectedByGlobalOptimum,
    Published,
}

impl ReportFunnelReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotOpen => "not_open",
            Self::CategoryDisabled => "category_disabled",
            Self::RouteNotActivated => "route_not_activated",
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
            Self::ExecutableEntryUnavailable => "executable_entry_unavailable",
            Self::ExecutionEconomicsUnavailable => "execution_economics_unavailable",
            Self::InsufficientLiveDepth => "insufficient_live_depth",
            Self::ScenarioExitCapacityInsufficient => "scenario_exit_capacity_insufficient",
            Self::NominalExpectedNetBelowFloor => "nominal_expected_net_below_floor",
            Self::RobustExpectedNetBelowFloor => "robust_expected_net_below_floor",
            Self::ProfitProbabilityBelowFloor => "profit_probability_below_floor",
            Self::ProbabilityIntervalTooWide => "probability_interval_too_wide",
            Self::LiquidityBufferInsufficient => "liquidity_buffer_insufficient",
            Self::SingleRecommendationExposureExceeded => "single_recommendation_exposure_exceeded",
            Self::ExistingStructuralConflict => "existing_structural_conflict",
            Self::NotSelectedByGlobalOptimum => "not_selected_by_global_optimum",
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
            Self::RouteNotActivated,
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
            Self::ExecutableEntryUnavailable,
            Self::ExecutionEconomicsUnavailable,
            Self::InsufficientLiveDepth,
            Self::ScenarioExitCapacityInsufficient,
            Self::NominalExpectedNetBelowFloor,
            Self::RobustExpectedNetBelowFloor,
            Self::ProfitProbabilityBelowFloor,
            Self::ProbabilityIntervalTooWide,
            Self::LiquidityBufferInsufficient,
            Self::SingleRecommendationExposureExceeded,
            Self::ExistingStructuralConflict,
            Self::NotSelectedByGlobalOptimum,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum ReportFunnelDiagnostics {
    None {},
    MissingModelFeatures {
        #[schemars(with = "Vec<String>")]
        features: Vec<FeatureName>,
    },
    FeatureDataQuality {
        status: DataQualityStatus,
        missing_required: Vec<MissingFeatureDiagnostic>,
    },
    PlannerRejection {
        detail: String,
    },
    ProfitProbabilityFloor {
        economic_tier_id: EconomicTierId,
        scenario_artifact_id: PortfolioScenarioArtifactId,
        scenario_artifact_hash: ContentHash,
        nominal_profit_probability_bps: Bps,
        lower_profit_probability_bps: u32,
        minimum_profit_probability_bps: u32,
        probability_interval_width_bps: u32,
        maximum_probability_interval_width_bps: u32,
        nominal_expected_net_usd: Usd,
        robust_expected_net_usd: Usd,
    },
    InsufficientLiveDepth {
        visible_usd: Usd,
        required_usd: Usd,
        limit_price: Price,
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
                    | ReportFunnelReason::InsufficientLiveDepth
                    | ReportFunnelReason::ScenarioExitCapacityInsufficient
                    | ReportFunnelReason::NominalExpectedNetBelowFloor
                    | ReportFunnelReason::RobustExpectedNetBelowFloor
                    | ReportFunnelReason::ProfitProbabilityBelowFloor
                    | ReportFunnelReason::ProbabilityIntervalTooWide
                    | ReportFunnelReason::LiquidityBufferInsufficient
                    | ReportFunnelReason::SingleRecommendationExposureExceeded
                    | ReportFunnelReason::ExistingStructuralConflict
                    | ReportFunnelReason::NotSelectedByGlobalOptimum
            ),
            Self::MissingModelFeatures { features } => {
                reason == ReportFunnelReason::ModelFeatureUnavailable && !features.is_empty()
            }
            Self::FeatureDataQuality { status, .. } => {
                reason == ReportFunnelReason::FeatureDataQualityRejected
                    && *status == DataQualityStatus::Insufficient
            }
            Self::PlannerRejection { detail } => {
                matches!(
                    reason,
                    ReportFunnelReason::ScenarioExitCapacityInsufficient
                        | ReportFunnelReason::NominalExpectedNetBelowFloor
                        | ReportFunnelReason::RobustExpectedNetBelowFloor
                        | ReportFunnelReason::ProbabilityIntervalTooWide
                        | ReportFunnelReason::LiquidityBufferInsufficient
                        | ReportFunnelReason::SingleRecommendationExposureExceeded
                        | ReportFunnelReason::ExistingStructuralConflict
                        | ReportFunnelReason::NotSelectedByGlobalOptimum
                ) && !detail.trim().is_empty()
            }
            Self::ProfitProbabilityFloor {
                economic_tier_id,
                scenario_artifact_id,
                scenario_artifact_hash,
                nominal_profit_probability_bps,
                lower_profit_probability_bps,
                minimum_profit_probability_bps,
                probability_interval_width_bps,
                maximum_probability_interval_width_bps,
                nominal_expected_net_usd,
                robust_expected_net_usd,
            } => {
                let nominal = nominal_profit_probability_bps.inner();
                let lower = Decimal::from(*lower_profit_probability_bps);
                let upper = lower + Decimal::from(*probability_interval_width_bps);
                reason == ReportFunnelReason::ProfitProbabilityBelowFloor
                    && !economic_tier_id.as_uuid().is_nil()
                    && *scenario_artifact_hash != ContentHash::from_bytes([0; 32])
                    && *scenario_artifact_id
                        == PortfolioScenarioArtifactId::from_content_hash(scenario_artifact_hash)
                    && lower_profit_probability_bps < minimum_profit_probability_bps
                    && *minimum_profit_probability_bps <= 10_000
                    && *probability_interval_width_bps <= 10_000
                    && *maximum_probability_interval_width_bps <= 10_000
                    && (Decimal::ZERO..=Decimal::from(10_000_u32)).contains(&nominal)
                    && lower <= nominal
                    && nominal <= upper
                    && upper <= Decimal::from(10_000_u32)
                    && robust_expected_net_usd >= &Usd::ZERO
                    && nominal_expected_net_usd >= robust_expected_net_usd
            }
            Self::InsufficientLiveDepth {
                visible_usd,
                required_usd,
                limit_price,
            } => {
                reason == ReportFunnelReason::InsufficientLiveDepth
                    && visible_usd < required_usd
                    && limit_price.inner() > Decimal::ZERO
            }
        };
        valid
            .then_some(())
            .ok_or("report funnel diagnostics do not match primary reason")
    }
}

/// One required feature rejected by the governed data-quality policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MissingFeatureDiagnostic {
    #[schemars(with = "String")]
    pub feature_name: FeatureName,
    pub reason: NullReason,
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use crate::enums::quant::DataQualityStatus;

    use super::{
        Bps, ContentHash, EconomicTierId, PortfolioScenarioArtifactId, ReportFunnelDiagnostics,
        ReportFunnelReason, Usd,
    };

    #[test]
    fn diagnostics_bound_primary_reason() {
        let diagnostics = ReportFunnelDiagnostics::PlannerRejection {
            detail: "portfolio cap reached".to_owned(),
        };
        assert!(
            diagnostics
                .validate_for(ReportFunnelReason::RobustExpectedNetBelowFloor)
                .is_ok()
        );
        assert!(
            diagnostics
                .validate_for(ReportFunnelReason::ProfitProbabilityBelowFloor)
                .is_err()
        );
        assert!(
            diagnostics
                .validate_for(ReportFunnelReason::Published)
                .is_err()
        );
        let scenario_hash = ContentHash::from_bytes([7; 32]);
        let mut probability = ReportFunnelDiagnostics::ProfitProbabilityFloor {
            economic_tier_id: EconomicTierId::from_v7(),
            scenario_artifact_id: PortfolioScenarioArtifactId::from_content_hash(&scenario_hash),
            scenario_artifact_hash: scenario_hash,
            nominal_profit_probability_bps: Bps::new(dec!(5175)),
            lower_profit_probability_bps: 5_100,
            minimum_profit_probability_bps: 5_200,
            probability_interval_width_bps: 75,
            maximum_probability_interval_width_bps: 2_000,
            nominal_expected_net_usd: Usd::new(dec!(4.2)),
            robust_expected_net_usd: Usd::new(dec!(2.8)),
        };
        assert!(
            probability
                .validate_for(ReportFunnelReason::ProfitProbabilityBelowFloor)
                .is_ok()
        );
        assert!(
            probability
                .validate_for(ReportFunnelReason::RobustExpectedNetBelowFloor)
                .is_err()
        );
        let ReportFunnelDiagnostics::ProfitProbabilityFloor {
            maximum_probability_interval_width_bps,
            ..
        } = &mut probability
        else {
            panic!("probability fixture changed diagnostics kind");
        };
        *maximum_probability_interval_width_bps = 0;
        assert!(
            probability
                .validate_for(ReportFunnelReason::ProfitProbabilityBelowFloor)
                .is_ok()
        );
        assert!(
            ReportFunnelDiagnostics::None {}
                .validate_for(ReportFunnelReason::FeatureDataQualityRejected)
                .is_err()
        );
        assert!(
            ReportFunnelDiagnostics::FeatureDataQuality {
                status: DataQualityStatus::Insufficient,
                missing_required: Vec::new(),
            }
            .validate_for(ReportFunnelReason::FeatureDataQualityRejected)
            .is_ok()
        );
        assert!(
            ReportFunnelDiagnostics::FeatureDataQuality {
                status: DataQualityStatus::Degraded,
                missing_required: Vec::new(),
            }
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

    #[test]
    fn probability_diagnostics_fail_closed() {
        let scenario_hash = ContentHash::from_bytes([7; 32]);
        let diagnostics = ReportFunnelDiagnostics::ProfitProbabilityFloor {
            economic_tier_id: EconomicTierId::from_v7(),
            scenario_artifact_id: PortfolioScenarioArtifactId::from_content_hash(&scenario_hash),
            scenario_artifact_hash: scenario_hash,
            nominal_profit_probability_bps: Bps::new(dec!(5175)),
            lower_profit_probability_bps: 5_100,
            minimum_profit_probability_bps: 5_200,
            probability_interval_width_bps: 75,
            maximum_probability_interval_width_bps: 2_000,
            nominal_expected_net_usd: Usd::new(dec!(4.2)),
            robust_expected_net_usd: Usd::new(dec!(2.8)),
        };
        let baseline = serde_json::to_value(diagnostics).expect("serialize diagnostics fixture");
        let cases = [
            (
                "/economic_tier_id",
                serde_json::json!("00000000-0000-0000-0000-000000000000"),
            ),
            (
                "/scenario_artifact_id",
                serde_json::json!(PortfolioScenarioArtifactId::from_v7()),
            ),
            (
                "/scenario_artifact_hash",
                serde_json::json!(ContentHash::from_bytes([0; 32])),
            ),
            ("/nominal_profit_probability_bps", serde_json::json!("-1")),
            ("/nominal_profit_probability_bps", serde_json::json!("5000")),
            ("/nominal_profit_probability_bps", serde_json::json!("5300")),
            ("/lower_profit_probability_bps", serde_json::json!(5200)),
            ("/minimum_profit_probability_bps", serde_json::json!(10001)),
            ("/probability_interval_width_bps", serde_json::json!(6000)),
            ("/probability_interval_width_bps", serde_json::json!(10001)),
            (
                "/maximum_probability_interval_width_bps",
                serde_json::json!(10001),
            ),
            ("/nominal_expected_net_usd", serde_json::json!("2")),
            ("/robust_expected_net_usd", serde_json::json!("-0.1")),
        ];
        for (pointer, replacement) in cases {
            let mut value = baseline.clone();
            *value
                .pointer_mut(pointer)
                .expect("diagnostics fixture field") = replacement;
            let damaged: ReportFunnelDiagnostics =
                serde_json::from_value(value).expect("decode damaged diagnostics");
            assert!(
                damaged
                    .validate_for(ReportFunnelReason::ProfitProbabilityBelowFloor)
                    .is_err(),
                "damaged field {pointer} must fail closed"
            );
        }
    }
}
