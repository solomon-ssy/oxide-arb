//! Serialized model artifacts: the [`ModelArtifact`] enum and its family bodies.
//!
//! Covers the common header, the weighted-factor scorer body (weights + governed
//! multipliers + return model + substitution penalties), and content-addressed
//! (de)serialization.
//! Artifact **bytes** live in the [`ArtifactStore`](crate::artifact::ArtifactStore)
//! at a content-addressed key (`models/<artifact_hash_hex>.json`); Postgres stores
//! only the [`ContentHash`] + metadata. The hash is the canonical digest of the
//! deserialized artifact ([`ModelArtifact::content_hash`]), so a corrupted or
//! swapped byte stream is caught on load (recomputed hash ≠ recorded hash).
//!
//! Same-cross-section normalization remains a factor-engine concern. The
//! small-cross-section training reference is artifact state, however: the
//! weighted body freezes the empirical CDF so serving cannot drift with mutable
//! online history. Training fills [`ReturnModelSpec::Calibrated`] and
//! [`TrainingObjectiveReport`].

use std::collections::BTreeSet;

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::quant::ModelVersionInfo,
    enums::{
        common::MarketCategory,
        model::{ClassicalKind, ModelFamily},
        quant::{DataQualityStatus, DownsideSource, ModelSerializationFormat},
    },
    hashing::CanonicalDigest,
    runtime_config::{FactorCrossSectionConfig, SellScorerConfig, SmallCrossSectionPolicy},
    types::{
        ArtifactUri, CalibrationArtifactId, ContentHash, ModelInputContract,
        ModelInputRequiredness, ModelSpecId, ModelVersionId, Price, ResearchProfileRef,
        TradePolicyArtifactId,
        calibration::CalibratedPayoutDistribution,
        factor::{FactorDefinitionRef, FactorServingPlane},
        model_serving::{
            ModelServingContract, ModelServingEstimatorBinding, ModelServingEstimatorInput,
            ModelServingIntrinsicInputKind, ModelServingIntrinsicInputRef,
            ModelServingTreeShapBinding,
        },
        model_training::TrainingObjectiveSpec,
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    attribution::TreeShapModelContract,
    factors::FrozenReferenceQuantiles,
    features::{FeatureName, FeatureUnit, FeatureValueKind, NullReason},
    hashing::ResearchHasher,
    model::{
        calibrator::ResolvedCalibration,
        category_scope::validate_category_scope,
        factor_heads::FactorHeadSpec,
        objective::{ObjectiveComponentReport, RankingDiagnostics},
    },
    naming::stable_name,
    precision::RESEARCH_DECIMAL_SCALE,
};

/// Tolerance for the weight-normalization check (`|Σ weights − 1| ≤ ε`).
fn weight_sum_tolerance() -> Decimal {
    Decimal::new(1, 9)
}

/// Canonical identity of an ordered model-input contract.
///
/// Input order and requiredness are estimator semantics, so neither is sorted
/// or projected away before hashing.
pub fn model_input_contract_hash(contract: &ModelInputContract) -> QuantResult<ContentHash> {
    if contract.inputs.is_empty() {
        return Err(ResearchError::InvalidModelArtifact {
            detail: "model input contract must contain at least one raw feature".to_owned(),
        }
        .into());
    }
    contract
        .validate()
        .map_err(|detail| ResearchError::InvalidModelArtifact {
            detail: format!("invalid model input contract: {detail}"),
        })?;
    ResearchHasher::canonical(contract)
}

fn required_features_from_contract(contract: &ModelInputContract) -> Vec<FeatureName> {
    contract
        .inputs
        .iter()
        .filter(|input| input.requiredness == ModelInputRequiredness::Required)
        .map(|input| FeatureName::new(input.feature_name.clone()))
        .collect()
}

fn input_features_from_contract(contract: &ModelInputContract) -> Vec<FeatureName> {
    contract
        .inputs
        .iter()
        .map(|input| FeatureName::new(input.feature_name.clone()))
        .collect()
}

/// Common model-artifact header.
///
/// The complete immutable serving contract is the sole source of serving
/// identity and lineage. Scalar projections are exposed only through semantic
/// getters; they are never serialized in parallel with the contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelArtifactHeader {
    serving_contract: ModelServingContract,
}

impl ModelArtifactHeader {
    #[must_use]
    pub const fn serving_contract(&self) -> &ModelServingContract {
        &self.serving_contract
    }

    #[must_use]
    pub const fn model_version_id(&self) -> ModelVersionId {
        self.serving_contract.bindings().model.model_version_id
    }

    #[must_use]
    pub const fn model_spec_id(&self) -> ModelSpecId {
        self.serving_contract.bindings().model.model_spec_id
    }

    #[must_use]
    pub const fn model_spec_definition_hash(&self) -> ContentHash {
        self.serving_contract
            .bindings()
            .model
            .model_spec_definition_hash
    }

    #[must_use]
    pub const fn profile_ref(&self) -> &ResearchProfileRef {
        &self.serving_contract.bindings().model.profile_ref
    }

    #[must_use]
    pub const fn model_family(&self) -> ModelFamily {
        self.serving_contract.bindings().model.model_family
    }

    #[must_use]
    pub const fn category_scope(&self) -> Option<MarketCategory> {
        self.serving_contract.bindings().model.category_scope
    }

    #[must_use]
    pub const fn prediction_horizon_secs(&self) -> u64 {
        self.serving_contract
            .bindings()
            .model
            .prediction_horizon_secs
    }

    #[must_use]
    pub const fn feature_schema_hash(&self) -> ContentHash {
        self.serving_contract.bindings().schemas.feature_schema_hash
    }

    #[must_use]
    pub const fn factor_schema_hash(&self) -> ContentHash {
        self.serving_contract
            .bindings()
            .factors
            .plane
            .factor_schema_hash()
    }

    #[must_use]
    pub const fn trade_policy_artifact_id(&self) -> Option<TradePolicyArtifactId> {
        match &self.serving_contract.bindings().trade_policy {
            Some(binding) => Some(binding.artifact_id),
            None => None,
        }
    }

    #[must_use]
    pub const fn trade_policy_hash(&self) -> Option<ContentHash> {
        match &self.serving_contract.bindings().trade_policy {
            Some(binding) => Some(binding.content_hash),
            None => None,
        }
    }
}

/// Governed multipliers applied to the base weighted score by data-quality
/// status. Each value is a `[0, 1]` multiplier; lower means a heavier penalty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataQualityMultipliers {
    /// Multiplier for [`DataQualityStatus::Fresh`].
    pub fresh: Decimal,
    /// Multiplier for [`DataQualityStatus::Acceptable`].
    pub acceptable: Decimal,
    /// Multiplier for [`DataQualityStatus::Degraded`].
    pub degraded: Decimal,
    /// Multiplier for [`DataQualityStatus::Stale`].
    pub stale: Decimal,
    /// Multiplier for [`DataQualityStatus::Insufficient`].
    pub insufficient: Decimal,
}

impl DataQualityMultipliers {
    /// The multiplier for a data-quality status (exhaustive, fail-closed).
    #[must_use]
    pub const fn multiplier_for(&self, status: DataQualityStatus) -> Decimal {
        match status {
            DataQualityStatus::Fresh => self.fresh,
            DataQualityStatus::Acceptable => self.acceptable,
            DataQualityStatus::Degraded => self.degraded,
            DataQualityStatus::Stale => self.stale,
            DataQualityStatus::Insufficient => self.insufficient,
        }
    }

    /// Conservative defaults: fresh data is unpenalized, quality decays the score.
    #[must_use]
    pub fn conservative() -> Self {
        Self {
            fresh: Decimal::ONE,
            acceptable: Decimal::new(95, 2),
            degraded: Decimal::new(70, 2),
            stale: Decimal::new(40, 2),
            insufficient: Decimal::ZERO,
        }
    }
}

/// One liquidity tier of a [`LiquidityMultipliers`] step function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiquidityTier {
    /// Inclusive lower bound, in visible-liquidity USD, for this tier.
    pub min_liquidity_usd: Decimal,
    /// `[0, 1]` multiplier applied at or above the bound.
    pub multiplier: Decimal,
}

/// Governed liquidity multiplier: a step function over visible-liquidity USD.
///
/// The highest tier whose `min_liquidity_usd` is `≤` the market's liquidity wins;
/// unknown or sub-threshold liquidity uses [`Self::floor`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiquidityMultipliers {
    /// Tiers in ascending threshold order.
    pub tiers: Vec<LiquidityTier>,
    /// Multiplier for unknown / below-lowest-tier liquidity.
    pub floor: Decimal,
}

impl LiquidityMultipliers {
    /// The multiplier for an optional visible-liquidity amount.
    #[must_use]
    pub fn multiplier_for(&self, liquidity_usd: Option<Decimal>) -> Decimal {
        let Some(liquidity) = liquidity_usd else {
            return self.floor;
        };
        self.tiers
            .iter()
            .rev()
            .find(|tier| liquidity >= tier.min_liquidity_usd)
            .map_or(self.floor, |tier| tier.multiplier)
    }

    /// Conservative defaults: thin books are penalized, deep books unpenalized.
    #[must_use]
    pub fn conservative() -> Self {
        Self {
            tiers: vec![
                LiquidityTier {
                    min_liquidity_usd: Decimal::from(1_000),
                    multiplier: Decimal::new(60, 2),
                },
                LiquidityTier {
                    min_liquidity_usd: Decimal::from(10_000),
                    multiplier: Decimal::new(85, 2),
                },
                LiquidityTier {
                    min_liquidity_usd: Decimal::from(50_000),
                    multiplier: Decimal::ONE,
                },
            ],
            floor: Decimal::new(30, 2),
        }
    }
}

/// Governed horizon multiplier by `time_to_resolution / prediction_horizon` ratio.
///
/// Markets that resolve far outside the model's prediction horizon are penalized;
/// markets within the acceptable window are unpenalized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HorizonMultipliers {
    /// Multiplier when the ratio is within `[min_ratio, max_ratio]`.
    pub in_window: Decimal,
    /// Multiplier when the ratio is below `min_ratio` (resolves too soon).
    pub too_soon: Decimal,
    /// Multiplier when the ratio is above `max_ratio` (resolves too late).
    pub too_late: Decimal,
    /// Lower ratio bound of the acceptable window.
    pub min_ratio: Decimal,
    /// Upper ratio bound of the acceptable window.
    pub max_ratio: Decimal,
}

impl HorizonMultipliers {
    /// The multiplier for an optional time-to-resolution against a prediction
    /// horizon. An unknown horizon or non-positive prediction horizon is treated
    /// as in-window (the horizon factor is simply not applied).
    #[must_use]
    pub fn multiplier_for(
        &self,
        time_to_resolution_secs: Option<u64>,
        prediction_horizon_secs: u64,
    ) -> Decimal {
        let (Some(ttr), true) = (time_to_resolution_secs, prediction_horizon_secs > 0) else {
            return self.in_window;
        };
        let ratio = Decimal::from(ttr) / Decimal::from(prediction_horizon_secs);
        if ratio < self.min_ratio {
            self.too_soon
        } else if ratio > self.max_ratio {
            self.too_late
        } else {
            self.in_window
        }
    }

    /// Conservative defaults: a market may resolve from half to four times the
    /// prediction horizon without penalty.
    #[must_use]
    pub fn conservative() -> Self {
        Self {
            in_window: Decimal::ONE,
            too_soon: Decimal::new(60, 2),
            too_late: Decimal::new(70, 2),
            min_ratio: Decimal::new(5, 1),
            max_ratio: Decimal::from(4),
        }
    }

    fn validate_horizon(&self) -> QuantResult<()> {
        for (label, value) in [
            ("horizon.in_window", self.in_window),
            ("horizon.too_soon", self.too_soon),
            ("horizon.too_late", self.too_late),
        ] {
            validate_unit_interval(label, value)?;
        }
        if self.min_ratio <= Decimal::ZERO || self.max_ratio < self.min_ratio {
            return Err(invalid_model_payload(
                "horizon ratios must satisfy 0 < min_ratio <= max_ratio".to_owned(),
            ));
        }
        Ok(())
    }
}

/// The three governed score multipliers a weighted runtime applies on top of the
/// base `Σ weightᵢ·normalizedᵢ·confidenceᵢ` magnitude.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScoreMultiplierSpec {
    /// Data-quality multiplier table.
    pub data_quality: DataQualityMultipliers,
    /// Liquidity multiplier step function.
    pub liquidity: LiquidityMultipliers,
    /// Horizon multiplier window.
    pub horizon: HorizonMultipliers,
}

impl ScoreMultiplierSpec {
    /// Conservative governed defaults suitable for a hand-authored bootstrap model.
    #[must_use]
    pub fn conservative() -> Self {
        Self {
            data_quality: DataQualityMultipliers::conservative(),
            liquidity: LiquidityMultipliers::conservative(),
            horizon: HorizonMultipliers::conservative(),
        }
    }
}

/// Governed confidence penalty applied per imputed / kept-missing feature value.
///
/// Each
/// audited substitution multiplies the candidate confidence by a declared
/// `[0, 1]` factor (1.0 = no penalty). The mapping is exhaustive over
/// [`NullReason`] so a new reason can never silently escape governance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubstitutionConfidenceRules {
    /// Penalty multiplier for [`NullReason::SourceUnavailable`].
    pub source_unavailable: Decimal,
    /// Penalty multiplier for [`NullReason::StaleBeyondPolicy`].
    pub stale_beyond_policy: Decimal,
    /// Penalty multiplier for [`NullReason::OutOfValidRange`].
    pub out_of_valid_range: Decimal,
    /// Penalty multiplier for [`NullReason::InsufficientHistory`].
    pub insufficient_history: Decimal,
    /// Penalty multiplier for [`NullReason::NotApplicable`] (structurally absent
    /// features are never imputed, so this is neutral by default).
    pub not_applicable: Decimal,
    /// Penalty multiplier for [`NullReason::LegBookMissing`].
    pub leg_book_missing: Decimal,
    /// Penalty multiplier for [`NullReason::FinalizedExecutionUnavailable`].
    pub execution_history_unavailable: Decimal,
    /// Penalty multiplier for [`NullReason::InsufficientExecutionHistory`].
    pub insufficient_execution_history: Decimal,
    /// Penalty multiplier for [`NullReason::InsufficientRoleCoverage`].
    pub insufficient_role_coverage: Decimal,
    /// Penalty multiplier for [`NullReason::DomainSourceUnavailable`].
    pub domain_source_unavailable: Decimal,
    /// Penalty multiplier for [`NullReason::LinkageUnresolved`].
    pub linkage_unresolved: Decimal,
}

impl SubstitutionConfidenceRules {
    /// The confidence multiplier for a substitution reason (exhaustive).
    #[must_use]
    pub const fn multiplier_for(&self, reason: NullReason) -> Decimal {
        match reason {
            NullReason::SourceUnavailable => self.source_unavailable,
            NullReason::StaleBeyondPolicy => self.stale_beyond_policy,
            NullReason::OutOfValidRange => self.out_of_valid_range,
            NullReason::InsufficientHistory => self.insufficient_history,
            NullReason::NotApplicable => self.not_applicable,
            NullReason::LegBookMissing => self.leg_book_missing,
            NullReason::FinalizedExecutionUnavailable => self.execution_history_unavailable,
            NullReason::InsufficientExecutionHistory => self.insufficient_execution_history,
            NullReason::InsufficientRoleCoverage => self.insufficient_role_coverage,
            NullReason::DomainSourceUnavailable => self.domain_source_unavailable,
            NullReason::LinkageUnresolved => self.linkage_unresolved,
        }
    }

    /// Conservative governed defaults: imputation always costs some confidence.
    #[must_use]
    pub fn conservative() -> Self {
        Self {
            source_unavailable: Decimal::new(80, 2),
            stale_beyond_policy: Decimal::new(85, 2),
            out_of_valid_range: Decimal::new(75, 2),
            insufficient_history: Decimal::new(85, 2),
            not_applicable: Decimal::ONE,
            leg_book_missing: Decimal::new(90, 2),
            execution_history_unavailable: Decimal::new(80, 2),
            insufficient_execution_history: Decimal::new(85, 2),
            insufficient_role_coverage: Decimal::new(90, 2),
            domain_source_unavailable: Decimal::new(80, 2),
            linkage_unresolved: Decimal::new(80, 2),
        }
    }
}

/// Uncalibrated, monotone heuristic return mapping — cold-start bootstrap only.
///
/// Auditable and explicitly flagged as a heuristic so candidates never present a
/// silently fabricated return; fail-closed at publish / auto-execution / semi-auto
/// This is enforced by `GateId::CalibrationRequired`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeuristicReturnModel {
    /// Expected return (bps) at `composite_score × confidence = 1`.
    pub max_expected_return_bps: Decimal,
    /// Downside (bps) bound at `composite_score × confidence = 0`; the realized
    /// downside scales down with conviction.
    pub max_downside_bps: Decimal,
}

/// A return model derived from a fitted `ProbabilityCalibrator`.
///
/// `calibrator_ref` names the [`CalibrationArtifactId`] (`kind = ModelScore`)
/// whose `P(win)` mapping and
/// reliability report the estimate is derived from; the artifact is resolved once
/// at runtime-load time (never re-fetched per candidate) into a
/// [`ResolvedCalibration`] passed to
/// [`ReturnModelSpec::estimate`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalibratedReturnModel {
    /// The bound `ProbabilityCalibrator` artifact.
    pub calibrator_ref: CalibrationArtifactId,
    /// Downside (bps) source read from the calibration artifact's reliability
    /// bins (v1: `MfeMae` only — see [`DownsideSource`]).
    pub downside_source: DownsideSource,
}

/// How a candidate's expected return / downside (bps) is produced from the ranking score.
///
/// Provenance is explicit: a candidate records whether its return is `Heuristic`
/// (uncalibrated, bootstrap-only) or `Calibrated` (independent held-out fit).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "calibration")]
pub enum ReturnModelSpec {
    /// Uncalibrated heuristic (cold-start bootstrap only).
    Heuristic(HeuristicReturnModel),
    /// Derived from an independently-fit `ProbabilityCalibrator`.
    Calibrated(CalibratedReturnModel),
}

/// The expected return and downside (both in basis points) a return model emits
/// for one candidate, with the calibration provenance that produced them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReturnEstimate {
    /// Expected return, in basis points.
    pub expected_return_bps: Decimal,
    /// Estimated downside, in basis points.
    pub downside_bps: Decimal,
    /// Whether the estimate came from a calibrated model (else heuristic).
    pub calibrated: bool,
    /// Calibrated loss/split/win payout distribution. `None` for an
    /// unpublishable heuristic bootstrap model.
    pub payout_distribution: Option<CalibratedPayoutDistribution>,
}

impl ReturnModelSpec {
    /// Estimate the expected return / downside (bps) for one candidate.
    ///
    /// `market_price` is the executable outcome-token reference price. The
    /// calibrated path computes `E[r] = E[terminal payout] / price - 1`, where
    /// terminal payout retains loss, split, and win mass. It is ignored by
    /// `Heuristic`. `calibration` must be `Some` whenever `self` is
    /// `Calibrated` — the verified serving preimage resolves it once before
    /// runtime construction and fails the load closed otherwise. A missing
    /// binding is rejected here as well; it never becomes a zero-valued
    /// business prediction.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::Inference`] when a calibrated model lacks its
    /// frozen calibration or the numeric calibration transform fails.
    pub fn estimate(
        &self,
        composite_score: Decimal,
        confidence: Decimal,
        market_price: Price,
        calibration: Option<&ResolvedCalibration>,
    ) -> QuantResult<ReturnEstimate> {
        match self {
            Self::Heuristic(model) => {
                let conviction = (composite_score * confidence).round_dp(RESEARCH_DECIMAL_SCALE);
                let expected_return_bps =
                    (model.max_expected_return_bps * conviction).round_dp(RESEARCH_DECIMAL_SCALE);
                let residual = (Decimal::ONE - conviction).max(Decimal::ZERO);
                let downside_bps =
                    (model.max_downside_bps * residual).round_dp(RESEARCH_DECIMAL_SCALE);
                Ok(ReturnEstimate {
                    expected_return_bps,
                    downside_bps,
                    calibrated: false,
                    payout_distribution: None,
                })
            }
            Self::Calibrated(_) => {
                let Some(resolved) = calibration else {
                    return Err(ResearchError::Inference {
                        detail: "calibrated return model is missing its frozen calibration"
                            .to_owned(),
                    }
                    .into());
                };
                resolved.estimate_return(composite_score, market_price)
            }
        }
    }

    /// A conservative heuristic default (cold-start bootstrap only).
    #[must_use]
    pub fn heuristic_default() -> Self {
        Self::Heuristic(HeuristicReturnModel {
            max_expected_return_bps: Decimal::from(300),
            max_downside_bps: Decimal::from(500),
        })
    }
}

/// Objective report produced by the weighted LTR trainer. `None` for a
/// hand-authored bootstrap artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingObjectiveReport {
    /// In-sample objective value (`-total_loss`).
    pub objective_value: Decimal,
    /// Frozen governed objective snapshot.
    pub spec: TrainingObjectiveSpec,
    /// Component-level loss decomposition.
    pub components: ObjectiveComponentReport,
    /// Ranking diagnostics (`Rank IC` + `NDCG@k`); not part of the loss.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<RankingDiagnostics>,
    /// Human-readable summary of the objective and result.
    pub summary: String,
}

/// Header-free `WeightedFactor` family payload.
///
/// Identity, schemas, dataset, category, horizon, and policy lineage live only
/// in the serving contract. This payload contains the executable preimages
/// whose hashes the contract commits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeightedFactorModelPayload {
    pub factor_head: FactorHeadSpec,
    pub input_contract: ModelInputContract,
    pub horizon_multipliers: HorizonMultipliers,
    pub substitution_confidence_rules: SubstitutionConfidenceRules,
    pub return_model: ReturnModelSpec,
    pub factor_cross_section: FactorCrossSectionConfig,
    pub frozen_reference_quantiles: FrozenReferenceQuantiles,
}

impl WeightedFactorModelPayload {
    #[must_use]
    pub fn required_features(&self) -> Vec<FeatureName> {
        required_features_from_contract(&self.input_contract)
    }

    pub fn input_transform_hash(&self) -> QuantResult<ContentHash> {
        #[derive(Serialize)]
        struct WeightedTransform<'a> {
            cross_section: &'a FactorCrossSectionConfig,
            frozen_reference_quantiles: &'a FrozenReferenceQuantiles,
        }

        ResearchHasher::canonical(&WeightedTransform {
            cross_section: &self.factor_cross_section,
            frozen_reference_quantiles: &self.frozen_reference_quantiles,
        })
    }

    pub fn model_payload_hash(&self) -> QuantResult<ContentHash> {
        typed_payload_hash(
            WEIGHTED_MODEL_PAYLOAD_HASH_DOMAIN,
            WEIGHTED_MODEL_PAYLOAD_VERSION,
            self,
        )
    }

    pub fn validate_for_plane(&self, plane: &FactorServingPlane) -> QuantResult<()> {
        self.factor_head.validate(plane)?;
        model_input_contract_hash(&self.input_contract)?;
        self.horizon_multipliers.validate_horizon()?;
        self.substitution_confidence_rules
            .validate_substitution_rules()?;
        validate_reference_quantiles(
            plane,
            &self.factor_cross_section,
            &self.frozen_reference_quantiles,
        )
    }
}

/// Governed Sell decision policy applied after the signed exit estimator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SellScorerOutputSpec {
    /// Expected exit alpha (bps over holding) at a net estimator score of one.
    pub max_exit_alpha_bps: Decimal,
    /// Positive logistic gain for the calibrated `P(exit_better)` projection.
    pub p_exit_gain: Decimal,
    /// Exit evidence at or below this non-negative threshold remains Hold.
    pub exit_deadband: Decimal,
    /// Governed cumulative exit target after the estimator clears the deadband.
    pub default_sell_pct: Decimal,
}

impl SellScorerOutputSpec {
    pub fn validate(&self) -> QuantResult<()> {
        if self.max_exit_alpha_bps <= Decimal::ZERO {
            return Err(invalid_model_payload(format!(
                "sell max_exit_alpha_bps must be positive, got {}",
                self.max_exit_alpha_bps
            )));
        }
        if self.p_exit_gain <= Decimal::ZERO {
            return Err(invalid_model_payload(format!(
                "sell p_exit_gain must be positive, got {}",
                self.p_exit_gain
            )));
        }
        if !(Decimal::ZERO..Decimal::ONE).contains(&self.exit_deadband) {
            return Err(invalid_model_payload(format!(
                "sell exit deadband must be in [0, 1), got {}",
                self.exit_deadband
            )));
        }
        if self.default_sell_pct <= Decimal::ZERO || self.default_sell_pct > Decimal::ONE {
            return Err(invalid_model_payload(format!(
                "sell default_sell_pct {} must be within (0, 1]",
                self.default_sell_pct
            )));
        }
        Ok(())
    }
}

impl TryFrom<&SellScorerConfig> for SellScorerOutputSpec {
    type Error = QuantError;

    /// Freeze the governed runtime-config preimage into artifact state.
    fn try_from(config: &SellScorerConfig) -> Result<Self, Self::Error> {
        let spec = Self {
            max_exit_alpha_bps: config.max_exit_alpha_bps.value(),
            p_exit_gain: config.p_exit_gain.value(),
            exit_deadband: config.exit_deadband.value(),
            default_sell_pct: config.default_sell_pct.value(),
        };
        spec.validate()?;
        Ok(spec)
    }
}

impl Default for SellScorerOutputSpec {
    fn default() -> Self {
        let config = SellScorerConfig::default();
        Self {
            max_exit_alpha_bps: config.max_exit_alpha_bps.value(),
            p_exit_gain: config.p_exit_gain.value(),
            exit_deadband: config.exit_deadband.value(),
            default_sell_pct: config.default_sell_pct.value(),
        }
    }
}

/// One exact content-addressed model-intrinsic Sell input weight.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SellIntrinsicWeight {
    pub binding: ModelServingIntrinsicInputRef,
    pub weight: Decimal,
}

/// Sell estimator composition.
///
/// The canonical market factor head is one input. The other four inputs are
/// model-intrinsic position features and can never masquerade as governed
/// factor definitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SellEstimatorSpec {
    pub market_head_weight: Decimal,
    pub intrinsic_weights: Vec<SellIntrinsicWeight>,
}

impl SellEstimatorSpec {
    pub fn validate(&self) -> QuantResult<()> {
        if self.market_head_weight < Decimal::ZERO {
            return Err(invalid_model_payload(format!(
                "sell market-head weight must be non-negative, got {}",
                self.market_head_weight
            )));
        }
        let expected_kinds = canonical_sell_intrinsic_kinds();
        if self.intrinsic_weights.len() != expected_kinds.len() {
            return Err(invalid_model_payload(
                "sell estimator must carry exactly four intrinsic inputs".to_owned(),
            ));
        }
        let mut sum = self.market_head_weight;
        for (weight, expected_kind) in self.intrinsic_weights.iter().zip(expected_kinds) {
            let expected_binding = ModelServingIntrinsicInputRef::try_from(expected_kind)
                .map_err(|error| invalid_model_payload(error.to_string()))?;
            if weight.binding != expected_binding {
                return Err(invalid_model_payload(format!(
                    "sell intrinsic inputs are not in canonical order at {expected_kind:?}"
                )));
            }
            if weight.weight < Decimal::ZERO {
                return Err(invalid_model_payload(format!(
                    "sell intrinsic {:?} has negative weight {}",
                    expected_kind, weight.weight
                )));
            }
            sum += weight.weight;
        }
        validate_simplex_sum("sell estimator", sum)
    }
}

impl TryFrom<&SellScorerConfig> for SellEstimatorSpec {
    type Error = QuantError;

    /// Freeze the exact governed market/intrinsic composition.
    fn try_from(config: &SellScorerConfig) -> Result<Self, Self::Error> {
        let weights = [
            config.position_take_profit_weight.value(),
            config.position_stop_loss_weight.value(),
            config.position_time_in_trade_weight.value(),
            config.position_peak_drawdown_weight.value(),
        ];
        let intrinsic_weights = canonical_sell_intrinsic_kinds()
            .into_iter()
            .zip(weights)
            .map(|(kind, weight)| {
                Ok(SellIntrinsicWeight {
                    binding: ModelServingIntrinsicInputRef::try_from(kind)
                        .map_err(|error| invalid_model_payload(error.to_string()))?,
                    weight,
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let spec = Self {
            market_head_weight: config.market_head_weight.value(),
            intrinsic_weights,
        };
        spec.validate()?;
        Ok(spec)
    }
}

/// Header-free `HoldVsExitWeighted` family payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SellScorerPayload {
    pub factor_head: FactorHeadSpec,
    pub estimator: SellEstimatorSpec,
    pub output_spec: SellScorerOutputSpec,
    pub input_contract: ModelInputContract,
    pub factor_cross_section: FactorCrossSectionConfig,
    pub frozen_reference_quantiles: FrozenReferenceQuantiles,
}

impl SellScorerPayload {
    #[must_use]
    pub fn required_features(&self) -> Vec<FeatureName> {
        required_features_from_contract(&self.input_contract)
    }

    pub fn input_transform_hash(&self) -> QuantResult<ContentHash> {
        #[derive(Serialize)]
        struct SellTransform<'a> {
            cross_section: &'a FactorCrossSectionConfig,
            frozen_reference_quantiles: &'a FrozenReferenceQuantiles,
        }

        ResearchHasher::canonical(&SellTransform {
            cross_section: &self.factor_cross_section,
            frozen_reference_quantiles: &self.frozen_reference_quantiles,
        })
    }

    pub fn model_payload_hash(&self) -> QuantResult<ContentHash> {
        typed_payload_hash(
            SELL_MODEL_PAYLOAD_HASH_DOMAIN,
            SELL_MODEL_PAYLOAD_VERSION,
            self,
        )
    }

    pub fn validate_for_plane(&self, plane: &FactorServingPlane) -> QuantResult<()> {
        self.factor_head.validate(plane)?;
        self.estimator.validate()?;
        model_input_contract_hash(&self.input_contract)?;
        self.output_spec.validate()?;
        validate_reference_quantiles(
            plane,
            &self.factor_cross_section,
            &self.frozen_reference_quantiles,
        )
    }
}

stable_name! {
    /// Name of a model-ready encoded column.
    ///
    /// This is intentionally a different type from [`FeatureName`]: missingness
    /// indicators are transform outputs, never governed source features and
    /// therefore must never leak into selection requirements.
    EncodedColumnName
}

/// Kind of model-ready column emitted for one raw feature input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EncodedColumnKind {
    /// Unit-normalized, imputed, then standardized numeric value.
    NumericValue,
    /// One frozen-vocabulary one-hot category value.
    CategoryValue,
    /// Explicit bucket for an observed category absent from the fitted vocabulary.
    CategoryUnknown,
    /// `1` only when the input was applicable but missing.
    MissingIndicator,
    /// `1` only when the input was structurally not applicable.
    NotApplicableIndicator,
    /// `1` only when an upstream audited substitution supplied the value.
    SubstitutedIndicator,
}

/// One emitted estimator column and the governed feature that produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncodedColumn {
    /// Stable encoded-column name.
    pub name: EncodedColumnName,
    /// Raw governed input feature.
    pub source_feature: FeatureName,
    /// Encoding role.
    pub kind: EncodedColumnKind,
}

/// Fitted numeric transform for one governed raw input feature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FittedInputColumn {
    /// Governed source feature, never a synthesized indicator.
    pub feature: FeatureName,
    /// Unit conversion applied before any fitted statistic (`Bps` becomes a
    /// ratio by dividing by 10,000).
    pub unit: FeatureUnit,
    /// Governed raw value kind. Categories are one-hot encoded and are never
    /// projected through their persistence ordinal.
    pub value_kind: FeatureValueKind,
    /// A required input rejects missing, not-applicable, and substituted cells.
    pub required: bool,
    /// Training-partition median used for missing/not-applicable optional cells.
    /// Required columns carry `None` because they are never imputed.
    pub median: Option<Decimal>,
    /// Mean fitted after unit conversion and optional imputation.
    pub mean: Option<Decimal>,
    /// Strictly-positive standard deviation fitted on the same rows.
    pub std: Option<Decimal>,
    /// Sorted vocabulary fitted from observed training-partition categories.
    /// Numeric inputs always carry an empty vocabulary.
    pub category_vocabulary: Vec<MarketCategory>,
    /// Training-partition state distribution, persisted for serving audit and
    /// model-detail diagnostics.
    pub state_rates: InputStateRates,
}

/// Frozen state distribution for one raw input in the final training partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputStateRates {
    pub observed: Decimal,
    pub missing: Decimal,
    pub not_applicable: Decimal,
    pub substituted: Decimal,
}

/// Complete fitted training/serving transform.
///
/// The raw-input and encoded-output contracts are persisted together so runtime
/// loading can validate shape before deserializing or invoking an estimator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FittedInputTransform {
    /// Ordered governed raw inputs.
    pub inputs: Vec<FittedInputColumn>,
    /// Exact estimator column order. Indicator columns are never features.
    pub encoded_columns: Vec<EncodedColumn>,
}

impl FittedInputTransform {
    /// Hash of the complete fitted transform graph and encoded-column order.
    pub fn transform_hash(&self) -> QuantResult<ContentHash> {
        ResearchHasher::canonical(self)
    }
}

/// One feature's global importance, as reported by the trained estimator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureImportance {
    /// Model-ready encoded column.
    pub feature: EncodedColumnName,
    /// Importance weight (non-negative; the explainability requirement).
    pub importance: Decimal,
}

/// Classical-ML training metrics (explainability + provenance).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassicalModelMetrics {
    /// Number of training rows the estimator was fit on.
    pub train_samples: u64,
    /// Number of feature columns.
    pub feature_count: u32,
    /// Validation objective (rank IC of predictions vs. labels).
    pub validation_objective: Decimal,
    /// Global feature importances.
    pub feature_importances: Vec<FeatureImportance>,
}

/// Frozen interpretation of a classical estimator's raw output.
///
/// This is deliberately narrower than a free-form label name. A runtime may
/// only turn an estimator output into a shadow signal when its supervised
/// target has one of these two well-defined business units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClassicalOutputSemantics {
    /// Regressor output is the predicted YES-token return over the frozen
    /// horizon, expressed in basis points.
    ForwardReturnBps,
    /// Logistic output is the uncalibrated probability that the token pays exactly 1.
    FullPayoutProbability,
}

/// Header-free classical-ML family payload (smartcore-backed).
///
/// The trained estimator's bytes live in the [`ArtifactStore`](crate::artifact::ArtifactStore)
/// at [`Self::serialized_model_uri`] (content-addressed by their own digest);
/// the serving contract owns estimator kind/format/hash plus all schema,
/// dataset, label, horizon, and training-input commitments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClassicalModelPayload {
    /// Exact estimator algorithm required to deserialize and execute the bytes.
    pub kind: ClassicalKind,
    /// ML crate that produced the estimator (`"smartcore"`).
    pub crate_name: String,
    /// Exact crate version; a load-time mismatch is rejected.
    pub crate_version: String,
    /// Exact governed meaning of the estimator's raw prediction.
    pub output_semantics: ClassicalOutputSemantics,
    /// Governed data-quality, liquidity, and horizon score multipliers applied
    /// to the shadow ranking score.
    pub multipliers: ScoreMultiplierSpec,
    /// Governed confidence penalty for every audited substituted input.
    pub substitution_confidence_rules: SubstitutionConfidenceRules,
    /// Exact ordered raw-input contract frozen by the owning `ModelSpec`.
    pub input_contract: ModelInputContract,
    /// Location of the serialized estimator bytes in the artifact store.
    pub serialized_model_uri: ArtifactUri,
    /// Exact content hash of the serialized estimator bytes.
    pub serialized_model_hash: ContentHash,
    /// Serialization codec required by the estimator adapter.
    pub serialization_format: ModelSerializationFormat,
    /// Frozen unit-conversion, imputation, indicators, and standardization.
    pub input_transform: FittedInputTransform,
    /// Exact portable tree representation and training-time verification.
    ///
    /// Required for GBDT and forbidden for every other classical estimator.
    pub tree_shap: Option<TreeShapModelContract>,
    /// Training metrics + feature importances.
    pub metrics: ClassicalModelMetrics,
}

impl ClassicalModelPayload {
    /// Required selection features derived from the frozen typed contract.
    #[must_use]
    pub fn required_features(&self) -> Vec<FeatureName> {
        required_features_from_contract(&self.input_contract)
    }

    /// All raw input features in the exact serving lookup order.
    #[must_use]
    pub fn input_features(&self) -> Vec<FeatureName> {
        input_features_from_contract(&self.input_contract)
    }

    /// Validate all metadata-only invariants before estimator bytes are loaded.
    pub fn model_payload_hash(&self) -> QuantResult<ContentHash> {
        typed_payload_hash(
            CLASSICAL_MODEL_PAYLOAD_HASH_DOMAIN,
            CLASSICAL_MODEL_PAYLOAD_VERSION,
            self,
        )
    }

    pub fn validate(&self) -> QuantResult<()> {
        self.validate_classical_identity()?;
        validate_fitted_inputs(&self.input_transform.inputs)?;
        let encoded_names = self.input_transform.validate_encoded_transform()?;
        validate_classical_metrics(self, &encoded_names)?;
        Ok(())
    }
}

fn invalid_classical_artifact(detail: impl Into<String>) -> QuantError {
    ResearchError::InvalidModelArtifact {
        detail: detail.into(),
    }
    .into()
}

impl ClassicalModelPayload {
    fn validate_classical_identity(&self) -> QuantResult<()> {
        if self.input_transform.inputs.is_empty() || self.input_transform.encoded_columns.is_empty()
        {
            return Err(invalid_classical_artifact(
                "classical input transform must contain inputs and encoded columns",
            ));
        }
        let expected_semantics = if self.kind == ClassicalKind::LogisticRegression {
            ClassicalOutputSemantics::FullPayoutProbability
        } else {
            ClassicalOutputSemantics::ForwardReturnBps
        };
        if self.output_semantics != expected_semantics {
            return Err(invalid_classical_artifact(format!(
                "classical kind {} requires {:?} output semantics, got {:?}",
                self.kind, expected_semantics, self.output_semantics
            )));
        }
        self.multipliers.validate_score_multipliers()?;
        self.substitution_confidence_rules
            .validate_substitution_rules()?;
        if self.serialization_format != ModelSerializationFormat::Bincode {
            return Err(invalid_classical_artifact(format!(
                "classical payload requires bincode serialization, got {:?}",
                self.serialization_format
            )));
        }
        model_input_contract_hash(&self.input_contract)?;
        if self.input_transform.inputs.len() != self.input_contract.inputs.len()
            || !self
                .input_transform
                .inputs
                .iter()
                .zip(&self.input_contract.inputs)
                .all(|(fitted, raw)| {
                    fitted.feature.as_str() == raw.feature_name
                        && fitted.required == (raw.requiredness == ModelInputRequiredness::Required)
                })
        {
            return Err(invalid_classical_artifact(
                "classical fitted inputs differ from the frozen typed input contract",
            ));
        }
        self.input_transform.transform_hash()?;
        match (self.kind, &self.tree_shap) {
            (ClassicalKind::GradientBoostedTrees, Some(tree_shap)) => {
                tree_shap.validate()?;
                let input_contract_hash = model_input_contract_hash(&self.input_contract)?;
                let encoded_names = self
                    .input_transform
                    .encoded_columns
                    .iter()
                    .map(|column| column.name.to_string())
                    .collect::<Vec<_>>();
                if tree_shap.ensemble.serialized_model_hash != self.serialized_model_hash
                    || tree_shap.ensemble.input_contract_hash != input_contract_hash
                    || tree_shap.ensemble.feature_names != encoded_names
                {
                    return Err(invalid_classical_artifact(
                        "GBDT TreeSHAP contract differs from serialized estimator or input contract",
                    ));
                }
            }
            (ClassicalKind::GradientBoostedTrees, None) => {
                return Err(invalid_classical_artifact(
                    "GBDT payload requires an exact TreeSHAP contract",
                ));
            }
            (_, Some(_)) => {
                return Err(invalid_classical_artifact(
                    "only GBDT payloads may carry a TreeSHAP contract",
                ));
            }
            (_, None) => {}
        }
        Ok(())
    }
}

fn validate_unit_interval(label: &str, value: Decimal) -> QuantResult<()> {
    if !(Decimal::ZERO..=Decimal::ONE).contains(&value) {
        return Err(invalid_classical_artifact(format!(
            "{label} must be within [0, 1], got {value}"
        )));
    }
    Ok(())
}

impl ScoreMultiplierSpec {
    fn validate_score_multipliers(&self) -> QuantResult<()> {
        for (label, value) in [
            ("data_quality.fresh", self.data_quality.fresh),
            ("data_quality.acceptable", self.data_quality.acceptable),
            ("data_quality.degraded", self.data_quality.degraded),
            ("data_quality.stale", self.data_quality.stale),
            ("data_quality.insufficient", self.data_quality.insufficient),
            ("liquidity.floor", self.liquidity.floor),
            ("horizon.in_window", self.horizon.in_window),
            ("horizon.too_soon", self.horizon.too_soon),
            ("horizon.too_late", self.horizon.too_late),
        ] {
            validate_unit_interval(label, value)?;
        }
        if self.horizon.min_ratio <= Decimal::ZERO
            || self.horizon.max_ratio < self.horizon.min_ratio
        {
            return Err(invalid_classical_artifact(
                "classical horizon multiplier ratios must satisfy 0 < min_ratio <= max_ratio",
            ));
        }
        let mut previous = None;
        for tier in &self.liquidity.tiers {
            if tier.min_liquidity_usd < Decimal::ZERO
                || previous.is_some_and(|bound| tier.min_liquidity_usd <= bound)
            {
                return Err(invalid_classical_artifact(
                    "classical liquidity tiers must have non-negative, strictly increasing bounds",
                ));
            }
            validate_unit_interval("liquidity.tier.multiplier", tier.multiplier)?;
            previous = Some(tier.min_liquidity_usd);
        }
        Ok(())
    }
}

impl SubstitutionConfidenceRules {
    fn validate_substitution_rules(&self) -> QuantResult<()> {
        for (label, value) in [
            ("source_unavailable", self.source_unavailable),
            ("stale_beyond_policy", self.stale_beyond_policy),
            ("out_of_valid_range", self.out_of_valid_range),
            ("insufficient_history", self.insufficient_history),
            ("not_applicable", self.not_applicable),
            ("leg_book_missing", self.leg_book_missing),
            (
                "execution_history_unavailable",
                self.execution_history_unavailable,
            ),
            (
                "insufficient_execution_history",
                self.insufficient_execution_history,
            ),
            (
                "insufficient_role_coverage",
                self.insufficient_role_coverage,
            ),
            ("domain_source_unavailable", self.domain_source_unavailable),
            ("linkage_unresolved", self.linkage_unresolved),
        ] {
            validate_unit_interval(label, value)?;
        }
        Ok(())
    }
}

fn validate_fitted_inputs(inputs: &[FittedInputColumn]) -> QuantResult<()> {
    let mut names = BTreeSet::new();
    for input in inputs {
        if !names.insert(input.feature.clone()) {
            return Err(invalid_classical_artifact(format!(
                "duplicate classical raw input `{}`",
                input.feature
            )));
        }
        (input).validate_input_state_rates()?;
        (input).validate_input_transform()?;
    }
    Ok(())
}

impl FittedInputColumn {
    fn validate_input_state_rates(&self) -> QuantResult<()> {
        let rates = &self.state_rates;
        let rate_sum = rates.observed + rates.missing + rates.not_applicable + rates.substituted;
        let invalid_rate = [
            rates.observed,
            rates.missing,
            rates.not_applicable,
            rates.substituted,
        ]
        .into_iter()
        .any(|rate| !(Decimal::ZERO..=Decimal::ONE).contains(&rate));
        if invalid_rate || (rate_sum - Decimal::ONE).abs() > Decimal::new(1, 12) {
            return Err(invalid_classical_artifact(format!(
                "input `{}` carries invalid training state rates",
                self.feature
            )));
        }
        Ok(())
    }
}

impl FittedInputColumn {
    fn validate_input_transform(&self) -> QuantResult<()> {
        if self.value_kind != FeatureValueKind::Category {
            if self.required != self.median.is_none()
                || self.mean.is_none()
                || self.std.is_none_or(|std| std <= Decimal::ZERO)
                || !self.category_vocabulary.is_empty()
            {
                return Err(invalid_classical_artifact(format!(
                    "numeric input `{}` carries an invalid fitted transform",
                    self.feature
                )));
            }
            return Ok(());
        }
        if self.unit != FeatureUnit::None
            || self.median.is_some()
            || self.mean.is_some()
            || self.std.is_some()
            || self.category_vocabulary.is_empty()
        {
            return Err(invalid_classical_artifact(format!(
                "categorical input `{}` carries invalid numeric statistics, unit, or empty vocabulary",
                self.feature
            )));
        }
        let vocabulary = self
            .category_vocabulary
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if vocabulary.len() != self.category_vocabulary.len()
            || !self
                .category_vocabulary
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        {
            return Err(invalid_classical_artifact(format!(
                "categorical input `{}` vocabulary must be unique and sorted",
                self.feature
            )));
        }
        Ok(())
    }
}

impl FittedInputTransform {
    fn validate_encoded_transform(&self) -> QuantResult<BTreeSet<EncodedColumnName>> {
        let names = validate_encoded_references(&self.inputs, &self.encoded_columns)?;
        let expected = expected_encoded_columns(&self.inputs);
        if self.encoded_columns.len() != expected.len() {
            return Err(invalid_classical_artifact(format!(
                "classical encoded width mismatch: expected {}, got {}",
                expected.len(),
                self.encoded_columns.len()
            )));
        }
        for (column, (feature, kind, suffix)) in self.encoded_columns.iter().zip(expected) {
            let expected_name = format!("{}.__{suffix}", feature.as_str());
            if column.source_feature != feature
                || column.kind != kind
                || column.name.as_str() != expected_name
            {
                return Err(invalid_classical_artifact(format!(
                    "encoded column `{}` does not match the deterministic transform contract `{expected_name}`",
                    column.name
                )));
            }
        }
        Ok(names)
    }
}

fn validate_encoded_references(
    inputs: &[FittedInputColumn],
    encoded: &[EncodedColumn],
) -> QuantResult<BTreeSet<EncodedColumnName>> {
    let mut names = BTreeSet::new();
    for column in encoded {
        if !names.insert(column.name.clone()) {
            return Err(invalid_classical_artifact(format!(
                "duplicate encoded column `{}`",
                column.name
            )));
        }
        let input = inputs
            .iter()
            .find(|input| input.feature == column.source_feature)
            .ok_or_else(|| {
                invalid_classical_artifact(format!(
                    "encoded column `{}` references unknown input `{}`",
                    column.name, column.source_feature
                ))
            })?;
        if input.required
            && matches!(
                column.kind,
                EncodedColumnKind::MissingIndicator
                    | EncodedColumnKind::NotApplicableIndicator
                    | EncodedColumnKind::SubstitutedIndicator
            )
        {
            return Err(invalid_classical_artifact(format!(
                "required input `{}` must not emit missingness indicators",
                input.feature
            )));
        }
    }
    Ok(names)
}

fn expected_encoded_columns(
    inputs: &[FittedInputColumn],
) -> Vec<(FeatureName, EncodedColumnKind, String)> {
    let mut expected = Vec::new();
    for input in inputs {
        if input.value_kind == FeatureValueKind::Category {
            expected.extend(input.category_vocabulary.iter().map(|category| {
                (
                    input.feature.clone(),
                    EncodedColumnKind::CategoryValue,
                    format!("category_{}", category.as_str()),
                )
            }));
            expected.push((
                input.feature.clone(),
                EncodedColumnKind::CategoryUnknown,
                "category_unknown".to_owned(),
            ));
        } else {
            expected.push((
                input.feature.clone(),
                EncodedColumnKind::NumericValue,
                "value".to_owned(),
            ));
        }
        if !input.required {
            expected.extend([
                (
                    input.feature.clone(),
                    EncodedColumnKind::MissingIndicator,
                    "missing".to_owned(),
                ),
                (
                    input.feature.clone(),
                    EncodedColumnKind::NotApplicableIndicator,
                    "not_applicable".to_owned(),
                ),
                (
                    input.feature.clone(),
                    EncodedColumnKind::SubstitutedIndicator,
                    "substituted".to_owned(),
                ),
            ]);
        }
    }
    expected
}

fn validate_classical_metrics(
    artifact: &ClassicalModelPayload,
    encoded_names: &BTreeSet<EncodedColumnName>,
) -> QuantResult<()> {
    let encoded = &artifact.input_transform.encoded_columns;
    let metric_width = usize::try_from(artifact.metrics.feature_count).map_err(|error| {
        invalid_classical_artifact(format!("invalid classical metric feature_count: {error}"))
    })?;
    if metric_width != encoded.len() {
        return Err(invalid_classical_artifact(format!(
            "classical metric width {metric_width} does not match transform width {}",
            encoded.len()
        )));
    }
    let mut importance_names = BTreeSet::new();
    for importance in &artifact.metrics.feature_importances {
        if importance.importance < Decimal::ZERO {
            return Err(invalid_classical_artifact(format!(
                "encoded column `{}` has negative importance",
                importance.feature
            )));
        }
        if !encoded_names.contains(&importance.feature)
            || !importance_names.insert(importance.feature.clone())
        {
            return Err(invalid_classical_artifact(format!(
                "classical importance references an unknown or duplicate column `{}`",
                importance.feature
            )));
        }
    }
    if importance_names.len() != encoded.len() {
        return Err(invalid_classical_artifact(format!(
            "classical importances cover {} columns but transform declares {}",
            importance_names.len(),
            encoded.len()
        )));
    }
    Ok(())
}

const WEIGHTED_MODEL_PAYLOAD_HASH_DOMAIN: &str = "quant-pivot/model-payload/weighted-factor";
const WEIGHTED_MODEL_PAYLOAD_VERSION: u32 = 1;
const CLASSICAL_MODEL_PAYLOAD_HASH_DOMAIN: &str = "quant-pivot/model-payload/classical";
const CLASSICAL_MODEL_PAYLOAD_VERSION: u32 = 2;
const SELL_MODEL_PAYLOAD_HASH_DOMAIN: &str = "quant-pivot/model-payload/hold-vs-exit";
const SELL_MODEL_PAYLOAD_VERSION: u32 = 1;

/// Header-free family payload committed before the serving contract exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "family",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ModelPayload {
    WeightedFactor(Box<WeightedFactorModelPayload>),
    Classical(Box<ClassicalModelPayload>),
    SellScorer(Box<SellScorerPayload>),
}

impl ModelPayload {
    pub fn model_payload_hash(&self) -> QuantResult<ContentHash> {
        match self {
            Self::WeightedFactor(payload) => payload.model_payload_hash(),
            Self::Classical(payload) => payload.model_payload_hash(),
            Self::SellScorer(payload) => payload.model_payload_hash(),
        }
    }

    /// Build the sole canonical estimator commitment for a serving contract.
    pub fn serving_estimator_binding(
        &self,
        plane: &FactorServingPlane,
    ) -> QuantResult<ModelServingEstimatorBinding> {
        let model_payload_hash = self.model_payload_hash()?;
        match self {
            Self::WeightedFactor(payload) => {
                payload.validate_for_plane(plane)?;
                Ok(ModelServingEstimatorBinding::FactorNative {
                    ordered_inputs: factor_estimator_inputs(plane, false)?,
                    model_payload_hash,
                })
            }
            Self::SellScorer(payload) => {
                payload.validate_for_plane(plane)?;
                Ok(ModelServingEstimatorBinding::FactorNative {
                    ordered_inputs: factor_estimator_inputs(plane, true)?,
                    model_payload_hash,
                })
            }
            Self::Classical(payload) => {
                if !plane.definitions().is_empty() {
                    return Err(invalid_model_payload(
                        "classical payload requires the canonical empty factor plane".to_owned(),
                    ));
                }
                payload.validate()?;
                Ok(ModelServingEstimatorBinding::Classical {
                    kind: payload.kind,
                    model_payload_hash,
                    serialized_model_hash: payload.serialized_model_hash,
                    serialization_format: payload.serialization_format,
                    tree_shap: payload.tree_shap.as_ref().map(|contract| {
                        ModelServingTreeShapBinding {
                            ensemble_hash: contract.ensemble_hash,
                            background_distribution_hash: contract
                                .ensemble
                                .background_distribution_hash,
                            verified_case_count: contract.verified_case_count,
                            max_efficiency_residual: contract.max_efficiency_residual,
                            max_prediction_residual: contract.max_prediction_residual,
                        }
                    }),
                })
            }
        }
    }
}

/// Sealed outer artifact. Construction is possible only after the payload hash
/// has been embedded in a complete serving contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelArtifact {
    header: ModelArtifactHeader,
    payload: ModelPayload,
}

/// Breaking stored-model wire version. No legacy parser is provided.
pub const MODEL_ARTIFACT_FORMAT_VERSION: u32 = 4;

#[derive(Serialize)]
pub(crate) struct StoredModelArtifactRef<'a> {
    pub format_version: u32,
    pub artifact: &'a ModelArtifact,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredModelArtifact {
    format_version: u32,
    artifact: ModelArtifact,
}

impl ModelArtifact {
    /// Load and verify the exact content-addressed artifact bound to a persisted
    /// model version.
    ///
    /// # Errors
    ///
    /// Rejects an invalid persisted serving-contract projection, missing or
    /// malformed bytes, a canonical hash mismatch, payload invariant failure,
    /// or any drift between the artifact header and persisted contract.
    pub async fn load_verified(
        store: &dyn ArtifactStore,
        model_version: &ModelVersionInfo,
    ) -> QuantResult<Self> {
        let persisted = model_version.verified_serving_contract().map_err(|error| {
            ResearchError::InvalidModelArtifact {
                detail: format!("invalid persisted serving contract: {error}"),
            }
        })?;
        let recorded = model_version.artifact_hash;
        let key = Self::artifact_key(&recorded)?;
        let bytes = store.get_by_key(&key).await?;
        let artifact = Self::from_bytes(&bytes)?;
        let recomputed = artifact.content_hash()?;
        if recomputed != recorded {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: recorded.to_string(),
                actual: recomputed.to_string(),
            }
            .into());
        }
        if artifact.header().serving_contract() != persisted {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "artifact header differs from the exact persisted serving contract"
                    .to_owned(),
            }
            .into());
        }
        Ok(artifact)
    }

    /// Seal one already-hashed family payload against its complete contract.
    pub fn try_seal(
        serving_contract: ModelServingContract,
        payload: ModelPayload,
    ) -> QuantResult<Self> {
        let artifact = Self {
            header: ModelArtifactHeader { serving_contract },
            payload,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    #[must_use]
    pub const fn header(&self) -> &ModelArtifactHeader {
        &self.header
    }

    #[must_use]
    pub const fn payload(&self) -> &ModelPayload {
        &self.payload
    }

    pub(crate) fn into_parts(self) -> QuantResult<(ModelArtifactHeader, ModelPayload)> {
        self.validate()?;
        Ok((self.header, self.payload))
    }

    #[must_use]
    pub const fn category_scope(&self) -> Option<MarketCategory> {
        self.header.category_scope()
    }

    pub fn validate(&self) -> QuantResult<()> {
        self.header
            .serving_contract
            .validate()
            .map_err(|error| invalid_model_payload(format!("invalid serving contract: {error}")))?;
        validate_payload_contract(&self.header.serving_contract, &self.payload)
    }

    /// The canonical content hash of this artifact (`blake3:<hex>`), the address
    /// it is stored and retrieved under.
    ///
    /// # Errors
    ///
    /// Propagates canonical-serialization failures.
    pub fn content_hash(&self) -> QuantResult<ContentHash> {
        self.validate()?;
        ResearchHasher::model_artifact(self)
    }

    /// Serialize the artifact to its stored byte form.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::Serialization`] on a serde failure.
    pub fn to_bytes(&self) -> QuantResult<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(&StoredModelArtifactRef {
            format_version: MODEL_ARTIFACT_FORMAT_VERSION,
            artifact: self,
        })
        .map_err(|error| {
            ResearchError::Serialization {
                detail: error.to_string(),
            }
            .into()
        })
    }

    /// Deserialize an artifact from its stored byte form.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::Serialization`] on a serde failure.
    pub fn from_bytes(bytes: &[u8]) -> QuantResult<Self> {
        let stored: StoredModelArtifact = serde_json::from_slice(bytes).map_err(|error| {
            ResearchError::Serialization {
                detail: format!(
                    "model artifact is not the required v{MODEL_ARTIFACT_FORMAT_VERSION} envelope: {error}"
                ),
            }
        })?;
        if stored.format_version != MODEL_ARTIFACT_FORMAT_VERSION {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "unsupported model artifact format {}, expected {}",
                    stored.format_version, MODEL_ARTIFACT_FORMAT_VERSION
                ),
            }
            .into());
        }
        stored.artifact.validate()?;
        Ok(stored.artifact)
    }

    /// The content-addressed store key for an artifact of the given hash.
    ///
    /// # Errors
    ///
    /// Propagates [`ArtifactKey`] validation (the hex digest is always valid, so
    /// this only fails on an internal contract violation).
    pub fn artifact_key(hash: &ContentHash) -> QuantResult<ArtifactKey> {
        ArtifactKey::new(ArtifactNamespace::Model, hash.hex(), "json")
    }

    /// Whether the artifact's return model is calibrated.
    ///
    /// Only weighted-factor buy models carry a return model; classical and
    /// sell-side artifacts are unconditionally uncalibrated for this gate.
    #[must_use]
    pub const fn return_model_is_calibrated(&self) -> bool {
        match &self.payload {
            ModelPayload::WeightedFactor(weighted) => {
                matches!(weighted.return_model, ReturnModelSpec::Calibrated(_))
            }
            ModelPayload::Classical(_) | ModelPayload::SellScorer(_) => false,
        }
    }

    /// The bound `model_score` calibrator id, when the return model is
    /// `Calibrated` — the target for a deep, calibrator-liveness admission
    /// check (the enum variant alone only proves *a* calibrator was bound at
    /// publish time, not that it is still active today).
    #[must_use]
    pub const fn calibrator_ref(&self) -> Option<&CalibrationArtifactId> {
        match &self.payload {
            ModelPayload::WeightedFactor(weighted) => match &weighted.return_model {
                ReturnModelSpec::Calibrated(calibrated) => Some(&calibrated.calibrator_ref),
                ReturnModelSpec::Heuristic(_) => None,
            },
            ModelPayload::Classical(_) | ModelPayload::SellScorer(_) => None,
        }
    }
}

fn validate_payload_contract(
    contract: &ModelServingContract,
    payload: &ModelPayload,
) -> QuantResult<()> {
    let bindings = contract.bindings();
    let payload_hash = payload.model_payload_hash()?;
    match (&bindings.model.estimator, payload) {
        (
            ModelServingEstimatorBinding::FactorNative {
                ordered_inputs,
                model_payload_hash,
            },
            ModelPayload::WeightedFactor(weighted),
        ) if bindings.model.model_family == ModelFamily::WeightedFactor => {
            validate_payload_hash(*model_payload_hash, payload_hash)?;
            weighted.validate_for_plane(&bindings.factors.plane)?;
            validate_factor_inputs(&bindings.factors.plane, ordered_inputs, false)?;
            validate_transform(
                contract,
                &weighted.input_contract,
                weighted.input_transform_hash()?,
            )?;
            validate_weighted_calibration(contract, &weighted.return_model)?;
            validate_category_scope(contract, &weighted.factor_head)
        }
        (
            ModelServingEstimatorBinding::FactorNative {
                ordered_inputs,
                model_payload_hash,
            },
            ModelPayload::SellScorer(sell),
        ) if bindings.model.model_family == ModelFamily::HoldVsExitWeighted => {
            validate_payload_hash(*model_payload_hash, payload_hash)?;
            sell.validate_for_plane(&bindings.factors.plane)?;
            validate_factor_inputs(&bindings.factors.plane, ordered_inputs, true)?;
            validate_transform(contract, &sell.input_contract, sell.input_transform_hash()?)?;
            validate_category_scope(contract, &sell.factor_head)
        }
        (
            ModelServingEstimatorBinding::Classical {
                kind,
                model_payload_hash,
                serialized_model_hash,
                serialization_format,
                tree_shap,
            },
            ModelPayload::Classical(classical),
        ) if bindings.model.model_family == ModelFamily::from_classical(*kind) => {
            validate_payload_hash(*model_payload_hash, payload_hash)?;
            if *serialization_format != ModelSerializationFormat::Bincode {
                return Err(invalid_model_payload(format!(
                    "classical payload requires bincode, got {serialization_format:?}"
                )));
            }
            if classical.serialization_format != *serialization_format
                || classical.serialized_model_hash != *serialized_model_hash
            {
                return Err(invalid_model_payload(
                    "classical serialized estimator binding differs from payload".to_owned(),
                ));
            }
            if classical.kind != *kind {
                return Err(invalid_model_payload(format!(
                    "classical payload kind {} differs from contract kind {}",
                    classical.kind, kind
                )));
            }
            let expected_tree_shap =
                classical
                    .tree_shap
                    .as_ref()
                    .map(|contract| ModelServingTreeShapBinding {
                        ensemble_hash: contract.ensemble_hash,
                        background_distribution_hash: contract
                            .ensemble
                            .background_distribution_hash,
                        verified_case_count: contract.verified_case_count,
                        max_efficiency_residual: contract.max_efficiency_residual,
                        max_prediction_residual: contract.max_prediction_residual,
                    });
            if *tree_shap != expected_tree_shap {
                return Err(invalid_model_payload(
                    "classical TreeSHAP serving binding differs from payload".to_owned(),
                ));
            }
            classical.validate()?;
            validate_transform(
                contract,
                &classical.input_contract,
                classical.input_transform.transform_hash()?,
            )
        }
        _ => Err(invalid_model_payload(format!(
            "payload family is incompatible with contract family {:?}",
            bindings.model.model_family
        ))),
    }
}

fn validate_payload_hash(expected: ContentHash, actual: ContentHash) -> QuantResult<()> {
    if expected != actual {
        return Err(invalid_model_payload(format!(
            "model payload hash mismatch: contract={expected}, payload={actual}"
        )));
    }
    Ok(())
}

fn validate_transform(
    contract: &ModelServingContract,
    input_contract: &ModelInputContract,
    input_transform_hash: ContentHash,
) -> QuantResult<()> {
    let transform = &contract.bindings().transform;
    let input_contract_hash = model_input_contract_hash(input_contract)?;
    if transform.input_contract_hash != input_contract_hash {
        return Err(invalid_model_payload(format!(
            "input contract hash mismatch: contract={}, payload={input_contract_hash}",
            transform.input_contract_hash
        )));
    }
    if transform.input_transform_hash != input_transform_hash {
        return Err(invalid_model_payload(format!(
            "input transform hash mismatch: contract={}, payload={input_transform_hash}",
            transform.input_transform_hash
        )));
    }
    Ok(())
}

fn validate_weighted_calibration(
    contract: &ModelServingContract,
    return_model: &ReturnModelSpec,
) -> QuantResult<()> {
    match (return_model, &contract.bindings().model.calibration) {
        (ReturnModelSpec::Heuristic(_), None) => Ok(()),
        (ReturnModelSpec::Calibrated(model), Some(binding))
            if model.calibrator_ref == binding.artifact_id =>
        {
            Ok(())
        }
        _ => Err(invalid_model_payload(
            "weighted return-model calibration differs from serving contract".to_owned(),
        )),
    }
}

fn validate_factor_inputs(
    plane: &FactorServingPlane,
    actual: &[ModelServingEstimatorInput],
    include_intrinsic: bool,
) -> QuantResult<()> {
    let expected = factor_estimator_inputs(plane, include_intrinsic)?;
    if actual != expected {
        return Err(invalid_model_payload(
            "contract estimator input order differs from canonical payload input order".to_owned(),
        ));
    }
    Ok(())
}

fn factor_estimator_inputs(
    plane: &FactorServingPlane,
    include_intrinsic: bool,
) -> QuantResult<Vec<ModelServingEstimatorInput>> {
    plane
        .validate()
        .map_err(|error| invalid_model_payload(format!("invalid factor serving plane: {error}")))?;
    let mut inputs = plane
        .definitions()
        .iter()
        .filter(|revision| !revision.definition().is_diagnostic())
        .map(|revision| ModelServingEstimatorInput::GovernedFactor {
            factor_definition_id: revision.factor_definition_id(),
        })
        .collect::<Vec<_>>();
    if include_intrinsic {
        for kind in canonical_sell_intrinsic_kinds() {
            let binding = ModelServingIntrinsicInputRef::try_from(kind)
                .map_err(|error| invalid_model_payload(error.to_string()))?;
            inputs.push(ModelServingEstimatorInput::ModelIntrinsic { binding });
        }
    }
    Ok(inputs)
}

fn validate_reference_quantiles(
    plane: &FactorServingPlane,
    cross_section: &FactorCrossSectionConfig,
    references: &FrozenReferenceQuantiles,
) -> QuantResult<()> {
    references.validate()?;
    match cross_section.small_cross_section_policy {
        SmallCrossSectionPolicy::Indeterminate if references.is_empty() => Ok(()),
        SmallCrossSectionPolicy::Indeterminate => Err(invalid_model_payload(
            "Indeterminate cross-section policy cannot carry frozen references".to_owned(),
        )),
        SmallCrossSectionPolicy::FrozenReferenceQuantile => {
            let required = plane
                .definitions()
                .iter()
                .filter(|revision| revision.definition().normalization.is_cross_sectional())
                .map(FactorDefinitionRef::factor_name)
                .collect::<BTreeSet<_>>();
            let actual = references.index();
            if required.len() != actual.len()
                || required.iter().any(|factor| !actual.contains_key(*factor))
            {
                return Err(invalid_model_payload(
                    "frozen reference CDFs do not exactly cover the cross-sectional factor plane"
                        .to_owned(),
                ));
            }
            Ok(())
        }
    }
}

const fn canonical_sell_intrinsic_kinds() -> [ModelServingIntrinsicInputKind; 4] {
    [
        ModelServingIntrinsicInputKind::PositionTakeProfitPressure,
        ModelServingIntrinsicInputKind::PositionStopLossPressure,
        ModelServingIntrinsicInputKind::PositionTimeInTrade,
        ModelServingIntrinsicInputKind::PositionPeakDrawdown,
    ]
}

fn validate_simplex_sum(label: &str, sum: Decimal) -> QuantResult<()> {
    if (sum - Decimal::ONE).abs() > weight_sum_tolerance() {
        return Err(invalid_model_payload(format!(
            "{label} weights must sum to one, got {sum}"
        )));
    }
    Ok(())
}

fn typed_payload_hash<T>(domain: &str, version: u32, payload: &T) -> QuantResult<ContentHash>
where
    T: Serialize,
{
    Ok(CanonicalDigest::content_hash_typed(
        domain, version, payload,
    )?)
}

fn invalid_model_payload(detail: String) -> QuantError {
    ResearchError::InvalidModelArtifact { detail }.into()
}

#[cfg(test)]
mod tests {
    use std::{env, process, sync::Arc};

    use chrono::Utc;
    use quant_pivot_models::{
        domain::quant::ModelVersionInfo,
        enums::{
            model::ModelFamily,
            quant::{DataQualityStatus, DownsideSource},
        },
        runtime_config::{DecimalValue, SellScorerConfig},
        types::{
            CalibrationArtifactId, ContentHash, ModelInputContract, ModelInputRequiredness,
            ModelInputSpec, PayoutRatio, Price, Probability,
            calibration::{
                IsotonicKnot, MonotoneMapping, ReliabilityBin, ReliabilityReport,
                SplitPayoutRateEvidence,
            },
            model_metrics::ModelVersionMetrics,
            model_spec::ModelSpecThesis,
            model_training::ModelTrainingObjective,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use serde_json::Value;

    use super::{
        CalibratedReturnModel, DataQualityMultipliers, HeuristicReturnModel, HorizonMultipliers,
        LiquidityMultipliers, MODEL_ARTIFACT_FORMAT_VERSION, ModelArtifact, ModelPayload,
        ReturnModelSpec, SellEstimatorSpec, SellScorerOutputSpec, StoredModelArtifactRef,
    };
    use crate::{
        artifact::{ArtifactStore, LocalArtifactStore},
        features::FeatureName,
        model::calibrator::ResolvedCalibration,
        test_support::{
            content_hash as hash, seal_model_payload, sell_payload, weighted_factor_plane,
            weighted_payload,
        },
    };

    fn version_info(artifact: &ModelArtifact, artifact_hash: ContentHash) -> ModelVersionInfo {
        let serving_contract = artifact.header().serving_contract().clone();
        let bindings = serving_contract.bindings();
        let model = &bindings.model;
        let trade_policy = bindings
            .trade_policy
            .as_ref()
            .map(|policy| (policy.artifact_id, policy.content_hash));
        ModelVersionInfo {
            model_version_id: model.model_version_id,
            model_spec_id: model.model_spec_id,
            model_spec_name: "artifact-load-test".to_owned(),
            model_family: model.model_family,
            model_spec_thesis: ModelSpecThesis {
                summary: "Artifact load contract".to_owned(),
                hypothesis: "The exact governed bytes load deterministically".to_owned(),
                limitations: vec!["Test-only artifact".to_owned()],
            },
            model_spec_definition_hash: model.model_spec_definition_hash,
            model_spec_prediction_horizon_secs: i64::try_from(model.prediction_horizon_secs)
                .expect("fixture horizon fits i64"),
            version: 1,
            artifact_hash,
            serving_contract_hash: serving_contract.contract_hash(),
            category_scope: model.category_scope,
            profile_ref: model.profile_ref.clone(),
            training_dataset_id: Some(bindings.dataset.manifest.training_dataset_id),
            trade_policy_artifact_id: trade_policy.map(|(artifact_id, _)| artifact_id),
            trade_policy_hash: trade_policy.map(|(_, content_hash)| content_hash),
            derivation_kind: ModelVersionInfo::training_derivation_kind(),
            parent_model_version_id: None,
            calibration_artifact_id: model
                .calibration
                .as_ref()
                .map(|calibration| calibration.artifact_id),
            derivation_evidence_hash: None,
            metrics: ModelVersionMetrics::not_measured("test fixture"),
            training_objective: ModelTrainingObjective::hand_authored("test fixture"),
            created_at: Utc::now(),
            serving_contract,
        }
    }

    fn temp_store() -> Arc<dyn ArtifactStore> {
        let root = env::temp_dir().join(format!(
            "qp_artifact_load_test_{}_{}",
            process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        Arc::new(LocalArtifactStore::new(root))
    }

    #[tokio::test]
    async fn verified_artifact_loads_consistently() {
        let store = temp_store();
        let artifact = ModelArtifact::weighted_fixture();
        let digest = artifact.content_hash().expect("hash");
        let key = ModelArtifact::artifact_key(&digest).expect("key");
        store
            .put(key, &artifact.to_bytes().expect("bytes"))
            .await
            .expect("put");

        let loaded = ModelArtifact::load_verified(store.as_ref(), &version_info(&artifact, digest))
            .await
            .expect("load");
        assert_eq!(loaded, artifact);
    }

    #[tokio::test]
    async fn load_rejects_artifact_mismatch() {
        let store = temp_store();
        let artifact = ModelArtifact::weighted_fixture();
        let wrong = hash("dead");
        let key = ModelArtifact::artifact_key(&wrong).expect("key");
        store
            .put(key, &artifact.to_bytes().expect("bytes"))
            .await
            .expect("put");

        let error = ModelArtifact::load_verified(store.as_ref(), &version_info(&artifact, wrong))
            .await
            .expect_err("hash mismatch must be rejected");
        assert!(error.to_string().contains("artifact hash mismatch"));
    }

    #[tokio::test]
    async fn load_rejects_contract_drift() {
        let store = temp_store();
        let artifact = ModelArtifact::weighted_fixture();
        let digest = artifact.content_hash().expect("hash");
        let key = ModelArtifact::artifact_key(&digest).expect("key");
        store
            .put(key, &artifact.to_bytes().expect("bytes"))
            .await
            .expect("put");

        let persisted = ModelArtifact::sell_fixture();
        let error = ModelArtifact::load_verified(store.as_ref(), &version_info(&persisted, digest))
            .await
            .expect_err("serving-contract drift must be rejected");
        assert!(
            error
                .to_string()
                .contains("artifact header differs from the exact persisted serving contract")
        );
    }

    #[tokio::test]
    async fn load_rejects_persisted_drift() {
        let store = temp_store();
        let artifact = ModelArtifact::weighted_fixture();
        let digest = artifact.content_hash().expect("hash");
        let key = ModelArtifact::artifact_key(&digest).expect("key");
        store
            .put(key, &artifact.to_bytes().expect("bytes"))
            .await
            .expect("put");
        let mut persisted = version_info(&artifact, digest);
        persisted.serving_contract_hash = hash("persisted-contract-drift");

        let error = ModelArtifact::load_verified(store.as_ref(), &persisted)
            .await
            .expect_err("persisted serving-contract hash drift must be rejected");
        assert!(
            error
                .to_string()
                .contains("invalid persisted serving contract")
        );
    }

    #[test]
    fn sell_config_freezes_exactly() {
        let config = SellScorerConfig {
            market_head_weight: DecimalValue::new(dec!(0.4)),
            position_take_profit_weight: DecimalValue::new(dec!(0.1)),
            position_stop_loss_weight: DecimalValue::new(dec!(0.2)),
            position_time_in_trade_weight: DecimalValue::new(dec!(0.1)),
            position_peak_drawdown_weight: DecimalValue::new(dec!(0.2)),
            max_exit_alpha_bps: DecimalValue::new(dec!(425)),
            p_exit_gain: DecimalValue::new(dec!(3.5)),
            exit_deadband: DecimalValue::new(dec!(0.08)),
            default_sell_pct: DecimalValue::new(dec!(0.6)),
        };

        let estimator = SellEstimatorSpec::try_from(&config).expect("sell estimator");
        assert_eq!(estimator.market_head_weight, dec!(0.4));
        assert_eq!(
            estimator
                .intrinsic_weights
                .iter()
                .map(|weight| weight.weight)
                .collect::<Vec<_>>(),
            vec![dec!(0.1), dec!(0.2), dec!(0.1), dec!(0.2)]
        );
        let output = SellScorerOutputSpec::try_from(&config).expect("sell output");
        assert_eq!(output.max_exit_alpha_bps, dec!(425));
        assert_eq!(output.p_exit_gain, dec!(3.5));
        assert_eq!(output.exit_deadband, dec!(0.08));
        assert_eq!(output.default_sell_pct, dec!(0.6));
    }

    #[test]
    fn sell_config_rejects_simplex() {
        let config = SellScorerConfig {
            market_head_weight: DecimalValue::new(dec!(0.6)),
            ..SellScorerConfig::default()
        };
        assert!(SellEstimatorSpec::try_from(&config).is_err());

        let output = SellScorerConfig {
            exit_deadband: DecimalValue::new(Decimal::ONE),
            ..SellScorerConfig::default()
        };
        assert!(SellScorerOutputSpec::try_from(&output).is_err());
    }

    #[test]
    fn weighted_artifact_serde_hash() {
        let artifact = ModelArtifact::weighted_fixture();
        let bytes = artifact.to_bytes().expect("serialize");
        let back = ModelArtifact::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, artifact);
        assert_eq!(
            artifact.content_hash().expect("hash"),
            back.content_hash().expect("hash"),
            "round-trip must preserve the canonical content hash"
        );
    }

    #[test]
    fn model_artifact_rejects_bytes() {
        let artifact = ModelArtifact::weighted_fixture();
        let legacy = serde_json::to_vec(&artifact).expect("legacy serialization");
        assert!(ModelArtifact::from_bytes(&legacy).is_err());
    }

    #[test]
    fn model_rejects_unknown_version() {
        let artifact = ModelArtifact::weighted_fixture();
        let bytes = serde_json::to_vec(&StoredModelArtifactRef {
            format_version: MODEL_ARTIFACT_FORMAT_VERSION + 1,
            artifact: &artifact,
        })
        .expect("serialization");
        assert!(ModelArtifact::from_bytes(&bytes).is_err());
    }

    #[test]
    fn nested_payloads_reject_unknown() {
        for pointer in [
            "/artifact/payload/payload/factor_head",
            "/artifact/payload/payload/factor_head/alpha_weights/0",
            "/artifact/payload/payload/factor_head/context_weights/0",
        ] {
            assert_unknown_field_rejected(&ModelArtifact::weighted_fixture(), pointer);
        }
        for pointer in [
            "/artifact/payload/payload/estimator",
            "/artifact/payload/payload/estimator/intrinsic_weights/0",
        ] {
            assert_unknown_field_rejected(&ModelArtifact::sell_fixture(), pointer);
        }
    }

    fn assert_unknown_field_rejected(artifact: &ModelArtifact, pointer: &str) {
        let mut document: Value =
            serde_json::from_slice(&artifact.to_bytes().expect("artifact bytes"))
                .expect("artifact JSON");
        let object = document
            .pointer_mut(pointer)
            .and_then(Value::as_object_mut)
            .expect("fixture object at JSON pointer");
        object.insert("unknown_nested_field".to_owned(), Value::Bool(true));
        let bytes = serde_json::to_vec(&document).expect("mutated artifact bytes");
        assert!(
            ModelArtifact::from_bytes(&bytes).is_err(),
            "unknown field at {pointer} must fail closed"
        );
    }

    #[test]
    fn calibrator_ref_present_factor() {
        let heuristic = ModelArtifact::weighted_fixture();
        assert!(heuristic.calibrator_ref().is_none());

        let calibrator_ref = CalibrationArtifactId::from_v7();
        let plane = weighted_factor_plane();
        let mut calibrated = weighted_payload(&plane);
        calibrated.return_model = ReturnModelSpec::Calibrated(CalibratedReturnModel {
            calibrator_ref,
            downside_source: DownsideSource::MfeMae,
        });
        let calibrated = seal_model_payload(
            ModelPayload::WeightedFactor(Box::new(calibrated)),
            plane,
            ModelFamily::WeightedFactor,
        );
        assert_eq!(calibrated.calibrator_ref(), Some(&calibrator_ref));

        let sell = ModelArtifact::sell_fixture();
        assert!(sell.calibrator_ref().is_none());
    }

    #[test]
    fn weighted_rejects_unnormalized_weights() {
        let plane = weighted_factor_plane();
        let mut payload = weighted_payload(&plane);
        payload.factor_head.alpha_weights[0].weight = dec!(0.9);
        assert!(
            payload.validate_for_plane(&plane).is_err(),
            "alpha weights must sum to one"
        );

        let mut negative = weighted_payload(&plane);
        negative.factor_head.alpha_weights[0].weight = dec!(-0.1);
        assert!(
            negative.validate_for_plane(&plane).is_err(),
            "negative alpha weight must fail closed"
        );

        weighted_payload(&plane)
            .validate_for_plane(&plane)
            .expect("valid weighted payload");
    }

    #[test]
    fn weighted_rejects_without_hash() {
        let plane = weighted_factor_plane();
        let mut payload = weighted_payload(&plane);
        payload.input_contract.inputs[0].requiredness = ModelInputRequiredness::Optional;
        assert!(
            ModelArtifact::try_seal(
                ModelArtifact::weighted_fixture()
                    .header()
                    .serving_contract()
                    .clone(),
                ModelPayload::WeightedFactor(Box::new(payload)),
            )
            .is_err()
        );
    }

    #[test]
    fn weighted_required_features_requiredness() {
        let plane = weighted_factor_plane();
        let mut payload = weighted_payload(&plane);
        payload.input_contract = ModelInputContract {
            inputs: vec![
                ModelInputSpec::required("book.mid"),
                ModelInputSpec::optional("market.age_secs"),
            ],
        };
        payload
            .validate_for_plane(&plane)
            .expect("valid weighted payload");
        assert_eq!(
            payload.required_features(),
            vec![FeatureName::new("book.mid")]
        );
    }

    #[test]
    fn sell_rejects_malformed_hash() {
        let plane = weighted_factor_plane();
        let mut malformed = sell_payload(&plane);
        malformed
            .input_contract
            .inputs
            .push(malformed.input_contract.inputs[0].clone());
        assert!(malformed.validate_for_plane(&plane).is_err());
    }

    #[test]
    fn sell_validates_rejects_family() {
        let sell = ModelArtifact::sell_fixture();
        sell.validate().expect("valid Sell artifact");
        let plane = weighted_factor_plane();
        assert!(
            ModelArtifact::try_seal(
                sell.header().serving_contract().clone(),
                ModelPayload::WeightedFactor(Box::new(weighted_payload(&plane))),
            )
            .is_err(),
            "payload-family confusion must fail closed"
        );
    }

    #[test]
    fn sell_rejects_non_scale() {
        let plane = weighted_factor_plane();
        let mut payload = sell_payload(&plane);
        payload.output_spec.max_exit_alpha_bps = Decimal::ZERO;
        assert!(
            payload.validate_for_plane(&plane).is_err(),
            "max_exit_alpha_bps must be > 0 (a zero/negative scale flips alpha sign)"
        );
    }

    #[test]
    fn artifact_key_content_addressed() {
        let artifact = ModelArtifact::weighted_fixture();
        let digest = artifact.content_hash().expect("hash");
        let key = ModelArtifact::artifact_key(&digest).expect("key");
        assert_eq!(key.relative_path(), format!("models/{}.json", digest.hex()));
    }

    #[test]
    fn multipliers_exhaustive_monotone() {
        let dq = DataQualityMultipliers::conservative();
        assert!(
            dq.multiplier_for(DataQualityStatus::Fresh)
                >= dq.multiplier_for(DataQualityStatus::Stale)
        );
        let liq = LiquidityMultipliers::conservative();
        assert!(
            liq.multiplier_for(Some(Decimal::from(100_000)))
                > liq.multiplier_for(Some(Decimal::from(500)))
        );
        assert_eq!(liq.multiplier_for(None), liq.floor);
        let horizon = HorizonMultipliers::conservative();
        assert_eq!(
            horizon.multiplier_for(Some(86_400), 86_400),
            horizon.in_window
        );
        assert_eq!(horizon.multiplier_for(None, 86_400), horizon.in_window);
    }

    #[test]
    fn return_model_heuristic_calibrated() {
        let heuristic = ReturnModelSpec::Heuristic(HeuristicReturnModel {
            max_expected_return_bps: dec!(400),
            max_downside_bps: dec!(600),
        });
        let est = heuristic
            .estimate(dec!(1), dec!(1), Price::new(dec!(0.5)), None)
            .expect("heuristic estimate");
        assert_eq!(est.expected_return_bps, dec!(400));
        assert_eq!(est.downside_bps, dec!(0));
        assert!(!est.calibrated);
        assert!(
            est.payout_distribution.is_none(),
            "heuristic path never carries a calibrated payout distribution"
        );

        let calibrated = ReturnModelSpec::Calibrated(CalibratedReturnModel {
            calibrator_ref: CalibrationArtifactId::from_v7(),
            downside_source: DownsideSource::MfeMae,
        });
        let resolved = ResolvedCalibration {
            artifact_id: CalibrationArtifactId::from_v7(),
            mapping: MonotoneMapping::Isotonic {
                knots: vec![
                    IsotonicKnot {
                        score: dec!(0),
                        probability: dec!(0.2),
                    },
                    IsotonicKnot {
                        score: dec!(1),
                        probability: dec!(0.8),
                    },
                ],
            },
            reliability: ReliabilityReport {
                bins: vec![ReliabilityBin {
                    predicted_lo: dec!(0),
                    predicted_hi: dec!(1),
                    sample_count: 100,
                    mean_predicted: Probability::new(dec!(0.5)),
                    empirical_frequency: Probability::new(dec!(0.5)),
                    wilson_ci: (Probability::new(dec!(0.4)), Probability::new(dec!(0.6))),
                    mean_adverse_excursion_bps: Some(dec!(-300)),
                }],
                brier_score: dec!(0.1),
                log_loss: dec!(0.3),
                ece: dec!(0.05),
                n_samples: 100,
            },
            split_payout_rate: SplitPayoutRateEvidence {
                total_sample_count: 100,
                split_sample_count: 0,
                empirical_probability: Probability::ZERO,
                wilson_ci: (
                    Probability::ZERO,
                    Probability::new(dec!(0.03699349820698568)),
                ),
                split_payout_ratio: PayoutRatio::try_new(dec!(0.5))
                    .expect("canonical split payout"),
            },
        };
        // score=0.5 sits exactly halfway between the score=0 (p=0.2) and
        // score=1 (p=0.8) knots -> linear interpolation yields p_win=0.5.
        // E[r] = 0.5*(1-0.4)/0.4 - 0.5 = 0.75 - 0.5 = 0.25 -> 2500 bps.
        let mid = calibrated
            .estimate(dec!(0.5), dec!(0.8), Price::new(dec!(0.4)), Some(&resolved))
            .expect("calibrated estimate");
        assert!(mid.calibrated);
        assert_eq!(mid.downside_bps, dec!(300));
        assert_eq!(mid.expected_return_bps, dec!(2500));
        assert_eq!(
            mid.payout_distribution
                .map(|distribution| distribution.winner_take_all_win_probability),
            Some(Probability::new(dec!(0.5))),
            "the payout distribution must preserve the calibrated conditional win probability"
        );

        // Missing resolved calibration never fabricates a value.
        let missing = calibrated.estimate(dec!(0.5), dec!(0.8), Price::new(dec!(0.4)), None);
        assert!(missing.is_err(), "missing calibration must fail closed");
    }
}
