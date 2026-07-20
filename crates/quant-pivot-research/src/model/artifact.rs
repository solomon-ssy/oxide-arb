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
//! online history. 3.6 fills [`ReturnModelSpec::Calibrated`] +
//! [`TrainingObjectiveReport`].

use std::collections::{BTreeMap, BTreeSet};

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::{
        common::MarketCategory,
        quant::{DataQualityStatus, DownsideSource, ModelSerializationFormat},
    },
    runtime_config::{FactorCrossSectionConfig, SmallCrossSectionPolicy},
    types::{
        ArtifactUri, CalibrationArtifactId, ContentHash, ModelArtifactId, ModelInputContract,
        ModelInputRequiredness, ModelVersionId, Price, Probability, ResearchProfileRef,
        TradePolicyArtifactId, model_training::TrainingObjectiveSpec,
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{ArtifactKey, ArtifactNamespace},
    factors::{FactorName, FrozenReferenceQuantiles},
    features::{FeatureName, FeatureUnit, FeatureValueKind, NullReason},
    hashing::ResearchHasher,
    model::{
        calibrator::ResolvedCalibration,
        objective::{ObjectiveComponentReport, RankingDiagnostics},
        runtime::{ClassicalKind, ModelFamily},
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

fn validate_frozen_input_contract(
    artifact_kind: &str,
    contract: &ModelInputContract,
    expected_hash: &ContentHash,
) -> QuantResult<()> {
    let actual_hash = model_input_contract_hash(contract)?;
    if &actual_hash != expected_hash {
        return Err(ResearchError::InvalidModelArtifact {
            detail: format!(
                "{artifact_kind} artifact input-contract hash mismatch: expected {expected_hash}, got {actual_hash}"
            ),
        }
        .into());
    }
    Ok(())
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

/// Provenance header shared by every model artifact: which version, family, and
/// the schema hashes it is bound to. Loading must reject a mismatch against the
/// active feature/factor schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelArtifactHeader {
    /// The published model version this artifact realizes.
    pub model_version_id: ModelVersionId,
    /// Immutable owning model-spec definition hash copied from the dataset.
    pub model_spec_definition_hash: ContentHash,
    /// Immutable research profile shared by the source slice and dataset.
    pub profile_ref: ResearchProfileRef,
    /// Model family.
    pub model_family: ModelFamily,
    /// Feature-schema hash the artifact was trained/built against.
    pub feature_schema_hash: ContentHash,
    /// Factor-schema hash the artifact was trained/built against.
    pub factor_schema_hash: ContentHash,
    /// Trade policy frozen into executable policy-derived training, when applicable.
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    pub trade_policy_hash: Option<ContentHash>,
}

/// A single factor weight in a weighted-factor artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorWeight {
    /// The weighted factor.
    pub factor: FactorName,
    /// The (frozen) non-negative weight; the set is normalized to sum to 1.
    pub weight: Decimal,
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
/// Fulfils the 3.4 governed-confidence-penalty contract deferred from 3.2: each
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
    /// Penalty multiplier for [`NullReason::TradeTapeUnavailable`].
    pub trade_tape_unavailable: Decimal,
    /// Penalty multiplier for [`NullReason::InsufficientTradeTape`].
    pub insufficient_trade_tape: Decimal,
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
            NullReason::TradeTapeUnavailable => self.trade_tape_unavailable,
            NullReason::InsufficientTradeTape => self.insufficient_trade_tape,
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
            trade_tape_unavailable: Decimal::new(80, 2),
            insufficient_trade_tape: Decimal::new(85, 2),
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
/// (Phase 11.3 §6, `GateId::CalibrationRequired`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeuristicReturnModel {
    /// Expected return (bps) at `composite_score × confidence = 1`.
    pub max_expected_return_bps: Decimal,
    /// Downside (bps) bound at `composite_score × confidence = 0`; the realized
    /// downside scales down with conviction.
    pub max_downside_bps: Decimal,
}

/// A return model derived from a fitted `ProbabilityCalibrator` (Phase 11.3 §3.3/§5).
///
/// Replaces the former hand-fit return-curve interpolation. `calibrator_ref` names
/// the [`CalibrationArtifactId`] (`kind = ModelScore`) whose `P(win)` mapping +
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
    /// The calibrated `P(win)` (`Some` only for `Calibrated`; Kelly sizing
    /// (Phase 11.3 §4 redesign) consumes this directly as its win probability
    /// — never re-derived from `expected_return_bps`/`downside_bps`, which
    /// would reintroduce the resolution-vs-TP/SL bet-structure mismatch this
    /// field exists to remove). `None` for `Heuristic`, whose sizing path is
    /// fenced off from production by fail-closed publish/admission gates.
    pub win_probability: Option<Probability>,
}

impl ReturnModelSpec {
    /// Estimate the expected return / downside (bps) for one candidate.
    ///
    /// `market_price` is the executable YES-leg reference price (required by
    /// the `Calibrated` formula `E[r] = P(win)·(1-p)/p − (1−P(win))`; ignored
    /// by `Heuristic`). `calibration` must be `Some` whenever `self` is
    /// `Calibrated` — the runtime factory resolves it once at load time and
    /// fails the load closed otherwise. A missing binding is rejected here as
    /// well; it never becomes a zero-valued business prediction.
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
                    win_probability: None,
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

/// Weighted-factor scorer artifact (first-class, fully explainable).
///
/// Inputs arrive already normalized by the [`FactorEngine`](crate::factors::FactorEngine);
/// this body governs the *scoring* on top: per-factor weights, the data-quality /
/// liquidity / horizon multipliers, the substitution confidence penalties, the
/// return model, and the feature requirements the selector must honour.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeightedFactorModelArtifact {
    /// Common provenance header.
    pub header: ModelArtifactHeader,
    /// Semantic hash of the exact frozen dataset consumed by training.
    pub training_dataset_hash: ContentHash,
    /// Canonical final estimator-input groups after the frozen reference
    /// transform, including aligned labels and decision boundaries.
    pub training_input_hash: ContentHash,
    /// Exact ordered raw-input contract frozen by the owning `ModelSpec`.
    pub input_contract: ModelInputContract,
    /// Canonical hash of [`Self::input_contract`], including order and
    /// requiredness. This is the contract identity written to serving audit.
    pub input_contract_hash: ContentHash,
    /// Frozen per-factor non-negative weights (normalized to sum to 1).
    pub weights: Vec<FactorWeight>,
    /// Model-intrinsic prediction horizon, in seconds. Frozen at authoring from
    /// `ModelConfig.prediction_horizon_secs`; authoritative for the horizon
    /// multiplier and the candidate's `suggested_horizon_secs`.
    pub prediction_horizon_secs: u64,
    /// Governed score multipliers (data-quality / liquidity / horizon).
    pub multipliers: ScoreMultiplierSpec,
    /// Governed confidence penalty for imputed / kept-missing features.
    pub substitution_confidence_rules: SubstitutionConfidenceRules,
    /// Return / downside mapping (heuristic now, calibrated by 3.6).
    pub return_model: ReturnModelSpec,
    /// Small-cross-section transform frozen at training time. Serving uses this
    /// artifact contract, not a mutable history or unrelated active config.
    pub factor_cross_section: FactorCrossSectionConfig,
    /// Empirical raw-factor CDFs fitted from the final training partition.
    pub frozen_reference_quantiles: FrozenReferenceQuantiles,
    /// Trainer objective report (filled by 3.6; `None` when hand-authored).
    pub objective_report: Option<TrainingObjectiveReport>,
    /// When set, this artifact is scoped to one market category (Phase 11.2.2
    /// category routing). `None` means the generic cross-category scorer.
    #[serde(default)]
    pub category_scope: Option<MarketCategory>,
}

impl WeightedFactorModelArtifact {
    /// Required selection features derived from the frozen typed contract.
    #[must_use]
    pub fn required_features(&self) -> Vec<FeatureName> {
        required_features_from_contract(&self.input_contract)
    }

    /// Hash the complete frozen factor-input transform applied by serving.
    ///
    /// Training-integrity verification and the online audit path share this
    /// canonical implementation, so neither side can serialize a look-alike
    /// transform graph independently.
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

    /// Validate the structural money-adjacent invariants: a non-empty weight set,
    /// every weight non-negative, and weights normalized to sum to 1.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::InvalidModelArtifact`] on any violation.
    pub fn validate(&self) -> QuantResult<()> {
        if self.header.model_family != ModelFamily::WeightedFactor {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "weighted artifact has incompatible model family {:?}",
                    self.header.model_family
                ),
            }
            .into());
        }
        if self.prediction_horizon_secs == 0 {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "weighted artifact prediction horizon must be positive".to_owned(),
            }
            .into());
        }
        validate_frozen_input_contract(
            "weighted",
            &self.input_contract,
            &self.input_contract_hash,
        )?;
        if self.weights.is_empty() {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "weighted artifact has no factor weights".to_owned(),
            }
            .into());
        }
        let mut sum = Decimal::ZERO;
        let mut factor_names = BTreeSet::new();
        for weight in &self.weights {
            if !factor_names.insert(weight.factor.clone()) {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!("duplicate weighted factor `{}`", weight.factor),
                }
                .into());
            }
            if weight.weight < Decimal::ZERO {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "factor `{}` has negative weight {}",
                        weight.factor, weight.weight
                    ),
                }
                .into());
            }
            sum += weight.weight;
        }
        if (sum - Decimal::ONE).abs() > weight_sum_tolerance() {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!("factor weights must sum to 1, got {sum}"),
            }
            .into());
        }
        self.frozen_reference_quantiles.validate()?;
        match self.factor_cross_section.small_cross_section_policy {
            SmallCrossSectionPolicy::Indeterminate => {
                if !self.frozen_reference_quantiles.is_empty() {
                    return Err(ResearchError::InvalidModelArtifact {
                        detail: "weighted artifact carries frozen reference CDFs while its \
                                 small-cross-section policy is Indeterminate"
                            .to_owned(),
                    }
                    .into());
                }
            }
            SmallCrossSectionPolicy::FrozenReferenceQuantile => {
                if self.frozen_reference_quantiles.is_empty() {
                    return Err(ResearchError::InvalidModelArtifact {
                        detail: "weighted artifact FrozenReferenceQuantile policy has no frozen \
                                 reference CDFs"
                            .to_owned(),
                    }
                    .into());
                }
                for weight in &self.weights {
                    if self
                        .frozen_reference_quantiles
                        .get(&weight.factor)
                        .is_none()
                    {
                        return Err(ResearchError::InvalidModelArtifact {
                            detail: format!(
                                "weighted factor `{}` has no frozen reference CDF",
                                weight.factor
                            ),
                        }
                        .into());
                    }
                }
            }
        }
        Ok(())
    }

    /// The frozen weights indexed by factor name for O(1) scorer lookup.
    #[must_use]
    pub fn weight_index(&self) -> BTreeMap<FactorName, Decimal> {
        self.weights
            .iter()
            .map(|weight| (weight.factor.clone(), weight.weight))
            .collect()
    }
}

/// Sell-side hold-vs-exit output mapping (Phase 06.1).
///
/// Converts the signed ranking `net = Σ weightᵢ·signedᵢ ∈ [-1, 1]` (positive ⇒
/// exiting now beats holding) into the three business outputs the opportunistic
/// evaluator consumes: expected exit alpha, the probability the exit is better,
/// and a target cumulative exit fraction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SellScorerOutputSpec {
    /// Expected exit alpha (bps over holding) at `net = 1`; scales linearly with
    /// `net`, so a negative net yields a negative alpha (hold is better).
    pub max_exit_alpha_bps: Decimal,
    /// Logistic gain mapping `net → P(exit_better) = 1 / (1 + e^{-gain·net})`.
    /// Must be strictly positive (a monotone increasing map).
    pub p_exit_gain: Decimal,
    /// Floor target cumulative exit fraction when the scorer fires; the realized
    /// target scales from this floor up to `1.0` with conviction (`net⁺`).
    pub default_sell_pct: Decimal,
}

impl SellScorerOutputSpec {
    /// A conservative bootstrap mapping (hand-authored, pre-calibration).
    #[must_use]
    pub fn conservative() -> Self {
        Self {
            max_exit_alpha_bps: Decimal::from(300),
            p_exit_gain: Decimal::from(4),
            default_sell_pct: Decimal::ONE,
        }
    }
}

/// Sell-side hold-vs-exit weighted scorer artifact (Phase 06.1).
///
/// Symmetric to [`WeightedFactorModelArtifact`] but scores the *exit* decision
/// for one open position lot rather than an entry ranking. Inputs are the
/// already-normalized market factors plus lot position-state pseudo-factors
/// (`unrealized_pnl_pct` / `time_in_trade` / `peak_mark_drawdown`); the frozen
/// weights and [`SellScorerOutputSpec`] govern the mapping to `(exit_alpha_bps,
/// p_exit_better, recommended cumulative exit fraction)`. A distinct artifact
/// family so a Sell scorer can never be loaded where a Buy scorer is expected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SellScorerArtifact {
    /// Common provenance header (`model_family = HoldVsExitWeighted`).
    pub header: ModelArtifactHeader,
    /// Frozen per-factor non-negative weights (normalized to sum to 1), over
    /// both market factors and position-state pseudo-factors.
    pub weights: Vec<FactorWeight>,
    /// Model-intrinsic hold-vs-exit horizon, in seconds.
    pub prediction_horizon_secs: u64,
    /// Governed net → business-output mapping.
    pub output_spec: SellScorerOutputSpec,
    /// Hold-vs-exit label-schema hash the scorer was trained against.
    pub label_schema_hash: ContentHash,
    /// Semantic hash of the exact frozen training dataset/partition.
    pub training_dataset_hash: ContentHash,
    /// Canonical final weighted estimator input commitment.
    pub training_input_hash: ContentHash,
    /// Exact ordered raw-input contract frozen by the owning `ModelSpec`.
    pub input_contract: ModelInputContract,
    /// Canonical hash of [`Self::input_contract`], including order and
    /// requiredness.
    pub input_contract_hash: ContentHash,
    /// Trainer objective report (`None` when hand-authored).
    pub objective_report: Option<TrainingObjectiveReport>,
}

impl SellScorerArtifact {
    /// Required selection features derived from the frozen typed contract.
    #[must_use]
    pub fn required_features(&self) -> Vec<FeatureName> {
        required_features_from_contract(&self.input_contract)
    }

    /// Hash the complete frozen factor-input transform applied by serving.
    pub fn input_transform_hash(&self) -> QuantResult<ContentHash> {
        ResearchHasher::canonical(&(
            &self.header.factor_schema_hash,
            &self.weights,
            &self.input_contract_hash,
        ))
    }

    /// Validate the structural, money-adjacent invariants: a non-empty weight
    /// set, every weight non-negative and summing to 1, a strictly positive
    /// logistic gain, and a default exit fraction in `(0, 1]`.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::InvalidModelArtifact`] on any violation.
    pub fn validate(&self) -> QuantResult<()> {
        // Family guard (doc Blocker): a mis-tagged header must never load as a
        // Sell scorer, or a Buy ranker could be scored as an exit model.
        if self.header.model_family != ModelFamily::HoldVsExitWeighted {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "sell scorer artifact has non-exit model family {:?}",
                    self.header.model_family
                ),
            }
            .into());
        }
        validate_frozen_input_contract(
            "sell scorer",
            &self.input_contract,
            &self.input_contract_hash,
        )?;
        if self.output_spec.max_exit_alpha_bps <= Decimal::ZERO {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "sell scorer max_exit_alpha_bps {} must be > 0",
                    self.output_spec.max_exit_alpha_bps
                ),
            }
            .into());
        }
        if self.weights.is_empty() {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "sell scorer artifact has no factor weights".to_owned(),
            }
            .into());
        }
        let mut sum = Decimal::ZERO;
        for weight in &self.weights {
            if weight.weight < Decimal::ZERO {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "sell factor `{}` has negative weight {}",
                        weight.factor, weight.weight
                    ),
                }
                .into());
            }
            sum += weight.weight;
        }
        if (sum - Decimal::ONE).abs() > weight_sum_tolerance() {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!("sell factor weights must sum to 1, got {sum}"),
            }
            .into());
        }
        if self.output_spec.p_exit_gain <= Decimal::ZERO {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "sell scorer p_exit_gain must be > 0".to_owned(),
            }
            .into());
        }
        if self.output_spec.default_sell_pct <= Decimal::ZERO
            || self.output_spec.default_sell_pct > Decimal::ONE
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "sell scorer default_sell_pct {} must be within (0, 1]",
                    self.output_spec.default_sell_pct
                ),
            }
            .into());
        }
        Ok(())
    }

    /// The frozen weights indexed by factor name for O(1) scorer lookup.
    #[must_use]
    pub fn weight_index(&self) -> BTreeMap<FactorName, Decimal> {
        self.weights
            .iter()
            .map(|weight| (weight.factor.clone(), weight.weight))
            .collect()
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
    /// Logistic output is the uncalibrated probability that YES settles at 1.
    SettlementProbability,
}

/// Classical-ML artifact (smartcore-backed).
///
/// The trained estimator's bytes live in the [`ArtifactStore`](crate::artifact::ArtifactStore)
/// at [`Self::serialized_model_uri`] (content-addressed by their own digest);
/// this JSON body — itself content-addressed as the `quant_model_version`
/// `artifact_hash` — carries only the metadata needed to reload, version-check,
/// and explain the model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassicalModelArtifact {
    /// Common provenance header.
    pub header: ModelArtifactHeader,
    /// Stored-artifact id for the serialized estimator bytes.
    pub artifact_id: ModelArtifactId,
    /// Concrete classical kind.
    pub kind: ClassicalKind,
    /// ML crate that produced the estimator (`"smartcore"`).
    pub crate_name: String,
    /// Exact crate version (load-time mismatch ⇒ reject; §15.6).
    pub crate_version: String,
    /// Label-schema hash the model was trained against.
    pub label_schema_hash: ContentHash,
    /// Dataset hash the model was trained on.
    pub training_dataset_hash: ContentHash,
    /// Model-intrinsic prediction horizon, frozen from the owning model spec.
    pub prediction_horizon_secs: u64,
    /// Exact governed meaning of the estimator's raw prediction.
    pub output_semantics: ClassicalOutputSemantics,
    /// Governed data-quality, liquidity, and horizon score multipliers applied
    /// to the shadow ranking score.
    pub multipliers: ScoreMultiplierSpec,
    /// Governed confidence penalty for every audited substituted input.
    pub substitution_confidence_rules: SubstitutionConfidenceRules,
    /// Exact ordered raw-input contract frozen by the owning `ModelSpec`.
    pub input_contract: ModelInputContract,
    /// Canonical hash of [`Self::input_contract`], including order and
    /// requiredness.
    pub input_contract_hash: ContentHash,
    /// Complete fitted transform hash.
    pub input_transform_hash: ContentHash,
    /// Exact final estimator-ready rows plus aligned labels hash.
    pub training_input_hash: ContentHash,
    /// Location of the serialized estimator bytes in the artifact store.
    pub serialized_model_uri: ArtifactUri,
    /// BLAKE3 digest of the exact serialized estimator bytes.
    pub serialized_model_hash: ContentHash,
    /// Serialization format of the estimator bytes.
    pub serialization_format: ModelSerializationFormat,
    /// Frozen unit-conversion, imputation, indicators, and standardization.
    pub input_transform: FittedInputTransform,
    /// Training metrics + feature importances.
    pub metrics: ClassicalModelMetrics,
}

impl ClassicalModelArtifact {
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
    pub fn validate(&self) -> QuantResult<()> {
        validate_classical_identity(self)?;
        validate_fitted_inputs(&self.input_transform.inputs)?;
        let encoded_names = validate_encoded_transform(&self.input_transform)?;
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

fn validate_classical_identity(artifact: &ClassicalModelArtifact) -> QuantResult<()> {
    if artifact.header.model_family != ModelFamily::from_classical(artifact.kind) {
        return Err(invalid_classical_artifact(format!(
            "classical artifact family {:?} does not match kind {}",
            artifact.header.model_family, artifact.kind
        )));
    }
    if artifact.input_transform.inputs.is_empty()
        || artifact.input_transform.encoded_columns.is_empty()
    {
        return Err(invalid_classical_artifact(
            "classical input transform must contain inputs and encoded columns",
        ));
    }
    if artifact.prediction_horizon_secs == 0 {
        return Err(invalid_classical_artifact(
            "classical artifact prediction horizon must be positive",
        ));
    }
    let expected_semantics = if artifact.kind == ClassicalKind::LogisticRegression {
        ClassicalOutputSemantics::SettlementProbability
    } else {
        ClassicalOutputSemantics::ForwardReturnBps
    };
    if artifact.output_semantics != expected_semantics {
        return Err(invalid_classical_artifact(format!(
            "classical kind {} requires {:?} output semantics, got {:?}",
            artifact.kind, expected_semantics, artifact.output_semantics
        )));
    }
    validate_score_multipliers(&artifact.multipliers)?;
    validate_substitution_rules(&artifact.substitution_confidence_rules)?;
    validate_frozen_input_contract(
        "classical",
        &artifact.input_contract,
        &artifact.input_contract_hash,
    )?;
    if artifact.input_transform.inputs.len() != artifact.input_contract.inputs.len()
        || !artifact
            .input_transform
            .inputs
            .iter()
            .zip(&artifact.input_contract.inputs)
            .all(|(fitted, raw)| {
                fitted.feature.as_str() == raw.feature_name
                    && fitted.required == (raw.requiredness == ModelInputRequiredness::Required)
            })
    {
        return Err(invalid_classical_artifact(
            "classical fitted inputs differ from the frozen typed input contract",
        ));
    }
    let transform_hash = artifact.input_transform.transform_hash()?;
    if transform_hash != artifact.input_transform_hash {
        return Err(invalid_classical_artifact(format!(
            "classical input-transform hash mismatch: expected {}, got {transform_hash}",
            artifact.input_transform_hash
        )));
    }
    Ok(())
}

fn validate_unit_interval(label: &str, value: Decimal) -> QuantResult<()> {
    if !(Decimal::ZERO..=Decimal::ONE).contains(&value) {
        return Err(invalid_classical_artifact(format!(
            "{label} must be within [0, 1], got {value}"
        )));
    }
    Ok(())
}

fn validate_score_multipliers(spec: &ScoreMultiplierSpec) -> QuantResult<()> {
    for (label, value) in [
        ("data_quality.fresh", spec.data_quality.fresh),
        ("data_quality.acceptable", spec.data_quality.acceptable),
        ("data_quality.degraded", spec.data_quality.degraded),
        ("data_quality.stale", spec.data_quality.stale),
        ("data_quality.insufficient", spec.data_quality.insufficient),
        ("liquidity.floor", spec.liquidity.floor),
        ("horizon.in_window", spec.horizon.in_window),
        ("horizon.too_soon", spec.horizon.too_soon),
        ("horizon.too_late", spec.horizon.too_late),
    ] {
        validate_unit_interval(label, value)?;
    }
    if spec.horizon.min_ratio <= Decimal::ZERO || spec.horizon.max_ratio < spec.horizon.min_ratio {
        return Err(invalid_classical_artifact(
            "classical horizon multiplier ratios must satisfy 0 < min_ratio <= max_ratio",
        ));
    }
    let mut previous = None;
    for tier in &spec.liquidity.tiers {
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

fn validate_substitution_rules(rules: &SubstitutionConfidenceRules) -> QuantResult<()> {
    for (label, value) in [
        ("source_unavailable", rules.source_unavailable),
        ("stale_beyond_policy", rules.stale_beyond_policy),
        ("out_of_valid_range", rules.out_of_valid_range),
        ("insufficient_history", rules.insufficient_history),
        ("not_applicable", rules.not_applicable),
        ("leg_book_missing", rules.leg_book_missing),
        ("trade_tape_unavailable", rules.trade_tape_unavailable),
        ("insufficient_trade_tape", rules.insufficient_trade_tape),
        (
            "insufficient_role_coverage",
            rules.insufficient_role_coverage,
        ),
        ("domain_source_unavailable", rules.domain_source_unavailable),
        ("linkage_unresolved", rules.linkage_unresolved),
    ] {
        validate_unit_interval(label, value)?;
    }
    Ok(())
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
        validate_input_state_rates(input)?;
        validate_input_transform(input)?;
    }
    Ok(())
}

fn validate_input_state_rates(input: &FittedInputColumn) -> QuantResult<()> {
    let rates = &input.state_rates;
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
            input.feature
        )));
    }
    Ok(())
}

fn validate_input_transform(input: &FittedInputColumn) -> QuantResult<()> {
    if input.value_kind != FeatureValueKind::Category {
        if input.required != input.median.is_none()
            || input.mean.is_none()
            || input.std.is_none_or(|std| std <= Decimal::ZERO)
            || !input.category_vocabulary.is_empty()
        {
            return Err(invalid_classical_artifact(format!(
                "numeric input `{}` carries an invalid fitted transform",
                input.feature
            )));
        }
        return Ok(());
    }
    if input.unit != FeatureUnit::None
        || input.median.is_some()
        || input.mean.is_some()
        || input.std.is_some()
        || input.category_vocabulary.is_empty()
    {
        return Err(invalid_classical_artifact(format!(
            "categorical input `{}` carries invalid numeric statistics, unit, or empty vocabulary",
            input.feature
        )));
    }
    let vocabulary = input
        .category_vocabulary
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if vocabulary.len() != input.category_vocabulary.len()
        || !input
            .category_vocabulary
            .windows(2)
            .all(|pair| pair[0] < pair[1])
    {
        return Err(invalid_classical_artifact(format!(
            "categorical input `{}` vocabulary must be unique and sorted",
            input.feature
        )));
    }
    Ok(())
}

fn validate_encoded_transform(
    transform: &FittedInputTransform,
) -> QuantResult<BTreeSet<EncodedColumnName>> {
    let names = validate_encoded_references(&transform.inputs, &transform.encoded_columns)?;
    let expected = expected_encoded_columns(&transform.inputs);
    if transform.encoded_columns.len() != expected.len() {
        return Err(invalid_classical_artifact(format!(
            "classical encoded width mismatch: expected {}, got {}",
            expected.len(),
            transform.encoded_columns.len()
        )));
    }
    for (column, (feature, kind, suffix)) in transform.encoded_columns.iter().zip(expected) {
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
    artifact: &ClassicalModelArtifact,
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

/// A versioned, content-addressable model artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelArtifact {
    /// Weighted-factor scorer (Buy-side entry ranking).
    WeightedFactor(Box<WeightedFactorModelArtifact>),
    /// Classical ML model.
    Classical(Box<ClassicalModelArtifact>),
    /// Sell-side hold-vs-exit scorer (Phase 06.1).
    SellScorer(Box<SellScorerArtifact>),
    // Onnx(OnnxArtifactRef) reserved — Phase 06.
}

/// Breaking stored-model wire version. No legacy parser is provided.
pub const MODEL_ARTIFACT_FORMAT_VERSION: u32 = 1;

#[derive(Serialize)]
pub(crate) struct StoredModelArtifactRef<'a> {
    pub format_version: u32,
    pub artifact: &'a ModelArtifact,
}

#[derive(Deserialize)]
struct StoredModelArtifact {
    format_version: u32,
    artifact: ModelArtifact,
}

impl ModelArtifact {
    /// The common provenance header, regardless of family.
    #[must_use]
    pub const fn header(&self) -> &ModelArtifactHeader {
        match self {
            Self::WeightedFactor(artifact) => &artifact.header,
            Self::Classical(artifact) => &artifact.header,
            Self::SellScorer(artifact) => &artifact.header,
        }
    }

    /// The single market category this artifact declares itself scoped to
    /// (11.2.2 category routing), or `None` for a generic cross-category
    /// scorer. Only the weighted-factor family carries a declared scope
    /// today; every other family is unconditionally generic.
    #[must_use]
    pub const fn category_scope(&self) -> Option<MarketCategory> {
        match self {
            Self::WeightedFactor(artifact) => artifact.category_scope,
            Self::Classical(_) | Self::SellScorer(_) => None,
        }
    }

    /// Validate the family-specific structural invariants.
    ///
    /// # Errors
    ///
    /// Propagates the family validator's [`ResearchError::InvalidModelArtifact`].
    pub fn validate(&self) -> QuantResult<()> {
        match self {
            Self::WeightedFactor(artifact) => artifact.validate(),
            Self::Classical(artifact) => artifact.validate(),
            Self::SellScorer(artifact) => artifact.validate(),
        }
    }

    /// The canonical content hash of this artifact (`blake3:<hex>`), the address
    /// it is stored and retrieved under.
    ///
    /// # Errors
    ///
    /// Propagates canonical-serialization failures.
    pub fn content_hash(&self) -> QuantResult<ContentHash> {
        ResearchHasher::model_artifact(self)
    }

    /// Serialize the artifact to its stored byte form.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::Serialization`] on a serde failure.
    pub fn to_bytes(&self) -> QuantResult<Vec<u8>> {
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
        match self {
            Self::WeightedFactor(weighted) => {
                matches!(weighted.return_model, ReturnModelSpec::Calibrated(_))
            }
            Self::Classical(_) | Self::SellScorer(_) => false,
        }
    }

    /// The bound `model_score` calibrator id, when the return model is
    /// `Calibrated` — the target for a deep, calibrator-liveness admission
    /// check (the enum variant alone only proves *a* calibrator was bound at
    /// publish time, not that it is still active today).
    #[must_use]
    pub const fn calibrator_ref(&self) -> Option<&CalibrationArtifactId> {
        match self {
            Self::WeightedFactor(weighted) => match &weighted.return_model {
                ReturnModelSpec::Calibrated(calibrated) => Some(&calibrated.calibrator_ref),
                ReturnModelSpec::Heuristic(_) => None,
            },
            Self::Classical(_) | Self::SellScorer(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DataQualityMultipliers, HorizonMultipliers, LiquidityMultipliers,
        MODEL_ARTIFACT_FORMAT_VERSION, ModelArtifact, ModelArtifactHeader, ReturnModelSpec,
        ScoreMultiplierSpec, SellScorerArtifact, SellScorerOutputSpec, StoredModelArtifactRef,
        SubstitutionConfidenceRules, WeightedFactorModelArtifact, model_input_contract_hash,
    };
    use quant_pivot_models::{
        enums::quant::{DataQualityStatus, DownsideSource},
        runtime_config::FactorCrossSectionConfig,
        types::{
            CalibrationArtifactId, ContentHash, ModelInputContract, ModelInputRequiredness,
            ModelInputSpec, ModelVersionId, Price, Probability, builtin_research_profiles,
            calibration::{IsotonicKnot, MonotoneMapping, ReliabilityBin, ReliabilityReport},
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::{
        factors::{
            FrozenReferenceQuantiles,
            names::{LIQUIDITY_DEPTH, MOMENTUM_ROC},
        },
        features::FeatureName,
        model::{
            artifact::{CalibratedReturnModel, FactorWeight, HeuristicReturnModel},
            calibrator::ResolvedCalibration,
            runtime::ModelFamily,
        },
        test_support::content_hash as hash,
    };

    fn input_contract() -> (ModelInputContract, ContentHash) {
        let contract = ModelInputContract::single_required("book.mid");
        let hash = model_input_contract_hash(&contract).expect("input contract hash");
        (contract, hash)
    }

    fn sample_artifact() -> WeightedFactorModelArtifact {
        let (input_contract, input_contract_hash) = input_contract();
        WeightedFactorModelArtifact {
            header: ModelArtifactHeader {
                model_version_id: ModelVersionId::from_v7(),
                model_spec_definition_hash: hash("spec"),
                profile_ref: builtin_research_profiles()
                    .expect("built-in profiles")
                    .remove(0)
                    .profile_ref,
                model_family: ModelFamily::WeightedFactor,
                feature_schema_hash: hash("aaa"),
                factor_schema_hash: hash("bbb"),
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
            },
            training_dataset_hash: hash("ccc"),
            training_input_hash: hash("ddd"),
            input_contract,
            input_contract_hash,
            weights: vec![
                FactorWeight {
                    factor: LIQUIDITY_DEPTH,
                    weight: dec!(0.6),
                },
                FactorWeight {
                    factor: MOMENTUM_ROC,
                    weight: dec!(0.4),
                },
            ],
            prediction_horizon_secs: 86_400,
            multipliers: ScoreMultiplierSpec::conservative(),
            substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
            return_model: ReturnModelSpec::heuristic_default(),
            factor_cross_section: FactorCrossSectionConfig::default(),
            frozen_reference_quantiles: FrozenReferenceQuantiles::empty(),
            objective_report: None,
            category_scope: None,
        }
    }

    #[test]
    fn weighted_artifact_serde_roundtrip_stable_hash() {
        let artifact = ModelArtifact::WeightedFactor(Box::new(sample_artifact()));
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
    fn model_artifact_rejects_legacy_unversioned_bytes() {
        let artifact = ModelArtifact::WeightedFactor(Box::new(sample_artifact()));
        let legacy = serde_json::to_vec(&artifact).expect("legacy serialization");
        assert!(ModelArtifact::from_bytes(&legacy).is_err());
    }

    #[test]
    fn model_artifact_rejects_unknown_envelope_version() {
        let artifact = ModelArtifact::WeightedFactor(Box::new(sample_artifact()));
        let bytes = serde_json::to_vec(&StoredModelArtifactRef {
            format_version: MODEL_ARTIFACT_FORMAT_VERSION + 1,
            artifact: &artifact,
        })
        .expect("serialization");
        assert!(ModelArtifact::from_bytes(&bytes).is_err());
    }

    #[test]
    fn calibrator_ref_present_only_for_calibrated_weighted_factor() {
        let heuristic = ModelArtifact::WeightedFactor(Box::new(sample_artifact()));
        assert!(heuristic.calibrator_ref().is_none());

        let calibrator_ref = CalibrationArtifactId::from_v7();
        let mut calibrated = sample_artifact();
        calibrated.return_model = ReturnModelSpec::Calibrated(CalibratedReturnModel {
            calibrator_ref: calibrator_ref.clone(),
            downside_source: DownsideSource::MfeMae,
        });
        let calibrated = ModelArtifact::WeightedFactor(Box::new(calibrated));
        assert_eq!(calibrated.calibrator_ref(), Some(&calibrator_ref));

        let sell =
            ModelArtifact::SellScorer(Box::new(sell_artifact(ModelFamily::HoldVsExitWeighted)));
        assert!(sell.calibrator_ref().is_none());
    }

    #[test]
    fn weighted_rejects_unnormalized_weights() {
        let mut artifact = sample_artifact();
        artifact.weights[0].weight = dec!(0.9);
        assert!(artifact.validate().is_err(), "weights must sum to 1");

        let mut negative = sample_artifact();
        negative.weights[0].weight = dec!(-0.1);
        negative.weights[1].weight = dec!(1.1);
        assert!(negative.validate().is_err(), "no negative weight allowed");

        assert!(sample_artifact().validate().is_ok());
    }

    fn sell_artifact(family: ModelFamily) -> SellScorerArtifact {
        let (input_contract, input_contract_hash) = input_contract();
        SellScorerArtifact {
            header: ModelArtifactHeader {
                model_version_id: ModelVersionId::from_v7(),
                model_spec_definition_hash: hash("spec"),
                profile_ref: builtin_research_profiles()
                    .expect("built-in profiles")
                    .remove(0)
                    .profile_ref,
                model_family: family,
                feature_schema_hash: hash("aaa"),
                factor_schema_hash: hash("bbb"),
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
            },
            weights: vec![
                FactorWeight {
                    factor: LIQUIDITY_DEPTH,
                    weight: dec!(0.6),
                },
                FactorWeight {
                    factor: MOMENTUM_ROC,
                    weight: dec!(0.4),
                },
            ],
            prediction_horizon_secs: 86_400,
            output_spec: SellScorerOutputSpec::conservative(),
            label_schema_hash: hash("ccc"),
            training_dataset_hash: hash("ddd"),
            training_input_hash: hash("eee"),
            input_contract,
            input_contract_hash,
            objective_report: None,
        }
    }

    #[test]
    fn weighted_rejects_contract_mutation_without_matching_hash() {
        let mut artifact = sample_artifact();
        artifact.input_contract.inputs[0].requiredness = ModelInputRequiredness::Optional;
        assert!(artifact.validate().is_err());
    }

    #[test]
    fn weighted_required_features_are_derived_from_typed_requiredness() {
        let mut artifact = sample_artifact();
        artifact.input_contract = ModelInputContract {
            inputs: vec![
                ModelInputSpec::required("book.mid"),
                ModelInputSpec::optional("market.age_secs"),
            ],
        };
        artifact.input_contract_hash =
            model_input_contract_hash(&artifact.input_contract).expect("contract hash");
        artifact.validate().expect("valid artifact");
        assert_eq!(
            artifact.required_features(),
            vec![FeatureName::new("book.mid")]
        );
    }

    #[test]
    fn sell_rejects_malformed_or_swapped_contract_hash() {
        let mut malformed = sell_artifact(ModelFamily::HoldVsExitWeighted);
        malformed
            .input_contract
            .inputs
            .push(malformed.input_contract.inputs[0].clone());
        assert!(malformed.validate().is_err());

        let mut swapped = sell_artifact(ModelFamily::HoldVsExitWeighted);
        let other = ModelInputContract::single_required("market.age_secs");
        swapped.input_contract_hash = model_input_contract_hash(&other).expect("other hash");
        assert!(swapped.validate().is_err());
    }

    #[test]
    fn sell_artifact_validates_and_rejects_wrong_family() {
        // A correctly-tagged exit scorer validates.
        assert!(
            sell_artifact(ModelFamily::HoldVsExitWeighted)
                .validate()
                .is_ok()
        );
        // A Buy family tagged into a Sell artifact is rejected (Blocker guard):
        // a Sell scorer must never load where a Buy ranker is expected.
        assert!(
            sell_artifact(ModelFamily::WeightedFactor)
                .validate()
                .is_err(),
            "non-exit model family must be rejected"
        );
    }

    #[test]
    fn sell_artifact_rejects_non_positive_alpha_scale() {
        let mut artifact = sell_artifact(ModelFamily::HoldVsExitWeighted);
        artifact.output_spec.max_exit_alpha_bps = Decimal::ZERO;
        assert!(
            artifact.validate().is_err(),
            "max_exit_alpha_bps must be > 0 (a zero/negative scale flips alpha sign)"
        );
    }

    #[test]
    fn artifact_key_is_content_addressed() {
        let artifact = ModelArtifact::WeightedFactor(Box::new(sample_artifact()));
        let digest = artifact.content_hash().expect("hash");
        let key = ModelArtifact::artifact_key(&digest).expect("key");
        assert_eq!(key.relative_path(), format!("models/{}.json", digest.hex()));
    }

    #[test]
    fn multipliers_are_exhaustive_and_monotone() {
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
    fn return_model_heuristic_and_calibrated() {
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
            est.win_probability.is_none(),
            "heuristic path never carries a calibrated win probability"
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
            mid.win_probability,
            Some(Probability::new(dec!(0.5))),
            "Kelly must receive the calibrated P(win) directly, not a value re-derived from E[r]"
        );

        // Missing resolved calibration never fabricates a value.
        let missing = calibrated.estimate(dec!(0.5), dec!(0.8), Price::new(dec!(0.4)), None);
        assert!(missing.is_err(), "missing calibration must fail closed");
    }
}
