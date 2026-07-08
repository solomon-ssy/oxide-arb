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
//! Normalization is **not** an artifact concern: the
//! [`FactorEngine`](crate::factors::FactorEngine) emits `[0, 1]` normalized factor
//! values and `factor_schema_hash` binds that contract. The weighted body instead
//! carries the *scoring* governance the runtime applies on top of already-normalized
//! factors. 3.6 fills [`ReturnModelSpec::Calibrated`] + [`TrainingObjectiveReport`].

use std::collections::BTreeMap;

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::{
        common::MarketCategory,
        quant::{DataQualityStatus, DownsideSource, ModelSerializationFormat},
    },
    types::{
        ArtifactUri, CalibrationArtifactId, ContentHash, ModelArtifactId, ModelVersionId, Price,
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{ArtifactKey, ArtifactNamespace},
    factors::FactorName,
    features::{FeatureName, NullReason},
    hashing::ResearchHasher,
    model::{
        calibrator::ResolvedCalibration,
        runtime::{ClassicalKind, ModelFamily},
    },
    precision::RESEARCH_DECIMAL_SCALE,
};

/// Tolerance for the weight-normalization check (`|Σ weights − 1| ≤ ε`).
fn weight_sum_tolerance() -> Decimal {
    Decimal::new(1, 9)
}

/// Provenance header shared by every model artifact: which version, family, and
/// the schema hashes it is bound to. Loading must reject a mismatch against the
/// active feature/factor schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelArtifactHeader {
    /// The published model version this artifact realizes.
    pub model_version_id: ModelVersionId,
    /// Model family.
    pub model_family: ModelFamily,
    /// Feature-schema hash the artifact was trained/built against.
    pub feature_schema_hash: ContentHash,
    /// Factor-schema hash the artifact was trained/built against.
    pub factor_schema_hash: ContentHash,
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
}

impl ReturnModelSpec {
    /// Estimate the expected return / downside (bps) for one candidate.
    ///
    /// `market_price` is the executable YES-leg reference price (required by
    /// the `Calibrated` formula `E[r] = P(win)·(1-p)/p − (1−P(win))`; ignored
    /// by `Heuristic`). `calibration` must be `Some` whenever `self` is
    /// `Calibrated` — the runtime factory resolves it once at load time and
    /// fails the load closed otherwise, so a `None` here is unreachable in
    /// production; defensively it yields a zero (never-fabricated) estimate,
    /// which downstream Kelly sizing already rejects via `InvalidEdgeInputs`.
    #[must_use]
    pub fn estimate(
        &self,
        composite_score: Decimal,
        confidence: Decimal,
        market_price: Price,
        calibration: Option<&ResolvedCalibration>,
    ) -> ReturnEstimate {
        match self {
            Self::Heuristic(model) => {
                let conviction = (composite_score * confidence).round_dp(RESEARCH_DECIMAL_SCALE);
                let expected_return_bps =
                    (model.max_expected_return_bps * conviction).round_dp(RESEARCH_DECIMAL_SCALE);
                let residual = (Decimal::ONE - conviction).max(Decimal::ZERO);
                let downside_bps =
                    (model.max_downside_bps * residual).round_dp(RESEARCH_DECIMAL_SCALE);
                ReturnEstimate {
                    expected_return_bps,
                    downside_bps,
                    calibrated: false,
                }
            }
            Self::Calibrated(_) => {
                let Some(resolved) = calibration else {
                    return ReturnEstimate {
                        expected_return_bps: Decimal::ZERO,
                        downside_bps: Decimal::ZERO,
                        calibrated: true,
                    };
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

/// Objective report produced by the 3.6 `WeightedFactorTrainer`. `None` for a
/// hand-authored bootstrap artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingObjectiveReport {
    /// Validation objective value achieved (e.g. validation rank IC).
    pub objective_value: Decimal,
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
    /// Features the model requires; injected into 03.1 selection eligibility.
    pub required_features: Vec<FeatureName>,
    /// Trainer objective report (filled by 3.6; `None` when hand-authored).
    pub objective_report: Option<TrainingObjectiveReport>,
    /// When set, this artifact is scoped to one market category (Phase 11.2.2
    /// category routing). `None` means the generic cross-category scorer.
    #[serde(default)]
    pub category_scope: Option<MarketCategory>,
}

impl WeightedFactorModelArtifact {
    /// Validate the structural money-adjacent invariants: a non-empty weight set,
    /// every weight non-negative, and weights normalized to sum to 1.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::InvalidModelArtifact`] on any violation.
    pub fn validate(&self) -> QuantResult<()> {
        if self.weights.is_empty() {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "weighted artifact has no factor weights".to_owned(),
            }
            .into());
        }
        let mut sum = Decimal::ZERO;
        for weight in &self.weights {
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
    /// Features the scorer requires (surfaced for eligibility / audit).
    pub required_features: Vec<FeatureName>,
    /// Trainer objective report (`None` when hand-authored).
    pub objective_report: Option<TrainingObjectiveReport>,
}

impl SellScorerArtifact {
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

/// Per-feature standardization captured at training time, so inference applies
/// the identical transform the model was fit on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreprocessingArtifact {
    /// Feature columns, in matrix column order.
    pub feature_names: Vec<FeatureName>,
    /// Per-column training mean.
    pub means: Vec<Decimal>,
    /// Per-column training standard deviation (`0` columns are left unscaled).
    pub stds: Vec<Decimal>,
}

/// One feature's global importance, as reported by the trained estimator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureImportance {
    /// The feature.
    pub feature: FeatureName,
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
    /// Location of the serialized estimator bytes in the artifact store.
    pub serialized_model_uri: ArtifactUri,
    /// Serialization format of the estimator bytes.
    pub serialization_format: ModelSerializationFormat,
    /// Frozen preprocessing (standardization) applied before inference.
    pub preprocessing: PreprocessingArtifact,
    /// Training metrics + feature importances.
    pub metrics: ClassicalModelMetrics,
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
            Self::Classical(_) => Ok(()),
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
        serde_json::to_vec(self).map_err(|error| {
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
        serde_json::from_slice(bytes).map_err(|error| {
            ResearchError::Serialization {
                detail: error.to_string(),
            }
            .into()
        })
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
}

#[cfg(test)]
mod tests {
    use super::{
        DataQualityMultipliers, HorizonMultipliers, LiquidityMultipliers, ModelArtifact,
        ModelArtifactHeader, ReturnModelSpec, ScoreMultiplierSpec, SellScorerArtifact,
        SellScorerOutputSpec, SubstitutionConfidenceRules, WeightedFactorModelArtifact,
    };
    use quant_pivot_models::{
        enums::quant::{DataQualityStatus, DownsideSource},
        types::{CalibrationArtifactId, ContentHash, ModelVersionId, Price, Probability},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::{
        factors::names::{LIQUIDITY_DEPTH, MOMENTUM_ROC},
        model::artifact::{CalibratedReturnModel, FactorWeight, HeuristicReturnModel},
        model::calibrator::{IsotonicKnot, MonotoneMapping, ResolvedCalibration},
        model::reliability::{ReliabilityBin, ReliabilityReport},
        model::runtime::ModelFamily,
    };

    fn hash(seed: &str) -> ContentHash {
        let hex = format!("{seed:0>64}").replace(|c: char| !c.is_ascii_hexdigit(), "0");
        ContentHash::parse(format!("blake3:{hex}")).expect("valid hash")
    }

    fn sample_artifact() -> WeightedFactorModelArtifact {
        WeightedFactorModelArtifact {
            header: ModelArtifactHeader {
                model_version_id: ModelVersionId::from_v7(),
                model_family: ModelFamily::WeightedFactor,
                feature_schema_hash: hash("aaa"),
                factor_schema_hash: hash("bbb"),
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
            multipliers: ScoreMultiplierSpec::conservative(),
            substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
            return_model: ReturnModelSpec::heuristic_default(),
            required_features: Vec::new(),
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
        SellScorerArtifact {
            header: ModelArtifactHeader {
                model_version_id: ModelVersionId::from_v7(),
                model_family: family,
                feature_schema_hash: hash("aaa"),
                factor_schema_hash: hash("bbb"),
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
            required_features: Vec::new(),
            objective_report: None,
        }
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
        let est = heuristic.estimate(dec!(1), dec!(1), Price::new(dec!(0.5)), None);
        assert_eq!(est.expected_return_bps, dec!(400));
        assert_eq!(est.downside_bps, dec!(0));
        assert!(!est.calibrated);

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
                    score_lo: dec!(0),
                    score_hi: dec!(1),
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
        // score=0.5 sits at/after the score=0 knot -> isotonic step yields
        // p_win=0.2 (piecewise-constant at the last knot <= score).
        // E[r] = 0.2*(1-0.4)/0.4 - 0.8 = -0.5 -> -5000 bps.
        let mid = calibrated.estimate(dec!(0.5), dec!(0.8), Price::new(dec!(0.4)), Some(&resolved));
        assert!(mid.calibrated);
        assert_eq!(mid.downside_bps, dec!(300));
        assert_eq!(mid.expected_return_bps, dec!(-5000));

        // Missing resolved calibration never fabricates a value.
        let missing = calibrated.estimate(dec!(0.5), dec!(0.8), Price::new(dec!(0.4)), None);
        assert_eq!(missing.expected_return_bps, Decimal::ZERO);
        assert_eq!(missing.downside_bps, Decimal::ZERO);
        assert!(missing.calibrated);
    }
}
