//! `ProbabilityCalibrator`: maps raw model scores to conditional winner-take-all
//! probabilities and combines them with explicit split-resolution evidence.
//!
//! Fit **only** on an independent held-out calibration split.
//!
//! Two methods, matching production guidance (scikit-learn `CalibratedClassifierCV`):
//! [`isotonic::IsotonicCalibrator`] (non-parametric monotone regression;
//! preferred with `>= min_samples_isotonic` samples) and
//! [`platt::PlattCalibrator`] (two-parameter sigmoid; data-efficient for small
//! samples or near-sigmoid miscalibration). Both live outside any optional
//! feature gate: calibration is a fail-closed, money-critical production path
//! and must never depend on an opt-in build feature (e.g. `optimize`/`argmin`).

pub mod isotonic;
pub mod platt;

use self::{isotonic::IsotonicCalibrator, platt::PlattCalibrator};

use async_trait::async_trait;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::quant::CalibrationMethod,
    hashing::CanonicalDigest,
    types::{
        CalibrationArtifactId, ContentHash, PayoutRatio, Price, Probability,
        calibration::{
            CalibratedPayoutDistribution, IsotonicKnot, MonotoneMapping, ReliabilityReport,
            SplitPayoutRateEvidence,
        },
    },
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    model::{
        ReliabilitySample, artifact::ReturnEstimate, reliability::compute_reliability,
        trainer::CancellationProbe,
    },
    precision::RESEARCH_DECIMAL_SCALE,
    stats::{count_f64, wilson_interval, wilson_z},
};

/// One allocation-independent, resolved observation admitted to a nested
/// calibration fit.
///
/// The caller must source these rows from a purge/embargo-isolated holdout.
/// Keeping the evidence hash outside the row prevents this pure fitter from
/// pretending it can prove temporal lineage on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct NestedCalibrationObservation {
    pub composite_score: Probability,
    pub token_payout_ratio: PayoutRatio,
    pub max_adverse_excursion_bps: Option<Decimal>,
}

/// Governed method selection for one nested calibration fit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct NestedCalibrationPolicy {
    pub preferred_method: CalibrationMethod,
    pub min_samples_isotonic: u64,
    pub ci_confidence: Decimal,
}

/// Complete disjoint preimage for one nested calibration fit.
pub struct NestedCalibrationFitInput<'a> {
    pub fit_observations: &'a [NestedCalibrationObservation],
    pub validation_observations: &'a [NestedCalibrationObservation],
    pub policy: NestedCalibrationPolicy,
    pub fit_evidence_hash: ContentHash,
    pub validation_evidence_hash: ContentHash,
    pub cancellation: CancellationProbe,
}

/// Content-addressed result of a purge/embargo-isolated calibration fit and
/// independent reliability evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NestedCalibration {
    pub resolved: ResolvedCalibration,
    pub method: CalibrationMethod,
    pub content_hash: ContentHash,
    pub fit_binary_sample_count: u64,
    pub fit_total_sample_count: u64,
    pub validation_binary_sample_count: u64,
    pub validation_total_sample_count: u64,
}

struct CalibrationPopulation<'a> {
    split: PayoutRatio,
    binary: Vec<&'a NestedCalibrationObservation>,
    binary_count: u64,
    total_count: u64,
}

impl<'a> CalibrationPopulation<'a> {
    fn try_new(
        role: &'static str,
        observations: &'a [NestedCalibrationObservation],
        cancellation: &CancellationProbe,
    ) -> QuantResult<Self> {
        cancellation.check("nested calibration population start")?;
        if observations.is_empty() {
            return Err(ResearchError::ValidationMethodology {
                detail: format!("nested calibration {role} population is empty"),
            }
            .into());
        }
        let split = PayoutRatio::try_new(Decimal::new(5, 1)).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("canonical split payout is invalid: {error}"),
            }
        })?;
        let mut binary = Vec::with_capacity(observations.len());
        for (index, observation) in observations.iter().enumerate() {
            if index % 1_024 == 0 {
                cancellation.check("nested calibration population scan")?;
            }
            if observation.token_payout_ratio != PayoutRatio::ZERO
                && observation.token_payout_ratio != split
                && observation.token_payout_ratio != PayoutRatio::ONE
            {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "nested calibration {role} accepts only payout ratios 0, 0.5, or 1; got {}",
                        observation.token_payout_ratio
                    ),
                }
                .into());
            }
            if observation.token_payout_ratio != split {
                binary.push(observation);
            }
        }
        let binary_count =
            u64::try_from(binary.len()).map_err(|error| ResearchError::ValidationMethodology {
                detail: format!("nested calibration {role} binary count does not fit u64: {error}"),
            })?;
        let total_count = u64::try_from(observations.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("nested calibration {role} total count does not fit u64: {error}"),
            }
        })?;
        if binary_count < 10 {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "nested calibration {role} has {binary_count} binary rows; at least 10 are required"
                ),
            }
            .into());
        }
        for (index, observation) in binary.iter().enumerate() {
            if index % 1_024 == 0 {
                cancellation.check("nested calibration downside scan")?;
            }
            if observation.max_adverse_excursion_bps.is_none() {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "nested calibration {role} score {} has no frozen adverse-excursion evidence",
                        observation.composite_score
                    ),
                }
                .into());
            }
        }
        Ok(Self {
            split,
            binary,
            binary_count,
            total_count,
        })
    }

    fn scores(&self, cancellation: &CancellationProbe) -> QuantResult<Vec<Decimal>> {
        let mut scores = Vec::with_capacity(self.binary.len());
        for (index, observation) in self.binary.iter().enumerate() {
            if index % 1_024 == 0 {
                cancellation.check("nested calibration score projection")?;
            }
            scores.push(observation.composite_score.inner());
        }
        Ok(scores)
    }

    fn outcomes(&self, cancellation: &CancellationProbe) -> QuantResult<Vec<bool>> {
        let mut outcomes = Vec::with_capacity(self.binary.len());
        for (index, observation) in self.binary.iter().enumerate() {
            if index % 1_024 == 0 {
                cancellation.check("nested calibration outcome projection")?;
            }
            outcomes.push(observation.token_payout_ratio == PayoutRatio::ONE);
        }
        Ok(outcomes)
    }

    fn reliability(
        &self,
        mapping: &MonotoneMapping,
        confidence: Decimal,
        cancellation: &CancellationProbe,
    ) -> QuantResult<ReliabilityReport> {
        let outcomes = self.outcomes(cancellation)?;
        let mut samples = Vec::with_capacity(self.binary.len());
        for (index, (observation, &won)) in self.binary.iter().zip(&outcomes).enumerate() {
            if index % 1_024 == 0 {
                cancellation.check("nested calibration reliability projection")?;
            }
            samples.push(ReliabilitySample {
                score: observation.composite_score.inner(),
                won,
                max_adverse_excursion_bps: observation.max_adverse_excursion_bps,
            });
        }
        compute_reliability(mapping, &samples, confidence, cancellation)
    }

    fn split_rate(&self, confidence: Decimal) -> QuantResult<SplitPayoutRateEvidence> {
        let split_count = self
            .total_count
            .checked_sub(self.binary_count)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "nested calibration binary population exceeds its total".to_owned(),
            })?;
        let interval = wilson_interval(
            count_f64(split_count)? / count_f64(self.total_count)?,
            self.total_count,
            wilson_z(confidence)?,
            RESEARCH_DECIMAL_SCALE,
        )?;
        Ok(SplitPayoutRateEvidence {
            total_sample_count: self.total_count,
            split_sample_count: split_count,
            empirical_probability: Probability::new(
                Decimal::from(split_count)
                    .checked_div(Decimal::from(self.total_count))
                    .ok_or_else(|| ResearchError::ValidationMethodology {
                        detail: "nested calibration population cannot be zero".to_owned(),
                    })?
                    .round_dp(18),
            ),
            wilson_ci: (Probability::new(interval.0), Probability::new(interval.1)),
            split_payout_ratio: self.split,
        })
    }
}

/// Fits a low-dimensional probability map on one held-out population and
/// computes reliability/downside evidence on a second disjoint population.
///
/// Isotonic is used only at or above its governed sample floor; below that
/// floor the data-efficient Platt map is selected explicitly.
pub struct NestedCalibrationFitter;

impl NestedCalibrationFitter {
    /// Fit and seal one nested calibration artifact.
    ///
    /// # Errors
    ///
    /// Rejects overlapping caller evidence, non-canonical payout states,
    /// missing downside evidence, fewer than ten binary rows in either
    /// population, invalid confidence policy, or a degenerate fit.
    pub fn fit(input: &NestedCalibrationFitInput<'_>) -> QuantResult<NestedCalibration> {
        input.cancellation.check("nested calibration start")?;
        if input.policy.min_samples_isotonic == 0
            || input.policy.ci_confidence <= Decimal::ZERO
            || input.policy.ci_confidence >= Decimal::ONE
            || input.fit_evidence_hash == input.validation_evidence_hash
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "nested calibration policy or disjoint evidence contract is invalid"
                    .to_owned(),
            }
            .into());
        }
        let fit =
            CalibrationPopulation::try_new("fit", input.fit_observations, &input.cancellation)?;
        let validation = CalibrationPopulation::try_new(
            "validation",
            input.validation_observations,
            &input.cancellation,
        )?;
        let scores = fit.scores(&input.cancellation)?;
        let outcomes = fit.outcomes(&input.cancellation)?;
        let method = if input.policy.preferred_method == CalibrationMethod::Isotonic
            && fit.binary_count >= input.policy.min_samples_isotonic
        {
            CalibrationMethod::Isotonic
        } else {
            CalibrationMethod::Platt
        };
        let mapping = match method {
            CalibrationMethod::Isotonic => IsotonicCalibrator::new(
                usize::try_from(input.policy.min_samples_isotonic).map_err(|error| {
                    ResearchError::ValidationMethodology {
                        detail: format!("isotonic sample floor does not fit usize: {error}"),
                    }
                })?,
            )
            .fit(&scores, &outcomes, &input.cancellation)?,
            CalibrationMethod::Platt => {
                PlattCalibrator.fit(&scores, &outcomes, &input.cancellation)?
            }
        };
        let reliability =
            validation.reliability(&mapping, input.policy.ci_confidence, &input.cancellation)?;
        let split_payout_rate = fit.split_rate(input.policy.ci_confidence)?;
        input
            .cancellation
            .check("nested calibration content hash")?;
        let content_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-nested-calibration",
            1,
            &(
                input.fit_evidence_hash,
                input.validation_evidence_hash,
                input.policy,
                method,
                fit.binary_count,
                fit.total_count,
                validation.binary_count,
                validation.total_count,
                &mapping,
                &reliability,
                split_payout_rate,
            ),
        )?;
        input
            .cancellation
            .check("nested calibration content hash completion")?;
        let resolved = ResolvedCalibration::try_new(
            CalibrationArtifactId::from_content_hash(&content_hash),
            mapping,
            reliability,
            split_payout_rate,
            &input.cancellation,
        )?;
        Ok(NestedCalibration {
            resolved,
            method,
            content_hash,
            fit_binary_sample_count: fit.binary_count,
            fit_total_sample_count: fit.total_count,
            validation_binary_sample_count: validation.binary_count,
            validation_total_sample_count: validation.total_count,
        })
    }
}

/// Maps raw model scores to empirically calibrated win probabilities.
///
/// Implementations are pure (no I/O); the caller is responsible for sourcing
/// `scores`/`outcomes` from an independent held-out calibration split; this
/// trait has no opinion on data provenance.
pub trait ProbabilityCalibrator: Send + Sync {
    /// The method this calibrator implements.
    fn method(&self) -> CalibrationMethod;

    /// Fit the monotone mapping from paired `(score, outcome)` observations.
    ///
    /// # Errors
    ///
    /// Returns an error when the sample count is insufficient for the method or
    /// the cooperative cancellation probe fires (fail-closed — never silently
    /// falls back to a different method).
    fn fit(
        &self,
        scores: &[Decimal],
        outcomes: &[bool],
        cancellation: &CancellationProbe,
    ) -> QuantResult<MonotoneMapping>;

    /// Map one raw score to a calibrated win probability, clamped to `[0, 1]`.
    ///
    /// # Errors
    ///
    /// Returns a typed inference error when the fitted mapping cannot be
    /// represented by the runtime numeric boundary.
    fn calibrate(&self, mapping: &MonotoneMapping, score: Decimal) -> QuantResult<Probability>;
}

/// Apply a fitted [`MonotoneMapping`] to one score (shared by every caller —
/// runtime scoring, reliability recomputation, and tests — so "how a mapping
/// is applied" has exactly one implementation).
pub fn apply_mapping(mapping: &MonotoneMapping, score: Decimal) -> QuantResult<Probability> {
    validate_mapping(mapping)?;
    apply_validated_mapping(mapping, score)
}

pub(crate) fn apply_validated_mapping(
    mapping: &MonotoneMapping,
    score: Decimal,
) -> QuantResult<Probability> {
    let raw = match mapping {
        MonotoneMapping::Isotonic { knots } => isotonic::interpolate(knots, score)?,
        MonotoneMapping::Platt { a, b } => platt::sigmoid(*a, *b, score)?,
    };
    Ok(Probability::new(raw.clamp(Decimal::ZERO, Decimal::ONE)))
}

/// Validate the frozen transform graph of a probability calibrator.
///
/// # Errors
///
/// Rejects empty, unordered, duplicate, out-of-range, or non-monotone
/// isotonic knots. Both artifact loading and inference call this boundary so a
/// malformed artifact cannot silently emit a plausible probability.
pub fn validate_mapping(mapping: &MonotoneMapping) -> QuantResult<()> {
    mapping
        .validate()
        .map_err(|detail| ResearchError::Inference { detail }.into())
}

pub(crate) fn validate_mapping_cancellable(
    mapping: &MonotoneMapping,
    cancellation: &CancellationProbe,
) -> QuantResult<()> {
    let MonotoneMapping::Isotonic { knots } = mapping else {
        cancellation.check("Platt mapping validation")?;
        return Ok(());
    };
    if knots.is_empty() {
        return Err(ResearchError::Inference {
            detail: "isotonic calibration mapping has no fitted knots".to_owned(),
        }
        .into());
    }
    for (index, knot) in knots.iter().enumerate() {
        if index % 1_024 == 0 {
            cancellation.check("isotonic mapping range validation")?;
        }
        if knot.probability < Decimal::ZERO || knot.probability > Decimal::ONE {
            return Err(ResearchError::Inference {
                detail: format!(
                    "isotonic probability {} at score {} is outside [0, 1]",
                    knot.probability, knot.score
                ),
            }
            .into());
        }
    }
    for (index, pair) in knots.windows(2).enumerate() {
        if index % 1_024 == 0 {
            cancellation.check("isotonic mapping order validation")?;
        }
        let [left, right] = pair else {
            continue;
        };
        if left.score >= right.score {
            return Err(ResearchError::Inference {
                detail: format!(
                    "isotonic scores must be strictly increasing, got {} then {}",
                    left.score, right.score
                ),
            }
            .into());
        }
        if left.probability > right.probability {
            return Err(ResearchError::Inference {
                detail: format!(
                    "isotonic probabilities must be non-decreasing, got {} then {}",
                    left.probability, right.probability
                ),
            }
            .into());
        }
    }
    cancellation.check("isotonic mapping validation completion")?;
    Ok(())
}

/// A `model_score` calibration artifact resolved for runtime scoring.
///
/// Carries the fitted [`MonotoneMapping`] plus its [`ReliabilityReport`] (the
/// `DownsideSource::MfeMae` per-score-bucket lookup). Bound once at model-runtime
/// load time and never re-fetched per candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedCalibration {
    pub artifact_id: CalibrationArtifactId,
    mapping: MonotoneMapping,
    pub reliability: ReliabilityReport,
    pub split_payout_rate: SplitPayoutRateEvidence,
}

impl ResolvedCalibration {
    /// Construct one immutable runtime calibration after validating its mapping
    /// exactly once. Keeping the mapping private prevents later mutation from
    /// invalidating that proof, so per-score lookup remains logarithmic.
    pub fn try_new(
        artifact_id: CalibrationArtifactId,
        mapping: MonotoneMapping,
        reliability: ReliabilityReport,
        split_payout_rate: SplitPayoutRateEvidence,
        cancellation: &CancellationProbe,
    ) -> QuantResult<Self> {
        validate_mapping_cancellable(&mapping, cancellation)?;
        Ok(Self {
            artifact_id,
            mapping,
            reliability,
            split_payout_rate,
        })
    }

    /// Hash the complete inference function while excluding audit-only artifact identity.
    ///
    /// Outer-CPCV subject and governed base-trial estimators deliberately have
    /// different lineage identities. They must nevertheless expose the exact
    /// same score mapping, reliability envelope, and split-payout model when
    /// fitted from the same rows. This commitment makes that functional parity
    /// auditable without conflating provenance with economic behavior.
    pub fn runtime_function_hash(&self) -> QuantResult<ContentHash> {
        CanonicalDigest::content_hash_typed(
            "quant-pivot/resolved-calibration-runtime-function",
            1,
            &(&self.mapping, &self.reliability, self.split_payout_rate),
        )
        .map_err(QuantError::from)
    }

    /// Calibrated terminal payout distribution for one composite score.
    pub fn calibrate_distribution(
        &self,
        composite_score: Decimal,
    ) -> QuantResult<CalibratedPayoutDistribution> {
        let distribution = CalibratedPayoutDistribution {
            winner_take_all_win_probability: apply_validated_mapping(
                &self.mapping,
                composite_score,
            )?,
            split_probability: self.split_payout_rate.empirical_probability,
            split_probability_interval: self.split_payout_rate.wilson_ci,
            split_payout_ratio: self.split_payout_rate.split_payout_ratio,
        };
        distribution
            .validate()
            .map_err(|detail| ResearchError::Inference {
                detail: format!("calibrated payout distribution is invalid: {detail}"),
            })?;
        Ok(distribution)
    }

    /// Derive expected terminal-payout return using
    /// `E[r] = E[payout] / entry_price - 1`, retaining explicit loss, split,
    /// and winner-take-all mass. Downside is the calibration split's mean
    /// `max_adverse_excursion_bps` in the candidate's
    /// **calibrated-probability** bucket (matching the reliability report's
    /// ECE bucketing — never the raw pre-calibration score bucket). Empty ECE
    /// buckets are omitted by construction, while monotone interpolation can
    /// still emit probabilities inside those gaps. Such sparse gaps use the
    /// report's worst observed absolute bucket mean as a conservative frozen
    /// envelope. An invalid market price (outside `(0, 1)`) or an artifact
    /// with no MAE evidence rejects inference. Neither condition is a business
    /// zero. Global sizing consumes the complete payout distribution through
    /// the joint scenario artifact; it never re-derives a binary probability
    /// from this expected-return scalar.
    pub fn estimate_return(
        &self,
        composite_score: Decimal,
        market_price: Price,
    ) -> QuantResult<ReturnEstimate> {
        let p = market_price.inner();
        if p <= Decimal::ZERO || p >= Decimal::ONE {
            return Err(ResearchError::Inference {
                detail: format!(
                    "calibrated return requires an executable market price in (0, 1), got {p}"
                ),
            }
            .into());
        }
        let payout_distribution = self.calibrate_distribution(composite_score)?;
        let expected_payout = payout_distribution.expected_payout().inner();
        let bps = Decimal::from(10_000);
        let expected_return_bps =
            ((expected_payout / p - Decimal::ONE) * bps).round_dp(RESEARCH_DECIMAL_SCALE);
        let downside_bps = self
            .reliability
            .conservative_downside_bps(payout_distribution.winner_take_all_win_probability.inner())
            .ok_or_else(|| ResearchError::Inference {
                detail: format!(
                    "calibrated winner-take-all probability {} has no frozen MAE downside evidence",
                    payout_distribution.winner_take_all_win_probability
                ),
            })?
            .round_dp(RESEARCH_DECIMAL_SCALE);
        Ok(ReturnEstimate {
            expected_return_bps,
            downside_bps,
            calibrated: true,
            payout_distribution: Some(payout_distribution),
        })
    }
}

/// Dependency-inversion boundary for resolving a `model_score`
/// [`CalibrationArtifactId`] into its compute-domain [`ResolvedCalibration`].
///
/// Defined here (not `quant-pivot-repository`) so `quant-pivot-research`
/// never depends on the persistence crate — mirrors [`crate::artifact::ArtifactStore`].
/// `quant-pivot-core` implements this over its `CalibrationArtifactRepository`.
#[async_trait]
pub trait CalibrationArtifactLoader: Send + Sync {
    /// Load and validate (kind = `ModelScore`) the calibration artifact.
    ///
    /// # Errors
    ///
    /// Fails closed when the artifact is absent, is the wrong `kind`, or its
    /// payload cannot be deserialized — a `Calibrated` return model must never
    /// silently fall back to an unresolved / stale calibrator.
    async fn load(&self, artifact_id: &CalibrationArtifactId) -> QuantResult<ResolvedCalibration>;
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        enums::quant::CalibrationMethod,
        types::{
            CalibrationArtifactId, ContentHash, PayoutRatio, Price, Probability,
            calibration::{
                IsotonicKnot, MonotoneMapping, ReliabilityBin, ReliabilityReport,
                SplitPayoutRateEvidence,
            },
        },
    };
    use rust_decimal_macros::dec;

    use super::{
        NestedCalibrationFitInput, NestedCalibrationFitter, NestedCalibrationObservation,
        NestedCalibrationPolicy, ResolvedCalibration,
    };
    use crate::model::CancellationProbe;

    fn content_hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64)))
            .expect("content hash")
    }

    fn nested_observations(count: usize, include_split: bool) -> Vec<NestedCalibrationObservation> {
        (0..count)
            .map(|index| {
                let is_split = include_split && index + 1 == count;
                let won = index % 2 == 1;
                NestedCalibrationObservation {
                    composite_score: Probability::new(if won { dec!(0.8) } else { dec!(0.2) }),
                    token_payout_ratio: PayoutRatio::try_new(if is_split {
                        dec!(0.5)
                    } else if won {
                        dec!(1)
                    } else {
                        dec!(0)
                    })
                    .expect("canonical payout"),
                    max_adverse_excursion_bps: Some(if won { dec!(-100) } else { dec!(-500) }),
                }
            })
            .collect()
    }

    impl NestedCalibrationPolicy {
        fn fixture() -> Self {
            Self {
                preferred_method: CalibrationMethod::Platt,
                min_samples_isotonic: 100,
                ci_confidence: dec!(0.95),
            }
        }
    }

    #[test]
    fn nested_fit_separates_evidence() {
        let fit = nested_observations(12, true);
        let validation = nested_observations(10, false);
        let fitted = NestedCalibrationFitter::fit(&NestedCalibrationFitInput {
            fit_observations: &fit,
            validation_observations: &validation,
            policy: NestedCalibrationPolicy::fixture(),
            fit_evidence_hash: content_hash('1'),
            validation_evidence_hash: content_hash('2'),
            cancellation: CancellationProbe::default(),
        })
        .expect("disjoint nested calibration");

        assert_eq!(fitted.fit_binary_sample_count, 11);
        assert_eq!(fitted.fit_total_sample_count, 12);
        assert_eq!(fitted.validation_binary_sample_count, 10);
        assert_eq!(fitted.validation_total_sample_count, 10);
        assert_eq!(fitted.resolved.reliability.n_samples, 10);
        assert_eq!(fitted.resolved.split_payout_rate.total_sample_count, 12);
        assert_eq!(fitted.resolved.split_payout_rate.split_sample_count, 1);
    }

    #[test]
    fn nested_fit_rejects_overlap() {
        let fit = nested_observations(12, true);
        let validation = nested_observations(10, false);
        let evidence_hash = content_hash('1');

        assert!(
            NestedCalibrationFitter::fit(&NestedCalibrationFitInput {
                fit_observations: &fit,
                validation_observations: &validation,
                policy: NestedCalibrationPolicy::fixture(),
                fit_evidence_hash: evidence_hash,
                validation_evidence_hash: evidence_hash,
                cancellation: CancellationProbe::default(),
            })
            .is_err()
        );
    }

    impl ResolvedCalibration {
        fn test_fixture() -> Self {
            Self {
                artifact_id: CalibrationArtifactId::from_v7(),
                mapping: MonotoneMapping::Isotonic {
                    knots: vec![
                        IsotonicKnot {
                            score: dec!(0),
                            probability: dec!(0.5),
                        },
                        IsotonicKnot {
                            score: dec!(1),
                            probability: dec!(0.5),
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
                        mean_adverse_excursion_bps: Some(dec!(-500)),
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
                    wilson_ci: (Probability::ZERO, Probability::new(dec!(0.036995))),
                    split_payout_ratio: PayoutRatio::try_new(dec!(0.5))
                        .expect("split payout ratio"),
                },
            }
        }
    }

    #[test]
    fn estimate_return_rejects_prices() {
        // `market_price` outside the *open* interval `(0, 1)` — a price of
        // exactly 0 or 1 is not an executable YES-leg price on a real
        // Polymarket order book, and `p=0` would divide by zero in
        // `(1-p)/p` if not rejected first — must be a typed inference failure,
        // never a zero-valued business prediction.
        let resolved = ResolvedCalibration::test_fixture();
        for boundary in [dec!(0), dec!(1)] {
            assert!(
                resolved
                    .estimate_return(dec!(0.5), Price::new(boundary))
                    .is_err(),
                "boundary price {boundary} must fail closed"
            );
        }
    }

    #[test]
    fn estimate_return_accepts_prices() {
        let resolved = ResolvedCalibration::test_fixture();
        let estimate = resolved
            .estimate_return(dec!(0.5), Price::new(dec!(0.01)))
            .expect("interior estimate");
        assert!(estimate.payout_distribution.is_some());
        let estimate = resolved
            .estimate_return(dec!(0.5), Price::new(dec!(0.99)))
            .expect("interior estimate");
        assert!(estimate.payout_distribution.is_some());
    }

    #[test]
    fn split_resolution_preserves_distribution() {
        let mut resolved = ResolvedCalibration::test_fixture();
        resolved.mapping = MonotoneMapping::Isotonic {
            knots: vec![
                IsotonicKnot {
                    score: dec!(0),
                    probability: dec!(0.8),
                },
                IsotonicKnot {
                    score: dec!(1),
                    probability: dec!(0.8),
                },
            ],
        };
        resolved.split_payout_rate = SplitPayoutRateEvidence {
            total_sample_count: 100,
            split_sample_count: 10,
            empirical_probability: Probability::new(dec!(0.1)),
            wilson_ci: (Probability::new(dec!(0.05)), Probability::new(dec!(0.2))),
            split_payout_ratio: PayoutRatio::try_new(dec!(0.5)).expect("split payout ratio"),
        };

        let estimate = resolved
            .estimate_return(dec!(0.5), Price::new(dec!(0.5)))
            .expect("three-state estimate");
        let distribution = estimate
            .payout_distribution
            .expect("calibrated payout distribution");

        assert_eq!(distribution.win_probability(), Probability::new(dec!(0.72)));
        assert_eq!(
            distribution.loss_probability(),
            Probability::new(dec!(0.18))
        );
        assert_eq!(distribution.split_probability, Probability::new(dec!(0.1)));
        assert_eq!(distribution.expected_payout(), Probability::new(dec!(0.77)));
        assert_eq!(estimate.expected_return_bps, dec!(5400));
    }

    #[test]
    fn estimate_rejects_missing_evidence() {
        let mut resolved = ResolvedCalibration::test_fixture();
        resolved.reliability.bins[0].mean_adverse_excursion_bps = None;
        assert!(
            resolved
                .estimate_return(dec!(0.5), Price::new(dec!(0.5)))
                .is_err()
        );
    }

    #[test]
    fn estimate_uses_sparse_envelope() {
        let mut resolved = ResolvedCalibration::test_fixture();
        resolved.reliability.bins = vec![
            ReliabilityBin {
                predicted_lo: dec!(0.2),
                predicted_hi: dec!(0.3),
                sample_count: 40,
                mean_predicted: Probability::new(dec!(0.25)),
                empirical_frequency: Probability::new(dec!(0.25)),
                wilson_ci: (Probability::new(dec!(0.1)), Probability::new(dec!(0.4))),
                mean_adverse_excursion_bps: Some(dec!(-300)),
            },
            ReliabilityBin {
                predicted_lo: dec!(0.6),
                predicted_hi: dec!(0.7),
                sample_count: 60,
                mean_predicted: Probability::new(dec!(0.65)),
                empirical_frequency: Probability::new(dec!(0.65)),
                wilson_ci: (Probability::new(dec!(0.5)), Probability::new(dec!(0.8))),
                mean_adverse_excursion_bps: Some(dec!(-700)),
            },
        ];
        let estimate = resolved
            .estimate_return(dec!(0.5), Price::new(dec!(0.5)))
            .expect("sparse reliability envelope");
        assert_eq!(estimate.downside_bps, dec!(700));
    }

    #[test]
    fn empty_isotonic_mapping_rejects() {
        let mapping = MonotoneMapping::Isotonic { knots: Vec::new() };
        assert!(super::apply_mapping(&mapping, dec!(0.5)).is_err());
    }
}
