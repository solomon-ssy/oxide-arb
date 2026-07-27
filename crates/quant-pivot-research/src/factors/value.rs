//! Factor compute-domain value types: [`FactorName`], [`FactorValue`],
//! [`FactorDefinitionDocument`], explanation, and the engine's per-market
//! [`MarketFactorOutcome`].
//!
//! A factor score is **not** a recommendation score — it is a normalized,
//! explainable model input. The pipeline splits responsibilities cleanly:
//! a [`FactorComputer`](crate::factors::FactorComputer) produces a per-market
//! [`RawFactor`] (no normalization, no cross-section), and the
//! [`FactorEngine`](crate::factors::FactorEngine) applies the (possibly
//! cross-sectional) normalization to yield the final [`FactorValue`].
//!
//! The normalization **method** ([`FactorNormalization`]) and executable
//! computation contract are sealed into the serving-plane hash. Distributional
//! parameter values remain owned by the immutable scoring profile.

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
pub(super) use quant_pivot_models::{
    enums::{
        factor::{FactorFamily, FactorIndeterminateReason, FactorValueState, NormalizationSource},
        quant::FactorDirection,
    },
    types::{
        FactorDefinitionId, MarketId, Probability,
        factor::{
            FactorAlphaOrientation, FactorContextEffect, FactorDefinitionDocument,
            FactorDefinitionRef, FactorDriver, FactorExplanation, FactorOutputSemantics,
            FactorRawValue,
        },
        stable_name::{FactorName, FeatureName},
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::factors::normalize::NormalizedFactor;

/// The computed output of a factor for one feature vector.
///
/// The `normalization` outcome is explicit: a `Scored` value carries the
/// `[0, 1]` normalized magnitude and its provenance; a factor whose inputs were
/// missing is `MissingInput` (`confidence = 0`); a factor whose cross-section
/// was too small or degenerate is `Indeterminate` (with a recorded reason) —
/// **never a silent neutral 0.5**. The `direction` carries the sign of the
/// factor's effect on a candidate score.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorValue {
    /// Governing definition id.
    pub definition_id: FactorDefinitionId,
    /// Factor name.
    pub name: FactorName,
    /// Factor family.
    pub family: FactorFamily,
    /// Raw (pre-normalization) value, when defined.
    pub raw_value: Option<Decimal>,
    /// Normalization outcome (scored / missing input / indeterminate).
    pub normalization: NormalizedFactor,
    /// Contribution direction.
    pub direction: FactorDirection,
    /// Confidence in the factor value.
    pub confidence: Probability,
    /// Explanation of the value.
    pub explanation: FactorExplanation,
    /// Features that fed this factor.
    pub input_feature_refs: Vec<FeatureName>,
}

/// Canonical typed projection consumed by every trainer/runtime scoring head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactorScoringProjection {
    /// Signed alpha in the definition's declared reference orientation.
    OutcomeAlpha {
        orientation: FactorAlphaOrientation,
        /// Signed normalized strength before reliability/confidence weighting.
        strength: Decimal,
        confidence: Probability,
    },
    /// Side-neutral opportunity adequacy. This may scale magnitude/confidence
    /// but can never select or reverse an outcome side.
    Context {
        adequacy: Probability,
        confidence: Probability,
    },
    /// Auditable non-estimator output. Callers must preserve it in lineage but
    /// must not coerce it into either scoring head.
    Diagnostic {
        score: Probability,
        confidence: Probability,
    },
}

impl FactorValue {
    /// Validate one value against its exact sealed definition revision.
    pub fn validate_against(&self, revision: &FactorDefinitionRef) -> QuantResult<()> {
        revision
            .validate()
            .map_err(|error| ResearchError::Serialization {
                detail: format!(
                    "factor revision {} is invalid while validating value: {error}",
                    revision.factor_definition_id()
                ),
            })?;
        let definition = revision.definition();
        let drivers_are_canonical = self
            .explanation
            .drivers
            .windows(2)
            .all(|pair| pair[0].feature_name < pair[1].feature_name)
            && self
                .explanation
                .drivers
                .iter()
                .all(|driver| definition.input_features.contains(&driver.feature_name));
        let confidence = self.confidence.inner();
        let normalized_in_range = self
            .normalized_score()
            .is_none_or(|score| (Decimal::ZERO..=Decimal::ONE).contains(&score.inner()));
        let expected_direction = self
            .raw_value
            .and_then(|raw| definition.contribution_direction(raw))
            .unwrap_or(FactorDirection::Neutral);
        let raw_is_valid = self.raw_value.is_none_or(|raw| {
            FactorRawValue::try_from(raw).is_ok() && definition.normalization_input(raw).is_some()
        });
        let tuple_is_valid = match (&self.normalization, self.raw_value) {
            (NormalizedFactor::Scored { score, .. }, Some(raw)) => {
                (Decimal::ZERO..=Decimal::ONE).contains(&score.inner())
                    && (!definition.is_outcome_alpha() || !raw.is_zero() || score.inner().is_zero())
                    && (!definition.is_outcome_alpha() || !raw.is_zero() || confidence.is_zero())
            }
            (NormalizedFactor::MissingInput | NormalizedFactor::NotApplicable, None) => {
                confidence.is_zero()
            }
            (NormalizedFactor::Indeterminate { .. }, _) => confidence.is_zero(),
            _ => false,
        };
        let exact = self.definition_id == revision.factor_definition_id()
            && self.name == definition.name
            && self.family == definition.family
            && self.input_feature_refs == definition.input_features
            && self.direction == expected_direction
            && (Decimal::ZERO..=Decimal::ONE).contains(&confidence)
            && normalized_in_range
            && raw_is_valid
            && tuple_is_valid
            && drivers_are_canonical;
        if !exact {
            return Err(ResearchError::Serialization {
                detail: format!(
                    "factor value `{}` does not exactly project sealed revision {}",
                    self.name,
                    revision.factor_definition_id()
                ),
            }
            .into());
        }
        Ok(())
    }

    /// Project a validated scored value into exactly one model head.
    pub fn scoring_projection(
        &self,
        revision: &FactorDefinitionRef,
    ) -> QuantResult<Option<FactorScoringProjection>> {
        self.validate_against(revision)?;
        let Some(score) = self.normalized_score() else {
            return Ok(None);
        };
        let definition = revision.definition();
        Ok(match definition.output {
            FactorOutputSemantics::OutcomeAlpha { orientation } => {
                Some(FactorScoringProjection::OutcomeAlpha {
                    orientation,
                    strength: Decimal::from(self.direction.as_i8()) * score.inner(),
                    confidence: self.confidence,
                })
            }
            FactorOutputSemantics::Context { .. } => {
                let adequacy = definition.context_adequacy(score).ok_or_else(|| {
                    ResearchError::Serialization {
                        detail: format!(
                            "context factor `{}` has no governed adequacy projection",
                            self.name
                        ),
                    }
                })?;
                Some(FactorScoringProjection::Context {
                    adequacy,
                    confidence: self.confidence,
                })
            }
            FactorOutputSemantics::Diagnostic => Some(FactorScoringProjection::Diagnostic {
                score,
                confidence: self.confidence,
            }),
        })
    }

    /// The normalized `[0, 1]` score when the factor was scored, else `None`.
    #[must_use]
    pub const fn normalized_score(&self) -> Option<Probability> {
        match &self.normalization {
            NormalizedFactor::Scored { score, .. } => Some(*score),
            NormalizedFactor::MissingInput
            | NormalizedFactor::NotApplicable
            | NormalizedFactor::Indeterminate { .. } => None,
        }
    }

    /// Whether the factor carries a usable normalized score.
    #[must_use]
    pub const fn is_scored(&self) -> bool {
        matches!(self.normalization, NormalizedFactor::Scored { .. })
    }

    /// Whether the factor does not apply to this market's structure.
    #[must_use]
    pub const fn is_not_applicable(&self) -> bool {
        matches!(self.normalization, NormalizedFactor::NotApplicable)
    }

    /// The authoritative persisted state of this factor value (orthogonal to
    /// `indeterminate_reason`, which is populated only for the indeterminate
    /// case). Keeps a structurally not-applicable factor durably distinct from a
    /// missing-input one.
    #[must_use]
    pub const fn value_state(&self) -> FactorValueState {
        match &self.normalization {
            NormalizedFactor::Scored { .. } => FactorValueState::Scored,
            NormalizedFactor::MissingInput => FactorValueState::MissingInput,
            NormalizedFactor::NotApplicable => FactorValueState::NotApplicable,
            NormalizedFactor::Indeterminate { .. } => FactorValueState::Indeterminate,
        }
    }

    /// How the score was derived, when the factor was scored.
    #[must_use]
    pub const fn normalization_source(&self) -> Option<NormalizationSource> {
        match &self.normalization {
            NormalizedFactor::Scored { source, .. } => Some(*source),
            NormalizedFactor::MissingInput
            | NormalizedFactor::NotApplicable
            | NormalizedFactor::Indeterminate { .. } => None,
        }
    }

    /// The indeterminate reason, when the factor could not be normalized.
    #[must_use]
    pub const fn indeterminate_reason(&self) -> Option<FactorIndeterminateReason> {
        match &self.normalization {
            NormalizedFactor::Indeterminate { reason } => Some(*reason),
            NormalizedFactor::Scored { .. }
            | NormalizedFactor::MissingInput
            | NormalizedFactor::NotApplicable => None,
        }
    }
}

/// How the engine must treat a factor's raw cell before/around normalization.
///
/// Normalizable cells flow through the cross-sectional normalizer; the other two
/// short-circuit it with an explicit outcome (a binary market's neg-risk factor
/// is `NotApplicable`; a neg-risk market missing a leg book is `Indeterminate`)
/// — never a silent zero and never a fabricated cross-section entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case", tag = "eligibility", content = "reason")]
pub enum RawFactorEligibility {
    /// The raw value participates in the same-`as_of` cross-section normalization.
    #[default]
    Normalizable,
    /// The factor does not apply to this market's structure (⇒ `NotApplicable`).
    NotApplicable,
    /// The factor should have computed but an input was structurally absent
    /// (⇒ `Indeterminate { reason }`), e.g. a neg-risk sibling leg's book.
    Indeterminate(FactorIndeterminateReason),
}

/// A per-market raw factor output, prior to (possibly cross-sectional)
/// normalization.
///
/// Produced by [`FactorComputer::compute_raw`](crate::factors::FactorComputer);
/// pure, side-effect-free, and deterministic. `raw_value = None` means the
/// factor's inputs were unavailable for this market (never silently zero).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawFactor {
    /// Governing definition id.
    pub definition_id: FactorDefinitionId,
    /// Factor name.
    pub name: FactorName,
    /// Factor family.
    pub family: FactorFamily,
    /// Raw value, or `None` when the inputs were unavailable.
    pub raw_value: Option<Decimal>,
    /// How the engine must treat this cell around normalization.
    pub eligibility: RawFactorEligibility,
    /// Contribution direction.
    pub direction: FactorDirection,
    /// Confidence in the raw value (0 when inputs are missing).
    pub confidence: Probability,
    /// One-line human summary carried into the final explanation.
    pub headline: String,
    /// Ranked, signed drivers carried into the final explanation.
    pub drivers: Vec<FactorDriver>,
    /// Features that fed this factor.
    pub input_feature_refs: Vec<FeatureName>,
}

impl RawFactor {
    /// Quantize the raw value and canonicalize derived fields before hashing.
    pub(super) fn canonicalize_against(
        &mut self,
        revision: &FactorDefinitionRef,
    ) -> QuantResult<()> {
        self.drivers
            .sort_unstable_by(|left, right| left.feature_name.cmp(&right.feature_name));
        if let Some(raw) = self.raw_value {
            self.raw_value = Some(
                FactorRawValue::quantize(raw)
                    .map_err(|error| ResearchError::FactorComputation {
                        detail: format!(
                            "factor `{}` raw output is not representable: {error}",
                            self.name
                        ),
                    })?
                    .inner(),
            );
        }
        self.direction = self
            .raw_value
            .and_then(|raw| revision.definition().contribution_direction(raw))
            .unwrap_or(FactorDirection::Neutral);
        self.validate_against(revision)
    }

    /// Verify that a computer output belongs to the exact sealed revision.
    ///
    /// Identity and lineage are owned by the serving plane. Computers may emit
    /// inputs in computation order, but the set must exactly equal the declared
    /// canonical inputs; the final persisted value is projected from the sealed
    /// definition, never from this transient copy.
    pub fn validate_against(&self, revision: &FactorDefinitionRef) -> QuantResult<()> {
        let definition = revision.definition();
        let mut inputs = self.input_feature_refs.clone();
        inputs.sort_unstable();
        let original_len = inputs.len();
        inputs.dedup();
        let canonical_drivers = self
            .drivers
            .windows(2)
            .all(|pair| pair[0].feature_name < pair[1].feature_name)
            && self
                .drivers
                .iter()
                .all(|driver| definition.input_features.contains(&driver.feature_name));
        let expected_direction = self
            .raw_value
            .and_then(|raw| definition.contribution_direction(raw))
            .unwrap_or(FactorDirection::Neutral);
        let exact = self.definition_id == revision.factor_definition_id()
            && self.name == definition.name
            && self.family == definition.family
            && self.direction == expected_direction
            && inputs.len() == original_len
            && inputs == definition.input_features
            && canonical_drivers;
        if !exact {
            return Err(ResearchError::FactorComputation {
                detail: format!(
                    "raw factor `{}` does not exactly project sealed revision {}",
                    self.name,
                    revision.factor_definition_id()
                ),
            }
            .into());
        }
        Ok(())
    }
}

/// A factor value paired with its **transient** scoring eligibility.
///
/// `contributes` / `below_confidence_floor` are scoring decisions derived from
/// the runtime `min_factor_confidence` floor and `missing_factor_policy`. They
/// are **not persisted** (the floor can hot-update and weighting is a model-layer
/// concern): the immutable [`FactorValue`] is the persisted fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScoredFactor {
    /// The persisted factor fact.
    pub value: FactorValue,
    /// Whether this factor contributes to downstream scoring (scored and at or
    /// above the confidence floor).
    pub contributes: bool,
    /// Whether the factor's confidence fell below the configured floor.
    pub below_confidence_floor: bool,
}

/// Whether a market's factor bundle is eligible to proceed to model inference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum FactorEligibility {
    /// The market proceeds; its factors feed the model runtime.
    Eligible,
    /// A required factor was missing / below floor under `RejectCandidate`; the
    /// market is excluded from the candidate set (it produces no factor rows).
    RejectCandidate {
        /// Why the market was rejected.
        reason: String,
    },
    /// A required factor is structurally not applicable to this market (e.g. a
    /// binary market required to carry a neg-risk full-leg factor). Distinct from
    /// `RejectCandidate` (a data/quality reject): the market is excluded because
    /// the required signal cannot exist for its structure, not because it is
    /// low quality.
    NotApplicable {
        /// Why the required factor does not apply.
        reason: String,
    },
}

impl FactorEligibility {
    /// Whether this market proceeds downstream.
    #[must_use]
    pub const fn is_eligible(&self) -> bool {
        matches!(self, Self::Eligible)
    }
}

/// The engine's complete per-market factor outcome: every enabled factor with
/// its transient scoring eligibility, plus the market-level eligibility verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketFactorOutcome {
    /// The market this outcome describes.
    pub market_id: MarketId,
    /// Frozen decision time for the factor computation.
    pub decision_at: DateTime<Utc>,
    /// Market-level eligibility verdict.
    pub eligibility: FactorEligibility,
    /// Every enabled factor for this market, with transient scoring flags.
    pub factors: Vec<ScoredFactor>,
}
