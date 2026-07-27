//! Canonical OutcomeAlpha/Context scoring algebra shared by training and serving.

use std::collections::{BTreeMap, HashMap, HashSet};

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::quant::OutcomeSide,
    runtime_config::{DecimalValue, FactorHeadConfig},
    types::{
        FactorDefinitionId, OutcomeTokenBinding,
        factor::{
            FactorAlphaOrientation, FactorDefinitionRef, FactorOutputSemantics, FactorServingPlane,
        },
        stable_name::FactorName,
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    factors::value::{FactorScoringProjection, FactorValue},
    precision::RESEARCH_DECIMAL_SCALE,
};

/// Simplex weight for one content-addressed `OutcomeAlpha` definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlphaFactorWeight {
    pub factor_definition_id: FactorDefinitionId,
    pub factor: FactorName,
    pub weight: Decimal,
}

/// Independent context penalty plus its reliability-coverage weight.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextFactorWeight {
    pub factor_definition_id: FactorDefinitionId,
    pub factor: FactorName,
    /// Simplex weight used only for context-quality coverage.
    pub coverage_weight: Decimal,
    /// Independent `[0, 1]` penalty strength; these values do not form a simplex.
    pub penalty_strength: Decimal,
}

/// Complete estimator-head binding for every non-diagnostic factor in a plane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactorHeadSpec {
    pub alpha_weights: Vec<AlphaFactorWeight>,
    pub context_weights: Vec<ContextFactorWeight>,
    /// Absolute canonical-YES alpha at or below this threshold emits no side.
    pub alpha_deadband: Decimal,
}

/// Fully separated result of the canonical factor-head transform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorHeadScore {
    /// Signed alpha in canonical-YES space.
    pub yes_alpha: Decimal,
    /// Applicable-weight-normalized alpha evidence quality.
    pub alpha_quality: Decimal,
    /// Side-neutral multiplicative opportunity adequacy.
    pub context_multiplier: Decimal,
    /// Context evidence coverage/reliability.
    pub context_quality: Decimal,
    /// Agreement of signed alpha evidence, with cancellation mapped to zero.
    pub directional_coherence: Decimal,
    /// Reliability, not a calibrated probability.
    pub reliability: Decimal,
    /// Absolute entry ranking magnitude after context and policy multipliers.
    pub composite_score: Decimal,
    /// Side selected only by canonical alpha outside the governed deadband.
    pub outcome_side: Option<OutcomeSide>,
}

impl FactorHeadSpec {
    /// Expand a governed name-keyed seed over one exact sealed serving plane.
    ///
    /// Empty maps produce canonical uniform weights. A non-empty map must
    /// exactly cover its semantic head; diagnostics and factors belonging to
    /// the other head are rejected rather than silently ignored.
    pub fn from_config(plane: &FactorServingPlane, config: &FactorHeadConfig) -> QuantResult<Self> {
        plane
            .validate()
            .map_err(|error| invalid_spec(format!("invalid factor serving plane: {error}")))?;
        validate_deadband(config.alpha_deadband.value())?;
        validate_unit(
            "default context penalty strength",
            config.default_context_penalty_strength.value(),
        )?;

        let alpha_revisions = plane
            .definitions()
            .iter()
            .filter(|revision| revision.definition().is_outcome_alpha())
            .collect::<Vec<_>>();
        let context_revisions = plane
            .definitions()
            .iter()
            .filter(|revision| revision.definition().is_context())
            .collect::<Vec<_>>();
        if alpha_revisions.is_empty() {
            return Err(invalid_spec(
                "factor serving plane has no OutcomeAlpha revision".to_owned(),
            ));
        }
        validate_config_coverage(&alpha_revisions, &config.alpha_seed_weights, "alpha seed")?;
        validate_config_coverage(
            &context_revisions,
            &config.context_coverage_weights,
            "context coverage",
        )?;
        validate_config_coverage(
            &context_revisions,
            &config.context_penalty_strengths,
            "context penalty",
        )?;

        let alpha_count = u64::try_from(alpha_revisions.len())
            .map_err(|error| invalid_spec(format!("alpha head width is unsupported: {error}")))?;
        let alpha_default = Decimal::ONE / Decimal::from(alpha_count);
        let context_default = if context_revisions.is_empty() {
            Decimal::ZERO
        } else {
            let context_count = u64::try_from(context_revisions.len()).map_err(|error| {
                invalid_spec(format!("context head width is unsupported: {error}"))
            })?;
            Decimal::ONE / Decimal::from(context_count)
        };
        let alpha_weights = alpha_revisions
            .iter()
            .map(|revision| AlphaFactorWeight {
                factor_definition_id: revision.factor_definition_id(),
                factor: revision.factor_name().clone(),
                weight: config
                    .alpha_seed_weights
                    .get(revision.factor_name().as_str())
                    .map_or(alpha_default, DecimalValue::value),
            })
            .collect();
        let context_weights = context_revisions
            .iter()
            .map(|revision| ContextFactorWeight {
                factor_definition_id: revision.factor_definition_id(),
                factor: revision.factor_name().clone(),
                coverage_weight: config
                    .context_coverage_weights
                    .get(revision.factor_name().as_str())
                    .map_or(context_default, DecimalValue::value),
                penalty_strength: config
                    .context_penalty_strengths
                    .get(revision.factor_name().as_str())
                    .map_or_else(
                        || config.default_context_penalty_strength.value(),
                        DecimalValue::value,
                    ),
            })
            .collect();
        let spec = Self {
            alpha_weights,
            context_weights,
            alpha_deadband: config.alpha_deadband.value(),
        };
        spec.validate(plane)?;
        Ok(spec)
    }

    /// Validate exact, revision-bound head coverage against a sealed plane.
    pub fn validate(&self, plane: &FactorServingPlane) -> QuantResult<()> {
        plane
            .validate()
            .map_err(|error| invalid_spec(format!("invalid factor serving plane: {error}")))?;
        validate_deadband(self.alpha_deadband)?;
        validate_alpha_weights(&self.alpha_weights)?;
        validate_context_weights(&self.context_weights)?;

        let mut alpha_ids = HashSet::with_capacity(self.alpha_weights.len());
        let mut context_ids = HashSet::with_capacity(self.context_weights.len());
        for weight in &self.alpha_weights {
            alpha_ids.insert(weight.factor_definition_id);
            let revision = revision_by_id(plane, weight.factor_definition_id)?;
            if revision.factor_name() != &weight.factor
                || !matches!(
                    revision.definition().output,
                    FactorOutputSemantics::OutcomeAlpha { .. }
                )
            {
                return Err(invalid_spec(format!(
                    "alpha weight `{}` does not bind an OutcomeAlpha revision",
                    weight.factor
                )));
            }
        }
        for weight in &self.context_weights {
            context_ids.insert(weight.factor_definition_id);
            let revision = revision_by_id(plane, weight.factor_definition_id)?;
            if revision.factor_name() != &weight.factor
                || !matches!(
                    revision.definition().output,
                    FactorOutputSemantics::Context { .. }
                )
            {
                return Err(invalid_spec(format!(
                    "context weight `{}` does not bind a Context revision",
                    weight.factor
                )));
            }
        }
        for revision in plane.definitions() {
            match revision.definition().output {
                FactorOutputSemantics::OutcomeAlpha { .. }
                    if !alpha_ids.contains(&revision.factor_definition_id()) =>
                {
                    return Err(invalid_spec(format!(
                        "OutcomeAlpha factor `{}` is missing from the alpha head",
                        revision.factor_name()
                    )));
                }
                FactorOutputSemantics::Context { .. }
                    if !context_ids.contains(&revision.factor_definition_id()) =>
                {
                    return Err(invalid_spec(format!(
                        "Context factor `{}` is missing from the context head",
                        revision.factor_name()
                    )));
                }
                FactorOutputSemantics::Diagnostic
                    if alpha_ids.contains(&revision.factor_definition_id())
                        || context_ids.contains(&revision.factor_definition_id()) =>
                {
                    return Err(invalid_spec(format!(
                        "Diagnostic factor `{}` cannot carry estimator weight",
                        revision.factor_name()
                    )));
                }
                FactorOutputSemantics::OutcomeAlpha { .. }
                | FactorOutputSemantics::Context { .. }
                | FactorOutputSemantics::Diagnostic => {}
            }
        }
        Ok(())
    }
}

fn validate_config_coverage(
    revisions: &[&FactorDefinitionRef],
    values: &BTreeMap<String, DecimalValue>,
    head: &str,
) -> QuantResult<()> {
    if values.is_empty() {
        return Ok(());
    }
    if values.len() != revisions.len() {
        return Err(invalid_spec(format!(
            "{head} config has {} entries but the sealed semantic head has {}",
            values.len(),
            revisions.len()
        )));
    }
    for revision in revisions {
        if !values.contains_key(revision.factor_name().as_str()) {
            return Err(invalid_spec(format!(
                "{head} config omits sealed factor `{}`",
                revision.factor_name()
            )));
        }
    }
    for name in values.keys() {
        if !FactorName::new(name).is_canonical() {
            return Err(invalid_spec(format!(
                "{head} config contains non-canonical factor name `{name}`"
            )));
        }
        if !revisions
            .iter()
            .any(|revision| revision.factor_name().as_str() == name)
        {
            return Err(invalid_spec(format!(
                "{head} config contains factor `{name}` outside its sealed semantic head"
            )));
        }
    }
    Ok(())
}

/// Evaluate the sole canonical factor-head formula.
pub fn score_factor_heads(
    factors: &[FactorValue],
    plane: &FactorServingPlane,
    spec: &FactorHeadSpec,
    outcome_binding: &OutcomeTokenBinding,
    substitution_reliability: Decimal,
    policy_multiplier: Decimal,
) -> QuantResult<FactorHeadScore> {
    spec.validate(plane)?;
    outcome_binding
        .validate()
        .map_err(|error| invalid_input(format!("invalid outcome binding: {error}")))?;
    validate_unit("substitution reliability", substitution_reliability)?;
    validate_unit("policy multiplier", policy_multiplier)?;
    let values = exact_factor_values(factors, plane)?;
    let alpha_weights = index_alpha(&spec.alpha_weights);
    let context_weights = index_context(&spec.context_weights);

    let mut alpha_denominator = Decimal::ZERO;
    let mut alpha_numerator = Decimal::ZERO;
    let mut alpha_quality_mass = Decimal::ZERO;
    let mut alpha_absolute_mass = Decimal::ZERO;
    let mut context_denominator = Decimal::ZERO;
    let mut context_quality_mass = Decimal::ZERO;
    let mut context_multiplier = Decimal::ONE;

    for revision in plane.definitions() {
        let value = values
            .get(&revision.factor_definition_id())
            .copied()
            .ok_or_else(|| {
                invalid_input(format!(
                    "factor row omitted sealed revision {}",
                    revision.factor_definition_id()
                ))
            })?;
        let projection = value.scoring_projection(revision)?;
        if value.is_not_applicable() {
            continue;
        }
        if projection.is_none() && revision.definition().required {
            return Err(invalid_input(format!(
                "required factor `{}` is missing or indeterminate",
                revision.factor_name()
            )));
        }
        match revision.definition().output {
            FactorOutputSemantics::OutcomeAlpha { .. } => {
                let weight = alpha_weights
                    .get(&revision.factor_definition_id())
                    .copied()
                    .ok_or_else(|| {
                        invalid_spec(format!("alpha head omitted `{}`", revision.factor_name()))
                    })?;
                alpha_denominator += weight;
                if let Some(FactorScoringProjection::OutcomeAlpha {
                    orientation,
                    strength,
                    confidence,
                }) = projection
                {
                    let yes_strength = match orientation {
                        FactorAlphaOrientation::FeatureToken => {
                            Decimal::from(outcome_binding.feature_to_yes_sign()) * strength
                        }
                        FactorAlphaOrientation::CanonicalYes => strength,
                    };
                    let quality = confidence.inner();
                    alpha_numerator += weight * quality * yes_strength;
                    alpha_quality_mass += weight * quality;
                    alpha_absolute_mass += weight * quality * yes_strength.abs();
                }
            }
            FactorOutputSemantics::Context { .. } => {
                let weight = context_weights
                    .get(&revision.factor_definition_id())
                    .copied()
                    .ok_or_else(|| {
                        invalid_spec(format!("context head omitted `{}`", revision.factor_name()))
                    })?;
                context_denominator += weight.coverage_weight;
                if let Some(FactorScoringProjection::Context {
                    adequacy,
                    confidence,
                }) = projection
                {
                    let quality = confidence.inner();
                    context_quality_mass += weight.coverage_weight * quality;
                    let penalty = Decimal::ONE
                        - weight.penalty_strength * quality * (Decimal::ONE - adequacy.inner());
                    context_multiplier *= penalty;
                }
            }
            FactorOutputSemantics::Diagnostic => {}
        }
    }

    let yes_alpha = rounded(safe_ratio(alpha_numerator, alpha_denominator));
    let alpha_quality = safe_ratio(alpha_quality_mass, alpha_denominator);
    let context_quality = if context_denominator.is_zero() {
        Decimal::ONE
    } else {
        safe_ratio(context_quality_mass, context_denominator)
    };
    let directional_coherence = if alpha_absolute_mass.is_zero() {
        Decimal::ZERO
    } else {
        safe_ratio(alpha_numerator.abs(), alpha_absolute_mass)
    };
    let reliability =
        substitution_reliability * alpha_quality.min(context_quality) * directional_coherence;
    let composite_score = yes_alpha.abs() * context_multiplier * policy_multiplier;
    let outcome_side = if yes_alpha.abs() <= spec.alpha_deadband {
        None
    } else if yes_alpha.is_sign_positive() {
        Some(OutcomeSide::Yes)
    } else {
        Some(OutcomeSide::No)
    };

    Ok(FactorHeadScore {
        yes_alpha,
        alpha_quality: unit_rounded(alpha_quality),
        context_multiplier: unit_rounded(context_multiplier),
        context_quality: unit_rounded(context_quality),
        directional_coherence: unit_rounded(directional_coherence),
        reliability: unit_rounded(reliability),
        composite_score: unit_rounded(composite_score),
        outcome_side,
    })
}

/// Convert canonical-YES support into signed exit-vs-hold market evidence.
pub fn sell_market_alpha(
    score: &FactorHeadScore,
    held_binding: &OutcomeTokenBinding,
) -> QuantResult<Decimal> {
    held_binding
        .validate()
        .map_err(|error| invalid_input(format!("invalid held outcome binding: {error}")))?;
    if !(Decimal::NEGATIVE_ONE..=Decimal::ONE).contains(&score.yes_alpha) {
        return Err(invalid_input(format!(
            "canonical YES alpha must be in [-1, 1], got {}",
            score.yes_alpha
        )));
    }
    let held_sign = Decimal::from(held_binding.feature_to_yes_sign());
    Ok(rounded(-held_sign * score.yes_alpha))
}

#[derive(Clone, Copy)]
struct ContextWeight {
    coverage_weight: Decimal,
    penalty_strength: Decimal,
}

fn exact_factor_values<'a>(
    factors: &'a [FactorValue],
    plane: &FactorServingPlane,
) -> QuantResult<HashMap<FactorDefinitionId, &'a FactorValue>> {
    let plane_ids = plane
        .definitions()
        .iter()
        .map(FactorDefinitionRef::factor_definition_id)
        .collect::<HashSet<_>>();
    let mut values = HashMap::with_capacity(factors.len());
    for value in factors {
        if !plane_ids.contains(&value.definition_id) {
            return Err(invalid_input(format!(
                "factor row contains revision {} outside the serving plane",
                value.definition_id
            )));
        }
        if values.insert(value.definition_id, value).is_some() {
            return Err(invalid_input(format!(
                "factor row duplicates revision {}",
                value.definition_id
            )));
        }
    }
    if values.len() != plane.definitions().len() {
        return Err(invalid_input(format!(
            "factor row has {} revisions, serving plane requires {}",
            values.len(),
            plane.definitions().len()
        )));
    }
    Ok(values)
}

fn index_alpha(weights: &[AlphaFactorWeight]) -> HashMap<FactorDefinitionId, Decimal> {
    weights
        .iter()
        .map(|weight| (weight.factor_definition_id, weight.weight))
        .collect()
}

fn index_context(weights: &[ContextFactorWeight]) -> HashMap<FactorDefinitionId, ContextWeight> {
    weights
        .iter()
        .map(|weight| {
            (
                weight.factor_definition_id,
                ContextWeight {
                    coverage_weight: weight.coverage_weight,
                    penalty_strength: weight.penalty_strength,
                },
            )
        })
        .collect()
}

fn validate_alpha_weights(weights: &[AlphaFactorWeight]) -> QuantResult<()> {
    if weights.is_empty() {
        return Err(invalid_spec("alpha head is empty".to_owned()));
    }
    let indexed = weights
        .iter()
        .map(|weight| (weight.factor_definition_id, &weight.factor, weight.weight))
        .collect::<Vec<_>>();
    validate_canonical_weights(&indexed, "alpha")
}

fn validate_context_weights(weights: &[ContextFactorWeight]) -> QuantResult<()> {
    if weights.is_empty() {
        return Ok(());
    }
    let indexed = weights
        .iter()
        .map(|weight| {
            (
                weight.factor_definition_id,
                &weight.factor,
                weight.coverage_weight,
            )
        })
        .collect::<Vec<_>>();
    validate_canonical_weights(&indexed, "context coverage")?;
    for weight in weights {
        if !(Decimal::ZERO..=Decimal::ONE).contains(&weight.penalty_strength) {
            return Err(invalid_spec(format!(
                "context penalty `{}` must be in [0, 1], got {}",
                weight.factor, weight.penalty_strength
            )));
        }
    }
    Ok(())
}

fn validate_canonical_weights(
    weights: &[(FactorDefinitionId, &FactorName, Decimal)],
    head: &str,
) -> QuantResult<()> {
    let mut ids = HashSet::with_capacity(weights.len());
    let mut sum = Decimal::ZERO;
    for (index, (definition_id, name, weight)) in weights.iter().enumerate() {
        if index > 0 && weights[index - 1].1 >= *name {
            return Err(invalid_spec(format!(
                "{head} weights are not strictly ordered by factor name"
            )));
        }
        if !ids.insert(*definition_id) {
            return Err(invalid_spec(format!(
                "{head} weights duplicate revision {definition_id}"
            )));
        }
        if *weight < Decimal::ZERO {
            return Err(invalid_spec(format!(
                "{head} factor `{name}` has negative weight {weight}"
            )));
        }
        sum += *weight;
    }
    if (sum - Decimal::ONE).abs() > Decimal::new(1, RESEARCH_DECIMAL_SCALE) {
        return Err(invalid_spec(format!(
            "{head} weights must sum to one, got {sum}"
        )));
    }
    Ok(())
}

fn revision_by_id(
    plane: &FactorServingPlane,
    factor_definition_id: FactorDefinitionId,
) -> QuantResult<&FactorDefinitionRef> {
    plane
        .definitions()
        .iter()
        .find(|revision| revision.factor_definition_id() == factor_definition_id)
        .ok_or_else(|| {
            invalid_spec(format!(
                "head references revision {factor_definition_id} outside the serving plane"
            ))
        })
}

fn validate_deadband(deadband: Decimal) -> QuantResult<()> {
    if !(Decimal::ZERO..Decimal::ONE).contains(&deadband) {
        return Err(invalid_spec(format!(
            "alpha deadband must be in [0, 1), got {deadband}"
        )));
    }
    Ok(())
}

fn validate_unit(label: &str, value: Decimal) -> QuantResult<()> {
    if !(Decimal::ZERO..=Decimal::ONE).contains(&value) {
        return Err(invalid_input(format!(
            "{label} must be in [0, 1], got {value}"
        )));
    }
    Ok(())
}

fn safe_ratio(numerator: Decimal, denominator: Decimal) -> Decimal {
    if denominator.is_zero() {
        Decimal::ZERO
    } else {
        numerator / denominator
    }
}

fn rounded(value: Decimal) -> Decimal {
    value.round_dp(RESEARCH_DECIMAL_SCALE)
}

fn unit_rounded(value: Decimal) -> Decimal {
    rounded(value.clamp(Decimal::ZERO, Decimal::ONE))
}

fn invalid_spec(detail: String) -> QuantError {
    ResearchError::InvalidModelArtifact { detail }.into()
}

fn invalid_input(detail: String) -> QuantError {
    ResearchError::FactorComputation { detail }.into()
}

#[cfg(test)]
mod tests {
    use std::slice::from_ref;

    use quant_pivot_models::{
        enums::{
            factor::{FactorFamily, FactorIndeterminateReason, FactorNormalization},
            quant::{FactorDirection, OutcomeSide},
        },
        runtime_config::{DecimalValue, FactorHeadConfig},
        types::{
            ContentHash, MarketId, OutcomeTokenBinding, Probability, SchemaVersion, TokenId,
            factor::{
                FactorAlphaOrientation, FactorComputationContract, FactorContextEffect,
                FactorDefinitionDocument, FactorDefinitionRef, FactorExplanation,
                FactorOutputSemantics, FactorServingPlane,
            },
            stable_name::FactorName,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        AlphaFactorWeight, ContextFactorWeight, FactorHeadScore, FactorHeadSpec,
        score_factor_heads, sell_market_alpha,
    };
    use crate::factors::{FactorValue, NormalizedFactor};

    fn revision(name: &str, output: FactorOutputSemantics, required: bool) -> FactorDefinitionRef {
        FactorDefinitionRef::try_seal(
            FactorDefinitionDocument {
                name: FactorName::new(name),
                family: FactorFamily::Momentum,
                input_features: Vec::new(),
                output,
                normalization: FactorNormalization::MinMax,
                owner: "factor-head-test".to_owned(),
                required,
                computation: FactorComputationContract {
                    semantic_version: 1,
                    semantic_key: format!("quant-pivot/factor-head-test/{name}@1"),
                },
            },
            ContentHash::from_bytes([1; 32]),
            SchemaVersion::FIRST,
            SchemaVersion::FIRST,
        )
        .expect("factor revision")
    }

    fn alpha_revision(name: &str, required: bool) -> FactorDefinitionRef {
        revision(
            name,
            FactorOutputSemantics::OutcomeAlpha {
                orientation: FactorAlphaOrientation::FeatureToken,
            },
            required,
        )
    }

    fn canonical_revision(name: &str, required: bool) -> FactorDefinitionRef {
        revision(
            name,
            FactorOutputSemantics::OutcomeAlpha {
                orientation: FactorAlphaOrientation::CanonicalYes,
            },
            required,
        )
    }

    fn context_revision(name: &str) -> FactorDefinitionRef {
        revision(
            name,
            FactorOutputSemantics::Context {
                effect: FactorContextEffect::HigherIsSupportive,
            },
            false,
        )
    }

    fn diagnostic_revision(name: &str) -> FactorDefinitionRef {
        revision(name, FactorOutputSemantics::Diagnostic, false)
    }

    fn scored(revision: &FactorDefinitionRef, raw: Decimal, score: Decimal) -> FactorValue {
        scored_with_confidence(revision, raw, score, Decimal::ONE)
    }

    fn scored_with_confidence(
        revision: &FactorDefinitionRef,
        raw: Decimal,
        score: Decimal,
        confidence: Decimal,
    ) -> FactorValue {
        FactorValue {
            definition_id: revision.factor_definition_id(),
            name: revision.factor_name().clone(),
            family: revision.definition().family,
            raw_value: Some(raw),
            normalization: NormalizedFactor::cross_section(Probability::new(score)),
            direction: revision
                .definition()
                .contribution_direction(raw)
                .expect("factor direction"),
            confidence: Probability::new(confidence),
            explanation: FactorExplanation {
                headline: "test".to_owned(),
                drivers: Vec::new(),
            },
            input_feature_refs: Vec::new(),
        }
    }

    fn unavailable(revision: &FactorDefinitionRef, normalization: NormalizedFactor) -> FactorValue {
        FactorValue {
            definition_id: revision.factor_definition_id(),
            name: revision.factor_name().clone(),
            family: revision.definition().family,
            raw_value: None,
            normalization,
            direction: FactorDirection::Neutral,
            confidence: Probability::new(Decimal::ZERO),
            explanation: FactorExplanation {
                headline: "unavailable".to_owned(),
                drivers: Vec::new(),
            },
            input_feature_refs: Vec::new(),
        }
    }

    fn binding(side: OutcomeSide) -> OutcomeTokenBinding {
        let feature_token = match side {
            OutcomeSide::Yes => TokenId::from("yes"),
            OutcomeSide::No => TokenId::from("no"),
        };
        OutcomeTokenBinding::try_new(
            MarketId::from("market"),
            TokenId::from("yes"),
            TokenId::from("no"),
            feature_token,
            side,
        )
        .expect("outcome binding")
    }

    fn spec(alpha: &[FactorDefinitionRef], context: &[FactorDefinitionRef]) -> FactorHeadSpec {
        let alpha_weight = Decimal::ONE / Decimal::from(alpha.len() as u64);
        let context_weight = if context.is_empty() {
            Decimal::ZERO
        } else {
            Decimal::ONE / Decimal::from(context.len() as u64)
        };
        FactorHeadSpec {
            alpha_weights: alpha
                .iter()
                .map(|revision| AlphaFactorWeight {
                    factor_definition_id: revision.factor_definition_id(),
                    factor: revision.factor_name().clone(),
                    weight: alpha_weight,
                })
                .collect(),
            context_weights: context
                .iter()
                .map(|revision| ContextFactorWeight {
                    factor_definition_id: revision.factor_definition_id(),
                    factor: revision.factor_name().clone(),
                    coverage_weight: context_weight,
                    penalty_strength: dec!(0.5),
                })
                .collect(),
            alpha_deadband: dec!(0.05),
        }
    }

    #[test]
    fn config_expands_exact_plane() {
        let alpha_a = alpha_revision("alpha.a", true);
        let alpha_b = alpha_revision("alpha.b", true);
        let context = context_revision("context.quality");
        let diagnostic = diagnostic_revision("diagnostic.only");
        let plane = FactorServingPlane::try_seal(vec![
            diagnostic,
            context.clone(),
            alpha_b.clone(),
            alpha_a.clone(),
        ])
        .expect("factor plane");

        let head =
            FactorHeadSpec::from_config(&plane, &FactorHeadConfig::default()).expect("head seed");
        assert_eq!(head.alpha_weights.len(), 2);
        assert_eq!(&head.alpha_weights[0].factor, alpha_a.factor_name());
        assert_eq!(head.alpha_weights[0].weight, dec!(0.5));
        assert_eq!(&head.alpha_weights[1].factor, alpha_b.factor_name());
        assert_eq!(head.context_weights.len(), 1);
        assert_eq!(&head.context_weights[0].factor, context.factor_name());
        assert_eq!(head.context_weights[0].coverage_weight, Decimal::ONE);
        assert_eq!(head.context_weights[0].penalty_strength, dec!(0.5));
        head.validate(&plane).expect("expanded head");
    }

    #[test]
    fn config_binds_explicit_weights() {
        let alpha_a = alpha_revision("alpha.a", true);
        let alpha_b = alpha_revision("alpha.b", true);
        let context = context_revision("context.quality");
        let plane =
            FactorServingPlane::try_seal(vec![context, alpha_b, alpha_a]).expect("factor plane");
        let mut config = FactorHeadConfig::default();
        config
            .alpha_seed_weights
            .insert("alpha.a".to_owned(), DecimalValue::new(dec!(0.75)));
        config
            .alpha_seed_weights
            .insert("alpha.b".to_owned(), DecimalValue::new(dec!(0.25)));
        config.context_coverage_weights.insert(
            "context.quality".to_owned(),
            DecimalValue::new(Decimal::ONE),
        );
        config
            .context_penalty_strengths
            .insert("context.quality".to_owned(), DecimalValue::new(dec!(0.8)));

        let head = FactorHeadSpec::from_config(&plane, &config).expect("explicit head");
        assert_eq!(head.alpha_weights[0].weight, dec!(0.75));
        assert_eq!(head.alpha_weights[1].weight, dec!(0.25));
        assert_eq!(head.context_weights[0].penalty_strength, dec!(0.8));
    }

    #[test]
    fn config_rejects_semantic_mismatch() {
        let alpha = alpha_revision("alpha.signal", true);
        let context = context_revision("context.quality");
        let plane =
            FactorServingPlane::try_seal(vec![context, alpha]).expect("factor serving plane");
        let mut missing = FactorHeadConfig::default();
        missing.alpha_seed_weights.insert(
            "context.quality".to_owned(),
            DecimalValue::new(Decimal::ONE),
        );
        assert!(FactorHeadSpec::from_config(&plane, &missing).is_err());

        let mut diagnostic = FactorHeadConfig::default();
        diagnostic.alpha_seed_weights.insert(
            "diagnostic.only".to_owned(),
            DecimalValue::new(Decimal::ONE),
        );
        assert!(FactorHeadSpec::from_config(&plane, &diagnostic).is_err());
    }

    #[test]
    fn optional_context_missing_neutral() {
        let alpha = alpha_revision("alpha.signal", true);
        let context = context_revision("context.quality");
        let plane =
            FactorServingPlane::try_seal(vec![context.clone(), alpha.clone()]).expect("plane");
        let score = score_factor_heads(
            &[
                scored(&alpha, dec!(1), dec!(0.8)),
                unavailable(&context, NormalizedFactor::MissingInput),
            ],
            &plane,
            &spec(&[alpha], &[context]),
            &binding(OutcomeSide::Yes),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("head score");

        assert_eq!(score.context_multiplier, Decimal::ONE);
        assert_eq!(score.context_quality, Decimal::ZERO);
        assert_eq!(score.reliability, Decimal::ZERO);
        assert_eq!(score.outcome_side, Some(OutcomeSide::Yes));
    }

    #[test]
    fn required_gap_rejects() {
        let alpha = alpha_revision("alpha.required", true);
        let plane = FactorServingPlane::try_seal(vec![alpha.clone()]).expect("plane");

        for normalization in [
            NormalizedFactor::MissingInput,
            NormalizedFactor::Indeterminate {
                reason: FactorIndeterminateReason::ZeroVariance,
            },
        ] {
            assert!(
                score_factor_heads(
                    &[unavailable(&alpha, normalization)],
                    &plane,
                    &spec(from_ref(&alpha), &[]),
                    &binding(OutcomeSide::Yes),
                    Decimal::ONE,
                    Decimal::ONE,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn context_na_excluded() {
        let alpha = alpha_revision("alpha.signal", true);
        let context = context_revision("context.optional");
        let plane =
            FactorServingPlane::try_seal(vec![context.clone(), alpha.clone()]).expect("plane");
        let score = score_factor_heads(
            &[
                scored(&alpha, dec!(1), dec!(0.7)),
                unavailable(&context, NormalizedFactor::NotApplicable),
            ],
            &plane,
            &spec(&[alpha], &[context]),
            &binding(OutcomeSide::Yes),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("head score");

        assert_eq!(score.context_multiplier, Decimal::ONE);
        assert_eq!(score.context_quality, Decimal::ONE);
    }

    #[test]
    fn context_preserves_side() {
        let alpha = alpha_revision("alpha.signal", true);
        let context = context_revision("context.quality");
        let plane =
            FactorServingPlane::try_seal(vec![context.clone(), alpha.clone()]).expect("plane");
        let head_spec = spec(from_ref(&alpha), from_ref(&context));
        let supportive = score_factor_heads(
            &[
                scored(&alpha, dec!(1), dec!(0.8)),
                scored(&context, dec!(1), Decimal::ONE),
            ],
            &plane,
            &head_spec,
            &binding(OutcomeSide::Yes),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("supportive context");
        let adverse = score_factor_heads(
            &[
                scored(&alpha, dec!(1), dec!(0.8)),
                scored(&context, dec!(1), Decimal::ZERO),
            ],
            &plane,
            &head_spec,
            &binding(OutcomeSide::Yes),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("adverse context");

        assert_eq!(supportive.yes_alpha, adverse.yes_alpha);
        assert_eq!(supportive.outcome_side, adverse.outcome_side);
        assert!(supportive.composite_score > adverse.composite_score);
    }

    #[test]
    fn token_projection_mirrors() {
        let alpha = alpha_revision("alpha.signal", true);
        let plane = FactorServingPlane::try_seal(vec![alpha.clone()]).expect("plane");
        let factor = scored(&alpha, dec!(1), dec!(0.8));
        let head_spec = spec(&[alpha], &[]);
        let yes = score_factor_heads(
            from_ref(&factor),
            &plane,
            &head_spec,
            &binding(OutcomeSide::Yes),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("YES score");
        let no = score_factor_heads(
            &[factor],
            &plane,
            &head_spec,
            &binding(OutcomeSide::No),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("NO score");

        assert_eq!(yes.yes_alpha, -no.yes_alpha);
        assert_eq!(
            sell_market_alpha(&yes, &binding(OutcomeSide::Yes)).expect("YES exit alpha"),
            sell_market_alpha(&no, &binding(OutcomeSide::No)).expect("NO exit alpha")
        );
    }

    #[test]
    fn held_support_orients_exit() {
        let score = FactorHeadScore {
            yes_alpha: dec!(0.8),
            alpha_quality: Decimal::ONE,
            context_multiplier: dec!(0.1),
            context_quality: Decimal::ONE,
            directional_coherence: Decimal::ONE,
            reliability: Decimal::ONE,
            composite_score: dec!(0.08),
            outcome_side: Some(OutcomeSide::Yes),
        };

        assert_eq!(
            sell_market_alpha(&score, &binding(OutcomeSide::Yes)).expect("YES exit alpha"),
            dec!(-0.8)
        );
        assert_eq!(
            sell_market_alpha(&score, &binding(OutcomeSide::No)).expect("NO exit alpha"),
            dec!(0.8)
        );
    }

    #[test]
    fn alpha_cancellation_deadband() {
        let left = alpha_revision("alpha.left", true);
        let right = alpha_revision("alpha.right", true);
        let plane = FactorServingPlane::try_seal(vec![right.clone(), left.clone()]).expect("plane");
        let score = score_factor_heads(
            &[
                scored(&left, dec!(1), Decimal::ONE),
                scored(&right, dec!(-1), Decimal::ONE),
            ],
            &plane,
            &spec(&[left, right], &[]),
            &binding(OutcomeSide::Yes),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("head score");

        assert_eq!(score.yes_alpha, Decimal::ZERO);
        assert_eq!(score.directional_coherence, Decimal::ZERO);
        assert_eq!(score.outcome_side, None);
    }

    #[test]
    fn diagnostic_weight_rejected() {
        let alpha = alpha_revision("alpha.signal", true);
        let diagnostic = diagnostic_revision("diagnostic.raw");
        let plane =
            FactorServingPlane::try_seal(vec![diagnostic.clone(), alpha.clone()]).expect("plane");
        let mut head_spec = spec(&[alpha], &[]);
        head_spec.alpha_weights.push(AlphaFactorWeight {
            factor_definition_id: diagnostic.factor_definition_id(),
            factor: diagnostic.factor_name().clone(),
            weight: Decimal::ZERO,
        });

        assert!(head_spec.validate(&plane).is_err());
    }

    #[test]
    fn weighted_confidence_is_exact() {
        let first = alpha_revision("alpha.first", true);
        let second = alpha_revision("alpha.second", true);
        let plane =
            FactorServingPlane::try_seal(vec![second.clone(), first.clone()]).expect("plane");
        let head_spec = FactorHeadSpec {
            alpha_weights: vec![
                AlphaFactorWeight {
                    factor_definition_id: first.factor_definition_id(),
                    factor: first.factor_name().clone(),
                    weight: dec!(0.25),
                },
                AlphaFactorWeight {
                    factor_definition_id: second.factor_definition_id(),
                    factor: second.factor_name().clone(),
                    weight: dec!(0.75),
                },
            ],
            context_weights: Vec::new(),
            alpha_deadband: Decimal::ZERO,
        };
        let score = score_factor_heads(
            &[
                scored_with_confidence(&first, dec!(1), dec!(0.8), dec!(0.2)),
                scored_with_confidence(&second, dec!(1), dec!(0.4), dec!(0.6)),
            ],
            &plane,
            &head_spec,
            &binding(OutcomeSide::Yes),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("head score");

        assert_eq!(score.yes_alpha, dec!(0.22));
        assert_eq!(score.alpha_quality, dec!(0.5));
        assert_eq!(score.directional_coherence, Decimal::ONE);
        assert_eq!(score.reliability, dec!(0.5));
    }

    #[test]
    fn optional_alpha_keeps_denominator() {
        let observed = alpha_revision("alpha.observed", true);
        let missing = alpha_revision("alpha.optional", false);
        let plane =
            FactorServingPlane::try_seal(vec![missing.clone(), observed.clone()]).expect("plane");
        let score = score_factor_heads(
            &[
                scored(&observed, dec!(1), dec!(0.8)),
                unavailable(&missing, NormalizedFactor::MissingInput),
            ],
            &plane,
            &spec(&[observed, missing], &[]),
            &binding(OutcomeSide::Yes),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("head score");

        assert_eq!(score.yes_alpha, dec!(0.4));
        assert_eq!(score.alpha_quality, dec!(0.5));
        assert_eq!(score.directional_coherence, Decimal::ONE);
    }

    #[test]
    fn alpha_na_excluded() {
        let observed = alpha_revision("alpha.observed", true);
        let absent = alpha_revision("alpha.optional", false);
        let plane =
            FactorServingPlane::try_seal(vec![absent.clone(), observed.clone()]).expect("plane");
        let score = score_factor_heads(
            &[
                scored(&observed, dec!(1), dec!(0.8)),
                unavailable(&absent, NormalizedFactor::NotApplicable),
            ],
            &plane,
            &spec(&[observed, absent], &[]),
            &binding(OutcomeSide::Yes),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("head score");

        assert_eq!(score.yes_alpha, dec!(0.8));
        assert_eq!(score.alpha_quality, Decimal::ONE);
    }

    #[test]
    fn context_penalties_multiply() {
        let alpha = alpha_revision("alpha.signal", true);
        let first = context_revision("context.first");
        let second = context_revision("context.second");
        let plane =
            FactorServingPlane::try_seal(vec![second.clone(), alpha.clone(), first.clone()])
                .expect("plane");
        let head_spec = FactorHeadSpec {
            alpha_weights: vec![AlphaFactorWeight {
                factor_definition_id: alpha.factor_definition_id(),
                factor: alpha.factor_name().clone(),
                weight: Decimal::ONE,
            }],
            context_weights: vec![
                ContextFactorWeight {
                    factor_definition_id: first.factor_definition_id(),
                    factor: first.factor_name().clone(),
                    coverage_weight: dec!(0.5),
                    penalty_strength: dec!(0.25),
                },
                ContextFactorWeight {
                    factor_definition_id: second.factor_definition_id(),
                    factor: second.factor_name().clone(),
                    coverage_weight: dec!(0.5),
                    penalty_strength: dec!(0.25),
                },
            ],
            alpha_deadband: Decimal::ZERO,
        };
        let score = score_factor_heads(
            &[
                scored(&alpha, dec!(1), Decimal::ONE),
                scored_with_confidence(&first, dec!(1), Decimal::ZERO, dec!(0.5)),
                scored(&second, dec!(1), Decimal::ZERO),
            ],
            &plane,
            &head_spec,
            &binding(OutcomeSide::Yes),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("head score");

        assert_eq!(score.context_multiplier, dec!(0.65625));
        assert_eq!(score.context_quality, dec!(0.75));
    }

    #[test]
    fn canonical_yes_stays_fixed() {
        let alpha = canonical_revision("alpha.canonical", true);
        let plane = FactorServingPlane::try_seal(vec![alpha.clone()]).expect("plane");
        let factor = scored(&alpha, dec!(1), dec!(0.8));
        let head_spec = spec(&[alpha], &[]);
        let yes = score_factor_heads(
            from_ref(&factor),
            &plane,
            &head_spec,
            &binding(OutcomeSide::Yes),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("YES score");
        let no = score_factor_heads(
            &[factor],
            &plane,
            &head_spec,
            &binding(OutcomeSide::No),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("NO score");

        assert_eq!(yes.yes_alpha, dec!(0.8));
        assert_eq!(no.yes_alpha, dec!(0.8));
    }

    #[test]
    fn malformed_na_rejected() {
        let alpha = alpha_revision("alpha.signal", true);
        let optional = context_revision("context.optional");
        let plane =
            FactorServingPlane::try_seal(vec![optional.clone(), alpha.clone()]).expect("plane");
        let mut malformed = unavailable(&optional, NormalizedFactor::NotApplicable);
        malformed.confidence = Probability::new(dec!(0.5));

        assert!(
            score_factor_heads(
                &[scored(&alpha, dec!(1), dec!(0.8)), malformed],
                &plane,
                &spec(&[alpha], &[optional]),
                &binding(OutcomeSide::Yes),
                Decimal::ONE,
                Decimal::ONE,
            )
            .is_err()
        );
    }

    #[test]
    fn diagnostic_is_lineage_only() {
        let alpha = alpha_revision("alpha.signal", true);
        let diagnostic = diagnostic_revision("diagnostic.raw");
        let plane =
            FactorServingPlane::try_seal(vec![diagnostic.clone(), alpha.clone()]).expect("plane");
        let head_spec = spec(from_ref(&alpha), &[]);
        let low = score_factor_heads(
            &[
                scored(&alpha, dec!(1), dec!(0.8)),
                scored(&diagnostic, Decimal::ZERO, Decimal::ZERO),
            ],
            &plane,
            &head_spec,
            &binding(OutcomeSide::Yes),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("low diagnostic score");
        let high = score_factor_heads(
            &[
                scored(&alpha, dec!(1), dec!(0.8)),
                scored(&diagnostic, Decimal::ONE, Decimal::ONE),
            ],
            &plane,
            &head_spec,
            &binding(OutcomeSide::Yes),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("high diagnostic score");

        assert_eq!(low, high);
    }

    #[test]
    fn precision_floor_no_side() {
        let alpha = alpha_revision("alpha.signal", true);
        let plane = FactorServingPlane::try_seal(vec![alpha.clone()]).expect("plane");
        let mut head_spec = spec(from_ref(&alpha), &[]);
        head_spec.alpha_deadband = Decimal::ZERO;
        let score = score_factor_heads(
            &[scored(&alpha, Decimal::ONE, dec!(0.0000000000004))],
            &plane,
            &head_spec,
            &binding(OutcomeSide::Yes),
            Decimal::ONE,
            Decimal::ONE,
        )
        .expect("head score");

        assert_eq!(score.yes_alpha, Decimal::ZERO);
        assert_eq!(score.composite_score, Decimal::ZERO);
        assert_eq!(score.outcome_side, None);
    }
}
