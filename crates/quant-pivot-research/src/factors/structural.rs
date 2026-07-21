//! Structural (prediction-market-aware) factor computers.
//!
//! Each is a pure, per-market function over a [`FeatureVector`]; distributional
//! normalization is the [`FactorEngine`](crate::factors::FactorEngine)'s job.
//! Neg-risk factors are `NotApplicable` on a binary market (structurally absent,
//! not a data gap) and `Indeterminate { LegBookMissing }` when a neg-risk market
//! is missing a leg book — never a silent zero. The favorite-longshot factor is
//! inert (no raw value, zero confidence) unless a fitted bias table is bound and
//! its category clears the IC gate.

use std::{cmp::Ordering, sync::Arc};

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::{
        common::MarketCategory,
        factor::{FactorFamily, FactorIndeterminateReason, FactorNormalization},
        quant::FactorDirection,
    },
    runtime_config::{DecimalValue, FactorsConfig, FeaturesConfig, StructuralFactorsConfig},
    types::{FactorDefinitionId, Price, Probability},
};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use crate::{
    factors::{
        computer::FactorComputer,
        generic::{data_quality_confidence, extract_decimal},
        identity::provisional_factor_definition_id,
        names::{
            STRUCT_BOOK_CHURN_INTENSITY, STRUCT_FAVORITE_LONGSHOT, STRUCT_NEGRISK_CONVERT_EDGE,
            STRUCT_NEGRISK_LEG_SUM_DRIFT, STRUCT_PARTICIPANT_CONCENTRATION,
            STRUCT_RESOLUTION_PROXIMITY_REGIME, STRUCT_REVERSAL_AFTER_SHOCK,
        },
        value::{
            FactorDefinitionDocument, FactorDriver, FactorName, FactorOutputKind, RawFactor,
            RawFactorEligibility,
        },
    },
    features::{
        self, FeatureCellState, FeatureName, FeatureValue, FeatureVector, NullReason,
        names::{
            book as book_names,
            book::MID,
            market as market_names,
            market::{CATEGORY, TIME_TO_RESOLUTION_SECS},
            structural as feat,
            structural::{
                NEGRISK_CONVERT_EDGE, NEGRISK_LEG_ASK_SUM, NEGRISK_LEG_BID_SUM, NEGRISK_LEG_COUNT,
                PARTICIPANT_CR1_SHARE, PARTICIPANT_GINI, PARTICIPANT_HHI, PRICE_EXTREMITY,
                SHOCK_RATIO, SHORT_RETURN,
            },
        },
    },
    model::FavoriteLongshotBiasTable,
    trade_tape::{ConcentrationCompositeWeights, composite_concentration},
};

/// Every structural `(spec, computer)` pair, resolved against the factor +
/// feature config. `bias_table` binds the favorite-longshot factor's fitted
/// artifact (or `None` — the factor is then inert).
#[must_use]
pub fn structural_factors(
    factors: &FactorsConfig,
    _features: &FeaturesConfig,
    bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
) -> Vec<(FactorDefinitionDocument, Arc<dyn FactorComputer>)> {
    let structural = &factors.structural;
    let shock_k = parse_decimal(
        &structural.reversal_after_shock.shock_k,
        "factors.structural.reversal_after_shock.shock_k",
    );
    let shock_cap = parse_decimal(
        &structural.reversal_after_shock.shock_cap,
        "factors.structural.reversal_after_shock.shock_cap",
    );
    let min_legs = structural.negrisk.min_legs;

    vec![
        computer(ReversalAfterShockFactor {
            definition_id: provisional_factor_definition_id(STRUCT_REVERSAL_AFTER_SHOCK.as_str()),
            spec: structural_spec(
                STRUCT_REVERSAL_AFTER_SHOCK,
                vec![feat::SHOCK_RATIO, feat::SHORT_RETURN],
                FactorDirection::Positive,
                FactorNormalization::WinsorizedZScore,
                FactorOutputKind::Directional,
            ),
            shock_k,
            shock_cap,
        }),
        computer(ResolutionProximityRegimeFactor {
            definition_id: provisional_factor_definition_id(
                STRUCT_RESOLUTION_PROXIMITY_REGIME.as_str(),
            ),
            spec: structural_spec(
                STRUCT_RESOLUTION_PROXIMITY_REGIME,
                vec![feat::PRICE_EXTREMITY, market_names::TIME_TO_RESOLUTION_SECS],
                FactorDirection::Positive,
                FactorNormalization::WinsorizedZScore,
                FactorOutputKind::Directional,
            ),
        }),
        computer(SingleFeatureStructuralFactor {
            definition_id: provisional_factor_definition_id(STRUCT_BOOK_CHURN_INTENSITY.as_str()),
            spec: structural_spec(
                STRUCT_BOOK_CHURN_INTENSITY,
                vec![feat::BOOK_CHURN_INTENSITY],
                FactorDirection::Negative,
                FactorNormalization::WinsorizedZScore,
                FactorOutputKind::Directional,
            ),
            input: feat::BOOK_CHURN_INTENSITY,
            headline_label: "book-churn intensity",
        }),
        participant_concentration_factor(structural),
        computer(NegRiskLegSumDriftFactor {
            definition_id: provisional_factor_definition_id(STRUCT_NEGRISK_LEG_SUM_DRIFT.as_str()),
            spec: structural_spec(
                STRUCT_NEGRISK_LEG_SUM_DRIFT,
                vec![
                    feat::NEGRISK_LEG_ASK_SUM,
                    feat::NEGRISK_LEG_BID_SUM,
                    feat::NEGRISK_LEG_COUNT,
                ],
                // Σ best-ask > 1 means the YES-leg basket is collectively
                // over-priced → fade it (NO side); Σ < 1 means the basket is a
                // capital-locked buy → YES side. The signal's sign is therefore
                // Negative in the leg-sum drift (`Σ − 1`).
                FactorDirection::Negative,
                FactorNormalization::WinsorizedZScore,
                FactorOutputKind::Directional,
            ),
            min_legs,
        }),
        computer(NegRiskConvertEdgeFactor {
            definition_id: provisional_factor_definition_id(STRUCT_NEGRISK_CONVERT_EDGE.as_str()),
            spec: structural_spec(
                STRUCT_NEGRISK_CONVERT_EDGE,
                vec![feat::NEGRISK_CONVERT_EDGE, feat::NEGRISK_LEG_COUNT],
                // Convert-edge is an event-level capital-efficiency *regime*
                // signal: it measures whether the basket-vs-adapter route is
                // mispriced, but does NOT indicate which leg to buy. It is a
                // magnitude/regime feature (Neutral direction, NormalizedScore)
                // that the learned model may weight — it never picks a
                // side in the heuristic net.
                FactorDirection::Neutral,
                FactorNormalization::WinsorizedZScore,
                FactorOutputKind::NormalizedScore,
            ),
            min_legs,
        }),
        computer(FavoriteLongshotFactor {
            definition_id: provisional_factor_definition_id(STRUCT_FAVORITE_LONGSHOT.as_str()),
            spec: structural_spec(
                STRUCT_FAVORITE_LONGSHOT,
                vec![
                    market_names::CATEGORY,
                    book_names::MID,
                    market_names::TIME_TO_RESOLUTION_SECS,
                ],
                FactorDirection::Positive,
                FactorNormalization::WinsorizedZScore,
                FactorOutputKind::Directional,
            ),
            bias_table,
            ic_gate: structural.per_category_ic_gate,
        }),
    ]
}

/// Box a computer into the `(spec, Arc<dyn FactorComputer>)` registry entry.
fn computer<C: FactorComputer + 'static>(
    factor: C,
) -> (FactorDefinitionDocument, Arc<dyn FactorComputer>) {
    let spec = factor.spec().clone();
    (spec, Arc::new(factor) as Arc<dyn FactorComputer>)
}

fn participant_concentration_factor(
    structural: &StructuralFactorsConfig,
) -> (FactorDefinitionDocument, Arc<dyn FactorComputer>) {
    let participant = &structural.participant_concentration;
    computer(ParticipantConcentrationFactor {
        definition_id: provisional_factor_definition_id(STRUCT_PARTICIPANT_CONCENTRATION.as_str()),
        spec: structural_spec(
            STRUCT_PARTICIPANT_CONCENTRATION,
            vec![
                feat::PARTICIPANT_GINI,
                feat::PARTICIPANT_CR1_SHARE,
                feat::PARTICIPANT_HHI,
            ],
            FactorDirection::Neutral,
            FactorNormalization::WinsorizedZScore,
            FactorOutputKind::NormalizedScore,
        ),
        gini_weight: parse_decimal(
            &participant.gini_weight,
            "factors.structural.participant_concentration.gini_weight",
        ),
        cr1_weight: parse_decimal(
            &participant.cr1_share_weight,
            "factors.structural.participant_concentration.cr1_share_weight",
        ),
        hhi_weight: parse_decimal(
            &participant.hhi_weight,
            "factors.structural.participant_concentration.hhi_weight",
        ),
    })
}

/// A structural factor spec. Structural signals are never required: missing
/// inputs degrade gracefully, while weighting is learned by the model.
fn structural_spec(
    name: FactorName,
    input_features: Vec<FeatureName>,
    direction: FactorDirection,
    normalization: FactorNormalization,
    output_kind: FactorOutputKind,
) -> FactorDefinitionDocument {
    FactorDefinitionDocument {
        name,
        family: FactorFamily::Structural,
        input_features,
        output_kind,
        default_direction: direction,
        normalization,
        owner: "quant-research".to_owned(),
        quality_gates: Vec::new(),
    }
}

/// Parse a config decimal string (must have passed runtime-config validation).
const fn parse_decimal(raw: &DecimalValue, _field: &'static str) -> Decimal {
    raw.value
}

/// A raw factor with no usable value and zero confidence, carrying an explicit
/// engine eligibility (structurally absent or indeterminate).
fn inert(
    definition_id: FactorDefinitionId,
    spec: &FactorDefinitionDocument,
    eligibility: RawFactorEligibility,
    headline: String,
) -> RawFactor {
    RawFactor {
        definition_id,
        name: spec.name.clone(),
        family: spec.family,
        raw_value: None,
        eligibility,
        direction: spec.default_direction,
        confidence: Probability::ZERO,
        headline,
        drivers: Vec::new(),
        input_feature_refs: spec.input_features.clone(),
    }
}

/// A scored raw factor from a computed raw value.
fn scored(
    definition_id: FactorDefinitionId,
    spec: &FactorDefinitionDocument,
    raw_value: Decimal,
    features: &FeatureVector,
    headline: String,
    drivers: Vec<FactorDriver>,
) -> RawFactor {
    RawFactor {
        definition_id,
        name: spec.name.clone(),
        family: spec.family,
        raw_value: Some(raw_value),
        eligibility: RawFactorEligibility::Normalizable,
        direction: spec.default_direction,
        confidence: Probability::new(data_quality_confidence(features.data_quality)),
        headline,
        drivers,
        input_feature_refs: spec.input_features.clone(),
    }
}

/// Read a numeric feature (missing ⇒ `None`).
fn read(features: &FeatureVector, name: &FeatureName) -> Option<Decimal> {
    features.value(name).and_then(extract_decimal)
}

// ── reversal_after_shock ────────────────────────────────────────────────────

struct ReversalAfterShockFactor {
    definition_id: FactorDefinitionId,
    spec: FactorDefinitionDocument,
    shock_k: Decimal,
    shock_cap: Decimal,
}

impl FactorComputer for ReversalAfterShockFactor {
    fn definition_id(&self) -> FactorDefinitionId {
        self.definition_id.clone()
    }
    fn spec(&self) -> &FactorDefinitionDocument {
        &self.spec
    }
    fn compute_raw(&self, features: &FeatureVector) -> QuantResult<RawFactor> {
        let (Some(shock), Some(short_return)) =
            (read(features, &SHOCK_RATIO), read(features, &SHORT_RETURN))
        else {
            return Ok(inert(
                self.definition_id.clone(),
                &self.spec,
                RawFactorEligibility::Normalizable,
                "no shock-window history".to_owned(),
            ));
        };
        // Below the shock threshold there is no reversal signal — the factor does
        // not contribute (raw value absent), it never fabricates a neutral.
        if shock <= self.shock_k {
            return Ok(inert(
                self.definition_id.clone(),
                &self.spec,
                RawFactorEligibility::Normalizable,
                format!("shock {shock} below threshold {}", self.shock_k),
            ));
        }
        let magnitude = shock.min(self.shock_cap);
        let raw = -sign(short_return) * magnitude;
        Ok(scored(
            self.definition_id.clone(),
            &self.spec,
            raw,
            features,
            format!("shock {shock} reversal (return {short_return})"),
            vec![
                FactorDriver {
                    feature_name: feat::SHOCK_RATIO,
                    contribution: shock,
                },
                FactorDriver {
                    feature_name: feat::SHORT_RETURN,
                    contribution: short_return,
                },
            ],
        ))
    }
}

/// The sign of a decimal as `-1 / 0 / 1`.
fn sign(value: Decimal) -> Decimal {
    match value.cmp(&Decimal::ZERO) {
        Ordering::Greater => Decimal::ONE,
        Ordering::Less => Decimal::NEGATIVE_ONE,
        Ordering::Equal => Decimal::ZERO,
    }
}

// ── resolution_proximity_regime ─────────────────────────────────────────────

struct ResolutionProximityRegimeFactor {
    definition_id: FactorDefinitionId,
    spec: FactorDefinitionDocument,
}

impl FactorComputer for ResolutionProximityRegimeFactor {
    fn definition_id(&self) -> FactorDefinitionId {
        self.definition_id.clone()
    }
    fn spec(&self) -> &FactorDefinitionDocument {
        &self.spec
    }
    fn compute_raw(&self, features: &FeatureVector) -> QuantResult<RawFactor> {
        let (Some(extremity), Some(ttr_secs)) = (
            read(features, &PRICE_EXTREMITY),
            read(features, &TIME_TO_RESOLUTION_SECS),
        ) else {
            return Ok(inert(
                self.definition_id.clone(),
                &self.spec,
                RawFactorEligibility::Normalizable,
                "missing extremity or time-to-resolution".to_owned(),
            ));
        };
        // Signed price extremity per remaining day: the interaction of pricing
        // maturity (signed `mid − 0.5`) with resolution proximity, orthogonal to
        // the linear ttr. Sign preserved so a maturing favorite reads YES and a
        // maturing longshot reads NO (favorite-longshot consistent).
        let ttr_days = (ttr_secs / Decimal::from(86_400u64)).max(Decimal::ONE);
        let raw = (extremity / ttr_days).round_dp(12);
        Ok(scored(
            self.definition_id.clone(),
            &self.spec,
            raw,
            features,
            format!("signed extremity {extremity} over {ttr_days} day(s)"),
            vec![
                FactorDriver {
                    feature_name: feat::PRICE_EXTREMITY,
                    contribution: extremity,
                },
                FactorDriver {
                    feature_name: market_names::TIME_TO_RESOLUTION_SECS,
                    contribution: ttr_secs,
                },
            ],
        ))
    }
}

// ── book_churn_intensity (single feature) ───────────────────────────────────

struct SingleFeatureStructuralFactor {
    definition_id: FactorDefinitionId,
    spec: FactorDefinitionDocument,
    input: FeatureName,
    headline_label: &'static str,
}

impl FactorComputer for SingleFeatureStructuralFactor {
    fn definition_id(&self) -> FactorDefinitionId {
        self.definition_id.clone()
    }
    fn spec(&self) -> &FactorDefinitionDocument {
        &self.spec
    }
    fn compute_raw(&self, features: &FeatureVector) -> QuantResult<RawFactor> {
        Ok(read(features, &self.input).map_or_else(
            || {
                inert(
                    self.definition_id.clone(),
                    &self.spec,
                    RawFactorEligibility::Normalizable,
                    format!("{} unavailable", self.headline_label),
                )
            },
            |value| {
                scored(
                    self.definition_id.clone(),
                    &self.spec,
                    value,
                    features,
                    format!("{} = {value}", self.headline_label),
                    vec![FactorDriver {
                        feature_name: self.input.clone(),
                        contribution: value,
                    }],
                )
            },
        ))
    }
}

// ── participant_concentration ───────────────────────────────────────────────

struct ParticipantConcentrationFactor {
    definition_id: FactorDefinitionId,
    spec: FactorDefinitionDocument,
    gini_weight: Decimal,
    cr1_weight: Decimal,
    hhi_weight: Decimal,
}

impl FactorComputer for ParticipantConcentrationFactor {
    fn definition_id(&self) -> FactorDefinitionId {
        self.definition_id.clone()
    }

    fn spec(&self) -> &FactorDefinitionDocument {
        &self.spec
    }

    fn compute_raw(&self, features: &FeatureVector) -> QuantResult<RawFactor> {
        let (Some(gini), Some(cr1), Some(hhi)) = (
            read(features, &PARTICIPANT_GINI),
            read(features, &PARTICIPANT_CR1_SHARE),
            read(features, &PARTICIPANT_HHI),
        ) else {
            return Ok(inert(
                self.definition_id.clone(),
                &self.spec,
                RawFactorEligibility::Normalizable,
                "trade-tape participant concentration unavailable".to_owned(),
            ));
        };
        let weights = ConcentrationCompositeWeights {
            gini: self.gini_weight,
            cr1_share: self.cr1_weight,
            hhi: self.hhi_weight,
        };
        let Some(raw) = composite_concentration(gini, cr1, hhi, &weights) else {
            return Ok(inert(
                self.definition_id.clone(),
                &self.spec,
                RawFactorEligibility::Normalizable,
                "participant concentration weights disabled".to_owned(),
            ));
        };
        Ok(scored(
            self.definition_id.clone(),
            &self.spec,
            raw.round_dp(12),
            features,
            format!("participant concentration gini={gini}, cr1={cr1}, hhi={hhi}"),
            vec![
                FactorDriver {
                    feature_name: feat::PARTICIPANT_GINI,
                    contribution: gini,
                },
                FactorDriver {
                    feature_name: feat::PARTICIPANT_CR1_SHARE,
                    contribution: cr1,
                },
                FactorDriver {
                    feature_name: feat::PARTICIPANT_HHI,
                    contribution: hhi,
                },
            ],
        ))
    }
}

// ── neg-risk factors ────────────────────────────────────────────────────────

/// The applicability of a neg-risk full-leg feature for one market.
enum NegRiskOutcome {
    /// The aggregate is present and the market has enough legs.
    Applicable(Decimal),
    /// The market is binary / has too few legs (structurally absent).
    NotApplicable,
    /// A neg-risk market's leg book was missing at `as_of`.
    LegMissing,
}

/// Resolve a neg-risk full-leg feature's applicability, honoring `min_legs`.
fn negrisk_outcome(
    features: &FeatureVector,
    value_name: &FeatureName,
    min_legs: u32,
) -> NegRiskOutcome {
    match features.cell(value_name) {
        Some(cell) if cell.state == FeatureCellState::NotApplicable => {
            NegRiskOutcome::NotApplicable
        }
        Some(cell) if cell.reason == Some(NullReason::LegBookMissing) => NegRiskOutcome::LegMissing,
        Some(cell) if cell.value().is_some() => {
            let Some(decimal) = cell.value().and_then(features::feature_scalar) else {
                return NegRiskOutcome::LegMissing;
            };
            let Some(legs) = read(features, &NEGRISK_LEG_COUNT) else {
                return NegRiskOutcome::LegMissing;
            };
            if legs < Decimal::from(min_legs) {
                NegRiskOutcome::NotApplicable
            } else {
                NegRiskOutcome::Applicable(decimal)
            }
        }
        _ => NegRiskOutcome::LegMissing,
    }
}

/// Build the raw factor for a neg-risk value transformed by `to_raw`.
fn negrisk_raw(
    definition_id: &FactorDefinitionId,
    spec: &FactorDefinitionDocument,
    features: &FeatureVector,
    value_name: FeatureName,
    min_legs: u32,
    to_raw: impl Fn(Decimal) -> Decimal,
    label: &str,
) -> RawFactor {
    match negrisk_outcome(features, &value_name, min_legs) {
        NegRiskOutcome::Applicable(value) => {
            let raw = to_raw(value).round_dp(12);
            scored(
                definition_id.clone(),
                spec,
                raw,
                features,
                format!("{label} = {raw}"),
                vec![FactorDriver {
                    feature_name: value_name,
                    contribution: value,
                }],
            )
        }
        NegRiskOutcome::NotApplicable => inert(
            definition_id.clone(),
            spec,
            RawFactorEligibility::NotApplicable,
            format!("{label} not applicable (binary / too few legs)"),
        ),
        NegRiskOutcome::LegMissing => inert(
            definition_id.clone(),
            spec,
            RawFactorEligibility::Indeterminate(FactorIndeterminateReason::LegBookMissing),
            format!("{label} indeterminate (leg book missing)"),
        ),
    }
}

struct NegRiskLegSumDriftFactor {
    definition_id: FactorDefinitionId,
    spec: FactorDefinitionDocument,
    min_legs: u32,
}

impl FactorComputer for NegRiskLegSumDriftFactor {
    fn definition_id(&self) -> FactorDefinitionId {
        self.definition_id.clone()
    }
    fn spec(&self) -> &FactorDefinitionDocument {
        &self.spec
    }
    fn compute_raw(&self, features: &FeatureVector) -> QuantResult<RawFactor> {
        let mut raw = negrisk_raw(
            &self.definition_id,
            &self.spec,
            features,
            NEGRISK_LEG_ASK_SUM,
            self.min_legs,
            |ask_sum| ask_sum - Decimal::ONE,
            "neg-risk leg-sum drift",
        );
        // Corroborate drift confidence with leg tightness: the wider the summed
        // ask/bid gap per leg, the less reliable the ask-side sum as a fair
        // mispricing measure. Only applies to a scored (computed) drift.
        if matches!(raw.eligibility, RawFactorEligibility::Normalizable)
            && raw.raw_value.is_some()
            && let Some(tightness) = leg_tightness(features)
        {
            raw.confidence = Probability::new(raw.confidence.inner() * tightness);
        }
        Ok(raw)
    }
}

/// Leg-tightness corroboration in `[0, 1]`: `1 − mean(ask−bid) per leg`, clamped.
/// Tight two-sided legs (small ask/bid gap) ⇒ the leg-sum drift is a reliable
/// mispricing measure; wide legs ⇒ discount confidence. `None` when the bid-sum
/// or count is unavailable (confidence then keeps its data-quality base).
fn leg_tightness(features: &FeatureVector) -> Option<Decimal> {
    let ask_sum = read(features, &NEGRISK_LEG_ASK_SUM)?;
    let bid_sum = read(features, &NEGRISK_LEG_BID_SUM)?;
    let count = read(features, &NEGRISK_LEG_COUNT)?;
    if count <= Decimal::ZERO {
        return None;
    }
    let mean_gap = (ask_sum - bid_sum) / count;
    Some((Decimal::ONE - mean_gap).clamp(Decimal::ZERO, Decimal::ONE))
}

struct NegRiskConvertEdgeFactor {
    definition_id: FactorDefinitionId,
    spec: FactorDefinitionDocument,
    min_legs: u32,
}

impl FactorComputer for NegRiskConvertEdgeFactor {
    fn definition_id(&self) -> FactorDefinitionId {
        self.definition_id.clone()
    }
    fn spec(&self) -> &FactorDefinitionDocument {
        &self.spec
    }
    fn compute_raw(&self, features: &FeatureVector) -> QuantResult<RawFactor> {
        Ok(negrisk_raw(
            &self.definition_id,
            &self.spec,
            features,
            NEGRISK_CONVERT_EDGE,
            self.min_legs,
            |edge| edge,
            "neg-risk convert edge",
        ))
    }
}

// ── favorite_longshot ───────────────────────────────────────────────────────

struct FavoriteLongshotFactor {
    definition_id: FactorDefinitionId,
    spec: FactorDefinitionDocument,
    bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
    ic_gate: bool,
}

impl FactorComputer for FavoriteLongshotFactor {
    fn definition_id(&self) -> FactorDefinitionId {
        self.definition_id.clone()
    }
    fn spec(&self) -> &FactorDefinitionDocument {
        &self.spec
    }
    fn compute_raw(&self, features: &FeatureVector) -> QuantResult<RawFactor> {
        // No fitted table ⇒ inert (never a fabricated constant).
        let Some(table) = self.bias_table.as_ref() else {
            return Ok(inert(
                self.definition_id.clone(),
                &self.spec,
                RawFactorEligibility::Normalizable,
                "no bias table bound".to_owned(),
            ));
        };
        let category = features.value(&CATEGORY).and_then(feature_category);
        let mid = read(features, &MID);
        let ttr = read(features, &TIME_TO_RESOLUTION_SECS);
        let (Some(category), Some(mid), Some(ttr_secs)) = (category, mid, ttr) else {
            return Ok(inert(
                self.definition_id.clone(),
                &self.spec,
                RawFactorEligibility::Normalizable,
                "missing category, mid, or time-to-resolution".to_owned(),
            ));
        };
        if ttr_secs < Decimal::ZERO {
            return Ok(inert(
                self.definition_id.clone(),
                &self.spec,
                RawFactorEligibility::Normalizable,
                "negative time-to-resolution".to_owned(),
            ));
        }
        let Some(ttr_secs) = ttr_secs.to_u64() else {
            return Ok(inert(
                self.definition_id.clone(),
                &self.spec,
                RawFactorEligibility::Normalizable,
                "time-to-resolution is outside u64 range".to_owned(),
            ));
        };
        Ok(table
            .bias_for(category, ttr_secs, Price::new(mid), self.ic_gate)
            .map_or_else(
                || {
                    inert(
                        self.definition_id.clone(),
                        &self.spec,
                        RawFactorEligibility::Normalizable,
                        "no significant bias for category / ttr / bucket".to_owned(),
                    )
                },
                |bias| {
                    scored(
                        self.definition_id.clone(),
                        &self.spec,
                        bias,
                        features,
                        format!("favorite-longshot bias {bias} @ mid {mid} ttr {ttr_secs}s"),
                        vec![FactorDriver {
                            feature_name: book_names::MID,
                            contribution: mid,
                        }],
                    )
                },
            ))
    }
}

/// Extract a market category from a feature value.
const fn feature_category(value: &FeatureValue) -> Option<MarketCategory> {
    match value {
        FeatureValue::Category(category) => Some(*category),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        enums::quant::DataQualityStatus,
        runtime_config::{FactorsConfig, FeaturesConfig},
        types::{MarketId, SchemaVersion},
    };
    use rust_decimal::Decimal;

    use super::structural_factors;
    use crate::{
        factors::{names::STRUCT_NEGRISK_LEG_SUM_DRIFT, value::RawFactorEligibility},
        features::{
            FeatureCell, FeatureName, FeatureStaleness, FeatureValue, FeatureVector, NullReason,
            names::structural::{NEGRISK_LEG_ASK_SUM, NEGRISK_LEG_COUNT},
        },
    };

    fn vector(values: BTreeMap<FeatureName, FeatureCell>) -> FeatureVector {
        FeatureVector {
            market_id: MarketId::new("m"),
            token_id: None,
            decision_at: Utc.timestamp_opt(0, 0).unwrap(),
            generic_schema_version: SchemaVersion::FIRST,
            generic: values,
            domain: None,
            data_quality: DataQualityStatus::Fresh,
        }
    }

    fn drift_eligibility(values: BTreeMap<FeatureName, FeatureCell>) -> RawFactorEligibility {
        let factors = FactorsConfig::default();
        let features = FeaturesConfig::default();
        let (_, computer) = structural_factors(&factors, &features, None)
            .into_iter()
            .find(|(spec, _)| spec.name == STRUCT_NEGRISK_LEG_SUM_DRIFT)
            .expect("leg-sum-drift factor is registered");
        computer.compute_raw(&vector(values)).unwrap().eligibility
    }

    #[test]
    fn binary_market_gets_not_applicable_for_negrisk_factor() {
        let mut values = BTreeMap::new();
        values.insert(
            NEGRISK_LEG_ASK_SUM,
            FeatureCell::not_applicable(NullReason::NotApplicable),
        );
        values.insert(
            NEGRISK_LEG_COUNT,
            FeatureCell::not_applicable(NullReason::NotApplicable),
        );
        assert!(matches!(
            drift_eligibility(values),
            RawFactorEligibility::NotApplicable
        ));
    }

    #[test]
    fn negrisk_factor_indeterminate_when_leg_book_missing() {
        let mut values = BTreeMap::new();
        values.insert(
            NEGRISK_LEG_ASK_SUM,
            FeatureCell::missing(NullReason::LegBookMissing, None, FeatureStaleness::Unknown),
        );
        values.insert(
            NEGRISK_LEG_COUNT,
            FeatureCell::missing(NullReason::LegBookMissing, None, FeatureStaleness::Unknown),
        );
        assert!(matches!(
            drift_eligibility(values),
            RawFactorEligibility::Indeterminate(_)
        ));
    }

    #[test]
    fn negrisk_leg_sum_drift_computed_for_neg_risk_market() {
        let mut values = BTreeMap::new();
        // Σ best-ask across 3 legs = 1.08 ⇒ drift = 0.08.
        values.insert(
            NEGRISK_LEG_ASK_SUM,
            FeatureCell::observed(
                FeatureValue::Decimal(Decimal::new(108, 2)),
                None,
                FeatureStaleness::Unknown,
            ),
        );
        values.insert(
            NEGRISK_LEG_COUNT,
            FeatureCell::observed(FeatureValue::Count(3), None, FeatureStaleness::Unknown),
        );
        let factors = FactorsConfig::default();
        let features = FeaturesConfig::default();
        let (_, computer) = structural_factors(&factors, &features, None)
            .into_iter()
            .find(|(spec, _)| spec.name == STRUCT_NEGRISK_LEG_SUM_DRIFT)
            .expect("factor");
        let raw = computer.compute_raw(&vector(values)).unwrap();
        assert!(matches!(
            raw.eligibility,
            RawFactorEligibility::Normalizable
        ));
        assert_eq!(raw.raw_value, Some(Decimal::new(8, 2)));
    }
}
