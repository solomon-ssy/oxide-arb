//! Reconstruct a [`MarketFactorOutcome`] from a recommendation's persisted,
//! entry-time factor breakdown.
//!
//! Exit-side re-inference must reproduce the **entry** factor thesis, not
//! recompute factors on a single-market pseudo cross-section. Post-11.1 a market
//! has no peers at exit time, so recomputing would yield
//! [`NormalizedFactor::Indeterminate`] for every cross-sectional factor and the
//! exit model would score a degenerate factor plane. The frozen breakdown carries
//! the exact normalized scores, provenance, directions, and confidences the entry
//! cross-section produced, so the exit model scores the identical factor plane it
//! entered on. Missing / empty breakdowns fail closed (`None`) rather than
//! fabricate a neutral.

use chrono::{DateTime, Utc};
use quant_pivot_models::types::{FactorBreakdownEntry, MarketId};

use crate::factors::{
    generic::factor_definition_id,
    normalize::NormalizedFactor,
    value::{
        FactorEligibility, FactorExplanation, FactorName, FactorValue, MarketFactorOutcome,
        ScoredFactor,
    },
};

/// Rebuild the entry-time [`MarketFactorOutcome`] from a recommendation's frozen
/// factor breakdown.
///
/// Returns `None` when the breakdown is empty — the entry thesis cannot be
/// reproduced, so the caller must fail closed rather than score an empty plane.
#[must_use]
pub fn frozen_factor_outcome(
    market_id: MarketId,
    as_of: DateTime<Utc>,
    breakdown: &[FactorBreakdownEntry],
) -> Option<MarketFactorOutcome> {
    if breakdown.is_empty() {
        return None;
    }
    let factors = breakdown.iter().map(scored_from_entry).collect();
    Some(MarketFactorOutcome {
        market_id,
        as_of,
        // The recommendation existed, so its entry cross-section was eligible.
        eligibility: FactorEligibility::Eligible,
        factors,
    })
}

/// Reconstruct one [`ScoredFactor`] from a persisted breakdown entry — the
/// faithful inverse of the report composer's `FactorBreakdownEntry` projection.
fn scored_from_entry(entry: &FactorBreakdownEntry) -> ScoredFactor {
    let normalization = match (
        entry.normalized_score,
        entry.normalization_source,
        entry.indeterminate_reason,
    ) {
        (Some(score), Some(source), _) => NormalizedFactor::Scored {
            score,
            source,
            // The clamp audit is a display-only annotation, not persisted on the
            // breakdown; scoring never reads it.
            clamp: None,
        },
        (_, _, Some(reason)) => NormalizedFactor::Indeterminate { reason },
        _ => NormalizedFactor::MissingInput,
    };
    let scored = matches!(normalization, NormalizedFactor::Scored { .. });
    let value = FactorValue {
        definition_id: factor_definition_id(entry.factor_name.as_str()),
        name: FactorName::new(entry.factor_name.clone()),
        family: entry.family,
        raw_value: entry.raw_value,
        normalization,
        direction: entry.direction,
        confidence: entry.confidence,
        explanation: FactorExplanation {
            headline: entry.explanation.clone(),
            drivers: Vec::new(),
        },
        input_feature_refs: Vec::new(),
    };
    ScoredFactor {
        value,
        contributes: scored,
        below_confidence_floor: false,
    }
}

#[cfg(test)]
mod tests {
    use super::frozen_factor_outcome;
    use chrono::Utc;
    use quant_pivot_models::{
        enums::{
            factor::{
                FactorFamily, FactorIndeterminateReason, FactorValueState, NormalizationSource,
            },
            quant::FactorDirection,
        },
        types::{FactorBreakdownEntry, MarketId, Probability},
    };
    use rust_decimal::Decimal;

    use crate::factors::NormalizedFactor;

    fn scored_entry(name: &str, score: i64) -> FactorBreakdownEntry {
        FactorBreakdownEntry {
            factor_name: name.to_owned(),
            family: FactorFamily::Momentum,
            value_state: FactorValueState::Scored,
            raw_value: Some(Decimal::from(score)),
            normalized_score: Some(Probability::new(Decimal::new(score, 1))),
            normalization_source: Some(NormalizationSource::CrossSection),
            indeterminate_reason: None,
            weight: Decimal::new(5, 1),
            contribution: Decimal::new(score, 2),
            confidence: Probability::new(Decimal::new(9, 1)),
            direction: FactorDirection::Positive,
            explanation: format!("{name} contributed"),
            source_refs: vec!["fact:1".to_owned()],
        }
    }

    fn indeterminate_entry(name: &str) -> FactorBreakdownEntry {
        FactorBreakdownEntry {
            factor_name: name.to_owned(),
            family: FactorFamily::Liquidity,
            value_state: FactorValueState::Indeterminate,
            raw_value: Some(Decimal::from(3)),
            normalized_score: None,
            normalization_source: None,
            indeterminate_reason: Some(FactorIndeterminateReason::CrossSectionTooSmall),
            weight: Decimal::new(5, 1),
            contribution: Decimal::ZERO,
            confidence: Probability::new(Decimal::new(8, 1)),
            direction: FactorDirection::Positive,
            explanation: format!("{name} indeterminate"),
            source_refs: Vec::new(),
        }
    }

    #[test]
    fn reconstructs_entry_factor_plane_not_all_indeterminate() {
        // The frozen breakdown replays the entry cross-section verbatim: scored
        // factors stay scored (the exit model sees the same plane it entered on),
        // never collapsing to indeterminate as a single-market recompute would.
        let breakdown = vec![
            scored_entry("momentum_roc", 7),
            scored_entry("momentum_vol_adjusted", 3),
            indeterminate_entry("liquidity_depth"),
        ];
        let outcome = frozen_factor_outcome(MarketId::new("0xmarket"), Utc::now(), &breakdown)
            .expect("outcome");
        assert!(outcome.eligibility.is_eligible());
        assert_eq!(outcome.factors.len(), 3);

        let scored_count = outcome
            .factors
            .iter()
            .filter(|f| f.value.is_scored())
            .count();
        assert_eq!(scored_count, 2, "the two scored entries must stay scored");

        let roc = &outcome.factors[0].value;
        assert_eq!(roc.name.as_str(), "momentum_roc");
        assert_eq!(
            roc.normalized_score(),
            Some(Probability::new(Decimal::new(7, 1)))
        );
        assert_eq!(
            roc.normalization_source(),
            Some(NormalizationSource::CrossSection)
        );
        assert!(matches!(
            outcome.factors[0].value.normalization,
            NormalizedFactor::Scored { .. }
        ));

        // The indeterminate entry round-trips with its recorded reason (no fake 0.5).
        let liq = &outcome.factors[2].value;
        assert_eq!(
            liq.indeterminate_reason(),
            Some(FactorIndeterminateReason::CrossSectionTooSmall)
        );
    }

    #[test]
    fn empty_breakdown_fails_closed() {
        assert!(frozen_factor_outcome(MarketId::new("0xmarket"), Utc::now(), &[]).is_none());
    }
}
