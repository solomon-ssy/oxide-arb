//! In-memory feature vector composed from canonical feature value objects.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
pub use quant_pivot_models::{
    enums::feature::{EvidenceSourceKind, FeatureValueKind},
    types::{
        DomainFeatureSlice, EvidenceSourceRef, FeatureCell, FeatureCellState, FeatureStaleness,
        FeatureValue, NullReason, stable_name::FeatureName,
    },
};
use quant_pivot_models::{
    enums::quant::DataQualityStatus,
    types::{MarketId, SchemaVersion, TokenId},
};
use serde::{Deserialize, Serialize};

/// An in-memory, point-in-time feature vector for one market.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureVector {
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub decision_at: DateTime<Utc>,
    pub generic_schema_version: SchemaVersion,
    pub generic: BTreeMap<FeatureName, FeatureCell>,
    pub domain: Option<DomainFeatureSlice>,
    pub data_quality: DataQualityStatus,
}

impl FeatureVector {
    #[must_use]
    pub fn cell(&self, name: &FeatureName) -> Option<&FeatureCell> {
        self.generic.get(name).or_else(|| {
            self.domain
                .as_ref()
                .and_then(|slice| slice.values.get(name))
        })
    }

    #[must_use]
    pub fn value(&self, name: &FeatureName) -> Option<&FeatureValue> {
        self.cell(name).and_then(FeatureCell::value)
    }

    pub fn iter_cells(&self) -> impl Iterator<Item = (&FeatureName, &FeatureCell)> {
        self.generic
            .iter()
            .chain(self.domain.iter().flat_map(|slice| slice.values.iter()))
    }

    pub fn iter_values(&self) -> impl Iterator<Item = (&FeatureName, &FeatureValue)> {
        self.iter_cells()
            .filter_map(|(name, cell)| cell.value().map(|value| (name, value)))
    }

    #[must_use]
    pub fn value_count(&self) -> usize {
        self.generic.len() + self.domain.as_ref().map_or(0, |slice| slice.values.len())
    }

    #[must_use]
    pub fn max_known_staleness_ms(&self) -> Option<u64> {
        self.iter_cells()
            .filter_map(|(_, cell)| match cell.staleness {
                FeatureStaleness::Known { age_ms } => Some(age_ms),
                FeatureStaleness::Unknown => None,
            })
            .max()
    }

    pub fn substituted_cells(&self) -> impl Iterator<Item = (&FeatureName, &FeatureCell)> {
        self.iter_cells()
            .filter(|(_, cell)| cell.state == FeatureCellState::Substituted)
    }

    #[must_use]
    pub fn evidence_refs(&self) -> Vec<EvidenceSourceRef> {
        let mut refs = Vec::new();
        for evidence in self
            .iter_cells()
            .filter_map(|(_, cell)| cell.evidence.as_ref())
        {
            if !refs.contains(evidence) {
                refs.push(evidence.clone());
            }
        }
        refs
    }

    #[must_use]
    pub fn substitution_reasons(&self) -> Vec<NullReason> {
        self.substituted_cells()
            .filter_map(|(_, cell)| cell.reason)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::types::{
        FeatureCell, FeatureCellState, FeatureStaleness, FeatureValue, NullReason,
    };
    use rust_decimal_macros::dec;

    #[test]
    fn feature_cell_states_do_not_fabricate_values() {
        let observed = FeatureCell::observed(
            FeatureValue::Decimal(dec!(1.25)),
            None,
            FeatureStaleness::Unknown,
        );
        let substituted = FeatureCell::substituted(
            FeatureValue::Decimal(dec!(0.5)),
            NullReason::SourceUnavailable,
            None,
            FeatureStaleness::Unknown,
        );
        let missing = FeatureCell::missing(
            NullReason::SourceUnavailable,
            None,
            FeatureStaleness::Unknown,
        );
        let not_applicable = FeatureCell::not_applicable(NullReason::NotApplicable);

        assert_eq!(observed.state, FeatureCellState::Observed);
        assert!(observed.value().is_some());
        assert_eq!(substituted.state, FeatureCellState::Substituted);
        assert!(substituted.value().is_some());
        assert_eq!(missing.state, FeatureCellState::Missing);
        assert!(missing.value().is_none());
        assert_eq!(not_applicable.state, FeatureCellState::NotApplicable);
        assert!(not_applicable.value().is_none());
    }
}
