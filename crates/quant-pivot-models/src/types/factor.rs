//! Canonical governed factor definition and explanation documents.

use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{
        factor::{FactorFamily, FactorNormalization},
        quant::FactorDirection,
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, Probability,
        stable_name::{FactorName, FeatureName},
    },
};

/// Output classification of a governed factor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorOutputKind {
    NormalizedScore,
    Directional,
}

/// One publication gate in a factor definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactorQualityGate {
    pub name: String,
    pub min_confidence: Probability,
}

/// Immutable factor-definition document persisted as one JSONB value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct FactorDefinitionDocument {
    pub name: FactorName,
    pub family: FactorFamily,
    pub input_features: Vec<FeatureName>,
    pub output_kind: FactorOutputKind,
    pub default_direction: FactorDirection,
    pub normalization: FactorNormalization,
    pub owner: String,
    pub quality_gates: Vec<FactorQualityGate>,
}

#[derive(Serialize)]
struct CanonicalFactorDefinition<'a> {
    definition: &'a FactorDefinitionDocument,
    feature_contract_hash: &'a ContentHash,
}

/// Derive the canonical content address for one immutable factor definition.
///
/// Producers and persistence verifiers must both call this function so the
/// hashed JSON shape cannot drift between the research and repository layers.
pub fn factor_definition_content_hash(
    definition: &FactorDefinitionDocument,
    feature_contract_hash: &ContentHash,
) -> Result<ContentHash, CanonicalDigestError> {
    CanonicalDigest::content_hash_json(&CanonicalFactorDefinition {
        definition,
        feature_contract_hash,
    })
}

impl FactorDefinitionDocument {
    #[must_use]
    pub const fn is_required(&self) -> bool {
        !self.quality_gates.is_empty()
    }
}

/// One feature contribution in a factor explanation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactorDriver {
    pub feature_name: FeatureName,
    pub contribution: Decimal,
}

/// Fixed factor explanation persisted atomically with a factor value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct FactorExplanation {
    pub headline: String,
    pub drivers: Vec<FactorDriver>,
}

#[cfg(test)]
mod tests {
    use super::{FactorDefinitionDocument, FactorExplanation};

    #[test]
    fn factor_documents_reject_unknown() {
        let explanation = serde_json::json!({
            "headline": "depth is strong",
            "drivers": [],
            "legacy_detail": true
        });
        assert!(serde_json::from_value::<FactorExplanation>(explanation).is_err());

        let definition = serde_json::json!({
            "name": "liquidity_depth",
            "family": "liquidity",
            "input_features": [],
            "output_kind": "normalized_score",
            "default_direction": "positive",
            "normalization": "rank",
            "owner": "research",
            "quality_gates": [],
            "unknown": true
        });
        assert!(serde_json::from_value::<FactorDefinitionDocument>(definition).is_err());
    }
}
