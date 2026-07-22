//! Content-addressed identity for immutable factor-definition revisions.

use quant_pivot_error::QuantResult;
use quant_pivot_models::types::{
    ContentHash, FactorDefinitionId, factor::factor_definition_content_hash,
};
use uuid::Uuid;

use crate::factors::FactorDefinitionDocument;

/// Immutable identity of one logical factor revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactorDefinitionIdentity {
    /// UUID primary key deterministically derived from `definition_hash`.
    pub factor_definition_id: FactorDefinitionId,
    /// Canonical digest of the full factor definition + feature contract.
    pub definition_hash: ContentHash,
    /// Feature contract the factor revision consumes.
    pub feature_contract_hash: ContentHash,
}

/// Derive one content-addressed revision identity.
///
/// # Errors
///
/// Propagates canonical serialization/hash failures.
pub fn factor_definition_identity(
    definition: &FactorDefinitionDocument,
    feature_contract_hash: &ContentHash,
) -> QuantResult<FactorDefinitionIdentity> {
    let definition_hash = factor_definition_content_hash(definition, feature_contract_hash)?;
    let factor_definition_id = FactorDefinitionId::from_definition_hash(&definition_hash);
    Ok(FactorDefinitionIdentity {
        factor_definition_id,
        definition_hash,
        feature_contract_hash: *feature_contract_hash,
    })
}

/// Provisional logical-name identity used only inside factor computers before
/// [`crate::factors::FactorEngine`] overwrites it with the content-addressed
/// governed revision. It is never persisted or exposed by the engine.
#[must_use]
pub fn provisional_factor_definition_id(name: &str) -> FactorDefinitionId {
    const PROVISIONAL_NAMESPACE: Uuid = Uuid::from_u128(0xe21f_20a1_094f_4c15_88b2_49cd_ee74_5018);
    FactorDefinitionId::new(Uuid::new_v5(&PROVISIONAL_NAMESPACE, name.as_bytes()))
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        enums::{
            factor::{FactorFamily, FactorNormalization},
            quant::FactorDirection,
        },
        types::{ContentHash, FactorDefinitionId, Probability},
    };

    use super::factor_definition_identity;
    use crate::factors::{
        FactorDefinitionDocument, FactorName, FactorOutputKind, FactorQualityGate,
    };

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }

    fn definition() -> FactorDefinitionDocument {
        FactorDefinitionDocument {
            name: FactorName::new("momentum"),
            family: FactorFamily::Momentum,
            input_features: Vec::new(),
            output_kind: FactorOutputKind::Directional,
            default_direction: FactorDirection::Positive,
            normalization: FactorNormalization::Rank,
            owner: "quant".to_owned(),
            quality_gates: vec![FactorQualityGate {
                name: "observed".to_owned(),
                min_confidence: Probability::ONE,
            }],
        }
    }

    #[test]
    fn identity_changes_with_definition_or_feature_contract() {
        let first = factor_definition_identity(&definition(), &hash('a')).expect("first");
        let same = factor_definition_identity(&definition(), &hash('a')).expect("same");
        let new_contract = factor_definition_identity(&definition(), &hash('b')).expect("new");
        let mut revised = definition();
        revised.owner = "research".to_owned();
        let new_definition = factor_definition_identity(&revised, &hash('a')).expect("revised");

        assert_eq!(first, same);
        assert_eq!(
            first.factor_definition_id,
            FactorDefinitionId::from_definition_hash(&first.definition_hash)
        );
        assert_ne!(
            first.factor_definition_id,
            new_contract.factor_definition_id
        );
        assert_ne!(
            first.factor_definition_id,
            new_definition.factor_definition_id
        );
    }
}
