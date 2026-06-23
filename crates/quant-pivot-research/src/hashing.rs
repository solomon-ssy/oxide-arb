//! Canonical content hashing for research artifacts.
//!
//! [`ResearchHasher`] is a thin, typed facade over
//! [`quant_pivot_models::hashing::CanonicalDigest`] so the research plane shares
//! the one platform-wide `blake3:` contract instead of re-implementing hashing.
//!
//! # Determinism contract
//!
//! Canonical hashing is byte-exact over the serialized form, so **set-like
//! inputs must be order-normalized before hashing**. Prefer the typed helpers
//! ([`ResearchHasher::factor_schema`], [`ResearchHasher::model_feature_requirements`],
//! [`ResearchHasher::feature_names`]) over raw [`ResearchHasher::ordered`] at
//! call sites. Map-keyed compute types (e.g. [`crate::features::FeatureVector::values`],
//! a `BTreeMap`) are already canonical by construction.

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    hashing::CanonicalDigest,
    types::{ContentHash, SchemaVersion},
};
use serde::Serialize;

use crate::{
    factors::FactorSet,
    features::{FeatureName, FeatureSchema, FeatureVector},
    model::ModelArtifact,
    selection::ModelFeatureRequirements,
    training::TrainingDatasetArtifact,
};

/// Canonical JSON shape for [`ResearchHasher::feature_schema`].
#[derive(serde::Serialize)]
struct FeatureSchemaCanonical {
    version: SchemaVersion,
    features: Vec<FeatureName>,
}

/// Typed canonical hasher for research artifacts (`blake3:<hex>`).
pub struct ResearchHasher;

impl ResearchHasher {
    /// Canonical hash of any serializable value, verbatim over its bytes.
    ///
    /// The caller owns determinism: only use this for values whose serialized
    /// form is already order-stable (structs, `BTreeMap`s). For sets, use
    /// [`Self::ordered`].
    pub fn canonical<T>(value: &T) -> QuantResult<ContentHash>
    where
        T: Serialize + ?Sized,
    {
        Ok(CanonicalDigest::content_hash_json(value)?)
    }

    /// Order-independent hash of a set: clones, sorts, then hashes.
    ///
    /// Guarantees the same digest regardless of input order, which is required
    /// for feature-name sets, id sets, and any other unordered collection that
    /// participates in a schema or artifact hash.
    pub fn ordered<T>(items: &[T]) -> QuantResult<ContentHash>
    where
        T: Ord + Clone + Serialize,
    {
        let mut sorted = items.to_vec();
        sorted.sort();
        Self::canonical(&sorted)
    }

    /// Order-independent hash of feature names (selection requirements, schema arms).
    pub fn feature_names(names: &[FeatureName]) -> QuantResult<ContentHash> {
        Self::ordered(names)
    }

    /// Order-independent hash of a governed feature schema (`feature_schema_hash`).
    ///
    /// Feature names are sorted before serialization so registry insertion order
    /// never perturbs the digest.
    pub fn feature_schema(schema: &FeatureSchema) -> QuantResult<ContentHash> {
        let mut features = schema.features.clone();
        features.sort();
        Self::canonical(&FeatureSchemaCanonical {
            version: schema.version,
            features,
        })
    }

    /// Order-independent hash of model feature requirements for selector inputs.
    pub fn model_feature_requirements(
        requirements: &ModelFeatureRequirements,
    ) -> QuantResult<ContentHash> {
        Self::feature_names(&requirements.required_features)
    }

    /// Order-independent hash of a governed factor set (`factor_schema_hash`).
    ///
    /// Definitions are sorted by stable [`FactorDefinitionSpec::name`] before
    /// serialization so registry insertion order never perturbs the digest.
    pub fn factor_schema(set: &FactorSet) -> QuantResult<ContentHash> {
        let mut definitions = set.definitions.clone();
        definitions.sort_by(|left, right| left.name.cmp(&right.name));
        Self::canonical(&definitions)
    }

    /// Canonical hash of an in-memory feature vector.
    ///
    /// `FeatureVector::values` is a `BTreeMap`, so the digest is independent of
    /// feature insertion order by construction.
    pub fn feature_vector(vector: &FeatureVector) -> QuantResult<ContentHash> {
        Self::canonical(vector)
    }

    /// Canonical hash of a serialized model artifact.
    pub fn model_artifact(artifact: &ModelArtifact) -> QuantResult<ContentHash> {
        Self::canonical(artifact)
    }

    /// Canonical hash of a frozen training dataset artifact.
    pub fn dataset(artifact: &TrainingDatasetArtifact) -> QuantResult<ContentHash> {
        Self::canonical(artifact)
    }
}

#[cfg(test)]
mod tests {
    use super::ResearchHasher;
    use crate::{
        factors::{
            FactorDefinitionSpec, FactorFamily, FactorName, FactorOutputKind, FactorSet,
            NormalizationSpec,
        },
        features::{FeatureName, FeatureSchema},
        selection::ModelFeatureRequirements,
    };
    use quant_pivot_models::{enums::quant::FactorDirection, types::SchemaVersion};

    fn sample_factor(name: &'static str) -> FactorDefinitionSpec {
        FactorDefinitionSpec {
            name: FactorName::from_static(name),
            family: FactorFamily::Liquidity,
            input_features: Vec::new(),
            output_kind: FactorOutputKind::NormalizedScore,
            default_direction: FactorDirection::Positive,
            normalization: NormalizationSpec::Rank,
            owner: "test".to_owned(),
            quality_gates: Vec::new(),
        }
    }

    #[test]
    fn ordered_is_independent_of_input_order() {
        let forward = ResearchHasher::ordered(&["alpha", "beta", "gamma"]).expect("hash");
        let shuffled = ResearchHasher::ordered(&["gamma", "alpha", "beta"]).expect("hash");
        assert_eq!(forward, shuffled, "set hash must be order-independent");
    }

    #[test]
    fn distinct_sets_differ() {
        let a = ResearchHasher::ordered(&["alpha", "beta"]).expect("hash");
        let b = ResearchHasher::ordered(&["alpha", "beta", "gamma"]).expect("hash");
        assert_ne!(a, b);
    }

    #[test]
    fn model_feature_requirements_is_order_independent() {
        let forward = ModelFeatureRequirements {
            required_features: vec![
                FeatureName::from_static("alpha"),
                FeatureName::from_static("beta"),
            ],
        };
        let shuffled = ModelFeatureRequirements {
            required_features: vec![
                FeatureName::from_static("beta"),
                FeatureName::from_static("alpha"),
            ],
        };
        let forward_hash = ResearchHasher::model_feature_requirements(&forward).expect("hash");
        let shuffled_hash = ResearchHasher::model_feature_requirements(&shuffled).expect("hash");
        assert_eq!(forward_hash, shuffled_hash);
    }

    #[test]
    fn feature_schema_is_order_independent() {
        let forward = FeatureSchema {
            version: SchemaVersion::new(1),
            features: vec![
                FeatureName::from_static("alpha"),
                FeatureName::from_static("beta"),
            ],
        };
        let shuffled = FeatureSchema {
            version: SchemaVersion::new(1),
            features: vec![
                FeatureName::from_static("beta"),
                FeatureName::from_static("alpha"),
            ],
        };
        assert_eq!(
            ResearchHasher::feature_schema(&forward).expect("hash"),
            ResearchHasher::feature_schema(&shuffled).expect("hash"),
        );
    }

    #[test]
    fn factor_schema_is_order_independent() {
        let forward = FactorSet {
            definitions: vec![sample_factor("alpha"), sample_factor("beta")],
        };
        let shuffled = FactorSet {
            definitions: vec![sample_factor("beta"), sample_factor("alpha")],
        };
        let forward_hash = ResearchHasher::factor_schema(&forward).expect("hash");
        let shuffled_hash = ResearchHasher::factor_schema(&shuffled).expect("hash");
        assert_eq!(forward_hash, shuffled_hash);
    }

    #[test]
    fn factor_schema_distinguishes_different_sets() {
        let two = FactorSet {
            definitions: vec![sample_factor("alpha"), sample_factor("beta")],
        };
        let three = FactorSet {
            definitions: vec![
                sample_factor("alpha"),
                sample_factor("beta"),
                sample_factor("gamma"),
            ],
        };
        assert_ne!(
            ResearchHasher::factor_schema(&two).expect("hash"),
            ResearchHasher::factor_schema(&three).expect("hash"),
        );
    }
}
