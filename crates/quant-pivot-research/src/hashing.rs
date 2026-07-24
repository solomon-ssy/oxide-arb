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
//! call sites. Map-keyed compute types (e.g. [`crate::features::FeatureVector::generic`]
//! / [`quant_pivot_models::types::DomainFeatureSlice::values`], both `BTreeMap`s)
//! are already canonical by construction.

use std::collections::BTreeMap;

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::{common::MarketCategory, domain::DomainFamily},
    hashing::CanonicalDigest,
    types::{ContentHash, SchemaVersion},
};
use serde::Serialize;

use crate::{
    factors::FactorSet,
    features::{
        FeatureCell, FeatureName, FeatureSchema, FeatureSpec, FeatureStaleness, FeatureValue,
        FeatureVector,
    },
    model::{
        ModelArtifact,
        artifact::{MODEL_ARTIFACT_FORMAT_VERSION, StoredModelArtifactRef},
    },
    selection::ModelFeatureRequirements,
    training::LabelName,
};

/// Canonical JSON shape for [`ResearchHasher::feature_schema`].
#[derive(serde::Serialize)]
struct FeatureSchemaCanonical {
    version: SchemaVersion,
    specs: Vec<FeatureSpec>,
}

/// Decimal places every hashed feature value is quantized to before it enters
/// [`ResearchHasher::feature_vector`]'s composite digest.
///
/// Mirrors the crate-wide statistical quantization convention (see
/// `features::generic::stats::STAT_SCALE`), kept as its own constant here
/// because quantizing **for hashing** must never mutate the value actually
/// stored in / consumed from the vector — only the hash input is rounded.
/// Twelve places sits well inside `f64`'s ~15 decimal digits of precision, so
/// an `f64`-derived statistic that differs only past the 12th decimal place
/// (cross-platform floating-point noise) can never perturb `feature_hash`.
const HASH_STAT_SCALE: u32 = 12;

/// Composite `feature_hash` layout: `generic_schema_version`, the sorted
/// generic-slice digest, and the domain slice's family / schema version /
/// digest (each `None` when the vector carries no domain slice at all).
///
/// `Option::None` serializes to JSON `null`, an unambiguous sentinel distinct
/// from every real `DomainFamily` / `SchemaVersion` / `ContentHash` value —
/// stronger than a string literal sentinel, which would risk colliding with a
/// family whose wire tag happened to be that literal string.
#[derive(Serialize)]
struct FeatureHashComposite {
    generic_schema_version: SchemaVersion,
    generic_hash: ContentHash,
    domain_family: Option<DomainFamily>,
    domain_schema_version: Option<SchemaVersion>,
    domain_hash: Option<ContentHash>,
}

/// Quantize every embedded `Decimal` in `values` at [`HASH_STAT_SCALE`],
/// returning a hash-only projection (the caller's stored map is untouched).
fn quantized_for_hash(
    values: &BTreeMap<FeatureName, FeatureCell>,
) -> BTreeMap<FeatureName, FeatureCell> {
    values
        .iter()
        .map(|(name, cell)| {
            let mut projected = cell.clone();
            projected.value = projected.value.as_ref().map(quantize_value_for_hash);
            projected.evidence = None;
            projected.staleness = FeatureStaleness::Unknown;
            (name.clone(), projected)
        })
        .collect()
}

/// Quantize the `Decimal` payload of one [`FeatureValue`] for hashing.
///
/// `Count` / `Bool` / `Category` / `Missing` carry no `Decimal` and pass
/// through verbatim.
fn quantize_value_for_hash(value: &FeatureValue) -> FeatureValue {
    match *value {
        FeatureValue::Decimal(inner) => FeatureValue::Decimal(inner.round_dp(HASH_STAT_SCALE)),
        FeatureValue::Bps(inner) => FeatureValue::Bps(inner.round_dp(HASH_STAT_SCALE)),
        FeatureValue::Usd(inner) => FeatureValue::Usd(inner.round_dp(HASH_STAT_SCALE)),
        FeatureValue::Probability(inner) => {
            FeatureValue::Probability(inner.round_dp(HASH_STAT_SCALE))
        }
        FeatureValue::Count(_) | FeatureValue::Bool(_) | FeatureValue::Category(_) => value.clone(),
    }
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
    /// Specs are sorted by name before serialization so registry insertion order
    /// never perturbs the digest; the schema version folds in, so a version bump
    /// (or any spec change) changes the digest.
    pub fn feature_schema(schema: &FeatureSchema) -> QuantResult<ContentHash> {
        let mut specs = schema.specs().to_vec();
        specs.sort_by(|left, right| left.name.cmp(&right.name));
        Self::canonical(&FeatureSchemaCanonical {
            version: schema.version(),
            specs,
        })
    }

    /// Order-independent hash of model feature requirements for selector inputs.
    ///
    /// Both `generic` and every `by_category` vector are sorted before
    /// hashing (via [`ModelFeatureRequirements::for_category`]'s underlying
    /// `BTreeSet` normalization is not reused here since categories must stay
    /// distinguishable); the map's own key order is already canonical
    /// (`BTreeMap`).
    pub fn model_feature_requirements(
        requirements: &ModelFeatureRequirements,
    ) -> QuantResult<ContentHash> {
        #[derive(Serialize)]
        struct Canonical {
            generic: Vec<FeatureName>,
            by_category: BTreeMap<MarketCategory, Vec<FeatureName>>,
        }
        let mut generic = requirements.generic.clone();
        generic.sort();
        let by_category: BTreeMap<_, Vec<_>> = requirements
            .by_category
            .iter()
            .map(|(category, features)| {
                let mut sorted = features.clone();
                sorted.sort();
                (*category, sorted)
            })
            .collect();
        Self::canonical(&Canonical {
            generic,
            by_category,
        })
    }

    /// Order-independent hash of a governed factor set (`factor_schema_hash`).
    ///
    /// Definitions are sorted by stable `FactorDefinitionDocument::name` before
    /// serialization so registry insertion order never perturbs the digest.
    pub fn factor_schema(set: &FactorSet) -> QuantResult<ContentHash> {
        let mut definitions = set.definitions.clone();
        definitions.sort_by(|left, right| left.name.cmp(&right.name));
        Self::canonical(&definitions)
    }

    /// Composite content hash of an in-memory two-layer feature vector.
    ///
    /// `feature_hash = H(generic_schema_version, H(generic), domain_family |
    /// None, domain_schema_version | None, H(domain_values) | None)`. Only
    /// the components above participate — `market_id` / `token_id` / `as_of`
    /// / `substitutions` / `data_quality` / `staleness_ms` / `source_refs`
    /// are audit/context metadata, never content, and must never perturb the
    /// digest of an otherwise-identical feature computation (e.g. two ingest
    /// runs at different staleness for the same underlying values). Every
    /// `Decimal` payload is quantized at `HASH_STAT_SCALE` before hashing
    /// (the stored vector itself is untouched) so cross-platform floating
    /// point noise can never flip the digest. `domain: None` (structurally
    /// absent) is byte-distinct from a present slice by construction (`Some`
    /// vs `None` in the composite, not string sentinels).
    ///
    /// # Errors
    ///
    /// Propagates canonical-serialization failures.
    pub fn feature_vector(vector: &FeatureVector) -> QuantResult<ContentHash> {
        let generic_hash = Self::canonical(&quantized_for_hash(&vector.generic))?;
        let (domain_family, domain_schema_version, domain_hash) = match &vector.domain {
            Some(slice) => (
                Some(slice.family),
                Some(slice.schema_version),
                Some(Self::canonical(&quantized_for_hash(&slice.values))?),
            ),
            None => (None, None, None),
        };
        Self::canonical(&FeatureHashComposite {
            generic_schema_version: vector.generic_schema_version,
            generic_hash,
            domain_family,
            domain_schema_version,
            domain_hash,
        })
    }

    /// Canonical hash of a serialized model artifact.
    pub fn model_artifact(artifact: &ModelArtifact) -> QuantResult<ContentHash> {
        Self::canonical(&StoredModelArtifactRef {
            format_version: MODEL_ARTIFACT_FORMAT_VERSION,
            artifact,
        })
    }

    /// Order-independent hash of a dataset's label schema (`label_schema_hash`).
    ///
    /// The label-name set defines the supervised target columns; sorting makes
    /// the digest independent of labeler registration order.
    pub fn label_schema(names: &[LabelName]) -> QuantResult<ContentHash> {
        Self::ordered(names)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        enums::{
            common::MarketCategory,
            domain::DomainFamily,
            factor::{FactorFamily, FactorNormalization},
            feature::EvidenceSourceKind,
            quant::{DataQualityStatus, FactorDirection},
        },
        runtime_config::FeatureFamily,
        types::{MarketId, Probability, SchemaVersion, TokenId},
    };
    use rust_decimal_macros::dec;

    use super::ResearchHasher;
    use crate::{
        factors::{FactorDefinitionDocument, FactorName, FactorOutputKind, FactorSet},
        features::{
            DomainFeatureSlice, EvidenceSourceRef, FeatureCell, FeatureName, FeatureSchema,
            FeatureSpec, FeatureStaleness, FeatureUnit, FeatureValue, FeatureValueKind,
            FeatureVector, NullPolicy, PitRule, SourceRequirement, StalenessRule,
        },
        selection::ModelFeatureRequirements,
    };

    fn sample_factor(name: &'static str) -> FactorDefinitionDocument {
        FactorDefinitionDocument {
            name: FactorName::from_static(name),
            family: FactorFamily::Liquidity,
            input_features: Vec::new(),
            output_kind: FactorOutputKind::NormalizedScore,
            default_direction: FactorDirection::Positive,
            normalization: FactorNormalization::Rank,
            owner: "test".to_owned(),
            quality_gates: Vec::new(),
        }
    }

    #[test]
    fn ordered_independent_input_order() {
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
    fn model_feature_requirements_independent() {
        let forward = ModelFeatureRequirements::generic_only(vec![
            FeatureName::from_static("alpha"),
            FeatureName::from_static("beta"),
        ]);
        let shuffled = ModelFeatureRequirements::generic_only(vec![
            FeatureName::from_static("beta"),
            FeatureName::from_static("alpha"),
        ]);
        let forward_hash = ResearchHasher::model_feature_requirements(&forward).expect("hash");
        let shuffled_hash = ResearchHasher::model_feature_requirements(&shuffled).expect("hash");
        assert_eq!(forward_hash, shuffled_hash);
    }

    #[test]
    fn model_feature_distinguishes_sets() {
        let generic_only =
            ModelFeatureRequirements::generic_only(vec![FeatureName::from_static("book.mid")]);
        let mut with_category = generic_only.clone();
        with_category.by_category.insert(
            MarketCategory::Crypto,
            vec![FeatureName::from_static("domain.crypto.distance_to_strike")],
        );
        assert_ne!(
            ResearchHasher::model_feature_requirements(&generic_only).expect("hash"),
            ResearchHasher::model_feature_requirements(&with_category).expect("hash"),
        );
    }

    fn sample_spec(name: &'static str) -> FeatureSpec {
        FeatureSpec {
            name: FeatureName::from_static(name),
            compute_revision: 1,
            family: FeatureFamily::PriceBook,
            value_kind: FeatureValueKind::Decimal,
            unit: FeatureUnit::Ratio,
            valid_range: None,
            null_policy: NullPolicy::Penalize,
            source_requirement: SourceRequirement::PublishedL2Book,
            point_in_time_rule: PitRule::BookVersionAtOrBeforeSourceCutoff,
            staleness_policy: StalenessRule::MaxBookAge,
        }
    }

    #[test]
    fn feature_schema_order_independent() {
        let forward = FeatureSchema::new(
            SchemaVersion::new(1),
            vec![sample_spec("alpha"), sample_spec("beta")],
        )
        .expect("schema");
        let shuffled = FeatureSchema::new(
            SchemaVersion::new(1),
            vec![sample_spec("beta"), sample_spec("alpha")],
        )
        .expect("schema");
        assert_eq!(
            ResearchHasher::feature_schema(&forward).expect("hash"),
            ResearchHasher::feature_schema(&shuffled).expect("hash"),
        );
    }

    #[test]
    fn feature_version_changes_hash() {
        let v1 =
            FeatureSchema::new(SchemaVersion::new(1), vec![sample_spec("alpha")]).expect("schema");
        let v2 =
            FeatureSchema::new(SchemaVersion::new(2), vec![sample_spec("alpha")]).expect("schema");
        assert_ne!(
            ResearchHasher::feature_schema(&v1).expect("hash"),
            ResearchHasher::feature_schema(&v2).expect("hash"),
        );
    }

    #[test]
    fn factor_schema_order_independent() {
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
    fn factor_schema_distinguishes_sets() {
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

    // ── Composite `feature_vector` hash ───────────────────────────────────

    /// A minimal, self-contained two-layer vector for hash unit tests. Every
    /// context/audit field (`market_id`/`token_id`/`as_of`/`staleness_ms`/
    /// `source_refs`/`data_quality`/`substitutions`) is deliberately varied
    /// across the two builders below so tests can prove those fields never
    /// perturb `feature_hash`.
    fn base_vector(
        generic: BTreeMap<FeatureName, FeatureCell>,
        domain: Option<DomainFeatureSlice>,
    ) -> FeatureVector {
        FeatureVector {
            market_id: MarketId::new("m1"),
            token_id: Some(TokenId::new("t1")),
            decision_at: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
            generic_schema_version: SchemaVersion::new(1),
            generic,
            domain,
            data_quality: DataQualityStatus::Fresh,
        }
    }

    fn generic_with(name: &'static str, value: FeatureValue) -> BTreeMap<FeatureName, FeatureCell> {
        let mut map = BTreeMap::new();
        map.insert(
            FeatureName::from_static(name),
            FeatureCell::observed(value, None, FeatureStaleness::Unknown),
        );
        map
    }

    fn crypto_slice(schema_version: SchemaVersion, value: FeatureValue) -> DomainFeatureSlice {
        DomainFeatureSlice {
            family: DomainFamily::Crypto,
            schema_version,
            values: generic_with("domain.crypto.distance_to_strike", value),
        }
    }

    #[test]
    fn feature_vector_hash_input() {
        let a = base_vector(
            generic_with(
                "book.mid",
                FeatureValue::Probability(Probability::new(dec!(0.5))),
            ),
            None,
        );
        let b = base_vector(
            generic_with(
                "book.mid",
                FeatureValue::Probability(Probability::new(dec!(0.5))),
            ),
            None,
        );
        assert_eq!(
            ResearchHasher::feature_vector(&a).expect("hash"),
            ResearchHasher::feature_vector(&b).expect("hash"),
        );
    }

    #[test]
    fn feature_vector_commits_ask() {
        let generic = generic_with(
            "book.mid",
            FeatureValue::Probability(Probability::new(dec!(0.5))),
        );
        let mut first = base_vector(generic, None);
        first.generic.insert(
            FeatureName::from_static("book.secondary_best_ask"),
            FeatureCell::observed(
                FeatureValue::Probability(Probability::new(dec!(0.44))),
                None,
                FeatureStaleness::Unknown,
            ),
        );
        let mut second = first.clone();
        second.generic.insert(
            FeatureName::from_static("book.secondary_best_ask"),
            FeatureCell::observed(
                FeatureValue::Probability(Probability::new(dec!(0.45))),
                None,
                FeatureStaleness::Unknown,
            ),
        );

        assert_ne!(
            ResearchHasher::feature_vector(&first).expect("first hash"),
            ResearchHasher::feature_vector(&second).expect("second hash"),
            "durable parity must observe an executable NO-ask change"
        );
    }

    #[test]
    fn feature_vector_ignores_metadata() {
        // Same generic/domain content; every context/audit field differs.
        let generic = generic_with(
            "book.mid",
            FeatureValue::Probability(Probability::new(dec!(0.5))),
        );
        let a = base_vector(generic.clone(), None);
        let mut b = FeatureVector {
            market_id: MarketId::new("m-different"),
            token_id: None,
            decision_at: Utc.with_ymd_and_hms(2026, 6, 1, 12, 30, 0).unwrap(),
            generic_schema_version: SchemaVersion::new(1),
            generic,
            domain: None,
            data_quality: DataQualityStatus::Degraded,
        };
        b.generic
            .get_mut(&FeatureName::from_static("book.mid"))
            .expect("cell")
            .evidence = Some(EvidenceSourceRef {
            source_kind: EvidenceSourceKind::Book,
            reference: "snapshot-42".to_owned(),
            effective_at: Utc.with_ymd_and_hms(2026, 6, 1, 12, 29, 0).unwrap(),
            available_at: Some(Utc.with_ymd_and_hms(2026, 6, 1, 12, 29, 1).unwrap()),
        });
        b.generic
            .get_mut(&FeatureName::from_static("book.mid"))
            .expect("cell")
            .staleness = FeatureStaleness::Known { age_ms: 987_654 };
        assert_eq!(
            ResearchHasher::feature_vector(&a).expect("hash"),
            ResearchHasher::feature_vector(&b).expect("hash"),
            "context and cell audit metadata must never perturb feature_hash",
        );
    }

    #[test]
    fn feature_vector_distinguishes_present() {
        let generic = generic_with(
            "book.mid",
            FeatureValue::Probability(Probability::new(dec!(0.5))),
        );
        let without_domain = base_vector(generic.clone(), None);
        let with_domain = base_vector(
            generic,
            Some(crypto_slice(
                SchemaVersion::new(1),
                FeatureValue::Decimal(dec!(0.02)),
            )),
        );
        assert_ne!(
            ResearchHasher::feature_vector(&without_domain).expect("hash"),
            ResearchHasher::feature_vector(&with_domain).expect("hash"),
        );
    }

    #[test]
    fn feature_vector_distinguishes_version() {
        // `DomainFamily` has a single live variant today (`Crypto` — see
        // `enums::domain::DomainFamily::ALL`); fabricating a second variant
        // solely to exercise this test would itself be dead semantics.
        // `domain_family` is nonetheless a
        // first-class field of `FeatureHashComposite` (verified by
        // construction / the type checker), so discrimination across
        // families is automatic the moment a second family is registered.
        // `domain_schema_version` is the axis we can exercise today with the
        // one live family.
        let generic = generic_with(
            "book.mid",
            FeatureValue::Probability(Probability::new(dec!(0.5))),
        );
        let v1 = base_vector(
            generic.clone(),
            Some(crypto_slice(
                SchemaVersion::new(1),
                FeatureValue::Decimal(dec!(0.02)),
            )),
        );
        let v2 = base_vector(
            generic,
            Some(crypto_slice(
                SchemaVersion::new(2),
                FeatureValue::Decimal(dec!(0.02)),
            )),
        );
        assert_ne!(
            ResearchHasher::feature_vector(&v1).expect("hash"),
            ResearchHasher::feature_vector(&v2).expect("hash"),
        );
    }

    #[test]
    fn feature_vector_hash_scale() {
        // Differ only past the 12th decimal place (HASH_STAT_SCALE): the
        // quantization pass must collapse both to the same digest.
        let a = base_vector(
            generic_with("book.mid", FeatureValue::Decimal(dec!(0.123456789012340))),
            None,
        );
        let b = base_vector(
            generic_with("book.mid", FeatureValue::Decimal(dec!(0.123456789012341))),
            None,
        );
        assert_eq!(
            ResearchHasher::feature_vector(&a).expect("hash"),
            ResearchHasher::feature_vector(&b).expect("hash"),
            "values differing only past HASH_STAT_SCALE must hash identically",
        );
    }

    #[test]
    fn feature_vector_distinguishes_scale() {
        let a = base_vector(
            generic_with("book.mid", FeatureValue::Decimal(dec!(0.120000000000))),
            None,
        );
        let b = base_vector(
            generic_with("book.mid", FeatureValue::Decimal(dec!(0.130000000000))),
            None,
        );
        assert_ne!(
            ResearchHasher::feature_vector(&a).expect("hash"),
            ResearchHasher::feature_vector(&b).expect("hash"),
        );
    }

    #[test]
    fn feature_vector_never_value() {
        // The hash-only quantization pass must be side-effect-free: the
        // vector's own stored `Decimal` retains full precision.
        let high_precision = dec!(0.123456789012345678);
        let vector = base_vector(
            generic_with("book.mid", FeatureValue::Decimal(high_precision)),
            None,
        );
        let _ = ResearchHasher::feature_vector(&vector).expect("hash");
        assert_eq!(
            vector.generic[&FeatureName::from_static("book.mid")].value(),
            Some(&FeatureValue::Decimal(high_precision)),
        );
    }
}
