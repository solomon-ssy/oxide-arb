//! Canonical governed factor definition and explanation documents.

use std::{cmp::Ordering, collections::HashSet};

use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    enums::{
        factor::{FactorFamily, FactorNormalization},
        quant::FactorDirection,
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, FactorDefinitionId, Probability, SchemaVersion,
        stable_name::{FactorName, FeatureName},
    },
};

impl FactorName {
    /// Whether this value is a canonical lowercase ASCII factor stable name.
    #[must_use]
    pub fn is_canonical(&self) -> bool {
        canonical_factor_name(self.as_str())
    }
}

/// Breaking wire and hash-domain version for a serving factor plane.
pub const FACTOR_SERVING_PLANE_FORMAT_VERSION: u32 = 2;
/// Breaking wire and hash-domain version for one factor-definition revision.
pub const FACTOR_DEFINITION_REVISION_VERSION: u32 = 2;
/// Output schema emitted by the typed alpha/context projection.
pub const FACTOR_VALUE_OUTPUT_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(2);
const FACTOR_SERVING_PLANE_HASH_DOMAIN: &str = "quant-pivot/factor-serving-plane";
const FACTOR_DEFINITION_REVISION_HASH_DOMAIN: &str = "quant-pivot/factor-definition-revision";
const MAX_FACTOR_NAME_BYTES: usize = 256;
const MAX_FACTOR_OWNER_BYTES: usize = 256;
const MAX_FACTOR_FEATURE_NAME_BYTES: usize = 256;
const MAX_FACTOR_SEMANTIC_KEY_BYTES: usize = 4_096;

/// Canonical persisted precision contract for a raw factor value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FactorRawValue(Decimal);

/// A raw factor value cannot be represented by the canonical
/// `numeric(28,12)` persistence contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FactorRawValueError {
    #[error("raw factor value {value} has scale {scale}; maximum is {maximum_scale}")]
    UnsupportedScale {
        value: Decimal,
        scale: u32,
        maximum_scale: u32,
    },
    #[error("raw factor value {value} exceeds numeric({precision},{scale})")]
    OutOfRange {
        value: Decimal,
        precision: u32,
        scale: u32,
    },
}

impl FactorRawValue {
    pub const PRECISION: u32 = 28;
    pub const SCALE: u32 = 12;

    /// Quantize a computed raw value once, before normalization and persistence.
    pub fn quantize(value: Decimal) -> Result<Self, FactorRawValueError> {
        Self::try_from(value.round_dp(Self::SCALE))
    }

    #[must_use]
    pub const fn inner(self) -> Decimal {
        self.0
    }
}

impl TryFrom<Decimal> for FactorRawValue {
    type Error = FactorRawValueError;

    fn try_from(value: Decimal) -> Result<Self, Self::Error> {
        let value = value.normalize();
        if value.scale() > Self::SCALE {
            return Err(FactorRawValueError::UnsupportedScale {
                value,
                scale: value.scale(),
                maximum_scale: Self::SCALE,
            });
        }
        let exclusive_limit = Decimal::from(10_000_000_000_000_000_i64);
        if value.abs() >= exclusive_limit {
            return Err(FactorRawValueError::OutOfRange {
                value,
                precision: Self::PRECISION,
                scale: Self::SCALE,
            });
        }
        Ok(Self(value))
    }
}

/// Monotone effect of a side-neutral context score on opportunity quality.
///
/// This effect may only scale conviction/confidence. It can never select an
/// outcome side or reverse an outcome-alpha contribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorContextEffect {
    HigherIsSupportive,
    LowerIsSupportive,
}

/// Reference side used by a signed outcome alpha.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorAlphaOrientation {
    /// Raw sign supports the token whose book/time-series features were used.
    FeatureToken,
    /// Raw sign supports the market's canonical YES outcome independently of
    /// which outcome token is currently held or scored.
    CanonicalYes,
}

/// Executable output semantics of a governed factor.
///
/// The tagged shape makes the two scoring heads mutually exclusive:
/// `OutcomeAlpha` is signed and token-oriented, whereas `Context` is an
/// unsigned magnitude that can only scale opportunity quality. `Diagnostic`
/// remains observable and lineage-bound but is forbidden from an estimator
/// until a monotone business projection is explicitly governed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "output_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FactorOutputSemantics {
    OutcomeAlpha { orientation: FactorAlphaOrientation },
    Context { effect: FactorContextEffect },
    Diagnostic,
}

/// Frozen raw-computation and normalization implementation semantics.
///
/// Runtime configuration values remain owned by the immutable scoring profile;
/// this contract names every parameter and algorithm whose implementation can
/// change the meaning of an otherwise-identical factor document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactorComputationContract {
    pub semantic_version: u32,
    pub semantic_key: String,
}

/// Immutable factor-definition document persisted as one JSONB value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct FactorDefinitionDocument {
    pub name: FactorName,
    pub family: FactorFamily,
    pub input_features: Vec<FeatureName>,
    pub output: FactorOutputSemantics,
    pub normalization: FactorNormalization,
    pub owner: String,
    /// Whether missing/indeterminate output rejects the market at the factor
    /// eligibility boundary. Confidence floors remain exclusively owned by
    /// the immutable scoring profile.
    pub required: bool,
    pub computation: FactorComputationContract,
}

/// Exact immutable factor revision available to one serving plane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "FactorDefinitionRefDocument")]
pub struct FactorDefinitionRef {
    revision_version: u32,
    factor_definition_id: FactorDefinitionId,
    definition_hash: ContentHash,
    feature_contract_hash: ContentHash,
    input_schema_version: SchemaVersion,
    output_schema_version: SchemaVersion,
    definition: FactorDefinitionDocument,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FactorDefinitionRefDocument {
    revision_version: u32,
    factor_definition_id: FactorDefinitionId,
    definition_hash: ContentHash,
    feature_contract_hash: ContentHash,
    input_schema_version: SchemaVersion,
    output_schema_version: SchemaVersion,
    definition: FactorDefinitionDocument,
}

/// Stable validation failures for one content-addressed factor revision.
#[derive(Debug, Error)]
pub enum FactorDefinitionRevisionError {
    #[error("unsupported factor-definition revision version {actual}; expected {expected}")]
    UnsupportedVersion { expected: u32, actual: u32 },
    #[error("factor name `{name}` is not a canonical lowercase ASCII stable name")]
    InvalidFactorName { name: String },
    #[error("factor definition `{name}` owner must be trimmed and non-empty")]
    InvalidOwner { name: String },
    #[error("factor definition `{name}` has an unsupported computation semantic version")]
    InvalidSemanticVersion { name: String },
    #[error("factor definition `{name}` has an invalid computation semantic key")]
    InvalidSemanticKey { name: String },
    #[error("factor definition `{name}` repeats input feature `{feature}`")]
    DuplicateInputFeature { name: String, feature: String },
    #[error("factor definition `{name}` input features are not in canonical stable-name order")]
    NonCanonicalInputFeatures { name: String },
    #[error("factor definition `{name}` has invalid input feature `{feature}`")]
    InvalidInputFeature { name: String, feature: String },
    #[error("factor `{name}` has invalid {binding} schema version {actual}")]
    InvalidSchemaVersion {
        name: String,
        binding: &'static str,
        actual: i32,
    },
    #[error("factor definition hash mismatch: expected {expected}, got {actual}")]
    DefinitionHashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
    #[error("factor {factor_definition_id} does not derive from its definition hash")]
    FactorIdentityMismatch {
        factor_definition_id: FactorDefinitionId,
    },
    #[error(transparent)]
    Hash(#[from] CanonicalDigestError),
}

/// Stable validation failures for a sealed factor serving plane.
#[derive(Debug, Error)]
pub enum FactorServingPlaneError {
    #[error("unsupported factor-serving plane version {actual}; expected {expected}")]
    UnsupportedVersion { expected: u32, actual: u32 },
    #[error("duplicate factor name `{name}`")]
    DuplicateFactorName { name: String },
    #[error("factor plane must be strictly ordered by unique stable name")]
    NonCanonicalOrder,
    #[error("duplicate factor definition id {factor_definition_id}")]
    DuplicateFactorId {
        factor_definition_id: FactorDefinitionId,
    },
    #[error("duplicate factor definition hash {definition_hash}")]
    DuplicateFactorHash { definition_hash: ContentHash },
    #[error("factor-serving plane hash mismatch: expected {expected}, got {actual}")]
    SchemaHashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
    #[error(transparent)]
    Revision(#[from] FactorDefinitionRevisionError),
    #[error(transparent)]
    Hash(#[from] CanonicalDigestError),
}

impl FactorDefinitionRef {
    /// Seal one complete factor revision and derive its content-addressed ID.
    pub fn try_seal(
        mut definition: FactorDefinitionDocument,
        feature_contract_hash: ContentHash,
        input_schema_version: SchemaVersion,
        output_schema_version: SchemaVersion,
    ) -> Result<Self, FactorDefinitionRevisionError> {
        definition.input_features.sort_unstable();
        Self::validate_definition(&definition)?;
        Self::validate_schema(
            &definition.name,
            input_schema_version,
            output_schema_version,
        )?;
        let definition_hash = Self::hash_revision(
            &definition,
            feature_contract_hash,
            input_schema_version,
            output_schema_version,
        )?;
        Ok(Self {
            revision_version: FACTOR_DEFINITION_REVISION_VERSION,
            factor_definition_id: FactorDefinitionId::from_definition_hash(&definition_hash),
            definition_hash,
            feature_contract_hash,
            input_schema_version,
            output_schema_version,
            definition,
        })
    }

    #[must_use]
    pub const fn revision_version(&self) -> u32 {
        self.revision_version
    }

    #[must_use]
    pub const fn factor_definition_id(&self) -> FactorDefinitionId {
        self.factor_definition_id
    }

    #[must_use]
    pub const fn definition_hash(&self) -> ContentHash {
        self.definition_hash
    }

    #[must_use]
    pub const fn feature_contract_hash(&self) -> ContentHash {
        self.feature_contract_hash
    }

    #[must_use]
    pub const fn input_schema_version(&self) -> SchemaVersion {
        self.input_schema_version
    }

    #[must_use]
    pub const fn output_schema_version(&self) -> SchemaVersion {
        self.output_schema_version
    }

    #[must_use]
    pub const fn definition(&self) -> &FactorDefinitionDocument {
        &self.definition
    }

    #[must_use]
    pub const fn factor_name(&self) -> &FactorName {
        &self.definition.name
    }

    /// Recompute the full revision preimage, stored hash, and derived ID.
    pub fn validate(&self) -> Result<(), FactorDefinitionRevisionError> {
        if self.revision_version != FACTOR_DEFINITION_REVISION_VERSION {
            return Err(FactorDefinitionRevisionError::UnsupportedVersion {
                expected: FACTOR_DEFINITION_REVISION_VERSION,
                actual: self.revision_version,
            });
        }
        Self::validate_definition(&self.definition)?;
        Self::validate_schema(
            &self.definition.name,
            self.input_schema_version,
            self.output_schema_version,
        )?;
        let expected = Self::hash_revision(
            &self.definition,
            self.feature_contract_hash,
            self.input_schema_version,
            self.output_schema_version,
        )?;
        if expected != self.definition_hash {
            return Err(FactorDefinitionRevisionError::DefinitionHashMismatch {
                expected,
                actual: self.definition_hash,
            });
        }
        if FactorDefinitionId::from_definition_hash(&self.definition_hash)
            != self.factor_definition_id
        {
            return Err(FactorDefinitionRevisionError::FactorIdentityMismatch {
                factor_definition_id: self.factor_definition_id,
            });
        }
        Ok(())
    }

    fn validate_definition(
        definition: &FactorDefinitionDocument,
    ) -> Result<(), FactorDefinitionRevisionError> {
        let name = definition.name.as_str();
        if !definition.name.is_canonical() {
            return Err(FactorDefinitionRevisionError::InvalidFactorName {
                name: name.to_owned(),
            });
        }
        if definition.owner.is_empty()
            || definition.owner.len() > MAX_FACTOR_OWNER_BYTES
            || definition.owner.trim() != definition.owner
        {
            return Err(FactorDefinitionRevisionError::InvalidOwner {
                name: name.to_owned(),
            });
        }
        if definition.computation.semantic_version == 0 {
            return Err(FactorDefinitionRevisionError::InvalidSemanticVersion {
                name: name.to_owned(),
            });
        }
        let semantic_key = definition.computation.semantic_key.as_str();
        if semantic_key.is_empty()
            || semantic_key.len() > MAX_FACTOR_SEMANTIC_KEY_BYTES
            || semantic_key.trim() != semantic_key
            || !semantic_key
                .bytes()
                .all(|byte| byte.is_ascii_graphic() && byte != b'"' && byte != b'\\')
        {
            return Err(FactorDefinitionRevisionError::InvalidSemanticKey {
                name: name.to_owned(),
            });
        }
        let mut input_features = HashSet::new();
        for feature in &definition.input_features {
            if feature.as_str().len() > MAX_FACTOR_FEATURE_NAME_BYTES
                || !canonical_stable_name(feature.as_str())
            {
                return Err(FactorDefinitionRevisionError::InvalidInputFeature {
                    name: name.to_owned(),
                    feature: feature.to_string(),
                });
            }
            if !input_features.insert(feature) {
                return Err(FactorDefinitionRevisionError::DuplicateInputFeature {
                    name: name.to_owned(),
                    feature: feature.to_string(),
                });
            }
        }
        if definition
            .input_features
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(FactorDefinitionRevisionError::NonCanonicalInputFeatures {
                name: name.to_owned(),
            });
        }
        Ok(())
    }

    fn validate_schema(
        name: &FactorName,
        input_schema_version: SchemaVersion,
        output_schema_version: SchemaVersion,
    ) -> Result<(), FactorDefinitionRevisionError> {
        for (binding, version) in [
            ("input", input_schema_version),
            ("output", output_schema_version),
        ] {
            if version.get() < 1 {
                return Err(FactorDefinitionRevisionError::InvalidSchemaVersion {
                    name: name.to_string(),
                    binding,
                    actual: version.get(),
                });
            }
        }
        Ok(())
    }

    fn hash_revision(
        definition: &FactorDefinitionDocument,
        feature_contract_hash: ContentHash,
        input_schema_version: SchemaVersion,
        output_schema_version: SchemaVersion,
    ) -> Result<ContentHash, CanonicalDigestError> {
        #[derive(Serialize)]
        struct RevisionPreimage<'a> {
            definition: &'a FactorDefinitionDocument,
            feature_contract_hash: ContentHash,
            input_schema_version: SchemaVersion,
            output_schema_version: SchemaVersion,
        }

        CanonicalDigest::content_hash_typed(
            FACTOR_DEFINITION_REVISION_HASH_DOMAIN,
            FACTOR_DEFINITION_REVISION_VERSION,
            &RevisionPreimage {
                definition,
                feature_contract_hash,
                input_schema_version,
                output_schema_version,
            },
        )
    }
}

impl TryFrom<FactorDefinitionRefDocument> for FactorDefinitionRef {
    type Error = FactorDefinitionRevisionError;

    fn try_from(document: FactorDefinitionRefDocument) -> Result<Self, Self::Error> {
        let revision = Self {
            revision_version: document.revision_version,
            factor_definition_id: document.factor_definition_id,
            definition_hash: document.definition_hash,
            feature_contract_hash: document.feature_contract_hash,
            input_schema_version: document.input_schema_version,
            output_schema_version: document.output_schema_version,
            definition: document.definition,
        };
        revision.validate()?;
        Ok(revision)
    }
}

/// Canonically ordered, content-addressed factor revisions for one model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields, try_from = "FactorServingPlaneDocument")]
pub struct FactorServingPlane {
    format_version: u32,
    factor_schema_hash: ContentHash,
    definitions: Vec<FactorDefinitionRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FactorServingPlaneDocument {
    format_version: u32,
    factor_schema_hash: ContentHash,
    definitions: Vec<FactorDefinitionRef>,
}

impl FactorServingPlane {
    /// Seal the canonical factor-free plane used by classical estimators.
    pub fn try_empty() -> Result<Self, FactorServingPlaneError> {
        Self::try_seal(Vec::new())
    }

    /// Canonicalize, validate, and seal a complete factor revision set.
    pub fn try_seal(
        mut definitions: Vec<FactorDefinitionRef>,
    ) -> Result<Self, FactorServingPlaneError> {
        definitions.sort_by(|left, right| left.factor_name().cmp(right.factor_name()));
        Self::validate_definitions(&definitions)?;
        let factor_schema_hash = Self::hash_definitions(&definitions)?;
        Ok(Self {
            format_version: FACTOR_SERVING_PLANE_FORMAT_VERSION,
            factor_schema_hash,
            definitions,
        })
    }

    #[must_use]
    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    #[must_use]
    pub const fn factor_schema_hash(&self) -> ContentHash {
        self.factor_schema_hash
    }

    #[must_use]
    pub fn definitions(&self) -> &[FactorDefinitionRef] {
        &self.definitions
    }

    /// Revalidate the canonical order, revision identities, and stored hash.
    pub fn validate(&self) -> Result<(), FactorServingPlaneError> {
        if self.format_version != FACTOR_SERVING_PLANE_FORMAT_VERSION {
            return Err(FactorServingPlaneError::UnsupportedVersion {
                expected: FACTOR_SERVING_PLANE_FORMAT_VERSION,
                actual: self.format_version,
            });
        }
        Self::validate_definitions(&self.definitions)?;
        let expected = Self::hash_definitions(&self.definitions)?;
        if expected != self.factor_schema_hash {
            return Err(FactorServingPlaneError::SchemaHashMismatch {
                expected,
                actual: self.factor_schema_hash,
            });
        }
        Ok(())
    }

    fn validate_definitions(
        definitions: &[FactorDefinitionRef],
    ) -> Result<(), FactorServingPlaneError> {
        for pair in definitions.windows(2) {
            if pair[0].factor_name() == pair[1].factor_name() {
                return Err(FactorServingPlaneError::DuplicateFactorName {
                    name: pair[0].factor_name().to_string(),
                });
            }
            if pair[0].factor_name() > pair[1].factor_name() {
                return Err(FactorServingPlaneError::NonCanonicalOrder);
            }
        }
        let mut ids = HashSet::new();
        let mut hashes = HashSet::new();
        for definition in definitions {
            definition.validate()?;
            if !hashes.insert(definition.definition_hash()) {
                return Err(FactorServingPlaneError::DuplicateFactorHash {
                    definition_hash: definition.definition_hash(),
                });
            }
            if !ids.insert(definition.factor_definition_id()) {
                return Err(FactorServingPlaneError::DuplicateFactorId {
                    factor_definition_id: definition.factor_definition_id(),
                });
            }
        }
        Ok(())
    }

    fn hash_definitions(
        definitions: &[FactorDefinitionRef],
    ) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_typed(
            FACTOR_SERVING_PLANE_HASH_DOMAIN,
            FACTOR_SERVING_PLANE_FORMAT_VERSION,
            definitions,
        )
    }
}

impl TryFrom<FactorServingPlaneDocument> for FactorServingPlane {
    type Error = FactorServingPlaneError;

    fn try_from(document: FactorServingPlaneDocument) -> Result<Self, Self::Error> {
        let plane = Self {
            format_version: document.format_version,
            factor_schema_hash: document.factor_schema_hash,
            definitions: document.definitions,
        };
        plane.validate()?;
        Ok(plane)
    }
}

fn canonical_factor_name(name: &str) -> bool {
    if name.is_empty() || name.len() > MAX_FACTOR_NAME_BYTES {
        return false;
    }
    canonical_stable_name(name)
}

fn canonical_stable_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    if !bytes.next().is_some_and(|byte| byte.is_ascii_lowercase()) {
        return false;
    }
    let mut segment_start = false;
    for byte in bytes {
        if byte == b'.' || byte == b'_' {
            if segment_start {
                return false;
            }
            segment_start = true;
        } else if byte.is_ascii_lowercase() || byte.is_ascii_digit() {
            segment_start = false;
        } else {
            return false;
        }
    }
    !segment_start
}

impl FactorDefinitionDocument {
    /// Project a raw computation onto the non-negative normalization domain.
    ///
    /// Outcome alpha carries an economically meaningful token-oriented sign,
    /// so only its absolute strength is normalized. Context is an unsigned
    /// magnitude and therefore rejects negative raw values instead of silently
    /// changing their meaning.
    #[must_use]
    pub fn normalization_input(&self, raw: Decimal) -> Option<Decimal> {
        match self.output {
            FactorOutputSemantics::OutcomeAlpha { .. } => Some(raw.abs()),
            FactorOutputSemantics::Context { .. } | FactorOutputSemantics::Diagnostic
                if raw >= Decimal::ZERO =>
            {
                Some(raw)
            }
            FactorOutputSemantics::Context { .. } | FactorOutputSemantics::Diagnostic => None,
        }
    }

    /// Resolve the token-oriented direction of one raw alpha output.
    ///
    /// Context values always return [`FactorDirection::Neutral`]; consumers
    /// must project them through [`Self::context_adequacy`] instead of a signed
    /// net.
    #[must_use]
    pub fn contribution_direction(&self, raw: Decimal) -> Option<FactorDirection> {
        match self.output {
            FactorOutputSemantics::OutcomeAlpha { .. } => Some(match raw.cmp(&Decimal::ZERO) {
                Ordering::Greater => FactorDirection::Positive,
                Ordering::Less => FactorDirection::Negative,
                Ordering::Equal => FactorDirection::Neutral,
            }),
            FactorOutputSemantics::Context { .. } | FactorOutputSemantics::Diagnostic
                if raw >= Decimal::ZERO =>
            {
                Some(FactorDirection::Neutral)
            }
            FactorOutputSemantics::Context { .. } | FactorOutputSemantics::Diagnostic => None,
        }
    }

    /// Convert a normalized context magnitude into opportunity adequacy.
    ///
    /// The result remains in `[0, 1]` and has no outcome-side orientation.
    #[must_use]
    pub fn context_adequacy(&self, normalized: Probability) -> Option<Probability> {
        match self.output {
            FactorOutputSemantics::Context {
                effect: FactorContextEffect::HigherIsSupportive,
            } => Some(normalized),
            FactorOutputSemantics::Context {
                effect: FactorContextEffect::LowerIsSupportive,
            } => Some(Probability::new(Decimal::ONE - normalized.inner())),
            FactorOutputSemantics::OutcomeAlpha { .. } | FactorOutputSemantics::Diagnostic => None,
        }
    }

    #[must_use]
    pub const fn is_outcome_alpha(&self) -> bool {
        matches!(self.output, FactorOutputSemantics::OutcomeAlpha { .. })
    }

    #[must_use]
    pub const fn is_context(&self) -> bool {
        matches!(self.output, FactorOutputSemantics::Context { .. })
    }

    #[must_use]
    pub const fn is_diagnostic(&self) -> bool {
        matches!(self.output, FactorOutputSemantics::Diagnostic)
    }

    #[must_use]
    pub const fn alpha_orientation(&self) -> Option<FactorAlphaOrientation> {
        match self.output {
            FactorOutputSemantics::OutcomeAlpha { orientation } => Some(orientation),
            FactorOutputSemantics::Context { .. } | FactorOutputSemantics::Diagnostic => None,
        }
    }

    #[must_use]
    pub const fn is_required(&self) -> bool {
        self.required
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
    use rust_decimal_macros::dec;
    use serde_json::{Value, from_value, to_value};

    use super::{
        FACTOR_DEFINITION_REVISION_VERSION, FactorComputationContract, FactorContextEffect,
        FactorDefinitionDocument, FactorDefinitionRef, FactorDefinitionRevisionError,
        FactorExplanation, FactorOutputSemantics, FactorRawValue, FactorRawValueError,
        FactorServingPlane, FactorServingPlaneError,
    };
    use crate::{
        enums::factor::{FactorFamily, FactorNormalization},
        types::{
            ContentHash, FactorDefinitionId, SchemaVersion,
            stable_name::{FactorName, FeatureName},
        },
    };

    fn hash(byte: u8) -> ContentHash {
        ContentHash::from_bytes([byte; 32])
    }

    fn definition(name: &str) -> FactorDefinitionDocument {
        FactorDefinitionDocument {
            name: FactorName::new(name),
            family: FactorFamily::Momentum,
            input_features: vec![FeatureName::new("ts.momentum_roc_900s")],
            output: FactorOutputSemantics::Context {
                effect: FactorContextEffect::HigherIsSupportive,
            },
            normalization: FactorNormalization::Rank,
            owner: "research".to_owned(),
            required: true,
            computation: FactorComputationContract {
                semantic_version: 1,
                semantic_key: "quant-pivot/raw-primary-roc-window-feature-scalar@1+quant-pivot/data-quality-confidence@1+quant-pivot/factor-normalization-boundary@1".to_owned(),
            },
        }
    }

    fn factor(name: &str) -> FactorDefinitionRef {
        FactorDefinitionRef::try_seal(
            definition(name),
            hash(1),
            SchemaVersion::FIRST,
            SchemaVersion::FIRST,
        )
        .expect("valid factor revision")
    }

    #[test]
    fn raw_value_precision_contract() {
        assert_eq!(
            FactorRawValue::quantize(dec!(0.0344827586206896551724137931))
                .expect("computed raw value quantizes")
                .inner(),
            dec!(0.034482758621)
        );
        assert!(matches!(
            FactorRawValue::try_from(dec!(0.0344827586206)),
            Err(FactorRawValueError::UnsupportedScale { .. })
        ));
        assert!(matches!(
            FactorRawValue::quantize(dec!(10000000000000000)),
            Err(FactorRawValueError::OutOfRange { .. })
        ));
    }

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
            "output": {
                "output_kind": "context",
                "effect": "higher_is_supportive"
            },
            "normalization": "rank",
            "owner": "research",
            "required": true,
            "computation": {
                "semantic_version": 1,
                "semantic_key": "quant-pivot/raw-feature-scalar-identity@1"
            },
            "unknown": true
        });
        assert!(serde_json::from_value::<FactorDefinitionDocument>(definition).is_err());
    }

    #[test]
    fn revision_hash_binds_preimage() {
        let base = factor("momentum");
        assert_eq!(base, factor("momentum"));
        assert_eq!(
            base.factor_definition_id(),
            FactorDefinitionId::from_definition_hash(&base.definition_hash())
        );
        assert_eq!(base.revision_version(), FACTOR_DEFINITION_REVISION_VERSION);

        let mut semantic_version = definition("momentum");
        semantic_version.computation.semantic_version = 2;
        let mut semantic_key = definition("momentum");
        semantic_key
            .computation
            .semantic_key
            .push_str("+revision=2");
        let cases = [
            FactorDefinitionRef::try_seal(
                semantic_version,
                hash(1),
                SchemaVersion::FIRST,
                SchemaVersion::FIRST,
            )
            .expect("semantic version revision"),
            FactorDefinitionRef::try_seal(
                semantic_key,
                hash(1),
                SchemaVersion::FIRST,
                SchemaVersion::FIRST,
            )
            .expect("semantic key revision"),
            FactorDefinitionRef::try_seal(
                definition("momentum"),
                hash(2),
                SchemaVersion::FIRST,
                SchemaVersion::FIRST,
            )
            .expect("feature contract revision"),
            FactorDefinitionRef::try_seal(
                definition("momentum"),
                hash(1),
                SchemaVersion::new(2),
                SchemaVersion::FIRST,
            )
            .expect("input schema revision"),
            FactorDefinitionRef::try_seal(
                definition("momentum"),
                hash(1),
                SchemaVersion::FIRST,
                SchemaVersion::new(2),
            )
            .expect("output schema revision"),
        ];
        for revision in cases {
            assert_ne!(revision.definition_hash(), base.definition_hash());
            assert_ne!(revision.factor_definition_id(), base.factor_definition_id());
        }
    }

    #[test]
    fn revision_preimage_is_canonical() {
        let mut first = definition("momentum");
        first.input_features = vec![
            FeatureName::new("ts.realized_vol_900s"),
            FeatureName::new("ts.momentum_roc_900s"),
        ];
        let mut equivalent = first.clone();
        equivalent.input_features.reverse();

        let first = FactorDefinitionRef::try_seal(
            first,
            hash(1),
            SchemaVersion::FIRST,
            SchemaVersion::FIRST,
        )
        .expect("canonical first revision");
        let equivalent = FactorDefinitionRef::try_seal(
            equivalent,
            hash(1),
            SchemaVersion::FIRST,
            SchemaVersion::FIRST,
        )
        .expect("canonical equivalent revision");
        assert_eq!(first, equivalent);
        assert_eq!(
            first
                .definition()
                .input_features
                .iter()
                .map(FeatureName::as_str)
                .collect::<Vec<_>>(),
            vec!["ts.momentum_roc_900s", "ts.realized_vol_900s"]
        );
        let mut noncanonical_inputs = first.definition().clone();
        noncanonical_inputs.input_features.reverse();
        assert!(matches!(
            FactorDefinitionRef::validate_definition(&noncanonical_inputs),
            Err(FactorDefinitionRevisionError::NonCanonicalInputFeatures { .. })
        ));
    }

    #[test]
    fn serving_plane_is_canonical() {
        let forward =
            FactorServingPlane::try_seal(vec![factor("momentum"), factor("liquidity_depth")])
                .expect("sealed forward plane");
        let reverse =
            FactorServingPlane::try_seal(vec![factor("liquidity_depth"), factor("momentum")])
                .expect("sealed reverse plane");

        assert_eq!(forward, reverse);
        assert_eq!(
            forward
                .definitions()
                .iter()
                .map(|definition| definition.factor_name().as_str())
                .collect::<Vec<_>>(),
            vec!["liquidity_depth", "momentum"]
        );
        let restored = from_value::<FactorServingPlane>(
            to_value(&forward).expect("serialize factor serving plane"),
        )
        .expect("deserialize factor serving plane");
        assert_eq!(restored, forward);

        let empty = FactorServingPlane::try_empty().expect("canonical empty plane");
        let repeated_empty = FactorServingPlane::try_empty().expect("repeated empty plane");
        assert_eq!(empty, repeated_empty);
        assert!(empty.definitions().is_empty());
    }

    #[test]
    fn serving_plane_rejects_tamper() {
        let plane =
            FactorServingPlane::try_seal(vec![factor("liquidity_depth"), factor("momentum")])
                .expect("sealed factor serving plane");

        let mut reordered = to_value(&plane).expect("serialize factor serving plane");
        reordered["definitions"]
            .as_array_mut()
            .expect("factor definitions")
            .swap(0, 1);
        assert!(from_value::<FactorServingPlane>(reordered).is_err());

        let mut changed_hash = to_value(&plane).expect("serialize factor serving plane");
        changed_hash["factor_schema_hash"] = Value::String(hash(4).to_string());
        assert!(from_value::<FactorServingPlane>(changed_hash).is_err());

        for version in [0_u64, 3] {
            let mut versioned = to_value(&plane).expect("serialize factor serving plane");
            versioned["format_version"] = Value::Number(version.into());
            assert!(from_value::<FactorServingPlane>(versioned).is_err());
        }

        let mut unknown = to_value(&plane).expect("serialize factor serving plane");
        unknown["legacy_schema"] = Value::Bool(true);
        assert!(from_value::<FactorServingPlane>(unknown).is_err());

        let mut unknown_ref = to_value(&plane).expect("serialize factor serving plane");
        unknown_ref["definitions"][0]["legacy_revision"] = Value::Bool(true);
        assert!(from_value::<FactorServingPlane>(unknown_ref).is_err());
    }

    #[test]
    fn revision_rejects_tamper() {
        let revision = factor("momentum");
        let mut changed_definition = to_value(&revision).expect("serialize factor revision");
        changed_definition["definition"]["computation"]["semantic_key"] =
            Value::String("quant-pivot/raw-primary-roc-window-feature-scalar@2".to_owned());
        assert!(from_value::<FactorDefinitionRef>(changed_definition).is_err());

        let mut changed_feature = to_value(&revision).expect("serialize factor revision");
        changed_feature["feature_contract_hash"] = Value::String(hash(3).to_string());
        assert!(from_value::<FactorDefinitionRef>(changed_feature).is_err());

        let mut changed_id = to_value(&revision).expect("serialize factor revision");
        changed_id["factor_definition_id"] =
            Value::String(FactorDefinitionId::from_v7().to_string());
        assert!(from_value::<FactorDefinitionRef>(changed_id).is_err());

        for version in [0_u64, 3] {
            let mut versioned = to_value(&revision).expect("serialize factor revision");
            versioned["revision_version"] = Value::Number(version.into());
            assert!(from_value::<FactorDefinitionRef>(versioned).is_err());
        }

        let mut unknown = to_value(&revision).expect("serialize factor revision");
        unknown["legacy_hash"] = Value::Bool(true);
        assert!(from_value::<FactorDefinitionRef>(unknown).is_err());

        let mut unknown_definition = to_value(&revision).expect("serialize factor revision");
        unknown_definition["definition"]["legacy_formula"] = Value::Bool(true);
        assert!(from_value::<FactorDefinitionRef>(unknown_definition).is_err());
    }

    #[test]
    fn invalid_revisions_fail_closed() {
        for name in [
            "",
            "Momentum",
            "momentum\n",
            ".momentum",
            "momentum.",
            "two..parts",
        ] {
            assert!(matches!(
                FactorDefinitionRef::try_seal(
                    definition(name),
                    hash(1),
                    SchemaVersion::FIRST,
                    SchemaVersion::FIRST,
                ),
                Err(FactorDefinitionRevisionError::InvalidFactorName { .. })
            ));
        }

        let mut invalid_owner = definition("momentum");
        invalid_owner.owner = " research".to_owned();
        assert!(matches!(
            FactorDefinitionRef::try_seal(
                invalid_owner,
                hash(1),
                SchemaVersion::FIRST,
                SchemaVersion::FIRST,
            ),
            Err(FactorDefinitionRevisionError::InvalidOwner { .. })
        ));

        let mut invalid_semantic_version = definition("momentum");
        invalid_semantic_version.computation.semantic_version = 0;
        assert!(matches!(
            FactorDefinitionRef::try_seal(
                invalid_semantic_version,
                hash(1),
                SchemaVersion::FIRST,
                SchemaVersion::FIRST,
            ),
            Err(FactorDefinitionRevisionError::InvalidSemanticVersion { .. })
        ));

        let mut invalid_semantic_key = definition("momentum");
        invalid_semantic_key.computation.semantic_key = "contains whitespace".to_owned();
        assert!(matches!(
            FactorDefinitionRef::try_seal(
                invalid_semantic_key,
                hash(1),
                SchemaVersion::FIRST,
                SchemaVersion::FIRST,
            ),
            Err(FactorDefinitionRevisionError::InvalidSemanticKey { .. })
        ));

        let mut duplicate_input = definition("momentum");
        duplicate_input
            .input_features
            .push(FeatureName::new("ts.momentum_roc_900s"));
        assert!(matches!(
            FactorDefinitionRef::try_seal(
                duplicate_input,
                hash(1),
                SchemaVersion::FIRST,
                SchemaVersion::FIRST,
            ),
            Err(FactorDefinitionRevisionError::DuplicateInputFeature { .. })
        ));

        let mut invalid_input = definition("momentum");
        invalid_input.input_features = vec![FeatureName::new("Invalid Feature")];
        assert!(matches!(
            FactorDefinitionRef::try_seal(
                invalid_input,
                hash(1),
                SchemaVersion::FIRST,
                SchemaVersion::FIRST,
            ),
            Err(FactorDefinitionRevisionError::InvalidInputFeature { .. })
        ));

        let mut oversized_input = definition("momentum");
        oversized_input.input_features = vec![FeatureName::new(format!("f.{}", "a".repeat(255)))];
        assert!(matches!(
            FactorDefinitionRef::try_seal(
                oversized_input,
                hash(1),
                SchemaVersion::FIRST,
                SchemaVersion::FIRST,
            ),
            Err(FactorDefinitionRevisionError::InvalidInputFeature { .. })
        ));

        for (input, output) in [
            (SchemaVersion::new(0), SchemaVersion::FIRST),
            (SchemaVersion::FIRST, SchemaVersion::new(0)),
        ] {
            assert!(matches!(
                FactorDefinitionRef::try_seal(definition("momentum"), hash(1), input, output),
                Err(FactorDefinitionRevisionError::InvalidSchemaVersion { .. })
            ));
        }
    }

    #[test]
    fn serving_plane_rejects_duplicates() {
        let duplicate_name = vec![factor("momentum"), factor("momentum")];
        assert!(matches!(
            FactorServingPlane::try_seal(duplicate_name),
            Err(FactorServingPlaneError::DuplicateFactorName { .. })
        ));
    }
}
