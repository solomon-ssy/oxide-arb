//! Immutable, content-addressed model-serving contract.

use std::collections::HashSet;

use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    enums::{
        common::MarketCategory,
        domain::DomainFamily,
        factor::FactorFamily,
        model::{ClassicalKind, ModelFamily},
        quant::{CalibrationKind, DatasetPurpose, ModelSerializationFormat},
        runtime_config::ProfileArtifactKind,
    },
    hashing::CanonicalDigest,
    runtime_config::{ImmutableProfileArtifactReferences, PolicyProfileArtifactReference},
    types::{
        CalibrationArtifactId, CapabilityRegistryHashes, ContentHash, DatasetManifest,
        DecisionPolicySnapshotId, FactorDefinitionId, ModelSpecId, ModelVersionId,
        ProfileArtifactId, ResearchProfileRef, TradePolicyArtifactId,
        factor::{FactorServingPlane, FactorServingPlaneError},
    },
};

/// Breaking wire and hash-domain version for the serving contract.
pub const MODEL_SERVING_CONTRACT_VERSION: u32 = 3;
const MODEL_SERVING_CONTRACT_HASH_DOMAIN: &str = "quant-pivot/model-serving-contract";
const MODEL_INTRINSIC_INPUT_HASH_DOMAIN: &str = "quant-pivot/model-intrinsic-input";
const MODEL_INTRINSIC_INPUT_VERSION: u32 = 2;
const FAVORITE_LONGSHOT_FACTOR_NAME: &str = "struct.favorite_longshot";

/// The four position-state inputs owned by the Sell estimator itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelServingIntrinsicInputKind {
    PositionTakeProfitPressure,
    PositionStopLossPressure,
    PositionTimeInTrade,
    PositionPeakDrawdown,
}

impl ModelServingIntrinsicInputKind {
    const SELL_KINDS: [Self; 4] = [
        Self::PositionTakeProfitPressure,
        Self::PositionStopLossPressure,
        Self::PositionTimeInTrade,
        Self::PositionPeakDrawdown,
    ];

    /// Stable name shared with the Sell scorer's weight vector.
    #[must_use]
    pub const fn stable_name(self) -> &'static str {
        match self {
            Self::PositionTakeProfitPressure => "position_take_profit_pressure",
            Self::PositionStopLossPressure => "position_stop_loss_pressure",
            Self::PositionTimeInTrade => "position_time_in_trade",
            Self::PositionPeakDrawdown => "position_peak_drawdown",
        }
    }

    /// Closed formula, missingness, normalization, and rounding semantics.
    ///
    /// Any change requires a new intrinsic-input and serving-contract version.
    #[must_use]
    pub const fn semantic_key(self) -> &'static str {
        match self {
            Self::PositionTakeProfitPressure => {
                "source=lot_state;preconditions=avg_price>0,max_hold_secs>0,opened_at<=decision_at,mark_if_present>0,peak_mark_if_present>0;raw=mark.map(max((mark-avg_price)/avg_price,0));missing=mark_none->none;arithmetic=checked_sub+checked_div,error_on_overflow;score=raw/0.2;clamp=0..1;round_dp=12"
            }
            Self::PositionStopLossPressure => {
                "source=lot_state;preconditions=avg_price>0,max_hold_secs>0,opened_at<=decision_at,mark_if_present>0,peak_mark_if_present>0;raw=mark.map(max((avg_price-mark)/avg_price,0));missing=mark_none->none;arithmetic=checked_sub+checked_div,error_on_overflow;score=raw/0.2;clamp=0..1;round_dp=12"
            }
            Self::PositionTimeInTrade => {
                "source=lot_state;preconditions=avg_price>0,max_hold_secs>0,opened_at<=decision_at,mark_if_present>0,peak_mark_if_present>0;raw=(decision_at-opened_at).whole_seconds/max_hold_secs;missing=never;arithmetic=checked_div,error_on_overflow;clamp=0..1;score=raw;clamp_signed=-1..1;round_dp=12"
            }
            Self::PositionPeakDrawdown => {
                "source=lot_state;preconditions=avg_price>0,max_hold_secs>0,opened_at<=decision_at,mark_if_present>0,peak_mark_if_present>0;raw=zip(peak_mark,mark).map((peak-mark)/peak);missing=mark_none_or_peak_none->none;arithmetic=checked_sub+checked_div,error_on_overflow;mark_above_peak=clamp_to_0;clamp=0..1;clamp_signed=-1..1;round_dp=12"
            }
        }
    }
}

/// Content-addressed semantic definition of one model-intrinsic input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ModelServingIntrinsicInputDocument")]
pub struct ModelServingIntrinsicInputRef {
    kind: ModelServingIntrinsicInputKind,
    semantic_version: u32,
    definition_hash: ContentHash,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelServingIntrinsicInputDocument {
    kind: ModelServingIntrinsicInputKind,
    semantic_version: u32,
    definition_hash: ContentHash,
}

impl ModelServingIntrinsicInputRef {
    #[must_use]
    pub const fn kind(&self) -> ModelServingIntrinsicInputKind {
        self.kind
    }

    #[must_use]
    pub const fn definition_hash(&self) -> ContentHash {
        self.definition_hash
    }

    fn validate(&self) -> Result<(), ModelServingContractError> {
        if self.semantic_version != MODEL_INTRINSIC_INPUT_VERSION {
            return Err(ModelServingContractError::IntrinsicVersionMismatch);
        }
        let expected = Self::expected_hash(self.kind)?;
        if self.definition_hash != expected {
            return Err(ModelServingContractError::IntrinsicDefinitionMismatch { kind: self.kind });
        }
        Ok(())
    }

    fn expected_hash(
        kind: ModelServingIntrinsicInputKind,
    ) -> Result<ContentHash, CanonicalDigestError> {
        #[derive(Serialize)]
        struct IntrinsicDefinition {
            kind: ModelServingIntrinsicInputKind,
            stable_name: &'static str,
            semantic_key: &'static str,
        }

        CanonicalDigest::content_hash_typed(
            MODEL_INTRINSIC_INPUT_HASH_DOMAIN,
            MODEL_INTRINSIC_INPUT_VERSION,
            &IntrinsicDefinition {
                kind,
                stable_name: kind.stable_name(),
                semantic_key: kind.semantic_key(),
            },
        )
    }
}

impl TryFrom<ModelServingIntrinsicInputKind> for ModelServingIntrinsicInputRef {
    type Error = ModelServingContractError;

    fn try_from(kind: ModelServingIntrinsicInputKind) -> Result<Self, Self::Error> {
        let definition_hash = Self::expected_hash(kind)?;
        Ok(Self {
            kind,
            semantic_version: MODEL_INTRINSIC_INPUT_VERSION,
            definition_hash,
        })
    }
}

impl TryFrom<ModelServingIntrinsicInputDocument> for ModelServingIntrinsicInputRef {
    type Error = ModelServingContractError;

    fn try_from(document: ModelServingIntrinsicInputDocument) -> Result<Self, Self::Error> {
        let binding = Self {
            kind: document.kind,
            semantic_version: document.semantic_version,
            definition_hash: document.definition_hash,
        };
        binding.validate()?;
        Ok(binding)
    }
}

/// One input in the estimator's exact weight/application order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "input_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelServingEstimatorInput {
    GovernedFactor {
        factor_definition_id: FactorDefinitionId,
    },
    ModelIntrinsic {
        binding: ModelServingIntrinsicInputRef,
    },
}

/// Immutable proof that a classical estimator has a portable exact `TreeSHAP`
/// representation and was cross-verified against the serialized estimator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelServingTreeShapBinding {
    pub ensemble_hash: ContentHash,
    pub background_distribution_hash: ContentHash,
    pub verified_case_count: u64,
    pub max_efficiency_residual: Decimal,
    pub max_prediction_residual: Decimal,
}

/// Family-specific model commitment created before the outer artifact envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "estimator_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelServingEstimatorBinding {
    FactorNative {
        ordered_inputs: Vec<ModelServingEstimatorInput>,
        /// Domain-separated hash of the family body with its header excluded.
        model_payload_hash: ContentHash,
    },
    Classical {
        kind: ClassicalKind,
        /// Domain-separated hash of the family body with its header excluded.
        model_payload_hash: ContentHash,
        serialized_model_hash: ContentHash,
        serialization_format: ModelSerializationFormat,
        tree_shap: Option<ModelServingTreeShapBinding>,
    },
}

/// Feature and label schemas frozen at training and serving.
///
/// The factor schema is owned exclusively by [`ModelServingFactorBinding`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelServingSchemaBinding {
    pub feature_schema_hash: ContentHash,
    pub label_schema_hash: ContentHash,
}

/// Exact factor plane and its governed runtime data dependencies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelServingFactorBinding {
    pub plane: FactorServingPlane,
    /// Frozen favorite-longshot correction table consumed by its factor.
    pub bias_table: Option<ModelServingCalibrationArtifactRef>,
}

/// Fitted preprocessing and exact estimator-row commitments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelServingTransformBinding {
    pub input_contract_hash: ContentHash,
    pub input_transform_hash: ContentHash,
    pub training_input_hash: ContentHash,
    pub training_dataset_hash: ContentHash,
}

/// Exact immutable calibration dependency, identified by ID, role, and hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelServingCalibrationArtifactRef {
    pub artifact_id: CalibrationArtifactId,
    pub kind: CalibrationKind,
    pub content_hash: ContentHash,
}

/// Model identity and family semantics frozen before the outer artifact hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelServingModelBinding {
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub model_spec_definition_hash: ContentHash,
    pub model_family: ModelFamily,
    pub category_scope: Option<MarketCategory>,
    pub profile_ref: ResearchProfileRef,
    pub prediction_horizon_secs: u64,
    pub estimator: ModelServingEstimatorBinding,
    pub calibration: Option<ModelServingCalibrationArtifactRef>,
}

/// Exact content-addressed entry/exit policy dependency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelServingTradePolicyBinding {
    pub artifact_id: TradePolicyArtifactId,
    pub content_hash: ContentHash,
}

/// Complete Dataset v3 lineage and exact materialized bytes used by training.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelServingDatasetBinding {
    pub manifest: DatasetManifest,
    pub manifest_hash: ContentHash,
    pub artifact_bytes_hash: ContentHash,
}

/// Policy snapshot and the exact four immutable profile artifacts it resolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelServingPolicySnapshotBinding {
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
    pub profile_artifacts: ImmutableProfileArtifactReferences,
}

/// Every semantic dependency required to recreate one serving plane.
///
/// This is deliberately one nested value: every future semantic field must be
/// added here and is therefore automatically covered by the contract hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelServingBindings {
    pub policy_snapshot: ModelServingPolicySnapshotBinding,
    /// Canonical domain families consumed by either the factor plane or the
    /// classical feature contract. Producers derive this from the full input
    /// plane; it is never inferred from category alone.
    pub required_domain_families: Vec<DomainFamily>,
    pub capability_registry_hashes: CapabilityRegistryHashes,
    /// Full canonical plane for factor-native families; canonical empty plane
    /// for classical families.
    pub factors: ModelServingFactorBinding,
    pub schemas: ModelServingSchemaBinding,
    pub transform: ModelServingTransformBinding,
    pub model: ModelServingModelBinding,
    pub trade_policy: Option<ModelServingTradePolicyBinding>,
    pub dataset: ModelServingDatasetBinding,
}

/// Stable validation failures for an immutable serving contract.
#[derive(Debug, Error)]
pub enum ModelServingContractError {
    #[error("unsupported model-serving contract version {actual}; expected {expected}")]
    UnsupportedVersion { expected: u32, actual: u32 },
    #[error("profile slot expected {expected:?}, got {actual:?}")]
    ProfileKindMismatch {
        expected: ProfileArtifactKind,
        actual: ProfileArtifactKind,
    },
    #[error("profile artifact identity does not match its {kind:?} content hash")]
    ProfileIdentityMismatch { kind: ProfileArtifactKind },
    #[error("policy snapshot id does not derive from its snapshot hash")]
    PolicySnapshotIdentityMismatch,
    #[error("capability registry hashes are not canonical")]
    NonCanonicalCapabilities,
    #[error("required domain families must be strictly ordered and unique")]
    NonCanonicalDomainFamilies,
    #[error("factor plane requires {family:?} domain capability lineage")]
    MissingFactorDomain { family: DomainFamily },
    #[error("enabled {category:?} serving requires capability registry lineage")]
    MissingCategoryCapabilities { category: MarketCategory },
    #[error("factor {factor_definition_id} is bound to a different feature contract")]
    FactorFeatureMismatch {
        factor_definition_id: FactorDefinitionId,
    },
    #[error("estimator input name `{name}` is duplicated or collides across input owners")]
    DuplicateEstimatorInput { name: String },
    #[error("governed factor name `{name}` collides with the reserved model-intrinsic namespace")]
    IntrinsicNameCollision { name: String },
    #[error("model-intrinsic input uses an unsupported semantic version")]
    IntrinsicVersionMismatch,
    #[error("model-intrinsic {kind:?} definition hash is not canonical")]
    IntrinsicDefinitionMismatch {
        kind: ModelServingIntrinsicInputKind,
    },
    #[error("model-intrinsic input definitions must be unique")]
    DuplicateIntrinsicDefinition,
    #[error("estimator references factor definition {factor_definition_id} outside its plane")]
    UnknownEstimatorFactor {
        factor_definition_id: FactorDefinitionId,
    },
    #[error("diagnostic factor definition {factor_definition_id} cannot be an estimator input")]
    DiagnosticEstimatorFactor {
        factor_definition_id: FactorDefinitionId,
    },
    #[error("factor plane contains unused definition {factor_definition_id}")]
    UnusedPlaneFactor {
        factor_definition_id: FactorDefinitionId,
    },
    #[error("factor-native estimators require at least one governed factor")]
    MissingGovernedFactor,
    #[error("classical estimators cannot carry an unused factor plane")]
    ClassicalFactorPlaneForbidden,
    #[error("v1 classical estimators require bincode serialization")]
    ClassicalFormatMismatch,
    #[error("only GradientBoostedTrees may bind exact TreeSHAP evidence")]
    ClassicalExplanationMismatch,
    #[error("estimator binding is incompatible with model family {model_family:?}")]
    EstimatorFamilyMismatch { model_family: ModelFamily },
    #[error("WeightedFactor cannot consume model-intrinsic position inputs")]
    WeightedIntrinsicForbidden,
    #[error("Sell estimator must bind the exact canonical position-input set")]
    SellIntrinsicMismatch,
    #[error("prediction horizon must be positive")]
    PredictionHorizonMismatch,
    #[error("invalid serving dataset manifest: {detail}")]
    InvalidDatasetManifest { detail: String },
    #[error("serving models require a Training dataset, got {actual:?}")]
    InvalidDatasetPurpose { actual: DatasetPurpose },
    #[error("serving models cannot bind an empty training dataset")]
    EmptyTrainingDataset,
    #[error("dataset manifest hash mismatch: expected {expected}, got {actual}")]
    DatasetManifestHashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
    #[error("{binding} schema does not match the dataset manifest")]
    SchemaBindingMismatch { binding: &'static str },
    #[error("model specification does not match the dataset manifest")]
    ModelSpecMismatch,
    #[error("model family does not match the dataset manifest")]
    DatasetModelFamilyMismatch,
    #[error("research profile does not match dataset source lineage")]
    ResearchProfileMismatch,
    #[error("policy snapshot does not match dataset source lineage")]
    PolicySnapshotMismatch,
    #[error("capability registry does not match dataset source lineage")]
    CapabilityBindingMismatch,
    #[error("training dataset hash does not match the dataset manifest")]
    TrainingDatasetMismatch,
    #[error("trade-policy binding does not match the dataset manifest")]
    TradePolicyBindingMismatch,
    #[error("trade-policy id does not derive from its content hash")]
    TradePolicyIdentityMismatch,
    #[error("only WeightedFactor may bind a model-score calibration dependency")]
    CalibrationFamilyMismatch,
    #[error("calibration artifact kind does not match its serving role")]
    CalibrationKindMismatch,
    #[error("one calibration artifact cannot occupy both model-score and bias-table roles")]
    CalibrationRoleCollision,
    #[error("bias-table binding requires the favorite-longshot factor in the serving plane")]
    UnusedBiasTable,
    #[error("model-serving contract hash mismatch: expected {expected}, got {actual}")]
    ContractHashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
    #[error("persisted serving-contract hash mismatch: expected {expected}, got {actual}")]
    PersistedHashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
    #[error(transparent)]
    FactorPlane(#[from] FactorServingPlaneError),
    #[error(transparent)]
    Hash(#[from] CanonicalDigestError),
}

/// Sealed serving contract. Its fields cannot be mutated after validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields, try_from = "ModelServingContractDocument")]
pub struct ModelServingContract {
    contract_version: u32,
    contract_hash: ContentHash,
    bindings: ModelServingBindings,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelServingContractDocument {
    contract_version: u32,
    contract_hash: ContentHash,
    bindings: ModelServingBindings,
}

impl ModelServingContract {
    /// Validate and content-address all serving dependencies atomically.
    pub fn try_seal(bindings: ModelServingBindings) -> Result<Self, ModelServingContractError> {
        bindings.validate()?;
        let contract_hash = Self::hash_bindings(&bindings)?;
        Ok(Self {
            contract_version: MODEL_SERVING_CONTRACT_VERSION,
            contract_hash,
            bindings,
        })
    }

    #[must_use]
    pub const fn contract_version(&self) -> u32 {
        self.contract_version
    }

    #[must_use]
    pub const fn contract_hash(&self) -> ContentHash {
        self.contract_hash
    }

    #[must_use]
    pub const fn bindings(&self) -> &ModelServingBindings {
        &self.bindings
    }

    /// Revalidate every cross-binding and the embedded content hash.
    pub fn validate(&self) -> Result<(), ModelServingContractError> {
        if self.contract_version != MODEL_SERVING_CONTRACT_VERSION {
            return Err(ModelServingContractError::UnsupportedVersion {
                expected: MODEL_SERVING_CONTRACT_VERSION,
                actual: self.contract_version,
            });
        }
        self.bindings.validate()?;
        let expected = Self::hash_bindings(&self.bindings)?;
        if expected != self.contract_hash {
            return Err(ModelServingContractError::ContractHashMismatch {
                expected,
                actual: self.contract_hash,
            });
        }
        Ok(())
    }

    /// Verify the normalized scalar hash stored beside the typed document.
    pub fn verify_persisted_hash(
        &self,
        persisted_hash: ContentHash,
    ) -> Result<(), ModelServingContractError> {
        self.validate()?;
        if persisted_hash != self.contract_hash {
            return Err(ModelServingContractError::PersistedHashMismatch {
                expected: self.contract_hash,
                actual: persisted_hash,
            });
        }
        Ok(())
    }

    fn hash_bindings(bindings: &ModelServingBindings) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_typed(
            MODEL_SERVING_CONTRACT_HASH_DOMAIN,
            MODEL_SERVING_CONTRACT_VERSION,
            bindings,
        )
    }
}

impl TryFrom<ModelServingContractDocument> for ModelServingContract {
    type Error = ModelServingContractError;

    fn try_from(document: ModelServingContractDocument) -> Result<Self, Self::Error> {
        let contract = Self {
            contract_version: document.contract_version,
            contract_hash: document.contract_hash,
            bindings: document.bindings,
        };
        contract.validate()?;
        Ok(contract)
    }
}

impl ModelServingBindings {
    /// Validate every identity and cross-document projection before sealing.
    pub fn validate(&self) -> Result<(), ModelServingContractError> {
        self.validate_profiles()?;
        if self
            .required_domain_families
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(ModelServingContractError::NonCanonicalDomainFamilies);
        }
        CapabilityRegistryHashes::try_new(self.capability_registry_hashes.as_slice().to_vec())
            .map_err(|_| ModelServingContractError::NonCanonicalCapabilities)?;
        self.validate_factor_plane()?;
        for (factor_family, domain_family) in [
            (FactorFamily::DomainCrypto, DomainFamily::Crypto),
            (FactorFamily::DomainWeather, DomainFamily::Weather),
        ] {
            if self
                .factors
                .plane
                .definitions()
                .iter()
                .any(|factor| factor.definition().family == factor_family)
                && !self.required_domain_families.contains(&domain_family)
            {
                return Err(ModelServingContractError::MissingFactorDomain {
                    family: domain_family,
                });
            }
        }
        if let Some(category @ (MarketCategory::Crypto | MarketCategory::Weather)) =
            self.model.category_scope
        {
            let required = match category {
                MarketCategory::Crypto => DomainFamily::Crypto,
                MarketCategory::Weather => DomainFamily::Weather,
                _ => {
                    return Err(ModelServingContractError::MissingCategoryCapabilities {
                        category,
                    });
                }
            };
            if !self.required_domain_families.contains(&required)
                || self.capability_registry_hashes.as_slice().is_empty()
            {
                return Err(ModelServingContractError::MissingCategoryCapabilities { category });
            }
        }
        if !self.required_domain_families.is_empty()
            && self.capability_registry_hashes.as_slice().is_empty()
        {
            return Err(ModelServingContractError::MissingFactorDomain {
                family: self.required_domain_families[0],
            });
        }
        self.validate_estimator()?;
        self.validate_dataset()?;
        Ok(())
    }

    fn validate_profiles(&self) -> Result<(), ModelServingContractError> {
        if DecisionPolicySnapshotId::from_content_hash(&self.policy_snapshot.snapshot_hash)
            != self.policy_snapshot.decision_policy_snapshot_id
        {
            return Err(ModelServingContractError::PolicySnapshotIdentityMismatch);
        }
        Self::validate_profile(
            &self.policy_snapshot.profile_artifacts.features,
            ProfileArtifactKind::Feature,
        )?;
        Self::validate_profile(
            &self.policy_snapshot.profile_artifacts.scoring,
            ProfileArtifactKind::Scoring,
        )?;
        Self::validate_profile(
            &self.policy_snapshot.profile_artifacts.domain,
            ProfileArtifactKind::Domain,
        )?;
        Self::validate_profile(
            &self.policy_snapshot.profile_artifacts.research_method,
            ProfileArtifactKind::ResearchMethod,
        )
    }

    fn validate_profile(
        reference: &PolicyProfileArtifactReference,
        expected: ProfileArtifactKind,
    ) -> Result<(), ModelServingContractError> {
        if reference.kind != expected {
            return Err(ModelServingContractError::ProfileKindMismatch {
                expected,
                actual: reference.kind,
            });
        }
        let expected_id =
            ProfileArtifactId::from_content_address(expected.as_str(), &reference.content_hash);
        if expected_id != reference.profile_artifact_id {
            return Err(ModelServingContractError::ProfileIdentityMismatch { kind: expected });
        }
        Ok(())
    }

    fn validate_factor_plane(&self) -> Result<(), ModelServingContractError> {
        self.factors.plane.validate()?;
        for factor in self.factors.plane.definitions() {
            if factor.feature_contract_hash() != self.schemas.feature_schema_hash {
                return Err(ModelServingContractError::FactorFeatureMismatch {
                    factor_definition_id: factor.factor_definition_id(),
                });
            }
        }
        Ok(())
    }

    fn validate_estimator(&self) -> Result<(), ModelServingContractError> {
        match (&self.model.estimator, self.model.model_family) {
            (
                ModelServingEstimatorBinding::FactorNative { ordered_inputs, .. },
                ModelFamily::WeightedFactor | ModelFamily::HoldVsExitWeighted,
            ) => self.validate_factor_inputs(ordered_inputs),
            (
                ModelServingEstimatorBinding::Classical {
                    kind,
                    serialization_format,
                    tree_shap,
                    ..
                },
                model_family,
            ) if model_family.classical_kind() == Some(*kind) => {
                let explanation_mismatch = (*kind == ClassicalKind::GradientBoostedTrees)
                    != tree_shap.is_some()
                    || tree_shap.as_ref().is_some_and(|binding| {
                        binding.verified_case_count == 0
                            || binding.max_efficiency_residual.is_sign_negative()
                            || binding.max_prediction_residual.is_sign_negative()
                            || binding.max_efficiency_residual
                                > Decimal::from_parts(1, 0, 0, false, 12)
                            || binding.max_prediction_residual
                                > Decimal::from_parts(1, 0, 0, false, 10)
                    });
                if *serialization_format != ModelSerializationFormat::Bincode {
                    Err(ModelServingContractError::ClassicalFormatMismatch)
                } else if explanation_mismatch {
                    Err(ModelServingContractError::ClassicalExplanationMismatch)
                } else if self.factors.plane.definitions().is_empty() {
                    Ok(())
                } else {
                    Err(ModelServingContractError::ClassicalFactorPlaneForbidden)
                }
            }
            (_, model_family) => {
                Err(ModelServingContractError::EstimatorFamilyMismatch { model_family })
            }
        }
    }

    fn validate_factor_inputs(
        &self,
        ordered_inputs: &[ModelServingEstimatorInput],
    ) -> Result<(), ModelServingContractError> {
        if let Some(factor) = self.factors.plane.definitions().iter().find(|factor| {
            ModelServingIntrinsicInputKind::SELL_KINDS
                .iter()
                .any(|kind| kind.stable_name() == factor.factor_name().as_str())
        }) {
            return Err(ModelServingContractError::IntrinsicNameCollision {
                name: factor.factor_name().to_string(),
            });
        }
        let mut input_names = HashSet::new();
        let mut governed_ids = HashSet::new();
        let mut intrinsic_kinds = HashSet::new();
        let mut intrinsic_hashes = HashSet::new();
        for input in ordered_inputs {
            match input {
                ModelServingEstimatorInput::GovernedFactor {
                    factor_definition_id,
                } => {
                    let factor = self
                        .factors
                        .plane
                        .definitions()
                        .iter()
                        .find(|factor| factor.factor_definition_id() == *factor_definition_id)
                        .ok_or(ModelServingContractError::UnknownEstimatorFactor {
                            factor_definition_id: *factor_definition_id,
                        })?;
                    if factor.definition().is_diagnostic() {
                        return Err(ModelServingContractError::DiagnosticEstimatorFactor {
                            factor_definition_id: *factor_definition_id,
                        });
                    }
                    let name = factor.factor_name().as_str();
                    if !input_names.insert(name) {
                        return Err(ModelServingContractError::DuplicateEstimatorInput {
                            name: name.to_owned(),
                        });
                    }
                    governed_ids.insert(*factor_definition_id);
                }
                ModelServingEstimatorInput::ModelIntrinsic { binding } => {
                    binding.validate()?;
                    let name = binding.kind().stable_name();
                    if !input_names.insert(name) {
                        return Err(ModelServingContractError::DuplicateEstimatorInput {
                            name: name.to_owned(),
                        });
                    }
                    if !intrinsic_hashes.insert(binding.definition_hash()) {
                        return Err(ModelServingContractError::DuplicateIntrinsicDefinition);
                    }
                    intrinsic_kinds.insert(binding.kind());
                }
            }
        }
        if governed_ids.is_empty() {
            return Err(ModelServingContractError::MissingGovernedFactor);
        }
        if let Some(unused) = self.factors.plane.definitions().iter().find(|factor| {
            !factor.definition().is_diagnostic()
                && !governed_ids.contains(&factor.factor_definition_id())
        }) {
            return Err(ModelServingContractError::UnusedPlaneFactor {
                factor_definition_id: unused.factor_definition_id(),
            });
        }
        match self.model.model_family {
            ModelFamily::WeightedFactor if intrinsic_kinds.is_empty() => Ok(()),
            ModelFamily::WeightedFactor => {
                Err(ModelServingContractError::WeightedIntrinsicForbidden)
            }
            ModelFamily::HoldVsExitWeighted
                if intrinsic_kinds
                    == ModelServingIntrinsicInputKind::SELL_KINDS
                        .into_iter()
                        .collect::<HashSet<_>>() =>
            {
                Ok(())
            }
            ModelFamily::HoldVsExitWeighted => {
                Err(ModelServingContractError::SellIntrinsicMismatch)
            }
            model_family => {
                Err(ModelServingContractError::EstimatorFamilyMismatch { model_family })
            }
        }
    }

    fn validate_dataset(&self) -> Result<(), ModelServingContractError> {
        let manifest = &self.dataset.manifest;
        manifest
            .validate()
            .map_err(|error| ModelServingContractError::InvalidDatasetManifest {
                detail: error.to_string(),
            })?;
        if manifest.purpose != DatasetPurpose::Training {
            return Err(ModelServingContractError::InvalidDatasetPurpose {
                actual: manifest.purpose,
            });
        }
        if manifest.sample_count == 0 {
            return Err(ModelServingContractError::EmptyTrainingDataset);
        }
        let manifest_hash = manifest
            .content_hash()
            .map_err(|detail| ModelServingContractError::InvalidDatasetManifest { detail })?;
        if manifest_hash != self.dataset.manifest_hash {
            return Err(ModelServingContractError::DatasetManifestHashMismatch {
                expected: manifest_hash,
                actual: self.dataset.manifest_hash,
            });
        }
        if self.schemas.feature_schema_hash != manifest.feature_schema_hash {
            return Err(ModelServingContractError::SchemaBindingMismatch { binding: "feature" });
        }
        if self.factors.plane.factor_schema_hash() != manifest.factor_schema_hash() {
            return Err(ModelServingContractError::SchemaBindingMismatch { binding: "factor" });
        }
        if self.schemas.label_schema_hash != manifest.label_schema_hash {
            return Err(ModelServingContractError::SchemaBindingMismatch { binding: "label" });
        }
        if self.model.model_spec_id != manifest.model_spec_id
            || self.model.model_spec_definition_hash != manifest.model_spec_definition_hash
        {
            return Err(ModelServingContractError::ModelSpecMismatch);
        }
        if self.model.model_family != manifest.model_family {
            return Err(ModelServingContractError::DatasetModelFamilyMismatch);
        }
        if self.model.profile_ref
            != manifest
                .source_lineage
                .research_profile_artifact_id
                .profile_ref()
        {
            return Err(ModelServingContractError::ResearchProfileMismatch);
        }
        if self.policy_snapshot.decision_policy_snapshot_id
            != manifest.source_lineage.decision_policy_snapshot_id
            || self.policy_snapshot.snapshot_hash != manifest.source_lineage.runtime_config_hash
        {
            return Err(ModelServingContractError::PolicySnapshotMismatch);
        }
        if self.capability_registry_hashes != manifest.source_lineage.capability_registry_hashes {
            return Err(ModelServingContractError::CapabilityBindingMismatch);
        }
        if self.transform.training_dataset_hash != manifest.semantic_dataset_hash {
            return Err(ModelServingContractError::TrainingDatasetMismatch);
        }
        if self.model.prediction_horizon_secs == 0 {
            return Err(ModelServingContractError::PredictionHorizonMismatch);
        }
        self.validate_trade_policy(manifest)?;
        if self.model.calibration.is_some()
            && self.model.model_family != ModelFamily::WeightedFactor
        {
            return Err(ModelServingContractError::CalibrationFamilyMismatch);
        }
        if self
            .model
            .calibration
            .as_ref()
            .is_some_and(|binding| binding.kind != CalibrationKind::ModelScore)
            || self
                .factors
                .bias_table
                .as_ref()
                .is_some_and(|binding| binding.kind != CalibrationKind::MarketPriceBias)
        {
            return Err(ModelServingContractError::CalibrationKindMismatch);
        }
        if let (Some(calibration), Some(bias_table)) =
            (&self.model.calibration, &self.factors.bias_table)
            && (calibration.artifact_id == bias_table.artifact_id
                || calibration.content_hash == bias_table.content_hash)
        {
            return Err(ModelServingContractError::CalibrationRoleCollision);
        }
        if self.factors.bias_table.is_some()
            && !self
                .factors
                .plane
                .definitions()
                .iter()
                .any(|factor| factor.factor_name().as_str() == FAVORITE_LONGSHOT_FACTOR_NAME)
        {
            return Err(ModelServingContractError::UnusedBiasTable);
        }
        Ok(())
    }

    fn validate_trade_policy(
        &self,
        manifest: &DatasetManifest,
    ) -> Result<(), ModelServingContractError> {
        match (
            &self.trade_policy,
            manifest.trade_policy_artifact_id,
            manifest.trade_policy_hash,
        ) {
            (None, None, None) => Ok(()),
            (Some(binding), Some(artifact_id), Some(content_hash)) => {
                if TradePolicyArtifactId::from_content_hash(&binding.content_hash)
                    != binding.artifact_id
                {
                    return Err(ModelServingContractError::TradePolicyIdentityMismatch);
                }
                if binding.artifact_id != artifact_id || binding.content_hash != content_hash {
                    return Err(ModelServingContractError::TradePolicyBindingMismatch);
                }
                Ok(())
            }
            _ => Err(ModelServingContractError::TradePolicyBindingMismatch),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;
    use serde_json::{Value, from_value, to_value};
    use uuid::Uuid;

    use super::{
        MODEL_INTRINSIC_INPUT_VERSION, MODEL_SERVING_CONTRACT_VERSION, ModelServingBindings,
        ModelServingCalibrationArtifactRef, ModelServingContract, ModelServingContractError,
        ModelServingDatasetBinding, ModelServingEstimatorBinding, ModelServingEstimatorInput,
        ModelServingFactorBinding, ModelServingIntrinsicInputKind, ModelServingIntrinsicInputRef,
        ModelServingModelBinding, ModelServingPolicySnapshotBinding, ModelServingSchemaBinding,
        ModelServingTradePolicyBinding, ModelServingTransformBinding, ModelServingTreeShapBinding,
    };
    use crate::{
        domain::quant::{
            CandidateExplanationMethod, CandidateExplanationValidation,
            CandidateExplanationVerification,
        },
        enums::{
            common::MarketCategory,
            domain::DomainFamily,
            factor::{FactorFamily, FactorNormalization},
            model::{ClassicalKind, ModelFamily},
            quant::{CalibrationKind, DatasetPurpose, ModelSerializationFormat},
            runtime_config::ProfileArtifactKind,
        },
        runtime_config::ImmutableProfileArtifacts,
        types::{
            ArtifactUri, CRYPTO_PRICE_15M_PROFILE_ID, CalibrationArtifactId,
            CapabilityRegistryHashes, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION,
            DATASET_SOURCE_LINEAGE_FORMAT_VERSION, DatasetManifest, DatasetSourceLineage,
            DecisionPolicySnapshotId, FactorDefinitionId, ModelSpecId, ModelVersionId,
            ProfileArtifactId, ReaderContractVersion, ResearchProfileArtifactId,
            SchemaContractVersion, SchemaVersion, SourceSliceId, SourceSliceManifestRef,
            TradePolicyArtifactId, TrainingDatasetId, builtin_research_profiles,
            factor::{
                FactorComputationContract, FactorContextEffect, FactorDefinitionDocument,
                FactorDefinitionRef, FactorOutputSemantics, FactorServingPlane,
            },
            stable_name::FactorName,
        },
    };

    fn hash(byte: u8) -> ContentHash {
        ContentHash::from_bytes([byte; 32])
    }

    fn factor(
        name: &str,
        family: FactorFamily,
        feature_contract_hash: ContentHash,
    ) -> FactorDefinitionRef {
        FactorDefinitionRef::try_seal(
            FactorDefinitionDocument {
                name: FactorName::new(name),
                family,
                input_features: Vec::new(),
                output: FactorOutputSemantics::Context {
                    effect: FactorContextEffect::HigherIsSupportive,
                },
                normalization: FactorNormalization::Rank,
                owner: "research".to_owned(),
                required: false,
                computation: FactorComputationContract {
                    semantic_version: 1,
                    semantic_key:
                        "quant-pivot/test-factor@1+quant-pivot/factor-normalization-boundary@1"
                            .to_owned(),
                },
            },
            feature_contract_hash,
            SchemaVersion::FIRST,
            SchemaVersion::FIRST,
        )
        .expect("valid factor revision")
    }

    impl DatasetManifest {
        fn serving_fixture() -> Self {
            let window_start = Utc
                .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
                .single()
                .expect("valid window start");
            let window_end = Utc
                .with_ymd_and_hms(2026, 1, 2, 0, 0, 0)
                .single()
                .expect("valid window end");
            let pit_cutoff = Utc
                .with_ymd_and_hms(2026, 1, 3, 0, 0, 0)
                .single()
                .expect("valid PIT cutoff");
            let profile = builtin_research_profiles()
                .expect("built-in profiles")
                .into_iter()
                .find(|profile| profile.profile_ref.id.as_str() == CRYPTO_PRICE_15M_PROFILE_ID)
                .expect("crypto profile");
            let capability_registry_hashes =
                CapabilityRegistryHashes::try_new(vec![hash(1), hash(2)])
                    .expect("canonical capabilities");
            let runtime_config_hash = hash(6);
            Self {
                format_version: DATASET_ARTIFACT_FORMAT_VERSION,
                training_dataset_id: TrainingDatasetId::from_v7(),
                source_lineage: DatasetSourceLineage {
                    format_version: DATASET_SOURCE_LINEAGE_FORMAT_VERSION,
                    fit_seal_id: Uuid::from_u128(18).into(),
                    fit_seal_hash: hash(18),
                    source_slice_id: SourceSliceId::from_v7(),
                    source_slice_identity_hash: hash(3),
                    research_profile_artifact_id: ResearchProfileArtifactId::from_profile_ref(
                        &profile.profile_ref,
                    ),
                    research_program_hash: hash(4),
                    source_slice: SourceSliceManifestRef {
                        manifest_uri: ArtifactUri::parse("file://source-slices/manifest.json")
                            .expect("source manifest URI"),
                        manifest_hash: hash(5),
                    },
                    source_window_start: window_start,
                    source_window_end: window_end,
                    pit_cutoff,
                    decision_policy_snapshot_id: DecisionPolicySnapshotId::from_content_hash(
                        &runtime_config_hash,
                    ),
                    runtime_config_hash,
                    reader_contract_version: ReaderContractVersion::v1(),
                    schema_contract_version: SchemaContractVersion::parse("source_slice_schema_v1")
                        .expect("schema contract"),
                    source_schema_hash: hash(7),
                    capability_registry_hashes,
                },
                cohort_manifest: None,
                model_spec_id: ModelSpecId::from_v7(),
                model_family: ModelFamily::HoldVsExitWeighted,
                model_spec_definition_hash: hash(8),
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
                window_start,
                window_end,
                purpose: DatasetPurpose::Training,
                knowledge_lag_secs: 60,
                sample_interval_secs: 300,
                horizons_secs: vec![900],
                feature_schema_version: SchemaVersion::FIRST,
                feature_schema_hash: hash(9),
                factor_serving_plane: FactorServingPlane::try_seal(vec![factor(
                    "manifest_factor",
                    FactorFamily::Momentum,
                    hash(9),
                )])
                .expect("manifest factor plane"),
                label_schema_hash: hash(11),
                semantic_dataset_hash: hash(12),
                source_fingerprint: hash(13),
                sample_count: 128,
            }
        }
    }

    impl ModelServingBindings {
        fn serving_fixture() -> Self {
            let mut manifest = DatasetManifest::serving_fixture();
            let profile_ref = manifest
                .source_lineage
                .research_profile_artifact_id
                .profile_ref();
            let feature_hash = manifest.feature_schema_hash;
            let first_factor = factor("momentum", FactorFamily::Momentum, feature_hash);
            let second_factor = factor("liquidity_depth", FactorFamily::Liquidity, feature_hash);
            let first_factor_id = first_factor.factor_definition_id();
            let second_factor_id = second_factor.factor_definition_id();
            let factor_plane = FactorServingPlane::try_seal(vec![second_factor, first_factor])
                .expect("factor serving plane");
            manifest.factor_serving_plane = factor_plane.clone();
            let manifest_hash = manifest.content_hash().expect("dataset manifest hash");
            Self {
                policy_snapshot: ModelServingPolicySnapshotBinding {
                    decision_policy_snapshot_id: manifest
                        .source_lineage
                        .decision_policy_snapshot_id,
                    snapshot_hash: manifest.source_lineage.runtime_config_hash,
                    profile_artifacts: ImmutableProfileArtifacts::default()
                        .references()
                        .expect("profile references"),
                },
                required_domain_families: vec![DomainFamily::Crypto],
                capability_registry_hashes: manifest
                    .source_lineage
                    .capability_registry_hashes
                    .clone(),
                factors: ModelServingFactorBinding {
                    plane: factor_plane,
                    bias_table: None,
                },
                schemas: ModelServingSchemaBinding {
                    feature_schema_hash: feature_hash,
                    label_schema_hash: manifest.label_schema_hash,
                },
                transform: ModelServingTransformBinding {
                    input_contract_hash: hash(22),
                    input_transform_hash: hash(23),
                    training_input_hash: hash(24),
                    training_dataset_hash: manifest.semantic_dataset_hash,
                },
                model: ModelServingModelBinding {
                    model_version_id: ModelVersionId::from_v7(),
                    model_spec_id: manifest.model_spec_id,
                    model_spec_definition_hash: manifest.model_spec_definition_hash,
                    model_family: ModelFamily::HoldVsExitWeighted,
                    category_scope: Some(MarketCategory::Crypto),
                    profile_ref,
                    prediction_horizon_secs: 900,
                    estimator: ModelServingEstimatorBinding::FactorNative {
                        ordered_inputs: vec![
                            ModelServingEstimatorInput::GovernedFactor {
                                factor_definition_id: first_factor_id,
                            },
                            ModelServingEstimatorInput::GovernedFactor {
                                factor_definition_id: second_factor_id,
                            },
                            ModelServingEstimatorInput::ModelIntrinsic {
                                binding: ModelServingIntrinsicInputKind::PositionTakeProfitPressure
                                    .try_into()
                                    .expect("take-profit definition"),
                            },
                            ModelServingEstimatorInput::ModelIntrinsic {
                                binding: ModelServingIntrinsicInputKind::PositionStopLossPressure
                                    .try_into()
                                    .expect("stop-loss definition"),
                            },
                            ModelServingEstimatorInput::ModelIntrinsic {
                                binding: ModelServingIntrinsicInputKind::PositionTimeInTrade
                                    .try_into()
                                    .expect("time-in-trade definition"),
                            },
                            ModelServingEstimatorInput::ModelIntrinsic {
                                binding: ModelServingIntrinsicInputKind::PositionPeakDrawdown
                                    .try_into()
                                    .expect("peak-drawdown definition"),
                            },
                        ],
                        model_payload_hash: hash(25),
                    },
                    calibration: None,
                },
                trade_policy: None,
                dataset: ModelServingDatasetBinding {
                    manifest,
                    manifest_hash,
                    artifact_bytes_hash: hash(26),
                },
            }
        }

        fn weighted_fixture() -> Self {
            let mut bindings = Self::serving_fixture();
            bindings.model.model_family = ModelFamily::WeightedFactor;
            let ModelServingEstimatorBinding::FactorNative { ordered_inputs, .. } =
                &mut bindings.model.estimator
            else {
                panic!("factor-native fixture");
            };
            ordered_inputs
                .retain(|input| matches!(input, ModelServingEstimatorInput::GovernedFactor { .. }));
            bindings.dataset.manifest.model_family = ModelFamily::WeightedFactor;
            bindings.refresh_manifest_hash();
            bindings
        }

        fn classical_fixture(kind: ClassicalKind) -> Self {
            let mut bindings = Self::serving_fixture();
            bindings.factors.plane =
                FactorServingPlane::try_empty().expect("empty factor serving plane");
            bindings.model.model_family = ModelFamily::from_classical(kind);
            bindings.dataset.manifest.model_family = bindings.model.model_family;
            bindings.dataset.manifest.factor_serving_plane = bindings.factors.plane.clone();
            bindings.refresh_manifest_hash();
            bindings.model.estimator = ModelServingEstimatorBinding::Classical {
                kind,
                model_payload_hash: hash(30),
                serialized_model_hash: hash(31),
                serialization_format: ModelSerializationFormat::Bincode,
                tree_shap: (kind == ClassicalKind::GradientBoostedTrees).then_some(
                    ModelServingTreeShapBinding {
                        ensemble_hash: hash(32),
                        background_distribution_hash: hash(33),
                        verified_case_count: 10,
                        max_efficiency_residual: Decimal::ZERO,
                        max_prediction_residual: Decimal::ZERO,
                    },
                ),
            };
            bindings.model.calibration = None;
            bindings
        }

        fn refresh_manifest_hash(&mut self) {
            self.dataset.manifest_hash = self
                .dataset
                .manifest
                .content_hash()
                .expect("dataset manifest hash");
        }

        fn add_bias_factor(&mut self) {
            let revision = factor(
                "struct.favorite_longshot",
                FactorFamily::Structural,
                self.schemas.feature_schema_hash,
            );
            let factor_definition_id = revision.factor_definition_id();
            let mut definitions = self.factors.plane.definitions().to_vec();
            definitions.push(revision);
            self.factors.plane =
                FactorServingPlane::try_seal(definitions).expect("factor plane with bias factor");
            let ModelServingEstimatorBinding::FactorNative { ordered_inputs, .. } =
                &mut self.model.estimator
            else {
                panic!("factor-native fixture");
            };
            ordered_inputs.push(ModelServingEstimatorInput::GovernedFactor {
                factor_definition_id,
            });
            self.dataset.manifest.factor_serving_plane = self.factors.plane.clone();
            self.refresh_manifest_hash();
        }
    }

    fn calibration_ref(kind: CalibrationKind, hash_byte: u8) -> ModelServingCalibrationArtifactRef {
        ModelServingCalibrationArtifactRef {
            artifact_id: CalibrationArtifactId::from_v7(),
            kind,
            content_hash: hash(hash_byte),
        }
    }

    #[test]
    fn roundtrip_preserves_contract() {
        let bindings = ModelServingBindings::serving_fixture();
        let contract = ModelServingContract::try_seal(bindings.clone()).expect("sealed contract");
        let repeated = ModelServingContract::try_seal(bindings).expect("repeated contract");
        assert_eq!(repeated.contract_hash(), contract.contract_hash());
        let restored = from_value::<ModelServingContract>(
            to_value(&contract).expect("serialize serving contract"),
        )
        .expect("deserialize serving contract");

        assert_eq!(restored, contract);
        restored
            .verify_persisted_hash(contract.contract_hash())
            .expect("persisted hash");
    }

    #[test]
    fn factor_order_is_semantic() {
        let base = ModelServingBindings::serving_fixture();
        let plane_hash = base.factors.plane.factor_schema_hash();
        let original = ModelServingContract::try_seal(base.clone()).expect("original contract");
        let mut reordered = base;
        let ModelServingEstimatorBinding::FactorNative { ordered_inputs, .. } =
            &mut reordered.model.estimator
        else {
            panic!("factor-native fixture");
        };
        ordered_inputs.swap(0, 1);
        let reordered =
            ModelServingContract::try_seal(reordered).expect("reordered valid contract");

        assert_eq!(
            reordered.bindings().factors.plane.factor_schema_hash(),
            plane_hash
        );
        assert_ne!(original.contract_hash(), reordered.contract_hash());

        let mut tampered_order = to_value(&original).expect("serialize serving contract");
        tampered_order["bindings"]["model"]["estimator"]["ordered_inputs"]
            .as_array_mut()
            .expect("ordered estimator inputs")
            .swap(0, 1);
        assert!(from_value::<ModelServingContract>(tampered_order).is_err());
    }

    #[test]
    fn tampered_contract_rejected() {
        let contract = ModelServingContract::try_seal(ModelServingBindings::serving_fixture())
            .expect("sealed contract");
        let mut changed_payload = to_value(&contract).expect("serialize serving contract");
        changed_payload["bindings"]["transform"]["training_input_hash"] =
            Value::String(hash(27).to_string());
        assert!(from_value::<ModelServingContract>(changed_payload).is_err());

        let mut changed_hash = to_value(&contract).expect("serialize serving contract");
        changed_hash["contract_hash"] = Value::String(hash(28).to_string());
        assert!(from_value::<ModelServingContract>(changed_hash).is_err());

        assert!(matches!(
            contract.verify_persisted_hash(hash(29)),
            Err(ModelServingContractError::PersistedHashMismatch { .. })
        ));
    }

    #[test]
    fn old_versions_fail_closed() {
        let contract = ModelServingContract::try_seal(ModelServingBindings::serving_fixture())
            .expect("sealed contract");
        let predecessor = MODEL_SERVING_CONTRACT_VERSION
            .checked_sub(1)
            .expect("serving contract version has a predecessor");
        for version in [0_u64, u64::from(predecessor)] {
            let mut document = to_value(&contract).expect("serialize serving contract");
            document["contract_version"] = Value::Number(version.into());
            assert!(from_value::<ModelServingContract>(document).is_err());
        }
    }

    #[test]
    fn unknown_fields_rejected() {
        let contract = ModelServingContract::try_seal(ModelServingBindings::serving_fixture())
            .expect("sealed contract");
        let mut document = to_value(&contract).expect("serialize serving contract");
        document["bindings"]["model"]["legacy_pointer"] = Value::Bool(true);
        assert!(from_value::<ModelServingContract>(document).is_err());

        let mut root = to_value(&contract).expect("serialize serving contract");
        root["legacy_contract"] = Value::Bool(true);
        assert!(from_value::<ModelServingContract>(root).is_err());
    }

    #[test]
    fn classical_contracts_are_exact() {
        for kind in [
            ClassicalKind::GradientBoostedTrees,
            ClassicalKind::RandomForest,
            ClassicalKind::ExtraTrees,
            ClassicalKind::LogisticRegression,
            ClassicalKind::Ridge,
            ClassicalKind::Lasso,
            ClassicalKind::ElasticNet,
        ] {
            ModelServingContract::try_seal(ModelServingBindings::classical_fixture(kind))
                .expect("valid classical serving contract");
        }

        let gbdt_bindings =
            ModelServingBindings::classical_fixture(ClassicalKind::GradientBoostedTrees);
        let explanation =
            CandidateExplanationValidation::try_from(&gbdt_bindings).expect("exact TreeSHAP proof");
        assert_eq!(
            explanation.method,
            CandidateExplanationMethod::ExactTreeShap
        );
        let CandidateExplanationVerification::ExactTreeShap {
            verified_case_count,
            max_efficiency_residual,
            max_prediction_residual,
        } = explanation.verification
        else {
            panic!("exact TreeSHAP verification");
        };
        assert_eq!(verified_case_count, 10);
        assert_eq!(max_efficiency_residual, Decimal::ZERO);
        assert_eq!(max_prediction_residual, Decimal::ZERO);

        let mut wrong_format = ModelServingBindings::classical_fixture(ClassicalKind::RandomForest);
        let ModelServingEstimatorBinding::Classical {
            serialization_format,
            ..
        } = &mut wrong_format.model.estimator
        else {
            panic!("classical fixture");
        };
        *serialization_format = ModelSerializationFormat::Json;
        assert!(matches!(
            ModelServingContract::try_seal(wrong_format),
            Err(ModelServingContractError::ClassicalFormatMismatch)
        ));

        let mut missing_tree_shap =
            ModelServingBindings::classical_fixture(ClassicalKind::GradientBoostedTrees);
        let ModelServingEstimatorBinding::Classical { tree_shap, .. } =
            &mut missing_tree_shap.model.estimator
        else {
            panic!("classical fixture");
        };
        *tree_shap = None;
        assert!(matches!(
            ModelServingContract::try_seal(missing_tree_shap),
            Err(ModelServingContractError::ClassicalExplanationMismatch)
        ));

        let mut imprecise_tree_shap =
            ModelServingBindings::classical_fixture(ClassicalKind::GradientBoostedTrees);
        let ModelServingEstimatorBinding::Classical {
            tree_shap: Some(tree_shap),
            ..
        } = &mut imprecise_tree_shap.model.estimator
        else {
            panic!("GBDT TreeSHAP fixture");
        };
        tree_shap.max_prediction_residual = Decimal::ONE;
        assert!(CandidateExplanationValidation::try_from(&imprecise_tree_shap).is_err());

        let mut wrong_family = ModelServingBindings::classical_fixture(ClassicalKind::ExtraTrees);
        wrong_family.model.model_family = ModelFamily::ClassicalRandomForest;
        assert!(matches!(
            ModelServingContract::try_seal(wrong_family),
            Err(ModelServingContractError::EstimatorFamilyMismatch { .. })
        ));

        let mut nonempty_plane =
            ModelServingBindings::classical_fixture(ClassicalKind::RandomForest);
        nonempty_plane.factors.plane = ModelServingBindings::serving_fixture().factors.plane;
        assert!(matches!(
            ModelServingContract::try_seal(nonempty_plane),
            Err(ModelServingContractError::ClassicalFactorPlaneForbidden)
        ));
    }

    #[test]
    fn factor_invariants_fail_closed() {
        let mut wrong_feature = ModelServingBindings::serving_fixture();
        let mut definitions = wrong_feature.factors.plane.definitions().to_vec();
        definitions[0] = factor(
            definitions[0].factor_name().as_str(),
            definitions[0].definition().family,
            hash(41),
        );
        wrong_feature.factors.plane =
            FactorServingPlane::try_seal(definitions).expect("wrong-feature plane");
        assert!(matches!(
            ModelServingContract::try_seal(wrong_feature),
            Err(ModelServingContractError::FactorFeatureMismatch { .. })
        ));

        let mut unknown_factor = ModelServingBindings::serving_fixture();
        let ModelServingEstimatorBinding::FactorNative { ordered_inputs, .. } =
            &mut unknown_factor.model.estimator
        else {
            panic!("factor-native fixture");
        };
        ordered_inputs[0] = ModelServingEstimatorInput::GovernedFactor {
            factor_definition_id: FactorDefinitionId::from_v7(),
        };
        assert!(matches!(
            ModelServingContract::try_seal(unknown_factor),
            Err(ModelServingContractError::UnknownEstimatorFactor { .. })
        ));

        let mut duplicate_factor = ModelServingBindings::serving_fixture();
        let ModelServingEstimatorBinding::FactorNative { ordered_inputs, .. } =
            &mut duplicate_factor.model.estimator
        else {
            panic!("factor-native fixture");
        };
        ordered_inputs.push(ordered_inputs[0].clone());
        assert!(matches!(
            ModelServingContract::try_seal(duplicate_factor),
            Err(ModelServingContractError::DuplicateEstimatorInput { .. })
        ));

        let mut unused_plane_factor = ModelServingBindings::serving_fixture();
        let unused_factor = factor(
            "unused_factor",
            FactorFamily::Momentum,
            unused_plane_factor.schemas.feature_schema_hash,
        );
        let unused_factor_id = unused_factor.factor_definition_id();
        let mut definitions = unused_plane_factor.factors.plane.definitions().to_vec();
        definitions.push(unused_factor);
        unused_plane_factor.factors.plane =
            FactorServingPlane::try_seal(definitions).expect("plane with unused factor");
        unused_plane_factor.dataset.manifest.factor_serving_plane =
            unused_plane_factor.factors.plane.clone();
        unused_plane_factor.refresh_manifest_hash();
        assert!(matches!(
            ModelServingContract::try_seal(unused_plane_factor),
            Err(ModelServingContractError::UnusedPlaneFactor {
                factor_definition_id,
            }) if factor_definition_id == unused_factor_id
        ));

        let mut missing_factor = ModelServingBindings::weighted_fixture();
        let ModelServingEstimatorBinding::FactorNative { ordered_inputs, .. } =
            &mut missing_factor.model.estimator
        else {
            panic!("factor-native fixture");
        };
        ordered_inputs.clear();
        assert!(matches!(
            ModelServingContract::try_seal(missing_factor),
            Err(ModelServingContractError::MissingGovernedFactor)
        ));

        let mut weighted_intrinsic = ModelServingBindings::serving_fixture();
        weighted_intrinsic.model.model_family = ModelFamily::WeightedFactor;
        assert!(matches!(
            ModelServingContract::try_seal(weighted_intrinsic),
            Err(ModelServingContractError::WeightedIntrinsicForbidden)
        ));

        let mut incomplete_sell = ModelServingBindings::serving_fixture();
        let ModelServingEstimatorBinding::FactorNative { ordered_inputs, .. } =
            &mut incomplete_sell.model.estimator
        else {
            panic!("factor-native fixture");
        };
        ordered_inputs.pop();
        assert!(matches!(
            ModelServingContract::try_seal(incomplete_sell),
            Err(ModelServingContractError::SellIntrinsicMismatch)
        ));

        let mut name_collision = ModelServingBindings::serving_fixture();
        let mut definitions = name_collision.factors.plane.definitions().to_vec();
        definitions.push(factor(
            "position_time_in_trade",
            FactorFamily::Momentum,
            name_collision.schemas.feature_schema_hash,
        ));
        name_collision.factors.plane =
            FactorServingPlane::try_seal(definitions).expect("name-collision plane");
        name_collision.dataset.manifest.factor_serving_plane = name_collision.factors.plane.clone();
        name_collision.refresh_manifest_hash();
        assert!(matches!(
            ModelServingContract::try_seal(name_collision),
            Err(ModelServingContractError::IntrinsicNameCollision { .. })
        ));
    }

    #[test]
    fn domain_lineage_follows_plane() {
        let mut mixed = ModelServingBindings::weighted_fixture();
        mixed.model.category_scope = None;
        mixed.required_domain_families.clear();
        mixed.capability_registry_hashes =
            CapabilityRegistryHashes::try_new(Vec::new()).expect("empty capabilities");
        mixed
            .dataset
            .manifest
            .source_lineage
            .capability_registry_hashes =
            CapabilityRegistryHashes::try_new(Vec::new()).expect("empty source capabilities");
        let mut definitions = mixed.factors.plane.definitions().to_vec();
        definitions.push(factor(
            "domain.crypto.test_alpha",
            FactorFamily::DomainCrypto,
            mixed.schemas.feature_schema_hash,
        ));
        mixed.factors.plane =
            FactorServingPlane::try_seal(definitions).expect("mixed serving plane");
        mixed.dataset.manifest.factor_serving_plane = mixed.factors.plane.clone();
        mixed.refresh_manifest_hash();
        assert!(matches!(
            ModelServingContract::try_seal(mixed),
            Err(ModelServingContractError::MissingFactorDomain {
                family: DomainFamily::Crypto,
            })
        ));

        let mut generic = ModelServingBindings::weighted_fixture();
        generic.model.category_scope = None;
        generic.required_domain_families.clear();
        generic.capability_registry_hashes =
            CapabilityRegistryHashes::try_new(Vec::new()).expect("empty capabilities");
        generic
            .dataset
            .manifest
            .source_lineage
            .capability_registry_hashes =
            CapabilityRegistryHashes::try_new(Vec::new()).expect("empty source capabilities");
        generic.refresh_manifest_hash();
        assert!(ModelServingContract::try_seal(generic).is_ok());
    }

    #[test]
    fn lineage_invariants_fail_closed() {
        let mut wrong_profile_kind = ModelServingBindings::serving_fixture();
        wrong_profile_kind
            .policy_snapshot
            .profile_artifacts
            .features
            .kind = ProfileArtifactKind::Scoring;
        assert!(matches!(
            ModelServingContract::try_seal(wrong_profile_kind),
            Err(ModelServingContractError::ProfileKindMismatch { .. })
        ));

        let mut wrong_profile_id = ModelServingBindings::serving_fixture();
        wrong_profile_id
            .policy_snapshot
            .profile_artifacts
            .features
            .profile_artifact_id = ProfileArtifactId::from_v7();
        assert!(matches!(
            ModelServingContract::try_seal(wrong_profile_id),
            Err(ModelServingContractError::ProfileIdentityMismatch { .. })
        ));

        let mut missing_capabilities = ModelServingBindings::serving_fixture();
        missing_capabilities.capability_registry_hashes =
            CapabilityRegistryHashes::try_new(Vec::new()).expect("empty capability set");
        assert!(matches!(
            ModelServingContract::try_seal(missing_capabilities),
            Err(ModelServingContractError::MissingCategoryCapabilities { .. })
        ));

        let mut wrong_schema = ModelServingBindings::serving_fixture();
        wrong_schema.schemas.label_schema_hash = hash(42);
        assert!(matches!(
            ModelServingContract::try_seal(wrong_schema),
            Err(ModelServingContractError::SchemaBindingMismatch { .. })
        ));

        let mut wrong_spec = ModelServingBindings::serving_fixture();
        wrong_spec.model.model_spec_id = ModelSpecId::from_v7();
        assert!(matches!(
            ModelServingContract::try_seal(wrong_spec),
            Err(ModelServingContractError::ModelSpecMismatch)
        ));

        let mut wrong_spec_hash = ModelServingBindings::serving_fixture();
        wrong_spec_hash.model.model_spec_definition_hash = hash(48);
        assert!(matches!(
            ModelServingContract::try_seal(wrong_spec_hash),
            Err(ModelServingContractError::ModelSpecMismatch)
        ));

        let mut wrong_profile = ModelServingBindings::serving_fixture();
        wrong_profile.model.profile_ref.content_hash = hash(43);
        assert!(matches!(
            ModelServingContract::try_seal(wrong_profile),
            Err(ModelServingContractError::ResearchProfileMismatch)
        ));

        let mut wrong_snapshot = ModelServingBindings::serving_fixture();
        let alternate_snapshot_hash = hash(49);
        wrong_snapshot.policy_snapshot.decision_policy_snapshot_id =
            DecisionPolicySnapshotId::from_content_hash(&alternate_snapshot_hash);
        wrong_snapshot.policy_snapshot.snapshot_hash = alternate_snapshot_hash;
        assert!(matches!(
            ModelServingContract::try_seal(wrong_snapshot),
            Err(ModelServingContractError::PolicySnapshotMismatch)
        ));

        let mut forged_snapshot = ModelServingBindings::serving_fixture();
        let forged_snapshot_id = DecisionPolicySnapshotId::from_v7();
        let forged_snapshot_hash = hash(61);
        forged_snapshot.policy_snapshot.decision_policy_snapshot_id = forged_snapshot_id;
        forged_snapshot.policy_snapshot.snapshot_hash = forged_snapshot_hash;
        forged_snapshot
            .dataset
            .manifest
            .source_lineage
            .decision_policy_snapshot_id = forged_snapshot_id;
        forged_snapshot
            .dataset
            .manifest
            .source_lineage
            .runtime_config_hash = forged_snapshot_hash;
        forged_snapshot.refresh_manifest_hash();
        assert!(matches!(
            ModelServingContract::try_seal(forged_snapshot),
            Err(ModelServingContractError::PolicySnapshotIdentityMismatch)
        ));

        let mut wrong_capabilities = ModelServingBindings::serving_fixture();
        wrong_capabilities.capability_registry_hashes =
            CapabilityRegistryHashes::try_new(vec![hash(44)]).expect("alternate capabilities");
        assert!(matches!(
            ModelServingContract::try_seal(wrong_capabilities),
            Err(ModelServingContractError::CapabilityBindingMismatch)
        ));

        let mut wrong_dataset = ModelServingBindings::serving_fixture();
        wrong_dataset.transform.training_dataset_hash = hash(45);
        assert!(matches!(
            ModelServingContract::try_seal(wrong_dataset),
            Err(ModelServingContractError::TrainingDatasetMismatch)
        ));

        let mut wrong_factor_schema = ModelServingBindings::serving_fixture();
        wrong_factor_schema.dataset.manifest.factor_serving_plane =
            FactorServingPlane::try_seal(vec![factor(
                "alternate_factor",
                FactorFamily::Momentum,
                wrong_factor_schema.schemas.feature_schema_hash,
            )])
            .expect("alternate dataset factor plane");
        wrong_factor_schema.refresh_manifest_hash();
        assert!(matches!(
            ModelServingContract::try_seal(wrong_factor_schema),
            Err(ModelServingContractError::SchemaBindingMismatch { binding: "factor" })
        ));

        let mut wrong_dataset_family = ModelServingBindings::serving_fixture();
        wrong_dataset_family.dataset.manifest.model_family = ModelFamily::WeightedFactor;
        wrong_dataset_family.refresh_manifest_hash();
        assert!(matches!(
            ModelServingContract::try_seal(wrong_dataset_family),
            Err(ModelServingContractError::DatasetModelFamilyMismatch)
        ));

        let mut wrong_purpose = ModelServingBindings::serving_fixture();
        wrong_purpose.dataset.manifest.purpose = DatasetPurpose::PolicyFit;
        assert!(matches!(
            ModelServingContract::try_seal(wrong_purpose),
            Err(ModelServingContractError::InvalidDatasetPurpose { .. })
        ));

        let mut empty_dataset = ModelServingBindings::serving_fixture();
        empty_dataset.dataset.manifest.sample_count = 0;
        assert!(matches!(
            ModelServingContract::try_seal(empty_dataset),
            Err(ModelServingContractError::EmptyTrainingDataset)
        ));

        let mut wrong_manifest_hash = ModelServingBindings::serving_fixture();
        wrong_manifest_hash.dataset.manifest_hash = hash(46);
        assert!(matches!(
            ModelServingContract::try_seal(wrong_manifest_hash),
            Err(ModelServingContractError::DatasetManifestHashMismatch { .. })
        ));

        let mut zero_horizon = ModelServingBindings::serving_fixture();
        zero_horizon.dataset.manifest.horizons_secs = vec![0];
        zero_horizon.refresh_manifest_hash();
        ModelServingContract::try_seal(zero_horizon)
            .expect("horizon-independent payout-label dataset");

        let mut missing_horizon = ModelServingBindings::serving_fixture();
        missing_horizon.model.prediction_horizon_secs = 0;
        assert!(matches!(
            ModelServingContract::try_seal(missing_horizon),
            Err(ModelServingContractError::PredictionHorizonMismatch)
        ));
    }

    #[test]
    fn artifact_bindings_fail_closed() {
        let mut calibrated = ModelServingBindings::weighted_fixture();
        calibrated.model.calibration = Some(calibration_ref(CalibrationKind::ModelScore, 50));
        ModelServingContract::try_seal(calibrated).expect("weighted calibrated contract");

        let mut wrong_family = ModelServingBindings::serving_fixture();
        wrong_family.model.calibration = Some(calibration_ref(CalibrationKind::ModelScore, 51));
        assert!(matches!(
            ModelServingContract::try_seal(wrong_family),
            Err(ModelServingContractError::CalibrationFamilyMismatch)
        ));

        let mut wrong_calibration_kind = ModelServingBindings::weighted_fixture();
        wrong_calibration_kind.model.calibration =
            Some(calibration_ref(CalibrationKind::MarketPriceBias, 52));
        assert!(matches!(
            ModelServingContract::try_seal(wrong_calibration_kind),
            Err(ModelServingContractError::CalibrationKindMismatch)
        ));

        let mut unused_bias = ModelServingBindings::weighted_fixture();
        unused_bias.factors.bias_table =
            Some(calibration_ref(CalibrationKind::MarketPriceBias, 53));
        assert!(matches!(
            ModelServingContract::try_seal(unused_bias),
            Err(ModelServingContractError::UnusedBiasTable)
        ));

        let mut bias_bound = ModelServingBindings::weighted_fixture();
        bias_bound.add_bias_factor();
        bias_bound.factors.bias_table = Some(calibration_ref(CalibrationKind::MarketPriceBias, 54));
        ModelServingContract::try_seal(bias_bound).expect("bias-bound serving contract");

        let mut role_collision = ModelServingBindings::weighted_fixture();
        role_collision.add_bias_factor();
        let calibration = calibration_ref(CalibrationKind::ModelScore, 55);
        role_collision.model.calibration = Some(calibration.clone());
        role_collision.factors.bias_table = Some(ModelServingCalibrationArtifactRef {
            artifact_id: calibration.artifact_id,
            kind: CalibrationKind::MarketPriceBias,
            content_hash: calibration.content_hash,
        });
        assert!(matches!(
            ModelServingContract::try_seal(role_collision),
            Err(ModelServingContractError::CalibrationRoleCollision)
        ));

        let mut policy_bound = ModelServingBindings::serving_fixture();
        let policy_hash = hash(56);
        let policy_id = TradePolicyArtifactId::from_content_hash(&policy_hash);
        policy_bound.dataset.manifest.trade_policy_artifact_id = Some(policy_id);
        policy_bound.dataset.manifest.trade_policy_hash = Some(policy_hash);
        policy_bound.trade_policy = Some(ModelServingTradePolicyBinding {
            artifact_id: policy_id,
            content_hash: policy_hash,
        });
        policy_bound.refresh_manifest_hash();
        ModelServingContract::try_seal(policy_bound).expect("policy-bound serving contract");

        let mut forged_policy = ModelServingBindings::serving_fixture();
        let forged_hash = hash(57);
        let forged_id = TradePolicyArtifactId::from_v7();
        forged_policy.dataset.manifest.trade_policy_artifact_id = Some(forged_id);
        forged_policy.dataset.manifest.trade_policy_hash = Some(forged_hash);
        forged_policy.trade_policy = Some(ModelServingTradePolicyBinding {
            artifact_id: forged_id,
            content_hash: forged_hash,
        });
        forged_policy.refresh_manifest_hash();
        assert!(matches!(
            ModelServingContract::try_seal(forged_policy),
            Err(ModelServingContractError::TradePolicyIdentityMismatch)
        ));

        let mut missing_policy = ModelServingBindings::serving_fixture();
        let missing_hash = hash(60);
        missing_policy.dataset.manifest.trade_policy_artifact_id =
            Some(TradePolicyArtifactId::from_content_hash(&missing_hash));
        missing_policy.dataset.manifest.trade_policy_hash = Some(missing_hash);
        missing_policy.refresh_manifest_hash();
        assert!(matches!(
            ModelServingContract::try_seal(missing_policy),
            Err(ModelServingContractError::TradePolicyBindingMismatch)
        ));
    }

    #[test]
    fn intrinsic_documents_fail_closed() {
        let binding: ModelServingIntrinsicInputRef =
            ModelServingIntrinsicInputKind::PositionTimeInTrade
                .try_into()
                .expect("canonical intrinsic input");

        let mut wrong_version = to_value(&binding).expect("serialize intrinsic input");
        let predecessor = MODEL_INTRINSIC_INPUT_VERSION
            .checked_sub(1)
            .expect("intrinsic input version has a predecessor");
        wrong_version["semantic_version"] = Value::Number(u64::from(predecessor).into());
        assert!(from_value::<ModelServingIntrinsicInputRef>(wrong_version).is_err());

        let mut wrong_hash = to_value(&binding).expect("serialize intrinsic input");
        wrong_hash["definition_hash"] = Value::String(hash(58).to_string());
        assert!(from_value::<ModelServingIntrinsicInputRef>(wrong_hash).is_err());

        let mut unknown = to_value(&binding).expect("serialize intrinsic input");
        unknown["legacy_formula"] = Value::Bool(true);
        assert!(from_value::<ModelServingIntrinsicInputRef>(unknown).is_err());
    }
}
