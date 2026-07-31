//! Governed model-route promotion permit contracts.

use chrono::{DateTime, Utc};
use quant_pivot_error::feedback::FeedbackError;
use sea_orm::{ActiveValue, DeriveIntoActiveModel, DerivePartialModel, IntoActiveValue};
use serde::{Deserialize, Serialize, Serializer};

use super::model::ModelVersionInfo;
use crate::{
    enums::{common::MarketCategory, model::ModelFamily, quant::QuantRuntimeMode},
    hashing::CanonicalDigest,
    runtime_config::{
        ActivePolicyBundle, BuyModelRoute, DecisionPolicySnapshot, DecisionPolicySnapshotDocument,
        ModelVersionRef,
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, FeatureParityRunId, FeatureParityStateId,
        FeedbackCycleId, FeedbackDecisionArtifactId, FeedbackShadowArtifactId,
        ModelCandidateManifestId, ModelSpecId, ModelVersionId, PolicyActivationId,
        PolicyApprovalId, PolicyBundleGeneration, PolicyIdempotencyKey, PolicyRevisionId,
        PromotionPermitId, ResearchProfileArtifactId, ResearchProfileRef, RoleCode,
        TrainingDatasetId, UserId,
    },
};

const PROMOTION_PERMIT_SCOPE_VERSION: u32 = 5;
const PROMOTION_PERMIT_SCOPE_DOMAIN: &str = "quant-pivot/promotion-permit-scope";
const PROMOTION_PERMIT_ISSUANCE_VERSION: u32 = 1;
const PROMOTION_PERMIT_ISSUANCE_DOMAIN: &str = "quant-pivot/promotion-permit-issuance";
const PROMOTION_NON_ROUTE_VERSION: u32 = 1;
const PROMOTION_NON_ROUTE_DOMAIN: &str = "quant-pivot/promotion-non-route-policy";
const PROMOTION_SERVING_VERSION: u32 = 4;
const PROMOTION_SERVING_DOMAIN: &str = "quant-pivot/promotion-serving-constraints";
const PROMOTION_PREFLIGHT_VERSION: u32 = 1;
const PROMOTION_PREFLIGHT_DOMAIN: &str = "quant-pivot/promotion-preflight";
const PROMOTION_TRANSACTION_VERSION: u32 = 2;
const PROMOTION_TRANSACTION_DOMAIN: &str = "quant-pivot/model-route-promotion";
const MAX_ACTOR_BYTES: usize = 256;
const MAX_REASON_BYTES: usize = 2_048;
const MAX_REASON_CODE_BYTES: usize = 128;
const MAX_ROLE_BYTES: usize = 64;

/// Canonically ordered, non-empty runtime-mode authority persisted as a
/// native `PostgreSQL` enum array.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PromotionRuntimeModes(Vec<QuantRuntimeMode>);

impl PromotionRuntimeModes {
    fn try_new(modes: Vec<QuantRuntimeMode>) -> Result<Self, FeedbackError> {
        validate_modes(&modes)?;
        Ok(Self(modes))
    }

    fn as_slice(&self) -> &[QuantRuntimeMode] {
        &self.0
    }
}

impl Serialize for PromotionRuntimeModes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl IntoActiveValue<Vec<QuantRuntimeMode>> for PromotionRuntimeModes {
    fn into_active_value(self) -> ActiveValue<Vec<QuantRuntimeMode>> {
        ActiveValue::Set(self.0)
    }
}

/// Immutable, exact authority boundary of one promotion permit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "PromotionPermitScopeDocument")]
pub struct PromotionPermitScope {
    format_version: u32,
    feedback_cycle_id: FeedbackCycleId,
    profile_ref: ResearchProfileRef,
    category: MarketCategory,
    expected_policy_generation: PolicyBundleGeneration,
    expected_runtime_control_revision: i64,
    expected_decision_policy_snapshot_id: DecisionPolicySnapshotId,
    expected_snapshot_hash: ContentHash,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    candidate_model_version_id: ModelVersionId,
    candidate_manifest_id: ModelCandidateManifestId,
    candidate_manifest_hash: ContentHash,
    promotion_gate_hash: ContentHash,
    allowed_runtime_modes: PromotionRuntimeModes,
    non_route_policy_hash: ContentHash,
    serving_constraints_hash: ContentHash,
    expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct PromotionPermitScopeDocument {
    format_version: u32,
    feedback_cycle_id: FeedbackCycleId,
    profile_ref: ResearchProfileRef,
    category: MarketCategory,
    expected_policy_generation: PolicyBundleGeneration,
    expected_runtime_control_revision: i64,
    expected_decision_policy_snapshot_id: DecisionPolicySnapshotId,
    expected_snapshot_hash: ContentHash,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    candidate_model_version_id: ModelVersionId,
    candidate_manifest_id: ModelCandidateManifestId,
    candidate_manifest_hash: ContentHash,
    promotion_gate_hash: ContentHash,
    allowed_runtime_modes: Vec<QuantRuntimeMode>,
    non_route_policy_hash: ContentHash,
    serving_constraints_hash: ContentHash,
    expires_at: DateTime<Utc>,
}

/// Server-frozen inputs for one exact permit scope.
#[derive(Debug, Clone)]
pub struct PromotionPermitScopeInput {
    pub feedback_cycle_id: FeedbackCycleId,
    pub profile_ref: ResearchProfileRef,
    pub category: MarketCategory,
    pub expected_policy_generation: PolicyBundleGeneration,
    pub expected_runtime_control_revision: i64,
    pub expected_decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub expected_snapshot_hash: ContentHash,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_model_version_id: ModelVersionId,
    pub candidate_manifest_id: ModelCandidateManifestId,
    pub candidate_manifest_hash: ContentHash,
    pub promotion_gate_hash: ContentHash,
    pub allowed_runtime_modes: Vec<QuantRuntimeMode>,
    pub non_route_policy_hash: ContentHash,
    pub serving_constraints_hash: ContentHash,
    pub expires_at: DateTime<Utc>,
}

impl PromotionPermitScope {
    /// Validate and freeze the complete route authority boundary.
    pub fn try_new(input: PromotionPermitScopeInput) -> Result<Self, FeedbackError> {
        let scope = Self {
            format_version: PROMOTION_PERMIT_SCOPE_VERSION,
            feedback_cycle_id: input.feedback_cycle_id,
            profile_ref: input.profile_ref,
            category: input.category,
            expected_policy_generation: input.expected_policy_generation,
            expected_runtime_control_revision: input.expected_runtime_control_revision,
            expected_decision_policy_snapshot_id: input.expected_decision_policy_snapshot_id,
            expected_snapshot_hash: input.expected_snapshot_hash,
            champion_model_version_id: input.champion_model_version_id,
            champion_serving_contract_hash: input.champion_serving_contract_hash,
            candidate_model_version_id: input.candidate_model_version_id,
            candidate_manifest_id: input.candidate_manifest_id,
            candidate_manifest_hash: input.candidate_manifest_hash,
            promotion_gate_hash: input.promotion_gate_hash,
            allowed_runtime_modes: PromotionRuntimeModes::try_new(input.allowed_runtime_modes)?,
            non_route_policy_hash: input.non_route_policy_hash,
            serving_constraints_hash: input.serving_constraints_hash,
            expires_at: input.expires_at,
        };
        scope.validate()?;
        Ok(scope)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        if self.format_version != PROMOTION_PERMIT_SCOPE_VERSION {
            return Err(invalid_permit(format!(
                "unsupported scope version {}; expected {PROMOTION_PERMIT_SCOPE_VERSION}",
                self.format_version
            )));
        }
        self.profile_ref
            .validate()
            .map_err(|error| invalid_permit(error.to_string()))?;
        if !matches!(
            self.category,
            MarketCategory::Crypto | MarketCategory::Weather
        ) {
            return Err(invalid_permit(
                "permit category must be an exact Crypto or Weather route",
            ));
        }
        let profile = self
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(invalid_permit)?;
        if profile.spec.category != Some(self.category) {
            return Err(invalid_permit(format!(
                "profile {} does not own category {}",
                self.profile_ref.id, self.category
            )));
        }
        if self.expected_runtime_control_revision < 0 {
            return Err(invalid_permit(
                "expected runtime-control revision cannot be negative",
            ));
        }
        if self.expected_decision_policy_snapshot_id
            != DecisionPolicySnapshotId::from_content_hash(&self.expected_snapshot_hash)
        {
            return Err(invalid_permit(
                "expected policy snapshot ID does not match its content hash",
            ));
        }
        if self.champion_model_version_id == self.candidate_model_version_id {
            return Err(invalid_permit(
                "permit candidate must differ from the current route champion",
            ));
        }
        if self.candidate_manifest_id
            != ModelCandidateManifestId::from_content_hash(&self.candidate_manifest_hash)
        {
            return Err(invalid_permit(
                "candidate manifest ID does not match its content hash",
            ));
        }
        validate_modes(&self.allowed_runtime_modes.0)
    }

    /// Domain-separated digest of every immutable route-authority field.
    pub fn scope_hash(&self) -> Result<ContentHash, FeedbackError> {
        self.validate()?;
        CanonicalDigest::content_hash_typed(
            PROMOTION_PERMIT_SCOPE_DOMAIN,
            PROMOTION_PERMIT_SCOPE_VERSION,
            self,
        )
        .map_err(FeedbackError::from)
    }

    #[must_use]
    pub const fn profile_ref(&self) -> &ResearchProfileRef {
        &self.profile_ref
    }

    #[must_use]
    pub const fn feedback_cycle_id(&self) -> FeedbackCycleId {
        self.feedback_cycle_id
    }

    #[must_use]
    pub const fn category(&self) -> MarketCategory {
        self.category
    }

    #[must_use]
    pub const fn expected_policy_generation(&self) -> PolicyBundleGeneration {
        self.expected_policy_generation
    }

    #[must_use]
    pub const fn expected_runtime_control_revision(&self) -> i64 {
        self.expected_runtime_control_revision
    }

    #[must_use]
    pub const fn expected_snapshot_id(&self) -> DecisionPolicySnapshotId {
        self.expected_decision_policy_snapshot_id
    }

    #[must_use]
    pub const fn expected_snapshot_hash(&self) -> ContentHash {
        self.expected_snapshot_hash
    }

    #[must_use]
    pub const fn champion_model_version_id(&self) -> ModelVersionId {
        self.champion_model_version_id
    }

    #[must_use]
    pub const fn champion_serving_contract_hash(&self) -> ContentHash {
        self.champion_serving_contract_hash
    }

    #[must_use]
    pub const fn candidate_model_version_id(&self) -> ModelVersionId {
        self.candidate_model_version_id
    }

    #[must_use]
    pub const fn candidate_manifest_id(&self) -> ModelCandidateManifestId {
        self.candidate_manifest_id
    }

    #[must_use]
    pub const fn candidate_manifest_hash(&self) -> ContentHash {
        self.candidate_manifest_hash
    }

    #[must_use]
    pub const fn promotion_gate_hash(&self) -> ContentHash {
        self.promotion_gate_hash
    }

    #[must_use]
    pub fn allowed_runtime_modes(&self) -> &[QuantRuntimeMode] {
        self.allowed_runtime_modes.as_slice()
    }

    #[must_use]
    pub const fn non_route_policy_hash(&self) -> ContentHash {
        self.non_route_policy_hash
    }

    #[must_use]
    pub const fn serving_constraints_hash(&self) -> ContentHash {
        self.serving_constraints_hash
    }

    #[must_use]
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }

    /// The only policy field this scope can authorize.
    pub fn field_mask(&self) -> Result<&'static str, FeedbackError> {
        match self.category {
            MarketCategory::Crypto => Ok("model.category_model_pointers.crypto"),
            MarketCategory::Weather => Ok("model.category_model_pointers.weather"),
            _ => Err(invalid_permit(
                "permit category does not own a promotable model route",
            )),
        }
    }

    #[must_use]
    pub fn allows_mode(&self, mode: QuantRuntimeMode) -> bool {
        self.allowed_runtime_modes.0.contains(&mode)
    }

    /// Revalidate the exact champion artifact projection bound by this scope.
    ///
    /// Champion authority is derived exclusively from the active route
    /// generation; a mutable model-global publication status is deliberately
    /// outside this contract.
    pub fn validate_champion(&self, model: &ModelVersionInfo) -> Result<(), FeedbackError> {
        self.validate()?;
        model
            .verified_serving_contract()
            .map_err(|error| invalid_preflight(error.to_string()))?;
        if model.model_version_id != self.champion_model_version_id
            || model.serving_contract_hash != self.champion_serving_contract_hash
            || model.profile_ref != self.profile_ref
            || model.category_scope != Some(self.category)
        {
            return Err(invalid_preflight(
                "champion model differs from the permit serving projection",
            ));
        }
        Ok(())
    }
}

impl TryFrom<PromotionPermitScopeDocument> for PromotionPermitScope {
    type Error = FeedbackError;

    fn try_from(document: PromotionPermitScopeDocument) -> Result<Self, Self::Error> {
        let scope = Self {
            format_version: document.format_version,
            feedback_cycle_id: document.feedback_cycle_id,
            profile_ref: document.profile_ref,
            category: document.category,
            expected_policy_generation: document.expected_policy_generation,
            expected_runtime_control_revision: document.expected_runtime_control_revision,
            expected_decision_policy_snapshot_id: document.expected_decision_policy_snapshot_id,
            expected_snapshot_hash: document.expected_snapshot_hash,
            champion_model_version_id: document.champion_model_version_id,
            champion_serving_contract_hash: document.champion_serving_contract_hash,
            candidate_model_version_id: document.candidate_model_version_id,
            candidate_manifest_id: document.candidate_manifest_id,
            candidate_manifest_hash: document.candidate_manifest_hash,
            promotion_gate_hash: document.promotion_gate_hash,
            allowed_runtime_modes: PromotionRuntimeModes::try_new(document.allowed_runtime_modes)?,
            non_route_policy_hash: document.non_route_policy_hash,
            serving_constraints_hash: document.serving_constraints_hash,
            expires_at: document.expires_at,
        };
        scope.validate()?;
        Ok(scope)
    }
}

#[derive(Serialize)]
struct PromotionNonRouteDocument<'a> {
    format_version: u32,
    category: MarketCategory,
    snapshot: &'a DecisionPolicySnapshotDocument,
}

/// Exact policy delta authorized by one category-model promotion.
///
/// The target category pointer and the exact global shadow pointer being
/// consumed are normalized out of the non-route digest. The `ModelRouting`
/// revision identity is also normalized because P04 must create a new revision
/// for this exact document. Every other policy field remains hash-bound.
#[derive(Debug, Clone)]
pub struct PromotionPolicyProjection {
    category: MarketCategory,
    champion_model_version_id: ModelVersionId,
    candidate_model_version_id: ModelVersionId,
    non_route_policy_hash: ContentHash,
    prospective_snapshot: DecisionPolicySnapshot,
}

impl PromotionPolicyProjection {
    /// Build the sole permitted champion/shadow-to-category transition.
    pub fn try_new(
        bundle: &ActivePolicyBundle,
        category: MarketCategory,
        candidate_model_version_id: ModelVersionId,
    ) -> Result<Self, FeedbackError> {
        let actual_hash = bundle
            .snapshot
            .persistence_hash()
            .map_err(|error| invalid_preflight(error.to_string()))?;
        if actual_hash != bundle.snapshot_hash
            || bundle.decision_policy_snapshot_id
                != DecisionPolicySnapshotId::from_content_hash(&actual_hash)
            || bundle.revision_vector != bundle.snapshot.revisions
        {
            return Err(invalid_preflight(
                "active policy bundle identity, hash, or revision vector is invalid",
            ));
        }
        let route = BuyModelRoute::try_from(Some(category))
            .map_err(|error| invalid_preflight(error.to_string()))?;
        let model = &bundle.snapshot.model_routing.model;
        let champion_model_version_id = model
            .active_pointer(route)
            .map_err(|error| invalid_preflight(error.to_string()))?
            .id;
        if champion_model_version_id == candidate_model_version_id {
            return Err(invalid_preflight(
                "promotion candidate already owns the target category route",
            ));
        }
        if model
            .shadow_model_version_id
            .as_ref()
            .map(|reference| reference.id)
            != Some(candidate_model_version_id)
        {
            return Err(invalid_preflight(
                "promotion candidate must be the exact current global shadow pointer",
            ));
        }
        let candidate_is_other_route = model
            .active_model_version_id
            .as_ref()
            .is_some_and(|reference| reference.id == candidate_model_version_id)
            || model
                .active_exit_model_version_id
                .as_ref()
                .is_some_and(|reference| reference.id == candidate_model_version_id)
            || model
                .category_model_pointers
                .iter()
                .any(|(other, reference)| {
                    *other != category && reference.id == candidate_model_version_id
                });
        if candidate_is_other_route {
            return Err(invalid_preflight(
                "promotion candidate is already referenced by another active route",
            ));
        }

        let non_route_policy_hash =
            Self::project_hash(&bundle.snapshot, category).map_err(invalid_preflight)?;
        let mut prospective_snapshot = bundle.snapshot.clone();
        prospective_snapshot
            .model_routing
            .model
            .category_model_pointers
            .insert(category, ModelVersionRef::new(candidate_model_version_id));
        prospective_snapshot
            .model_routing
            .model
            .shadow_model_version_id = None;
        let prospective_hash =
            Self::project_hash(&prospective_snapshot, category).map_err(invalid_preflight)?;
        if prospective_hash != non_route_policy_hash {
            return Err(invalid_preflight(
                "prospective promotion changed non-route policy fields",
            ));
        }
        Ok(Self {
            category,
            champion_model_version_id,
            candidate_model_version_id,
            non_route_policy_hash,
            prospective_snapshot,
        })
    }

    fn project_hash(
        snapshot: &DecisionPolicySnapshot,
        category: MarketCategory,
    ) -> Result<ContentHash, String> {
        let mut document = snapshot
            .persistence_document()
            .map_err(|error| error.to_string())?;
        if document
            .model_routing
            .model
            .category_model_pointers
            .remove(&category)
            .is_none()
        {
            return Err(format!(
                "policy has no exact {category} category pointer to project"
            ));
        }
        document.model_routing.model.shadow_model_version_id = None;
        document.revisions.model_routing = None;
        CanonicalDigest::content_hash_typed(
            PROMOTION_NON_ROUTE_DOMAIN,
            PROMOTION_NON_ROUTE_VERSION,
            &PromotionNonRouteDocument {
                format_version: PROMOTION_NON_ROUTE_VERSION,
                category,
                snapshot: &document,
            },
        )
        .map_err(|error| error.to_string())
    }

    /// Revalidate a committed candidate snapshot against the exact authorized
    /// route and shadow-consumption delta.
    pub fn validate_candidate(
        &self,
        candidate: &DecisionPolicySnapshot,
    ) -> Result<(), FeedbackError> {
        let route = BuyModelRoute::try_from(Some(self.category))
            .map_err(|error| invalid_preflight(error.to_string()))?;
        let pointer = candidate
            .model_routing
            .model
            .active_pointer(route)
            .map_err(|error| invalid_preflight(error.to_string()))?;
        if pointer.id != self.candidate_model_version_id
            || candidate
                .model_routing
                .model
                .shadow_model_version_id
                .is_some()
            || Self::project_hash(candidate, self.category).map_err(invalid_preflight)?
                != self.non_route_policy_hash
        {
            return Err(invalid_preflight(
                "candidate snapshot differs from the exact category promotion delta",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub const fn category(&self) -> MarketCategory {
        self.category
    }

    #[must_use]
    pub const fn champion_model_version_id(&self) -> ModelVersionId {
        self.champion_model_version_id
    }

    #[must_use]
    pub const fn candidate_model_version_id(&self) -> ModelVersionId {
        self.candidate_model_version_id
    }

    #[must_use]
    pub const fn non_route_policy_hash(&self) -> ContentHash {
        self.non_route_policy_hash
    }

    #[must_use]
    pub const fn prospective_snapshot(&self) -> &DecisionPolicySnapshot {
        &self.prospective_snapshot
    }
}

/// Immutable model/artifact plane that must remain exact from permit issue
/// through the promotion transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "PromotionServingConstraintsDocument")]
pub struct PromotionServingConstraints {
    format_version: u32,
    candidate_model_version_id: ModelVersionId,
    candidate_manifest_id: ModelCandidateManifestId,
    candidate_manifest_hash: ContentHash,
    promotion_gate_hash: ContentHash,
    candidate_model_spec_id: ModelSpecId,
    candidate_model_family: ModelFamily,
    candidate_artifact_hash: ContentHash,
    candidate_serving_contract_hash: ContentHash,
    candidate_model_spec_definition_hash: ContentHash,
    candidate_training_dataset_id: TrainingDatasetId,
    feature_parity_run_id: FeatureParityRunId,
    feature_parity_state_id: FeatureParityStateId,
    feature_parity_evidence_hash: ContentHash,
    profile_ref: ResearchProfileRef,
    category: MarketCategory,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct PromotionServingConstraintsDocument {
    format_version: u32,
    candidate_model_version_id: ModelVersionId,
    candidate_manifest_id: ModelCandidateManifestId,
    candidate_manifest_hash: ContentHash,
    promotion_gate_hash: ContentHash,
    candidate_model_spec_id: ModelSpecId,
    candidate_model_family: ModelFamily,
    candidate_artifact_hash: ContentHash,
    candidate_serving_contract_hash: ContentHash,
    candidate_model_spec_definition_hash: ContentHash,
    candidate_training_dataset_id: TrainingDatasetId,
    feature_parity_run_id: FeatureParityRunId,
    feature_parity_state_id: FeatureParityStateId,
    feature_parity_evidence_hash: ContentHash,
    profile_ref: ResearchProfileRef,
    category: MarketCategory,
}

/// Server-resolved inputs for one immutable candidate serving plane.
pub struct PromotionServingConstraintsInput {
    pub candidate_model_version_id: ModelVersionId,
    pub candidate_manifest_id: ModelCandidateManifestId,
    pub candidate_manifest_hash: ContentHash,
    pub promotion_gate_hash: ContentHash,
    pub candidate_model_spec_id: ModelSpecId,
    pub candidate_model_family: ModelFamily,
    pub candidate_artifact_hash: ContentHash,
    pub candidate_serving_contract_hash: ContentHash,
    pub candidate_model_spec_definition_hash: ContentHash,
    pub candidate_training_dataset_id: TrainingDatasetId,
    pub feature_parity_run_id: FeatureParityRunId,
    pub feature_parity_state_id: FeatureParityStateId,
    pub feature_parity_evidence_hash: ContentHash,
    pub profile_ref: ResearchProfileRef,
    pub category: MarketCategory,
}

impl PromotionServingConstraints {
    pub fn try_new(input: PromotionServingConstraintsInput) -> Result<Self, FeedbackError> {
        let constraints = Self {
            format_version: PROMOTION_SERVING_VERSION,
            candidate_model_version_id: input.candidate_model_version_id,
            candidate_manifest_id: input.candidate_manifest_id,
            candidate_manifest_hash: input.candidate_manifest_hash,
            promotion_gate_hash: input.promotion_gate_hash,
            candidate_model_spec_id: input.candidate_model_spec_id,
            candidate_model_family: input.candidate_model_family,
            candidate_artifact_hash: input.candidate_artifact_hash,
            candidate_serving_contract_hash: input.candidate_serving_contract_hash,
            candidate_model_spec_definition_hash: input.candidate_model_spec_definition_hash,
            candidate_training_dataset_id: input.candidate_training_dataset_id,
            feature_parity_run_id: input.feature_parity_run_id,
            feature_parity_state_id: input.feature_parity_state_id,
            feature_parity_evidence_hash: input.feature_parity_evidence_hash,
            profile_ref: input.profile_ref,
            category: input.category,
        };
        constraints.validate()?;
        Ok(constraints)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.profile_ref
            .validate()
            .map_err(|error| invalid_preflight(error.to_string()))?;
        let profile = self
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(invalid_preflight)?;
        if self.format_version != PROMOTION_SERVING_VERSION
            || !matches!(
                self.candidate_model_family,
                ModelFamily::WeightedFactor | ModelFamily::ClassicalGradientBoostedTrees
            )
            || self.candidate_manifest_id
                != ModelCandidateManifestId::from_content_hash(&self.candidate_manifest_hash)
            || profile.spec.category != Some(self.category)
            || !matches!(
                self.category,
                MarketCategory::Crypto | MarketCategory::Weather
            )
        {
            return Err(invalid_preflight(
                "candidate serving family, profile, or category is not promotable",
            ));
        }
        Ok(())
    }

    pub fn constraints_hash(&self) -> Result<ContentHash, FeedbackError> {
        self.validate()?;
        CanonicalDigest::content_hash_typed(
            PROMOTION_SERVING_DOMAIN,
            PROMOTION_SERVING_VERSION,
            self,
        )
        .map_err(Into::into)
    }

    #[must_use]
    pub const fn candidate_model_version_id(&self) -> ModelVersionId {
        self.candidate_model_version_id
    }

    #[must_use]
    pub const fn candidate_manifest_id(&self) -> ModelCandidateManifestId {
        self.candidate_manifest_id
    }

    #[must_use]
    pub const fn candidate_manifest_hash(&self) -> ContentHash {
        self.candidate_manifest_hash
    }

    #[must_use]
    pub const fn promotion_gate_hash(&self) -> ContentHash {
        self.promotion_gate_hash
    }

    #[must_use]
    pub const fn candidate_model_spec_id(&self) -> ModelSpecId {
        self.candidate_model_spec_id
    }

    #[must_use]
    pub const fn candidate_spec_hash(&self) -> ContentHash {
        self.candidate_model_spec_definition_hash
    }

    #[must_use]
    pub const fn candidate_training_dataset_id(&self) -> TrainingDatasetId {
        self.candidate_training_dataset_id
    }

    #[must_use]
    pub const fn feature_parity_run_id(&self) -> FeatureParityRunId {
        self.feature_parity_run_id
    }

    #[must_use]
    pub const fn feature_parity_state_id(&self) -> FeatureParityStateId {
        self.feature_parity_state_id
    }

    #[must_use]
    pub const fn feature_parity_evidence_hash(&self) -> ContentHash {
        self.feature_parity_evidence_hash
    }

    #[must_use]
    pub const fn candidate_artifact_hash(&self) -> ContentHash {
        self.candidate_artifact_hash
    }

    #[must_use]
    pub const fn candidate_serving_contract_hash(&self) -> ContentHash {
        self.candidate_serving_contract_hash
    }

    #[must_use]
    pub const fn profile_ref(&self) -> &ResearchProfileRef {
        &self.profile_ref
    }

    #[must_use]
    pub const fn category(&self) -> MarketCategory {
        self.category
    }

    /// Revalidate every persisted candidate/model-spec projection frozen by
    /// this serving contract.
    pub fn validate_model(&self, model: &ModelVersionInfo) -> Result<(), FeedbackError> {
        self.validate()?;
        model
            .verified_serving_contract()
            .map_err(|error| invalid_preflight(error.to_string()))?;
        if model.model_version_id != self.candidate_model_version_id
            || model.model_spec_id != self.candidate_model_spec_id
            || model.model_family != self.candidate_model_family
            || model.artifact_hash != self.candidate_artifact_hash
            || model.serving_contract_hash != self.candidate_serving_contract_hash
            || model.model_spec_definition_hash != self.candidate_model_spec_definition_hash
            || model.training_dataset_id != Some(self.candidate_training_dataset_id)
            || model.profile_ref != self.profile_ref
            || model.category_scope != Some(self.category)
        {
            return Err(invalid_preflight(
                "candidate model differs from the frozen serving constraints",
            ));
        }
        Ok(())
    }
}

impl TryFrom<PromotionServingConstraintsDocument> for PromotionServingConstraints {
    type Error = FeedbackError;

    fn try_from(document: PromotionServingConstraintsDocument) -> Result<Self, Self::Error> {
        let constraints = Self {
            format_version: document.format_version,
            candidate_model_version_id: document.candidate_model_version_id,
            candidate_manifest_id: document.candidate_manifest_id,
            candidate_manifest_hash: document.candidate_manifest_hash,
            promotion_gate_hash: document.promotion_gate_hash,
            candidate_model_spec_id: document.candidate_model_spec_id,
            candidate_model_family: document.candidate_model_family,
            candidate_artifact_hash: document.candidate_artifact_hash,
            candidate_serving_contract_hash: document.candidate_serving_contract_hash,
            candidate_model_spec_definition_hash: document.candidate_model_spec_definition_hash,
            candidate_training_dataset_id: document.candidate_training_dataset_id,
            feature_parity_run_id: document.feature_parity_run_id,
            feature_parity_state_id: document.feature_parity_state_id,
            feature_parity_evidence_hash: document.feature_parity_evidence_hash,
            profile_ref: document.profile_ref,
            category: document.category,
        };
        constraints.validate()?;
        Ok(constraints)
    }
}

/// Complete, content-addressed preflight bound by a promotion permit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "PromotionPreflightDocument")]
pub struct PromotionPreflight {
    format_version: u32,
    preflight_hash: ContentHash,
    scope: PromotionPermitScope,
    scope_hash: ContentHash,
    feedback_cycle_id: FeedbackCycleId,
    cycle_idempotency_hash: ContentHash,
    decision_artifact_id: FeedbackDecisionArtifactId,
    decision_artifact_hash: ContentHash,
    decision_object_hash: ContentHash,
    decision_job_input_hash: ContentHash,
    shadow_artifact_id: FeedbackShadowArtifactId,
    shadow_artifact_hash: ContentHash,
    shadow_object_hash: ContentHash,
    shadow_contract_hash: ContentHash,
    candidate_recipe_hash: ContentHash,
    serving_constraints: PromotionServingConstraints,
    current_runtime_mode: QuantRuntimeMode,
    runtime_control_revision: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct PromotionPreflightDocument {
    format_version: u32,
    preflight_hash: ContentHash,
    scope: PromotionPermitScope,
    scope_hash: ContentHash,
    feedback_cycle_id: FeedbackCycleId,
    cycle_idempotency_hash: ContentHash,
    decision_artifact_id: FeedbackDecisionArtifactId,
    decision_artifact_hash: ContentHash,
    decision_object_hash: ContentHash,
    decision_job_input_hash: ContentHash,
    shadow_artifact_id: FeedbackShadowArtifactId,
    shadow_artifact_hash: ContentHash,
    shadow_object_hash: ContentHash,
    shadow_contract_hash: ContentHash,
    candidate_recipe_hash: ContentHash,
    serving_constraints: PromotionServingConstraints,
    current_runtime_mode: QuantRuntimeMode,
    runtime_control_revision: i64,
}

#[derive(Serialize)]
struct PromotionPreflightPreimage<'a> {
    format_version: u32,
    scope: &'a PromotionPermitScope,
    scope_hash: ContentHash,
    feedback_cycle_id: FeedbackCycleId,
    cycle_idempotency_hash: ContentHash,
    decision_artifact_id: FeedbackDecisionArtifactId,
    decision_artifact_hash: ContentHash,
    decision_object_hash: ContentHash,
    decision_job_input_hash: ContentHash,
    shadow_artifact_id: FeedbackShadowArtifactId,
    shadow_artifact_hash: ContentHash,
    shadow_object_hash: ContentHash,
    shadow_contract_hash: ContentHash,
    candidate_recipe_hash: ContentHash,
    serving_constraints: &'a PromotionServingConstraints,
    current_runtime_mode: QuantRuntimeMode,
    runtime_control_revision: i64,
}

/// Server-derived inputs for one promotion preflight.
pub struct PromotionPreflightInput {
    pub scope: PromotionPermitScope,
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub decision_artifact_id: FeedbackDecisionArtifactId,
    pub decision_artifact_hash: ContentHash,
    pub decision_object_hash: ContentHash,
    pub decision_job_input_hash: ContentHash,
    pub shadow_artifact_id: FeedbackShadowArtifactId,
    pub shadow_artifact_hash: ContentHash,
    pub shadow_object_hash: ContentHash,
    pub shadow_contract_hash: ContentHash,
    pub candidate_recipe_hash: ContentHash,
    pub serving_constraints: PromotionServingConstraints,
    pub current_runtime_mode: QuantRuntimeMode,
    pub runtime_control_revision: i64,
}

impl PromotionPreflight {
    pub fn try_seal(input: PromotionPreflightInput) -> Result<Self, FeedbackError> {
        let scope_hash = input.scope.scope_hash()?;
        let preflight_hash = Self::derive_hash(&PromotionPreflightPreimage {
            format_version: PROMOTION_PREFLIGHT_VERSION,
            scope: &input.scope,
            scope_hash,
            feedback_cycle_id: input.feedback_cycle_id,
            cycle_idempotency_hash: input.cycle_idempotency_hash,
            decision_artifact_id: input.decision_artifact_id,
            decision_artifact_hash: input.decision_artifact_hash,
            decision_object_hash: input.decision_object_hash,
            decision_job_input_hash: input.decision_job_input_hash,
            shadow_artifact_id: input.shadow_artifact_id,
            shadow_artifact_hash: input.shadow_artifact_hash,
            shadow_object_hash: input.shadow_object_hash,
            shadow_contract_hash: input.shadow_contract_hash,
            candidate_recipe_hash: input.candidate_recipe_hash,
            serving_constraints: &input.serving_constraints,
            current_runtime_mode: input.current_runtime_mode,
            runtime_control_revision: input.runtime_control_revision,
        })?;
        let preflight = Self {
            format_version: PROMOTION_PREFLIGHT_VERSION,
            preflight_hash,
            scope: input.scope,
            scope_hash,
            feedback_cycle_id: input.feedback_cycle_id,
            cycle_idempotency_hash: input.cycle_idempotency_hash,
            decision_artifact_id: input.decision_artifact_id,
            decision_artifact_hash: input.decision_artifact_hash,
            decision_object_hash: input.decision_object_hash,
            decision_job_input_hash: input.decision_job_input_hash,
            shadow_artifact_id: input.shadow_artifact_id,
            shadow_artifact_hash: input.shadow_artifact_hash,
            shadow_object_hash: input.shadow_object_hash,
            shadow_contract_hash: input.shadow_contract_hash,
            candidate_recipe_hash: input.candidate_recipe_hash,
            serving_constraints: input.serving_constraints,
            current_runtime_mode: input.current_runtime_mode,
            runtime_control_revision: input.runtime_control_revision,
        };
        preflight.validate()?;
        Ok(preflight)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.scope.validate()?;
        self.serving_constraints.validate()?;
        let expected_scope_hash = self.scope.scope_hash()?;
        let serving_hash = self.serving_constraints.constraints_hash()?;
        if self.format_version != PROMOTION_PREFLIGHT_VERSION
            || self.scope_hash != expected_scope_hash
            || self.scope.serving_constraints_hash() != serving_hash
            || self.scope.profile_ref() != self.serving_constraints.profile_ref()
            || self.scope.category() != self.serving_constraints.category()
            || self.scope.candidate_model_version_id()
                != self.serving_constraints.candidate_model_version_id()
            || self.scope.candidate_manifest_id()
                != self.serving_constraints.candidate_manifest_id()
            || self.scope.candidate_manifest_hash()
                != self.serving_constraints.candidate_manifest_hash()
            || self.scope.promotion_gate_hash() != self.serving_constraints.promotion_gate_hash()
            || !self.scope.allows_mode(self.current_runtime_mode)
            || self.runtime_control_revision < 0
            || self.scope.expected_runtime_control_revision() != self.runtime_control_revision
            || FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
                != self.feedback_cycle_id
            || self.scope.feedback_cycle_id() != self.feedback_cycle_id
            || self.decision_artifact_id
                != FeedbackDecisionArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.shadow_artifact_id
                != FeedbackShadowArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.preflight_hash != Self::derive_hash(&self.preimage())?
        {
            return Err(invalid_preflight(
                "preflight identity, scope, serving constraints, mode, or hash is invalid",
            ));
        }
        Ok(())
    }

    const fn preimage(&self) -> PromotionPreflightPreimage<'_> {
        PromotionPreflightPreimage {
            format_version: self.format_version,
            scope: &self.scope,
            scope_hash: self.scope_hash,
            feedback_cycle_id: self.feedback_cycle_id,
            cycle_idempotency_hash: self.cycle_idempotency_hash,
            decision_artifact_id: self.decision_artifact_id,
            decision_artifact_hash: self.decision_artifact_hash,
            decision_object_hash: self.decision_object_hash,
            decision_job_input_hash: self.decision_job_input_hash,
            shadow_artifact_id: self.shadow_artifact_id,
            shadow_artifact_hash: self.shadow_artifact_hash,
            shadow_object_hash: self.shadow_object_hash,
            shadow_contract_hash: self.shadow_contract_hash,
            candidate_recipe_hash: self.candidate_recipe_hash,
            serving_constraints: &self.serving_constraints,
            current_runtime_mode: self.current_runtime_mode,
            runtime_control_revision: self.runtime_control_revision,
        }
    }

    fn derive_hash(
        preimage: &PromotionPreflightPreimage<'_>,
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            PROMOTION_PREFLIGHT_DOMAIN,
            PROMOTION_PREFLIGHT_VERSION,
            preimage,
        )
        .map_err(Into::into)
    }

    #[must_use]
    pub const fn preflight_hash(&self) -> ContentHash {
        self.preflight_hash
    }

    #[must_use]
    pub const fn scope(&self) -> &PromotionPermitScope {
        &self.scope
    }

    #[must_use]
    pub const fn feedback_cycle_id(&self) -> FeedbackCycleId {
        self.feedback_cycle_id
    }

    #[must_use]
    pub const fn cycle_idempotency_hash(&self) -> ContentHash {
        self.cycle_idempotency_hash
    }

    #[must_use]
    pub const fn decision_artifact_id(&self) -> FeedbackDecisionArtifactId {
        self.decision_artifact_id
    }

    #[must_use]
    pub const fn decision_artifact_hash(&self) -> ContentHash {
        self.decision_artifact_hash
    }

    #[must_use]
    pub const fn decision_object_hash(&self) -> ContentHash {
        self.decision_object_hash
    }

    #[must_use]
    pub const fn decision_job_input_hash(&self) -> ContentHash {
        self.decision_job_input_hash
    }

    #[must_use]
    pub const fn shadow_artifact_id(&self) -> FeedbackShadowArtifactId {
        self.shadow_artifact_id
    }

    #[must_use]
    pub const fn shadow_artifact_hash(&self) -> ContentHash {
        self.shadow_artifact_hash
    }

    #[must_use]
    pub const fn shadow_object_hash(&self) -> ContentHash {
        self.shadow_object_hash
    }

    #[must_use]
    pub const fn shadow_contract_hash(&self) -> ContentHash {
        self.shadow_contract_hash
    }

    #[must_use]
    pub const fn candidate_recipe_hash(&self) -> ContentHash {
        self.candidate_recipe_hash
    }

    #[must_use]
    pub const fn serving_constraints(&self) -> &PromotionServingConstraints {
        &self.serving_constraints
    }

    #[must_use]
    pub const fn current_runtime_mode(&self) -> QuantRuntimeMode {
        self.current_runtime_mode
    }

    #[must_use]
    pub const fn runtime_control_revision(&self) -> i64 {
        self.runtime_control_revision
    }
}

impl TryFrom<PromotionPreflightDocument> for PromotionPreflight {
    type Error = FeedbackError;

    fn try_from(document: PromotionPreflightDocument) -> Result<Self, Self::Error> {
        let preflight = Self {
            format_version: document.format_version,
            preflight_hash: document.preflight_hash,
            scope: document.scope,
            scope_hash: document.scope_hash,
            feedback_cycle_id: document.feedback_cycle_id,
            cycle_idempotency_hash: document.cycle_idempotency_hash,
            decision_artifact_id: document.decision_artifact_id,
            decision_artifact_hash: document.decision_artifact_hash,
            decision_object_hash: document.decision_object_hash,
            decision_job_input_hash: document.decision_job_input_hash,
            shadow_artifact_id: document.shadow_artifact_id,
            shadow_artifact_hash: document.shadow_artifact_hash,
            shadow_object_hash: document.shadow_object_hash,
            shadow_contract_hash: document.shadow_contract_hash,
            candidate_recipe_hash: document.candidate_recipe_hash,
            serving_constraints: document.serving_constraints,
            current_runtime_mode: document.current_runtime_mode,
            runtime_control_revision: document.runtime_control_revision,
        };
        preflight.validate()?;
        Ok(preflight)
    }
}

/// Exact category-route and model artifact delta committed by one promotion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRoutePromotionRoute {
    pub category: MarketCategory,
    pub champion_model_version_id: ModelVersionId,
    pub champion_artifact_hash: ContentHash,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_model_version_id: ModelVersionId,
    pub candidate_artifact_hash: ContentHash,
    pub candidate_serving_contract_hash: ContentHash,
    pub consumed_shadow_model_version_id: ModelVersionId,
}

/// Old/new policy identities and the single database transaction revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRoutePromotionPolicy {
    pub previous_generation: PolicyBundleGeneration,
    pub transaction_revision: PolicyBundleGeneration,
    pub previous_snapshot_id: DecisionPolicySnapshotId,
    pub previous_snapshot_hash: ContentHash,
    pub committed_snapshot_id: DecisionPolicySnapshotId,
    pub committed_snapshot_hash: ContentHash,
    pub previous_model_routing_revision_id: PolicyRevisionId,
    pub committed_model_routing_revision_id: PolicyRevisionId,
    pub policy_approval_id: PolicyApprovalId,
    pub policy_activation_id: PolicyActivationId,
}

#[derive(Serialize)]
struct ModelRoutePromotionPreimage<'a> {
    format_version: u32,
    promotion_permit_id: PromotionPermitId,
    permit_issuance_hash: ContentHash,
    permit_issued_at: DateTime<Utc>,
    preflight: &'a PromotionPreflight,
    actor_user_id: UserId,
    actor_username: &'a str,
    actor_role: &'a RoleCode,
    idempotency_key: &'a PolicyIdempotencyKey,
    reason_code: &'a str,
    note: &'a str,
    route: &'a ModelRoutePromotionRoute,
    policy: &'a ModelRoutePromotionPolicy,
}

/// Inputs jointly sealed into the immutable promotion audit record.
pub struct ModelRoutePromotionRecordInput {
    pub promotion_permit_id: PromotionPermitId,
    pub permit_issuance_hash: ContentHash,
    pub permit_issued_at: DateTime<Utc>,
    pub preflight: PromotionPreflight,
    pub actor_user_id: UserId,
    pub actor_username: String,
    pub actor_role: RoleCode,
    pub idempotency_key: PolicyIdempotencyKey,
    pub reason_code: String,
    pub note: String,
    pub route: ModelRoutePromotionRoute,
    pub policy: ModelRoutePromotionPolicy,
}

/// Complete content-addressed record shared by model, policy and outbox ledgers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRoutePromotionRecord {
    format_version: u32,
    transaction_hash: ContentHash,
    promotion_permit_id: PromotionPermitId,
    permit_issuance_hash: ContentHash,
    permit_issued_at: DateTime<Utc>,
    preflight: PromotionPreflight,
    actor_user_id: UserId,
    actor_username: String,
    actor_role: RoleCode,
    idempotency_key: PolicyIdempotencyKey,
    reason_code: String,
    note: String,
    route: ModelRoutePromotionRoute,
    policy: ModelRoutePromotionPolicy,
}

impl ModelRoutePromotionRecord {
    pub fn try_seal(input: ModelRoutePromotionRecordInput) -> Result<Self, FeedbackError> {
        let transaction_hash = Self::derive_hash(&ModelRoutePromotionPreimage {
            format_version: PROMOTION_TRANSACTION_VERSION,
            promotion_permit_id: input.promotion_permit_id,
            permit_issuance_hash: input.permit_issuance_hash,
            permit_issued_at: input.permit_issued_at,
            preflight: &input.preflight,
            actor_user_id: input.actor_user_id,
            actor_username: &input.actor_username,
            actor_role: &input.actor_role,
            idempotency_key: &input.idempotency_key,
            reason_code: &input.reason_code,
            note: &input.note,
            route: &input.route,
            policy: &input.policy,
        })?;
        let record = Self {
            format_version: PROMOTION_TRANSACTION_VERSION,
            transaction_hash,
            promotion_permit_id: input.promotion_permit_id,
            permit_issuance_hash: input.permit_issuance_hash,
            permit_issued_at: input.permit_issued_at,
            preflight: input.preflight,
            actor_user_id: input.actor_user_id,
            actor_username: input.actor_username,
            actor_role: input.actor_role,
            idempotency_key: input.idempotency_key,
            reason_code: input.reason_code,
            note: input.note,
            route: input.route,
            policy: input.policy,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.preflight.validate()?;
        validate_actor(&self.actor_username, &self.actor_role, &self.note)?;
        validate_reason_code(&self.reason_code)?;
        let scope = self.preflight.scope();
        let candidate = self.preflight.serving_constraints();
        let next_revision = self
            .policy
            .previous_generation
            .checked_next()
            .map_err(|error| invalid_preflight(error.to_string()))?;
        let exact_projection = self.format_version == PROMOTION_TRANSACTION_VERSION
            && self.promotion_permit_id
                == PromotionPermitId::from_issuance_hash(&self.permit_issuance_hash)
            && self.permit_issued_at < scope.expires_at()
            && self.route.category == scope.category()
            && self.route.champion_model_version_id == scope.champion_model_version_id()
            && self.route.champion_serving_contract_hash == scope.champion_serving_contract_hash()
            && self.route.candidate_model_version_id == candidate.candidate_model_version_id()
            && self.route.candidate_artifact_hash == candidate.candidate_artifact_hash()
            && self.route.candidate_serving_contract_hash
                == candidate.candidate_serving_contract_hash()
            && self.route.consumed_shadow_model_version_id == self.route.candidate_model_version_id
            && self.route.champion_model_version_id != self.route.candidate_model_version_id
            && self.policy.previous_generation == scope.expected_policy_generation()
            && self.policy.transaction_revision == next_revision
            && self.policy.previous_snapshot_id == scope.expected_snapshot_id()
            && self.policy.previous_snapshot_hash == scope.expected_snapshot_hash()
            && self.policy.committed_snapshot_id
                == DecisionPolicySnapshotId::from_content_hash(
                    &self.policy.committed_snapshot_hash,
                )
            && self.policy.previous_model_routing_revision_id
                != self.policy.committed_model_routing_revision_id;
        if !exact_projection || self.transaction_hash != Self::derive_hash(&self.preimage())? {
            return Err(invalid_preflight(
                "promotion transaction record has inconsistent permit, route, policy, or hash",
            ));
        }
        Ok(())
    }

    fn preimage(&self) -> ModelRoutePromotionPreimage<'_> {
        ModelRoutePromotionPreimage {
            format_version: self.format_version,
            promotion_permit_id: self.promotion_permit_id,
            permit_issuance_hash: self.permit_issuance_hash,
            permit_issued_at: self.permit_issued_at,
            preflight: &self.preflight,
            actor_user_id: self.actor_user_id,
            actor_username: &self.actor_username,
            actor_role: &self.actor_role,
            idempotency_key: &self.idempotency_key,
            reason_code: &self.reason_code,
            note: &self.note,
            route: &self.route,
            policy: &self.policy,
        }
    }

    fn derive_hash(
        preimage: &ModelRoutePromotionPreimage<'_>,
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            PROMOTION_TRANSACTION_DOMAIN,
            PROMOTION_TRANSACTION_VERSION,
            preimage,
        )
        .map_err(Into::into)
    }

    #[must_use]
    pub const fn transaction_hash(&self) -> ContentHash {
        self.transaction_hash
    }

    #[must_use]
    pub const fn promotion_permit_id(&self) -> PromotionPermitId {
        self.promotion_permit_id
    }

    #[must_use]
    pub const fn preflight(&self) -> &PromotionPreflight {
        &self.preflight
    }

    #[must_use]
    pub const fn actor_user_id(&self) -> UserId {
        self.actor_user_id
    }

    #[must_use]
    pub fn actor_username(&self) -> &str {
        &self.actor_username
    }

    #[must_use]
    pub const fn actor_role(&self) -> &RoleCode {
        &self.actor_role
    }

    #[must_use]
    pub const fn idempotency_key(&self) -> &PolicyIdempotencyKey {
        &self.idempotency_key
    }

    #[must_use]
    pub fn reason_code(&self) -> &str {
        &self.reason_code
    }

    #[must_use]
    pub fn note(&self) -> &str {
        &self.note
    }

    #[must_use]
    pub fn audit_reason(&self) -> String {
        format!("{}: {}", self.reason_code, self.note)
    }

    #[must_use]
    pub const fn route(&self) -> &ModelRoutePromotionRoute {
        &self.route
    }

    #[must_use]
    pub const fn policy(&self) -> &ModelRoutePromotionPolicy {
        &self.policy
    }
}

/// Internal command that carries only a freshly verified P03 preflight.
#[derive(Debug, Clone)]
pub struct CommitModelRoutePromotion {
    promotion_permit_id: PromotionPermitId,
    preflight: PromotionPreflight,
    actor: PromotionPermitActor,
    idempotency_key: PolicyIdempotencyKey,
    reason_code: String,
    note: String,
}

impl CommitModelRoutePromotion {
    pub fn try_new(
        request: PromoteModelRoute,
        preflight: PromotionPreflight,
    ) -> Result<Self, FeedbackError> {
        request.validate()?;
        preflight.validate()?;
        if request.feedback_cycle_id != preflight.feedback_cycle_id()
            || request.expected_policy_generation != preflight.scope().expected_policy_generation()
            || request.expected_runtime_control_revision != preflight.runtime_control_revision()
        {
            return Err(invalid_preflight(
                "activation request cycle, policy generation, or runtime revision differs from preflight",
            ));
        }
        Ok(Self {
            promotion_permit_id: request.promotion_permit_id,
            preflight,
            actor: request.actor,
            idempotency_key: request.idempotency_key,
            reason_code: request.reason_code,
            note: request.note,
        })
    }

    #[must_use]
    pub const fn promotion_permit_id(&self) -> PromotionPermitId {
        self.promotion_permit_id
    }

    #[must_use]
    pub const fn preflight(&self) -> &PromotionPreflight {
        &self.preflight
    }

    #[must_use]
    pub const fn actor(&self) -> &PromotionPermitActor {
        &self.actor
    }

    #[must_use]
    pub const fn idempotency_key(&self) -> &PolicyIdempotencyKey {
        &self.idempotency_key
    }

    #[must_use]
    pub fn reason_code(&self) -> &str {
        &self.reason_code
    }

    #[must_use]
    pub fn note(&self) -> &str {
        &self.note
    }
}

/// Stable authenticated activation intent; all serving authority is
/// server-loaded and checked against the permit-bound preflight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromoteModelRoute {
    pub promotion_permit_id: PromotionPermitId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub expected_policy_generation: PolicyBundleGeneration,
    pub expected_runtime_control_revision: i64,
    pub idempotency_key: PolicyIdempotencyKey,
    pub actor: PromotionPermitActor,
    pub reason_code: String,
    pub note: String,
}

impl PromoteModelRoute {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        if self.expected_runtime_control_revision < 0 {
            return Err(invalid_preflight(
                "expected runtime-control revision cannot be negative",
            ));
        }
        validate_role_reason(&self.actor.acting_role, &self.note)?;
        validate_reason_code(&self.reason_code)
    }
}

/// Authenticated principal and explicit role used for a governed permit action.
///
/// Username is deliberately absent: the transaction-owning repository resolves
/// it from the locked active user row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionPermitActor {
    pub user_id: UserId,
    pub acting_role: RoleCode,
}

/// Validated command for one governed permit issuance.
#[derive(Debug, Clone)]
pub struct IssuePromotionPermit {
    pub actor: PromotionPermitActor,
    pub idempotency_key: PolicyIdempotencyKey,
    pub scope: PromotionPermitScope,
    pub preflight_hash: ContentHash,
    pub reason: String,
}

impl IssuePromotionPermit {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.scope.validate()?;
        validate_role_reason(&self.actor.acting_role, &self.reason)
    }
}

/// Base-revision CAS command for one governed permit revocation.
#[derive(Debug, Clone)]
pub struct RevokePromotionPermit {
    pub promotion_permit_id: PromotionPermitId,
    pub expected_revision: i64,
    pub actor: PromotionPermitActor,
    pub reason: String,
}

impl RevokePromotionPermit {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        if self.expected_revision != 0 {
            return Err(FeedbackError::PromotionPermitConflict {
                detail: format!(
                    "promotion-permit revoke base revision must be 0; got {}",
                    self.expected_revision
                ),
            });
        }
        validate_role_reason(&self.actor.acting_role, &self.reason)
    }
}

/// Complete operator-authenticated issuance preimage.
#[derive(Debug, Clone)]
pub struct PromotionPermitIssueInput {
    pub idempotency_key: PolicyIdempotencyKey,
    pub scope: PromotionPermitScope,
    pub preflight_hash: ContentHash,
    pub issued_by_user_id: UserId,
    pub issued_by_username: String,
    pub issued_by_role: RoleCode,
    pub issuance_reason: String,
}

#[derive(Serialize)]
struct PromotionPermitIssuanceDocument<'a> {
    idempotency_key: &'a PolicyIdempotencyKey,
    scope: &'a PromotionPermitScope,
    preflight_hash: ContentHash,
    issued_by_user_id: UserId,
    issued_by_username: &'a str,
    issued_by_role: &'a RoleCode,
    issuance_reason: &'a str,
}

/// Validated insert payload; all timestamps and revocation fields remain
/// database-owned.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_feedback_promotion_permit::ActiveModel")]
pub struct NewPromotionPermit {
    promotion_permit_id: PromotionPermitId,
    idempotency_key: PolicyIdempotencyKey,
    scope_hash: ContentHash,
    issuance_hash: ContentHash,
    feedback_cycle_id: FeedbackCycleId,
    #[sea_orm(column_type = "JsonBinary")]
    profile_ref: ResearchProfileRef,
    research_profile_artifact_id: ResearchProfileArtifactId,
    profile_hash: ContentHash,
    #[sea_orm(column_type = r#"custom("qp_market_category")"#)]
    category: MarketCategory,
    expected_policy_generation: PolicyBundleGeneration,
    expected_runtime_control_revision: i64,
    expected_decision_policy_snapshot_id: DecisionPolicySnapshotId,
    expected_snapshot_hash: ContentHash,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    candidate_model_version_id: ModelVersionId,
    candidate_manifest_id: ModelCandidateManifestId,
    candidate_manifest_hash: ContentHash,
    promotion_gate_hash: ContentHash,
    #[sea_orm(column_type = r#"custom("qp_quant_runtime_mode[]")"#)]
    allowed_runtime_modes: PromotionRuntimeModes,
    non_route_policy_hash: ContentHash,
    serving_constraints_hash: ContentHash,
    preflight_hash: ContentHash,
    issued_by_user_id: UserId,
    issued_by_username: String,
    issued_by_role: RoleCode,
    issuance_reason: String,
    expires_at: DateTime<Utc>,
}

impl NewPromotionPermit {
    pub fn try_seal(input: PromotionPermitIssueInput) -> Result<Self, FeedbackError> {
        input.scope.validate()?;
        validate_actor(
            &input.issued_by_username,
            &input.issued_by_role,
            &input.issuance_reason,
        )?;
        let scope_hash = input.scope.scope_hash()?;
        let document = PromotionPermitIssuanceDocument {
            idempotency_key: &input.idempotency_key,
            scope: &input.scope,
            preflight_hash: input.preflight_hash,
            issued_by_user_id: input.issued_by_user_id,
            issued_by_username: &input.issued_by_username,
            issued_by_role: &input.issued_by_role,
            issuance_reason: &input.issuance_reason,
        };
        let issuance_hash = CanonicalDigest::content_hash_typed(
            PROMOTION_PERMIT_ISSUANCE_DOMAIN,
            PROMOTION_PERMIT_ISSUANCE_VERSION,
            &document,
        )?;
        let profile_ref = input.scope.profile_ref;
        Ok(Self {
            promotion_permit_id: PromotionPermitId::from_issuance_hash(&issuance_hash),
            idempotency_key: input.idempotency_key,
            scope_hash,
            issuance_hash,
            feedback_cycle_id: input.scope.feedback_cycle_id,
            research_profile_artifact_id: profile_ref.artifact_id(),
            profile_hash: profile_ref.content_hash,
            profile_ref,
            category: input.scope.category,
            expected_policy_generation: input.scope.expected_policy_generation,
            expected_runtime_control_revision: input.scope.expected_runtime_control_revision,
            expected_decision_policy_snapshot_id: input.scope.expected_decision_policy_snapshot_id,
            expected_snapshot_hash: input.scope.expected_snapshot_hash,
            champion_model_version_id: input.scope.champion_model_version_id,
            champion_serving_contract_hash: input.scope.champion_serving_contract_hash,
            candidate_model_version_id: input.scope.candidate_model_version_id,
            candidate_manifest_id: input.scope.candidate_manifest_id,
            candidate_manifest_hash: input.scope.candidate_manifest_hash,
            promotion_gate_hash: input.scope.promotion_gate_hash,
            allowed_runtime_modes: input.scope.allowed_runtime_modes,
            non_route_policy_hash: input.scope.non_route_policy_hash,
            serving_constraints_hash: input.scope.serving_constraints_hash,
            preflight_hash: input.preflight_hash,
            issued_by_user_id: input.issued_by_user_id,
            issued_by_username: input.issued_by_username,
            issued_by_role: input.issued_by_role,
            issuance_reason: input.issuance_reason,
            expires_at: input.scope.expires_at,
        })
    }

    #[must_use]
    pub const fn promotion_permit_id(&self) -> PromotionPermitId {
        self.promotion_permit_id
    }

    #[must_use]
    pub const fn idempotency_key(&self) -> &PolicyIdempotencyKey {
        &self.idempotency_key
    }

    #[must_use]
    pub fn scope(&self) -> PromotionPermitScope {
        PromotionPermitScope {
            format_version: PROMOTION_PERMIT_SCOPE_VERSION,
            feedback_cycle_id: self.feedback_cycle_id,
            profile_ref: self.profile_ref.clone(),
            category: self.category,
            expected_policy_generation: self.expected_policy_generation,
            expected_runtime_control_revision: self.expected_runtime_control_revision,
            expected_decision_policy_snapshot_id: self.expected_decision_policy_snapshot_id,
            expected_snapshot_hash: self.expected_snapshot_hash,
            champion_model_version_id: self.champion_model_version_id,
            champion_serving_contract_hash: self.champion_serving_contract_hash,
            candidate_model_version_id: self.candidate_model_version_id,
            candidate_manifest_id: self.candidate_manifest_id,
            candidate_manifest_hash: self.candidate_manifest_hash,
            promotion_gate_hash: self.promotion_gate_hash,
            allowed_runtime_modes: self.allowed_runtime_modes.clone(),
            non_route_policy_hash: self.non_route_policy_hash,
            serving_constraints_hash: self.serving_constraints_hash,
            expires_at: self.expires_at,
        }
    }

    #[must_use]
    pub const fn scope_hash(&self) -> ContentHash {
        self.scope_hash
    }

    #[must_use]
    pub const fn issuance_hash(&self) -> ContentHash {
        self.issuance_hash
    }

    #[must_use]
    pub const fn preflight_hash(&self) -> ContentHash {
        self.preflight_hash
    }

    #[must_use]
    pub const fn issued_by_user_id(&self) -> UserId {
        self.issued_by_user_id
    }

    #[must_use]
    pub fn issued_by_username(&self) -> &str {
        &self.issued_by_username
    }

    #[must_use]
    pub const fn issued_by_role(&self) -> &RoleCode {
        &self.issued_by_role
    }

    #[must_use]
    pub fn issuance_reason(&self) -> &str {
        &self.issuance_reason
    }
}

/// Read-time validity derived from immutable expiry plus the one-way revoke.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromotionPermitStatus {
    Active,
    Expired,
    Revoked,
}

/// Complete one-way revocation tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionPermitRevocation {
    pub revoked_by_user_id: UserId,
    pub revoked_by_username: String,
    pub revoked_by_role: RoleCode,
    pub revocation_reason: String,
    pub revoked_at: DateTime<Utc>,
}

/// Domain decision before the repository applies the revocation CAS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotionPermitRevocationCheck {
    Apply,
    ExactReplay,
}

/// Full persisted projection of a promotion permit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_feedback_promotion_permit::Entity")]
pub struct PromotionPermitInfo {
    pub promotion_permit_id: PromotionPermitId,
    pub idempotency_key: PolicyIdempotencyKey,
    pub scope_hash: ContentHash,
    pub issuance_hash: ContentHash,
    pub feedback_cycle_id: FeedbackCycleId,
    pub profile_ref: ResearchProfileRef,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub profile_hash: ContentHash,
    pub category: MarketCategory,
    pub expected_policy_generation: PolicyBundleGeneration,
    pub expected_runtime_control_revision: i64,
    pub expected_decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub expected_snapshot_hash: ContentHash,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_model_version_id: ModelVersionId,
    pub candidate_manifest_id: ModelCandidateManifestId,
    pub candidate_manifest_hash: ContentHash,
    pub promotion_gate_hash: ContentHash,
    pub allowed_runtime_modes: Vec<QuantRuntimeMode>,
    pub non_route_policy_hash: ContentHash,
    pub serving_constraints_hash: ContentHash,
    pub preflight_hash: ContentHash,
    pub issued_by_user_id: UserId,
    pub issued_by_username: String,
    pub issued_by_role: RoleCode,
    pub issuance_reason: String,
    pub expires_at: DateTime<Utc>,
    pub revoked_by_user_id: Option<UserId>,
    pub revoked_by_username: Option<String>,
    pub revoked_by_role: Option<RoleCode>,
    pub revocation_reason: Option<String>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub revision: i64,
    pub issued_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl PromotionPermitInfo {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        let scope = self.scope()?;
        let expected_scope_hash = scope.scope_hash()?;
        if self.research_profile_artifact_id != self.profile_ref.artifact_id()
            || self.profile_hash != self.profile_ref.content_hash
            || self.scope_hash != expected_scope_hash
        {
            return Err(invalid_permit(
                "persisted profile or scope projection does not match the immutable scope",
            ));
        }
        validate_actor(
            &self.issued_by_username,
            &self.issued_by_role,
            &self.issuance_reason,
        )?;
        let document = PromotionPermitIssuanceDocument {
            idempotency_key: &self.idempotency_key,
            scope: &scope,
            preflight_hash: self.preflight_hash,
            issued_by_user_id: self.issued_by_user_id,
            issued_by_username: &self.issued_by_username,
            issued_by_role: &self.issued_by_role,
            issuance_reason: &self.issuance_reason,
        };
        let expected_issuance_hash = CanonicalDigest::content_hash_typed(
            PROMOTION_PERMIT_ISSUANCE_DOMAIN,
            PROMOTION_PERMIT_ISSUANCE_VERSION,
            &document,
        )?;
        if self.issuance_hash != expected_issuance_hash
            || self.promotion_permit_id
                != PromotionPermitId::from_issuance_hash(&expected_issuance_hash)
            || self.expires_at <= self.issued_at
        {
            return Err(invalid_permit(
                "persisted issuance identity or validity window is invalid",
            ));
        }
        self.validate_lifecycle()
    }

    pub fn scope(&self) -> Result<PromotionPermitScope, FeedbackError> {
        PromotionPermitScope::try_new(PromotionPermitScopeInput {
            feedback_cycle_id: self.feedback_cycle_id,
            profile_ref: self.profile_ref.clone(),
            category: self.category,
            expected_policy_generation: self.expected_policy_generation,
            expected_runtime_control_revision: self.expected_runtime_control_revision,
            expected_decision_policy_snapshot_id: self.expected_decision_policy_snapshot_id,
            expected_snapshot_hash: self.expected_snapshot_hash,
            champion_model_version_id: self.champion_model_version_id,
            champion_serving_contract_hash: self.champion_serving_contract_hash,
            candidate_model_version_id: self.candidate_model_version_id,
            candidate_manifest_id: self.candidate_manifest_id,
            candidate_manifest_hash: self.candidate_manifest_hash,
            promotion_gate_hash: self.promotion_gate_hash,
            allowed_runtime_modes: self.allowed_runtime_modes.clone(),
            non_route_policy_hash: self.non_route_policy_hash,
            serving_constraints_hash: self.serving_constraints_hash,
            expires_at: self.expires_at,
        })
    }

    /// Compare every immutable issuance projection, never only a digest or
    /// unique key, before accepting an idempotent replay.
    pub fn has_same_issuance(&self, expected: &NewPromotionPermit) -> Result<bool, FeedbackError> {
        self.validate()?;
        expected.scope().validate()?;
        Ok(self.promotion_permit_id == expected.promotion_permit_id
            && self.idempotency_key == expected.idempotency_key
            && self.scope_hash == expected.scope_hash
            && self.issuance_hash == expected.issuance_hash
            && self.feedback_cycle_id == expected.feedback_cycle_id
            && self.profile_ref == expected.profile_ref
            && self.research_profile_artifact_id == expected.research_profile_artifact_id
            && self.profile_hash == expected.profile_hash
            && self.category == expected.category
            && self.expected_policy_generation == expected.expected_policy_generation
            && self.expected_runtime_control_revision == expected.expected_runtime_control_revision
            && self.expected_decision_policy_snapshot_id
                == expected.expected_decision_policy_snapshot_id
            && self.expected_snapshot_hash == expected.expected_snapshot_hash
            && self.champion_model_version_id == expected.champion_model_version_id
            && self.champion_serving_contract_hash == expected.champion_serving_contract_hash
            && self.candidate_model_version_id == expected.candidate_model_version_id
            && self.candidate_manifest_id == expected.candidate_manifest_id
            && self.candidate_manifest_hash == expected.candidate_manifest_hash
            && self.promotion_gate_hash == expected.promotion_gate_hash
            && self.allowed_runtime_modes == expected.allowed_runtime_modes.0
            && self.non_route_policy_hash == expected.non_route_policy_hash
            && self.serving_constraints_hash == expected.serving_constraints_hash
            && self.preflight_hash == expected.preflight_hash
            && self.issued_by_user_id == expected.issued_by_user_id
            && self.issued_by_username == expected.issued_by_username
            && self.issued_by_role == expected.issued_by_role
            && self.issuance_reason == expected.issuance_reason
            && self.expires_at == expected.expires_at)
    }

    pub fn status_at(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<PromotionPermitStatus, FeedbackError> {
        self.validate()?;
        if observed_at < self.issued_at {
            return Err(invalid_permit(
                "permit status cannot be observed before issuance",
            ));
        }
        if self
            .revoked_at
            .is_some_and(|revoked_at| revoked_at <= observed_at)
        {
            return Ok(PromotionPermitStatus::Revoked);
        }
        if observed_at >= self.expires_at {
            Ok(PromotionPermitStatus::Expired)
        } else {
            Ok(PromotionPermitStatus::Active)
        }
    }

    pub fn check_revocation(
        &self,
        candidate: &PromotionPermitRevocation,
    ) -> Result<PromotionPermitRevocationCheck, FeedbackError> {
        self.validate()?;
        validate_actor(
            &candidate.revoked_by_username,
            &candidate.revoked_by_role,
            &candidate.revocation_reason,
        )?;
        if candidate.revoked_at < self.issued_at {
            return Err(invalid_permit("revocation precedes permit issuance"));
        }
        match (
            self.revoked_by_user_id,
            self.revoked_by_username.as_deref(),
            self.revoked_by_role.as_ref(),
            self.revocation_reason.as_deref(),
            self.revoked_at,
        ) {
            (None, None, None, None, None) => Ok(PromotionPermitRevocationCheck::Apply),
            (
                Some(stored_user_id),
                Some(stored_username),
                Some(stored_role),
                Some(stored_reason),
                Some(stored_at),
            ) if stored_user_id == candidate.revoked_by_user_id
                && stored_username == candidate.revoked_by_username
                && stored_role == &candidate.revoked_by_role
                && stored_reason == candidate.revocation_reason
                && stored_at == candidate.revoked_at =>
            {
                Ok(PromotionPermitRevocationCheck::ExactReplay)
            }
            _ => Err(FeedbackError::PromotionPermitConflict {
                detail: "permit is already revoked with a different immutable tuple".to_owned(),
            }),
        }
    }

    fn validate_lifecycle(&self) -> Result<(), FeedbackError> {
        let active = self.revoked_by_user_id.is_none()
            && self.revoked_by_username.is_none()
            && self.revoked_by_role.is_none()
            && self.revocation_reason.is_none()
            && self.revoked_at.is_none()
            && self.revision == 0
            && self.updated_at == self.issued_at;
        let revoked = match (
            self.revoked_by_user_id,
            self.revoked_by_username.as_deref(),
            self.revoked_by_role.as_ref(),
            self.revocation_reason.as_deref(),
            self.revoked_at,
        ) {
            (Some(_), Some(username), Some(role), Some(reason), Some(revoked_at)) => {
                validate_actor(username, role, reason)?;
                self.revision == 1 && revoked_at >= self.issued_at && self.updated_at == revoked_at
            }
            _ => false,
        };
        if active || revoked {
            Ok(())
        } else {
            Err(invalid_permit(
                "revocation tuple, revision, or lifecycle timestamps are inconsistent",
            ))
        }
    }
}

fn validate_modes(modes: &[QuantRuntimeMode]) -> Result<(), FeedbackError> {
    if modes.is_empty()
        || modes.len() > 3
        || !modes.windows(2).all(|pair| pair[0].rank() < pair[1].rank())
    {
        return Err(invalid_permit(
            "allowed runtime modes must be a non-empty, unique capability-ordered set",
        ));
    }
    Ok(())
}

fn validate_actor(username: &str, role: &RoleCode, reason: &str) -> Result<(), FeedbackError> {
    if username.is_empty()
        || username.len() > MAX_ACTOR_BYTES
        || username != username.trim()
        || username.chars().any(char::is_control)
    {
        return Err(invalid_permit(
            "actor username violates the governed text contract",
        ));
    }
    validate_role_reason(role, reason)
}

fn validate_role_reason(role: &RoleCode, reason: &str) -> Result<(), FeedbackError> {
    let role = role.as_str();
    if role.is_empty()
        || role.len() > MAX_ROLE_BYTES
        || !role
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        || reason.is_empty()
        || reason.len() > MAX_REASON_BYTES
        || reason != reason.trim()
        || reason.chars().any(char::is_control)
    {
        return Err(invalid_permit(
            "actor role or reason violates the governed text contract",
        ));
    }
    Ok(())
}

fn validate_reason_code(reason_code: &str) -> Result<(), FeedbackError> {
    if reason_code.is_empty()
        || reason_code.len() > MAX_REASON_CODE_BYTES
        || !reason_code
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(invalid_preflight(
            "activation reason code must be lowercase snake_case",
        ));
    }
    Ok(())
}

fn invalid_permit(detail: impl Into<String>) -> FeedbackError {
    FeedbackError::InvalidPromotionPermit {
        detail: detail.into(),
    }
}

fn invalid_preflight(detail: impl Into<String>) -> FeedbackError {
    FeedbackError::InvalidPromotionPreflight {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use sea_orm::{ActiveValue, IntoActiveModel};

    use crate::{
        enums::{common::MarketCategory, model::ModelFamily, quant::QuantRuntimeMode},
        runtime_config::{ActivePolicyBundle, DecisionPolicySnapshot, ModelVersionRef},
        types::{
            ContentHash, DecisionPolicySnapshotId, FeatureParityRunId, FeatureParityStateId,
            FeedbackCycleId, FeedbackDecisionArtifactId, FeedbackShadowArtifactId,
            ModelCandidateManifestId, ModelSpecId, ModelVersionId, PolicyBundleGeneration,
            PolicyIdempotencyKey, PolicyRevisionId, ResearchProfileRef, RoleCode,
            TrainingDatasetId, UserId,
            research_profile::{CRYPTO_PRICE_15M_PROFILE_ID, builtin_research_profiles},
        },
    };

    use super::{
        NewPromotionPermit, PromotionPermitInfo, PromotionPermitIssueInput,
        PromotionPermitRevocation, PromotionPermitRevocationCheck, PromotionPermitScope,
        PromotionPermitScopeInput, PromotionPermitStatus, PromotionPolicyProjection,
        PromotionPreflight, PromotionPreflightInput, PromotionServingConstraints,
        PromotionServingConstraintsInput,
    };

    struct PermitFixture {
        feedback_cycle_id: FeedbackCycleId,
        profile_ref: ResearchProfileRef,
        issued_by_user_id: UserId,
    }

    impl PermitFixture {
        fn new() -> Self {
            let profile_ref = builtin_research_profiles()
                .expect("build profile registry")
                .into_iter()
                .find(|profile| profile.profile_ref.id.as_str() == CRYPTO_PRICE_15M_PROFILE_ID)
                .expect("crypto profile")
                .profile_ref;
            Self {
                feedback_cycle_id: FeedbackCycleId::from_v7(),
                profile_ref,
                issued_by_user_id: UserId::from_v7(),
            }
        }

        fn scope(&self) -> PromotionPermitScope {
            let snapshot_hash = hash(1);
            let candidate_manifest_hash = hash(30);
            PromotionPermitScope::try_new(PromotionPermitScopeInput {
                feedback_cycle_id: self.feedback_cycle_id,
                profile_ref: self.profile_ref.clone(),
                category: MarketCategory::Crypto,
                expected_policy_generation: PolicyBundleGeneration::FIRST,
                expected_runtime_control_revision: 0,
                expected_decision_policy_snapshot_id: DecisionPolicySnapshotId::from_content_hash(
                    &snapshot_hash,
                ),
                expected_snapshot_hash: snapshot_hash,
                champion_model_version_id: ModelVersionId::from_v7(),
                champion_serving_contract_hash: hash(2),
                candidate_model_version_id: ModelVersionId::from_v7(),
                candidate_manifest_id: ModelCandidateManifestId::from_content_hash(
                    &candidate_manifest_hash,
                ),
                candidate_manifest_hash,
                promotion_gate_hash: hash(31),
                allowed_runtime_modes: vec![
                    QuantRuntimeMode::ReportOnly,
                    QuantRuntimeMode::SemiAuto,
                ],
                non_route_policy_hash: hash(3),
                serving_constraints_hash: hash(4),
                expires_at: Utc
                    .with_ymd_and_hms(2026, 8, 1, 0, 0, 0)
                    .single()
                    .expect("permit expiry"),
            })
            .expect("valid permit scope")
        }

        fn permit(&self) -> NewPromotionPermit {
            NewPromotionPermit::try_seal(PromotionPermitIssueInput {
                idempotency_key: "permit-crypto-0001"
                    .parse::<PolicyIdempotencyKey>()
                    .expect("valid idempotency key"),
                scope: self.scope(),
                preflight_hash: hash(5),
                issued_by_user_id: self.issued_by_user_id,
                issued_by_username: "risk-owner".to_owned(),
                issued_by_role: RoleCode::new("risk_owner"),
                issuance_reason: "authorize exact Crypto champion replacement".to_owned(),
            })
            .expect("seal permit")
        }

        fn info(&self) -> PromotionPermitInfo {
            let new = self.permit();
            let issued_at = Utc
                .with_ymd_and_hms(2026, 7, 28, 12, 0, 0)
                .single()
                .expect("issued at");
            let scope = new.scope();
            PromotionPermitInfo {
                promotion_permit_id: new.promotion_permit_id(),
                idempotency_key: new.idempotency_key().clone(),
                scope_hash: new.scope_hash(),
                issuance_hash: new.issuance_hash(),
                feedback_cycle_id: scope.feedback_cycle_id(),
                profile_ref: scope.profile_ref().clone(),
                research_profile_artifact_id: scope.profile_ref().artifact_id(),
                profile_hash: scope.profile_ref().content_hash,
                category: scope.category(),
                expected_policy_generation: scope.expected_policy_generation(),
                expected_runtime_control_revision: scope.expected_runtime_control_revision(),
                expected_decision_policy_snapshot_id: scope.expected_snapshot_id(),
                expected_snapshot_hash: scope.expected_snapshot_hash(),
                champion_model_version_id: scope.champion_model_version_id(),
                champion_serving_contract_hash: scope.champion_serving_contract_hash(),
                candidate_model_version_id: scope.candidate_model_version_id(),
                candidate_manifest_id: scope.candidate_manifest_id(),
                candidate_manifest_hash: scope.candidate_manifest_hash(),
                promotion_gate_hash: scope.promotion_gate_hash(),
                allowed_runtime_modes: scope.allowed_runtime_modes().to_vec(),
                non_route_policy_hash: scope.non_route_policy_hash(),
                serving_constraints_hash: scope.serving_constraints_hash(),
                preflight_hash: new.preflight_hash(),
                issued_by_user_id: new.issued_by_user_id(),
                issued_by_username: new.issued_by_username().to_owned(),
                issued_by_role: new.issued_by_role().clone(),
                issuance_reason: new.issuance_reason().to_owned(),
                expires_at: scope.expires_at(),
                revoked_by_user_id: None,
                revoked_by_username: None,
                revoked_by_role: None,
                revocation_reason: None,
                revoked_at: None,
                revision: 0,
                issued_at,
                updated_at: issued_at,
            }
        }

        fn bundle(
            champion: ModelVersionId,
            candidate: ModelVersionId,
            weather: ModelVersionId,
        ) -> ActivePolicyBundle {
            let mut snapshot = DecisionPolicySnapshot::default();
            snapshot.model_routing.model.category_model_pointers = [
                (MarketCategory::Crypto, ModelVersionRef::new(champion)),
                (MarketCategory::Weather, ModelVersionRef::new(weather)),
            ]
            .into_iter()
            .collect();
            snapshot.model_routing.model.shadow_model_version_id =
                Some(ModelVersionRef::new(candidate));
            let snapshot_hash = snapshot.persistence_hash().expect("policy hash");
            ActivePolicyBundle::from_parts(
                PolicyBundleGeneration::FIRST,
                DecisionPolicySnapshotId::from_content_hash(&snapshot_hash),
                snapshot_hash,
                snapshot,
            )
        }

        fn constraints(&self, candidate: ModelVersionId) -> PromotionServingConstraints {
            let candidate_manifest_hash = hash(30);
            PromotionServingConstraints::try_new(PromotionServingConstraintsInput {
                candidate_model_version_id: candidate,
                candidate_manifest_id: ModelCandidateManifestId::from_content_hash(
                    &candidate_manifest_hash,
                ),
                candidate_manifest_hash,
                promotion_gate_hash: hash(31),
                candidate_model_spec_id: ModelSpecId::from_v7(),
                candidate_model_family: ModelFamily::WeightedFactor,
                candidate_artifact_hash: hash(31),
                candidate_serving_contract_hash: hash(32),
                candidate_model_spec_definition_hash: hash(33),
                candidate_training_dataset_id: TrainingDatasetId::from_v7(),
                feature_parity_run_id: FeatureParityRunId::from_v7(),
                feature_parity_state_id: FeatureParityStateId::from_v7(),
                feature_parity_evidence_hash: hash(35),
                profile_ref: self.profile_ref.clone(),
                category: MarketCategory::Crypto,
            })
            .expect("promotion serving constraints")
        }

        fn preflight(&self) -> PromotionPreflight {
            let champion = ModelVersionId::from_v7();
            let candidate = ModelVersionId::from_v7();
            let bundle = Self::bundle(champion, candidate, ModelVersionId::from_v7());
            let projection =
                PromotionPolicyProjection::try_new(&bundle, MarketCategory::Crypto, candidate)
                    .expect("promotion policy projection");
            let constraints = self.constraints(candidate);
            let cycle_hash = hash(35);
            let feedback_cycle_id = FeedbackCycleId::from_idempotency_hash(&cycle_hash);
            let scope = PromotionPermitScope::try_new(PromotionPermitScopeInput {
                feedback_cycle_id,
                profile_ref: self.profile_ref.clone(),
                category: MarketCategory::Crypto,
                expected_policy_generation: bundle.generation,
                expected_runtime_control_revision: 7,
                expected_decision_policy_snapshot_id: bundle.decision_policy_snapshot_id,
                expected_snapshot_hash: bundle.snapshot_hash,
                champion_model_version_id: champion,
                champion_serving_contract_hash: hash(34),
                candidate_model_version_id: constraints.candidate_model_version_id(),
                candidate_manifest_id: constraints.candidate_manifest_id(),
                candidate_manifest_hash: constraints.candidate_manifest_hash(),
                promotion_gate_hash: constraints.promotion_gate_hash(),
                allowed_runtime_modes: vec![
                    QuantRuntimeMode::ReportOnly,
                    QuantRuntimeMode::SemiAuto,
                ],
                non_route_policy_hash: projection.non_route_policy_hash(),
                serving_constraints_hash: constraints
                    .constraints_hash()
                    .expect("serving constraints hash"),
                expires_at: Utc
                    .with_ymd_and_hms(2026, 8, 1, 0, 0, 0)
                    .single()
                    .expect("preflight expiry"),
            })
            .expect("preflight scope");
            PromotionPreflight::try_seal(PromotionPreflightInput {
                scope,
                feedback_cycle_id,
                cycle_idempotency_hash: cycle_hash,
                decision_artifact_id: FeedbackDecisionArtifactId::from_cycle_id(feedback_cycle_id),
                decision_artifact_hash: hash(36),
                decision_object_hash: hash(37),
                decision_job_input_hash: hash(38),
                shadow_artifact_id: FeedbackShadowArtifactId::from_cycle_id(feedback_cycle_id),
                shadow_artifact_hash: hash(39),
                shadow_object_hash: hash(40),
                shadow_contract_hash: hash(41),
                candidate_recipe_hash: hash(42),
                serving_constraints: constraints,
                current_runtime_mode: QuantRuntimeMode::SemiAuto,
                runtime_control_revision: 7,
            })
            .expect("promotion preflight")
        }
    }

    fn hash(byte: u8) -> ContentHash {
        ContentHash::from_bytes([byte; 32])
    }

    #[test]
    fn validates_scope_and_roundtrip() {
        let scope = PermitFixture::new().scope();
        assert_eq!(
            scope.field_mask().expect("Crypto route field mask"),
            "model.category_model_pointers.crypto"
        );
        assert!(scope.allows_mode(QuantRuntimeMode::SemiAuto));
        assert!(!scope.allows_mode(QuantRuntimeMode::AutoExecution));
        let encoded = serde_json::to_vec(&scope).expect("serialize permit scope");
        let decoded =
            serde_json::from_slice::<PromotionPermitScope>(&encoded).expect("decode permit scope");
        assert_eq!(decoded, scope);
        assert_eq!(
            decoded.scope_hash().expect("scope hash"),
            scope.scope_hash().expect("scope hash")
        );

        let mut duplicate = serde_json::to_value(&scope).expect("scope value");
        duplicate["allowed_runtime_modes"] = serde_json::json!(["report_only", "report_only"]);
        assert!(serde_json::from_value::<PromotionPermitScope>(duplicate).is_err());

        let mut wrong_category = serde_json::to_value(&scope).expect("scope value");
        wrong_category["category"] = serde_json::json!("weather");
        assert!(serde_json::from_value::<PromotionPermitScope>(wrong_category).is_err());
    }

    #[test]
    fn derives_insert_active_model() {
        let active = PermitFixture::new().permit().into_active_model();
        assert_eq!(
            active.allowed_runtime_modes,
            ActiveValue::Set(vec![
                QuantRuntimeMode::ReportOnly,
                QuantRuntimeMode::SemiAuto
            ])
        );
        assert_eq!(active.revoked_by_user_id, ActiveValue::NotSet);
        assert_eq!(active.revoked_by_username, ActiveValue::NotSet);
        assert_eq!(active.revoked_by_role, ActiveValue::NotSet);
        assert_eq!(active.revocation_reason, ActiveValue::NotSet);
        assert_eq!(active.revoked_at, ActiveValue::NotSet);
        assert_eq!(active.revision, ActiveValue::NotSet);
        assert_eq!(active.issued_at, ActiveValue::NotSet);
        assert_eq!(active.updated_at, ActiveValue::NotSet);
    }

    #[test]
    fn derives_expiry_and_revoke() {
        let mut permit = PermitFixture::new().info();
        permit.validate().expect("validate active permit");
        assert_eq!(
            permit
                .status_at(permit.expires_at - Duration::nanoseconds(1))
                .expect("active status"),
            PromotionPermitStatus::Active
        );
        assert_eq!(
            permit.status_at(permit.expires_at).expect("expired status"),
            PromotionPermitStatus::Expired
        );

        let revoked_at = permit.issued_at + Duration::hours(1);
        let revocation = PromotionPermitRevocation {
            revoked_by_user_id: UserId::from_v7(),
            revoked_by_username: "risk-owner".to_owned(),
            revoked_by_role: RoleCode::new("risk_owner"),
            revocation_reason: "promotion window closed".to_owned(),
            revoked_at,
        };
        assert_eq!(
            permit
                .check_revocation(&revocation)
                .expect("new revocation"),
            PromotionPermitRevocationCheck::Apply
        );
        permit.revoked_by_user_id = Some(revocation.revoked_by_user_id);
        permit.revoked_by_username = Some(revocation.revoked_by_username.clone());
        permit.revoked_by_role = Some(revocation.revoked_by_role.clone());
        permit.revocation_reason = Some(revocation.revocation_reason.clone());
        permit.revoked_at = Some(revoked_at);
        permit.revision = 1;
        permit.updated_at = revoked_at;
        permit.validate().expect("validate revoked permit");
        assert_eq!(
            permit.status_at(revoked_at).expect("revoked status"),
            PromotionPermitStatus::Revoked
        );
        assert_eq!(
            permit
                .check_revocation(&revocation)
                .expect("exact revoke replay"),
            PromotionPermitRevocationCheck::ExactReplay
        );

        let mut drift = revocation;
        drift.revocation_reason = "different reason".to_owned();
        assert!(permit.check_revocation(&drift).is_err());
    }

    #[test]
    fn projects_only_promotion_delta() {
        let champion = ModelVersionId::from_v7();
        let candidate = ModelVersionId::from_v7();
        let weather = ModelVersionId::from_v7();
        let bundle = PermitFixture::bundle(champion, candidate, weather);
        let projection =
            PromotionPolicyProjection::try_new(&bundle, MarketCategory::Crypto, candidate)
                .expect("project exact promotion");
        assert_eq!(projection.champion_model_version_id(), champion);
        assert_eq!(projection.candidate_model_version_id(), candidate);
        assert_eq!(
            projection
                .prospective_snapshot()
                .model_routing
                .model
                .category_model_pointers
                .get(&MarketCategory::Weather)
                .expect("weather route")
                .id,
            weather
        );
        assert!(
            projection
                .prospective_snapshot()
                .model_routing
                .model
                .shadow_model_version_id
                .is_none()
        );

        let mut candidate_snapshot = projection.prospective_snapshot().clone();
        candidate_snapshot.revisions.model_routing = Some(PolicyRevisionId::from_v7());
        projection
            .validate_candidate(&candidate_snapshot)
            .expect("derived routing revision is normalized");
        candidate_snapshot
            .model_routing
            .model
            .category_model_pointers
            .insert(
                MarketCategory::Weather,
                ModelVersionRef::new(ModelVersionId::from_v7()),
            );
        assert!(projection.validate_candidate(&candidate_snapshot).is_err());

        let mut aliased = bundle.snapshot.clone();
        aliased
            .model_routing
            .model
            .category_model_pointers
            .insert(MarketCategory::Weather, ModelVersionRef::new(candidate));
        let aliased_hash = aliased.persistence_hash().expect("aliased policy hash");
        let aliased_bundle = ActivePolicyBundle::from_parts(
            bundle.generation,
            DecisionPolicySnapshotId::from_content_hash(&aliased_hash),
            aliased_hash,
            aliased,
        );
        assert!(
            PromotionPolicyProjection::try_new(&aliased_bundle, MarketCategory::Crypto, candidate)
                .is_err()
        );
    }

    #[test]
    fn seals_preflight_roundtrip() {
        let preflight = PermitFixture::new().preflight();
        preflight.validate().expect("valid preflight");
        let encoded = serde_json::to_vec(&preflight).expect("serialize preflight");
        let decoded =
            serde_json::from_slice::<PromotionPreflight>(&encoded).expect("decode preflight");
        assert_eq!(decoded, preflight);
        assert_eq!(decoded.preflight_hash(), preflight.preflight_hash());

        let mut mode_drift = serde_json::to_value(&preflight).expect("preflight value");
        mode_drift["current_runtime_mode"] = serde_json::json!("auto_execution");
        assert!(serde_json::from_value::<PromotionPreflight>(mode_drift).is_err());

        let mut artifact_drift = serde_json::to_value(&preflight).expect("preflight value");
        artifact_drift["decision_artifact_hash"] = serde_json::json!(hash(99));
        assert!(serde_json::from_value::<PromotionPreflight>(artifact_drift).is_err());
    }
}
