//! Side-effect-free verification of complete trade-policy preimages.

use std::sync::Arc;

use quant_pivot_error::{QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::quant::{
        ModelSpecInfo, ModelVersionInfo, TradePolicyArtifactInfo, TrainingDatasetInfo,
        TrainingDatasetMaterialization,
    },
    enums::quant::{DatasetPurpose, TradePolicyStatus, TrainingDatasetStatus},
    types::{
        ContentHash, DecisionPolicySnapshotId, ResearchProfileArtifact, TradePolicyArtifactId,
        model_serving::{ModelServingPolicySnapshotBinding, ModelServingTradePolicyBinding},
    },
};
use quant_pivot_repository::traits::{
    ModelRegistryRepository, TradePolicyRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    hashing::ResearchHasher, model::model_input_contract_hash, training::label_names_for_sources,
};

use crate::service::{
    model_serving_preimage::{
        ModelServingPreimageService, PreimageVerificationDepth, VerifiedModelServingPreimage,
    },
    trade_policy_evidence::{TradePolicyEvidenceDurability, TradePolicyEvidenceVerifier},
    training_dataset::require_dataset_materialization,
};

/// Canonical persisted inputs against which a trade-policy binding is checked.
#[derive(Clone, Copy)]
pub struct TradePolicyPreimageTarget<'a> {
    pub dataset: &'a TrainingDatasetInfo,
    pub model_spec: &'a ModelSpecInfo,
    pub policy_snapshot: &'a ModelServingPolicySnapshotBinding,
    pub profile: &'a ResearchProfileArtifact,
}

/// Read-only repositories used to resolve a trade-policy dependency graph.
pub struct TradePolicyPreimageVerifierDeps {
    pub trade_policy_repo: Arc<dyn TradePolicyRepository>,
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    pub evidence: Arc<TradePolicyEvidenceVerifier>,
}

/// Complete canonical preimages behind one trade-policy serving binding.
pub struct VerifiedTradePolicyPreimage {
    binding: ModelServingTradePolicyBinding,
    policy: TradePolicyArtifactInfo,
    source_dataset: TrainingDatasetInfo,
    subject: ModelVersionInfo,
    subject_preimage: VerifiedModelServingPreimage,
}

impl VerifiedTradePolicyPreimage {
    #[must_use]
    pub const fn binding(&self) -> &ModelServingTradePolicyBinding {
        &self.binding
    }

    #[must_use]
    pub const fn policy(&self) -> &TradePolicyArtifactInfo {
        &self.policy
    }

    #[must_use]
    pub const fn source_dataset(&self) -> &TrainingDatasetInfo {
        &self.source_dataset
    }

    #[must_use]
    pub const fn subject(&self) -> &ModelVersionInfo {
        &self.subject
    }

    #[must_use]
    pub const fn subject_preimage(&self) -> &VerifiedModelServingPreimage {
        &self.subject_preimage
    }
}

struct TradePolicyCrossBindings<'a> {
    policy: &'a TradePolicyArtifactInfo,
    source_dataset: &'a TrainingDatasetInfo,
    subject: &'a ModelVersionInfo,
    subject_preimage: &'a VerifiedModelServingPreimage,
    target: TradePolicyPreimageTarget<'a>,
}

impl TradePolicyCrossBindings<'_> {
    fn verify(&self) -> QuantResult<()> {
        let source = require_dataset_materialization(self.source_dataset)?;
        let target = require_dataset_materialization(self.target.dataset)?;
        self.verify_source_dataset(&source)?;
        let subject_input_hash = self.verify_subject(&source)?;
        self.verify_target_binding(&source, &target, &subject_input_hash)
    }

    fn verify_source_dataset(
        &self,
        source: &TrainingDatasetMaterialization<'_>,
    ) -> QuantResult<()> {
        let payload = &self.policy.payload_json;
        let fit = &payload.fit_contract;
        if self.source_dataset.status != TrainingDatasetStatus::Ready
            || self.source_dataset.purpose != DatasetPurpose::PolicyFit
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy source Dataset {} must be Ready/PolicyFit",
                    self.source_dataset.training_dataset_id
                ),
            }
            .into());
        }
        let sample_sources = self.source_dataset.sample_sources.as_ref().ok_or_else(|| {
            ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy source Dataset {} has no frozen sample-source contract",
                    self.source_dataset.training_dataset_id
                ),
            }
        })?;
        let label_names = label_names_for_sources(
            sample_sources.as_slice(),
            source.manifest.trade_policy_artifact_id.is_some(),
        );
        let canonical_label_schema_hash = ResearchHasher::label_schema(&label_names)?;
        if self.source_dataset.training_dataset_id != fit.source_dataset_id
            || source.manifest.training_dataset_id != fit.source_dataset_id
            || payload.source_dataset_hash != *source.dataset_hash
            || payload.feature_schema_hash != *source.feature_schema_hash
            || payload.label_schema_hash != *source.label_schema_hash
            || canonical_label_schema_hash != *source.label_schema_hash
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy {} Dataset identity/hash/schema preimage differs from the canonical PolicyFit Dataset",
                    self.policy.artifact_id
                ),
            }
            .into());
        }
        let source_lineage = &source.manifest.source_lineage;
        if source_lineage.research_profile_artifact_id.profile_ref()
            != self.target.profile.profile_ref
            || source_lineage.research_program_hash != fit.research_program_hash
            || source_lineage.decision_policy_snapshot_id != fit.decision_policy_snapshot_id
            || DecisionPolicySnapshotId::from_content_hash(&source_lineage.runtime_config_hash)
                != fit.decision_policy_snapshot_id
            || self.source_dataset.window_start > fit.fit_window_start
            || self.source_dataset.window_end < fit.fit_window_end
            || self.source_dataset.pit_cutoff > fit.pit_cutoff
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy {} profile/program/window/PIT lineage differs from its canonical PolicyFit Dataset",
                    self.policy.artifact_id
                ),
            }
            .into());
        }
        if payload.evidence_bundle.as_ref().is_none_or(|evidence| {
            evidence.source_slice_manifest_hash != source_lineage.source_slice.manifest_hash
        }) {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy {} evidence bundle differs from the PolicyFit Source Slice",
                    self.policy.artifact_id
                ),
            }
            .into());
        }
        Ok(())
    }

    fn verify_subject(
        &self,
        source: &TrainingDatasetMaterialization<'_>,
    ) -> QuantResult<ContentHash> {
        let fit = &self.policy.payload_json.fit_contract;
        let subject_artifact = self.subject_preimage.artifact();
        let subject_spec = self.subject_preimage.model_spec();
        let serving = subject_artifact.header().serving_contract();
        let bindings = serving.bindings();
        let model = &bindings.model;
        let subject_contract = self.subject.verified_serving_contract().map_err(|error| {
            ResearchError::InvalidModelArtifact {
                detail: format!(
                    "trade-policy subject {} has an invalid serving contract: {error}",
                    self.subject.model_version_id
                ),
            }
        })?;
        if subject_contract != serving || self.subject.model_version_id != fit.model_version_id {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy {} model subject is not the exact route-bound serving artifact",
                    self.policy.artifact_id
                ),
            }
            .into());
        }
        self.subject.verified_derivation().map_err(|error| {
            ResearchError::InvalidModelArtifact {
                detail: format!(
                    "trade-policy subject {} has invalid derivation lineage: {error}",
                    self.subject.model_version_id
                ),
            }
        })?;

        let definition = subject_spec.definition();
        definition
            .validate()
            .map_err(|detail| ResearchError::InvalidModelArtifact {
                detail: format!(
                    "trade-policy subject ModelSpec {} is invalid: {detail}",
                    subject_spec.model_spec_id
                ),
            })?;
        let definition_hash =
            definition
                .content_hash()
                .map_err(|error| ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "trade-policy subject ModelSpec {} hash failed: {error}",
                        subject_spec.model_spec_id
                    ),
                })?;
        let subject_input_hash = model_input_contract_hash(&subject_spec.input_contract)?;
        let expected_category = self.target.profile.spec.category;
        let expected_horizon = self.target.profile.spec.target_horizon_secs;
        if definition_hash != subject_spec.definition_hash
            || subject_spec.model_spec_id != self.subject.model_spec_id
            || subject_spec.definition_hash != self.subject.model_spec_definition_hash
            || subject_spec.model_family != self.subject.model_family
            || subject_spec.prediction_horizon_secs
                != self.subject.model_spec_prediction_horizon_secs
            || source.manifest.model_spec_id != subject_spec.model_spec_id
            || source.manifest.model_spec_definition_hash != definition_hash
            || source.manifest.model_family != subject_spec.model_family
            || source.manifest.feature_schema_version != subject_spec.feature_schema_version
            || model.model_spec_id != subject_spec.model_spec_id
            || model.model_spec_definition_hash != definition_hash
            || model.model_family != subject_spec.model_family
            || model.category_scope != expected_category
            || model.profile_ref != self.target.profile.profile_ref
            || model.prediction_horizon_secs != expected_horizon
            || bindings.transform.input_contract_hash != subject_input_hash
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy {} subject family/category/horizon/ModelSpec/input preimage differs from its Dataset and serving bindings",
                    self.policy.artifact_id
                ),
            }
            .into());
        }
        let source_lineage = &source.manifest.source_lineage;
        if bindings.schemas.feature_schema_hash != *source.feature_schema_hash
            || bindings.factors.plane != *source.factor_serving_plane
            || bindings.policy_snapshot.profile_artifacts
                != self.target.policy_snapshot.profile_artifacts
            || bindings.capability_registry_hashes != source_lineage.capability_registry_hashes
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy {} subject serving schema/factor/profile/capability preimage differs from the PolicyFit Dataset or target training context",
                    self.policy.artifact_id
                ),
            }
            .into());
        }
        Ok(subject_input_hash)
    }

    fn verify_target_binding(
        &self,
        source: &TrainingDatasetMaterialization<'_>,
        target: &TrainingDatasetMaterialization<'_>,
        subject_input_hash: &ContentHash,
    ) -> QuantResult<()> {
        let subject_spec = self.subject_preimage.model_spec();
        let target_input_hash = model_input_contract_hash(&self.target.model_spec.input_contract)?;
        if self.target.model_spec.model_family != subject_spec.model_family
            || self.target.model_spec.prediction_horizon_secs
                != subject_spec.prediction_horizon_secs
            || self.target.model_spec.feature_schema_version != subject_spec.feature_schema_version
            || target_input_hash != *subject_input_hash
            || target.feature_schema_hash != source.feature_schema_hash
            || target.factor_serving_plane != source.factor_serving_plane
            || target.manifest.source_lineage.capability_registry_hashes
                != source.manifest.source_lineage.capability_registry_hashes
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy {} target ModelSpec/Dataset family/horizon/schema/input/capability context differs from its fitted subject",
                    self.policy.artifact_id
                ),
            }
            .into());
        }
        Ok(())
    }
}

/// Canonical resolver for every immutable dependency behind a trade policy.
pub struct TradePolicyPreimageVerifier {
    deps: TradePolicyPreimageVerifierDeps,
}

impl TradePolicyPreimageVerifier {
    #[must_use]
    pub const fn new(deps: TradePolicyPreimageVerifierDeps) -> Self {
        Self { deps }
    }

    /// Resolve and verify a target's optional trade-policy binding.
    pub async fn verify(
        &self,
        serving_preimages: &ModelServingPreimageService,
        target: TradePolicyPreimageTarget<'_>,
        durability: TradePolicyEvidenceDurability,
    ) -> QuantResult<Option<VerifiedTradePolicyPreimage>> {
        Box::pin(self.verify_depth(
            serving_preimages,
            target,
            durability,
            PreimageVerificationDepth::FullObjects,
        ))
        .await
    }

    pub(crate) async fn verify_depth(
        &self,
        serving_preimages: &ModelServingPreimageService,
        target: TradePolicyPreimageTarget<'_>,
        durability: TradePolicyEvidenceDurability,
        depth: PreimageVerificationDepth,
    ) -> QuantResult<Option<VerifiedTradePolicyPreimage>> {
        let manifest = require_dataset_materialization(target.dataset)?.manifest;
        let (Some(artifact_id), Some(expected_hash)) = (
            manifest.trade_policy_artifact_id,
            manifest.trade_policy_hash,
        ) else {
            if manifest.trade_policy_artifact_id.is_some()
                || manifest.trade_policy_hash.is_some()
                || target
                    .model_spec
                    .training_contract
                    .evaluation_trade_policy_artifact_id
                    .is_some()
            {
                return Err(ResearchError::DatasetBuild {
                    detail: "Dataset v3 carries an incomplete trade-policy binding".to_owned(),
                }
                .into());
            }
            return Ok(None);
        };
        let policy = self
            .deps
            .trade_policy_repo
            .find(&artifact_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "trade_policy_artifact",
                id: artifact_id.to_string(),
            })?;
        let binding = Self::verify_target(artifact_id, expected_hash, &policy, target)?;
        self.deps
            .evidence
            .verify(&policy.payload_json, durability)
            .await?;

        let source_dataset_id = policy.source_dataset_id;
        let source_dataset = self
            .deps
            .dataset_repo
            .find_by_id(&source_dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: source_dataset_id.to_string(),
            })?;
        serving_preimages
            .verify_dataset(&source_dataset, target.profile, depth)
            .await?;

        let subject_id = policy.payload_json.fit_contract.model_version_id;
        let subject = self
            .deps
            .model_registry_repo
            .find_model_version(&subject_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_model_version",
                id: subject_id.to_string(),
            })?;
        let subject_preimage = serving_preimages.load_base(&subject, depth).await?;
        TradePolicyCrossBindings {
            policy: &policy,
            source_dataset: &source_dataset,
            subject: &subject,
            subject_preimage: &subject_preimage,
            target,
        }
        .verify()?;
        Ok(Some(VerifiedTradePolicyPreimage {
            binding,
            policy,
            source_dataset,
            subject,
            subject_preimage,
        }))
    }

    fn verify_target(
        artifact_id: TradePolicyArtifactId,
        expected_hash: ContentHash,
        policy: &TradePolicyArtifactInfo,
        target: TradePolicyPreimageTarget<'_>,
    ) -> QuantResult<ModelServingTradePolicyBinding> {
        let actual_hash = ResearchHasher::canonical(&policy.payload_json)?;
        if policy.artifact_id != artifact_id
            || policy.content_hash != expected_hash
            || actual_hash != expected_hash
            || TradePolicyArtifactId::from_content_hash(&actual_hash) != artifact_id
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy {artifact_id} failed canonical row/payload identity verification"
                ),
            }
            .into());
        }
        if policy.status != TradePolicyStatus::Published || !policy.payload_json.is_publishable() {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy {artifact_id} is not an intact publishable Published artifact"
                ),
            }
            .into());
        }

        let materialization = require_dataset_materialization(target.dataset)?;
        let expected_contract_id = target
            .model_spec
            .training_contract
            .evaluation_trade_policy_artifact_id;
        if expected_contract_id != Some(artifact_id)
            || materialization.manifest.trade_policy_artifact_id != Some(artifact_id)
            || materialization.manifest.trade_policy_hash != Some(expected_hash)
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy {artifact_id} differs from the exact ModelSpec/Dataset binding"
                ),
            }
            .into());
        }

        let fit = &policy.payload_json.fit_contract;
        fit.validate()
            .map_err(|detail| ResearchError::DatasetBuild {
                detail: format!("trade-policy {artifact_id} fit contract is invalid: {detail}"),
            })?;
        if fit.profile_ref != target.profile.profile_ref {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy ResearchProfile mismatch: policy {}@{}, training Dataset {}@{}",
                    fit.profile_ref.id,
                    fit.profile_ref.version,
                    target.profile.profile_ref.id,
                    target.profile.profile_ref.version,
                ),
            }
            .into());
        }
        if DecisionPolicySnapshotId::from_content_hash(&target.policy_snapshot.snapshot_hash)
            != target.policy_snapshot.decision_policy_snapshot_id
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy {artifact_id} target training policy binding is not content-addressed"
                ),
            }
            .into());
        }
        if fit.target_horizon_secs != target.profile.spec.target_horizon_secs {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy {artifact_id} horizon differs from the canonical ResearchProfile"
                ),
            }
            .into());
        }
        if policy.source_dataset_id != fit.source_dataset_id {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy {artifact_id} source Dataset row projection differs from its payload"
                ),
            }
            .into());
        }
        let cohort_drift = policy.payload_json.cohorts.iter().any(|cohort| {
            cohort.key.profile_ref != target.profile.profile_ref
                || cohort.key.horizon_secs != target.profile.spec.target_horizon_secs
                || target
                    .profile
                    .spec
                    .category
                    .is_some_and(|category| cohort.key.category != category)
        });
        if cohort_drift {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "trade-policy {artifact_id} cohort subject differs from its canonical ResearchProfile"
                ),
            }
            .into());
        }

        Ok(ModelServingTradePolicyBinding {
            artifact_id,
            content_hash: actual_hash,
        })
    }
}
