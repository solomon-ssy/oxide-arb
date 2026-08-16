//! Shared fail-closed CPCV evidence validation for first-route bootstrap.

use quant_pivot_error::feedback::FeedbackError;
use quant_pivot_models::{
    domain::{
        data_plane::HistoryFitSeal,
        quant::{BacktestPathSetInfo, ModelVersionInfo, TrainingDatasetInfo},
    },
    enums::quant::TrainingDatasetStatus,
    hashing::CanonicalDigest,
    types::{
        ContentHash, DecisionPolicySnapshotId, ResearchProfileArtifact, ResearchProfileArtifactId,
        backtest::CpcvFoldValidationRegime,
    },
};

/// Exact immutable inputs that must agree before CPCV can authorize bootstrap.
pub struct BootstrapCpcvEvidence<'a> {
    pub path_set: &'a BacktestPathSetInfo,
    pub model: &'a ModelVersionInfo,
    pub dataset: &'a TrainingDatasetInfo,
    pub fit_seal: &'a HistoryFitSeal,
    pub profile: &'a ResearchProfileArtifact,
    pub policy_snapshot_id: DecisionPolicySnapshotId,
    pub policy_snapshot_hash: ContentHash,
    pub required_regime: CpcvFoldValidationRegime,
}

impl BootstrapCpcvEvidence<'_> {
    /// Verify all relational and content-addressed identities used by bootstrap.
    pub fn validate(self) -> Result<(), FeedbackError> {
        self.path_set.verify_hash().map_err(|error| {
            Self::invalid(format!("CPCV path-set verification failed: {error}"))
        })?;
        self.model.verified_serving_contract().map_err(|error| {
            Self::invalid(format!(
                "bootstrap model serving contract is invalid: {error}"
            ))
        })?;
        let materialization = self.dataset.materialization().ok_or_else(|| {
            Self::invalid("bootstrap Dataset has no complete verified materialization")
        })?;
        if self.dataset.status != TrainingDatasetStatus::Ready {
            return Err(Self::invalid("bootstrap Dataset is not ready"));
        }

        let profile_artifact_id =
            ResearchProfileArtifactId::from_profile_ref(&self.profile.profile_ref);
        let cohort_hash = CanonicalDigest::content_hash_json(&self.profile.spec.cohort_contract)?;
        if self.model.profile_ref != self.profile.profile_ref
            || self.dataset.research_profile_artifact_id != profile_artifact_id
            || self.dataset.source_lineage.research_profile_artifact_id != profile_artifact_id
            || self.model.category_scope != self.profile.spec.category
            || self.profile.spec.cohort_contract.category() != self.profile.spec.category
            || self.fit_seal.seal.profile_hash != self.profile.profile_ref.content_hash
            || self.fit_seal.seal.cohort_hash != cohort_hash
        {
            return Err(Self::invalid(
                "bootstrap model, Dataset, profile, cohort, and FitSeal identities differ",
            ));
        }
        if self.dataset.source_lineage.fit_seal_id != self.fit_seal.seal.fit_seal_id
            || self.dataset.source_lineage.fit_seal_hash != self.fit_seal.seal.seal_hash
        {
            return Err(Self::invalid(
                "bootstrap Dataset lineage does not bind the validated FitSeal",
            ));
        }

        let subject = &self.path_set.subject;
        if self.path_set.model_version_id != self.model.model_version_id
            || self.path_set.training_dataset_id != self.dataset.training_dataset_id
            || self.path_set.decision_policy_snapshot_id != self.policy_snapshot_id
            || self.model.training_dataset_id != Some(self.dataset.training_dataset_id)
            || self.model.model_spec_id != self.dataset.model_spec_id
            || self.model.model_family != self.dataset.model_family
            || self.dataset.decision_policy_snapshot_id != self.policy_snapshot_id
            || subject.model_artifact_hash != self.model.artifact_hash
            || subject.serving_contract_hash != self.model.serving_contract_hash
            || subject.training_dataset_hash != *materialization.dataset_hash
            || subject.dataset_manifest_hash != *materialization.manifest_hash
            || subject.dataset_artifact_bytes_hash != *materialization.artifact_bytes_hash
            || subject.policy_snapshot_hash != self.policy_snapshot_hash
        {
            return Err(Self::invalid(
                "CPCV subject differs from the model, Dataset, or policy preimage",
            ));
        }
        let actual_regime = self
            .path_set
            .fold_artifacts
            .validation_regime()
            .map_err(|error| Self::invalid(error.to_string()))?;
        if actual_regime != self.required_regime {
            return Err(Self::invalid(format!(
                "CPCV validation regime is {actual_regime:?}; expected {:?}",
                self.required_regime
            )));
        }
        Ok(())
    }

    fn invalid(detail: impl Into<String>) -> FeedbackError {
        FeedbackError::InvalidBootstrapPreflight {
            detail: detail.into(),
        }
    }
}
