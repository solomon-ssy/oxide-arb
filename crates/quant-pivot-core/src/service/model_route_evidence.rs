//! Shared fail-closed evidence resolver for model-route governance.

use std::sync::Arc;

use quant_pivot_error::{QuantError, QuantResult, feedback::FeedbackError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        governance::RuntimeControlInfo,
        quant::{FrozenFeatureParitySubjectId, ModelVersionInfo},
    },
    enums::quant::{FeatureParityLatchState, FeatureParityRunKind, FeatureParityRunStatus},
    runtime_config::{ActivePolicyBundle, BuyModelRoute},
    types::{ContentHash, FeatureParityRunId, FeatureParityStateId, ModelVersionId},
};
use quant_pivot_repository::traits::{
    FeatureParityRepository, ModelRegistryRepository, PolicyRepository, RuntimeControlRepository,
};

use crate::{
    governance::RuntimeControlsHandle,
    runtime_config::DecisionPolicyStore,
    service::{
        model_serving_generation::{ModelServingGenerationStore, ModelServingRouteSnapshot},
        model_serving_registry::ModelServingRuntimeRegistry,
    },
};

/// Exact full-parity proof used by bootstrap and promotion preflights.
#[derive(Debug, Clone, Copy)]
pub struct ModelRouteParityProof {
    pub run_id: FeatureParityRunId,
    pub state_id: FeatureParityStateId,
    pub evidence_hash: ContentHash,
}

/// Persistence and runtime snapshots shared by route transitions.
pub struct ModelRouteEvidenceDeps {
    pub policies: Arc<dyn PolicyRepository>,
    pub durable_runtime: Arc<dyn RuntimeControlRepository>,
    pub runtime_controls: RuntimeControlsHandle,
    pub policy_store: Arc<DecisionPolicyStore>,
    pub models: Arc<dyn ModelRegistryRepository>,
    pub feature_parity: Arc<dyn FeatureParityRepository>,
    pub runtime_registry: Arc<ModelServingRuntimeRegistry>,
    pub serving_generations: Arc<ModelServingGenerationStore>,
}

/// Canonical resolver for current policy/runtime/model/parity truth.
pub struct ModelRouteEvidenceService {
    deps: ModelRouteEvidenceDeps,
}

impl ModelRouteEvidenceService {
    #[must_use]
    pub const fn new(deps: ModelRouteEvidenceDeps) -> Self {
        Self { deps }
    }

    pub async fn current_bundle(&self) -> QuantResult<ActivePolicyBundle> {
        let durable = self
            .deps
            .policies
            .load_current_bundle()
            .await?
            .ok_or_else(|| Self::invalid("no committed policy bundle exists"))?;
        let local = self
            .deps
            .policy_store
            .active_bundle()
            .ok_or_else(|| Self::invalid("local committed policy bundle is unavailable"))?;
        let actual_hash = durable.snapshot.persistence_hash().map_err(|error| {
            Self::invalid(format!("committed policy hash projection failed: {error}"))
        })?;
        let validation = durable.snapshot.validate_runtime_config();
        if durable != local
            || actual_hash != durable.snapshot_hash
            || durable.revision_vector != durable.snapshot.revisions
            || validation.has_errors()
        {
            return Err(Self::invalid(format!(
                "durable/local policy bundle, hash, revisions, or validation differ: {validation}"
            )));
        }
        Ok(durable)
    }

    pub async fn current_runtime(&self) -> QuantResult<RuntimeControlInfo> {
        let durable = self.deps.durable_runtime.load().await?;
        let local = self.deps.runtime_controls.snapshot();
        if durable.entry_authorization_policy != local.entry_authorization_policy
            || durable.settlement_write_policy != local.settlement_write_policy
            || durable.kill_switch_state != local.kill_switch_state
            || durable.kill_switch_requires_ack != local.kill_switch_requires_ack
            || durable.revision != local.revision
            || durable.changed_by != local.changed_by
            || durable.reason != local.reason
            || durable.changed_at != local.changed_at
        {
            return Err(Self::invalid(
                "durable runtime control differs from the atomically applied local revision",
            ));
        }
        Ok(durable)
    }

    pub async fn load_model(&self, model_id: ModelVersionId) -> QuantResult<ModelVersionInfo> {
        self.deps
            .models
            .find_model_version(&model_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_model_version", model_id).into())
    }

    pub async fn load_runtime(&self, model: &ModelVersionInfo) -> QuantResult<()> {
        self.deps.runtime_registry.load(model).await.map(|_| ())
    }

    #[must_use]
    pub fn current_route(&self, route: BuyModelRoute) -> Option<ModelServingRouteSnapshot> {
        self.deps.serving_generations.current_route(route)
    }

    pub async fn parity_proof(
        &self,
        candidate: &ModelVersionInfo,
    ) -> QuantResult<ModelRouteParityProof> {
        let training_dataset_id = candidate.training_dataset_id.ok_or_else(|| {
            Self::invalid("route candidate has no exact training dataset binding")
        })?;
        let contract = candidate
            .verified_serving_contract()
            .map_err(|error| Self::invalid(error.to_string()))?;
        let run = self
            .deps
            .feature_parity
            .latest_full_for_model(&candidate.model_version_id, &training_dataset_id)
            .await?
            .ok_or_else(|| Self::invalid("route candidate has no full parity proof"))?;
        let run_exact = run.kind == FeatureParityRunKind::Full
            && run.status == FeatureParityRunStatus::Passed
            && run.report_id.is_none()
            && run.model_version_id == Some(candidate.model_version_id)
            && run.training_dataset_id == Some(training_dataset_id)
            && run.total_count > 0
            && run.compared_count == run.total_count
            && run.matched_count == run.total_count
            && run.mismatched_count == 0
            && run.pending_materialization_count == 0
            && run.feature_contract_hash == Some(contract.bindings().schemas.feature_schema_hash)
            && run.transform_hash == Some(contract.bindings().transform.input_transform_hash)
            && run
                .finished_at
                .is_some_and(|finished_at| finished_at >= candidate.created_at)
            && run.failure_code.is_none()
            && run.failure_detail.is_none();
        if !run_exact {
            return Err(Self::invalid(
                "latest full parity proof is not an exact passed route proof",
            ));
        }
        let mut subjects = self
            .deps
            .feature_parity
            .load_frozen_subjects(&run.run_id)
            .await?;
        if subjects.len() != 1 {
            return Err(Self::invalid(
                "route full parity proof must have exactly one frozen subject",
            ));
        }
        let subject = subjects
            .pop()
            .ok_or_else(|| Self::invalid("route full parity subject disappeared"))?;
        let subject_exact = matches!(
            subject.subject_id,
            FrozenFeatureParitySubjectId::ModelVersion {
                model_version_id,
                training_dataset_id: subject_dataset_id,
            } if model_version_id == candidate.model_version_id
                && subject_dataset_id == training_dataset_id
        ) && subject.subject_generation == candidate.artifact_hash
            && subject.market_selection_id.is_none()
            && subject.decision_at.is_none()
            && subject.selection_hash.is_none()
            && subject.candidates.is_empty();
        if !subject_exact {
            return Err(Self::invalid(
                "route full parity subject differs from the immutable model artifact",
            ));
        }
        if let Some(unsettled) = self.deps.feature_parity.find_unsettled_runtime().await? {
            return Err(Self::invalid(format!(
                "runtime feature parity run {} is still {}; route governance requires settled serving evidence",
                unsettled.run_id,
                unsettled.status.as_str()
            )));
        }
        let state = self
            .deps
            .feature_parity
            .current_state()
            .await?
            .ok_or_else(|| Self::invalid("feature parity latch is uninitialized"))?;
        if state.state != FeatureParityLatchState::Clear {
            return Err(Self::invalid(
                "feature parity latch is not clear for route governance",
            ));
        }
        Ok(ModelRouteParityProof {
            run_id: run.run_id,
            state_id: state.state_id,
            evidence_hash: subject.evidence_hash,
        })
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidModelRouteEvidence {
            detail: detail.into(),
        }
        .into()
    }
}
