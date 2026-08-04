//! Side-effect-free first-champion route bootstrap preflight.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, feedback::FeedbackError};
use quant_pivot_models::{
    domain::{
        ports::{
            BootstrapQualityGateEvidence, BootstrapQualityGateInput, CandidateQualityGateEvidence,
            ModelGovernancePort,
        },
        quant::{
            BacktestPathSetInfo, CandidateExplanationValidation, ModelBootstrapManifest,
            ModelBootstrapManifestInput, ModelBootstrapPolicyProjection,
            ModelRouteBootstrapPreflight, ModelRouteBootstrapPreflightInput, ModelVersionInfo,
        },
    },
    enums::{model::ModelFamily, quant::QuantRuntimeMode, runtime_config::ConfigResourceKind},
    runtime_config::BuyModelRoute,
    types::{DecisionPolicySnapshotId, ModelVersionId, TrainingDatasetId},
};
use quant_pivot_repository::traits::{
    BacktestPathSetRepository, BacktestReportRepository, FeedbackCycleRepository,
};

use crate::service::model_route_evidence::ModelRouteEvidenceService;

/// Exact preflight and policy projection derived from current server truth.
#[derive(Debug, Clone)]
pub struct ModelRouteBootstrapPlan {
    preflight: ModelRouteBootstrapPreflight,
    projection: ModelBootstrapPolicyProjection,
}

impl ModelRouteBootstrapPlan {
    #[must_use]
    pub const fn preflight(&self) -> &ModelRouteBootstrapPreflight {
        &self.preflight
    }

    #[must_use]
    pub const fn projection(&self) -> &ModelBootstrapPolicyProjection {
        &self.projection
    }
}

/// Dependencies for the dedicated bootstrap preflight.
pub struct ModelRouteBootstrapServiceDeps {
    pub route_evidence: Arc<ModelRouteEvidenceService>,
    pub path_sets: Arc<dyn BacktestPathSetRepository>,
    pub backtests: Arc<dyn BacktestReportRepository>,
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub model_governance: Arc<dyn ModelGovernancePort>,
}

/// Canonical owner of first-champion candidate evidence resolution.
pub struct ModelRouteBootstrapService {
    deps: ModelRouteBootstrapServiceDeps,
}

struct BootstrapQualityEvidence {
    path_set: BacktestPathSetInfo,
    quality: BootstrapQualityGateEvidence,
}

impl ModelRouteBootstrapService {
    #[must_use]
    pub const fn new(deps: ModelRouteBootstrapServiceDeps) -> Self {
        Self { deps }
    }

    /// Derive the full bootstrap proof without mutating serving authority.
    pub async fn prepare(
        &self,
        model_version_id: ModelVersionId,
    ) -> QuantResult<ModelRouteBootstrapPlan> {
        let evaluated_at = self.deps.cycles.database_time().await?;
        let bundle = self
            .deps
            .route_evidence
            .current_bundle()
            .await
            .map_err(Self::route_error)?;
        let runtime = self
            .deps
            .route_evidence
            .current_runtime()
            .await
            .map_err(Self::route_error)?;
        if runtime.quant_runtime_mode != QuantRuntimeMode::ReportOnly {
            return Err(Self::invalid(
                "first-route bootstrap is allowed only in ReportOnly",
            ));
        }
        let model = self
            .deps
            .route_evidence
            .load_model(model_version_id)
            .await?;
        let route = BuyModelRoute::try_from(model.category_scope)
            .map_err(|error| Self::invalid(error.to_string()))?;
        if self.deps.route_evidence.current_route(route).is_some() {
            return Err(Self::invalid(
                "in-memory serving generation already contains the bootstrap target route",
            ));
        }
        if !matches!(
            model.model_family,
            ModelFamily::WeightedFactor | ModelFamily::ClassicalGradientBoostedTrees
        ) {
            return Err(Self::invalid(
                "bootstrap candidate family is not executable by the canonical runtime",
            ));
        }
        self.deps
            .route_evidence
            .load_runtime(&model)
            .await
            .map_err(Self::route_error)?;
        let contract = model
            .verified_serving_contract()
            .map_err(|error| Self::invalid(error.to_string()))?;
        let bindings = contract.bindings();
        let training_dataset_id = model.training_dataset_id.ok_or_else(|| {
            Self::invalid("bootstrap candidate has no immutable training dataset")
        })?;
        let BootstrapQualityEvidence { path_set, quality } = self
            .quality_evidence(
                &model,
                training_dataset_id,
                bundle.decision_policy_snapshot_id,
                evaluated_at,
            )
            .await?;
        if !quality.quality_gate_report.passed {
            return Err(Self::invalid(format!(
                "bootstrap candidate failed {} hard quality gates",
                quality.quality_gate_report.hard_failures.len()
            )));
        }
        let parity = self
            .deps
            .route_evidence
            .parity_proof(&model)
            .await
            .map_err(Self::route_error)?;
        let explanation = CandidateExplanationValidation::try_from(bindings)
            .map_err(|error| Self::invalid(error.to_string()))?;
        let calibration = bindings
            .model
            .calibration
            .as_ref()
            .map(|binding| (binding.artifact_id, binding.content_hash));
        let manifest = ModelBootstrapManifest::try_seal(ModelBootstrapManifestInput {
            model_version_id: model.model_version_id,
            model_spec_id: model.model_spec_id,
            model_family: model.model_family,
            model_spec_definition_hash: model.model_spec_definition_hash,
            model_artifact_hash: model.artifact_hash,
            serving_contract_hash: model.serving_contract_hash,
            training_dataset_id,
            training_dataset_hash: bindings.transform.training_dataset_hash,
            dataset_manifest_hash: bindings.dataset.manifest_hash,
            dataset_artifact_hash: bindings.dataset.artifact_bytes_hash,
            feature_schema_hash: bindings.schemas.feature_schema_hash,
            input_contract_hash: bindings.transform.input_contract_hash,
            input_transform_hash: bindings.transform.input_transform_hash,
            calibration_artifact_id: calibration.map(|(id, _)| id),
            calibration_artifact_hash: calibration.map(|(_, hash)| hash),
            profile_ref: model.profile_ref.clone(),
            route,
            cpcv_path_set_id: path_set.path_set_id,
            cpcv_path_set_hash: path_set.path_set_hash,
            backtest_report_id: quality.backtest_report_id,
            backtest_report_hash: quality.backtest_report_hash,
            explanation_validation: explanation,
            quality_gate_report: quality.quality_gate_report,
            feature_parity_run_id: parity.run_id,
            feature_parity_state_id: parity.state_id,
            feature_parity_evidence_hash: parity.evidence_hash,
        })?;
        let projection = ModelBootstrapPolicyProjection::try_new(
            &bundle,
            route,
            model.model_version_id,
            evaluated_at,
        )?;
        let expected_model_routing_revision_id = bundle
            .snapshot
            .resource_revision_id(ConfigResourceKind::ModelRouting)
            .copied()
            .ok_or_else(|| Self::invalid("active snapshot has no ModelRouting revision"))?;
        let preflight =
            ModelRouteBootstrapPreflight::try_seal(ModelRouteBootstrapPreflightInput {
                manifest,
                expected_policy_generation: bundle.generation,
                expected_snapshot_id: bundle.decision_policy_snapshot_id,
                expected_snapshot_hash: bundle.snapshot_hash,
                expected_model_routing_revision_id,
                expected_runtime_control_revision: runtime.revision,
                current_runtime_mode: runtime.quant_runtime_mode,
                non_route_policy_hash: projection.non_route_policy_hash(),
                evaluated_at,
            })?;
        Ok(ModelRouteBootstrapPlan {
            preflight,
            projection,
        })
    }

    async fn quality_evidence(
        &self,
        model: &ModelVersionInfo,
        training_dataset_id: TrainingDatasetId,
        policy_snapshot_id: DecisionPolicySnapshotId,
        evaluated_at: DateTime<Utc>,
    ) -> QuantResult<BootstrapQualityEvidence> {
        let path_set = self
            .deps
            .path_sets
            .list_by_model_version(&model.model_version_id)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| Self::invalid("bootstrap candidate has no CPCV path set"))?;
        path_set
            .verify_hash()
            .map_err(|error| Self::invalid(error.to_string()))?;
        if path_set.model_version_id != model.model_version_id
            || path_set.training_dataset_id != training_dataset_id
            || path_set.decision_policy_snapshot_id != policy_snapshot_id
        {
            return Err(Self::invalid(
                "latest CPCV path set differs from the candidate or current policy snapshot",
            ));
        }
        let backtest = self
            .deps
            .backtests
            .list_by_model_version(&model.model_version_id)
            .await?
            .into_iter()
            .find(|report| report.decision_policy_snapshot_id == policy_snapshot_id)
            .ok_or_else(|| {
                Self::invalid("bootstrap candidate has no backtest for the current policy snapshot")
            })?;
        backtest.verify_hash().map_err(Self::invalid)?;
        let quality = self
            .deps
            .model_governance
            .evaluate_bootstrap(
                &model.model_version_id,
                BootstrapQualityGateInput {
                    candidate: CandidateQualityGateEvidence::Cpcv {
                        path_set_id: path_set.path_set_id,
                        path_set_hash: path_set.path_set_hash,
                    },
                    backtest_report_id: backtest.backtest_report_id,
                    backtest_report_hash: backtest.report_hash,
                },
                evaluated_at,
            )
            .await?;
        Ok(BootstrapQualityEvidence { path_set, quality })
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidBootstrapPreflight {
            detail: detail.into(),
        }
        .into()
    }

    fn route_error(error: QuantError) -> QuantError {
        match error {
            QuantError::Feedback(FeedbackError::InvalidModelRouteEvidence { detail }) => {
                Self::invalid(detail)
            }
            other => other,
        }
    }
}
