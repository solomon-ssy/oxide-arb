//! First-champion route bootstrap scenario fit and preflight.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, feedback::FeedbackError};
use quant_pivot_models::{
    domain::{
        governance::RuntimeControlInfo,
        ports::{
            BootstrapQualityGateEvidence, BootstrapQualityGateInput, CandidateQualityGateEvidence,
            ModelGovernancePort,
        },
        quant::{
            BacktestPathSetInfo, CalibrationArtifactInfo, CandidateExplanationValidation,
            ModelBootstrapManifest, ModelBootstrapManifestInput, ModelBootstrapPolicyProjection,
            ModelBootstrapValidationEvidence, ModelRouteBootstrapPreflight,
            ModelRouteBootstrapPreflightInput, ModelVersionInfo, PortfolioScenarioEvidenceRegime,
            PortfolioScenarioRouteModelLineage, RepresentedRouteSet, RouteCompatibilityDigests,
            RouteContractHash, TrainingDatasetInfo,
        },
    },
    enums::{
        model::ServingEligibility, quant::QuantRuntimeMode, runtime_config::ConfigResourceKind,
    },
    runtime_config::{ActivePolicyBundle, BuyModelRoute, PortfolioScenarioModelArtifactBinding},
    types::{ModelVersionId, ServingAuthority, backtest::CpcvFoldValidationRegime},
};
use quant_pivot_repository::traits::{
    BacktestPathSetRepository, BacktestReportRepository, CalibrationArtifactRepository,
    ExchangeHistoryRepository, FeedbackCycleRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    portfolio::{
        PortfolioScenarioMethodology, PortfolioScenarioModelFitInput, PortfolioScenarioModelFitter,
        PortfolioScenarioRouteFitInput,
    },
};

use crate::service::{
    bootstrap_cpcv_evidence::BootstrapCpcvEvidence,
    model_route_evidence::ModelRouteEvidenceService, portfolio_context::PromotedRouteContract,
};

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
    pub history: Arc<dyn ExchangeHistoryRepository>,
    pub backtests: Arc<dyn BacktestReportRepository>,
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub model_governance: Arc<dyn ModelGovernancePort>,
    pub calibrations: Arc<dyn CalibrationArtifactRepository>,
    pub datasets: Arc<dyn TrainingDatasetRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
}

/// Canonical owner of first-champion candidate evidence resolution.
pub struct ModelRouteBootstrapService {
    deps: ModelRouteBootstrapServiceDeps,
}

struct BootstrapQualityEvidence {
    path_set: BacktestPathSetInfo,
    quality: BootstrapQualityGateEvidence,
}

struct ScenarioRouteEvidence {
    route: BuyModelRoute,
    model: ModelVersionInfo,
    contract: PromotedRouteContract,
    path_set: BacktestPathSetInfo,
    calibration: CalibrationArtifactInfo,
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
        Self::validate_report_controls(&bundle, &runtime)?;
        let model = self
            .deps
            .route_evidence
            .load_model(model_version_id)
            .await?;
        let route = BuyModelRoute::try_from(model.category_scope)
            .map_err(|error| Self::invalid(error.to_string()))?;
        self.validate_target(&model, route)?;
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
        let dataset = self
            .deps
            .datasets
            .find_by_id(&training_dataset_id)
            .await?
            .ok_or_else(|| Self::invalid("bootstrap training dataset does not exist"))?;
        if dataset
            .sample_count
            .and_then(|count| u64::try_from(count).ok())
            .is_none()
        {
            return Err(Self::invalid(
                "bootstrap training dataset has no valid sample count",
            ));
        }
        if dataset.coverage.is_none() {
            return Err(Self::invalid(
                "bootstrap training dataset has no coverage ledger",
            ));
        }
        let BootstrapQualityEvidence { path_set, quality } = self
            .quality_evidence(&model, &dataset, &bundle, evaluated_at)
            .await?;
        if !quality.quality_gate_report.passed {
            return Err(Self::invalid(format!(
                "bootstrap candidate failed {} hard quality gates",
                quality.quality_gate_report.hard_failures.len()
            )));
        }
        let scenario_binding = self
            .fit_reference_scenario(&bundle, &model, &path_set, evaluated_at)
            .await?;
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
            validation_evidence: quality.validation_evidence,
            scenario_model_binding: scenario_binding.clone(),
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
            scenario_binding,
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

    fn validate_report_controls(
        bundle: &ActivePolicyBundle,
        runtime: &RuntimeControlInfo,
    ) -> QuantResult<()> {
        if runtime.quant_runtime_mode != QuantRuntimeMode::ReportOnly {
            return Err(Self::invalid(
                "first-route bootstrap is allowed only in ReportOnly",
            ));
        }
        if !bundle.snapshot.recommendation.reports.ad_hoc_report_enabled {
            return Err(Self::invalid(
                "first-route bootstrap requires governed ad-hoc reports to be enabled",
            ));
        }
        if !bundle
            .snapshot
            .report_schedule
            .schedules
            .iter()
            .any(|schedule| schedule.enabled)
        {
            return Err(Self::invalid(
                "first-route bootstrap requires at least one enabled report schedule",
            ));
        }
        Ok(())
    }

    fn validate_target(&self, model: &ModelVersionInfo, route: BuyModelRoute) -> QuantResult<()> {
        if self.deps.route_evidence.current_route(route).is_some() {
            return Err(Self::invalid(
                "in-memory serving generation already contains the bootstrap target route",
            ));
        }
        if model.model_family.serving_eligibility() != ServingEligibility::ActiveBuyCapable {
            return Err(Self::invalid(
                "bootstrap candidate family is not executable by the canonical runtime",
            ));
        }
        Ok(())
    }

    async fn quality_evidence(
        &self,
        model: &ModelVersionInfo,
        dataset: &TrainingDatasetInfo,
        bundle: &ActivePolicyBundle,
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
        let fold_regime = path_set
            .fold_artifacts
            .validation_regime()
            .map_err(|error| Self::invalid(error.to_string()))?;
        let profile = model
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(Self::invalid)?;
        let required_regime = match profile.spec.serving_authority {
            ServingAuthority::ReportOnlyWithLiveL2 => CpcvFoldValidationRegime::PredictiveUtility,
            ServingAuthority::ExecutionEligible => CpcvFoldValidationRegime::PortfolioEconomics,
        };
        let fit_seal = self
            .deps
            .history
            .validate_fit_seal(
                dataset.source_lineage.fit_seal_id,
                dataset.source_lineage.fit_seal_hash,
            )
            .await?;
        BootstrapCpcvEvidence {
            path_set: &path_set,
            model,
            dataset,
            fit_seal: &fit_seal,
            profile: &profile,
            policy_snapshot_id: bundle.decision_policy_snapshot_id,
            policy_snapshot_hash: bundle.snapshot_hash,
            required_regime,
        }
        .validate()?;
        let validation_evidence = match profile.spec.serving_authority {
            ServingAuthority::ReportOnlyWithLiveL2 => {
                if fold_regime != CpcvFoldValidationRegime::PredictiveUtility {
                    return Err(Self::invalid(
                        "ReportOnlyWithLiveL2 bootstrap requires predictive CPCV evidence",
                    ));
                }
                ModelBootstrapValidationEvidence::PredictiveCpcv {
                    path_set_id: path_set.path_set_id,
                    path_set_hash: path_set.path_set_hash,
                }
            }
            ServingAuthority::ExecutionEligible => {
                if fold_regime != CpcvFoldValidationRegime::PortfolioEconomics {
                    return Err(Self::invalid(
                        "ExecutionEligible bootstrap requires portfolio-economic CPCV evidence",
                    ));
                }
                let backtest = self
                    .deps
                    .backtests
                    .list_by_model_version(&model.model_version_id)
                    .await?
                    .into_iter()
                    .find(|report| {
                        report.decision_policy_snapshot_id == bundle.decision_policy_snapshot_id
                    })
                    .ok_or_else(|| {
                        Self::invalid(
                            "execution-eligible bootstrap has no portfolio backtest for the current policy snapshot",
                        )
                    })?;
                backtest.verify_hash().map_err(Self::invalid)?;
                ModelBootstrapValidationEvidence::PortfolioEconomics {
                    path_set_id: path_set.path_set_id,
                    path_set_hash: path_set.path_set_hash,
                    backtest_report_id: backtest.backtest_report_id,
                    backtest_report_hash: backtest.report_hash,
                }
            }
        };
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
                    validation_evidence,
                },
                evaluated_at,
            )
            .await?;
        Ok(BootstrapQualityEvidence { path_set, quality })
    }

    async fn scenario_route_evidence(
        &self,
        bundle: &ActivePolicyBundle,
        model: &ModelVersionInfo,
        path_set: &BacktestPathSetInfo,
    ) -> QuantResult<Vec<ScenarioRouteEvidence>> {
        let target_route = BuyModelRoute::try_from(model.category_scope)
            .map_err(|error| Self::invalid(error.to_string()))?;
        let mut route_models = vec![(target_route, model.clone(), path_set.clone())];
        for (route, binding) in &bundle.snapshot.model_routing.model.buy_routes {
            if *route == target_route {
                return Err(Self::invalid(
                    "bootstrap target already has an active Route binding",
                ));
            }
            let active_model = self
                .deps
                .route_evidence
                .load_model(binding.champion.model_version_id)
                .await?;
            let active_path_set = self
                .deps
                .path_sets
                .list_by_model_version(&active_model.model_version_id)
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    Self::invalid(format!(
                        "active bootstrap Route {route:?} has no immutable CPCV path set"
                    ))
                })?;
            route_models.push((*route, active_model, active_path_set));
        }
        route_models.sort_by_key(|(route, _, _)| *route);

        let mut evidence = Vec::with_capacity(route_models.len());
        for (route, route_model, route_path_set) in route_models {
            route_path_set
                .verify_hash()
                .map_err(|error| Self::invalid(error.to_string()))?;
            if route_path_set
                .fold_artifacts
                .validation_regime()
                .map_err(|error| Self::invalid(error.to_string()))?
                != CpcvFoldValidationRegime::PredictiveUtility
            {
                return Err(Self::invalid(format!(
                    "bootstrap reference scenario Route {route:?} lacks predictive CPCV evidence"
                )));
            }
            let contract = PromotedRouteContract::from_version(route, &route_model)?;
            if contract.serving_authority != ServingAuthority::ReportOnlyWithLiveL2 {
                return Err(Self::invalid(
                    "bootstrap reference scenario cannot mix execution-eligible and L2-free Routes",
                ));
            }
            let calibration = self
                .deps
                .calibrations
                .find_by_id(&contract.calibration_artifact_id)
                .await?
                .ok_or_else(|| {
                    Self::invalid(format!(
                        "bootstrap calibration artifact {} does not exist",
                        contract.calibration_artifact_id
                    ))
                })?;
            calibration.verify_model_score().map_err(Self::invalid)?;
            if calibration.content_hash != contract.calibration_contract_hash {
                return Err(Self::invalid(
                    "bootstrap calibration artifact differs from the serving contract",
                ));
            }
            evidence.push(ScenarioRouteEvidence {
                route,
                model: route_model,
                contract,
                path_set: route_path_set,
                calibration,
            });
        }
        Ok(evidence)
    }

    async fn fit_reference_scenario(
        &self,
        bundle: &ActivePolicyBundle,
        model: &ModelVersionInfo,
        path_set: &BacktestPathSetInfo,
        bound_at: DateTime<Utc>,
    ) -> QuantResult<PortfolioScenarioModelArtifactBinding> {
        let evidence = self
            .scenario_route_evidence(bundle, model, path_set)
            .await?;

        let represented = RepresentedRouteSet::from_routes(
            evidence.iter().map(|route_evidence| route_evidence.route),
        )
        .map_err(|error| Self::invalid(error.to_string()))?;
        let compatibility = RouteCompatibilityDigests::try_new(
            &represented,
            &evidence
                .iter()
                .map(|route_evidence| RouteContractHash {
                    route: route_evidence.route,
                    content_hash: route_evidence.contract.serving_contract_hash,
                })
                .collect::<Vec<_>>(),
            &evidence
                .iter()
                .map(|route_evidence| RouteContractHash {
                    route: route_evidence.route,
                    content_hash: route_evidence.contract.calibration_contract_hash,
                })
                .collect::<Vec<_>>(),
            &evidence
                .iter()
                .map(|route_evidence| RouteContractHash {
                    route: route_evidence.route,
                    content_hash: route_evidence.contract.recommendation_contract_hash,
                })
                .collect::<Vec<_>>(),
        )
        .map_err(|error| Self::invalid(error.to_string()))?;
        let horizon_secs = evidence
            .iter()
            .map(|route_evidence| {
                u64::try_from(route_evidence.contract.prediction_horizon_secs).map_err(|error| {
                    Self::invalid(format!(
                        "bootstrap prediction horizon does not fit u64: {error}"
                    ))
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let max_horizon_secs = horizon_secs.iter().copied().max().ok_or_else(|| {
            Self::invalid("bootstrap scenario has no represented prediction horizon")
        })?;
        let methodology =
            PortfolioScenarioMethodology::bootstrap_reference(&represented, max_horizon_secs)?;
        let route_inputs = evidence
            .iter()
            .zip(horizon_secs)
            .map(|(route_evidence, prediction_horizon_secs)| {
                let calibration_source = &route_evidence
                    .calibration
                    .verify_model_score()
                    .map_err(Self::invalid)?
                    .fit_contract
                    .model;
                Ok(PortfolioScenarioRouteFitInput {
                    route: route_evidence.route,
                    model_lineage: PortfolioScenarioRouteModelLineage {
                        evaluated_model_version_id: route_evidence.model.model_version_id,
                        evaluated_model_artifact_hash: route_evidence.model.artifact_hash,
                        evaluated_serving_contract_hash: route_evidence.model.serving_contract_hash,
                        calibration_source_model_version_id: calibration_source.model_version_id,
                        calibration_source_model_artifact_hash: calibration_source.artifact_hash,
                        calibration_source_serving_contract_hash: calibration_source
                            .serving_contract_hash,
                    },
                    calibration_artifact_id: route_evidence.calibration.artifact_id,
                    calibration_artifact_hash: route_evidence.calibration.content_hash,
                    recommendation_contract_hash: route_evidence
                        .contract
                        .recommendation_contract_hash,
                    prediction_horizon_secs,
                    path_set: &route_evidence.path_set,
                    calibration: &route_evidence.calibration,
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let fitted = PortfolioScenarioModelFitter::fit(&PortfolioScenarioModelFitInput {
            methodology: &methodology,
            represented_routes: &represented,
            compatibility,
            evidence_regime: PortfolioScenarioEvidenceRegime::FinalizedReferenceReturns,
            routes: route_inputs,
            bound_at,
        })?;
        let key = ArtifactKey::new(
            ArtifactNamespace::PortfolioScenarioModel,
            fitted
                .artifact
                .portfolio_scenario_model_artifact_id
                .to_string(),
            "json",
        )?;
        let bytes = serde_json::to_vec(&fitted.artifact).map_err(|error| {
            Self::invalid(format!("encode bootstrap reference scenario: {error}"))
        })?;
        if self.deps.artifacts.exists_by_key(&key).await? {
            if self.deps.artifacts.get_by_key(&key).await? != bytes {
                return Err(Self::invalid(
                    "bootstrap scenario content-addressed key contains different bytes",
                ));
            }
        } else {
            self.deps.artifacts.put(key.clone(), &bytes).await?;
        }
        let stored = self.deps.artifacts.get_by_key(&key).await?;
        if stored != bytes {
            return Err(Self::invalid(
                "bootstrap scenario failed exact artifact-store readback",
            ));
        }
        Ok(fitted.binding)
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
