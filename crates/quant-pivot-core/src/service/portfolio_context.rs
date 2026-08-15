//! Atomic promoted Route/scenario readiness for report and offline replay.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    config::PortfolioSolverDeployConfig,
    domain::quant::{
        ModelVersionInfo, PortfolioScenarioModelArtifact, PortfolioScenarioVisibility,
        RepresentedRouteSet, RouteCompatibilityDigests, RouteContractHash,
    },
    hashing::CanonicalDigest,
    runtime_config::{
        BuyModelRoute, DecisionPolicySnapshot, PortfolioConfig,
        PortfolioScenarioModelArtifactBinding,
    },
    types::{
        CalibrationArtifactId, ContentHash, ModelVersionId, PortfolioScenarioModelArtifactId,
        ReportRouteRunId, ResearchFeatureContract, ResearchProfileArtifact,
        ResearchProfileArtifactId, ResearchProfileRef, ServingAuthority, TradePolicyArtifactId,
        model_lineage::ModelVersionDerivation, model_serving::ModelServingTradePolicyBinding,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    backtest::{BacktestPortfolioContext, BacktestScenarioContext},
    portfolio::{PortfolioScenarioGenerator, PortfolioScenarioMethodology},
};

use super::model_serving_preimage::VerifiedModelServingPreimage;

#[derive(serde::Serialize)]
struct BootstrapRecommendationContract<'a> {
    profile_ref: &'a ResearchProfileRef,
    feature_contract: ResearchFeatureContract,
    serving_authority: ServingAuthority,
}

/// Route-owned immutable lineage required by a promoted joint-scenario binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotedRouteContract {
    pub route: BuyModelRoute,
    pub model_version_id: ModelVersionId,
    pub serving_contract_hash: ContentHash,
    pub calibration_source_model_version_id: ModelVersionId,
    pub calibration_artifact_id: CalibrationArtifactId,
    pub calibration_contract_hash: ContentHash,
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    pub recommendation_contract_hash: ContentHash,
    pub serving_authority: ServingAuthority,
    pub feature_contract: ResearchFeatureContract,
    pub profile: ResearchProfileArtifact,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub research_profile_ref: ResearchProfileRef,
    pub prediction_horizon_secs: i64,
    pub feature_contract_digest: ContentHash,
    pub pit_lineage_digest: ContentHash,
}

impl PromotedRouteContract {
    /// Project one already verified serving row into scenario-compatibility lineage.
    pub fn from_version(route: BuyModelRoute, version: &ModelVersionInfo) -> QuantResult<Self> {
        if BuyModelRoute::try_from(version.category_scope)? != route {
            return Err(invalid(format!(
                "model {} category scope does not own represented Route {route:?}",
                version.model_version_id
            ))
            .into());
        }
        let serving = version.verified_serving_contract().map_err(|error| {
            invalid(format!("verify Route {route:?} serving contract: {error}"))
        })?;
        let bindings = serving.bindings();
        let calibration = bindings.model.calibration.as_ref().ok_or_else(|| {
            invalid(format!(
                "Route {route:?} serving contract has no probability calibration"
            ))
        })?;
        let profile = bindings
            .model
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(|error| invalid(format!("resolve Route {route:?} profile: {error}")))?;
        let trade_policy = bindings.trade_policy.as_ref();
        let recommendation_contract_hash = recommendation_contract_hash(&profile, trade_policy)?;
        let calibration_source_model_version_id = match version
            .verified_derivation()
            .map_err(|error| invalid(format!("verify Route {route:?} derivation: {error}")))?
        {
            ModelVersionDerivation::ReturnCalibration {
                parent_model_version_id,
                calibration_artifact_id,
            } if calibration_artifact_id == calibration.artifact_id => parent_model_version_id,
            ModelVersionDerivation::ReturnCalibration { .. } => {
                return Err(invalid(format!(
                    "Route {route:?} serving calibration differs from its derivation edge"
                ))
                .into());
            }
            ModelVersionDerivation::Training => {
                return Err(invalid(format!(
                    "Route {route:?} serving model is not a calibrated derived version"
                ))
                .into());
            }
        };
        Ok(Self {
            route,
            model_version_id: version.model_version_id,
            serving_contract_hash: serving.contract_hash(),
            calibration_source_model_version_id,
            calibration_artifact_id: calibration.artifact_id,
            calibration_contract_hash: calibration.content_hash,
            trade_policy_artifact_id: trade_policy.map(|binding| binding.artifact_id),
            recommendation_contract_hash,
            serving_authority: profile.spec.serving_authority,
            feature_contract: profile.spec.feature_contract,
            profile,
            research_profile_artifact_id: bindings.model.profile_ref.artifact_id(),
            research_profile_ref: bindings.model.profile_ref.clone(),
            prediction_horizon_secs: version.model_spec_prediction_horizon_secs,
            feature_contract_digest: bindings.transform.input_contract_hash,
            pit_lineage_digest: bindings.dataset.manifest.source_fingerprint,
        })
    }
}

fn recommendation_contract_hash(
    profile: &ResearchProfileArtifact,
    trade_policy: Option<&ModelServingTradePolicyBinding>,
) -> QuantResult<ContentHash> {
    match profile.spec.serving_authority {
        ServingAuthority::ExecutionEligible => trade_policy
            .map(|binding| binding.content_hash)
            .ok_or_else(|| {
                invalid("execution-eligible Route serving contract has no Trade Policy".to_owned())
                    .into()
            }),
        ServingAuthority::ReportOnlyWithLiveL2 => {
            if trade_policy.is_some() {
                return Err(invalid(
                    "bootstrap Route must not bind an executable Trade Policy".to_owned(),
                )
                .into());
            }
            Ok(CanonicalDigest::content_hash_typed(
                "quant-pivot/bootstrap-recommendation-contract",
                1,
                &BootstrapRecommendationContract {
                    profile_ref: &profile.profile_ref,
                    feature_contract: profile.spec.feature_contract,
                    serving_authority: profile.spec.serving_authority,
                },
            )?)
        }
    }
}

/// Complete immutable portfolio context shared by all represented Routes.
#[derive(Debug, Clone)]
pub struct PromotedPortfolioContext {
    pub scenario_model_binding: PortfolioScenarioModelArtifactBinding,
    pub scenario_model: PortfolioScenarioModelArtifact,
    pub policy: PortfolioConfig,
    pub solver: PortfolioSolverDeployConfig,
}

/// Static portfolio policy plus the exact promoted scenario estimator used by
/// a chronological backtest or paired evaluation.
#[derive(Debug, Clone)]
pub struct PreparedBacktestPortfolio {
    pub portfolio: BacktestPortfolioContext,
    pub scenario: BacktestScenarioContext,
    pub scenario_visibility: PortfolioScenarioVisibility,
}

/// Static portfolio policy and data-free scenario methodology used to build
/// independent nested estimators inside CPCV.
#[derive(Debug, Clone)]
pub struct PreparedCpcvPortfolio {
    pub portfolio: BacktestPortfolioContext,
    pub scenario_methodology: PortfolioScenarioMethodology,
}

/// Loads and verifies one complete promoted scenario graph from a frozen policy.
pub struct PromotedPortfolioContextLoader {
    artifact_store: Arc<dyn ArtifactStore>,
    solver: PortfolioSolverDeployConfig,
    policy: DecisionPolicySnapshot,
}

/// The two independent clocks needed to validate a scenario graph.
///
/// `scenario_data_cutoff` protects every historical decision from future
/// scenario evidence. `binding_governance_cutoff` proves that the immutable
/// binding was frozen before the live decision or offline evaluation began.
/// They are intentionally distinct for historical replay: a policy may be
/// governed today while its scenario evidence must predate the replay window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScenarioVisibilityBoundary {
    PointInTime {
        decision_at: DateTime<Utc>,
    },
    HistoricalReplay {
        decision_at: DateTime<Utc>,
        governance_frozen_at: DateTime<Utc>,
    },
}

impl ScenarioVisibilityBoundary {
    const fn live(decision_at: DateTime<Utc>) -> Self {
        Self::PointInTime { decision_at }
    }

    const fn offline(
        replay_data_cutoff: DateTime<Utc>,
        evaluation_frozen_at: DateTime<Utc>,
    ) -> Self {
        Self::HistoricalReplay {
            decision_at: replay_data_cutoff,
            governance_frozen_at: evaluation_frozen_at,
        }
    }

    const fn decision_at(self) -> DateTime<Utc> {
        match self {
            Self::PointInTime { decision_at } | Self::HistoricalReplay { decision_at, .. } => {
                decision_at
            }
        }
    }

    const fn domain_visibility(self) -> PortfolioScenarioVisibility {
        match self {
            Self::PointInTime { .. } => PortfolioScenarioVisibility::PointInTime,
            Self::HistoricalReplay {
                governance_frozen_at,
                ..
            } => PortfolioScenarioVisibility::HistoricalReplay {
                governance_frozen_at,
            },
        }
    }
}

impl PromotedPortfolioContextLoader {
    #[must_use]
    pub fn new(
        artifact_store: Arc<dyn ArtifactStore>,
        solver: PortfolioSolverDeployConfig,
        policy: DecisionPolicySnapshot,
    ) -> Self {
        Self {
            artifact_store,
            solver,
            policy,
        }
    }

    /// Load the active Route's PIT-visible scenario model as a fixed benchmark
    /// risk policy for an unpromoted challenger replay.
    ///
    /// The candidate Route is derived from its immutable category scope, never
    /// from active serving authority. The active scenario artifact is verified
    /// against its own frozen binding, but is deliberately not relabelled as
    /// candidate-compatible: CPCV and paired comparison use one ex-ante risk
    /// policy for every contender, while final promotion separately refits and
    /// atomically binds a candidate-specific scenario model.
    pub async fn load_evaluation_single(
        &self,
        source: &VerifiedModelServingPreimage,
        replay_data_cutoff: DateTime<Utc>,
        evaluation_frozen_at: DateTime<Utc>,
        top_n: u32,
    ) -> QuantResult<PreparedBacktestPortfolio> {
        self.validate_top_n(top_n)?;
        let model_version_id = source.artifact().header().model_version_id();
        let route = BuyModelRoute::try_from(source.artifact().category_scope())?;
        self.policy
            .model_routing
            .model
            .route_binding(route)
            .map_err(|error| {
                invalid(format!("evaluation Route has no active benchmark: {error}"))
            })?;
        let represented_routes = RepresentedRouteSet::from_routes([route])?;
        let scenario_model_binding = self
            .policy
            .model_routing
            .model
            .scenario_model_binding(&represented_routes.routes, &represented_routes.digest)?
            .clone();
        let scenario_model = self
            .load_bound_scenario(
                &represented_routes,
                &scenario_model_binding,
                ScenarioVisibilityBoundary::offline(replay_data_cutoff, evaluation_frozen_at),
            )
            .await?;
        let route_run_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/evaluation-route-run",
            1,
            &(
                model_version_id,
                replay_data_cutoff,
                evaluation_frozen_at,
                route,
                scenario_model.content_hash,
            ),
        )?;
        let scenario = BacktestScenarioContext::try_new(
            scenario_model_binding,
            scenario_model,
            represented_routes.clone(),
        )?;
        Ok(PreparedBacktestPortfolio {
            portfolio: BacktestPortfolioContext {
                report_route_run_id: ReportRouteRunId::from_content_hash(&route_run_hash),
                route,
                represented_routes,
                policy: self.policy.execution_risk.portfolio.clone(),
                solver: self.solver,
                top_n,
            },
            scenario,
            scenario_visibility: PortfolioScenarioVisibility::HistoricalReplay {
                governance_frozen_at: evaluation_frozen_at,
            },
        })
    }

    /// Load only the governed, data-free scenario methodology for CPCV.
    /// Learned ranks, calibration shifts, and fit clocks from the promoted
    /// model are erased by [`PortfolioScenarioMethodology`] before return.
    pub async fn load_cpcv_single(
        &self,
        source: &VerifiedModelServingPreimage,
        evaluation_frozen_at: DateTime<Utc>,
        top_n: u32,
    ) -> QuantResult<PreparedCpcvPortfolio> {
        self.validate_top_n(top_n)?;
        let model_version_id = source.artifact().header().model_version_id();
        let route = BuyModelRoute::try_from(source.artifact().category_scope())?;
        self.policy
            .model_routing
            .model
            .route_binding(route)
            .map_err(|error| invalid(format!("CPCV Route has no governed methodology: {error}")))?;
        let represented_routes = RepresentedRouteSet::from_routes([route])?;
        let binding = self
            .policy
            .model_routing
            .model
            .scenario_model_binding(&represented_routes.routes, &represented_routes.digest)?
            .clone();
        let promoted = self
            .load_bound_scenario(
                &represented_routes,
                &binding,
                ScenarioVisibilityBoundary::live(evaluation_frozen_at),
            )
            .await?;
        let scenario_methodology = PortfolioScenarioMethodology::from_promoted(&promoted)?;
        let route_run_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-route-run",
            1,
            &(
                model_version_id,
                evaluation_frozen_at,
                route,
                promoted.content_hash,
            ),
        )?;
        Ok(PreparedCpcvPortfolio {
            portfolio: BacktestPortfolioContext {
                report_route_run_id: ReportRouteRunId::from_content_hash(&route_run_hash),
                route,
                represented_routes,
                policy: self.policy.execution_risk.portfolio.clone(),
                solver: self.solver,
                top_n,
            },
            scenario_methodology,
        })
    }

    /// Load and verify the exact promoted joint-scenario graph for a complete Route set.
    pub async fn load_routes(
        &self,
        represented_routes: RepresentedRouteSet,
        route_contracts: Vec<PromotedRouteContract>,
        visible_at: DateTime<Utc>,
        top_n: u32,
    ) -> QuantResult<PromotedPortfolioContext> {
        self.validate_top_n(top_n)?;
        if route_contracts.len() != represented_routes.routes.len()
            || route_contracts
                .iter()
                .zip(&represented_routes.routes)
                .any(|(contract, route)| contract.route != *route)
        {
            return Err(invalid(
                "promoted Route contracts differ from the canonical represented Route set"
                    .to_owned(),
            )
            .into());
        }
        let compatibility = RouteCompatibilityDigests::try_new(
            &represented_routes,
            &route_contracts
                .iter()
                .map(|contract| RouteContractHash {
                    route: contract.route,
                    content_hash: contract.serving_contract_hash,
                })
                .collect::<Vec<_>>(),
            &route_contracts
                .iter()
                .map(|contract| RouteContractHash {
                    route: contract.route,
                    content_hash: contract.calibration_contract_hash,
                })
                .collect::<Vec<_>>(),
            &route_contracts
                .iter()
                .map(|contract| RouteContractHash {
                    route: contract.route,
                    content_hash: contract.recommendation_contract_hash,
                })
                .collect::<Vec<_>>(),
        )
        .map_err(|error| invalid(format!("Route compatibility digest failed: {error}")))?;
        let scenario_model_binding = self
            .policy
            .model_routing
            .model
            .scenario_model_binding(&represented_routes.routes, &represented_routes.digest)?
            .clone();
        if scenario_model_binding.serving_contract_digest != compatibility.serving_contract_digest
            || scenario_model_binding.calibration_contract_digest
                != compatibility.calibration_contract_digest
            || scenario_model_binding.recommendation_contract_digest
                != compatibility.recommendation_contract_digest
        {
            return Err(invalid(
                "scenario binding differs from the exact Route serving, calibration, or Trade Policy contracts"
                    .to_owned(),
            )
            .into());
        }

        let scenario_model = self
            .load_bound_scenario(
                &represented_routes,
                &scenario_model_binding,
                ScenarioVisibilityBoundary::live(visible_at),
            )
            .await?;
        Ok(PromotedPortfolioContext {
            scenario_model_binding,
            scenario_model,
            policy: self.policy.execution_risk.portfolio.clone(),
            solver: self.solver,
        })
    }

    fn validate_top_n(&self, top_n: u32) -> QuantResult<()> {
        if top_n == 0 || top_n > self.policy.recommendation.reports.max_top_n {
            return Err(invalid(format!(
                "portfolio TopN {top_n} is outside the frozen policy range 1..={}",
                self.policy.recommendation.reports.max_top_n
            ))
            .into());
        }
        Ok(())
    }

    async fn load_bound_scenario(
        &self,
        represented_routes: &RepresentedRouteSet,
        scenario_model_binding: &PortfolioScenarioModelArtifactBinding,
        visibility: ScenarioVisibilityBoundary,
    ) -> QuantResult<PortfolioScenarioModelArtifact> {
        let key = ArtifactKey::new(
            ArtifactNamespace::PortfolioScenarioModel,
            scenario_model_binding
                .portfolio_scenario_model_artifact_id
                .to_string(),
            "json",
        )?;
        let bytes = self.artifact_store.get_by_key(&key).await?;
        let scenario_model = serde_json::from_slice::<PortfolioScenarioModelArtifact>(&bytes)
            .map_err(|error| invalid(format!("decode promoted scenario model: {error}")))?;
        Self::verify_scenario_contract(
            represented_routes,
            scenario_model_binding,
            &scenario_model,
            visibility,
        )?;
        Ok(scenario_model)
    }

    fn verify_scenario_contract(
        represented_routes: &RepresentedRouteSet,
        binding: &PortfolioScenarioModelArtifactBinding,
        model: &PortfolioScenarioModelArtifact,
        visibility: ScenarioVisibilityBoundary,
    ) -> QuantResult<()> {
        let recomputed_hash = model.recomputed_hash()?;
        let canonical_id = PortfolioScenarioModelArtifactId::from_content_hash(&recomputed_hash);
        if model.portfolio_scenario_model_artifact_id
            != binding.portfolio_scenario_model_artifact_id
        {
            return Err(invalid(format!(
                "scenario artifact id {} differs from bound id {}",
                model.portfolio_scenario_model_artifact_id,
                binding.portfolio_scenario_model_artifact_id
            ))
            .into());
        }
        if model.portfolio_scenario_model_artifact_id != canonical_id {
            return Err(invalid(format!(
                "scenario artifact id {} is not the canonical id {canonical_id} for its content",
                model.portfolio_scenario_model_artifact_id
            ))
            .into());
        }
        if model.content_hash != binding.model_content_hash {
            return Err(invalid(format!(
                "scenario content hash {} differs from bound hash {}",
                model.content_hash, binding.model_content_hash
            ))
            .into());
        }
        if recomputed_hash != model.content_hash {
            return Err(invalid(format!(
                "scenario content hash {} differs from recomputed hash {recomputed_hash}",
                model.content_hash
            ))
            .into());
        }
        if model.schema_version != binding.scenario_model_schema_version {
            return Err(invalid(format!(
                "scenario schema version {} differs from bound version {}",
                model.schema_version, binding.scenario_model_schema_version
            ))
            .into());
        }
        if model.route_set_digest != represented_routes.digest
            || binding.route_set_digest != represented_routes.digest
        {
            return Err(invalid(format!(
                "scenario Route-set digest mismatch: represented={}, binding={}, artifact={}",
                represented_routes.digest, binding.route_set_digest, model.route_set_digest
            ))
            .into());
        }
        if model.ordered_routes != represented_routes.routes
            || binding.ordered_routes != represented_routes.routes
        {
            return Err(invalid(format!(
                "scenario ordered Routes mismatch: represented={:?}, binding={:?}, artifact={:?}",
                represented_routes.routes, binding.ordered_routes, model.ordered_routes
            ))
            .into());
        }
        if model.serving_contract_digest != binding.serving_contract_digest {
            return Err(invalid(format!(
                "scenario serving-contract digest {} differs from bound digest {}",
                model.serving_contract_digest, binding.serving_contract_digest
            ))
            .into());
        }
        if model.calibration_contract_digest != binding.calibration_contract_digest {
            return Err(invalid(format!(
                "scenario calibration-contract digest {} differs from bound digest {}",
                model.calibration_contract_digest, binding.calibration_contract_digest
            ))
            .into());
        }
        if model.recommendation_contract_digest != binding.recommendation_contract_digest {
            return Err(invalid(format!(
                "scenario Trade-Policy digest {} differs from bound digest {}",
                model.recommendation_contract_digest, binding.recommendation_contract_digest
            ))
            .into());
        }
        if model.capital_time_bucket_contract_digest != binding.capital_time_bucket_contract_digest
        {
            return Err(invalid(format!(
                "scenario capital-time bucket digest {} differs from bound digest {}",
                model.capital_time_bucket_contract_digest,
                binding.capital_time_bucket_contract_digest
            ))
            .into());
        }
        PortfolioScenarioGenerator::verify_model(
            binding,
            model,
            represented_routes,
            visibility.decision_at(),
            visibility.domain_visibility(),
        )
        .map_err(|error| invalid(format!("scenario semantic validation failed: {error}")))?;
        Ok(())
    }

    pub const fn policy(&self) -> &DecisionPolicySnapshot {
        &self.policy
    }
}

const fn invalid(detail: String) -> ResearchError {
    ResearchError::InvalidModelArtifact { detail }
}
