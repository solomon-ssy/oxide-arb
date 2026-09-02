//! Fit the existing joint OOS panel with the production scenario fitter.

use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use quant_pivot_models::{
    domain::quant::{
        BacktestPathSetInfo, CalibrationArtifactInfo, PortfolioScenarioFitEvidence,
        PortfolioScenarioModelArtifact, PortfolioScenarioVisibility, RepresentedRouteSet,
        RouteCompatibilityDigests,
    },
    enums::runtime_config::ConfigResourceKind,
    runtime_config::BuyModelRoute,
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestPathSetRepository, PgCalibrationArtifactRepository, PgModelRegistryRepository,
        PgPolicyRepository,
    },
    traits::{
        BacktestPathSetRepository, CalibrationArtifactRepository, ModelRegistryRepository,
        PolicyRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    portfolio::{
        FittedPortfolioScenarioModel, PortfolioScenarioGenerator, PortfolioScenarioMethodology,
        PortfolioScenarioModelFitInput, PortfolioScenarioModelFitter,
        PortfolioScenarioRouteFitInput,
    },
};
use sea_orm::DatabaseConnection;
use serde_json::json;

use crate::{
    postgres::PostgresClock,
    support::{execution_pg_seed::SharedDemoInfra, policy_fixtures::activate_policy_bundle},
};

pub(super) struct ScenarioRefit;

struct RouteEvidence {
    path: BacktestPathSetInfo,
    calibration: CalibrationArtifactInfo,
    horizon: u64,
}

impl ScenarioRefit {
    pub(super) async fn apply(
        db: &DatabaseConnection,
        store: &Arc<dyn ArtifactStore>,
        mut infra: SharedDemoInfra,
    ) -> Result<SharedDemoInfra> {
        let policies = PgPolicyRepository::new(db.clone());
        let before = policies
            .load_current_bundle()
            .await?
            .context("mixed scenario policy")?;
        let represented = RepresentedRouteSet::from_routes([
            BuyModelRoute::Pooled,
            BuyModelRoute::Crypto,
            BuyModelRoute::Weather,
        ])?;
        let binding = before
            .snapshot
            .model_routing
            .model
            .portfolio_scenario_model_bindings
            .iter()
            .find(|binding| binding.ordered_routes == represented.routes)
            .context("complete three-Route scenario binding")?;
        let key = ArtifactKey::new(
            ArtifactNamespace::PortfolioScenarioModel,
            binding.portfolio_scenario_model_artifact_id.to_string(),
            "json",
        )?;
        let template: PortfolioScenarioModelArtifact =
            serde_json::from_slice(&store.get_by_key(&key).await?)?;
        PortfolioScenarioGenerator::verify_model(
            binding,
            &template,
            &represented,
            db.statement_time().await,
            PortfolioScenarioVisibility::PointInTime,
        )?;
        let fitted = Self::fit(db, &template, &represented).await?;
        ensure!(
            fitted.artifact.content_hash != template.content_hash,
            "scenario refit retained the placeholder ranks"
        );
        ensure!(
            PortfolioScenarioMethodology::from_promoted(&fitted.artifact)?
                == PortfolioScenarioMethodology::from_promoted(&template)?,
            "scenario fit changed governed haircuts, distributions or capital-time methodology"
        );
        let key = ArtifactKey::new(
            ArtifactNamespace::PortfolioScenarioModel,
            fitted
                .artifact
                .portfolio_scenario_model_artifact_id
                .to_string(),
            "json",
        )?;
        let bytes = serde_json::to_vec(&fitted.artifact)?;
        store.put(key.clone(), &bytes).await?;
        let readback = store.get_by_key(&key).await?;
        let decoded: PortfolioScenarioModelArtifact = serde_json::from_slice(&readback)?;
        ensure!(
            readback == bytes
                && decoded == fitted.artifact
                && decoded.recomputed_hash()? == fitted.artifact.content_hash,
            "refitted scenario did not preserve exact stored preimage"
        );
        PortfolioScenarioGenerator::verify_model(
            &fitted.binding,
            &decoded,
            &represented,
            db.statement_time().await,
            PortfolioScenarioVisibility::PointInTime,
        )?;
        println!(
            "source-side-scenario {}",
            json!({"previous_hash": template.content_hash, "fitted_hash": decoded.content_hash, "fit_start": decoded.fit_window_start, "fit_end": decoded.as_of, "routes": decoded.ordered_routes})
        );
        let generation = before
            .generation
            .checked_next()
            .context("scenario activation generation")?;
        let mut bindings = before
            .snapshot
            .model_routing
            .model
            .portfolio_scenario_model_bindings
            .clone();
        let previous = bindings
            .iter_mut()
            .find(|current| current.ordered_routes == represented.routes)
            .context("scenario binding disappeared")?;
        *previous = fitted.binding;
        let snapshot_id = activate_policy_bundle(
            &policies,
            ConfigResourceKind::ModelRouting,
            "mixed-scenario-refit",
            "fit existing joint OOS evidence without changing models or risk policy",
            move |snapshot| {
                snapshot
                    .model_routing
                    .model
                    .portfolio_scenario_model_bindings = bindings;
                for binding in snapshot.model_routing.model.buy_routes.values_mut() {
                    binding.champion.config_revision = generation;
                }
            },
        )
        .await;
        let after = policies
            .load_current_bundle()
            .await?
            .context("refitted scenario policy")?;
        ensure!(
            after.decision_policy_snapshot_id == snapshot_id && after.generation == generation,
            "scenario activation identity differs"
        );
        let mut expected = before.snapshot.clone();
        expected.model_routing = after.snapshot.model_routing.clone();
        expected.revisions.model_routing = after.snapshot.revisions.model_routing;
        ensure!(
            expected == after.snapshot,
            "scenario activation changed a non-routing policy"
        );
        for (route, binding) in &before.snapshot.model_routing.model.buy_routes {
            ensure!(
                after
                    .snapshot
                    .model_routing
                    .model
                    .route_binding(*route)?
                    .champion
                    .model_version_id
                    == binding.champion.model_version_id,
                "scenario refit changed a Route model identity"
            );
        }
        infra.decision_policy_snapshot_id = snapshot_id;
        Ok(infra)
    }

    async fn fit(
        db: &DatabaseConnection,
        template: &PortfolioScenarioModelArtifact,
        represented: &RepresentedRouteSet,
    ) -> Result<FittedPortfolioScenarioModel> {
        let paths = PgBacktestPathSetRepository::new(db.clone());
        let calibrations = PgCalibrationArtifactRepository::new(db.clone());
        let models = PgModelRegistryRepository::new(db.clone());
        let mut evidence = Vec::with_capacity(template.route_fit_lineage.len());
        for lineage in &template.route_fit_lineage {
            let PortfolioScenarioFitEvidence::CpcvPath {
                backtest_path_set_id,
                backtest_path_set_hash,
                ..
            } = lineage.fit_evidence
            else {
                bail!("mixed scenario fixture must supply an existing joint OOS path set");
            };
            let path = paths
                .find_by_id(&backtest_path_set_id)
                .await?
                .context("existing Route OOS panel")?;
            ensure!(
                path.path_set_hash == backtest_path_set_hash,
                "existing Route OOS panel hash differs"
            );
            let calibration = calibrations
                .find_by_id(&lineage.calibration_artifact_id)
                .await?
                .context("Route calibrator")?;
            let model = models
                .find_model_version(&lineage.model_lineage.evaluated_model_version_id)
                .await?
                .context("Route evaluated model")?;
            let horizon = u64::try_from(model.model_spec_prediction_horizon_secs)?;
            evidence.push(RouteEvidence {
                path,
                calibration,
                horizon,
            });
        }
        let methodology = PortfolioScenarioMethodology::from_promoted(template)?;
        let routes = template
            .route_fit_lineage
            .iter()
            .zip(&evidence)
            .map(|(lineage, evidence)| PortfolioScenarioRouteFitInput {
                route: lineage.route,
                model_lineage: lineage.model_lineage,
                calibration_artifact_id: lineage.calibration_artifact_id,
                calibration_artifact_hash: lineage.calibration_artifact_hash,
                recommendation_contract_hash: lineage.recommendation_contract_hash,
                prediction_horizon_secs: evidence.horizon,
                path_set: &evidence.path,
                calibration: &evidence.calibration,
            })
            .collect();
        Ok(PortfolioScenarioModelFitter::fit(
            &PortfolioScenarioModelFitInput {
                methodology: &methodology,
                represented_routes: represented,
                compatibility: RouteCompatibilityDigests {
                    serving_contract_digest: template.serving_contract_digest,
                    calibration_contract_digest: template.calibration_contract_digest,
                    recommendation_contract_digest: template.recommendation_contract_digest,
                },
                evidence_regime: template.evidence_regime,
                routes,
                bound_at: db.statement_time().await,
            },
        )?)
    }
}
