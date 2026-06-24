//! Research plane bundle (Phase 3+): artifacts, selection, feature + factor pipelines.

use super::{DataBundle, GovernanceBundle, InfraBundle};
use crate::{
    pipeline::{
        feature_window_provider::FeatureWindowProvider,
        market_candidate_provider::MarketCandidateProvider,
    },
    service::{
        factor_pipeline::FactorPipelineService,
        feature_pipeline::FeaturePipelineService,
        model_runner::{DispatcherAlertSink, ModelRunner},
        training_dataset::{
            TrainingDatasetBuildConfig, TrainingDatasetService, TrainingDatasetServiceDeps,
            default_labelers,
        },
    },
};
use quant_pivot_models::{
    config::DeployConfig,
    runtime_config::{DataQualityConfig, FactorsConfig, FeaturesConfig, TrainingConfig},
};
use quant_pivot_repository::{
    postgres::{
        PgFactorRepository, PgFeatureRepository, PgMarketSelectionRepository,
        PgModelRegistryRepository, PgModelRunRepository, PgTrainingDatasetRepository,
    },
    traits::{
        FactorRepository, FeatureRepository, MarketRepository, MarketSelectionRepository,
        ModelRegistryRepository, ModelRunRepository, QuantFactReadRepository,
        TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    model::DefaultModelRuntimeFactoryBuilder,
    selection::{ConfiguredMarketSelector, MarketSelector},
};
use std::sync::Arc;

/// Dependencies required to assemble the research plane after infra + data.
pub struct ResearchBundleDeps<'a> {
    /// Deploy-time configuration (artifact root, etc.).
    pub deploy: &'a DeployConfig,
    /// Persistence and analytics handles.
    pub infra: &'a InfraBundle,
    /// Live data plane (books, registry, PIT source for online feature builds).
    pub data: &'a DataBundle,
    /// Governance plane (operator alert dispatcher for inference degradation).
    pub governance: &'a GovernanceBundle,
}

/// Research plane: selection, feature/factor pipelines, and artifact store (Phase 3+).
pub struct ResearchBundle {
    /// Local (or future object-store) backend for dataset / model artifact bytes.
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Pure, config-driven market selector (3.1).
    pub market_selector: Arc<dyn MarketSelector>,
    /// Persistence port for selection snapshots and their members (3.1).
    pub market_selection_repo: Arc<dyn MarketSelectionRepository>,
    /// Core-side projector freezing market facts into selector inputs (3.1).
    pub candidate_provider: Arc<MarketCandidateProvider>,
    /// Postgres persistence for feature vectors (3.2).
    pub feature_repo: Arc<dyn FeatureRepository>,
    /// Online feature build loop: resolve → build → persist → emit (3.2).
    pub feature_pipeline: FeaturePipelineService,
    /// Postgres persistence for factor definitions + values (3.3).
    pub factor_repo: Arc<dyn FactorRepository>,
    /// Online factor build loop: compute → partition → persist → emit (3.3).
    pub factor_pipeline: Arc<FactorPipelineService>,
    /// Model-run persistence (create / finalize live + shadow runs) (3.4).
    pub model_run_repo: Arc<dyn ModelRunRepository>,
    /// Model registry persistence (resolve active / shadow versions) (3.4).
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    /// Online inference orchestrator: selection/features/factors → candidates (3.4).
    pub model_runner: Arc<ModelRunner>,
    /// Frozen training-dataset ledger persistence (3.5).
    pub training_dataset_repo: Arc<dyn TrainingDatasetRepository>,
    /// Historical fact read port (PIT book / microstructure / settlement) (3.5).
    pub quant_fact_read: Arc<dyn QuantFactReadRepository>,
    /// Market catalog read port for PIT metadata + sampling candidates (3.5).
    pub market_repo: Arc<dyn MarketRepository>,
}

impl ResearchBundle {
    /// Build the research bundle from deploy config plus wired infra/data handles.
    ///
    /// No report scheduler or trigger is wired here — periodic report generation
    /// is a Phase 4 concern. The feature and factor pipelines are ready for
    /// on-demand invocation with a frozen runtime-config snapshot per round.
    #[must_use]
    pub fn assemble(deps: &ResearchBundleDeps<'_>) -> Self {
        let artifact_store: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(
            deps.deploy.research.artifact_root.clone(),
        ));
        let market_selector: Arc<dyn MarketSelector> = Arc::new(ConfiguredMarketSelector::new());
        let market_selection_repo: Arc<dyn MarketSelectionRepository> = Arc::new(
            PgMarketSelectionRepository::new(deps.infra.pg.connection().clone()),
        );
        let candidate_provider = Arc::new(MarketCandidateProvider::new(
            Arc::clone(&deps.data.market_registry),
            Arc::clone(&deps.data.book_store),
            Arc::clone(&deps.infra.fact_lag_tracker),
        ));
        let feature_repo: Arc<dyn FeatureRepository> =
            Arc::new(PgFeatureRepository::new(deps.infra.pg.connection().clone()));
        let feature_pipeline = FeaturePipelineService::new(
            FeatureWindowProvider::new(Arc::clone(&deps.infra.quant_fact_read)),
            Arc::clone(&feature_repo),
            Arc::clone(&deps.infra.feature_event_writer),
        );
        let factor_repo: Arc<dyn FactorRepository> =
            Arc::new(PgFactorRepository::new(deps.infra.pg.connection().clone()));
        let factor_pipeline = Arc::new(FactorPipelineService::new(
            Arc::clone(&factor_repo),
            Arc::clone(&deps.infra.factor_event_writer),
        ));

        let model_run_repo: Arc<dyn ModelRunRepository> = Arc::new(PgModelRunRepository::new(
            deps.infra.pg.connection().clone(),
        ));
        let model_registry_repo: Arc<dyn ModelRegistryRepository> = Arc::new(
            PgModelRegistryRepository::new(deps.infra.pg.connection().clone()),
        );
        let model_runner = Arc::new(ModelRunner::new(
            Arc::clone(&model_run_repo),
            Arc::clone(&model_registry_repo),
            Arc::new(DefaultModelRuntimeFactoryBuilder::new(Arc::clone(
                &artifact_store,
            ))),
            Arc::clone(&factor_pipeline),
            Arc::clone(&deps.infra.signal_candidate_event_writer),
            Arc::new(DispatcherAlertSink::new(Arc::clone(
                &deps.governance.alerts,
            ))),
        ));

        let training_dataset_repo: Arc<dyn TrainingDatasetRepository> = Arc::new(
            PgTrainingDatasetRepository::new(deps.infra.pg.connection().clone()),
        );

        Self {
            artifact_store,
            market_selector,
            market_selection_repo,
            candidate_provider,
            feature_repo,
            feature_pipeline,
            factor_repo,
            factor_pipeline,
            model_run_repo,
            model_registry_repo,
            model_runner,
            training_dataset_repo,
            quant_fact_read: Arc::clone(&deps.infra.quant_fact_read),
            market_repo: Arc::clone(&deps.data.market_repo),
        }
    }

    /// Construct an offline training-dataset service bound to a frozen
    /// runtime-config snapshot. The service plans a deterministic sample grid,
    /// batch-prefetches historical facts, materializes PIT features + forward
    /// labels, and writes a content-hashed Parquet artifact + ledger row.
    pub fn training_dataset_service(
        &self,
        features: FeaturesConfig,
        factors: FactorsConfig,
        data_quality: DataQualityConfig,
        training: TrainingConfig,
    ) -> quant_pivot_error::QuantResult<TrainingDatasetService> {
        TrainingDatasetService::new(
            TrainingDatasetServiceDeps {
                fact_read: Arc::clone(&self.quant_fact_read),
                market_repo: Arc::clone(&self.market_repo),
                artifact_store: Arc::clone(&self.artifact_store),
                dataset_repo: Arc::clone(&self.training_dataset_repo),
            },
            TrainingDatasetBuildConfig {
                features,
                factors,
                data_quality,
                training,
                labelers: default_labelers(),
            },
        )
    }
}
