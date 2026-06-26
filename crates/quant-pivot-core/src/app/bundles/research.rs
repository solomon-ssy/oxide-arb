//! Research plane bundle (Phase 3+): artifacts, selection, feature + factor pipelines.

use super::{DataBundle, GovernanceBundle, InfraBundle};
use crate::{
    governance::{ModelGovernanceDeps, ModelGovernanceService},
    pipeline::{
        feature_window_provider::FeatureWindowProvider,
        market_candidate_provider::MarketCandidateProvider,
    },
    service::{
        factor_pipeline::FactorPipelineService,
        feature_pipeline::FeaturePipelineService,
        model_runner::{DispatcherAlertSink, ModelRunner, ModelRunnerDeps},
        training_dataset::{
            TrainingDatasetBuildConfig, TrainingDatasetService, TrainingDatasetServiceDeps,
            default_labelers,
        },
    },
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::domain::ModelGovernancePort;
use quant_pivot_models::{
    config::DeployConfig,
    domain::RuntimeConfigPort,
    runtime_config::{DataQualityConfig, FactorsConfig, FeaturesConfig, TrainingConfig},
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestReportRepository, PgFactorRepository, PgFeatureRepository,
        PgMarketSelectionRepository, PgModelComparisonReportRepository,
        PgModelGovernanceAuditRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgRuntimeConfigVersionRepository, PgShadowComparisonRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        BacktestReportRepository, FactorRepository, FeatureRepository, MarketRepository,
        MarketSelectionRepository, ModelComparisonReportRepository, ModelGovernanceAuditRepository,
        ModelRegistryRepository, ModelRunRepository, QuantFactReadRepository,
        RuntimeConfigVersionRepository, ShadowComparisonRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::gates::{DefaultModelQualityGate, ModelQualityGate};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    model::{DefaultModelRuntimeFactoryBuilder, ModelRuntimeFactoryBuilder},
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
    pub feature_pipeline: Arc<FeaturePipelineService>,
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
    /// Append-only backtest-report ledger persistence (3.6).
    pub backtest_report_repo: Arc<dyn BacktestReportRepository>,
    /// Append-only pairwise comparison-report ledger persistence (3.6 §5.6).
    pub comparison_report_repo: Arc<dyn ModelComparisonReportRepository>,
    /// Append-only shadow-comparison ledger persistence (3.7).
    pub shadow_comparison_repo: Arc<dyn ShadowComparisonRepository>,
    /// Append-only model-governance audit trail persistence (3.7).
    pub governance_audit_repo: Arc<dyn ModelGovernanceAuditRepository>,
    /// Offline governance orchestration: publish / rollback / dataset promotion (3.7).
    pub model_governance: Arc<dyn ModelGovernancePort>,
    /// Schema-bound runtime factory builder (loads model artifacts) (3.4/3.6).
    pub model_runtime_factory_builder: Arc<dyn ModelRuntimeFactoryBuilder>,
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
        let feature_pipeline = Arc::new(FeaturePipelineService::new(
            FeatureWindowProvider::new(Arc::clone(&deps.infra.quant_fact_read)),
            Arc::clone(&feature_repo),
            Arc::clone(&deps.infra.feature_event_writer),
        ));
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
        let model_runtime_factory_builder: Arc<dyn ModelRuntimeFactoryBuilder> = Arc::new(
            DefaultModelRuntimeFactoryBuilder::new(Arc::clone(&artifact_store)),
        );
        let shadow_comparison_repo: Arc<dyn ShadowComparisonRepository> = Arc::new(
            PgShadowComparisonRepository::new(deps.infra.pg.connection().clone()),
        );
        let governance_audit_repo: Arc<dyn ModelGovernanceAuditRepository> = Arc::new(
            PgModelGovernanceAuditRepository::new(deps.infra.pg.connection().clone()),
        );
        let model_runner = Arc::new(ModelRunner::new(ModelRunnerDeps {
            model_run_repo: Arc::clone(&model_run_repo),
            model_registry_repo: Arc::clone(&model_registry_repo),
            shadow_comparison_repo: Arc::clone(&shadow_comparison_repo),
            factory_builder: Arc::clone(&model_runtime_factory_builder),
            factor_pipeline: Arc::clone(&factor_pipeline),
            signal_writer: Arc::clone(&deps.infra.signal_candidate_event_writer),
            alerts: Arc::new(DispatcherAlertSink::new(Arc::clone(
                &deps.governance.alerts,
            ))),
            weight_overlay: Arc::clone(&deps.governance.weight_overlay),
        }));

        let training_dataset_repo: Arc<dyn TrainingDatasetRepository> = Arc::new(
            PgTrainingDatasetRepository::new(deps.infra.pg.connection().clone()),
        );
        let backtest_report_repo: Arc<dyn BacktestReportRepository> = Arc::new(
            PgBacktestReportRepository::new(deps.infra.pg.connection().clone()),
        );
        let comparison_report_repo: Arc<dyn ModelComparisonReportRepository> = Arc::new(
            PgModelComparisonReportRepository::new(deps.infra.pg.connection().clone()),
        );

        let model_governance = Self::assemble_model_governance(
            deps,
            &model_registry_repo,
            &backtest_report_repo,
            &shadow_comparison_repo,
            &governance_audit_repo,
            &training_dataset_repo,
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
            backtest_report_repo,
            comparison_report_repo,
            shadow_comparison_repo,
            governance_audit_repo,
            model_governance,
            model_runtime_factory_builder,
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
    ) -> QuantResult<TrainingDatasetService> {
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

    /// Wire offline publish / rollback / dataset-promotion governance (3.7).
    fn assemble_model_governance(
        deps: &ResearchBundleDeps<'_>,
        model_registry_repo: &Arc<dyn ModelRegistryRepository>,
        backtest_report_repo: &Arc<dyn BacktestReportRepository>,
        shadow_comparison_repo: &Arc<dyn ShadowComparisonRepository>,
        governance_audit_repo: &Arc<dyn ModelGovernanceAuditRepository>,
        training_dataset_repo: &Arc<dyn TrainingDatasetRepository>,
    ) -> Arc<dyn ModelGovernancePort> {
        let gate: Arc<dyn ModelQualityGate> = Arc::new(DefaultModelQualityGate::new());
        let runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository> = Arc::new(
            PgRuntimeConfigVersionRepository::new(deps.infra.pg.connection().clone()),
        );
        let runtime_config_apply: Arc<dyn RuntimeConfigPort> =
            Arc::clone(&deps.governance.applicator) as Arc<dyn RuntimeConfigPort>;
        Arc::new(ModelGovernanceService::new(ModelGovernanceDeps {
            model_registry_repo: Arc::clone(model_registry_repo),
            backtest_report_repo: Arc::clone(backtest_report_repo),
            shadow_comparison_repo: Arc::clone(shadow_comparison_repo),
            governance_audit_repo: Arc::clone(governance_audit_repo),
            dataset_repo: Arc::clone(training_dataset_repo),
            gate,
            runtime_config: Arc::clone(&deps.governance.runtime_config),
            runtime_config_apply,
            runtime_config_repo,
        }))
    }
}
