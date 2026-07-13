//! Research plane bundle (Phase 3+): artifacts, selection, feature + factor pipelines.

use super::{DataBundle, GovernanceBundle, InfraBundle};
use crate::{
    governance::{
        CoreCalibrationArtifactLoader, FactorGovernanceDeps, FactorGovernanceService,
        ModelGovernanceDeps, ModelGovernanceService, ModelSpecDeps, ModelSpecService,
    },
    prefetch::{feature_window::FeatureWindowProvider, market_candidates::MarketCandidateProvider},
    service::{
        bias_table_fit::BiasTableFitService,
        factor_pipeline::FactorPipelineService,
        feature_pipeline::{FeaturePipelineDeps, FeaturePipelineService},
        frozen_model_parity::{FrozenModelParityDeps, FrozenModelParityService},
        model_runner::{DispatcherAlertSink, ModelRunner, ModelRunnerDeps},
        training_dataset::{
            TrainingDatasetService, TrainingDatasetServiceDeps, TrainingDatasetServiceWire,
        },
    },
};
use quant_pivot_api::fees::FeeCalculator;
use quant_pivot_error::QuantResult;
use quant_pivot_models::domain::{
    CalibrationArtifactFitPort, FactorGovernancePort, ModelGovernancePort, ModelSpecPort,
};
use quant_pivot_models::{
    clickhouse::QuantFeatureParityEventRow, config::DeployConfig, domain::RuntimeConfigPort,
};
use quant_pivot_repository::{
    clickhouse::ChFactWriter,
    traits::{
        AttributionRepository, BacktestPathSetRepository, BacktestReportRepository,
        BasisAlertRepository, CalibrationArtifactRepository, CatalogVersionRepository, FactWriter,
        FactorRepository, FeatureParityRepository, FeatureRepository, MarketLinkageRepository,
        MarketRepository, MarketSelectionRepository, ModelComparisonReportRepository,
        ModelGovernanceAuditRepository, ModelRegistryRepository, ModelRunRepository,
        PositionRepository, QuantFactReadRepository, RecommendationRepository,
        RuntimeConfigVersionRepository, ShadowComparisonRepository, TradeTapeBlockCursorRepository,
        TrainingDatasetRepository,
    },
};
use quant_pivot_research::gates::{DefaultModelQualityGate, ModelQualityGate};
use quant_pivot_research::model::CalibrationArtifactLoader;
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

fn frozen_parity_evidence_writer(
    infra: &InfraBundle,
) -> Arc<dyn FactWriter<QuantFeatureParityEventRow>> {
    Arc::new(ChFactWriter::new(
        Arc::clone(&infra.ch),
        Arc::clone(&infra.ch_write_manager),
        "quant_feature_parity_event",
    ))
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
    /// Full frozen dataset/model verification used by training and publish.
    pub frozen_model_parity: Arc<FrozenModelParityService>,
    /// Online inference orchestrator: selection/features/factors → candidates (3.4).
    pub model_runner: Arc<ModelRunner>,
    /// Frozen training-dataset ledger persistence (3.5).
    pub training_dataset_repo: Arc<dyn TrainingDatasetRepository>,
    /// Final attribution rows available for supervised live samples (5.7).
    pub attribution_repo: Arc<dyn AttributionRepository>,
    /// Recommendation rows carrying frozen evidence refs for live attribution samples.
    pub recommendation_repo: Arc<dyn RecommendationRepository>,
    /// Append-only backtest-report ledger persistence (3.6).
    pub backtest_report_repo: Arc<dyn BacktestReportRepository>,
    /// Append-only CPCV + trial-grid path-set ledger persistence (11.5).
    pub backtest_path_set_repo: Arc<dyn BacktestPathSetRepository>,
    /// Append-only pairwise comparison-report ledger persistence (3.6 §5.6).
    pub comparison_report_repo: Arc<dyn ModelComparisonReportRepository>,
    /// Append-only shadow-comparison ledger persistence (3.7).
    pub shadow_comparison_repo: Arc<dyn ShadowComparisonRepository>,
    /// Append-only model-governance audit trail persistence (3.7).
    pub governance_audit_repo: Arc<dyn ModelGovernanceAuditRepository>,
    /// Offline governance orchestration: publish / rollback / dataset promotion (3.7).
    pub model_governance: Arc<dyn ModelGovernancePort>,
    /// Factor-definition publish / retire orchestration (05.7).
    pub factor_governance: Arc<dyn FactorGovernancePort>,
    /// Model-spec authoring (the offline research lifecycle root write path).
    pub model_spec: Arc<dyn ModelSpecPort>,
    /// Schema-bound runtime factory builder (loads model artifacts) (3.4/3.6).
    pub model_runtime_factory_builder: Arc<dyn ModelRuntimeFactoryBuilder>,
    /// Resolves `model_score` calibration artifacts for runtime + Kelly safety (11.3).
    pub calibration_loader: Arc<dyn CalibrationArtifactLoader>,
    /// Historical fact read port (PIT book / microstructure / settlement) (3.5).
    pub quant_fact_read: Arc<dyn QuantFactReadRepository>,
    /// Append-only catalog ledger for every offline PIT metadata read.
    pub catalog_version_repo: Arc<dyn CatalogVersionRepository>,
    /// Market catalog read port for PIT metadata + sampling candidates (3.5).
    pub market_repo: Arc<dyn MarketRepository>,
    /// Frozen market → external-subject linkage ledger (11.2.2).
    pub market_linkage_repo: Arc<dyn MarketLinkageRepository>,
    /// Position ledger for `ExitDecision` lot-timeline training (06.1).
    pub position_repo: Arc<dyn PositionRepository>,
    /// Venue fee calculator for governed exit-fee-aware Sell labels (06.1).
    pub fee_calculator: Arc<FeeCalculator>,
    /// Unified calibration-artifact ledger port: favorite-longshot bias-table
    /// fitter + generic catalog read/activate for every artifact kind (11.2.1, 11.3).
    pub calibration_artifact_fit: Arc<dyn CalibrationArtifactFitPort>,
}

struct OfflineResearchRepos {
    training_dataset: Arc<dyn TrainingDatasetRepository>,
    attribution: Arc<dyn AttributionRepository>,
    recommendation: Arc<dyn RecommendationRepository>,
    backtest_report: Arc<dyn BacktestReportRepository>,
    backtest_path_set: Arc<dyn BacktestPathSetRepository>,
    comparison_report: Arc<dyn ModelComparisonReportRepository>,
    governance_audit: Arc<dyn ModelGovernanceAuditRepository>,
}

struct ResearchPipelines {
    candidate_provider: Arc<MarketCandidateProvider>,
    feature_repo: Arc<dyn FeatureRepository>,
    feature_pipeline: Arc<FeaturePipelineService>,
    factor_repo: Arc<dyn FactorRepository>,
    factor_pipeline: Arc<FactorPipelineService>,
}

fn assemble_research_pipelines(
    deps: &ResearchBundleDeps<'_>,
    market_linkage_repo: &Arc<dyn MarketLinkageRepository>,
) -> ResearchPipelines {
    let candidate_provider = Arc::new(MarketCandidateProvider::new(
        Arc::clone(&deps.data.pit_source),
        Arc::clone(market_linkage_repo),
        Arc::clone(&deps.infra.quant_fact_read),
    ));
    let feature_repo: Arc<dyn FeatureRepository> =
        Arc::clone(&deps.infra.repos.feature) as Arc<dyn FeatureRepository>;
    let feature_pipeline = Arc::new(FeaturePipelineService::new(FeaturePipelineDeps {
        window_provider: FeatureWindowProvider::new(Arc::clone(&deps.infra.quant_fact_read)),
        feature_repo: Arc::clone(&feature_repo),
        event_writer: Arc::clone(&deps.infra.feature_event_writer),
        market_registry: Arc::clone(&deps.data.market_registry),
        block_cursor_repo: Arc::clone(&deps.infra.repos.trade_tape_block_cursor)
            as Arc<dyn TradeTapeBlockCursorRepository>,
        linkage_repo: Arc::clone(market_linkage_repo),
        basis_alert_repo: Arc::clone(&deps.infra.repos.basis_alert)
            as Arc<dyn BasisAlertRepository>,
        trade_tape_on_chain: deps.deploy.market_data.trade_tape_on_chain.clone(),
    }));
    let factor_repo: Arc<dyn FactorRepository> =
        Arc::clone(&deps.infra.repos.factor) as Arc<dyn FactorRepository>;
    let factor_pipeline = Arc::new(FactorPipelineService::new(
        Arc::clone(&factor_repo),
        Arc::clone(&deps.infra.factor_event_writer),
        Arc::clone(&deps.governance.bias_table),
    ));
    ResearchPipelines {
        candidate_provider,
        feature_repo,
        feature_pipeline,
        factor_repo,
        factor_pipeline,
    }
}

impl ResearchBundle {
    /// Build the research bundle from deploy config plus wired infra/data handles.
    ///
    /// No report scheduler or trigger is wired here — periodic report generation
    /// is a Phase 4 concern. The feature and factor pipelines are ready for
    /// on-demand invocation with a frozen runtime-config snapshot per round.
    #[must_use]
    pub fn assemble(deps: &ResearchBundleDeps<'_>) -> Self {
        let repos = &deps.infra.repos;
        let artifact_store: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(
            deps.deploy.research.artifact_root.clone(),
        ));
        let market_selector: Arc<dyn MarketSelector> = Arc::new(ConfiguredMarketSelector::new());
        let market_selection_repo: Arc<dyn MarketSelectionRepository> =
            Arc::clone(&repos.market_selection) as Arc<dyn MarketSelectionRepository>;
        let market_linkage_repo: Arc<dyn MarketLinkageRepository> =
            Arc::clone(&repos.market_linkage) as Arc<dyn MarketLinkageRepository>;
        let pipelines = assemble_research_pipelines(deps, &market_linkage_repo);
        let candidate_provider = pipelines.candidate_provider;
        let feature_repo = pipelines.feature_repo;
        let feature_pipeline = pipelines.feature_pipeline;
        let factor_repo = pipelines.factor_repo;
        let factor_pipeline = pipelines.factor_pipeline;

        let model_run_repo: Arc<dyn ModelRunRepository> =
            Arc::clone(&repos.model_run) as Arc<dyn ModelRunRepository>;
        let model_registry_repo: Arc<dyn ModelRegistryRepository> =
            Arc::clone(&repos.model_registry) as Arc<dyn ModelRegistryRepository>;
        let calibration_loader: Arc<dyn CalibrationArtifactLoader> =
            Arc::new(CoreCalibrationArtifactLoader::new(
                Arc::clone(&repos.calibration_artifact) as Arc<dyn CalibrationArtifactRepository>,
            ));
        let model_runtime_factory_builder: Arc<dyn ModelRuntimeFactoryBuilder> =
            Arc::new(DefaultModelRuntimeFactoryBuilder::new(
                Arc::clone(&artifact_store),
                Arc::clone(&calibration_loader),
            ));
        let shadow_comparison_repo: Arc<dyn ShadowComparisonRepository> =
            Arc::clone(&repos.shadow_comparison) as Arc<dyn ShadowComparisonRepository>;
        let model_runner = Self::assemble_model_runner(
            deps,
            &model_run_repo,
            &model_registry_repo,
            &shadow_comparison_repo,
            &model_runtime_factory_builder,
            &factor_pipeline,
        );

        let offline = Self::assemble_offline_repositories(deps);

        let frozen_model_parity = Arc::new(FrozenModelParityService::new(FrozenModelParityDeps {
            dataset_repo: Arc::clone(&offline.training_dataset),
            model_registry_repo: Arc::clone(&model_registry_repo),
            parity_repo: Arc::clone(&deps.infra.repos.feature_parity)
                as Arc<dyn FeatureParityRepository>,
            artifact_store: Arc::clone(&artifact_store),
            evidence_writer: frozen_parity_evidence_writer(deps.infra),
        }));
        let model_governance = Self::assemble_model_governance(
            deps,
            &artifact_store,
            &model_registry_repo,
            &shadow_comparison_repo,
            &offline,
            &calibration_loader,
            &frozen_model_parity,
        );
        let factor_governance = Self::assemble_factor_governance(&factor_repo);
        let model_spec: Arc<dyn ModelSpecPort> = Arc::new(ModelSpecService::new(ModelSpecDeps {
            model_registry: Arc::clone(&model_registry_repo),
            runtime_config: Arc::clone(&deps.governance.runtime_config),
        }));
        let calibration_artifact_fit =
            assemble_calibration_artifact_fit(deps, &market_linkage_repo, &offline);

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
            frozen_model_parity,
            model_runner,
            training_dataset_repo: offline.training_dataset,
            attribution_repo: offline.attribution,
            recommendation_repo: offline.recommendation,
            backtest_report_repo: offline.backtest_report,
            backtest_path_set_repo: Arc::clone(&offline.backtest_path_set),
            comparison_report_repo: offline.comparison_report,
            shadow_comparison_repo,
            governance_audit_repo: offline.governance_audit,
            model_governance,
            factor_governance,
            model_spec,
            model_runtime_factory_builder,
            calibration_loader,
            quant_fact_read: Arc::clone(&deps.infra.quant_fact_read),
            catalog_version_repo: Arc::clone(&deps.data.catalog_version_repo),
            market_repo: Arc::clone(&deps.data.market_repo),
            market_linkage_repo: Arc::clone(&market_linkage_repo),
            position_repo: Arc::clone(&repos.position) as Arc<dyn PositionRepository>,
            fee_calculator: Arc::clone(&deps.data.fee_calculator),
            calibration_artifact_fit,
        }
    }

    /// Construct an offline training-dataset service bound to a frozen
    /// runtime-config snapshot. The service plans a deterministic sample grid,
    /// batch-prefetches historical facts, materializes PIT features + forward
    /// labels, and writes a content-hashed Parquet artifact + ledger row.
    pub fn training_dataset_service(
        &self,
        wire: TrainingDatasetServiceWire,
    ) -> QuantResult<TrainingDatasetService> {
        TrainingDatasetService::new(
            TrainingDatasetServiceDeps {
                fact_read: Arc::clone(&self.quant_fact_read),
                catalog_repo: Arc::clone(&self.catalog_version_repo),
                market_repo: Arc::clone(&self.market_repo),
                artifact_store: Arc::clone(&self.artifact_store),
                dataset_repo: Arc::clone(&self.training_dataset_repo),
                attribution_repo: Arc::clone(&self.attribution_repo),
                recommendation_repo: Arc::clone(&self.recommendation_repo),
                feature_repo: Arc::clone(&self.feature_repo),
                selection_repo: Arc::clone(&self.market_selection_repo),
                position_repo: Arc::clone(&self.position_repo),
                fee_calculator: Arc::clone(&self.fee_calculator),
                linkage_repo: Arc::clone(&self.market_linkage_repo),
                model_registry: Arc::clone(&self.model_registry_repo),
            },
            wire.config,
            wire.max_spine_samples,
        )
    }

    /// Wire the online inference orchestrator (3.4).
    fn assemble_model_runner(
        deps: &ResearchBundleDeps<'_>,
        model_run_repo: &Arc<dyn ModelRunRepository>,
        model_registry_repo: &Arc<dyn ModelRegistryRepository>,
        shadow_comparison_repo: &Arc<dyn ShadowComparisonRepository>,
        factory_builder: &Arc<dyn ModelRuntimeFactoryBuilder>,
        factor_pipeline: &Arc<FactorPipelineService>,
    ) -> Arc<ModelRunner> {
        Arc::new(ModelRunner::new(ModelRunnerDeps {
            model_run_repo: Arc::clone(model_run_repo),
            model_registry_repo: Arc::clone(model_registry_repo),
            shadow_comparison_repo: Arc::clone(shadow_comparison_repo),
            factory_builder: Arc::clone(factory_builder),
            factor_pipeline: Arc::clone(factor_pipeline),
            signal_writer: Arc::clone(&deps.infra.signal_candidate_event_writer),
            model_input_writer: Arc::clone(&deps.infra.model_input_event_writer),
            alerts: Arc::new(DispatcherAlertSink::new(Arc::clone(
                &deps.governance.alerts,
            ))),
            weight_overlay: Arc::clone(&deps.governance.weight_overlay),
            bias_table: Arc::clone(&deps.governance.bias_table),
        }))
    }

    fn assemble_offline_repositories(deps: &ResearchBundleDeps<'_>) -> OfflineResearchRepos {
        let repos = &deps.infra.repos;
        OfflineResearchRepos {
            training_dataset: Arc::clone(&repos.training_dataset)
                as Arc<dyn TrainingDatasetRepository>,
            attribution: Arc::clone(&repos.attribution) as Arc<dyn AttributionRepository>,
            recommendation: Arc::clone(&repos.recommendation) as Arc<dyn RecommendationRepository>,
            backtest_report: Arc::clone(&repos.backtest_report)
                as Arc<dyn BacktestReportRepository>,
            backtest_path_set: Arc::clone(&repos.backtest_path_set)
                as Arc<dyn BacktestPathSetRepository>,
            comparison_report: Arc::clone(&repos.comparison_report)
                as Arc<dyn ModelComparisonReportRepository>,
            governance_audit: Arc::clone(&repos.governance_audit)
                as Arc<dyn ModelGovernanceAuditRepository>,
        }
    }

    /// Wire offline publish / rollback / dataset-promotion governance (3.7).
    fn assemble_model_governance(
        deps: &ResearchBundleDeps<'_>,
        artifact_store: &Arc<dyn ArtifactStore>,
        model_registry_repo: &Arc<dyn ModelRegistryRepository>,
        shadow_comparison_repo: &Arc<dyn ShadowComparisonRepository>,
        offline: &OfflineResearchRepos,
        calibration_loader: &Arc<dyn CalibrationArtifactLoader>,
        frozen_model_parity: &Arc<FrozenModelParityService>,
    ) -> Arc<dyn ModelGovernancePort> {
        let gate: Arc<dyn ModelQualityGate> = Arc::new(DefaultModelQualityGate::new());
        let runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository> =
            Arc::clone(&deps.infra.repos.runtime_config) as Arc<dyn RuntimeConfigVersionRepository>;
        let runtime_config_apply: Arc<dyn RuntimeConfigPort> =
            Arc::clone(&deps.governance.applicator) as Arc<dyn RuntimeConfigPort>;
        Arc::new(ModelGovernanceService::new(ModelGovernanceDeps {
            model_registry_repo: Arc::clone(model_registry_repo),
            backtest_report_repo: Arc::clone(&offline.backtest_report),
            backtest_path_set_repo: Arc::clone(&offline.backtest_path_set),
            shadow_comparison_repo: Arc::clone(shadow_comparison_repo),
            governance_audit_repo: Arc::clone(&offline.governance_audit),
            dataset_repo: Arc::clone(&offline.training_dataset),
            artifact_store: Arc::clone(artifact_store),
            calibration_repo: Arc::clone(&deps.infra.repos.calibration_artifact)
                as Arc<dyn CalibrationArtifactRepository>,
            calibration_loader: Arc::clone(calibration_loader),
            gate,
            runtime_config: Arc::clone(&deps.governance.runtime_config),
            runtime_config_apply,
            runtime_config_repo,
            feature_parity_gate: Arc::new(
                crate::service::feature_integrity::RepositoryFeatureParityGate::new(Arc::clone(
                    &deps.infra.repos.feature_parity,
                )
                    as Arc<dyn FeatureParityRepository>),
            ),
            frozen_model_parity: Arc::clone(frozen_model_parity),
        }))
    }

    fn assemble_factor_governance(
        factor_repo: &Arc<dyn FactorRepository>,
    ) -> Arc<dyn FactorGovernancePort> {
        Arc::new(FactorGovernanceService::new(FactorGovernanceDeps {
            factor_repo: Arc::clone(factor_repo),
        }))
    }
}

fn assemble_calibration_artifact_fit(
    deps: &ResearchBundleDeps<'_>,
    market_linkage_repo: &Arc<dyn MarketLinkageRepository>,
    offline: &OfflineResearchRepos,
) -> Arc<dyn CalibrationArtifactFitPort> {
    Arc::new(BiasTableFitService::new(
        Arc::clone(&deps.infra.quant_fact_read),
        Arc::clone(&deps.data.catalog_version_repo),
        Arc::clone(&deps.data.market_repo),
        Arc::clone(market_linkage_repo),
        Arc::clone(&deps.infra.repos.calibration_artifact)
            as Arc<dyn CalibrationArtifactRepository>,
        Arc::clone(&deps.infra.repos.runtime_config) as Arc<dyn RuntimeConfigVersionRepository>,
        Arc::clone(&offline.training_dataset) as Arc<dyn TrainingDatasetRepository>,
    ))
}
