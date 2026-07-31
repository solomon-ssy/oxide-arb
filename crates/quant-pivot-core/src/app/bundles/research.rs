//! Research plane bundle: artifacts, selection, feature, and factor pipelines.

use std::sync::Arc;

use quant_pivot_compute::ComputeExecutor;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    clickhouse::QuantFeatureParityEventRow,
    config::DeployConfig,
    domain::ports::{
        CalibrationArtifactFitPort, CommittedPolicyApplyPort, ModelGovernancePort, ModelSpecPort,
    },
};
use quant_pivot_repository::{
    clickhouse::ChFactWriter,
    traits::{
        BacktestPathSetRepository, BacktestReportRepository, BasisAlertRepository,
        CalibrationArtifactRepository, CatalogLedgerRepository, ClobMarketInfoRepository,
        FactWriter, FactorRepository, FeatureParityRepository, FeatureRepository,
        FeedbackCycleRepository, MarketLinkageRepository, MarketRepository,
        MarketSelectionRepository, ModelCandidateManifestRepository,
        ModelComparisonReportRepository, ModelGovernanceAuditRepository, ModelRegistryRepository,
        ModelRouteBootstrapRepository, ModelRoutePromotionRepository, ModelRunRepository,
        PolicyRepository, PositionRepository, PromotionPermitRepository, QuantFactReadRepository,
        ResearchJobRepository, ResearchReadinessEvidenceRepository, RuntimeControlRepository,
        ShadowComparisonRepository, SourceSliceRepository, TradePolicyRepository,
        TradeTapeBlockCursorRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, build_artifact_store},
    gates::{DefaultModelQualityGate, ModelQualityGate},
    model::CalibrationArtifactLoader,
    selection::{ConfiguredMarketSelector, MarketSelector},
};

use super::{DataBundle, GovernanceBundle, InfraBundle};
use crate::{
    governance::{
        CoreCalibrationArtifactLoader, ModelGovernanceDeps, ModelGovernanceService, ModelSpecDeps,
        ModelSpecService,
    },
    prefetch::{feature_window::FeatureWindowProvider, market_candidates::MarketCandidateProvider},
    service::{
        bias_table_fit::BiasTableFitService,
        factor_pipeline::FactorPipelineService,
        feature_pipeline::{FeaturePipelineDeps, FeaturePipelineService},
        feedback_decision_stage::{FeedbackDecisionStageAdapter, FeedbackDecisionStageDeps},
        frozen_model_parity::{FrozenModelParityDeps, FrozenModelParityService},
        model_route_bootstrap::{ModelRouteBootstrapService, ModelRouteBootstrapServiceDeps},
        model_route_evidence::{ModelRouteEvidenceDeps, ModelRouteEvidenceService},
        model_route_governance::{ModelRouteGovernanceService, ModelRouteGovernanceServiceDeps},
        model_runner::{DispatcherAlertSink, ModelRunner, ModelRunnerDeps},
        model_serving_generation::ModelServingGenerationStore,
        model_serving_preimage::{ModelServingPreimageDeps, ModelServingPreimageService},
        model_serving_registry::ModelServingRuntimeRegistry,
        promotion_preflight::{PromotionPreflightService, PromotionPreflightServiceDeps},
        research_readiness::{
            EvidenceAttestor, EvidenceScopeIdentity, ResearchReadinessEvidenceService,
        },
        trade_policy_evidence::{TradePolicyEvidenceVerifier, TradePolicyEvidenceVerifierDeps},
        trade_policy_preimage::{TradePolicyPreimageVerifier, TradePolicyPreimageVerifierDeps},
        training_dataset::{
            TrainingDatasetService, TrainingDatasetServiceDeps, TrainingDatasetServiceWire,
        },
    },
};

/// Dependencies required to assemble the research plane after infra + data.
pub struct ResearchBundleDeps<'a> {
    /// Deploy-time configuration (artifact root, etc.).
    pub deploy: &'a DeployConfig,
    /// Process-wide serving/offline CPU and memory governor.
    pub compute: &'a Arc<ComputeExecutor>,
    /// Persistence and analytics handles.
    pub infra: &'a InfraBundle,
    /// Live data plane (books, registry, PIT source for online feature builds).
    pub data: &'a DataBundle,
    /// Governance plane (operator alert dispatcher for inference degradation).
    pub governance: &'a GovernanceBundle,
}

impl InfraBundle {
    fn frozen_parity_evidence_writer(&self) -> Arc<dyn FactWriter<QuantFeatureParityEventRow>> {
        Arc::new(ChFactWriter::new(
            Arc::clone(&self.ch),
            Arc::clone(&self.ch_write_manager),
            "quant_feature_parity_event",
        ))
    }
}

/// Research plane: selection, feature/factor pipelines, and artifact store.
pub struct ResearchBundle {
    /// Process-wide serving/offline CPU and memory governor.
    pub compute: Arc<ComputeExecutor>,
    /// Local (or future object-store) backend for dataset / model artifact bytes.
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Pure, config-driven market selector.
    pub market_selector: Arc<dyn MarketSelector>,
    /// Persistence port for selection snapshots and their members.
    pub market_selection_repo: Arc<dyn MarketSelectionRepository>,
    /// Core-side projector freezing market facts into selector inputs.
    pub candidate_provider: Arc<MarketCandidateProvider>,
    /// Postgres persistence for feature vectors.
    pub feature_repo: Arc<dyn FeatureRepository>,
    /// Online feature build loop: resolve → build → persist → emit.
    pub feature_pipeline: Arc<FeaturePipelineService>,
    /// Postgres persistence for factor definitions + values.
    pub factor_repo: Arc<dyn FactorRepository>,
    /// Online factor build loop: compute → partition → persist → emit.
    pub factor_pipeline: Arc<FactorPipelineService>,
    /// Model-run persistence (create / finalize live + shadow runs).
    pub model_run_repo: Arc<dyn ModelRunRepository>,
    /// Model registry persistence (resolve active / shadow versions).
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    /// Full frozen dataset/model verification used by training and publish.
    pub frozen_model_parity: Arc<FrozenModelParityService>,
    /// Canonical deep verifier for every opaque model-serving preimage.
    pub serving_preimages: Arc<ModelServingPreimageService>,
    /// Process-wide bounded cache for fully verified serving runtimes/planes.
    pub runtime_registry: Arc<ModelServingRuntimeRegistry>,
    /// Sole atomic owner of complete active/shadow/category serving
    /// generations.
    pub serving_generations: Arc<ModelServingGenerationStore>,
    /// Shared current policy/runtime/model/parity resolver for route governance.
    pub model_route_evidence: Arc<ModelRouteEvidenceService>,
    /// Dedicated first-champion route bootstrap preflight.
    pub model_route_bootstrap: Arc<ModelRouteBootstrapService>,
    /// Read-only server-derived permit and promotion preflight boundary.
    pub promotion_preflight: Arc<PromotionPreflightService>,
    /// Canonical verifier for the terminal feedback decision evidence graph.
    pub feedback_decisions: Arc<FeedbackDecisionStageAdapter>,
    /// Governed owner of the exact preflight-to-transaction handoff.
    pub model_route_governance: Arc<ModelRouteGovernanceService>,
    /// Process-wide verifier for signed operational readiness evidence.
    pub research_readiness: Arc<ResearchReadinessEvidenceService>,
    /// Canonical resolver for immutable trade-policy evidence graphs.
    pub trade_policy_evidence: Arc<TradePolicyEvidenceVerifier>,
    /// Canonical resolver for executable `TradePolicy` serving dependency graphs.
    pub trade_policy_preimages: Arc<TradePolicyPreimageVerifier>,
    /// Online inference orchestrator: selection/features/factors → candidates.
    pub model_runner: Arc<ModelRunner>,
    /// Frozen training-dataset ledger persistence.
    pub training_dataset_repo: Arc<dyn TrainingDatasetRepository>,
    /// Server-owned point-in-time source-slice materialization ledger.
    pub source_slice_repo: Arc<dyn SourceSliceRepository>,
    /// Governed executable trade-policy catalog.
    pub trade_policy_repo: Arc<dyn TradePolicyRepository>,
    /// Append-only backtest-report ledger persistence.
    pub backtest_report_repo: Arc<dyn BacktestReportRepository>,
    /// Append-only CPCV + trial-grid path-set ledger persistence.
    pub backtest_path_set_repo: Arc<dyn BacktestPathSetRepository>,
    /// Append-only pairwise comparison-report ledger persistence.
    pub comparison_report_repo: Arc<dyn ModelComparisonReportRepository>,
    /// Append-only shadow-comparison ledger persistence.
    pub shadow_comparison_repo: Arc<dyn ShadowComparisonRepository>,
    /// Append-only model-governance audit trail persistence.
    pub governance_audit_repo: Arc<dyn ModelGovernanceAuditRepository>,
    /// Offline governance orchestration: publish / rollback / dataset promotion.
    pub model_governance: Arc<dyn ModelGovernancePort>,
    /// Model-spec authoring (the offline research lifecycle root write path).
    pub model_spec: Arc<dyn ModelSpecPort>,
    /// Resolves `model_score` calibration artifacts for runtime + Kelly safety.
    pub calibration_loader: Arc<dyn CalibrationArtifactLoader>,
    /// Historical fact read port (PIT book / microstructure / settlement).
    pub quant_fact_read: Arc<dyn QuantFactReadRepository>,
    /// Append-only catalog ledger for every offline PIT metadata read.
    pub catalog_ledger_repo: Arc<dyn CatalogLedgerRepository>,
    /// Append-only point-in-time CLOB parameters and fee schedules.
    pub clob_market_info_repo: Arc<dyn ClobMarketInfoRepository>,
    /// Market catalog read port for PIT metadata + sampling candidates.
    pub market_repo: Arc<dyn MarketRepository>,
    /// Frozen market → external-subject linkage ledger.
    pub market_linkage_repo: Arc<dyn MarketLinkageRepository>,
    /// Position ledger for `ExitDecision` lot-timeline training.
    pub position_repo: Arc<dyn PositionRepository>,
    /// Unified calibration-artifact ledger port: favorite-longshot bias-table
    /// fitter plus generic catalog read/activate for every artifact kind.
    pub calibration_artifact_fit: Arc<dyn CalibrationArtifactFitPort>,
    /// Calibration artifact catalog used by historical PIT feature windows.
    pub calibration_artifact_repo: Arc<dyn CalibrationArtifactRepository>,
}

struct OfflineResearchRepos {
    training_dataset: Arc<dyn TrainingDatasetRepository>,
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

struct ResearchEvidence {
    readiness: Arc<ResearchReadinessEvidenceService>,
    trade_policy: Arc<TradePolicyEvidenceVerifier>,
}

#[derive(Clone, Copy)]
struct ModelGovernanceAssembly<'a, 'b> {
    deps: &'a ResearchBundleDeps<'b>,
    artifact_store: &'a Arc<dyn ArtifactStore>,
    model_registry_repo: &'a Arc<dyn ModelRegistryRepository>,
    shadow_comparison_repo: &'a Arc<dyn ShadowComparisonRepository>,
    offline: &'a OfflineResearchRepos,
    calibration_loader: &'a Arc<dyn CalibrationArtifactLoader>,
    frozen_model_parity: &'a Arc<FrozenModelParityService>,
    serving_preimages: &'a Arc<ModelServingPreimageService>,
}

#[derive(Clone, Copy)]
struct PromotionPreflightAssembly<'a, 'b> {
    deps: &'a ResearchBundleDeps<'b>,
    decisions: &'a Arc<FeedbackDecisionStageAdapter>,
    route_evidence: &'a Arc<ModelRouteEvidenceService>,
}

#[derive(Clone, Copy)]
struct RouteServiceAssembly<'a, 'b> {
    deps: &'a ResearchBundleDeps<'b>,
    artifact_store: &'a Arc<dyn ArtifactStore>,
    model_registry_repo: &'a Arc<dyn ModelRegistryRepository>,
    runtime_registry: &'a Arc<ModelServingRuntimeRegistry>,
    serving_generations: &'a Arc<ModelServingGenerationStore>,
    offline: &'a OfflineResearchRepos,
    model_governance: &'a Arc<dyn ModelGovernancePort>,
}

struct RouteServices {
    evidence: Arc<ModelRouteEvidenceService>,
    bootstrap: Arc<ModelRouteBootstrapService>,
    promotion_preflight: Arc<PromotionPreflightService>,
    feedback_decisions: Arc<FeedbackDecisionStageAdapter>,
    governance: Arc<ModelRouteGovernanceService>,
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
        compute: Arc::clone(deps.compute),
        window_provider: FeatureWindowProvider::new(Arc::clone(&deps.infra.quant_fact_read)),
        feature_repo: Arc::clone(&feature_repo),
        event_writer: Arc::clone(&deps.infra.feature_event_writer),
        market_registry: Arc::clone(&deps.data.market_registry),
        block_cursor_repo: Arc::clone(&deps.infra.repos.trade_tape_block_cursor)
            as Arc<dyn TradeTapeBlockCursorRepository>,
        linkage_repo: Arc::clone(market_linkage_repo),
        basis_alert_repo: Arc::clone(&deps.infra.repos.basis_alert)
            as Arc<dyn BasisAlertRepository>,
        calibration_repo: Arc::clone(&deps.infra.repos.calibration_artifact)
            as Arc<dyn CalibrationArtifactRepository>,
        trade_tape_on_chain: deps.deploy.market_data.trade_tape_on_chain.clone(),
    }));
    let factor_repo: Arc<dyn FactorRepository> =
        Arc::clone(&deps.infra.repos.factor) as Arc<dyn FactorRepository>;
    let factor_pipeline = Arc::new(FactorPipelineService::new(
        Arc::clone(&factor_repo),
        Arc::clone(&deps.infra.factor_event_writer),
        Arc::clone(deps.compute),
    ));
    ResearchPipelines {
        candidate_provider,
        feature_repo,
        feature_pipeline,
        factor_repo,
        factor_pipeline,
    }
}

fn assemble_research_evidence(
    deps: &ResearchBundleDeps<'_>,
    artifact_store: &Arc<dyn ArtifactStore>,
) -> QuantResult<ResearchEvidence> {
    let repos = &deps.infra.repos;
    let evidence_scope = EvidenceScopeIdentity::from_config(
        &deps.deploy.db.clickhouse,
        &deps.deploy.research.artifact_store,
    )?;
    let readiness = Arc::new(ResearchReadinessEvidenceService::new(
        Arc::clone(&repos.research_readiness) as Arc<dyn ResearchReadinessEvidenceRepository>,
        Arc::clone(artifact_store),
        EvidenceAttestor::from_config(&deps.deploy.research.evidence_attestation)?,
        &evidence_scope,
    )?);
    let trade_policy = Arc::new(TradePolicyEvidenceVerifier::new(
        TradePolicyEvidenceVerifierDeps {
            artifacts: Arc::clone(artifact_store),
            policies: Arc::clone(&repos.trade_policy) as Arc<dyn TradePolicyRepository>,
            readiness: Arc::clone(&readiness),
        },
    ));
    Ok(ResearchEvidence {
        readiness,
        trade_policy,
    })
}

impl ResearchBundle {
    /// Build the research bundle from deploy config plus wired infra/data handles.
    ///
    /// No report scheduler or trigger is wired here; report orchestration belongs
    /// to the report bundle. The feature and factor pipelines are ready for
    /// on-demand invocation with a frozen runtime-config snapshot per round.
    pub async fn assemble(deps: &ResearchBundleDeps<'_>) -> QuantResult<Self> {
        let repos = &deps.infra.repos;
        let artifact_store: Arc<dyn ArtifactStore> =
            build_artifact_store(&deps.deploy.research.artifact_store)?;
        let evidence = assemble_research_evidence(deps, &artifact_store)?;
        let market_selector: Arc<dyn MarketSelector> = Arc::new(ConfiguredMarketSelector::new());
        let market_selection_repo: Arc<dyn MarketSelectionRepository> =
            Arc::clone(&repos.market_selection) as Arc<dyn MarketSelectionRepository>;
        let market_linkage_repo: Arc<dyn MarketLinkageRepository> =
            Arc::clone(&repos.market_linkage) as Arc<dyn MarketLinkageRepository>;
        let pipelines = assemble_research_pipelines(deps, &market_linkage_repo);

        let model_run_repo: Arc<dyn ModelRunRepository> =
            Arc::clone(&repos.model_run) as Arc<dyn ModelRunRepository>;
        let model_registry_repo: Arc<dyn ModelRegistryRepository> =
            Arc::clone(&repos.model_registry) as Arc<dyn ModelRegistryRepository>;
        let calibration_loader: Arc<dyn CalibrationArtifactLoader> =
            Arc::new(CoreCalibrationArtifactLoader::new(
                Arc::clone(&repos.calibration_artifact) as Arc<dyn CalibrationArtifactRepository>,
            ));
        let shadow_comparison_repo: Arc<dyn ShadowComparisonRepository> =
            Arc::clone(&repos.shadow_comparison) as Arc<dyn ShadowComparisonRepository>;
        let offline = Self::assemble_offline_repositories(deps);
        let trade_policy_preimages = Arc::new(TradePolicyPreimageVerifier::new(
            TradePolicyPreimageVerifierDeps {
                trade_policy_repo: Arc::clone(&repos.trade_policy)
                    as Arc<dyn TradePolicyRepository>,
                dataset_repo: Arc::clone(&offline.training_dataset),
                model_registry_repo: Arc::clone(&model_registry_repo),
                evidence: Arc::clone(&evidence.trade_policy),
            },
        ));
        let serving_preimages =
            Arc::new(ModelServingPreimageService::new(ModelServingPreimageDeps {
                model_registry_repo: Arc::clone(&model_registry_repo),
                dataset_repo: Arc::clone(&offline.training_dataset),
                source_slice_repo: Arc::clone(&repos.source_slice)
                    as Arc<dyn SourceSliceRepository>,
                policy_repo: Arc::clone(&repos.runtime_config) as Arc<dyn PolicyRepository>,
                calibration_repo: Arc::clone(&repos.calibration_artifact)
                    as Arc<dyn CalibrationArtifactRepository>,
                trade_policy_preimages: Arc::clone(&trade_policy_preimages),
                artifact_store: Arc::clone(&artifact_store),
            }));
        let runtime_registry = Arc::new(ModelServingRuntimeRegistry::new(
            deps.deploy.research.model_serving_registry,
            Arc::clone(&serving_preimages),
        )?);
        let serving_generations =
            Self::assemble_serving_generations(deps, &model_registry_repo, &runtime_registry)
                .await?;
        deps.governance
            .applicator
            .attach_model_serving(Arc::clone(&serving_generations))?;
        let model_runner = Self::assemble_model_runner(
            deps,
            &model_run_repo,
            &shadow_comparison_repo,
            &serving_generations,
            &pipelines.factor_pipeline,
        );

        let frozen_model_parity = Arc::new(FrozenModelParityService::new(FrozenModelParityDeps {
            dataset_repo: Arc::clone(&offline.training_dataset),
            model_registry_repo: Arc::clone(&model_registry_repo),
            parity_repo: Arc::clone(&deps.infra.repos.feature_parity)
                as Arc<dyn FeatureParityRepository>,
            artifact_store: Arc::clone(&artifact_store),
            evidence_writer: (deps.infra).frozen_parity_evidence_writer(),
        }));
        let model_governance = Self::assemble_model_governance(ModelGovernanceAssembly {
            deps,
            artifact_store: &artifact_store,
            model_registry_repo: &model_registry_repo,
            shadow_comparison_repo: &shadow_comparison_repo,
            offline: &offline,
            calibration_loader: &calibration_loader,
            frozen_model_parity: &frozen_model_parity,
            serving_preimages: &serving_preimages,
        });
        let RouteServices {
            evidence: model_route_evidence,
            bootstrap: model_route_bootstrap,
            promotion_preflight,
            feedback_decisions,
            governance: model_route_governance,
        } = Self::assemble_route_services(RouteServiceAssembly {
            deps,
            artifact_store: &artifact_store,
            model_registry_repo: &model_registry_repo,
            runtime_registry: &runtime_registry,
            serving_generations: &serving_generations,
            offline: &offline,
            model_governance: &model_governance,
        })?;
        let model_spec: Arc<dyn ModelSpecPort> = Arc::new(ModelSpecService::new(ModelSpecDeps {
            model_registry: Arc::clone(&model_registry_repo),
            runtime_config: Arc::clone(&deps.governance.runtime_config),
        }));
        let calibration_artifact_fit =
            assemble_calibration_artifact_fit(deps, &market_linkage_repo, &offline);

        Ok(Self {
            compute: Arc::clone(deps.compute),
            artifact_store,
            market_selector,
            market_selection_repo,
            candidate_provider: pipelines.candidate_provider,
            feature_repo: pipelines.feature_repo,
            feature_pipeline: pipelines.feature_pipeline,
            factor_repo: pipelines.factor_repo,
            factor_pipeline: pipelines.factor_pipeline,
            model_run_repo,
            model_registry_repo,
            frozen_model_parity,
            serving_preimages,
            runtime_registry,
            serving_generations,
            model_route_evidence,
            model_route_bootstrap,
            promotion_preflight,
            feedback_decisions,
            model_route_governance,
            research_readiness: evidence.readiness,
            trade_policy_evidence: evidence.trade_policy,
            trade_policy_preimages,
            model_runner,
            training_dataset_repo: offline.training_dataset,
            source_slice_repo: Arc::clone(&repos.source_slice) as Arc<dyn SourceSliceRepository>,
            trade_policy_repo: Arc::clone(&repos.trade_policy) as Arc<dyn TradePolicyRepository>,
            backtest_report_repo: offline.backtest_report,
            backtest_path_set_repo: Arc::clone(&offline.backtest_path_set),
            comparison_report_repo: offline.comparison_report,
            shadow_comparison_repo,
            governance_audit_repo: offline.governance_audit,
            model_governance,
            model_spec,
            calibration_loader,
            quant_fact_read: Arc::clone(&deps.infra.quant_fact_read),
            catalog_ledger_repo: Arc::clone(&deps.data.catalog_ledger_repo),
            clob_market_info_repo: Arc::clone(&repos.clob_market_info)
                as Arc<dyn ClobMarketInfoRepository>,
            market_repo: Arc::clone(&deps.data.market_repo),
            market_linkage_repo: Arc::clone(&market_linkage_repo),
            position_repo: Arc::clone(&repos.position) as Arc<dyn PositionRepository>,
            calibration_artifact_fit,
            calibration_artifact_repo: Arc::clone(&repos.calibration_artifact)
                as Arc<dyn CalibrationArtifactRepository>,
        })
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
                compute: Arc::clone(&self.compute),
                fact_read: Arc::clone(&self.quant_fact_read),
                catalog_repo: Arc::clone(&self.catalog_ledger_repo),
                market_repo: Arc::clone(&self.market_repo),
                artifact_store: Arc::clone(&self.artifact_store),
                dataset_repo: Arc::clone(&self.training_dataset_repo),
                position_repo: Arc::clone(&self.position_repo),
                clob_market_info_repo: Arc::clone(&self.clob_market_info_repo),
                linkage_repo: Arc::clone(&self.market_linkage_repo),
                model_registry: Arc::clone(&self.model_registry_repo),
                trade_policy_repo: Arc::clone(&self.trade_policy_repo),
                calibration_repo: Arc::clone(&self.calibration_artifact_repo),
            },
            wire.config,
            wire.max_spine_samples,
        )
    }

    /// Wire the online inference orchestrator.
    fn assemble_model_runner(
        deps: &ResearchBundleDeps<'_>,
        model_run_repo: &Arc<dyn ModelRunRepository>,
        shadow_comparison_repo: &Arc<dyn ShadowComparisonRepository>,
        serving_generations: &Arc<ModelServingGenerationStore>,
        factor_pipeline: &Arc<FactorPipelineService>,
    ) -> Arc<ModelRunner> {
        Arc::new(ModelRunner::new(ModelRunnerDeps {
            model_run_repo: Arc::clone(model_run_repo),
            shadow_comparison_repo: Arc::clone(shadow_comparison_repo),
            serving_generations: Arc::clone(serving_generations),
            factor_pipeline: Arc::clone(factor_pipeline),
            signal_writer: Arc::clone(&deps.infra.signal_candidate_event_writer),
            model_input_writer: Arc::clone(&deps.infra.model_input_event_writer),
            alerts: Arc::new(DispatcherAlertSink::new(Arc::clone(
                &deps.governance.alerts,
            ))),
        }))
    }

    fn assemble_offline_repositories(deps: &ResearchBundleDeps<'_>) -> OfflineResearchRepos {
        let repos = &deps.infra.repos;
        OfflineResearchRepos {
            training_dataset: Arc::clone(&repos.training_dataset)
                as Arc<dyn TrainingDatasetRepository>,
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

    fn assemble_promotion_preflight(
        assembly: PromotionPreflightAssembly<'_, '_>,
    ) -> Arc<PromotionPreflightService> {
        let PromotionPreflightAssembly {
            deps,
            decisions,
            route_evidence,
        } = assembly;
        let repos = &deps.infra.repos;
        Arc::new(PromotionPreflightService::new(
            PromotionPreflightServiceDeps {
                permits: Arc::clone(&repos.promotion_permit) as Arc<dyn PromotionPermitRepository>,
                cycles: Arc::clone(&repos.feedback_cycle) as Arc<dyn FeedbackCycleRepository>,
                decisions: Arc::clone(decisions),
                manifests: Arc::clone(&repos.model_candidate_manifest)
                    as Arc<dyn ModelCandidateManifestRepository>,
                route_evidence: Arc::clone(route_evidence),
                metrics: Arc::clone(&deps.infra.metrics),
            },
        ))
    }

    fn assemble_route_services(
        assembly: RouteServiceAssembly<'_, '_>,
    ) -> QuantResult<RouteServices> {
        let RouteServiceAssembly {
            deps,
            artifact_store,
            model_registry_repo,
            runtime_registry,
            serving_generations,
            offline,
            model_governance,
        } = assembly;
        let repos = &deps.infra.repos;
        let evidence = Arc::new(ModelRouteEvidenceService::new(ModelRouteEvidenceDeps {
            policies: Arc::clone(&repos.runtime_config) as Arc<dyn PolicyRepository>,
            durable_runtime: Arc::clone(&repos.runtime_control)
                as Arc<dyn RuntimeControlRepository>,
            runtime_controls: deps.governance.runtime_controls.clone(),
            policy_store: Arc::clone(&deps.governance.runtime_config),
            models: Arc::clone(model_registry_repo),
            feature_parity: Arc::clone(&repos.feature_parity) as Arc<dyn FeatureParityRepository>,
            runtime_registry: Arc::clone(runtime_registry),
            serving_generations: Arc::clone(serving_generations),
        }));
        let feedback_decisions = Arc::new(FeedbackDecisionStageAdapter::try_new(
            FeedbackDecisionStageDeps {
                cycles: Arc::clone(&repos.feedback_cycle) as Arc<dyn FeedbackCycleRepository>,
                jobs: Arc::clone(&repos.research_job) as Arc<dyn ResearchJobRepository>,
                artifacts: Arc::clone(artifact_store),
                max_recovery_attempts: deps.deploy.quant.research_jobs.max_recovery_attempts,
            },
        )?);
        let promotion_preflight = Self::assemble_promotion_preflight(PromotionPreflightAssembly {
            deps,
            decisions: &feedback_decisions,
            route_evidence: &evidence,
        });
        let bootstrap = Arc::new(ModelRouteBootstrapService::new(
            ModelRouteBootstrapServiceDeps {
                route_evidence: Arc::clone(&evidence),
                path_sets: Arc::clone(&offline.backtest_path_set),
                backtests: Arc::clone(&offline.backtest_report),
                cycles: Arc::clone(&repos.feedback_cycle) as Arc<dyn FeedbackCycleRepository>,
                model_governance: Arc::clone(model_governance),
            },
        ));
        let governance = Arc::new(ModelRouteGovernanceService::new(
            ModelRouteGovernanceServiceDeps {
                bootstrap_preflight: Arc::clone(&bootstrap),
                bootstrap_repository: Arc::clone(&repos.model_route_bootstrap)
                    as Arc<dyn ModelRouteBootstrapRepository>,
                preflight: Arc::clone(&promotion_preflight),
                repository: Arc::clone(&repos.model_route_promotion)
                    as Arc<dyn ModelRoutePromotionRepository>,
                policies: Arc::clone(&repos.runtime_config) as Arc<dyn PolicyRepository>,
                policy_apply: Arc::clone(&deps.governance.committed_policy)
                    as Arc<dyn CommittedPolicyApplyPort>,
            },
        ));
        Ok(RouteServices {
            evidence,
            bootstrap,
            promotion_preflight,
            feedback_decisions,
            governance,
        })
    }

    async fn assemble_serving_generations(
        deps: &ResearchBundleDeps<'_>,
        model_registry_repo: &Arc<dyn ModelRegistryRepository>,
        runtime_registry: &Arc<ModelServingRuntimeRegistry>,
    ) -> QuantResult<Arc<ModelServingGenerationStore>> {
        let active_bundle = deps
            .governance
            .runtime_config
            .active_bundle()
            .ok_or_else(|| {
                QuantError::config(
                    "active policy bundle metadata is unavailable during serving bootstrap",
                )
            })?;
        Ok(Arc::new(
            ModelServingGenerationStore::bootstrap(
                Arc::clone(model_registry_repo),
                Arc::clone(runtime_registry),
                active_bundle,
            )
            .await?,
        ))
    }

    /// Wire offline publish / rollback / dataset-promotion governance.
    fn assemble_model_governance(
        assembly: ModelGovernanceAssembly<'_, '_>,
    ) -> Arc<dyn ModelGovernancePort> {
        let ModelGovernanceAssembly {
            deps,
            artifact_store,
            model_registry_repo,
            shadow_comparison_repo,
            offline,
            calibration_loader,
            frozen_model_parity,
            serving_preimages,
        } = assembly;
        let gate: Arc<dyn ModelQualityGate> = Arc::new(DefaultModelQualityGate::new());
        Arc::new(ModelGovernanceService::new(ModelGovernanceDeps {
            model_registry_repo: Arc::clone(model_registry_repo),
            backtest_report_repo: Arc::clone(&offline.backtest_report),
            backtest_path_set_repo: Arc::clone(&offline.backtest_path_set),
            shadow_comparison_repo: Arc::clone(shadow_comparison_repo),
            governance_audit_repo: Arc::clone(&offline.governance_audit),
            dataset_repo: Arc::clone(&offline.training_dataset),
            serving_preimages: Arc::clone(serving_preimages),
            artifact_store: Arc::clone(artifact_store),
            calibration_repo: Arc::clone(&deps.infra.repos.calibration_artifact)
                as Arc<dyn CalibrationArtifactRepository>,
            calibration_loader: Arc::clone(calibration_loader),
            gate,
            runtime_config: Arc::clone(&deps.governance.runtime_config),
            frozen_model_parity: Arc::clone(frozen_model_parity),
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
        Arc::clone(&deps.data.catalog_ledger_repo),
        Arc::clone(&deps.data.market_repo),
        Arc::clone(market_linkage_repo),
        Arc::clone(&deps.infra.repos.calibration_artifact)
            as Arc<dyn CalibrationArtifactRepository>,
        Arc::clone(&deps.infra.repos.runtime_config) as Arc<dyn PolicyRepository>,
        Arc::clone(&offline.training_dataset) as Arc<dyn TrainingDatasetRepository>,
    ))
}
