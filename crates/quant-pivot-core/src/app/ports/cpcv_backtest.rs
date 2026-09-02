//! Core implementation of [`CpcvBacktestPort`] for the Admin API.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    config::PortfolioSolverDeployConfig,
    domain::{
        api::{BacktestPathSetView, CpcvBacktestJobParams},
        ports::CpcvBacktestPort,
        quant::{
            BacktestPathSetInfo, JobProgressSink, ModelRunInfo, ModelVersionInfo,
            NewBacktestPathSet, NewBacktestPathSetInput, NewModelRun,
        },
    },
    enums::quant::{DatasetPurpose, ModelRunErrorCode, ModelRunKind, ModelRunStatus},
    types::{
        BacktestPathSetId, DecisionPolicySnapshotId, ModelRunId, ModelVersionId, TrainingDatasetId,
        Usd, model_lineage::ModelVersionDerivation, model_metrics::ModelVersionMetricsDefinition,
    },
};
use quant_pivot_repository::traits::{
    BacktestPathSetRepository, CalibrationArtifactRepository, CpcvPathSetCommit,
    ModelRegistryRepository, ModelRunRepository,
};
use quant_pivot_research::artifact::ArtifactStore;
use tokio_util::sync::CancellationToken;

use crate::{
    app::bundles::ResearchBundle,
    service::{
        bias_table_fit::resolve_frozen_bias_table,
        cpcv_backtest::{
            CpcvBacktestConfig, CpcvBacktestInput, CpcvBacktestOutcome, CpcvBacktestService,
            CpcvBacktestServiceDeps, PreparedCpcvRun,
        },
        historical_replay::ReplayConfig,
        model_serving_preimage::{
            ModelPreimageReadContext, ModelServingPreimageService, VerifiedModelServingPreimage,
        },
    },
};

/// Admin port wired from the canonical [`ResearchBundle`].
pub struct CoreCpcvBacktestPort {
    compute: Arc<ComputeExecutor>,
    portfolio_solver: PortfolioSolverDeployConfig,
    artifact_store: Arc<dyn ArtifactStore>,
    path_set_repo: Arc<dyn BacktestPathSetRepository>,
    model_registry_repo: Arc<dyn ModelRegistryRepository>,
    model_run_repo: Arc<dyn ModelRunRepository>,
    bias_table_repo: Arc<dyn CalibrationArtifactRepository>,
    serving_preimages: Arc<ModelServingPreimageService>,
}

/// Explicit dependencies for the canonical CPCV execution adapter.
pub struct CoreCpcvBacktestPortDeps {
    pub compute: Arc<ComputeExecutor>,
    pub portfolio_solver: PortfolioSolverDeployConfig,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub path_set_repo: Arc<dyn BacktestPathSetRepository>,
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    pub model_run_repo: Arc<dyn ModelRunRepository>,
    pub bias_table_repo: Arc<dyn CalibrationArtifactRepository>,
    pub serving_preimages: Arc<ModelServingPreimageService>,
}

struct ResolvedCpcvRun {
    version: ModelVersionInfo,
    service: CpcvBacktestService,
    prepared: PreparedCpcvRun,
}

impl CoreCpcvBacktestPort {
    /// Assemble the port from explicit, already-owned infrastructure.
    #[must_use]
    pub fn new(deps: CoreCpcvBacktestPortDeps) -> Self {
        Self {
            compute: deps.compute,
            portfolio_solver: deps.portfolio_solver,
            artifact_store: deps.artifact_store,
            path_set_repo: deps.path_set_repo,
            model_registry_repo: deps.model_registry_repo,
            model_run_repo: deps.model_run_repo,
            bias_table_repo: deps.bias_table_repo,
            serving_preimages: deps.serving_preimages,
        }
    }

    /// Assemble the port from an already-wired research bundle.
    #[must_use]
    pub fn from_research(research: &ResearchBundle) -> Self {
        Self::new(CoreCpcvBacktestPortDeps {
            compute: Arc::clone(&research.compute),
            portfolio_solver: research.portfolio_solver,
            artifact_store: Arc::clone(&research.artifact_store),
            path_set_repo: Arc::clone(&research.backtest_path_set_repo),
            model_registry_repo: Arc::clone(&research.model_registry_repo),
            model_run_repo: Arc::clone(&research.model_run_repo),
            bias_table_repo: Arc::clone(&research.calibration_artifact_repo),
            serving_preimages: Arc::clone(&research.serving_preimages),
        })
    }

    async fn service_for(
        &self,
        source: &VerifiedModelServingPreimage,
    ) -> QuantResult<CpcvBacktestService> {
        let policy = source.policy_snapshot();
        let runtime = &policy.snapshot;
        let model_family = source
            .artifact()
            .header()
            .serving_contract()
            .bindings()
            .model
            .model_family;
        let bias_table = if model_family.is_classical() {
            None
        } else {
            resolve_frozen_bias_table(
                self.bias_table_repo.as_ref(),
                &runtime.profile_artifacts.scoring.definition,
            )
            .await?
        };
        CpcvBacktestService::new(
            CpcvBacktestServiceDeps {
                compute: Arc::clone(&self.compute),
                artifact_store: Arc::clone(&self.artifact_store),
            },
            CpcvBacktestConfig::from_policy(runtime, model_family)?,
            policy,
            self.portfolio_solver,
            ReplayConfig {
                features: runtime.profile_artifacts.features.definition.clone(),
                factors: runtime.profile_artifacts.scoring.definition.clone(),
                domain: runtime.profile_artifacts.domain.definition.clone(),
                data_quality: runtime.recommendation.data_quality.clone(),
                liquidity_cap_usd: Usd::new(
                    runtime
                        .execution_risk
                        .portfolio
                        .exposure_limits
                        .max_single_recommendation_usd
                        .value,
                ),
                feature_contract: source.profile().spec.feature_contract,
                bias_table,
            },
        )
    }

    /// Read the typed LTR validation trial count from the already-resolved
    /// production model version. Non-LTR versions contribute zero.
    /// Persisted on the path set for audit only — **not** part of DSR N
    /// (Bailey N/V must describe the same trial-grid population).
    const fn coord_search_effective_n(version: &ModelVersionInfo) -> u32 {
        match &version.metrics.definition {
            ModelVersionMetricsDefinition::LearningToRank { validation, .. } => {
                validation.coordinate_search_effective_trials
            }
            ModelVersionMetricsDefinition::ClassicalPointwise { .. }
            | ModelVersionMetricsDefinition::GovernedSellEstimator { .. }
            | ModelVersionMetricsDefinition::NotMeasured { .. } => 0,
        }
    }

    async fn load_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> QuantResult<ModelVersionInfo> {
        self.model_registry_repo
            .find_model_version(model_version_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(StorageError::NotFound {
                    entity: "model_version",
                    id: model_version_id.to_string(),
                })
            })
    }

    async fn resolve_run(
        &self,
        model_version_id: &ModelVersionId,
        training_dataset_id: &TrainingDatasetId,
        decision_policy_snapshot_id: &DecisionPolicySnapshotId,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<ResolvedCpcvRun> {
        let version = self.load_version(model_version_id).await?;
        CpcvBacktestService::validate_family(version.model_family)?;
        let source = Arc::new(self.serving_preimages.load(&version, context).await?);
        let contract = source.artifact().header().serving_contract();
        let bindings = contract.bindings();
        let dataset = source.training_dataset();
        if dataset.training_dataset_id != *training_dataset_id
            || version.training_dataset_id != Some(*training_dataset_id)
            || bindings.dataset.manifest.training_dataset_id != *training_dataset_id
            || source.policy_snapshot().decision_policy_snapshot_id != *decision_policy_snapshot_id
            || bindings.policy_snapshot.decision_policy_snapshot_id != *decision_policy_snapshot_id
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV request subject differs from verified serving graph: model={}, \
                     dataset={training_dataset_id}, policy={decision_policy_snapshot_id}",
                    version.model_version_id,
                ),
            }
            .into());
        }
        Box::pin(self.serving_preimages.verify_replay_dataset(
            &source,
            dataset,
            DatasetPurpose::Training,
            &bindings.policy_snapshot,
            context,
        ))
        .await?;
        let parent = match version.verified_derivation().map_err(|error| {
            ResearchError::InvalidModelArtifact {
                detail: format!("verify CPCV model derivation: {error}"),
            }
        })? {
            ModelVersionDerivation::Training => None,
            ModelVersionDerivation::ReturnCalibration {
                parent_model_version_id,
                ..
            } => {
                let parent_version = self.load_version(&parent_model_version_id).await?;
                let parent_source = self
                    .serving_preimages
                    .load(&parent_version, context)
                    .await?;
                Some((parent_version, parent_source))
            }
        };
        let service = self.service_for(&source).await?;
        let prepared = service.prepare_run(
            &version,
            Arc::clone(&source),
            parent
                .as_ref()
                .map(|(parent_version, parent_source)| (parent_version, parent_source)),
        )?;
        Ok(ResolvedCpcvRun {
            version,
            service,
            prepared,
        })
    }

    async fn create_run(
        &self,
        model_run_id: &ModelRunId,
        resolved: &ResolvedCpcvRun,
    ) -> QuantResult<()> {
        let dataset = resolved.prepared.source().training_dataset();
        let decision_policy_snapshot_id = resolved
            .prepared
            .source()
            .policy_snapshot()
            .decision_policy_snapshot_id;
        self.model_run_repo
            .start_exact(NewModelRun {
                model_run_id: *model_run_id,
                run_kind: ModelRunKind::Cpcv,
                model_version_id: Some(resolved.version.model_version_id),
                decision_policy_snapshot_id,
                market_selection_id: None,
                window_start: dataset.window_start,
                window_end: dataset.window_end,
                input_hash: resolved.prepared.input_hash(),
            })
            .await
            .map_err(QuantError::from)?;
        Ok(())
    }

    async fn persist_path_set(
        &self,
        path_set_id: BacktestPathSetId,
        model_run_id: ModelRunId,
        resolved: &ResolvedCpcvRun,
        outcome: &CpcvBacktestOutcome,
    ) -> QuantResult<BacktestPathSetView> {
        let dataset = resolved.prepared.source().training_dataset();
        let path_count = i64::try_from(outcome.path_set.paths.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("CPCV path count does not fit i64: {error}"),
            }
        })?;
        let combination_count =
            i64::try_from(outcome.path_set.combination_count).map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!("CPCV combination count does not fit i64: {error}"),
                }
            })?;
        let new_path_set = NewBacktestPathSet::try_seal(NewBacktestPathSetInput {
            path_set_id,
            model_version_id: resolved.version.model_version_id,
            model_run_id,
            training_dataset_id: dataset.training_dataset_id,
            decision_policy_snapshot_id: resolved
                .prepared
                .source()
                .policy_snapshot()
                .decision_policy_snapshot_id,
            window_start: outcome.window_start,
            window_end: outcome.window_end,
            subject: resolved.prepared.subject(),
            methodology: resolved.prepared.methodology().clone(),
            fold_artifacts: outcome.fold_artifacts.clone(),
            path_count,
            combination_count,
            median_target_rank_ic: outcome.path_set.median_target_rank_ic,
            sharpe_distribution: outcome.path_set.sharpe_distribution,
            paths: outcome.path_set.paths.clone().into(),
            deflated_sharpe: outcome.dsr.deflated_sharpe,
            dsr_benchmark_sharpe: outcome.dsr.benchmark_sharpe,
            pbo: outcome.cscv_selection_evidence.pbo,
            cscv_selection_evidence: outcome.cscv_selection_evidence.clone(),
            min_track_record_length_secs: outcome
                .min_track_record_length
                .map(|duration| duration.num_seconds()),
            dsr_conservative_independent_trial_count: i64::from(
                outcome.dsr_conservative_independent_trial_count,
            ),
            trial_grid_count: i64::from(outcome.trial_grid_count),
            coord_search_effective_n: i64::from(outcome.coord_search_effective_n),
        })
        .map_err(|error| ResearchError::ValidationMethodology {
            detail: format!("seal CPCV path-set evidence: {error}"),
        })?;
        let info = self
            .path_set_repo
            .commit_cpcv(CpcvPathSetCommit {
                path_set: new_path_set,
                input_hash: resolved.prepared.input_hash(),
            })
            .await
            .map_err(QuantError::from)?;

        // CPCV persists path sets only. A candidate manifest later binds the
        // immutable path-set hash together with the final promotion gate.
        Ok(BacktestPathSetView::from(info))
    }

    async fn execute_cpcv_job(
        &self,
        resolved: ResolvedCpcvRun,
        model_run_id: ModelRunId,
        path_set_id: BacktestPathSetId,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<BacktestPathSetView> {
        let coord_search_effective_n = Self::coord_search_effective_n(&resolved.version);
        let cpcv_input = CpcvBacktestInput::from_prepared(
            &resolved.prepared,
            model_run_id,
            path_set_id,
            coord_search_effective_n,
        );
        self.create_run(&model_run_id, &resolved).await?;

        let outcome = match resolved.service.run(cpcv_input, progress, &cancel).await {
            Ok(outcome) => outcome,
            Err(error) => {
                self.fail_model_run(&model_run_id, &error).await;
                return Err(error);
            }
        };

        let view = match self
            .persist_path_set(path_set_id, model_run_id, &resolved, &outcome)
            .await
        {
            Ok(view) => view,
            Err(error) => {
                self.fail_model_run(&model_run_id, &error).await;
                return Err(error);
            }
        };

        Ok(view)
    }

    async fn fail_model_run(&self, model_run_id: &ModelRunId, error: &QuantError) {
        let _ = self
            .model_run_repo
            .fail(
                model_run_id,
                ModelRunErrorCode::ActiveInferenceFailed,
                error.to_string(),
            )
            .await;
    }

    async fn find_version_path_set(
        &self,
        path_set_id: &BacktestPathSetId,
        resolved: &ResolvedCpcvRun,
    ) -> QuantResult<Option<BacktestPathSetView>> {
        let Some(info) = self
            .path_set_repo
            .find_by_id(path_set_id)
            .await
            .map_err(QuantError::from)?
        else {
            return Ok(None);
        };
        self.verify_path_set(&info, resolved).await?;
        Ok(Some(BacktestPathSetView::from(info)))
    }

    async fn verify_path_set(
        &self,
        info: &BacktestPathSetInfo,
        resolved: &ResolvedCpcvRun,
    ) -> QuantResult<()> {
        info.verify_hash()
            .map_err(|error| ResearchError::ValidationMethodology {
                detail: format!("verify cached CPCV path-set hash: {error}"),
            })?;
        let dataset = resolved.prepared.source().training_dataset();
        let policy_id = resolved
            .prepared
            .source()
            .policy_snapshot()
            .decision_policy_snapshot_id;
        if info.model_version_id != resolved.version.model_version_id
            || info.training_dataset_id != dataset.training_dataset_id
            || info.decision_policy_snapshot_id != policy_id
            || info.window_start != dataset.window_start
            || info.window_end != dataset.window_end
            || info.subject != resolved.prepared.subject()
            || info.methodology != *resolved.prepared.methodology()
            || info.coord_search_effective_n
                != i64::from(Self::coord_search_effective_n(&resolved.version))
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "cached CPCV path set {} differs from its deeply verified \
                     subject/methodology/window",
                    info.path_set_id,
                ),
            }
            .into());
        }
        let run = self
            .model_run_repo
            .find_by_id(&info.model_run_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "model_run",
                id: info.model_run_id.to_string(),
            })?;
        verify_path_set_run(info, resolved, &run)
    }
}

fn verify_path_set_run(
    info: &BacktestPathSetInfo,
    resolved: &ResolvedCpcvRun,
    run: &ModelRunInfo,
) -> QuantResult<()> {
    let finished_at = run
        .finished_at
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: format!(
                "cached CPCV ModelRun {} has no finished_at",
                run.model_run_id
            ),
        })?;
    if run.model_run_id != info.model_run_id
        || run.run_kind != ModelRunKind::Cpcv
        || run.model_version_id != Some(resolved.version.model_version_id)
        || run.decision_policy_snapshot_id != info.decision_policy_snapshot_id
        || run.market_selection_id.is_some()
        || run.window_start != info.window_start
        || run.window_end != info.window_end
        || run.status != ModelRunStatus::Succeeded
        || run.input_hash != resolved.prepared.input_hash()
        || run.output_hash != Some(info.path_set_hash)
        || run.error_code.is_some()
        || run.error_message.is_some()
        || finished_at < run.started_at
    {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "cached CPCV path set {} has no exact successful ModelRun binding",
                info.path_set_id
            ),
        }
        .into());
    }
    Ok(())
}

#[async_trait]
impl CpcvBacktestPort for CoreCpcvBacktestPort {
    async fn run(
        &self,
        params: CpcvBacktestJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<BacktestPathSetView> {
        let CpcvBacktestJobParams {
            model_version_id,
            model_run_id,
            request,
        } = params;
        let context = ModelPreimageReadContext::new(&cancel, None);
        let resolved = Box::pin(self.resolve_run(
            &model_version_id,
            &request.training_dataset_id,
            &request.decision_policy_snapshot_id,
            &context,
        ))
        .await?;
        drop(context);
        if let Some(path_set_id) = &request.path_set_id
            && let Some(view) = self.find_version_path_set(path_set_id, &resolved).await?
        {
            if view.model_run_id != model_run_id {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "cached CPCV path set {path_set_id} belongs to run {}, not exact run {model_run_id}",
                        view.model_run_id
                    ),
                }
                .into());
            }
            return Ok(view);
        }
        let path_set_id = request
            .path_set_id
            .unwrap_or_else(BacktestPathSetId::from_v7);
        Box::pin(self.execute_cpcv_job(resolved, model_run_id, path_set_id, progress, cancel)).await
    }

    async fn find_path_set(
        &self,
        path_set_id: &BacktestPathSetId,
    ) -> QuantResult<Option<BacktestPathSetView>> {
        let Some(info) = self
            .path_set_repo
            .find_by_id(path_set_id)
            .await
            .map_err(QuantError::from)?
        else {
            return Ok(None);
        };
        let context = ModelPreimageReadContext::default();
        let resolved = Box::pin(self.resolve_run(
            &info.model_version_id,
            &info.training_dataset_id,
            &info.decision_policy_snapshot_id,
            &context,
        ))
        .await?;
        drop(context);
        self.verify_path_set(&info, &resolved).await?;
        Ok(Some(BacktestPathSetView::from(info)))
    }

    async fn latest_path_set(
        &self,
        model_version_id: &ModelVersionId,
    ) -> QuantResult<Option<BacktestPathSetView>> {
        let rows = self
            .path_set_repo
            .list_by_model_version(model_version_id)
            .await
            .map_err(QuantError::from)?;
        let Some(info) = rows.into_iter().next() else {
            return Ok(None);
        };
        let context = ModelPreimageReadContext::default();
        let resolved = Box::pin(self.resolve_run(
            model_version_id,
            &info.training_dataset_id,
            &info.decision_policy_snapshot_id,
            &context,
        ))
        .await?;
        drop(context);
        self.verify_path_set(&info, &resolved).await?;
        Ok(Some(BacktestPathSetView::from(info)))
    }
}
