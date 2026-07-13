//! Core implementation of [`CpcvBacktestPort`] for the Admin API (Phase 11.5).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio_util::sync::CancellationToken;

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        BacktestPathSetView, CpcvBacktestPort, JobProgressSink, ModelSpecInfo, ModelVersionInfo,
        NewBacktestPathSet, NewModelRun, RunCpcvBacktestRequest, TrainingDatasetInfo,
    },
    enums::{
        common::MarketCategory,
        quant::{ModelRunErrorCode, ModelRunKind, ModelRunStatus},
    },
    hashing::CanonicalDigest,
    runtime_config::{DecimalString, RuntimeConfig},
    types::{
        BacktestPathSetId, ContentHash, ModelInputContract, ModelRunId, ModelVersionId,
        RuntimeConfigVersionId, TrainingDatasetId,
    },
};
use quant_pivot_repository::traits::{
    BacktestPathSetRepository, CalibrationArtifactRepository, ModelRegistryRepository,
    ModelRunRepository, RuntimeConfigVersionRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    model::{
        LabelSelector, ModelFamily, SellSignalPolicy, TrainingObjectiveSpec,
        load_hash_verified_artifact,
    },
    training::LabelName,
    validation::{
        ClassicalTrialGrid, CpcvConfig, PboInput, PurgeConfig, TrialGridSpec,
        WeightedFactorTrialGrid,
    },
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    app::bundles::ResearchBundle,
    service::{
        bias_table_fit::resolve_frozen_bias_table,
        cpcv_backtest::{
            CpcvBacktestConfig, CpcvBacktestInput, CpcvBacktestOutcome, CpcvBacktestService,
            CpcvBacktestServiceDeps,
        },
        historical_replay::ReplayConfig,
    },
};

/// Repository / store wiring for [`CoreCpcvBacktestPort`] (tests + non-bundle).
pub struct CoreCpcvBacktestPortDeps {
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub path_set_repo: Arc<dyn BacktestPathSetRepository>,
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    pub model_run_repo: Arc<dyn ModelRunRepository>,
    pub runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    pub bias_table_repo: Arc<dyn CalibrationArtifactRepository>,
}

/// Admin port wired from [`ResearchBundle`] plus runtime-config catalog reads.
pub struct CoreCpcvBacktestPort {
    dataset_repo: Arc<dyn TrainingDatasetRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    path_set_repo: Arc<dyn BacktestPathSetRepository>,
    model_registry_repo: Arc<dyn ModelRegistryRepository>,
    model_run_repo: Arc<dyn ModelRunRepository>,
    runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    bias_table_repo: Arc<dyn CalibrationArtifactRepository>,
}

/// Authoritative CPCV training contract resolved from the candidate's immutable
/// registry lineage. No client-owned family, label, or horizon can enter this
/// structure.
struct ResolvedCpcvContract {
    version: ModelVersionInfo,
    dataset: TrainingDatasetInfo,
    label: LabelSelector,
    prediction_horizon_secs: u64,
    input_contract: ModelInputContract,
}

impl CoreCpcvBacktestPort {
    /// Direct constructor for tests and non-bundle wiring.
    #[must_use]
    pub fn new(deps: CoreCpcvBacktestPortDeps) -> Self {
        Self {
            dataset_repo: deps.dataset_repo,
            artifact_store: deps.artifact_store,
            path_set_repo: deps.path_set_repo,
            model_registry_repo: deps.model_registry_repo,
            model_run_repo: deps.model_run_repo,
            runtime_config: deps.runtime_config,
            bias_table_repo: deps.bias_table_repo,
        }
    }

    /// Assemble the port from an already-wired research bundle.
    #[must_use]
    pub fn from_research(
        research: &ResearchBundle,
        runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
        bias_table_repo: Arc<dyn CalibrationArtifactRepository>,
    ) -> Self {
        Self::new(CoreCpcvBacktestPortDeps {
            dataset_repo: Arc::clone(&research.training_dataset_repo),
            artifact_store: Arc::clone(&research.artifact_store),
            path_set_repo: Arc::clone(&research.backtest_path_set_repo),
            model_registry_repo: Arc::clone(&research.model_registry_repo),
            model_run_repo: Arc::clone(&research.model_run_repo),
            runtime_config,
            bias_table_repo,
        })
    }

    async fn service_for(
        &self,
        runtime: &RuntimeConfig,
        model_family: ModelFamily,
    ) -> QuantResult<CpcvBacktestService> {
        let bias_table =
            resolve_frozen_bias_table(self.bias_table_repo.as_ref(), &runtime.factors).await?;
        CpcvBacktestService::new(
            CpcvBacktestServiceDeps {
                dataset_repo: Arc::clone(&self.dataset_repo),
                artifact_store: Arc::clone(&self.artifact_store),
            },
            cpcv_config_from_runtime(runtime, model_family)?,
            &runtime.portfolio,
            ReplayConfig {
                features: runtime.features.clone(),
                factors: runtime.factors.clone(),
                domain: runtime.domain.clone(),
                data_quality: runtime.data_quality.clone(),
                bias_table,
            },
        )
    }

    async fn load_runtime_config(
        &self,
        runtime_config_version_id: &RuntimeConfigVersionId,
    ) -> QuantResult<RuntimeConfig> {
        let version = self
            .runtime_config
            .load_version(runtime_config_version_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "runtime_config_version",
                id: runtime_config_version_id.to_string(),
            })?;
        RuntimeConfig::from_json(&version.config_json).map_err(Into::into)
    }

    /// Read `metrics_json.validation.coord_search_effective_n` from the
    /// already-resolved production model version (`WeightedFactor`). Missing /
    /// classical → 0.
    /// Persisted on the path set for audit only — **not** part of DSR N
    /// (Bailey N/V must describe the same trial-grid population).
    fn coord_search_effective_n(version: &ModelVersionInfo) -> u32 {
        version
            .metrics_json
            .pointer("/validation/coord_search_effective_n")
            .and_then(serde_json::Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
            .unwrap_or(0)
    }

    /// Resolve the trained artifact's `category_scope` so CPCV folds evaluate
    /// the same population the published model was fit on.
    async fn category_scope_for_version(
        &self,
        version: &ModelVersionInfo,
    ) -> QuantResult<Option<MarketCategory>> {
        let artifact = load_hash_verified_artifact(&self.artifact_store, version).await?;
        if artifact.header().model_version_id != version.model_version_id {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV artifact model_version_id {} does not match registry version {}",
                    artifact.header().model_version_id,
                    version.model_version_id
                ),
            }
            .into());
        }
        if artifact.header().model_family != version.model_family {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "CPCV artifact family {} does not match frozen model-spec family {}",
                    artifact.header().model_family,
                    version.model_family
                ),
            }
            .into());
        }
        Ok(artifact.category_scope())
    }

    async fn load_dataset(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> QuantResult<TrainingDatasetInfo> {
        self.dataset_repo
            .find_by_id(training_dataset_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(StorageError::NotFound {
                    entity: "training_dataset",
                    id: training_dataset_id.to_string(),
                })
            })
    }

    async fn create_run(
        &self,
        model_run_id: &ModelRunId,
        model_version_id: &ModelVersionId,
        request: &RunCpcvBacktestRequest,
        dataset: &TrainingDatasetInfo,
    ) -> QuantResult<()> {
        let materialization =
            crate::service::training_dataset::require_dataset_materialization(dataset)?;
        self.model_run_repo
            .create(NewModelRun {
                model_run_id: model_run_id.clone(),
                run_kind: ModelRunKind::Cpcv,
                model_version_id: Some(model_version_id.clone()),
                runtime_config_version_id: request.runtime_config_version_id.clone(),
                market_selection_id: None,
                window_start: dataset.window_start,
                window_end: dataset.window_end,
                status: ModelRunStatus::Running,
                input_hash: materialization.dataset_hash.clone(),
                output_hash: None,
                metrics_json: serde_json::json!({}),
                error_code: None,
                error_message: None,
                started_at: Utc::now(),
                finished_at: None,
            })
            .await
            .map_err(QuantError::from)?;
        Ok(())
    }

    async fn persist_path_set(
        &self,
        path_set_id: BacktestPathSetId,
        model_version_id: ModelVersionId,
        model_run_id: ModelRunId,
        request: &RunCpcvBacktestRequest,
        outcome: &CpcvBacktestOutcome,
    ) -> QuantResult<BacktestPathSetView> {
        let paths_json = serde_json::to_value(&outcome.path_set.paths).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("failed to serialize CPCV paths JSON: {error}"),
            }
        })?;
        let sharpe_distribution_json = serde_json::to_value(outcome.path_set.sharpe_distribution)
            .map_err(|error| ResearchError::ValidationMethodology {
            detail: format!("failed to serialize Sharpe distribution JSON: {error}"),
        })?;
        let path_set_hash = path_set_content_hash(&PathSetHashInput {
            paths: &paths_json,
            sharpe_distribution: &sharpe_distribution_json,
            median_rank_ic: outcome.path_set.median_rank_ic,
            deflated_sharpe: outcome.dsr.deflated_sharpe,
            dsr_benchmark_sharpe: outcome.dsr.benchmark_sharpe,
            pbo: outcome.pbo,
            trial_count: outcome.trial_count,
            trial_grid_count: outcome.trial_grid_count,
            coord_search_effective_n: outcome.coord_search_effective_n,
        })?;
        let info = self
            .path_set_repo
            .create(NewBacktestPathSet {
                path_set_id: path_set_id.clone(),
                model_version_id: model_version_id.clone(),
                model_run_id,
                training_dataset_id: request.training_dataset_id.clone(),
                runtime_config_version_id: request.runtime_config_version_id.clone(),
                window_start: outcome.window_start,
                window_end: outcome.window_end,
                path_count: i64::try_from(outcome.path_set.paths.len()).unwrap_or(i64::MAX),
                combination_count: i64::try_from(outcome.path_set.combination_count)
                    .unwrap_or(i64::MAX),
                median_rank_ic: outcome.path_set.median_rank_ic,
                sharpe_distribution: sharpe_distribution_json,
                paths: paths_json,
                deflated_sharpe: outcome.dsr.deflated_sharpe,
                dsr_benchmark_sharpe: outcome.dsr.benchmark_sharpe,
                pbo: outcome.pbo,
                min_track_record_length_secs: outcome
                    .min_track_record_length
                    .map(|duration| duration.num_seconds()),
                trial_count: i64::from(outcome.trial_count),
                trial_grid_count: i64::from(outcome.trial_grid_count),
                coord_search_effective_n: i64::from(outcome.coord_search_effective_n),
                path_set_hash,
            })
            .await
            .map_err(QuantError::from)?;

        // CPCV persists path sets only; publish binding is explicit via
        // `ModelGovernanceService::bind_publish_path_set` (never silent auto-bind).
        Ok(BacktestPathSetView::from(info))
    }

    async fn resolve_cpcv_contract(
        &self,
        model_version_id: &ModelVersionId,
        request: &RunCpcvBacktestRequest,
    ) -> QuantResult<ResolvedCpcvContract> {
        let version = self
            .model_registry_repo
            .find_model_version_by_id(model_version_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "model_version",
                id: model_version_id.to_string(),
            })?;
        let dataset = self.load_dataset(&request.training_dataset_id).await?;
        validate_cpcv_dataset_binding(&version, &dataset, &request.training_dataset_id)?;
        let model_spec = self
            .model_registry_repo
            .find_model_spec_by_id(&version.model_spec_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "model_spec",
                id: version.model_spec_id.to_string(),
            })?;
        let (label, prediction_horizon_secs, input_contract) =
            validated_cpcv_model_spec(&version, model_spec)?;
        Ok(ResolvedCpcvContract {
            version,
            dataset,
            label,
            prediction_horizon_secs,
            input_contract,
        })
    }

    async fn execute_cpcv_job(
        &self,
        contract: ResolvedCpcvContract,
        request: RunCpcvBacktestRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<BacktestPathSetView> {
        let model_version_id = contract.version.model_version_id.clone();
        let model_family = contract.version.model_family;
        let runtime = self
            .load_runtime_config(&request.runtime_config_version_id)
            .await?;
        let service = self.service_for(&runtime, model_family).await?;

        let path_set_id = request
            .path_set_id
            .clone()
            .unwrap_or_else(BacktestPathSetId::from_v7);
        let category_scope = self.category_scope_for_version(&contract.version).await?;
        let coord_search_effective_n = Self::coord_search_effective_n(&contract.version);

        let model_run_id = ModelRunId::from_v7();
        self.create_run(
            &model_run_id,
            &model_version_id,
            &request,
            &contract.dataset,
        )
        .await?;

        let outcome = match service
            .run(
                CpcvBacktestInput {
                    model_run_id: model_run_id.clone(),
                    training_dataset_id: request.training_dataset_id.clone(),
                    runtime_config_version_id: request.runtime_config_version_id.clone(),
                    label: contract.label,
                    model_family,
                    prediction_horizon_secs: contract.prediction_horizon_secs,
                    category_scope,
                    input_contract: contract.input_contract,
                    path_set_id: Some(path_set_id.clone()),
                    coord_search_effective_n,
                },
                progress.as_ref(),
                &cancel,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                self.fail_model_run(&model_run_id, &error).await;
                return Err(error);
            }
        };

        let view = match self
            .persist_path_set(
                path_set_id.clone(),
                model_version_id.clone(),
                model_run_id.clone(),
                &request,
                &outcome,
            )
            .await
        {
            Ok(view) => view,
            Err(error) => {
                self.fail_model_run(&model_run_id, &error).await;
                return Err(error);
            }
        };

        self.model_run_repo
            .succeed(
                &model_run_id,
                view.path_set_hash.clone(),
                serde_json::json!({
                    "path_set_id": view.path_set_id.to_string(),
                    "trial_count": view.trial_count,
                    "trial_grid_count": view.trial_grid_count,
                    "coord_search_effective_n": view.coord_search_effective_n,
                }),
                Utc::now(),
                Some(model_version_id),
            )
            .await
            .map_err(QuantError::from)?;
        Ok(view)
    }

    async fn fail_model_run(&self, model_run_id: &ModelRunId, error: &QuantError) {
        let _ = self
            .model_run_repo
            .fail(
                model_run_id,
                ModelRunErrorCode::ActiveInferenceFailed,
                error.to_string(),
                Utc::now(),
            )
            .await;
    }

    async fn find_path_set_for_version(
        &self,
        path_set_id: &BacktestPathSetId,
        model_version_id: &ModelVersionId,
        request: &RunCpcvBacktestRequest,
    ) -> QuantResult<Option<BacktestPathSetView>> {
        let Some(view) = self
            .path_set_repo
            .find_by_id(path_set_id)
            .await
            .map(|maybe| maybe.map(BacktestPathSetView::from))
            .map_err(QuantError::from)?
        else {
            return Ok(None);
        };
        if view.model_version_id != *model_version_id {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "path set {path_set_id} belongs to model version {}, not {model_version_id}",
                    view.model_version_id
                ),
            }
            .into());
        }
        if view.training_dataset_id != request.training_dataset_id
            || view.runtime_config_version_id != request.runtime_config_version_id
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "path set {path_set_id} retry binding differs: stored dataset/runtime = {}/{}, requested = {}/{}",
                    view.training_dataset_id,
                    view.runtime_config_version_id,
                    request.training_dataset_id,
                    request.runtime_config_version_id
                ),
            }
            .into());
        }
        Ok(Some(view))
    }
}

/// Canonical hash input for a persisted CPCV path set (audit / replay association).
#[derive(Serialize)]
struct PathSetHashInput<'a> {
    paths: &'a serde_json::Value,
    sharpe_distribution: &'a serde_json::Value,
    median_rank_ic: Decimal,
    deflated_sharpe: Decimal,
    dsr_benchmark_sharpe: Decimal,
    pbo: Decimal,
    trial_count: u32,
    trial_grid_count: u32,
    coord_search_effective_n: u32,
}

fn path_set_content_hash(input: &PathSetHashInput<'_>) -> QuantResult<ContentHash> {
    CanonicalDigest::content_hash_json(input).map_err(|error| {
        QuantError::from(ResearchError::ValidationMethodology {
            detail: format!("failed to hash CPCV path set: {error}"),
        })
    })
}

/// Project the governed `research.validation.*` runtime-config section into
/// the pure research-side config types. The trial grid is family-specific:
/// `WeightedFactor` uses λ × rank-loss; classical uses forest/linear multipliers.
fn cpcv_config_from_runtime(
    runtime: &RuntimeConfig,
    model_family: ModelFamily,
) -> QuantResult<CpcvBacktestConfig> {
    let validation = &runtime.research.validation;
    let decimal = |field: &'static str, value: &str| -> QuantResult<Decimal> {
        value.parse::<Decimal>().map_err(|error| {
            QuantError::from(ResearchError::ValidationMethodology {
                detail: format!("`{field}` = `{value}` is not a valid decimal: {error}"),
            })
        })
    };
    let parse_multipliers = |field: &'static str, values: &[DecimalString]| {
        values
            .iter()
            .map(|value| decimal(field, &value.value))
            .collect::<QuantResult<Vec<_>>>()
    };

    let trials = if model_family.is_classical() {
        TrialGridSpec::Classical(ClassicalTrialGrid {
            forest_n_trees_multipliers: parse_multipliers(
                "research.validation.trials.forest_n_trees_multipliers",
                &validation.trials.forest_n_trees_multipliers,
            )?,
            linear_alpha_multipliers: parse_multipliers(
                "research.validation.trials.linear_alpha_multipliers",
                &validation.trials.linear_alpha_multipliers,
            )?,
            max_trials: validation.trials.max_trials,
        })
    } else {
        TrialGridSpec::WeightedFactor(WeightedFactorTrialGrid {
            lambda_multipliers: parse_multipliers(
                "research.validation.trials.lambda_multipliers",
                &validation.trials.lambda_multipliers,
            )?,
            rank_loss_kinds: validation.trials.rank_loss_kinds.clone(),
            max_trials: validation.trials.max_trials,
        })
    };

    Ok(CpcvBacktestConfig {
        factors: runtime.factors.clone(),
        objective: TrainingObjectiveSpec::from_runtime_config(&runtime.research.training)?,
        cpcv: CpcvConfig {
            n_groups: validation.cpcv.n_groups,
            k_test: validation.cpcv.k_test,
        },
        purge: PurgeConfig {
            embargo_pct: decimal(
                "research.validation.purge.embargo_pct",
                &validation.purge.embargo_pct.value,
            )?,
            min_embargo_secs: runtime.features.max_lookback_secs(),
        },
        trials,
        pbo: PboInput {
            block_count: validation.pbo.block_count,
        },
        dsr_significance: decimal(
            "research.validation.gates.dsr_significance",
            &validation.gates.dsr_significance.value,
        )?,
        // Sell-side lot replay (Phase 11.5.1) fires on the exact same
        // opportunistic-exit thresholds production uses — never a parallel
        // CPCV-only policy that could drift from the live decision rule.
        sell_policy: SellSignalPolicy::try_from_runtime(
            &runtime.execution.exit_monitor.opportunistic_sell,
        )?,
    })
}

fn validate_cpcv_dataset_binding(
    version: &ModelVersionInfo,
    dataset: &TrainingDatasetInfo,
    requested_dataset_id: &TrainingDatasetId,
) -> QuantResult<()> {
    let expected = version.training_dataset_id.as_ref().ok_or_else(|| {
        ResearchError::ValidationMethodology {
            detail: format!(
                "model version {} has no linked training_dataset_id; CPCV requires the same \
                 frozen dataset the model was trained on",
                version.model_version_id
            ),
        }
    })?;
    if expected != requested_dataset_id {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "CPCV training_dataset_id {requested_dataset_id} does not match model version linked dataset {expected}"
            ),
        }
        .into());
    }
    if dataset.model_spec_id != version.model_spec_id {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "CPCV dataset {} belongs to model spec {}, but model version {} belongs to {}",
                dataset.training_dataset_id,
                dataset.model_spec_id,
                version.model_version_id,
                version.model_spec_id
            ),
        }
        .into());
    }
    Ok(())
}

fn validated_cpcv_model_spec(
    version: &ModelVersionInfo,
    model_spec: ModelSpecInfo,
) -> QuantResult<(LabelSelector, u64, ModelInputContract)> {
    if model_spec.model_family != version.model_family {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "CPCV registry family {} does not match model spec {} family {}",
                version.model_family, model_spec.model_spec_id, model_spec.model_family
            ),
        }
        .into());
    }
    model_spec.training_contract.validate().map_err(|detail| {
        ResearchError::ValidationMethodology {
            detail: format!(
                "model spec {} carries invalid training_contract: {detail}",
                model_spec.model_spec_id
            ),
        }
    })?;
    model_spec.input_contract.validate().map_err(|detail| {
        ResearchError::ValidationMethodology {
            detail: format!(
                "model spec {} carries invalid input_contract: {detail}",
                model_spec.model_spec_id
            ),
        }
    })?;
    if model_spec.input_contract.inputs.is_empty() {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "model spec {} input_contract must contain at least one raw feature",
                model_spec.model_spec_id
            ),
        }
        .into());
    }
    let prediction_horizon_secs =
        u64::try_from(model_spec.prediction_horizon_secs).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!(
                    "model spec {} prediction_horizon_secs is invalid: {error}",
                    model_spec.model_spec_id
                ),
            }
        })?;
    if prediction_horizon_secs == 0 {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "model spec {} prediction_horizon_secs must be positive",
                model_spec.model_spec_id
            ),
        }
        .into());
    }
    Ok((
        LabelSelector {
            name: LabelName::new(model_spec.training_contract.target_label_name),
            horizon_secs: model_spec.training_contract.target_label_horizon_secs,
        },
        prediction_horizon_secs,
        model_spec.input_contract,
    ))
}

#[async_trait]
impl CpcvBacktestPort for CoreCpcvBacktestPort {
    async fn run(
        &self,
        model_version_id: ModelVersionId,
        request: RunCpcvBacktestRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<BacktestPathSetView> {
        if let Some(path_set_id) = &request.path_set_id
            && let Some(view) = self
                .find_path_set_for_version(path_set_id, &model_version_id, &request)
                .await?
        {
            return Ok(view);
        }
        let contract = self
            .resolve_cpcv_contract(&model_version_id, &request)
            .await?;
        self.execute_cpcv_job(contract, request, progress, cancel)
            .await
    }

    async fn find_path_set(
        &self,
        path_set_id: &BacktestPathSetId,
    ) -> QuantResult<Option<BacktestPathSetView>> {
        self.path_set_repo
            .find_by_id(path_set_id)
            .await
            .map(|maybe| maybe.map(BacktestPathSetView::from))
            .map_err(QuantError::from)
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
        Ok(rows.into_iter().next().map(BacktestPathSetView::from))
    }
}
