//! Core implementation of [`BacktestPort`] for the Admin API.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::{BacktestReportView, RunBacktestRequest},
        governance::DecisionPolicySnapshotInfo,
        ports::BacktestPort,
        quant::{BacktestReportInfo, JobProgressSink, ModelComparisonReportInfo},
    },
    enums::quant::{DatasetPurpose, ModelRunKind, ModelRunStatus, TrainingDatasetStatus},
    types::{BacktestReportId, DecisionPolicySnapshotId, ModelComparisonReportId, ModelVersionId},
};
use quant_pivot_repository::traits::{
    BacktestReportRepository, ModelComparisonReportRepository, ModelRegistryRepository,
    ModelRunRepository, PolicyRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    backtest::{BacktestReport, ModelComparisonReport},
};
use tokio_util::sync::CancellationToken;

use crate::{
    app::bundles::ResearchBundle,
    service::{
        backtest::{BacktestInput, BacktestService, BacktestServiceDeps},
        model_serving_preimage::ModelServingPreimageService,
        training_dataset::require_dataset_materialization,
    },
};

/// Repository/store wiring for [`CoreBacktestPort`] tests and non-bundle use.
pub struct CoreBacktestPortDeps {
    pub compute: Arc<ComputeExecutor>,
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    pub model_run_repo: Arc<dyn ModelRunRepository>,
    pub backtest_report_repo: Arc<dyn BacktestReportRepository>,
    pub comparison_report_repo: Arc<dyn ModelComparisonReportRepository>,
    pub runtime_config: Arc<dyn PolicyRepository>,
    pub serving_preimages: Arc<ModelServingPreimageService>,
}

/// Admin port wired from immutable research dependencies.
pub struct CoreBacktestPort {
    deps: CoreBacktestPortDeps,
}

impl CoreBacktestPort {
    /// Assemble the port from explicit production-shaped dependencies.
    #[must_use]
    pub const fn new(deps: CoreBacktestPortDeps) -> Self {
        Self { deps }
    }

    /// Assemble the port from an already-wired research bundle.
    #[must_use]
    pub fn from_research(
        research: &ResearchBundle,
        runtime_config: Arc<dyn PolicyRepository>,
    ) -> Self {
        Self::new(CoreBacktestPortDeps {
            compute: Arc::clone(&research.compute),
            dataset_repo: Arc::clone(&research.training_dataset_repo),
            artifact_store: Arc::clone(&research.artifact_store),
            model_registry_repo: Arc::clone(&research.model_registry_repo),
            model_run_repo: Arc::clone(&research.model_run_repo),
            backtest_report_repo: Arc::clone(&research.backtest_report_repo),
            comparison_report_repo: Arc::clone(&research.comparison_report_repo),
            runtime_config,
            serving_preimages: Arc::clone(&research.serving_preimages),
        })
    }

    /// Build a fresh [`BacktestService`] bound to a frozen runtime-config
    /// version (portfolio caps and execution slippage). Model semantics,
    /// including any bias table, come from the sealed serving contract. `pub` so
    /// [`crate::service::model_calibration_fit::ModelCalibrationFitService`]
    /// can reuse the exact same replay-engine assembly for calibration fits
    /// — one construction path, never duplicated.
    pub async fn backtest_service_for(
        &self,
        decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    ) -> QuantResult<BacktestService> {
        let policy = self.load_policy(decision_policy_snapshot_id).await?;
        BacktestService::new(
            BacktestServiceDeps {
                compute: Arc::clone(&self.deps.compute),
                dataset_repo: Arc::clone(&self.deps.dataset_repo),
                artifact_store: Arc::clone(&self.deps.artifact_store),
                model_registry_repo: Arc::clone(&self.deps.model_registry_repo),
                model_run_repo: Arc::clone(&self.deps.model_run_repo),
                backtest_report_repo: Arc::clone(&self.deps.backtest_report_repo),
                comparison_report_repo: Arc::clone(&self.deps.comparison_report_repo),
                serving_preimages: Arc::clone(&self.deps.serving_preimages),
            },
            &policy,
        )
    }

    async fn load_policy(
        &self,
        decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    ) -> QuantResult<DecisionPolicySnapshotInfo> {
        let version = self
            .deps
            .runtime_config
            .load_snapshot(decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "decision_policy_snapshot",
                id: decision_policy_snapshot_id.to_string(),
            })?;
        Ok(version)
    }

    async fn cached_report(
        &self,
        backtest_report_id: &BacktestReportId,
    ) -> QuantResult<Option<(BacktestReportInfo, Option<ModelComparisonReportInfo>)>> {
        let Some(info) = self
            .deps
            .backtest_report_repo
            .find_by_id(backtest_report_id)
            .await?
        else {
            return Ok(None);
        };
        if info.backtest_report_id != *backtest_report_id {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "backtest report lookup for {backtest_report_id} returned {}",
                    info.backtest_report_id
                ),
            }
            .into());
        }
        let comparison = self
            .deps
            .comparison_report_repo
            .find_by_backtest_report(backtest_report_id)
            .await?;
        if let Some(comparison) = &comparison {
            if comparison.candidate_report_id != info.backtest_report_id
                && comparison.baseline_report_id != info.backtest_report_id
            {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "comparison {} does not contain requested backtest report {}",
                        comparison.comparison_report_id, info.backtest_report_id
                    ),
                }
                .into());
            }
            Box::pin(self.verify_comparison_rows(comparison)).await?;
        } else {
            let service = self
                .backtest_service_for(&info.decision_policy_snapshot_id)
                .await?;
            service
                .verify(&BacktestInput {
                    model_version_id: info.model_version_id,
                    evaluation_dataset_id: info.evaluation_dataset_id,
                    decision_policy_snapshot_id: info.decision_policy_snapshot_id,
                    backtest_report_id: Some(info.backtest_report_id),
                })
                .await?;
        }
        self.verify_report_row(&info).await?;
        Ok(Some((info, comparison)))
    }

    async fn verify_comparison_rows(
        &self,
        comparison: &ModelComparisonReportInfo,
    ) -> QuantResult<()> {
        let baseline = self
            .deps
            .backtest_report_repo
            .find_by_id(&comparison.baseline_report_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "backtest_report",
                id: comparison.baseline_report_id.to_string(),
            })?;
        let candidate = self
            .deps
            .backtest_report_repo
            .find_by_id(&comparison.candidate_report_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "backtest_report",
                id: comparison.candidate_report_id.to_string(),
            })?;
        let service = self
            .backtest_service_for(&candidate.decision_policy_snapshot_id)
            .await?;
        service
            .verify_comparison(
                &BacktestInput {
                    model_version_id: candidate.model_version_id,
                    evaluation_dataset_id: candidate.evaluation_dataset_id,
                    decision_policy_snapshot_id: candidate.decision_policy_snapshot_id,
                    backtest_report_id: Some(candidate.backtest_report_id),
                },
                baseline.model_version_id,
            )
            .await?;
        self.verify_report_row(&baseline).await?;
        self.verify_report_row(&candidate).await?;
        verify_comparison_row(comparison, &baseline, &candidate)
    }

    async fn verify_report_row(&self, info: &BacktestReportInfo) -> QuantResult<()> {
        BacktestReport {
            backtest_report_id: info.backtest_report_id,
            model_version_id: info.model_version_id,
            dataset_id: info.evaluation_dataset_id,
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            window_start: info.window_start,
            window_end: info.window_end,
            coverage: info.coverage,
            sample_count: u64::try_from(info.sample_count).map_err(|error| {
                ResearchError::InvalidModelArtifact {
                    detail: format!("cached backtest sample_count is invalid: {error}"),
                }
            })?,
            missing_feature_count: u64::try_from(info.missing_feature_count).map_err(|error| {
                ResearchError::InvalidModelArtifact {
                    detail: format!("cached backtest missing_feature_count is invalid: {error}"),
                }
            })?,
            rank_ic: info.rank_ic,
            sharpe: info.sharpe,
            hit_rate: info.hit_rate,
            expected_vs_realized: info.expected_vs_realized.clone(),
            max_drawdown: info.max_drawdown,
            turnover: info.turnover,
            liquidity_feasibility: info.liquidity_feasibility,
            category_breakdown: info.category_breakdown.iter().cloned().collect(),
            tail_loss: info.tail_loss,
            report_pnl_simulation: info.report_pnl_simulation.clone(),
            report_hash: info.report_hash,
        }
        .verify_hash()?;
        let dataset = self
            .deps
            .dataset_repo
            .find_by_id(&info.evaluation_dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: info.evaluation_dataset_id.to_string(),
            })?;
        let materialization = require_dataset_materialization(&dataset)?;
        let run = self
            .deps
            .model_run_repo
            .find_by_id(&info.model_run_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "model_run",
                id: info.model_run_id.to_string(),
            })?;
        if dataset.status != TrainingDatasetStatus::Ready
            || dataset.training_dataset_id != info.evaluation_dataset_id
            || dataset.purpose != DatasetPurpose::Evaluation
            || dataset.window_start != info.window_start
            || dataset.window_end != info.window_end
            || run.run_kind != ModelRunKind::Backtest
            || run.model_run_id != info.model_run_id
            || run.model_version_id != Some(info.model_version_id)
            || run.decision_policy_snapshot_id != info.decision_policy_snapshot_id
            || run.market_selection_id.is_some()
            || run.window_start != info.window_start
            || run.window_end != info.window_end
            || run.status != ModelRunStatus::Succeeded
            || run.input_hash != *materialization.dataset_hash
            || run.output_hash != Some(info.report_hash)
            || run.error_code.is_some()
            || run.error_message.is_some()
            || run.finished_at.is_none()
            || run
                .finished_at
                .is_some_and(|finished_at| finished_at < run.started_at)
            || info.parquet_uri.is_some()
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "cached backtest report {} differs from its exact Dataset/model-run subject",
                    info.backtest_report_id
                ),
            }
            .into());
        }
        Ok(())
    }
}

fn verify_comparison_row(
    info: &ModelComparisonReportInfo,
    baseline: &BacktestReportInfo,
    candidate: &BacktestReportInfo,
) -> QuantResult<()> {
    if info.baseline_model_version_id != baseline.model_version_id
        || info.candidate_model_version_id != candidate.model_version_id
        || info.baseline_report_id != baseline.backtest_report_id
        || info.candidate_report_id != candidate.backtest_report_id
        || info.model_run_id != candidate.model_run_id
        || baseline.evaluation_dataset_id != candidate.evaluation_dataset_id
        || baseline.decision_policy_snapshot_id != candidate.decision_policy_snapshot_id
        || baseline.window_start != candidate.window_start
        || baseline.window_end != candidate.window_end
    {
        return Err(ResearchError::InvalidModelArtifact {
            detail: format!(
                "cached comparison {} differs from its exact candidate/baseline subjects",
                info.comparison_report_id
            ),
        }
        .into());
    }
    let comparison = ModelComparisonReport {
        baseline_model_version_id: info.baseline_model_version_id,
        candidate_model_version_id: info.candidate_model_version_id,
        baseline_report_hash: baseline.report_hash,
        candidate_report_hash: candidate.report_hash,
        rank_ic_delta: info.rank_ic_delta,
        hit_rate_delta: info.hit_rate_delta,
        realized_pnl_delta: info.realized_pnl_delta,
        score_correlation: info.score_correlation,
        side_disagreement_rate: info.side_disagreement_rate,
        common_samples: u64::try_from(info.common_samples).map_err(|error| {
            ResearchError::InvalidModelArtifact {
                detail: format!("cached comparison common_samples is invalid: {error}"),
            }
        })?,
        category_breakdown_diff: info.category_breakdown_diff.iter().cloned().collect(),
        comparison_hash: info.comparison_hash,
    };
    comparison.verify_hash()
}

fn verify_cached_subject(
    info: &BacktestReportInfo,
    comparison: Option<&ModelComparisonReportInfo>,
    request: &RunBacktestRequest,
    model_version_id: ModelVersionId,
) -> QuantResult<()> {
    let report_matches = request.backtest_report_id == Some(info.backtest_report_id)
        && info.model_version_id == model_version_id
        && info.evaluation_dataset_id == request.evaluation_dataset_id
        && info.decision_policy_snapshot_id == request.decision_policy_snapshot_id;
    let comparison_matches = match (request.comparison_model_version_id, comparison) {
        (None, None) => true,
        (Some(baseline), Some(comparison)) => {
            comparison.candidate_report_id == info.backtest_report_id
                && comparison.candidate_model_version_id == model_version_id
                && comparison.baseline_model_version_id == baseline
        }
        _ => false,
    };
    if !report_matches || !comparison_matches {
        return Err(ResearchError::InvalidModelArtifact {
            detail: format!(
                "cached backtest report {} belongs to a different request subject",
                info.backtest_report_id
            ),
        }
        .into());
    }
    Ok(())
}

#[async_trait]
impl BacktestPort for CoreBacktestPort {
    async fn run(
        &self,
        model_version_id: ModelVersionId,
        request: RunBacktestRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<BacktestReportView> {
        let service = self
            .backtest_service_for(&request.decision_policy_snapshot_id)
            .await?;
        let input = BacktestInput {
            model_version_id,
            evaluation_dataset_id: request.evaluation_dataset_id,
            decision_policy_snapshot_id: request.decision_policy_snapshot_id,
            backtest_report_id: request.backtest_report_id,
        };
        if let Some(backtest_report_id) = &request.backtest_report_id
            && let Some((info, comparison)) = self.cached_report(backtest_report_id).await?
        {
            verify_cached_subject(&info, comparison.as_ref(), &request, model_version_id)?;
            return Ok(BacktestReportView::from_info(
                info,
                comparison.map(|row| row.comparison_report_id),
            ));
        }
        let result = match request.comparison_model_version_id {
            Some(baseline) => {
                let (info, comparison) = Box::pin(service.run_comparison(
                    input,
                    baseline,
                    Arc::clone(&progress),
                    cancel,
                ))
                .await?;
                BacktestReportView::from_info(info, Some(comparison.comparison_report_id))
            }
            None => BacktestReportView::from(Box::pin(service.run(input, progress, cancel)).await?),
        };
        Ok(result)
    }

    async fn find_report(
        &self,
        backtest_report_id: &BacktestReportId,
    ) -> QuantResult<Option<BacktestReportView>> {
        let Some((info, comparison)) = self.cached_report(backtest_report_id).await? else {
            return Ok(None);
        };
        Ok(Some(BacktestReportView::from_info(
            info,
            comparison.map(|row| row.comparison_report_id),
        )))
    }

    async fn backtest_comparison_ids(
        &self,
        backtest_report_ids: &[BacktestReportId],
    ) -> QuantResult<HashMap<BacktestReportId, ModelComparisonReportId>> {
        let ids = self
            .deps
            .comparison_report_repo
            .backtest_comparison_ids(backtest_report_ids)
            .await
            .map_err(QuantError::from)?;
        let requested = backtest_report_ids.iter().copied().collect::<HashSet<_>>();
        for (backtest_report_id, comparison_report_id) in &ids {
            if !requested.contains(backtest_report_id) {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "comparison batch lookup returned unrequested backtest report \
                         {backtest_report_id}"
                    ),
                }
                .into());
            }
            let info = self
                .deps
                .comparison_report_repo
                .find_by_id(comparison_report_id)
                .await?
                .ok_or_else(|| StorageError::NotFound {
                    entity: "model_comparison_report",
                    id: comparison_report_id.to_string(),
                })?;
            if info.comparison_report_id != *comparison_report_id {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "comparison lookup for {comparison_report_id} returned {}",
                        info.comparison_report_id
                    ),
                }
                .into());
            }
            if info.candidate_report_id != *backtest_report_id
                && info.baseline_report_id != *backtest_report_id
            {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "comparison batch key {backtest_report_id} is not a subject of comparison \
                         {comparison_report_id}"
                    ),
                }
                .into());
            }
            self.verify_comparison_rows(&info).await?;
        }
        Ok(ids)
    }

    async fn find_comparison_report(
        &self,
        comparison_report_id: &ModelComparisonReportId,
    ) -> QuantResult<Option<ModelComparisonReportInfo>> {
        let info = self
            .deps
            .comparison_report_repo
            .find_by_id(comparison_report_id)
            .await
            .map_err(QuantError::from)?;
        if let Some(info) = &info {
            if info.comparison_report_id != *comparison_report_id {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "comparison lookup for {comparison_report_id} returned {}",
                        info.comparison_report_id
                    ),
                }
                .into());
            }
            self.verify_comparison_rows(info).await?;
        }
        Ok(info)
    }
}
