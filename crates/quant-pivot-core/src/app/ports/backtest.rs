//! Core implementation of [`BacktestPort`] for the Admin API.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::{BacktestReportView, RunBacktestRequest},
        ports::BacktestPort,
        quant::{JobProgressSink, ModelComparisonReportInfo},
    },
    runtime_config::DecisionPolicySnapshot,
    types::{
        BacktestReportId, Bps, DecisionPolicySnapshotId, ModelComparisonReportId, ModelVersionId,
    },
};
use quant_pivot_repository::traits::{
    BacktestReportRepository, CalibrationArtifactRepository, ModelComparisonReportRepository,
    ModelRegistryRepository, ModelRunRepository, PolicyRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{artifact::ArtifactStore, model::ModelRuntimeFactoryBuilder};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

use crate::{
    app::bundles::ResearchBundle,
    service::{
        backtest::{BacktestInput, BacktestService, BacktestServiceDeps},
        bias_table_fit::resolve_frozen_bias_table,
    },
};

/// Admin port wired from [`ResearchBundle`] plus runtime-config catalog reads.
pub struct CoreBacktestPort {
    compute: Arc<ComputeExecutor>,
    dataset_repo: Arc<dyn TrainingDatasetRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    model_registry_repo: Arc<dyn ModelRegistryRepository>,
    model_run_repo: Arc<dyn ModelRunRepository>,
    backtest_report_repo: Arc<dyn BacktestReportRepository>,
    comparison_report_repo: Arc<dyn ModelComparisonReportRepository>,
    factory_builder: Arc<dyn ModelRuntimeFactoryBuilder>,
    runtime_config: Arc<dyn PolicyRepository>,
    bias_table_repo: Arc<dyn CalibrationArtifactRepository>,
}

impl CoreBacktestPort {
    /// Assemble the port from an already-wired research bundle.
    #[must_use]
    pub fn from_research(
        research: &ResearchBundle,
        runtime_config: Arc<dyn PolicyRepository>,
        bias_table_repo: Arc<dyn CalibrationArtifactRepository>,
    ) -> Self {
        Self {
            compute: Arc::clone(&research.compute),
            dataset_repo: Arc::clone(&research.training_dataset_repo),
            artifact_store: Arc::clone(&research.artifact_store),
            model_registry_repo: Arc::clone(&research.model_registry_repo),
            model_run_repo: Arc::clone(&research.model_run_repo),
            backtest_report_repo: Arc::clone(&research.backtest_report_repo),
            comparison_report_repo: Arc::clone(&research.comparison_report_repo),
            factory_builder: Arc::clone(&research.model_runtime_factory_builder),
            runtime_config,
            bias_table_repo,
        }
    }

    /// Build a fresh [`BacktestService`] bound to a frozen runtime-config
    /// version (bias table, replay config, portfolio caps). `pub` so
    /// [`crate::service::model_calibration_fit::ModelCalibrationFitService`]
    /// can reuse the exact same replay-engine assembly for calibration fits
    /// — one construction path, never duplicated.
    pub async fn backtest_service_for(
        &self,
        decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    ) -> QuantResult<BacktestService> {
        let runtime = self
            .load_runtime_config(decision_policy_snapshot_id)
            .await?;
        let bias_table = resolve_frozen_bias_table(
            self.bias_table_repo.as_ref(),
            &runtime.profile_artifacts.scoring.definition,
        )
        .await?;
        BacktestService::new(
            BacktestServiceDeps {
                compute: Arc::clone(&self.compute),
                dataset_repo: Arc::clone(&self.dataset_repo),
                artifact_store: Arc::clone(&self.artifact_store),
                model_registry_repo: Arc::clone(&self.model_registry_repo),
                model_run_repo: Arc::clone(&self.model_run_repo),
                backtest_report_repo: Arc::clone(&self.backtest_report_repo),
                comparison_report_repo: Arc::clone(&self.comparison_report_repo),
                factory_builder: Arc::clone(&self.factory_builder),
            },
            &runtime.execution_risk.portfolio,
            bias_table.map(|table| table.content_hash),
            Bps::new(Decimal::from(
                runtime.execution_risk.entry_order_policy.max_slippage_bps,
            )),
        )
    }

    async fn load_runtime_config(
        &self,
        decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    ) -> QuantResult<DecisionPolicySnapshot> {
        let version = self
            .runtime_config
            .load_snapshot(decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "decision_policy_snapshot",
                id: decision_policy_snapshot_id.to_string(),
            })?;
        Ok(version.snapshot)
    }
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
        if let Some(backtest_report_id) = &request.backtest_report_id
            && let Some(view) = self.find_report(backtest_report_id).await?
        {
            return Ok(view);
        }
        let service = self
            .backtest_service_for(&request.decision_policy_snapshot_id)
            .await?;
        let input = BacktestInput {
            model_version_id,
            evaluation_dataset_id: request.evaluation_dataset_id,
            decision_policy_snapshot_id: request.decision_policy_snapshot_id,
            backtest_report_id: request.backtest_report_id,
        };
        let result = match request.comparison_model_version_id {
            Some(baseline) => {
                let (info, comparison) = service
                    .run_comparison(input, baseline, Arc::clone(&progress), cancel)
                    .await?;
                BacktestReportView::from_info(info, Some(comparison.comparison_report_id))
            }
            None => BacktestReportView::from(service.run(input, progress, cancel).await?),
        };
        Ok(result)
    }

    async fn find_report(
        &self,
        backtest_report_id: &BacktestReportId,
    ) -> QuantResult<Option<BacktestReportView>> {
        let info = self
            .backtest_report_repo
            .find_by_id(backtest_report_id)
            .await
            .map_err(QuantError::from)?;
        let Some(info) = info else {
            return Ok(None);
        };
        let comparison = self
            .comparison_report_repo
            .find_by_backtest_report(backtest_report_id)
            .await
            .map_err(QuantError::from)?;
        Ok(Some(BacktestReportView::from_info(
            info,
            comparison.map(|row| row.comparison_report_id),
        )))
    }

    async fn backtest_comparison_ids(
        &self,
        backtest_report_ids: &[BacktestReportId],
    ) -> QuantResult<HashMap<BacktestReportId, ModelComparisonReportId>> {
        self.comparison_report_repo
            .backtest_comparison_ids(backtest_report_ids)
            .await
            .map_err(QuantError::from)
    }

    async fn find_comparison_report(
        &self,
        comparison_report_id: &ModelComparisonReportId,
    ) -> QuantResult<Option<ModelComparisonReportInfo>> {
        self.comparison_report_repo
            .find_by_id(comparison_report_id)
            .await
            .map_err(QuantError::from)
    }
}
