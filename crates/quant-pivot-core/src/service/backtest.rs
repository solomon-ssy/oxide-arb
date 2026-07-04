//! Offline backtest orchestration (Phase 3.6).
//!
//! Replays a registered model over a frozen training dataset by **recomputing**
//! each `as_of` cross-section's features and factors point-in-time through the
//! shared [`materialize_cross_section`] kernel — the same computation graph the
//! dataset build and the online plane use. The frozen Parquet supplies only the
//! replay **schedule** (`(as_of, market, token)`) and the forward **settlement
//! truth**; it is never trusted as the factor source, so a backtest validates
//! the exact model the live runtime would run, and config/schema drift is caught
//! rather than masked. No live `BookStore` is ever touched (the window is
//! batch-prefetched from `ClickHouse`).
//!
//! Per run it persists a `quant_backtest_report` + a `Backtest` `quant_model_run`
//! and optionally fits a calibrated return curve into a fresh Candidate version.

use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use tokio::{runtime::Handle, task};
use tokio_util::sync::CancellationToken;

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        BacktestReportInfo, JobProgressSink, ModelComparisonReportInfo, ModelVersionInfo,
        NewBacktestReport, NewModelComparisonReport, NewModelRun, NewModelVersion,
        ResearchJobProgress, TrainingDatasetInfo,
    },
    enums::quant::{
        ModelRunErrorCode, ModelRunKind, ModelRunStatus, PublicationStatus, TrainingDatasetStatus,
    },
    runtime_config::PortfolioConfig,
    types::{
        BacktestReportId, MarketId, ModelComparisonReportId, ModelRunId, ModelVersionId,
        RuntimeConfigVersionId, TrainingDatasetId, Usd,
    },
};
use quant_pivot_repository::traits::{
    BacktestReportRepository, MarketRepository, ModelComparisonReportRepository,
    ModelRegistryRepository, ModelRunRepository, QuantFactReadRepository,
    TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    backtest::{
        BacktestInputs, BacktestMarketMeta, BacktestReport, BacktestRequest, BacktestRunResult,
        BacktestTick, Backtester, MarketOutcome, ModelComparisonReport, PortfolioCaps,
        PortfolioReplayBacktester, SampleOutcome, compare_reports,
    },
    factors::FactorEngine,
    features::ConfiguredFeatureBuilder,
    model::{
        ActiveSchemaBinding, CalibrationSample, ModelArtifact, ModelRuntimeFactoryBuilder,
        QuantModelRuntime, calibrate_weighted_artifact,
    },
    training::DatasetParquetCodec,
};

use crate::{
    pipeline::{
        historical_window::{HistoricalWindow, HistoricalWindowLoader, WindowSpec},
        inference_batch::build_runtime_input,
        inference_context::build_market_inference_context,
    },
    service::{
        dataset_replay::{ReplaySchedule, max_horizon},
        historical_replay::{
            CrossSectionRequest, ReplayConfig, ReplayCrossSection, materialize_cross_section,
        },
    },
};

/// Repository + store + factory dependencies for the backtest service.
pub struct BacktestServiceDeps {
    /// Frozen training-dataset ledger.
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    /// Content-addressed artifact store.
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Model registry (version lookup + calibrated re-registration).
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    /// Model-run ledger.
    pub model_run_repo: Arc<dyn ModelRunRepository>,
    /// Backtest-report ledger.
    pub backtest_report_repo: Arc<dyn BacktestReportRepository>,
    /// Pairwise comparison-report ledger (pair mode).
    pub comparison_report_repo: Arc<dyn ModelComparisonReportRepository>,
    /// Runtime factory builder (loads the model under test).
    pub factory_builder: Arc<dyn ModelRuntimeFactoryBuilder>,
    /// `ClickHouse` fact reader for the point-in-time replay window.
    pub fact_read: Arc<dyn QuantFactReadRepository>,
    /// Postgres market catalog for the replay window.
    pub market_repo: Arc<dyn MarketRepository>,
}

/// A backtest request resolved by the admin port.
pub struct BacktestInput {
    /// Model version under test.
    pub model_version_id: ModelVersionId,
    /// Frozen dataset whose schedule + settlement truth the replay uses.
    pub training_dataset_id: TrainingDatasetId,
    /// Runtime-config version (provenance + portfolio caps).
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Whether to fit a calibrated return curve and register a child candidate.
    pub calibrate: bool,
    /// Pre-assigned candidate report id (async job engine); minted when absent.
    pub backtest_report_id: Option<BacktestReportId>,
}

/// Offline backtest service, bound to one frozen runtime-config snapshot.
pub struct BacktestService {
    deps: BacktestServiceDeps,
    caps: PortfolioCaps,
    replay: ReplayConfig,
    max_book_staleness: Duration,
}

impl BacktestService {
    /// Assemble the service from deps + the frozen replay/portfolio config.
    #[must_use]
    pub fn new(
        deps: BacktestServiceDeps,
        portfolio: &PortfolioConfig,
        replay: ReplayConfig,
        max_book_staleness: Duration,
    ) -> Self {
        Self {
            deps,
            caps: PortfolioCaps::from(portfolio),
            replay,
            max_book_staleness,
        }
    }

    /// Run a single backtest and persist its report.
    pub async fn run(
        &self,
        input: BacktestInput,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<BacktestReportInfo> {
        let version = self.find_version(&input.model_version_id).await?;
        let dataset = self.load_ready_dataset(&input.training_dataset_id).await?;
        Ok(self
            .run_recorded(&version, &dataset, &input, &progress, &cancel)
            .await?
            .info)
    }

    /// Pair mode: replay the candidate (`input.model_version_id`) and the
    /// `baseline_version_id` over the **same** frozen dataset, persist both
    /// reports, and persist the candidate − baseline comparison. Returns the
    /// candidate's report info plus the persisted comparison.
    pub async fn run_comparison(
        &self,
        input: BacktestInput,
        baseline_version_id: ModelVersionId,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<(BacktestReportInfo, ModelComparisonReportInfo)> {
        let candidate_version = self.find_version(&input.model_version_id).await?;
        let baseline_version = self.find_version(&baseline_version_id).await?;
        let dataset = self.load_ready_dataset(&input.training_dataset_id).await?;

        let candidate = self
            .run_recorded(&candidate_version, &dataset, &input, &progress, &cancel)
            .await?;
        let baseline_input = BacktestInput {
            model_version_id: baseline_version_id,
            training_dataset_id: input.training_dataset_id.clone(),
            runtime_config_version_id: input.runtime_config_version_id.clone(),
            calibrate: false,
            backtest_report_id: None,
        };
        let baseline = self
            .run_recorded(
                &baseline_version,
                &dataset,
                &baseline_input,
                &progress,
                &cancel,
            )
            .await?;

        let comparison = compare_reports(&baseline.result, &candidate.result)?;
        let info = self
            .persist_comparison(&candidate, &baseline, &comparison)
            .await?;
        Ok((candidate.info, info))
    }

    /// Resolve a registered model version or fail with a not-found error.
    async fn find_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> QuantResult<ModelVersionInfo> {
        self.deps
            .model_registry_repo
            .find_model_version_by_id(model_version_id)
            .await?
            .ok_or_else(|| {
                StorageError::NotFound {
                    entity: "quant_model_version",
                    id: model_version_id.to_string(),
                }
                .into()
            })
    }

    /// Create the run record, replay, and finalize it (succeed/fail), returning
    /// the report info, the run result (for comparison), and the run id.
    async fn run_recorded(
        &self,
        version: &ModelVersionInfo,
        dataset: &TrainingDatasetInfo,
        input: &BacktestInput,
        progress: &Arc<dyn JobProgressSink>,
        cancel: &CancellationToken,
    ) -> QuantResult<RecordedRun> {
        let model_run_id = ModelRunId::from_v7();
        let backtest_report_id = input
            .backtest_report_id
            .clone()
            .unwrap_or_else(BacktestReportId::from_v7);
        self.create_run(&model_run_id, input, dataset).await?;

        match self
            .run_inner(
                RunInnerCtx {
                    backtest_report_id: &backtest_report_id,
                    model_run_id: &model_run_id,
                    version,
                    dataset,
                    input,
                },
                progress,
                cancel,
            )
            .await
        {
            Ok((info, result)) => {
                self.deps
                    .model_run_repo
                    .succeed(
                        &model_run_id,
                        info.report_hash.clone(),
                        serde_json::json!({ "backtest_report_id": info.backtest_report_id.to_string() }),
                        Utc::now(),
                        Some(version.model_version_id.clone()),
                    )
                    .await?;
                Ok(RecordedRun {
                    model_run_id,
                    info,
                    result,
                })
            }
            Err(error) => {
                let _ = self
                    .deps
                    .model_run_repo
                    .fail(
                        &model_run_id,
                        ModelRunErrorCode::ActiveInferenceFailed,
                        error.to_string(),
                        Utc::now(),
                    )
                    .await;
                Err(error)
            }
        }
    }

    /// Persist the pairwise comparison row (FK to the candidate run + both reports).
    async fn persist_comparison(
        &self,
        candidate: &RecordedRun,
        baseline: &RecordedRun,
        comparison: &ModelComparisonReport,
    ) -> QuantResult<ModelComparisonReportInfo> {
        self.deps
            .comparison_report_repo
            .create(NewModelComparisonReport {
                comparison_report_id: ModelComparisonReportId::from_v7(),
                baseline_model_version_id: baseline.info.model_version_id.clone(),
                candidate_model_version_id: candidate.info.model_version_id.clone(),
                baseline_report_id: baseline.info.backtest_report_id.clone(),
                candidate_report_id: candidate.info.backtest_report_id.clone(),
                model_run_id: candidate.model_run_id.clone(),
                rank_ic_delta: comparison.rank_ic_delta,
                hit_rate_delta: comparison.hit_rate_delta,
                realized_pnl_delta: comparison.realized_pnl_delta,
                score_correlation: comparison.score_correlation,
                side_disagreement_rate: comparison.side_disagreement_rate,
                common_samples: i64::try_from(comparison.common_samples).unwrap_or(i64::MAX),
                category_breakdown_diff: serde_json::to_value(&comparison.category_breakdown_diff)
                    .unwrap_or_default(),
                comparison_hash: comparison.comparison_hash.clone(),
            })
            .await
            .map_err(QuantError::from)
    }

    /// The core PIT replay + persistence (errors finalize the run as failed).
    async fn run_inner(
        &self,
        ctx: RunInnerCtx<'_>,
        progress: &Arc<dyn JobProgressSink>,
        cancel: &CancellationToken,
    ) -> QuantResult<(BacktestReportInfo, BacktestRunResult)> {
        let RunInnerCtx {
            backtest_report_id,
            model_run_id,
            version,
            dataset,
            input,
        } = ctx;
        // Load the model under test, bound to the dataset's frozen schema.
        let binding = ActiveSchemaBinding {
            feature_schema_hash: dataset.feature_schema_hash.clone(),
            factor_schema_hash: dataset.factor_schema_hash.clone(),
        };
        let factory = self.deps.factory_builder.build(binding);
        // Backtests are deterministic and never apply a config weight overlay.
        let model = factory.load(version, None).await?;

        // Prefetch the replay window (real ClickHouse I/O) on the async runtime.
        let bytes = self.deps.artifact_store.get(&dataset.parquet_uri).await?;
        let examples = DatasetParquetCodec::decode(&bytes)?;
        let schedule = ReplaySchedule::from_examples(&examples);
        let source_delay =
            Duration::from_secs(u64::try_from(dataset.source_delay_secs).unwrap_or(0));
        let lookback = Duration::from_secs(self.replay.features.max_lookback_secs());
        let max_horizon_secs = max_horizon(dataset);
        let loader = HistoricalWindowLoader::new(
            Arc::clone(&self.deps.fact_read),
            Arc::clone(&self.deps.market_repo),
            self.max_book_staleness,
        );
        let window = loader
            .load(&WindowSpec {
                window_start: dataset.window_start,
                window_end: dataset.window_end,
                samples: schedule.sample_set.clone(),
                lookback,
                source_delay,
                max_horizon_secs,
            })
            .await?;

        let request = BacktestRequest {
            backtest_report_id: backtest_report_id.clone(),
            model_version_id: version.model_version_id.clone(),
            runtime_config_version_id: input.runtime_config_version_id.clone(),
            window_start: dataset.window_start,
            window_end: dataset.window_end,
        };

        // Offload the CPU-bound per-section factor recompute + portfolio replay
        // to a blocking thread so it never occupies an async runtime worker,
        // polling `cancel` at each cross-section boundary.
        let inputs = BacktestReplayInputs {
            model,
            window,
            schedule,
            replay: self.replay.clone(),
            caps: self.caps.clone(),
            request,
            model_run_id: model_run_id.clone(),
            source_delay,
            lookback,
            sink: Arc::clone(progress),
            cancel: cancel.clone(),
        };
        let result = task::spawn_blocking(move || run_backtest_replay_blocking(inputs))
            .await
            .map_err(|error| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: format!("backtest replay task join failed: {error}"),
                })
            })??;
        let Some(result) = result else {
            return Err(ResearchError::Cancelled {
                detail: "backtest cancelled during the replay".to_owned(),
            }
            .into());
        };

        progress.report(ResearchJobProgress::indeterminate("finalize", 0));
        let info = self.persist_report(model_run_id, &result.report).await?;

        if input.calibrate {
            self.maybe_calibrate(version, dataset, &result.sample_outcomes)
                .await?;
        }
        Ok((info, result))
    }

    /// Persist the backtest report ledger row.
    async fn persist_report(
        &self,
        model_run_id: &ModelRunId,
        report: &BacktestReport,
    ) -> QuantResult<BacktestReportInfo> {
        let info = self
            .deps
            .backtest_report_repo
            .create(NewBacktestReport {
                backtest_report_id: report.backtest_report_id.clone(),
                model_version_id: report.model_version_id.clone(),
                model_run_id: model_run_id.clone(),
                runtime_config_version_id: report.runtime_config_version_id.clone(),
                window_start: report.window_start,
                window_end: report.window_end,
                coverage: report.coverage,
                sample_count: i64::try_from(report.sample_count).unwrap_or(i64::MAX),
                missing_feature_count: i64::try_from(report.missing_feature_count)
                    .unwrap_or(i64::MAX),
                rank_ic: report.rank_ic,
                hit_rate: report.hit_rate,
                expected_vs_realized: serde_json::to_value(&report.expected_vs_realized)
                    .unwrap_or_default(),
                max_drawdown: report.max_drawdown,
                turnover: report.turnover,
                liquidity_feasibility: report.liquidity_feasibility,
                category_breakdown: serde_json::to_value(&report.category_breakdown)
                    .unwrap_or_default(),
                tail_loss: report.tail_loss,
                report_pnl_simulation: serde_json::to_value(&report.report_pnl_simulation)
                    .unwrap_or_default(),
                report_hash: report.report_hash.clone(),
                parquet_uri: None,
            })
            .await?;
        Ok(info)
    }

    /// Calibrate the full governed scoring surface (return curve + data-quality /
    /// liquidity / horizon multipliers + substitution penalties) from realized
    /// stratified outcomes and register a fresh calibrated Candidate version
    /// (weighted models only). A `None` fit (too little evidence) leaves the
    /// candidate uncalibrated — the conservative governed baseline stays in force.
    async fn maybe_calibrate(
        &self,
        version: &ModelVersionInfo,
        dataset: &TrainingDatasetInfo,
        samples: &[SampleOutcome],
    ) -> QuantResult<()> {
        let calibration: Vec<CalibrationSample> = samples.iter().map(calibration_sample).collect();

        let bytes = self
            .deps
            .artifact_store
            .get_by_key(&ModelArtifact::artifact_key(&version.artifact_hash)?)
            .await?;
        let ModelArtifact::WeightedFactor(mut weighted) = ModelArtifact::from_bytes(&bytes)? else {
            return Ok(());
        };

        // Fail-closed: only re-register a calibrated candidate when there is
        // enough evidence to fit a real return curve.
        let Some(result) = calibrate_weighted_artifact(&calibration, &weighted) else {
            return Ok(());
        };

        let new_version_id = ModelVersionId::from_v7();
        weighted.header.model_version_id = new_version_id.clone();
        weighted.return_model = result.return_model;
        weighted.multipliers = result.multipliers;
        weighted.substitution_confidence_rules = result.substitution_rules;
        let calibrated = ModelArtifact::WeightedFactor(weighted);
        calibrated.validate()?;
        let artifact_hash = calibrated.content_hash()?;
        self.deps
            .artifact_store
            .put(
                ModelArtifact::artifact_key(&artifact_hash)?,
                &calibrated.to_bytes()?,
            )
            .await?;

        let next = self
            .deps
            .model_registry_repo
            .next_version_for_spec(&version.model_spec_id)
            .await?;
        self.deps
            .model_registry_repo
            .create_model_version(NewModelVersion {
                model_version_id: new_version_id,
                model_spec_id: version.model_spec_id.clone(),
                version: next,
                artifact_hash,
                training_dataset_id: Some(dataset.training_dataset_id.clone()),
                metrics_json: serde_json::json!({
                    "calibrated_from": version.model_version_id.to_string(),
                    "calibration": serde_json::to_value(&result.report).unwrap_or_default(),
                }),
                quality_gate_report: serde_json::json!({}),
                publication_status: PublicationStatus::Candidate,
                published_at: None,
                retired_at: None,
            })
            .await?;
        Ok(())
    }

    async fn load_ready_dataset(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> QuantResult<TrainingDatasetInfo> {
        let dataset = self
            .deps
            .dataset_repo
            .find_by_id(training_dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: training_dataset_id.to_string(),
            })?;
        if !matches!(
            dataset.status,
            TrainingDatasetStatus::Built | TrainingDatasetStatus::Ready
        ) {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "backtest requires a Built/Ready dataset, got {}",
                    dataset.status.as_str()
                ),
            }
            .into());
        }
        Ok(dataset)
    }

    async fn create_run(
        &self,
        model_run_id: &ModelRunId,
        input: &BacktestInput,
        dataset: &TrainingDatasetInfo,
    ) -> QuantResult<()> {
        self.deps
            .model_run_repo
            .create(NewModelRun {
                model_run_id: model_run_id.clone(),
                run_kind: ModelRunKind::Backtest,
                model_version_id: Some(input.model_version_id.clone()),
                runtime_config_version_id: input.runtime_config_version_id.clone(),
                market_selection_id: None,
                window_start: dataset.window_start,
                window_end: dataset.window_end,
                status: ModelRunStatus::Running,
                input_hash: dataset.dataset_hash.clone(),
                output_hash: None,
                metrics_json: serde_json::json!({}),
                error_code: None,
                error_message: None,
                started_at: Utc::now(),
                finished_at: None,
            })
            .await?;
        Ok(())
    }
}

/// Borrowed inputs for one recorded backtest replay (pre-assigned ids + the
/// resolved model version, frozen dataset, and request).
struct RunInnerCtx<'a> {
    backtest_report_id: &'a BacktestReportId,
    model_run_id: &'a ModelRunId,
    version: &'a ModelVersionInfo,
    dataset: &'a TrainingDatasetInfo,
    input: &'a BacktestInput,
}

/// A completed, recorded single replay: the run id, persisted report info, and
/// the in-memory result (sample outcomes) needed for a pairwise comparison.
struct RecordedRun {
    model_run_id: ModelRunId,
    info: BacktestReportInfo,
    result: BacktestRunResult,
}

/// Owned inputs for the blocking backtest replay (moved into `spawn_blocking`).
struct BacktestReplayInputs {
    model: Box<dyn QuantModelRuntime>,
    window: HistoricalWindow,
    schedule: ReplaySchedule,
    replay: ReplayConfig,
    caps: PortfolioCaps,
    request: BacktestRequest,
    model_run_id: ModelRunId,
    source_delay: Duration,
    lookback: Duration,
    sink: Arc<dyn JobProgressSink>,
    cancel: CancellationToken,
}

/// Run the backtest replay on a blocking thread.
///
/// The per-section materialization is in-memory (`Ready` async), so `block_on`
/// drives it on this blocking thread without occupying an async runtime worker.
/// Returns `None` when cancelled at a cross-section boundary.
fn run_backtest_replay_blocking(
    inputs: BacktestReplayInputs,
) -> QuantResult<Option<BacktestRunResult>> {
    Handle::current().block_on(run_backtest_replay(inputs))
}

/// Recompute every `as_of` cross-section point-in-time, assemble the replay
/// ticks (factor table + forward settlement truth + market metadata), and run
/// the portfolio replay. The schedule + settlement come from the frozen dataset
/// Parquet; the factors are recomputed through the shared kernel (never trusted
/// from Parquet). Polls `cancel` per section for a ~one-section cancel latency.
async fn run_backtest_replay(
    inputs: BacktestReplayInputs,
) -> QuantResult<Option<BacktestRunResult>> {
    let builder = ConfiguredFeatureBuilder::new(&inputs.replay.features);
    let engine = FactorEngine::new(&inputs.replay.factors, &inputs.replay.features);

    let total_sections = inputs.schedule.by_as_of.len() as u64;
    let mut processed_sections: u64 = 0;
    let mut ticks = Vec::with_capacity(inputs.schedule.by_as_of.len());
    for (as_of, group) in &inputs.schedule.by_as_of {
        if inputs.cancel.is_cancelled() {
            return Ok(None);
        }
        processed_sections += 1;
        inputs.sink.report(ResearchJobProgress::with_total(
            "materialize",
            processed_sections,
            total_sections,
        ));
        let Some(cross) = materialize_cross_section(
            &builder,
            &engine,
            &inputs.replay,
            &CrossSectionRequest {
                pit: &inputs.window.pit,
                prefetched: &inputs.window.prefetched,
                as_of: *as_of,
                group,
                source_delay: inputs.source_delay,
                lookback: inputs.lookback,
            },
        )
        .await?
        else {
            continue;
        };
        if let Some(tick) = build_tick(
            inputs.model.as_ref(),
            &inputs.model_run_id,
            *as_of,
            &cross,
            &inputs.schedule.settlement,
        ) {
            ticks.push(tick);
        }
    }
    if inputs.cancel.is_cancelled() {
        return Ok(None);
    }

    inputs.sink.report(ResearchJobProgress::indeterminate(
        "replay",
        ticks.len() as u64,
    ));
    let result = PortfolioReplayBacktester::new()
        .run(BacktestInputs {
            request: inputs.request,
            model: inputs.model.as_ref(),
            ticks,
            caps: inputs.caps,
        })
        .await?;
    Ok(Some(result))
}

/// Assemble one replay tick from a recomputed cross-section + forward settlement.
///
/// The model input is built for the runtime under test's family (factor table or
/// feature matrix) from the same cross-section; the per-market metadata +
/// realized outcomes cover every scoreable market in the cross-section (the
/// runner joins them to emitted candidates by market id).
fn build_tick(
    model: &dyn QuantModelRuntime,
    model_run_id: &ModelRunId,
    as_of: DateTime<Utc>,
    cross: &ReplayCrossSection,
    settlement: &HashMap<(DateTime<Utc>, MarketId), (bool, bool)>,
) -> Option<BacktestTick> {
    let model_input = build_runtime_input(
        model,
        model_run_id,
        as_of,
        &cross.markets,
        &cross.vectors,
        &cross.outcomes,
    );
    if model_input.market_contexts().is_empty() {
        return None;
    }

    let mut market_meta = Vec::new();
    let mut outcomes = Vec::new();
    for (market, vector) in cross.markets.iter().zip(&cross.vectors) {
        let Some(context) = build_market_inference_context(vector, market) else {
            continue;
        };
        market_meta.push(BacktestMarketMeta {
            market_id: market.market_id.clone(),
            category: market.category,
            event_id: Some(market.event_id.clone()),
            liquidity_usd: context.liquidity_usd,
        });
        let (settled_yes, matured) = settlement
            .get(&(as_of, market.market_id.clone()))
            .copied()
            .unwrap_or((false, false));
        outcomes.push(MarketOutcome {
            market_id: market.market_id.clone(),
            settled_yes,
            matured,
        });
    }
    Some(BacktestTick {
        as_of,
        model_input,
        outcomes,
        market_meta,
    })
}

/// Project a resolved backtest outcome into a calibration sample, carrying the
/// **PIT-resolved** scoring strata (data-quality / liquidity / horizon /
/// substitution) the candidate was actually scored under — never assumed.
fn calibration_sample(sample: &SampleOutcome) -> CalibrationSample {
    CalibrationSample {
        composite_score: sample.composite_score.inner(),
        realized_return_bps: sample.realized_return_bps,
        data_quality: sample.data_quality,
        liquidity_usd: sample.liquidity_usd.map(Usd::inner),
        time_to_resolution_secs: sample.time_to_resolution_secs,
        prediction_horizon_secs: sample.prediction_horizon_secs,
        substitution_reasons: sample
            .substitutions
            .iter()
            .map(|audit| audit.reason)
            .collect(),
    }
}
