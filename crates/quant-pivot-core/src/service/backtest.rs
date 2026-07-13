//! Offline backtest orchestration (Phase 3.6).
//!
//! Replays a registered model over the exact immutable v2 Parquet rows. Frozen
//! selection context, `FeatureCell`s, factor values, and labels are the sole
//! evaluation input. Historical rematerialization is a separate parity job and
//! can never replace bytes consumed by training, CPCV, or backtest.
//!
//! Per run it persists a `quant_backtest_report` + a `Backtest` `quant_model_run`
//! and optionally fits a calibrated return curve into a fresh Candidate version.

use std::{collections::BTreeMap, sync::Arc};

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
        BacktestReportId, ContentHash, ModelComparisonReportId, ModelRunId, ModelVersionId,
        RuntimeConfigVersionId, TrainingDatasetId, Usd,
    },
};
use quant_pivot_repository::traits::{
    BacktestReportRepository, ModelComparisonReportRepository, ModelRegistryRepository,
    ModelRunRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    backtest::{
        BacktestInputs, BacktestMarketMeta, BacktestReport, BacktestRequest, BacktestRunResult,
        BacktestTick, Backtester, MarketOutcome, ModelComparisonReport, PortfolioCaps,
        PortfolioReplayBacktester, SampleOutcome, compare_reports,
    },
    model::{
        ActiveSchemaBinding, CalibrationSample, ModelArtifact, ModelRuntimeFactoryBuilder,
        QuantModelRuntime, calibrate_weighted_artifact,
    },
    training::{LabelName, TrainingExample},
};

use crate::{
    projection::inference_batch::build_frozen_runtime_input,
    service::training_dataset::{require_dataset_materialization, verify_frozen_dataset_artifact},
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
    bias_table_hash: Option<ContentHash>,
}

impl BacktestService {
    /// Assemble the service from deps + the frozen replay/portfolio config.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error when any frozen portfolio cap is not a
    /// valid decimal; invalid caps never become a zero budget/cap.
    pub fn new(
        deps: BacktestServiceDeps,
        portfolio: &PortfolioConfig,
        bias_table_hash: Option<ContentHash>,
    ) -> QuantResult<Self> {
        Ok(Self {
            deps,
            caps: PortfolioCaps::try_from(portfolio)?,
            bias_table_hash,
        })
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

    /// Run a backtest purely to harvest per-sample `(score, outcome)` pairs for
    /// Phase 11.3 `ProbabilityCalibrator` fitting.
    ///
    /// Persists the same backtest-report / model-run ledger rows as [`Self::run`]
    /// (a full audit trail of the exact PIT replay that produced the
    /// calibration evidence — recomputed features/factors, never trusted from
    /// Parquet, so the calibrator fits the identical computation graph the
    /// live plane scores), and additionally returns the raw per-sample
    /// outcomes `run()` discards.
    pub async fn run_for_calibration(
        &self,
        input: BacktestInput,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<(BacktestReportInfo, Vec<SampleOutcome>)> {
        let version = self.find_version(&input.model_version_id).await?;
        let dataset = self.load_ready_dataset(&input.training_dataset_id).await?;
        let recorded = self
            .run_recorded(&version, &dataset, &input, &progress, &cancel)
            .await?;
        Ok((recorded.info, recorded.result.sample_outcomes))
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
        let materialization = require_dataset_materialization(dataset)?;
        // Load the model under test, bound to the dataset's frozen schema.
        let binding = ActiveSchemaBinding {
            feature_schema_hash: materialization.feature_schema_hash.clone(),
            factor_schema_hash: materialization.factor_schema_hash.clone(),
            bias_table_hash: self.bias_table_hash.clone(),
        };
        let factory = self.deps.factory_builder.build(binding);
        // Backtests are deterministic and never apply a config weight overlay.
        let model = factory.load(version, None).await?;

        // Exact frozen bytes are the evaluation input; no rematerialization is
        // permitted on this path.
        let bytes = self
            .deps
            .artifact_store
            .get(materialization.parquet_uri)
            .await?;
        let examples = verify_frozen_dataset_artifact(dataset, &bytes)?;

        let request = BacktestRequest {
            backtest_report_id: backtest_report_id.clone(),
            model_version_id: version.model_version_id.clone(),
            runtime_config_version_id: input.runtime_config_version_id.clone(),
            window_start: dataset.window_start,
            window_end: dataset.window_end,
        };

        // Offload frozen tick assembly + portfolio replay
        // to a blocking thread so it never occupies an async runtime worker,
        // polling `cancel` at each cross-section boundary.
        let inputs = BacktestReplayInputs {
            model,
            examples,
            caps: self.caps.clone(),
            request,
            model_run_id: model_run_id.clone(),
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
                sharpe: report.sharpe,
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

    /// Tighten the governed score multipliers (data-quality / liquidity /
    /// horizon / substitution penalties) from realized stratified outcomes and
    /// register a fresh Candidate version (weighted models only). A `None` fit
    /// (too little evidence) leaves the candidate untightened — the
    /// conservative governed baseline stays in force. Does **not** touch
    /// `return_model`: the return model is calibrated separately, from an
    /// independent held-out split, via `ProbabilityCalibrator` +
    /// `ModelGovernanceService::bind_calibration` (Phase 11.3 §5) — never as a
    /// same-backtest side effect (that would be a leaked "calibration").
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

        // Fail-closed: only re-register a tightened candidate when there is
        // enough evidence.
        let Some(result) = calibrate_weighted_artifact(&calibration, &weighted) else {
            return Ok(());
        };

        let new_version_id = ModelVersionId::from_v7();
        weighted.header.model_version_id = new_version_id.clone();
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
                trade_policy_artifact_id: calibrated.header().trade_policy_artifact_id.clone(),
                trade_policy_hash: calibrated.header().trade_policy_hash.clone(),
                publish_path_set_id: None,
                metrics_json: serde_json::json!({
                    "calibrated_from": version.model_version_id.to_string(),
                    "calibration": serde_json::to_value(&result.report).unwrap_or_default(),
                }),
                training_objective_json: version.training_objective_json.clone(),
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
        if dataset.status != TrainingDatasetStatus::Ready {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "backtest requires a Ready dataset, got {}",
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
        let materialization = require_dataset_materialization(dataset)?;
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
                input_hash: materialization.dataset_hash.clone(),
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
    examples: Vec<TrainingExample>,
    caps: PortfolioCaps,
    request: BacktestRequest,
    model_run_id: ModelRunId,
    sink: Arc<dyn JobProgressSink>,
    cancel: CancellationToken,
}

/// Run the backtest replay on a blocking thread.
///
/// Frozen tick assembly is synchronous; `block_on` drives only the pure async
/// portfolio replay without occupying an async runtime worker.
/// Returns `None` when cancelled at a cross-section boundary.
fn run_backtest_replay_blocking(
    inputs: BacktestReplayInputs,
) -> QuantResult<Option<BacktestRunResult>> {
    Handle::current().block_on(run_backtest_replay(inputs))
}

/// Assemble exact frozen runtime inputs and run the portfolio replay.
async fn run_backtest_replay(
    inputs: BacktestReplayInputs,
) -> QuantResult<Option<BacktestRunResult>> {
    let Some(ticks) = frozen_ticks(
        &inputs.examples,
        inputs.model.as_ref(),
        &inputs.model_run_id,
        &inputs.cancel,
        inputs.sink.as_ref(),
    )?
    else {
        return Ok(None);
    };

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

/// Assemble replay ticks directly from immutable v2 Parquet examples.
///
/// Shared by single-path backtest and CPCV. No database, current config, or
/// rematerialized feature/factor value participates in the scored input.
///
/// # Errors
///
/// Rejects malformed cross-sections, duplicate market rows, and invalid frozen
/// model-input bindings.
pub(crate) fn frozen_ticks(
    examples: &[TrainingExample],
    model: &dyn QuantModelRuntime,
    model_run_id: &ModelRunId,
    cancel: &CancellationToken,
    sink: &dyn JobProgressSink,
) -> QuantResult<Option<Vec<BacktestTick>>> {
    let mut by_decision: BTreeMap<DateTime<Utc>, Vec<&TrainingExample>> = BTreeMap::new();
    for example in examples {
        by_decision
            .entry(example.decision_at())
            .or_default()
            .push(example);
    }
    let total_sections =
        u64::try_from(by_decision.len()).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("frozen cross-section count does not fit u64: {error}"),
        })?;
    let mut processed_sections: u64 = 0;
    let mut ticks = Vec::with_capacity(by_decision.len());
    let settlement_label = LabelName::new("settlement_outcome");
    let mae_label = LabelName::new("max_adverse_excursion_bps");
    for (decision_at, group) in by_decision {
        if cancel.is_cancelled() {
            return Ok(None);
        }
        processed_sections =
            processed_sections
                .checked_add(1)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: "frozen cross-section progress overflowed u64".to_owned(),
                })?;
        sink.report(ResearchJobProgress::with_total(
            "frozen_input",
            processed_sections,
            total_sections,
        ));
        let model_input = build_frozen_runtime_input(model, model_run_id, &group)?;
        let contexts = model_input
            .market_contexts()
            .into_iter()
            .map(|(market_id, context)| (market_id.clone(), context))
            .collect::<std::collections::HashMap<_, _>>();
        if contexts.is_empty() {
            continue;
        }
        let mut market_meta = Vec::with_capacity(contexts.len());
        let mut outcomes = Vec::with_capacity(contexts.len());
        for example in group {
            let Some(context) = contexts.get(&example.market_id) else {
                continue;
            };
            market_meta.push(BacktestMarketMeta {
                market_id: example.market_id.clone(),
                category: example.selected_market.category,
                event_id: Some(example.selected_market.event_id.clone()),
                liquidity_usd: context.liquidity_usd,
            });
            let (settled_yes, matured) = settlement_outcome(example, &settlement_label);
            outcomes.push(MarketOutcome {
                market_id: example.market_id.clone(),
                settled_yes,
                matured,
                max_adverse_excursion_bps: max_adverse_excursion(example, &mae_label),
            });
        }
        ticks.push(BacktestTick {
            decision_at,
            model_input,
            outcomes,
            market_meta,
        });
    }
    if cancel.is_cancelled() {
        return Ok(None);
    }
    Ok(Some(ticks))
}

fn settlement_outcome(example: &TrainingExample, label: &LabelName) -> (bool, bool) {
    example
        .labels
        .iter()
        .find(|row| row.label_name == *label)
        .map_or((false, false), |row| {
            (row.value >= rust_decimal::Decimal::ONE, row.is_resolved)
        })
}

fn max_adverse_excursion(
    example: &TrainingExample,
    label: &LabelName,
) -> Option<rust_decimal::Decimal> {
    example
        .labels
        .iter()
        .find(|row| row.label_name == *label && row.is_resolved)
        .map(|row| row.value)
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
        substitution_reasons: sample.substitution_reasons.clone(),
    }
}
