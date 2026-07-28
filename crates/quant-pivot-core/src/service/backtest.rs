//! Offline backtest orchestration.
//!
//! Replays a registered model over exact immutable v3 Parquet rows plus their
//! Dataset-bound Source Slice execution facts. Frozen
//! selection context, `FeatureCell`s, factor values, and labels are the sole
//! evaluation input. Historical rematerialization is a separate parity job and
//! can never replace bytes consumed by training, CPCV, or backtest.
//!
//! Per run it persists a `quant_backtest_report` + a `Backtest` `quant_model_run`.
//! Evaluation is read-only with respect to model artifacts.

use std::{
    collections::{BTreeMap, HashMap},
    iter,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
use quant_pivot_error::{
    QuantError, QuantResult, feedback::FeedbackError, research::ResearchError,
    storage::StorageError,
};
use quant_pivot_models::{
    domain::{
        governance::DecisionPolicySnapshotInfo,
        ports::{FeedbackComparisonCandidateRef, FeedbackComparisonJobParams},
        quant::{
            BacktestReportInfo, JobProgressSink, ModelComparisonReportInfo, ModelVersionInfo,
            NewBacktestReport, NewModelComparisonReport, NewModelRun, TrainingDatasetInfo,
        },
        query::TimeWindow,
    },
    enums::quant::{DatasetPurpose, ModelRunErrorCode, ModelRunKind},
    hashing::CanonicalDigest,
    types::{
        BacktestReportId, Bps, ContentHash, DecisionPolicySnapshotId, MarketId,
        ModelComparisonReportId, ModelRunId, ModelVersionId, PayoutRatio, ResearchJobProgress,
        ResearchProfileArtifact, TokenId, TrainingDatasetId,
        calibration::ModelScoreCalibrationFitContract,
        model_serving::ModelServingPolicySnapshotBinding,
    },
};
use quant_pivot_repository::traits::{
    BacktestReportRepository, ModelComparisonReportRepository, ModelRegistryRepository,
    ModelRunRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    backtest::{
        BacktestExecutionSnapshot, BacktestInputs, BacktestMarketMeta, BacktestReport,
        BacktestRequest, BacktestRunResult, BacktestTick, Backtester, MarketOutcome,
        ModelComparisonReport, PortfolioCaps, PortfolioReplayBacktester,
        PortfolioReturnObservation, SampleOutcome,
    },
    execution_semantics::{PitFeeSchedule, aggressive_buy_limit},
    model::QuantModelRuntime,
    training::{LabelName, TrainingExample},
};
use rust_decimal::Decimal;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use crate::{
    governance::policy_snapshot::VerifiedPolicySnapshotBinding,
    prefetch::{
        replay_page::{MAX_REPLAY_PAGE_MARKETS, ReplayPage, ReplayPageRequest},
        source_slice::{FrozenSourceSlice, SourceSliceReader},
    },
    projection::inference_batch::build_frozen_runtime_input,
    service::{
        model_serving_preimage::{
            ModelServingPreimageService, VerifiedModelServingPreimage, VerifiedReplayDataset,
        },
        training_dataset::{require_dataset_materialization, verify_frozen_dataset_artifact},
    },
};

/// Repository, store, and verified-preimage dependencies for the backtest service.
pub struct BacktestServiceDeps {
    /// Process-wide offline CPU and memory governor.
    pub compute: Arc<ComputeExecutor>,
    /// Frozen training-dataset ledger.
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    /// Content-addressed artifact store.
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Model registry (version lookup).
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    /// Model-run ledger.
    pub model_run_repo: Arc<dyn ModelRunRepository>,
    /// Backtest-report ledger.
    pub backtest_report_repo: Arc<dyn BacktestReportRepository>,
    /// Pairwise comparison-report ledger (pair mode).
    pub comparison_report_repo: Arc<dyn ModelComparisonReportRepository>,
    /// Canonical full-graph model and replay-Dataset preimage verifier.
    pub serving_preimages: Arc<ModelServingPreimageService>,
}

/// A backtest request resolved by the admin port.
pub struct BacktestInput {
    /// Model version under test.
    pub model_version_id: ModelVersionId,
    /// Frozen reusable holdout whose schedule + settlement truth the replay uses.
    pub evaluation_dataset_id: TrainingDatasetId,
    /// Frozen decision-policy snapshot (provenance + portfolio caps).
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    /// Pre-assigned candidate report id (async job engine); minted when absent.
    pub backtest_report_id: Option<BacktestReportId>,
}

/// One model's deterministic, non-persisted replay inside a feedback
/// comparison job.
pub struct FeedbackModelReplay {
    pub model_version_id: ModelVersionId,
    pub serving_contract_hash: ContentHash,
    pub report_hash: ContentHash,
    pub portfolio_returns: Vec<PortfolioReturnObservation>,
}

/// Shared-object replay result for the champion and all challengers.
pub struct FeedbackFamilyReplay {
    pub champion: FeedbackModelReplay,
    pub candidates: Vec<FeedbackModelReplay>,
}

/// Internal replay request for model-score calibration evidence.
///
/// Calibration reuses the deterministic replay computation but does not write
/// a `quant_backtest_report`: that ledger is reserved for reusable evaluation
/// evidence and its `evaluation_dataset_id` invariant.
pub(crate) struct CalibrationReplayInput {
    /// Pre-assigned run id frozen in the durable calibration job.
    pub model_run_id: ModelRunId,
    /// Model version whose scores are being calibrated.
    pub source_model: ModelVersionId,
    /// Independent purpose-bound calibration dataset.
    pub calibration_dataset: TrainingDatasetId,
    /// Frozen decision-policy snapshot governing the replay.
    pub policy_snapshot: DecisionPolicySnapshotId,
}

/// Exact samples and immutable provenance produced by one calibration replay.
pub(crate) struct CalibrationReplayEvidence {
    /// Running ledger row whose terminal state is owned by the calibrator
    /// producer after fit and artifact persistence finish.
    pub model_run_id: ModelRunId,
    /// Per-sample score/outcome evidence.
    pub samples: Vec<SampleOutcome>,
    /// Exact catalog window of the deeply verified Calibration Dataset.
    pub fit_window: TimeWindow,
    /// Canonical model, Dataset/Source Slice, and policy preimages.
    pub fit_contract: ModelScoreCalibrationFitContract,
}

/// Offline backtest service, bound to one frozen runtime-config snapshot.
pub struct BacktestService {
    deps: BacktestServiceDeps,
    policy_binding: ModelServingPolicySnapshotBinding,
    caps: PortfolioCaps,
    entry_max_slippage_bps: Bps,
}

impl BacktestService {
    /// Assemble the service from dependencies and one canonical frozen policy
    /// snapshot.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error when the snapshot identity, revision
    /// projections, four profile artifacts, or portfolio caps are invalid.
    pub fn new(
        deps: BacktestServiceDeps,
        policy: &DecisionPolicySnapshotInfo,
    ) -> QuantResult<Self> {
        let policy_binding = ModelServingPolicySnapshotBinding::from(
            VerifiedPolicySnapshotBinding::try_from(policy)?,
        );
        let caps = PortfolioCaps::try_from(&policy.snapshot.execution_risk.portfolio)?;
        let entry_max_slippage_bps = Bps::new(Decimal::from(
            policy
                .snapshot
                .execution_risk
                .entry_order_policy
                .max_slippage_bps,
        ));
        Ok(Self {
            deps,
            policy_binding,
            caps,
            entry_max_slippage_bps,
        })
    }

    /// Run a single backtest and persist its report.
    pub async fn run(
        &self,
        input: BacktestInput,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<BacktestReportInfo> {
        let prepared = Box::pin(self.prepare(
            &input.model_version_id,
            &input.evaluation_dataset_id,
            input.decision_policy_snapshot_id,
            DatasetPurpose::Evaluation,
        ))
        .await?;
        Ok(
            Box::pin(self.run_recorded(prepared, &input, &progress, &cancel))
                .await?
                .info,
        )
    }

    /// Replay a complete reserved candidate family after loading the Evaluation
    /// Dataset Parquet and Source Slice exactly once.
    pub async fn replay_feedback_family(
        &self,
        params: &FeedbackComparisonJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackFamilyReplay> {
        params.validate()?;
        if params.decision_policy_snapshot_id != self.policy_binding.decision_policy_snapshot_id {
            return Err(Self::comparison_error(
                "comparison policy differs from the bound BacktestService",
            ));
        }
        let dataset = self
            .find_dataset(&params.evaluation_use.evaluation_dataset_id)
            .await?;
        Self::verify_reserved_dataset(params, &dataset)?;

        let champion_version = self.find_version(&params.champion_model_version_id).await?;
        let champion_source = self.deps.serving_preimages.load(&champion_version).await?;
        Self::verify_comparison_model(
            &champion_version,
            params.champion_model_version_id,
            params.champion_serving_contract_hash,
        )?;
        let replay = Box::pin(self.deps.serving_preimages.verify_replay_dataset(
            &champion_source,
            &dataset,
            DatasetPurpose::Evaluation,
        ))
        .await?;
        let (examples, frozen_source) = self
            .load_replay_artifacts(&replay, champion_source.profile())
            .await?;

        let mut prepared = Vec::with_capacity(params.candidates.len() + 1);
        prepared.push(FeedbackPreparedModel::champion(params, &champion_source)?);
        for candidate in &params.candidates {
            let version = self.find_version(&candidate.model_version_id).await?;
            let source = self.deps.serving_preimages.load(&version).await?;
            Self::verify_comparison_model(
                &version,
                candidate.model_version_id,
                candidate.serving_contract_hash,
            )?;
            Box::pin(self.deps.serving_preimages.verify_replay_bindings(
                &source,
                &dataset,
                DatasetPurpose::Evaluation,
            ))
            .await?;
            prepared.push(FeedbackPreparedModel::candidate(candidate, &source)?);
        }

        let batch = FeedbackReplayBatch {
            prepared,
            examples,
            frozen_source,
            caps: self.caps.clone(),
            entry_max_slippage_bps: self.entry_max_slippage_bps,
            evaluation_dataset_id: dataset.training_dataset_id,
            decision_policy_snapshot_id: params.decision_policy_snapshot_id,
            window_start: dataset.window_start,
            window_end: dataset.window_end,
            progress,
            cancel: cancel.clone(),
        };
        let runtime = Handle::current();
        let replayed = self
            .deps
            .compute
            .run_offline_cancellable(OfflineMemory::try_gib(4)?, &cancel, move || {
                let _runtime = runtime.enter();
                batch.run_blocking()
            })
            .await?;
        replayed.ok_or_else(|| {
            ResearchError::Cancelled {
                detail: "feedback comparison cancelled during shared replay".to_owned(),
            }
            .into()
        })
    }

    /// Verify every canonical replay preimage without creating a run, report,
    /// comparison, or other evidence row.
    pub async fn verify(&self, input: &BacktestInput) -> QuantResult<()> {
        let prepared = Box::pin(self.prepare(
            &input.model_version_id,
            &input.evaluation_dataset_id,
            input.decision_policy_snapshot_id,
            DatasetPurpose::Evaluation,
        ))
        .await?;
        drop(prepared);
        Ok(())
    }

    /// Verify both candidate and baseline before a cached comparison can be
    /// returned or either model run can be created.
    pub async fn verify_comparison(
        &self,
        input: &BacktestInput,
        baseline_version_id: ModelVersionId,
    ) -> QuantResult<()> {
        let candidate = Box::pin(self.prepare(
            &input.model_version_id,
            &input.evaluation_dataset_id,
            input.decision_policy_snapshot_id,
            DatasetPurpose::Evaluation,
        ))
        .await?;
        let baseline = Box::pin(self.prepare(
            &baseline_version_id,
            &input.evaluation_dataset_id,
            input.decision_policy_snapshot_id,
            DatasetPurpose::Evaluation,
        ))
        .await?;
        drop((candidate, baseline));
        Ok(())
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
        // Both exact contracts are resolved before the first candidate run is
        // created. A bad baseline can therefore never leave half a pair.
        let candidate_prepared = Box::pin(self.prepare(
            &input.model_version_id,
            &input.evaluation_dataset_id,
            input.decision_policy_snapshot_id,
            DatasetPurpose::Evaluation,
        ))
        .await?;
        let baseline_prepared = Box::pin(self.prepare(
            &baseline_version_id,
            &input.evaluation_dataset_id,
            input.decision_policy_snapshot_id,
            DatasetPurpose::Evaluation,
        ))
        .await?;

        let candidate =
            Box::pin(self.run_recorded(candidate_prepared, &input, &progress, &cancel)).await?;
        let baseline_input = BacktestInput {
            model_version_id: baseline_version_id,
            evaluation_dataset_id: input.evaluation_dataset_id,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            backtest_report_id: None,
        };
        let baseline =
            Box::pin(self.run_recorded(baseline_prepared, &baseline_input, &progress, &cancel))
                .await?;

        let comparison = baseline.result.compare(&candidate.result)?;
        let info = self
            .persist_comparison(&candidate, &baseline, &comparison)
            .await?;
        Ok((candidate.info, info))
    }

    /// Replay a calibration dataset to harvest per-sample `(score, outcome)`
    /// pairs for `ProbabilityCalibrator` fitting.
    ///
    /// The replay writes a purpose-specific `Calibration` model run. Replay
    /// failure marks it failed; replay success leaves it `Running` so the sole
    /// calibrator producer can bind the terminal output to the persisted
    /// calibration artifact. It does not write an evaluation backtest report.
    pub(crate) async fn run_for_calibration(
        &self,
        input: CalibrationReplayInput,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<CalibrationReplayEvidence> {
        let prepared = Box::pin(self.prepare(
            &input.source_model,
            &input.calibration_dataset,
            input.policy_snapshot,
            DatasetPurpose::Calibration,
        ))
        .await?;
        let fit_contract = prepared.calibration_contract.clone().ok_or_else(|| {
            ResearchError::InvalidModelArtifact {
                detail: "calibration replay did not resolve its exact fit contract".to_owned(),
            }
        })?;
        let fit_window =
            TimeWindow::new(prepared.dataset.window_start, prepared.dataset.window_end);
        let model_run_id = input.model_run_id;
        self.create_run(
            &model_run_id,
            input.source_model,
            input.policy_snapshot,
            &prepared.dataset,
            ModelRunKind::Calibration,
        )
        .await?;

        let replay = Box::pin(self.replay(
            ReplayCtx {
                backtest_report_id: BacktestReportId::from_v7(),
                model_run_id,
                prepared,
                decision_policy_snapshot_id: input.policy_snapshot,
            },
            &progress,
            &cancel,
        ))
        .await;
        match replay {
            Ok(result) => Ok(CalibrationReplayEvidence {
                model_run_id,
                samples: result.sample_outcomes,
                fit_window,
                fit_contract,
            }),
            Err(error) => {
                let _ = self
                    .deps
                    .model_run_repo
                    .fail(
                        &model_run_id,
                        ModelRunErrorCode::CalibrationFailed,
                        error.to_string(),
                    )
                    .await;
                Err(error)
            }
        }
    }

    /// Resolve a registered model version or fail with a not-found error.
    async fn find_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> QuantResult<ModelVersionInfo> {
        self.deps
            .model_registry_repo
            .find_model_version(model_version_id)
            .await?
            .ok_or_else(|| {
                StorageError::NotFound {
                    entity: "quant_model_version",
                    id: model_version_id.to_string(),
                }
                .into()
            })
    }

    /// Resolve and verify every immutable preimage, then load the runtime.
    /// Nothing in this path creates evidence or mutates a repository.
    async fn prepare(
        &self,
        model_version_id: &ModelVersionId,
        dataset_id: &TrainingDatasetId,
        policy_id: DecisionPolicySnapshotId,
        purpose: DatasetPurpose,
    ) -> QuantResult<PreparedReplay> {
        let version = self.find_version(model_version_id).await?;
        let source = self.deps.serving_preimages.load(&version).await?;
        let source_policy = &source
            .artifact()
            .header()
            .serving_contract()
            .bindings()
            .policy_snapshot;
        if policy_id != self.policy_binding.decision_policy_snapshot_id
            || source_policy != &self.policy_binding
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "backtest policy snapshot {policy_id} differs from exact model source {}",
                    self.policy_binding.decision_policy_snapshot_id
                ),
            }
            .into());
        }
        let dataset = self.find_dataset(dataset_id).await?;
        let replay = Box::pin(
            self.deps
                .serving_preimages
                .verify_replay_dataset(&source, &dataset, purpose),
        )
        .await?;
        let calibration_contract = if purpose == DatasetPurpose::Calibration {
            Some(
                Box::pin(
                    self.deps
                        .serving_preimages
                        .calibration_fit_contract(&source, &dataset),
                )
                .await?,
            )
        } else {
            None
        };
        let (examples, frozen_source) = self
            .load_replay_artifacts(&replay, source.profile())
            .await?;

        // Runtime construction is intentionally last and consumes only the
        // already verified source retained above.
        let model = source.buy_runtime()?;
        Ok(PreparedReplay {
            version,
            dataset,
            model,
            examples,
            frozen_source,
            calibration_contract,
        })
    }

    async fn load_replay_artifacts(
        &self,
        replay: &VerifiedReplayDataset<'_>,
        profile: &ResearchProfileArtifact,
    ) -> QuantResult<(Vec<TrainingExample>, FrozenSourceSlice)> {
        let dataset = replay.dataset();
        let materialization = replay.materialization();
        let bytes = self
            .deps
            .artifact_store
            .get(materialization.parquet_uri)
            .await?;
        let examples = verify_frozen_dataset_artifact(dataset, &bytes)?;
        let frozen = SourceSliceReader::new(Arc::clone(&self.deps.artifact_store))
            .read_ref(&dataset.source_lineage.source_slice)
            .await?;
        dataset
            .source_lineage
            .verify_manifest(&frozen.manifest)
            .map_err(|error| ResearchError::InvalidModelArtifact {
                detail: format!(
                    "Dataset source lineage differs from the verified Source Slice: {error}"
                ),
            })?;
        frozen
            .manifest
            .validate_for_profile(
                profile,
                &dataset.source_lineage.research_program_hash,
                dataset.window_start,
                dataset.window_end,
                dataset.pit_cutoff,
            )
            .map_err(|detail| ResearchError::InvalidModelArtifact {
                detail: format!("Source Slice profile/PIT preimage failed: {detail}"),
            })?;
        Ok((examples, frozen))
    }

    /// Create the run record, replay, and finalize it (succeed/fail), returning
    /// the report info, the run result (for comparison), and the run id.
    async fn run_recorded(
        &self,
        prepared: PreparedReplay,
        input: &BacktestInput,
        progress: &Arc<dyn JobProgressSink>,
        cancel: &CancellationToken,
    ) -> QuantResult<RecordedRun> {
        let model_run_id = ModelRunId::from_v7();
        let backtest_report_id = input
            .backtest_report_id
            .unwrap_or_else(BacktestReportId::from_v7);
        self.create_run(
            &model_run_id,
            input.model_version_id,
            input.decision_policy_snapshot_id,
            &prepared.dataset,
            ModelRunKind::Backtest,
        )
        .await?;

        match Box::pin(self.run_inner(
            RunInnerCtx {
                backtest_report_id,
                model_run_id,
                prepared,
                input,
            },
            progress,
            cancel,
        ))
        .await
        {
            Ok((info, result)) => {
                self.deps
                    .model_run_repo
                    .succeed(
                        &model_run_id,
                        info.report_hash,
                        Some(input.model_version_id),
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
        let common_samples = i64::try_from(comparison.common_samples).map_err(|_| {
            ResearchError::EvidenceCountOverflow {
                field: "comparison.common_samples",
                value: comparison.common_samples,
            }
        })?;
        self.deps
            .comparison_report_repo
            .create(NewModelComparisonReport {
                comparison_report_id: ModelComparisonReportId::from_v7(),
                baseline_model_version_id: baseline.info.model_version_id,
                candidate_model_version_id: candidate.info.model_version_id,
                baseline_report_id: baseline.info.backtest_report_id,
                candidate_report_id: candidate.info.backtest_report_id,
                model_run_id: candidate.model_run_id,
                rank_ic_delta: comparison.rank_ic_delta,
                hit_rate_delta: comparison.hit_rate_delta,
                realized_pnl_delta: comparison.realized_pnl_delta,
                score_correlation: comparison.score_correlation,
                side_disagreement_rate: comparison.side_disagreement_rate,
                common_samples,
                category_breakdown_diff: comparison.category_breakdown_diff.clone().into(),
                comparison_hash: comparison.comparison_hash,
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
            prepared,
            input,
        } = ctx;
        let result = Box::pin(self.replay(
            ReplayCtx {
                backtest_report_id,
                model_run_id,
                prepared,
                decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            },
            progress,
            cancel,
        ))
        .await?;

        progress.report(ResearchJobProgress::indeterminate("finalize", 0));
        let info = self.persist_report(&model_run_id, &result.report).await?;
        Ok((info, result))
    }

    /// Execute one deterministic replay without deciding which evidence ledger
    /// owns the result.
    async fn replay(
        &self,
        ctx: ReplayCtx,
        progress: &Arc<dyn JobProgressSink>,
        cancel: &CancellationToken,
    ) -> QuantResult<BacktestRunResult> {
        let ReplayCtx {
            backtest_report_id,
            model_run_id,
            prepared,
            decision_policy_snapshot_id,
        } = ctx;
        let PreparedReplay {
            version,
            dataset,
            model,
            examples,
            frozen_source,
            calibration_contract: _,
        } = prepared;

        let request = BacktestRequest {
            backtest_report_id,
            model_version_id: version.model_version_id,
            dataset_id: dataset.training_dataset_id,
            decision_policy_snapshot_id,
            window_start: dataset.window_start,
            window_end: dataset.window_end,
        };

        // Offload frozen tick assembly + portfolio replay
        // to the governed offline pool so it never occupies an async runtime worker,
        // polling `cancel` at each cross-section boundary.
        let inputs = BacktestReplayInputs {
            model,
            examples,
            caps: self.caps.clone(),
            request,
            model_run_id,
            sink: Arc::clone(progress),
            cancel: cancel.clone(),
            frozen_source,
            entry_max_slippage_bps: self.entry_max_slippage_bps,
        };
        let runtime = Handle::current();
        let result = self
            .deps
            .compute
            .run_offline_cancellable(OfflineMemory::try_gib(4)?, cancel, move || {
                let _runtime = runtime.enter();
                (inputs).run_backtest_replay_blocking()
            })
            .await?;
        let Some(result) = result else {
            return Err(ResearchError::Cancelled {
                detail: "backtest cancelled during the replay".to_owned(),
            }
            .into());
        };

        Ok(result)
    }

    /// Persist the backtest report ledger row.
    async fn persist_report(
        &self,
        model_run_id: &ModelRunId,
        report: &BacktestReport,
    ) -> QuantResult<BacktestReportInfo> {
        let sample_count = i64::try_from(report.sample_count).map_err(|_| {
            ResearchError::EvidenceCountOverflow {
                field: "backtest.sample_count",
                value: report.sample_count,
            }
        })?;
        let missing_feature_count = i64::try_from(report.missing_feature_count).map_err(|_| {
            ResearchError::EvidenceCountOverflow {
                field: "backtest.missing_feature_count",
                value: report.missing_feature_count,
            }
        })?;
        let info = self
            .deps
            .backtest_report_repo
            .create(NewBacktestReport {
                backtest_report_id: report.backtest_report_id,
                model_version_id: report.model_version_id,
                evaluation_dataset_id: report.dataset_id,
                model_run_id: *model_run_id,
                decision_policy_snapshot_id: report.decision_policy_snapshot_id,
                window_start: report.window_start,
                window_end: report.window_end,
                coverage: report.coverage,
                sample_count,
                missing_feature_count,
                rank_ic: report.rank_ic,
                sharpe: report.sharpe,
                hit_rate: report.hit_rate,
                expected_vs_realized: report.expected_vs_realized.clone(),
                max_drawdown: report.max_drawdown,
                turnover: report.turnover,
                liquidity_feasibility: report.liquidity_feasibility,
                category_breakdown: report.category_breakdown.clone().into(),
                tail_loss: report.tail_loss,
                report_pnl_simulation: report.report_pnl_simulation.clone(),
                report_hash: report.report_hash,
                parquet_uri: None,
            })
            .await?;
        Ok(info)
    }

    async fn find_dataset(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> QuantResult<TrainingDatasetInfo> {
        self.deps
            .dataset_repo
            .find_by_id(training_dataset_id)
            .await?
            .ok_or_else(|| {
                StorageError::NotFound {
                    entity: "training_dataset",
                    id: training_dataset_id.to_string(),
                }
                .into()
            })
    }

    fn verify_reserved_dataset(
        params: &FeedbackComparisonJobParams,
        dataset: &TrainingDatasetInfo,
    ) -> QuantResult<()> {
        let materialization = require_dataset_materialization(dataset)?;
        let cohort_manifest = dataset.cohort_manifest.as_ref().ok_or_else(|| {
            Self::comparison_error("reserved Evaluation Dataset has no cohort manifest")
        })?;
        let cohort_manifest_hash = CanonicalDigest::content_hash_json(cohort_manifest)?;
        if dataset.training_dataset_id != params.evaluation_use.evaluation_dataset_id
            || dataset.window_start != params.evaluation_use.evaluation_window_start
            || dataset.window_end != params.evaluation_use.evaluation_window_end
            || dataset.research_profile_artifact_id.profile_ref()
                != params.evaluation_use.profile_ref
            || dataset.decision_policy_snapshot_id != params.decision_policy_snapshot_id
            || *materialization.dataset_hash != params.evaluation_use.evaluation_dataset_hash
            || *materialization.artifact_bytes_hash
                != params.evaluation_use.evaluation_artifact_bytes_hash
            || cohort_manifest_hash != params.evaluation_use.cohort_manifest_hash
        {
            return Err(Self::comparison_error(
                "Evaluation Dataset differs from its durable one-time reservation",
            ));
        }
        Ok(())
    }

    fn verify_comparison_model(
        version: &ModelVersionInfo,
        expected_id: ModelVersionId,
        expected_contract_hash: ContentHash,
    ) -> QuantResult<()> {
        version.verified_serving_contract().map_err(|error| {
            Self::comparison_error(format!("invalid serving contract: {error}"))
        })?;
        if version.model_version_id != expected_id
            || version.serving_contract_hash != expected_contract_hash
        {
            return Err(Self::comparison_error(
                "comparison model differs from its frozen id/serving-contract hash",
            ));
        }
        Ok(())
    }

    fn comparison_error(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidComparisonEvidence {
            detail: detail.into(),
        }
        .into()
    }

    async fn create_run(
        &self,
        model_run_id: &ModelRunId,
        model_version_id: ModelVersionId,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        dataset: &TrainingDatasetInfo,
        run_kind: ModelRunKind,
    ) -> QuantResult<()> {
        let materialization = require_dataset_materialization(dataset)?;
        self.deps
            .model_run_repo
            .start_exact(NewModelRun {
                model_run_id: *model_run_id,
                run_kind,
                model_version_id: Some(model_version_id),
                decision_policy_snapshot_id,
                market_selection_id: None,
                window_start: dataset.window_start,
                window_end: dataset.window_end,
                input_hash: *materialization.dataset_hash,
            })
            .await?;
        Ok(())
    }
}

/// Fully verified replay state. Runtime construction is complete, but no
/// evidence row has been created.
struct PreparedReplay {
    version: ModelVersionInfo,
    dataset: TrainingDatasetInfo,
    model: Arc<dyn QuantModelRuntime>,
    examples: Vec<TrainingExample>,
    frozen_source: FrozenSourceSlice,
    calibration_contract: Option<ModelScoreCalibrationFitContract>,
}

struct FeedbackPreparedModel {
    model_version_id: ModelVersionId,
    serving_contract_hash: ContentHash,
    model_run_id: ModelRunId,
    backtest_report_id: BacktestReportId,
    model: Arc<dyn QuantModelRuntime>,
}

impl FeedbackPreparedModel {
    fn champion(
        params: &FeedbackComparisonJobParams,
        source: &VerifiedModelServingPreimage,
    ) -> QuantResult<Self> {
        Ok(Self {
            model_version_id: params.champion_model_version_id,
            serving_contract_hash: params.champion_serving_contract_hash,
            model_run_id: params.champion_model_run_id,
            backtest_report_id: params.champion_backtest_report_id,
            model: source.buy_runtime()?,
        })
    }

    fn candidate(
        candidate: &FeedbackComparisonCandidateRef,
        source: &VerifiedModelServingPreimage,
    ) -> QuantResult<Self> {
        Ok(Self {
            model_version_id: candidate.model_version_id,
            serving_contract_hash: candidate.serving_contract_hash,
            model_run_id: candidate.model_run_id,
            backtest_report_id: candidate.backtest_report_id,
            model: source.buy_runtime()?,
        })
    }
}

struct FeedbackReplayBatch {
    prepared: Vec<FeedbackPreparedModel>,
    examples: Vec<TrainingExample>,
    frozen_source: FrozenSourceSlice,
    caps: PortfolioCaps,
    entry_max_slippage_bps: Bps,
    evaluation_dataset_id: TrainingDatasetId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    progress: Arc<dyn JobProgressSink>,
    cancel: CancellationToken,
}

impl FeedbackReplayBatch {
    fn run_blocking(self) -> QuantResult<Option<FeedbackFamilyReplay>> {
        Handle::current().block_on(self.run())
    }

    async fn run(self) -> QuantResult<Option<FeedbackFamilyReplay>> {
        let mut outputs = Vec::with_capacity(self.prepared.len());
        let total = u64::try_from(self.prepared.len()).map_err(|error| {
            BacktestService::comparison_error(format!(
                "comparison model count does not fit u64: {error}"
            ))
        })?;
        for (index, prepared) in self.prepared.into_iter().enumerate() {
            if self.cancel.is_cancelled() {
                return Ok(None);
            }
            let completed = u64::try_from(index).map_err(|error| {
                BacktestService::comparison_error(format!(
                    "comparison progress index does not fit u64: {error}"
                ))
            })?;
            self.progress.report(ResearchJobProgress::with_total(
                "comparison_replay",
                completed,
                total,
            ));
            let Some(ticks) = frozen_ticks(
                &self.examples,
                &self.frozen_source,
                self.entry_max_slippage_bps,
                prepared.model.as_ref(),
                &prepared.model_run_id,
                &self.cancel,
                self.progress.as_ref(),
            )?
            else {
                return Ok(None);
            };
            let result = PortfolioReplayBacktester::new()
                .run(BacktestInputs {
                    request: BacktestRequest {
                        backtest_report_id: prepared.backtest_report_id,
                        model_version_id: prepared.model_version_id,
                        dataset_id: self.evaluation_dataset_id,
                        decision_policy_snapshot_id: self.decision_policy_snapshot_id,
                        window_start: self.window_start,
                        window_end: self.window_end,
                    },
                    model: prepared.model.as_ref(),
                    ticks,
                    caps: self.caps.clone(),
                })
                .await?;
            outputs.push(FeedbackModelReplay {
                model_version_id: prepared.model_version_id,
                serving_contract_hash: prepared.serving_contract_hash,
                report_hash: result.report.report_hash,
                portfolio_returns: result.portfolio_returns,
            });
        }
        let mut outputs = outputs.into_iter();
        let champion = outputs
            .next()
            .ok_or_else(|| BacktestService::comparison_error("comparison lost champion replay"))?;
        Ok(Some(FeedbackFamilyReplay {
            champion,
            candidates: outputs.collect(),
        }))
    }
}

/// Inputs for one recorded backtest replay.
struct RunInnerCtx<'a> {
    backtest_report_id: BacktestReportId,
    model_run_id: ModelRunId,
    prepared: PreparedReplay,
    input: &'a BacktestInput,
}

/// Verified inputs shared by evaluation and calibration replay execution.
struct ReplayCtx {
    backtest_report_id: BacktestReportId,
    model_run_id: ModelRunId,
    prepared: PreparedReplay,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
}

/// A completed, recorded single replay: the run id, persisted report info, and
/// the in-memory result (sample outcomes) needed for a pairwise comparison.
struct RecordedRun {
    model_run_id: ModelRunId,
    info: BacktestReportInfo,
    result: BacktestRunResult,
}

/// Owned inputs moved into the governed offline backtest replay.
struct BacktestReplayInputs {
    model: Arc<dyn QuantModelRuntime>,
    examples: Vec<TrainingExample>,
    caps: PortfolioCaps,
    request: BacktestRequest,
    model_run_id: ModelRunId,
    sink: Arc<dyn JobProgressSink>,
    cancel: CancellationToken,
    frozen_source: FrozenSourceSlice,
    entry_max_slippage_bps: Bps,
}

impl BacktestReplayInputs {
    /// Run the backtest replay on an offline Rayon worker.
    ///
    /// Frozen tick assembly is synchronous; `block_on` drives only the pure async
    /// portfolio replay without occupying an async runtime worker.
    /// Returns `None` when cancelled at a cross-section boundary.
    fn run_backtest_replay_blocking(self) -> QuantResult<Option<BacktestRunResult>> {
        Handle::current().block_on((self).run_backtest_replay())
    }
}

impl BacktestReplayInputs {
    /// Assemble exact frozen runtime inputs and run the portfolio replay.
    async fn run_backtest_replay(self) -> QuantResult<Option<BacktestRunResult>> {
        let Some(ticks) = frozen_ticks(
            &self.examples,
            &self.frozen_source,
            self.entry_max_slippage_bps,
            self.model.as_ref(),
            &self.model_run_id,
            &self.cancel,
            self.sink.as_ref(),
        )?
        else {
            return Ok(None);
        };

        self.sink.report(ResearchJobProgress::indeterminate(
            "replay",
            ticks.len() as u64,
        ));
        let result = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: self.request,
                model: self.model.as_ref(),
                ticks,
                caps: self.caps,
            })
            .await?;
        Ok(Some(result))
    }
}

/// Assemble replay ticks from immutable v1 Parquet examples and Source Slice.
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
    frozen_source: &FrozenSourceSlice,
    entry_max_slippage_bps: Bps,
    model: &dyn QuantModelRuntime,
    model_run_id: &ModelRunId,
    cancel: &CancellationToken,
    sink: &dyn JobProgressSink,
) -> QuantResult<Option<Vec<BacktestTick>>> {
    let execution = frozen_execution_snapshots(examples, frozen_source, entry_max_slippage_bps)?;
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
    let settlement_label = LabelName::new("token_payout_ratio");
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
            .collect::<HashMap<_, _>>();
        if contexts.is_empty() {
            continue;
        }
        let mut market_meta = Vec::with_capacity(contexts.len());
        let mut outcomes = Vec::with_capacity(contexts.len());
        let mut execution_snapshots = Vec::new();
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
            let yes_payout_ratio = settlement_payout(example, &settlement_label)?;
            outcomes.push(MarketOutcome {
                market_id: example.market_id.clone(),
                yes_payout_ratio,
                max_adverse_excursion_bps: max_adverse_excursion(example, &mae_label),
            });
            for token_id in iter::once(&example.selected_market.primary_token_id)
                .chain(example.selected_market.secondary_token_id.as_ref())
            {
                let key = (decision_at, token_id.as_str());
                let snapshot = execution.get(&key).ok_or_else(|| {
                    ResearchError::DatasetBuild {
                        detail: format!(
                            "backtest execution snapshot is missing for token {token_id} at {decision_at}"
                        ),
                    }
                })?;
                if !execution_snapshots
                    .iter()
                    .any(|existing: &BacktestExecutionSnapshot| existing.token_id == *token_id)
                {
                    execution_snapshots.push(snapshot.clone());
                }
            }
        }
        ticks.push(BacktestTick {
            decision_at,
            model_input,
            outcomes,
            market_meta,
            execution: execution_snapshots,
        });
    }
    if cancel.is_cancelled() {
        return Ok(None);
    }
    Ok(Some(ticks))
}

fn frozen_execution_snapshots<'a>(
    examples: &'a [TrainingExample],
    source: &FrozenSourceSlice,
    max_slippage_bps: Bps,
) -> QuantResult<HashMap<(DateTime<Utc>, &'a str), BacktestExecutionSnapshot>> {
    let pages = replay_pages_for_examples(examples, source)?;
    let page_by_market = pages
        .iter()
        .enumerate()
        .flat_map(|(index, page)| {
            page.market_ids
                .iter()
                .map(move |market_id| (market_id.as_str(), index))
        })
        .collect::<HashMap<_, _>>();
    let mut snapshots = HashMap::new();
    for example in examples {
        let page_index = page_by_market
            .get(example.market_id.as_str())
            .copied()
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: format!(
                    "Source Slice replay page is missing market {}",
                    example.market_id
                ),
            })?;
        let page = &pages[page_index];
        for token_id in iter::once(&example.selected_market.primary_token_id)
            .chain(example.selected_market.secondary_token_id.as_ref())
        {
            let boundary = &example.decision_boundary;
            let book = page.book_at_boundary(token_id, boundary)?.ok_or_else(|| {
                ResearchError::DatasetBuild {
                    detail: format!(
                        "Source Slice has no full L2 for token {token_id} at {}",
                        example.decision_at()
                    ),
                }
            })?;
            let market_info = page
                .market_info_at(&example.market_id, token_id, boundary)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: format!(
                        "Source Slice has no PIT fee schedule for token {token_id} at {}",
                        example.decision_at()
                    ),
                })?;
            let fee_schedule = PitFeeSchedule::from_market_fee_schedule(
                &market_info.fee_schedule(),
            )
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("invalid PIT fee schedule for token {token_id}: {error:?}"),
            })?;
            let best_ask = book
                .asks
                .first()
                .map(|level| level.price_decimal())
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: format!(
                        "Source Slice full L2 has no ask for token {token_id} at {}",
                        example.decision_at()
                    ),
                })?;
            let book_hash = CanonicalDigest::content_hash_json(&(
                book.token_id.clone(),
                book.timestamp_ms,
                book.version,
                book.sequence,
                book.bids.as_ref(),
                book.asks.as_ref(),
            ))?;
            snapshots.insert(
                (example.decision_at(), token_id.as_str()),
                BacktestExecutionSnapshot {
                    market_id: example.market_id.clone(),
                    token_id: token_id.clone(),
                    asks: book.asks.to_vec(),
                    fee_schedule,
                    fill_at: example.decision_at(),
                    limit_price: aggressive_buy_limit(best_ask, max_slippage_bps),
                    book_hash,
                },
            );
        }
    }
    Ok(snapshots)
}

fn replay_pages_for_examples(
    examples: &[TrainingExample],
    source: &FrozenSourceSlice,
) -> QuantResult<Vec<ReplayPage>> {
    let mut by_market = BTreeMap::<MarketId, Vec<TokenId>>::new();
    for example in examples {
        let tokens = by_market.entry(example.market_id.clone()).or_default();
        for token_id in iter::once(&example.selected_market.primary_token_id)
            .chain(example.selected_market.secondary_token_id.as_ref())
        {
            if !tokens.contains(token_id) {
                tokens.push(token_id.clone());
            }
        }
    }
    let markets = by_market.into_iter().collect::<Vec<_>>();
    markets
        .chunks(MAX_REPLAY_PAGE_MARKETS)
        .map(|chunk| {
            let market_ids = chunk
                .iter()
                .map(|(market_id, _)| market_id.clone())
                .collect::<Vec<_>>();
            let mut token_ids = chunk
                .iter()
                .flat_map(|(_, tokens)| tokens.iter().cloned())
                .collect::<Vec<_>>();
            token_ids.sort();
            token_ids.dedup();
            source.replay_page(&ReplayPageRequest {
                market_ids,
                token_ids,
                window_start: source.window_start,
                window_end: source.window_end,
                available_by: source.pit_cutoff,
            })
        })
        .collect()
}

fn settlement_payout(
    example: &TrainingExample,
    label: &LabelName,
) -> QuantResult<Option<PayoutRatio>> {
    let Some(row) = example.labels.iter().find(|row| row.label_name == *label) else {
        return Ok(None);
    };
    if !row.is_resolved {
        return Ok(None);
    }
    PayoutRatio::try_new(row.value).map(Some).map_err(|error| {
        ResearchError::LabelResolution {
            detail: format!(
                "settlement payout for market {} is invalid: {error}",
                example.market_id
            ),
        }
        .into()
    })
}

fn max_adverse_excursion(example: &TrainingExample, label: &LabelName) -> Option<Decimal> {
    example
        .labels
        .iter()
        .find(|row| row.label_name == *label && row.is_resolved)
        .map(|row| row.value)
}
