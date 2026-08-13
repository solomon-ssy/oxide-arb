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
    collections::{BTreeMap, BTreeSet, HashMap},
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
    config::PortfolioSolverDeployConfig,
    domain::{
        data_plane::{DecisionBoundary, DecisionSource},
        governance::DecisionPolicySnapshotInfo,
        ports::{FeedbackComparisonCandidateRef, FeedbackComparisonJobParams},
        quant::{
            BacktestReportInfo, JobProgressSink, ModelComparisonReportInfo, ModelVersionInfo,
            NewBacktestReport, NewModelComparisonReport, NewModelRun, PortfolioScenarioVisibility,
            TrainingDatasetInfo,
        },
        query::TimeWindow,
    },
    enums::quant::{DatasetPurpose, ModelRunErrorCode, ModelRunKind},
    hashing::CanonicalDigest,
    types::{
        BacktestReportId, Bps, ContentHash, DecisionPolicySnapshotId, MarketId,
        ModelComparisonReportId, ModelRunId, ModelVersionId, PayoutRatio, Price,
        ResearchJobProgress, ResearchProfileArtifact, TokenId, TrainingDatasetId,
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
        BacktestDownsidePoint, BacktestDownsideTrajectory, BacktestExecutionSnapshot,
        BacktestInputs, BacktestLiquidationSnapshot, BacktestMarketMeta, BacktestPortfolioContext,
        BacktestRankTarget, BacktestReport, BacktestRequest, BacktestRunResult,
        BacktestScenarioContext, BacktestTick, Backtester, CalibrationReplayTick, MarketOutcome,
        ModelCalibrationOutcome, ModelCalibrationReplay, ModelComparisonReport,
        PortfolioReplayBacktester, PortfolioReturnObservation,
    },
    execution_semantics::{PitFeeSchedule, aggressive_buy_limit},
    model::{LabelSelector, ModelRankTarget, QuantModelRuntime},
    pit::BookSnapshotAt,
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
        portfolio_context::{PreparedBacktestPortfolio, PromotedPortfolioContextLoader},
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
    /// Unique deterministic production/backtest MILP boundary.
    pub portfolio_solver: PortfolioSolverDeployConfig,
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
    pub samples: Vec<ModelCalibrationOutcome>,
    /// Exact catalog window of the deeply verified Calibration Dataset.
    pub fit_window: TimeWindow,
    /// Canonical model, Dataset/Source Slice, and policy preimages.
    pub fit_contract: ModelScoreCalibrationFitContract,
}

/// Offline backtest service, bound to one frozen runtime-config snapshot.
pub struct BacktestService {
    deps: BacktestServiceDeps,
    policy_binding: ModelServingPolicySnapshotBinding,
    portfolio_contexts: PromotedPortfolioContextLoader,
    evaluation_frozen_at: DateTime<Utc>,
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
        let entry_max_slippage_bps = Bps::new(Decimal::from(
            policy
                .snapshot
                .execution_risk
                .entry_order_policy
                .max_slippage_bps,
        ));
        let portfolio_contexts = PromotedPortfolioContextLoader::new(
            Arc::clone(&deps.artifact_store),
            deps.portfolio_solver,
            policy.snapshot.clone(),
        );
        Ok(Self {
            deps,
            policy_binding,
            portfolio_contexts,
            evaluation_frozen_at: policy.created_at,
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
        let prepared = Box::pin(self.prepare_evaluation(
            &input.model_version_id,
            &input.evaluation_dataset_id,
            input.decision_policy_snapshot_id,
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
            &self.policy_binding,
        ))
        .await?;
        let (examples, frozen_source) = self
            .load_replay_artifacts(&replay, champion_source.profile())
            .await?;

        let mut prepared = Vec::with_capacity(params.candidates.len() + 1);
        let champion_portfolio = self.portfolio_context(&champion_source, &dataset).await?;
        prepared.push(FeedbackPreparedModel::champion(
            params,
            &champion_source,
            champion_portfolio,
        )?);
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
                &self.policy_binding,
            ))
            .await?;
            let portfolio = self.portfolio_context(&source, &dataset).await?;
            prepared.push(FeedbackPreparedModel::candidate(
                candidate, &source, portfolio,
            )?);
        }

        let batch = FeedbackReplayBatch {
            prepared,
            examples,
            frozen_source,
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
        let prepared = Box::pin(self.prepare_evaluation(
            &input.model_version_id,
            &input.evaluation_dataset_id,
            input.decision_policy_snapshot_id,
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
        let candidate = Box::pin(self.prepare_evaluation(
            &input.model_version_id,
            &input.evaluation_dataset_id,
            input.decision_policy_snapshot_id,
        ))
        .await?;
        let baseline = Box::pin(self.prepare_evaluation(
            &baseline_version_id,
            &input.evaluation_dataset_id,
            input.decision_policy_snapshot_id,
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
        let candidate_prepared = Box::pin(self.prepare_evaluation(
            &input.model_version_id,
            &input.evaluation_dataset_id,
            input.decision_policy_snapshot_id,
        ))
        .await?;
        let baseline_prepared = Box::pin(self.prepare_evaluation(
            &baseline_version_id,
            &input.evaluation_dataset_id,
            input.decision_policy_snapshot_id,
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
        let prepared = Box::pin(self.prepare_calibration(
            &input.source_model,
            &input.calibration_dataset,
            input.policy_snapshot,
        ))
        .await?;
        let fit_contract = prepared.fit_contract.clone();
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

        let replay =
            Box::pin(self.replay_calibration(model_run_id, prepared, &progress, &cancel)).await;
        match replay {
            Ok(samples) => Ok(CalibrationReplayEvidence {
                model_run_id,
                samples,
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

    async fn portfolio_context(
        &self,
        source: &VerifiedModelServingPreimage,
        dataset: &TrainingDatasetInfo,
    ) -> QuantResult<PreparedBacktestPortfolio> {
        self.portfolio_contexts
            .load_evaluation_single(
                source,
                dataset.window_start,
                self.evaluation_frozen_at,
                self.portfolio_contexts
                    .policy()
                    .recommendation
                    .reports
                    .ad_hoc_default_top_n,
            )
            .await
    }

    /// Resolve and verify every immutable preimage, then load the runtime.
    /// Nothing in this path creates evidence or mutates a repository.
    async fn prepare_evaluation(
        &self,
        model_version_id: &ModelVersionId,
        dataset_id: &TrainingDatasetId,
        policy_id: DecisionPolicySnapshotId,
    ) -> QuantResult<PreparedReplay> {
        let prepared = Box::pin(self.prepare_source(
            model_version_id,
            dataset_id,
            policy_id,
            DatasetPurpose::Evaluation,
        ))
        .await?;
        let portfolio = self
            .portfolio_context(&prepared.source, &prepared.dataset)
            .await?;
        let model = prepared.source.buy_runtime()?;
        let rank_target = prepared.source.replay_rank_target();
        Ok(PreparedReplay {
            version: prepared.version,
            dataset: prepared.dataset,
            model,
            rank_target,
            examples: prepared.examples,
            frozen_source: prepared.frozen_source,
            portfolio: portfolio.portfolio,
            scenario: portfolio.scenario,
            scenario_visibility: portfolio.scenario_visibility,
        })
    }

    async fn prepare_calibration(
        &self,
        model_version_id: &ModelVersionId,
        dataset_id: &TrainingDatasetId,
        policy_id: DecisionPolicySnapshotId,
    ) -> QuantResult<PreparedCalibrationReplay> {
        let prepared = Box::pin(self.prepare_source(
            model_version_id,
            dataset_id,
            policy_id,
            DatasetPurpose::Calibration,
        ))
        .await?;
        let fit_contract = Box::pin(
            self.deps
                .serving_preimages
                .calibration_fit_contract(&prepared.source, &prepared.dataset),
        )
        .await?;
        let model = prepared.source.buy_runtime()?;
        Ok(PreparedCalibrationReplay {
            dataset: prepared.dataset,
            model,
            examples: prepared.examples,
            frozen_source: prepared.frozen_source,
            fit_contract,
        })
    }

    async fn prepare_source(
        &self,
        model_version_id: &ModelVersionId,
        dataset_id: &TrainingDatasetId,
        policy_id: DecisionPolicySnapshotId,
        purpose: DatasetPurpose,
    ) -> QuantResult<PreparedReplaySource> {
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
        let replay = Box::pin(self.deps.serving_preimages.verify_replay_dataset(
            &source,
            &dataset,
            purpose,
            &self.policy_binding,
        ))
        .await?;
        let (examples, frozen_source) = self
            .load_replay_artifacts(&replay, source.profile())
            .await?;
        Ok(PreparedReplaySource {
            version,
            dataset,
            source,
            examples,
            frozen_source,
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
            rank_target,
            examples,
            frozen_source,
            portfolio,
            scenario,
            scenario_visibility,
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
            rank_target,
            examples,
            portfolio,
            scenario,
            scenario_visibility,
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

    /// Replay the complete calibration population without constructing a
    /// portfolio contract. A newly trained challenger has no active Route or
    /// promoted scenario authority yet; requiring either here would make the
    /// promotion DAG cyclic and would incorrectly censor calibration through
    /// downstream allocation.
    async fn replay_calibration(
        &self,
        model_run_id: ModelRunId,
        prepared: PreparedCalibrationReplay,
        progress: &Arc<dyn JobProgressSink>,
        cancel: &CancellationToken,
    ) -> QuantResult<Vec<ModelCalibrationOutcome>> {
        let inputs = CalibrationReplayInputs {
            model: prepared.model,
            examples: prepared.examples,
            model_run_id,
            sink: Arc::clone(progress),
            cancel: cancel.clone(),
            frozen_source: prepared.frozen_source,
            entry_max_slippage_bps: self.entry_max_slippage_bps,
        };
        let runtime = Handle::current();
        let result = self
            .deps
            .compute
            .run_offline_cancellable(OfflineMemory::try_gib(4)?, cancel, move || {
                let _runtime = runtime.enter();
                inputs.run_calibration_replay_blocking()
            })
            .await?;
        result.ok_or_else(|| {
            ResearchError::Cancelled {
                detail: "calibration cancelled during allocation-independent replay".to_owned(),
            }
            .into()
        })
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
                portfolio_funnel: report.portfolio_funnel.clone(),
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
    rank_target: LabelSelector,
    examples: Vec<TrainingExample>,
    frozen_source: FrozenSourceSlice,
    portfolio: BacktestPortfolioContext,
    scenario: BacktestScenarioContext,
    scenario_visibility: PortfolioScenarioVisibility,
}

/// Common, fully verified source graph before purpose-specific replay context
/// is attached.
struct PreparedReplaySource {
    version: ModelVersionInfo,
    dataset: TrainingDatasetInfo,
    source: VerifiedModelServingPreimage,
    examples: Vec<TrainingExample>,
    frozen_source: FrozenSourceSlice,
}

/// Calibration replay state deliberately excludes portfolio and active Route
/// authority so a challenger can be calibrated before promotion.
struct PreparedCalibrationReplay {
    dataset: TrainingDatasetInfo,
    model: Arc<dyn QuantModelRuntime>,
    examples: Vec<TrainingExample>,
    frozen_source: FrozenSourceSlice,
    fit_contract: ModelScoreCalibrationFitContract,
}

struct FeedbackPreparedModel {
    model_version_id: ModelVersionId,
    serving_contract_hash: ContentHash,
    model_run_id: ModelRunId,
    backtest_report_id: BacktestReportId,
    model: Arc<dyn QuantModelRuntime>,
    rank_target: LabelSelector,
    portfolio: BacktestPortfolioContext,
    scenario: BacktestScenarioContext,
    scenario_visibility: PortfolioScenarioVisibility,
}

impl FeedbackPreparedModel {
    fn champion(
        params: &FeedbackComparisonJobParams,
        source: &VerifiedModelServingPreimage,
        portfolio: PreparedBacktestPortfolio,
    ) -> QuantResult<Self> {
        Ok(Self {
            model_version_id: params.champion_model_version_id,
            serving_contract_hash: params.champion_serving_contract_hash,
            model_run_id: params.champion_model_run_id,
            backtest_report_id: params.champion_backtest_report_id,
            model: source.buy_runtime()?,
            rank_target: LabelSelector {
                name: LabelName::new(
                    source
                        .model_spec()
                        .training_contract
                        .target
                        .label_name()
                        .to_owned(),
                ),
                horizon_secs: source
                    .model_spec()
                    .training_contract
                    .target
                    .label_horizon_secs(),
            },
            portfolio: portfolio.portfolio,
            scenario: portfolio.scenario,
            scenario_visibility: portfolio.scenario_visibility,
        })
    }

    fn candidate(
        candidate: &FeedbackComparisonCandidateRef,
        source: &VerifiedModelServingPreimage,
        portfolio: PreparedBacktestPortfolio,
    ) -> QuantResult<Self> {
        Ok(Self {
            model_version_id: candidate.model_version_id,
            serving_contract_hash: candidate.serving_contract_hash,
            model_run_id: candidate.model_run_id,
            backtest_report_id: candidate.backtest_report_id,
            model: source.buy_runtime()?,
            rank_target: LabelSelector {
                name: LabelName::new(
                    source
                        .model_spec()
                        .training_contract
                        .target
                        .label_name()
                        .to_owned(),
                ),
                horizon_secs: source
                    .model_spec()
                    .training_contract
                    .target
                    .label_horizon_secs(),
            },
            portfolio: portfolio.portfolio,
            scenario: portfolio.scenario,
            scenario_visibility: portfolio.scenario_visibility,
        })
    }
}

struct FeedbackReplayBatch {
    prepared: Vec<FeedbackPreparedModel>,
    examples: Vec<TrainingExample>,
    frozen_source: FrozenSourceSlice,
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
            let Some(ticks) = frozen_ticks(FrozenTickBuild {
                examples: &self.examples,
                frozen_source: &self.frozen_source,
                entry_max_slippage_bps: self.entry_max_slippage_bps,
                rank_target: &prepared.rank_target,
                model: prepared.model.as_ref(),
                model_run_id: &prepared.model_run_id,
                portfolio: &prepared.portfolio,
                cancel: &self.cancel,
                sink: self.progress.as_ref(),
            })?
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
                    scenario: &prepared.scenario,
                    scenario_visibility: prepared.scenario_visibility,
                    ticks,
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
    rank_target: LabelSelector,
    examples: Vec<TrainingExample>,
    portfolio: BacktestPortfolioContext,
    scenario: BacktestScenarioContext,
    scenario_visibility: PortfolioScenarioVisibility,
    request: BacktestRequest,
    model_run_id: ModelRunId,
    sink: Arc<dyn JobProgressSink>,
    cancel: CancellationToken,
    frozen_source: FrozenSourceSlice,
    entry_max_slippage_bps: Bps,
}

/// Owned inputs moved into the governed offline calibration replay.
struct CalibrationReplayInputs {
    model: Arc<dyn QuantModelRuntime>,
    examples: Vec<TrainingExample>,
    model_run_id: ModelRunId,
    sink: Arc<dyn JobProgressSink>,
    cancel: CancellationToken,
    frozen_source: FrozenSourceSlice,
    entry_max_slippage_bps: Bps,
}

impl CalibrationReplayInputs {
    fn run_calibration_replay_blocking(self) -> QuantResult<Option<Vec<ModelCalibrationOutcome>>> {
        Handle::current().block_on(self.run())
    }

    async fn run(self) -> QuantResult<Option<Vec<ModelCalibrationOutcome>>> {
        let Some(ticks) = frozen_calibration_ticks(FrozenCalibrationBuild {
            examples: &self.examples,
            frozen_source: &self.frozen_source,
            entry_max_slippage_bps: self.entry_max_slippage_bps,
            model: self.model.as_ref(),
            model_run_id: &self.model_run_id,
            cancel: &self.cancel,
            sink: self.sink.as_ref(),
        })?
        else {
            return Ok(None);
        };
        let outcomes = ModelCalibrationReplay::new()
            .run(self.model.as_ref(), ticks)
            .await?;
        if self.cancel.is_cancelled() {
            return Ok(None);
        }
        Ok(Some(outcomes))
    }
}

impl VerifiedModelServingPreimage {
    fn replay_rank_target(&self) -> LabelSelector {
        LabelSelector {
            name: LabelName::new(
                self.model_spec()
                    .training_contract
                    .target
                    .label_name()
                    .to_owned(),
            ),
            horizon_secs: self
                .model_spec()
                .training_contract
                .target
                .label_horizon_secs(),
        }
    }
}

#[derive(Clone, Copy)]
struct FrozenCalibrationBuild<'a> {
    examples: &'a [TrainingExample],
    frozen_source: &'a FrozenSourceSlice,
    entry_max_slippage_bps: Bps,
    model: &'a dyn QuantModelRuntime,
    model_run_id: &'a ModelRunId,
    cancel: &'a CancellationToken,
    sink: &'a dyn JobProgressSink,
}

/// Assemble allocation-independent calibration ticks from the exact frozen
/// Dataset and Source Slice. This runs before any Route activation and cannot
/// consult portfolio admission, scenario selection, or account state.
fn frozen_calibration_ticks(
    input: FrozenCalibrationBuild<'_>,
) -> QuantResult<Option<Vec<CalibrationReplayTick>>> {
    let FrozenCalibrationBuild {
        examples,
        frozen_source,
        entry_max_slippage_bps,
        model,
        model_run_id,
        cancel,
        sink,
    } = input;
    let pages = replay_pages_for_examples(examples, frozen_source)?;
    let execution = frozen_execution_snapshots(examples, &pages, entry_max_slippage_bps)?;
    let downside_trajectories = frozen_downside_trajectories(examples, frozen_source, &execution)?;
    let mut by_decision: BTreeMap<DateTime<Utc>, Vec<&TrainingExample>> = BTreeMap::new();
    for example in examples {
        by_decision
            .entry(example.decision_at())
            .or_default()
            .push(example);
    }
    let total_sections =
        u64::try_from(by_decision.len()).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("calibration cross-section count does not fit u64: {error}"),
        })?;
    let mut processed_sections = 0_u64;
    let mut ticks = Vec::with_capacity(by_decision.len());
    let settlement_label = LabelName::new("token_payout_ratio");
    for (decision_at, group) in by_decision {
        if cancel.is_cancelled() {
            return Ok(None);
        }
        processed_sections =
            processed_sections
                .checked_add(1)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: "calibration cross-section progress overflowed u64".to_owned(),
                })?;
        sink.report(ResearchJobProgress::with_total(
            "frozen_calibration_input",
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
        let mut outcomes = Vec::with_capacity(contexts.len());
        let mut seen_markets = BTreeSet::new();
        for example in group {
            if !contexts.contains_key(&example.market_id) {
                continue;
            }
            if !seen_markets.insert(example.market_id.as_str()) {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "calibration cross-section {decision_at} duplicates market {}",
                        example.market_id
                    ),
                }
                .into());
            }
            outcomes.push(settlement_outcome(
                example,
                &settlement_label,
                frozen_source,
            )?);
        }
        ticks.push(CalibrationReplayTick {
            decision_at,
            model_input,
            outcomes,
            downside_trajectories: downside_trajectories
                .iter()
                .filter(|trajectory| trajectory.anchor == decision_at)
                .cloned()
                .collect(),
        });
    }
    if cancel.is_cancelled() {
        return Ok(None);
    }
    Ok(Some(ticks))
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
        let Some(ticks) = frozen_ticks(FrozenTickBuild {
            examples: &self.examples,
            frozen_source: &self.frozen_source,
            entry_max_slippage_bps: self.entry_max_slippage_bps,
            rank_target: &self.rank_target,
            model: self.model.as_ref(),
            model_run_id: &self.model_run_id,
            portfolio: &self.portfolio,
            cancel: &self.cancel,
            sink: self.sink.as_ref(),
        })?
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
                scenario: &self.scenario,
                scenario_visibility: self.scenario_visibility,
                ticks,
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
#[derive(Clone, Copy)]
pub(crate) struct FrozenTickBuild<'a> {
    pub examples: &'a [TrainingExample],
    pub frozen_source: &'a FrozenSourceSlice,
    pub entry_max_slippage_bps: Bps,
    pub rank_target: &'a LabelSelector,
    pub model: &'a dyn QuantModelRuntime,
    pub model_run_id: &'a ModelRunId,
    pub portfolio: &'a BacktestPortfolioContext,
    pub cancel: &'a CancellationToken,
    pub sink: &'a dyn JobProgressSink,
}

pub(crate) fn frozen_ticks(input: FrozenTickBuild<'_>) -> QuantResult<Option<Vec<BacktestTick>>> {
    let FrozenTickBuild {
        examples,
        frozen_source,
        entry_max_slippage_bps,
        rank_target,
        model,
        model_run_id,
        portfolio,
        cancel,
        sink,
    } = input;
    let pages = replay_pages_for_examples(examples, frozen_source)?;
    let boundaries = canonical_decision_boundaries(examples)?;
    let execution = frozen_execution_snapshots(examples, &pages, entry_max_slippage_bps)?;
    let downside_trajectories = frozen_downside_trajectories(examples, frozen_source, &execution)?;
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
        let mut rank_targets = Vec::with_capacity(contexts.len());
        let mut execution_snapshots = Vec::new();
        for example in group {
            let Some(context) = contexts.get(&example.market_id) else {
                continue;
            };
            market_meta.push(BacktestMarketMeta {
                market_id: example.market_id.clone(),
                category: example.selected_market.category,
                event_id: example.selected_market.event_id.clone(),
                liquidity_usd: context.liquidity_usd,
            });
            outcomes.push(settlement_outcome(
                example,
                &settlement_label,
                frozen_source,
            )?);
            if let Some(label) = example.labels.iter().find(|label| {
                label.is_resolved
                    && label.label_name == rank_target.name
                    && label.horizon_secs == rank_target.horizon_secs
            }) {
                rank_targets.push(BacktestRankTarget {
                    market_id: example.market_id.clone(),
                    token_id: example.token_id.clone(),
                    target: ModelRankTarget {
                        label_name: rank_target.name.clone(),
                        label_horizon_secs: rank_target.horizon_secs,
                    },
                    realized: label.value,
                });
            }
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
            rank_targets,
            market_meta,
            execution: execution_snapshots,
            liquidation: Vec::new(),
            downside_trajectories: downside_trajectories
                .iter()
                .filter(|trajectory| trajectory.anchor == decision_at)
                .cloned()
                .collect(),
            portfolio_contract: portfolio.contract(decision_at)?,
        });
    }
    if cancel.is_cancelled() {
        return Ok(None);
    }
    bind_liquidation_plane(&mut ticks, &boundaries, &pages)?;
    Ok(Some(ticks))
}

fn canonical_decision_boundaries(
    examples: &[TrainingExample],
) -> QuantResult<BTreeMap<DateTime<Utc>, DecisionBoundary>> {
    let mut boundaries = BTreeMap::new();
    for example in examples {
        let decision_at = example.decision_at();
        if let Some(existing) = boundaries.get(&decision_at) {
            if existing != &example.decision_boundary {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "frozen cross-section at {decision_at} contains conflicting decision boundaries"
                    ),
                }
                .into());
            }
        } else {
            boundaries.insert(decision_at, example.decision_boundary.clone());
        }
    }
    Ok(boundaries)
}

fn frozen_downside_trajectories(
    examples: &[TrainingExample],
    source: &FrozenSourceSlice,
    execution: &HashMap<(DateTime<Utc>, &str), BacktestExecutionSnapshot>,
) -> QuantResult<Vec<BacktestDownsideTrajectory>> {
    let mut trajectories = Vec::new();
    for example in examples {
        let decision_at = example.decision_at();
        for token_id in iter::once(&example.selected_market.primary_token_id)
            .chain(example.selected_market.secondary_token_id.as_ref())
        {
            let snapshot = execution
                .get(&(decision_at, token_id.as_str()))
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: format!(
                        "backtest downside entry snapshot is missing for token {token_id} at {decision_at}"
                    ),
                })?;
            let entry_ask = snapshot
                .asks
                .first()
                .map(|level| level.price_decimal())
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: format!(
                        "backtest downside entry book has no ask for token {token_id} at {decision_at}"
                    ),
                })?;
            let mut rows = source
                .prefetched
                .micro
                .get(token_id)
                .map_or(&[][..], Vec::as_slice)
                .iter()
                .map(|row| {
                    DateTime::from_timestamp_millis(row.bucket_time)
                        .map(|at| (at, row))
                        .ok_or_else(|| ResearchError::DatasetBuild {
                            detail: format!(
                                "backtest downside bucket timestamp {} is invalid",
                                row.bucket_time
                            ),
                        })
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|(at, _)| *at > decision_at && *at <= source.window_end)
                .collect::<Vec<_>>();
            rows.sort_by_key(|(at, _)| *at);
            if rows.windows(2).any(|pair| pair[0].0 == pair[1].0) {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "backtest downside source has duplicate buckets for token {token_id}"
                    ),
                }
                .into());
            }
            let data_available_until = rows.last().map_or(decision_at, |(at, _)| *at);
            let points = rows
                .into_iter()
                .map(|(at, row)| BacktestDownsidePoint {
                    at,
                    best_bid_low: row.best_bid_low.map(Price::from),
                })
                .collect();
            trajectories.push(BacktestDownsideTrajectory {
                market_id: example.market_id.clone(),
                token_id: token_id.clone(),
                anchor: decision_at,
                entry_ask,
                data_available_until,
                points,
            });
        }
    }
    Ok(trajectories)
}

fn frozen_execution_snapshots<'a>(
    examples: &'a [TrainingExample],
    pages: &[ReplayPage],
    max_slippage_bps: Bps,
) -> QuantResult<HashMap<(DateTime<Utc>, &'a str), BacktestExecutionSnapshot>> {
    let page_by_market = replay_page_indices(pages)?;
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
            let book_hash = replay_book_hash(&book)?;
            snapshots.insert(
                (example.decision_at(), token_id.as_str()),
                BacktestExecutionSnapshot {
                    market_id: example.market_id.clone(),
                    token_id: token_id.clone(),
                    bids: book.bids.to_vec(),
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

struct LiquidationRetention {
    market_id: MarketId,
    resolved_at: DateTime<Utc>,
    mark_times: BTreeSet<DateTime<Utc>>,
}

fn bind_liquidation_plane(
    ticks: &mut [BacktestTick],
    boundaries: &BTreeMap<DateTime<Utc>, DecisionBoundary>,
    pages: &[ReplayPage],
) -> QuantResult<()> {
    let requirements = liquidation_requirements(ticks)?;
    let page_by_market = replay_page_indices(pages)?;
    let mut by_decision = BTreeMap::<DateTime<Utc>, Vec<BacktestLiquidationSnapshot>>::new();
    for (token_id, requirement) in requirements {
        if requirement.mark_times.is_empty() {
            continue;
        }
        let page_index = page_by_market
            .get(requirement.market_id.as_str())
            .copied()
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: format!(
                    "Source Slice replay page is missing liquidation market {}",
                    requirement.market_id
                ),
            })?;
        let page = &pages[page_index];
        let mark_boundaries = requirement
            .mark_times
            .iter()
            .map(|at| {
                boundaries
                    .get(at)
                    .cloned()
                    .ok_or_else(|| ResearchError::DatasetBuild {
                        detail: format!(
                            "frozen liquidation plane has no decision boundary at {at}"
                        ),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let books = replay_mark_books(page, &token_id, &mark_boundaries)?;
        if books.len() != mark_boundaries.len() {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "liquidation replay for token {token_id} returned {} books for {} boundaries",
                    books.len(),
                    mark_boundaries.len()
                ),
            }
            .into());
        }
        for (boundary, book) in mark_boundaries.iter().zip(books) {
            let marked_at = boundary.decision_at();
            let book = book.ok_or_else(|| ResearchError::DatasetBuild {
                detail: format!(
                    "Source Slice has no exact PIT liquidation book for token {token_id} at {marked_at}"
                ),
            })?;
            let market_info = page
                .market_info_at(&requirement.market_id, &token_id, boundary)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: format!(
                        "Source Slice has no PIT liquidation fee schedule for token {token_id} at {marked_at}"
                    ),
                })?;
            let fee_schedule = PitFeeSchedule::from_market_fee_schedule(
                &market_info.fee_schedule(),
            )
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!(
                    "invalid PIT liquidation fee schedule for token {token_id}: {error:?}"
                ),
            })?;
            by_decision
                .entry(marked_at)
                .or_default()
                .push(BacktestLiquidationSnapshot {
                    market_id: requirement.market_id.clone(),
                    token_id: token_id.clone(),
                    bids: book.bids.to_vec(),
                    fee_schedule,
                    marked_at,
                    book_hash: replay_book_hash(&book)?,
                });
        }
    }
    for snapshots in by_decision.values_mut() {
        snapshots.sort_by(|left, right| {
            (left.market_id.as_str(), left.token_id.as_str())
                .cmp(&(right.market_id.as_str(), right.token_id.as_str()))
        });
    }
    for tick in ticks {
        tick.liquidation = by_decision.remove(&tick.decision_at).unwrap_or_default();
    }
    if !by_decision.is_empty() {
        return Err(ResearchError::DatasetBuild {
            detail: "liquidation plane contains marks outside the frozen tick timeline".to_owned(),
        }
        .into());
    }
    Ok(())
}

fn liquidation_requirements(
    ticks: &[BacktestTick],
) -> QuantResult<BTreeMap<TokenId, LiquidationRetention>> {
    if ticks
        .windows(2)
        .any(|pair| pair[0].decision_at >= pair[1].decision_at)
    {
        return Err(ResearchError::DatasetBuild {
            detail: "frozen liquidation timeline is not strictly increasing".to_owned(),
        }
        .into());
    }
    let mut requirements = BTreeMap::<TokenId, LiquidationRetention>::new();
    for (entry_index, tick) in ticks.iter().enumerate() {
        for snapshot in &tick.execution {
            let outcome = tick
                .outcomes
                .iter()
                .find(|outcome| outcome.market_id == snapshot.market_id)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: format!(
                        "entry token {} has no settlement contract at {}",
                        snapshot.token_id, tick.decision_at
                    ),
                })?;
            let resolved_at = match (outcome.resolved_at, outcome.yes_payout_ratio) {
                (Some(resolved_at), Some(_)) if resolved_at > tick.decision_at => resolved_at,
                (None, None) => continue,
                _ => {
                    return Err(ResearchError::DatasetBuild {
                        detail: format!(
                            "entry token {} has inconsistent or non-causal settlement truth at {}",
                            snapshot.token_id, tick.decision_at
                        ),
                    }
                    .into());
                }
            };
            let requirement = requirements
                .entry(snapshot.token_id.clone())
                .or_insert_with(|| LiquidationRetention {
                    market_id: snapshot.market_id.clone(),
                    resolved_at,
                    mark_times: BTreeSet::new(),
                });
            if requirement.market_id != snapshot.market_id || requirement.resolved_at != resolved_at
            {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "token {} has conflicting market or resolution bindings across replay ticks",
                        snapshot.token_id
                    ),
                }
                .into());
            }
            requirement.mark_times.extend(
                ticks
                    .iter()
                    .skip(entry_index + 1)
                    .take_while(|later| later.decision_at < resolved_at)
                    .map(|later| later.decision_at),
            );
        }
    }
    Ok(requirements)
}

fn replay_mark_books(
    page: &ReplayPage,
    token_id: &TokenId,
    boundaries: &[DecisionBoundary],
) -> QuantResult<Vec<Option<BookSnapshotAt>>> {
    let strictly_monotonic = boundaries.windows(2).all(|pair| {
        pair[0].decision_at() < pair[1].decision_at()
            && pair[0].cutoff_for(DecisionSource::Book) < pair[1].cutoff_for(DecisionSource::Book)
    });
    if strictly_monotonic {
        return page.books_at_boundaries(token_id, boundaries);
    }
    boundaries
        .iter()
        .map(|boundary| page.book_at_boundary(token_id, boundary))
        .collect()
}

fn replay_book_hash(book: &BookSnapshotAt) -> QuantResult<ContentHash> {
    Ok(CanonicalDigest::content_hash_json(&(
        book.token_id.clone(),
        book.timestamp_ms,
        book.version,
        book.sequence,
        book.bids.as_ref(),
        book.asks.as_ref(),
    ))?)
}

fn replay_page_indices(pages: &[ReplayPage]) -> QuantResult<HashMap<&str, usize>> {
    let mut indices = HashMap::new();
    for (index, page) in pages.iter().enumerate() {
        for market_id in &page.market_ids {
            if indices.insert(market_id.as_str(), index).is_some() {
                return Err(ResearchError::DatasetBuild {
                    detail: format!("Source Slice replay pages duplicate market {market_id}"),
                }
                .into());
            }
        }
    }
    Ok(indices)
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

fn settlement_outcome(
    example: &TrainingExample,
    label: &LabelName,
    source: &FrozenSourceSlice,
) -> QuantResult<MarketOutcome> {
    let label_payout = settlement_payout(example, label)?;
    let Some(label_payout) = label_payout else {
        return Ok(MarketOutcome {
            market_id: example.market_id.clone(),
            resolved_at: None,
            yes_payout_ratio: None,
        });
    };
    let rows = source
        .prefetched
        .resolutions
        .get(&example.market_id)
        .ok_or_else(|| ResearchError::LabelResolution {
            detail: format!(
                "resolved settlement label for market {} has no frozen resolution fact",
                example.market_id
            ),
        })?;
    let [resolution] = rows.as_slice() else {
        return Err(ResearchError::LabelResolution {
            detail: format!(
                "resolved settlement label for market {} requires exactly one frozen resolution fact, got {}",
                example.market_id,
                rows.len()
            ),
        }
        .into());
    };
    let token_payout = resolution.payout_for(&example.token_id).map_err(|error| {
        ResearchError::LabelResolution {
            detail: format!(
                "resolution fact for market {} cannot resolve token {}: {error}",
                example.market_id, example.token_id
            ),
        }
    })?;
    if token_payout != label_payout {
        return Err(ResearchError::LabelResolution {
            detail: format!(
                "settlement label/fact payout differs for market {} token {}: label={}, fact={}",
                example.market_id, example.token_id, label_payout, token_payout
            ),
        }
        .into());
    }
    let yes_payout_ratio = resolution
        .payout_for(&example.selected_market.primary_token_id)
        .map_err(|error| ResearchError::LabelResolution {
            detail: format!(
                "resolution fact for market {} cannot resolve its primary token: {error}",
                example.market_id
            ),
        })?;
    let resolved_at = DateTime::from_timestamp_millis(resolution.resolved_at).ok_or_else(|| {
        ResearchError::LabelResolution {
            detail: format!(
                "resolution timestamp {} for market {} is outside chrono range",
                resolution.resolved_at, example.market_id
            ),
        }
    })?;
    if resolved_at <= example.decision_at() {
        return Err(ResearchError::LabelResolution {
            detail: format!(
                "market {} resolved at {resolved_at} no later than replay decision {}",
                example.market_id,
                example.decision_at()
            ),
        }
        .into());
    }
    Ok(MarketOutcome {
        market_id: example.market_id.clone(),
        resolved_at: Some(resolved_at),
        yes_payout_ratio: Some(yes_payout_ratio),
    })
}
