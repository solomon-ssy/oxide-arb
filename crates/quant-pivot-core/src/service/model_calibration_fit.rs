//! Model-score probability-calibrator fit orchestration (Phase 11.3 §4).
//!
//! Reuses the existing PIT backtest-replay engine
//! ([`BacktestService::run_for_calibration`]) to harvest per-sample
//! `(composite_score, settled_yes, max_adverse_excursion_bps)` triples over an
//! **independent, disjoint + embargoed** `purpose = Calibration` dataset —
//! never the model's own training spine — then fits a
//! [`ProbabilityCalibrator`] and persists the unified `CalibrationArtifact`
//! (`kind = ModelScore`).

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        CalibrationArtifactInfo, FitModelCalibratorRequest, JobProgressSink,
        ModelCalibrationFitJobParams, ModelCalibrationFitOutcome, ModelCalibrationFitPort,
        ModelCalibrationFitPreflightView, NewCalibrationArtifact, query::TimeWindow,
    },
    enums::quant::{
        CalibrationKind, CalibrationMethod, DatasetPurpose, OutcomeSide, TrainingDatasetStatus,
    },
    types::{CalibrationArtifactId, DecisionPolicySnapshotId, ModelVersionId, TrainingDatasetId},
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, ModelRegistryRepository, PolicyRepository,
    TrainingDatasetRepository,
};
use quant_pivot_research::{
    backtest::SampleOutcome,
    model::{
        IsotonicCalibrator, PlattCalibrator, ProbabilityCalibrator, ReliabilitySample,
        compute_reliability,
    },
};
use rust_decimal::Decimal;

use crate::{
    app::ports::backtest::CoreBacktestPort,
    governance::{ModelScoreCalibrationPayload, model_score_content_hash},
    service::{
        backtest::BacktestInput,
        calibration_shared::{
            assert_disjoint_from_all_training_datasets, assert_embargoed_after,
            calibration_split_hash,
        },
    },
};

/// Governed knobs the fit needs from the frozen `model.calibration` config
/// section (Phase 11.3 §7), resolved from the pinned runtime-config version at
/// fit time — deterministic on replay, mirrors the bias-table fit's
/// `frozen_fit`.
#[derive(Debug, Clone, Copy)]
struct CalibrationFitPolicy {
    min_samples_isotonic: usize,
    embargo_secs: i64,
    ci_confidence: Decimal,
}

/// Core model-score calibrator fitter.
pub struct ModelCalibrationFitService {
    /// Reused for its `backtest_service_for` assembly (frozen runtime-config
    /// replay engine) — one construction path, never duplicated.
    backtest_port: Arc<CoreBacktestPort>,
    model_registry_repo: Arc<dyn ModelRegistryRepository>,
    training_dataset_repo: Arc<dyn TrainingDatasetRepository>,
    calibration_repo: Arc<dyn CalibrationArtifactRepository>,
    runtime_config_repo: Arc<dyn PolicyRepository>,
}

impl ModelCalibrationFitService {
    #[must_use]
    pub const fn new(
        backtest_port: Arc<CoreBacktestPort>,
        model_registry_repo: Arc<dyn ModelRegistryRepository>,
        training_dataset_repo: Arc<dyn TrainingDatasetRepository>,
        calibration_repo: Arc<dyn CalibrationArtifactRepository>,
        runtime_config_repo: Arc<dyn PolicyRepository>,
    ) -> Self {
        Self {
            backtest_port,
            model_registry_repo,
            training_dataset_repo,
            calibration_repo,
            runtime_config_repo,
        }
    }

    /// Load the governed `model.calibration` policy from the pinned runtime-
    /// config version (frozen at enqueue).
    async fn frozen_policy(
        &self,
        decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    ) -> QuantResult<CalibrationFitPolicy> {
        let version = self
            .runtime_config_repo
            .load_snapshot(decision_policy_snapshot_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: "runtime config version for model calibration fit not found".to_owned(),
                })
            })?;
        let config = version.snapshot;
        let calibration = config.model_routing.model.calibration;
        let ci_confidence = calibration.ci_confidence.value;
        Ok(CalibrationFitPolicy {
            min_samples_isotonic: usize::try_from(calibration.min_samples_isotonic).map_err(
                |error| ResearchError::DatasetBuild {
                    detail: format!(
                        "model.calibration.min_samples_isotonic exceeds platform usize: {error}"
                    ),
                },
            )?,
            embargo_secs: i64::try_from(calibration.embargo_secs).map_err(|error| {
                ResearchError::DatasetBuild {
                    detail: format!(
                        "model.calibration.embargo_secs does not fit chrono seconds: {error}"
                    ),
                }
            })?,
            ci_confidence,
        })
    }

    /// Governed embargo gap from the *current* runtime-config version — used
    /// by the read-only preflight check, which (unlike `fit`) has no pinned
    /// `decision_policy_snapshot_id` to freeze against yet (no job has been
    /// enqueued). The actual fit still freezes whatever version is current at
    /// enqueue time, so this is a consistent, good-enough preflight estimate.
    async fn current_embargo_secs(&self) -> QuantResult<i64> {
        let version = self
            .runtime_config_repo
            .load_current()
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: "no current runtime config version".to_owned(),
                })
            })?;
        let config = version.snapshot;
        i64::try_from(config.model_routing.model.calibration.embargo_secs).map_err(|error| {
            ResearchError::DatasetBuild {
                detail: format!(
                    "model.calibration.embargo_secs does not fit chrono seconds: {error}"
                ),
            }
            .into()
        })
    }

    /// Validate the calibration dataset's purpose/spec-match and its
    /// disjoint + embargoed relationship to the target model's own training
    /// dataset (Phase 11.3 §0/§4 — the `WalkForwardSplit`-with-embargo purge
    /// primitive). Fail-closed on any mismatch.
    ///
    /// Returns the **catalog** calibration window (`calibration_dataset.window_start/end`) —
    /// the exact window this function verified disjoint + embargoed. `fit()`
    /// persists this same window as `fit_window`, never a window re-derived
    /// from which samples happened to materialize (that could be a strict
    /// subset of the verified window, e.g. under sparse market activity,
    /// silently decoupling the persisted provenance from what was actually
    /// checked).
    async fn validate_split(
        &self,
        model_version_id: &ModelVersionId,
        request: &FitModelCalibratorRequest,
        policy: &CalibrationFitPolicy,
    ) -> QuantResult<TimeWindow> {
        let version = self
            .model_registry_repo
            .find_model_version_by_id(model_version_id)
            .await?
            .ok_or_else(|| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: format!("model version `{model_version_id}` not found"),
                })
            })?;
        let calibration_dataset = self
            .training_dataset_repo
            .find_by_id(&request.calibration_dataset_id)
            .await?
            .ok_or_else(|| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: format!(
                        "calibration dataset `{}` not found",
                        request.calibration_dataset_id
                    ),
                })
            })?;
        if calibration_dataset.purpose != DatasetPurpose::Calibration {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: format!(
                    "dataset `{}` has purpose `{}`, expected `calibration` — a model \
                     calibrator must never fit on a `training` dataset",
                    calibration_dataset.training_dataset_id,
                    calibration_dataset.purpose.as_str()
                ),
            }));
        }
        if calibration_dataset.status != TrainingDatasetStatus::Ready {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: format!(
                    "calibration requires a Ready dataset, got {}",
                    calibration_dataset.status.as_str()
                ),
            }));
        }
        if calibration_dataset.model_spec_id != version.model_spec_id {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: "calibration dataset model_spec_id does not match the target model version"
                    .to_owned(),
            }));
        }
        let calibration_window = TimeWindow::new(
            calibration_dataset.window_start,
            calibration_dataset.window_end,
        );
        // Purge: disjoint from every Ready training dataset in the system.
        assert_disjoint_from_all_training_datasets(
            self.training_dataset_repo.as_ref(),
            &calibration_window,
            "model calibration fit",
        )
        .await?;
        // Embargo: additionally must start at/after the target model's own
        // training-dataset window end + the governed embargo gap.
        if let Some(training_dataset_id) = &version.training_dataset_id {
            let training_dataset = self
                .training_dataset_repo
                .find_by_id(training_dataset_id)
                .await?
                .ok_or_else(|| {
                    QuantError::from(ResearchError::DatasetBuild {
                        detail: format!("training dataset `{training_dataset_id}` not found"),
                    })
                })?;
            if training_dataset.status != TrainingDatasetStatus::Ready {
                return Err(QuantError::from(ResearchError::DatasetBuild {
                    detail: format!(
                        "target model training dataset must remain Ready, got {}",
                        training_dataset.status.as_str()
                    ),
                }));
            }
            let training_window =
                TimeWindow::new(training_dataset.window_start, training_dataset.window_end);
            assert_embargoed_after(
                &calibration_window,
                &training_window,
                policy.embargo_secs,
                "model calibration fit",
            )?;
        }
        Ok(calibration_window)
    }
}

#[async_trait]
impl ModelCalibrationFitPort for ModelCalibrationFitService {
    async fn fit(
        &self,
        params: ModelCalibrationFitJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<ModelCalibrationFitOutcome> {
        let ModelCalibrationFitJobParams {
            request,
            decision_policy_snapshot_id,
        } = params;
        let policy = self.frozen_policy(&decision_policy_snapshot_id).await?;
        let fit_window = self
            .validate_split(&request.model_version_id, &request, &policy)
            .await?;
        let samples = self
            .harvest_calibration_samples(
                &request,
                &decision_policy_snapshot_id,
                Arc::clone(&progress),
                cancel,
            )
            .await?;
        self.fit_and_persist(&request, &policy, &fit_window, &samples)
            .await
    }

    async fn preflight(
        &self,
        model_version_id: &ModelVersionId,
        calibration_dataset_id: &TrainingDatasetId,
    ) -> QuantResult<ModelCalibrationFitPreflightView> {
        let mut messages = Vec::new();

        let version = self
            .model_registry_repo
            .find_model_version_by_id(model_version_id)
            .await?
            .ok_or_else(|| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: format!("model version `{model_version_id}` not found"),
                })
            })?;
        let calibration_dataset = self
            .training_dataset_repo
            .find_by_id(calibration_dataset_id)
            .await?
            .ok_or_else(|| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: format!("calibration dataset `{calibration_dataset_id}` not found"),
                })
            })?;

        if calibration_dataset.purpose != DatasetPurpose::Calibration {
            messages.push(format!(
                "dataset `{calibration_dataset_id}` has purpose `{}`, expected `calibration` \
                 — a model calibrator must never fit on a `training` dataset",
                calibration_dataset.purpose.as_str()
            ));
        }
        if calibration_dataset.model_spec_id != version.model_spec_id {
            messages.push(
                "calibration dataset model_spec_id does not match the target model version"
                    .to_owned(),
            );
        }

        let calibration_window = TimeWindow::new(
            calibration_dataset.window_start,
            calibration_dataset.window_end,
        );

        let disjoint_ok = match assert_disjoint_from_all_training_datasets(
            self.training_dataset_repo.as_ref(),
            &calibration_window,
            "model calibration fit preflight",
        )
        .await
        {
            Ok(()) => true,
            Err(error) => {
                messages.push(error.to_string());
                false
            }
        };

        let mut training_window_start = None;
        let mut training_window_end = None;
        let mut required_start = None;
        let mut embargo_ok = true;
        if let Some(training_dataset_id) = &version.training_dataset_id {
            if let Some(training_dataset) = self
                .training_dataset_repo
                .find_by_id(training_dataset_id)
                .await?
            {
                let training_window =
                    TimeWindow::new(training_dataset.window_start, training_dataset.window_end);
                training_window_start = Some(training_window.from);
                training_window_end = Some(training_window.to);
                let embargo_secs = self.current_embargo_secs().await?;
                required_start =
                    Some(training_window.to + chrono::Duration::seconds(embargo_secs.max(0)));
                if let Err(error) = assert_embargoed_after(
                    &calibration_window,
                    &training_window,
                    embargo_secs,
                    "model calibration fit preflight",
                ) {
                    messages.push(error.to_string());
                    embargo_ok = false;
                }
            } else {
                messages.push(format!(
                    "training dataset `{training_dataset_id}` not found"
                ));
                embargo_ok = false;
            }
        }

        Ok(ModelCalibrationFitPreflightView {
            disjoint_ok,
            embargo_ok,
            calibration_window_start: calibration_window.from,
            calibration_window_end: calibration_window.to,
            training_window_start,
            training_window_end,
            required_start,
            messages,
        })
    }
}

impl ModelCalibrationFitService {
    async fn harvest_calibration_samples(
        &self,
        request: &FitModelCalibratorRequest,
        decision_policy_snapshot_id: &DecisionPolicySnapshotId,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<Vec<SampleOutcome>> {
        // Reuse the PIT backtest replay engine to harvest (score, outcome, MAE)
        // triples — the same computation graph the live plane scores, never a
        // bespoke re-derivation.
        let backtest = self
            .backtest_port
            .backtest_service_for(decision_policy_snapshot_id)
            .await?;
        let (_report, samples) = backtest
            .run_for_calibration(
                BacktestInput {
                    model_version_id: request.model_version_id.clone(),
                    training_dataset_id: request.calibration_dataset_id.clone(),
                    decision_policy_snapshot_id: decision_policy_snapshot_id.clone(),
                    calibrate: false,
                    backtest_report_id: None,
                },
                progress,
                cancel,
            )
            .await?;
        Ok(samples)
    }

    async fn fit_and_persist(
        &self,
        request: &FitModelCalibratorRequest,
        policy: &CalibrationFitPolicy,
        fit_window: &TimeWindow,
        samples: &[SampleOutcome],
    ) -> QuantResult<ModelCalibrationFitOutcome> {
        let min_samples = match request.method {
            CalibrationMethod::Isotonic => policy.min_samples_isotonic,
            CalibrationMethod::Platt => 10,
        };
        if samples.len() < min_samples {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: format!(
                    "calibration split has {} samples, below the {} floor for method `{}`",
                    samples.len(),
                    min_samples,
                    request.method.as_str()
                ),
            }));
        }

        let scores: Vec<Decimal> = samples.iter().map(|s| s.composite_score.inner()).collect();
        let outcomes: Vec<bool> = samples.iter().map(sample_won).collect();

        let mapping = match request.method {
            CalibrationMethod::Isotonic => {
                IsotonicCalibrator::new(policy.min_samples_isotonic).fit(&scores, &outcomes)?
            }
            CalibrationMethod::Platt => PlattCalibrator.fit(&scores, &outcomes)?,
        };

        let reliability_samples: Vec<ReliabilitySample> = samples
            .iter()
            .zip(&outcomes)
            .map(|(sample, &won)| ReliabilitySample {
                score: sample.composite_score.inner(),
                won,
                max_adverse_excursion_bps: sample.max_adverse_excursion_bps,
            })
            .collect();
        let reliability =
            compute_reliability(&mapping, &reliability_samples, policy.ci_confidence)?;

        let split_hash = calibration_split_hash(
            fit_window,
            samples
                .iter()
                .map(|s| (s.market_id.to_string(), s.decision_at)),
        )?;

        let payload = ModelScoreCalibrationPayload {
            model_version_id: request.model_version_id.clone(),
            calibration_dataset_id: request.calibration_dataset_id.clone(),
            mapping,
            reliability,
        };
        let payload_json = serde_json::to_value(&payload).map_err(|error| {
            QuantError::from(ResearchError::DatasetBuild {
                detail: format!("calibration payload serialization failed: {error}"),
            })
        })?;
        // Self-contained hash (fit_window + split_hash + the full,
        // provenance-carrying payload) — recomputable by the loader from the
        // persisted row alone, symmetric with `market_price_bias`.
        let content_hash = model_score_content_hash(fit_window, &split_hash, &payload)?;

        let sample_count =
            i64::try_from(samples.len()).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("calibration sample count exceeds Postgres bigint: {error}"),
            })?;
        let outcome_sample_count =
            u64::try_from(samples.len()).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("calibration sample count exceeds u64: {error}"),
            })?;
        let artifact_id = CalibrationArtifactId::from_v7();
        let created = self
            .calibration_repo
            .create(NewCalibrationArtifact {
                artifact_id: artifact_id.clone(),
                kind: CalibrationKind::ModelScore,
                content_hash,
                calibration_split_hash: split_hash,
                fit_window_start: fit_window.from,
                fit_window_end: fit_window.to,
                sample_count,
                payload_json,
                active: false,
            })
            .await
            .map_err(QuantError::from)?;

        let _: CalibrationArtifactInfo = created;
        Ok(ModelCalibrationFitOutcome {
            artifact_id: Some(artifact_id),
            sample_count: outcome_sample_count,
        })
    }
}

/// Whether the bought side won: `Yes` bets win iff `settled_yes`, `No` bets
/// win iff `!settled_yes`.
const fn sample_won(sample: &SampleOutcome) -> bool {
    match sample.outcome_side {
        OutcomeSide::Yes => sample.settled_yes,
        OutcomeSide::No => !sample.settled_yes,
    }
}
