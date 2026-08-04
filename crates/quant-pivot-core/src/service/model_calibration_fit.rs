//! Model-score probability-calibrator fit orchestration.
//!
//! Reuses the existing PIT backtest-replay engine
//! (`BacktestService::run_for_calibration`) to harvest per-sample
//! `(composite_score, token_payout_ratio, max_adverse_excursion_bps)` triples over an
//! **independent, disjoint + embargoed** `purpose = Calibration` dataset —
//! never the model's own training spine — then fits a
//! [`ProbabilityCalibrator`] and persists the unified `CalibrationArtifact`
//! (`kind = ModelScore`). The current calibrators are Bernoulli estimators, so
//! fractional split-payout samples are explicitly excluded rather than coerced.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Duration;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        api::{FitModelCalibratorRequest, ModelCalibrationFitPreflightView},
        ports::{
            ModelCalibrationFitJobParams, ModelCalibrationFitOutcome, ModelCalibrationFitPort,
        },
        quant::{
            CalibrationArtifactPayload, JobProgressSink, ModelRunInfo, ModelScoreCalibrationCommit,
            NewCalibrationArtifact,
        },
        query::TimeWindow,
    },
    enums::quant::{
        CalibrationKind, CalibrationMethod, DatasetPurpose, DownsideSource, ModelRunErrorCode,
        ModelRunKind, ModelRunStatus, TrainingDatasetStatus,
    },
    hashing::CanonicalDigest,
    types::{
        CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId, ModelRunId, ModelVersionId,
        PayoutRatio, TrainingDatasetId,
        calibration::{
            MODEL_SCORE_CALIBRATION_FORMAT_VERSION, ModelScoreCalibrationFitContract,
            ModelScoreCalibrationPayload,
        },
        model_serving::ModelServingPolicySnapshotBinding,
    },
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, ModelRegistryRepository, ModelRunRepository, PolicyRepository,
    TrainingDatasetRepository,
};
use quant_pivot_research::{
    backtest::ModelCalibrationOutcome,
    model::{
        IsotonicCalibrator, PlattCalibrator, ProbabilityCalibrator, ReliabilitySample,
        compute_reliability,
    },
};
use rust_decimal::Decimal;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::{
    app::ports::backtest::CoreBacktestPort,
    governance::{model_score_content_hash, policy_snapshot::VerifiedPolicySnapshotBinding},
    service::{
        backtest::{CalibrationReplayEvidence, CalibrationReplayInput},
        calibration_shared::{
            CalibrationSampleKey, assert_dataset_disjoint, assert_embargoed_after,
            calibration_split_hash,
        },
    },
};

const INSUFFICIENT_CALIBRATION_DOMAIN: &str = "quant-pivot/model-score-calibration-insufficient";
const INSUFFICIENT_CALIBRATION_VERSION: u32 = 1;

/// Governed knobs the fit needs from the frozen `model.calibration` config
/// section, resolved from the pinned runtime-config version at
/// fit time — deterministic on replay, mirrors the bias-table fit's
/// `frozen_fit`.
#[derive(Debug, Clone, Copy)]
struct CalibrationFitPolicy {
    min_samples_isotonic: usize,
    embargo_secs: i64,
    ci_confidence: Decimal,
}

#[derive(Serialize)]
struct InsufficientCalibrationCommitment<'a> {
    fit_contract: &'a ModelScoreCalibrationFitContract,
    fit_window: TimeWindow,
    method: CalibrationMethod,
    calibration_split_hash: ContentHash,
    sample_count: u64,
    total_sample_count: u64,
    minimum_sample_count: u64,
}

struct CalibrationFitSamples<'a> {
    binary: Vec<&'a ModelCalibrationOutcome>,
    split_hash: ContentHash,
    sample_count: u64,
    total_sample_count: u64,
    minimum_sample_count: u64,
}

impl<'a> CalibrationFitSamples<'a> {
    fn new(
        request: &FitModelCalibratorRequest,
        policy: &CalibrationFitPolicy,
        evidence: &'a CalibrationReplayEvidence,
    ) -> QuantResult<Self> {
        let binary = evidence
            .samples
            .iter()
            .filter(|sample| {
                sample.token_payout_ratio == PayoutRatio::ZERO
                    || sample.token_payout_ratio == PayoutRatio::ONE
            })
            .collect::<Vec<_>>();
        let minimum = match request.method {
            CalibrationMethod::Isotonic => policy.min_samples_isotonic,
            CalibrationMethod::Platt => 10,
        };
        let sample_count =
            u64::try_from(binary.len()).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("calibration sample count exceeds u64: {error}"),
            })?;
        let total_sample_count =
            u64::try_from(evidence.samples.len()).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("total calibration sample count exceeds u64: {error}"),
            })?;
        let minimum_sample_count =
            u64::try_from(minimum).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("calibration sample floor exceeds u64: {error}"),
            })?;
        let split_hash = calibration_split_hash(
            &evidence.fit_window,
            binary.iter().map(|sample| {
                CalibrationSampleKey::for_instrument(
                    sample.market_id.clone(),
                    sample.token_id.clone(),
                    sample.decision_at,
                )
            }),
        )?;
        Ok(Self {
            binary,
            split_hash,
            sample_count,
            total_sample_count,
            minimum_sample_count,
        })
    }

    const fn is_insufficient(&self) -> bool {
        self.sample_count < self.minimum_sample_count
    }

    fn validate_downside_support(&self, downside_source: DownsideSource) -> QuantResult<()> {
        match downside_source {
            DownsideSource::MfeMae => {
                let missing = self
                    .binary
                    .iter()
                    .filter(|sample| sample.max_adverse_excursion_bps.is_none())
                    .count();
                let positive = self
                    .binary
                    .iter()
                    .filter(|sample| {
                        sample
                            .max_adverse_excursion_bps
                            .is_some_and(|value| value > Decimal::ZERO)
                    })
                    .count();
                if missing > 0 || positive > 0 {
                    return Err(ResearchError::ValidationMethodology {
                        detail: format!(
                            "MfeMae calibration requires one non-positive frozen MAE observation per binary sample; missing={missing}, positive={positive}, total={}",
                            self.binary.len()
                        ),
                    }
                    .into());
                }
            }
        }
        Ok(())
    }

    fn insufficient_hash(
        &self,
        evidence: &CalibrationReplayEvidence,
        method: CalibrationMethod,
    ) -> QuantResult<ContentHash> {
        CanonicalDigest::content_hash_typed(
            INSUFFICIENT_CALIBRATION_DOMAIN,
            INSUFFICIENT_CALIBRATION_VERSION,
            &InsufficientCalibrationCommitment {
                fit_contract: &evidence.fit_contract,
                fit_window: evidence.fit_window,
                method,
                calibration_split_hash: self.split_hash,
                sample_count: self.sample_count,
                total_sample_count: self.total_sample_count,
                minimum_sample_count: self.minimum_sample_count,
            },
        )
        .map_err(Into::into)
    }

    fn fit_payload(
        &self,
        method: CalibrationMethod,
        policy: &CalibrationFitPolicy,
        fit_contract: &ModelScoreCalibrationFitContract,
    ) -> QuantResult<ModelScoreCalibrationPayload> {
        let scores = self
            .binary
            .iter()
            .map(|sample| sample.composite_score.inner())
            .collect::<Vec<_>>();
        let outcomes = self
            .binary
            .iter()
            .map(|sample| sample.token_payout_ratio == PayoutRatio::ONE)
            .collect::<Vec<_>>();
        let mapping = match method {
            CalibrationMethod::Isotonic => {
                IsotonicCalibrator::new(policy.min_samples_isotonic).fit(&scores, &outcomes)?
            }
            CalibrationMethod::Platt => PlattCalibrator.fit(&scores, &outcomes)?,
        };
        let reliability_samples = self
            .binary
            .iter()
            .zip(&outcomes)
            .map(|(sample, &won)| ReliabilitySample {
                score: sample.composite_score.inner(),
                won,
                max_adverse_excursion_bps: sample.max_adverse_excursion_bps,
            })
            .collect::<Vec<_>>();
        let reliability =
            compute_reliability(&mapping, &reliability_samples, policy.ci_confidence)?;
        let payload = ModelScoreCalibrationPayload {
            format_version: MODEL_SCORE_CALIBRATION_FORMAT_VERSION,
            fit_contract: fit_contract.clone(),
            mapping,
            reliability,
        };
        payload.validate(self.sample_count).map_err(|detail| {
            ResearchError::InvalidModelArtifact {
                detail: format!("model-score calibration payload is invalid: {detail}"),
            }
        })?;
        Ok(payload)
    }
}

struct CalibrationRunExpectation {
    model_run_id: ModelRunId,
    model_version_id: ModelVersionId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    window: TimeWindow,
    input_hash: ContentHash,
    output_hash: ContentHash,
}

impl CalibrationRunExpectation {
    const fn new(
        request: &FitModelCalibratorRequest,
        evidence: &CalibrationReplayEvidence,
        output_hash: ContentHash,
    ) -> Self {
        Self {
            model_run_id: evidence.model_run_id,
            model_version_id: request.model_version_id,
            decision_policy_snapshot_id: evidence
                .fit_contract
                .policy_snapshot
                .decision_policy_snapshot_id,
            window: evidence.fit_window,
            input_hash: evidence.fit_contract.calibration_dataset.dataset_hash,
            output_hash,
        }
    }

    fn validate(&self, terminal: &ModelRunInfo) -> QuantResult<()> {
        let identity_matches = terminal.model_run_id == self.model_run_id
            && terminal.run_kind == ModelRunKind::Calibration
            && terminal.model_version_id == Some(self.model_version_id);
        let preimage_matches = terminal.decision_policy_snapshot_id
            == self.decision_policy_snapshot_id
            && terminal.market_selection_id.is_none()
            && terminal.window_start == self.window.from
            && terminal.window_end == self.window.to
            && terminal.input_hash == self.input_hash;
        let terminal_matches = terminal.status == ModelRunStatus::Succeeded
            && terminal.output_hash == Some(self.output_hash)
            && terminal.error_code.is_none()
            && terminal.error_message.is_none()
            && terminal
                .finished_at
                .is_some_and(|finished_at| finished_at >= terminal.started_at);
        if !identity_matches || !preimage_matches || !terminal_matches {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "calibration terminal run {} differs from its exact producer preimage",
                    terminal.model_run_id
                ),
            }
            .into());
        }
        Ok(())
    }
}

/// Core model-score calibrator fitter.
pub struct ModelCalibrationFitService {
    /// Reused for its `backtest_service_for` assembly (frozen runtime-config
    /// replay engine) — one construction path, never duplicated.
    backtest_port: Arc<CoreBacktestPort>,
    model_registry_repo: Arc<dyn ModelRegistryRepository>,
    training_dataset_repo: Arc<dyn TrainingDatasetRepository>,
    calibration_repo: Arc<dyn CalibrationArtifactRepository>,
    model_run_repo: Arc<dyn ModelRunRepository>,
    runtime_config_repo: Arc<dyn PolicyRepository>,
}

impl ModelCalibrationFitService {
    #[must_use]
    pub const fn new(
        backtest_port: Arc<CoreBacktestPort>,
        model_registry_repo: Arc<dyn ModelRegistryRepository>,
        training_dataset_repo: Arc<dyn TrainingDatasetRepository>,
        calibration_repo: Arc<dyn CalibrationArtifactRepository>,
        model_run_repo: Arc<dyn ModelRunRepository>,
        runtime_config_repo: Arc<dyn PolicyRepository>,
    ) -> Self {
        Self {
            backtest_port,
            model_registry_repo,
            training_dataset_repo,
            calibration_repo,
            model_run_repo,
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
        let policy_binding = ModelServingPolicySnapshotBinding::from(
            VerifiedPolicySnapshotBinding::try_from(&version)?,
        );
        if policy_binding.decision_policy_snapshot_id != *decision_policy_snapshot_id {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "loaded calibration policy snapshot {} differs from requested snapshot \
                     {decision_policy_snapshot_id}",
                    policy_binding.decision_policy_snapshot_id
                ),
            }
            .into());
        }
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
        let _policy_binding = ModelServingPolicySnapshotBinding::from(
            VerifiedPolicySnapshotBinding::try_from(&version)?,
        );
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
    /// dataset using the `WalkForwardSplit`-with-embargo purge primitive.
    /// Fail closed on any mismatch.
    ///
    async fn validate_split(
        &self,
        model_version_id: &ModelVersionId,
        request: &FitModelCalibratorRequest,
        policy: &CalibrationFitPolicy,
    ) -> QuantResult<()> {
        let version = self
            .model_registry_repo
            .find_model_version(model_version_id)
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
        assert_dataset_disjoint(
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
        Ok(())
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
            model_run_id,
            request,
            decision_policy_snapshot_id,
            downside_source,
            actor: _,
        } = params;
        let policy = self.frozen_policy(&decision_policy_snapshot_id).await?;
        self.validate_split(&request.model_version_id, &request, &policy)
            .await?;
        let evidence = Box::pin(self.harvest_calibration_samples(
            model_run_id,
            &request,
            &decision_policy_snapshot_id,
            Arc::clone(&progress),
            cancel,
        ))
        .await?;
        let persisted =
            Box::pin(self.fit_and_persist(&request, &policy, &evidence, downside_source)).await;
        match persisted {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                let _ = self
                    .model_run_repo
                    .fail(
                        &evidence.model_run_id,
                        ModelRunErrorCode::CalibrationFailed,
                        error.to_string(),
                    )
                    .await;
                Err(error)
            }
        }
    }

    async fn preflight(
        &self,
        model_version_id: &ModelVersionId,
        calibration_dataset_id: &TrainingDatasetId,
    ) -> QuantResult<ModelCalibrationFitPreflightView> {
        let mut messages = Vec::new();

        let version = self
            .model_registry_repo
            .find_model_version(model_version_id)
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

        let disjoint_ok = match assert_dataset_disjoint(
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
                required_start = Some(training_window.to + Duration::seconds(embargo_secs.max(0)));
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
        model_run_id: ModelRunId,
        request: &FitModelCalibratorRequest,
        decision_policy_snapshot_id: &DecisionPolicySnapshotId,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<CalibrationReplayEvidence> {
        // Reuse the PIT backtest replay engine to harvest (score, outcome, MAE)
        // triples — the same computation graph the live plane scores, never a
        // bespoke re-derivation.
        let backtest = self
            .backtest_port
            .backtest_service_for(decision_policy_snapshot_id)
            .await?;
        let evidence = Box::pin(backtest.run_for_calibration(
            CalibrationReplayInput {
                model_run_id,
                source_model: request.model_version_id,
                calibration_dataset: request.calibration_dataset_id,
                policy_snapshot: *decision_policy_snapshot_id,
            },
            progress,
            cancel,
        ))
        .await?;
        Ok(evidence)
    }

    async fn fit_and_persist(
        &self,
        request: &FitModelCalibratorRequest,
        policy: &CalibrationFitPolicy,
        evidence: &CalibrationReplayEvidence,
        downside_source: DownsideSource,
    ) -> QuantResult<ModelCalibrationFitOutcome> {
        let samples = CalibrationFitSamples::new(request, policy, evidence)?;
        if samples.is_insufficient() {
            return self.commit_insufficient(request, evidence, &samples).await;
        }
        samples.validate_downside_support(downside_source)?;
        self.commit_calibrated(request, policy, evidence, &samples)
            .await
    }

    async fn commit_insufficient(
        &self,
        request: &FitModelCalibratorRequest,
        evidence: &CalibrationReplayEvidence,
        samples: &CalibrationFitSamples<'_>,
    ) -> QuantResult<ModelCalibrationFitOutcome> {
        let outcome_hash = samples.insufficient_hash(evidence, request.method)?;
        let terminal = self
            .model_run_repo
            .succeed_exact(
                &evidence.model_run_id,
                outcome_hash,
                Some(request.model_version_id),
            )
            .await?;
        CalibrationRunExpectation::new(request, evidence, outcome_hash).validate(&terminal)?;
        Ok(ModelCalibrationFitOutcome::Insufficient {
            sample_count: samples.sample_count,
            total_sample_count: samples.total_sample_count,
            minimum_sample_count: samples.minimum_sample_count,
            outcome_hash,
        })
    }

    async fn commit_calibrated(
        &self,
        request: &FitModelCalibratorRequest,
        policy: &CalibrationFitPolicy,
        evidence: &CalibrationReplayEvidence,
        samples: &CalibrationFitSamples<'_>,
    ) -> QuantResult<ModelCalibrationFitOutcome> {
        let payload = samples.fit_payload(request.method, policy, &evidence.fit_contract)?;
        let sample_count_i64 =
            i64::try_from(samples.sample_count).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("calibration sample count exceeds Postgres bigint: {error}"),
            })?;
        // Self-contained hash (fit_window + split_hash + the full,
        // provenance-carrying payload) — recomputable by the repository and
        // loader from the persisted row alone.
        let content_hash =
            model_score_content_hash(&evidence.fit_window, &samples.split_hash, &payload)?;

        let artifact_id = CalibrationArtifactId::from_v7();
        let artifact_payload = CalibrationArtifactPayload::ModelScore(Box::new(payload));
        let committed = self
            .calibration_repo
            .commit_model_score(ModelScoreCalibrationCommit {
                model_run_id: evidence.model_run_id,
                artifact: NewCalibrationArtifact {
                    artifact_id,
                    kind: CalibrationKind::ModelScore,
                    content_hash,
                    calibration_split_hash: samples.split_hash,
                    fit_window_start: evidence.fit_window.from,
                    fit_window_end: evidence.fit_window.to,
                    sample_count: sample_count_i64,
                    payload: artifact_payload.clone(),
                    active: false,
                },
            })
            .await
            .map_err(QuantError::from)?;
        let created = committed.artifact();
        let terminal = committed.model_run();

        let identity_matches = created.kind == CalibrationKind::ModelScore
            && created.content_hash == content_hash
            && !created.active;
        let evidence_matches = created.calibration_split_hash == samples.split_hash
            && created.fit_window_start == evidence.fit_window.from
            && created.fit_window_end == evidence.fit_window.to
            && created.sample_count == sample_count_i64
            && created.payload == artifact_payload;
        if !identity_matches || !evidence_matches {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "persisted model-score calibration artifact {} differs from its exact \
                     producer preimage",
                    created.artifact_id
                ),
            }
            .into());
        }
        CalibrationRunExpectation::new(request, evidence, content_hash).validate(terminal)?;
        Ok(ModelCalibrationFitOutcome::Calibrated {
            artifact_id: created.artifact_id,
            sample_count: samples.sample_count,
        })
    }
}
