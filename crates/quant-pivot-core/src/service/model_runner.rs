//! Online inference closure: the [`ModelRunner`].
//!
//! Given a persisted selection and built feature vectors, one round:
//!
//! 1. creates a `Running` `quant_model_run` (so factor values can take its FK),
//! 2. runs the factor plane ([`FactorPipelineService`]) under that run,
//! 3. loads the active model runtime (content-addressed, hash + schema verified),
//! 4. scores the eligible markets into [`SignalCandidate`]s,
//! 5. durably writes exact input evidence and its run completion marker,
//! 6. finalizes the run (`succeed` with the output hash + metrics, or `fail`), and
//! 7. emits the non-authoritative `quant_signal_candidate_event` facts **after**
//!    the run reaches a terminal Postgres state.
//!
//! A shadow model, when configured, runs as an isolated `Shadow` run whose failure
//! never affects the active result (see the inference degradation policy).
//! Active-path failures fail the run and raise a critical alert — an empty report
//! is never silently fabricated. Business callers depend only on this service
//! and `dyn QuantModelRuntime`, never on a concrete runtime type.

use std::{
    collections::{BTreeSet, HashMap},
    mem,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{QuantModelInputEventRow, QuantSignalCandidateEventRow},
    domain::{
        data_plane::DecisionBoundary,
        governance::DecisionPolicySnapshotInfo,
        quant::{ModelVersionInfo, NewModelRun, NewShadowComparison},
    },
    enums::{
        common::{AlertCategory, AlertLevel, AlertSource, MarketCategory},
        model::ModelFamily,
        quant::{ModelRunKind, ModelWeightSource, OutcomeSide},
    },
    runtime_config::BuyModelRoute,
    types::{
        ContentHash, DecisionPolicySnapshotId, FeatureVectorId, MarketId, MarketSelectionId,
        ModelRunId, ModelVersionId, Probability, SignalCandidateId, TokenId,
        shadow::ShadowComparison,
    },
};
use quant_pivot_repository::traits::{ModelRunRepository, ShadowComparisonRepository};
use quant_pivot_research::{
    factors::{FrozenReferenceQuantiles, MarketFactorOutcome},
    features::FeatureVector,
    governance::shadow::{ShadowComparisonRequest, compute_shadow_comparison},
    hashing::ResearchHasher,
    model::{
        InferenceStage, ModelRuntimeInput, ModelRuntimeOutput, QuantModelRuntime, SignalCandidate,
        canonical_business_prediction_hash, finalize_candidates, signal_candidate_event,
    },
    selection::{ModelFeatureRequirements, SelectedMarket},
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    observability::{
        alert_dispatcher::{Alert, AlertDispatcher},
        model_input_fact_writer::ModelInputEventWriter,
        serving_evidence::{FeatureEvidenceCommitment, ModelInputEvidenceBatch},
        signal_candidate_fact_writer::SignalCandidateEventWriter,
    },
    projection::inference_batch::build_runtime_input,
    service::{
        factor_pipeline::{FactorPipelineRequest, FactorPipelineService},
        model_serving_generation::{
            ModelServingGenerationRequest, ModelServingGenerationStore, ModelServingRouteSnapshot,
        },
        model_serving_registry::LoadedModelServingRuntime,
    },
};

/// Decimal places shadow-diff aggregates are rounded to.
const DIFF_SCALE: u32 = 12;

/// Consecutive classical-shadow inference failures before a critical alert is
/// raised (a misconfigured / schema-incompatible classical shadow must not fail
/// silently forever).
const SHADOW_CLASSICAL_ALERT_THRESHOLD: u32 = 3;

/// A sink for the critical alerts the active inference path raises on failure.
///
/// Abstracted so the runner is unit-testable without a live notification config.
pub trait InferenceAlertSink: Send + Sync {
    /// Raise a critical, trading-safety alert.
    fn critical(&self, title: String, body: String);
}

/// Production [`InferenceAlertSink`] backed by the operator [`AlertDispatcher`].
pub struct DispatcherAlertSink(Arc<AlertDispatcher>);

impl DispatcherAlertSink {
    /// Wrap the shared dispatcher.
    #[must_use]
    pub const fn new(dispatcher: Arc<AlertDispatcher>) -> Self {
        Self(dispatcher)
    }
}

impl InferenceAlertSink for DispatcherAlertSink {
    fn critical(&self, title: String, body: String) {
        let alert = Alert::new(
            format!("model-inference:{title}"),
            AlertLevel::Critical,
            AlertCategory::TradingSafety,
            AlertSource::ReportGenerator,
            title,
            body,
            Utc::now(),
        );
        self.0.dispatch_background(alert);
    }
}

/// Frozen inputs for one inference round.
pub struct ModelRunRequest<'a> {
    /// Config version governing this round.
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    /// The selection snapshot this round scores, when persisted.
    pub market_selection_id: Option<MarketSelectionId>,
    /// Selected markets (token ids, liquidity) for the scoring context.
    pub selection: &'a [SelectedMarket],
    /// Accepted feature vectors whose data-quality bar already passed.
    pub feature_vectors: &'a [FeatureVector],
    /// Persisted feature-vector ids, aligned 1:1 with `feature_vectors`.
    pub feature_vector_ids: &'a [FeatureVectorId],
    /// Durable commitment for the exact feature-cell rows backing
    /// `feature_vector_ids`.
    pub feature_evidence: &'a FeatureEvidenceCommitment,
    /// Exact route/runtime generation pinned before market selection.
    pub serving: &'a ModelServingRouteSnapshot,
    /// `TopN` bound for the shadow comparison overlap (the report's resolved `TopN`).
    pub top_n: usize,
    /// Sole decision/cutoff contract for this inference round.
    pub boundary: DecisionBoundary,
}

/// Frozen inputs required to resolve active model feature requirements before
/// market selection.
pub struct ActiveModelRequirementsRequest<'a> {
    /// Frozen durable policy identity and document.
    pub policy: &'a DecisionPolicySnapshotInfo,
    /// Frozen decision time used by load-time governance checks.
    pub decision_at: DateTime<Utc>,
}

/// Active model metadata and selector-facing feature requirements.
#[derive(Clone)]
pub struct ActiveModelRequirements {
    /// Exact report/model route.
    pub route: BuyModelRoute,
    /// Active production model version.
    pub model_version_id: ModelVersionId,
    /// Registry row loaded and quality-gate checked.
    pub version: ModelVersionInfo,
    /// Feature availability required before a market enters selection.
    pub model_requirements: ModelFeatureRequirements,
    /// Exact active/shadow runtime generation retained through inference.
    pub serving: ModelServingRouteSnapshot,
}

/// Shadow vs active divergence over the markets both models scored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowDiff {
    /// Mean absolute composite-score difference.
    pub mean_score_diff: Decimal,
    /// Fraction of common markets where the side disagreed.
    pub side_disagreement_rate: Decimal,
    /// Whether `mean_score_diff` exceeded the configured threshold.
    pub exceeds_threshold: bool,
}

/// Outcome of the shadow run, when one was configured.
#[derive(Debug, Clone)]
pub struct ShadowRunOutcome {
    /// Present only when a `quant_model_run` row was created for this shadow attempt.
    pub model_run_id: Option<ModelRunId>,
    /// Number of shadow candidates emitted.
    pub emitted: u32,
    /// Computed divergence, when the shadow run succeeded.
    pub diff: Option<ShadowDiff>,
    /// Failure detail, when the shadow path degraded (active is unaffected).
    pub failure: Option<String>,
}

/// Outcome of one inference round.
pub struct ModelRunOutcome {
    /// The active run id.
    pub model_run_id: ModelRunId,
    /// Exact single route represented by the durable model-run row.
    pub model_version_id: ModelVersionId,
    /// Accepted candidates (score + confidence above the configured floors),
    /// ranked, ready for the portfolio planner.
    pub accepted: Vec<SignalCandidate>,
    /// Total candidates emitted to the fact stream (accepted + audited rejects).
    pub emitted: u32,
    /// One row-level gate decision for every scored market candidate.
    pub decisions: Vec<ModelMarketDecision>,
    /// Shadow outcome, when a shadow model was configured.
    pub shadow: Option<ShadowRunOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelMarketDecision {
    pub signal_candidate_id: SignalCandidateId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub gate_passed: bool,
    pub primary_reason: Option<String>,
}

/// The successful active-path result threaded back to [`ModelRunner::run`].
struct ActiveResult {
    output_hash: ContentHash,
    accepted: Vec<SignalCandidate>,
    emitted: u32,
    decisions: Vec<ModelMarketDecision>,
    /// The active model version that produced this result (for the shadow record).
    active_version_id: ModelVersionId,
    /// Per-market `(composite_score, outcome_side)` for the shadow diff.
    active_index: HashMap<MarketId, (Probability, OutcomeSide)>,
    /// Full ranked active candidates for the signal-layer shadow comparison.
    active_candidates: Vec<SignalCandidate>,
    /// Projected signal rows; written only after the active run succeeds in PG.
    ch_rows: Vec<QuantSignalCandidateEventRow>,
    /// Exact model inputs; durably committed before the active run may succeed.
    model_input_rows: Vec<QuantModelInputEventRow>,
}

pub(crate) struct AlignedFeatureCrossSection {
    pub(crate) markets: Vec<SelectedMarket>,
    pub(crate) vectors: Arc<[FeatureVector]>,
    pub(crate) vector_ids: Vec<FeatureVectorId>,
}

/// Boot-time dependencies for the [`ModelRunner`].
pub struct ModelRunnerDeps {
    /// Model-run ledger (create / finalize live + shadow runs).
    pub model_run_repo: Arc<dyn ModelRunRepository>,
    /// Shadow-comparison ledger (signal-layer divergence persistence).
    pub shadow_comparison_repo: Arc<dyn ShadowComparisonRepository>,
    /// Sole atomic owner of validated active/shadow/category generations.
    pub serving_generations: Arc<ModelServingGenerationStore>,
    /// Online factor build pipeline.
    pub factor_pipeline: Arc<FactorPipelineService>,
    /// `ClickHouse` signal-candidate fact writer.
    pub signal_writer: Arc<SignalCandidateEventWriter>,
    /// `ClickHouse` exact model-input evidence writer.
    pub model_input_writer: Arc<ModelInputEventWriter>,
    /// Operator alert sink for inference degradation.
    pub alerts: Arc<dyn InferenceAlertSink>,
}

/// Online inference orchestrator.
pub struct ModelRunner {
    model_run_repo: Arc<dyn ModelRunRepository>,
    shadow_comparison_repo: Arc<dyn ShadowComparisonRepository>,
    serving_generations: Arc<ModelServingGenerationStore>,
    factor_pipeline: Arc<FactorPipelineService>,
    signal_writer: Arc<SignalCandidateEventWriter>,
    model_input_writer: Arc<ModelInputEventWriter>,
    alerts: Arc<dyn InferenceAlertSink>,
    /// Consecutive classical-shadow inference failures (reset on any success).
    shadow_classical_failures: AtomicU32,
}

impl ModelRunner {
    /// Wire the runner from boot-time dependencies.
    #[must_use]
    pub fn new(deps: ModelRunnerDeps) -> Self {
        Self {
            model_run_repo: deps.model_run_repo,
            shadow_comparison_repo: deps.shadow_comparison_repo,
            serving_generations: deps.serving_generations,
            factor_pipeline: deps.factor_pipeline,
            signal_writer: deps.signal_writer,
            model_input_writer: deps.model_input_writer,
            alerts: deps.alerts,
            shadow_classical_failures: AtomicU32::new(0),
        }
    }

    /// Run one live-inference round.
    ///
    /// # Errors
    ///
    /// On any active-path failure the run is finalized `Failed`, a critical alert
    /// is raised, and the error is returned. A shadow failure is isolated and
    /// reported in [`ModelRunOutcome::shadow`], never as an error.
    pub async fn run(&self, request: ModelRunRequest<'_>) -> QuantResult<ModelRunOutcome> {
        request
            .serving
            .ensure_policy(request.decision_policy_snapshot_id)?;
        request.serving.validate_active()?;
        ensure_route_membership(request.serving.route(), request.selection)?;
        let run_version_id = request.serving.champion_model_version_id();
        let run_serving = request.serving.active_runtime();

        let model_run_id = ModelRunId::from_v7();
        let input_hash = input_hash(
            &request,
            "live_inference",
            &run_serving.contract_hash(),
            Some(&run_version_id),
        )?;
        self.create_run(
            &model_run_id,
            ModelRunKind::LiveInference,
            Some(run_version_id),
            &request,
            input_hash,
        )
        .await?;

        match Box::pin(self.run_active(&model_run_id, &request, run_serving, &run_version_id)).await
        {
            Ok(mut active) => {
                let model_input_rows = mem::take(&mut active.model_input_rows);
                if let Err(error) = self
                    .model_input_writer
                    .commit_run(
                        &model_run_id,
                        &request.boundary,
                        request.feature_evidence,
                        model_input_rows,
                    )
                    .await
                {
                    return Err(finalize_active_failure(
                        &self.model_run_repo,
                        &self.alerts,
                        &model_run_id,
                        InferenceStage::ActiveInference,
                        error,
                    )
                    .await);
                }
                self.model_run_repo
                    .succeed(&model_run_id, active.output_hash, Some(run_version_id))
                    .await?;
                let shadow = Box::pin(self.run_shadow(&request, &active)).await;
                self.signal_writer.write_batch(active.ch_rows);
                Ok(ModelRunOutcome {
                    model_run_id,
                    model_version_id: run_version_id,
                    accepted: active.accepted,
                    emitted: active.emitted,
                    decisions: active.decisions,
                    shadow,
                })
            }
            Err((stage, error)) => Err(finalize_active_failure(
                &self.model_run_repo,
                &self.alerts,
                &model_run_id,
                stage,
                error,
            )
            .await),
        }
    }

    /// Resolve, governance-check, and load the active runtime only far enough to
    /// expose its required features before market selection.
    ///
    /// This does not create a model-run row and does not emit facts; it is a
    /// deterministic precondition for the report builder's selection step.
    pub async fn active_requirements(
        &self,
        request: ActiveModelRequirementsRequest<'_>,
    ) -> QuantResult<ActiveModelRequirements> {
        let serving = self
            .serving_generations
            .resolve_route(ModelServingGenerationRequest::from(request.policy))
            .await?;
        serving.validate_active()?;
        let route = serving.route();
        let version = serving.active_version().clone();
        let model_requirements = serving.model_requirements();

        Ok(ActiveModelRequirements {
            route,
            model_version_id: version.model_version_id,
            version,
            model_requirements,
            serving,
        })
    }

    /// The active path: exact route → family dispatch → infer → emit.
    async fn run_active(
        &self,
        model_run_id: &ModelRunId,
        request: &ModelRunRequest<'_>,
        loaded: &LoadedModelServingRuntime,
        run_version_id: &ModelVersionId,
    ) -> Result<ActiveResult, (InferenceStage, QuantError)> {
        let aligned = align_feature_cross_section(request)
            .map_err(|error| (InferenceStage::ActiveInference, error))?;
        let mut output = self
            .infer_active(model_run_id, request, loaded, &aligned)
            .await?;
        finalize_candidates(&mut output.candidates)
            .map_err(|error| (InferenceStage::ActiveInference, error))?;

        let output_hash = canonical_business_prediction_hash(&output.candidates)
            .map_err(|error| (InferenceStage::ActiveInference, error))?;
        let active_index: HashMap<MarketId, (Probability, OutcomeSide)> = output
            .candidates
            .iter()
            .map(|candidate| {
                (
                    candidate.market_id.clone(),
                    (candidate.composite_score, candidate.outcome_side),
                )
            })
            .collect();

        let floor = request.serving.candidate_score_floor();
        let min_confidence = request.serving.min_model_confidence();
        let event_time = Utc::now().timestamp_millis();
        let model_input_rows =
            ModelInputEvidenceBatch::try_new(aligned.vectors.as_ref(), &aligned.vector_ids)
                .and_then(|batch| {
                    batch.project(
                        model_run_id,
                        &request.boundary,
                        &output.input_audit,
                        event_time,
                    )
                })
                .map_err(|error| (InferenceStage::ActiveInference, error))?;

        let emitted = u32::try_from(output.candidates.len()).map_err(|error| {
            (
                InferenceStage::ActiveInference,
                QuantError::from(ResearchError::Inference {
                    detail: format!("candidate count does not fit u32: {error}"),
                }),
            )
        })?;
        // The full ranked candidate set feeds the signal-layer shadow comparison.
        let active_candidates = output.candidates.clone();
        let (rows, accepted, decisions) =
            partition_candidates(output.candidates, floor, min_confidence, event_time);
        Ok(ActiveResult {
            output_hash,
            accepted,
            emitted,
            decisions,
            active_version_id: *run_version_id,
            active_index,
            active_candidates,
            ch_rows: rows,
            model_input_rows,
        })
    }

    /// The isolated shadow path never fails the active round.
    async fn run_shadow(
        &self,
        request: &ModelRunRequest<'_>,
        active: &ActiveResult,
    ) -> Option<ShadowRunOutcome> {
        let (shadow_version, loaded) = request.serving.shadow()?;
        let shadow_version_id = shadow_version.model_version_id;
        if let Err(error) = ModelServingRouteSnapshot::validate_shadow(shadow_version) {
            return Some(
                finalize_shadow_failure(
                    &self.model_run_repo,
                    None,
                    InferenceStage::ShadowLoad,
                    error,
                )
                .await,
            );
        }

        let model_run_id = ModelRunId::from_v7();
        let input_hash = match input_hash(
            request,
            "shadow",
            &loaded.contract_hash(),
            Some(&shadow_version_id),
        ) {
            Ok(hash) => hash,
            Err(error) => {
                return Some(
                    finalize_shadow_failure(
                        &self.model_run_repo,
                        None,
                        InferenceStage::ShadowLoad,
                        error,
                    )
                    .await,
                );
            }
        };
        if let Err(error) = self
            .create_run(
                &model_run_id,
                ModelRunKind::Shadow,
                Some(shadow_version_id),
                request,
                input_hash,
            )
            .await
        {
            return Some(
                finalize_shadow_failure(
                    &self.model_run_repo,
                    None,
                    InferenceStage::ShadowLoad,
                    error,
                )
                .await,
            );
        }

        match self
            .run_shadow_inference(&model_run_id, request, &shadow_version_id, loaded, active)
            .await
        {
            Ok(outcome) => Some(outcome),
            Err((stage, error)) => Some(
                finalize_shadow_failure(&self.model_run_repo, Some(&model_run_id), stage, error)
                    .await,
            ),
        }
    }

    /// Load + score the shadow model and finalize its run on success.
    async fn run_shadow_inference(
        &self,
        model_run_id: &ModelRunId,
        request: &ModelRunRequest<'_>,
        shadow_version_id: &ModelVersionId,
        loaded: &LoadedModelServingRuntime,
        active: &ActiveResult,
    ) -> Result<ShadowRunOutcome, (InferenceStage, QuantError)> {
        let runtime = loaded.runtime();
        let (aligned, mut output) = self
            .score_shadow_cross_section(loaded, model_run_id, request)
            .await?;
        finalize_candidates(&mut output.candidates)
            .map_err(|error| (InferenceStage::ShadowInference, error))?;

        let output_hash = canonical_business_prediction_hash(&output.candidates)
            .map_err(|error| (InferenceStage::ShadowInference, error))?;
        let threshold = request.serving.shadow_diff_threshold();
        let diff = shadow_diff(&active.active_index, &output.candidates, threshold);

        let event_time = Utc::now().timestamp_millis();
        let model_input_rows =
            ModelInputEvidenceBatch::try_new(aligned.vectors.as_ref(), &aligned.vector_ids)
                .and_then(|batch| {
                    batch.project(
                        model_run_id,
                        &request.boundary,
                        &output.input_audit,
                        event_time,
                    )
                })
                .map_err(|error| (InferenceStage::ShadowInference, error))?;
        let emitted = u32::try_from(output.candidates.len()).map_err(|error| {
            (
                InferenceStage::ShadowInference,
                QuantError::from(ResearchError::Inference {
                    detail: format!("shadow candidate count does not fit u32: {error}"),
                }),
            )
        })?;
        let rows: Vec<_> = output
            .candidates
            .iter()
            .map(|candidate| signal_candidate_event(candidate, "", event_time))
            .collect();
        let weight_source = runtime.weight_source();

        self.model_input_writer
            .commit_run(
                model_run_id,
                &request.boundary,
                request.feature_evidence,
                model_input_rows,
            )
            .await
            .map_err(|error| (InferenceStage::ShadowInference, error))?;
        // The comparison is governed promotion evidence, so it must be durable
        // before the shadow run may succeed. Failure remains isolated from the
        // active result, but the shadow attempt is terminally failed instead of
        // manufacturing a successful observation.
        self.persist_shadow_comparison(
            request,
            active,
            shadow_version_id,
            weight_source,
            &output.candidates,
        )
        .await
        .map_err(|error| (InferenceStage::ShadowInference, error))?;

        self.model_run_repo
            .succeed(model_run_id, output_hash, Some(*shadow_version_id))
            .await
            .map_err(|error| (InferenceStage::ShadowInference, QuantError::from(error)))?;
        self.signal_writer.write_batch(rows);

        Ok(ShadowRunOutcome {
            model_run_id: Some(*model_run_id),
            emitted,
            diff: Some(diff),
            failure: None,
        })
    }

    async fn score_shadow_cross_section(
        &self,
        loaded: &LoadedModelServingRuntime,
        model_run_id: &ModelRunId,
        request: &ModelRunRequest<'_>,
    ) -> Result<(AlignedFeatureCrossSection, ModelRuntimeOutput), (InferenceStage, QuantError)>
    {
        let runtime = loaded.runtime();
        let aligned = align_feature_cross_section(request)
            .map_err(|error| (InferenceStage::ShadowInference, error))?;
        let outcomes = if runtime.model_family() == ModelFamily::WeightedFactor {
            let references = weighted_references(runtime.as_ref())
                .map_err(|error| (InferenceStage::ShadowLoad, error))?;
            let factor_execution = loaded.factor_execution().ok_or_else(|| {
                (
                    InferenceStage::ShadowLoad,
                    QuantError::from(ResearchError::InvalidModelArtifact {
                        detail: format!(
                            "weighted runtime {} has no verified factor execution plane",
                            runtime.model_version_id()
                        ),
                    }),
                )
            })?;
            let result = self
                .factor_pipeline
                .run_with_references(
                    FactorPipelineRequest {
                        model_run_id,
                        vectors: Arc::clone(&aligned.vectors),
                        feature_vector_ids: &aligned.vector_ids,
                        factor_execution,
                    },
                    references,
                )
                .await
                .map_err(|error| (InferenceStage::ShadowInference, error))?;
            order_factor_outcomes(&aligned.markets, result.outcomes)
                .map_err(|error| (InferenceStage::ShadowInference, error))?
        } else {
            Vec::new()
        };
        let input = build_runtime_input(
            runtime.as_ref(),
            model_run_id,
            request.boundary.decision_at(),
            &aligned.markets,
            &aligned.vectors,
            &outcomes,
        );
        let output = self
            .score_shadow(
                runtime.as_ref(),
                input,
                runtime.model_family().is_classical(),
            )
            .await?;
        Ok((aligned, output))
    }

    /// Score the shadow runtime, tracking consecutive classical failures and
    /// escalating to a critical alert past the threshold (a misconfigured
    /// classical shadow must not degrade silently forever).
    async fn score_shadow(
        &self,
        runtime: &dyn QuantModelRuntime,
        input: ModelRuntimeInput,
        is_classical: bool,
    ) -> Result<ModelRuntimeOutput, (InferenceStage, QuantError)> {
        match runtime.infer_batch(input).await {
            Ok(output) => {
                self.shadow_classical_failures.store(0, Ordering::Relaxed);
                Ok(output)
            }
            Err(error) => {
                if is_classical {
                    let count = self
                        .shadow_classical_failures
                        .fetch_add(1, Ordering::Relaxed)
                        + 1;
                    if count >= SHADOW_CLASSICAL_ALERT_THRESHOLD {
                        self.alerts.critical(
                            format!("classical shadow inference failing ({count}x consecutive)"),
                            error.to_string(),
                        );
                    }
                }
                Err((InferenceStage::ShadowInference, error))
            }
        }
    }

    async fn infer_active(
        &self,
        model_run_id: &ModelRunId,
        request: &ModelRunRequest<'_>,
        loaded: &LoadedModelServingRuntime,
        batch: &AlignedFeatureCrossSection,
    ) -> Result<ModelRuntimeOutput, (InferenceStage, QuantError)> {
        let runtime = loaded.runtime();
        let outcomes = if runtime.model_family() == ModelFamily::WeightedFactor {
            let references = weighted_references(runtime.as_ref())
                .map_err(|error| (InferenceStage::ActiveLoad, error))?;
            let factor_execution = loaded.factor_execution().ok_or_else(|| {
                (
                    InferenceStage::ActiveLoad,
                    QuantError::from(ResearchError::InvalidModelArtifact {
                        detail: format!(
                            "weighted runtime {} has no verified factor execution plane",
                            runtime.model_version_id()
                        ),
                    }),
                )
            })?;
            let result = self
                .factor_pipeline
                .run_with_references(
                    FactorPipelineRequest {
                        model_run_id,
                        vectors: Arc::clone(&batch.vectors),
                        feature_vector_ids: &batch.vector_ids,
                        factor_execution,
                    },
                    references,
                )
                .await
                .map_err(|error| (InferenceStage::ActiveInference, error))?;
            order_factor_outcomes(&batch.markets, result.outcomes)
                .map_err(|error| (InferenceStage::ActiveInference, error))?
        } else {
            Vec::new()
        };
        let input = build_runtime_input(
            runtime.as_ref(),
            model_run_id,
            request.boundary.decision_at(),
            &batch.markets,
            &batch.vectors,
            &outcomes,
        );
        runtime
            .infer_batch(input)
            .await
            .map_err(|error| (InferenceStage::ActiveInference, error))
    }

    /// Compute + persist the signal-layer [`ShadowComparison`], alerting on a hard
    /// divergence.
    async fn persist_shadow_comparison(
        &self,
        request: &ModelRunRequest<'_>,
        active: &ActiveResult,
        shadow_version_id: &ModelVersionId,
        weight_source: ModelWeightSource,
        shadow_candidates: &[SignalCandidate],
    ) -> QuantResult<()> {
        let observation = request.serving.published_shadow_identity()?;
        if observation.champion_model_version_id != active.active_version_id
            || observation.candidate_model_version_id != *shadow_version_id
        {
            return Err(ResearchError::Inference {
                detail: format!(
                    "shadow observation subjects drifted from pinned generation: champion={} candidate={}",
                    active.active_version_id, shadow_version_id
                ),
            }
            .into());
        }
        let threshold = request.serving.shadow_diff_threshold();
        let comparison = compute_shadow_comparison(&ShadowComparisonRequest {
            champion_model_version_id: active.active_version_id,
            candidate_model_version_id: *shadow_version_id,
            champion_serving_contract_hash: observation.champion_serving_contract_hash,
            candidate_serving_contract_hash: observation.candidate_serving_contract_hash,
            research_profile_artifact_id: observation.research_profile_artifact_id,
            category_scope: observation.category_scope,
            decision_policy_snapshot_id: observation.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: observation.decision_policy_snapshot_hash,
            policy_bundle_generation: observation.policy_bundle_generation,
            weight_source,
            decision_at: request.boundary.decision_at(),
            active: &active.active_candidates,
            shadow: shadow_candidates,
            top_n: request.top_n,
            score_divergence_threshold: threshold,
        })?;

        let row = new_shadow_comparison(&comparison);
        let _ = self.shadow_comparison_repo.create(row).await?;

        if comparison.hard_divergence {
            self.alerts.critical(
                format!(
                    "shadow model {shadow_version_id} diverged from active {}",
                    active.active_version_id
                ),
                format!(
                    "mean_abs_score_delta={}, topn_decision_overlap={}, side_disagreement_rate={}",
                    comparison.score_delta.mean_abs_score_delta,
                    comparison.topn_decision_overlap.inner(),
                    comparison.score_delta.side_disagreement_rate,
                ),
            );
        }
        Ok(())
    }

    /// Insert a fresh `Running` run row.
    async fn create_run(
        &self,
        model_run_id: &ModelRunId,
        run_kind: ModelRunKind,
        model_version_id: Option<ModelVersionId>,
        request: &ModelRunRequest<'_>,
        input_hash: ContentHash,
    ) -> QuantResult<()> {
        let run = NewModelRun {
            model_run_id: *model_run_id,
            run_kind,
            model_version_id,
            decision_policy_snapshot_id: request.decision_policy_snapshot_id,
            market_selection_id: request.market_selection_id,
            window_start: request.boundary.decision_at(),
            window_end: request.boundary.decision_at(),
            input_hash,
        };
        self.model_run_repo
            .create(run)
            .await
            .map(|_| ())
            .map_err(QuantError::from)
    }
}

async fn finalize_active_failure(
    model_run_repo: &Arc<dyn ModelRunRepository>,
    alerts: &Arc<dyn InferenceAlertSink>,
    model_run_id: &ModelRunId,
    stage: InferenceStage,
    error: QuantError,
) -> QuantError {
    let _ = model_run_repo
        .fail(model_run_id, stage.active_error_code(), error.to_string())
        .await;
    alerts.critical(
        format!("active model inference failed for run {model_run_id}"),
        error.to_string(),
    );
    error
}

/// Record a shadow-path failure per [`degrade_action`]; active is unaffected.
async fn finalize_shadow_failure(
    model_run_repo: &Arc<dyn ModelRunRepository>,
    model_run_id: Option<&ModelRunId>,
    stage: InferenceStage,
    error: QuantError,
) -> ShadowRunOutcome {
    if let Some(run_id) = model_run_id {
        let _ = model_run_repo
            .fail(run_id, stage.shadow_error_code(), error.to_string())
            .await;
    }
    tracing::warn!(%error, ?stage, "shadow inference failed; keeping active result");
    ShadowRunOutcome {
        model_run_id: model_run_id.copied(),
        emitted: 0,
        diff: None,
        failure: Some(error.to_string()),
    }
}

/// Split emitted candidates into their fact rows and the accepted subset (score
/// + confidence above the configured floors).
fn partition_candidates(
    candidates: Vec<SignalCandidate>,
    floor: Decimal,
    min_confidence: Decimal,
    event_time: i64,
) -> (
    Vec<QuantSignalCandidateEventRow>,
    Vec<SignalCandidate>,
    Vec<ModelMarketDecision>,
) {
    let mut rows = Vec::with_capacity(candidates.len());
    let mut accepted = Vec::new();
    let mut decisions = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let reason = rejection_reason(&candidate, floor, min_confidence);
        decisions.push(ModelMarketDecision {
            signal_candidate_id: candidate.signal_candidate_id,
            market_id: candidate.market_id.clone(),
            token_id: candidate.token_id.clone(),
            gate_passed: reason.is_empty(),
            primary_reason: (!reason.is_empty()).then(|| reason.to_owned()),
        });
        rows.push(signal_candidate_event(&candidate, reason, event_time));
        if reason.is_empty() {
            accepted.push(candidate);
        }
    }
    (rows, accepted, decisions)
}

/// The empty string for an accepted candidate, or the deterministic reason a
/// candidate was recorded for audit but excluded from the report.
fn rejection_reason(
    candidate: &SignalCandidate,
    floor: Decimal,
    min_confidence: Decimal,
) -> &'static str {
    if candidate.composite_score.inner() < floor {
        "score_below_floor"
    } else if candidate.confidence.inner() < min_confidence {
        "low_confidence"
    } else {
        ""
    }
}

/// Align accepted feature vectors with the immutable selection without using
/// factor output as the join spine. This lets a classical runtime bypass the
/// factor plane while retaining the exact same market snapshot.
fn align_feature_cross_section(
    request: &ModelRunRequest<'_>,
) -> QuantResult<AlignedFeatureCrossSection> {
    if request.feature_vectors.len() != request.feature_vector_ids.len() {
        return Err(ResearchError::Inference {
            detail: format!(
                "feature-vector/id width mismatch: {} vectors, {} ids",
                request.feature_vectors.len(),
                request.feature_vector_ids.len()
            ),
        }
        .into());
    }
    let mut selection = HashMap::with_capacity(request.selection.len());
    for market in request.selection {
        if selection.insert(&market.market_id, market).is_some() {
            return Err(ResearchError::Inference {
                detail: format!("duplicate selected market {}", market.market_id),
            }
            .into());
        }
    }
    let mut seen_vectors = BTreeSet::new();
    let mut markets = Vec::with_capacity(request.feature_vectors.len());
    let mut vectors = Vec::with_capacity(request.feature_vectors.len());
    let mut vector_ids = Vec::with_capacity(request.feature_vector_ids.len());
    for (vector, vector_id) in request
        .feature_vectors
        .iter()
        .zip(request.feature_vector_ids)
    {
        if !seen_vectors.insert(vector.market_id.clone()) {
            return Err(ResearchError::Inference {
                detail: format!("duplicate feature vector for market {}", vector.market_id),
            }
            .into());
        }
        let market = selection
            .get(&vector.market_id)
            .ok_or_else(|| ResearchError::Inference {
                detail: format!(
                    "feature vector for market {} is absent from the frozen selection",
                    vector.market_id
                ),
            })?;
        markets.push((*market).clone());
        vectors.push(vector.clone());
        vector_ids.push(*vector_id);
    }
    Ok(AlignedFeatureCrossSection {
        markets,
        vectors: Arc::from(vectors),
        vector_ids,
    })
}

fn ensure_route_membership(route: BuyModelRoute, selection: &[SelectedMarket]) -> QuantResult<()> {
    for market in selection {
        let scope_matches = match route {
            BuyModelRoute::Pooled => !matches!(
                market.category,
                MarketCategory::Crypto | MarketCategory::Weather
            ),
            BuyModelRoute::Crypto => market.category == MarketCategory::Crypto,
            BuyModelRoute::Weather => market.category == MarketCategory::Weather,
        };
        if !scope_matches {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "selected market {} category {} is outside exact route {route:?}",
                    market.market_id, market.category
                ),
            }
            .into());
        }
    }
    Ok(())
}

fn order_factor_outcomes(
    markets: &[SelectedMarket],
    outcomes: Vec<MarketFactorOutcome>,
) -> QuantResult<Vec<MarketFactorOutcome>> {
    let mut by_market = HashMap::with_capacity(outcomes.len());
    for outcome in outcomes {
        let market_id = outcome.market_id.clone();
        if by_market.insert(market_id.clone(), outcome).is_some() {
            return Err(ResearchError::Inference {
                detail: format!("factor plane emitted duplicate market {market_id}"),
            }
            .into());
        }
    }
    let ordered = markets
        .iter()
        .map(|market| {
            by_market.remove(&market.market_id).ok_or_else(|| {
                ResearchError::Inference {
                    detail: format!(
                        "factor plane emitted no outcome for market {}",
                        market.market_id
                    ),
                }
                .into()
            })
        })
        .collect::<QuantResult<Vec<_>>>()?;
    if !by_market.is_empty() {
        return Err(ResearchError::Inference {
            detail: format!(
                "factor plane emitted {} outcomes outside the routed cross-section",
                by_market.len()
            ),
        }
        .into());
    }
    Ok(ordered)
}

fn weighted_references(runtime: &dyn QuantModelRuntime) -> QuantResult<&FrozenReferenceQuantiles> {
    runtime.frozen_reference_quantiles().ok_or_else(|| {
        QuantError::from(ResearchError::InvalidModelArtifact {
            detail: format!(
                "weighted runtime {} has no frozen reference distribution contract",
                runtime.model_version_id()
            ),
        })
    })
}

/// Shadow vs active divergence over the markets both models scored.
fn shadow_diff(
    active_index: &HashMap<MarketId, (Probability, OutcomeSide)>,
    shadow: &[SignalCandidate],
    threshold: Decimal,
) -> ShadowDiff {
    let mut count: i64 = 0;
    let mut score_sum = Decimal::ZERO;
    let mut disagreements: i64 = 0;
    for candidate in shadow {
        if let Some((score, outcome_side)) = active_index.get(&candidate.market_id) {
            count += 1;
            score_sum += (candidate.composite_score.inner() - score.inner()).abs();
            if candidate.outcome_side != *outcome_side {
                disagreements += 1;
            }
        }
    }
    if count == 0 {
        return ShadowDiff {
            mean_score_diff: Decimal::ZERO,
            side_disagreement_rate: Decimal::ZERO,
            exceeds_threshold: false,
        };
    }
    let divisor = Decimal::from(count);
    let mean_score_diff = (score_sum / divisor).round_dp(DIFF_SCALE);
    let side_disagreement_rate = (Decimal::from(disagreements) / divisor).round_dp(DIFF_SCALE);
    ShadowDiff {
        exceeds_threshold: mean_score_diff > threshold,
        mean_score_diff,
        side_disagreement_rate,
    }
}

/// Project a computed [`ShadowComparison`] into its persistence insert payload.
fn new_shadow_comparison(comparison: &ShadowComparison) -> NewShadowComparison {
    NewShadowComparison {
        shadow_comparison_id: comparison.shadow_comparison_id,
        champion_model_version_id: comparison.champion_model_version_id,
        candidate_model_version_id: comparison.candidate_model_version_id,
        champion_serving_contract_hash: comparison.champion_serving_contract_hash,
        candidate_serving_contract_hash: comparison.candidate_serving_contract_hash,
        research_profile_artifact_id: comparison.research_profile_artifact_id.clone(),
        category_scope: comparison.category_scope,
        decision_policy_snapshot_id: comparison.decision_policy_snapshot_id,
        decision_policy_snapshot_hash: comparison.decision_policy_snapshot_hash,
        policy_bundle_generation: comparison.policy_bundle_generation,
        weight_source: comparison.weight_source,
        decision_at: comparison.decision_at,
        topn_decision_overlap: comparison.topn_decision_overlap,
        rank_delta_json: comparison.rank_delta,
        score_delta_json: comparison.score_delta,
        matured_outcome_json: comparison.matured_outcome_delta,
        hard_divergence: comparison.hard_divergence,
        comparison_hash: comparison.comparison_hash,
    }
}

/// Canonical input hash for a run.
fn input_hash(
    request: &ModelRunRequest<'_>,
    run_kind: &str,
    serving_contract_hash: &ContentHash,
    model_version_id: Option<&ModelVersionId>,
) -> QuantResult<ContentHash> {
    #[derive(Serialize)]
    struct Digest<'a> {
        run_kind: &'a str,
        market_selection_id: Option<&'a MarketSelectionId>,
        feature_vector_ids: &'a [FeatureVectorId],
        serving_contract_hash: &'a ContentHash,
        model_version_id: Option<&'a ModelVersionId>,
        boundary: &'a DecisionBoundary,
    }
    ResearchHasher::canonical(&Digest {
        run_kind,
        market_selection_id: request.market_selection_id.as_ref(),
        feature_vector_ids: request.feature_vector_ids,
        serving_contract_hash,
        model_version_id,
        boundary: &request.boundary,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Utc;
    use quant_pivot_models::{
        domain::data_plane::DecisionClock,
        enums::{common::MarketCategory, model::ModelFamily, quant::DataQualityStatus},
        runtime_config::BuyModelRoute,
        types::{
            ContentHash, EventId, FeatureVectorId, MarketId, ModelRunId, ModelVersionId,
            SchemaVersion, TokenId,
        },
    };
    use quant_pivot_research::{
        features::FeatureVector,
        model::{ModelInputAuditRow, ModelInputAuditState},
        selection::SelectedMarket,
    };

    use crate::observability::serving_evidence::ModelInputEvidenceBatch;

    use super::ensure_route_membership;
    #[test]
    fn route_authority_rejects_mismatch() {
        let selected_market = |category| SelectedMarket {
            market_id: MarketId::new(format!("route-{category}")),
            event_id: EventId::new("route-event"),
            category,
            primary_token_id: TokenId::new("route-yes"),
            secondary_token_id: Some(TokenId::new("route-no")),
            liquidity_usd: None,
            volume_24h_usd: None,
            source_refs: Vec::new(),
        };
        assert!(
            ensure_route_membership(
                BuyModelRoute::Weather,
                &[selected_market(MarketCategory::Sports)],
            )
            .is_err()
        );
        assert!(
            ensure_route_membership(
                BuyModelRoute::Pooled,
                &[selected_market(MarketCategory::Weather)],
            )
            .is_err()
        );
        assert!(
            ensure_route_membership(
                BuyModelRoute::Weather,
                &[selected_market(MarketCategory::Weather)],
            )
            .is_ok()
        );
    }

    #[test]
    fn model_input_binds_vector() {
        let decision_at = Utc::now();
        let market_id = MarketId::new("0xaudit");
        let vector = FeatureVector {
            market_id: market_id.clone(),
            token_id: None,
            decision_at,
            generic_schema_version: SchemaVersion::FIRST,
            generic: BTreeMap::new(),
            domain: None,
            data_quality: DataQualityStatus::Fresh,
        };
        let hash = ContentHash::parse(&format!("blake3:{}", "1".repeat(64))).expect("hash");
        let audit = [ModelInputAuditRow {
            model_version_id: ModelVersionId::from_v7(),
            model_family: ModelFamily::WeightedFactor,
            market_id,
            raw_input_name: "factor.liquidity".to_owned(),
            raw_state: ModelInputAuditState::Scored,
            raw_value: Some("1.25".to_owned()),
            encoded_column: "factor.liquidity.normalized_score".to_owned(),
            encoded_value_bits: Some(0.75_f64.to_bits()),
            input_contract_hash: hash,
            transform_hash: hash,
            training_input_hash: hash,
        }];
        let first_id = FeatureVectorId::from_v7();
        let first_vectors = [vector.clone()];
        let first_ids = [first_id];
        let boundary = DecisionClock::new(7)
            .boundary(decision_at)
            .expect("boundary");
        let model_run_id = ModelRunId::from_v7();
        let first_row = ModelInputEvidenceBatch::try_new(&first_vectors, &first_ids)
            .and_then(|batch| {
                batch.project(
                    &model_run_id,
                    &boundary,
                    &audit,
                    decision_at.timestamp_millis(),
                )
            })
            .expect("projection")
            .pop()
            .expect("row");
        assert_eq!(first_row.feature_vector_id, first_id);
        assert!(!first_row.audit_fingerprint.is_empty());

        let second_vectors = [vector];
        let second_ids = [FeatureVectorId::from_v7()];
        let second_row = ModelInputEvidenceBatch::try_new(&second_vectors, &second_ids)
            .and_then(|batch| {
                batch.project(
                    &model_run_id,
                    &boundary,
                    &audit,
                    decision_at.timestamp_millis(),
                )
            })
            .expect("projection")
            .pop()
            .expect("row");
        assert_ne!(
            first_row.audit_fingerprint, second_row.audit_fingerprint,
            "feature-vector identity must participate in the audit fingerprint"
        );
    }
}
