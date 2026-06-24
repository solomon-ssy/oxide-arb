//! Online inference closure: the [`ModelRunner`].
//!
//! Given a persisted selection and built feature vectors, one round:
//!
//! 1. creates a `Running` [`quant_model_run`] (so factor values can take its FK),
//! 2. runs the factor plane ([`FactorPipelineService`]) under that run,
//! 3. loads the active model runtime (content-addressed, hash + schema verified),
//! 4. scores the eligible markets into [`SignalCandidate`]s,
//! 5. finalizes the run (`succeed` with the output hash + metrics, or `fail`), and
//! 6. emits the non-authoritative `quant_signal_candidate_event` facts **after**
//!    the run reaches a terminal PG state (§5.3 ordering).
//!
//! A shadow model, when configured, runs as an isolated `Shadow` run whose failure
//! never affects the active result (父文档 §28 degrade table). Active-path failures
//! fail the run and raise a critical alert — an empty report is never silently
//! fabricated. Business callers depend only on this service and `dyn QuantModelRuntime`,
//! never on a concrete runtime type.

use std::{collections::HashMap, str::FromStr, sync::Arc};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::QuantSignalCandidateEventRow,
    domain::NewModelRun,
    enums::{
        common::{AlertCategory, AlertLevel, AlertSource},
        quant::{ModelRunErrorCode, ModelRunKind, ModelRunStatus, SignalSide},
    },
    runtime_config::{DecimalString, FactorsConfig, FeaturesConfig, ModelConfig, ModelVersionRef},
    types::{
        ContentHash, FeatureVectorId, MarketId, MarketSelectionId, ModelRunId, ModelVersionId,
        Price, Probability, RuntimeConfigVersionId, Usd,
    },
};
use quant_pivot_repository::traits::{ModelRegistryRepository, ModelRunRepository};
use quant_pivot_research::{
    factors::{FactorEngine, MarketFactorOutcome},
    features::{
        FeatureName, FeatureSchema, FeatureValue, FeatureVector,
        names::{book, market},
    },
    hashing::ResearchHasher,
    model::{
        ActiveSchemaBinding, DegradeAction, FactorInferenceRow, FactorInferenceTable,
        InferenceStage, MarketInferenceContext, ModelRuntimeFactoryBuilder, ModelRuntimeInput,
        SignalCandidate, degrade_action, signal_candidate_event,
    },
    selection::SelectedMarket,
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::observability::{
    alert_dispatcher::{Alert, AlertDispatcher},
    signal_candidate_fact_writer::SignalCandidateEventWriter,
};
use crate::service::factor_pipeline::{FactorPipelineRequest, FactorPipelineService};

/// Decimal places shadow-diff aggregates are rounded to.
const DIFF_SCALE: u32 = 12;

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
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// The selection snapshot this round scores, when persisted.
    pub market_selection_id: Option<MarketSelectionId>,
    /// Selected markets (token ids, liquidity) for the scoring context.
    pub selection: &'a [SelectedMarket],
    /// Accepted feature vectors (data-quality bar already passed at 3.2).
    pub feature_vectors: &'a [FeatureVector],
    /// Persisted feature-vector ids, aligned 1:1 with `feature_vectors`.
    pub feature_vector_ids: &'a [FeatureVectorId],
    /// Frozen feature config.
    pub features: &'a FeaturesConfig,
    /// Frozen factor config.
    pub factors: &'a FactorsConfig,
    /// Frozen model config (active / shadow refs, floors, horizon).
    pub model: &'a ModelConfig,
    /// Decision time.
    pub as_of: DateTime<Utc>,
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
    /// Accepted candidates (score + confidence above the configured floors),
    /// ranked, ready for the Phase 04 portfolio planner.
    pub accepted: Vec<SignalCandidate>,
    /// Total candidates emitted to the fact stream (accepted + audited rejects).
    pub emitted: u32,
    /// Shadow outcome, when a shadow model was configured.
    pub shadow: Option<ShadowRunOutcome>,
}

/// The successful active-path result threaded back to [`ModelRunner::run`].
struct ActiveResult {
    output_hash: ContentHash,
    metrics: serde_json::Value,
    accepted: Vec<SignalCandidate>,
    emitted: u32,
    /// Per-market `(composite_score, side)` for the shadow diff.
    active_index: HashMap<MarketId, (Probability, SignalSide)>,
    /// Eligible factor outcomes reused (no recompute) for the shadow table.
    outcomes: Vec<MarketFactorOutcome>,
    /// Projected CH rows; written only after the active run succeeds in PG.
    ch_rows: Vec<QuantSignalCandidateEventRow>,
}

/// Online inference orchestrator (3.4 capstone).
pub struct ModelRunner {
    model_run_repo: Arc<dyn ModelRunRepository>,
    model_registry_repo: Arc<dyn ModelRegistryRepository>,
    factory_builder: Arc<dyn ModelRuntimeFactoryBuilder>,
    factor_pipeline: Arc<FactorPipelineService>,
    signal_writer: Arc<SignalCandidateEventWriter>,
    alerts: Arc<dyn InferenceAlertSink>,
}

impl ModelRunner {
    /// Wire the runner from boot-time dependencies.
    #[must_use]
    pub fn new(
        model_run_repo: Arc<dyn ModelRunRepository>,
        model_registry_repo: Arc<dyn ModelRegistryRepository>,
        factory_builder: Arc<dyn ModelRuntimeFactoryBuilder>,
        factor_pipeline: Arc<FactorPipelineService>,
        signal_writer: Arc<SignalCandidateEventWriter>,
        alerts: Arc<dyn InferenceAlertSink>,
    ) -> Self {
        Self {
            model_run_repo,
            model_registry_repo,
            factory_builder,
            factor_pipeline,
            signal_writer,
            alerts,
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
        let factor_schema_hash =
            FactorEngine::new(request.factors, request.features).factor_schema_hash()?;
        let feature_schema_hash =
            ResearchHasher::feature_schema(&FeatureSchema::build(request.features))?;
        let binding = ActiveSchemaBinding {
            feature_schema_hash,
            factor_schema_hash: factor_schema_hash.clone(),
        };

        let active_version_id = match request.model.active_model_version_id.as_ref() {
            Some(reference) => Some(parse_version(reference)?),
            None => None,
        };

        let model_run_id = ModelRunId::from_v7();
        let input_hash = input_hash(
            &request,
            "live_inference",
            &factor_schema_hash,
            active_version_id.as_ref(),
        )?;
        self.create_run(
            &model_run_id,
            ModelRunKind::LiveInference,
            active_version_id.clone(),
            &request,
            input_hash,
        )
        .await?;

        match self
            .run_active(
                &model_run_id,
                &request,
                &binding,
                active_version_id.as_ref(),
            )
            .await
        {
            Ok(active) => {
                self.model_run_repo
                    .succeed(
                        &model_run_id,
                        active.output_hash.clone(),
                        active.metrics.clone(),
                        Utc::now(),
                    )
                    .await?;
                let shadow = self
                    .run_shadow(&request, &binding, &factor_schema_hash, &active)
                    .await;
                self.signal_writer.write_batch(active.ch_rows);
                Ok(ModelRunOutcome {
                    model_run_id,
                    accepted: active.accepted,
                    emitted: active.emitted,
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

    /// The active path: factor plane → load → infer → emit.
    async fn run_active(
        &self,
        model_run_id: &ModelRunId,
        request: &ModelRunRequest<'_>,
        binding: &ActiveSchemaBinding,
        active_version_id: Option<&ModelVersionId>,
    ) -> Result<ActiveResult, (InferenceStage, QuantError)> {
        let version_id = active_version_id.ok_or_else(|| {
            (
                InferenceStage::ActiveLoad,
                QuantError::config("model.active_model_version_id is not configured"),
            )
        })?;
        let version = self
            .model_registry_repo
            .find_model_version_by_id(version_id)
            .await
            .map_err(QuantError::from)
            .and_then(|row| {
                row.ok_or_else(|| {
                    ResearchError::InvalidModelArtifact {
                        detail: format!("active model version {version_id} not found"),
                    }
                    .into()
                })
            })
            .map_err(|error| (InferenceStage::ActiveLoad, error))?;

        let factor_result = self
            .factor_pipeline
            .run(FactorPipelineRequest {
                model_run_id,
                vectors: request.feature_vectors,
                feature_vector_ids: request.feature_vector_ids,
                factors: request.factors,
                features: request.features,
            })
            .await
            .map_err(|error| (InferenceStage::ActiveInference, error))?;

        let factory = self.factory_builder.build(binding.clone());
        let runtime = factory
            .load(&version)
            .await
            .map_err(|error| (InferenceStage::ActiveLoad, error))?;

        let table = build_table(model_run_id, request, &factor_result.outcomes);
        let output = runtime
            .infer_batch(ModelRuntimeInput::FactorTable(table))
            .await
            .map_err(|error| (InferenceStage::ActiveInference, error))?;

        let output_hash = ResearchHasher::canonical(&output.candidates)
            .map_err(|error| (InferenceStage::ActiveInference, error))?;
        let active_index: HashMap<MarketId, (Probability, SignalSide)> = output
            .candidates
            .iter()
            .map(|candidate| {
                (
                    candidate.market_id.clone(),
                    (candidate.composite_score, candidate.side),
                )
            })
            .collect();

        let floor = parse_decimal(
            &request.model.candidate_score_floor,
            "candidate_score_floor",
        )
        .map_err(|error| (InferenceStage::ActiveInference, error))?;
        let min_confidence =
            parse_decimal(&request.model.min_model_confidence, "min_model_confidence")
                .map_err(|error| (InferenceStage::ActiveInference, error))?;
        let event_time = Utc::now().timestamp_millis();

        let emitted = u32::try_from(output.candidates.len()).unwrap_or(u32::MAX);
        let mut rows = Vec::with_capacity(output.candidates.len());
        let mut accepted = Vec::new();
        for candidate in output.candidates {
            let reason = rejection_reason(&candidate, floor, min_confidence);
            rows.push(signal_candidate_event(&candidate, reason, event_time));
            if reason.is_empty() {
                accepted.push(candidate);
            }
        }
        let metrics = serde_json::json!({
            "markets_scored": output.runtime_metrics.markets_scored,
            "candidates_emitted": output.runtime_metrics.candidates_emitted,
            "accepted": accepted.len(),
            "inference_duration_ms": output.runtime_metrics.inference_duration_ms,
        });

        Ok(ActiveResult {
            output_hash,
            metrics,
            accepted,
            emitted,
            active_index,
            outcomes: factor_result.outcomes,
            ch_rows: rows,
        })
    }

    /// The isolated shadow path: never fails the round (父文档 §28).
    async fn run_shadow(
        &self,
        request: &ModelRunRequest<'_>,
        binding: &ActiveSchemaBinding,
        factor_schema_hash: &ContentHash,
        active: &ActiveResult,
    ) -> Option<ShadowRunOutcome> {
        let reference = request.model.shadow_model_version_id.as_ref()?;
        let shadow_version_id = match parse_version(reference) {
            Ok(id) => id,
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

        let model_run_id = ModelRunId::from_v7();
        let input_hash = match input_hash(
            request,
            "shadow",
            factor_schema_hash,
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
                Some(shadow_version_id.clone()),
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
            .run_shadow_inference(&model_run_id, request, binding, &shadow_version_id, active)
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
        binding: &ActiveSchemaBinding,
        shadow_version_id: &ModelVersionId,
        active: &ActiveResult,
    ) -> Result<ShadowRunOutcome, (InferenceStage, QuantError)> {
        let version = self
            .model_registry_repo
            .find_model_version_by_id(shadow_version_id)
            .await
            .map_err(QuantError::from)
            .and_then(|row| {
                row.ok_or_else(|| {
                    ResearchError::InvalidModelArtifact {
                        detail: format!("shadow model version {shadow_version_id} not found"),
                    }
                    .into()
                })
            })
            .map_err(|error| (InferenceStage::ShadowLoad, error))?;

        let factory = self.factory_builder.build(binding.clone());
        let runtime = factory
            .load(&version)
            .await
            .map_err(|error| (InferenceStage::ShadowLoad, error))?;

        let table = build_table(model_run_id, request, &active.outcomes);
        let output = runtime
            .infer_batch(ModelRuntimeInput::FactorTable(table))
            .await
            .map_err(|error| (InferenceStage::ShadowInference, error))?;

        let output_hash = ResearchHasher::canonical(&output.candidates)
            .map_err(|error| (InferenceStage::ShadowInference, error))?;
        let threshold = parse_decimal(
            &request.model.shadow_diff_threshold,
            "shadow_diff_threshold",
        )
        .map_err(|error| (InferenceStage::ShadowInference, error))?;
        let diff = shadow_diff(&active.active_index, &output.candidates, threshold);

        let event_time = Utc::now().timestamp_millis();
        let emitted = u32::try_from(output.candidates.len()).unwrap_or(u32::MAX);
        let rows: Vec<_> = output
            .candidates
            .iter()
            .map(|candidate| signal_candidate_event(candidate, "", event_time))
            .collect();

        let metrics = serde_json::json!({
            "candidates_emitted": emitted,
            "diff_mean_score": diff.mean_score_diff.to_string(),
            "side_disagreement_rate": diff.side_disagreement_rate.to_string(),
            "exceeds_threshold": diff.exceeds_threshold,
        });
        self.model_run_repo
            .succeed(model_run_id, output_hash, metrics, Utc::now())
            .await
            .map_err(|error| (InferenceStage::ShadowInference, QuantError::from(error)))?;
        self.signal_writer.write_batch(rows);

        Ok(ShadowRunOutcome {
            model_run_id: Some(model_run_id.clone()),
            emitted,
            diff: Some(diff),
            failure: None,
        })
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
        let now = Utc::now();
        let run = NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind,
            model_version_id,
            runtime_config_version_id: request.runtime_config_version_id.clone(),
            market_selection_id: request.market_selection_id.clone(),
            window_start: request.as_of,
            window_end: request.as_of,
            status: ModelRunStatus::Running,
            input_hash,
            output_hash: None,
            metrics_json: serde_json::json!({}),
            error_code: None,
            error_message: None,
            started_at: now,
            finished_at: None,
        };
        self.model_run_repo
            .create(run)
            .await
            .map(|_| ())
            .map_err(QuantError::from)
    }
}

/// Map an active-path failure stage to a persisted [`ModelRunErrorCode`].
const fn active_error_code(stage: InferenceStage) -> ModelRunErrorCode {
    match stage {
        InferenceStage::ActiveLoad => ModelRunErrorCode::ArtifactLoadFailed,
        InferenceStage::ActiveInference => ModelRunErrorCode::ActiveInferenceFailed,
        InferenceStage::ShadowLoad | InferenceStage::ShadowInference => {
            ModelRunErrorCode::ActiveInferenceFailed
        }
    }
}

/// Map a shadow-path failure stage to a persisted [`ModelRunErrorCode`].
const fn shadow_error_code(stage: InferenceStage) -> ModelRunErrorCode {
    match stage {
        InferenceStage::ShadowLoad => ModelRunErrorCode::ArtifactLoadFailed,
        InferenceStage::ShadowInference => ModelRunErrorCode::ShadowInferenceFailed,
        InferenceStage::ActiveLoad | InferenceStage::ActiveInference => {
            ModelRunErrorCode::ShadowInferenceFailed
        }
    }
}

/// Finalize an active-path failure per [`degrade_action`]: fail the run + critical alert.
async fn finalize_active_failure(
    model_run_repo: &Arc<dyn ModelRunRepository>,
    alerts: &Arc<dyn InferenceAlertSink>,
    model_run_id: &ModelRunId,
    stage: InferenceStage,
    error: QuantError,
) -> QuantError {
    let action = degrade_action(stage);
    debug_assert_eq!(action, DegradeAction::FailRunCritical);
    let _ = action;
    let _ = model_run_repo
        .fail(
            model_run_id,
            active_error_code(stage),
            error.to_string(),
            Utc::now(),
        )
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
    let action = degrade_action(stage);
    debug_assert_eq!(action, DegradeAction::KeepActiveRecordShadow);
    let _ = action;
    if let Some(run_id) = model_run_id {
        let _ = model_run_repo
            .fail(
                run_id,
                shadow_error_code(stage),
                error.to_string(),
                Utc::now(),
            )
            .await;
    }
    tracing::warn!(%error, ?stage, "shadow inference failed; keeping active result");
    ShadowRunOutcome {
        model_run_id: model_run_id.cloned(),
        emitted: 0,
        diff: None,
        failure: Some(error.to_string()),
    }
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

/// Assemble the factor inference table from eligible outcomes + per-market context.
fn build_table(
    model_run_id: &ModelRunId,
    request: &ModelRunRequest<'_>,
    outcomes: &[MarketFactorOutcome],
) -> FactorInferenceTable {
    let vectors: HashMap<&MarketId, &FeatureVector> = request
        .feature_vectors
        .iter()
        .map(|vector| (&vector.market_id, vector))
        .collect();
    let selection: HashMap<&MarketId, &SelectedMarket> = request
        .selection
        .iter()
        .map(|market| (&market.market_id, market))
        .collect();

    let mut rows = Vec::new();
    for outcome in outcomes {
        if !outcome.eligibility.is_eligible() {
            continue;
        }
        let (Some(selected), Some(vector)) = (
            selection.get(&outcome.market_id),
            vectors.get(&outcome.market_id),
        ) else {
            continue;
        };
        let Some(context) = market_context(vector, selected) else {
            continue;
        };
        let factors = outcome
            .factors
            .iter()
            .map(|scored| scored.value.clone())
            .collect();
        rows.push(FactorInferenceRow {
            market_id: outcome.market_id.clone(),
            token_id: selected.primary_token_id.clone(),
            factors,
            context,
        });
    }

    FactorInferenceTable {
        model_run_id: model_run_id.clone(),
        as_of: request.as_of,
        rows,
    }
}

/// Project the per-market scoring context, or `None` when no entry price exists.
fn market_context(
    vector: &FeatureVector,
    selected: &SelectedMarket,
) -> Option<MarketInferenceContext> {
    let yes_price = yes_price(vector)?;
    Some(MarketInferenceContext {
        secondary_token_id: selected.secondary_token_id.clone(),
        yes_price,
        no_price: None,
        liquidity_usd: selected
            .liquidity_usd
            .or_else(|| usd_feature(vector, &book::VISIBLE_LIQUIDITY_USD)),
        data_quality: vector.data_quality,
        time_to_resolution_secs: count_feature(vector, &market::TIME_TO_RESOLUTION_SECS),
        substitutions: vector.substitutions.clone(),
    })
}

/// The YES executable reference price: the mid, else the bid/ask midpoint.
fn yes_price(vector: &FeatureVector) -> Option<Price> {
    if let Some(mid) = probability_feature(vector, &book::MID) {
        return Some(Price::new(mid.inner()));
    }
    let bid = probability_feature(vector, &book::BEST_BID)?;
    let ask = probability_feature(vector, &book::BEST_ASK)?;
    Some(Price::new(
        ((bid.inner() + ask.inner()) / Decimal::from(2)).clamp(Decimal::ZERO, Decimal::ONE),
    ))
}

/// Read a `[0, 1]` probability-valued feature.
fn probability_feature(vector: &FeatureVector, name: &FeatureName) -> Option<Probability> {
    match vector.values.get(name) {
        Some(FeatureValue::Probability(value)) => Some(*value),
        _ => None,
    }
}

/// Read a USD-valued feature.
fn usd_feature(vector: &FeatureVector, name: &FeatureName) -> Option<Usd> {
    match vector.values.get(name) {
        Some(FeatureValue::Usd(value)) => Some(*value),
        _ => None,
    }
}

/// Read a count-valued feature.
fn count_feature(vector: &FeatureVector, name: &FeatureName) -> Option<u64> {
    match vector.values.get(name) {
        Some(FeatureValue::Count(value)) => Some(*value),
        _ => None,
    }
}

/// Shadow vs active divergence over the markets both models scored.
fn shadow_diff(
    active_index: &HashMap<MarketId, (Probability, SignalSide)>,
    shadow: &[SignalCandidate],
    threshold: Decimal,
) -> ShadowDiff {
    let mut count: i64 = 0;
    let mut score_sum = Decimal::ZERO;
    let mut disagreements: i64 = 0;
    for candidate in shadow {
        if let Some((score, side)) = active_index.get(&candidate.market_id) {
            count += 1;
            score_sum += (candidate.composite_score.inner() - score.inner()).abs();
            if candidate.side != *side {
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

/// Canonical input hash for a run.
fn input_hash(
    request: &ModelRunRequest<'_>,
    run_kind: &str,
    factor_schema_hash: &ContentHash,
    model_version_id: Option<&ModelVersionId>,
) -> QuantResult<ContentHash> {
    #[derive(Serialize)]
    struct Digest<'a> {
        run_kind: &'a str,
        market_selection_id: Option<&'a MarketSelectionId>,
        feature_vector_ids: &'a [FeatureVectorId],
        factor_schema_hash: &'a ContentHash,
        model_version_id: Option<&'a ModelVersionId>,
        as_of: DateTime<Utc>,
    }
    ResearchHasher::canonical(&Digest {
        run_kind,
        market_selection_id: request.market_selection_id.as_ref(),
        feature_vector_ids: request.feature_vector_ids,
        factor_schema_hash,
        model_version_id,
        as_of: request.as_of,
    })
}

/// Parse a config model-version reference into a typed id.
fn parse_version(reference: &ModelVersionRef) -> QuantResult<ModelVersionId> {
    ModelVersionId::from_str(reference.id.trim()).map_err(|error| {
        QuantError::config(format!(
            "invalid model_version_id `{}`: {error}",
            reference.id
        ))
    })
}

/// Parse a governed decimal-string config value, failing closed.
fn parse_decimal(value: &DecimalString, field: &str) -> QuantResult<Decimal> {
    Decimal::from_str(value.value.trim())
        .map_err(|error| QuantError::config(format!("invalid {field} `{}`: {error}", value.value)))
}
