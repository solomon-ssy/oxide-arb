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

use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::QuantSignalCandidateEventRow,
    domain::{ModelVersionInfo, NewModelRun, NewShadowComparison},
    enums::{
        common::{AlertCategory, AlertLevel, AlertSource, MarketCategory},
        quant::{ModelRunKind, ModelRunStatus, OutcomeSide, PublicationStatus},
    },
    runtime_config::{
        DecimalString, DomainConfig, FactorsConfig, FeaturesConfig, ModelConfig, ModelVersionRef,
    },
    types::{
        ContentHash, FeatureVectorId, MarketId, MarketSelectionId, ModelRunId, ModelVersionId,
        Probability, RuntimeConfigVersionId,
    },
};
use quant_pivot_repository::traits::{
    ModelRegistryRepository, ModelRunRepository, ShadowComparisonRepository,
};
use quant_pivot_research::{
    factors::{FactorEngine, MarketFactorOutcome},
    features::{FeatureName, FeatureSchema, FeatureVector},
    governance::{ShadowComparison, compute_shadow_comparison},
    hashing::ResearchHasher,
    model::{
        ActiveSchemaBinding, InferenceStage, ModelRouting, ModelRuntimeFactory,
        ModelRuntimeFactoryBuilder, ModelRuntimeInput, ModelRuntimeMetrics, ModelRuntimeOutput,
        QuantModelRuntime, SignalCandidate, WeightOverlay, annotate, attach_rank_scores,
        resolve_model_route, signal_candidate_event,
    },
    selection::{ModelFeatureRequirements, SelectedMarket},
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    governance::{BiasTableApplicator, WeightOverlayApplicator, active_load_ok, shadow_load_ok},
    observability::{
        alert_dispatcher::{Alert, AlertDispatcher},
        signal_candidate_fact_writer::SignalCandidateEventWriter,
    },
    projection::inference_batch::build_runtime_input,
    service::factor_pipeline::{FactorPipelineRequest, FactorPipelineService},
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
    /// Frozen domain config (category-routed domain factor plane).
    pub domain: &'a DomainConfig,
    /// Frozen model config (active / shadow refs, floors, horizon).
    pub model: &'a ModelConfig,
    /// `TopN` bound for the shadow comparison overlap (the report's resolved `TopN`).
    pub top_n: usize,
    /// Decision time.
    pub as_of: DateTime<Utc>,
}

/// Frozen inputs required to resolve active model feature requirements before
/// market selection.
pub struct ActiveModelRequirementsRequest<'a> {
    /// Frozen feature config.
    pub features: &'a FeaturesConfig,
    /// Frozen factor config.
    pub factors: &'a FactorsConfig,
    /// Frozen domain config (category-routed domain factor plane).
    pub domain: &'a DomainConfig,
    /// Frozen model config.
    pub model: &'a ModelConfig,
    /// Decision time used by load-time governance checks.
    pub as_of: DateTime<Utc>,
}

/// Active model metadata and selector-facing feature requirements.
#[derive(Debug, Clone)]
pub struct ActiveModelRequirements {
    /// Active production model version.
    pub model_version_id: ModelVersionId,
    /// Registry row loaded and quality-gate checked.
    pub version: ModelVersionInfo,
    /// Feature availability required before a market enters selection.
    pub model_requirements: ModelFeatureRequirements,
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
    /// The active model version that produced this result (for the shadow record).
    active_version_id: ModelVersionId,
    /// Per-market `(composite_score, outcome_side)` for the shadow diff.
    active_index: HashMap<MarketId, (Probability, OutcomeSide)>,
    /// Full ranked active candidates for the signal-layer shadow comparison.
    active_candidates: Vec<SignalCandidate>,
    /// Eligible factor outcomes reused (no recompute) for the shadow table.
    outcomes: Vec<MarketFactorOutcome>,
    /// Projected CH rows; written only after the active run succeeds in PG.
    ch_rows: Vec<QuantSignalCandidateEventRow>,
}

#[derive(Clone, Copy)]
struct CrossSectionBatch<'a> {
    markets: &'a [SelectedMarket],
    vectors: &'a [FeatureVector],
    outcomes: &'a [MarketFactorOutcome],
}

struct RoutedInference<'a> {
    factory: &'a Arc<dyn ModelRuntimeFactory>,
    request: &'a ModelRunRequest<'a>,
    model_run_id: &'a ModelRunId,
    generic_version_id: &'a ModelVersionId,
    batch: CrossSectionBatch<'a>,
}

struct PartitionInference<'a> {
    factory: &'a Arc<dyn ModelRuntimeFactory>,
    version_id: &'a ModelVersionId,
    request: &'a ModelRunRequest<'a>,
    model_run_id: &'a ModelRunId,
    indices: &'a [usize],
    batch: CrossSectionBatch<'a>,
}
/// Boot-time dependencies for the [`ModelRunner`].
pub struct ModelRunnerDeps {
    /// Model-run ledger (create / finalize live + shadow runs).
    pub model_run_repo: Arc<dyn ModelRunRepository>,
    /// Model registry (resolve active / shadow versions).
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    /// Shadow-comparison ledger (signal-layer divergence persistence).
    pub shadow_comparison_repo: Arc<dyn ShadowComparisonRepository>,
    /// Schema-bound runtime factory builder (loads model artifacts).
    pub factory_builder: Arc<dyn ModelRuntimeFactoryBuilder>,
    /// Online factor build pipeline.
    pub factor_pipeline: Arc<FactorPipelineService>,
    /// `ClickHouse` signal-candidate fact writer.
    pub signal_writer: Arc<SignalCandidateEventWriter>,
    /// Operator alert sink for inference degradation.
    pub alerts: Arc<dyn InferenceAlertSink>,
    /// Hot-reloadable factor-weight overlay for non-published candidate / shadow.
    pub weight_overlay: Arc<WeightOverlayApplicator>,
    /// Hot-reloadable favorite-longshot bias table (provenance + factor plane).
    pub bias_table: Arc<BiasTableApplicator>,
}

/// Online inference orchestrator (3.4 capstone).
pub struct ModelRunner {
    model_run_repo: Arc<dyn ModelRunRepository>,
    model_registry_repo: Arc<dyn ModelRegistryRepository>,
    shadow_comparison_repo: Arc<dyn ShadowComparisonRepository>,
    factory_builder: Arc<dyn ModelRuntimeFactoryBuilder>,
    factor_pipeline: Arc<FactorPipelineService>,
    signal_writer: Arc<SignalCandidateEventWriter>,
    alerts: Arc<dyn InferenceAlertSink>,
    /// Hot-reloadable factor-weight overlay for non-published candidate / shadow.
    weight_overlay: Arc<WeightOverlayApplicator>,
    /// Hot-reloadable favorite-longshot bias table (provenance audit).
    bias_table: Arc<BiasTableApplicator>,
    /// Consecutive classical-shadow inference failures (reset on any success).
    shadow_classical_failures: AtomicU32,
}

impl ModelRunner {
    /// Wire the runner from boot-time dependencies.
    #[must_use]
    pub fn new(deps: ModelRunnerDeps) -> Self {
        Self {
            model_run_repo: deps.model_run_repo,
            model_registry_repo: deps.model_registry_repo,
            shadow_comparison_repo: deps.shadow_comparison_repo,
            factory_builder: deps.factory_builder,
            factor_pipeline: deps.factor_pipeline,
            signal_writer: deps.signal_writer,
            alerts: deps.alerts,
            weight_overlay: deps.weight_overlay,
            bias_table: deps.bias_table,
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
        let factor_schema_hash =
            FactorEngine::new(request.factors, request.features, request.domain, None)
                .factor_schema_hash()?;
        let feature_schema_hash =
            ResearchHasher::feature_schema(&FeatureSchema::build(request.features))?;
        let bias_table_hash = self.bias_table.current_content_hash();
        let binding = ActiveSchemaBinding {
            feature_schema_hash,
            factor_schema_hash: factor_schema_hash.clone(),
            bias_table_hash: bias_table_hash.clone(),
        };

        let active_version_id = match request.model.active_model_version_id.as_ref() {
            Some(reference) => Some(ModelVersionId::try_from(reference)?),
            None => None,
        };

        let model_run_id = ModelRunId::from_v7();
        let input_hash = input_hash(
            &request,
            "live_inference",
            &factor_schema_hash,
            bias_table_hash.as_ref(),
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
                        active_version_id.clone(),
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

    /// Resolve, governance-check, and load the active runtime only far enough to
    /// expose its required features before market selection.
    ///
    /// This does not create a model-run row and does not emit facts; it is a
    /// deterministic precondition for the 04.2 report builder's selection step.
    pub async fn active_requirements(
        &self,
        request: ActiveModelRequirementsRequest<'_>,
    ) -> QuantResult<ActiveModelRequirements> {
        let version_id = request
            .model
            .active_model_version_id
            .as_ref()
            .ok_or_else(|| QuantError::config("model.active_model_version_id is not configured"))
            .and_then(ModelVersionId::try_from)?;
        let version = self
            .model_registry_repo
            .find_model_version_by_id(&version_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(ResearchError::InvalidModelArtifact {
                    detail: format!("active model version {version_id} not found"),
                })
            })?;
        if let Err(reason) = active_load_ok(
            &version,
            request.model.min_quality_gate_age_secs,
            request.as_of,
        ) {
            return Err(QuantError::config(format!(
                "active model {version_id} load denied: {reason}"
            )));
        }

        let binding = ActiveSchemaBinding {
            feature_schema_hash: ResearchHasher::feature_schema(&FeatureSchema::build(
                request.features,
            ))?,
            factor_schema_hash: FactorEngine::new(
                request.factors,
                request.features,
                request.domain,
                None,
            )
            .factor_schema_hash()?,
            bias_table_hash: self.bias_table.current_content_hash(),
        };
        let factory = self.factory_builder.build(binding);
        let runtime = factory
            .load(&version, self.resolve_overlay(&version))
            .await?;

        let mut by_category = HashMap::new();
        for (category, reference) in &request.model.category_model_pointers {
            if let Some(features) = self
                .resolve_category_requirements(*category, reference, &factory, &request)
                .await
            {
                by_category.insert(*category, features);
            }
        }

        Ok(ActiveModelRequirements {
            model_version_id: version_id,
            version,
            model_requirements: ModelFeatureRequirements {
                generic: runtime.required_features(),
                by_category: by_category.into_iter().collect(),
            },
        })
    }

    /// Resolve one category pointer's required features for selection
    /// eligibility, enforcing the same governance a live routed inference
    /// would (11.2.2 remediation R7): the pointer must parse, resolve to a
    /// registered version that passes the active-load gate, and — new in
    /// this remediation — the loaded artifact's `category_scope` must be
    /// exactly this category or `None`. Any failure falls back to `None`
    /// (selection then only checks the generic bar for this category,
    /// mirroring `resolve_model_route`'s own fallback), logged so a
    /// dangling or mis-scoped pointer is never a silent misconfiguration.
    async fn resolve_category_requirements(
        &self,
        category: MarketCategory,
        reference: &ModelVersionRef,
        factory: &Arc<dyn ModelRuntimeFactory>,
        request: &ActiveModelRequirementsRequest<'_>,
    ) -> Option<Vec<FeatureName>> {
        let version_id = match ModelVersionId::try_from(reference) {
            Ok(id) => id,
            Err(error) => {
                tracing::warn!(
                    %category, reference = %reference.id, %error,
                    "category_model_pointers entry is not a valid model version id; \
                     selection falls back to the generic requirement bar for this category"
                );
                return None;
            }
        };
        let version = match self
            .model_registry_repo
            .find_model_version_by_id(&version_id)
            .await
        {
            Ok(Some(version)) => version,
            Ok(None) => {
                tracing::warn!(
                    %category, %version_id,
                    "category pointer model version not found; selection falls back to generic"
                );
                return None;
            }
            Err(error) => {
                tracing::warn!(
                    %category, %version_id, %error,
                    "failed to load category pointer model version; selection falls back to generic"
                );
                return None;
            }
        };
        if let Err(reason) = active_load_ok(
            &version,
            request.model.min_quality_gate_age_secs,
            request.as_of,
        ) {
            tracing::warn!(
                %category, %version_id, %reason,
                "category pointer model failed the active-load gate; selection falls back to generic"
            );
            return None;
        }
        let runtime = match factory.load(&version, self.resolve_overlay(&version)).await {
            Ok(runtime) => runtime,
            Err(error) => {
                tracing::warn!(
                    %category, %version_id, %error,
                    "failed to load category pointer model artifact; selection falls back to generic"
                );
                return None;
            }
        };
        if runtime
            .category_scope()
            .is_some_and(|scope| scope != category)
        {
            tracing::warn!(
                %category, %version_id, category_scope = ?runtime.category_scope(),
                "category pointer model artifact's category_scope disagrees with the pointer's \
                 own category key; selection falls back to generic (fail-closed)"
            );
            return None;
        }
        Some(runtime.required_features())
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
        let factor_result = self
            .factor_pipeline
            .run(FactorPipelineRequest {
                model_run_id,
                vectors: request.feature_vectors,
                feature_vector_ids: request.feature_vector_ids,
                factors: request.factors,
                features: request.features,
                domain: request.domain,
            })
            .await
            .map_err(|error| (InferenceStage::ActiveInference, error))?;

        let factory = self.factory_builder.build(binding.clone());
        let (markets, vectors, outcomes) = align_cross_section(request, &factor_result.outcomes);
        let output = self
            .infer_routed_cross_section(RoutedInference {
                factory: &factory,
                model_run_id,
                request,
                generic_version_id: version_id,
                batch: CrossSectionBatch {
                    markets: &markets,
                    vectors: &vectors,
                    outcomes: &outcomes,
                },
            })
            .await?;

        let version = self.resolve_active_version(version_id, request).await?;
        let runtime = factory
            .load(&version, self.resolve_overlay(&version))
            .await
            .map_err(|error| (InferenceStage::ActiveLoad, error))?;

        let output_hash = ResearchHasher::canonical(&output.candidates)
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
        // The full ranked candidate set feeds the signal-layer shadow comparison.
        let active_candidates = output.candidates.clone();
        let (rows, mut accepted) =
            partition_candidates(output.candidates, floor, min_confidence, event_time);
        annotate(&mut accepted);
        for candidate in &mut accepted {
            attach_rank_scores(candidate);
        }
        let metrics = serde_json::json!({
            "markets_scored": output.runtime_metrics.markets_scored,
            "candidates_emitted": output.runtime_metrics.candidates_emitted,
            "accepted": accepted.len(),
            "inference_duration_ms": output.runtime_metrics.inference_duration_ms,
            "weight_source": runtime.weight_source().as_str(),
        });

        Ok(ActiveResult {
            output_hash,
            metrics,
            accepted,
            emitted,
            active_version_id: version_id.clone(),
            active_index,
            active_candidates,
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
        let shadow_version_id = match ModelVersionId::try_from(reference) {
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
            binding.bias_table_hash.as_ref(),
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

        if let Err(reason) = shadow_load_ok(
            &version,
            request.model.min_quality_gate_age_secs,
            request.as_of,
        ) {
            return Err((
                InferenceStage::ShadowLoad,
                QuantError::config(format!(
                    "shadow model {shadow_version_id} load denied: {reason}"
                )),
            ));
        }

        let factory = self.factory_builder.build(binding.clone());
        let overlay = self.resolve_overlay(&version);
        let runtime = factory
            .load(&version, overlay)
            .await
            .map_err(|error| (InferenceStage::ShadowLoad, error))?;

        let is_classical = runtime.model_family().is_classical();
        let (markets, vectors, outcomes) = align_cross_section(request, &active.outcomes);
        let input = build_runtime_input(
            runtime.as_ref(),
            model_run_id,
            request.as_of,
            &markets,
            &vectors,
            &outcomes,
        );
        let output = self
            .score_shadow(runtime.as_ref(), input, is_classical)
            .await?;

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
            "weight_source": runtime.weight_source().as_str(),
        });
        self.model_run_repo
            .succeed(
                model_run_id,
                output_hash,
                metrics,
                Utc::now(),
                Some(shadow_version_id.clone()),
            )
            .await
            .map_err(|error| (InferenceStage::ShadowInference, QuantError::from(error)))?;
        self.signal_writer.write_batch(rows);

        // Persist the richer signal-layer shadow comparison (TopN overlap + rank /
        // score deltas) and raise a critical alert on a hard divergence. This is
        // governance evidence and best-effort: a persistence hiccup must not fail
        // the isolated shadow path.
        self.persist_shadow_comparison(request, active, shadow_version_id, &output.candidates)
            .await;
        self.maybe_promote_shadow_status(shadow_version_id).await;

        Ok(ShadowRunOutcome {
            model_run_id: Some(model_run_id.clone()),
            emitted,
            diff: Some(diff),
            failure: None,
        })
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

    /// Whether `artifact`'s frozen `category_scope` is exactly `category` or
    /// `None` (11.2.2 remediation R7). `false` (never scores `category`) on
    /// any resolution failure — a category pointer's target must be provably
    /// scoped to that category before it is trusted to score real markets; a
    /// missing version or unloadable artifact fails closed to the generic
    /// route, loudly.
    async fn category_scope_ok(
        &self,
        category: MarketCategory,
        artifact: &ModelVersionId,
        factory: &Arc<dyn ModelRuntimeFactory>,
    ) -> bool {
        let version = match self
            .model_registry_repo
            .find_model_version_by_id(artifact)
            .await
        {
            Ok(Some(version)) => version,
            Ok(None) => {
                tracing::warn!(
                    %category, %artifact,
                    "category pointer model version not found; inference falls back to generic"
                );
                return false;
            }
            Err(error) => {
                tracing::warn!(
                    %category, %artifact, %error,
                    "failed to load category pointer model version; inference falls back to generic"
                );
                return false;
            }
        };
        let runtime = match factory.load(&version, self.resolve_overlay(&version)).await {
            Ok(runtime) => runtime,
            Err(error) => {
                tracing::warn!(
                    %category, %artifact, %error,
                    "failed to load category pointer model artifact; inference falls back to generic"
                );
                return false;
            }
        };
        let scope = runtime.category_scope();
        if scope.is_none_or(|scope| scope == category) {
            true
        } else {
            tracing::warn!(
                %category, %artifact, category_scope = ?scope,
                "category pointer model artifact's category_scope disagrees with the pointer's \
                 own category key; inference falls back to generic (fail-closed)"
            );
            false
        }
    }

    /// Resolve + freshness-check the active model version: look up the registry
    /// row and enforce the load-time quality-gate staleness deny (3.7).
    /// Score the aligned cross-section, honoring `category_model_pointers` with
    /// fallback to the generic active model when a category artifact is missing.
    async fn infer_routed_cross_section(
        &self,
        inputs: RoutedInference<'_>,
    ) -> Result<ModelRuntimeOutput, (InferenceStage, QuantError)> {
        let RoutedInference {
            factory,
            request,
            model_run_id,
            generic_version_id,
            batch,
        } = inputs;

        // Resolve each distinct category's route ONCE (not per-market) and
        // enforce `category_scope` at the exact point a wrong-scoped model
        // would otherwise score real markets (11.2.2 remediation R7): a
        // pointer whose target's spec disagrees with the routed category
        // falls back to generic, loudly, rather than silently scoring with a
        // mis-scoped artifact.
        let mut category_routes: HashMap<MarketCategory, ModelVersionId> = HashMap::new();
        for market in batch.markets {
            if category_routes.contains_key(&market.category) {
                continue;
            }
            let version_id = match resolve_model_route(market.category, request.model) {
                ModelRouting::CategorySpecific { category, artifact } => {
                    if self.category_scope_ok(category, &artifact, factory).await {
                        artifact
                    } else {
                        generic_version_id.clone()
                    }
                }
                ModelRouting::GenericWeighted => generic_version_id.clone(),
            };
            category_routes.insert(market.category, version_id);
        }

        let mut groups: HashMap<ModelVersionId, Vec<usize>> = HashMap::new();
        for (idx, market) in batch.markets.iter().enumerate() {
            let version_id = category_routes
                .get(&market.category)
                .cloned()
                .unwrap_or_else(|| generic_version_id.clone());
            groups.entry(version_id).or_default().push(idx);
        }

        let mut merged_candidates = Vec::new();
        let mut merged_metrics = ModelRuntimeMetrics {
            markets_scored: 0,
            candidates_emitted: 0,
            inference_duration_ms: 0,
        };
        let mut merged_warnings = Vec::new();

        for (version_id, indices) in groups {
            let partition = PartitionInference {
                factory,
                version_id: &version_id,
                request,
                model_run_id,
                indices: &indices,
                batch,
            };
            let output = match self.infer_partition(partition).await {
                Ok(output) => output,
                Err(_) if version_id != *generic_version_id => {
                    self.infer_partition(PartitionInference {
                        factory,
                        version_id: generic_version_id,
                        request,
                        model_run_id,
                        indices: &indices,
                        batch,
                    })
                    .await?
                }
                Err(error) => return Err(error),
            };

            merged_metrics.markets_scored = merged_metrics
                .markets_scored
                .saturating_add(output.runtime_metrics.markets_scored);
            merged_metrics.candidates_emitted = merged_metrics
                .candidates_emitted
                .saturating_add(output.runtime_metrics.candidates_emitted);
            merged_metrics.inference_duration_ms = merged_metrics
                .inference_duration_ms
                .max(output.runtime_metrics.inference_duration_ms);
            merged_warnings.extend(output.warnings);
            merged_candidates.extend(output.candidates);
        }

        Ok(ModelRuntimeOutput {
            candidates: merged_candidates,
            runtime_metrics: merged_metrics,
            warnings: merged_warnings,
        })
    }

    /// Load one model version and score a subset of the cross-section.
    async fn infer_partition(
        &self,
        inputs: PartitionInference<'_>,
    ) -> Result<ModelRuntimeOutput, (InferenceStage, QuantError)> {
        let PartitionInference {
            factory,
            version_id,
            request,
            model_run_id,
            indices,
            batch,
        } = inputs;
        let version = self.resolve_active_version(version_id, request).await?;
        let overlay = self.resolve_overlay(&version);
        let runtime = factory
            .load(&version, overlay)
            .await
            .map_err(|error| (InferenceStage::ActiveLoad, error))?;

        let subset_markets: Vec<_> = indices
            .iter()
            .map(|&idx| batch.markets[idx].clone())
            .collect();
        let subset_vectors: Vec<_> = indices
            .iter()
            .map(|&idx| batch.vectors[idx].clone())
            .collect();
        let subset_outcomes: Vec<_> = indices
            .iter()
            .map(|&idx| batch.outcomes[idx].clone())
            .collect();
        let input = build_runtime_input(
            runtime.as_ref(),
            model_run_id,
            request.as_of,
            &subset_markets,
            &subset_vectors,
            &subset_outcomes,
        );
        runtime
            .infer_batch(input)
            .await
            .map_err(|error| (InferenceStage::ActiveInference, error))
    }

    async fn resolve_active_version(
        &self,
        version_id: &ModelVersionId,
        request: &ModelRunRequest<'_>,
    ) -> Result<ModelVersionInfo, (InferenceStage, QuantError)> {
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
        if let Err(reason) = active_load_ok(
            &version,
            request.model.min_quality_gate_age_secs,
            request.as_of,
        ) {
            return Err((
                InferenceStage::ActiveLoad,
                QuantError::config(format!("active model {version_id} load denied: {reason}")),
            ));
        }
        Ok(version)
    }

    /// Resolve the factor-weight overlay for a version: non-published candidate /
    /// shadow versions may carry a config overlay; a `Published` version always
    /// scores on its frozen artifact weights (overlay forbidden).
    fn resolve_overlay(&self, version: &ModelVersionInfo) -> Option<WeightOverlay> {
        if version.publication_status == PublicationStatus::Published {
            return None;
        }
        self.weight_overlay.overlay_for(&version.model_version_id)
    }

    /// Compute + persist the signal-layer [`ShadowComparison`], alerting on a hard
    /// divergence.
    async fn persist_shadow_comparison(
        &self,
        request: &ModelRunRequest<'_>,
        active: &ActiveResult,
        shadow_version_id: &ModelVersionId,
        shadow_candidates: &[SignalCandidate],
    ) {
        let threshold = match parse_decimal(
            &request.model.shadow_diff_threshold,
            "shadow_diff_threshold",
        ) {
            Ok(threshold) => threshold,
            Err(error) => {
                tracing::warn!(%error, "skipping shadow comparison: invalid shadow_diff_threshold");
                return;
            }
        };
        let comparison = match compute_shadow_comparison(
            active.active_version_id.clone(),
            shadow_version_id.clone(),
            request.as_of,
            &active.active_candidates,
            shadow_candidates,
            request.top_n,
            threshold,
        ) {
            Ok(comparison) => comparison,
            Err(error) => {
                tracing::warn!(%error, "skipping shadow comparison: computation failed");
                return;
            }
        };

        if comparison.hard_divergence {
            self.alerts.critical(
                format!(
                    "shadow model {shadow_version_id} diverged from active {}",
                    active.active_version_id
                ),
                format!(
                    "mean_abs_score_delta={}, topn_overlap={}, side_disagreement_rate={}",
                    comparison.score_delta.mean_abs_score_delta,
                    comparison.topn_overlap.inner(),
                    comparison.score_delta.side_disagreement_rate,
                ),
            );
        }

        if let Err(error) = self
            .shadow_comparison_repo
            .create(new_shadow_comparison(&comparison))
            .await
        {
            tracing::warn!(%error, "failed to persist shadow comparison");
        }
    }

    /// Promote a candidate to `Shadow` after the first successful shadow inference
    /// round (best-effort; idempotent when already `Shadow`).
    async fn maybe_promote_shadow_status(&self, shadow_version_id: &ModelVersionId) {
        let Ok(Some(version)) = self
            .model_registry_repo
            .find_model_version_by_id(shadow_version_id)
            .await
        else {
            return;
        };
        if version.publication_status != PublicationStatus::Candidate {
            return;
        }
        if let Err(error) = self
            .model_registry_repo
            .promote_model_to_shadow(shadow_version_id)
            .await
        {
            tracing::warn!(
                %error,
                %shadow_version_id,
                "failed to promote shadow model to Shadow status"
            );
        }
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

async fn finalize_active_failure(
    model_run_repo: &Arc<dyn ModelRunRepository>,
    alerts: &Arc<dyn InferenceAlertSink>,
    model_run_id: &ModelRunId,
    stage: InferenceStage,
    error: QuantError,
) -> QuantError {
    let _ = model_run_repo
        .fail(
            model_run_id,
            stage.active_error_code(),
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
    if let Some(run_id) = model_run_id {
        let _ = model_run_repo
            .fail(
                run_id,
                stage.shadow_error_code(),
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

/// Split emitted candidates into their fact rows and the accepted subset (score
/// + confidence above the configured floors).
fn partition_candidates(
    candidates: Vec<SignalCandidate>,
    floor: Decimal,
    min_confidence: Decimal,
    event_time: i64,
) -> (Vec<QuantSignalCandidateEventRow>, Vec<SignalCandidate>) {
    let mut rows = Vec::with_capacity(candidates.len());
    let mut accepted = Vec::new();
    for candidate in candidates {
        let reason = rejection_reason(&candidate, floor, min_confidence);
        rows.push(signal_candidate_event(&candidate, reason, event_time));
        if reason.is_empty() {
            accepted.push(candidate);
        }
    }
    (rows, accepted)
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

/// Join the round's factor outcomes with their selection snapshot + feature
/// vector into index-aligned cross-section slices, so the family-specific input
/// builder ([`build_runtime_input`]) sees the same aligned `(market, vector,
/// outcome)` triples the offline replay does.
fn align_cross_section(
    request: &ModelRunRequest<'_>,
    outcomes: &[MarketFactorOutcome],
) -> (
    Vec<SelectedMarket>,
    Vec<FeatureVector>,
    Vec<MarketFactorOutcome>,
) {
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

    let mut markets = Vec::new();
    let mut feature_vectors = Vec::new();
    let mut aligned = Vec::new();
    for outcome in outcomes {
        let (Some(selected), Some(vector)) = (
            selection.get(&outcome.market_id),
            vectors.get(&outcome.market_id),
        ) else {
            continue;
        };
        markets.push((*selected).clone());
        feature_vectors.push((*vector).clone());
        aligned.push(outcome.clone());
    }
    (markets, feature_vectors, aligned)
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
        shadow_comparison_id: comparison.shadow_comparison_id.clone(),
        active_model_version_id: comparison.active_model_version_id.clone(),
        shadow_model_version_id: comparison.shadow_model_version_id.clone(),
        as_of: comparison.as_of,
        topn_overlap: comparison.topn_overlap,
        rank_delta_json: serde_json::to_value(&comparison.rank_delta).unwrap_or_default(),
        score_delta_json: serde_json::to_value(&comparison.score_delta).unwrap_or_default(),
        matured_outcome_json: comparison
            .matured_outcome_delta
            .as_ref()
            .map(|delta| serde_json::to_value(delta).unwrap_or_default()),
        hard_divergence: comparison.hard_divergence,
        comparison_hash: comparison.comparison_hash.clone(),
    }
}

/// Canonical input hash for a run.
fn input_hash(
    request: &ModelRunRequest<'_>,
    run_kind: &str,
    factor_schema_hash: &ContentHash,
    bias_table_hash: Option<&ContentHash>,
    model_version_id: Option<&ModelVersionId>,
) -> QuantResult<ContentHash> {
    #[derive(Serialize)]
    struct Digest<'a> {
        run_kind: &'a str,
        market_selection_id: Option<&'a MarketSelectionId>,
        feature_vector_ids: &'a [FeatureVectorId],
        factor_schema_hash: &'a ContentHash,
        bias_table_hash: Option<&'a ContentHash>,
        model_version_id: Option<&'a ModelVersionId>,
        as_of: DateTime<Utc>,
    }
    ResearchHasher::canonical(&Digest {
        run_kind,
        market_selection_id: request.market_selection_id.as_ref(),
        feature_vector_ids: request.feature_vector_ids,
        factor_schema_hash,
        bias_table_hash,
        model_version_id,
        as_of: request.as_of,
    })
}

/// Parse a governed decimal-string config value, failing closed.
fn parse_decimal(value: &DecimalString, field: &str) -> QuantResult<Decimal> {
    Decimal::from_str(value.value.trim())
        .map_err(|error| QuantError::config(format!("invalid {field} `{}`: {error}", value.value)))
}

#[cfg(test)]
mod tests {
    use crate::governance::quality_gate_staleness_ok;
    use chrono::{Duration, Utc};
    use quant_pivot_models::{
        domain::ModelVersionInfo,
        enums::quant::PublicationStatus,
        types::{ContentHash, ModelSpecId, ModelVersionId},
    };

    fn version(status: PublicationStatus, report: serde_json::Value) -> ModelVersionInfo {
        ModelVersionInfo {
            model_version_id: ModelVersionId::from_v7(),
            model_spec_id: ModelSpecId::from_v7(),
            version: 1,
            artifact_hash: ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("hash"),
            training_dataset_id: None,
            publish_path_set_id: None,
            metrics_json: serde_json::json!({}),
            training_objective_json: serde_json::json!({"kind": "not_trained"}),
            quality_gate_report: report,
            publication_status: status,
            published_at: None,
            retired_at: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn fresh_gate_report_is_accepted_for_candidate() {
        let now = Utc::now();
        let report = serde_json::json!({ "evaluated_at": now.to_rfc3339() });
        assert!(
            quality_gate_staleness_ok(&version(PublicationStatus::Candidate, report), 86_400, now,)
                .is_ok()
        );
    }

    #[test]
    fn stale_quality_gate_report_is_denied_for_candidate() {
        let now = Utc::now();
        let stale = now - Duration::seconds(90_000);
        let report = serde_json::json!({ "evaluated_at": stale.to_rfc3339() });
        assert!(
            quality_gate_staleness_ok(&version(PublicationStatus::Candidate, report), 86_400, now,)
                .is_err()
        );
    }

    #[test]
    fn published_active_is_exempt_from_staleness_deny() {
        let now = Utc::now();
        let stale = now - Duration::seconds(90_000);
        let report = serde_json::json!({ "evaluated_at": stale.to_rfc3339() });
        assert!(
            quality_gate_staleness_ok(&version(PublicationStatus::Published, report), 86_400, now,)
                .is_ok()
        );
    }

    #[test]
    fn zero_budget_disables_the_check() {
        let now = Utc::now();
        let stale = now - Duration::days(365);
        let report = serde_json::json!({ "evaluated_at": stale.to_rfc3339() });
        assert!(
            quality_gate_staleness_ok(&version(PublicationStatus::Candidate, report), 0, now,)
                .is_ok()
        );
    }

    #[test]
    fn absent_report_is_not_subject_to_staleness() {
        let now = Utc::now();
        assert!(
            quality_gate_staleness_ok(
                &version(PublicationStatus::Candidate, serde_json::json!({})),
                86_400,
                now,
            )
            .is_ok()
        );
    }
}
