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
        quant::{ModelVersionInfo, NewModelRun, NewShadowComparison},
    },
    enums::{
        common::{AlertCategory, AlertLevel, AlertSource, MarketCategory},
        model::ModelFamily,
        quant::{ModelRunKind, ModelRunStatus, ModelWeightSource, OutcomeSide, PublicationStatus},
    },
    runtime_config::{
        DomainConfig, FactorCrossSectionConfig, FactorsConfig, FeaturesConfig, ModelConfig,
        ModelVersionRef,
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, FeatureVectorId, MarketId, MarketSelectionId,
        ModelRunId, ModelVersionId, Probability, SignalCandidateId, TokenId,
        shadow::ShadowComparison, stable_name::FeatureName,
    },
};
use quant_pivot_repository::traits::{
    ModelRegistryRepository, ModelRunRepository, ShadowComparisonRepository,
};
use quant_pivot_research::{
    factors::{FactorEngine, FrozenReferenceQuantiles, MarketFactorOutcome},
    features::{FeatureSchema, FeatureVector},
    governance::shadow::{ShadowComparisonRequest, compute_shadow_comparison},
    hashing::ResearchHasher,
    model::{
        ActiveSchemaBinding, InferenceStage, ModelInputAuditRow, ModelRuntimeFactory,
        ModelRuntimeFactoryBuilder, ModelRuntimeInput, ModelRuntimeMetrics, ModelRuntimeOutput,
        QuantModelRuntime, SignalCandidate, WeightOverlay, annotate,
        canonical_business_prediction_hash, signal_candidate_event,
    },
    selection::{ModelFeatureRequirements, SelectedMarket},
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    governance::{BiasTableApplicator, WeightOverlayApplicator, active_load_ok, shadow_load_ok},
    observability::{
        alert_dispatcher::{Alert, AlertDispatcher},
        model_input_fact_writer::ModelInputEventWriter,
        serving_evidence::FeatureEvidenceCommitment,
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
    /// Sole decision/cutoff contract for this inference round.
    pub boundary: DecisionBoundary,
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
    /// Frozen decision time used by load-time governance checks.
    pub decision_at: DateTime<Utc>,
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

struct LoadedRoutes {
    generic_version_id: ModelVersionId,
    category_routes: HashMap<MarketCategory, ModelVersionId>,
    runtimes: HashMap<ModelVersionId, Box<dyn QuantModelRuntime>>,
}

pub(crate) struct AlignedFeatureCrossSection {
    pub(crate) markets: Vec<SelectedMarket>,
    pub(crate) vectors: Vec<FeatureVector>,
    pub(crate) vector_ids: Vec<FeatureVectorId>,
}

struct RoutedFeatureBatch {
    markets: Vec<SelectedMarket>,
    vectors: Vec<FeatureVector>,
    vector_ids: Vec<FeatureVectorId>,
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
    /// `ClickHouse` exact model-input evidence writer.
    pub model_input_writer: Arc<ModelInputEventWriter>,
    /// Operator alert sink for inference degradation.
    pub alerts: Arc<dyn InferenceAlertSink>,
    /// Hot-reloadable factor-weight overlay for non-published candidate / shadow.
    pub weight_overlay: Arc<WeightOverlayApplicator>,
    /// Hot-reloadable favorite-longshot bias table (provenance + factor plane).
    pub bias_table: Arc<BiasTableApplicator>,
}

/// Online inference orchestrator.
pub struct ModelRunner {
    model_run_repo: Arc<dyn ModelRunRepository>,
    model_registry_repo: Arc<dyn ModelRegistryRepository>,
    shadow_comparison_repo: Arc<dyn ShadowComparisonRepository>,
    factory_builder: Arc<dyn ModelRuntimeFactoryBuilder>,
    factor_pipeline: Arc<FactorPipelineService>,
    signal_writer: Arc<SignalCandidateEventWriter>,
    model_input_writer: Arc<ModelInputEventWriter>,
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
            model_input_writer: deps.model_input_writer,
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
            ResearchHasher::feature_schema(&FeatureSchema::build(request.features)?)?;
        let bias_table_hash = self.bias_table.current_content_hash();
        let binding = ActiveSchemaBinding {
            feature_schema_hash,
            factor_schema_hash: factor_schema_hash.clone(),
            bias_table_hash: bias_table_hash.clone(),
        };

        let active_version_id = request
            .model
            .active_model_version_id
            .as_ref()
            .map(|reference| reference.id.clone());

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
                    .succeed(
                        &model_run_id,
                        active.output_hash.clone(),
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
        let version_id = request
            .model
            .active_model_version_id
            .as_ref()
            .ok_or_else(|| QuantError::config("model.active_model_version_id is not configured"))
            .map(|reference| reference.id.clone())?;
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
            request.decision_at,
        ) {
            return Err(QuantError::config(format!(
                "active model {version_id} load denied: {reason}"
            )));
        }

        let binding = ActiveSchemaBinding {
            feature_schema_hash: ResearchHasher::feature_schema(&FeatureSchema::build(
                request.features,
            )?)?,
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
        ensure_production_buy_runtime_family(runtime.as_ref())?;

        let mut by_category = HashMap::new();
        for (category, reference) in &request.model.category_model_pointers {
            let features = self
                .resolve_category_requirements(*category, reference, &factory, &request)
                .await?;
            by_category.insert(*category, features);
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
    /// would: the pointer must parse, resolve to a
    /// registered version that passes the active-load gate; the loaded
    /// artifact's `category_scope` must be
    /// exactly this category. Any configured-pointer failure aborts selection;
    /// substituting the generic model would silently change governed behavior.
    async fn resolve_category_requirements(
        &self,
        category: MarketCategory,
        reference: &ModelVersionRef,
        factory: &Arc<dyn ModelRuntimeFactory>,
        request: &ActiveModelRequirementsRequest<'_>,
    ) -> QuantResult<Vec<FeatureName>> {
        let version_id = reference.id.clone();
        let version = self
            .model_registry_repo
            .find_model_version_by_id(&version_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| ResearchError::InvalidModelArtifact {
                detail: format!("category pointer {category} model version {version_id} not found"),
            })?;
        if let Err(reason) = active_load_ok(
            &version,
            request.model.min_quality_gate_age_secs,
            request.decision_at,
        ) {
            return Err(QuantError::config(format!(
                "category pointer {category} model {version_id} load denied: {reason}"
            )));
        }
        let runtime = factory
            .load(&version, self.resolve_overlay(&version))
            .await?;
        ensure_production_buy_runtime_family(runtime.as_ref())?;
        if runtime.category_scope() != Some(category) {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "category pointer {category} model {version_id} has category_scope {:?}",
                    runtime.category_scope()
                ),
            }
            .into());
        }
        Ok(runtime.required_features())
    }

    /// The active path: load every route → family dispatch → infer → emit.
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
        let factory = self.factory_builder.build(binding.clone());
        // All configured routes are hash/schema/scope validated before the
        // first factor or estimator operation. A category route can never fail
        // after another route has already emitted partial business output.
        let routes = self
            .load_active_routes(version_id, request, &factory)
            .await?;
        let aligned = align_feature_cross_section(request)
            .map_err(|error| (InferenceStage::ActiveInference, error))?;
        let mut output = self
            .infer_loaded_routes(model_run_id, request, &routes, &aligned)
            .await?;
        finalize_candidate_batch(&mut output.candidates)
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

        let floor = request.model.candidate_score_floor.value;
        let min_confidence = request.model.min_model_confidence.value;
        let event_time = Utc::now().timestamp_millis();
        let model_input_rows = project_model_input_rows(
            model_run_id,
            &request.boundary,
            &aligned,
            &output.input_audit,
            event_time,
        )
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
            active_version_id: version_id.clone(),
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
        binding: &ActiveSchemaBinding,
        factor_schema_hash: &ContentHash,
        active: &ActiveResult,
    ) -> Option<ShadowRunOutcome> {
        let reference = request.model.shadow_model_version_id.as_ref()?;
        let shadow_version_id = reference.id.clone();

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
        let runtime = self
            .load_shadow_runtime(request, binding, shadow_version_id)
            .await?;
        let (aligned, mut output) = self
            .score_shadow_cross_section(runtime.as_ref(), model_run_id, request)
            .await?;
        finalize_candidate_batch(&mut output.candidates)
            .map_err(|error| (InferenceStage::ShadowInference, error))?;

        let output_hash = canonical_business_prediction_hash(&output.candidates)
            .map_err(|error| (InferenceStage::ShadowInference, error))?;
        let threshold = request.model.shadow_diff_threshold.value;
        let diff = shadow_diff(&active.active_index, &output.candidates, threshold);

        let event_time = Utc::now().timestamp_millis();
        let model_input_rows = project_model_input_rows(
            model_run_id,
            &request.boundary,
            &aligned,
            &output.input_audit,
            event_time,
        )
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
        self.model_run_repo
            .succeed(
                model_run_id,
                output_hash,
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
        self.persist_shadow_comparison(
            request,
            active,
            shadow_version_id,
            weight_source,
            &output.candidates,
        )
        .await;
        self.maybe_promote_shadow_status(shadow_version_id).await;

        Ok(ShadowRunOutcome {
            model_run_id: Some(model_run_id.clone()),
            emitted,
            diff: Some(diff),
            failure: None,
        })
    }

    async fn load_shadow_runtime(
        &self,
        request: &ModelRunRequest<'_>,
        binding: &ActiveSchemaBinding,
        shadow_version_id: &ModelVersionId,
    ) -> Result<Box<dyn QuantModelRuntime>, (InferenceStage, QuantError)> {
        let registry_entry = self
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
            &registry_entry,
            request.model.min_quality_gate_age_secs,
            request.boundary.decision_at(),
        ) {
            return Err((
                InferenceStage::ShadowLoad,
                QuantError::config(format!(
                    "shadow model {shadow_version_id} load denied: {reason}"
                )),
            ));
        }

        let factory = self.factory_builder.build(binding.clone());
        let overlay = self.resolve_overlay(&registry_entry);
        let runtime = factory
            .load(&registry_entry, overlay)
            .await
            .map_err(|error| (InferenceStage::ShadowLoad, error))?;
        ensure_buy_runtime_family(runtime.as_ref())
            .map_err(|error| (InferenceStage::ShadowLoad, error))?;
        Ok(runtime)
    }

    async fn score_shadow_cross_section(
        &self,
        runtime: &dyn QuantModelRuntime,
        model_run_id: &ModelRunId,
        request: &ModelRunRequest<'_>,
    ) -> Result<(AlignedFeatureCrossSection, ModelRuntimeOutput), (InferenceStage, QuantError)>
    {
        let aligned = align_feature_cross_section(request)
            .map_err(|error| (InferenceStage::ShadowInference, error))?;
        let outcomes = if runtime.model_family() == ModelFamily::WeightedFactor {
            let (cross_section, references) = weighted_factor_contract(runtime)
                .map_err(|error| (InferenceStage::ShadowLoad, error))?;
            let mut factors = request.factors.clone();
            factors.cross_section = cross_section.clone();
            let result = self
                .factor_pipeline
                .run_with_references(
                    FactorPipelineRequest {
                        model_run_id,
                        vectors: &aligned.vectors,
                        feature_vector_ids: &aligned.vector_ids,
                        factors: &factors,
                        features: request.features,
                        domain: request.domain,
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
            runtime,
            model_run_id,
            request.boundary.decision_at(),
            &aligned.markets,
            &aligned.vectors,
            &outcomes,
        );
        let output = self
            .score_shadow(runtime, input, runtime.model_family().is_classical())
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

    /// Resolve and load the generic model plus every configured category route
    /// before computation. Loaded runtimes are retained for the full round so
    /// no route can change or fail between validation and inference.
    async fn load_active_routes(
        &self,
        generic_version_id: &ModelVersionId,
        request: &ModelRunRequest<'_>,
        factory: &Arc<dyn ModelRuntimeFactory>,
    ) -> Result<LoadedRoutes, (InferenceStage, QuantError)> {
        let generic_version = self
            .resolve_active_version(generic_version_id, request)
            .await?;
        let generic_runtime = factory
            .load(&generic_version, self.resolve_overlay(&generic_version))
            .await
            .map_err(|error| (InferenceStage::ActiveLoad, error))?;
        if generic_runtime.category_scope().is_some() {
            return Err((
                InferenceStage::ActiveLoad,
                ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "generic active model {generic_version_id} must not have category_scope {:?}",
                        generic_runtime.category_scope()
                    ),
                }
                .into(),
            ));
        }
        ensure_production_buy_runtime_family(generic_runtime.as_ref())
            .map_err(|error| (InferenceStage::ActiveLoad, error))?;

        let mut category_routes = HashMap::new();
        let mut runtimes = HashMap::from([(generic_version_id.clone(), generic_runtime)]);
        for (category, reference) in &request.model.category_model_pointers {
            let version_id = reference.id.clone();
            let version = self.resolve_active_version(&version_id, request).await?;
            let runtime = factory
                .load(&version, self.resolve_overlay(&version))
                .await
                .map_err(|error| (InferenceStage::ActiveLoad, error))?;
            if runtime.category_scope() != Some(*category) {
                return Err((
                    InferenceStage::ActiveLoad,
                    ResearchError::InvalidModelArtifact {
                        detail: format!(
                            "category pointer {category} model {version_id} has category_scope {:?}",
                            runtime.category_scope()
                        ),
                    }
                    .into(),
                ));
            }
            ensure_production_buy_runtime_family(runtime.as_ref())
                .map_err(|error| (InferenceStage::ActiveLoad, error))?;
            if runtimes.insert(version_id.clone(), runtime).is_some() {
                return Err((
                    InferenceStage::ActiveLoad,
                    ResearchError::InvalidModelArtifact {
                        detail: format!(
                            "model version {version_id} is configured for multiple route scopes"
                        ),
                    }
                    .into(),
                ));
            }
            category_routes.insert(*category, version_id);
        }
        Ok(LoadedRoutes {
            generic_version_id: generic_version_id.clone(),
            category_routes,
            runtimes,
        })
    }

    /// Partition the immutable cross-section by loaded route. Only weighted
    /// partitions enter the factor plane; classical partitions consume frozen
    /// feature transforms directly.
    async fn infer_loaded_routes(
        &self,
        model_run_id: &ModelRunId,
        request: &ModelRunRequest<'_>,
        routes: &LoadedRoutes,
        batch: &AlignedFeatureCrossSection,
    ) -> Result<ModelRuntimeOutput, (InferenceStage, QuantError)> {
        let mut merged = empty_runtime_output();
        for (version_id, indices) in ordered_route_groups(routes, batch) {
            let runtime = routes.runtimes.get(&version_id).ok_or_else(|| {
                (
                    InferenceStage::ActiveLoad,
                    QuantError::from(ResearchError::InvalidModelArtifact {
                        detail: format!("loaded route {version_id} has no runtime"),
                    }),
                )
            })?;
            let routed_batch = routed_feature_batch(batch, &indices)
                .map_err(|error| (InferenceStage::ActiveInference, error))?;
            let output = self
                .infer_route(model_run_id, request, runtime.as_ref(), &routed_batch)
                .await?;
            merge_runtime_output(&mut merged, output)
                .map_err(|error| (InferenceStage::ActiveInference, error))?;
        }
        Ok(merged)
    }

    async fn infer_route(
        &self,
        model_run_id: &ModelRunId,
        request: &ModelRunRequest<'_>,
        runtime: &dyn QuantModelRuntime,
        batch: &RoutedFeatureBatch,
    ) -> Result<ModelRuntimeOutput, (InferenceStage, QuantError)> {
        let outcomes = if runtime.model_family() == ModelFamily::WeightedFactor {
            let (cross_section, references) = weighted_factor_contract(runtime)
                .map_err(|error| (InferenceStage::ActiveLoad, error))?;
            let mut factors = request.factors.clone();
            factors.cross_section = cross_section.clone();
            let result = self
                .factor_pipeline
                .run_with_references(
                    FactorPipelineRequest {
                        model_run_id,
                        vectors: &batch.vectors,
                        feature_vector_ids: &batch.vector_ids,
                        factors: &factors,
                        features: request.features,
                        domain: request.domain,
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
            runtime,
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
            request.boundary.decision_at(),
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
        weight_source: ModelWeightSource,
        shadow_candidates: &[SignalCandidate],
    ) {
        let threshold = request.model.shadow_diff_threshold.value;
        let comparison = match compute_shadow_comparison(ShadowComparisonRequest {
            active_model_version_id: active.active_version_id.clone(),
            shadow_model_version_id: shadow_version_id.clone(),
            weight_source,
            decision_at: request.boundary.decision_at(),
            active: &active.active_candidates,
            shadow: shadow_candidates,
            top_n: request.top_n,
            score_divergence_threshold: threshold,
        }) {
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

        let row = new_shadow_comparison(&comparison);
        if let Err(error) = self.shadow_comparison_repo.create(row).await {
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
            decision_policy_snapshot_id: request.decision_policy_snapshot_id.clone(),
            market_selection_id: request.market_selection_id.clone(),
            window_start: request.boundary.decision_at(),
            window_end: request.boundary.decision_at(),
            status: ModelRunStatus::Running,
            input_hash,
            output_hash: None,
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

/// Bind runtime-produced input evidence to the exact persisted feature vector
/// and decision boundary, then fingerprint the complete replay identity.
pub(crate) fn project_model_input_rows(
    model_run_id: &ModelRunId,
    boundary: &DecisionBoundary,
    aligned: &AlignedFeatureCrossSection,
    audit: &[ModelInputAuditRow],
    event_time: i64,
) -> QuantResult<Vec<QuantModelInputEventRow>> {
    #[derive(Serialize)]
    struct Fingerprint<'a> {
        model_run_id: &'a ModelRunId,
        model_version_id: &'a ModelVersionId,
        feature_vector_id: &'a FeatureVectorId,
        market_id: &'a MarketId,
        model_family: String,
        boundary: &'a DecisionBoundary,
        raw_input_name: &'a str,
        raw_state: &'a str,
        raw_value: Option<&'a str>,
        encoded_column: &'a str,
        encoded_value_bits: Option<u64>,
        input_contract_hash: &'a ContentHash,
        transform_hash: &'a ContentHash,
        training_input_hash: &'a ContentHash,
    }

    let mut vector_by_market = HashMap::with_capacity(aligned.vectors.len());
    for (vector, vector_id) in aligned.vectors.iter().zip(&aligned.vector_ids) {
        if vector_by_market
            .insert(vector.market_id.clone(), vector_id.clone())
            .is_some()
        {
            return Err(ResearchError::Inference {
                detail: format!(
                    "duplicate feature-vector binding for model-input market {}",
                    vector.market_id
                ),
            }
            .into());
        }
    }

    audit
        .iter()
        .map(|row| {
            let feature_vector_id = vector_by_market.get(&row.market_id).ok_or_else(|| {
                ResearchError::Inference {
                    detail: format!(
                        "model-input audit for market {} has no persisted feature-vector binding",
                        row.market_id
                    ),
                }
            })?;
            if row.raw_input_name.is_empty() || row.encoded_column.is_empty() {
                return Err(ResearchError::Inference {
                    detail: format!(
                        "model-input audit for market {} has an empty input/encoded name",
                        row.market_id
                    ),
                }
                .into());
            }
            let model_family = row.model_family.to_string();
            let raw_state = row.raw_state.as_str();
            let fingerprint = ResearchHasher::canonical(&Fingerprint {
                model_run_id,
                model_version_id: &row.model_version_id,
                feature_vector_id,
                market_id: &row.market_id,
                model_family: model_family.clone(),
                boundary,
                raw_input_name: &row.raw_input_name,
                raw_state,
                raw_value: row.raw_value.as_deref(),
                encoded_column: &row.encoded_column,
                encoded_value_bits: row.encoded_value_bits,
                input_contract_hash: &row.input_contract_hash,
                transform_hash: &row.transform_hash,
                training_input_hash: &row.training_input_hash,
            })?;
            Ok(QuantModelInputEventRow {
                event_time,
                decision_at: boundary.decision_at().timestamp_millis(),
                knowledge_cutoff: boundary.knowledge_cutoff().timestamp_millis(),
                model_run_id: model_run_id.clone(),
                model_version_id: row.model_version_id.clone(),
                recommendation_report_id: None,
                market_id: row.market_id.clone(),
                feature_vector_id: feature_vector_id.clone(),
                model_family,
                raw_input_name: row.raw_input_name.clone(),
                raw_state: raw_state.to_owned(),
                raw_value: row.raw_value.clone(),
                encoded_column: row.encoded_column.clone(),
                encoded_value_bits: row.encoded_value_bits,
                input_contract_hash: row.input_contract_hash.to_string(),
                transform_hash: row.transform_hash.to_string(),
                training_input_hash: row.training_input_hash.to_string(),
                audit_fingerprint: fingerprint.to_string(),
                ingestion_time: event_time,
            })
        })
        .collect()
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
            signal_candidate_id: candidate.signal_candidate_id.clone(),
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

/// Establish the one global pre-portfolio order after all category routes have
/// been merged. Runtime-local ranks are not business ranks and must never enter
/// serving evidence or parity hashes.
fn finalize_candidate_batch(candidates: &mut [SignalCandidate]) -> QuantResult<()> {
    candidates.sort_by(|left, right| {
        right
            .composite_score
            .inner()
            .cmp(&left.composite_score.inner())
            .then_with(|| left.market_id.cmp(&right.market_id))
            .then_with(|| left.token_id.cmp(&right.token_id))
            .then_with(|| left.outcome_side.as_str().cmp(right.outcome_side.as_str()))
    });
    for (index, candidate) in candidates.iter_mut().enumerate() {
        candidate.rank_before_portfolio =
            u32::try_from(index + 1).map_err(|error| ResearchError::Inference {
                detail: format!("global candidate rank does not fit u32: {error}"),
            })?;
    }
    annotate(candidates);
    Ok(())
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
/// factor output as the join spine. This is what lets classical routes bypass
/// the factor plane entirely while retaining the exact same market snapshot.
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
        vector_ids.push(vector_id.clone());
    }
    Ok(AlignedFeatureCrossSection {
        markets,
        vectors,
        vector_ids,
    })
}

fn ordered_route_groups(
    routes: &LoadedRoutes,
    batch: &AlignedFeatureCrossSection,
) -> Vec<(ModelVersionId, Vec<usize>)> {
    let mut groups: HashMap<ModelVersionId, Vec<usize>> = HashMap::new();
    for (index, market) in batch.markets.iter().enumerate() {
        let version_id = routes
            .category_routes
            .get(&market.category)
            .unwrap_or(&routes.generic_version_id)
            .clone();
        groups.entry(version_id).or_default().push(index);
    }
    let mut ordered = groups.into_iter().collect::<Vec<_>>();
    ordered.sort_by_key(|(version_id, _)| version_id.to_string());
    ordered
}

fn routed_feature_batch(
    batch: &AlignedFeatureCrossSection,
    indices: &[usize],
) -> QuantResult<RoutedFeatureBatch> {
    let mut routed = RoutedFeatureBatch {
        markets: Vec::with_capacity(indices.len()),
        vectors: Vec::with_capacity(indices.len()),
        vector_ids: Vec::with_capacity(indices.len()),
    };
    for &index in indices {
        let missing = |column: &str| {
            QuantError::from(ResearchError::Inference {
                detail: format!("route index {index} is outside aligned {column} cross-section"),
            })
        };
        routed.markets.push(
            batch
                .markets
                .get(index)
                .ok_or_else(|| missing("market"))?
                .clone(),
        );
        routed.vectors.push(
            batch
                .vectors
                .get(index)
                .ok_or_else(|| missing("feature"))?
                .clone(),
        );
        routed.vector_ids.push(
            batch
                .vector_ids
                .get(index)
                .ok_or_else(|| missing("feature-vector id"))?
                .clone(),
        );
    }
    Ok(routed)
}

const fn empty_runtime_output() -> ModelRuntimeOutput {
    ModelRuntimeOutput {
        candidates: Vec::new(),
        runtime_metrics: ModelRuntimeMetrics {
            markets_scored: 0,
            candidates_emitted: 0,
            inference_duration_ms: 0,
        },
        input_audit: Vec::new(),
    }
}

fn merge_runtime_output(
    merged: &mut ModelRuntimeOutput,
    output: ModelRuntimeOutput,
) -> QuantResult<()> {
    let overflow = |field: &str| {
        QuantError::from(ResearchError::Inference {
            detail: format!("routed model {field} count overflow"),
        })
    };
    merged.runtime_metrics.markets_scored = merged
        .runtime_metrics
        .markets_scored
        .checked_add(output.runtime_metrics.markets_scored)
        .ok_or_else(|| overflow("markets_scored"))?;
    merged.runtime_metrics.candidates_emitted = merged
        .runtime_metrics
        .candidates_emitted
        .checked_add(output.runtime_metrics.candidates_emitted)
        .ok_or_else(|| overflow("candidates_emitted"))?;
    merged.runtime_metrics.inference_duration_ms = merged
        .runtime_metrics
        .inference_duration_ms
        .max(output.runtime_metrics.inference_duration_ms);
    merged.input_audit.extend(output.input_audit);
    merged.candidates.extend(output.candidates);
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

fn ensure_buy_runtime_family(runtime: &dyn QuantModelRuntime) -> QuantResult<()> {
    let family = runtime.model_family();
    if family == ModelFamily::WeightedFactor || family.is_classical() {
        return Ok(());
    }
    Err(ResearchError::InvalidModelArtifact {
        detail: format!("buy-side model runner cannot dispatch family {family}"),
    }
    .into())
}

fn ensure_production_buy_runtime_family(runtime: &dyn QuantModelRuntime) -> QuantResult<()> {
    let family = runtime.model_family();
    if family == ModelFamily::WeightedFactor {
        return Ok(());
    }
    let detail = if family.is_classical() {
        format!(
            "classical family {family} is ShadowOnly until an independently validated probability-to-return/downside calibration is frozen into its artifact"
        )
    } else {
        format!("buy-side model runner cannot dispatch family {family}")
    };
    Err(ResearchError::InvalidModelArtifact { detail }.into())
}

fn weighted_factor_contract(
    runtime: &dyn QuantModelRuntime,
) -> QuantResult<(&FactorCrossSectionConfig, &FrozenReferenceQuantiles)> {
    let cross_section =
        runtime
            .factor_cross_section()
            .ok_or_else(|| ResearchError::InvalidModelArtifact {
                detail: format!(
                    "weighted runtime {} has no frozen cross-section policy",
                    runtime.model_version_id()
                ),
            })?;
    let references = runtime.frozen_reference_quantiles().ok_or_else(|| {
        ResearchError::InvalidModelArtifact {
            detail: format!(
                "weighted runtime {} has no frozen reference distribution contract",
                runtime.model_version_id()
            ),
        }
    })?;
    Ok((cross_section, references))
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
        weight_source: comparison.weight_source,
        decision_at: comparison.decision_at,
        topn_overlap: comparison.topn_overlap,
        rank_delta_json: comparison.rank_delta,
        score_delta_json: comparison.score_delta,
        matured_outcome_json: comparison.matured_outcome_delta,
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
        boundary: &'a DecisionBoundary,
    }
    ResearchHasher::canonical(&Digest {
        run_kind,
        market_selection_id: request.market_selection_id.as_ref(),
        feature_vector_ids: request.feature_vector_ids,
        factor_schema_hash,
        bias_table_hash,
        model_version_id,
        boundary: &request.boundary,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Duration, Utc};
    use quant_pivot_models::{
        domain::{data_plane::DecisionClock, quant::ModelVersionInfo},
        enums::{
            model::ModelFamily,
            quant::{DataQualityStatus, PublicationStatus},
        },
        types::{
            ContentHash, FeatureVectorId, MarketId, ModelRunId, ModelSpecId, ModelVersionId,
            SchemaVersion,
            model_metrics::ModelVersionMetrics,
            model_quality::{
                GateIntent, GateSubject, QUALITY_GATE_REPORT_FORMAT_VERSION, QualityGateReport,
            },
            model_training::ModelTrainingObjective,
        },
    };
    use quant_pivot_research::{
        features::FeatureVector,
        model::{ModelInputAuditRow, ModelInputAuditState},
    };

    use super::{AlignedFeatureCrossSection, project_model_input_rows};
    use crate::{
        governance::quality_gate_staleness_ok,
        test_fixtures::{
            execution_pg_seed::fixture_profile_ref, model_spec_fixtures::model_spec_lineage_fixture,
        },
    };

    fn gate_report(evaluated_at: DateTime<Utc>) -> QualityGateReport {
        QualityGateReport {
            format_version: QUALITY_GATE_REPORT_FORMAT_VERSION,
            subject: GateSubject::ModelVersion(ModelVersionId::from_v7()),
            intent: GateIntent::Candidate,
            evaluated_at,
            gates: Vec::new(),
            hard_failures: Vec::new(),
            soft_warnings: Vec::new(),
            passed: true,
            report_hash: ContentHash::parse(format!("blake3:{}", "1".repeat(64))).expect("hash"),
        }
    }

    fn version(
        status: PublicationStatus,
        quality_gate_report: Option<QualityGateReport>,
    ) -> ModelVersionInfo {
        let (model_spec_thesis, model_spec_definition_hash) =
            model_spec_lineage_fixture("model-runner-test-spec");
        ModelVersionInfo {
            model_version_id: ModelVersionId::from_v7(),
            model_spec_id: ModelSpecId::from_v7(),
            model_spec_name: "model-runner-test-spec".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            model_spec_thesis,
            model_spec_definition_hash,
            version: 1,
            artifact_hash: ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("hash"),
            category_scope: None,
            profile_ref: fixture_profile_ref(),
            training_dataset_id: None,
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            publish_path_set_id: None,
            derivation_kind: ModelVersionInfo::training_derivation_kind(),
            parent_model_version_id: None,
            source_backtest_report_id: None,
            calibration_artifact_id: None,
            score_multiplier_calibration_report: None,
            derivation_evidence_hash: None,
            metrics: ModelVersionMetrics::not_measured("test fixture"),
            training_objective: ModelTrainingObjective::hand_authored("test fixture"),
            quality_gate_report,
            publication_status: status,
            published_at: None,
            retired_at: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn model_input_fingerprint_binds_exact_feature_vector() {
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
        let hash = ContentHash::parse(format!("blake3:{}", "1".repeat(64))).expect("hash");
        let audit = [ModelInputAuditRow {
            model_version_id: ModelVersionId::from_v7(),
            model_family: ModelFamily::WeightedFactor,
            market_id,
            raw_input_name: "factor.liquidity".to_owned(),
            raw_state: ModelInputAuditState::Scored,
            raw_value: Some("1.25".to_owned()),
            encoded_column: "factor.liquidity.normalized_score".to_owned(),
            encoded_value_bits: Some(0.75_f64.to_bits()),
            input_contract_hash: hash.clone(),
            transform_hash: hash.clone(),
            training_input_hash: hash,
        }];
        let first_id = FeatureVectorId::from_v7();
        let first = AlignedFeatureCrossSection {
            markets: Vec::new(),
            vectors: vec![vector.clone()],
            vector_ids: vec![first_id.clone()],
        };
        let boundary = DecisionClock::new(7)
            .boundary(decision_at)
            .expect("boundary");
        let model_run_id = ModelRunId::from_v7();
        let first_row = project_model_input_rows(
            &model_run_id,
            &boundary,
            &first,
            &audit,
            decision_at.timestamp_millis(),
        )
        .expect("projection")
        .pop()
        .expect("row");
        assert_eq!(first_row.feature_vector_id, first_id);
        assert!(!first_row.audit_fingerprint.is_empty());

        let second = AlignedFeatureCrossSection {
            markets: Vec::new(),
            vectors: vec![vector],
            vector_ids: vec![FeatureVectorId::from_v7()],
        };
        let second_row = project_model_input_rows(
            &model_run_id,
            &boundary,
            &second,
            &audit,
            decision_at.timestamp_millis(),
        )
        .expect("projection")
        .pop()
        .expect("row");
        assert_ne!(
            first_row.audit_fingerprint, second_row.audit_fingerprint,
            "feature-vector identity must participate in the audit fingerprint"
        );
    }

    #[test]
    fn fresh_gate_report_is_accepted_for_candidate() {
        let now = Utc::now();
        assert!(
            quality_gate_staleness_ok(
                &version(PublicationStatus::Candidate, Some(gate_report(now))),
                86_400,
                now,
            )
            .is_ok()
        );
    }

    #[test]
    fn stale_quality_gate_report_is_denied_for_candidate() {
        let now = Utc::now();
        let stale = now - Duration::seconds(90_000);
        assert!(
            quality_gate_staleness_ok(
                &version(PublicationStatus::Candidate, Some(gate_report(stale))),
                86_400,
                now,
            )
            .is_err()
        );
    }

    #[test]
    fn published_active_is_exempt_from_staleness_deny() {
        let now = Utc::now();
        let stale = now - Duration::seconds(90_000);
        assert!(
            quality_gate_staleness_ok(
                &version(PublicationStatus::Published, Some(gate_report(stale))),
                86_400,
                now,
            )
            .is_ok()
        );
    }

    #[test]
    fn zero_budget_disables_the_check() {
        let now = Utc::now();
        let stale = now - Duration::days(365);
        assert!(
            quality_gate_staleness_ok(
                &version(PublicationStatus::Candidate, Some(gate_report(stale))),
                0,
                now,
            )
            .is_ok()
        );
    }

    #[test]
    fn absent_report_is_not_subject_to_staleness() {
        let now = Utc::now();
        assert!(
            quality_gate_staleness_ok(&version(PublicationStatus::Candidate, None), 86_400, now,)
                .is_ok()
        );
    }
}
