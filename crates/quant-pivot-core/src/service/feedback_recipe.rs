//! Production execution of governed feedback recipe planning.

use std::{cmp::Ordering, collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_compute::OFFLINE_MEMORY_BYTES;
use quant_pivot_error::{
    QuantError, QuantResult, feedback::FeedbackError, research::ResearchError,
    storage::StorageError,
};
use quant_pivot_models::{
    domain::{
        ports::{
            CandidateRecipePlanArtifact, CandidateRecipePlanExecutionPort,
            CandidateRecipePlanExecutionResult, CandidateRecipePlanJobParams,
            CandidateRecipePlanOutcome, CandidateRecipeReadinessBlocker, CandidateRecipeSelection,
            FeedbackAttributionManifest, FeedbackAttributionUse, FeedbackCandidateFamily,
            FeedbackCandidateFamilyInput, FeedbackCandidateRecipe, FeedbackCandidateRecipeInput,
            FeedbackComparisonContract, FeedbackDatasetBuildRequest,
            FeedbackRecipeDiagnosticEvidence, FeedbackRecipeOosEvidence, FeedbackRecipeOosSummary,
            FeedbackRecipeTemplate,
        },
        quant::{
            FeedbackCohortWindow, FeedbackCycleInfo, JobProgressSink, ModelSpecInfo,
            ResearchJobArtifactRef, ResearchJobInfo,
        },
    },
    enums::quant::{
        DatasetPurpose, FeedbackDriftMetric, FeedbackEvaluationMode, FeedbackRecipeTemplateStatus,
        FeedbackStage, FeedbackStageEventKind, ResearchJobKind, ResearchJobResultKind,
        ResearchJobStatus,
    },
    hashing::CanonicalDigest,
    runtime_config::ResearchValidationConfig,
    types::{
        ContentHash, DATASET_ARTIFACT_FORMAT_VERSION, DatasetSourceLineage, ResearchJobParams,
        ResearchJobProgress, ResearchProfileArtifact, TrainingDatasetId,
    },
};
use quant_pivot_repository::traits::{
    FeedbackCycleRepository, FeedbackRecipeTemplateRepository, ModelRegistryRepository,
    PolicyRepository, ResearchJobRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    attribution::{AttributionArtifact, AttributionArtifactCodec},
    feedback::{DriftGateOutcome, FeedbackDriftCodec},
    feedback_comparison::{FeedbackComparisonCodec, RomanoWolfOutcome},
    feedback_governance::FeedbackGovernanceCodec,
    feedback_recipe::CandidateRecipePlanCodec,
};
use tokio_util::sync::CancellationToken;

use crate::{
    app::ports::{
        feedback_mutation::FeedbackCycleFreezePlan,
        training_dataset::{CoreTrainingDatasetPort, FeedbackSourceFreeze},
    },
    service::model_serving_preimage::ModelServingPreimageService,
};

const DATASET_PLAN_DOMAIN: &str = "quant-pivot/feedback-dataset-plan";
const DATASET_PLAN_VERSION: u32 = 2;

pub struct CandidateRecipePlanExecutionDeps {
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub templates: Arc<dyn FeedbackRecipeTemplateRepository>,
    pub models: Arc<dyn ModelRegistryRepository>,
    pub jobs: Arc<dyn ResearchJobRepository>,
    pub policies: Arc<dyn PolicyRepository>,
    pub training_datasets: Arc<CoreTrainingDatasetPort>,
    pub serving_preimages: Arc<ModelServingPreimageService>,
    pub artifacts: Arc<dyn ArtifactStore>,
}

pub struct CandidateRecipePlanExecutionService {
    cycles: Arc<dyn FeedbackCycleRepository>,
    templates: Arc<dyn FeedbackRecipeTemplateRepository>,
    models: Arc<dyn ModelRegistryRepository>,
    jobs: Arc<dyn ResearchJobRepository>,
    policies: Arc<dyn PolicyRepository>,
    training_datasets: Arc<CoreTrainingDatasetPort>,
    serving_preimages: Arc<ModelServingPreimageService>,
    artifacts: Arc<dyn ArtifactStore>,
}

struct CandidateRecipeSealInput<'a> {
    params: &'a CandidateRecipePlanJobParams,
    cycle: &'a FeedbackCycleInfo,
    profile: &'a ResearchProfileArtifact,
    templates: Vec<SelectedRecipeTemplate>,
    plan: &'a FeedbackCycleFreezePlan,
    source_lineage: &'a DatasetSourceLineage,
}

enum RecipeTemplateSelection {
    Ready(Vec<SelectedRecipeTemplate>),
    NoAction(CandidateRecipeReadinessBlocker),
}

struct AvailableDiagnosticEvidence {
    use_: FeedbackAttributionUse,
    feature_names: Vec<String>,
}

struct SelectedRecipeTemplate {
    template: FeedbackRecipeTemplate,
    matched_triggers: Vec<FeedbackDriftMetric>,
    diagnostic_evidence: Vec<FeedbackRecipeDiagnosticEvidence>,
    historical_oos: Option<FeedbackRecipeOosSummary>,
}

impl SelectedRecipeTemplate {
    fn stable_order(&self, other: &Self) -> Ordering {
        let self_trigger_match = !self.matched_triggers.is_empty();
        let other_trigger_match = !other.matched_triggers.is_empty();
        let self_lower = self
            .historical_oos
            .as_ref()
            .map(|summary| summary.lower_bound_bps);
        let other_lower = other
            .historical_oos
            .as_ref()
            .map(|summary| summary.lower_bound_bps);
        other_trigger_match
            .cmp(&self_trigger_match)
            .then_with(|| other_lower.cmp(&self_lower))
            .then_with(|| {
                self.template
                    .catalog_priority
                    .cmp(&other.template.catalog_priority)
            })
            .then_with(|| {
                self.template
                    .recipe_template_id
                    .cmp(&other.template.recipe_template_id)
            })
    }
}

impl CandidateRecipePlanExecutionService {
    #[must_use]
    pub fn new(deps: CandidateRecipePlanExecutionDeps) -> Self {
        Self {
            cycles: deps.cycles,
            templates: deps.templates,
            models: deps.models,
            jobs: deps.jobs,
            policies: deps.policies,
            training_datasets: deps.training_datasets,
            serving_preimages: deps.serving_preimages,
            artifacts: deps.artifacts,
        }
    }

    async fn build_artifact(
        &self,
        params: &CandidateRecipePlanJobParams,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<CandidateRecipePlanArtifact> {
        Self::require_active(cancel)?;
        let cycle = self
            .cycles
            .find_cycle(&params.feedback_cycle_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found("quant_feedback_cycle", params.feedback_cycle_id)
            })?;
        Self::require_cycle(&cycle, params)?;
        let champion = self
            .models
            .find_model_version(&cycle.champion_model_version_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found("quant_model_version", cycle.champion_model_version_id)
            })?;
        let preimage = self.serving_preimages.load(&champion).await?;
        preimage.verify_feedback_cycle(&cycle)?;
        let profile = preimage.profile().clone();
        let model_spec = preimage.model_spec();
        let bundle = self
            .policies
            .load_current_bundle()
            .await?
            .ok_or_else(|| Self::invalid("recipe-plan has no active policy bundle"))?;
        let route = bundle
            .snapshot
            .model_routing
            .model
            .route_binding(cycle.route)
            .map_err(|error| Self::invalid(error.to_string()))?;
        let policy_generation_exact = bundle.generation == cycle.policy_bundle_generation;
        if !policy_generation_exact
            || bundle.decision_policy_snapshot_id != cycle.decision_policy_snapshot_id
            || bundle.snapshot_hash != cycle.decision_policy_snapshot_hash
            || route.champion.model_version_id != cycle.champion_model_version_id
            || i64::try_from(route.champion.generation)
                .map_err(|error| Self::invalid(format!("route generation overflow: {error}")))?
                != cycle.route_generation
        {
            return Self::no_action(
                params,
                &cycle,
                CandidateRecipeReadinessBlocker::RouteStateStale,
            );
        }
        let active_profiles = bundle
            .snapshot
            .profile_artifacts
            .references()
            .map_err(|error| Self::invalid(error.to_string()))?;
        let champion_profiles = &preimage
            .artifact()
            .header()
            .serving_contract()
            .bindings()
            .policy_snapshot
            .profile_artifacts;
        if &active_profiles != champion_profiles {
            return Self::no_action(
                params,
                &cycle,
                CandidateRecipeReadinessBlocker::RouteStateStale,
            );
        }
        if route.shadow.is_some() {
            return Self::no_action(
                params,
                &cycle,
                CandidateRecipeReadinessBlocker::ShadowOccupied,
            );
        }
        let attribution = self.verify_attribution(&cycle, params).await?;
        self.verify_drift(&cycle, params).await?;
        let diagnostics = self.load_diagnostic_evidence(&cycle, &attribution).await?;
        let templates = match self
            .select_templates(
                &cycle,
                params,
                &profile,
                model_spec,
                &bundle
                    .snapshot
                    .profile_artifacts
                    .research_method
                    .research
                    .validation,
                &diagnostics,
            )
            .await?
        {
            RecipeTemplateSelection::Ready(templates) => templates,
            RecipeTemplateSelection::NoAction(blocker) => {
                return Self::no_action(params, &cycle, blocker);
            }
        };
        progress.report(ResearchJobProgress::with_total(
            "recipe-source-freeze",
            0,
            1,
        ));
        let plan = FeedbackCycleFreezePlan::derive_at_cutoff(
            &profile,
            cycle.champion_model_spec_id,
            cycle.champion_model_spec_definition_hash,
            cycle.decision_policy_snapshot_id,
            cycle.decision_policy_snapshot_hash,
            cycle.label_cutoff,
        )?;
        let source_lineage = self
            .training_datasets
            .freeze_feedback_source(
                FeedbackSourceFreeze {
                    profile: &profile,
                    runtime: &bundle.snapshot,
                    decision_policy_snapshot_id: bundle.decision_policy_snapshot_id,
                    runtime_config_hash: bundle.snapshot_hash,
                    research_program_hash: plan.research_program_hash(),
                    window_start: plan.source_start(),
                    window_end: plan.label_cutoff(),
                    pit_cutoff: plan.label_cutoff(),
                },
                cancel,
            )
            .await?;
        Self::require_active(cancel)?;
        Self::seal_plan(CandidateRecipeSealInput {
            params,
            cycle: &cycle,
            profile: &profile,
            templates,
            plan: &plan,
            source_lineage: &source_lineage,
        })
    }

    async fn select_templates(
        &self,
        cycle: &FeedbackCycleInfo,
        params: &CandidateRecipePlanJobParams,
        profile: &ResearchProfileArtifact,
        model_spec: &ModelSpecInfo,
        validation: &ResearchValidationConfig,
        diagnostics: &[AvailableDiagnosticEvidence],
    ) -> QuantResult<RecipeTemplateSelection> {
        let mut templates = self
            .templates
            .list_approved(&cycle.profile_ref, cycle.route, cycle.champion_model_family)
            .await?;
        let had_approved = !templates.is_empty();
        templates.retain(|template| {
            template.status == FeedbackRecipeTemplateStatus::Approved
                && template.training_spec.model_spec_id == cycle.champion_model_spec_id
                && template.training_spec.model_spec_definition_hash
                    == cycle.champion_model_spec_definition_hash
                && template.training_spec.input_contract == model_spec.input_contract
                && template.training_spec.training_contract == model_spec.training_contract
                && template.training_spec.training_window_days <= profile.spec.fit_span_days
                && template.calibration_spec.calibration_window_days
                    <= profile.spec.feedback_policy.evaluation_window_days
                && template.cpcv_spec.validation == *validation
                && template.cpcv_spec.target_horizon_secs == profile.spec.target_horizon_secs
                && template.cpcv_spec.purge_embargo_secs == profile.spec.purge_embargo_secs
        });
        if templates.is_empty() {
            return Ok(RecipeTemplateSelection::NoAction(if had_approved {
                CandidateRecipeReadinessBlocker::CatalogRevisionStale
            } else {
                CandidateRecipeReadinessBlocker::NoApprovedTemplate
            }));
        }
        let platform_memory_bytes = u64::try_from(OFFLINE_MEMORY_BYTES)
            .map_err(|error| Self::invalid(format!("offline memory budget overflow: {error}")))?;
        templates.retain(|template| {
            template.resource_budget.max_concurrency == 1
                && template.resource_budget.max_working_set_bytes == platform_memory_bytes
                && template.resource_budget.deadline_secs <= 86_400
        });
        if templates.is_empty() {
            return Ok(RecipeTemplateSelection::NoAction(
                CandidateRecipeReadinessBlocker::ResourceBudgetUnsupported,
            ));
        }
        let trigger_compatible = templates
            .into_iter()
            .filter_map(|template| {
                let matched_triggers = template
                    .responsive_triggers
                    .iter()
                    .filter(|trigger| params.drift.exceeded_metrics.contains(trigger))
                    .copied()
                    .collect::<Vec<_>>();
                (params.evaluation_mode == FeedbackEvaluationMode::ForcedRetraining
                    || !matched_triggers.is_empty())
                .then_some((template, matched_triggers))
            })
            .collect::<Vec<_>>();
        if trigger_compatible.is_empty() {
            return Ok(RecipeTemplateSelection::NoAction(
                CandidateRecipeReadinessBlocker::NoTriggerCompatibleTemplate,
            ));
        }
        let mut selected = Vec::with_capacity(trigger_compatible.len());
        for (template, matched_triggers) in trigger_compatible {
            let Some(diagnostic_evidence) = Self::match_diagnostics(&template, diagnostics)? else {
                continue;
            };
            let historical_oos = self
                .historical_oos(cycle, cycle.label_cutoff, &template, &diagnostic_evidence)
                .await?;
            selected.push(SelectedRecipeTemplate {
                template,
                matched_triggers,
                diagnostic_evidence,
                historical_oos,
            });
        }
        if selected.is_empty() {
            return Ok(RecipeTemplateSelection::NoAction(
                CandidateRecipeReadinessBlocker::NoDiagnosticCompatibleTemplate,
            ));
        }
        selected.sort_by(SelectedRecipeTemplate::stable_order);
        let max_challengers = usize::try_from(params.max_challengers)
            .map_err(|error| Self::invalid(format!("challenger bound overflow: {error}")))?;
        selected.truncate(max_challengers);
        Ok(RecipeTemplateSelection::Ready(selected))
    }

    fn seal_plan(input: CandidateRecipeSealInput<'_>) -> QuantResult<CandidateRecipePlanArtifact> {
        let CandidateRecipeSealInput {
            params,
            cycle,
            profile,
            templates,
            plan,
            source_lineage,
        } = input;
        let evaluation = Self::dataset_request(
            DatasetPurpose::Evaluation,
            plan.evaluation().clone(),
            cycle,
            source_lineage,
            None,
        )?;
        let mut candidates = Vec::with_capacity(templates.len());
        let mut selections = Vec::with_capacity(templates.len());
        for selected in templates {
            let template = selected.template;
            let training_window = Self::bounded_window(
                plan.training(),
                template.training_spec.training_window_days,
                "training",
            )?;
            let calibration_window = Self::bounded_window(
                plan.calibration(),
                template.calibration_spec.calibration_window_days,
                "calibration",
            )?;
            let training = Self::dataset_request(
                DatasetPurpose::Training,
                training_window,
                cycle,
                source_lineage,
                Some(template.template_hash),
            )?;
            let calibration = Self::dataset_request(
                DatasetPurpose::Calibration,
                calibration_window,
                cycle,
                source_lineage,
                Some(template.template_hash),
            )?;
            let planner_evidence_hash = CandidateRecipeSelection::planner_evidence_hash(
                template.template_hash,
                params.attribution.use_set_hash,
                &selected.matched_triggers,
                &selected.diagnostic_evidence,
                &selected.historical_oos,
            )?;
            let recipe = FeedbackCandidateRecipe::try_seal(FeedbackCandidateRecipeInput {
                recipe_template_hash: template.template_hash,
                planner_evidence_hash,
                resource_budget: template.resource_budget,
                training,
                calibration,
                calibration_method: template.calibration_spec.method,
                cpcv_spec: template.cpcv_spec.clone(),
                downside_source: template.downside_spec.source,
                decision_policy_snapshot_id: cycle.decision_policy_snapshot_id,
            })?;
            let candidate_recipe_hash = recipe.candidate_recipe_hash();
            candidates.push(recipe);
            selections.push(CandidateRecipeSelection::try_new(
                template,
                candidate_recipe_hash,
                params.attribution.use_set_hash,
                selected.matched_triggers,
                selected.diagnostic_evidence,
                selected.historical_oos,
            )?);
        }
        selections.sort_by(selection_order);
        let candidate_family = FeedbackCandidateFamily::try_seal(FeedbackCandidateFamilyInput {
            shared_evaluation: evaluation,
            comparison_contract: FeedbackComparisonContract::try_from_policy(
                &profile.spec.feedback_policy,
            )?,
            candidates,
        })?;
        let artifact = CandidateRecipePlanArtifact {
            format_version: CandidateRecipePlanArtifact::FORMAT_VERSION,
            artifact_id: params.artifact_id,
            feedback_cycle_id: cycle.feedback_cycle_id,
            cycle_idempotency_hash: cycle.idempotency_hash,
            input_hash: params.input_hash()?,
            label_cutoff: cycle.label_cutoff,
            planned_at: params.planned_at,
            evaluation_mode: cycle.evaluation_mode,
            profile_ref: cycle.profile_ref.clone(),
            route: cycle.route,
            model_family: cycle.champion_model_family,
            attribution: params.attribution.clone(),
            drift: params.drift.clone(),
            outcome: CandidateRecipePlanOutcome::Ready {
                candidate_family: Box::new(candidate_family),
                selections,
            },
        };
        artifact.validate()?;
        Ok(artifact)
    }

    async fn load_diagnostic_evidence(
        &self,
        cycle: &FeedbackCycleInfo,
        attribution: &FeedbackAttributionManifest,
    ) -> QuantResult<Vec<AvailableDiagnosticEvidence>> {
        let mut evidence = Vec::with_capacity(attribution.uses.len());
        for use_ in &attribution.uses {
            let bytes = self.artifacts.get(&use_.artifact_uri).await?;
            Self::require_hash(use_.artifact_hash, AttributionArtifactCodec::hash(&bytes))?;
            let artifact = AttributionArtifactCodec::decode(&bytes)?;
            let lineage = artifact.lineage();
            if artifact.kind() != use_.artifact_kind
                || lineage.source_feedback_cycle_id != use_.source_feedback_cycle_id
                || lineage.source_cohort != use_.source_cohort
                || lineage.source_cutoff != use_.source_cutoff
                || use_.available_at > cycle.label_cutoff
            {
                return Err(Self::invalid(
                    "recipe diagnostic payload differs from its PIT attribution use",
                ));
            }
            let feature_names = match artifact {
                AttributionArtifact::PredictionExplanation(artifact) => artifact
                    .contributions
                    .iter()
                    .map(|contribution| contribution.input_name.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
                AttributionArtifact::DecisionInterventionReplay(artifact) => artifact
                    .interventions
                    .iter()
                    .map(|intervention| intervention.input_name.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
                AttributionArtifact::ResolutionOutcomeAssociation(_)
                | AttributionArtifact::ExecutionOutcomeAssociation(_)
                | AttributionArtifact::ExecutionTrajectory(_)
                | AttributionArtifact::PolicyCounterfactualOutcome(_) => Vec::new(),
            };
            evidence.push(AvailableDiagnosticEvidence {
                use_: use_.clone(),
                feature_names,
            });
        }
        evidence.sort_by_key(|item| item.use_.artifact_hash);
        Ok(evidence)
    }

    fn match_diagnostics(
        template: &FeedbackRecipeTemplate,
        available: &[AvailableDiagnosticEvidence],
    ) -> QuantResult<Option<Vec<FeedbackRecipeDiagnosticEvidence>>> {
        let spec = &template.diagnostic_spec;
        let responsive = spec
            .responsive_feature_names
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let requires_features = !responsive.is_empty();
        let mut matched_features = BTreeSet::new();
        let mut evidence = Vec::new();
        for available in available {
            if !spec
                .accepted_artifact_kinds
                .contains(&available.use_.artifact_kind)
            {
                continue;
            }
            let item_features = available
                .feature_names
                .iter()
                .filter(|name| responsive.contains(name.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            if requires_features && item_features.is_empty() {
                continue;
            }
            matched_features.extend(item_features.iter().cloned());
            evidence.push(FeedbackRecipeDiagnosticEvidence {
                source_feedback_cycle_id: available.use_.source_feedback_cycle_id,
                artifact_kind: available.use_.artifact_kind,
                source_cohort: available.use_.source_cohort,
                artifact_hash: available.use_.artifact_hash,
                available_at: available.use_.available_at,
                matched_feature_names: item_features,
            });
        }
        let evidence_count = u32::try_from(evidence.len()).map_err(|error| {
            Self::invalid(format!("diagnostic evidence count overflow: {error}"))
        })?;
        let feature_count = u32::try_from(matched_features.len()).map_err(|error| {
            Self::invalid(format!("diagnostic feature count overflow: {error}"))
        })?;
        if evidence_count < spec.minimum_evidence_count
            || feature_count < spec.minimum_feature_matches
        {
            return Ok(None);
        }
        evidence.sort_by_key(|item| item.artifact_hash);
        for item in &evidence {
            item.validate()?;
        }
        Ok(Some(evidence))
    }

    async fn historical_oos(
        &self,
        cycle: &FeedbackCycleInfo,
        cutoff: DateTime<Utc>,
        template: &FeedbackRecipeTemplate,
        diagnostics: &[FeedbackRecipeDiagnosticEvidence],
    ) -> QuantResult<Option<FeedbackRecipeOosSummary>> {
        let mut source_cycles = diagnostics
            .iter()
            .map(|evidence| evidence.source_feedback_cycle_id)
            .collect::<Vec<_>>();
        source_cycles.sort_by_key(|cycle_id| cycle_id.as_uuid());
        source_cycles.dedup();
        let mut evidence = Vec::new();
        for source_cycle_id in source_cycles {
            let Some(source_cycle) = self.cycles.find_cycle(&source_cycle_id).await? else {
                return Err(
                    StorageError::not_found("quant_feedback_cycle", source_cycle_id).into(),
                );
            };
            if source_cycle.feedback_cycle_id == cycle.feedback_cycle_id
                || source_cycle.profile_ref != cycle.profile_ref
                || source_cycle.route != cycle.route
                || source_cycle.champion_model_family != cycle.champion_model_family
                || source_cycle.label_cutoff >= cycle.label_cutoff
            {
                return Err(Self::invalid(
                    "historical OOS source violates cycle/profile/route/family PIT isolation",
                ));
            }
            let Some((plan_job, plan_ref, _)) = self
                .historical_stage(
                    &source_cycle,
                    FeedbackStage::RecipePlan,
                    ResearchJobKind::FeedbackRecipePlan,
                    ResearchJobResultKind::CandidateRecipePlanArtifact,
                    cutoff,
                )
                .await?
            else {
                continue;
            };
            let ResearchJobParams::FeedbackRecipePlan(plan_params) = &plan_job.params_json else {
                return Err(Self::invalid(
                    "historical RecipePlan job lost its typed parameters",
                ));
            };
            let plan_bytes = self.artifacts.get(&plan_ref.uri).await?;
            Self::require_hash(
                plan_ref.content_hash,
                CanonicalDigest::content_hash_bytes(&plan_bytes),
            )?;
            let plan = CandidateRecipePlanCodec::decode(&plan_bytes)?;
            if plan.feedback_cycle_id != source_cycle_id
                || plan.artifact_id != plan_params.artifact_id
                || plan.input_hash != plan_params.input_hash()?
            {
                return Err(Self::invalid(
                    "historical RecipePlan differs from its terminal job",
                ));
            }
            let Some(selection) = plan.selections().and_then(|selections| {
                selections
                    .iter()
                    .find(|selection| selection.template.template_hash == template.template_hash)
            }) else {
                continue;
            };
            let Some((comparison_job, comparison_ref, available_at)) = self
                .historical_stage(
                    &source_cycle,
                    FeedbackStage::Comparison,
                    ResearchJobKind::FeedbackComparison,
                    ResearchJobResultKind::FeedbackComparisonArtifact,
                    cutoff,
                )
                .await?
            else {
                continue;
            };
            let ResearchJobParams::FeedbackComparison(comparison_params) =
                &comparison_job.params_json
            else {
                return Err(Self::invalid(
                    "historical Comparison job lost its typed parameters",
                ));
            };
            let comparison_bytes = self.artifacts.get(&comparison_ref.uri).await?;
            Self::require_hash(
                comparison_ref.content_hash,
                FeedbackComparisonCodec::bytes_hash(&comparison_bytes),
            )?;
            let comparison = FeedbackComparisonCodec::decode(&comparison_bytes)?;
            comparison.validate_for(comparison_params)?;
            let RomanoWolfOutcome::Compared {
                evidence: comparison_evidence,
            } = comparison.outcome()
            else {
                continue;
            };
            let Some(result) = comparison_evidence
                .candidates
                .iter()
                .find(|result| result.candidate_recipe_hash == selection.candidate_recipe_hash)
            else {
                return Err(Self::invalid(
                    "historical Comparison omitted an exact-template candidate",
                ));
            };
            evidence.push(FeedbackRecipeOosEvidence {
                source_feedback_cycle_id: source_cycle_id,
                recipe_plan_artifact_hash: plan_ref.content_hash,
                comparison_artifact_hash: comparison_ref.content_hash,
                candidate_recipe_hash: selection.candidate_recipe_hash,
                simultaneous_lower_bound_bps: result.simultaneous_lower_bound_bps,
                available_at,
            });
        }
        if evidence.is_empty() {
            Ok(None)
        } else {
            FeedbackRecipeOosSummary::try_new(evidence)
                .map(Some)
                .map_err(Into::into)
        }
    }

    async fn historical_stage(
        &self,
        cycle: &FeedbackCycleInfo,
        stage: FeedbackStage,
        kind: ResearchJobKind,
        result_kind: ResearchJobResultKind,
        cutoff: DateTime<Utc>,
    ) -> QuantResult<Option<(ResearchJobInfo, ResearchJobArtifactRef, DateTime<Utc>)>> {
        let events = self
            .cycles
            .list_stage_events(&cycle.feedback_cycle_id)
            .await?;
        let Some(event) = events.iter().rev().find(|event| {
            event.stage == stage && event.event_kind == FeedbackStageEventKind::Succeeded
        }) else {
            return Ok(None);
        };
        event.validate()?;
        if event.occurred_at > cutoff {
            return Ok(None);
        }
        let job_id = event
            .research_job_id
            .ok_or_else(|| Self::invalid(format!("historical {stage} event has no job")))?;
        let job = self
            .jobs
            .find_by_id(&job_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_research_job", job_id))?;
        let artifact = job.result_artifact().ok_or_else(|| {
            Self::invalid(format!("historical {stage} job has no terminal artifact"))
        })?;
        if job.feedback_cycle_id != Some(cycle.feedback_cycle_id)
            || job.feedback_stage != Some(stage)
            || job.kind != kind
            || job.status != ResearchJobStatus::Succeeded
            || job.result().is_none_or(|result| result.kind != result_kind)
            || job
                .finished_at
                .is_none_or(|finished_at| finished_at > cutoff)
            || event.evidence_uri.as_ref() != Some(&artifact.uri)
            || event.evidence_hash != Some(artifact.content_hash)
        {
            return Err(Self::invalid(format!(
                "historical {stage} job and WORM event differ"
            )));
        }
        Ok(Some((job, artifact, event.occurred_at)))
    }

    fn bounded_window(
        full: &FeedbackCohortWindow,
        window_days: u32,
        label: &str,
    ) -> QuantResult<FeedbackCohortWindow> {
        let start = full
            .cutoff()
            .checked_sub_signed(Duration::days(i64::from(window_days)))
            .ok_or_else(|| Self::invalid(format!("{label} recipe window underflowed")))?;
        if start < full.window_start() {
            return Err(Self::invalid(format!(
                "{label} recipe window exceeds its frozen profile envelope"
            )));
        }
        FeedbackCohortWindow::try_new(full.profile_ref().clone(), start, full.cutoff())
            .map_err(|error| Self::invalid(error.to_string()))
    }

    async fn verify_attribution(
        &self,
        cycle: &FeedbackCycleInfo,
        params: &CandidateRecipePlanJobParams,
    ) -> QuantResult<FeedbackAttributionManifest> {
        let bytes = self.artifacts.get(&params.attribution.artifact.uri).await?;
        Self::require_hash(
            params.attribution.artifact.content_hash,
            FeedbackGovernanceCodec::bytes_hash(&bytes),
        )?;
        let artifact = FeedbackGovernanceCodec::decode_attribution(&bytes)?;
        let cycle_identity_exact = artifact.cycle_idempotency_hash == cycle.idempotency_hash;
        if artifact.feedback_cycle_id != cycle.feedback_cycle_id
            || !cycle_identity_exact
            || artifact.use_set_hash != params.attribution.use_set_hash
            || artifact.produced_set_hash != params.attribution.produced_set_hash
        {
            return Err(Self::invalid(
                "recipe-plan attribution manifest differs from its cycle",
            ));
        }
        for use_ in &artifact.uses {
            let source = self
                .cycles
                .find_cycle(&use_.source_feedback_cycle_id)
                .await?
                .ok_or_else(|| {
                    StorageError::not_found("quant_feedback_cycle", use_.source_feedback_cycle_id)
                })?;
            let available_before_cutoff = use_.available_at <= cycle.label_cutoff;
            if source.feedback_cycle_id == cycle.feedback_cycle_id
                || source.profile_ref != cycle.profile_ref
                || source.route != cycle.route
                || source.champion_model_family != cycle.champion_model_family
                || !available_before_cutoff
            {
                return Err(Self::invalid(
                    "attribution evidence violates cycle/profile/route/family PIT isolation",
                ));
            }
        }
        Ok(artifact)
    }

    async fn verify_drift(
        &self,
        cycle: &FeedbackCycleInfo,
        params: &CandidateRecipePlanJobParams,
    ) -> QuantResult<()> {
        let bytes = self.artifacts.get(&params.drift.artifact.uri).await?;
        Self::require_hash(
            params.drift.artifact.content_hash,
            FeedbackDriftCodec::bytes_hash(&bytes),
        )?;
        let artifact = FeedbackDriftCodec::decode(&bytes)?;
        let exceeded_metrics = match artifact.gate_outcome {
            DriftGateOutcome::Advance { exceeded_metrics } => exceeded_metrics,
            DriftGateOutcome::NoAction { .. }
                if cycle.evaluation_mode == FeedbackEvaluationMode::ForcedRetraining =>
            {
                Vec::new()
            }
            DriftGateOutcome::NoAction { .. } => {
                return Err(Self::invalid(
                    "conditional recipe-plan cannot bypass a Drift NoAction",
                ));
            }
        };
        let cycle_identity_exact = artifact.cycle_idempotency_hash == cycle.idempotency_hash;
        if artifact.feedback_cycle_id != cycle.feedback_cycle_id
            || !cycle_identity_exact
            || exceeded_metrics != params.drift.exceeded_metrics
        {
            return Err(Self::invalid(
                "recipe-plan Drift manifest differs from the exact predecessor",
            ));
        }
        Ok(())
    }

    fn dataset_request(
        purpose: DatasetPurpose,
        window: FeedbackCohortWindow,
        cycle: &FeedbackCycleInfo,
        source_lineage: &DatasetSourceLineage,
        template_hash: Option<ContentHash>,
    ) -> QuantResult<FeedbackDatasetBuildRequest> {
        let plan_hash = CanonicalDigest::content_hash_typed(
            DATASET_PLAN_DOMAIN,
            DATASET_PLAN_VERSION,
            &(
                DATASET_PLAN_VERSION,
                purpose,
                window.profile_ref(),
                window.window_start(),
                window.cutoff(),
                cycle.champion_model_spec_id,
                cycle.champion_model_spec_definition_hash,
                template_hash,
                source_lineage,
                DATASET_ARTIFACT_FORMAT_VERSION,
            ),
        )?;
        let request = FeedbackDatasetBuildRequest {
            training_dataset_id: TrainingDatasetId::from_feedback_plan_hash(&plan_hash),
            model_spec_id: cycle.champion_model_spec_id,
            model_spec_definition_hash: cycle.champion_model_spec_definition_hash,
            source_lineage: source_lineage.clone(),
            window,
            purpose,
        };
        request.validate()?;
        Ok(request)
    }

    fn no_action(
        params: &CandidateRecipePlanJobParams,
        cycle: &FeedbackCycleInfo,
        blocker: CandidateRecipeReadinessBlocker,
    ) -> QuantResult<CandidateRecipePlanArtifact> {
        let artifact = CandidateRecipePlanArtifact {
            format_version: CandidateRecipePlanArtifact::FORMAT_VERSION,
            artifact_id: params.artifact_id,
            feedback_cycle_id: cycle.feedback_cycle_id,
            cycle_idempotency_hash: cycle.idempotency_hash,
            input_hash: params.input_hash()?,
            label_cutoff: cycle.label_cutoff,
            planned_at: params.planned_at,
            evaluation_mode: cycle.evaluation_mode,
            profile_ref: cycle.profile_ref.clone(),
            route: cycle.route,
            model_family: cycle.champion_model_family,
            attribution: params.attribution.clone(),
            drift: params.drift.clone(),
            outcome: CandidateRecipePlanOutcome::NoAction { blocker },
        };
        artifact.validate()?;
        Ok(artifact)
    }

    async fn persist(
        &self,
        artifact: &CandidateRecipePlanArtifact,
    ) -> QuantResult<ResearchJobArtifactRef> {
        let bytes = CandidateRecipePlanCodec::encode(artifact)?;
        let content_hash = CanonicalDigest::content_hash_bytes(&bytes);
        let key = ArtifactKey::new(
            ArtifactNamespace::FeedbackRecipePlan,
            content_hash.hex(),
            "json",
        )?;
        let uri = self.artifacts.put(key, &bytes).await?;
        let persisted = self.artifacts.get(&uri).await?;
        Self::require_hash(
            content_hash,
            CanonicalDigest::content_hash_bytes(&persisted),
        )?;
        CandidateRecipePlanCodec::decode(&persisted)?;
        Ok(ResearchJobArtifactRef { uri, content_hash })
    }

    fn require_cycle(
        cycle: &FeedbackCycleInfo,
        params: &CandidateRecipePlanJobParams,
    ) -> QuantResult<()> {
        cycle.validate()?;
        let cycle_identity_exact = cycle.idempotency_hash == params.cycle_idempotency_hash;
        if cycle.feedback_cycle_id != params.feedback_cycle_id
            || !cycle_identity_exact
            || cycle.label_cutoff != params.label_cutoff
            || cycle.profile_ref.artifact_id() != cycle.research_profile_artifact_id
        {
            return Err(Self::invalid(
                "recipe-plan cycle differs from its durable job identity",
            ));
        }
        Ok(())
    }

    fn require_active(cancel: &CancellationToken) -> QuantResult<()> {
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: "feedback recipe planning cancelled".to_owned(),
            }
            .into());
        }
        Ok(())
    }

    fn require_hash(expected: ContentHash, actual: ContentHash) -> QuantResult<()> {
        if expected != actual {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: expected.to_string(),
                actual: actual.to_string(),
            }
            .into());
        }
        Ok(())
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidJobContract {
            detail: detail.into(),
        }
        .into()
    }
}

#[async_trait]
impl CandidateRecipePlanExecutionPort for CandidateRecipePlanExecutionService {
    async fn plan_recipe(
        &self,
        params: CandidateRecipePlanJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<CandidateRecipePlanExecutionResult> {
        params.validate()?;
        let artifact = Box::pin(self.build_artifact(&params, progress.as_ref(), &cancel)).await?;
        let result = self.persist(&artifact).await?;
        Ok(CandidateRecipePlanExecutionResult {
            artifact_id: artifact.artifact_id,
            artifact: result,
        })
    }
}

fn selection_order(left: &CandidateRecipeSelection, right: &CandidateRecipeSelection) -> Ordering {
    right
        .historical_lower_bound()
        .cmp(&left.historical_lower_bound())
        .then_with(|| {
            left.template
                .catalog_priority
                .cmp(&right.template.catalog_priority)
        })
        .then_with(|| {
            left.template
                .recipe_template_id
                .cmp(&right.template.recipe_template_id)
        })
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::ports::{
            FeedbackAttributionUse, FeedbackRecipeCalibrationSpec, FeedbackRecipeCpcvSpec,
            FeedbackRecipeDiagnosticSpec, FeedbackRecipeDownsideSpec, FeedbackRecipeOosEvidence,
            FeedbackRecipeOosSummary, FeedbackRecipeResourceBudget, FeedbackRecipeTemplate,
            FeedbackRecipeTemplateInput, FeedbackRecipeTrainingSpec,
        },
        enums::{
            model::ModelFamily,
            quant::{
                AttributionArtifactKind, AttributionCohort, CalibrationMethod, DownsideSource,
                FeedbackDriftMetric, FeedbackRecipeTemplateStatus,
            },
        },
        runtime_config::{BuyModelRoute, ResearchValidationConfig},
        types::{
            ArtifactUri, Bps, ContentHash, FeedbackCycleId, FeedbackRecipeTemplateId,
            ModelInputContract, ModelSpecId, ModelTrainingContract, RoleCode, UserId,
            builtin_research_profiles,
        },
    };
    use rust_decimal::Decimal;
    use uuid::Uuid;

    use super::{
        AvailableDiagnosticEvidence, CandidateRecipePlanExecutionService, OFFLINE_MEMORY_BYTES,
        SelectedRecipeTemplate,
    };

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64)))
            .expect("valid test content hash")
    }

    fn observed_at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 31, 0, 0, 0)
            .single()
            .expect("valid test timestamp")
    }

    fn template(id: u128, priority: i32) -> FeedbackRecipeTemplate {
        let profile = builtin_research_profiles()
            .expect("built-in research profiles")
            .into_iter()
            .find(|profile| profile.spec.category.is_none())
            .expect("pooled research profile");
        FeedbackRecipeTemplate::try_seal(FeedbackRecipeTemplateInput {
            recipe_template_id: FeedbackRecipeTemplateId::new(Uuid::from_u128(id)),
            revision: 1,
            profile_ref: profile.profile_ref.clone(),
            route: BuyModelRoute::Pooled,
            model_family: ModelFamily::WeightedFactor,
            training_spec: FeedbackRecipeTrainingSpec::try_new(
                ModelSpecId::new(Uuid::from_u128(0x100)),
                hash('1'),
                ModelInputContract::single_required("fixture_feature"),
                ModelTrainingContract::outcome_default(),
                1,
            )
            .expect("valid training recipe"),
            calibration_spec: FeedbackRecipeCalibrationSpec::try_new(CalibrationMethod::Platt, 1)
                .expect("valid calibration recipe"),
            cpcv_spec: FeedbackRecipeCpcvSpec::try_new(
                ResearchValidationConfig::default(),
                profile.spec.target_horizon_secs,
                profile.spec.purge_embargo_secs,
            )
            .expect("valid CPCV recipe"),
            downside_spec: FeedbackRecipeDownsideSpec::try_new(DownsideSource::MfeMae)
                .expect("valid downside recipe"),
            diagnostic_spec: FeedbackRecipeDiagnosticSpec {
                accepted_artifact_kinds: vec![AttributionArtifactKind::PredictionExplanation],
                responsive_feature_names: vec!["fixture_feature".to_owned()],
                minimum_evidence_count: 1,
                minimum_feature_matches: 1,
            },
            responsive_triggers: vec![FeedbackDriftMetric::PopulationStabilityIndex],
            catalog_priority: priority,
            resource_budget: FeedbackRecipeResourceBudget {
                max_concurrency: 1,
                max_working_set_bytes: u64::try_from(OFFLINE_MEMORY_BYTES)
                    .expect("offline memory budget fits u64"),
                max_resident_model_bytes: 16 * 1024 * 1024,
                deadline_secs: 60,
            },
            status: FeedbackRecipeTemplateStatus::Approved,
            approved_by_user_id: Some(UserId::new(Uuid::from_u128(0x101))),
            approved_by_role: Some(RoleCode::new("research_approver")),
            approved_at: Some(observed_at()),
            governance_reason: "deterministic recipe ordering test".to_owned(),
        })
        .expect("valid recipe template")
    }

    fn diagnostics(seed: u128) -> Vec<AvailableDiagnosticEvidence> {
        vec![AvailableDiagnosticEvidence {
            use_: FeedbackAttributionUse {
                source_feedback_cycle_id: FeedbackCycleId::new(Uuid::from_u128(seed)),
                artifact_kind: AttributionArtifactKind::PredictionExplanation,
                source_cohort: AttributionCohort::Evaluation,
                artifact_uri: ArtifactUri::parse(format!("s3://recipe-test/{seed}.json"))
                    .expect("valid test artifact URI"),
                artifact_hash: hash('2'),
                source_cutoff: observed_at() - Duration::days(2),
                available_at: observed_at() - Duration::days(1),
            },
            feature_names: vec!["fixture_feature".to_owned()],
        }]
    }

    fn oos(seed: u128, lower: i64) -> FeedbackRecipeOosSummary {
        FeedbackRecipeOosSummary::try_new(vec![FeedbackRecipeOosEvidence {
            source_feedback_cycle_id: FeedbackCycleId::new(Uuid::from_u128(seed)),
            recipe_plan_artifact_hash: hash('3'),
            comparison_artifact_hash: hash('4'),
            candidate_recipe_hash: hash('5'),
            simultaneous_lower_bound_bps: Bps::new(Decimal::from(lower)),
            available_at: observed_at() - Duration::hours(1),
        }])
        .expect("valid historical OOS summary")
    }

    fn selected(
        id: u128,
        priority: i32,
        matched: bool,
        lower: Option<i64>,
    ) -> SelectedRecipeTemplate {
        let template = template(id, priority);
        let available = diagnostics(id + 0x1000);
        let diagnostic_evidence =
            CandidateRecipePlanExecutionService::match_diagnostics(&template, &available)
                .expect("match diagnostics")
                .expect("diagnostic contract matches");
        SelectedRecipeTemplate {
            template,
            matched_triggers: if matched {
                vec![FeedbackDriftMetric::PopulationStabilityIndex]
            } else {
                Vec::new()
            },
            diagnostic_evidence,
            historical_oos: lower.map(|value| oos(id + 0x2000, value)),
        }
    }

    #[test]
    fn trigger_match_precedes_oos() {
        let mut selected = [
            selected(2, 0, false, Some(500)),
            selected(1, 10, true, None),
        ];
        selected.sort_by(SelectedRecipeTemplate::stable_order);
        assert_eq!(
            selected[0].template.recipe_template_id,
            FeedbackRecipeTemplateId::new(Uuid::from_u128(1))
        );
    }

    #[test]
    fn seven_path_recipe_rejected() {
        let mut validation = ResearchValidationConfig::default();
        validation.cpcv.k_test = 2;
        let error = FeedbackRecipeCpcvSpec::try_new(validation, 3_600, 300)
            .expect_err("seven-path CPCV recipe must fail closed");
        assert!(error.to_string().contains("path floor"));
    }

    #[test]
    fn oos_priority_id_order() {
        let mut selected = [
            selected(4, -1, true, Some(5)),
            selected(3, 9, true, Some(10)),
            selected(2, -1, true, Some(5)),
            selected(1, 0, true, Some(5)),
        ];
        selected.sort_by(SelectedRecipeTemplate::stable_order);
        let ids = selected
            .iter()
            .map(|item| item.template.recipe_template_id)
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                FeedbackRecipeTemplateId::new(Uuid::from_u128(3)),
                FeedbackRecipeTemplateId::new(Uuid::from_u128(2)),
                FeedbackRecipeTemplateId::new(Uuid::from_u128(4)),
                FeedbackRecipeTemplateId::new(Uuid::from_u128(1)),
            ]
        );
    }
}
