//! Authoritative feedback-cycle reads for the operator workbench.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{
    QuantError, QuantResult, feedback::FeedbackError, research::ResearchError,
};
use quant_pivot_models::{
    domain::{
        api::{
            DriftReportListQuery, DriftReportView, FeedbackAttributionSummaryView,
            FeedbackCandidateComparisonView, FeedbackCandidateReadyView,
            FeedbackCandidateShadowView, FeedbackCohortCountsView, FeedbackCoverageDecision,
            FeedbackCoverageView, FeedbackCycleDetailView, FeedbackCycleListQuery,
            FeedbackCycleView, FeedbackEvaluationUseView, FeedbackOverviewView,
            FeedbackProfileOverviewView, FeedbackQueueView, FeedbackReadinessView,
            FeedbackRouteDiffView, FeedbackSchedulerListView, FeedbackSchedulerStateView,
            FeedbackStageEventView, FeedbackTriggerEventView, FeedbackTruthOperationsView,
        },
        pagination::{PageRequest, Paginated},
        ports::{
            FeedbackActivationReadPort, FeedbackReadPort, ResearchReadinessPort,
            ShadowBindingLifecycle,
        },
        quant::{FeedbackCycleInfo, FeedbackStageEventInfo},
    },
    enums::quant::{
        AttributionArtifactKind, FeedbackCycleStatus, FeedbackDecision, FeedbackStage,
        FeedbackStageEventKind, ShadowBindingStatus,
    },
    types::{
        ArtifactUri, ContentHash, FeedbackCycleId, ResearchProfileArtifact,
        builtin_research_profiles,
    },
};
use quant_pivot_repository::traits::{
    ExecutionAttemptOutcomeRepository, FeedbackCycleRepository, FeedbackSchedulerRepository,
    ModelRouteShadowBindingRepository, RecommendationExecutionRollupRepository,
    ResolutionObservationRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    feedback::{CoverageGateOutcome, FeedbackCoverageArtifact, FeedbackCoverageCodec},
};

use crate::service::feedback_decision_stage::{
    FeedbackDecisionStageAdapter, PromotionDecisionEvidence,
};

/// Read-only feedback dependencies.
pub struct CoreFeedbackReadDeps {
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub scheduler: Arc<dyn FeedbackSchedulerRepository>,
    pub readiness: Arc<dyn ResearchReadinessPort>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub resolutions: Arc<dyn ResolutionObservationRepository>,
    pub attempts: Arc<dyn ExecutionAttemptOutcomeRepository>,
    pub rollups: Arc<dyn RecommendationExecutionRollupRepository>,
    pub shadow_bindings: Arc<dyn ModelRouteShadowBindingRepository>,
    pub decisions: Arc<FeedbackDecisionStageAdapter>,
    pub activations: Arc<dyn FeedbackActivationReadPort>,
}

pub struct CoreFeedbackReadPort {
    cycles: Arc<dyn FeedbackCycleRepository>,
    scheduler: Arc<dyn FeedbackSchedulerRepository>,
    readiness: Arc<dyn ResearchReadinessPort>,
    artifacts: Arc<dyn ArtifactStore>,
    resolutions: Arc<dyn ResolutionObservationRepository>,
    attempts: Arc<dyn ExecutionAttemptOutcomeRepository>,
    rollups: Arc<dyn RecommendationExecutionRollupRepository>,
    shadow_bindings: Arc<dyn ModelRouteShadowBindingRepository>,
    decisions: Arc<FeedbackDecisionStageAdapter>,
    activations: Arc<dyn FeedbackActivationReadPort>,
}

impl CoreFeedbackReadPort {
    #[must_use]
    pub fn new(deps: CoreFeedbackReadDeps) -> Self {
        Self {
            cycles: deps.cycles,
            scheduler: deps.scheduler,
            readiness: deps.readiness,
            artifacts: deps.artifacts,
            resolutions: deps.resolutions,
            attempts: deps.attempts,
            rollups: deps.rollups,
            shadow_bindings: deps.shadow_bindings,
            decisions: deps.decisions,
            activations: deps.activations,
        }
    }

    fn evidence_count(value: usize, field: &'static str) -> QuantResult<u64> {
        u64::try_from(value).map_err(|error| {
            FeedbackError::InvalidCycleState {
                detail: format!("{field} cannot be represented as u64: {error}"),
            }
            .into()
        })
    }

    fn candidate_ready_view(
        evidence: &PromotionDecisionEvidence,
        lifecycle: &ShadowBindingLifecycle,
    ) -> QuantResult<FeedbackCandidateReadyView> {
        if lifecycle.binding_id != evidence.shadow_binding.artifact_id
            || lifecycle.feedback_cycle_id != evidence.cycle.feedback_cycle_id
            || lifecycle.route != evidence.shadow_binding.route
            || lifecycle.binding_generation != evidence.shadow_binding.binding_generation
            || lifecycle.champion_model_version_id
                != evidence.shadow_binding.champion_model_version_id
            || lifecycle.candidate_model_version_id
                != evidence.shadow_binding.candidate_model_version_id
            || lifecycle.committed_policy_generation
                != evidence.shadow_binding.committed_policy_generation
            || lifecycle.bound_at != evidence.shadow_binding.bound_at
        {
            return Err(FeedbackError::InvalidCycleState {
                detail: "shadow-binding lifecycle differs from CandidateReady evidence".to_owned(),
            }
            .into());
        }
        let mut prediction_explanation_count = 0_u64;
        let mut decision_intervention_replay_count = 0_u64;
        let mut resolution_outcome_association_count = 0_u64;
        let mut execution_outcome_association_count = 0_u64;
        let mut execution_trajectory_count = 0_u64;
        let mut policy_counterfactual_count = 0_u64;
        for artifact in &evidence.dag.attribution_manifest.produced {
            let count = match artifact.artifact_kind {
                AttributionArtifactKind::PredictionExplanation => &mut prediction_explanation_count,
                AttributionArtifactKind::DecisionInterventionReplay => {
                    &mut decision_intervention_replay_count
                }
                AttributionArtifactKind::ResolutionOutcomeAssociation => {
                    &mut resolution_outcome_association_count
                }
                AttributionArtifactKind::ExecutionOutcomeAssociation => {
                    &mut execution_outcome_association_count
                }
                AttributionArtifactKind::ExecutionTrajectory => &mut execution_trajectory_count,
                AttributionArtifactKind::PolicyCounterfactualOutcome => {
                    &mut policy_counterfactual_count
                }
            };
            *count = count
                .checked_add(1)
                .ok_or_else(|| FeedbackError::InvalidCycleState {
                    detail: "attribution artifact count overflowed u64".to_owned(),
                })?;
        }
        let contract = &evidence.shadow_contract;
        let comparison = &evidence.comparison;
        let shadow = &evidence.shadow;
        let current_route_generation = evidence.shadow_binding.binding_generation;
        let proposed_route_generation =
            current_route_generation.checked_add(1).ok_or_else(|| {
                FeedbackError::InvalidCycleState {
                    detail: "candidate route generation overflowed".to_owned(),
                }
            })?;
        Ok(FeedbackCandidateReadyView {
            quality_gate: (&evidence.dag.quality_gate_report).into(),
            comparison: FeedbackCandidateComparisonView {
                observation_count: evidence.comparison_observation_count,
                effect_bps: comparison.effect_bps.inner().normalize().to_string(),
                simultaneous_lower_bound_bps: comparison
                    .simultaneous_lower_bound_bps
                    .inner()
                    .normalize()
                    .to_string(),
                adjusted_p_value: comparison.adjusted_p_value.normalize().to_string(),
                confidence: comparison.confidence.normalize().to_string(),
            },
            shadow: FeedbackCandidateShadowView {
                observed: shadow.observed,
                required: contract.minimum_observations(),
                served_window_secs: shadow.served_window_secs,
                required_window_secs: contract.required_window_secs(),
                mean_topn_decision_overlap: shadow
                    .mean_topn_decision_overlap
                    .inner()
                    .normalize()
                    .to_string(),
                minimum_topn_decision_overlap: contract
                    .minimum_topn_decision_overlap()
                    .inner()
                    .normalize()
                    .to_string(),
                any_hard_divergence: shadow.any_hard_divergence,
            },
            attribution: FeedbackAttributionSummaryView {
                prior_cycle_use_count: Self::evidence_count(
                    evidence.dag.attribution_manifest.uses.len(),
                    "attribution prior-cycle use count",
                )?,
                prediction_explanation_count,
                decision_intervention_replay_count,
                resolution_outcome_association_count,
                execution_outcome_association_count,
                execution_trajectory_count,
                policy_counterfactual_count,
                use_set_hash: evidence.dag.attribution_manifest.use_set_hash,
                produced_set_hash: evidence.dag.attribution_manifest.produced_set_hash,
            },
            route_diff: FeedbackRouteDiffView {
                route: evidence.shadow_binding.route,
                shadow_binding_id: evidence.shadow_binding.artifact_id,
                shadow_bound_at: evidence.shadow_binding.bound_at,
                shadow_binding_generation: evidence.shadow_binding.binding_generation,
                shadow_binding_status: lifecycle.status,
                shadow_lifecycle_generation: lifecycle.lifecycle_generation,
                shadow_terminated_at: lifecycle.terminated_at,
                shadow_termination_policy_activation_id: lifecycle.termination_policy_activation_id,
                shadow_termination_reason_code: lifecycle.termination_reason_code.clone(),
                current_policy_generation: evidence.shadow_binding.committed_policy_generation,
                current_route_generation,
                proposed_route_generation,
                champion_model_version_id: contract.champion_model_version_id(),
                candidate_model_version_id: contract.candidate_model_version_id(),
                execution_authority_unchanged: true,
            },
            blockers: Vec::new(),
        })
    }

    async fn load_coverage(
        &self,
        cycle: &FeedbackCycleInfo,
        timeline: &[FeedbackStageEventInfo],
    ) -> QuantResult<Option<FeedbackCoverageView>> {
        let Some(event) = timeline.iter().rev().find(|event| {
            event.stage == FeedbackStage::Coverage
                && event.event_kind == FeedbackStageEventKind::Succeeded
        }) else {
            return Ok(None);
        };
        let (Some(uri), Some(expected_hash)) = (event.evidence_uri.as_ref(), event.evidence_hash)
        else {
            return Err(FeedbackError::InvalidStageEvent {
                detail: format!(
                    "successful coverage event {} has no complete artifact evidence",
                    event.feedback_stage_event_id
                ),
            }
            .into());
        };
        let bytes = self.artifacts.get(uri).await?;
        let actual_hash = FeedbackCoverageCodec::bytes_hash(&bytes);
        if actual_hash != expected_hash {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: expected_hash.to_string(),
                actual: actual_hash.to_string(),
            }
            .into());
        }
        let artifact = FeedbackCoverageCodec::decode(&bytes)?;
        Self::validate_coverage(cycle, &artifact)?;
        Ok(Some(Self::coverage_view(
            &artifact,
            uri.clone(),
            expected_hash,
        )))
    }

    fn validate_coverage(
        cycle: &FeedbackCycleInfo,
        artifact: &FeedbackCoverageArtifact,
    ) -> QuantResult<()> {
        let cycle_matches = (artifact.feedback_cycle_id, artifact.cycle_idempotency_hash)
            == (cycle.feedback_cycle_id, cycle.idempotency_hash);
        let profile_matches = (&artifact.profile_ref, artifact.feedback_policy_hash)
            == (&cycle.profile_ref, cycle.feedback_policy_hash);
        let champion_matches = (
            artifact.champion_model_version_id,
            artifact.champion_serving_contract_hash,
        ) == (
            cycle.champion_model_version_id,
            cycle.champion_serving_contract_hash,
        );
        if !cycle_matches || !profile_matches || !champion_matches {
            return Err(FeedbackError::InvalidCycleIdentity {
                detail: format!(
                    "coverage artifact {} differs from feedback cycle {}",
                    artifact.artifact_id, cycle.feedback_cycle_id
                ),
            }
            .into());
        }
        Ok(())
    }

    fn coverage_view(
        artifact: &FeedbackCoverageArtifact,
        artifact_uri: ArtifactUri,
        artifact_hash: ContentHash,
    ) -> FeedbackCoverageView {
        let (decision, reason_code, coverage) = match artifact.gate_outcome {
            CoverageGateOutcome::Advance { coverage } => {
                (FeedbackCoverageDecision::Advance, None, coverage)
            }
            CoverageGateOutcome::NoAction { reason, coverage } => (
                FeedbackCoverageDecision::NoAction,
                Some(reason.as_str().to_owned()),
                coverage,
            ),
        };
        FeedbackCoverageView {
            artifact_id: artifact.artifact_id,
            artifact_uri,
            artifact_hash,
            evaluation_window_start: artifact.evaluation_window.window_start(),
            label_cutoff: artifact.evaluation_window.cutoff(),
            model_learning_candidate_count: artifact.gate_input.model_learning_candidate_count,
            mature_label_count: artifact.gate_input.mature_label_count,
            new_mature_label_count: artifact.gate_input.new_mature_label_count,
            minimum_mature_labels: artifact.gate_input.minimum_mature_labels,
            minimum_new_mature_labels: artifact.gate_input.minimum_new_mature_labels,
            minimum_coverage: artifact.gate_input.minimum_coverage.normalize().to_string(),
            coverage: coverage.normalize().to_string(),
            decision,
            reason_code,
            model_learning: FeedbackCohortCountsView::from(&artifact.cohorts.model_learning),
            execution_learning: FeedbackCohortCountsView::from(
                &artifact.cohorts.execution_learning,
            ),
            policy_evaluation: FeedbackCohortCountsView::from(&artifact.cohorts.policy_evaluation),
        }
    }

    async fn profile_overview(
        &self,
        profile: ResearchProfileArtifact,
    ) -> QuantResult<FeedbackProfileOverviewView> {
        let policy_hash = profile
            .spec
            .feedback_policy
            .content_hash()
            .map_err(|error| QuantError::config(error.to_string()))?;
        let page = self
            .cycles
            .page_cycles(FeedbackCycleListQuery {
                profile_id: Some(profile.profile_ref.id.clone()),
                status: None,
                trigger_family: None,
                page: PageRequest::new(1, 1),
            })
            .await?;
        let latest = page.items.into_iter().next();
        let latest_coverage = if let Some(cycle) = latest.as_ref() {
            let timeline = self
                .cycles
                .list_stage_events(&cycle.feedback_cycle_id)
                .await?;
            self.load_coverage(cycle, &timeline).await?
        } else {
            None
        };
        let policy = profile.spec.feedback_policy;
        Ok(FeedbackProfileOverviewView {
            profile_ref: profile.profile_ref,
            category: profile.spec.category,
            activation_eligibility: profile.spec.activation_eligibility,
            feedback_policy_hash: policy_hash,
            evaluation_window_days: policy.evaluation_window_days,
            feedback_cadence_secs: policy.feedback_cadence_secs,
            minimum_mature_labels: policy.minimum_mature_labels,
            minimum_new_mature_labels: policy.minimum_new_mature_labels,
            retraining_cooldown_secs: policy.retraining_cooldown_secs,
            minimum_coverage: policy.minimum_coverage.normalize().to_string(),
            latest_cycle: latest.map(FeedbackCycleView::from),
            latest_coverage,
        })
    }
}

#[async_trait]
impl FeedbackReadPort for CoreFeedbackReadPort {
    async fn overview(&self) -> QuantResult<FeedbackOverviewView> {
        let truth_observed_at = self.cycles.database_time().await?;
        let (queue, resolution, resolution_attention, attempts, rollups) = tokio::try_join!(
            self.cycles.queue_snapshot(),
            self.resolutions.barrier(truth_observed_at),
            self.resolutions.list_attention(50),
            self.attempts.barrier(truth_observed_at),
            self.rollups.barrier(truth_observed_at),
        )?;
        let revision = self.cycles.latest_outbox_revision().await?;
        let readiness = self
            .readiness
            .snapshot()
            .await?
            .map(|snapshot| FeedbackReadinessView {
                observed_at: snapshot.observed_at,
                required_history_days: snapshot.required_history_days,
                observed_history_days: snapshot.observed_history_days,
                retention_ready: snapshot.retention_ready,
                latency_ready: snapshot.latency_ready,
            });
        let profiles = builtin_research_profiles().map_err(QuantError::config)?;
        let mut profile_views = Vec::with_capacity(profiles.len());
        for profile in profiles {
            profile_views.push(self.profile_overview(profile).await?);
        }
        let generated_at = self.cycles.database_time().await?;
        Ok(FeedbackOverviewView {
            revision,
            generated_at,
            queue: FeedbackQueueView::from(queue),
            truth_operations: FeedbackTruthOperationsView {
                observed_at: truth_observed_at,
                resolution_unresolved_count: resolution.unresolved_count,
                resolution_mapping_blocked_count: resolution.mapping_blocked_count,
                resolution_quarantined_count: resolution.quarantined_count,
                resolution_excluded_count: resolution.excluded_count,
                resolution_oldest_unresolved_at: resolution.oldest_unresolved_at,
                resolution_terminal_through: resolution.terminal_through,
                resolution_attention,
                execution_attempt_unsealed_count: attempts.eligible_unsealed_count,
                execution_attempt_sealed_through: attempts.sealed_through,
                recommendation_rollup_unsealed_count: rollups.eligible_unsealed_count,
                recommendation_rollup_sealed_through: rollups.sealed_through,
            },
            readiness,
            profiles: profile_views,
        })
    }

    async fn list_schedulers(&self) -> QuantResult<FeedbackSchedulerListView> {
        let observed_at = self.cycles.database_time().await?;
        let items = self
            .scheduler
            .list_states()
            .await?
            .into_iter()
            .map(FeedbackSchedulerStateView::from)
            .collect();
        Ok(FeedbackSchedulerListView { observed_at, items })
    }

    async fn list_cycles(
        &self,
        query: FeedbackCycleListQuery,
    ) -> QuantResult<Paginated<FeedbackCycleView>> {
        self.cycles
            .page_cycles(query)
            .await
            .map_err(QuantError::from)
            .map(|page| page.map(FeedbackCycleView::from))
    }

    async fn list_drift_reports(
        &self,
        query: DriftReportListQuery,
    ) -> QuantResult<Paginated<DriftReportView>> {
        self.cycles
            .page_drift_reports(query)
            .await
            .map_err(QuantError::from)
            .map(|page| page.map(DriftReportView::from))
    }

    async fn get_cycle(
        &self,
        cycle_id: &FeedbackCycleId,
    ) -> QuantResult<Option<FeedbackCycleDetailView>> {
        let Some(cycle) = self.cycles.find_cycle(cycle_id).await? else {
            return Ok(None);
        };
        let (triggers, timeline, drift_reports, evaluation_uses) = tokio::try_join!(
            self.cycles.list_trigger_events(cycle_id),
            self.cycles.list_stage_events(cycle_id),
            self.cycles.list_drift_reports(cycle_id),
            self.cycles.list_evaluation_uses(cycle_id),
        )?;
        let coverage = self.load_coverage(&cycle, &timeline).await?;
        let candidate_context = if cycle.status == FeedbackCycleStatus::Succeeded
            && matches!(
                cycle.decision,
                Some(FeedbackDecision::CandidateReady | FeedbackDecision::Promoted)
            ) {
            let evidence = self
                .decisions
                .candidate_evidence(&cycle.feedback_cycle_id)
                .await?;
            let lifecycle = self
                .shadow_bindings
                .find_lifecycle(&evidence.shadow_binding.artifact_id)
                .await?
                .ok_or_else(|| FeedbackError::InvalidCycleState {
                    detail: format!(
                        "candidate-evidence cycle {} has no shadow-binding lifecycle",
                        cycle.feedback_cycle_id
                    ),
                })?;
            Some((evidence, lifecycle))
        } else {
            None
        };
        let candidate_ready = candidate_context
            .as_ref()
            .map(|(evidence, lifecycle)| Self::candidate_ready_view(evidence, lifecycle))
            .transpose()?;
        let activation_receipt = if cycle.status == FeedbackCycleStatus::Succeeded
            && cycle.decision == Some(FeedbackDecision::Promoted)
        {
            let (_, lifecycle) =
                candidate_context
                    .as_ref()
                    .ok_or_else(|| FeedbackError::InvalidCycleState {
                        detail: format!(
                            "Promoted cycle {} has no candidate evidence",
                            cycle.feedback_cycle_id
                        ),
                    })?;
            let receipt = self
                .activations
                .get_cycle_activation(cycle.feedback_cycle_id)
                .await?
                .ok_or_else(|| FeedbackError::InvalidCycleState {
                    detail: format!(
                        "Promoted cycle {} activation receipt is missing",
                        cycle.feedback_cycle_id
                    ),
                })?;
            if lifecycle.feedback_cycle_id != cycle.feedback_cycle_id
                || lifecycle.status != ShadowBindingStatus::Promoted
                || receipt.feedback_cycle_id != cycle.feedback_cycle_id
                || receipt.route != lifecycle.route
                || receipt.previous_model_version_id != lifecycle.champion_model_version_id
                || receipt.activated_model_version_id != lifecycle.candidate_model_version_id
                || receipt.rollback_target.route != lifecycle.route
                || receipt.rollback_target.restored_model_version_id
                    != lifecycle.champion_model_version_id
                || receipt.rollback_target.activated_model_version_id
                    != lifecycle.candidate_model_version_id
                || !receipt.rollback_target.shadow_cleared
            {
                return Err(FeedbackError::InvalidCycleState {
                    detail: format!(
                        "Promoted cycle {} activation receipt differs from shadow lifecycle",
                        cycle.feedback_cycle_id
                    ),
                }
                .into());
            }
            Some(receipt)
        } else {
            None
        };
        Ok(Some(FeedbackCycleDetailView {
            cycle: FeedbackCycleView::from(cycle),
            triggers: triggers
                .into_iter()
                .map(FeedbackTriggerEventView::from)
                .collect(),
            timeline: timeline
                .into_iter()
                .map(FeedbackStageEventView::from)
                .collect(),
            coverage,
            candidate_ready,
            activation_receipt,
            drift_reports: drift_reports.into_iter().map(Into::into).collect(),
            evaluation_uses: evaluation_uses
                .into_iter()
                .map(FeedbackEvaluationUseView::from)
                .collect(),
        }))
    }
}
