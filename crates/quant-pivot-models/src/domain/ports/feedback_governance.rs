//! Typed execution contracts for feedback truth, attribution, and validation stages.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, feedback::FeedbackError};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    domain::{
        ports::FeedbackLearningStageArtifactRef,
        quant::{
            ExecutionAttemptBarrier, ExecutionRollupBarrier, FeedbackCohortSnapshot,
            FeedbackCohortWindow, JobProgressSink, ResearchJobArtifactRef,
            ResolutionProjectionBarrier,
        },
    },
    enums::quant::{AttributionArtifactKind, AttributionCohort, FeedbackStage},
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, AttributionArtifactId, ContentHash, FeedbackAttributionPlanArtifactId,
        FeedbackCycleId, FeedbackTruthFreezeArtifactId, FeedbackValidationArtifactId,
        ModelVersionId, OrderIntentId, RecommendationId, ResearchJobId,
        model_quality::{
            GateIntent, GateSubject, QUALITY_GATE_REPORT_FORMAT_VERSION, QualityGateReport,
        },
    },
};

const GOVERNANCE_INPUT_DOMAIN: &str = "quant-pivot/feedback-governance-input";
const GOVERNANCE_INPUT_VERSION: u32 = 1;

/// Frozen `TruthFreeze` job input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackTruthFreezeJobParams {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub artifact_id: FeedbackTruthFreezeArtifactId,
    pub cutoff: DateTime<Utc>,
}

impl FeedbackTruthFreezeJobParams {
    pub fn try_new(
        feedback_cycle_id: FeedbackCycleId,
        cycle_idempotency_hash: ContentHash,
        cutoff: DateTime<Utc>,
    ) -> Result<Self, FeedbackError> {
        let params = Self {
            feedback_cycle_id,
            cycle_idempotency_hash,
            artifact_id: FeedbackTruthFreezeArtifactId::from_cycle_id(feedback_cycle_id),
            cutoff,
        };
        params.validate()?;
        Ok(params)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        if FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
            != self.feedback_cycle_id
            || self.artifact_id
                != FeedbackTruthFreezeArtifactId::from_cycle_id(self.feedback_cycle_id)
        {
            return Err(invalid("truth-freeze cycle or artifact identity differs"));
        }
        Ok(())
    }

    pub fn input_hash(&self) -> Result<ContentHash, FeedbackError> {
        governance_hash("truth_freeze", self)
    }
}

/// Typed reason canonical truth cannot cover a cycle cutoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FeedbackTruthBlocker {
    ResolutionProjection {
        unresolved_count: u64,
        quarantined_count: u64,
        oldest_unresolved_at: Option<DateTime<Utc>>,
    },
    ExecutionAttempt {
        eligible_unsealed_count: u64,
    },
    RecommendationRollup {
        eligible_unsealed_count: u64,
    },
}

/// Immutable canonical-truth barrier result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackTruthFreezeArtifact {
    pub format_version: u32,
    pub artifact_id: FeedbackTruthFreezeArtifactId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub input_hash: ContentHash,
    pub cutoff: DateTime<Utc>,
    pub resolution: ResolutionProjectionBarrier,
    pub execution_attempts: ExecutionAttemptBarrier,
    pub recommendation_rollups: ExecutionRollupBarrier,
    pub blockers: Vec<FeedbackTruthBlocker>,
}

impl FeedbackTruthFreezeArtifact {
    pub fn try_new(
        params: &FeedbackTruthFreezeJobParams,
        resolution: ResolutionProjectionBarrier,
        execution_attempts: ExecutionAttemptBarrier,
        recommendation_rollups: ExecutionRollupBarrier,
    ) -> Result<Self, FeedbackError> {
        let blockers = truth_blockers(resolution, execution_attempts, recommendation_rollups);
        let artifact = Self {
            format_version: GOVERNANCE_INPUT_VERSION,
            artifact_id: params.artifact_id,
            feedback_cycle_id: params.feedback_cycle_id,
            cycle_idempotency_hash: params.cycle_idempotency_hash,
            input_hash: params.input_hash()?,
            cutoff: params.cutoff,
            resolution,
            execution_attempts,
            recommendation_rollups,
            blockers,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        let identity_valid = self.format_version == GOVERNANCE_INPUT_VERSION
            && FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
                == self.feedback_cycle_id
            && self.artifact_id
                == FeedbackTruthFreezeArtifactId::from_cycle_id(self.feedback_cycle_id)
            && self.resolution.cutoff == self.cutoff
            && self.execution_attempts.cutoff == self.cutoff
            && self.recommendation_rollups.cutoff == self.cutoff;
        let complete = self.resolution.is_complete()
            && self.execution_attempts.is_complete()
            && self.recommendation_rollups.is_complete();
        let expected_blockers = truth_blockers(
            self.resolution,
            self.execution_attempts,
            self.recommendation_rollups,
        );
        if !identity_valid
            || complete != self.blockers.is_empty()
            || self.blockers != expected_blockers
        {
            return Err(invalid(
                "truth-freeze identity, frontier, or blocker set is inconsistent",
            ));
        }
        Ok(())
    }
}

/// Exact immutable artifact admitted to a cycle recipe planner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackAttributionUse {
    pub source_feedback_cycle_id: FeedbackCycleId,
    pub artifact_kind: AttributionArtifactKind,
    pub source_cohort: AttributionCohort,
    pub artifact_uri: ArtifactUri,
    pub artifact_hash: ContentHash,
    pub source_cutoff: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
}

/// One attribution artifact materialized by the current cycle. This is
/// evidence output, never an input to the same cycle's recipe planner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackAttributionProduced {
    pub attribution_artifact_id: AttributionArtifactId,
    pub artifact_kind: AttributionArtifactKind,
    pub source_cohort: AttributionCohort,
    pub model_version_id: Option<ModelVersionId>,
    pub recommendation_id: Option<RecommendationId>,
    pub order_intent_id: Option<OrderIntentId>,
    pub artifact_uri: ArtifactUri,
    pub artifact_hash: ContentHash,
    pub source_cutoff: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
}

impl FeedbackAttributionProduced {
    const fn subject_is_valid(&self) -> bool {
        match self.artifact_kind {
            AttributionArtifactKind::PredictionExplanation
            | AttributionArtifactKind::DecisionCounterfactual => {
                self.model_version_id.is_some()
                    && self.recommendation_id.is_some()
                    && self.order_intent_id.is_none()
            }
            AttributionArtifactKind::OutcomeAssociation => {
                self.model_version_id.is_some()
                    && self.recommendation_id.is_none()
                    && self.order_intent_id.is_none()
            }
            AttributionArtifactKind::ExecutionTrajectory
            | AttributionArtifactKind::PolicyCounterfactualOutcome => {
                self.model_version_id.is_none()
                    && self.recommendation_id.is_some()
                    && self.order_intent_id.is_some()
            }
        }
    }
}

/// Frozen `AttributionPlan` job input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackAttributionPlanJobParams {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub artifact_id: FeedbackAttributionPlanArtifactId,
    pub cutoff: DateTime<Utc>,
    pub generated_at: DateTime<Utc>,
    pub window: FeedbackCohortWindow,
    pub truth_artifact: ResearchJobArtifactRef,
}

impl FeedbackAttributionPlanJobParams {
    pub fn try_new(
        feedback_cycle_id: FeedbackCycleId,
        cycle_idempotency_hash: ContentHash,
        cutoff: DateTime<Utc>,
        generated_at: DateTime<Utc>,
        window: FeedbackCohortWindow,
        truth_artifact: ResearchJobArtifactRef,
    ) -> Result<Self, FeedbackError> {
        let params = Self {
            feedback_cycle_id,
            cycle_idempotency_hash,
            artifact_id: FeedbackAttributionPlanArtifactId::from_cycle_id(feedback_cycle_id),
            cutoff,
            generated_at,
            window,
            truth_artifact,
        };
        params.validate()?;
        Ok(params)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        if FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
            != self.feedback_cycle_id
            || self.artifact_id
                != FeedbackAttributionPlanArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.window.cutoff() > self.cutoff
            || self.generated_at < self.cutoff
        {
            return Err(invalid(
                "attribution-plan cycle or artifact identity differs",
            ));
        }
        Ok(())
    }

    pub fn cohort_snapshot(&self) -> Result<FeedbackCohortSnapshot, FeedbackError> {
        FeedbackCohortSnapshot::try_new(self.window.clone(), self.cutoff).map_err(|error| {
            FeedbackError::InvalidJobContract {
                detail: format!("invalid attribution cohort snapshot: {error}"),
            }
        })
    }

    pub fn input_hash(&self) -> Result<ContentHash, FeedbackError> {
        governance_hash("attribution_plan", self)
    }
}

/// Immutable PIT-safe attribution input plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackAttributionPlanArtifact {
    pub format_version: u32,
    pub artifact_id: FeedbackAttributionPlanArtifactId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub input_hash: ContentHash,
    pub cutoff: DateTime<Utc>,
    pub truth_artifact: ResearchJobArtifactRef,
    pub uses: Vec<FeedbackAttributionUse>,
    pub use_set_hash: ContentHash,
    pub produced: Vec<FeedbackAttributionProduced>,
    pub produced_set_hash: ContentHash,
}

impl FeedbackAttributionPlanArtifact {
    pub fn try_new(
        params: &FeedbackAttributionPlanJobParams,
        mut uses: Vec<FeedbackAttributionUse>,
        mut produced: Vec<FeedbackAttributionProduced>,
    ) -> Result<Self, FeedbackError> {
        uses.sort_by_key(|use_| use_.artifact_hash);
        let use_set_hash = governance_hash("attribution_use_set", &uses)?;
        produced.sort_by_key(|artifact| artifact.artifact_hash);
        let produced_set_hash = governance_hash("attribution_produced_set", &produced)?;
        let artifact = Self {
            format_version: GOVERNANCE_INPUT_VERSION,
            artifact_id: params.artifact_id,
            feedback_cycle_id: params.feedback_cycle_id,
            cycle_idempotency_hash: params.cycle_idempotency_hash,
            input_hash: params.input_hash()?,
            cutoff: params.cutoff,
            truth_artifact: params.truth_artifact.clone(),
            uses,
            use_set_hash,
            produced,
            produced_set_hash,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        if self.format_version != GOVERNANCE_INPUT_VERSION
            || FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
                != self.feedback_cycle_id
            || self.artifact_id
                != FeedbackAttributionPlanArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.uses.iter().any(|use_| {
                use_.source_feedback_cycle_id == self.feedback_cycle_id
                    || use_.source_cutoff >= self.cutoff
                    || use_.available_at > self.cutoff
            })
            || self
                .uses
                .windows(2)
                .any(|pair| pair[0].artifact_hash >= pair[1].artifact_hash)
            || governance_hash("attribution_use_set", &self.uses)? != self.use_set_hash
            || self.produced.iter().any(|artifact| {
                artifact.source_cutoff != self.cutoff
                    || artifact.available_at < self.cutoff
                    || AttributionArtifactId::from_content_hash(&artifact.artifact_hash)
                        != artifact.attribution_artifact_id
                    || !artifact.subject_is_valid()
            })
            || self
                .produced
                .windows(2)
                .any(|pair| pair[0].artifact_hash >= pair[1].artifact_hash)
            || governance_hash("attribution_produced_set", &self.produced)?
                != self.produced_set_hash
        {
            return Err(invalid(
                "attribution plan contains adaptive, late, duplicated, or non-canonical evidence",
            ));
        }
        Ok(())
    }
}

/// Frozen `Validation` stage input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackValidationJobParams {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub artifact_id: FeedbackValidationArtifactId,
    pub evaluated_at: DateTime<Utc>,
    pub cpcv: FeedbackLearningStageArtifactRef,
}

impl FeedbackValidationJobParams {
    pub fn try_new(
        feedback_cycle_id: FeedbackCycleId,
        cycle_idempotency_hash: ContentHash,
        evaluated_at: DateTime<Utc>,
        cpcv: FeedbackLearningStageArtifactRef,
    ) -> Result<Self, FeedbackError> {
        let params = Self {
            feedback_cycle_id,
            cycle_idempotency_hash,
            artifact_id: FeedbackValidationArtifactId::from_cycle_id(feedback_cycle_id),
            evaluated_at,
            cpcv,
        };
        params.validate()?;
        Ok(params)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.cpcv
            .validate_for(self.feedback_cycle_id, FeedbackStage::Cpcv)?;
        if FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
            != self.feedback_cycle_id
            || self.artifact_id
                != FeedbackValidationArtifactId::from_cycle_id(self.feedback_cycle_id)
        {
            return Err(invalid("validation cycle or artifact identity differs"));
        }
        Ok(())
    }

    pub fn input_hash(&self) -> Result<ContentHash, FeedbackError> {
        governance_hash("validation", self)
    }
}

/// One attempted recipe and its sole quality-gate report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackValidationTrialOutcome {
    CpcvEvaluated,
    CalibrationInsufficient,
}

/// One attempted recipe and its sole quality-gate report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackCandidateValidation {
    pub candidate_recipe_hash: ContentHash,
    pub model_version_id: ModelVersionId,
    pub trial_outcome: FeedbackValidationTrialOutcome,
    pub quality_gate_report: QualityGateReport,
}

impl FeedbackCandidateValidation {
    #[must_use]
    pub const fn is_comparison_eligible(&self) -> bool {
        matches!(
            self.trial_outcome,
            FeedbackValidationTrialOutcome::CpcvEvaluated
        ) && self.quality_gate_report.passed
    }
}

/// Immutable complete validation universe for one feedback cycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackValidationArtifact {
    pub format_version: u32,
    pub artifact_id: FeedbackValidationArtifactId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub input_hash: ContentHash,
    pub evaluated_at: DateTime<Utc>,
    pub candidates: Vec<FeedbackCandidateValidation>,
    pub trial_universe_hash: ContentHash,
}

impl FeedbackValidationArtifact {
    pub fn try_new(
        params: &FeedbackValidationJobParams,
        mut candidates: Vec<FeedbackCandidateValidation>,
    ) -> Result<Self, FeedbackError> {
        candidates.sort_by_key(|candidate| candidate.candidate_recipe_hash);
        let trial_universe_hash = governance_hash(
            "validation_trial_universe",
            &candidates
                .iter()
                .map(|candidate| {
                    (
                        candidate.candidate_recipe_hash,
                        candidate.model_version_id,
                        candidate.trial_outcome,
                        candidate.quality_gate_report.report_hash,
                    )
                })
                .collect::<Vec<_>>(),
        )?;
        let artifact = Self {
            format_version: GOVERNANCE_INPUT_VERSION,
            artifact_id: params.artifact_id,
            feedback_cycle_id: params.feedback_cycle_id,
            cycle_idempotency_hash: params.cycle_idempotency_hash,
            input_hash: params.input_hash()?,
            evaluated_at: params.evaluated_at,
            candidates,
            trial_universe_hash,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        let canonical = self
            .candidates
            .windows(2)
            .all(|pair| pair[0].candidate_recipe_hash < pair[1].candidate_recipe_hash);
        let reports_match = self.candidates.iter().all(|candidate| {
            candidate.quality_gate_report.validate().is_ok()
                && candidate.quality_gate_report.format_version
                    == QUALITY_GATE_REPORT_FORMAT_VERSION
                && candidate.quality_gate_report.intent == GateIntent::Candidate
                && candidate.quality_gate_report.evaluated_at == self.evaluated_at
                && candidate.quality_gate_report.subject
                    == GateSubject::ModelVersion(candidate.model_version_id)
                && candidate.quality_gate_report.passed
                    == candidate.quality_gate_report.hard_failures.is_empty()
        });
        let expected_trial_hash = governance_hash(
            "validation_trial_universe",
            &self
                .candidates
                .iter()
                .map(|candidate| {
                    (
                        candidate.candidate_recipe_hash,
                        candidate.model_version_id,
                        candidate.trial_outcome,
                        candidate.quality_gate_report.report_hash,
                    )
                })
                .collect::<Vec<_>>(),
        )?;
        if self.format_version != GOVERNANCE_INPUT_VERSION
            || FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
                != self.feedback_cycle_id
            || self.artifact_id
                != FeedbackValidationArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.candidates.is_empty()
            || !canonical
            || !reports_match
            || self.trial_universe_hash != expected_trial_hash
        {
            return Err(invalid(
                "validation artifact identity, trial universe, or report lineage differs",
            ));
        }
        Ok(())
    }
}

/// Exact terminal Validation object consumed by Comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackValidationArtifactRef {
    pub feedback_cycle_id: FeedbackCycleId,
    pub job_id: ResearchJobId,
    pub artifact_id: FeedbackValidationArtifactId,
    pub input_hash: ContentHash,
    pub cpcv: FeedbackLearningStageArtifactRef,
    pub artifact: ResearchJobArtifactRef,
}

impl FeedbackValidationArtifactRef {
    pub fn validate_for(&self, feedback_cycle_id: FeedbackCycleId) -> Result<(), FeedbackError> {
        self.cpcv
            .validate_for(feedback_cycle_id, FeedbackStage::Cpcv)?;
        if self.feedback_cycle_id != feedback_cycle_id
            || self.artifact_id
                != FeedbackValidationArtifactId::from_cycle_id(self.feedback_cycle_id)
        {
            return Err(invalid(
                "validation reference differs from its cycle or artifact identity",
            ));
        }
        Ok(())
    }
}

fn truth_blockers(
    resolution: ResolutionProjectionBarrier,
    execution_attempts: ExecutionAttemptBarrier,
    recommendation_rollups: ExecutionRollupBarrier,
) -> Vec<FeedbackTruthBlocker> {
    let mut blockers = Vec::new();
    if !resolution.is_complete() {
        blockers.push(FeedbackTruthBlocker::ResolutionProjection {
            unresolved_count: resolution.unresolved_count,
            quarantined_count: resolution.quarantined_count,
            oldest_unresolved_at: resolution.oldest_unresolved_at,
        });
    }
    if !execution_attempts.is_complete() {
        blockers.push(FeedbackTruthBlocker::ExecutionAttempt {
            eligible_unsealed_count: execution_attempts.eligible_unsealed_count,
        });
    }
    if !recommendation_rollups.is_complete() {
        blockers.push(FeedbackTruthBlocker::RecommendationRollup {
            eligible_unsealed_count: recommendation_rollups.eligible_unsealed_count,
        });
    }
    blockers
}

/// Verified object-store result from one governance stage.
pub struct FeedbackGovernanceExecutionResult<T> {
    pub artifact_id: T,
    pub artifact: ResearchJobArtifactRef,
}

/// Executes truth, attribution planning, and the sole model-quality gate.
#[async_trait]
pub trait FeedbackGovernanceExecutionPort: Send + Sync {
    async fn freeze_truth(
        &self,
        params: FeedbackTruthFreezeJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackGovernanceExecutionResult<FeedbackTruthFreezeArtifactId>>;

    async fn plan_attribution(
        &self,
        params: FeedbackAttributionPlanJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackGovernanceExecutionResult<FeedbackAttributionPlanArtifactId>>;

    async fn validate_candidates(
        &self,
        params: FeedbackValidationJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackGovernanceExecutionResult<FeedbackValidationArtifactId>>;
}

fn governance_hash<T: Serialize>(
    stage: &'static str,
    value: &T,
) -> Result<ContentHash, FeedbackError> {
    CanonicalDigest::content_hash_typed(
        GOVERNANCE_INPUT_DOMAIN,
        GOVERNANCE_INPUT_VERSION,
        &(stage, value),
    )
    .map_err(Into::into)
}

fn invalid(detail: impl Into<String>) -> FeedbackError {
    FeedbackError::InvalidJobContract {
        detail: detail.into(),
    }
}
