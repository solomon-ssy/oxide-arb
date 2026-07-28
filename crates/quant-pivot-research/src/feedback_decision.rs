//! Evidence-only terminal feedback decision over exact F06/F09/F10 artifacts.

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, feedback::FeedbackError, research::ResearchError};
use quant_pivot_models::{
    domain::ports::{
        FeedbackDecisionJobParams, FeedbackShadowSubject, FeedbackShadowUnavailableReason,
    },
    enums::quant::FeedbackDecision,
    hashing::CanonicalDigest,
    types::{
        ContentHash, FeedbackComparisonArtifactId, FeedbackCycleId, FeedbackDecisionArtifactId,
        FeedbackDriftArtifactId, FeedbackShadowReplayArtifactId, ModelVersionId,
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    feedback::{DriftGateOutcome, DriftObservation, FeedbackDriftArtifact, drift_gate},
    feedback_comparison::{
        FeedbackComparisonArtifact, FeedbackComparisonReplayRef, RomanoWolfCandidateResult,
        RomanoWolfGateVerdict, RomanoWolfOutcome,
    },
    feedback_shadow::{
        FeedbackShadowEvidence, FeedbackShadowOutcome, FeedbackShadowReplayArtifact,
        FeedbackShadowUnstableReason,
    },
};

const ARTIFACT_FORMAT_VERSION: u32 = 1;
const ARTIFACT_HASH_DOMAIN: &str = "quant-pivot/feedback-decision-artifact";
const ARTIFACT_SCHEMA_DOMAIN: &str = "quant-pivot/feedback-decision-schema";

/// Exact F09 numeric result and replay identity for one challenger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackDecisionCandidateEvidence {
    pub comparison: RomanoWolfCandidateResult,
    pub replay: FeedbackComparisonReplayRef,
}

impl FeedbackDecisionCandidateEvidence {
    fn try_new(
        comparison: &RomanoWolfCandidateResult,
        replay: &FeedbackComparisonReplayRef,
    ) -> Result<Self, FeedbackError> {
        let evidence = Self {
            comparison: comparison.clone(),
            replay: replay.clone(),
        };
        evidence.validate()?;
        Ok(evidence)
    }

    fn validate(&self) -> Result<(), FeedbackError> {
        if self.comparison.candidate_recipe_hash != self.replay.candidate_recipe_hash
            || self.comparison.observation_hash != self.replay.observation_hash
        {
            return Err(invalid(
                "decision candidate comparison and replay identities differ",
            ));
        }
        Ok(())
    }
}

/// Exact observed-time shape for an insufficient production-shadow window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "window", rename_all = "snake_case", deny_unknown_fields)]
pub enum FeedbackDecisionShadowWindow {
    NoObservations,
    Observed {
        first_decision_at: DateTime<Utc>,
        last_decision_at: DateTime<Utc>,
    },
}

impl FeedbackDecisionShadowWindow {
    fn try_from_timestamps(
        first_decision_at: Option<DateTime<Utc>>,
        last_decision_at: Option<DateTime<Utc>>,
    ) -> Result<Self, FeedbackError> {
        match (first_decision_at, last_decision_at) {
            (None, None) => Ok(Self::NoObservations),
            (Some(first_decision_at), Some(last_decision_at))
                if first_decision_at <= last_decision_at =>
            {
                Ok(Self::Observed {
                    first_decision_at,
                    last_decision_at,
                })
            }
            _ => Err(invalid(
                "insufficient shadow timestamps do not form one exact window",
            )),
        }
    }
}

/// Typed reason and exact evidence for a terminal no-action decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case", deny_unknown_fields)]
pub enum FeedbackDecisionNoActionEvidence {
    ComparisonInsufficient {
        observed: u64,
        required: u64,
        champion_observation_hash: ContentHash,
        candidate_observation_hashes: Vec<ContentHash>,
    },
    ShadowInsufficient {
        candidate: Box<FeedbackDecisionCandidateEvidence>,
        observed: u64,
        required: u64,
        window: FeedbackDecisionShadowWindow,
        observed_window_secs: u64,
        required_window_secs: u64,
    },
}

/// Exact evidence for a rejected challenger family or production-shadow run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case", deny_unknown_fields)]
pub enum FeedbackDecisionRejectionEvidence {
    Comparison {
        candidates: Vec<FeedbackDecisionCandidateEvidence>,
    },
    ShadowUnstable {
        candidate: Box<FeedbackDecisionCandidateEvidence>,
        evidence: FeedbackShadowEvidence,
        reasons: Vec<FeedbackShadowUnstableReason>,
    },
}

/// Why an eligible stable candidate stops short of route mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackCandidateReadyReason {
    PromotionGovernanceRequired,
}

/// Complete `CandidateReady` evidence; it carries no permit or promotion receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackCandidateReadyEvidence {
    pub candidate: Box<FeedbackDecisionCandidateEvidence>,
    pub shadow: FeedbackShadowEvidence,
    pub reason: FeedbackCandidateReadyReason,
}

/// Evidence-only F11 result. `Promoted` is deliberately unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum FeedbackDecisionOutcome {
    NoAction {
        evidence: FeedbackDecisionNoActionEvidence,
    },
    ChallengerRejected {
        evidence: FeedbackDecisionRejectionEvidence,
    },
    CandidateReady {
        evidence: FeedbackCandidateReadyEvidence,
    },
}

impl FeedbackDecisionOutcome {
    #[must_use]
    pub const fn decision(&self) -> FeedbackDecision {
        match self {
            Self::NoAction { .. } => FeedbackDecision::NoAction,
            Self::ChallengerRejected { .. } => FeedbackDecision::ChallengerRejected,
            Self::CandidateReady { .. } => FeedbackDecision::CandidateReady,
        }
    }

    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::NoAction {
                evidence: FeedbackDecisionNoActionEvidence::ComparisonInsufficient { .. },
            } => "feedback_comparison_insufficient_observations",
            Self::NoAction {
                evidence: FeedbackDecisionNoActionEvidence::ShadowInsufficient { .. },
            } => "feedback_shadow_insufficient_observations",
            Self::ChallengerRejected {
                evidence: FeedbackDecisionRejectionEvidence::Comparison { .. },
            } => "feedback_all_candidates_rejected",
            Self::ChallengerRejected {
                evidence: FeedbackDecisionRejectionEvidence::ShadowUnstable { .. },
            } => "feedback_shadow_unstable",
            Self::CandidateReady { .. } => "feedback_candidate_ready_governance_required",
        }
    }

    fn validate(&self) -> Result<(), FeedbackError> {
        match self {
            Self::NoAction {
                evidence:
                    FeedbackDecisionNoActionEvidence::ComparisonInsufficient {
                        observed,
                        required,
                        candidate_observation_hashes,
                        ..
                    },
            } if observed < required && !candidate_observation_hashes.is_empty() => Ok(()),
            Self::NoAction {
                evidence:
                    FeedbackDecisionNoActionEvidence::ShadowInsufficient {
                        candidate,
                        observed,
                        required,
                        window,
                        observed_window_secs,
                        required_window_secs,
                    },
            } => {
                candidate.validate()?;
                let count_insufficient = observed < required;
                let time_insufficient = observed_window_secs < required_window_secs;
                let window_matches = match window {
                    FeedbackDecisionShadowWindow::NoObservations => *observed == 0,
                    FeedbackDecisionShadowWindow::Observed {
                        first_decision_at,
                        last_decision_at,
                    } => *observed > 0 && first_decision_at <= last_decision_at,
                };
                if (!count_insufficient && !time_insufficient) || !window_matches {
                    return Err(invalid("decision shadow insufficiency evidence is invalid"));
                }
                Ok(())
            }
            Self::ChallengerRejected {
                evidence: FeedbackDecisionRejectionEvidence::Comparison { candidates },
            } if !candidates.is_empty() => {
                let mut previous = None;
                for candidate in candidates {
                    candidate.validate()?;
                    if !matches!(
                        candidate.comparison.gate_verdict,
                        RomanoWolfGateVerdict::Rejected { .. }
                    ) || previous
                        .is_some_and(|hash| hash >= candidate.comparison.candidate_recipe_hash)
                    {
                        return Err(invalid(
                            "decision comparison rejection evidence is not canonical",
                        ));
                    }
                    previous = Some(candidate.comparison.candidate_recipe_hash);
                }
                Ok(())
            }
            Self::ChallengerRejected {
                evidence:
                    FeedbackDecisionRejectionEvidence::ShadowUnstable {
                        candidate, reasons, ..
                    },
            } if !reasons.is_empty() => candidate.validate(),
            Self::CandidateReady { evidence } => {
                evidence.candidate.validate()?;
                if evidence.reason != FeedbackCandidateReadyReason::PromotionGovernanceRequired {
                    return Err(invalid("candidate-ready governance reason is invalid"));
                }
                Ok(())
            }
            _ => Err(invalid(
                "decision outcome has contradictory or incomplete evidence",
            )),
        }
    }
}

/// Pure owner of the F06/F09/F10-to-terminal decision mapping.
pub struct FeedbackDecisionEvaluator;

enum FeedbackComparisonProjection {
    Insufficient {
        observed: u64,
        required: u64,
        champion_observation_hash: ContentHash,
        candidate_observation_hashes: Vec<ContentHash>,
    },
    Rejected {
        candidates: Vec<FeedbackDecisionCandidateEvidence>,
    },
    Candidate {
        candidate: Box<FeedbackDecisionCandidateEvidence>,
    },
}

impl FeedbackDecisionEvaluator {
    pub fn evaluate(
        params: &FeedbackDecisionJobParams,
        drift: &FeedbackDriftArtifact,
        comparison: &FeedbackComparisonArtifact,
        shadow: &FeedbackShadowReplayArtifact,
    ) -> Result<FeedbackDecisionOutcome, FeedbackError> {
        Self::validate_lineage(params, drift, comparison, shadow)?;
        let projection = Self::project_comparison(comparison)?;
        Self::map_projection(projection, shadow.subject(), shadow.outcome())
    }

    fn project_comparison(
        comparison: &FeedbackComparisonArtifact,
    ) -> Result<FeedbackComparisonProjection, FeedbackError> {
        match comparison.outcome() {
            RomanoWolfOutcome::InsufficientObservations {
                observed,
                required,
                champion_observation_hash,
                candidate_observation_hashes,
            } => Ok(FeedbackComparisonProjection::Insufficient {
                observed: *observed,
                required: *required,
                champion_observation_hash: *champion_observation_hash,
                candidate_observation_hashes: candidate_observation_hashes.clone(),
            }),
            RomanoWolfOutcome::Compared { evidence } => {
                if let Some((result, replay)) = comparison.selected_candidate() {
                    return Ok(FeedbackComparisonProjection::Candidate {
                        candidate: Box::new(FeedbackDecisionCandidateEvidence::try_new(
                            result, replay,
                        )?),
                    });
                }
                let candidates = evidence
                    .candidates
                    .iter()
                    .zip(comparison.candidate_replays())
                    .map(|(result, replay)| {
                        FeedbackDecisionCandidateEvidence::try_new(result, replay)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(FeedbackComparisonProjection::Rejected { candidates })
            }
        }
    }

    fn map_projection(
        comparison: FeedbackComparisonProjection,
        shadow_subject: &FeedbackShadowSubject,
        shadow_outcome: &FeedbackShadowOutcome,
    ) -> Result<FeedbackDecisionOutcome, FeedbackError> {
        match comparison {
            FeedbackComparisonProjection::Insufficient {
                observed,
                required,
                champion_observation_hash,
                candidate_observation_hashes,
            } => {
                if shadow_outcome
                    != &(FeedbackShadowOutcome::NoEligibleCandidate {
                        reason: FeedbackShadowUnavailableReason::ComparisonInsufficientObservations,
                    })
                {
                    return Err(invalid(
                        "comparison insufficiency differs from its shadow predecessor outcome",
                    ));
                }
                Ok(FeedbackDecisionOutcome::NoAction {
                    evidence: FeedbackDecisionNoActionEvidence::ComparisonInsufficient {
                        observed,
                        required,
                        champion_observation_hash,
                        candidate_observation_hashes,
                    },
                })
            }
            FeedbackComparisonProjection::Rejected { candidates } => {
                if shadow_outcome
                    != &(FeedbackShadowOutcome::NoEligibleCandidate {
                        reason: FeedbackShadowUnavailableReason::AllCandidatesRejected,
                    })
                {
                    return Err(invalid(
                        "comparison rejection differs from its shadow predecessor outcome",
                    ));
                }
                Ok(FeedbackDecisionOutcome::ChallengerRejected {
                    evidence: FeedbackDecisionRejectionEvidence::Comparison { candidates },
                })
            }
            FeedbackComparisonProjection::Candidate { candidate } => {
                Self::evaluate_shadow(shadow_subject, shadow_outcome, candidate)
            }
        }
    }

    fn evaluate_shadow(
        subject: &FeedbackShadowSubject,
        outcome: &FeedbackShadowOutcome,
        candidate: Box<FeedbackDecisionCandidateEvidence>,
    ) -> Result<FeedbackDecisionOutcome, FeedbackError> {
        let FeedbackShadowSubject::Candidate {
            candidate_recipe_hash,
            contract,
        } = subject
        else {
            return Err(invalid(
                "eligible comparison candidate has no exact shadow subject",
            ));
        };
        if *candidate_recipe_hash != candidate.comparison.candidate_recipe_hash
            || contract.candidate_model_version_id() != candidate.replay.model_version_id
            || contract.candidate_serving_contract_hash() != candidate.replay.serving_contract_hash
        {
            return Err(invalid(
                "shadow subject differs from the selected comparison candidate",
            ));
        }
        match outcome {
            FeedbackShadowOutcome::InsufficientObservations {
                observed,
                required,
                first_decision_at,
                last_decision_at,
                observed_window_secs,
                required_window_secs,
            } => Ok(FeedbackDecisionOutcome::NoAction {
                evidence: FeedbackDecisionNoActionEvidence::ShadowInsufficient {
                    candidate,
                    observed: *observed,
                    required: *required,
                    window: FeedbackDecisionShadowWindow::try_from_timestamps(
                        *first_decision_at,
                        *last_decision_at,
                    )?,
                    observed_window_secs: *observed_window_secs,
                    required_window_secs: *required_window_secs,
                },
            }),
            FeedbackShadowOutcome::Unstable { evidence, reasons } => {
                Ok(FeedbackDecisionOutcome::ChallengerRejected {
                    evidence: FeedbackDecisionRejectionEvidence::ShadowUnstable {
                        candidate,
                        evidence: evidence.clone(),
                        reasons: reasons.clone(),
                    },
                })
            }
            FeedbackShadowOutcome::Stable { evidence } => {
                Ok(FeedbackDecisionOutcome::CandidateReady {
                    evidence: FeedbackCandidateReadyEvidence {
                        candidate,
                        shadow: evidence.clone(),
                        reason: FeedbackCandidateReadyReason::PromotionGovernanceRequired,
                    },
                })
            }
            FeedbackShadowOutcome::NoEligibleCandidate { .. } => Err(invalid(
                "eligible comparison candidate has a no-eligible shadow outcome",
            )),
        }
    }

    fn validate_lineage(
        params: &FeedbackDecisionJobParams,
        drift: &FeedbackDriftArtifact,
        comparison: &FeedbackComparisonArtifact,
        shadow: &FeedbackShadowReplayArtifact,
    ) -> Result<(), FeedbackError> {
        params.validate()?;
        drift
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        comparison.validate()?;
        shadow.validate()?;
        if !matches!(drift.gate_outcome, DriftGateOutcome::Advance { .. })
            || drift_gate(&drift.observations) != drift.gate_outcome
            || drift.feedback_cycle_id != params.feedback_cycle_id
            || drift.artifact_id != params.drift.artifact_id
            || drift.profile_ref != params.profile_ref
            || drift.feedback_policy_hash != params.feedback_policy_hash
            || drift.champion_model_version_id != params.champion_model_version_id
            || drift.champion_serving_contract_hash != params.champion_serving_contract_hash
            || comparison.feedback_cycle_id() != params.feedback_cycle_id
            || comparison.artifact_id() != params.comparison.artifact_id
            || comparison.job_input_hash() != params.comparison.input_hash
            || comparison.candidate_family_hash() != params.candidate_family_hash
            || comparison.champion_model_version_id() != params.champion_model_version_id
            || comparison.champion_serving_contract_hash() != params.champion_serving_contract_hash
            || shadow.feedback_cycle_id() != params.feedback_cycle_id
            || shadow.artifact_id() != params.shadow_replay.artifact_id
            || shadow.job_input_hash() != params.shadow_replay.input_hash
            || shadow.previous() != &params.comparison
            || shadow.profile_ref() != &params.profile_ref
            || shadow.feedback_policy_hash() != params.feedback_policy_hash
        {
            return Err(invalid(
                "decision predecessors do not carry one exact advancing lineage",
            ));
        }
        Ok(())
    }
}

/// Content-addressed F11 input derived from exact predecessor artifacts.
#[derive(Clone, Copy)]
pub struct FeedbackDecisionArtifactInput<'a> {
    pub params: &'a FeedbackDecisionJobParams,
    pub drift: &'a FeedbackDriftArtifact,
    pub comparison: &'a FeedbackComparisonArtifact,
    pub shadow: &'a FeedbackShadowReplayArtifact,
}

/// Immutable evidence-only terminal decision artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "FeedbackDecisionArtifactDocument")]
pub struct FeedbackDecisionArtifact {
    format_version: u32,
    artifact_hash: ContentHash,
    artifact_id: FeedbackDecisionArtifactId,
    feedback_cycle_id: FeedbackCycleId,
    job_input_hash: ContentHash,
    drift_artifact_id: FeedbackDriftArtifactId,
    comparison_artifact_id: FeedbackComparisonArtifactId,
    shadow_artifact_id: FeedbackShadowReplayArtifactId,
    champion_model_version_id: ModelVersionId,
    drift_outcome: DriftGateOutcome,
    drift_observations: Vec<DriftObservation>,
    outcome: FeedbackDecisionOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedbackDecisionArtifactDocument {
    format_version: u32,
    artifact_hash: ContentHash,
    artifact_id: FeedbackDecisionArtifactId,
    feedback_cycle_id: FeedbackCycleId,
    job_input_hash: ContentHash,
    drift_artifact_id: FeedbackDriftArtifactId,
    comparison_artifact_id: FeedbackComparisonArtifactId,
    shadow_artifact_id: FeedbackShadowReplayArtifactId,
    champion_model_version_id: ModelVersionId,
    drift_outcome: DriftGateOutcome,
    drift_observations: Vec<DriftObservation>,
    outcome: FeedbackDecisionOutcome,
}

#[derive(Serialize)]
struct FeedbackDecisionArtifactPreimage<'a> {
    format_version: u32,
    artifact_id: FeedbackDecisionArtifactId,
    feedback_cycle_id: FeedbackCycleId,
    job_input_hash: ContentHash,
    drift_artifact_id: FeedbackDriftArtifactId,
    comparison_artifact_id: FeedbackComparisonArtifactId,
    shadow_artifact_id: FeedbackShadowReplayArtifactId,
    champion_model_version_id: ModelVersionId,
    drift_outcome: &'a DriftGateOutcome,
    drift_observations: &'a [DriftObservation],
    outcome: &'a FeedbackDecisionOutcome,
}

impl FeedbackDecisionArtifact {
    pub fn try_seal(input: FeedbackDecisionArtifactInput<'_>) -> Result<Self, FeedbackError> {
        let outcome = FeedbackDecisionEvaluator::evaluate(
            input.params,
            input.drift,
            input.comparison,
            input.shadow,
        )?;
        let artifact_hash = Self::derive_hash(&FeedbackDecisionArtifactPreimage {
            format_version: ARTIFACT_FORMAT_VERSION,
            artifact_id: input.params.artifact_id,
            feedback_cycle_id: input.params.feedback_cycle_id,
            job_input_hash: input.params.input_hash()?,
            drift_artifact_id: input.params.drift.artifact_id,
            comparison_artifact_id: input.params.comparison.artifact_id,
            shadow_artifact_id: input.params.shadow_replay.artifact_id,
            champion_model_version_id: input.params.champion_model_version_id,
            drift_outcome: &input.drift.gate_outcome,
            drift_observations: &input.drift.observations,
            outcome: &outcome,
        })?;
        let artifact = Self {
            format_version: ARTIFACT_FORMAT_VERSION,
            artifact_hash,
            artifact_id: input.params.artifact_id,
            feedback_cycle_id: input.params.feedback_cycle_id,
            job_input_hash: input.params.input_hash()?,
            drift_artifact_id: input.params.drift.artifact_id,
            comparison_artifact_id: input.params.comparison.artifact_id,
            shadow_artifact_id: input.params.shadow_replay.artifact_id,
            champion_model_version_id: input.params.champion_model_version_id,
            drift_outcome: input.drift.gate_outcome.clone(),
            drift_observations: input.drift.observations.clone(),
            outcome,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.outcome.validate()?;
        if self.format_version != ARTIFACT_FORMAT_VERSION
            || self.artifact_id != FeedbackDecisionArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.drift_artifact_id
                != FeedbackDriftArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.comparison_artifact_id
                != FeedbackComparisonArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.shadow_artifact_id
                != FeedbackShadowReplayArtifactId::from_cycle_id(self.feedback_cycle_id)
            || !matches!(self.drift_outcome, DriftGateOutcome::Advance { .. })
            || drift_gate(&self.drift_observations) != self.drift_outcome
            || self.artifact_hash != Self::derive_hash(&self.preimage())?
        {
            return Err(invalid(
                "decision artifact identity, drift evidence, or content hash is invalid",
            ));
        }
        Ok(())
    }

    pub fn validate_against(
        &self,
        params: &FeedbackDecisionJobParams,
        drift: &FeedbackDriftArtifact,
        comparison: &FeedbackComparisonArtifact,
        shadow: &FeedbackShadowReplayArtifact,
    ) -> Result<(), FeedbackError> {
        self.validate()?;
        let expected = FeedbackDecisionEvaluator::evaluate(params, drift, comparison, shadow)?;
        if self.artifact_id != params.artifact_id
            || self.feedback_cycle_id != params.feedback_cycle_id
            || self.job_input_hash != params.input_hash()?
            || self.drift_artifact_id != params.drift.artifact_id
            || self.comparison_artifact_id != params.comparison.artifact_id
            || self.shadow_artifact_id != params.shadow_replay.artifact_id
            || self.champion_model_version_id != params.champion_model_version_id
            || self.drift_outcome != drift.gate_outcome
            || self.drift_observations != drift.observations
            || self.outcome != expected
        {
            return Err(invalid(
                "decision artifact differs from its exact job and predecessor evidence",
            ));
        }
        Ok(())
    }

    fn preimage(&self) -> FeedbackDecisionArtifactPreimage<'_> {
        FeedbackDecisionArtifactPreimage {
            format_version: self.format_version,
            artifact_id: self.artifact_id,
            feedback_cycle_id: self.feedback_cycle_id,
            job_input_hash: self.job_input_hash,
            drift_artifact_id: self.drift_artifact_id,
            comparison_artifact_id: self.comparison_artifact_id,
            shadow_artifact_id: self.shadow_artifact_id,
            champion_model_version_id: self.champion_model_version_id,
            drift_outcome: &self.drift_outcome,
            drift_observations: &self.drift_observations,
            outcome: &self.outcome,
        }
    }

    fn derive_hash(
        preimage: &FeedbackDecisionArtifactPreimage<'_>,
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(ARTIFACT_HASH_DOMAIN, ARTIFACT_FORMAT_VERSION, preimage)
            .map_err(Into::into)
    }

    #[must_use]
    pub const fn artifact_id(&self) -> FeedbackDecisionArtifactId {
        self.artifact_id
    }

    #[must_use]
    pub const fn artifact_hash(&self) -> ContentHash {
        self.artifact_hash
    }

    #[must_use]
    pub const fn outcome(&self) -> &FeedbackDecisionOutcome {
        &self.outcome
    }
}

impl TryFrom<FeedbackDecisionArtifactDocument> for FeedbackDecisionArtifact {
    type Error = FeedbackError;

    fn try_from(document: FeedbackDecisionArtifactDocument) -> Result<Self, Self::Error> {
        let artifact = Self {
            format_version: document.format_version,
            artifact_hash: document.artifact_hash,
            artifact_id: document.artifact_id,
            feedback_cycle_id: document.feedback_cycle_id,
            job_input_hash: document.job_input_hash,
            drift_artifact_id: document.drift_artifact_id,
            comparison_artifact_id: document.comparison_artifact_id,
            shadow_artifact_id: document.shadow_artifact_id,
            champion_model_version_id: document.champion_model_version_id,
            drift_outcome: document.drift_outcome,
            drift_observations: document.drift_observations,
            outcome: document.outcome,
        };
        artifact.validate()?;
        Ok(artifact)
    }
}

/// Canonical JSON boundary for F11 decision artifacts.
pub struct FeedbackDecisionCodec;

impl FeedbackDecisionCodec {
    pub fn encode(artifact: &FeedbackDecisionArtifact) -> QuantResult<Vec<u8>> {
        artifact.validate()?;
        CanonicalDigest::canonical_json_bytes(artifact).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> QuantResult<FeedbackDecisionArtifact> {
        let artifact =
            serde_json::from_slice::<FeedbackDecisionArtifact>(bytes).map_err(|error| {
                ResearchError::Serialization {
                    detail: format!("decode feedback decision artifact: {error}"),
                }
            })?;
        artifact.validate()?;
        if Self::encode(&artifact)? != bytes {
            return Err(ResearchError::Serialization {
                detail: "feedback decision artifact is not canonical JSON".to_owned(),
            }
            .into());
        }
        Ok(artifact)
    }

    #[must_use]
    pub fn bytes_hash(bytes: &[u8]) -> ContentHash {
        CanonicalDigest::content_hash_bytes(bytes)
    }

    pub fn schema_hash() -> QuantResult<ContentHash> {
        CanonicalDigest::content_hash_typed(
            ARTIFACT_SCHEMA_DOMAIN,
            ARTIFACT_FORMAT_VERSION,
            &[
                "identity",
                "job_input",
                "drift_predecessor",
                "comparison_predecessor",
                "shadow_predecessor",
                "typed_terminal_outcome",
                "governance_boundary",
            ],
        )
        .map_err(Into::into)
    }
}

fn invalid(detail: impl Into<String>) -> FeedbackError {
    FeedbackError::InvalidJobContract {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_models::{
        domain::ports::{
            FeedbackShadowContract, FeedbackShadowContractInput, FeedbackShadowSubject,
            FeedbackShadowUnavailableReason,
        },
        enums::quant::FeedbackDecision,
        types::{
            BacktestPathSetId, BacktestReportId, Bps, ContentHash, DecisionPolicySnapshotId,
            ModelRunId, ModelVersionId, PolicyBundleGeneration, Probability,
            builtin_research_profiles,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        FeedbackComparisonProjection, FeedbackDecisionCandidateEvidence, FeedbackDecisionEvaluator,
        FeedbackDecisionOutcome, FeedbackShadowEvidence, FeedbackShadowOutcome,
        FeedbackShadowUnstableReason, RomanoWolfCandidateResult, RomanoWolfGateVerdict,
    };
    use crate::feedback_comparison::{FeedbackComparisonReplayRef, RomanoWolfGateFailure};

    const fn hash(seed: u8) -> ContentHash {
        ContentHash::from_bytes([seed; 32])
    }

    fn instant(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().expect("timestamp")
    }

    fn candidate_fixture(
        eligible: bool,
    ) -> (
        Box<FeedbackDecisionCandidateEvidence>,
        FeedbackShadowSubject,
    ) {
        let profile = builtin_research_profiles()
            .expect("built-in profiles")
            .remove(0);
        let champion_model_version_id = ModelVersionId::from_v7();
        let candidate_model_version_id = ModelVersionId::from_v7();
        let candidate_recipe_hash = hash(1);
        let observation_hash = hash(2);
        let replay = FeedbackComparisonReplayRef {
            candidate_recipe_hash,
            model_version_id: candidate_model_version_id,
            serving_contract_hash: hash(3),
            path_set_id: BacktestPathSetId::from_v7(),
            path_set_hash: hash(4),
            model_run_id: ModelRunId::from_v7(),
            backtest_report_id: BacktestReportId::from_v7(),
            backtest_report_hash: hash(5),
            observation_hash,
        };
        let gate_verdict = if eligible {
            RomanoWolfGateVerdict::Eligible
        } else {
            RomanoWolfGateVerdict::Rejected {
                failures: vec![RomanoWolfGateFailure::NonPositiveEffect],
            }
        };
        let comparison = RomanoWolfCandidateResult {
            candidate_recipe_hash,
            observation_hash,
            effect_bps: Bps::new(if eligible { dec!(100) } else { Decimal::ZERO }),
            simultaneous_lower_bound_bps: Bps::new(if eligible { dec!(50) } else { dec!(-1) }),
            raw_p_value: if eligible { dec!(0.01) } else { Decimal::ONE },
            adjusted_p_value: if eligible { dec!(0.02) } else { Decimal::ONE },
            confidence: dec!(0.95),
            familywise_alpha: dec!(0.05),
            gate_verdict,
        };
        let candidate = Box::new(
            FeedbackDecisionCandidateEvidence::try_new(&comparison, &replay)
                .expect("candidate evidence"),
        );
        let contract = FeedbackShadowContract::try_seal(FeedbackShadowContractInput {
            profile_ref: profile.profile_ref,
            feedback_policy_hash: profile
                .spec
                .feedback_policy
                .content_hash()
                .expect("policy hash"),
            category_scope: profile.spec.category,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            decision_policy_snapshot_hash: hash(6),
            policy_bundle_generation: PolicyBundleGeneration::FIRST,
            champion_model_version_id,
            champion_serving_contract_hash: hash(7),
            candidate_model_version_id,
            candidate_serving_contract_hash: replay.serving_contract_hash,
            observation_window_start: instant(0),
            observation_window_end: instant(3_600),
            minimum_observations: profile.spec.feedback_policy.shadow_minimum_observations,
            required_window_secs: 600,
            minimum_topn_overlap: Probability::new(dec!(0.60)),
        })
        .expect("shadow contract");
        (
            candidate,
            FeedbackShadowSubject::Candidate {
                candidate_recipe_hash,
                contract: Box::new(contract),
            },
        )
    }

    fn shadow_evidence(divergent: bool) -> FeedbackShadowEvidence {
        FeedbackShadowEvidence {
            observation_hash: hash(8),
            observed: 1_000,
            first_decision_at: instant(0),
            last_decision_at: instant(700),
            observed_window_secs: 700,
            mean_topn_overlap: Probability::new(if divergent { dec!(0.50) } else { dec!(0.90) }),
            any_hard_divergence: divergent,
        }
    }

    #[test]
    fn comparison_terminal_mapping() {
        let no_subject = FeedbackShadowSubject::NoEligibleCandidate {
            reason: FeedbackShadowUnavailableReason::ComparisonInsufficientObservations,
        };
        let insufficient = FeedbackDecisionEvaluator::map_projection(
            FeedbackComparisonProjection::Insufficient {
                observed: 499,
                required: 500,
                champion_observation_hash: hash(9),
                candidate_observation_hashes: vec![hash(10)],
            },
            &no_subject,
            &FeedbackShadowOutcome::NoEligibleCandidate {
                reason: FeedbackShadowUnavailableReason::ComparisonInsufficientObservations,
            },
        )
        .expect("comparison insufficiency");
        insufficient.validate().expect("valid no-action outcome");

        let (rejected, _) = candidate_fixture(false);
        let rejected_subject = FeedbackShadowSubject::NoEligibleCandidate {
            reason: FeedbackShadowUnavailableReason::AllCandidatesRejected,
        };
        let rejection = FeedbackDecisionEvaluator::map_projection(
            FeedbackComparisonProjection::Rejected {
                candidates: vec![*rejected],
            },
            &rejected_subject,
            &FeedbackShadowOutcome::NoEligibleCandidate {
                reason: FeedbackShadowUnavailableReason::AllCandidatesRejected,
            },
        )
        .expect("comparison rejection");
        rejection.validate().expect("valid rejection outcome");

        assert_eq!(insufficient.decision(), FeedbackDecision::NoAction);
        assert_eq!(rejection.decision(), FeedbackDecision::ChallengerRejected);
        assert!(
            FeedbackDecisionEvaluator::map_projection(
                FeedbackComparisonProjection::Insufficient {
                    observed: 499,
                    required: 500,
                    champion_observation_hash: hash(9),
                    candidate_observation_hashes: vec![hash(10)],
                },
                &rejected_subject,
                &FeedbackShadowOutcome::NoEligibleCandidate {
                    reason: FeedbackShadowUnavailableReason::AllCandidatesRejected,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn shadow_terminal_mapping() {
        let (candidate, subject) = candidate_fixture(true);
        let insufficient = FeedbackDecisionEvaluator::map_projection(
            FeedbackComparisonProjection::Candidate {
                candidate: candidate.clone(),
            },
            &subject,
            &FeedbackShadowOutcome::InsufficientObservations {
                observed: 1,
                required: 1_000,
                first_decision_at: Some(instant(0)),
                last_decision_at: Some(instant(0)),
                observed_window_secs: 0,
                required_window_secs: 600,
            },
        )
        .expect("shadow insufficiency");
        let unstable_evidence = shadow_evidence(true);
        let unstable = FeedbackDecisionEvaluator::map_projection(
            FeedbackComparisonProjection::Candidate {
                candidate: candidate.clone(),
            },
            &subject,
            &FeedbackShadowOutcome::Unstable {
                evidence: unstable_evidence,
                reasons: vec![
                    FeedbackShadowUnstableReason::HardDivergence,
                    FeedbackShadowUnstableReason::TopnOverlapBelowMinimum,
                ],
            },
        )
        .expect("shadow rejection");
        let ready = FeedbackDecisionEvaluator::map_projection(
            FeedbackComparisonProjection::Candidate { candidate },
            &subject,
            &FeedbackShadowOutcome::Stable {
                evidence: shadow_evidence(false),
            },
        )
        .expect("candidate ready");
        for outcome in [&insufficient, &unstable, &ready] {
            outcome.validate().expect("valid decision mapping");
            assert_ne!(outcome.decision(), FeedbackDecision::Promoted);
            assert!(
                !serde_json::to_string(outcome)
                    .expect("serialize decision")
                    .contains("promoted")
            );
        }
        assert!(matches!(
            insufficient,
            FeedbackDecisionOutcome::NoAction { .. }
        ));
        assert!(matches!(
            unstable,
            FeedbackDecisionOutcome::ChallengerRejected { .. }
        ));
        assert!(matches!(
            ready,
            FeedbackDecisionOutcome::CandidateReady { .. }
        ));
    }
}
