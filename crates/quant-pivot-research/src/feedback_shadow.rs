//! Production-generation shadow observation gate and immutable F10 artifact.

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, feedback::FeedbackError, research::ResearchError};
use quant_pivot_models::{
    domain::{
        ports::{
            FeedbackComparisonArtifactRef, FeedbackShadowContract, FeedbackShadowJobParams,
            FeedbackShadowSubject, FeedbackShadowUnavailableReason,
        },
        quant::ShadowObservationWindow,
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, FeedbackCycleId, FeedbackShadowReplayArtifactId, Probability,
        ResearchProfileRef,
    },
};
use serde::{Deserialize, Serialize};

const ARTIFACT_FORMAT_VERSION: u32 = 1;
const ARTIFACT_HASH_DOMAIN: &str = "quant-pivot/feedback-shadow-replay-artifact";
const ARTIFACT_SCHEMA_DOMAIN: &str = "quant-pivot/feedback-shadow-replay-schema";
const OBSERVATION_HASH_DOMAIN: &str = "quant-pivot/feedback-shadow-observation";

/// Why a sufficiently observed candidate failed the production-shadow gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackShadowUnstableReason {
    HardDivergence,
    TopnOverlapBelowMinimum,
}

/// Numeric evidence is present only after count and actual time coverage pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackShadowEvidence {
    pub observation_hash: ContentHash,
    pub observed: u64,
    pub first_decision_at: DateTime<Utc>,
    pub last_decision_at: DateTime<Utc>,
    pub observed_window_secs: u64,
    pub mean_topn_overlap: Probability,
    pub any_hard_divergence: bool,
}

/// Typed F10 result with no numeric stability placeholder on insufficient data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum FeedbackShadowOutcome {
    NoEligibleCandidate {
        reason: FeedbackShadowUnavailableReason,
    },
    InsufficientObservations {
        observed: u64,
        required: u64,
        first_decision_at: Option<DateTime<Utc>>,
        last_decision_at: Option<DateTime<Utc>>,
        observed_window_secs: u64,
        required_window_secs: u64,
    },
    Stable {
        evidence: FeedbackShadowEvidence,
    },
    Unstable {
        evidence: FeedbackShadowEvidence,
        reasons: Vec<FeedbackShadowUnstableReason>,
    },
}

/// Pure owner of the count/time/overlap/divergence gate.
pub struct FeedbackShadowEvaluator;

impl FeedbackShadowEvaluator {
    pub fn evaluate(
        contract: &FeedbackShadowContract,
        window: &ShadowObservationWindow,
    ) -> Result<FeedbackShadowOutcome, FeedbackError> {
        contract.validate()?;
        let observed_window_secs = Self::observed_secs(window)?;
        if window.sample_count < contract.minimum_observations()
            || observed_window_secs < contract.required_window_secs()
        {
            return Ok(FeedbackShadowOutcome::InsufficientObservations {
                observed: window.sample_count,
                required: contract.minimum_observations(),
                first_decision_at: window.first_decision_at,
                last_decision_at: window.last_decision_at,
                observed_window_secs,
                required_window_secs: contract.required_window_secs(),
            });
        }
        let first_decision_at = window.first_decision_at.ok_or_else(|| {
            invalid("sufficient shadow observations have no first decision timestamp")
        })?;
        let last_decision_at = window.last_decision_at.ok_or_else(|| {
            invalid("sufficient shadow observations have no last decision timestamp")
        })?;
        let mean_topn_overlap = window
            .mean_topn_overlap
            .ok_or_else(|| invalid("sufficient shadow observations have no overlap aggregate"))?;
        let observation_hash = CanonicalDigest::content_hash_typed(
            OBSERVATION_HASH_DOMAIN,
            ARTIFACT_FORMAT_VERSION,
            &(
                contract.contract_hash(),
                window.sample_count,
                first_decision_at,
                last_decision_at,
                observed_window_secs,
                mean_topn_overlap,
                window.any_hard_divergence,
            ),
        )?;
        let evidence = FeedbackShadowEvidence {
            observation_hash,
            observed: window.sample_count,
            first_decision_at,
            last_decision_at,
            observed_window_secs,
            mean_topn_overlap,
            any_hard_divergence: window.any_hard_divergence,
        };
        let mut reasons = Vec::with_capacity(2);
        if window.any_hard_divergence {
            reasons.push(FeedbackShadowUnstableReason::HardDivergence);
        }
        if mean_topn_overlap.inner() < contract.minimum_topn_overlap().inner() {
            reasons.push(FeedbackShadowUnstableReason::TopnOverlapBelowMinimum);
        }
        if reasons.is_empty() {
            Ok(FeedbackShadowOutcome::Stable { evidence })
        } else {
            Ok(FeedbackShadowOutcome::Unstable { evidence, reasons })
        }
    }

    fn observed_secs(window: &ShadowObservationWindow) -> Result<u64, FeedbackError> {
        match (window.first_decision_at, window.last_decision_at) {
            (None, None)
                if window.sample_count == 0
                    && window.mean_topn_overlap.is_none()
                    && !window.any_hard_divergence =>
            {
                Ok(0)
            }
            (Some(first), Some(last)) if window.sample_count > 0 && first <= last => {
                u64::try_from(last.signed_duration_since(first).num_seconds()).map_err(|error| {
                    invalid(format!("shadow observation duration is invalid: {error}"))
                })
            }
            _ => Err(invalid(
                "shadow observation count, timestamps, or aggregate shape is inconsistent",
            )),
        }
    }
}

/// Sealing input for one immutable F10 artifact.
pub struct FeedbackShadowReplayArtifactInput {
    pub artifact_id: FeedbackShadowReplayArtifactId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub job_input_hash: ContentHash,
    pub previous: FeedbackComparisonArtifactRef,
    pub profile_ref: ResearchProfileRef,
    pub feedback_policy_hash: ContentHash,
    pub subject: FeedbackShadowSubject,
    pub outcome: FeedbackShadowOutcome,
}

/// Content-addressed F10 output with no decision or promotion authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "FeedbackShadowReplayArtifactDocument")]
pub struct FeedbackShadowReplayArtifact {
    format_version: u32,
    artifact_hash: ContentHash,
    artifact_id: FeedbackShadowReplayArtifactId,
    feedback_cycle_id: FeedbackCycleId,
    job_input_hash: ContentHash,
    previous: FeedbackComparisonArtifactRef,
    profile_ref: ResearchProfileRef,
    feedback_policy_hash: ContentHash,
    subject: FeedbackShadowSubject,
    outcome: FeedbackShadowOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedbackShadowReplayArtifactDocument {
    format_version: u32,
    artifact_hash: ContentHash,
    artifact_id: FeedbackShadowReplayArtifactId,
    feedback_cycle_id: FeedbackCycleId,
    job_input_hash: ContentHash,
    previous: FeedbackComparisonArtifactRef,
    profile_ref: ResearchProfileRef,
    feedback_policy_hash: ContentHash,
    subject: FeedbackShadowSubject,
    outcome: FeedbackShadowOutcome,
}

#[derive(Serialize)]
struct FeedbackShadowReplayArtifactPreimage<'a> {
    format_version: u32,
    artifact_id: FeedbackShadowReplayArtifactId,
    feedback_cycle_id: FeedbackCycleId,
    job_input_hash: ContentHash,
    previous: &'a FeedbackComparisonArtifactRef,
    profile_ref: &'a ResearchProfileRef,
    feedback_policy_hash: ContentHash,
    subject: &'a FeedbackShadowSubject,
    outcome: &'a FeedbackShadowOutcome,
}

impl FeedbackShadowReplayArtifact {
    pub fn try_seal(input: FeedbackShadowReplayArtifactInput) -> Result<Self, FeedbackError> {
        let artifact_hash = Self::derive_hash(&FeedbackShadowReplayArtifactPreimage {
            format_version: ARTIFACT_FORMAT_VERSION,
            artifact_id: input.artifact_id,
            feedback_cycle_id: input.feedback_cycle_id,
            job_input_hash: input.job_input_hash,
            previous: &input.previous,
            profile_ref: &input.profile_ref,
            feedback_policy_hash: input.feedback_policy_hash,
            subject: &input.subject,
            outcome: &input.outcome,
        })?;
        let artifact = Self {
            format_version: ARTIFACT_FORMAT_VERSION,
            artifact_hash,
            artifact_id: input.artifact_id,
            feedback_cycle_id: input.feedback_cycle_id,
            job_input_hash: input.job_input_hash,
            previous: input.previous,
            profile_ref: input.profile_ref,
            feedback_policy_hash: input.feedback_policy_hash,
            subject: input.subject,
            outcome: input.outcome,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.previous.validate_for(self.feedback_cycle_id)?;
        self.profile_ref
            .validate()
            .map_err(|error| invalid(format!("shadow artifact profile is invalid: {error}")))?;
        let profile = self
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(invalid)?;
        let expected_policy_hash = profile
            .spec
            .feedback_policy
            .content_hash()
            .map_err(|error| invalid(error.to_string()))?;
        if self.format_version != ARTIFACT_FORMAT_VERSION
            || self.artifact_id
                != FeedbackShadowReplayArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.feedback_policy_hash != expected_policy_hash
            || self.artifact_hash != Self::derive_hash(&self.preimage())?
        {
            return Err(invalid(
                "shadow artifact identity or content hash is invalid",
            ));
        }
        match (&self.subject, &self.outcome) {
            (
                FeedbackShadowSubject::NoEligibleCandidate { reason: expected },
                FeedbackShadowOutcome::NoEligibleCandidate { reason: actual },
            ) if expected == actual => Ok(()),
            (
                FeedbackShadowSubject::Candidate { contract, .. },
                FeedbackShadowOutcome::InsufficientObservations {
                    observed,
                    required,
                    first_decision_at,
                    last_decision_at,
                    observed_window_secs,
                    required_window_secs,
                },
            ) => {
                Self::require_contract(contract, &self.profile_ref, self.feedback_policy_hash)?;
                let count_insufficient = observed < required;
                let time_insufficient = observed_window_secs < required_window_secs;
                if *required != contract.minimum_observations()
                    || *required_window_secs != contract.required_window_secs()
                    || (!count_insufficient && !time_insufficient)
                    || (first_decision_at.is_none() != last_decision_at.is_none())
                {
                    return Err(invalid(
                        "insufficient shadow outcome differs from its exact contract",
                    ));
                }
                Ok(())
            }
            (
                FeedbackShadowSubject::Candidate { contract, .. },
                FeedbackShadowOutcome::Stable { evidence },
            ) => {
                Self::require_contract(contract, &self.profile_ref, self.feedback_policy_hash)?;
                Self::validate_evidence(contract, evidence, &[])
            }
            (
                FeedbackShadowSubject::Candidate { contract, .. },
                FeedbackShadowOutcome::Unstable { evidence, reasons },
            ) if !reasons.is_empty() => {
                Self::require_contract(contract, &self.profile_ref, self.feedback_policy_hash)?;
                Self::validate_evidence(contract, evidence, reasons)
            }
            _ => Err(invalid(
                "shadow artifact subject and typed outcome are contradictory",
            )),
        }
    }

    pub fn validate_for(&self, params: &FeedbackShadowJobParams) -> Result<(), FeedbackError> {
        params.validate()?;
        self.validate()?;
        if self.artifact_id != params.artifact_id
            || self.feedback_cycle_id != params.feedback_cycle_id
            || self.job_input_hash != params.input_hash()?
            || self.previous != params.previous
            || self.profile_ref != params.profile_ref
            || self.feedback_policy_hash != params.feedback_policy_hash
            || self.subject != params.subject
        {
            return Err(invalid(
                "shadow artifact differs from its exact terminal job input",
            ));
        }
        Ok(())
    }

    fn validate_evidence(
        contract: &FeedbackShadowContract,
        evidence: &FeedbackShadowEvidence,
        reasons: &[FeedbackShadowUnstableReason],
    ) -> Result<(), FeedbackError> {
        contract.validate()?;
        let expected_hash = CanonicalDigest::content_hash_typed(
            OBSERVATION_HASH_DOMAIN,
            ARTIFACT_FORMAT_VERSION,
            &(
                contract.contract_hash(),
                evidence.observed,
                evidence.first_decision_at,
                evidence.last_decision_at,
                evidence.observed_window_secs,
                evidence.mean_topn_overlap,
                evidence.any_hard_divergence,
            ),
        )?;
        let mut expected_reasons = Vec::with_capacity(2);
        if evidence.any_hard_divergence {
            expected_reasons.push(FeedbackShadowUnstableReason::HardDivergence);
        }
        if evidence.mean_topn_overlap.inner() < contract.minimum_topn_overlap().inner() {
            expected_reasons.push(FeedbackShadowUnstableReason::TopnOverlapBelowMinimum);
        }
        if evidence.observed < contract.minimum_observations()
            || evidence.observed_window_secs < contract.required_window_secs()
            || evidence.first_decision_at > evidence.last_decision_at
            || evidence.observation_hash != expected_hash
            || reasons != expected_reasons
        {
            return Err(invalid(
                "shadow numeric evidence or typed stability verdict is invalid",
            ));
        }
        Ok(())
    }

    fn require_contract(
        contract: &FeedbackShadowContract,
        profile_ref: &ResearchProfileRef,
        feedback_policy_hash: ContentHash,
    ) -> Result<(), FeedbackError> {
        contract.validate()?;
        if contract.profile_ref() != profile_ref
            || contract.feedback_policy_hash() != feedback_policy_hash
        {
            return Err(invalid(
                "shadow candidate contract differs from artifact profile or policy",
            ));
        }
        Ok(())
    }

    const fn preimage(&self) -> FeedbackShadowReplayArtifactPreimage<'_> {
        FeedbackShadowReplayArtifactPreimage {
            format_version: self.format_version,
            artifact_id: self.artifact_id,
            feedback_cycle_id: self.feedback_cycle_id,
            job_input_hash: self.job_input_hash,
            previous: &self.previous,
            profile_ref: &self.profile_ref,
            feedback_policy_hash: self.feedback_policy_hash,
            subject: &self.subject,
            outcome: &self.outcome,
        }
    }

    fn derive_hash(
        preimage: &FeedbackShadowReplayArtifactPreimage<'_>,
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(ARTIFACT_HASH_DOMAIN, ARTIFACT_FORMAT_VERSION, preimage)
            .map_err(Into::into)
    }

    #[must_use]
    pub const fn artifact_id(&self) -> FeedbackShadowReplayArtifactId {
        self.artifact_id
    }

    #[must_use]
    pub const fn artifact_hash(&self) -> ContentHash {
        self.artifact_hash
    }

    #[must_use]
    pub const fn feedback_cycle_id(&self) -> FeedbackCycleId {
        self.feedback_cycle_id
    }

    #[must_use]
    pub const fn job_input_hash(&self) -> ContentHash {
        self.job_input_hash
    }

    #[must_use]
    pub const fn previous(&self) -> &FeedbackComparisonArtifactRef {
        &self.previous
    }

    #[must_use]
    pub const fn profile_ref(&self) -> &ResearchProfileRef {
        &self.profile_ref
    }

    #[must_use]
    pub const fn feedback_policy_hash(&self) -> ContentHash {
        self.feedback_policy_hash
    }

    #[must_use]
    pub const fn subject(&self) -> &FeedbackShadowSubject {
        &self.subject
    }

    #[must_use]
    pub const fn outcome(&self) -> &FeedbackShadowOutcome {
        &self.outcome
    }
}

impl TryFrom<FeedbackShadowReplayArtifactDocument> for FeedbackShadowReplayArtifact {
    type Error = FeedbackError;

    fn try_from(document: FeedbackShadowReplayArtifactDocument) -> Result<Self, Self::Error> {
        let artifact = Self {
            format_version: document.format_version,
            artifact_hash: document.artifact_hash,
            artifact_id: document.artifact_id,
            feedback_cycle_id: document.feedback_cycle_id,
            job_input_hash: document.job_input_hash,
            previous: document.previous,
            profile_ref: document.profile_ref,
            feedback_policy_hash: document.feedback_policy_hash,
            subject: document.subject,
            outcome: document.outcome,
        };
        artifact.validate()?;
        Ok(artifact)
    }
}

/// Canonical JSON boundary for F10 artifacts.
pub struct FeedbackShadowReplayCodec;

impl FeedbackShadowReplayCodec {
    pub fn encode(artifact: &FeedbackShadowReplayArtifact) -> QuantResult<Vec<u8>> {
        artifact.validate()?;
        CanonicalDigest::canonical_json_bytes(artifact).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> QuantResult<FeedbackShadowReplayArtifact> {
        let artifact =
            serde_json::from_slice::<FeedbackShadowReplayArtifact>(bytes).map_err(|error| {
                ResearchError::Serialization {
                    detail: format!("decode feedback shadow artifact: {error}"),
                }
            })?;
        artifact.validate()?;
        if Self::encode(&artifact)? != bytes {
            return Err(ResearchError::Serialization {
                detail: "feedback shadow artifact is not canonical JSON".to_owned(),
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
                "comparison_predecessor",
                "profile_policy",
                "published_generation_subject",
                "typed_observation_outcome",
            ],
        )
        .map_err(Into::into)
    }
}

fn invalid(detail: impl Into<String>) -> FeedbackError {
    FeedbackError::InvalidComparisonEvidence {
        detail: detail.into(),
    }
}
