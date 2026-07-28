//! Same-window candidate-family comparison with Romano-Wolf FWER control.

use std::cmp::Ordering;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, feedback::FeedbackError, research::ResearchError};
use quant_pivot_models::{
    domain::ports::{
        FeedbackComparisonContract, FeedbackComparisonJobParams, FeedbackEvaluationUseRef,
    },
    hashing::CanonicalDigest,
    types::{
        BacktestPathSetId, BacktestReportId, Bps, ContentHash, FeedbackComparisonArtifactId,
        FeedbackCycleId, ModelRunId, ModelVersionId, Usd,
    },
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};

use crate::backtest::PortfolioReturnObservation;

const OBSERVATION_HASH_VERSION: u32 = 1;
const OBSERVATION_HASH_DOMAIN: &str = "quant-pivot/feedback-comparison-observations";
const NULL_MATRIX_HASH_VERSION: u32 = 1;
const NULL_MATRIX_HASH_DOMAIN: &str = "quant-pivot/feedback-comparison-null-matrix";
const NULL_ROW_HASH_DOMAIN: &str = "quant-pivot/feedback-comparison-null-row";
const GENERATOR_VERSION: u32 = 1;
const GENERATOR_DOMAIN: &str = "quant-pivot/feedback-comparison-bootstrap-index";
const ARTIFACT_FORMAT_VERSION: u32 = 1;
const ARTIFACT_HASH_DOMAIN: &str = "quant-pivot/feedback-comparison-artifact";
const ARTIFACT_SCHEMA_DOMAIN: &str = "quant-pivot/feedback-comparison-schema";

/// One recipe's same-window return series.
pub struct RomanoWolfCandidateInput<'a> {
    pub candidate_recipe_hash: ContentHash,
    pub observations: &'a [PortfolioReturnObservation],
}

/// Numeric comparison evidence for one predeclared candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RomanoWolfCandidateResult {
    pub candidate_recipe_hash: ContentHash,
    pub observation_hash: ContentHash,
    pub effect_bps: Bps,
    pub simultaneous_lower_bound_bps: Bps,
    pub raw_p_value: Decimal,
    pub adjusted_p_value: Decimal,
    pub confidence: Decimal,
    pub familywise_alpha: Decimal,
    pub gate_verdict: RomanoWolfGateVerdict,
}

/// Typed reason a candidate failed one governed comparison gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RomanoWolfGateFailure {
    NonPositiveEffect,
    MinimumEffectNotMet,
    FamilywiseConfidenceNotMet,
    NonPositiveSimultaneousBound,
}

/// Complete, non-contradictory gate result derived from numeric evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "snake_case", deny_unknown_fields)]
pub enum RomanoWolfGateVerdict {
    Eligible,
    Rejected {
        failures: Vec<RomanoWolfGateFailure>,
    },
}

impl RomanoWolfGateVerdict {
    fn evaluate(
        effect: Decimal,
        minimum_effect: Decimal,
        adjusted_p_value: Decimal,
        familywise_alpha: Decimal,
        simultaneous_lower_bound: Decimal,
    ) -> Self {
        let mut failures = Vec::with_capacity(4);
        if effect <= Decimal::ZERO {
            failures.push(RomanoWolfGateFailure::NonPositiveEffect);
        }
        if effect < minimum_effect {
            failures.push(RomanoWolfGateFailure::MinimumEffectNotMet);
        }
        if adjusted_p_value > familywise_alpha {
            failures.push(RomanoWolfGateFailure::FamilywiseConfidenceNotMet);
        }
        if simultaneous_lower_bound <= Decimal::ZERO {
            failures.push(RomanoWolfGateFailure::NonPositiveSimultaneousBound);
        }
        if failures.is_empty() {
            Self::Eligible
        } else {
            Self::Rejected { failures }
        }
    }

    #[must_use]
    pub const fn is_eligible(&self) -> bool {
        matches!(self, Self::Eligible)
    }
}

impl RomanoWolfCandidateResult {
    fn validate(
        &self,
        contract: &FeedbackComparisonContract,
        simultaneous_critical: Decimal,
    ) -> Result<(), FeedbackError> {
        let alpha = Decimal::ONE
            .checked_sub(contract.confidence())
            .ok_or_else(|| invalid("comparison familywise alpha overflowed"))?;
        let expected_lower = self
            .effect_bps
            .inner()
            .checked_sub(simultaneous_critical)
            .map(|value| value.round_dp(contract.effect_precision_dp()))
            .ok_or_else(|| invalid("simultaneous lower-bound validation overflowed"))?;
        let expected_verdict = RomanoWolfGateVerdict::evaluate(
            self.effect_bps.inner(),
            contract.minimum_effect_bps().inner(),
            self.adjusted_p_value,
            alpha,
            self.simultaneous_lower_bound_bps.inner(),
        );
        if self.raw_p_value < Decimal::ZERO
            || self.raw_p_value > Decimal::ONE
            || self.adjusted_p_value < self.raw_p_value
            || self.adjusted_p_value > Decimal::ONE
            || self.confidence != contract.confidence()
            || self.familywise_alpha != alpha
            || self.simultaneous_lower_bound_bps.inner() != expected_lower
            || self.gate_verdict != expected_verdict
        {
            return Err(invalid(
                "candidate comparison metric, interval, p-value, or gate verdict is invalid",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub const fn is_eligible(&self) -> bool {
        self.gate_verdict.is_eligible()
    }
}

/// Complete numeric evidence when the governed observation floor is met.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RomanoWolfEvidence {
    pub observation_count: u64,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub capital_base_usd: Usd,
    pub champion_observation_hash: ContentHash,
    pub bootstrap_null_matrix_hash: ContentHash,
    pub simultaneous_critical_value_bps: Bps,
    pub candidates: Vec<RomanoWolfCandidateResult>,
}

/// Typed comparison outcome. Insufficient evidence carries no fabricated
/// effect, confidence interval, p-value, or bootstrap statistic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum RomanoWolfOutcome {
    InsufficientObservations {
        observed: u64,
        required: u64,
        champion_observation_hash: ContentHash,
        candidate_observation_hashes: Vec<ContentHash>,
    },
    Compared {
        evidence: RomanoWolfEvidence,
    },
}

/// Exact challenger replay bound to one CPCV-qualified recipe and path set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackComparisonReplayRef {
    pub candidate_recipe_hash: ContentHash,
    pub model_version_id: ModelVersionId,
    pub serving_contract_hash: ContentHash,
    pub path_set_id: BacktestPathSetId,
    pub path_set_hash: ContentHash,
    pub model_run_id: ModelRunId,
    pub backtest_report_id: BacktestReportId,
    pub backtest_report_hash: ContentHash,
    pub observation_hash: ContentHash,
}

/// Sealing input for one immutable comparison artifact.
pub struct FeedbackComparisonArtifactInput {
    pub artifact_id: FeedbackComparisonArtifactId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub job_input_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub comparison_contract: FeedbackComparisonContract,
    pub evaluation_use: FeedbackEvaluationUseRef,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub champion_model_run_id: ModelRunId,
    pub champion_backtest_report_id: BacktestReportId,
    pub champion_backtest_report_hash: ContentHash,
    pub champion_observation_hash: ContentHash,
    pub candidate_replays: Vec<FeedbackComparisonReplayRef>,
    pub outcome: RomanoWolfOutcome,
}

/// Content-addressed F09 output. It records comparison evidence only and has no
/// terminal-decision or promotion authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "FeedbackComparisonArtifactDocument")]
pub struct FeedbackComparisonArtifact {
    format_version: u32,
    artifact_hash: ContentHash,
    artifact_id: FeedbackComparisonArtifactId,
    feedback_cycle_id: FeedbackCycleId,
    job_input_hash: ContentHash,
    candidate_family_hash: ContentHash,
    comparison_contract: FeedbackComparisonContract,
    evaluation_use: FeedbackEvaluationUseRef,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    champion_model_run_id: ModelRunId,
    champion_backtest_report_id: BacktestReportId,
    champion_backtest_report_hash: ContentHash,
    champion_observation_hash: ContentHash,
    candidate_replays: Vec<FeedbackComparisonReplayRef>,
    outcome: RomanoWolfOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedbackComparisonArtifactDocument {
    format_version: u32,
    artifact_hash: ContentHash,
    artifact_id: FeedbackComparisonArtifactId,
    feedback_cycle_id: FeedbackCycleId,
    job_input_hash: ContentHash,
    candidate_family_hash: ContentHash,
    comparison_contract: FeedbackComparisonContract,
    evaluation_use: FeedbackEvaluationUseRef,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    champion_model_run_id: ModelRunId,
    champion_backtest_report_id: BacktestReportId,
    champion_backtest_report_hash: ContentHash,
    champion_observation_hash: ContentHash,
    candidate_replays: Vec<FeedbackComparisonReplayRef>,
    outcome: RomanoWolfOutcome,
}

#[derive(Serialize)]
struct FeedbackComparisonArtifactPreimage<'a> {
    format_version: u32,
    artifact_id: FeedbackComparisonArtifactId,
    feedback_cycle_id: FeedbackCycleId,
    job_input_hash: ContentHash,
    candidate_family_hash: ContentHash,
    comparison_contract: &'a FeedbackComparisonContract,
    evaluation_use: &'a FeedbackEvaluationUseRef,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    champion_model_run_id: ModelRunId,
    champion_backtest_report_id: BacktestReportId,
    champion_backtest_report_hash: ContentHash,
    champion_observation_hash: ContentHash,
    candidate_replays: &'a [FeedbackComparisonReplayRef],
    outcome: &'a RomanoWolfOutcome,
}

impl FeedbackComparisonArtifact {
    pub fn try_seal(input: FeedbackComparisonArtifactInput) -> Result<Self, FeedbackError> {
        let artifact_hash = Self::derive_hash(&FeedbackComparisonArtifactPreimage {
            format_version: ARTIFACT_FORMAT_VERSION,
            artifact_id: input.artifact_id,
            feedback_cycle_id: input.feedback_cycle_id,
            job_input_hash: input.job_input_hash,
            candidate_family_hash: input.candidate_family_hash,
            comparison_contract: &input.comparison_contract,
            evaluation_use: &input.evaluation_use,
            champion_model_version_id: input.champion_model_version_id,
            champion_serving_contract_hash: input.champion_serving_contract_hash,
            champion_model_run_id: input.champion_model_run_id,
            champion_backtest_report_id: input.champion_backtest_report_id,
            champion_backtest_report_hash: input.champion_backtest_report_hash,
            champion_observation_hash: input.champion_observation_hash,
            candidate_replays: &input.candidate_replays,
            outcome: &input.outcome,
        })?;
        let artifact = Self {
            format_version: ARTIFACT_FORMAT_VERSION,
            artifact_hash,
            artifact_id: input.artifact_id,
            feedback_cycle_id: input.feedback_cycle_id,
            job_input_hash: input.job_input_hash,
            candidate_family_hash: input.candidate_family_hash,
            comparison_contract: input.comparison_contract,
            evaluation_use: input.evaluation_use,
            champion_model_version_id: input.champion_model_version_id,
            champion_serving_contract_hash: input.champion_serving_contract_hash,
            champion_model_run_id: input.champion_model_run_id,
            champion_backtest_report_id: input.champion_backtest_report_id,
            champion_backtest_report_hash: input.champion_backtest_report_hash,
            champion_observation_hash: input.champion_observation_hash,
            candidate_replays: input.candidate_replays,
            outcome: input.outcome,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.comparison_contract.validate()?;
        if self.format_version != ARTIFACT_FORMAT_VERSION
            || self.artifact_id
                != FeedbackComparisonArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.evaluation_use.feedback_cycle_id != self.feedback_cycle_id
            || self.evaluation_use.candidate_family_hash != self.candidate_family_hash
            || self.evaluation_use.comparison_contract_hash
                != self.comparison_contract.comparison_contract_hash()
            || self.evaluation_use.champion_model_version_id != self.champion_model_version_id
            || self.evaluation_use.champion_serving_contract_hash
                != self.champion_serving_contract_hash
            || self.champion_model_run_id
                != ModelRunId::from_feedback_comparison(
                    self.artifact_id,
                    self.champion_model_version_id,
                )
            || self.champion_backtest_report_id
                != BacktestReportId::from_feedback_comparison(
                    self.artifact_id,
                    self.champion_model_version_id,
                )
            || self.candidate_replays.is_empty()
        {
            return Err(invalid(
                "comparison artifact identity, reservation, champion, or family is invalid",
            ));
        }
        let mut previous = None;
        for replay in &self.candidate_replays {
            if previous.is_some_and(|previous| previous >= replay.candidate_recipe_hash)
                || replay.model_version_id == self.champion_model_version_id
                || replay.model_run_id
                    != ModelRunId::from_feedback_comparison(
                        self.artifact_id,
                        replay.model_version_id,
                    )
                || replay.backtest_report_id
                    != BacktestReportId::from_feedback_comparison(
                        self.artifact_id,
                        replay.model_version_id,
                    )
            {
                return Err(invalid(
                    "comparison artifact challenger replays are not canonical",
                ));
            }
            previous = Some(replay.candidate_recipe_hash);
        }
        self.validate_outcome()?;
        let expected_hash = Self::derive_hash(&self.preimage())?;
        if self.artifact_hash != expected_hash {
            return Err(invalid(
                "comparison artifact hash differs from its canonical preimage",
            ));
        }
        Ok(())
    }

    /// Revalidate this immutable object against the exact terminal job input.
    pub fn validate_for(&self, params: &FeedbackComparisonJobParams) -> Result<(), FeedbackError> {
        params.validate()?;
        self.validate()?;
        let candidates_match =
            params.candidates.len() == self.candidate_replays.len()
                && params.candidates.iter().zip(&self.candidate_replays).all(
                    |(candidate, replay)| {
                        candidate.candidate_recipe_hash == replay.candidate_recipe_hash
                            && candidate.model_version_id == replay.model_version_id
                            && candidate.serving_contract_hash == replay.serving_contract_hash
                            && candidate.path_set_id == replay.path_set_id
                            && candidate.path_set_hash == replay.path_set_hash
                            && candidate.model_run_id == replay.model_run_id
                            && candidate.backtest_report_id == replay.backtest_report_id
                    },
                );
        if self.artifact_id != params.artifact_id
            || self.feedback_cycle_id != params.feedback_cycle_id
            || self.job_input_hash != params.input_hash()?
            || self.candidate_family_hash != params.candidate_family_hash
            || self.comparison_contract != params.comparison_contract
            || self.evaluation_use != params.evaluation_use
            || self.champion_model_version_id != params.champion_model_version_id
            || self.champion_serving_contract_hash != params.champion_serving_contract_hash
            || self.champion_model_run_id != params.champion_model_run_id
            || self.champion_backtest_report_id != params.champion_backtest_report_id
            || !candidates_match
        {
            return Err(invalid(
                "comparison artifact differs from its exact terminal job input",
            ));
        }
        Ok(())
    }

    fn validate_outcome(&self) -> Result<(), FeedbackError> {
        match &self.outcome {
            RomanoWolfOutcome::InsufficientObservations {
                observed,
                required,
                champion_observation_hash,
                candidate_observation_hashes,
            } => {
                if *required != self.comparison_contract.minimum_observations()
                    || observed >= required
                    || *champion_observation_hash != self.champion_observation_hash
                    || candidate_observation_hashes.len() != self.candidate_replays.len()
                    || !candidate_observation_hashes
                        .iter()
                        .zip(&self.candidate_replays)
                        .all(|(hash, replay)| *hash == replay.observation_hash)
                {
                    return Err(invalid(
                        "insufficient comparison outcome differs from replay evidence",
                    ));
                }
            }
            RomanoWolfOutcome::Compared { evidence } => {
                for result in &evidence.candidates {
                    result.validate(
                        &self.comparison_contract,
                        evidence.simultaneous_critical_value_bps.inner(),
                    )?;
                }
                let candidate_shape_matches = evidence.candidates.len()
                    == self.candidate_replays.len()
                    && evidence.candidates.iter().zip(&self.candidate_replays).all(
                        |(result, replay)| {
                            result.candidate_recipe_hash == replay.candidate_recipe_hash
                                && result.observation_hash == replay.observation_hash
                        },
                    );
                let window_matches = evidence.window_start
                    >= self.evaluation_use.evaluation_window_start
                    && evidence.window_end < self.evaluation_use.evaluation_window_end
                    && evidence.window_start <= evidence.window_end;
                if evidence.observation_count < self.comparison_contract.minimum_observations()
                    || !window_matches
                    || evidence.champion_observation_hash != self.champion_observation_hash
                    || !candidate_shape_matches
                {
                    return Err(invalid(
                        "numeric comparison outcome differs from its reserved replay evidence",
                    ));
                }
            }
        }
        Ok(())
    }

    fn preimage(&self) -> FeedbackComparisonArtifactPreimage<'_> {
        FeedbackComparisonArtifactPreimage {
            format_version: self.format_version,
            artifact_id: self.artifact_id,
            feedback_cycle_id: self.feedback_cycle_id,
            job_input_hash: self.job_input_hash,
            candidate_family_hash: self.candidate_family_hash,
            comparison_contract: &self.comparison_contract,
            evaluation_use: &self.evaluation_use,
            champion_model_version_id: self.champion_model_version_id,
            champion_serving_contract_hash: self.champion_serving_contract_hash,
            champion_model_run_id: self.champion_model_run_id,
            champion_backtest_report_id: self.champion_backtest_report_id,
            champion_backtest_report_hash: self.champion_backtest_report_hash,
            champion_observation_hash: self.champion_observation_hash,
            candidate_replays: &self.candidate_replays,
            outcome: &self.outcome,
        }
    }

    fn derive_hash(
        preimage: &FeedbackComparisonArtifactPreimage<'_>,
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(ARTIFACT_HASH_DOMAIN, ARTIFACT_FORMAT_VERSION, preimage)
            .map_err(FeedbackError::from)
    }

    #[must_use]
    pub const fn artifact_hash(&self) -> ContentHash {
        self.artifact_hash
    }

    #[must_use]
    pub const fn artifact_id(&self) -> FeedbackComparisonArtifactId {
        self.artifact_id
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
    pub const fn candidate_family_hash(&self) -> ContentHash {
        self.candidate_family_hash
    }

    #[must_use]
    pub const fn comparison_contract(&self) -> &FeedbackComparisonContract {
        &self.comparison_contract
    }

    #[must_use]
    pub const fn evaluation_use(&self) -> &FeedbackEvaluationUseRef {
        &self.evaluation_use
    }

    #[must_use]
    pub const fn champion_model_version_id(&self) -> ModelVersionId {
        self.champion_model_version_id
    }

    #[must_use]
    pub const fn champion_serving_contract_hash(&self) -> ContentHash {
        self.champion_serving_contract_hash
    }

    #[must_use]
    pub const fn outcome(&self) -> &RomanoWolfOutcome {
        &self.outcome
    }

    #[must_use]
    pub fn candidate_replays(&self) -> &[FeedbackComparisonReplayRef] {
        &self.candidate_replays
    }

    /// Select the highest-effect eligible challenger with a stable recipe-hash
    /// tie break. The immutable comparison artifact is the canonical owner of
    /// this projection for both shadow binding and terminal decision.
    #[must_use]
    pub fn selected_candidate(
        &self,
    ) -> Option<(&RomanoWolfCandidateResult, &FeedbackComparisonReplayRef)> {
        let RomanoWolfOutcome::Compared { evidence } = &self.outcome else {
            return None;
        };
        evidence
            .candidates
            .iter()
            .zip(&self.candidate_replays)
            .filter(|(result, _)| result.is_eligible())
            .max_by(|(left, _), (right, _)| {
                left.effect_bps
                    .inner()
                    .cmp(&right.effect_bps.inner())
                    .then_with(|| right.candidate_recipe_hash.cmp(&left.candidate_recipe_hash))
            })
    }
}

impl TryFrom<FeedbackComparisonArtifactDocument> for FeedbackComparisonArtifact {
    type Error = FeedbackError;

    fn try_from(document: FeedbackComparisonArtifactDocument) -> Result<Self, Self::Error> {
        let artifact = Self {
            format_version: document.format_version,
            artifact_hash: document.artifact_hash,
            artifact_id: document.artifact_id,
            feedback_cycle_id: document.feedback_cycle_id,
            job_input_hash: document.job_input_hash,
            candidate_family_hash: document.candidate_family_hash,
            comparison_contract: document.comparison_contract,
            evaluation_use: document.evaluation_use,
            champion_model_version_id: document.champion_model_version_id,
            champion_serving_contract_hash: document.champion_serving_contract_hash,
            champion_model_run_id: document.champion_model_run_id,
            champion_backtest_report_id: document.champion_backtest_report_id,
            champion_backtest_report_hash: document.champion_backtest_report_hash,
            champion_observation_hash: document.champion_observation_hash,
            candidate_replays: document.candidate_replays,
            outcome: document.outcome,
        };
        artifact.validate()?;
        Ok(artifact)
    }
}

/// Canonical JSON boundary for comparison artifacts.
pub struct FeedbackComparisonCodec;

impl FeedbackComparisonCodec {
    pub fn encode(artifact: &FeedbackComparisonArtifact) -> QuantResult<Vec<u8>> {
        artifact.validate()?;
        CanonicalDigest::canonical_json_bytes(artifact).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> QuantResult<FeedbackComparisonArtifact> {
        let artifact =
            serde_json::from_slice::<FeedbackComparisonArtifact>(bytes).map_err(|error| {
                ResearchError::Serialization {
                    detail: format!("decode feedback comparison artifact: {error}"),
                }
            })?;
        artifact.validate()?;
        if Self::encode(&artifact)? != bytes {
            return Err(ResearchError::Serialization {
                detail: "feedback comparison artifact is not canonical JSON".to_owned(),
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
                "candidate_family",
                "comparison_contract",
                "evaluation_use",
                "champion_replay",
                "candidate_replays",
                "romano_wolf_outcome",
            ],
        )
        .map_err(Into::into)
    }
}

/// Pure owner of the versioned basic Romano-Wolf `StepM` procedure.
pub struct RomanoWolfStepdown;

impl RomanoWolfStepdown {
    /// Compare every challenger with the champion over the exact same ordered
    /// decision-tick universe.
    pub fn evaluate(
        contract: &FeedbackComparisonContract,
        champion: &[PortfolioReturnObservation],
        candidates: &[RomanoWolfCandidateInput<'_>],
    ) -> Result<RomanoWolfOutcome, FeedbackError> {
        contract.validate()?;
        let window = SameWindow::validate(contract, champion, candidates)?;
        let champion_hash = observation_hash(champion)?;
        let candidate_hashes = candidates
            .iter()
            .map(|candidate| observation_hash(candidate.observations))
            .collect::<Result<Vec<_>, _>>()?;
        if window.observation_count < contract.minimum_observations() {
            return Ok(RomanoWolfOutcome::InsufficientObservations {
                observed: window.observation_count,
                required: contract.minimum_observations(),
                champion_observation_hash: champion_hash,
                candidate_observation_hashes: candidate_hashes,
            });
        }

        let differences = candidates
            .iter()
            .map(|candidate| paired_differences(champion, candidate.observations))
            .collect::<Result<Vec<_>, _>>()?;
        let effects = differences
            .iter()
            .map(|values| statistic_mean(values, contract.effect_precision_dp()))
            .collect::<Result<Vec<_>, _>>()?;
        let null_matrix = BootstrapMatrix::build(contract, &differences, &effects)?;
        let adjusted = stepdown_p_values(contract, &effects, &null_matrix.rows)?;
        let raw = raw_p_values(contract, &effects, &null_matrix.rows)?;
        let critical = simultaneous_critical(contract, &null_matrix.rows)?;
        let alpha = Decimal::ONE
            .checked_sub(contract.confidence())
            .ok_or_else(|| invalid("comparison familywise alpha overflowed"))?;
        let results = candidates
            .iter()
            .zip(candidate_hashes)
            .enumerate()
            .map(|(index, (candidate, observation_hash))| {
                let effect = effects[index];
                let lower = effect
                    .checked_sub(critical)
                    .ok_or_else(|| invalid("simultaneous lower bound overflowed"))?
                    .round_dp(contract.effect_precision_dp());
                Ok(RomanoWolfCandidateResult {
                    candidate_recipe_hash: candidate.candidate_recipe_hash,
                    observation_hash,
                    effect_bps: Bps::new(effect),
                    simultaneous_lower_bound_bps: Bps::new(lower),
                    raw_p_value: raw[index],
                    adjusted_p_value: adjusted[index],
                    confidence: contract.confidence(),
                    familywise_alpha: alpha,
                    gate_verdict: RomanoWolfGateVerdict::evaluate(
                        effect,
                        contract.minimum_effect_bps().inner(),
                        adjusted[index],
                        alpha,
                        lower,
                    ),
                })
            })
            .collect::<Result<Vec<_>, FeedbackError>>()?;

        Ok(RomanoWolfOutcome::Compared {
            evidence: RomanoWolfEvidence {
                observation_count: window.observation_count,
                window_start: window.window_start,
                window_end: window.window_end,
                capital_base_usd: window.capital_base_usd,
                champion_observation_hash: champion_hash,
                bootstrap_null_matrix_hash: null_matrix.matrix_hash,
                simultaneous_critical_value_bps: Bps::new(critical),
                candidates: results,
            },
        })
    }
}

struct SameWindow {
    observation_count: u64,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    capital_base_usd: Usd,
}

impl SameWindow {
    fn validate(
        contract: &FeedbackComparisonContract,
        champion: &[PortfolioReturnObservation],
        candidates: &[RomanoWolfCandidateInput<'_>],
    ) -> Result<Self, FeedbackError> {
        if candidates.is_empty() {
            return Err(invalid("comparison candidate family is empty"));
        }
        let mut previous_recipe = None;
        for candidate in candidates {
            if previous_recipe.is_some_and(|previous| previous >= candidate.candidate_recipe_hash) {
                return Err(invalid(
                    "comparison candidates are not strictly recipe-hash ordered",
                ));
            }
            previous_recipe = Some(candidate.candidate_recipe_hash);
        }
        validate_series(contract, champion)?;
        if champion.is_empty() {
            return Ok(Self {
                observation_count: 0,
                window_start: DateTime::<Utc>::MIN_UTC,
                window_end: DateTime::<Utc>::MIN_UTC,
                capital_base_usd: Usd::ZERO,
            });
        }
        let capital_base_usd = champion[0].capital_base_usd;
        for candidate in candidates {
            validate_series(contract, candidate.observations)?;
            if candidate.observations.len() != champion.len() {
                return Err(window_mismatch(
                    "champion and candidate observation counts differ",
                ));
            }
            for (index, (champion_row, candidate_row)) in
                champion.iter().zip(candidate.observations).enumerate()
            {
                if champion_row.decision_at != candidate_row.decision_at
                    || champion_row.capital_base_usd != candidate_row.capital_base_usd
                {
                    return Err(window_mismatch(format!(
                        "candidate differs from champion at decision-tick index {index}"
                    )));
                }
            }
        }
        Ok(Self {
            observation_count: u64::try_from(champion.len())
                .map_err(|error| invalid(format!("observation count does not fit u64: {error}")))?,
            window_start: champion[0].decision_at,
            window_end: champion[champion.len() - 1].decision_at,
            capital_base_usd,
        })
    }
}

struct BootstrapMatrix {
    rows: Vec<Vec<Decimal>>,
    matrix_hash: ContentHash,
}

impl BootstrapMatrix {
    fn build(
        contract: &FeedbackComparisonContract,
        differences: &[Vec<Decimal>],
        effects: &[Decimal],
    ) -> Result<Self, FeedbackError> {
        let observation_count = differences
            .first()
            .map(Vec::len)
            .ok_or_else(|| invalid("comparison difference matrix is empty"))?;
        let block_length = usize::try_from(contract.block_length())
            .map_err(|error| invalid(format!("block length does not fit usize: {error}")))?;
        let full_blocks = observation_count / block_length;
        let remainder = observation_count % block_length;
        let full_sums = block_sum_matrix(differences, block_length)?;
        let remainder_sums = if remainder == 0 {
            Vec::new()
        } else {
            block_sum_matrix(differences, remainder)?
        };
        let repetition_count = usize::try_from(contract.bootstrap_repetitions())
            .map_err(|error| invalid(format!("bootstrap count does not fit usize: {error}")))?;
        let mut rows = Vec::with_capacity(repetition_count);
        let mut row_hashes = Vec::with_capacity(repetition_count);
        for repetition in 0..contract.bootstrap_repetitions() {
            let mut totals = vec![Decimal::ZERO; differences.len()];
            for draw in 0..full_blocks {
                let start = bootstrap_index(
                    contract.bootstrap_seed(),
                    repetition,
                    u64::try_from(draw).map_err(|error| {
                        invalid(format!("bootstrap draw index does not fit u64: {error}"))
                    })?,
                    observation_count,
                )?;
                add_block(&mut totals, &full_sums, start)?;
            }
            if remainder > 0 {
                let start = bootstrap_index(
                    contract.bootstrap_seed(),
                    repetition,
                    u64::try_from(full_blocks).map_err(|error| {
                        invalid(format!(
                            "bootstrap remainder index does not fit u64: {error}"
                        ))
                    })?,
                    observation_count,
                )?;
                add_block(&mut totals, &remainder_sums, start)?;
            }
            let row = totals
                .into_iter()
                .zip(effects)
                .map(|(total, effect)| {
                    total
                        .checked_div(Decimal::from(u64::try_from(observation_count).map_err(
                            |error| {
                                invalid(format!(
                                    "bootstrap observation count does not fit u64: {error}"
                                ))
                            },
                        )?))
                        .and_then(|mean| mean.checked_sub(*effect))
                        .map(|value| value.round_dp(contract.effect_precision_dp()))
                        .ok_or_else(|| invalid("bootstrap null statistic overflowed"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            row_hashes.push(
                CanonicalDigest::content_hash_typed(
                    NULL_ROW_HASH_DOMAIN,
                    NULL_MATRIX_HASH_VERSION,
                    &row,
                )
                .map_err(FeedbackError::from)?,
            );
            rows.push(row);
        }
        let matrix_hash = CanonicalDigest::content_hash_typed(
            NULL_MATRIX_HASH_DOMAIN,
            NULL_MATRIX_HASH_VERSION,
            &row_hashes,
        )
        .map_err(FeedbackError::from)?;
        Ok(Self { rows, matrix_hash })
    }
}

fn validate_series(
    contract: &FeedbackComparisonContract,
    observations: &[PortfolioReturnObservation],
) -> Result<(), FeedbackError> {
    let mut previous_at = None;
    let mut capital_base = None;
    for observation in observations {
        if observation.capital_base_usd.inner() <= Decimal::ZERO {
            return Err(invalid(
                "comparison observation capital base must be positive",
            ));
        }
        if previous_at.is_some_and(|previous| previous >= observation.decision_at) {
            return Err(window_mismatch(
                "comparison observations are not strictly time ordered",
            ));
        }
        if capital_base.is_some_and(|base| base != observation.capital_base_usd) {
            return Err(window_mismatch(
                "comparison observation capital base changed within the window",
            ));
        }
        let expected = observation
            .realized_pnl_usd
            .inner()
            .checked_div(observation.capital_base_usd.inner())
            .and_then(|ratio| ratio.checked_mul(Decimal::from(10_000)))
            .map(|value| value.round_dp(contract.effect_precision_dp()))
            .ok_or_else(|| invalid("comparison observation return overflowed"))?;
        if observation.net_return_bps.inner() != expected {
            return Err(invalid(
                "comparison observation net return differs from PnL/capital",
            ));
        }
        previous_at = Some(observation.decision_at);
        capital_base = Some(observation.capital_base_usd);
    }
    Ok(())
}

fn paired_differences(
    champion: &[PortfolioReturnObservation],
    candidate: &[PortfolioReturnObservation],
) -> Result<Vec<Decimal>, FeedbackError> {
    champion
        .iter()
        .zip(candidate)
        .map(|(champion_row, candidate_row)| {
            candidate_row
                .net_return_bps
                .inner()
                .checked_sub(champion_row.net_return_bps.inner())
                .ok_or_else(|| invalid("paired comparison effect overflowed"))
        })
        .collect()
}

fn statistic_mean(values: &[Decimal], precision: u32) -> Result<Decimal, FeedbackError> {
    if values.is_empty() {
        return Err(invalid("comparison statistic has no observations"));
    }
    let total = values.iter().try_fold(Decimal::ZERO, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| invalid("comparison statistic sum overflowed"))
    })?;
    total
        .checked_div(Decimal::from(u64::try_from(values.len()).map_err(
            |error| invalid(format!("statistic count does not fit u64: {error}")),
        )?))
        .map(|mean| mean.round_dp(precision))
        .ok_or_else(|| invalid("comparison statistic mean overflowed"))
}

fn block_sum_matrix(
    differences: &[Vec<Decimal>],
    length: usize,
) -> Result<Vec<Vec<Decimal>>, FeedbackError> {
    differences
        .iter()
        .map(|series| circular_block_sums(series, length))
        .collect()
}

fn circular_block_sums(values: &[Decimal], length: usize) -> Result<Vec<Decimal>, FeedbackError> {
    if values.is_empty() || length == 0 || length > values.len() {
        return Err(invalid("circular bootstrap block dimensions are invalid"));
    }
    let first = values
        .iter()
        .take(length)
        .try_fold(Decimal::ZERO, |total, value| {
            total
                .checked_add(*value)
                .ok_or_else(|| invalid("bootstrap block sum overflowed"))
        })?;
    let mut sums = Vec::with_capacity(values.len());
    sums.push(first);
    for start in 1..values.len() {
        let outgoing = values[start - 1];
        let incoming = values[(start + length - 1) % values.len()];
        let next = sums[start - 1]
            .checked_sub(outgoing)
            .and_then(|value| value.checked_add(incoming))
            .ok_or_else(|| invalid("bootstrap rolling block sum overflowed"))?;
        sums.push(next);
    }
    Ok(sums)
}

fn add_block(
    totals: &mut [Decimal],
    block_sums: &[Vec<Decimal>],
    start: usize,
) -> Result<(), FeedbackError> {
    for (candidate, total) in totals.iter_mut().enumerate() {
        *total = total
            .checked_add(block_sums[candidate][start])
            .ok_or_else(|| invalid("bootstrap replication sum overflowed"))?;
    }
    Ok(())
}

#[derive(Serialize)]
struct BootstrapWord {
    generator_version: u32,
    seed: u64,
    repetition: u32,
    draw: u64,
    attempt: u64,
}

fn bootstrap_index(
    seed: u64,
    repetition: u32,
    draw: u64,
    upper_bound: usize,
) -> Result<usize, FeedbackError> {
    let bound = u64::try_from(upper_bound)
        .map_err(|error| invalid(format!("bootstrap bound does not fit u64: {error}")))?;
    if bound == 0 {
        return Err(invalid("bootstrap bound must be positive"));
    }
    let space = u128::from(u64::MAX) + 1;
    let acceptance_limit = space - space % u128::from(bound);
    let mut attempt = 0_u64;
    loop {
        let digest = CanonicalDigest::content_hash_typed(
            GENERATOR_DOMAIN,
            GENERATOR_VERSION,
            &BootstrapWord {
                generator_version: GENERATOR_VERSION,
                seed,
                repetition,
                draw,
                attempt,
            },
        )
        .map_err(FeedbackError::from)?;
        let bytes: [u8; 8] = digest.as_bytes()[..8]
            .try_into()
            .map_err(|error| invalid(format!("bootstrap digest word is invalid: {error}")))?;
        let word = u64::from_le_bytes(bytes);
        if u128::from(word) < acceptance_limit {
            let index = word % bound;
            return usize::try_from(index)
                .map_err(|error| invalid(format!("bootstrap index does not fit usize: {error}")));
        }
        attempt = attempt
            .checked_add(1)
            .ok_or_else(|| invalid("bootstrap rejection counter overflowed"))?;
    }
}

fn raw_p_values(
    contract: &FeedbackComparisonContract,
    effects: &[Decimal],
    null_rows: &[Vec<Decimal>],
) -> Result<Vec<Decimal>, FeedbackError> {
    effects
        .iter()
        .enumerate()
        .map(|(candidate, effect)| {
            let exceedances = null_rows
                .iter()
                .filter(|row| row[candidate] >= *effect)
                .count();
            plus_one_p_value(contract, exceedances)
        })
        .collect()
}

fn stepdown_p_values(
    contract: &FeedbackComparisonContract,
    effects: &[Decimal],
    null_rows: &[Vec<Decimal>],
) -> Result<Vec<Decimal>, FeedbackError> {
    let mut order = (0..effects.len()).collect::<Vec<_>>();
    order.sort_by(|left, right| {
        effects[*right]
            .partial_cmp(&effects[*left])
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.cmp(right))
    });
    let mut adjusted = vec![Decimal::ZERO; effects.len()];
    let mut prior = Decimal::ZERO;
    let mut group_start = 0_usize;
    while group_start < order.len() {
        let effect = effects[order[group_start]];
        let mut group_end = group_start + 1;
        while group_end < order.len() && effects[order[group_end]] == effect {
            group_end += 1;
        }
        let exceedances = null_rows
            .iter()
            .filter(|row| {
                order[group_start..]
                    .iter()
                    .map(|candidate| row[*candidate])
                    .max()
                    .is_some_and(|maximum| maximum >= effect)
            })
            .count();
        let current = plus_one_p_value(contract, exceedances)?.max(prior);
        for candidate in &order[group_start..group_end] {
            adjusted[*candidate] = current;
        }
        prior = current;
        group_start = group_end;
    }
    Ok(adjusted)
}

fn plus_one_p_value(
    contract: &FeedbackComparisonContract,
    exceedances: usize,
) -> Result<Decimal, FeedbackError> {
    let numerator = u64::try_from(exceedances)
        .map_err(|error| {
            invalid(format!(
                "bootstrap exceedance count does not fit u64: {error}"
            ))
        })?
        .checked_add(1)
        .ok_or_else(|| invalid("bootstrap exceedance count overflowed"))?;
    let denominator = u64::from(contract.bootstrap_repetitions())
        .checked_add(1)
        .ok_or_else(|| invalid("bootstrap p-value denominator overflowed"))?;
    Decimal::from(numerator)
        .checked_div(Decimal::from(denominator))
        .map(|value| value.round_dp(contract.effect_precision_dp()))
        .ok_or_else(|| invalid("bootstrap p-value division failed"))
}

fn simultaneous_critical(
    contract: &FeedbackComparisonContract,
    null_rows: &[Vec<Decimal>],
) -> Result<Decimal, FeedbackError> {
    let mut maxima = null_rows
        .iter()
        .map(|row| {
            row.iter()
                .copied()
                .max()
                .ok_or_else(|| invalid("bootstrap null row is empty"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    maxima.sort();
    let repetitions = contract.bootstrap_repetitions();
    let rank = (contract.confidence() * Decimal::from(u64::from(repetitions) + 1))
        .ceil()
        .to_u32()
        .ok_or_else(|| invalid("simultaneous confidence rank does not fit u32"))?
        .min(repetitions)
        .max(1);
    let index = usize::try_from(rank - 1)
        .map_err(|error| invalid(format!("confidence rank does not fit usize: {error}")))?;
    maxima
        .get(index)
        .copied()
        .ok_or_else(|| invalid("simultaneous confidence rank exceeds bootstrap evidence"))
}

fn observation_hash(
    observations: &[PortfolioReturnObservation],
) -> Result<ContentHash, FeedbackError> {
    CanonicalDigest::content_hash_typed(
        OBSERVATION_HASH_DOMAIN,
        OBSERVATION_HASH_VERSION,
        observations,
    )
    .map_err(FeedbackError::from)
}

fn invalid(detail: impl Into<String>) -> FeedbackError {
    FeedbackError::InvalidComparisonEvidence {
        detail: detail.into(),
    }
}

fn window_mismatch(detail: impl Into<String>) -> FeedbackError {
    FeedbackError::SameWindowMismatch {
        detail: detail.into(),
    }
}
