//! Reserved same-window feedback comparison execution.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{
    QuantError, QuantResult, feedback::FeedbackError, research::ResearchError,
    storage::StorageError,
};
use quant_pivot_models::{
    domain::{
        ports::{
            FeedbackComparisonExecutionPort, FeedbackComparisonExecutionResult,
            FeedbackComparisonJobParams,
        },
        quant::{JobProgressSink, ResearchJobArtifactRef},
    },
    types::{ContentHash, ResearchJobProgress},
};
use quant_pivot_repository::traits::BacktestPathSetRepository;
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    feedback_comparison::{
        FeedbackComparisonArtifact, FeedbackComparisonArtifactInput, FeedbackComparisonCodec,
        FeedbackComparisonReplayRef, RomanoWolfCandidateInput, RomanoWolfOutcome,
        RomanoWolfStepdown,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{app::ports::backtest::CoreBacktestPort, service::backtest::FeedbackFamilyReplay};

/// Dependencies for [`FeedbackComparisonExecutionService`].
pub struct FeedbackComparisonExecutionDeps {
    pub backtests: Arc<CoreBacktestPort>,
    pub path_sets: Arc<dyn BacktestPathSetRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
}

/// Executes F09 without writing model routes or terminal feedback decisions.
pub struct FeedbackComparisonExecutionService {
    backtests: Arc<CoreBacktestPort>,
    path_sets: Arc<dyn BacktestPathSetRepository>,
    artifacts: Arc<dyn ArtifactStore>,
}

impl FeedbackComparisonExecutionService {
    #[must_use]
    pub fn new(deps: FeedbackComparisonExecutionDeps) -> Self {
        Self {
            backtests: deps.backtests,
            path_sets: deps.path_sets,
            artifacts: deps.artifacts,
        }
    }

    async fn verify_path_sets(&self, params: &FeedbackComparisonJobParams) -> QuantResult<()> {
        for candidate in &params.candidates {
            let path_set = self
                .path_sets
                .find_by_id(&candidate.path_set_id)
                .await?
                .ok_or_else(|| {
                    StorageError::not_found("quant_backtest_path_set", candidate.path_set_id)
                })?;
            path_set.verify_hash().map_err(|error| {
                Self::invalid(format!(
                    "candidate {} CPCV path-set hash is invalid: {error}",
                    candidate.candidate_recipe_hash
                ))
            })?;
            if path_set.path_set_id != candidate.path_set_id
                || path_set.path_set_hash != candidate.path_set_hash
                || path_set.model_version_id != candidate.model_version_id
                || path_set.decision_policy_snapshot_id != params.decision_policy_snapshot_id
                || path_set.subject.serving_contract_hash != candidate.serving_contract_hash
            {
                return Err(Self::invalid(
                    "candidate CPCV path set differs from comparison job lineage",
                ));
            }
        }
        Ok(())
    }

    fn seal_artifact(
        params: &FeedbackComparisonJobParams,
        replay: FeedbackFamilyReplay,
    ) -> QuantResult<FeedbackComparisonArtifact> {
        if replay.champion.model_version_id != params.champion_model_version_id
            || replay.champion.serving_contract_hash != params.champion_serving_contract_hash
            || replay.candidates.len() != params.candidates.len()
        {
            return Err(Self::invalid(
                "shared replay output differs from comparison job subjects",
            ));
        }
        let statistical_inputs = params
            .candidates
            .iter()
            .zip(&replay.candidates)
            .map(|(candidate, output)| {
                if output.model_version_id != candidate.model_version_id
                    || output.serving_contract_hash != candidate.serving_contract_hash
                {
                    return Err(Self::invalid(
                        "candidate replay output differs from frozen model subject",
                    ));
                }
                Ok(RomanoWolfCandidateInput {
                    candidate_recipe_hash: candidate.candidate_recipe_hash,
                    observations: &output.portfolio_returns,
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let outcome = RomanoWolfStepdown::evaluate(
            &params.comparison_contract,
            &replay.champion.portfolio_returns,
            &statistical_inputs,
        )?;
        let (champion_observation_hash, candidate_observation_hashes) =
            Self::observation_hashes(&outcome);
        let candidate_replays = params
            .candidates
            .iter()
            .zip(replay.candidates)
            .zip(candidate_observation_hashes)
            .map(
                |((candidate, output), observation_hash)| FeedbackComparisonReplayRef {
                    candidate_recipe_hash: candidate.candidate_recipe_hash,
                    model_version_id: candidate.model_version_id,
                    serving_contract_hash: candidate.serving_contract_hash,
                    path_set_id: candidate.path_set_id,
                    path_set_hash: candidate.path_set_hash,
                    model_run_id: candidate.model_run_id,
                    backtest_report_id: candidate.backtest_report_id,
                    backtest_report_hash: output.report_hash,
                    observation_hash,
                },
            )
            .collect();
        FeedbackComparisonArtifact::try_seal(FeedbackComparisonArtifactInput {
            artifact_id: params.artifact_id,
            feedback_cycle_id: params.feedback_cycle_id,
            job_input_hash: params.input_hash()?,
            candidate_family_hash: params.candidate_family_hash,
            comparison_contract: params.comparison_contract.clone(),
            evaluation_use: params.evaluation_use.clone(),
            champion_model_version_id: params.champion_model_version_id,
            champion_serving_contract_hash: params.champion_serving_contract_hash,
            champion_model_run_id: params.champion_model_run_id,
            champion_backtest_report_id: params.champion_backtest_report_id,
            champion_backtest_report_hash: replay.champion.report_hash,
            champion_observation_hash,
            candidate_replays,
            outcome,
        })
        .map_err(Into::into)
    }

    fn observation_hashes(outcome: &RomanoWolfOutcome) -> (ContentHash, Vec<ContentHash>) {
        match outcome {
            RomanoWolfOutcome::InsufficientObservations {
                champion_observation_hash,
                candidate_observation_hashes,
                ..
            } => (
                *champion_observation_hash,
                candidate_observation_hashes.clone(),
            ),
            RomanoWolfOutcome::Compared { evidence } => (
                evidence.champion_observation_hash,
                evidence
                    .candidates
                    .iter()
                    .map(|candidate| candidate.observation_hash)
                    .collect(),
            ),
        }
    }

    async fn persist(
        &self,
        artifact: FeedbackComparisonArtifact,
    ) -> QuantResult<FeedbackComparisonExecutionResult> {
        let artifact_id = artifact.artifact_id();
        let bytes = FeedbackComparisonCodec::encode(&artifact)?;
        let content_hash = FeedbackComparisonCodec::bytes_hash(&bytes);
        let key = ArtifactKey::new(
            ArtifactNamespace::FeedbackComparison,
            content_hash.hex(),
            "json",
        )?;
        let uri = self.artifacts.put(key, &bytes).await?;
        let persisted = self.artifacts.get(&uri).await?;
        if FeedbackComparisonCodec::bytes_hash(&persisted) != content_hash
            || FeedbackComparisonCodec::decode(&persisted)? != artifact
        {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: content_hash.to_string(),
                actual: FeedbackComparisonCodec::bytes_hash(&persisted).to_string(),
            }
            .into());
        }
        Ok(FeedbackComparisonExecutionResult {
            artifact_id,
            artifact: ResearchJobArtifactRef { uri, content_hash },
        })
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidComparisonEvidence {
            detail: detail.into(),
        }
        .into()
    }
}

#[async_trait]
impl FeedbackComparisonExecutionPort for FeedbackComparisonExecutionService {
    async fn execute(
        &self,
        params: FeedbackComparisonJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackComparisonExecutionResult> {
        params.validate()?;
        self.verify_path_sets(&params).await?;
        progress.report(ResearchJobProgress::indeterminate(
            "comparison_preflight",
            0,
        ));
        let backtests = self
            .backtests
            .backtest_service_for(&params.decision_policy_snapshot_id)
            .await?;
        let replay =
            Box::pin(backtests.replay_feedback_family(&params, Arc::clone(&progress), cancel))
                .await?;
        progress.report(ResearchJobProgress::indeterminate(
            "comparison_statistics",
            0,
        ));
        let artifact = Self::seal_artifact(&params, replay)?;
        self.persist(artifact).await
    }
}
