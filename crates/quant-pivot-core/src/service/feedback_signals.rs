//! Durable feedback coverage and statistical-drift execution.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::{FeedbackCoverageJobParams, FeedbackDriftJobParams},
        ports::{
            FeedbackCoverageExecutionPort, FeedbackCoverageExecutionResult,
            FeedbackDriftExecutionPort, FeedbackDriftExecutionResult,
        },
        quant::{FeedbackCohortWindow, FeedbackCycleInfo, JobProgressSink, ResearchJobArtifactRef},
    },
    enums::quant::{FeedbackCycleStatus, FeedbackDriftMetric},
    hashing::CanonicalDigest,
    types::{
        ContentHash, FeedbackCoverageArtifactId, FeedbackCycleId, FeedbackDriftArtifactId,
        ModelRunId, ResearchJobProgress,
    },
};
use quant_pivot_repository::traits::{
    FactorRepository, FeatureRepository, FeedbackCohortRepository, FeedbackCycleRepository,
    ModelRegistryRepository, PolicyRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    feedback::{
        ChampionBaselineRef, ConceptDriftDetail, CoverageGateInput, CoverageGateOutcome,
        DriftObservation, FEEDBACK_COVERAGE_ARTIFACT_FORMAT_VERSION,
        FEEDBACK_DRIFT_ARTIFACT_FORMAT_VERSION, FeatureDriftDetail, FeedbackCoverageArtifact,
        FeedbackCoverageCodec, FeedbackDriftArtifact, FeedbackDriftCodec, LabelDriftDetail,
        drift_gate, drift_observations, jensen_shannon, rank_ic_drift,
    },
    model::QuantModelRuntime,
    training::{TOKEN_PAYOUT_RATIO, TrainingExample},
};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    app::ports::feedback_mutation::FeedbackCycleFreezePlan,
    governance::policy_snapshot::VerifiedPolicySnapshotBinding,
    projection::inference_batch::build_frozen_runtime_input,
    service::{
        feedback_dataset::{
            FeedbackCohortMaterializer, FeedbackCohortMaterializerDeps,
            FeedbackCoverageMaterialization,
        },
        model_serving_preimage::{ModelServingPreimageService, VerifiedModelServingPreimage},
        training_dataset::{require_dataset_materialization, verify_frozen_dataset_artifact},
    },
};

const PAYOUT_HISTOGRAM_BIN_COUNT: usize = 11;
const DRIFT_RUN_NAMESPACE: Uuid = Uuid::from_u128(0x6bc1_1f75_8ca8_4f31_9be5_17ad_1170_f208);

/// Dependencies for [`FeedbackSignalService`].
pub struct FeedbackSignalServiceDeps {
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub models: Arc<dyn ModelRegistryRepository>,
    pub policies: Arc<dyn PolicyRepository>,
    pub preimages: Arc<ModelServingPreimageService>,
    pub cohort_repository: Arc<dyn FeedbackCohortRepository>,
    pub feature_repository: Arc<dyn FeatureRepository>,
    pub factor_repository: Arc<dyn FactorRepository>,
    pub artifact_store: Arc<dyn ArtifactStore>,
}

/// Executes coverage and drift through verified immutable preimages only.
pub struct FeedbackSignalService {
    cycles: Arc<dyn FeedbackCycleRepository>,
    models: Arc<dyn ModelRegistryRepository>,
    policies: Arc<dyn PolicyRepository>,
    preimages: Arc<ModelServingPreimageService>,
    materializer: FeedbackCohortMaterializer,
    artifact_store: Arc<dyn ArtifactStore>,
}

impl FeedbackSignalService {
    #[must_use]
    pub fn new(deps: FeedbackSignalServiceDeps) -> Self {
        Self {
            cycles: deps.cycles,
            models: deps.models,
            policies: deps.policies,
            preimages: deps.preimages,
            materializer: FeedbackCohortMaterializer::new(FeedbackCohortMaterializerDeps {
                cohorts: deps.cohort_repository,
                features: deps.feature_repository,
                factors: deps.factor_repository,
            }),
            artifact_store: deps.artifact_store,
        }
    }

    async fn load_cycle(
        &self,
        feedback_cycle_id: FeedbackCycleId,
        idempotency_hash: ContentHash,
    ) -> QuantResult<FeedbackCycleInfo> {
        let cycle = self
            .cycles
            .find_cycle(&feedback_cycle_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_feedback_cycle", feedback_cycle_id))?;
        cycle.validate()?;
        if cycle.feedback_cycle_id != feedback_cycle_id
            || cycle.idempotency_hash != idempotency_hash
            || cycle.status != FeedbackCycleStatus::Running
        {
            return Err(contract(
                "feedback job parameters differ from a live exact cycle",
            ));
        }
        Ok(cycle)
    }

    async fn load_champion(
        &self,
        cycle: &FeedbackCycleInfo,
    ) -> QuantResult<VerifiedModelServingPreimage> {
        let version = self
            .models
            .find_model_version(&cycle.champion_model_version_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found("quant_model_version", cycle.champion_model_version_id)
            })?;
        let preimage = self.preimages.load(&version).await?;
        preimage.verify_feedback_cycle(cycle)?;
        self.verify_cycle_policy(cycle, &preimage).await?;
        Ok(preimage)
    }

    async fn verify_cycle_policy(
        &self,
        cycle: &FeedbackCycleInfo,
        preimage: &VerifiedModelServingPreimage,
    ) -> QuantResult<()> {
        let policy = self
            .policies
            .load_snapshot(&cycle.decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(
                    "decision_policy_snapshot",
                    cycle.decision_policy_snapshot_id,
                )
            })?;
        let verified = VerifiedPolicySnapshotBinding::try_from(&policy)?;
        let route = policy
            .snapshot
            .model_routing
            .model
            .route_binding(cycle.route)
            .map_err(|error| contract(format!("feedback Route binding failed: {error}")))?;
        let route_generation = i64::try_from(route.champion.generation).map_err(|error| {
            contract(format!(
                "feedback Route generation does not fit persistence type: {error}"
            ))
        })?;
        let source_profiles = &preimage
            .artifact()
            .header()
            .serving_contract()
            .bindings()
            .policy_snapshot
            .profile_artifacts;
        let valid = verified.binding().decision_policy_snapshot_id
            == cycle.decision_policy_snapshot_id
            && verified.binding().snapshot_hash == cycle.decision_policy_snapshot_hash
            && &verified.binding().profile_artifacts == source_profiles
            && route.champion.model_version_id == cycle.champion_model_version_id
            && route.champion.config_revision == cycle.policy_bundle_generation
            && route_generation == cycle.route_generation
            && route.champion.bound_at <= cycle.created_at;
        if !valid {
            return Err(contract(
                "decision-time policy or Route generation differs from the frozen feedback cycle",
            ));
        }
        Ok(())
    }

    fn evaluation_window(
        cycle: &FeedbackCycleInfo,
        preimage: &VerifiedModelServingPreimage,
    ) -> QuantResult<FeedbackCohortWindow> {
        let plan = FeedbackCycleFreezePlan::derive_at_cutoff(
            preimage.profile(),
            cycle.champion_model_spec_id,
            cycle.champion_model_spec_definition_hash,
            cycle.decision_policy_snapshot_id,
            cycle.decision_policy_snapshot_hash,
            cycle.label_cutoff,
        )?;
        Ok(plan.evaluation().clone())
    }

    fn baseline_ref(preimage: &VerifiedModelServingPreimage) -> QuantResult<ChampionBaselineRef> {
        let dataset = preimage.training_dataset();
        let materialization = require_dataset_materialization(dataset)?;
        Ok(ChampionBaselineRef {
            training_dataset_id: dataset.training_dataset_id,
            purpose: dataset.purpose,
            dataset_hash: *materialization.dataset_hash,
            manifest_hash: *materialization.manifest_hash,
            artifact_bytes_hash: *materialization.artifact_bytes_hash,
            parquet_uri: materialization.parquet_uri.clone(),
            feature_schema_hash: *materialization.feature_schema_hash,
            label_schema_hash: *materialization.label_schema_hash,
            window_start: dataset.window_start,
            window_end: dataset.window_end,
            pit_cutoff: dataset.pit_cutoff,
            sample_count: u64::try_from(materialization.sample_count).map_err(|error| {
                contract(format!(
                    "champion sample count is not a positive u64: {error}"
                ))
            })?,
        })
    }

    fn coverage_artifact(
        cycle: FeedbackCycleInfo,
        preimage: &VerifiedModelServingPreimage,
        evaluation_window: FeedbackCohortWindow,
        champion_baseline: ChampionBaselineRef,
        frozen: FeedbackCoverageMaterialization,
    ) -> QuantResult<FeedbackCoverageArtifact> {
        let policy = preimage.profile().spec.feedback_policy.clone();
        let gate_input = CoverageGateInput {
            policy_evaluation_count: frozen.cohorts.policy_evaluation.eligible_count(),
            mature_label_count: frozen.cohorts.model_learning.eligible_count(),
            new_mature_label_count: frozen.new_mature_label_count,
            minimum_mature_labels: policy.minimum_mature_labels,
            minimum_new_mature_labels: policy.minimum_new_mature_labels,
            minimum_coverage: policy.minimum_coverage,
        };
        let artifact = FeedbackCoverageArtifact {
            format_version: FEEDBACK_COVERAGE_ARTIFACT_FORMAT_VERSION,
            artifact_id: FeedbackCoverageArtifactId::from_cycle_id(cycle.feedback_cycle_id),
            feedback_cycle_id: cycle.feedback_cycle_id,
            cycle_idempotency_hash: cycle.idempotency_hash,
            cycle_key: cycle.idempotency_key,
            profile_ref: cycle.profile_ref,
            feedback_policy: policy,
            feedback_policy_hash: cycle.feedback_policy_hash,
            capability_registry_hashes: preimage
                .training_dataset()
                .source_lineage
                .capability_registry_hashes
                .clone(),
            champion_model_version_id: cycle.champion_model_version_id,
            champion_serving_contract_hash: cycle.champion_serving_contract_hash,
            evaluation_window,
            champion_baseline,
            cohorts: frozen.cohorts,
            mature_labels: frozen.mature_labels,
            new_mature_label_count: frozen.new_mature_label_count,
            gate_input,
            gate_outcome: gate_input.evaluate()?,
            champion_rows: frozen.champion_rows,
            champion_examples: frozen.champion_examples,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    async fn persist_coverage(
        &self,
        artifact: &FeedbackCoverageArtifact,
    ) -> QuantResult<ResearchJobArtifactRef> {
        let bytes = FeedbackCoverageCodec::encode(artifact)?;
        let content_hash = FeedbackCoverageCodec::bytes_hash(&bytes);
        let key = ArtifactKey::new(
            ArtifactNamespace::FeedbackCoverage,
            artifact.artifact_id.to_string(),
            "json",
        )?;
        let uri = self.artifact_store.put(key, &bytes).await?;
        let persisted = self.artifact_store.get(&uri).await?;
        if FeedbackCoverageCodec::bytes_hash(&persisted) != content_hash
            || FeedbackCoverageCodec::decode(&persisted)? != *artifact
        {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: content_hash.to_string(),
                actual: FeedbackCoverageCodec::bytes_hash(&persisted).to_string(),
            }
            .into());
        }
        Ok(ResearchJobArtifactRef { uri, content_hash })
    }

    async fn load_coverage(
        &self,
        params: &FeedbackDriftJobParams,
    ) -> QuantResult<FeedbackCoverageArtifact> {
        let bytes = self
            .artifact_store
            .get(&params.coverage_artifact_uri)
            .await?;
        let hash = FeedbackCoverageCodec::bytes_hash(&bytes);
        if hash != params.coverage_artifact_hash {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: params.coverage_artifact_hash.to_string(),
                actual: hash.to_string(),
            }
            .into());
        }
        let artifact = FeedbackCoverageCodec::decode(&bytes)?;
        let coverage_id_matches = artifact.artifact_id == params.coverage_artifact_id;
        if artifact.feedback_cycle_id != params.feedback_cycle_id
            || artifact.cycle_idempotency_hash != params.cycle_idempotency_hash
            || !coverage_id_matches
        {
            return Err(contract(
                "drift job coverage reference differs from the decoded artifact",
            ));
        }
        Ok(artifact)
    }

    async fn persist_drift(
        &self,
        artifact: &FeedbackDriftArtifact,
    ) -> QuantResult<ResearchJobArtifactRef> {
        let bytes = FeedbackDriftCodec::encode(artifact)?;
        let content_hash = FeedbackDriftCodec::bytes_hash(&bytes);
        let key = ArtifactKey::new(
            ArtifactNamespace::FeedbackDrift,
            artifact.artifact_id.to_string(),
            "json",
        )?;
        let uri = self.artifact_store.put(key, &bytes).await?;
        let persisted = self.artifact_store.get(&uri).await?;
        if FeedbackDriftCodec::bytes_hash(&persisted) != content_hash
            || FeedbackDriftCodec::decode(&persisted)? != *artifact
        {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: content_hash.to_string(),
                actual: FeedbackDriftCodec::bytes_hash(&persisted).to_string(),
            }
            .into());
        }
        Ok(ResearchJobArtifactRef { uri, content_hash })
    }
}

#[async_trait]
impl FeedbackCoverageExecutionPort for FeedbackSignalService {
    async fn execute(
        &self,
        params: FeedbackCoverageJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackCoverageExecutionResult> {
        require_running(&cancel, "coverage start")?;
        let cycle = self
            .load_cycle(params.feedback_cycle_id, params.cycle_idempotency_hash)
            .await?;
        if params.artifact_id != FeedbackCoverageArtifactId::from_cycle_id(cycle.feedback_cycle_id)
        {
            return Err(contract(
                "coverage artifact id differs from the exact cycle",
            ));
        }
        let preimage = self.load_champion(&cycle).await?;
        let evaluation_window = Self::evaluation_window(&cycle, &preimage)?;
        let champion_baseline = Self::baseline_ref(&preimage)?;
        progress.report(ResearchJobProgress::indeterminate(
            "feedback-coverage-freeze",
            0,
        ));
        let frozen = self
            .materializer
            .freeze_coverage(
                &evaluation_window,
                cycle.champion_model_version_id,
                cycle.label_cutoff,
                champion_baseline.pit_cutoff,
                &progress,
                &cancel,
            )
            .await?;
        require_running(&cancel, "coverage artifact seal")?;
        let artifact = Self::coverage_artifact(
            cycle,
            &preimage,
            evaluation_window,
            champion_baseline,
            frozen,
        )?;
        let result = self.persist_coverage(&artifact).await?;
        progress.report(ResearchJobProgress::with_total(
            "feedback-coverage-complete",
            1,
            1,
        ));
        Ok(FeedbackCoverageExecutionResult {
            artifact_id: artifact.artifact_id,
            artifact: result,
        })
    }
}

#[async_trait]
impl FeedbackDriftExecutionPort for FeedbackSignalService {
    async fn execute(
        &self,
        params: FeedbackDriftJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackDriftExecutionResult> {
        require_running(&cancel, "drift start")?;
        if params.artifact_id != FeedbackDriftArtifactId::from_cycle_id(params.feedback_cycle_id) {
            return Err(contract("drift artifact id differs from the exact cycle"));
        }
        let cycle = self
            .load_cycle(params.feedback_cycle_id, params.cycle_idempotency_hash)
            .await?;
        let coverage = self.load_coverage(&params).await?;
        if !matches!(coverage.gate_outcome, CoverageGateOutcome::Advance { .. }) {
            return Err(ResearchError::NotEligible {
                code: "feedback_coverage_no_action",
                detail: "statistical drift cannot run after a terminal coverage NoAction"
                    .to_owned(),
            }
            .into());
        }
        let preimage = self.load_champion(&cycle).await?;
        validate_coverage(&coverage, &cycle, &preimage)?;
        progress.report(ResearchJobProgress::indeterminate(
            "feedback-drift-baseline",
            0,
        ));
        let dataset = preimage.training_dataset();
        let materialization = require_dataset_materialization(dataset)?;
        let bytes = self.artifact_store.get(materialization.parquet_uri).await?;
        let baseline_examples = verify_frozen_dataset_artifact(dataset, &bytes)?;
        require_running(&cancel, "drift computation")?;
        let artifact =
            if coverage.champion_baseline.window_end > coverage.evaluation_window.window_start() {
                overlapping_drift(&params, &coverage)
            } else {
                compute_drift(
                    &params,
                    &coverage,
                    preimage.buy_runtime()?,
                    &baseline_examples,
                )
                .await
            }?;
        require_running(&cancel, "drift artifact seal")?;
        let result = self.persist_drift(&artifact).await?;
        progress.report(ResearchJobProgress::with_total(
            "feedback-drift-complete",
            1,
            1,
        ));
        Ok(FeedbackDriftExecutionResult {
            artifact_id: artifact.artifact_id,
            artifact: result,
        })
    }
}

fn validate_coverage(
    coverage: &FeedbackCoverageArtifact,
    cycle: &FeedbackCycleInfo,
    preimage: &VerifiedModelServingPreimage,
) -> QuantResult<()> {
    let cycle_hash_matches = coverage.cycle_idempotency_hash == cycle.idempotency_hash;
    let baseline_dataset_matches = coverage.champion_baseline.training_dataset_id
        == preimage.training_dataset().training_dataset_id;
    let valid = coverage.feedback_cycle_id == cycle.feedback_cycle_id
        && cycle_hash_matches
        && coverage.profile_ref == cycle.profile_ref
        && coverage.feedback_policy_hash == cycle.feedback_policy_hash
        && coverage.capability_registry_hashes
            == preimage
                .training_dataset()
                .source_lineage
                .capability_registry_hashes
        && coverage.champion_model_version_id == cycle.champion_model_version_id
        && coverage.champion_serving_contract_hash == cycle.champion_serving_contract_hash
        && baseline_dataset_matches;
    if !valid {
        return Err(contract(
            "coverage artifact differs from the current exact cycle preimage",
        ));
    }
    Ok(())
}

async fn compute_drift(
    params: &FeedbackDriftJobParams,
    coverage: &FeedbackCoverageArtifact,
    runtime: Arc<dyn QuantModelRuntime>,
    baseline_examples: &[TrainingExample],
) -> QuantResult<FeedbackDriftArtifact> {
    let data_details = feature_details(baseline_examples, &coverage.champion_examples)?;
    let (baseline_scores, baseline_labels) = score_examples(
        runtime.as_ref(),
        coverage.feedback_cycle_id,
        "baseline",
        baseline_examples,
    )
    .await?;
    let (evaluation_scores, evaluation_labels) = score_examples(
        runtime.as_ref(),
        coverage.feedback_cycle_id,
        "evaluation",
        &coverage.champion_examples,
    )
    .await?;
    let rank_summary = rank_ic_drift(
        &baseline_scores,
        &baseline_labels,
        &evaluation_scores,
        &evaluation_labels,
    )?;
    let concept_detail = ConceptDriftDetail {
        baseline_scored_count: exact_count("baseline champion scores", baseline_scores.len())?,
        evaluation_scored_count: exact_count(
            "evaluation champion scores",
            evaluation_scores.len(),
        )?,
        summary: rank_summary,
    };
    let baseline_counts = payout_histogram(baseline_examples)?;
    let evaluation_counts = payout_histogram(&coverage.champion_examples)?;
    let label_detail = LabelDriftDetail {
        divergence: jensen_shannon(&baseline_counts, &evaluation_counts)?,
        baseline_counts,
        evaluation_counts,
    };
    let observations = drift_observations(
        &coverage.feedback_policy,
        &data_details,
        &concept_detail,
        &label_detail,
    )?;
    drift_artifact(
        params,
        coverage,
        Some(coverage.evaluation_window.window_start()),
        data_details,
        concept_detail,
        label_detail,
        observations,
    )
}

fn overlapping_drift(
    params: &FeedbackDriftJobParams,
    coverage: &FeedbackCoverageArtifact,
) -> QuantResult<FeedbackDriftArtifact> {
    let policy = &coverage.feedback_policy;
    let observations = vec![
        DriftObservation::try_new(
            FeedbackDriftMetric::PopulationStabilityIndex,
            None,
            policy.data_drift_psi_threshold,
            0,
        )?,
        DriftObservation::try_new(
            FeedbackDriftMetric::KolmogorovSmirnovPValue,
            None,
            policy.data_drift_ks_p_value,
            0,
        )?,
        DriftObservation::try_new(
            FeedbackDriftMetric::RankIcDrop,
            None,
            policy.concept_rank_ic_drop,
            0,
        )?,
        DriftObservation::try_new(
            FeedbackDriftMetric::JensenShannonDivergence,
            None,
            policy.label_js_divergence,
            0,
        )?,
    ];
    drift_artifact(
        params,
        coverage,
        None,
        Vec::new(),
        ConceptDriftDetail {
            baseline_scored_count: 0,
            evaluation_scored_count: 0,
            summary: None,
        },
        LabelDriftDetail {
            baseline_counts: vec![0; PAYOUT_HISTOGRAM_BIN_COUNT],
            evaluation_counts: vec![0; PAYOUT_HISTOGRAM_BIN_COUNT],
            divergence: None,
        },
        observations,
    )
}

fn drift_artifact(
    params: &FeedbackDriftJobParams,
    coverage: &FeedbackCoverageArtifact,
    comparison_window_start: Option<DateTime<Utc>>,
    data_details: Vec<FeatureDriftDetail>,
    concept_detail: ConceptDriftDetail,
    label_detail: LabelDriftDetail,
    observations: Vec<DriftObservation>,
) -> QuantResult<FeedbackDriftArtifact> {
    let artifact = FeedbackDriftArtifact {
        format_version: FEEDBACK_DRIFT_ARTIFACT_FORMAT_VERSION,
        artifact_id: params.artifact_id,
        feedback_cycle_id: params.feedback_cycle_id,
        cycle_idempotency_hash: params.cycle_idempotency_hash,
        coverage_artifact_id: params.coverage_artifact_id,
        coverage_artifact_uri: params.coverage_artifact_uri.clone(),
        coverage_artifact_hash: params.coverage_artifact_hash,
        profile_ref: coverage.profile_ref.clone(),
        feedback_policy: coverage.feedback_policy.clone(),
        feedback_policy_hash: coverage.feedback_policy_hash,
        champion_model_version_id: coverage.champion_model_version_id,
        champion_serving_contract_hash: coverage.champion_serving_contract_hash,
        champion_baseline: coverage.champion_baseline.clone(),
        evaluation_window: coverage.evaluation_window.clone(),
        comparison_window_start,
        gate_outcome: drift_gate(&observations),
        data_details,
        concept_detail,
        label_detail,
        observations,
        observed_at: coverage.cycle_key.label_cutoff(),
    };
    artifact.validate()?;
    Ok(artifact)
}

fn feature_details(
    baseline: &[TrainingExample],
    evaluation: &[TrainingExample],
) -> QuantResult<Vec<FeatureDriftDetail>> {
    let mut names = BTreeSet::new();
    for example in baseline.iter().chain(evaluation) {
        names.extend(
            example
                .feature_vector
                .iter_cells()
                .map(|(name, _)| name.clone()),
        );
    }
    names
        .into_iter()
        .map(|name| {
            let baseline = baseline
                .iter()
                .map(|example| example.feature_vector.value(&name).cloned())
                .collect::<Vec<_>>();
            let evaluation = evaluation
                .iter()
                .map(|example| example.feature_vector.value(&name).cloned())
                .collect::<Vec<_>>();
            FeatureDriftDetail::compute(name, &baseline, &evaluation)
        })
        .collect()
}

async fn score_examples(
    runtime: &dyn QuantModelRuntime,
    feedback_cycle_id: FeedbackCycleId,
    population: &'static str,
    examples: &[TrainingExample],
) -> QuantResult<(Vec<Decimal>, Vec<Decimal>)> {
    let mut groups = BTreeMap::<DateTime<Utc>, Vec<&TrainingExample>>::new();
    for example in examples {
        groups
            .entry(example.decision_at())
            .or_default()
            .push(example);
    }
    let mut scores = Vec::new();
    let mut labels = Vec::new();
    for (decision_at, group) in groups {
        let run_id = drift_run_id(feedback_cycle_id, population, decision_at)?;
        let input = build_frozen_runtime_input(runtime, &run_id, &group)?;
        let output = runtime.infer_batch(input).await?;
        let mut candidates = HashMap::new();
        for candidate in output.candidates {
            if candidate.model_run_id != run_id
                || candidates
                    .insert(candidate.market_id.clone(), candidate)
                    .is_some()
            {
                return Err(contract(
                    "champion drift inference emitted duplicate or foreign candidates",
                ));
            }
        }
        for example in group {
            let Some(candidate) = candidates.remove(&example.market_id) else {
                continue;
            };
            let score = if candidate.token_id == example.token_id {
                candidate.composite_score.inner()
            } else if example.selected_market.secondary_token_id.as_ref()
                == Some(&candidate.token_id)
            {
                Decimal::ONE - candidate.composite_score.inner()
            } else {
                return Err(contract(
                    "champion drift candidate targets neither frozen outcome token",
                ));
            };
            scores.push(score);
            labels.push(payout_label(example)?);
        }
        if !candidates.is_empty() {
            return Err(contract(
                "champion drift inference emitted a market outside its frozen batch",
            ));
        }
    }
    Ok((scores, labels))
}

fn payout_label(example: &TrainingExample) -> QuantResult<Decimal> {
    let labels = example
        .labels
        .iter()
        .filter(|label| label.label_name == TOKEN_PAYOUT_RATIO && label.horizon_secs == 0)
        .collect::<Vec<_>>();
    let label = labels
        .first()
        .ok_or_else(|| contract("frozen example has no token payout label"))?;
    if labels.len() != 1
        || !label.is_resolved
        || label.value < Decimal::ZERO
        || label.value > Decimal::ONE
    {
        return Err(contract(
            "frozen token payout label is duplicate, unresolved, or out of range",
        ));
    }
    Ok(label.value)
}

fn payout_histogram(examples: &[TrainingExample]) -> QuantResult<Vec<u64>> {
    let mut counts = vec![0_u64; PAYOUT_HISTOGRAM_BIN_COUNT];
    for example in examples {
        let value = payout_label(example)?;
        let mut index = 0_usize;
        while index < PAYOUT_HISTOGRAM_BIN_COUNT - 1
            && value
                >= Decimal::from(u64::try_from(index + 1).map_err(|error| {
                    contract(format!("payout bin index conversion failed: {error}"))
                })?) / Decimal::TEN
        {
            index += 1;
        }
        let count = counts
            .get_mut(index)
            .ok_or_else(|| contract("payout label resolved outside fixed histogram"))?;
        *count = count
            .checked_add(1)
            .ok_or_else(|| contract("payout histogram count overflowed"))?;
    }
    Ok(counts)
}

fn drift_run_id(
    feedback_cycle_id: FeedbackCycleId,
    population: &'static str,
    decision_at: DateTime<Utc>,
) -> QuantResult<ModelRunId> {
    let bytes =
        CanonicalDigest::canonical_json_bytes(&(feedback_cycle_id, population, decision_at))?;
    Ok(ModelRunId::new(Uuid::new_v5(&DRIFT_RUN_NAMESPACE, &bytes)))
}

fn exact_count(field: &'static str, count: usize) -> QuantResult<u64> {
    u64::try_from(count)
        .map_err(|error| contract(format!("{field} count cannot be represented: {error}")))
}

fn require_running(cancel: &CancellationToken, phase: &'static str) -> QuantResult<()> {
    if cancel.is_cancelled() {
        return Err(ResearchError::Cancelled {
            detail: format!("feedback {phase} cancelled"),
        }
        .into());
    }
    Ok(())
}

fn contract(detail: impl Into<String>) -> QuantError {
    ResearchError::ValidationMethodology {
        detail: detail.into(),
    }
    .into()
}
