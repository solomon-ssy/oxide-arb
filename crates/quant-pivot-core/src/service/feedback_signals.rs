//! Durable feedback coverage and statistical-drift execution.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_compute::{ComputeExecutor, OFFLINE_MEMORY_BYTES, OfflineMemory};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::{FeedbackCoverageJobParams, FeedbackDriftJobParams},
        ports::{
            FeedbackCoverageExecutionPort, FeedbackCoverageExecutionResult,
            FeedbackDriftExecutionPort, FeedbackDriftExecutionResult,
        },
        quant::{
            FeedbackCohortWindow, FeedbackCycleInfo, JobProgressSink, ResearchJobArtifactRef,
            TrainingDatasetInfo,
        },
    },
    enums::quant::{FeedbackCycleStatus, FeedbackDriftMetric},
    hashing::CanonicalDigest,
    types::{
        ContentHash, FeedbackCoverageArtifactId, FeedbackCycleId, FeedbackDriftArtifactId,
        ModelRunId, ResearchFeedbackPolicy, ResearchJobProgress, ResearchProfileArtifact,
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
        drift_gate, drift_observations, jensen_shannon, target_rank_ic_drift,
    },
    model::QuantModelRuntime,
    training::{TOKEN_PAYOUT_RATIO, TrainingExample},
};
use rust_decimal::Decimal;
use tokio::runtime::Handle;
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
        model_serving_preimage::{
            ModelPreimageReadContext, ModelServingPreimageService, VerifiedModelServingPreimage,
        },
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
    pub compute: Arc<ComputeExecutor>,
}

/// Executes coverage and drift through verified immutable preimages only.
pub struct FeedbackSignalService {
    cycles: Arc<dyn FeedbackCycleRepository>,
    models: Arc<dyn ModelRegistryRepository>,
    policies: Arc<dyn PolicyRepository>,
    preimages: Arc<ModelServingPreimageService>,
    materializer: FeedbackCohortMaterializer,
    artifact_store: Arc<dyn ArtifactStore>,
    compute: Arc<ComputeExecutor>,
    compute_memory: OfflineMemory,
}

impl FeedbackSignalService {
    pub fn try_new(deps: FeedbackSignalServiceDeps) -> QuantResult<Self> {
        Ok(Self {
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
            compute: deps.compute,
            compute_memory: OfflineMemory::try_bytes(OFFLINE_MEMORY_BYTES)?,
        })
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
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<VerifiedModelServingPreimage> {
        let version = self
            .models
            .find_model_version(&cycle.champion_model_version_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found("quant_model_version", cycle.champion_model_version_id)
            })?;
        let preimage = self.preimages.load(&version, context).await?;
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
        profile: &ResearchProfileArtifact,
    ) -> QuantResult<FeedbackCohortWindow> {
        let plan = FeedbackCycleFreezePlan::derive_at_cutoff(
            profile,
            cycle.champion_model_spec_id,
            cycle.champion_model_spec_definition_hash,
            cycle.decision_policy_snapshot_id,
            cycle.decision_policy_snapshot_hash,
            cycle.label_cutoff,
        )?;
        Ok(plan.evaluation().clone())
    }

    fn baseline_ref(dataset: &TrainingDatasetInfo) -> QuantResult<ChampionBaselineRef> {
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
        profile: &ResearchProfileArtifact,
        dataset: &TrainingDatasetInfo,
        evaluation_window: FeedbackCohortWindow,
        champion_baseline: ChampionBaselineRef,
        frozen: FeedbackCoverageMaterialization,
    ) -> QuantResult<FeedbackCoverageArtifact> {
        let policy = profile.spec.feedback_policy.clone();
        let gate_input = coverage_gate_input(&policy, &frozen);
        let artifact = FeedbackCoverageArtifact {
            format_version: FEEDBACK_COVERAGE_ARTIFACT_FORMAT_VERSION,
            artifact_id: FeedbackCoverageArtifactId::from_cycle_id(cycle.feedback_cycle_id),
            feedback_cycle_id: cycle.feedback_cycle_id,
            cycle_idempotency_hash: cycle.idempotency_hash,
            cycle_key: cycle.idempotency_key,
            profile_ref: cycle.profile_ref,
            feedback_policy: policy,
            feedback_policy_hash: cycle.feedback_policy_hash,
            capability_registry_hashes: dataset.source_lineage.capability_registry_hashes.clone(),
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
        artifact: FeedbackCoverageArtifact,
        cancel: &CancellationToken,
    ) -> QuantResult<ResearchJobArtifactRef> {
        let artifact_id = artifact.artifact_id;
        let (artifact, bytes, content_hash) = self
            .run_compute(cancel, move || {
                let bytes = FeedbackCoverageCodec::encode(&artifact)?;
                let content_hash = FeedbackCoverageCodec::bytes_hash(&bytes);
                Ok((artifact, bytes, content_hash))
            })
            .await?;
        let key = ArtifactKey::new(
            ArtifactNamespace::FeedbackCoverage,
            artifact_id.to_string(),
            "json",
        )?;
        require_running(cancel, "coverage artifact commit")?;
        let uri = self.artifact_store.put(key, &bytes).await?;
        let persisted = self.artifact_store.get(&uri).await?;
        self.run_finalize(move || {
            verify_readback(
                &persisted,
                content_hash,
                &artifact,
                FeedbackCoverageCodec::bytes_hash,
                FeedbackCoverageCodec::decode,
            )
        })
        .await?;
        Ok(ResearchJobArtifactRef { uri, content_hash })
    }

    async fn load_coverage(
        &self,
        params: &FeedbackDriftJobParams,
        cancel: &CancellationToken,
    ) -> QuantResult<FeedbackCoverageArtifact> {
        let bytes = self
            .artifact_store
            .get(&params.coverage_artifact_uri)
            .await?;
        let expected_hash = params.coverage_artifact_hash;
        let artifact_id = params.coverage_artifact_id;
        let feedback_cycle_id = params.feedback_cycle_id;
        let cycle_idempotency_hash = params.cycle_idempotency_hash;
        self.run_compute(cancel, move || {
            let hash = FeedbackCoverageCodec::bytes_hash(&bytes);
            if hash != expected_hash {
                return Err(ResearchError::ArtifactHashMismatch {
                    expected: expected_hash.to_string(),
                    actual: hash.to_string(),
                }
                .into());
            }
            let artifact = FeedbackCoverageCodec::decode(&bytes)?;
            if artifact.artifact_id != artifact_id
                || artifact.feedback_cycle_id != feedback_cycle_id
                || artifact.cycle_idempotency_hash != cycle_idempotency_hash
            {
                return Err(contract(
                    "drift job coverage reference differs from the decoded artifact",
                ));
            }
            Ok(artifact)
        })
        .await
    }

    async fn persist_drift(
        &self,
        artifact: FeedbackDriftArtifact,
        cancel: &CancellationToken,
    ) -> QuantResult<ResearchJobArtifactRef> {
        let artifact_id = artifact.artifact_id;
        let (artifact, bytes, content_hash) = self
            .run_compute(cancel, move || {
                let bytes = FeedbackDriftCodec::encode(&artifact)?;
                let content_hash = FeedbackDriftCodec::bytes_hash(&bytes);
                Ok((artifact, bytes, content_hash))
            })
            .await?;
        let key = ArtifactKey::new(
            ArtifactNamespace::FeedbackDrift,
            artifact_id.to_string(),
            "json",
        )?;
        require_running(cancel, "drift artifact commit")?;
        let uri = self.artifact_store.put(key, &bytes).await?;
        let persisted = self.artifact_store.get(&uri).await?;
        self.run_finalize(move || {
            verify_readback(
                &persisted,
                content_hash,
                &artifact,
                FeedbackDriftCodec::bytes_hash,
                FeedbackDriftCodec::decode,
            )
        })
        .await?;
        Ok(ResearchJobArtifactRef { uri, content_hash })
    }

    async fn run_compute<T, F>(&self, cancel: &CancellationToken, work: F) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        self.compute
            .run_offline_cancellable(self.compute_memory, cancel, work)
            .await
    }

    async fn run_finalize<T, F>(&self, work: F) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        self.compute.run_offline(self.compute_memory, work).await
    }
}

const fn coverage_gate_input(
    policy: &ResearchFeedbackPolicy,
    frozen: &FeedbackCoverageMaterialization,
) -> CoverageGateInput {
    CoverageGateInput {
        model_learning_candidate_count: frozen.cohorts.model_learning.candidate_count(),
        mature_label_count: frozen.cohorts.model_learning.eligible_count(),
        new_mature_label_count: frozen.new_mature_label_count,
        minimum_mature_labels: policy.minimum_mature_labels,
        minimum_new_mature_labels: policy.minimum_new_mature_labels,
        minimum_coverage: policy.minimum_coverage,
    }
}

fn dispatch_drift<T>(
    overlaps: bool,
    overlap: impl FnOnce() -> QuantResult<T>,
    non_overlap: impl FnOnce() -> QuantResult<T>,
) -> QuantResult<T> {
    if overlaps { overlap() } else { non_overlap() }
}

fn verify_readback<T>(
    persisted: &[u8],
    expected_hash: ContentHash,
    expected: &T,
    hash: impl FnOnce(&[u8]) -> ContentHash,
    decode: impl FnOnce(&[u8]) -> QuantResult<T>,
) -> QuantResult<()>
where
    T: PartialEq,
{
    let actual_hash = hash(persisted);
    if actual_hash != expected_hash || decode(persisted)? != *expected {
        return Err(ResearchError::ArtifactHashMismatch {
            expected: expected_hash.to_string(),
            actual: actual_hash.to_string(),
        }
        .into());
    }
    Ok(())
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
        let context = ModelPreimageReadContext::new(&cancel, None);
        let preimage = self.load_champion(&cycle, &context).await?;
        drop(context);
        let profile = preimage.profile().clone();
        let dataset = preimage.training_dataset().clone();
        let planning_cycle = cycle.clone();
        let planning_profile = profile.clone();
        let planning_dataset = dataset.clone();
        let (evaluation_window, champion_baseline) =
            Box::pin(self.run_compute(&cancel, move || {
                Ok((
                    Self::evaluation_window(&planning_cycle, &planning_profile)?,
                    Self::baseline_ref(&planning_dataset)?,
                ))
            }))
            .await?;
        progress.report(ResearchJobProgress::indeterminate(
            "feedback-coverage-freeze",
            0,
        ));
        let frozen = Box::pin(self.materializer.freeze_coverage(
            &evaluation_window,
            cycle.champion_model_version_id,
            cycle.label_cutoff,
            champion_baseline.pit_cutoff,
            &progress,
            &cancel,
        ))
        .await?;
        require_running(&cancel, "coverage artifact seal")?;
        let artifact = Box::pin(self.run_compute(&cancel, move || {
            Self::coverage_artifact(
                cycle,
                &profile,
                &dataset,
                evaluation_window,
                champion_baseline,
                frozen,
            )
        }))
        .await?;
        let artifact_id = artifact.artifact_id;
        let result = self.persist_coverage(artifact, &cancel).await?;
        progress.report(ResearchJobProgress::with_total(
            "feedback-coverage-complete",
            1,
            1,
        ));
        Ok(FeedbackCoverageExecutionResult {
            artifact_id,
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
        let coverage = self.load_coverage(&params, &cancel).await?;
        if !matches!(coverage.gate_outcome, CoverageGateOutcome::Advance { .. }) {
            return Err(ResearchError::NotEligible {
                code: "feedback_coverage_no_action",
                detail: "statistical drift cannot run after a terminal coverage NoAction"
                    .to_owned(),
            }
            .into());
        }
        let context = ModelPreimageReadContext::new(&cancel, None);
        let preimage = self.load_champion(&cycle, &context).await?;
        drop(context);
        validate_coverage(&coverage, &cycle, &preimage)?;
        progress.report(ResearchJobProgress::indeterminate(
            "feedback-drift-baseline",
            0,
        ));
        let dataset = preimage.training_dataset();
        let materialization = require_dataset_materialization(dataset)?;
        let bytes = self.artifact_store.get(materialization.parquet_uri).await?;
        let dataset = dataset.clone();
        let baseline_examples = self
            .run_compute(&cancel, move || {
                verify_frozen_dataset_artifact(&dataset, &bytes)
            })
            .await?;
        require_running(&cancel, "drift computation")?;
        let runtime = preimage.buy_runtime()?;
        let compute_runtime = Handle::current();
        let compute_cancel = cancel.clone();
        let artifact = self
            .run_compute(&cancel, move || {
                require_running(&compute_cancel, "drift computation")?;
                let overlaps = coverage.champion_baseline.window_end
                    > coverage.evaluation_window.window_start();
                dispatch_drift(
                    overlaps,
                    || overlapping_drift(&params, &coverage),
                    || {
                        compute_runtime.block_on(Box::pin(compute_drift(
                            &params,
                            &coverage,
                            runtime,
                            &baseline_examples,
                            &compute_cancel,
                        )))
                    },
                )
            })
            .await?;
        require_running(&cancel, "drift artifact seal")?;
        let artifact_id = artifact.artifact_id;
        let result = self.persist_drift(artifact, &cancel).await?;
        progress.report(ResearchJobProgress::with_total(
            "feedback-drift-complete",
            1,
            1,
        ));
        Ok(FeedbackDriftExecutionResult {
            artifact_id,
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
    cancel: &CancellationToken,
) -> QuantResult<FeedbackDriftArtifact> {
    require_running(cancel, "drift feature scan")?;
    let data_details = feature_details(baseline_examples, &coverage.champion_examples, cancel)?;
    let (baseline_scores, baseline_labels) = score_examples(
        runtime.as_ref(),
        coverage.feedback_cycle_id,
        "baseline",
        baseline_examples,
        cancel,
    )
    .await?;
    let (evaluation_scores, evaluation_labels) = score_examples(
        runtime.as_ref(),
        coverage.feedback_cycle_id,
        "evaluation",
        &coverage.champion_examples,
        cancel,
    )
    .await?;
    let target_rank_summary = target_rank_ic_drift(
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
        summary: target_rank_summary,
    };
    require_running(cancel, "drift metrics")?;
    let baseline_counts = payout_histogram(baseline_examples, cancel)?;
    let evaluation_counts = payout_histogram(&coverage.champion_examples, cancel)?;
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
            FeedbackDriftMetric::TargetRankIcDrop,
            None,
            policy.concept_target_rank_ic_drop,
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
    cancel: &CancellationToken,
) -> QuantResult<Vec<FeatureDriftDetail>> {
    let mut names = BTreeSet::new();
    for example in baseline.iter().chain(evaluation) {
        require_running(cancel, "drift feature scan")?;
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
            require_running(cancel, "drift feature distribution")?;
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
    cancel: &CancellationToken,
) -> QuantResult<(Vec<Decimal>, Vec<Decimal>)> {
    let mut groups = BTreeMap::<DateTime<Utc>, Vec<&TrainingExample>>::new();
    for example in examples {
        require_running(cancel, "drift score grouping")?;
        groups
            .entry(example.decision_at())
            .or_default()
            .push(example);
    }
    let mut scores = Vec::new();
    let mut labels = Vec::new();
    for (decision_at, group) in groups {
        require_running(cancel, "drift inference")?;
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
            require_running(cancel, "drift score validation")?;
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

fn payout_histogram(
    examples: &[TrainingExample],
    cancel: &CancellationToken,
) -> QuantResult<Vec<u64>> {
    let mut counts = vec![0_u64; PAYOUT_HISTOGRAM_BIN_COUNT];
    for example in examples {
        require_running(cancel, "drift payout histogram")?;
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

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use async_trait::async_trait;
    use chrono::Utc;
    use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
    use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
    use quant_pivot_models::{
        enums::model::ModelFamily,
        hashing::CanonicalDigest,
        types::{
            ContentHash, DatasetCohortCounts, ModelRunId, ModelVersionId,
            builtin_research_profiles, stable_name::FeatureName,
        },
    };
    use quant_pivot_research::{
        feedback::FeedbackCoverageCohorts,
        model::{
            FactorInferenceTable, ModelRuntimeInput, ModelRuntimeMetrics, ModelRuntimeOutput,
            QuantModelRuntime,
        },
    };
    use tokio::{
        runtime::Handle,
        task,
        time::{sleep, timeout},
    };
    use tokio_util::sync::CancellationToken;

    use super::{
        FeedbackCoverageMaterialization, coverage_gate_input, dispatch_drift, require_running,
        verify_readback,
    };

    #[test]
    fn corrupt_readback_fails_closed() {
        let expected = b"canonical".to_vec();
        let expected_hash = CanonicalDigest::content_hash_bytes(&expected);
        let error = verify_readback(
            b"corrupt",
            expected_hash,
            &expected,
            CanonicalDigest::content_hash_bytes,
            |bytes| Ok(bytes.to_vec()),
        )
        .expect_err("corrupt artifact readback must fail");
        assert!(matches!(
            error,
            QuantError::Research(ResearchError::ArtifactHashMismatch { .. })
        ));
    }

    struct ThreadProbe {
        observed: Arc<Mutex<Option<String>>>,
    }

    #[async_trait]
    impl QuantModelRuntime for ThreadProbe {
        fn model_version_id(&self) -> ModelVersionId {
            ModelVersionId::from_v7()
        }
        fn model_family(&self) -> ModelFamily {
            ModelFamily::WeightedFactor
        }
        fn feature_schema_hash(&self) -> ContentHash {
            ContentHash::from_bytes([1; 32])
        }
        fn required_features(&self) -> Vec<FeatureName> {
            Vec::new()
        }

        async fn infer_batch(&self, _input: ModelRuntimeInput) -> QuantResult<ModelRuntimeOutput> {
            *self.observed.lock().expect("thread probe lock") =
                thread::current().name().map(str::to_owned);
            Ok(ModelRuntimeOutput {
                calibration_scores: Vec::new(),
                rank_scores: Vec::new(),
                candidates: Vec::new(),
                runtime_metrics: ModelRuntimeMetrics {
                    markets_scored: 0,
                    candidates_emitted: 0,
                    inference_duration_ms: 0,
                },
                input_audit: Vec::new(),
            })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn drift_dispatch_isolates_inference() {
        let compute = Arc::new(ComputeExecutor::new().expect("compute executor"));
        let observed = Arc::new(Mutex::new(None));
        let runtime = Arc::new(ThreadProbe {
            observed: Arc::clone(&observed),
        });
        let handle = Handle::current();
        let work = task::spawn({
            let compute = Arc::clone(&compute);
            async move {
                compute
                    .run_offline(
                        OfflineMemory::try_gib(1).expect("memory budget"),
                        move || {
                            dispatch_drift(
                                false,
                                || panic!("non-overlap must not use overlap path"),
                                || {
                                    handle.block_on(runtime.infer_batch(
                                        ModelRuntimeInput::FactorTable(FactorInferenceTable {
                                            model_run_id: ModelRunId::from_v7(),
                                            decision_at: Utc::now(),
                                            rows: Vec::new(),
                                        }),
                                    ))?;
                                    Ok(())
                                },
                            )
                        },
                    )
                    .await
            }
        });
        timeout(Duration::from_millis(50), sleep(Duration::from_millis(10)))
            .await
            .expect("heartbeat advances");
        work.await
            .expect("drift probe joins")
            .expect("drift probe succeeds");
        assert!(
            observed
                .lock()
                .expect("thread probe lock")
                .as_deref()
                .is_some_and(|name| name.starts_with("quant-offline-"))
        );
    }

    #[test]
    fn overlap_skips_inference() {
        let inferred = Arc::new(Mutex::new(false));
        let probe = Arc::clone(&inferred);
        dispatch_drift(
            true,
            || Ok(()),
            || {
                *probe.lock().expect("overlap probe lock") = true;
                Ok(())
            },
        )
        .expect("overlap succeeds");
        assert!(!*inferred.lock().expect("overlap probe lock"));
    }

    #[test]
    fn coverage_uses_learning_denominator() {
        let profile = builtin_research_profiles()
            .expect("builtin research profiles")
            .into_iter()
            .next()
            .expect("builtin research profile");
        let counts = |count| {
            DatasetCohortCounts::try_new(count, count, count, Vec::new(), Vec::new())
                .expect("valid cohort counts")
        };
        let frozen = FeedbackCoverageMaterialization {
            cohorts: FeedbackCoverageCohorts {
                model_learning: counts(40),
                execution_learning: counts(20),
                policy_evaluation: counts(10),
            },
            mature_labels: Vec::new(),
            new_mature_label_count: 5,
            champion_rows: Vec::new(),
            champion_examples: Vec::new(),
        };
        let input = coverage_gate_input(&profile.spec.feedback_policy, &frozen);
        assert_eq!(input.model_learning_candidate_count, 40);
        assert_eq!(input.mature_label_count, 40);
        assert_ne!(
            input.model_learning_candidate_count,
            frozen.cohorts.policy_evaluation.eligible_count()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn offline_compute_preserves_heartbeat() {
        let compute = Arc::new(ComputeExecutor::new().expect("compute executor"));
        let worker = Arc::clone(&compute);
        let cancel = CancellationToken::new();
        let work_cancel = cancel.clone();
        let work = task::spawn(async move {
            worker
                .run_offline_cancellable(
                    OfflineMemory::try_gib(1).expect("memory budget"),
                    &work_cancel,
                    move || {
                        thread::sleep(Duration::from_millis(100));
                        Ok(())
                    },
                )
                .await
        });

        timeout(Duration::from_millis(50), sleep(Duration::from_millis(10)))
            .await
            .expect("Tokio heartbeat must not wait for offline CPU");
        work.await
            .expect("offline task joins")
            .expect("offline work succeeds");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn offline_compute_observes_cancel() {
        let compute = Arc::new(ComputeExecutor::new().expect("compute executor"));
        let cancel = CancellationToken::new();
        let work_cancel = cancel.clone();
        let closure_cancel = cancel.clone();
        let worker = task::spawn(async move {
            compute
                .run_offline_cancellable(
                    OfflineMemory::try_gib(1).expect("memory budget"),
                    &work_cancel,
                    move || -> QuantResult<()> {
                        loop {
                            require_running(&closure_cancel, "drift test kernel")?;
                            thread::sleep(Duration::from_millis(1));
                        }
                    },
                )
                .await
        });
        task::yield_now().await;
        cancel.cancel();

        let error = timeout(Duration::from_secs(1), worker)
            .await
            .expect("cooperative kernel stops")
            .expect("offline task joins")
            .expect_err("cancelled kernel fails closed");
        assert!(error.to_string().contains("cancelled"));
    }
}
