//! Trade-policy artifact fitting, catalog reads, and governed transitions.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    iter, mem,
    sync::Arc,
    time::Duration as StdDuration,
};

use async_trait::async_trait;
use blake3::Hasher;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::ExecutionParticipantFactRow,
    config::WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS,
    domain::{
        api::{
            BuildTrainingDatasetRequest, FitTradePolicyRequest, TradePolicyAuditListQuery,
            TradePolicyEvidenceDownloadView, TradePolicyEvidenceRowListQuery,
            TradePolicyEvidenceRowView, TradePolicyFitPreflightRequest,
            TradePolicyFitPreflightView, TradePolicyFitReadiness, TradePolicyFitSelection,
            TradePolicyListQuery, TradePolicyOperationalEvidenceView,
            TradePolicyPreflightBlockerDetail, TradePolicyPreflightBlockerView,
            TradePolicySourceSliceObjectListQuery, TradePolicySourceSliceObjectView,
            TradePolicySourceSliceView, TradePolicyTrialListQuery, TradePolicyValidationListQuery,
            TradePolicyValidationRowListQuery, TrainingDatasetListQuery,
        },
        data_plane::WeatherObservationFact,
        pagination::{PageRequest, Paginated},
        ports::{PolicyFitDatasetBuildRequest, TradePolicyPort, TrainingDatasetPort},
        quant::{
            CompleteTradePolicyValidation, FailTradePolicyValidation, JobProgressSink,
            MarketLinkage, NewTradePolicyArtifact, NewTradePolicyGovernanceAudit,
            NewTradePolicyTrialAttempt, NewTradePolicyValidationRow, NewTradePolicyValidationRun,
            ResearchReadinessEvidenceInfo, SourceSliceIdentity, SourceSliceIdentityInput,
            SourceSliceInfo, TradePolicyArtifactInfo, TradePolicyGovernanceAuditInfo,
            TradePolicyTrialAttemptInfo, TradePolicyValidationRowInfo,
            TradePolicyValidationRunInfo, TrainingDatasetInfo,
        },
    },
    enums::quant::{
        DatasetPurpose, SourceSliceStatus, TradePolicyGovernanceAction, TradePolicyStatus,
        TradePolicyTrialScope, TradePolicyTrialStatus, TradePolicyValidationStatus,
        TrainingDatasetStatus,
    },
    hashing::CanonicalDigest,
    runtime_config::{BuyModelRoute, PolicyValidationConfig},
    types::{
        ArtifactUri, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION, DecisionPolicySnapshotId,
        DiagnosticCode, ExecutablePriceBasis, MarketId, ModelSpecId, ModelVersionId,
        ResearchEvaluationTrack, ResearchJobId, ResearchJobProgress, ResearchProfileArtifact,
        ResearchProfileId, ResearchReadinessEvidenceId, ResearchReadinessEvidencePayload,
        SchemaVersion, ShadowLatencyProfileV1, SourceSliceManifest, SourceSliceManifestRef,
        SourceSliceObjectKind, TRADE_POLICY_ARTIFACT_FORMAT_VERSION,
        TRADE_POLICY_EVIDENCE_BUNDLE_FORMAT_VERSION, TokenId, TradePolicyArtifactId,
        TradePolicyArtifactPayload, TradePolicyCandidateId, TradePolicyCandidateSpec,
        TradePolicyCandidateTrialRow, TradePolicyCohortTrialRow, TradePolicyCoverageGapRow,
        TradePolicyEvidenceBundleManifest, TradePolicyEvidenceBundleRef,
        TradePolicyEvidenceObjectKind, TradePolicyEvidenceObjectRef, TradePolicyExecutionEvidence,
        TradePolicyFillEvidenceRow, TradePolicyFitContract, TradePolicyGovernanceAuditId,
        TradePolicyObservationEligibilityRow, TradePolicyPitCutoffEvidence,
        TradePolicyTrialAttemptId, TradePolicyTrialMetrics, TradePolicyValidationEvidence,
        TradePolicyValidationRunId, TrainingDatasetId, TrainingExampleId, TrainingSampleSources,
        UserId, VerticalActivationTarget, builtin_research_profiles,
        canonicalize_policy_candidates,
    },
};
use quant_pivot_repository::traits::{
    ModelRegistryRepository, PolicyRepository, SourceSliceRepository, TradePolicyRepository,
    TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    execution_semantics::EXECUTION_SEMANTICS_VERSION,
    hashing::ResearchHasher,
    policy_evidence::{PolicyEvidenceParquetCodec, PolicyEvidenceRecord},
    policy_validation::POLICY_PERFORMANCE_METHODOLOGY_VERSION,
    structural_volatility::evaluate_structural_volatility_oos,
    training::{
        LIQUIDITY_EXIT_POSSIBLE, MAX_ADVERSE_EXCURSION_BPS, MAX_FAVORABLE_EXCURSION_BPS,
        TrainingExample, TrainingLabel,
    },
    weather_proxy_validation::evaluate_weather_proxy_gate,
};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

use crate::{
    prefetch::{
        replay_page::{MAX_REPLAY_PAGE_MARKETS, ReplayPageRequest},
        source_slice::{FrozenSourceSlice, SourceSliceReader},
    },
    service::{
        model_serving_preimage::ModelServingPreimageService,
        research_readiness::{ResearchReadinessEvidenceService, VerifiedOperationalEvidence},
        trade_policy_evidence::{
            TradePolicyEvidenceDurability, TradePolicyEvidenceVerifier,
            TradePolicyEvidenceVerifierDeps, WeatherExperimentFamilyInput,
            content_hash_from_hasher, weather_experiment_family_hash,
        },
        trade_policy_replay::{
            FrozenPolicySignals, PolicyStatisticalRun, WeatherEvidenceRequest,
            WeatherExampleReplay, WeatherPolicyEvidence, WeatherReplayRequest,
            evaluate_weather_policy_evidence, reinfer_frozen_policy_signals, replay_weather_page,
        },
        training_dataset::{require_dataset_materialization, verify_frozen_dataset_artifact},
    },
};

pub struct TradePolicyService {
    compute: Arc<ComputeExecutor>,
    datasets: Arc<dyn TrainingDatasetRepository>,
    dataset_builder: Arc<dyn TrainingDatasetPort>,
    artifacts: Arc<dyn ArtifactStore>,
    policies: Arc<dyn TradePolicyRepository>,
    model_registry: Arc<dyn ModelRegistryRepository>,
    runtime_configs: Arc<dyn PolicyRepository>,
    source_slices: Arc<dyn SourceSliceRepository>,
    readiness: Arc<ResearchReadinessEvidenceService>,
    evidence_verifier: TradePolicyEvidenceVerifier,
    serving_preimages: Arc<ModelServingPreimageService>,
}

pub struct TradePolicyServiceDeps {
    pub compute: Arc<ComputeExecutor>,
    pub datasets: Arc<dyn TrainingDatasetRepository>,
    pub dataset_builder: Arc<dyn TrainingDatasetPort>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub policies: Arc<dyn TradePolicyRepository>,
    pub model_registry: Arc<dyn ModelRegistryRepository>,
    pub runtime_configs: Arc<dyn PolicyRepository>,
    pub source_slices: Arc<dyn SourceSliceRepository>,
    pub readiness: Arc<ResearchReadinessEvidenceService>,
    pub serving_preimages: Arc<ModelServingPreimageService>,
}

struct RuntimePolicyLimits {
    max_candidates: u32,
    methodology_hash: ContentHash,
    runtime_config_hash: ContentHash,
    min_latency_profile_secs: u64,
    fit_model: FrozenPolicyFitModel,
}

struct FrozenPolicyFitModel {
    model_version_id: ModelVersionId,
    model_spec_id: ModelSpecId,
    feature_schema_version: SchemaVersion,
    knowledge_lag_secs: u64,
}

struct ContractPreflight {
    valid: PreflightCheck,
    profile_fitter_available: bool,
    pit_cutoff_not_future: bool,
    profile_quality_gate_available: bool,
    decision_policy_snapshot_id: Option<DecisionPolicySnapshotId>,
    runtime_limits: Option<RuntimePolicyLimits>,
    canonical_candidates: Option<Vec<TradePolicyCandidateSpec>>,
    candidate_set_hash: Option<ContentHash>,
    profile: Option<ResearchProfileArtifact>,
    fit_window_start: Option<DateTime<Utc>>,
    fit_window_end: Option<DateTime<Utc>>,
    research_program_hash: Option<ContentHash>,
    source_slice_identity: Option<SourceSliceIdentity>,
    messages: Vec<String>,
}

#[derive(Clone, Copy)]
enum PreflightCheck {
    Pass,
    Fail,
}

impl PreflightCheck {
    const fn is_pass(self) -> bool {
        matches!(self, Self::Pass)
    }
}

impl From<bool> for PreflightCheck {
    fn from(value: bool) -> Self {
        if value { Self::Pass } else { Self::Fail }
    }
}

struct DatasetPreflight {
    ready: PreflightCheck,
    policy_fit: PreflightCheck,
    raw_trajectory_labels_present: PreflightCheck,
    profile_lineage_valid: PreflightCheck,
    source_slice_verified: PreflightCheck,
    full_l2_trajectory_present: PreflightCheck,
    fee_model_present: PreflightCheck,
    fit_window_contained: PreflightCheck,
    pit_cutoff_valid: PreflightCheck,
    labels_matured_by_cutoff: u64,
    labels_excluded_after_cutoff: u64,
    messages: Vec<String>,
}

struct SourceSlicePreflight {
    verified: bool,
    full_l2: bool,
    fee_model: bool,
    messages: Vec<String>,
}

struct ValidationRowSummary {
    total_rows: i64,
    passed_rows: i64,
    failed_rows: i64,
    row_chain_hash: ContentHash,
}

struct ValidationInputs {
    current: TradePolicyArtifactInfo,
    dataset: TrainingDatasetInfo,
    examples: Vec<TrainingExample>,
    source_slice_manifest_hash: ContentHash,
    evidence_manifest_hash: ContentHash,
}

struct PolicyEvidenceObjects {
    objects: Vec<TradePolicyEvidenceObjectRef>,
}

#[derive(Clone, Copy)]
enum PolicyReplayPurpose {
    Fit,
    Validation,
}

struct WeatherPolicyRecomputeInput<'a> {
    purpose: PolicyReplayPurpose,
    source: &'a FrozenSourceSlice,
    examples: &'a [TrainingExample],
    profile: &'a ResearchProfileArtifact,
    candidates: &'a [TradePolicyCandidateSpec],
    model_version_id: &'a ModelVersionId,
    decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    feature_schema_hash: &'a ContentHash,
    factor_schema_hash: &'a ContentHash,
    experiment_family_hash: &'a ContentHash,
    latency_profile: &'a ShadowLatencyProfileV1,
    fit_window_start: DateTime<Utc>,
    fit_window_end: DateTime<Utc>,
    pit_cutoff: DateTime<Utc>,
    activation_target: VerticalActivationTarget,
    progress: &'a dyn JobProgressSink,
    cancel: &'a CancellationToken,
}

struct WeatherPolicyRecomputeResult {
    evidence: WeatherPolicyEvidence,
    experiment_family_hash: ContentHash,
    embargo_secs: u64,
}

struct WeatherReplayInputs {
    structural_examples: Vec<TrainingExample>,
    replayed_examples: Vec<WeatherExampleReplay>,
    gate_linkages: Vec<MarketLinkage>,
    gate_observations: Vec<WeatherObservationFact>,
    structural_executions: Vec<ExecutionParticipantFactRow>,
}

struct FrozenFitPlan {
    profile: ResearchProfileArtifact,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    runtime_limits: RuntimePolicyLimits,
    research_program_hash: ContentHash,
    fit_window_start: DateTime<Utc>,
    fit_window_end: DateTime<Utc>,
    candidates: Vec<TradePolicyCandidateSpec>,
    candidate_set_hash: ContentHash,
    reusable_source_dataset_id: Option<TrainingDatasetId>,
}

struct FitDatasetInputs {
    source_dataset_id: TrainingDatasetId,
    dataset_hash: ContentHash,
    feature_schema_hash: ContentHash,
    factor_schema_hash: ContentHash,
    label_schema_hash: ContentHash,
    source_slice_ref: SourceSliceManifestRef,
    examples: Vec<TrainingExample>,
    frozen_source: FrozenSourceSlice,
    latency_evidence_id: ResearchReadinessEvidenceId,
    latency_profile_hash: ContentHash,
    latency_profile: ShadowLatencyProfileV1,
}

struct SealedFitEvidence {
    evidence: WeatherPolicyEvidence,
    embargo_secs: u64,
    manifest: TradePolicyEvidenceBundleManifest,
    manifest_uri: ArtifactUri,
    manifest_hash: ContentHash,
    simulator_hash: ContentHash,
    replay_kernel_hash: ContentHash,
    catalog_ledger_hash: ContentHash,
    trial_ledger_hash: ContentHash,
}

struct PolicySourceSlice {
    manifest_ref: SourceSliceManifestRef,
    manifest: SourceSliceManifest,
}

struct ValidationCompletionInput<'a> {
    validation_run_id: &'a TradePolicyValidationRunId,
    artifact_id: &'a TradePolicyArtifactId,
    actor_id: UserId,
    reason: String,
    current: &'a TradePolicyArtifactInfo,
    source_slice_manifest_hash: &'a ContentHash,
    evidence_manifest_hash: &'a ContentHash,
    row_summary: &'a ValidationRowSummary,
}

struct DatasetEvaluationInput<'a> {
    selection: &'a TradePolicyFitSelection,
    evaluation_track: ResearchEvaluationTrack,
    profile: Option<&'a ResearchProfileArtifact>,
    research_program_hash: Option<&'a ContentHash>,
    fit_window_start: Option<DateTime<Utc>>,
    fit_window_end: Option<DateTime<Utc>>,
    dataset: Option<&'a TrainingDatasetInfo>,
    source_slice: Option<&'a SourceSliceInfo>,
}

struct ContractIdentityInput<'a> {
    request: &'a TradePolicyFitPreflightRequest,
    profile: Option<&'a ResearchProfileArtifact>,
    research_program_hash: Option<&'a ContentHash>,
    decision_policy_snapshot_id: Option<&'a DecisionPolicySnapshotId>,
    runtime_limits: Option<&'a RuntimePolicyLimits>,
    fit_window_start: Option<DateTime<Utc>>,
    fit_window_end: Option<DateTime<Utc>>,
}

struct OperationalPreflight {
    evidence: VerifiedOperationalEvidence,
    latency_profile_present: bool,
    retention_runway_days: Option<u32>,
    required_raw_retention_days: Option<u32>,
    retention_runway_proven: bool,
}

impl SourceSlicePreflight {
    fn blocked(message: impl Into<String>) -> Self {
        Self {
            verified: false,
            full_l2: false,
            fee_model: false,
            messages: vec![message.into()],
        }
    }
}

impl TradePolicyService {
    #[must_use]
    pub fn new(deps: TradePolicyServiceDeps) -> Self {
        let evidence_verifier = TradePolicyEvidenceVerifier::new(TradePolicyEvidenceVerifierDeps {
            artifacts: Arc::clone(&deps.artifacts),
            policies: Arc::clone(&deps.policies),
            readiness: Arc::clone(&deps.readiness),
        });
        Self {
            compute: deps.compute,
            datasets: deps.datasets,
            dataset_builder: deps.dataset_builder,
            artifacts: deps.artifacts,
            policies: deps.policies,
            model_registry: deps.model_registry,
            runtime_configs: deps.runtime_configs,
            source_slices: deps.source_slices,
            readiness: deps.readiness,
            evidence_verifier,
            serving_preimages: deps.serving_preimages,
        }
    }

    async fn runtime_policy_limits(
        &self,
        version_id: &DecisionPolicySnapshotId,
        profile: &ResearchProfileArtifact,
    ) -> QuantResult<RuntimePolicyLimits> {
        let version = self
            .runtime_configs
            .load_snapshot(version_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "decision_policy_snapshot",
                id: version_id.to_string(),
            })?;
        let config = version.snapshot;
        let route = BuyModelRoute::try_from(profile.spec.category)?;
        let model_binding = config
            .model_routing
            .model
            .champion(route)
            .map_err(|error| ResearchError::ValidationMethodology {
                detail: format!(
                    "runtime config {version_id} has no exact {route:?} model binding for profile \
                     {}: {error}",
                    profile.profile_ref.id
                ),
            })?;
        let model_version_id = model_binding.model_version_id;
        let model_version = self
            .model_registry
            .find_model_version(&model_version_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_model_version",
                id: model_version_id.to_string(),
            })?;
        if model_version.profile_ref != profile.profile_ref {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "fit model {} must be the active route artifact and bind profile {} exactly",
                    model_version_id, profile.profile_ref.id
                ),
            }
            .into());
        }
        let model_spec = self
            .model_registry
            .find_model_spec(&model_version.model_spec_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_model_spec",
                id: model_version.model_spec_id.to_string(),
            })?;
        let model_spec_hash = model_spec.definition().content_hash().map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("fit model spec hash failed: {error}"),
            }
        })?;
        if model_spec_hash != model_spec.definition_hash
            || model_spec.feature_schema_version
                != config
                    .profile_artifacts
                    .features
                    .definition
                    .feature_schema_version
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "fit model spec {} is not an intact immutable definition on the frozen feature schema",
                    model_spec.model_spec_id
                ),
            }
            .into());
        }
        let knowledge_lag_secs = config.pit_knowledge_lag_secs().ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: "frozen runtime config has no unambiguous PIT knowledge lag".to_owned(),
            }
        })?;
        let policy = &config
            .profile_artifacts
            .research_method
            .research
            .policy_validation;
        let methodology_hash = CanonicalDigest::content_hash_json(&(
            (
                "trade_policy_methodology_v1",
                POLICY_PERFORMANCE_METHODOLOGY_VERSION,
                "common_executable_candidate_intersection",
                "interval_uniqueness_weighted_groups",
                "cpcv_path_closest_to_path_sharpe_median_for_dsr_observed_sharpe",
                "governed_candidate_count_and_cross_candidate_sharpe_variance_for_dsr_trials",
                "eight_block_cscv_pbo",
                "market_clustered_one_sided_95pct_bootstrap_2000_replicates",
            ),
            (
                PolicyValidationConfig::CPCV_N_GROUPS,
                PolicyValidationConfig::CPCV_K_TEST,
                PolicyValidationConfig::PBO_BLOCK_COUNT,
                PolicyValidationConfig::FOLD_COUNT,
                PolicyValidationConfig::COMPLETE_PATH_COUNT,
                PolicyValidationConfig::UTILITY_CONFIDENCE_BPS,
                PolicyValidationConfig::LATENCY_STRESS_MULTIPLIER,
                WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS,
                policy.min_latency_profile_secs,
            ),
        ))?;
        Ok(RuntimePolicyLimits {
            max_candidates: config
                .profile_artifacts
                .research_method
                .research
                .policy_validation
                .max_candidates_per_experiment,
            methodology_hash,
            runtime_config_hash: version.snapshot_hash,
            min_latency_profile_secs: policy.min_latency_profile_secs,
            fit_model: FrozenPolicyFitModel {
                model_version_id,
                model_spec_id: model_spec.model_spec_id,
                feature_schema_version: model_spec.feature_schema_version,
                knowledge_lag_secs,
            },
        })
    }

    async fn evaluate_contract(
        &self,
        request: &TradePolicyFitPreflightRequest,
    ) -> QuantResult<ContractPreflight> {
        let mut messages = Vec::new();
        let selection_valid = match request.selection.validate() {
            Ok(()) => true,
            Err(detail) => {
                messages.push(detail);
                false
            }
        };
        let profile = match request
            .selection
            .profile_ref
            .resolve_builtin_research_profile()
        {
            Ok(profile) => Some(profile),
            Err(detail) => {
                messages.push(detail);
                None
            }
        };
        let decision_policy_snapshot_id = self
            .runtime_configs
            .load_active_at(request.selection.pit_cutoff)
            .await?
            .map(|version| version.decision_policy_snapshot_id);
        let runtime_limits = match (decision_policy_snapshot_id.as_ref(), profile.as_ref()) {
            (Some(version_id), Some(profile)) => {
                match self.runtime_policy_limits(version_id, profile).await {
                    Ok(limits) => Some(limits),
                    Err(error) => {
                        messages.push(format!(
                            "frozen runtime v1 fit contract is unavailable: {error}"
                        ));
                        None
                    }
                }
            }
            _ => None,
        };
        let pit_cutoff_not_future = request.selection.pit_cutoff <= Utc::now();
        if !pit_cutoff_not_future {
            messages.push("PIT cutoff cannot be in the future".to_owned());
        }
        let evaluation_track_allowed = profile
            .as_ref()
            .is_some_and(|profile| profile.spec.permits(request.evaluation_track));
        if !evaluation_track_allowed {
            messages
                .push("research profile does not permit the requested evaluation track".to_owned());
        }
        let canonical_candidates = match canonicalize_policy_candidates(request.candidates.clone())
        {
            Ok(candidates) => Some(candidates),
            Err(detail) => {
                messages.push(detail);
                None
            }
        };
        let profile_quality_gate_available = profile.is_some();
        if !profile_quality_gate_available {
            messages.push("immutable profile publication quality gate is unavailable".to_owned());
        }
        let profile_fitter_available = profile
            .as_ref()
            .is_some_and(|profile| profile.spec.policy_fitter.is_some());
        if !profile_fitter_available {
            messages.push("research profile has no implemented policy fitter".to_owned());
        }
        let candidate_count_allowed = runtime_limits.as_ref().is_some_and(|limits| {
            canonical_candidates.as_ref().is_some_and(|candidates| {
                u32::try_from(candidates.len()).is_ok_and(|count| count <= limits.max_candidates)
            })
        });
        if !candidate_count_allowed {
            messages.push(
                "candidate count exceeds the frozen runtime experiment limit; preflight never truncates"
                    .to_owned(),
            );
        }
        let (fit_window_start, fit_window_end) =
            contract_fit_window(profile.as_ref(), request.selection.pit_cutoff);
        let research_program_hash = derive_research_program_hash(
            request,
            profile.as_ref(),
            canonical_candidates.as_deref(),
            decision_policy_snapshot_id.as_ref(),
            runtime_limits.as_ref(),
        )?;
        let source_slice_identity = derive_contract_source_identity(&ContractIdentityInput {
            request,
            profile: profile.as_ref(),
            research_program_hash: research_program_hash.as_ref(),
            decision_policy_snapshot_id: decision_policy_snapshot_id.as_ref(),
            runtime_limits: runtime_limits.as_ref(),
            fit_window_start,
            fit_window_end,
        })?;
        let valid = selection_valid
            && pit_cutoff_not_future
            && evaluation_track_allowed
            && canonical_candidates.is_some()
            && profile_quality_gate_available
            && profile_fitter_available
            && runtime_limits.is_some()
            && candidate_count_allowed;
        let candidate_set_hash = canonical_candidates
            .as_ref()
            .map(CanonicalDigest::content_hash_json)
            .transpose()?;
        Ok(ContractPreflight {
            valid: valid.into(),
            profile_fitter_available,
            pit_cutoff_not_future,
            profile_quality_gate_available,
            decision_policy_snapshot_id,
            runtime_limits,
            canonical_candidates,
            candidate_set_hash,
            profile,
            fit_window_start,
            fit_window_end,
            research_program_hash,
            source_slice_identity,
            messages,
        })
    }

    async fn find_source_slice(
        &self,
        contract: &ContractPreflight,
    ) -> QuantResult<Option<SourceSliceInfo>> {
        let Some(identity) = contract.source_slice_identity.as_ref() else {
            return Ok(None);
        };
        self.source_slices
            .find_by_identity(&identity.identity_hash)
            .await
            .map_err(Into::into)
    }

    async fn find_reusable_dataset(
        &self,
        contract: &ContractPreflight,
    ) -> QuantResult<Option<TrainingDatasetInfo>> {
        let (
            Some(profile),
            Some(program_hash),
            Some(decision_policy_snapshot_id),
            Some(fit_window_start),
            Some(fit_window_end),
        ) = (
            contract.profile.as_ref(),
            contract.research_program_hash.as_ref(),
            contract.decision_policy_snapshot_id.as_ref(),
            contract.fit_window_start,
            contract.fit_window_end,
        )
        else {
            return Ok(None);
        };
        let page = self
            .datasets
            .page(TrainingDatasetListQuery {
                status: Some(TrainingDatasetStatus::Ready),
                purpose: Some(DatasetPurpose::PolicyFit),
                page: PageRequest::new(1, PageRequest::MAX_SIZE),
                ..TrainingDatasetListQuery::default()
            })
            .await?;
        Ok(page.items.into_iter().find(|dataset| {
            dataset.decision_policy_snapshot_id == *decision_policy_snapshot_id
                && dataset.window_start <= fit_window_start
                && dataset.window_end >= fit_window_end
                && dataset.manifest.as_ref().is_some_and(|manifest| {
                    manifest.format_version == DATASET_ARTIFACT_FORMAT_VERSION
                        && manifest.training_dataset_id == dataset.training_dataset_id
                        && manifest
                            .source_lineage
                            .research_profile_artifact_id
                            .profile_ref()
                            == profile.profile_ref
                        && &manifest.source_lineage.research_program_hash == program_hash
                })
        }))
    }

    async fn evaluate_dataset(
        &self,
        input: DatasetEvaluationInput<'_>,
    ) -> QuantResult<DatasetPreflight> {
        let mut messages = Vec::new();
        let ready = input
            .dataset
            .is_some_and(|row| row.status == TrainingDatasetStatus::Ready);
        if !ready {
            messages.push("source dataset is missing or not Ready".to_owned());
        }
        let policy_fit = input
            .dataset
            .is_some_and(|row| row.purpose == DatasetPurpose::PolicyFit);
        if !policy_fit {
            messages.push("source dataset purpose must be PolicyFit".to_owned());
        }
        let fit_window_contained = input.dataset.is_some_and(|row| {
            input
                .fit_window_start
                .is_some_and(|start| start >= row.window_start)
                && input
                    .fit_window_end
                    .is_some_and(|end| end <= row.window_end)
        });
        if !fit_window_contained {
            messages.push("fit window is outside the source dataset".to_owned());
        }
        let profile_lineage_valid = input
            .dataset
            .zip(input.profile)
            .zip(input.research_program_hash)
            .is_some_and(|((row, profile), program_hash)| {
                row.manifest.as_ref().is_some_and(|manifest| {
                    manifest.format_version == DATASET_ARTIFACT_FORMAT_VERSION
                        && manifest.training_dataset_id == row.training_dataset_id
                        && manifest
                            .source_lineage
                            .research_profile_artifact_id
                            .profile_ref()
                            == profile.profile_ref
                        && &manifest.source_lineage.research_program_hash == program_hash
                })
            });
        if !profile_lineage_valid {
            messages.push(
                "Dataset v3 profile/program lineage does not match the fit request".to_owned(),
            );
        }
        let source_slice = self.evaluate_source_slice(&input).await;
        messages.extend(source_slice.messages);
        let pit_cutoff_valid = input
            .fit_window_end
            .is_some_and(|end| end <= input.selection.pit_cutoff);
        if !pit_cutoff_valid {
            messages.push("fit window ends after the PIT cutoff".to_owned());
        }
        let (raw_trajectory_labels_present, labels_matured_by_cutoff, labels_excluded_after_cutoff) =
            if ready && fit_window_contained {
                let dataset =
                    input
                        .dataset
                        .ok_or_else(|| ResearchError::ValidationMethodology {
                            detail: "ready dataset disappeared during policy preflight".to_owned(),
                        })?;
                let materialization = require_dataset_materialization(dataset)?;
                let bytes = self.artifacts.get(materialization.parquet_uri).await?;
                let examples = verify_frozen_dataset_artifact(dataset, &bytes)?;
                let (matured, excluded) = label_cutoff_counts(
                    input.fit_window_start,
                    input.fit_window_end,
                    input.selection.pit_cutoff,
                    input
                        .profile
                        .map(|profile| profile.spec.target_horizon_secs),
                    &examples,
                );
                (matured > 0, matured, excluded)
            } else {
                (false, 0, 0)
            };
        if !raw_trajectory_labels_present {
            messages.push(
                "source dataset has no complete PIT-mature raw return/MFE/MAE/liquidity trajectory rows for the profile horizon"
                    .to_owned(),
            );
        }
        Ok(DatasetPreflight {
            ready: ready.into(),
            policy_fit: policy_fit.into(),
            raw_trajectory_labels_present: raw_trajectory_labels_present.into(),
            profile_lineage_valid: profile_lineage_valid.into(),
            source_slice_verified: source_slice.verified.into(),
            full_l2_trajectory_present: source_slice.full_l2.into(),
            fee_model_present: source_slice.fee_model.into(),
            fit_window_contained: fit_window_contained.into(),
            pit_cutoff_valid: pit_cutoff_valid.into(),
            labels_matured_by_cutoff,
            labels_excluded_after_cutoff,
            messages,
        })
    }

    async fn evaluate_source_slice(
        &self,
        input: &DatasetEvaluationInput<'_>,
    ) -> SourceSlicePreflight {
        let Some(dataset_manifest) = input.dataset.and_then(|row| row.manifest.as_ref()) else {
            return SourceSlicePreflight::blocked("Dataset v3 manifest is unavailable");
        };
        let Some(source_slice_info) = input.source_slice else {
            return SourceSlicePreflight::blocked(
                "dataset Source Slice has no canonical materialization ledger row",
            );
        };
        if source_slice_info.manifest_uri.as_ref()
            != Some(&dataset_manifest.source_lineage.source_slice.manifest_uri)
            || source_slice_info.manifest_hash.as_ref()
                != Some(&dataset_manifest.source_lineage.source_slice.manifest_hash)
        {
            return SourceSlicePreflight::blocked(
                "dataset Source Slice reference differs from the canonical ledger binding",
            );
        }
        let frozen = match SourceSliceReader::new(Arc::clone(&self.artifacts))
            .read(source_slice_info)
            .await
        {
            Ok(frozen) => frozen,
            Err(error) => {
                return SourceSlicePreflight::blocked(format!(
                    "source-slice objects cannot be independently read and verified: {error}"
                ));
            }
        };
        let Some(manifest) = source_slice_info.manifest.as_ref() else {
            return SourceSlicePreflight::blocked("Ready Source Slice has no manifest payload");
        };
        let (
            Some(profile),
            Some(research_program_hash),
            Some(fit_window_start),
            Some(fit_window_end),
        ) = (
            input.profile,
            input.research_program_hash,
            input.fit_window_start,
            input.fit_window_end,
        )
        else {
            return SourceSlicePreflight::blocked(
                "source-slice profile/program/window contract is unavailable",
            );
        };
        let kinds = match manifest.validate_for_profile(
            profile,
            research_program_hash,
            fit_window_start,
            fit_window_end,
            input.selection.pit_cutoff,
        ) {
            Ok(kinds) => kinds,
            Err(detail) => {
                return SourceSlicePreflight::blocked(format!(
                    "source-slice profile contract failed: {detail}"
                ));
            }
        };
        if manifest.evaluation_track != input.evaluation_track {
            return SourceSlicePreflight::blocked(
                "source-slice evaluation track does not match the fit request",
            );
        }
        let full_l2 = !frozen.l2_ledger.is_empty()
            && !frozen.sessions.is_empty()
            && [
                SourceSliceObjectKind::L2Ledger,
                SourceSliceObjectKind::L2Session,
                SourceSliceObjectKind::L2Gap,
                SourceSliceObjectKind::MarketExecution,
                SourceSliceObjectKind::ExecutionParticipant,
            ]
            .into_iter()
            .all(|kind| kinds.contains(&kind));
        let fee_model = !frozen.prefetched.clob_market_info.is_empty()
            && kinds.contains(&SourceSliceObjectKind::ClobMarketInfo);
        SourceSlicePreflight {
            verified: true,
            full_l2,
            fee_model,
            messages: Vec::new(),
        }
    }

    async fn write_policy_evidence_objects(
        &self,
        evidence: &WeatherPolicyEvidence,
    ) -> QuantResult<PolicyEvidenceObjects> {
        let mut by_kind = evidence.records_by_kind()?;
        let mut objects = Vec::with_capacity(TradePolicyEvidenceObjectKind::REQUIRED.len());
        for kind in TradePolicyEvidenceObjectKind::REQUIRED {
            let mut records =
                by_kind
                    .remove(&kind)
                    .ok_or_else(|| ResearchError::ValidationMethodology {
                        detail: format!("policy evidence producer omitted {kind:?}"),
                    })?;
            records.sort_by(|left, right| left.record_key.cmp(&right.record_key));
            let bytes = PolicyEvidenceParquetCodec::encode(&records)?;
            let byte_hash = CanonicalDigest::content_hash_bytes(&bytes);
            let uri = self
                .artifacts
                .put(
                    ArtifactKey::new(
                        ArtifactNamespace::PolicyEvidence,
                        format!("{}-{}", evidence_kind_slug(kind), byte_hash.hex()),
                        "parquet",
                    )?,
                    &bytes,
                )
                .await?;
            let verified_bytes = self.artifacts.get(&uri).await?;
            let verified_hash = CanonicalDigest::content_hash_bytes(&verified_bytes);
            if verified_hash != byte_hash {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!("policy evidence {kind:?} changed after write"),
                }
                .into());
            }
            let verified_records = PolicyEvidenceParquetCodec::decode(&verified_bytes)?;
            if verified_records != records {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!("policy evidence {kind:?} semantic round-trip differs"),
                }
                .into());
            }
            objects.push(TradePolicyEvidenceObjectRef {
                kind,
                uri,
                byte_hash,
                row_chain_hash: PolicyEvidenceParquetCodec::row_chain_hash(&verified_records)?,
                row_count: u64::try_from(verified_records.len()).map_err(|error| {
                    ResearchError::ValidationMethodology {
                        detail: format!("policy evidence row count does not fit u64: {error}"),
                    }
                })?,
            });
        }
        Ok(PolicyEvidenceObjects { objects })
    }

    async fn append_policy_trial_ledger(
        &self,
        fit_job_id: &ResearchJobId,
        experiment_family_hash: &ContentHash,
        research_program_hash: &ContentHash,
        candidates: &[TradePolicyCandidateSpec],
        evidence: &WeatherPolicyEvidence,
        objects: Option<&PolicyEvidenceObjects>,
    ) -> QuantResult<(DateTime<Utc>, ContentHash)> {
        let candidate_hashes = candidates
            .iter()
            .map(|candidate| {
                Ok((
                    candidate.candidate_id.clone(),
                    CanonicalDigest::content_hash_json(candidate)?,
                ))
            })
            .collect::<QuantResult<BTreeMap<_, _>>>()?;
        let mut attempts = Vec::new();
        for trial in &evidence.cohort_trials {
            let metrics = TradePolicyTrialMetrics {
                sample_count: trial.sample_count,
                effective_sample_size: trial.effective_sample_size,
                expected_net_return_bps: trial.weighted_mean_expected_return_bps,
                risk_net_return_bps: trial.weighted_mean_risk_return_bps,
                expected_sharpe_ratio: Some(trial.expected_sharpe_ratio),
                executable_coverage: trial.executable_coverage,
                full_l2_coverage: trial.full_l2_coverage,
                fee_catalog_coverage: trial.fee_catalog_coverage,
                passive_rebate_evidence_coverage: trial.passive_rebate_evidence_coverage,
                ambiguous_touch_rate: trial.ambiguous_touch_rate,
                depth_failure_rate: trial.depth_failure_rate,
                latency_stress_multiplier: trial.latency_multiplier,
            };
            attempts.push(TrialAttemptSpec {
                candidate_id: trial.candidate_id.clone(),
                scope: TradePolicyTrialScope::Candidate,
                fold_index: None,
                path_index: None,
                status: TradePolicyTrialStatus::Succeeded,
                metrics: Some(metrics),
                evidence_kind: Some(TradePolicyEvidenceObjectKind::CohortTrials),
                failure_detail: None,
            });
        }
        for run in &evidence.statistical_runs {
            let selected_trial = evidence
                .cohort_trials
                .iter()
                .find(|trial| {
                    trial.cohort_hash == run.cohort_hash
                        && trial.latency_multiplier == run.latency_multiplier
                        && trial.candidate_id == run.summary.selected_candidate_id
                })
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "selected statistical trial has no aggregate evidence".to_owned(),
                })?;
            let passed = evidence.statistical_summaries.iter().any(|summary| {
                summary.cohort_hash == run.cohort_hash
                    && summary.latency_multiplier == run.latency_multiplier
                    && summary.passed
            });
            append_statistical_attempts(&mut attempts, run, selected_trial, passed)?;
        }
        for (ordinal, spec) in attempts.into_iter().enumerate() {
            let attempt_ordinal =
                i64::try_from(ordinal).map_err(|error| ResearchError::ValidationMethodology {
                    detail: format!("policy trial ordinal does not fit i64: {error}"),
                })?;
            let candidate_hash = candidate_hashes
                .get(&spec.candidate_id)
                .copied()
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: format!(
                        "trial ledger candidate {} is outside the frozen family",
                        spec.candidate_id
                    ),
                })?;
            let evidence_object = spec
                .evidence_kind
                .and_then(|kind| objects.and_then(|objects| evidence_object(objects, kind)));
            let mut attempt = NewTradePolicyTrialAttempt {
                trial_attempt_id: TradePolicyTrialAttemptId::from_fit_job_ordinal(
                    fit_job_id,
                    attempt_ordinal,
                ),
                fit_job_id: *fit_job_id,
                attempt_ordinal,
                experiment_family_hash: *experiment_family_hash,
                research_program_hash: *research_program_hash,
                candidate_id: TradePolicyCandidateId::parse(spec.candidate_id).map_err(
                    |error| ResearchError::ValidationMethodology {
                        detail: error.to_string(),
                    },
                )?,
                candidate_hash,
                scope: spec.scope,
                fold_index: spec.fold_index,
                path_index: spec.path_index,
                status: spec.status,
                metrics_json: spec.metrics,
                evidence_uri: evidence_object.map(|object| object.uri.clone()),
                evidence_hash: evidence_object.map(|object| object.byte_hash),
                evidence_row_count: evidence_object
                    .map(|object| i64::try_from(object.row_count))
                    .transpose()
                    .map_err(|error| ResearchError::ValidationMethodology {
                        detail: format!("trial evidence row count does not fit i64: {error}"),
                    })?,
                failure_detail: spec.failure_detail,
                row_hash: ResearchHasher::canonical(&("pending_policy_trial_row", ordinal))?,
            };
            attempt.row_hash = attempt.expected_row_hash().map_err(QuantError::from)?;
            self.policies.append_trial_attempt(attempt).await?;
        }
        let ledger = self.policies.list_trial_attempts(fit_job_id, None).await?;
        let cutoff = ledger
            .last()
            .map(|attempt| attempt.created_at)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "policy trial ledger is empty after append".to_owned(),
            })?;
        let ledger_hash = ResearchHasher::canonical(&(
            "trade_policy_trial_ledger_v1",
            fit_job_id,
            ledger
                .iter()
                .map(|attempt| (attempt.attempt_ordinal, &attempt.row_hash))
                .collect::<Vec<_>>(),
        ))?;
        Ok((cutoff, ledger_hash))
    }

    async fn append_fit_terminal_attempts(
        &self,
        fit_job_id: &ResearchJobId,
        experiment_family_hash: &ContentHash,
        research_program_hash: &ContentHash,
        candidates: &[TradePolicyCandidateSpec],
        status: TradePolicyTrialStatus,
        detail: &str,
    ) -> QuantResult<()> {
        for (ordinal, candidate) in candidates.iter().enumerate() {
            let attempt_ordinal =
                i64::try_from(ordinal).map_err(|error| ResearchError::ValidationMethodology {
                    detail: format!("terminal trial ordinal does not fit i64: {error}"),
                })?;
            let candidate_hash = CanonicalDigest::content_hash_json(candidate)?;
            let mut attempt = NewTradePolicyTrialAttempt {
                trial_attempt_id: TradePolicyTrialAttemptId::from_fit_job_ordinal(
                    fit_job_id,
                    attempt_ordinal,
                ),
                fit_job_id: *fit_job_id,
                attempt_ordinal,
                experiment_family_hash: *experiment_family_hash,
                research_program_hash: *research_program_hash,
                candidate_id: TradePolicyCandidateId::parse(&candidate.candidate_id).map_err(
                    |error| ResearchError::ValidationMethodology {
                        detail: error.to_string(),
                    },
                )?,
                candidate_hash,
                scope: TradePolicyTrialScope::Candidate,
                fold_index: None,
                path_index: None,
                status,
                metrics_json: None,
                evidence_uri: None,
                evidence_hash: None,
                evidence_row_count: None,
                failure_detail: Some(detail.to_owned()),
                row_hash: ResearchHasher::canonical(&("pending_terminal_policy_trial", ordinal))?,
            };
            attempt.row_hash = attempt.expected_row_hash().map_err(QuantError::from)?;
            self.policies.append_trial_attempt(attempt).await?;
        }
        Ok(())
    }

    async fn read_validation_source_slice(
        &self,
        dataset: &TrainingDatasetInfo,
    ) -> QuantResult<FrozenSourceSlice> {
        let dataset_manifest =
            dataset
                .manifest
                .as_ref()
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: format!(
                        "policy source dataset {} has no immutable manifest",
                        dataset.training_dataset_id
                    ),
                })?;
        let bytes = self
            .evidence_verifier
            .read_manifest(
                &dataset_manifest.source_lineage.source_slice.manifest_uri,
                &dataset_manifest.source_lineage.source_slice.manifest_hash,
            )
            .await?;
        let manifest = serde_json::from_slice::<SourceSliceManifest>(&bytes).map_err(|error| {
            ResearchError::Serialization {
                detail: format!("invalid Source Slice manifest during validation: {error}"),
            }
        })?;
        let identity = SourceSliceIdentity::derive(SourceSliceIdentityInput {
            profile_ref: manifest.profile_ref.clone(),
            evaluation_track: manifest.evaluation_track,
            research_program_hash: manifest.research_program_hash,
            decision_policy_snapshot_id: manifest.decision_policy_snapshot_id,
            runtime_config_hash: manifest.runtime_config_hash,
            fit_seal_id: manifest.fit_seal_id,
            fit_seal_hash: manifest.fit_seal_hash,
            window_start: manifest.window_start,
            window_end: manifest.window_end,
            pit_cutoff: manifest.pit_cutoff,
        })?;
        let source_slice = self
            .source_slices
            .find_by_identity(&identity.identity_hash)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "source_slice",
                id: identity.identity_hash.to_string(),
            })?;
        if source_slice.manifest_hash.as_ref()
            != Some(&dataset_manifest.source_lineage.source_slice.manifest_hash)
            || source_slice.manifest_uri.as_ref()
                != Some(&dataset_manifest.source_lineage.source_slice.manifest_uri)
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "source-slice ledger binding differs from the Dataset manifest".to_owned(),
            }
            .into());
        }
        SourceSliceReader::new(Arc::clone(&self.artifacts))
            .read(&source_slice)
            .await
    }

    async fn freeze_fit_plan(&self, request: &FitTradePolicyRequest) -> QuantResult<FrozenFitPlan> {
        let preflight_request = TradePolicyFitPreflightRequest {
            selection: request.selection.clone(),
            evaluation_track: request.evaluation_track,
            candidates: request.candidates.clone(),
        };
        let preflight = self.preflight(&preflight_request).await?;
        if !preflight.blockers.is_empty() {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "trade-policy fit preflight is blocked; no Draft was created: {:?}",
                    preflight
                        .blockers
                        .iter()
                        .map(|blocker| &blocker.detail)
                        .collect::<Vec<_>>()
                ),
            }
            .into());
        }
        let contract = self.evaluate_contract(&preflight_request).await?;
        let missing = |field: &str| ResearchError::ValidationMethodology {
            detail: format!("{field} disappeared after successful preflight"),
        };
        Ok(FrozenFitPlan {
            profile: contract
                .profile
                .ok_or_else(|| missing("frozen fit profile"))?,
            decision_policy_snapshot_id: contract
                .decision_policy_snapshot_id
                .ok_or_else(|| missing("frozen runtime config"))?,
            runtime_limits: contract
                .runtime_limits
                .ok_or_else(|| missing("frozen runtime methodology"))?,
            research_program_hash: contract
                .research_program_hash
                .ok_or_else(|| missing("research program hash"))?,
            fit_window_start: contract
                .fit_window_start
                .ok_or_else(|| missing("fit window start"))?,
            fit_window_end: contract
                .fit_window_end
                .ok_or_else(|| missing("fit window end"))?,
            candidates: contract
                .canonical_candidates
                .ok_or_else(|| missing("canonical fit candidates"))?,
            candidate_set_hash: contract
                .candidate_set_hash
                .ok_or_else(|| missing("candidate-set hash"))?,
            reusable_source_dataset_id: preflight.reusable_source_dataset_id,
        })
    }

    async fn prepare_fit_dataset(
        &self,
        plan: &FrozenFitPlan,
        fit_dataset_id: &TrainingDatasetId,
        request: &FitTradePolicyRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FitDatasetInputs> {
        let source_dataset_id = if let Some(id) = &plan.reusable_source_dataset_id {
            *id
        } else {
            progress.report(ResearchJobProgress::indeterminate(
                "materializing_source_slice",
                0,
            ));
            let build_request = PolicyFitDatasetBuildRequest {
                dataset: BuildTrainingDatasetRequest {
                    model_spec_id: plan.runtime_limits.fit_model.model_spec_id,
                    profile_ref: plan.profile.profile_ref.clone(),
                    purpose: DatasetPurpose::PolicyFit,
                    decision_policy_snapshot_id: plan.decision_policy_snapshot_id,
                    fit_seal_id: request.selection.fit_seal_id,
                    fit_seal_hash: request.selection.fit_seal_hash,
                    window_start: plan.fit_window_start,
                    window_end: plan.fit_window_end,
                    pit_cutoff: request.selection.pit_cutoff,
                    sample_interval_secs: plan.profile.spec.decision_cadence_secs,
                    horizons_secs: vec![plan.profile.spec.target_horizon_secs],
                    knowledge_lag_secs: plan.runtime_limits.fit_model.knowledge_lag_secs,
                    feature_schema_version: plan.runtime_limits.fit_model.feature_schema_version,
                    sample_sources: TrainingSampleSources::default(),
                    reason: request.reason.clone(),
                    training_dataset_id: Some(*fit_dataset_id),
                },
                evaluation_track: request.evaluation_track,
                research_program_hash: plan.research_program_hash,
            };
            ensure_fit_active(&cancel, "before Source Slice materialization")?;
            self.dataset_builder
                .build_policy_fit(build_request, Arc::clone(&progress), cancel)
                .await?
                .training_dataset_id
        };
        progress.report(ResearchJobProgress::indeterminate("building_dataset", 0));
        let dataset = self
            .datasets
            .find_by_id(&source_dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_training_dataset",
                id: source_dataset_id.to_string(),
            })?;
        require_fit_dataset(&dataset)?;
        let materialization = require_dataset_materialization(&dataset)?;
        let dataset_bytes = self.artifacts.get(materialization.parquet_uri).await?;
        let examples = verify_frozen_dataset_artifact(&dataset, &dataset_bytes)?;
        let operational = self.readiness.latest_verified(Utc::now()).await?;
        let latency_item =
            operational
                .latency
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "verified shadow-latency evidence disappeared after preflight"
                        .to_owned(),
                })?;
        let ResearchReadinessEvidencePayload::ShadowLatencyProfile(latency_profile) =
            latency_item.payload_json
        else {
            return Err(ResearchError::ValidationMethodology {
                detail: "verified latency evidence carries the wrong payload kind".to_owned(),
            }
            .into());
        };
        let frozen_source = self.read_validation_source_slice(&dataset).await?;
        frozen_source.require_replayable_validation_source()?;
        Ok(FitDatasetInputs {
            source_dataset_id,
            dataset_hash: *materialization.dataset_hash,
            feature_schema_hash: *materialization.feature_schema_hash,
            factor_schema_hash: materialization.factor_schema_hash(),
            label_schema_hash: *materialization.label_schema_hash,
            source_slice_ref: materialization.manifest.source_lineage.source_slice.clone(),
            examples,
            frozen_source,
            latency_evidence_id: latency_item.evidence_id,
            latency_profile_hash: latency_item.payload_hash,
            latency_profile,
        })
    }

    async fn recompute_fit_evidence(
        &self,
        fit_job_id: &ResearchJobId,
        request: &FitTradePolicyRequest,
        plan: &FrozenFitPlan,
        data: &FitDatasetInputs,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<WeatherPolicyRecomputeResult> {
        let experiment_family_hash =
            weather_experiment_family_hash(WeatherExperimentFamilyInput {
                profile_ref: &plan.profile.profile_ref,
                evaluation_track: request.evaluation_track,
                research_program_hash: &plan.research_program_hash,
                model_version_id: &plan.runtime_limits.fit_model.model_version_id,
                methodology_hash: &plan.runtime_limits.methodology_hash,
                latency_profile_hash: &data.latency_profile_hash,
                candidate_set_hash: &plan.candidate_set_hash,
                fit_window_start: plan.fit_window_start,
                fit_window_end: plan.fit_window_end,
            })?;
        let recomputed = self
            .recompute_weather_policy(WeatherPolicyRecomputeInput {
                purpose: PolicyReplayPurpose::Fit,
                source: &data.frozen_source,
                examples: &data.examples,
                profile: &plan.profile,
                candidates: &plan.candidates,
                model_version_id: &plan.runtime_limits.fit_model.model_version_id,
                decision_policy_snapshot_id: &plan.decision_policy_snapshot_id,
                feature_schema_hash: &data.feature_schema_hash,
                factor_schema_hash: &data.factor_schema_hash,
                experiment_family_hash: &experiment_family_hash,
                latency_profile: &data.latency_profile,
                fit_window_start: plan.fit_window_start,
                fit_window_end: plan.fit_window_end,
                pit_cutoff: request.selection.pit_cutoff,
                activation_target: match request.evaluation_track {
                    ResearchEvaluationTrack::ResearchOnly => VerticalActivationTarget::ResearchOnly,
                    ResearchEvaluationTrack::SemiAutoCandidate => {
                        VerticalActivationTarget::SemiAuto
                    }
                },
                progress,
                cancel,
            })
            .await;
        match recomputed {
            Ok(recomputed) => Ok(recomputed),
            Err(error) => {
                let status = if cancel.is_cancelled() {
                    TradePolicyTrialStatus::Cancelled
                } else {
                    TradePolicyTrialStatus::Failed
                };
                self.append_fit_terminal_attempts(
                    fit_job_id,
                    &experiment_family_hash,
                    &plan.research_program_hash,
                    &plan.candidates,
                    status,
                    &error.to_string(),
                )
                .await?;
                Err(error)
            }
        }
    }

    async fn seal_fit_evidence(
        &self,
        fit_job_id: &ResearchJobId,
        plan: &FrozenFitPlan,
        data: &FitDatasetInputs,
        recomputed: WeatherPolicyRecomputeResult,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<SealedFitEvidence> {
        let WeatherPolicyRecomputeResult {
            evidence,
            experiment_family_hash,
            embargo_secs,
        } = recomputed;
        if let Err(error) = ensure_fit_active(cancel, "before trial-ledger append") {
            self.append_fit_terminal_attempts(
                fit_job_id,
                &experiment_family_hash,
                &plan.research_program_hash,
                &plan.candidates,
                TradePolicyTrialStatus::Cancelled,
                &error.to_string(),
            )
            .await?;
            return Err(error);
        }
        let evidence_objects = if evidence.all_gates_passed {
            progress.report(ResearchJobProgress::indeterminate("sealing_evidence", 0));
            match self.write_policy_evidence_objects(&evidence).await {
                Ok(objects) => Some(objects),
                Err(error) => {
                    self.append_fit_terminal_attempts(
                        fit_job_id,
                        &experiment_family_hash,
                        &plan.research_program_hash,
                        &plan.candidates,
                        TradePolicyTrialStatus::Failed,
                        &error.to_string(),
                    )
                    .await?;
                    return Err(error);
                }
            }
        } else {
            None
        };
        let (trial_ledger_cutoff, trial_ledger_hash) = self
            .append_policy_trial_ledger(
                fit_job_id,
                &experiment_family_hash,
                &plan.research_program_hash,
                &plan.candidates,
                &evidence,
                evidence_objects.as_ref(),
            )
            .await?;
        if !evidence.all_gates_passed {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "Weather policy fit completed {} row-level candidate replays, but at least one 1x/2x execution, ESS, CPCV, DSR, PBO, coverage, or bootstrap-utility gate failed; no Draft was created",
                    evidence.candidate_trials.len()
                ),
            }
            .into());
        }
        let objects = evidence_objects
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "passing fit has no sealed evidence objects".to_owned(),
            })?
            .objects;
        let catalog_ledger_hash = self
            .verified_source_catalog_hash(&data.source_slice_ref)
            .await?;
        let simulator_hash = TradePolicyEvidenceVerifier::active_simulator_hash()?;
        let replay_kernel_hash = TradePolicyEvidenceVerifier::active_replay_hash()?;
        let manifest = TradePolicyEvidenceBundleManifest {
            format_version: TRADE_POLICY_EVIDENCE_BUNDLE_FORMAT_VERSION,
            source_dataset_hash: data.dataset_hash,
            candidate_set_hash: plan.candidate_set_hash,
            simulator_hash,
            replay_kernel_hash,
            methodology_hash: plan.runtime_limits.methodology_hash,
            latency_evidence_id: data.latency_evidence_id,
            latency_profile_hash: data.latency_profile_hash,
            catalog_ledger_hash,
            source_slice_manifest_hash: data.source_slice_ref.manifest_hash,
            fit_job_id: *fit_job_id,
            trial_ledger_cutoff,
            trial_ledger_hash,
            objects,
        };
        let (manifest_uri, manifest_hash) = self.write_evidence_manifest(&manifest).await?;
        Ok(SealedFitEvidence {
            evidence,
            embargo_secs,
            manifest,
            manifest_uri,
            manifest_hash,
            simulator_hash,
            replay_kernel_hash,
            catalog_ledger_hash,
            trial_ledger_hash,
        })
    }

    async fn verified_source_catalog_hash(
        &self,
        source_slice_ref: &SourceSliceManifestRef,
    ) -> QuantResult<ContentHash> {
        let bytes = self
            .evidence_verifier
            .read_manifest(
                &source_slice_ref.manifest_uri,
                &source_slice_ref.manifest_hash,
            )
            .await?;
        let manifest = serde_json::from_slice::<SourceSliceManifest>(&bytes).map_err(|error| {
            ResearchError::Serialization {
                detail: format!("invalid Source Slice manifest while sealing policy: {error}"),
            }
        })?;
        manifest
            .validate()
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?;
        CanonicalDigest::content_hash_json(&manifest.catalog_proof).map_err(Into::into)
    }

    async fn write_evidence_manifest(
        &self,
        manifest: &TradePolicyEvidenceBundleManifest,
    ) -> QuantResult<(ArtifactUri, ContentHash)> {
        manifest
            .validate()
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?;
        let bytes = serde_json::to_vec(manifest).map_err(|error| ResearchError::Serialization {
            detail: format!("policy evidence manifest serialization failed: {error}"),
        })?;
        let hash = CanonicalDigest::content_hash_bytes(&bytes);
        let uri = self
            .artifacts
            .put(
                ArtifactKey::new(
                    ArtifactNamespace::PolicyEvidence,
                    format!("bundle-{}", hash.hex()),
                    "json",
                )?,
                &bytes,
            )
            .await?;
        if self.artifacts.get(&uri).await? != bytes {
            return Err(ResearchError::ValidationMethodology {
                detail: "policy evidence manifest changed after write".to_owned(),
            }
            .into());
        }
        Ok((uri, hash))
    }

    async fn fit_policy_job(
        &self,
        fit_job_id: &ResearchJobId,
        training_dataset_id: &TrainingDatasetId,
        request: FitTradePolicyRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TradePolicyArtifactInfo> {
        ensure_fit_active(&cancel, "before freezing the plan")?;
        progress.report(ResearchJobProgress::indeterminate("freezing_plan", 0));
        let plan = self.freeze_fit_plan(&request).await?;
        ensure_fit_active(&cancel, "after freezing the plan")?;
        let data = self
            .prepare_fit_dataset(
                &plan,
                training_dataset_id,
                &request,
                Arc::clone(&progress),
                cancel.clone(),
            )
            .await?;
        let recomputed = self
            .recompute_fit_evidence(
                fit_job_id,
                &request,
                &plan,
                &data,
                progress.as_ref(),
                &cancel,
            )
            .await?;
        let sealed = self
            .seal_fit_evidence(
                fit_job_id,
                &plan,
                &data,
                recomputed,
                progress.as_ref(),
                &cancel,
            )
            .await?;
        let payload = build_fit_artifact_payload(&request, &plan, &data, sealed)?;
        let content_hash = CanonicalDigest::content_hash_json(&payload)?;
        let artifact_id = TradePolicyArtifactId::from_content_hash(&content_hash);
        ensure_fit_active(&cancel, "before Draft creation")?;
        let artifact = self
            .policies
            .insert(NewTradePolicyArtifact {
                artifact_id,
                content_hash,
                status: TradePolicyStatus::Draft,
                source_dataset_id: data.source_dataset_id,
                payload_json: payload,
            })
            .await?;
        progress.report(ResearchJobProgress::indeterminate("draft_created", 1));
        Ok(artifact)
    }

    async fn recompute_weather_policy(
        &self,
        input: WeatherPolicyRecomputeInput<'_>,
    ) -> QuantResult<WeatherPolicyRecomputeResult> {
        ensure_policy_replay_active(input.purpose, input.cancel, "before model re-inference")?;
        let model_version = self
            .model_registry
            .find_model_version(input.model_version_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_model_version",
                id: input.model_version_id.to_string(),
            })?;
        let source = Box::pin(self.serving_preimages.load(&model_version)).await?;
        let runtime = source.buy_runtime()?;
        let signals = reinfer_frozen_policy_signals(
            runtime.as_ref(),
            &model_version,
            input.feature_schema_hash,
            input.factor_schema_hash,
            input.examples,
        )
        .await?;
        let replay = collect_weather_replay_inputs(&input, &signals)?;
        let embargo_secs = weather_embargo_secs(&input)?;
        input
            .progress
            .report(ResearchJobProgress::indeterminate("evaluating_trials", 0));
        let (mut evidence, vertical_gate) = self
            .compute
            .run_offline_scoped(OfflineMemory::try_gib(6)?, input.cancel, || {
                ensure_policy_replay_active(
                    input.purpose,
                    input.cancel,
                    "offline validation boundary",
                )?;
                let (structural_volatility_oos, structural_volatility_folds) =
                    evaluate_structural_volatility_oos(
                        &replay.structural_examples,
                        &replay.structural_executions,
                    )?;
                let evidence = evaluate_weather_policy_evidence(&WeatherEvidenceRequest {
                    profile: input.profile,
                    candidates: input.candidates,
                    experiment_family_hash: input.experiment_family_hash,
                    min_embargo_secs: embargo_secs,
                    replayed: &replay.replayed_examples,
                    structural_volatility_oos,
                    structural_volatility_folds,
                })?;
                let vertical_gate = evaluate_weather_proxy_gate(
                    &replay.gate_linkages,
                    &replay.gate_observations,
                    input.fit_window_start,
                    input.fit_window_end,
                    input.activation_target,
                )?;
                ensure_policy_replay_active(
                    input.purpose,
                    input.cancel,
                    "offline validation completion",
                )?;
                Ok((evidence, vertical_gate))
            })
            .await?;
        if input.activation_target != VerticalActivationTarget::ResearchOnly {
            evidence.all_gates_passed &= vertical_gate.passes(input.activation_target);
        }
        evidence.vertical_gate_evidence = vec![vertical_gate];
        Ok(WeatherPolicyRecomputeResult {
            evidence,
            experiment_family_hash: *input.experiment_family_hash,
            embargo_secs,
        })
    }

    async fn read_policy_source_slice(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> QuantResult<Option<PolicySourceSlice>> {
        let Some(policy) = self.policies.find(artifact_id).await? else {
            return Ok(None);
        };
        let dataset = self
            .datasets
            .find_by_id(&policy.source_dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: policy.source_dataset_id.to_string(),
            })?;
        if dataset.status != TrainingDatasetStatus::Ready
            || dataset.purpose != DatasetPurpose::PolicyFit
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "policy {artifact_id} must bind an immutable Ready PolicyFit Dataset"
                ),
            }
            .into());
        }
        let dataset_manifest =
            dataset
                .manifest
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: format!(
                        "policy {artifact_id} source dataset has no Dataset v3 manifest"
                    ),
                })?;
        if dataset_manifest
            .source_lineage
            .research_profile_artifact_id
            .profile_ref()
            != policy.payload_json.fit_contract.profile_ref
            || dataset_manifest.source_lineage.research_program_hash
                != policy.payload_json.fit_contract.research_program_hash
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "policy {artifact_id} source-slice lineage does not match the frozen fit contract"
                ),
            }
            .into());
        }
        let manifest_ref = dataset_manifest.source_lineage.source_slice;
        let bytes = self
            .evidence_verifier
            .read_manifest(&manifest_ref.manifest_uri, &manifest_ref.manifest_hash)
            .await?;
        let manifest = serde_json::from_slice::<SourceSliceManifest>(&bytes).map_err(|error| {
            ResearchError::Serialization {
                detail: format!("invalid policy Source Slice manifest: {error}"),
            }
        })?;
        manifest
            .validate()
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?;
        if manifest.profile_ref != policy.payload_json.fit_contract.profile_ref
            || manifest.research_program_hash
                != policy.payload_json.fit_contract.research_program_hash
            || manifest.decision_policy_snapshot_id
                != policy.payload_json.fit_contract.decision_policy_snapshot_id
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "policy {artifact_id} Source Slice manifest identity does not match the artifact"
                ),
            }
            .into());
        }
        let identity = SourceSliceIdentity::derive(SourceSliceIdentityInput {
            profile_ref: manifest.profile_ref.clone(),
            evaluation_track: manifest.evaluation_track,
            research_program_hash: manifest.research_program_hash,
            decision_policy_snapshot_id: manifest.decision_policy_snapshot_id,
            runtime_config_hash: manifest.runtime_config_hash,
            fit_seal_id: manifest.fit_seal_id,
            fit_seal_hash: manifest.fit_seal_hash,
            window_start: manifest.window_start,
            window_end: manifest.window_end,
            pit_cutoff: manifest.pit_cutoff,
        })?;
        let ledger = self
            .source_slices
            .find_by_identity(&identity.identity_hash)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "source_slice",
                id: identity.identity_hash.to_string(),
            })?;
        if ledger.status != SourceSliceStatus::Ready
            || ledger.manifest_uri.as_ref() != Some(&manifest_ref.manifest_uri)
            || ledger.manifest_hash.as_ref() != Some(&manifest_ref.manifest_hash)
            || ledger.manifest.as_ref() != Some(&manifest)
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "policy {artifact_id} Source Slice differs from its canonical Ready ledger binding"
                ),
            }
            .into());
        }
        Ok(Some(PolicySourceSlice {
            manifest_ref,
            manifest,
        }))
    }

    async fn verify_publish_validation_binding(
        &self,
        policy: &TradePolicyArtifactInfo,
    ) -> QuantResult<()> {
        let validation = self
            .policies
            .latest_successful_validation(&policy.artifact_id)
            .await?
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "publish requires a successful independent validation run".to_owned(),
            })?;
        let evidence_manifest_hash = policy
            .payload_json
            .evidence_bundle
            .as_ref()
            .map(|bundle| &bundle.manifest_hash)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "publish requires an immutable evidence bundle".to_owned(),
            })?;
        if validation.status != TradePolicyValidationStatus::Succeeded
            || validation.validation_hash.is_none()
            || validation.total_rows <= 0
            || validation.passed_rows != validation.total_rows
            || validation.failed_rows != 0
            || validation.artifact_id != policy.artifact_id
            || validation.artifact_hash != policy.content_hash
            || validation.source_dataset_id != policy.source_dataset_id
            || validation.source_dataset_hash != policy.payload_json.source_dataset_hash
            || &validation.evidence_manifest_hash != evidence_manifest_hash
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "latest validation run does not exactly bind the publish candidate"
                    .to_owned(),
            }
            .into());
        }

        let dataset = self
            .datasets
            .find_by_id(&policy.source_dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: policy.source_dataset_id.to_string(),
            })?;
        if dataset.status != TrainingDatasetStatus::Ready
            || dataset.purpose != DatasetPurpose::PolicyFit
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "publish requires the exact Ready PolicyFit dataset validated earlier"
                    .to_owned(),
            }
            .into());
        }
        let materialization = require_dataset_materialization(&dataset)?;
        if materialization.dataset_hash != &validation.source_dataset_hash
            || materialization
                .manifest
                .source_lineage
                .source_slice
                .manifest_hash
                != validation.source_slice_manifest_hash
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "Dataset or Source Slice identity changed after validation".to_owned(),
            }
            .into());
        }
        self.evidence_verifier
            .require_durable(materialization.parquet_uri)
            .await?;
        let dataset_bytes = self.artifacts.get(materialization.parquet_uri).await?;
        verify_frozen_dataset_artifact(&dataset, &dataset_bytes)?;

        let source_manifest = dataset
            .manifest
            .as_ref()
            .map(|manifest| &manifest.source_lineage.source_slice)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "Ready PolicyFit dataset has no Source Slice binding".to_owned(),
            })?;
        self.evidence_verifier
            .require_durable(&source_manifest.manifest_uri)
            .await?;
        let source_manifest_bytes = self
            .evidence_verifier
            .read_manifest(
                &source_manifest.manifest_uri,
                &source_manifest.manifest_hash,
            )
            .await?;
        let source_manifest = serde_json::from_slice::<SourceSliceManifest>(&source_manifest_bytes)
            .map_err(|error| ResearchError::Serialization {
                detail: format!("invalid Source Slice manifest during publish: {error}"),
            })?;
        source_manifest
            .validate()
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?;
        for object in &source_manifest.objects {
            self.evidence_verifier.require_durable(&object.uri).await?;
        }
        self.read_validation_source_slice(&dataset).await?;
        Ok(())
    }

    async fn persist_validation_rows(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
        expected: &BTreeMap<TradePolicyEvidenceObjectKind, Vec<PolicyEvidenceRecord>>,
        actual: &BTreeMap<TradePolicyEvidenceObjectKind, Vec<PolicyEvidenceRecord>>,
        examples: &[TrainingExample],
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<ValidationRowSummary> {
        const BATCH_SIZE: usize = 1_000;

        let total = validation_comparison_total(expected, actual)?;
        progress.report(ResearchJobProgress::with_total(
            "comparing_evidence_rows",
            0,
            total,
        ));
        let example_index = examples
            .iter()
            .map(|example| (example.example_id, example))
            .collect::<HashMap<_, _>>();
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut row_chain = Hasher::new();
        row_chain.update(b"trade_policy_validation_row_chain_v2\0");
        row_chain.update(validation_run_id.to_string().as_bytes());
        row_chain.update(b"\0");
        let mut total_rows = 0_i64;
        let mut passed_rows = 0_i64;
        let mut failed_rows = 0_i64;
        for kind in TradePolicyEvidenceObjectKind::REQUIRED {
            let expected_rows = index_policy_evidence(expected.get(&kind), kind, "sealed")?;
            let actual_rows = index_policy_evidence(actual.get(&kind), kind, "recomputed")?;
            let keys = expected_rows
                .keys()
                .chain(actual_rows.keys())
                .cloned()
                .collect::<BTreeSet<_>>();
            for record_key in keys {
                if total_rows % 256 == 0 {
                    ensure_validation_active(cancel, "during evidence row comparison")?;
                    let completed = u64::try_from(total_rows).map_err(|error| {
                        ResearchError::ValidationMethodology {
                            detail: format!("validation row progress does not fit u64: {error}"),
                        }
                    })?;
                    progress.report(ResearchJobProgress::with_total(
                        "comparing_evidence_rows",
                        completed,
                        total,
                    ));
                }
                let expected_record = expected_rows.get(&record_key).copied();
                let actual_record = actual_rows.get(&record_key).copied();
                let passed = matches!(
                    (expected_record, actual_record),
                    (Some(expected), Some(actual)) if expected == actual
                );
                let (diagnostic_kind, detail) = evidence_comparison_diagnostic(
                    kind,
                    &record_key,
                    expected_record,
                    actual_record,
                );
                let diagnostic_kind = diagnostic_kind.map(DiagnosticCode::new);
                let lineage_record = actual_record.or(expected_record).ok_or_else(|| {
                    ResearchError::ValidationMethodology {
                        detail: "validation evidence union produced an empty row".to_owned(),
                    }
                })?;
                let lineage = validation_row_lineage(kind, lineage_record, &example_index)?;
                let expected_row_hash = expected_record.map(|record| record.row_hash);
                let actual_row_hash = actual_record.map(|record| record.row_hash);
                let evidence_kind = evidence_kind_name(kind).to_owned();
                let row_hash = ResearchHasher::canonical(&(
                    "trade_policy_validation_row_v2",
                    validation_run_id,
                    total_rows,
                    &evidence_kind,
                    &record_key,
                    &lineage,
                    &expected_row_hash,
                    &actual_row_hash,
                    passed,
                    &diagnostic_kind,
                    &detail,
                ))?;
                let row_hash_text = row_hash.canonical_text();
                row_chain.update(row_hash_text.as_bytes());
                row_chain.update(b"\n");
                batch.push(NewTradePolicyValidationRow {
                    validation_run_id: *validation_run_id,
                    row_ordinal: total_rows,
                    evidence_kind,
                    record_key,
                    example_id: lineage.example_id,
                    market_id: lineage.market_id,
                    token_id: lineage.token_id,
                    decision_at: lineage.decision_at,
                    expected_row_hash,
                    actual_row_hash,
                    passed,
                    diagnostic_kind,
                    detail,
                    row_hash,
                });
                total_rows = total_rows.checked_add(1).ok_or_else(|| {
                    ResearchError::ValidationMethodology {
                        detail: "validation row ordinal overflow".to_owned(),
                    }
                })?;
                if passed {
                    passed_rows = passed_rows.checked_add(1).ok_or_else(|| {
                        ResearchError::ValidationMethodology {
                            detail: "validation passed-row count overflow".to_owned(),
                        }
                    })?;
                } else {
                    failed_rows = failed_rows.checked_add(1).ok_or_else(|| {
                        ResearchError::ValidationMethodology {
                            detail: "validation failed-row count overflow".to_owned(),
                        }
                    })?;
                }
                if batch.len() == BATCH_SIZE {
                    self.policies
                        .append_validation_rows(mem::take(&mut batch))
                        .await?;
                    batch.reserve(BATCH_SIZE);
                }
            }
        }
        if !batch.is_empty() {
            self.policies.append_validation_rows(batch).await?;
        }
        let persisted_total =
            u64::try_from(total_rows).map_err(|error| ResearchError::ValidationMethodology {
                detail: format!("persisted validation row count does not fit u64: {error}"),
            })?;
        progress.report(ResearchJobProgress::with_total(
            "comparing_evidence_rows",
            persisted_total,
            persisted_total,
        ));
        Ok(ValidationRowSummary {
            total_rows,
            passed_rows,
            failed_rows,
            row_chain_hash: content_hash_from_hasher(&row_chain),
        })
    }

    async fn require_validation_input_durability(
        &self,
        dataset: &TrainingDatasetInfo,
    ) -> QuantResult<()> {
        let materialization = require_dataset_materialization(dataset)?;
        self.evidence_verifier
            .require_durable(materialization.parquet_uri)
            .await?;
        let source_ref = &materialization.manifest.source_lineage.source_slice;
        self.evidence_verifier
            .require_durable(&source_ref.manifest_uri)
            .await?;
        let manifest_bytes = self
            .evidence_verifier
            .read_manifest(&source_ref.manifest_uri, &source_ref.manifest_hash)
            .await?;
        let manifest =
            serde_json::from_slice::<SourceSliceManifest>(&manifest_bytes).map_err(|error| {
                ResearchError::Serialization {
                    detail: format!(
                        "invalid Source Slice manifest at validation durability boundary: {error}"
                    ),
                }
            })?;
        manifest
            .validate()
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?;
        for object in &manifest.objects {
            self.evidence_verifier.require_durable(&object.uri).await?;
        }
        Ok(())
    }

    async fn load_validation_inputs(
        &self,
        artifact_id: &TradePolicyArtifactId,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<ValidationInputs> {
        ensure_validation_active(cancel, "before loading the Draft")?;
        progress.report(ResearchJobProgress::indeterminate("loading_draft", 0));
        let current =
            self.policies
                .find(artifact_id)
                .await?
                .ok_or_else(|| StorageError::NotFound {
                    entity: "trade_policy_artifact",
                    id: artifact_id.to_string(),
                })?;
        if current.status != TradePolicyStatus::Draft {
            return Err(StorageError::state_conflict(
                "trade_policy_artifact",
                Some(artifact_id),
                format!("validation requires Draft, got {}", current.status),
            )
            .into());
        }
        let actual_content_hash = ResearchHasher::canonical(&current.payload_json)?;
        if actual_content_hash != current.content_hash
            || TradePolicyArtifactId::from_content_hash(&actual_content_hash) != *artifact_id
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "trade-policy payload does not match its immutable content identity"
                    .to_owned(),
            }
            .into());
        }

        ensure_validation_active(cancel, "before reading the Dataset")?;
        progress.report(ResearchJobProgress::indeterminate("reading_dataset", 0));
        let dataset = self
            .datasets
            .find_by_id(&current.source_dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: current.source_dataset_id.to_string(),
            })?;
        if dataset.status != TrainingDatasetStatus::Ready
            || dataset.purpose != DatasetPurpose::PolicyFit
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "policy validation requires a Ready PolicyFit dataset, got {} {}",
                    dataset.status, dataset.purpose
                ),
            }
            .into());
        }
        let materialization = require_dataset_materialization(&dataset)?;
        if materialization.dataset_hash != &current.payload_json.source_dataset_hash {
            return Err(ResearchError::ValidationMethodology {
                detail: "policy source_dataset_hash differs from the immutable Dataset ledger"
                    .to_owned(),
            }
            .into());
        }
        let source_slice_manifest_hash = dataset
            .manifest
            .as_ref()
            .map(|manifest| manifest.source_lineage.source_slice.manifest_hash)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "Ready PolicyFit dataset has no manifest".to_owned(),
            })?;
        let evidence_manifest_hash = current
            .payload_json
            .evidence_bundle
            .as_ref()
            .map(|bundle| bundle.manifest_hash)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "trade policy has no evidence bundle".to_owned(),
            })?;
        self.require_validation_input_durability(&dataset).await?;
        let dataset_bytes = self.artifacts.get(materialization.parquet_uri).await?;
        let examples = verify_frozen_dataset_artifact(&dataset, &dataset_bytes)?;
        Ok(ValidationInputs {
            current,
            dataset,
            examples,
            source_slice_manifest_hash,
            evidence_manifest_hash,
        })
    }

    async fn validate_rows_and_evidence(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
        inputs: &ValidationInputs,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<ValidationRowSummary> {
        ensure_validation_active(cancel, "before reading Source Slice objects")?;
        progress.report(ResearchJobProgress::indeterminate(
            "verifying_source_slice",
            0,
        ));
        let source = self.read_validation_source_slice(&inputs.dataset).await?;
        source.require_replayable_validation_source()?;
        ensure_validation_active(cancel, "before evidence verification")?;
        progress.report(ResearchJobProgress::indeterminate(
            "verifying_evidence_bundle",
            0,
        ));
        let verified = self
            .evidence_verifier
            .verify(
                &inputs.current.payload_json,
                TradePolicyEvidenceDurability::Production,
            )
            .await?;
        let payload = &inputs.current.payload_json;
        payload
            .fit_contract
            .validate()
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?;
        let profile = payload
            .fit_contract
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?;
        let runtime_limits = self
            .runtime_policy_limits(&payload.fit_contract.decision_policy_snapshot_id, &profile)
            .await?;
        if runtime_limits.fit_model.model_version_id != payload.fit_contract.model_version_id
            || runtime_limits.methodology_hash != payload.fit_contract.methodology_hash
            || payload.fit_contract.methodology_hash != verified.manifest().methodology_hash
        {
            return Err(ResearchError::ValidationMethodology {
                detail:
                    "frozen model or methodology binding differs from the governed runtime config"
                        .to_owned(),
            }
            .into());
        }
        let candidates = canonicalize_policy_candidates(payload.candidates.clone())
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?;
        let candidate_set_hash = CanonicalDigest::content_hash_json(&candidates)?;
        if candidates != payload.candidates || candidate_set_hash != payload.candidate_set_hash {
            return Err(ResearchError::ValidationMethodology {
                detail: "trade-policy candidate family is not canonical or changed after Fit"
                    .to_owned(),
            }
            .into());
        }
        let materialization = require_dataset_materialization(&inputs.dataset)?;
        if materialization.feature_schema_hash != &payload.feature_schema_hash
            || materialization.label_schema_hash != &payload.label_schema_hash
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "Dataset schema hashes differ from the frozen policy artifact".to_owned(),
            }
            .into());
        }
        let experiment_family_hash =
            weather_experiment_family_hash(WeatherExperimentFamilyInput {
                profile_ref: &profile.profile_ref,
                evaluation_track: payload.fit_contract.evaluation_track,
                research_program_hash: &payload.fit_contract.research_program_hash,
                model_version_id: &payload.fit_contract.model_version_id,
                methodology_hash: &payload.fit_contract.methodology_hash,
                latency_profile_hash: &payload.fit_contract.latency_profile_hash,
                candidate_set_hash: &payload.candidate_set_hash,
                fit_window_start: payload.fit_contract.fit_window_start,
                fit_window_end: payload.fit_contract.fit_window_end,
            })?;
        let factor_schema_hash = materialization.factor_schema_hash();
        let recomputed = self
            .recompute_weather_policy(WeatherPolicyRecomputeInput {
                purpose: PolicyReplayPurpose::Validation,
                source: &source,
                examples: &inputs.examples,
                profile: &profile,
                candidates: &candidates,
                model_version_id: &payload.fit_contract.model_version_id,
                decision_policy_snapshot_id: &payload.fit_contract.decision_policy_snapshot_id,
                feature_schema_hash: materialization.feature_schema_hash,
                factor_schema_hash: &factor_schema_hash,
                experiment_family_hash: &experiment_family_hash,
                latency_profile: verified.latency_profile(),
                fit_window_start: payload.fit_contract.fit_window_start,
                fit_window_end: payload.fit_contract.fit_window_end,
                pit_cutoff: payload.fit_contract.pit_cutoff,
                activation_target: payload.activation_target,
                progress,
                cancel,
            })
            .await?;
        let recomputed_contract_passes = recomputed.evidence.all_gates_passed
            && recomputed.experiment_family_hash == experiment_family_hash
            && recomputed.embargo_secs == payload.embargo_secs
            && recomputed.evidence.cohorts == payload.cohorts
            && recomputed.evidence.vertical_gate_evidence == payload.vertical_gate_evidence
            && recomputed.evidence.structural_volatility_oos == payload.structural_volatility_oos;
        let mut actual_records = recomputed.evidence.records_by_kind()?;
        for records in actual_records.values_mut() {
            records.sort_by(|left, right| left.record_key.cmp(&right.record_key));
        }
        let row_summary = self
            .persist_validation_rows(
                validation_run_id,
                verified.records(),
                &actual_records,
                &inputs.examples,
                progress,
                cancel,
            )
            .await?;
        if !recomputed_contract_passes || row_summary.failed_rows > 0 {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "independent Weather replay contract_passes={recomputed_contract_passes}; {} of {} sealed evidence rows differ",
                    row_summary.failed_rows,
                    row_summary.total_rows
                ),
            }
            .into());
        }
        Ok(row_summary)
    }

    async fn complete_validation_run(
        &self,
        input: ValidationCompletionInput<'_>,
    ) -> QuantResult<TradePolicyArtifactInfo> {
        let validation_hash = ResearchHasher::canonical(&(
            "trade_policy_validation_v1",
            input.validation_run_id,
            input.artifact_id,
            &input.current.content_hash,
            &input.current.payload_json.source_dataset_hash,
            input.source_slice_manifest_hash,
            input.evidence_manifest_hash,
            input.row_summary.total_rows,
            input.row_summary.passed_rows,
            &input.row_summary.row_chain_hash,
        ))?;
        let (_, validated) = self
            .policies
            .complete_validation(
                input.validation_run_id,
                CompleteTradePolicyValidation {
                    total_rows: input.row_summary.total_rows,
                    passed_rows: input.row_summary.passed_rows,
                    validation_hash,
                    audit: NewTradePolicyGovernanceAudit {
                        audit_id: TradePolicyGovernanceAuditId::from_v7(),
                        artifact_id: *input.artifact_id,
                        action: TradePolicyGovernanceAction::Validate,
                        from_status: TradePolicyStatus::Draft,
                        to_status: TradePolicyStatus::Validated,
                        content_hash: input.current.content_hash,
                        actor_id: input.actor_id,
                        reason: input.reason,
                    },
                },
            )
            .await?;
        Ok(validated)
    }

    async fn record_validation_failure(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
        status: TradePolicyValidationStatus,
        detail: &str,
    ) -> QuantResult<()> {
        let bounded_detail = detail.chars().take(8_192).collect::<String>();
        let validation_hash = ResearchHasher::canonical(&(
            "trade_policy_validation_terminal_v1",
            validation_run_id,
            status,
            &bounded_detail,
        ))?;
        self.policies
            .fail_validation(
                validation_run_id,
                FailTradePolicyValidation {
                    status,
                    validation_hash,
                    failure_detail: bounded_detail,
                },
            )
            .await?;
        Ok(())
    }

    async fn evaluate_operational_preflight(
        &self,
        contract: &ContractPreflight,
    ) -> QuantResult<OperationalPreflight> {
        let required_raw_retention_days = contract
            .profile
            .as_ref()
            .map(|profile| {
                profile
                    .spec
                    .required_days()?
                    .checked_mul(2)
                    .map(|days| days.max(180))
                    .ok_or_else(|| "profile retention runway overflow".to_owned())
            })
            .transpose()
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?;
        let evidence = self.readiness.latest_verified(Utc::now()).await?;
        let latency_profile = evidence
            .latency
            .as_ref()
            .and_then(|item| match &item.payload_json {
                ResearchReadinessEvidencePayload::ShadowLatencyProfile(profile) => Some(profile),
                ResearchReadinessEvidencePayload::RetentionRunway(_) => None,
            });
        let latency_profile_present = contract
            .runtime_limits
            .as_ref()
            .zip(latency_profile)
            .is_some_and(|(limits, profile)| profile.complete_for(limits.min_latency_profile_secs));
        let retention = evidence
            .retention
            .as_ref()
            .and_then(|item| match &item.payload_json {
                ResearchReadinessEvidencePayload::RetentionRunway(retention) => Some(retention),
                ResearchReadinessEvidencePayload::ShadowLatencyProfile(_) => None,
            });
        let retention_runway_days = retention.and_then(|retention| retention.measured_history_days);
        let retention_runway_proven =
            retention
                .zip(required_raw_retention_days)
                .is_some_and(|(retention, required_days)| {
                    retention.required_days >= required_days
                        && retention
                            .measured_history_days
                            .is_some_and(|days| days >= required_days)
                        && retention.proven()
                });
        Ok(OperationalPreflight {
            evidence,
            latency_profile_present,
            retention_runway_days,
            required_raw_retention_days,
            retention_runway_proven,
        })
    }
}

struct TrialAttemptSpec {
    candidate_id: String,
    scope: TradePolicyTrialScope,
    fold_index: Option<i32>,
    path_index: Option<i32>,
    status: TradePolicyTrialStatus,
    metrics: Option<TradePolicyTrialMetrics>,
    evidence_kind: Option<TradePolicyEvidenceObjectKind>,
    failure_detail: Option<String>,
}

struct AggregatePolicyEvidence {
    cpcv_path_count: u32,
    deflated_sharpe_ratio: Decimal,
    probability_of_backtest_overfitting: Decimal,
    effective_sample_size: Decimal,
    ambiguous_touch_rate: Decimal,
    depth_failure_rate: Decimal,
    common_candidate_support: Decimal,
    fee_catalog_coverage: Decimal,
    passive_rebate_evidence_coverage: Decimal,
    eligible_market_coverage: Decimal,
}

fn build_fit_artifact_payload(
    request: &FitTradePolicyRequest,
    plan: &FrozenFitPlan,
    data: &FitDatasetInputs,
    sealed: SealedFitEvidence,
) -> QuantResult<TradePolicyArtifactPayload> {
    let SealedFitEvidence {
        evidence,
        embargo_secs,
        manifest,
        manifest_uri,
        manifest_hash,
        simulator_hash,
        replay_kernel_hash,
        catalog_ledger_hash,
        trial_ledger_hash,
    } = sealed;
    let (labels_matured_by_cutoff, labels_excluded_after_cutoff) = label_cutoff_counts(
        Some(plan.fit_window_start),
        Some(plan.fit_window_end),
        request.selection.pit_cutoff,
        Some(plan.profile.spec.target_horizon_secs),
        &data.examples,
    );
    let filtered_examples = data
        .examples
        .iter()
        .filter(|example| {
            example.decision_at() >= plan.fit_window_start
                && example.decision_at() < plan.fit_window_end
                && raw_trajectory_labels_matured(
                    &example.labels,
                    plan.profile.spec.target_horizon_secs,
                    example.decision_at(),
                    request.selection.pit_cutoff,
                ) == Some(true)
        })
        .map(|example| &example.example_id)
        .collect::<Vec<_>>();
    let full_l2_sample_count = u64::try_from(
        evidence
            .candidate_trials
            .iter()
            .filter(|trial| trial.full_l2_coverage.is_covered())
            .count(),
    )
    .map_err(|error| ResearchError::ValidationMethodology {
        detail: format!("full-L2 sample count does not fit u64: {error}"),
    })?;
    let trial_count = u64::try_from(evidence.candidate_trials.len()).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("candidate-trial count does not fit u64: {error}"),
        }
    })?;
    if trial_count == 0 {
        return Err(ResearchError::ValidationMethodology {
            detail: "policy evidence contains no candidate trials".to_owned(),
        }
        .into());
    }
    let aggregate = evidence.aggregate_policy_evidence()?;
    let fee_model_hash = CanonicalDigest::content_hash_json(
        &data
            .frozen_source
            .prefetched
            .clob_market_info
            .iter()
            .map(|version| &version.payload_hash)
            .collect::<BTreeSet<_>>(),
    )?;
    Ok(TradePolicyArtifactPayload {
        format_version: TRADE_POLICY_ARTIFACT_FORMAT_VERSION,
        activation_target: match request.evaluation_track {
            ResearchEvaluationTrack::ResearchOnly => VerticalActivationTarget::ResearchOnly,
            ResearchEvaluationTrack::SemiAutoCandidate => VerticalActivationTarget::SemiAuto,
        },
        fit_contract: TradePolicyFitContract {
            profile_ref: plan.profile.profile_ref.clone(),
            evaluation_track: request.evaluation_track,
            research_program_hash: plan.research_program_hash,
            source_dataset_id: data.source_dataset_id,
            model_version_id: plan.runtime_limits.fit_model.model_version_id,
            decision_policy_snapshot_id: plan.decision_policy_snapshot_id,
            fit_window_start: plan.fit_window_start,
            fit_window_end: plan.fit_window_end,
            pit_cutoff: request.selection.pit_cutoff,
            target_horizon_secs: plan.profile.spec.target_horizon_secs,
            cash_budget_tiers: plan.profile.spec.allowed_cash_budget_tiers.clone(),
            methodology_hash: plan.runtime_limits.methodology_hash,
            latency_evidence_id: data.latency_evidence_id,
            latency_profile_hash: data.latency_profile_hash,
            quality_gate: plan.profile.spec.quality_gate.clone(),
        },
        source_dataset_hash: data.dataset_hash,
        feature_schema_hash: data.feature_schema_hash,
        label_schema_hash: data.label_schema_hash,
        fill_simulator_version: EXECUTION_SEMANTICS_VERSION.to_owned(),
        embargo_secs,
        pit_cutoff_evidence: Some(TradePolicyPitCutoffEvidence {
            filtered_sample_count: u64::try_from(filtered_examples.len()).map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!("filtered sample count does not fit u64: {error}"),
                }
            })?,
            labels_matured_by_cutoff,
            labels_excluded_after_cutoff,
            filtered_sample_hash: ResearchHasher::canonical(&filtered_examples)?,
        }),
        execution_evidence: TradePolicyExecutionEvidence {
            entry_basis: Some(ExecutablePriceBasis::FullL2Vwap),
            exit_basis: Some(ExecutablePriceBasis::FullL2Vwap),
            full_l2_sample_count,
            full_l2_coverage: Some(
                Decimal::from(full_l2_sample_count) / Decimal::from(trial_count),
            ),
            fee_model_hash: Some(fee_model_hash),
            gaps: Vec::new(),
        },
        candidate_set_hash: manifest.candidate_set_hash,
        candidates: plan.candidates.clone(),
        evidence_bundle: Some(TradePolicyEvidenceBundleRef {
            manifest_uri,
            manifest_hash,
            simulator_hash,
            replay_kernel_hash,
            methodology_hash: plan.runtime_limits.methodology_hash,
            latency_evidence_id: data.latency_evidence_id,
            latency_profile_hash: data.latency_profile_hash,
            catalog_ledger_hash,
            source_slice_manifest_hash: data.source_slice_ref.manifest_hash,
            fit_job_id: manifest.fit_job_id,
            trial_ledger_hash,
        }),
        vertical_gate_evidence: evidence.vertical_gate_evidence,
        structural_volatility_oos: evidence.structural_volatility_oos,
        cohorts: evidence.cohorts,
        validation: policy_validation_evidence(
            &aggregate,
            &manifest,
            trial_ledger_hash,
            &plan.candidates,
        )?,
    })
}

fn policy_validation_evidence(
    aggregate: &AggregatePolicyEvidence,
    manifest: &TradePolicyEvidenceBundleManifest,
    trial_ledger_hash: ContentHash,
    candidates: &[TradePolicyCandidateSpec],
) -> QuantResult<TradePolicyValidationEvidence> {
    Ok(TradePolicyValidationEvidence {
        trial_ledger_cutoff: Some(manifest.trial_ledger_cutoff),
        trial_ledger_hash: Some(trial_ledger_hash),
        attempted_candidate_count: Some(u32::try_from(candidates.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("attempted candidate count does not fit u32: {error}"),
            }
        })?),
        cpcv_path_count: Some(aggregate.cpcv_path_count),
        deflated_sharpe_ratio: Some(aggregate.deflated_sharpe_ratio),
        probability_of_backtest_overfitting: Some(aggregate.probability_of_backtest_overfitting),
        effective_sample_size: Some(aggregate.effective_sample_size),
        ambiguous_touch_rate: Some(aggregate.ambiguous_touch_rate),
        depth_failure_rate: Some(aggregate.depth_failure_rate),
        common_candidate_support: Some(aggregate.common_candidate_support),
        fee_catalog_coverage: Some(aggregate.fee_catalog_coverage),
        passive_rebate_evidence_coverage: Some(aggregate.passive_rebate_evidence_coverage),
        eligible_market_coverage: Some(aggregate.eligible_market_coverage),
    })
}

impl WeatherPolicyEvidence {
    fn aggregate_policy_evidence(&self) -> QuantResult<AggregatePolicyEvidence> {
        let first = self
            .cohorts
            .first()
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "passing Weather fit has no fitted cohort".to_owned(),
            })?;
        let passive_rebate_evidence_coverage = self
            .cohorts
            .iter()
            .filter_map(|cohort| cohort.passive_rebate_evidence_coverage)
            .min()
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "passing Weather fit has no applicable Passive rebate evidence".to_owned(),
            })?;
        Ok(AggregatePolicyEvidence {
            cpcv_path_count: self
                .cohorts
                .iter()
                .map(|cohort| cohort.cpcv_path_count)
                .min()
                .unwrap_or(first.cpcv_path_count),
            deflated_sharpe_ratio: self
                .cohorts
                .iter()
                .map(|cohort| cohort.deflated_sharpe_ratio)
                .min()
                .unwrap_or(first.deflated_sharpe_ratio),
            probability_of_backtest_overfitting: self
                .cohorts
                .iter()
                .map(|cohort| cohort.probability_of_backtest_overfitting)
                .max()
                .unwrap_or(first.probability_of_backtest_overfitting),
            effective_sample_size: self
                .cohorts
                .iter()
                .map(|cohort| cohort.effective_sample_size)
                .min()
                .unwrap_or(first.effective_sample_size),
            ambiguous_touch_rate: self
                .cohorts
                .iter()
                .map(|cohort| cohort.ambiguous_touch_rate)
                .max()
                .unwrap_or(first.ambiguous_touch_rate),
            depth_failure_rate: self
                .cohorts
                .iter()
                .map(|cohort| cohort.depth_failure_rate)
                .max()
                .unwrap_or(first.depth_failure_rate),
            common_candidate_support: self
                .cohorts
                .iter()
                .map(|cohort| cohort.common_candidate_support)
                .min()
                .unwrap_or(first.common_candidate_support),
            fee_catalog_coverage: self
                .cohorts
                .iter()
                .map(|cohort| cohort.fee_catalog_coverage)
                .min()
                .unwrap_or(first.fee_catalog_coverage),
            passive_rebate_evidence_coverage,
            eligible_market_coverage: self
                .cohorts
                .iter()
                .map(|cohort| cohort.executable_coverage)
                .min()
                .unwrap_or(first.executable_coverage),
        })
    }
}

fn append_statistical_attempts(
    attempts: &mut Vec<TrialAttemptSpec>,
    run: &PolicyStatisticalRun,
    selected_trial: &TradePolicyCohortTrialRow,
    passed: bool,
) -> QuantResult<()> {
    let base_metrics = |expected_net_return_bps: Decimal,
                        risk_net_return_bps: Decimal,
                        expected_sharpe_ratio: Option<Decimal>|
     -> TradePolicyTrialMetrics {
        TradePolicyTrialMetrics {
            sample_count: run.summary.common_sample_count,
            effective_sample_size: run.summary.effective_sample_size,
            expected_net_return_bps,
            risk_net_return_bps,
            expected_sharpe_ratio,
            executable_coverage: selected_trial.executable_coverage,
            full_l2_coverage: selected_trial.full_l2_coverage,
            fee_catalog_coverage: selected_trial.fee_catalog_coverage,
            passive_rebate_evidence_coverage: selected_trial.passive_rebate_evidence_coverage,
            ambiguous_touch_rate: selected_trial.ambiguous_touch_rate,
            depth_failure_rate: selected_trial.depth_failure_rate,
            latency_stress_multiplier: run.latency_multiplier,
        }
    };
    for fold in &run.summary.cpcv_folds {
        attempts.push(TrialAttemptSpec {
            candidate_id: fold.selected_candidate_id.clone(),
            scope: TradePolicyTrialScope::Fold,
            fold_index: Some(i32::try_from(fold.fold_index).map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!("policy fold index does not fit i32: {error}"),
                }
            })?),
            path_index: None,
            status: TradePolicyTrialStatus::Succeeded,
            metrics: Some(base_metrics(
                fold.test_utility_bps,
                fold.test_risk_utility_bps,
                None,
            )),
            evidence_kind: Some(TradePolicyEvidenceObjectKind::StatisticalSummaries),
            failure_detail: None,
        });
    }
    for path in &run.summary.cpcv_paths {
        let expected_path_mean_bps = if path.expected_group_returns.is_empty() {
            Decimal::ZERO
        } else {
            path.expected_group_returns.iter().sum::<Decimal>()
                / Decimal::from(path.expected_group_returns.len())
                * Decimal::from(10_000)
        };
        let risk_path_mean_bps = if path.risk_group_returns.is_empty() {
            Decimal::ZERO
        } else {
            path.risk_group_returns.iter().sum::<Decimal>()
                / Decimal::from(path.risk_group_returns.len())
                * Decimal::from(10_000)
        };
        attempts.push(TrialAttemptSpec {
            candidate_id: run.summary.selected_candidate_id.clone(),
            scope: TradePolicyTrialScope::Path,
            fold_index: None,
            path_index: Some(i32::try_from(path.path_index).map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!("policy path index does not fit i32: {error}"),
                }
            })?),
            status: TradePolicyTrialStatus::Succeeded,
            metrics: Some(base_metrics(
                expected_path_mean_bps,
                risk_path_mean_bps,
                Some(path.expected_sharpe_ratio),
            )),
            evidence_kind: Some(TradePolicyEvidenceObjectKind::CpcvPaths),
            failure_detail: None,
        });
    }
    let selected = run
        .summary
        .candidate_performance
        .iter()
        .find(|candidate| candidate.candidate_id == run.summary.selected_candidate_id)
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "selected policy candidate has no performance summary".to_owned(),
        })?;
    attempts.push(TrialAttemptSpec {
        candidate_id: run.summary.selected_candidate_id.clone(),
        scope: TradePolicyTrialScope::LatencyStress,
        fold_index: None,
        path_index: None,
        status: if passed {
            TradePolicyTrialStatus::Succeeded
        } else {
            TradePolicyTrialStatus::Failed
        },
        metrics: Some(base_metrics(
            selected.weighted_mean_expected_return_bps,
            selected.weighted_mean_risk_return_bps,
            Some(selected.expected_sharpe_ratio),
        )),
        evidence_kind: Some(TradePolicyEvidenceObjectKind::StatisticalSummaries),
        failure_detail: (!passed).then(|| {
            format!(
                "{}x latency statistical or execution-quality gate failed",
                run.latency_multiplier
            )
        }),
    });
    Ok(())
}

fn evidence_object(
    objects: &PolicyEvidenceObjects,
    kind: TradePolicyEvidenceObjectKind,
) -> Option<&TradePolicyEvidenceObjectRef> {
    objects.objects.iter().find(|object| object.kind == kind)
}

const fn evidence_kind_slug(kind: TradePolicyEvidenceObjectKind) -> &'static str {
    match kind {
        TradePolicyEvidenceObjectKind::ObservationEligibility => "observation-eligibility",
        TradePolicyEvidenceObjectKind::Fills => "fills",
        TradePolicyEvidenceObjectKind::CandidateTrials => "candidate-trials",
        TradePolicyEvidenceObjectKind::CohortTrials => "cohort-trials",
        TradePolicyEvidenceObjectKind::CpcvPaths => "cpcv-paths",
        TradePolicyEvidenceObjectKind::CoverageGaps => "coverage-gaps",
        TradePolicyEvidenceObjectKind::StatisticalSummaries => "statistical-summaries",
        TradePolicyEvidenceObjectKind::VerticalGates => "vertical-gates",
        TradePolicyEvidenceObjectKind::StructuralVolatilityOos => "structural-volatility-oos",
    }
}

#[derive(serde::Serialize)]
struct ValidationRowLineage {
    example_id: Option<TrainingExampleId>,
    market_id: Option<MarketId>,
    token_id: Option<TokenId>,
    decision_at: Option<DateTime<Utc>>,
}

fn validation_comparison_total(
    expected: &BTreeMap<TradePolicyEvidenceObjectKind, Vec<PolicyEvidenceRecord>>,
    actual: &BTreeMap<TradePolicyEvidenceObjectKind, Vec<PolicyEvidenceRecord>>,
) -> QuantResult<u64> {
    TradePolicyEvidenceObjectKind::REQUIRED
        .iter()
        .try_fold(0_u64, |total, kind| {
            let union_count = expected
                .get(kind)
                .into_iter()
                .flatten()
                .chain(actual.get(kind).into_iter().flatten())
                .map(|record| &record.record_key)
                .collect::<BTreeSet<_>>()
                .len();
            let union_count = u64::try_from(union_count).map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!("validation evidence row count overflow: {error}"),
                }
            })?;
            total.checked_add(union_count).ok_or_else(|| {
                ResearchError::ValidationMethodology {
                    detail: "validation evidence total row count overflow".to_owned(),
                }
                .into()
            })
        })
}

fn index_policy_evidence<'a>(
    records: Option<&'a Vec<PolicyEvidenceRecord>>,
    kind: TradePolicyEvidenceObjectKind,
    source: &str,
) -> QuantResult<BTreeMap<String, &'a PolicyEvidenceRecord>> {
    let mut indexed = BTreeMap::new();
    for record in records.into_iter().flatten() {
        if indexed.insert(record.record_key.clone(), record).is_some() {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "{source} policy evidence {kind:?} contains duplicate key {}",
                    record.record_key
                ),
            }
            .into());
        }
    }
    Ok(indexed)
}

fn evidence_comparison_diagnostic(
    kind: TradePolicyEvidenceObjectKind,
    record_key: &str,
    expected: Option<&PolicyEvidenceRecord>,
    actual: Option<&PolicyEvidenceRecord>,
) -> (Option<String>, Option<String>) {
    match (expected, actual) {
        (Some(expected), Some(actual)) if expected == actual => (None, None),
        (Some(expected), Some(actual)) => (
            Some("evidence_row_mismatch".to_owned()),
            Some(format!(
                "{kind:?} record {record_key} differs: sealed={} recomputed={}",
                expected.row_hash, actual.row_hash
            )),
        ),
        (Some(expected), None) => (
            Some("recomputed_row_missing".to_owned()),
            Some(format!(
                "{kind:?} record {record_key} was sealed as {} but was not recomputed",
                expected.row_hash
            )),
        ),
        (None, Some(actual)) => (
            Some("unexpected_recomputed_row".to_owned()),
            Some(format!(
                "{kind:?} record {record_key} recomputed as {} but is absent from the sealed bundle",
                actual.row_hash
            )),
        ),
        (None, None) => (
            Some("evidence_union_invariant".to_owned()),
            Some(format!(
                "{kind:?} record {record_key} has no evidence on either side"
            )),
        ),
    }
}

fn validation_row_lineage(
    kind: TradePolicyEvidenceObjectKind,
    record: &PolicyEvidenceRecord,
    examples: &HashMap<TrainingExampleId, &TrainingExample>,
) -> QuantResult<ValidationRowLineage> {
    let lineage = match kind {
        TradePolicyEvidenceObjectKind::ObservationEligibility => {
            let row: TradePolicyObservationEligibilityRow = record.decode_typed()?;
            ValidationRowLineage {
                example_id: Some(row.example_id),
                market_id: Some(row.market_id),
                token_id: Some(row.token_id),
                decision_at: Some(row.decision_at),
            }
        }
        TradePolicyEvidenceObjectKind::Fills => {
            let row: TradePolicyFillEvidenceRow = record.decode_typed()?;
            validation_example_lineage(&row.example_id, examples, Some(row.filled_at))
        }
        TradePolicyEvidenceObjectKind::CandidateTrials => {
            let row: TradePolicyCandidateTrialRow = record.decode_typed()?;
            ValidationRowLineage {
                example_id: Some(row.example_id),
                market_id: Some(row.market_id),
                token_id: Some(row.token_id),
                decision_at: record.event_at,
            }
        }
        TradePolicyEvidenceObjectKind::CoverageGaps => {
            let row: TradePolicyCoverageGapRow = record.decode_typed()?;
            ValidationRowLineage {
                example_id: Some(row.example_id),
                market_id: Some(row.market_id),
                token_id: Some(row.token_id),
                decision_at: Some(row.decision_at),
            }
        }
        TradePolicyEvidenceObjectKind::CohortTrials
        | TradePolicyEvidenceObjectKind::CpcvPaths
        | TradePolicyEvidenceObjectKind::StatisticalSummaries
        | TradePolicyEvidenceObjectKind::VerticalGates
        | TradePolicyEvidenceObjectKind::StructuralVolatilityOos => ValidationRowLineage {
            example_id: None,
            market_id: None,
            token_id: None,
            decision_at: record.event_at,
        },
    };
    Ok(lineage)
}

fn validation_example_lineage(
    example_id: &TrainingExampleId,
    examples: &HashMap<TrainingExampleId, &TrainingExample>,
    event_at: Option<DateTime<Utc>>,
) -> ValidationRowLineage {
    let example = examples.get(example_id).copied();
    ValidationRowLineage {
        example_id: Some(*example_id),
        market_id: example.map(|example| example.market_id.clone()),
        token_id: example.map(|example| example.token_id.clone()),
        decision_at: event_at.or_else(|| example.map(TrainingExample::decision_at)),
    }
}

const fn evidence_kind_name(kind: TradePolicyEvidenceObjectKind) -> &'static str {
    match kind {
        TradePolicyEvidenceObjectKind::ObservationEligibility => "observation_eligibility",
        TradePolicyEvidenceObjectKind::Fills => "fills",
        TradePolicyEvidenceObjectKind::CandidateTrials => "candidate_trials",
        TradePolicyEvidenceObjectKind::CohortTrials => "cohort_trials",
        TradePolicyEvidenceObjectKind::CpcvPaths => "cpcv_paths",
        TradePolicyEvidenceObjectKind::CoverageGaps => "coverage_gaps",
        TradePolicyEvidenceObjectKind::StatisticalSummaries => "statistical_summaries",
        TradePolicyEvidenceObjectKind::VerticalGates => "vertical_gates",
        TradePolicyEvidenceObjectKind::StructuralVolatilityOos => "structural_volatility_oos",
    }
}

impl FrozenSourceSlice {
    fn require_replayable_validation_source(&self) -> QuantResult<()> {
        if !self.invalid_sessions.is_empty() {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "Source Slice contains {} invalid L2 sessions",
                    self.invalid_sessions.len()
                ),
            }
            .into());
        }
        if self.l2_ledger.is_empty() || self.prefetched.books.is_empty() {
            return Err(ResearchError::ValidationMethodology {
                detail: "Source Slice has no replayable L2 event/checkpoint evidence".to_owned(),
            }
            .into());
        }
        Ok(())
    }
}

fn require_fit_dataset(dataset: &TrainingDatasetInfo) -> QuantResult<()> {
    if dataset.status == TrainingDatasetStatus::Ready
        && dataset.purpose == DatasetPurpose::PolicyFit
    {
        return Ok(());
    }
    Err(StorageError::state_conflict(
        "quant_training_dataset",
        Some(&dataset.training_dataset_id),
        format!(
            "trade-policy fit requires a Ready PolicyFit Dataset, got {} {}",
            dataset.status, dataset.purpose
        ),
    )
    .into())
}

struct PreflightBlockerContext<'a> {
    request: &'a TradePolicyFitPreflightRequest,
    dataset_info: Option<&'a TrainingDatasetInfo>,
    contract: &'a ContractPreflight,
    dataset: &'a DatasetPreflight,
    source_slice_messages: &'a [String],
    dataset_link: Option<&'a str>,
    latency_profile_present: bool,
    latency_profile: Option<&'a ShadowLatencyProfileV1>,
    retention_runway_days: Option<u32>,
    required_raw_retention_days: Option<u32>,
    retention_runway_proven: bool,
}

fn preflight_blockers(
    context: &PreflightBlockerContext<'_>,
) -> Vec<TradePolicyPreflightBlockerView> {
    let mut blockers = Vec::new();
    append_contract_dataset_blockers(context, &mut blockers);
    append_source_operations_blockers(context, &mut blockers);
    blockers
}

fn append_contract_dataset_blockers(
    context: &PreflightBlockerContext<'_>,
    blockers: &mut Vec<TradePolicyPreflightBlockerView>,
) {
    let contract = context.contract;
    let dataset = context.dataset;
    push_blocker(
        blockers,
        contract.valid.is_pass(),
        TradePolicyPreflightBlockerDetail::ContractInvalid {
            diagnostics: contract.messages.clone(),
        },
        "Select a registered immutable profile and a canonical candidate family permitted by Runtime v1.",
        None,
    );
    push_blocker(
        blockers,
        contract.profile_fitter_available,
        TradePolicyPreflightBlockerDetail::ProfileFitterUnavailable {
            configured_fitter: contract
                .profile
                .as_ref()
                .and_then(|profile| profile.spec.policy_fitter),
        },
        "Select a profile with an explicitly implemented policy fitter.",
        None,
    );
    push_blocker(
        blockers,
        contract.profile_quality_gate_available,
        TradePolicyPreflightBlockerDetail::QualityGateUnavailable,
        "Select a registered immutable profile with a valid publication quality gate.",
        None,
    );
    if context.dataset_info.is_none() {
        return;
    }
    push_blocker(
        blockers,
        dataset.ready.is_pass(),
        TradePolicyPreflightBlockerDetail::DatasetNotReady {
            actual_status: context.dataset_info.map(|row| row.status),
        },
        "Build and integrity-verify the exact Dataset v3 source before fitting.",
        blocker_dataset_link(context),
    );
    push_blocker(
        blockers,
        dataset.policy_fit.is_pass(),
        TradePolicyPreflightBlockerDetail::DatasetPurposeMismatch {
            actual_purpose: context.dataset_info.map(|row| row.purpose),
        },
        "Select a Dataset v3 materialized specifically for PolicyFit.",
        blocker_dataset_link(context),
    );
    push_blocker(
        blockers,
        dataset.raw_trajectory_labels_present.is_pass(),
        TradePolicyPreflightBlockerDetail::RawTrajectoryLabelsMissing {
            labels_matured_by_cutoff: dataset.labels_matured_by_cutoff,
            labels_excluded_after_cutoff: dataset.labels_excluded_after_cutoff,
        },
        "Rebuild the source slice and Dataset v3 with mature row-level trajectory labels.",
        blocker_dataset_link(context),
    );
    push_blocker(
        blockers,
        dataset.profile_lineage_valid.is_pass(),
        TradePolicyPreflightBlockerDetail::ProfileLineageMismatch {
            actual_profile_ref: context
                .dataset_info
                .and_then(|row| row.manifest.as_ref())
                .map(|manifest| {
                    manifest
                        .source_lineage
                        .research_profile_artifact_id
                        .profile_ref()
                }),
            required_profile_ref: context.request.selection.profile_ref.clone(),
        },
        "Select or rebuild a dataset whose immutable profile reference matches exactly.",
        blocker_dataset_link(context),
    );
    push_blocker(
        blockers,
        dataset.source_slice_verified.is_pass(),
        TradePolicyPreflightBlockerDetail::SourceSliceUnverified {
            diagnostics: context.source_slice_messages.to_vec(),
        },
        "Materialize and hash-verify every required Source Slice v1 object.",
        blocker_dataset_link(context),
    );
    push_blocker(
        blockers,
        dataset.fit_window_contained.is_pass(),
        TradePolicyPreflightBlockerDetail::FitWindowNotContained {
            dataset_window_start: context.dataset_info.map(|row| row.window_start),
            dataset_window_end: context.dataset_info.map(|row| row.window_end),
            required_window_start: contract.fit_window_start,
            required_window_end: contract.fit_window_end,
        },
        "Build a dataset and source slice that contain the immutable profile fit span.",
        blocker_dataset_link(context),
    );
}

fn append_source_operations_blockers(
    context: &PreflightBlockerContext<'_>,
    blockers: &mut Vec<TradePolicyPreflightBlockerView>,
) {
    let contract = context.contract;
    let dataset = context.dataset;
    push_blocker(
        blockers,
        contract.pit_cutoff_not_future && dataset.pit_cutoff_valid.is_pass(),
        TradePolicyPreflightBlockerDetail::PitCutoffInvalid {
            pit_cutoff: context.request.selection.pit_cutoff,
            fit_window_end: contract.fit_window_end,
            not_future: contract.pit_cutoff_not_future,
        },
        "Choose a non-future PIT cutoff only after every required source is durably available.",
        blocker_dataset_link(context),
    );
    if context.dataset_info.is_some() {
        push_blocker(
            blockers,
            dataset.full_l2_trajectory_present.is_pass(),
            TradePolicyPreflightBlockerDetail::FullL2TrajectoryMissing,
            "Materialize continuous snapshot-rooted L2 sessions and finalized execution history.",
            blocker_dataset_link(context),
        );
        push_blocker(
            blockers,
            dataset.fee_model_present.is_pass(),
            TradePolicyPreflightBlockerDetail::PitFeeFactsMissing,
            "Backfill append-only CLOB market-info versions for every sample.",
            blocker_dataset_link(context),
        );
    }
    push_blocker(
        blockers,
        context.latency_profile_present,
        TradePolicyPreflightBlockerDetail::ProductionLatencyProfileMissing {
            observed_profile: context.latency_profile.cloned(),
        },
        "Capture, sign, and bind the latest complete 24-hour production latency profile.",
        None,
    );
    push_blocker(
        blockers,
        context.retention_runway_proven,
        TradePolicyPreflightBlockerDetail::RetentionRunwayUnproven {
            actual_runway_days: context.retention_runway_days,
            required_minimum_days: context.required_raw_retention_days,
        },
        "Capture signed ClickHouse Cloud raw-history evidence with monthly partitions, no unmanaged table TTL, and the required coverage window.",
        None,
    );
}

fn blocker_dataset_link(context: &PreflightBlockerContext<'_>) -> Option<String> {
    context.dataset_link.map(str::to_owned)
}

fn push_blocker(
    blockers: &mut Vec<TradePolicyPreflightBlockerView>,
    condition: bool,
    detail: TradePolicyPreflightBlockerDetail,
    remediation: &str,
    evidence_link: Option<String>,
) {
    if !condition {
        blockers.push(TradePolicyPreflightBlockerView {
            detail,
            remediation: remediation.to_owned(),
            evidence_link,
        });
    }
}

fn operational_evidence_view(
    info: &ResearchReadinessEvidenceInfo,
) -> TradePolicyOperationalEvidenceView {
    TradePolicyOperationalEvidenceView {
        evidence_id: info.evidence_id,
        kind: info.kind,
        payload_hash: info.payload_hash,
        artifact_version: info.artifact_version.clone(),
        attestation_key_id: info.attestation_key_id.clone(),
        observed_at: info.observed_at,
        expires_at: info.expires_at,
    }
}

fn contract_fit_window(
    profile: Option<&ResearchProfileArtifact>,
    pit_cutoff: DateTime<Utc>,
) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    let end = profile.and_then(|profile| {
        pit_cutoff.checked_sub_signed(ChronoDuration::seconds(
            i64::try_from(profile.spec.target_horizon_secs).ok()?,
        ))
    });
    let start = end.and_then(|end| {
        profile.and_then(|profile| {
            end.checked_sub_signed(ChronoDuration::days(i64::from(profile.spec.fit_span_days)))
        })
    });
    (start, end)
}

fn derive_research_program_hash(
    request: &TradePolicyFitPreflightRequest,
    profile: Option<&ResearchProfileArtifact>,
    candidates: Option<&[TradePolicyCandidateSpec]>,
    decision_policy_snapshot_id: Option<&DecisionPolicySnapshotId>,
    limits: Option<&RuntimePolicyLimits>,
) -> QuantResult<Option<ContentHash>> {
    profile
        .zip(candidates)
        .zip(limits)
        .map(|((profile, candidates), limits)| {
            CanonicalDigest::content_hash_json(&(
                &profile.profile_ref,
                request.evaluation_track,
                candidates,
                decision_policy_snapshot_id,
                &limits.runtime_config_hash,
                &limits.methodology_hash,
                &limits.fit_model.model_version_id,
                &limits.fit_model.model_spec_id,
                "source_slice_reader_v1",
                "source_slice_schema_v1",
                DATASET_ARTIFACT_FORMAT_VERSION,
            ))
            .map_err(Into::into)
        })
        .transpose()
}

fn derive_contract_source_identity(
    input: &ContractIdentityInput<'_>,
) -> QuantResult<Option<SourceSliceIdentity>> {
    let Some(profile) = input.profile else {
        return Ok(None);
    };
    let Some(program_hash) = input.research_program_hash else {
        return Ok(None);
    };
    let Some(decision_policy_snapshot_id) = input.decision_policy_snapshot_id else {
        return Ok(None);
    };
    let Some(limits) = input.runtime_limits else {
        return Ok(None);
    };
    let Some(start) = input.fit_window_start else {
        return Ok(None);
    };
    let Some(end) = input.fit_window_end else {
        return Ok(None);
    };
    let lookback = i64::try_from(profile.spec.max_feature_lookback_secs).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("profile lookback does not fit chrono seconds: {error}"),
        }
    })?;
    let horizon = i64::try_from(profile.spec.target_horizon_secs).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("profile horizon does not fit chrono seconds: {error}"),
        }
    })?;
    let window_start = start
        .checked_sub_signed(ChronoDuration::seconds(lookback))
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "source-slice lookback window overflows chrono".to_owned(),
        })?;
    let window_end = end
        .checked_add_signed(ChronoDuration::seconds(horizon))
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "source-slice horizon window overflows chrono".to_owned(),
        })?;
    SourceSliceIdentity::derive(SourceSliceIdentityInput {
        profile_ref: profile.profile_ref.clone(),
        evaluation_track: input.request.evaluation_track,
        research_program_hash: *program_hash,
        decision_policy_snapshot_id: *decision_policy_snapshot_id,
        runtime_config_hash: limits.runtime_config_hash,
        fit_seal_id: input.request.selection.fit_seal_id,
        fit_seal_hash: input.request.selection.fit_seal_hash,
        window_start,
        window_end,
        pit_cutoff: input.request.selection.pit_cutoff,
    })
    .map(Some)
    .map_err(Into::into)
}

fn append_preflight_diagnostics(
    contract: &mut ContractPreflight,
    dataset: &mut DatasetPreflight,
    operational: &mut OperationalPreflight,
) -> Vec<String> {
    let source_slice_messages = dataset.messages.clone();
    contract.messages.append(&mut dataset.messages);
    if !dataset.full_l2_trajectory_present.is_pass() {
        contract.messages.push(
            "source slice does not contain the complete L2/session/gap/finalized-execution replay contract"
                .to_owned(),
        );
    }
    if !dataset.fee_model_present.is_pass() {
        contract
            .messages
            .push("source slice does not contain PIT CLOB market-info fee facts".to_owned());
    }
    contract
        .messages
        .append(&mut operational.evidence.diagnostics);
    if !operational.latency_profile_present {
        contract.messages.push(
            "the latest signed shadow-latency evidence is missing one or more complete 24-hour dimensions"
                .to_owned(),
        );
    }
    if !operational.retention_runway_proven {
        contract.messages.push(
            "the latest signed retention evidence does not prove the required ClickHouse Cloud raw-history coverage"
                .to_owned(),
        );
    }
    source_slice_messages
}

const fn reusable_dataset_ready(
    dataset_info: Option<&TrainingDatasetInfo>,
    dataset: &DatasetPreflight,
) -> bool {
    dataset_info.is_some()
        && dataset.ready.is_pass()
        && dataset.policy_fit.is_pass()
        && dataset.raw_trajectory_labels_present.is_pass()
        && dataset.profile_lineage_valid.is_pass()
        && dataset.source_slice_verified.is_pass()
        && dataset.fit_window_contained.is_pass()
        && dataset.pit_cutoff_valid.is_pass()
        && dataset.full_l2_trajectory_present.is_pass()
        && dataset.fee_model_present.is_pass()
}

impl ContractPreflight {
    fn estimated_fit_work(&self) -> QuantResult<(u64, u64)> {
        let trials = self
            .canonical_candidates
            .as_ref()
            .and_then(|candidates| u64::try_from(candidates.len()).ok())
            .unwrap_or(0);
        let folds = trials.checked_mul(56).ok_or_else(|| {
            QuantError::from(ResearchError::ValidationMethodology {
                detail: "estimated policy fold-evaluation count overflow".to_owned(),
            })
        })?;
        Ok((trials, folds))
    }
}

fn assemble_preflight_view(
    request: &TradePolicyFitPreflightRequest,
    mut contract: ContractPreflight,
    source_slice_info: Option<&SourceSliceInfo>,
    dataset_info: Option<&TrainingDatasetInfo>,
    mut dataset: DatasetPreflight,
    mut operational: OperationalPreflight,
) -> QuantResult<TradePolicyFitPreflightView> {
    let (estimated_candidate_trials, estimated_fold_evaluations) = contract.estimated_fit_work()?;
    let source_slice_messages =
        append_preflight_diagnostics(&mut contract, &mut dataset, &mut operational);
    let reusable_dataset_ready = reusable_dataset_ready(dataset_info, &dataset);
    let operationally_ready = contract.valid.is_pass()
        && contract.profile_quality_gate_available
        && operational.latency_profile_present
        && operational.retention_runway_proven;
    let publishable_input =
        operationally_ready && (dataset_info.is_none() || reusable_dataset_ready);
    let methodology_hash = contract
        .runtime_limits
        .as_ref()
        .map(|limits| limits.methodology_hash);
    let dataset_link = dataset_info.map(|dataset| {
        format!(
            "/research/training-datasets/{}",
            dataset.training_dataset_id
        )
    });
    let latency_profile =
        operational
            .evidence
            .latency
            .as_ref()
            .and_then(|item| match &item.payload_json {
                ResearchReadinessEvidencePayload::ShadowLatencyProfile(profile) => Some(profile),
                ResearchReadinessEvidencePayload::RetentionRunway(_) => None,
            });
    let blocker_context = PreflightBlockerContext {
        request,
        dataset_info,
        contract: &contract,
        dataset: &dataset,
        source_slice_messages: &source_slice_messages,
        dataset_link: dataset_link.as_deref(),
        latency_profile_present: operational.latency_profile_present,
        latency_profile,
        retention_runway_days: operational.retention_runway_days,
        required_raw_retention_days: operational.required_raw_retention_days,
        retention_runway_proven: operational.retention_runway_proven,
    };
    let blockers = preflight_blockers(&blocker_context);
    let insufficient_history = operational
        .retention_runway_days
        .zip(operational.required_raw_retention_days)
        .is_some_and(|(observed, required)| observed < required);
    let readiness = if !blockers.is_empty() && insufficient_history {
        TradePolicyFitReadiness::BlockedInsufficientHistory
    } else if !blockers.is_empty() {
        TradePolicyFitReadiness::Blocked
    } else if reusable_dataset_ready {
        TradePolicyFitReadiness::Reusable
    } else {
        TradePolicyFitReadiness::ReadyToMaterialize
    };
    Ok(TradePolicyFitPreflightView {
        readiness,
        reusable_source_dataset_id: dataset_info.map(|dataset| dataset.training_dataset_id),
        profile: contract.profile,
        fit_window_start: contract.fit_window_start,
        fit_window_end: contract.fit_window_end,
        research_program_hash: contract.research_program_hash,
        source_slice_id: source_slice_info.map(|source_slice| source_slice.source_slice_id),
        source_slice_identity_hash: contract
            .source_slice_identity
            .as_ref()
            .map(|identity| identity.identity_hash),
        estimated_candidate_trials,
        estimated_fold_evaluations,
        catalog_completeness_proven: dataset.source_slice_verified.is_pass().into(),
        source_completeness_proven: (dataset.source_slice_verified.is_pass()
            && dataset.full_l2_trajectory_present.is_pass()
            && dataset.fee_model_present.is_pass())
        .into(),
        required_raw_retention_days: operational.required_raw_retention_days,
        retention_runway_days: operational.retention_runway_days,
        retention_runway_proven: operational.retention_runway_proven.into(),
        contract_valid: contract.valid.is_pass().into(),
        profile_fitter_available: contract.profile_fitter_available.into(),
        source_dataset_ready: dataset.ready.is_pass().into(),
        source_dataset_policy_fit: dataset.policy_fit.is_pass().into(),
        raw_trajectory_labels_present: dataset.raw_trajectory_labels_present.is_pass().into(),
        profile_lineage_valid: dataset.profile_lineage_valid.is_pass().into(),
        source_slice_verified: dataset.source_slice_verified.is_pass().into(),
        fit_window_contained: dataset.fit_window_contained.is_pass().into(),
        profile_quality_gate_available: contract.profile_quality_gate_available.into(),
        decision_policy_snapshot_id: contract.decision_policy_snapshot_id,
        methodology_hash,
        latency_profile_present: operational.latency_profile_present.into(),
        latency_evidence: operational
            .evidence
            .latency
            .as_ref()
            .map(operational_evidence_view),
        pit_cutoff_valid: (contract.pit_cutoff_not_future && dataset.pit_cutoff_valid.is_pass())
            .into(),
        labels_matured_by_cutoff: dataset.labels_matured_by_cutoff,
        labels_excluded_after_cutoff: dataset.labels_excluded_after_cutoff,
        full_l2_trajectory_present: dataset.full_l2_trajectory_present.is_pass().into(),
        fee_model_present: dataset.fee_model_present.is_pass().into(),
        retention_evidence: operational
            .evidence
            .retention
            .as_ref()
            .map(operational_evidence_view),
        publishable_input: publishable_input.into(),
        canonical_candidates: contract.canonical_candidates,
        candidate_set_hash: contract.candidate_set_hash,
        blockers,
    })
}

#[async_trait]
impl TradePolicyPort for TradePolicyService {
    fn list_profiles(&self) -> QuantResult<Vec<ResearchProfileArtifact>> {
        builtin_research_profiles()
            .map_err(|detail| ResearchError::ValidationMethodology { detail }.into())
    }

    fn find_profile(
        &self,
        id: &ResearchProfileId,
        version: u32,
    ) -> QuantResult<Option<ResearchProfileArtifact>> {
        Ok(self.list_profiles()?.into_iter().find(|profile| {
            profile.profile_ref.id == *id && profile.profile_ref.version == version
        }))
    }

    async fn preflight(
        &self,
        request: &TradePolicyFitPreflightRequest,
    ) -> QuantResult<TradePolicyFitPreflightView> {
        let contract = self.evaluate_contract(request).await?;
        let source_slice_info = self.find_source_slice(&contract).await?;
        let dataset_info = self.find_reusable_dataset(&contract).await?;
        let dataset = self
            .evaluate_dataset(DatasetEvaluationInput {
                selection: &request.selection,
                evaluation_track: request.evaluation_track,
                profile: contract.profile.as_ref(),
                research_program_hash: contract.research_program_hash.as_ref(),
                fit_window_start: contract.fit_window_start,
                fit_window_end: contract.fit_window_end,
                dataset: dataset_info.as_ref(),
                source_slice: source_slice_info.as_ref(),
            })
            .await?;
        let operational = self.evaluate_operational_preflight(&contract).await?;
        assemble_preflight_view(
            request,
            contract,
            source_slice_info.as_ref(),
            dataset_info.as_ref(),
            dataset,
            operational,
        )
    }

    async fn fit(
        &self,
        fit_job_id: &ResearchJobId,
        training_dataset_id: &TrainingDatasetId,
        request: FitTradePolicyRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TradePolicyArtifactInfo> {
        Box::pin(self.fit_policy_job(fit_job_id, training_dataset_id, request, progress, cancel))
            .await
    }
    async fn validate(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
        artifact_id: &TradePolicyArtifactId,
        actor_id: UserId,
        reason: String,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<TradePolicyArtifactInfo> {
        let inputs = self
            .load_validation_inputs(artifact_id, progress, cancel)
            .await?;
        let run = self
            .policies
            .begin_validation(NewTradePolicyValidationRun {
                validation_run_id: *validation_run_id,
                artifact_id: *artifact_id,
                artifact_hash: inputs.current.content_hash,
                source_dataset_id: inputs.dataset.training_dataset_id,
                source_dataset_hash: inputs.current.payload_json.source_dataset_hash,
                source_slice_manifest_hash: inputs.source_slice_manifest_hash,
                evidence_manifest_hash: inputs.evidence_manifest_hash,
                status: TradePolicyValidationStatus::Running,
                actor_id,
                reason: reason.clone(),
            })
            .await?;
        if run.status != TradePolicyValidationStatus::Running {
            return Err(StorageError::state_conflict(
                "quant_trade_policy_validation",
                Some(validation_run_id),
                format!("validation run is already terminal: {}", run.status),
            )
            .into());
        }
        let validation = async {
            let row_summary = self
                .validate_rows_and_evidence(validation_run_id, &inputs, progress, cancel)
                .await?;
            ensure_validation_active(cancel, "before the governance transition")?;
            progress.report(ResearchJobProgress::indeterminate(
                "committing_validation",
                u64::try_from(inputs.examples.len()).unwrap_or(u64::MAX),
            ));
            self.complete_validation_run(ValidationCompletionInput {
                validation_run_id,
                artifact_id,
                actor_id,
                reason,
                current: &inputs.current,
                source_slice_manifest_hash: &inputs.source_slice_manifest_hash,
                evidence_manifest_hash: &inputs.evidence_manifest_hash,
                row_summary: &row_summary,
            })
            .await
        }
        .await;
        match validation {
            Ok(validated) => Ok(validated),
            Err(error) => {
                let status = if cancel.is_cancelled() {
                    TradePolicyValidationStatus::Cancelled
                } else {
                    TradePolicyValidationStatus::Failed
                };
                self.record_validation_failure(validation_run_id, status, &error.to_string())
                    .await?;
                Err(error)
            }
        }
    }

    async fn find(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> QuantResult<Option<TradePolicyArtifactInfo>> {
        self.policies.find(artifact_id).await.map_err(Into::into)
    }

    async fn source_slice(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> QuantResult<Option<TradePolicySourceSliceView>> {
        let Some(source) = self.read_policy_source_slice(artifact_id).await? else {
            return Ok(None);
        };
        Ok(Some(TradePolicySourceSliceView {
            artifact_id: *artifact_id,
            profile_ref: source.manifest.profile_ref,
            source_slice: source.manifest_ref,
        }))
    }

    async fn page_source_slice_objects(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicySourceSliceObjectListQuery,
    ) -> QuantResult<Option<Paginated<TradePolicySourceSliceObjectView>>> {
        let Some(source) = self.read_policy_source_slice(artifact_id).await? else {
            return Ok(None);
        };
        let page = query.page.normalized();
        let rows = source
            .manifest
            .objects
            .into_iter()
            .filter(|object| query.kind.is_none_or(|kind| object.kind == kind))
            .collect::<Vec<_>>();
        let total =
            u64::try_from(rows.len()).map_err(|error| ResearchError::ValidationMethodology {
                detail: format!("Source Slice object count does not fit u64: {error}"),
            })?;
        let offset = usize::try_from(page.offset()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("Source Slice object page offset does not fit usize: {error}"),
            }
        })?;
        let limit = usize::try_from(page.limit()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("Source Slice object page size does not fit usize: {error}"),
            }
        })?;
        let items = rows
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(TradePolicySourceSliceObjectView::from)
            .collect();
        Ok(Some(Paginated::new(items, total, page.page, page.size)))
    }

    async fn evidence_download(
        &self,
        artifact_id: &TradePolicyArtifactId,
        kind: TradePolicyEvidenceObjectKind,
    ) -> QuantResult<Option<TradePolicyEvidenceDownloadView>> {
        const SIGNED_DOWNLOAD_SECS: u64 = 300;

        let Some(policy) = self.policies.find(artifact_id).await? else {
            return Ok(None);
        };
        let verified = self
            .evidence_verifier
            .verify(
                &policy.payload_json,
                TradePolicyEvidenceDurability::Production,
            )
            .await?;
        let object = verified
            .manifest()
            .objects
            .iter()
            .find(|object| object.kind == kind)
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: format!("policy {artifact_id} evidence object {kind:?} is missing"),
            })?;
        let url = self
            .artifacts
            .signed_download_url(&object.uri, StdDuration::from_secs(SIGNED_DOWNLOAD_SECS))
            .await?;
        Ok(Some(TradePolicyEvidenceDownloadView {
            artifact_id: *artifact_id,
            kind,
            byte_hash: object.byte_hash,
            row_count: object.row_count,
            expires_at: Utc::now() + ChronoDuration::seconds(300),
            url,
        }))
    }

    async fn page_evidence_rows(
        &self,
        artifact_id: &TradePolicyArtifactId,
        kind: TradePolicyEvidenceObjectKind,
        query: TradePolicyEvidenceRowListQuery,
    ) -> QuantResult<Option<Paginated<TradePolicyEvidenceRowView>>> {
        let Some(policy) = self.policies.find(artifact_id).await? else {
            return Ok(None);
        };
        let mut evidence = self
            .evidence_verifier
            .verify(
                &policy.payload_json,
                TradePolicyEvidenceDurability::ContentVerified,
            )
            .await?;
        let rows = evidence.records_mut().remove(&kind).ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: format!("policy {artifact_id} evidence object {kind:?} is missing"),
            }
        })?;
        let page = query.page.normalized();
        let total =
            u64::try_from(rows.len()).map_err(|error| ResearchError::ValidationMethodology {
                detail: format!("policy evidence row count does not fit u64: {error}"),
            })?;
        let offset = usize::try_from(page.offset()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("policy evidence page offset does not fit usize: {error}"),
            }
        })?;
        let limit = usize::try_from(page.limit()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("policy evidence page size does not fit usize: {error}"),
            }
        })?;
        let items = rows
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|record| TradePolicyEvidenceRowView {
                kind,
                record_key: record.record_key,
                event_at: record.event_at,
                payload: record.payload,
                row_hash: record.row_hash,
            })
            .collect();
        Ok(Some(Paginated::new(items, total, page.page, page.size)))
    }

    async fn page(
        &self,
        query: TradePolicyListQuery,
    ) -> QuantResult<Paginated<TradePolicyArtifactInfo>> {
        self.policies.page(query).await.map_err(Into::into)
    }

    async fn page_audits(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicyAuditListQuery,
    ) -> QuantResult<Paginated<TradePolicyGovernanceAuditInfo>> {
        self.policies
            .page_audits(artifact_id, query)
            .await
            .map_err(Into::into)
    }

    async fn page_trials(
        &self,
        fit_job_id: &ResearchJobId,
        query: TradePolicyTrialListQuery,
    ) -> QuantResult<Paginated<TradePolicyTrialAttemptInfo>> {
        let page = query.page.normalized();
        let rows = self
            .policies
            .list_trial_attempts(fit_job_id, None)
            .await?
            .into_iter()
            .filter(|row| {
                query
                    .candidate_id
                    .as_ref()
                    .is_none_or(|candidate_id| row.candidate_id.as_str() == candidate_id)
                    && query.scope.is_none_or(|scope| row.scope == scope)
                    && query.status.is_none_or(|status| row.status == status)
            })
            .collect::<Vec<_>>();
        let total =
            u64::try_from(rows.len()).map_err(|error| ResearchError::ValidationMethodology {
                detail: format!("trial ledger row count does not fit u64: {error}"),
            })?;
        let offset = usize::try_from(page.offset()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("trial ledger page offset does not fit usize: {error}"),
            }
        })?;
        let limit = usize::try_from(page.limit()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("trial ledger page size does not fit usize: {error}"),
            }
        })?;
        let items = rows.into_iter().skip(offset).take(limit).collect();
        Ok(Paginated::new(items, total, page.page, page.size))
    }

    async fn find_validation(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
    ) -> QuantResult<Option<TradePolicyValidationRunInfo>> {
        self.policies
            .find_validation(validation_run_id)
            .await
            .map_err(Into::into)
    }

    async fn page_validations(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicyValidationListQuery,
    ) -> QuantResult<Paginated<TradePolicyValidationRunInfo>> {
        self.policies
            .page_validations(artifact_id, query)
            .await
            .map_err(Into::into)
    }

    async fn page_validation_rows(
        &self,
        validation_run_id: &TradePolicyValidationRunId,
        query: TradePolicyValidationRowListQuery,
    ) -> QuantResult<Paginated<TradePolicyValidationRowInfo>> {
        self.policies
            .page_validation_rows(validation_run_id, query)
            .await
            .map_err(Into::into)
    }

    async fn transition(
        &self,
        artifact_id: &TradePolicyArtifactId,
        target: TradePolicyStatus,
        actor_id: UserId,
        reason: String,
    ) -> QuantResult<TradePolicyArtifactInfo> {
        let current =
            self.policies
                .find(artifact_id)
                .await?
                .ok_or_else(|| StorageError::NotFound {
                    entity: "trade_policy_artifact",
                    id: artifact_id.to_string(),
                })?;
        if target == TradePolicyStatus::Validated {
            return Err(ResearchError::ValidationMethodology {
                detail: "Draft → Validated is only available through the asynchronous independent validation job"
                    .to_owned(),
            }
            .into());
        }
        if target == TradePolicyStatus::Published {
            self.verify_publish_validation_binding(&current).await?;
            self.evidence_verifier
                .verify(
                    &current.payload_json,
                    TradePolicyEvidenceDurability::Production,
                )
                .await?;
        }
        let publication_blockers = current.payload_json.publication_blockers();
        if matches!(
            target,
            TradePolicyStatus::Validated | TradePolicyStatus::Published
        ) && !publication_blockers.is_empty()
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!("trade policy is not publishable: {publication_blockers:?}"),
            }
            .into());
        }
        let action = match target {
            TradePolicyStatus::Validated => TradePolicyGovernanceAction::Validate,
            TradePolicyStatus::Published => TradePolicyGovernanceAction::Publish,
            TradePolicyStatus::Retired => TradePolicyGovernanceAction::Retire,
            TradePolicyStatus::Draft => {
                return Err(ResearchError::ValidationMethodology {
                    detail: "trade-policy governance cannot transition back to Draft".to_owned(),
                }
                .into());
            }
        };
        self.policies
            .transition(
                artifact_id,
                current.status,
                target,
                NewTradePolicyGovernanceAudit {
                    audit_id: TradePolicyGovernanceAuditId::from_v7(),
                    artifact_id: *artifact_id,
                    action,
                    from_status: current.status,
                    to_status: target,
                    content_hash: current.content_hash,
                    actor_id,
                    reason,
                },
            )
            .await
            .map_err(Into::into)
    }
}

fn ensure_validation_active(cancel: &CancellationToken, stage: &str) -> QuantResult<()> {
    if cancel.is_cancelled() {
        return Err(ResearchError::Cancelled {
            detail: format!("trade-policy validation cancelled {stage}"),
        }
        .into());
    }
    Ok(())
}

fn ensure_fit_active(cancel: &CancellationToken, stage: &str) -> QuantResult<()> {
    if cancel.is_cancelled() {
        return Err(ResearchError::Cancelled {
            detail: format!("trade-policy fit cancelled {stage}"),
        }
        .into());
    }
    Ok(())
}

fn ensure_policy_replay_active(
    purpose: PolicyReplayPurpose,
    cancel: &CancellationToken,
    stage: &str,
) -> QuantResult<()> {
    match purpose {
        PolicyReplayPurpose::Fit => ensure_fit_active(cancel, stage),
        PolicyReplayPurpose::Validation => ensure_validation_active(cancel, stage),
    }
}

impl PolicyReplayPurpose {
    const fn replay_progress_stage(self) -> &'static str {
        match self {
            Self::Fit => "replaying",
            Self::Validation => "recomputing_replay",
        }
    }
}

fn collect_weather_replay_inputs(
    input: &WeatherPolicyRecomputeInput<'_>,
    signals: &FrozenPolicySignals,
) -> QuantResult<WeatherReplayInputs> {
    let fit_examples = input
        .examples
        .iter()
        .filter(|example| {
            example.decision_at() >= input.fit_window_start
                && example.decision_at() < input.fit_window_end
        })
        .cloned()
        .collect::<Vec<_>>();
    if fit_examples.is_empty() {
        return Err(ResearchError::ValidationMethodology {
            detail: "Weather replay has no Dataset rows inside the frozen fit window".to_owned(),
        }
        .into());
    }
    let total = u64::try_from(fit_examples.len()).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("Weather replay example count does not fit u64: {error}"),
        }
    })?;
    input.progress.report(ResearchJobProgress::with_total(
        (input.purpose).replay_progress_stage(),
        0,
        total,
    ));
    let structural_examples = fit_examples.clone();
    let mut by_market = BTreeMap::<_, Vec<TrainingExample>>::new();
    for example in fit_examples {
        by_market
            .entry(example.market_id.clone())
            .or_default()
            .push(example);
    }
    let (replay_window_start, replay_window_end) = weather_replay_window(input)?;
    let market_ids = by_market.keys().cloned().collect::<Vec<_>>();
    let mut replayed_examples = Vec::with_capacity(usize::try_from(total).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("Weather replay capacity does not fit usize: {error}"),
        }
    })?);
    let mut gate_linkages = BTreeMap::<ContentHash, MarketLinkage>::new();
    let mut gate_observations = BTreeMap::<ContentHash, WeatherObservationFact>::new();
    let mut structural_executions = BTreeMap::<ContentHash, ExecutionParticipantFactRow>::new();
    for market_chunk in market_ids.chunks(MAX_REPLAY_PAGE_MARKETS) {
        ensure_policy_replay_active(input.purpose, input.cancel, "during L2 replay")?;
        let page_examples = market_chunk
            .iter()
            .flat_map(|market_id| by_market.get(market_id).into_iter().flatten())
            .cloned()
            .collect::<Vec<_>>();
        let token_ids = page_examples
            .iter()
            .flat_map(|example| {
                iter::once(example.selected_market.primary_token_id.clone())
                    .chain(example.selected_market.secondary_token_id.clone())
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let page = input.source.replay_page(&ReplayPageRequest {
            market_ids: market_chunk.to_vec(),
            token_ids,
            window_start: replay_window_start,
            window_end: replay_window_end,
            available_by: input.pit_cutoff,
        })?;
        for linkage in &page.linkages {
            gate_linkages
                .entry(linkage.content_hash)
                .or_insert_with(|| linkage.clone());
        }
        for observation in &page.weather_observations {
            gate_observations
                .entry(observation.report_hash)
                .or_insert_with(|| observation.clone());
        }
        for execution in &page.finalized_executions {
            structural_executions
                .entry(CanonicalDigest::content_hash_json(execution)?)
                .or_insert_with(|| execution.clone());
        }
        replayed_examples.extend(replay_weather_page(&WeatherReplayRequest {
            page: &page,
            examples: &page_examples,
            signals,
            candidates: input.candidates,
            profile: input.profile,
            model_version_id: input.model_version_id,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            latency_profile: input.latency_profile,
        })?);
        let completed = u64::try_from(replayed_examples.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("Weather replay progress does not fit u64: {error}"),
            }
        })?;
        input.progress.report(ResearchJobProgress::with_total(
            (input.purpose).replay_progress_stage(),
            completed,
            total,
        ));
    }
    Ok(WeatherReplayInputs {
        structural_examples,
        replayed_examples,
        gate_linkages: gate_linkages.into_values().collect(),
        gate_observations: gate_observations.into_values().collect(),
        structural_executions: structural_executions.into_values().collect(),
    })
}

fn weather_replay_window(
    input: &WeatherPolicyRecomputeInput<'_>,
) -> QuantResult<(DateTime<Utc>, DateTime<Utc>)> {
    let lookback_secs =
        i64::try_from(input.profile.spec.max_feature_lookback_secs).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("Weather replay lookback does not fit chrono: {error}"),
            }
        })?;
    let horizon_secs = i64::try_from(input.profile.spec.target_horizon_secs).map_err(|error| {
        ResearchError::ValidationMethodology {
            detail: format!("Weather replay horizon does not fit chrono: {error}"),
        }
    })?;
    let start = input
        .fit_window_start
        .checked_sub_signed(ChronoDuration::seconds(lookback_secs))
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "Weather replay lookback overflows chrono".to_owned(),
        })?;
    let end = input
        .fit_window_end
        .checked_add_signed(ChronoDuration::seconds(horizon_secs))
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "Weather replay horizon overflows chrono".to_owned(),
        })?;
    Ok((start, end))
}

fn weather_embargo_secs(input: &WeatherPolicyRecomputeInput<'_>) -> QuantResult<u64> {
    let fit_span_secs = u64::try_from(
        (input.fit_window_end - input.fit_window_start).num_seconds(),
    )
    .map_err(|error| ResearchError::ValidationMethodology {
        detail: format!("Weather fit span does not fit u64: {error}"),
    })?;
    fit_span_secs
        .checked_mul(2)
        .and_then(|value| value.checked_add(99))
        .map(|value| value / 100)
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "Weather fit embargo overflows u64".to_owned(),
        })
        .map(|value| value.max(input.profile.spec.max_feature_lookback_secs))
        .map_err(Into::into)
}

fn label_cutoff_counts(
    fit_window_start: Option<DateTime<Utc>>,
    fit_window_end: Option<DateTime<Utc>>,
    pit_cutoff: DateTime<Utc>,
    target_horizon_secs: Option<u64>,
    examples: &[TrainingExample],
) -> (u64, u64) {
    let mut matured = 0_u64;
    let mut excluded = 0_u64;
    for example in examples.iter().filter(|example| {
        let at = example.decision_at();
        fit_window_start.is_some_and(|start| at >= start)
            && fit_window_end.is_some_and(|end| at < end)
    }) {
        let Some(target_horizon_secs) = target_horizon_secs else {
            continue;
        };
        match raw_trajectory_labels_matured(
            &example.labels,
            target_horizon_secs,
            example.decision_at(),
            pit_cutoff,
        ) {
            Some(true) => matured = matured.saturating_add(1),
            Some(false) => excluded = excluded.saturating_add(1),
            None => {}
        }
    }
    (matured, excluded)
}

fn raw_trajectory_labels_matured(
    labels: &[TrainingLabel],
    target_horizon_secs: u64,
    decision_at: DateTime<Utc>,
    pit_cutoff: DateTime<Utc>,
) -> Option<bool> {
    let required = [
        MAX_FAVORABLE_EXCURSION_BPS,
        MAX_ADVERSE_EXCURSION_BPS,
        LIQUIDITY_EXIT_POSSIBLE,
    ];
    let labels = required
        .iter()
        .map(|name| {
            labels.iter().find(|label| {
                label.label_name == *name
                    && label.horizon_secs == target_horizon_secs
                    && label.is_resolved
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(
        labels
            .iter()
            .all(|label| label.matured_at >= decision_at && label.matured_at <= pit_cutoff),
    )
}

#[cfg(test)]
fn label_visible_at_cutoff(
    fit_window_start: Option<DateTime<Utc>>,
    fit_window_end: Option<DateTime<Utc>>,
    pit_cutoff: DateTime<Utc>,
    decision_at: DateTime<Utc>,
    matured_at: DateTime<Utc>,
) -> bool {
    fit_window_start.is_some_and(|start| decision_at >= start)
        && fit_window_end.is_some_and(|end| decision_at < end)
        && matured_at <= pit_cutoff
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::types::TradePolicyEvidenceObjectKind;
    use quant_pivot_research::{
        policy_evidence::PolicyEvidenceRecord,
        training::{
            LIQUIDITY_EXIT_POSSIBLE, MAX_ADVERSE_EXCURSION_BPS, MAX_FAVORABLE_EXCURSION_BPS,
            TrainingLabel,
        },
    };
    use rust_decimal::Decimal;

    use super::{
        evidence_comparison_diagnostic, index_policy_evidence, label_visible_at_cutoff,
        raw_trajectory_labels_matured,
    };

    #[test]
    fn decision_inside_after_excluded() {
        let start = Utc.timestamp_opt(1_700_000_000, 0).single().expect("time");
        let end = start + Duration::days(10);
        let pit_cutoff = end + Duration::days(2);
        let decision_at = end - Duration::hours(1);

        assert!(!label_visible_at_cutoff(
            Some(start),
            Some(end),
            pit_cutoff,
            decision_at,
            pit_cutoff + Duration::seconds(1),
        ));
        assert!(label_visible_at_cutoff(
            Some(start),
            Some(end),
            pit_cutoff,
            decision_at,
            pit_cutoff,
        ));
    }

    #[test]
    fn raw_distinguishes_missing_rows() {
        let decision_at = Utc.timestamp_opt(1_700_000_000, 0).single().expect("time");
        let horizon_secs = 86_400;
        let matured_at = decision_at + Duration::seconds(86_400);
        let labels = [
            MAX_FAVORABLE_EXCURSION_BPS,
            MAX_ADVERSE_EXCURSION_BPS,
            LIQUIDITY_EXIT_POSSIBLE,
        ]
        .into_iter()
        .map(|label_name| TrainingLabel {
            label_name,
            horizon_secs,
            value: Decimal::ZERO,
            is_resolved: true,
            matured_at,
        })
        .collect::<Vec<_>>();

        assert_eq!(
            raw_trajectory_labels_matured(&labels[..2], horizon_secs, decision_at, matured_at,),
            None
        );
        assert_eq!(
            raw_trajectory_labels_matured(
                &labels,
                horizon_secs,
                decision_at,
                matured_at - Duration::seconds(1),
            ),
            Some(false)
        );
        assert_eq!(
            raw_trajectory_labels_matured(&labels, horizon_secs, decision_at, matured_at,),
            Some(true)
        );
    }

    #[test]
    fn independent_detects_missing_rows() {
        let sealed =
            PolicyEvidenceRecord::from_typed("candidate/key", None, &1_u32).expect("sealed record");
        let recomputed = PolicyEvidenceRecord::from_typed("candidate/key", None, &2_u32)
            .expect("recomputed record");

        let mismatch = evidence_comparison_diagnostic(
            TradePolicyEvidenceObjectKind::CandidateTrials,
            &sealed.record_key,
            Some(&sealed),
            Some(&recomputed),
        );
        assert_eq!(mismatch.0.as_deref(), Some("evidence_row_mismatch"));

        let missing = evidence_comparison_diagnostic(
            TradePolicyEvidenceObjectKind::CandidateTrials,
            &sealed.record_key,
            Some(&sealed),
            None,
        );
        assert_eq!(missing.0.as_deref(), Some("recomputed_row_missing"));
    }

    #[test]
    fn independent_rejects_duplicate_keys() {
        let record =
            PolicyEvidenceRecord::from_typed("candidate/key", None, &1_u32).expect("record");
        let records = vec![record.clone(), record];

        assert!(
            index_policy_evidence(
                Some(&records),
                TradePolicyEvidenceObjectKind::CandidateTrials,
                "sealed",
            )
            .is_err()
        );
    }
}
