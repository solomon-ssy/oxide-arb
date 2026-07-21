//! Core implementation of [`TrainingDatasetPort`] for the Admin API.

use std::{sync::Arc, time::Duration as StdDuration};

use async_trait::async_trait;
use blake3::Hasher;
use chrono::Duration;
use futures_util::StreamExt;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::{BuildTrainingDatasetRequest, TrainingDatasetPlanView, TrainingDatasetView},
        ports::{PolicyFitDatasetBuildRequest, TrainingDatasetPort},
        quant::{
            JobProgressSink, SourceSliceIdentity, SourceSliceIdentityInput, SourceSliceInfo,
            TrainingDatasetInfo,
        },
    },
    enums::quant::{DatasetPurpose, SourceSliceStatus, TrainingDatasetStatus},
    hashing::CanonicalDigest,
    runtime_config::DecisionPolicySnapshot,
    types::{
        ContentHash, DATASET_ARTIFACT_FORMAT_VERSION, DecisionPolicySnapshotId,
        ResearchEvaluationTrack, ResearchProfileArtifact, SourceSliceManifestRef,
        SourceSliceManifestV1, TrainingDatasetId, TrainingSampleSource,
        resolve_builtin_research_profile,
    },
};
use quant_pivot_repository::traits::{
    AttributionRepository, CalibrationArtifactRepository, CatalogLedgerRepository,
    ClobMarketInfoRepository, FeatureRepository, MarketLinkageRepository, MarketRepository,
    MarketSelectionRepository, ModelRegistryRepository, PolicyRepository, PositionRepository,
    QuantFactReadRepository, RecommendationRepository, SourceSliceRepository,
    TradePolicyRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{artifact::ArtifactStore, training::DatasetPlanRequest};
use tokio_util::sync::CancellationToken;

use crate::{
    app::bundles::ResearchBundle,
    prefetch::source_slice::{
        SourceSliceMaterializer, SourceSliceMaterializerDeps, SourceSliceReader,
    },
    service::{
        bias_table_fit::resolve_frozen_bias_table,
        training_dataset::{
            TrainingDatasetBuildConfig, TrainingDatasetService, TrainingDatasetServiceDeps,
            default_labelers,
        },
    },
};

/// Admin port wired from [`ResearchBundle`] plus runtime-config catalog reads.
pub struct CoreTrainingDatasetPort {
    fact_read: Arc<dyn QuantFactReadRepository>,
    catalog_repo: Arc<dyn CatalogLedgerRepository>,
    market_repo: Arc<dyn MarketRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    dataset_repo: Arc<dyn TrainingDatasetRepository>,
    attribution_repo: Arc<dyn AttributionRepository>,
    recommendation_repo: Arc<dyn RecommendationRepository>,
    feature_repo: Arc<dyn FeatureRepository>,
    selection_repo: Arc<dyn MarketSelectionRepository>,
    position_repo: Arc<dyn PositionRepository>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    model_registry: Arc<dyn ModelRegistryRepository>,
    trade_policy_repo: Arc<dyn TradePolicyRepository>,
    runtime_config: Arc<dyn PolicyRepository>,
    bias_table_repo: Arc<dyn CalibrationArtifactRepository>,
    source_slice_repo: Arc<dyn SourceSliceRepository>,
    clob_market_info_repo: Arc<dyn ClobMarketInfoRepository>,
    /// Deploy guard: hard cap on the deterministic historical spine.
    max_spine_samples: u64,
}

impl CoreTrainingDatasetPort {
    /// Assemble the port from an already-wired research bundle + deploy tunables.
    #[must_use]
    pub fn from_research(
        research: &ResearchBundle,
        runtime_config: Arc<dyn PolicyRepository>,
        bias_table_repo: Arc<dyn CalibrationArtifactRepository>,
        max_spine_samples: u64,
        _plan_sample_slices: u32,
        _plan_sample_markets: u32,
    ) -> Self {
        Self {
            fact_read: Arc::clone(&research.quant_fact_read),
            catalog_repo: Arc::clone(&research.catalog_ledger_repo),
            market_repo: Arc::clone(&research.market_repo),
            artifact_store: Arc::clone(&research.artifact_store),
            dataset_repo: Arc::clone(&research.training_dataset_repo),
            attribution_repo: Arc::clone(&research.attribution_repo),
            recommendation_repo: Arc::clone(&research.recommendation_repo),
            feature_repo: Arc::clone(&research.feature_repo),
            selection_repo: Arc::clone(&research.market_selection_repo),
            position_repo: Arc::clone(&research.position_repo),
            linkage_repo: Arc::clone(&research.market_linkage_repo),
            model_registry: Arc::clone(&research.model_registry_repo),
            trade_policy_repo: Arc::clone(&research.trade_policy_repo),
            runtime_config,
            bias_table_repo,
            source_slice_repo: Arc::clone(&research.source_slice_repo),
            clob_market_info_repo: Arc::clone(&research.clob_market_info_repo),
            max_spine_samples,
        }
    }

    async fn service_for(
        &self,
        decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    ) -> QuantResult<TrainingDatasetService> {
        let version = self
            .runtime_config
            .load_snapshot(decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "decision_policy_snapshot",
                id: decision_policy_snapshot_id.to_string(),
            })?;
        let runtime = version.snapshot;
        let bias_table = resolve_frozen_bias_table(
            self.bias_table_repo.as_ref(),
            &runtime.profile_artifacts.scoring.definition,
        )
        .await?;
        TrainingDatasetService::new(
            TrainingDatasetServiceDeps {
                fact_read: Arc::clone(&self.fact_read),
                catalog_repo: Arc::clone(&self.catalog_repo),
                market_repo: Arc::clone(&self.market_repo),
                artifact_store: Arc::clone(&self.artifact_store),
                dataset_repo: Arc::clone(&self.dataset_repo),
                attribution_repo: Arc::clone(&self.attribution_repo),
                recommendation_repo: Arc::clone(&self.recommendation_repo),
                feature_repo: Arc::clone(&self.feature_repo),
                selection_repo: Arc::clone(&self.selection_repo),
                position_repo: Arc::clone(&self.position_repo),
                clob_market_info_repo: Arc::clone(&self.clob_market_info_repo),
                linkage_repo: Arc::clone(&self.linkage_repo),
                model_registry: Arc::clone(&self.model_registry),
                trade_policy_repo: Arc::clone(&self.trade_policy_repo),
                calibration_repo: Arc::clone(&self.bias_table_repo),
            },
            TrainingDatasetBuildConfig {
                features: runtime.profile_artifacts.features.definition,
                factors: runtime.profile_artifacts.scoring.definition,
                domain: runtime.profile_artifacts.domain.definition,
                data_quality: runtime.recommendation.data_quality,
                training: runtime.profile_artifacts.research_method.training,
                selection: runtime.recommendation.selection,
                labelers: default_labelers(),
                bias_table,
            },
            self.max_spine_samples,
        )
    }

    async fn verify_source_slice_request(
        &self,
        request: &DatasetPlanRequest,
        verify_object_bytes: bool,
    ) -> QuantResult<()> {
        let profile = resolve_builtin_research_profile(&request.profile_ref)
            .map_err(|detail| ResearchError::DatasetPlan { detail })?;
        let bytes = self
            .artifact_store
            .get(&request.source_slice.manifest_uri)
            .await?;
        let actual_manifest_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(&bytes))?;
        if actual_manifest_hash != request.source_slice.manifest_hash {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "source-slice manifest byte hash mismatch: expected {}, got {actual_manifest_hash}",
                    request.source_slice.manifest_hash
                ),
            }
            .into());
        }
        let manifest =
            serde_json::from_slice::<SourceSliceManifestV1>(&bytes).map_err(|error| {
                ResearchError::DatasetPlan {
                    detail: format!("source-slice manifest is not valid v2 JSON: {error}"),
                }
            })?;
        manifest
            .validate_for_profile(
                &profile,
                &request.research_program_hash,
                request.window_start,
                request.window_end,
                request.pit_cutoff,
            )
            .map_err(|detail| ResearchError::DatasetPlan { detail })?;

        for object in &manifest.objects {
            let metadata = self.artifact_store.metadata(&object.uri).await?;
            if metadata.byte_size == 0 {
                return Err(ResearchError::DatasetPlan {
                    detail: format!("source-slice object {} is empty", object.uri),
                }
                .into());
            }
            if !verify_object_bytes {
                continue;
            }
            let mut stream = self.artifact_store.get_stream(&object.uri).await?;
            let mut hasher = Hasher::new();
            while let Some(chunk) = stream.next().await {
                hasher.update(&chunk?);
            }
            let actual = ContentHash::parse(format!("blake3:{}", hasher.finalize().to_hex()))?;
            if actual != object.byte_hash {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "source-slice object {} byte hash mismatch: expected {}, got {actual}",
                        object.uri, object.byte_hash
                    ),
                }
                .into());
            }
        }
        Ok(())
    }

    async fn resolve_source_slice(
        &self,
        identity: SourceSliceIdentity,
        profile: &ResearchProfileArtifact,
        runtime: &DecisionPolicySnapshot,
        materialize: Option<&CancellationToken>,
    ) -> QuantResult<SourceSliceInfo> {
        let source_slice = self
            .source_slice_repo
            .find_by_identity(&identity.identity_hash)
            .await?;
        let source_slice = match (source_slice, materialize) {
            (Some(source_slice), _) => source_slice,
            (None, Some(cancel)) => {
                SourceSliceMaterializer::new(
                    SourceSliceMaterializerDeps {
                        facts: Arc::clone(&self.fact_read),
                        catalog: Arc::clone(&self.catalog_repo),
                        clob_market_info: Arc::clone(&self.clob_market_info_repo),
                        linkage: Arc::clone(&self.linkage_repo),
                        calibration: Arc::clone(&self.bias_table_repo),
                        artifacts: Arc::clone(&self.artifact_store),
                        ledger: Arc::clone(&self.source_slice_repo),
                    },
                    runtime.profile_artifacts.domain.definition.clone(),
                    StdDuration::from_millis(
                        runtime
                            .profile_artifacts
                            .research_method
                            .training
                            .max_book_staleness_ms,
                    ),
                )
                .materialize(identity, profile, cancel)
                .await?
            }
            (None, None) => {
                return Err(ResearchError::DatasetPlan {
                    detail: format!(
                        "server-derived Source Slice {} is not materialized",
                        identity.identity_hash
                    ),
                }
                .into());
            }
        };
        if source_slice.status != SourceSliceStatus::Ready {
            return Err(StorageError::state_conflict(
                "quant_source_slice",
                Some(&source_slice.source_slice_id),
                format!("source slice is {}", source_slice.status),
            )
            .into());
        }
        Ok(source_slice)
    }

    async fn resolve_plan_request(
        &self,
        body: &BuildTrainingDatasetRequest,
        training_dataset_id: Option<TrainingDatasetId>,
        materialize: Option<&CancellationToken>,
        policy_fit: Option<FrozenPolicyFitProgram<'_>>,
    ) -> QuantResult<(DatasetPlanRequest, SourceSliceInfo)> {
        if body.purpose == DatasetPurpose::PolicyFit && policy_fit.is_none() {
            return Err(ResearchError::DatasetPlan {
                detail:
                    "PolicyFit datasets are internal artifacts of the trade-policy fit workflow"
                        .to_owned(),
            }
            .into());
        }
        if body.purpose != DatasetPurpose::PolicyFit && policy_fit.is_some() {
            return Err(ResearchError::DatasetPlan {
                detail: "internal PolicyFit authority requires purpose=policy_fit".to_owned(),
            }
            .into());
        }
        let profile = resolve_builtin_research_profile(&body.profile_ref)
            .map_err(|detail| ResearchError::DatasetPlan { detail })?;
        let runtime = self
            .runtime_config
            .load_snapshot(&body.decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "decision_policy_snapshot",
                id: body.decision_policy_snapshot_id.to_string(),
            })?;
        let frozen_runtime = runtime.snapshot;
        let mut sample_sources = body.sample_sources.clone();
        sample_sources.sort_by_key(|source| sample_source_order(*source));
        sample_sources.dedup();
        let (identity, research_program_hash) = derive_dataset_source_identity(
            body,
            &profile,
            &runtime.snapshot_hash,
            policy_fit,
            &sample_sources,
        )?;
        let source_slice = self
            .resolve_source_slice(identity, &profile, &frozen_runtime, materialize)
            .await?;
        let source_slice_ref = SourceSliceManifestRef {
            manifest_uri: source_slice.manifest_uri.clone().ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("quant_source_slice"),
                    "ready source slice has no manifest URI",
                )
            })?,
            manifest_hash: source_slice.manifest_hash.clone().ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("quant_source_slice"),
                    "ready source slice has no manifest hash",
                )
            })?,
        };
        Ok((
            DatasetPlanRequest {
                model_spec_id: body.model_spec_id.clone(),
                profile_ref: body.profile_ref.clone(),
                research_program_hash,
                source_slice: source_slice_ref,
                decision_policy_snapshot_id: body.decision_policy_snapshot_id.clone(),
                window_start: body.window_start,
                window_end: body.window_end,
                pit_cutoff: body.pit_cutoff,
                sample_interval_secs: body.sample_interval_secs,
                horizons_secs: body.horizons_secs.clone(),
                knowledge_lag_secs: body.knowledge_lag_secs,
                feature_schema_version: body.feature_schema_version,
                sample_sources,
                training_dataset_id,
                purpose: body.purpose,
            },
            source_slice,
        ))
    }
}

#[derive(Clone, Copy)]
struct FrozenPolicyFitProgram<'a> {
    evaluation_track: ResearchEvaluationTrack,
    research_program_hash: &'a ContentHash,
}

fn derive_dataset_source_identity(
    body: &BuildTrainingDatasetRequest,
    profile: &ResearchProfileArtifact,
    runtime_config_hash: &ContentHash,
    policy_fit: Option<FrozenPolicyFitProgram<'_>>,
    sample_sources: &[TrainingSampleSource],
) -> QuantResult<(SourceSliceIdentity, ContentHash)> {
    let research_program_hash = match policy_fit {
        Some(frozen) => frozen.research_program_hash.clone(),
        None => CanonicalDigest::content_hash_json(&(
            "training_dataset_program_v1",
            &profile.profile_ref,
            &body.model_spec_id,
            body.purpose,
            &body.decision_policy_snapshot_id,
            runtime_config_hash,
            body.window_start,
            body.window_end,
            body.pit_cutoff,
            body.sample_interval_secs,
            &body.horizons_secs,
            body.knowledge_lag_secs,
            body.feature_schema_version,
            sample_sources,
            DATASET_ARTIFACT_FORMAT_VERSION,
        ))?,
    };
    let max_horizon_secs = body
        .horizons_secs
        .iter()
        .copied()
        .max()
        .map(|horizon| horizon.max(profile.spec.target_horizon_secs))
        .ok_or_else(|| ResearchError::DatasetPlan {
            detail: "dataset requires at least one label horizon".to_owned(),
        })?;
    let source_window_start = body
        .window_start
        .checked_sub_signed(Duration::seconds(
            i64::try_from(profile.spec.max_feature_lookback_secs).map_err(|error| {
                ResearchError::DatasetPlan {
                    detail: format!("profile lookback does not fit chrono seconds: {error}"),
                }
            })?,
        ))
        .ok_or_else(|| ResearchError::DatasetPlan {
            detail: "source-slice lookback window overflows chrono".to_owned(),
        })?;
    let source_window_end = body
        .window_end
        .checked_add_signed(Duration::seconds(i64::try_from(max_horizon_secs).map_err(
            |error| ResearchError::DatasetPlan {
                detail: format!("dataset horizon does not fit chrono seconds: {error}"),
            },
        )?))
        .ok_or_else(|| ResearchError::DatasetPlan {
            detail: "source-slice horizon window overflows chrono".to_owned(),
        })?;
    if source_window_end > body.pit_cutoff {
        return Err(ResearchError::DatasetPlan {
            detail: "PIT cutoff must be at or after the complete label horizon".to_owned(),
        }
        .into());
    }
    let identity = SourceSliceIdentity::derive(SourceSliceIdentityInput {
        profile_ref: profile.profile_ref.clone(),
        evaluation_track: policy_fit.map_or(ResearchEvaluationTrack::ResearchOnly, |frozen| {
            frozen.evaluation_track
        }),
        research_program_hash: research_program_hash.clone(),
        decision_policy_snapshot_id: body.decision_policy_snapshot_id.clone(),
        runtime_config_hash: runtime_config_hash.clone(),
        window_start: source_window_start,
        window_end: source_window_end,
        pit_cutoff: body.pit_cutoff,
    })?;
    Ok((identity, research_program_hash))
}

const fn sample_source_order(source: TrainingSampleSource) -> u8 {
    match source {
        TrainingSampleSource::HistoricalPit => 0,
        TrainingSampleSource::LiveAttribution => 1,
        TrainingSampleSource::ExitDecision => 2,
    }
}

#[async_trait]
impl TrainingDatasetPort for CoreTrainingDatasetPort {
    async fn find_by_id(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> QuantResult<Option<TrainingDatasetInfo>> {
        self.dataset_repo
            .find_by_id(training_dataset_id)
            .await
            .map_err(QuantError::from)
    }

    async fn plan(
        &self,
        request: BuildTrainingDatasetRequest,
    ) -> QuantResult<TrainingDatasetPlanView> {
        let (plan_request, source_slice) = self
            .resolve_plan_request(&request, None, None, None)
            .await?;
        self.verify_source_slice_request(&plan_request, false)
            .await?;
        let frozen = SourceSliceReader::new(Arc::clone(&self.artifact_store))
            .read(&source_slice)
            .await?;
        let service = self
            .service_for(&request.decision_policy_snapshot_id)
            .await?;
        let plan = service
            .plan_with_frozen_source(plan_request, &frozen)
            .await?;
        let planned_samples = service.count_planned_samples(&plan).await?;
        Ok(TrainingDatasetPlanView {
            training_dataset_id: plan.training_dataset_id,
            model_spec_id: request.model_spec_id,
            model_spec_definition_hash: plan.model_spec_definition_hash,
            decision_policy_snapshot_id: request.decision_policy_snapshot_id,
            window_start: request.window_start,
            window_end: request.window_end,
            planned_samples,
            spine_upper_bound: planned_samples,
            hard_cap_exceeded: planned_samples > self.max_spine_samples,
            estimated_eligible_samples: planned_samples,
            keep_rate: None,
            keep_rate_sample_size: 0,
        })
    }

    async fn build(
        &self,
        request: BuildTrainingDatasetRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainingDatasetView> {
        let (plan_request, source_slice) = self
            .resolve_plan_request(
                &request,
                request.training_dataset_id.clone(),
                Some(&cancel),
                None,
            )
            .await?;
        self.verify_source_slice_request(&plan_request, true)
            .await?;
        let frozen = SourceSliceReader::new(Arc::clone(&self.artifact_store))
            .read(&source_slice)
            .await?;
        // Effectively-once recovery: completed artifacts are returned as-is;
        // planned/building rows are validated and resumed by the service.
        if let Some(training_dataset_id) = &request.training_dataset_id
            && let Some(existing) = self.dataset_repo.find_by_id(training_dataset_id).await?
        {
            match existing.status {
                TrainingDatasetStatus::Ready | TrainingDatasetStatus::InsufficientLabels => {
                    return Ok(TrainingDatasetView::from(existing));
                }
                TrainingDatasetStatus::Failed | TrainingDatasetStatus::Expired => {
                    return Err(StorageError::state_conflict(
                        "quant_training_dataset",
                        Some(training_dataset_id),
                        format!(
                            "dataset build cannot resume from terminal status {}",
                            existing.status
                        ),
                    )
                    .into());
                }
                TrainingDatasetStatus::Planned | TrainingDatasetStatus::Building => {}
            }
        }
        let service = self
            .service_for(&request.decision_policy_snapshot_id)
            .await?;
        let plan = service
            .plan_with_frozen_source(plan_request, &frozen)
            .await?;
        let training_dataset_id = plan.training_dataset_id.clone();
        Box::pin(service.build_with_frozen_source(plan, frozen, progress, cancel)).await?;
        let info = self
            .dataset_repo
            .find_by_id(&training_dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: training_dataset_id.to_string(),
            })?;
        Ok(TrainingDatasetView::from(info))
    }

    async fn build_policy_fit(
        &self,
        request: PolicyFitDatasetBuildRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainingDatasetView> {
        let frozen = FrozenPolicyFitProgram {
            evaluation_track: request.evaluation_track,
            research_program_hash: &request.research_program_hash,
        };
        let body = request.dataset;
        let (plan_request, source_slice) = self
            .resolve_plan_request(
                &body,
                body.training_dataset_id.clone(),
                Some(&cancel),
                Some(frozen),
            )
            .await?;
        self.verify_source_slice_request(&plan_request, true)
            .await?;
        let frozen_source = SourceSliceReader::new(Arc::clone(&self.artifact_store))
            .read(&source_slice)
            .await?;
        if let Some(training_dataset_id) = &body.training_dataset_id
            && let Some(existing) = self.dataset_repo.find_by_id(training_dataset_id).await?
        {
            match existing.status {
                TrainingDatasetStatus::Ready | TrainingDatasetStatus::InsufficientLabels => {
                    return Ok(TrainingDatasetView::from(existing));
                }
                TrainingDatasetStatus::Failed | TrainingDatasetStatus::Expired => {
                    return Err(StorageError::state_conflict(
                        "quant_training_dataset",
                        Some(training_dataset_id),
                        format!(
                            "PolicyFit Dataset build cannot resume from terminal status {}",
                            existing.status
                        ),
                    )
                    .into());
                }
                TrainingDatasetStatus::Planned | TrainingDatasetStatus::Building => {}
            }
        }
        let service = self.service_for(&body.decision_policy_snapshot_id).await?;
        let plan = service
            .plan_with_frozen_source(plan_request, &frozen_source)
            .await?;
        let training_dataset_id = plan.training_dataset_id.clone();
        Box::pin(service.build_with_frozen_source(plan, frozen_source, progress, cancel)).await?;
        let info = self
            .dataset_repo
            .find_by_id(&training_dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: training_dataset_id.to_string(),
            })?;
        Ok(TrainingDatasetView::from(info))
    }
}
