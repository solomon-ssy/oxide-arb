//! Side-effect-free verification of complete model-serving preimages.

use std::{
    collections::{BTreeSet, HashSet},
    future::Future,
    sync::Arc,
};

use futures_util::future::BoxFuture;
use quant_pivot_compute::{ComputeExecutor, OfflineMemoryLease};
use quant_pivot_error::{
    QuantResult, feedback::FeedbackError, research::ResearchError, storage::StorageError,
};
use quant_pivot_models::{
    domain::{
        governance::DecisionPolicySnapshotInfo,
        quant::{
            CalibrationArtifactInfo, FeedbackCycleInfo, ModelSpecInfo, ModelVersionInfo,
            SourceSliceIdentity, SourceSliceIdentityInput, TrainingDatasetInfo,
            TrainingDatasetMaterialization,
        },
        query::TimeWindow,
    },
    enums::{
        common::MarketCategory,
        domain::DomainFamily,
        factor::FactorFamily,
        quant::{CalibrationKind, DatasetPurpose, SourceSliceStatus, TrainingDatasetStatus},
    },
    hashing::CanonicalDigest,
    types::{
        CalibrationArtifactId, ModelInputContract, ModelVersionId, ResearchProfileArtifact,
        SourceSliceManifest,
        calibration::{
            ModelScoreCalibrationDatasetBinding, ModelScoreCalibrationFitContract,
            ModelScoreCalibrationModelBinding, ModelScoreCalibrationPolicyBinding,
        },
        factor::FactorServingPlane,
        model_lineage::ModelVersionDerivation,
        model_serving::{ModelServingContract, ModelServingPolicySnapshotBinding},
        stable_name::FeatureName,
    },
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, ModelRegistryRepository, PolicyRepository,
    SourceSliceRepository, TrainingDatasetRepository,
};
#[cfg(feature = "ml-classical")]
use quant_pivot_research::model::ClassicalRuntime;
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    factors::FactorEngine,
    features::{ExecutableFeatureSchema, SourceRequirement},
    hashing::ResearchHasher,
    model::{
        FavoriteLongshotBiasTable, ModelArtifact, QuantModelRuntime, ResolvedCalibration,
        ReturnModelSpec, SellScorerRuntime, WeightedFactorRuntime, WeightedSellScorerRuntime,
        artifact::ModelPayload, model_input_contract_hash,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{
    governance::{
        calibration_loader::VerifiedModelScoreCalibration,
        policy_snapshot::VerifiedPolicySnapshotBinding,
    },
    prefetch::source_slice::SourceSliceReader,
    service::{
        calibration_shared::assert_embargoed_after,
        trade_policy_evidence::TradePolicyEvidenceDurability,
        trade_policy_preimage::{
            TradePolicyPreimageTarget, TradePolicyPreimageVerifier, VerifiedTradePolicyPreimage,
        },
        training_dataset::{require_dataset_materialization, verify_frozen_dataset_artifact},
    },
};

/// Persistence and object-store dependencies for deep serving verification.
pub struct ModelServingPreimageDeps {
    pub compute: Arc<ComputeExecutor>,
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    pub source_slice_repo: Arc<dyn SourceSliceRepository>,
    pub policy_repo: Arc<dyn PolicyRepository>,
    pub calibration_repo: Arc<dyn CalibrationArtifactRepository>,
    pub trade_policy_preimages: Arc<TradePolicyPreimageVerifier>,
    pub artifact_store: Arc<dyn ArtifactStore>,
}

/// One verification operation's cancellation scope and existing memory budget.
///
/// Nested graph reads borrow this context instead of admitting another memory
/// lease. Dropping the operation cancels outstanding cooperative CPU work;
/// that work retains its actual compute and memory permits until it stops.
pub struct ModelPreimageReadContext<'a> {
    cancel: CancellationToken,
    memory_lease: Option<&'a OfflineMemoryLease>,
}

impl<'a> ModelPreimageReadContext<'a> {
    /// Attach verification to its job and, when present, its admitted budget.
    #[must_use]
    pub fn new(cancel: &CancellationToken, memory_lease: Option<&'a OfflineMemoryLease>) -> Self {
        Self {
            cancel: cancel.child_token(),
            memory_lease,
        }
    }

    /// Cancellation shared by every nested verification and object read.
    #[must_use]
    pub const fn cancel(&self) -> &CancellationToken {
        &self.cancel
    }

    /// The caller's already-admitted memory budget, if any.
    #[must_use]
    pub const fn memory_lease(&self) -> Option<&'a OfflineMemoryLease> {
        self.memory_lease
    }

    pub(crate) fn run<T>(
        &self,
        work: impl Future<Output = QuantResult<T>>,
    ) -> impl Future<Output = QuantResult<T>> {
        // Allocate before constructing the cancellation future so its state
        // never embeds the complete recursively verified model graph.
        let work = Box::pin(work);
        async move {
            tokio::select! {
                biased;
                () = self.cancel.cancelled() => Err(ResearchError::Cancelled {
                    detail: "model preimage verification cancelled".to_owned(),
                }.into()),
                result = work => result,
            }
        }
    }
}

impl Default for ModelPreimageReadContext<'_> {
    fn default() -> Self {
        Self {
            cancel: CancellationToken::new(),
            memory_lease: None,
        }
    }
}

impl Drop for ModelPreimageReadContext<'_> {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// A fully verified immutable serving source.
///
/// Construction has no durable side effects. Every opaque commitment in the
/// serving envelope has been resolved to its canonical preimage. Model,
/// Dataset, and Source Slice manifest bytes are always read back; research
/// verification additionally materializes every historical Source Slice fact
/// object, while runtime verification deliberately stops at immutable lineage.
pub struct VerifiedModelServingPreimage {
    artifact: ModelArtifact,
    estimator_bytes: Option<Vec<u8>>,
    calibration: Option<ResolvedCalibration>,
    model_spec: ModelSpecInfo,
    training_dataset: TrainingDatasetInfo,
    policy_snapshot: DecisionPolicySnapshotInfo,
    profile: ResearchProfileArtifact,
    bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
    graph_verified: bool,
}

struct VerifiedCalibrationParent {
    version: ModelVersionInfo,
    source: VerifiedModelServingPreimage,
    calibration: ResolvedCalibration,
}

fn label_horizon_matches(horizons_secs: &[u64], target_label_horizon_secs: u64) -> bool {
    target_label_horizon_secs == 0 || horizons_secs.contains(&target_label_horizon_secs)
}

/// Integrity-gated frozen Dataset accepted for deterministic replay against
/// one exact model-serving source.
pub struct VerifiedReplayDataset<'a> {
    dataset: &'a TrainingDatasetInfo,
    materialization: TrainingDatasetMaterialization<'a>,
}

impl<'a> VerifiedReplayDataset<'a> {
    #[must_use]
    pub const fn dataset(&self) -> &'a TrainingDatasetInfo {
        self.dataset
    }

    #[must_use]
    pub const fn materialization(&self) -> &TrainingDatasetMaterialization<'a> {
        &self.materialization
    }
}

impl VerifiedModelServingPreimage {
    #[must_use]
    pub const fn artifact(&self) -> &ModelArtifact {
        &self.artifact
    }

    #[must_use]
    pub const fn model_spec(&self) -> &ModelSpecInfo {
        &self.model_spec
    }

    #[must_use]
    pub const fn training_dataset(&self) -> &TrainingDatasetInfo {
        &self.training_dataset
    }

    #[must_use]
    pub const fn policy_snapshot(&self) -> &DecisionPolicySnapshotInfo {
        &self.policy_snapshot
    }

    #[must_use]
    pub const fn profile(&self) -> &ResearchProfileArtifact {
        &self.profile
    }

    /// Verify that this immutable serving graph is the exact champion frozen
    /// into one feedback cycle.
    ///
    /// `FeedbackCycleInfo::decision_policy_snapshot_*` identifies the active
    /// decision policy that published the Route at trigger time. The model's
    /// own build-time policy preimage is already committed by
    /// `champion_serving_contract_hash` and is intentionally allowed to differ
    /// after a governed Route or scenario-model activation. Treating those two
    /// snapshots as one identity makes every successful promotion invalidate
    /// the newly published champion.
    ///
    /// The diagnostic names every divergent champion dimension so a
    /// fail-closed cycle never collapses distinct contract failures into an
    /// opaque worker error.
    pub(crate) fn verify_feedback_cycle(&self, cycle: &FeedbackCycleInfo) -> QuantResult<()> {
        let contract = self.artifact.header().serving_contract();
        let feedback_policy_hash =
            self.profile
                .spec
                .feedback_policy
                .content_hash()
                .map_err(|error| FeedbackError::InvalidCycleIdentity {
                    detail: format!("champion feedback-policy hash failed: {error}"),
                })?;
        let mut mismatches = Vec::new();
        if self.artifact.header().model_version_id() != cycle.champion_model_version_id {
            mismatches.push("model_version_id");
        }
        if contract.contract_hash() != cycle.champion_serving_contract_hash {
            mismatches.push("serving_contract_hash");
        }
        if self.profile.profile_ref != cycle.profile_ref {
            mismatches.push("research_profile_ref");
        }
        if self.profile.profile_ref.artifact_id() != cycle.research_profile_artifact_id {
            mismatches.push("research_profile_artifact_id");
        }
        if self.profile.profile_ref.content_hash != cycle.profile_hash {
            mismatches.push("research_profile_hash");
        }
        if feedback_policy_hash != cycle.feedback_policy_hash {
            mismatches.push("feedback_policy_hash");
        }
        if self.model_spec.model_spec_id != cycle.champion_model_spec_id {
            mismatches.push("model_spec_id");
        }
        if self.model_spec.definition_hash != cycle.champion_model_spec_definition_hash {
            mismatches.push("model_spec_definition_hash");
        }
        if self.model_spec.model_family != cycle.champion_model_family {
            mismatches.push("model_family");
        }
        if mismatches.is_empty() {
            return Ok(());
        }
        Err(FeedbackError::InvalidCycleIdentity {
            detail: format!(
                "champion serving preimage differs from frozen feedback cycle {}: {}",
                cycle.feedback_cycle_id,
                mismatches.join(", ")
            ),
        }
        .into())
    }

    #[must_use]
    pub fn bias_table(&self) -> Option<Arc<FavoriteLongshotBiasTable>> {
        self.bias_table.as_ref().map(Arc::clone)
    }

    /// Build the Buy-side runtime solely from this already verified immutable
    /// preimage. No repository or object-store read occurs here.
    pub fn buy_runtime(&self) -> QuantResult<Arc<dyn QuantModelRuntime>> {
        self.ensure_runtime_materialized()?;
        match self.artifact.payload() {
            ModelPayload::WeightedFactor(_) => Ok(Arc::new(WeightedFactorRuntime::new(
                self.artifact.clone(),
                self.calibration.clone(),
            )?)),
            ModelPayload::Classical(_) => {
                #[cfg(feature = "ml-classical")]
                {
                    let bytes = self.estimator_bytes.as_deref().ok_or_else(|| {
                        ResearchError::InvalidModelArtifact {
                            detail: "verified classical preimage lost estimator bytes".to_owned(),
                        }
                    })?;
                    Ok(Arc::new(ClassicalRuntime::load(
                        self.artifact.clone(),
                        bytes,
                    )?))
                }
                #[cfg(not(feature = "ml-classical"))]
                {
                    Err(ResearchError::RuntimeUnavailable {
                        family: self.artifact.header().model_family().to_string(),
                        detail: "classical runtimes require the `ml-classical` build".to_owned(),
                    }
                    .into())
                }
            }
            ModelPayload::SellScorer(_) => Err(ResearchError::InvalidModelArtifact {
                detail: "sell scorer artifact must be loaded through sell_runtime".to_owned(),
            }
            .into()),
        }
    }

    /// Build the Sell-side scorer solely from this already verified immutable
    /// preimage. Buy-side families fail closed.
    pub fn sell_runtime(&self) -> QuantResult<Arc<dyn SellScorerRuntime>> {
        self.ensure_runtime_materialized()?;
        match self.artifact.payload() {
            ModelPayload::SellScorer(_) => Ok(Arc::new(WeightedSellScorerRuntime::new(
                self.artifact.clone(),
            )?)),
            _ => Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "model {} is not a sell scorer (family {})",
                    self.artifact.header().model_version_id(),
                    self.artifact.header().model_family()
                ),
            }
            .into()),
        }
    }

    fn ensure_runtime_materialized(&self) -> QuantResult<()> {
        if !self.graph_verified {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "runtime construction requires a fully verified model dependency graph"
                    .to_owned(),
            }
            .into());
        }
        let valid = match self.artifact.payload() {
            ModelPayload::WeightedFactor(weighted) => match (
                &weighted.return_model,
                self.calibration.as_ref(),
                self.estimator_bytes.as_ref(),
            ) {
                (ReturnModelSpec::Heuristic(_), None, None) => true,
                (ReturnModelSpec::Calibrated(model), Some(calibration), None) => {
                    model.calibrator_ref == calibration.artifact_id
                }
                _ => false,
            },
            ModelPayload::Classical(_) => {
                self.calibration.is_none() && self.estimator_bytes.is_some()
            }
            ModelPayload::SellScorer(_) => {
                self.calibration.is_none() && self.estimator_bytes.is_none()
            }
        };
        if !valid {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "verified runtime materialization differs from model {} payload semantics",
                    self.artifact.header().model_version_id()
                ),
            }
            .into());
        }
        Ok(())
    }
}

/// Canonical side-effect-free verifier shared by producers and consumers.
pub struct ModelServingPreimageService {
    deps: ModelServingPreimageDeps,
}

/// Controls whether preimage verification must materialize historical fact
/// objects or only the immutable lineage required to construct a runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PreimageVerificationDepth {
    RuntimeLineage,
    FullObjects,
}

impl ModelServingPreimageService {
    #[must_use]
    pub const fn new(deps: ModelServingPreimageDeps) -> Self {
        Self { deps }
    }

    /// Resolve and verify every source-model preimage before a caller creates a
    /// run, cache entry, artifact, or repository row.
    pub async fn load(
        &self,
        version: &ModelVersionInfo,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<VerifiedModelServingPreimage> {
        context
            .run(self.load_depth(version, PreimageVerificationDepth::FullObjects, context))
            .await
    }

    /// Resolve the complete executable graph while avoiding historical fact
    /// materialization that is irrelevant to inference. Dataset bytes, the
    /// WORM Source Slice row, and the exact manifest bytes remain verified.
    pub(crate) async fn load_runtime(
        &self,
        version: &ModelVersionInfo,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<VerifiedModelServingPreimage> {
        context
            .run(self.load_depth(version, PreimageVerificationDepth::RuntimeLineage, context))
            .await
    }

    async fn load_depth(
        &self,
        version: &ModelVersionInfo,
        depth: PreimageVerificationDepth,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<VerifiedModelServingPreimage> {
        let mut root = self.load_base(version, depth, context).await?;
        let mut visiting = HashSet::new();
        let mut verified = HashSet::new();
        root.calibration = self
            .verify_graph(version, &root, &mut visiting, &mut verified, depth, context)
            .await?;
        root.graph_verified = true;
        root.ensure_runtime_materialized()?;
        Ok(root)
    }

    pub(crate) async fn load_base(
        &self,
        version: &ModelVersionInfo,
        depth: PreimageVerificationDepth,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<VerifiedModelServingPreimage> {
        let artifact =
            ModelArtifact::load_verified(self.deps.artifact_store.as_ref(), version).await?;
        let contract = artifact.header().serving_contract();
        let estimator_bytes = self.load_estimator_bytes(&artifact).await?;
        let model_spec = self.load_model_spec(version, contract).await?;
        let profile = Self::verify_research_profile(contract)?;
        let policy_snapshot = self
            .load_policy_preimage(&contract.bindings().policy_snapshot)
            .await?;
        let training_dataset = self
            .load_training_preimage(
                &artifact,
                &model_spec,
                &profile,
                &policy_snapshot,
                depth,
                context,
            )
            .await?;
        let bias_table = self
            .verify_bias_preimage(contract, &policy_snapshot)
            .await?;
        Ok(VerifiedModelServingPreimage {
            artifact,
            estimator_bytes,
            calibration: None,
            model_spec,
            training_dataset,
            policy_snapshot,
            profile,
            bias_table,
            graph_verified: false,
        })
    }

    fn verify_graph<'a>(
        &'a self,
        version: &'a ModelVersionInfo,
        source: &'a VerifiedModelServingPreimage,
        visiting: &'a mut HashSet<ModelVersionId>,
        verified: &'a mut HashSet<ModelVersionId>,
        depth: PreimageVerificationDepth,
        context: &'a ModelPreimageReadContext<'_>,
    ) -> BoxFuture<'a, QuantResult<Option<ResolvedCalibration>>> {
        Box::pin(async move {
            let version_id = version.model_version_id;
            if verified.contains(&version_id) {
                return Ok(None);
            }
            if !visiting.insert(version_id) {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "model serving dependency graph contains a cycle at {version_id}"
                    ),
                }
                .into());
            }
            let calibration = if let Some(parent) =
                Box::pin(self.load_calibration_parent(version, source, depth, context)).await?
            {
                self.verify_graph(
                    &parent.version,
                    &parent.source,
                    visiting,
                    verified,
                    depth,
                    context,
                )
                .await?;
                Some(parent.calibration)
            } else {
                None
            };
            if let Some(policy) = Box::pin(
                self.deps.trade_policy_preimages.verify_depth(
                    self,
                    TradePolicyPreimageTarget {
                        dataset: source.training_dataset(),
                        model_spec: source.model_spec(),
                        policy_snapshot: &source
                            .artifact()
                            .header()
                            .serving_contract()
                            .bindings()
                            .policy_snapshot,
                        profile: source.profile(),
                    },
                    TradePolicyEvidenceDurability::Production,
                    depth,
                    context,
                ),
            )
            .await?
            {
                let expected = source
                    .artifact()
                    .header()
                    .serving_contract()
                    .bindings()
                    .trade_policy
                    .as_ref();
                if expected != Some(policy.binding()) {
                    return Err(ResearchError::InvalidModelArtifact {
                        detail: format!(
                            "model {version_id} TradePolicy preimage differs from its serving binding"
                        ),
                    }
                    .into());
                }
                self.verify_graph(
                    policy.subject(),
                    policy.subject_preimage(),
                    visiting,
                    verified,
                    depth,
                    context,
                )
                .await?;
            }
            visiting.remove(&version_id);
            verified.insert(version_id);
            Ok(calibration)
        })
    }

    /// Verify an immutable model-score calibrator against the exact source
    /// model and held-out Calibration Dataset graph. Lifecycle is deliberately
    /// caller-owned: fit/governance verifies an inactive candidate, whereas
    /// serving additionally requires the same row to remain active.
    pub(crate) async fn verify_calibrator(
        &self,
        source: &VerifiedModelServingPreimage,
        info: &CalibrationArtifactInfo,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<VerifiedModelScoreCalibration> {
        context
            .run(self.verify_calibrator_depth(
                source,
                info,
                PreimageVerificationDepth::FullObjects,
                context,
            ))
            .await
    }

    async fn verify_calibrator_depth(
        &self,
        source: &VerifiedModelServingPreimage,
        info: &CalibrationArtifactInfo,
        depth: PreimageVerificationDepth,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<VerifiedModelScoreCalibration> {
        let calibrator = VerifiedModelScoreCalibration::try_from(info)?;
        let source_model_version_id = source
            .artifact()
            .header()
            .serving_contract()
            .bindings()
            .model
            .model_version_id;
        if calibrator.payload().fit_contract.model.model_version_id != source_model_version_id {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "calibrator {} was fitted for model {}, not exact source {source_model_version_id}",
                    calibrator.artifact_id(),
                    calibrator.payload().fit_contract.model.model_version_id,
                ),
            }
            .into());
        }
        self.verify_calibration_dataset(source, &calibrator, depth, context)
            .await?;
        Ok(calibrator)
    }

    /// Verify a frozen non-training Dataset against one exact model source
    /// before replay, cache lookup, run creation, or artifact persistence.
    ///
    /// The model source retains its immutable build-time policy preimage. The
    /// Dataset instead binds the decision-time policy that produced its frozen
    /// cohort. Those identities may differ after a compatible Route/scenario
    /// activation; feature, factor, input, Trade Policy, source-schema, and PIT
    /// contracts remain exact and fail closed.
    pub(crate) async fn verify_replay_dataset<'a>(
        &self,
        source: &VerifiedModelServingPreimage,
        dataset: &'a TrainingDatasetInfo,
        purpose: DatasetPurpose,
        dataset_policy: &ModelServingPolicySnapshotBinding,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<VerifiedReplayDataset<'a>> {
        context
            .run(async {
                let replay = Box::pin(self.verify_replay_bindings(
                    source,
                    dataset,
                    purpose,
                    dataset_policy,
                    context,
                ))
                .await?;
                self.verify_source_slice_ledger(dataset).await?;
                Ok(replay)
            })
            .await
    }

    /// Verify one model against an already-loaded replay Dataset without
    /// reopening Dataset or Source Slice objects. F09 uses this for every
    /// member of one reserved candidate family.
    pub(crate) async fn verify_replay_bindings<'a>(
        &self,
        source: &VerifiedModelServingPreimage,
        dataset: &'a TrainingDatasetInfo,
        purpose: DatasetPurpose,
        dataset_policy: &ModelServingPolicySnapshotBinding,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<VerifiedReplayDataset<'a>> {
        if !source.graph_verified {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "replay requires a fully verified model dependency graph".to_owned(),
            }
            .into());
        }
        if dataset.status != TrainingDatasetStatus::Ready || dataset.purpose != purpose {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "replay Dataset {} must remain Ready/{purpose}",
                    dataset.training_dataset_id
                ),
            }
            .into());
        }
        let contract = source.artifact().header().serving_contract();
        Self::verify_semantic_bindings(
            source.artifact(),
            source.model_spec(),
            source.profile(),
            source.policy_snapshot(),
            dataset_policy,
            dataset,
        )?;
        let trade_policy = Box::pin(self.deps.trade_policy_preimages.verify(
            self,
            TradePolicyPreimageTarget {
                dataset,
                model_spec: source.model_spec(),
                policy_snapshot: &contract.bindings().policy_snapshot,
                profile: source.profile(),
            },
            TradePolicyEvidenceDurability::ContentVerified,
            context,
        ))
        .await?;
        if trade_policy
            .as_ref()
            .map(VerifiedTradePolicyPreimage::binding)
            != contract.bindings().trade_policy.as_ref()
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "replay Dataset {} TradePolicy preimage differs from source model {}",
                    dataset.training_dataset_id,
                    contract.bindings().model.model_version_id
                ),
            }
            .into());
        }
        let materialization = require_dataset_materialization(dataset)?;
        let training = require_dataset_materialization(source.training_dataset())?;
        let lineage = &materialization.manifest.source_lineage;
        let training_lineage = &training.manifest.source_lineage;
        if lineage.reader_contract_version != training_lineage.reader_contract_version
            || lineage.schema_contract_version != training_lineage.schema_contract_version
            || lineage.source_schema_hash != training_lineage.source_schema_hash
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "replay Dataset {} reader/source schema differs from source model {}",
                    dataset.training_dataset_id,
                    contract.bindings().model.model_version_id
                ),
            }
            .into());
        }
        Ok(VerifiedReplayDataset {
            dataset,
            materialization,
        })
    }

    /// Build the sole calibration-fit provenance contract from verified
    /// model, policy, Dataset, Source Slice, and object-byte preimages.
    pub(crate) async fn calibration_fit_contract(
        &self,
        source: &VerifiedModelServingPreimage,
        dataset: &TrainingDatasetInfo,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<ModelScoreCalibrationFitContract> {
        let model_policy = &source
            .artifact()
            .header()
            .serving_contract()
            .bindings()
            .policy_snapshot;
        let verified = Box::pin(self.verify_replay_dataset(
            source,
            dataset,
            DatasetPurpose::Calibration,
            model_policy,
            context,
        ))
        .await?;
        let calibration = verified.materialization();
        let training = require_dataset_materialization(source.training_dataset())?;
        if calibration.manifest.source_lineage.research_program_hash
            != training.manifest.source_lineage.research_program_hash
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "Calibration Dataset {} research program differs from source model training Dataset",
                    dataset.training_dataset_id
                ),
            }
            .into());
        }
        let artifact = source.artifact();
        let contract = artifact.header().serving_contract();
        let bindings = contract.bindings();
        Ok(ModelScoreCalibrationFitContract {
            model: ModelScoreCalibrationModelBinding {
                model_version_id: bindings.model.model_version_id,
                artifact_hash: artifact.content_hash()?,
                serving_contract_hash: contract.contract_hash(),
                model_spec_id: bindings.model.model_spec_id,
                model_spec_definition_hash: bindings.model.model_spec_definition_hash,
                model_family: bindings.model.model_family,
                profile_ref: bindings.model.profile_ref.clone(),
                category_scope: bindings.model.category_scope,
                prediction_horizon_secs: bindings.model.prediction_horizon_secs,
                training_dataset_id: training.manifest.training_dataset_id,
                training_dataset_hash: *training.dataset_hash,
            },
            calibration_dataset: ModelScoreCalibrationDatasetBinding {
                calibration_dataset_id: calibration.manifest.training_dataset_id,
                dataset_hash: *calibration.dataset_hash,
                manifest_hash: *calibration.manifest_hash,
                artifact_bytes_hash: *calibration.artifact_bytes_hash,
                source_slice_manifest_hash: calibration
                    .manifest
                    .source_lineage
                    .source_slice
                    .manifest_hash,
                feature_schema_hash: *calibration.feature_schema_hash,
                factor_schema_hash: calibration.factor_schema_hash(),
                label_schema_hash: *calibration.label_schema_hash,
            },
            policy_snapshot: ModelScoreCalibrationPolicyBinding {
                decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
                snapshot_hash: bindings.policy_snapshot.snapshot_hash,
            },
        })
    }

    async fn load_calibration_parent(
        &self,
        version: &ModelVersionInfo,
        source: &VerifiedModelServingPreimage,
        depth: PreimageVerificationDepth,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<Option<VerifiedCalibrationParent>> {
        let derivation =
            version
                .verified_derivation()
                .map_err(|error| ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "model {} has invalid derivation lineage: {error}",
                        version.model_version_id
                    ),
                })?;
        let binding = source
            .artifact()
            .header()
            .serving_contract()
            .bindings()
            .model
            .calibration
            .as_ref();
        let (parent_id, calibration_artifact_id) = match (binding, derivation) {
            (None, ModelVersionDerivation::Training) => return Ok(None),
            (
                Some(binding),
                ModelVersionDerivation::ReturnCalibration {
                    parent_model_version_id,
                    calibration_artifact_id,
                },
            ) if binding.kind == CalibrationKind::ModelScore
                && binding.artifact_id == calibration_artifact_id =>
            {
                (parent_model_version_id, calibration_artifact_id)
            }
            _ => {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "model {} calibration binding differs from its typed derivation lineage",
                        version.model_version_id
                    ),
                }
                .into());
            }
        };
        let parent = self
            .deps
            .model_registry_repo
            .find_model_version(&parent_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_model_version",
                id: parent_id.to_string(),
            })?;
        let parent_source = self.load_base(&parent, depth, context).await?;
        let info = self
            .deps
            .calibration_repo
            .find_by_id(&calibration_artifact_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "calibration_artifact",
                id: calibration_artifact_id.to_string(),
            })?;
        let verified = self
            .verify_calibrator_depth(&parent_source, &info, depth, context)
            .await?;
        let binding = binding.ok_or_else(|| ResearchError::InvalidModelArtifact {
            detail: format!(
                "model {} lost its calibration binding during verification",
                version.model_version_id
            ),
        })?;
        if !info.active
            || binding.content_hash != verified.content_hash()
            || binding.artifact_id != verified.artifact_id()
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "model {} calibration artifact is inactive or differs from its exact serving binding",
                    version.model_version_id
                ),
            }
            .into());
        }
        Self::verify_calibrated_child(version, source, &parent, &parent_source)?;
        Ok(Some(VerifiedCalibrationParent {
            version: parent,
            source: parent_source,
            calibration: ResolvedCalibration::try_from(verified)?,
        }))
    }

    fn verify_calibrated_child(
        version: &ModelVersionInfo,
        source: &VerifiedModelServingPreimage,
        parent: &ModelVersionInfo,
        parent_source: &VerifiedModelServingPreimage,
    ) -> QuantResult<()> {
        let ModelPayload::WeightedFactor(current) = source.artifact().payload() else {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "return-calibration derivation requires a weighted-factor child".to_owned(),
            }
            .into());
        };
        let ModelPayload::WeightedFactor(parent_payload) = parent_source.artifact().payload()
        else {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "return-calibration derivation requires a weighted-factor parent"
                    .to_owned(),
            }
            .into());
        };
        let mut expected_payload = parent_payload.as_ref().clone();
        expected_payload.return_model = current.return_model.clone();
        if current.as_ref() != &expected_payload
            || version.metrics != parent.metrics
            || version.training_objective != parent.training_objective
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "calibrated model {} changes parent payload or training evidence outside the governed calibration transform",
                    version.model_version_id
                ),
            }
            .into());
        }
        let payload = ModelPayload::WeightedFactor(Box::new(expected_payload));
        let current_contract = source.artifact().header().serving_contract();
        let current_binding = current_contract
            .bindings()
            .model
            .calibration
            .clone()
            .ok_or_else(|| ResearchError::InvalidModelArtifact {
                detail: format!(
                    "calibrated model {} has no calibration binding",
                    version.model_version_id
                ),
            })?;
        let mut expected = parent_source
            .artifact()
            .header()
            .serving_contract()
            .bindings()
            .clone();
        expected.model.model_version_id = version.model_version_id;
        expected.model.estimator = payload.serving_estimator_binding(&expected.factors.plane)?;
        expected.model.calibration = Some(current_binding);
        let expected_contract = ModelServingContract::try_seal(expected).map_err(|error| {
            ResearchError::InvalidModelArtifact {
                detail: format!("cannot reconstruct calibrated child contract: {error}"),
            }
        })?;
        if &expected_contract != current_contract {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "calibrated model {} changes parent serving commitments outside the governed calibration transform",
                    version.model_version_id
                ),
            }
            .into());
        }
        Ok(())
    }

    async fn load_estimator_bytes(&self, artifact: &ModelArtifact) -> QuantResult<Option<Vec<u8>>> {
        let ModelPayload::Classical(classical) = artifact.payload() else {
            return Ok(None);
        };
        let bytes = self
            .deps
            .artifact_store
            .get(&classical.serialized_model_uri)
            .await?;
        let actual_hash = CanonicalDigest::content_hash_bytes(&bytes);
        if actual_hash != classical.serialized_model_hash {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "classical estimator bytes hash mismatch: expected {}, got {actual_hash}",
                    classical.serialized_model_hash
                ),
            }
            .into());
        }
        let key = ArtifactKey::new(
            ArtifactNamespace::Model,
            CanonicalDigest::raw_hex(&bytes),
            "bin",
        )?;
        let canonical = self.deps.artifact_store.get_by_key(&key).await?;
        if canonical != bytes {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "classical estimator URI for model {} differs from its canonical content-addressed object",
                    artifact.header().model_version_id()
                ),
            }
            .into());
        }
        #[cfg(feature = "ml-classical")]
        {
            let _ = ClassicalRuntime::load(artifact.clone(), &bytes)?;
        }
        Ok(Some(bytes))
    }

    async fn load_model_spec(
        &self,
        version: &ModelVersionInfo,
        contract: &ModelServingContract,
    ) -> QuantResult<ModelSpecInfo> {
        let spec = self
            .deps
            .model_registry_repo
            .find_model_spec(&version.model_spec_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "model_spec",
                id: version.model_spec_id.to_string(),
            })?;
        spec.input_contract
            .validate()
            .map_err(|detail| ResearchError::InvalidModelArtifact {
                detail: format!("invalid source model input contract: {detail}"),
            })?;
        spec.training_contract.validate().map_err(|detail| {
            ResearchError::InvalidModelArtifact {
                detail: format!("invalid source model training contract: {detail}"),
            }
        })?;
        let definition = spec.definition();
        definition
            .validate()
            .map_err(|detail| ResearchError::InvalidModelArtifact {
                detail: format!("invalid source model specification: {detail}"),
            })?;
        let definition_hash =
            definition
                .content_hash()
                .map_err(|error| ResearchError::InvalidModelArtifact {
                    detail: format!("source model-spec definition hash failed: {error}"),
                })?;
        let prediction_horizon_secs =
            u64::try_from(spec.prediction_horizon_secs).map_err(|error| {
                ResearchError::InvalidModelArtifact {
                    detail: format!("source model prediction horizon is invalid: {error}"),
                }
            })?;
        let bindings = contract.bindings();
        if definition_hash != spec.definition_hash
            || spec.definition_hash != version.model_spec_definition_hash
            || spec.model_family != version.model_family
            || spec.model_spec_id != bindings.model.model_spec_id
            || spec.definition_hash != bindings.model.model_spec_definition_hash
            || spec.model_family != bindings.model.model_family
            || prediction_horizon_secs != bindings.model.prediction_horizon_secs
            || model_input_contract_hash(&spec.input_contract)?
                != bindings.transform.input_contract_hash
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "source ModelSpec {} preimage differs from its version or serving contract",
                    spec.model_spec_id
                ),
            }
            .into());
        }
        Ok(spec)
    }

    fn verify_research_profile(
        contract: &ModelServingContract,
    ) -> QuantResult<ResearchProfileArtifact> {
        let model = &contract.bindings().model;
        let profile = model
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(|detail| ResearchError::InvalidModelArtifact {
                detail: format!("source ResearchProfile preimage failed: {detail}"),
            })?;
        if profile.profile_ref != model.profile_ref
            || profile.spec.category != model.category_scope
            || profile.spec.target_horizon_secs != model.prediction_horizon_secs
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "source model {} category/horizon differs from canonical ResearchProfile {}@{}",
                    model.model_version_id, profile.profile_ref.id, profile.profile_ref.version,
                ),
            }
            .into());
        }
        Ok(profile)
    }

    async fn load_policy_preimage(
        &self,
        expected: &ModelServingPolicySnapshotBinding,
    ) -> QuantResult<DecisionPolicySnapshotInfo> {
        let info = self
            .deps
            .policy_repo
            .load_snapshot(&expected.decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "decision_policy_snapshot",
                id: expected.decision_policy_snapshot_id.to_string(),
            })?;
        let verified = VerifiedPolicySnapshotBinding::try_from(&info)?;
        if verified.binding() != expected {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "policy snapshot {} preimage differs from the source serving contract",
                    expected.decision_policy_snapshot_id
                ),
            }
            .into());
        }
        Ok(info)
    }

    /// Re-read one Dataset and its canonical Source Slice object graph.
    pub async fn verify_dataset_objects(
        &self,
        dataset: &TrainingDatasetInfo,
        profile: &ResearchProfileArtifact,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<()> {
        context
            .run(async {
                self.verify_dataset_bytes(dataset).await?;
                self.verify_source_slice_ledger(dataset).await?;

                let source_slice = &dataset.source_lineage.source_slice;
                let reader = SourceSliceReader::new(
                    Arc::clone(&self.deps.artifact_store),
                    Arc::clone(&self.deps.compute),
                );
                let frozen = match context.memory_lease() {
                    Some(lease) => {
                        reader
                            .read_ref_leased(source_slice, context.cancel(), lease)
                            .await?
                    }
                    None => reader.read_ref(source_slice, context.cancel()).await?,
                };
                Self::verify_source_manifest(dataset, profile, &frozen.manifest)
            })
            .await
    }

    pub(crate) async fn verify_dataset(
        &self,
        dataset: &TrainingDatasetInfo,
        profile: &ResearchProfileArtifact,
        depth: PreimageVerificationDepth,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<()> {
        match depth {
            PreimageVerificationDepth::RuntimeLineage => {
                self.verify_dataset_lineage(dataset, profile).await
            }
            PreimageVerificationDepth::FullObjects => {
                self.verify_dataset_objects(dataset, profile, context).await
            }
        }
    }

    async fn verify_dataset_lineage(
        &self,
        dataset: &TrainingDatasetInfo,
        profile: &ResearchProfileArtifact,
    ) -> QuantResult<()> {
        self.verify_dataset_bytes(dataset).await?;
        self.verify_source_slice_ledger(dataset).await?;
        let lineage = &dataset.source_lineage;
        let manifest = SourceSliceReader::new(
            Arc::clone(&self.deps.artifact_store),
            Arc::clone(&self.deps.compute),
        )
        .verify_manifest_ref(&lineage.source_slice)
        .await?;
        Self::verify_source_manifest(dataset, profile, &manifest)
    }

    async fn verify_dataset_bytes(&self, dataset: &TrainingDatasetInfo) -> QuantResult<()> {
        let materialization = require_dataset_materialization(dataset)?;
        let bytes = self
            .deps
            .artifact_store
            .get(materialization.parquet_uri)
            .await?;
        verify_frozen_dataset_artifact(dataset, &bytes).map(|_| ())
    }

    fn verify_source_manifest(
        dataset: &TrainingDatasetInfo,
        profile: &ResearchProfileArtifact,
        manifest: &SourceSliceManifest,
    ) -> QuantResult<()> {
        let lineage = &dataset.source_lineage;
        lineage
            .verify_manifest(manifest)
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!(
                    "dataset {} Source Slice manifest differs from its frozen lineage: {error}",
                    dataset.training_dataset_id
                ),
            })?;
        manifest
            .validate_for_profile(
                profile,
                &lineage.research_program_hash,
                dataset.window_start,
                dataset.window_end,
                dataset.pit_cutoff,
            )
            .map_err(|detail| ResearchError::DatasetBuild {
                detail: format!(
                    "dataset {} Source Slice profile/PIT contract failed: {detail}",
                    dataset.training_dataset_id
                ),
            })?;
        Ok(())
    }

    /// Verify the Dataset-bound Source Slice row without opening any object.
    async fn verify_source_slice_ledger(&self, dataset: &TrainingDatasetInfo) -> QuantResult<()> {
        let lineage = &dataset.source_lineage;
        let ledger = self
            .deps
            .source_slice_repo
            .find_by_id(&lineage.source_slice_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "source_slice",
                id: lineage.source_slice_id.to_string(),
            })?;
        let identity = SourceSliceIdentity::derive(SourceSliceIdentityInput {
            profile_ref: ledger.profile_ref.clone(),
            evaluation_track: ledger.evaluation_track,
            research_program_hash: ledger.research_program_hash,
            decision_policy_snapshot_id: ledger.decision_policy_snapshot_id,
            runtime_config_hash: ledger.runtime_config_hash,
            fit_seal_id: ledger.fit_seal_id,
            fit_seal_hash: ledger.fit_seal_hash,
            window_start: ledger.window_start,
            window_end: ledger.window_end,
            pit_cutoff: ledger.pit_cutoff,
        })
        .map_err(|error| ResearchError::DatasetBuild {
            detail: format!("Source Slice identity hash failed: {error}"),
        })?;
        if ledger.status != SourceSliceStatus::Ready
            || identity.identity_hash != ledger.identity_hash
            || ledger.source_slice_id != lineage.source_slice_id
            || ledger.identity_hash != lineage.source_slice_identity_hash
            || ledger.profile_ref != lineage.research_profile_artifact_id.profile_ref()
            || ledger.research_program_hash != lineage.research_program_hash
            || ledger.decision_policy_snapshot_id != lineage.decision_policy_snapshot_id
            || ledger.runtime_config_hash != lineage.runtime_config_hash
            || ledger.fit_seal_id != lineage.fit_seal_id
            || ledger.fit_seal_hash != lineage.fit_seal_hash
            || ledger.window_start != lineage.source_window_start
            || ledger.window_end != lineage.source_window_end
            || ledger.pit_cutoff != lineage.pit_cutoff
            || ledger.reader_contract_version != lineage.reader_contract_version
            || ledger.schema_contract_version != lineage.schema_contract_version
            || ledger.manifest_uri.as_ref() != Some(&lineage.source_slice.manifest_uri)
            || ledger.manifest_hash != Some(lineage.source_slice.manifest_hash)
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "dataset {} Source Slice lineage differs from its canonical Ready ledger",
                    dataset.training_dataset_id
                ),
            }
            .into());
        }
        Ok(())
    }

    async fn load_training_preimage(
        &self,
        artifact: &ModelArtifact,
        model_spec: &ModelSpecInfo,
        profile: &ResearchProfileArtifact,
        policy: &DecisionPolicySnapshotInfo,
        depth: PreimageVerificationDepth,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<TrainingDatasetInfo> {
        let contract = artifact.header().serving_contract();
        let binding = &contract.bindings().dataset;
        let dataset_id = binding.manifest.training_dataset_id;
        let dataset = self
            .deps
            .dataset_repo
            .find_by_id(&dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: dataset_id.to_string(),
            })?;
        if dataset.status != TrainingDatasetStatus::Ready
            || dataset.purpose != DatasetPurpose::Training
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!("source model dataset {dataset_id} must remain Ready/Training"),
            }
            .into());
        }
        let materialization = require_dataset_materialization(&dataset)?;
        if materialization.manifest != &binding.manifest
            || materialization.manifest_hash != &binding.manifest_hash
            || materialization.artifact_bytes_hash != &binding.artifact_bytes_hash
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "source model Dataset {dataset_id} preimage differs from its serving binding"
                ),
            }
            .into());
        }
        Self::verify_semantic_bindings(
            artifact,
            model_spec,
            profile,
            policy,
            &contract.bindings().policy_snapshot,
            &dataset,
        )?;
        self.verify_dataset(&dataset, profile, depth, context)
            .await?;
        Ok(dataset)
    }

    fn verify_semantic_bindings(
        artifact: &ModelArtifact,
        model_spec: &ModelSpecInfo,
        profile: &ResearchProfileArtifact,
        model_policy: &DecisionPolicySnapshotInfo,
        dataset_policy: &ModelServingPolicySnapshotBinding,
        dataset: &TrainingDatasetInfo,
    ) -> QuantResult<()> {
        let contract = artifact.header().serving_contract();
        let bindings = contract.bindings();
        let materialization = require_dataset_materialization(dataset)?;
        let manifest = materialization.manifest;
        let feature_schema = ExecutableFeatureSchema::build(
            &model_policy.snapshot.profile_artifacts.features.definition,
            profile.spec.feature_contract,
        )?;
        let feature_schema_hash = ResearchHasher::feature_schema(&feature_schema)?;
        let factor_plane = if model_spec.model_family.is_classical() {
            FactorServingPlane::try_empty().map_err(|error| {
                ResearchError::InvalidModelArtifact {
                    detail: format!("canonical classical factor plane failed: {error}"),
                }
            })?
        } else {
            FactorEngine::for_model_scope(
                &model_policy.snapshot.profile_artifacts.scoring.definition,
                &model_policy.snapshot.profile_artifacts.features.definition,
                &model_policy.snapshot.profile_artifacts.domain.definition,
                profile.spec.feature_contract,
                profile.spec.category,
                None,
            )
            .serving_plane()?
            .clone()
        };
        let required_domains = Self::required_domain_families(
            &feature_schema,
            &factor_plane,
            &model_spec.input_contract,
            profile.spec.category,
        )?;
        let prediction_horizon_secs =
            u64::try_from(model_spec.prediction_horizon_secs).map_err(|error| {
                ResearchError::InvalidModelArtifact {
                    detail: format!("source model prediction horizon is invalid: {error}"),
                }
            })?;
        let expected_trade_policy = model_spec
            .training_contract
            .evaluation_trade_policy_artifact_id;
        let bound_trade_policy = bindings
            .trade_policy
            .as_ref()
            .map(|binding| binding.artifact_id);
        let bound_trade_policy_hash = bindings
            .trade_policy
            .as_ref()
            .map(|binding| binding.content_hash);
        let target_label_horizon = model_spec.training_contract.target.label_horizon_secs();
        let expected_model_identity = (
            model_spec.model_spec_id,
            model_spec.definition_hash,
            model_spec.model_family,
        );
        let dataset_model_matches = (
            dataset.model_spec_id,
            dataset.model_spec_definition_hash,
            dataset.model_family,
        ) == expected_model_identity;
        let manifest_model_matches = (
            manifest.model_spec_id,
            manifest.model_spec_definition_hash,
            manifest.model_family,
        ) == expected_model_identity;
        let model_identity_matches = dataset_model_matches && manifest_model_matches;
        let schema_plane_matches = dataset.feature_schema_version
            == model_spec.feature_schema_version
            && manifest.feature_schema_version == model_spec.feature_schema_version
            && feature_schema.version() == model_spec.feature_schema_version
            && feature_schema_hash == bindings.schemas.feature_schema_hash
            && feature_schema_hash == manifest.feature_schema_hash
            && factor_plane == bindings.factors.plane
            && factor_plane == manifest.factor_serving_plane
            && required_domains == bindings.required_domain_families
            && model_input_contract_hash(&model_spec.input_contract)?
                == bindings.transform.input_contract_hash;
        let profile_matches = dataset.research_profile_artifact_id
            == profile.profile_ref.artifact_id()
            && dataset.source_lineage.research_profile_artifact_id
                == profile.profile_ref.artifact_id()
            && prediction_horizon_secs == profile.spec.target_horizon_secs;
        let dataset_policy_matches = dataset.decision_policy_snapshot_id
            == dataset_policy.decision_policy_snapshot_id
            && dataset.source_lineage.decision_policy_snapshot_id
                == dataset_policy.decision_policy_snapshot_id
            && dataset.source_lineage.runtime_config_hash == dataset_policy.snapshot_hash;
        let trade_policy_matches = expected_trade_policy == bound_trade_policy
            && expected_trade_policy == manifest.trade_policy_artifact_id
            && bound_trade_policy_hash == manifest.trade_policy_hash;
        let label_horizon_matches =
            label_horizon_matches(&manifest.horizons_secs, target_label_horizon);
        let configured_cross_section = &model_policy
            .snapshot
            .profile_artifacts
            .scoring
            .definition
            .cross_section;
        let runtime_transform_matches = match artifact.payload() {
            ModelPayload::WeightedFactor(payload) => {
                &payload.factor_cross_section == configured_cross_section
            }
            ModelPayload::SellScorer(payload) => {
                &payload.factor_cross_section == configured_cross_section
            }
            ModelPayload::Classical(_) => true,
        };
        let mismatches = [
            ("model_identity", model_identity_matches),
            ("schema_plane", schema_plane_matches),
            ("profile", profile_matches),
            ("dataset_policy", dataset_policy_matches),
            ("trade_policy", trade_policy_matches),
            ("label_horizon", label_horizon_matches),
            ("runtime_transform", runtime_transform_matches),
        ]
        .into_iter()
        .filter_map(|(dimension, matches)| (!matches).then_some(dimension))
        .collect::<Vec<_>>();
        if !mismatches.is_empty() {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "model {} ModelSpec/profile/policy/feature/factor/domain/training preimage matrix differs from its serving contract and Dataset: {}",
                    bindings.model.model_version_id,
                    mismatches.join(",")
                ),
            }
            .into());
        }
        Ok(())
    }

    pub(crate) fn required_domain_families(
        schema: &ExecutableFeatureSchema,
        plane: &FactorServingPlane,
        input_contract: &ModelInputContract,
        category_scope: Option<MarketCategory>,
    ) -> QuantResult<Vec<DomainFamily>> {
        let mut required = BTreeSet::new();
        match category_scope {
            Some(MarketCategory::Crypto) => {
                required.insert(DomainFamily::Crypto);
            }
            Some(MarketCategory::Weather) => {
                required.insert(DomainFamily::Weather);
            }
            _ => {}
        }
        for factor in plane.definitions() {
            match factor.definition().family {
                FactorFamily::DomainCrypto => {
                    required.insert(DomainFamily::Crypto);
                }
                FactorFamily::DomainWeather => {
                    required.insert(DomainFamily::Weather);
                }
                _ => {}
            }
        }
        for input in &input_contract.inputs {
            let feature_name = FeatureName::new(input.feature_name.clone());
            let feature = schema.by_name(&feature_name).ok_or_else(|| {
                ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "model input `{feature_name}` is absent from the verified feature schema"
                    ),
                }
            })?;
            match feature.source_requirement {
                SourceRequirement::DomainCryptoObservationWindow => {
                    required.insert(DomainFamily::Crypto);
                }
                SourceRequirement::DomainWeatherObservationWindow => {
                    required.insert(DomainFamily::Weather);
                }
                _ => {}
            }
        }
        Ok(required.into_iter().collect())
    }

    async fn verify_calibration_dataset(
        &self,
        source: &VerifiedModelServingPreimage,
        calibrator: &VerifiedModelScoreCalibration,
        depth: PreimageVerificationDepth,
        context: &ModelPreimageReadContext<'_>,
    ) -> QuantResult<()> {
        let contract = source.artifact().header().serving_contract();
        let training = require_dataset_materialization(source.training_dataset())?;
        Self::verify_calibration_model(source, calibrator, contract, &training)?;
        let dataset = self.load_calibration_dataset(calibrator).await?;
        let materialization = require_dataset_materialization(&dataset)?;
        Self::verify_calibration_binding(
            calibrator,
            &dataset,
            contract,
            &materialization,
            &training,
            source
                .model_spec()
                .training_contract
                .target
                .label_horizon_secs(),
        )?;
        Self::verify_calibration_window(source, calibrator, &dataset, &materialization)?;
        self.verify_dataset(&dataset, source.profile(), depth, context)
            .await
    }

    fn verify_calibration_model(
        source: &VerifiedModelServingPreimage,
        calibrator: &VerifiedModelScoreCalibration,
        contract: &ModelServingContract,
        training: &TrainingDatasetMaterialization<'_>,
    ) -> QuantResult<()> {
        let bindings = contract.bindings();
        let fit = &calibrator.payload().fit_contract;
        let model = &fit.model;
        if model.model_version_id != bindings.model.model_version_id
            || model.artifact_hash != source.artifact().content_hash()?
            || model.serving_contract_hash != contract.contract_hash()
            || model.model_spec_id != bindings.model.model_spec_id
            || model.model_spec_definition_hash != bindings.model.model_spec_definition_hash
            || model.model_family != bindings.model.model_family
            || model.profile_ref != bindings.model.profile_ref
            || model.category_scope != bindings.model.category_scope
            || model.prediction_horizon_secs != bindings.model.prediction_horizon_secs
            || model.training_dataset_id != training.manifest.training_dataset_id
            || model.training_dataset_hash != *training.dataset_hash
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "calibrator {} source-model fit contract differs from the exact artifact",
                    calibrator.artifact_id()
                ),
            }
            .into());
        }
        if fit.policy_snapshot.decision_policy_snapshot_id
            != bindings.policy_snapshot.decision_policy_snapshot_id
            || fit.policy_snapshot.snapshot_hash != bindings.policy_snapshot.snapshot_hash
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "calibrator {} policy fit contract differs from the exact snapshot preimage",
                    calibrator.artifact_id()
                ),
            }
            .into());
        }
        Ok(())
    }

    async fn load_calibration_dataset(
        &self,
        calibrator: &VerifiedModelScoreCalibration,
    ) -> QuantResult<TrainingDatasetInfo> {
        let dataset_id = calibrator
            .payload()
            .fit_contract
            .calibration_dataset
            .calibration_dataset_id;
        let dataset = self
            .deps
            .dataset_repo
            .find_by_id(&dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: dataset_id.to_string(),
            })?;
        if dataset.status != TrainingDatasetStatus::Ready
            || dataset.purpose != DatasetPurpose::Calibration
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!("calibrator {dataset_id} must bind a Ready/Calibration Dataset"),
            }
            .into());
        }
        Ok(dataset)
    }

    fn verify_calibration_binding(
        calibrator: &VerifiedModelScoreCalibration,
        dataset: &TrainingDatasetInfo,
        contract: &ModelServingContract,
        materialization: &TrainingDatasetMaterialization<'_>,
        training: &TrainingDatasetMaterialization<'_>,
        target_label_horizon_secs: u64,
    ) -> QuantResult<()> {
        let dataset_id = dataset.training_dataset_id;
        let bindings = contract.bindings();
        let calibration = &calibrator.payload().fit_contract.calibration_dataset;
        if calibration.dataset_hash != *materialization.dataset_hash
            || calibration.manifest_hash != *materialization.manifest_hash
            || calibration.artifact_bytes_hash != *materialization.artifact_bytes_hash
            || calibration.source_slice_manifest_hash
                != materialization
                    .manifest
                    .source_lineage
                    .source_slice
                    .manifest_hash
            || calibration.feature_schema_hash != *materialization.feature_schema_hash
            || calibration.factor_schema_hash != materialization.factor_schema_hash()
            || calibration.label_schema_hash != *materialization.label_schema_hash
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "calibrator {} Dataset fit contract differs from Dataset {dataset_id}",
                    calibrator.artifact_id()
                ),
            }
            .into());
        }
        let manifest = materialization.manifest;
        let training_manifest = training.manifest;
        let source_lineage = &manifest.source_lineage;
        let training_lineage = &training_manifest.source_lineage;
        let model_identity_matches = dataset.model_spec_id == bindings.model.model_spec_id
            && dataset.model_spec_definition_hash == bindings.model.model_spec_definition_hash
            && dataset.model_family == bindings.model.model_family
            && manifest.model_spec_id == bindings.model.model_spec_id
            && manifest.model_spec_definition_hash == bindings.model.model_spec_definition_hash
            && manifest.model_family == bindings.model.model_family;
        let schema_plane_matches = manifest.feature_schema_version
            == training_manifest.feature_schema_version
            && manifest.feature_schema_hash == bindings.schemas.feature_schema_hash
            && manifest.factor_serving_plane == bindings.factors.plane
            && manifest.label_schema_hash == bindings.schemas.label_schema_hash;
        let trade_policy_matches = manifest.trade_policy_artifact_id
            == training_manifest.trade_policy_artifact_id
            && manifest.trade_policy_hash == training_manifest.trade_policy_hash;
        let profile_matches =
            source_lineage.research_profile_artifact_id.profile_ref() == bindings.model.profile_ref;
        let policy_matches = source_lineage.decision_policy_snapshot_id
            == bindings.policy_snapshot.decision_policy_snapshot_id
            && source_lineage.runtime_config_hash == bindings.policy_snapshot.snapshot_hash;
        let source_contract_matches = source_lineage.research_program_hash
            == training_lineage.research_program_hash
            && source_lineage.reader_contract_version == training_lineage.reader_contract_version
            && source_lineage.schema_contract_version == training_lineage.schema_contract_version
            && source_lineage.source_schema_hash == training_lineage.source_schema_hash
            && source_lineage.capability_registry_hashes == bindings.capability_registry_hashes;
        let label_horizon_matches =
            label_horizon_matches(&manifest.horizons_secs, target_label_horizon_secs);
        if !model_identity_matches
            || !schema_plane_matches
            || !trade_policy_matches
            || !profile_matches
            || !policy_matches
            || !source_contract_matches
            || !label_horizon_matches
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "calibration Dataset {dataset_id} preimage differs from model {}: model_identity={model_identity_matches}, schema_plane={schema_plane_matches}, trade_policy={trade_policy_matches}, profile={profile_matches}, policy={policy_matches}, source_contract={source_contract_matches}, target_label_horizon={label_horizon_matches}",
                    bindings.model.model_version_id,
                ),
            }
            .into());
        }
        Ok(())
    }

    fn verify_calibration_window(
        source: &VerifiedModelServingPreimage,
        calibrator: &VerifiedModelScoreCalibration,
        dataset: &TrainingDatasetInfo,
        materialization: &TrainingDatasetMaterialization<'_>,
    ) -> QuantResult<()> {
        let dataset_id = dataset.training_dataset_id;
        let fit_window = calibrator.fit_window();
        if fit_window.from != dataset.window_start || fit_window.to != dataset.window_end {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "calibrator {} fit window differs from Calibration Dataset {dataset_id}",
                    calibrator.artifact_id()
                ),
            }
            .into());
        }
        let calibration_samples = u64::try_from(materialization.sample_count).map_err(|error| {
            ResearchError::DatasetBuild {
                detail: format!(
                    "Calibration Dataset {dataset_id} sample count is invalid: {error}"
                ),
            }
        })?;
        if calibrator.payload().reliability.n_samples > calibration_samples {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "calibrator {} uses more samples than Calibration Dataset {dataset_id}",
                    calibrator.artifact_id()
                ),
            }
            .into());
        }
        let embargo_secs = i64::try_from(
            source
                .policy_snapshot()
                .snapshot
                .model_routing
                .model
                .calibration
                .embargo_secs,
        )
        .map_err(|error| ResearchError::InvalidModelArtifact {
            detail: format!("model calibration embargo is invalid: {error}"),
        })?;
        assert_embargoed_after(
            fit_window,
            &TimeWindow::new(
                source.training_dataset().window_start,
                source.training_dataset().window_end,
            ),
            embargo_secs,
            "model calibration binding",
        )?;
        Ok(())
    }

    async fn verify_bias_preimage(
        &self,
        contract: &ModelServingContract,
        policy: &DecisionPolicySnapshotInfo,
    ) -> QuantResult<Option<Arc<FavoriteLongshotBiasTable>>> {
        let bindings = contract.bindings();
        let configured = if bindings.model.model_family.is_classical() {
            None
        } else {
            policy
                .snapshot
                .profile_artifacts
                .scoring
                .definition
                .structural
                .favorite_longshot
                .bias_table_ref
                .as_deref()
        };
        let configured = configured
            .map(|raw| {
                raw.parse::<CalibrationArtifactId>().map_err(|error| {
                    ResearchError::InvalidModelArtifact {
                        detail: format!("source bias-table reference `{raw}` is invalid: {error}"),
                    }
                })
            })
            .transpose()?;
        let bound = bindings.factors.bias_table.as_ref();
        if configured != bound.map(|binding| binding.artifact_id) {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "source model {} bias-table binding differs from its policy preimage",
                    bindings.model.model_version_id
                ),
            }
            .into());
        }
        let Some(binding) = bound else {
            return Ok(None);
        };
        let info = self
            .deps
            .calibration_repo
            .find_by_id(&binding.artifact_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "calibration_artifact",
                id: binding.artifact_id.to_string(),
            })?;
        let table = FavoriteLongshotBiasTable::from_persisted(&info)?;
        if binding.kind != CalibrationKind::MarketPriceBias
            || table.table_id != binding.artifact_id
            || table.content_hash != binding.content_hash
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "source model {} bias-table preimage differs from its serving binding",
                    bindings.model.model_version_id
                ),
            }
            .into());
        }
        Ok(Some(Arc::new(table)))
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, future};

    use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
    use tokio_util::sync::CancellationToken;

    use super::{ModelPreimageReadContext, label_horizon_matches};

    #[test]
    fn event_driven_label_matches() {
        assert!(label_horizon_matches(&[0], 0));
        assert!(label_horizon_matches(&[3_600, 86_400], 86_400));
        assert!(!label_horizon_matches(&[0], 86_400));
    }

    #[test]
    fn scope_drop_cancels_children() {
        let parent = CancellationToken::new();
        let context = ModelPreimageReadContext::new(&parent, None);
        let nested = context.cancel().child_token();
        drop(context);
        assert!(nested.is_cancelled());
        assert!(!parent.is_cancelled());
    }

    #[test]
    fn standalone_scope_cancels_children() {
        let context = ModelPreimageReadContext::default();
        let nested = context.cancel().child_token();
        drop(context);
        assert!(nested.is_cancelled());
    }

    #[tokio::test]
    async fn cancelled_scope_skips_work() {
        let parent = CancellationToken::new();
        let context = ModelPreimageReadContext::new(&parent, None);
        let polled = Cell::new(false);
        parent.cancel();
        let result = context
            .run(async {
                polled.set(true);
                Ok(())
            })
            .await;
        drop(context);
        assert!(matches!(
            result,
            Err(QuantError::Research(ResearchError::Cancelled { .. }))
        ));
        assert!(!polled.get());
    }

    #[tokio::test]
    async fn parent_cancels_waiting_scope() {
        let parent = CancellationToken::new();
        let context = ModelPreimageReadContext::new(&parent, None);
        let (result, ()) = tokio::join!(context.run(future::pending::<QuantResult<()>>()), async {
            parent.cancel();
        },);
        drop(context);
        assert!(matches!(
            result,
            Err(QuantError::Research(ResearchError::Cancelled { .. }))
        ));
    }
}
