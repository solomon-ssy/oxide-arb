//! Atomic serving generations for active and shadow Buy-model routes.
//!
//! Every fallible repository read, serving-preimage verification, runtime
//! construction, and route-scope check completes before a generation can be
//! published. Readers take one owned generation snapshot and keep it across
//! the complete requirements → selection → inference boundary, so a policy
//! activation cannot splice together routes from different generations.

use std::{collections::BTreeMap, sync::Arc};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use parking_lot::Mutex;
use quant_pivot_error::{QuantError, QuantResult, control::ControlError, research::ResearchError};
use quant_pivot_models::{
    domain::{governance::DecisionPolicySnapshotInfo, quant::ModelVersionInfo},
    enums::{common::MarketCategory, model::ModelFamily},
    runtime_config::{
        ActivePolicyBundle, BuyModelRoute, DecisionPolicySnapshot, ModelBinding,
        PolicyBundleIdentity,
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, ModelVersionId, PolicyBundleGeneration, Probability,
        ResearchProfileArtifactId,
    },
};
use quant_pivot_repository::traits::ModelRegistryRepository;
use quant_pivot_research::selection::ModelFeatureRequirements;
use rust_decimal::Decimal;

use crate::{
    governance::{active_load_ok, shadow_load_ok},
    service::model_serving_registry::{LoadedModelServingRuntime, ModelServingRuntimeRegistry},
};

/// Frozen policy identity and document used to resolve one serving generation.
#[derive(Clone, Copy)]
pub struct ModelServingGenerationRequest<'a> {
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
    pub snapshot: &'a DecisionPolicySnapshot,
}

impl<'a> From<&'a DecisionPolicySnapshotInfo> for ModelServingGenerationRequest<'a> {
    fn from(info: &'a DecisionPolicySnapshotInfo) -> Self {
        Self {
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            snapshot_hash: info.snapshot_hash,
            snapshot: &info.snapshot,
        }
    }
}

#[derive(Clone, Copy)]
struct ModelInferencePolicy {
    min_model_confidence: Decimal,
    candidate_score_floor: Decimal,
    shadow_diff_threshold: Decimal,
    minimum_shadow_decision_overlap: Probability,
    required_shadow_window_secs: u64,
}

#[derive(Clone, Copy)]
enum ModelServingGenerationAuthority {
    Activated(PolicyBundleGeneration),
    HistoricalSnapshot,
}

struct ServingModel {
    binding: ModelBinding,
    version: ModelVersionInfo,
    loaded: Arc<LoadedModelServingRuntime>,
}

/// One complete, immutable policy-to-runtime projection.
pub struct ModelServingGeneration {
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    snapshot_hash: ContentHash,
    active: BTreeMap<BuyModelRoute, Arc<ServingModel>>,
    shadow: BTreeMap<BuyModelRoute, Arc<ServingModel>>,
    inference_policy: ModelInferencePolicy,
    authority: ModelServingGenerationAuthority,
}

impl ModelServingGeneration {
    fn route_snapshot(
        generation: Arc<Self>,
        route: BuyModelRoute,
    ) -> Option<ModelServingRouteSnapshot> {
        let active = generation.active.get(&route).cloned()?;
        let shadow = generation.shadow.get(&route).cloned();
        Some(ModelServingRouteSnapshot {
            generation,
            route,
            active,
            shadow,
        })
    }

    /// Exact active contract hash for readiness and concurrency verification.
    #[must_use]
    pub fn active_contract_hash(&self, route: BuyModelRoute) -> Option<ContentHash> {
        self.active
            .get(&route)
            .map(|model| model.loaded.contract_hash())
    }

    /// Contract hash of one route-owned shadow, when present.
    #[must_use]
    pub fn shadow_contract(&self, route: BuyModelRoute) -> Option<ContentHash> {
        self.shadow
            .get(&route)
            .map(|model| model.loaded.contract_hash())
    }

    /// Durable policy snapshot represented by every route in this generation.
    #[must_use]
    pub const fn decision_policy_snapshot_id(&self) -> DecisionPolicySnapshotId {
        self.decision_policy_snapshot_id
    }

    /// Canonical policy document hash represented by this generation.
    #[must_use]
    pub const fn snapshot_hash(&self) -> ContentHash {
        self.snapshot_hash
    }
}

/// Exact active/shadow identity from one atomically published all-route generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedShadowRouteIdentity {
    pub route: BuyModelRoute,
    pub category_scope: Option<MarketCategory>,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub decision_policy_snapshot_hash: ContentHash,
    pub policy_bundle_generation: PolicyBundleGeneration,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_model_version_id: ModelVersionId,
    pub candidate_serving_contract_hash: ContentHash,
    pub shadow_bound_at: DateTime<Utc>,
    pub route_generation: u64,
    pub minimum_topn_decision_overlap: Probability,
    pub required_shadow_window_secs: u64,
}

/// Exact champion identity used to freeze one feedback occurrence before a
/// challenger recipe exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedChampionRouteIdentity {
    pub route: BuyModelRoute,
    pub category_scope: Option<MarketCategory>,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub decision_policy_snapshot_hash: ContentHash,
    pub policy_bundle_generation: PolicyBundleGeneration,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub champion_bound_at: DateTime<Utc>,
    pub route_generation: u64,
}

/// One route pinned to an owned immutable generation across await boundaries.
#[derive(Clone)]
pub struct ModelServingRouteSnapshot {
    generation: Arc<ModelServingGeneration>,
    route: BuyModelRoute,
    active: Arc<ServingModel>,
    shadow: Option<Arc<ServingModel>>,
}

impl ModelServingRouteSnapshot {
    #[must_use]
    pub const fn route(&self) -> BuyModelRoute {
        self.route
    }

    #[must_use]
    pub fn champion_model_version_id(&self) -> ModelVersionId {
        self.active.version.model_version_id
    }

    /// Freeze the exact route-owned champion without requiring a Shadow.
    pub fn published_champion_identity(
        &self,
    ) -> Result<PublishedChampionRouteIdentity, ControlError> {
        let ModelServingGenerationAuthority::Activated(policy_bundle_generation) =
            self.generation.authority
        else {
            return Err(ControlError::Precondition(
                "historical serving snapshots cannot start feedback cycles".to_owned(),
            ));
        };
        let contract_hash = self.active.loaded.contract_hash();
        if self.active.binding.model_version_id != self.active.version.model_version_id
            || self.active.version.category_scope != self.route.category()
        {
            return Err(ControlError::Precondition(
                "published champion binding differs from its model or route".to_owned(),
            ));
        }
        Ok(PublishedChampionRouteIdentity {
            route: self.route,
            category_scope: self.route.category(),
            research_profile_artifact_id: self.active.version.profile_ref.artifact_id(),
            decision_policy_snapshot_id: self.generation.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: self.generation.snapshot_hash,
            policy_bundle_generation,
            champion_model_version_id: self.active.version.model_version_id,
            champion_serving_contract_hash: contract_hash,
            champion_bound_at: self.active.binding.bound_at,
            route_generation: self.active.binding.generation,
        })
    }

    /// Require this route to contain two distinct contracts from the current
    /// atomically published policy generation.
    pub fn published_shadow_identity(&self) -> Result<PublishedShadowRouteIdentity, ControlError> {
        let ModelServingGenerationAuthority::Activated(policy_bundle_generation) =
            self.generation.authority
        else {
            return Err(ControlError::Precondition(
                "historical serving snapshots cannot produce production shadow evidence".to_owned(),
            ));
        };
        let shadow = self.shadow.as_ref().ok_or_else(|| {
            ControlError::Precondition(format!(
                "published route {:?} has no configured shadow model",
                self.route
            ))
        })?;
        let active_contract = self.active.loaded.contract_hash();
        let shadow_contract = shadow.loaded.contract_hash();
        if self.active.version.model_version_id == shadow.version.model_version_id
            || self.active.binding.model_version_id != self.active.version.model_version_id
            || shadow.binding.model_version_id != shadow.version.model_version_id
            || active_contract == shadow_contract
            || self.active.version.profile_ref != shadow.version.profile_ref
            || self.active.version.category_scope != self.route.category()
            || shadow.version.category_scope != self.route.category()
        {
            return Err(ControlError::Precondition(
                "published active/shadow route has aliased subjects, contract, profile, or category"
                    .to_owned(),
            ));
        }
        Ok(PublishedShadowRouteIdentity {
            route: self.route,
            category_scope: self.route.category(),
            research_profile_artifact_id: self.active.version.profile_ref.artifact_id(),
            decision_policy_snapshot_id: self.generation.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: self.generation.snapshot_hash,
            policy_bundle_generation,
            champion_model_version_id: self.active.version.model_version_id,
            champion_serving_contract_hash: active_contract,
            candidate_model_version_id: shadow.version.model_version_id,
            candidate_serving_contract_hash: shadow_contract,
            shadow_bound_at: shadow.binding.bound_at,
            route_generation: shadow.binding.generation,
            minimum_topn_decision_overlap: self
                .generation
                .inference_policy
                .minimum_shadow_decision_overlap,
            required_shadow_window_secs: self
                .generation
                .inference_policy
                .required_shadow_window_secs,
        })
    }

    #[must_use]
    pub fn decision_policy_snapshot_id(&self) -> DecisionPolicySnapshotId {
        self.generation.decision_policy_snapshot_id
    }

    pub(crate) fn ensure_policy(
        &self,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
    ) -> QuantResult<()> {
        if self.decision_policy_snapshot_id() == decision_policy_snapshot_id {
            return Ok(());
        }
        Err(ResearchError::InvalidModelArtifact {
            detail: format!(
                "serving generation policy {} differs from model-run policy \
                 {decision_policy_snapshot_id}",
                self.decision_policy_snapshot_id()
            ),
        }
        .into())
    }

    pub(crate) fn active_version(&self) -> &ModelVersionInfo {
        &self.active.version
    }

    pub(crate) fn active_runtime(&self) -> &LoadedModelServingRuntime {
        self.active.loaded.as_ref()
    }

    pub(crate) fn shadow(&self) -> Option<(&ModelVersionInfo, &LoadedModelServingRuntime)> {
        self.shadow
            .as_ref()
            .map(|model| (&model.version, model.loaded.as_ref()))
    }

    pub(crate) fn validate_active(&self) -> QuantResult<()> {
        let version = self.active_version();
        active_load_ok(version).map_err(|reason| {
            QuantError::config(format!(
                "active model {} load denied: {reason}",
                version.model_version_id
            ))
        })
    }

    pub(crate) fn validate_shadow(version: &ModelVersionInfo) -> QuantResult<()> {
        shadow_load_ok(version).map_err(|reason| {
            QuantError::config(format!(
                "shadow model {} load denied: {reason}",
                version.model_version_id
            ))
        })
    }

    #[must_use]
    pub(crate) fn model_requirements(&self) -> ModelFeatureRequirements {
        let required = self.active_runtime().runtime().required_features();
        match self.route {
            BuyModelRoute::Pooled => ModelFeatureRequirements {
                generic: required,
                by_category: BTreeMap::new(),
            },
            BuyModelRoute::Crypto => ModelFeatureRequirements {
                generic: Vec::new(),
                by_category: BTreeMap::from([(MarketCategory::Crypto, required)]),
            },
            BuyModelRoute::Weather => ModelFeatureRequirements {
                generic: Vec::new(),
                by_category: BTreeMap::from([(MarketCategory::Weather, required)]),
            },
        }
    }

    #[must_use]
    pub(crate) fn min_model_confidence(&self) -> Decimal {
        self.generation.inference_policy.min_model_confidence
    }

    #[must_use]
    pub(crate) fn candidate_score_floor(&self) -> Decimal {
        self.generation.inference_policy.candidate_score_floor
    }

    #[must_use]
    pub(crate) fn shadow_diff_threshold(&self) -> Decimal {
        self.generation.inference_policy.shadow_diff_threshold
    }
}

#[derive(Clone, Copy)]
enum ServingRole {
    Active(BuyModelRoute),
    Shadow(BuyModelRoute),
}

impl ServingRole {
    fn path(self) -> String {
        match self {
            Self::Active(BuyModelRoute::Pooled) => "model.buy_routes.pooled.champion".to_owned(),
            Self::Active(BuyModelRoute::Crypto) => "model.buy_routes.crypto.champion".to_owned(),
            Self::Active(BuyModelRoute::Weather) => "model.buy_routes.weather.champion".to_owned(),
            Self::Shadow(BuyModelRoute::Pooled) => "model.buy_routes.pooled.shadow".to_owned(),
            Self::Shadow(BuyModelRoute::Crypto) => "model.buy_routes.crypto.shadow".to_owned(),
            Self::Shadow(BuyModelRoute::Weather) => "model.buy_routes.weather.shadow".to_owned(),
        }
    }
}

#[derive(Clone)]
struct ServingPointer {
    role: ServingRole,
    binding: ModelBinding,
}

struct LoadedServingPointer {
    role: ServingRole,
    route: BuyModelRoute,
    model: ServingModel,
}

#[async_trait]
trait ModelServingPointerResolver: Send + Sync {
    async fn load_pointer(
        &self,
        pointer: ServingPointer,
    ) -> Result<LoadedServingPointer, ControlError>;
}

struct RepositoryServingPointerResolver {
    model_registry: Arc<dyn ModelRegistryRepository>,
    runtime_registry: Arc<ModelServingRuntimeRegistry>,
}

#[async_trait]
impl ModelServingPointerResolver for RepositoryServingPointerResolver {
    async fn load_pointer(
        &self,
        pointer: ServingPointer,
    ) -> Result<LoadedServingPointer, ControlError> {
        let path = pointer.role.path();
        let version = self
            .model_registry
            .find_model_version(&pointer.binding.model_version_id)
            .await
            .map_err(|error| ControlError::Precondition(format!("{path} load failed: {error}")))?
            .ok_or_else(|| {
                ControlError::Precondition(format!(
                    "{path} = `{}` not found",
                    pointer.binding.model_version_id
                ))
            })?;
        let load_result = match pointer.role {
            ServingRole::Active(_) => active_load_ok(&version),
            ServingRole::Shadow(_) => shadow_load_ok(&version),
        };
        if let Err(reason) = load_result {
            return Err(ControlError::Precondition(format!(
                "{path} = `{}` load denied: {reason}",
                pointer.binding.model_version_id
            )));
        }
        let loaded = self
            .runtime_registry
            .load(&version)
            .await
            .map_err(|error| {
                ControlError::Precondition(format!(
                    "{path} = `{}` serving preimage failed: {error}",
                    pointer.binding.model_version_id
                ))
            })?;
        let runtime = loaded.runtime();
        let route = BuyModelRoute::try_from(runtime.category_scope())
            .map_err(|error| ControlError::Precondition(format!("{path}: {error}")))?;
        match pointer.role {
            ServingRole::Active(expected) => {
                if route != expected {
                    return Err(ControlError::Precondition(format!(
                        "{path} = `{}` resolved route {route:?}; expected {expected:?}",
                        pointer.binding.model_version_id
                    )));
                }
                ensure_active_family(runtime.model_family(), &path)?;
            }
            ServingRole::Shadow(expected) => {
                if route != expected {
                    return Err(ControlError::Precondition(format!(
                        "{path} resolved route {route:?}; expected {expected:?}"
                    )));
                }
                ensure_shadow_family(runtime.model_family(), &path)?;
            }
        }
        Ok(LoadedServingPointer {
            role: pointer.role,
            route,
            model: ServingModel {
                binding: pointer.binding,
                version,
                loaded,
            },
        })
    }
}

/// Fully loaded generation awaiting a durable policy publication identity.
pub(crate) struct PreparedModelServingGeneration {
    snapshot_hash: ContentHash,
    active: BTreeMap<BuyModelRoute, Arc<ServingModel>>,
    shadow: BTreeMap<BuyModelRoute, Arc<ServingModel>>,
    inference_policy: ModelInferencePolicy,
}

impl PreparedModelServingGeneration {
    fn finalize(
        self,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        snapshot_hash: ContentHash,
        authority: ModelServingGenerationAuthority,
    ) -> Result<ModelServingGeneration, ControlError> {
        if self.snapshot_hash != snapshot_hash {
            return Err(ControlError::Precondition(format!(
                "prepared serving generation hash {} differs from committed policy hash \
                 {snapshot_hash}",
                self.snapshot_hash
            )));
        }
        let expected_id = DecisionPolicySnapshotId::from_content_hash(&snapshot_hash);
        if decision_policy_snapshot_id != expected_id {
            return Err(ControlError::Precondition(format!(
                "committed policy identity {decision_policy_snapshot_id} differs from \
                 content-addressed identity {expected_id}"
            )));
        }
        Ok(ModelServingGeneration {
            decision_policy_snapshot_id,
            snapshot_hash,
            active: self.active,
            shadow: self.shadow,
            inference_policy: self.inference_policy,
            authority,
        })
    }
}

/// Complete-generation builder plus the single atomically published current
/// generation.
pub struct ModelServingGenerationStore {
    current: ArcSwap<ModelServingGeneration>,
    publication: Mutex<PolicyBundleIdentity>,
    pointer_resolver: Arc<dyn ModelServingPointerResolver>,
}

impl ModelServingGenerationStore {
    /// Build and publish the boot policy's complete generation before any
    /// reconciler or report worker can start.
    ///
    /// # Errors
    ///
    /// Fails boot when any configured active/shadow pointer or complete serving
    /// preimage cannot be resolved.
    pub async fn bootstrap(
        model_registry: Arc<dyn ModelRegistryRepository>,
        runtime_registry: Arc<ModelServingRuntimeRegistry>,
        bundle: ActivePolicyBundle,
    ) -> QuantResult<Self> {
        Self::bootstrap_with_resolver(
            bundle,
            Arc::new(RepositoryServingPointerResolver {
                model_registry,
                runtime_registry,
            }),
        )
        .await
    }

    async fn bootstrap_with_resolver(
        bundle: ActivePolicyBundle,
        pointer_resolver: Arc<dyn ModelServingPointerResolver>,
    ) -> QuantResult<Self> {
        let published = PolicyBundleIdentity::from(&bundle);
        let prepared = Self::prepare_with_resolver(pointer_resolver.as_ref(), &bundle.snapshot)
            .await
            .map_err(QuantError::from)?;
        let generation = prepared
            .finalize(
                published.decision_policy_snapshot_id,
                published.snapshot_hash,
                ModelServingGenerationAuthority::Activated(published.generation),
            )
            .map_err(QuantError::from)?;
        Ok(Self {
            current: ArcSwap::from(Arc::new(generation)),
            publication: Mutex::new(published),
            pointer_resolver,
        })
    }

    /// Resolve and pin the exact route from a frozen durable policy snapshot.
    ///
    /// The current generation is reused only on exact ID+hash equality.
    /// Historical report recovery builds an owned immutable generation through
    /// the same resolver without replacing the current publication.
    ///
    /// # Errors
    ///
    /// Rejects policy hash drift, invalid report scope, or any unresolved
    /// serving route/preimage.
    pub async fn resolve_route(
        &self,
        request: ModelServingGenerationRequest<'_>,
    ) -> QuantResult<ModelServingRouteSnapshot> {
        let actual_hash = request
            .snapshot
            .persistence_hash()
            .map_err(|error| QuantError::config(error.to_string()))?;
        if actual_hash != request.snapshot_hash {
            return Err(QuantError::config(format!(
                "policy snapshot {} content hash {} differs from persisted hash {}",
                request.decision_policy_snapshot_id, actual_hash, request.snapshot_hash
            )));
        }
        let expected_id = DecisionPolicySnapshotId::from_content_hash(&actual_hash);
        if request.decision_policy_snapshot_id != expected_id {
            return Err(QuantError::config(format!(
                "policy snapshot identity {} differs from content-addressed identity {expected_id}",
                request.decision_policy_snapshot_id
            )));
        }
        let current = self.current.load_full();
        let generation = if current.decision_policy_snapshot_id
            == request.decision_policy_snapshot_id
            && current.snapshot_hash == request.snapshot_hash
        {
            current
        } else {
            let prepared = self
                .prepare(request.snapshot)
                .await
                .map_err(QuantError::from)?;
            Arc::new(
                prepared
                    .finalize(
                        request.decision_policy_snapshot_id,
                        request.snapshot_hash,
                        ModelServingGenerationAuthority::HistoricalSnapshot,
                    )
                    .map_err(QuantError::from)?,
            )
        };
        let route = BuyModelRoute::try_from(&request.snapshot.recommendation.selection)
            .map_err(|error| QuantError::config(error.to_string()))?;
        let generation_id = generation.decision_policy_snapshot_id;
        ModelServingGeneration::route_snapshot(generation, route).ok_or_else(|| {
            ResearchError::InvalidModelArtifact {
                detail: format!(
                    "serving generation {generation_id} has no exact active route {route:?}"
                ),
            }
            .into()
        })
    }

    /// Owned current generation for readiness and tests.
    #[must_use]
    pub fn current(&self) -> Arc<ModelServingGeneration> {
        self.current.load_full()
    }

    /// Pin one route from the currently published all-route generation.
    #[must_use]
    pub fn current_route(&self, route: BuyModelRoute) -> Option<ModelServingRouteSnapshot> {
        ModelServingGeneration::route_snapshot(self.current.load_full(), route)
    }

    pub(crate) async fn prepare(
        &self,
        snapshot: &DecisionPolicySnapshot,
    ) -> Result<PreparedModelServingGeneration, ControlError> {
        Self::prepare_with_resolver(self.pointer_resolver.as_ref(), snapshot).await
    }

    async fn prepare_with_resolver(
        pointer_resolver: &dyn ModelServingPointerResolver,
        snapshot: &DecisionPolicySnapshot,
    ) -> Result<PreparedModelServingGeneration, ControlError> {
        let snapshot_hash = snapshot
            .persistence_hash()
            .map_err(|error| ControlError::Precondition(error.to_string()))?;
        let selected_route = if snapshot
            .recommendation
            .selection
            .enabled_categories
            .is_empty()
        {
            None
        } else {
            Some(
                BuyModelRoute::try_from(&snapshot.recommendation.selection)
                    .map_err(|error| ControlError::Precondition(error.to_string()))?,
            )
        };
        if let Some(route) = selected_route {
            snapshot
                .model_routing
                .model
                .champion(route)
                .map_err(|error| ControlError::Precondition(error.to_string()))?;
        }

        let model = &snapshot.model_routing.model;
        let mut pointers = Vec::with_capacity(model.buy_routes.len().saturating_mul(2));
        for (route, binding) in &model.buy_routes {
            pointers.push(ServingPointer {
                role: ServingRole::Active(*route),
                binding: binding.champion.clone(),
            });
            if let Some(shadow) = &binding.shadow {
                pointers.push(ServingPointer {
                    role: ServingRole::Shadow(*route),
                    binding: shadow.clone(),
                });
            }
        }

        let loaded = join_all(
            pointers
                .into_iter()
                .map(|pointer| pointer_resolver.load_pointer(pointer)),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
        let mut active = BTreeMap::new();
        let mut shadow = BTreeMap::new();
        for pointer in loaded {
            match pointer.role {
                ServingRole::Active(route) => {
                    if active.insert(route, Arc::new(pointer.model)).is_some() {
                        return Err(ControlError::Precondition(format!(
                            "serving generation contains duplicate active route {route:?}"
                        )));
                    }
                }
                ServingRole::Shadow(route) => {
                    if route != pointer.route
                        || shadow.insert(route, Arc::new(pointer.model)).is_some()
                    {
                        return Err(ControlError::Precondition(format!(
                            "serving generation contains duplicate or mismatched shadow route \
                             {route:?}"
                        )));
                    }
                }
            }
        }

        for (route, active_model) in &active {
            validate_policy_profiles(snapshot, *route, &active_model.loaded)?;
        }
        for (route, shadow_model) in &shadow {
            validate_policy_profiles(snapshot, *route, &shadow_model.loaded)?;
        }

        if let Some(route) = selected_route {
            active.get(&route).ok_or_else(|| {
                ControlError::Precondition(format!(
                    "selected report route {route:?} has no loaded active model"
                ))
            })?;
        }

        Ok(PreparedModelServingGeneration {
            snapshot_hash,
            active,
            shadow,
            inference_policy: inference_policy(snapshot),
        })
    }

    pub(crate) fn publish_committed(
        &self,
        prepared: PreparedModelServingGeneration,
        bundle: &PolicyBundleIdentity,
    ) -> Result<(), ControlError> {
        let mut current = self.publication.lock();
        if bundle.generation < current.generation {
            return Ok(());
        }
        if bundle.generation == current.generation {
            if bundle.decision_policy_snapshot_id == current.decision_policy_snapshot_id
                && bundle.snapshot_hash == current.snapshot_hash
            {
                return Ok(());
            }
            return Err(ControlError::Precondition(
                "same serving generation resolved to a different policy identity or hash"
                    .to_owned(),
            ));
        }
        let generation = prepared.finalize(
            bundle.decision_policy_snapshot_id,
            bundle.snapshot_hash,
            ModelServingGenerationAuthority::Activated(bundle.generation),
        )?;
        self.current.store(Arc::new(generation));
        *current = *bundle;
        drop(current);
        Ok(())
    }
}

const fn inference_policy(snapshot: &DecisionPolicySnapshot) -> ModelInferencePolicy {
    let model = &snapshot.model_routing.model;
    ModelInferencePolicy {
        min_model_confidence: model.min_model_confidence.value(),
        candidate_score_floor: model.candidate_score_floor.value(),
        shadow_diff_threshold: model.shadow_diff_threshold.value(),
        minimum_shadow_decision_overlap: Probability::new(
            snapshot
                .profile_artifacts
                .research_method
                .model_promotion
                .min_shadow_decision_overlap
                .value,
        ),
        required_shadow_window_secs: snapshot
            .profile_artifacts
            .research_method
            .model_promotion
            .required_shadow_window_secs,
    }
}

fn validate_policy_profiles(
    snapshot: &DecisionPolicySnapshot,
    route: BuyModelRoute,
    loaded: &LoadedModelServingRuntime,
) -> Result<(), ControlError> {
    let current = snapshot
        .profile_artifacts
        .references()
        .map_err(|error| ControlError::Precondition(error.to_string()))?;
    let bound = &loaded
        .contract()
        .bindings()
        .policy_snapshot
        .profile_artifacts;
    if current == *bound {
        return Ok(());
    }
    Err(ControlError::Precondition(format!(
        "selected route {route:?} serving contract was built from different immutable policy \
         profile artifacts"
    )))
}

fn ensure_active_family(family: ModelFamily, path: &str) -> Result<(), ControlError> {
    if family == ModelFamily::WeightedFactor {
        return Ok(());
    }
    let detail = if family.is_classical() {
        format!(
            "classical family {family} is ShadowOnly until a governed probability-to-return \
             calibration is frozen"
        )
    } else {
        format!("Buy-side serving does not support family {family}")
    };
    Err(ControlError::Precondition(format!("{path}: {detail}")))
}

fn ensure_shadow_family(family: ModelFamily, path: &str) -> Result<(), ControlError> {
    if family == ModelFamily::WeightedFactor || family.is_classical() {
        return Ok(());
    }
    Err(ControlError::Precondition(format!(
        "{path}: Buy-side shadow serving does not support family {family}"
    )))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use chrono::Utc;
    use parking_lot::Mutex;
    use quant_pivot_error::{
        QuantResult,
        control::{ControlError, RuntimeApplyStage},
    };
    use quant_pivot_models::{
        domain::{
            ports::{CommittedPolicyApplyPort, PolicySnapshotPort, PreparedPolicySnapshot},
            quant::ModelVersionInfo,
        },
        enums::{common::MarketCategory, model::ModelFamily, quant::ModelWeightSource},
        runtime_config::{
            ActivePolicyBundle, BuyModelRoute, BuyRouteBinding, DecisionPolicySnapshot,
            FactorCrossSectionConfig, ModelBinding, ModelBindingSource, PolicyApplyDegradedCause,
            PolicyApplyReadiness, PolicyBundleIdentity,
        },
        types::{
            ContentHash, DecisionPolicySnapshotId, ModelVersionId, PolicyBundleGeneration,
            factor::FactorServingPlane, stable_name::FeatureName,
        },
    };
    use quant_pivot_research::{
        factors::FrozenReferenceQuantiles,
        model::{ModelRuntimeInput, ModelRuntimeMetrics, ModelRuntimeOutput, QuantModelRuntime},
    };

    use super::{
        LoadedServingPointer, ModelServingGeneration, ModelServingGenerationAuthority,
        ModelServingGenerationRequest, ModelServingGenerationStore, ModelServingPointerResolver,
        ServingModel, ServingPointer, ServingRole,
    };
    use crate::{
        runtime_config::{CommittedPolicyApplicator, DecisionPolicyStore},
        service::{
            model_serving_registry::LoadedModelServingRuntime,
            model_serving_test_support::{model_artifact, model_version},
        },
    };

    struct StubRuntime {
        version_id: ModelVersionId,
        category_scope: Option<MarketCategory>,
        feature_schema_hash: ContentHash,
        factor_plane: FactorServingPlane,
    }

    #[async_trait]
    impl QuantModelRuntime for StubRuntime {
        fn model_version_id(&self) -> ModelVersionId {
            self.version_id
        }

        fn model_family(&self) -> ModelFamily {
            ModelFamily::WeightedFactor
        }

        fn feature_schema_hash(&self) -> ContentHash {
            self.feature_schema_hash
        }

        fn required_features(&self) -> Vec<FeatureName> {
            Vec::new()
        }

        fn category_scope(&self) -> Option<MarketCategory> {
            self.category_scope
        }

        fn weight_source(&self) -> ModelWeightSource {
            ModelWeightSource::Artifact
        }

        fn factor_cross_section(&self) -> Option<&FactorCrossSectionConfig> {
            None
        }

        fn factor_serving_plane(&self) -> Option<&FactorServingPlane> {
            Some(&self.factor_plane)
        }

        fn frozen_reference_quantiles(&self) -> Option<&FrozenReferenceQuantiles> {
            None
        }

        async fn infer_batch(&self, _input: ModelRuntimeInput) -> QuantResult<ModelRuntimeOutput> {
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

    #[derive(Clone)]
    struct StubEntry {
        version: ModelVersionInfo,
        loaded: Arc<LoadedModelServingRuntime>,
    }

    struct StubPointerResolver {
        entries: HashMap<ModelVersionId, StubEntry>,
        failures: Mutex<HashSet<ModelVersionId>>,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
    }

    impl StubPointerResolver {
        fn new(entries: HashMap<ModelVersionId, StubEntry>) -> Self {
            Self {
                entries,
                failures: Mutex::new(HashSet::new()),
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
            }
        }

        fn fail(&self, model_version_id: ModelVersionId) {
            self.failures.lock().insert(model_version_id);
        }

        fn recover(&self, model_version_id: &ModelVersionId) {
            self.failures.lock().remove(model_version_id);
        }

        fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ModelServingPointerResolver for StubPointerResolver {
        async fn load_pointer(
            &self,
            pointer: ServingPointer,
        ) -> Result<LoadedServingPointer, ControlError> {
            let active = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            let result = async {
                if self
                    .failures
                    .lock()
                    .contains(&pointer.binding.model_version_id)
                {
                    return Err(ControlError::Precondition(format!(
                        "injected serving failure for {}",
                        pointer.binding.model_version_id
                    )));
                }
                let entry = self
                    .entries
                    .get(&pointer.binding.model_version_id)
                    .cloned()
                    .ok_or_else(|| {
                        ControlError::Precondition(format!(
                            "missing stub model {}",
                            pointer.binding.model_version_id
                        ))
                    })?;
                let route = BuyModelRoute::try_from(entry.version.category_scope)
                    .map_err(|error| ControlError::Precondition(format!("stub route: {error}")))?;
                let expected = match pointer.role {
                    ServingRole::Active(expected) | ServingRole::Shadow(expected) => expected,
                };
                if route == expected {
                    Ok(LoadedServingPointer {
                        role: pointer.role,
                        route,
                        model: ServingModel {
                            binding: pointer.binding,
                            version: entry.version,
                            loaded: entry.loaded,
                        },
                    })
                } else {
                    Err(ControlError::Precondition(format!(
                        "stub route {route:?} differs from {expected:?}"
                    )))
                }
            }
            .await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            result
        }
    }

    struct VersionInventory {
        pooled: ModelVersionInfo,
        crypto: ModelVersionInfo,
        weather: ModelVersionInfo,
        shadow: ModelVersionInfo,
        weather_shadow: ModelVersionInfo,
    }

    impl VersionInventory {
        fn fixture() -> Self {
            Self {
                pooled: model_version(&model_artifact(None)),
                crypto: model_version(&model_artifact(Some(MarketCategory::Crypto))),
                weather: model_version(&model_artifact(Some(MarketCategory::Weather))),
                shadow: model_version(&model_artifact(None)),
                weather_shadow: model_version(&model_artifact(Some(MarketCategory::Weather))),
            }
        }

        fn entries(&self) -> HashMap<ModelVersionId, StubEntry> {
            [
                &self.pooled,
                &self.crypto,
                &self.weather,
                &self.shadow,
                &self.weather_shadow,
            ]
            .into_iter()
            .map(|version| {
                let bindings = version.serving_contract.bindings();
                let runtime: Arc<dyn QuantModelRuntime> = Arc::new(StubRuntime {
                    version_id: version.model_version_id,
                    category_scope: version.category_scope,
                    feature_schema_hash: bindings.schemas.feature_schema_hash,
                    factor_plane: bindings.factors.plane.clone(),
                });
                let loaded = LoadedModelServingRuntime::from_loader(version, runtime, None)
                    .expect("valid serving entry");
                (
                    version.model_version_id,
                    StubEntry {
                        version: version.clone(),
                        loaded: Arc::new(loaded),
                    },
                )
            })
            .collect()
        }
    }

    fn binding(model_version_id: ModelVersionId, generation: u64) -> ModelBinding {
        ModelBinding::new(
            model_version_id,
            ModelBindingSource::Bootstrap,
            Utc::now(),
            PolicyBundleGeneration::FIRST,
            generation,
        )
    }

    fn policy(
        inventory: &VersionInventory,
        shadow: Option<(BuyModelRoute, ModelVersionId)>,
    ) -> DecisionPolicySnapshot {
        let mut snapshot = DecisionPolicySnapshot::default();
        snapshot.recommendation.selection.enabled_categories = vec![MarketCategory::Politics];
        snapshot.model_routing.model.buy_routes = [
            (
                BuyModelRoute::Pooled,
                BuyRouteBinding {
                    champion: binding(inventory.pooled.model_version_id, 1),
                    shadow: None,
                },
            ),
            (
                BuyModelRoute::Crypto,
                BuyRouteBinding {
                    champion: binding(inventory.crypto.model_version_id, 1),
                    shadow: None,
                },
            ),
            (
                BuyModelRoute::Weather,
                BuyRouteBinding {
                    champion: binding(inventory.weather.model_version_id, 1),
                    shadow: None,
                },
            ),
        ]
        .into_iter()
        .collect();
        if let Some((route, model_version_id)) = shadow {
            snapshot
                .model_routing
                .model
                .buy_routes
                .get_mut(&route)
                .expect("route fixture")
                .shadow = Some(binding(model_version_id, 2));
        }
        snapshot
    }

    fn bundle(
        generation: PolicyBundleGeneration,
        snapshot: DecisionPolicySnapshot,
    ) -> ActivePolicyBundle {
        let snapshot_hash = snapshot.persistence_hash().expect("policy hash");
        ActivePolicyBundle::from_parts(
            generation,
            DecisionPolicySnapshotId::from_content_hash(&snapshot_hash),
            snapshot_hash,
            snapshot,
        )
    }

    struct ServingPolicyPort {
        policies: Arc<DecisionPolicyStore>,
        generations: Arc<ModelServingGenerationStore>,
    }

    #[async_trait]
    impl PolicySnapshotPort for ServingPolicyPort {
        fn current(&self) -> Arc<DecisionPolicySnapshot> {
            self.policies.current()
        }

        async fn prepare(
            &self,
            config: DecisionPolicySnapshot,
        ) -> Result<PreparedPolicySnapshot, ControlError> {
            let prepared = self.generations.prepare(&config).await?;
            let policies = Arc::clone(&self.policies);
            let generations = Arc::clone(&self.generations);
            Ok(PreparedPolicySnapshot::new_governed(
                Arc::new(config),
                move |bundle| {
                    let bundle = bundle.ok_or_else(|| {
                        ControlError::Precondition(
                            "serving test publication requires committed identity".to_owned(),
                        )
                    })?;
                    let identity = PolicyBundleIdentity::from(&bundle);
                    policies
                        .publish_committed(bundle, move |_config| {
                            generations.publish_committed(prepared, &identity)
                        })
                        .map(|_outcome| ())
                },
            ))
        }
    }

    #[tokio::test]
    async fn inventory_loads_concurrently() {
        let versions = VersionInventory::fixture();
        let resolver = Arc::new(StubPointerResolver::new(versions.entries()));
        let active = bundle(
            PolicyBundleGeneration::FIRST,
            policy(
                &versions,
                Some((BuyModelRoute::Pooled, versions.shadow.model_version_id)),
            ),
        );
        let store = ModelServingGenerationStore::bootstrap_with_resolver(
            active,
            Arc::clone(&resolver) as Arc<dyn ModelServingPointerResolver>,
        )
        .await
        .expect("complete serving generation");
        let current = store.current();

        assert_eq!(
            current.active_contract_hash(BuyModelRoute::Pooled),
            Some(versions.pooled.serving_contract_hash)
        );
        assert_eq!(
            current.active_contract_hash(BuyModelRoute::Crypto),
            Some(versions.crypto.serving_contract_hash)
        );
        assert_eq!(
            current.active_contract_hash(BuyModelRoute::Weather),
            Some(versions.weather.serving_contract_hash)
        );
        assert_eq!(
            current.shadow_contract(BuyModelRoute::Pooled),
            Some(versions.shadow.serving_contract_hash)
        );
        assert!(
            resolver.max_in_flight() >= 4,
            "all configured routes must prepare concurrently"
        );
    }

    #[tokio::test]
    async fn published_shadow_requires_current() {
        let versions = VersionInventory::fixture();
        let resolver = Arc::new(StubPointerResolver::new(versions.entries()));
        let active = bundle(
            PolicyBundleGeneration::FIRST,
            policy(
                &versions,
                Some((BuyModelRoute::Pooled, versions.shadow.model_version_id)),
            ),
        );
        let store = ModelServingGenerationStore::bootstrap_with_resolver(
            active.clone(),
            resolver as Arc<dyn ModelServingPointerResolver>,
        )
        .await
        .expect("complete serving generation");
        let published = store
            .current_route(BuyModelRoute::Pooled)
            .expect("current pooled route")
            .published_shadow_identity()
            .expect("published shadow identity");
        assert_eq!(
            published.champion_model_version_id,
            versions.pooled.model_version_id
        );
        assert_eq!(
            published.candidate_model_version_id,
            versions.shadow.model_version_id
        );
        assert_eq!(
            published.policy_bundle_generation,
            PolicyBundleGeneration::FIRST
        );

        let historical = Arc::new(
            store
                .prepare(&active.snapshot)
                .await
                .expect("prepare historical generation")
                .finalize(
                    active.decision_policy_snapshot_id,
                    active.snapshot_hash,
                    ModelServingGenerationAuthority::HistoricalSnapshot,
                )
                .expect("finalize historical generation"),
        );
        let historical_route =
            ModelServingGeneration::route_snapshot(historical, BuyModelRoute::Pooled)
                .expect("historical pooled route");
        assert!(
            historical_route.published_shadow_identity().is_err(),
            "historical replays cannot create production shadow evidence"
        );
    }

    #[tokio::test]
    async fn unselected_shadow_is_allowed() {
        let versions = VersionInventory::fixture();
        let resolver = Arc::new(StubPointerResolver::new(versions.entries()));
        let active = bundle(
            PolicyBundleGeneration::FIRST,
            policy(
                &versions,
                Some((
                    BuyModelRoute::Weather,
                    versions.weather_shadow.model_version_id,
                )),
            ),
        );
        let store = ModelServingGenerationStore::bootstrap_with_resolver(
            active,
            resolver as Arc<dyn ModelServingPointerResolver>,
        )
        .await
        .expect("unselected route shadow is independently valid");
        assert!(
            store
                .current_route(BuyModelRoute::Pooled)
                .expect("pooled route")
                .shadow()
                .is_none()
        );
        assert_eq!(
            store
                .current_route(BuyModelRoute::Weather)
                .expect("weather route")
                .shadow()
                .map(|(version, _)| version.model_version_id),
            Some(versions.weather_shadow.model_version_id)
        );
    }

    #[tokio::test]
    async fn policy_identity_drift_rejected() {
        let versions = VersionInventory::fixture();
        let resolver = Arc::new(StubPointerResolver::new(versions.entries()));
        let active = bundle(
            PolicyBundleGeneration::FIRST,
            policy(
                &versions,
                Some((BuyModelRoute::Pooled, versions.shadow.model_version_id)),
            ),
        );
        let store = ModelServingGenerationStore::bootstrap_with_resolver(
            active.clone(),
            resolver as Arc<dyn ModelServingPointerResolver>,
        )
        .await
        .expect("complete serving generation");
        let result = store
            .resolve_route(ModelServingGenerationRequest {
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                snapshot_hash: active.snapshot_hash,
                snapshot: &active.snapshot,
            })
            .await;

        assert!(
            result.is_err(),
            "a policy ID not derived from the frozen content hash must fail closed"
        );
    }

    #[tokio::test]
    async fn weather_route_resolves_exactly() {
        let versions = VersionInventory::fixture();
        let resolver = Arc::new(StubPointerResolver::new(versions.entries()));
        let mut snapshot = policy(
            &versions,
            Some((
                BuyModelRoute::Weather,
                versions.weather_shadow.model_version_id,
            )),
        );
        snapshot.recommendation.selection.enabled_categories = vec![MarketCategory::Weather];
        let active = bundle(PolicyBundleGeneration::FIRST, snapshot);
        let store = ModelServingGenerationStore::bootstrap_with_resolver(
            active.clone(),
            resolver as Arc<dyn ModelServingPointerResolver>,
        )
        .await
        .expect("Weather generation");
        let route = store
            .resolve_route(ModelServingGenerationRequest {
                decision_policy_snapshot_id: active.decision_policy_snapshot_id,
                snapshot_hash: active.snapshot_hash,
                snapshot: &active.snapshot,
            })
            .await
            .expect("resolve exact Weather route");

        assert_eq!(route.route(), BuyModelRoute::Weather);
        assert_eq!(
            route.champion_model_version_id(),
            versions.weather.model_version_id
        );
        assert_eq!(
            route.shadow().map(|(version, _)| version.model_version_id),
            Some(versions.weather_shadow.model_version_id)
        );
    }

    #[tokio::test]
    async fn failed_prepare_retains_generation() {
        let old = VersionInventory::fixture();
        let new = VersionInventory::fixture();
        let mut entries = old.entries();
        entries.extend(new.entries());
        let resolver = Arc::new(StubPointerResolver::new(entries));
        let old_bundle = bundle(
            PolicyBundleGeneration::FIRST,
            policy(
                &old,
                Some((BuyModelRoute::Pooled, old.shadow.model_version_id)),
            ),
        );
        let store = Arc::new(
            ModelServingGenerationStore::bootstrap_with_resolver(
                old_bundle.clone(),
                Arc::clone(&resolver) as Arc<dyn ModelServingPointerResolver>,
            )
            .await
            .expect("old generation"),
        );
        let policies = Arc::new(DecisionPolicyStore::new_active(old_bundle.clone()));
        let raw = Arc::new(ServingPolicyPort {
            policies,
            generations: Arc::clone(&store),
        });
        let applicator = CommittedPolicyApplicator::new(
            raw as Arc<dyn PolicySnapshotPort>,
            PolicyBundleIdentity::from(&old_bundle),
        );
        let old_route = store
            .resolve_route(ModelServingGenerationRequest {
                decision_policy_snapshot_id: old_bundle.decision_policy_snapshot_id,
                snapshot_hash: old_bundle.snapshot_hash,
                snapshot: &old_bundle.snapshot,
            })
            .await
            .expect("pin old route");
        let new_bundle = bundle(
            PolicyBundleGeneration::FIRST
                .checked_next()
                .expect("next generation"),
            policy(
                &new,
                Some((BuyModelRoute::Pooled, new.shadow.model_version_id)),
            ),
        );
        resolver.fail(new.weather.model_version_id);
        let error = applicator
            .apply_committed(new_bundle.clone())
            .await
            .expect_err("one failed route must reject the complete prepared generation");
        assert!(matches!(
            error,
            ControlError::CommittedGenerationApply {
                stage: RuntimeApplyStage::Prepare,
                ..
            }
        ));
        assert_eq!(
            store.current().active_contract_hash(BuyModelRoute::Pooled),
            Some(old.pooled.serving_contract_hash),
            "failed preparation must retain every old route"
        );
        assert_eq!(
            applicator.readiness(),
            PolicyApplyReadiness::Degraded {
                desired: PolicyBundleIdentity::from(&new_bundle),
                applied: PolicyBundleIdentity::from(&old_bundle),
                cause: PolicyApplyDegradedCause::PrepareFailed,
            }
        );

        resolver.recover(&new.weather.model_version_id);
        assert_eq!(
            applicator
                .apply_committed(new_bundle.clone())
                .await
                .expect("publish complete successor"),
            PolicyApplyReadiness::Ready {
                applied: PolicyBundleIdentity::from(&new_bundle),
            }
        );
        let current = store.current();
        assert_eq!(
            current.active_contract_hash(BuyModelRoute::Pooled),
            Some(new.pooled.serving_contract_hash)
        );
        assert_eq!(
            current.active_contract_hash(BuyModelRoute::Crypto),
            Some(new.crypto.serving_contract_hash)
        );
        assert_eq!(
            current.active_contract_hash(BuyModelRoute::Weather),
            Some(new.weather.serving_contract_hash)
        );
        assert_eq!(
            old_route.champion_model_version_id(),
            old.pooled.model_version_id,
            "a concurrent reader must remain pinned to the old generation"
        );
    }
}
