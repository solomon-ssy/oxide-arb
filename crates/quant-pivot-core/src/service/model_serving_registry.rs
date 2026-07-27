//! Bounded, contract-keyed registry for fully verified serving runtimes.
//!
//! A cache miss is admitted only when both the pending-call and cold-load
//! budgets permit it. Concurrent misses for the same validated
//! `ModelServingContract` hash share one initialization. A value becomes
//! visible only after the complete serving preimage, executable runtime, and
//! factor engine have all matched; failures are returned to every waiter and
//! are never cached.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use moka::future::Cache;
use quant_pivot_error::{QuantError, QuantResult, infra::InfraError, research::ResearchError};
use quant_pivot_models::{
    config::ModelServingRegistryConfig,
    domain::quant::ModelVersionInfo,
    enums::model::ModelFamily,
    types::{ContentHash, model_serving::ModelServingContract},
};
use quant_pivot_research::model::QuantModelRuntime;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::service::{
    factor_pipeline::FactorExecutionPlane,
    model_serving_preimage::{ModelServingPreimageService, VerifiedModelServingPreimage},
};

/// One completely loaded Buy-side runtime and its immutable contract plane.
pub(crate) struct LoadedModelServingRuntime {
    contract: ModelServingContract,
    artifact_hash: ContentHash,
    runtime: Arc<dyn QuantModelRuntime>,
    factor_execution: Option<FactorExecutionPlane>,
}

impl LoadedModelServingRuntime {
    pub(crate) fn from_loader(
        version: &ModelVersionInfo,
        runtime: Arc<dyn QuantModelRuntime>,
        factor_execution: Option<FactorExecutionPlane>,
    ) -> Result<Self, ResearchError> {
        let contract = version.verified_serving_contract().map_err(|error| {
            ResearchError::ModelServingLoad {
                contract_hash: version.serving_contract_hash.to_string(),
                stage: "version_projection",
                detail: error.to_string(),
            }
        })?;
        let runtime_entry = Self {
            contract: contract.clone(),
            artifact_hash: version.artifact_hash,
            runtime,
            factor_execution,
        };
        runtime_entry.verify_version(version, contract)?;
        Ok(runtime_entry)
    }

    fn verify_version(
        &self,
        version: &ModelVersionInfo,
        contract: &ModelServingContract,
    ) -> Result<(), ResearchError> {
        let key = contract.contract_hash();
        let bindings = contract.bindings();
        let runtime_plane = self.runtime.factor_serving_plane();
        let plane_matches = match version.model_family {
            ModelFamily::WeightedFactor => runtime_plane == Some(&bindings.factors.plane),
            family if family.is_classical() => {
                runtime_plane.is_none() && bindings.factors.plane.definitions().is_empty()
            }
            _ => false,
        };
        if self.contract != *contract
            || self.contract.contract_hash() != version.serving_contract_hash
            || self.artifact_hash != version.artifact_hash
            || self.runtime.model_version_id() != version.model_version_id
            || self.runtime.model_family() != version.model_family
            || self.runtime.category_scope() != version.category_scope
            || self.runtime.feature_schema_hash() != bindings.schemas.feature_schema_hash
            || !plane_matches
        {
            return Err(ResearchError::ModelServingLoad {
                contract_hash: key.to_string(),
                stage: "loaded_projection",
                detail: format!(
                    "loaded runtime projections differ from model version {}",
                    version.model_version_id
                ),
            });
        }
        Ok(())
    }

    #[must_use]
    pub(crate) fn runtime(&self) -> Arc<dyn QuantModelRuntime> {
        Arc::clone(&self.runtime)
    }

    #[must_use]
    pub(crate) const fn factor_execution(&self) -> Option<&FactorExecutionPlane> {
        self.factor_execution.as_ref()
    }

    #[must_use]
    pub(crate) const fn contract(&self) -> &ModelServingContract {
        &self.contract
    }

    #[must_use]
    pub(crate) const fn contract_hash(&self) -> ContentHash {
        self.contract.contract_hash()
    }
}

/// Cold-load boundary used by the registry's single-flight initializer.
#[async_trait]
pub(crate) trait ModelServingPlaneLoader: Send + Sync {
    async fn load(
        &self,
        version: ModelVersionInfo,
    ) -> Result<Arc<LoadedModelServingRuntime>, ResearchError>;
}

/// Production cold loader: full preimage verification, then runtime/plane build.
struct VerifiedModelServingPlaneLoader {
    preimages: Arc<ModelServingPreimageService>,
}

impl VerifiedModelServingPlaneLoader {
    #[must_use]
    const fn new(preimages: Arc<ModelServingPreimageService>) -> Self {
        Self { preimages }
    }

    fn factor_execution(
        source: &VerifiedModelServingPreimage,
        runtime: &dyn QuantModelRuntime,
    ) -> Result<Option<FactorExecutionPlane>, ResearchError> {
        if runtime.model_family().is_classical() {
            return Ok(None);
        }
        if runtime.model_family() != ModelFamily::WeightedFactor {
            return Err(ResearchError::ModelServingLoad {
                contract_hash: source
                    .artifact()
                    .header()
                    .serving_contract()
                    .contract_hash()
                    .to_string(),
                stage: "runtime_family",
                detail: format!(
                    "Buy-side registry does not serve model family {}",
                    runtime.model_family()
                ),
            });
        }
        let contract = source.artifact().header().serving_contract();
        let profiles = &source.policy_snapshot().snapshot.profile_artifacts;
        let bias_table = source.bias_table();
        let actual_bias = bias_table
            .as_ref()
            .map(|table| (table.table_id, table.content_hash));
        let expected_bias = contract
            .bindings()
            .factors
            .bias_table
            .as_ref()
            .map(|binding| (binding.artifact_id, binding.content_hash));
        if actual_bias != expected_bias {
            return Err(ResearchError::ModelServingLoad {
                contract_hash: contract.contract_hash().to_string(),
                stage: "bias_table_projection",
                detail: "loaded bias table differs from the exact serving binding".to_owned(),
            });
        }
        let execution = FactorExecutionPlane::try_new(
            &profiles.scoring.definition,
            &profiles.features.definition,
            &profiles.domain.definition,
            source.profile().spec.category,
            bias_table,
        )
        .map_err(|error| load_failure(source, "factor_plane_build", &error))?;
        let actual_plane = execution
            .engine()
            .serving_plane()
            .map_err(|error| load_failure(source, "factor_plane_projection", &error))?
            .clone();
        if actual_plane != contract.bindings().factors.plane {
            return Err(ResearchError::ModelServingLoad {
                contract_hash: contract.contract_hash().to_string(),
                stage: "factor_plane_projection",
                detail: "factor engine built from verified profiles differs from the exact serving plane"
                    .to_owned(),
            });
        }
        if runtime.factor_cross_section() != Some(&execution.config().cross_section) {
            return Err(ResearchError::ModelServingLoad {
                contract_hash: contract.contract_hash().to_string(),
                stage: "factor_plane_projection",
                detail: "runtime normalization differs from the verified policy preimage"
                    .to_owned(),
            });
        }
        Ok(Some(execution))
    }
}

#[async_trait]
impl ModelServingPlaneLoader for VerifiedModelServingPlaneLoader {
    async fn load(
        &self,
        version: ModelVersionInfo,
    ) -> Result<Arc<LoadedModelServingRuntime>, ResearchError> {
        let source = self
            .preimages
            .load(&version)
            .await
            .map_err(|error| load_failure_for(&version, "preimage_graph", &error))?;
        let runtime = source
            .buy_runtime()
            .map_err(|error| load_failure_for(&version, "runtime_build", &error))?;
        let factor_execution = Self::factor_execution(&source, runtime.as_ref())?;
        let runtime_entry =
            LoadedModelServingRuntime::from_loader(&version, runtime, factor_execution)?;
        Ok(Arc::new(runtime_entry))
    }
}

fn load_failure(
    source: &VerifiedModelServingPreimage,
    stage: &'static str,
    error: &QuantError,
) -> ResearchError {
    ResearchError::ModelServingLoad {
        contract_hash: source
            .artifact()
            .header()
            .serving_contract()
            .contract_hash()
            .to_string(),
        stage,
        detail: error.to_string(),
    }
}

fn load_failure_for(
    version: &ModelVersionInfo,
    stage: &'static str,
    error: &QuantError,
) -> ResearchError {
    ResearchError::ModelServingLoad {
        contract_hash: version.serving_contract_hash.to_string(),
        stage,
        detail: error.to_string(),
    }
}

/// Process-wide successful-value cache and cold-load admission controller.
pub struct ModelServingRuntimeRegistry {
    cache: Cache<ContentHash, Arc<LoadedModelServingRuntime>>,
    loader: Arc<dyn ModelServingPlaneLoader>,
    pending: Arc<Semaphore>,
    builders: Arc<Semaphore>,
    max_pending_loads: usize,
    load_timeout: Duration,
    accepting: AtomicBool,
    shutdown: CancellationToken,
}

impl ModelServingRuntimeRegistry {
    /// Build the process-wide registry with the canonical deep verifier and
    /// preimage-owned runtime constructor.
    ///
    /// # Errors
    ///
    /// Rejects invalid admission, cache, concurrency, or timeout budgets.
    pub fn new(
        config: ModelServingRegistryConfig,
        preimages: Arc<ModelServingPreimageService>,
    ) -> QuantResult<Self> {
        Self::with_loader(
            config,
            Arc::new(VerifiedModelServingPlaneLoader::new(preimages)),
        )
    }

    fn with_loader(
        config: ModelServingRegistryConfig,
        loader: Arc<dyn ModelServingPlaneLoader>,
    ) -> QuantResult<Self> {
        if config.max_cached_contracts == 0
            || config.max_pending_loads == 0
            || config.max_concurrent_loads == 0
            || config.max_pending_loads < config.max_concurrent_loads
            || config.load_timeout_ms == 0
        {
            return Err(InfraError::Misconfigured {
                detail: "model serving registry budgets must be positive and pending loads must contain concurrent loads"
                    .to_owned(),
            }
            .into());
        }
        Ok(Self {
            cache: Cache::builder()
                .name("model-serving-contracts")
                .max_capacity(config.max_cached_contracts)
                .build(),
            loader,
            pending: Arc::new(Semaphore::new(config.max_pending_loads)),
            builders: Arc::new(Semaphore::new(config.max_concurrent_loads)),
            max_pending_loads: config.max_pending_loads,
            load_timeout: Duration::from_millis(config.load_timeout_ms),
            accepting: AtomicBool::new(true),
            shutdown: CancellationToken::new(),
        })
    }

    /// Load one exact contract, coalescing concurrent misses by validated hash.
    ///
    /// # Errors
    ///
    /// Rejects invalid model-version projections, exhausted admission, load
    /// timeout/failure, or shutdown. Failed initializers are never cached.
    pub(crate) async fn load(
        &self,
        version: &ModelVersionInfo,
    ) -> QuantResult<Arc<LoadedModelServingRuntime>> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(ResearchError::ModelServingShutdown.into());
        }
        let contract = version.verified_serving_contract().map_err(|error| {
            ResearchError::ModelServingLoad {
                contract_hash: version.serving_contract_hash.to_string(),
                stage: "version_projection",
                detail: error.to_string(),
            }
        })?;
        let key = contract.contract_hash();
        if let Some(cached) = self.cache.get(&key).await {
            cached.verify_version(version, contract)?;
            return Ok(cached);
        }

        let _pending = Arc::clone(&self.pending).try_acquire_owned().map_err(|_| {
            ResearchError::ModelServingCapacity {
                limit: self.max_pending_loads,
            }
        })?;
        if !self.accepting.load(Ordering::Acquire) {
            return Err(ResearchError::ModelServingShutdown.into());
        }
        if let Some(cached) = self.cache.get(&key).await {
            cached.verify_version(version, contract)?;
            return Ok(cached);
        }

        let plane_loader = Arc::clone(&self.loader);
        let builders = Arc::clone(&self.builders);
        let shutdown = self.shutdown.clone();
        let load_timeout = self.load_timeout;
        let owned_version = version.clone();
        let expected_contract = contract.clone();
        let init = async move {
            let cold_load = async {
                let _builder = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        return Err(ResearchError::ModelServingShutdown);
                    }
                    permit = builders.acquire_owned() => permit.map_err(|_| {
                        ResearchError::ModelServingShutdown
                    })?,
                };
                let cold_entry = plane_loader.load(owned_version.clone()).await?;
                cold_entry.verify_version(&owned_version, &expected_contract)?;
                Ok(cold_entry)
            };
            tokio::time::timeout(load_timeout, cold_load)
                .await
                .map_err(|_| ResearchError::ModelServingLoad {
                    contract_hash: key.to_string(),
                    stage: "load_timeout",
                    detail: format!("cold load exceeded {} ms", load_timeout.as_millis()),
                })?
        };
        let result = tokio::select! {
            biased;
            () = self.shutdown.cancelled() => {
                Err(ResearchError::ModelServingShutdown)
            }
            cache_result = self.cache.try_get_with(key, init) => {
                cache_result.map_err(|error| error.as_ref().clone())
            }
        }?;
        if !self.accepting.load(Ordering::Acquire) {
            self.cache.invalidate(&key).await;
            return Err(ResearchError::ModelServingShutdown.into());
        }
        result.verify_version(version, contract)?;
        Ok(result)
    }

    /// Stop admission, cancel cold loads, and make every prior entry invisible.
    pub(crate) async fn shutdown(&self) {
        if self.accepting.swap(false, Ordering::AcqRel) {
            self.shutdown.cancel();
        }
        self.cache.invalidate_all();
        self.cache.run_pending_tasks().await;
    }

    /// Exact maintained successful-entry count for readiness/tests.
    #[cfg(test)]
    pub(crate) async fn cached_contracts(&self) -> u64 {
        self.cache.run_pending_tasks().await;
        self.cache.entry_count()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        future,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use futures_util::future::join_all;
    use quant_pivot_error::{QuantResult, research::ResearchError};
    use quant_pivot_models::{
        config::ModelServingRegistryConfig,
        domain::quant::ModelVersionInfo,
        enums::{
            common::MarketCategory,
            model::ModelFamily,
            quant::{ModelWeightSource, PublicationStatus},
        },
        runtime_config::FactorCrossSectionConfig,
        types::{
            ContentHash, ModelVersionId, factor::FactorServingPlane, stable_name::FeatureName,
        },
    };
    use quant_pivot_research::{
        factors::FrozenReferenceQuantiles,
        model::{ModelRuntimeInput, ModelRuntimeMetrics, ModelRuntimeOutput, QuantModelRuntime},
    };
    use tokio::task::yield_now;

    use super::{LoadedModelServingRuntime, ModelServingPlaneLoader, ModelServingRuntimeRegistry};
    use crate::service::model_serving_test_support::{model_artifact, model_version};

    struct StubRuntime {
        version_id: ModelVersionId,
        family: ModelFamily,
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
            self.family
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

    struct StubLoader {
        entries: HashMap<ContentHash, Arc<LoadedModelServingRuntime>>,
        failures: HashSet<ContentHash>,
        calls: Mutex<HashMap<ContentHash, usize>>,
        delay: Duration,
        stall_first: AtomicBool,
        started: AtomicUsize,
    }

    impl StubLoader {
        fn new(
            entries: HashMap<ContentHash, Arc<LoadedModelServingRuntime>>,
            failures: HashSet<ContentHash>,
        ) -> Self {
            Self {
                entries,
                failures,
                calls: Mutex::new(HashMap::new()),
                delay: Duration::from_millis(40),
                stall_first: AtomicBool::new(false),
                started: AtomicUsize::new(0),
            }
        }

        fn stalling(entry: Arc<LoadedModelServingRuntime>) -> Self {
            let key = entry.contract_hash();
            Self {
                entries: HashMap::from([(key, entry)]),
                failures: HashSet::new(),
                calls: Mutex::new(HashMap::new()),
                delay: Duration::ZERO,
                stall_first: AtomicBool::new(true),
                started: AtomicUsize::new(0),
            }
        }

        fn call_count(&self, key: &ContentHash) -> usize {
            *self
                .calls
                .lock()
                .expect("stub loader calls lock")
                .get(key)
                .unwrap_or(&0)
        }

        async fn await_started(&self) {
            while self.started.load(Ordering::SeqCst) == 0 {
                yield_now().await;
            }
        }
    }

    #[async_trait]
    impl ModelServingPlaneLoader for StubLoader {
        async fn load(
            &self,
            version: ModelVersionInfo,
        ) -> Result<Arc<LoadedModelServingRuntime>, ResearchError> {
            let key = version.serving_contract_hash;
            *self
                .calls
                .lock()
                .expect("stub loader calls lock")
                .entry(key)
                .or_default() += 1;
            self.started.fetch_add(1, Ordering::SeqCst);
            if self.stall_first.swap(false, Ordering::SeqCst) {
                future::pending::<()>().await;
            }
            tokio::time::sleep(self.delay).await;
            if self.failures.contains(&key) {
                return Err(ResearchError::ModelServingLoad {
                    contract_hash: key.to_string(),
                    stage: "stub",
                    detail: "injected invalid contract".to_owned(),
                });
            }
            self.entries
                .get(&key)
                .cloned()
                .ok_or_else(|| ResearchError::ModelServingLoad {
                    contract_hash: key.to_string(),
                    stage: "stub",
                    detail: "missing stub entry".to_owned(),
                })
        }
    }

    fn version_and_entry() -> (ModelVersionInfo, Arc<LoadedModelServingRuntime>) {
        let artifact = model_artifact(None);
        let version = model_version(&artifact, PublicationStatus::Published, None);
        let bindings = version.serving_contract.bindings();
        let runtime: Arc<dyn QuantModelRuntime> = Arc::new(StubRuntime {
            version_id: version.model_version_id,
            family: version.model_family,
            category_scope: version.category_scope,
            feature_schema_hash: bindings.schemas.feature_schema_hash,
            factor_plane: bindings.factors.plane.clone(),
        });
        let loaded = LoadedModelServingRuntime::from_loader(&version, runtime, None)
            .expect("valid loaded runtime fixture");
        (version, Arc::new(loaded))
    }

    const fn registry_config(max_cached_contracts: u64) -> ModelServingRegistryConfig {
        ModelServingRegistryConfig {
            max_cached_contracts,
            max_pending_loads: 32,
            max_concurrent_loads: 4,
            load_timeout_ms: 2_000,
        }
    }

    #[tokio::test]
    async fn sequential_reuses_entry() {
        let (version, entry) = version_and_entry();
        let key = version.serving_contract_hash;
        let loader = Arc::new(StubLoader::new(
            HashMap::from([(key, Arc::clone(&entry))]),
            HashSet::new(),
        ));
        let registry = ModelServingRuntimeRegistry::with_loader(
            registry_config(4),
            Arc::clone(&loader) as Arc<dyn ModelServingPlaneLoader>,
        )
        .expect("registry");

        let first = registry.load(&version).await.expect("first load");
        let second = registry.load(&version).await.expect("cached load");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(loader.call_count(&key), 1);
    }

    #[tokio::test]
    async fn concurrent_builds_once() {
        let (version, entry) = version_and_entry();
        let key = version.serving_contract_hash;
        let loader = Arc::new(StubLoader::new(
            HashMap::from([(key, Arc::clone(&entry))]),
            HashSet::new(),
        ));
        let registry = Arc::new(
            ModelServingRuntimeRegistry::with_loader(
                registry_config(4),
                Arc::clone(&loader) as Arc<dyn ModelServingPlaneLoader>,
            )
            .expect("registry"),
        );
        let version = Arc::new(version);
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let registry = Arc::clone(&registry);
            let version = Arc::clone(&version);
            tasks.push(tokio::spawn(async move { registry.load(&version).await }));
        }
        let mut entries = Vec::new();
        for task in tasks {
            entries.push(task.await.expect("join").expect("load"));
        }

        assert!(entries.iter().all(|value| Arc::ptr_eq(value, &entries[0])));
        assert_eq!(loader.call_count(&key), 1);
    }

    #[tokio::test]
    async fn failure_isolated() {
        let (healthy, healthy_entry) = version_and_entry();
        let (bad, _) = version_and_entry();
        let healthy_key = healthy.serving_contract_hash;
        let bad_key = bad.serving_contract_hash;
        let loader = Arc::new(StubLoader::new(
            HashMap::from([(healthy_key, healthy_entry)]),
            HashSet::from([bad_key]),
        ));
        let registry = ModelServingRuntimeRegistry::with_loader(
            registry_config(1),
            Arc::clone(&loader) as Arc<dyn ModelServingPlaneLoader>,
        )
        .expect("registry");

        let first = registry.load(&healthy).await.expect("healthy load");
        let bad_results = join_all((0..8).map(|_| registry.load(&bad))).await;
        assert!(bad_results.iter().all(Result::is_err));
        let second = registry.load(&healthy).await.expect("healthy cache hit");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(loader.call_count(&healthy_key), 1);
        assert_eq!(loader.call_count(&bad_key), 1);
        assert_eq!(registry.cached_contracts().await, 1);
    }

    #[tokio::test]
    async fn cancelled_load_retries() {
        let (version, entry) = version_and_entry();
        let key = version.serving_contract_hash;
        let loader = Arc::new(StubLoader::stalling(entry));
        let registry = Arc::new(
            ModelServingRuntimeRegistry::with_loader(
                registry_config(1),
                Arc::clone(&loader) as Arc<dyn ModelServingPlaneLoader>,
            )
            .expect("registry"),
        );
        let first_registry = Arc::clone(&registry);
        let first_version = version.clone();
        let first = tokio::spawn(async move { first_registry.load(&first_version).await });
        loader.await_started().await;
        first.abort();
        let _ = first.await;

        let runtime_entry = tokio::time::timeout(Duration::from_secs(1), registry.load(&version))
            .await
            .expect("retry timeout")
            .expect("retry load");

        assert_eq!(runtime_entry.contract_hash(), key);
        assert_eq!(loader.call_count(&key), 2);
    }

    #[tokio::test]
    async fn shutdown_drains_cache() {
        let (version, entry) = version_and_entry();
        let loader = Arc::new(StubLoader::stalling(entry));
        let registry = Arc::new(
            ModelServingRuntimeRegistry::with_loader(
                registry_config(1),
                Arc::clone(&loader) as Arc<dyn ModelServingPlaneLoader>,
            )
            .expect("registry"),
        );
        let load_registry = Arc::clone(&registry);
        let load_version = version.clone();
        let load = tokio::spawn(async move { load_registry.load(&load_version).await });
        loader.await_started().await;

        registry.shutdown().await;
        assert!(load.await.expect("join").is_err());
        assert!(registry.load(&version).await.is_err());
        assert_eq!(registry.cached_contracts().await, 0);
    }
}
