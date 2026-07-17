//! Runtime-config activation applicator — Phase 0 minimal propagation.

use crate::{
    execution::breaker::ExecutionBreaker,
    governance::{BiasTableApplicator, CategoryPointerGuard, WeightOverlayApplicator},
    ingest::{
        data_quality::BookDataQualityService, market_cache::MarketCache,
        market_filter::MarketFilter,
    },
    observability::alert_dispatcher::AlertDispatcher,
    runtime_config::RuntimeConfigStore,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use quant_pivot_error::control::ControlError;
use quant_pivot_models::{
    domain::{PreparedRuntimeConfig, RuntimeConfigPort},
    runtime_config::{RUNTIME_CONFIG_SCHEMA_VERSION, RuntimeConfig},
};
use std::sync::Arc;

#[derive(Clone)]
pub struct RuntimeConfigSubscribers {
    pub market_filter: Arc<MarketFilter>,
    pub market_cache: Arc<MarketCache>,
    pub data_quality: Arc<BookDataQualityService>,
    /// Operator alert dispatcher; its notification channels are hot-swapped on
    /// activation so a new Telegram/webhook config takes effect without restart.
    pub alerts: Arc<AlertDispatcher>,
    /// Candidate / shadow factor-weight overlay snapshot (3.7 hot-update).
    pub weight_overlay: Arc<WeightOverlayApplicator>,
    /// Favorite-longshot bias-table snapshot bound to the factor plane (11.2.1).
    /// Reloaded (and content-hash verified) on activation; a bad ref fails the
    /// activation closed.
    pub bias_table: Arc<BiasTableApplicator>,
    /// `model.category_model_pointers` config-apply-time validator (11.2.2
    /// remediation R7): a dangling or mis-scoped pointer fails the activation
    /// closed rather than surfacing only as a runtime fallback.
    pub category_pointer_guard: Arc<CategoryPointerGuard>,
}

pub struct RuntimeConfigApplicator {
    store: Arc<RuntimeConfigStore>,
    subscribers: RuntimeConfigSubscribers,
    /// Execution breaker, late-bound after the execution bundle is assembled
    /// (the breaker is built after governance). `None` until
    /// [`Self::attach_execution_breaker`] is called. Activations hot-swap its
    /// venue-health / daily-loss thresholds without a restart.
    execution_breaker: Mutex<Option<Arc<ExecutionBreaker>>>,
}

impl RuntimeConfigApplicator {
    #[must_use]
    pub const fn new(
        store: Arc<RuntimeConfigStore>,
        subscribers: RuntimeConfigSubscribers,
    ) -> Self {
        Self {
            store,
            subscribers,
            execution_breaker: Mutex::new(None),
        }
    }

    /// Bind the execution breaker so activations hot-reload its thresholds.
    pub fn attach_execution_breaker(&self, breaker: Arc<ExecutionBreaker>) {
        *self.execution_breaker.lock() = Some(breaker);
    }

    fn preflight_internal(candidate: &RuntimeConfig) -> Result<(), ControlError> {
        if candidate.schema_version != RUNTIME_CONFIG_SCHEMA_VERSION {
            return Err(ControlError::Precondition(
                "unsupported runtime config schema version".to_owned(),
            ));
        }
        Ok(())
    }

    fn propagate(subs: &RuntimeConfigSubscribers, config: &Arc<RuntimeConfig>) {
        subs.market_filter
            .reload(&config.selection.enabled_categories);
        subs.market_cache.rebuild();
        subs.data_quality.reload(&config.data_quality);
        subs.alerts.reload(&config.notification);
        subs.weight_overlay.reload(&config.factors, &config.model);
    }
}

#[async_trait]
impl RuntimeConfigPort for RuntimeConfigApplicator {
    fn current(&self) -> Arc<RuntimeConfig> {
        self.store.current()
    }

    async fn prepare(&self, config: RuntimeConfig) -> Result<PreparedRuntimeConfig, ControlError> {
        Self::preflight_internal(&config)?;
        let arc = Arc::new(config);
        let bias_table = self
            .subscribers
            .bias_table
            .prepare(&arc.factors.structural.favorite_longshot)
            .await?;

        // Validate every category pointer before mutating the live snapshot:
        // a dangling or mis-scoped pointer must fail the activation closed,
        // never rely solely on the router's runtime fallback (11.2.2
        // remediation R7).
        self.subscribers
            .category_pointer_guard
            .validate(&arc.model)
            .await?;

        let breaker = self.execution_breaker.lock().clone();
        let breaker_thresholds = breaker
            .as_ref()
            .map(|_| ExecutionBreaker::prepare_reload(&arc.execution.breaker))
            .transpose()
            .map_err(ControlError::from)?;
        let store = Arc::clone(&self.store);
        let subscribers = self.subscribers.clone();
        let publish_config = Arc::clone(&arc);
        Ok(PreparedRuntimeConfig::new(arc, move || {
            subscribers.bias_table.publish(bias_table);
            if let (Some(breaker), Some(thresholds)) = (breaker, breaker_thresholds) {
                breaker.publish_reload(thresholds);
            }
            Self::propagate(&subscribers, &publish_config);
            store.swap(publish_config);
        }))
    }
}
