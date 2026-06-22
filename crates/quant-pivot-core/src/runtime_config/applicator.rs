//! Runtime-config activation applicator — Phase 0 minimal propagation.

use crate::{
    observability::metrics_hub::MetricsHub,
    pipeline::{
        market_cache::MarketCache, market_registry::MarketRegistry,
        universe_filter::MarketUniverseFilter,
    },
    runtime_config::RuntimeConfigStore,
    service::ws_subscription::WsSubscriptionCoordinator,
};
use async_trait::async_trait;
use quant_pivot_models::{
    domain::{RuntimeConfigPort, RuntimeControlError},
    runtime_config::{RuntimeConfig, validation::validate_runtime_config},
};
use std::sync::Arc;

pub struct RuntimeConfigSubscribers {
    pub universe: Arc<MarketUniverseFilter>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub ws_subscription: Option<Arc<WsSubscriptionCoordinator>>,
    pub metrics: Arc<MetricsHub>,
    /// Deploy-time WS subscription look-ahead (hours); not yet runtime-config v3.
    pub subscription_window_hours: u64,
}

pub struct RuntimeConfigApplicator {
    store: Arc<RuntimeConfigStore>,
    subscribers: RuntimeConfigSubscribers,
}

impl RuntimeConfigApplicator {
    #[must_use]
    pub const fn new(
        store: Arc<RuntimeConfigStore>,
        subscribers: RuntimeConfigSubscribers,
    ) -> Self {
        Self { store, subscribers }
    }

    fn preflight_internal(candidate: &RuntimeConfig) -> Result<(), RuntimeControlError> {
        let report = validate_runtime_config(candidate);
        if report.has_errors() {
            return Err(RuntimeControlError::Precondition(report.to_string()));
        }
        Ok(())
    }

    fn propagate(&self, config: &Arc<RuntimeConfig>) {
        let subs = &self.subscribers;
        subs.universe.reload(&config.market_data.enabled_categories);
        subs.market_cache.rebuild();
        if let Some(ws) = &subs.ws_subscription {
            let _ = ws.sync_engine_hotset(
                subs.market_registry.as_ref(),
                subs.universe.as_ref(),
                subs.subscription_window_hours,
                &subs.metrics,
            );
        }
    }
}

#[async_trait]
impl RuntimeConfigPort for RuntimeConfigApplicator {
    fn current(&self) -> Arc<RuntimeConfig> {
        self.store.current()
    }

    fn preflight(&self, candidate: &RuntimeConfig) -> Result<(), RuntimeControlError> {
        Self::preflight_internal(candidate)
    }

    async fn apply(&self, config: RuntimeConfig) -> Result<(), RuntimeControlError> {
        Self::preflight_internal(&config)?;
        let arc = Arc::new(config);
        self.propagate(&arc);
        self.store.swap(arc);
        Ok(())
    }
}
