//! Runtime-config activation applicator — Phase 0 minimal propagation.

use crate::{
    governance::WeightOverlayApplicator,
    observability::metrics_hub::MetricsHub,
    pipeline::{
        data_quality::BookDataQualityService, market_cache::MarketCache,
        market_filter::MarketFilter, market_registry::MarketRegistry,
    },
    runtime_config::RuntimeConfigStore,
    service::ws_subscription::WsSubscriptionCoordinator,
};
use async_trait::async_trait;
use quant_pivot_models::{
    domain::{RuntimeConfigPort, RuntimeControlError},
    runtime_config::{RUNTIME_CONFIG_SCHEMA_VERSION, RuntimeConfig},
};
use std::sync::Arc;

pub struct RuntimeConfigSubscribers {
    pub market_filter: Arc<MarketFilter>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub ws_subscription: Option<Arc<WsSubscriptionCoordinator>>,
    pub data_quality: Arc<BookDataQualityService>,
    pub metrics: Arc<MetricsHub>,
    /// Candidate / shadow factor-weight overlay snapshot (3.7 hot-update).
    pub weight_overlay: Arc<WeightOverlayApplicator>,
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
        if candidate.schema_version != RUNTIME_CONFIG_SCHEMA_VERSION {
            return Err(RuntimeControlError::Precondition(
                "unsupported runtime config schema version".to_owned(),
            ));
        }
        Ok(())
    }

    fn propagate(&self, config: &Arc<RuntimeConfig>) {
        let subs = &self.subscribers;
        subs.market_filter
            .reload(&config.selection.enabled_categories);
        subs.market_cache.rebuild();
        subs.data_quality.reload(&config.data_quality);
        subs.weight_overlay.reload(&config.factors, &config.model);
        if let Some(ws) = &subs.ws_subscription {
            let _ = ws.sync_subscription(
                subs.market_registry.as_ref(),
                subs.market_filter.as_ref(),
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
