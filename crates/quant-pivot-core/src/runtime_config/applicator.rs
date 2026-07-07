//! Runtime-config activation applicator — Phase 0 minimal propagation.

use crate::{
    execution::breaker::ExecutionBreaker,
    governance::{BiasTableApplicator, WeightOverlayApplicator},
    infra::schedule::ReportScheduleRunner,
    ingest::{
        data_quality::BookDataQualityService, market_cache::MarketCache,
        market_filter::MarketFilter, market_registry::MarketRegistry,
    },
    observability::{alert_dispatcher::AlertDispatcher, metrics_hub::MetricsHub},
    runtime_config::RuntimeConfigStore,
    service::ws_subscription::WsSubscriptionCoordinator,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use quant_pivot_error::control::ControlError;
use quant_pivot_models::{
    domain::RuntimeConfigPort,
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
    /// Operator alert dispatcher; its notification channels are hot-swapped on
    /// activation so a new Telegram/webhook config takes effect without restart.
    pub alerts: Arc<AlertDispatcher>,
    /// Candidate / shadow factor-weight overlay snapshot (3.7 hot-update).
    pub weight_overlay: Arc<WeightOverlayApplicator>,
    /// Favorite-longshot bias-table snapshot bound to the factor plane (11.2.1).
    /// Reloaded (and content-hash verified) on activation; a bad ref fails the
    /// activation closed.
    pub bias_table: Arc<BiasTableApplicator>,
    /// Deploy-time WS subscription look-ahead (hours); this is a structural
    /// (restart-bound) parameter from `market_data.websocket`, not runtime config.
    pub subscription_window_hours: u64,
}

pub struct RuntimeConfigApplicator {
    store: Arc<RuntimeConfigStore>,
    subscribers: RuntimeConfigSubscribers,
    /// Report schedule runner, late-bound after the report bundle is assembled
    /// (the runner depends on the report lifecycle, which is built after
    /// governance). `None` until [`Self::attach_report_scheduler`] is called.
    report_scheduler: Mutex<Option<Arc<dyn ReportScheduleRunner>>>,
    /// Execution breaker, late-bound after the execution bundle is assembled
    /// (the breaker is built after governance). `None` until
    /// [`Self::attach_execution_breaker`] is called. Activations hot-swap its
    /// venue-health / daily-loss thresholds without a restart.
    execution_breaker: Mutex<Option<Arc<ExecutionBreaker>>>,
}

impl RuntimeConfigApplicator {
    #[must_use]
    pub fn new(store: Arc<RuntimeConfigStore>, subscribers: RuntimeConfigSubscribers) -> Self {
        Self {
            store,
            subscribers,
            report_scheduler: Mutex::new(None),
            execution_breaker: Mutex::new(None),
        }
    }

    /// Bind the report schedule runner so activations rebuild report jobs.
    pub fn attach_report_scheduler(&self, runner: Arc<dyn ReportScheduleRunner>) {
        *self.report_scheduler.lock() = Some(runner);
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

    fn propagate(&self, config: &Arc<RuntimeConfig>) {
        let subs = &self.subscribers;
        subs.market_filter
            .reload(&config.selection.enabled_categories);
        subs.market_cache.rebuild();
        subs.data_quality.reload(&config.data_quality);
        subs.alerts.reload(&config.notification);
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

    fn preflight(&self, candidate: &RuntimeConfig) -> Result<(), ControlError> {
        Self::preflight_internal(candidate)
    }

    async fn apply(&self, config: RuntimeConfig) -> Result<(), ControlError> {
        Self::preflight_internal(&config)?;
        let arc = Arc::new(config);

        // Rebuild report jobs before mutating the live snapshot so a schedule
        // sync failure leaves store + subscribers untouched and the HTTP
        // activation path can record a compensating rollback.
        let scheduler = self.report_scheduler.lock().clone();
        if let Some(scheduler) = scheduler {
            scheduler
                .sync_from_config(&arc.reports)
                .await
                .map_err(ControlError::from)?;
        }

        // Resolve + hash-verify the favorite-longshot bias table before mutating
        // the live snapshot: a config pinning a missing / corrupt table must fail
        // the activation closed, never bind a stale table to the factor plane.
        self.subscribers
            .bias_table
            .reload(&arc.factors.structural.favorite_longshot)
            .await?;

        self.propagate(&arc);
        let breaker = self.execution_breaker.lock().clone();
        if let Some(breaker) = breaker {
            breaker.reload(&arc.execution.breaker);
        }
        self.store.swap(Arc::clone(&arc));
        Ok(())
    }
}
