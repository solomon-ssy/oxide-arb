//! Runtime-config activation applicator — Phase 0 minimal propagation.

use crate::{
    governance::WeightOverlayApplicator,
    infra::schedule::ReportScheduleRunner,
    observability::{alert_dispatcher::AlertDispatcher, metrics_hub::MetricsHub},
    pipeline::{
        data_quality::BookDataQualityService, market_cache::MarketCache,
        market_filter::MarketFilter, market_registry::MarketRegistry,
    },
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
    /// Deploy-time WS subscription look-ahead (hours); not yet runtime-config v3.
    pub subscription_window_hours: u64,
}

pub struct RuntimeConfigApplicator {
    store: Arc<RuntimeConfigStore>,
    subscribers: RuntimeConfigSubscribers,
    /// Report schedule runner, late-bound after the report bundle is assembled
    /// (the runner depends on the report lifecycle, which is built after
    /// governance). `None` until [`Self::attach_report_scheduler`] is called.
    report_scheduler: Mutex<Option<Arc<dyn ReportScheduleRunner>>>,
}

impl RuntimeConfigApplicator {
    #[must_use]
    pub fn new(store: Arc<RuntimeConfigStore>, subscribers: RuntimeConfigSubscribers) -> Self {
        Self {
            store,
            subscribers,
            report_scheduler: Mutex::new(None),
        }
    }

    /// Bind the report schedule runner so activations rebuild report jobs.
    pub fn attach_report_scheduler(&self, runner: Arc<dyn ReportScheduleRunner>) {
        *self.report_scheduler.lock() = Some(runner);
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
        self.propagate(&arc);
        let scheduler = self.report_scheduler.lock().clone();
        self.store.swap(Arc::clone(&arc));

        // Rebuild report jobs from the just-activated snapshot. Best-effort:
        // runtime-config is the schedule truth source, so a transient rebuild
        // failure is logged and retried on the next activation / restart rather
        // than rolling back an already-validated, already-stored activation.
        if let Some(scheduler) = scheduler {
            if let Err(error) = scheduler.sync_from_config(&arc.reports).await {
                tracing::warn!(
                    %error,
                    "report schedule rebuild after activation failed; \
                     will retry on next activation or restart"
                );
            }
        }
        Ok(())
    }
}
