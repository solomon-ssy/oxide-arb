//! Order-intent service wiring: web `OrderIntentPort`, report-cascade
//! `IntentInvalidationHook`, and the TTL expiry sweep worker.

use std::{sync::Arc, time::Duration};

use chrono::Utc;
use quant_pivot_models::domain::OrderIntentPort;
use quant_pivot_repository::{
    postgres::{
        PgOrderIntentRepository, PgRecommendationReportRepository, PgRecommendationRepository,
        PgRuntimeConfigVersionRepository,
    },
    traits::{
        OrderIntentRepository, RecommendationReportRepository, RecommendationRepository,
        RuntimeConfigVersionRepository,
    },
};

use super::AppContext;
use crate::{
    app::{task_id::TaskId, task_registry::AppRunner},
    execution::{
        CoreOrderIntentService, DefaultRuntimeModeGate, IntentInvalidationHook,
        OrderIntentServiceDeps, RuntimeModeGate,
    },
    infra::periodic_task::PeriodicTask,
};

/// Max intents expired per sweep pass (bounds one transaction burst).
const INTENT_EXPIRE_SWEEP_BATCH: usize = 256;

impl AppContext {
    /// Assemble the governed order-intent service over the execution + governance
    /// planes. The same instance serves the web `OrderIntentPort`, the report
    /// cascade `IntentInvalidationHook`, and the expiry sweep.
    #[must_use]
    pub fn build_order_intent_service(&self) -> Arc<CoreOrderIntentService> {
        let pg = self.infra.pg.connection();
        let mode_gate: Arc<dyn RuntimeModeGate> =
            Arc::new(DefaultRuntimeModeGate::new(self.runtime_config()));
        Arc::new(CoreOrderIntentService::new(OrderIntentServiceDeps {
            mode_gate,
            runtime_mode: self.runtime_mode(),
            kill_switch: self.kill_switch_handle(),
            recommendations: Arc::new(PgRecommendationRepository::new(pg.clone()))
                as Arc<dyn RecommendationRepository>,
            reports: Arc::new(PgRecommendationReportRepository::new(pg.clone()))
                as Arc<dyn RecommendationReportRepository>,
            intents: Arc::new(PgOrderIntentRepository::new(pg.clone()))
                as Arc<dyn OrderIntentRepository>,
            config: self.runtime_config(),
            config_versions: Arc::new(PgRuntimeConfigVersionRepository::new(pg.clone()))
                as Arc<dyn RuntimeConfigVersionRepository>,
            events: self.events.clone(),
        }))
    }

    /// Build the order-intent service, install the report-termination cascade
    /// hook, register the expiry sweep, and return the web port handle.
    #[must_use]
    pub fn register_execution_services(&self, runner: &mut AppRunner) -> Arc<dyn OrderIntentPort> {
        let service = self.build_order_intent_service();
        self.report_lifecycle()
            .set_intent_invalidation_hook(Arc::clone(&service) as Arc<dyn IntentInvalidationHook>);
        self.register_intent_expire_sweep(runner, Arc::clone(&service));
        service as Arc<dyn OrderIntentPort>
    }

    /// Register the intent TTL expiry sweep (`TaskId::IntentExpireSweep`).
    fn register_intent_expire_sweep(
        &self,
        runner: &mut AppRunner,
        service: Arc<CoreOrderIntentService>,
    ) {
        let sweep_secs = self.config.quant.workers.intent_expire_sweep_secs;
        runner.spawn(TaskId::IntentExpireSweep, move |token| async move {
            let _ = PeriodicTask::run(
                "intent-expire-sweep",
                move || Duration::from_secs(sweep_secs),
                0.0,
                true,
                token,
                move || {
                    let service = Arc::clone(&service);
                    async move {
                        service
                            .expire_due(Utc::now(), INTENT_EXPIRE_SWEEP_BATCH)
                            .await?;
                        Ok(())
                    }
                },
            )
            .await;
        });
    }
}
