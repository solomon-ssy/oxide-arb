//! Order-intent service wiring: web `OrderIntentPort`, report-cascade
//! `IntentInvalidationHook`, and the TTL expiry sweep worker.

use std::{sync::Arc, time::Duration};

use chrono::Utc;
use quant_pivot_models::domain::OrderIntentPort;
use quant_pivot_repository::traits::{
    EntryConditionRepository, FeatureParityRepository, OrderIntentRepository,
    RecommendationReportRepository, RecommendationRepository, TradePolicyRepository,
};

use super::AppContext;
use crate::{
    app::{task_id::TaskId, task_registry::AppRunner},
    execution::{
        CoreOrderIntentService, DefaultRuntimeModeGate, IntentInvalidationHook,
        OrderIntentServiceDeps, RuntimeModeGate,
    },
    infra::{deadline_scheduler, periodic_task::PeriodicTask},
    service::feature_integrity::RepositoryFeatureParityGate,
};

/// Max intents expired per sweep pass (bounds one transaction burst).
const INTENT_EXPIRE_SWEEP_BATCH: usize = 256;

impl AppContext {
    /// Assemble the governed order-intent service over the execution + governance
    /// planes. The same instance serves the web `OrderIntentPort`, the report
    /// cascade `IntentInvalidationHook`, and the expiry sweep.
    #[must_use]
    pub fn build_order_intent_service(&self) -> Arc<CoreOrderIntentService> {
        let repos = &self.infra.repos;
        let mode_gate: Arc<dyn RuntimeModeGate> =
            Arc::new(DefaultRuntimeModeGate::new(self.runtime_config()));
        Arc::new(CoreOrderIntentService::new(OrderIntentServiceDeps {
            mode_gate,
            runtime_mode: self.runtime_mode(),
            kill_switch: self.kill_switch_handle(),
            recommendations: Arc::clone(&repos.recommendation) as Arc<dyn RecommendationRepository>,
            reports: Arc::clone(&repos.recommendation_report)
                as Arc<dyn RecommendationReportRepository>,
            intents: Arc::clone(&repos.order_intent) as Arc<dyn OrderIntentRepository>,
            conditions: Arc::clone(&repos.entry_condition) as Arc<dyn EntryConditionRepository>,
            metrics: Arc::clone(&self.infra.metrics),
            intent_lifecycle: Arc::clone(&self.intent_lifecycle),
            dispatch_wake: self.execution_wake(),
            model_registry: Arc::clone(&self.research.model_registry_repo),
            trade_policies: Arc::clone(&repos.trade_policy) as Arc<dyn TradePolicyRepository>,
            artifact_store: Arc::clone(&self.research.artifact_store),
            calibration_loader: Arc::clone(&self.research.calibration_loader),
            feature_parity_gate: Arc::new(RepositoryFeatureParityGate::new(Arc::clone(
                &repos.feature_parity,
            )
                as Arc<dyn FeatureParityRepository>)),
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
        self.register_intent_deadline_scheduler(runner, Arc::clone(&service));
        service as Arc<dyn OrderIntentPort>
    }

    /// Register the precise per-intent TTL deadline scheduler
    /// (`TaskId::IntentDeadlineScheduler`). It fires capital-releasing expiry
    /// exactly at each intent's `expires_at`; `IntentExpireSweep` is the durable
    /// backstop and the DB is the source of truth (every fire re-checks it).
    fn register_intent_deadline_scheduler(
        &self,
        runner: &mut AppRunner,
        service: Arc<CoreOrderIntentService>,
    ) {
        let intents: Arc<dyn OrderIntentRepository> =
            Arc::clone(&self.infra.repos.order_intent) as Arc<dyn OrderIntentRepository>;
        let reconcile = Duration::from_secs(self.config.quant.workers.intent_expire_sweep_secs);
        runner.spawn(TaskId::IntentDeadlineScheduler, move |token| async move {
            deadline_scheduler::run(
                "intent-deadline-scheduler",
                reconcile,
                token,
                move |horizon| {
                    let intents = Arc::clone(&intents);
                    async move {
                        intents
                            .upcoming_expirations(horizon, INTENT_EXPIRE_SWEEP_BATCH as u64)
                            .await
                            .map_err(Into::into)
                    }
                },
                move || {
                    let service = Arc::clone(&service);
                    async move {
                        service
                            .expire_due(Utc::now(), INTENT_EXPIRE_SWEEP_BATCH)
                            .await
                    }
                },
            )
            .await;
        });
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
