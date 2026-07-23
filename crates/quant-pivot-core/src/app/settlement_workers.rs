//! Bounded production settlement discovery, preflight, and recovery workers.

use std::{sync::Arc, time::Duration};

use chrono::Utc;
use futures_util::{StreamExt, stream};
use quant_pivot_models::enums::settlement::SettlementRoute;
use tokio::time::{MissedTickBehavior, interval};

use super::AppContext;
use crate::{
    app::{task_id::TaskId, task_registry::AppRunner},
    execution::{
        settlement_external::{
            SettlementExternalObservationService, SettlementExternalPassOutcome,
        },
        settlement_governed_action_service::{
            SettlementGovernedActionPassOutcome, SettlementGovernedActionService,
        },
        settlement_preflight::{SettlementPreflightOutcome, SettlementPreflightService},
        settlement_service::{SettlementPassOutcome, SettlementService},
    },
    observability::metrics_hub::MetricsHub,
};

impl AppContext {
    pub fn register_settlement_workers(&self, runner: &mut AppRunner) {
        self.register_settlement_discovery_worker(runner);
        self.register_settlement_preflight_worker(runner);
        self.register_settlement_execution_worker(runner);
        self.register_settlement_governed_action_worker(runner);
        self.register_settlement_external_worker(runner);
    }

    fn register_settlement_discovery_worker(&self, runner: &mut AppRunner) {
        let service = Arc::clone(&self.execution.settlement_discovery);
        let metrics = Arc::clone(&self.infra.metrics);
        let wake = self.execution.settlement_discovery_wake.clone();
        let poll = Duration::from_secs(self.config.polymarket.settlement.discovery_poll_secs);
        let limit = self.config.polymarket.settlement.max_claims_per_tick;
        runner.spawn(TaskId::SettlementDiscovery, move |token| async move {
            let mut ticker = interval(poll);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = token.cancelled() => return,
                    _ = ticker.tick() => {}
                    () = wake.wait() => {}
                }
                match service.run_once(Utc::now(), limit).await {
                    Ok(summary) => {
                        metrics.record_settlement_worker_pass("discovery", "completed");
                        if summary.max_discovery_lag_ms > 0 {
                            metrics
                                .observe_settlement_discovery_lag_ms(summary.max_discovery_lag_ms);
                        }
                    }
                    Err(error) => {
                        metrics.record_settlement_worker_error("discovery");
                        tracing::error!(%error, "settlement discovery pass failed");
                    }
                }
            }
        });
    }

    fn register_settlement_preflight_worker(&self, runner: &mut AppRunner) {
        let service = Arc::clone(&self.execution.settlement_preflight);
        let metrics = Arc::clone(&self.infra.metrics);
        let config = self.config.polymarket.settlement.clone();
        runner.spawn(TaskId::SettlementPreflight, move |token| async move {
            let mut ticker = interval(Duration::from_secs(config.submission_poll_secs));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = token.cancelled() => return,
                    _ = ticker.tick() => {}
                }
                run_preflight_batch(
                    Arc::clone(&service),
                    config.max_claims_per_tick,
                    config.rpc_concurrency,
                    Arc::clone(&metrics),
                )
                .await;
            }
        });
    }

    fn register_settlement_execution_worker(&self, runner: &mut AppRunner) {
        let service = Arc::clone(&self.execution.settlement);
        let metrics = Arc::clone(&self.infra.metrics);
        let config = self.config.polymarket.settlement.clone();
        runner.spawn(TaskId::SettlementExecution, move |token| async move {
            let mut ticker = interval(Duration::from_secs(config.submission_poll_secs));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = token.cancelled() => return,
                    _ = ticker.tick() => {}
                }
                run_execution_batch(
                    Arc::clone(&service),
                    config.max_claims_per_tick,
                    config.rpc_concurrency,
                    Arc::clone(&metrics),
                )
                .await;
            }
        });
    }

    fn register_settlement_external_worker(&self, runner: &mut AppRunner) {
        let service = Arc::clone(&self.execution.settlement_external);
        let metrics = Arc::clone(&self.infra.metrics);
        let poll = Duration::from_secs(self.config.polymarket.settlement.submission_poll_secs);
        runner.spawn(
            TaskId::SettlementExternalObservation,
            move |token| async move {
                let mut ticker = interval(poll);
                ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        () = token.cancelled() => return,
                        _ = ticker.tick() => {}
                    }
                    run_external_pass(Arc::clone(&service), Arc::clone(&metrics)).await;
                }
            },
        );
    }

    fn register_settlement_governed_action_worker(&self, runner: &mut AppRunner) {
        let service = Arc::clone(&self.execution.settlement_governed_actions);
        let metrics = Arc::clone(&self.infra.metrics);
        let config = self.config.polymarket.settlement.clone();
        runner.spawn(TaskId::SettlementGovernedAction, move |token| async move {
            let mut ticker = interval(Duration::from_secs(config.submission_poll_secs));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = token.cancelled() => return,
                    _ = ticker.tick() => {}
                }
                run_governed_action_batch(
                    Arc::clone(&service),
                    config.max_claims_per_tick,
                    config.rpc_concurrency,
                    Arc::clone(&metrics),
                )
                .await;
            }
        });
    }
}

async fn run_external_pass(
    service: Arc<SettlementExternalObservationService>,
    metrics: Arc<MetricsHub>,
) {
    for route in [SettlementRoute::StandardV2, SettlementRoute::NegRiskV2] {
        match service.run_once(route, Utc::now()).await {
            Ok(SettlementExternalPassOutcome::NotFinalized) => {
                metrics.record_settlement_worker_pass("external", "not_finalized");
            }
            Ok(SettlementExternalPassOutcome::Advanced {
                observations,
                through_block,
                ..
            }) => {
                metrics.record_settlement_worker_pass("external", "advanced");
                tracing::debug!(
                    ?route,
                    observations,
                    through_block,
                    "settlement external observation cursor advanced"
                );
            }
            Err(error) => {
                metrics.record_settlement_worker_error("external");
                tracing::error!(?route, %error, "settlement external observation failed");
            }
        }
    }
}

async fn run_preflight_batch(
    service: Arc<SettlementPreflightService>,
    limit: u64,
    concurrency: usize,
    metrics: Arc<MetricsHub>,
) {
    let concurrency = concurrency.max(1);
    let mut remaining = limit;
    while remaining > 0 {
        let width = remaining.min(concurrency as u64);
        let outcomes = stream::iter(0..width)
            .map(|_| {
                let service = Arc::clone(&service);
                async move { Box::pin(service.run_once(Utc::now())).await }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;
        let mut work_observed = false;
        for outcome in outcomes {
            match outcome {
                Ok(outcome) => {
                    metrics.record_settlement_worker_pass(
                        "preflight",
                        preflight_outcome_label(outcome),
                    );
                    if outcome != SettlementPreflightOutcome::Idle {
                        work_observed = true;
                    }
                }
                Err(error) => {
                    metrics.record_settlement_worker_error("preflight");
                    tracing::error!(%error, "settlement preflight pass failed");
                }
            }
        }
        if !work_observed {
            break;
        }
        remaining -= width;
    }
}

async fn run_execution_batch(
    service: Arc<SettlementService>,
    limit: u64,
    concurrency: usize,
    metrics: Arc<MetricsHub>,
) {
    let concurrency = concurrency.max(1);
    let mut remaining = limit;
    while remaining > 0 {
        let width = remaining.min(concurrency as u64);
        let outcomes = stream::iter(0..width)
            .map(|_| {
                let service = Arc::clone(&service);
                async move { Box::pin(service.run_once(Utc::now())).await }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;
        let mut work_observed = false;
        for outcome in outcomes {
            match outcome {
                Ok(outcome) => {
                    metrics.record_settlement_worker_pass(
                        "execution",
                        settlement_outcome_label(&outcome),
                    );
                    if !matches!(outcome, SettlementPassOutcome::Idle) {
                        work_observed = true;
                    }
                }
                Err(error) => {
                    metrics.record_settlement_worker_error("execution");
                    tracing::error!(%error, "settlement execution pass failed");
                }
            }
        }
        if !work_observed {
            break;
        }
        remaining -= width;
    }
}

async fn run_governed_action_batch(
    service: Arc<SettlementGovernedActionService>,
    limit: u64,
    concurrency: usize,
    metrics: Arc<MetricsHub>,
) {
    let concurrency = concurrency.max(1);
    let mut remaining = limit;
    while remaining > 0 {
        let width = remaining.min(concurrency as u64);
        let outcomes = stream::iter(0..width)
            .map(|_| {
                let service = Arc::clone(&service);
                async move { Box::pin(service.run_once(Utc::now())).await }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;
        let mut work_observed = false;
        for outcome in outcomes {
            match outcome {
                Ok(outcome) => {
                    metrics.record_settlement_worker_pass(
                        "governed_action",
                        governed_action_outcome_label(&outcome),
                    );
                    if !matches!(outcome, SettlementGovernedActionPassOutcome::Idle) {
                        work_observed = true;
                    }
                }
                Err(error) => {
                    metrics.record_settlement_worker_error("governed_action");
                    tracing::error!(%error, "settlement governed-action pass failed");
                }
            }
        }
        if !work_observed {
            break;
        }
        remaining -= width;
    }
}

const fn preflight_outcome_label(outcome: SettlementPreflightOutcome) -> &'static str {
    match outcome {
        SettlementPreflightOutcome::Idle => "idle",
        SettlementPreflightOutcome::Ready => "ready",
        SettlementPreflightOutcome::Blocked => "blocked",
    }
}

const fn settlement_outcome_label(outcome: &SettlementPassOutcome) -> &'static str {
    match outcome {
        SettlementPassOutcome::Idle => "idle",
        SettlementPassOutcome::AuthorizationPending { .. } => "authorization_pending",
        SettlementPassOutcome::NewSubmissionBlocked { .. } => "new_submission_blocked",
        SettlementPassOutcome::DispatchAccepted { .. } => "dispatch_accepted",
        SettlementPassOutcome::ExistingSubmissionTracked { .. } => "existing_submission_tracked",
        SettlementPassOutcome::SettlementConfirmed { .. } => "confirmed",
        SettlementPassOutcome::ReconciliationRequired { .. } => "reconciliation_required",
        SettlementPassOutcome::RetryScheduled { .. } => "retry_scheduled",
    }
}

const fn governed_action_outcome_label(
    outcome: &SettlementGovernedActionPassOutcome,
) -> &'static str {
    match outcome {
        SettlementGovernedActionPassOutcome::Idle => "idle",
        SettlementGovernedActionPassOutcome::Deferred { .. } => "deferred",
        SettlementGovernedActionPassOutcome::DispatchAccepted { .. } => "dispatch_accepted",
        SettlementGovernedActionPassOutcome::ExistingSubmissionTracked { .. } => {
            "existing_submission_tracked"
        }
        SettlementGovernedActionPassOutcome::Confirmed { .. } => "confirmed",
        SettlementGovernedActionPassOutcome::ReconciliationRequired { .. } => {
            "reconciliation_required"
        }
        SettlementGovernedActionPassOutcome::RetryScheduled { .. } => "retry_scheduled",
        SettlementGovernedActionPassOutcome::Failed { .. } => "failed",
    }
}
