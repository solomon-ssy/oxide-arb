//! Entry-execution worker wiring (Phase 05.4): the `auto_execution` dispatcher
//! poll loop, the execution-breaker self-heal tick, and crash recovery.
//!
//! Correctness note: the per-intent `SELECT … FOR UPDATE` claim inside
//! [`ExecutionSubmissionRepository::claim_for_submission`] is the authoritative
//! double-submit guard and holds **across processes**. The worker is a single
//! spawned task; multi-replica leader election is Phase 8+ (the row lock keeps it
//! safe regardless of replica count).

use std::{future::Future, sync::Arc, time::Duration};

use chrono::Utc;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        ExecutionOrderInfo, ExecutionSubmitPort, NewReconciliation, OrderIntentListQuery,
        PageRequest,
    },
    enums::{
        execution::{ReconciliationEvidenceKind, ReconciliationResult},
        quant::{EntryConditionState, OrderIntentStatus, QuantRuntimeMode},
        system::CapabilityId,
    },
    types::{ReconciliationEvidence, ReconciliationEvidenceChain, ReconciliationId},
};
use quant_pivot_repository::traits::{
    EntryConditionRepository, ExecutionSubmissionRepository, OrderIntentRepository,
    ReconciliationRepository,
};

use super::AppContext;
use crate::{
    app::{capability_gate::wait_for_capability, task_id::TaskId, task_registry::AppRunner},
    governance::{KillSwitchHandle, RuntimeModeHandle},
    infra::periodic_task::PeriodicTask,
};

/// Max armed intents pulled per dispatch pass.
const AUTO_DISPATCH_BATCH: u64 = 64;
/// Max dangling orders handed to reconciliation per boot recovery pass.
const RECOVER_DANGLING_LIMIT: u64 = 1_024;
/// Backoff between boot-recovery retries (recovery is fail-closed: the submit
/// loop does not start until it succeeds).
const RECOVERY_RETRY_BACKOFF: Duration = Duration::from_secs(5);

impl AppContext {
    /// Register the `auto_execution` dispatch worker and the breaker self-heal
    /// tick. Both run regardless of mode and gate internally (mode is hot-swappable).
    pub fn register_execution_dispatcher(&self, runner: &mut AppRunner) {
        self.register_execution_breaker_tick(runner);
        self.register_auto_dispatch_worker(runner);
    }

    fn register_execution_breaker_tick(&self, runner: &mut AppRunner) {
        let breaker = Arc::clone(&self.execution.breaker);
        let tick_secs = self.config.quant.workers.execution_breaker_tick_secs;
        runner.spawn(TaskId::ExecutionBreakerTick, move |token| async move {
            let _ = PeriodicTask::run(
                "execution-breaker-tick",
                move || Duration::from_secs(tick_secs),
                0.0,
                true,
                token,
                move || {
                    let breaker = Arc::clone(&breaker);
                    async move {
                        breaker.tick();
                        Ok(())
                    }
                },
            )
            .await;
        });
    }

    fn register_auto_dispatch_worker(&self, runner: &mut AppRunner) {
        let submission = Arc::clone(&self.execution.submission);
        let reconciliation: Arc<dyn ReconciliationRepository> =
            Arc::clone(&self.infra.repos.reconciliation) as Arc<dyn ReconciliationRepository>;
        let worker = ArmedDispatchWorker {
            dispatcher: self.execution_dispatcher(),
            intents: Arc::clone(&self.infra.repos.order_intent) as Arc<dyn OrderIntentRepository>,
            conditions: Arc::clone(&self.infra.repos.entry_condition)
                as Arc<dyn EntryConditionRepository>,
            reconciliation: Arc::clone(&reconciliation),
            runtime_mode: self.runtime_mode(),
            kill_switch: self.kill_switch_handle(),
        };
        let wake = self.execution_wake();
        let bootstrap = Arc::clone(&self.governance.bootstrap);
        let poll = Duration::from_secs(self.config.quant.workers.execution_dispatch_secs);
        runner.spawn(TaskId::ExecutionDispatcher, move |token| async move {
            // Crash recovery is the dispatcher's first action — it must complete
            // before any new submission. In-flight (`Submitted`/`Ambiguous`)
            // orders are handed to reconciliation so a crash mid-submit is
            // reconciled (05.5) rather than re-submitted or lost. Fail-closed:
            // retry with backoff until it succeeds; the submit loop is not
            // entered (so nothing is auto-submitted) until recovery is durably
            // done. The report plane runs independently and is unaffected.
            loop {
                match recover_dangling_orders(&submission, &reconciliation, RECOVER_DANGLING_LIMIT)
                    .await
                {
                    Ok(recovered) => {
                        if recovered > 0 {
                            tracing::warn!(
                                recovered,
                                "boot recovery enqueued in-flight execution orders",
                            );
                        }
                        break;
                    }
                    Err(error) => {
                        tracing::error!(
                            %error,
                            "boot recovery failed; auto-execution paused, retrying",
                        );
                        tokio::select! {
                            biased;
                            () = token.cancelled() => return,
                            () = tokio::time::sleep(RECOVERY_RETRY_BACKOFF) => {}
                        }
                    }
                }
            }

            loop {
                if !wait_for_capability(
                    Arc::clone(&bootstrap),
                    CapabilityId::OrderSubmissionEligible,
                    &token,
                )
                .await
                {
                    return;
                }

                let mut capabilities = bootstrap.subscribe_capabilities();
                loop {
                    // Low-latency wake on a fresh `ApprovedByPolicy` approval, or the
                    // poll backstop. Capability revisions revoke submission immediately.
                    tokio::select! {
                        biased;
                        () = token.cancelled() => return,
                        changed = capabilities.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            if !capabilities
                                .borrow()
                                .get(CapabilityId::OrderSubmissionEligible)
                                .enabled
                            {
                                break;
                            }
                            continue;
                        }
                        () = wake.wait() => {}
                        () = tokio::time::sleep(poll) => {}
                    }
                    if !bootstrap
                        .capability_snapshot()
                        .get(CapabilityId::OrderSubmissionEligible)
                        .enabled
                    {
                        break;
                    }
                    if let Err(error) = armed_dispatch_pass(&worker).await {
                        tracing::warn!(%error, "auto-execution dispatch pass failed");
                    }
                }
            }
        });
    }
}

struct ArmedDispatchWorker {
    dispatcher: Arc<dyn ExecutionSubmitPort>,
    intents: Arc<dyn OrderIntentRepository>,
    conditions: Arc<dyn EntryConditionRepository>,
    reconciliation: Arc<dyn ReconciliationRepository>,
    runtime_mode: RuntimeModeHandle,
    kill_switch: KillSwitchHandle,
}

/// Boot recovery: scan in-flight (`Submitted`/`Ambiguous`) orders with no
/// reconciliation row and enqueue a `Pending` one, so a crash mid-submit is
/// reconciled (05.5) rather than silently lost. Returns the count enqueued.
///
/// The 05.5 `ReconciliationWorker` consumes these `Pending` rows (and every
/// `find_reconcilable` order), resolves venue truth for `Ambiguous`/resting
/// orders, and either drives them to a terminal verdict (clearing the
/// `Ambiguous` admission block `#17`) or escalates to `Unresolvable` (latching
/// the kill-switch for an operator).
async fn recover_dangling_orders(
    submission: &Arc<dyn ExecutionSubmissionRepository>,
    reconciliation: &Arc<dyn ReconciliationRepository>,
    limit: u64,
) -> QuantResult<u32> {
    let dangling = submission.recover_dangling(limit).await?;
    let mut enqueued = 0;
    for order in &dangling {
        if reconciliation
            .find_by_execution_order(&order.execution_order_id)
            .await?
            .is_none()
        {
            reconciliation
                .create(boot_recovery_reconciliation(order))
                .await?;
            enqueued += 1;
        }
    }
    if !dangling.is_empty() {
        tracing::warn!(
            dangling = dangling.len(),
            enqueued,
            "boot recovery handed in-flight execution orders to reconciliation",
        );
    }
    Ok(enqueued)
}

/// One auto-dispatch pass: worker-level early-exit unless mode is
/// `auto_execution`, the kill-switch admits new entries, and no unresolvable
/// reconciliation is outstanding; then pull a bounded batch of
/// `ApprovedByPolicy` intents and submit each. Each submit is independently
/// row-locked + admitted; a per-intent failure never aborts the batch. The
/// `has_unresolvable` early-exit is a cheap batch-level backstop in addition to
/// admission `#17`, which denies the same condition per intent (05.5).
async fn armed_dispatch_pass(worker: &ArmedDispatchWorker) -> QuantResult<()> {
    dispatch_for_runtime_mode(worker.runtime_mode.current(), |status| {
        armed_dispatch_enabled_pass(worker, status)
    })
    .await
}

async fn dispatch_for_runtime_mode<F, Fut>(mode: QuantRuntimeMode, dispatch: F) -> QuantResult<()>
where
    F: FnOnce(OrderIntentStatus) -> Fut,
    Fut: Future<Output = QuantResult<()>>,
{
    let status = match mode {
        QuantRuntimeMode::ReportOnly => return Ok(()),
        QuantRuntimeMode::SemiAuto => OrderIntentStatus::Approved,
        QuantRuntimeMode::AutoExecution => OrderIntentStatus::ApprovedByPolicy,
    };
    dispatch(status).await
}

async fn armed_dispatch_enabled_pass(
    worker: &ArmedDispatchWorker,
    status: OrderIntentStatus,
) -> QuantResult<()> {
    if !worker.kill_switch.allows_new_entry() {
        return Ok(());
    }
    if worker.reconciliation.has_unresolvable().await? {
        return Ok(());
    }
    let query = OrderIntentListQuery {
        status: Some(status),
        page: PageRequest::new(PageRequest::DEFAULT_PAGE, AUTO_DISPATCH_BATCH),
        ..Default::default()
    };
    let page = worker.intents.page(query).await?;
    for intent in page.items {
        let condition = worker
            .conditions
            .find_instance(&intent.condition_instance_id)
            .await?;
        let Some(condition) = condition else {
            tracing::warn!(
                intent_id = %intent.order_intent_id,
                condition_instance_id = %intent.condition_instance_id,
                "armed intent references a missing condition instance",
            );
            continue;
        };
        if !matches!(
            condition.state,
            EntryConditionState::NotRequired | EntryConditionState::Qualified
        ) {
            continue;
        }
        if let Err(error) = worker
            .dispatcher
            .submit_if_admitted(&intent.order_intent_id)
            .await
        {
            tracing::warn!(
                %error,
                intent_id = %intent.order_intent_id,
                "auto-execution dispatch failed for intent",
            );
        }
    }
    Ok(())
}

/// Pending reconciliation row for an order found in-flight at boot.
fn boot_recovery_reconciliation(order: &ExecutionOrderInfo) -> NewReconciliation {
    let evidence = ReconciliationEvidenceChain(vec![ReconciliationEvidence {
        kind: ReconciliationEvidenceKind::OperatorNote,
        observed_at: Utc::now(),
        detail: format!(
            "boot recovery: execution order {} found in-flight state {}",
            order.execution_order_id,
            order.state.as_str()
        ),
        venue_ref: order.venue_order_id.as_ref().map(ToString::to_string),
        shares: None,
        price: None,
        fee_evidence: None,
    }]);
    NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: order.execution_order_id.clone(),
        order_intent_id: order.order_intent_id.clone(),
        // Truth is not yet known at boot — this is `Pending` (awaiting the 05.5
        // recon worker), never `Unresolvable` (that is the worker's terminal
        // verdict). The fail-closed block on truly truth-unknown exposure keys
        // off the order's `Ambiguous` state, not this row's result.
        result: ReconciliationResult::Pending,
        evidence_json: evidence,
        venue_filled_shares: None,
        venue_avg_price: None,
        expected_cash_delta_usd: None,
        venue_cash_delta_usd: None,
        realized_pnl_usd: None,
        expected_fee_usd: None,
        observed_fee_usd: None,
        fee_delta_usd: None,
        resolved_by: None,
        resolved_at: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use quant_pivot_models::enums::quant::QuantRuntimeMode;

    use super::dispatch_for_runtime_mode;

    #[tokio::test]
    async fn report_only_never_invokes_the_submission_path() {
        let calls = AtomicUsize::new(0);

        dispatch_for_runtime_mode(QuantRuntimeMode::ReportOnly, |_| {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Ok(()) }
        })
        .await
        .expect("ReportOnly dispatch gate");

        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}
