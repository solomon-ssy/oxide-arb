//! Execution recovery playbook projection for operators.

use std::sync::Arc;

use arc_swap::ArcSwap;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        api::{
            ExecutionRecoveryStep, ExecutionRecoverySummary, ExecutionRecoveryView,
            ReconciliationListQuery, ReconciliationView,
        },
        governance::KillSwitchView,
        pagination::{PageRequest, Paginated},
        ports::KillSwitchPort,
    },
    enums::{execution::ReconciliationResult, quant::EntryAuthorizationPolicy},
};
use quant_pivot_repository::traits::ReconciliationRepository;

use super::RuntimeControlsHandle;

/// Max blocking reconciliation rows returned in the detailed recovery view.
const BLOCKING_RECONCILIATION_LIMIT: u64 = 32;

/// Lock-free hot read of the latest execution recovery summary for dashboards.
#[derive(Debug, Clone)]
pub struct ExecutionRecoveryHandle {
    inner: Arc<ArcSwap<ExecutionRecoverySummary>>,
}

impl ExecutionRecoveryHandle {
    #[must_use]
    pub fn new(initial: ExecutionRecoverySummary) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    #[must_use]
    pub fn current(&self) -> ExecutionRecoverySummary {
        self.inner.load().as_ref().clone()
    }

    /// Recompute and publish the recovery summary from live reconciliation + kill-switch state.
    pub async fn refresh(
        &self,
        reconciliation: &Arc<dyn ReconciliationRepository>,
        kill_switch: &Arc<dyn KillSwitchPort>,
        runtime_controls: &RuntimeControlsHandle,
    ) -> QuantResult<()> {
        let summary =
            build_execution_recovery_summary(reconciliation, kill_switch, runtime_controls).await?;
        self.inner.store(Arc::new(summary));
        Ok(())
    }
}

/// Shared recovery refresh surface wired into kill-switch, reconciliation resolve,
/// and the reconciliation worker.
#[derive(Clone)]
pub struct ExecutionRecoveryCoordinator {
    handle: ExecutionRecoveryHandle,
    reconciliation: Arc<dyn ReconciliationRepository>,
    kill_switch: Arc<dyn KillSwitchPort>,
    runtime_controls: RuntimeControlsHandle,
}

impl ExecutionRecoveryCoordinator {
    #[must_use]
    pub fn new(
        handle: ExecutionRecoveryHandle,
        reconciliation: Arc<dyn ReconciliationRepository>,
        kill_switch: Arc<dyn KillSwitchPort>,
        runtime_controls: RuntimeControlsHandle,
    ) -> Self {
        Self {
            handle,
            reconciliation,
            kill_switch,
            runtime_controls,
        }
    }

    #[must_use]
    pub fn handle(&self) -> ExecutionRecoveryHandle {
        self.handle.clone()
    }

    /// Recompute and publish the recovery summary.
    pub async fn refresh(&self) -> QuantResult<()> {
        self.handle
            .refresh(
                &self.reconciliation,
                &self.kill_switch,
                &self.runtime_controls,
            )
            .await
    }
}

/// Build the lightweight recovery summary for [`SystemStatus`](quant_pivot_models::domain::governance::SystemStatus).
pub async fn build_execution_recovery_summary(
    reconciliation: &Arc<dyn ReconciliationRepository>,
    kill_switch: &Arc<dyn KillSwitchPort>,
    runtime_controls: &RuntimeControlsHandle,
) -> QuantResult<ExecutionRecoverySummary> {
    let unresolvable_count = reconciliation.count_blocking_unresolvable().await?;
    let kill_switch_view = kill_switch.view();
    let mode = runtime_controls.entry_authorization_policy();
    Ok(summary_from_parts(
        unresolvable_count,
        &kill_switch_view,
        mode,
    ))
}

/// Build the detailed recovery view for `GET /api/system/execution-recovery`.
pub async fn build_execution_recovery_view(
    reconciliation: &Arc<dyn ReconciliationRepository>,
    kill_switch: &Arc<dyn KillSwitchPort>,
    runtime_controls: &RuntimeControlsHandle,
) -> QuantResult<ExecutionRecoveryView> {
    let kill_switch_view = kill_switch.view();
    let mode = runtime_controls.entry_authorization_policy();
    let unresolvable_count = reconciliation.count_blocking_unresolvable().await?;
    let summary = summary_from_parts(unresolvable_count, &kill_switch_view, mode);

    let blocking_page = if unresolvable_count > 0 {
        reconciliation
            .page(ReconciliationListQuery {
                result: Some(ReconciliationResult::Unresolvable),
                resolved: Some(false),
                page: PageRequest::new(PageRequest::DEFAULT_PAGE, BLOCKING_RECONCILIATION_LIMIT),
                ..Default::default()
            })
            .await?
    } else {
        Paginated::empty(PageRequest::DEFAULT_PAGE, BLOCKING_RECONCILIATION_LIMIT)
    };

    Ok(ExecutionRecoveryView {
        summary,
        blocking_reconciliations: blocking_page
            .items
            .into_iter()
            .map(ReconciliationView::from)
            .collect(),
        kill_switch: kill_switch_view,
    })
}

fn summary_from_parts(
    unresolvable_count: u64,
    kill_switch: &KillSwitchView,
    mode: EntryAuthorizationPolicy,
) -> ExecutionRecoverySummary {
    let has_unresolvable = unresolvable_count > 0;
    let kill_switch_requires_ack = kill_switch.requires_operator_ack;
    let policy_automatic_blocked = mode == EntryAuthorizationPolicy::PolicyAutomatic
        && (!kill_switch.state.allows_new_entry() || has_unresolvable || kill_switch_requires_ack);

    let mut next_steps = Vec::new();
    if has_unresolvable {
        next_steps.push(ExecutionRecoveryStep::ResolveUnresolvableReconciliations);
    }
    if kill_switch_requires_ack {
        next_steps.push(ExecutionRecoveryStep::AcknowledgeKillSwitch);
    }
    if mode == EntryAuthorizationPolicy::PolicyAutomatic
        && next_steps.is_empty()
        && policy_automatic_blocked
    {
        next_steps.push(ExecutionRecoveryStep::VerifyAuthorizationPreflight);
    }

    ExecutionRecoverySummary {
        has_unresolvable_reconciliation: has_unresolvable,
        unresolvable_count,
        kill_switch_requires_ack,
        kill_switch_state: kill_switch.state,
        entry_authorization_policy: mode,
        policy_automatic_blocked,
        next_steps,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use quant_pivot_models::{
        domain::{api::ExecutionRecoveryStep, governance::KillSwitchView},
        enums::{execution::KillSwitchState, quant::EntryAuthorizationPolicy},
    };

    use super::summary_from_parts;

    fn kill_switch(state: KillSwitchState, requires_ack: bool) -> KillSwitchView {
        KillSwitchView {
            state,
            requires_operator_ack: requires_ack,
            revision: 0,
            last_reason: "test".to_owned(),
            changed_by: "test".to_owned(),
            changed_at: Utc::now(),
        }
    }

    #[test]
    fn recovery_steps_order_ack() {
        let summary = summary_from_parts(
            2,
            &kill_switch(KillSwitchState::ExecutionHalted, true),
            EntryAuthorizationPolicy::PolicyAutomatic,
        );
        assert_eq!(
            summary.next_steps,
            vec![
                ExecutionRecoveryStep::ResolveUnresolvableReconciliations,
                ExecutionRecoveryStep::AcknowledgeKillSwitch,
            ]
        );
        assert!(summary.policy_automatic_blocked);
    }

    #[test]
    fn recovery_after_requires_ack() {
        let summary = summary_from_parts(
            0,
            &kill_switch(KillSwitchState::ExecutionHalted, true),
            EntryAuthorizationPolicy::PolicyAutomatic,
        );
        assert_eq!(
            summary.next_steps,
            vec![ExecutionRecoveryStep::AcknowledgeKillSwitch]
        );
        assert!(summary.policy_automatic_blocked);
    }
}
