//! Operational kill-switch: lock-free hot-read handle + governed control service.
//!
//! The kill-switch is the execution safety valve. Its authoritative state lives
//! in the `system_kill_switch` Postgres singleton; [`KillSwitchHandle`] mirrors it
//! in an [`ArcSwap`] for zero-DB hot reads on the admission / dispatch / exit
//! paths (mirroring [`RuntimeModeHandle`](crate::governance::RuntimeModeHandle)).
//!
//! It is orthogonal to [`QuantRuntimeMode`](quant_pivot_models::enums::quant::QuantRuntimeMode):
//! tightening the kill-switch never mutates the runtime mode; it only overrides
//! at the execution gates via [`KillSwitchState`]'s behavior table. Any state may
//! transition to any other (it is a safety valve), but clearing `emergency_halted`
//! requires an explicit operator acknowledgement.

use crate::observability::metrics_hub::MetricsHub;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_error::{QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    domain::{KillSwitchPort, KillSwitchView, SetKillSwitchCommand, UpsertKillSwitchState},
    enums::execution::KillSwitchState,
};
use quant_pivot_repository::{postgres::SYSTEM_KILL_SWITCH_ID, traits::KillSwitchStateRepository};
use std::sync::Arc;

use super::system_status::SystemStatusPublisher;

/// Lock-free, process-wide kill-switch state shared with the execution hot path.
#[derive(Debug, Clone)]
pub struct KillSwitchHandle {
    inner: Arc<ArcSwap<KillSwitchState>>,
}

impl KillSwitchHandle {
    #[must_use]
    pub fn new(initial: KillSwitchState) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    #[must_use]
    #[inline]
    pub fn current(&self) -> KillSwitchState {
        **self.inner.load()
    }

    pub fn store(&self, state: KillSwitchState) {
        self.inner.store(Arc::new(state));
    }

    /// Whether new entry orders may be opened (admission consumes this).
    #[must_use]
    #[inline]
    pub fn allows_new_entry(&self) -> bool {
        self.current().allows_new_entry()
    }

    /// Whether normal (TP/SL/time/signal) auto-exits may run (exit monitor).
    #[must_use]
    #[inline]
    pub fn allows_auto_exit(&self) -> bool {
        self.current().allows_auto_exit()
    }

    /// Whether the emergency-exit path must run over open positions (05.6).
    #[must_use]
    #[inline]
    pub fn requires_emergency_exit(&self) -> bool {
        self.current().requires_emergency_exit()
    }
}

impl Default for KillSwitchHandle {
    fn default() -> Self {
        Self::new(KillSwitchState::Closed)
    }
}

/// Governed kill-switch control: persistence + hot-swap + metric.
///
/// Operation-log auditing is performed by the web layer's `operation_audit`
/// middleware around the governed `POST /api/system/kill-switch` handler.
pub struct KillSwitchControl {
    handle: KillSwitchHandle,
    repo: Arc<dyn KillSwitchStateRepository>,
    metrics: Arc<MetricsHub>,
    view: Arc<ArcSwap<KillSwitchView>>,
    status_publisher: Arc<SystemStatusPublisher>,
}

impl KillSwitchControl {
    /// Build the control from the restored singleton snapshot. Publishes the
    /// initial `auto_execution_halted` metric so observability is correct from boot.
    #[must_use]
    pub fn new(
        handle: KillSwitchHandle,
        initial_view: KillSwitchView,
        repo: Arc<dyn KillSwitchStateRepository>,
        metrics: Arc<MetricsHub>,
        status_publisher: Arc<SystemStatusPublisher>,
    ) -> Self {
        metrics.set_auto_execution_halted(!handle.current().allows_new_entry());
        Self {
            handle,
            repo,
            metrics,
            view: Arc::new(ArcSwap::from_pointee(initial_view)),
            status_publisher,
        }
    }

    /// Clone of the hot-read handle for the preflight engine and system status.
    #[must_use]
    pub fn handle(&self) -> KillSwitchHandle {
        self.handle.clone()
    }
}

#[async_trait]
impl KillSwitchPort for KillSwitchControl {
    fn current(&self) -> KillSwitchState {
        self.handle.current()
    }

    fn view(&self) -> KillSwitchView {
        self.view.load_full().as_ref().clone()
    }

    async fn set(&self, command: SetKillSwitchCommand) -> QuantResult<KillSwitchView> {
        let current = self.handle.current();
        // Safety valve: a latched state (emergency, or one escalated with
        // `latch`) can only be loosened with an explicit operator ack. The
        // current state is latched when it is emergency or its persisted view
        // carries `requires_operator_ack`.
        let current_latched = current.is_emergency() || self.view.load().requires_operator_ack;
        let loosening = command.target.restriction_rank() < current.restriction_rank();
        if current_latched && loosening && !command.ack {
            return Err(ExecutionError::KillSwitchBlocks {
                state: current.as_str().to_owned(),
                operation: "loosen_latched_requires_ack".to_owned(),
            }
            .into());
        }

        let info = self
            .repo
            .upsert(UpsertKillSwitchState {
                id: SYSTEM_KILL_SWITCH_ID,
                state: command.target,
                changed_by: command.actor,
                reason: command.reason,
                requires_operator_ack: command.target.is_emergency() || command.latch,
                changed_at: Utc::now(),
            })
            .await?;

        self.handle.store(info.state);
        self.metrics
            .set_auto_execution_halted(!info.state.allows_new_entry());
        let view = KillSwitchView::from(info);
        self.view.store(Arc::new(view.clone()));
        self.status_publisher.publish();
        Ok(view)
    }
}
