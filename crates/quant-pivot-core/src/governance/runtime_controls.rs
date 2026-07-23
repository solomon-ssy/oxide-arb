//! Lock-free coherent runtime-control snapshot shared by every hot path.

use std::sync::Arc;

use arc_swap::{ArcSwap, Guard};
use chrono::Utc;
use quant_pivot_models::{
    domain::governance::RuntimeControlSnapshot,
    enums::{
        execution::KillSwitchState, quant::QuantRuntimeMode, settlement::SettlementWritePolicy,
    },
};

#[derive(Debug, Clone)]
pub struct RuntimeControlsHandle {
    inner: Arc<ArcSwap<RuntimeControlSnapshot>>,
}

impl RuntimeControlsHandle {
    #[must_use]
    pub fn new(initial: RuntimeControlSnapshot) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    #[must_use]
    #[inline]
    pub fn load(&self) -> Guard<Arc<RuntimeControlSnapshot>> {
        self.inner.load()
    }

    #[must_use]
    pub fn snapshot(&self) -> RuntimeControlSnapshot {
        self.load().as_ref().clone()
    }

    #[must_use]
    #[inline]
    pub fn quant_runtime_mode(&self) -> QuantRuntimeMode {
        self.load().quant_runtime_mode
    }

    #[must_use]
    #[inline]
    pub fn settlement_write_policy(&self) -> SettlementWritePolicy {
        self.load().settlement_write_policy
    }

    #[must_use]
    #[inline]
    pub fn kill_switch_state(&self) -> KillSwitchState {
        self.load().kill_switch_state
    }

    /// Publish only monotonic DB truth. Duplicate notifications are harmless;
    /// stale reads can never roll a process backward.
    pub fn publish_if_newer(&self, snapshot: RuntimeControlSnapshot) -> bool {
        if snapshot.revision <= self.load().revision {
            return false;
        }
        self.inner.store(Arc::new(snapshot));
        true
    }

    /// Publish the result of a successful local CAS, including an exact replay
    /// at the current revision.
    pub fn publish_local(&self, snapshot: RuntimeControlSnapshot) {
        if snapshot.revision >= self.load().revision {
            self.inner.store(Arc::new(snapshot));
        }
    }
}

impl Default for RuntimeControlsHandle {
    fn default() -> Self {
        Self::new(RuntimeControlSnapshot {
            quant_runtime_mode: QuantRuntimeMode::ReportOnly,
            settlement_write_policy: SettlementWritePolicy::Disabled,
            kill_switch_state: KillSwitchState::Closed,
            kill_switch_requires_ack: false,
            revision: 0,
            changed_by: "bootstrap".to_owned(),
            reason: "fresh boot safe defaults".to_owned(),
            changed_at: Utc::now(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_is_atomic_and_revision_monotonic() {
        let handle = RuntimeControlsHandle::default();
        let mut next = handle.snapshot();
        next.revision = 1;
        next.quant_runtime_mode = QuantRuntimeMode::SemiAuto;
        next.settlement_write_policy = SettlementWritePolicy::SemiAuto;
        assert!(handle.publish_if_newer(next.clone()));
        assert_eq!(handle.snapshot(), next);

        let mut stale = next;
        stale.revision = 0;
        stale.quant_runtime_mode = QuantRuntimeMode::ReportOnly;
        assert!(!handle.publish_if_newer(stale));
        assert_eq!(handle.quant_runtime_mode(), QuantRuntimeMode::SemiAuto);
    }
}
