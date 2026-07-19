//! Lock-free in-process snapshot of the active runtime configuration.

use arc_swap::{ArcSwap, Guard};
use quant_pivot_models::runtime_config::DecisionPolicySnapshot;
use std::sync::Arc;

/// Process-wide holder of the active [`DecisionPolicySnapshot`].
///
/// Hot-path readers call [`Self::load`] (lock-free, no refcount bump for short
/// borrows); tasks that hold the snapshot across awaits use [`Self::current`].
/// Writes go exclusively through the
/// [`PolicySnapshotApplicator`](super::PolicySnapshotApplicator) after a
/// durable, audited activation.
pub struct DecisionPolicyStore {
    inner: ArcSwap<DecisionPolicySnapshot>,
}

impl DecisionPolicyStore {
    #[must_use]
    pub fn new(initial: DecisionPolicySnapshot) -> Self {
        Self {
            inner: ArcSwap::from_pointee(initial),
        }
    }

    /// Lock-free snapshot borrow for short, synchronous reads.
    #[must_use]
    #[inline]
    pub fn load(&self) -> Guard<Arc<DecisionPolicySnapshot>> {
        self.inner.load()
    }

    /// Owned snapshot for reads held across await points or task boundaries.
    #[must_use]
    #[inline]
    pub fn current(&self) -> Arc<DecisionPolicySnapshot> {
        self.inner.load_full()
    }

    /// Install a new active snapshot (used by [`PolicySnapshotPort`] implementations).
    pub fn replace(&self, config: DecisionPolicySnapshot) {
        self.inner.store(Arc::new(config));
    }

    /// Swap the active snapshot. Crate-private: only the applicator writes.
    pub(crate) fn swap(&self, config: Arc<DecisionPolicySnapshot>) {
        self.inner.store(config);
    }
}
