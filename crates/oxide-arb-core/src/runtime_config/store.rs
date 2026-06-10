//! Lock-free in-process snapshot of the active runtime configuration.

use arc_swap::{ArcSwap, Guard};
use oxide_arb_models::runtime_config::RuntimeConfig;
use std::sync::Arc;

/// Process-wide holder of the active [`RuntimeConfig`].
///
/// Hot-path readers call [`Self::load`] (lock-free, no refcount bump for short
/// borrows); tasks that hold the snapshot across awaits use [`Self::current`].
/// Writes go exclusively through the
/// [`RuntimeConfigApplicator`](super::RuntimeConfigApplicator) after a
/// durable, audited activation.
pub struct RuntimeConfigStore {
    inner: ArcSwap<RuntimeConfig>,
}

impl RuntimeConfigStore {
    #[must_use]
    pub fn new(initial: RuntimeConfig) -> Self {
        Self {
            inner: ArcSwap::from_pointee(initial),
        }
    }

    /// Lock-free snapshot borrow for short, synchronous reads.
    #[must_use]
    #[inline]
    pub fn load(&self) -> Guard<Arc<RuntimeConfig>> {
        self.inner.load()
    }

    /// Owned snapshot for reads held across await points or task boundaries.
    #[must_use]
    #[inline]
    pub fn current(&self) -> Arc<RuntimeConfig> {
        self.inner.load_full()
    }

    /// Swap the active snapshot. Crate-private: only the applicator writes.
    pub(crate) fn swap(&self, config: Arc<RuntimeConfig>) {
        self.inner.store(config);
    }
}
