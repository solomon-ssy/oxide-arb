//! Lock-free process-wide [`QuantRuntimeMode`] holder.

use std::sync::Arc;

use arc_swap::{ArcSwap, Guard};
use quant_pivot_models::enums::quant::QuantRuntimeMode;

/// Hot-path readable runtime mode shared across ingest, web, and future report planes.
#[derive(Debug, Clone)]
pub struct RuntimeModeHandle {
    inner: Arc<ArcSwap<QuantRuntimeMode>>,
}

impl RuntimeModeHandle {
    #[must_use]
    pub fn new(initial: QuantRuntimeMode) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    #[must_use]
    #[inline]
    pub fn current(&self) -> QuantRuntimeMode {
        **self.inner.load()
    }

    #[must_use]
    #[inline]
    pub fn load(&self) -> Guard<Arc<QuantRuntimeMode>> {
        self.inner.load()
    }

    pub fn store(&self, mode: QuantRuntimeMode) {
        self.inner.store(Arc::new(mode));
    }

    /// Whether CLOB order submission is permitted in the current mode.
    #[must_use]
    #[inline]
    pub fn blocks_order_submission(&self) -> bool {
        !self.current().allows_order_submission()
    }
}

impl Default for RuntimeModeHandle {
    fn default() -> Self {
        Self::new(QuantRuntimeMode::ReportOnly)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_only_blocks_orders() {
        let handle = RuntimeModeHandle::default();
        assert!(handle.blocks_order_submission());
    }

    #[test]
    fn clone_shares_state() {
        let handle = RuntimeModeHandle::default();
        let cloned = handle.clone();
        handle.store(QuantRuntimeMode::SemiAuto);
        assert_eq!(cloned.current(), QuantRuntimeMode::SemiAuto);
    }
}
