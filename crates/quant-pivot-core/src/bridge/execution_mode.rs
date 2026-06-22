//! Shared, atomically swappable execution-mode handle.
//!
//! `ExecutionMode` (`DryRun` / `Paper` / `Live`) is a money-critical value. Supporting
//! a governed runtime hot-swap (`POST /system/mode`) requires a single source of
//! truth that every hot-path branch reads, rather than the independent copies a
//! boot-time `Copy` value would scatter across the execution, risk, settlement,
//! heartbeat, and health subsystems.
//!
//! This handle wraps an [`ArcSwap`] so reads are lock-free on the hot path and a
//! commit is one atomic store that every reader observes on its next access. The
//! transition protocol (quiesce → commit → activate) lives in
//! [`crate::control::mode_transition`]; this type only owns the atomic cell.

use arc_swap::ArcSwap;
use oxide_arb_models::enums::common::ExecutionMode;
use std::sync::Arc;

/// Lock-free shared handle to the currently active [`ExecutionMode`].
#[derive(Clone)]
pub struct ExecutionModeHandle(Arc<ArcSwap<ExecutionMode>>);

impl ExecutionModeHandle {
    /// Create a handle seeded with the boot execution mode.
    #[must_use]
    pub fn new(mode: ExecutionMode) -> Self {
        Self(Arc::new(ArcSwap::from_pointee(mode)))
    }

    /// Lock-free read of the currently active execution mode.
    #[must_use]
    pub fn current(&self) -> ExecutionMode {
        **self.0.load()
    }

    /// Atomically commit a new active execution mode.
    ///
    /// All readers observe the new value on their next [`current`](Self::current)
    /// call. This is the single commit point of the mode-transition protocol and
    /// must only be invoked after the trading loop has been quiesced.
    pub fn store(&self, mode: ExecutionMode) {
        self.0.store(Arc::new(mode));
    }
}
