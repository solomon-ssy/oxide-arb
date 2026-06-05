//! Lock-free storage for the live control-factor snapshots.
//!
//! Holds the compiled Published snapshot (read on the hot path via
//! [`ControlFactorProvider`]) and the Shadow snapshot (read only by the shadow
//! evaluator). Reads are wait-free `ArcSwap` loads; the single-writer refresher
//! swaps validated snapshots atomically.

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use oxide_arb_models::domain::control_factor::{ControlFactorProvider, ControlFactorSnapshot};
use std::sync::Arc;

/// Atomic dual-slot store for Published and Shadow control-factor snapshots.
pub struct FactorSnapshotStore {
    published: ArcSwap<ControlFactorSnapshot>,
    shadow: ArcSwap<ControlFactorSnapshot>,
}

impl FactorSnapshotStore {
    /// Initialize both slots to the neutral (fail-neutral) snapshot.
    #[must_use]
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            published: ArcSwap::from_pointee(ControlFactorSnapshot::neutral(now)),
            shadow: ArcSwap::from_pointee(ControlFactorSnapshot::neutral(now)),
        }
    }

    /// Load the current Published snapshot (wait-free).
    #[inline]
    #[must_use]
    pub fn published(&self) -> Arc<ControlFactorSnapshot> {
        self.published.load_full()
    }

    /// Load the current Shadow snapshot (wait-free).
    #[inline]
    #[must_use]
    pub fn shadow(&self) -> Arc<ControlFactorSnapshot> {
        self.shadow.load_full()
    }

    /// Atomically replace the Published snapshot.
    pub fn store_published(&self, snapshot: Arc<ControlFactorSnapshot>) {
        self.published.store(snapshot);
    }

    /// Atomically replace the Shadow snapshot.
    pub fn store_shadow(&self, snapshot: Arc<ControlFactorSnapshot>) {
        self.shadow.store(snapshot);
    }
}

impl ControlFactorProvider for FactorSnapshotStore {
    #[inline]
    fn snapshot(&self) -> Arc<ControlFactorSnapshot> {
        self.published()
    }
}
