//! Market-catalog readiness gate.
//!
//! The catalog starts `Warming` and flips to `Ready` after the first
//! successful Gamma sync (see `queue_gamma_sync`). Consumers split by need:
//!
//! - **Hot path** (scanner gating): [`CatalogReadiness::is_ready`] — a relaxed
//!   atomic load, no locks.
//! - **Control plane** (readiness report, `GET /system`): the full
//!   [`CatalogState`] snapshot via the [`CatalogStatusPort`] trait.
//! - **Reactive consumers**: [`CatalogReadiness::subscribe`] for a
//!   `tokio::sync::watch` receiver that fires on state changes.

use chrono::{DateTime, Utc};
use oxide_arb_models::domain::{CatalogState, CatalogStatusPort};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::watch;

/// Shared catalog warmup state (fail-closed: starts `Warming`).
pub struct CatalogReadiness {
    /// Lock-free fast flag mirrored from the watch state for hot-path reads.
    ready: AtomicBool,
    state: watch::Sender<CatalogState>,
}

impl Default for CatalogReadiness {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogReadiness {
    /// Create a gate in the `Warming` state.
    #[must_use]
    pub fn new() -> Self {
        let (state, _) = watch::channel(CatalogState::Warming);
        Self {
            ready: AtomicBool::new(false),
            state,
        }
    }

    /// Record a successful catalog sync (idempotent; refreshes the snapshot).
    pub fn mark_ready(&self, markets: u64, synced_at: DateTime<Utc>) {
        self.state
            .send_replace(CatalogState::Ready { markets, synced_at });
        self.ready.store(true, Ordering::Release);
    }

    /// Subscribe to state transitions (`Warming` → `Ready` and refreshes).
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<CatalogState> {
        self.state.subscribe()
    }
}

impl CatalogStatusPort for CatalogReadiness {
    fn catalog_state(&self) -> CatalogState {
        self.state.borrow().clone()
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_warming_and_flips_ready() {
        let gate = CatalogReadiness::new();
        assert!(!gate.is_ready());
        assert_eq!(gate.catalog_state(), CatalogState::Warming);

        let at = Utc::now();
        gate.mark_ready(42, at);
        assert!(gate.is_ready());
        assert_eq!(
            gate.catalog_state(),
            CatalogState::Ready {
                markets: 42,
                synced_at: at
            }
        );
    }

    #[tokio::test]
    async fn subscribers_observe_the_ready_transition() {
        let gate = CatalogReadiness::new();
        let mut rx = gate.subscribe();
        gate.mark_ready(7, Utc::now());
        rx.changed().await.expect("sender alive");
        assert!(rx.borrow().is_ready());
    }
}
