//! Lock-free detection gate mirrored from the authoritative operational phase.

use oxide_arb_models::domain::OperationalPhase;
use std::sync::atomic::{AtomicBool, Ordering};

/// Hot-path mirror of `OperationalPhase::allows_detection()` for the scanner.
#[derive(Default)]
pub struct DetectionReadiness {
    allows: AtomicBool,
}

impl DetectionReadiness {
    /// Refresh from the latest lifecycle evaluation (called on status publish).
    pub fn update_from_phase(&self, phase: &OperationalPhase) {
        self.allows
            .store(phase.allows_detection(), Ordering::Release);
    }

    /// Whether endgame detection may emit opportunities on the scan hot path.
    #[inline]
    #[must_use]
    pub fn allows_detection(&self) -> bool {
        self.allows.load(Ordering::Acquire)
    }
}
