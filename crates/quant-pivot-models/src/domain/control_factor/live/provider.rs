//! Hot-path abstraction for reading the active control-factor snapshot.
//!
//! The concrete `ArcSwap`-backed implementation lives in `quant-pivot-core`; this
//! trait lets the algorithm, risk, and core crates consume the snapshot without
//! depending on the live refresher's I/O. Implementations must be lock-free and
//! cheap: a single `ArcSwap::load_full`.

use super::snapshot::ControlFactorSnapshot;
use std::sync::Arc;

/// Lock-free provider of the current published control-factor snapshot.
pub trait ControlFactorProvider: Send + Sync {
    /// Returns the currently published snapshot (never blocks; never does I/O).
    fn snapshot(&self) -> Arc<ControlFactorSnapshot>;
}
