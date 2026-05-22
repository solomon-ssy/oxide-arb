//! Endgame convergence detection subsystem.

pub mod confidence;
pub mod convergence;
pub mod detector;

pub use confidence::{ConfidenceFusion, compute_realtime_confidence};
pub use convergence::{ConvergenceDirection, InMemoryConvergenceTracker};
pub use detector::EndgameDetector;
