//! Empirical Bayes calibration system for endgame resolution accuracy.

pub mod calibrator;
pub mod fallback;
pub mod prior;
pub mod types;
pub mod updater;

pub use calibrator::ResolutionCalibrator;
pub use types::CalibrationEntry;
pub use updater::{CalibrationDataSource, CalibrationUpdater, UnresolvedOutcome, UpdateStats};
