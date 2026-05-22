//! Cached repository wrappers using the tiered cache (L1 Moka + L2 Redis).
//!
//! Each wrapper delegates writes to the inner repository and invalidates
//! relevant cache keys on mutation. Reads attempt cache lookup first,
//! falling through to the inner repository on miss and backfilling the cache.

pub mod calibration;
pub mod event;
pub mod market;
pub mod risk_state;
pub mod runtime_config;

pub use calibration::CachedCalibrationRepository;
pub use event::CachedEventRepository;
pub use market::CachedMarketRepository;
pub use risk_state::CachedRiskStateRepository;
pub use runtime_config::CachedRuntimeConfigRepository;
