//! Cached repository wrappers using the tiered cache (L1 Moka + L2 Redis).
//!
//! Each wrapper delegates writes to the inner repository and invalidates
//! relevant cache keys on mutation. Reads attempt cache lookup first,
//! falling through to the inner repository on miss and backfilling the cache.

pub mod event;
pub mod market;

pub use event::CachedEventRepository;
pub use market::CachedMarketRepository;
