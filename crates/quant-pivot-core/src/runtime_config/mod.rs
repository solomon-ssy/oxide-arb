//! In-process runtime-config store and activation applicator.
//!
//! ```text
//! POST /runtime-config/versions/{id}/activate
//!   → registry (audit chain, durable activation)
//!   → RuntimeConfigApplicator::apply
//!       → re-preflight against the live money state (fail-closed)
//!       → stage fallible reloads (oracle sources, redeem route) — abort
//!         here leaves no live mutation at all
//!       → commit staged states + infallible subscriber propagation
//!         (risk → exposure → detection → execution → settlement →
//!         notification)
//!       → RuntimeConfigStore swap last (ArcSwap; lock-free hot-path reads)
//! ```

mod applicator;
mod store;

pub use applicator::{RuntimeConfigApplicator, RuntimeConfigSubscribers};
pub use store::RuntimeConfigStore;
