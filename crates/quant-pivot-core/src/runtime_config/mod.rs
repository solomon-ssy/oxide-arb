//! In-process runtime-config store and activation applicator.
//!
//! ```text
//! POST /runtime-config/versions/{id}/activate
//!   → registry (audit chain, durable activation)
//!   → PolicySnapshotApplicator::apply
//!       → re-preflight against the live money state (fail-closed)
//!       → stage fallible reloads (oracle sources, settlement-redeem policy) — abort
//!         here leaves no live mutation at all
//!       → commit staged states + infallible subscriber propagation
//!         (risk → exposure → detection → execution → settlement →
//!         notification)
//!       → DecisionPolicyStore swap last (ArcSwap; lock-free hot-path reads)
//! ```

mod applicator;
pub(crate) mod store;

pub use applicator::{PolicySnapshotApplicator, PolicySnapshotSubscribers};
pub use store::DecisionPolicyStore;
