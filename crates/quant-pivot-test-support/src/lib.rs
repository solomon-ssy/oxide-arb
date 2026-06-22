//! Shared integration-test harnesses, fixtures, and mocks for `quant-pivot`.
//!
//! This crate is `publish = false` — it exists solely to share test-only
//! fixtures across `quant-pivot-core` integration tests and `quant-pivot-bench`
//! benchmarks.

pub mod async_writer;
pub mod mocks;

use uuid::Uuid;

/// Deterministic, reproducible UUID derived from a stable string seed.
///
/// This is a **test-only** helper and is deliberately **not** a v7 id. Domain
/// ids are always UUID v7 (time-ordered, generated via `XxxId::from_v7()`), but
/// v7 is non-deterministic — two calls never collide. Some fixtures instead
/// need *the same* id to appear in independently-built rows (e.g. a config
/// version and an activation that references it, or repeated `sample_manifest()`
/// calls whose hashes must be byte-identical). A name-based UUID (RFC 9562 v5)
/// is the standard way to get "same name → same id", so we use it here. It
/// never reaches production code, so it does not weaken the v7-everywhere policy
/// for real identifiers.
#[must_use]
pub fn seeded_uuid(name: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes())
}
