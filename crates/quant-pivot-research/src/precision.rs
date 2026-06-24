//! Research-plane computational contracts.
//!
//! Values here are **not** operator tunables — they define cross-platform
//! determinism for scoring, normalization, and artifact hashing. Changing them
//! invalidates golden tests and content-addressed artifacts.

/// Fixed decimal scale for all research-plane intermediate arithmetic.
///
/// Append-only contract: changing this invalidates artifact hashes and golden
/// tests.
pub const RESEARCH_DECIMAL_SCALE: u32 = 12;
