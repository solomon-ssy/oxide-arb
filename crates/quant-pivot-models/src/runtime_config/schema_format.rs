//! Shared `x-format` schemars fragments for runtime-config field metadata.
//!
//! Every numeric or decimal wire leaf should carry an explicit format so the
//! UI schema walker does not rely on path-suffix heuristics or widget inference.

/// `x-format` value for money/ratio decimal strings (`with = "String"`).
pub const X_FORMAT_DECIMAL: &str = "decimal";
/// `x-format` value for integer counts and second-based durations.
pub const X_FORMAT_INTEGER: &str = "integer";
/// `x-format` value for millisecond timers.
pub const X_FORMAT_DURATION_MS: &str = "duration_ms";
