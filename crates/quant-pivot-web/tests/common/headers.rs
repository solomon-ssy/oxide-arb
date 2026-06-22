//! Shared HTTP header names for governance integration tests.

/// Correlation id recorded on the operation log.
pub const REQUEST_ID: &str = "x-request-id";

/// Explicit role the caller acts as on governed endpoints.
pub const ACTING_ROLE: &str = "x-acting-role";
