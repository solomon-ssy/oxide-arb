//! Governance versioned runtime-config API contract.
//!
//! Runtime configuration is changed only through immutable, audited versions:
//! create a version, then activate (Promote) or roll back to one. There is no
//! bare in-place config mutation. Each request carries a `reason` recorded on
//! the chained audit event; the acting role is supplied via the `X-Acting-Role`
//! header and authorized by the authz middleware.

use serde::Deserialize;
use validator::Validate;

/// Create a new immutable runtime-config version.
///
/// The handler derives the content hash from `config_json` (the single-source
/// `runtime_config_hash`), mints the version id, sets the source to `Operator`,
/// and records `created_by` from the authenticated actor.
#[derive(Debug, Deserialize, Validate)]
pub struct CreateRuntimeConfigVersionRequest {
    /// The full runtime-config document as JSON.
    pub config_json: serde_json::Value,
    /// Schema version of `config_json`; defaults to 1 in the handler when absent.
    pub schema_version: Option<i32>,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Activate an existing runtime-config version (Promote).
#[derive(Debug, Deserialize, Validate)]
pub struct ActivateRuntimeConfigRequest {
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Roll back to an existing runtime-config version (Rollback).
#[derive(Debug, Deserialize, Validate)]
pub struct RollbackRuntimeConfigRequest {
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Version-catalog page size (capped in the handler).
#[derive(Debug, Deserialize)]
pub struct RuntimeConfigVersionListQuery {
    pub limit: Option<u64>,
}
