//! Account capital subsystem errors.
//!
//! Report sizing is built on the real venue account; these errors are the
//! fail-closed signals when credentials are missing or a venue read fails.

use thiserror::Error;

/// Failures producing a venue account snapshot for report sizing.
#[derive(Debug, Error)]
pub enum AccountError {
    /// The private key (read credential) is not loaded — fail closed.
    #[error("account credentials missing: private key not loaded")]
    CredentialsMissing,

    /// The Polymarket proxy/funder address is not configured — fail closed.
    #[error("account funder address not configured")]
    FunderMissing,

    /// A venue read (collateral / positions / reserved) failed — fail closed.
    #[error("venue account unavailable: {0}")]
    VenueUnavailable(String),
}
