//! Venue account capital subsystem (credential-gated, fail-closed).
//!
//! Report sizing is built on the **real** Polymarket account: CLOB collateral
//! (private-key derived L2 read credential) plus Data API positions (keyless,
//! funder address). `capital_base = min(collateral + Σ position value, budget cap)`.
//! Any read failure, or a missing private key / funder, fails closed — there is
//! no simulated account and no configured-budget fallback.

mod client;
mod mapping;
mod provider;
mod reserved;
mod venue;

pub use client::{PolymarketAccountClient, VenuePolymarketAccountClient};
pub use mapping::map_position;
pub use provider::{AccountProvider, AccountProviderFactory};
pub use reserved::{RepoReservedCapitalReader, ReservedCapitalReader};
pub use venue::VenueAccountProvider;
