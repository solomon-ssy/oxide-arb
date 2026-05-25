//! Polymarket data access layer for oxide-arb.
//!
//! This crate wraps [`polymarket_client_sdk_v2`] to provide:
//!
//! - **CLOB REST** — order placement, cancellation, book snapshots
//! - **CLOB WebSocket** — sharded real-time orderbook streaming
//! - **Gamma API** — market/event discovery and metadata sync
//! - **Fee Calculator** — Polymarket fee formula with rate caching
//! - **Settlement Oracle** — 2-of-3 multi-source resolution verification
//! - **Keystore** — EIP-712 signing and L2 HMAC credential management
//!
//! All types propagate errors through [`oxide_arb_error`]'s sub-error system.

pub mod clob;
pub mod fees;
pub mod gamma;
pub mod infra;
pub mod keystore;
pub mod oracle;
pub mod ws;

pub use oracle::{VotingOracle, build_voting_oracle};
