//! Polymarket data access layer for quant-pivot.
//!
//! This crate wraps [`polymarket_client_sdk_v2`] to provide:
//!
//! - **CLOB REST** — order placement, cancellation, book snapshots
//! - **CLOB WebSocket** — sharded real-time orderbook streaming
//! - **Gamma API** — market/event discovery and metadata sync
//! - **Keystore** — EIP-712 signing and L2 HMAC credential management
//!
//! All types propagate errors through [`quant_pivot_error`]'s sub-error system.

use quant_pivot_allocator as _;

pub mod binance;
#[cfg(feature = "domain-chainlink")]
pub mod chainlink;
pub mod clob;
pub mod data_api;
pub mod domain;
pub mod exchange;
pub mod gamma;
pub mod infra;
pub mod keystore;
pub mod rtds;
pub mod settlement;
pub mod wallet;
pub mod weather;
pub mod wire;
pub mod ws;
