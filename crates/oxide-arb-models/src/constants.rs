//! Immutable protocol-level constants.
//!
//! Only chain-level facts belong here: chain IDs and token decimals.
//! Polymarket deployment addresses are Live execution parameters and belong
//! in `crate::config::settlement::SettlementContractsSection`.

/// Polygon chain ID (137).
pub const POLYGON_CHAIN_ID: u64 = 137;

// ── USDC Decimals ───────────────────────────────────────────────────────

/// USDC.e has 6 decimals on Polygon.
pub const USDC_DECIMALS: u8 = 6;

/// Scaling factor for USDC (10^6).
pub const USDC_SCALE: u64 = 1_000_000;
