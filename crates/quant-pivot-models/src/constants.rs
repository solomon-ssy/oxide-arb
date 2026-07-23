//! Immutable protocol-level constants.
//!
//! Only chain-level facts belong here. Settlement deployment addresses are
//! deliberately absent: they require provenance-aware, on-chain verification
//! before they may become a money-moving capability.

/// Polygon chain ID (137).
pub const POLYGON_CHAIN_ID: u64 = 137;

// ── Collateral Decimals ─────────────────────────────────────────────────

/// Polymarket collateral scale is 6 decimals.
pub const COLLATERAL_DECIMALS: u8 = 6;

/// Scaling factor for Polymarket collateral amounts (10^6).
pub const COLLATERAL_SCALE: u64 = 1_000_000;
