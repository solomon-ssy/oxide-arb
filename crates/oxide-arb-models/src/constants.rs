//! Immutable protocol-level constants.
//!
//! Only chain-level facts belong here: contract addresses, chain IDs,
//! token decimals. All tunable trading parameters live in the config system
//! (`crate::config`) and are adjustable at runtime via TOML / env vars / API.
//!
//! ── Polymarket Contract Addresses (Polygon) ─────────────────────────────
/// Polymarket CTF Exchange (standard binary markets).
pub const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";

/// Polymarket Neg Risk CTF Exchange (multi-outcome markets).
pub const NEG_RISK_CTF_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

/// USDC.e on Polygon (bridged USDC, 6 decimals).
pub const USDC_E: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

/// Conditional Tokens Framework contract (ERC-1155 position tokens).
pub const CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";

/// Polygon chain ID (137).
pub const POLYGON_CHAIN_ID: u64 = 137;

// ── USDC Decimals ───────────────────────────────────────────────────────

/// USDC.e has 6 decimals on Polygon.
pub const USDC_DECIMALS: u8 = 6;

/// Scaling factor for USDC (10^6).
pub const USDC_SCALE: u64 = 1_000_000;
