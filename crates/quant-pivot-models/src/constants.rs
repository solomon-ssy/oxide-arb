//! Immutable protocol-level constants.
//!
//! Only chain-level facts belong here: chain IDs, token decimals, and the
//! Polymarket contract deployment addresses on Polygon mainnet. These are
//! facts about the chain — not tunables — so they are compiled in rather
//! than configured. If Polymarket ever redeploys a contract, that is a code
//! change reviewed like any other money-critical change.

/// Polygon chain ID (137).
pub const POLYGON_CHAIN_ID: u64 = 137;

// ── Collateral Decimals ─────────────────────────────────────────────────

/// Polymarket collateral scale is 6 decimals.
pub const COLLATERAL_DECIMALS: u8 = 6;

/// Scaling factor for Polymarket collateral amounts (10^6).
pub const COLLATERAL_SCALE: u64 = 1_000_000;

// ── Polymarket contract deployments (Polygon mainnet) ───────────────────
//
// Sources: https://docs.polymarket.com (contract addresses) and the verified
// deployments on polygonscan. Consumed by the CTF redeem client and the CTF
// oracle source. Exchange addresses are intentionally absent: order signing
// targets are owned by `polymarket_client_sdk_v2`, not application code.

/// Gnosis Conditional Tokens Framework (CTF) — `redeemPositions` target for
/// standard (non-neg-risk) markets and the oracle `payoutDenominator` source.
pub const CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";

/// Polymarket pUSD collateral token used by current CTF mint/redeem flows.
pub const PUSD_ADDRESS: &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";

/// USDC.e (bridged USDC) retained for adapter/accounting references where
/// Polymarket wrapper contracts expose legacy collateral internals.
pub const USDC_E_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

/// Neg-risk adapter (legacy) — `redeemPositions(conditionId, amounts)` target
/// for future neg-risk auto-redeem support.
pub const NEG_RISK_ADAPTER_ADDRESS: &str = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296";

/// CTF collateral adapter retained for future verified adapter routing.
pub const CTF_COLLATERAL_ADAPTER_ADDRESS: &str = "0xAdA100Db00Ca00073811820692005400218FcE1f";

/// Neg-risk collateral adapter retained for future verified adapter routing.
pub const NEG_RISK_COLLATERAL_ADAPTER_ADDRESS: &str = "0xadA2005600Dec949baf300f4C6120000bDB6eAab";
