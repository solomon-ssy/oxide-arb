//! Immutable protocol-level constants.
//!
//! Only chain-level facts belong here: chain IDs, token decimals, and the
//! Polymarket contract deployment addresses on Polygon mainnet. These are
//! facts about the chain — not tunables — so they are compiled in rather
//! than configured. If Polymarket ever redeploys a contract, that is a code
//! change reviewed like any other money-critical change.

/// Polygon chain ID (137).
pub const POLYGON_CHAIN_ID: u64 = 137;

// ── USDC Decimals ───────────────────────────────────────────────────────

/// USDC.e has 6 decimals on Polygon.
pub const USDC_DECIMALS: u8 = 6;

/// Scaling factor for USDC (10^6).
pub const USDC_SCALE: u64 = 1_000_000;

// ── Polymarket contract deployments (Polygon mainnet) ───────────────────
//
// Sources: https://docs.polymarket.com (contract addresses) and the verified
// deployments on polygonscan. Consumed by the CTF redeem client and the CTF
// oracle source. Exchange addresses are intentionally absent: order signing
// targets are owned by `polymarket_client_sdk_v2`, not application code.

/// Gnosis Conditional Tokens Framework (CTF) — `redeemPositions` target for
/// standard (non-neg-risk) markets and the oracle `payoutDenominator` source.
pub const CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";

/// USDC.e (bridged USDC) — collateral token for all Polymarket markets.
pub const USDC_E_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

/// Neg-risk adapter (legacy) — `redeemPositions(conditionId, amounts)` target
/// for neg-risk markets when `settlement.redeem.route = neg_risk_legacy_adapter`.
pub const NEG_RISK_ADAPTER_ADDRESS: &str = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296";

/// CTF collateral adapter — alternative standard-market redeem target when
/// `settlement.redeem.route = ctf_collateral_adapter`.
pub const CTF_COLLATERAL_ADAPTER_ADDRESS: &str = "0xAdA100Db00Ca00073811820692005400218FcE1f";

/// Neg-risk collateral adapter — alternative neg-risk redeem target when
/// `settlement.redeem.route = neg_risk_collateral_adapter`.
pub const NEG_RISK_COLLATERAL_ADAPTER_ADDRESS: &str = "0xadA2005600Dec949baf300f4C6120000bDB6eAab";
