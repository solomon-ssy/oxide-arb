//! Order submission, cancellation, and query types.

use chrono::{DateTime, Utc};
use quant_pivot_error::api::ClobFundingDeficit;
use quant_pivot_models::{
    enums::{common::Side, execution::VenueTradeStatus, fee::FeeLiquidityRole},
    types::{
        Bps, EvmAddress, EvmTransactionHash, EvmUint256, MarketId, OrderId, Price, Shares, TokenId,
        Usd, VenueTradeId,
    },
};
use serde::{Deserialize, Serialize};

/// Asset whose live CLOB balance and allowance authorize one order side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VenueFundingAsset {
    Collateral,
    Conditional,
}

/// Human-scale balance decoded from the raw six-decimal venue quantity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "asset", content = "balance", rename_all = "snake_case")]
pub enum VenueFundingBalance {
    Collateral(Usd),
    Conditional(Shares),
}

impl VenueFundingBalance {
    #[must_use]
    pub const fn collateral(self) -> Option<Usd> {
        match self {
            Self::Collateral(balance) => Some(balance),
            Self::Conditional(_) => None,
        }
    }
}

impl VenueFundingAsset {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Collateral => "collateral",
            Self::Conditional => "conditional",
        }
    }
}

/// Exact route-specific projection of one authenticated `/balance-allowance`
/// response. Amounts remain raw six-decimal token units so comparison never
/// crosses floating point or a lossy domain conversion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VenueBalanceAllowanceSnapshot {
    pub asset: VenueFundingAsset,
    pub token_id: Option<TokenId>,
    pub spender: EvmAddress,
    pub balance: EvmUint256,
    pub human_balance: VenueFundingBalance,
    pub allowance: Option<EvmUint256>,
}

/// Funding proof for the canonical maker leg of one order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum VenueFundingEvidence {
    Ready {
        snapshot: VenueBalanceAllowanceSnapshot,
        required: EvmUint256,
    },
    MissingAllowance {
        snapshot: VenueBalanceAllowanceSnapshot,
        required: EvmUint256,
    },
    InsufficientBalance {
        snapshot: VenueBalanceAllowanceSnapshot,
        required: EvmUint256,
    },
    InsufficientAllowance {
        snapshot: VenueBalanceAllowanceSnapshot,
        required: EvmUint256,
    },
}

impl VenueFundingEvidence {
    #[must_use]
    pub const fn snapshot(&self) -> &VenueBalanceAllowanceSnapshot {
        match self {
            Self::Ready { snapshot, .. }
            | Self::MissingAllowance { snapshot, .. }
            | Self::InsufficientBalance { snapshot, .. }
            | Self::InsufficientAllowance { snapshot, .. } => snapshot,
        }
    }

    #[must_use]
    pub const fn required(&self) -> &EvmUint256 {
        match self {
            Self::Ready { required, .. }
            | Self::MissingAllowance { required, .. }
            | Self::InsufficientBalance { required, .. }
            | Self::InsufficientAllowance { required, .. } => required,
        }
    }

    #[must_use]
    pub const fn deficit(&self) -> Option<ClobFundingDeficit> {
        match self {
            Self::Ready { .. } => None,
            Self::MissingAllowance { .. } => Some(ClobFundingDeficit::MissingAllowance),
            Self::InsufficientBalance { .. } => Some(ClobFundingDeficit::InsufficientBalance),
            Self::InsufficientAllowance { .. } => Some(ClobFundingDeficit::InsufficientAllowance),
        }
    }

    #[must_use]
    pub const fn is_sufficient(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }
}

/// Result of cancelling a single order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelResult {
    pub order_id: OrderId,
    pub success: bool,
    pub reason: Option<String>,
}

/// Result of cancelling all orders.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelAllResult {
    pub canceled: Vec<OrderId>,
    pub not_canceled: Vec<(OrderId, String)>,
}

/// An open order resting on the book.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenOrder {
    pub order_id: OrderId,
    pub token_id: TokenId,
    pub side: Side,
    pub price: Price,
    pub size: Shares,
    pub filled: Shares,
}

/// Exact authenticated order snapshot used to discover the order's canonical
/// trade identities without scanning account history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClobOrder {
    pub order_id: OrderId,
    pub is_working: bool,
    pub original_size: Shares,
    pub matched_size: Shares,
    pub associated_trade_ids: Vec<VenueTradeId>,
}

/// Authenticated account trade from CLOB data history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClobTrade {
    pub trade_id: VenueTradeId,
    pub bucket_index: u32,
    pub order_id: OrderId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub size: Shares,
    pub price: Price,
    pub fee_rate_bps: Bps,
    pub trader_side: FeeLiquidityRole,
    pub maker_orders: Vec<ClobMakerOrder>,
    pub status: VenueTradeStatus,
    pub transaction_hash: Option<EvmTransactionHash>,
    pub matched_at: DateTime<Utc>,
}

/// Maker leg retained from an authenticated CLOB trade response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClobMakerOrder {
    pub order_id: OrderId,
    pub side: Side,
    pub size: Shares,
    pub price: Price,
    pub fee_rate_bps: Bps,
    pub token_id: TokenId,
}
