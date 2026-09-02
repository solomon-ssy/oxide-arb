//! Polymarket CLOB REST client with rate limiting and retry.

mod book;
mod convert;
mod orders;
mod rate_limiter;
mod sdk_error;
mod token;

use std::{
    collections::BTreeMap, convert::TryFrom, fmt::Debug, str::FromStr, sync::Arc, time::Duration,
};

use alloy::primitives::Address;
pub use book::{OrderbookSnapshot, VenueOrderMetadata};
use chrono::{DateTime, NaiveDate, Utc};
pub use convert::{ClobSide, SdkSideConversionError};
use num_traits::ToPrimitive;
pub use orders::{
    CancelAllResult, CancelResult, ClobMakerOrder, ClobOrder, ClobTrade, OpenOrder,
    VenueBalanceAllowanceSnapshot, VenueFundingAsset, VenueFundingBalance, VenueFundingEvidence,
};
use polymarket_client_sdk_v2::{
    auth::{Normal, state::Authenticated},
    clob::{
        Client as SdkClient, Config as SdkConfig,
        types::{
            Amount, AssetType, OrderPayload, OrderStatusType, OrderType as SdkOrderType,
            Side as SdkSide, SignableOrder, SignatureType, TickSize as SdkTickSize,
            TradeStatusType, TraderSide,
            request::{
                BalanceAllowanceRequest, OrderBookSummaryRequest, OrdersRequest, TradesRequest,
            },
            response::{
                BalanceAllowanceResponse, ClobMarketInfoResponse, OpenOrderResponse,
                OrderBookSummaryResponse, PostOrderResponse, TradeResponse,
            },
        },
    },
    contract_config,
    types::{B256, U256},
};
use quant_pivot_error::api::{ApiError, ClobOrderError};
use quant_pivot_models::{
    config::PolymarketConfig,
    domain::{
        market::{
            BookLevel,
            book::{OrderbookSide, QuantBookSnapshot},
        },
        order::{CanonicalOrderAmounts, OrderRequest, OrderResponse, PolymarketOrderRules},
    },
    enums::{
        common::{OrderType, Side, TickSize},
        execution::{VenueOrderStatus, VenueTradeStatus},
        fee::FeeLiquidityRole,
    },
    hashing::CanonicalDigest,
    types::{
        Bps, ClobFeeDetails, ClobMarketInfoVersion, ClobMarketInfoVersionId, ClobTokenDescriptor,
        EvmAddress, EvmTransactionHash, EvmUint256, MarketId, OrderId, Price, Shares, TokenId, Usd,
        VenueOrderAmount, VenueTradeId,
    },
};
pub use rate_limiter::RateLimiter;
use reqwest::Client;
use rust_decimal::Decimal;
pub use sdk_error::SdkClobError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
pub use token::WireTokenId;
use tokio::sync::Mutex;

use crate::{
    infra::{
        http::get_text_with_retry,
        retry::{self, RetryPolicy},
    },
    keystore::OrderSigner,
    wallet::WalletTopology,
    wire::decimal::de_decimal,
    ws::BookLevelRejectHook,
};

/// Stage reached by a single-attempt money-changing order submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSubmissionStage {
    Prepare,
    Sign,
    Post,
}

/// Stage-aware order error. Only `Post` failures can be execution-ambiguous.
#[derive(Debug, thiserror::Error)]
#[error("order submission failed during {stage:?}: {source}")]
pub struct OrderSubmissionError {
    pub stage: OrderSubmissionStage,
    #[source]
    pub source: ApiError,
}

impl OrderSubmissionError {
    const fn prepare(source: ApiError) -> Self {
        Self {
            stage: OrderSubmissionStage::Prepare,
            source,
        }
    }

    const fn sign(source: ApiError) -> Self {
        Self {
            stage: OrderSubmissionStage::Sign,
            source,
        }
    }

    const fn post(source: ApiError) -> Self {
        Self {
            stage: OrderSubmissionStage::Post,
            source,
        }
    }
}

/// Polymarket CLOB REST client backed by the official SDK.
///
/// Created via [`ClobClient::connect`] which performs async SDK authentication.
/// All methods enforce per-endpoint rate limiting. Read endpoints and idempotent
/// cancellation may retry through [`retry_with_policy`](retry::retry_with_policy);
/// money-changing `POST /order` is always a single bounded attempt.
pub struct ClobClient {
    sdk: Arc<SdkClient<Authenticated<Normal>>>,
    http: Client,
    clob_base_url: String,
    maker_address: EvmAddress,
    chain_id: u64,
    signature_type: SignatureType,
    signer: Arc<OrderSigner>,
    sdk_rule_lock: Mutex<()>,
    order_post_timeout: Duration,
    rate_limiter: RateLimiter,
    on_book_level_rejected: Option<BookLevelRejectHook>,
}

#[derive(Debug, Deserialize)]
struct RawClobMarketInfo {
    #[serde(rename = "t")]
    tokens: Vec<RawClobToken>,
    #[serde(rename = "mts")]
    minimum_tick_size: Decimal,
    #[serde(rename = "mos")]
    minimum_order_size: Decimal,
    #[serde(rename = "nr")]
    neg_risk: bool,
    #[serde(rename = "itode")]
    #[serde(default)]
    taker_order_delay_enabled: bool,
    #[serde(rename = "ibce")]
    blockaid_check_enabled: bool,
    #[serde(rename = "oas")]
    minimum_order_age_secs: u64,
    #[serde(rename = "fd")]
    fee_details: RawClobFeeDetails,
    #[serde(rename = "mbf")]
    builder_maker_fee_rate_bps: u32,
    #[serde(rename = "tbf")]
    builder_taker_fee_rate_bps: u32,
}

#[derive(Debug, Deserialize)]
struct RawClobToken {
    #[serde(rename = "t")]
    token_id: String,
    #[serde(rename = "o")]
    outcome: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
struct RawClobFeeDetails {
    #[serde(rename = "r")]
    rate: Decimal,
    #[serde(rename = "e")]
    exponent: u32,
    #[serde(rename = "to")]
    taker_only: bool,
}

#[derive(Debug, Deserialize)]
struct RawMakerRebateReportedAccrual {
    date: NaiveDate,
    condition_id: String,
    asset_address: String,
    maker_address: String,
    #[serde(deserialize_with = "de_decimal")]
    rebated_fees_usdc: Decimal,
}

/// Venue-awarded maker rebate at the canonical market/day dimension.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MakerRebateReportedAccrual {
    pub program_date: NaiveDate,
    pub market_id: MarketId,
    pub asset_address: EvmAddress,
    pub maker_address: EvmAddress,
    pub amount_usd: Usd,
}

fn push_rest_level(
    levels: &mut Vec<BookLevel>,
    price: Decimal,
    size: Decimal,
    on_rejected: Option<&BookLevelRejectHook>,
) {
    match BookLevel::try_from_decimal(Price::new(price), Shares::new(size)) {
        Some(level) if level.size.is_positive() => levels.push(level),
        _ => {
            tracing::warn!(?price, ?size, "rejecting invalid REST book level");
            if let Some(hook) = on_rejected {
                hook();
            }
        }
    }
}

fn sdk_amount(amount: VenueOrderAmount) -> Result<Amount, ApiError> {
    match amount {
        VenueOrderAmount::PrincipalUsd(value) => Amount::usdc(value.inner()),
        VenueOrderAmount::Shares(value) => Amount::shares(value.inner()),
    }
    .map_err(|error| ApiError::Sdk(error.to_string()))
}

fn limit_order_shares(amount: VenueOrderAmount) -> Result<Decimal, ApiError> {
    amount
        .shares()
        .map(Shares::inner)
        .ok_or_else(|| ApiError::Clob {
            endpoint: "order.build".to_owned(),
            code: "invalid_order_amount_unit".to_owned(),
            message: "limit orders require share-denominated amount".to_owned(),
            retryable: false,
        })
}

struct ParsedBalanceAllowance {
    balance: U256,
    allowances: BTreeMap<Address, U256>,
}

struct ParsedFundingSnapshot {
    snapshot: VenueBalanceAllowanceSnapshot,
    balance: U256,
    allowance: Option<U256>,
}

struct PreparedOrderEvidence {
    metadata: VenueOrderMetadata,
    canonical: CanonicalOrderAmounts,
    funding: VenueFundingEvidence,
}

fn strict_u256(field: &'static str, value: &str) -> Result<U256, ApiError> {
    let canonical = EvmUint256::parse(value).map_err(|error| ClobOrderError::MalformedUint256 {
        field,
        value: value.to_owned(),
        detail: error.to_string(),
    })?;
    U256::from_str(canonical.as_str()).map_err(|error| {
        ClobOrderError::MalformedUint256 {
            field,
            value: value.to_owned(),
            detail: error.to_string(),
        }
        .into()
    })
}

fn project_uint256(field: &'static str, value: U256) -> Result<EvmUint256, ApiError> {
    let value = value.to_string();
    EvmUint256::parse(value.as_str()).map_err(|error| {
        ClobOrderError::MalformedUint256 {
            field,
            value,
            detail: error.to_string(),
        }
        .into()
    })
}

fn parse_balance_allowance(
    response: BalanceAllowanceResponse,
) -> Result<ParsedBalanceAllowance, ApiError> {
    let balance_text = response.balance.normalize().to_string();
    let balance = strict_u256("balance", &balance_text)?;
    let allowances = response
        .allowances
        .into_iter()
        .map(|(spender, value)| {
            strict_u256("allowance", &value).map(|allowance| (spender, allowance))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    Ok(ParsedBalanceAllowance {
        balance,
        allowances,
    })
}

fn wire_units(field: &'static str, value: Decimal) -> Result<U256, ApiError> {
    let scale = Decimal::from(1_000_000_u64);
    let scaled = value
        .checked_mul(scale)
        .ok_or_else(|| ClobOrderError::RuleViolation {
            detail: format!("{field} overflows six-decimal wire scaling"),
        })?;
    if !scaled.fract().is_zero() {
        return Err(ClobOrderError::RuleViolation {
            detail: format!("{field} `{value}` is finer than six-decimal wire precision"),
        }
        .into());
    }
    strict_u256(field, &scaled.normalize().to_string())
}

fn human_units(field: &'static str, value: U256) -> Result<Decimal, ApiError> {
    let raw = value.to_string();
    let decimal = Decimal::from_str(&raw)
        .map_err(|_| ClobOrderError::HumanScaleOverflow { field, value: raw })?;
    decimal
        .checked_div(Decimal::from(1_000_000_u64))
        .ok_or_else(|| {
            ClobOrderError::RuleViolation {
                detail: format!("{field} cannot be converted from six-decimal wire units"),
            }
            .into()
        })
}

fn evm_address(address: Address) -> Result<EvmAddress, ApiError> {
    let value = format!("{address:#x}");
    EvmAddress::parse(value.as_str()).map_err(|error| {
        ClobOrderError::IdentityMismatch {
            field: "spender",
            expected: "canonical lower-case EVM address".to_owned(),
            actual: format!("{value}: {error}"),
        }
        .into()
    })
}

fn metadata_from_book(
    token_id: &TokenId,
    response: &OrderBookSummaryResponse,
) -> Result<VenueOrderMetadata, ApiError> {
    let observed_token_id = TokenId::new(response.asset_id.to_string());
    if &observed_token_id != token_id {
        return Err(ClobOrderError::IdentityMismatch {
            field: "book.asset_id",
            expected: token_id.to_string(),
            actual: observed_token_id.to_string(),
        }
        .into());
    }
    let tick_size = TickSize::try_from(response.tick_size.as_decimal()).map_err(|error| {
        ClobOrderError::RuleViolation {
            detail: error.to_string(),
        }
    })?;
    let minimum_order_size = Shares::new(response.min_order_size);
    PolymarketOrderRules::new(tick_size, minimum_order_size).map_err(|error| {
        ClobOrderError::RuleViolation {
            detail: error.to_string(),
        }
    })?;
    Ok(VenueOrderMetadata {
        market_id: MarketId::new(format!("{:#x}", response.market)),
        token_id: observed_token_id,
        tick_size,
        minimum_order_size,
        neg_risk: response.neg_risk,
    })
}

fn verify_order_identity(
    request: &OrderRequest,
    metadata: &VenueOrderMetadata,
) -> Result<(), ApiError> {
    for (field, expected, actual) in [
        (
            "book.market",
            request.market_id.as_str(),
            metadata.market_id.as_str(),
        ),
        (
            "book.asset_id",
            request.token_id.as_str(),
            metadata.token_id.as_str(),
        ),
    ] {
        if expected != actual {
            return Err(ClobOrderError::IdentityMismatch {
                field,
                expected: expected.to_owned(),
                actual: actual.to_owned(),
            }
            .into());
        }
    }
    for result in [
        require_payload_match(
            "book.tick_size",
            &request.expected_tick_size,
            &metadata.tick_size,
        ),
        require_payload_match(
            "book.minimum_order_size",
            &request.expected_minimum_order_size,
            &metadata.minimum_order_size,
        ),
        require_payload_match(
            "book.neg_risk",
            &request.expected_neg_risk,
            &metadata.neg_risk,
        ),
    ] {
        result?;
    }
    Ok(())
}

fn verify_market_info_identity(
    request: &OrderRequest,
    metadata: &VenueOrderMetadata,
    market_info: &ClobMarketInfoVersion,
) -> Result<(), ApiError> {
    let token_matches = market_info
        .tokens
        .iter()
        .any(|token| token.token_id == request.token_id);
    for result in [
        require_payload_match(
            "market_info.market_id",
            &request.market_id,
            &market_info.market_id,
        ),
        require_payload_match("market_info.token_id", &true, &token_matches),
        require_payload_match(
            "market_info.tick_size",
            &request.expected_tick_size,
            &market_info.tick_size,
        ),
        require_payload_match(
            "market_info.minimum_order_size",
            &request.expected_minimum_order_size,
            &market_info.minimum_order_size,
        ),
        require_payload_match(
            "market_info.neg_risk",
            &request.expected_neg_risk,
            &market_info.neg_risk,
        ),
        require_payload_match(
            "market_info.payload_hash",
            &request.expected_clob_market_info_payload_hash,
            &market_info.payload_hash,
        ),
        require_payload_match(
            "book.market_info.market_id",
            &metadata.market_id,
            &market_info.market_id,
        ),
        require_payload_match(
            "book.market_info.tick_size",
            &metadata.tick_size,
            &market_info.tick_size,
        ),
        require_payload_match(
            "book.market_info.minimum_order_size",
            &metadata.minimum_order_size,
            &market_info.minimum_order_size,
        ),
        require_payload_match(
            "book.market_info.neg_risk",
            &metadata.neg_risk,
            &market_info.neg_risk,
        ),
    ] {
        result?;
    }
    Ok(())
}

fn require_payload_match<T>(field: &'static str, expected: &T, actual: &T) -> Result<(), ApiError>
where
    T: Debug + PartialEq,
{
    if expected == actual {
        return Ok(());
    }
    Err(ClobOrderError::PayloadMismatch {
        field,
        expected: format!("{expected:?}"),
        actual: format!("{actual:?}"),
    }
    .into())
}

fn map_post_order_response(
    req: &OrderRequest,
    order_type: OrderType,
    order_amount: VenueOrderAmount,
    submitted_at: DateTime<Utc>,
    resp: &PostOrderResponse,
) -> Result<OrderResponse, ApiError> {
    let (filled_shares, cash_amount) = match req.side {
        Side::Buy => (Shares::new(resp.taking_amount), resp.making_amount),
        Side::Sell => (Shares::new(resp.making_amount), resp.taking_amount),
    };
    let status = if !resp.success || filled_shares.inner() <= Decimal::ZERO {
        VenueOrderStatus::Rejected
    } else if order_type == OrderType::Fok {
        if order_amount
            .shares()
            .is_some_and(|shares| filled_shares >= shares)
            || order_amount
                .principal_usd()
                .is_some_and(|usd| cash_amount >= usd.inner())
        {
            VenueOrderStatus::Filled
        } else {
            // FOK is all-or-nothing; success with under-fill is a contract violation.
            // Tag the raw response fill ratio only — `ClobOrderClient` reinterprets
            // this as [`VenueOutcome::Ambiguous`] (Hold + recon), not GTC partial.
            VenueOrderStatus::PartiallyFilled
        }
    } else if order_amount
        .shares()
        .is_some_and(|shares| filled_shares >= shares)
        || order_amount
            .principal_usd()
            .is_some_and(|usd| cash_amount >= usd.inner())
    {
        VenueOrderStatus::Filled
    } else {
        VenueOrderStatus::PartiallyFilled
    };

    let avg_fill_price = if filled_shares.inner() > Decimal::ZERO {
        Some(Price::new(cash_amount / filled_shares.inner()))
    } else {
        None
    };

    let transaction_hashes = resp
        .transaction_hashes
        .iter()
        .map(|hash| EvmTransactionHash::parse(format!("{hash:#x}")))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| ApiError::Deserialize {
            context: "CLOB post-order transaction identity".to_owned(),
            detail: error.to_string(),
        })?;

    Ok(OrderResponse {
        order_id: OrderId::new(&resp.order_id),
        status,
        trade_ids: resp.trade_ids.iter().map(VenueTradeId::new).collect(),
        transaction_hashes,
        filled_shares,
        avg_fill_price,
        submitted_at,
        responded_at: Utc::now(),
    })
}

fn map_order_lookup(response: OpenOrderResponse) -> Result<ClobOrder, ApiError> {
    let is_working = match response.status {
        OrderStatusType::Live | OrderStatusType::Delayed | OrderStatusType::Unmatched => true,
        OrderStatusType::Matched | OrderStatusType::Canceled => false,
        OrderStatusType::Unknown(value) => {
            return Err(ApiError::Deserialize {
                context: "CLOB exact order status".to_owned(),
                detail: format!("unknown order status: {value}"),
            });
        }
        _ => {
            return Err(ApiError::Deserialize {
                context: "CLOB exact order status".to_owned(),
                detail: "unsupported order status".to_owned(),
            });
        }
    };
    Ok(ClobOrder {
        order_id: OrderId::new(response.id),
        is_working,
        original_size: Shares::new(response.original_size),
        matched_size: Shares::new(response.size_matched),
        associated_trade_ids: response
            .associate_trades
            .into_iter()
            .map(VenueTradeId::new)
            .collect(),
    })
}

fn map_trade_response(trade: TradeResponse) -> Result<ClobTrade, ApiError> {
    let status = match trade.status {
        TradeStatusType::Matched => VenueTradeStatus::Matched,
        TradeStatusType::Mined => VenueTradeStatus::Mined,
        TradeStatusType::Confirmed => VenueTradeStatus::Confirmed,
        TradeStatusType::Retrying => VenueTradeStatus::Retrying,
        TradeStatusType::Failed => VenueTradeStatus::Failed,
        TradeStatusType::Unknown(value) => {
            return Err(ApiError::Deserialize {
                context: "CLOB trade status".to_owned(),
                detail: format!("unknown trade status: {value}"),
            });
        }
        _ => {
            return Err(ApiError::Deserialize {
                context: "CLOB trade status".to_owned(),
                detail: "unsupported trade status".to_owned(),
            });
        }
    };
    let trader_side = match trade.trader_side {
        TraderSide::Taker => FeeLiquidityRole::Taker,
        TraderSide::Maker => FeeLiquidityRole::Maker,
        TraderSide::Unknown(value) => {
            return Err(ApiError::Deserialize {
                context: "CLOB trade trader_side".to_owned(),
                detail: format!("unknown trader side: {value}"),
            });
        }
        _ => {
            return Err(ApiError::Deserialize {
                context: "CLOB trade trader_side".to_owned(),
                detail: "unsupported trader side".to_owned(),
            });
        }
    };
    let authenticated_maker_order = trade
        .maker_orders
        .iter()
        .find(|order| order.owner == trade.owner);
    let (order_id, token_id, side, matched_size, matched_price, matched_fee_rate_bps) =
        match trader_side {
            FeeLiquidityRole::Taker => (
                OrderId::new(&trade.taker_order_id),
                TokenId::new(trade.asset_id.to_string()),
                ClobSide::try_from(trade.side)
                    .map(|side| side.0)
                    .map_err(|error| ApiError::Deserialize {
                        context: "CLOB trade side".to_owned(),
                        detail: error.to_string(),
                    })?,
                Shares::new(trade.size),
                Price::new(trade.price),
                Bps::new(trade.fee_rate_bps),
            ),
            FeeLiquidityRole::Maker => {
                let order = authenticated_maker_order.ok_or_else(|| ApiError::Deserialize {
                    context: "CLOB trade maker_orders".to_owned(),
                    detail: "maker-side trade has no order owned by the authenticated account"
                        .to_owned(),
                })?;
                (
                    OrderId::new(&order.order_id),
                    TokenId::new(order.asset_id.to_string()),
                    ClobSide::try_from(order.side)
                        .map(|side| side.0)
                        .map_err(|error| ApiError::Deserialize {
                            context: "CLOB authenticated maker order side".to_owned(),
                            detail: error.to_string(),
                        })?,
                    Shares::new(order.matched_amount),
                    Price::new(order.price),
                    Bps::new(order.fee_rate_bps),
                )
            }
        };
    let maker_orders = trade
        .maker_orders
        .into_iter()
        .map(|order| {
            Ok(ClobMakerOrder {
                order_id: OrderId::new(&order.order_id),
                side: ClobSide::try_from(order.side)
                    .map(|side| side.0)
                    .map_err(|error| ApiError::Deserialize {
                        context: "CLOB trade maker order side".to_owned(),
                        detail: error.to_string(),
                    })?,
                size: Shares::new(order.matched_amount),
                price: Price::new(order.price),
                fee_rate_bps: Bps::new(order.fee_rate_bps),
                token_id: TokenId::new(order.asset_id.to_string()),
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let transaction_hash = if trade.transaction_hash == B256::ZERO {
        None
    } else {
        Some(
            EvmTransactionHash::parse(format!("{:#x}", trade.transaction_hash)).map_err(
                |error| ApiError::Deserialize {
                    context: "CLOB trade transaction hash".to_owned(),
                    detail: error.to_string(),
                },
            )?,
        )
    };
    Ok(ClobTrade {
        trade_id: VenueTradeId::new(trade.id),
        bucket_index: trade.bucket_index,
        order_id,
        market_id: MarketId::new(format!("{:#x}", trade.market)),
        token_id,
        side,
        size: matched_size,
        price: matched_price,
        fee_rate_bps: matched_fee_rate_bps,
        trader_side,
        maker_orders,
        status,
        transaction_hash,
        matched_at: trade.match_time,
    })
}

impl ClobClient {
    /// Fetch the venue's market/day maker-rebate awards. The endpoint is
    /// keyless, but the response identity is still validated against the exact
    /// requested wallet and date.
    pub async fn maker_rebate_reported_accruals(
        &self,
        date: NaiveDate,
    ) -> Result<Vec<MakerRebateReportedAccrual>, ApiError> {
        self.rate_limiter.acquire("GET /rebates/current").await;
        let url = format!(
            "{}/rebates/current?date={date}&maker_address={}",
            self.clob_base_url.trim_end_matches('/'),
            self.maker_address,
        );
        let raw_body = get_text_with_retry(&self.http, &RetryPolicy::clob_default(), &url).await?;
        let raw = serde_json::from_str::<Vec<RawMakerRebateReportedAccrual>>(&raw_body).map_err(
            |error| ApiError::Deserialize {
                context: "CLOB maker rebate awards".to_owned(),
                detail: error.to_string(),
            },
        )?;
        raw.into_iter()
            .map(|award| normalize_maker_rebate_award(award, date, &self.maker_address))
            .collect()
    }

    /// Maker identity frozen into the authenticated SDK order builder and all
    /// maker-scoped read APIs.
    #[must_use]
    pub const fn maker_address(&self) -> &EvmAddress {
        &self.maker_address
    }

    /// Capture one append-only V2 CLOB market-info observation.
    #[tracing::instrument(skip(self), fields(market_id = %market_id))]
    pub async fn clob_market_info_version(
        &self,
        market_id: &MarketId,
    ) -> Result<ClobMarketInfoVersion, ApiError> {
        self.rate_limiter
            .acquire("GET /clob-markets/{condition_id}")
            .await;
        let url = format!(
            "{}/clob-markets/{}",
            self.clob_base_url.trim_end_matches('/'),
            market_id.as_str()
        );
        let raw_body = get_text_with_retry(&self.http, &RetryPolicy::clob_default(), &url).await?;
        let raw_payload =
            serde_json::from_str::<Value>(&raw_body).map_err(|error| ApiError::Deserialize {
                context: "CLOB market-info raw payload".to_owned(),
                detail: error.to_string(),
            })?;
        let raw =
            serde_json::from_value::<RawClobMarketInfo>(raw_payload.clone()).map_err(|error| {
                ApiError::Deserialize {
                    context: "CLOB market-info contract".to_owned(),
                    detail: error.to_string(),
                }
            })?;
        // Validate the exact captured bytes against the pinned official SDK wire
        // contract. A second HTTP request would compare different observations,
        // double venue load, and introduce a market-update TOCTOU race.
        let response = serde_json::from_value::<ClobMarketInfoResponse>(raw_payload.clone())
            .map_err(|error| ApiError::Deserialize {
                context: "pinned SDK CLOB market-info contract".to_owned(),
                detail: error.to_string(),
            })?;
        let sdk_tokens = response
            .tokens
            .into_iter()
            .flatten()
            .map(|token| ClobTokenDescriptor {
                token_id: TokenId::new(token.token_id.to_string()),
                outcome: token.outcome,
            })
            .collect::<Vec<_>>();
        let tick_size =
            TickSize::try_from(raw.minimum_tick_size).map_err(|error| ApiError::Clob {
                endpoint: "GET /clob-markets/{condition_id}".to_owned(),
                code: "unsupported_tick_size".to_owned(),
                message: error.to_string(),
                retryable: false,
            })?;
        let tokens = raw
            .tokens
            .into_iter()
            .map(|token| ClobTokenDescriptor {
                token_id: TokenId::new(token.token_id),
                outcome: token.outcome,
            })
            .collect::<Vec<_>>();
        let fee_details = ClobFeeDetails {
            rate: raw.fee_details.rate,
            exponent: raw.fee_details.exponent,
            taker_only: raw.fee_details.taker_only,
        };
        let sdk_fee_details = response.fee_details.map(|fee| ClobFeeDetails {
            rate: fee.rate,
            exponent: fee.exponent,
            taker_only: fee.taker_only,
        });
        if sdk_tokens != tokens
            || response.min_tick_size.as_decimal() != raw.minimum_tick_size
            || response.min_order_size != raw.minimum_order_size
            || response.neg_risk != raw.neg_risk
            || sdk_fee_details.as_ref() != Some(&fee_details)
            || response.maker_base_fee.and_then(|value| value.to_u32())
                != Some(raw.builder_maker_fee_rate_bps)
            || response.taker_base_fee.and_then(|value| value.to_u32())
                != Some(raw.builder_taker_fee_rate_bps)
        {
            return Err(ApiError::Clob {
                endpoint: "GET /clob-markets/{condition_id}".to_owned(),
                code: "market_info_read_inconsistent".to_owned(),
                message: "raw response and pinned SDK projection disagree".to_owned(),
                retryable: true,
            });
        }
        let payload_hash = CanonicalDigest::content_hash_json(&raw_payload).map_err(|error| {
            ApiError::Deserialize {
                context: "CLOB market info hash".to_owned(),
                detail: error.to_string(),
            }
        })?;
        let available_at = Utc::now();
        let version = ClobMarketInfoVersion {
            version_id: ClobMarketInfoVersionId::from_v7(),
            market_id: market_id.clone(),
            tokens,
            tick_size,
            minimum_order_size: Shares::new(raw.minimum_order_size),
            neg_risk: raw.neg_risk,
            taker_order_delay_enabled: raw.taker_order_delay_enabled,
            minimum_order_age_secs: Some(raw.minimum_order_age_secs),
            blockaid_check_enabled: raw.blockaid_check_enabled,
            fee_details,
            builder_maker_fee_rate_bps: raw.builder_maker_fee_rate_bps,
            builder_taker_fee_rate_bps: raw.builder_taker_fee_rate_bps,
            effective_at: available_at,
            available_at,
            payload_hash,
            raw_payload,
        };
        version.validate().map_err(|detail| ApiError::Deserialize {
            context: "CLOB market info".to_owned(),
            detail,
        })?;
        Ok(version)
    }

    /// Read the venue's atomic `/book` identity and order rules for a token.
    /// The response is validated against the requested asset before exposure.
    pub async fn order_metadata(&self, token_id: &TokenId) -> Result<VenueOrderMetadata, ApiError> {
        let response = self.fetch_book_response(token_id).await?;
        metadata_from_book(token_id, &response)
    }

    async fn fetch_book_response(
        &self,
        token_id: &TokenId,
    ) -> Result<OrderBookSummaryResponse, ApiError> {
        self.rate_limiter.acquire("GET /book").await;
        let wire_token_id = WireTokenId::try_from(token_id)?.0;
        let sdk = Arc::clone(&self.sdk);
        let expected = token_id.clone();
        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            let expected = expected.clone();
            async move {
                let request = OrderBookSummaryRequest::builder()
                    .token_id(wire_token_id)
                    .build();
                let response = sdk
                    .order_book(&request)
                    .await
                    .map_err(|error| SdkClobError(&error).snapshot("order book"))?;
                metadata_from_book(&expected, &response)?;
                Ok(response)
            }
        })
        .await
    }

    /// Authenticate with Polymarket CLOB and create a connected client.
    ///
    /// The [`WalletTopology`] binds the venue signature type and the money-holding
    /// funder: an EOA signs as itself (signature type 0, no funder), while a
    /// Proxy / Gnosis Safe routes orders and balance reads through the derived
    /// funder wallet (signature type 1 / 2). This keeps order placement and Data
    /// API position reads on the *same* account.
    pub async fn connect(
        signer: Arc<OrderSigner>,
        config: &PolymarketConfig,
        topology: &WalletTopology,
    ) -> Result<Self, ApiError> {
        // Authentication and every later EIP-712 order signature must share the
        // same chain-bound signer. Keeping the unbound input would make the SDK
        // panic after WAL persistence when it reads `Signer::chain_id()`.
        let signer = Arc::new(signer.as_ref().clone().with_chain_id(Some(config.chain_id)));

        let sdk_config = SdkConfig::builder().use_server_time(true).build();
        let sdk = SdkClient::new(&config.clob_base_url, sdk_config)
            .map_err(|error| ApiError::from(SdkClobError(&error)))?;
        let protocol_version = sdk
            .version()
            .await
            .map_err(|error| ApiError::from(SdkClobError(&error)))?;
        if protocol_version != 2 {
            return Err(ApiError::Clob {
                endpoint: "GET /version".to_owned(),
                code: "unsupported_protocol_version".to_owned(),
                message: format!(
                    "quant-pivot requires CLOB V2 but the endpoint reported version {protocol_version}"
                ),
                retryable: false,
            });
        }
        let builder = sdk.authentication_builder(signer.inner());
        // EOA is the SDK default; only Proxy / Safe attach an explicit funder +
        // signature type (the SDK rejects a funder paired with the EOA type).
        let builder = if topology.is_eoa() {
            builder
        } else {
            builder
                .signature_type(topology.signature_type)
                .funder(topology.funder)
        };
        let sdk = builder
            .authenticate()
            .await
            .map_err(|e| ApiError::from(SdkClobError(&e)))?;
        let http = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|error| ApiError::Http {
                method: "GET",
                url: config.clob_base_url.clone(),
                status: 0,
                body: error.to_string(),
                retryable: false,
            })?;
        let maker_address =
            EvmAddress::parse(format!("{:#x}", topology.funder)).map_err(|error| {
                ApiError::Deserialize {
                    context: "authenticated CLOB maker address".to_owned(),
                    detail: error.to_string(),
                }
            })?;

        Ok(Self {
            sdk: Arc::new(sdk),
            http,
            clob_base_url: config.clob_base_url.clone(),
            maker_address,
            chain_id: config.chain_id,
            signature_type: topology.signature_type,
            signer,
            sdk_rule_lock: Mutex::new(()),
            order_post_timeout: Duration::from_millis(config.order_post_timeout_ms),
            rate_limiter: RateLimiter::new(),
            on_book_level_rejected: None,
        })
    }

    /// Attach a hook invoked when REST book ingest rejects an invalid level.
    #[must_use]
    pub fn with_level_reject_hook(mut self, hook: Option<BookLevelRejectHook>) -> Self {
        self.on_book_level_rejected = hook;
        self
    }

    fn validate_order_semantics(order_type: OrderType, post_only: bool) -> Result<(), ApiError> {
        if post_only && !matches!(order_type, OrderType::Gtc | OrderType::Gtd { .. }) {
            return Err(ApiError::Clob {
                endpoint: "POST /order".to_owned(),
                code: "invalid_post_only_order_type".to_owned(),
                message: "post-only is valid only for GTC/GTD limit orders".to_owned(),
                retryable: false,
            });
        }
        Ok(())
    }

    fn exact_spender(&self, neg_risk: bool) -> Result<Address, ApiError> {
        // V2 order transfer authority is the same route-specific exchange used
        // as the EIP-712 verifying contract. The NegRisk adapter owns separate
        // split/merge operations and is never accepted as an order-allowance
        // fallback here.
        contract_config(self.chain_id, neg_risk)
            .and_then(|config| config.exchange_v2)
            .ok_or_else(|| {
                ClobOrderError::SpenderUnavailable {
                    chain_id: self.chain_id,
                    neg_risk,
                }
                .into()
            })
    }

    async fn fetch_balance_allowance(
        &self,
        asset: VenueFundingAsset,
        token_id: Option<&TokenId>,
    ) -> Result<ParsedBalanceAllowance, ApiError> {
        let wire_token_id = token_id
            .map(WireTokenId::try_from)
            .transpose()?
            .map(|id| id.0);
        match (asset, wire_token_id) {
            (VenueFundingAsset::Collateral, Some(_)) => {
                return Err(ClobOrderError::IdentityMismatch {
                    field: "balance_allowance.token_id",
                    expected: "absent for collateral".to_owned(),
                    actual: token_id.map_or_else(String::new, ToString::to_string),
                }
                .into());
            }
            (VenueFundingAsset::Conditional, None) => {
                return Err(ClobOrderError::IdentityMismatch {
                    field: "balance_allowance.token_id",
                    expected: "present for conditional token".to_owned(),
                    actual: "absent".to_owned(),
                }
                .into());
            }
            (VenueFundingAsset::Collateral, None) | (VenueFundingAsset::Conditional, Some(_)) => {}
        }
        self.rate_limiter.acquire("GET /balance-allowance").await;
        let sdk = Arc::clone(&self.sdk);
        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            async move {
                let mut request = BalanceAllowanceRequest::builder()
                    .asset_type(match asset {
                        VenueFundingAsset::Collateral => AssetType::Collateral,
                        VenueFundingAsset::Conditional => AssetType::Conditional,
                    })
                    .build();
                request.token_id = wire_token_id;
                let response = sdk
                    .balance_allowance(request)
                    .await
                    .map_err(|error| SdkClobError(&error).snapshot("balance allowance"))?;
                parse_balance_allowance(response)
            }
        })
        .await
    }

    async fn parsed_funding_snapshot(
        &self,
        asset: VenueFundingAsset,
        token_id: Option<&TokenId>,
        metadata: &VenueOrderMetadata,
    ) -> Result<ParsedFundingSnapshot, ApiError> {
        if let Some(token_id) = token_id
            && token_id != &metadata.token_id
        {
            return Err(ClobOrderError::IdentityMismatch {
                field: "balance_allowance.token_id",
                expected: metadata.token_id.to_string(),
                actual: token_id.to_string(),
            }
            .into());
        }
        let spender = self.exact_spender(metadata.neg_risk)?;
        let parsed = self.fetch_balance_allowance(asset, token_id).await?;
        let allowance = parsed.allowances.get(&spender).copied();
        let human_balance = match asset {
            VenueFundingAsset::Collateral => VenueFundingBalance::Collateral(Usd::new(
                human_units("collateral_balance", parsed.balance)?,
            )),
            VenueFundingAsset::Conditional => VenueFundingBalance::Conditional(Shares::new(
                human_units("conditional_balance", parsed.balance)?,
            )),
        };
        let snapshot = VenueBalanceAllowanceSnapshot {
            asset,
            token_id: token_id.cloned(),
            spender: evm_address(spender)?,
            balance: project_uint256("balance", parsed.balance)?,
            human_balance,
            allowance: allowance
                .map(|value| project_uint256("allowance", value))
                .transpose()?,
        };
        Ok(ParsedFundingSnapshot {
            snapshot,
            balance: parsed.balance,
            allowance,
        })
    }

    /// Read and strictly parse the exact V2 exchange funding snapshot for one
    /// route and asset. This never sends an approval or cache-update request.
    pub async fn balance_allowance_snapshot(
        &self,
        asset: VenueFundingAsset,
        token_id: Option<&TokenId>,
        metadata: &VenueOrderMetadata,
    ) -> Result<VenueBalanceAllowanceSnapshot, ApiError> {
        Ok(self
            .parsed_funding_snapshot(asset, token_id, metadata)
            .await?
            .snapshot)
    }

    async fn funding_evidence(
        &self,
        request: &OrderRequest,
        metadata: &VenueOrderMetadata,
        canonical: &CanonicalOrderAmounts,
    ) -> Result<VenueFundingEvidence, ApiError> {
        let (asset, token_id, required_amount) = match request.side {
            Side::Buy => {
                let total = canonical
                    .maker_amount
                    .checked_add(request.expected_fee.inner())
                    .ok_or_else(|| ClobOrderError::RuleViolation {
                        detail: "BUY principal plus fee overflows decimal".to_owned(),
                    })?;
                (VenueFundingAsset::Collateral, None, total)
            }
            Side::Sell => (
                VenueFundingAsset::Conditional,
                Some(&request.token_id),
                canonical.maker_amount,
            ),
        };
        let parsed = self
            .parsed_funding_snapshot(asset, token_id, metadata)
            .await?;
        let required = wire_units("required_funding", required_amount)?;
        let required_value = project_uint256("required_funding", required)?;
        let evidence = if parsed.balance < required {
            VenueFundingEvidence::InsufficientBalance {
                snapshot: parsed.snapshot,
                required: required_value,
            }
        } else {
            match parsed.allowance {
                None => VenueFundingEvidence::MissingAllowance {
                    snapshot: parsed.snapshot,
                    required: required_value,
                },
                Some(allowance) if allowance < required => {
                    VenueFundingEvidence::InsufficientAllowance {
                        snapshot: parsed.snapshot,
                        required: required_value,
                    }
                }
                Some(_) => VenueFundingEvidence::Ready {
                    snapshot: parsed.snapshot,
                    required: required_value,
                },
            }
        };
        Ok(evidence)
    }

    async fn validated_funding_evidence(
        &self,
        request: &OrderRequest,
        metadata: &VenueOrderMetadata,
    ) -> Result<(CanonicalOrderAmounts, VenueFundingEvidence), ApiError> {
        Self::validate_order_semantics(request.order_type, request.post_only)?;
        verify_order_identity(request, metadata)?;
        let rules = PolymarketOrderRules::new(metadata.tick_size, metadata.minimum_order_size)
            .map_err(|error| ClobOrderError::RuleViolation {
                detail: error.to_string(),
            })?;
        let canonical = rules
            .validate_order(request.side, request.amount, request.price)
            .map_err(|error| ClobOrderError::RuleViolation {
                detail: error.to_string(),
            })?;
        let funding = self.funding_evidence(request, metadata, &canonical).await?;
        Ok((canonical, funding))
    }

    async fn prepare_order_evidence(
        &self,
        request: &OrderRequest,
    ) -> Result<PreparedOrderEvidence, ApiError> {
        let metadata = self.order_metadata(&request.token_id).await?;
        verify_order_identity(request, &metadata)?;
        let market_info = self.clob_market_info_version(&request.market_id).await?;
        verify_market_info_identity(request, &metadata, &market_info)?;
        let (canonical, funding) = self.validated_funding_evidence(request, &metadata).await?;
        Ok(PreparedOrderEvidence {
            metadata,
            canonical,
            funding,
        })
    }

    /// Build funding evidence from the caller's single live `/book` metadata
    /// observation and one `/balance-allowance` read. This method revalidates
    /// request identity but never fetches `/book` itself. Admission may map a
    /// valid closed state to Defer; malformed, rule, and transport failures
    /// remain errors.
    pub async fn order_funding_evidence(
        &self,
        request: &OrderRequest,
        metadata: &VenueOrderMetadata,
    ) -> Result<VenueFundingEvidence, ApiError> {
        Ok(self.validated_funding_evidence(request, metadata).await?.1)
    }

    fn require_funding(evidence: &VenueFundingEvidence) -> Result<(), ApiError> {
        let Some(deficit) = evidence.deficit() else {
            return Ok(());
        };
        Err(ClobOrderError::FundingUnavailable {
            deficit,
            asset: evidence.snapshot().asset.as_str(),
            spender: evidence.snapshot().spender.to_string(),
            required: evidence.required().to_string(),
            balance: evidence.snapshot().balance.to_string(),
            allowance: evidence
                .snapshot()
                .allowance
                .as_ref()
                .map_or_else(|| "missing".to_owned(), ToString::to_string),
        }
        .into())
    }

    fn seed_order_rules(
        &self,
        token_id: U256,
        metadata: &VenueOrderMetadata,
    ) -> Result<(), ApiError> {
        let tick_size =
            SdkTickSize::try_from(metadata.tick_size.as_decimal()).map_err(|error| {
                ClobOrderError::RuleViolation {
                    detail: error.to_string(),
                }
            })?;
        self.sdk.set_tick_size(token_id, tick_size);
        self.sdk.set_neg_risk(token_id, metadata.neg_risk);
        Ok(())
    }

    async fn build_unsigned_order(
        &self,
        req: &OrderRequest,
        canonical: CanonicalOrderAmounts,
        token_id: U256,
        order_side: SdkSide,
    ) -> Result<SignableOrder, OrderSubmissionError> {
        let price = req.price.inner();
        let order_amount = canonical.venue_amount;
        let post_only = req.post_only;
        match req.order_type {
            OrderType::Fok | OrderType::Fak => {
                let amount = sdk_amount(order_amount).map_err(OrderSubmissionError::prepare)?;
                let order_type = if matches!(req.order_type, OrderType::Fok) {
                    SdkOrderType::FOK
                } else {
                    SdkOrderType::FAK
                };
                self.sdk
                    .market_order()
                    .token_id(token_id)
                    .order_type(order_type)
                    .price(price)
                    .amount(amount)
                    .side(order_side)
                    .build()
                    .await
                    .map_err(|error| {
                        OrderSubmissionError::prepare(ApiError::from(SdkClobError(&error)))
                    })
            }
            OrderType::Gtd { expiration } => {
                let expiration = ToPrimitive::to_i64(&expiration)
                    .and_then(|timestamp| DateTime::from_timestamp(timestamp, 0))
                    .ok_or_else(|| {
                        OrderSubmissionError::prepare(ApiError::Clob {
                            endpoint: "POST /order".to_owned(),
                            code: "invalid_gtd_expiration".to_owned(),
                            message: format!("GTD expiration {expiration} is not representable"),
                            retryable: false,
                        })
                    })?;
                let shares =
                    limit_order_shares(order_amount).map_err(OrderSubmissionError::prepare)?;
                self.sdk
                    .limit_order()
                    .token_id(token_id)
                    .order_type(SdkOrderType::GTD)
                    .expiration(expiration)
                    .price(price)
                    .size(shares)
                    .side(order_side)
                    .post_only(post_only)
                    .build()
                    .await
                    .map_err(|error| {
                        OrderSubmissionError::prepare(ApiError::from(SdkClobError(&error)))
                    })
            }
            OrderType::Gtc => {
                let shares =
                    limit_order_shares(order_amount).map_err(OrderSubmissionError::prepare)?;
                self.sdk
                    .limit_order()
                    .token_id(token_id)
                    .order_type(SdkOrderType::GTC)
                    .price(price)
                    .size(shares)
                    .side(order_side)
                    .post_only(post_only)
                    .build()
                    .await
                    .map_err(|error| {
                        OrderSubmissionError::prepare(ApiError::from(SdkClobError(&error)))
                    })
            }
        }
    }

    fn validate_unsigned_order(
        &self,
        request: &OrderRequest,
        token_id: U256,
        order_side: SdkSide,
        canonical: &CanonicalOrderAmounts,
        unsigned: &SignableOrder,
    ) -> Result<(), ApiError> {
        let OrderPayload::V2(payload) = &unsigned.payload else {
            return Err(ClobOrderError::PayloadMismatch {
                field: "protocol_version",
                expected: "2".to_owned(),
                actual: unsigned.payload.version().to_string(),
            }
            .into());
        };
        let expected_maker = Address::from_str(self.maker_address.as_str()).map_err(|error| {
            ClobOrderError::IdentityMismatch {
                field: "authenticated_maker",
                expected: "canonical EVM address".to_owned(),
                actual: error.to_string(),
            }
        })?;
        let expected_signer = if self.signature_type == SignatureType::Poly1271 {
            expected_maker
        } else {
            self.signer.inner().address()
        };
        let expected_order_type = match request.order_type {
            OrderType::Fok => SdkOrderType::FOK,
            OrderType::Fak => SdkOrderType::FAK,
            OrderType::Gtc => SdkOrderType::GTC,
            OrderType::Gtd { .. } => SdkOrderType::GTD,
        };
        let expected_post_only = match request.order_type {
            OrderType::Fok | OrderType::Fak => None,
            OrderType::Gtc | OrderType::Gtd { .. } => Some(request.post_only),
        };
        let expected_expiration = match request.order_type {
            OrderType::Gtd { expiration } => U256::from(expiration),
            OrderType::Fok | OrderType::Fak | OrderType::Gtc => U256::ZERO,
        };
        let maker_amount = wire_units("maker_amount", canonical.maker_amount)?;
        let taker_amount = wire_units("taker_amount", canonical.taker_amount)?;
        for result in [
            require_payload_match("token_id", &token_id, &payload.order.tokenId),
            require_payload_match("maker", &expected_maker, &payload.order.maker),
            require_payload_match("signer", &expected_signer, &payload.order.signer),
            require_payload_match("maker_amount", &maker_amount, &payload.order.makerAmount),
            require_payload_match("taker_amount", &taker_amount, &payload.order.takerAmount),
            require_payload_match("side", &(order_side as u8), &payload.order.side),
            require_payload_match(
                "signature_type",
                &(self.signature_type as u8),
                &payload.order.signatureType,
            ),
            require_payload_match("expiration", &expected_expiration, &payload.expiration),
            require_payload_match("order_type", &expected_order_type, &unsigned.order_type),
            require_payload_match("post_only", &expected_post_only, &unsigned.post_only),
        ] {
            result?;
        }
        Ok(())
    }

    /// Place an order on the CLOB.
    #[tracing::instrument(skip(self, req), fields(market_id = %req.market_id, side = %req.side))]
    pub async fn place_order(
        &self,
        req: &OrderRequest,
    ) -> Result<OrderResponse, OrderSubmissionError> {
        self.rate_limiter.acquire("POST /order").await;

        let sdk = Arc::clone(&self.sdk);
        let signer = Arc::clone(&self.signer);
        let token_id = WireTokenId::try_from(&req.token_id)
            .map_err(OrderSubmissionError::prepare)?
            .0;
        let order_side = SdkSide::from(ClobSide::from(req.side));
        let order_type = req.order_type;
        let prepared = self
            .prepare_order_evidence(req)
            .await
            .map_err(OrderSubmissionError::prepare)?;
        Self::require_funding(&prepared.funding).map_err(OrderSubmissionError::prepare)?;
        let sdk_rule_guard = self.sdk_rule_lock.lock().await;
        self.seed_order_rules(token_id, &prepared.metadata)
            .map_err(OrderSubmissionError::prepare)?;

        let submitted_at = Utc::now();
        let unsigned = self
            .build_unsigned_order(req, prepared.canonical, token_id, order_side)
            .await?;
        self.validate_unsigned_order(req, token_id, order_side, &prepared.canonical, &unsigned)
            .map_err(OrderSubmissionError::prepare)?;

        let signed_order = sdk
            .sign(signer.inner(), unsigned)
            .await
            .map_err(|e| OrderSubmissionError::sign(ApiError::from(SdkClobError(&e))))?;
        drop(sdk_rule_guard);

        let resp = tokio::time::timeout(self.order_post_timeout, sdk.post_order(signed_order))
            .await
            .map_err(|_| {
                OrderSubmissionError::post(ApiError::Timeout {
                    operation: "POST /order".to_owned(),
                    elapsed_ms: u64::try_from(self.order_post_timeout.as_millis())
                        .unwrap_or(u64::MAX),
                })
            })?
            .map_err(|e| OrderSubmissionError::post(ApiError::from(SdkClobError(&e))))?;

        map_post_order_response(
            req,
            order_type,
            prepared.canonical.venue_amount,
            submitted_at,
            &resp,
        )
        .map_err(OrderSubmissionError::post)
    }

    /// Cancel a single order by ID.
    #[tracing::instrument(skip(self), fields(order_id = %order_id))]
    pub async fn cancel_order(&self, order_id: &OrderId) -> Result<CancelResult, ApiError> {
        self.rate_limiter.acquire("DELETE /order").await;

        let sdk = Arc::clone(&self.sdk);
        let oid = order_id.to_string();

        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            let oid = oid.clone();
            async move {
                let resp = sdk
                    .cancel_order(&oid)
                    .await
                    .map_err(|e| ApiError::from(SdkClobError(&e)))?;

                let success = resp.canceled.contains(&oid);
                let reason = resp.not_canceled.get(&oid).cloned();

                Ok(CancelResult {
                    order_id: OrderId::new(&oid),
                    success,
                    reason,
                })
            }
        })
        .await
    }

    /// Cancel all open orders.
    #[tracing::instrument(skip(self))]
    pub async fn cancel_all(&self) -> Result<CancelAllResult, ApiError> {
        self.rate_limiter.acquire("DELETE /order").await;

        let sdk = Arc::clone(&self.sdk);

        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            async move {
                let resp = sdk
                    .cancel_all_orders()
                    .await
                    .map_err(|e| ApiError::from(SdkClobError(&e)))?;

                Ok(CancelAllResult {
                    canceled: resp
                        .canceled
                        .into_iter()
                        .map(|id| OrderId::new(&id))
                        .collect(),
                    not_canceled: resp
                        .not_canceled
                        .into_iter()
                        .map(|(id, reason)| (OrderId::new(&id), reason))
                        .collect(),
                })
            }
        })
        .await
    }

    /// Fetch a full orderbook snapshot for a token.
    #[tracing::instrument(skip(self), fields(token_id = %token_id))]
    pub async fn get_book(&self, token_id: &TokenId) -> Result<OrderbookSnapshot, ApiError> {
        let response = self.fetch_book_response(token_id).await?;
        let metadata = metadata_from_book(token_id, &response)?;
        let on_rejected = self.on_book_level_rejected.clone();
        let mut bids = Vec::with_capacity(response.bids.len());
        for level in &response.bids {
            push_rest_level(&mut bids, level.price, level.size, on_rejected.as_ref());
        }
        let mut asks = Vec::with_capacity(response.asks.len());
        for level in &response.asks {
            push_rest_level(&mut asks, level.price, level.size, on_rejected.as_ref());
        }
        let timestamp_ms =
            u64::try_from(response.timestamp.timestamp_millis()).map_err(|error| {
                ClobOrderError::MalformedUint256 {
                    field: "book.timestamp",
                    value: response.timestamp.to_rfc3339(),
                    detail: error.to_string(),
                }
            })?;
        Ok(OrderbookSnapshot {
            metadata,
            bids,
            asks,
            hash: response.hash.unwrap_or_default(),
            timestamp_ms,
        })
    }

    /// Fetch both YES and NO token books and build a strict binary-market snapshot.
    ///
    /// This method intentionally fails if either token book cannot be fetched.
    /// Consumers must never synthesize a NO book from YES prices.
    #[tracing::instrument(skip(self), fields(yes_token = %yes_token, no_token = %no_token))]
    pub async fn get_dual_book(
        &self,
        yes_token: &TokenId,
        no_token: &TokenId,
    ) -> Result<QuantBookSnapshot, ApiError> {
        let (yes_book, no_book) =
            tokio::try_join!(self.get_book(yes_token), self.get_book(no_token))?;

        Ok(QuantBookSnapshot {
            yes_bids: OrderbookSide {
                levels: yes_book.bids,
                timestamp_ms: yes_book.timestamp_ms,
            },
            yes_asks: OrderbookSide {
                levels: yes_book.asks,
                timestamp_ms: yes_book.timestamp_ms,
            },
            no_bids: OrderbookSide {
                levels: no_book.bids,
                timestamp_ms: no_book.timestamp_ms,
            },
            no_asks: OrderbookSide {
                levels: no_book.asks,
                timestamp_ms: no_book.timestamp_ms,
            },
        })
    }

    /// Query current pUSD collateral balance.
    ///
    /// Uses the CLOB `balance-allowance` endpoint with `AssetType::Collateral`.
    /// Strictly decodes the raw six-decimal units and returns human-scale USD
    /// before subtracting local reservations.
    #[tracing::instrument(skip(self))]
    pub async fn collateral_balance(&self) -> Result<Usd, ApiError> {
        let snapshot = self
            .fetch_balance_allowance(VenueFundingAsset::Collateral, None)
            .await?;
        Ok(Usd::new(human_units(
            "collateral_balance",
            snapshot.balance,
        )?))
    }

    /// Query the current conditional-token (ERC-1155 outcome share) balance.
    ///
    /// Uses the CLOB `balance-allowance` endpoint with `AssetType::Conditional`
    /// scoped to `token_id`. Strictly decodes raw six-decimal units into the
    /// human-scale share balance used by reconciliation.
    #[tracing::instrument(skip(self), fields(token_id = ?token_id))]
    pub async fn token_balance(&self, token_id: &TokenId) -> Result<Shares, ApiError> {
        let snapshot = self
            .fetch_balance_allowance(VenueFundingAsset::Conditional, Some(token_id))
            .await?;
        Ok(Shares::new(human_units(
            "conditional_balance",
            snapshot.balance,
        )?))
    }

    /// List authenticated account trades inside an explicit query boundary.
    #[tracing::instrument(skip(self), fields(market_id = ?market_id, token_id = ?token_id))]
    pub async fn get_trades(
        &self,
        market_id: Option<&MarketId>,
        token_id: Option<&TokenId>,
        after: Option<i64>,
        before: Option<i64>,
    ) -> Result<Vec<ClobTrade>, ApiError> {
        self.rate_limiter.acquire("GET /data/trades").await;

        let sdk = Arc::clone(&self.sdk);
        let market = market_id
            .map(|id| B256::from_str(id.as_str()))
            .transpose()
            .map_err(|error| ApiError::Deserialize {
                context: "CLOB trades market id".into(),
                detail: error.to_string(),
            })?;
        let asset_id = token_id
            .map(|id| U256::from_str(id.as_str()))
            .transpose()
            .map_err(|error| ApiError::Deserialize {
                context: "CLOB trades token id".into(),
                detail: error.to_string(),
            })?;

        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            async move {
                let mut request = TradesRequest::builder().build();
                request.market = market;
                request.asset_id = asset_id;
                request.after = after;
                request.before = before;
                let resp = sdk
                    .trades(&request, None)
                    .await
                    .map_err(|e| ApiError::from(SdkClobError(&e)))?;

                resp.data.into_iter().map(map_trade_response).collect()
            }
        })
        .await
    }

    /// Load one order by its durable venue ID, including its associated trade IDs.
    #[tracing::instrument(skip(self), fields(order_id = %order_id))]
    pub async fn get_order(&self, order_id: &OrderId) -> Result<ClobOrder, ApiError> {
        self.rate_limiter.acquire("GET /data/order/{id}").await;
        let sdk = Arc::clone(&self.sdk);
        let order_id = order_id.to_string();
        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            let order_id = order_id.clone();
            async move {
                sdk.order(&order_id)
                    .await
                    .map_err(|error| ApiError::from(SdkClobError(&error)))
                    .and_then(map_order_lookup)
            }
        })
        .await
    }

    /// Load one authenticated trade by its globally unique venue trade ID.
    #[tracing::instrument(skip(self), fields(trade_id = %trade_id))]
    pub async fn get_trade(&self, trade_id: &VenueTradeId) -> Result<Option<ClobTrade>, ApiError> {
        self.rate_limiter.acquire("GET /data/trades?id").await;
        let sdk = Arc::clone(&self.sdk);
        let trade_id = trade_id.to_string();
        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            let trade_id = trade_id.clone();
            async move {
                let request = TradesRequest::builder().id(trade_id.clone()).build();
                let response = sdk
                    .trades(&request, None)
                    .await
                    .map_err(|error| ApiError::from(SdkClobError(&error)))?;
                response
                    .data
                    .into_iter()
                    .find(|trade| trade.id == trade_id)
                    .map(map_trade_response)
                    .transpose()
            }
        })
        .await
    }

    /// List all open orders for the authenticated account.
    #[tracing::instrument(skip(self))]
    pub async fn get_open_orders(&self) -> Result<Vec<OpenOrder>, ApiError> {
        self.rate_limiter.acquire("GET /orders").await;

        let sdk = Arc::clone(&self.sdk);

        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            async move {
                let request = OrdersRequest::default();
                let resp = sdk
                    .orders(&request, None)
                    .await
                    .map_err(|e| ApiError::from(SdkClobError(&e)))?;

                let orders: Vec<OpenOrder> = resp
                    .data
                    .into_iter()
                    .map(|o| {
                        Ok(OpenOrder {
                            order_id: OrderId::new(&o.id),
                            token_id: TokenId::new(o.asset_id.to_string()),
                            side: ClobSide::try_from(o.side).map(|s| s.0).map_err(|e| {
                                ApiError::Deserialize {
                                    context: "CLOB open order side".into(),
                                    detail: e.to_string(),
                                }
                            })?,
                            price: Price::new(o.price),
                            size: Shares::new(o.original_size),
                            filled: Shares::new(o.size_matched),
                        })
                    })
                    .collect::<Result<Vec<_>, ApiError>>()?;

                Ok(orders)
            }
        })
        .await
    }
}

fn normalize_maker_rebate_award(
    raw: RawMakerRebateReportedAccrual,
    expected_date: NaiveDate,
    expected_maker: &EvmAddress,
) -> Result<MakerRebateReportedAccrual, ApiError> {
    let asset_address =
        EvmAddress::parse(raw.asset_address.to_ascii_lowercase()).map_err(|error| {
            ApiError::Deserialize {
                context: "CLOB maker rebate asset address".to_owned(),
                detail: error.to_string(),
            }
        })?;
    let maker_address =
        EvmAddress::parse(raw.maker_address.to_ascii_lowercase()).map_err(|error| {
            ApiError::Deserialize {
                context: "CLOB maker rebate maker address".to_owned(),
                detail: error.to_string(),
            }
        })?;
    if raw.date != expected_date || &maker_address != expected_maker {
        return Err(ApiError::Deserialize {
            context: "CLOB maker rebate identity".to_owned(),
            detail: "response date or maker differs from request".to_owned(),
        });
    }
    if raw.rebated_fees_usdc < Decimal::ZERO {
        return Err(ApiError::Deserialize {
            context: "CLOB maker rebate amount".to_owned(),
            detail: "negative venue award".to_owned(),
        });
    }
    Ok(MakerRebateReportedAccrual {
        program_date: raw.date,
        market_id: MarketId::new(raw.condition_id),
        asset_address,
        maker_address,
        amount_usd: Usd::new(raw.rebated_fees_usdc),
    })
}

#[cfg(test)]
mod tests;
