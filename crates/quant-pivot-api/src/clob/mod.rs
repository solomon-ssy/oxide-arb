//! Polymarket CLOB REST client with rate limiting and retry.

mod book;
mod convert;
mod orders;
mod rate_limiter;
mod sdk_error;
mod token;

use std::{convert::TryFrom, str::FromStr, sync::Arc, time::Duration};

use alloy::signers::Signer;
pub use book::OrderbookSnapshot;
use chrono::{DateTime, NaiveDate, Utc};
pub use convert::{ClobSide, SdkSideConversionError};
use num_traits::ToPrimitive;
pub use orders::{CancelAllResult, CancelResult, ClobMakerOrder, ClobOrder, ClobTrade, OpenOrder};
use polymarket_client_sdk_v2::{
    auth::{Normal, state::Authenticated},
    clob::{
        Client as SdkClient, Config as SdkConfig,
        types::{
            Amount, AssetType, OrderStatusType, OrderType as SdkOrderType, Side as SdkSide,
            SignableOrder, TradeStatusType, TraderSide,
            request::{
                BalanceAllowanceRequest, OrderBookSummaryRequest, OrdersRequest, TradesRequest,
            },
            response::{
                ClobMarketInfoResponse, OpenOrderResponse, PostOrderResponse, TradeResponse,
            },
        },
    },
    types::{B256, U256},
};
use quant_pivot_error::api::ApiError;
use quant_pivot_models::{
    config::PolymarketConfig,
    domain::{
        market::{
            BookLevel,
            book::{OrderbookSide, QuantBookSnapshot},
        },
        order::{OrderRequest, OrderResponse},
    },
    enums::{
        common::{OrderType, Side, TickSize},
        execution::{VenueOrderStatus, VenueTradeStatus},
        fee::FeeLiquidityRole,
    },
    hashing::CanonicalDigest,
    types::{
        Bps, ClobFeeDetails, ClobMarketInfoVersion, ClobMarketInfoVersionId, ClobTokenDescriptor,
        EvmAddress, EvmTransactionHash, MarketId, OrderId, Price, Shares, TokenId, Usd,
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
    signer: Arc<OrderSigner>,
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

/// Venue-owned order metadata re-read immediately before admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenueOrderMetadata {
    pub tick_size: TickSize,
    pub neg_risk: bool,
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
        VenueOrderAmount::GrossUsd(value) => Amount::usdc(value.inner()),
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
                .gross_usd()
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
            .gross_usd()
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
            minimum_order_size: raw.minimum_order_size,
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

    /// Read the SDK's authoritative tick-size and `NegRisk` metadata for a token.
    ///
    /// The official SDK caches these endpoints and also consumes them while
    /// building the signed order. Admission compares this view with the frozen
    /// registry before allowing a money-changing claim.
    pub async fn order_metadata(&self, token_id: &TokenId) -> Result<VenueOrderMetadata, ApiError> {
        let token_id = WireTokenId::try_from(token_id)?.0;
        let (tick, neg_risk) = tokio::try_join!(
            async {
                self.sdk
                    .tick_size(token_id)
                    .await
                    .map_err(|error| ApiError::from(sdk_error::SdkClobError(&error)))
            },
            async {
                self.sdk
                    .neg_risk(token_id)
                    .await
                    .map_err(|error| ApiError::from(sdk_error::SdkClobError(&error)))
            }
        )?;
        let tick_size =
            TickSize::try_from(tick.minimum_tick_size.as_decimal()).map_err(|error| {
                ApiError::Clob {
                    endpoint: "GET /tick-size".to_owned(),
                    code: "unsupported_tick_size".to_owned(),
                    message: error.to_string(),
                    retryable: false,
                }
            })?;
        Ok(VenueOrderMetadata {
            tick_size,
            neg_risk: neg_risk.neg_risk,
        })
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
        // Polymarket L1 auth / EIP-712 requires `chain_id` on the alloy signer.
        let mut auth_signer = signer.inner().clone();
        auth_signer.set_chain_id(Some(config.chain_id));

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
        let builder = sdk.authentication_builder(&auth_signer);
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
            signer,
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

    async fn build_unsigned_order(
        &self,
        req: &OrderRequest,
        token_id: U256,
        order_side: SdkSide,
    ) -> Result<SignableOrder, OrderSubmissionError> {
        let price = req.price.inner();
        let order_amount = req.amount;
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

    async fn verify_buy_balance(&self, req: &OrderRequest) -> Result<(), OrderSubmissionError> {
        if req.side != Side::Buy {
            return Ok(());
        }
        let principal = match req.amount {
            VenueOrderAmount::GrossUsd(gross) => gross,
            VenueOrderAmount::Shares(shares) => Usd::new(shares.inner() * req.price.inner()),
        };
        let required = principal + req.expected_fee;
        let available = self
            .collateral_balance()
            .await
            .map_err(OrderSubmissionError::prepare)?;
        if available < required {
            return Err(OrderSubmissionError::prepare(ApiError::Clob {
                endpoint: "GET /balance-allowance".to_owned(),
                code: "insufficient_pusd_balance".to_owned(),
                message: format!(
                    "live pUSD collateral {available} is below exact admitted cash requirement {required}"
                ),
                retryable: false,
            }));
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
        let order_amount = req.amount;
        let order_type = req.order_type;
        let post_only = req.post_only;
        Self::validate_order_semantics(order_type, post_only)
            .map_err(OrderSubmissionError::prepare)?;
        self.verify_buy_balance(req).await?;

        let submitted_at = Utc::now();
        let unsigned = self.build_unsigned_order(req, token_id, order_side).await?;

        let signed_order = sdk
            .sign(signer.inner(), unsigned)
            .await
            .map_err(|e| OrderSubmissionError::sign(ApiError::from(SdkClobError(&e))))?;

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

        map_post_order_response(req, order_type, order_amount, submitted_at, &resp)
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
        self.rate_limiter.acquire("GET /book").await;

        let tid = WireTokenId::try_from(token_id)?.0;
        let sdk = Arc::clone(&self.sdk);
        let on_rejected = self.on_book_level_rejected.clone();

        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            let on_rejected = on_rejected.clone();
            async move {
                let request = OrderBookSummaryRequest::builder().token_id(tid).build();
                let resp = sdk
                    .order_book(&request)
                    .await
                    .map_err(|e| ApiError::from(SdkClobError(&e)))?;

                let mut bids = Vec::with_capacity(resp.bids.len());
                for level in &resp.bids {
                    push_rest_level(&mut bids, level.price, level.size, on_rejected.as_ref());
                }

                let mut asks = Vec::with_capacity(resp.asks.len());
                for level in &resp.asks {
                    push_rest_level(&mut asks, level.price, level.size, on_rejected.as_ref());
                }

                Ok(OrderbookSnapshot {
                    token_id: token_id.clone(),
                    bids,
                    asks,
                    hash: resp.hash.unwrap_or_default(),
                    timestamp_ms: ToPrimitive::to_u64(&resp.timestamp.timestamp_millis().max(0))
                        .unwrap_or(0),
                })
            }
        })
        .await
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
    /// Returns the raw on-exchange balance (before subtracting reservations).
    #[tracing::instrument(skip(self))]
    pub async fn collateral_balance(&self) -> Result<Usd, ApiError> {
        self.rate_limiter.acquire("GET /balance-allowance").await;

        let sdk = Arc::clone(&self.sdk);

        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            async move {
                let request = BalanceAllowanceRequest::builder()
                    .asset_type(AssetType::Collateral)
                    .build();

                let resp = sdk
                    .balance_allowance(request)
                    .await
                    .map_err(|e| ApiError::from(SdkClobError(&e)))?;

                Ok(Usd::new(resp.balance))
            }
        })
        .await
    }

    /// Query the current conditional-token (ERC-1155 outcome share) balance.
    ///
    /// Uses the CLOB `balance-allowance` endpoint with `AssetType::Conditional`
    /// scoped to `token_id`. Returns the raw on-exchange share balance — the
    /// reconciliation evidence for whether outcome shares were actually received.
    #[tracing::instrument(skip(self), fields(token_id = ?token_id))]
    pub async fn token_balance(&self, token_id: &TokenId) -> Result<Shares, ApiError> {
        self.rate_limiter.acquire("GET /balance-allowance").await;

        let sdk = Arc::clone(&self.sdk);
        let asset_id = WireTokenId::try_from(token_id)?.0;

        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            async move {
                let mut request = BalanceAllowanceRequest::builder()
                    .asset_type(AssetType::Conditional)
                    .build();
                request.token_id = Some(asset_id);

                let resp = sdk
                    .balance_allowance(request)
                    .await
                    .map_err(|e| ApiError::from(SdkClobError(&e)))?;

                Ok(Shares::new(resp.balance))
            }
        })
        .await
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
