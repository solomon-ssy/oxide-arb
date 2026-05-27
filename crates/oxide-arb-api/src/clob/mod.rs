//! Polymarket CLOB REST client with rate limiting and retry.

mod book;
mod convert;
mod orders;
mod rate_limiter;
mod sdk_error;
mod token;

pub use book::OrderbookSnapshot;
pub use convert::{ClobSide, SdkSideConversionError};
pub use orders::{CancelAllResult, CancelResult, OpenOrder};
pub use rate_limiter::RateLimiter;
pub use sdk_error::SdkClobError;
pub use token::WireTokenId;

use crate::{
    infra::retry::{self, RetryPolicy},
    keystore::OrderSigner,
    ws::BookLevelRejectHook,
};
use num_traits::ToPrimitive;
use oxide_arb_error::api::ApiError;
use oxide_arb_models::{
    config::PolymarketConfig,
    domain::{
        BookLevel,
        book::{EndgameBookSnapshot, OrderbookSide},
        order::{OrderRequest, OrderResponse},
    },
    enums::{common::OrderType, order::OrderStatus},
    types::{OrderId, Price, Shares, TokenId, Usd},
};
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::clob::types::Side as SdkSide;
use polymarket_client_sdk_v2::clob::types::request::{
    BalanceAllowanceRequest, OrderBookSummaryRequest,
};
use polymarket_client_sdk_v2::clob::types::{AssetType, OrderType as SdkOrderType};
use polymarket_client_sdk_v2::clob::{Client as SdkClient, Config as SdkConfig};
use rust_decimal::Decimal;
use std::{convert::TryFrom, sync::Arc};

/// Polymarket CLOB REST client backed by the official SDK.
///
/// Created via [`ClobClient::connect`] which performs async SDK authentication.
/// All methods enforce per-endpoint rate limiting via [`governor`] and
/// retry transient errors via [`retry_with_policy`](retry::retry_with_policy).
pub struct ClobClient {
    sdk: Arc<SdkClient<Authenticated<Normal>>>,
    signer: Arc<OrderSigner>,
    rate_limiter: RateLimiter,
    on_book_level_rejected: Option<BookLevelRejectHook>,
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

impl ClobClient {
    /// Authenticate with Polymarket CLOB and create a connected client.
    pub async fn connect(
        signer: Arc<OrderSigner>,
        config: &PolymarketConfig,
    ) -> Result<Self, ApiError> {
        let sdk_config = SdkConfig::builder().use_server_time(true).build();
        let sdk = SdkClient::new(&config.clob_base_url, sdk_config)
            .map_err(|e| ApiError::from(sdk_error::SdkClobError(&e)))?
            .authentication_builder(signer.inner())
            .authenticate()
            .await
            .map_err(|e| ApiError::from(sdk_error::SdkClobError(&e)))?;

        Ok(Self {
            sdk: Arc::new(sdk),
            signer,
            rate_limiter: RateLimiter::new(),
            on_book_level_rejected: None,
        })
    }

    /// Attach a hook invoked when REST book ingest rejects an invalid level.
    #[must_use]
    pub fn with_book_level_reject_hook(mut self, hook: Option<BookLevelRejectHook>) -> Self {
        self.on_book_level_rejected = hook;
        self
    }

    /// Place an order on the CLOB.
    #[tracing::instrument(skip(self, req), fields(market_id = %req.market_id, side = %req.side))]
    pub async fn place_order(&self, req: &OrderRequest) -> Result<OrderResponse, ApiError> {
        self.rate_limiter.acquire("POST /order").await;

        let sdk = Arc::clone(&self.sdk);
        let signer = Arc::clone(&self.signer);
        let token_id = WireTokenId::try_from(&req.token_id)?.0;
        let order_side = SdkSide::from(ClobSide::from(req.side));
        let price = req.price.inner();
        let share_qty = req.shares.inner();
        let order_type = req.order_type;

        let submitted_at = chrono::Utc::now();
        let retry_policy = RetryPolicy::for_order_type(order_type);

        retry::retry_with_policy(&retry_policy, || {
            let sdk = Arc::clone(&sdk);
            let signer = Arc::clone(&signer);
            async move {
                let unsigned = match order_type {
                    OrderType::Fok => sdk
                        .limit_order()
                        .token_id(token_id)
                        .order_type(SdkOrderType::FOK)
                        .price(price)
                        .size(share_qty)
                        .side(order_side)
                        .build()
                        .await
                        .map_err(|e| ApiError::from(sdk_error::SdkClobError(&e)))?,
                    OrderType::Gtd { expiration } => {
                        let exp = ToPrimitive::to_i64(&expiration)
                            .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0))
                            .unwrap_or_else(chrono::Utc::now);
                        sdk.limit_order()
                            .token_id(token_id)
                            .order_type(SdkOrderType::GTD)
                            .expiration(exp)
                            .price(price)
                            .size(share_qty)
                            .side(order_side)
                            .build()
                            .await
                            .map_err(|e| ApiError::from(sdk_error::SdkClobError(&e)))?
                    }
                    OrderType::Gtc => sdk
                        .limit_order()
                        .token_id(token_id)
                        .order_type(SdkOrderType::GTC)
                        .price(price)
                        .size(share_qty)
                        .side(order_side)
                        .build()
                        .await
                        .map_err(|e| ApiError::from(sdk_error::SdkClobError(&e)))?,
                };

                let signed_order = sdk
                    .sign(signer.inner(), unsigned)
                    .await
                    .map_err(|e| ApiError::from(sdk_error::SdkClobError(&e)))?;

                let resp = sdk
                    .post_order(signed_order)
                    .await
                    .map_err(|e| ApiError::from(sdk_error::SdkClobError(&e)))?;

                let filled_shares = Shares::new(resp.making_amount);
                let requested_shares = share_qty;
                let status = if !resp.success || filled_shares.inner() <= Decimal::ZERO {
                    OrderStatus::Rejected
                } else if filled_shares.inner() >= requested_shares {
                    OrderStatus::Filled
                } else {
                    OrderStatus::PartiallyFilled
                };

                let avg_fill_price = if filled_shares.inner() > Decimal::ZERO {
                    Some(Price::new(resp.taking_amount / filled_shares.inner()))
                } else {
                    None
                };

                Ok(OrderResponse {
                    order_id: OrderId::new(&resp.order_id),
                    status,
                    tx_hash: resp.transaction_hashes.first().map(|h| format!("{h:#x}")),
                    filled_shares,
                    avg_fill_price,
                    fee_paid: Usd::ZERO,
                    submitted_at,
                    responded_at: chrono::Utc::now(),
                })
            }
        })
        .await
    }

    /// Cancel a single order by ID.
    #[tracing::instrument(skip(self), fields(order_id = %order_id))]
    pub async fn cancel_order(&self, order_id: &OrderId) -> Result<CancelResult, ApiError> {
        self.rate_limiter.acquire("DELETE /order").await;

        let sdk = Arc::clone(&self.sdk);
        let oid = order_id.as_str().to_owned();

        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            let oid = oid.clone();
            async move {
                let resp = sdk
                    .cancel_order(&oid)
                    .await
                    .map_err(|e| ApiError::from(sdk_error::SdkClobError(&e)))?;

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
                    .map_err(|e| ApiError::from(sdk_error::SdkClobError(&e)))?;

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
                    .map_err(|e| ApiError::from(sdk_error::SdkClobError(&e)))?;

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

    /// Fetch both YES and NO token books and build a strict endgame snapshot.
    ///
    /// This method intentionally fails if either token book cannot be fetched.
    /// The endgame strategy must never synthesize a NO book from YES prices.
    #[tracing::instrument(skip(self), fields(yes_token = %yes_token, no_token = %no_token))]
    pub async fn get_dual_book(
        &self,
        yes_token: &TokenId,
        no_token: &TokenId,
    ) -> Result<EndgameBookSnapshot, ApiError> {
        let (yes_book, no_book) =
            tokio::try_join!(self.get_book(yes_token), self.get_book(no_token))?;

        Ok(EndgameBookSnapshot {
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

    /// Query current USDC.e collateral balance.
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

    /// List all open orders for the authenticated account.
    #[tracing::instrument(skip(self))]
    pub async fn get_open_orders(&self) -> Result<Vec<OpenOrder>, ApiError> {
        self.rate_limiter.acquire("GET /orders").await;

        let sdk = Arc::clone(&self.sdk);

        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            async move {
                let request =
                    polymarket_client_sdk_v2::clob::types::request::OrdersRequest::default();
                let resp = sdk
                    .orders(&request, None)
                    .await
                    .map_err(|e| ApiError::from(sdk_error::SdkClobError(&e)))?;

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

    /// Inject a pre-authenticated SDK client for wiremock/integration tests.
    #[doc(hidden)]
    pub fn from_sdk_for_test(
        sdk: Arc<SdkClient<Authenticated<Normal>>>,
        signer: Arc<OrderSigner>,
    ) -> Self {
        Self {
            sdk,
            signer,
            rate_limiter: RateLimiter::new(),
            on_book_level_rejected: None,
        }
    }
}
