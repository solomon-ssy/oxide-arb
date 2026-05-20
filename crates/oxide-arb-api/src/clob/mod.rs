//! Polymarket CLOB REST client with rate limiting and retry.

mod book;
mod orders;
mod rate_limiter;

pub use book::{BookLevel, OrderbookSnapshot};
pub use orders::{CancelAllResult, CancelResult, OpenOrder};
pub use rate_limiter::RateLimiter;

use crate::infra::retry::{self, RetryPolicy};
use crate::keystore::OrderSigner;
use oxide_arb_error::api::ApiError;
use oxide_arb_models::config::PolymarketConfig;
use oxide_arb_models::domain::order::{OrderRequest, OrderResponse, OrderStatus};
use oxide_arb_models::enums::common::{OrderType, Side};
use oxide_arb_models::types::{OrderId, Price, Shares, TokenId, Usd};
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::clob::types::request::OrderBookSummaryRequest;
use polymarket_client_sdk_v2::clob::types::{OrderType as SdkOrderType, Side as SdkSide};
use polymarket_client_sdk_v2::clob::{Client as SdkClient, Config as SdkConfig};
use polymarket_client_sdk_v2::types::U256;
use std::str::FromStr;
use std::sync::Arc;

/// Polymarket CLOB REST client backed by the official SDK.
///
/// Created via [`ClobClient::connect`] which performs async SDK authentication.
/// All methods enforce per-endpoint rate limiting via [`governor`] and
/// retry transient errors via [`retry_with_policy`](retry::retry_with_policy).
pub struct ClobClient {
    sdk: Arc<SdkClient<Authenticated<Normal>>>,
    signer: Arc<OrderSigner>,
    rate_limiter: RateLimiter,
}

impl ClobClient {
    /// Authenticate with Polymarket CLOB and create a connected client.
    pub async fn connect(
        signer: Arc<OrderSigner>,
        config: &PolymarketConfig,
    ) -> Result<Self, ApiError> {
        let sdk_config = SdkConfig::builder().use_server_time(true).build();
        let sdk = SdkClient::new(&config.clob_base_url, sdk_config)
            .map_err(|e| ApiError::Sdk(e.to_string()))?
            .authentication_builder(signer.inner())
            .authenticate()
            .await
            .map_err(|e| ApiError::Sdk(e.to_string()))?;

        Ok(Self {
            sdk: Arc::new(sdk),
            signer,
            rate_limiter: RateLimiter::new(),
        })
    }

    /// Place an order on the CLOB.
    #[tracing::instrument(skip(self, req), fields(market_id = %req.market_id, side = %req.side))]
    pub async fn place_order(&self, req: &OrderRequest) -> Result<OrderResponse, ApiError> {
        self.rate_limiter.acquire("POST /order").await;

        let sdk = Arc::clone(&self.sdk);
        let signer = Arc::clone(&self.signer);
        let token_id = parse_token_id(&req.token_id)?;
        let order_side = SdkSide::from(req.side);
        let price = req.price.inner();
        let share_qty = req.shares.inner();
        let order_type = &req.order_type;

        let order_type_clone = *order_type;
        let submitted_at = chrono::Utc::now();

        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            let signer = Arc::clone(&signer);
            async move {
                let unsigned = match order_type_clone {
                    OrderType::Fok => sdk
                        .limit_order()
                        .token_id(token_id)
                        .order_type(SdkOrderType::FOK)
                        .price(price)
                        .size(share_qty)
                        .side(order_side)
                        .build()
                        .await
                        .map_err(|e| ApiError::Sdk(e.to_string()))?,
                    OrderType::Gtd { expiration } => {
                        let exp = i64::try_from(expiration)
                            .ok()
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
                            .map_err(|e| ApiError::Sdk(e.to_string()))?
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
                        .map_err(|e| ApiError::Sdk(e.to_string()))?,
                    OrderType::Fak => sdk
                        .limit_order()
                        .token_id(token_id)
                        .order_type(SdkOrderType::FOK)
                        .price(price)
                        .size(share_qty)
                        .side(order_side)
                        .build()
                        .await
                        .map_err(|e| ApiError::Sdk(e.to_string()))?,
                };

                let signed_order = sdk
                    .sign(signer.inner(), unsigned)
                    .await
                    .map_err(|e| ApiError::Sdk(e.to_string()))?;

                let resp = sdk
                    .post_order(signed_order)
                    .await
                    .map_err(|e| ApiError::Sdk(e.to_string()))?;

                Ok(OrderResponse {
                    order_id: OrderId::new(&resp.order_id),
                    status: if resp.success {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::Rejected
                    },
                    tx_hash: resp.transaction_hashes.first().map(|h| format!("{h:#x}")),
                    filled_shares: Shares::new(resp.making_amount),
                    avg_fill_price: None,
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
                    .map_err(|e| ApiError::Sdk(e.to_string()))?;

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
                    .map_err(|e| ApiError::Sdk(e.to_string()))?;

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

        let sdk = Arc::clone(&self.sdk);
        let tid = parse_token_id(token_id)?;

        retry::retry_with_policy(&RetryPolicy::clob_default(), || {
            let sdk = Arc::clone(&sdk);
            async move {
                let request = OrderBookSummaryRequest::builder().token_id(tid).build();
                let resp = sdk
                    .order_book(&request)
                    .await
                    .map_err(|e| ApiError::Sdk(e.to_string()))?;

                let bids = resp
                    .bids
                    .iter()
                    .map(|l| BookLevel {
                        price: Price::new(l.price),
                        size: Shares::new(l.size),
                    })
                    .collect();

                let asks = resp
                    .asks
                    .iter()
                    .map(|l| BookLevel {
                        price: Price::new(l.price),
                        size: Shares::new(l.size),
                    })
                    .collect();

                Ok(OrderbookSnapshot {
                    token_id: token_id.clone(),
                    bids,
                    asks,
                    hash: resp.hash.unwrap_or_default(),
                    timestamp_ms: u64::try_from(resp.timestamp.timestamp_millis()).unwrap_or(0),
                })
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
                    .map_err(|e| ApiError::Sdk(e.to_string()))?;

                let orders: Vec<OpenOrder> = resp
                    .data
                    .into_iter()
                    .map(|o| {
                        Ok(OpenOrder {
                            order_id: OrderId::new(&o.id),
                            token_id: TokenId::new(o.asset_id.to_string()),
                            side: Side::try_from(o.side).map_err(|e| ApiError::Deserialize {
                                context: "CLOB open order side".into(),
                                detail: e.to_string(),
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

#[inline]
fn parse_token_id(token_id: &TokenId) -> Result<U256, ApiError> {
    U256::from_str(token_id.as_str()).map_err(|e| ApiError::Deserialize {
        context: "token_id to U256".into(),
        detail: e.to_string(),
    })
}
