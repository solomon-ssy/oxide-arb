//! Polymarket Data API client (keyless venue position reads).
//!
//! The Data API (`https://data-api.polymarket.com`) exposes a wallet's open
//! positions — already marked to the venue's current price — via
//! `GET /positions?user=<proxy/funder>`. No credentials are required; only the
//! funder (proxy) address. The report capital base uses these positions to
//! compute net liquidation value, so this client is on the report-generation
//! critical path.
//!
//! Wire shape follows the official [`OpenAPI`] `Position` schema:
//! <https://docs.polymarket.com/api-reference/core/get-current-positions-for-a-user>.
//! Unknown response fields are ignored so the venue may extend the schema without
//! breaking reads.
//!
//! [`OpenAPI`]: https://polymarket-docs.copilot.markets/api-reference/core/get-current-positions-for-a-user

use chrono::{DateTime, Utc};
use quant_pivot_error::api::ApiError;
use quant_pivot_models::{
    config::DataApiConfig,
    types::{EvmAddress, EvmTransactionHash, MarketId, Usd},
};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    infra::{
        http::is_retryable_status,
        retry::{self, RetryPolicy},
    },
    wire::decimal::de_decimal,
};

const ACTIVITY_PAGE_MAX: u32 = 500;
const ACTIVITY_OFFSET_MAX: u32 = 5_000;

/// Wallet-credit incentive type exposed by the venue activity API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum VenueIncentiveCreditKind {
    MakerRebate,
    TakerRebate,
}

/// Wallet-confirmed incentive credit. It remains at the dimensions supplied by
/// the venue and is never backfilled into trade economics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VenueIncentiveCredit {
    pub proxy_wallet: EvmAddress,
    pub occurred_at: DateTime<Utc>,
    pub market_id: Option<MarketId>,
    pub kind: VenueIncentiveCreditKind,
    pub amount_usd: Usd,
    pub transaction_hash: EvmTransactionHash,
}

#[derive(Debug, Clone, Copy, Deserialize)]
enum RawIncentiveCreditKind {
    #[serde(rename = "MAKER_REBATE")]
    MakerRebate,
    #[serde(rename = "TAKER_REBATE")]
    TakerRebate,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawIncentiveCredit {
    proxy_wallet: String,
    timestamp: i64,
    condition_id: String,
    #[serde(rename = "type")]
    kind: RawIncentiveCreditKind,
    #[serde(deserialize_with = "de_decimal")]
    usdc_size: Decimal,
    transaction_hash: String,
}

/// A single venue position as returned by the Data API.
///
/// Tiered mapping of the `OpenAPI` `Position` schema: core fields used for capital
/// base / registry mapping, plus `PnL` and flag fields for contract fidelity.
/// UI-only metadata (`title`, `slug`, `icon`, etc.) is intentionally omitted.
///
/// Money/price fields are parsed losslessly from JSON via their decimal text
/// (never through binary `f64`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VenuePosition {
    /// Proxy wallet that holds the position on-chain (when present).
    pub proxy_wallet: Option<String>,
    /// CLOB outcome token id.
    pub asset: String,
    /// Owning market (`condition_id`).
    pub condition_id: String,
    /// Shares held.
    #[serde(deserialize_with = "de_decimal")]
    pub size: Decimal,
    /// Average entry price (cost basis).
    #[serde(deserialize_with = "de_decimal")]
    pub avg_price: Decimal,
    /// Initial marked value at entry (venue-reported).
    #[serde(deserialize_with = "de_decimal")]
    pub initial_value: Decimal,
    /// Current venue mark price.
    #[serde(deserialize_with = "de_decimal")]
    pub cur_price: Decimal,
    /// Current marked value in USD (venue-reported).
    #[serde(deserialize_with = "de_decimal")]
    pub current_value: Decimal,
    /// Unrealized cash `PnL` (venue-reported).
    #[serde(deserialize_with = "de_decimal")]
    pub cash_pnl: Decimal,
    /// Unrealized percent `PnL` (venue-reported).
    #[serde(deserialize_with = "de_decimal")]
    pub percent_pnl: Decimal,
    /// Total shares bought (venue-reported).
    #[serde(deserialize_with = "de_decimal")]
    pub total_bought: Decimal,
    /// Realized cash `PnL` (venue-reported).
    #[serde(deserialize_with = "de_decimal")]
    pub realized_pnl: Decimal,
    /// Realized percent `PnL` (venue-reported).
    #[serde(deserialize_with = "de_decimal")]
    pub percent_realized_pnl: Decimal,
    /// Whether the position is redeemable (market resolved).
    pub redeemable: bool,
    /// Whether the position can be merged with its opposite outcome.
    pub mergeable: bool,
    /// Whether the market uses negative-risk collateral.
    pub negative_risk: bool,
    /// Outcome label (e.g. `Yes` / `No`).
    pub outcome: String,
    /// Outcome index within the market.
    pub outcome_index: i32,
}

/// Polymarket Data API client.
///
/// All calls are wrapped with retry/backoff. Reads are keyless.
pub struct DataApiClient {
    config: DataApiConfig,
    http: Client,
    retry_policy: RetryPolicy,
}

impl DataApiClient {
    /// Build a client from deploy configuration.
    #[must_use]
    pub fn new(config: DataApiConfig) -> Self {
        Self {
            config,
            http: Client::new(),
            retry_policy: RetryPolicy::gamma_default(),
        }
    }

    /// Override the HTTP client (tests inject a `no_proxy` client at a mock URL).
    #[must_use]
    pub fn with_http_client(mut self, http: Client) -> Self {
        self.http = http;
        self
    }

    /// Override the retry policy.
    #[must_use]
    pub const fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    /// Fetch all open positions for a proxy/funder address, following pagination.
    ///
    /// Pages are requested with `limit` / `offset` until a short page is seen.
    pub async fn positions(&self, funder: &str) -> Result<Vec<VenuePosition>, ApiError> {
        let limit = self.config.page_size.max(1);
        let mut offset: u32 = 0;
        let mut all = Vec::new();
        loop {
            let page = self.fetch_page(funder, limit, offset).await?;
            let page_len = page.len();
            all.extend(page);
            if u32::try_from(page_len).unwrap_or(u32::MAX) < limit {
                break;
            }
            offset = offset.saturating_add(limit);
        }
        Ok(all)
    }

    /// Fetch wallet-confirmed maker/taker rebate credits in a closed epoch-
    /// second window. Stable ascending pagination is mandatory; reaching the
    /// venue's offset cap fails closed so credits cannot be silently skipped.
    pub async fn incentive_credits(
        &self,
        funder: &EvmAddress,
        start: i64,
        end: i64,
    ) -> Result<Vec<VenueIncentiveCredit>, ApiError> {
        if start < 0 || end < start {
            return Err(ApiError::Deserialize {
                context: "data-api incentive activity window".to_owned(),
                detail: "expected 0 <= start <= end".to_owned(),
            });
        }
        let limit = self.config.page_size.clamp(1, ACTIVITY_PAGE_MAX);
        let mut offset = 0_u32;
        let mut credits = Vec::new();
        loop {
            let page = self
                .fetch_incentive_page(funder, start, end, limit, offset)
                .await?;
            let page_len = u32::try_from(page.len()).unwrap_or(u32::MAX);
            for raw in page {
                credits.push(normalize_incentive_credit(raw, funder)?);
            }
            if page_len < limit {
                break;
            }
            offset = offset
                .checked_add(limit)
                .ok_or_else(|| ApiError::Deserialize {
                    context: "data-api incentive activity pagination".to_owned(),
                    detail: "offset overflow".to_owned(),
                })?;
            if offset > ACTIVITY_OFFSET_MAX {
                return Err(ApiError::Deserialize {
                    context: "data-api incentive activity pagination".to_owned(),
                    detail: "activity window exceeds venue offset cap; split the time window"
                        .to_owned(),
                });
            }
        }
        Ok(credits)
    }

    /// Fetch one positions page.
    async fn fetch_page(
        &self,
        funder: &str,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<VenuePosition>, ApiError> {
        let base_url = self.config.base_url.trim_end_matches('/');
        let url = format!(
            "{base_url}/positions?user={funder}&limit={limit}&offset={offset}&sizeThreshold={}",
            self.config.size_threshold
        );
        retry::retry_with_policy(&self.retry_policy, || {
            let http = self.http.clone();
            let url = url.clone();
            async move {
                let response = http
                    .get(&url)
                    .send()
                    .await
                    .map_err(|error| ApiError::Http {
                        method: "GET",
                        url: url.clone(),
                        status: 0,
                        body: error.to_string(),
                        retryable: true,
                    })?;
                let status = response.status();
                if !status.is_success() {
                    let code = status.as_u16();
                    let body = response.text().await.unwrap_or_default();
                    return Err(ApiError::Http {
                        method: "GET",
                        url: url.clone(),
                        status: code,
                        body,
                        retryable: is_retryable_status(code),
                    });
                }
                response
                    .json::<Vec<VenuePosition>>()
                    .await
                    .map_err(|error| ApiError::Deserialize {
                        context: "data-api positions".to_owned(),
                        detail: error.to_string(),
                    })
            }
        })
        .await
    }

    async fn fetch_incentive_page(
        &self,
        funder: &EvmAddress,
        start: i64,
        end: i64,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<RawIncentiveCredit>, ApiError> {
        let base_url = self.config.base_url.trim_end_matches('/');
        let url = format!(
            "{base_url}/activity?user={funder}&type=MAKER_REBATE,TAKER_REBATE&start={start}&end={end}&sortBy=TIMESTAMP&sortDirection=ASC&limit={limit}&offset={offset}"
        );
        retry::retry_with_policy(&self.retry_policy, || {
            let http = self.http.clone();
            let url = url.clone();
            async move {
                let response = http
                    .get(&url)
                    .send()
                    .await
                    .map_err(|error| ApiError::Http {
                        method: "GET",
                        url: url.clone(),
                        status: 0,
                        body: error.to_string(),
                        retryable: true,
                    })?;
                let status = response.status();
                if !status.is_success() {
                    let code = status.as_u16();
                    let body = response.text().await.unwrap_or_default();
                    return Err(ApiError::Http {
                        method: "GET",
                        url: url.clone(),
                        status: code,
                        body,
                        retryable: is_retryable_status(code),
                    });
                }
                response
                    .json::<Vec<RawIncentiveCredit>>()
                    .await
                    .map_err(|error| ApiError::Deserialize {
                        context: "data-api incentive activity".to_owned(),
                        detail: error.to_string(),
                    })
            }
        })
        .await
    }
}

fn normalize_incentive_credit(
    raw: RawIncentiveCredit,
    expected_wallet: &EvmAddress,
) -> Result<VenueIncentiveCredit, ApiError> {
    let proxy_wallet =
        EvmAddress::parse(raw.proxy_wallet.to_ascii_lowercase()).map_err(|error| {
            ApiError::Deserialize {
                context: "data-api incentive proxy wallet".to_owned(),
                detail: error.to_string(),
            }
        })?;
    if &proxy_wallet != expected_wallet {
        return Err(ApiError::Deserialize {
            context: "data-api incentive proxy wallet".to_owned(),
            detail: "response wallet differs from requested wallet".to_owned(),
        });
    }
    if raw.usdc_size < Decimal::ZERO {
        return Err(ApiError::Deserialize {
            context: "data-api incentive amount".to_owned(),
            detail: "negative wallet credit".to_owned(),
        });
    }
    let occurred_at =
        DateTime::from_timestamp(raw.timestamp, 0).ok_or_else(|| ApiError::Deserialize {
            context: "data-api incentive timestamp".to_owned(),
            detail: "timestamp is outside chrono range".to_owned(),
        })?;
    let transaction_hash = EvmTransactionHash::parse(raw.transaction_hash.to_ascii_lowercase())
        .map_err(|error| ApiError::Deserialize {
            context: "data-api incentive transaction hash".to_owned(),
            detail: error.to_string(),
        })?;
    Ok(VenueIncentiveCredit {
        proxy_wallet,
        occurred_at,
        market_id: (!raw.condition_id.is_empty()).then(|| MarketId::new(raw.condition_id)),
        kind: match raw.kind {
            RawIncentiveCreditKind::MakerRebate => VenueIncentiveCreditKind::MakerRebate,
            RawIncentiveCreditKind::TakerRebate => VenueIncentiveCreditKind::TakerRebate,
        },
        amount_usd: Usd::new(raw.usdc_size),
        transaction_hash,
    })
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{config::DataApiConfig, types::EvmAddress};
    use reqwest::Client;
    use rust_decimal_macros::dec;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::*;

    #[test]
    fn deserializes_position_numeric_decimals() {
        let json = serde_json::json!({
            "proxyWallet": "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
            "asset": "123456",
            "conditionId": "0xdeadbeef",
            "size": 100.0,
            "avgPrice": 0.01,
            "initialValue": 1.0,
            "curPrice": 0.02,
            "currentValue": 2.0,
            "cashPnl": 1.0,
            "percentPnl": 100.0,
            "totalBought": 100.0,
            "realizedPnl": 0.0,
            "percentRealizedPnl": 0.0,
            "redeemable": true,
            "mergeable": false,
            "negativeRisk": false,
            "outcome": "Yes",
            "outcomeIndex": 0,
            "title": "ignored UI field"
        });
        let position: VenuePosition = serde_json::from_value(json).expect("parse");
        assert_eq!(
            position.proxy_wallet.as_deref(),
            Some("0x56687bf447db6ffa42ffe2204a05edaa20f55839")
        );
        assert_eq!(position.asset, "123456");
        assert_eq!(position.condition_id, "0xdeadbeef");
        assert_eq!(position.size, dec!(100));
        assert_eq!(position.avg_price, dec!(0.01));
        assert_eq!(position.initial_value, dec!(1));
        assert_eq!(position.current_value, dec!(2));
        assert_eq!(position.cash_pnl, dec!(1));
        assert_eq!(position.percent_pnl, dec!(100));
        assert!(position.redeemable);
        assert!(!position.mergeable);
        assert!(!position.negative_risk);
        assert_eq!(position.outcome, "Yes");
    }

    #[test]
    fn missing_evidence_is_rejected() {
        let json = serde_json::json!({
            "asset": "1",
            "conditionId": "0x1"
        });
        let error = serde_json::from_value::<VenuePosition>(json)
            .expect_err("missing critical account evidence must fail closed");
        assert!(error.to_string().contains("size"));
    }

    #[tokio::test]
    async fn incentive_contract_is_strict() {
        let server = MockServer::start().await;
        let wallet = EvmAddress::parse(format!("0x{}", "1".repeat(40))).expect("wallet");
        let market = format!("0x{}", "2".repeat(64));
        let maker_tx = format!("0x{}", "3".repeat(64));
        let taker_tx = format!("0x{}", "4".repeat(64));
        Mock::given(method("GET"))
            .and(path("/activity"))
            .and(query_param("user", wallet.as_str()))
            .and(query_param("type", "MAKER_REBATE,TAKER_REBATE"))
            .and(query_param("start", "100"))
            .and(query_param("end", "200"))
            .and(query_param("sortBy", "TIMESTAMP"))
            .and(query_param("sortDirection", "ASC"))
            .and(query_param("limit", "500"))
            .and(query_param("offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {
                    "proxyWallet": wallet.as_str(),
                    "timestamp": 101,
                    "conditionId": market.clone(),
                    "type": "MAKER_REBATE",
                    "usdcSize": "1.25",
                    "transactionHash": maker_tx
                },
                {
                    "proxyWallet": wallet.as_str(),
                    "timestamp": 102,
                    "conditionId": "",
                    "type": "TAKER_REBATE",
                    "usdcSize": 0.5,
                    "transactionHash": taker_tx
                }
            ])))
            .expect(1)
            .mount(&server)
            .await;
        let client = DataApiClient::new(DataApiConfig {
            base_url: server.uri(),
            page_size: 500,
            size_threshold: 1,
        })
        .with_http_client(Client::builder().no_proxy().build().expect("HTTP client"));

        let credits = client
            .incentive_credits(&wallet, 100, 200)
            .await
            .expect("incentive credits");

        assert_eq!(credits.len(), 2);
        assert_eq!(credits[0].kind, VenueIncentiveCreditKind::MakerRebate);
        assert_eq!(
            credits[0].market_id.as_ref().map(ToString::to_string),
            Some(market)
        );
        assert_eq!(credits[0].amount_usd, Usd::new(dec!(1.25)));
        assert_eq!(credits[1].kind, VenueIncentiveCreditKind::TakerRebate);
        assert!(credits[1].market_id.is_none());
        assert_eq!(credits[1].amount_usd, Usd::new(dec!(0.5)));
    }

    #[test]
    fn incentive_rejects_wrong_wallet() {
        let expected = EvmAddress::parse(format!("0x{}", "1".repeat(40))).expect("wallet");
        let raw = RawIncentiveCredit {
            proxy_wallet: format!("0x{}", "2".repeat(40)),
            timestamp: 101,
            condition_id: String::new(),
            kind: RawIncentiveCreditKind::MakerRebate,
            usdc_size: dec!(1),
            transaction_hash: format!("0x{}", "3".repeat(64)),
        };

        let error = normalize_incentive_credit(raw, &expected)
            .expect_err("wrong response wallet must fail closed");

        assert!(error.to_string().contains("differs from requested wallet"));
    }
}
