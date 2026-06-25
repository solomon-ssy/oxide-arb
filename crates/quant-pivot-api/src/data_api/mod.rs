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
//! <https://polymarket-docs.copilot.markets/api-reference/core/get-current-positions-for-a-user>.
//! Unknown response fields are ignored so the venue may extend the schema without
//! breaking reads.
//!
//! [`OpenAPI`]: https://polymarket-docs.copilot.markets/api-reference/core/get-current-positions-for-a-user

use crate::infra::retry::{self, RetryPolicy};
use quant_pivot_error::api::ApiError;
use quant_pivot_models::config::DataApiConfig;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};

/// A single venue position as returned by the Data API.
///
/// Tiered mapping of the [`OpenAPI`] `Position` schema: core fields used for capital
/// base / registry mapping, plus `PnL` and flag fields for contract fidelity.
/// UI-only metadata (`title`, `slug`, `icon`, etc.) is intentionally omitted.
///
/// Money/price fields are parsed losslessly from JSON via their decimal text
/// (never through binary `f64`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VenuePosition {
    /// Proxy wallet that holds the position on-chain (when present).
    #[serde(default)]
    pub proxy_wallet: Option<String>,
    /// CLOB outcome token id.
    #[serde(default)]
    pub asset: String,
    /// Owning market (`condition_id`).
    #[serde(default)]
    pub condition_id: String,
    /// Shares held.
    #[serde(default, deserialize_with = "de_decimal")]
    pub size: Decimal,
    /// Average entry price (cost basis).
    #[serde(default, deserialize_with = "de_decimal")]
    pub avg_price: Decimal,
    /// Initial marked value at entry (venue-reported).
    #[serde(default, deserialize_with = "de_decimal")]
    pub initial_value: Decimal,
    /// Current venue mark price.
    #[serde(default, deserialize_with = "de_decimal")]
    pub cur_price: Decimal,
    /// Current marked value in USD (venue-reported).
    #[serde(default, deserialize_with = "de_decimal")]
    pub current_value: Decimal,
    /// Unrealized cash `PnL` (venue-reported).
    #[serde(default, deserialize_with = "de_decimal")]
    pub cash_pnl: Decimal,
    /// Unrealized percent `PnL` (venue-reported).
    #[serde(default, deserialize_with = "de_decimal")]
    pub percent_pnl: Decimal,
    /// Total shares bought (venue-reported).
    #[serde(default, deserialize_with = "de_decimal")]
    pub total_bought: Decimal,
    /// Realized cash `PnL` (venue-reported).
    #[serde(default, deserialize_with = "de_decimal")]
    pub realized_pnl: Decimal,
    /// Realized percent `PnL` (venue-reported).
    #[serde(default, deserialize_with = "de_decimal")]
    pub percent_realized_pnl: Decimal,
    /// Whether the position is redeemable (market resolved).
    #[serde(default)]
    pub redeemable: bool,
    /// Whether the position can be merged with its opposite outcome.
    #[serde(default)]
    pub mergeable: bool,
    /// Whether the market uses negative-risk collateral.
    #[serde(default)]
    pub negative_risk: bool,
    /// Outcome label (e.g. `Yes` / `No`).
    #[serde(default)]
    pub outcome: String,
    /// Outcome index within the market.
    #[serde(default)]
    pub outcome_index: i32,
}

/// Polymarket Data API client.
///
/// All calls are wrapped with retry/backoff. Reads are keyless.
pub struct DataApiClient {
    config: DataApiConfig,
    http: reqwest::Client,
    retry_policy: RetryPolicy,
}

impl DataApiClient {
    /// Build a client from deploy configuration.
    #[must_use]
    pub fn new(config: DataApiConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
            retry_policy: RetryPolicy::gamma_default(),
        }
    }

    /// Override the HTTP client (tests inject a `no_proxy` client at a mock URL).
    #[must_use]
    pub fn with_http_client(mut self, http: reqwest::Client) -> Self {
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
}

/// HTTP status codes worth retrying (rate limit + transient server errors).
const fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

/// Deserialize a decimal from a JSON number or string without binary `f64` loss.
fn de_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    use std::str::FromStr;

    let value = serde_json::Value::deserialize(deserializer)?;
    match &value {
        serde_json::Value::Null => Ok(Decimal::ZERO),
        serde_json::Value::Number(number) => {
            Decimal::from_str(&number.to_string()).map_err(serde::de::Error::custom)
        }
        serde_json::Value::String(text) => {
            Decimal::from_str(text).map_err(serde::de::Error::custom)
        }
        other => Err(serde::de::Error::custom(format!(
            "expected decimal number or string, got {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn deserializes_position_with_numeric_and_lossless_decimals() {
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
    fn missing_optional_numbers_default_to_zero() {
        let json = serde_json::json!({
            "asset": "1",
            "conditionId": "0x1"
        });
        let position: VenuePosition = serde_json::from_value(json).expect("parse");
        assert_eq!(position.size, Decimal::ZERO);
        assert_eq!(position.current_value, Decimal::ZERO);
        assert!(!position.redeemable);
    }
}
