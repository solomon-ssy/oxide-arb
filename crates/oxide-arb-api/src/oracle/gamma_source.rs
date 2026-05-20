//! Gamma API-based oracle source.

use async_trait::async_trait;
use oxide_arb_error::rpc::RpcError;
use oxide_arb_models::types::MarketId;

use super::source::OracleSource;
use super::types::SourceVote;

/// Oracle source that checks Gamma API for market resolution.
pub struct GammaOracleSource {
    base_url: String,
    http: reqwest::Client,
}

impl GammaOracleSource {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            http: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl OracleSource for GammaOracleSource {
    fn source_id(&self) -> &'static str {
        "gamma"
    }

    async fn query_resolution(
        &self,
        _market_id: &MarketId,
        condition_id: &str,
    ) -> Result<Option<SourceVote>, RpcError> {
        let url = format!("{}/markets/{}", self.base_url, condition_id);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| RpcError::CallFailed {
                method: "gamma/markets".into(),
                reason: e.to_string(),
            })?;

        if !resp.status().is_success() {
            return Err(RpcError::CallFailed {
                method: "gamma/markets".into(),
                reason: format!("HTTP {}", resp.status()),
            });
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| RpcError::CallFailed {
            method: "gamma/markets".into(),
            reason: e.to_string(),
        })?;

        let closed = body
            .get("closed")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if !closed {
            return Ok(None);
        }

        let outcome = body.get("outcome").and_then(|v| v.as_str()).unwrap_or("");

        let actual_yes = outcome == "Yes" || outcome == "yes" || outcome == "1";

        Ok(Some(SourceVote {
            source_id: "gamma".into(),
            actual_yes,
            confidence: rust_decimal::Decimal::ONE,
            reported_at: chrono::Utc::now(),
        }))
    }
}
