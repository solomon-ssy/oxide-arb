//! UMA Optimistic Oracle REST source (third vote in 2-of-3 quorum).

use async_trait::async_trait;
use oxide_arb_error::rpc::RpcError;
use oxide_arb_models::config::SettlementOracleConfig;
use oxide_arb_models::types::MarketId;
use std::time::Duration;

use super::source::OracleSource;
use super::types::SourceVote;

/// Queries UMA DVM for assertion settlement status by `condition_id`.
pub struct UmaOracleSource {
    http: reqwest::Client,
    endpoint: String,
}

impl UmaOracleSource {
    pub fn new(config: &SettlementOracleConfig) -> Result<Self, RpcError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.uma_timeout_secs))
            .build()
            .map_err(|e| RpcError::ConnectionFailed(e.to_string()))?;

        Ok(Self {
            http,
            endpoint: config.uma_endpoint.clone(),
        })
    }
}

#[derive(Debug, serde::Deserialize)]
struct UmaAssertion {
    settled: Option<bool>,
    settlement_resolution: Option<bool>,
}

#[async_trait]
impl OracleSource for UmaOracleSource {
    fn source_id(&self) -> &'static str {
        "uma"
    }

    async fn query_resolution(
        &self,
        _market_id: &MarketId,
        condition_id: &str,
    ) -> Result<Option<SourceVote>, RpcError> {
        let url = format!(
            "{}/assertions?condition_id={}",
            self.endpoint.trim_end_matches('/'),
            condition_id
        );

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| RpcError::CallFailed {
                method: "uma/assertions".into(),
                reason: e.to_string(),
            })?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !resp.status().is_success() {
            return Err(RpcError::CallFailed {
                method: "uma/assertions".into(),
                reason: format!("HTTP {}", resp.status()),
            });
        }

        let assertions: Vec<UmaAssertion> =
            resp.json().await.map_err(|e| RpcError::CallFailed {
                method: "uma/assertions".into(),
                reason: e.to_string(),
            })?;

        let settled = assertions.iter().find(|a| a.settled.unwrap_or(false));

        let Some(assertion) = settled else {
            return Ok(None);
        };

        let Some(resolution) = assertion.settlement_resolution else {
            return Ok(None);
        };

        Ok(Some(SourceVote {
            source_id: "uma".into(),
            actual_yes: resolution,
            confidence: rust_decimal::Decimal::ONE,
            reported_at: chrono::Utc::now(),
        }))
    }
}
