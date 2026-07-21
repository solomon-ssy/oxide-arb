//! Read-only Polygon RPC client for exchange `OrderFilled` logs.

use std::time::Duration;

use alloy::{
    eips::BlockNumberOrTag,
    primitives::{Address, B256},
    providers::{DynProvider as AlloyProvider, Provider, ProviderBuilder},
    rpc::{
        client::RpcClient,
        types::{Filter, Log},
    },
    transports::http::Http,
};
use quant_pivot_error::rpc::RpcError;
use quant_pivot_models::config::OnchainConfig;
use reqwest::Client as ReqwestClient;
use url::Url;

/// One log returned from `eth_getLogs`.
#[derive(Debug, Clone)]
pub struct FetchedLog {
    pub log: Log,
    pub block_number: u64,
    pub block_timestamp: u64,
}

/// Errors from exchange log fetches.
#[derive(Debug, thiserror::Error)]
pub enum LogFetchError {
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error("log response missing block number")]
    MissingBlockNumber,
    #[error("log response missing block timestamp")]
    MissingBlockTimestamp,
}

impl From<LogFetchError> for RpcError {
    fn from(error: LogFetchError) -> Self {
        match error {
            LogFetchError::Rpc(rpc) => rpc,
            other => Self::CallFailed {
                method: "exchange_order_filled_logs".into(),
                reason: other.to_string(),
            },
        }
    }
}

/// Read-only client for Polymarket exchange `OrderFilled` logs.
pub struct ExchangeLogClient {
    provider: AlloyProvider,
}

impl ExchangeLogClient {
    /// Connect a read-only client from deploy on-chain config.
    pub fn connect(config: &OnchainConfig) -> Result<Self, RpcError> {
        let rpc_url = Url::parse(config.rpc_url()).map_err(|error| {
            RpcError::ConnectionFailed(format!(
                "configured Polygon RPC endpoint is invalid: {error}"
            ))
        })?;
        let http_client = ReqwestClient::builder()
            .timeout(Duration::from_millis(config.rpc_timeout_ms))
            .build()
            .map_err(|error| {
                RpcError::ConnectionFailed(format!(
                    "failed to build Polygon RPC HTTP client: {error}"
                ))
            })?;
        let transport = Http::with_client(http_client, rpc_url);
        let rpc_client = RpcClient::new(transport, false);
        let provider = ProviderBuilder::new().connect_client(rpc_client).erased();
        Ok(Self { provider })
    }

    /// Current chain head block number.
    pub async fn head_block(&self) -> Result<u64, LogFetchError> {
        self.provider
            .get_block_number()
            .await
            .map_err(|error| RpcError::CallFailed {
                method: "eth_blockNumber".into(),
                reason: error.to_string(),
            })
            .map_err(LogFetchError::from)
    }

    /// Fetch `OrderFilled` logs for one contract in an inclusive block range.
    pub async fn fetch_order_filled_logs(
        &self,
        contract: Address,
        topic: B256,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<FetchedLog>, LogFetchError> {
        let filter = Filter::new()
            .address(contract)
            .event_signature(topic)
            .from_block(from_block)
            .to_block(to_block);
        let logs = self
            .provider
            .get_logs(&filter)
            .await
            .map_err(|error| RpcError::CallFailed {
                method: "eth_getLogs".into(),
                reason: error.to_string(),
            })?;
        let mut out = Vec::with_capacity(logs.len());
        for log in logs {
            let block_number = log.block_number.ok_or(LogFetchError::MissingBlockNumber)?;
            let block = self
                .provider
                .get_block_by_number(BlockNumberOrTag::Number(block_number))
                .await
                .map_err(|error| RpcError::CallFailed {
                    method: "eth_getBlockByNumber".into(),
                    reason: error.to_string(),
                })?
                .ok_or(LogFetchError::MissingBlockTimestamp)?;
            let block_timestamp = block.header.timestamp;
            out.push(FetchedLog {
                log,
                block_number,
                block_timestamp,
            });
        }
        Ok(out)
    }
}
