//! Finalized Polygon exchange-history extraction and independent RPC attestation.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use alloy::{
    primitives::{Address, B256, Bytes, Log as PrimitiveLog, LogData},
    rpc::types::Log as RpcAlloyLog,
};
use blake3::Hasher;
use chrono::Utc;
use futures_util::{StreamExt, stream};
use hypersync_client::{
    Client as HyperSyncClient, ClientConfig as HyperSyncClientConfig,
    format::Quantity as HyperSyncQuantity,
    net_types::{LogFilter, Query, block::BlockField, log::LogField},
    simple_types::{Block as HyperSyncBlock, Log as HyperSyncLog},
};
use quant_pivot_models::config::FinalizedExchangeHistoryConfig;
use reqwest::Client as ReqwestClient;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::constants::EXCHANGE_CONTRACTS;

const CHAIN_ID: u64 = 137;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalBlockHeader {
    pub number: u64,
    pub hash: String,
    pub parent_hash: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalExchangeLog {
    pub address: String,
    pub block_number: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub model_available_timestamp: u64,
    pub parent_block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: u64,
    pub log_index: u64,
    pub topics: Vec<String>,
    pub data: String,
    pub removed: bool,
}

impl CanonicalExchangeLog {
    pub fn alloy_log(&self) -> Result<RpcAlloyLog, HistoryClientError> {
        let address =
            self.address
                .parse::<Address>()
                .map_err(|_| HistoryClientError::InvalidField {
                    provider: "accepted history",
                    field: "log.address",
                })?;
        let topics = self
            .topics
            .iter()
            .map(|topic| {
                topic
                    .parse::<B256>()
                    .map_err(|_| HistoryClientError::InvalidField {
                        provider: "accepted history",
                        field: "log.topic",
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let data = Bytes::from(decode_hex(&self.data, "log.data")?);
        let log_data = LogData::new(topics, data).ok_or(HistoryClientError::InvalidField {
            provider: "accepted history",
            field: "log.topics",
        })?;
        Ok(RpcAlloyLog {
            inner: PrimitiveLog {
                address,
                data: log_data,
            },
            block_hash: Some(self.block_hash.parse().map_err(|_| {
                HistoryClientError::InvalidField {
                    provider: "accepted history",
                    field: "log.block_hash",
                }
            })?),
            block_number: Some(self.block_number),
            block_timestamp: Some(self.block_timestamp),
            transaction_hash: Some(self.transaction_hash.parse().map_err(|_| {
                HistoryClientError::InvalidField {
                    provider: "accepted history",
                    field: "log.transaction_hash",
                }
            })?),
            transaction_index: Some(self.transaction_index),
            log_index: Some(self.log_index),
            removed: self.removed,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryDigest(pub [u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryContinuityProofBasis {
    /// `HyperSync` supplied its optional in-memory rollback guard.
    HyperSyncRollbackGuard,
    /// Archive-only response omitted the optional guard; selected boundary
    /// headers provide continuity and are independently attested by RPC.
    HyperSyncBoundaryHeaders,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryContinuityProof {
    pub basis: HistoryContinuityProofBasis,
    pub attested_block_number: u64,
    pub attested_block_hash: String,
    pub first_block_number: u64,
    pub first_parent_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedHistoryChunk {
    pub from_block: u64,
    pub to_block: u64,
    pub archive_height: u64,
    pub first_block: CanonicalBlockHeader,
    pub last_block: CanonicalBlockHeader,
    pub confirmation_anchor: CanonicalBlockHeader,
    pub logs: Vec<CanonicalExchangeLog>,
    pub digest: HistoryDigest,
    pub continuity_proof: HistoryContinuityProof,
    pub observed_at_millis: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttestedHistoryChunk {
    pub from_block: u64,
    pub to_block: u64,
    pub first_block: CanonicalBlockHeader,
    pub last_block: CanonicalBlockHeader,
    pub confirmation_anchor: CanonicalBlockHeader,
    pub logs: Vec<CanonicalExchangeLog>,
    pub digest: HistoryDigest,
    pub observed_at_millis: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveProbe {
    pub finalized_head: CanonicalBlockHeader,
    pub contract_code_hashes: BTreeMap<String, String>,
}

#[derive(Debug, thiserror::Error)]
pub enum HistoryClientError {
    #[error("invalid exchange-history client configuration: {0}")]
    InvalidConfig(String),
    #[error("{provider} request failed during {operation}")]
    Network {
        provider: &'static str,
        operation: &'static str,
    },
    #[error("archive RPC returned HTTP status {status} during {method}")]
    HttpStatus { method: &'static str, status: u16 },
    #[error("archive RPC rejected {method} with code {code}: {message}")]
    RpcRejected {
        method: &'static str,
        code: i64,
        message: String,
    },
    #[error("archive RPC response exceeded the configured {limit} byte budget")]
    ResponseBudget { limit: usize },
    #[error("{provider} omitted required {field}")]
    MissingField {
        provider: &'static str,
        field: &'static str,
    },
    #[error(
        "{provider} omitted required block {number}; returned {count} headers spanning {first:?}..={last:?}"
    )]
    MissingBoundaryBlock {
        provider: &'static str,
        number: u64,
        count: usize,
        first: Option<u64>,
        last: Option<u64>,
    },
    #[error("{provider} returned malformed {field}")]
    InvalidField {
        provider: &'static str,
        field: &'static str,
    },
    #[error("HyperSync archive height {archive_height} is below required block {required_block}")]
    ArchiveLag {
        archive_height: u64,
        required_block: u64,
    },
    #[error("HyperSync pagination did not advance from block {block}")]
    StalledPagination { block: u64 },
    #[error("contract {contract_key} bytecode attestation failed")]
    CodeMismatch { contract_key: &'static str },
    #[error("contract {contract_key} {boundary} block hash attestation failed")]
    ContractBoundaryMismatch {
        contract_key: &'static str,
        boundary: &'static str,
    },
    #[error("archive RPC block-by-hash attestation failed")]
    BlockHashMismatch,
    #[error("{provider} log block hash disagrees with header at block {number}")]
    LogBlockHashMismatch { provider: &'static str, number: u64 },
    #[error("{provider} header parent chain is broken at block {number}")]
    BrokenParentChain { provider: &'static str, number: u64 },
}

pub struct ExchangeHistoryExtractor {
    client: HyperSyncClient,
    confirmation_blocks: u64,
    max_response_bytes: usize,
}

impl ExchangeHistoryExtractor {
    pub fn connect(config: &FinalizedExchangeHistoryConfig) -> Result<Self, HistoryClientError> {
        let client = HyperSyncClient::new(HyperSyncClientConfig {
            url: config.hypersync.endpoint.clone(),
            api_token: config.hypersync.api_token.expose_secret().to_owned(),
            http_req_timeout_millis: config.request_timeout_ms,
            retry_backoff_ms: config.retry_initial_ms,
            retry_base_ms: config.retry_initial_ms,
            retry_ceiling_ms: config.retry_max_ms,
            ..HyperSyncClientConfig::default()
        })
        .map_err(|_| {
            HistoryClientError::InvalidConfig("HyperSync client rejected deploy values".to_owned())
        })?;
        Ok(Self {
            client,
            confirmation_blocks: config.model_confirmation_blocks,
            max_response_bytes: config.max_response_bytes,
        })
    }

    pub async fn fetch_chunk(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<ExtractedHistoryChunk, HistoryClientError> {
        if to_block < from_block {
            return Err(HistoryClientError::InvalidConfig(
                "chunk end precedes chunk start".to_owned(),
            ));
        }
        let confirmation_end = to_block
            .checked_add(self.confirmation_blocks)
            .ok_or_else(|| {
                HistoryClientError::InvalidConfig("confirmation range overflow".to_owned())
            })?;
        let log_end_exclusive = to_block
            .checked_add(1)
            .ok_or_else(|| HistoryClientError::InvalidConfig("query range overflow".to_owned()))?;
        let addresses = EXCHANGE_CONTRACTS
            .iter()
            .map(|contract| format!("{:#x}", contract.address))
            .collect::<Vec<_>>();
        let topics = EXCHANGE_CONTRACTS
            .iter()
            .flat_map(|contract| [contract.order_filled_topic, contract.orders_matched_topic])
            .map(|topic| format!("{topic:#x}"))
            .collect::<Vec<_>>();
        let log_filter = LogFilter::all()
            .and_address(addresses)
            .and_then(|filter| filter.and_topic0(topics))
            .map_err(|_| {
                HistoryClientError::InvalidConfig("exchange filter is invalid".to_owned())
            })?;
        let mut cursor = from_block;
        let mut archive_height = 0_u64;
        let mut logs = Vec::new();
        let mut continuity_proof = None;
        while cursor < log_end_exclusive {
            let query = Query::new()
                .from_block(cursor)
                .to_block_excl(log_end_exclusive)
                .where_logs(log_filter.clone())
                .select_log_fields([
                    LogField::Address,
                    LogField::BlockNumber,
                    LogField::BlockHash,
                    LogField::TransactionHash,
                    LogField::TransactionIndex,
                    LogField::LogIndex,
                    LogField::Topic0,
                    LogField::Topic1,
                    LogField::Topic2,
                    LogField::Topic3,
                    LogField::Data,
                    LogField::Removed,
                ]);
            let response =
                self.client
                    .get(&query)
                    .await
                    .map_err(|_| HistoryClientError::Network {
                        provider: "HyperSync",
                        operation: "query",
                    })?;
            let page_archive = response
                .archive_height
                .ok_or(HistoryClientError::MissingField {
                    provider: "HyperSync",
                    field: "archive_height",
                })?;
            archive_height = archive_height.max(page_archive);
            if response.next_block <= cursor {
                return Err(HistoryClientError::StalledPagination { block: cursor });
            }
            for log in response.data.logs.into_iter().flatten() {
                logs.push(hypersync_log(log)?);
            }
            if continuity_proof.is_none()
                && let Some(guard) = response.rollback_guard
                && guard.first_block_number == from_block
                && guard.block_number >= to_block
            {
                continuity_proof = Some(HistoryContinuityProof {
                    basis: HistoryContinuityProofBasis::HyperSyncRollbackGuard,
                    attested_block_number: guard.block_number,
                    attested_block_hash: guard.hash.to_string(),
                    first_block_number: guard.first_block_number,
                    first_parent_hash: guard.first_parent_hash.to_string(),
                });
            }
            cursor = response.next_block.min(log_end_exclusive);
        }
        let (header_archive_height, header_blocks, header_proof) = self
            .fetch_headers(from_block, confirmation_end, to_block)
            .await?;
        archive_height = archive_height.max(header_archive_height);
        let blocks = header_blocks;
        validate_header_chain(&blocks, from_block, confirmation_end, "HyperSync")?;
        if continuity_proof.is_none() {
            continuity_proof = header_proof;
        }
        if archive_height < confirmation_end {
            return Err(HistoryClientError::ArchiveLag {
                archive_height,
                required_block: confirmation_end,
            });
        }
        hydrate_log_times(&mut logs, &blocks, self.confirmation_blocks, "HyperSync")?;
        canonical_sort(&mut logs);
        enforce_budget(&logs, self.max_response_bytes)?;
        let first_block = required_block(&blocks, from_block, "HyperSync")?.clone();
        let last_block = required_block(&blocks, to_block, "HyperSync")?.clone();
        let confirmation_anchor = required_block(&blocks, confirmation_end, "HyperSync")?.clone();
        let continuity_proof = continuity_proof.unwrap_or_else(|| HistoryContinuityProof {
            basis: HistoryContinuityProofBasis::HyperSyncBoundaryHeaders,
            attested_block_number: last_block.number,
            attested_block_hash: last_block.hash.clone(),
            first_block_number: first_block.number,
            first_parent_hash: first_block.parent_hash.clone(),
        });
        Ok(ExtractedHistoryChunk {
            from_block,
            to_block,
            archive_height,
            first_block,
            last_block,
            confirmation_anchor,
            digest: canonical_digest(&logs),
            logs,
            continuity_proof,
            observed_at_millis: Utc::now().timestamp_millis(),
        })
    }

    async fn fetch_headers(
        &self,
        from_block: u64,
        to_block: u64,
        proof_through_block: u64,
    ) -> Result<
        (
            u64,
            BTreeMap<u64, CanonicalBlockHeader>,
            Option<HistoryContinuityProof>,
        ),
        HistoryClientError,
    > {
        let end_exclusive = to_block
            .checked_add(1)
            .ok_or_else(|| HistoryClientError::InvalidConfig("header range overflow".to_owned()))?;
        let mut cursor = from_block;
        let mut archive_height = 0_u64;
        let mut blocks = BTreeMap::new();
        let mut proof = None;
        while cursor < end_exclusive {
            let query = Query::new()
                .from_block(cursor)
                .to_block_excl(end_exclusive)
                .include_all_blocks()
                .select_block_fields([
                    BlockField::Number,
                    BlockField::Hash,
                    BlockField::ParentHash,
                    BlockField::Timestamp,
                ]);
            let response =
                self.client
                    .get(&query)
                    .await
                    .map_err(|_| HistoryClientError::Network {
                        provider: "HyperSync",
                        operation: "block header query",
                    })?;
            let page_archive = response
                .archive_height
                .ok_or(HistoryClientError::MissingField {
                    provider: "HyperSync",
                    field: "archive_height",
                })?;
            archive_height = archive_height.max(page_archive);
            if response.next_block <= cursor {
                return Err(HistoryClientError::StalledPagination { block: cursor });
            }
            for block in response.data.blocks.into_iter().flatten() {
                let header = hypersync_block(block)?;
                blocks.insert(header.number, header);
            }
            if proof.is_none()
                && let Some(guard) = response.rollback_guard
                && guard.first_block_number == from_block
                && guard.block_number >= proof_through_block
            {
                proof = Some(HistoryContinuityProof {
                    basis: HistoryContinuityProofBasis::HyperSyncRollbackGuard,
                    attested_block_number: guard.block_number,
                    attested_block_hash: guard.hash.to_string(),
                    first_block_number: guard.first_block_number,
                    first_parent_hash: guard.first_parent_hash.to_string(),
                });
            }
            cursor = response.next_block.min(end_exclusive);
        }
        Ok((archive_height, blocks, proof))
    }
}

pub struct ExchangeHistoryAttestor {
    client: ReqwestClient,
    endpoint: String,
    max_response_bytes: usize,
    max_blocks_per_log_request: u64,
    max_concurrent_log_requests: usize,
    confirmation_blocks: u64,
}

impl ExchangeHistoryAttestor {
    pub fn connect(config: &FinalizedExchangeHistoryConfig) -> Result<Self, HistoryClientError> {
        let client = ReqwestClient::builder()
            .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .map_err(|_| {
                HistoryClientError::InvalidConfig(
                    "archive RPC client rejected deploy values".to_owned(),
                )
            })?;
        Ok(Self {
            client,
            endpoint: config.attestor.rpc_url().to_owned(),
            max_response_bytes: config.max_response_bytes,
            max_blocks_per_log_request: config.attestor.max_blocks_per_log_request,
            max_concurrent_log_requests: config.attestor.max_concurrent_log_requests,
            confirmation_blocks: config.model_confirmation_blocks,
        })
    }

    pub async fn probe_archive(&self) -> Result<ArchiveProbe, HistoryClientError> {
        let finalized = self.block_by_tag("finalized").await?;
        let by_hash = self.block_by_hash(&finalized.hash).await?;
        if by_hash != finalized {
            return Err(HistoryClientError::BlockHashMismatch);
        }
        let mut hashes = BTreeMap::new();
        for contract in EXCHANGE_CONTRACTS {
            let first = self.block_by_number(contract.first_valid_block).await?;
            if first.hash != contract.first_valid_block_hash {
                return Err(HistoryClientError::ContractBoundaryMismatch {
                    contract_key: contract.key,
                    boundary: "first_valid",
                });
            }
            if let (Some(last_valid_block), Some(last_valid_block_hash)) =
                (contract.last_valid_block, contract.last_valid_block_hash)
            {
                let last = self.block_by_number(last_valid_block).await?;
                if last.hash != last_valid_block_hash {
                    return Err(HistoryClientError::ContractBoundaryMismatch {
                        contract_key: contract.key,
                        boundary: "last_valid",
                    });
                }
            }
            let block = format!("0x{:x}", contract.first_valid_block);
            let code: String = self
                .call_rpc(
                    "eth_getCode",
                    serde_json::json!([format!("{:#x}", contract.address), block]),
                )
                .await?;
            let code_bytes = decode_hex(&code, "contract bytecode")?;
            let code_hash = blake3::hash(&code_bytes).to_hex().to_string();
            if code_hash != contract.bytecode_blake3 {
                return Err(HistoryClientError::CodeMismatch {
                    contract_key: contract.key,
                });
            }
            hashes.insert(contract.key.to_owned(), code_hash);
            let probe_end = contract.first_valid_block.saturating_add(32);
            let _ = self
                .fetch_logs(contract.first_valid_block, probe_end)
                .await?;
        }
        Ok(ArchiveProbe {
            finalized_head: finalized,
            contract_code_hashes: hashes,
        })
    }

    /// Return the canonical Polygon finalized head used to bound every model
    /// frontier. The worker additionally subtracts its explicit confirmation
    /// policy before choosing a chunk end.
    pub async fn finalized_head(&self) -> Result<CanonicalBlockHeader, HistoryClientError> {
        self.block_by_tag("finalized").await
    }

    /// Read one canonical header for continuity and rollback-buffer checks.
    pub async fn block_header(
        &self,
        block_number: u64,
    ) -> Result<CanonicalBlockHeader, HistoryClientError> {
        self.block_by_number(block_number).await
    }

    /// Locate the first canonical block whose timestamp is at or after the
    /// requested Unix second using block-header binary search.
    pub async fn block_at_or_after(
        &self,
        timestamp: u64,
        upper_block: u64,
    ) -> Result<CanonicalBlockHeader, HistoryClientError> {
        let mut lower = 0_u64;
        let mut upper = upper_block;
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let header = self.block_by_number(middle).await?;
            if header.timestamp < timestamp {
                lower = middle.saturating_add(1);
            } else {
                upper = middle;
            }
        }
        self.block_by_number(lower).await
    }

    pub async fn fetch_chunk(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<AttestedHistoryChunk, HistoryClientError> {
        let mut logs = self.fetch_logs(from_block, to_block).await?;
        let confirmation_anchor_number = to_block
            .checked_add(self.confirmation_blocks)
            .ok_or_else(|| {
                HistoryClientError::InvalidConfig("attestor confirmation range overflow".to_owned())
            })?;
        let mut required_blocks =
            BTreeSet::from([from_block, to_block, confirmation_anchor_number]);
        for log in &logs {
            required_blocks.insert(log.block_number);
            required_blocks.insert(
                log.block_number
                    .checked_add(self.confirmation_blocks)
                    .ok_or_else(|| {
                        HistoryClientError::InvalidConfig(
                            "attestor confirmation range overflow".to_owned(),
                        )
                    })?,
            );
        }
        let headers = stream::iter(required_blocks)
            .map(
                |number| async move { self.block_by_number(number).await.map(|row| (number, row)) },
            )
            .buffer_unordered(self.max_concurrent_log_requests)
            .collect::<Vec<_>>()
            .await;
        let mut blocks = BTreeMap::new();
        for header in headers {
            let (number, header) = header?;
            blocks.insert(number, header);
        }
        hydrate_log_times(&mut logs, &blocks, self.confirmation_blocks, "archive RPC")?;
        canonical_sort(&mut logs);
        let first_block = required_block(&blocks, from_block, "archive RPC")?.clone();
        let last_block = required_block(&blocks, to_block, "archive RPC")?.clone();
        let confirmation_anchor =
            required_block(&blocks, confirmation_anchor_number, "archive RPC")?.clone();
        Ok(AttestedHistoryChunk {
            from_block,
            to_block,
            first_block,
            last_block,
            confirmation_anchor,
            digest: canonical_digest(&logs),
            logs,
            observed_at_millis: Utc::now().timestamp_millis(),
        })
    }

    /// Independently attest the exact `HyperSync` continuity-proof block hash.
    pub async fn verify_continuity(
        &self,
        proof: &HistoryContinuityProof,
    ) -> Result<bool, HistoryClientError> {
        let header = self.block_by_number(proof.attested_block_number).await?;
        Ok(header.hash == proof.attested_block_hash)
    }

    async fn fetch_logs(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<CanonicalExchangeLog>, HistoryClientError> {
        if to_block < from_block {
            return Err(HistoryClientError::InvalidConfig(
                "log range end precedes its start".to_owned(),
            ));
        }
        if self.max_blocks_per_log_request == 0 || self.max_concurrent_log_requests == 0 {
            return Err(HistoryClientError::InvalidConfig(
                "attestor log request budgets must be positive".to_owned(),
            ));
        }
        let mut ranges = Vec::new();
        let mut cursor = from_block;
        loop {
            let end = cursor
                .saturating_add(self.max_blocks_per_log_request.saturating_sub(1))
                .min(to_block);
            ranges.push((cursor, end));
            if end == to_block {
                break;
            }
            cursor = end.checked_add(1).ok_or_else(|| {
                HistoryClientError::InvalidConfig("log range overflow".to_owned())
            })?;
        }
        let pages = stream::iter(ranges)
            .map(|(start, end)| self.fetch_log_range(start, end))
            .buffer_unordered(self.max_concurrent_log_requests)
            .collect::<Vec<_>>()
            .await;
        let mut logs = Vec::new();
        for page in pages {
            logs.extend(page?);
        }
        enforce_budget(&logs, self.max_response_bytes)?;
        Ok(logs)
    }

    async fn fetch_log_range(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<CanonicalExchangeLog>, HistoryClientError> {
        let addresses = EXCHANGE_CONTRACTS
            .iter()
            .map(|contract| format!("{:#x}", contract.address))
            .collect::<Vec<_>>();
        let topics = EXCHANGE_CONTRACTS
            .iter()
            .flat_map(|contract| [contract.order_filled_topic, contract.orders_matched_topic])
            .map(|topic| format!("{topic:#x}"))
            .collect::<Vec<_>>();
        let filter = RpcLogFilter {
            from_block: format!("0x{from_block:x}"),
            to_block: format!("0x{to_block:x}"),
            address: addresses,
            topics: vec![topics],
        };
        let rows: Vec<RpcLog> = self
            .call_rpc("eth_getLogs", serde_json::json!([filter]))
            .await?;
        rows.into_iter()
            .map(CanonicalExchangeLog::try_from)
            .collect()
    }

    async fn block_by_number(
        &self,
        block_number: u64,
    ) -> Result<CanonicalBlockHeader, HistoryClientError> {
        self.block_by_tag(&format!("0x{block_number:x}")).await
    }

    async fn block_by_tag(&self, block: &str) -> Result<CanonicalBlockHeader, HistoryClientError> {
        let row: Option<RpcBlock> = self
            .call_rpc("eth_getBlockByNumber", serde_json::json!([block, false]))
            .await?;
        row.ok_or(HistoryClientError::MissingField {
            provider: "archive RPC",
            field: "block",
        })?
        .try_into()
    }

    async fn block_by_hash(
        &self,
        block_hash: &str,
    ) -> Result<CanonicalBlockHeader, HistoryClientError> {
        let row: Option<RpcBlock> = self
            .call_rpc("eth_getBlockByHash", serde_json::json!([block_hash, false]))
            .await?;
        row.ok_or(HistoryClientError::MissingField {
            provider: "archive RPC",
            field: "block_by_hash",
        })?
        .try_into()
    }

    async fn call_rpc<T: DeserializeOwned>(
        &self,
        method: &'static str,
        params: Value,
    ) -> Result<T, HistoryClientError> {
        let request = RpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method,
            params,
        };
        let response = self
            .client
            .post(&self.endpoint)
            .json(&request)
            .send()
            .await
            .map_err(|_| HistoryClientError::Network {
                provider: "archive RPC",
                operation: method,
            })?;
        let status = response.status();
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| HistoryClientError::Network {
                provider: "archive RPC",
                operation: method,
            })?;
            if body.len().saturating_add(chunk.len()) > self.max_response_bytes {
                return Err(HistoryClientError::ResponseBudget {
                    limit: self.max_response_bytes,
                });
            }
            body.extend_from_slice(&chunk);
        }
        let parsed = serde_json::from_slice::<RpcResponse<T>>(&body);
        if !status.is_success() {
            if let Ok(envelope) = parsed
                && let Some(error) = envelope.error
            {
                return Err(HistoryClientError::RpcRejected {
                    method,
                    code: error.code,
                    message: error.message.chars().take(256).collect(),
                });
            }
            return Err(HistoryClientError::HttpStatus {
                method,
                status: status.as_u16(),
            });
        }
        let envelope = parsed.map_err(|_| HistoryClientError::InvalidField {
            provider: "archive RPC",
            field: "JSON-RPC envelope",
        })?;
        if let Some(error) = envelope.error {
            return Err(HistoryClientError::RpcRejected {
                method,
                code: error.code,
                message: error.message.chars().take(256).collect(),
            });
        }
        envelope.result.ok_or(HistoryClientError::MissingField {
            provider: "archive RPC",
            field: "result",
        })
    }
}

pub fn chunks_agree(extracted: &ExtractedHistoryChunk, attested: &AttestedHistoryChunk) -> bool {
    extracted.from_block == attested.from_block
        && extracted.to_block == attested.to_block
        && extracted.logs.len() == attested.logs.len()
        && extracted.digest == attested.digest
        && extracted.first_block == attested.first_block
        && extracted.last_block == attested.last_block
        && extracted.confirmation_anchor == attested.confirmation_anchor
        && extracted.continuity_proof.first_block_number == extracted.from_block
        && extracted.continuity_proof.first_parent_hash == extracted.first_block.parent_hash
        && extracted.continuity_proof.attested_block_number >= extracted.to_block
}

pub fn canonical_digest(logs: &[CanonicalExchangeLog]) -> HistoryDigest {
    let mut hasher = Hasher::new();
    hasher.update(b"quant-pivot/exchange-history-log-set/v2\0");
    for log in logs {
        hash_field(&mut hasher, log.address.as_bytes());
        hasher.update(&log.block_number.to_be_bytes());
        hasher.update(&log.transaction_index.to_be_bytes());
        hasher.update(&log.log_index.to_be_bytes());
        hash_field(&mut hasher, log.block_hash.as_bytes());
        hasher.update(&log.block_timestamp.to_be_bytes());
        hasher.update(&log.model_available_timestamp.to_be_bytes());
        hash_field(&mut hasher, log.parent_block_hash.as_bytes());
        hash_field(&mut hasher, log.transaction_hash.as_bytes());
        for topic in &log.topics {
            hash_field(&mut hasher, topic.as_bytes());
        }
        hash_field(&mut hasher, log.data.as_bytes());
        hasher.update(&[u8::from(log.removed)]);
    }
    HistoryDigest(*hasher.finalize().as_bytes())
}

fn hash_field(hasher: &mut Hasher, value: &[u8]) {
    hasher.update(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

fn canonical_sort(logs: &mut [CanonicalExchangeLog]) {
    logs.sort_by(|left, right| {
        (
            left.block_number,
            left.transaction_index,
            left.log_index,
            &left.block_hash,
            &left.transaction_hash,
        )
            .cmp(&(
                right.block_number,
                right.transaction_index,
                right.log_index,
                &right.block_hash,
                &right.transaction_hash,
            ))
    });
}

fn enforce_budget(
    logs: &[CanonicalExchangeLog],
    max_response_bytes: usize,
) -> Result<(), HistoryClientError> {
    let estimated = logs.iter().fold(0_usize, |total, log| {
        total
            .saturating_add(log.address.len())
            .saturating_add(log.block_hash.len())
            .saturating_add(log.transaction_hash.len())
            .saturating_add(log.data.len())
            .saturating_add(log.topics.iter().map(String::len).sum::<usize>())
            .saturating_add(64)
    });
    if estimated > max_response_bytes {
        Err(HistoryClientError::ResponseBudget {
            limit: max_response_bytes,
        })
    } else {
        Ok(())
    }
}

fn required_block<'a>(
    blocks: &'a BTreeMap<u64, CanonicalBlockHeader>,
    number: u64,
    provider: &'static str,
) -> Result<&'a CanonicalBlockHeader, HistoryClientError> {
    blocks
        .get(&number)
        .ok_or_else(|| HistoryClientError::MissingBoundaryBlock {
            provider,
            number,
            count: blocks.len(),
            first: blocks.first_key_value().map(|(block, _)| *block),
            last: blocks.last_key_value().map(|(block, _)| *block),
        })
}

fn hydrate_log_times(
    logs: &mut [CanonicalExchangeLog],
    blocks: &BTreeMap<u64, CanonicalBlockHeader>,
    confirmation_blocks: u64,
    provider: &'static str,
) -> Result<(), HistoryClientError> {
    for log in logs {
        let block = required_block(blocks, log.block_number, provider)?;
        if log.block_hash != block.hash {
            return Err(HistoryClientError::LogBlockHashMismatch {
                provider,
                number: log.block_number,
            });
        }
        log.block_timestamp = block.timestamp;
        log.parent_block_hash.clone_from(&block.parent_hash);
        let confirmation_number = log.block_number.checked_add(confirmation_blocks).ok_or(
            HistoryClientError::InvalidField {
                provider,
                field: "model confirmation block",
            },
        )?;
        log.model_available_timestamp =
            required_block(blocks, confirmation_number, provider)?.timestamp;
    }
    Ok(())
}

fn validate_header_chain(
    blocks: &BTreeMap<u64, CanonicalBlockHeader>,
    from_block: u64,
    through_block: u64,
    provider: &'static str,
) -> Result<(), HistoryClientError> {
    let mut number = from_block;
    let mut previous = required_block(blocks, number, provider)?;
    while number < through_block {
        number = number
            .checked_add(1)
            .ok_or(HistoryClientError::InvalidField {
                provider,
                field: "header chain range",
            })?;
        let current = required_block(blocks, number, provider)?;
        if current.parent_hash != previous.hash {
            return Err(HistoryClientError::BrokenParentChain { provider, number });
        }
        previous = current;
    }
    Ok(())
}

fn hypersync_block(block: HyperSyncBlock) -> Result<CanonicalBlockHeader, HistoryClientError> {
    let timestamp = quantity_u64(
        block
            .timestamp
            .as_ref()
            .ok_or(HistoryClientError::MissingField {
                provider: "HyperSync",
                field: "block.timestamp",
            })?,
        "block.timestamp",
    )?;
    Ok(CanonicalBlockHeader {
        number: block.number.ok_or(HistoryClientError::MissingField {
            provider: "HyperSync",
            field: "block.number",
        })?,
        hash: block
            .hash
            .ok_or(HistoryClientError::MissingField {
                provider: "HyperSync",
                field: "block.hash",
            })?
            .to_string(),
        parent_hash: block
            .parent_hash
            .ok_or(HistoryClientError::MissingField {
                provider: "HyperSync",
                field: "block.parent_hash",
            })?
            .to_string(),
        timestamp,
    })
}

fn hypersync_log(log: HyperSyncLog) -> Result<CanonicalExchangeLog, HistoryClientError> {
    let topics = log
        .topics
        .into_iter()
        .flatten()
        .map(|topic| topic.to_string())
        .collect::<Vec<_>>();
    Ok(CanonicalExchangeLog {
        address: log
            .address
            .ok_or(HistoryClientError::MissingField {
                provider: "HyperSync",
                field: "log.address",
            })?
            .to_string(),
        block_number: log
            .block_number
            .map(Into::into)
            .ok_or(HistoryClientError::MissingField {
                provider: "HyperSync",
                field: "log.block_number",
            })?,
        block_hash: log
            .block_hash
            .ok_or(HistoryClientError::MissingField {
                provider: "HyperSync",
                field: "log.block_hash",
            })?
            .to_string(),
        block_timestamp: 0,
        model_available_timestamp: 0,
        parent_block_hash: String::new(),
        transaction_hash: log
            .transaction_hash
            .ok_or(HistoryClientError::MissingField {
                provider: "HyperSync",
                field: "log.transaction_hash",
            })?
            .to_string(),
        transaction_index: log.transaction_index.map(Into::into).ok_or(
            HistoryClientError::MissingField {
                provider: "HyperSync",
                field: "log.transaction_index",
            },
        )?,
        log_index: log
            .log_index
            .map(Into::into)
            .ok_or(HistoryClientError::MissingField {
                provider: "HyperSync",
                field: "log.log_index",
            })?,
        topics,
        data: log
            .data
            .map(|data| format!("0x{}", hex::encode(data.as_ref())))
            .ok_or(HistoryClientError::MissingField {
                provider: "HyperSync",
                field: "log.data",
            })?,
        removed: log.removed.unwrap_or(false),
    })
}

fn quantity_u64(value: &HyperSyncQuantity, field: &'static str) -> Result<u64, HistoryClientError> {
    value.as_ref().iter().try_fold(0_u64, |total, byte| {
        total
            .checked_mul(256)
            .and_then(|value| value.checked_add(u64::from(*byte)))
            .ok_or(HistoryClientError::InvalidField {
                provider: "HyperSync",
                field,
            })
    })
}

fn decode_hex(value: &str, field: &'static str) -> Result<Vec<u8>, HistoryClientError> {
    hex::decode(value.strip_prefix("0x").unwrap_or(value)).map_err(|_| {
        HistoryClientError::InvalidField {
            provider: "archive RPC",
            field,
        }
    })
}

fn parse_hex_u64(value: &str, field: &'static str) -> Result<u64, HistoryClientError> {
    u64::from_str_radix(value.strip_prefix("0x").unwrap_or(value), 16).map_err(|_| {
        HistoryClientError::InvalidField {
            provider: "archive RPC",
            field,
        }
    })
}

#[derive(Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    params: Value,
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcErrorPayload>,
}

#[derive(Deserialize)]
struct RpcErrorPayload {
    code: i64,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct RpcLogFilter {
    from_block: String,
    to_block: String,
    address: Vec<String>,
    topics: Vec<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcBlock {
    number: String,
    hash: String,
    parent_hash: String,
    timestamp: String,
}

impl TryFrom<RpcBlock> for CanonicalBlockHeader {
    type Error = HistoryClientError;

    fn try_from(value: RpcBlock) -> Result<Self, Self::Error> {
        Ok(Self {
            number: parse_hex_u64(&value.number, "block.number")?,
            hash: value.hash.to_ascii_lowercase(),
            parent_hash: value.parent_hash.to_ascii_lowercase(),
            timestamp: parse_hex_u64(&value.timestamp, "block.timestamp")?,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcLog {
    address: String,
    block_number: String,
    block_hash: String,
    transaction_hash: String,
    transaction_index: String,
    log_index: String,
    topics: Vec<String>,
    data: String,
    removed: Option<bool>,
}

impl TryFrom<RpcLog> for CanonicalExchangeLog {
    type Error = HistoryClientError;

    fn try_from(value: RpcLog) -> Result<Self, Self::Error> {
        Ok(Self {
            address: value.address.to_ascii_lowercase(),
            block_number: parse_hex_u64(&value.block_number, "log.block_number")?,
            block_hash: value.block_hash.to_ascii_lowercase(),
            block_timestamp: 0,
            model_available_timestamp: 0,
            parent_block_hash: String::new(),
            transaction_hash: value.transaction_hash.to_ascii_lowercase(),
            transaction_index: parse_hex_u64(&value.transaction_index, "log.transaction_index")?,
            log_index: parse_hex_u64(&value.log_index, "log.log_index")?,
            topics: value
                .topics
                .into_iter()
                .map(|topic| topic.to_ascii_lowercase())
                .collect(),
            data: value.data.to_ascii_lowercase(),
            removed: value.removed.unwrap_or(false),
        })
    }
}

#[must_use]
pub const fn polygon_chain_id() -> u64 {
    CHAIN_ID
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use reqwest::{Client as ReqwestClient, Error as ReqwestError};
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_partial_json, method},
    };

    use super::{
        AttestedHistoryChunk, CanonicalBlockHeader, ExchangeHistoryAttestor, ExtractedHistoryChunk,
        HistoryClientError, HistoryContinuityProof, HistoryContinuityProofBasis, HistoryDigest,
        chunks_agree,
    };

    fn attestor_for(
        server: &MockServer,
        max_response_bytes: usize,
        max_blocks_per_log_request: u64,
    ) -> Result<ExchangeHistoryAttestor, ReqwestError> {
        Ok(ExchangeHistoryAttestor {
            client: ReqwestClient::builder().build()?,
            endpoint: server.uri(),
            max_response_bytes,
            max_blocks_per_log_request,
            max_concurrent_log_requests: 2,
            confirmation_blocks: 12,
        })
    }

    fn header(number: u64, hash: &str) -> CanonicalBlockHeader {
        CanonicalBlockHeader {
            number,
            hash: hash.to_owned(),
            parent_hash: format!("0x{:064x}", number.saturating_sub(1)),
            timestamp: 1_700_000_000 + number,
        }
    }

    #[tokio::test]
    async fn pagination_is_bounded() -> Result<(), Box<dyn Error>> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(json!({ "method": "eth_getLogs" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": []
            })))
            .expect(3)
            .mount(&server)
            .await;
        let attestor = attestor_for(&server, 16_384, 2)?;

        let logs = attestor.fetch_logs(10, 14).await?;

        assert!(logs.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn finalized_header_keeps_time() -> Result<(), Box<dyn Error>> {
        let server = MockServer::start().await;
        let hash = format!("0x{:064x}", 42);
        Mock::given(method("POST"))
            .and(body_partial_json(json!({
                "method": "eth_getBlockByNumber",
                "params": ["finalized", false]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "number": "0x2a",
                    "hash": hash,
                    "parentHash": format!("0x{:064x}", 41),
                    "timestamp": "0x6553f100"
                }
            })))
            .mount(&server)
            .await;
        let attestor = attestor_for(&server, 16_384, 100)?;

        let finalized = attestor.finalized_head().await?;

        assert_eq!(finalized.number, 42);
        assert_eq!(finalized.timestamp, 1_700_000_000);
        Ok(())
    }

    #[tokio::test]
    async fn rpc_failures_are_typed() -> Result<(), Box<dyn Error>> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": { "code": -32005, "message": "rate limit" }
            })))
            .mount(&server)
            .await;
        let attestor = attestor_for(&server, 16_384, 100)?;

        let error = attestor.finalized_head().await.err();

        assert!(matches!(
            error,
            Some(HistoryClientError::RpcRejected { code: -32005, .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn response_budget_is_enforced() -> Result<(), Box<dyn Error>> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string("x".repeat(128)))
            .mount(&server)
            .await;
        let attestor = attestor_for(&server, 32, 100)?;

        let error = attestor.finalized_head().await.err();

        assert!(matches!(
            error,
            Some(HistoryClientError::ResponseBudget { limit: 32 })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn continuity_detects_rollback() -> Result<(), Box<dyn Error>> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "number": "0x2a",
                    "hash": format!("0x{:064x}", 42),
                    "parentHash": format!("0x{:064x}", 41),
                    "timestamp": "0x6553f100"
                }
            })))
            .mount(&server)
            .await;
        let attestor = attestor_for(&server, 16_384, 100)?;
        let proof = HistoryContinuityProof {
            basis: HistoryContinuityProofBasis::HyperSyncBoundaryHeaders,
            attested_block_number: 42,
            attested_block_hash: format!("0x{:064x}", 99),
            first_block_number: 43,
            first_parent_hash: format!("0x{:064x}", 42),
        };

        assert!(!attestor.verify_continuity(&proof).await?);
        Ok(())
    }

    #[test]
    fn divergence_is_detected() {
        let boundary = header(42, &format!("0x{:064x}", 42));
        let confirmation_anchor = header(54, &format!("0x{:064x}", 54));
        let extracted = ExtractedHistoryChunk {
            from_block: 42,
            to_block: 42,
            archive_height: 54,
            first_block: boundary.clone(),
            last_block: boundary.clone(),
            confirmation_anchor: confirmation_anchor.clone(),
            logs: Vec::new(),
            digest: HistoryDigest([1; 32]),
            continuity_proof: HistoryContinuityProof {
                basis: HistoryContinuityProofBasis::HyperSyncBoundaryHeaders,
                attested_block_number: 41,
                attested_block_hash: format!("0x{:064x}", 41),
                first_block_number: 42,
                first_parent_hash: format!("0x{:064x}", 41),
            },
            observed_at_millis: 1,
        };
        let attested = AttestedHistoryChunk {
            from_block: 42,
            to_block: 42,
            first_block: boundary.clone(),
            last_block: boundary,
            confirmation_anchor,
            logs: Vec::new(),
            digest: HistoryDigest([2; 32]),
            observed_at_millis: 2,
        };

        assert!(!chunks_agree(&extracted, &attested));
    }
}
