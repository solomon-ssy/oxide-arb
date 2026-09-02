//! Finalized Polygon exchange-history extraction and independent RPC attestation.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    io::{Error as IoError, Result as IoResult, Write},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use alloy::{
    primitives::{Address, B256, Bytes, Log as PrimitiveLog, LogData},
    rpc::types::Log as RpcAlloyLog,
};
use blake3::Hasher;
use chrono::Utc;
use futures_util::{StreamExt, stream};
use hypersync_format::{
    Address as HyperSyncAddress, BlockNumber as HyperSyncBlockNumber, Data as HyperSyncData,
    Hash as HyperSyncHash, LogArgument as HyperSyncLogArgument, LogIndex as HyperSyncLogIndex,
    Quantity as HyperSyncQuantity, TransactionIndex as HyperSyncTransactionIndex,
};
use hypersync_net_types::{JoinMode, LogFilter, Query, block::BlockField, log::LogField};
use quant_pivot_models::config::{FinalizedExchangeHistoryConfig, secret::SecretText};
use reqwest::{Client as ReqwestClient, StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::{
    runtime::{Builder as RuntimeBuilder, Runtime},
    sync::Semaphore,
};

use super::constants::EXCHANGE_CONTRACTS;

const CHAIN_ID: u64 = 137;
// HyperSync JSON decoding is synchronous CPU work. Keep it and its network
// reactor off the three application workers. One request may be in flight;
// streamed body and canonical-chunk budgets bound accepted bytes, while one
// async worker and at most three blocking replacements bound thread ownership.
const HYPERSYNC_RUNTIME_THREADS: usize = 1;
const HYPERSYNC_BLOCKING_THREADS: usize = 3;
const HYPERSYNC_REQUESTS: usize = 1;

#[derive(Debug, Deserialize)]
struct HyperSyncBlock {
    number: Option<u64>,
    hash: Option<HyperSyncHash>,
    parent_hash: Option<HyperSyncHash>,
    timestamp: Option<HyperSyncQuantity>,
}

#[derive(Debug, Deserialize)]
struct HyperSyncLog {
    removed: Option<bool>,
    log_index: Option<HyperSyncLogIndex>,
    transaction_index: Option<HyperSyncTransactionIndex>,
    transaction_hash: Option<HyperSyncHash>,
    block_hash: Option<HyperSyncHash>,
    block_number: Option<HyperSyncBlockNumber>,
    address: Option<HyperSyncAddress>,
    data: Option<HyperSyncData>,
    topic0: Option<HyperSyncLogArgument>,
    topic1: Option<HyperSyncLogArgument>,
    topic2: Option<HyperSyncLogArgument>,
    topic3: Option<HyperSyncLogArgument>,
}

#[derive(Debug, Deserialize)]
struct HyperSyncJsonData {
    #[serde(default)]
    blocks: Vec<HyperSyncBlock>,
    #[serde(default)]
    logs: Vec<HyperSyncLog>,
}

#[derive(Debug, Deserialize)]
struct HyperSyncRollbackGuard {
    block_number: u64,
    #[serde(rename = "timestamp")]
    _timestamp: i64,
    hash: String,
    first_block_number: u64,
    first_parent_hash: String,
}

#[derive(Debug, Deserialize)]
struct HyperSyncJsonResponse {
    archive_height: Option<u64>,
    next_block: u64,
    data: HyperSyncJsonData,
    rollback_guard: Option<HyperSyncRollbackGuard>,
}

struct HyperSyncJsonClient {
    client: ReqwestClient,
    endpoint: Url,
    api_token: SecretText,
    max_response_body_bytes: usize,
}

impl HyperSyncJsonClient {
    fn connect(config: &FinalizedExchangeHistoryConfig) -> Result<Self, HistoryClientError> {
        let mut endpoint = Url::parse(&config.hypersync.endpoint).map_err(|_| {
            HistoryClientError::InvalidConfig("HyperSync endpoint is invalid".to_owned())
        })?;
        endpoint
            .path_segments_mut()
            .map_err(|()| {
                HistoryClientError::InvalidConfig(
                    "HyperSync endpoint cannot own a query path".to_owned(),
                )
            })?
            .pop_if_empty()
            .push("query");
        let client = ReqwestClient::builder()
            .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .no_gzip()
            .build()
            .map_err(|_| {
                HistoryClientError::InvalidConfig(
                    "HyperSync HTTP client rejected deploy values".to_owned(),
                )
            })?;
        Ok(Self {
            client,
            endpoint,
            api_token: config.hypersync.api_token.clone(),
            max_response_body_bytes: config.max_hypersync_response_body_bytes,
        })
    }

    async fn query(
        self: Arc<Self>,
        query: Query,
    ) -> Result<HyperSyncJsonResponse, HistoryClientError> {
        let response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(self.api_token.expose_secret())
            .json(&query)
            .send()
            .await
            .map_err(|_| HistoryClientError::Network {
                provider: "HyperSync",
                operation: "query",
            })?;
        let status = response.status();
        if status == StatusCode::PAYLOAD_TOO_LARGE {
            return Err(HistoryClientError::HyperSyncPayloadTooLarge);
        }
        if !status.is_success() {
            return Err(HistoryClientError::HyperSyncHttpStatus {
                status: status.as_u16(),
            });
        }
        let declared_limit = u64::try_from(self.max_response_body_bytes).unwrap_or(u64::MAX);
        if response
            .content_length()
            .is_some_and(|length| length > declared_limit)
        {
            return Err(HistoryClientError::HyperSyncResponseBodyBudget {
                limit: self.max_response_body_bytes,
            });
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| HistoryClientError::Network {
                provider: "HyperSync",
                operation: "response body",
            })?;
            let next_length = body.len().checked_add(chunk.len()).ok_or(
                HistoryClientError::HyperSyncResponseBodyBudget {
                    limit: self.max_response_body_bytes,
                },
            )?;
            if next_length > self.max_response_body_bytes {
                return Err(HistoryClientError::HyperSyncResponseBodyBudget {
                    limit: self.max_response_body_bytes,
                });
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).map_err(|_| HistoryClientError::InvalidPayload {
            provider: "HyperSync",
            operation: "query response",
        })
    }
}

struct HyperSyncRuntimeState {
    runtime: Option<Runtime>,
    closing: bool,
}

struct HyperSyncRuntime {
    state: Mutex<HyperSyncRuntimeState>,
    requests: Arc<Semaphore>,
}

impl HyperSyncRuntime {
    fn new() -> Result<Self, HistoryClientError> {
        let runtime = RuntimeBuilder::new_multi_thread()
            .worker_threads(HYPERSYNC_RUNTIME_THREADS)
            .max_blocking_threads(HYPERSYNC_BLOCKING_THREADS)
            .thread_name("quant-hypersync")
            .enable_all()
            .build()
            .map_err(|error| {
                HistoryClientError::InvalidConfig(format!(
                    "cannot build bounded HyperSync runtime: {error}"
                ))
            })?;
        Ok(Self {
            state: Mutex::new(HyperSyncRuntimeState {
                runtime: Some(runtime),
                closing: false,
            }),
            requests: Arc::new(Semaphore::new(HYPERSYNC_REQUESTS)),
        })
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, HyperSyncRuntimeState>, HistoryClientError> {
        self.state
            .lock()
            .map_err(|_| HistoryClientError::RuntimeStatePoisoned {
                provider: "HyperSync",
            })
    }

    async fn run<T, F>(&self, operation: &'static str, future: F) -> Result<T, HistoryClientError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, HistoryClientError>> + Send + 'static,
    {
        let task = {
            // Admission and transition to closing are serialized by this lock.
            // The permit moves into the task so caller cancellation cannot make
            // shutdown mistake detached SDK work for an idle runtime.
            let state = self.lock_state()?;
            let runtime = state.runtime.as_ref().filter(|_| !state.closing).ok_or(
                HistoryClientError::RuntimeUnavailable {
                    provider: "HyperSync",
                },
            )?;
            let permit = Arc::clone(&self.requests)
                .try_acquire_owned()
                .map_err(|_| HistoryClientError::CapacityUnavailable {
                    provider: "HyperSync",
                    resource: "isolated request slot",
                })?;
            let handle = runtime.handle().clone();
            drop(state);
            handle.spawn(async move {
                let _permit = permit;
                future.await
            })
        };
        task.await
            .map_err(|_| HistoryClientError::RuntimeTaskFailed {
                provider: "HyperSync",
                operation,
            })?
    }

    async fn shutdown(&self) -> Result<(), HistoryClientError> {
        {
            let mut state = self.lock_state()?;
            if state.runtime.is_none() {
                return Ok(());
            }
            state.closing = true;
        }

        // Once closing is visible, no new task can consume the sole permit.
        // An in-flight task owns it even if its original caller was cancelled.
        let permit = Arc::clone(&self.requests)
            .acquire_owned()
            .await
            .map_err(|_| HistoryClientError::RuntimeUnavailable {
                provider: "HyperSync",
            })?;
        let runtime = self.lock_state()?.runtime.take();
        drop(permit);
        if let Some(runtime) = runtime {
            // The sole request permit proves that no SDK task is still active.
            // Background shutdown avoids blocking an application Tokio worker.
            runtime.shutdown_background();
        }
        Ok(())
    }
}

impl Drop for HyperSyncRuntime {
    fn drop(&mut self) {
        if let Ok(state) = self.state.get_mut()
            && let Some(runtime) = state.runtime.take()
        {
            // Explicit async shutdown is the clean path. This is only a
            // best-effort fallback for construction unwinds or owner misuse.
            runtime.shutdown_background();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalBlockHeader {
    pub number: u64,
    pub hash: String,
    pub parent_hash: String,
    pub timestamp: u64,
}

#[derive(Serialize)]
struct CanonicalHeaderBudget<'a> {
    number: u64,
    hash: &'a str,
    parent_hash: &'a str,
    timestamp: u64,
}

impl CanonicalBlockHeader {
    fn canonical_budget_bytes(&self) -> Result<usize, HistoryClientError> {
        let budget = CanonicalHeaderBudget {
            number: u64::MAX,
            hash: &self.hash,
            parent_hash: &self.parent_hash,
            timestamp: u64::MAX,
        };
        let mut counter = CanonicalByteCounter::default();
        serde_json::to_writer(&mut counter, &budget)
            .map_err(|_| HistoryClientError::CanonicalEncoding)?;
        Ok(counter.bytes)
    }
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

#[derive(Serialize)]
struct CanonicalLogBudget<'a> {
    address: &'a str,
    block_number: u64,
    block_hash: &'a str,
    block_timestamp: u64,
    model_available_timestamp: u64,
    parent_block_hash: &'a str,
    transaction_hash: &'a str,
    transaction_index: u64,
    log_index: u64,
    topics: &'a [String],
    data: &'a str,
    removed: bool,
}

#[derive(Default)]
struct CanonicalByteCounter {
    bytes: usize,
}

impl Write for CanonicalByteCounter {
    fn write(&mut self, buffer: &[u8]) -> IoResult<usize> {
        self.bytes = self
            .bytes
            .checked_add(buffer.len())
            .ok_or_else(|| IoError::other("canonical log byte count overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
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

    fn canonical_budget_bytes(&self) -> Result<usize, HistoryClientError> {
        let parent_block_hash = if self.parent_block_hash.is_empty() {
            &self.block_hash
        } else {
            &self.parent_block_hash
        };
        let budget = CanonicalLogBudget {
            address: &self.address,
            block_number: u64::MAX,
            block_hash: &self.block_hash,
            block_timestamp: u64::MAX,
            model_available_timestamp: u64::MAX,
            parent_block_hash,
            transaction_hash: &self.transaction_hash,
            transaction_index: u64::MAX,
            log_index: u64::MAX,
            topics: &self.topics,
            data: &self.data,
            removed: false,
        };
        let mut counter = CanonicalByteCounter::default();
        serde_json::to_writer(&mut counter, &budget)
            .map_err(|_| HistoryClientError::CanonicalEncoding)?;
        Ok(counter.bytes)
    }

    fn validate_range(
        &self,
        from_block: u64,
        to_block: u64,
        provider: &'static str,
    ) -> Result<(), HistoryClientError> {
        if (from_block..=to_block).contains(&self.block_number) {
            Ok(())
        } else {
            Err(HistoryClientError::UnexpectedBlock {
                provider,
                entity: "log",
                number: self.block_number,
                from_block,
                to_block,
            })
        }
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
    #[error("{provider} is not accepting isolated runtime requests")]
    RuntimeUnavailable { provider: &'static str },
    #[error("{provider} {resource} capacity is unavailable")]
    CapacityUnavailable {
        provider: &'static str,
        resource: &'static str,
    },
    #[error("{provider} isolated runtime task failed during {operation}")]
    RuntimeTaskFailed {
        provider: &'static str,
        operation: &'static str,
    },
    #[error("{provider} isolated runtime state is poisoned")]
    RuntimeStatePoisoned { provider: &'static str },
    #[error("archive RPC returned HTTP status {status} during {method}")]
    HttpStatus { method: &'static str, status: u16 },
    #[error("archive RPC rejected {method} with code {code}: {message}")]
    RpcRejected {
        method: &'static str,
        code: i64,
        message: String,
    },
    #[error("archive RPC response body exceeded the configured {limit} byte budget")]
    RpcResponseBodyBudget { limit: usize },
    #[error("HyperSync response body exceeded the configured {limit} byte budget")]
    HyperSyncResponseBodyBudget { limit: usize },
    #[error("canonical history chunk exceeded the configured {limit} byte budget")]
    CanonicalChunkBudget { limit: usize },
    #[error("canonical exchange-history log encoding failed")]
    CanonicalEncoding,
    #[error("{provider} returned an invalid payload during {operation}")]
    InvalidPayload {
        provider: &'static str,
        operation: &'static str,
    },
    #[error("HyperSync returned HTTP status {status}")]
    HyperSyncHttpStatus { status: u16 },
    #[error("HyperSync response exceeded the provider payload limit")]
    HyperSyncPayloadTooLarge,
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
    #[error(
        "{provider} returned {entity} block {number} outside requested range {from_block}..={to_block}"
    )]
    UnexpectedBlock {
        provider: &'static str,
        entity: &'static str,
        number: u64,
        from_block: u64,
        to_block: u64,
    },
    #[error("{provider} returned duplicate block header {number}")]
    DuplicateBlockHeader { provider: &'static str, number: u64 },
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
    #[error(
        "{provider} pagination advanced to block {next_block} beyond requested exclusive end {requested_end}"
    )]
    PaginationOverrun {
        provider: &'static str,
        next_block: u64,
        requested_end: u64,
    },
    #[error(
        "{provider} returned {actual} unique headers for page {from_block}..={to_block}; expected {expected}"
    )]
    IncompleteHeaderPage {
        provider: &'static str,
        from_block: u64,
        to_block: u64,
        expected: usize,
        actual: usize,
    },
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

struct CanonicalLogBuffer {
    rows: Vec<CanonicalExchangeLog>,
    canonical_bytes: usize,
    limit: usize,
}

impl CanonicalLogBuffer {
    fn new(limit: usize, prefix_bytes: usize) -> Result<Self, HistoryClientError> {
        let canonical_bytes = prefix_bytes
            .checked_add(2)
            .ok_or(HistoryClientError::CanonicalChunkBudget { limit })?;
        if canonical_bytes > limit {
            return Err(HistoryClientError::CanonicalChunkBudget { limit });
        }
        Ok(Self {
            rows: Vec::new(),
            canonical_bytes,
            limit,
        })
    }

    fn push(&mut self, row: CanonicalExchangeLog) -> Result<(), HistoryClientError> {
        let separator_bytes = usize::from(!self.rows.is_empty());
        let row_bytes = row.canonical_budget_bytes()?;
        let canonical_bytes = self
            .canonical_bytes
            .checked_add(separator_bytes)
            .and_then(|bytes| bytes.checked_add(row_bytes))
            .ok_or(HistoryClientError::CanonicalChunkBudget { limit: self.limit })?;
        if canonical_bytes > self.limit {
            return Err(HistoryClientError::CanonicalChunkBudget { limit: self.limit });
        }
        self.rows.push(row);
        self.canonical_bytes = canonical_bytes;
        Ok(())
    }

    fn extend<I>(&mut self, rows: I) -> Result<(), HistoryClientError>
    where
        I: IntoIterator<Item = CanonicalExchangeLog>,
    {
        for row in rows {
            self.push(row)?;
        }
        Ok(())
    }
}

impl From<CanonicalLogBuffer> for (Vec<CanonicalExchangeLog>, usize) {
    fn from(buffer: CanonicalLogBuffer) -> Self {
        (buffer.rows, buffer.canonical_bytes)
    }
}

fn add_header_bytes(
    canonical_bytes: usize,
    header_count: usize,
    header: &CanonicalBlockHeader,
    limit: usize,
) -> Result<usize, HistoryClientError> {
    let separator_bytes = usize::from(header_count > 0);
    let header_bytes = header.canonical_budget_bytes()?;
    let canonical_bytes = canonical_bytes
        .checked_add(separator_bytes)
        .and_then(|bytes| bytes.checked_add(header_bytes))
        .ok_or(HistoryClientError::CanonicalChunkBudget { limit })?;
    if canonical_bytes > limit {
        Err(HistoryClientError::CanonicalChunkBudget { limit })
    } else {
        Ok(canonical_bytes)
    }
}

pub struct ExchangeHistoryExtractor {
    client: Arc<HyperSyncJsonClient>,
    runtime: HyperSyncRuntime,
    confirmation_blocks: u64,
    max_blocks_per_chunk: u64,
    max_canonical_chunk_bytes: usize,
}

impl ExchangeHistoryExtractor {
    pub fn connect(config: &FinalizedExchangeHistoryConfig) -> Result<Self, HistoryClientError> {
        Ok(Self {
            client: Arc::new(HyperSyncJsonClient::connect(config)?),
            runtime: HyperSyncRuntime::new()?,
            confirmation_blocks: config.model_confirmation_blocks,
            max_blocks_per_chunk: config.max_blocks_per_chunk,
            max_canonical_chunk_bytes: config.max_canonical_chunk_bytes,
        })
    }

    pub async fn shutdown(&self) -> Result<(), HistoryClientError> {
        self.runtime.shutdown().await
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
        let block_span = to_block
            .checked_sub(from_block)
            .and_then(|span| span.checked_add(1))
            .ok_or_else(|| HistoryClientError::InvalidConfig("chunk span overflow".to_owned()))?;
        if block_span > self.max_blocks_per_chunk {
            return Err(HistoryClientError::InvalidConfig(
                "chunk span exceeds the configured maximum".to_owned(),
            ));
        }
        let confirmation_end = to_block
            .checked_add(self.confirmation_blocks)
            .ok_or_else(|| {
                HistoryClientError::InvalidConfig("confirmation range overflow".to_owned())
            })?;
        let (mut archive_height, blocks, header_proof, header_bytes) = self
            .fetch_headers(from_block, confirmation_end, to_block)
            .await?;
        validate_header_chain(&blocks, from_block, confirmation_end, "HyperSync")?;
        if archive_height < confirmation_end {
            return Err(HistoryClientError::ArchiveLag {
                archive_height,
                required_block: confirmation_end,
            });
        }
        let (mut logs, log_archive_height, log_proof) =
            self.fetch_logs(from_block, to_block, header_bytes).await?;
        archive_height = archive_height.max(log_archive_height);
        let continuity_proof = header_proof.or(log_proof);
        hydrate_log_times(&mut logs, &blocks, self.confirmation_blocks, "HyperSync")?;
        canonical_sort(&mut logs);
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

    async fn fetch_logs(
        &self,
        from_block: u64,
        to_block: u64,
        header_bytes: usize,
    ) -> Result<
        (
            Vec<CanonicalExchangeLog>,
            u64,
            Option<HistoryContinuityProof>,
        ),
        HistoryClientError,
    > {
        let log_end_exclusive = to_block
            .checked_add(1)
            .ok_or_else(|| HistoryClientError::InvalidConfig("query range overflow".to_owned()))?;
        let addresses = EXCHANGE_CONTRACTS
            .iter()
            .map(|contract| format!("{:#x}", contract.address))
            .collect::<Vec<_>>();
        let topics = EXCHANGE_CONTRACTS
            .iter()
            .flat_map(|contract| {
                [
                    contract.order_filled_topic,
                    contract.orders_matched_topic,
                    contract.fee_charged_topic,
                ]
            })
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
        let mut continuity_proof = None;
        let mut logs = CanonicalLogBuffer::new(self.max_canonical_chunk_bytes, header_bytes)?;
        while cursor < log_end_exclusive {
            let query = Query::new()
                .from_block(cursor)
                .to_block_excl(log_end_exclusive)
                .where_logs(log_filter.clone())
                .join_mode(JoinMode::JoinNothing)
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
            let client = Arc::clone(&self.client);
            let response = self.runtime.run("query", client.query(query)).await?;
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
            if response.next_block > log_end_exclusive {
                return Err(HistoryClientError::PaginationOverrun {
                    provider: "HyperSync",
                    next_block: response.next_block,
                    requested_end: log_end_exclusive,
                });
            }
            let page_end = response.next_block;
            for log in response.data.logs {
                let log = CanonicalExchangeLog::try_from(log)?;
                log.validate_range(cursor, page_end.saturating_sub(1), "HyperSync")?;
                logs.push(log)?;
            }
            if continuity_proof.is_none()
                && let Some(guard) = response.rollback_guard
                && guard.first_block_number == from_block
                && guard.block_number >= to_block
            {
                continuity_proof = Some(HistoryContinuityProof {
                    basis: HistoryContinuityProofBasis::HyperSyncRollbackGuard,
                    attested_block_number: guard.block_number,
                    attested_block_hash: guard.hash,
                    first_block_number: guard.first_block_number,
                    first_parent_hash: guard.first_parent_hash,
                });
            }
            cursor = page_end;
        }
        let (logs, _) = logs.into();
        Ok((logs, archive_height, continuity_proof))
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
            usize,
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
        let mut canonical_bytes = 2_usize;
        while cursor < end_exclusive {
            let mut query = Query::new()
                .from_block(cursor)
                .to_block_excl(end_exclusive)
                .include_all_blocks()
                .select_block_fields([
                    BlockField::Number,
                    BlockField::Hash,
                    BlockField::ParentHash,
                    BlockField::Timestamp,
                ]);
            query.max_num_blocks =
                Some(usize::try_from(end_exclusive.saturating_sub(cursor)).unwrap_or(usize::MAX));
            let client = Arc::clone(&self.client);
            let response = self
                .runtime
                .run("block header query", client.query(query))
                .await?;
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
            if response.next_block > end_exclusive {
                return Err(HistoryClientError::PaginationOverrun {
                    provider: "HyperSync",
                    next_block: response.next_block,
                    requested_end: end_exclusive,
                });
            }
            let page_end = response.next_block;
            let initial_count = blocks.len();
            for block in response.data.blocks {
                let header = CanonicalBlockHeader::try_from(block)?;
                if !(cursor..page_end).contains(&header.number) {
                    return Err(HistoryClientError::UnexpectedBlock {
                        provider: "HyperSync",
                        entity: "header",
                        number: header.number,
                        from_block: cursor,
                        to_block: page_end.saturating_sub(1),
                    });
                }
                let number = header.number;
                canonical_bytes = add_header_bytes(
                    canonical_bytes,
                    blocks.len(),
                    &header,
                    self.max_canonical_chunk_bytes,
                )?;
                if blocks.insert(number, header).is_some() {
                    return Err(HistoryClientError::DuplicateBlockHeader {
                        provider: "HyperSync",
                        number,
                    });
                }
            }
            let actual = blocks.len().saturating_sub(initial_count);
            let expected = usize::try_from(page_end.saturating_sub(cursor)).unwrap_or(usize::MAX);
            if actual != expected {
                return Err(HistoryClientError::IncompleteHeaderPage {
                    provider: "HyperSync",
                    from_block: cursor,
                    to_block: page_end.saturating_sub(1),
                    expected,
                    actual,
                });
            }
            if proof.is_none()
                && let Some(guard) = response.rollback_guard
                && guard.first_block_number == from_block
                && guard.block_number >= proof_through_block
            {
                proof = Some(HistoryContinuityProof {
                    basis: HistoryContinuityProofBasis::HyperSyncRollbackGuard,
                    attested_block_number: guard.block_number,
                    attested_block_hash: guard.hash,
                    first_block_number: guard.first_block_number,
                    first_parent_hash: guard.first_parent_hash,
                });
            }
            cursor = page_end;
        }
        Ok((archive_height, blocks, proof, canonical_bytes))
    }
}

pub struct ExchangeHistoryAttestor {
    client: ReqwestClient,
    endpoint: String,
    max_rpc_response_body_bytes: usize,
    max_canonical_chunk_bytes: usize,
    max_blocks_per_log_request: u64,
    max_concurrent_log_requests: usize,
    confirmation_blocks: u64,
    max_blocks_per_chunk: u64,
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
            max_rpc_response_body_bytes: config.max_rpc_response_body_bytes,
            max_canonical_chunk_bytes: config.max_canonical_chunk_bytes,
            max_blocks_per_log_request: config.attestor.max_blocks_per_log_request,
            max_concurrent_log_requests: config.attestor.max_concurrent_log_requests,
            confirmation_blocks: config.model_confirmation_blocks,
            max_blocks_per_chunk: config.max_blocks_per_chunk,
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
        if to_block < from_block {
            return Err(HistoryClientError::InvalidConfig(
                "attestor chunk end precedes chunk start".to_owned(),
            ));
        }
        let block_span = to_block
            .checked_sub(from_block)
            .and_then(|span| span.checked_add(1))
            .ok_or_else(|| {
                HistoryClientError::InvalidConfig("attestor chunk span overflow".to_owned())
            })?;
        if block_span > self.max_blocks_per_chunk {
            return Err(HistoryClientError::InvalidConfig(
                "attestor chunk span exceeds the configured maximum".to_owned(),
            ));
        }
        let (mut logs, log_bytes) = self.fetch_logs(from_block, to_block).await?;
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
        let mut headers = stream::iter(required_blocks)
            .map(
                |number| async move { self.block_by_number(number).await.map(|row| (number, row)) },
            )
            .buffer_unordered(self.max_concurrent_log_requests);
        let mut blocks = BTreeMap::new();
        let mut canonical_bytes =
            log_bytes
                .checked_add(2)
                .ok_or(HistoryClientError::CanonicalChunkBudget {
                    limit: self.max_canonical_chunk_bytes,
                })?;
        while let Some(header) = headers.next().await {
            let (number, header) = header?;
            if header.number != number {
                return Err(HistoryClientError::UnexpectedBlock {
                    provider: "archive RPC",
                    entity: "header",
                    number: header.number,
                    from_block: number,
                    to_block: number,
                });
            }
            canonical_bytes = add_header_bytes(
                canonical_bytes,
                blocks.len(),
                &header,
                self.max_canonical_chunk_bytes,
            )?;
            if blocks.insert(number, header).is_some() {
                return Err(HistoryClientError::DuplicateBlockHeader {
                    provider: "archive RPC",
                    number,
                });
            }
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
    ) -> Result<(Vec<CanonicalExchangeLog>, usize), HistoryClientError> {
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
        let mut pages = stream::iter(ranges)
            .map(|(start, end)| self.fetch_log_range(start, end))
            .buffer_unordered(self.max_concurrent_log_requests);
        let mut logs = CanonicalLogBuffer::new(self.max_canonical_chunk_bytes, 0)?;
        while let Some(page) = pages.next().await {
            logs.extend(page?)?;
        }
        Ok(logs.into())
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
            .flat_map(|contract| {
                [
                    contract.order_filled_topic,
                    contract.orders_matched_topic,
                    contract.fee_charged_topic,
                ]
            })
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
        let mut logs = Vec::with_capacity(rows.len());
        for row in rows {
            let log = CanonicalExchangeLog::try_from(row)?;
            log.validate_range(from_block, to_block, "archive RPC")?;
            logs.push(log);
        }
        Ok(logs)
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
            if body.len().saturating_add(chunk.len()) > self.max_rpc_response_body_bytes {
                return Err(HistoryClientError::RpcResponseBodyBudget {
                    limit: self.max_rpc_response_body_bytes,
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

impl TryFrom<HyperSyncBlock> for CanonicalBlockHeader {
    type Error = HistoryClientError;

    fn try_from(block: HyperSyncBlock) -> Result<Self, Self::Error> {
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
        Ok(Self {
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
}

impl TryFrom<HyperSyncLog> for CanonicalExchangeLog {
    type Error = HistoryClientError;

    fn try_from(log: HyperSyncLog) -> Result<Self, Self::Error> {
        let signature_topic = log.topic0.ok_or(HistoryClientError::MissingField {
            provider: "HyperSync",
            field: "log.topic0",
        })?;
        let mut canonical_topics = vec![signature_topic.to_string()];
        let mut found_gap = false;
        for topic in [log.topic1, log.topic2, log.topic3] {
            match topic {
                Some(_) if found_gap => {
                    return Err(HistoryClientError::InvalidField {
                        provider: "HyperSync",
                        field: "log.topics",
                    });
                }
                Some(topic) => canonical_topics.push(topic.to_string()),
                None => found_gap = true,
            }
        }
        Ok(Self {
            address: log
                .address
                .ok_or(HistoryClientError::MissingField {
                    provider: "HyperSync",
                    field: "log.address",
                })?
                .to_string(),
            block_number: log.block_number.map(Into::into).ok_or(
                HistoryClientError::MissingField {
                    provider: "HyperSync",
                    field: "log.block_number",
                },
            )?,
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
            topics: canonical_topics,
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
    use std::{
        error::Error,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        thread,
        time::Duration,
    };

    use hypersync_net_types::Query;
    use quant_pivot_models::config::FinalizedExchangeHistoryConfig;
    use reqwest::{Client as ReqwestClient, Error as ReqwestError};
    use serde_json::{Value, json};
    use tokio::{sync::Notify, task, time::timeout};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_partial_json, method},
    };

    use super::{
        AttestedHistoryChunk, CanonicalBlockHeader, CanonicalExchangeLog, CanonicalLogBuffer,
        ExchangeHistoryAttestor, ExtractedHistoryChunk, HistoryClientError, HistoryContinuityProof,
        HistoryContinuityProofBasis, HistoryDigest, HyperSyncJsonClient, HyperSyncLog,
        HyperSyncRuntime, add_header_bytes, chunks_agree,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn hypersync_runtime_is_isolated() -> Result<(), Box<dyn Error>> {
        let runtime = HyperSyncRuntime::new()?;
        let thread_name = runtime
            .run("thread isolation", async {
                task::block_in_place(|| {
                    thread::current().name().map(str::to_owned).ok_or_else(|| {
                        HistoryClientError::InvalidConfig(
                            "isolated runtime thread has no name".to_owned(),
                        )
                    })
                })
            })
            .await?;

        assert!(thread_name.starts_with("quant-hypersync"));
        runtime.shutdown().await?;
        runtime.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn runtime_admission_is_bounded() -> Result<(), Box<dyn Error>> {
        let runtime = Arc::new(HyperSyncRuntime::new()?);
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let first_runtime = Arc::clone(&runtime);
        let first_started = Arc::clone(&started);
        let first_release = Arc::clone(&release);
        let first = task::spawn(async move {
            first_runtime
                .run("held request", async move {
                    first_started.notify_one();
                    first_release.notified().await;
                    Ok(())
                })
                .await
        });
        started.notified().await;

        let error = runtime
            .run("overflow request", async { Ok(()) })
            .await
            .expect_err("a second request must fail without entering a waiter queue");
        assert!(matches!(
            error,
            HistoryClientError::CapacityUnavailable {
                provider: "HyperSync",
                resource: "isolated request slot"
            }
        ));

        release.notify_one();
        first.await??;
        runtime.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_waits_after_cancellation() -> Result<(), Box<dyn Error>> {
        let runtime = Arc::new(HyperSyncRuntime::new()?);
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let request_runtime = Arc::clone(&runtime);
        let request = task::spawn(async move {
            request_runtime
                .run("cancelled caller", async move {
                    task::block_in_place(move || {
                        started_tx.send(()).map_err(|_| {
                            HistoryClientError::InvalidConfig(
                                "cancellation test start receiver closed".to_owned(),
                            )
                        })?;
                        release_rx.recv().map_err(|_| {
                            HistoryClientError::InvalidConfig(
                                "cancellation test release sender closed".to_owned(),
                            )
                        })?;
                        Ok(())
                    })
                })
                .await
        });
        task::block_in_place(|| started_rx.recv_timeout(Duration::from_secs(5)))?;
        request.abort();
        assert!(
            request
                .await
                .expect_err("request caller must be cancelled")
                .is_cancelled()
        );

        let shutdown_runtime = Arc::clone(&runtime);
        let mut shutdown = task::spawn(async move { shutdown_runtime.shutdown().await });
        let executed = Arc::new(AtomicBool::new(false));
        loop {
            let attempt_executed = Arc::clone(&executed);
            match runtime
                .run("closing race", async move {
                    attempt_executed.store(true, Ordering::SeqCst);
                    Ok(())
                })
                .await
            {
                Err(HistoryClientError::CapacityUnavailable { .. }) => task::yield_now().await,
                Err(HistoryClientError::RuntimeUnavailable {
                    provider: "HyperSync",
                }) => break,
                result => panic!("unexpected run-versus-shutdown result: {result:?}"),
            }
        }
        assert!(!executed.load(Ordering::SeqCst));
        assert!(
            timeout(Duration::from_millis(50), &mut shutdown)
                .await
                .is_err()
        );

        release_tx.send(())?;
        timeout(Duration::from_secs(5), &mut shutdown).await???;
        runtime.shutdown().await?;
        let error = runtime
            .run("after shutdown", async { Ok(()) })
            .await
            .expect_err("closed runtime must reject admission");
        assert!(matches!(
            error,
            HistoryClientError::RuntimeUnavailable {
                provider: "HyperSync"
            }
        ));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn runtime_panic_is_typed() -> Result<(), Box<dyn Error>> {
        let runtime = HyperSyncRuntime::new()?;
        let error = runtime
            .run::<(), _>("panic probe", async {
                panic!("isolated runtime probe panic")
            })
            .await
            .expect_err("task panic must cross the runtime boundary as a typed error");
        assert!(matches!(
            error,
            HistoryClientError::RuntimeTaskFailed {
                provider: "HyperSync",
                operation: "panic probe"
            }
        ));
        runtime.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn hypersync_body_is_bounded() -> Result<(), Box<dyn Error>> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string("x".repeat(33)))
            .expect(1)
            .mount(&server)
            .await;
        let mut config = FinalizedExchangeHistoryConfig::default();
        config.hypersync.endpoint = server.uri();
        config.hypersync.api_token = "00000000-0000-0000-0000-000000000137".into();
        config.max_hypersync_response_body_bytes = 32;
        let client = Arc::new(HyperSyncJsonClient::connect(&config)?);

        let error = client
            .query(Query::new().from_block(1).to_block_excl(2))
            .await
            .expect_err("declared or streamed HyperSync body must fail before decoding");
        assert!(matches!(
            error,
            HistoryClientError::HyperSyncResponseBodyBudget { limit: 32 }
        ));
        Ok(())
    }

    #[test]
    fn hypersync_wire_decodes_topics() -> Result<(), Box<dyn Error>> {
        let base = json!({
            "removed": false,
            "log_index": "0x0",
            "transaction_index": "0x1",
            "transaction_hash": format!("0x{:064x}", 2),
            "block_hash": format!("0x{:064x}", 3),
            "block_number": "0x4",
            "address": "0x0000000000000000000000000000000000000005",
            "data": "0x00",
            "topic0": format!("0x{:064x}", 6),
            "topic1": format!("0x{:064x}", 7),
            "topic2": format!("0x{:064x}", 8),
            "topic3": null
        });
        let canonical =
            CanonicalExchangeLog::try_from(serde_json::from_value::<HyperSyncLog>(base.clone())?)?;
        assert_eq!(canonical.topics.len(), 3);

        let mut gap = base;
        gap["topic1"] = Value::Null;
        let error = CanonicalExchangeLog::try_from(serde_json::from_value::<HyperSyncLog>(gap)?)
            .expect_err("a non-empty topic after a gap must fail closed");
        assert!(matches!(
            error,
            HistoryClientError::InvalidField {
                provider: "HyperSync",
                field: "log.topics"
            }
        ));
        Ok(())
    }

    fn attestor_for(
        server: &MockServer,
        max_response_body_bytes: usize,
        max_blocks_per_log_request: u64,
    ) -> Result<ExchangeHistoryAttestor, ReqwestError> {
        Ok(ExchangeHistoryAttestor {
            client: ReqwestClient::builder().build()?,
            endpoint: server.uri(),
            max_rpc_response_body_bytes: max_response_body_bytes,
            max_canonical_chunk_bytes: max_response_body_bytes,
            max_blocks_per_log_request,
            max_concurrent_log_requests: 2,
            confirmation_blocks: 12,
            max_blocks_per_chunk: 2_000,
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

    fn log_with_data(data: &str) -> CanonicalExchangeLog {
        CanonicalExchangeLog {
            address: "0x0000000000000000000000000000000000000001".to_owned(),
            block_number: 1,
            block_hash: format!("0x{:064x}", 1),
            block_timestamp: 1_700_000_001,
            model_available_timestamp: 1_700_000_013,
            parent_block_hash: format!("0x{:064x}", 0),
            transaction_hash: format!("0x{:064x}", 2),
            transaction_index: 0,
            log_index: 0,
            topics: vec![format!("0x{:064x}", 3)],
            data: data.to_owned(),
            removed: false,
        }
    }

    #[test]
    fn canonical_budget_rejects_extension() -> Result<(), HistoryClientError> {
        let first = log_with_data("0x01");
        let limit = first.canonical_budget_bytes()?.saturating_add(2);
        let mut buffer = CanonicalLogBuffer::new(limit, 0)?;
        buffer.push(first)?;

        let error = buffer
            .push(log_with_data("0x02"))
            .expect_err("overflowing row must be rejected before aggregate extension");
        assert!(matches!(
            error,
            HistoryClientError::CanonicalChunkBudget { limit: value } if value == limit
        ));
        assert_eq!(buffer.rows.len(), 1);
        assert_eq!(buffer.canonical_bytes, limit);
        Ok(())
    }

    #[test]
    fn header_budget_rejects_extension() -> Result<(), HistoryClientError> {
        let header = header(1, &format!("0x{:064x}", 1));
        let limit = header.canonical_budget_bytes()?.saturating_add(2);
        let accepted = add_header_bytes(2, 0, &header, limit)?;
        assert_eq!(accepted, limit);

        let error = add_header_bytes(accepted, 1, &header, limit)
            .expect_err("second header must include a separator and exceed the chunk budget");
        assert!(matches!(
            error,
            HistoryClientError::CanonicalChunkBudget { limit: value } if value == limit
        ));
        Ok(())
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

        assert!(logs.0.is_empty());
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
            Some(HistoryClientError::RpcResponseBodyBudget { limit: 32 })
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
