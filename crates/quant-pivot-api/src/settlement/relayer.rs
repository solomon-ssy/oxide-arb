//! Generic Polymarket Proxy/Safe settlement request journal and transport.

use std::{str::FromStr, time::Duration};

use alloy::{
    eips::BlockNumberOrTag,
    primitives::{Address, B256, Bytes, Signature, U256, keccak256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::{
        client::RpcClient,
        types::{TransactionInput, TransactionRequest},
    },
    signers::SignerSync,
    sol,
    sol_types::{SolCall, SolStruct, eip712_domain},
    transports::http::Http,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use polymarket_client_sdk_v2::{derive_proxy_wallet, derive_safe_wallet};
use quant_pivot_models::{
    config::{OnchainConfig, RelayerConfig},
    enums::quant::ExecutionWalletKind,
    types::{
        ContentHash, EvmAddress, EvmBlockHash, EvmCalldataHash, EvmTransactionHash, EvmUint256,
        RelayerTransactionId,
    },
};
use reqwest::{Client, RequestBuilder, Response, Url, header::CONTENT_TYPE};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use self::deposit_wallet_wire::{
    Batch as DepositWalletBatch, Call as DepositWalletCall,
    Factory::proxyCall as DepositWalletProxyCall,
};
use super::{
    adapter::PreparedSettlementCall,
    eoa::EoaPreparedBlock,
    typed::{
        IntoAlloyAddress, IntoEvmAddress, IntoEvmBlockHash, IntoEvmUint, SettlementValueError,
    },
};
use crate::{keystore::OrderSigner, wallet::WalletTopology};

const POLYGON_CHAIN_ID: u64 = 137;
const PROXY_FACTORY: &str = "0xab45c5a4b0c941a2f231c04c3f49182e1a254052";
const RELAY_HUB: &str = "0xd216153c06e857cd7f72665e0af1d7d82172f494";
const PROXY_CALL_TYPE: u8 = 1;
const SAFE_OPERATION_CALL: u8 = 0;
const PROXY_GAS_BUFFER_NUMERATOR: u64 = 120;
const PROXY_GAS_BUFFER_DENOMINATOR: u64 = 100;
const PROXY_GAS_LIMIT_CAP: u64 = 10_000_000;
const DEPOSIT_WALLET_FACTORY: &str = "0x00000000000fb5c9adea0298d729a0cb3823cc07";
const DEPOSIT_WALLET_DEADLINE_SECONDS: u64 = 600;

sol! {
    struct SafeTx {
        address to;
        uint256 value;
        bytes data;
        uint8 operation;
        uint256 safeTxGas;
        uint256 baseGas;
        uint256 gasPrice;
        address gasToken;
        address refundReceiver;
        uint256 nonce;
    }

    struct ProxyCall {
        uint8 typeCode;
        address to;
        uint256 value;
        bytes data;
    }

    function proxy(ProxyCall[] calls);
}

pub(crate) mod deposit_wallet_wire {
    use alloy::sol;

    sol! {
        struct Call {
            address target;
            uint256 value;
            bytes data;
        }

        struct Batch {
            address wallet;
            uint256 nonce;
            uint256 deadline;
            Call[] calls;
        }

        interface Factory {
            function proxy(Batch[] batches, bytes[] signatures) external;
        }
    }
}

/// Relayer-side transaction state. It is never inferred from an EVM hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayerTransactionState {
    New,
    Executed,
    Mined,
    Invalid,
    Confirmed,
    Failed,
}

impl RelayerTransactionState {
    fn parse(value: &str) -> Result<Self, RelayerError> {
        match value {
            "STATE_NEW" => Ok(Self::New),
            "STATE_EXECUTED" => Ok(Self::Executed),
            "STATE_MINED" => Ok(Self::Mined),
            "STATE_INVALID" => Ok(Self::Invalid),
            "STATE_CONFIRMED" => Ok(Self::Confirmed),
            "STATE_FAILED" => Ok(Self::Failed),
            other => Err(RelayerError::InvalidResponse {
                operation: "relayer_state",
                detail: format!("unknown state '{other}'"),
            }),
        }
    }

    const fn terminal_failure(self) -> bool {
        matches!(self, Self::Invalid | Self::Failed)
    }

    const fn requires_chain_hash(self) -> bool {
        matches!(self, Self::Mined | Self::Confirmed)
    }
}

/// Immutable signed relayer body that must be durable before `POST /submit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedRelayerEnvelope {
    wallet_kind: ExecutionWalletKind,
    funder: EvmAddress,
    target_adapter: EvmAddress,
    call_target: EvmAddress,
    deployment_digest: ContentHash,
    calldata_hash: EvmCalldataHash,
    prepared_block: EoaPreparedBlock,
    nonce: EvmUint256,
    gas_limit: Option<EvmUint256>,
    signed_envelope: Vec<u8>,
    signed_envelope_hash: ContentHash,
}

/// Complete durable journal payload required to restore one relayer request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableRelayerEnvelope {
    pub wallet_kind: ExecutionWalletKind,
    pub funder: EvmAddress,
    pub target_adapter: EvmAddress,
    pub call_target: EvmAddress,
    pub deployment_digest: ContentHash,
    pub calldata_hash: EvmCalldataHash,
    pub prepared_block: EoaPreparedBlock,
    pub nonce: EvmUint256,
    pub gas_limit: Option<EvmUint256>,
    pub signed_envelope: Vec<u8>,
    pub signed_envelope_hash: ContentHash,
}

impl PreparedRelayerEnvelope {
    /// Rehydrate an exact relayer body from the durable submission journal.
    /// This recovery constructor never signs, fetches a nonce, or changes the
    /// call target; transport methods revalidate the body hash before use.
    pub fn restore_durable(durable: DurableRelayerEnvelope) -> Result<Self, RelayerError> {
        if durable.wallet_kind == ExecutionWalletKind::Eoa
            || durable.signed_envelope.is_empty()
            || content_hash(&durable.signed_envelope) != durable.signed_envelope_hash
        {
            return Err(RelayerError::CorruptDurableEnvelope);
        }
        Ok(Self {
            wallet_kind: durable.wallet_kind,
            funder: durable.funder,
            target_adapter: durable.target_adapter,
            call_target: durable.call_target,
            deployment_digest: durable.deployment_digest,
            calldata_hash: durable.calldata_hash,
            prepared_block: durable.prepared_block,
            nonce: durable.nonce,
            gas_limit: durable.gas_limit,
            signed_envelope: durable.signed_envelope,
            signed_envelope_hash: durable.signed_envelope_hash,
        })
    }

    #[must_use]
    pub const fn wallet_kind(&self) -> ExecutionWalletKind {
        self.wallet_kind
    }

    #[must_use]
    pub const fn funder(&self) -> &EvmAddress {
        &self.funder
    }

    #[must_use]
    pub const fn target_adapter(&self) -> &EvmAddress {
        &self.target_adapter
    }

    #[must_use]
    pub const fn call_target(&self) -> &EvmAddress {
        &self.call_target
    }

    #[must_use]
    pub const fn deployment_digest(&self) -> ContentHash {
        self.deployment_digest
    }

    #[must_use]
    pub const fn calldata_hash(&self) -> &EvmCalldataHash {
        &self.calldata_hash
    }

    #[must_use]
    pub const fn prepared_block(&self) -> &EoaPreparedBlock {
        &self.prepared_block
    }

    #[must_use]
    pub const fn nonce(&self) -> &EvmUint256 {
        &self.nonce
    }

    #[must_use]
    pub const fn gas_limit(&self) -> Option<&EvmUint256> {
        self.gas_limit.as_ref()
    }

    #[must_use]
    pub fn signed_envelope(&self) -> &[u8] {
        &self.signed_envelope
    }

    #[must_use]
    pub const fn signed_envelope_hash(&self) -> ContentHash {
        self.signed_envelope_hash
    }
}

/// Immediate response to `POST /submit`. No chain hash exists at this stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayerSubmission {
    pub transaction_id: RelayerTransactionId,
    pub state: RelayerTransactionState,
}

/// Poll outcome for one durable relayer transaction identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayerPollOutcome {
    Pending {
        state: RelayerTransactionState,
    },
    ChainHashObserved {
        state: RelayerTransactionState,
        transaction_hash: EvmTransactionHash,
    },
    TerminalFailure {
        state: RelayerTransactionState,
    },
}

/// Generic relayer preparation, submission, or polling failure.
#[derive(Debug, thiserror::Error)]
pub enum RelayerError {
    #[error("relayer settlement requires a Proxy or Gnosis Safe topology")]
    WrongWalletKind,
    #[error("relayer signer/funder/call identity does not match")]
    WalletIdentityMismatch,
    #[error("relayer API key owner does not match the settlement signer")]
    CredentialOwnerMismatch,
    #[error("relayer credentials are not configured")]
    MissingCredentials,
    #[error("invalid relayer URL or official contract address: {detail}")]
    InvalidConfiguration { detail: String },
    #[error("relayer preparation RPC is connected to chain {actual}, expected Polygon chain 137")]
    WrongChain { actual: u64 },
    #[error("relayer preparation call {operation} failed: {detail}")]
    PreparationCall {
        operation: &'static str,
        detail: String,
    },
    #[error("prepared block {block_number} changed before relayer body preparation completed")]
    CanonicalBlockChanged { block_number: u64 },
    #[error("{kind} funder is not deployed and registered with the relayer")]
    WalletNotReady { kind: &'static str },
    #[error("proxy gas estimate {estimated} cannot be buffered within the 10,000,000 cap")]
    ProxyGasLimitOutOfBounds { estimated: u64 },
    #[error("relayer prepared value is not canonical: {detail}")]
    InvalidPreparedValue { detail: String },
    #[error("failed to sign relayer request: {detail}")]
    Signing { detail: String },
    #[error("failed to serialize the signed relayer request: {detail}")]
    Serialization { detail: String },
    #[error("relayer call {operation} failed: {detail}")]
    TransportCall {
        operation: &'static str,
        detail: String,
    },
    #[error("relayer rejected submission with HTTP {status}")]
    SubmissionRejected { status: u16 },
    #[error(
        "relayer submission outcome is ambiguous; replay only the exact durable body: {detail}"
    )]
    AmbiguousSubmission { detail: String },
    #[error("invalid relayer response for {operation}: {detail}")]
    InvalidResponse {
        operation: &'static str,
        detail: String,
    },
    #[error("durable relayer body bytes do not match their frozen hash")]
    CorruptDurableEnvelope,
}

impl From<SettlementValueError> for RelayerError {
    fn from(error: SettlementValueError) -> Self {
        Self::InvalidPreparedValue {
            detail: error.detail().to_owned(),
        }
    }
}

/// Read-only Polygon boundary used while freezing a relayer body.
#[async_trait]
pub trait RelayerPreparationRpc: Send + Sync {
    async fn chain_id(&self) -> Result<u64, RelayerError>;

    async fn canonical_head(&self) -> Result<EoaPreparedBlock, RelayerError>;

    /// `Ok(None)` means `eth_estimateGas` failed and invokes the bounded
    /// 10,000,000 fallback. Head reads and canonical checks never fall back.
    async fn estimate_proxy_gas(
        &self,
        signer: Address,
        factory: Address,
        calldata: &[u8],
    ) -> Result<Option<u64>, RelayerError>;

    async fn canonical_hash(&self, block_number: u64)
    -> Result<Option<EvmBlockHash>, RelayerError>;
}

/// Stateless builder for the official generic Proxy/Safe relayer wire format.
#[derive(Debug, Default, Clone, Copy)]
pub struct RelayerRequestBuilder;

impl RelayerRequestBuilder {
    pub async fn prepare<R: RelayerPreparationRpc>(
        &self,
        transport: &RelayerTransport,
        rpc: &R,
        signer: &OrderSigner,
        topology: &WalletTopology,
        call: &PreparedSettlementCall,
    ) -> Result<PreparedRelayerEnvelope, RelayerError> {
        verify_relayer_identity(signer, topology, call)?;
        let chain_id = rpc.chain_id().await?;
        if chain_id != POLYGON_CHAIN_ID {
            return Err(RelayerError::WrongChain { actual: chain_id });
        }
        let prepared_block = rpc.canonical_head().await?;
        let target = (call.call_target()).into_alloy_address()?;

        let (signed_envelope, nonce, gas_limit) = match topology.kind {
            ExecutionWalletKind::GnosisSafe => {
                if !transport.wallet_deployed(topology.funder, None).await? {
                    return Err(RelayerError::WalletNotReady {
                        kind: "gnosis_safe",
                    });
                }
                let nonce = transport.safe_nonce(topology.signer).await?;
                let body = build_safe_body(
                    signer,
                    topology,
                    call.call_target().as_str(),
                    target,
                    call.calldata(),
                    nonce,
                )?;
                (serialize_envelope(&body)?, nonce, None)
            }
            ExecutionWalletKind::Proxy => {
                let payload = transport.proxy_relay_payload(topology.signer).await?;
                let factory = official_address(PROXY_FACTORY)?;
                let proxy_calldata = proxyCall {
                    calls: vec![ProxyCall {
                        typeCode: PROXY_CALL_TYPE,
                        to: target,
                        value: U256::ZERO,
                        data: Bytes::copy_from_slice(call.calldata()),
                    }],
                }
                .abi_encode();
                let gas_limit = proxy_gas_limit(
                    rpc.estimate_proxy_gas(topology.signer, factory, &proxy_calldata)
                        .await?,
                )?;
                let body = build_proxy_body(
                    signer,
                    topology,
                    factory,
                    &proxy_calldata,
                    &payload,
                    gas_limit,
                )?;
                (serialize_envelope(&body)?, payload.nonce, Some(gas_limit))
            }
            ExecutionWalletKind::DepositWallet => {
                if !transport
                    .wallet_deployed(topology.funder, Some("WALLET"))
                    .await?
                {
                    return Err(RelayerError::WalletNotReady {
                        kind: "deposit_wallet",
                    });
                }
                let nonce = transport.wallet_nonce(topology.signer).await?;
                let now = u64::try_from(Utc::now().timestamp()).map_err(|error| {
                    RelayerError::InvalidPreparedValue {
                        detail: error.to_string(),
                    }
                })?;
                let deadline = now
                    .checked_add(DEPOSIT_WALLET_DEADLINE_SECONDS)
                    .ok_or_else(|| RelayerError::InvalidPreparedValue {
                        detail: "deposit wallet deadline overflow".to_owned(),
                    })?;
                let body = build_deposit_wallet_body(signer, topology, call, nonce, deadline)?;
                (serialize_envelope(&body)?, nonce, None)
            }
            ExecutionWalletKind::Eoa => return Err(RelayerError::WrongWalletKind),
        };

        if rpc.canonical_hash(prepared_block.number).await?.as_ref() != Some(&prepared_block.hash) {
            return Err(RelayerError::CanonicalBlockChanged {
                block_number: prepared_block.number,
            });
        }

        let signed_envelope_hash = content_hash(&signed_envelope);
        Ok(PreparedRelayerEnvelope {
            wallet_kind: topology.kind,
            funder: call.funder().clone(),
            target_adapter: call.target_adapter().clone(),
            call_target: call.call_target().clone(),
            deployment_digest: call.deployment_digest(),
            calldata_hash: call.calldata_hash().clone(),
            prepared_block,
            nonce: (nonce).into_evm_uint()?,
            gas_limit: gas_limit.map(IntoEvmUint::into_evm_uint).transpose()?,
            signed_envelope,
            signed_envelope_hash,
        })
    }
}

/// HTTP-only relayer transport. It accepts frozen signed bytes and contains no
/// settlement route, contract, approval, or redemption knowledge.
#[derive(Clone)]
pub struct RelayerTransport {
    http: Client,
    base_url: Url,
    config: RelayerConfig,
}

impl RelayerTransport {
    pub fn connect(
        config: &RelayerConfig,
        topology: &WalletTopology,
    ) -> Result<Self, RelayerError> {
        let api_key_owner = config
            .api_key_address()
            .ok_or(RelayerError::MissingCredentials)
            .and_then(|value| {
                Address::from_str(value).map_err(|error| RelayerError::InvalidConfiguration {
                    detail: error.to_string(),
                })
            })?;
        if config.api_key().is_none() {
            return Err(RelayerError::MissingCredentials);
        }
        if api_key_owner != topology.signer {
            return Err(RelayerError::CredentialOwnerMismatch);
        }
        let base_url = Url::parse(&ensure_trailing_slash(&config.base_url)).map_err(|error| {
            RelayerError::InvalidConfiguration {
                detail: error.to_string(),
            }
        })?;
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .map_err(|error| RelayerError::InvalidConfiguration {
                detail: error.to_string(),
            })?;
        Ok(Self {
            http,
            base_url,
            config: config.clone(),
        })
    }

    /// Submit or replay the exact durable bytes. A transport/server/decode
    /// failure is ambiguous because the relayer may already have accepted it.
    pub async fn submit(
        &self,
        prepared: &PreparedRelayerEnvelope,
    ) -> Result<RelayerSubmission, RelayerError> {
        self.submit_frozen(prepared.signed_envelope(), prepared.signed_envelope_hash)
            .await
    }

    /// Replay bytes loaded from the durable journal. This recovery boundary
    /// never rebuilds a request, nonce, signature, target, or wallet body.
    pub async fn submit_durable(
        &self,
        signed_envelope: &[u8],
        signed_envelope_hash: ContentHash,
    ) -> Result<RelayerSubmission, RelayerError> {
        self.submit_frozen(signed_envelope, signed_envelope_hash)
            .await
    }

    async fn submit_frozen(
        &self,
        signed_envelope: &[u8],
        signed_envelope_hash: ContentHash,
    ) -> Result<RelayerSubmission, RelayerError> {
        if content_hash(signed_envelope) != signed_envelope_hash {
            return Err(RelayerError::CorruptDurableEnvelope);
        }
        let url = self.endpoint("submit")?;
        let response = self
            .authenticated(self.http.post(url))?
            .header(CONTENT_TYPE, "application/json")
            .body(signed_envelope.to_vec())
            .send()
            .await
            .map_err(|error| RelayerError::AmbiguousSubmission {
                detail: error.to_string(),
            })?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|error| RelayerError::AmbiguousSubmission {
                detail: error.to_string(),
            })?;
        if status.is_server_error() {
            return Err(RelayerError::AmbiguousSubmission {
                detail: format!("HTTP {status}"),
            });
        }
        if !status.is_success() {
            return Err(RelayerError::SubmissionRejected {
                status: status.as_u16(),
            });
        }
        let response: SubmitResponse =
            serde_json::from_slice(&body).map_err(|error| RelayerError::AmbiguousSubmission {
                detail: format!("successful response could not be decoded: {error}"),
            })?;
        Ok(RelayerSubmission {
            transaction_id: RelayerTransactionId::parse(response.transaction_id).map_err(
                |error| RelayerError::AmbiguousSubmission {
                    detail: format!("successful response carried invalid transactionID: {error}"),
                },
            )?,
            state: RelayerTransactionState::parse(&response.state).map_err(|error| {
                RelayerError::AmbiguousSubmission {
                    detail: error.to_string(),
                }
            })?,
        })
    }

    pub async fn poll(
        &self,
        transaction_id: &RelayerTransactionId,
        prepared: &PreparedRelayerEnvelope,
    ) -> Result<RelayerPollOutcome, RelayerError> {
        if content_hash(prepared.signed_envelope()) != prepared.signed_envelope_hash {
            return Err(RelayerError::CorruptDurableEnvelope);
        }
        let Some(transaction) = self.transaction_record(transaction_id).await? else {
            return Ok(RelayerPollOutcome::Pending {
                state: RelayerTransactionState::New,
            });
        };
        verify_transaction_record(transaction_id, prepared, &transaction)?;
        transaction.transaction_poll_outcome()
    }

    async fn transaction_record(
        &self,
        transaction_id: &RelayerTransactionId,
    ) -> Result<Option<TransactionResponse>, RelayerError> {
        let transactions: Vec<TransactionResponse> = self
            .get_json("transaction", &[("id", transaction_id.as_str().to_owned())])
            .await?;
        match transactions.as_slice() {
            [] => Ok(None),
            [_] => Ok(transactions.into_iter().next()),
            _ => Err(RelayerError::InvalidResponse {
                operation: "GET /transaction",
                detail: "relayer returned multiple records for one transactionID".to_owned(),
            }),
        }
    }

    async fn wallet_deployed(
        &self,
        funder: Address,
        wallet_type: Option<&'static str>,
    ) -> Result<bool, RelayerError> {
        let mut params = vec![("address", funder.to_checksum(None))];
        if let Some(wallet_type) = wallet_type {
            params.push(("type", wallet_type.to_owned()));
        }
        self.get_json("deployed", &params)
            .await
            .map(|response: DeployedResponse| response.deployed)
    }

    async fn safe_nonce(&self, signer: Address) -> Result<u64, RelayerError> {
        let response: NonceResponse = self
            .get_json(
                "nonce",
                &[
                    ("address", signer.to_checksum(None)),
                    ("type", "SAFE".to_owned()),
                ],
            )
            .await?;
        parse_u64("safe nonce", &response.nonce)
    }

    async fn wallet_nonce(&self, signer: Address) -> Result<u64, RelayerError> {
        let response: NonceResponse = self
            .get_json(
                "nonce",
                &[
                    ("address", signer.to_checksum(None)),
                    ("type", "WALLET".to_owned()),
                ],
            )
            .await?;
        parse_u64("deposit wallet nonce", &response.nonce)
    }

    async fn proxy_relay_payload(
        &self,
        signer: Address,
    ) -> Result<ProxyRelayPayload, RelayerError> {
        let response: RelayPayloadResponse = self
            .get_json(
                "relay-payload",
                &[
                    ("address", signer.to_checksum(None)),
                    ("type", "PROXY".to_owned()),
                ],
            )
            .await?;
        Ok(ProxyRelayPayload {
            relay: Address::from_str(&response.address).map_err(|error| {
                RelayerError::InvalidResponse {
                    operation: "GET /relay-payload",
                    detail: format!("invalid relay address: {error}"),
                }
            })?,
            nonce: parse_u64("proxy nonce", &response.nonce)?,
        })
    }

    async fn get_json<R: DeserializeOwned>(
        &self,
        path: &'static str,
        params: &[(&'static str, String)],
    ) -> Result<R, RelayerError> {
        let url = self.endpoint(path)?;
        let response = self
            .authenticated(self.http.get(url))?
            .query(params)
            .send()
            .await
            .map_err(|error| RelayerError::TransportCall {
                operation: path,
                detail: error.to_string(),
            })?;
        parse_response(path, response).await
    }

    fn authenticated(&self, request: RequestBuilder) -> Result<RequestBuilder, RelayerError> {
        let api_key = self
            .config
            .api_key()
            .ok_or(RelayerError::MissingCredentials)?;
        let owner = self
            .config
            .api_key_address()
            .ok_or(RelayerError::MissingCredentials)?;
        Ok(request
            .header("RELAYER_API_KEY", api_key)
            .header("RELAYER_API_KEY_ADDRESS", owner))
    }

    fn endpoint(&self, path: &'static str) -> Result<Url, RelayerError> {
        self.base_url
            .join(path)
            .map_err(|error| RelayerError::InvalidConfiguration {
                detail: error.to_string(),
            })
    }
}

/// Alloy-backed Polygon reader for relayer body preparation.
pub struct AlloyRelayerPreparationRpc {
    provider: DynProvider,
}

impl AlloyRelayerPreparationRpc {
    pub fn connect(config: &OnchainConfig) -> Result<Self, RelayerError> {
        let rpc_url =
            Url::parse(config.rpc_url()).map_err(|error| RelayerError::InvalidConfiguration {
                detail: error.to_string(),
            })?;
        let http_client = Client::builder()
            .timeout(Duration::from_millis(config.rpc_timeout_ms))
            .build()
            .map_err(|error| RelayerError::InvalidConfiguration {
                detail: error.to_string(),
            })?;
        let transport = Http::with_client(http_client, rpc_url);
        let rpc_client = RpcClient::new(transport, false);
        Ok(Self {
            provider: ProviderBuilder::new().connect_client(rpc_client).erased(),
        })
    }
}

#[async_trait]
impl RelayerPreparationRpc for AlloyRelayerPreparationRpc {
    async fn chain_id(&self) -> Result<u64, RelayerError> {
        self.provider
            .get_chain_id()
            .await
            .map_err(|error| preparation_error("eth_chainId", &error))
    }

    async fn canonical_head(&self) -> Result<EoaPreparedBlock, RelayerError> {
        let block: Option<CanonicalBlockResponse> = self
            .provider
            .raw_request(
                "eth_getBlockByNumber".into(),
                (BlockNumberOrTag::Latest, false),
            )
            .await
            .map_err(|error| preparation_error("eth_getBlockByNumber(latest)", &error))?;
        let block = block.ok_or_else(|| RelayerError::PreparationCall {
            operation: "eth_getBlockByNumber(latest)",
            detail: "canonical latest block was absent".to_owned(),
        })?;
        Ok(EoaPreparedBlock {
            number: block.number,
            hash: (block.hash).into_evm_block_hash()?,
        })
    }

    async fn estimate_proxy_gas(
        &self,
        signer: Address,
        factory: Address,
        calldata: &[u8],
    ) -> Result<Option<u64>, RelayerError> {
        Ok(self
            .provider
            .estimate_gas(
                TransactionRequest::default()
                    .from(signer)
                    .to(factory)
                    .input(TransactionInput::new(Bytes::copy_from_slice(calldata))),
            )
            .await
            .ok())
    }

    async fn canonical_hash(
        &self,
        block_number: u64,
    ) -> Result<Option<EvmBlockHash>, RelayerError> {
        let block: Option<CanonicalBlockResponse> = self
            .provider
            .raw_request(
                "eth_getBlockByNumber".into(),
                (BlockNumberOrTag::Number(block_number), false),
            )
            .await
            .map_err(|error| {
                preparation_error("eth_getBlockByNumber(canonical recheck)", &error)
            })?;
        block
            .map(|value| (value.hash).into_evm_block_hash())
            .transpose()
            .map_err(Into::into)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FrozenSafeProxyRequest {
    #[serde(rename = "type")]
    pub(crate) tx_type: String,
    pub(crate) from: String,
    pub(crate) to: String,
    pub(crate) proxy_wallet: String,
    pub(crate) data: String,
    pub(crate) nonce: String,
    pub(crate) signature: String,
    pub(crate) signature_params: SignatureParamsBody,
    pub(crate) metadata: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SignatureParamsBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) gas_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) relayer_fee: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) gas_limit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) relay_hub: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) relay: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) operation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) safe_txn_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) base_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) gas_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) refund_receiver: Option<String>,
}

/// Canonical `WALLET` request body. The same immutable bytes are submitted,
/// polled, and later matched against the factory's mined `proxy` call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FrozenDepositWalletRequest {
    #[serde(rename = "type")]
    pub(crate) tx_type: String,
    pub(crate) from: String,
    pub(crate) to: String,
    pub(crate) nonce: String,
    pub(crate) signature: String,
    pub(crate) deposit_wallet_params: FrozenDepositWalletParams,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FrozenDepositWalletParams {
    pub(crate) deposit_wallet: String,
    pub(crate) deadline: String,
    pub(crate) calls: Vec<FrozenDepositWalletCall>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FrozenDepositWalletCall {
    pub(crate) target: String,
    pub(crate) value: String,
    pub(crate) data: String,
}

#[derive(Debug, Deserialize)]
struct SubmitResponse {
    #[serde(rename = "transactionID")]
    transaction_id: String,
    state: String,
}

#[derive(Debug, Deserialize)]
struct TransactionResponse {
    #[serde(rename = "transactionID")]
    transaction_id: String,
    state: String,
    #[serde(rename = "transactionHash")]
    transaction_hash: Option<String>,
    from: String,
    to: String,
    #[serde(rename = "proxyAddress")]
    proxy_address: String,
    data: String,
    nonce: String,
    value: String,
    signature: String,
    #[serde(rename = "type")]
    tx_type: String,
    owner: String,
    metadata: String,
    #[serde(rename = "createdAt")]
    created_at: DateTime<Utc>,
    #[serde(rename = "updatedAt")]
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct DeployedResponse {
    deployed: bool,
}

#[derive(Debug, Deserialize)]
struct NonceResponse {
    nonce: String,
}

#[derive(Debug, Deserialize)]
struct RelayPayloadResponse {
    address: String,
    nonce: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalBlockResponse {
    number: u64,
    hash: B256,
}

struct ProxyRelayPayload {
    relay: Address,
    nonce: u64,
}

fn verify_transaction_record(
    transaction_id: &RelayerTransactionId,
    prepared: &PreparedRelayerEnvelope,
    record: &TransactionResponse,
) -> Result<(), RelayerError> {
    if record.transaction_id != transaction_id.as_str()
        || record.updated_at < record.created_at
        || !record.value.is_empty()
    {
        return record_mismatch("identity, timestamp, or value");
    }
    match prepared.wallet_kind {
        ExecutionWalletKind::GnosisSafe | ExecutionWalletKind::Proxy => {
            let body: FrozenSafeProxyRequest = serde_json::from_slice(prepared.signed_envelope())
                .map_err(|_| RelayerError::CorruptDurableEnvelope)?;
            verify_safe_scope(prepared, &body)?;
            if record.tx_type != body.tx_type
                || !same_address(&record.from, &body.from)
                || !same_address(&record.owner, &body.from)
                || !same_address(&record.to, &body.to)
                || !same_address(&record.proxy_address, &body.proxy_wallet)
                || record.data != body.data
                || record.nonce != body.nonce
                || record.signature != body.signature
                || record.metadata != body.metadata
            {
                return record_mismatch("Safe/Proxy record does not match frozen request");
            }
        }
        ExecutionWalletKind::DepositWallet => {
            let body: FrozenDepositWalletRequest =
                serde_json::from_slice(prepared.signed_envelope())
                    .map_err(|_| RelayerError::CorruptDurableEnvelope)?;
            let factory_calldata = verify_deposit_scope(prepared, &body)?;
            if record.tx_type != body.tx_type
                || !same_address(&record.from, &body.from)
                || !same_address(&record.owner, &body.from)
                || !same_address(&record.to, &body.to)
                || !same_address(
                    &record.proxy_address,
                    &body.deposit_wallet_params.deposit_wallet,
                )
                || record.data != hex_prefixed(&factory_calldata)
                || record.nonce != body.nonce
                || record.signature != body.signature
                || !record.metadata.is_empty()
            {
                return record_mismatch("Deposit Wallet record does not match frozen request");
            }
        }
        ExecutionWalletKind::Eoa => {
            return record_mismatch("EOA cannot own a relayer submission");
        }
    }
    Ok(())
}

impl TransactionResponse {
    fn transaction_poll_outcome(&self) -> Result<RelayerPollOutcome, RelayerError> {
        let state = RelayerTransactionState::parse(&self.state)?;
        if state.terminal_failure() {
            return Ok(RelayerPollOutcome::TerminalFailure { state });
        }
        let chain_hash = self
            .transaction_hash
            .as_deref()
            .filter(|value| !value.is_empty());
        if let Some(chain_hash) = chain_hash {
            let transaction_hash = EvmTransactionHash::parse(chain_hash).map_err(|error| {
                RelayerError::InvalidResponse {
                    operation: "GET /transaction",
                    detail: format!("invalid transactionHash: {error}"),
                }
            })?;
            return Ok(RelayerPollOutcome::ChainHashObserved {
                state,
                transaction_hash,
            });
        }
        if state.requires_chain_hash() {
            return Err(RelayerError::InvalidResponse {
                operation: "GET /transaction",
                detail: "mined/confirmed state omitted transactionHash".to_owned(),
            });
        }
        Ok(RelayerPollOutcome::Pending { state })
    }
}

fn verify_safe_scope(
    prepared: &PreparedRelayerEnvelope,
    body: &FrozenSafeProxyRequest,
) -> Result<(), RelayerError> {
    let data = decode_wire_hex(&body.data)?;
    let (target, calldata) = match body.tx_type.as_str() {
        "SAFE" => (wire_address(&body.to)?, data),
        "PROXY" => {
            let proxy =
                proxyCall::abi_decode(&data).map_err(|_| RelayerError::CorruptDurableEnvelope)?;
            let [inner] = proxy.calls.as_slice() else {
                return Err(RelayerError::CorruptDurableEnvelope);
            };
            if inner.typeCode != PROXY_CALL_TYPE || !inner.value.is_zero() {
                return Err(RelayerError::CorruptDurableEnvelope);
            }
            (inner.to, inner.data.to_vec())
        }
        _ => return Err(RelayerError::CorruptDurableEnvelope),
    };
    if !same_address(&body.proxy_wallet, prepared.funder.as_str())
        || target
            .into_evm_address()
            .map_err(|_| RelayerError::CorruptDurableEnvelope)?
            != prepared.call_target
        || !calldata_hash_matches(&calldata, &prepared.calldata_hash)
        || body.nonce != prepared.nonce.as_str()
    {
        return Err(RelayerError::CorruptDurableEnvelope);
    }
    Ok(())
}

fn verify_deposit_scope(
    prepared: &PreparedRelayerEnvelope,
    body: &FrozenDepositWalletRequest,
) -> Result<Vec<u8>, RelayerError> {
    let params = &body.deposit_wallet_params;
    let [call] = params.calls.as_slice() else {
        return Err(RelayerError::CorruptDurableEnvelope);
    };
    let calldata = decode_wire_hex(&call.data)?;
    let nonce = parse_u64("deposit wallet nonce", &body.nonce)?;
    let deadline = parse_u64("deposit wallet deadline", &params.deadline)?;
    let signature = decode_wire_hex(&body.signature)?;
    if body.tx_type != "WALLET"
        || body.nonce != prepared.nonce.as_str()
        || !same_address(&body.to, DEPOSIT_WALLET_FACTORY)
        || !same_address(&params.deposit_wallet, prepared.funder.as_str())
        || !same_address(&call.target, prepared.call_target.as_str())
        || call.value != "0"
        || !calldata_hash_matches(&calldata, &prepared.calldata_hash)
    {
        return Err(RelayerError::CorruptDurableEnvelope);
    }
    let factory_call = DepositWalletProxyCall {
        batches: vec![DepositWalletBatch {
            wallet: wire_address(&params.deposit_wallet)?,
            nonce: U256::from(nonce),
            deadline: U256::from(deadline),
            calls: vec![DepositWalletCall {
                target: wire_address(&call.target)?,
                value: U256::ZERO,
                data: Bytes::from(calldata),
            }],
        }],
        signatures: vec![Bytes::from(signature)],
    }
    .abi_encode();
    Ok(factory_call)
}

fn same_address(left: &str, right: &str) -> bool {
    matches!(
        (Address::from_str(left), Address::from_str(right)),
        (Ok(left), Ok(right)) if left == right
    )
}

fn wire_address(value: &str) -> Result<Address, RelayerError> {
    Address::from_str(value).map_err(|_| RelayerError::CorruptDurableEnvelope)
}

fn decode_wire_hex(value: &str) -> Result<Vec<u8>, RelayerError> {
    let encoded = value
        .strip_prefix("0x")
        .ok_or(RelayerError::CorruptDurableEnvelope)?;
    hex::decode(encoded).map_err(|_| RelayerError::CorruptDurableEnvelope)
}

fn calldata_hash_matches(calldata: &[u8], expected: &EvmCalldataHash) -> bool {
    format!("{:#x}", keccak256(calldata)) == expected.as_str()
}

fn record_mismatch<T>(detail: &'static str) -> Result<T, RelayerError> {
    Err(RelayerError::InvalidResponse {
        operation: "GET /transaction",
        detail: detail.to_owned(),
    })
}

fn build_safe_body(
    signer: &OrderSigner,
    topology: &WalletTopology,
    target_text: &str,
    target: Address,
    calldata: &[u8],
    nonce: u64,
) -> Result<FrozenSafeProxyRequest, RelayerError> {
    let safe_tx = SafeTx {
        to: target,
        value: U256::ZERO,
        data: Bytes::copy_from_slice(calldata),
        operation: SAFE_OPERATION_CALL,
        safeTxGas: U256::ZERO,
        baseGas: U256::ZERO,
        gasPrice: U256::ZERO,
        gasToken: Address::ZERO,
        refundReceiver: Address::ZERO,
        nonce: U256::from(nonce),
    };
    let domain = eip712_domain! {
        chain_id: POLYGON_CHAIN_ID,
        verifying_contract: topology.funder,
    };
    let digest = safe_tx.eip712_signing_hash(&domain);
    let signature = sign_personal_digest(signer, &digest, 31)?;
    Ok(FrozenSafeProxyRequest {
        tx_type: "SAFE".to_owned(),
        from: topology.signer.to_checksum(None),
        to: target_text.to_owned(),
        proxy_wallet: topology.funder.to_checksum(None),
        data: hex_prefixed(calldata),
        nonce: nonce.to_string(),
        signature,
        signature_params: SignatureParamsBody {
            gas_price: Some("0".to_owned()),
            operation: Some(SAFE_OPERATION_CALL.to_string()),
            safe_txn_gas: Some("0".to_owned()),
            base_gas: Some("0".to_owned()),
            gas_token: Some(Address::ZERO.to_checksum(None)),
            refund_receiver: Some(Address::ZERO.to_checksum(None)),
            ..SignatureParamsBody::default()
        },
        metadata: String::new(),
    })
}

fn build_proxy_body(
    signer: &OrderSigner,
    topology: &WalletTopology,
    factory: Address,
    proxy_calldata: &[u8],
    payload: &ProxyRelayPayload,
    gas_limit: u64,
) -> Result<FrozenSafeProxyRequest, RelayerError> {
    let relay_hub = official_address(RELAY_HUB)?;
    let digest = proxy_relay_hash(&ProxyRelayHashArgs {
        from: topology.signer,
        to: factory,
        data: proxy_calldata,
        gas_limit: U256::from(gas_limit),
        nonce: U256::from(payload.nonce),
        relay_hub,
        relay: payload.relay,
    });
    let signature = sign_personal_digest(signer, &digest, 27)?;
    Ok(FrozenSafeProxyRequest {
        tx_type: "PROXY".to_owned(),
        from: topology.signer.to_checksum(None),
        to: factory.to_checksum(None),
        proxy_wallet: topology.funder.to_checksum(None),
        data: hex_prefixed(proxy_calldata),
        nonce: payload.nonce.to_string(),
        signature,
        signature_params: SignatureParamsBody {
            gas_price: Some("0".to_owned()),
            relayer_fee: Some("0".to_owned()),
            gas_limit: Some(gas_limit.to_string()),
            relay_hub: Some(relay_hub.to_checksum(None)),
            relay: Some(payload.relay.to_checksum(None)),
            ..SignatureParamsBody::default()
        },
        metadata: String::new(),
    })
}

fn build_deposit_wallet_body(
    signer: &OrderSigner,
    topology: &WalletTopology,
    call: &PreparedSettlementCall,
    nonce: u64,
    deadline: u64,
) -> Result<FrozenDepositWalletRequest, RelayerError> {
    let target = (call.call_target()).into_alloy_address()?;
    let batch = DepositWalletBatch {
        wallet: topology.funder,
        nonce: U256::from(nonce),
        deadline: U256::from(deadline),
        calls: vec![DepositWalletCall {
            target,
            value: U256::ZERO,
            data: Bytes::copy_from_slice(call.calldata()),
        }],
    };
    let domain = eip712_domain! {
        name: "DepositWallet",
        version: "1",
        chain_id: POLYGON_CHAIN_ID,
        verifying_contract: topology.funder,
    };
    let signature = signer
        .inner()
        .sign_hash_sync(&batch.eip712_signing_hash(&domain))
        .map_err(|error| RelayerError::Signing {
            detail: error.to_string(),
        })?;
    Ok(FrozenDepositWalletRequest {
        tx_type: "WALLET".to_owned(),
        from: topology.signer.to_checksum(None),
        to: official_address(DEPOSIT_WALLET_FACTORY)?.to_checksum(None),
        nonce: nonce.to_string(),
        signature: encode_signature(&signature, 27),
        deposit_wallet_params: FrozenDepositWalletParams {
            deposit_wallet: topology.funder.to_checksum(None),
            deadline: deadline.to_string(),
            calls: vec![FrozenDepositWalletCall {
                target: call.call_target().as_str().to_owned(),
                value: "0".to_owned(),
                data: hex_prefixed(call.calldata()),
            }],
        },
    })
}

struct ProxyRelayHashArgs<'a> {
    from: Address,
    to: Address,
    data: &'a [u8],
    gas_limit: U256,
    nonce: U256,
    relay_hub: Address,
    relay: Address,
}

fn proxy_relay_hash(args: &ProxyRelayHashArgs<'_>) -> B256 {
    let mut encoded = Vec::with_capacity(4 + 20 + 20 + args.data.len() + 32 * 4 + 20 + 20);
    encoded.extend_from_slice(b"rlx:");
    encoded.extend_from_slice(args.from.as_slice());
    encoded.extend_from_slice(args.to.as_slice());
    encoded.extend_from_slice(args.data);
    encoded.extend_from_slice(&U256::ZERO.to_be_bytes::<32>());
    encoded.extend_from_slice(&U256::ZERO.to_be_bytes::<32>());
    encoded.extend_from_slice(&args.gas_limit.to_be_bytes::<32>());
    encoded.extend_from_slice(&args.nonce.to_be_bytes::<32>());
    encoded.extend_from_slice(args.relay_hub.as_slice());
    encoded.extend_from_slice(args.relay.as_slice());
    keccak256(encoded)
}

fn sign_personal_digest(
    signer: &OrderSigner,
    digest: &B256,
    base_v: u8,
) -> Result<String, RelayerError> {
    let signature = signer
        .inner()
        .sign_message_sync(digest.as_slice())
        .map_err(|error| RelayerError::Signing {
            detail: error.to_string(),
        })?;
    Ok(encode_signature(&signature, base_v))
}

fn encode_signature(signature: &Signature, base_v: u8) -> String {
    let parity = u8::from(signature.v());
    let mut bytes = [0_u8; 65];
    bytes[..32].copy_from_slice(&signature.r().to_be_bytes::<32>());
    bytes[32..64].copy_from_slice(&signature.s().to_be_bytes::<32>());
    bytes[64] = base_v + parity;
    format!("0x{}", hex::encode(bytes))
}

fn verify_relayer_identity(
    signer: &OrderSigner,
    topology: &WalletTopology,
    call: &PreparedSettlementCall,
) -> Result<(), RelayerError> {
    if topology.kind == ExecutionWalletKind::Eoa {
        return Err(RelayerError::WrongWalletKind);
    }
    if topology.signer != signer.address()
        || call.funder().as_str() != format!("{:#x}", topology.funder)
    {
        return Err(RelayerError::WalletIdentityMismatch);
    }
    let derived_funder = match topology.kind {
        ExecutionWalletKind::Proxy => derive_proxy_wallet(topology.signer, POLYGON_CHAIN_ID),
        ExecutionWalletKind::GnosisSafe => derive_safe_wallet(topology.signer, POLYGON_CHAIN_ID),
        ExecutionWalletKind::DepositWallet => {
            topology
                .contract_identity()
                .map_err(|error| RelayerError::InvalidPreparedValue {
                    detail: error.to_string(),
                })?;
            Some(topology.funder)
        }
        ExecutionWalletKind::Eoa => return Err(RelayerError::WrongWalletKind),
    }
    .ok_or(RelayerError::WalletIdentityMismatch)?;
    if derived_funder != topology.funder {
        return Err(RelayerError::WalletIdentityMismatch);
    }
    Ok(())
}

fn serialize_envelope(body: &impl Serialize) -> Result<Vec<u8>, RelayerError> {
    serde_json::to_vec(body).map_err(|error| RelayerError::Serialization {
        detail: error.to_string(),
    })
}

fn proxy_gas_limit(estimate: Option<u64>) -> Result<u64, RelayerError> {
    let Some(estimate) = estimate else {
        return Ok(PROXY_GAS_LIMIT_CAP);
    };
    estimate
        .checked_mul(PROXY_GAS_BUFFER_NUMERATOR)
        .and_then(|value| value.checked_add(PROXY_GAS_BUFFER_DENOMINATOR - 1))
        .map(|value| value / PROXY_GAS_BUFFER_DENOMINATOR)
        .filter(|value| *value > 0 && *value <= PROXY_GAS_LIMIT_CAP)
        .ok_or(RelayerError::ProxyGasLimitOutOfBounds {
            estimated: estimate,
        })
}

async fn parse_response<R: DeserializeOwned>(
    operation: &'static str,
    response: Response,
) -> Result<R, RelayerError> {
    let status = response.status();
    if !status.is_success() {
        return Err(RelayerError::TransportCall {
            operation,
            detail: format!("HTTP {status}"),
        });
    }
    response
        .json()
        .await
        .map_err(|error| RelayerError::InvalidResponse {
            operation,
            detail: error.to_string(),
        })
}

fn parse_u64(field: &'static str, value: &str) -> Result<u64, RelayerError> {
    let parsed = value
        .parse::<u64>()
        .map_err(|error| RelayerError::InvalidResponse {
            operation: field,
            detail: error.to_string(),
        })?;
    if parsed.to_string() != value {
        return Err(RelayerError::InvalidResponse {
            operation: field,
            detail: "value is not canonical base-10".to_owned(),
        });
    }
    Ok(parsed)
}

fn official_address(value: &str) -> Result<Address, RelayerError> {
    Address::from_str(value).map_err(|error| RelayerError::InvalidConfiguration {
        detail: error.to_string(),
    })
}

fn content_hash(bytes: &[u8]) -> ContentHash {
    ContentHash::from_bytes(*blake3::hash(bytes).as_bytes())
}

fn hex_prefixed(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn ensure_trailing_slash(value: &str) -> String {
    if value.ends_with('/') {
        value.to_owned()
    } else {
        format!("{value}/")
    }
}

fn preparation_error(operation: &'static str, error: &impl ToString) -> RelayerError {
    RelayerError::PreparationCall {
        operation,
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use alloy::primitives::Address;
    use async_trait::async_trait;
    use polymarket_client_sdk_v2::{
        clob::types::SignatureType, derive_proxy_wallet, derive_safe_wallet,
    };
    use quant_pivot_models::{
        config::RelayerConfig,
        enums::{quant::ExecutionWalletKind, settlement::SettlementRoute},
        types::{EvmAddress, EvmBlockHash, EvmUint256, MarketId, RelayerTransactionId},
    };
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, header, method, path, query_param},
    };

    use super::{
        EoaPreparedBlock, FrozenDepositWalletRequest, PreparedSettlementCall, RelayerError,
        RelayerPollOutcome, RelayerPreparationRpc, RelayerRequestBuilder, RelayerTransactionState,
        RelayerTransport, build_deposit_wallet_body, verify_deposit_scope,
    };
    use crate::{
        keystore::OrderSigner,
        settlement::{
            adapter::{SettlementAdapterGateway, verified_redeem_fixture},
            contracts::verified_deployment_fixture_at,
        },
        wallet::{WalletTopology, derive_deposit_wallet_address},
    };

    struct FakeRpc {
        block: EoaPreparedBlock,
        gas_estimate: Option<u64>,
    }

    #[async_trait]
    impl RelayerPreparationRpc for FakeRpc {
        async fn chain_id(&self) -> Result<u64, RelayerError> {
            Ok(137)
        }

        async fn canonical_head(&self) -> Result<EoaPreparedBlock, RelayerError> {
            Ok(self.block.clone())
        }

        async fn estimate_proxy_gas(
            &self,
            _signer: Address,
            _factory: Address,
            _calldata: &[u8],
        ) -> Result<Option<u64>, RelayerError> {
            Ok(self.gas_estimate)
        }

        async fn canonical_hash(
            &self,
            _block_number: u64,
        ) -> Result<Option<EvmBlockHash>, RelayerError> {
            Ok(Some(self.block.hash.clone()))
        }
    }

    #[tokio::test]
    async fn safe_body_matches_byte() {
        let server = MockServer::start().await;
        let signer = OrderSigner::from_bytes(&[0x21; 32]).expect("test signer");
        let topology = topology(&signer, ExecutionWalletKind::GnosisSafe);
        mount_get(
            &server,
            "deployed",
            "address",
            &topology.funder.to_checksum(None),
            json!({"deployed": true}),
        )
        .await;
        mount_typed_get(
            &server,
            "nonce",
            &topology.signer.to_checksum(None),
            "SAFE",
            json!({"nonce": "7"}),
        )
        .await;
        let transport = transport(&server, &topology, 500);
        let (call, rpc) = call_and_rpc(&topology, SettlementRoute::StandardV2, Some(100_000));
        let prepared = RelayerRequestBuilder
            .prepare(&transport, &rpc, &signer, &topology, &call)
            .await
            .expect("prepare safe request");
        let body: Value = serde_json::from_slice(prepared.signed_envelope()).expect("safe body");
        let official: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/relayer/official_safe_submit.json"
        ))
        .expect("official Safe fixture");
        assert_eq!(body, official);
        assert_eq!(body["type"], "SAFE");
        assert_eq!(body["to"], call.call_target().as_str());
        assert_eq!(body["proxyWallet"], topology.funder.to_checksum(None));
        assert_eq!(body["nonce"], "7");
        assert_eq!(body["signatureParams"]["operation"], "0");
        assert_eq!(body["signatureParams"]["safeTxnGas"], "0");
        assert_eq!(body["signature"].as_str().expect("signature").len(), 132);

        Mock::given(method("POST"))
            .and(path("/submit"))
            .and(header("RELAYER_API_KEY", "test-relayer-key"))
            .and(header(
                "RELAYER_API_KEY_ADDRESS",
                format!("{:#x}", topology.signer),
            ))
            .and(body_json(&body))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "transactionID": "relay_safe_7",
                "state": "STATE_NEW"
            })))
            .expect(2)
            .mount(&server)
            .await;

        let first = transport.submit(&prepared).await.expect("submit");
        let replay = transport.submit(&prepared).await.expect("replay");
        assert_eq!(first, replay);
        assert_eq!(first.state, RelayerTransactionState::New);
        assert_eq!(first.transaction_id.as_str(), "relay_safe_7");
    }

    #[test]
    fn official_wire_matches_body() {
        let lock = include_str!("../../tests/fixtures/relayer/source.lock");
        assert_eq!(
            lock_value(lock, "repository"),
            "https://github.com/Polymarket/builder-relayer-client"
        );
        assert_eq!(
            lock_value(lock, "commit"),
            "9122f6fb1856f1ecfe4406685bfa19a2c5a7b290"
        );
        assert_relayer_fixture_hash(
            lock,
            "safe_fixture_sha256",
            include_bytes!("../../tests/fixtures/relayer/official_safe_submit.json"),
        );
        assert_relayer_fixture_hash(
            lock,
            "proxy_fixture_sha256",
            include_bytes!("../../tests/fixtures/relayer/official_proxy_submit.json"),
        );
        assert_relayer_fixture_hash(
            lock,
            "deposit_wallet_fixture_sha256",
            include_bytes!("../../tests/fixtures/relayer/official_deposit_wallet_submit.json"),
        );
    }

    fn assert_relayer_fixture_hash(lock: &str, key: &str, content: &[u8]) {
        assert_eq!(
            lock_value(lock, key),
            hex::encode(Sha256::digest(content)),
            "{key} must match its reviewed wire fixture"
        );
    }

    fn lock_value<'a>(lock: &'a str, key: &str) -> &'a str {
        lock.lines()
            .find_map(|line| {
                line.strip_prefix(key)
                    .and_then(|value| value.strip_prefix('='))
            })
            .unwrap_or_else(|| panic!("{key} is missing from source.lock"))
    }

    #[tokio::test]
    async fn proxy_body_uses_fallback() {
        let server = MockServer::start().await;
        let signer = OrderSigner::from_bytes(&[0x31; 32]).expect("test signer");
        let topology = topology(&signer, ExecutionWalletKind::Proxy);
        mount_typed_get(
            &server,
            "relay-payload",
            &topology.signer.to_checksum(None),
            "PROXY",
            json!({
                "address": "0x7777777777777777777777777777777777777777",
                "nonce": "9"
            }),
        )
        .await;
        let transport = transport(&server, &topology, 500);
        let (call, rpc) = call_and_rpc(&topology, SettlementRoute::NegRiskV2, Some(500_001));
        let prepared = RelayerRequestBuilder
            .prepare(&transport, &rpc, &signer, &topology, &call)
            .await
            .expect("prepare proxy request");
        let body: Value = serde_json::from_slice(prepared.signed_envelope()).expect("proxy body");
        let official: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/relayer/official_proxy_submit.json"
        ))
        .expect("official Proxy fixture");
        assert_eq!(body, official);
        assert_eq!(prepared.gas_limit().map(EvmUint256::as_str), Some("600002"));
        assert_eq!(body["type"], "PROXY");
        assert_eq!(body["to"], "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052");
        assert_eq!(body["signatureParams"]["gasLimit"], "600002");
        assert_eq!(body["signatureParams"]["gasPrice"], "0");
        assert_eq!(body["signatureParams"]["relayerFee"], "0");
        assert_ne!(body["data"], format!("0x{}", hex::encode(call.calldata())));

        let (fallback_call, fallback_rpc) =
            call_and_rpc(&topology, SettlementRoute::NegRiskV2, None);
        let fallback = RelayerRequestBuilder
            .prepare(
                &transport,
                &fallback_rpc,
                &signer,
                &topology,
                &fallback_call,
            )
            .await
            .expect("bounded fallback");
        assert_eq!(
            fallback.gas_limit().map(EvmUint256::as_str),
            Some("10000000")
        );
    }

    #[test]
    fn deposit_wallet_matches_fixture() {
        let signer = OrderSigner::from_bytes(&[0x51; 32]).expect("test signer");
        let topology = topology(&signer, ExecutionWalletKind::DepositWallet);
        let (call, _) = call_and_rpc(&topology, SettlementRoute::StandardV2, None);
        let body = build_deposit_wallet_body(&signer, &topology, &call, 13, 2_000_000_000)
            .expect("build deposit wallet request");
        let actual = serde_json::to_value(body).expect("serialize wallet body");
        let expected: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/relayer/official_deposit_wallet_submit.json"
        ))
        .expect("official Deposit Wallet fixture");
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn deposit_wallet_prepare_record() {
        let server = MockServer::start().await;
        let signer = OrderSigner::from_bytes(&[0x52; 32]).expect("test signer");
        let topology = topology(&signer, ExecutionWalletKind::DepositWallet);
        mount_typed_get(
            &server,
            "deployed",
            &topology.funder.to_checksum(None),
            "WALLET",
            json!({"deployed": true}),
        )
        .await;
        mount_typed_get(
            &server,
            "nonce",
            &topology.signer.to_checksum(None),
            "WALLET",
            json!({"nonce": "17"}),
        )
        .await;
        let transport = transport(&server, &topology, 500);
        let (call, rpc) = call_and_rpc(&topology, SettlementRoute::NegRiskV2, None);
        let prepared = RelayerRequestBuilder
            .prepare(&transport, &rpc, &signer, &topology, &call)
            .await
            .expect("prepare Deposit Wallet request");
        let body: FrozenDepositWalletRequest =
            serde_json::from_slice(prepared.signed_envelope()).expect("wallet body");
        assert_eq!(body.tx_type, "WALLET");
        assert_eq!(body.nonce, "17");
        assert_eq!(body.deposit_wallet_params.calls.len(), 1);
        assert_eq!(prepared.nonce().as_str(), "17");
        let factory_calldata = verify_deposit_scope(&prepared, &body).expect("factory calldata");
        Mock::given(method("GET"))
            .and(path("/transaction"))
            .and(query_param("id", "relay_wallet_17"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{
                "transactionID": "relay_wallet_17",
                "transactionHash": format!("0x{}", "cd".repeat(32)),
                "from": body.from,
                "to": body.to,
                "proxyAddress": body.deposit_wallet_params.deposit_wallet,
                "data": format!("0x{}", hex::encode(factory_calldata)),
                "nonce": body.nonce,
                "value": "",
                "signature": body.signature,
                "state": "STATE_MINED",
                "type": body.tx_type,
                "owner": topology.signer.to_checksum(None),
                "metadata": "",
                "createdAt": "2026-07-23T00:00:00Z",
                "updatedAt": "2026-07-23T00:00:01Z"
            }])))
            .mount(&server)
            .await;
        let transaction_id = RelayerTransactionId::parse("relay_wallet_17").expect("relayer ID");
        assert!(matches!(
            transport.poll(&transaction_id, &prepared).await,
            Ok(RelayerPollOutcome::ChainHashObserved {
                state: RelayerTransactionState::Mined,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn submit_timeout_adds_hash() {
        let server = MockServer::start().await;
        let signer = OrderSigner::from_bytes(&[0x41; 32]).expect("test signer");
        let topology = topology(&signer, ExecutionWalletKind::GnosisSafe);
        mount_get(
            &server,
            "deployed",
            "address",
            &topology.funder.to_checksum(None),
            json!({"deployed": true}),
        )
        .await;
        mount_typed_get(
            &server,
            "nonce",
            &topology.signer.to_checksum(None),
            "SAFE",
            json!({"nonce": "11"}),
        )
        .await;
        let transport = transport(&server, &topology, 30);
        let (call, rpc) = call_and_rpc(&topology, SettlementRoute::StandardV2, Some(100_000));
        let prepared = RelayerRequestBuilder
            .prepare(&transport, &rpc, &signer, &topology, &call)
            .await
            .expect("prepare safe request");
        let body: Value = serde_json::from_slice(prepared.signed_envelope()).expect("safe body");
        Mock::given(method("POST"))
            .and(path("/submit"))
            .and(body_json(&body))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(100))
                    .set_body_json(json!({
                        "transactionID": "relay_timeout_11",
                        "state": "STATE_NEW"
                    })),
            )
            .mount(&server)
            .await;
        let error = transport
            .submit(&prepared)
            .await
            .expect_err("timeout is ambiguous");
        assert!(matches!(error, RelayerError::AmbiguousSubmission { .. }));

        Mock::given(method("GET"))
            .and(path("/transaction"))
            .and(query_param("id", "relay_timeout_11"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{
                "transactionID": "relay_timeout_11",
                "state": "STATE_EXECUTED",
                "transactionHash": format!("0x{}", "ab".repeat(32)),
                "from": body["from"],
                "to": body["to"],
                "proxyAddress": body["proxyWallet"],
                "data": body["data"],
                "nonce": body["nonce"],
                "value": "",
                "signature": body["signature"],
                "type": body["type"],
                "owner": body["from"],
                "metadata": body["metadata"],
                "createdAt": "2026-07-23T00:00:00Z",
                "updatedAt": "2026-07-23T00:00:01Z"
            }])))
            .mount(&server)
            .await;
        let transaction_id = RelayerTransactionId::parse("relay_timeout_11").expect("relayer ID");
        let outcome = transport
            .poll(&transaction_id, &prepared)
            .await
            .expect("poll");
        assert!(matches!(
            outcome,
            RelayerPollOutcome::ChainHashObserved {
                state: RelayerTransactionState::Executed,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn poll_rejects_wrong_records() {
        let server = MockServer::start().await;
        let signer = OrderSigner::from_bytes(&[0x42; 32]).expect("test signer");
        let topology = topology(&signer, ExecutionWalletKind::GnosisSafe);
        mount_get(
            &server,
            "deployed",
            "address",
            &topology.funder.to_checksum(None),
            json!({"deployed": true}),
        )
        .await;
        mount_typed_get(
            &server,
            "nonce",
            &topology.signer.to_checksum(None),
            "SAFE",
            json!({"nonce": "19"}),
        )
        .await;
        let transport = transport(&server, &topology, 500);
        let (call, rpc) = call_and_rpc(&topology, SettlementRoute::StandardV2, None);
        let prepared = RelayerRequestBuilder
            .prepare(&transport, &rpc, &signer, &topology, &call)
            .await
            .expect("prepare Safe request");
        let body: Value =
            serde_json::from_slice(prepared.signed_envelope()).expect("prepared body");

        let mut wrong_id = transaction_record_fixture("poll_wrong_id", &body);
        wrong_id["transactionID"] = Value::String("different_id".to_owned());
        mount_transaction(&server, "poll_wrong_id", json!([wrong_id])).await;

        let mut wrong_nonce = transaction_record_fixture("poll_wrong_nonce", &body);
        wrong_nonce["nonce"] = Value::String("20".to_owned());
        mount_transaction(&server, "poll_wrong_nonce", json!([wrong_nonce])).await;

        let mut wrong_signature = transaction_record_fixture("poll_wrong_signature", &body);
        wrong_signature["signature"] = Value::String(format!("0x{}", "01".repeat(65)));
        mount_transaction(&server, "poll_wrong_signature", json!([wrong_signature])).await;

        let duplicate = transaction_record_fixture("poll_multiple", &body);
        mount_transaction(
            &server,
            "poll_multiple",
            json!([duplicate.clone(), duplicate]),
        )
        .await;

        for transaction_id in [
            "poll_wrong_id",
            "poll_wrong_nonce",
            "poll_wrong_signature",
            "poll_multiple",
        ] {
            let transaction_id =
                RelayerTransactionId::parse(transaction_id).expect("fixture relayer ID");
            assert!(matches!(
                transport.poll(&transaction_id, &prepared).await,
                Err(RelayerError::InvalidResponse { .. })
            ));
        }
    }

    fn topology(signer: &OrderSigner, kind: ExecutionWalletKind) -> WalletTopology {
        let funder = match kind {
            ExecutionWalletKind::Eoa => signer.address(),
            ExecutionWalletKind::Proxy => {
                derive_proxy_wallet(signer.address(), 137).expect("proxy derivation")
            }
            ExecutionWalletKind::GnosisSafe => {
                derive_safe_wallet(signer.address(), 137).expect("safe derivation")
            }
            ExecutionWalletKind::DepositWallet => derive_deposit_wallet_address(signer.address()),
        };
        WalletTopology {
            kind,
            signer: signer.address(),
            owner: signer.address(),
            funder,
            signature_type: match kind {
                ExecutionWalletKind::Eoa => SignatureType::Eoa,
                ExecutionWalletKind::Proxy => SignatureType::Proxy,
                ExecutionWalletKind::GnosisSafe => SignatureType::GnosisSafe,
                ExecutionWalletKind::DepositWallet => SignatureType::Poly1271,
            },
        }
    }

    fn transport(
        server: &MockServer,
        topology: &WalletTopology,
        timeout_ms: u64,
    ) -> RelayerTransport {
        let config: RelayerConfig = serde_json::from_value(json!({
            "base_url": server.uri(),
            "api_key": "test-relayer-key",
            "api_key_address": format!("{:#x}", topology.signer),
            "request_timeout_ms": timeout_ms,
        }))
        .expect("relayer config");
        RelayerTransport::connect(&config, topology).expect("transport")
    }

    fn call_and_rpc(
        topology: &WalletTopology,
        route: SettlementRoute,
        gas_estimate: Option<u64>,
    ) -> (PreparedSettlementCall, FakeRpc) {
        let block_hash = EvmBlockHash::parse(format!("0x{}", "77".repeat(32))).expect("block hash");
        let target = match route {
            SettlementRoute::StandardV2 => "0xada100db00ca00073811820692005400218fce1f",
            SettlementRoute::NegRiskV2 => "0xada2005600dec949baf300f4c6120000bdb6eaab",
        };
        let capability = verified_deployment_fixture_at(
            route,
            EvmAddress::parse(target).expect("adapter"),
            EvmAddress::parse(format!("{:#x}", topology.funder)).expect("funder"),
            90_685_200,
            block_hash.clone(),
        );
        let redeem =
            verified_redeem_fixture(capability, &MarketId::new(format!("0x{}", "88".repeat(32))));
        let call = SettlementAdapterGateway.prepare_redeem(&redeem);
        (
            call,
            FakeRpc {
                block: EoaPreparedBlock {
                    number: 90_685_200,
                    hash: block_hash,
                },
                gas_estimate,
            },
        )
    }

    fn transaction_record_fixture(transaction_id: &str, body: &Value) -> Value {
        json!({
            "transactionID": transaction_id,
            "transactionHash": Value::Null,
            "from": body["from"],
            "to": body["to"],
            "proxyAddress": body["proxyWallet"],
            "data": body["data"],
            "nonce": body["nonce"],
            "value": "",
            "signature": body["signature"],
            "state": "STATE_NEW",
            "type": body["type"],
            "owner": body["from"],
            "metadata": body["metadata"],
            "createdAt": "2026-07-23T00:00:00Z",
            "updatedAt": "2026-07-23T00:00:01Z"
        })
    }

    async fn mount_transaction(server: &MockServer, transaction_id: &str, response: Value) {
        Mock::given(method("GET"))
            .and(path("/transaction"))
            .and(query_param("id", transaction_id))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .mount(server)
            .await;
    }

    async fn mount_get(
        server: &MockServer,
        endpoint: &str,
        key: &str,
        value: &str,
        response: Value,
    ) {
        Mock::given(method("GET"))
            .and(path(format!("/{endpoint}")))
            .and(query_param(key, value))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .mount(server)
            .await;
    }

    async fn mount_typed_get(
        server: &MockServer,
        endpoint: &str,
        signer: &str,
        tx_type: &str,
        response: Value,
    ) {
        Mock::given(method("GET"))
            .and(path(format!("/{endpoint}")))
            .and(query_param("address", signer))
            .and(query_param("type", tx_type))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .mount(server)
            .await;
    }
}
