//! Durable direct-EOA settlement envelope preparation and replay.

use std::{str::FromStr, time::Duration};

use alloy::{
    consensus::{SignableTransaction, TxEip1559},
    eips::{BlockNumberOrTag, eip2718::Encodable2718, eip2930::AccessList},
    network::TxSignerSync,
    primitives::{Address, B256, Bytes, TxKind, U256, keccak256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::{
        client::RpcClient,
        types::{TransactionInput, TransactionRequest},
    },
    transports::http::Http,
};
use async_trait::async_trait;
use quant_pivot_models::{
    config::OnchainConfig,
    enums::quant::ExecutionWalletKind,
    types::{
        ContentHash, EvmAddress, EvmBlockHash, EvmCalldataHash, EvmTransactionHash, EvmUint256,
    },
};
use reqwest::{Client, Url};
use serde::Deserialize;

use super::adapter::PreparedSettlementCall;
use crate::{keystore::OrderSigner, wallet::WalletTopology};

const POLYGON_CHAIN_ID: u64 = 137;
const GAS_BUFFER_NUMERATOR: u64 = 120;
const GAS_BUFFER_DENOMINATOR: u64 = 100;
const MAX_SETTLEMENT_GAS_LIMIT: u64 = 10_000_000;

/// Canonical chain head used to freeze a prepared envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EoaPreparedBlock {
    pub number: u64,
    pub hash: EvmBlockHash,
}

/// EIP-1559 fee fields returned by the Polygon RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EoaFeeEstimate {
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
}

/// Immutable EIP-2718 bytes that must be durably inserted before broadcast.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedEoaEnvelope {
    target_adapter: EvmAddress,
    call_target: EvmAddress,
    deployment_digest: ContentHash,
    calldata_hash: EvmCalldataHash,
    prepared_block: EoaPreparedBlock,
    nonce: EvmUint256,
    gas_limit: EvmUint256,
    signed_envelope: Vec<u8>,
    signed_envelope_hash: ContentHash,
    transaction_hash: EvmTransactionHash,
}

/// Complete durable journal payload required to restore one direct envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableEoaEnvelope {
    pub target_adapter: EvmAddress,
    pub call_target: EvmAddress,
    pub deployment_digest: ContentHash,
    pub calldata_hash: EvmCalldataHash,
    pub prepared_block: EoaPreparedBlock,
    pub nonce: EvmUint256,
    pub gas_limit: EvmUint256,
    pub signed_envelope: Vec<u8>,
    pub signed_envelope_hash: ContentHash,
    pub transaction_hash: EvmTransactionHash,
}

impl PreparedEoaEnvelope {
    /// Rehydrate an envelope exclusively from its durable journal fields.
    /// This does not prepare or sign a new transaction; replay still verifies
    /// both frozen hashes before any network call.
    pub fn restore_durable(durable: DurableEoaEnvelope) -> Result<Self, EoaSettlementError> {
        let restored = Self {
            target_adapter: durable.target_adapter,
            call_target: durable.call_target,
            deployment_digest: durable.deployment_digest,
            calldata_hash: durable.calldata_hash,
            prepared_block: durable.prepared_block,
            nonce: durable.nonce,
            gas_limit: durable.gas_limit,
            signed_envelope: durable.signed_envelope,
            signed_envelope_hash: durable.signed_envelope_hash,
            transaction_hash: durable.transaction_hash,
        };
        restored.verify_durable_identity()?;
        Ok(restored)
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
    pub const fn gas_limit(&self) -> &EvmUint256 {
        &self.gas_limit
    }

    #[must_use]
    pub fn signed_envelope(&self) -> &[u8] {
        &self.signed_envelope
    }

    #[must_use]
    pub const fn signed_envelope_hash(&self) -> ContentHash {
        self.signed_envelope_hash
    }

    #[must_use]
    pub const fn transaction_hash(&self) -> &EvmTransactionHash {
        &self.transaction_hash
    }

    fn verify_durable_identity(&self) -> Result<(), EoaSettlementError> {
        let local_hash = keccak256(self.signed_envelope());
        if content_hash(self.signed_envelope()) != self.signed_envelope_hash
            || format!("{local_hash:#x}") != self.transaction_hash.as_str()
        {
            return Err(EoaSettlementError::CorruptDurableEnvelope);
        }
        Ok(())
    }
}

/// Direct-EOA preparation or replay failure.
#[derive(Debug, thiserror::Error)]
pub enum EoaSettlementError {
    #[error("direct EOA settlement requires an EOA wallet topology")]
    WrongWalletKind,
    #[error("direct EOA signer/funder/call identity does not match")]
    WalletIdentityMismatch,
    #[error("settlement RPC is connected to chain {actual}, expected Polygon chain 137")]
    WrongChain { actual: u64 },
    #[error("settlement RPC call {operation} failed: {detail}")]
    RpcCall {
        operation: &'static str,
        detail: String,
    },
    #[error("Polygon RPC returned no canonical latest block")]
    MissingCanonicalHead,
    #[error("prepared block {block_number} changed before envelope preparation completed")]
    CanonicalBlockChanged { block_number: u64 },
    #[error("settlement gas estimate {estimated} cannot be buffered within the 10,000,000 cap")]
    GasLimitOutOfBounds { estimated: u64 },
    #[error("prepared EOA value is not canonical: {detail}")]
    InvalidPreparedValue { detail: String },
    #[error("failed to sign the prepared EOA envelope: {detail}")]
    Signing { detail: String },
    #[error("EOA broadcast outcome is ambiguous; replay only the exact durable envelope: {detail}")]
    AmbiguousBroadcast { detail: String },
    #[error("RPC returned transaction hash {actual}, expected locally computed {expected}")]
    BroadcastHashMismatch { expected: String, actual: String },
    #[error("durable EOA envelope bytes do not match their frozen hashes")]
    CorruptDurableEnvelope,
}

/// Minimum Polygon RPC boundary required by the EOA journal.
#[async_trait]
pub trait EoaSettlementRpc: Send + Sync {
    async fn chain_id(&self) -> Result<u64, EoaSettlementError>;

    async fn canonical_head(&self) -> Result<EoaPreparedBlock, EoaSettlementError>;

    async fn pending_nonce(&self, signer: Address) -> Result<u64, EoaSettlementError>;

    async fn estimate_gas(
        &self,
        signer: Address,
        target: Address,
        calldata: &[u8],
    ) -> Result<u64, EoaSettlementError>;

    async fn estimate_fees(&self) -> Result<EoaFeeEstimate, EoaSettlementError>;

    async fn canonical_hash(
        &self,
        block_number: u64,
    ) -> Result<Option<EvmBlockHash>, EoaSettlementError>;

    async fn broadcast_raw(&self, envelope: &[u8]) -> Result<B256, EoaSettlementError>;
}

/// Stateless EOA envelope builder. It never broadcasts during preparation.
#[derive(Debug, Default, Clone, Copy)]
pub struct EoaSettlementEnvelopeBuilder;

impl EoaSettlementEnvelopeBuilder {
    pub async fn prepare<R: EoaSettlementRpc>(
        &self,
        rpc: &R,
        signer: &OrderSigner,
        topology: &WalletTopology,
        call: &PreparedSettlementCall,
    ) -> Result<PreparedEoaEnvelope, EoaSettlementError> {
        verify_eoa_identity(signer, topology, call)?;
        let chain_id = rpc.chain_id().await?;
        if chain_id != POLYGON_CHAIN_ID {
            return Err(EoaSettlementError::WrongChain { actual: chain_id });
        }

        let prepared_block = rpc.canonical_head().await?;
        let nonce = rpc.pending_nonce(topology.signer).await?;
        let target = Address::from_str(call.call_target().as_str()).map_err(|error| {
            EoaSettlementError::InvalidPreparedValue {
                detail: error.to_string(),
            }
        })?;
        let gas_estimate = rpc
            .estimate_gas(topology.signer, target, call.calldata())
            .await?;
        let gas_limit = buffered_gas_limit(gas_estimate)?;
        let fees = rpc.estimate_fees().await?;

        if rpc.canonical_hash(prepared_block.number).await?.as_ref() != Some(&prepared_block.hash) {
            return Err(EoaSettlementError::CanonicalBlockChanged {
                block_number: prepared_block.number,
            });
        }

        let mut transaction = TxEip1559 {
            chain_id,
            nonce,
            gas_limit,
            max_fee_per_gas: fees.max_fee_per_gas,
            max_priority_fee_per_gas: fees.max_priority_fee_per_gas,
            to: TxKind::Call(target),
            value: U256::ZERO,
            access_list: AccessList::default(),
            input: Bytes::copy_from_slice(call.calldata()),
        };
        let signature = signer
            .inner()
            .sign_transaction_sync(&mut transaction)
            .map_err(|error| EoaSettlementError::Signing {
                detail: error.to_string(),
            })?;
        let signed_envelope = transaction.into_signed(signature).encoded_2718();
        let signed_envelope_hash = content_hash(&signed_envelope);
        let transaction_hash = typed_transaction_hash(keccak256(&signed_envelope))?;

        Ok(PreparedEoaEnvelope {
            target_adapter: call.target_adapter().clone(),
            call_target: call.call_target().clone(),
            deployment_digest: call.deployment_digest(),
            calldata_hash: call.calldata_hash().clone(),
            prepared_block,
            nonce: typed_uint(nonce)?,
            gas_limit: typed_uint(gas_limit)?,
            signed_envelope,
            signed_envelope_hash,
            transaction_hash,
        })
    }

    /// Replay only a previously durable envelope. Network failures remain
    /// ambiguous because the RPC may have accepted the bytes before disconnect.
    pub async fn broadcast<R: EoaSettlementRpc>(
        &self,
        rpc: &R,
        prepared: &PreparedEoaEnvelope,
    ) -> Result<EvmTransactionHash, EoaSettlementError> {
        prepared.verify_durable_identity()?;
        let returned_hash = rpc
            .broadcast_raw(prepared.signed_envelope())
            .await
            .map_err(|error| match error {
                ambiguous @ EoaSettlementError::AmbiguousBroadcast { .. } => ambiguous,
                other => EoaSettlementError::AmbiguousBroadcast {
                    detail: other.to_string(),
                },
            })?;
        let returned = typed_transaction_hash(returned_hash)?;
        if returned != prepared.transaction_hash {
            return Err(EoaSettlementError::BroadcastHashMismatch {
                expected: prepared.transaction_hash.to_string(),
                actual: returned.to_string(),
            });
        }
        Ok(returned)
    }
}

/// Alloy-backed Polygon RPC implementation for direct EOA envelopes.
pub struct AlloyEoaSettlementRpc {
    provider: DynProvider,
}

impl AlloyEoaSettlementRpc {
    pub fn connect(config: &OnchainConfig) -> Result<Self, EoaSettlementError> {
        let rpc_url =
            Url::parse(config.rpc_url()).map_err(|error| EoaSettlementError::RpcCall {
                operation: "rpc_url_parse",
                detail: error.to_string(),
            })?;
        let http_client = Client::builder()
            .timeout(Duration::from_millis(config.rpc_timeout_ms))
            .build()
            .map_err(|error| EoaSettlementError::RpcCall {
                operation: "rpc_client_build",
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
impl EoaSettlementRpc for AlloyEoaSettlementRpc {
    async fn chain_id(&self) -> Result<u64, EoaSettlementError> {
        self.provider
            .get_chain_id()
            .await
            .map_err(|error| rpc_error("eth_chainId", &error))
    }

    async fn canonical_head(&self) -> Result<EoaPreparedBlock, EoaSettlementError> {
        let block: Option<CanonicalBlockResponse> = self
            .provider
            .raw_request(
                "eth_getBlockByNumber".into(),
                (BlockNumberOrTag::Latest, false),
            )
            .await
            .map_err(|error| rpc_error("eth_getBlockByNumber(latest)", &error))?;
        let block = block.ok_or(EoaSettlementError::MissingCanonicalHead)?;
        Ok(EoaPreparedBlock {
            number: block.number,
            hash: typed_block_hash(block.hash)?,
        })
    }

    async fn pending_nonce(&self, signer: Address) -> Result<u64, EoaSettlementError> {
        self.provider
            .get_transaction_count(signer)
            .pending()
            .await
            .map_err(|error| rpc_error("eth_getTransactionCount(pending)", &error))
    }

    async fn estimate_gas(
        &self,
        signer: Address,
        target: Address,
        calldata: &[u8],
    ) -> Result<u64, EoaSettlementError> {
        self.provider
            .estimate_gas(
                TransactionRequest::default()
                    .from(signer)
                    .to(target)
                    .input(TransactionInput::new(Bytes::copy_from_slice(calldata))),
            )
            .await
            .map_err(|error| rpc_error("eth_estimateGas", &error))
    }

    async fn estimate_fees(&self) -> Result<EoaFeeEstimate, EoaSettlementError> {
        self.provider
            .estimate_eip1559_fees()
            .await
            .map(|fees| EoaFeeEstimate {
                max_fee_per_gas: fees.max_fee_per_gas,
                max_priority_fee_per_gas: fees.max_priority_fee_per_gas,
            })
            .map_err(|error| rpc_error("eth_feeHistory", &error))
    }

    async fn canonical_hash(
        &self,
        block_number: u64,
    ) -> Result<Option<EvmBlockHash>, EoaSettlementError> {
        let block: Option<CanonicalBlockResponse> = self
            .provider
            .raw_request(
                "eth_getBlockByNumber".into(),
                (BlockNumberOrTag::Number(block_number), false),
            )
            .await
            .map_err(|error| rpc_error("eth_getBlockByNumber(canonical recheck)", &error))?;
        block.map(|value| typed_block_hash(value.hash)).transpose()
    }

    async fn broadcast_raw(&self, envelope: &[u8]) -> Result<B256, EoaSettlementError> {
        self.provider
            .send_raw_transaction(envelope)
            .await
            .map(|pending| *pending.tx_hash())
            .map_err(|error| EoaSettlementError::AmbiguousBroadcast {
                detail: error.to_string(),
            })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalBlockResponse {
    number: u64,
    hash: B256,
}

fn verify_eoa_identity(
    signer: &OrderSigner,
    topology: &WalletTopology,
    call: &PreparedSettlementCall,
) -> Result<(), EoaSettlementError> {
    if topology.kind != ExecutionWalletKind::Eoa {
        return Err(EoaSettlementError::WrongWalletKind);
    }
    if topology.signer != signer.address()
        || topology.funder != signer.address()
        || call.funder().as_str() != format!("{:#x}", signer.address())
    {
        return Err(EoaSettlementError::WalletIdentityMismatch);
    }
    Ok(())
}

fn buffered_gas_limit(estimate: u64) -> Result<u64, EoaSettlementError> {
    let buffered = estimate
        .checked_mul(GAS_BUFFER_NUMERATOR)
        .and_then(|value| value.checked_add(GAS_BUFFER_DENOMINATOR - 1))
        .map(|value| value / GAS_BUFFER_DENOMINATOR)
        .filter(|value| *value > 0 && *value <= MAX_SETTLEMENT_GAS_LIMIT)
        .ok_or(EoaSettlementError::GasLimitOutOfBounds {
            estimated: estimate,
        })?;
    Ok(buffered)
}

fn content_hash(bytes: &[u8]) -> ContentHash {
    ContentHash::from_bytes(*blake3::hash(bytes).as_bytes())
}

fn typed_uint(value: u64) -> Result<EvmUint256, EoaSettlementError> {
    EvmUint256::parse(value.to_string()).map_err(|error| EoaSettlementError::InvalidPreparedValue {
        detail: error.to_string(),
    })
}

fn typed_transaction_hash(hash: B256) -> Result<EvmTransactionHash, EoaSettlementError> {
    EvmTransactionHash::parse(format!("{hash:#x}")).map_err(|error| {
        EoaSettlementError::InvalidPreparedValue {
            detail: error.to_string(),
        }
    })
}

fn typed_block_hash(hash: B256) -> Result<EvmBlockHash, EoaSettlementError> {
    EvmBlockHash::parse(format!("{hash:#x}")).map_err(|error| {
        EoaSettlementError::InvalidPreparedValue {
            detail: error.to_string(),
        }
    })
}

fn rpc_error(operation: &'static str, error: &impl ToString) -> EoaSettlementError {
    EoaSettlementError::RpcCall {
        operation,
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use alloy::primitives::{Address, B256, keccak256};
    use async_trait::async_trait;
    use polymarket_client_sdk_v2::clob::types::SignatureType;
    use quant_pivot_models::{
        enums::{quant::ExecutionWalletKind, settlement::SettlementRoute},
        types::{EvmAddress, EvmBlockHash, MarketId},
    };

    use super::{
        EoaFeeEstimate, EoaPreparedBlock, EoaSettlementEnvelopeBuilder, EoaSettlementError,
        EoaSettlementRpc,
    };
    use crate::{
        keystore::OrderSigner,
        settlement::{
            adapter::{SettlementAdapterGateway, verified_redeem_fixture},
            contracts::verified_deployment_fixture_at,
        },
        wallet::WalletTopology,
    };

    struct FakeRpc {
        block: EoaPreparedBlock,
        returned_hash: Mutex<Option<B256>>,
        broadcasts: Mutex<Vec<Vec<u8>>>,
    }

    #[async_trait]
    impl EoaSettlementRpc for FakeRpc {
        async fn chain_id(&self) -> Result<u64, EoaSettlementError> {
            Ok(137)
        }

        async fn canonical_head(&self) -> Result<EoaPreparedBlock, EoaSettlementError> {
            Ok(self.block.clone())
        }

        async fn pending_nonce(&self, _signer: Address) -> Result<u64, EoaSettlementError> {
            Ok(17)
        }

        async fn estimate_gas(
            &self,
            _signer: Address,
            _target: Address,
            _calldata: &[u8],
        ) -> Result<u64, EoaSettlementError> {
            Ok(100_000)
        }

        async fn estimate_fees(&self) -> Result<EoaFeeEstimate, EoaSettlementError> {
            Ok(EoaFeeEstimate {
                max_fee_per_gas: 50_000_000_000,
                max_priority_fee_per_gas: 30_000_000_000,
            })
        }

        async fn canonical_hash(
            &self,
            _block_number: u64,
        ) -> Result<Option<EvmBlockHash>, EoaSettlementError> {
            Ok(Some(self.block.hash.clone()))
        }

        async fn broadcast_raw(&self, envelope: &[u8]) -> Result<B256, EoaSettlementError> {
            self.broadcasts
                .lock()
                .expect("broadcast lock")
                .push(envelope.to_vec());
            Ok(self
                .returned_hash
                .lock()
                .expect("returned hash lock")
                .unwrap_or_else(|| keccak256(envelope)))
        }
    }

    #[tokio::test]
    async fn freezes_nonce_envelope_and_hash_before_exact_replay() {
        let signer = OrderSigner::from_bytes(&[0x11; 32]).expect("test signer");
        let topology = topology(&signer);
        let block_hash =
            EvmBlockHash::parse(format!("0x{}", "55".repeat(32))).expect("canonical block hash");
        let capability = verified_deployment_fixture_at(
            SettlementRoute::StandardV2,
            EvmAddress::parse("0xada100db00ca00073811820692005400218fce1f").expect("adapter"),
            EvmAddress::parse(format!("{:#x}", signer.address())).expect("funder"),
            90_685_100,
            block_hash.clone(),
        );
        let redeem =
            verified_redeem_fixture(capability, &MarketId::new(format!("0x{}", "22".repeat(32))));
        let call = SettlementAdapterGateway.prepare_redeem(&redeem);
        let rpc = Arc::new(FakeRpc {
            block: EoaPreparedBlock {
                number: 90_685_100,
                hash: block_hash,
            },
            returned_hash: Mutex::new(None),
            broadcasts: Mutex::new(Vec::new()),
        });

        let prepared = EoaSettlementEnvelopeBuilder
            .prepare(rpc.as_ref(), &signer, &topology, &call)
            .await
            .expect("prepare envelope");
        assert_eq!(prepared.nonce().as_str(), "17");
        assert_eq!(prepared.gas_limit().as_str(), "120000");
        assert_eq!(prepared.signed_envelope()[0], 0x02);
        assert_eq!(
            prepared.transaction_hash().as_str(),
            format!("{:#x}", keccak256(prepared.signed_envelope()))
        );

        let first = EoaSettlementEnvelopeBuilder
            .broadcast(rpc.as_ref(), &prepared)
            .await
            .expect("first broadcast");
        let second = EoaSettlementEnvelopeBuilder
            .broadcast(rpc.as_ref(), &prepared)
            .await
            .expect("idempotent replay");
        assert_eq!(first, second);
        let broadcasts = rpc.broadcasts.lock().expect("broadcast lock");
        assert_eq!(broadcasts.len(), 2);
        assert_eq!(broadcasts[0], broadcasts[1]);
        drop(broadcasts);
    }

    #[tokio::test]
    async fn rejects_rpc_hash_that_differs_from_the_local_envelope_hash() {
        let signer = OrderSigner::from_bytes(&[0x12; 32]).expect("test signer");
        let topology = topology(&signer);
        let block_hash =
            EvmBlockHash::parse(format!("0x{}", "66".repeat(32))).expect("canonical block hash");
        let capability = verified_deployment_fixture_at(
            SettlementRoute::NegRiskV2,
            EvmAddress::parse("0xada2005600dec949baf300f4c6120000bdb6eaab").expect("adapter"),
            EvmAddress::parse(format!("{:#x}", signer.address())).expect("funder"),
            90_685_101,
            block_hash.clone(),
        );
        let redeem =
            verified_redeem_fixture(capability, &MarketId::new(format!("0x{}", "33".repeat(32))));
        let call = SettlementAdapterGateway.prepare_redeem(&redeem);
        let rpc = FakeRpc {
            block: EoaPreparedBlock {
                number: 90_685_101,
                hash: block_hash,
            },
            returned_hash: Mutex::new(Some(B256::repeat_byte(0xaa))),
            broadcasts: Mutex::new(Vec::new()),
        };
        let prepared = EoaSettlementEnvelopeBuilder
            .prepare(&rpc, &signer, &topology, &call)
            .await
            .expect("prepare envelope");
        let error = EoaSettlementEnvelopeBuilder
            .broadcast(&rpc, &prepared)
            .await
            .expect_err("hash mismatch must fail closed");
        assert!(matches!(
            error,
            EoaSettlementError::BroadcastHashMismatch { .. }
        ));
    }

    fn topology(signer: &OrderSigner) -> WalletTopology {
        WalletTopology {
            kind: ExecutionWalletKind::Eoa,
            signer: signer.address(),
            owner: signer.address(),
            funder: signer.address(),
            signature_type: SignatureType::Eoa,
        }
    }
}
