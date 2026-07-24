//! Finalized Polygon receipt and settlement-business evidence verification.

use std::{fmt::Display, str::FromStr, time::Duration};

use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    hex,
    primitives::{Address, B256, Bytes, U256, keccak256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::client::RpcClient,
    sol,
    sol_types::SolCall,
    transports::http::Http,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_models::{
    config::OnchainConfig,
    domain::quant::{
        settlement::{SettlementChainSubmissionInfo, SettlementRedeemInfo},
        settlement_governance::SettlementGovernedActionInfo,
    },
    enums::{
        quant::ExecutionWalletKind,
        settlement::{
            SettlementFailureCode, SettlementGovernedActionKind, SettlementGovernedActionState,
            SettlementSubmissionKind, SettlementSubmissionPurpose, SettlementSubmissionState,
        },
    },
    hashing::CanonicalDigest,
    types::{
        EvmAddress, EvmBlockHash, EvmCalldataHash, EvmCodeHash, EvmTransactionHash, EvmUint256,
        Shares, TokenId, Usd,
        settlement_payload::{
            SettlementBalanceEvidence, SettlementMinedCallEvidence,
            SettlementOperatorApprovalReceiptEvidence, SettlementPusdMintEvidence,
            SettlementReceiptEvidence, SettlementTokenBalance, SettlementWrappedPayoutEvidence,
        },
    },
};
use reqwest::{Client, Url};
use rust_decimal::Decimal;
use serde::Deserialize;

use super::{
    relayer::{
        FrozenDepositWalletRequest, FrozenSafeProxyRequest,
        deposit_wallet_wire::Factory::proxyCall as DepositWalletProxyCall,
    },
    typed::{
        IntoEvmAddress, IntoEvmBlockHash, IntoEvmCodeHash, IntoEvmTransactionHash, IntoEvmUint,
        SettlementValueError, SettlementValueKind,
    },
};
use crate::wallet::WalletTopology;

const POLYGON_CHAIN_ID: u64 = 137;
const PUSD_SCALE: u64 = 1_000_000;
const PROXY_CALL_TYPE: u8 = 1;
const PROXY_FACTORY: &str = "0xab45c5a4b0c941a2f231c04c3f49182e1a254052";
const PROXY_RELAY_HUB: &str = "0xd216153c06e857cd7f72665e0af1d7d82172f494";
const DEPOSIT_WALLET_FACTORY: &str = "0x00000000000fb5c9adea0298d729a0cb3823cc07";

sol! {
    struct ProxyCall {
        uint8 typeCode;
        address to;
        uint256 value;
        bytes data;
    }

    function proxy(ProxyCall[] calls);
}

sol! {
    function execTransaction(
        address to,
        uint256 value,
        bytes data,
        uint8 operation,
        uint256 safeTxGas,
        uint256 baseGas,
        uint256 gasPrice,
        address gasToken,
        address payable refundReceiver,
        bytes signatures
    ) returns (bool success);
}

sol! {
    function relayCall(
        address from,
        address recipient,
        bytes encodedFunction,
        uint256 transactionFee,
        uint256 gasPrice,
        uint256 gasLimit,
        uint256 nonce,
        bytes signature,
        bytes approvalData
    );
}

sol! {
    #[sol(rpc)]
    interface ConditionalTokensConfirmationView {
        function balanceOf(address account, uint256 id) external view returns (uint256);
        function isApprovedForAll(address account, address operator) external view returns (bool);
    }
}

/// On-chain transaction fields used to prove the frozen call identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementTransactionObservation {
    pub transaction_hash: EvmTransactionHash,
    pub outer_sender: EvmAddress,
    pub outer_target: EvmAddress,
    pub input: Vec<u8>,
}

/// One decoded ERC-20 `Transfer` log from the receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedErc20Transfer {
    pub token: EvmAddress,
    pub from: EvmAddress,
    pub to: EvmAddress,
    pub raw_amount: U256,
    pub log_index: u64,
}

/// One decoded pUSD `Wrapped` log from the receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedWrappedPayout {
    pub collateral_token: EvmAddress,
    pub caller: EvmAddress,
    pub asset: EvmAddress,
    pub to: EvmAddress,
    pub raw_amount: U256,
    pub log_index: u64,
}

/// Receipt and finalized-chain observation collected without a signer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementChainObservation {
    pub chain_id: u64,
    pub transaction: SettlementTransactionObservation,
    pub receipt_transaction_hash: EvmTransactionHash,
    pub receipt_success: bool,
    pub receipt_block_number: u64,
    pub receipt_block_hash: EvmBlockHash,
    pub canonical_receipt_block_hash: Option<EvmBlockHash>,
    pub finalized_block_number: u64,
    pub finalized_block_hash: EvmBlockHash,
    pub target_code_hash: EvmCodeHash,
    pub transfers: Vec<ObservedErc20Transfer>,
    pub wrapped_payouts: Vec<ObservedWrappedPayout>,
    pub balances_after: SettlementBalanceEvidence,
    pub gas_used: u64,
    pub effective_gas_price_wei: u128,
    pub observed_at: DateTime<Utc>,
}

/// Finalized-chain observation for an ERC-1155 operator approval or
/// revocation. It deliberately excludes redemption payout/balance fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementOperatorApprovalObservation {
    pub chain_id: u64,
    pub transaction: SettlementTransactionObservation,
    pub receipt_transaction_hash: EvmTransactionHash,
    pub receipt_success: bool,
    pub receipt_block_number: u64,
    pub receipt_block_hash: EvmBlockHash,
    pub canonical_receipt_block_hash: Option<EvmBlockHash>,
    pub finalized_block_number: u64,
    pub finalized_block_hash: EvmBlockHash,
    pub target_code_hash: EvmCodeHash,
    pub operator_approved: bool,
    pub observed_at: DateTime<Utc>,
}

/// Read-only observation boundary. `None` means the durable transaction has no
/// receipt yet and must remain in finality tracking.
#[async_trait]
pub trait SettlementConfirmationReader: Send + Sync {
    async fn observe(
        &self,
        redeem: &SettlementRedeemInfo,
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<Option<SettlementChainObservation>, SettlementConfirmationReadError>;
}

/// Closed read-side failures. These are infrastructure/evidence acquisition
/// failures and never imply successful settlement.
#[derive(Debug, thiserror::Error)]
pub enum SettlementConfirmationReadError {
    #[error("invalid settlement confirmation RPC configuration: {detail}")]
    InvalidConfiguration { detail: String },
    #[error("settlement confirmation requires a durable transaction hash")]
    MissingTransactionHash,
    #[error("settlement case has no frozen balance evidence")]
    MissingFrozenBalances,
    #[error("settlement confirmation RPC call {operation} failed: {detail}")]
    RpcCall {
        operation: &'static str,
        detail: String,
    },
    #[error("settlement confirmation RPC omitted required {field}")]
    MissingRpcField { field: &'static str },
    #[error("settlement confirmation RPC returned invalid typed evidence: {detail}")]
    InvalidEvidence { detail: String },
    #[error(transparent)]
    TransferLog(#[from] SettlementConfirmationError),
}

impl From<SettlementValueError> for SettlementConfirmationReadError {
    fn from(error: SettlementValueError) -> Self {
        Self::InvalidEvidence {
            detail: error.detail().to_owned(),
        }
    }
}

/// Alloy-backed, signer-free Polygon confirmation reader.
pub struct AlloySettlementConfirmationReader {
    provider: DynProvider,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcTransactionObservation {
    hash: B256,
    from: Address,
    to: Option<Address>,
    input: Bytes,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcReceiptObservation {
    transaction_hash: B256,
    #[serde(with = "alloy::serde::quantity")]
    status: u64,
    #[serde(with = "alloy::serde::quantity")]
    block_number: u64,
    block_hash: B256,
    #[serde(with = "alloy::serde::quantity")]
    gas_used: u64,
    #[serde(with = "alloy::serde::quantity")]
    effective_gas_price: u128,
    logs: Vec<RpcLogObservation>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcLogObservation {
    address: Address,
    topics: Vec<B256>,
    data: Bytes,
    #[serde(with = "alloy::serde::quantity")]
    log_index: u64,
}

#[derive(Debug, Deserialize)]
struct RpcBlockObservation {
    #[serde(with = "alloy::serde::quantity")]
    number: u64,
    hash: B256,
}

impl AlloySettlementConfirmationReader {
    /// Build a bounded read-only client. No network request is issued here.
    pub fn connect(config: &OnchainConfig) -> Result<Self, SettlementConfirmationReadError> {
        let rpc_url = Url::parse(config.rpc_url()).map_err(|error| {
            SettlementConfirmationReadError::InvalidConfiguration {
                detail: error.to_string(),
            }
        })?;
        let http = Client::builder()
            .timeout(Duration::from_millis(config.rpc_timeout_ms))
            .build()
            .map_err(
                |error| SettlementConfirmationReadError::InvalidConfiguration {
                    detail: error.to_string(),
                },
            )?;
        let transport = Http::with_client(http, rpc_url);
        let client = RpcClient::new(transport, false);
        Ok(Self {
            provider: ProviderBuilder::new().connect_client(client).erased(),
        })
    }

    /// Observe a durable operator-approval transaction at its receipt block.
    ///
    /// The post-state read is pinned to the receipt hash, while finality and
    /// canonicality are independently rechecked before confirmation.
    pub async fn observe_operator_approval(
        &self,
        funder: &EvmAddress,
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<Option<SettlementOperatorApprovalObservation>, SettlementConfirmationReadError>
    {
        let transaction_hash = submission
            .transaction_hash
            .as_ref()
            .ok_or(SettlementConfirmationReadError::MissingTransactionHash)?;
        let rpc_hash = B256::from_str(transaction_hash.as_str()).map_err(|error| {
            SettlementConfirmationReadError::InvalidEvidence {
                detail: error.to_string(),
            }
        })?;
        let receipt: Option<RpcReceiptObservation> = self
            .provider
            .raw_request("eth_getTransactionReceipt".into(), (rpc_hash,))
            .await
            .map_err(|error| read_error("eth_getTransactionReceipt", &error))?;
        let Some(receipt) = receipt else {
            return Ok(None);
        };
        let transaction: Option<RpcTransactionObservation> = self
            .provider
            .raw_request("eth_getTransactionByHash".into(), (rpc_hash,))
            .await
            .map_err(|error| read_error("eth_getTransactionByHash", &error))?;
        let transaction = transaction.ok_or(SettlementConfirmationReadError::MissingRpcField {
            field: "transaction",
        })?;
        let outer_target =
            transaction
                .to
                .ok_or(SettlementConfirmationReadError::MissingRpcField {
                    field: "transaction.to",
                })?;
        let finalized: Option<RpcBlockObservation> = self
            .provider
            .raw_request(
                "eth_getBlockByNumber".into(),
                (BlockNumberOrTag::Finalized, false),
            )
            .await
            .map_err(|error| read_error("eth_getBlockByNumber(finalized)", &error))?;
        let finalized = finalized.ok_or(SettlementConfirmationReadError::MissingRpcField {
            field: "finalized block",
        })?;
        let canonical: Option<RpcBlockObservation> = self
            .provider
            .raw_request(
                "eth_getBlockByNumber".into(),
                (BlockNumberOrTag::Number(receipt.block_number), false),
            )
            .await
            .map_err(|error| read_error("eth_getBlockByNumber(canonical recheck)", &error))?;
        let canonical_receipt_block_hash = canonical
            .map(|block| (block.hash).into_evm_block_hash())
            .transpose()?;
        let target = Address::from_str(submission.target_adapter.as_str()).map_err(|error| {
            SettlementConfirmationReadError::InvalidEvidence {
                detail: error.to_string(),
            }
        })?;
        let target_code = self
            .provider
            .get_code_at(target)
            .block_id(BlockId::hash_canonical(receipt.block_hash))
            .await
            .map_err(|error| read_error("eth_getCode(target@receiptHash)", &error))?;
        let conditional_tokens = Address::from_str(submission.conditional_tokens.as_str())
            .map_err(|error| SettlementConfirmationReadError::InvalidEvidence {
                detail: error.to_string(),
            })?;
        let funder = Address::from_str(funder.as_str()).map_err(|error| {
            SettlementConfirmationReadError::InvalidEvidence {
                detail: error.to_string(),
            }
        })?;
        let ctf = ConditionalTokensConfirmationView::new(conditional_tokens, &self.provider);
        let operator_approved = ctf
            .isApprovedForAll(funder, target)
            .block(BlockId::hash_canonical(receipt.block_hash))
            .call()
            .await
            .map_err(|error| read_error("ctf.isApprovedForAll@receiptHash", &error))?;
        Ok(Some(SettlementOperatorApprovalObservation {
            chain_id: self
                .provider
                .get_chain_id()
                .await
                .map_err(|error| read_error("eth_chainId", &error))?,
            transaction: SettlementTransactionObservation {
                transaction_hash: (transaction.hash).into_evm_transaction_hash()?,
                outer_sender: (transaction.from).into_evm_address()?,
                outer_target: (outer_target).into_evm_address()?,
                input: transaction.input.to_vec(),
            },
            receipt_transaction_hash: (receipt.transaction_hash).into_evm_transaction_hash()?,
            receipt_success: receipt.status == 1,
            receipt_block_number: receipt.block_number,
            receipt_block_hash: (receipt.block_hash).into_evm_block_hash()?,
            canonical_receipt_block_hash,
            finalized_block_number: finalized.number,
            finalized_block_hash: (finalized.hash).into_evm_block_hash()?,
            target_code_hash: (keccak256(&target_code)).into_evm_code_hash()?,
            operator_approved,
            observed_at: Utc::now(),
        }))
    }
}

#[async_trait]
impl SettlementConfirmationReader for AlloySettlementConfirmationReader {
    async fn observe(
        &self,
        redeem: &SettlementRedeemInfo,
        submission: &SettlementChainSubmissionInfo,
    ) -> Result<Option<SettlementChainObservation>, SettlementConfirmationReadError> {
        let transaction_hash = submission
            .transaction_hash
            .as_ref()
            .ok_or(SettlementConfirmationReadError::MissingTransactionHash)?;
        let rpc_hash = B256::from_str(transaction_hash.as_str()).map_err(|error| {
            SettlementConfirmationReadError::InvalidEvidence {
                detail: error.to_string(),
            }
        })?;
        let receipt: Option<RpcReceiptObservation> = self
            .provider
            .raw_request("eth_getTransactionReceipt".into(), (rpc_hash,))
            .await
            .map_err(|error| read_error("eth_getTransactionReceipt", &error))?;
        let Some(receipt) = receipt else {
            return Ok(None);
        };
        let transaction: Option<RpcTransactionObservation> = self
            .provider
            .raw_request("eth_getTransactionByHash".into(), (rpc_hash,))
            .await
            .map_err(|error| read_error("eth_getTransactionByHash", &error))?;
        let transaction = transaction.ok_or(SettlementConfirmationReadError::MissingRpcField {
            field: "transaction",
        })?;
        let receipt_block_number = receipt.block_number;
        let receipt_block_hash = receipt.block_hash;
        let outer_target =
            transaction
                .to
                .ok_or(SettlementConfirmationReadError::MissingRpcField {
                    field: "transaction.to",
                })?;
        let finalized: Option<RpcBlockObservation> = self
            .provider
            .raw_request(
                "eth_getBlockByNumber".into(),
                (BlockNumberOrTag::Finalized, false),
            )
            .await
            .map_err(|error| read_error("eth_getBlockByNumber(finalized)", &error))?;
        let finalized = finalized.ok_or(SettlementConfirmationReadError::MissingRpcField {
            field: "finalized block",
        })?;
        let target = Address::from_str(submission.target_adapter.as_str()).map_err(|error| {
            SettlementConfirmationReadError::InvalidEvidence {
                detail: error.to_string(),
            }
        })?;
        let target_code = self
            .provider
            .get_code_at(target)
            .block_id(BlockId::hash_canonical(receipt_block_hash))
            .await
            .map_err(|error| read_error("eth_getCode(target@receiptHash)", &error))?;
        let target_code_hash = (keccak256(&target_code)).into_evm_code_hash()?;
        let balances_after = self
            .balances_at_receipt_hash(redeem, submission, receipt_block_hash)
            .await?;
        let canonical: Option<RpcBlockObservation> = self
            .provider
            .raw_request(
                "eth_getBlockByNumber".into(),
                (BlockNumberOrTag::Number(receipt_block_number), false),
            )
            .await
            .map_err(|error| read_error("eth_getBlockByNumber(canonical recheck)", &error))?;
        let canonical_receipt_block_hash = canonical
            .map(|block| (block.hash).into_evm_block_hash())
            .transpose()?;
        let pusd = Address::from_str(submission.collateral_token.as_str()).map_err(|error| {
            SettlementConfirmationReadError::InvalidEvidence {
                detail: error.to_string(),
            }
        })?;
        let transfers = receipt
            .logs
            .iter()
            .filter(|log| log.address == pusd)
            .filter_map(|log| {
                decode_erc20_transfer(log.address, &log.topics, &log.data, log.log_index)
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let wrapped_payouts = receipt
            .logs
            .iter()
            .filter(|log| log.address == pusd)
            .filter_map(|log| {
                decode_wrapped_payout(log.address, &log.topics, &log.data, log.log_index)
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(SettlementChainObservation {
            chain_id: self
                .provider
                .get_chain_id()
                .await
                .map_err(|error| read_error("eth_chainId", &error))?,
            transaction: SettlementTransactionObservation {
                transaction_hash: (transaction.hash).into_evm_transaction_hash()?,
                outer_sender: (transaction.from).into_evm_address()?,
                outer_target: (outer_target).into_evm_address()?,
                input: transaction.input.to_vec(),
            },
            receipt_transaction_hash: (receipt.transaction_hash).into_evm_transaction_hash()?,
            receipt_success: receipt.status == 1,
            receipt_block_number,
            receipt_block_hash: (receipt_block_hash).into_evm_block_hash()?,
            canonical_receipt_block_hash,
            finalized_block_number: finalized.number,
            finalized_block_hash: (finalized.hash).into_evm_block_hash()?,
            target_code_hash,
            transfers,
            wrapped_payouts,
            balances_after,
            gas_used: receipt.gas_used,
            effective_gas_price_wei: receipt.effective_gas_price,
            observed_at: Utc::now(),
        }))
    }
}

impl AlloySettlementConfirmationReader {
    async fn balances_at_receipt_hash(
        &self,
        redeem: &SettlementRedeemInfo,
        submission: &SettlementChainSubmissionInfo,
        receipt_block_hash: B256,
    ) -> Result<SettlementBalanceEvidence, SettlementConfirmationReadError> {
        let before = redeem
            .balance_before_json
            .as_ref()
            .ok_or(SettlementConfirmationReadError::MissingFrozenBalances)?;
        let ctf_address =
            Address::from_str(submission.conditional_tokens.as_str()).map_err(|error| {
                SettlementConfirmationReadError::InvalidEvidence {
                    detail: error.to_string(),
                }
            })?;
        let funder = Address::from_str(redeem.funder_address.as_str()).map_err(|error| {
            SettlementConfirmationReadError::InvalidEvidence {
                detail: error.to_string(),
            }
        })?;
        let yes_token = U256::from_str(before.yes.token_id.as_str()).map_err(|error| {
            SettlementConfirmationReadError::InvalidEvidence {
                detail: error.to_string(),
            }
        })?;
        let no_token = U256::from_str(before.no.token_id.as_str()).map_err(|error| {
            SettlementConfirmationReadError::InvalidEvidence {
                detail: error.to_string(),
            }
        })?;
        let ctf = ConditionalTokensConfirmationView::new(ctf_address, self.provider.clone());
        let block = BlockId::hash_canonical(receipt_block_hash);
        let yes = ctf
            .balanceOf(funder, yes_token)
            .block(block)
            .call()
            .await
            .map_err(|error| read_error("ctf.balanceOf(YES@receiptHash)", &error))?;
        let no = ctf
            .balanceOf(funder, no_token)
            .block(block)
            .call()
            .await
            .map_err(|error| read_error("ctf.balanceOf(NO@receiptHash)", &error))?;
        Ok(SettlementBalanceEvidence {
            yes: observed_token_balance(before.yes.token_id.clone(), yes)?,
            no: observed_token_balance(before.no.token_id.clone(), no)?,
        })
    }
}

/// Evidence ready for the atomic accounting confirmation transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSettlementConfirmation {
    pub receipt: SettlementReceiptEvidence,
    pub balances_after: SettlementBalanceEvidence,
    pub actual_payout_usd: Usd,
    pub gas_fee_pol: Decimal,
}

/// Finality may be pending without being a reconciliation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettlementConfirmationStatus {
    PendingFinality,
    Confirmed(Box<VerifiedSettlementConfirmation>),
}

/// Poll classification keeps transport/read failures separate from durable
/// business-evidence mismatches.
#[derive(Debug)]
pub enum SettlementConfirmationPollOutcome {
    PendingReceipt,
    PendingFinality,
    Confirmed(Box<VerifiedSettlementConfirmation>),
    ReconciliationRequired(SettlementConfirmationError),
}

/// Poll classification for an operator approval/revocation submission.
#[derive(Debug)]
pub enum SettlementOperatorApprovalPollOutcome {
    PendingReceipt,
    PendingFinality,
    Confirmed(Box<SettlementOperatorApprovalReceiptEvidence>),
    ReconciliationRequired(SettlementConfirmationError),
}

/// Observe and verify one durable submission without signing or broadcasting.
pub async fn poll_settlement_confirmation(
    reader: &impl SettlementConfirmationReader,
    topology: &WalletTopology,
    redeem: &SettlementRedeemInfo,
    submission: &SettlementChainSubmissionInfo,
) -> Result<SettlementConfirmationPollOutcome, SettlementConfirmationReadError> {
    let Some(observation) = reader.observe(redeem, submission).await? else {
        return Ok(SettlementConfirmationPollOutcome::PendingReceipt);
    };
    Ok(
        match verify_settlement_confirmation(topology, redeem, submission, observation) {
            Ok(SettlementConfirmationStatus::PendingFinality) => {
                SettlementConfirmationPollOutcome::PendingFinality
            }
            Ok(SettlementConfirmationStatus::Confirmed(confirmation)) => {
                SettlementConfirmationPollOutcome::Confirmed(confirmation)
            }
            Err(error) => SettlementConfirmationPollOutcome::ReconciliationRequired(error),
        },
    )
}

/// Observe and verify one durable operator-approval submission.
pub async fn poll_operator_approval_confirmation(
    reader: &AlloySettlementConfirmationReader,
    topology: &WalletTopology,
    action: &SettlementGovernedActionInfo,
    submission: &SettlementChainSubmissionInfo,
) -> Result<SettlementOperatorApprovalPollOutcome, SettlementConfirmationReadError> {
    let funder = (topology.funder)
        .into_evm_address()
        .map_err(|error| SettlementConfirmationReadError::TransferLog(error.into()))?;
    let Some(observation) = reader
        .observe_operator_approval(&funder, submission)
        .await?
    else {
        return Ok(SettlementOperatorApprovalPollOutcome::PendingReceipt);
    };
    Ok(
        match verify_operator_approval_confirmation(topology, action, submission, observation) {
            Ok(SettlementOperatorApprovalConfirmationStatus::PendingFinality) => {
                SettlementOperatorApprovalPollOutcome::PendingFinality
            }
            Ok(SettlementOperatorApprovalConfirmationStatus::Confirmed(evidence)) => {
                SettlementOperatorApprovalPollOutcome::Confirmed(evidence)
            }
            Err(error) => SettlementOperatorApprovalPollOutcome::ReconciliationRequired(error),
        },
    )
}

/// Finality-aware verification result for an operator approval/revocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettlementOperatorApprovalConfirmationStatus {
    PendingFinality,
    Confirmed(Box<SettlementOperatorApprovalReceiptEvidence>),
}

/// Verify a finalized operator-approval observation against the immutable
/// action scope and exact durable envelope.
pub fn verify_operator_approval_confirmation(
    topology: &WalletTopology,
    action: &SettlementGovernedActionInfo,
    submission: &SettlementChainSubmissionInfo,
    observation: SettlementOperatorApprovalObservation,
) -> Result<SettlementOperatorApprovalConfirmationStatus, SettlementConfirmationError> {
    let funder = (topology.funder).into_evm_address()?;
    let desired_approval = action
        .desired_approval
        .ok_or(SettlementConfirmationError::GovernedActionScopeMismatch)?;
    let expected_purpose = if desired_approval {
        SettlementSubmissionPurpose::OutcomeTokenApproval
    } else {
        SettlementSubmissionPurpose::OutcomeTokenRevocation
    };
    let expected_kind = if desired_approval {
        SettlementGovernedActionKind::OutcomeTokenApproval
    } else {
        SettlementGovernedActionKind::OutcomeTokenRevocation
    };
    if action.kind != expected_kind
        || !matches!(
            action.state,
            SettlementGovernedActionState::Authorized
                | SettlementGovernedActionState::RetryScheduled
        )
        || submission.settlement_redeem_id.is_some()
        || submission.settlement_governed_action_id != Some(action.settlement_governed_action_id)
        || submission.canary_action_id.is_some()
        || submission.purpose != expected_purpose
        || submission.state != SettlementSubmissionState::AwaitingFinality
        || Some(submission.route) != action.route
        || Some(submission.target_adapter.clone()) != action.target_adapter
        || Some(submission.deployment_digest) != action.deployment_digest
        || Some(submission.deployment_evidence_version.clone())
            != action.deployment_evidence_version
        || action
            .verified_block_number
            .is_none_or(|block| submission.verified_block_number < block)
    {
        return Err(SettlementConfirmationError::GovernedActionScopeMismatch);
    }
    if observation.chain_id != POLYGON_CHAIN_ID {
        return Err(SettlementConfirmationError::WrongChain {
            actual: observation.chain_id,
        });
    }
    let transaction_hash = submission
        .transaction_hash
        .clone()
        .ok_or(SettlementConfirmationError::MissingTransactionHash)?;
    if observation.transaction.transaction_hash != transaction_hash
        || observation.receipt_transaction_hash != transaction_hash
    {
        return Err(SettlementConfirmationError::TransactionIdentityMismatch);
    }
    if observation.finalized_block_number < observation.receipt_block_number {
        return Ok(SettlementOperatorApprovalConfirmationStatus::PendingFinality);
    }
    if observation.canonical_receipt_block_hash.as_ref() != Some(&observation.receipt_block_hash) {
        return Err(SettlementConfirmationError::CanonicalBlockChanged {
            block_number: observation.receipt_block_number,
        });
    }
    if !observation.receipt_success {
        return Err(SettlementConfirmationError::ReceiptReverted);
    }
    if observation.target_code_hash != submission.target_code_hash {
        return Err(SettlementConfirmationError::TargetCodeHashMismatch);
    }
    let call = verify_scope_identity(
        topology,
        topology.kind,
        &funder,
        submission,
        &observation.transaction,
        false,
    )?;
    if observation.operator_approved != desired_approval {
        return Err(SettlementConfirmationError::OperatorApprovalStateMismatch);
    }
    Ok(SettlementOperatorApprovalConfirmationStatus::Confirmed(
        Box::new(SettlementOperatorApprovalReceiptEvidence {
            chain_id: observation.chain_id,
            transaction_hash,
            block_number: observation.receipt_block_number,
            block_hash: observation.receipt_block_hash,
            finalized_block_number: observation.finalized_block_number,
            finalized_block_hash: observation.finalized_block_hash,
            call,
            receipt_success: true,
            desired_approval,
            operator_approved: observation.operator_approved,
            canonical_checked_at: observation.observed_at,
            observed_at: observation.observed_at,
        }),
    ))
}

/// Closed failure taxonomy for receipt/business evidence.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SettlementConfirmationError {
    #[error("settlement confirmation observed chain {actual}, expected Polygon 137")]
    WrongChain { actual: u64 },
    #[error("submission has no durable EVM transaction hash")]
    MissingTransactionHash,
    #[error("transaction or receipt hash does not match the durable submission")]
    TransactionIdentityMismatch,
    #[error("submission identity or frozen settlement scope does not match the case")]
    CaseScopeMismatch,
    #[error("submission identity does not match the governed action scope")]
    GovernedActionScopeMismatch,
    #[error("receipt block {block_number} is no longer canonical")]
    CanonicalBlockChanged { block_number: u64 },
    #[error("successful receipt call identity does not match frozen target/calldata")]
    CallEvidenceMismatch,
    #[error("receipt-block adapter runtime code hash does not match the frozen deployment")]
    TargetCodeHashMismatch,
    #[error("frozen relayer envelope is corrupt or does not prove the inner call")]
    RelayerEnvelopeMismatch,
    #[error("Polygon receipt reverted")]
    ReceiptReverted,
    #[error("outcome balances were not fully consumed")]
    OutcomeBalanceNotConsumed,
    #[error("ERC-1155 operator approval post-state does not match the authorized action")]
    OperatorApprovalStateMismatch,
    #[error("pUSD mint Transfer evidence is missing")]
    PusdMintMissing,
    #[error("pUSD mint Transfer evidence is ambiguous")]
    PusdMintAmbiguous,
    #[error("pUSD Wrapped payout evidence is missing")]
    WrappedPayoutMissing,
    #[error("pUSD Wrapped payout evidence is ambiguous")]
    WrappedPayoutAmbiguous,
    #[error("pUSD payout does not equal payout vector multiplied by frozen balances")]
    PayoutMismatch,
    #[error("settlement numeric evidence is invalid: {detail}")]
    InvalidNumericEvidence { detail: String },
    #[error("receipt payout log is malformed: {detail}")]
    InvalidPayoutLog { detail: String },
}

impl From<SettlementValueError> for SettlementConfirmationError {
    fn from(error: SettlementValueError) -> Self {
        if error.kind() == SettlementValueKind::Uint {
            Self::InvalidNumericEvidence {
                detail: error.detail().to_owned(),
            }
        } else {
            Self::InvalidPayoutLog {
                detail: error.detail().to_owned(),
            }
        }
    }
}

impl SettlementConfirmationError {
    #[must_use]
    pub const fn failure_code(&self) -> SettlementFailureCode {
        match self {
            Self::ReceiptReverted => SettlementFailureCode::OnChainReverted,
            Self::OutcomeBalanceNotConsumed => SettlementFailureCode::BalanceMismatch,
            Self::PayoutMismatch => SettlementFailureCode::PayoutMismatch,
            Self::WrongChain { .. }
            | Self::MissingTransactionHash
            | Self::TransactionIdentityMismatch
            | Self::CaseScopeMismatch
            | Self::GovernedActionScopeMismatch
            | Self::CanonicalBlockChanged { .. }
            | Self::CallEvidenceMismatch
            | Self::TargetCodeHashMismatch
            | Self::RelayerEnvelopeMismatch
            | Self::OperatorApprovalStateMismatch
            | Self::PusdMintMissing
            | Self::PusdMintAmbiguous
            | Self::WrappedPayoutMissing
            | Self::WrappedPayoutAmbiguous
            | Self::InvalidNumericEvidence { .. }
            | Self::InvalidPayoutLog { .. } => SettlementFailureCode::ReceiptEvidenceMismatch,
        }
    }
}

/// Verify finalized chain evidence against one immutable submission and case.
pub fn verify_settlement_confirmation(
    topology: &WalletTopology,
    redeem: &SettlementRedeemInfo,
    submission: &SettlementChainSubmissionInfo,
    observation: SettlementChainObservation,
) -> Result<SettlementConfirmationStatus, SettlementConfirmationError> {
    if topology.kind != redeem.wallet_kind
        || (topology.funder).into_evm_address()? != redeem.funder_address
    {
        return Err(SettlementConfirmationError::CaseScopeMismatch);
    }
    if observation.chain_id != POLYGON_CHAIN_ID {
        return Err(SettlementConfirmationError::WrongChain {
            actual: observation.chain_id,
        });
    }
    let transaction_hash = submission
        .transaction_hash
        .clone()
        .ok_or(SettlementConfirmationError::MissingTransactionHash)?;
    if observation.transaction.transaction_hash != transaction_hash
        || observation.receipt_transaction_hash != transaction_hash
    {
        return Err(SettlementConfirmationError::TransactionIdentityMismatch);
    }
    verify_case_scope(redeem, submission)?;
    if observation.finalized_block_number < observation.receipt_block_number {
        return Ok(SettlementConfirmationStatus::PendingFinality);
    }
    if observation.canonical_receipt_block_hash.as_ref() != Some(&observation.receipt_block_hash) {
        return Err(SettlementConfirmationError::CanonicalBlockChanged {
            block_number: observation.receipt_block_number,
        });
    }
    if !observation.receipt_success {
        return Err(SettlementConfirmationError::ReceiptReverted);
    }
    if observation.target_code_hash != submission.target_code_hash {
        return Err(SettlementConfirmationError::TargetCodeHashMismatch);
    }
    let call = verify_scope_identity(
        topology,
        redeem.wallet_kind,
        &redeem.funder_address,
        submission,
        &observation.transaction,
        true,
    )?;
    verify_zero_balances(redeem, &observation.balances_after)?;
    let expected_raw = expected_payout_raw(redeem)?;
    let (mint, wrapped) = exact_payout_evidence(
        redeem,
        submission,
        &observation.transfers,
        &observation.wrapped_payouts,
        expected_raw,
    )?;
    let actual_payout_usd = raw_pusd_to_usd(mint.raw_amount)?;
    if redeem.expected_payout_usd != Some(actual_payout_usd) {
        return Err(SettlementConfirmationError::PayoutMismatch);
    }
    let gas_fee_pol = gas_fee_pol(observation.gas_used, observation.effective_gas_price_wei)?;
    let receipt = SettlementReceiptEvidence {
        chain_id: observation.chain_id,
        transaction_hash,
        block_number: observation.receipt_block_number,
        block_hash: observation.receipt_block_hash,
        finalized_block_number: observation.finalized_block_number,
        finalized_block_hash: observation.finalized_block_hash,
        call,
        receipt_success: true,
        pusd_mint: SettlementPusdMintEvidence {
            token: mint.token,
            from: mint.from,
            to: mint.to,
            raw_amount: (mint.raw_amount).into_evm_uint()?,
            amount_usd: actual_payout_usd,
            log_index: mint.log_index,
        },
        wrapped_payout: SettlementWrappedPayoutEvidence {
            collateral_token: wrapped.collateral_token,
            caller: wrapped.caller,
            asset: wrapped.asset,
            to: wrapped.to,
            raw_amount: (wrapped.raw_amount).into_evm_uint()?,
            amount_usd: actual_payout_usd,
            log_index: wrapped.log_index,
        },
        canonical_checked_at: observation.observed_at,
        observed_at: observation.observed_at,
    };
    Ok(SettlementConfirmationStatus::Confirmed(Box::new(
        VerifiedSettlementConfirmation {
            receipt,
            balances_after: observation.balances_after,
            actual_payout_usd,
            gas_fee_pol,
        },
    )))
}

/// Decode one raw receipt log when it is an ERC-20 `Transfer` event.
pub fn decode_erc20_transfer(
    token: Address,
    topics: &[B256],
    data: &Bytes,
    log_index: u64,
) -> Result<Option<ObservedErc20Transfer>, SettlementConfirmationError> {
    let transfer_signature = keccak256("Transfer(address,address,uint256)");
    if topics.first() != Some(&transfer_signature) {
        return Ok(None);
    }
    if topics.len() != 3 || data.len() != 32 {
        return Err(SettlementConfirmationError::InvalidPayoutLog {
            detail: "Transfer requires three topics and 32 data bytes".to_owned(),
        });
    }
    let from = Address::from_slice(&topics[1].as_slice()[12..]);
    let to = Address::from_slice(&topics[2].as_slice()[12..]);
    Ok(Some(ObservedErc20Transfer {
        token: (token).into_evm_address()?,
        from: (from).into_evm_address()?,
        to: (to).into_evm_address()?,
        raw_amount: U256::from_be_slice(data.as_ref()),
        log_index,
    }))
}

/// Decode one raw receipt log when it is the pUSD `Wrapped` event.
pub fn decode_wrapped_payout(
    collateral_token: Address,
    topics: &[B256],
    data: &Bytes,
    log_index: u64,
) -> Result<Option<ObservedWrappedPayout>, SettlementConfirmationError> {
    let wrapped_signature = keccak256("Wrapped(address,address,address,uint256)");
    if topics.first() != Some(&wrapped_signature) {
        return Ok(None);
    }
    if topics.len() != 4 || data.len() != 32 {
        return Err(SettlementConfirmationError::InvalidPayoutLog {
            detail: "Wrapped requires four topics and 32 data bytes".to_owned(),
        });
    }
    let caller = Address::from_slice(&topics[1].as_slice()[12..]);
    let asset = Address::from_slice(&topics[2].as_slice()[12..]);
    let to = Address::from_slice(&topics[3].as_slice()[12..]);
    Ok(Some(ObservedWrappedPayout {
        collateral_token: (collateral_token).into_evm_address()?,
        caller: (caller).into_evm_address()?,
        asset: (asset).into_evm_address()?,
        to: (to).into_evm_address()?,
        raw_amount: U256::from_be_slice(data.as_ref()),
        log_index,
    }))
}

fn verify_scope_identity(
    topology: &WalletTopology,
    wallet_kind: ExecutionWalletKind,
    funder: &EvmAddress,
    submission: &SettlementChainSubmissionInfo,
    transaction: &SettlementTransactionObservation,
    allow_external: bool,
) -> Result<SettlementMinedCallEvidence, SettlementConfirmationError> {
    match submission.kind {
        SettlementSubmissionKind::DirectEoa => {
            if wallet_kind != ExecutionWalletKind::Eoa
                || transaction.outer_sender != *funder
                || transaction.outer_target != submission.call_target
                || calldata_hash(&transaction.input)? != submission.calldata_hash
            {
                return Err(SettlementConfirmationError::CallEvidenceMismatch);
            }
            Ok(SettlementMinedCallEvidence {
                wallet_kind,
                outer_sender: transaction.outer_sender.clone(),
                outer_target: transaction.outer_target.clone(),
                outer_calldata_hash: calldata_hash(&transaction.input)?,
                inner_target: submission.call_target.clone(),
                inner_calldata_hash: submission.calldata_hash.clone(),
            })
        }
        SettlementSubmissionKind::ExternallyObserved => {
            if !allow_external {
                return Err(SettlementConfirmationError::CallEvidenceMismatch);
            }
            verify_externally_observed_call(topology, wallet_kind, funder, submission, transaction)
        }
        SettlementSubmissionKind::Relayer => {
            verify_relayer_envelope(wallet_kind, funder, submission, transaction)
        }
    }
}

fn verify_externally_observed_call(
    topology: &WalletTopology,
    wallet_kind: ExecutionWalletKind,
    funder: &EvmAddress,
    submission: &SettlementChainSubmissionInfo,
    transaction: &SettlementTransactionObservation,
) -> Result<SettlementMinedCallEvidence, SettlementConfirmationError> {
    match wallet_kind {
        ExecutionWalletKind::Eoa => verify_external_eoa_call(funder, submission, transaction)?,
        ExecutionWalletKind::GnosisSafe => {
            verify_external_safe_call(funder, submission, transaction)?;
        }
        ExecutionWalletKind::Proxy => {
            verify_external_proxy_call(topology, submission, transaction)?;
        }
        ExecutionWalletKind::DepositWallet => {
            verify_deposit_call(funder, submission, transaction)?;
        }
    }
    Ok(SettlementMinedCallEvidence {
        wallet_kind,
        outer_sender: transaction.outer_sender.clone(),
        outer_target: transaction.outer_target.clone(),
        outer_calldata_hash: calldata_hash(&transaction.input)?,
        inner_target: submission.call_target.clone(),
        inner_calldata_hash: submission.calldata_hash.clone(),
    })
}

fn verify_external_eoa_call(
    funder: &EvmAddress,
    submission: &SettlementChainSubmissionInfo,
    transaction: &SettlementTransactionObservation,
) -> Result<(), SettlementConfirmationError> {
    if transaction.outer_sender != *funder
        || transaction.outer_target != submission.call_target
        || calldata_hash(&transaction.input)? != submission.calldata_hash
    {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    }
    Ok(())
}

fn verify_external_safe_call(
    funder: &EvmAddress,
    submission: &SettlementChainSubmissionInfo,
    transaction: &SettlementTransactionObservation,
) -> Result<(), SettlementConfirmationError> {
    if transaction.outer_target != *funder {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    }
    let mined = execTransactionCall::abi_decode(&transaction.input)
        .map_err(|_| SettlementConfirmationError::CallEvidenceMismatch)?;
    if (mined.to).into_evm_address()? != submission.call_target
        || !mined.value.is_zero()
        || mined.data.as_ref() != submission.calldata.as_slice()
        || mined.operation != 0
        || !mined.gasPrice.is_zero()
        || mined.gasToken != Address::ZERO
        || mined.refundReceiver != Address::ZERO
        || mined.signatures.is_empty()
    {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    }
    Ok(())
}

fn verify_external_proxy_call(
    topology: &WalletTopology,
    submission: &SettlementChainSubmissionInfo,
    transaction: &SettlementTransactionObservation,
) -> Result<(), SettlementConfirmationError> {
    if transaction.outer_target != relayer_address(PROXY_RELAY_HUB)? {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    }
    let mined = relayCallCall::abi_decode(&transaction.input)
        .map_err(|_| SettlementConfirmationError::CallEvidenceMismatch)?;
    if (mined.from).into_evm_address()? != (topology.signer).into_evm_address()?
        || (mined.recipient).into_evm_address()? != relayer_address(PROXY_FACTORY)?
        || !mined.transactionFee.is_zero()
        || !mined.gasPrice.is_zero()
        || mined.signature.is_empty()
        || !mined.approvalData.is_empty()
    {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    }
    let proxy = proxyCall::abi_decode(&mined.encodedFunction)
        .map_err(|_| SettlementConfirmationError::CallEvidenceMismatch)?;
    let [inner] = proxy.calls.as_slice() else {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    };
    if inner.typeCode != PROXY_CALL_TYPE
        || !inner.value.is_zero()
        || (inner.to).into_evm_address()? != submission.call_target
        || inner.data.as_ref() != submission.calldata.as_slice()
    {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    }
    Ok(())
}

fn verify_deposit_call(
    funder: &EvmAddress,
    submission: &SettlementChainSubmissionInfo,
    transaction: &SettlementTransactionObservation,
) -> Result<(), SettlementConfirmationError> {
    if transaction.outer_target != relayer_address(DEPOSIT_WALLET_FACTORY)? {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    }
    let mined = DepositWalletProxyCall::abi_decode(&transaction.input)
        .map_err(|_| SettlementConfirmationError::CallEvidenceMismatch)?;
    let [batch] = mined.batches.as_slice() else {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    };
    let [signature] = mined.signatures.as_slice() else {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    };
    let [call] = batch.calls.as_slice() else {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    };
    if (batch.wallet).into_evm_address()? != *funder
        || signature.is_empty()
        || (call.target).into_evm_address()? != submission.call_target
        || !call.value.is_zero()
        || call.data.as_ref() != submission.calldata.as_slice()
    {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    }
    Ok(())
}

fn verify_relayer_envelope(
    wallet_kind: ExecutionWalletKind,
    funder: &EvmAddress,
    submission: &SettlementChainSubmissionInfo,
    transaction: &SettlementTransactionObservation,
) -> Result<SettlementMinedCallEvidence, SettlementConfirmationError> {
    let envelope = submission
        .signed_envelope
        .as_deref()
        .ok_or(SettlementConfirmationError::RelayerEnvelopeMismatch)?;
    let expected_hash = submission
        .signed_envelope_hash
        .ok_or(SettlementConfirmationError::RelayerEnvelopeMismatch)?;
    if CanonicalDigest::content_hash_bytes(envelope) != expected_hash {
        return Err(SettlementConfirmationError::RelayerEnvelopeMismatch);
    }
    match wallet_kind {
        ExecutionWalletKind::GnosisSafe | ExecutionWalletKind::Proxy => {
            verify_safe_relay(wallet_kind, funder, submission, transaction, envelope)?;
        }
        ExecutionWalletKind::DepositWallet => {
            verify_deposit_wallet_envelope(funder, submission, transaction, envelope)?;
        }
        ExecutionWalletKind::Eoa => {
            return Err(SettlementConfirmationError::RelayerEnvelopeMismatch);
        }
    }
    Ok(SettlementMinedCallEvidence {
        wallet_kind,
        outer_sender: transaction.outer_sender.clone(),
        outer_target: transaction.outer_target.clone(),
        outer_calldata_hash: calldata_hash(&transaction.input)?,
        inner_target: submission.call_target.clone(),
        inner_calldata_hash: submission.calldata_hash.clone(),
    })
}

fn verify_safe_relay(
    wallet_kind: ExecutionWalletKind,
    funder: &EvmAddress,
    submission: &SettlementChainSubmissionInfo,
    transaction: &SettlementTransactionObservation,
    envelope: &[u8],
) -> Result<(), SettlementConfirmationError> {
    let body: FrozenSafeProxyRequest = serde_json::from_slice(envelope)
        .map_err(|_| SettlementConfirmationError::RelayerEnvelopeMismatch)?;
    if relayer_address(&body.proxy_wallet)? != *funder
        || submission.prepared_nonce.as_ref().map(EvmUint256::as_str) != Some(body.nonce.as_str())
        || parse_canonical_uint(&body.nonce).is_err()
    {
        return Err(SettlementConfirmationError::RelayerEnvelopeMismatch);
    }
    let data = decode_hex(&body.data)?;
    match body.tx_type.as_str() {
        "SAFE" => {
            verify_safe_mined_call(wallet_kind, funder, submission, transaction, &body, &data)?;
        }
        "PROXY" => {
            verify_proxy_mined_call(wallet_kind, submission, transaction, &body, &data)?;
        }
        _ => return Err(SettlementConfirmationError::RelayerEnvelopeMismatch),
    }
    Ok(())
}

fn verify_deposit_wallet_envelope(
    funder: &EvmAddress,
    submission: &SettlementChainSubmissionInfo,
    transaction: &SettlementTransactionObservation,
    envelope: &[u8],
) -> Result<(), SettlementConfirmationError> {
    let body: FrozenDepositWalletRequest = serde_json::from_slice(envelope)
        .map_err(|_| SettlementConfirmationError::RelayerEnvelopeMismatch)?;
    let params = &body.deposit_wallet_params;
    let [call] = params.calls.as_slice() else {
        return Err(SettlementConfirmationError::RelayerEnvelopeMismatch);
    };
    let nonce = parse_canonical_uint(&body.nonce)?;
    let deadline = parse_canonical_uint(&params.deadline)?;
    let signature = decode_hex(&body.signature)?;
    let calldata = decode_hex(&call.data)?;
    let factory = relayer_address(DEPOSIT_WALLET_FACTORY)?;
    if body.tx_type != "WALLET"
        || relayer_address(&body.to)? != factory
        || relayer_address(&params.deposit_wallet)? != *funder
        || submission.prepared_nonce.as_ref().map(EvmUint256::as_str) != Some(body.nonce.as_str())
        || relayer_address(&call.target)? != submission.call_target
        || parse_canonical_uint(&call.value)? != U256::ZERO
        || calldata != submission.calldata
        || transaction.outer_target != factory
    {
        return Err(SettlementConfirmationError::RelayerEnvelopeMismatch);
    }
    let mined = DepositWalletProxyCall::abi_decode(&transaction.input)
        .map_err(|_| SettlementConfirmationError::CallEvidenceMismatch)?;
    let [batch] = mined.batches.as_slice() else {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    };
    let [mined_signature] = mined.signatures.as_slice() else {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    };
    let [mined_call] = batch.calls.as_slice() else {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    };
    if (batch.wallet).into_evm_address()? != *funder
        || batch.nonce != nonce
        || batch.deadline != deadline
        || (mined_call.target).into_evm_address()? != submission.call_target
        || !mined_call.value.is_zero()
        || mined_call.data.as_ref() != submission.calldata.as_slice()
        || mined_signature.as_ref() != signature.as_slice()
    {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    }
    Ok(())
}

fn verify_safe_mined_call(
    wallet_kind: ExecutionWalletKind,
    funder: &EvmAddress,
    submission: &SettlementChainSubmissionInfo,
    transaction: &SettlementTransactionObservation,
    body: &FrozenSafeProxyRequest,
    data: &[u8],
) -> Result<(), SettlementConfirmationError> {
    if wallet_kind != ExecutionWalletKind::GnosisSafe
        || transaction.outer_target != *funder
        || relayer_address(&body.to)? != submission.call_target
        || data != submission.calldata
        || !body.metadata.is_empty()
        || body.signature_params.gas_price.as_deref() != Some("0")
        || body.signature_params.operation.as_deref() != Some("0")
        || body.signature_params.safe_txn_gas.as_deref() != Some("0")
        || body.signature_params.base_gas.as_deref() != Some("0")
        || canonical_optional_address(body.signature_params.gas_token.as_deref())?
            != Some(Address::ZERO)
        || canonical_optional_address(body.signature_params.refund_receiver.as_deref())?
            != Some(Address::ZERO)
        || body.signature_params.relayer_fee.is_some()
        || body.signature_params.gas_limit.is_some()
        || body.signature_params.relay_hub.is_some()
        || body.signature_params.relay.is_some()
    {
        return Err(SettlementConfirmationError::RelayerEnvelopeMismatch);
    }
    let mined = execTransactionCall::abi_decode(&transaction.input)
        .map_err(|_| SettlementConfirmationError::CallEvidenceMismatch)?;
    let signature = decode_hex(&body.signature)?;
    if (mined.to).into_evm_address()? != submission.call_target
        || !mined.value.is_zero()
        || mined.data.as_ref() != submission.calldata.as_slice()
        || mined.operation != 0
        || !mined.safeTxGas.is_zero()
        || !mined.baseGas.is_zero()
        || !mined.gasPrice.is_zero()
        || mined.gasToken != Address::ZERO
        || mined.refundReceiver != Address::ZERO
        || mined.signatures.as_ref() != signature.as_slice()
    {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    }
    Ok(())
}

fn verify_proxy_mined_call(
    wallet_kind: ExecutionWalletKind,
    submission: &SettlementChainSubmissionInfo,
    transaction: &SettlementTransactionObservation,
    body: &FrozenSafeProxyRequest,
    data: &[u8],
) -> Result<(), SettlementConfirmationError> {
    let relay_hub = relayer_address(PROXY_RELAY_HUB)?;
    let relay = body
        .signature_params
        .relay
        .as_deref()
        .ok_or(SettlementConfirmationError::RelayerEnvelopeMismatch)
        .and_then(relayer_address)?;
    if wallet_kind != ExecutionWalletKind::Proxy
        || transaction.outer_sender != relay
        || transaction.outer_target != relay_hub
        || body.signature_params.gas_price.as_deref() != Some("0")
        || body.signature_params.relayer_fee.as_deref() != Some("0")
        || body
            .signature_params
            .relay_hub
            .as_deref()
            .map(relayer_address)
            .transpose()?
            != Some(relay_hub)
        || body.signature_params.operation.is_some()
        || body.signature_params.safe_txn_gas.is_some()
        || body.signature_params.base_gas.is_some()
        || body.signature_params.gas_token.is_some()
        || body.signature_params.refund_receiver.is_some()
        || !body.metadata.is_empty()
    {
        return Err(SettlementConfirmationError::RelayerEnvelopeMismatch);
    }
    let call = proxyCall::abi_decode(data)
        .map_err(|_| SettlementConfirmationError::RelayerEnvelopeMismatch)?;
    let [inner] = call.calls.as_slice() else {
        return Err(SettlementConfirmationError::RelayerEnvelopeMismatch);
    };
    if inner.typeCode != PROXY_CALL_TYPE
        || !inner.value.is_zero()
        || (inner.to).into_evm_address()? != submission.call_target
        || inner.data.as_ref() != submission.calldata.as_slice()
    {
        return Err(SettlementConfirmationError::RelayerEnvelopeMismatch);
    }
    let mined = relayCallCall::abi_decode(&transaction.input)
        .map_err(|_| SettlementConfirmationError::CallEvidenceMismatch)?;
    let gas_limit = body
        .signature_params
        .gas_limit
        .as_deref()
        .ok_or(SettlementConfirmationError::RelayerEnvelopeMismatch)
        .and_then(parse_canonical_uint)?;
    let nonce = parse_canonical_uint(&body.nonce)?;
    let signature = decode_hex(&body.signature)?;
    if (mined.from).into_evm_address()? != relayer_address(&body.from)?
        || (mined.recipient).into_evm_address()? != relayer_address(&body.to)?
        || mined.encodedFunction.as_ref() != data
        || !mined.transactionFee.is_zero()
        || !mined.gasPrice.is_zero()
        || mined.gasLimit != gas_limit
        || mined.nonce != nonce
        || mined.signature.as_ref() != signature.as_slice()
        || !mined.approvalData.is_empty()
    {
        return Err(SettlementConfirmationError::CallEvidenceMismatch);
    }
    Ok(())
}

fn canonical_optional_address(
    value: Option<&str>,
) -> Result<Option<Address>, SettlementConfirmationError> {
    value
        .map(Address::from_str)
        .transpose()
        .map_err(|_| SettlementConfirmationError::RelayerEnvelopeMismatch)
}

fn parse_canonical_uint(value: &str) -> Result<U256, SettlementConfirmationError> {
    let parsed =
        parse_uint(value).map_err(|_| SettlementConfirmationError::RelayerEnvelopeMismatch)?;
    if parsed.to_string() != value {
        return Err(SettlementConfirmationError::RelayerEnvelopeMismatch);
    }
    Ok(parsed)
}

fn verify_case_scope(
    redeem: &SettlementRedeemInfo,
    submission: &SettlementChainSubmissionInfo,
) -> Result<(), SettlementConfirmationError> {
    if submission.settlement_redeem_id != Some(redeem.settlement_redeem_id)
        || submission.purpose != SettlementSubmissionPurpose::Redeem
        || submission.state != SettlementSubmissionState::AwaitingFinality
        || submission.route != redeem.route
    {
        return Err(SettlementConfirmationError::CaseScopeMismatch);
    }
    if redeem.target_adapter.as_ref() != Some(&submission.target_adapter)
        || redeem.target_code_hash.as_ref() != Some(&submission.target_code_hash)
        || redeem.deployment_digest != Some(submission.deployment_digest)
        || redeem.deployment_evidence_version.as_ref()
            != Some(&submission.deployment_evidence_version)
        || redeem.verified_block_number != Some(submission.verified_block_number)
        || redeem.verified_block_hash.as_ref() != Some(&submission.verified_block_hash)
    {
        return Err(SettlementConfirmationError::CaseScopeMismatch);
    }
    Ok(())
}

fn verify_zero_balances(
    redeem: &SettlementRedeemInfo,
    balances: &SettlementBalanceEvidence,
) -> Result<(), SettlementConfirmationError> {
    let before = redeem
        .balance_before_json
        .as_ref()
        .ok_or(SettlementConfirmationError::OutcomeBalanceNotConsumed)?;
    if balances.yes.token_id != before.yes.token_id
        || balances.no.token_id != before.no.token_id
        || balances.yes.raw_balance.as_str() != "0"
        || balances.no.raw_balance.as_str() != "0"
        || !balances.yes.shares.is_zero()
        || !balances.no.shares.is_zero()
    {
        return Err(SettlementConfirmationError::OutcomeBalanceNotConsumed);
    }
    Ok(())
}

fn expected_payout_raw(redeem: &SettlementRedeemInfo) -> Result<U256, SettlementConfirmationError> {
    let balances = redeem
        .balance_before_json
        .as_ref()
        .ok_or_else(|| invalid_numeric("missing frozen balance evidence"))?;
    let denominator = parse_uint(redeem.payout_vector_json.denominator.as_str())?;
    if denominator.is_zero() {
        return Err(invalid_numeric("zero payout denominator"));
    }
    let yes = payout_component(
        parse_uint(balances.yes.raw_balance.as_str())?,
        parse_uint(redeem.payout_vector_json.yes.as_str())?,
        denominator,
    )?;
    let no = payout_component(
        parse_uint(balances.no.raw_balance.as_str())?,
        parse_uint(redeem.payout_vector_json.no.as_str())?,
        denominator,
    )?;
    yes.checked_add(no)
        .ok_or_else(|| invalid_numeric("payout sum overflows uint256"))
}

fn payout_component(
    balance: U256,
    numerator: U256,
    denominator: U256,
) -> Result<U256, SettlementConfirmationError> {
    balance
        .checked_mul(numerator)
        .map(|product| product / denominator)
        .ok_or_else(|| invalid_numeric("payout multiplication overflows uint256"))
}

fn exact_payout_evidence(
    redeem: &SettlementRedeemInfo,
    submission: &SettlementChainSubmissionInfo,
    transfers: &[ObservedErc20Transfer],
    wrapped_payouts: &[ObservedWrappedPayout],
    expected_raw: U256,
) -> Result<(ObservedErc20Transfer, ObservedWrappedPayout), SettlementConfirmationError> {
    let pusd = canonical_address(submission.collateral_token.as_str())?;
    let usdce = canonical_address(submission.usdce.as_str())?;
    let mint_matches: Vec<&ObservedErc20Transfer> = transfers
        .iter()
        .filter(|transfer| transfer.token == pusd && transfer.to == redeem.funder_address)
        .collect();
    let [mint] = mint_matches.as_slice() else {
        return if mint_matches.is_empty() {
            Err(SettlementConfirmationError::PusdMintMissing)
        } else {
            Err(SettlementConfirmationError::PusdMintAmbiguous)
        };
    };
    let wrapped_matches: Vec<&ObservedWrappedPayout> = wrapped_payouts
        .iter()
        .filter(|wrapped| wrapped.collateral_token == pusd && wrapped.to == redeem.funder_address)
        .collect();
    let [wrapped] = wrapped_matches.as_slice() else {
        return if wrapped_matches.is_empty() {
            Err(SettlementConfirmationError::WrappedPayoutMissing)
        } else {
            Err(SettlementConfirmationError::WrappedPayoutAmbiguous)
        };
    };
    if mint.from != canonical_address("0x0000000000000000000000000000000000000000")?
        || mint.raw_amount != expected_raw
        || wrapped.caller != submission.target_adapter
        || wrapped.asset != usdce
        || wrapped.raw_amount != expected_raw
        || wrapped.raw_amount != mint.raw_amount
    {
        return Err(SettlementConfirmationError::PayoutMismatch);
    }
    Ok(((*mint).clone(), (*wrapped).clone()))
}

fn gas_fee_pol(
    gas_used: u64,
    effective_gas_price_wei: u128,
) -> Result<Decimal, SettlementConfirmationError> {
    let price = Decimal::from_str(&effective_gas_price_wei.to_string())
        .map_err(|error| invalid_numeric(&format!("gas price: {error}")))?;
    Ok(Decimal::from(gas_used) * price / Decimal::from(1_000_000_000_000_000_000_u64))
}

fn raw_pusd_to_usd(raw: U256) -> Result<Usd, SettlementConfirmationError> {
    let decimal = Decimal::from_str(&raw.to_string())
        .map_err(|error| invalid_numeric(&format!("pUSD raw amount: {error}")))?;
    Ok(Usd::new(decimal / Decimal::from(PUSD_SCALE)))
}

fn parse_uint(value: &str) -> Result<U256, SettlementConfirmationError> {
    U256::from_str(value).map_err(|error| invalid_numeric(&error.to_string()))
}

fn observed_token_balance(
    token_id: TokenId,
    raw: U256,
) -> Result<SettlementTokenBalance, SettlementConfirmationReadError> {
    let raw_text = raw.to_string();
    let decimal = Decimal::from_str(&raw_text).map_err(|error| {
        SettlementConfirmationReadError::InvalidEvidence {
            detail: error.to_string(),
        }
    })?;
    Ok(SettlementTokenBalance {
        token_id,
        raw_balance: EvmUint256::parse(raw_text).map_err(|error| {
            SettlementConfirmationReadError::InvalidEvidence {
                detail: error.to_string(),
            }
        })?,
        shares: Shares::new(decimal / Decimal::from(PUSD_SCALE)),
    })
}

fn read_error(operation: &'static str, error: &impl Display) -> SettlementConfirmationReadError {
    SettlementConfirmationReadError::RpcCall {
        operation,
        detail: error.to_string(),
    }
}

fn canonical_address(value: &str) -> Result<EvmAddress, SettlementConfirmationError> {
    EvmAddress::parse(value).map_err(|error| SettlementConfirmationError::InvalidPayoutLog {
        detail: error.to_string(),
    })
}

fn relayer_address(value: &str) -> Result<EvmAddress, SettlementConfirmationError> {
    EvmAddress::parse(value).map_err(|_| SettlementConfirmationError::RelayerEnvelopeMismatch)
}

fn calldata_hash(value: &[u8]) -> Result<EvmCalldataHash, SettlementConfirmationError> {
    EvmCalldataHash::parse(format!("{:#x}", keccak256(value)))
        .map_err(|error| invalid_numeric(&error.to_string()))
}

fn decode_hex(value: &str) -> Result<Vec<u8>, SettlementConfirmationError> {
    let value = value
        .strip_prefix("0x")
        .ok_or(SettlementConfirmationError::RelayerEnvelopeMismatch)?;
    hex::decode(value).map_err(|_| SettlementConfirmationError::RelayerEnvelopeMismatch)
}

fn invalid_numeric(detail: &str) -> SettlementConfirmationError {
    SettlementConfirmationError::InvalidNumericEvidence {
        detail: detail.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use alloy::{
        primitives::{Address, B256, Bytes, U256, keccak256},
        sol_types::SolCall,
    };
    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_models::{
        config::{OnchainConfig, PolygonRpcEndpoint},
        domain::quant::{
            settlement::{SettlementChainSubmissionInfo, SettlementRedeemInfo},
            settlement_governance::SettlementGovernedActionInfo,
        },
        enums::{
            quant::ExecutionWalletKind,
            settlement::{
                SettlementAuthorizationState, SettlementCaseState, SettlementEffectivePolicy,
                SettlementGovernedActionKind, SettlementGovernedActionState,
                SettlementReadinessStatus, SettlementReconciliationState, SettlementRoute,
                SettlementSubmissionKind, SettlementSubmissionPurpose, SettlementSubmissionState,
            },
        },
        hashing::CanonicalDigest,
        types::{
            ContentHash, EvmAddress, EvmBlockHash, EvmCodeHash, EvmTransactionHash, EvmUint256,
            ExecutionAccountId, MarketId, RelayerTransactionId, SettlementActionIdempotencyKey,
            SettlementChainSubmissionId, SettlementEvidenceVersion, SettlementGovernedActionId,
            SettlementRedeemId, Shares, TokenId, Usd, UserId,
            settlement_payload::{
                SettlementBalanceEvidence, SettlementFailureHistory, SettlementPayoutVector,
                SettlementReadinessEvidence, SettlementTokenBalance,
            },
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use serde_json::{Value, json};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate, matchers::method};

    use super::{
        AlloySettlementConfirmationReader, ObservedErc20Transfer, ObservedWrappedPayout,
        SettlementChainObservation, SettlementConfirmationError, SettlementConfirmationReader,
        SettlementConfirmationStatus, SettlementOperatorApprovalConfirmationStatus,
        SettlementOperatorApprovalObservation, SettlementTransactionObservation, WalletTopology,
        calldata_hash, decode_erc20_transfer, decode_wrapped_payout,
        verify_operator_approval_confirmation,
        verify_settlement_confirmation as verify_with_topology,
    };
    use crate::settlement::relayer::deposit_wallet_wire::{
        Batch as DepositWalletBatch, Call as DepositWalletCall,
        Factory::proxyCall as DepositWalletProxyCall,
    };

    const CONDITIONAL_TOKENS: &str = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045";
    const PUSD: &str = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb";
    const USDCE: &str = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174";

    fn verify_settlement_confirmation(
        redeem: &SettlementRedeemInfo,
        submission: &SettlementChainSubmissionInfo,
        observation: SettlementChainObservation,
    ) -> Result<SettlementConfirmationStatus, SettlementConfirmationError> {
        let funder = Address::from_str(redeem.funder_address.as_str()).expect("fixture funder");
        let signer = if redeem.wallet_kind == ExecutionWalletKind::Eoa {
            funder
        } else {
            Address::from_str("0x2222222222222222222222222222222222222222").expect("fixture signer")
        };
        let topology = WalletTopology::attested(redeem.wallet_kind, signer, signer, funder);
        verify_with_topology(&topology, redeem, submission, observation)
    }

    #[tokio::test]
    async fn alloy_reader_reads_block() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(rpc_response)
            .mount(&server)
            .await;
        let reader = AlloySettlementConfirmationReader::connect(&OnchainConfig {
            rpc_endpoint: PolygonRpcEndpoint::Public { url: server.uri() },
            rpc_timeout_ms: 5_000,
        })
        .expect("test confirmation reader");
        let mut redeem = redeem_case();
        let mut submission = submission(
            SettlementSubmissionKind::DirectEoa,
            current_adapter(),
            &redeem,
        );
        let fixture_code_hash = code_hash(&format!("{:#x}", keccak256([0x60, 0x00])));
        redeem.target_code_hash = Some(fixture_code_hash.clone());
        submission.target_code_hash = fixture_code_hash;
        let observation = reader
            .observe(&redeem, &submission)
            .await
            .expect("read fixed receipt evidence")
            .expect("receipt exists");
        assert_eq!(observation.receipt_block_number, 100);
        assert_eq!(
            observation.canonical_receipt_block_hash,
            Some(block_hash(0x20))
        );
        assert!(matches!(
            verify_settlement_confirmation(&redeem, &submission, observation),
            Ok(SettlementConfirmationStatus::Confirmed(_))
        ));
    }

    #[test]
    fn finalized_receipt_requires_balances() {
        let redeem = redeem_case();
        let submission = submission(
            SettlementSubmissionKind::DirectEoa,
            current_adapter(),
            &redeem,
        );
        let confirmed =
            verify_settlement_confirmation(&redeem, &submission, observation(&submission))
                .expect("valid confirmation evidence");
        let SettlementConfirmationStatus::Confirmed(evidence) = confirmed else {
            panic!("expected confirmed evidence");
        };
        assert_eq!(evidence.actual_payout_usd, Usd::new(dec!(1)));
        assert_eq!(
            evidence.receipt.call.inner_calldata_hash,
            submission.calldata_hash
        );
        assert_eq!(evidence.receipt.pusd_mint.from, zero_address());
        assert_eq!(evidence.receipt.wrapped_payout.caller, current_adapter());
        assert_eq!(
            evidence.receipt.wrapped_payout.asset,
            address("0x2791bca1f2de4661ed88a30c99a7a9449aa84174")
        );

        let mut missing_log = observation(&submission);
        missing_log.transfers.clear();
        assert_eq!(
            verify_settlement_confirmation(&redeem, &submission, missing_log),
            Err(SettlementConfirmationError::PusdMintMissing)
        );

        let mut missing_wrapped = observation(&submission);
        missing_wrapped.wrapped_payouts.clear();
        assert_eq!(
            verify_settlement_confirmation(&redeem, &submission, missing_wrapped),
            Err(SettlementConfirmationError::WrappedPayoutMissing)
        );

        let mut wrong_minter = observation(&submission);
        wrong_minter.transfers[0].from = current_adapter();
        assert_eq!(
            verify_settlement_confirmation(&redeem, &submission, wrong_minter),
            Err(SettlementConfirmationError::PayoutMismatch)
        );

        let mut wrong_wrapper = observation(&submission);
        wrong_wrapper.wrapped_payouts[0].caller = funder();
        assert_eq!(
            verify_settlement_confirmation(&redeem, &submission, wrong_wrapper),
            Err(SettlementConfirmationError::PayoutMismatch)
        );

        let mut wrong_asset = observation(&submission);
        wrong_asset.wrapped_payouts[0].asset = current_adapter();
        assert_eq!(
            verify_settlement_confirmation(&redeem, &submission, wrong_asset),
            Err(SettlementConfirmationError::PayoutMismatch)
        );

        let mut nonzero_balance = observation(&submission);
        nonzero_balance.balances_after.yes.raw_balance = uint("1");
        nonzero_balance.balances_after.yes.shares = Shares::new(dec!(0.000001));
        assert_eq!(
            verify_settlement_confirmation(&redeem, &submission, nonzero_balance),
            Err(SettlementConfirmationError::OutcomeBalanceNotConsumed)
        );
    }

    #[test]
    fn finality_reorg_mismatch_distinct() {
        let redeem = redeem_case();
        let submission = submission(
            SettlementSubmissionKind::DirectEoa,
            current_adapter(),
            &redeem,
        );
        let mut pending = observation(&submission);
        pending.finalized_block_number = pending.receipt_block_number - 1;
        assert_eq!(
            verify_settlement_confirmation(&redeem, &submission, pending),
            Ok(SettlementConfirmationStatus::PendingFinality)
        );

        let mut reorg = observation(&submission);
        reorg.canonical_receipt_block_hash = Some(block_hash(0x99));
        assert_eq!(
            verify_settlement_confirmation(&redeem, &submission, reorg),
            Err(SettlementConfirmationError::CanonicalBlockChanged { block_number: 100 })
        );

        let mut reverted = observation(&submission);
        reverted.receipt_success = false;
        assert_eq!(
            verify_settlement_confirmation(&redeem, &submission, reverted),
            Err(SettlementConfirmationError::ReceiptReverted)
        );

        let mut wrong_payout = observation(&submission);
        wrong_payout.transfers[0].raw_amount = U256::from(999_999_u64);
        assert_eq!(
            verify_settlement_confirmation(&redeem, &submission, wrong_payout),
            Err(SettlementConfirmationError::PayoutMismatch)
        );
    }

    #[test]
    fn operator_approval_requires_state() {
        let redeem = redeem_case();
        let action = governed_approval_action(&redeem);
        let submission = operator_approval_submission(&redeem, &action);
        let topology = WalletTopology::attested(
            ExecutionWalletKind::Eoa,
            Address::from_str(funder().as_str()).expect("funder signer"),
            Address::from_str(funder().as_str()).expect("funder owner"),
            Address::from_str(funder().as_str()).expect("funder wallet"),
        );

        assert!(matches!(
            verify_operator_approval_confirmation(
                &topology,
                &action,
                &submission,
                operator_approval_observation(&submission),
            ),
            Ok(SettlementOperatorApprovalConfirmationStatus::Confirmed(_))
        ));

        let mut noncanonical = operator_approval_observation(&submission);
        noncanonical.canonical_receipt_block_hash = Some(block_hash(0x99));
        assert_eq!(
            verify_operator_approval_confirmation(&topology, &action, &submission, noncanonical,),
            Err(SettlementConfirmationError::CanonicalBlockChanged { block_number: 100 })
        );

        let mut wrong_call = operator_approval_observation(&submission);
        wrong_call.transaction.outer_target = current_adapter();
        assert_eq!(
            verify_operator_approval_confirmation(&topology, &action, &submission, wrong_call),
            Err(SettlementConfirmationError::CallEvidenceMismatch)
        );

        let mut wrong_post_state = operator_approval_observation(&submission);
        wrong_post_state.operator_approved = false;
        assert_eq!(
            verify_operator_approval_confirmation(
                &topology,
                &action,
                &submission,
                wrong_post_state,
            ),
            Err(SettlementConfirmationError::OperatorApprovalStateMismatch)
        );

        let mut wrong_target = submission;
        wrong_target.target_adapter = address("0x3333333333333333333333333333333333333333");
        assert_eq!(
            verify_operator_approval_confirmation(
                &topology,
                &action,
                &wrong_target,
                operator_approval_observation(&wrong_target),
            ),
            Err(SettlementConfirmationError::GovernedActionScopeMismatch)
        );
    }

    #[test]
    fn safe_relayer_proves_wrapper() {
        let mut redeem = redeem_case();
        redeem.wallet_kind = ExecutionWalletKind::GnosisSafe;
        let mut relayer = submission(
            SettlementSubmissionKind::Relayer,
            current_adapter(),
            &redeem,
        );
        let signature = vec![0x44; 65];
        let envelope = serde_json::to_vec(&serde_json::json!({
            "type": "SAFE",
            "from": "0x2222222222222222222222222222222222222222",
            "proxyWallet": funder().as_str(),
            "to": current_adapter().as_str(),
            "data": format!("0x{}", hex::encode(&relayer.calldata)),
            "nonce": "7",
            "signature": format!("0x{}", hex::encode(&signature)),
            "signatureParams": {
                "gasPrice": "0",
                "operation": "0",
                "safeTxnGas": "0",
                "baseGas": "0",
                "gasToken": "0x0000000000000000000000000000000000000000",
                "refundReceiver": "0x0000000000000000000000000000000000000000"
            },
            "metadata": ""
        }))
        .expect("relayer envelope");
        relayer.signed_envelope_hash = Some(CanonicalDigest::content_hash_bytes(&envelope));
        relayer.signed_envelope = Some(envelope);
        relayer.prepared_nonce = Some(uint("7"));
        relayer.relayer_transaction_id =
            Some(RelayerTransactionId::parse("relayer-confirmation-test").expect("relayer id"));
        let mut mined = observation(&relayer);
        mined.transaction.outer_sender = address("0x3333333333333333333333333333333333333333");
        mined.transaction.outer_target = funder();
        mined.transaction.input = super::execTransactionCall {
            to: Address::from_str(current_adapter().as_str()).expect("adapter address"),
            value: U256::ZERO,
            data: Bytes::copy_from_slice(&relayer.calldata),
            operation: 0,
            safeTxGas: U256::ZERO,
            baseGas: U256::ZERO,
            gasPrice: U256::ZERO,
            gasToken: Address::ZERO,
            refundReceiver: Address::ZERO,
            signatures: Bytes::from(signature.clone()),
        }
        .abi_encode();
        assert!(matches!(
            verify_settlement_confirmation(&redeem, &relayer, mined.clone()),
            Ok(SettlementConfirmationStatus::Confirmed(_))
        ));
        let external = externally_observed(relayer.clone());
        assert!(matches!(
            verify_settlement_confirmation(&redeem, &external, mined.clone()),
            Ok(SettlementConfirmationStatus::Confirmed(_))
        ));
        let mut mismatched_signature = signature;
        mismatched_signature[0] ^= 0x01;
        mined.transaction.input = super::execTransactionCall {
            to: Address::from_str(current_adapter().as_str()).expect("adapter address"),
            value: U256::ZERO,
            data: Bytes::copy_from_slice(&relayer.calldata),
            operation: 0,
            safeTxGas: U256::ZERO,
            baseGas: U256::ZERO,
            gasPrice: U256::ZERO,
            gasToken: Address::ZERO,
            refundReceiver: Address::ZERO,
            signatures: Bytes::from(mismatched_signature),
        }
        .abi_encode();
        assert_eq!(
            verify_settlement_confirmation(&redeem, &relayer, mined),
            Err(SettlementConfirmationError::CallEvidenceMismatch)
        );
    }

    #[test]
    fn proxy_relayer_proves_transaction() {
        let mut redeem = redeem_case();
        redeem.wallet_kind = ExecutionWalletKind::Proxy;
        let mut relayer = submission(
            SettlementSubmissionKind::Relayer,
            current_adapter(),
            &redeem,
        );
        let adapter = Address::from_str(current_adapter().as_str()).expect("adapter address");
        let proxy_data = super::proxyCall {
            calls: vec![super::ProxyCall {
                typeCode: 1,
                to: adapter,
                value: U256::ZERO,
                data: Bytes::copy_from_slice(&relayer.calldata),
            }],
        }
        .abi_encode();
        let signer = address("0x2222222222222222222222222222222222222222");
        let factory = address("0xab45c5a4b0c941a2f231c04c3f49182e1a254052");
        let relay_hub = address("0xd216153c06e857cd7f72665e0af1d7d82172f494");
        let relay = address("0x3333333333333333333333333333333333333333");
        let signature = vec![0x55; 65];
        let envelope = serde_json::to_vec(&serde_json::json!({
            "type": "PROXY",
            "from": signer.as_str(),
            "to": factory.as_str(),
            "proxyWallet": funder().as_str(),
            "data": format!("0x{}", hex::encode(&proxy_data)),
            "nonce": "9",
            "signature": format!("0x{}", hex::encode(&signature)),
            "signatureParams": {
                "gasPrice": "0",
                "relayerFee": "0",
                "gasLimit": "120000",
                "relayHub": relay_hub.as_str(),
                "relay": relay.as_str()
            },
            "metadata": ""
        }))
        .expect("proxy envelope");
        relayer.signed_envelope_hash = Some(CanonicalDigest::content_hash_bytes(&envelope));
        relayer.signed_envelope = Some(envelope);
        relayer.prepared_nonce = Some(uint("9"));
        relayer.relayer_transaction_id =
            Some(RelayerTransactionId::parse("relayer-proxy-test").expect("relayer id"));
        let mut mined = observation(&relayer);
        mined.transaction.outer_sender = relay;
        mined.transaction.outer_target = relay_hub;
        mined.transaction.input = super::relayCallCall {
            from: Address::from_str(signer.as_str()).expect("signer"),
            recipient: Address::from_str(factory.as_str()).expect("factory"),
            encodedFunction: Bytes::from(proxy_data),
            transactionFee: U256::ZERO,
            gasPrice: U256::ZERO,
            gasLimit: U256::from(120_000_u64),
            nonce: U256::from(9_u64),
            signature: Bytes::from(signature),
            approvalData: Bytes::new(),
        }
        .abi_encode();
        assert!(matches!(
            verify_settlement_confirmation(&redeem, &relayer, mined.clone()),
            Ok(SettlementConfirmationStatus::Confirmed(_))
        ));
        assert!(matches!(
            verify_settlement_confirmation(
                &redeem,
                &externally_observed(relayer.clone()),
                mined.clone()
            ),
            Ok(SettlementConfirmationStatus::Confirmed(_))
        ));
        mined.transaction.outer_sender = funder();
        assert_eq!(
            verify_settlement_confirmation(&redeem, &relayer, mined),
            Err(SettlementConfirmationError::RelayerEnvelopeMismatch)
        );
    }

    #[test]
    fn deposit_wallet_proves_call() {
        let mut redeem = redeem_case();
        redeem.wallet_kind = ExecutionWalletKind::DepositWallet;
        let mut relayer = submission(
            SettlementSubmissionKind::Relayer,
            current_adapter(),
            &redeem,
        );
        let factory = address("0x00000000000fb5c9adea0298d729a0cb3823cc07");
        let signature = vec![0x66; 65];
        let envelope = serde_json::to_vec(&serde_json::json!({
            "type": "WALLET",
            "from": "0x2222222222222222222222222222222222222222",
            "to": factory.as_str(),
            "nonce": "1",
            "signature": format!("0x{}", hex::encode(&signature)),
            "depositWalletParams": {
                "depositWallet": funder().as_str(),
                "deadline": "2000000000",
                "calls": [{
                    "target": current_adapter().as_str(),
                    "value": "0",
                    "data": format!("0x{}", hex::encode(&relayer.calldata))
                }]
            }
        }))
        .expect("deposit wallet envelope");
        relayer.signed_envelope_hash = Some(CanonicalDigest::content_hash_bytes(&envelope));
        relayer.signed_envelope = Some(envelope);
        relayer.relayer_transaction_id =
            Some(RelayerTransactionId::parse("relayer-wallet-test").expect("relayer id"));
        let mut mined = observation(&relayer);
        mined.transaction.outer_sender = address("0x3333333333333333333333333333333333333333");
        mined.transaction.outer_target = factory;
        mined.transaction.input = DepositWalletProxyCall {
            batches: vec![DepositWalletBatch {
                wallet: Address::from_str(funder().as_str()).expect("wallet"),
                nonce: U256::from(1_u64),
                deadline: U256::from(2_000_000_000_u64),
                calls: vec![DepositWalletCall {
                    target: Address::from_str(current_adapter().as_str()).expect("adapter"),
                    value: U256::ZERO,
                    data: Bytes::copy_from_slice(&relayer.calldata),
                }],
            }],
            signatures: vec![Bytes::from(signature.clone())],
        }
        .abi_encode();
        assert!(matches!(
            verify_settlement_confirmation(&redeem, &relayer, mined.clone()),
            Ok(SettlementConfirmationStatus::Confirmed(_))
        ));
        assert!(matches!(
            verify_settlement_confirmation(
                &redeem,
                &externally_observed(relayer.clone()),
                mined.clone()
            ),
            Ok(SettlementConfirmationStatus::Confirmed(_))
        ));
        let mut mismatched_signature = signature;
        mismatched_signature[0] ^= 0x01;
        mined.transaction.input = DepositWalletProxyCall {
            batches: vec![DepositWalletBatch {
                wallet: Address::from_str(funder().as_str()).expect("wallet"),
                nonce: U256::from(1_u64),
                deadline: U256::from(2_000_000_000_u64),
                calls: vec![DepositWalletCall {
                    target: Address::from_str(current_adapter().as_str()).expect("adapter"),
                    value: U256::ZERO,
                    data: Bytes::copy_from_slice(&relayer.calldata),
                }],
            }],
            signatures: vec![Bytes::from(mismatched_signature)],
        }
        .abi_encode();
        assert_eq!(
            verify_settlement_confirmation(&redeem, &relayer, mined),
            Err(SettlementConfirmationError::CallEvidenceMismatch)
        );
    }

    #[test]
    fn erc20_rejects_malformed_event() {
        let signature = keccak256("Transfer(address,address,uint256)");
        assert_eq!(
            decode_erc20_transfer(Address::ZERO, &[signature, B256::ZERO], &Bytes::new(), 0,),
            Err(SettlementConfirmationError::InvalidPayoutLog {
                detail: "Transfer requires three topics and 32 data bytes".to_owned(),
            })
        );
        let wrapped = keccak256("Wrapped(address,address,address,uint256)");
        assert_eq!(
            decode_wrapped_payout(Address::ZERO, &[wrapped, B256::ZERO], &Bytes::new(), 0,),
            Err(SettlementConfirmationError::InvalidPayoutLog {
                detail: "Wrapped requires four topics and 32 data bytes".to_owned(),
            })
        );
    }

    fn rpc_response(request: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&request.body).expect("JSON-RPC request");
        let id = body["id"].clone();
        let rpc_method = body["method"].as_str().expect("JSON-RPC method");
        let result = match rpc_method {
            "eth_chainId" => json!("0x89"),
            "eth_getTransactionReceipt" => receipt_rpc_result(),
            "eth_getTransactionByHash" => json!({
                "hash": transaction_hash().as_str(),
                "from": funder().as_str(),
                "to": current_adapter().as_str(),
                "input": "0xdeadbeef"
            }),
            "eth_getBlockByNumber" => {
                let block = body["params"][0].as_str().expect("block tag");
                if block == "finalized" {
                    json!({ "number": "0x65", "hash": block_hash(0x21).as_str() })
                } else {
                    assert_eq!(block, "0x64");
                    json!({ "number": "0x64", "hash": block_hash(0x20).as_str() })
                }
            }
            "eth_getCode" => {
                let block = &body["params"][1];
                assert_eq!(block["blockHash"], block_hash(0x20).as_str());
                assert_eq!(block["requireCanonical"], true);
                json!("0x6000")
            }
            "eth_call" => {
                let block = &body["params"][1];
                assert_eq!(block["blockHash"], block_hash(0x20).as_str());
                assert_eq!(block["requireCanonical"], true);
                json!(format!("0x{:064x}", 0_u64))
            }
            unexpected => panic!("unexpected JSON-RPC method: {unexpected}"),
        };
        ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        }))
    }

    fn receipt_rpc_result() -> Value {
        let transfer = keccak256("Transfer(address,address,uint256)");
        let wrapped = keccak256("Wrapped(address,address,address,uint256)");
        json!({
            "transactionHash": transaction_hash().as_str(),
            "status": "0x1",
            "blockNumber": "0x64",
            "blockHash": block_hash(0x20).as_str(),
            "gasUsed": "0x186a0",
            "effectiveGasPrice": "0x6fc23ac00",
            "logs": [
                {
                    "address": PUSD,
                    "topics": [
                        format!("{transfer:#x}"),
                        address_topic(&zero_address()),
                        address_topic(&funder())
                    ],
                    "data": format!("0x{:064x}", 1_000_000_u64),
                    "logIndex": "0x3"
                },
                {
                    "address": PUSD,
                    "topics": [
                        format!("{wrapped:#x}"),
                        address_topic(&current_adapter()),
                        address_topic(&address(USDCE)),
                        address_topic(&funder())
                    ],
                    "data": format!("0x{:064x}", 1_000_000_u64),
                    "logIndex": "0x4"
                }
            ]
        })
    }

    fn address_topic(address: &EvmAddress) -> String {
        format!("0x{:0>64}", address.as_str().trim_start_matches("0x"))
    }

    fn redeem_case() -> SettlementRedeemInfo {
        let now = timestamp();
        SettlementRedeemInfo {
            settlement_redeem_id: SettlementRedeemId::from_v7(),
            market_id: MarketId::new("0xsettlement-confirmation"),
            yes_token_id: TokenId::new("101"),
            no_token_id: TokenId::new("102"),
            execution_account_id: ExecutionAccountId::from_v7(),
            resolution_content_hash: ContentHash::from_bytes([0x30; 32]),
            resolution_outcome: "Yes".to_owned(),
            resolved_at: now,
            funder_address: funder(),
            wallet_kind: ExecutionWalletKind::Eoa,
            route: SettlementRoute::StandardV2,
            effective_policy: SettlementEffectivePolicy::AutomaticEligible,
            inventory_digest: ContentHash::from_bytes([0x31; 32]),
            contributor_lots_digest: ContentHash::from_bytes([0x32; 32]),
            state: SettlementCaseState::Submitted,
            readiness_status: SettlementReadinessStatus::Ready,
            readiness_evidence_json: SettlementReadinessEvidence::default(),
            target_adapter: Some(current_adapter()),
            target_code_hash: Some(code_hash(
                "0x93b965351d01c1a128821ac79fc98a18105daefb46bda0d1e5b52306d713aa4f",
            )),
            deployment_digest: Some(ContentHash::from_bytes([0x11; 32])),
            deployment_evidence_version: Some(evidence_version()),
            verified_block_number: Some(90),
            verified_block_hash: Some(block_hash(0x12)),
            current_authorization_id: None,
            authorization_state: SettlementAuthorizationState::Consumed,
            authorization_digest: Some(ContentHash::from_bytes([0x13; 32])),
            authorization_expires_at: Some(now),
            authorized_by: None,
            authorized_at: None,
            authorization_revoked_at: None,
            authorization_consumed_at: Some(now),
            reconciliation_state: SettlementReconciliationState::AwaitingReceipt,
            payout_vector_json: SettlementPayoutVector {
                denominator: uint("1"),
                yes: uint("1"),
                no: uint("0"),
            },
            balance_before_json: Some(SettlementBalanceEvidence {
                yes: token_balance("1", "1000000", dec!(1)),
                no: token_balance("2", "0", dec!(0)),
            }),
            balance_after_json: None,
            expected_payout_usd: Some(Usd::new(dec!(1))),
            actual_payout_usd: None,
            gas_fee_pol: None,
            failure_code: None,
            attempt_count: 1,
            retry_count: 0,
            next_attempt_at: None,
            claim_owner: None,
            lease_expires_at: None,
            last_error: None,
            prepared_at: Some(now),
            submitted_at: Some(now),
            confirmed_at: None,
            failed_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn submission(
        kind: SettlementSubmissionKind,
        target: EvmAddress,
        redeem: &SettlementRedeemInfo,
    ) -> SettlementChainSubmissionInfo {
        let now = timestamp();
        let calldata = vec![0xde, 0xad, 0xbe, 0xef];
        let envelope = vec![0x02, 0x01];
        SettlementChainSubmissionInfo {
            settlement_chain_submission_id: SettlementChainSubmissionId::from_v7(),
            settlement_redeem_id: Some(redeem.settlement_redeem_id),
            settlement_governed_action_id: None,
            canary_action_id: None,
            purpose: SettlementSubmissionPurpose::Redeem,
            kind,
            state: SettlementSubmissionState::AwaitingFinality,
            route: SettlementRoute::StandardV2,
            target_adapter: target.clone(),
            target_code_hash: code_hash(
                "0x93b965351d01c1a128821ac79fc98a18105daefb46bda0d1e5b52306d713aa4f",
            ),
            conditional_tokens: address(CONDITIONAL_TOKENS),
            collateral_token: address(PUSD),
            usdce: address(USDCE),
            call_target: target,
            deployment_digest: ContentHash::from_bytes([0x11; 32]),
            deployment_evidence_version: evidence_version(),
            verified_block_number: 90,
            verified_block_hash: block_hash(0x12),
            prepared_block_number: Some(91),
            prepared_block_hash: Some(block_hash(0x14)),
            calldata_hash: calldata_hash(&calldata).expect("calldata hash"),
            calldata,
            signed_envelope: Some(envelope.clone()),
            signed_envelope_hash: Some(CanonicalDigest::content_hash_bytes(&envelope)),
            prepared_nonce: Some(uint("1")),
            gas_limit: Some(uint("100000")),
            relayer_transaction_id: None,
            transaction_hash: Some(transaction_hash()),
            failure_code: None,
            failure_history_json: SettlementFailureHistory::default(),
            receipt_evidence_json: None,
            attempt_ordinal: 1,
            last_error: None,
            dispatched_at: Some(now),
            chain_hash_observed_at: Some(now),
            confirmed_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn observation(submission: &SettlementChainSubmissionInfo) -> SettlementChainObservation {
        let block = block_hash(0x20);
        SettlementChainObservation {
            chain_id: 137,
            transaction: SettlementTransactionObservation {
                transaction_hash: transaction_hash(),
                outer_sender: funder(),
                outer_target: submission.call_target.clone(),
                input: submission.calldata.clone(),
            },
            receipt_transaction_hash: transaction_hash(),
            receipt_success: true,
            receipt_block_number: 100,
            receipt_block_hash: block.clone(),
            canonical_receipt_block_hash: Some(block),
            finalized_block_number: 101,
            finalized_block_hash: block_hash(0x21),
            target_code_hash: submission.target_code_hash.clone(),
            transfers: vec![ObservedErc20Transfer {
                token: address("0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb"),
                from: zero_address(),
                to: funder(),
                raw_amount: U256::from(1_000_000_u64),
                log_index: 3,
            }],
            wrapped_payouts: vec![ObservedWrappedPayout {
                collateral_token: address(PUSD),
                caller: submission.target_adapter.clone(),
                asset: address(USDCE),
                to: funder(),
                raw_amount: U256::from(1_000_000_u64),
                log_index: 4,
            }],
            balances_after: SettlementBalanceEvidence {
                yes: token_balance("1", "0", dec!(0)),
                no: token_balance("2", "0", dec!(0)),
            },
            gas_used: 100_000,
            effective_gas_price_wei: 30_000_000_000,
            observed_at: timestamp(),
        }
    }

    fn governed_approval_action(redeem: &SettlementRedeemInfo) -> SettlementGovernedActionInfo {
        let now = timestamp();
        SettlementGovernedActionInfo {
            settlement_governed_action_id: SettlementGovernedActionId::from_v7(),
            execution_account_id: redeem.execution_account_id,
            settlement_redeem_id: None,
            kind: SettlementGovernedActionKind::OutcomeTokenApproval,
            state: SettlementGovernedActionState::Authorized,
            route: Some(redeem.route),
            target_adapter: redeem.target_adapter.clone(),
            deployment_digest: redeem.deployment_digest,
            deployment_evidence_version: redeem.deployment_evidence_version.clone(),
            verified_block_number: redeem.verified_block_number,
            verified_block_hash: redeem.verified_block_hash.clone(),
            desired_approval: Some(true),
            authorization_digest: None,
            payout_ceiling_usd: None,
            scope_digest: ContentHash::from_bytes([0x42; 32]),
            idempotency_key: SettlementActionIdempotencyKey::parse("approval-confirmation-test")
                .expect("approval idempotency key"),
            authorization_reason: "approve exact current adapter".to_owned(),
            authorized_by: UserId::from_v7(),
            revoked_by: None,
            revocation_reason: None,
            expires_at: now,
            authorized_at: now,
            consumed_at: None,
            revoked_at: None,
            failure_code: None,
            retry_count: 0,
            claim_owner: None,
            lease_expires_at: None,
            next_attempt_at: Some(now),
            last_error: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn operator_approval_submission(
        redeem: &SettlementRedeemInfo,
        action: &SettlementGovernedActionInfo,
    ) -> SettlementChainSubmissionInfo {
        let mut submission = submission(
            SettlementSubmissionKind::DirectEoa,
            current_adapter(),
            redeem,
        );
        submission.settlement_redeem_id = None;
        submission.settlement_governed_action_id = Some(action.settlement_governed_action_id);
        submission.purpose = SettlementSubmissionPurpose::OutcomeTokenApproval;
        submission.call_target = address(CONDITIONAL_TOKENS);
        submission.calldata = vec![0xa2, 0x2c, 0xb4, 0x65];
        submission.calldata_hash =
            calldata_hash(&submission.calldata).expect("approval calldata hash");
        submission
    }

    fn operator_approval_observation(
        submission: &SettlementChainSubmissionInfo,
    ) -> SettlementOperatorApprovalObservation {
        let block = block_hash(0x20);
        SettlementOperatorApprovalObservation {
            chain_id: 137,
            transaction: SettlementTransactionObservation {
                transaction_hash: transaction_hash(),
                outer_sender: funder(),
                outer_target: submission.call_target.clone(),
                input: submission.calldata.clone(),
            },
            receipt_transaction_hash: transaction_hash(),
            receipt_success: true,
            receipt_block_number: 100,
            receipt_block_hash: block.clone(),
            canonical_receipt_block_hash: Some(block),
            finalized_block_number: 101,
            finalized_block_hash: block_hash(0x21),
            target_code_hash: submission.target_code_hash.clone(),
            operator_approved: true,
            observed_at: timestamp(),
        }
    }

    fn token_balance(token: &str, raw: &str, shares: Decimal) -> SettlementTokenBalance {
        SettlementTokenBalance {
            token_id: TokenId::new(token),
            raw_balance: uint(raw),
            shares: Shares::new(shares),
        }
    }

    fn externally_observed(
        mut submission: SettlementChainSubmissionInfo,
    ) -> SettlementChainSubmissionInfo {
        submission.kind = SettlementSubmissionKind::ExternallyObserved;
        submission.signed_envelope = None;
        submission.signed_envelope_hash = None;
        submission.prepared_nonce = None;
        submission.gas_limit = None;
        submission.relayer_transaction_id = None;
        submission
    }

    fn current_adapter() -> EvmAddress {
        address("0xada100db00ca00073811820692005400218fce1f")
    }

    fn funder() -> EvmAddress {
        address("0x1111111111111111111111111111111111111111")
    }

    fn zero_address() -> EvmAddress {
        address("0x0000000000000000000000000000000000000000")
    }

    fn address(value: &str) -> EvmAddress {
        EvmAddress::parse(value).expect("address")
    }

    fn code_hash(value: &str) -> EvmCodeHash {
        EvmCodeHash::parse(value).expect("code hash")
    }

    fn block_hash(byte: u8) -> EvmBlockHash {
        let octet = format!("{byte:02x}");
        EvmBlockHash::parse(format!("0x{}", octet.repeat(32))).expect("block hash")
    }

    fn transaction_hash() -> EvmTransactionHash {
        EvmTransactionHash::parse(format!("0x{}", "55".repeat(32))).expect("transaction hash")
    }

    fn uint(value: &str) -> EvmUint256 {
        EvmUint256::parse(value).expect("uint256")
    }

    fn evidence_version() -> SettlementEvidenceVersion {
        SettlementEvidenceVersion::parse("polymarket-v2-2026-07-22.1").expect("evidence version")
    }

    fn timestamp() -> DateTime<Utc> {
        Utc.timestamp_opt(1_000, 0).single().expect("timestamp")
    }
}
