//! Capability-gated Polymarket V2 settlement calldata.

use std::{str::FromStr, time::Duration};

use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    primitives::{Address, B256, Bytes, U256, keccak256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::{
        client::RpcClient,
        types::{TransactionInput, TransactionRequest},
    },
    sol,
    sol_types::SolCall,
    transports::http::Http,
};
use quant_pivot_models::{
    config::OnchainConfig,
    enums::settlement::{SettlementRoute, SettlementSubmissionPurpose},
    types::{
        ContentHash, EvmAddress, EvmBlockHash, EvmCalldataHash, EvmCodeHash, EvmUint256, MarketId,
        SettlementEvidenceVersion, Shares, TokenId,
        settlement_payload::{
            SettlementBalanceEvidence, SettlementPayoutVector, SettlementTokenBalance,
        },
    },
};
use reqwest::{Client, Url};
use rust_decimal::Decimal;
use serde::Deserialize;

use self::SettlementAdapterWrite::redeemPositionsCall;
use super::contracts::VerifiedSettlementDeployment;

const POLYGON_CHAIN_ID: u64 = 137;
const OUTCOME_TOKEN_DECIMALS: u32 = 6;

sol! {
    interface SettlementAdapterWrite {
        function redeemPositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] indexSets
        ) external;
    }
}

sol! {
    interface ConditionalTokensWrite {
        function setApprovalForAll(address operator, bool approved) external;
    }
}

sol! {
    #[sol(rpc)]
    interface ConditionalTokensSettlementView {
        function balanceOf(address account, uint256 id) external view returns (uint256);
        function isApprovedForAll(address account, address operator) external view returns (bool);
        function payoutDenominator(bytes32 conditionId) external view returns (uint256);
        function payoutNumerators(bytes32 conditionId, uint256 outcomeSlotIndex)
            external view returns (uint256);
    }
}

sol! {
    #[sol(rpc)]
    interface SettlementAdapterStateView {
        function paused(address asset) external view returns (bool);
    }
}

sol! {
    #[sol(rpc)]
    interface Erc20BalanceView {
        function balanceOf(address account) external view returns (uint256);
    }
}

/// Immutable call intent produced only from a verified current deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSettlementCall {
    purpose: SettlementSubmissionPurpose,
    route: SettlementRoute,
    funder: EvmAddress,
    target_adapter: EvmAddress,
    target_code_hash: EvmCodeHash,
    conditional_tokens: EvmAddress,
    collateral_token: EvmAddress,
    usdce: EvmAddress,
    call_target: EvmAddress,
    calldata: Vec<u8>,
    calldata_hash: EvmCalldataHash,
    deployment_digest: ContentHash,
    deployment_evidence_version: SettlementEvidenceVersion,
    verified_block_number: u64,
    verified_block_hash: EvmBlockHash,
}

impl PreparedSettlementCall {
    #[must_use]
    pub const fn purpose(&self) -> SettlementSubmissionPurpose {
        self.purpose
    }

    #[must_use]
    pub const fn route(&self) -> SettlementRoute {
        self.route
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
    pub const fn target_code_hash(&self) -> &EvmCodeHash {
        &self.target_code_hash
    }

    #[must_use]
    pub const fn conditional_tokens(&self) -> &EvmAddress {
        &self.conditional_tokens
    }

    #[must_use]
    pub const fn collateral_token(&self) -> &EvmAddress {
        &self.collateral_token
    }

    #[must_use]
    pub const fn usdce(&self) -> &EvmAddress {
        &self.usdce
    }

    #[must_use]
    pub const fn call_target(&self) -> &EvmAddress {
        &self.call_target
    }

    #[must_use]
    pub fn calldata(&self) -> &[u8] {
        &self.calldata
    }

    #[must_use]
    pub const fn calldata_hash(&self) -> &EvmCalldataHash {
        &self.calldata_hash
    }

    #[must_use]
    pub const fn deployment_digest(&self) -> ContentHash {
        self.deployment_digest
    }

    #[must_use]
    pub const fn deployment_evidence_version(&self) -> &SettlementEvidenceVersion {
        &self.deployment_evidence_version
    }

    #[must_use]
    pub const fn verified_block_number(&self) -> u64 {
        self.verified_block_number
    }

    #[must_use]
    pub const fn verified_block_hash(&self) -> &EvmBlockHash {
        &self.verified_block_hash
    }
}

/// Fixed YES/NO token identities for one binary condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementBinaryTokenPair {
    pub yes: TokenId,
    pub no: TokenId,
}

/// Read-only evidence and successful call simulation captured at the verified
/// canonical block. A successful simulation does not authorize submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementRedeemPreflight {
    pub block_number: u64,
    pub block_hash: EvmBlockHash,
    pub payout_vector: SettlementPayoutVector,
    pub balances: SettlementBalanceEvidence,
}

/// Narrow money-movement capability minted only after live inventory,
/// operator approval, adapter state, residual balance, simulation, and
/// canonical-block checks all pass.
///
/// It cannot be serialized or constructed outside this module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedRedeemRoute {
    deployment: VerifiedSettlementDeployment,
    prepared_call: PreparedSettlementCall,
    preflight: SettlementRedeemPreflight,
}

/// Canonical calldata identity used only to reconcile an already mined
/// redemption. This is deliberately distinct from [`PreparedSettlementCall`]
/// and cannot enter the submission executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalRedeemCallExpectation {
    route: SettlementRoute,
    target_adapter: EvmAddress,
    target_code_hash: EvmCodeHash,
    conditional_tokens: EvmAddress,
    collateral_token: EvmAddress,
    usdce: EvmAddress,
    calldata: Vec<u8>,
    calldata_hash: EvmCalldataHash,
    deployment_digest: ContentHash,
    deployment_evidence_version: SettlementEvidenceVersion,
    verified_block_number: u64,
    verified_block_hash: EvmBlockHash,
}

impl ExternalRedeemCallExpectation {
    #[must_use]
    pub const fn route(&self) -> SettlementRoute {
        self.route
    }

    #[must_use]
    pub const fn target_adapter(&self) -> &EvmAddress {
        &self.target_adapter
    }

    #[must_use]
    pub const fn target_code_hash(&self) -> &EvmCodeHash {
        &self.target_code_hash
    }

    #[must_use]
    pub const fn conditional_tokens(&self) -> &EvmAddress {
        &self.conditional_tokens
    }

    #[must_use]
    pub const fn collateral_token(&self) -> &EvmAddress {
        &self.collateral_token
    }

    #[must_use]
    pub const fn usdce(&self) -> &EvmAddress {
        &self.usdce
    }

    #[must_use]
    pub fn calldata(&self) -> &[u8] {
        &self.calldata
    }

    #[must_use]
    pub const fn calldata_hash(&self) -> &EvmCalldataHash {
        &self.calldata_hash
    }

    #[must_use]
    pub const fn deployment_digest(&self) -> ContentHash {
        self.deployment_digest
    }

    #[must_use]
    pub const fn deployment_evidence_version(&self) -> &SettlementEvidenceVersion {
        &self.deployment_evidence_version
    }

    #[must_use]
    pub const fn verified_block_number(&self) -> u64 {
        self.verified_block_number
    }

    #[must_use]
    pub const fn verified_block_hash(&self) -> &EvmBlockHash {
        &self.verified_block_hash
    }
}

impl VerifiedRedeemRoute {
    #[must_use]
    pub const fn deployment(&self) -> &VerifiedSettlementDeployment {
        &self.deployment
    }

    #[must_use]
    pub const fn preflight(&self) -> &SettlementRedeemPreflight {
        &self.preflight
    }
}

/// Typed adapter call construction failure.
#[derive(Debug, thiserror::Error)]
pub enum SettlementAdapterError {
    #[error("invalid built-in settlement address for {contract}: {detail}")]
    InvalidOfficialAddress {
        contract: &'static str,
        detail: String,
    },
    #[error("market ID is not a canonical bytes32 condition ID: {market_id}")]
    InvalidConditionId { market_id: String },
    #[error("generated calldata hash is not canonical: {detail}")]
    InvalidCalldataHash { detail: String },
    #[error("invalid {side} token ID: {token_id}")]
    InvalidTokenId {
        side: &'static str,
        token_id: String,
    },
    #[error("condition is not resolved because its payout denominator is zero")]
    ConditionNotResolved,
    #[error("binary payout vector is invalid: denominator={denominator}, yes={yes}, no={no}")]
    InvalidPayoutVector {
        denominator: String,
        yes: String,
        no: String,
    },
    #[error("settlement outcome balances are both zero")]
    EmptyOutcomeBalances,
    #[error("verified adapter is no longer approved as CTF operator at the pinned block")]
    MissingOperatorApproval,
    #[error("verified adapter is paused for USDC.e at the pinned block")]
    AdapterPaused,
    #[error("verified adapter retains {raw_balance} raw USDC.e at the pinned block")]
    AdapterResidualUsdce { raw_balance: String },
    #[error("settlement numeric evidence cannot be represented exactly: {detail}")]
    NumericEvidence { detail: String },
    #[error("settlement Polygon RPC connection failed: {detail}")]
    RpcConnection { detail: String },
    #[error("settlement Polygon RPC call {operation} failed: {detail}")]
    RpcCall {
        operation: &'static str,
        detail: String,
    },
    #[error("settlement RPC is connected to chain {actual}, expected Polygon chain 137")]
    WrongChain { actual: u64 },
    #[error("verified block {block_number} is no longer canonical")]
    CanonicalBlockChanged { block_number: u64 },
    #[error("prepared settlement call reverted during read-only simulation: {detail}")]
    SimulationReverted { detail: String },
}

/// Stateless V2 adapter gateway. Raw target addresses are never accepted by
/// its construction methods.
#[derive(Debug, Default, Clone, Copy)]
pub struct SettlementAdapterGateway;

impl SettlementAdapterGateway {
    /// Materialize the exact call frozen into a verified redeem capability.
    /// A deployment capability alone can never construct a redemption.
    #[must_use]
    pub fn prepare_redeem(&self, capability: &VerifiedRedeemRoute) -> PreparedSettlementCall {
        capability.prepared_call.clone()
    }

    /// Reconstruct the exact calldata identity for a redemption that has
    /// already been observed on-chain. The returned evidence type cannot be
    /// submitted and therefore does not weaken the redeem capability gate.
    pub fn expected_external_redeem_call(
        &self,
        capability: &VerifiedSettlementDeployment,
        market_id: &MarketId,
    ) -> Result<ExternalRedeemCallExpectation, SettlementAdapterError> {
        let call = Self::build_redeem_call(capability, market_id)?;
        Ok(ExternalRedeemCallExpectation {
            route: call.route,
            target_adapter: call.target_adapter,
            target_code_hash: call.target_code_hash,
            conditional_tokens: call.conditional_tokens,
            collateral_token: call.collateral_token,
            usdce: call.usdce,
            calldata: call.calldata,
            calldata_hash: call.calldata_hash,
            deployment_digest: call.deployment_digest,
            deployment_evidence_version: call.deployment_evidence_version,
            verified_block_number: call.verified_block_number,
            verified_block_hash: call.verified_block_hash,
        })
    }

    fn build_redeem_call(
        capability: &VerifiedSettlementDeployment,
        market_id: &MarketId,
    ) -> Result<PreparedSettlementCall, SettlementAdapterError> {
        let condition_id = condition_id(market_id)?;
        let collateral = capability_address("verified pUSD", capability.collateral_token())?;
        let calldata = redeemPositionsCall {
            collateralToken: collateral,
            parentCollectionId: B256::ZERO,
            conditionId: condition_id,
            indexSets: vec![U256::from(1), U256::from(2)],
        }
        .abi_encode();
        prepared_call(
            capability,
            SettlementSubmissionPurpose::Redeem,
            capability.target().clone(),
            calldata,
        )
    }

    /// Build the exact ERC-1155 operator-approval call for the verified route.
    pub fn prepare_operator_approval(
        &self,
        capability: &VerifiedSettlementDeployment,
    ) -> Result<PreparedSettlementCall, SettlementAdapterError> {
        Self::prepare_operator_approval_state(capability, true)
    }

    /// Build the exact ERC-1155 operator-revocation call for the verified route.
    pub fn prepare_operator_revocation(
        &self,
        capability: &VerifiedSettlementDeployment,
    ) -> Result<PreparedSettlementCall, SettlementAdapterError> {
        Self::prepare_operator_approval_state(capability, false)
    }

    fn prepare_operator_approval_state(
        capability: &VerifiedSettlementDeployment,
        approved: bool,
    ) -> Result<PreparedSettlementCall, SettlementAdapterError> {
        let operator = Address::from_str(capability.target().as_str()).map_err(|error| {
            SettlementAdapterError::InvalidOfficialAddress {
                contract: "verified adapter",
                detail: error.to_string(),
            }
        })?;
        let calldata =
            ConditionalTokensWrite::setApprovalForAllCall { operator, approved }.abi_encode();
        prepared_call(
            capability,
            if approved {
                SettlementSubmissionPurpose::OutcomeTokenApproval
            } else {
                SettlementSubmissionPurpose::OutcomeTokenRevocation
            },
            capability.conditional_tokens().clone(),
            calldata,
        )
    }
}

/// Read-only Polygon client for frozen balance, payout-vector, approval, pause,
/// and call-simulation evidence. It has no signer and cannot broadcast.
pub struct AlloySettlementAdapterReader {
    provider: DynProvider,
}

#[derive(Debug, Deserialize)]
struct CanonicalBlockHashResponse {
    hash: B256,
}

impl AlloySettlementAdapterReader {
    /// Build a bounded read-only RPC client. No network request is issued here.
    pub fn connect(config: &OnchainConfig) -> Result<Self, SettlementAdapterError> {
        let rpc_url = Url::parse(config.rpc_url()).map_err(|error| {
            SettlementAdapterError::RpcConnection {
                detail: format!("configured Polygon RPC endpoint is invalid: {error}"),
            }
        })?;
        let http_client = Client::builder()
            .timeout(Duration::from_millis(config.rpc_timeout_ms))
            .build()
            .map_err(|error| SettlementAdapterError::RpcConnection {
                detail: format!("failed to build settlement RPC client: {error}"),
            })?;
        let transport = Http::with_client(http_client, rpc_url);
        let rpc_client = RpcClient::new(transport, false);
        Ok(Self {
            provider: ProviderBuilder::new().connect_client(rpc_client).erased(),
        })
    }

    /// Read and simulate one full-balance redemption at the exact verified
    /// canonical block. This method never signs or submits a transaction.
    pub async fn verify_redeem_route(
        &self,
        capability: &VerifiedSettlementDeployment,
        market_id: &MarketId,
        tokens: &SettlementBinaryTokenPair,
    ) -> Result<VerifiedRedeemRoute, SettlementAdapterError> {
        let call = SettlementAdapterGateway::build_redeem_call(capability, market_id)?;
        let chain_id = self.provider.get_chain_id().await.map_err(|error| {
            SettlementAdapterError::RpcCall {
                operation: "eth_chainId",
                detail: error.to_string(),
            }
        })?;
        if chain_id != POLYGON_CHAIN_ID {
            return Err(SettlementAdapterError::WrongChain { actual: chain_id });
        }

        let block_hash =
            B256::from_str(capability.verified_block_hash().as_str()).map_err(|error| {
                SettlementAdapterError::RpcCall {
                    operation: "parse verified block hash",
                    detail: error.to_string(),
                }
            })?;
        let block = BlockId::hash_canonical(block_hash);
        let funder = parse_address("verified funder", capability.funder().as_str())?;
        let adapter = parse_address("verified adapter", capability.target().as_str())?;
        let conditional_tokens = capability_address(
            "verified Conditional Tokens",
            capability.conditional_tokens(),
        )?;
        let condition = condition_id(market_id)?;
        let yes_token = outcome_token_id("YES", &tokens.yes)?;
        let no_token = outcome_token_id("NO", &tokens.no)?;
        let ctf = ConditionalTokensSettlementView::new(conditional_tokens, &self.provider);

        let denominator = ctf
            .payoutDenominator(condition)
            .block(block)
            .call()
            .await
            .map_err(|error| SettlementAdapterError::RpcCall {
                operation: "ctf.payoutDenominator",
                detail: error.to_string(),
            })?;
        let yes_payout = ctf
            .payoutNumerators(condition, U256::ZERO)
            .block(block)
            .call()
            .await
            .map_err(|error| SettlementAdapterError::RpcCall {
                operation: "ctf.payoutNumerators(YES)",
                detail: error.to_string(),
            })?;
        let no_payout = ctf
            .payoutNumerators(condition, U256::from(1))
            .block(block)
            .call()
            .await
            .map_err(|error| SettlementAdapterError::RpcCall {
                operation: "ctf.payoutNumerators(NO)",
                detail: error.to_string(),
            })?;
        validate_payout_vector(denominator, yes_payout, no_payout)?;

        let yes_balance = ctf
            .balanceOf(funder, yes_token)
            .block(block)
            .call()
            .await
            .map_err(|error| SettlementAdapterError::RpcCall {
                operation: "ctf.balanceOf(YES)",
                detail: error.to_string(),
            })?;
        let no_balance = ctf
            .balanceOf(funder, no_token)
            .block(block)
            .call()
            .await
            .map_err(|error| SettlementAdapterError::RpcCall {
                operation: "ctf.balanceOf(NO)",
                detail: error.to_string(),
            })?;
        if yes_balance.is_zero() && no_balance.is_zero() {
            return Err(SettlementAdapterError::EmptyOutcomeBalances);
        }
        if !ctf
            .isApprovedForAll(funder, adapter)
            .block(block)
            .call()
            .await
            .map_err(|error| SettlementAdapterError::RpcCall {
                operation: "ctf.isApprovedForAll",
                detail: error.to_string(),
            })?
        {
            return Err(SettlementAdapterError::MissingOperatorApproval);
        }
        let usdce = capability_address("verified USDC.e", capability.usdce())?;
        self.verify_adapter_state_and_simulate(adapter, usdce, funder, block, &call)
            .await?;
        self.recheck_canonical_block(capability).await?;

        let preflight = SettlementRedeemPreflight {
            block_number: capability.verified_block(),
            block_hash: capability.verified_block_hash().clone(),
            payout_vector: SettlementPayoutVector {
                denominator: typed_uint256(denominator)?,
                yes: typed_uint256(yes_payout)?,
                no: typed_uint256(no_payout)?,
            },
            balances: SettlementBalanceEvidence {
                yes: token_balance(tokens.yes.clone(), yes_balance)?,
                no: token_balance(tokens.no.clone(), no_balance)?,
            },
        };
        Ok(VerifiedRedeemRoute {
            deployment: capability.clone(),
            prepared_call: call,
            preflight,
        })
    }

    async fn verify_adapter_state_and_simulate(
        &self,
        adapter: Address,
        usdce: Address,
        funder: Address,
        block: BlockId,
        call: &PreparedSettlementCall,
    ) -> Result<(), SettlementAdapterError> {
        if SettlementAdapterStateView::new(adapter, &self.provider)
            .paused(usdce)
            .block(block)
            .call()
            .await
            .map_err(|error| SettlementAdapterError::RpcCall {
                operation: "adapter.paused",
                detail: error.to_string(),
            })?
        {
            return Err(SettlementAdapterError::AdapterPaused);
        }
        let residual_usdce = Erc20BalanceView::new(usdce, &self.provider)
            .balanceOf(adapter)
            .block(block)
            .call()
            .await
            .map_err(|error| SettlementAdapterError::RpcCall {
                operation: "usdce.balanceOf(adapter)",
                detail: error.to_string(),
            })?;
        if !residual_usdce.is_zero() {
            return Err(SettlementAdapterError::AdapterResidualUsdce {
                raw_balance: residual_usdce.to_string(),
            });
        }

        let transaction = TransactionRequest::default()
            .from(funder)
            .to(parse_address(
                "prepared call target",
                call.call_target().as_str(),
            )?)
            .input(TransactionInput::new(Bytes::copy_from_slice(
                call.calldata(),
            )));
        self.provider
            .call(transaction)
            .block(block)
            .await
            .map_err(|error| SettlementAdapterError::SimulationReverted {
                detail: error.to_string(),
            })?;
        Ok(())
    }

    async fn recheck_canonical_block(
        &self,
        capability: &VerifiedSettlementDeployment,
    ) -> Result<(), SettlementAdapterError> {
        let current_block: Option<CanonicalBlockHashResponse> = self
            .provider
            .raw_request(
                "eth_getBlockByNumber".into(),
                (BlockNumberOrTag::Number(capability.verified_block()), false),
            )
            .await
            .map_err(|error| SettlementAdapterError::RpcCall {
                operation: "eth_getBlockByNumber(canonical recheck)",
                detail: error.to_string(),
            })?;
        let current_hash = current_block.map(|block| format!("{:#x}", block.hash));
        if current_hash.as_deref() != Some(capability.verified_block_hash().as_str()) {
            return Err(SettlementAdapterError::CanonicalBlockChanged {
                block_number: capability.verified_block(),
            });
        }
        Ok(())
    }
}

fn prepared_call(
    capability: &VerifiedSettlementDeployment,
    purpose: SettlementSubmissionPurpose,
    call_target: EvmAddress,
    calldata: Vec<u8>,
) -> Result<PreparedSettlementCall, SettlementAdapterError> {
    let calldata_hash =
        EvmCalldataHash::parse(format!("{:#x}", keccak256(&calldata))).map_err(|error| {
            SettlementAdapterError::InvalidCalldataHash {
                detail: error.to_string(),
            }
        })?;
    Ok(PreparedSettlementCall {
        purpose,
        route: capability.route(),
        funder: capability.funder().clone(),
        target_adapter: capability.target().clone(),
        target_code_hash: capability.target_code_hash().clone(),
        conditional_tokens: capability.conditional_tokens().clone(),
        collateral_token: capability.collateral_token().clone(),
        usdce: capability.usdce().clone(),
        call_target,
        calldata,
        calldata_hash,
        deployment_digest: capability.deployment_digest(),
        deployment_evidence_version: capability.evidence_version().clone(),
        verified_block_number: capability.verified_block(),
        verified_block_hash: capability.verified_block_hash().clone(),
    })
}

fn condition_id(market_id: &MarketId) -> Result<B256, SettlementAdapterError> {
    B256::from_str(market_id.as_str()).map_err(|_| SettlementAdapterError::InvalidConditionId {
        market_id: market_id.as_str().to_owned(),
    })
}

fn outcome_token_id(
    side: &'static str,
    token_id: &TokenId,
) -> Result<U256, SettlementAdapterError> {
    let value =
        U256::from_str(token_id.as_str()).map_err(|_| SettlementAdapterError::InvalidTokenId {
            side,
            token_id: token_id.as_str().to_owned(),
        })?;
    if value.to_string() != token_id.as_str() {
        return Err(SettlementAdapterError::InvalidTokenId {
            side,
            token_id: token_id.as_str().to_owned(),
        });
    }
    Ok(value)
}

fn validate_payout_vector(
    denominator: U256,
    yes: U256,
    no: U256,
) -> Result<(), SettlementAdapterError> {
    if denominator.is_zero() {
        return Err(SettlementAdapterError::ConditionNotResolved);
    }
    if yes > denominator || no > denominator || yes.checked_add(no) != Some(denominator) {
        return Err(SettlementAdapterError::InvalidPayoutVector {
            denominator: denominator.to_string(),
            yes: yes.to_string(),
            no: no.to_string(),
        });
    }
    Ok(())
}

fn typed_uint256(value: U256) -> Result<EvmUint256, SettlementAdapterError> {
    EvmUint256::parse(value.to_string()).map_err(|error| SettlementAdapterError::NumericEvidence {
        detail: error.to_string(),
    })
}

fn token_balance(
    token_id: TokenId,
    raw_balance: U256,
) -> Result<SettlementTokenBalance, SettlementAdapterError> {
    let raw = Decimal::from_str_exact(&raw_balance.to_string()).map_err(|error| {
        SettlementAdapterError::NumericEvidence {
            detail: error.to_string(),
        }
    })?;
    let divisor = Decimal::from(10_u64.pow(OUTCOME_TOKEN_DECIMALS));
    Ok(SettlementTokenBalance {
        token_id,
        raw_balance: typed_uint256(raw_balance)?,
        shares: Shares::new(raw / divisor),
    })
}

fn parse_address(contract: &'static str, value: &str) -> Result<Address, SettlementAdapterError> {
    Address::from_str(value).map_err(|error| SettlementAdapterError::InvalidOfficialAddress {
        contract,
        detail: error.to_string(),
    })
}

fn capability_address(
    contract: &'static str,
    value: &EvmAddress,
) -> Result<Address, SettlementAdapterError> {
    Address::from_str(value.as_str()).map_err(|error| {
        SettlementAdapterError::InvalidOfficialAddress {
            contract,
            detail: error.to_string(),
        }
    })
}

#[cfg(test)]
pub(crate) fn verified_redeem_fixture(
    deployment: VerifiedSettlementDeployment,
    market_id: &MarketId,
) -> VerifiedRedeemRoute {
    let prepared_call = SettlementAdapterGateway::build_redeem_call(&deployment, market_id)
        .expect("test deployment and condition build a redeem call");
    VerifiedRedeemRoute {
        preflight: SettlementRedeemPreflight {
            block_number: deployment.verified_block(),
            block_hash: deployment.verified_block_hash().clone(),
            payout_vector: SettlementPayoutVector {
                denominator: EvmUint256::parse("1").expect("fixture denominator"),
                yes: EvmUint256::parse("1").expect("fixture YES payout"),
                no: EvmUint256::parse("0").expect("fixture NO payout"),
            },
            balances: SettlementBalanceEvidence {
                yes: token_balance(TokenId::new("1"), U256::from(1_000_000_u64))
                    .expect("fixture YES balance"),
                no: token_balance(TokenId::new("2"), U256::ZERO).expect("fixture NO balance"),
            },
        },
        deployment,
        prepared_call,
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use alloy::{
        eips::BlockNumberOrTag,
        primitives::{Address, Bytes, U256},
        providers::Provider,
        rpc::types::{TransactionInput, TransactionRequest},
        sol,
        sol_types::SolCall,
    };
    use quant_pivot_models::{
        config::{OnchainConfig, PolygonRpcEndpoint},
        enums::settlement::{SettlementRoute, SettlementSubmissionPurpose},
        types::{EvmAddress, EvmBlockHash, MarketId, Shares, TokenId},
    };
    use rust_decimal_macros::dec;
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
    use testcontainers::{
        ContainerAsync, GenericImage, ImageExt,
        core::{AccessMode, Mount, WaitFor},
        runners::AsyncRunner,
    };
    use wiremock::{Mock, MockServer, Request, ResponseTemplate, matchers::method};

    use super::{
        AlloySettlementAdapterReader,
        ConditionalTokensSettlementView::{
            self, balanceOfCall, isApprovedForAllCall, payoutDenominatorCall, payoutNumeratorsCall,
        },
        Erc20BalanceView::balanceOfCall as Erc20BalanceOfCall,
        SettlementAdapterError, SettlementAdapterGateway,
        SettlementAdapterStateView::pausedCall,
        SettlementBinaryTokenPair, redeemPositionsCall,
    };
    use crate::settlement::contracts::{
        SettlementDeploymentCatalog, VerifiedSettlementDeployment, verified_deployment_fixture,
        verified_deployment_fixture_at,
    };

    sol! {
        #[sol(rpc)]
        interface LocalSettlementHarnessView {
            function CTF_TEMPLATE() external view returns (address);
            function PUSD_TEMPLATE() external view returns (address);
            function USDCE_TEMPLATE() external view returns (address);
            function standard() external view returns (address);
            function negRisk() external view returns (address);
            function initialize(address funder, address ctf, address pusd, address usdce) external;
            function seed(address funder, address adapter, address ctf) external;
        }
    }

    sol! {
        #[sol(rpc)]
        interface LocalPusdView {
            function balanceOf(address account) external view returns (uint256);
        }
    }

    sol! {
        interface LocalAdapterWrite {
            function setPaused(bool value) external;
        }
    }

    const CONDITION: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
    const ANVIL_FUNDER: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";
    const HARNESS_ADDRESS: &str = "0x5fbdb2315678afecb367f032d93f642f64180aa3";
    const FOUNDRY_IMAGE_TAG: &str = concat!(
        "stable@sha256:",
        "043752653d5be351c71709091b3db97c4421c907eb40ea294195e7f532aadf46"
    );

    #[test]
    fn local_chain_fixture_lock_matches_every_consumed_input() {
        let lock = include_str!("../../tests/fixtures/settlement-local-chain/artifact.lock");
        assert_fixture_hash(
            lock,
            "source_sha256",
            include_bytes!("../../tests/fixtures/settlement-local-chain/src/SettlementHarness.sol"),
        );
        assert_fixture_hash(
            lock,
            "foundry_toml_sha256",
            include_bytes!("../../tests/fixtures/settlement-local-chain/foundry.toml"),
        );
        assert_fixture_hash(
            lock,
            "creation_bytecode_sha256",
            include_bytes!(
                "../../tests/fixtures/settlement-local-chain/artifacts/SettlementHarness.bytecode"
            ),
        );
        assert_eq!(
            lock_value(lock, "foundry_image"),
            format!("ghcr.io/foundry-rs/foundry:{FOUNDRY_IMAGE_TAG}")
        );
    }

    fn assert_fixture_hash(lock: &str, key: &str, content: &[u8]) {
        assert_eq!(
            lock_value(lock, key),
            hex::encode(Sha256::digest(content)),
            "{key} must match its reviewed fixture"
        );
    }

    fn lock_value<'a>(lock: &'a str, key: &str) -> &'a str {
        lock.lines()
            .find_map(|line| {
                line.strip_prefix(key)
                    .and_then(|value| value.strip_prefix('='))
            })
            .unwrap_or_else(|| panic!("{key} is missing from artifact.lock"))
    }
    const STANDARD_REDEEM_GOLDEN: &str = concat!(
        "01b7037c",
        "000000000000000000000000c011a7e12a19f7b1f670d46f03b03f3342e82dfb",
        "0000000000000000000000000000000000000000000000000000000000000000",
        "1111111111111111111111111111111111111111111111111111111111111111",
        "0000000000000000000000000000000000000000000000000000000000000080",
        "0000000000000000000000000000000000000000000000000000000000000002",
        "0000000000000000000000000000000000000000000000000000000000000001",
        "0000000000000000000000000000000000000000000000000000000000000002",
    );

    #[derive(Clone, Copy)]
    struct RpcScenario {
        approved: bool,
        paused: bool,
        residual_usdce: u64,
        simulation_reverts: bool,
    }

    impl Default for RpcScenario {
        fn default() -> Self {
            Self {
                approved: true,
                paused: false,
                residual_usdce: 0,
                simulation_reverts: false,
            }
        }
    }

    async fn test_reader(scenario: RpcScenario) -> (MockServer, AlloySettlementAdapterReader) {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(move |request: &Request| rpc_response(request, scenario))
            .mount(&server)
            .await;
        let reader = AlloySettlementAdapterReader::connect(&OnchainConfig {
            rpc_endpoint: PolygonRpcEndpoint::Public { url: server.uri() },
            rpc_timeout_ms: 5_000,
        })
        .expect("test RPC client");
        (server, reader)
    }

    fn rpc_response(request: &Request, scenario: RpcScenario) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&request.body).expect("JSON-RPC request");
        let id = body["id"].clone();
        let method = body["method"].as_str().expect("JSON-RPC method");
        if method == "eth_call" {
            return rpc_call_response(&body, &id, scenario);
        }
        let result = match method {
            "eth_chainId" => json!("0x89"),
            "eth_getBlockByNumber" => json!({
                "hash": verified_deployment_fixture(SettlementRoute::StandardV2)
                    .verified_block_hash()
                    .as_str()
            }),
            unexpected => panic!("unexpected JSON-RPC method: {unexpected}"),
        };
        ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        }))
    }

    fn rpc_call_response(body: &Value, id: &Value, scenario: RpcScenario) -> ResponseTemplate {
        let call = &body["params"][0];
        let input = call
            .get("input")
            .or_else(|| call.get("data"))
            .and_then(Value::as_str)
            .expect("eth_call input");
        let selector = &input[2..10];
        let result = if selector == call_selector::<payoutDenominatorCall>() {
            uint_result(1)
        } else if selector == call_selector::<payoutNumeratorsCall>() {
            if input.ends_with(&format!("{:064x}", 0_u64)) {
                uint_result(1)
            } else {
                uint_result(0)
            }
        } else if selector == call_selector::<balanceOfCall>() {
            if input.ends_with(&format!("{:064x}", 11_u64)) {
                uint_result(12_500_000)
            } else {
                uint_result(3_000_000)
            }
        } else if selector == call_selector::<isApprovedForAllCall>() {
            bool_result(scenario.approved)
        } else if selector == call_selector::<pausedCall>() {
            bool_result(scenario.paused)
        } else if selector == call_selector::<Erc20BalanceOfCall>() {
            uint_result(scenario.residual_usdce)
        } else if selector == call_selector::<redeemPositionsCall>() {
            if scenario.simulation_reverts {
                return ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {
                        "code": 3,
                        "message": "execution reverted: fixture"
                    }
                }));
            }
            "0x".to_owned()
        } else {
            panic!("unexpected eth_call selector: {selector}");
        };
        ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        }))
    }

    fn uint_result(value: u64) -> String {
        format!("0x{value:064x}")
    }

    fn call_selector<C: SolCall>() -> String {
        hex::encode(C::SELECTOR)
    }

    fn bool_result(value: bool) -> String {
        uint_result(u64::from(value))
    }

    fn binary_tokens() -> SettlementBinaryTokenPair {
        SettlementBinaryTokenPair {
            yes: TokenId::new("11"),
            no: TokenId::new("22"),
        }
    }

    async fn start_local_chain() -> (ContainerAsync<GenericImage>, AlloySettlementAdapterReader) {
        let fixture_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/settlement-local-chain"
        );
        let command = concat!(
            "anvil --host 0.0.0.0 --port 8545 --chain-id 137 >/tmp/anvil.log 2>&1 & ",
            "anvil_pid=$!; ",
            "until cast chain-id --rpc-url http://127.0.0.1:8545 >/dev/null 2>&1; ",
            "do sleep 0.1; done; ",
            "cast send --rpc-url http://127.0.0.1:8545 --unlocked ",
            "--from 0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266 --json ",
            "--create \"$(cat /fixture/artifacts/SettlementHarness.bytecode)\"; ",
            "wait $anvil_pid"
        );
        let container = GenericImage::new("ghcr.io/foundry-rs/foundry", FOUNDRY_IMAGE_TAG)
            .with_exposed_port(8545.into())
            .with_wait_for(WaitFor::message_on_stdout("contractAddress"))
            .with_mount(
                Mount::bind_mount(fixture_path, "/fixture").with_access_mode(AccessMode::ReadOnly),
            )
            .with_cmd([command])
            .start()
            .await
            .expect("start disposable Anvil settlement fixture");
        let port = container
            .get_host_port_ipv4(8545)
            .await
            .expect("resolve disposable Anvil port");
        let reader = AlloySettlementAdapterReader::connect(&OnchainConfig {
            rpc_endpoint: PolygonRpcEndpoint::Public {
                url: format!("http://127.0.0.1:{port}"),
            },
            rpc_timeout_ms: 5_000,
        })
        .expect("local-chain reader");
        (container, reader)
    }

    async fn initialize_local_chain(
        reader: &AlloySettlementAdapterReader,
    ) -> (Address, Address, Address) {
        let catalog =
            SettlementDeploymentCatalog::official_current().expect("built-in settlement catalog");
        let harness_address = Address::from_str(HARNESS_ADDRESS).expect("harness address");
        let harness = LocalSettlementHarnessView::new(harness_address, &reader.provider);
        let ctf_template = harness
            .CTF_TEMPLATE()
            .call()
            .await
            .expect("CTF template address");
        let pusd_template = harness
            .PUSD_TEMPLATE()
            .call()
            .await
            .expect("pUSD template address");
        let usdce_template = harness
            .USDCE_TEMPLATE()
            .call()
            .await
            .expect("USDC.e template address");
        let ctf =
            Address::from_str(catalog.conditional_tokens.as_str()).expect("catalog CTF address");
        let pusd =
            Address::from_str(catalog.collateral_token.as_str()).expect("catalog pUSD address");
        let usdce = Address::from_str(catalog.usdce.as_str()).expect("catalog USDC.e address");
        let ctf_code = reader
            .provider
            .get_code_at(ctf_template)
            .await
            .expect("CTF fixture runtime");
        let pusd_code = reader
            .provider
            .get_code_at(pusd_template)
            .await
            .expect("pUSD fixture runtime");
        let usdce_code = reader
            .provider
            .get_code_at(usdce_template)
            .await
            .expect("USDC.e fixture runtime");
        let ctf_set: Value = reader
            .provider
            .raw_request("anvil_setCode".into(), (ctf, ctf_code))
            .await
            .expect("etch CTF fixture at official target");
        let pusd_set: Value = reader
            .provider
            .raw_request("anvil_setCode".into(), (pusd, pusd_code))
            .await
            .expect("etch pUSD fixture at official target");
        let usdce_set: Value = reader
            .provider
            .raw_request("anvil_setCode".into(), (usdce, usdce_code))
            .await
            .expect("etch USDC.e fixture at official target");
        assert!(ctf_set.is_null() && pusd_set.is_null() && usdce_set.is_null());

        let funder = Address::from_str(ANVIL_FUNDER).expect("Anvil funder");
        let initialize = LocalSettlementHarnessView::initializeCall {
            funder,
            ctf,
            pusd,
            usdce,
        }
        .abi_encode();
        send_local_call(reader, funder, harness_address, initialize).await;
        (
            harness.standard().call().await.expect("standard adapter"),
            harness.negRisk().call().await.expect("Neg Risk adapter"),
            harness_address,
        )
    }

    async fn send_local_call(
        reader: &AlloySettlementAdapterReader,
        funder: Address,
        target: Address,
        calldata: Vec<u8>,
    ) {
        let receipt = reader
            .provider
            .send_transaction(
                TransactionRequest::default()
                    .from(funder)
                    .to(target)
                    .input(TransactionInput::new(Bytes::from(calldata))),
            )
            .await
            .expect("submit disposable local-chain call")
            .get_receipt()
            .await
            .expect("confirm disposable local-chain call");
        assert!(receipt.status());
    }

    async fn local_capability(
        reader: &AlloySettlementAdapterReader,
        route: SettlementRoute,
        target: Address,
    ) -> VerifiedSettlementDeployment {
        let block_number = reader
            .provider
            .get_block_number()
            .await
            .expect("local block number");
        let block = reader
            .provider
            .get_block_by_number(BlockNumberOrTag::Number(block_number))
            .await
            .expect("local block read")
            .expect("local block exists");
        verified_deployment_fixture_at(
            route,
            EvmAddress::parse(format!("{target:#x}")).expect("local adapter address"),
            EvmAddress::parse(ANVIL_FUNDER).expect("local funder address"),
            block_number,
            EvmBlockHash::parse(format!("{:#x}", block.hash())).expect("local block hash"),
        )
    }

    #[test]
    fn redeem_calldata_matches_official_full_balance_abi_for_both_routes() {
        for route in [SettlementRoute::StandardV2, SettlementRoute::NegRiskV2] {
            let capability = verified_deployment_fixture(route);
            let call =
                SettlementAdapterGateway::build_redeem_call(&capability, &MarketId::new(CONDITION))
                    .expect("verified route builds canonical redemption");

            assert_eq!(call.purpose(), SettlementSubmissionPurpose::Redeem);
            assert_eq!(call.route(), route);
            assert_eq!(call.call_target(), capability.target());
            assert_eq!(call.target_adapter(), capability.target());
            assert_eq!(call.conditional_tokens(), capability.conditional_tokens());
            assert_eq!(call.collateral_token(), capability.collateral_token());
            assert_eq!(call.usdce(), capability.usdce());
            assert_eq!(hex::encode(call.calldata()), STANDARD_REDEEM_GOLDEN);
        }
    }

    #[test]
    fn approval_and_revocation_target_ctf_and_bind_the_verified_adapter() {
        let capability = verified_deployment_fixture(SettlementRoute::NegRiskV2);
        let approval = SettlementAdapterGateway
            .prepare_operator_approval(&capability)
            .expect("approval calldata");
        let revocation = SettlementAdapterGateway
            .prepare_operator_revocation(&capability)
            .expect("revocation calldata");

        assert_eq!(approval.call_target(), capability.conditional_tokens());
        assert_eq!(approval.collateral_token(), capability.collateral_token());
        assert_eq!(approval.usdce(), capability.usdce());
        assert_eq!(
            approval.purpose(),
            SettlementSubmissionPurpose::OutcomeTokenApproval
        );
        assert_eq!(
            revocation.purpose(),
            SettlementSubmissionPurpose::OutcomeTokenRevocation
        );
        assert_eq!(&approval.calldata()[..4], &[0xa2, 0x2c, 0xb4, 0x65]);
        assert_eq!(
            &approval.calldata()[4 + 12..4 + 32],
            &hex::decode(capability.target().as_str().trim_start_matches("0x"))
                .expect("adapter hex")
        );
        assert_eq!(approval.calldata().last(), Some(&1));
        assert_eq!(revocation.calldata().last(), Some(&0));
    }

    #[test]
    fn invalid_market_identity_cannot_reach_calldata() {
        let capability = verified_deployment_fixture(SettlementRoute::StandardV2);
        assert!(
            SettlementAdapterGateway::build_redeem_call(
                &capability,
                &MarketId::new("not-a-condition")
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn read_only_preflight_freezes_balances_payout_and_canonical_block() {
        let (_server, reader) = test_reader(RpcScenario::default()).await;
        let capability = verified_deployment_fixture(SettlementRoute::StandardV2);
        let route = reader
            .verify_redeem_route(&capability, &MarketId::new(CONDITION), &binary_tokens())
            .await
            .expect("read-only preflight succeeds");
        let preflight = route.preflight();

        assert_eq!(preflight.block_number, capability.verified_block());
        assert_eq!(preflight.block_hash, *capability.verified_block_hash());
        assert_eq!(preflight.payout_vector.denominator.as_str(), "1");
        assert_eq!(preflight.payout_vector.yes.as_str(), "1");
        assert_eq!(preflight.payout_vector.no.as_str(), "0");
        assert_eq!(preflight.balances.yes.shares, Shares::new(dec!(12.5)));
        assert_eq!(preflight.balances.no.shares, Shares::new(dec!(3)));
    }

    #[tokio::test]
    async fn preflight_fails_closed_on_pause_missing_approval_residual_or_revert() {
        for (scenario, expected) in [
            (
                RpcScenario {
                    residual_usdce: 1,
                    ..RpcScenario::default()
                },
                "residual",
            ),
            (
                RpcScenario {
                    paused: true,
                    ..RpcScenario::default()
                },
                "paused",
            ),
            (
                RpcScenario {
                    approved: false,
                    ..RpcScenario::default()
                },
                "approval",
            ),
            (
                RpcScenario {
                    simulation_reverts: true,
                    ..RpcScenario::default()
                },
                "simulation",
            ),
        ] {
            let (_server, reader) = test_reader(scenario).await;
            let capability = verified_deployment_fixture(SettlementRoute::NegRiskV2);
            let error = reader
                .verify_redeem_route(&capability, &MarketId::new(CONDITION), &binary_tokens())
                .await
                .expect_err("unsafe preflight must fail");
            match (expected, error) {
                ("paused", SettlementAdapterError::AdapterPaused)
                | ("approval", SettlementAdapterError::MissingOperatorApproval)
                | ("residual", SettlementAdapterError::AdapterResidualUsdce { .. })
                | ("simulation", SettlementAdapterError::SimulationReverted { .. }) => {}
                (_, unexpected) => panic!("unexpected {expected} failure: {unexpected}"),
            }
        }
    }

    #[tokio::test]
    async fn local_chain_sweeps_full_balances_for_both_routes_and_enforces_safety_gates() {
        let (container, reader) = start_local_chain().await;
        let (standard, neg_risk, harness_address) = initialize_local_chain(&reader).await;
        let funder = Address::from_str(ANVIL_FUNDER).expect("Anvil funder");
        let capability = verified_deployment_fixture(SettlementRoute::StandardV2);
        let ctf = Address::from_str(capability.conditional_tokens().as_str())
            .expect("fixture CTF address");
        let pusd = Address::from_str(capability.collateral_token().as_str())
            .expect("fixture pUSD address");
        let ctf_view = ConditionalTokensSettlementView::new(ctf, &reader.provider);
        let pusd_view = LocalPusdView::new(pusd, &reader.provider);

        for (route, adapter) in [
            (SettlementRoute::StandardV2, standard),
            (SettlementRoute::NegRiskV2, neg_risk),
        ] {
            let seed = LocalSettlementHarnessView::seedCall {
                funder,
                adapter,
                ctf,
            }
            .abi_encode();
            send_local_call(&reader, funder, harness_address, seed).await;
            let capability = local_capability(&reader, route, adapter).await;
            let redeem_route = reader
                .verify_redeem_route(&capability, &MarketId::new(CONDITION), &binary_tokens())
                .await
                .expect("local route preflight");
            let preflight = redeem_route.preflight();
            let call = SettlementAdapterGateway.prepare_redeem(&redeem_route);
            assert_eq!(preflight.balances.yes.shares, Shares::new(dec!(12.5)));
            assert_eq!(preflight.balances.no.shares, Shares::new(dec!(3)));

            let payout_before = pusd_view
                .balanceOf(funder)
                .call()
                .await
                .expect("pUSD balance before");
            send_local_call(&reader, funder, adapter, call.calldata().to_vec()).await;
            let yes_after = ctf_view
                .balanceOf(funder, U256::from(11))
                .call()
                .await
                .expect("YES balance after");
            let no_after = ctf_view
                .balanceOf(funder, U256::from(22))
                .call()
                .await
                .expect("NO balance after");
            let payout_after = pusd_view
                .balanceOf(funder)
                .call()
                .await
                .expect("pUSD balance after");
            assert!(yes_after.is_zero() && no_after.is_zero());
            assert_eq!(
                payout_after.checked_sub(payout_before),
                Some(U256::from(12_500_000))
            );
        }

        let seed_standard = LocalSettlementHarnessView::seedCall {
            funder,
            adapter: standard,
            ctf,
        }
        .abi_encode();
        send_local_call(&reader, funder, harness_address, seed_standard).await;
        send_local_call(
            &reader,
            funder,
            standard,
            LocalAdapterWrite::setPausedCall { value: true }.abi_encode(),
        )
        .await;
        let paused_capability =
            local_capability(&reader, SettlementRoute::StandardV2, standard).await;
        assert!(matches!(
            reader
                .verify_redeem_route(
                    &paused_capability,
                    &MarketId::new(CONDITION),
                    &binary_tokens(),
                )
                .await,
            Err(SettlementAdapterError::AdapterPaused)
        ));

        let seed_neg_risk = LocalSettlementHarnessView::seedCall {
            funder,
            adapter: neg_risk,
            ctf,
        }
        .abi_encode();
        send_local_call(&reader, funder, harness_address, seed_neg_risk).await;
        let neg_risk_capability =
            local_capability(&reader, SettlementRoute::NegRiskV2, neg_risk).await;
        let revoke = SettlementAdapterGateway
            .prepare_operator_revocation(&neg_risk_capability)
            .expect("local operator revocation");
        send_local_call(&reader, funder, ctf, revoke.calldata().to_vec()).await;
        let revoked_capability =
            local_capability(&reader, SettlementRoute::NegRiskV2, neg_risk).await;
        assert!(matches!(
            reader
                .verify_redeem_route(
                    &revoked_capability,
                    &MarketId::new(CONDITION),
                    &binary_tokens(),
                )
                .await,
            Err(SettlementAdapterError::MissingOperatorApproval)
        ));

        container
            .rm()
            .await
            .expect("remove disposable Anvil settlement fixture");
    }
}
