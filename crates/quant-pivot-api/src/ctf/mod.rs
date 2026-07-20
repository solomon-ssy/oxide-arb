//! Narrow CTF client for standard binary settlement redemption.

use crate::keystore::OrderSigner;
use alloy::{
    network::{Ethereum, EthereumWallet},
    primitives::{Address, B256, TxHash, U256},
    providers::{DynProvider, PendingTransactionBuilder, Provider, ProviderBuilder},
    rpc::client::RpcClient,
    signers::Signer,
    sol,
    transports::http::Http,
};
use quant_pivot_error::rpc::RpcError;
use quant_pivot_models::{
    config::PolymarketConfig,
    constants::{CTF_ADDRESS, POLYGON_CHAIN_ID, PUSD_ADDRESS},
    types::{MarketId, TokenId},
};
use reqwest::Url;
use std::{fmt::Display, str::FromStr, time::Duration};

const STANDARD_BINARY_INDEX_SETS: [u64; 2] = [1, 2];

sol! {
    #[sol(rpc)]
    interface ConditionalTokens {
        function payoutDenominator(bytes32 conditionId) external view returns (uint256);
        function payoutNumerators(bytes32 conditionId, uint256 index) external view returns (uint256);
        function balanceOf(address account, uint256 id) external view returns (uint256);
        function redeemPositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] calldata indexSets
        ) external;
    }
}

/// Resolved CTF payout vector for a binary condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CtfBinaryPayoutVector {
    pub denominator: U256,
    pub yes: U256,
    pub no: U256,
}

impl CtfBinaryPayoutVector {
    /// Returns true once the CTF oracle has published final payout numerators.
    #[must_use]
    pub fn is_resolved(&self) -> bool {
        !self.denominator.is_zero()
    }
}

/// ERC-1155 balances for the YES/NO tokens of a standard binary market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CtfBinaryBalances {
    pub yes: U256,
    pub no: U256,
}

/// Mined receipt summary for a standard binary redeem transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CtfRedeemReceipt {
    pub tx_hash: String,
    pub block_number: u64,
    pub gas_used: u64,
    pub effective_gas_price_wei: u128,
}

/// Receipt status for an already-submitted standard binary redeem transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CtfSubmittedRedeemReceipt {
    Pending,
    Confirmed(CtfRedeemReceipt),
    Reverted { tx_hash: String },
}

/// Submitted redeem transaction awaiting finality.
#[derive(Debug)]
pub struct CtfPendingRedeem {
    tx_hash: String,
    pending_tx: PendingTransactionBuilder<Ethereum>,
}

impl CtfPendingRedeem {
    /// Transaction hash returned by Polygon immediately after submission.
    #[must_use]
    pub fn tx_hash(&self) -> &str {
        &self.tx_hash
    }

    /// Wait for the configured number of confirmations and return the mined receipt.
    pub async fn wait(self, confirmations: u64) -> Result<CtfRedeemReceipt, RpcError> {
        let receipt = self
            .pending_tx
            .with_required_confirmations(confirmations)
            .get_receipt()
            .await
            .map_err(|e| rpc_call_failed("redeemPositions.receipt", e))?;

        if !receipt.status() {
            return Err(RpcError::CallFailed {
                method: "redeemPositions.receipt".into(),
                reason: format!("transaction {} reverted", self.tx_hash),
            });
        }

        let block_number = receipt.block_number.ok_or_else(|| RpcError::CallFailed {
            method: "redeemPositions.receipt".into(),
            reason: format!("transaction {} receipt has no block_number", self.tx_hash),
        })?;

        Ok(CtfRedeemReceipt {
            tx_hash: self.tx_hash,
            block_number,
            gas_used: receipt.gas_used,
            effective_gas_price_wei: receipt.effective_gas_price,
        })
    }
}

/// Polygon CTF client scoped to standard binary `redeemPositions`.
#[derive(Clone)]
pub struct CtfClient {
    contract: ConditionalTokens::ConditionalTokensInstance<DynProvider>,
    provider: DynProvider,
    collateral_token: Address,
    signer_address: Address,
}

impl CtfClient {
    /// Connect to Polygon CTF using the EOA signer configured for quant-pivot.
    pub fn connect(signer: &OrderSigner, config: &PolymarketConfig) -> Result<Self, RpcError> {
        if config.chain_id != POLYGON_CHAIN_ID {
            return Err(RpcError::ConnectionFailed(format!(
                "unsupported chain id {}; expected Polygon {POLYGON_CHAIN_ID}",
                config.chain_id
            )));
        }

        let rpc_url = Url::parse(config.onchain.rpc_url()).map_err(|error| {
            RpcError::ConnectionFailed(format!(
                "configured Polygon RPC endpoint is invalid: {error}"
            ))
        })?;

        let mut private_key = signer.inner().clone();
        private_key.set_chain_id(Some(config.chain_id));
        let wallet = EthereumWallet::new(private_key);

        // Bind the RPC request timeout: reqwest defaults to *no* timeout, which
        // would let a money-moving redeem/oracle call hang indefinitely on a
        // stalled provider. `onchain.rpc_timeout_ms` is the hard per-call bound.
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.onchain.rpc_timeout_ms))
            .build()
            .map_err(|e| {
                RpcError::ConnectionFailed(format!("failed to build Polygon RPC HTTP client: {e}"))
            })?;
        let transport = Http::with_client(http_client, rpc_url);
        let rpc_client = RpcClient::new(transport, false);
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_client(rpc_client)
            .erased();

        let ctf_address = parse_address(CTF_ADDRESS, "CTF_ADDRESS")?;
        let collateral_token = parse_address(PUSD_ADDRESS, "PUSD_ADDRESS")?;
        let contract = ConditionalTokens::new(ctf_address, provider.clone());

        Ok(Self {
            contract,
            provider,
            collateral_token,
            signer_address: signer.address(),
        })
    }

    /// Address that will sign and pay gas for redeem transactions.
    #[must_use]
    pub const fn signer_address(&self) -> Address {
        self.signer_address
    }

    /// Read the CTF payout vector for a binary condition.
    pub async fn binary_payout_vector(
        &self,
        market_id: &MarketId,
    ) -> Result<CtfBinaryPayoutVector, RpcError> {
        let condition_id = parse_condition_id(market_id)?;
        let denominator = self
            .contract
            .payoutDenominator(condition_id)
            .call()
            .await
            .map_err(|e| rpc_call_failed("payoutDenominator", e))?;
        let yes = self
            .contract
            .payoutNumerators(condition_id, U256::from(0_u8))
            .call()
            .await
            .map_err(|e| rpc_call_failed("payoutNumerators[0]", e))?;
        let no = self
            .contract
            .payoutNumerators(condition_id, U256::from(1_u8))
            .call()
            .await
            .map_err(|e| rpc_call_failed("payoutNumerators[1]", e))?;

        Ok(CtfBinaryPayoutVector {
            denominator,
            yes,
            no,
        })
    }

    /// Read YES/NO token balances for the funder before or after redemption.
    pub async fn binary_balances(
        &self,
        funder: Address,
        yes_token_id: &TokenId,
        no_token_id: &TokenId,
    ) -> Result<CtfBinaryBalances, RpcError> {
        let yes_token = parse_token_id(yes_token_id)?;
        let no_token = parse_token_id(no_token_id)?;
        let yes = self
            .contract
            .balanceOf(funder, yes_token)
            .call()
            .await
            .map_err(|e| rpc_call_failed("balanceOf[yes]", e))?;
        let no = self
            .contract
            .balanceOf(funder, no_token)
            .call()
            .await
            .map_err(|e| rpc_call_failed("balanceOf[no]", e))?;

        Ok(CtfBinaryBalances { yes, no })
    }

    /// Read YES/NO token balances for a checksummed or lower-case funder address.
    pub async fn binary_balances_for_funder(
        &self,
        funder_address: &str,
        yes_token_id: &TokenId,
        no_token_id: &TokenId,
    ) -> Result<CtfBinaryBalances, RpcError> {
        let funder = Address::from_str(funder_address).map_err(|e| RpcError::CallFailed {
            method: "parse_funder_address".into(),
            reason: format!("invalid funder address '{funder_address}': {e}"),
        })?;
        self.binary_balances(funder, yes_token_id, no_token_id)
            .await
    }

    /// Run an `eth_call` simulation for standard binary redemption (from the EOA
    /// signer — valid only when the signer itself holds the YES/NO tokens).
    pub async fn simulate_standard_binary_redeem(
        &self,
        market_id: &MarketId,
    ) -> Result<(), RpcError> {
        let condition_id = parse_condition_id(market_id)?;
        self.contract
            .redeemPositions(
                self.collateral_token,
                B256::ZERO,
                condition_id,
                standard_binary_index_sets(),
            )
            .call()
            .await
            .map_err(|e| rpc_call_failed("redeemPositions.call", e))?;

        Ok(())
    }

    /// Run an `eth_call` simulation for standard binary redemption *from* the
    /// money-holding wallet (the proxy/Safe funder), which is the address that
    /// actually owns the positions when settling through the relayer.
    pub async fn simulate_standard_binary_redeem_from_funder(
        &self,
        market_id: &MarketId,
        funder_address: &str,
    ) -> Result<(), RpcError> {
        let from = Address::from_str(funder_address).map_err(|e| RpcError::CallFailed {
            method: "parse_funder_address".into(),
            reason: format!("invalid funder address '{funder_address}': {e}"),
        })?;
        let condition_id = parse_condition_id(market_id)?;
        self.contract
            .redeemPositions(
                self.collateral_token,
                B256::ZERO,
                condition_id,
                standard_binary_index_sets(),
            )
            .from(from)
            .call()
            .await
            .map_err(|e| rpc_call_failed("redeemPositions.call", e))?;

        Ok(())
    }

    /// Submit a standard binary CTF redeem transaction without waiting for finality.
    pub async fn submit_standard_binary_redeem(
        &self,
        market_id: &MarketId,
    ) -> Result<CtfPendingRedeem, RpcError> {
        let condition_id = parse_condition_id(market_id)?;
        let pending_tx = self
            .contract
            .redeemPositions(
                self.collateral_token,
                B256::ZERO,
                condition_id,
                standard_binary_index_sets(),
            )
            .send()
            .await
            .map_err(|e| rpc_call_failed("redeemPositions.send", e))?;
        let tx_hash = *pending_tx.tx_hash();

        Ok(CtfPendingRedeem {
            tx_hash: tx_hash.to_string(),
            pending_tx,
        })
    }

    /// Fetch an already-submitted redeem receipt once it reaches confirmation depth.
    pub async fn submitted_redeem_receipt(
        &self,
        tx_hash: &str,
        confirmations: u64,
    ) -> Result<CtfSubmittedRedeemReceipt, RpcError> {
        let tx_hash = TxHash::from_str(tx_hash).map_err(|e| RpcError::CallFailed {
            method: "parse_tx_hash".into(),
            reason: format!("invalid redeem tx hash '{tx_hash}': {e}"),
        })?;
        let Some(receipt) = self
            .provider
            .get_transaction_receipt(tx_hash)
            .await
            .map_err(|e| rpc_call_failed("eth_getTransactionReceipt", e))?
        else {
            return Ok(CtfSubmittedRedeemReceipt::Pending);
        };

        let Some(block_number) = receipt.block_number else {
            return Ok(CtfSubmittedRedeemReceipt::Pending);
        };
        let latest = self
            .provider
            .get_block_number()
            .await
            .map_err(|e| rpc_call_failed("eth_blockNumber", e))?;
        let required_depth = confirmations.saturating_sub(1);
        if latest < block_number.saturating_add(required_depth) {
            return Ok(CtfSubmittedRedeemReceipt::Pending);
        }
        if !receipt.status() {
            return Ok(CtfSubmittedRedeemReceipt::Reverted {
                tx_hash: tx_hash.to_string(),
            });
        }

        Ok(CtfSubmittedRedeemReceipt::Confirmed(CtfRedeemReceipt {
            tx_hash: tx_hash.to_string(),
            block_number,
            gas_used: receipt.gas_used,
            effective_gas_price_wei: receipt.effective_gas_price,
        }))
    }

    /// Submit and wait for a standard binary CTF redeem transaction.
    pub async fn redeem_standard_binary(
        &self,
        market_id: &MarketId,
        confirmations: u64,
    ) -> Result<CtfRedeemReceipt, RpcError> {
        self.submit_standard_binary_redeem(market_id)
            .await?
            .wait(confirmations)
            .await
    }
}

fn standard_binary_index_sets() -> Vec<U256> {
    STANDARD_BINARY_INDEX_SETS
        .into_iter()
        .map(U256::from)
        .collect()
}

fn parse_condition_id(market_id: &MarketId) -> Result<B256, RpcError> {
    B256::from_str(market_id.as_str()).map_err(|e| RpcError::CallFailed {
        method: "parse_condition_id".into(),
        reason: format!("invalid MarketId '{}': {e}", market_id.as_str()),
    })
}

fn parse_token_id(token_id: &TokenId) -> Result<U256, RpcError> {
    U256::from_str_radix(token_id.as_str(), 10).map_err(|e| RpcError::CallFailed {
        method: "parse_token_id".into(),
        reason: format!("invalid TokenId '{}': {e}", token_id.as_str()),
    })
}

fn parse_address(value: &str, label: &str) -> Result<Address, RpcError> {
    Address::from_str(value).map_err(|e| RpcError::ConnectionFailed(format!("{label}: {e}")))
}

fn rpc_call_failed<E: Display>(method: &str, err: E) -> RpcError {
    RpcError::CallFailed {
        method: method.into(),
        reason: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_binary_index_sets_are_yes_no_partition() {
        assert_eq!(
            standard_binary_index_sets(),
            vec![U256::from(1_u8), U256::from(2_u8)]
        );
    }

    #[test]
    fn parses_condition_id_and_decimal_token_id_separately() -> Result<(), RpcError> {
        let condition =
            MarketId::new("0x0102030405060708091011121314151617181920212223242526272829303132");
        let token = TokenId::new("12345678901234567890");

        assert_eq!(
            parse_condition_id(&condition)?.to_string(),
            "0x0102030405060708091011121314151617181920212223242526272829303132"
        );
        assert_eq!(parse_token_id(&token)?.to_string(), "12345678901234567890");
        Ok(())
    }
}
