//! Finalized account collateral and outcome-token balance evidence.

use std::{collections::BTreeSet, str::FromStr, time::Duration};

use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    primitives::{Address, B256, U256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::client::RpcClient,
    sol,
    transports::http::Http,
};
use quant_pivot_models::{
    config::OnchainConfig,
    constants::COLLATERAL_SCALE,
    domain::quant::AccountRecoveryTokenBalance,
    hashing::CanonicalDigest,
    types::{ContentHash, EvmAddress, EvmBlockHash, Shares, TokenId, Usd},
};
use reqwest::{Client, Url};
use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

use super::constants::EXCHANGE_CONTRACTS;

const POLYGON_CHAIN_ID: u64 = 137;
const SNAPSHOT_DOMAIN: &str = "quant-pivot/finalized-account-balances";
const SNAPSHOT_VERSION: u32 = 1;

sol! {
    #[sol(rpc)]
    interface ExchangeAssetView {
        function getCollateral() external view returns (address);
        function getCtf() external view returns (address);
    }

    #[sol(rpc)]
    interface Erc20BalanceView {
        function balanceOf(address account) external view returns (uint256);
    }

    #[sol(rpc)]
    interface Erc1155BalanceView {
        function balanceOf(address account, uint256 id) external view returns (uint256);
    }
}

#[derive(Debug, Error)]
pub enum AccountRecoveryReadError {
    #[error("invalid account-recovery reader configuration: {detail}")]
    InvalidConfiguration { detail: String },
    #[error("account-recovery chain read failed: {detail}")]
    Read { detail: String },
    #[error("account-recovery reader connected to chain {actual}, expected Polygon 137")]
    WrongChain { actual: u64 },
    #[error("finalized block {block_number} changed during account recovery")]
    FinalizedBlockChanged { block_number: u64 },
    #[error("account-recovery numeric evidence is invalid: {detail}")]
    Numeric { detail: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedAccountBalanceSnapshot {
    pub block_number: u64,
    pub block_hash: EvmBlockHash,
    pub collateral_usd: Usd,
    pub positions: Vec<AccountRecoveryTokenBalance>,
    pub source_hash: ContentHash,
}

#[derive(Serialize)]
struct SnapshotPreimage<'a> {
    block_number: u64,
    block_hash: &'a EvmBlockHash,
    collateral_tokens: &'a [String],
    conditional_tokens: &'a [String],
    collateral_usd: Usd,
    positions: &'a [AccountRecoveryTokenBalance],
}

pub struct AlloyAccountRecoveryReader {
    provider: DynProvider,
}

impl AlloyAccountRecoveryReader {
    pub fn connect(config: &OnchainConfig) -> Result<Self, AccountRecoveryReadError> {
        let rpc_url = Url::parse(config.rpc_url()).map_err(|error| {
            AccountRecoveryReadError::InvalidConfiguration {
                detail: error.to_string(),
            }
        })?;
        let http = Client::builder()
            .timeout(Duration::from_millis(config.rpc_timeout_ms))
            .build()
            .map_err(|error| AccountRecoveryReadError::InvalidConfiguration {
                detail: error.to_string(),
            })?;
        let client = RpcClient::new(Http::with_client(http, rpc_url), false);
        Ok(Self {
            provider: ProviderBuilder::new().connect_client(client).erased(),
        })
    }

    pub async fn snapshot(
        &self,
        funder: &EvmAddress,
        token_ids: &[TokenId],
    ) -> Result<FinalizedAccountBalanceSnapshot, AccountRecoveryReadError> {
        let chain_id = self
            .provider
            .get_chain_id()
            .await
            .map_err(|error| read_error(&error))?;
        if chain_id != POLYGON_CHAIN_ID {
            return Err(AccountRecoveryReadError::WrongChain { actual: chain_id });
        }
        let finalized = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Finalized)
            .await
            .map_err(|error| read_error(&error))?
            .ok_or_else(|| AccountRecoveryReadError::Read {
                detail: "Polygon RPC returned no finalized block".to_owned(),
            })?;
        let block_number = finalized.header.number;
        let canonical_hash = finalized.hash();
        let block = BlockId::hash_canonical(canonical_hash);
        let funder = Address::from_str(funder.as_str()).map_err(|error| {
            AccountRecoveryReadError::InvalidConfiguration {
                detail: error.to_string(),
            }
        })?;
        let (collateral_tokens, conditional_tokens) = self.asset_contracts(block).await?;
        let collateral_usd = self
            .collateral_balance(funder, block, &collateral_tokens)
            .await?;
        let positions = self
            .position_balances(funder, block, &conditional_tokens, token_ids)
            .await?;
        let block_hash = EvmBlockHash::parse(format!("{canonical_hash:#x}")).map_err(|error| {
            AccountRecoveryReadError::Read {
                detail: error.to_string(),
            }
        })?;
        let collateral_addresses = address_strings(&collateral_tokens);
        let conditional_addresses = address_strings(&conditional_tokens);
        let source_hash = CanonicalDigest::content_hash_typed(
            SNAPSHOT_DOMAIN,
            SNAPSHOT_VERSION,
            &SnapshotPreimage {
                block_number,
                block_hash: &block_hash,
                collateral_tokens: &collateral_addresses,
                conditional_tokens: &conditional_addresses,
                collateral_usd,
                positions: &positions,
            },
        )
        .map_err(|error| AccountRecoveryReadError::Read {
            detail: error.to_string(),
        })?;
        self.recheck_block(block_number, canonical_hash).await?;
        Ok(FinalizedAccountBalanceSnapshot {
            block_number,
            block_hash,
            collateral_usd,
            positions,
            source_hash,
        })
    }

    async fn asset_contracts(
        &self,
        block: BlockId,
    ) -> Result<(BTreeSet<Address>, BTreeSet<Address>), AccountRecoveryReadError> {
        let mut collateral = BTreeSet::new();
        let mut conditional = BTreeSet::new();
        for exchange in EXCHANGE_CONTRACTS {
            let view = ExchangeAssetView::new(exchange.address, &self.provider);
            collateral.insert(
                view.getCollateral()
                    .block(block)
                    .call()
                    .await
                    .map_err(|error| read_error(&error))?,
            );
            conditional.insert(
                view.getCtf()
                    .block(block)
                    .call()
                    .await
                    .map_err(|error| read_error(&error))?,
            );
        }
        Ok((collateral, conditional))
    }

    async fn collateral_balance(
        &self,
        funder: Address,
        block: BlockId,
        contracts: &BTreeSet<Address>,
    ) -> Result<Usd, AccountRecoveryReadError> {
        let mut raw = U256::ZERO;
        for contract in contracts {
            raw = raw
                .checked_add(
                    Erc20BalanceView::new(*contract, &self.provider)
                        .balanceOf(funder)
                        .block(block)
                        .call()
                        .await
                        .map_err(|error| read_error(&error))?,
                )
                .ok_or_else(|| AccountRecoveryReadError::Numeric {
                    detail: "collateral balance overflow".to_owned(),
                })?;
        }
        scaled(raw).map(Usd::new)
    }

    async fn position_balances(
        &self,
        funder: Address,
        block: BlockId,
        contracts: &BTreeSet<Address>,
        token_ids: &[TokenId],
    ) -> Result<Vec<AccountRecoveryTokenBalance>, AccountRecoveryReadError> {
        let mut unique = token_ids.to_vec();
        unique.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        unique.dedup();
        let mut positions = Vec::with_capacity(unique.len());
        for token_id in unique {
            let token = U256::from_str(token_id.as_str()).map_err(|error| {
                AccountRecoveryReadError::Numeric {
                    detail: format!("invalid token id {}: {error}", token_id.as_str()),
                }
            })?;
            let mut raw = U256::ZERO;
            for contract in contracts {
                raw = raw
                    .checked_add(
                        Erc1155BalanceView::new(*contract, &self.provider)
                            .balanceOf(funder, token)
                            .block(block)
                            .call()
                            .await
                            .map_err(|error| read_error(&error))?,
                    )
                    .ok_or_else(|| AccountRecoveryReadError::Numeric {
                        detail: format!("position balance overflow for {token_id}"),
                    })?;
            }
            let shares = Shares::new(scaled(raw)?);
            if !shares.is_zero() {
                positions.push(AccountRecoveryTokenBalance { token_id, shares });
            }
        }
        Ok(positions)
    }

    async fn recheck_block(
        &self,
        block_number: u64,
        expected_hash: B256,
    ) -> Result<(), AccountRecoveryReadError> {
        let current = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Number(block_number))
            .await
            .map_err(|error| read_error(&error))?
            .ok_or_else(|| AccountRecoveryReadError::Read {
                detail: format!("finalized block {block_number} disappeared"),
            })?;
        if current.hash() != expected_hash {
            return Err(AccountRecoveryReadError::FinalizedBlockChanged { block_number });
        }
        Ok(())
    }
}

fn scaled(raw: U256) -> Result<Decimal, AccountRecoveryReadError> {
    let integer =
        Decimal::from_str(&raw.to_string()).map_err(|error| AccountRecoveryReadError::Numeric {
            detail: error.to_string(),
        })?;
    Ok(integer / Decimal::from(COLLATERAL_SCALE))
}

fn address_strings(addresses: &BTreeSet<Address>) -> Vec<String> {
    addresses
        .iter()
        .map(|address| format!("{address:#x}"))
        .collect()
}

fn read_error(error: &impl ToString) -> AccountRecoveryReadError {
    AccountRecoveryReadError::Read {
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;
    use rust_decimal_macros::dec;

    use super::scaled;

    #[test]
    fn raw_balances_scale_exactly() {
        assert_eq!(scaled(U256::from(12_345_678)).ok(), Some(dec!(12.345678)));
    }
}
