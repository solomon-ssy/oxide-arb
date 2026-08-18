//! V2 per-user pause reads and calldata preparation.

use std::{str::FromStr, time::Duration};

use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    primitives::{Address, Bytes, U256, keccak256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::client::RpcClient,
    rpc::types::Filter,
    sol,
    sol_types::{SolCall, SolEvent},
    transports::http::Http,
};
use quant_pivot_models::{
    config::OnchainConfig,
    types::{ContentHash, EvmAddress, EvmBlockHash, EvmCalldataHash, EvmTransactionHash},
};
use reqwest::{Client, Url};
use thiserror::Error;

use self::IUserPausableV2::{UserPaused, UserUnpaused};
use crate::{exchange::constants::EXCHANGE_CONTRACTS, settlement::wallet_call::PreparedWalletCall};

sol! {
    #[sol(rpc)]
    interface IUserPausableV2 {
        function userPauseBlockInterval() external view returns (uint256);
        function userPausedBlockAt(address user) external view returns (uint256);
        function isUserPaused(address user) external view returns (bool);
        function pauseUser() external;
        function unpauseUser() external;
        event UserPaused(address indexed user, uint256 effectivePauseBlock);
        event UserUnpaused(address indexed user);
    }
}

#[derive(Debug, Error)]
pub enum UserPauseError {
    #[error("invalid user-pause client configuration: {detail}")]
    InvalidConfiguration { detail: String },
    #[error("user-pause chain read failed: {detail}")]
    Read { detail: String },
    #[error("user-pause value exceeds runtime range: {field}={value}")]
    Overflow { field: &'static str, value: U256 },
    #[error("pause is already requested or active at block {effective_block}")]
    AlreadyPaused { effective_block: u64 },
    #[error("user is not paused")]
    NotPaused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserPauseState {
    pub current_block: u64,
    pub interval_blocks: u64,
    pub effective_block: Option<u64>,
    pub active: bool,
}

impl UserPauseState {
    fn try_new(
        current_block: u64,
        interval: U256,
        paused_at: U256,
        active: bool,
    ) -> Result<Self, UserPauseError> {
        let interval_blocks = u64::try_from(interval).map_err(|_| UserPauseError::Overflow {
            field: "userPauseBlockInterval",
            value: interval,
        })?;
        let effective_block = if paused_at.is_zero() {
            None
        } else {
            Some(
                u64::try_from(paused_at).map_err(|_| UserPauseError::Overflow {
                    field: "userPausedBlockAt",
                    value: paused_at,
                })?,
            )
        };
        Ok(Self {
            current_block,
            interval_blocks,
            effective_block,
            active,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedUserPauseCall {
    pub funder: EvmAddress,
    pub exchange: EvmAddress,
    pub calldata: Bytes,
    pub requested_block: u64,
    pub interval_blocks: u64,
    pub effective_block: u64,
    pub deployment_digest: ContentHash,
    pub calldata_hash: EvmCalldataHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserPauseEventEvidence {
    pub block_number: u64,
    pub block_hash: EvmBlockHash,
    pub transaction_hash: EvmTransactionHash,
    pub log_index: u64,
    pub effective_block: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedUserUnpauseCall {
    pub funder: EvmAddress,
    pub exchange: EvmAddress,
    pub calldata: Bytes,
    pub requested_block: u64,
    pub deployment_digest: ContentHash,
    pub calldata_hash: EvmCalldataHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserUnpauseEventEvidence {
    pub block_number: u64,
    pub block_hash: EvmBlockHash,
    pub transaction_hash: EvmTransactionHash,
    pub log_index: u64,
}

impl PreparedWalletCall for PreparedUserPauseCall {
    fn funder(&self) -> &EvmAddress {
        &self.funder
    }

    fn call_target(&self) -> &EvmAddress {
        &self.exchange
    }

    fn calldata(&self) -> &[u8] {
        self.calldata.as_ref()
    }

    fn target_adapter(&self) -> &EvmAddress {
        &self.exchange
    }

    fn deployment_digest(&self) -> ContentHash {
        self.deployment_digest
    }

    fn calldata_hash(&self) -> &EvmCalldataHash {
        &self.calldata_hash
    }
}

impl PreparedWalletCall for PreparedUserUnpauseCall {
    fn funder(&self) -> &EvmAddress {
        &self.funder
    }

    fn call_target(&self) -> &EvmAddress {
        &self.exchange
    }

    fn calldata(&self) -> &[u8] {
        self.calldata.as_ref()
    }

    fn target_adapter(&self) -> &EvmAddress {
        &self.exchange
    }

    fn deployment_digest(&self) -> ContentHash {
        self.deployment_digest
    }

    fn calldata_hash(&self) -> &EvmCalldataHash {
        &self.calldata_hash
    }
}

pub struct AlloyUserPauseReader {
    provider: DynProvider,
}

impl AlloyUserPauseReader {
    pub fn connect(config: &OnchainConfig) -> Result<Self, UserPauseError> {
        let rpc_url =
            Url::parse(config.rpc_url()).map_err(|error| UserPauseError::InvalidConfiguration {
                detail: error.to_string(),
            })?;
        let http = Client::builder()
            .timeout(Duration::from_millis(config.rpc_timeout_ms))
            .build()
            .map_err(|error| UserPauseError::InvalidConfiguration {
                detail: error.to_string(),
            })?;
        let transport = Http::with_client(http, rpc_url);
        let client = RpcClient::new(transport, false);
        Ok(Self {
            provider: ProviderBuilder::new().connect_client(client).erased(),
        })
    }

    pub async fn state(
        &self,
        exchange: &EvmAddress,
        user: &EvmAddress,
    ) -> Result<UserPauseState, UserPauseError> {
        let exchange = Address::from_str(exchange.as_str()).map_err(|error| {
            UserPauseError::InvalidConfiguration {
                detail: error.to_string(),
            }
        })?;
        let user = Address::from_str(user.as_str()).map_err(|error| {
            UserPauseError::InvalidConfiguration {
                detail: error.to_string(),
            }
        })?;
        let contract = IUserPausableV2::new(exchange, &self.provider);
        let finalized = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Finalized)
            .await
            .map_err(|error| UserPauseError::Read {
                detail: error.to_string(),
            })?
            .ok_or_else(|| UserPauseError::Read {
                detail: "Polygon RPC returned no finalized block".to_owned(),
            })?;
        let current_block = finalized.header.number;
        let block = BlockId::hash_canonical(finalized.hash());
        let interval = contract
            .userPauseBlockInterval()
            .block(block)
            .call()
            .await
            .map_err(|error| UserPauseError::Read {
                detail: error.to_string(),
            })?;
        let paused_at = contract
            .userPausedBlockAt(user)
            .block(block)
            .call()
            .await
            .map_err(|error| UserPauseError::Read {
                detail: error.to_string(),
            })?;
        let active = contract
            .isUserPaused(user)
            .block(block)
            .call()
            .await
            .map_err(|error| UserPauseError::Read {
                detail: error.to_string(),
            })?;
        UserPauseState::try_new(current_block, interval, paused_at, active)
    }

    pub async fn prepare_pause(
        &self,
        exchange: &EvmAddress,
        user: &EvmAddress,
    ) -> Result<PreparedUserPauseCall, UserPauseError> {
        let state = self.state(exchange, user).await?;
        if let Some(effective_block) = state.effective_block {
            return Err(UserPauseError::AlreadyPaused { effective_block });
        }
        let effective_block = state
            .current_block
            .checked_add(state.interval_blocks)
            .ok_or(UserPauseError::Overflow {
                field: "effectivePauseBlock",
                value: U256::from(state.current_block) + U256::from(state.interval_blocks),
            })?;
        let contract = EXCHANGE_CONTRACTS
            .iter()
            .find(|contract| {
                format!("{:#x}", contract.address).eq_ignore_ascii_case(exchange.as_str())
            })
            .ok_or_else(|| UserPauseError::InvalidConfiguration {
                detail: "exchange is outside the registered V2 topology".to_owned(),
            })?;
        let calldata: Bytes = IUserPausableV2::pauseUserCall {}.abi_encode().into();
        let calldata_hash = EvmCalldataHash::parse(format!("{:#x}", keccak256(&calldata)))
            .map_err(|error| UserPauseError::InvalidConfiguration {
                detail: error.to_string(),
            })?;
        let deployment_digest = ContentHash::parse(contract.bytecode_blake3).map_err(|error| {
            UserPauseError::InvalidConfiguration {
                detail: error.to_string(),
            }
        })?;
        Ok(PreparedUserPauseCall {
            funder: user.clone(),
            exchange: exchange.clone(),
            calldata,
            requested_block: state.current_block,
            interval_blocks: state.interval_blocks,
            effective_block,
            deployment_digest,
            calldata_hash,
        })
    }

    pub async fn prepare_unpause(
        &self,
        exchange: &EvmAddress,
        user: &EvmAddress,
    ) -> Result<PreparedUserUnpauseCall, UserPauseError> {
        let state = self.state(exchange, user).await?;
        if state.effective_block.is_none() {
            return Err(UserPauseError::NotPaused);
        }
        let contract = EXCHANGE_CONTRACTS
            .iter()
            .find(|contract| {
                format!("{:#x}", contract.address).eq_ignore_ascii_case(exchange.as_str())
            })
            .ok_or_else(|| UserPauseError::InvalidConfiguration {
                detail: "exchange is outside the registered V2 topology".to_owned(),
            })?;
        let calldata: Bytes = IUserPausableV2::unpauseUserCall {}.abi_encode().into();
        let calldata_hash = EvmCalldataHash::parse(format!("{:#x}", keccak256(&calldata)))
            .map_err(|error| UserPauseError::InvalidConfiguration {
                detail: error.to_string(),
            })?;
        let deployment_digest = ContentHash::parse(contract.bytecode_blake3).map_err(|error| {
            UserPauseError::InvalidConfiguration {
                detail: error.to_string(),
            }
        })?;
        Ok(PreparedUserUnpauseCall {
            funder: user.clone(),
            exchange: exchange.clone(),
            calldata,
            requested_block: state.current_block,
            deployment_digest,
            calldata_hash,
        })
    }

    pub async fn pause_event(
        &self,
        exchange: &EvmAddress,
        user: &EvmAddress,
        from_block: u64,
    ) -> Result<Option<UserPauseEventEvidence>, UserPauseError> {
        let exchange_address = Address::from_str(exchange.as_str()).map_err(|error| {
            UserPauseError::InvalidConfiguration {
                detail: error.to_string(),
            }
        })?;
        let user_address = Address::from_str(user.as_str()).map_err(|error| {
            UserPauseError::InvalidConfiguration {
                detail: error.to_string(),
            }
        })?;
        let finalized = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Finalized)
            .await
            .map_err(|error| UserPauseError::Read {
                detail: error.to_string(),
            })?
            .ok_or_else(|| UserPauseError::Read {
                detail: "Polygon RPC returned no finalized block".to_owned(),
            })?;
        if from_block > finalized.header.number {
            return Ok(None);
        }
        let filter = Filter::new()
            .address(exchange_address)
            .event_signature(UserPaused::SIGNATURE_HASH)
            .from_block(from_block)
            .to_block(finalized.header.number);
        let logs = self
            .provider
            .get_logs(&filter)
            .await
            .map_err(|error| UserPauseError::Read {
                detail: error.to_string(),
            })?;
        let mut evidence = None;
        for log in logs {
            let Ok(decoded) = UserPaused::decode_log(log.as_ref()) else {
                continue;
            };
            if decoded.user != user_address {
                continue;
            }
            let block_number = log.block_number.ok_or_else(|| UserPauseError::Read {
                detail: "finalized UserPaused log has no block number".to_owned(),
            })?;
            let block_hash = log.block_hash.ok_or_else(|| UserPauseError::Read {
                detail: "finalized UserPaused log has no block hash".to_owned(),
            })?;
            let transaction_hash = log.transaction_hash.ok_or_else(|| UserPauseError::Read {
                detail: "finalized UserPaused log has no transaction hash".to_owned(),
            })?;
            let log_index = log.log_index.ok_or_else(|| UserPauseError::Read {
                detail: "finalized UserPaused log has no log index".to_owned(),
            })?;
            evidence = Some(UserPauseEventEvidence {
                block_number,
                block_hash: EvmBlockHash::parse(format!("{block_hash:#x}")).map_err(|error| {
                    UserPauseError::Read {
                        detail: error.to_string(),
                    }
                })?,
                transaction_hash: EvmTransactionHash::parse(format!("{transaction_hash:#x}"))
                    .map_err(|error| UserPauseError::Read {
                        detail: error.to_string(),
                    })?,
                log_index,
                effective_block: u64::try_from(decoded.effectivePauseBlock).map_err(|_| {
                    UserPauseError::Overflow {
                        field: "UserPaused.effectivePauseBlock",
                        value: decoded.effectivePauseBlock,
                    }
                })?,
            });
            break;
        }
        Ok(evidence)
    }

    pub async fn unpause_event(
        &self,
        exchange: &EvmAddress,
        user: &EvmAddress,
        from_block: u64,
    ) -> Result<Option<UserUnpauseEventEvidence>, UserPauseError> {
        let exchange_address = Address::from_str(exchange.as_str()).map_err(|error| {
            UserPauseError::InvalidConfiguration {
                detail: error.to_string(),
            }
        })?;
        let user_address = Address::from_str(user.as_str()).map_err(|error| {
            UserPauseError::InvalidConfiguration {
                detail: error.to_string(),
            }
        })?;
        let finalized = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Finalized)
            .await
            .map_err(|error| UserPauseError::Read {
                detail: error.to_string(),
            })?
            .ok_or_else(|| UserPauseError::Read {
                detail: "Polygon RPC returned no finalized block".to_owned(),
            })?;
        if from_block > finalized.header.number {
            return Ok(None);
        }
        let filter = Filter::new()
            .address(exchange_address)
            .event_signature(UserUnpaused::SIGNATURE_HASH)
            .from_block(from_block)
            .to_block(finalized.header.number);
        let logs = self
            .provider
            .get_logs(&filter)
            .await
            .map_err(|error| UserPauseError::Read {
                detail: error.to_string(),
            })?;
        for log in logs {
            let Ok(decoded) = UserUnpaused::decode_log(log.as_ref()) else {
                continue;
            };
            if decoded.user != user_address {
                continue;
            }
            let block_number = log.block_number.ok_or_else(|| UserPauseError::Read {
                detail: "finalized UserUnpaused log has no block number".to_owned(),
            })?;
            let block_hash = log.block_hash.ok_or_else(|| UserPauseError::Read {
                detail: "finalized UserUnpaused log has no block hash".to_owned(),
            })?;
            let transaction_hash = log.transaction_hash.ok_or_else(|| UserPauseError::Read {
                detail: "finalized UserUnpaused log has no transaction hash".to_owned(),
            })?;
            let log_index = log.log_index.ok_or_else(|| UserPauseError::Read {
                detail: "finalized UserUnpaused log has no log index".to_owned(),
            })?;
            return Ok(Some(UserUnpauseEventEvidence {
                block_number,
                block_hash: EvmBlockHash::parse(format!("{block_hash:#x}")).map_err(|error| {
                    UserPauseError::Read {
                        detail: error.to_string(),
                    }
                })?,
                transaction_hash: EvmTransactionHash::parse(format!("{transaction_hash:#x}"))
                    .map_err(|error| UserPauseError::Read {
                        detail: error.to_string(),
                    })?,
                log_index,
            }));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use alloy::{primitives::U256, sol_types::SolCall};

    use super::{IUserPausableV2, UserPauseState};

    #[test]
    fn interval_is_dynamic() {
        let state = UserPauseState::try_new(1_000, U256::from(237), U256::ZERO, false)
            .expect("pause state");
        assert_eq!(state.interval_blocks, 237);
        assert_eq!(state.effective_block, None);
    }

    #[test]
    fn calldata_matches_abi() {
        assert_eq!(
            &IUserPausableV2::pauseUserCall {}.abi_encode()[..4],
            IUserPausableV2::pauseUserCall::SELECTOR,
        );
        assert_eq!(
            &IUserPausableV2::unpauseUserCall {}.abi_encode()[..4],
            IUserPausableV2::unpauseUserCall::SELECTOR,
        );
    }
}
