//! Finalized current-deployment scanner for externally initiated redemptions.

use std::{fmt::Display, str::FromStr, time::Duration};

use alloy::{
    eips::BlockNumberOrTag,
    primitives::{Address, B256, Bytes, U256, keccak256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::{
        client::RpcClient,
        types::{Filter, Log},
    },
    transports::http::Http,
};
use quant_pivot_models::{
    config::OnchainConfig,
    types::{EvmAddress, EvmBlockHash, EvmTransactionHash, EvmUint256, MarketId},
};
use reqwest::{Client, Url};

use super::{
    confirmation::{ObservedWrappedPayout, decode_wrapped_payout},
    contracts::VerifiedSettlementDeployment,
    typed::{
        IntoEvmAddress, IntoEvmBlockHash, IntoEvmTransactionHash, IntoEvmUint, SettlementValueError,
    },
};

const POLYGON_CHAIN_ID: u64 = 137;

/// One exact current-adapter payout log suitable for external reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalSettlementObservation {
    pub transaction_hash: EvmTransactionHash,
    pub block_number: u64,
    pub block_hash: EvmBlockHash,
    pub payout_log_index: u64,
    pub wrapped_log_index: u64,
    pub market_id: MarketId,
    pub raw_payout: EvmUint256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedPayoutRedemption {
    conditional_tokens: EvmAddress,
    redeemer: EvmAddress,
    collateral_token: EvmAddress,
    parent_collection_id: B256,
    market_id: MarketId,
    index_sets: [U256; 2],
    raw_payout: U256,
}

#[derive(Debug, Clone)]
struct LocatedPayoutRedemption {
    transaction_hash: EvmTransactionHash,
    block_number: u64,
    block_hash: EvmBlockHash,
    log_index: u64,
    payout: ObservedPayoutRedemption,
}

#[derive(Debug, Clone)]
struct LocatedWrappedPayout {
    transaction_hash: EvmTransactionHash,
    block_number: u64,
    block_hash: EvmBlockHash,
    log_index: u64,
    payout: ObservedWrappedPayout,
}

/// Canonical finalized range result. Persist observations before advancing its cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalSettlementScan {
    pub from_block: u64,
    pub to_block: u64,
    pub to_block_hash: EvmBlockHash,
    pub observations: Vec<ExternalSettlementObservation>,
}

/// Read-only scanner. A raw address cannot select a target; callers must supply
/// a freshly verified current-deployment capability.
pub struct ExternalSettlementScanner {
    provider: DynProvider,
}

impl ExternalSettlementScanner {
    /// Build a bounded read-only Polygon client. No request is issued here.
    pub fn connect(config: &OnchainConfig) -> Result<Self, ExternalSettlementScanError> {
        let rpc_url = Url::parse(config.rpc_url()).map_err(|source| {
            ExternalSettlementScanError::InvalidConfiguration {
                detail: source.to_string(),
            }
        })?;
        let http = Client::builder()
            .timeout(Duration::from_millis(config.rpc_timeout_ms))
            .build()
            .map_err(|source| ExternalSettlementScanError::InvalidConfiguration {
                detail: source.to_string(),
            })?;
        let client = RpcClient::new(Http::with_client(http, rpc_url), false);
        Ok(Self {
            provider: ProviderBuilder::new().connect_client(client).erased(),
        })
    }

    /// Scan one inclusive finalized range. `None` means `from_block` is not finalized yet.
    pub async fn scan_finalized(
        &self,
        deployment: &VerifiedSettlementDeployment,
        from_block: u64,
        requested_to_block: u64,
    ) -> Result<Option<ExternalSettlementScan>, ExternalSettlementScanError> {
        if requested_to_block < from_block {
            return Err(ExternalSettlementScanError::InvalidRange {
                from_block,
                to_block: requested_to_block,
            });
        }
        let chain_id = self
            .provider
            .get_chain_id()
            .await
            .map_err(|source| rpc_error("eth_chainId", &source))?;
        if chain_id != POLYGON_CHAIN_ID {
            return Err(ExternalSettlementScanError::WrongChain {
                expected: POLYGON_CHAIN_ID,
                actual: chain_id,
            });
        }
        let finalized = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Finalized)
            .await
            .map_err(|source| rpc_error("eth_getBlockByNumber(finalized)", &source))?
            .ok_or(ExternalSettlementScanError::MissingFinalizedBlock)?;
        let finalized_number = finalized.header.number;
        if from_block > finalized_number {
            return Ok(None);
        }
        let to_block = requested_to_block.min(finalized_number);
        let collateral =
            Address::from_str(deployment.collateral_token().as_str()).map_err(|source| {
                ExternalSettlementScanError::CapabilityCorrupt {
                    detail: source.to_string(),
                }
            })?;
        let conditional_tokens = Address::from_str(deployment.conditional_tokens().as_str())
            .map_err(|source| ExternalSettlementScanError::CapabilityCorrupt {
                detail: source.to_string(),
            })?;
        let payout_filter = Filter::new()
            .address(conditional_tokens)
            .event_signature(keccak256(
                "PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)",
            ))
            .from_block(from_block)
            .to_block(to_block);
        let payout_logs = self
            .provider
            .get_logs(&payout_filter)
            .await
            .map_err(|source| rpc_error("eth_getLogs(PayoutRedemption)", &source))?;
        let wrapped_filter = Filter::new()
            .address(collateral)
            .event_signature(keccak256("Wrapped(address,address,address,uint256)"))
            .from_block(from_block)
            .to_block(to_block);
        let wrapped_logs = self
            .provider
            .get_logs(&wrapped_filter)
            .await
            .map_err(|source| rpc_error("eth_getLogs(Wrapped)", &source))?;
        let mut payouts = locate_payout_redemptions(payout_logs, deployment)?;
        let mut wrapped = locate_wrapped_payouts(wrapped_logs, deployment)?;
        payouts.sort_by_key(|entry| (entry.block_number, entry.log_index));
        wrapped.sort_by_key(|entry| (entry.block_number, entry.log_index));
        let observations = pair_external_redemptions(&payouts, &wrapped)?;
        let canonical = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Number(to_block))
            .await
            .map_err(|source| rpc_error("eth_getBlockByNumber(cursor recheck)", &source))?
            .ok_or(ExternalSettlementScanError::MissingCanonicalBlock { block: to_block })?;
        let to_block_hash = (canonical.header.hash).into_evm_block_hash()?;
        if to_block == finalized_number && canonical.header.hash != finalized.header.hash {
            return Err(ExternalSettlementScanError::CanonicalHashChanged { block: to_block });
        }
        Ok(Some(ExternalSettlementScan {
            from_block,
            to_block,
            to_block_hash,
            observations,
        }))
    }
}

fn locate_payout_redemptions(
    logs: Vec<Log>,
    deployment: &VerifiedSettlementDeployment,
) -> Result<Vec<LocatedPayoutRedemption>, ExternalSettlementScanError> {
    let mut payouts = Vec::new();
    for log in logs {
        if log.removed {
            return Err(ExternalSettlementScanError::RemovedLog);
        }
        let block_number =
            log.block_number
                .ok_or(ExternalSettlementScanError::MissingLogField {
                    field: "block_number",
                })?;
        let block_hash = log
            .block_hash
            .ok_or(ExternalSettlementScanError::MissingLogField {
                field: "block_hash",
            })?;
        let transaction_hash =
            log.transaction_hash
                .ok_or(ExternalSettlementScanError::MissingLogField {
                    field: "transaction_hash",
                })?;
        let log_index = log
            .log_index
            .ok_or(ExternalSettlementScanError::MissingLogField { field: "log_index" })?;
        let log_address = Address::from_slice(log.address().as_slice());
        let log_topics = log
            .topics()
            .iter()
            .map(|topic| B256::from_slice(topic.as_slice()))
            .collect::<Vec<_>>();
        let log_data = Bytes::copy_from_slice(log.data().data.as_ref());
        let Some(payout) = decode_payout_redemption(log_address, &log_topics, &log_data)? else {
            continue;
        };
        if !matches_deployment_redemption(
            deployment.conditional_tokens().as_str(),
            deployment.target().as_str(),
            deployment.usdce().as_str(),
            &payout,
        ) {
            continue;
        }
        payouts.push(LocatedPayoutRedemption {
            transaction_hash: (transaction_hash).into_evm_transaction_hash()?,
            block_number,
            block_hash: (block_hash).into_evm_block_hash()?,
            log_index,
            payout,
        });
    }
    Ok(payouts)
}

fn locate_wrapped_payouts(
    logs: Vec<Log>,
    deployment: &VerifiedSettlementDeployment,
) -> Result<Vec<LocatedWrappedPayout>, ExternalSettlementScanError> {
    let mut wrapped = Vec::new();
    for log in logs {
        if log.removed {
            return Err(ExternalSettlementScanError::RemovedLog);
        }
        let block_number =
            log.block_number
                .ok_or(ExternalSettlementScanError::MissingLogField {
                    field: "block_number",
                })?;
        let block_hash = log
            .block_hash
            .ok_or(ExternalSettlementScanError::MissingLogField {
                field: "block_hash",
            })?;
        let transaction_hash =
            log.transaction_hash
                .ok_or(ExternalSettlementScanError::MissingLogField {
                    field: "transaction_hash",
                })?;
        let log_index = log
            .log_index
            .ok_or(ExternalSettlementScanError::MissingLogField { field: "log_index" })?;
        let log_address = Address::from_slice(log.address().as_slice());
        let log_topics = log
            .topics()
            .iter()
            .map(|topic| B256::from_slice(topic.as_slice()))
            .collect::<Vec<_>>();
        let log_data = Bytes::copy_from_slice(log.data().data.as_ref());
        let Some(payout) = decode_wrapped_payout(log_address, &log_topics, &log_data, log_index)
            .map_err(|source| ExternalSettlementScanError::InvalidLog {
                detail: source.to_string(),
            })?
        else {
            continue;
        };
        if !matches_deployment_payout(
            deployment.target().as_str(),
            deployment.usdce().as_str(),
            deployment.funder().as_str(),
            &payout,
        ) {
            continue;
        }
        wrapped.push(LocatedWrappedPayout {
            transaction_hash: (transaction_hash).into_evm_transaction_hash()?,
            block_number,
            block_hash: (block_hash).into_evm_block_hash()?,
            log_index,
            payout,
        });
    }
    Ok(wrapped)
}

fn decode_payout_redemption(
    conditional_tokens: Address,
    topics: &[B256],
    data: &Bytes,
) -> Result<Option<ObservedPayoutRedemption>, ExternalSettlementScanError> {
    let signature =
        keccak256("PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)");
    if topics.first() != Some(&signature) {
        return Ok(None);
    }
    if topics.len() != 4 || data.len() != 192 {
        return Err(invalid_log(
            "PayoutRedemption requires four topics and exact binary [1,2] ABI data",
        ));
    }
    let offset = U256::from_be_slice(&data[32..64]);
    let index_count = U256::from_be_slice(&data[96..128]);
    let index_sets = [
        U256::from_be_slice(&data[128..160]),
        U256::from_be_slice(&data[160..192]),
    ];
    if offset != U256::from(96) || index_count != U256::from(2) {
        return Err(invalid_log(
            "PayoutRedemption indexSets must be the canonical two-element dynamic field",
        ));
    }
    Ok(Some(ObservedPayoutRedemption {
        conditional_tokens: (conditional_tokens).into_evm_address()?,
        redeemer: (Address::from_slice(&topics[1].as_slice()[12..])).into_evm_address()?,
        collateral_token: (Address::from_slice(&topics[2].as_slice()[12..])).into_evm_address()?,
        parent_collection_id: topics[3],
        market_id: MarketId::new(format!("{:#x}", B256::from_slice(&data[..32]))),
        index_sets,
        raw_payout: U256::from_be_slice(&data[64..96]),
    }))
}

fn matches_deployment_redemption(
    conditional_tokens: &str,
    target: &str,
    usdce: &str,
    payout: &ObservedPayoutRedemption,
) -> bool {
    payout.conditional_tokens.as_str() == conditional_tokens
        && payout.redeemer.as_str() == target
        && payout.collateral_token.as_str() == usdce
        && payout.parent_collection_id == B256::ZERO
        && payout.index_sets == [U256::from(1), U256::from(2)]
}

fn pair_external_redemptions(
    payouts: &[LocatedPayoutRedemption],
    wrapped: &[LocatedWrappedPayout],
) -> Result<Vec<ExternalSettlementObservation>, ExternalSettlementScanError> {
    let mut observations = Vec::with_capacity(payouts.len());
    for (index, payout) in payouts.iter().enumerate() {
        let next_payout_log = payouts[index + 1..]
            .iter()
            .find(|candidate| candidate.transaction_hash == payout.transaction_hash)
            .map_or(u64::MAX, |candidate| candidate.log_index);
        let candidates = wrapped
            .iter()
            .filter(|candidate| {
                let wrapped_raw = candidate.payout.raw_amount;
                let redemption_raw = payout.payout.raw_payout;
                candidate.transaction_hash == payout.transaction_hash
                    && candidate.block_number == payout.block_number
                    && candidate.block_hash == payout.block_hash
                    && candidate.log_index > payout.log_index
                    && candidate.log_index < next_payout_log
                    && wrapped_raw == redemption_raw
            })
            .collect::<Vec<_>>();
        let [wrapped] = candidates.as_slice() else {
            return Err(ExternalSettlementScanError::PayoutPairAmbiguous {
                transaction_hash: payout.transaction_hash.clone(),
                payout_log_index: payout.log_index,
                wrapped_matches: candidates.len(),
            });
        };
        observations.push(ExternalSettlementObservation {
            transaction_hash: payout.transaction_hash.clone(),
            block_number: payout.block_number,
            block_hash: payout.block_hash.clone(),
            payout_log_index: payout.log_index,
            wrapped_log_index: wrapped.log_index,
            market_id: payout.payout.market_id.clone(),
            raw_payout: (payout.payout.raw_payout).into_evm_uint()?,
        });
    }
    Ok(observations)
}

fn matches_deployment_payout(
    target: &str,
    usdce: &str,
    funder: &str,
    payout: &ObservedWrappedPayout,
) -> bool {
    payout.caller.as_str() == target
        && payout.asset.as_str() == usdce
        && payout.to.as_str() == funder
}

/// Closed read-side failures; none can be interpreted as a successful redemption.
#[derive(Debug, thiserror::Error)]
pub enum ExternalSettlementScanError {
    #[error("invalid settlement scanner configuration: {detail}")]
    InvalidConfiguration { detail: String },
    #[error("invalid finalized scan range {from_block}..={to_block}")]
    InvalidRange { from_block: u64, to_block: u64 },
    #[error("settlement scanner requires chain {expected}, observed {actual}")]
    WrongChain { expected: u64, actual: u64 },
    #[error("verified settlement capability is corrupt: {detail}")]
    CapabilityCorrupt { detail: String },
    #[error("Polygon RPC call `{method}` failed: {detail}")]
    Rpc {
        method: &'static str,
        detail: String,
    },
    #[error("Polygon RPC did not return a finalized block")]
    MissingFinalizedBlock,
    #[error("Polygon RPC did not return canonical block {block}")]
    MissingCanonicalBlock { block: u64 },
    #[error("canonical block hash changed while scanning block {block}")]
    CanonicalHashChanged { block: u64 },
    #[error("Wrapped log was marked removed")]
    RemovedLog,
    #[error("Wrapped log is missing `{field}`")]
    MissingLogField { field: &'static str },
    #[error("Wrapped log is invalid: {detail}")]
    InvalidLog { detail: String },
    #[error(
        "external redemption {transaction_hash} payout log {payout_log_index} has {wrapped_matches} matching Wrapped logs"
    )]
    PayoutPairAmbiguous {
        transaction_hash: EvmTransactionHash,
        payout_log_index: u64,
        wrapped_matches: usize,
    },
}

impl From<SettlementValueError> for ExternalSettlementScanError {
    fn from(error: SettlementValueError) -> Self {
        Self::InvalidLog {
            detail: error.detail().to_owned(),
        }
    }
}

fn invalid_log(detail: impl Into<String>) -> ExternalSettlementScanError {
    ExternalSettlementScanError::InvalidLog {
        detail: detail.into(),
    }
}

fn rpc_error(method: &'static str, source: &impl Display) -> ExternalSettlementScanError {
    ExternalSettlementScanError::Rpc {
        method,
        detail: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, Bytes, U256, keccak256};
    use quant_pivot_models::{enums::settlement::SettlementRoute, types::EvmAddress};

    use super::*;
    use crate::settlement::contracts::verified_deployment_fixture;

    #[test]
    fn scanner_requires_exact_pair() {
        let capability = verified_deployment_fixture(SettlementRoute::StandardV2);
        let wrapped_topics = vec![
            keccak256("Wrapped(address,address,address,uint256)"),
            address_topic(capability.target()),
            address_topic(capability.usdce()),
            address_topic(capability.funder()),
        ];
        let wrapped_data = Bytes::copy_from_slice(&U256::from(1_000_000_u64).to_be_bytes::<32>());
        let wrapped_payout = decode_wrapped_payout(
            Address::from_str(capability.collateral_token().as_str())
                .expect("fixture collateral address"),
            &wrapped_topics,
            &wrapped_data,
            7,
        )
        .expect("decode exact Wrapped event")
        .expect("Wrapped event matches signature");
        assert!(matches_deployment_payout(
            capability.target().as_str(),
            capability.usdce().as_str(),
            capability.funder().as_str(),
            &wrapped_payout,
        ));

        let condition = B256::repeat_byte(0x11);
        let redemption_topics = vec![
            keccak256("PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)"),
            address_topic(capability.target()),
            address_topic(capability.usdce()),
            B256::ZERO,
        ];
        let redemption_data = payout_redemption_data(condition, U256::from(1_000_000_u64));
        let redemption = decode_payout_redemption(
            Address::from_str(capability.conditional_tokens().as_str())
                .expect("fixture CTF address"),
            &redemption_topics,
            &redemption_data,
        )
        .expect("decode exact PayoutRedemption")
        .expect("PayoutRedemption event matches signature");
        assert!(matches_deployment_redemption(
            capability.conditional_tokens().as_str(),
            capability.target().as_str(),
            capability.usdce().as_str(),
            &redemption,
        ));
        assert_eq!(redemption.market_id.as_str(), format!("{condition:#x}"));

        let transaction_hash = EvmTransactionHash::parse(format!("{:#x}", B256::repeat_byte(0x22)))
            .expect("fixture transaction hash");
        let block_hash = EvmBlockHash::parse(format!("{:#x}", B256::repeat_byte(0x33)))
            .expect("fixture block hash");
        let observations = pair_external_redemptions(
            &[LocatedPayoutRedemption {
                transaction_hash: transaction_hash.clone(),
                block_number: 10,
                block_hash: block_hash.clone(),
                log_index: 5,
                payout: redemption,
            }],
            &[LocatedWrappedPayout {
                transaction_hash,
                block_number: 10,
                block_hash,
                log_index: 7,
                payout: wrapped_payout,
            }],
        )
        .expect("exact payout evidence pair");
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].payout_log_index, 5);
        assert_eq!(observations[0].wrapped_log_index, 7);
    }

    fn payout_redemption_data(condition: B256, payout: U256) -> Bytes {
        let mut data = Vec::with_capacity(192);
        data.extend_from_slice(condition.as_slice());
        data.extend_from_slice(&U256::from(96).to_be_bytes::<32>());
        data.extend_from_slice(&payout.to_be_bytes::<32>());
        data.extend_from_slice(&U256::from(2).to_be_bytes::<32>());
        data.extend_from_slice(&U256::from(1).to_be_bytes::<32>());
        data.extend_from_slice(&U256::from(2).to_be_bytes::<32>());
        Bytes::from(data)
    }

    fn address_topic(address: &EvmAddress) -> B256 {
        let parsed = Address::from_str(address.as_str()).expect("fixture address");
        let mut topic = [0_u8; 32];
        topic[12..].copy_from_slice(parsed.as_slice());
        B256::from(topic)
    }
}
