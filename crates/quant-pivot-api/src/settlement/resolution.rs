//! Signer-free finalized Conditional Tokens resolution source.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
    str::FromStr,
    time::Duration,
};

use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    primitives::{Address, B256, Bytes, U256, keccak256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::{
        client::RpcClient,
        types::{Filter, Log},
    },
    sol,
    transports::http::Http,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use quant_pivot_models::{
    config::OnchainConfig,
    hashing::CanonicalDigest,
    types::{
        ContentHash, EvmAddress, EvmBlockHash, EvmTransactionHash, EvmUint256, MarketId,
        PayoutRatio,
    },
};
use reqwest::{Client, Url};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::contracts::SettlementDeploymentCatalog;

const POLYGON_CHAIN_ID: u64 = 137;
const BINARY_OUTCOME_COUNT: usize = 2;
const MAX_PAYOUT_SCALE: u32 = 18;
const CONDITION_RESOLUTION_SIGNATURE: &str =
    "ConditionResolution(bytes32,address,bytes32,uint256,uint256[])";

sol! {
    #[sol(rpc)]
    interface ConditionalTokensResolutionView {
        function payoutDenominator(bytes32 conditionId) external view returns (uint256);
        function payoutNumerators(bytes32 conditionId, uint256 outcomeSlotIndex)
            external view returns (uint256);
    }
}

/// Exact binary CTF payout source and its decimal projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedResolutionVector {
    denominator: EvmUint256,
    numerators: [EvmUint256; BINARY_OUTCOME_COUNT],
    payout_ratios: [PayoutRatio; BINARY_OUTCOME_COUNT],
}

impl FinalizedResolutionVector {
    /// Parse raw CTF integers and reject any lossy decimal projection.
    pub fn try_from_decimal_parts(
        denominator: &str,
        numerators: [&str; BINARY_OUTCOME_COUNT],
    ) -> Result<Self, ResolutionSourceReadError> {
        let denominator = parse_u256("payout denominator", denominator)?;
        let yes = parse_u256("YES payout numerator", numerators[0])?;
        let no = parse_u256("NO payout numerator", numerators[1])?;
        Self::try_from_u256(denominator, [yes, no])
    }

    fn try_from_u256(
        denominator: U256,
        numerators: [U256; BINARY_OUTCOME_COUNT],
    ) -> Result<Self, ResolutionSourceReadError> {
        if denominator.is_zero() {
            return Err(ResolutionSourceReadError::ConditionNotResolved);
        }
        let total = numerators[0].checked_add(numerators[1]);
        if numerators.iter().any(|value| *value > denominator) || total != Some(denominator) {
            return Err(ResolutionSourceReadError::InvalidPayoutVector {
                denominator: denominator.to_string(),
                yes: numerators[0].to_string(),
                no: numerators[1].to_string(),
            });
        }
        let payout_ratios = [
            exact_payout_ratio(numerators[0], denominator)?,
            exact_payout_ratio(numerators[1], denominator)?,
        ];
        Ok(Self {
            denominator: typed_uint(denominator)?,
            numerators: [typed_uint(numerators[0])?, typed_uint(numerators[1])?],
            payout_ratios,
        })
    }

    #[must_use]
    pub const fn denominator(&self) -> &EvmUint256 {
        &self.denominator
    }

    #[must_use]
    pub const fn numerators(&self) -> &[EvmUint256; BINARY_OUTCOME_COUNT] {
        &self.numerators
    }

    #[must_use]
    pub const fn payout_ratios(&self) -> [PayoutRatio; BINARY_OUTCOME_COUNT] {
        self.payout_ratios
    }
}

/// One canonical finalized Polygon block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedResolutionBlock {
    pub block_number: u64,
    pub block_hash: EvmBlockHash,
    pub block_time: DateTime<Utc>,
}

/// One immutable CTF `ConditionResolution` observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedResolutionObservation {
    pub market_id: MarketId,
    pub vector: FinalizedResolutionVector,
    pub oracle: EvmAddress,
    pub question_id: String,
    pub transaction_hash: EvmTransactionHash,
    pub block_number: u64,
    pub block_hash: EvmBlockHash,
    pub log_index: u64,
    pub resolved_at: DateTime<Utc>,
    pub source_checkpoint_hash: ContentHash,
}

/// One inclusive finalized scan. Persist observations before advancing `to_block`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedResolutionScan {
    pub from_block: u64,
    pub to_block: u64,
    pub to_block_hash: EvmBlockHash,
    pub to_block_time: DateTime<Utc>,
    pub observations: Vec<FinalizedResolutionObservation>,
}

/// Mockable finalized-resolution read boundary.
#[async_trait]
pub trait ResolutionSourceReader: Send + Sync {
    async fn finalized_head(&self) -> Result<FinalizedResolutionBlock, ResolutionSourceReadError>;

    async fn block_at_or_before(
        &self,
        timestamp: DateTime<Utc>,
    ) -> Result<FinalizedResolutionBlock, ResolutionSourceReadError>;

    async fn scan_finalized(
        &self,
        from_block: u64,
        requested_to_block: u64,
    ) -> Result<Option<FinalizedResolutionScan>, ResolutionSourceReadError>;
}

/// Alloy-backed reader with no signer, wallet, approval, or submission method.
pub struct AlloyFinalizedResolutionReader {
    provider: DynProvider,
    conditional_tokens: EvmAddress,
    conditional_tokens_address: Address,
}

#[derive(Debug, Deserialize)]
struct RpcBlockObservation {
    #[serde(with = "alloy::serde::quantity")]
    number: u64,
    hash: B256,
    #[serde(with = "alloy::serde::quantity")]
    timestamp: u64,
}

impl AlloyFinalizedResolutionReader {
    /// Build a bounded read-only RPC client from the official CTF catalog.
    pub fn connect(config: &OnchainConfig) -> Result<Self, ResolutionSourceReadError> {
        let rpc_url = Url::parse(config.rpc_url()).map_err(|source| {
            ResolutionSourceReadError::InvalidConfiguration {
                detail: source.to_string(),
            }
        })?;
        let http = Client::builder()
            .timeout(Duration::from_millis(config.rpc_timeout_ms))
            .build()
            .map_err(|source| ResolutionSourceReadError::InvalidConfiguration {
                detail: source.to_string(),
            })?;
        let catalog = SettlementDeploymentCatalog::official_current().map_err(|source| {
            ResolutionSourceReadError::InvalidConfiguration {
                detail: source.to_string(),
            }
        })?;
        if catalog.chain_id != POLYGON_CHAIN_ID {
            return Err(ResolutionSourceReadError::InvalidConfiguration {
                detail: format!(
                    "official settlement catalog uses chain {}, expected {POLYGON_CHAIN_ID}",
                    catalog.chain_id
                ),
            });
        }
        let conditional_tokens_address = Address::from_str(catalog.conditional_tokens.as_str())
            .map_err(|source| ResolutionSourceReadError::InvalidConfiguration {
                detail: format!("official Conditional Tokens address is invalid: {source}"),
            })?;
        let client = RpcClient::new(Http::with_client(http, rpc_url), false);
        Ok(Self {
            provider: ProviderBuilder::new().connect_client(client).erased(),
            conditional_tokens: catalog.conditional_tokens,
            conditional_tokens_address,
        })
    }

    async fn ensure_polygon(&self) -> Result<(), ResolutionSourceReadError> {
        let actual = self
            .provider
            .get_chain_id()
            .await
            .map_err(|source| rpc_error("eth_chainId", &source))?;
        if actual != POLYGON_CHAIN_ID {
            return Err(ResolutionSourceReadError::WrongChain {
                expected: POLYGON_CHAIN_ID,
                actual,
            });
        }
        Ok(())
    }

    async fn block(
        &self,
        number: BlockNumberOrTag,
        operation: &'static str,
    ) -> Result<FinalizedResolutionBlock, ResolutionSourceReadError> {
        let block: Option<RpcBlockObservation> = self
            .provider
            .raw_request("eth_getBlockByNumber".into(), (number, false))
            .await
            .map_err(|source| rpc_error(operation, &source))?;
        let block = block.ok_or(ResolutionSourceReadError::MissingBlock { operation })?;
        let block_time = block_time(block.timestamp)?;
        Ok(FinalizedResolutionBlock {
            block_number: block.number,
            block_hash: typed_block_hash(block.hash)?,
            block_time,
        })
    }

    async fn verify_contract_state(
        &self,
        condition_id: B256,
        block_hash: B256,
        event_vector: &[U256; BINARY_OUTCOME_COUNT],
    ) -> Result<FinalizedResolutionVector, ResolutionSourceReadError> {
        let block = BlockId::hash_canonical(block_hash);
        let ctf =
            ConditionalTokensResolutionView::new(self.conditional_tokens_address, &self.provider);
        let denominator = ctf
            .payoutDenominator(condition_id)
            .block(block)
            .call()
            .await
            .map_err(|source| rpc_error("ctf.payoutDenominator", &source))?;
        let yes = ctf
            .payoutNumerators(condition_id, U256::ZERO)
            .block(block)
            .call()
            .await
            .map_err(|source| rpc_error("ctf.payoutNumerators(YES)", &source))?;
        let no = ctf
            .payoutNumerators(condition_id, U256::from(1))
            .block(block)
            .call()
            .await
            .map_err(|source| rpc_error("ctf.payoutNumerators(NO)", &source))?;
        if [yes, no] != *event_vector {
            return Err(ResolutionSourceReadError::StateEventMismatch {
                market_id: format!("{condition_id:#x}"),
            });
        }
        FinalizedResolutionVector::try_from_u256(denominator, [yes, no])
    }

    async fn locate_observations(
        &self,
        logs: Vec<Log>,
    ) -> Result<Vec<FinalizedResolutionObservation>, ResolutionSourceReadError> {
        let mut blocks: BTreeMap<u64, FinalizedResolutionBlock> = BTreeMap::new();
        let mut observations = Vec::with_capacity(logs.len());
        for log in logs {
            if log.removed {
                return Err(ResolutionSourceReadError::RemovedLog);
            }
            let block_number =
                log.block_number
                    .ok_or(ResolutionSourceReadError::MissingLogField {
                        field: "block_number",
                    })?;
            let block_hash = log
                .block_hash
                .ok_or(ResolutionSourceReadError::MissingLogField {
                    field: "block_hash",
                })?;
            let transaction_hash =
                log.transaction_hash
                    .ok_or(ResolutionSourceReadError::MissingLogField {
                        field: "transaction_hash",
                    })?;
            let log_index = log
                .log_index
                .ok_or(ResolutionSourceReadError::MissingLogField { field: "log_index" })?;
            let decoded = decode_condition_resolution(
                log.topics(),
                &Bytes::copy_from_slice(log.data().data.as_ref()),
            )?;
            let source_block = if let Some(block) = blocks.get(&block_number) {
                block.clone()
            } else {
                let block = self
                    .block(
                        BlockNumberOrTag::Number(block_number),
                        "eth_getBlockByNumber(resolution event)",
                    )
                    .await?;
                blocks.insert(block_number, block.clone());
                block
            };
            let typed_log_block_hash = typed_block_hash(block_hash)?;
            if source_block.block_hash != typed_log_block_hash {
                return Err(ResolutionSourceReadError::CanonicalHashChanged {
                    block: block_number,
                });
            }
            let vector = self
                .verify_contract_state(decoded.condition_id, block_hash, &decoded.numerators)
                .await?;
            let market_id = MarketId::new(format!("{:#x}", decoded.condition_id));
            let typed_transaction_hash = typed_transaction_hash(transaction_hash)?;
            let source_checkpoint_hash = resolution_checkpoint_hash(
                &self.conditional_tokens,
                &market_id,
                &decoded,
                &typed_transaction_hash,
                &source_block,
                log_index,
            )?;
            observations.push(FinalizedResolutionObservation {
                market_id,
                vector,
                oracle: typed_address(decoded.oracle)?,
                question_id: format!("{:#x}", decoded.question_id),
                transaction_hash: typed_transaction_hash,
                block_number,
                block_hash: source_block.block_hash,
                log_index,
                resolved_at: source_block.block_time,
                source_checkpoint_hash,
            });
        }
        observations.sort_by_key(|observation| (observation.block_number, observation.log_index));
        let mut resolved_markets = BTreeSet::new();
        for observation in &observations {
            if !resolved_markets.insert(observation.market_id.clone()) {
                return Err(ResolutionSourceReadError::DuplicateResolution {
                    market_id: observation.market_id.clone(),
                });
            }
        }
        Ok(observations)
    }
}

#[async_trait]
impl ResolutionSourceReader for AlloyFinalizedResolutionReader {
    async fn finalized_head(&self) -> Result<FinalizedResolutionBlock, ResolutionSourceReadError> {
        self.ensure_polygon().await?;
        self.block(
            BlockNumberOrTag::Finalized,
            "eth_getBlockByNumber(finalized)",
        )
        .await
    }

    async fn block_at_or_before(
        &self,
        timestamp: DateTime<Utc>,
    ) -> Result<FinalizedResolutionBlock, ResolutionSourceReadError> {
        let target = u64::try_from(timestamp.timestamp())
            .map_err(|_| ResolutionSourceReadError::TimestampBeforeGenesis { timestamp })?;
        let finalized = self.finalized_head().await?;
        if u64::try_from(finalized.block_time.timestamp()).ok() <= Some(target) {
            return Ok(finalized);
        }
        let genesis = self
            .block(BlockNumberOrTag::Number(0), "eth_getBlockByNumber(genesis)")
            .await?;
        if u64::try_from(genesis.block_time.timestamp()).ok() > Some(target) {
            return Err(ResolutionSourceReadError::TimestampBeforeGenesis { timestamp });
        }

        let mut low = genesis;
        let mut high = finalized.block_number;
        while low.block_number < high {
            let distance = high - low.block_number;
            let middle = low.block_number + distance.div_ceil(2);
            let candidate = self
                .block(
                    BlockNumberOrTag::Number(middle),
                    "eth_getBlockByNumber(timestamp search)",
                )
                .await?;
            let candidate_timestamp =
                u64::try_from(candidate.block_time.timestamp()).map_err(|_| {
                    ResolutionSourceReadError::InvalidBlockTime {
                        seconds: candidate.block_time.timestamp().to_string(),
                    }
                })?;
            if candidate_timestamp <= target {
                low = candidate;
            } else {
                high = middle - 1;
            }
        }
        Ok(low)
    }

    async fn scan_finalized(
        &self,
        from_block: u64,
        requested_to_block: u64,
    ) -> Result<Option<FinalizedResolutionScan>, ResolutionSourceReadError> {
        if requested_to_block < from_block {
            return Err(ResolutionSourceReadError::InvalidRange {
                from_block,
                to_block: requested_to_block,
            });
        }
        let finalized = self.finalized_head().await?;
        if from_block > finalized.block_number {
            return Ok(None);
        }
        let to_block = requested_to_block.min(finalized.block_number);
        let filter = Filter::new()
            .address(self.conditional_tokens_address)
            .event_signature(keccak256(CONDITION_RESOLUTION_SIGNATURE))
            .from_block(from_block)
            .to_block(to_block);
        let logs = self
            .provider
            .get_logs(&filter)
            .await
            .map_err(|source| rpc_error("eth_getLogs(ConditionResolution)", &source))?;
        let observations = self.locate_observations(logs).await?;
        let cursor = self
            .block(
                BlockNumberOrTag::Number(to_block),
                "eth_getBlockByNumber(cursor recheck)",
            )
            .await?;
        if to_block == finalized.block_number && cursor.block_hash != finalized.block_hash {
            return Err(ResolutionSourceReadError::CanonicalHashChanged { block: to_block });
        }
        Ok(Some(FinalizedResolutionScan {
            from_block,
            to_block,
            to_block_hash: cursor.block_hash,
            to_block_time: cursor.block_time,
            observations,
        }))
    }
}

#[derive(Debug)]
struct DecodedConditionResolution {
    condition_id: B256,
    oracle: Address,
    question_id: B256,
    numerators: [U256; BINARY_OUTCOME_COUNT],
}

fn decode_condition_resolution(
    topics: &[B256],
    data: &Bytes,
) -> Result<DecodedConditionResolution, ResolutionSourceReadError> {
    if topics.first() != Some(&keccak256(CONDITION_RESOLUTION_SIGNATURE)) {
        return Err(ResolutionSourceReadError::InvalidLog {
            detail: "unexpected event signature".to_owned(),
        });
    }
    if topics.len() != 4 || data.len() != 160 {
        return Err(ResolutionSourceReadError::InvalidLog {
            detail: "ConditionResolution requires four topics and exact binary payout ABI data"
                .to_owned(),
        });
    }
    if topics[2].as_slice()[..12].iter().any(|byte| *byte != 0) {
        return Err(ResolutionSourceReadError::InvalidLog {
            detail: "ConditionResolution oracle topic is not a canonical address".to_owned(),
        });
    }
    let outcome_slot_count = U256::from_be_slice(&data[..32]);
    let payout_offset = U256::from_be_slice(&data[32..64]);
    let payout_count = U256::from_be_slice(&data[64..96]);
    if outcome_slot_count != U256::from(BINARY_OUTCOME_COUNT)
        || payout_offset != U256::from(64)
        || payout_count != U256::from(BINARY_OUTCOME_COUNT)
    {
        return Err(ResolutionSourceReadError::UnsupportedOutcomeVector);
    }
    Ok(DecodedConditionResolution {
        condition_id: topics[1],
        oracle: Address::from_slice(&topics[2].as_slice()[12..]),
        question_id: topics[3],
        numerators: [
            U256::from_be_slice(&data[96..128]),
            U256::from_be_slice(&data[128..160]),
        ],
    })
}

#[derive(Serialize)]
struct ResolutionCheckpointHashInput<'a> {
    chain_id: u64,
    conditional_tokens: &'a EvmAddress,
    market_id: &'a MarketId,
    oracle: String,
    question_id: String,
    transaction_hash: &'a EvmTransactionHash,
    block_number: u64,
    block_hash: &'a EvmBlockHash,
    log_index: u64,
    payout_numerators: [String; BINARY_OUTCOME_COUNT],
}

fn resolution_checkpoint_hash(
    conditional_tokens: &EvmAddress,
    market_id: &MarketId,
    decoded: &DecodedConditionResolution,
    transaction_hash: &EvmTransactionHash,
    block: &FinalizedResolutionBlock,
    log_index: u64,
) -> Result<ContentHash, ResolutionSourceReadError> {
    let input = ResolutionCheckpointHashInput {
        chain_id: POLYGON_CHAIN_ID,
        conditional_tokens,
        market_id,
        oracle: format!("{:#x}", decoded.oracle),
        question_id: format!("{:#x}", decoded.question_id),
        transaction_hash,
        block_number: block.block_number,
        block_hash: &block.block_hash,
        log_index,
        payout_numerators: [
            decoded.numerators[0].to_string(),
            decoded.numerators[1].to_string(),
        ],
    };
    CanonicalDigest::content_hash_typed("quant-pivot/ctf-resolution-checkpoint", 1, &input)
        .map_err(Into::into)
}

fn exact_payout_ratio(
    numerator: U256,
    denominator: U256,
) -> Result<PayoutRatio, ResolutionSourceReadError> {
    if numerator.is_zero() {
        return Ok(PayoutRatio::ZERO);
    }
    let divisor = gcd(numerator, denominator);
    let reduced_numerator = numerator / divisor;
    let mut reduced_denominator = denominator / divisor;
    let mut twos = 0_u32;
    let mut fives = 0_u32;
    while reduced_denominator % U256::from(2) == U256::ZERO {
        reduced_denominator /= U256::from(2);
        twos += 1;
    }
    while reduced_denominator % U256::from(5) == U256::ZERO {
        reduced_denominator /= U256::from(5);
        fives += 1;
    }
    if reduced_denominator != U256::from(1) {
        return Err(ResolutionSourceReadError::NonTerminatingPayout {
            numerator: numerator.to_string(),
            denominator: denominator.to_string(),
        });
    }
    let scale = twos.max(fives);
    if scale > MAX_PAYOUT_SCALE {
        return Err(ResolutionSourceReadError::PayoutPrecisionExceeded {
            scale,
            maximum: MAX_PAYOUT_SCALE,
        });
    }
    let multiplier_two = checked_pow(U256::from(2), scale - twos)?;
    let multiplier_five = checked_pow(U256::from(5), scale - fives)?;
    let scaled = reduced_numerator
        .checked_mul(multiplier_two)
        .and_then(|value| value.checked_mul(multiplier_five))
        .ok_or_else(|| ResolutionSourceReadError::NumericEvidence {
            detail: "exact payout numerator overflow".to_owned(),
        })?;
    let decimal = decimal_from_scaled(scaled, scale)?;
    PayoutRatio::try_new(decimal).map_err(|source| ResolutionSourceReadError::NumericEvidence {
        detail: source.to_string(),
    })
}

fn gcd(mut left: U256, mut right: U256) -> U256 {
    while !right.is_zero() {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn checked_pow(mut base: U256, mut exponent: u32) -> Result<U256, ResolutionSourceReadError> {
    let mut value = U256::from(1);
    while exponent > 0 {
        if exponent % 2 == 1 {
            value = value.checked_mul(base).ok_or_else(|| {
                ResolutionSourceReadError::NumericEvidence {
                    detail: "exact payout scale multiplier overflow".to_owned(),
                }
            })?;
        }
        exponent /= 2;
        if exponent > 0 {
            base = base.checked_mul(base).ok_or_else(|| {
                ResolutionSourceReadError::NumericEvidence {
                    detail: "exact payout scale base overflow".to_owned(),
                }
            })?;
        }
    }
    Ok(value)
}

fn decimal_from_scaled(scaled: U256, scale: u32) -> Result<Decimal, ResolutionSourceReadError> {
    let digits = scaled.to_string();
    let scale =
        usize::try_from(scale).map_err(|source| ResolutionSourceReadError::NumericEvidence {
            detail: source.to_string(),
        })?;
    let decimal = if scale == 0 {
        digits
    } else if digits.len() <= scale {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    Decimal::from_str_exact(&decimal).map_err(|source| ResolutionSourceReadError::NumericEvidence {
        detail: source.to_string(),
    })
}

fn parse_u256(field: &'static str, value: &str) -> Result<U256, ResolutionSourceReadError> {
    U256::from_str(value).map_err(|_| ResolutionSourceReadError::InvalidUint256 {
        field,
        value: value.to_owned(),
    })
}

fn typed_uint(value: U256) -> Result<EvmUint256, ResolutionSourceReadError> {
    EvmUint256::parse(value.to_string()).map_err(|source| {
        ResolutionSourceReadError::NumericEvidence {
            detail: source.to_string(),
        }
    })
}

fn typed_address(value: Address) -> Result<EvmAddress, ResolutionSourceReadError> {
    EvmAddress::parse(format!("{value:#x}").to_lowercase()).map_err(|source| {
        ResolutionSourceReadError::InvalidLog {
            detail: source.to_string(),
        }
    })
}

fn typed_block_hash(value: B256) -> Result<EvmBlockHash, ResolutionSourceReadError> {
    EvmBlockHash::parse(format!("{value:#x}")).map_err(|source| {
        ResolutionSourceReadError::InvalidLog {
            detail: source.to_string(),
        }
    })
}

fn typed_transaction_hash(value: B256) -> Result<EvmTransactionHash, ResolutionSourceReadError> {
    EvmTransactionHash::parse(format!("{value:#x}")).map_err(|source| {
        ResolutionSourceReadError::InvalidLog {
            detail: source.to_string(),
        }
    })
}

fn block_time(seconds: u64) -> Result<DateTime<Utc>, ResolutionSourceReadError> {
    let seconds =
        i64::try_from(seconds).map_err(|source| ResolutionSourceReadError::InvalidBlockTime {
            seconds: source.to_string(),
        })?;
    DateTime::from_timestamp(seconds, 0).ok_or_else(|| {
        ResolutionSourceReadError::InvalidBlockTime {
            seconds: seconds.to_string(),
        }
    })
}

fn rpc_error(method: &'static str, source: &impl Display) -> ResolutionSourceReadError {
    ResolutionSourceReadError::Rpc {
        method,
        detail: source.to_string(),
    }
}

/// Closed source failures. None can be interpreted as a resolution label.
#[derive(Debug, thiserror::Error)]
pub enum ResolutionSourceReadError {
    #[error("invalid finalized resolution reader configuration: {detail}")]
    InvalidConfiguration { detail: String },
    #[error("invalid {field} uint256 value: {value}")]
    InvalidUint256 { field: &'static str, value: String },
    #[error("condition is not resolved because payout denominator is zero")]
    ConditionNotResolved,
    #[error("binary payout vector is invalid: denominator={denominator}, yes={yes}, no={no}")]
    InvalidPayoutVector {
        denominator: String,
        yes: String,
        no: String,
    },
    #[error("payout {numerator}/{denominator} has no finite decimal representation")]
    NonTerminatingPayout {
        numerator: String,
        denominator: String,
    },
    #[error("payout requires decimal scale {scale}, maximum is {maximum}")]
    PayoutPrecisionExceeded { scale: u32, maximum: u32 },
    #[error("resolution numeric evidence cannot be represented exactly: {detail}")]
    NumericEvidence { detail: String },
    #[error("resolution reader requires chain {expected}, observed {actual}")]
    WrongChain { expected: u64, actual: u64 },
    #[error("Polygon RPC call `{method}` failed: {detail}")]
    Rpc {
        method: &'static str,
        detail: String,
    },
    #[error("Polygon RPC omitted block for {operation}")]
    MissingBlock { operation: &'static str },
    #[error("invalid finalized scan range {from_block}..={to_block}")]
    InvalidRange { from_block: u64, to_block: u64 },
    #[error("timestamp {timestamp} is before Polygon genesis")]
    TimestampBeforeGenesis { timestamp: DateTime<Utc> },
    #[error("Polygon block has invalid timestamp {seconds}")]
    InvalidBlockTime { seconds: String },
    #[error("ConditionResolution log was marked removed")]
    RemovedLog,
    #[error("ConditionResolution log is missing `{field}`")]
    MissingLogField { field: &'static str },
    #[error("ConditionResolution log is invalid: {detail}")]
    InvalidLog { detail: String },
    #[error("only an exact binary ConditionResolution payout vector is supported")]
    UnsupportedOutcomeVector,
    #[error("finalized CTF state differs from ConditionResolution event for {market_id}")]
    StateEventMismatch { market_id: String },
    #[error("canonical block hash changed while reading block {block}")]
    CanonicalHashChanged { block: u64 },
    #[error("condition {market_id} emitted more than one immutable resolution")]
    DuplicateResolution { market_id: MarketId },
    #[error(transparent)]
    CanonicalDigest(#[from] CanonicalDigestError),
}

#[cfg(test)]
mod tests {
    use alloy::{
        primitives::{Address, B256, Bytes, U256, keccak256},
        sol_types::SolCall,
    };
    use quant_pivot_models::{
        config::{OnchainConfig, PolygonRpcEndpoint},
        types::PayoutRatio,
    };
    use rust_decimal::Decimal;
    use serde_json::{Value, json};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate, matchers::method};

    use super::{
        AlloyFinalizedResolutionReader, CONDITION_RESOLUTION_SIGNATURE,
        ConditionalTokensResolutionView::{payoutDenominatorCall, payoutNumeratorsCall},
        ResolutionSourceReadError, ResolutionSourceReader, decode_condition_resolution,
    };
    use crate::settlement::contracts::SettlementDeploymentCatalog;

    #[test]
    fn condition_resolution_decoder_requires_exact_binary_abi() {
        let condition_id = B256::repeat_byte(0x11);
        let oracle = Address::repeat_byte(0x22);
        let question_id = B256::repeat_byte(0x33);
        let topics = vec![
            keccak256(CONDITION_RESOLUTION_SIGNATURE),
            condition_id,
            address_topic(oracle),
            question_id,
        ];
        let data = condition_resolution_data([U256::from(1), U256::from(1)]);
        let decoded =
            decode_condition_resolution(&topics, &data).expect("canonical binary resolution");
        assert_eq!(decoded.condition_id, condition_id);
        assert_eq!(decoded.oracle, oracle);
        assert_eq!(decoded.question_id, question_id);
        assert_eq!(decoded.numerators, [U256::from(1), U256::from(1)]);

        let mut malformed = data.to_vec();
        malformed.truncate(128);
        assert!(matches!(
            decode_condition_resolution(&topics, &Bytes::from(malformed)),
            Err(ResolutionSourceReadError::InvalidLog { .. })
        ));
    }

    #[tokio::test]
    async fn finalized_scan_binds_event_state_and_canonical_checkpoint() {
        let (_server, reader) = test_reader(RpcScenario {
            state_yes: 1,
            state_no: 1,
        })
        .await;
        let first = reader
            .scan_finalized(100, 100)
            .await
            .expect("valid finalized source")
            .expect("range is finalized");
        let second = reader
            .scan_finalized(100, 100)
            .await
            .expect("idempotent finalized source")
            .expect("range remains finalized");
        assert_eq!(first, second);
        assert_eq!(first.to_block, 100);
        assert_eq!(first.observations.len(), 1);
        let observation = &first.observations[0];
        assert_eq!(
            observation.vector.payout_ratios(),
            [
                PayoutRatio::try_new(Decimal::from_str_exact("0.5").expect("fixture decimal"))
                    .expect("fixture payout"),
                PayoutRatio::try_new(Decimal::from_str_exact("0.5").expect("fixture decimal"))
                    .expect("fixture payout"),
            ]
        );
        assert_eq!(observation.block_number, 100);
        assert_eq!(observation.log_index, 3);
    }

    #[tokio::test]
    async fn finalized_scan_rejects_event_state_divergence() {
        let (_server, reader) = test_reader(RpcScenario {
            state_yes: 1,
            state_no: 0,
        })
        .await;
        assert!(matches!(
            reader.scan_finalized(100, 100).await,
            Err(ResolutionSourceReadError::StateEventMismatch { .. })
        ));
    }

    #[derive(Clone, Copy)]
    struct RpcScenario {
        state_yes: u64,
        state_no: u64,
    }

    async fn test_reader(scenario: RpcScenario) -> (MockServer, AlloyFinalizedResolutionReader) {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(move |request: &Request| rpc_response(request, scenario))
            .mount(&server)
            .await;
        let reader = AlloyFinalizedResolutionReader::connect(&OnchainConfig {
            rpc_endpoint: PolygonRpcEndpoint::Public { url: server.uri() },
            rpc_timeout_ms: 5_000,
        })
        .expect("test resolution reader");
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
            "eth_getBlockByNumber" => block_response(&body),
            "eth_getLogs" => resolution_log_response(),
            unexpected => panic!("unexpected JSON-RPC method: {unexpected}"),
        };
        ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        }))
    }

    fn block_response(body: &Value) -> Value {
        match body["params"][0].as_str().expect("block parameter") {
            "finalized" => json!({
                "number": "0x65",
                "hash": format!("{:#x}", B256::repeat_byte(0x65)),
                "timestamp": "0x6553f164"
            }),
            "0x64" => json!({
                "number": "0x64",
                "hash": format!("{:#x}", B256::repeat_byte(0x64)),
                "timestamp": "0x6553f100"
            }),
            unexpected => panic!("unexpected block parameter: {unexpected}"),
        }
    }

    fn resolution_log_response() -> Value {
        let catalog =
            SettlementDeploymentCatalog::official_current().expect("official test catalog");
        let oracle = Address::repeat_byte(0x22);
        let data = condition_resolution_data([U256::from(1), U256::from(1)]);
        json!([{
            "address": catalog.conditional_tokens.as_str(),
            "topics": [
                format!("{:#x}", keccak256(CONDITION_RESOLUTION_SIGNATURE)),
                format!("{:#x}", B256::repeat_byte(0x11)),
                format!("{:#x}", address_topic(oracle)),
                format!("{:#x}", B256::repeat_byte(0x33))
            ],
            "data": format!("0x{}", hex::encode(data)),
            "blockNumber": "0x64",
            "transactionHash": format!("{:#x}", B256::repeat_byte(0x44)),
            "transactionIndex": "0x0",
            "blockHash": format!("{:#x}", B256::repeat_byte(0x64)),
            "logIndex": "0x3",
            "removed": false
        }])
    }

    fn rpc_call_response(body: &Value, id: &Value, scenario: RpcScenario) -> ResponseTemplate {
        let call = &body["params"][0];
        let input = call
            .get("input")
            .or_else(|| call.get("data"))
            .and_then(Value::as_str)
            .expect("eth_call input");
        let block = &body["params"][1];
        assert_eq!(
            block["blockHash"],
            format!("{:#x}", B256::repeat_byte(0x64))
        );
        assert_eq!(block["requireCanonical"], true);
        let selector = &input[2..10];
        let result = if selector == call_selector::<payoutDenominatorCall>() {
            uint_result(scenario.state_yes + scenario.state_no)
        } else if selector == call_selector::<payoutNumeratorsCall>() {
            if input.ends_with(&format!("{:064x}", 0_u64)) {
                uint_result(scenario.state_yes)
            } else {
                uint_result(scenario.state_no)
            }
        } else {
            panic!("unexpected eth_call selector: {selector}");
        };
        ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        }))
    }

    fn call_selector<C: SolCall>() -> String {
        hex::encode(C::SELECTOR)
    }

    fn uint_result(value: u64) -> String {
        format!("0x{value:064x}")
    }

    fn address_topic(address: Address) -> B256 {
        let mut bytes = [0_u8; 32];
        bytes[12..].copy_from_slice(address.as_slice());
        B256::from(bytes)
    }

    fn condition_resolution_data(values: [U256; 2]) -> Bytes {
        let mut data = Vec::with_capacity(160);
        data.extend_from_slice(&U256::from(2).to_be_bytes::<32>());
        data.extend_from_slice(&U256::from(64).to_be_bytes::<32>());
        data.extend_from_slice(&U256::from(2).to_be_bytes::<32>());
        data.extend_from_slice(&values[0].to_be_bytes::<32>());
        data.extend_from_slice(&values[1].to_be_bytes::<32>());
        Bytes::from(data)
    }
}
