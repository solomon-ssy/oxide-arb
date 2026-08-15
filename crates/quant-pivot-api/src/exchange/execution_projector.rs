//! Deterministic projection from accepted exchange logs to economic executions.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use alloy::{
    primitives::{Address, U256},
    rpc::types::Log as RpcAlloyLog,
};
use blake3::Hasher;
use quant_pivot_models::{
    clickhouse::{
        ChAssetAmount, ChDigest, ChPrice, ChShares, ChUsd, ExchangeEventRow, ExchangeLogRawRow,
        ExchangeMatchRow, ExecutionParticipantRow, MarketExecutionRow,
    },
    enums::clickhouse::{
        ChAvailabilityBasis, ChExchangeEventKind, ChExchangeSide, ChExchangeVersion,
        ChExecutionParticipantRole,
    },
    types::{ContentHash, MarketId, Price, Shares, TokenId, Usd},
};
use rust_decimal::Decimal;
use uuid::Uuid;

use super::{
    CanonicalExchangeLog, EXCHANGE_CONTRACTS, ExchangeContract, ExchangeVersion, order_filled_v1,
    order_filled_v2, orders_matched_v1, orders_matched_v2, polygon_chain_id,
};

#[derive(Debug, thiserror::Error)]
pub enum ExecutionProjectionError {
    #[error("log address is outside the fixed exchange contract registry")]
    UnknownContract,
    #[error("exchange log is outside the contract's attested deployment interval")]
    ContractInterval,
    #[error("selected exchange log failed its declared event ABI")]
    DecodeFailure,
    #[error("OrdersMatched is missing its immediately preceding aggregate taker OrderFilled")]
    MissingAggregate,
    #[error("OrdersMatched aggregate fields disagree with OrderFilled")]
    AggregateMismatch,
    #[error("OrdersMatched has no maker-level OrderFilled executions")]
    MissingMaker,
    #[error(
        "exchange execution token {token_id} is absent from the complete Gamma identity catalog"
    )]
    UnknownToken { token_id: TokenId },
    #[error("exchange amount cannot be represented exactly as a six-decimal fact")]
    InvalidAmount,
    #[error("exchange execution has zero shares or non-positive notional")]
    ZeroExecution,
    #[error("exchange timestamp cannot be represented as DateTime64 milliseconds")]
    InvalidTimestamp,
    #[error("removed log cannot enter the accepted semantic projection")]
    RemovedLog,
}

#[derive(Debug, Clone)]
pub struct ExchangeHistoryProjection {
    pub raw_logs: Vec<ExchangeLogRawRow>,
    pub events: Vec<ExchangeEventRow>,
    pub matches: Vec<ExchangeMatchRow>,
    pub executions: Vec<MarketExecutionRow>,
    pub participants: Vec<ExecutionParticipantRow>,
}

/// Decode the accepted event stream and return every token identity that must
/// resolve before a chunk can advance its semantic watermark.
pub fn history_token_ids(
    logs: &[CanonicalExchangeLog],
) -> Result<BTreeSet<TokenId>, ExecutionProjectionError> {
    let context = ProjectionContext {
        observed_at: 0,
        policy_hash: ChDigest::new([0; 32]),
        chunk_id: Uuid::nil(),
    };
    let mut token_ids = BTreeSet::new();
    for log in logs {
        let contract = log.exchange_contract()?;
        let event = decode_event(log, contract, log.canonical_hash(), context)?;
        match event {
            DecodedEvent::Fill(fill) => {
                token_ids.insert(fill.token_id);
            }
            DecodedEvent::Matched(matched) => {
                token_ids.insert(matched.token_id);
            }
        }
    }
    Ok(token_ids)
}

pub fn project_history(
    logs: &[CanonicalExchangeLog],
    hypersync_observed_at: i64,
    attestor_observed_at: i64,
    availability_policy_hash: ContentHash,
    chunk_id: Uuid,
    market_for_token: impl Fn(&TokenId) -> Option<MarketId>,
) -> Result<ExchangeHistoryProjection, ExecutionProjectionError> {
    let observed_at = hypersync_observed_at.max(attestor_observed_at);
    let policy_hash = ChDigest::from(availability_policy_hash);
    let context = ProjectionContext {
        observed_at,
        policy_hash,
        chunk_id,
    };
    let observations = ProviderObservations {
        hypersync_at: hypersync_observed_at,
        attestor_at: attestor_observed_at,
    };
    let mut decoded = Vec::with_capacity(logs.len());
    let mut raw_logs = Vec::with_capacity(logs.len());
    for log in logs {
        if log.removed {
            return Err(ExecutionProjectionError::RemovedLog);
        }
        let contract = log.exchange_contract()?;
        let raw_log_hash = log.canonical_hash();
        raw_logs.push(raw_row(log, contract, raw_log_hash, observations, context)?);
        decoded.push(decode_event(log, contract, raw_log_hash, context)?);
    }

    let mut aggregate_ids = BTreeSet::new();
    let mut matched_fills = BTreeMap::new();
    let mut match_rows = Vec::new();
    let mut start = 0_usize;
    while start < decoded.len() {
        let transaction_hash = decoded[start].row().transaction_hash.clone();
        let contract_address = decoded[start].row().contract_address.clone();
        let mut end = start + 1;
        while end < decoded.len()
            && decoded[end].row().transaction_hash == transaction_hash
            && decoded[end].row().contract_address == contract_address
        {
            end += 1;
        }
        correlate_transaction(
            &decoded[start..end],
            &mut aggregate_ids,
            &mut matched_fills,
            &mut match_rows,
        )?;
        start = end;
    }

    let mut events = Vec::with_capacity(decoded.len());
    let mut executions = Vec::new();
    let mut participants = Vec::new();
    for event in decoded {
        let event_id = event.row().event_id;
        if let DecodedEvent::Fill(fill) = &event
            && !aggregate_ids.contains(&event_id)
        {
            let match_binding = matched_fills.get(&event_id);
            let execution = execution_from_fill(
                fill,
                match_binding,
                policy_hash,
                chunk_id,
                &market_for_token,
            )?;
            participants.extend(participants_for(&execution, policy_hash, chunk_id));
            executions.push(execution);
        }
        events.push(event.into());
    }
    Ok(ExchangeHistoryProjection {
        raw_logs,
        events,
        matches: match_rows,
        executions,
        participants,
    })
}

#[derive(Debug, Clone)]
enum DecodedEvent {
    Fill(DecodedFill),
    Matched(DecodedMatch),
}

impl DecodedEvent {
    const fn row(&self) -> &ExchangeEventRow {
        match self {
            Self::Fill(fill) => &fill.row,
            Self::Matched(matched) => &matched.row,
        }
    }
}

impl From<DecodedEvent> for ExchangeEventRow {
    fn from(value: DecodedEvent) -> Self {
        match value {
            DecodedEvent::Fill(fill) => fill.row,
            DecodedEvent::Matched(matched) => matched.row,
        }
    }
}

#[derive(Debug, Clone)]
struct DecodedFill {
    row: ExchangeEventRow,
    order_hash: String,
    maker: String,
    taker: String,
    side: ChExchangeSide,
    token_id: TokenId,
    collateral_raw: U256,
    shares_raw: U256,
    fee_raw: U256,
    fee_asset_id: String,
}

#[derive(Debug, Clone)]
struct DecodedMatch {
    row: ExchangeEventRow,
    order_hash: String,
    taker: String,
    side: ChExchangeSide,
    token_id: TokenId,
    maker_asset_id: Option<String>,
    taker_asset_id: Option<String>,
    maker_amount: U256,
    taker_amount: U256,
}

#[derive(Debug, Clone)]
struct MatchedFill {
    match_id: ChDigest,
    taker_address: String,
}

#[derive(Debug, Clone, Copy)]
struct ProjectionContext {
    observed_at: i64,
    policy_hash: ChDigest,
    chunk_id: Uuid,
}

#[derive(Debug, Clone, Copy)]
struct ProviderObservations {
    hypersync_at: i64,
    attestor_at: i64,
}

#[derive(Debug)]
struct EventFields {
    event_kind: ChExchangeEventKind,
    order_hash: String,
    maker: String,
    taker: Option<String>,
    side: ChExchangeSide,
    token_id: Option<String>,
    maker_asset_id: Option<String>,
    taker_asset_id: Option<String>,
    maker_amount: String,
    taker_amount: String,
    fee_amount: Option<String>,
    builder: Option<String>,
    metadata: Option<String>,
}

fn correlate_transaction(
    events: &[DecodedEvent],
    aggregate_ids: &mut BTreeSet<ChDigest>,
    matched_fills: &mut BTreeMap<ChDigest, MatchedFill>,
    match_rows: &mut Vec<ExchangeMatchRow>,
) -> Result<(), ExecutionProjectionError> {
    let mut maker_start = 0_usize;
    for (match_index, event) in events.iter().enumerate() {
        let DecodedEvent::Matched(match_event) = event else {
            continue;
        };
        let aggregate_index = match_index
            .checked_sub(1)
            .ok_or(ExecutionProjectionError::MissingAggregate)?;
        let DecodedEvent::Fill(aggregate) = &events[aggregate_index] else {
            return Err(ExecutionProjectionError::MissingAggregate);
        };
        validate_aggregate(aggregate, match_event)?;
        if aggregate_index == maker_start {
            return Err(ExecutionProjectionError::MissingMaker);
        }
        let match_id = digest_parts(
            b"quant-pivot/exchange-match/v1\0",
            &[
                match_event.row.transaction_hash.as_bytes(),
                match_event.order_hash.as_bytes(),
            ],
        );
        let mut maker_count = 0_u32;
        for maker_event in &events[maker_start..aggregate_index] {
            let DecodedEvent::Fill(fill) = maker_event else {
                return Err(ExecutionProjectionError::MissingMaker);
            };
            matched_fills.insert(
                fill.row.event_id,
                MatchedFill {
                    match_id,
                    taker_address: match_event.taker.clone(),
                },
            );
            maker_count = maker_count.saturating_add(1);
        }
        aggregate_ids.insert(aggregate.row.event_id);
        match_rows.push(ExchangeMatchRow {
            match_id,
            orders_matched_event_id: match_event.row.event_id,
            aggregate_taker_event_id: aggregate.row.event_id,
            contract_key: match_event.row.contract_key.clone(),
            exchange_version: match_event.row.exchange_version,
            transaction_hash: match_event.row.transaction_hash.clone(),
            block_number: match_event.row.block_number,
            block_timestamp: match_event.row.block_timestamp,
            taker_order_hash: match_event.order_hash.clone(),
            taker_address: match_event.taker.clone(),
            side: match_event.side,
            token_id: Some(match_event.token_id.as_str().to_owned()),
            maker_asset_id: match_event.maker_asset_id.clone(),
            taker_asset_id: match_event.taker_asset_id.clone(),
            maker_amount: match_event.maker_amount.to_string(),
            taker_amount: match_event.taker_amount.to_string(),
            maker_execution_count: maker_count,
            observed_at: match_event.row.observed_at,
            model_available_at: match_event.row.model_available_at,
            availability_policy_hash: match_event.row.availability_policy_hash,
            chunk_id: match_event.row.chunk_id,
            schema_version: ExchangeMatchRow::SCHEMA_VERSION,
        });
        maker_start = match_index.saturating_add(1);
    }
    Ok(())
}

fn validate_aggregate(
    fill: &DecodedFill,
    matched: &DecodedMatch,
) -> Result<(), ExecutionProjectionError> {
    let same_order = fill.order_hash == matched.order_hash;
    let same_taker = fill.maker == matched.taker;
    let common = same_order
        && same_taker
        && fill.side == matched.side
        && fill.token_id == matched.token_id
        && fill.row.maker_amount == matched.maker_amount.to_string()
        && fill.row.taker_amount == matched.taker_amount.to_string();
    let assets = fill.row.maker_asset_id == matched.maker_asset_id
        && fill.row.taker_asset_id == matched.taker_asset_id;
    if common && assets {
        Ok(())
    } else {
        Err(ExecutionProjectionError::AggregateMismatch)
    }
}

fn execution_from_fill(
    fill: &DecodedFill,
    matched: Option<&MatchedFill>,
    policy_hash: ChDigest,
    chunk_id: Uuid,
    market_for_token: &impl Fn(&TokenId) -> Option<MarketId>,
) -> Result<MarketExecutionRow, ExecutionProjectionError> {
    let collateral = raw_decimal(fill.collateral_raw)?;
    let shares = raw_decimal(fill.shares_raw)?;
    if collateral <= Decimal::ZERO || shares <= Decimal::ZERO {
        return Err(ExecutionProjectionError::ZeroExecution);
    }
    let market_id =
        market_for_token(&fill.token_id).ok_or_else(|| ExecutionProjectionError::UnknownToken {
            token_id: fill.token_id.clone(),
        })?;
    let price = Price::new((collateral / shares).round_dp(8));
    let taker_address =
        matched.map_or_else(|| fill.taker.clone(), |value| value.taker_address.clone());
    let execution_id = digest_parts(
        b"quant-pivot/market-execution/v1\0",
        &[fill.row.event_id.as_bytes(), taker_address.as_bytes()],
    );
    Ok(MarketExecutionRow {
        execution_id,
        match_id: matched.map(|value| value.match_id),
        maker_order_filled_event_id: fill.row.event_id,
        market_id,
        token_id: fill.token_id.clone(),
        contract_key: fill.row.contract_key.clone(),
        exchange_version: fill.row.exchange_version,
        transaction_hash: fill.row.transaction_hash.clone(),
        block_number: fill.row.block_number,
        transaction_index: fill.row.transaction_index,
        log_index: fill.row.log_index,
        maker_address: fill.maker.clone(),
        taker_address,
        side: fill.side,
        price: ChPrice::from(price),
        size_shares: ChShares::from(Shares::new(shares)),
        notional_usd: ChUsd::from(Usd::new(collateral)),
        fee_amount: ChAssetAmount::from(raw_decimal(fill.fee_raw)?),
        fee_asset_id: fill.fee_asset_id.clone(),
        effective_at: fill.row.block_timestamp,
        observed_at: fill.row.observed_at,
        model_available_at: fill.row.model_available_at,
        availability_basis: ChAvailabilityBasis::BlockConfirmation,
        availability_policy_hash: policy_hash,
        chunk_id,
        schema_version: MarketExecutionRow::SCHEMA_VERSION,
    })
}

fn participants_for(
    execution: &MarketExecutionRow,
    policy_hash: ChDigest,
    chunk_id: Uuid,
) -> [ExecutionParticipantRow; 2] {
    let build = |address: &str, role| ExecutionParticipantRow {
        execution_id: execution.execution_id,
        market_id: execution.market_id.clone(),
        token_id: execution.token_id.clone(),
        participant_address: address.to_owned(),
        participant_role: role,
        participant_notional: execution.notional_usd,
        effective_at: execution.effective_at,
        model_available_at: execution.model_available_at,
        availability_policy_hash: policy_hash,
        chunk_id,
        schema_version: ExecutionParticipantRow::SCHEMA_VERSION,
    };
    [
        build(&execution.maker_address, ChExecutionParticipantRole::Maker),
        build(&execution.taker_address, ChExecutionParticipantRole::Taker),
    ]
}

fn decode_event(
    log: &CanonicalExchangeLog,
    contract: ExchangeContract,
    raw_log_hash: ChDigest,
    context: ProjectionContext,
) -> Result<DecodedEvent, ExecutionProjectionError> {
    let rpc_log = log
        .alloy_log()
        .map_err(|_| ExecutionProjectionError::DecodeFailure)?;
    let topic = rpc_log
        .topic0()
        .copied()
        .ok_or(ExecutionProjectionError::DecodeFailure)?;
    match contract.version {
        ExchangeVersion::V1 if topic == contract.order_filled_topic => {
            let decoded = order_filled_v1::decode_log(&rpc_log)
                .ok_or(ExecutionProjectionError::DecodeFailure)?;
            let is_buy = decoded.maker_asset_id.is_zero();
            let token_raw = if is_buy {
                decoded.taker_asset_id
            } else {
                decoded.maker_asset_id
            };
            let side = if is_buy {
                ChExchangeSide::Buy
            } else {
                ChExchangeSide::Sell
            };
            let token_id = TokenId::new(token_raw.to_string());
            let row = event_row(
                log,
                contract,
                raw_log_hash,
                context,
                EventFields {
                    event_kind: ChExchangeEventKind::OrderFilled,
                    order_hash: format!("{:#x}", decoded.order_hash),
                    maker: format!("{:#x}", decoded.maker),
                    taker: Some(format!("{:#x}", decoded.taker)),
                    side,
                    token_id: Some(token_id.as_str().to_owned()),
                    maker_asset_id: Some(decoded.maker_asset_id.to_string()),
                    taker_asset_id: Some(decoded.taker_asset_id.to_string()),
                    maker_amount: decoded.maker_amount_filled.to_string(),
                    taker_amount: decoded.taker_amount_filled.to_string(),
                    fee_amount: Some(decoded.fee.to_string()),
                    builder: None,
                    metadata: None,
                },
            )?;
            Ok(DecodedEvent::Fill(DecodedFill {
                row,
                order_hash: format!("{:#x}", decoded.order_hash),
                maker: address_text(decoded.maker),
                taker: address_text(decoded.taker),
                side,
                token_id,
                collateral_raw: if is_buy {
                    decoded.maker_amount_filled
                } else {
                    decoded.taker_amount_filled
                },
                shares_raw: if is_buy {
                    decoded.taker_amount_filled
                } else {
                    decoded.maker_amount_filled
                },
                fee_raw: decoded.fee,
                fee_asset_id: decoded.taker_asset_id.to_string(),
            }))
        }
        ExchangeVersion::V2 if topic == contract.order_filled_topic => {
            let decoded = order_filled_v2::decode_log(&rpc_log)
                .ok_or(ExecutionProjectionError::DecodeFailure)?;
            let is_buy = decoded.side == 0;
            let side = if is_buy {
                ChExchangeSide::Buy
            } else {
                ChExchangeSide::Sell
            };
            let token_id = TokenId::new(decoded.token_id.to_string());
            let row = event_row(
                log,
                contract,
                raw_log_hash,
                context,
                EventFields {
                    event_kind: ChExchangeEventKind::OrderFilled,
                    order_hash: format!("{:#x}", decoded.order_hash),
                    maker: format!("{:#x}", decoded.maker),
                    taker: Some(format!("{:#x}", decoded.taker)),
                    side,
                    token_id: Some(token_id.as_str().to_owned()),
                    maker_asset_id: None,
                    taker_asset_id: None,
                    maker_amount: decoded.maker_amount_filled.to_string(),
                    taker_amount: decoded.taker_amount_filled.to_string(),
                    fee_amount: Some(decoded.fee.to_string()),
                    builder: Some(format!("{:#x}", decoded.builder)),
                    metadata: Some(format!("{:#x}", decoded.metadata)),
                },
            )?;
            Ok(DecodedEvent::Fill(DecodedFill {
                row,
                order_hash: format!("{:#x}", decoded.order_hash),
                maker: address_text(decoded.maker),
                taker: address_text(decoded.taker),
                side,
                token_id: token_id.clone(),
                collateral_raw: if is_buy {
                    decoded.maker_amount_filled
                } else {
                    decoded.taker_amount_filled
                },
                shares_raw: if is_buy {
                    decoded.taker_amount_filled
                } else {
                    decoded.maker_amount_filled
                },
                fee_raw: decoded.fee,
                fee_asset_id: if is_buy {
                    token_id.as_str().to_owned()
                } else {
                    "0".to_owned()
                },
            }))
        }
        ExchangeVersion::V1 if topic == contract.orders_matched_topic => {
            decode_matched_v1(log, &rpc_log, contract, raw_log_hash, context)
        }
        ExchangeVersion::V2 if topic == contract.orders_matched_topic => {
            decode_matched_v2(log, &rpc_log, contract, raw_log_hash, context)
        }
        _ => Err(ExecutionProjectionError::DecodeFailure),
    }
}

fn decode_matched_v1(
    log: &CanonicalExchangeLog,
    rpc_log: &RpcAlloyLog,
    contract: ExchangeContract,
    raw_log_hash: ChDigest,
    context: ProjectionContext,
) -> Result<DecodedEvent, ExecutionProjectionError> {
    let decoded =
        orders_matched_v1::decode_log(rpc_log).ok_or(ExecutionProjectionError::DecodeFailure)?;
    let is_buy = decoded.maker_asset_id.is_zero();
    let side = if is_buy {
        ChExchangeSide::Buy
    } else {
        ChExchangeSide::Sell
    };
    let token_raw = if is_buy {
        decoded.taker_asset_id
    } else {
        decoded.maker_asset_id
    };
    let token_id = TokenId::new(token_raw.to_string());
    let maker_asset_id = Some(decoded.maker_asset_id.to_string());
    let taker_asset_id = Some(decoded.taker_asset_id.to_string());
    let order_hash = format!("{:#x}", decoded.taker_order_hash);
    let taker = address_text(decoded.taker_order_maker);
    let row = event_row(
        log,
        contract,
        raw_log_hash,
        context,
        EventFields {
            event_kind: ChExchangeEventKind::OrdersMatched,
            order_hash: order_hash.clone(),
            maker: taker.clone(),
            taker: None,
            side,
            token_id: Some(token_id.as_str().to_owned()),
            maker_asset_id: maker_asset_id.clone(),
            taker_asset_id: taker_asset_id.clone(),
            maker_amount: decoded.maker_amount_filled.to_string(),
            taker_amount: decoded.taker_amount_filled.to_string(),
            fee_amount: None,
            builder: None,
            metadata: None,
        },
    )?;
    Ok(DecodedEvent::Matched(DecodedMatch {
        row,
        order_hash,
        taker,
        side,
        token_id,
        maker_asset_id,
        taker_asset_id,
        maker_amount: decoded.maker_amount_filled,
        taker_amount: decoded.taker_amount_filled,
    }))
}

fn decode_matched_v2(
    log: &CanonicalExchangeLog,
    rpc_log: &RpcAlloyLog,
    contract: ExchangeContract,
    raw_log_hash: ChDigest,
    context: ProjectionContext,
) -> Result<DecodedEvent, ExecutionProjectionError> {
    let decoded =
        orders_matched_v2::decode_log(rpc_log).ok_or(ExecutionProjectionError::DecodeFailure)?;
    let side = if decoded.side == 0 {
        ChExchangeSide::Buy
    } else {
        ChExchangeSide::Sell
    };
    let token_id = TokenId::new(decoded.token_id.to_string());
    let order_hash = format!("{:#x}", decoded.taker_order_hash);
    let taker = address_text(decoded.taker_order_maker);
    let row = event_row(
        log,
        contract,
        raw_log_hash,
        context,
        EventFields {
            event_kind: ChExchangeEventKind::OrdersMatched,
            order_hash: order_hash.clone(),
            maker: taker.clone(),
            taker: None,
            side,
            token_id: Some(token_id.as_str().to_owned()),
            maker_asset_id: None,
            taker_asset_id: None,
            maker_amount: decoded.maker_amount_filled.to_string(),
            taker_amount: decoded.taker_amount_filled.to_string(),
            fee_amount: None,
            builder: None,
            metadata: None,
        },
    )?;
    Ok(DecodedEvent::Matched(DecodedMatch {
        row,
        order_hash,
        taker,
        side,
        token_id,
        maker_asset_id: None,
        taker_asset_id: None,
        maker_amount: decoded.maker_amount_filled,
        taker_amount: decoded.taker_amount_filled,
    }))
}

fn event_row(
    log: &CanonicalExchangeLog,
    contract: ExchangeContract,
    raw_log_hash: ChDigest,
    context: ProjectionContext,
    fields: EventFields,
) -> Result<ExchangeEventRow, ExecutionProjectionError> {
    let event_id = digest_parts(
        b"quant-pivot/exchange-event/v1\0",
        &[
            raw_log_hash.as_bytes(),
            &[(fields.event_kind as i8).cast_unsigned()],
        ],
    );
    Ok(ExchangeEventRow {
        event_id,
        raw_log_hash,
        event_kind: fields.event_kind,
        contract_key: contract.key.to_owned(),
        exchange_version: contract.version.into(),
        contract_address: log.address.clone(),
        block_number: log.block_number,
        block_hash: log.block_hash.clone(),
        block_timestamp: millis(log.block_timestamp)?,
        transaction_hash: log.transaction_hash.clone(),
        transaction_index: log.transaction_index,
        log_index: log.log_index,
        order_hash: fields.order_hash,
        maker: fields.maker,
        taker: fields.taker,
        side: fields.side,
        token_id: fields.token_id,
        maker_asset_id: fields.maker_asset_id,
        taker_asset_id: fields.taker_asset_id,
        maker_amount: fields.maker_amount,
        taker_amount: fields.taker_amount,
        fee_amount: fields.fee_amount,
        builder: fields.builder,
        metadata: fields.metadata,
        observed_at: context.observed_at,
        model_available_at: millis(log.model_available_timestamp)?,
        availability_policy_hash: context.policy_hash,
        chunk_id: context.chunk_id,
        schema_version: ExchangeEventRow::SCHEMA_VERSION,
    })
}

fn raw_row(
    log: &CanonicalExchangeLog,
    contract: ExchangeContract,
    raw_log_hash: ChDigest,
    observations: ProviderObservations,
    context: ProjectionContext,
) -> Result<ExchangeLogRawRow, ExecutionProjectionError> {
    Ok(ExchangeLogRawRow {
        chain_id: polygon_chain_id(),
        contract_key: contract.key.to_owned(),
        exchange_version: contract.version.into(),
        contract_address: log.address.clone(),
        block_number: log.block_number,
        block_hash: log.block_hash.clone(),
        parent_block_hash: log.parent_block_hash.clone(),
        block_timestamp: millis(log.block_timestamp)?,
        transaction_hash: log.transaction_hash.clone(),
        transaction_index: log.transaction_index,
        log_index: log.log_index,
        topic0: log
            .topics
            .first()
            .cloned()
            .ok_or(ExecutionProjectionError::DecodeFailure)?,
        topic1: log.topics.get(1).cloned(),
        topic2: log.topics.get(2).cloned(),
        topic3: log.topics.get(3).cloned(),
        data: log.data.clone(),
        removed: log.removed,
        hypersync_observed_at: observations.hypersync_at,
        attestor_observed_at: observations.attestor_at,
        observed_at: context.observed_at,
        model_available_at: millis(log.model_available_timestamp)?,
        availability_basis: ChAvailabilityBasis::BlockConfirmation,
        availability_policy_hash: context.policy_hash,
        chunk_id: context.chunk_id,
        raw_log_hash,
        schema_version: ExchangeLogRawRow::SCHEMA_VERSION,
    })
}

impl CanonicalExchangeLog {
    fn exchange_contract(&self) -> Result<ExchangeContract, ExecutionProjectionError> {
        let address = self
            .address
            .parse::<Address>()
            .map_err(|_| ExecutionProjectionError::UnknownContract)?;
        let contract = EXCHANGE_CONTRACTS
            .into_iter()
            .find(|contract| contract.address == address)
            .ok_or(ExecutionProjectionError::UnknownContract)?;
        let within_end = contract
            .last_valid_block
            .is_none_or(|last| self.block_number <= last);
        if self.block_number < contract.first_valid_block || !within_end {
            return Err(ExecutionProjectionError::ContractInterval);
        }
        Ok(contract)
    }

    fn canonical_hash(&self) -> ChDigest {
        digest_parts(
            b"quant-pivot/exchange-raw-log/v1\0",
            &[
                self.address.as_bytes(),
                &self.block_number.to_be_bytes(),
                self.block_hash.as_bytes(),
                self.transaction_hash.as_bytes(),
                &self.transaction_index.to_be_bytes(),
                &self.log_index.to_be_bytes(),
                self.data.as_bytes(),
            ],
        )
    }
}

fn digest_parts(domain: &[u8], parts: &[&[u8]]) -> ChDigest {
    let mut hasher = Hasher::new();
    hasher.update(domain);
    for part in parts {
        hasher.update(&u64::try_from(part.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(part);
    }
    ChDigest::new(*hasher.finalize().as_bytes())
}

fn raw_decimal(value: U256) -> Result<Decimal, ExecutionProjectionError> {
    let raw = Decimal::from_str(&value.to_string())
        .map_err(|_| ExecutionProjectionError::InvalidAmount)?;
    Ok((raw / Decimal::from(1_000_000_u64)).round_dp(6))
}

fn millis(timestamp: u64) -> Result<i64, ExecutionProjectionError> {
    i64::try_from(timestamp)
        .ok()
        .and_then(|value| value.checked_mul(1_000))
        .ok_or(ExecutionProjectionError::InvalidTimestamp)
}

fn address_text(address: Address) -> String {
    format!("{address:#x}")
}

impl From<ExchangeVersion> for ChExchangeVersion {
    fn from(value: ExchangeVersion) -> Self {
        match value {
            ExchangeVersion::V1 => Self::V1,
            ExchangeVersion::V2 => Self::V2,
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::{
        primitives::{Address, B256, LogData, U256, address, b256, keccak256},
        sol_types::SolEvent,
    };
    use quant_pivot_models::types::{ContentHash, MarketId, Price, TokenId, Usd};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    use super::{
        CanonicalExchangeLog, ExchangeHistoryProjection, ExecutionProjectionError, project_history,
    };
    use crate::exchange::{
        constants::{CTF_EXCHANGE_V1, CTF_EXCHANGE_V2},
        order_filled_v1::ORDER_FILLED_TOPIC as ORDER_FILLED_V1_TOPIC,
        order_filled_v2::ORDER_FILLED_TOPIC as ORDER_FILLED_V2_TOPIC,
        orders_matched_v1::ORDERS_MATCHED_TOPIC as ORDERS_MATCHED_V1_TOPIC,
        orders_matched_v2::ORDERS_MATCHED_TOPIC as ORDERS_MATCHED_V2_TOPIC,
    };

    mod v1_events {
        use alloy::sol;

        sol! {
            event OrderFilled(
                bytes32 indexed orderHash,
                address indexed maker,
                address indexed taker,
                uint256 makerAssetId,
                uint256 takerAssetId,
                uint256 makerAmountFilled,
                uint256 takerAmountFilled,
                uint256 fee
            );

            event OrdersMatched(
                bytes32 indexed takerOrderHash,
                address indexed takerOrderMaker,
                uint256 makerAssetId,
                uint256 takerAssetId,
                uint256 makerAmountFilled,
                uint256 takerAmountFilled
            );
        }
    }

    mod v2_events {
        use alloy::sol;

        sol! {
            event OrderFilled(
                bytes32 indexed orderHash,
                address indexed maker,
                address indexed taker,
                uint8 side,
                uint256 tokenId,
                uint256 makerAmountFilled,
                uint256 takerAmountFilled,
                uint256 fee,
                bytes32 builder,
                bytes32 metadata
            );

            event OrdersMatched(
                bytes32 indexed takerOrderHash,
                address indexed takerOrderMaker,
                uint8 side,
                uint256 tokenId,
                uint256 makerAmountFilled,
                uint256 takerAmountFilled
            );
        }
    }

    use v1_events::{OrderFilled as OrderFilledV1, OrdersMatched as OrdersMatchedV1};
    use v2_events::{OrderFilled as OrderFilledV2, OrdersMatched as OrdersMatchedV2};

    const MAKER_ONE: Address = address!("0x1000000000000000000000000000000000000001");
    const MAKER_TWO: Address = address!("0x2000000000000000000000000000000000000002");
    const TAKER: Address = address!("0x3000000000000000000000000000000000000003");
    const EXCHANGE: Address = address!("0xE111180000d2663C0091e4f400237545B87B996B");
    const MAKER_HASH_ONE: B256 =
        b256!("0101010101010101010101010101010101010101010101010101010101010101");
    const MAKER_HASH_TWO: B256 =
        b256!("0202020202020202020202020202020202020202020202020202020202020202");
    const TAKER_HASH: B256 =
        b256!("0303030303030303030303030303030303030303030303030303030303030303");

    #[test]
    fn canonical_topics_match() {
        assert_eq!(
            ORDER_FILLED_V1_TOPIC,
            keccak256(
                "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
            )
        );
        assert_eq!(
            ORDERS_MATCHED_V1_TOPIC,
            keccak256("OrdersMatched(bytes32,address,uint256,uint256,uint256,uint256)")
        );
        assert_eq!(
            ORDER_FILLED_V2_TOPIC,
            keccak256(
                "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"
            )
        );
        assert_eq!(
            ORDERS_MATCHED_V2_TOPIC,
            keccak256("OrdersMatched(bytes32,address,uint8,uint256,uint256,uint256)")
        );
    }

    #[test]
    fn v2_excludes_aggregate() {
        let token = U256::from(42_u64);
        let logs = vec![
            canonical(
                CTF_EXCHANGE_V2.address,
                0,
                OrderFilledV2 {
                    orderHash: MAKER_HASH_ONE,
                    maker: MAKER_ONE,
                    taker: TAKER,
                    side: 1,
                    tokenId: token,
                    makerAmountFilled: U256::from(60_000_000_u64),
                    takerAmountFilled: U256::from(30_000_000_u64),
                    fee: U256::ZERO,
                    builder: B256::ZERO,
                    metadata: B256::ZERO,
                }
                .encode_log_data(),
            ),
            canonical(
                CTF_EXCHANGE_V2.address,
                1,
                OrderFilledV2 {
                    orderHash: MAKER_HASH_TWO,
                    maker: MAKER_TWO,
                    taker: TAKER,
                    side: 1,
                    tokenId: token,
                    makerAmountFilled: U256::from(140_000_000_u64),
                    takerAmountFilled: U256::from(70_000_000_u64),
                    fee: U256::ZERO,
                    builder: B256::ZERO,
                    metadata: B256::ZERO,
                }
                .encode_log_data(),
            ),
            canonical(
                CTF_EXCHANGE_V2.address,
                2,
                OrderFilledV2 {
                    orderHash: TAKER_HASH,
                    maker: TAKER,
                    taker: EXCHANGE,
                    side: 0,
                    tokenId: token,
                    makerAmountFilled: U256::from(100_000_000_u64),
                    takerAmountFilled: U256::from(200_000_000_u64),
                    fee: U256::from(1_000_000_u64),
                    builder: B256::ZERO,
                    metadata: B256::ZERO,
                }
                .encode_log_data(),
            ),
            canonical(
                CTF_EXCHANGE_V2.address,
                3,
                OrdersMatchedV2 {
                    takerOrderHash: TAKER_HASH,
                    takerOrderMaker: TAKER,
                    side: 0,
                    tokenId: token,
                    makerAmountFilled: U256::from(100_000_000_u64),
                    takerAmountFilled: U256::from(200_000_000_u64),
                }
                .encode_log_data(),
            ),
        ];

        let projection = project(&logs).expect("valid V2 projection");
        assert_eq!(projection.events.len(), 4);
        assert_eq!(projection.matches.len(), 1);
        assert_eq!(projection.executions.len(), 2);
        assert_eq!(projection.participants.len(), 4);
        assert_eq!(
            projection
                .executions
                .iter()
                .map(|execution| Usd::from(execution.notional_usd).inner())
                .sum::<Decimal>(),
            Decimal::from(100_u64)
        );
        assert_eq!(
            projection
                .participants
                .iter()
                .map(|participant| Usd::from(participant.participant_notional).inner())
                .sum::<Decimal>(),
            Decimal::from(200_u64)
        );
        assert!(
            projection
                .executions
                .iter()
                .all(|execution| execution.taker_address == format!("{TAKER:#x}"))
        );
    }

    #[test]
    fn v1_projects_execution() {
        let token = U256::from(84_u64);
        let logs = vec![
            canonical(
                CTF_EXCHANGE_V1.address,
                0,
                OrderFilledV1 {
                    orderHash: MAKER_HASH_ONE,
                    maker: MAKER_ONE,
                    taker: TAKER,
                    makerAssetId: token,
                    takerAssetId: U256::ZERO,
                    makerAmountFilled: U256::from(20_000_000_u64),
                    takerAmountFilled: U256::from(5_000_000_u64),
                    fee: U256::from(100_000_u64),
                }
                .encode_log_data(),
            ),
            canonical(
                CTF_EXCHANGE_V1.address,
                1,
                OrderFilledV1 {
                    orderHash: TAKER_HASH,
                    maker: TAKER,
                    taker: EXCHANGE,
                    makerAssetId: U256::ZERO,
                    takerAssetId: token,
                    makerAmountFilled: U256::from(5_000_000_u64),
                    takerAmountFilled: U256::from(20_000_000_u64),
                    fee: U256::ZERO,
                }
                .encode_log_data(),
            ),
            canonical(
                CTF_EXCHANGE_V1.address,
                2,
                OrdersMatchedV1 {
                    takerOrderHash: TAKER_HASH,
                    takerOrderMaker: TAKER,
                    makerAssetId: U256::ZERO,
                    takerAssetId: token,
                    makerAmountFilled: U256::from(5_000_000_u64),
                    takerAmountFilled: U256::from(20_000_000_u64),
                }
                .encode_log_data(),
            ),
        ];

        let projection = project(&logs).expect("valid V1 projection");
        assert_eq!(projection.executions.len(), 1);
        assert_eq!(projection.participants.len(), 2);
        assert_eq!(
            Usd::from(projection.executions[0].notional_usd).inner(),
            Decimal::from(5_u64)
        );
        assert_eq!(
            Price::from(projection.executions[0].price).inner(),
            Decimal::new(25, 2)
        );
    }

    #[test]
    fn mismatch_blocks_projection() {
        let token = U256::from(42_u64);
        let logs = vec![
            canonical(
                CTF_EXCHANGE_V2.address,
                0,
                OrderFilledV2 {
                    orderHash: MAKER_HASH_ONE,
                    maker: MAKER_ONE,
                    taker: TAKER,
                    side: 1,
                    tokenId: token,
                    makerAmountFilled: U256::from(20_000_000_u64),
                    takerAmountFilled: U256::from(10_000_000_u64),
                    fee: U256::ZERO,
                    builder: B256::ZERO,
                    metadata: B256::ZERO,
                }
                .encode_log_data(),
            ),
            canonical(
                CTF_EXCHANGE_V2.address,
                1,
                OrderFilledV2 {
                    orderHash: TAKER_HASH,
                    maker: TAKER,
                    taker: EXCHANGE,
                    side: 0,
                    tokenId: token,
                    makerAmountFilled: U256::from(10_000_000_u64),
                    takerAmountFilled: U256::from(20_000_000_u64),
                    fee: U256::ZERO,
                    builder: B256::ZERO,
                    metadata: B256::ZERO,
                }
                .encode_log_data(),
            ),
            canonical(
                CTF_EXCHANGE_V2.address,
                2,
                OrdersMatchedV2 {
                    takerOrderHash: TAKER_HASH,
                    takerOrderMaker: TAKER,
                    side: 0,
                    tokenId: token,
                    makerAmountFilled: U256::from(11_000_000_u64),
                    takerAmountFilled: U256::from(20_000_000_u64),
                }
                .encode_log_data(),
            ),
        ];

        assert!(matches!(
            project(&logs),
            Err(ExecutionProjectionError::AggregateMismatch)
        ));
    }

    fn project(
        logs: &[CanonicalExchangeLog],
    ) -> Result<ExchangeHistoryProjection, ExecutionProjectionError> {
        project_history(
            logs,
            1_800_000_000_000,
            1_800_000_000_001,
            ContentHash::from_bytes([9; 32]),
            Uuid::from_u128(7),
            |token| {
                (token == &TokenId::new("42") || token == &TokenId::new("84"))
                    .then(|| MarketId::new("market"))
            },
        )
    }

    fn canonical(address: Address, log_index: u64, encoded: LogData) -> CanonicalExchangeLog {
        let (topics, data) = encoded.split();
        CanonicalExchangeLog {
            address: format!("{address:#x}"),
            block_number: if address == CTF_EXCHANGE_V1.address {
                CTF_EXCHANGE_V1.first_valid_block
            } else {
                CTF_EXCHANGE_V2.first_valid_block
            },
            block_hash: format!(
                "{:#x}",
                b256!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            ),
            block_timestamp: 1_800_000_000,
            model_available_timestamp: 1_800_000_024,
            parent_block_hash: format!(
                "{:#x}",
                b256!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
            ),
            transaction_hash: format!(
                "{:#x}",
                b256!("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
            ),
            transaction_index: 4,
            log_index,
            topics: topics
                .into_iter()
                .map(|topic| format!("{topic:#x}"))
                .collect(),
            data: format!("{data:#x}"),
            removed: false,
        }
    }
}
