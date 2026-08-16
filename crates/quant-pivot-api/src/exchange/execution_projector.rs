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
        ChDigest, ChPrice, ChShares, ChUsd, ExchangeEventRow, ExchangeFeeChargeRow,
        ExchangeLogRawRow, ExchangeMatchRow, ExecutionParticipantRow, MarketExecutionRow,
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
    constants::{EXCHANGE_CONTRACTS, ExchangeContract},
    fee_charged_v2,
    history_client::{CanonicalExchangeLog, polygon_chain_id},
    order_filled_v2, orders_matched_v2,
};

#[derive(Debug, thiserror::Error)]
pub enum ExecutionProjectionError {
    #[error("log address is outside the fixed exchange contract registry")]
    UnknownContract,
    #[error("exchange log is outside the contract's attested deployment interval")]
    ContractInterval,
    #[error("selected exchange log failed its declared event ABI")]
    DecodeFailure,
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
    #[error(
        "invalid {version} exchange transaction grammar for {contract} transaction {transaction_hash} at log {log_index}: expected {expected}, got {actual}"
    )]
    InvalidTransactionGrammar {
        version: &'static str,
        contract: String,
        transaction_hash: String,
        log_index: u64,
        expected: &'static str,
        actual: &'static str,
    },
    #[error(
        "V2 fee conservation failed for {contract} transaction {transaction_hash}: OrderFilled fees={order_filled_fee}, FeeCharged amounts={fee_charged_amount}"
    )]
    FeeConservation {
        contract: String,
        transaction_hash: String,
        order_filled_fee: String,
        fee_charged_amount: String,
    },
}

#[derive(Debug, Clone)]
pub struct ExchangeHistoryProjection {
    pub raw_logs: Vec<ExchangeLogRawRow>,
    pub events: Vec<ExchangeEventRow>,
    pub fee_charges: Vec<ExchangeFeeChargeRow>,
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
            DecodedEvent::FeeCharge(_) => {}
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
    let provider_observations = ProviderObservations {
        hypersync_at: hypersync_observed_at,
        attestor_at: attestor_observed_at,
    };
    let mut observations = Vec::with_capacity(logs.len());
    for log in logs {
        if log.removed {
            return Err(ExecutionProjectionError::RemovedLog);
        }
        let contract = log.exchange_contract()?;
        let raw_log_hash = log.canonical_hash();
        observations.push((
            raw_row(log, contract, raw_log_hash, provider_observations, context)?,
            decode_event(log, contract, raw_log_hash, context)?,
        ));
    }
    observations.sort_by(|left, right| {
        (
            left.1.contract_address(),
            left.1.transaction_hash(),
            left.1.log_index(),
        )
            .cmp(&(
                right.1.contract_address(),
                right.1.transaction_hash(),
                right.1.log_index(),
            ))
    });
    let (raw_logs, decoded): (Vec<_>, Vec<_>) = observations.into_iter().unzip();
    validate_fee_conservation(&decoded)?;
    let mut fee_charges = Vec::new();
    let mut trading_events = Vec::new();
    for event in decoded {
        match event {
            DecodedEvent::FeeCharge(fee_charge) => fee_charges.push(fee_charge.row),
            event => trading_events.push(event),
        }
    }
    let decoded = trading_events;

    let mut aggregate_ids = BTreeSet::new();
    let mut matched_fills = BTreeMap::new();
    let mut match_rows = Vec::new();
    let mut start = 0_usize;
    while start < decoded.len() {
        let transaction_hash = decoded[start].transaction_hash().to_owned();
        let contract_address = decoded[start].contract_address().to_owned();
        let mut end = start + 1;
        while end < decoded.len()
            && decoded[end].transaction_hash() == transaction_hash
            && decoded[end].contract_address() == contract_address
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
        match event {
            DecodedEvent::Fill(fill) => {
                if !aggregate_ids.contains(&fill.row.event_id) {
                    let match_binding = matched_fills.get(&fill.row.event_id);
                    let execution = execution_from_fill(
                        &fill,
                        match_binding,
                        policy_hash,
                        chunk_id,
                        &market_for_token,
                    )?;
                    participants.extend(participants_for(&execution, policy_hash, chunk_id));
                    executions.push(execution);
                }
                events.push(fill.row);
            }
            DecodedEvent::Matched(matched) => events.push(matched.row),
            DecodedEvent::FeeCharge(_) => return Err(ExecutionProjectionError::DecodeFailure),
        }
    }
    Ok(ExchangeHistoryProjection {
        raw_logs,
        events,
        fee_charges,
        matches: match_rows,
        executions,
        participants,
    })
}

#[derive(Debug, Clone)]
enum DecodedEvent {
    Fill(DecodedFill),
    Matched(DecodedMatch),
    FeeCharge(DecodedFeeCharge),
}

impl DecodedEvent {
    fn contract_address(&self) -> &str {
        match self {
            Self::Fill(fill) => &fill.row.contract_address,
            Self::Matched(matched) => &matched.row.contract_address,
            Self::FeeCharge(fee_charge) => &fee_charge.row.contract_address,
        }
    }

    fn transaction_hash(&self) -> &str {
        match self {
            Self::Fill(fill) => &fill.row.transaction_hash,
            Self::Matched(matched) => &matched.row.transaction_hash,
            Self::FeeCharge(fee_charge) => &fee_charge.row.transaction_hash,
        }
    }

    const fn log_index(&self) -> u64 {
        match self {
            Self::Fill(fill) => fill.row.log_index,
            Self::Matched(matched) => matched.row.log_index,
            Self::FeeCharge(fee_charge) => fee_charge.row.log_index,
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
struct DecodedFeeCharge {
    row: ExchangeFeeChargeRow,
    amount_raw: U256,
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

fn validate_fee_conservation(events: &[DecodedEvent]) -> Result<(), ExecutionProjectionError> {
    let mut totals = BTreeMap::<(&str, &str), (U256, U256)>::new();
    for event in events {
        let totals = totals
            .entry((event.contract_address(), event.transaction_hash()))
            .or_default();
        match event {
            DecodedEvent::Fill(fill) => {
                totals.0 = totals
                    .0
                    .checked_add(fill.fee_raw)
                    .ok_or(ExecutionProjectionError::InvalidAmount)?;
            }
            DecodedEvent::FeeCharge(fee_charge) => {
                totals.1 = totals
                    .1
                    .checked_add(fee_charge.amount_raw)
                    .ok_or(ExecutionProjectionError::InvalidAmount)?;
            }
            DecodedEvent::Matched(_) => {}
        }
    }
    for ((contract, transaction_hash), (order_filled_fee, fee_charged_amount)) in totals {
        if order_filled_fee != fee_charged_amount {
            return Err(ExecutionProjectionError::FeeConservation {
                contract: contract.to_owned(),
                transaction_hash: transaction_hash.to_owned(),
                order_filled_fee: order_filled_fee.to_string(),
                fee_charged_amount: fee_charged_amount.to_string(),
            });
        }
    }
    Ok(())
}

fn correlate_transaction(
    events: &[DecodedEvent],
    aggregate_ids: &mut BTreeSet<ChDigest>,
    matched_fills: &mut BTreeMap<ChDigest, MatchedFill>,
    match_rows: &mut Vec<ExchangeMatchRow>,
) -> Result<(), ExecutionProjectionError> {
    let Some(first) = events.first() else {
        return Ok(());
    };
    if events
        .windows(2)
        .any(|pair| pair[0].log_index() >= pair[1].log_index())
    {
        return Err(transaction_grammar_error(
            first,
            "strictly increasing unique log indexes",
            "duplicate or non-increasing log index",
        ));
    }
    let has_match = events
        .iter()
        .any(|event| matches!(event, DecodedEvent::Matched(_)));
    if !has_match {
        return Err(transaction_grammar_error(
            first,
            "one or more complete matchOrders groups",
            "standalone OrderFilled",
        ));
    }
    let mut maker_start = 0_usize;
    for (match_index, event) in events.iter().enumerate() {
        let DecodedEvent::Matched(match_event) = event else {
            continue;
        };
        let aggregate_index = match_index.checked_sub(1).ok_or_else(|| {
            transaction_grammar_error(
                event,
                "maker fills followed by aggregate taker fill and OrdersMatched",
                "OrdersMatched without aggregate fill",
            )
        })?;
        let DecodedEvent::Fill(aggregate) = &events[aggregate_index] else {
            return Err(transaction_grammar_error(
                event,
                "aggregate taker OrderFilled immediately before OrdersMatched",
                "non-fill event before OrdersMatched",
            ));
        };
        if !validate_aggregate(aggregate, match_event) {
            return Err(transaction_grammar_error(
                event,
                "aggregate taker fill matching OrdersMatched identity and amounts",
                "mismatched aggregate fill",
            ));
        }
        if aggregate_index == maker_start {
            return Err(transaction_grammar_error(
                event,
                "at least one maker fill before the aggregate taker fill",
                "empty maker fill set",
            ));
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
                return Err(transaction_grammar_error(
                    maker_event,
                    "only maker OrderFilled events inside a match group",
                    "nested or ambiguous OrdersMatched",
                ));
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
    if maker_start != events.len() {
        return Err(transaction_grammar_error(
            &events[maker_start],
            "all fills consumed by complete matchOrders groups",
            "unconsumed trailing OrderFilled",
        ));
    }
    Ok(())
}

fn transaction_grammar_error(
    event: &DecodedEvent,
    expected: &'static str,
    actual: &'static str,
) -> ExecutionProjectionError {
    ExecutionProjectionError::InvalidTransactionGrammar {
        version: "v2",
        contract: event.contract_address().to_owned(),
        transaction_hash: event.transaction_hash().to_owned(),
        log_index: event.log_index(),
        expected,
        actual,
    }
}

fn validate_aggregate(fill: &DecodedFill, matched: &DecodedMatch) -> bool {
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
    common && assets
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
        order_hash: fill.order_hash.clone(),
        contract_key: fill.row.contract_key.clone(),
        exchange_version: fill.row.exchange_version,
        contract_address: fill.row.contract_address.clone(),
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
        fee_usd: ChUsd::from(Usd::new(raw_decimal(fill.fee_raw)?)),
        builder: fill.row.builder.clone(),
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
    if topic == contract.order_filled_topic {
        let decoded =
            order_filled_v2::decode_log(&rpc_log).ok_or(ExecutionProjectionError::DecodeFailure)?;
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
        return Ok(DecodedEvent::Fill(DecodedFill {
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
        }));
    }
    if topic == contract.orders_matched_topic {
        return decode_matched_v2(log, &rpc_log, contract, raw_log_hash, context);
    }
    if topic == contract.fee_charged_topic {
        let decoded =
            fee_charged_v2::decode_log(&rpc_log).ok_or(ExecutionProjectionError::DecodeFailure)?;
        let fee_charge_id = digest_parts(
            b"quant-pivot/exchange-fee-charge/v2\0",
            &[raw_log_hash.as_bytes()],
        );
        return Ok(DecodedEvent::FeeCharge(DecodedFeeCharge {
            row: ExchangeFeeChargeRow {
                fee_charge_id,
                raw_log_hash,
                chain_id: polygon_chain_id(),
                contract_key: contract.key.to_owned(),
                exchange_version: ChExchangeVersion::V2,
                contract_address: log.address.clone(),
                block_number: log.block_number,
                block_hash: log.block_hash.clone(),
                block_timestamp: millis(log.block_timestamp)?,
                transaction_hash: log.transaction_hash.clone(),
                transaction_index: log.transaction_index,
                log_index: log.log_index,
                receiver: address_text(decoded.receiver),
                amount_usd: ChUsd::from(Usd::new(raw_decimal(decoded.amount)?)),
                observed_at: context.observed_at,
                model_available_at: millis(log.model_available_timestamp)?,
                availability_policy_hash: context.policy_hash,
                chunk_id: context.chunk_id,
                schema_version: ExchangeFeeChargeRow::SCHEMA_VERSION,
            },
            amount_raw: decoded.amount,
        }));
    }
    Err(ExecutionProjectionError::DecodeFailure)
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
        chain_id: polygon_chain_id(),
        event_kind: fields.event_kind,
        contract_key: contract.key.to_owned(),
        exchange_version: ChExchangeVersion::V2,
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
        exchange_version: ChExchangeVersion::V2,
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

#[cfg(test)]
mod tests {
    use alloy::{
        primitives::{Address, B256, LogData, U256, address, b256, keccak256},
        sol_types::SolEvent,
    };
    use quant_pivot_models::types::{ContentHash, MarketId, TokenId, Usd};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    use super::{
        CanonicalExchangeLog, ExchangeHistoryProjection, ExecutionProjectionError, project_history,
    };
    use crate::exchange::{
        constants::CTF_EXCHANGE_V2, fee_charged_v2::FEE_CHARGED_TOPIC,
        order_filled_v2::ORDER_FILLED_TOPIC as ORDER_FILLED_V2_TOPIC,
        orders_matched_v2::ORDERS_MATCHED_TOPIC as ORDERS_MATCHED_V2_TOPIC,
    };

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

            event FeeCharged(address indexed receiver, uint256 amount);
        }
    }

    use v2_events::{
        FeeCharged as FeeChargedV2, OrderFilled as OrderFilledV2, OrdersMatched as OrdersMatchedV2,
    };

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
    const TX_HASH: B256 = b256!("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");

    #[test]
    fn canonical_topics_match() {
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
        assert_eq!(FEE_CHARGED_TOPIC, keccak256("FeeCharged(address,uint256)"));
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
            canonical(
                CTF_EXCHANGE_V2.address,
                4,
                FeeChargedV2 {
                    receiver: EXCHANGE,
                    amount: U256::from(1_000_000_u64),
                }
                .encode_log_data(),
            ),
        ];

        let projection = project(&logs).expect("valid V2 projection");
        assert_eq!(projection.events.len(), 4);
        assert_eq!(projection.fee_charges.len(), 1);
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
            Err(ExecutionProjectionError::InvalidTransactionGrammar {
                version: "v2",
                contract,
                transaction_hash,
                log_index: 2,
                expected: "aggregate taker fill matching OrdersMatched identity and amounts",
                actual: "mismatched aggregate fill",
            }) if contract == format!("{:#x}", CTF_EXCHANGE_V2.address)
                && transaction_hash == format!("{TX_HASH:#x}")
        ));
    }

    #[test]
    fn fee_conservation_blocks_projection() {
        let logs = vec![
            canonical(
                CTF_EXCHANGE_V2.address,
                0,
                OrderFilledV2 {
                    orderHash: MAKER_HASH_ONE,
                    maker: MAKER_ONE,
                    taker: TAKER,
                    side: 1,
                    tokenId: U256::from(42_u64),
                    makerAmountFilled: U256::from(20_000_000_u64),
                    takerAmountFilled: U256::from(10_000_000_u64),
                    fee: U256::from(100_u64),
                    builder: B256::ZERO,
                    metadata: B256::ZERO,
                }
                .encode_log_data(),
            ),
            canonical(
                CTF_EXCHANGE_V2.address,
                1,
                FeeChargedV2 {
                    receiver: EXCHANGE,
                    amount: U256::from(99_u64),
                }
                .encode_log_data(),
            ),
        ];

        assert!(matches!(
            project(&logs),
            Err(ExecutionProjectionError::FeeConservation {
                order_filled_fee,
                fee_charged_amount,
                ..
            }) if order_filled_fee == "100" && fee_charged_amount == "99"
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
            |token| (token == &TokenId::new("42")).then(|| MarketId::new("market")),
        )
    }

    fn canonical(address: Address, log_index: u64, encoded: LogData) -> CanonicalExchangeLog {
        let (topics, data) = encoded.split();
        CanonicalExchangeLog {
            address: format!("{address:#x}"),
            block_number: CTF_EXCHANGE_V2.first_valid_block,
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
            transaction_hash: format!("{TX_HASH:#x}"),
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
