//! Normalize decoded `OrderFilled` logs into trade-tape prints.

use alloy::primitives::{Address, B256, U256};
use chrono::{DateTime, TimeZone, Utc};
use quant_pivot_models::{
    domain::data_plane::{
        TradeParticipantRole, TradeTapePrint, TradeTapeSourceKind,
        trade_tape_coverage::{PARTICIPANT_ADDRESS, PARTICIPANT_ROLE, SIDE, TOKEN_ID, TX_HASH},
    },
    enums::{common::Side, fee::FeeLiquidityRole},
    types::{FeeEvidence, MarketId, OrderId, Price, Shares, TokenId, Usd, VenueFillObservation},
};
use rust_decimal::Decimal;
use serde_json::json;

use super::{
    constants::ExchangeContract,
    denylist::is_human_participant,
    log_client::FetchedLog,
    order_filled_v1::{self, DecodedOrderFilledV1},
    order_filled_v2::{self, DecodedOrderFilledV2},
};

/// Why a log could not be normalized into trade-tape prints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeRejectReason {
    DecodeFailed,
    InvalidTimestamp,
    UnknownToken,
    ZeroNotional,
    DenylistedMaker,
    MissingIdentity,
}

/// Normalized primary/secondary prints for one `OrderFilled` log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedFillLegs {
    pub primary: TradeTapePrint,
    pub secondary_taker: Option<TradeTapePrint>,
    pub venue_fill: Option<VenueFillObservation>,
}

/// Normalize a V1 `OrderFilled` log.
pub fn normalize_v1_log(
    contract: ExchangeContract,
    fetched: &FetchedLog,
    market_for_token: impl Fn(&TokenId) -> Option<MarketId>,
) -> Result<NormalizedFillLegs, DecodeRejectReason> {
    let decoded =
        order_filled_v1::decode_log(&fetched.log).ok_or(DecodeRejectReason::DecodeFailed)?;
    normalize_v1_decoded(contract, fetched, &decoded, market_for_token)
}

/// Normalize a V2 `OrderFilled` log.
pub fn normalize_v2_log(
    contract: ExchangeContract,
    fetched: &FetchedLog,
    market_for_token: impl Fn(&TokenId) -> Option<MarketId>,
) -> Result<NormalizedFillLegs, DecodeRejectReason> {
    let decoded =
        order_filled_v2::decode_log(&fetched.log).ok_or(DecodeRejectReason::DecodeFailed)?;
    normalize_v2_decoded(contract, fetched, &decoded, market_for_token)
}

fn normalize_v1_decoded(
    contract: ExchangeContract,
    fetched: &FetchedLog,
    decoded: &DecodedOrderFilledV1,
    market_for_token: impl Fn(&TokenId) -> Option<MarketId>,
) -> Result<NormalizedFillLegs, DecodeRejectReason> {
    if !is_human_participant(decoded.maker) {
        return Err(DecodeRejectReason::DenylistedMaker);
    }
    let is_buy = decoded.maker_asset_id.is_zero();
    let token_raw = if is_buy {
        decoded.taker_asset_id
    } else {
        decoded.maker_asset_id
    };
    if token_raw.is_zero() {
        return Err(DecodeRejectReason::UnknownToken);
    }
    let collateral_raw = if is_buy {
        decoded.maker_amount_filled
    } else {
        decoded.taker_amount_filled
    };
    let shares_raw = if is_buy {
        decoded.taker_amount_filled
    } else {
        decoded.maker_amount_filled
    };
    let side = if is_buy { Side::Buy } else { Side::Sell };
    build_legs(
        FillAmounts {
            contract,
            fetched,
            token_id: TokenId::new(token_raw.to_string()),
            side,
            collateral_raw,
            shares_raw,
            maker: decoded.maker,
            taker: decoded.taker,
            exact_order_evidence: None,
        },
        market_for_token,
    )
}

fn normalize_v2_decoded(
    contract: ExchangeContract,
    fetched: &FetchedLog,
    decoded: &DecodedOrderFilledV2,
    market_for_token: impl Fn(&TokenId) -> Option<MarketId>,
) -> Result<NormalizedFillLegs, DecodeRejectReason> {
    if !is_human_participant(decoded.maker) {
        return Err(DecodeRejectReason::DenylistedMaker);
    }
    if decoded.token_id.is_zero() {
        return Err(DecodeRejectReason::UnknownToken);
    }
    let is_buy = decoded.side == 0;
    let collateral_raw = if is_buy {
        decoded.maker_amount_filled
    } else {
        decoded.taker_amount_filled
    };
    let shares_raw = if is_buy {
        decoded.taker_amount_filled
    } else {
        decoded.maker_amount_filled
    };
    let side = if is_buy { Side::Buy } else { Side::Sell };
    build_legs(
        FillAmounts {
            contract,
            fetched,
            token_id: TokenId::new(decoded.token_id.to_string()),
            side,
            collateral_raw,
            shares_raw,
            maker: decoded.maker,
            taker: decoded.taker,
            exact_order_evidence: Some(ExactOrderEvidence {
                order_hash: decoded.order_hash,
                fee: decoded.fee,
                builder: decoded.builder,
                metadata: decoded.metadata,
                liquidity_role: if decoded.taker == contract.address {
                    FeeLiquidityRole::Taker
                } else {
                    FeeLiquidityRole::Maker
                },
            }),
        },
        market_for_token,
    )
}

struct FillAmounts<'a> {
    contract: ExchangeContract,
    fetched: &'a FetchedLog,
    token_id: TokenId,
    side: Side,
    collateral_raw: U256,
    shares_raw: U256,
    maker: Address,
    taker: Address,
    exact_order_evidence: Option<ExactOrderEvidence>,
}

#[derive(Clone, Copy)]
struct ExactOrderEvidence {
    order_hash: B256,
    fee: U256,
    builder: B256,
    metadata: B256,
    liquidity_role: FeeLiquidityRole,
}

fn build_legs(
    fill: FillAmounts<'_>,
    market_for_token: impl Fn(&TokenId) -> Option<MarketId>,
) -> Result<NormalizedFillLegs, DecodeRejectReason> {
    let FillAmounts {
        contract,
        fetched,
        token_id,
        side,
        collateral_raw,
        shares_raw,
        maker,
        taker,
        exact_order_evidence,
    } = fill;
    let market_id = market_for_token(&token_id).ok_or(DecodeRejectReason::UnknownToken)?;
    let event_time = block_event_time(fetched.block_timestamp)?;
    let (notional, price, size_shares) = amounts_from_raw(collateral_raw, shares_raw)?;
    if notional <= Decimal::ZERO {
        return Err(DecodeRejectReason::ZeroNotional);
    }
    let tx_hash = fetched
        .log
        .transaction_hash
        .map(|hash| format!("{hash:#x}"));
    let log_index = fetched.log.log_index;
    let trade_id_base = format!(
        "{}:{}:{}",
        contract.key,
        tx_hash.as_deref().unwrap_or("0x0"),
        log_index.unwrap_or_default()
    );
    let raw_payload = json!({
        "exchange_version": contract.version.as_str(),
        "exchange_key": contract.key,
        "block_number": fetched.block_number,
        "log_index": log_index,
        "order_hash": exact_order_evidence.map(|value| format!("{:#x}", value.order_hash)),
        "fee_raw": exact_order_evidence.map(|value| value.fee.to_string()),
        "builder": exact_order_evidence.map(|value| format!("{:#x}", value.builder)),
        "metadata": exact_order_evidence.map(|value| format!("{:#x}", value.metadata)),
    })
    .to_string();
    let primary_role = match exact_order_evidence.map(|value| value.liquidity_role) {
        Some(FeeLiquidityRole::Taker) => TradeParticipantRole::Taker,
        Some(FeeLiquidityRole::Maker) | None => TradeParticipantRole::Maker,
    };
    let coverage = PARTICIPANT_ADDRESS | PARTICIPANT_ROLE | TOKEN_ID | SIDE | TX_HASH;
    let primary = TradeTapePrint {
        market_id: market_id.clone(),
        token_id: token_id.clone(),
        event_time,
        available_at: None,
        participant_address: maker.to_checksum(None),
        participant_role: primary_role,
        side: Some(side),
        price,
        size_shares,
        notional_usd: Usd::new(notional),
        tx_hash: tx_hash.clone(),
        trade_id: format!("{trade_id_base}:maker"),
        source: TradeTapeSourceKind::OnChain,
        coverage_flags: coverage,
        raw_payload_json: Some(raw_payload.clone()),
    };
    let secondary_taker = if is_human_participant(taker) && taker != maker {
        Some(TradeTapePrint {
            market_id,
            token_id,
            event_time,
            available_at: None,
            participant_address: taker.to_checksum(None),
            participant_role: TradeParticipantRole::Taker,
            side: Some(side.opposite()),
            price,
            size_shares,
            notional_usd: Usd::new(notional),
            tx_hash: tx_hash.clone(),
            trade_id: format!("{trade_id_base}:taker"),
            source: TradeTapeSourceKind::OnChain,
            coverage_flags: coverage,
            raw_payload_json: Some(raw_payload),
        })
    } else {
        None
    };
    let venue_fill = exact_order_evidence
        .map(|exact| {
            let transaction_hash = tx_hash.clone().ok_or(DecodeRejectReason::MissingIdentity)?;
            let log_index = log_index.ok_or(DecodeRejectReason::MissingIdentity)?;
            let actual_fee = Usd::new(u256_to_decimal(exact.fee, 6)?);
            let builder_code =
                (exact.builder != B256::ZERO).then(|| format!("{:#x}", exact.builder));
            Ok(VenueFillObservation {
                order_id: OrderId::new(format!("{:#x}", exact.order_hash)),
                liquidity_role: exact.liquidity_role,
                filled_shares: size_shares,
                average_price: price,
                matched_at: event_time,
                maker_order_ids: Vec::new(),
                builder_code: builder_code.clone(),
                fee_evidence: FeeEvidence::OnChainExact {
                    order_id: OrderId::new(format!("{:#x}", exact.order_hash)),
                    liquidity_role: exact.liquidity_role,
                    transaction_hash,
                    log_index,
                    matched_at: event_time,
                    actual_fee,
                    builder_code,
                },
            })
        })
        .transpose()?;
    Ok(NormalizedFillLegs {
        primary,
        secondary_taker,
        venue_fill,
    })
}

fn block_event_time(timestamp: u64) -> Result<DateTime<Utc>, DecodeRejectReason> {
    i64::try_from(timestamp)
        .ok()
        .and_then(|secs| Utc.timestamp_opt(secs, 0).single())
        .ok_or(DecodeRejectReason::InvalidTimestamp)
}

fn amounts_from_raw(
    collateral_raw: U256,
    shares_raw: U256,
) -> Result<(Decimal, Price, Shares), DecodeRejectReason> {
    let collateral = u256_to_decimal(collateral_raw, 6)?;
    let shares = u256_to_decimal(shares_raw, 6)?;
    if shares <= Decimal::ZERO {
        return Err(DecodeRejectReason::ZeroNotional);
    }
    let price_value = (collateral / shares).round_dp(8);
    Ok((collateral, Price::new(price_value), Shares::new(shares)))
}

fn u256_to_decimal(value: U256, scale: u32) -> Result<Decimal, DecodeRejectReason> {
    let text = value.to_string();
    let raw = Decimal::from_str_exact(&text).map_err(|_| DecodeRejectReason::ZeroNotional)?;
    let divisor = Decimal::from(10_u64.pow(scale));
    Ok((raw / divisor).round_dp(scale))
}

#[cfg(test)]
mod tests {
    use alloy::{
        primitives::{Address, B256, Log as PrimitiveLog, U256},
        rpc::types::Log,
        sol_types::SolEvent,
    };
    use quant_pivot_models::types::MarketId;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::exchange::{
        constants::{CTF_EXCHANGE_V1, CTF_EXCHANGE_V2},
        order_filled_v1::OrderFilledV1,
        order_filled_v2::OrderFilledV2,
    };

    fn fetched_log(event: &impl SolEvent, block_timestamp: u64) -> FetchedLog {
        let log = Log {
            inner: PrimitiveLog {
                address: Address::ZERO,
                data: event.encode_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(B256::with_last_byte(1)),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        };
        FetchedLog {
            log,
            block_number: 1,
            block_timestamp,
        }
    }

    #[test]
    fn v1_buy_normalizes_primary() {
        let maker = Address::with_last_byte(0x11);
        let taker = Address::with_last_byte(0x22);
        let token_id = U256::from(123_u64);
        let event = OrderFilledV1 {
            orderHash: B256::with_last_byte(9),
            maker,
            taker,
            makerAssetId: U256::ZERO,
            takerAssetId: token_id,
            makerAmountFilled: U256::from(2_000_000_u64),
            takerAmountFilled: U256::from(4_000_000_u64),
            fee: U256::ZERO,
        };
        let fetched = fetched_log(&event, 1_700_000_000);
        let token = TokenId::new(token_id.to_string());
        let market = MarketId::new("m1");
        let decoded = DecodedOrderFilledV1 {
            order_hash: event.orderHash,
            maker: event.maker,
            taker: event.taker,
            maker_asset_id: event.makerAssetId,
            taker_asset_id: event.takerAssetId,
            maker_amount_filled: event.makerAmountFilled,
            taker_amount_filled: event.takerAmountFilled,
            fee: event.fee,
        };
        let legs = normalize_v1_decoded(CTF_EXCHANGE_V1, &fetched, &decoded, |id| {
            (id == &token).then_some(market.clone())
        })
        .expect("normalized");
        assert_eq!(legs.primary.participant_role, TradeParticipantRole::Maker);
        assert_eq!(legs.primary.notional_usd.inner(), dec!(2));
        assert_eq!(legs.primary.side, Some(Side::Buy));
        assert!(legs.secondary_taker.is_some());
        assert_eq!(legs.secondary_taker.unwrap().side, Some(Side::Sell));
    }

    #[test]
    fn v2_sell_normalizes_side() {
        let maker = Address::with_last_byte(0x31);
        let taker = Address::with_last_byte(0x32);
        let token_id = U256::from(456_u64);
        let event = OrderFilledV2 {
            orderHash: B256::with_last_byte(8),
            maker,
            taker,
            side: 1,
            tokenId: token_id,
            makerAmountFilled: U256::from(3_000_000_u64),
            takerAmountFilled: U256::from(6_000_000_u64),
            fee: U256::ZERO,
            builder: B256::ZERO,
            metadata: B256::ZERO,
        };
        let fetched = fetched_log(&event, 1_700_000_100);
        let token = TokenId::new(token_id.to_string());
        let market = MarketId::new("m2");
        let legs = normalize_v2_log(CTF_EXCHANGE_V2, &fetched, |id| {
            (id == &token).then_some(market.clone())
        })
        .expect("normalized");
        assert_eq!(legs.primary.participant_role, TradeParticipantRole::Maker);
        assert_eq!(legs.primary.side, Some(Side::Sell));
        assert_eq!(legs.primary.notional_usd.inner(), dec!(6));
        let taker_leg = legs.secondary_taker.expect("taker leg");
        assert_eq!(taker_leg.participant_role, TradeParticipantRole::Taker);
        assert_eq!(taker_leg.side, Some(Side::Buy));
    }
}
