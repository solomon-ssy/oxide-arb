//! Late-arriving finalized V2 fee settlement projection.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use quant_pivot_api::exchange::constants::EXCHANGE_CONTRACTS;
use quant_pivot_error::{QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    clickhouse::ExchangeEventRow,
    domain::quant::ExecutionFillInfo,
    enums::{
        clickhouse::{ChExchangeEventKind, ChExchangeSide, ChExchangeVersion},
        common::Side,
    },
    types::{EvmAddress, EvmTransactionHash, FeeMeasurement, OrderId, Price, Shares, Usd},
};
use quant_pivot_repository::traits::{ExecutionSubmissionRepository, QuantFactReadRepository};
use rust_decimal::Decimal;

const UNSETTLED_FILL_BATCH: u64 = 256;
const V2_ASSET_SCALE: u64 = 1_000_000;

/// Independently upgrades authenticated fee derivations after finalized chain
/// facts arrive, including after execution-order reconciliation is terminal.
pub struct OnChainFeeSettlementService {
    submission: Arc<dyn ExecutionSubmissionRepository>,
    facts: Arc<dyn QuantFactReadRepository>,
    funder: EvmAddress,
}

impl OnChainFeeSettlementService {
    #[must_use]
    pub const fn new(
        submission: Arc<dyn ExecutionSubmissionRepository>,
        facts: Arc<dyn QuantFactReadRepository>,
        funder: EvmAddress,
    ) -> Self {
        Self {
            submission,
            facts,
            funder,
        }
    }

    pub async fn settle_pass(&self, now: DateTime<Utc>) -> QuantResult<()> {
        let fills = self
            .submission
            .unsettled_fills(UNSETTLED_FILL_BATCH)
            .await?;
        if fills.is_empty() {
            return Ok(());
        }
        let mut settlement_keys = BTreeSet::new();
        for pending in &fills {
            let key = (
                pending.fill.venue_order_id.as_str().to_owned(),
                pending.transaction_hash.as_str().to_owned(),
            );
            if !settlement_keys.insert(key) {
                return Err(ExecutionError::ReconciliationUnresolvable {
                    reason: "multiple authenticated fills share one order/transaction identity; exact V2 fee allocation is ambiguous"
                        .to_owned(),
                }
                .into());
            }
        }
        let order_ids = fills
            .iter()
            .map(|settlement| settlement.fill.venue_order_id.clone())
            .collect::<Vec<OrderId>>();
        let events = index_events(self.facts.order_filled_events(order_ids).await?)?;
        let mut settlements = Vec::new();
        for pending in fills {
            let fill = pending.fill;
            let transaction_hash = pending.transaction_hash;
            let key = (
                fill.venue_order_id.as_str().to_owned(),
                transaction_hash.as_str().to_owned(),
            );
            let Some(event) = events.get(&key) else {
                continue;
            };
            if event.model_available_at > now.timestamp_millis() {
                continue;
            }
            settlements.push(self.measurement(&fill, &transaction_hash, event, now)?);
        }
        self.submission.record_fee_settlements(settlements).await?;
        Ok(())
    }

    fn measurement(
        &self,
        fill: &ExecutionFillInfo,
        authenticated_transaction_hash: &EvmTransactionHash,
        event: &ExchangeEventRow,
        available_at: DateTime<Utc>,
    ) -> QuantResult<FeeMeasurement> {
        validate_event(fill, authenticated_transaction_hash, event, &self.funder)?;
        let raw_fee = event
            .fee_amount
            .as_deref()
            .ok_or_else(|| fee_error(fill, "finalized V2 OrderFilled has no fee amount"))?;
        let fee_raw = Decimal::from_str(raw_fee)
            .map_err(|error| fee_error(fill, &format!("invalid finalized fee: {error}")))?;
        let exchange_address = EvmAddress::parse(event.contract_address.clone())
            .map_err(|error| fee_error(fill, &format!("invalid exchange address: {error}")))?;
        let transaction_hash = EvmTransactionHash::parse(event.transaction_hash.clone())
            .map_err(|error| fee_error(fill, &format!("invalid transaction hash: {error}")))?;
        let matched_at = DateTime::from_timestamp_millis(event.block_timestamp)
            .ok_or_else(|| fee_error(fill, "invalid block timestamp"))?;
        let zero_builder = format!("0x{}", "0".repeat(64));
        Ok(FeeMeasurement::OnChainSettled {
            venue_trade_id: fill.venue_trade_id.clone(),
            chain_id: 137,
            protocol_version: 2,
            exchange_address,
            order_id: fill.venue_order_id.clone(),
            liquidity_role: fill.liquidity_role,
            transaction_hash,
            log_index: event.log_index,
            matched_at,
            available_at,
            settled_fee: Usd::new(fee_raw / Decimal::from(V2_ASSET_SCALE)),
            builder_code: event
                .builder
                .clone()
                .filter(|builder| builder != &zero_builder),
        })
    }
}

fn index_events(
    events: Vec<ExchangeEventRow>,
) -> QuantResult<BTreeMap<(String, String), ExchangeEventRow>> {
    let mut indexed = BTreeMap::new();
    for event in events {
        let key = (event.order_hash.clone(), event.transaction_hash.clone());
        if indexed.insert(key, event).is_some() {
            return Err(ExecutionError::ReconciliationUnresolvable {
                reason:
                    "multiple finalized V2 OrderFilled logs share one order/transaction identity"
                        .to_owned(),
            }
            .into());
        }
    }
    Ok(indexed)
}

fn validate_event(
    fill: &ExecutionFillInfo,
    authenticated_transaction_hash: &EvmTransactionHash,
    event: &ExchangeEventRow,
    funder: &EvmAddress,
) -> QuantResult<()> {
    let expected_side = match fill.side {
        Side::Buy => ChExchangeSide::Buy,
        Side::Sell => ChExchangeSide::Sell,
    };
    let registered_exchange = EXCHANGE_CONTRACTS.iter().any(|contract| {
        format!("{:#x}", contract.address).eq_ignore_ascii_case(&event.contract_address)
    });
    let event_maker = EvmAddress::parse(event.maker.to_ascii_lowercase())
        .map_err(|error| fee_error(fill, &format!("invalid order maker address: {error}")))?;
    if event.chain_id != 137
        || event.event_kind != ChExchangeEventKind::OrderFilled
        || event.exchange_version != ChExchangeVersion::V2
        || event.schema_version != ExchangeEventRow::SCHEMA_VERSION
        || !registered_exchange
        || &event_maker != funder
        || event.order_hash != fill.venue_order_id.as_str()
        || event.transaction_hash != authenticated_transaction_hash.as_str()
        || event.token_id.as_deref() != Some(fill.token_id.as_str())
        || event.side != expected_side
    {
        return Err(fee_error(
            fill,
            "finalized V2 OrderFilled identity differs from authenticated fill",
        )
        .into());
    }
    let maker_amount = decimal_asset(&event.maker_amount, fill)?;
    let taker_amount = decimal_asset(&event.taker_amount, fill)?;
    let (principal, shares) = match fill.side {
        Side::Buy => (maker_amount, taker_amount),
        Side::Sell => (taker_amount, maker_amount),
    };
    if shares <= Decimal::ZERO
        || Shares::new(shares) != fill.shares
        || Usd::new(principal) != fill.principal_usd
        || Price::new(principal / shares) != fill.price
    {
        return Err(fee_error(
            fill,
            "finalized V2 OrderFilled amounts differ from authenticated fill",
        )
        .into());
    }
    Ok(())
}

fn decimal_asset(raw: &str, fill: &ExecutionFillInfo) -> QuantResult<Decimal> {
    Decimal::from_str(raw)
        .map(|value| value / Decimal::from(V2_ASSET_SCALE))
        .map_err(|error| fee_error(fill, &format!("invalid V2 asset amount: {error}")).into())
}

fn fee_error(fill: &ExecutionFillInfo, reason: &str) -> ExecutionError {
    ExecutionError::ReconciliationUnresolvable {
        reason: format!(
            "fee settlement for trade {} failed: {reason}",
            fill.venue_trade_id
        ),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_api::exchange::constants::CTF_EXCHANGE_V2;
    use quant_pivot_error::{QuantError, QuantResult};
    use quant_pivot_models::{
        clickhouse::{ChDigest, ExchangeEventRow},
        domain::quant::ExecutionFillInfo,
        enums::{
            clickhouse::{ChExchangeEventKind, ChExchangeSide, ChExchangeVersion},
            common::Side,
            execution::ExecutionOrderPhase,
            fee::FeeLiquidityRole,
        },
        types::{
            ContentHash, EvmAddress, EvmTransactionHash, ExecutionAccountId, ExecutionFillId,
            ExecutionOrderId, MarketId, OrderId, OrderIntentId, Price, Shares, TokenId, Usd,
            VenueTradeId,
        },
    };
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use super::{index_events, validate_event};

    struct SettlementFixture {
        fill: ExecutionFillInfo,
        transaction_hash: EvmTransactionHash,
        funder: EvmAddress,
        event: ExchangeEventRow,
    }

    impl SettlementFixture {
        fn build() -> QuantResult<Self> {
            let matched_at = Utc
                .timestamp_opt(1_750_000_000, 0)
                .single()
                .ok_or_else(|| QuantError::config("invalid fee-settlement fixture timestamp"))?;
            let funder = EvmAddress::parse(format!("0x{}", "1".repeat(40)))
                .map_err(|error| QuantError::config(error.to_string()))?;
            let transaction_hash = EvmTransactionHash::parse(format!("0x{}", "2".repeat(64)))
                .map_err(|error| QuantError::config(error.to_string()))?;
            let order_id = OrderId::new(format!("0x{}", "3".repeat(64)));
            let evidence_hash = ContentHash::from_bytes([4; 32]);
            let token_id = TokenId::new("42");
            let fill = ExecutionFillInfo {
                execution_fill_id: ExecutionFillId::from_v7(),
                execution_order_id: ExecutionOrderId::from_v7(),
                order_intent_id: OrderIntentId::from_v7(),
                execution_account_id: ExecutionAccountId::from_v7(),
                venue_trade_id: VenueTradeId::new("trade-1"),
                venue_bucket_index: 0,
                venue_order_id: order_id.clone(),
                order_phase: ExecutionOrderPhase::Entry,
                market_id: MarketId::new("market-1"),
                token_id: token_id.clone(),
                side: Side::Buy,
                liquidity_role: FeeLiquidityRole::Taker,
                shares: Shares::new(dec!(100)),
                price: Price::new(dec!(0.5)),
                principal_usd: Usd::new(dec!(50)),
                matched_at,
                available_at: matched_at,
                evidence_hash,
                created_at: matched_at,
            };
            let event = ExchangeEventRow {
                event_id: ChDigest::new([5; 32]),
                raw_log_hash: ChDigest::new([6; 32]),
                chain_id: 137,
                event_kind: ChExchangeEventKind::OrderFilled,
                contract_key: CTF_EXCHANGE_V2.key.to_owned(),
                exchange_version: ChExchangeVersion::V2,
                contract_address: format!("{:#x}", CTF_EXCHANGE_V2.address),
                block_number: 90_000_000,
                block_hash: format!("0x{}", "7".repeat(64)),
                block_timestamp: matched_at.timestamp_millis(),
                transaction_hash: transaction_hash.as_str().to_owned(),
                transaction_index: 1,
                log_index: 2,
                order_hash: order_id.as_str().to_owned(),
                maker: funder.as_str().to_owned(),
                taker: Some(format!("0x{}", "8".repeat(40))),
                side: ChExchangeSide::Buy,
                token_id: Some(token_id.as_str().to_owned()),
                maker_asset_id: Some("0".to_owned()),
                taker_asset_id: Some(token_id.as_str().to_owned()),
                maker_amount: "50000000".to_owned(),
                taker_amount: "100000000".to_owned(),
                fee_amount: Some("1000000".to_owned()),
                builder: None,
                metadata: None,
                observed_at: matched_at.timestamp_millis(),
                model_available_at: matched_at.timestamp_millis(),
                availability_policy_hash: ChDigest::new([9; 32]),
                chunk_id: Uuid::now_v7(),
                schema_version: ExchangeEventRow::SCHEMA_VERSION,
            };
            Ok(Self {
                fill,
                transaction_hash,
                funder,
                event,
            })
        }
    }

    #[test]
    fn exact_event_identity_validates() -> QuantResult<()> {
        let fixture = SettlementFixture::build()?;

        validate_event(
            &fixture.fill,
            &fixture.transaction_hash,
            &fixture.event,
            &fixture.funder,
        )?;
        Ok(())
    }

    #[test]
    fn wrong_chain_fails_closed() -> QuantResult<()> {
        let mut fixture = SettlementFixture::build()?;
        fixture.event.chain_id = 1;

        assert!(
            validate_event(
                &fixture.fill,
                &fixture.transaction_hash,
                &fixture.event,
                &fixture.funder,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn duplicate_exact_logs_fail() -> QuantResult<()> {
        let fixture = SettlementFixture::build()?;

        assert!(index_events(vec![fixture.event.clone(), fixture.event]).is_err());
        Ok(())
    }
}
