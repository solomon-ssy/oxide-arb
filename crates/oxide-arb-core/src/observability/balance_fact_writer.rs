//! Postgres fact writer for collateral and token-balance observations.

use chrono::{DateTime, Utc};
use oxide_arb_error::OxideError;
use oxide_arb_models::{
    domain::{NewBalanceSnapshot, NewTokenBalanceSnapshot, position::PositionInfo},
    enums::fact::BalanceSnapshotSource,
    types::{BalanceSnapshotId, Shares, TokenBalanceSnapshotId, Usd},
};
use oxide_arb_repository::{postgres::PgFactDataRepository, traits::BalanceSnapshotRepository};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone)]
pub struct BalanceFactObservation {
    pub holder_address: String,
    pub internal_available_usd: Usd,
    pub internal_reserved_usd: Usd,
    pub external_available_usd: Usd,
    pub external_locked_usd: Usd,
    pub block_number: Option<i64>,
    pub reconciliation_report_id: Option<i64>,
    pub observed_at: DateTime<Utc>,
    pub positions: Vec<PositionInfo>,
}

pub struct BalanceFactWriter {
    repo: Arc<PgFactDataRepository>,
}

impl BalanceFactWriter {
    pub const fn new(repo: Arc<PgFactDataRepository>) -> Self {
        Self { repo }
    }

    pub async fn write_observation(
        &self,
        observation: BalanceFactObservation,
    ) -> Result<(), OxideError> {
        let internal_total = observation.internal_available_usd + observation.internal_reserved_usd;
        let external_total = observation.external_available_usd + observation.external_locked_usd;
        self.repo
            .create_balance_snapshot(NewBalanceSnapshot {
                balance_snapshot_id: BalanceSnapshotId::new_v7(),
                holder_address: observation.holder_address.clone(),
                internal_available_usd: observation.internal_available_usd,
                internal_reserved_usd: observation.internal_reserved_usd,
                external_available_usd: observation.external_available_usd,
                external_locked_usd: observation.external_locked_usd,
                drift_usd: internal_total - external_total,
                source: BalanceSnapshotSource::ClobApi,
                block_number: observation.block_number,
                reconciliation_report_id: observation.reconciliation_report_id,
                observed_at: observation.observed_at,
            })
            .await?;

        let token_snapshots = token_snapshots_from_positions(&observation);
        self.repo
            .create_token_balance_snapshots(token_snapshots)
            .await?;
        Ok(())
    }
}

fn token_snapshots_from_positions(
    observation: &BalanceFactObservation,
) -> Vec<NewTokenBalanceSnapshot> {
    let mut by_token = HashMap::new();
    for position in &observation.positions {
        let key = (
            position.market_id.clone(),
            position.token_id.clone(),
            position.side,
        );
        let shares = by_token.entry(key).or_insert(Shares::ZERO);
        *shares += position.shares;
    }

    by_token
        .into_iter()
        .map(
            |((market_id, token_id, side), internal_shares)| NewTokenBalanceSnapshot {
                token_balance_snapshot_id: TokenBalanceSnapshotId::new_v7(),
                holder_address: observation.holder_address.clone(),
                market_id,
                token_id,
                side,
                internal_shares,
                external_shares: None,
                drift_shares: None,
                source: BalanceSnapshotSource::InternalLedger,
                block_number: observation.block_number,
                reconciliation_report_id: observation.reconciliation_report_id,
                observed_at: observation.observed_at,
            },
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::{
        enums::common::{PositionStatus, RedeemStatus, SettlementAccountingStatus, Side},
        types::{MarketId, PositionId, Price, TokenId, TradeId},
    };
    use rust_decimal_macros::dec;

    #[test]
    fn token_snapshots_aggregate_internal_shares_without_faking_external_balances() {
        let market_id = MarketId::new("0xmarket");
        let token_id = TokenId::new("token");
        let observation = BalanceFactObservation {
            holder_address: "0xholder".into(),
            internal_available_usd: Usd::new(dec!(900)),
            internal_reserved_usd: Usd::new(dec!(100)),
            external_available_usd: Usd::new(dec!(1000)),
            external_locked_usd: Usd::ZERO,
            block_number: None,
            reconciliation_report_id: None,
            observed_at: Utc::now(),
            positions: vec![
                position(&market_id, &token_id, dec!(4)),
                position(&market_id, &token_id, dec!(6)),
            ],
        };

        let snapshots = token_snapshots_from_positions(&observation);

        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].internal_shares, Shares::new(dec!(10)));
        assert!(snapshots[0].external_shares.is_none());
        assert!(snapshots[0].drift_shares.is_none());
        assert_eq!(snapshots[0].source, BalanceSnapshotSource::InternalLedger);
    }

    fn position(
        market_id: &MarketId,
        token_id: &TokenId,
        shares: rust_decimal::Decimal,
    ) -> PositionInfo {
        PositionInfo {
            position_id: PositionId::generate(),
            trade_id: TradeId::generate(),
            market_id: market_id.clone(),
            token_id: token_id.clone(),
            side: Side::Buy,
            shares: Shares::new(shares),
            avg_entry_price: Price::new(dec!(0.9)),
            total_cost_usd: Usd::new(shares * dec!(0.9)),
            total_fees_usd: Usd::ZERO,
            unrealized_pnl: Usd::ZERO,
            realized_pnl: Usd::ZERO,
            status: PositionStatus::Open,
            opened_at: Utc::now(),
            closed_at: None,
            settled_at: None,
            winning_token_id: None,
            settlement_payout_usd: None,
            redeem_tx_hash: None,
            redeem_status: RedeemStatus::Pending,
            redeem_attempts: 0,
            oracle_verdict: None,
            settlement_trigger: None,
            settlement_accounting_status: SettlementAccountingStatus::Pending,
            settlement_accounting_error: None,
            settlement_accounted_at: None,
            redeem_terminal_reason: None,
        }
    }
}
