//! In-memory repository mocks for integration tests and benchmarks.

use async_trait::async_trait;
use chrono::Utc;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    clickhouse::{
        BookSnapshotRow, CalibrationSnapshotRow, OpportunityAuditRow, OpportunityDetectionRow,
        TickEventRow,
    },
    domain::{
        MarkRedeemedParams, NewPosition, NewTrade, PositionInfo, SettlePositionParams, TradeInfo,
        UpdatePosition, UpdateTradeOutcome,
    },
    enums::common::{
        PositionStatus, RedeemStatus, SettlementAccountingStatus, SettlementTrigger, TradeOutcome,
    },
    types::{MarketId, PositionId, TradeId, Usd},
};
use oxide_arb_repository::traits::{PositionRepository, TimeseriesRepository, TradeRepository};
use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Default)]
pub struct MockTradeRepository {
    trades: Mutex<HashMap<String, TradeInfo>>,
    create_should_fail: AtomicBool,
}

impl MockTradeRepository {
    pub fn fail_create(&self) {
        self.create_should_fail.store(true, Ordering::Relaxed);
    }

    pub fn trade_count(&self) -> usize {
        self.trades.lock().unwrap().len()
    }

    pub fn find(&self, trade_id: &TradeId) -> Option<TradeInfo> {
        self.trades.lock().unwrap().get(trade_id.as_str()).cloned()
    }
}

#[derive(Default)]
pub struct MockPositionRepository {
    positions: Mutex<HashMap<String, PositionInfo>>,
}

#[async_trait]
impl PositionRepository for MockPositionRepository {
    async fn find_open(&self) -> Result<Vec<PositionInfo>, StorageError> {
        Ok(self
            .positions
            .lock()
            .unwrap()
            .values()
            .filter(|position| position.status == PositionStatus::Open)
            .cloned()
            .collect())
    }

    async fn find_by_id(
        &self,
        position_id: &PositionId,
    ) -> Result<Option<PositionInfo>, StorageError> {
        Ok(self
            .positions
            .lock()
            .unwrap()
            .get(position_id.as_str())
            .cloned())
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        Ok(self
            .positions
            .lock()
            .unwrap()
            .values()
            .filter(|position| &position.market_id == market_id)
            .cloned()
            .collect())
    }

    async fn find_open_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        Ok(self
            .find_by_market(market_id)
            .await?
            .into_iter()
            .filter(|position| position.status == PositionStatus::Open)
            .collect())
    }

    async fn find_by_trade_id(
        &self,
        trade_id: &TradeId,
    ) -> Result<Option<PositionInfo>, StorageError> {
        Ok(self
            .positions
            .lock()
            .unwrap()
            .values()
            .find(|position| &position.trade_id == trade_id)
            .cloned())
    }

    async fn find_redeem_retry_candidates(
        &self,
        max_attempts: u32,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        let max_attempts = i32::try_from(max_attempts).unwrap_or(i32::MAX);
        Ok(self
            .positions
            .lock()
            .unwrap()
            .values()
            .filter(|position| {
                position.status == PositionStatus::Open
                    && matches!(
                        position.redeem_status,
                        RedeemStatus::Pending | RedeemStatus::Failed
                    )
                    && position.redeem_attempts < max_attempts
            })
            .cloned()
            .collect())
    }

    async fn find_open_for_resolved_markets(
        &self,
        limit: u64,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        Ok(self.find_open().await?.into_iter().take(limit).collect())
    }

    async fn find_accounting_retry_candidates(
        &self,
        max_attempts: u32,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        let max_attempts = i32::try_from(max_attempts).unwrap_or(i32::MAX);
        Ok(self
            .positions
            .lock()
            .unwrap()
            .values()
            .filter(|position| {
                position.status == PositionStatus::Open
                    && position.settlement_accounting_status == SettlementAccountingStatus::Failed
                    && position.redeem_attempts < max_attempts
            })
            .cloned()
            .collect())
    }

    async fn create(&self, position: NewPosition) -> Result<PositionInfo, StorageError> {
        let now = Utc::now();
        let info = PositionInfo {
            position_id: PositionId::generate(),
            trade_id: position.trade_id,
            market_id: position.market_id,
            token_id: position.token_id,
            side: position.side,
            shares: position.shares,
            avg_entry_price: position.avg_entry_price,
            total_cost_usd: position.total_cost_usd,
            total_fees_usd: position.total_fees_usd,
            unrealized_pnl: Usd::ZERO,
            realized_pnl: Usd::ZERO,
            status: PositionStatus::Open,
            opened_at: now,
            closed_at: None,
            settled_at: None,
            winning_token_id: None,
            settlement_payout_usd: None,
            redeem_tx_hash: None,
            redeem_status: position.redeem_status,
            redeem_attempts: 0,
            oracle_verdict: None,
            settlement_trigger: None,
            settlement_accounting_status: SettlementAccountingStatus::Pending,
            settlement_accounting_error: None,
            settlement_accounted_at: None,
            redeem_terminal_reason: None,
        };
        self.positions
            .lock()
            .unwrap()
            .insert(info.position_id.to_string(), info.clone());
        Ok(info)
    }

    async fn update(
        &self,
        position_id: &PositionId,
        _update: UpdatePosition,
    ) -> Result<PositionInfo, StorageError> {
        self.find_by_id(position_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "position",
                id: position_id.to_string(),
            })
    }

    async fn close_position(
        &self,
        position_id: &PositionId,
        realized_pnl: rust_decimal::Decimal,
    ) -> Result<(), StorageError> {
        let mut positions = self.positions.lock().unwrap();
        let position =
            positions
                .get_mut(position_id.as_str())
                .ok_or_else(|| StorageError::NotFound {
                    entity: "position",
                    id: position_id.to_string(),
                })?;
        position.status = PositionStatus::Closed;
        position.realized_pnl = Usd::new(realized_pnl);
        position.closed_at = Some(Utc::now());
        drop(positions);
        Ok(())
    }

    async fn settle_position(
        &self,
        position_id: &PositionId,
        params: SettlePositionParams,
    ) -> Result<PositionInfo, StorageError> {
        let mut positions = self.positions.lock().unwrap();
        let position =
            positions
                .get_mut(position_id.as_str())
                .ok_or_else(|| StorageError::NotFound {
                    entity: "position",
                    id: position_id.to_string(),
                })?;
        position.status = PositionStatus::Settled;
        position.realized_pnl = Usd::new(params.realized_pnl);
        position.winning_token_id = Some(params.winning_token_id);
        position.settlement_payout_usd = Some(params.settlement_payout_usd);
        position.redeem_tx_hash = params.redeem_tx_hash;
        position.redeem_status = params.redeem_status;
        position.settlement_trigger = Some(params.settlement_trigger);
        position.oracle_verdict = params.oracle_verdict;
        position.settlement_accounting_status = SettlementAccountingStatus::Accounted;
        position.settlement_accounted_at = Some(Utc::now());
        position.settled_at = Some(Utc::now());
        let updated = position.clone();
        drop(positions);
        Ok(updated)
    }

    async fn mark_redeemed(
        &self,
        position_id: &PositionId,
        params: MarkRedeemedParams,
    ) -> Result<PositionInfo, StorageError> {
        let mut positions = self.positions.lock().unwrap();
        let position =
            positions
                .get_mut(position_id.as_str())
                .ok_or_else(|| StorageError::NotFound {
                    entity: "position",
                    id: position_id.to_string(),
                })?;
        position.winning_token_id = Some(params.winning_token_id);
        position.settlement_payout_usd = Some(params.settlement_payout_usd);
        position.redeem_tx_hash = params.redeem_tx_hash;
        position.redeem_status = params.redeem_status;
        position.settlement_trigger = Some(params.settlement_trigger);
        position.redeem_terminal_reason = params.redeem_terminal_reason;
        position.settlement_accounting_status = SettlementAccountingStatus::Redeemed;
        let updated = position.clone();
        drop(positions);
        Ok(updated)
    }

    async fn mark_accounted(
        &self,
        position_id: &PositionId,
        accounted_at: chrono::DateTime<Utc>,
    ) -> Result<PositionInfo, StorageError> {
        let mut positions = self.positions.lock().unwrap();
        let position =
            positions
                .get_mut(position_id.as_str())
                .ok_or_else(|| StorageError::NotFound {
                    entity: "position",
                    id: position_id.to_string(),
                })?;
        position.status = PositionStatus::Settled;
        position.settlement_accounting_status = SettlementAccountingStatus::Accounted;
        position.settlement_accounting_error = None;
        position.settlement_accounted_at = Some(accounted_at);
        position.settled_at = Some(accounted_at);
        let updated = position.clone();
        drop(positions);
        Ok(updated)
    }

    async fn mark_accounting_failed(
        &self,
        position_id: &PositionId,
        error: String,
    ) -> Result<PositionInfo, StorageError> {
        let mut positions = self.positions.lock().unwrap();
        let position =
            positions
                .get_mut(position_id.as_str())
                .ok_or_else(|| StorageError::NotFound {
                    entity: "position",
                    id: position_id.to_string(),
                })?;
        position.settlement_accounting_status = SettlementAccountingStatus::Failed;
        position.settlement_accounting_error = Some(error);
        let updated = position.clone();
        drop(positions);
        Ok(updated)
    }

    async fn record_redeem_failure(
        &self,
        position_id: &PositionId,
        attempts: u32,
        winning_token_id: &oxide_arb_models::types::TokenId,
        settlement_trigger: SettlementTrigger,
    ) -> Result<PositionInfo, StorageError> {
        let mut positions = self.positions.lock().unwrap();
        let position =
            positions
                .get_mut(position_id.as_str())
                .ok_or_else(|| StorageError::NotFound {
                    entity: "position",
                    id: position_id.to_string(),
                })?;
        position.redeem_status = RedeemStatus::Failed;
        position.redeem_attempts = i32::try_from(attempts).unwrap_or(i32::MAX);
        position.winning_token_id = Some(winning_token_id.clone());
        position.settlement_trigger = Some(settlement_trigger);
        let updated = position.clone();
        drop(positions);
        Ok(updated)
    }

    async fn patch_oracle_verdict(
        &self,
        position_id: &PositionId,
        verdict: serde_json::Value,
    ) -> Result<(), StorageError> {
        let mut positions = self.positions.lock().unwrap();
        let position =
            positions
                .get_mut(position_id.as_str())
                .ok_or_else(|| StorageError::NotFound {
                    entity: "position",
                    id: position_id.to_string(),
                })?;
        position.oracle_verdict = Some(verdict);
        drop(positions);
        Ok(())
    }

    async fn total_exposure(&self) -> Result<Usd, StorageError> {
        Ok(self
            .find_open()
            .await?
            .iter()
            .map(|position| position.total_cost_usd)
            .sum())
    }

    async fn count_open(&self) -> Result<usize, StorageError> {
        Ok(self.find_open().await?.len())
    }
}

#[async_trait]
impl TradeRepository for MockTradeRepository {
    async fn create(&self, trade: NewTrade) -> Result<TradeInfo, StorageError> {
        if self.create_should_fail.load(Ordering::Relaxed) {
            return Err(StorageError::Connection("mock create failure".into()));
        }
        let now = Utc::now();
        let info = TradeInfo {
            trade_id: trade.trade_id.clone(),
            execution_id: trade.execution_id.clone(),
            opportunity_id: trade.opportunity_id.clone(),
            market_id: trade.market_id.clone(),
            event_id: trade.event_id.clone(),
            token_id: trade.token_id.clone(),
            side: trade.side,
            shares: trade.shares,
            price: trade.price,
            cost_usd: trade.cost_usd,
            fee_usd: trade.fee_usd,
            detected_edge_bps: trade.detected_edge_bps,
            detected_profit_usd: trade.detected_profit_usd,
            net_profit_usd: None,
            order_id: None,
            tx_hash: None,
            outcome: TradeOutcome::Pending,
            execution_mode: trade.execution_mode,
            latency_ms: None,
            error_message: None,
            confirmed_at: None,
            created_at: now,
            updated_at: now,
        };
        self.trades
            .lock()
            .unwrap()
            .insert(trade.trade_id.to_string(), info.clone());
        Ok(info)
    }

    async fn create_batch(&self, trades: Vec<NewTrade>) -> Result<u64, StorageError> {
        let mut count = 0;
        for trade in trades {
            self.create(trade).await?;
            count += 1;
        }
        Ok(count)
    }

    async fn update(
        &self,
        trade_id: &TradeId,
        update: UpdateTradeOutcome,
    ) -> Result<TradeInfo, StorageError> {
        let mut guard = self.trades.lock().unwrap();
        let existing = guard
            .get_mut(trade_id.as_str())
            .ok_or_else(|| StorageError::NotFound {
                entity: "trade",
                id: trade_id.to_string(),
            })?;
        existing.outcome = update.outcome;
        if let Some(shares) = update.shares {
            existing.shares = shares;
        }
        if let Some(price) = update.price {
            existing.price = price;
        }
        if let Some(cost_usd) = update.cost_usd {
            existing.cost_usd = cost_usd;
        }
        if let Some(fee_usd) = update.fee_usd {
            existing.fee_usd = fee_usd;
        }
        existing.order_id = update.order_id;
        existing.tx_hash = update.tx_hash;
        existing.net_profit_usd = update.net_profit_usd;
        existing.latency_ms = update.latency_ms;
        existing.error_message = update.error_message;
        existing.confirmed_at = update.confirmed_at;
        existing.updated_at = Utc::now();
        let updated = existing.clone();
        drop(guard);
        Ok(updated)
    }

    async fn find_by_id(&self, trade_id: &TradeId) -> Result<Option<TradeInfo>, StorageError> {
        Ok(self.find(trade_id))
    }

    async fn find_by_execution(&self, _execution_id: &str) -> Result<Vec<TradeInfo>, StorageError> {
        Ok(vec![])
    }

    async fn find_by_market(
        &self,
        _market_id: &MarketId,
        _limit: u64,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        Ok(vec![])
    }

    async fn find_recent(
        &self,
        _since: chrono::DateTime<Utc>,
        _limit: u64,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        Ok(vec![])
    }

    async fn count_by_outcome(
        &self,
        _since: chrono::DateTime<Utc>,
    ) -> Result<HashMap<String, i64>, StorageError> {
        Ok(HashMap::new())
    }
}

#[derive(Default)]
pub struct MockTimeseriesRepository {
    audits: Mutex<Vec<OpportunityAuditRow>>,
}

impl MockTimeseriesRepository {
    pub fn audit_rows(&self) -> Vec<OpportunityAuditRow> {
        self.audits.lock().unwrap().clone()
    }
}

#[async_trait]
impl TimeseriesRepository for MockTimeseriesRepository {
    async fn insert_tick_events(&self, _events: &[TickEventRow]) -> Result<(), StorageError> {
        Ok(())
    }

    async fn insert_book_snapshot(&self, _snapshot: &BookSnapshotRow) -> Result<(), StorageError> {
        Ok(())
    }

    async fn insert_opportunity_audit(
        &self,
        audit: &OpportunityAuditRow,
    ) -> Result<(), StorageError> {
        self.audits.lock().unwrap().push(audit.clone());
        Ok(())
    }

    async fn insert_calibration_snapshot(
        &self,
        _snapshot: &CalibrationSnapshotRow,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn insert_detection_batch(
        &self,
        _rows: &[OpportunityDetectionRow],
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn query_tick_events(
        &self,
        _token_id: &str,
        _from: chrono::DateTime<Utc>,
        _to: chrono::DateTime<Utc>,
        _limit: u64,
    ) -> Result<Vec<TickEventRow>, StorageError> {
        Ok(vec![])
    }

    async fn query_opportunity_audit(
        &self,
        _from: chrono::DateTime<Utc>,
        _to: chrono::DateTime<Utc>,
    ) -> Result<Vec<OpportunityAuditRow>, StorageError> {
        Ok(vec![])
    }

    async fn query_calibration_history(
        &self,
        _category: &str,
        _price_zone: &str,
        _duration_bucket: &str,
        _days: u32,
    ) -> Result<Vec<CalibrationSnapshotRow>, StorageError> {
        Ok(vec![])
    }
}
