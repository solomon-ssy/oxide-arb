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
        MarkRedeemedParams, NewPosition, NewTrade, PositionInfo, ReportTradeStats,
        SettlePositionParams, SettledPositionStats, TradeInfo, TradeObservation, UpdatePosition,
    },
    enums::common::{
        PositionStatus, RedeemStatus, SettlementAccountingStatus, SettlementTrigger,
        TradeBusinessOutcome, TradeState,
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

    async fn aggregate_settled_between(
        &self,
        _start: chrono::DateTime<Utc>,
        _end: chrono::DateTime<Utc>,
    ) -> Result<SettledPositionStats, StorageError> {
        Ok(SettledPositionStats {
            realized_pnl: Usd::ZERO,
            total_payout: Usd::ZERO,
            total_cost: Usd::ZERO,
            total_fees: Usd::ZERO,
            settled_position_count: 0,
            winning_position_count: 0,
            losing_position_count: 0,
            unsettled_position_count: u32::try_from(self.count_open().await?).unwrap_or(u32::MAX),
            failed_accounting_count: 0,
            largest_single_profit: Usd::ZERO,
            largest_single_loss: Usd::ZERO,
        })
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
            reservation_id: trade.reservation_id.clone(),
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
            state: TradeState::Intent,
            business_outcome: None,
            scored_snapshot: trade.scored_snapshot.clone(),
            category: trade.category,
            needs_reconcile: false,
            post_trade_claim_owner: None,
            post_trade_claimed_at: None,
            post_trade_attempts: 0,
            execution_mode: trade.execution_mode,
            latency_ms: None,
            error_message: None,
            submitted_at: None,
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

    async fn mark_submitted(
        &self,
        trade_id: &TradeId,
        submitted_at: chrono::DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let mut guard = self.trades.lock().unwrap();
        let existing = guard
            .get_mut(trade_id.as_str())
            .ok_or_else(|| StorageError::NotFound {
                entity: "trade",
                id: trade_id.to_string(),
            })?;
        if existing.state != TradeState::Intent {
            drop(guard);
            return Ok(false);
        }
        existing.state = TradeState::Submitted;
        existing.submitted_at = Some(submitted_at);
        existing.updated_at = Utc::now();
        drop(guard);
        Ok(true)
    }

    async fn mark_observed(
        &self,
        trade_id: &TradeId,
        observation: TradeObservation,
    ) -> Result<(), StorageError> {
        let mut guard = self.trades.lock().unwrap();
        let existing = guard
            .get_mut(trade_id.as_str())
            .ok_or_else(|| StorageError::NotFound {
                entity: "trade",
                id: trade_id.to_string(),
            })?;
        if existing.state != TradeState::Submitted {
            return Err(StorageError::StaleData(format!(
                "trade {trade_id} was not in submitted state"
            )));
        }
        existing.state = observation.state;
        existing.business_outcome = observation.state.business_outcome();
        existing.shares = observation.shares;
        existing.price = observation.price;
        existing.cost_usd = observation.cost_usd;
        existing.fee_usd = observation.fee_usd;
        existing.order_id = observation.order_id;
        existing.tx_hash = observation.tx_hash;
        existing.net_profit_usd = observation.net_profit_usd;
        existing.latency_ms = observation.latency_ms;
        existing.error_message = observation.error_message;
        existing.confirmed_at = Some(observation.confirmed_at);
        existing.updated_at = Utc::now();
        drop(guard);
        Ok(())
    }

    async fn claim_unprocessed(
        &self,
        limit: u64,
        owner: &str,
        claimed_at: chrono::DateTime<Utc>,
        lease_expired_before: chrono::DateTime<Utc>,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        let mut guard = self.trades.lock().unwrap();
        let mut claimable: Vec<_> = guard
            .values()
            .filter(|trade| {
                trade.state.is_unprocessed()
                    || (trade.state.is_processing()
                        && trade.post_trade_claimed_at < Some(lease_expired_before))
            })
            .cloned()
            .collect();
        claimable.sort_by_key(|trade| trade.created_at);
        claimable.truncate(usize::try_from(limit).unwrap_or(usize::MAX));

        let mut claimed = Vec::with_capacity(claimable.len());
        for trade in claimable {
            let Some(existing) = guard.get_mut(trade.trade_id.as_str()) else {
                continue;
            };
            existing.state = match existing.state {
                TradeState::FillObserved | TradeState::FillProcessing => TradeState::FillProcessing,
                TradeState::MissObserved | TradeState::MissProcessing => TradeState::MissProcessing,
                TradeState::FailObserved | TradeState::FailProcessing => TradeState::FailProcessing,
                state => state,
            };
            existing.business_outcome = existing.state.business_outcome();
            existing.post_trade_claim_owner = Some(owner.to_owned());
            existing.post_trade_claimed_at = Some(claimed_at);
            existing.post_trade_attempts = existing.post_trade_attempts.saturating_add(1);
            existing.updated_at = claimed_at;
            claimed.push(existing.clone());
        }
        drop(guard);
        Ok(claimed)
    }

    async fn advance_state(
        &self,
        trade_id: &TradeId,
        from: TradeState,
        to: TradeState,
    ) -> Result<bool, StorageError> {
        let mut guard = self.trades.lock().unwrap();
        let existing = guard
            .get_mut(trade_id.as_str())
            .ok_or_else(|| StorageError::NotFound {
                entity: "trade",
                id: trade_id.to_string(),
            })?;
        if existing.state != from {
            drop(guard);
            return Ok(false);
        }
        existing.state = to;
        existing.business_outcome = to.business_outcome();
        existing.post_trade_claim_owner = None;
        existing.post_trade_claimed_at = None;
        existing.updated_at = Utc::now();
        drop(guard);
        Ok(true)
    }

    async fn mark_orphaned(&self, trade_id: &TradeId) -> Result<bool, StorageError> {
        let mut guard = self.trades.lock().unwrap();
        let existing = guard
            .get_mut(trade_id.as_str())
            .ok_or_else(|| StorageError::NotFound {
                entity: "trade",
                id: trade_id.to_string(),
            })?;
        if existing.state != TradeState::Submitted {
            drop(guard);
            return Ok(false);
        }
        existing.state = TradeState::Orphaned;
        existing.business_outcome = Some(TradeBusinessOutcome::Failed);
        existing.needs_reconcile = true;
        existing.updated_at = Utc::now();
        drop(guard);
        Ok(true)
    }

    async fn find_stale_submitted(
        &self,
        older_than: chrono::DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        let mut stale: Vec<_> = self
            .trades
            .lock()
            .unwrap()
            .values()
            .filter(|trade| {
                trade.state == TradeState::Submitted && trade.submitted_at < Some(older_than)
            })
            .cloned()
            .collect();
        stale.sort_by_key(|trade| trade.submitted_at);
        stale.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        Ok(stale)
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

    async fn aggregate_between(
        &self,
        _start: chrono::DateTime<Utc>,
        _end: chrono::DateTime<Utc>,
    ) -> Result<ReportTradeStats, StorageError> {
        Ok(ReportTradeStats {
            trade_count: 0,
            success_count: 0,
            miss_count: 0,
            failed_count: 0,
            total_fill_cost: Usd::ZERO,
            total_fill_fees: Usd::ZERO,
            fill_expected_pnl: Usd::ZERO,
        })
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

    async fn query_opportunity_lifecycle(
        &self,
        opportunity_id: &str,
    ) -> Result<Vec<OpportunityAuditRow>, StorageError> {
        Ok(self
            .audits
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row.opportunity_id == opportunity_id)
            .cloned()
            .collect())
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
