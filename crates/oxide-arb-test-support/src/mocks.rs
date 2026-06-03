//! In-memory repository mocks for integration tests and benchmarks.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    clickhouse::{
        BookSnapshotRow, CalibrationSnapshotRow, OpportunityAuditRow, OpportunityDetectionRow,
        TickEventL2Row, TickEventRow,
    },
    domain::{
        CalibrationBucketInfo, CalibrationOutcomeInfo, MarkRedeemedParams, NewCalibrationOutcome,
        NewPosition, NewTrade, PositionInfo, PositionPatch, ReportTradeStats, SettlePositionParams,
        SettledPositionStats, TradeInfo, TradeObservation, UpsertCalibration,
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::{
            MarketCategory, PositionStatus, RedeemStatus, SettlementAccountingStatus,
            SettlementTrigger, TradeBusinessOutcome, TradeState,
        },
    },
    types::{MarketId, OpportunityId, PositionId, TokenId, TradeId, Usd},
};
use oxide_arb_repository::traits::{
    CalibrationRepository, EvidenceTimeseriesRepository, MarketFilter, PositionRepository,
    TimeWindow, TimeseriesFactWriter, TradeRepository,
};
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
    mark_submitted_should_fail: AtomicBool,
    mark_observed_should_fail: AtomicBool,
}

impl MockTradeRepository {
    pub fn fail_create(&self) {
        self.create_should_fail.store(true, Ordering::Relaxed);
    }

    pub fn fail_mark_submitted(&self) {
        self.mark_submitted_should_fail
            .store(true, Ordering::Relaxed);
    }

    pub fn fail_mark_observed(&self) {
        self.mark_observed_should_fail
            .store(true, Ordering::Relaxed);
    }

    pub fn trade_count(&self) -> usize {
        self.trades.lock().unwrap().len()
    }

    pub fn find(&self, trade_id: &TradeId) -> Option<TradeInfo> {
        self.trades.lock().unwrap().get(trade_id.as_str()).cloned()
    }

    pub fn trades_snapshot(&self) -> Vec<TradeInfo> {
        self.trades.lock().unwrap().values().cloned().collect()
    }
}

#[derive(Default)]
pub struct MockPositionRepository {
    positions: Mutex<HashMap<String, PositionInfo>>,
}

#[derive(Default)]
pub struct MockCalibrationRepository {
    outcomes: Mutex<HashMap<String, CalibrationOutcomeInfo>>,
}

impl MockCalibrationRepository {
    pub fn outcome_count(&self) -> usize {
        self.outcomes.lock().unwrap().len()
    }
}

impl MockPositionRepository {
    pub fn positions_snapshot(&self) -> Vec<PositionInfo> {
        self.positions.lock().unwrap().values().cloned().collect()
    }
}

#[async_trait]
impl CalibrationRepository for MockCalibrationRepository {
    async fn get_bucket(
        &self,
        _category: MarketCategory,
        _price_zone: PriceZone,
        _duration_bucket: DurationBucket,
    ) -> Result<Option<CalibrationBucketInfo>, StorageError> {
        Ok(None)
    }

    async fn get_buckets_by_category(
        &self,
        _category: MarketCategory,
    ) -> Result<Vec<CalibrationBucketInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn get_all_buckets(&self) -> Result<Vec<CalibrationBucketInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn upsert(
        &self,
        _bucket: UpsertCalibration,
    ) -> Result<CalibrationBucketInfo, StorageError> {
        Err(StorageError::Codec(
            "MockCalibrationRepository::upsert is not implemented".into(),
        ))
    }

    async fn create_outcome(
        &self,
        outcome: NewCalibrationOutcome,
    ) -> Result<CalibrationOutcomeInfo, StorageError> {
        let mut outcomes = self.outcomes.lock().unwrap();
        if let Some(existing) = outcomes.get(outcome.trade_id.as_str()) {
            return Ok(existing.clone());
        }
        let info = CalibrationOutcomeInfo {
            id: i64::try_from(outcomes.len()).unwrap_or(i64::MAX) + 1,
            trade_id: outcome.trade_id.clone(),
            opportunity_id: outcome.opportunity_id,
            market_id: outcome.market_id,
            category: outcome.category,
            price_zone: outcome.price_zone,
            duration_bucket: outcome.duration_bucket,
            predicted_yes: outcome.predicted_yes,
            actual_yes: outcome.actual_yes,
            entry_price: outcome.entry_price,
            confidence_at_entry: outcome.confidence_at_entry,
            convergence_secs: outcome.convergence_secs,
            resolved_at: outcome.resolved_at,
            created_at: Utc::now(),
        };
        outcomes.insert(outcome.trade_id.to_string(), info.clone());
        drop(outcomes);
        Ok(info)
    }

    async fn get_unresolved_outcomes(&self) -> Result<Vec<CalibrationOutcomeInfo>, StorageError> {
        Ok(self
            .outcomes
            .lock()
            .unwrap()
            .values()
            .filter(|outcome| outcome.actual_yes.is_none())
            .cloned()
            .collect())
    }

    async fn resolve_outcome(&self, outcome_id: i64, actual_yes: bool) -> Result<(), StorageError> {
        let mut outcomes = self.outcomes.lock().unwrap();
        if let Some(outcome) = outcomes
            .values_mut()
            .find(|outcome| outcome.id == outcome_id)
        {
            outcome.actual_yes = Some(actual_yes);
            outcome.resolved_at = Some(Utc::now());
        }
        drop(outcomes);
        Ok(())
    }
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

    async fn open_as_of(&self, at: DateTime<Utc>) -> Result<Vec<PositionInfo>, StorageError> {
        let mut positions: Vec<_> = self
            .positions
            .lock()
            .unwrap()
            .values()
            .filter(|position| {
                position.opened_at <= at
                    && position.closed_at.is_none_or(|closed_at| closed_at > at)
            })
            .cloned()
            .collect();
        positions.sort_by(|left, right| {
            left.opened_at
                .cmp(&right.opened_at)
                .then_with(|| left.position_id.as_str().cmp(right.position_id.as_str()))
        });
        Ok(positions)
    }

    async fn changed_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        let in_window = |at: DateTime<Utc>| at >= start && at <= end;
        let optional_in_window = |at: Option<DateTime<Utc>>| at.is_some_and(in_window);
        let mut positions: Vec<_> = self
            .positions
            .lock()
            .unwrap()
            .values()
            .filter(|position| {
                in_window(position.opened_at)
                    || optional_in_window(position.closed_at)
                    || optional_in_window(position.settled_at)
                    || optional_in_window(position.settlement_accounted_at)
            })
            .cloned()
            .collect();
        positions.sort_by(|left, right| {
            left.opened_at
                .cmp(&right.opened_at)
                .then_with(|| left.position_id.as_str().cmp(right.position_id.as_str()))
        });
        Ok(positions)
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
            position_id: position.position_id,
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
        _patch: PositionPatch,
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

    async fn mark_redeem_terminal(
        &self,
        position_id: &PositionId,
        attempts: u32,
        winning_token_id: &oxide_arb_models::types::TokenId,
        settlement_trigger: SettlementTrigger,
        reason: String,
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
        position.settlement_accounting_status = SettlementAccountingStatus::Failed;
        position.settlement_accounting_error = Some(reason.clone());
        position.redeem_terminal_reason = Some(reason);
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
        if self.mark_submitted_should_fail.load(Ordering::Relaxed) {
            return Err(StorageError::Connection(
                "mock mark_submitted failure".into(),
            ));
        }
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
        if self.mark_observed_should_fail.load(Ordering::Relaxed) {
            return Err(StorageError::Connection(
                "mock mark_observed failure".into(),
            ));
        }
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
        market_id: &MarketId,
        limit: u64,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        let mut trades: Vec<_> = self
            .trades
            .lock()
            .unwrap()
            .values()
            .filter(|trade| &trade.market_id == market_id)
            .cloned()
            .collect();
        trades.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.trade_id.as_str().cmp(left.trade_id.as_str()))
        });
        trades.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        Ok(trades)
    }

    async fn find_recent(
        &self,
        since: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        let mut trades: Vec<_> = self
            .trades
            .lock()
            .unwrap()
            .values()
            .filter(|trade| trade.created_at >= since)
            .cloned()
            .collect();
        trades.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.trade_id.as_str().cmp(left.trade_id.as_str()))
        });
        trades.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        Ok(trades)
    }

    async fn find_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<TradeInfo>, StorageError> {
        let mut trades: Vec<_> = self
            .trades
            .lock()
            .unwrap()
            .values()
            .filter(|trade| trade.created_at >= start && trade.created_at < end)
            .cloned()
            .collect();
        trades.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.trade_id.as_str().cmp(right.trade_id.as_str()))
        });
        Ok(trades)
    }

    async fn count_by_outcome(
        &self,
        _since: chrono::DateTime<Utc>,
    ) -> Result<HashMap<TradeBusinessOutcome, i64>, StorageError> {
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
impl TimeseriesFactWriter for MockTimeseriesRepository {
    async fn insert_tick_events(&self, _events: Vec<TickEventRow>) -> Result<(), StorageError> {
        Ok(())
    }

    async fn insert_l2_events(&self, _rows: Vec<TickEventL2Row>) -> Result<(), StorageError> {
        Ok(())
    }

    async fn insert_book_snapshots(&self, _rows: Vec<BookSnapshotRow>) -> Result<(), StorageError> {
        Ok(())
    }

    async fn insert_detections(
        &self,
        _rows: Vec<OpportunityDetectionRow>,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn insert_audits(&self, rows: Vec<OpportunityAuditRow>) -> Result<(), StorageError> {
        self.audits.lock().unwrap().extend(rows);
        Ok(())
    }

    async fn insert_calibration_snapshots(
        &self,
        _rows: Vec<CalibrationSnapshotRow>,
    ) -> Result<(), StorageError> {
        Ok(())
    }
}

#[async_trait]
impl EvidenceTimeseriesRepository for MockTimeseriesRepository {
    async fn tick_events(
        &self,
        _token_ids: &[TokenId],
        _window: TimeWindow,
        _limit: u64,
    ) -> Result<Vec<TickEventRow>, StorageError> {
        Ok(vec![])
    }

    async fn l2_events(
        &self,
        _token_ids: &[TokenId],
        _window: TimeWindow,
    ) -> Result<Vec<TickEventL2Row>, StorageError> {
        Ok(vec![])
    }

    async fn book_snapshots_before(
        &self,
        _token_ids: &[TokenId],
        _before: chrono::DateTime<Utc>,
        _limit_per_token: usize,
    ) -> Result<Vec<BookSnapshotRow>, StorageError> {
        Ok(vec![])
    }

    async fn detections(
        &self,
        _filter: MarketFilter,
        _window: TimeWindow,
    ) -> Result<Vec<OpportunityDetectionRow>, StorageError> {
        Ok(vec![])
    }

    async fn audits(
        &self,
        opportunity_ids: &[OpportunityId],
    ) -> Result<Vec<OpportunityAuditRow>, StorageError> {
        let ids = opportunity_ids
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        Ok(self
            .audits
            .lock()
            .unwrap()
            .iter()
            .filter(|row| ids.contains(&row.opportunity_id))
            .cloned()
            .collect())
    }

    async fn calibration_snapshots(
        &self,
        _window: TimeWindow,
    ) -> Result<Vec<CalibrationSnapshotRow>, StorageError> {
        Ok(vec![])
    }
}
