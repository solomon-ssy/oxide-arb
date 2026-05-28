//! In-memory repository mocks for integration tests and benchmarks.

use async_trait::async_trait;
use chrono::Utc;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    clickhouse::{
        BookSnapshotRow, CalibrationSnapshotRow, OpportunityAuditRow, OpportunityDetectionRow,
        TickEventRow,
    },
    domain::{NewTrade, TradeInfo, UpdateTradeOutcome},
    enums::common::TradeOutcome,
    types::{MarketId, TradeId},
};
use oxide_arb_repository::traits::{TimeseriesRepository, TradeRepository};
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
