//! Periodic refresh of risk metrics snapshot from CLOB balance and open positions.

use crate::{bridge::risk_metrics::CoreRiskMetrics, observability::metrics_hub::MetricsHub};
use arc_swap::ArcSwap;
use dashmap::DashMap;
use num_traits::ToPrimitive;
use oxide_arb_api::clob::ClobClient;
use oxide_arb_error::OxideError;
use oxide_arb_models::{
    domain::position::PositionInfo,
    enums::common::Side,
    types::{MarketId, Usd},
};
use oxide_arb_repository::{postgres::PgPositionRepository, traits::PositionRepository};
use oxide_arb_risk::engine::RiskEngine;
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{
        Arc, atomic,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct MetricsSnapshot {
    cached_balance: Usd,
    positions: Vec<PositionInfo>,
    total_position_exposure: Usd,
    market_position_exposures: HashMap<MarketId, Usd>,
    open_position_count: usize,
    open_buy_count: usize,
    open_sell_count: usize,
    refresh_sequence: u64,
}

impl MetricsSnapshot {
    fn initial() -> Self {
        Self {
            cached_balance: Usd::ZERO,
            positions: Vec::new(),
            total_position_exposure: Usd::ZERO,
            market_position_exposures: HashMap::new(),
            open_position_count: 0,
            open_buy_count: 0,
            open_sell_count: 0,
            refresh_sequence: 0,
        }
    }
}

/// Sliding-window tracker for API request/error counts (lock-free reads).
pub struct ApiHealthTracker {
    current_requests: AtomicU64,
    current_errors: AtomicU64,
    previous_requests: AtomicU64,
    previous_errors: AtomicU64,
    window_start_ms: AtomicU64,
    window_duration: Duration,
}

impl ApiHealthTracker {
    pub fn new(window_duration: Duration) -> Self {
        Self {
            current_requests: AtomicU64::new(0),
            current_errors: AtomicU64::new(0),
            previous_requests: AtomicU64::new(0),
            previous_errors: AtomicU64::new(0),
            window_start_ms: AtomicU64::new(now_ms()),
            window_duration,
        }
    }

    pub fn record_request(&self, success: bool) {
        self.maybe_rotate();
        self.current_requests.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.current_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn requests_in_window(&self) -> u64 {
        self.maybe_rotate();
        self.previous_requests.load(Ordering::Relaxed)
            + self.current_requests.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn errors_in_window(&self) -> u64 {
        self.maybe_rotate();
        self.previous_errors.load(Ordering::Relaxed) + self.current_errors.load(Ordering::Relaxed)
    }

    fn maybe_rotate(&self) {
        let now = now_ms();
        let start = self.window_start_ms.load(Ordering::Relaxed);
        if now.saturating_sub(start)
            < ToPrimitive::to_u64(&self.window_duration.as_millis()).unwrap_or(u64::MAX)
        {
            return;
        }

        if self
            .window_start_ms
            .compare_exchange(start, now, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            self.previous_requests.store(
                self.current_requests.swap(0, Ordering::Relaxed),
                Ordering::Relaxed,
            );
            self.previous_errors.store(
                self.current_errors.swap(0, Ordering::Relaxed),
                Ordering::Relaxed,
            );
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| ToPrimitive::to_u64(&d.as_millis()))
        .unwrap_or(0)
}

/// Shared mutable risk metrics state read by [`crate::bridge::risk_metrics::CoreRiskMetrics`].
pub struct RiskMetricsState {
    snapshot: ArcSwap<MetricsSnapshot>,
    pub api_tracker: Arc<ApiHealthTracker>,
    pub market_misses: Arc<DashMap<MarketId, AtomicU32>>,
    daily_buy_trades: AtomicU32,
    daily_sell_trades: AtomicU32,
    daily_rollover_date: parking_lot::Mutex<chrono::NaiveDate>,
    stale: atomic::AtomicBool,
    last_successful_refresh: AtomicU64,
}

impl RiskMetricsState {
    pub fn new(api_tracker: Arc<ApiHealthTracker>) -> Self {
        let now_ms = ToPrimitive::to_u64(&Instant::now().elapsed().as_millis()).unwrap_or(0);
        Self {
            snapshot: ArcSwap::from_pointee(MetricsSnapshot::initial()),
            api_tracker,
            market_misses: Arc::new(DashMap::new()),
            daily_buy_trades: AtomicU32::new(0),
            daily_sell_trades: AtomicU32::new(0),
            daily_rollover_date: parking_lot::Mutex::new(chrono::Utc::now().date_naive()),
            stale: AtomicBool::new(true),
            last_successful_refresh: AtomicU64::new(now_ms),
        }
    }

    pub fn mark_stale(&self) {
        self.stale.store(true, Ordering::Release);
    }

    #[inline]
    pub fn is_stale(&self) -> bool {
        self.stale.load(Ordering::Acquire)
    }

    #[inline]
    pub fn metrics_version(&self) -> u64 {
        self.snapshot.load().refresh_sequence
    }

    #[inline]
    pub fn cached_balance(&self) -> Usd {
        self.snapshot.load().cached_balance
    }

    #[inline]
    pub fn open_position_count(&self) -> usize {
        self.snapshot.load().open_position_count
    }

    pub fn open_positions(&self) -> Vec<PositionInfo> {
        self.snapshot.load().positions.clone()
    }

    #[inline]
    pub fn total_position_exposure(&self) -> Usd {
        self.snapshot.load().total_position_exposure
    }

    #[inline]
    pub fn market_position_exposure(&self, market_id: &MarketId) -> Usd {
        self.snapshot
            .load()
            .market_position_exposures
            .get(market_id)
            .copied()
            .unwrap_or(Usd::ZERO)
    }

    #[inline]
    pub fn open_directional_count(&self, side: Side) -> usize {
        let snap = self.snapshot.load();
        match side {
            Side::Buy => snap.open_buy_count,
            Side::Sell => snap.open_sell_count,
        }
    }

    #[inline]
    pub fn daily_directional_trades(&self, side: Side) -> u32 {
        self.check_daily_rollover();
        match side {
            Side::Buy => self.daily_buy_trades.load(Ordering::Relaxed),
            Side::Sell => self.daily_sell_trades.load(Ordering::Relaxed),
        }
    }

    #[inline]
    pub fn load_metrics_snapshot(&self, market_id: &MarketId) -> LoadedMetricsSnapshot {
        self.check_daily_rollover();
        let snap = self.snapshot.load();
        LoadedMetricsSnapshot {
            cached_balance: snap.cached_balance,
            total_position_exposure: snap.total_position_exposure,
            market_position_exposure: snap
                .market_position_exposures
                .get(market_id)
                .copied()
                .unwrap_or(Usd::ZERO),
            open_position_count: snap.open_position_count,
            open_buy_count: snap.open_buy_count,
            open_sell_count: snap.open_sell_count,
            daily_buy_trades: self.daily_buy_trades.load(Ordering::Relaxed),
            daily_sell_trades: self.daily_sell_trades.load(Ordering::Relaxed),
            consecutive_market_misses: self.consecutive_market_misses(market_id),
        }
    }

    #[inline]
    pub fn consecutive_market_misses(&self, market_id: &MarketId) -> u32 {
        self.market_misses
            .get(market_id)
            .map_or(0, |v| v.load(Ordering::Relaxed))
    }

    /// Record a trade outcome for daily directional counters and market miss tracking.
    pub fn record_trade_outcome(&self, side: Side, market_id: &MarketId, was_miss: bool) {
        self.check_daily_rollover();
        match side {
            Side::Buy => {
                self.daily_buy_trades.fetch_add(1, Ordering::Relaxed);
            }
            Side::Sell => {
                self.daily_sell_trades.fetch_add(1, Ordering::Relaxed);
            }
        }
        if was_miss {
            self.market_misses
                .entry(market_id.clone())
                .or_insert_with(|| AtomicU32::new(0))
                .fetch_add(1, Ordering::Relaxed);
        } else if let Some(counter) = self.market_misses.get(market_id) {
            counter.store(0, Ordering::Relaxed);
        }
    }

    fn check_daily_rollover(&self) {
        let today = chrono::Utc::now().date_naive();
        let mut date = self.daily_rollover_date.lock();
        if *date != today {
            *date = today;
            self.daily_buy_trades.store(0, Ordering::Relaxed);
            self.daily_sell_trades.store(0, Ordering::Relaxed);
        }
        drop(date);
    }

    fn store_snapshot(&self, snapshot: MetricsSnapshot) {
        self.snapshot.store(Arc::new(snapshot));
        self.stale.store(false, Ordering::Release);
        self.last_successful_refresh.store(
            ToPrimitive::to_u64(&chrono::Utc::now().timestamp_millis().max(0)).unwrap_or(0),
            Ordering::Release,
        );
    }

    pub fn last_successful_refresh_ms(&self) -> u64 {
        self.last_successful_refresh.load(Ordering::Acquire)
    }

    /// Seed a minimal metrics snapshot for integration tests (no CLOB/DB refresh loop).
    #[doc(hidden)]
    pub fn seed_test_snapshot(&self, cached_balance: Usd) {
        self.store_snapshot(MetricsSnapshot {
            cached_balance,
            positions: Vec::new(),
            total_position_exposure: Usd::ZERO,
            market_position_exposures: HashMap::new(),
            open_position_count: 0,
            open_buy_count: 0,
            open_sell_count: 0,
            refresh_sequence: 1,
        });
    }
}

/// Point-in-time copy of position metrics (single `ArcSwap` load).
#[derive(Debug, Clone, Copy)]
pub struct LoadedMetricsSnapshot {
    pub cached_balance: Usd,
    pub total_position_exposure: Usd,
    pub market_position_exposure: Usd,
    pub open_position_count: usize,
    pub open_buy_count: usize,
    pub open_sell_count: usize,
    pub daily_buy_trades: u32,
    pub daily_sell_trades: u32,
    pub consecutive_market_misses: u32,
}

struct ReconciliationContext {
    risk_engine: Arc<RiskEngine>,
    risk_metrics: Arc<CoreRiskMetrics>,
    interval: Duration,
    last_run: Mutex<Instant>,
}

pub struct RiskMetricsRefreshService {
    state: Arc<RiskMetricsState>,
    clob_client: Arc<ClobClient>,
    position_repo: Arc<PgPositionRepository>,
    metrics: Arc<MetricsHub>,
    reconciliation: Option<ReconciliationContext>,
}

impl RiskMetricsRefreshService {
    pub const fn new(
        state: Arc<RiskMetricsState>,
        clob_client: Arc<ClobClient>,
        position_repo: Arc<PgPositionRepository>,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            state,
            clob_client,
            position_repo,
            metrics,
            reconciliation: None,
        }
    }

    /// Attach reconciliation so it piggybacks on the same CLOB+PG fetch.
    #[must_use]
    pub fn with_reconciliation(
        mut self,
        risk_engine: Arc<RiskEngine>,
        risk_metrics: Arc<CoreRiskMetrics>,
        reconciliation_interval_secs: u64,
    ) -> Self {
        self.reconciliation = Some(ReconciliationContext {
            risk_engine,
            risk_metrics,
            interval: Duration::from_secs(reconciliation_interval_secs.max(60)),
            last_run: Mutex::new(Instant::now()),
        });
        self
    }

    /// Refresh the snapshot from CLOB/PG. Called by the startup gate.
    pub async fn refresh(&self) -> Result<(), OxideError> {
        self.fetch_and_store_snapshot().await?;
        Ok(())
    }

    /// Refresh snapshot and, if the reconciliation interval has elapsed,
    /// run ledger reconciliation on the same fetched data — zero extra I/O.
    pub async fn refresh_and_maybe_reconcile(&self) -> Result<(), OxideError> {
        let (balance, ext_positions) = self.fetch_and_store_snapshot().await?;

        if let Some(ctx) = &self.reconciliation {
            let should_reconcile = ctx.last_run.lock().elapsed() >= ctx.interval;
            if should_reconcile {
                let reconciler = ctx.risk_engine.reconciler();
                let report = reconciler.reconcile_fetched(
                    ctx.risk_metrics.as_ref(),
                    balance,
                    Usd::ZERO,
                    &ext_positions,
                );
                if let Err(error) = ctx
                    .risk_engine
                    .on_reconciliation_result(&report, ctx.risk_metrics.as_ref())
                    .await
                {
                    tracing::error!(%error, "reconciliation result processing failed");
                }
                *ctx.last_run.lock() = Instant::now();
            }
        }
        Ok(())
    }

    async fn fetch_and_store_snapshot(&self) -> Result<(Usd, Vec<(MarketId, Usd)>), OxideError> {
        let balance = match self.clob_client.collateral_balance().await {
            Ok(b) => {
                self.state.api_tracker.record_request(true);
                b
            }
            Err(error) => {
                self.state.api_tracker.record_request(false);
                tracing::warn!(%error, "risk metrics refresh: balance fetch failed");
                self.metrics.metrics_refresh_failures.inc();
                return Err(OxideError::from(error));
            }
        };

        let positions = self.position_repo.find_open().await.map_err(|error| {
            tracing::warn!(%error, "risk metrics refresh: position fetch failed");
            self.metrics.metrics_refresh_failures.inc();
            OxideError::from(error)
        })?;

        let mut total_exp = Usd::ZERO;
        let mut market_exps: HashMap<MarketId, Usd> = HashMap::new();
        let mut buy_count = 0usize;
        let mut sell_count = 0usize;
        let ext_positions: Vec<(MarketId, Usd)> = positions
            .iter()
            .map(|p| (p.market_id.clone(), p.total_cost_usd))
            .collect();

        for p in &positions {
            total_exp += p.total_cost_usd;
            *market_exps.entry(p.market_id.clone()).or_insert(Usd::ZERO) += p.total_cost_usd;
            match p.side {
                Side::Buy => buy_count += 1,
                Side::Sell => sell_count += 1,
            }
        }

        let prev = self.state.snapshot.load();
        let seq = prev.refresh_sequence + 1;

        self.state.store_snapshot(MetricsSnapshot {
            cached_balance: balance,
            open_position_count: positions.len(),
            open_buy_count: buy_count,
            open_sell_count: sell_count,
            total_position_exposure: total_exp,
            market_position_exposures: market_exps,
            positions,
            refresh_sequence: seq,
        });
        Ok((balance, ext_positions))
    }
}
