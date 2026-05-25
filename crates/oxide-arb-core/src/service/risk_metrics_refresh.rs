//! Periodic refresh of risk metrics snapshot from CLOB balance and open positions.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::observability::metrics_hub::MetricsHub;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use oxide_arb_api::clob::ClobClient;
use oxide_arb_models::domain::position::PositionInfo;
use oxide_arb_models::enums::common::Side;
use oxide_arb_models::types::{MarketId, Usd};
use oxide_arb_repository::postgres::PgPositionRepository;
use oxide_arb_repository::traits::PositionRepository;

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

/// Sliding-window tracker for API request/error counts.
///
/// Uses a two-bucket rotation scheme: when the current window expires,
/// `current` is swapped into `previous` and a fresh window starts.
pub struct ApiHealthTracker {
    current_requests: AtomicU64,
    current_errors: AtomicU64,
    previous_requests: AtomicU64,
    previous_errors: AtomicU64,
    window_start: parking_lot::Mutex<Instant>,
    window_duration: Duration,
}

impl ApiHealthTracker {
    pub fn new(window_duration: Duration) -> Self {
        Self {
            current_requests: AtomicU64::new(0),
            current_errors: AtomicU64::new(0),
            previous_requests: AtomicU64::new(0),
            previous_errors: AtomicU64::new(0),
            window_start: parking_lot::Mutex::new(Instant::now()),
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

    pub fn requests_in_window(&self) -> u64 {
        self.maybe_rotate();
        self.current_requests.load(Ordering::Relaxed)
            + self.previous_requests.load(Ordering::Relaxed)
    }

    pub fn errors_in_window(&self) -> u64 {
        self.maybe_rotate();
        self.current_errors.load(Ordering::Relaxed) + self.previous_errors.load(Ordering::Relaxed)
    }

    fn maybe_rotate(&self) {
        let mut start = self.window_start.lock();
        if start.elapsed() >= self.window_duration {
            self.previous_requests.store(
                self.current_requests.swap(0, Ordering::Relaxed),
                Ordering::Relaxed,
            );
            self.previous_errors.store(
                self.current_errors.swap(0, Ordering::Relaxed),
                Ordering::Relaxed,
            );
            *start = Instant::now();
        }
    }
}

/// Shared mutable risk metrics state read by [`crate::bridge::risk_metrics::CoreRiskMetrics`].
pub struct RiskMetricsState {
    snapshot: ArcSwap<MetricsSnapshot>,
    pub api_tracker: Arc<ApiHealthTracker>,
    pub market_misses: Arc<DashMap<MarketId, AtomicU32>>,
    daily_buy_trades: AtomicU32,
    daily_sell_trades: AtomicU32,
    daily_rollover_date: parking_lot::Mutex<chrono::NaiveDate>,
}

impl RiskMetricsState {
    pub fn new(api_tracker: Arc<ApiHealthTracker>) -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(MetricsSnapshot::initial()),
            api_tracker,
            market_misses: Arc::new(DashMap::new()),
            daily_buy_trades: AtomicU32::new(0),
            daily_sell_trades: AtomicU32::new(0),
            daily_rollover_date: parking_lot::Mutex::new(chrono::Utc::now().date_naive()),
        }
    }

    pub fn cached_balance(&self) -> Usd {
        self.snapshot.load().cached_balance
    }

    pub fn open_position_count(&self) -> usize {
        self.snapshot.load().open_position_count
    }

    pub fn open_positions(&self) -> Vec<PositionInfo> {
        self.snapshot.load().positions.clone()
    }

    pub fn total_position_exposure(&self) -> Usd {
        self.snapshot.load().total_position_exposure
    }

    pub fn market_position_exposure(&self, market_id: &MarketId) -> Usd {
        self.snapshot
            .load()
            .market_position_exposures
            .get(market_id)
            .copied()
            .unwrap_or(Usd::ZERO)
    }

    pub fn open_directional_count(&self, side: Side) -> usize {
        let snap = self.snapshot.load();
        match side {
            Side::Buy => snap.open_buy_count,
            Side::Sell => snap.open_sell_count,
        }
    }

    pub fn daily_directional_trades(&self, side: Side) -> u32 {
        self.check_daily_rollover();
        match side {
            Side::Buy => self.daily_buy_trades.load(Ordering::Relaxed),
            Side::Sell => self.daily_sell_trades.load(Ordering::Relaxed),
        }
    }

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
    }
}

pub struct RiskMetricsRefreshService {
    state: Arc<RiskMetricsState>,
    clob_client: Arc<ClobClient>,
    position_repo: Arc<PgPositionRepository>,
    metrics: Arc<MetricsHub>,
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
        }
    }

    /// Refresh the `ArcSwap` snapshot from DB/API. Intended for a ~1s periodic task.
    pub async fn refresh(&self) {
        let balance = match self.clob_client.collateral_balance().await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "risk metrics refresh: balance fetch failed");
                self.metrics.metrics_refresh_failures.inc();
                Usd::ZERO
            }
        };

        let positions = match self.position_repo.find_open().await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "risk metrics refresh: position fetch failed");
                self.metrics.metrics_refresh_failures.inc();
                Vec::new()
            }
        };

        let mut total_exp = Usd::ZERO;
        let mut market_exps: HashMap<MarketId, Usd> = HashMap::new();
        let mut buy_count = 0usize;
        let mut sell_count = 0usize;

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
    }
}
