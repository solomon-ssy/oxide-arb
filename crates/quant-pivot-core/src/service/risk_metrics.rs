//! Mode-aware periodic refresh of the risk metrics snapshot.
//!
//! `Live` reads the authoritative CLOB collateral balance; `DryRun`/`Paper`
//! derive the simulated cash ledger from Postgres facts. Open positions are
//! always read from Postgres, scoped to the active execution mode.

use crate::{
    bridge::execution_mode::ExecutionModeHandle, observability::metrics_hub::MetricsHub,
    runtime_config::RuntimeConfigStore, service::equity_valuator::EquityValuator,
};
use arc_swap::ArcSwap;
use dashmap::DashMap;
use num_traits::ToPrimitive;
use oxide_arb_api::clob::ClobClient;
use oxide_arb_error::OxideError;
use oxide_arb_models::{
    domain::position::PositionInfo,
    enums::common::{ExecutionMode, Side},
    runtime_config::RiskConfig,
    types::{MarketId, Usd},
};
use oxide_arb_repository::{
    postgres::{PgPositionRepository, PgTradeRepository},
    traits::PositionRepository,
};
use std::{
    collections::HashMap,
    fmt::Display,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

struct MetricsSnapshot {
    cash_balance: Usd,
    positions: Vec<PositionInfo>,
    total_position_exposure: Usd,
    position_mark_value: Usd,
    equity: Usd,
    market_position_exposures: HashMap<MarketId, Usd>,
    open_position_count: usize,
    open_buy_count: usize,
    open_sell_count: usize,
    refresh_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskMetricsSource {
    NeverRefreshed = 0,
    AuthoritativeClob = 1,
    SimulatedPaper = 2,
    SimulatedDryRun = 3,
}

impl From<u8> for RiskMetricsSource {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::AuthoritativeClob,
            2 => Self::SimulatedPaper,
            3 => Self::SimulatedDryRun,
            _ => Self::NeverRefreshed,
        }
    }
}

impl From<RiskMetricsSource> for u8 {
    fn from(source: RiskMetricsSource) -> Self {
        source as Self
    }
}

impl MetricsSnapshot {
    fn initial() -> Self {
        Self {
            cash_balance: Usd::ZERO,
            positions: Vec::new(),
            total_position_exposure: Usd::ZERO,
            position_mark_value: Usd::ZERO,
            equity: Usd::ZERO,
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
    stale: AtomicBool,
    last_successful_refresh: AtomicU64,
    source: AtomicU8,
}

impl RiskMetricsState {
    pub fn new(api_tracker: Arc<ApiHealthTracker>) -> Self {
        let now_ms =
            ToPrimitive::to_u64(&chrono::Utc::now().timestamp_millis().max(0)).unwrap_or(0);
        Self {
            snapshot: ArcSwap::from_pointee(MetricsSnapshot::initial()),
            api_tracker,
            market_misses: Arc::new(DashMap::new()),
            daily_buy_trades: AtomicU32::new(0),
            daily_sell_trades: AtomicU32::new(0),
            daily_rollover_date: parking_lot::Mutex::new(chrono::Utc::now().date_naive()),
            stale: AtomicBool::new(true),
            last_successful_refresh: AtomicU64::new(now_ms),
            source: AtomicU8::new(RiskMetricsSource::NeverRefreshed as u8),
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
    pub fn source(&self) -> RiskMetricsSource {
        self.source.load(Ordering::Acquire).into()
    }

    #[inline]
    pub fn metrics_version(&self) -> u64 {
        self.snapshot.load().refresh_sequence
    }

    #[inline]
    pub fn cash_balance(&self) -> Usd {
        self.snapshot.load().cash_balance
    }

    #[inline]
    pub fn position_mark_value(&self) -> Usd {
        self.snapshot.load().position_mark_value
    }

    #[inline]
    pub fn equity(&self) -> Usd {
        self.snapshot.load().equity
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
            cash_balance: snap.cash_balance,
            position_mark_value: snap.position_mark_value,
            equity: snap.equity,
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

    fn store_snapshot(&self, snapshot: MetricsSnapshot, source: RiskMetricsSource) {
        self.snapshot.store(Arc::new(snapshot));
        self.stale.store(false, Ordering::Release);
        self.source.store(source.into(), Ordering::Release);
        self.last_successful_refresh.store(
            ToPrimitive::to_u64(&chrono::Utc::now().timestamp_millis().max(0)).unwrap_or(0),
            Ordering::Release,
        );
    }

    pub fn last_successful_refresh_ms(&self) -> u64 {
        self.last_successful_refresh.load(Ordering::Acquire)
    }

    /// Store an empty-position snapshot with the given cash and the source
    /// matching `mode`.
    ///
    /// Production paths hydrate snapshots exclusively through the mode-aware
    /// [`RiskMetricsRefreshService`]; this seeding shortcut exists for tests
    /// and benches that need a populated snapshot without Postgres.
    pub fn seed_simulated_snapshot(&self, mode: ExecutionMode, cash_balance: Usd) {
        let source = match mode {
            ExecutionMode::DryRun => RiskMetricsSource::SimulatedDryRun,
            ExecutionMode::Paper => RiskMetricsSource::SimulatedPaper,
            ExecutionMode::Live => RiskMetricsSource::AuthoritativeClob,
        };
        self.store_empty_position_snapshot(cash_balance, source);
    }

    /// Hot-reload reaction to a `risk.bankroll_usd` activation: rebase the
    /// simulated cash balance from the previous config to the next.
    ///
    /// Applies the bankroll delta to the live snapshot — positions and
    /// exposure are preserved, equity is recomputed — so exposure-percentage
    /// and balance checks see the new bankroll immediately. Authoritative
    /// CLOB snapshots are never touched: Live cash comes from the venue, not
    /// from configuration.
    pub fn reload(&self, previous: &RiskConfig, next: &RiskConfig) {
        let previous_bankroll = Usd::new(previous.bankroll_usd);
        let next_bankroll = Usd::new(next.bankroll_usd);
        if previous_bankroll == next_bankroll {
            return;
        }
        if !matches!(
            self.source(),
            RiskMetricsSource::SimulatedDryRun | RiskMetricsSource::SimulatedPaper
        ) {
            return;
        }
        let delta = next_bankroll - previous_bankroll;
        self.snapshot.rcu(|prev| {
            let cash_balance = prev.cash_balance + delta;
            Arc::new(MetricsSnapshot {
                cash_balance,
                positions: prev.positions.clone(),
                total_position_exposure: prev.total_position_exposure,
                position_mark_value: prev.position_mark_value,
                equity: cash_balance + prev.position_mark_value,
                market_position_exposures: prev.market_position_exposures.clone(),
                open_position_count: prev.open_position_count,
                open_buy_count: prev.open_buy_count,
                open_sell_count: prev.open_sell_count,
                refresh_sequence: prev.refresh_sequence + 1,
            })
        });
        tracing::info!(
            %previous_bankroll,
            %next_bankroll,
            "simulated cash balance rebased after bankroll activation"
        );
    }

    fn store_empty_position_snapshot(&self, cash_balance: Usd, source: RiskMetricsSource) {
        self.store_snapshot(
            MetricsSnapshot {
                cash_balance,
                positions: Vec::new(),
                total_position_exposure: Usd::ZERO,
                position_mark_value: Usd::ZERO,
                equity: cash_balance,
                market_position_exposures: HashMap::new(),
                open_position_count: 0,
                open_buy_count: 0,
                open_sell_count: 0,
                refresh_sequence: 1,
            },
            source,
        );
    }
}

/// Point-in-time copy of position metrics (single `ArcSwap` load).
#[derive(Debug, Clone, Copy)]
pub struct LoadedMetricsSnapshot {
    pub cash_balance: Usd,
    pub position_mark_value: Usd,
    pub equity: Usd,
    pub total_position_exposure: Usd,
    pub market_position_exposure: Usd,
    pub open_position_count: usize,
    pub open_buy_count: usize,
    pub open_sell_count: usize,
    pub daily_buy_trades: u32,
    pub daily_sell_trades: u32,
    pub consecutive_market_misses: u32,
}

/// Construction dependencies for [`RiskMetricsRefreshService`].
pub struct RiskMetricsRefreshDeps {
    pub state: Arc<RiskMetricsState>,
    pub execution_mode: ExecutionModeHandle,
    pub runtime_config: Arc<RuntimeConfigStore>,
    /// Optional: only the Live cash path needs the venue. Simulated modes
    /// refresh from Postgres alone, so the service works without credentials.
    pub clob_client: Option<Arc<ClobClient>>,
    pub trade_repo: Arc<PgTradeRepository>,
    pub position_repo: Arc<PgPositionRepository>,
    pub equity_valuator: Arc<EquityValuator>,
    pub metrics: Arc<MetricsHub>,
}

/// Mode-aware refresh of the [`RiskMetricsState`] snapshot.
///
/// Cash source per execution mode:
/// - **Live** — authoritative CLOB collateral balance (fails closed without a
///   `ClobClient`).
/// - **`DryRun` / `Paper`** — derived simulated ledger
///   `bankroll − successful spend(mode) + settlement payout(mode)`, fully
///   recomputable from Postgres so simulated funds evolve with simulated
///   fills and never touch the venue.
pub struct RiskMetricsRefreshService {
    state: Arc<RiskMetricsState>,
    execution_mode: ExecutionModeHandle,
    runtime_config: Arc<RuntimeConfigStore>,
    clob_client: Option<Arc<ClobClient>>,
    trade_repo: Arc<PgTradeRepository>,
    position_repo: Arc<PgPositionRepository>,
    equity_valuator: Arc<EquityValuator>,
    metrics: Arc<MetricsHub>,
}

impl RiskMetricsRefreshService {
    #[must_use]
    pub fn new(deps: RiskMetricsRefreshDeps) -> Self {
        Self {
            state: deps.state,
            execution_mode: deps.execution_mode,
            runtime_config: deps.runtime_config,
            clob_client: deps.clob_client,
            trade_repo: deps.trade_repo,
            position_repo: deps.position_repo,
            equity_valuator: deps.equity_valuator,
            metrics: deps.metrics,
        }
    }

    /// Refresh the snapshot for the currently active execution mode.
    pub async fn refresh(&self) -> Result<(), OxideError> {
        let mode = self.execution_mode.current();
        let (cash_balance, source) = match mode {
            ExecutionMode::Live => (
                self.fetch_live_cash().await?,
                RiskMetricsSource::AuthoritativeClob,
            ),
            ExecutionMode::DryRun => (
                self.derive_simulated_cash(mode).await?,
                RiskMetricsSource::SimulatedDryRun,
            ),
            ExecutionMode::Paper => (
                self.derive_simulated_cash(mode).await?,
                RiskMetricsSource::SimulatedPaper,
            ),
        };
        self.store_position_snapshot(mode, cash_balance, source)
            .await
    }

    /// Live cash: authoritative CLOB collateral balance.
    async fn fetch_live_cash(&self) -> Result<Usd, OxideError> {
        let Some(clob_client) = &self.clob_client else {
            self.note_refresh_failure(&"ClobClient unavailable", "Live cash refresh");
            return Err(OxideError::Internal(
                "Live risk metrics refresh requires a ClobClient".into(),
            ));
        };
        match clob_client.collateral_balance().await {
            Ok(balance) => {
                self.state.api_tracker.record_request(true);
                Ok(balance)
            }
            Err(error) => {
                self.state.api_tracker.record_request(false);
                self.note_refresh_failure(&error, "CLOB balance fetch failed");
                Err(OxideError::from(error))
            }
        }
    }

    /// Simulated cash: deterministic mode-scoped ledger derived from Postgres.
    async fn derive_simulated_cash(&self, mode: ExecutionMode) -> Result<Usd, OxideError> {
        let bankroll = Usd::new(self.runtime_config.load().risk.bankroll_usd);
        let spend = self
            .trade_repo
            .successful_spend_total(mode)
            .await
            .map_err(|error| {
                self.note_refresh_failure(&error, "spend aggregate failed");
                OxideError::from(error)
            })?;
        let payout = self
            .position_repo
            .settlement_payout_total(mode)
            .await
            .map_err(|error| {
                self.note_refresh_failure(&error, "payout aggregate failed");
                OxideError::from(error)
            })?;
        Ok(bankroll - spend + payout)
    }

    /// Aggregate mode-scoped open positions and publish the new snapshot.
    async fn store_position_snapshot(
        &self,
        mode: ExecutionMode,
        cash_balance: Usd,
        source: RiskMetricsSource,
    ) -> Result<(), OxideError> {
        let positions = self.position_repo.find_open(mode).await.map_err(|error| {
            self.note_refresh_failure(&error, "position fetch failed");
            OxideError::from(error)
        })?;

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

        let (position_mark_value, _) = self.equity_valuator.value(&positions, chrono::Utc::now());
        let equity = cash_balance + position_mark_value;
        let prev = self.state.snapshot.load();
        let seq = prev.refresh_sequence + 1;

        self.state.store_snapshot(
            MetricsSnapshot {
                cash_balance,
                open_position_count: positions.len(),
                open_buy_count: buy_count,
                open_sell_count: sell_count,
                total_position_exposure: total_exp,
                position_mark_value,
                equity,
                market_position_exposures: market_exps,
                positions,
                refresh_sequence: seq,
            },
            source,
        );
        Ok(())
    }

    /// Uniform failure observability: warn + bump the failure counter.
    fn note_refresh_failure(&self, error: &dyn Display, what: &'static str) {
        tracing::warn!(%error, what, "risk metrics refresh failed");
        self.metrics.metrics_refresh_failures.inc();
    }
}
