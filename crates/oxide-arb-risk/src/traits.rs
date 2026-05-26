//! Dependency injection traits for the risk engine.
//!
//! External dependencies are injected through these traits.
//! `oxide-arb-risk` does **not** implement them — implementations
//! live in `oxide-arb-core` (Phase 4.2) or test mocks.

use std::sync::Arc;
use std::time::Duration;

use oxide_arb_error::OxideResult;
use oxide_arb_error::reservation::ReservationError;
use oxide_arb_models::domain::NewPotentialLoss;
use oxide_arb_models::domain::blacklist::{BlacklistInfo, UpsertBlacklistEntry};
use oxide_arb_models::domain::position::PositionInfo;
use oxide_arb_models::domain::potential_loss::PotentialLossInfo;
use oxide_arb_models::domain::risk::{
    NewEmergencySnapshot, NewReconciliationReport, NewRiskAuditEvent, RiskStateInfo,
    UpsertRiskEngineState,
};
use oxide_arb_models::enums::common::Side;
use oxide_arb_models::types::{LedgerId, MarketId, ReservationId, Usd};

/// Pre-loaded metrics for a single pre-trade decision (one batch read).
#[derive(Debug, Clone, Copy)]
pub struct RiskMetricsSnapshot {
    pub version: u64,
    pub cached_balance: Usd,
    pub total_exposure: Usd,
    pub market_exposure: Usd,
    pub open_position_count: usize,
    pub active_reservation_count: usize,
    pub reserved_usd: Usd,
    pub open_directional_count_buy: usize,
    pub open_directional_count_sell: usize,
    pub daily_directional_trades_buy: u32,
    pub daily_directional_trades_sell: u32,
    pub consecutive_market_misses: u32,
    pub ws_disconnect_secs: u64,
    pub api_error_count: u64,
    pub api_request_count: u64,
}

impl RiskMetricsSnapshot {
    #[must_use]
    pub const fn zeroed() -> Self {
        Self {
            version: 0,
            cached_balance: Usd::ZERO,
            total_exposure: Usd::ZERO,
            market_exposure: Usd::ZERO,
            open_position_count: 0,
            active_reservation_count: 0,
            reserved_usd: Usd::ZERO,
            open_directional_count_buy: 0,
            open_directional_count_sell: 0,
            daily_directional_trades_buy: 0,
            daily_directional_trades_sell: 0,
            consecutive_market_misses: 0,
            ws_disconnect_secs: 0,
            api_error_count: 0,
            api_request_count: 0,
        }
    }

    #[inline]
    pub const fn open_directional_count(&self, side: Side) -> usize {
        match side {
            Side::Buy => self.open_directional_count_buy,
            Side::Sell => self.open_directional_count_sell,
        }
    }

    #[inline]
    pub const fn daily_directional_trades(&self, side: Side) -> u32 {
        match side {
            Side::Buy => self.daily_directional_trades_buy,
            Side::Sell => self.daily_directional_trades_sell,
        }
    }
}

/// Read-only accessor for live system metrics required by risk checks.
///
/// Implementations must be `Send + Sync` and safe for concurrent access.
/// All monetary amounts use `Usd` (never `f64`). Methods are intentionally
/// synchronous — implementors should cache aggressively and never block
/// on I/O in these methods.
pub trait RiskMetrics: Send + Sync + 'static {
    /// Current total portfolio exposure across all open positions and
    /// pending reservations (USD).
    fn total_exposure(&self) -> Usd;

    /// Exposure in a single market (positions + reservations, USD).
    fn market_exposure(&self, market_id: &MarketId) -> Usd;

    /// Number of currently open positions.
    fn open_position_count(&self) -> usize;

    /// All open positions as a snapshot.
    fn open_positions(&self) -> Vec<PositionInfo>;

    /// Last known platform balance (USDC.e on Polygon), cached.
    fn cached_balance(&self) -> Usd;

    /// Count of active exposure reservations.
    fn active_reservation_count(&self) -> usize;

    /// Total USD currently locked in pending reservations.
    fn reserved_usd(&self) -> Usd;

    /// Number of currently open positions in a given directional side
    /// across the entire portfolio.
    fn open_directional_count(&self, side: Side) -> usize;

    /// Number of trades executed today in a given directional side.
    fn daily_directional_trades(&self, side: Side) -> u32;

    /// Count of consecutive misses for a specific market (for auto-blacklist).
    fn consecutive_market_misses(&self, market_id: &MarketId) -> u32;

    /// Seconds since last successful WebSocket heartbeat.
    fn ws_disconnect_secs(&self) -> u64;

    /// Rolling-window API error count (recent window, e.g. 5 minutes).
    fn api_error_count(&self) -> u64;

    /// Rolling-window API request count (same window as error count).
    fn api_request_count(&self) -> u64;

    /// Batch-load all live metrics for one pre-trade check.
    ///
    /// Default implementation delegates to individual accessors; production
    /// implementations should override with a single snapshot read.
    fn snapshot_for(&self, market_id: &MarketId) -> RiskMetricsSnapshot {
        RiskMetricsSnapshot {
            version: 0,
            cached_balance: self.cached_balance(),
            total_exposure: self.total_exposure(),
            market_exposure: self.market_exposure(market_id),
            open_position_count: self.open_position_count(),
            active_reservation_count: self.active_reservation_count(),
            reserved_usd: self.reserved_usd(),
            open_directional_count_buy: self.open_directional_count(Side::Buy),
            open_directional_count_sell: self.open_directional_count(Side::Sell),
            daily_directional_trades_buy: self.daily_directional_trades(Side::Buy),
            daily_directional_trades_sell: self.daily_directional_trades(Side::Sell),
            consecutive_market_misses: self.consecutive_market_misses(market_id),
            ws_disconnect_secs: self.ws_disconnect_secs(),
            api_error_count: self.api_error_count(),
            api_request_count: self.api_request_count(),
        }
    }
}

/// Async persistence interface for risk engine state.
///
/// Called by `RiskEngine` inside state transitions to ensure durability.
/// Critical mutations must be committed before this method returns `Ok`.
/// Returning `Err` means the engine must enter fail-closed mode.
#[async_trait::async_trait]
pub trait RiskPersistence: Send + Sync + 'static {
    /// Upsert the full risk engine state (crash recovery).
    async fn upsert_state(&self, state: UpsertRiskEngineState) -> OxideResult<()>;

    /// Load the most recent state snapshot (startup recovery).
    async fn load_state(&self) -> OxideResult<RiskStateInfo>;

    /// Upsert a blacklist entry (add or update).
    async fn upsert_blacklist(&self, entry: UpsertBlacklistEntry) -> OxideResult<()>;

    /// Remove a blacklist entry by `market_id`.
    async fn remove_blacklist(&self, market_id: &MarketId) -> OxideResult<()>;

    /// Load all active (non-expired) blacklist entries.
    async fn load_blacklist(&self) -> OxideResult<Vec<BlacklistInfo>>;

    /// Persist an emergency snapshot for post-mortem analysis.
    async fn create_emergency(&self, emergency: NewEmergencySnapshot) -> OxideResult<()>;

    /// Persist a reconciliation report.
    async fn create_reconciliation(&self, report: NewReconciliationReport) -> OxideResult<()>;

    /// Append an immutable audit event.
    async fn create_audit(&self, audit: NewRiskAuditEvent) -> OxideResult<()>;
}

// ── Potential Loss Persistence ───────────────────────────────────────────────

/// Async write-through persistence for the potential loss ledger.
///
/// Ensures that potential-loss entries survive process crashes. The risk
/// engine writes to PG **before** updating the in-memory ledger (PG-first
/// ordering). If PG write fails, the engine enters fail-closed halt.
#[async_trait::async_trait]
pub trait PotentialLossStore: Send + Sync + 'static {
    /// Persist a new potential-loss entry (Fill phase).
    async fn create(&self, entry: NewPotentialLoss) -> OxideResult<PotentialLossInfo>;

    /// Mark an entry as resolved (Settlement phase / position closed).
    async fn resolve(&self, ledger_id: &LedgerId) -> OxideResult<()>;

    /// Load all active (unresolved) entries — used during startup recovery.
    async fn find_active(&self) -> OxideResult<Vec<PotentialLossInfo>>;

    /// Find active entries older than `max_age` — used for escalation tick.
    async fn find_stale(&self, max_age: Duration) -> OxideResult<Vec<PotentialLossInfo>>;
}

// ── Exposure Reservation ────────────────────────────────────────────────────

/// Backend trait for exposure reservation management.
///
/// Implementations must be concurrency-safe — multiple execution workers
/// may call `try_reserve` simultaneously. The in-memory implementation
/// uses `AtomicU64` CAS loops; distributed implementations use Redis
/// atomic scripts.
#[async_trait::async_trait]
pub trait ExposureReservationBackend: Send + Sync + 'static {
    /// Attempt to reserve capital. Returns a reservation ID on success.
    ///
    /// This operation MUST be atomic: the `total_reserved` counter and the
    /// reservation entry must be updated together without race conditions.
    async fn try_reserve(
        &self,
        market_id: &MarketId,
        amount: Usd,
        ttl: Duration,
    ) -> Result<ReservationId, ReservationError>;

    /// Confirm a reservation (trade executed successfully).
    ///
    /// Releases the reservation from tracking — the exposure is now a
    /// real position tracked elsewhere.
    async fn confirm(&self, id: &ReservationId) -> Result<(), ReservationError>;

    /// Explicitly release a reservation (trade cancelled/failed).
    async fn release(&self, id: &ReservationId) -> Result<(), ReservationError>;

    /// Current total reserved amount in USD.
    async fn total_reserved_usd(&self) -> Usd;

    /// Number of active reservations.
    async fn active_count(&self) -> usize;

    /// Reserved amount for a specific market (USD).
    async fn per_market_reserved(&self, market_id: &MarketId) -> Usd;
}

/// Query the authoritative on-chain or exchange-side balance.
///
/// Separated from `RiskMetrics` because it involves actual I/O (API calls
/// or RPC) and may be slow. Called only during periodic reconciliation,
/// never on the hot trade path.
#[async_trait::async_trait]
pub trait BalanceQuerier: Send + Sync + 'static {
    /// Fetch the current USDC.e balance from the exchange/chain.
    /// Returns `(available_balance, locked_in_orders)`.
    async fn query_balance(&self) -> OxideResult<(Usd, Usd)>;

    /// Fetch per-market position values from the exchange.
    async fn query_positions(&self) -> OxideResult<Vec<(MarketId, Usd)>>;
}

#[async_trait::async_trait]
impl RiskPersistence for Arc<dyn RiskPersistence> {
    async fn upsert_state(&self, state: UpsertRiskEngineState) -> OxideResult<()> {
        (**self).upsert_state(state).await
    }

    async fn load_state(&self) -> OxideResult<RiskStateInfo> {
        (**self).load_state().await
    }

    async fn upsert_blacklist(&self, entry: UpsertBlacklistEntry) -> OxideResult<()> {
        (**self).upsert_blacklist(entry).await
    }

    async fn remove_blacklist(&self, market_id: &MarketId) -> OxideResult<()> {
        (**self).remove_blacklist(market_id).await
    }

    async fn load_blacklist(&self) -> OxideResult<Vec<BlacklistInfo>> {
        (**self).load_blacklist().await
    }

    async fn create_emergency(&self, emergency: NewEmergencySnapshot) -> OxideResult<()> {
        (**self).create_emergency(emergency).await
    }

    async fn create_reconciliation(&self, report: NewReconciliationReport) -> OxideResult<()> {
        (**self).create_reconciliation(report).await
    }

    async fn create_audit(&self, audit: NewRiskAuditEvent) -> OxideResult<()> {
        (**self).create_audit(audit).await
    }
}
