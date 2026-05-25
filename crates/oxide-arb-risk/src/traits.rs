//! Dependency injection traits for the risk engine.
//!
//! All external dependencies are injected through three traits.
//! `oxide-arb-risk` does **not** implement these — implementations
//! live in `oxide-arb-core` (Phase 4.2) or test mocks.

use std::time::Duration;

use oxide_arb_error::OxideResult;
use oxide_arb_error::reservation::ReservationError;
use oxide_arb_models::domain::blacklist::{BlacklistInfo, UpsertBlacklistEntry};
use oxide_arb_models::domain::position::PositionInfo;
use oxide_arb_models::domain::risk::{
    NewEmergencySnapshot, NewReconciliationReport, NewRiskAuditEvent, RiskStateInfo,
    UpsertRiskEngineState,
};
use oxide_arb_models::enums::common::Side;
use oxide_arb_models::types::{MarketId, ReservationId, Usd};

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
