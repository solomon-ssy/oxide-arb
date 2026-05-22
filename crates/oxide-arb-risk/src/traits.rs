//! Dependency injection traits for the risk engine.
//!
//! All external dependencies are injected through three traits.
//! `oxide-arb-risk` does **not** implement these — implementations
//! live in `oxide-arb-core` (Phase 4.2) or test mocks.

use crate::audit::RiskAuditEvent;
use crate::types::ReconciliationReport;
use oxide_arb_error::OxideResult;
use oxide_arb_models::domain::blacklist::BlacklistEntry;
use oxide_arb_models::domain::position::PositionInfo;
use oxide_arb_models::domain::risk::{EmergencySnapshot, RiskEngineSnapshot};
use oxide_arb_models::enums::common::Side;
use oxide_arb_models::types::{MarketId, Usd};

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
    /// Persist the full risk engine snapshot (crash recovery).
    async fn save_snapshot(&self, snapshot: &RiskEngineSnapshot) -> OxideResult<()>;

    /// Load the most recent snapshot (startup recovery).
    async fn load_snapshot(&self) -> OxideResult<Option<RiskEngineSnapshot>>;

    /// Persist a blacklist entry (add or update).
    async fn save_blacklist_entry(&self, entry: &BlacklistEntry) -> OxideResult<()>;

    /// Remove a blacklist entry by `market_id`.
    async fn remove_blacklist_entry(&self, market_id: &MarketId) -> OxideResult<()>;

    /// Load all active (non-expired) blacklist entries.
    async fn load_blacklist_entries(&self) -> OxideResult<Vec<BlacklistEntry>>;

    /// Persist an emergency snapshot for post-mortem analysis.
    async fn save_emergency_snapshot(&self, snapshot: &EmergencySnapshot) -> OxideResult<()>;

    /// Persist a reconciliation report.
    async fn save_reconciliation_report(&self, report: &ReconciliationReport) -> OxideResult<()>;

    /// Append an immutable audit event. This is not a best-effort log:
    /// critical state transitions and denied/allowed trade decisions require
    /// a durable audit record before they are acknowledged to the caller.
    async fn append_audit_event(&self, event: &RiskAuditEvent) -> OxideResult<()>;
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
