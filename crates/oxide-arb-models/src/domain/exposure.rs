//! Exposure reservation types and backend trait.
//!
//! Defines the contract for pre-trade capital reservation to prevent
//! concurrent orders from exceeding exposure limits. The trait is
//! intentionally async to support both in-memory and distributed
//! (Redis-backed) implementations.

use crate::types::{MarketId, Usd};
use std::fmt;
use std::time::Duration;
use uuid::Uuid;

/// Unique identifier for a capital reservation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReservationId(String);

impl ReservationId {
    /// Generate a new unique reservation ID.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    /// Create from an existing string (e.g. loaded from DB).
    #[must_use]
    pub const fn from_string(s: String) -> Self {
        Self(s)
    }

    /// Access the inner string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for ReservationId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ReservationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Error returned when a reservation cannot be fulfilled.
#[derive(Debug, Clone)]
pub enum ReservationError {
    /// Requested amount would exceed the global exposure limit.
    ExceedsLimit {
        current_cents: u64,
        requested_cents: u64,
        max_cents: u64,
    },

    /// Reservation not found (already confirmed/released or expired).
    NotFound { id: String },

    /// Backend-specific error.
    Backend(String),
}

impl fmt::Display for ReservationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExceedsLimit {
                current_cents,
                requested_cents,
                max_cents,
            } => write!(
                f,
                "exposure limit exceeded: current={current_cents} requested={requested_cents} max={max_cents}"
            ),
            Self::NotFound { id } => write!(f, "reservation not found: {id}"),
            Self::Backend(msg) => write!(f, "reservation backend error: {msg}"),
        }
    }
}

impl std::error::Error for ReservationError {}

/// Configuration for the exposure reservation system.
#[derive(Debug, Clone)]
pub struct ExposureReservationConfig {
    /// Maximum total exposure across all active reservations (USD cents).
    pub max_total_exposure_cents: u64,
    /// Maximum exposure per market (USD cents).
    pub max_per_market_cents: u64,
    /// Default TTL for reservations (auto-expire if not confirmed/released).
    pub default_ttl: Duration,
    /// GC interval for cleaning expired reservations.
    pub gc_interval: Duration,
}

impl Default for ExposureReservationConfig {
    fn default() -> Self {
        Self {
            max_total_exposure_cents: 5_000_000, // $50,000
            max_per_market_cents: 1_000_000,     // $10,000
            default_ttl: Duration::from_secs(300),
            gc_interval: Duration::from_secs(30),
        }
    }
}

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
}
