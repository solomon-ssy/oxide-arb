//! Time abstraction for testable risk engine components.
//!
//! Production code uses [`UtcClock`]; tests inject [`FakeClock`] to control
//! time deterministically without `thread::sleep`.

use chrono::{DateTime, NaiveDate, Utc};
use std::sync::Arc;
#[cfg(any(test, feature = "test-support"))]
use std::sync::atomic::{AtomicI64, Ordering};

/// Abstraction over the system clock.
///
/// Every time-dependent component in the risk crate receives an
/// `Arc<dyn Clock>` at construction time. This makes rollover boundaries,
/// cooldown expiry, and TTL eviction fully testable.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> DateTime<Utc>;

    fn today(&self) -> NaiveDate {
        self.now().date_naive()
    }
}

/// Real UTC clock for production use.
#[derive(Debug, Clone, Copy)]
pub struct UtcClock;

impl Clock for UtcClock {
    #[inline]
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Deterministic clock for unit tests.
///
/// Internally stores a Unix timestamp (milliseconds) in an `AtomicI64`.
/// Callers advance time via [`FakeClock::advance`] or [`FakeClock::set`].
#[cfg(any(test, feature = "test-support"))]
pub struct FakeClock {
    millis: AtomicI64,
}

#[cfg(any(test, feature = "test-support"))]
impl FakeClock {
    /// Create a fake clock anchored at the given instant.
    pub const fn new(initial: DateTime<Utc>) -> Self {
        Self {
            millis: AtomicI64::new(initial.timestamp_millis()),
        }
    }

    /// Advance the clock by `duration`.
    pub fn advance(&self, duration: chrono::Duration) {
        self.millis
            .fetch_add(duration.num_milliseconds(), Ordering::SeqCst);
    }

    /// Jump the clock to an absolute instant.
    pub fn set(&self, instant: DateTime<Utc>) {
        self.millis
            .store(instant.timestamp_millis(), Ordering::SeqCst);
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Clock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        let ms = self.millis.load(Ordering::SeqCst);
        DateTime::from_timestamp_millis(ms).expect("valid timestamp")
    }
}

#[cfg(any(test, feature = "test-support"))]
impl std::fmt::Debug for FakeClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FakeClock({})", self.now())
    }
}

/// Convenience constructor for production wiring.
#[must_use]
pub fn utc_clock() -> Arc<dyn Clock> {
    Arc::new(UtcClock)
}
