//! Per-endpoint rate limiter backed by the `governor` crate (GCRA algorithm).
//!
//! Each CLOB endpoint has its own rate limit bucket. The limiter is lock-free
//! (`AtomicU64` under the hood) and handles burst correctly.

use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use governor::{
    Quota, RateLimiter as GovLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};

type Limiter = GovLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// Per-endpoint rate limiter using `governor`'s GCRA algorithm.
///
/// Polymarket enforces per-endpoint rate limits. We proactively throttle
/// to avoid 429 responses. Limits are configured per endpoint.
pub struct RateLimiter {
    limiters: HashMap<&'static str, Arc<Limiter>>,
}

impl RateLimiter {
    /// Create with Polymarket's known endpoint rate limits.
    pub fn new() -> Self {
        let mut limiters = HashMap::new();

        // POST /order: 10 requests/second
        limiters.insert(
            "POST /order",
            Arc::new(GovLimiter::direct(Quota::per_second(
                NonZeroU32::new(10).expect("nonzero"),
            ))),
        );

        // DELETE /order: 20 requests/second
        limiters.insert(
            "DELETE /order",
            Arc::new(GovLimiter::direct(Quota::per_second(
                NonZeroU32::new(20).expect("nonzero"),
            ))),
        );

        // GET /book: 30 requests/second
        limiters.insert(
            "GET /book",
            Arc::new(GovLimiter::direct(Quota::per_second(
                NonZeroU32::new(30).expect("nonzero"),
            ))),
        );

        // GET /orders: 10 requests/second
        limiters.insert(
            "GET /orders",
            Arc::new(GovLimiter::direct(Quota::per_second(
                NonZeroU32::new(10).expect("nonzero"),
            ))),
        );

        // GET /balance-allowance: 5 requests/second (reconciliation only, not hot path)
        limiters.insert(
            "GET /balance-allowance",
            Arc::new(GovLimiter::direct(Quota::per_second(
                NonZeroU32::new(5).expect("nonzero"),
            ))),
        );

        Self { limiters }
    }

    /// Wait until a request to the given endpoint is permitted.
    ///
    /// If the endpoint is unknown, allows immediately (no throttling).
    pub async fn acquire(&self, endpoint: &'static str) {
        if let Some(limiter) = self.limiters.get(endpoint) {
            limiter.until_ready().await;
        }
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquire_unknown_endpoint_is_immediate() {
        let rl = RateLimiter::new();
        rl.acquire("UNKNOWN /endpoint").await;
    }

    #[tokio::test]
    async fn acquire_known_endpoint_succeeds() {
        let rl = RateLimiter::new();
        rl.acquire("POST /order").await;
        rl.acquire("GET /book").await;
    }
}
