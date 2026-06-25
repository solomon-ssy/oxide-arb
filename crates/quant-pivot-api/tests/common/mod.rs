//! Shared helpers for wiremock integration tests.

use quant_pivot_api::infra::retry::RetryPolicy;
use reqwest::Client;

/// Fast retry policy for deterministic network-shaped tests.
pub const fn fast_retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: Some(2),
        initial_interval_ms: 1,
        max_interval_ms: 1,
        randomization_factor: 0.0,
        multiplier: 1.0,
        max_elapsed_time_ms: None,
    }
}

/// HTTP client that bypasses system proxy settings.
pub fn fast_http_client() -> Client {
    Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(0)
        .build()
        .expect("test reqwest client")
}
