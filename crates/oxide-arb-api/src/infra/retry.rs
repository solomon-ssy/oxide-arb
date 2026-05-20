//! Retry strategy built on the [`backoff`] crate's [`ExponentialBackoff`].
//!
//! [`RetryPolicy`] is the serializable configuration object shared across the
//! codebase.  [`RetryController`] wraps `ExponentialBackoff` and enforces
//! **both** count-based (`max_attempts`) and time-based (`max_elapsed_time`)
//! budgets.
//!
//! [`retry_with_policy`] is a convenience async executor that drives the full
//! retry loop, classifying each error and applying backoff.

use backoff::{ExponentialBackoff, backoff::Backoff};
use oxide_arb_error::api::ApiError;
use oxide_arb_models::enums::common::OrderType;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, future::Future, time::Duration};

// ─── Error classification ───────────────────────────────────────────────────

/// Classification of errors for retry decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Transient error — safe to retry (network timeout, 429, 503).
    Transient,
    /// Permanent error — do NOT retry (400, 401, 403, invalid signature).
    Permanent,
}

impl From<&ApiError> for ErrorKind {
    fn from(err: &ApiError) -> Self {
        if err.is_retryable() {
            Self::Transient
        } else {
            Self::Permanent
        }
    }
}

impl From<ApiError> for ErrorKind {
    fn from(err: ApiError) -> Self {
        Self::from(&err)
    }
}

// ─── RetryPolicy ────────────────────────────────────────────────────────────

/// Configurable retry policy with exponential backoff and jitter.
///
/// # Presets
///
/// | Preset | Attempts | Initial | Max interval | Use case |
/// |--------|----------|---------|-------------|----------|
/// | [`clob_default`](Self::clob_default) | 3 | 100 ms | 5 s | CLOB API calls |
/// | [`gamma_default`](Self::gamma_default) | 5 | 1 s | 30 s | Gamma REST polling |
/// | [`ws_reconnect`](Self::ws_reconnect) | ∞ | 1 s | 30 s | WebSocket shards |
/// | [`no_retry`](Self::no_retry) | 0 | — | — | FOK orders |
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (`None` = unlimited).
    pub max_attempts: Option<u32>,
    /// Initial retry interval in milliseconds.
    pub initial_interval_ms: u64,
    /// Maximum retry interval cap in milliseconds.
    pub max_interval_ms: u64,
    /// Randomization factor in `[0.0, 1.0]` (e.g. `0.25` means ±25% jitter).
    pub randomization_factor: f64,
    /// Multiplicative factor for each retry step. Typically `2.0`.
    pub multiplier: f64,
    /// Optional maximum total elapsed time in milliseconds.
    pub max_elapsed_time_ms: Option<u64>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: Some(3),
            initial_interval_ms: 1_000,
            max_interval_ms: 30_000,
            randomization_factor: 0.25,
            multiplier: 2.0,
            max_elapsed_time_ms: None,
        }
    }
}

impl RetryPolicy {
    /// CLOB API calls: 3 retries, 100 ms initial, 5 s cap, ±25% jitter.
    #[must_use]
    pub const fn clob_default() -> Self {
        Self {
            max_attempts: Some(3),
            initial_interval_ms: 100,
            max_interval_ms: 5_000,
            randomization_factor: 0.25,
            multiplier: 2.0,
            max_elapsed_time_ms: Some(15_000),
        }
    }

    /// Gamma REST polling: 5 retries, 1 s initial, 30 s cap.
    #[must_use]
    pub const fn gamma_default() -> Self {
        Self {
            max_attempts: Some(5),
            initial_interval_ms: 1_000,
            max_interval_ms: 30_000,
            randomization_factor: 0.25,
            multiplier: 2.0,
            max_elapsed_time_ms: Some(120_000),
        }
    }

    /// WebSocket reconnection: unlimited retries, 1 s initial, 30 s cap.
    #[must_use]
    pub const fn ws_reconnect() -> Self {
        Self {
            max_attempts: None,
            initial_interval_ms: 1_000,
            max_interval_ms: 30_000,
            randomization_factor: 0.20,
            multiplier: 2.0,
            max_elapsed_time_ms: None,
        }
    }

    /// Retry policy for a CLOB order type (FOK is never retried).
    #[must_use]
    pub const fn for_order_type(order_type: OrderType) -> Self {
        match order_type {
            OrderType::Fok => Self::no_retry(),
            OrderType::Gtc | OrderType::Gtd { expiration: _ } => Self::clob_default(),
        }
    }

    /// No retries — for FOK orders that must not be retried.
    #[must_use]
    pub const fn no_retry() -> Self {
        Self {
            max_attempts: Some(0),
            initial_interval_ms: 0,
            max_interval_ms: 0,
            randomization_factor: 0.0,
            multiplier: 1.0,
            max_elapsed_time_ms: None,
        }
    }

    /// Build the underlying [`ExponentialBackoff`] from this policy.
    #[must_use]
    fn build_backoff(&self) -> ExponentialBackoff {
        ExponentialBackoff {
            initial_interval: Duration::from_millis(self.initial_interval_ms.max(1)),
            max_interval: Duration::from_millis(self.max_interval_ms.max(self.initial_interval_ms)),
            randomization_factor: self.randomization_factor.clamp(0.0, 1.0),
            multiplier: self.multiplier.max(1.0),
            max_elapsed_time: self.max_elapsed_time_ms.map(Duration::from_millis),
            ..ExponentialBackoff::default()
        }
    }
}

// ─── RetryController ────────────────────────────────────────────────────────

/// Decision returned by [`RetryController::on_failure`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Retry after the given delay.
    RetryAfter(Duration),
    /// All retry budget exhausted — give up.
    Exhausted,
}

/// Stateful retry controller enforcing both count-based and time-based budgets.
#[derive(Debug, Clone)]
pub struct RetryController {
    backoff: ExponentialBackoff,
    max_attempts: Option<u32>,
    retries_used: u32,
}

impl RetryController {
    #[must_use]
    pub fn new(policy: &RetryPolicy) -> Self {
        Self {
            backoff: policy.build_backoff(),
            max_attempts: policy.max_attempts,
            retries_used: 0,
        }
    }

    /// Reset after a successful operation.
    pub fn reset(&mut self) {
        self.backoff.reset();
        self.retries_used = 0;
    }

    /// Process a failure and decide whether to retry.
    pub fn on_failure(&mut self) -> RetryDecision {
        if let Some(max) = self.max_attempts {
            if self.retries_used >= max {
                return RetryDecision::Exhausted;
            }
        }
        match self.backoff.next_backoff() {
            Some(dur) => {
                self.retries_used = self.retries_used.saturating_add(1);
                RetryDecision::RetryAfter(dur)
            }
            None => RetryDecision::Exhausted,
        }
    }

    pub const fn retries_used(&self) -> u32 {
        self.retries_used
    }
}

// ─── Convenience async executor ─────────────────────────────────────────────

/// Execute an async operation with the given [`RetryPolicy`].
///
/// Errors are classified via [`ErrorKind::from`] (`&E: Into<ErrorKind>`).
pub async fn retry_with_policy<F, Fut, T, E>(policy: &RetryPolicy, operation: F) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Display,
    for<'a> ErrorKind: From<&'a E>,
{
    let mut ctrl = RetryController::new(policy);

    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if ErrorKind::from(&err) == ErrorKind::Permanent {
                    tracing::warn!(error = %err, "Permanent error — not retrying");
                    return Err(err);
                }

                match ctrl.on_failure() {
                    RetryDecision::Exhausted => {
                        tracing::warn!(
                            retries_used = ctrl.retries_used(),
                            error = %err,
                            "Retry budget exhausted — giving up",
                        );
                        return Err(err);
                    }
                    RetryDecision::RetryAfter(delay) => {
                        tracing::warn!(
                            retries_used = ctrl.retries_used(),
                            backoff_ms = delay.as_millis(),
                            error = %err,
                            "Transient error — retrying after backoff",
                        );
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn controller_exhausts_after_max_attempts() {
        let policy = RetryPolicy {
            max_attempts: Some(2),
            initial_interval_ms: 10,
            max_interval_ms: 100,
            randomization_factor: 0.0,
            multiplier: 2.0,
            max_elapsed_time_ms: None,
        };
        let mut ctrl = RetryController::new(&policy);

        assert!(matches!(ctrl.on_failure(), RetryDecision::RetryAfter(_)));
        assert!(matches!(ctrl.on_failure(), RetryDecision::RetryAfter(_)));
        assert_eq!(ctrl.on_failure(), RetryDecision::Exhausted);
    }

    #[test]
    fn no_retry_exhausts_immediately() {
        let policy = RetryPolicy::no_retry();
        let mut ctrl = RetryController::new(&policy);
        assert_eq!(ctrl.on_failure(), RetryDecision::Exhausted);
    }

    #[test]
    fn reset_restores_budget() {
        let policy = RetryPolicy {
            max_attempts: Some(1),
            initial_interval_ms: 10,
            max_interval_ms: 100,
            randomization_factor: 0.0,
            multiplier: 2.0,
            max_elapsed_time_ms: None,
        };
        let mut ctrl = RetryController::new(&policy);
        assert!(matches!(ctrl.on_failure(), RetryDecision::RetryAfter(_)));
        assert_eq!(ctrl.on_failure(), RetryDecision::Exhausted);

        ctrl.reset();
        assert_eq!(ctrl.retries_used(), 0);
        assert!(matches!(ctrl.on_failure(), RetryDecision::RetryAfter(_)));
    }

    #[derive(Debug)]
    struct TestErr(ErrorKind, String);

    impl std::fmt::Display for TestErr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.1.fmt(f)
        }
    }

    impl From<&TestErr> for ErrorKind {
        fn from(err: &TestErr) -> Self {
            err.0
        }
    }

    #[tokio::test]
    async fn retry_succeeds_after_transient_failures() {
        tokio::time::pause();

        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = Arc::clone(&counter);

        let policy = RetryPolicy {
            max_attempts: Some(3),
            initial_interval_ms: 10,
            max_interval_ms: 1_000,
            randomization_factor: 0.0,
            multiplier: 2.0,
            max_elapsed_time_ms: None,
        };

        let result: Result<&str, TestErr> = retry_with_policy(&policy, || {
            let c = Arc::clone(&counter_clone);
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(TestErr(ErrorKind::Transient, format!("transient #{n}")))
                } else {
                    Ok("recovered")
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), "recovered");
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn api_error_maps_to_error_kind_via_from() {
        use oxide_arb_error::api::ApiError;

        let transient = ApiError::Timeout {
            operation: "gamma".into(),
            elapsed_ms: 5_000,
        };
        assert_eq!(ErrorKind::from(&transient), ErrorKind::Transient);

        let permanent = ApiError::Gamma {
            endpoint: "/events".into(),
            status: 400,
            body: "bad".into(),
        };
        assert_eq!(ErrorKind::from(permanent), ErrorKind::Permanent);
    }

    #[tokio::test]
    async fn retry_gives_up_on_permanent() {
        tokio::time::pause();
        let policy = RetryPolicy::clob_default();

        let result: Result<&str, TestErr> = retry_with_policy(&policy, || async {
            Err(TestErr(ErrorKind::Permanent, "bad request".to_owned()))
        })
        .await;
        assert_eq!(result.unwrap_err().1, "bad request");
    }
}
