//! Retry strategy built on the [`backoff`] crate's [`ExponentialBackoff`].
//!
//! [`RetryPolicy`] is the serializable configuration object shared across the
//! codebase.  [`RetryController`] wraps `ExponentialBackoff` and enforces
//! **both** count-based (`max_attempts`) and time-based (`max_elapsed_time`)
//! budgets.
//!
//! [`retry_with_policy`] is a convenience async executor that drives the full
//! retry loop, classifying each error and applying backoff.

use std::{
    fmt::Display,
    future::Future,
    time::{Duration, Instant},
};

use backoff::{ExponentialBackoff, backoff::Backoff};
use quant_pivot_error::api::ApiError;
use serde::{Deserialize, Serialize};

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

/// Error contract consumed by the retry executor.
pub trait RetryableError: Display {
    fn error_kind(&self) -> ErrorKind;

    fn retry_after(&self) -> Option<Duration> {
        None
    }
}

impl RetryableError for ApiError {
    fn error_kind(&self) -> ErrorKind {
        ErrorKind::from(self)
    }

    fn retry_after(&self) -> Option<Duration> {
        self.retry_after_ms().map(Duration::from_millis)
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
        let mut backoff = ExponentialBackoff {
            initial_interval: Duration::from_millis(self.initial_interval_ms.max(1)),
            max_interval: Duration::from_millis(self.max_interval_ms.max(self.initial_interval_ms)),
            randomization_factor: self.randomization_factor.clamp(0.0, 1.0),
            multiplier: self.multiplier.max(1.0),
            max_elapsed_time: self.max_elapsed_time_ms.map(Duration::from_millis),
            ..ExponentialBackoff::default()
        };
        backoff.reset();
        backoff
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
    max_elapsed_time: Option<Duration>,
    started_at: Instant,
    retries_used: u32,
}

impl RetryController {
    #[must_use]
    pub fn new(policy: &RetryPolicy) -> Self {
        Self {
            backoff: policy.build_backoff(),
            max_attempts: policy.max_attempts,
            max_elapsed_time: policy.max_elapsed_time_ms.map(Duration::from_millis),
            started_at: Instant::now(),
            retries_used: 0,
        }
    }

    /// Reset after a successful operation.
    pub fn reset(&mut self) {
        self.backoff.reset();
        self.started_at = Instant::now();
        self.retries_used = 0;
    }

    /// Process a failure and decide whether to retry.
    pub fn on_failure(&mut self) -> RetryDecision {
        self.on_failure_with_minimum(None)
    }

    /// Process a failure while honoring an upstream `Retry-After` lower bound.
    pub fn on_failure_with_minimum(&mut self, minimum: Option<Duration>) -> RetryDecision {
        if let Some(max) = self.max_attempts
            && self.retries_used >= max
        {
            return RetryDecision::Exhausted;
        }
        match self.backoff.next_backoff() {
            Some(backoff) => {
                let delay = minimum.map_or(backoff, |minimum| minimum.max(backoff));
                if self
                    .max_elapsed_time
                    .is_some_and(|budget| self.started_at.elapsed().saturating_add(delay) > budget)
                {
                    return RetryDecision::Exhausted;
                }
                self.retries_used = self.retries_used.saturating_add(1);
                RetryDecision::RetryAfter(delay)
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
/// Errors provide classification and optional server retry hints through
/// [`RetryableError`].
pub async fn retry_with_policy<F, Fut, T, E>(policy: &RetryPolicy, operation: F) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: RetryableError,
{
    let mut ctrl = RetryController::new(policy);

    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if err.error_kind() == ErrorKind::Permanent {
                    tracing::warn!(error = %err, "Permanent error — not retrying");
                    return Err(err);
                }

                match ctrl.on_failure_with_minimum(err.retry_after()) {
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
    use std::{
        fmt::{Formatter, Result as FmtResult},
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering},
        },
    };

    use super::*;

    #[test]
    fn controller_exhausts_after_attempts() {
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

    #[test]
    fn server_retry_hint_budget() {
        let within_budget = RetryPolicy {
            max_attempts: Some(1),
            initial_interval_ms: 10,
            max_interval_ms: 10,
            randomization_factor: 0.0,
            multiplier: 1.0,
            max_elapsed_time_ms: Some(100),
        };
        let mut controller = RetryController::new(&within_budget);
        assert_eq!(
            controller.on_failure_with_minimum(Some(Duration::from_millis(50))),
            RetryDecision::RetryAfter(Duration::from_millis(50))
        );

        let outside_budget = RetryPolicy {
            max_elapsed_time_ms: Some(40),
            ..within_budget
        };
        let mut controller = RetryController::new(&outside_budget);
        assert_eq!(
            controller.on_failure_with_minimum(Some(Duration::from_millis(50))),
            RetryDecision::Exhausted
        );
    }

    #[derive(Debug)]
    struct TestErr(ErrorKind, String);

    impl Display for TestErr {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            self.1.fmt(f)
        }
    }

    impl From<&TestErr> for ErrorKind {
        fn from(err: &TestErr) -> Self {
            err.0
        }
    }

    impl RetryableError for TestErr {
        fn error_kind(&self) -> ErrorKind {
            self.0
        }
    }

    #[tokio::test]
    async fn retry_succeeds_after_failures() {
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
    fn api_error_error_via() {
        let transient = ApiError::Timeout {
            operation: "gamma".into(),
            elapsed_ms: 5_000,
        };
        assert_eq!(ErrorKind::from(&transient), ErrorKind::Transient);

        let permanent = ApiError::Gamma {
            endpoint: "/events".into(),
            status: 400,
            body: "bad".into(),
            retry_after_ms: None,
        };
        assert_eq!(ErrorKind::from(permanent), ErrorKind::Permanent);
    }

    #[tokio::test]
    async fn retry_gives_up_permanent() {
        tokio::time::pause();
        let policy = RetryPolicy::clob_default();

        let result: Result<&str, TestErr> = retry_with_policy(&policy, || async {
            Err(TestErr(ErrorKind::Permanent, "bad request".to_owned()))
        })
        .await;
        assert_eq!(result.unwrap_err().1, "bad request");
    }
}
