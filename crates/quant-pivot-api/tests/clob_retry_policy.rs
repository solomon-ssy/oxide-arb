//! CLOB retry policy exercises `ApiError::RateLimited` end-to-end.

use std::sync::atomic::{AtomicU32, Ordering};

use quant_pivot_api::infra::retry::{ErrorKind, RetryPolicy, retry_with_policy};
use quant_pivot_error::api::ApiError;

#[tokio::test]
async fn rate_limited_error_retries_then_succeeds() {
    let attempts = AtomicU32::new(0);
    let policy = RetryPolicy::clob_default();

    let result = retry_with_policy(&policy, || {
        let n = attempts.fetch_add(1, Ordering::SeqCst);
        async move {
            if n == 0 {
                Err(ApiError::RateLimited {
                    retry_after_ms: 1,
                    bucket: "POST /order".into(),
                })
            } else {
                Ok(42)
            }
        }
    })
    .await;

    assert_eq!(result.unwrap(), 42);
    assert!(attempts.load(Ordering::SeqCst) >= 2);
}

#[test]
fn rate_limited_classified_transient() {
    let err = ApiError::RateLimited {
        retry_after_ms: 500,
        bucket: "test".into(),
    };
    assert_eq!(ErrorKind::from(&err), ErrorKind::Transient);
    assert!(err.is_retryable());
}
