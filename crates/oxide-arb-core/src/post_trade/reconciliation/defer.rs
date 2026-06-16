//! Exponential backoff for reconciliation deferrals.

use chrono::{DateTime, Duration, Utc};
use oxide_arb_models::runtime_config::ReconciliationConfig;

/// Compute the next defer-until timestamp after a reconciliation scan found
/// insufficient evidence.
#[must_use]
pub fn next_defer_until(
    config: &ReconciliationConfig,
    attempts_after_increment: i32,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    let attempt = u32::try_from(attempts_after_increment.max(1)).unwrap_or(1);
    let base = config.backoff_base_secs.max(1);
    let max = config.backoff_max_secs.max(base);
    let exponent = attempt.saturating_sub(1).min(16);
    let delay_secs = (base.saturating_mul(1u64 << exponent)).min(max);
    let delay_i64 = i64::try_from(delay_secs).unwrap_or(i64::MAX);
    now + Duration::seconds(delay_i64)
}
