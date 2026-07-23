//! Deterministic bounded timing for settlement leases and durable retries.

use chrono::{DateTime, TimeDelta, Utc};
use quant_pivot_error::{QuantError, QuantResult, execution::ExecutionError};

pub fn deadline(
    now: DateTime<Utc>,
    seconds: u64,
    field: &'static str,
) -> QuantResult<DateTime<Utc>> {
    let seconds = i64::try_from(seconds).map_err(|source| ExecutionError::TimeConversion {
        field,
        value: seconds.to_string(),
        detail: source.to_string(),
    })?;
    now.checked_add_signed(TimeDelta::seconds(seconds))
        .ok_or_else(|| {
            QuantError::from(ExecutionError::TimeConversion {
                field,
                value: seconds.to_string(),
                detail: "deadline overflow".to_owned(),
            })
        })
}

pub fn retry_deadline(
    now: DateTime<Utc>,
    retry_count: i32,
    initial_seconds: u64,
    maximum_seconds: u64,
    identity: &str,
) -> QuantResult<DateTime<Utc>> {
    let exponent = u32::try_from(retry_count.max(0)).map_or(31, |value| value.min(31));
    let multiplier = 1_u64.checked_shl(exponent).map_or(u64::MAX, |value| value);
    let base = initial_seconds
        .saturating_mul(multiplier)
        .min(maximum_seconds);
    let jitter_bound = base / 5;
    let mut seed_material = identity.as_bytes().to_vec();
    seed_material.extend_from_slice(&retry_count.to_be_bytes());
    let digest = blake3::hash(&seed_material);
    let mut seed = [0_u8; 8];
    seed.copy_from_slice(&digest.as_bytes()[..8]);
    let jitter = if jitter_bound == 0 {
        0
    } else {
        u64::from_be_bytes(seed) % (jitter_bound + 1)
    };
    deadline(
        now,
        base.saturating_add(jitter).min(maximum_seconds),
        "polymarket.settlement.retry",
    )
}

#[must_use]
pub fn elapsed_ms_since(now: DateTime<Utc>, started_at: DateTime<Utc>) -> u64 {
    u64::try_from(
        now.signed_duration_since(started_at)
            .num_milliseconds()
            .max(0),
    )
    .unwrap_or(u64::MAX)
}
