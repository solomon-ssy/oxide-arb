//! Shared statistics helpers for evidence materialization reports.

/// Return an integer percentile from a copied, sorted sample.
#[must_use]
pub fn percentile_i64(values: &[i64], pct: usize) -> Option<i64> {
    percentile(values, pct)
}

/// Return an integer percentile from a copied, sorted sample.
#[must_use]
pub fn percentile_u64(values: &[u64], pct: usize) -> Option<u64> {
    percentile(values, pct)
}

fn percentile<T>(values: &[T], pct: usize) -> Option<T>
where
    T: Copy + Ord,
{
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let idx = sorted
        .len()
        .saturating_sub(1)
        .saturating_mul(pct)
        .saturating_div(100);
    Some(sorted[idx])
}
