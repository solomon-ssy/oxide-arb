//! Shared, deterministic `n`-choose-`k` combination enumeration used by both
//! [`super::cpcv`] (test-partition combinations) and [`super::pbo`] (CSCV
//! in-sample/out-of-sample block combinations).

/// Every `k`-combination of `0..n`, in a fixed bitmask enumeration order
/// (Gosper's hack) — deterministic across runs and platforms. `n` must be
/// `<= 63` (a `u64` bitmask); every caller in this crate bounds `n` far below
/// that (partition/block counts are config-capped to double digits).
pub fn combinations(n: usize, k: usize) -> Vec<Vec<usize>> {
    if k == 0 || k > n {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut mask: u64 = (1 << k) - 1;
    let limit: u64 = 1 << n;
    while mask < limit {
        out.push((0..n).filter(|&i| mask & (1 << i) != 0).collect());
        let lowest = mask & mask.wrapping_neg();
        let next_lowest = mask + lowest;
        mask = (((next_lowest ^ mask) / lowest) >> 2) | next_lowest;
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::combinations;

    #[test]
    fn combinations_count_matches_binomial_coefficient() {
        assert_eq!(combinations(6, 3).len(), 20);
        assert_eq!(combinations(8, 4).len(), 70);
        assert_eq!(combinations(8, 2).len(), 28);
    }

    #[test]
    fn combinations_are_distinct_k_subsets() {
        let combos = combinations(5, 2);
        let mut seen = HashSet::new();
        for combo in &combos {
            assert_eq!(combo.len(), 2);
            assert!(
                seen.insert(combo.clone()),
                "duplicate combination {combo:?}"
            );
        }
    }

    #[test]
    fn empty_for_degenerate_inputs() {
        assert!(combinations(5, 0).is_empty());
        assert!(combinations(5, 6).is_empty());
    }
}
