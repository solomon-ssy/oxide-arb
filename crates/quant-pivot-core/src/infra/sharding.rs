//! Stable shard routing for book-apply and execution runners.
//!
//! Fixed-seed hasher — shard assignment stable across process restarts and CI.
const SHARD_SEED: ahash::RandomState = ahash::RandomState::with_seeds(
    0x9E37_79B9_7F4A_7C15,
    0xBF58_476D_1CE4_E5B9,
    0x94D0_49BB_1331_11EB,
    0x2545_F491_4F6C_DD1D,
);

/// Route `id` to a shard in `[0, shard_count)`.
#[inline]
pub fn shard_index(id: &str, shard_count: usize) -> usize {
    let shards = shard_count.max(1);
    let hash = SHARD_SEED.hash_one(id);
    usize::try_from(hash % u64::try_from(shards).unwrap_or(1)).unwrap_or(0)
}
