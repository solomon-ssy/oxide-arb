use oxide_arb_storage::cache::{CacheKey, TieredCache};

/// Invalidate catalog-derived cache entries after a Gamma sync.
pub async fn invalidate_post_gamma_sync(cache: &TieredCache) {
    if let Err(error) = cache.invalidate(&CacheKey::ActiveMarkets).await {
        tracing::warn!(%error, "failed to invalidate active markets cache");
    }
}
