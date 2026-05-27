use crate::service::{
    position_summary::PositionSummaryService, wallet_balance::WalletBalanceService,
};
use oxide_arb_models::{enums::common::MarketCategory, types::MarketId};
use oxide_arb_repository::traits::PositionRepository;
use oxide_arb_storage::cache::{CacheKey, TieredCache};
use std::sync::Arc;

/// Invalidate all caches derived from a Gamma catalog sync.
pub async fn invalidate_post_gamma_sync(cache: &TieredCache) {
    if let Err(e) = cache.invalidate(&CacheKey::ActiveMarkets).await {
        tracing::warn!(error = %e, "failed to invalidate active markets cache");
    }
    for category in MarketCategory::ALL_VARIANTS {
        if let Err(e) = cache.invalidate(&CacheKey::FeeParams { category }).await {
            tracing::warn!(
                category = %category,
                error = %e,
                "failed to invalidate fee params cache"
            );
        }
    }
}

pub struct CacheInvalidationCoordinator<R: PositionRepository> {
    position_summary: Arc<PositionSummaryService<R>>,
    wallet_balance: Arc<WalletBalanceService>,
    cache: Arc<TieredCache>,
}

impl<R: PositionRepository> CacheInvalidationCoordinator<R> {
    pub const fn new(
        position_summary: Arc<PositionSummaryService<R>>,
        wallet_balance: Arc<WalletBalanceService>,
        cache: Arc<TieredCache>,
    ) -> Self {
        Self {
            position_summary,
            wallet_balance,
            cache,
        }
    }

    pub async fn on_trade_filled(&self, market_id: &MarketId) {
        let _ = tokio::join!(
            self.position_summary.invalidate(market_id),
            self.wallet_balance.invalidate(),
            self.cache.invalidate(&CacheKey::RiskState),
        );
    }

    pub async fn on_trade_missed(&self) {
        let _ = self.wallet_balance.invalidate().await;
    }

    pub async fn on_market_settled(&self, market_id: &MarketId) {
        let _ = tokio::join!(
            self.position_summary.invalidate(market_id),
            self.wallet_balance.invalidate(),
        );
    }

    pub async fn on_gamma_sync(&self) {
        invalidate_post_gamma_sync(&self.cache).await;
    }
}
