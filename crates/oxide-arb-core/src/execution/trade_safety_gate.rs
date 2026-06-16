//! Guards automatic emergency recovery until durable trade state is safe.

use oxide_arb_error::storage::StorageError;
use oxide_arb_repository::traits::TradeRepository;
use std::sync::Arc;

/// Pre-trade safety gate based on durable trade rows blocking resumption.
pub struct TradeSafetyGate {
    trade_repo: Arc<dyn TradeRepository>,
}

impl TradeSafetyGate {
    pub const fn new(trade_repo: Arc<dyn TradeRepository>) -> Self {
        Self { trade_repo }
    }

    /// Returns `true` when submitted/orphaned/reconcile-pending trades exist.
    pub async fn has_blocking_trades(&self) -> Result<bool, StorageError> {
        let count = self.trade_repo.count_blocking_trades().await?;
        Ok(count > 0)
    }
}
