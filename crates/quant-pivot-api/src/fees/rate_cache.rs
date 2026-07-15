//! ArcSwap-backed market fee schedule book.

use ahash::{HashMap, HashMapExt};
use chrono::{DateTime, Utc};
use quant_pivot_models::{domain::fee::MarketFeeSchedule, types::MarketId};
use std::sync::Arc;

/// Snapshot of fee parameters, atomically swapped on refresh.
#[derive(Debug, Clone)]
pub struct FeeSnapshot {
    pub market_schedules: HashMap<MarketId, Arc<MarketFeeSchedule>>,
    pub updated_at: DateTime<Utc>,
}

impl FeeSnapshot {
    /// Build an empty snapshot; every usable schedule must come from CLOB.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            market_schedules: HashMap::new(),
            updated_at: Utc::now(),
        }
    }
}
