//! Web-facing read port for venue account snapshots (live + persisted).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;

use crate::{
    domain::{AccountSnapshotInfo, LiveAccountSnapshot},
    types::{AccountSnapshotId, Usd},
};

/// Live venue account read result (not persisted).
#[derive(Debug, Clone)]
pub struct LiveAccountInfo {
    pub fetched_at: DateTime<Utc>,
    pub budget_cap_usd: Usd,
    pub snapshot: LiveAccountSnapshot,
}

#[async_trait]
pub trait AccountReadPort: Send + Sync {
    async fn find_snapshot_by_id(
        &self,
        id: &AccountSnapshotId,
    ) -> QuantResult<Option<AccountSnapshotInfo>>;

    async fn live_account(&self) -> QuantResult<LiveAccountInfo>;
}
