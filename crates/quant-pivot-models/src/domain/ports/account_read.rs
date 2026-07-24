//! Web-facing read port for venue account snapshots (live + persisted).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;

use crate::{
    domain::{
        pagination::Paginated,
        quant::{
            AccountSnapshotInfo, EquitySnapshotInfo, EquitySnapshotQuery, LiveAccountSnapshot,
        },
    },
    types::{AccountSnapshotId, EquitySnapshotId, Usd},
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

    async fn latest_equity_snapshot(&self) -> QuantResult<Option<EquitySnapshotInfo>>;

    async fn find_equity_snapshot(
        &self,
        id: &EquitySnapshotId,
    ) -> QuantResult<Option<EquitySnapshotInfo>>;

    async fn equity_snapshots(
        &self,
        query: EquitySnapshotQuery,
    ) -> QuantResult<Paginated<EquitySnapshotInfo>>;
}
