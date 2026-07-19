//! Core implementation of [`AccountReadPort`] for the Admin API.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        AccountReadPort, AccountSnapshotInfo, EquitySnapshotInfo, EquitySnapshotQuery,
        LiveAccountInfo, LiveAccountSnapshot, Paginated, PolicySnapshotPort,
    },
    types::{AccountSnapshotId, EquitySnapshotId, Usd},
};
use quant_pivot_repository::traits::{AccountSnapshotRepository, EquitySnapshotRepository};
use quant_pivot_research::portfolio::AccountSnapshot;

use crate::service::account::AccountProviderFactory;

pub struct CoreAccountReadPort {
    snapshots: Arc<dyn AccountSnapshotRepository>,
    equity_snapshots: Arc<dyn EquitySnapshotRepository>,
    account_factory: Arc<AccountProviderFactory>,
    runtime_config: Arc<dyn PolicySnapshotPort>,
}

impl CoreAccountReadPort {
    #[must_use]
    pub const fn new(
        snapshots: Arc<dyn AccountSnapshotRepository>,
        equity_snapshots: Arc<dyn EquitySnapshotRepository>,
        account_factory: Arc<AccountProviderFactory>,
        runtime_config: Arc<dyn PolicySnapshotPort>,
    ) -> Self {
        Self {
            snapshots,
            equity_snapshots,
            account_factory,
            runtime_config,
        }
    }
}

#[async_trait]
impl AccountReadPort for CoreAccountReadPort {
    async fn find_snapshot_by_id(
        &self,
        id: &AccountSnapshotId,
    ) -> QuantResult<Option<AccountSnapshotInfo>> {
        self.snapshots.find_by_id(id).await.map_err(Into::into)
    }

    async fn live_account(&self) -> QuantResult<LiveAccountInfo> {
        let fetched_at = Utc::now();
        let config = self.runtime_config.current();
        let budget_cap = Usd::new(
            config
                .execution_risk
                .portfolio
                .budget
                .total_budget_usd
                .value,
        );
        let provider = self.account_factory.create(budget_cap)?;
        let snapshot = provider.snapshot(fetched_at).await?;
        Ok(LiveAccountInfo {
            fetched_at,
            budget_cap_usd: budget_cap,
            snapshot: live_account_snapshot(snapshot),
        })
    }

    async fn latest_equity_snapshot(&self) -> QuantResult<Option<EquitySnapshotInfo>> {
        self.equity_snapshots.latest().await.map_err(Into::into)
    }

    async fn find_equity_snapshot_by_id(
        &self,
        id: &EquitySnapshotId,
    ) -> QuantResult<Option<EquitySnapshotInfo>> {
        self.equity_snapshots
            .find_by_id(id)
            .await
            .map_err(Into::into)
    }

    async fn equity_snapshots(
        &self,
        query: EquitySnapshotQuery,
    ) -> QuantResult<Paginated<EquitySnapshotInfo>> {
        self.equity_snapshots.page(query).await.map_err(Into::into)
    }
}

fn live_account_snapshot(snapshot: AccountSnapshot) -> LiveAccountSnapshot {
    LiveAccountSnapshot::new(
        snapshot.as_of,
        snapshot.source,
        snapshot.venue_net_liquidation_usd,
        snapshot.capital_base_usd,
        snapshot.available_usd,
        snapshot.reserved_usd,
        snapshot.positions,
    )
}
