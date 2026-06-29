//! Core implementation of [`AccountReadPort`] for the Admin API.

use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        AccountReadPort, AccountSnapshotInfo, LiveAccountInfo, LiveAccountSnapshot,
        RuntimeConfigPort,
    },
    types::{AccountSnapshotId, Usd},
};
use quant_pivot_repository::traits::AccountSnapshotRepository;
use quant_pivot_research::portfolio::AccountSnapshot;
use rust_decimal::Decimal;

use crate::service::account::AccountProviderFactory;

pub struct CoreAccountReadPort {
    snapshots: Arc<dyn AccountSnapshotRepository>,
    account_factory: Arc<AccountProviderFactory>,
    runtime_config: Arc<dyn RuntimeConfigPort>,
}

impl CoreAccountReadPort {
    #[must_use]
    pub const fn new(
        snapshots: Arc<dyn AccountSnapshotRepository>,
        account_factory: Arc<AccountProviderFactory>,
        runtime_config: Arc<dyn RuntimeConfigPort>,
    ) -> Self {
        Self {
            snapshots,
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
        let budget_cap = parse_budget_cap(&config.portfolio.budget.total_budget_usd.value)?;
        let provider = self.account_factory.create(budget_cap)?;
        let snapshot = provider.snapshot(fetched_at).await?;
        Ok(LiveAccountInfo {
            fetched_at,
            budget_cap_usd: budget_cap,
            snapshot: live_account_snapshot(snapshot),
        })
    }
}

fn live_account_snapshot(snapshot: AccountSnapshot) -> LiveAccountSnapshot {
    LiveAccountSnapshot::new(
        snapshot.as_of,
        snapshot.source,
        snapshot.equity_usd,
        snapshot.available_usd,
        snapshot.reserved_usd,
        snapshot.positions,
    )
}

fn parse_budget_cap(raw: &str) -> QuantResult<Usd> {
    let value = Decimal::from_str(raw.trim()).map_err(|error| {
        QuantError::config(format!(
            "invalid portfolio.budget.total_budget_usd `{raw}`: {error}"
        ))
    })?;
    Ok(Usd::new(value))
}
