//! Stub venue account wiring for integration tests.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_api::data_api::VenuePosition;
use quant_pivot_core::{
    app::ports::account_read::CoreAccountReadPort,
    ingest::market_registry::MarketRegistry,
    service::account::{AccountProviderFactory, PolymarketAccountClient, ReservedCapitalReader},
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{AccountReadPort, PolicySnapshotPort},
    types::Usd,
};
use quant_pivot_repository::{
    postgres::{PgAccountSnapshotRepository, PgEquitySnapshotRepository},
    traits::{AccountSnapshotRepository, EquitySnapshotRepository},
};
use sea_orm::DatabaseConnection;

/// Default funder used by stub account reads in tests.
pub const STUB_FUNDER: &str = "0x56687bf447db6ffa42ffe2204a05edaa20f55839";

struct ZeroCollateralClient;

#[async_trait]
impl PolymarketAccountClient for ZeroCollateralClient {
    async fn available_collateral(&self) -> QuantResult<Usd> {
        Ok(Usd::ZERO)
    }

    async fn positions(&self, _funder: &str) -> QuantResult<Vec<VenuePosition>> {
        Ok(Vec::new())
    }
}

struct ZeroReservedCapital;

#[async_trait]
impl ReservedCapitalReader for ZeroReservedCapital {
    async fn sum_locked(&self) -> QuantResult<Usd> {
        Ok(Usd::ZERO)
    }
}

/// Credential-ready factory backed by zero collateral and no venue positions.
#[must_use]
pub fn stub_account_provider_factory() -> Arc<AccountProviderFactory> {
    Arc::new(AccountProviderFactory::new(
        Some(Arc::new(ZeroCollateralClient)),
        Arc::new(MarketRegistry::new()),
        Arc::new(ZeroReservedCapital),
        Some(STUB_FUNDER.to_owned()),
    ))
}

/// Real [`CoreAccountReadPort`] over Postgres snapshots + stub venue reads.
#[must_use]
pub fn core_account_read_port(
    db: &DatabaseConnection,
    runtime_config: Arc<dyn PolicySnapshotPort>,
) -> Arc<dyn AccountReadPort> {
    Arc::new(CoreAccountReadPort::new(
        Arc::new(PgAccountSnapshotRepository::new(db.clone()))
            as Arc<dyn AccountSnapshotRepository>,
        Arc::new(PgEquitySnapshotRepository::new(db.clone())) as Arc<dyn EquitySnapshotRepository>,
        stub_account_provider_factory(),
        runtime_config,
    ))
}
