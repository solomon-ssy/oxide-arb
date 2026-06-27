//! Venue account capital subsystem bundle (Phase 4).
//!
//! Assembled at boot: loads the keystore, authenticates the CLOB client (the
//! private-key-derived L2 read credential), and wires the Data API +
//! reserved-capital readers into the [`AccountProviderFactory`]. Boot fails
//! closed if the private key is missing or CLOB authentication fails — there is
//! no simulated account path.

use std::sync::Arc;

use quant_pivot_api::{clob::ClobClient, data_api::DataApiClient};
use quant_pivot_error::QuantResult;
use quant_pivot_models::config::DeployConfig;
use quant_pivot_repository::{
    postgres::PgReservedCapitalRepository, traits::ReservedCapitalRepository,
};

use super::InfraBundle;
use crate::{
    pipeline::market_registry::MarketRegistry,
    service::account::{
        AccountProviderFactory, PolymarketAccountClient, RepoReservedCapitalReader,
        ReservedCapitalReader, VenuePolymarketAccountClient,
    },
};

/// Credential-gated venue account subsystem.
pub struct AccountBundle {
    /// Factory that mints a per-report [`crate::service::account::AccountProvider`]
    /// bound to a budget governance cap.
    pub provider_factory: Arc<AccountProviderFactory>,
}

/// Dependencies for [`AccountBundle::assemble`].
pub struct AccountBundleDeps<'a> {
    /// Deploy config (keys, polymarket, `data_api`, funder).
    pub deploy: &'a DeployConfig,
    /// Persistence connections (reserved-capital reader).
    pub infra: &'a InfraBundle,
    /// Market registry for position → market/event/category mapping.
    pub market_registry: Arc<MarketRegistry>,
    /// Shared authenticated CLOB client (single L1+L2 identity, also used by
    /// the execution bundle for order writes).
    pub clob: Arc<ClobClient>,
}

impl AccountBundle {
    /// Assemble the account subsystem from the shared authenticated CLOB client.
    pub fn assemble(deps: AccountBundleDeps<'_>) -> QuantResult<Self> {
        let data_api = Arc::new(DataApiClient::new(deps.deploy.market_data.data_api.clone()));
        let client: Arc<dyn PolymarketAccountClient> =
            Arc::new(VenuePolymarketAccountClient::new(deps.clob, data_api));

        let reserved_repo: Arc<dyn ReservedCapitalRepository> = Arc::new(
            PgReservedCapitalRepository::new(deps.infra.pg.connection().clone()),
        );
        let reserved_reader: Arc<dyn ReservedCapitalReader> =
            Arc::new(RepoReservedCapitalReader::new(reserved_repo));

        let factory = AccountProviderFactory::new(
            Some(client),
            deps.market_registry,
            reserved_reader,
            deps.deploy.quant.account.funder.clone(),
        );

        Ok(Self {
            provider_factory: Arc::new(factory),
        })
    }
}
