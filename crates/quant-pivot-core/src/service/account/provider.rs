//! Account provider trait and the credential-gated factory.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, account::AccountError};
use quant_pivot_models::types::Usd;
use quant_pivot_research::portfolio::AccountSnapshot;

use super::{
    client::PolymarketAccountClient, reserved::ReservedCapitalReader, venue::VenueAccountProvider,
};
use crate::pipeline::market_registry::MarketRegistry;

/// Produces the decision-time account capital base for report sizing.
#[async_trait]
pub trait AccountProvider: Send + Sync {
    /// Snapshot the real venue account at `as_of`, or fail closed.
    async fn snapshot(&self, as_of: DateTime<Utc>) -> QuantResult<AccountSnapshot>;
}

/// Credential-gated factory for the venue account provider.
///
/// `client` is `Some` only when the private key was loaded at boot; `funder` is
/// `Some` only when configured. [`AccountProviderFactory::create`] fails closed
/// if either is absent — there is no simulated provider.
pub struct AccountProviderFactory {
    client: Option<Arc<dyn PolymarketAccountClient>>,
    registry: Arc<MarketRegistry>,
    reserved_reader: Arc<dyn ReservedCapitalReader>,
    funder: Option<String>,
}

impl AccountProviderFactory {
    #[must_use]
    pub fn new(
        client: Option<Arc<dyn PolymarketAccountClient>>,
        registry: Arc<MarketRegistry>,
        reserved_reader: Arc<dyn ReservedCapitalReader>,
        funder: Option<String>,
    ) -> Self {
        Self {
            client,
            registry,
            reserved_reader,
            funder,
        }
    }

    /// Whether both the signing client (private key) and a non-blank funder are
    /// present, i.e. [`Self::create`] would succeed. Consumed by the admission
    /// `CredentialReadinessCheck` without minting a provider.
    #[must_use]
    pub fn credentials_ready(&self) -> bool {
        self.client.is_some()
            && self
                .funder
                .as_ref()
                .is_some_and(|funder| !funder.trim().is_empty())
    }

    /// Build a provider for the given budget governance cap, or fail closed.
    ///
    /// # Errors
    ///
    /// [`AccountError::CredentialsMissing`] when the private key was not loaded,
    /// [`AccountError::FunderMissing`] when no funder address is configured.
    pub fn create(&self, budget_cap: Usd) -> QuantResult<Arc<dyn AccountProvider>> {
        let client = self
            .client
            .clone()
            .ok_or(AccountError::CredentialsMissing)?;
        let funder = self
            .funder
            .as_ref()
            .map(|funder| funder.trim().to_owned())
            .filter(|funder| !funder.is_empty())
            .ok_or(AccountError::FunderMissing)?;
        Ok(Arc::new(VenueAccountProvider::new(
            client,
            Arc::clone(&self.registry),
            Arc::clone(&self.reserved_reader),
            budget_cap,
            funder,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_pivot_api::data_api::VenuePosition;
    use quant_pivot_error::{QuantError, account::AccountError};
    use quant_pivot_models::types::Usd;
    use rust_decimal_macros::dec;

    struct StubClient;

    #[async_trait]
    impl PolymarketAccountClient for StubClient {
        async fn available_collateral(&self) -> QuantResult<Usd> {
            Ok(Usd::ZERO)
        }
        async fn positions(&self, _funder: &str) -> QuantResult<Vec<VenuePosition>> {
            Ok(Vec::new())
        }
    }

    struct StubReserved;

    #[async_trait]
    impl ReservedCapitalReader for StubReserved {
        async fn sum_locked(&self) -> QuantResult<Usd> {
            Ok(Usd::ZERO)
        }
    }

    #[test]
    fn fails_closed_without_private_key() {
        let factory = AccountProviderFactory::new(
            None,
            Arc::new(MarketRegistry::new()),
            Arc::new(StubReserved),
            Some("0xfunder".to_owned()),
        );
        let result = factory.create(Usd::new(dec!(100)));
        assert!(matches!(
            result,
            Err(QuantError::Account(AccountError::CredentialsMissing))
        ));
    }

    #[test]
    fn fails_closed_without_funder() {
        let factory = AccountProviderFactory::new(
            Some(Arc::new(StubClient)),
            Arc::new(MarketRegistry::new()),
            Arc::new(StubReserved),
            None,
        );
        let result = factory.create(Usd::new(dec!(100)));
        assert!(matches!(
            result,
            Err(QuantError::Account(AccountError::FunderMissing))
        ));
    }

    #[test]
    fn blank_funder_fails_closed() {
        let factory = AccountProviderFactory::new(
            Some(Arc::new(StubClient)),
            Arc::new(MarketRegistry::new()),
            Arc::new(StubReserved),
            Some("   ".to_owned()),
        );
        assert!(factory.create(Usd::new(dec!(100))).is_err());
    }
}
