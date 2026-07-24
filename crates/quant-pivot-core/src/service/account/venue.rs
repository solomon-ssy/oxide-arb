//! The single venue account provider (all modes, credential-gated).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::quant::AccountSource,
    types::{PositionSnapshot, Usd},
};
use quant_pivot_research::portfolio::AccountSnapshot;

use super::{
    client::PolymarketAccountClient, mapping::map_position, provider::AccountProvider,
    reserved::ReservedCapitalReader,
};
use crate::ingest::market_registry::MarketRegistry;

/// Builds an [`AccountSnapshot`] from the real venue account.
///
/// `venue_net_liquidation = collateral + Σ position current value`, while
/// `capital_base = min(venue_net_liquidation, budget cap)`. Any read failure
/// propagates as an error so the report fails closed; the budget cap is never
/// used as a substitute for venue truth.
pub struct VenueAccountProvider {
    client: Arc<dyn PolymarketAccountClient>,
    registry: Arc<MarketRegistry>,
    reserved_reader: Arc<dyn ReservedCapitalReader>,
    budget_cap: Usd,
    funder: String,
}

impl VenueAccountProvider {
    #[must_use]
    pub const fn new(
        client: Arc<dyn PolymarketAccountClient>,
        registry: Arc<MarketRegistry>,
        reserved_reader: Arc<dyn ReservedCapitalReader>,
        budget_cap: Usd,
        funder: String,
    ) -> Self {
        Self {
            client,
            registry,
            reserved_reader,
            budget_cap,
            funder,
        }
    }
}

#[async_trait]
impl AccountProvider for VenueAccountProvider {
    async fn snapshot(&self, as_of: DateTime<Utc>) -> QuantResult<AccountSnapshot> {
        let collateral = self.client.available_collateral().await?;
        let venue_positions = self.client.positions(&self.funder).await?;
        let positions = venue_positions
            .iter()
            .map(|position| map_position(position, &self.registry))
            .collect::<Vec<PositionSnapshot>>();

        let positions_value = positions
            .iter()
            .map(|position| position.current_value)
            .sum::<Usd>();
        let net_liquidation = collateral + positions_value;
        let capital_base = net_liquidation.min(self.budget_cap);

        let reserved = self.reserved_reader.sum_locked().await?;
        let available = (collateral - reserved).max(Usd::ZERO);

        Ok(AccountSnapshot::new(
            as_of,
            AccountSource::Polymarket,
            net_liquidation,
            capital_base,
            available,
            reserved,
            positions,
        ))
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_api::data_api::VenuePosition;
    use quant_pivot_error::account::AccountError;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::ingest::data_plane_index::DataPlane;

    struct StubClient {
        collateral: Usd,
        positions: Vec<VenuePosition>,
        fail: bool,
    }

    #[async_trait]
    impl PolymarketAccountClient for StubClient {
        async fn available_collateral(&self) -> QuantResult<Usd> {
            if self.fail {
                return Err(AccountError::VenueUnavailable("boom".to_owned()).into());
            }
            Ok(self.collateral)
        }

        async fn positions(&self, _funder: &str) -> QuantResult<Vec<VenuePosition>> {
            if self.fail {
                return Err(AccountError::VenueUnavailable("boom".to_owned()).into());
            }
            Ok(self.positions.clone())
        }
    }

    struct StubReserved(Usd);

    #[async_trait]
    impl ReservedCapitalReader for StubReserved {
        async fn sum_locked(&self) -> QuantResult<Usd> {
            Ok(self.0)
        }
    }

    fn venue_position(current_value: Decimal) -> VenuePosition {
        VenuePosition {
            asset: "token".to_owned(),
            condition_id: "0xmarket".to_owned(),
            size: dec!(100),
            avg_price: dec!(0.5),
            cur_price: dec!(0.6),
            current_value,
            redeemable: false,
            outcome: "Yes".to_owned(),
            outcome_index: 0,
            ..Default::default()
        }
    }

    fn provider(
        collateral: Usd,
        positions: Vec<VenuePosition>,
        reserved: Usd,
        budget_cap: Usd,
        fail: bool,
    ) -> VenueAccountProvider {
        VenueAccountProvider::new(
            Arc::new(StubClient {
                collateral,
                positions,
                fail,
            }),
            Arc::new(MarketRegistry::new(Arc::new(DataPlane::new()))),
            Arc::new(StubReserved(reserved)),
            budget_cap,
            "0xfunder".to_owned(),
        )
    }

    #[tokio::test]
    async fn capital_base_tracks_cap() {
        let provider = provider(
            Usd::new(dec!(1000)),
            vec![venue_position(dec!(200)), venue_position(dec!(50))],
            Usd::new(dec!(100)),
            Usd::new(dec!(100000)),
            false,
        );
        let snapshot = provider.snapshot(Utc::now()).await.expect("snapshot");
        assert_eq!(snapshot.venue_net_liquidation_usd, Usd::new(dec!(1250)));
        assert_eq!(snapshot.capital_base_usd, Usd::new(dec!(1250)));
        assert_eq!(snapshot.available_usd, Usd::new(dec!(900)));
        assert_eq!(snapshot.reserved_usd, Usd::new(dec!(100)));
        assert_eq!(snapshot.positions.len(), 2);
    }

    #[tokio::test]
    async fn separates_venue_net_base() {
        let provider = provider(
            Usd::new(dec!(1000)),
            vec![venue_position(dec!(500))],
            Usd::ZERO,
            Usd::new(dec!(800)),
            false,
        );
        let snapshot = provider.snapshot(Utc::now()).await.expect("snapshot");
        // Net liquidation 1500 > budget cap 800 → sizing capital is the cap.
        assert_eq!(snapshot.venue_net_liquidation_usd, Usd::new(dec!(1500)));
        assert_eq!(snapshot.capital_base_usd, Usd::new(dec!(800)));
    }

    #[tokio::test]
    async fn fail_closed_venue_error() {
        let provider = provider(
            Usd::new(dec!(1000)),
            Vec::new(),
            Usd::ZERO,
            Usd::new(dec!(800)),
            true,
        );
        assert!(provider.snapshot(Utc::now()).await.is_err());
    }
}
