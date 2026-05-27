use crate::exposure::in_memory::InMemoryExposureReservation;
use oxide_arb_error::reservation::ReservationError;
use oxide_arb_models::{
    config::ExposureReservationConfig,
    domain::execution::ReservationHandle,
    types::{MarketId, Usd},
};
use std::{sync::Arc, time::Duration};

pub struct CapitalManager {
    backend: Arc<InMemoryExposureReservation>,
    config: ExposureReservationConfig,
}

impl CapitalManager {
    pub const fn new(
        backend: Arc<InMemoryExposureReservation>,
        config: ExposureReservationConfig,
    ) -> Self {
        Self { backend, config }
    }

    pub fn reserve_sync(
        &self,
        market_id: &MarketId,
        amount: Usd,
    ) -> Result<ReservationHandle, ReservationError> {
        let ttl = Duration::from_secs(self.config.default_ttl_secs);
        let id = self.backend.try_reserve_sync(market_id, amount, ttl)?;
        Ok(ReservationHandle {
            id,
            amount,
            market_id: market_id.clone(),
        })
    }

    pub fn confirm_sync(&self, handle: &ReservationHandle) -> Result<(), ReservationError> {
        self.backend.confirm_sync(&handle.id)
    }

    pub fn release_sync(&self, handle: &ReservationHandle) -> Result<(), ReservationError> {
        self.backend.release_sync(&handle.id)
    }

    pub fn reserve(
        &self,
        market_id: &MarketId,
        amount: Usd,
    ) -> Result<ReservationHandle, ReservationError> {
        self.reserve_sync(market_id, amount)
    }

    pub fn confirm(&self, handle: &ReservationHandle) -> Result<(), ReservationError> {
        self.confirm_sync(handle)
    }

    pub fn release(&self, handle: &ReservationHandle) -> Result<(), ReservationError> {
        self.release_sync(handle)
    }
}
