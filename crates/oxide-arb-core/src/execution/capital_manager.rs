use crate::exposure::in_memory::InMemoryExposureReservation;
use oxide_arb_error::reservation::ReservationError;
use oxide_arb_models::{
    domain::execution::ReservationHandle,
    runtime_config::ExposureReservationConfig,
    types::{MarketId, Usd},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

pub struct CapitalManager {
    backend: Arc<InMemoryExposureReservation>,
    reservation_ttl_secs: AtomicU64,
}

impl CapitalManager {
    pub const fn new(
        backend: Arc<InMemoryExposureReservation>,
        config: &ExposureReservationConfig,
    ) -> Self {
        Self {
            backend,
            reservation_ttl_secs: AtomicU64::new(config.default_ttl_secs),
        }
    }

    /// Hot-reload reservation parameters (runtime-config activation). The
    /// exposure ceilings themselves live in the shared backend and are
    /// reloaded there.
    pub fn reload(&self, config: &ExposureReservationConfig) {
        self.reservation_ttl_secs
            .store(config.default_ttl_secs, Ordering::Relaxed);
    }

    pub fn reserve_sync(
        &self,
        market_id: &MarketId,
        amount: Usd,
    ) -> Result<ReservationHandle, ReservationError> {
        let ttl = Duration::from_secs(self.reservation_ttl_secs.load(Ordering::Relaxed));
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

    pub fn pin_for_reconciliation_sync(
        &self,
        handle: &ReservationHandle,
    ) -> Result<(), ReservationError> {
        self.backend.pin_for_reconciliation_sync(&handle.id)
    }

    pub fn resize_sync(
        &self,
        handle: &ReservationHandle,
        new_amount: Usd,
    ) -> Result<(), ReservationError> {
        self.backend.resize_sync(&handle.id, new_amount)
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

    pub fn pin_for_reconciliation(
        &self,
        handle: &ReservationHandle,
    ) -> Result<(), ReservationError> {
        self.pin_for_reconciliation_sync(handle)
    }
}
