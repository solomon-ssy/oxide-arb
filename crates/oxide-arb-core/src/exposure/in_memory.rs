use arc_swap::ArcSwap;
use dashmap::DashMap;
use num_traits::ToPrimitive;
use oxide_arb_error::reservation::ReservationError;
use oxide_arb_models::{
    runtime_config::ExposureReservationConfig,
    types::{MarketId, ReservationId, Usd},
};
use oxide_arb_risk::traits::ExposureReservationBackend;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

struct ReservationEntry {
    market_id: MarketId,
    amount_cents: u64,
    expires_at: Instant,
}

pub struct InMemoryExposureReservation {
    reservations: DashMap<ReservationId, ReservationEntry>,
    total_reserved_cents: AtomicU64,
    per_market_cents: DashMap<MarketId, AtomicU64>,
    config: ArcSwap<ExposureReservationConfig>,
}

impl InMemoryExposureReservation {
    pub fn new(config: ExposureReservationConfig) -> Self {
        Self {
            reservations: DashMap::new(),
            total_reserved_cents: AtomicU64::new(0),
            per_market_cents: DashMap::new(),
            config: ArcSwap::from_pointee(config),
        }
    }

    /// Hot-reload exposure limits (runtime-config activation).
    ///
    /// In-flight reservations are untouched; new limits gate subsequent
    /// reservations only. The activation preflight guarantees the new
    /// ceilings are not below currently reserved totals.
    pub fn reload(&self, config: ExposureReservationConfig) {
        self.config.store(Arc::new(config));
    }

    /// Largest single-market in-flight exposure (preflight input).
    #[must_use]
    pub fn max_market_reserved_usd_sync(&self) -> Usd {
        let max_cents = self
            .per_market_cents
            .iter()
            .map(|entry| entry.value().load(Ordering::Acquire))
            .max()
            .unwrap_or(0);
        Usd::new(Decimal::from(max_cents) / dec!(100))
    }

    #[inline]
    pub fn total_reserved_usd_sync(&self) -> Usd {
        let cents = self.total_reserved_cents.load(Ordering::Acquire);
        Usd::new(Decimal::from(cents) / dec!(100))
    }

    #[inline]
    pub fn active_count_sync(&self) -> usize {
        self.reservations.len()
    }

    #[inline]
    pub fn per_market_reserved_sync(&self, market_id: &MarketId) -> Usd {
        self.per_market_cents.get(market_id).map_or(Usd::ZERO, |v| {
            Usd::new(Decimal::from(v.load(Ordering::Acquire)) / dec!(100))
        })
    }

    /// Single-pass reservation read for pre-trade metrics snapshots.
    #[inline]
    pub fn reservation_snapshot_sync(&self, market_id: &MarketId) -> (Usd, Usd, usize) {
        (
            self.total_reserved_usd_sync(),
            self.per_market_reserved_sync(market_id),
            self.active_count_sync(),
        )
    }

    /// GC expired reservations. Returns count of expired entries removed.
    pub fn gc_expired(&self) -> u32 {
        let now = Instant::now();
        let mut expired_count = 0u32;

        self.reservations.retain(|_, entry| {
            if now >= entry.expires_at {
                self.total_reserved_cents
                    .fetch_sub(entry.amount_cents, Ordering::AcqRel);
                if let Some(market_total) = self.per_market_cents.get(&entry.market_id) {
                    market_total.fetch_sub(entry.amount_cents, Ordering::AcqRel);
                }
                expired_count += 1;
                false
            } else {
                true
            }
        });

        expired_count
    }

    fn usd_to_cents(amount: Usd) -> Result<u64, ReservationError> {
        let cents_decimal = (amount.inner() * dec!(100)).floor();
        if cents_decimal.is_sign_negative() {
            return Err(ReservationError::Backend(
                "amount overflow or negative".into(),
            ));
        }
        cents_decimal
            .to_u64()
            .ok_or_else(|| ReservationError::Backend("amount overflow or negative".into()))
    }

    /// Synchronous reserve — hot-path entry point for execution pipeline.
    pub fn try_reserve_sync(
        &self,
        market_id: &MarketId,
        amount: Usd,
        ttl: Duration,
    ) -> Result<ReservationId, ReservationError> {
        let amount_cents = Self::usd_to_cents(amount)?;
        let config = self.config.load();

        // CAS loop for global limit
        loop {
            let current = self.total_reserved_cents.load(Ordering::Acquire);
            let Some(new_total) = current.checked_add(amount_cents) else {
                return Err(ReservationError::Backend(
                    "reservation total overflow".into(),
                ));
            };

            if new_total > config.max_total_exposure_cents {
                return Err(ReservationError::ExceedsLimit {
                    current_cents: current,
                    requested_cents: amount_cents,
                    max_cents: config.max_total_exposure_cents,
                });
            }

            if self
                .total_reserved_cents
                .compare_exchange_weak(current, new_total, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        // Per-market limit check
        let market_counter = self
            .per_market_cents
            .entry(market_id.clone())
            .or_insert_with(|| AtomicU64::new(0));
        let market_current = market_counter.fetch_add(amount_cents, Ordering::AcqRel);
        let Some(market_new_total) = market_current.checked_add(amount_cents) else {
            market_counter.fetch_sub(amount_cents, Ordering::AcqRel);
            drop(market_counter);
            self.total_reserved_cents
                .fetch_sub(amount_cents, Ordering::AcqRel);
            return Err(ReservationError::Backend(
                "reservation market total overflow".into(),
            ));
        };

        if market_new_total > config.max_per_market_cents {
            market_counter.fetch_sub(amount_cents, Ordering::AcqRel);
            drop(market_counter);
            self.total_reserved_cents
                .fetch_sub(amount_cents, Ordering::AcqRel);
            return Err(ReservationError::ExceedsLimit {
                current_cents: market_current,
                requested_cents: amount_cents,
                max_cents: config.max_per_market_cents,
            });
        }

        let id = ReservationId::from_v7();
        self.reservations.insert(
            id.clone(),
            ReservationEntry {
                market_id: market_id.clone(),
                amount_cents,
                expires_at: Instant::now() + ttl,
            },
        );

        Ok(id)
    }

    /// Synchronous confirm — releases reservation tracking after a fill.
    pub fn confirm_sync(&self, id: &ReservationId) -> Result<(), ReservationError> {
        let (_, entry) = self
            .reservations
            .remove(id)
            .ok_or_else(|| ReservationError::NotFound { id: id.to_string() })?;

        self.total_reserved_cents
            .fetch_sub(entry.amount_cents, Ordering::AcqRel);
        if let Some(market_total) = self.per_market_cents.get(&entry.market_id) {
            market_total.fetch_sub(entry.amount_cents, Ordering::AcqRel);
        }
        Ok(())
    }

    /// Synchronous release — same as confirm for in-memory backend.
    pub fn release_sync(&self, id: &ReservationId) -> Result<(), ReservationError> {
        self.confirm_sync(id)
    }

    /// Integration-test helper: snapshot active reservation ids for race injection.
    #[doc(hidden)]
    pub fn test_snapshot_active_ids(&self) -> Vec<ReservationId> {
        self.reservations
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }
}

#[async_trait::async_trait]
impl ExposureReservationBackend for InMemoryExposureReservation {
    async fn try_reserve(
        &self,
        market_id: &MarketId,
        amount: Usd,
        ttl: Duration,
    ) -> Result<ReservationId, ReservationError> {
        self.try_reserve_sync(market_id, amount, ttl)
    }

    async fn confirm(&self, id: &ReservationId) -> Result<(), ReservationError> {
        self.confirm_sync(id)
    }

    async fn release(&self, id: &ReservationId) -> Result<(), ReservationError> {
        self.release_sync(id)
    }

    async fn total_reserved_usd(&self) -> Usd {
        self.total_reserved_usd_sync()
    }

    async fn active_count(&self) -> usize {
        self.active_count_sync()
    }

    async fn per_market_reserved(&self, market_id: &MarketId) -> Usd {
        self.per_market_reserved_sync(market_id)
    }
}
