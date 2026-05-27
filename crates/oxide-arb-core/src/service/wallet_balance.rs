use oxide_arb_api::clob::ClobClient;
use oxide_arb_error::OxideError;
use oxide_arb_models::{domain::trade::WalletBalanceSnapshot, types::Usd};
use oxide_arb_risk::traits::ExposureReservationBackend;
use oxide_arb_storage::cache::{CacheKey, TieredCache};
use std::sync::Arc;

pub struct WalletBalanceService {
    cache: Arc<TieredCache>,
    clob_client: Arc<ClobClient>,
    exposure_backend: Arc<dyn ExposureReservationBackend>,
}

impl WalletBalanceService {
    pub fn new(
        cache: Arc<TieredCache>,
        clob_client: Arc<ClobClient>,
        exposure_backend: Arc<dyn ExposureReservationBackend>,
    ) -> Self {
        Self {
            cache,
            clob_client,
            exposure_backend,
        }
    }

    pub async fn get_snapshot(&self) -> Result<WalletBalanceSnapshot, OxideError> {
        let key = CacheKey::Balance;

        if let Some(cached) = self
            .cache
            .get_json::<WalletBalanceSnapshot>(&key)
            .await
            .map_err(|e| OxideError::Internal(format!("balance cache read: {e}")))?
        {
            return Ok(cached);
        }

        let raw = self.clob_client.collateral_balance().await?;
        let reserved = self.exposure_backend.total_reserved_usd().await;
        let available = Usd::new((raw.inner() - reserved.inner()).max(rust_decimal::Decimal::ZERO));

        let snapshot = WalletBalanceSnapshot {
            raw_balance: raw,
            reserved,
            available,
            queried_at: chrono::Utc::now(),
        };

        self.cache
            .set_json(&key, &snapshot)
            .await
            .map_err(|e| OxideError::Internal(format!("balance cache write: {e}")))?;
        Ok(snapshot)
    }

    pub async fn get_available(&self) -> Result<Usd, OxideError> {
        Ok(self.get_snapshot().await?.available)
    }

    pub async fn invalidate(&self) -> Result<(), OxideError> {
        self.cache
            .invalidate(&CacheKey::Balance)
            .await
            .map_err(|e| OxideError::Internal(format!("balance cache invalidate: {e}")))
    }
}
