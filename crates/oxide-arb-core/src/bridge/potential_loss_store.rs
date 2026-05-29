//! `PotentialLossStore` bridge — wraps `PgPotentialLossRepository` into
//! the risk crate's DI trait.

use chrono::Utc;
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::{
    domain::{NewPotentialLoss, ResolvePotentialLoss, potential_loss::PotentialLossInfo},
    types::LedgerId,
};
use oxide_arb_repository::{postgres::PgPotentialLossRepository, traits::PotentialLossRepository};
use oxide_arb_risk::traits::PotentialLossStore;
use std::{sync::Arc, time::Duration};

pub struct CorePotentialLossStore {
    repo: Arc<PgPotentialLossRepository>,
}

impl CorePotentialLossStore {
    pub const fn new(repo: Arc<PgPotentialLossRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait::async_trait]
impl PotentialLossStore for CorePotentialLossStore {
    async fn create(&self, entry: NewPotentialLoss) -> OxideResult<PotentialLossInfo> {
        self.repo.create(entry).await.map_err(OxideError::from)
    }

    async fn resolve(&self, ledger_id: &LedgerId) -> OxideResult<()> {
        let command = ResolvePotentialLoss {
            resolved_at: Utc::now(),
        };
        self.repo
            .resolve(ledger_id, command)
            .await
            .map_err(OxideError::from)?;
        Ok(())
    }

    async fn find_active(&self) -> OxideResult<Vec<PotentialLossInfo>> {
        self.repo.find_active().await.map_err(OxideError::from)
    }

    async fn find_stale(&self, max_age: Duration) -> OxideResult<Vec<PotentialLossInfo>> {
        let cutoff =
            Utc::now() - chrono::Duration::from_std(max_age).unwrap_or(chrono::TimeDelta::MAX);
        let active = self.repo.find_active().await.map_err(OxideError::from)?;
        Ok(active
            .into_iter()
            .filter(|e| e.created_at < cutoff)
            .collect())
    }
}
