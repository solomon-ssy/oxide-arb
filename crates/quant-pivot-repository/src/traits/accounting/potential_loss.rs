use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        NewPotentialLoss, PotentialLossInfo, ResolvePotentialLoss, evidence::EvidenceQueryResult,
    },
    types::{LedgerId, MarketId, Usd},
};

use chrono::{DateTime, Utc};

use crate::traits::timeseries::evidence_query_result;

#[async_trait::async_trait]
pub trait PotentialLossRepository: Send + Sync {
    async fn create(&self, entry: NewPotentialLoss) -> Result<PotentialLossInfo, StorageError>;

    async fn find_active(&self) -> Result<Vec<PotentialLossInfo>, StorageError>;

    async fn find_active_as_of(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Vec<PotentialLossInfo>, StorageError>;

    async fn find_active_as_of_evidence(
        &self,
        at: DateTime<Utc>,
    ) -> Result<EvidenceQueryResult<PotentialLossInfo>, StorageError> {
        let rows = self.find_active_as_of(at).await?;
        evidence_query_result(
            "PotentialLossRepository",
            "find_active_as_of",
            &at,
            vec!["created_at ASC".to_owned(), "ledger_id ASC".to_owned()],
            Some(1),
            rows,
        )
    }

    async fn find_changed_between(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<PotentialLossInfo>, StorageError>;

    async fn find_changed_between_evidence(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<EvidenceQueryResult<PotentialLossInfo>, StorageError> {
        let rows = self.find_changed_between(from, to).await?;
        evidence_query_result(
            "PotentialLossRepository",
            "find_changed_between",
            &(from, to),
            vec!["created_at ASC".to_owned(), "ledger_id ASC".to_owned()],
            Some(1),
            rows,
        )
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PotentialLossInfo>, StorageError>;

    async fn resolve(
        &self,
        ledger_id: &LedgerId,
        command: ResolvePotentialLoss,
    ) -> Result<PotentialLossInfo, StorageError>;

    async fn total_active_loss(&self) -> Result<Usd, StorageError>;
}
