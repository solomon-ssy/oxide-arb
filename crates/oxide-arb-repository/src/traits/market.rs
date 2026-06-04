use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{MarketInfo, MarketPitSnapshotInfo, UpsertMarket, evidence::EvidenceQueryResult},
    types::MarketId,
};
use std::{collections::HashSet, sync::Arc};

use crate::traits::timeseries::evidence_query_result;

#[async_trait::async_trait]
pub trait MarketRepository: Send + Sync {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<Arc<MarketInfo>>, StorageError>;
    async fn find_by_ids(&self, ids: &[MarketId]) -> Result<Vec<Arc<MarketInfo>>, StorageError>;

    async fn find_by_ids_evidence(
        &self,
        ids: &[MarketId],
    ) -> Result<EvidenceQueryResult<Arc<MarketInfo>>, StorageError> {
        let mut sorted_ids = ids.to_vec();
        sorted_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        sorted_ids.dedup();
        let rows = self.find_by_ids(&sorted_ids).await?;
        evidence_query_result(
            "MarketRepository",
            "find_by_ids",
            &sorted_ids,
            vec!["market_id ASC".to_owned()],
            Some(1),
            rows,
        )
    }

    async fn latest_pit_snapshots_before(
        &self,
        ids: &[MarketId],
        as_of: DateTime<Utc>,
    ) -> Result<Vec<MarketPitSnapshotInfo>, StorageError>;

    async fn latest_pit_snapshots_before_evidence(
        &self,
        ids: &[MarketId],
        as_of: DateTime<Utc>,
    ) -> Result<EvidenceQueryResult<MarketPitSnapshotInfo>, StorageError> {
        let mut sorted_ids = ids.to_vec();
        sorted_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        sorted_ids.dedup();
        let rows = self.latest_pit_snapshots_before(&sorted_ids, as_of).await?;
        evidence_query_result(
            "MarketRepository",
            "latest_pit_snapshots_before",
            &(sorted_ids, as_of),
            vec![
                "market_id ASC".to_owned(),
                "observed_at DESC".to_owned(),
                "created_at DESC".to_owned(),
            ],
            Some(1),
            rows,
        )
    }
    async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError>;
    async fn find_by_event(&self, event_id: &str) -> Result<Vec<Arc<MarketInfo>>, StorageError>;
    async fn find_endgame_candidates(
        &self,
        before_deadline: DateTime<Utc>,
    ) -> Result<Vec<Arc<MarketInfo>>, StorageError>;
    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError>;

    async fn upsert(&self, market: UpsertMarket) -> Result<Arc<MarketInfo>, StorageError>;

    /// Insert new rows and update existing rows in one round-trip (`ON CONFLICT DO UPDATE`).
    async fn upsert_batch(&self, markets: Vec<UpsertMarket>) -> Result<u64, StorageError>;

    async fn update_status(
        &self,
        id: &MarketId,
        status: &str,
        outcome: Option<&str>,
    ) -> Result<(), StorageError>;
}
