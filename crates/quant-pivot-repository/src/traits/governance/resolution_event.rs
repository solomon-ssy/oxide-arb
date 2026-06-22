use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        evidence::EvidenceQueryResult,
        settlement::{NewResolutionEvent, ResolutionEventInfo},
    },
    types::MarketId,
};

use crate::traits::timeseries::evidence_query_result;

#[async_trait::async_trait]
pub trait ResolutionEventRepository: Send + Sync {
    async fn append(&self, event: NewResolutionEvent) -> Result<(), StorageError>;

    async fn latest_for_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<ResolutionEventInfo>, StorageError>;

    async fn latest_before(
        &self,
        market_id: &MarketId,
        before: DateTime<Utc>,
    ) -> Result<Option<ResolutionEventInfo>, StorageError>;

    async fn latest_before_evidence(
        &self,
        market_id: &MarketId,
        before: DateTime<Utc>,
    ) -> Result<EvidenceQueryResult<ResolutionEventInfo>, StorageError> {
        let rows = self
            .latest_before(market_id, before)
            .await?
            .into_iter()
            .collect();
        evidence_query_result(
            "ResolutionEventRepository",
            "latest_before",
            &(market_id, before),
            vec![
                "market_id ASC".to_owned(),
                "resolved_at DESC".to_owned(),
                "created_at DESC".to_owned(),
            ],
            Some(1),
            rows,
        )
    }

    async fn settlement_truth_before_evidence(
        &self,
        market_ids: &[MarketId],
        before: DateTime<Utc>,
    ) -> Result<EvidenceQueryResult<ResolutionEventInfo>, StorageError> {
        let mut sorted_market_ids = market_ids.to_vec();
        sorted_market_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        sorted_market_ids.dedup();

        let mut rows = Vec::new();
        for market_id in &sorted_market_ids {
            if let Some(row) = self.latest_before(market_id, before).await? {
                rows.push(row);
            }
        }
        evidence_query_result(
            "ResolutionEventRepository",
            "settlement_truth_before",
            &(sorted_market_ids, before),
            vec![
                "market_id ASC".to_owned(),
                "resolved_at DESC".to_owned(),
                "created_at DESC".to_owned(),
            ],
            Some(1),
            rows,
        )
    }

    async fn latest_by_source(
        &self,
        market_id: &MarketId,
        source: &str,
    ) -> Result<Option<ResolutionEventInfo>, StorageError>;
}
