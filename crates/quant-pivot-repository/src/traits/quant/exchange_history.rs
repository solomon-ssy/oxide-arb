use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::data_plane::{
    ExchangeHistoryChunkInfo, ExchangeHistoryFrontier, ExchangeHistoryPlanInfo,
    ExchangeHistoryQuarantineInfo, ExchangeHistoryQuarantineResolutionInfo,
    NewExchangeHistoryChunk, NewExchangeHistoryPlan, NewExchangeHistoryQuarantine,
    NewExchangeHistoryQuarantineResolution, ResolveAcceptedHistoryRange,
};

#[async_trait::async_trait]
pub trait ExchangeHistoryRepository: Send + Sync {
    async fn create_or_load_plan(
        &self,
        plan: NewExchangeHistoryPlan,
    ) -> Result<ExchangeHistoryPlanInfo, StorageError>;

    async fn load_plan(
        &self,
        chain_id: i64,
    ) -> Result<Option<ExchangeHistoryPlanInfo>, StorageError>;

    async fn find_range(
        &self,
        frontier: ExchangeHistoryFrontier,
        from_block: i64,
        to_block: i64,
    ) -> Result<Option<ExchangeHistoryChunkInfo>, StorageError>;

    async fn save_chunk(
        &self,
        chunk: NewExchangeHistoryChunk,
    ) -> Result<ExchangeHistoryChunkInfo, StorageError>;

    async fn latest_accepted(
        &self,
        frontier: ExchangeHistoryFrontier,
    ) -> Result<Option<ExchangeHistoryChunkInfo>, StorageError>;

    async fn earliest_accepted(
        &self,
        frontier: ExchangeHistoryFrontier,
    ) -> Result<Option<ExchangeHistoryChunkInfo>, StorageError>;

    async fn accepted_from(
        &self,
        frontier: ExchangeHistoryFrontier,
        from_block: i64,
    ) -> Result<Vec<ExchangeHistoryChunkInfo>, StorageError>;

    async fn rewind_from(
        &self,
        frontier: ExchangeHistoryFrontier,
        from_block: i64,
        updated_at: DateTime<Utc>,
    ) -> Result<Vec<ExchangeHistoryChunkInfo>, StorageError>;

    async fn quarantine_chunk(
        &self,
        chunk: NewExchangeHistoryChunk,
        quarantine: NewExchangeHistoryQuarantine,
    ) -> Result<ExchangeHistoryQuarantineInfo, StorageError>;

    async fn list_quarantine(
        &self,
        frontier: ExchangeHistoryFrontier,
        limit: u64,
    ) -> Result<Vec<ExchangeHistoryQuarantineInfo>, StorageError>;

    async fn active_quarantine(
        &self,
        frontier: ExchangeHistoryFrontier,
        from_block: i64,
        to_block: i64,
        limit: u64,
    ) -> Result<Vec<ExchangeHistoryQuarantineInfo>, StorageError>;

    async fn resolve_quarantine(
        &self,
        resolution: NewExchangeHistoryQuarantineResolution,
    ) -> Result<ExchangeHistoryQuarantineResolutionInfo, StorageError>;

    async fn resolve_accepted_range(
        &self,
        resolution: ResolveAcceptedHistoryRange,
    ) -> Result<Vec<ExchangeHistoryQuarantineResolutionInfo>, StorageError>;
}
