use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        governance::NewOperationLog,
        quant::{OrderIntentInfo, RecommendationInfo},
    },
    types::{RecommendationId, RecommendationReportId},
};

/// Recommendation access + per-recommendation TTL expiry.
///
/// Recommendations are inserted only as part of the report-creation transaction
/// (the recommendation-report repository's atomic create operation); there is no
/// standalone batch insert. Their lifecycle transition is per-recommendation:
/// each expires at its own data-driven `valid_until` (the report rolls up to
/// `Expired` once all its recommendations are terminal).
#[async_trait::async_trait]
pub trait RecommendationRepository: Send + Sync {
    async fn find_by_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<RecommendationInfo>, StorageError>;

    async fn find_by_id(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationInfo>, StorageError>;

    /// Batch-load recommendations by id (chunked `IN` lists). Missing ids are omitted.
    async fn find_by_ids(
        &self,
        recommendation_ids: &[RecommendationId],
    ) -> Result<Vec<RecommendationInfo>, StorageError>;

    /// Ids of recommendations eligible for TTL expiry: still actionable
    /// (`Published` / `IntentCreated`) and past their data-driven `valid_until`,
    /// earliest deadline first, capped at `limit` (per-recommendation sweep input).
    async fn find_expirable(
        &self,
        now: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<RecommendationId>, StorageError>;

    /// Ids + `valid_until` of actionable recommendations due at or before
    /// `before`, earliest first, capped — the deadline scheduler's look-ahead.
    async fn upcoming_expirations(
        &self,
        before: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<(RecommendationId, DateTime<Utc>)>, StorageError>;

    /// Expire a single recommendation (`Published` / `IntentCreated` ->
    /// `Expired`), atomically invalidating every pre-submission intent, releasing
    /// its capital, and writing WORM logs. Returns the invalidated intents for
    /// post-commit event publication. A terminal recommendation is a conflict.
    async fn expire(
        &self,
        recommendation_id: &RecommendationId,
        expired_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<(RecommendationInfo, Vec<OrderIntentInfo>), StorageError>;

    /// Expired or superseded recommendations with no final attribution row.
    /// Covers report-only rows that reached an unfilled terminal state without
    /// ever creating an intent.
    async fn find_unfilled_attribution_candidates(
        &self,
        limit: u64,
    ) -> Result<Vec<RecommendationInfo>, StorageError>;

    /// Returns `true` when execution ledger truth is still ambiguous and final
    /// attribution must defer.
    async fn recommendation_blocks_final_attribution(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<bool, StorageError>;
}
