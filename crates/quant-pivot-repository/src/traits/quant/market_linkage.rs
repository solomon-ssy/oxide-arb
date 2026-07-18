//! Market-linkage ledger repository trait (Phase 11.2.2).

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        DecisionBoundary, MarketLinkageInfo, MarketLinkageListQuery, NewMarketLinkage, Paginated,
    },
    types::{MarketId, MarketLinkageId},
};

/// Persistence port for the append-only, content-addressed, bitemporal
/// `quant_market_linkage` ledger.
///
/// Rows are never updated or deleted; the write path is idempotent on
/// `content_hash`, so re-running the resolver over unchanged metadata never
/// grows the ledger.
#[async_trait::async_trait]
pub trait MarketLinkageRepository: Send + Sync {
    /// Append one resolver outcome. Idempotent: when a row with the same
    /// `content_hash` already exists, the existing row is returned untouched.
    async fn append(&self, linkage: NewMarketLinkage) -> Result<MarketLinkageInfo, StorageError>;

    /// Atomically append one already validated decision-group batch.
    ///
    /// Implementations must commit all rows and source bindings together or
    /// leave the ledger unchanged. This prevents a persistence failure from
    /// exposing only part of a mutually exclusive Weather sibling set.
    async fn append_batch(
        &self,
        linkages: Vec<NewMarketLinkage>,
    ) -> Result<Vec<MarketLinkageInfo>, StorageError>;

    /// The latest row visible at `boundary`: source-effective no later than the
    /// linkage cutoff and system-available no later than the decision time.
    async fn valid_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> Result<Option<MarketLinkageInfo>, StorageError>;

    /// Batched [`Self::valid_at`], one row per market
    /// that has one. This is the **only** PIT-correct batch read — the online
    /// domain-availability projector must use this, never
    /// [`Self::latest_for_markets`] (which ignores the decision boundary and would
    /// leak a future metadata revision into a past decision).
    async fn valid_at_for_markets(
        &self,
        market_ids: &[MarketId],
        boundary: &DecisionBoundary,
    ) -> Result<Vec<MarketLinkageInfo>, StorageError>;

    /// Each market's newest ledger record (any status), ignoring decision-time
    /// visibility entirely. Drives **only** resolver idempotence (skip re-resolving a
    /// market whose newest record already matches the current metadata/ruleset)
    /// — never a PIT decision-time read; use [`Self::valid_at_for_markets`] for
    /// that.
    async fn latest_for_markets(
        &self,
        market_ids: &[MarketId],
    ) -> Result<Vec<MarketLinkageInfo>, StorageError>;

    /// Latest resolver outcome for every currently active market. This is the
    /// authoritative discovery surface for live external-source workers; they
    /// must not fall back to a static instrument list.
    async fn latest_for_active_markets(&self) -> Result<Vec<MarketLinkageInfo>, StorageError>;

    /// The full ledger history visible through `end_boundary`, ordered by
    /// market, effective time, availability time, and stable linkage id. Feeds
    /// offline replay; each sample applies its own boundary in memory.
    async fn ledger_for_markets(
        &self,
        market_ids: &[MarketId],
        end_boundary: &DecisionBoundary,
    ) -> Result<Vec<MarketLinkageInfo>, StorageError>;

    /// Look up one ledger record by id.
    async fn find_by_id(
        &self,
        linkage_id: &MarketLinkageId,
    ) -> Result<Option<MarketLinkageInfo>, StorageError>;

    /// Page the ledger for the governance catalog, newest first.
    async fn page(
        &self,
        query: MarketLinkageListQuery,
    ) -> Result<Paginated<MarketLinkageInfo>, StorageError>;
}
