//! Market-linkage ledger repository trait (Phase 11.2.2).

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{MarketLinkageInfo, MarketLinkageListQuery, NewMarketLinkage, Paginated},
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

    /// The PIT-valid record for `market_id` at `as_of`: the latest row with
    /// `derived_at <= as_of` (never a future revision).
    async fn valid_at(
        &self,
        market_id: &MarketId,
        as_of: DateTime<Utc>,
    ) -> Result<Option<MarketLinkageInfo>, StorageError>;

    /// Batched [`Self::valid_at`]: each market's PIT-valid record at `as_of`
    /// (`derived_at <= as_of`, never a future revision), one row per market
    /// that has one. This is the **only** PIT-correct batch read — the online
    /// domain-availability projector must use this, never
    /// [`Self::latest_for_markets`] (which ignores `as_of` entirely and would
    /// leak a future metadata revision into a past decision).
    async fn valid_at_for_markets(
        &self,
        market_ids: &[MarketId],
        as_of: DateTime<Utc>,
    ) -> Result<Vec<MarketLinkageInfo>, StorageError>;

    /// Each market's newest ledger record (any status), ignoring `as_of`
    /// entirely. Drives **only** resolver idempotence (skip re-resolving a
    /// market whose newest record already matches the current metadata/ruleset)
    /// — never a PIT decision-time read; use [`Self::valid_at_for_markets`] for
    /// that.
    async fn latest_for_markets(
        &self,
        market_ids: &[MarketId],
    ) -> Result<Vec<MarketLinkageInfo>, StorageError>;

    /// The full ledger history for the given markets with
    /// `derived_at <= derived_before`, ascending by `derived_at`. Feeds the
    /// offline replay prefetch (bitemporal per-`as_of` selection is in-memory).
    async fn ledger_for_markets(
        &self,
        market_ids: &[MarketId],
        derived_before: DateTime<Utc>,
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
