//! Market-linkage governance HTTP contract (Phase 11.2.2).
//!
//! Read surface for the append-only, bitemporal `quant_market_linkage` ledger
//! (catalog, unresolved review queue, per-market history) plus the governed
//! mutations: an audited operator override and a manual re-resolution trigger.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{DomainSourceCursorInfo, MarketLinkageInfo, pagination::PageRequest},
    enums::domain::{DomainFamily, LinkageStatus, ResolverTier},
    types::{
        ContentHash, DomainInstrumentKey, MarketId, MarketLinkageId, Probability, ResolverVersion,
    },
};

/// Paginated filter over the linkage ledger.
///
/// `latest_only` collapses the ledger to each market's newest record — the
/// operator catalog view; the full per-market history stays reachable through
/// the detail endpoint.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct MarketLinkageListQuery {
    /// Filter by derived lifecycle status.
    pub status: Option<LinkageStatus>,
    /// Filter by vertical.
    pub family: Option<DomainFamily>,
    /// Filter by market.
    pub market_id: Option<MarketId>,
    /// Inclusive lower bound on `derived_at`.
    pub from: Option<DateTime<Utc>>,
    /// Exclusive upper bound on `derived_at`.
    pub to: Option<DateTime<Utc>>,
    /// When true, return only each market's newest ledger record.
    #[serde(default)]
    pub latest_only: bool,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Inbound body for `POST /research/market-linkages/{market_id}/override`.
///
/// The operator supplies a full subject JSON (validated against the same
/// grounding-free operator contract — overrides are audited human decisions,
/// recorded with `resolver_tier = override`).
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct OverrideLinkageRequest {
    /// The full `MarketSubject` document to bind.
    pub subject: serde_json::Value,
    /// Canonical instrument key the subject joins to (e.g. `BINANCE:BTCUSDT:1m`).
    #[validate(length(min = 3, max = 128))]
    pub instrument_key: String,
    /// Operator reason recorded on the operation log and the ledger row.
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Inbound body for `POST /research/market-linkages/resolve`.
///
/// Triggers an offline resolver pass. Empty `market_ids` re-resolves every
/// market whose category maps to an enabled vertical.
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct ResolveLinkagesRequest {
    /// Markets to re-resolve; empty = all category-mapped markets.
    #[serde(default)]
    pub market_ids: Vec<MarketId>,
    /// Operator reason recorded on the operation log.
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Linkage summary row for the governance catalog grid.
#[derive(Debug, Clone, Serialize)]
pub struct MarketLinkageSummaryView {
    pub linkage_id: MarketLinkageId,
    pub market_id: MarketId,
    pub domain_family: DomainFamily,
    pub status: LinkageStatus,
    pub resolver_tier: ResolverTier,
    pub resolver_version: ResolverVersion,
    pub confidence: Probability,
    pub instrument_key: Option<DomainInstrumentKey>,
    pub content_hash: ContentHash,
    pub derived_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl From<MarketLinkageInfo> for MarketLinkageSummaryView {
    fn from(info: MarketLinkageInfo) -> Self {
        Self {
            linkage_id: info.linkage_id,
            market_id: info.market_id,
            domain_family: info.domain_family,
            status: info.status,
            resolver_tier: info.resolver_tier,
            resolver_version: info.resolver_version,
            confidence: info.confidence,
            instrument_key: info.instrument_key,
            content_hash: info.content_hash,
            derived_at: info.derived_at,
            created_at: info.created_at,
        }
    }
}

/// Full linkage detail: provenance plus the outcome payload (subject, binding,
/// grounding spans, or the unresolved reason).
#[derive(Debug, Clone, Serialize)]
pub struct MarketLinkageDetailView {
    pub linkage_id: MarketLinkageId,
    pub market_id: MarketId,
    pub domain_family: DomainFamily,
    pub status: LinkageStatus,
    pub resolver_tier: ResolverTier,
    pub resolver_version: ResolverVersion,
    pub confidence: Probability,
    /// The full `LinkageOutcome` document (subject + grounding, or reason).
    pub outcome: serde_json::Value,
    pub instrument_key: Option<DomainInstrumentKey>,
    pub metadata_hash: ContentHash,
    pub content_hash: ContentHash,
    pub derived_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl From<MarketLinkageInfo> for MarketLinkageDetailView {
    fn from(info: MarketLinkageInfo) -> Self {
        Self {
            linkage_id: info.linkage_id,
            market_id: info.market_id,
            domain_family: info.domain_family,
            status: info.status,
            resolver_tier: info.resolver_tier,
            resolver_version: info.resolver_version,
            confidence: info.confidence,
            outcome: info.outcome,
            instrument_key: info.instrument_key,
            metadata_hash: info.metadata_hash,
            content_hash: info.content_hash,
            derived_at: info.derived_at,
            created_at: info.created_at,
        }
    }
}

/// Summary of one offline resolver pass (returned by the resolve trigger).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkageResolveSummaryView {
    /// Markets examined by the pass.
    pub examined: u64,
    /// New ledger records appended (metadata / ruleset changed).
    pub appended: u64,
    /// Markets skipped because the newest record already covers the current
    /// metadata under the current ruleset (idempotent no-op).
    pub unchanged: u64,
    /// Appended records that resolved.
    pub resolved: u64,
    /// Appended records that failed closed.
    pub unresolved: u64,
}

/// One `(source, instrument)` domain ingest cursor health row.
#[derive(Debug, Clone, Serialize)]
pub struct DomainSourceCursorView {
    pub source_id: String,
    pub instrument_key: String,
    pub last_event_time: DateTime<Utc>,
    pub status: String,
    /// Seconds since the last persisted observation (ingest lag proxy).
    pub lag_secs: i64,
    pub updated_at: DateTime<Utc>,
}

impl From<DomainSourceCursorInfo> for DomainSourceCursorView {
    fn from(info: DomainSourceCursorInfo) -> Self {
        let lag_secs = (Utc::now() - info.last_event_time).num_seconds();
        Self {
            source_id: info.source_id.to_string(),
            instrument_key: info.instrument_key.to_string(),
            last_event_time: info.last_event_time,
            status: info.status,
            lag_secs,
            updated_at: info.updated_at,
        }
    }
}
