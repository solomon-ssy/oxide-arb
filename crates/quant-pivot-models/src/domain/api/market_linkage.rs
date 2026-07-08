//! Market-linkage governance HTTP contract (Phase 11.2.2).
//!
//! Read surface for the append-only, bitemporal `quant_market_linkage` ledger
//! (catalog, unresolved review queue, per-market history) plus the governed
//! mutations: an audited operator override and a manual re-resolution trigger.
//! Also carries the basis-cross-check alert feed (11.2.2 remediation R6): the
//! durable, queryable record of every feature-source-vs-settlement-oracle
//! divergence that crossed the governed threshold.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{
        BasisAlertInfo, DomainSourceCursorInfo, ManualEvidenceInput, MarketLinkageInfo,
        pagination::PageRequest,
    },
    enums::domain::{DomainFamily, LinkageStatus, ResolverTier},
    types::{
        BasisAlertId, Bps, ContentHash, DomainInstrumentKey, MarketId, MarketLinkageId,
        Probability, ResolverVersion,
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
/// The operator supplies a full subject JSON plus literal-text citations for
/// every load-bearing identity field (11.2.2 remediation R4) — an override is
/// a human decision, never text-extracted, but it must still cite real
/// source text for `asset` / `resolution_oracle` / `strike` (when present),
/// verified byte-exact by [`quant_pivot_research::linkage::validate_manual_override`].
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
    /// Literal-text citations grounding `asset` / `resolution_oracle` / and
    /// `strike` (when the subject has one) — checked byte-exact against the
    /// market's real metadata, never trusted as submitted.
    #[serde(default)]
    pub evidence: Vec<ManualEvidenceInput>,
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
    /// Populated only for `resolver_tier = override` rows (R4 audit columns).
    pub override_reason: Option<String>,
    pub override_actor: Option<String>,
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
            override_reason: info.override_reason,
            override_actor: info.override_actor,
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
    /// Populated only for `resolver_tier = override` rows (R4 audit columns).
    pub override_reason: Option<String>,
    pub override_actor: Option<String>,
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
            override_reason: info.override_reason,
            override_actor: info.override_actor,
            created_at: info.created_at,
        }
    }
}

/// One historical ledger row for a market's linkage audit trail.
///
/// The detail drawer's "history" tab. Every append is a first-class,
/// immutable audit entry — a resolve pass, or an operator override, in
/// `derived_at` order (R8: UI/UX closed loop, override/resolve audit trail).
#[derive(Debug, Clone, Serialize)]
pub struct MarketLinkageHistoryEntryView {
    pub linkage_id: MarketLinkageId,
    pub status: LinkageStatus,
    pub resolver_tier: ResolverTier,
    pub resolver_version: ResolverVersion,
    pub confidence: Probability,
    /// The full `LinkageOutcome` document (subject + grounding, or reason;
    /// carries `override_context` when `resolver_tier = override`).
    pub outcome: serde_json::Value,
    pub instrument_key: Option<DomainInstrumentKey>,
    pub content_hash: ContentHash,
    pub derived_at: DateTime<Utc>,
    /// Populated only for `resolver_tier = override` rows (R4 audit columns).
    pub override_reason: Option<String>,
    pub override_actor: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl From<MarketLinkageInfo> for MarketLinkageHistoryEntryView {
    fn from(info: MarketLinkageInfo) -> Self {
        Self {
            linkage_id: info.linkage_id,
            status: info.status,
            resolver_tier: info.resolver_tier,
            resolver_version: info.resolver_version,
            confidence: info.confidence,
            outcome: info.outcome,
            instrument_key: info.instrument_key,
            content_hash: info.content_hash,
            derived_at: info.derived_at,
            override_reason: info.override_reason,
            override_actor: info.override_actor,
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
    /// Detail from the most recent failed tick; `null` when the last tick for
    /// this instrument succeeded (R10 ingest hardening).
    pub last_error: Option<String>,
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
            last_error: info.last_error,
            lag_secs,
            updated_at: info.updated_at,
        }
    }
}

/// Paginated filter over the basis-alert feed.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct BasisAlertListQuery {
    /// Filter by market.
    pub market_id: Option<MarketId>,
    /// Inclusive lower bound on `as_of`.
    pub from: Option<DateTime<Utc>>,
    /// Exclusive upper bound on `as_of`.
    pub to: Option<DateTime<Utc>>,
    /// When true, return only unacknowledged alerts (the review-queue default
    /// view; R6 remediation).
    #[serde(default)]
    pub open_only: bool,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Inbound body for `POST /research/basis-alerts/{alert_id}/acknowledge`.
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct AcknowledgeBasisAlertRequest {
    /// Operator reason recorded on the operation log (not on the ledger row —
    /// the row only carries who/when, mirroring the linkage `resolve` action).
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// One basis-cross-check exceedance row for the governance feed.
#[derive(Debug, Clone, Serialize)]
pub struct BasisAlertView {
    pub alert_id: BasisAlertId,
    pub market_id: MarketId,
    pub instrument_key: String,
    pub oracle_instrument_key: String,
    pub basis_bps: Bps,
    pub threshold_bps: Bps,
    pub as_of: DateTime<Utc>,
    pub acknowledged: bool,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub acknowledged_by: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl From<BasisAlertInfo> for BasisAlertView {
    fn from(info: BasisAlertInfo) -> Self {
        Self {
            alert_id: info.alert_id,
            market_id: info.market_id,
            instrument_key: info.instrument_key,
            oracle_instrument_key: info.oracle_instrument_key,
            basis_bps: info.basis_bps,
            threshold_bps: info.threshold_bps,
            as_of: info.as_of,
            acknowledged: info.acknowledged,
            acknowledged_at: info.acknowledged_at,
            acknowledged_by: info.acknowledged_by,
            created_at: info.created_at,
        }
    }
}
