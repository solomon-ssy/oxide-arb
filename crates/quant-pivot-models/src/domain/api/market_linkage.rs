//! Market-linkage governance HTTP contract.
//!
//! Read surface for the append-only, bitemporal `quant_market_linkage` ledger
//! (catalog, unresolved review queue, per-market history) plus the governed
//! mutations: an audited operator override and a manual re-resolution trigger.
//! Also carries the basis-cross-check alert feed: the
//! durable, queryable record of every feature-source-vs-settlement-oracle
//! divergence that crossed the governed threshold.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{
        data_plane::{
            DomainCursorStatus, DomainSourceCheckpoint, DomainSourceCursorInfo,
            DomainSourceExpectationInfo,
        },
        pagination::PageRequest,
        quant::{
            BasisAlertInfo, LinkageOutcome, ManualEvidenceInput, MarketLinkageInfo, MarketSubject,
            ResolvedSourceBinding,
        },
    },
    enums::domain::{
        DomainFamily, DomainSourceExpectationStatus, LinkageSourceRole, LinkageStatus, ResolverTier,
    },
    types::{
        BasisAlertId, Bps, ContentHash, DomainInstrumentKey, DomainSourceExpectationId,
        DomainSourceId, MarketId, MarketLinkageId, Probability, ResearchProfileId, ResolverVersion,
    },
};

/// Operator-proposed binding identity. Availability and content hashes are
/// server-owned and therefore cannot be supplied by the client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverrideSourceBindingInput {
    pub role: LinkageSourceRole,
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
}

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
/// The operator supplies a full typed subject plus literal-text citations for
/// every load-bearing identity field — an override is
/// a human decision, never text-extracted, but it must still cite real
/// source text for `asset` / `resolution_oracle` / `strike` (when present),
/// verified byte-exact by the research linkage validator.
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct OverrideLinkageRequest {
    /// The full `MarketSubject` document to bind.
    pub subject: MarketSubject,
    /// Exact source/role/instrument bindings. A single generic instrument is invalid.
    #[validate(length(min = 1, max = 8))]
    pub source_bindings: Vec<OverrideSourceBindingInput>,
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
    pub source_bindings: Vec<ResolvedSourceBinding>,
    pub content_hash: ContentHash,
    pub derived_at: DateTime<Utc>,
    /// Populated only for `resolver_tier = override` rows.
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
            source_bindings: source_bindings_from_outcome(&info.outcome),
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
    pub outcome: LinkageOutcome,
    pub source_bindings: Vec<ResolvedSourceBinding>,
    pub metadata_hash: ContentHash,
    pub content_hash: ContentHash,
    pub derived_at: DateTime<Utc>,
    /// Populated only for `resolver_tier = override` rows.
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
            source_bindings: source_bindings_from_outcome(&info.outcome),
            outcome: info.outcome,
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
/// `derived_at` order, preserving the override/resolve audit trail.
#[derive(Debug, Clone, Serialize)]
pub struct MarketLinkageHistoryEntryView {
    pub linkage_id: MarketLinkageId,
    pub status: LinkageStatus,
    pub resolver_tier: ResolverTier,
    pub resolver_version: ResolverVersion,
    pub confidence: Probability,
    /// The full `LinkageOutcome` document (subject + grounding, or reason;
    /// carries `override_context` when `resolver_tier = override`).
    pub outcome: LinkageOutcome,
    pub source_bindings: Vec<ResolvedSourceBinding>,
    pub content_hash: ContentHash,
    pub derived_at: DateTime<Utc>,
    /// Populated only for `resolver_tier = override` rows.
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
            source_bindings: source_bindings_from_outcome(&info.outcome),
            outcome: info.outcome,
            content_hash: info.content_hash,
            derived_at: info.derived_at,
            override_reason: info.override_reason,
            override_actor: info.override_actor,
            created_at: info.created_at,
        }
    }
}

fn source_bindings_from_outcome(outcome: &LinkageOutcome) -> Vec<ResolvedSourceBinding> {
    match outcome {
        LinkageOutcome::Resolved(binding) => binding.source_bindings.clone(),
        LinkageOutcome::Unresolved { .. } => Vec::new(),
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

/// Expected + observed health for one capability-declared source binding.
#[derive(Debug, Clone, Serialize)]
pub struct DomainSourceExpectationView {
    pub expectation_id: DomainSourceExpectationId,
    pub family: DomainFamily,
    pub source_id: String,
    pub instrument_key: String,
    pub capability_registry_hash: ContentHash,
    pub binding_hash: ContentHash,
    pub required: bool,
    pub credential_required: bool,
    pub freshness_secs: i64,
    pub affected_market_ids: Vec<MarketId>,
    pub affected_profile_ids: Vec<ResearchProfileId>,
    pub status: DomainSourceExpectationStatus,
    pub status_reason: Option<String>,
    pub cursor_status: Option<DomainCursorStatus>,
    pub checkpoint: Option<DomainSourceCheckpoint>,
    pub checkpoint_hash: Option<ContentHash>,
    pub last_event_time: Option<DateTime<Utc>>,
    /// Source liveness timestamp. Archive checkpoints use their last
    /// successful refresh while live feeds use the source-effective event.
    pub freshness_observed_at: Option<DateTime<Utc>>,
    /// `None` means no cursor exists. It must never be rendered as zero lag.
    pub lag_secs: Option<i64>,
    pub cursor_updated_at: Option<DateTime<Utc>>,
    pub observed_at: DateTime<Utc>,
}

impl DomainSourceExpectationView {
    #[must_use]
    pub fn from_expected_and_observed(
        expected: DomainSourceExpectationInfo,
        cursor: Option<DomainSourceCursorInfo>,
        observed_at: DateTime<Utc>,
    ) -> Self {
        let observation = cursor.map(|cursor| {
            let last_event_time = cursor.checkpoint_json.event_time();
            let freshness_observed_at = cursor.checkpoint_json.freshness_time(cursor.updated_at);
            let lag_secs = (observed_at - freshness_observed_at).num_seconds();
            (cursor, last_event_time, freshness_observed_at, lag_secs)
        });
        let (status, status_reason) = effective_source_status(&expected, observation.as_ref());
        let (
            cursor_status,
            checkpoint,
            checkpoint_hash,
            last_event_time,
            freshness_observed_at,
            lag_secs,
            cursor_updated_at,
        ) = match observation {
            Some((cursor, last_event_time, freshness_observed_at, lag_secs)) => (
                Some(cursor.status),
                Some(cursor.checkpoint_json),
                Some(cursor.checkpoint_hash),
                Some(last_event_time),
                Some(freshness_observed_at),
                Some(lag_secs),
                Some(cursor.updated_at),
            ),
            None => (None, None, None, None, None, None, None),
        };
        Self {
            expectation_id: expected.expectation_id,
            family: expected.family,
            source_id: expected.source_id.to_string(),
            instrument_key: expected.instrument_key.to_string(),
            capability_registry_hash: expected.capability_registry_hash,
            binding_hash: expected.binding_hash,
            required: expected.required,
            credential_required: expected.credential_required,
            freshness_secs: expected.freshness_secs,
            affected_market_ids: expected.affected_market_ids.0,
            affected_profile_ids: expected.affected_profile_ids.0,
            status,
            status_reason,
            cursor_status,
            checkpoint,
            checkpoint_hash,
            last_event_time,
            freshness_observed_at,
            lag_secs,
            cursor_updated_at,
            observed_at,
        }
    }
}

fn effective_source_status(
    expected: &DomainSourceExpectationInfo,
    observation: Option<&(DomainSourceCursorInfo, DateTime<Utc>, DateTime<Utc>, i64)>,
) -> (DomainSourceExpectationStatus, Option<String>) {
    if matches!(
        expected.status,
        DomainSourceExpectationStatus::CredentialBlocked
            | DomainSourceExpectationStatus::Failed
            | DomainSourceExpectationStatus::Unsupported
    ) {
        return (expected.status, expected.status_reason.clone());
    }
    let Some((cursor, _, _, lag_secs)) = observation else {
        return (
            DomainSourceExpectationStatus::NotStarted,
            Some("cursor_not_created".to_owned()),
        );
    };
    match cursor.status {
        DomainCursorStatus::Failed => (
            DomainSourceExpectationStatus::Failed,
            Some(
                cursor
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "cursor_error_without_detail".to_owned()),
            ),
        ),
        DomainCursorStatus::Bootstrap | DomainCursorStatus::Backfilling => (
            DomainSourceExpectationStatus::NotStarted,
            Some(format!("cursor_{}", cursor.status)),
        ),
        DomainCursorStatus::Live if *lag_secs < 0 => (
            DomainSourceExpectationStatus::Failed,
            Some("cursor_event_time_in_future".to_owned()),
        ),
        DomainCursorStatus::Live if *lag_secs > expected.freshness_secs => (
            DomainSourceExpectationStatus::Stale,
            Some("freshness_budget_exceeded".to_owned()),
        ),
        DomainCursorStatus::Live => (DomainSourceExpectationStatus::Live, None),
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
    /// When true, return only unacknowledged alerts (the review-queue default).
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

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};

    use super::DomainSourceExpectationView;
    use crate::{
        domain::data_plane::{
            AffectedMarketIds, AffectedProfileIds, DomainCursorStatus, DomainSourceCheckpoint,
            DomainSourceCursorInfo, DomainSourceExpectationInfo,
        },
        enums::domain::{DomainFamily, DomainSourceExpectationStatus},
        types::{
            ContentHash, DomainInstrumentKey, DomainSourceExpectationId, DomainSourceId,
            ResearchProfileId,
        },
    };

    fn hash(fill: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", fill.to_string().repeat(64))).expect("hash")
    }

    fn expectation(
        status: DomainSourceExpectationStatus,
        status_reason: Option<&str>,
    ) -> DomainSourceExpectationInfo {
        let now = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();
        DomainSourceExpectationInfo {
            expectation_id: DomainSourceExpectationId::from_v7(),
            family: DomainFamily::Weather,
            source_id: DomainSourceId::aviation_weather(),
            instrument_key: DomainInstrumentKey::new("METAR:KLGA"),
            capability_registry_hash: hash('a'),
            binding_hash: hash('b'),
            required: true,
            credential_required: false,
            freshness_secs: 900,
            affected_market_ids: AffectedMarketIds::default(),
            affected_profile_ids: AffectedProfileIds::new(vec![ResearchProfileId::new(
                "weather_forecast_24h",
            )]),
            status,
            status_reason: status_reason.map(str::to_owned),
            created_at: now,
            updated_at: now,
        }
    }

    fn cursor(event_time: DateTime<Utc>) -> DomainSourceCursorInfo {
        DomainSourceCursorInfo {
            source_id: DomainSourceId::aviation_weather(),
            instrument_key: DomainInstrumentKey::new("METAR:KLGA"),
            checkpoint_json: DomainSourceCheckpoint::AviationWeather {
                available_at: event_time,
                published_at: event_time,
                observation_time: event_time,
                revision: 1,
                report_hash: hash('c'),
            },
            checkpoint_hash: hash('d'),
            status: DomainCursorStatus::Live,
            last_error: None,
            created_at: event_time,
            updated_at: event_time,
        }
    }

    #[test]
    fn missing_cursor_is_unknown_not_zero_lag() {
        let observed_at = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();
        let view = DomainSourceExpectationView::from_expected_and_observed(
            expectation(DomainSourceExpectationStatus::NotStarted, None),
            None,
            observed_at,
        );
        assert_eq!(view.status, DomainSourceExpectationStatus::NotStarted);
        assert_eq!(view.status_reason.as_deref(), Some("cursor_not_created"));
        assert_eq!(view.lag_secs, None);
        assert_eq!(view.last_event_time, None);
    }

    #[test]
    fn observed_freshness_and_declared_blockers_fail_closed() {
        let observed_at = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();
        let stale = DomainSourceExpectationView::from_expected_and_observed(
            expectation(DomainSourceExpectationStatus::Live, None),
            Some(cursor(observed_at - Duration::seconds(901))),
            observed_at,
        );
        assert_eq!(stale.status, DomainSourceExpectationStatus::Stale);

        let blocked = DomainSourceExpectationView::from_expected_and_observed(
            expectation(
                DomainSourceExpectationStatus::CredentialBlocked,
                Some("credential_required"),
            ),
            Some(cursor(observed_at)),
            observed_at,
        );
        assert_eq!(
            blocked.status,
            DomainSourceExpectationStatus::CredentialBlocked
        );
        assert_eq!(
            blocked.status_reason.as_deref(),
            Some("credential_required")
        );
    }

    #[test]
    fn historical_archive_health_uses_successful_refresh_not_old_event_time() {
        let observed_at = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();
        let event_time = Utc.with_ymd_and_hms(2021, 8, 29, 18, 0, 0).unwrap();
        let mut archive_cursor = cursor(event_time);
        archive_cursor.checkpoint_json = DomainSourceCheckpoint::NhcHurdat2 {
            last_observation: event_time,
            collection_date: NaiveDate::from_ymd_opt(2026, 4, 2).expect("date"),
            file_hash: hash('e'),
        };
        archive_cursor.updated_at = observed_at - Duration::seconds(10);
        let view = DomainSourceExpectationView::from_expected_and_observed(
            expectation(DomainSourceExpectationStatus::Live, None),
            Some(archive_cursor),
            observed_at,
        );
        assert_eq!(view.last_event_time, Some(event_time));
        assert_eq!(
            view.freshness_observed_at,
            Some(observed_at - Duration::seconds(10))
        );
        assert_eq!(view.lag_secs, Some(10));
        assert_eq!(view.status, DomainSourceExpectationStatus::Live);
    }
}
