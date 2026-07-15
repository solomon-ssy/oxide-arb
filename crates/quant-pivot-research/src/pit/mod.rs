//! Unified point-in-time query contract: [`PointInTimeSnapshotSource`].
//!
//! PIT correctness is a hard invariant: an implementation must never return
//! state newer than the requested source cutoff. Production serving and replay
//! both resolve from durable `ClickHouse` facts plus the append-only Postgres
//! catalog ledger. The in-memory [`MaterializedPitEngine`] serves a pre-fetched
//! immutable window so dataset construction never performs per-row I/O.

use std::{collections::HashMap, fmt::Display, sync::Arc};
mod materialized;

pub use materialized::MaterializedPitEngine;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        CatalogMarketLeg, CatalogSnapshotInfo, DecisionBoundary, DecisionSource,
        MarketCatalogVersionInfo,
        market::{
            book::BookLevel,
            fee::MarketFeeSchedule,
            registry::{EventRegistryInfo, MarketRegistryInfo, NegRiskLegSet},
        },
    },
    enums::market::MarketStatus,
    types::{
        CatalogSyncBatchId, ContentHash, EventCatalogVersionId, MarketCatalogVersionId, MarketId,
        TokenId,
    },
};
use serde::Serialize;
use uuid::Uuid;

use crate::hashing::ResearchHasher;

/// Immutable identity of the latest canonical L2 event applied to a resolved
/// book state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalBookEventRef {
    pub stream_session_id: Uuid,
    pub token_sequence: u64,
    pub source_event_hash: ContentHash,
}

/// A book snapshot resolved strictly as of a past decision time.
///
/// Carries the full level payload so it normalizes into
/// [`ResolvedBook`](crate::features::ResolvedBook) exactly like a live snapshot.
///
/// There is **no** separate `observed_at`: the publish time is `timestamp_ms`,
/// the single source of truth for when the datum was observed. Both the live and
/// historical paths derive `observed_at` from it identically (see
/// [`ResolvedBook`](crate::features::ResolvedBook)), so `book.age_ms` — and thus
/// the feature hash — can never diverge between online and offline builds. A PIT
/// engine must guarantee `timestamp_ms <= source_cutoff` and
/// `available_at <= decision_at` (never look-ahead).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookSnapshotAt {
    /// Token the snapshot describes.
    pub token_id: TokenId,
    /// Source-effective cutoff the engine resolved against.
    pub source_cutoff: DateTime<Utc>,
    /// Decision time governing source availability.
    pub decision_at: DateTime<Utc>,
    /// Bid levels, best-first.
    pub bids: Arc<[BookLevel]>,
    /// Ask levels, best-first.
    pub asks: Arc<[BookLevel]>,
    /// Publish timestamp of the resolved snapshot, in epoch milliseconds
    /// (`<= source_cutoff`); the canonical observed time.
    pub timestamp_ms: u64,
    /// Monotonic publish version of the resolved snapshot.
    pub version: u64,
    /// Stable source sequence for rows sharing effective and availability time.
    pub sequence: u64,
    /// Canonical event identity proving the state belongs to one stream
    /// session. Synthetic/test sources may omit it, but governed evidence
    /// capture fails closed when it is absent.
    pub source_event: Option<CanonicalBookEventRef>,
    /// Time at which the snapshot became visible to the system.
    pub available_at: DateTime<Utc>,
}

/// Market catalog context resolved strictly as of a past decision time.
///
/// Carries the metadata payload so it normalizes into
/// [`ResolvedMarketContext`](crate::features::ResolvedMarketContext).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketContextAt {
    /// Market the context describes.
    pub market_id: MarketId,
    /// Source-effective timestamp of the resolved catalog revision.
    pub effective_at: DateTime<Utc>,
    /// Time at which the catalog revision became visible to the system.
    pub available_at: DateTime<Utc>,
    /// Lifecycle status.
    pub status: MarketStatus,
    /// Whether the market is a neg-risk market.
    pub neg_risk: bool,
    /// Scheduled market start time, when published by Gamma.
    pub start_date: Option<DateTime<Utc>>,
    /// Scheduled resolution time, when known.
    pub end_date: Option<DateTime<Utc>>,
    /// Upstream catalog creation time (event-age proxy). `None` means Gamma did
    /// not publish a source clock; callers must preserve Missing.
    pub created_at: Option<DateTime<Utc>>,
    /// Fee schedule resolved from the independent append-only CLOB market-info ledger.
    pub fee_schedule: Option<MarketFeeSchedule>,
}

/// One immutable catalog projection used by selection, feature computation,
/// decision capture, and structural event-membership resolution.
///
/// The market revision and event revision are linked in the durable ledger and
/// loaded in one repeatable-read transaction. Consumers must project from this
/// value instead of issuing separate metadata queries.
#[derive(Debug, Clone)]
pub struct ResolvedMarketSnapshot {
    /// Boundary that selected every component of this snapshot.
    pub boundary: DecisionBoundary,
    /// Full normalized market metadata at the catalog cutoff.
    pub market: Arc<MarketRegistryInfo>,
    /// Exact event revision referenced by `market`.
    pub event: Arc<EventRegistryInfo>,
    /// Source-agnostic feature context projected from `market`.
    pub context: MarketContextAt,
    /// Expected and materialized neg-risk members from this event revision.
    pub neg_risk_leg_set: NegRiskLegSet,
    /// Atomic synchronization batch owning the market revision.
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    /// Immutable market revision identity.
    pub market_catalog_version_id: MarketCatalogVersionId,
    /// Immutable event revision identity linked by the market revision.
    pub event_catalog_version_id: EventCatalogVersionId,
    /// Canonical normalized market content identity.
    pub market_content_hash: ContentHash,
    /// Canonical normalized event content identity.
    pub event_content_hash: ContentHash,
    /// Canonical identity of the exact event membership revision.
    pub membership_hash: ContentHash,
    /// Upstream timestamp quality labels for the selected revisions.
    pub market_timestamp_quality: String,
    pub event_timestamp_quality: String,
    /// Source-effective and availability clocks of the exact market revision.
    pub market_effective_at: DateTime<Utc>,
    pub market_available_at: DateTime<Utc>,
    /// Source-effective and availability clocks of the linked event revision.
    pub event_effective_at: DateTime<Utc>,
    pub event_available_at: DateTime<Utc>,
}

/// Decode and validate one repository snapshot into the canonical PIT catalog
/// value consumed by both streaming and materialized replay.
pub fn resolve_catalog_snapshot(
    snapshot: CatalogSnapshotInfo,
    boundary: &DecisionBoundary,
) -> QuantResult<ResolvedMarketSnapshot> {
    validate_catalog_visibility(&snapshot, boundary)?;
    let market: MarketRegistryInfo = decode_catalog_payload(
        "market",
        &snapshot.market.market_catalog_version_id,
        snapshot.market.payload.clone(),
    )?;
    let event: EventRegistryInfo = decode_catalog_payload(
        "event",
        &snapshot.event.event_catalog_version_id,
        snapshot.event.payload.clone(),
    )?;
    if snapshot.market.event_catalog_version_id != snapshot.event.event_catalog_version_id
        || market.event_id != event.event_id
        || snapshot.market.event_id != event.event_id
    {
        return Err(ResearchError::PitResolution {
            detail: format!(
                "catalog snapshot linkage mismatch for market {}: market event version {}, event version {}, market event {}, event {}",
                market.market_id,
                snapshot.market.event_catalog_version_id,
                snapshot.event.event_catalog_version_id,
                market.event_id,
                event.event_id
            ),
        }
        .into());
    }
    if !event.market_ids.contains(&market.market_id) {
        return Err(ResearchError::PitResolution {
            detail: format!(
                "catalog event version {} does not contain market {}",
                snapshot.event.event_catalog_version_id, market.market_id
            ),
        }
        .into());
    }

    let membership_hash = catalog_membership_hash(
        &snapshot.event.event_catalog_version_id,
        &event,
        &snapshot.event_markets,
    )?;
    let members = decode_event_members(&snapshot.event_markets)?;
    let neg_risk_leg_set = event_leg_set(&event, &members);
    let context = MarketContextAt {
        market_id: market.market_id.clone(),
        effective_at: snapshot.market.source_effective_at,
        available_at: snapshot.market.available_at,
        status: market.status,
        neg_risk: market.neg_risk,
        start_date: market.start_date,
        end_date: market.end_date,
        created_at: snapshot.market.source_created_at,
        fee_schedule: None,
    };
    Ok(ResolvedMarketSnapshot {
        boundary: boundary.clone(),
        market: Arc::new(market),
        event: Arc::new(event),
        context,
        neg_risk_leg_set,
        catalog_sync_batch_id: snapshot.market.catalog_sync_batch_id,
        market_catalog_version_id: snapshot.market.market_catalog_version_id,
        event_catalog_version_id: snapshot.event.event_catalog_version_id,
        market_content_hash: snapshot.market.content_hash,
        event_content_hash: snapshot.event.content_hash,
        membership_hash,
        market_timestamp_quality: snapshot.market.source_timestamp_quality,
        event_timestamp_quality: snapshot.event.source_timestamp_quality,
        market_effective_at: snapshot.market.source_effective_at,
        market_available_at: snapshot.market.available_at,
        event_effective_at: snapshot.event.source_effective_at,
        event_available_at: snapshot.event.available_at,
    })
}

fn validate_catalog_visibility(
    snapshot: &CatalogSnapshotInfo,
    boundary: &DecisionBoundary,
) -> QuantResult<()> {
    let source_cutoff = boundary.cutoff_for(DecisionSource::Catalog);
    let decision_at = boundary.decision_at();
    let visible = |effective_at: DateTime<Utc>, available_at: DateTime<Utc>| {
        effective_at <= source_cutoff && available_at <= decision_at
    };
    if !visible(
        snapshot.market.source_effective_at,
        snapshot.market.available_at,
    ) {
        return Err(ResearchError::PitResolution {
            detail: format!(
                "market catalog version {} is outside boundary: effective {}, available {}, source cutoff {source_cutoff}, decision {decision_at}",
                snapshot.market.market_catalog_version_id,
                snapshot.market.source_effective_at,
                snapshot.market.available_at,
            ),
        }
        .into());
    }
    if !visible(
        snapshot.event.source_effective_at,
        snapshot.event.available_at,
    ) {
        return Err(ResearchError::PitResolution {
            detail: format!(
                "event catalog version {} is outside boundary: effective {}, available {}, source cutoff {source_cutoff}, decision {decision_at}",
                snapshot.event.event_catalog_version_id,
                snapshot.event.source_effective_at,
                snapshot.event.available_at,
            ),
        }
        .into());
    }
    if let Some(member) = snapshot
        .event_markets
        .iter()
        .find(|member| !visible(member.source_effective_at, member.available_at))
    {
        return Err(ResearchError::PitResolution {
            detail: format!(
                "event member market catalog version {} is outside boundary: effective {}, available {}, source cutoff {source_cutoff}, decision {decision_at}",
                member.market_catalog_version_id,
                member.source_effective_at,
                member.available_at,
            ),
        }
        .into());
    }
    Ok(())
}

fn catalog_membership_hash(
    event_version_id: &EventCatalogVersionId,
    event: &EventRegistryInfo,
    versions: &[MarketCatalogVersionInfo],
) -> QuantResult<ContentHash> {
    #[derive(Serialize)]
    struct Member<'a> {
        market_id: &'a MarketId,
        market_catalog_version_id: &'a MarketCatalogVersionId,
        content_hash: &'a ContentHash,
    }

    #[derive(Serialize)]
    struct Membership<'a> {
        event_catalog_version_id: &'a EventCatalogVersionId,
        expected_market_ids: Vec<&'a MarketId>,
        materialized_members: Vec<Member<'a>>,
    }

    let mut expected_market_ids = event.market_ids.iter().collect::<Vec<_>>();
    expected_market_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    let mut materialized_members = versions
        .iter()
        .map(|version| Member {
            market_id: &version.market_id,
            market_catalog_version_id: &version.market_catalog_version_id,
            content_hash: &version.content_hash,
        })
        .collect::<Vec<_>>();
    materialized_members
        .sort_by(|left, right| left.market_id.as_str().cmp(right.market_id.as_str()));
    ResearchHasher::canonical(&Membership {
        event_catalog_version_id: event_version_id,
        expected_market_ids,
        materialized_members,
    })
}

fn decode_event_members(
    versions: &[MarketCatalogVersionInfo],
) -> QuantResult<HashMap<MarketId, MarketRegistryInfo>> {
    versions
        .iter()
        .map(|version| {
            decode_catalog_payload::<MarketRegistryInfo>(
                "market",
                &version.market_catalog_version_id,
                version.payload.clone(),
            )
            .map(|market| (market.market_id.clone(), market))
        })
        .collect()
}

fn decode_catalog_payload<T>(
    entity: &'static str,
    version_id: &impl Display,
    payload: serde_json::Value,
) -> QuantResult<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(payload).map_err(|error| {
        ResearchError::PitResolution {
            detail: format!("{entity} catalog version {version_id} payload is invalid: {error}"),
        }
        .into()
    })
}

fn event_leg_set(
    event: &EventRegistryInfo,
    members: &HashMap<MarketId, MarketRegistryInfo>,
) -> NegRiskLegSet {
    if !event.neg_risk {
        return NegRiskLegSet::empty();
    }
    NegRiskLegSet::from_catalog(&event.market_ids, |market_id| {
        members.get(market_id).map(|market| {
            if market.neg_risk {
                CatalogMarketLeg::NegRisk {
                    yes_token_id: market.token_yes.clone(),
                }
            } else {
                CatalogMarketLeg::NonNegRisk
            }
        })
    })
}

/// Resolves durable book and catalog context with no look-ahead.
#[async_trait]
pub trait PointInTimeSnapshotSource: Send + Sync {
    /// The book visible at the boundary's already-frozen source cutoff and
    /// decision-time availability watermark.
    ///
    /// Implementations must constrain both clocks. The boundary is the sole
    /// public PIT entrypoint and performs no lag subtraction.
    async fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>>;

    /// Resolve the freshest visible book for every requested token at the same
    /// frozen boundary.
    async fn books_at_boundary(
        &self,
        token_ids: &[TokenId],
        boundary: &DecisionBoundary,
    ) -> QuantResult<HashMap<TokenId, BookSnapshotAt>> {
        let mut books = HashMap::with_capacity(token_ids.len());
        for token_id in token_ids {
            if let Some(book) = self.book_at_boundary(token_id, boundary).await? {
                books.insert(token_id.clone(), book);
            }
        }
        Ok(books)
    }

    /// Resolve full market/event metadata and membership from one immutable
    /// durable catalog snapshot.
    ///
    /// Engines that cannot supply a versioned catalog must return a PIT error
    /// from their production path rather than fabricate current metadata.
    async fn market_snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<ResolvedMarketSnapshot>> {
        Err(ResearchError::PitResolution {
            detail: format!(
                "PIT engine has no durable catalog snapshot for market {market_id} at {}",
                boundary.decision_at()
            ),
        }
        .into())
    }

    /// Resolve the complete market candidate set visible at one boundary as one
    /// immutable catalog snapshot.
    async fn market_snapshots_at_boundary(
        &self,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Vec<ResolvedMarketSnapshot>> {
        Err(ResearchError::PitResolution {
            detail: format!(
                "PIT engine cannot enumerate the durable market candidate set at {}",
                boundary.decision_at()
            ),
        }
        .into())
    }
}
