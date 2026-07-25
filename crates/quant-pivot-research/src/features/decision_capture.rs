//! Decision-time market capture frozen at PIT resolve.
//!
//! [`MarketDecisionCapture`] is the single source of truth for report readability
//! fields (identity, market context, book replay handle) and is built alongside
//! [`ResolvedInputs`](super::builder::ResolvedInputs) — never re-derived in the composer.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        data_plane::DecisionBoundary,
        market::{book::BookLevel, registry::MarketRegistryInfo},
        quant::{FeatureVectorInfo, NewReportDataQualitySnapshot, ResolvedBinding},
    },
    enums::quant::DataQualityStatus,
    hashing::CanonicalDigest,
    types::{
        BookSnapshotRef, BookSnapshotSource, Bps, CatalogDecisionRef, ContentHash,
        DecisionCaptureEvidence, DecisionPolicySnapshotId, DecisionSnapshotEvidence, EventId,
        MarketContext, MarketId, MarketLinkageId, Probability, RecommendationIdentity,
        ReportDataQualitySnapshotId, ReportDataQualityTokens, TokenDataQualityRecord, TokenId, Usd,
    },
};
use rust_decimal::Decimal;
use serde::Serialize;

use super::DomainSliceInputs;
use crate::{
    features::{
        FeatureName, FeatureVector, MarketWindowSnapshot, NullReason,
        builder::ResolvedInputs,
        resolved::{ResolvedBook, ResolvedMarketContext},
        value::{EvidenceSourceKind, EvidenceSourceRef},
    },
    hashing::ResearchHasher,
    pit::ResolvedMarketSnapshot,
    selection::SelectedMarket,
};

impl From<&ResolvedMarketSnapshot> for CatalogDecisionRef {
    fn from(snapshot: &ResolvedMarketSnapshot) -> Self {
        Self {
            catalog_sync_batch_id: snapshot.catalog_sync_batch_id,
            market_change_id: snapshot.market_change_id,
            event_change_id: snapshot.event_change_id,
            market_content_hash: snapshot.market_content_hash,
            event_content_hash: snapshot.event_content_hash,
            membership_hash: snapshot.membership_hash,
            market_effective_at: snapshot.market_effective_at,
            market_available_at: snapshot.market_available_at,
            event_effective_at: snapshot.event_effective_at,
            event_available_at: snapshot.event_available_at,
            market_timestamp_quality: snapshot.market_timestamp_quality,
            event_timestamp_quality: snapshot.event_timestamp_quality,
        }
    }
}

/// Frozen decision-time bundle for one market: PIT inputs + capture payload.
pub struct ResolvedMarketBundle<'a> {
    /// Source-agnostic inputs for the pure feature build step.
    pub inputs: ResolvedInputs<'a>,
    /// Decision capture frozen at resolve (identity, book ref, market context).
    pub capture: MarketDecisionCapture,
}

/// Everything frozen at feature resolve for downstream report composition.
#[derive(Debug, Clone)]
pub struct MarketDecisionCapture {
    /// Market the capture describes.
    pub market_id: MarketId,
    /// Owning event id frozen at decision time.
    pub event_id: EventId,
    /// Primary outcome token.
    pub token_id: TokenId,
    /// Exact PIT linkage revision, present only for a resolved vertical slice.
    pub market_linkage_id: Option<MarketLinkageId>,
    pub market_linkage_hash: Option<ContentHash>,
    /// Full typed subject and source roles from that same PIT linkage revision.
    pub domain_binding: Option<ResolvedBinding>,
    /// Order book resolved at `as_of`.
    pub book: ResolvedBook,
    /// Market metadata resolved at `as_of`.
    pub market: ResolvedMarketContext,
    /// Human-readable identity for the recommendation.
    pub identity: RecommendationIdentity,
    /// Fully materialized market context.
    pub market_context: MarketContext,
    /// Replay handle for the frozen book.
    pub book_snapshot_ref: BookSnapshotRef,
    /// Aggregate data quality after feature classification (updated post-build).
    pub data_quality: DataQualityStatus,
    /// Normalized visible liquidity in `[0, 1]` vs the configured cap.
    pub liquidity_score: Probability,
    /// Durable source snapshot identity shared by online and replay.
    pub snapshot: DecisionSnapshotEvidence,
}

impl MarketDecisionCapture {
    #[must_use]
    pub fn evidence(&self) -> DecisionCaptureEvidence {
        DecisionCaptureEvidence {
            snapshot: self.snapshot.clone(),
            identity: self.identity.clone(),
            market_context: self.market_context.clone(),
            data_quality: self.data_quality,
            liquidity_score: self.liquidity_score,
        }
    }

    pub fn evidence_hash(&self) -> QuantResult<ContentHash> {
        ResearchHasher::canonical(&self.evidence())
    }
}

/// Build [`MarketContext`] from resolved book, metadata, and selection snapshot fields.
pub fn market_context_from_resolved(
    as_of: DateTime<Utc>,
    book: &ResolvedBook,
    market: &ResolvedMarketContext,
    selected: &SelectedMarket,
    registry: Option<&MarketRegistryInfo>,
) -> QuantResult<MarketContext> {
    let spread_bps = match (book.best_bid(), book.best_ask(), book.mid()) {
        (Some(bid), Some(ask), Some(mid)) if mid.inner() > Decimal::ZERO => {
            Bps::relative(ask.inner() - bid.inner(), mid.inner()).map(Bps::inner)
        }
        _ => None,
    };
    let book_age_ms = book_age_ms(as_of, book);
    let time_to_resolution_secs = market
        .end_date
        .map(|end| {
            u64::try_from((end - as_of).num_seconds()).map_err(|error| {
                ResearchError::PitResolution {
                    detail: format!(
                        "market {} resolution time {end} is before decision_at {as_of} or cannot be represented: {error}",
                        selected.market_id
                    ),
                }
            })
        })
        .transpose()?;
    let fee_rate = market
        .fee_schedule
        .as_ref()
        .map(|schedule| schedule.platform_rate);

    Ok(MarketContext {
        best_bid: book.best_bid(),
        best_ask: book.best_ask(),
        mid_price: book.mid(),
        spread_bps: spread_bps.map(Bps::new),
        depth_usd: book.visible_liquidity_usd(),
        volume_24h_usd: selected
            .volume_24h_usd
            .or_else(|| registry.and_then(|info| info.volume_24h)),
        book_age_ms: book_age_ms?,
        time_to_resolution_secs,
        market_status: market.status,
        neg_risk: market.neg_risk,
        tick_size: registry
            .ok_or_else(|| ResearchError::PitResolution {
                detail: format!(
                    "market {} has no tick-size registry metadata",
                    selected.market_id
                ),
            })?
            .tick_size,
        fee_rate,
    })
}

/// Build display identity from the selection row and registry metadata.
pub fn recommendation_identity_from_resolved(
    selected: &SelectedMarket,
    registry: Option<&MarketRegistryInfo>,
) -> QuantResult<RecommendationIdentity> {
    let info = registry.ok_or_else(|| ResearchError::PitResolution {
        detail: format!(
            "market {} has no point-in-time catalog identity",
            selected.market_id
        ),
    })?;
    let outcome_name = info
        .tokens
        .iter()
        .find(|token| token.token_id == selected.primary_token_id)
        .map(|token| token.outcome.clone())
        .or_else(|| info.outcome.clone())
        .filter(|outcome| !outcome.trim().is_empty())
        .ok_or_else(|| ResearchError::PitResolution {
            detail: format!(
                "market {} has no outcome identity for token {}",
                selected.market_id, selected.primary_token_id
            ),
        })?;
    if info.question.trim().is_empty() {
        return Err(ResearchError::PitResolution {
            detail: format!(
                "market {} has an empty catalog question",
                selected.market_id
            ),
        }
        .into());
    }
    Ok(RecommendationIdentity {
        category: selected.category,
        question: info.question.clone(),
        outcome_name,
    })
}

impl ResolvedBook {
    /// Build a live [`BookSnapshotRef`] with a blake3 digest over bid/ask levels.
    ///
    /// # Errors
    ///
    /// Propagates canonical JSON serialization failures for the level digest.
    pub fn snapshot_ref(&self) -> QuantResult<BookSnapshotRef> {
        let source_event =
            self.source_event
                .as_ref()
                .ok_or_else(|| ResearchError::PitResolution {
                    detail: format!("book {} has no canonical L2 event identity", self.token_id),
                })?;
        Ok(BookSnapshotRef {
            token_id: self.token_id.clone(),
            source: BookSnapshotSource::CanonicalL2 {
                stream_session_id: source_event.stream_session_id,
                token_sequence: source_event.token_sequence,
                source_event_hash: source_event.source_event_hash,
                event_time_ms: i64::try_from(self.timestamp_ms).map_err(|error| {
                    ResearchError::PitResolution {
                        detail: format!(
                            "book {} event time does not fit i64: {error}",
                            self.token_id
                        ),
                    }
                })?,
                ingestion_time_ms: self.available_at.timestamp_millis(),
            },
            content_hash: (self).book_levels_content_hash()?,
        })
    }
}

/// Map visible book liquidity to a normalized score in `[0, 1]`.
#[must_use]
pub fn liquidity_score_from_resolved(book: &ResolvedBook, liquidity_cap_usd: Usd) -> Probability {
    let cap = liquidity_cap_usd.inner();
    if cap <= Decimal::ZERO {
        return Probability::new(Decimal::ONE);
    }
    let visible = book.visible_liquidity_usd().inner();
    let ratio = (visible / cap).clamp(Decimal::ZERO, Decimal::ONE);
    Probability::new(ratio)
}

/// Canonical book evidence string shared by feature provenance and report evidence.
#[must_use]
pub fn book_evidence_ref(
    book_snapshot_ref: &BookSnapshotRef,
    effective_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
) -> EvidenceSourceRef {
    EvidenceSourceRef {
        source_kind: EvidenceSourceKind::Book,
        reference: book_snapshot_ref.canonical_string(),
        effective_at,
        available_at: Some(available_at),
    }
}

/// Assemble a full capture from resolved inputs and registry metadata.
///
/// # Errors
///
/// Propagates book content-hash failures.
pub struct MarketDecisionCaptureInput<'a> {
    pub boundary: &'a DecisionBoundary,
    pub selected: &'a SelectedMarket,
    pub book: ResolvedBook,
    pub market: ResolvedMarketContext,
    pub registry: Option<&'a MarketRegistryInfo>,
    pub catalog: CatalogDecisionRef,
    pub domain: Option<&'a DomainSliceInputs>,
    pub liquidity_cap_usd: Usd,
}

pub fn capture_market_decision(
    input: MarketDecisionCaptureInput<'_>,
) -> QuantResult<MarketDecisionCapture> {
    let MarketDecisionCaptureInput {
        boundary,
        selected,
        book,
        market,
        registry,
        catalog,
        domain,
        liquidity_cap_usd,
    } = input;
    let as_of = boundary.decision_at();
    let book_snapshot_ref = book.snapshot_ref()?;
    let identity = recommendation_identity_from_resolved(selected, registry)?;
    let market_context = market_context_from_resolved(as_of, &book, &market, selected, registry)?;
    let liquidity_score = liquidity_score_from_resolved(&book, liquidity_cap_usd);
    let book_effective_at = book.effective_at;
    let book_available_at = book.available_at;
    Ok(MarketDecisionCapture {
        market_id: selected.market_id.clone(),
        event_id: selected.event_id.clone(),
        token_id: selected.primary_token_id.clone(),
        market_linkage_id: domain.map(|inputs| inputs.linkage_id),
        market_linkage_hash: domain.map(|inputs| inputs.linkage_hash),
        domain_binding: domain.map(|inputs| inputs.binding.clone()),
        book,
        market,
        identity,
        market_context,
        book_snapshot_ref: book_snapshot_ref.clone(),
        data_quality: DataQualityStatus::Fresh,
        liquidity_score,
        snapshot: DecisionSnapshotEvidence {
            boundary: boundary.clone(),
            market_id: selected.market_id.clone(),
            event_id: selected.event_id.clone(),
            token_id: selected.primary_token_id.clone(),
            catalog,
            book_snapshot_ref,
            book_effective_at,
            book_available_at,
            selection: selected.into(),
        },
    })
}

/// Draft the report-level DQ snapshot covering every market in the round.
pub fn draft_data_quality_snapshot(
    as_of: DateTime<Utc>,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    bundles: &[ResolvedMarketBundle<'_>],
    vectors: &[FeatureVector],
    persisted: &[FeatureVectorInfo],
    rejected_markets: &[RejectedMarketDraft],
) -> QuantResult<NewReportDataQualitySnapshot> {
    if bundles.len() != vectors.len() || vectors.len() != persisted.len() {
        return Err(ResearchError::Determinism {
            detail: format!(
                "data-quality snapshot alignment mismatch: bundles={}, vectors={}, persisted={}",
                bundles.len(),
                vectors.len(),
                persisted.len()
            ),
        }
        .into());
    }
    let rejected_by_market: HashMap<_, _> = rejected_markets
        .iter()
        .map(|row| (row.market_id.clone(), row))
        .collect();
    let records = bundles
        .iter()
        .zip(vectors)
        .zip(persisted)
        .map(
            |((bundle, vector), persisted)| -> QuantResult<TokenDataQualityRecord> {
                let wrong_market = persisted.market_id != vector.market_id;
                let wrong_token = persisted.token_id.as_ref() != vector.token_id.as_ref();
                let wrong_decision = persisted.decision_at != as_of;
                let wrong_data_quality = persisted.data_quality != vector.data_quality;
                if wrong_market || wrong_token || wrong_decision || wrong_data_quality {
                    return Err(ResearchError::Determinism {
                        detail: format!(
                            "persisted feature vector {} is not aligned with DQ row for market {}",
                            persisted.feature_vector_id, vector.market_id
                        ),
                    }
                    .into());
                }
                let book = &bundle.capture.book;
                let missing = rejected_by_market
                    .get(&vector.market_id)
                    .map(|row| {
                        row.missing_required
                            .iter()
                            .map(|(name, _)| name.to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(TokenDataQualityRecord {
                    feature_vector_id: Some(persisted.feature_vector_id),
                    token_id: bundle.capture.token_id.clone(),
                    market_id: bundle.capture.market_id.clone(),
                    status: vector.data_quality,
                    book_age_ms: book_age_ms(as_of, book)?,
                    crossed: book.is_crossed(),
                    empty: book.is_empty(),
                    fact_lag_ms: fact_lag_ms(as_of, bundle.inputs.window)?,
                    missing_required: missing,
                })
            },
        )
        .collect::<QuantResult<Vec<_>>>()?;
    Ok(NewReportDataQualitySnapshot {
        report_data_quality_snapshot_id: ReportDataQualitySnapshotId::from_v7(),
        decision_at: as_of,
        decision_policy_snapshot_id,
        tokens_json: ReportDataQualityTokens(records),
    })
}

/// Rejected market summary used when drafting the DQ snapshot (mirrors core partition).
pub struct RejectedMarketDraft {
    /// Excluded market id.
    pub market_id: MarketId,
    /// Required features that were missing.
    pub missing_required: Vec<(FeatureName, NullReason)>,
}

#[derive(Serialize)]
struct BookLevelsDigest<'a> {
    bids: &'a [BookLevel],
    asks: &'a [BookLevel],
}

impl ResolvedBook {
    fn book_levels_content_hash(&self) -> QuantResult<ContentHash> {
        CanonicalDigest::content_hash_json(&BookLevelsDigest {
            bids: &self.bids,
            asks: &self.asks,
        })
        .map_err(Into::into)
    }
}

fn book_age_ms(as_of: DateTime<Utc>, book: &ResolvedBook) -> QuantResult<u64> {
    u64::try_from((as_of - book.effective_at).num_milliseconds()).map_err(|_| {
        ResearchError::PitResolution {
            detail: format!(
                "book observation {} is after decision time {as_of}",
                book.effective_at
            ),
        }
        .into()
    })
}

fn fact_lag_ms(as_of: DateTime<Utc>, window: &MarketWindowSnapshot) -> QuantResult<Option<u64>> {
    window
        .freshest_bucket_time()
        .map(|bucket_time| {
            u64::try_from((as_of - bucket_time).num_milliseconds()).map_err(|_| {
                ResearchError::PitResolution {
                    detail: format!("feature bucket {bucket_time} is after decision time {as_of}"),
                }
                .into()
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use quant_pivot_models::{
        domain::market::{TokenInfo, book::BookLevel, registry::MarketRegistryInfo},
        enums::{
            catalog::CatalogFilterReasonSet,
            common::{CategorySet, MarketCategory, TickSize},
            market::MarketStatus,
        },
        types::{ContentHash, EventId, MarketId, Price, Shares, TokenId, Usd},
    };
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use super::{
        liquidity_score_from_resolved, market_context_from_resolved,
        recommendation_identity_from_resolved,
    };
    use crate::{
        features::{ResolvedBook, resolved::ResolvedMarketContext},
        pit::CanonicalBookEventRef,
        selection::SelectedMarket,
    };

    impl ResolvedBook {
        fn test_fixture() -> Self {
            let token = TokenId::new("123");
            Self {
                token_id: token,
                bids: Arc::new([BookLevel::from_decimal(
                    Price::new(dec!(0.48)),
                    Shares::new(dec!(100)),
                )
                .expect("level")]),
                asks: Arc::new([BookLevel::from_decimal(
                    Price::new(dec!(0.52)),
                    Shares::new(dec!(100)),
                )
                .expect("level")]),
                timestamp_ms: 1_700_000_000_000,
                version: 42,
                sequence: 42,
                source_event: Some(CanonicalBookEventRef {
                    stream_session_id: Uuid::from_u128(1),
                    token_sequence: 42,
                    source_event_hash: ContentHash::parse(&format!("blake3:{}", "d".repeat(64)))
                        .expect("canonical event hash"),
                }),
                effective_at: Utc::now(),
                available_at: Utc::now(),
            }
        }
    }

    fn sample_registry() -> MarketRegistryInfo {
        MarketRegistryInfo {
            market_id: MarketId::new("0xm"),
            event_id: EventId::new("evt"),
            token_yes: TokenId::new("yes-token"),
            token_no: TokenId::new("no-token"),
            question: "Will it happen?".to_owned(),
            slug: "slug".to_owned(),
            description: None,
            categories: CategorySet::default(),
            status: MarketStatus::Active,
            filter_reasons: CatalogFilterReasonSet::default(),
            outcome: None,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: TokenId::new("yes-token"),
                    outcome: "Yes".to_owned(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: TokenId::new("no-token"),
                    outcome: "No".to_owned(),
                    neg_risk: false,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: dec!(1),
            liquidity_usd: None,
            volume_24h: None,
            start_date: None,
            end_date: None,
            resolved_at: None,
            created_at: Some(Utc::now()),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn book_snapshot_ref_stable() {
        let book = ResolvedBook::test_fixture();
        let first = book.snapshot_ref().expect("hash");
        let second = book.snapshot_ref().expect("hash");
        assert_eq!(first, second);
        assert!(first.canonical_string().starts_with("book:l2|"));
    }

    #[test]
    fn market_context_materializes_fields() {
        let as_of = Utc::now();
        let book = ResolvedBook::test_fixture();
        let market = ResolvedMarketContext {
            market_id: MarketId::new("0xm"),
            effective_at: as_of,
            available_at: as_of,
            status: MarketStatus::Active,
            neg_risk: false,
            start_date: None,
            end_date: None,
            created_at: Some(as_of),
            fee_schedule: None,
        };
        let selected = SelectedMarket {
            market_id: MarketId::new("0xm"),
            event_id: EventId::new("evt"),
            category: MarketCategory::Sports,
            primary_token_id: TokenId::new("123"),
            secondary_token_id: None,
            liquidity_usd: Some(Usd::new(dec!(5000))),
            volume_24h_usd: Some(Usd::new(dec!(1000))),
            source_refs: Vec::new(),
        };
        let registry = sample_registry();
        let ctx = market_context_from_resolved(as_of, &book, &market, &selected, Some(&registry))
            .expect("context");
        assert_eq!(ctx.depth_usd, book.visible_liquidity_usd());
        assert!(ctx.spread_bps.is_some());
        assert_eq!(ctx.volume_24h_usd, selected.volume_24h_usd);
        assert_eq!(ctx.tick_size, TickSize::Hundredth);
    }

    #[test]
    fn identity_reads_registry_outcome() {
        let selected = SelectedMarket {
            market_id: MarketId::new("0xm"),
            event_id: EventId::new("evt"),
            category: MarketCategory::Politics,
            primary_token_id: TokenId::new("yes-token"),
            secondary_token_id: Some(TokenId::new("no-token")),
            liquidity_usd: None,
            volume_24h_usd: None,
            source_refs: Vec::new(),
        };
        let registry = sample_registry();
        let identity =
            recommendation_identity_from_resolved(&selected, Some(&registry)).expect("identity");
        assert_eq!(identity.question, "Will it happen?");
        assert_eq!(identity.outcome_name, "Yes");
        assert_eq!(identity.category, MarketCategory::Politics);
    }

    #[test]
    fn liquidity_score_clamps_interval() {
        let book = ResolvedBook::test_fixture();
        let score = liquidity_score_from_resolved(&book, Usd::new(dec!(10)));
        assert!(score.inner() <= dec!(1));
        assert!(score.inner() > dec!(0));
        let no_levels = ResolvedBook {
            token_id: TokenId::new("t"),
            bids: Arc::new([]),
            asks: Arc::new([]),
            timestamp_ms: 1,
            version: 1,
            sequence: 1,
            source_event: None,
            effective_at: Utc::now(),
            available_at: Utc::now(),
        };
        let empty = liquidity_score_from_resolved(&no_levels, Usd::new(dec!(100)));
        assert_eq!(empty.inner(), dec!(0));
    }
}
