//! Decision-time market capture frozen at PIT resolve.
//!
//! [`MarketDecisionCapture`] is the single source of truth for report readability
//! fields (identity, market context, book replay handle) and is built alongside
//! [`ResolvedInputs`](super::builder::ResolvedInputs) — never re-derived in the composer.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        market::{book::BookLevel, registry::MarketRegistryInfo},
        quant::NewReportDataQualitySnapshot,
    },
    enums::{market::MarketStatus, quant::DataQualityStatus},
    hashing::CanonicalDigest,
    types::{
        BookSnapshotRef, BookSnapshotSource, Bps, ContentHash, EventId, MarketContext, MarketId,
        Probability, RecommendationIdentity, ReportDataQualitySnapshotId, ReportDataQualityTokens,
        RuntimeConfigVersionId, TokenDataQualityRecord, TokenId, Usd,
    },
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    features::{
        builder::ResolvedInputs,
        resolved::{ResolvedBook, ResolvedMarketContext},
        value::EvidenceSourceRef,
    },
    selection::SelectedMarket,
};

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
    /// Order book resolved at `as_of`.
    pub book: ResolvedBook,
    /// Market metadata resolved at `as_of`.
    pub market: ResolvedMarketContext,
    /// Human-readable identity for the recommendation.
    pub identity: RecommendationIdentity,
    /// Fully materialized market context (parent doc §7).
    pub market_context: MarketContext,
    /// Replay handle for the frozen book.
    pub book_snapshot_ref: BookSnapshotRef,
    /// Aggregate data quality after feature classification (updated post-build).
    pub data_quality: DataQualityStatus,
    /// Normalized visible liquidity in `[0, 1]` vs the configured cap.
    pub liquidity_score: Probability,
}

/// Build [`MarketContext`] from resolved book, metadata, and selection snapshot fields.
#[must_use]
pub fn market_context_from_resolved(
    as_of: DateTime<Utc>,
    book: &ResolvedBook,
    market: &ResolvedMarketContext,
    selected: &SelectedMarket,
    registry: Option<&MarketRegistryInfo>,
) -> MarketContext {
    let spread_bps = match (book.best_bid(), book.best_ask(), book.mid()) {
        (Some(bid), Some(ask), Some(mid)) if mid.inner() > Decimal::ZERO => {
            Bps::relative(ask.inner() - bid.inner(), mid.inner()).map(Bps::inner)
        }
        _ => None,
    };
    let book_age_ms = book_age_ms(as_of, book);
    let time_to_resolution_secs = market
        .end_date
        .map(|end| u64::try_from((end - as_of).num_seconds()).unwrap_or(0));
    let fee_rate = registry
        .and_then(|info| info.fee_schedule.as_ref())
        .map(|schedule| schedule.fee_rate);

    MarketContext {
        best_bid: book.best_bid(),
        best_ask: book.best_ask(),
        mid_price: book.mid(),
        spread_bps: spread_bps.map(Bps::new),
        depth_usd: book.visible_liquidity_usd(),
        volume_24h_usd: selected
            .volume_24h_usd
            .or_else(|| registry.and_then(|info| info.volume_24h)),
        book_age_ms,
        time_to_resolution_secs,
        market_status: market.status,
        neg_risk: market.neg_risk,
        fee_rate,
    }
}

/// Build display identity from the selection row and registry metadata.
#[must_use]
pub fn recommendation_identity_from_resolved(
    selected: &SelectedMarket,
    registry: Option<&MarketRegistryInfo>,
) -> RecommendationIdentity {
    let (question, outcome_name) = registry.map_or_else(
        || (String::new(), String::new()),
        |info| {
            let outcome = info
                .tokens
                .iter()
                .find(|token| token.token_id == selected.primary_token_id)
                .map(|token| token.outcome.clone())
                .or_else(|| info.outcome.clone())
                .unwrap_or_default();
            (info.question.clone(), outcome)
        },
    );
    RecommendationIdentity {
        category: selected.category,
        question,
        outcome_name,
    }
}

/// Build a live [`BookSnapshotRef`] with a blake3 digest over bid/ask levels.
///
/// # Errors
///
/// Propagates canonical JSON serialization failures for the level digest.
pub fn book_snapshot_ref_from_resolved(book: &ResolvedBook) -> QuantResult<BookSnapshotRef> {
    Ok(BookSnapshotRef {
        token_id: book.token_id.clone(),
        source: BookSnapshotSource::Live {
            book_version: book.version,
            event_time_ms: book.timestamp_ms,
        },
        content_hash: book_levels_content_hash(book)?,
    })
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
    observed_at: DateTime<Utc>,
) -> EvidenceSourceRef {
    use crate::features::value::EvidenceSourceKind;
    EvidenceSourceRef {
        source_kind: EvidenceSourceKind::Book,
        reference: book_snapshot_ref.canonical_string(),
        observed_at,
    }
}

/// Assemble a full capture from resolved inputs and registry metadata.
///
/// # Errors
///
/// Propagates book content-hash failures.
pub fn market_decision_capture_from_resolved(
    as_of: DateTime<Utc>,
    selected: &SelectedMarket,
    book: ResolvedBook,
    market: ResolvedMarketContext,
    registry: Option<&MarketRegistryInfo>,
    liquidity_cap_usd: Usd,
) -> QuantResult<MarketDecisionCapture> {
    let book_snapshot_ref = book_snapshot_ref_from_resolved(&book)?;
    let identity = recommendation_identity_from_resolved(selected, registry);
    let market_context = market_context_from_resolved(as_of, &book, &market, selected, registry);
    let liquidity_score = liquidity_score_from_resolved(&book, liquidity_cap_usd);
    Ok(MarketDecisionCapture {
        market_id: selected.market_id.clone(),
        event_id: selected.event_id.clone(),
        token_id: selected.primary_token_id.clone(),
        book,
        market,
        identity,
        market_context,
        book_snapshot_ref,
        data_quality: DataQualityStatus::Fresh,
        liquidity_score,
    })
}

/// Draft the report-level DQ snapshot covering every market in the round.
#[must_use]
pub fn draft_data_quality_snapshot(
    as_of: DateTime<Utc>,
    runtime_config_version_id: RuntimeConfigVersionId,
    bundles: &[ResolvedMarketBundle<'_>],
    vectors: &[crate::features::FeatureVector],
    rejected_markets: &[RejectedMarketDraft],
) -> NewReportDataQualitySnapshot {
    let rejected_by_market: std::collections::HashMap<_, _> = rejected_markets
        .iter()
        .map(|row| (row.market_id.clone(), row))
        .collect();
    let records = bundles
        .iter()
        .zip(vectors)
        .map(|(bundle, vector)| {
            let book = &bundle.capture.book;
            let missing = rejected_by_market
                .get(&vector.market_id)
                .map(|row| {
                    row.missing_required
                        .iter()
                        .map(|(name, _)| name.as_str().to_owned())
                        .collect()
                })
                .unwrap_or_default();
            TokenDataQualityRecord {
                token_id: bundle.capture.token_id.clone(),
                market_id: bundle.capture.market_id.clone(),
                status: vector.data_quality,
                book_age_ms: book_age_ms(as_of, book),
                crossed: book.is_crossed(),
                empty: book.is_empty(),
                fact_lag_ms: Some(fact_lag_ms(as_of, bundle.inputs.window)),
                missing_required: missing,
            }
        })
        .collect();
    NewReportDataQualitySnapshot {
        report_data_quality_snapshot_id: ReportDataQualitySnapshotId::from_v7(),
        as_of,
        runtime_config_version_id,
        tokens_json: ReportDataQualityTokens(records),
    }
}

/// Rejected market summary used when drafting the DQ snapshot (mirrors core partition).
pub struct RejectedMarketDraft {
    /// Excluded market id.
    pub market_id: MarketId,
    /// Required features that were missing.
    pub missing_required: Vec<(crate::features::FeatureName, crate::features::NullReason)>,
}

/// Empty book stub when PIT resolve returns no snapshot (DQ flags `empty`).
#[must_use]
pub fn empty_book(token_id: TokenId, as_of: DateTime<Utc>) -> ResolvedBook {
    ResolvedBook {
        token_id,
        bids: Arc::new([]),
        asks: Arc::new([]),
        timestamp_ms: 0,
        version: 0,
        observed_at: as_of,
    }
}

/// Minimal market context when registry metadata is unavailable at resolve.
#[must_use]
pub const fn stub_market_context(
    market_id: MarketId,
    as_of: DateTime<Utc>,
) -> ResolvedMarketContext {
    ResolvedMarketContext {
        market_id,
        observed_at: as_of,
        status: MarketStatus::Active,
        neg_risk: false,
        end_date: None,
        created_at: as_of,
        outcome_count: 2,
    }
}

#[derive(Serialize)]
struct BookLevelsDigest<'a> {
    bids: &'a [BookLevel],
    asks: &'a [BookLevel],
}

fn book_levels_content_hash(book: &ResolvedBook) -> QuantResult<ContentHash> {
    CanonicalDigest::content_hash_json(&BookLevelsDigest {
        bids: &book.bids,
        asks: &book.asks,
    })
    .map_err(Into::into)
}

fn book_age_ms(as_of: DateTime<Utc>, book: &ResolvedBook) -> u64 {
    u64::try_from((as_of - book.observed_at).num_milliseconds()).unwrap_or(0)
}

fn fact_lag_ms(as_of: DateTime<Utc>, window: &crate::features::MarketWindowSnapshot) -> u64 {
    window.freshest_bucket_time().map_or(0, |bucket_time| {
        u64::try_from((as_of - bucket_time).num_milliseconds()).unwrap_or(0)
    })
}

#[cfg(test)]
mod tests {
    use super::{
        book_snapshot_ref_from_resolved, empty_book, liquidity_score_from_resolved,
        market_context_from_resolved, recommendation_identity_from_resolved,
    };
    use crate::features::resolved::ResolvedMarketContext;
    use crate::selection::SelectedMarket;
    use chrono::Utc;
    use quant_pivot_models::{
        domain::{
            TokenInfo,
            market::{book::BookLevel, registry::MarketRegistryInfo},
        },
        enums::{
            common::{CategorySet, MarketCategory, TickSize},
            market::MarketStatus,
        },
        types::{EventId, MarketId, Price, Shares, TokenId, Usd},
    };
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    fn sample_book() -> super::ResolvedBook {
        let token = TokenId::new("123");
        super::ResolvedBook {
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
            observed_at: Utc::now(),
        }
    }

    #[test]
    fn book_snapshot_ref_hash_is_stable() {
        let book = sample_book();
        let first = book_snapshot_ref_from_resolved(&book).expect("hash");
        let second = book_snapshot_ref_from_resolved(&book).expect("hash");
        assert_eq!(first, second);
        assert!(first.canonical_string().starts_with("book:live:"));
    }

    #[test]
    fn market_context_materializes_core_fields() {
        let as_of = Utc::now();
        let book = sample_book();
        let market = ResolvedMarketContext {
            market_id: MarketId::new("0xm"),
            observed_at: as_of,
            status: MarketStatus::Active,
            neg_risk: false,
            end_date: None,
            created_at: as_of,
            outcome_count: 2,
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
        let ctx = market_context_from_resolved(as_of, &book, &market, &selected, None);
        assert_eq!(ctx.depth_usd, book.visible_liquidity_usd());
        assert!(ctx.spread_bps.is_some());
        assert_eq!(ctx.volume_24h_usd, selected.volume_24h_usd);
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
        let registry = MarketRegistryInfo {
            market_id: MarketId::new("0xm"),
            event_id: EventId::new("evt"),
            token_yes: TokenId::new("yes-token"),
            token_no: TokenId::new("no-token"),
            question: "Will it happen?".to_owned(),
            slug: "slug".to_owned(),
            categories: CategorySet::default(),
            status: MarketStatus::Active,
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
            fee_schedule: None,
            end_date: None,
            resolved_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let identity = recommendation_identity_from_resolved(&selected, Some(&registry));
        assert_eq!(identity.question, "Will it happen?");
        assert_eq!(identity.outcome_name, "Yes");
        assert_eq!(identity.category, MarketCategory::Politics);
    }

    #[test]
    fn liquidity_score_clamps_to_unit_interval() {
        let book = sample_book();
        let score = liquidity_score_from_resolved(&book, Usd::new(dec!(10)));
        assert!(score.inner() <= dec!(1));
        assert!(score.inner() > dec!(0));
        let empty = liquidity_score_from_resolved(
            &empty_book(TokenId::new("t"), Utc::now()),
            Usd::new(dec!(100)),
        );
        assert_eq!(empty.inner(), dec!(0));
    }
}
