//! Price & book feature builder: top-of-book prices, spread, depth structure,
//! imbalance, slope, and freshness flags from a resolved order book.

use crate::features::{
    builder::{FeatureComputeCtx, FeatureGroupBuilder, RawFeature},
    decision_capture::book_evidence_ref,
    names::book,
    resolved::ResolvedBook,
    value::{EvidenceSourceKind, EvidenceSourceRef, FeatureName, FeatureValue, NullReason},
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    runtime_config::FeatureFamily,
    types::{Bps, Price, Probability},
};
use rust_decimal::Decimal;

/// Builds the [`FeatureFamily::PriceBook`] features.
pub struct PriceBookFeatureBuilder;

impl FeatureGroupBuilder for PriceBookFeatureBuilder {
    fn family(&self) -> FeatureFamily {
        FeatureFamily::PriceBook
    }

    fn compute(&self, ctx: &FeatureComputeCtx<'_>) -> QuantResult<Vec<RawFeature>> {
        // No book ⇒ produce nothing; model-required book inputs then reject the market.
        let Some(book) = ctx.book else {
            return Ok(Vec::new());
        };
        let evidence = ctx.book_snapshot_ref.map_or_else(
            || EvidenceSourceRef {
                source_kind: EvidenceSourceKind::Book,
                reference: book.token_id.as_str().to_owned(),
                effective_at: book.effective_at,
                available_at: Some(book.available_at),
            },
            |book_ref| book_evidence_ref(book_ref, book.effective_at, book.available_at),
        );

        let mut out = vec![
            price_feature(book::BEST_BID, book.best_bid().map(Price::inner), &evidence),
            price_feature(book::BEST_ASK, book.best_ask().map(Price::inner), &evidence),
            secondary_best_ask(ctx),
            price_feature(book::MID, book.mid().map(Price::inner), &evidence),
            spread_bps(book, &evidence),
            decimal_feature(
                book::DEPTH_IMBALANCE,
                book.depth_imbalance(),
                &evidence,
                NullReason::InsufficientHistory,
            ),
            decimal_feature(
                book::SLOPE,
                book.slope(),
                &evidence,
                NullReason::InsufficientHistory,
            ),
            RawFeature::present(
                book::VISIBLE_LIQUIDITY_USD,
                FeatureValue::Usd(book.visible_liquidity_usd()),
                evidence.clone(),
            ),
            book_age(ctx, book, &evidence),
            RawFeature::present(
                book::CROSSED,
                FeatureValue::Bool(book.is_crossed()),
                evidence.clone(),
            ),
            RawFeature::present(
                book::EMPTY,
                FeatureValue::Bool(book.is_empty()),
                evidence.clone(),
            ),
        ];

        for level in &ctx.config.depth_levels {
            out.push(RawFeature::present(
                FeatureName::book_depth_top(*level),
                FeatureValue::Usd(book.top_n_depth_usd(*level)),
                evidence.clone(),
            ));
        }
        Ok(out)
    }
}

fn secondary_best_ask(ctx: &FeatureComputeCtx<'_>) -> RawFeature {
    let Some(secondary) = ctx.secondary_book else {
        return RawFeature::missing(book::SECONDARY_BEST_ASK, NullReason::SourceUnavailable);
    };
    let evidence = ctx.secondary_book_snapshot_ref.map_or_else(
        || EvidenceSourceRef {
            source_kind: EvidenceSourceKind::Book,
            reference: secondary.token_id.as_str().to_owned(),
            effective_at: secondary.effective_at,
            available_at: Some(secondary.available_at),
        },
        |book_ref| book_evidence_ref(book_ref, secondary.effective_at, secondary.available_at),
    );
    price_feature(
        book::SECONDARY_BEST_ASK,
        secondary.best_ask().map(Price::inner),
        &evidence,
    )
}

/// A price feature carried as a `[0, 1]` probability, or missing when unquoted.
fn price_feature(
    name: FeatureName,
    value: Option<Decimal>,
    evidence: &EvidenceSourceRef,
) -> RawFeature {
    match value {
        Some(decimal) => RawFeature::present(
            name,
            FeatureValue::Probability(Probability::new(decimal)),
            evidence.clone(),
        ),
        None => {
            RawFeature::missing_with_evidence(name, NullReason::SourceUnavailable, evidence.clone())
        }
    }
}

/// A dimensionless decimal feature, or missing with `reason` when undefined.
fn decimal_feature(
    name: FeatureName,
    value: Option<Decimal>,
    evidence: &EvidenceSourceRef,
    reason: NullReason,
) -> RawFeature {
    match value {
        Some(decimal) => {
            RawFeature::present(name, FeatureValue::Decimal(decimal), evidence.clone())
        }
        None => RawFeature::missing(name, reason),
    }
}

/// Top-of-book spread in basis points, or missing when the book is one-sided.
fn spread_bps(book: &ResolvedBook, evidence: &EvidenceSourceRef) -> RawFeature {
    match (book.best_bid(), book.best_ask(), book.mid()) {
        (Some(bid), Some(ask), Some(mid)) if mid.inner() > Decimal::ZERO => {
            Bps::relative(ask.inner() - bid.inner(), mid.inner()).map_or_else(
                || RawFeature::missing(book::SPREAD_BPS, NullReason::OutOfValidRange),
                |bps| {
                    RawFeature::present(
                        book::SPREAD_BPS,
                        FeatureValue::Bps(bps.inner()),
                        evidence.clone(),
                    )
                },
            )
        }
        _ => RawFeature::missing(book::SPREAD_BPS, NullReason::SourceUnavailable),
    }
}

/// Book age in milliseconds at the decision time.
fn book_age(
    ctx: &FeatureComputeCtx<'_>,
    book: &ResolvedBook,
    evidence: &EvidenceSourceRef,
) -> RawFeature {
    let age = (ctx.decision_at - book.effective_at).num_milliseconds();
    u64::try_from(age).map_or_else(
        |_| RawFeature::missing(book::AGE_MS, NullReason::OutOfValidRange),
        |age| RawFeature::present(book::AGE_MS, FeatureValue::Count(age), evidence.clone()),
    )
}
