//! Price & book feature builder: top-of-book prices, spread, depth structure,
//! imbalance, slope, and freshness flags from a resolved order book.

use crate::features::{
    builder::{FeatureComputeCtx, FeatureGroupBuilder, RawFeature},
    resolved::ResolvedBook,
    value::{EvidenceSourceKind, EvidenceSourceRef, FeatureName, FeatureValue, NullReason},
};
use quant_pivot_models::{
    runtime_config::FeatureFamily,
    types::{Price, Probability},
};
use rust_decimal::Decimal;

/// Builds the [`FeatureFamily::PriceBook`] features.
pub struct PriceBookFeatureBuilder;

impl FeatureGroupBuilder for PriceBookFeatureBuilder {
    fn family(&self) -> FeatureFamily {
        FeatureFamily::PriceBook
    }

    fn compute(&self, ctx: &FeatureComputeCtx<'_>) -> Vec<RawFeature> {
        // No book ⇒ produce nothing; critical book specs then reject the market.
        let Some(book) = ctx.book else {
            return Vec::new();
        };
        let evidence = EvidenceSourceRef {
            source_kind: EvidenceSourceKind::Book,
            reference: book.token_id.as_str().to_owned(),
            observed_at: book.observed_at,
        };

        let mut out = vec![
            price_feature(
                "book.best_bid",
                book.best_bid().map(Price::inner),
                &evidence,
            ),
            price_feature(
                "book.best_ask",
                book.best_ask().map(Price::inner),
                &evidence,
            ),
            price_feature("book.mid", book.mid().map(Price::inner), &evidence),
            spread_bps(book, &evidence),
            decimal_feature(
                "book.depth_imbalance",
                book.depth_imbalance(),
                &evidence,
                NullReason::InsufficientHistory,
            ),
            decimal_feature(
                "book.slope",
                book.slope(),
                &evidence,
                NullReason::InsufficientHistory,
            ),
            RawFeature::present(
                FeatureName::from_static("book.visible_liquidity_usd"),
                FeatureValue::Usd(book.visible_liquidity_usd()),
                evidence.clone(),
            ),
            RawFeature::present(
                FeatureName::from_static("book.age_ms"),
                FeatureValue::Count(book_age_ms(ctx, book)),
                evidence.clone(),
            ),
            RawFeature::present(
                FeatureName::from_static("book.crossed"),
                FeatureValue::Bool(book.is_crossed()),
                evidence.clone(),
            ),
            RawFeature::present(
                FeatureName::from_static("book.empty"),
                FeatureValue::Bool(book.is_empty()),
                evidence.clone(),
            ),
        ];

        for level in &ctx.config.depth_levels {
            out.push(RawFeature::present(
                FeatureName::new(format!("book.depth_top{level}_usd")),
                FeatureValue::Usd(book.top_n_depth_usd(*level)),
                evidence.clone(),
            ));
        }
        out
    }
}

/// A price feature carried as a `[0, 1]` probability, or missing when unquoted.
fn price_feature(
    name: &'static str,
    value: Option<Decimal>,
    evidence: &EvidenceSourceRef,
) -> RawFeature {
    let name = FeatureName::from_static(name);
    match value {
        Some(decimal) => RawFeature::present(
            name,
            FeatureValue::Probability(Probability::new(decimal)),
            evidence.clone(),
        ),
        None => RawFeature::missing(name, NullReason::SourceUnavailable),
    }
}

/// A dimensionless decimal feature, or missing with `reason` when undefined.
fn decimal_feature(
    name: &'static str,
    value: Option<Decimal>,
    evidence: &EvidenceSourceRef,
    reason: NullReason,
) -> RawFeature {
    let name = FeatureName::from_static(name);
    match value {
        Some(decimal) => {
            RawFeature::present(name, FeatureValue::Decimal(decimal), evidence.clone())
        }
        None => RawFeature::missing(name, reason),
    }
}

/// Top-of-book spread in basis points, or missing when the book is one-sided.
fn spread_bps(book: &ResolvedBook, evidence: &EvidenceSourceRef) -> RawFeature {
    let name = FeatureName::from_static("book.spread_bps");
    match (book.best_bid(), book.best_ask(), book.mid()) {
        (Some(bid), Some(ask), Some(mid)) if mid.inner() > Decimal::ZERO => {
            let bps = (ask.inner() - bid.inner()) / mid.inner() * Decimal::from(10_000);
            RawFeature::present(name, FeatureValue::Bps(bps), evidence.clone())
        }
        _ => RawFeature::missing(name, NullReason::SourceUnavailable),
    }
}

/// Book age in milliseconds at the decision time (clamped at zero).
fn book_age_ms(ctx: &FeatureComputeCtx<'_>, book: &ResolvedBook) -> u64 {
    let age = (ctx.as_of - book.observed_at).num_milliseconds();
    u64::try_from(age).unwrap_or(0)
}
