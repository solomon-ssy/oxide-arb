//! Structural (prediction-market-aware) feature builder (Phase 11.2.1).
//!
//! Platform-computable from existing facts — no external data source. Produces:
//!
//! - `struct.short_return` / `struct.shock_ratio` — the shock-window return and
//!   its volatility-normalized magnitude (gate the shock-reversal factor).
//! - `struct.price_extremity` — signed `mid − 0.5` (interacts with
//!   time-to-resolution in the resolution-proximity factor).
//! - `struct.book_churn_intensity` — a book-churn (delta/update) proxy over the
//!   maker window. NOT true maker concentration (needs trade-tape; see 11.2.1.1).
//! - `struct.negrisk_leg_ask_sum` / `struct.negrisk_leg_bid_sum` /
//!   `struct.negrisk_leg_count` / `struct.negrisk_convert_edge` — same-`as_of`
//!   full-leg aggregates over a neg-risk event's YES legs.
//!
//! Neg-risk aggregates on a **binary** market are `NullReason::NotApplicable`
//! (structurally absent — not a data gap); a neg-risk market missing any leg
//! book is `NullReason::LegBookMissing`. Neither is ever a fabricated zero.

use std::time::Duration;

use quant_pivot_models::{runtime_config::FeatureFamily, types::Price};
use rust_decimal::Decimal;

use crate::features::{
    builder::{FeatureComputeCtx, FeatureGroupBuilder, RawFeature, ResolvedLeg},
    names::structural as names,
    resolved::{MicrostructureBucket, ResolvedBook},
    stats::{realized_volatility, simple_return},
    value::{EvidenceSourceKind, EvidenceSourceRef, FeatureName, FeatureValue, NullReason},
};

/// One half of `[0, 1]` — the neutral prediction-market price.
fn half() -> Decimal {
    Decimal::new(5, 1)
}

/// Builds the [`FeatureFamily::Structural`] features.
pub struct StructuralFeatureBuilder;

impl FeatureGroupBuilder for StructuralFeatureBuilder {
    fn family(&self) -> FeatureFamily {
        FeatureFamily::Structural
    }

    fn compute(&self, ctx: &FeatureComputeCtx<'_>) -> Vec<RawFeature> {
        let mut out = Vec::with_capacity(8);
        out.extend(shock_features(ctx));
        out.push(price_extremity_feature(ctx));
        out.push(book_churn_intensity_feature(ctx));
        out.extend(negrisk_features(ctx));
        out
    }
}

/// The shock window's signed return and its volatility-normalized magnitude.
fn shock_features(ctx: &FeatureComputeCtx<'_>) -> Vec<RawFeature> {
    let window = ctx.window;
    let evidence = EvidenceSourceRef {
        source_kind: EvidenceSourceKind::ClickHouseFact,
        reference: window.token_id.as_str().to_owned(),
        observed_at: window
            .freshest_bucket_time()
            .unwrap_or_else(|| window.cutoff()),
    };
    let lookback = Duration::from_secs(ctx.config.structural.shock_window_secs);
    let mids: Vec<_> = window
        .mids_in(lookback)
        .into_iter()
        .map(Price::inner)
        .collect();

    let short_return = simple_return(&mids);
    let realized_vol = realized_volatility(&mids);
    // Window-length-invariant shock: the total window return's standard deviation
    // is the per-step realized vol scaled by √(steps), so `|ret| / (vol·√steps)`
    // is a proper z-score of the move that does not implicitly re-scale with the
    // `shock_window_secs` / bucket density (the `shock_k` gate stays comparable
    // across window configs).
    let steps = mids.len().saturating_sub(1);
    let shock_ratio = match (short_return, realized_vol) {
        (Some(ret), Some(vol)) if vol > Decimal::ZERO && steps > 0 => {
            sqrt_decimal(steps).map(|scale| (ret.abs() / (vol * scale)).round_dp(12))
        }
        _ => None,
    };

    vec![
        decimal_window(names::SHORT_RETURN, short_return, &evidence),
        decimal_window(names::SHOCK_RATIO, shock_ratio, &evidence),
    ]
}

/// `√n` as a `Decimal` via an `f64` intermediate (no `maths` feature needed);
/// `None` on a non-finite result.
fn sqrt_decimal(n: usize) -> Option<Decimal> {
    let n = u32::try_from(n).ok()?;
    let root = f64::from(n).sqrt();
    root.is_finite()
        .then(|| Decimal::from_f64_retain(root))
        .flatten()
}

/// Signed price extremity `mid − 0.5` from the primary-token book.
///
/// Signed so the resolution-proximity factor's interaction preserves side: a
/// favorite (mid > 0.5) reads positive and a longshot (mid < 0.5) negative.
fn price_extremity_feature(ctx: &FeatureComputeCtx<'_>) -> RawFeature {
    let Some(book) = ctx.book else {
        return RawFeature::missing(names::PRICE_EXTREMITY, NullReason::SourceUnavailable);
    };
    let Some(mid) = book.mid() else {
        return RawFeature::missing(names::PRICE_EXTREMITY, NullReason::SourceUnavailable);
    };
    let extremity = mid.inner() - half();
    RawFeature::present(
        names::PRICE_EXTREMITY,
        FeatureValue::Decimal(extremity),
        book_evidence(book),
    )
}

/// Book-churn intensity: delta-to-update ratio over the maker window — a
/// book-derived liquidity-turnover proxy. This is NOT true maker participant
/// concentration (Gini / top-1% share), which requires trade-tape the platform
/// does not yet ingest; the honest concentration signal is designed in 11.2.1.1.
fn book_churn_intensity_feature(ctx: &FeatureComputeCtx<'_>) -> RawFeature {
    let window = ctx.window;
    let lookback = Duration::from_secs(ctx.config.structural.book_churn_window_secs);
    let buckets = window.buckets_in(lookback);
    book_churn(&buckets).map_or_else(
        || RawFeature::missing(names::BOOK_CHURN_INTENSITY, NullReason::InsufficientHistory),
        |churn| {
            RawFeature::present(
                names::BOOK_CHURN_INTENSITY,
                FeatureValue::Decimal(churn),
                EvidenceSourceRef {
                    source_kind: EvidenceSourceKind::ClickHouseFact,
                    reference: window.token_id.as_str().to_owned(),
                    observed_at: window
                        .freshest_bucket_time()
                        .unwrap_or_else(|| window.cutoff()),
                },
            )
        },
    )
}

/// Delta-to-update ratio across the window buckets (`None` for no flow).
fn book_churn(buckets: &[&MicrostructureBucket]) -> Option<Decimal> {
    let deltas: u64 = buckets.iter().map(|bucket| bucket.delta_count).sum();
    let updates: u64 = buckets.iter().map(|bucket| bucket.update_count).sum();
    if updates == 0 {
        return None;
    }
    Some((Decimal::from(deltas) / Decimal::from(updates)).round_dp(12))
}

/// Neg-risk full-leg aggregates. Binary market ⇒ `NotApplicable`; a neg-risk
/// market missing any leg book ⇒ `LegBookMissing`; never a fabricated zero.
fn negrisk_features(ctx: &FeatureComputeCtx<'_>) -> Vec<RawFeature> {
    let neg_risk = ctx.market.is_some_and(|market| market.neg_risk);
    if !neg_risk {
        return negrisk_all_missing(NullReason::NotApplicable);
    }
    // Every event YES leg must be resolved (expected == resolved) and quote both
    // sides; otherwise the full-leg sum is incomplete and fails closed.
    if ctx.sibling_leg_total == 0 || ctx.sibling_legs.len() != ctx.sibling_leg_total {
        return negrisk_all_missing(NullReason::LegBookMissing);
    }
    let Some(aggregate) = LegAggregate::from_legs(ctx.sibling_legs) else {
        return negrisk_all_missing(NullReason::LegBookMissing);
    };
    let evidence = sibling_evidence(ctx.sibling_legs, ctx.as_of);
    vec![
        RawFeature::present(
            names::NEGRISK_LEG_ASK_SUM,
            FeatureValue::Decimal(aggregate.ask_sum.round_dp(12)),
            evidence.clone(),
        ),
        RawFeature::present(
            names::NEGRISK_LEG_BID_SUM,
            FeatureValue::Decimal(aggregate.bid_sum.round_dp(12)),
            evidence.clone(),
        ),
        RawFeature::present(
            names::NEGRISK_LEG_COUNT,
            FeatureValue::Count(aggregate.leg_count),
            evidence.clone(),
        ),
        RawFeature::present(
            names::NEGRISK_CONVERT_EDGE,
            FeatureValue::Decimal(aggregate.convert_edge.round_dp(12)),
            evidence,
        ),
    ]
}

/// Full-leg aggregate over a neg-risk event's YES legs.
struct LegAggregate {
    ask_sum: Decimal,
    bid_sum: Decimal,
    leg_count: u64,
    convert_edge: Decimal,
}

impl LegAggregate {
    /// Compute the aggregate, or `None` when any leg does not quote both sides.
    fn from_legs(legs: &[ResolvedLeg]) -> Option<Self> {
        let mut ask_sum = Decimal::ZERO;
        let mut bid_sum = Decimal::ZERO;
        let mut favorite_ask = Decimal::MIN;
        let mut favorite_bid = Decimal::ZERO;
        for leg in legs {
            let ask = leg.book.best_ask()?.inner();
            let bid = leg.book.best_bid()?.inner();
            ask_sum += ask;
            bid_sum += bid;
            if ask > favorite_ask {
                favorite_ask = ask;
                favorite_bid = bid;
            }
        }
        // Buy-YES basket of all-but-favorite vs. buy-NO of the favorite (≈ 1 −
        // favorite YES bid). A positive edge favors the basket route.
        let basket_cost = ask_sum - favorite_ask;
        let no_favorite_cost = Decimal::ONE - favorite_bid;
        Some(Self {
            ask_sum,
            bid_sum,
            leg_count: u64::try_from(legs.len()).unwrap_or(u64::MAX),
            convert_edge: basket_cost - no_favorite_cost,
        })
    }
}

/// The neg-risk features, all missing with one reason.
fn negrisk_all_missing(reason: NullReason) -> Vec<RawFeature> {
    [
        names::NEGRISK_LEG_ASK_SUM,
        names::NEGRISK_LEG_BID_SUM,
        names::NEGRISK_LEG_COUNT,
        names::NEGRISK_CONVERT_EDGE,
    ]
    .into_iter()
    .map(|name| RawFeature::missing(name, reason))
    .collect()
}

/// Book-derived evidence anchored on the book's observation time.
fn book_evidence(book: &ResolvedBook) -> EvidenceSourceRef {
    EvidenceSourceRef {
        source_kind: EvidenceSourceKind::Book,
        reference: book.token_id.as_str().to_owned(),
        observed_at: book.observed_at,
    }
}

/// Sibling-leg evidence anchored on the STALEST leg (worst-case freshness),
/// falling back to `as_of` for an empty set (kept total and deterministic).
fn sibling_evidence(
    legs: &[ResolvedLeg],
    as_of: chrono::DateTime<chrono::Utc>,
) -> EvidenceSourceRef {
    let observed_at = legs
        .iter()
        .map(|leg| leg.book.observed_at)
        .min()
        .unwrap_or(as_of);
    EvidenceSourceRef {
        source_kind: EvidenceSourceKind::Book,
        reference: "negrisk_event_legs".to_owned(),
        observed_at,
    }
}

/// Wrap a windowed decimal, or mark it missing for insufficient history.
fn decimal_window(
    name: FeatureName,
    value: Option<Decimal>,
    evidence: &EvidenceSourceRef,
) -> RawFeature {
    match value {
        Some(decimal) => {
            RawFeature::present(name, FeatureValue::Decimal(decimal), evidence.clone())
        }
        None => RawFeature::missing(name, NullReason::InsufficientHistory),
    }
}
