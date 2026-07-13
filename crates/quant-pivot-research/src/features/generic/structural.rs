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

use std::{str::FromStr, time::Duration};

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{TradeParticipantRole, TradeTapePrint},
    runtime_config::{DecimalString, FeatureFamily},
    types::{Price, Usd},
};
use rust_decimal::Decimal;

use crate::features::{
    builder::{FeatureComputeCtx, FeatureGroupBuilder, RawFeature, ResolvedLeg},
    generic::stats::{realized_volatility, simple_return},
    names::structural as names,
    resolved::{MicrostructureBucket, ResolvedBook},
    value::{EvidenceSourceKind, EvidenceSourceRef, FeatureName, FeatureValue, NullReason},
};
use crate::trade_tape::{
    ConcentrationMissing, ParticipantConcentrationGate, compute_concentration, compute_role_gini,
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

    fn compute(&self, ctx: &FeatureComputeCtx<'_>) -> QuantResult<Vec<RawFeature>> {
        let mut out = Vec::with_capacity(16);
        out.extend(shock_features(ctx));
        out.push(price_extremity_feature(ctx));
        out.push(book_churn_intensity_feature(ctx));
        out.extend(trade_tape_features(ctx));
        out.extend(negrisk_features(ctx));
        Ok(out)
    }
}

/// The shock window's signed return and its volatility-normalized magnitude.
fn shock_features(ctx: &FeatureComputeCtx<'_>) -> Vec<RawFeature> {
    let window = ctx.window;
    let evidence = EvidenceSourceRef {
        source_kind: EvidenceSourceKind::ClickHouseFact,
        reference: window.token_id.as_str().to_owned(),
        effective_at: window
            .freshest_bucket_time()
            .unwrap_or_else(|| window.cutoff()),
        available_at: window.latest_available_at(),
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
                    effective_at: window
                        .freshest_bucket_time()
                        .unwrap_or_else(|| window.cutoff()),
                    available_at: window.latest_available_at(),
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

/// Trade-tape participant concentration features over the configured PIT window.
fn trade_tape_features(ctx: &FeatureComputeCtx<'_>) -> Vec<RawFeature> {
    if !ctx.trade_tape.source_available {
        return trade_tape_feature_names()
            .into_iter()
            .map(|name| RawFeature::missing(name, NullReason::TradeTapeUnavailable))
            .collect();
    }

    let lookback = Duration::from_secs(ctx.config.structural.trade_tape_window_secs);
    let prints: Vec<TradeTapePrint> = ctx
        .trade_tape
        .prints_in(lookback)
        .into_iter()
        .cloned()
        .collect();
    if prints.is_empty() {
        return trade_tape_feature_names()
            .into_iter()
            .map(|name| RawFeature::missing(name, NullReason::InsufficientTradeTape))
            .collect();
    }

    let evidence = trade_tape_evidence(ctx);
    let gate = concentration_gate(ctx);
    let concentration = compute_concentration(&prints, true, &gate);
    let mut out = Vec::with_capacity(9);
    match concentration {
        Ok(snapshot) => {
            out.push(RawFeature::present(
                names::TRADE_TAPE_COUNT,
                FeatureValue::Count(snapshot.observed_print_count),
                evidence.clone(),
            ));
            out.push(RawFeature::present(
                names::PARTICIPANT_COUNT,
                FeatureValue::Count(snapshot.unique_participants),
                evidence.clone(),
            ));
            out.push(RawFeature::present(
                names::TRADE_TAPE_NOTIONAL_USD,
                FeatureValue::Usd(Usd::new(snapshot.total_notional_usd)),
                evidence.clone(),
            ));
            out.push(RawFeature::present(
                names::PARTICIPANT_COVERAGE_RATIO,
                FeatureValue::Decimal(snapshot.coverage_ratio),
                evidence.clone(),
            ));
            out.extend([
                RawFeature::present(
                    names::PARTICIPANT_GINI,
                    FeatureValue::Decimal(snapshot.gini),
                    evidence.clone(),
                ),
                RawFeature::present(
                    names::PARTICIPANT_HHI,
                    FeatureValue::Decimal(snapshot.hhi),
                    evidence.clone(),
                ),
                RawFeature::present(
                    names::PARTICIPANT_CR1_SHARE,
                    FeatureValue::Decimal(snapshot.cr1_share),
                    evidence.clone(),
                ),
            ]);
        }
        Err(ConcentrationMissing::InsufficientTradeTape) => {
            return trade_tape_feature_names()
                .into_iter()
                .map(|name| RawFeature::missing(name, NullReason::InsufficientTradeTape))
                .collect();
        }
        Err(ConcentrationMissing::TradeTapeUnavailable) => {
            return trade_tape_feature_names()
                .into_iter()
                .map(|name| RawFeature::missing(name, NullReason::TradeTapeUnavailable))
                .collect();
        }
        Err(ConcentrationMissing::InsufficientRoleCoverage) => {
            unreachable!("compute_concentration does not return role coverage missing");
        }
    }
    out.extend(role_metric_features(&prints, &gate, &evidence));
    out
}

const fn trade_tape_feature_names() -> [FeatureName; 9] {
    [
        names::TRADE_TAPE_COUNT,
        names::PARTICIPANT_COUNT,
        names::TRADE_TAPE_NOTIONAL_USD,
        names::PARTICIPANT_COVERAGE_RATIO,
        names::PARTICIPANT_GINI,
        names::PARTICIPANT_HHI,
        names::PARTICIPANT_CR1_SHARE,
        names::MAKER_GINI,
        names::TAKER_GINI,
    ]
}

fn trade_tape_evidence(ctx: &FeatureComputeCtx<'_>) -> EvidenceSourceRef {
    EvidenceSourceRef {
        source_kind: EvidenceSourceKind::TradeTape,
        reference: ctx.trade_tape.market_id.as_str().to_owned(),
        effective_at: ctx
            .trade_tape
            .freshest_trade_time()
            .unwrap_or_else(|| ctx.trade_tape.cutoff()),
        available_at: ctx.trade_tape.latest_available_at(),
    }
}

fn concentration_gate(ctx: &FeatureComputeCtx<'_>) -> ParticipantConcentrationGate {
    ParticipantConcentrationGate {
        min_unique_participants: ctx.config.structural.trade_tape_min_unique_participants,
        min_notional_usd: config_decimal(
            &ctx.config.structural.trade_tape_min_notional_usd,
            "features.structural.trade_tape_min_notional_usd",
        ),
        min_coverage_ratio: config_decimal(
            &ctx.config.structural.trade_tape_min_coverage_ratio,
            "features.structural.trade_tape_min_coverage_ratio",
        ),
    }
}

fn role_metric_features(
    prints: &[TradeTapePrint],
    gate: &ParticipantConcentrationGate,
    evidence: &EvidenceSourceRef,
) -> [RawFeature; 2] {
    [
        role_gini_feature(
            names::MAKER_GINI,
            prints,
            TradeParticipantRole::Maker,
            gate,
            evidence,
        ),
        role_gini_feature(
            names::TAKER_GINI,
            prints,
            TradeParticipantRole::Taker,
            gate,
            evidence,
        ),
    ]
}

fn config_decimal(raw: &DecimalString, field: &'static str) -> Decimal {
    Decimal::from_str(raw.value.trim()).unwrap_or_else(|error| {
        panic!(
            "{field} `{}` invalid despite config validation: {error}",
            raw.value
        )
    })
}

fn role_gini_feature(
    name: FeatureName,
    prints: &[TradeTapePrint],
    role: TradeParticipantRole,
    gate: &ParticipantConcentrationGate,
    evidence: &EvidenceSourceRef,
) -> RawFeature {
    match compute_role_gini(prints, role, gate) {
        Ok(metrics) => {
            RawFeature::present(name, FeatureValue::Decimal(metrics.gini), evidence.clone())
        }
        Err(ConcentrationMissing::InsufficientRoleCoverage) => {
            RawFeature::missing(name, NullReason::InsufficientRoleCoverage)
        }
        Err(_) => RawFeature::missing(name, NullReason::InsufficientTradeTape),
    }
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
    let Ok(leg_count) = u64::try_from(ctx.sibling_legs.len()) else {
        return negrisk_all_missing(NullReason::OutOfValidRange);
    };
    let Some(aggregate) = LegAggregate::from_legs(ctx.sibling_legs, leg_count) else {
        return negrisk_all_missing(NullReason::LegBookMissing);
    };
    let Some(evidence) = sibling_evidence(ctx.sibling_legs) else {
        return negrisk_all_missing(NullReason::LegBookMissing);
    };
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
    fn from_legs(legs: &[ResolvedLeg], leg_count: u64) -> Option<Self> {
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
            leg_count,
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
        effective_at: book.effective_at,
        available_at: Some(book.available_at),
    }
}

/// Sibling-leg evidence anchored on the stalest leg (worst-case freshness).
/// An empty leg set has no evidence and remains missing.
fn sibling_evidence(legs: &[ResolvedLeg]) -> Option<EvidenceSourceRef> {
    let effective_at = legs.iter().map(|leg| leg.book.effective_at).min()?;
    let available_at = legs.iter().map(|leg| leg.book.available_at).max();
    Some(EvidenceSourceRef {
        source_kind: EvidenceSourceKind::Book,
        reference: "negrisk_event_legs".to_owned(),
        effective_at,
        available_at,
    })
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

#[cfg(test)]
mod trade_tape_null_reason_tests {
    use chrono::Utc;
    use quant_pivot_models::{
        domain::{TradeParticipantRole, TradeTapePrint, TradeTapeSourceKind},
        enums::common::MarketCategory,
        runtime_config::{DataQualityConfig, FeaturesConfig},
        types::{MarketId, Price, Shares, TokenId, Usd},
    };
    use rust_decimal::Decimal;

    use super::*;
    use crate::features::{
        builder::{FeatureComputeCtx, FeatureGroupBuilder},
        names::structural as trade_tape_names,
        resolved::{MarketWindowSnapshot, TradeTapeWindowSnapshot},
    };

    fn trade_tape_snapshot(
        source_available: bool,
        prints: Vec<TradeTapePrint>,
    ) -> TradeTapeWindowSnapshot {
        let market_id = MarketId::new("m-null");
        let as_of = Utc::now();
        if source_available {
            TradeTapeWindowSnapshot::available(market_id, as_of, as_of, prints)
        } else {
            TradeTapeWindowSnapshot::empty(market_id, as_of, as_of)
        }
    }

    fn is_trade_tape_feature(name: &FeatureName) -> bool {
        name == &trade_tape_names::TRADE_TAPE_COUNT
            || name == &trade_tape_names::TRADE_TAPE_NOTIONAL_USD
            || name == &trade_tape_names::PARTICIPANT_GINI
            || name == &trade_tape_names::PARTICIPANT_HHI
            || name == &trade_tape_names::PARTICIPANT_CR1_SHARE
            || name == &trade_tape_names::PARTICIPANT_COVERAGE_RATIO
            || name == &trade_tape_names::MAKER_GINI
            || name == &trade_tape_names::TAKER_GINI
            || name == &trade_tape_names::PARTICIPANT_COUNT
    }

    fn trade_tape_only(
        trade_tape: &TradeTapeWindowSnapshot,
        config: &FeaturesConfig,
    ) -> Vec<RawFeature> {
        let as_of = trade_tape.decision_at;
        let window = MarketWindowSnapshot::empty(TokenId::new("tok-yes"), as_of, as_of);
        let ctx = FeatureComputeCtx {
            decision_at: as_of,
            category: MarketCategory::Sports,
            book: None,
            secondary_book: None,
            market: None,
            window: &window,
            trade_tape,
            sibling_legs: &[],
            sibling_leg_total: 0,
            config,
            data_quality: &DataQualityConfig::default(),
            book_snapshot_ref: None,
            secondary_book_snapshot_ref: None,
        };
        StructuralFeatureBuilder
            .compute(&ctx)
            .expect("valid structural fixture")
            .into_iter()
            .filter(|feature| is_trade_tape_feature(&feature.name))
            .collect()
    }

    fn fill_print(address: &str, notional: Decimal) -> TradeTapePrint {
        TradeTapePrint {
            market_id: MarketId::new("m-null"),
            token_id: TokenId::new("tok-yes"),
            event_time: Utc::now(),
            available_at: None,
            participant_address: address.to_owned(),
            participant_role: TradeParticipantRole::Maker,
            side: None,
            price: Price::new(Decimal::new(50, 2)),
            size_shares: Shares::new(notional * Decimal::from(2)),
            notional_usd: Usd::new(notional),
            tx_hash: None,
            trade_id: format!("trade-{address}"),
            source: TradeTapeSourceKind::OnChain,
            coverage_flags: 0,
            raw_payload_json: None,
        }
    }

    #[test]
    fn unavailable_source_marks_all_trade_tape_features_unavailable() {
        let config = FeaturesConfig::default();
        let trade_tape = trade_tape_snapshot(false, Vec::new());
        let features = trade_tape_only(&trade_tape, &config);
        assert_eq!(features.len(), 9);
        for feature in features {
            assert_eq!(feature.value, Err(NullReason::TradeTapeUnavailable));
        }
    }

    #[test]
    fn empty_available_window_marks_all_trade_tape_features_insufficient() {
        let config = FeaturesConfig::default();
        let trade_tape = trade_tape_snapshot(true, Vec::new());
        let features = trade_tape_only(&trade_tape, &config);
        assert_eq!(features.len(), 9);
        for feature in features {
            assert_eq!(feature.value, Err(NullReason::InsufficientTradeTape));
        }
    }

    #[test]
    fn below_min_unique_participants_is_insufficient() {
        let mut config = FeaturesConfig::default();
        config.structural.trade_tape_min_unique_participants = 3;
        let prints = vec![
            fill_print("0x1", Decimal::from(100)),
            fill_print("0x2", Decimal::from(100)),
        ];
        let trade_tape = trade_tape_snapshot(true, prints);
        let features = trade_tape_only(&trade_tape, &config);
        let gini = features
            .iter()
            .find(|feature| feature.name == trade_tape_names::PARTICIPANT_GINI)
            .expect("gini");
        assert_eq!(gini.value, Err(NullReason::InsufficientTradeTape));
    }
}
