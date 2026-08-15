//! Structural (prediction-market-aware) feature builder.
//!
//! Platform-computable from existing facts — no external data source. Produces:
//!
//! - `struct.short_return` / `struct.shock_ratio` — the shock-window return and
//!   its volatility-normalized magnitude (gate the shock-reversal factor).
//! - `struct.price_extremity` — signed `mid − 0.5` (interacts with
//!   time-to-resolution in the resolution-proximity factor).
//! - `struct.book_churn_intensity` — a book-churn (delta/update) proxy over the
//!   maker window. It is distinct from finalized execution-participant concentration.
//! - `struct.negrisk_leg_ask_sum` / `struct.negrisk_leg_bid_sum` /
//!   `struct.negrisk_leg_count` / `struct.negrisk_convert_edge` — same-`as_of`
//!   full-leg aggregates over a neg-risk event's YES legs.
//!
//! Neg-risk aggregates on a **binary** market are `NullReason::NotApplicable`
//! (structurally absent — not a data gap); a neg-risk market missing any leg
//! book is `NullReason::LegBookMissing`. Neither is ever a fabricated zero.

use std::time::Duration;

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::data_plane::{ExecutionParticipantPrint, ExecutionParticipantRole},
    runtime_config::{DecimalValue, FeatureFamily},
    types::{Price, Usd},
};
use rust_decimal::Decimal;

use crate::{
    execution_history::{
        ConcentrationMissing, ParticipantConcentrationGate, compute_concentration,
        compute_role_gini,
    },
    features::{
        builder::{FeatureComputeCtx, FeatureGroupBuilder, RawFeature, ResolvedLeg},
        generic::stats::{realized_volatility, simple_return},
        names::{
            structural as names,
            structural::{
                BOOK_CHURN_INTENSITY, EXECUTION_HISTORY_COUNT, EXECUTION_HISTORY_NOTIONAL_USD,
                MAKER_GINI, NEGRISK_CONVERT_EDGE, NEGRISK_LEG_ASK_SUM, NEGRISK_LEG_BID_SUM,
                NEGRISK_LEG_COUNT, PARTICIPANT_COUNT, PARTICIPANT_COVERAGE_RATIO,
                PARTICIPANT_CR1_SHARE, PARTICIPANT_GINI, PARTICIPANT_HHI, PRICE_EXTREMITY,
                TAKER_GINI,
            },
        },
        resolved::{MicrostructureBucket, ResolvedBook},
        value::{EvidenceSourceKind, EvidenceSourceRef, FeatureName, FeatureValue, NullReason},
    },
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
        out.extend(execution_history_features(ctx));
        out.extend(negrisk_features(ctx));
        Ok(out)
    }
}

/// The shock window's signed return and its volatility-normalized magnitude.
fn shock_features(ctx: &FeatureComputeCtx<'_>) -> Vec<RawFeature> {
    let window = ctx.window;
    let evidence = EvidenceSourceRef {
        source_kind: EvidenceSourceKind::ClickHouseFact,
        reference: window.token_id.to_string(),
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
        return RawFeature::missing(PRICE_EXTREMITY, NullReason::SourceUnavailable);
    };
    let Some(mid) = book.mid() else {
        return RawFeature::missing(PRICE_EXTREMITY, NullReason::SourceUnavailable);
    };
    let extremity = mid.inner() - half();
    RawFeature::present(
        PRICE_EXTREMITY,
        FeatureValue::Decimal(extremity),
        (book).book_evidence(),
    )
}

/// Book-churn intensity: delta-to-update ratio over the maker window — a
/// book-derived liquidity-turnover proxy. This is not maker participant
/// concentration (Gini / CR1 / HHI), computed from finalized executions.
fn book_churn_intensity_feature(ctx: &FeatureComputeCtx<'_>) -> RawFeature {
    let window = ctx.window;
    let lookback = Duration::from_secs(ctx.config.structural.book_churn_window_secs);
    let buckets = window.buckets_in(lookback);
    book_churn(&buckets).map_or_else(
        || RawFeature::missing(BOOK_CHURN_INTENSITY, NullReason::InsufficientHistory),
        |churn| {
            RawFeature::present(
                BOOK_CHURN_INTENSITY,
                FeatureValue::Decimal(churn),
                EvidenceSourceRef {
                    source_kind: EvidenceSourceKind::ClickHouseFact,
                    reference: window.token_id.to_string(),
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
fn execution_history_features(ctx: &FeatureComputeCtx<'_>) -> Vec<RawFeature> {
    if !ctx.execution_history.source_available {
        return execution_history_feature_names()
            .into_iter()
            .map(|name| RawFeature::missing(name, NullReason::FinalizedExecutionUnavailable))
            .collect();
    }

    let lookback = Duration::from_secs(ctx.config.structural.execution_window_secs);
    let prints: Vec<ExecutionParticipantPrint> = ctx
        .execution_history
        .prints_in(lookback)
        .into_iter()
        .cloned()
        .collect();
    if prints.is_empty() {
        return execution_history_feature_names()
            .into_iter()
            .map(|name| RawFeature::missing(name, NullReason::InsufficientExecutionHistory))
            .collect();
    }

    let evidence = execution_history_evidence(ctx);
    let gate = concentration_gate(ctx);
    let concentration = compute_concentration(&prints, true, &gate);
    let mut out = Vec::with_capacity(9);
    match concentration {
        Ok(snapshot) => {
            out.push(RawFeature::present(
                EXECUTION_HISTORY_COUNT,
                FeatureValue::Count(snapshot.observed_print_count),
                evidence.clone(),
            ));
            out.push(RawFeature::present(
                PARTICIPANT_COUNT,
                FeatureValue::Count(snapshot.unique_participants),
                evidence.clone(),
            ));
            out.push(RawFeature::present(
                EXECUTION_HISTORY_NOTIONAL_USD,
                FeatureValue::Usd(Usd::new(snapshot.total_notional_usd)),
                evidence.clone(),
            ));
            out.push(RawFeature::present(
                PARTICIPANT_COVERAGE_RATIO,
                FeatureValue::Decimal(snapshot.coverage_ratio),
                evidence.clone(),
            ));
            out.extend([
                RawFeature::present(
                    PARTICIPANT_GINI,
                    FeatureValue::Decimal(snapshot.gini),
                    evidence.clone(),
                ),
                RawFeature::present(
                    PARTICIPANT_HHI,
                    FeatureValue::Decimal(snapshot.hhi),
                    evidence.clone(),
                ),
                RawFeature::present(
                    PARTICIPANT_CR1_SHARE,
                    FeatureValue::Decimal(snapshot.cr1_share),
                    evidence.clone(),
                ),
            ]);
        }
        Err(ConcentrationMissing::InsufficientExecutionHistory) => {
            return execution_history_feature_names()
                .into_iter()
                .map(|name| RawFeature::missing(name, NullReason::InsufficientExecutionHistory))
                .collect();
        }
        Err(ConcentrationMissing::FinalizedExecutionUnavailable) => {
            return execution_history_feature_names()
                .into_iter()
                .map(|name| RawFeature::missing(name, NullReason::FinalizedExecutionUnavailable))
                .collect();
        }
        Err(ConcentrationMissing::InsufficientRoleCoverage) => {
            unreachable!("compute_concentration does not return role coverage missing");
        }
    }
    out.extend(role_metric_features(&prints, &gate, &evidence));
    out
}

const fn execution_history_feature_names() -> [FeatureName; 9] {
    [
        EXECUTION_HISTORY_COUNT,
        PARTICIPANT_COUNT,
        EXECUTION_HISTORY_NOTIONAL_USD,
        PARTICIPANT_COVERAGE_RATIO,
        PARTICIPANT_GINI,
        PARTICIPANT_HHI,
        PARTICIPANT_CR1_SHARE,
        MAKER_GINI,
        TAKER_GINI,
    ]
}

fn execution_history_evidence(ctx: &FeatureComputeCtx<'_>) -> EvidenceSourceRef {
    EvidenceSourceRef {
        source_kind: EvidenceSourceKind::FinalizedExecution,
        reference: ctx.execution_history.market_id.to_string(),
        effective_at: ctx
            .execution_history
            .freshest_execution_time()
            .unwrap_or_else(|| ctx.execution_history.cutoff()),
        available_at: ctx.execution_history.latest_available_at(),
    }
}

const fn concentration_gate(ctx: &FeatureComputeCtx<'_>) -> ParticipantConcentrationGate {
    ParticipantConcentrationGate {
        min_unique_participants: ctx.config.structural.execution_min_unique_participants,
        min_notional_usd: config_decimal(
            &ctx.config.structural.execution_min_notional_usd,
            "features.structural.execution_min_notional_usd",
        ),
        min_coverage_ratio: config_decimal(
            &ctx.config.structural.execution_min_coverage_ratio,
            "features.structural.execution_min_coverage_ratio",
        ),
    }
}

fn role_metric_features(
    prints: &[ExecutionParticipantPrint],
    gate: &ParticipantConcentrationGate,
    evidence: &EvidenceSourceRef,
) -> [RawFeature; 2] {
    [
        role_gini_feature(
            MAKER_GINI,
            prints,
            ExecutionParticipantRole::Maker,
            gate,
            evidence,
        ),
        role_gini_feature(
            TAKER_GINI,
            prints,
            ExecutionParticipantRole::Taker,
            gate,
            evidence,
        ),
    ]
}

const fn config_decimal(raw: &DecimalValue, _field: &'static str) -> Decimal {
    raw.value
}

fn role_gini_feature(
    name: FeatureName,
    prints: &[ExecutionParticipantPrint],
    role: ExecutionParticipantRole,
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
        Err(_) => RawFeature::missing(name, NullReason::InsufficientExecutionHistory),
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
        NEGRISK_LEG_ASK_SUM,
        NEGRISK_LEG_BID_SUM,
        NEGRISK_LEG_COUNT,
        NEGRISK_CONVERT_EDGE,
    ]
    .into_iter()
    .map(|name| RawFeature::missing(name, reason))
    .collect()
}

impl ResolvedBook {
    /// Book-derived evidence anchored on the book's observation time.
    fn book_evidence(&self) -> EvidenceSourceRef {
        EvidenceSourceRef {
            source_kind: EvidenceSourceKind::Book,
            reference: self.token_id.to_string(),
            effective_at: self.effective_at,
            available_at: Some(self.available_at),
        }
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
mod execution_history_null_reason_tests {
    use chrono::Utc;
    use quant_pivot_models::{
        domain::data_plane::{ExecutionParticipantPrint, ExecutionParticipantRole},
        enums::common::{MarketCategory, Side},
        runtime_config::{DataQualityConfig, FeaturesConfig},
        types::{ContentHash, FinalizedExecutionEvidence, MarketId, Price, Shares, TokenId, Usd},
    };
    use rust_decimal::Decimal;

    use super::*;
    use crate::features::{
        builder::{FeatureComputeCtx, FeatureGroupBuilder},
        names::structural::{
            EXECUTION_HISTORY_COUNT, EXECUTION_HISTORY_NOTIONAL_USD, MAKER_GINI, PARTICIPANT_COUNT,
            PARTICIPANT_COVERAGE_RATIO, PARTICIPANT_CR1_SHARE, PARTICIPANT_GINI, PARTICIPANT_HHI,
            TAKER_GINI,
        },
        resolved::{FinalizedExecutionWindowSnapshot, MarketWindowSnapshot},
    };

    fn execution_history_snapshot(
        source_available: bool,
        prints: Vec<ExecutionParticipantPrint>,
    ) -> FinalizedExecutionWindowSnapshot {
        let market_id = MarketId::new("m-null");
        let as_of = Utc::now();
        if source_available {
            FinalizedExecutionWindowSnapshot::available(market_id, as_of, as_of, prints)
        } else {
            FinalizedExecutionWindowSnapshot::available(market_id, as_of, as_of, prints)
                .with_source_evidence(
                    FinalizedExecutionEvidence::runtime(false, None, None),
                    false,
                )
        }
    }

    fn is_execution_history_feature(name: &FeatureName) -> bool {
        name == &EXECUTION_HISTORY_COUNT
            || name == &EXECUTION_HISTORY_NOTIONAL_USD
            || name == &PARTICIPANT_GINI
            || name == &PARTICIPANT_HHI
            || name == &PARTICIPANT_CR1_SHARE
            || name == &PARTICIPANT_COVERAGE_RATIO
            || name == &MAKER_GINI
            || name == &TAKER_GINI
            || name == &PARTICIPANT_COUNT
    }

    fn execution_history_only(
        execution_history: &FinalizedExecutionWindowSnapshot,
        config: &FeaturesConfig,
    ) -> Vec<RawFeature> {
        let as_of = execution_history.decision_at;
        let window = MarketWindowSnapshot::empty(TokenId::new("tok-yes"), as_of, as_of);
        let ctx = FeatureComputeCtx {
            decision_at: as_of,
            category: MarketCategory::Sports,
            book: None,
            secondary_book: None,
            market: None,
            window: &window,
            execution_history,
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
            .filter(|feature| is_execution_history_feature(&feature.name))
            .collect()
    }

    fn fill_print(address: &str, notional: Decimal) -> ExecutionParticipantPrint {
        let now = Utc::now();
        let hash = ContentHash::parse(&format!("blake3:{}", "a".repeat(64)))
            .expect("valid test content hash");
        ExecutionParticipantPrint {
            execution_id: hash,
            market_id: MarketId::new("m-null"),
            token_id: TokenId::new("tok-yes"),
            effective_at: now,
            observed_at: now,
            model_available_at: now,
            participant_address: address.to_owned(),
            participant_role: ExecutionParticipantRole::Maker,
            side: Side::Buy,
            price: Price::new(Decimal::new(50, 2)),
            size_shares: Shares::new(notional * Decimal::from(2)),
            notional_usd: Usd::new(notional),
            transaction_hash: format!("tx-{address}"),
            availability_policy_hash: hash,
        }
    }

    #[test]
    fn unavailable_source_marks_unavailable() {
        let config = FeaturesConfig::default();
        let execution_history =
            execution_history_snapshot(false, vec![fill_print("0x1", Decimal::from(100))]);
        let features = execution_history_only(&execution_history, &config);
        assert_eq!(features.len(), 9);
        for feature in features {
            assert_eq!(
                feature.value,
                Err(NullReason::FinalizedExecutionUnavailable)
            );
        }
    }

    #[test]
    fn empty_window_marks_insufficient() {
        let config = FeaturesConfig::default();
        let execution_history = execution_history_snapshot(true, Vec::new());
        let features = execution_history_only(&execution_history, &config);
        assert_eq!(features.len(), 9);
        for feature in features {
            assert_eq!(feature.value, Err(NullReason::InsufficientExecutionHistory));
        }
    }

    #[test]
    fn below_min_unique_insufficient() {
        let mut config = FeaturesConfig::default();
        config.structural.execution_min_unique_participants = 3;
        let prints = vec![
            fill_print("0x1", Decimal::from(100)),
            fill_print("0x2", Decimal::from(100)),
        ];
        let execution_history = execution_history_snapshot(true, prints);
        let features = execution_history_only(&execution_history, &config);
        let gini = features
            .iter()
            .find(|feature| feature.name == PARTICIPANT_GINI)
            .expect("gini");
        assert_eq!(gini.value, Err(NullReason::InsufficientExecutionHistory));
    }
}
