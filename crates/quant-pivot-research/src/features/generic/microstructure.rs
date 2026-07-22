//! Microstructure feature builder: order-flow rate, churn, queue depletion,
//! liquidity withdrawal, adverse-selection proxy, and stale-quote frequency from
//! the pre-fetched microstructure window.

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    runtime_config::FeatureFamily,
    types::{Probability, Usd},
};
use rust_decimal::Decimal;

use crate::features::{
    builder::{FeatureComputeCtx, FeatureGroupBuilder, RawFeature},
    names::{
        micro,
        micro::{
            ADVERSE_SELECTION_PROXY, BOOK_CHURN, QUEUE_DEPLETION, QUOTE_UPDATE_RATE,
            STALE_QUOTE_FREQUENCY, SUDDEN_LIQUIDITY_WITHDRAWAL,
        },
    },
    resolved::MicrostructureBucket,
    value::{EvidenceSourceKind, EvidenceSourceRef, FeatureName, FeatureValue, NullReason},
};

/// Builds the [`FeatureFamily::Microstructure`] features.
pub struct MicrostructureFeatureBuilder;

impl FeatureGroupBuilder for MicrostructureFeatureBuilder {
    fn family(&self) -> FeatureFamily {
        FeatureFamily::Microstructure
    }

    fn compute(&self, ctx: &FeatureComputeCtx<'_>) -> QuantResult<Vec<RawFeature>> {
        let window = ctx.window;
        let evidence = EvidenceSourceRef {
            source_kind: EvidenceSourceKind::ClickHouseFact,
            reference: window.token_id.to_string(),
            // Anchor on the freshest fact so the fact-lag staleness check sees the
            // true age; an empty window falls back to the PIT cutoff.
            effective_at: window
                .freshest_bucket_time()
                .unwrap_or_else(|| window.cutoff()),
            available_at: window.latest_available_at(),
        };
        let buckets = &window.buckets;
        if buckets.is_empty() {
            return Ok(missing_all());
        }

        Ok(vec![
            decimal(
                micro::QUOTE_UPDATE_RATE,
                quote_update_rate(buckets),
                &evidence,
            ),
            decimal(micro::BOOK_CHURN, book_churn(buckets), &evidence),
            decimal(micro::QUEUE_DEPLETION, queue_depletion(buckets), &evidence),
            decimal(
                micro::SUDDEN_LIQUIDITY_WITHDRAWAL,
                sudden_withdrawal(buckets),
                &evidence,
            ),
            decimal(
                micro::ADVERSE_SELECTION_PROXY,
                adverse_selection(buckets),
                &evidence,
            ),
            stale_quote_frequency(ctx, buckets, &evidence),
        ])
    }
}

/// Average book updates per bucket (1s buckets ⇒ updates per second).
fn quote_update_rate(buckets: &[MicrostructureBucket]) -> Option<Decimal> {
    let total: u64 = buckets.iter().map(|bucket| bucket.update_count).sum();
    let count = u64::try_from(buckets.len()).ok()?;
    if count == 0 {
        return None;
    }
    Some(Decimal::from(total) / Decimal::from(count))
}

/// Delta-to-update ratio: how much of the flow was incremental churn.
fn book_churn(buckets: &[MicrostructureBucket]) -> Option<Decimal> {
    let deltas: u64 = buckets.iter().map(|bucket| bucket.delta_count).sum();
    let updates: u64 = buckets.iter().map(|bucket| bucket.update_count).sum();
    if updates == 0 {
        return None;
    }
    Some(Decimal::from(deltas) / Decimal::from(updates))
}

/// Net top-1 depth decline across the window: `(first - last) / first`.
fn queue_depletion(buckets: &[MicrostructureBucket]) -> Option<Decimal> {
    let depths: Vec<Decimal> = buckets
        .iter()
        .filter_map(|bucket| bucket.top1_depth_usd_avg.map(Usd::inner))
        .collect();
    let first = *depths.first()?;
    let last = *depths.last()?;
    if depths.len() < 2 || first.is_zero() {
        return None;
    }
    Some((first - last) / first)
}

/// Largest single-bucket fractional depth drop (worst withdrawal).
fn sudden_withdrawal(buckets: &[MicrostructureBucket]) -> Option<Decimal> {
    let depths: Vec<Decimal> = buckets
        .iter()
        .filter_map(|bucket| bucket.top1_depth_usd_avg.map(Usd::inner))
        .collect();
    if depths.len() < 2 {
        return None;
    }
    let mut worst = Decimal::ZERO;
    for pair in depths.windows(2) {
        let prev = pair[0];
        let next = pair[1];
        if prev > Decimal::ZERO {
            let drop = (prev - next) / prev;
            if drop > worst {
                worst = drop;
            }
        }
    }
    Some(worst)
}

/// Crossed/gap events relative to total updates (adverse-selection proxy).
fn adverse_selection(buckets: &[MicrostructureBucket]) -> Option<Decimal> {
    let adverse: u64 = buckets
        .iter()
        .map(|bucket| bucket.crossed_count + bucket.gap_count)
        .sum();
    let updates: u64 = buckets.iter().map(|bucket| bucket.update_count).sum();
    if updates == 0 {
        return None;
    }
    Some(Decimal::from(adverse) / Decimal::from(updates))
}

/// Fraction of buckets whose worst book age exceeded the freshness threshold.
fn stale_quote_frequency(
    ctx: &FeatureComputeCtx<'_>,
    buckets: &[MicrostructureBucket],
    evidence: &EvidenceSourceRef,
) -> RawFeature {
    let threshold = ctx.data_quality.max_book_age_ms;
    let stale = buckets
        .iter()
        .filter(|bucket| bucket.max_book_age_ms > threshold)
        .count();
    let Ok(total) = u64::try_from(buckets.len()) else {
        return RawFeature::missing(STALE_QUOTE_FREQUENCY, NullReason::InsufficientHistory);
    };
    if total == 0 {
        return RawFeature::missing(STALE_QUOTE_FREQUENCY, NullReason::InsufficientHistory);
    }
    let Ok(stale) = u64::try_from(stale) else {
        return RawFeature::missing(STALE_QUOTE_FREQUENCY, NullReason::InsufficientHistory);
    };
    let fraction = Decimal::from(stale) / Decimal::from(total);
    RawFeature::present(
        STALE_QUOTE_FREQUENCY,
        FeatureValue::Probability(Probability::new(fraction)),
        evidence.clone(),
    )
}

/// Wrap a computed value, or mark it missing for insufficient history.
fn decimal(name: FeatureName, value: Option<Decimal>, evidence: &EvidenceSourceRef) -> RawFeature {
    match value {
        Some(decimal) => {
            RawFeature::present(name, FeatureValue::Decimal(decimal), evidence.clone())
        }
        None => RawFeature::missing(name, NullReason::InsufficientHistory),
    }
}

/// Every microstructure feature marked missing for an empty window.
fn missing_all() -> Vec<RawFeature> {
    [
        QUOTE_UPDATE_RATE,
        BOOK_CHURN,
        QUEUE_DEPLETION,
        SUDDEN_LIQUIDITY_WITHDRAWAL,
        ADVERSE_SELECTION_PROXY,
        STALE_QUOTE_FREQUENCY,
    ]
    .into_iter()
    .map(|name| RawFeature::missing(name, NullReason::InsufficientHistory))
    .collect()
}
