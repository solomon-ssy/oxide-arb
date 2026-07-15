//! Polymarket-native out-of-sample structural volatility benchmark.
//!
//! This implements the closed-form deadline-resolution (DR) and DR-AS
//! conditional-variance specifications from arXiv:2607.08199. It intentionally
//! produces sealed risk-model evidence rather than pretending that a volatility
//! forecast is an entry/exit policy candidate.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::TradeTapeRow,
    enums::clickhouse::{ChTradeReconciliationStatus, ChTradeTapeSource},
    hashing::CanonicalDigest,
    types::{
        MarketId, StructuralVolatilityOosEvidence, StructuralVolatilityOosFoldRow, TokenId, Usd,
    },
};
use rust_decimal::{Decimal, MathematicalOps};

use crate::training::TrainingExample;

pub const STRUCTURAL_VOLATILITY_OOS_VERSION: &str = "polymarket_dr_as_expanding_monthly_oos_v1";
const MINIMUM_CONTRACT_OBSERVATIONS: usize = 48;
const MINIMUM_TRAINING_OBSERVATIONS: usize = 100;
const MINIMUM_OOS_FOLDS: usize = 2;
const MINIMUM_OOS_FORECASTS: u64 = 100;

#[derive(Debug, Clone)]
struct ForecastPoint {
    at: DateTime<Utc>,
    price: Decimal,
    next_price: Decimal,
    deadline_variance: Decimal,
    adverse_selection_basis: Decimal,
    volume_weight: Decimal,
}

struct IntervalMetrics {
    score: Decimal,
    coverage: Decimal,
    volume_weight: Decimal,
}

/// Compute expanding-calendar-month OOS benchmark evidence from the exact
/// Dataset rows and reconciled Source-Slice trade tape used by policy replay.
pub fn evaluate_structural_volatility_oos(
    examples: &[TrainingExample],
    trade_tape: &[TradeTapeRow],
) -> QuantResult<(
    StructuralVolatilityOosEvidence,
    Vec<StructuralVolatilityOosFoldRow>,
)> {
    let points = forecast_points(examples, trade_tape)?;
    let mut by_month = BTreeMap::<(i32, u32), Vec<&ForecastPoint>>::new();
    for point in &points {
        by_month
            .entry((point.at.year(), point.at.month()))
            .or_default()
            .push(point);
    }
    let months = by_month.keys().copied().collect::<Vec<_>>();
    let mut folds = Vec::new();
    let mut total_forecasts = 0_u64;
    let mut total_weight = Decimal::ZERO;
    let mut deadline_score_sum = Decimal::ZERO;
    let mut dr_as_score_sum = Decimal::ZERO;
    let mut deadline_coverage_sum = Decimal::ZERO;
    let mut dr_as_coverage_sum = Decimal::ZERO;
    for (month_index, month) in months.iter().enumerate().skip(1) {
        let training = months[..month_index]
            .iter()
            .flat_map(|month| by_month.get(month).into_iter().flatten())
            .copied()
            .collect::<Vec<_>>();
        let test = by_month.get(month).cloned().unwrap_or_default();
        if training.len() < MINIMUM_TRAINING_OBSERVATIONS || test.is_empty() {
            continue;
        }
        let fitted_k = fit_nonnegative_k(&training)?;
        let deadline = interval_metrics(&test, Decimal::ZERO)?;
        let dr_as = interval_metrics(&test, fitted_k)?;
        if deadline.volume_weight != dr_as.volume_weight || deadline.volume_weight <= Decimal::ZERO
        {
            return Err(methodology(
                "structural volatility benchmarks do not share positive OOS support".to_owned(),
            ));
        }
        let test_start = month_start(*month)?;
        let test_end = next_month_start(*month)?;
        let training_start = training
            .iter()
            .map(|point| point.at)
            .min()
            .ok_or_else(|| methodology("structural OOS training fold is empty".to_owned()))?;
        let fold_index = u32::try_from(folds.len()).map_err(|error| {
            methodology(format!(
                "structural OOS fold index does not fit u32: {error}"
            ))
        })?;
        let forecast_count = u64::try_from(test.len()).map_err(|error| {
            methodology(format!(
                "structural OOS forecast count does not fit u64: {error}"
            ))
        })?;
        folds.push(StructuralVolatilityOosFoldRow {
            fold_index,
            training_window_start: training_start,
            training_window_end: test_start,
            test_window_start: test_start,
            test_window_end: test_end,
            training_sample_count: u64::try_from(training.len()).map_err(|error| {
                methodology(format!(
                    "structural OOS training count does not fit u64: {error}"
                ))
            })?,
            forecast_count,
            test_volume_weight: Usd::new(deadline.volume_weight),
            fitted_nonnegative_k: fitted_k,
            deadline_vw_interval_score: deadline.score,
            dr_as_vw_interval_score: dr_as.score,
            deadline_volume_weighted_coverage: deadline.coverage,
            dr_as_volume_weighted_coverage: dr_as.coverage,
        });
        total_forecasts = total_forecasts.checked_add(forecast_count).ok_or_else(|| {
            methodology("structural OOS total forecast count overflow".to_owned())
        })?;
        total_weight += deadline.volume_weight;
        deadline_score_sum += deadline.score * deadline.volume_weight;
        dr_as_score_sum += dr_as.score * dr_as.volume_weight;
        deadline_coverage_sum += deadline.coverage * deadline.volume_weight;
        dr_as_coverage_sum += dr_as.coverage * dr_as.volume_weight;
    }
    let fold_count = u32::try_from(folds.len()).map_err(|error| {
        methodology(format!(
            "structural OOS fold count does not fit u32: {error}"
        ))
    })?;
    let valid = folds.len() >= MINIMUM_OOS_FOLDS
        && total_forecasts >= MINIMUM_OOS_FORECASTS
        && total_weight > Decimal::ZERO;
    let divide = |sum: Decimal| {
        if total_weight > Decimal::ZERO {
            sum / total_weight
        } else {
            Decimal::ZERO
        }
    };
    let methodology_hash = CanonicalDigest::content_hash_json(&(
        STRUCTURAL_VOLATILITY_OOS_VERSION,
        "arxiv:2607.08199v1",
        "conditional_active_hourly_mid_quote_updates_after_causal_contract_warmup",
        "deadline_variance=p*(1-p)/time_to_resolution_hours",
        "dr_as_variance=deadline_variance+K*sqrt(hourly_volume_usd)*spread^2/4",
        "volume_weighted_gaussian_qml_nonnegative_K",
        "monthly_expanding_window",
        "clipped_symmetric_normal_reference_95pct_interval",
        "volume_weighted_winkler_interval_score",
        MINIMUM_CONTRACT_OBSERVATIONS,
        MINIMUM_TRAINING_OBSERVATIONS,
        MINIMUM_OOS_FOLDS,
        MINIMUM_OOS_FORECASTS,
    ))?;
    Ok((
        StructuralVolatilityOosEvidence {
            methodology_hash,
            active_update_only: true,
            activity_proxy: "sqrt_reconciled_trailing_hour_volume_usd".to_owned(),
            minimum_contract_observations: u32::try_from(MINIMUM_CONTRACT_OBSERVATIONS).map_err(
                |error| {
                    methodology(format!(
                        "structural minimum observation count does not fit u32: {error}"
                    ))
                },
            )?,
            fold_count,
            forecast_count: total_forecasts,
            deadline_vw_interval_score: divide(deadline_score_sum),
            dr_as_vw_interval_score: divide(dr_as_score_sum),
            deadline_volume_weighted_coverage: divide(deadline_coverage_sum),
            dr_as_volume_weighted_coverage: divide(dr_as_coverage_sum),
            valid,
        },
        folds,
    ))
}

fn forecast_points(
    examples: &[TrainingExample],
    trade_tape: &[TradeTapeRow],
) -> QuantResult<Vec<ForecastPoint>> {
    let mut decisions = BTreeMap::<TokenId, BTreeSet<DateTime<Utc>>>::new();
    for example in examples {
        decisions
            .entry(example.token_id.clone())
            .or_default()
            .insert(example.decision_at());
    }
    let mut hourly_volume = BTreeMap::<(TokenId, DateTime<Utc>), Decimal>::new();
    for trade in trade_tape
        .iter()
        .filter(|trade| canonical_activity_trade(trade))
    {
        let Some(event_at) = DateTime::from_timestamp_millis(trade.event_time) else {
            continue;
        };
        let Some(token_decisions) = decisions.get(&trade.token_id) else {
            continue;
        };
        let Some(decision_at) = token_decisions.range(event_at..).next().copied() else {
            continue;
        };
        if event_at <= decision_at - Duration::hours(1)
            || trade.ingestion_time > decision_at.timestamp_millis()
        {
            continue;
        }
        *hourly_volume
            .entry((trade.token_id.clone(), decision_at))
            .or_default() += trade.notional_usd.to_usd().inner();
    }
    let mut timelines = BTreeMap::<(MarketId, TokenId), Vec<&TrainingExample>>::new();
    for example in examples {
        timelines
            .entry((example.market_id.clone(), example.token_id.clone()))
            .or_default()
            .push(example);
    }
    let mut qualified = Vec::new();
    for ((_market_id, token_id), mut timeline) in timelines {
        timeline.sort_by_key(|example| example.decision_at());
        let mut contract_points = Vec::new();
        for pair in timeline.windows(2) {
            let current = pair[0];
            let next = pair[1];
            if next.decision_at() - current.decision_at() != Duration::hours(1) {
                continue;
            }
            let Some(context) = current
                .decision_capture
                .as_ref()
                .map(|capture| &capture.market_context)
            else {
                continue;
            };
            let Some(next_context) = next
                .decision_capture
                .as_ref()
                .map(|capture| &capture.market_context)
            else {
                continue;
            };
            let (Some(price), Some(next_price), Some(best_bid), Some(best_ask), Some(ttr_secs)) = (
                context.mid_price,
                next_context.mid_price,
                context.best_bid,
                context.best_ask,
                context.time_to_resolution_secs,
            ) else {
                continue;
            };
            let price = price.inner();
            let next_price = next_price.inner();
            let spread = best_ask.inner() - best_bid.inner();
            let volume = hourly_volume
                .get(&(token_id.clone(), current.decision_at()))
                .copied()
                .unwrap_or(Decimal::ZERO);
            if price <= Decimal::ZERO
                || price >= Decimal::ONE
                || next_price == price
                || spread <= Decimal::ZERO
                || volume <= Decimal::ZERO
                || ttr_secs < 3_600
            {
                continue;
            }
            let tau_hours = Decimal::from(ttr_secs) / Decimal::from(3_600);
            let deadline_variance = price * (Decimal::ONE - price) / tau_hours;
            let volume_scale = volume.sqrt().ok_or_else(|| {
                methodology("structural OOS volume square root is invalid".to_owned())
            })?;
            contract_points.push(ForecastPoint {
                at: current.decision_at(),
                price,
                next_price,
                deadline_variance,
                adverse_selection_basis: volume_scale * spread * spread / Decimal::from(4),
                volume_weight: volume,
            });
        }
        // Eligibility is causal: the first forecast is admitted only after the
        // contract has already produced the required number of active hourly
        // innovations. Filtering on the contract's eventual full-sample count
        // would leak future activity into earlier OOS folds.
        qualified.extend(
            contract_points
                .into_iter()
                .skip(MINIMUM_CONTRACT_OBSERVATIONS),
        );
    }
    Ok(qualified)
}

const fn canonical_activity_trade(trade: &TradeTapeRow) -> bool {
    matches!(
        (trade.source, trade.reconciliation_status),
        (
            ChTradeTapeSource::MarketWs,
            ChTradeReconciliationStatus::Matched
        ) | (
            ChTradeTapeSource::OnChainOrderFilled,
            ChTradeReconciliationStatus::OnChainOnly
        )
    )
}

fn fit_nonnegative_k(points: &[&ForecastPoint]) -> QuantResult<Decimal> {
    let upper = points
        .iter()
        .filter_map(|point| {
            let innovation = point.next_price - point.price;
            let residual = innovation * innovation - point.deadline_variance;
            (residual > Decimal::ZERO && point.adverse_selection_basis > Decimal::ZERO)
                .then_some(residual / point.adverse_selection_basis)
        })
        .max()
        .unwrap_or(Decimal::ONE)
        .max(Decimal::ONE)
        * Decimal::from(4);
    let mut low = Decimal::ZERO;
    let mut high = upper;
    let golden = Decimal::new(6_180_339_887_498_949, 16);
    for _ in 0..96 {
        let left = high - golden * (high - low);
        let right = low + golden * (high - low);
        if gaussian_qml(points, left)? <= gaussian_qml(points, right)? {
            high = right;
        } else {
            low = left;
        }
    }
    Ok((low + high) / Decimal::TWO)
}

fn gaussian_qml(points: &[&ForecastPoint], k: Decimal) -> QuantResult<Decimal> {
    let variance_floor = Decimal::new(1, 12);
    let mut weighted = Decimal::ZERO;
    let mut total_weight = Decimal::ZERO;
    for point in points {
        let variance =
            (point.deadline_variance + k * point.adverse_selection_basis).max(variance_floor);
        let innovation = point.next_price - point.price;
        let log_variance = variance.checked_ln().ok_or_else(|| {
            methodology("structural OOS QML variance logarithm is invalid".to_owned())
        })?;
        weighted += point.volume_weight * (log_variance + innovation * innovation / variance);
        total_weight += point.volume_weight;
    }
    if total_weight <= Decimal::ZERO {
        return Err(methodology(
            "structural OOS QML has no positive volume weight".to_owned(),
        ));
    }
    Ok(weighted / total_weight)
}

fn interval_metrics(points: &[&ForecastPoint], k: Decimal) -> QuantResult<IntervalMetrics> {
    let alpha = Decimal::new(5, 2);
    let z = Decimal::new(1_959_963_984_540_054, 15);
    let variance_floor = Decimal::new(1, 12);
    let mut score_sum = Decimal::ZERO;
    let mut covered_weight = Decimal::ZERO;
    let mut total_weight = Decimal::ZERO;
    for point in points {
        let variance =
            (point.deadline_variance + k * point.adverse_selection_basis).max(variance_floor);
        let scale = variance.sqrt().ok_or_else(|| {
            methodology("structural OOS forecast variance square root is invalid".to_owned())
        })?;
        let lower = (point.price - z * scale).max(Decimal::ZERO);
        let upper = (point.price + z * scale).min(Decimal::ONE);
        let miss_penalty = if point.next_price < lower {
            Decimal::TWO / alpha * (lower - point.next_price)
        } else if point.next_price > upper {
            Decimal::TWO / alpha * (point.next_price - upper)
        } else {
            covered_weight += point.volume_weight;
            Decimal::ZERO
        };
        score_sum += point.volume_weight * (upper - lower + miss_penalty);
        total_weight += point.volume_weight;
    }
    if total_weight <= Decimal::ZERO {
        return Err(methodology(
            "structural OOS interval score has no positive volume weight".to_owned(),
        ));
    }
    Ok(IntervalMetrics {
        score: score_sum / total_weight,
        coverage: covered_weight / total_weight,
        volume_weight: total_weight,
    })
}

fn month_start(month: (i32, u32)) -> QuantResult<DateTime<Utc>> {
    Utc.with_ymd_and_hms(month.0, month.1, 1, 0, 0, 0)
        .single()
        .ok_or_else(|| {
            methodology(format!(
                "invalid structural OOS month {}-{}",
                month.0, month.1
            ))
        })
}

fn next_month_start(month: (i32, u32)) -> QuantResult<DateTime<Utc>> {
    let next = if month.1 == 12 {
        (month.0 + 1, 1)
    } else {
        (month.0, month.1 + 1)
    };
    month_start(next)
}

fn methodology(detail: String) -> QuantError {
    ResearchError::ValidationMethodology { detail }.into()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        ForecastPoint, fit_nonnegative_k, interval_metrics, month_start, next_month_start,
    };

    fn point(
        hour: u32,
        next_price: Decimal,
        deadline_variance: Decimal,
        adverse_selection_basis: Decimal,
        volume_weight: Decimal,
    ) -> ForecastPoint {
        ForecastPoint {
            at: Utc
                .with_ymd_and_hms(2026, 1, 1, hour, 0, 0)
                .single()
                .expect("time"),
            price: dec!(0.5),
            next_price,
            deadline_variance,
            adverse_selection_basis,
            volume_weight,
        }
    }

    #[test]
    fn month_windows_are_half_open_across_year_boundary() {
        assert_eq!(
            next_month_start((2025, 12)).expect("next"),
            month_start((2026, 1)).expect("month")
        );
    }

    #[test]
    fn qml_scale_is_nonnegative_and_collapses_without_excess_variance() {
        let points = [
            point(0, dec!(0.55), dec!(0.01), dec!(0.002), dec!(100)),
            point(1, dec!(0.45), dec!(0.01), dec!(0.003), dec!(200)),
        ];
        let refs = points.iter().collect::<Vec<_>>();

        let fitted = fit_nonnegative_k(&refs).expect("fit");

        assert!(fitted >= Decimal::ZERO);
        assert!(fitted < dec!(0.000001));
    }

    #[test]
    fn interval_metrics_use_forecast_origin_volume_weights() {
        let points = [
            point(0, dec!(0.51), dec!(0.0025), Decimal::ZERO, dec!(9)),
            point(1, dec!(0.99), dec!(0.0001), Decimal::ZERO, dec!(1)),
        ];
        let refs = points.iter().collect::<Vec<_>>();

        let metrics = interval_metrics(&refs, Decimal::ZERO).expect("metrics");

        assert_eq!(metrics.volume_weight, dec!(10));
        assert_eq!(metrics.coverage, dec!(0.9));
        assert!(metrics.score > Decimal::ZERO);
    }
}
