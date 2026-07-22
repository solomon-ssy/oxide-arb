//! Time-series feature builder: windowed returns, momentum, realized volatility,
//! reversal, and spread/depth trends from the pre-fetched microstructure window.
//!
//! Statistical reductions live in the feature-statistics module; returns are exact
//! `Decimal`, and the one `f64` reduction (realized vol) is quantized there so a
//! value never reaches the vector unrounded. Provenance is anchored on the
//! window's freshest bucket, so the fact-lag staleness check sees the true age.

use std::time::Duration;

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    runtime_config::FeatureFamily,
    types::{Bps, Price, Usd},
};
use rust_decimal::Decimal;

use crate::features::{
    builder::{FeatureComputeCtx, FeatureGroupBuilder, RawFeature},
    generic::stats,
    resolved::MarketWindowSnapshot,
    value::{EvidenceSourceKind, EvidenceSourceRef, FeatureName, FeatureValue, NullReason},
};

/// Builds the [`FeatureFamily::TimeSeries`] features.
pub struct TimeSeriesFeatureBuilder;

impl FeatureGroupBuilder for TimeSeriesFeatureBuilder {
    fn family(&self) -> FeatureFamily {
        FeatureFamily::TimeSeries
    }

    fn compute(&self, ctx: &FeatureComputeCtx<'_>) -> QuantResult<Vec<RawFeature>> {
        let window = ctx.window;
        let evidence = EvidenceSourceRef {
            source_kind: EvidenceSourceKind::ClickHouseFact,
            reference: window.token_id.to_string(),
            // Anchor provenance on the freshest fact actually available so the
            // fact-lag staleness check measures the true age (not the cutoff).
            effective_at: window
                .freshest_bucket_time()
                .unwrap_or_else(|| window.cutoff()),
            available_at: window.latest_available_at(),
        };
        let mut out = Vec::new();

        for secs in &ctx.config.bar_windows_secs {
            let mids = mids(window, *secs);
            out.push(decimal_or_missing(
                format!("ts.return_{secs}s"),
                stats::simple_return(&mids),
                &evidence,
            ));
            out.push(decimal_or_missing(
                format!("ts.spread_trend_{secs}s"),
                stats::simple_return(&spread_series(window, *secs)),
                &evidence,
            ));
            out.push(decimal_or_missing(
                format!("ts.depth_trend_{secs}s"),
                stats::simple_return(&depth_series(window, *secs)),
                &evidence,
            ));
        }
        let momentum = &ctx.config.momentum;
        for secs in &momentum.roc_windows_secs {
            out.push(decimal_or_missing(
                format!("ts.momentum_roc_{secs}s"),
                lag_skipped_roc(window, *secs, momentum.roc_lag_secs),
                &evidence,
            ));
        }
        for secs in &momentum.slope_windows_secs {
            out.push(decimal_or_missing(
                format!("ts.ema_slope_{secs}s"),
                stats::ema_slope_time(&mids_ts(window, *secs), momentum.ema_fast_secs)?,
                &evidence,
            ));
        }
        for secs in &ctx.config.volatility_windows_secs {
            let series = mids(window, *secs);
            out.push(decimal_or_missing(
                format!("ts.realized_vol_{secs}s"),
                stats::realized_volatility(&series),
                &evidence,
            ));
            out.push(decimal_or_missing(
                format!("ts.vol_adjusted_return_{secs}s"),
                vol_adjusted_return(&series),
                &evidence,
            ));
        }
        // Vol-normalized MACD over the slow-EMA window (long enough for both legs).
        out.push(decimal_or_missing(
            "ts.macd_norm".to_owned(),
            stats::macd_time(
                &mids_ts(window, momentum.ema_slow_secs),
                momentum.ema_fast_secs,
                momentum.ema_slow_secs,
            )?,
            &evidence,
        ));

        let reversal = ctx
            .config
            .bar_windows_secs
            .iter()
            .copied()
            .max()
            .and_then(|largest| stats::mean_reversion(&mids(window, largest)));
        out.push(decimal_or_missing(
            "ts.price_reversal".to_owned(),
            reversal,
            &evidence,
        ));
        Ok(out)
    }
}

/// Mid prices within the trailing `secs` window as decimals.
fn mids(window: &MarketWindowSnapshot, secs: u64) -> Vec<Decimal> {
    window
        .mids_in(Duration::from_secs(secs))
        .into_iter()
        .map(Price::inner)
        .collect()
}

/// `(epoch_ms, mid)` samples within the trailing `secs` window — the time-native
/// input for the duration-based EMA / MACD estimators (weighted by real elapsed
/// time, so a sparse book smooths over the same horizon as a dense one).
fn mids_ts(window: &MarketWindowSnapshot, secs: u64) -> Vec<(i64, Decimal)> {
    window
        .mids_ts_in(Duration::from_secs(secs))
        .into_iter()
        .map(|(at, mid)| (at.timestamp_millis(), mid.inner()))
        .collect()
}

/// Lag-skipped rate of change over `[t - window, t - lag]`.
///
/// The base is the mid at `t - window` (oldest mid in the window) and the recent
/// value is the mid at `t - lag` (oldest mid within the trailing `lag`). Skipping
/// the most recent `lag` seconds excludes the short-term reversal segment
/// (classic 12-1 momentum), so this is **not** the endpoint simple return. `None`
/// when either endpoint is unavailable.
fn lag_skipped_roc(
    window: &MarketWindowSnapshot,
    window_secs: u64,
    lag_secs: u64,
) -> Option<Decimal> {
    if window_secs <= lag_secs {
        return None;
    }
    let full = mids(window, window_secs);
    let recent = mids(window, lag_secs);
    let base = *full.first()?;
    let at_lag = *recent.first()?;
    stats::rate_of_change(base, at_lag)
}

/// Volatility-adjusted return over a mid series: `simple_return / realized_vol`.
fn vol_adjusted_return(series: &[Decimal]) -> Option<Decimal> {
    let simple = stats::simple_return(series)?;
    let vol = stats::realized_volatility(series)?;
    stats::vol_adjusted(simple, vol)
}

/// Spread (bps) series within the trailing `secs` window.
fn spread_series(window: &MarketWindowSnapshot, secs: u64) -> Vec<Decimal> {
    window
        .buckets_in(Duration::from_secs(secs))
        .into_iter()
        .filter_map(|bucket| bucket.spread_bps_avg.map(Bps::inner))
        .collect()
}

/// Top-1 depth (USD) series within the trailing `secs` window.
fn depth_series(window: &MarketWindowSnapshot, secs: u64) -> Vec<Decimal> {
    window
        .buckets_in(Duration::from_secs(secs))
        .into_iter()
        .filter_map(|bucket| bucket.top1_depth_usd_avg.map(Usd::inner))
        .collect()
}

/// Wrap a computed value, or mark it missing for insufficient history.
fn decimal_or_missing(
    name: String,
    value: Option<Decimal>,
    evidence: &EvidenceSourceRef,
) -> RawFeature {
    let name = FeatureName::new(name);
    match value {
        Some(decimal) => {
            RawFeature::present(name, FeatureValue::Decimal(decimal), evidence.clone())
        }
        None => RawFeature::missing(name, NullReason::InsufficientHistory),
    }
}
