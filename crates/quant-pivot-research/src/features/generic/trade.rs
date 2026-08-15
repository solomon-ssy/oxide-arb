//! Finalized-execution feature family for L2-free bootstrap profiles.

use std::{collections::BTreeMap, time::Duration};

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::data_plane::ExecutionParticipantPrint,
    enums::common::Side,
    runtime_config::{DecimalValue, FeatureFamily},
};
use rust_decimal::Decimal;

use crate::{
    execution_history::{ParticipantConcentrationGate, compute_concentration},
    features::{
        builder::{FeatureComputeCtx, FeatureGroupBuilder, RawFeature},
        generic::stats::{ema_slope_time, macd_time, realized_volatility, simple_return},
        names::trade::{
            EMA_SLOPE, EXECUTION_COVERAGE_RATIO, EXECUTION_INTENSITY, EXECUTION_STALENESS_SECS,
            LAGGED_MOMENTUM, LAST_FILL_RETURN, MACD_NORM, PARTICIPANT_GINI, PARTICIPANT_HHI,
            REALIZED_VOLATILITY, SIGNED_NOTIONAL_IMBALANCE,
        },
        value::{EvidenceSourceKind, EvidenceSourceRef, FeatureName, FeatureValue, NullReason},
    },
};

pub struct TradeFeatureBuilder;

impl FeatureGroupBuilder for TradeFeatureBuilder {
    fn family(&self) -> FeatureFamily {
        FeatureFamily::Trade
    }

    fn compute(&self, ctx: &FeatureComputeCtx<'_>) -> QuantResult<Vec<RawFeature>> {
        if !ctx.execution_history.source_available {
            return Ok(all_missing(NullReason::FinalizedExecutionUnavailable));
        }
        let window_secs = ctx.config.structural.execution_window_secs;
        let prints = ctx
            .execution_history
            .prints_in(Duration::from_secs(window_secs));
        if prints.is_empty() {
            return Ok(all_missing(NullReason::InsufficientExecutionHistory));
        }
        let executions = unique_executions(&prints);
        if executions.len() < 3 {
            return Ok(all_missing(NullReason::InsufficientHistory));
        }
        let evidence = execution_evidence(ctx);
        let timed_prices = executions
            .iter()
            .map(|print| (print.effective_at.timestamp_millis(), print.price.inner()))
            .collect::<Vec<_>>();
        let prices = timed_prices
            .iter()
            .map(|(_, price)| *price)
            .collect::<Vec<_>>();
        let recent_prices = &prices[prices.len().saturating_sub(2)..];
        let lagged_prices = &prices[..prices.len().saturating_sub(1)];
        let last_return = simple_return(recent_prices);
        let volatility = realized_volatility(&prices);
        let lagged_momentum = simple_return(lagged_prices);
        let ema_slope = ema_slope_time(&timed_prices, ctx.config.momentum.ema_fast_secs)?;
        let macd = macd_time(
            &timed_prices,
            ctx.config.momentum.ema_fast_secs,
            ctx.config.momentum.ema_slow_secs,
        )?;
        let signed_imbalance = signed_imbalance(&executions);
        let intensity = Decimal::from(executions.len()) / Decimal::from(window_secs);
        let staleness = ctx
            .decision_at
            .signed_duration_since(executions[executions.len() - 1].effective_at)
            .num_seconds()
            .max(0);
        let coverage = execution_coverage(&prints);
        let concentration = compute_concentration(&owned_prints(&prints), true, &gate(ctx)).ok();

        Ok(vec![
            decimal_feature(LAST_FILL_RETURN, last_return, &evidence),
            decimal_feature(REALIZED_VOLATILITY, volatility, &evidence),
            decimal_feature(LAGGED_MOMENTUM, lagged_momentum, &evidence),
            decimal_feature(EMA_SLOPE, ema_slope, &evidence),
            decimal_feature(MACD_NORM, macd, &evidence),
            decimal_feature(SIGNED_NOTIONAL_IMBALANCE, signed_imbalance, &evidence),
            RawFeature::present(
                EXECUTION_INTENSITY,
                FeatureValue::Decimal(intensity.round_dp(12)),
                evidence.clone(),
            ),
            decimal_feature(
                PARTICIPANT_GINI,
                concentration.as_ref().map(|snapshot| snapshot.gini),
                &evidence,
            ),
            decimal_feature(
                PARTICIPANT_HHI,
                concentration.as_ref().map(|snapshot| snapshot.hhi),
                &evidence,
            ),
            RawFeature::present(
                EXECUTION_STALENESS_SECS,
                FeatureValue::Count(u64::try_from(staleness).unwrap_or(u64::MAX)),
                evidence.clone(),
            ),
            RawFeature::present(
                EXECUTION_COVERAGE_RATIO,
                FeatureValue::Decimal(coverage),
                evidence,
            ),
        ])
    }
}

fn unique_executions<'a>(
    prints: &[&'a ExecutionParticipantPrint],
) -> Vec<&'a ExecutionParticipantPrint> {
    let mut executions = BTreeMap::new();
    for print in prints {
        executions.entry(print.execution_id).or_insert(*print);
    }
    let mut executions = executions.into_values().collect::<Vec<_>>();
    executions.sort_by_key(|print| (print.effective_at, print.execution_id));
    executions
}

fn signed_imbalance(executions: &[&ExecutionParticipantPrint]) -> Option<Decimal> {
    let mut signed = Decimal::ZERO;
    let mut total = Decimal::ZERO;
    for execution in executions {
        let notional = execution.notional_usd.inner();
        total += notional;
        match execution.side {
            Side::Buy => signed += notional,
            Side::Sell => signed -= notional,
        }
    }
    (total > Decimal::ZERO).then(|| (signed / total).round_dp(12))
}

fn execution_coverage(prints: &[&ExecutionParticipantPrint]) -> Decimal {
    let known = prints
        .iter()
        .filter(|print| {
            !print.participant_address.is_empty() && print.model_available_at >= print.effective_at
        })
        .count();
    (Decimal::from(known) / Decimal::from(prints.len())).round_dp(12)
}

fn execution_evidence(ctx: &FeatureComputeCtx<'_>) -> EvidenceSourceRef {
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

const fn gate(ctx: &FeatureComputeCtx<'_>) -> ParticipantConcentrationGate {
    ParticipantConcentrationGate {
        min_unique_participants: ctx.config.structural.execution_min_unique_participants,
        min_notional_usd: config_decimal(&ctx.config.structural.execution_min_notional_usd),
        min_coverage_ratio: config_decimal(&ctx.config.structural.execution_min_coverage_ratio),
    }
}

const fn config_decimal(value: &DecimalValue) -> Decimal {
    value.value
}

fn owned_prints(prints: &[&ExecutionParticipantPrint]) -> Vec<ExecutionParticipantPrint> {
    prints.iter().map(|print| (*print).clone()).collect()
}

fn decimal_feature(
    name: FeatureName,
    value: Option<Decimal>,
    evidence: &EvidenceSourceRef,
) -> RawFeature {
    match value {
        Some(value) => RawFeature::present(name, FeatureValue::Decimal(value), evidence.clone()),
        None => RawFeature::missing(name, NullReason::InsufficientHistory),
    }
}

fn all_missing(reason: NullReason) -> Vec<RawFeature> {
    [
        LAST_FILL_RETURN,
        REALIZED_VOLATILITY,
        LAGGED_MOMENTUM,
        EMA_SLOPE,
        MACD_NORM,
        SIGNED_NOTIONAL_IMBALANCE,
        EXECUTION_INTENSITY,
        PARTICIPANT_GINI,
        PARTICIPANT_HHI,
        EXECUTION_STALENESS_SECS,
        EXECUTION_COVERAGE_RATIO,
    ]
    .into_iter()
    .map(|name| RawFeature::missing(name, reason))
    .collect()
}
