//! [`PortfolioReplayBacktester`]: the deterministic PIT replay loop.
//!
//! Per tick: model inference (over the PIT-resolved factor table) → LP/MILP
//! allocation → outcome resolution against settled truth → metric accumulation.
//! The engine never touches a live `BookStore`; its only inputs are the
//! in-memory ticks and the model runtime. It pins the deterministic continuous
//! relaxation mode on the pure-Rust microlp backend
//! ([`backtest_optimizer`](crate::portfolio::backtest_optimizer)) so the report
//! hash is reproducible and build-independent.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::quant::DataQualityStatus,
    types::{
        BacktestReportId, ContentHash, ExposureBreakdown, ModelVersionId, Probability,
        RuntimeConfigVersionId, Usd,
    },
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    backtest::{
        BacktestInputs, BacktestMarketMeta, BacktestReport, BacktestRequest, BacktestRunResult,
        BacktestTick, Backtester, CategoryMetric, ExpectedVsRealized, MarketOutcome, PnlCurvePoint,
        PnlSimulation, PortfolioCaps, SampleOutcome, metrics, simulator,
    },
    hashing::ResearchHasher,
    model::runtime::{MarketInferenceContext, ModelInputAuditState, ModelRuntimeOutput},
    portfolio::{
        allocator::{
            Allocation, AllocationInput, AllocationOutput, CandidateMeta, PortfolioAllocator,
        },
        optimizer::backtest_optimizer,
    },
    precision::RESEARCH_DECIMAL_SCALE,
};

/// The lower tail fraction used for the tail-loss (`CVaR`) metric.
fn tail_quantile() -> Decimal {
    Decimal::new(10, 2) // 0.10
}

/// Deterministic PIT-replay backtester over the LP/MILP portfolio allocator.
#[derive(Clone)]
pub struct PortfolioReplayBacktester {
    allocator: Arc<dyn PortfolioAllocator>,
}

impl PortfolioReplayBacktester {
    /// Construct the backtester with the pinned deterministic relaxation allocator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            allocator: backtest_optimizer(),
        }
    }
}

impl Default for PortfolioReplayBacktester {
    fn default() -> Self {
        Self::new()
    }
}

/// Mutable replay accumulators threaded across ticks.
#[derive(Default)]
struct RunAccumulator {
    samples: Vec<SampleOutcome>,
    pnl_curve: Vec<PnlCurvePoint>,
    tick_weights: Vec<BTreeMap<String, Decimal>>,
    /// Per-tick return (`tick_pnl / tick_allocated`), `0` for a tick with no
    /// allocation — the Sharpe-ratio input series (Phase 11.5 §3.4).
    tick_returns: Vec<Decimal>,
    missing_feature_count: u64,
    total_emitted: u64,
    total_allocated: Decimal,
    realized_pnl: Decimal,
}

#[async_trait]
impl Backtester for PortfolioReplayBacktester {
    async fn run(&self, inputs: BacktestInputs<'_>) -> QuantResult<BacktestRunResult> {
        let mut ticks = inputs.ticks;
        ticks.sort_by_key(|tick| tick.decision_at);

        let mut acc = RunAccumulator::default();
        for tick in &ticks {
            let output = inputs.model.infer_batch(tick.model_input.clone()).await?;
            process_tick(
                tick,
                &output,
                self.allocator.as_ref(),
                &inputs.caps,
                &mut acc,
            )?;
        }

        let metrics = BuildMetrics {
            samples: &acc.samples,
            pnl_curve: &acc.pnl_curve,
            tick_weights: &acc.tick_weights,
            tick_returns: &acc.tick_returns,
            missing_feature_count: acc.missing_feature_count,
            total_emitted: acc.total_emitted,
            total_allocated: acc.total_allocated,
            realized_pnl: acc.realized_pnl,
            budget: inputs.caps.total_budget_usd,
        };
        let report = build_report(&inputs.request, &metrics)?;
        Ok(BacktestRunResult {
            report,
            sample_outcomes: acc.samples,
        })
    }
}

/// Process one replay tick: allocate over the inferred candidates, resolve their
/// realized outcomes against settled truth, and fold the results into `acc`.
fn process_tick(
    tick: &BacktestTick,
    output: &ModelRuntimeOutput,
    allocator: &dyn PortfolioAllocator,
    caps: &PortfolioCaps,
    acc: &mut RunAccumulator,
) -> QuantResult<()> {
    acc.total_emitted += output.candidates.len() as u64;
    record_missing_input_count(acc, output)?;

    let meta: BTreeMap<&str, &BacktestMarketMeta> = tick
        .market_meta
        .iter()
        .map(|m| (m.market_id.as_str(), m))
        .collect();
    let outcomes: BTreeMap<&str, &MarketOutcome> = tick
        .outcomes
        .iter()
        .map(|o| (o.market_id.as_str(), o))
        .collect();
    // The PIT-resolved scoring context per market, so each realized sample
    // records the *actual* data-quality / liquidity / horizon / substitution
    // stratum it was scored under (never a hardcoded `Fresh`).
    let context: BTreeMap<&str, &MarketInferenceContext> = tick
        .model_input
        .market_contexts()
        .into_iter()
        .map(|(market_id, context)| (market_id.as_str(), context))
        .collect();

    let allocation = allocate_tick(output, allocator, caps, &meta)?;
    let alloc_by_id: BTreeMap<String, &Allocation> = allocation
        .allocations
        .iter()
        .map(|a| (a.signal_candidate_id.to_string(), a))
        .collect();
    acc.tick_weights
        .push(weights_for_tick(&allocation.allocations));

    let mut tick_pnl = Decimal::ZERO;
    let mut tick_allocated = Decimal::ZERO;
    for candidate in &output.candidates {
        let (Some(market), Some(outcome)) = (
            meta.get(candidate.market_id.as_str()),
            outcomes.get(candidate.market_id.as_str()),
        ) else {
            continue;
        };
        if !outcome.matured {
            continue;
        }
        let realized = simulator::realized_return_bps(
            candidate.outcome_side,
            candidate.entry_price_ref,
            outcome.settled_yes,
        );
        let allocation = alloc_by_id.get(&candidate.signal_candidate_id.to_string());
        let allocated_usd = allocation.map_or(Usd::ZERO, |a| a.allocated_usd);
        let liquidity_feasible = allocation.is_none_or(|a| a.liquidity_feasible);
        tick_pnl += simulator::realized_pnl_usd(
            allocated_usd.inner(),
            candidate.outcome_side,
            candidate.entry_price_ref,
            outcome.settled_yes,
        );
        acc.total_allocated += allocated_usd.inner();
        tick_allocated += allocated_usd.inner();
        let market_context = context.get(candidate.market_id.as_str());
        acc.samples.push(SampleOutcome {
            decision_at: tick.decision_at,
            market_id: candidate.market_id.clone(),
            token_id: candidate.token_id.clone(),
            category: market.category,
            outcome_side: candidate.outcome_side,
            composite_score: candidate.composite_score,
            confidence: candidate.confidence,
            expected_return_bps: candidate.expected_return_bps,
            realized_return_bps: realized,
            settled_yes: outcome.settled_yes,
            max_adverse_excursion_bps: outcome.max_adverse_excursion_bps,
            allocated_usd,
            liquidity_feasible,
            data_quality: market_context
                .map_or(DataQualityStatus::Insufficient, |c| c.data_quality),
            liquidity_usd: market_context.and_then(|c| c.liquidity_usd),
            time_to_resolution_secs: market_context.and_then(|c| c.time_to_resolution_secs),
            prediction_horizon_secs: candidate.suggested_horizon_secs,
            substitution_reasons: market_context
                .map_or_else(Vec::new, |c| c.substitution_reasons.clone()),
        });
    }
    acc.realized_pnl += tick_pnl;
    acc.pnl_curve.push(PnlCurvePoint {
        decision_at: tick.decision_at,
        cumulative_realized_pnl_usd: acc.realized_pnl.round_dp(RESEARCH_DECIMAL_SCALE),
    });
    // A tick with no allocation contributes no return observation (never a
    // silent zero-return sample, which would bias the Sharpe ratio's
    // variance downward).
    if tick_allocated > Decimal::ZERO {
        acc.tick_returns.push(tick_pnl / tick_allocated);
    }
    Ok(())
}

fn allocate_tick(
    output: &ModelRuntimeOutput,
    allocator: &dyn PortfolioAllocator,
    caps: &PortfolioCaps,
    meta: &BTreeMap<&str, &BacktestMarketMeta>,
) -> QuantResult<AllocationOutput> {
    let candidate_metas = backtest_candidate_metas(output, meta, caps);
    // Per-tick independent allocation (no inventory carry): `initial_exposures`
    // is always empty, correlation clustering is off, and every candidate is
    // eligible. Cross-tick netting is the production planner's job.
    let top_n = candidate_metas.len();
    allocator.allocate(&AllocationInput {
        candidates: candidate_metas,
        caps,
        initial_exposures: &ExposureBreakdown::default(),
        available_usd: caps.total_budget_usd,
        // Historical replay has no venue account. The governed test budget is
        // both available capital and capital base.
        capital_base_usd: caps.total_budget_usd,
        correlation: None,
        top_n,
    })
}

fn record_missing_input_count(
    acc: &mut RunAccumulator,
    output: &ModelRuntimeOutput,
) -> QuantResult<()> {
    let missing_input_count = output
        .input_audit
        .iter()
        .filter(|row| {
            matches!(
                row.raw_state,
                ModelInputAuditState::Missing | ModelInputAuditState::MissingInput
            )
        })
        .count();
    acc.missing_feature_count = acc
        .missing_feature_count
        .checked_add(u64::try_from(missing_input_count).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("missing model-input count does not fit u64: {error}"),
            }
        })?)
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: "missing model-input count overflowed u64".to_owned(),
        })?;
    Ok(())
}

/// Build the allocator metas for one tick: every candidate desires the
/// single-recommendation cap (flat sizing, preserving the original behavior).
fn backtest_candidate_metas<'a>(
    output: &'a ModelRuntimeOutput,
    meta: &BTreeMap<&str, &'a BacktestMarketMeta>,
    caps: &PortfolioCaps,
) -> Vec<CandidateMeta<'a>> {
    let desired = Usd::new(caps.max_single_recommendation_usd);
    output
        .candidates
        .iter()
        .filter_map(|candidate| {
            let market = meta.get(candidate.market_id.as_str())?;
            Some(CandidateMeta {
                candidate,
                desired_usd: desired,
                category: market.category,
                event_id: market.event_id.clone(),
                liquidity_usd: market.liquidity_usd,
            })
        })
        .collect()
}

/// Per-market allocation weights for one tick (normalized to the tick total).
fn weights_for_tick(allocations: &[Allocation]) -> BTreeMap<String, Decimal> {
    let total: Decimal = allocations.iter().map(|a| a.allocated_usd.inner()).sum();
    let mut weights = BTreeMap::new();
    if total > Decimal::ZERO {
        for allocation in allocations {
            let amount = allocation.allocated_usd.inner();
            if amount > Decimal::ZERO {
                *weights
                    .entry(allocation.market_id.as_str().to_owned())
                    .or_insert(Decimal::ZERO) += amount / total;
            }
        }
    }
    weights
}

/// Aggregated inputs for report assembly.
struct BuildMetrics<'a> {
    samples: &'a [SampleOutcome],
    pnl_curve: &'a [PnlCurvePoint],
    tick_weights: &'a [BTreeMap<String, Decimal>],
    tick_returns: &'a [Decimal],
    missing_feature_count: u64,
    total_emitted: u64,
    total_allocated: Decimal,
    realized_pnl: Decimal,
    budget: Decimal,
}

/// Assemble the metrics + canonical report hash.
fn build_report(request: &BacktestRequest, m: &BuildMetrics<'_>) -> QuantResult<BacktestReport> {
    let sample_count = m.samples.len() as u64;
    let coverage = if m.total_emitted > 0 {
        (Decimal::from(sample_count) / Decimal::from(m.total_emitted))
            .round_dp(RESEARCH_DECIMAL_SCALE)
    } else {
        Decimal::ZERO
    };
    let rank_ic = metrics::rank_ic(m.samples);
    let sharpe = metrics::sharpe_ratio(m.tick_returns, Decimal::ONE);
    let hit_rate = metrics::hit_rate(m.samples);
    let expected_vs_realized = metrics::expected_vs_realized(m.samples);
    let max_drawdown = metrics::max_drawdown(m.pnl_curve, m.budget);
    let turnover = metrics::turnover(m.tick_weights);
    let liquidity_feasibility = metrics::liquidity_feasibility(m.samples);
    let category_breakdown = metrics::category_breakdown(m.samples);
    let tail_loss = metrics::tail_loss(m.samples, tail_quantile())?;

    let gross_return = if m.total_allocated > Decimal::ZERO {
        (m.realized_pnl / m.total_allocated).round_dp(RESEARCH_DECIMAL_SCALE)
    } else {
        Decimal::ZERO
    };
    let report_pnl_simulation = PnlSimulation {
        total_allocated_usd: m.total_allocated.round_dp(RESEARCH_DECIMAL_SCALE),
        realized_pnl_usd: m.realized_pnl.round_dp(RESEARCH_DECIMAL_SCALE),
        gross_return,
        pnl_curve: m.pnl_curve.to_vec(),
    };

    let report_hash = hash_report(&ReportHashInput {
        backtest_report_id: &request.backtest_report_id,
        model_version_id: &request.model_version_id,
        runtime_config_version_id: &request.runtime_config_version_id,
        window_start: request.window_start,
        window_end: request.window_end,
        coverage,
        sample_count,
        missing_feature_count: m.missing_feature_count,
        rank_ic,
        sharpe,
        hit_rate,
        expected_vs_realized: &expected_vs_realized,
        max_drawdown,
        turnover,
        liquidity_feasibility,
        category_breakdown: &category_breakdown,
        tail_loss,
        report_pnl_simulation: &report_pnl_simulation,
    })?;

    Ok(BacktestReport {
        backtest_report_id: request.backtest_report_id.clone(),
        model_version_id: request.model_version_id.clone(),
        runtime_config_version_id: request.runtime_config_version_id.clone(),
        window_start: request.window_start,
        window_end: request.window_end,
        coverage,
        sample_count,
        missing_feature_count: m.missing_feature_count,
        rank_ic,
        sharpe,
        hit_rate,
        expected_vs_realized,
        max_drawdown,
        turnover,
        liquidity_feasibility,
        category_breakdown,
        tail_loss,
        report_pnl_simulation,
        report_hash,
    })
}

/// Canonical-hash projection of every report field except the hash itself.
#[derive(Serialize)]
struct ReportHashInput<'a> {
    backtest_report_id: &'a BacktestReportId,
    model_version_id: &'a ModelVersionId,
    runtime_config_version_id: &'a RuntimeConfigVersionId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    coverage: Decimal,
    sample_count: u64,
    missing_feature_count: u64,
    rank_ic: Decimal,
    sharpe: Decimal,
    hit_rate: Probability,
    expected_vs_realized: &'a ExpectedVsRealized,
    max_drawdown: Decimal,
    turnover: Decimal,
    liquidity_feasibility: Probability,
    category_breakdown: &'a [CategoryMetric],
    tail_loss: Decimal,
    report_pnl_simulation: &'a PnlSimulation,
}

/// Canonical blake3 hash of the report content.
fn hash_report(input: &ReportHashInput<'_>) -> QuantResult<ContentHash> {
    ResearchHasher::canonical(input)
}

#[cfg(test)]
mod tests {
    use super::PortfolioReplayBacktester;
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        enums::{
            common::MarketCategory,
            factor::FactorFamily,
            quant::{DataQualityStatus, FactorDirection},
        },
        runtime_config::FactorCrossSectionConfig,
        types::{
            BacktestReportId, ContentHash, FactorDefinitionId, MarketId, ModelInputContract,
            ModelRunId, ModelVersionId, Price, Probability, RuntimeConfigVersionId, TokenId, Usd,
            builtin_research_profiles,
        },
    };
    use rust_decimal_macros::dec;

    use crate::{
        backtest::{
            BacktestInputs, BacktestMarketMeta, BacktestRequest, BacktestTick, Backtester,
            MarketOutcome, PortfolioCaps,
        },
        factors::{
            FactorExplanation, FactorValue, FrozenReferenceQuantiles, NormalizedFactor,
            names::MOMENTUM_ROC,
        },
        model::{
            ReturnModelSpec,
            artifact::{
                FactorWeight, ModelArtifactHeader, ScoreMultiplierSpec,
                SubstitutionConfidenceRules, WeightedFactorModelArtifact,
                model_input_contract_hash,
            },
            runtime::{
                FactorInferenceRow, FactorInferenceTable, MarketInferenceContext, ModelFamily,
                ModelRuntimeInput,
            },
            weighted::WeightedFactorRuntime,
        },
    };

    fn hash(seed: &str) -> ContentHash {
        ContentHash::parse(format!("blake3:{seed:0>64}")).expect("hash")
    }

    fn runtime() -> WeightedFactorRuntime {
        let input_contract = ModelInputContract::single_required("book.mid");
        let input_contract_hash =
            model_input_contract_hash(&input_contract).expect("input contract hash");
        WeightedFactorRuntime::new(
            WeightedFactorModelArtifact {
                header: ModelArtifactHeader {
                    model_version_id: ModelVersionId::from_v7(),
                    profile_ref: builtin_research_profiles()
                        .expect("built-in profiles")
                        .remove(0)
                        .profile_ref,
                    model_family: ModelFamily::WeightedFactor,
                    feature_schema_hash: hash("aa"),
                    factor_schema_hash: hash("bb"),
                    trade_policy_artifact_id: None,
                    trade_policy_hash: None,
                },
                training_dataset_hash: hash("cc"),
                training_input_hash: hash("dd"),
                input_contract,
                input_contract_hash,
                weights: vec![FactorWeight {
                    factor: MOMENTUM_ROC,
                    weight: dec!(1),
                }],
                prediction_horizon_secs: 86_400,
                multipliers: ScoreMultiplierSpec::conservative(),
                substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
                return_model: ReturnModelSpec::heuristic_default(),
                factor_cross_section: FactorCrossSectionConfig::default(),
                frozen_reference_quantiles: FrozenReferenceQuantiles::empty(),
                objective_report: None,
                category_scope: None,
            },
            None,
            None,
        )
        .expect("runtime")
    }

    fn row(market: &str, bullish: bool) -> FactorInferenceRow {
        let direction = if bullish {
            FactorDirection::Positive
        } else {
            FactorDirection::Negative
        };
        FactorInferenceRow {
            market_id: MarketId::new(market),
            token_id: TokenId::new("yes"),
            factors: vec![FactorValue {
                definition_id: FactorDefinitionId::from_v7(),
                name: MOMENTUM_ROC,
                family: FactorFamily::Momentum,
                raw_value: Some(dec!(1)),
                normalization: NormalizedFactor::cross_section(Probability::new(dec!(0.9))),
                direction,
                confidence: Probability::new(dec!(1)),
                explanation: FactorExplanation {
                    headline: "t".to_owned(),
                    drivers: Vec::new(),
                },
                input_feature_refs: Vec::new(),
            }],
            context: MarketInferenceContext {
                secondary_token_id: Some(TokenId::new("no")),
                yes_price: Price::new(dec!(0.5)),
                no_price: Some(Price::new(dec!(0.52))),
                liquidity_usd: Some(Usd::new(dec!(50000))),
                data_quality: DataQualityStatus::Fresh,
                time_to_resolution_secs: Some(86_400),
                substitution_reasons: Vec::new(),
            },
        }
    }

    fn tick(idx: i64, model_run_id: &ModelRunId) -> BacktestTick {
        let as_of = Utc.timestamp_opt(1_700_000_000 + idx * 3600, 0).unwrap();
        // Bullish market settles YES (correct), bearish settles NO (correct).
        let meta = vec![
            BacktestMarketMeta {
                market_id: MarketId::new("0xbull"),
                category: MarketCategory::Crypto,
                event_id: None,
                liquidity_usd: Some(Usd::new(dec!(50000))),
            },
            BacktestMarketMeta {
                market_id: MarketId::new("0xbear"),
                category: MarketCategory::Sports,
                event_id: None,
                liquidity_usd: Some(Usd::new(dec!(50000))),
            },
        ];
        BacktestTick {
            decision_at: as_of,
            model_input: ModelRuntimeInput::FactorTable(FactorInferenceTable {
                model_run_id: model_run_id.clone(),
                decision_at: as_of,
                rows: vec![row("0xbull", true), row("0xbear", false)],
            }),
            outcomes: vec![
                MarketOutcome {
                    market_id: MarketId::new("0xbull"),
                    settled_yes: true,
                    matured: true,
                    max_adverse_excursion_bps: None,
                },
                MarketOutcome {
                    market_id: MarketId::new("0xbear"),
                    settled_yes: false,
                    matured: true,
                    max_adverse_excursion_bps: None,
                },
            ],
            market_meta: meta,
        }
    }

    fn caps() -> PortfolioCaps {
        PortfolioCaps {
            total_budget_usd: dec!(1000),
            max_single_recommendation_usd: dec!(200),
            min_recommendation_usd: dec!(10),
            max_market_exposure_usd: dec!(0),
            max_event_exposure_usd: dec!(0),
            max_category_exposure_usd: dec!(0),
            liquidity_usage_cap_pct: dec!(0.1),
            max_aggregate_exposure_pct: dec!(0),
        }
    }

    fn request() -> BacktestRequest {
        BacktestRequest {
            backtest_report_id: BacktestReportId::from_v7(),
            model_version_id: ModelVersionId::from_v7(),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            window_start: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            window_end: Utc.timestamp_opt(1_700_100_000, 0).unwrap(),
        }
    }

    #[tokio::test]
    async fn backtest_report_metrics_complete() {
        let model = runtime();
        let run_id = ModelRunId::from_v7();
        let ticks = vec![tick(0, &run_id), tick(1, &run_id)];
        let result = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: request(),
                model: &model,
                ticks,
                caps: caps(),
            })
            .await
            .expect("backtest");

        let report = &result.report;
        // Every market matured ⇒ full coverage, all candidates resolved.
        assert!(report.sample_count > 0, "resolved samples produced");
        assert_eq!(report.coverage, dec!(1), "all emitted candidates matured");
        // Correct directional calls ⇒ positive hit rate + rank IC.
        assert!(report.hit_rate.inner() > dec!(0.5), "mostly correct calls");
        assert!(
            !report.category_breakdown.is_empty(),
            "category breakdown filled"
        );
        assert!(
            report.report_pnl_simulation.total_allocated_usd > dec!(0),
            "capital allocated"
        );
        assert!(
            !report.report_pnl_simulation.pnl_curve.is_empty(),
            "PnL curve filled"
        );
        assert!(
            report.report_hash.as_str().starts_with("blake3:"),
            "canonical report hash"
        );
    }

    /// The same inputs must produce a byte-identical report hash (the report id /
    /// version are fixed here), proving deterministic replay.
    #[tokio::test]
    async fn backtest_report_hash_is_deterministic() {
        let model = runtime();
        let run_id = ModelRunId::from_v7();
        let req = request();
        let ticks_a = vec![tick(0, &run_id), tick(1, &run_id)];
        let ticks_b = vec![tick(0, &run_id), tick(1, &run_id)];
        let a = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: req.clone(),
                model: &model,
                ticks: ticks_a,
                caps: caps(),
            })
            .await
            .expect("a");
        let b = PortfolioReplayBacktester::new()
            .run(BacktestInputs {
                request: req,
                model: &model,
                ticks: ticks_b,
                caps: caps(),
            })
            .await
            .expect("b");
        assert_eq!(a.report.report_hash, b.report.report_hash);
    }
}
