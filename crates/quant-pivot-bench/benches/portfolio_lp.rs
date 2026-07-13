//! Phase 05.8 portfolio LP spike: 100-candidate MILP vs relaxation throughput.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use quant_pivot_models::{
    enums::{common::MarketCategory, quant::OutcomeSide},
    types::{
        EventId, ExposureBreakdown, MarketId, ModelRunId, Price, Probability, SignalCandidateId,
        TokenId, Usd,
    },
};
use quant_pivot_research::{
    backtest::PortfolioCaps,
    model::signal::{ModelExplanation, SignalCandidate},
    portfolio::{
        AllocationInput, CandidateMeta, LinearProgrammingPortfolioAllocator, OptimizerConfig,
        PortfolioAllocator,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn candidate(index: usize, composite: Decimal) -> SignalCandidate {
    SignalCandidate {
        signal_candidate_id: SignalCandidateId::from_v7(),
        model_run_id: ModelRunId::from_v7(),
        market_id: MarketId::new(format!("0x{index:040x}")),
        token_id: TokenId::new("yes"),
        outcome_side: OutcomeSide::Yes,
        composite_score: Probability::new(composite),
        confidence: Probability::new(dec!(0.9)),
        expected_return_bps: dec!(100),
        downside_bps: dec!(100),
        win_probability: None,
        entry_price_ref: Price::new(dec!(0.5)),
        suggested_horizon_secs: 3_600,
        factor_breakdown: Vec::new(),
        model_explanation: ModelExplanation {
            headline: "bench".to_owned(),
            top_positive: Vec::new(),
            top_negative: Vec::new(),
        },
        rejection_warnings: Vec::new(),
        rank_before_portfolio: 0,
        liquidity_score: Probability::ZERO,
        data_quality_score: Probability::ZERO,
        model_score_percentile: Probability::ZERO,
        decision_at: chrono::Utc::now(),
    }
}

fn bench_input(candidate_count: usize) -> AllocationInput<'static> {
    let caps = Box::leak(Box::new(PortfolioCaps {
        total_budget_usd: dec!(50000),
        max_single_recommendation_usd: dec!(2000),
        min_recommendation_usd: dec!(10),
        max_market_exposure_usd: dec!(5000),
        max_event_exposure_usd: dec!(10000),
        max_category_exposure_usd: dec!(25000),
        liquidity_usage_cap_pct: dec!(0.25),
        max_aggregate_exposure_pct: dec!(0),
    }));
    let mut candidates = Vec::with_capacity(candidate_count);
    for index in 0..candidate_count {
        let composite = Decimal::new(50 + i64::try_from(index % 50).expect("index"), 2);
        let owned = candidate(index, composite);
        let leaked = Box::leak(Box::new(owned));
        candidates.push(CandidateMeta {
            candidate: leaked,
            desired_usd: Usd::new(dec!(1500)),
            category: if index % 2 == 0 {
                MarketCategory::Crypto
            } else {
                MarketCategory::Sports
            },
            event_id: Some(EventId::new(format!("evt-{}", index % 10))),
            liquidity_usd: Some(Usd::new(dec!(10000))),
        });
    }
    AllocationInput {
        candidates,
        caps,
        initial_exposures: Box::leak(Box::new(ExposureBreakdown::default())),
        available_usd: caps.total_budget_usd,
        capital_base_usd: caps.total_budget_usd,
        correlation: None,
        top_n: 20,
    }
}

fn bench_portfolio_lp(c: &mut Criterion) {
    let mut group = c.benchmark_group("portfolio_lp_100_candidates");
    for mode in [("milp", true), ("relaxation", false)] {
        let (label, integer_inclusion) = mode;
        let allocator = LinearProgrammingPortfolioAllocator::new(OptimizerConfig {
            solver: quant_pivot_models::enums::quant::PortfolioSolverKind::Microlp,
            integer_inclusion,
            lambda: Decimal::ZERO,
        });
        let input = bench_input(100);
        group.bench_with_input(BenchmarkId::new(label, 100), &allocator, |b, allocator| {
            b.iter(|| {
                allocator
                    .allocate(&input)
                    .expect("portfolio allocation must succeed");
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_portfolio_lp);
criterion_main!(benches);
