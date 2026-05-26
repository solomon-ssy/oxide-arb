use std::sync::{Arc, OnceLock};

use chrono::{Duration, Utc};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use oxide_arb_algorithm::{
    calibration::ResolutionCalibrator,
    cooldown::InMemoryEmissionCooldown,
    endgame::{EndgameDetector, convergence::ConvergenceDirection},
    fee::FeeEstimator,
    pipeline::OpportunityPipeline,
    scorer::EndgameScorer,
    walker::OrderbookWalker,
};
use oxide_arb_core::observability::metrics_hub::MetricsHub;
use oxide_arb_core::pipeline::order_book::OrderBook;
use oxide_arb_models::{
    config::{
        CalibrationConfig, EmissionCooldownConfig, EndgameDetectionConfig, FillProbabilityConfig,
        RiskConfig, ScorerConfig,
    },
    domain::{
        book::{BookLevel, BookSnapshot, EndgameBookPair},
        calibration::BucketKey,
        opportunity::{EndgameMeta, Opportunity},
    },
    enums::calibration::{DurationBucket, PriceZone},
    enums::common::{MarketCategory, Side, StalenessLevel},
    enums::opportunity::PayoutModel,
    types::{
        Bps, EventId, MarketId, MicroPrice, MicroUsd, OpportunityId, Price, Shares, TokenId, Usd,
    },
};
use oxide_arb_risk::{
    builder::RiskEngineBuilder, clock::utc_clock, traits::RiskMetrics, types::ReportMode,
};
use rust_decimal_macros::dec;

struct ZeroFeeEstimator;

impl FeeEstimator for ZeroFeeEstimator {
    fn estimate_fee(
        &self,
        _shares: Shares,
        _price: Price,
        _category: MarketCategory,
        _token_id: &TokenId,
    ) -> Usd {
        Usd::ZERO
    }
}

fn sample_levels(n: usize) -> Vec<BookLevel> {
    (0..n)
        .map(|i| {
            BookLevel::from_decimal_unchecked(
                Price::new(dec!(0.95) + dec!(0.0001) * rust_decimal::Decimal::from(i)),
                Shares::new(dec!(100)),
            )
        })
        .collect()
}

fn make_endgame_book() -> EndgameBookPair {
    let yes_asks = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.97)),
        Shares::new(dec!(1000)),
    )];
    let no_bids = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.02)),
        Shares::new(dec!(1000)),
    )];
    let no_asks = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.03)),
        Shares::new(dec!(1000)),
    )];
    EndgameBookPair {
        yes: Arc::new(BookSnapshot::new(
            Arc::from([] as [BookLevel; 0]),
            Arc::from(yes_asks),
            0,
        )),
        no: Arc::new(BookSnapshot::new(Arc::from(no_bids), Arc::from(no_asks), 0)),
    }
}

fn make_detector() -> EndgameDetector<ZeroFeeEstimator> {
    let cal_config = CalibrationConfig::default();
    let calibrator = Arc::new(ResolutionCalibrator::empty(cal_config.clone()));
    let config = EndgameDetectionConfig {
        min_convergence_duration_secs: 0,
        ..Default::default()
    };
    EndgameDetector::new(&config, &cal_config, calibrator, ZeroFeeEstimator)
}

fn make_pipeline() -> OpportunityPipeline<ZeroFeeEstimator> {
    let detector = make_detector();
    let scorer_config = ScorerConfig::default();
    let scorer = EndgameScorer::new(scorer_config.clone(), &FillProbabilityConfig::default(), 24);
    let cooldown = InMemoryEmissionCooldown::new(&EmissionCooldownConfig::default());
    OpportunityPipeline::new(detector, scorer, cooldown, dec!(0.01), &scorer_config)
}

struct BenchMetrics;

impl RiskMetrics for BenchMetrics {
    fn total_exposure(&self) -> Usd {
        Usd::new(dec!(100))
    }
    fn market_exposure(&self, _: &MarketId) -> Usd {
        Usd::ZERO
    }
    fn open_position_count(&self) -> usize {
        0
    }
    fn open_positions(&self) -> Vec<oxide_arb_models::domain::position::PositionInfo> {
        Vec::new()
    }
    fn cached_balance(&self) -> Usd {
        Usd::new(dec!(5000))
    }
    fn active_reservation_count(&self) -> usize {
        0
    }
    fn reserved_usd(&self) -> Usd {
        Usd::ZERO
    }
    fn open_directional_count(&self, _: oxide_arb_models::enums::common::Side) -> usize {
        0
    }
    fn daily_directional_trades(&self, _: oxide_arb_models::enums::common::Side) -> u32 {
        0
    }
    fn consecutive_market_misses(&self, _: &MarketId) -> u32 {
        0
    }
    fn ws_disconnect_secs(&self) -> u64 {
        0
    }
    fn api_error_count(&self) -> u64 {
        0
    }
    fn api_request_count(&self) -> u64 {
        0
    }
}

fn bench_opportunity() -> Opportunity {
    Opportunity {
        opportunity_id: OpportunityId::new_v7(),
        market_id: MarketId::new("bench_market"),
        event_id: EventId::new("bench_event"),
        token_id: TokenId::new("12345"),
        side: Side::Buy,
        payout_model: PayoutModel::DirectionalSettlement {
            projected_payout_if_correct: Usd::new(dec!(100)),
            expected_payout: Usd::new(dec!(95)),
            predicted_side: Side::Buy,
        },
        shares: Shares::new(dec!(100)),
        entry_price: Price::new(dec!(0.92)),
        total_cost: Usd::new(dec!(20)),
        total_fees: Usd::new(dec!(0.40)),
        net_profit: Usd::new(dec!(5)),
        expected_net_profit: Usd::new(dec!(4.5)),
        edge_bps: Bps::new(dec!(300)),
        resolution_adjust: dec!(0.95),
        depth_used_pct: dec!(10),
        staleness: StalenessLevel::Fresh,
        category: MarketCategory::Politics,
        meta: EndgameMeta {
            predicted_yes: true,
            confidence: dec!(0.95),
            convergence_duration_secs: 600,
            price_zone: PriceZone::Z97,
            duration_bucket: DurationBucket::Medium,
            settlement_deadline: None,
        },
        calibration: oxide_arb_models::domain::calibration::CalibrationSnapshot {
            bucket_key: BucketKey {
                category: MarketCategory::Politics,
                price_zone: PriceZone::Z97,
                duration_bucket: DurationBucket::Medium,
            },
            posterior_mean: dec!(0.93),
            sample_size: 50,
            alpha_prior: dec!(2.0),
            beta_prior: dec!(1.0),
            fallback_tier: 1,
            fused_probability: dec!(0.95),
        },
        detected_at: Utc::now(),
    }
}

fn risk_engine() -> &'static oxide_arb_risk::engine::RiskEngine {
    static ENGINE: OnceLock<oxide_arb_risk::engine::RiskEngine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async {
            let config = RiskConfig {
                max_total_exposure_usd: dec!(5000),
                max_single_market_exposure_usd: dec!(500),
                max_single_bet_usd: dec!(25),
                max_open_positions: 5,
                max_daily_loss_usd: dec!(75),
                max_weekly_loss_usd: dec!(120),
                daily_budget_usd: dec!(200),
                min_balance_usd: dec!(50),
                reserve_balance_usd: dec!(100),
                min_trade_usd: dec!(1),
                max_consecutive_misses: 3,
                ..RiskConfig::default()
            };
            RiskEngineBuilder::new()
                .config(config)
                .clock(utc_clock())
                .initial_equity(Usd::new(dec!(5000)))
                .build(&BenchMetrics)
                .await
                .expect("engine build")
        })
    })
}

fn bench_walker_micro(c: &mut Criterion, name: &str, n: usize) {
    let levels = sample_levels(n);
    let depth = oxide_arb_models::domain::book::total_depth_usd(&levels);
    let budget = MicroUsd::try_from_decimal(dec!(500)).unwrap();
    let floor = MicroPrice::try_from_decimal(dec!(0.95)).unwrap();
    c.bench_function(name, |b| {
        b.iter(|| OrderbookWalker::walk_asks_by_cost(black_box(&levels), budget, floor, depth));
    });
}

fn bench_walk_micro_5(c: &mut Criterion) {
    bench_walker_micro(c, "walk_micro_5", 5);
}

fn bench_walk_micro_50(c: &mut Criterion) {
    bench_walker_micro(c, "walk_micro_50", 50);
}

fn bench_walk_micro_200(c: &mut Criterion) {
    bench_walker_micro(c, "walk_micro_200", 200);
}

fn bench_detect_with_direction(c: &mut Criterion) {
    let detector = make_detector();
    let book = make_endgame_book();
    let market_id = MarketId::new("m1");
    let event_id = EventId::new("e1");
    let token_yes = TokenId::new("yes-1");
    let token_no = TokenId::new("no-1");
    let now = Utc::now();
    let deadline = now + Duration::hours(12);
    let direction = ConvergenceDirection::YesLikely;

    c.bench_function("detect_with_direction", |b| {
        b.iter(|| {
            detector.detect_with_direction(
                black_box(&market_id),
                black_box(&event_id),
                black_box(&token_yes),
                black_box(&token_no),
                black_box(&book),
                direction,
                MarketCategory::Geopolitics,
                StalenessLevel::Fresh,
                Some(deadline),
                now,
            )
        });
    });
}

fn bench_pipeline_process(c: &mut Criterion) {
    let pipeline = make_pipeline();
    let book = make_endgame_book();
    let market_id = MarketId::new("m1");
    let event_id = EventId::new("e1");
    let token_yes = TokenId::new("yes-1");
    let token_no = TokenId::new("no-1");
    let now = Utc::now();
    let deadline = now + Duration::hours(12);

    c.bench_function("pipeline_process", |b| {
        b.iter(|| {
            pipeline.process(
                black_box(&market_id),
                black_box(&event_id),
                black_box(&token_yes),
                black_box(&token_no),
                black_box(&book),
                MarketCategory::Geopolitics,
                StalenessLevel::Fresh,
                Some(deadline),
                now,
            )
        });
    });
}

fn bench_pre_trade_pass(c: &mut Criterion) {
    let engine = risk_engine();
    let opp = Arc::new(bench_opportunity());
    let probability = oxide_arb_models::domain::risk::ProbabilityInput {
        calibrated_win_prob: dec!(0.99),
        fill_prob: dec!(0.99),
        calibration_confidence: dec!(0.99),
        sample_size: 100,
        model_staleness_secs: 0,
        expected_slippage_pct: dec!(0.001),
        expected_failure_cost_pct: dec!(0.001),
    };
    let metrics = BenchMetrics;

    c.bench_function("pre_trade_pass", |b| {
        b.iter(|| {
            engine.pre_trade_check_core(
                black_box(&opp),
                black_box(&probability),
                black_box(&metrics),
                ReportMode::ShortCircuit,
            )
        });
    });
}

fn risk_engine_halted() -> &'static oxide_arb_risk::engine::RiskEngine {
    static ENGINE: OnceLock<oxide_arb_risk::engine::RiskEngine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async {
            let config = RiskConfig {
                max_total_exposure_usd: dec!(5000),
                max_single_market_exposure_usd: dec!(500),
                max_single_bet_usd: dec!(25),
                max_open_positions: 5,
                max_daily_loss_usd: dec!(75),
                max_weekly_loss_usd: dec!(120),
                daily_budget_usd: dec!(200),
                min_balance_usd: dec!(50),
                reserve_balance_usd: dec!(100),
                min_trade_usd: dec!(1),
                max_consecutive_misses: 3,
                ..RiskConfig::default()
            };
            let engine = RiskEngineBuilder::new()
                .config(config)
                .clock(utc_clock())
                .initial_equity(Usd::new(dec!(5000)))
                .build(&BenchMetrics)
                .await
                .expect("engine build");
            engine.halt("bench halted engine".into()).await;
            engine
        })
    })
}

fn bench_pre_trade_fail_short(c: &mut Criterion) {
    let engine = risk_engine_halted();
    let opp = Arc::new(bench_opportunity());
    let probability = oxide_arb_models::domain::risk::ProbabilityInput {
        calibrated_win_prob: dec!(0.99),
        fill_prob: dec!(0.99),
        calibration_confidence: dec!(0.99),
        sample_size: 100,
        model_staleness_secs: 0,
        expected_slippage_pct: dec!(0.001),
        expected_failure_cost_pct: dec!(0.001),
    };
    let metrics = BenchMetrics;

    c.bench_function("pre_trade_fail_short", |b| {
        b.iter(|| {
            engine.pre_trade_check_core(
                black_box(&opp),
                black_box(&probability),
                black_box(&metrics),
                ReportMode::ShortCircuit,
            )
        });
    });
}

fn bench_book_apply_snapshot(c: &mut Criterion) {
    let levels = sample_levels(50);
    c.bench_function("book_apply_snapshot_50", |b| {
        b.iter(|| {
            let mut ob = OrderBook::new(TokenId::new("t"));
            ob.apply_snapshot(levels.clone(), levels.clone(), 1);
            black_box(ob.publish());
        });
    });
}

fn bench_dual_book_assemble(c: &mut Criterion) {
    use oxide_arb_core::pipeline::{book_store::BookStore, dual_book_assembler::DualBookAssembler};
    let metrics = Arc::new(MetricsHub::new());
    let store = BookStore::new(Arc::clone(&metrics));
    let yes = TokenId::new("yes");
    let no = TokenId::new("no");
    let levels = sample_levels(50);
    store.apply_snapshot(&yes, levels.clone(), levels, 1);
    let levels = sample_levels(50);
    store.apply_snapshot(&no, levels.clone(), levels, 1);

    c.bench_function("dual_book_assemble_50_levels", |b| {
        b.iter(|| DualBookAssembler::assemble(black_box(&store), &yes, &no));
    });
}

criterion_group!(
    benches,
    bench_walk_micro_5,
    bench_walk_micro_50,
    bench_walk_micro_200,
    bench_detect_with_direction,
    bench_pipeline_process,
    bench_pre_trade_pass,
    bench_pre_trade_fail_short,
    bench_book_apply_snapshot,
    bench_dual_book_assemble
);
criterion_main!(benches);
