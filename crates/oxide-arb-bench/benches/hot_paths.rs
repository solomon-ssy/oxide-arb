use chrono::{Duration, Utc};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use num_traits::ToPrimitive;
use oxide_arb_algorithm::{
    calibration::ResolutionCalibrator,
    cooldown::InMemoryEmissionCooldown,
    endgame::{EndgameDetectInput, EndgameDetector, convergence::ConvergenceDirection},
    fee::FeeEstimator,
    pipeline::{MarketScanInputRef, OpportunityPipeline},
    scorer::{EndgameScorer, ScoredOpportunity},
    walker::OrderbookWalker,
};
use oxide_arb_api::{
    fees::FeeCalculator,
    ws::{ClobWsManager, normalize::normalize_ws_message},
};
use oxide_arb_core::{
    bridge::{CoreFeeEstimator, CoreOpportunityPipeline, risk_metrics::CoreRiskMetrics},
    detection::{coalescer::Coalescer, funnel::Funnel, scanner::Scanner},
    execution::{
        capital_manager::CapitalManager,
        dispatcher::Dispatcher,
        execution_pipeline::{ExecutionPipeline, ExecutionPipelineDeps},
        fsm::ExecutionFSM,
        market_inflight::MarketInFlightRegistry,
        plan_builder::PlanBuilder,
        tiered_strategy::OrderStrategy,
        validator::Validator,
    },
    exposure::in_memory::InMemoryExposureReservation,
    observability::{backpressure::BackpressurePolicy, metrics_hub::MetricsHub},
    outbox::in_memory::InMemoryEventStore,
    pipeline::{
        book_store::BookStore,
        dual_book_assembler::DualBookAssembler,
        market_cache::{CachedMarketScanEntry, MarketCache},
        market_registry::MarketRegistry,
        order_book::OrderBook,
        staleness_classifier::StalenessClassifier,
    },
    service::risk_metrics::{ApiHealthTracker, RiskMetricsState},
};
use oxide_arb_models::{
    config::{
        CalibrationConfig, EmissionCooldownConfig, EndgameDetectionConfig,
        ExposureReservationConfig, FillProbabilityConfig, MarketDataConfig, PolymarketConfig,
        RiskConfig, ScorerConfig, WebSocketConfig,
    },
    domain::{
        book::{BookLevel, BookSnapshot, EndgameBookPair},
        calibration::{BucketKey, CalibrationSnapshot},
        latency::LatencyTrace,
        market::MarketRegistryInfo,
        opportunity::{EndgameMeta, Opportunity},
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::{ExecutionMode, MarketCategory, Side, StalenessLevel, TickSize},
        market::MarketStatus,
        opportunity::PayoutModel,
    },
    types::{
        Bps, EventId, MarketId, MicroPrice, MicroProb, MicroScore, MicroUsd, OpportunityId, Price,
        Shares, TokenId, Usd,
    },
};
use oxide_arb_risk::{
    builder::RiskEngineBuilder, clock::utc_clock, traits::RiskMetrics, types::ReportMode,
};
use polymarket_client_sdk_v2::clob::ws::types::response::{BookUpdate, OrderBookLevel, WsMessage};
use polymarket_client_sdk_v2::types::{B256, U256};
use rust_decimal_macros::dec;
use std::{
    sync::{Arc, OnceLock},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

static EXECUTION_RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

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
        yes: Arc::new(BookSnapshot::new(Arc::from([]), Arc::from(yes_asks), 0, 0)),
        no: Arc::new(BookSnapshot::new(
            Arc::from(no_bids),
            Arc::from(no_asks),
            0,
            0,
        )),
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
    OpportunityPipeline::new(
        detector,
        scorer,
        cooldown,
        MicroUsd::try_from_decimal(dec!(0.01)).unwrap(),
        &scorer_config,
    )
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
            .expect("engine build")
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

    let input = EndgameDetectInput {
        market_id: &market_id,
        event_id: &event_id,
        token_yes: &token_yes,
        token_no: &token_no,
        book: &book,
        direction,
        category: MarketCategory::Geopolitics,
        staleness: StalenessLevel::Fresh,
        settlement_deadline: Some(deadline),
    };

    c.bench_function("detect_with_direction", |b| {
        b.iter(|| detector.detect_with_direction(black_box(&input), now));
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

    let scan_input = MarketScanInputRef {
        market_id: &market_id,
        event_id: &event_id,
        token_yes: &token_yes,
        token_no: &token_no,
        book: &book,
        category: MarketCategory::Geopolitics,
        staleness: StalenessLevel::Fresh,
        settlement_deadline: Some(deadline),
        latency: Arc::new(LatencyTrace::default()),
    };

    c.bench_function("pipeline_process", |b| {
        b.iter(|| pipeline.process_ref(black_box(&scan_input), black_box(now)));
    });
}

fn bench_pre_trade_pass(c: &mut Criterion) {
    let engine = risk_engine();
    let opp = bench_opportunity();
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
            .expect("engine build");
        rt.block_on(engine.halt("bench halted engine".into()));
        engine
    })
}

fn bench_pre_trade_fail_short(c: &mut Criterion) {
    let engine = risk_engine_halted();
    let opp = bench_opportunity();
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
    let bids = sample_levels(50);
    let asks = sample_levels(50);
    c.bench_function("book_apply_snapshot_50", |b| {
        b.iter_with_setup(
            || {
                let ob = OrderBook::new(TokenId::new("t"));
                (ob, bids.clone(), asks.clone())
            },
            |(mut ob, bids, asks)| {
                ob.apply_snapshot(bids, asks, 1);
                black_box(ob.publish_cow(1));
            },
        );
    });
}

fn bench_book_apply_delta_50(c: &mut Criterion) {
    let bids = sample_levels(50);
    let asks = sample_levels(50);
    // Hot path: single-level update on an existing bid (no insert / dual-side probe).
    let delta = [(Side::Buy, Price::new(dec!(0.95)), Shares::new(dec!(10)))];

    c.bench_function("book_apply_delta_50", |b| {
        b.iter_with_setup(
            || {
                let mut ob = OrderBook::new(TokenId::new("t"));
                ob.apply_snapshot(bids.clone(), asks.clone(), 1);
                ob
            },
            |mut ob| {
                ob.apply_delta_cow(delta, 2);
                black_box(&ob);
            },
        );
    });
}

fn bench_dual_book_assemble(c: &mut Criterion) {
    let metrics = Arc::new(MetricsHub::new());
    let store = BookStore::new(Arc::clone(&metrics));
    let yes = TokenId::new("yes");
    let no = TokenId::new("no");
    let levels = Arc::from(sample_levels(50).into_boxed_slice());
    store.apply_snapshot(&yes, Arc::clone(&levels), Arc::clone(&levels), 1, None);
    let levels = Arc::from(sample_levels(50).into_boxed_slice());
    store.apply_snapshot(&no, Arc::clone(&levels), levels, 1, None);

    c.bench_function("dual_book_assemble_50_levels", |b| {
        b.iter(|| DualBookAssembler::assemble(black_box(&store), &yes, &no));
    });
}

fn bench_coalescer_pair_flush(c: &mut Criterion) {
    let metrics = Arc::new(MetricsHub::new());
    let registry = Arc::new(MarketRegistry::new());
    registry.register_market(oxide_arb_models::domain::market::MarketRegistryInfo {
        market_id: MarketId::new("bench-m1"),
        event_id: EventId::new("evt"),
        token_yes: TokenId::new("bench-m1-yes"),
        token_no: TokenId::new("bench-m1-no"),
        question: "Q".into(),
        slug: "q".into(),
        category: MarketCategory::Other,
        status: oxide_arb_models::enums::market::MarketStatus::Active,
        neg_risk: false,
        tick_size: oxide_arb_models::enums::common::TickSize::Hundredth,
        tokens: vec![],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: dec!(1),
        volume_24h: Usd::ZERO,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });
    let (tx, _rx) = flume::bounded(8);
    let coalescer = Coalescer::new(
        registry,
        std::time::Duration::from_millis(500),
        tx,
        metrics,
        CancellationToken::new(),
    );
    let yes = TokenId::new("bench-m1-yes");
    let no = TokenId::new("bench-m1-no");

    c.bench_function("coalescer_pair_flush", |b| {
        b.iter(|| {
            coalescer.notify_token_update(black_box(&yes));
            coalescer.notify_token_update(black_box(&no));
        });
    });
}

fn bench_funnel_immediate_dispatch(c: &mut Criterion) {
    let metrics = Arc::new(MetricsHub::new());
    let (tx, _rx) = flume::bounded(256);
    let funnel = Funnel::new(vec![tx], 50, std::time::Duration::from_millis(75), metrics);
    let scored = Arc::new(ScoredOpportunity {
        opportunity: Arc::new(Opportunity {
            opportunity_id: OpportunityId::new_v7(),
            market_id: MarketId::new("bench"),
            event_id: EventId::new("e"),
            token_id: TokenId::new("t"),
            side: Side::Buy,
            payout_model: PayoutModel::DirectionalSettlement {
                projected_payout_if_correct: Usd::ZERO,
                expected_payout: Usd::ZERO,
                predicted_side: Side::Buy,
            },
            shares: Shares::ZERO,
            entry_price: Price::new(dec!(0.95)),
            total_cost: Usd::ZERO,
            total_fees: Usd::ZERO,
            net_profit: Usd::new(dec!(1)),
            expected_net_profit: Usd::new(dec!(1)),
            edge_bps: Bps::ZERO,
            resolution_adjust: dec!(1),
            depth_used_pct: dec!(1),
            staleness: StalenessLevel::Fresh,
            category: MarketCategory::Other,
            meta: EndgameMeta {
                predicted_yes: true,
                confidence: dec!(0.9),
                convergence_duration_secs: 0,
                price_zone: PriceZone::Z95,
                duration_bucket: DurationBucket::Short,
                settlement_deadline: None,
            },
            calibration: oxide_arb_models::domain::calibration::CalibrationSnapshot {
                bucket_key: oxide_arb_models::domain::calibration::BucketKey {
                    category: MarketCategory::Other,
                    price_zone: PriceZone::Z95,
                    duration_bucket: DurationBucket::Short,
                },
                posterior_mean: dec!(0.9),
                sample_size: 10,
                alpha_prior: dec!(1),
                beta_prior: dec!(1),
                fallback_tier: 1,
                fused_probability: dec!(0.9),
            },
            detected_at: Utc::now(),
        }),
        token_yes: TokenId::new("y"),
        token_no: TokenId::new("n"),
        score: MicroScore::try_from_decimal(dec!(0.9)).unwrap(),
        fill_probability: MicroProb::ONE,
        urgency_factor: MicroProb::ONE,
        category_weight: MicroProb::ONE,
        staleness_discount: MicroProb::ONE,
        book_yes_version: 1,
        book_no_version: 1,
        trace: Arc::new(LatencyTrace::default()),
    });

    c.bench_function("funnel_immediate_dispatch", |b| {
        b.iter(|| {
            let _ = black_box(funnel.try_dispatch_immediate(Arc::clone(&scored)));
            black_box(());
        });
    });
}

fn bench_ws_normalize_book_50(c: &mut Criterion) {
    let levels: Vec<OrderBookLevel> = (0..50)
        .map(|i| {
            OrderBookLevel::builder()
                .price(dec!(0.95) + dec!(0.0001) * rust_decimal::Decimal::from(i))
                .size(dec!(100))
                .build()
        })
        .collect();

    let book = BookUpdate::builder()
        .asset_id(U256::from(42_u64))
        .market(B256::ZERO)
        .timestamp(1000)
        .bids(levels.clone())
        .asks(levels)
        .build();

    c.bench_function("ws_normalize_book_50", |b| {
        b.iter(|| {
            black_box(normalize_ws_message(
                WsMessage::Book(book.clone()),
                Instant::now(),
                None,
            ))
        });
    });
}

fn make_core_pipeline() -> CoreOpportunityPipeline {
    let cal_config = CalibrationConfig::default();
    let calibrator = Arc::new(ResolutionCalibrator::empty(cal_config.clone()));
    let config = EndgameDetectionConfig {
        min_convergence_duration_secs: 0,
        ..Default::default()
    };
    let detector = EndgameDetector::new(
        &config,
        &cal_config,
        calibrator,
        CoreFeeEstimator(Arc::new(FeeCalculator::default())),
    );
    let scorer_config = ScorerConfig::default();
    let scorer = EndgameScorer::new(scorer_config.clone(), &FillProbabilityConfig::default(), 24);
    let cooldown = InMemoryEmissionCooldown::new(&EmissionCooldownConfig::default());
    OpportunityPipeline::new(
        detector,
        scorer,
        cooldown,
        MicroUsd::try_from_decimal(dec!(0.01)).unwrap(),
        &scorer_config,
    )
}

fn bench_scanner_scan_market(c: &mut Criterion) {
    let metrics = Arc::new(MetricsHub::new());
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    let registry = Arc::new(MarketRegistry::new());
    registry.register_market(MarketRegistryInfo {
        market_id: MarketId::new("bench-m1"),
        event_id: EventId::new("evt"),
        token_yes: TokenId::new("bench-m1-yes"),
        token_no: TokenId::new("bench-m1-no"),
        question: "Q".into(),
        slug: "q".into(),
        category: MarketCategory::Geopolitics,
        status: MarketStatus::Active,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: dec!(1),
        volume_24h: Usd::ZERO,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });
    let market_cache = Arc::new(MarketCache::new(registry));
    let yes = TokenId::new("bench-m1-yes");
    let no = TokenId::new("bench-m1-no");
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
    let now_ms = ToPrimitive::to_u64(&Utc::now().timestamp_millis().max(0)).unwrap_or(0);
    book_store.apply_snapshot(&yes, vec![], yes_asks, now_ms, None);
    book_store.apply_snapshot(&no, no_bids, no_asks, now_ms, None);

    let pipeline = Arc::new(make_core_pipeline());
    let scanner = Scanner::new(
        pipeline,
        book_store,
        market_cache,
        StalenessClassifier::new(&MarketDataConfig::default()),
        metrics,
    );
    let entry = CachedMarketScanEntry {
        market_id: MarketId::new("bench-m1"),
        event_id: EventId::new("evt"),
        token_yes: yes,
        token_no: no,
        category: MarketCategory::Geopolitics,
        tick_size: TickSize::Hundredth,
        neg_risk: false,
        settlement_deadline: Some(Utc::now() + Duration::hours(12)),
    };
    let now = Utc::now();

    c.bench_function("scanner_scan_market", |b| {
        b.iter(|| black_box(scanner.scan_market(black_box(&entry), now)));
    });
}

fn execution_bench_scored() -> ScoredOpportunity {
    ScoredOpportunity {
        opportunity: Arc::new(Opportunity {
            opportunity_id: OpportunityId::new_v7(),
            market_id: MarketId::new("bench-exec"),
            event_id: EventId::new("e"),
            token_id: TokenId::new("bench-m1-yes"),
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
            calibration: CalibrationSnapshot {
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
                fused_probability: dec!(0.99),
            },
            detected_at: Utc::now(),
        }),
        token_yes: TokenId::new("bench-m1-yes"),
        token_no: TokenId::new("bench-m1-no"),
        score: MicroScore::try_from_decimal(dec!(0.9)).unwrap(),
        fill_probability: MicroProb::ONE,
        urgency_factor: MicroProb::ONE,
        category_weight: MicroProb::ONE,
        staleness_discount: MicroProb::ONE,
        book_yes_version: 1,
        book_no_version: 1,
        trace: Arc::new(LatencyTrace::default()),
    }
}

fn seed_execution_books(store: &BookStore, scored: &ScoredOpportunity) {
    let yes_asks = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.92)),
        Shares::new(dec!(1000)),
    )];
    let no_bids = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.07)),
        Shares::new(dec!(1000)),
    )];
    let no_asks = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.08)),
        Shares::new(dec!(1000)),
    )];
    let now_ms = ToPrimitive::to_u64(&Utc::now().timestamp_millis().max(0)).unwrap_or(0);
    store.apply_snapshot(&scored.token_yes, vec![], yes_asks, now_ms, None);
    store.apply_snapshot(&scored.token_no, no_bids, no_asks, now_ms, None);
}

fn execution_bench_risk_engine() -> Arc<oxide_arb_risk::engine::RiskEngine> {
    Arc::new(
        RiskEngineBuilder::new()
            .config(RiskConfig {
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
            })
            .clock(utc_clock())
            .initial_equity(Usd::new(dec!(5000)))
            .build(&BenchMetrics)
            .expect("engine build"),
    )
}

fn execution_bench_risk_metrics(
    exposure: Arc<InMemoryExposureReservation>,
) -> Arc<CoreRiskMetrics> {
    let ws_manager = Arc::new(ClobWsManager::new(
        &PolymarketConfig::default(),
        &WebSocketConfig::default(),
        CancellationToken::new(),
        None,
        None,
    ));
    ws_manager.seed_test_connectivity();
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        std::time::Duration::from_secs(60),
    ))));
    metrics_state.seed_test_snapshot(Usd::new(dec!(5000)));
    Arc::new(CoreRiskMetrics::new(metrics_state, exposure, ws_manager))
}

fn execution_bench_setup() -> (&'static ExecutionPipeline, &'static Arc<ScoredOpportunity>) {
    static SETUP: OnceLock<(ExecutionPipeline, Arc<ScoredOpportunity>)> = OnceLock::new();
    SETUP.get_or_init(|| {
        let metrics = Arc::new(MetricsHub::new());
        let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics)));
        let backpressure = Arc::new(BackpressurePolicy::new(
            Arc::clone(&metrics),
            None,
            Arc::new(InMemoryEventStore::new()),
            1,
        ));
        let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
        let exposure = Arc::new(InMemoryExposureReservation::new(
            ExposureReservationConfig::default(),
        ));
        let capital = Arc::new(CapitalManager::new(
            Arc::clone(&exposure),
            ExposureReservationConfig::default(),
        ));
        let scored = Arc::new(execution_bench_scored());
        seed_execution_books(&book_store, &scored);
        let (outcome_tx, _rx) = ExecutionPipeline::outcome_channel();

        let pipeline = ExecutionPipeline::new(ExecutionPipelineDeps {
            validator: Validator::new(
                Arc::clone(&book_store),
                StalenessClassifier::new(&MarketDataConfig::default()),
                dec!(50),
                5_000,
                Arc::clone(&metrics),
            ),
            plan_builder: PlanBuilder::new(Arc::new(FeeCalculator::default())),
            dispatcher: Dispatcher::new(ExecutionMode::Paper, Arc::clone(&metrics)),
            order_strategy: OrderStrategy::new(ExecutionMode::Paper, None, Arc::clone(&metrics)),
            capital_manager: capital,
            risk_engine: execution_bench_risk_engine(),
            risk_metrics: execution_bench_risk_metrics(exposure),
            fsm,
            market_inflight: Arc::new(MarketInFlightRegistry::new()),
            metrics,
            execution_mode: ExecutionMode::Paper,
            outcome_tx,
            backpressure,
        });

        (pipeline, scored)
    });
    let (pipeline, scored) = SETUP.get().expect("execution bench setup");
    (pipeline, scored)
}

fn bench_execution_pipeline_paper_sync(c: &mut Criterion) {
    let (pipeline, scored) = execution_bench_setup();

    let rt = EXECUTION_RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    });

    rt.block_on(async {
        black_box(pipeline.execute(Arc::clone(scored)).await);
    });

    c.bench_function("execution_pipeline_paper_sync", |b| {
        b.iter(|| {
            rt.block_on(async {
                black_box(pipeline.execute(Arc::clone(scored)).await);
            });
        });
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
    bench_book_apply_delta_50,
    bench_dual_book_assemble,
    bench_coalescer_pair_flush,
    bench_funnel_immediate_dispatch,
    bench_ws_normalize_book_50,
    bench_scanner_scan_market,
    bench_execution_pipeline_paper_sync
);
criterion_main!(benches);
