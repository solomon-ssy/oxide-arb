//! End-to-end latency benchmarks: normalize → book → coalescer → scanner.

use chrono::{Duration, Utc};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use num_traits::ToPrimitive;
use oxide_arb_algorithm::{
    calibration::ResolutionCalibrator, cooldown::InMemoryEmissionCooldown,
    endgame::EndgameDetector, pipeline::OpportunityPipeline, scorer::EndgameScorer,
};
use oxide_arb_api::{fees::FeeCalculator, ws::normalize::normalize_ws_message};
use oxide_arb_core::{
    bridge::{CoreOpportunityPipeline, fee_estimator::CoreFeeEstimator},
    control::factor_snapshot::FactorSnapshotStore,
    detection::{coalescer::Coalescer, scanner::Scanner},
    observability::metrics_hub::MetricsHub,
    pipeline::{
        book_store::BookStore,
        market_cache::{CachedMarketScanEntry, MarketCache},
        market_registry::MarketRegistry,
        staleness_classifier::StalenessClassifier,
    },
};
use oxide_arb_models::{
    domain::{
        CoreEventPublisher, book::BookLevel, control_factor::ControlFactorProvider,
        market::MarketRegistryInfo, pipeline::PipelineEvent,
    },
    enums::{
        common::{MarketCategory, TickSize},
        market::MarketStatus,
    },
    runtime_config::{
        CalibrationConfig, DetectionConfig, EmissionCooldownConfig, EndgameDetectionConfig,
        FillProbabilityConfig, MarketDataStalenessConfig,
    },
    types::{EventId, MarketId, Price, Shares, TokenId, Usd},
};
use polymarket_client_sdk_v2::clob::ws::types::response::{BookUpdate, OrderBookLevel, WsMessage};
use polymarket_client_sdk_v2::types::{B256, U256};
use rust_decimal_macros::dec;
use std::time::Duration as StdTimeDuration;
use std::{sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

fn make_core_pipeline() -> CoreOpportunityPipeline {
    let calibrator = Arc::new(ResolutionCalibrator::empty(CalibrationConfig::default()));
    let detector = EndgameDetector::new(
        &EndgameDetectionConfig::default(),
        &CalibrationConfig::default(),
        calibrator,
        CoreFeeEstimator(Arc::new(FeeCalculator::default())),
    );
    let detection_config = DetectionConfig {
        min_profit_threshold_usd: dec!(0.01),
        ..DetectionConfig::default()
    };
    let scorer = EndgameScorer::new(
        &detection_config.endgame.scorer,
        &FillProbabilityConfig::default(),
        24,
    );
    let cooldown = InMemoryEmissionCooldown::new(&EmissionCooldownConfig::default());
    OpportunityPipeline::new(
        detector,
        scorer,
        cooldown,
        Arc::new(FactorSnapshotStore::new(Utc::now())) as Arc<dyn ControlFactorProvider>,
        &detection_config,
    )
}

fn bench_e2e_ws_to_scan(c: &mut Criterion) {
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
        outcome: None,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: dec!(1),
        volume_24h: Usd::ZERO,
        fee_schedule: None,
        end_date: None,
        resolved_at: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });
    let market_cache = Arc::new(MarketCache::new(registry));
    let yes = TokenId::new("bench-m1-yes");
    let no = TokenId::new("bench-m1-no");
    let now_ms = ToPrimitive::to_u64(&Utc::now().timestamp_millis().max(0)).unwrap_or(0);
    book_store.apply_snapshot(
        &yes,
        vec![],
        vec![BookLevel::from_decimal_unchecked(
            Price::new(dec!(0.97)),
            Shares::new(dec!(1000)),
        )],
        now_ms,
        None,
    );
    book_store.apply_snapshot(
        &no,
        vec![BookLevel::from_decimal_unchecked(
            Price::new(dec!(0.02)),
            Shares::new(dec!(1000)),
        )],
        vec![BookLevel::from_decimal_unchecked(
            Price::new(dec!(0.03)),
            Shares::new(dec!(1000)),
        )],
        now_ms,
        None,
    );

    let scanner = Scanner::new(
        Arc::new(make_core_pipeline()),
        book_store,
        market_cache,
        StalenessClassifier::new(&MarketDataStalenessConfig::default()),
        metrics,
        None,
        CoreEventPublisher::bounded(1).0,
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

    c.bench_function("e2e_ws_to_scan", |b| {
        b.iter(|| black_box(scanner.scan_market(black_box(&entry), now)));
    });
}

fn bench_e2e_ws_normalize_to_coalescer(c: &mut Criterion) {
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
        category: MarketCategory::Other,
        status: MarketStatus::Active,
        outcome: None,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: dec!(1),
        volume_24h: Usd::ZERO,
        fee_schedule: None,
        end_date: None,
        resolved_at: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });
    let (market_tx, _market_rx) = flume::bounded(8);
    let coalescer = Coalescer::new(
        registry,
        StdTimeDuration::from_millis(500),
        market_tx,
        Arc::clone(&metrics),
        CancellationToken::new(),
    );

    let levels: Vec<OrderBookLevel> = (0..50)
        .map(|i| {
            OrderBookLevel::builder()
                .price(dec!(0.95) + dec!(0.0001) * rust_decimal::Decimal::from(i))
                .size(dec!(100))
                .build()
        })
        .collect();
    let book = BookUpdate::builder()
        .asset_id(U256::from(1_u64))
        .market(B256::ZERO)
        .timestamp(1000)
        .bids(levels.clone())
        .asks(levels)
        .build();
    let yes = TokenId::new("bench-m1-yes");
    let no = TokenId::new("bench-m1-no");

    c.bench_function("e2e_ws_normalize_to_coalescer", |b| {
        b.iter(|| {
            let ws_ingress = Instant::now();
            let events = normalize_ws_message(WsMessage::Book(book.clone()), ws_ingress, None);
            for event in events {
                if let PipelineEvent::BookSnapshot(cmd) = event {
                    book_store.apply_snapshot(
                        &cmd.asset_id,
                        Arc::clone(&cmd.bids.levels),
                        Arc::clone(&cmd.asks.levels),
                        cmd.timestamp_ms,
                        None,
                    );
                }
            }
            coalescer.notify_token_update(black_box(&yes));
            coalescer.notify_token_update(black_box(&no));
        });
    });
}

criterion_group!(
    e2e_benches,
    bench_e2e_ws_to_scan,
    bench_e2e_ws_normalize_to_coalescer
);
criterion_main!(e2e_benches);
