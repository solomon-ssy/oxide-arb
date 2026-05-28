//! Scanner integration: stale books are rejected at `BookGate` before scoring.

use chrono::{TimeZone, Utc};
use oxide_arb_algorithm::{
    calibration::ResolutionCalibrator, cooldown::InMemoryEmissionCooldown,
    endgame::EndgameDetector, pipeline::OpportunityPipeline, scorer::EndgameScorer,
};
use oxide_arb_api::fees::FeeCalculator;
use oxide_arb_core::{
    bridge::{CoreOpportunityPipeline, fee_estimator::CoreFeeEstimator},
    detection::scanner::Scanner,
    observability::metrics_hub::MetricsHub,
    pipeline::{
        book_store::BookStore, market_cache::MarketCache, market_registry::MarketRegistry,
        staleness_classifier::StalenessClassifier,
    },
};
use oxide_arb_models::{
    config::Settings,
    domain::{
        book::BookLevel,
        market::{MarketRegistryInfo, TokenInfo},
    },
    enums::{
        common::{MarketCategory, TickSize},
        market::MarketStatus,
    },
    types::{EventId, MarketId, MicroUsd, Price, Shares, TokenId, Usd},
};
use rust_decimal_macros::dec;
use std::sync::Arc;

fn sample_market(id: &str) -> MarketRegistryInfo {
    MarketRegistryInfo {
        market_id: MarketId::new(id),
        event_id: EventId::new("evt"),
        token_yes: TokenId::new(format!("{id}-yes")),
        token_no: TokenId::new(format!("{id}-no")),
        question: "Q".into(),
        slug: "q".into(),
        category: MarketCategory::Politics,
        status: MarketStatus::Active,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![
            TokenInfo {
                token_id: TokenId::new(format!("{id}-yes")),
                outcome: "Yes".into(),
                neg_risk: false,
            },
            TokenInfo {
                token_id: TokenId::new(format!("{id}-no")),
                outcome: "No".into(),
                neg_risk: false,
            },
        ],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: dec!(1),
        volume_24h: Usd::ZERO,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

fn build_scanner(
    book_store: Arc<BookStore>,
    market_cache: Arc<MarketCache>,
    metrics: Arc<MetricsHub>,
) -> Scanner {
    let settings = Settings::new("nonexistent_dir_for_test").expect("default settings");
    let fee_calculator = Arc::new(FeeCalculator::from_config(&settings.polymarket.fees));
    let calibrator = Arc::new(ResolutionCalibrator::empty(
        settings.detection.calibration.clone(),
    ));
    let detector = EndgameDetector::new(
        &settings.detection.endgame,
        &settings.detection.calibration,
        Arc::clone(&calibrator),
        CoreFeeEstimator(fee_calculator),
    );
    let scorer = EndgameScorer::new(
        settings.detection.endgame.scorer.clone(),
        &settings.detection.endgame.fill_probability,
        settings.detection.endgame.settlement_window_hours,
    );
    let cooldown = InMemoryEmissionCooldown::new(&settings.detection.endgame.emission_cooldown);
    let min_profit = MicroUsd::try_from_decimal(settings.detection.min_profit_threshold_usd)
        .unwrap_or(MicroUsd::ZERO);
    let pipeline: Arc<CoreOpportunityPipeline> = Arc::new(OpportunityPipeline::new(
        detector,
        scorer,
        cooldown,
        min_profit,
        &settings.detection.endgame.scorer,
    ));
    let staleness = StalenessClassifier::new(&settings.market_data);

    Scanner::new(pipeline, book_store, market_cache, staleness, metrics, None)
}

fn seed_full_books(store: &BookStore, yes: &TokenId, no: &TokenId, timestamp_ms: u64) {
    let level = |p: rust_decimal::Decimal| {
        BookLevel::from_decimal_unchecked(Price::new(p), Shares::new(dec!(1000)))
    };
    store.apply_snapshot(
        yes,
        vec![level(dec!(0.90))],
        vec![level(dec!(0.92))],
        timestamp_ms,
        None,
    );
    store.apply_snapshot(
        no,
        vec![level(dec!(0.07))],
        vec![level(dec!(0.08))],
        timestamp_ms,
        None,
    );
}

#[test]
fn scan_market_returns_none_when_books_are_stale() {
    let metrics = Arc::new(MetricsHub::new());
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    let registry = Arc::new(MarketRegistry::new());
    registry.register_market(sample_market("m-stale"));
    let market_cache = Arc::new(MarketCache::new(Arc::clone(&registry)));
    let scanner = build_scanner(
        Arc::clone(&book_store),
        Arc::clone(&market_cache),
        Arc::clone(&metrics),
    );

    let entry = market_cache
        .entries()
        .first()
        .expect("cached market")
        .clone();
    let yes = entry.token_yes.clone();
    let no = entry.token_no.clone();

    let now_ms = 20_000u64;
    let stale_ts = 1_000u64;
    seed_full_books(&book_store, &yes, &no, stale_ts);

    let now = Utc
        .timestamp_millis_opt(i64::try_from(now_ms).unwrap_or(i64::MAX))
        .single()
        .expect("valid timestamp");

    let rejected_before = metrics.scans_gate_rejected.get();
    let result = scanner.scan_market(&entry, now);
    assert!(
        result.is_none(),
        "stale books should not produce opportunities"
    );
    assert_eq!(
        metrics.scans_gate_rejected.get(),
        rejected_before + 1,
        "BookGate rejection should increment scans_gate_rejected"
    );
}

#[test]
fn scan_market_passes_gate_with_fresh_books() {
    let metrics = Arc::new(MetricsHub::new());
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    let registry = Arc::new(MarketRegistry::new());
    registry.register_market(sample_market("m-fresh"));
    let market_cache = Arc::new(MarketCache::new(Arc::clone(&registry)));
    let scanner = build_scanner(
        Arc::clone(&book_store),
        Arc::clone(&market_cache),
        Arc::clone(&metrics),
    );

    let entry = market_cache
        .entries()
        .first()
        .expect("cached market")
        .clone();
    let yes = entry.token_yes.clone();
    let no = entry.token_no.clone();

    let now_ms = 10_000u64;
    let fresh_ts = now_ms - 1_000;
    seed_full_books(&book_store, &yes, &no, fresh_ts);

    let now = Utc
        .timestamp_millis_opt(i64::try_from(now_ms).unwrap_or(i64::MAX))
        .single()
        .expect("valid timestamp");

    let rejected_before = metrics.scans_gate_rejected.get();
    let _ = scanner.scan_market(&entry, now);
    assert_eq!(
        metrics.scans_gate_rejected.get(),
        rejected_before,
        "fresh books should pass BookGate without incrementing scans_gate_rejected"
    );
}
