//! Full runtime e2e tests (PR-8 F5/F6).

#[path = "support/runtime_harness.rs"]
mod runtime_harness;

use std::time::{Duration, Instant};

use oxide_arb_models::domain::book::BookLevel;
use oxide_arb_models::enums::ExecutionOutcome;
use oxide_arb_models::types::{Price, Shares, TokenId};
use runtime_harness::RuntimeHarness;
use runtime_harness::book_store_seed::seed_book_store;
use runtime_harness::scored_opportunity::sample_scored;

#[tokio::test]
async fn e2e_book_update_to_paper_fill() {
    let mut harness = RuntimeHarness::build();
    harness.register_fixture_market();
    harness.start();
    harness.inject_fixture_books();

    let books_applied = harness
        .run_until(
            |metrics| metrics.book_snapshots_applied.get() >= 2,
            Duration::from_millis(500),
        )
        .await;
    assert!(books_applied, "fixture books should reach book store");

    seed_book_store(harness.book_store(), sample_scored().as_ref());
    let result = harness.pipeline().execute(sample_scored()).await;
    assert!(
        result.is_filled(),
        "paper execution should fill after book update, got {result:?}"
    );

    let job = harness
        .try_recv_post_trade()
        .expect("post_trade_rx should receive job after fill");
    assert!(
        matches!(job.outcome, ExecutionOutcome::Filled { .. }),
        "expected filled outcome, got {:?}",
        job.outcome
    );
    assert!(!harness.fsm().is_emergency());
}

#[tokio::test]
async fn e2e_backpressure_no_halt() {
    let mut harness = RuntimeHarness::build();
    harness.start();

    let yes = TokenId::new("yes-token");
    for i in 0..1000 {
        let price = rust_decimal_macros::dec!(0.90)
            + rust_decimal::Decimal::from(i % 10) * rust_decimal_macros::dec!(0.0001);
        harness.inject_book_snapshot(
            &yes,
            vec![],
            vec![BookLevel::from_decimal_unchecked(
                Price::new(price),
                Shares::new(rust_decimal_macros::dec!(100)),
            )],
        );
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!harness.fsm().is_emergency());
}

#[tokio::test]
async fn e2e_market_inflight_blocks_double_execute() {
    let harness = RuntimeHarness::build();
    seed_book_store(harness.book_store(), sample_scored().as_ref());
    let scored = sample_scored();
    let market_id = scored.opportunity.market_id.clone();

    let _held = harness
        .market_inflight()
        .try_acquire(&market_id)
        .expect("hold inflight slot");

    let result = harness.pipeline().execute(scored).await;
    assert!(result.is_rejected());
    assert_eq!(result.rejection_stage.as_deref(), Some("inflight"));
}

#[tokio::test]
async fn e2e_latency_trace_populated() {
    let harness = RuntimeHarness::build();
    seed_book_store(harness.book_store(), sample_scored().as_ref());

    let before = harness.metrics().latency_tick_to_http_us.get_sample_count();
    let result = harness.pipeline().execute(sample_scored()).await;
    assert!(result.is_filled(), "expected paper fill, got {result:?}");

    assert!(
        harness.metrics().latency_tick_to_http_us.get_sample_count() > before,
        "tick_to_http histogram should record on fill"
    );
}

#[tokio::test]
async fn e2e_tick_to_order_submitted_p99_under_5ms_localhost() {
    let harness = RuntimeHarness::build();
    seed_book_store(harness.book_store(), sample_scored().as_ref());

    let mut latencies_us = Vec::with_capacity(32);
    for _ in 0..32 {
        let start = Instant::now();
        let result = harness.pipeline().execute(sample_scored()).await;
        assert!(result.is_filled(), "expected fill, got {result:?}");
        latencies_us.push(start.elapsed().as_micros());
    }

    latencies_us.sort_unstable();
    let p99_idx = (latencies_us.len() * 99) / 100;
    let p99 = latencies_us[p99_idx.min(latencies_us.len() - 1)];
    assert!(
        p99 < 5_000,
        "in-process paper tick_to_order P99 should be <5ms, got {p99}µs"
    );
}

#[tokio::test]
async fn e2e_coalescer_emits_on_book_pair() {
    let mut harness = RuntimeHarness::build();
    harness.register_endgame_market("m-coalesce");
    harness.start();
    harness.inject_endgame_pair("m-coalesce");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let market = harness
        .market_rx_tap()
        .try_recv()
        .expect("coalescer should emit market after YES+NO update");
    assert_eq!(market.as_str(), "m-coalesce");
}
