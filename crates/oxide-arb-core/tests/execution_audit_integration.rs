//! Execution audit row integration tests (CH audit writer capture).

use oxide_arb_core::{
    execution::execution_pipeline::PostTradeDrainDeps,
    service::risk_metrics::{ApiHealthTracker, RiskMetricsState},
};
use oxide_arb_test_support::{persistence::spawn_test_outcome_drain, pipeline::build_pipeline};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn execution_fill_enqueues_post_trade_job() {
    let harness = build_pipeline();
    let result = harness.pipeline.execute(Arc::clone(&harness.scored)).await;
    assert!(result.is_filled(), "expected fill, got {result:?}");

    let job = harness.outcome_rx.try_recv();
    assert!(job.is_ok(), "expected a PostTradeJob enqueued after fill");
}

#[tokio::test]
async fn post_trade_terminal_records_audit_row() {
    let harness = build_pipeline();
    let result = harness.pipeline.execute(Arc::clone(&harness.scored)).await;
    assert!(result.is_filled(), "expected fill, got {result:?}");
    let job = harness.outcome_rx.recv_async().await.expect("job");

    let shutdown = CancellationToken::new();
    let (outcome_tx, outcome_rx) = flume::bounded(1024);
    outcome_tx.try_send(job).expect("enqueue");

    let drain = tokio::spawn({
        let risk_engine = Arc::clone(&harness.risk_engine);
        let risk_metrics = Arc::clone(&harness.risk_metrics);
        let fsm = Arc::clone(&harness.fsm);
        let trade_repo = Arc::clone(&harness.persistence.trade_repo);
        let audit_writer = Arc::clone(&harness.persistence.audit_writer);
        let alerts = Arc::clone(&harness.persistence.alerts);
        let spill = harness.pipeline.post_trade_spill().clone();
        let shutdown = shutdown.clone();
        let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
            Duration::from_secs(60),
        ))));
        async move {
            spawn_test_outcome_drain(
                outcome_rx,
                PostTradeDrainDeps {
                    risk_engine,
                    risk_metrics,
                    fsm,
                    trade_repo,
                    audit_writer,
                    alerts,
                    post_trade_spill: spill,
                    metrics_state,
                    metrics_refresh: None,
                },
                shutdown,
            )
            .await
            .expect("drain");
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    shutdown.cancel();
    drain.await.expect("drain task");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let has_success = harness
        .persistence
        .audit_rows
        .lock()
        .unwrap()
        .iter()
        .any(|r| r.outcome.as_deref() == Some("success"));
    assert!(
        has_success,
        "expected a terminal audit row with outcome=success"
    );
}

#[tokio::test]
async fn risk_deny_emits_rejected_without_trade_row() {
    let harness = build_pipeline();
    harness.fsm.enter_emergency("risk deny simulation");
    let before = harness.persistence.trade_repo.trade_count();
    let result = harness.pipeline.execute(harness.scored).await;
    assert!(result.is_rejected());
    assert_eq!(harness.persistence.trade_repo.trade_count(), before);
}
