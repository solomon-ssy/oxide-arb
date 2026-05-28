//! Test persistence wiring built on repository mocks.

use crate::mocks::MockTradeRepository;
use oxide_arb_core::{
    bridge::risk_metrics::CoreRiskMetrics,
    execution::{
        execution_pipeline::{ExecutionPipeline, PostTradeDrainDeps},
        fsm::ExecutionFSM,
    },
    infra::async_writer::AsyncWriter,
    observability::{
        alert_dispatcher::AlertDispatcher, execution_audit::ExecutionAuditWriter,
        metrics_hub::MetricsHub,
    },
    outbox::in_memory::SharedInMemoryEventStore,
    service::risk_metrics::{ApiHealthTracker, RiskMetricsState},
};
use oxide_arb_error::OxideError;
use oxide_arb_models::{clickhouse::OpportunityAuditRow, domain::execution::PostTradeJob};
use oxide_arb_risk::engine::RiskEngine;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct TestPersistence {
    pub trade_repo: Arc<MockTradeRepository>,
    pub audit_writer: Arc<ExecutionAuditWriter>,
    pub alerts: Arc<AlertDispatcher>,
    pub audit_rows: Arc<Mutex<Vec<OpportunityAuditRow>>>,
    _audit_worker: JoinHandle<()>,
}

pub fn test_persistence(shutdown: CancellationToken) -> TestPersistence {
    let metrics = Arc::new(MetricsHub::new());
    let audit_rows = Arc::new(Mutex::new(Vec::new()));
    let audit_rows_flush = Arc::clone(&audit_rows);
    let (audit_writer_inner, audit_worker) = AsyncWriter::new(
        "test-execution-audit",
        1,
        Duration::from_millis(10),
        move |batch: Vec<OpportunityAuditRow>| {
            let rows = Arc::clone(&audit_rows_flush);
            Box::pin(async move {
                rows.lock().unwrap().extend(batch);
                Ok(())
            })
        },
        Arc::clone(&metrics),
        shutdown,
    );
    let audit_worker = tokio::spawn(async move {
        let _ = audit_worker.await;
    });
    let audit_writer = Arc::new(ExecutionAuditWriter::new(Arc::new(audit_writer_inner)));
    let alerts = Arc::new(AlertDispatcher::new(None, None, None, 60));

    TestPersistence {
        trade_repo: Arc::new(MockTradeRepository::default()),
        audit_writer,
        alerts,
        audit_rows,
        _audit_worker: audit_worker,
    }
}

#[must_use]
pub fn post_trade_drain_deps(
    persistence: &TestPersistence,
    risk_engine: Arc<RiskEngine>,
    risk_metrics: Arc<CoreRiskMetrics>,
    fsm: Arc<ExecutionFSM>,
    post_trade_spill: SharedInMemoryEventStore,
) -> PostTradeDrainDeps<MockTradeRepository> {
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        Duration::from_secs(60),
    ))));
    PostTradeDrainDeps {
        risk_engine,
        risk_metrics,
        fsm,
        trade_repo: Arc::clone(&persistence.trade_repo),
        audit_writer: Arc::clone(&persistence.audit_writer),
        alerts: Arc::clone(&persistence.alerts),
        post_trade_spill,
        metrics_state,
        metrics_refresh: None,
    }
}

pub async fn spawn_test_outcome_drain(
    rx: flume::Receiver<PostTradeJob>,
    deps: PostTradeDrainDeps<MockTradeRepository>,
    shutdown: CancellationToken,
) -> Result<(), OxideError> {
    ExecutionPipeline::<MockTradeRepository>::spawn_outcome_drain(rx, deps, shutdown).await
}
