//! Report scheduler closed-loop tests (04.3 §25 / §8).
//!
//! In-process, no database: a fake [`ScheduledReportExecutor`] drives the
//! `TokioCronScheduleRunner` so the tests assert scheduling semantics —
//! independent fires, cron+interval coexistence, cadence hot-reload,
//! skip-if-running overlap, ad-hoc one-shot, graceful in-flight drain, and
//! failure isolation — without depending on the report pipeline.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_core::{
    infra::schedule::{
        ReportScheduleRunner, ReportSchedulerDeps, ScheduleOverlapGuard, ScheduledReportExecutor,
        TokioCronScheduleRunner,
    },
    observability::{alert_dispatcher::AlertDispatcher, metrics_hub::MetricsHub},
    report::{AdHocReportRequest, ScheduledReportRequest},
};
use quant_pivot_error::{QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::RecommendationReportInfo,
    runtime_config::{ReportScheduleConfig, ReportsConfig, ScheduleCadence},
};
use tokio::{sync::Mutex as AsyncMutex, time::sleep};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

/// Fake report executor: counts fires per `schedule_id`, can block on a gate to
/// simulate a long pipeline, and always returns an error (the report value is
/// irrelevant to scheduling behaviour).
#[derive(Default)]
struct FakeExecutor {
    scheduled: Mutex<HashMap<String, usize>>,
    ad_hoc: AtomicUsize,
    blocking: AtomicBool,
    gate: AsyncMutex<()>,
}

impl FakeExecutor {
    fn scheduled_count(&self, schedule_id: &str) -> usize {
        self.scheduled
            .lock()
            .expect("scheduled lock")
            .get(schedule_id)
            .copied()
            .unwrap_or(0)
    }

    fn total_scheduled(&self) -> usize {
        self.scheduled
            .lock()
            .expect("scheduled lock")
            .values()
            .sum()
    }

    fn ad_hoc_count(&self) -> usize {
        self.ad_hoc.load(Ordering::Relaxed)
    }

    async fn maybe_block(&self) {
        if self.blocking.load(Ordering::Relaxed) {
            let _gate = self.gate.lock().await;
        }
    }
}

#[async_trait]
impl ScheduledReportExecutor for FakeExecutor {
    async fn run_scheduled(
        &self,
        request: ScheduledReportRequest,
    ) -> QuantResult<RecommendationReportInfo> {
        *self
            .scheduled
            .lock()
            .expect("scheduled lock")
            .entry(request.schedule_id)
            .or_insert(0) += 1;
        self.maybe_block().await;
        Err(ReportError::InvariantViolation {
            stage: "test",
            detail: "fake report".into(),
        }
        .into())
    }

    async fn run_ad_hoc(
        &self,
        _request: AdHocReportRequest,
    ) -> QuantResult<RecommendationReportInfo> {
        self.ad_hoc.fetch_add(1, Ordering::Relaxed);
        self.maybe_block().await;
        Err(ReportError::InvariantViolation {
            stage: "test",
            detail: "fake report".into(),
        }
        .into())
    }
}

const fn interval(secs: u64) -> ScheduleCadence {
    ScheduleCadence::Interval {
        interval_secs: secs,
    }
}

fn schedule(schedule_id: &str, cadence: ScheduleCadence) -> ReportScheduleConfig {
    ReportScheduleConfig {
        schedule_id: schedule_id.to_owned(),
        cadence,
        top_n: 10,
        source_delay_secs: 0,
        enabled: true,
    }
}

fn reports(schedules: Vec<ReportScheduleConfig>) -> ReportsConfig {
    ReportsConfig {
        schedules,
        ..ReportsConfig::default()
    }
}

async fn build_runner(
    executor: Arc<FakeExecutor>,
) -> (Arc<TokioCronScheduleRunner>, Arc<MetricsHub>) {
    let metrics = Arc::new(MetricsHub::new());
    let alerts = Arc::new(AlertDispatcher::with_recordings(Arc::new(Mutex::new(
        Vec::new(),
    ))));
    let executor: Arc<dyn ScheduledReportExecutor> = executor;
    let deps = Arc::new(ReportSchedulerDeps {
        executor,
        overlap: ScheduleOverlapGuard::new(),
        inflight: TaskTracker::new(),
        metrics: Arc::clone(&metrics),
        alerts,
    });
    let runner = Arc::new(
        TokioCronScheduleRunner::new(deps)
            .await
            .expect("build runner"),
    );
    (runner, metrics)
}

fn spawn_run(
    runner: Arc<TokioCronScheduleRunner>,
    token: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _ = runner.run(token).await;
    })
}

async fn wait_until<F: Fn() -> bool>(condition: F, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    condition()
}

const FIRE_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_interval_schedules_fire_independently() {
    let executor = Arc::new(FakeExecutor::default());
    let (runner, _metrics) = build_runner(Arc::clone(&executor)).await;
    runner
        .sync_from_config(&reports(vec![
            schedule("a", interval(1)),
            schedule("b", interval(1)),
        ]))
        .await
        .expect("sync");

    let token = CancellationToken::new();
    let handle = spawn_run(Arc::clone(&runner), token.clone());
    let fired = wait_until(
        || executor.scheduled_count("a") >= 2 && executor.scheduled_count("b") >= 2,
        FIRE_TIMEOUT,
    )
    .await;
    token.cancel();
    let _ = handle.await;

    assert!(fired, "both interval schedules must fire independently");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cron_and_interval_schedules_coexist() {
    let executor = Arc::new(FakeExecutor::default());
    let (runner, _metrics) = build_runner(Arc::clone(&executor)).await;
    runner
        .sync_from_config(&reports(vec![
            schedule("interval", interval(1)),
            schedule(
                "cron",
                ScheduleCadence::Cron {
                    expr: "* * * * * *".to_owned(),
                    timezone: None,
                },
            ),
        ]))
        .await
        .expect("sync");

    let token = CancellationToken::new();
    let handle = spawn_run(Arc::clone(&runner), token.clone());
    let fired = wait_until(
        || executor.scheduled_count("interval") >= 1 && executor.scheduled_count("cron") >= 1,
        FIRE_TIMEOUT,
    )
    .await;
    token.cancel();
    let _ = handle.await;

    assert!(
        fired,
        "cron and interval schedules must coexist and both fire"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activation_changes_cadence_without_restart() {
    let executor = Arc::new(FakeExecutor::default());
    let (runner, _metrics) = build_runner(Arc::clone(&executor)).await;
    // Slow cadence: must not fire during the test window.
    runner
        .sync_from_config(&reports(vec![schedule("daily", interval(3600))]))
        .await
        .expect("initial sync");

    let token = CancellationToken::new();
    let handle = spawn_run(Arc::clone(&runner), token.clone());
    sleep(Duration::from_secs(2)).await;
    assert_eq!(
        executor.scheduled_count("daily"),
        0,
        "slow cadence must not have fired yet"
    );

    // Re-activate with a fast cadence — no restart.
    runner
        .sync_from_config(&reports(vec![schedule("daily", interval(1))]))
        .await
        .expect("re-sync");
    let fired = wait_until(|| executor.scheduled_count("daily") >= 1, FIRE_TIMEOUT).await;
    token.cancel();
    let _ = handle.await;

    assert!(fired, "cadence change must take effect without a restart");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_pipeline_triggers_skip_if_running() {
    let executor = Arc::new(FakeExecutor::default());
    executor.blocking.store(true, Ordering::Relaxed);
    // Hold the gate so the first fire stays in flight.
    let gate = executor.gate.lock().await;

    let (runner, metrics) = build_runner(Arc::clone(&executor)).await;
    runner
        .sync_from_config(&reports(vec![schedule("slow", interval(1))]))
        .await
        .expect("sync");

    let token = CancellationToken::new();
    let handle = spawn_run(Arc::clone(&runner), token.clone());
    let skipped = wait_until(
        || {
            metrics
                .report_schedule_skipped_overlap_total
                .with_label_values(&["slow"])
                .get()
                >= 1
        },
        FIRE_TIMEOUT,
    )
    .await;

    assert!(skipped, "overlapping fires must increment the skip counter");
    assert_eq!(
        executor.total_scheduled(),
        1,
        "only one run may be in flight; overlapping fires are skipped, not queued"
    );

    drop(gate);
    token.cancel();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ad_hoc_run_uses_same_lifecycle_path() {
    let executor = Arc::new(FakeExecutor::default());
    let (runner, _metrics) = build_runner(Arc::clone(&executor)).await;

    let token = CancellationToken::new();
    let handle = spawn_run(Arc::clone(&runner), token.clone());
    runner
        .enqueue_ad_hoc(AdHocReportRequest {
            request_id: "req-1".to_owned(),
            trigger_time: Utc::now(),
            top_n: None,
            source_delay_secs: None,
        })
        .await
        .expect("enqueue ad-hoc");

    let fired = wait_until(|| executor.ad_hoc_count() >= 1, FIRE_TIMEOUT).await;
    token.cancel();
    let _ = handle.await;

    assert!(fired, "ad-hoc one-shot must run through the executor");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_drains_inflight_report() {
    let executor = Arc::new(FakeExecutor::default());
    executor.blocking.store(true, Ordering::Relaxed);
    let gate = executor.gate.lock().await;

    let (runner, _metrics) = build_runner(Arc::clone(&executor)).await;
    runner
        .sync_from_config(&reports(vec![schedule("slow", interval(1))]))
        .await
        .expect("sync");

    let token = CancellationToken::new();
    let handle = spawn_run(Arc::clone(&runner), token.clone());
    assert!(
        wait_until(|| executor.total_scheduled() >= 1, FIRE_TIMEOUT).await,
        "a report must be in flight before shutdown"
    );

    // Cancel: run() stops the scheduler and then blocks draining the in-flight
    // build, which is parked on the gate.
    token.cancel();
    sleep(Duration::from_millis(600)).await;
    assert!(
        !handle.is_finished(),
        "shutdown must wait for the in-flight report to drain"
    );

    // Release the gate: the build finishes, the drain completes, run() returns.
    drop(gate);
    assert!(
        wait_until(|| handle.is_finished(), FIRE_TIMEOUT).await,
        "drain must complete once the in-flight report finishes"
    );
    handle.await.expect("join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn report_failure_does_not_kill_scheduler() {
    let executor = Arc::new(FakeExecutor::default());
    let (runner, metrics) = build_runner(Arc::clone(&executor)).await;
    runner
        .sync_from_config(&reports(vec![schedule("failing", interval(1))]))
        .await
        .expect("sync");

    let token = CancellationToken::new();
    let handle = spawn_run(Arc::clone(&runner), token.clone());
    let kept_running = wait_until(|| executor.scheduled_count("failing") >= 2, FIRE_TIMEOUT).await;
    token.cancel();
    let _ = handle.await;

    assert!(
        kept_running,
        "the scheduler must keep firing despite repeated report failures"
    );
    assert!(
        metrics
            .report_schedule_fires_total
            .with_label_values(&["failing", "error"])
            .get()
            >= 2,
        "failed fires must be recorded"
    );
}
