use std::{hint::black_box, time::Instant};

use bytestring::ByteString;
use criterion::{Criterion, criterion_group, criterion_main};
use prometheus::{GaugeVec, Histogram, HistogramOpts, IntCounter, IntGauge, IntGaugeVec, Opts};
use quant_pivot_models::{
    domain::ws::{SubscriptionKey, WsChannel},
    types::{MarketId, UserId},
};
use quant_pivot_web::ws::{DeliveryClass, SessionHubMetrics, SessionRegistration, SessionRegistry};
use tokio::{runtime::Builder, sync::mpsc};
use tokio_util::sync::CancellationToken;

const SESSION_COUNT: usize = 10_000;
const SUBSCRIBER_COUNT: usize = 1_000;

fn bench_session_hub_fanout(c: &mut Criterion) {
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("session-hub benchmark runtime");
    let shutdown = CancellationToken::new();
    let (registry, hub) = SessionRegistry::new(SessionHubMetrics {
        best_effort_dropped: IntCounter::new("bench_ws_best_effort_dropped", "benchmark")
            .expect("drop counter"),
        best_effort_coalesced: IntCounter::new("bench_ws_best_effort_coalesced", "benchmark")
            .expect("coalesced counter"),
        reliable_disconnects: IntCounter::new("bench_ws_reliable_disconnects", "benchmark")
            .expect("disconnect counter"),
        control_timeouts: IntCounter::new("bench_ws_control_timeouts", "benchmark")
            .expect("timeout counter"),
        control_latency_seconds: Histogram::with_opts(HistogramOpts::new(
            "bench_ws_control_latency_seconds",
            "benchmark",
        ))
        .expect("control latency"),
        queue_depth: IntGaugeVec::new(Opts::new("bench_ws_queue_depth", "benchmark"), &["lane"])
            .expect("queue depth"),
        queue_oldest_age_seconds: GaugeVec::new(
            Opts::new("bench_ws_queue_oldest_age_seconds", "benchmark"),
            &["lane"],
        )
        .expect("queue age"),
        frame_bytes: IntGauge::new("bench_ws_frame_bytes", "benchmark").expect("frame bytes"),
    });
    let hub_task = runtime.spawn(hub.run(shutdown.clone()));
    let topic = SubscriptionKey::scoped(WsChannel::MarketBookUpdate, MarketId::new("bench-market"));
    let mut receivers = runtime.block_on(async {
        let mut receivers = Vec::with_capacity(SESSION_COUNT);
        for index in 0..SESSION_COUNT {
            let (outbound, receiver) = mpsc::channel(256);
            let session_id = registry
                .register(SessionRegistration {
                    outbound,
                    subject: UserId::from_v7(),
                    family_id: format!("bench-family-{index}"),
                    can_read_system: false,
                    cancellation: CancellationToken::new(),
                })
                .await
                .expect("register benchmark session");
            if index < SUBSCRIBER_COUNT {
                assert!(registry.subscribe(session_id, topic.clone()).await);
            }
            receivers.push(receiver);
        }
        receivers
    });

    c.bench_function("session_hub_10k_sessions_1k_topic_fanout", |b| {
        b.iter_custom(|iterations| {
            runtime.block_on(async {
                let start = Instant::now();
                for _ in 0..iterations {
                    assert!(
                        registry
                            .fanout(
                                topic.clone(),
                                ByteString::from_static(r#"{"type":"market.book_update"}"#),
                                DeliveryClass::BestEffort,
                            )
                            .await
                    );
                    for receiver in &mut receivers[..SUBSCRIBER_COUNT] {
                        black_box(receiver.recv().await.expect("benchmark fanout frame"));
                    }
                }
                start.elapsed()
            })
        });
    });

    runtime.block_on(async {
        registry.close_all().await;
        shutdown.cancel();
        hub_task.await.expect("session hub benchmark task");
    });
}

criterion_group!(benches, bench_session_hub_fanout);
criterion_main!(benches);
