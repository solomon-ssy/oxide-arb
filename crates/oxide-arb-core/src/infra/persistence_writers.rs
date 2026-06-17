use crate::{
    app::{task_id::TaskId, task_registry::PendingTaskQueue},
    infra::async_writer::{AsyncWriter, AsyncWriterConfig},
    observability::{
        book_decision_context_writer::BookDecisionContextWriter, book_fact_writer::BookFactWriter,
        detection_writer::DetectionWriter, execution_audit::ExecutionAuditWriter,
        metrics_hub::MetricsHub,
    },
};
use oxide_arb_error::OxideError;
use oxide_arb_models::clickhouse::{
    BookDecisionContextRow, BookL2ReplayRow, BookMicrostructureRow, BookSnapshotRow,
    OpportunityAuditRow, OpportunityDetectionRow, TickEventRow,
};
use oxide_arb_repository::{
    clickhouse::ChTimeseriesRepository, postgres::PgTradeRepository, traits::TimeseriesFactWriter,
};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub struct PersistenceBundle {
    pub trade_repo: Arc<PgTradeRepository>,
    pub timeseries: Arc<ChTimeseriesRepository>,
    pub audit_writer: Arc<ExecutionAuditWriter>,
    pub detection_writer: Arc<DetectionWriter>,
    pub book_decision_context_writer: Arc<BookDecisionContextWriter>,
    pub book_fact_writer: Arc<BookFactWriter>,
}

pub struct PersistenceBackgroundWorkers {
    audit: Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
    detection: Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
    tick: Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
    book_l2_replay: Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
    book_snapshot: Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
    book_decision_context: Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
    book_microstructure_1s: Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
}

pub struct PersistenceWireInput {
    pub metrics: Arc<MetricsHub>,
    pub shutdown: CancellationToken,
    pub trade_repo: Arc<PgTradeRepository>,
    pub timeseries: Arc<ChTimeseriesRepository>,
}

impl PersistenceBundle {
    fn wire_book_l2_replay_writer(
        input: &PersistenceWireInput,
    ) -> (
        AsyncWriter<BookL2ReplayRow>,
        impl Future<Output = Result<(), OxideError>> + Send + 'static,
    ) {
        let ts_book_l2_replay = Arc::clone(&input.timeseries);
        AsyncWriter::new(
            AsyncWriterConfig::new("book-l2-replay")
                .capacity(32_768)
                .batch_size(1_000)
                .flush_interval(Duration::from_millis(250)),
            move |batch: Vec<BookL2ReplayRow>| {
                let ts = Arc::clone(&ts_book_l2_replay);
                Box::pin(async move {
                    ts.insert_book_l2_replay(batch).await?;
                    Ok(())
                })
            },
            Arc::clone(&input.metrics),
            input.shutdown.clone(),
        )
    }

    fn wire_book_snapshot_writer(
        input: &PersistenceWireInput,
    ) -> (
        AsyncWriter<BookSnapshotRow>,
        impl Future<Output = Result<(), OxideError>> + Send + 'static,
    ) {
        let ts_book_snapshot = Arc::clone(&input.timeseries);
        AsyncWriter::new(
            AsyncWriterConfig::new("book-snapshot")
                .capacity(32_768)
                .batch_size(1_000)
                .flush_interval(Duration::from_millis(500)),
            move |batch: Vec<BookSnapshotRow>| {
                let ts = Arc::clone(&ts_book_snapshot);
                Box::pin(async move {
                    ts.insert_book_snapshots(batch).await?;
                    Ok(())
                })
            },
            Arc::clone(&input.metrics),
            input.shutdown.clone(),
        )
    }

    fn wire_book_decision_context_writer(
        input: &PersistenceWireInput,
    ) -> (
        AsyncWriter<BookDecisionContextRow>,
        impl Future<Output = Result<(), OxideError>> + Send + 'static,
    ) {
        let ts_book_decision_context = Arc::clone(&input.timeseries);
        AsyncWriter::new(
            AsyncWriterConfig::new("book-decision-context")
                .capacity(8_192)
                .batch_size(250)
                .flush_interval(Duration::from_millis(250)),
            move |batch: Vec<BookDecisionContextRow>| {
                let ts = Arc::clone(&ts_book_decision_context);
                Box::pin(async move {
                    ts.insert_book_decision_contexts(batch).await?;
                    Ok(())
                })
            },
            Arc::clone(&input.metrics),
            input.shutdown.clone(),
        )
    }

    fn wire_book_microstructure_1s_writer(
        input: &PersistenceWireInput,
    ) -> (
        AsyncWriter<BookMicrostructureRow>,
        impl Future<Output = Result<(), OxideError>> + Send + 'static,
    ) {
        let ts = Arc::clone(&input.timeseries);
        AsyncWriter::new(
            AsyncWriterConfig::new("book-microstructure-1s")
                .capacity(32_768)
                .batch_size(1_000)
                .flush_interval(Duration::from_millis(500)),
            move |batch: Vec<BookMicrostructureRow>| {
                let ts = Arc::clone(&ts);
                Box::pin(async move {
                    ts.insert_book_microstructure_1s(batch).await?;
                    Ok(())
                })
            },
            Arc::clone(&input.metrics),
            input.shutdown.clone(),
        )
    }
}

impl PersistenceBundle {
    pub fn wire(input: PersistenceWireInput) -> (Self, PersistenceBackgroundWorkers) {
        // Capacities are sized against measured ingest rates: the L2 feed
        // peaks at ~3K rows/s during the post-subscribe snapshot flood, so its
        // queue buys ~10s of burst absorption; tick events grow with the BBO
        // stream; audit/detection are low-volume.
        let ts_audit = Arc::clone(&input.timeseries);
        let (audit_raw, audit_writer_worker) = AsyncWriter::new(
            AsyncWriterConfig::new("execution-audit")
                .batch_size(50)
                .flush_interval(Duration::from_millis(500)),
            move |batch: Vec<OpportunityAuditRow>| {
                let ts = Arc::clone(&ts_audit);
                Box::pin(async move {
                    ts.insert_audits(batch).await?;
                    Ok(())
                })
            },
            Arc::clone(&input.metrics),
            input.shutdown.clone(),
        );

        let ts_detection = Arc::clone(&input.timeseries);
        let (detection_raw, detection_writer_worker) = AsyncWriter::new(
            AsyncWriterConfig::new("detection"),
            move |batch: Vec<OpportunityDetectionRow>| {
                let ts = Arc::clone(&ts_detection);
                Box::pin(async move {
                    ts.insert_detections(batch).await?;
                    Ok(())
                })
            },
            Arc::clone(&input.metrics),
            input.shutdown.clone(),
        );

        let ts_tick = Arc::clone(&input.timeseries);
        let (tick_raw, tick_writer_worker) = AsyncWriter::new(
            AsyncWriterConfig::new("tick-events")
                .capacity(16_384)
                .batch_size(500)
                .flush_interval(Duration::from_millis(500)),
            move |batch: Vec<TickEventRow>| {
                let ts = Arc::clone(&ts_tick);
                Box::pin(async move {
                    ts.insert_tick_events(batch).await?;
                    Ok(())
                })
            },
            Arc::clone(&input.metrics),
            input.shutdown.clone(),
        );

        let (book_l2_replay_raw, book_l2_replay_writer_worker) =
            Self::wire_book_l2_replay_writer(&input);
        let (book_snapshot_raw, book_snapshot_writer_worker) =
            Self::wire_book_snapshot_writer(&input);
        let (book_decision_context_raw, book_decision_context_worker) =
            Self::wire_book_decision_context_writer(&input);
        let (second_microstructure_raw, second_microstructure_worker) =
            Self::wire_book_microstructure_1s_writer(&input);

        let audit_writer = Arc::new(ExecutionAuditWriter::new(Arc::new(audit_raw)));
        let book_decision_context_writer = Arc::new(BookDecisionContextWriter::new(Arc::new(
            book_decision_context_raw,
        )));
        let detection_writer = Arc::new(DetectionWriter::new(
            Arc::new(detection_raw),
            Arc::clone(&book_decision_context_writer),
        ));
        let book_fact_writer = Arc::new(BookFactWriter::new(
            Arc::new(tick_raw),
            Arc::new(book_l2_replay_raw),
            Arc::new(book_snapshot_raw),
            Arc::new(second_microstructure_raw),
        ));

        let bundle = Self {
            trade_repo: input.trade_repo,
            timeseries: input.timeseries,
            audit_writer,
            detection_writer,
            book_decision_context_writer,
            book_fact_writer,
        };
        let workers = PersistenceBackgroundWorkers {
            audit: Box::pin(audit_writer_worker),
            detection: Box::pin(detection_writer_worker),
            tick: Box::pin(tick_writer_worker),
            book_l2_replay: Box::pin(book_l2_replay_writer_worker),
            book_snapshot: Box::pin(book_snapshot_writer_worker),
            book_decision_context: Box::pin(book_decision_context_worker),
            book_microstructure_1s: Box::pin(second_microstructure_worker),
        };
        (bundle, workers)
    }

    pub fn queue_background_tasks(
        self,
        workers: PersistenceBackgroundWorkers,
        pending: &mut PendingTaskQueue,
    ) {
        let audit_worker = workers.audit;
        pending.push(TaskId::ExecutionAuditWriter, move |shutdown| async move {
            tokio::select! {
                () = shutdown.cancelled() => {}
                result = audit_worker => {
                    if let Err(error) = result {
                        tracing::error!(%error, "execution audit writer exited with error");
                    }
                }
            }
        });

        let detection_worker = workers.detection;
        pending.push(TaskId::DetectionWriter, move |shutdown| async move {
            tokio::select! {
                () = shutdown.cancelled() => {}
                result = detection_worker => {
                    if let Err(error) = result {
                        tracing::error!(%error, "detection writer exited with error");
                    }
                }
            }
        });

        let tick_worker = workers.tick;
        pending.push(TaskId::TickEventsWriter, move |shutdown| async move {
            tokio::select! {
                () = shutdown.cancelled() => {}
                result = tick_worker => {
                    if let Err(error) = result {
                        tracing::error!(%error, "tick events writer exited with error");
                    }
                }
            }
        });

        let book_l2_worker = workers.book_l2_replay;
        pending.push(TaskId::BookL2ReplayWriter, move |shutdown| async move {
            tokio::select! {
                () = shutdown.cancelled() => {}
                result = book_l2_worker => {
                    if let Err(error) = result {
                        tracing::error!(%error, "book L2 writer exited with error");
                    }
                }
            }
        });

        let book_snapshot_worker = workers.book_snapshot;
        pending.push(TaskId::BookSnapshotWriter, move |shutdown| async move {
            tokio::select! {
                () = shutdown.cancelled() => {}
                result = book_snapshot_worker => {
                    if let Err(error) = result {
                        tracing::error!(%error, "book snapshot writer exited with error");
                    }
                }
            }
        });

        let book_decision_context_worker = workers.book_decision_context;
        pending.push(TaskId::BookDecisionContextWriter, move |shutdown| async move {
            tokio::select! {
                () = shutdown.cancelled() => {}
                result = book_decision_context_worker => {
                    if let Err(error) = result {
                        tracing::error!(%error, "book decision context writer exited with error");
                    }
                }
            }
        });

        let second_microstructure_worker = workers.book_microstructure_1s;
        pending.push(TaskId::BookMicrostructure1sWriter, move |shutdown| async move {
            tokio::select! {
                () = shutdown.cancelled() => {}
                result = second_microstructure_worker => {
                    if let Err(error) = result {
                        tracing::error!(%error, "book microstructure 1s writer exited with error");
                    }
                }
            }
        });
    }
}
