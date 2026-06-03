use crate::{
    app::{task_id::TaskId, task_registry::PendingTaskQueue},
    infra::async_writer::AsyncWriter,
    observability::{
        book_fact_writer::BookFactWriter, detection_writer::DetectionWriter,
        execution_audit::ExecutionAuditWriter, metrics_hub::MetricsHub,
    },
};
use oxide_arb_error::OxideError;
use oxide_arb_models::clickhouse::{
    BookSnapshotRow, OpportunityAuditRow, OpportunityDetectionRow, TickEventL2Row, TickEventRow,
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
    pub book_fact_writer: Arc<BookFactWriter>,
}

pub struct PersistenceBackgroundWorkers {
    audit: Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
    detection: Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
    tick: Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
    book_l2: Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
    book_snapshot: Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
}

pub struct PersistenceWireInput {
    pub metrics: Arc<MetricsHub>,
    pub shutdown: CancellationToken,
    pub trade_repo: Arc<PgTradeRepository>,
    pub timeseries: Arc<ChTimeseriesRepository>,
}

impl PersistenceBundle {
    pub fn wire(input: PersistenceWireInput) -> (Self, PersistenceBackgroundWorkers) {
        let ts_audit = Arc::clone(&input.timeseries);
        let (audit_raw, audit_writer_worker) = AsyncWriter::new(
            "execution-audit",
            50,
            Duration::from_millis(500),
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
            "detection",
            100,
            Duration::from_secs(1),
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
            "tick-events",
            200,
            Duration::from_millis(500),
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

        let ts_book_l2 = Arc::clone(&input.timeseries);
        let (book_l2_raw, book_l2_writer_worker) = AsyncWriter::new(
            "book-l2",
            200,
            Duration::from_millis(500),
            move |batch: Vec<TickEventL2Row>| {
                let ts = Arc::clone(&ts_book_l2);
                Box::pin(async move {
                    ts.insert_l2_events(batch).await?;
                    Ok(())
                })
            },
            Arc::clone(&input.metrics),
            input.shutdown.clone(),
        );

        let ts_book_snapshot = Arc::clone(&input.timeseries);
        let (book_snapshot_raw, book_snapshot_writer_worker) = AsyncWriter::new(
            "book-snapshot",
            100,
            Duration::from_secs(1),
            move |batch: Vec<BookSnapshotRow>| {
                let ts = Arc::clone(&ts_book_snapshot);
                Box::pin(async move {
                    ts.insert_book_snapshots(batch).await?;
                    Ok(())
                })
            },
            Arc::clone(&input.metrics),
            input.shutdown.clone(),
        );

        let audit_writer = Arc::new(ExecutionAuditWriter::new(Arc::new(audit_raw)));
        let detection_writer = Arc::new(DetectionWriter::new(Arc::new(detection_raw)));
        let book_fact_writer = Arc::new(BookFactWriter::new(
            Arc::new(tick_raw),
            Arc::new(book_l2_raw),
            Arc::new(book_snapshot_raw),
        ));

        let bundle = Self {
            trade_repo: input.trade_repo,
            timeseries: input.timeseries,
            audit_writer,
            detection_writer,
            book_fact_writer,
        };
        let workers = PersistenceBackgroundWorkers {
            audit: Box::pin(audit_writer_worker),
            detection: Box::pin(detection_writer_worker),
            tick: Box::pin(tick_writer_worker),
            book_l2: Box::pin(book_l2_writer_worker),
            book_snapshot: Box::pin(book_snapshot_writer_worker),
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

        let book_l2_worker = workers.book_l2;
        pending.push(TaskId::BookL2Writer, move |shutdown| async move {
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
    }
}
