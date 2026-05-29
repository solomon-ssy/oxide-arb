use crate::{
    app::{task_id::TaskId, task_registry::PendingTaskQueue},
    infra::async_writer::AsyncWriter,
    observability::{
        detection_writer::DetectionWriter, execution_audit::ExecutionAuditWriter,
        metrics_hub::MetricsHub,
    },
    outbox::{
        event_store::{EventStore, PgEventStore},
        flusher::OutboxFlusher,
    },
};
use oxide_arb_error::OxideError;
use oxide_arb_models::clickhouse::{OpportunityAuditRow, OpportunityDetectionRow};
use oxide_arb_repository::{
    clickhouse::ChTimeseriesRepository,
    postgres::{PgOutboxRepository, PgTradeRepository},
    traits::TimeseriesRepository,
};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub struct PersistenceBundle {
    pub trade_repo: Arc<PgTradeRepository>,
    pub timeseries: Arc<ChTimeseriesRepository>,
    pub audit_writer: Arc<ExecutionAuditWriter>,
    pub detection_writer: Arc<DetectionWriter>,
    pub event_store: Arc<dyn EventStore>,
    outbox_flusher: Arc<OutboxFlusher>,
}

pub struct PersistenceBackgroundWorkers {
    audit_writer_worker: Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
    detection_writer_worker: Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
}

pub struct PersistenceWireInput {
    pub metrics: Arc<MetricsHub>,
    pub shutdown: CancellationToken,
    pub trade_repo: Arc<PgTradeRepository>,
    pub outbox_repo: Arc<PgOutboxRepository>,
    pub timeseries: Arc<ChTimeseriesRepository>,
}

impl PersistenceBundle {
    pub fn wire(input: PersistenceWireInput) -> (Self, PersistenceBackgroundWorkers) {
        let event_store: Arc<dyn EventStore> =
            Arc::new(PgEventStore::new(Arc::clone(&input.outbox_repo)));

        let ts_audit = Arc::clone(&input.timeseries);
        let (audit_raw, audit_writer_worker) = AsyncWriter::new(
            "execution-audit",
            50,
            Duration::from_millis(500),
            move |batch: Vec<OpportunityAuditRow>| {
                let ts = Arc::clone(&ts_audit);
                Box::pin(async move {
                    for row in &batch {
                        ts.insert_opportunity_audit(row).await?;
                    }
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
                    ts.insert_detection_batch(&batch).await?;
                    Ok(())
                })
            },
            Arc::clone(&input.metrics),
            input.shutdown.clone(),
        );

        let audit_writer = Arc::new(ExecutionAuditWriter::new(Arc::new(audit_raw)));
        let detection_writer = Arc::new(DetectionWriter::new(Arc::new(detection_raw)));

        let metrics = Arc::clone(&input.metrics);
        let outbox_flusher = Arc::new(OutboxFlusher::new(
            Arc::clone(&event_store),
            Vec::new(),
            100,
            3,
            metrics,
            input.shutdown,
        ));

        let bundle = Self {
            trade_repo: input.trade_repo,
            timeseries: input.timeseries,
            audit_writer,
            detection_writer,
            event_store,
            outbox_flusher,
        };
        let workers = PersistenceBackgroundWorkers {
            audit_writer_worker: Box::pin(audit_writer_worker),
            detection_writer_worker: Box::pin(detection_writer_worker),
        };
        (bundle, workers)
    }

    pub fn queue_background_tasks(
        self,
        workers: PersistenceBackgroundWorkers,
        pending: &mut PendingTaskQueue,
    ) {
        let audit_worker = workers.audit_writer_worker;
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

        let detection_worker = workers.detection_writer_worker;
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

        let flusher = self.outbox_flusher;
        pending.push(TaskId::OutboxFlusher, move |shutdown| async move {
            if let Err(error) = flusher.run().await {
                tracing::error!(%error, "outbox flusher exited with error");
            }
            let _ = shutdown;
        });
    }
}
