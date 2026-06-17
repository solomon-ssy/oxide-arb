//! Test harness for [`oxide_arb_core::infra::async_writer::AsyncWriter`] workers.

use oxide_arb_core::{
    infra::async_writer::{AsyncWriter, AsyncWriterConfig},
    observability::metrics_hub::MetricsHub,
};
use oxide_arb_error::OxideError;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::{task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;

/// Owns a spawned [`AsyncWriter`] worker until dropped.
pub struct TestAsyncWriterGuard {
    shutdown: CancellationToken,
    worker: Option<JoinHandle<()>>,
}

impl TestAsyncWriterGuard {
    /// Cancel the worker and wait briefly for graceful shutdown.
    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(worker) = self.worker.take() {
            let _ = timeout(Duration::from_secs(2), worker).await;
        }
    }
}

impl Drop for TestAsyncWriterGuard {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}

/// Spawns an [`AsyncWriter`] background worker for integration tests.
///
/// The returned [`TestAsyncWriterGuard`] must stay alive for the writer channel
/// to remain open.
pub fn spawn_test_async_writer<T, F>(
    name: &'static str,
    flush_fn: F,
    metrics: Arc<MetricsHub>,
) -> (Arc<AsyncWriter<T>>, TestAsyncWriterGuard)
where
    T: Send + 'static,
    F: Fn(Vec<T>) -> Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>> + Send + 'static,
{
    let shutdown = CancellationToken::new();
    let (writer, worker) = AsyncWriter::new(
        AsyncWriterConfig::new(name)
            .batch_size(128)
            .flush_interval(Duration::from_secs(3600)),
        flush_fn,
        metrics,
        shutdown.clone(),
    );
    let handle = tokio::spawn(async move {
        let _ = worker.await;
    });
    (
        Arc::new(writer),
        TestAsyncWriterGuard {
            shutdown,
            worker: Some(handle),
        },
    )
}
