//! Background runtime tasks for Phase 0 ingest plane.

use super::AppContext;
use crate::app::{task_id::TaskId, task_registry::AppRunner};
use std::sync::Arc;

impl AppContext {
    pub fn register_runtime_tasks(&self, runner: &mut AppRunner) {
        let pipeline = Arc::clone(&self.data.data_pipeline);
        runner.spawn(TaskId::DataPipeline, move |token| async move {
            tokio::select! {
                () = token.cancelled() => {}
                result = pipeline.run() => {
                    if let Err(error) = result {
                        tracing::error!(%error, "DataPipeline exited with error");
                    }
                }
            }
        });
    }
}
