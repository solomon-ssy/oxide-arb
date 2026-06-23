//! `ClickHouse` book-fact flush workers queued during build.

use super::AppContext;
use crate::app::task_registry::AppRunner;

impl AppContext {
    /// Register `AsyncWriter` flush workers for each book fact stream.
    pub fn register_fact_writer_tasks(&self, runner: &mut AppRunner) {
        runner.absorb_pending_queue(&self.data.fact_writer_queue);
    }
}
