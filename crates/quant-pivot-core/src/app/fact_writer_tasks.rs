//! `ClickHouse` book-fact flush workers queued during build.

use super::AppContext;
use crate::app::task_registry::AppRunner;

impl AppContext {
    /// Register `AsyncWriter` flush workers for each book fact stream.
    pub fn register_fact_writer_tasks(&self, runner: &mut AppRunner) {
        self.infra.register_fact_writer_tasks(runner);
    }
}
