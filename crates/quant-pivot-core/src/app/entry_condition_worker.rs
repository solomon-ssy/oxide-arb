//! Recommendation entry-condition shadow/evaluation worker wiring.

use std::sync::Arc;

use quant_pivot_models::{
    clickhouse::EntryConditionEvaluationEventRow, types::ENTRY_CONDITION_INPUT_CHANNEL,
};
use quant_pivot_repository::{
    clickhouse::ChFactWriter,
    traits::{EntryConditionRepository, FactWriter, FactorRepository},
};

use super::{AppContext, task_id::TaskId, task_registry::AppRunner};
use crate::execution::{EntryConditionWorker, LiveEntryConditionInputProvider};

impl AppContext {
    /// Register the condition evaluator in every runtime mode. `ReportOnly` uses
    /// the same durable shadow instances and never signs or submits orders.
    pub fn register_entry_condition_worker(&self, runner: &mut AppRunner) {
        let conditions =
            Arc::clone(&self.infra.repos.entry_condition) as Arc<dyn EntryConditionRepository>;
        let factors = Arc::clone(&self.research.factor_repo) as Arc<dyn FactorRepository>;
        let books = Arc::clone(&self.data.book_store);
        let inputs = Arc::new(LiveEntryConditionInputProvider::new(
            Arc::clone(&books),
            Arc::clone(&conditions),
            factors,
        ));
        let evaluations: Arc<dyn FactWriter<EntryConditionEvaluationEventRow>> =
            Arc::new(ChFactWriter::<EntryConditionEvaluationEventRow>::new(
                Arc::clone(&self.infra.ch),
                Arc::clone(&self.infra.ch_write_manager),
                "quant_entry_condition_evaluation_event",
            ));
        let worker = Arc::new(EntryConditionWorker::new(
            conditions,
            inputs,
            evaluations,
            books,
            self.runtime_config(),
            self.events.clone(),
        ));
        let pg = Arc::clone(&self.infra.pg);
        runner.spawn(TaskId::EntryConditionWorker, move |token| async move {
            let notifications = match pg.listen(ENTRY_CONDITION_INPUT_CHANNEL).await {
                Ok(listener) => Some(listener),
                Err(error) => {
                    tracing::warn!(%error, "entry-condition PostgreSQL wake listener unavailable; using backstop");
                    None
                }
            };
            worker.run(token, notifications).await;
        });
    }
}
