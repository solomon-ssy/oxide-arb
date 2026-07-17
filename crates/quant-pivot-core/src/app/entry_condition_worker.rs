//! Recommendation entry-condition shadow/evaluation worker wiring.

use std::sync::Arc;

use quant_pivot_models::{
    clickhouse::EntryConditionEvaluationEventRow, enums::system::CapabilityId,
    types::ENTRY_CONDITION_INPUT_CHANNEL,
};
use quant_pivot_repository::{
    clickhouse::ChFactWriter,
    traits::{
        EntryConditionRepository, FactWriter, FactorRepository, MarketLinkageRepository,
        MarketSelectionRepository, ModelRegistryRepository, RecommendationRepository,
        RuntimeConfigVersionRepository,
    },
};

use super::{
    AppContext, capability_gate::run_while_capable,
    entry_condition_evaluation_outbox_worker::EntryConditionEvaluationOutboxWorker,
    task_id::TaskId, task_registry::AppRunner,
};
use crate::execution::{
    EntryConditionWorker, LiveEntryConditionInputDeps, LiveEntryConditionInputProvider,
};

impl AppContext {
    /// Register the condition evaluator in every runtime mode. `ReportOnly` uses
    /// the same durable shadow instances and never signs or submits orders.
    pub fn register_entry_condition_worker(&self, runner: &mut AppRunner) {
        let conditions =
            Arc::clone(&self.infra.repos.entry_condition) as Arc<dyn EntryConditionRepository>;
        let factors = Arc::clone(&self.research.factor_repo) as Arc<dyn FactorRepository>;
        let books = Arc::clone(&self.data.book_store);
        let inputs = Arc::new(LiveEntryConditionInputProvider::new(
            LiveEntryConditionInputDeps {
                books: Arc::clone(&books),
                conditions: Arc::clone(&conditions),
                factors,
                facts: Arc::clone(&self.infra.quant_fact_read),
                recommendations: Arc::clone(&self.infra.repos.recommendation)
                    as Arc<dyn RecommendationRepository>,
                linkages: Arc::clone(&self.infra.repos.market_linkage)
                    as Arc<dyn MarketLinkageRepository>,
                selections: Arc::clone(&self.infra.repos.market_selection)
                    as Arc<dyn MarketSelectionRepository>,
                models: Arc::clone(&self.infra.repos.model_registry)
                    as Arc<dyn ModelRegistryRepository>,
                runtime_configs: Arc::clone(&self.infra.repos.runtime_config)
                    as Arc<dyn RuntimeConfigVersionRepository>,
                runtime_config: self.runtime_config(),
            },
        ));
        let evaluation_writer: Arc<dyn FactWriter<EntryConditionEvaluationEventRow>> =
            Arc::new(ChFactWriter::<EntryConditionEvaluationEventRow>::new(
                Arc::clone(&self.infra.ch),
                Arc::clone(&self.infra.ch_write_manager),
                "quant_entry_condition_evaluation_event",
            ));
        let worker = Arc::new(EntryConditionWorker::new(
            Arc::clone(&conditions),
            inputs,
            books,
            self.runtime_config(),
            self.events.clone(),
        ));
        let pg = Arc::clone(&self.infra.pg);
        let bootstrap = Arc::clone(&self.governance.bootstrap);
        runner.spawn(TaskId::EntryConditionWorker, move |token| async move {
            run_while_capable(
                bootstrap,
                CapabilityId::ReportGenerationEligible,
                token,
                move |worker_token| {
                    let pg = Arc::clone(&pg);
                    let worker = Arc::clone(&worker);
                    async move {
                        let notifications = match pg.listen(ENTRY_CONDITION_INPUT_CHANNEL).await {
                            Ok(listener) => Some(listener),
                            Err(error) => {
                                tracing::warn!(%error, "entry-condition PostgreSQL wake listener unavailable; using backstop");
                                None
                            }
                        };
                        worker.run(worker_token, notifications).await;
                    }
                },
            )
            .await;
        });
        let outbox_worker = Arc::new(EntryConditionEvaluationOutboxWorker::new(
            conditions,
            evaluation_writer,
        ));
        runner.spawn(
            TaskId::EntryConditionEvaluationOutboxWorker,
            move |token| async move {
                if let Err(error) = outbox_worker.run(token).await {
                    tracing::error!(%error, "EntryConditionEvaluationOutboxWorker exited with error");
                }
            },
        );
    }
}
