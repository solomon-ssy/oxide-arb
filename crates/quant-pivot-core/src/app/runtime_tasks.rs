//! Background runtime tasks for Phase 0 ingest plane.

use super::AppContext;
use crate::app::{task_id::TaskId, task_registry::AppRunner, trade_tape_worker::TradeTapeWorker};
use quant_pivot_api::exchange::ExchangeLogClient;
use quant_pivot_models::clickhouse::TradeTapeRow;
use quant_pivot_repository::{clickhouse::ChFactWriter, traits::TradeTapeBlockCursorRepository};
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
        if let Some(worker) = self.build_trade_tape_worker() {
            runner.spawn(TaskId::TradeTapeWorker, move |token| async move {
                if let Err(error) = worker.run(token).await {
                    tracing::error!(%error, "TradeTapeWorker exited with error");
                }
            });
        }
    }

    fn build_trade_tape_worker(&self) -> Option<Arc<TradeTapeWorker>> {
        let config = &self.config.market_data.trade_tape_on_chain;
        if !config.enabled {
            return None;
        }
        let log_client = match ExchangeLogClient::connect(&self.config.polymarket.onchain) {
            Ok(client) => Arc::new(client),
            Err(error) => {
                tracing::error!(%error, "trade-tape worker disabled: RPC connect failed");
                return None;
            }
        };
        Some(Arc::new(TradeTapeWorker::new(
            log_client,
            Arc::clone(&self.data.market_registry),
            Arc::clone(&self.infra.repos.trade_tape_block_cursor)
                as Arc<dyn TradeTapeBlockCursorRepository>,
            Arc::new(ChFactWriter::<TradeTapeRow>::new(
                Arc::clone(&self.infra.ch),
                Arc::clone(&self.infra.ch_write_manager),
                "quant_trade_tape",
            )),
            config.clone(),
        )))
    }
}
