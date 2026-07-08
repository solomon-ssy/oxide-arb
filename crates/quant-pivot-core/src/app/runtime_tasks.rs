//! Background runtime tasks for Phase 0 ingest plane.

use super::AppContext;
use crate::{
    app::{
        domain_ingest_worker::DomainIngestWorker, task_id::TaskId, task_registry::AppRunner,
        trade_tape_worker::TradeTapeWorker,
    },
    service::domain_ingest::DomainIngestor,
};
use quant_pivot_api::{
    binance::BinanceKlineSource, chainlink::ChainlinkAggregatorSource, domain::DomainDataSource,
    exchange::ExchangeLogClient,
};
use quant_pivot_models::clickhouse::{DomainObservationRow, TradeTapeRow};
use quant_pivot_repository::{
    clickhouse::ChFactWriter,
    traits::{DomainSourceCursorRepository, FactWriter, TradeTapeBlockCursorRepository},
};
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
        if let Some(worker) = self.build_domain_ingest_worker() {
            runner.spawn(TaskId::DomainIngestWorker, move |token| async move {
                if let Err(error) = worker.run(token).await {
                    tracing::error!(%error, "DomainIngestWorker exited with error");
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

    fn build_domain_ingest_worker(&self) -> Option<Arc<DomainIngestWorker>> {
        let sources_config = &self.config.domain_sources;
        if !sources_config.binance.enabled && !sources_config.chainlink.enabled {
            return None;
        }

        let mut sources: Vec<Arc<dyn DomainDataSource>> = Vec::new();
        let mut poll_secs = u64::MAX;

        if sources_config.binance.enabled {
            sources.push(Arc::new(BinanceKlineSource::new(
                sources_config.binance.clone(),
            )));
            poll_secs = poll_secs.min(sources_config.binance.poll_secs);
        }
        if sources_config.chainlink.enabled {
            match ChainlinkAggregatorSource::connect(
                &self.config.polymarket.onchain,
                sources_config.chainlink.clone(),
            ) {
                Ok(source) => {
                    sources.push(Arc::new(source));
                    poll_secs = poll_secs.min(sources_config.chainlink.poll_secs);
                }
                Err(error) => {
                    tracing::error!(%error, "domain-ingest worker disabled: Chainlink RPC connect failed");
                    if sources.is_empty() {
                        return None;
                    }
                }
            }
        }

        if sources.is_empty() {
            return None;
        }

        Some(Arc::new(DomainIngestWorker::new(
            Arc::new(DomainIngestor::new(
                sources,
                Arc::clone(&self.infra.repos.domain_source_cursor)
                    as Arc<dyn DomainSourceCursorRepository>,
                Arc::new(ChFactWriter::<DomainObservationRow>::new(
                    Arc::clone(&self.infra.ch),
                    Arc::clone(&self.infra.ch_write_manager),
                    "quant_domain_observation",
                )) as Arc<dyn FactWriter<DomainObservationRow>>,
                self.runtime_config(),
                sources_config.clone(),
            )),
            poll_secs.max(1),
        )))
    }
}
