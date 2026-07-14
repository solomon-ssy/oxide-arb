//! Background runtime tasks for Phase 0 ingest plane.

use super::AppContext;
use crate::{
    app::{
        archive_partition_worker::ArchivePartitionWorker,
        domain_event_outbox_worker::DomainEventOutboxWorker,
        domain_ingest_worker::DomainIngestWorker,
        domain_live_ingest_worker::{DomainLiveIngestDeps, DomainLiveIngestWorker},
        task_id::TaskId,
        task_registry::AppRunner,
        trade_tape_worker::TradeTapeWorker,
    },
    service::domain_ingest::DomainIngestor,
};
use quant_pivot_api::{
    binance::{BinanceAggTradeSource, BinanceKlineSource},
    chainlink::ChainlinkDataStreamsSource,
    domain::DomainDataSource,
    exchange::ExchangeLogClient,
    weather::{AviationWeatherSource, GefsSource, GhcnhSource},
};
use quant_pivot_models::clickhouse::{
    CryptoPriceReportRow, DomainEventRow, DomainObservationRow, TradeTapeRow,
    WeatherForecastPointRow, WeatherObservationReportRow,
};
use quant_pivot_repository::{
    clickhouse::ChFactWriter,
    traits::{
        ArchivePartitionRepository, CalibrationArtifactRepository, DomainProjectionRepository,
        DomainSourceCursorRepository, FactWriter, MarketLinkageRepository,
        TradeTapeBlockCursorRepository,
    },
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
        self.register_domain_live_ingest_worker(runner);
        let projections =
            Arc::clone(&self.infra.repos.domain_projection) as Arc<dyn DomainProjectionRepository>;
        let worker = Arc::new(DomainEventOutboxWorker::new(
            projections,
            Arc::new(ChFactWriter::<DomainEventRow>::new(
                Arc::clone(&self.infra.ch),
                Arc::clone(&self.infra.ch_write_manager),
                "quant_domain_event",
            )),
        ));
        runner.spawn(TaskId::DomainEventOutboxWorker, move |token| async move {
            if let Err(error) = worker.run(token).await {
                tracing::error!(%error, "DomainEventOutboxWorker exited with error");
            }
        });
        let archive_worker = Arc::new(ArchivePartitionWorker::new(
            Arc::clone(&self.infra.ch),
            Arc::clone(&self.infra.repos.archive_partition) as Arc<dyn ArchivePartitionRepository>,
            self.artifact_store(),
        ));
        runner.spawn(TaskId::ArchivePartitionWorker, move |token| async move {
            if let Err(error) = archive_worker.run(token).await {
                tracing::error!(%error, "ArchivePartitionWorker exited with error");
            }
        });
    }

    fn register_domain_live_ingest_worker(&self, runner: &mut AppRunner) {
        let sources = &self.config.domain_sources;
        let binance = if sources.binance.enabled {
            match BinanceAggTradeSource::connect(sources.binance.clone()) {
                Ok(source) => Some(Arc::new(source)),
                Err(error) => {
                    tracing::error!(%error, "Binance aggTrade unavailable; bound conditions fail closed");
                    None
                }
            }
        } else {
            None
        };
        let chainlink = if sources.chainlink_data_streams.enabled {
            match ChainlinkDataStreamsSource::connect(sources.chainlink_data_streams.clone()) {
                Ok(source) => Some(Arc::new(source)),
                Err(error) => {
                    tracing::error!(%error, "Chainlink Data Streams unavailable; bound conditions fail closed");
                    None
                }
            }
        } else {
            None
        };
        let aviation = if sources.aviation_weather.enabled {
            match AviationWeatherSource::connect(sources.aviation_weather.clone()) {
                Ok(source) => Some(Arc::new(source)),
                Err(error) => {
                    tracing::error!(%error, "AviationWeather unavailable; bound conditions fail closed");
                    None
                }
            }
        } else {
            None
        };
        let ghcnh = if sources.ghcnh.enabled {
            match GhcnhSource::connect(sources.ghcnh.clone()) {
                Ok(source) => Some(Arc::new(source)),
                Err(error) => {
                    tracing::error!(%error, "GHCNh unavailable; Weather calibration remains blocked");
                    None
                }
            }
        } else {
            None
        };
        let gefs = if sources.gefs.enabled {
            match GefsSource::connect(sources.gefs.clone()) {
                Ok(source) => Some(Arc::new(source)),
                Err(error) => {
                    tracing::error!(%error, "GEFS unavailable; Weather forecast factors remain blocked");
                    None
                }
            }
        } else {
            None
        };
        let worker = Arc::new(DomainLiveIngestWorker::new(DomainLiveIngestDeps {
            linkages: Arc::clone(&self.infra.repos.market_linkage)
                as Arc<dyn MarketLinkageRepository>,
            cursors: Arc::clone(&self.infra.repos.domain_source_cursor)
                as Arc<dyn DomainSourceCursorRepository>,
            projections: Arc::clone(&self.infra.repos.domain_projection)
                as Arc<dyn DomainProjectionRepository>,
            crypto_writer: Arc::new(ChFactWriter::<CryptoPriceReportRow>::new(
                Arc::clone(&self.infra.ch),
                Arc::clone(&self.infra.ch_write_manager),
                "quant_crypto_price_report",
            )),
            weather_writer: Arc::new(ChFactWriter::<WeatherObservationReportRow>::new(
                Arc::clone(&self.infra.ch),
                Arc::clone(&self.infra.ch_write_manager),
                "quant_weather_observation_report",
            )),
            forecast_writer: Arc::new(ChFactWriter::<WeatherForecastPointRow>::new(
                Arc::clone(&self.infra.ch),
                Arc::clone(&self.infra.ch_write_manager),
                "quant_weather_forecast_point",
            )),
            fact_read: Arc::clone(&self.infra.quant_fact_read),
            calibrations: Arc::clone(&self.infra.repos.calibration_artifact)
                as Arc<dyn CalibrationArtifactRepository>,
            runtime_config: Arc::clone(&self.governance.runtime_config),
            binance,
            chainlink,
            aviation,
            ghcnh,
            gefs,
            aviation_config: sources.aviation_weather.clone(),
            ghcnh_config: sources.ghcnh.clone(),
            gefs_config: sources.gefs.clone(),
            station_profiles: sources.weather_stations.clone(),
        }));
        runner.spawn(TaskId::DomainLiveIngestWorker, move |token| async move {
            worker.run(token).await;
        });
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
        if !sources_config.binance.enabled {
            return None;
        }

        let mut sources: Vec<Arc<dyn DomainDataSource>> = Vec::new();
        let mut poll_secs = u64::MAX;

        if sources_config.binance.enabled {
            let source = match BinanceKlineSource::connect(sources_config.binance.clone()) {
                Ok(source) => source,
                Err(error) => {
                    tracing::error!(%error, "Binance kline source unavailable");
                    return None;
                }
            };
            sources.push(Arc::new(source));
            poll_secs = poll_secs.min(sources_config.binance.kline_poll_secs);
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
