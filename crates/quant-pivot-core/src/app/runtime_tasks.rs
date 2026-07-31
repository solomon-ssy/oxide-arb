//! Background runtime tasks for ingest plane.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use quant_pivot_api::{
    binance::{BinanceAggTradeSource, BinanceKlineSource, BinanceRequestBudget},
    domain::DomainDataSource,
    exchange::ExchangeLogClient,
    rtds::PolymarketRtdsSource,
    weather::{
        AviationWeatherSource, GefsSource, GhcnhSource, airnow::AirNowSource, ghcnd::GhcndSource,
        gistemp::NasaGistempSource, hko::HkoOpenDataSource, nhc::NhcSource,
        nsidc::NsidcSeaIceSource, nws::NwsObservationSource, tornado::TornadoSource,
    },
};
#[cfg(feature = "domain-chainlink")]
use quant_pivot_models::types::DomainSourceId;
use quant_pivot_models::{
    clickhouse::{
        CryptoPriceReportRow, DomainEventRow, DomainObservationRow, TradeTapeRow,
        WeatherForecastFactRow, WeatherObservationFactRow,
    },
    config::BinanceSourceConfig,
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChNativeReadRepository},
    traits::{
        CalibrationArtifactRepository, ClobMarketInfoRepository, DomainProjectionRepository,
        DomainSourceCursorRepository, DomainSourceExpectationRepository, FactWriter,
        MarketLinkageRepository, MarketRepository, TradeTapeBlockCursorRepository,
    },
};

use super::AppContext;
#[cfg(feature = "domain-chainlink")]
use crate::app::crypto_live_ingest_worker::ChainlinkDataStreamsSource;
use crate::{
    app::{
        clob_market_info_worker::ClobMarketInfoWorker,
        crypto_kline_ingest_worker::CryptoKlineIngestWorker,
        crypto_live_ingest_worker::{CryptoLiveIngestDeps, CryptoLiveIngestWorker},
        crypto_rtds_ingest_worker::{CryptoRtdsIngestDeps, CryptoRtdsIngestWorker},
        domain_event_outbox_worker::DomainEventOutboxWorker,
        domain_source_supervisor::DomainSourceSupervisor,
        task_id::TaskId,
        task_registry::AppRunner,
        trade_tape_reconciliation_worker::TradeTapeReconciliationWorker,
        trade_tape_worker::TradeTapeWorker,
        weather_backfill_worker::WeatherBackfillWorker,
        weather_ingest_worker::{WeatherIngestDeps, WeatherIngestWorker},
        weather_public_ingest_worker::{WeatherPublicIngestDeps, WeatherPublicIngestWorker},
    },
    service::{
        crypto_kline_ingest::CryptoKlineIngestor, weather_fact_ingest::WeatherFactIngestService,
    },
};

struct ConnectedDomainSources {
    binance: Option<Arc<BinanceAggTradeSource>>,
    binance_usdm_futures: Option<Arc<BinanceAggTradeSource>>,
    #[cfg(feature = "domain-chainlink")]
    chainlink: Option<Arc<ChainlinkDataStreamsSource>>,
    rtds: Option<Arc<PolymarketRtdsSource>>,
    aviation: Option<Arc<AviationWeatherSource>>,
    ghcnh: Option<Arc<GhcnhSource>>,
    ghcnd: Option<Arc<GhcndSource>>,
    gefs: Option<Arc<GefsSource>>,
    weather_public: ConnectedPublicWeatherSources,
}

struct ConnectedCryptoSources {
    binance: Option<Arc<BinanceAggTradeSource>>,
    binance_usdm_futures: Option<Arc<BinanceAggTradeSource>>,
    #[cfg(feature = "domain-chainlink")]
    chainlink: Option<Arc<ChainlinkDataStreamsSource>>,
    rtds: Option<Arc<PolymarketRtdsSource>>,
}

#[derive(Clone)]
struct BinanceBudgets {
    spot: Option<BinanceRequestBudget>,
    usdm_futures: Option<BinanceRequestBudget>,
}

struct ConnectedPublicWeatherSources {
    hko: Option<Arc<HkoOpenDataSource>>,
    airnow: Option<Arc<AirNowSource>>,
    tornado: Option<Arc<TornadoSource>>,
    nhc: Option<Arc<NhcSource>>,
    gistemp: Option<Arc<NasaGistempSource>>,
    nsidc: Option<Arc<NsidcSeaIceSource>>,
    nws: Option<Arc<NwsObservationSource>>,
}

impl AppContext {
    pub fn register_runtime_tasks(&self, runner: &mut AppRunner) {
        let binance_budgets = BinanceBudgets {
            spot: build_binance_budget(&self.config.domain_sources.binance, "Spot"),
            usdm_futures: build_binance_budget(
                &self.config.domain_sources.binance_usdm_futures,
                "USD-M Futures",
            ),
        };
        let pipeline = Arc::clone(&self.data.data_pipeline);
        runner.spawn_critical(TaskId::DataPipeline, move |_token| async move {
            // DataPipeline owns the root shutdown token and must finish its
            // ingress/worker drain before the Analytics stage closes sinks.
            pipeline.run().await
        });
        if let Some(worker) = self.build_trade_tape_worker() {
            runner.spawn(TaskId::TradeTapeWorker, move |token| async move {
                if let Err(error) = worker.run(token).await {
                    tracing::error!(%error, "TradeTapeWorker exited with error");
                }
            });
        }
        if let Some(worker) = self.build_tape_reconciler() {
            runner.spawn(
                TaskId::TradeTapeReconciliationWorker,
                move |token| async move {
                    if let Err(error) = worker.run(token).await {
                        tracing::error!(%error, "TradeTapeReconciliationWorker exited with error");
                    }
                },
            );
        }
        self.register_domain_ingest_workers(runner, binance_budgets);
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
        let report_fact_delivery = Arc::clone(&self.report.fact_delivery);
        runner.spawn(TaskId::ReportFactDeliveryWorker, move |token| async move {
            if let Err(error) = report_fact_delivery.run(token).await {
                tracing::error!(%error, "ReportFactDeliveryWorker exited with error");
            }
        });
        let clob_market_info = Arc::new(ClobMarketInfoWorker::new(
            Arc::clone(&self.execution.clob),
            Arc::clone(&self.infra.repos.market) as Arc<dyn MarketRepository>,
            Arc::clone(&self.infra.repos.clob_market_info) as Arc<dyn ClobMarketInfoRepository>,
            Duration::from_secs(self.config.polymarket.clob_market_info_refresh_secs),
        ));
        runner.spawn(TaskId::ClobMarketInfoSync, move |token| async move {
            clob_market_info.run(token).await;
        });
    }

    fn connect_domain_sources(&self, binance_budgets: BinanceBudgets) -> ConnectedDomainSources {
        let sources = &self.config.domain_sources;
        let binance = if sources.binance.enabled {
            binance_budgets.spot.map_or_else(
                || {
                    tracing::error!(
                        "Binance aggTrade unavailable without a shared request budget"
                    );
                    None
                },
                |binance_budget| {
                    BinanceAggTradeSource::connect_with_budget(
                        sources.binance.clone(),
                        binance_budget,
                        Arc::clone(&self.compute),
                    )
                    .map(Arc::new)
                    .map_err(|error| {
                        tracing::error!(%error, "Binance aggTrade unavailable; bound conditions fail closed");
                    })
                    .ok()
                },
            )
        } else {
            None
        };
        let binance_usdm_futures = if sources.binance_usdm_futures.enabled {
            binance_budgets.usdm_futures.map_or_else(
                || {
                    tracing::error!(
                        "Binance USD-M Futures aggTrade unavailable without a request budget"
                    );
                    None
                },
                |budget| {
                    BinanceAggTradeSource::connect_usdm_futures(
                        sources.binance_usdm_futures.clone(),
                        budget,
                        Arc::clone(&self.compute),
                    )
                    .map(Arc::new)
                    .map_err(|error| {
                        tracing::error!(%error, "Binance USD-M Futures aggTrade unavailable");
                    })
                    .ok()
                },
            )
        } else {
            None
        };
        #[cfg(feature = "domain-chainlink")]
        let chainlink = sources.chainlink_data_streams.enabled.then(|| {
            ChainlinkDataStreamsSource::connect(sources.chainlink_data_streams.clone())
                .map(Arc::new)
                .map_err(|error| {
                    tracing::error!(%error, "Chainlink Data Streams unavailable; bound conditions fail closed");
                })
                .ok()
        }).flatten();
        #[cfg(not(feature = "domain-chainlink"))]
        {
            if sources.chainlink_data_streams.enabled {
                tracing::error!(
                    "Chainlink Data Streams configured but compile-time `domain-chainlink` is disabled; bound conditions fail closed"
                );
            }
        }
        let rtds = sources.polymarket_rtds.enabled.then(|| {
            Arc::new(PolymarketRtdsSource::connect(
                sources.polymarket_rtds.clone(),
            ))
        });
        let aviation = sources.aviation_weather.enabled.then(|| {
            AviationWeatherSource::connect(sources.aviation_weather.clone())
                .map(Arc::new)
                .map_err(|error| {
                    tracing::error!(%error, "AviationWeather unavailable; bound conditions fail closed");
                })
                .ok()
        }).flatten();
        let ghcn_hourly = sources.ghcnh.enabled.then(|| {
            GhcnhSource::connect(sources.ghcnh.clone())
                .map(Arc::new)
                .map_err(|error| {
                    tracing::error!(%error, "GHCNh unavailable; Weather calibration remains blocked");
                })
                .ok()
        }).flatten();
        let ghcn_daily = sources.ghcnd.enabled.then(|| {
            GhcndSource::connect(sources.ghcnd.clone())
                .map(Arc::new)
                .map_err(|error| {
                    tracing::error!(%error, "GHCNd unavailable; Weather daily labels remain blocked");
                })
                .ok()
        }).flatten();
        let gefs = sources.gefs.enabled.then(|| {
            GefsSource::connect(sources.gefs.clone())
                .map(Arc::new)
                .map_err(|error| {
                    tracing::error!(%error, "GEFS unavailable; Weather forecast factors remain blocked");
                })
                .ok()
        }).flatten();
        let weather_public = self.connect_public_weather_sources();
        ConnectedDomainSources {
            binance,
            binance_usdm_futures,
            #[cfg(feature = "domain-chainlink")]
            chainlink,
            rtds,
            aviation,
            ghcnh: ghcn_hourly,
            ghcnd: ghcn_daily,
            gefs,
            weather_public,
        }
    }

    fn connect_public_weather_sources(&self) -> ConnectedPublicWeatherSources {
        let sources = &self.config.domain_sources;
        let hko = sources
            .hko_open_data
            .enabled
            .then(|| {
                HkoOpenDataSource::connect(sources.hko_open_data.clone())
                    .map(Arc::new)
                    .map_err(|error| {
                        tracing::error!(%error, "HKO unavailable; precipitation remains blocked");
                    })
                    .ok()
            })
            .flatten();
        let airnow = sources
            .airnow
            .enabled
            .then(|| {
                AirNowSource::connect(sources.airnow.clone())
                    .map(Arc::new)
                    .map_err(|error| {
                        tracing::error!(%error, "AirNow unavailable; AQI remains blocked");
                    })
                    .ok()
            })
            .flatten();
        let tornado = sources
            .tornado
            .enabled
            .then(|| {
                TornadoSource::connect(sources.tornado.clone(), Arc::clone(&self.compute))
                    .map(Arc::new)
                    .map_err(|error| {
                        tracing::error!(%error, "NOAA tornado sources unavailable");
                    })
                    .ok()
            })
            .flatten();
        let nhc = sources
            .nhc
            .enabled
            .then(|| {
                NhcSource::connect(sources.nhc.clone())
                    .map(Arc::new)
                    .map_err(|error| {
                        tracing::error!(%error, "NHC unavailable; cyclone facts remain blocked");
                    })
                    .ok()
            })
            .flatten();
        let gistemp = sources
            .nasa_gistemp
            .enabled
            .then(|| {
                NasaGistempSource::connect(sources.nasa_gistemp.clone())
                    .map(Arc::new)
                    .map_err(|error| {
                        tracing::error!(%error, "NASA GISTEMP unavailable");
                    })
                    .ok()
            })
            .flatten();
        let nsidc = sources
            .nsidc_sea_ice
            .enabled
            .then(|| {
                NsidcSeaIceSource::connect(sources.nsidc_sea_ice.clone())
                    .map(Arc::new)
                    .map_err(|error| {
                        tracing::error!(%error, "NSIDC unavailable; sea-ice facts remain blocked");
                    })
                    .ok()
            })
            .flatten();
        let nws = sources
            .nws_observation
            .enabled
            .then(|| {
                NwsObservationSource::connect(sources.nws_observation.clone())
                    .map(Arc::new)
                    .map_err(|error| {
                        tracing::error!(%error, "NWS unavailable; wind facts remain blocked");
                    })
                    .ok()
            })
            .flatten();
        ConnectedPublicWeatherSources {
            hko,
            airnow,
            tornado,
            nhc,
            gistemp,
            nsidc,
            nws,
        }
    }

    fn register_domain_ingest_workers(
        &self,
        runner: &mut AppRunner,
        binance_budgets: BinanceBudgets,
    ) {
        let sources = &self.config.domain_sources;
        let ConnectedDomainSources {
            binance,
            binance_usdm_futures,
            #[cfg(feature = "domain-chainlink")]
            chainlink,
            rtds,
            aviation,
            ghcnh,
            ghcnd,
            gefs,
            weather_public:
                ConnectedPublicWeatherSources {
                    hko,
                    airnow,
                    tornado,
                    nhc,
                    gistemp,
                    nsidc,
                    nws,
                },
        } = self.connect_domain_sources(binance_budgets.clone());
        #[cfg(feature = "domain-chainlink")]
        let credential_ready_sources = chainlink
            .is_some()
            .then(DomainSourceId::chainlink_data_streams)
            .into_iter()
            .collect::<BTreeSet<_>>();
        #[cfg(not(feature = "domain-chainlink"))]
        let credential_ready_sources = BTreeSet::new();
        let source_supervisor = match DomainSourceSupervisor::new(
            Arc::clone(&self.infra.repos.domain_source_expectation)
                as Arc<dyn DomainSourceExpectationRepository>,
            Arc::clone(&self.infra.repos.market_linkage) as Arc<dyn MarketLinkageRepository>,
            sources.weather_stations.clone(),
            sources.weather_vertical_bindings.clone(),
            credential_ready_sources,
        ) {
            Ok(supervisor) => Arc::new(supervisor),
            Err(error) => {
                tracing::error!(%error, "domain ingest disabled: capability registry is invalid");
                return;
            }
        };
        let supervisor_task = Arc::clone(&source_supervisor);
        runner.spawn(TaskId::DomainSourceSupervisor, move |token| async move {
            if let Err(error) = supervisor_task.ensure_boot_reconciled().await {
                tracing::error!(%error, "domain source supervisor boot reconciliation failed");
                return;
            }
            supervisor_task.run_periodic(token).await;
        });
        self.register_crypto_ingest_workers(
            runner,
            binance_budgets,
            Arc::clone(&source_supervisor),
            ConnectedCryptoSources {
                binance,
                binance_usdm_futures,
                #[cfg(feature = "domain-chainlink")]
                chainlink,
                rtds,
            },
        );
        let weather_writer = Arc::new(ChFactWriter::<WeatherObservationFactRow>::new(
            Arc::clone(&self.infra.ch),
            Arc::clone(&self.infra.ch_write_manager),
            "quant_weather_observation_fact",
        )) as Arc<dyn FactWriter<WeatherObservationFactRow>>;
        let forecast_writer = Arc::new(ChFactWriter::<WeatherForecastFactRow>::new(
            Arc::clone(&self.infra.ch),
            Arc::clone(&self.infra.ch_write_manager),
            "quant_weather_forecast_fact",
        )) as Arc<dyn FactWriter<WeatherForecastFactRow>>;
        let weather_facts = Arc::new(WeatherFactIngestService::new(
            Arc::clone(&weather_writer),
            Arc::clone(&forecast_writer),
            Arc::clone(&self.infra.quant_fact_read),
        ));
        let weather_worker = Arc::new(WeatherIngestWorker::new(WeatherIngestDeps {
            source_supervisor: Arc::clone(&source_supervisor),
            linkages: Arc::clone(&self.infra.repos.market_linkage)
                as Arc<dyn MarketLinkageRepository>,
            cursors: Arc::clone(&self.infra.repos.domain_source_cursor)
                as Arc<dyn DomainSourceCursorRepository>,
            projections: Arc::clone(&self.infra.repos.domain_projection)
                as Arc<dyn DomainProjectionRepository>,
            weather_writer,
            forecast_writer,
            fact_read: Arc::clone(&self.infra.quant_fact_read),
            calibrations: Arc::clone(&self.infra.repos.calibration_artifact)
                as Arc<dyn CalibrationArtifactRepository>,
            runtime_config: Arc::clone(&self.governance.runtime_config),
            aviation,
            ghcnh,
            ghcnd,
            gefs,
            aviation_config: sources.aviation_weather.clone(),
            ghcnh_config: sources.ghcnh.clone(),
            ghcnd_config: sources.ghcnd.clone(),
            gefs_config: sources.gefs.clone(),
            station_profiles: sources.weather_stations.clone(),
        }));
        let weather_backfill = Arc::new(WeatherBackfillWorker::new(Arc::clone(&weather_worker)));
        runner.spawn(TaskId::WeatherBackfillWorker, move |token| async move {
            weather_backfill.run(token).await;
        });
        runner.spawn(TaskId::WeatherIngestWorker, move |token| async move {
            weather_worker.run(token).await;
        });
        let weather_public = Arc::new(WeatherPublicIngestWorker::new(WeatherPublicIngestDeps {
            source_supervisor,
            cursors: Arc::clone(&self.infra.repos.domain_source_cursor)
                as Arc<dyn DomainSourceCursorRepository>,
            facts: weather_facts,
            hko,
            airnow,
            tornado,
            nhc,
            gistemp,
            nsidc,
            nws,
            hko_config: sources.hko_open_data.clone(),
            airnow_config: sources.airnow.clone(),
            tornado_config: sources.tornado.clone(),
            nhc_config: sources.nhc.clone(),
            gistemp_config: sources.nasa_gistemp.clone(),
            nsidc_config: sources.nsidc_sea_ice.clone(),
            nws_config: sources.nws_observation.clone(),
            bindings: sources.weather_vertical_bindings.clone(),
        }));
        runner.spawn(TaskId::WeatherPublicIngestWorker, move |token| async move {
            Box::pin(weather_public.run(token)).await;
        });
    }

    fn register_crypto_ingest_workers(
        &self,
        runner: &mut AppRunner,
        binance_budgets: BinanceBudgets,
        source_supervisor: Arc<DomainSourceSupervisor>,
        sources: ConnectedCryptoSources,
    ) {
        if let Some(worker) =
            self.build_kline_worker(binance_budgets, Arc::clone(&source_supervisor))
        {
            runner.spawn(TaskId::CryptoKlineIngestWorker, move |token| async move {
                if let Err(error) = worker.run(token).await {
                    tracing::error!(%error, "CryptoKlineIngestWorker exited with error");
                }
            });
        }
        let crypto_writer = Arc::new(ChFactWriter::<CryptoPriceReportRow>::new(
            Arc::clone(&self.infra.ch),
            Arc::clone(&self.infra.ch_write_manager),
            "quant_crypto_price_report",
        )) as Arc<dyn FactWriter<CryptoPriceReportRow>>;
        let rtds_worker = Arc::new(CryptoRtdsIngestWorker::new(CryptoRtdsIngestDeps {
            source_supervisor: Arc::clone(&source_supervisor),
            linkages: Arc::clone(&self.infra.repos.market_linkage)
                as Arc<dyn MarketLinkageRepository>,
            cursors: Arc::clone(&self.infra.repos.domain_source_cursor)
                as Arc<dyn DomainSourceCursorRepository>,
            projections: Arc::clone(&self.infra.repos.domain_projection)
                as Arc<dyn DomainProjectionRepository>,
            writer: Arc::clone(&crypto_writer),
            source: sources.rtds,
        }));
        runner.spawn(TaskId::CryptoRtdsIngestWorker, move |token| async move {
            rtds_worker.run(token).await;
        });
        let crypto_worker = Arc::new(CryptoLiveIngestWorker::new(CryptoLiveIngestDeps {
            source_supervisor,
            linkages: Arc::clone(&self.infra.repos.market_linkage)
                as Arc<dyn MarketLinkageRepository>,
            cursors: Arc::clone(&self.infra.repos.domain_source_cursor)
                as Arc<dyn DomainSourceCursorRepository>,
            projections: Arc::clone(&self.infra.repos.domain_projection)
                as Arc<dyn DomainProjectionRepository>,
            crypto_writer,
            binance: sources.binance,
            binance_usdm_futures: sources.binance_usdm_futures,
            #[cfg(feature = "domain-chainlink")]
            chainlink: sources.chainlink,
        }));
        runner.spawn(TaskId::CryptoLiveIngestWorker, move |token| async move {
            crypto_worker.run(token).await;
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

    fn build_tape_reconciler(&self) -> Option<Arc<TradeTapeReconciliationWorker>> {
        let config = &self.config.market_data.trade_tape_on_chain;
        config.enabled.then(|| {
            Arc::new(TradeTapeReconciliationWorker::new(
                Arc::new(ChNativeReadRepository::new(Arc::clone(&self.infra.ch))),
                Arc::new(ChFactWriter::<TradeTapeRow>::new(
                    Arc::clone(&self.infra.ch),
                    Arc::clone(&self.infra.ch_write_manager),
                    "quant_trade_tape",
                )),
                config.clone(),
            ))
        })
    }

    fn build_kline_worker(
        &self,
        binance_budgets: BinanceBudgets,
        source_supervisor: Arc<DomainSourceSupervisor>,
    ) -> Option<Arc<CryptoKlineIngestWorker>> {
        let sources_config = &self.config.domain_sources;
        if !sources_config.binance.enabled && !sources_config.binance_usdm_futures.enabled {
            return None;
        }

        let mut sources: Vec<Arc<dyn DomainDataSource>> = Vec::new();
        let mut binance_archive_sources = Vec::new();
        let mut poll_secs = u64::MAX;

        if sources_config.binance.enabled {
            let binance_budget = binance_budgets.spot?;
            let source = match BinanceKlineSource::connect_with_budget(
                sources_config.binance.clone(),
                binance_budget,
                Arc::clone(&self.compute),
            ) {
                Ok(source) => source,
                Err(error) => {
                    tracing::error!(%error, "Binance kline source unavailable");
                    return None;
                }
            };
            let source = Arc::new(source);
            sources.push(Arc::clone(&source) as Arc<dyn DomainDataSource>);
            binance_archive_sources.push(source);
            poll_secs = poll_secs.min(sources_config.binance.kline_poll_secs);
        }

        if sources_config.binance_usdm_futures.enabled {
            let budget = binance_budgets.usdm_futures?;
            let source = match BinanceKlineSource::connect_usdm_futures(
                sources_config.binance_usdm_futures.clone(),
                budget,
                Arc::clone(&self.compute),
            ) {
                Ok(source) => Arc::new(source),
                Err(error) => {
                    tracing::error!(%error, "Binance USD-M Futures kline source unavailable");
                    return None;
                }
            };
            sources.push(Arc::clone(&source) as Arc<dyn DomainDataSource>);
            binance_archive_sources.push(source);
            poll_secs = poll_secs.min(sources_config.binance_usdm_futures.kline_poll_secs);
        }

        if sources.is_empty() {
            return None;
        }

        Some(Arc::new(CryptoKlineIngestWorker::new(
            Arc::new(CryptoKlineIngestor::new(
                sources,
                binance_archive_sources,
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
            source_supervisor,
            poll_secs.max(1),
        )))
    }
}

fn build_binance_budget(
    config: &BinanceSourceConfig,
    market_label: &'static str,
) -> Option<BinanceRequestBudget> {
    if !config.enabled {
        return None;
    }
    BinanceRequestBudget::new(config)
        .map_err(|error| {
            tracing::error!(%error, market = market_label, "Binance request budget unavailable");
        })
        .ok()
}
