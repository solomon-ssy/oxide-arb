//! Shared helpers for `ClickHouse` repository integration tests.

use std::{sync::Arc, time::Duration};

use quant_pivot_models::config::ClickHouseConfig;
use quant_pivot_repository::clickhouse::ChTimeseriesRepository;
use quant_pivot_storage::clickhouse::{ChWriteManager, ClickHousePool};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};
use tokio_util::sync::CancellationToken;

pub fn test_ch_config(port: u16) -> ClickHouseConfig {
    ClickHouseConfig {
        url: format!("http://localhost:{port}"),
        database: "default".into(),
        user: "default".into(),
        password: String::new(),
        batch_size: 10,
        flush_interval_secs: 1,
        max_concurrent_inserts: 4,
    }
}

pub async fn setup_timeseries_repo() -> (
    ChTimeseriesRepository,
    CancellationToken,
    ContainerAsync<GenericImage>,
) {
    let container = GenericImage::new("clickhouse/clickhouse-server", "24")
        .with_exposed_port(8123.into())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/ping")
                .with_port(8123.into())
                .with_expected_status_code(200u16),
        ))
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("ClickHouse container");
    let port = container.get_host_port_ipv4(8123).await.expect("port");
    let config = test_ch_config(port);
    let pool = ClickHousePool::connect(&config).await.expect("connect");
    pool.ensure_schema().await.expect("schema");

    let shutdown = CancellationToken::new();
    let write_manager = Arc::new(ChWriteManager::new(config.max_concurrent_inserts));
    let repo = ChTimeseriesRepository::new(
        pool.client().clone(),
        &config,
        write_manager,
        shutdown.clone(),
    );

    (repo, shutdown, container)
}
