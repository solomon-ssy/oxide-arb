//! Periodic producer for append-only CLOB market-info truth.

use std::{sync::Arc, time::Duration};

use futures_util::{StreamExt, stream};
use quant_pivot_api::clob::ClobClient;
use quant_pivot_error::QuantError;
use quant_pivot_repository::traits::{ClobMarketInfoRepository, MarketRepository};
use tokio_util::sync::CancellationToken;

const FETCH_CONCURRENCY: usize = 8;

pub struct ClobMarketInfoWorker {
    clob: Arc<ClobClient>,
    markets: Arc<dyn MarketRepository>,
    observations: Arc<dyn ClobMarketInfoRepository>,
    refresh_interval: Duration,
}

impl ClobMarketInfoWorker {
    pub const fn new(
        clob: Arc<ClobClient>,
        markets: Arc<dyn MarketRepository>,
        observations: Arc<dyn ClobMarketInfoRepository>,
        refresh_interval: Duration,
    ) -> Self {
        Self {
            clob,
            markets,
            observations,
            refresh_interval,
        }
    }

    pub async fn run(&self, token: CancellationToken) {
        let mut interval = tokio::time::interval(self.refresh_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = token.cancelled() => break,
                _ = interval.tick() => {
                    tokio::select! {
                        biased;
                        () = token.cancelled() => break,
                        () = self.refresh_once() => {}
                    }
                },
            }
        }
    }

    async fn refresh_once(&self) {
        let markets = match self.markets.find_active().await {
            Ok(markets) => markets,
            Err(error) => {
                tracing::error!(%error, "CLOB market-info refresh could not load active catalog");
                return;
            }
        };
        let clob = Arc::clone(&self.clob);
        let observations = Arc::clone(&self.observations);
        let market_ids = markets
            .iter()
            .map(|market| market.market_id.clone())
            .collect::<Vec<_>>();
        let results = stream::iter(market_ids)
            .map(move |market_id| {
                let clob = Arc::clone(&clob);
                let observations = Arc::clone(&observations);
                async move {
                    let observation = clob.clob_market_info_version(&market_id).await?;
                    observations
                        .insert_observation(observation)
                        .await
                        .map_err(QuantError::from)?;
                    Ok::<_, QuantError>(market_id)
                }
            })
            .buffer_unordered(FETCH_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        let mut succeeded = 0_u64;
        let mut failed = 0_u64;
        for result in results {
            match result {
                Ok(_) => succeeded += 1,
                Err(error) => {
                    failed += 1;
                    tracing::warn!(%error, "CLOB market-info observation failed");
                }
            }
        }
        tracing::info!(succeeded, failed, "CLOB market-info refresh complete");
    }
}
