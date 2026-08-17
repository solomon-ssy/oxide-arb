//! Periodic producer for append-only CLOB market-info truth.

use std::{sync::Arc, time::Duration};

use futures_util::{StreamExt, stream};
use quant_pivot_api::clob::ClobClient;
use quant_pivot_error::QuantError;
use quant_pivot_repository::traits::{ClobMarketInfoRepository, MarketRepository};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::{execution::terms_drift_wake::TermsDriftWake, observability::metrics_hub::MetricsHub};

const FETCH_CONCURRENCY: usize = 8;

pub struct ClobMarketInfoWorker {
    clob: Arc<ClobClient>,
    markets: Arc<dyn MarketRepository>,
    observations: Arc<dyn ClobMarketInfoRepository>,
    metrics: Arc<MetricsHub>,
    terms_drift_wake: TermsDriftWake,
    refresh_interval: Duration,
}

impl ClobMarketInfoWorker {
    pub const fn new(
        clob: Arc<ClobClient>,
        markets: Arc<dyn MarketRepository>,
        observations: Arc<dyn ClobMarketInfoRepository>,
        metrics: Arc<MetricsHub>,
        terms_drift_wake: TermsDriftWake,
        refresh_interval: Duration,
    ) -> Self {
        Self {
            clob,
            markets,
            observations,
            metrics,
            terms_drift_wake,
            refresh_interval,
        }
    }

    pub async fn run(&self, token: CancellationToken) {
        let mut interval = tokio::time::interval(self.refresh_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
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
                    let changed = observations
                        .latest(&market_id)
                        .await
                        .map_err(QuantError::from)?
                        .is_none_or(|current| current.fee_schedule() != observation.fee_schedule());
                    observations
                        .insert_observation(observation)
                        .await
                        .map_err(QuantError::from)?;
                    Ok::<_, QuantError>((market_id, changed))
                }
            })
            .buffer_unordered(FETCH_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        let mut succeeded = 0_u64;
        let mut failed = 0_u64;
        let mut changed_market_ids = Vec::new();
        for result in results {
            match result {
                Ok((market_id, changed)) => {
                    succeeded += 1;
                    if changed {
                        changed_market_ids.push(market_id);
                    }
                }
                Err(error) => {
                    failed += 1;
                    tracing::warn!(%error, "CLOB market-info observation failed");
                }
            }
        }
        let changed = u64::try_from(changed_market_ids.len()).unwrap_or(u64::MAX);
        self.metrics
            .record_maker_rebate_diagnostics("clob_terms_commit", "changed", changed);
        self.metrics.record_maker_rebate_diagnostics(
            "clob_terms_commit",
            "unchanged",
            succeeded.saturating_sub(changed),
        );
        self.terms_drift_wake.publish(changed_market_ids);
        tracing::info!(succeeded, failed, "CLOB market-info refresh complete");
    }
}
