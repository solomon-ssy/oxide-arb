//! Public Polymarket RTDS Crypto ingestion.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use quant_pivot_api::rtds::{PolymarketRtdsSource, RtdsCryptoSource};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    clickhouse::CryptoPriceReportRow,
    domain::{
        data_plane::{CryptoPriceReport, DomainSourceCheckpoint},
        quant::LinkageOutcome,
    },
    enums::domain::LinkageSourceRole,
    types::{ContentHash, DomainInstrumentKey},
};
use quant_pivot_repository::traits::{
    DomainProjectionRepository, DomainSourceCursorRepository, FactWriter, MarketLinkageRepository,
};
use quant_pivot_research::linkage::rules;
use tokio::{task::JoinHandle, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::domain_source_supervisor::DomainSourceSupervisor;

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(30);
const RECONNECT_BACKOFF: Duration = Duration::from_secs(2);

/// Runs one exact-filter RTDS connection per governed instrument. The desired
/// set is capability-seeded and enriched by active linkages.
pub struct CryptoRtdsIngestWorker {
    source_supervisor: Arc<DomainSourceSupervisor>,
    linkages: Arc<dyn MarketLinkageRepository>,
    cursors: Arc<dyn DomainSourceCursorRepository>,
    projections: Arc<dyn DomainProjectionRepository>,
    writer: Arc<dyn FactWriter<CryptoPriceReportRow>>,
    source: Option<Arc<PolymarketRtdsSource>>,
}

pub struct CryptoRtdsIngestDeps {
    pub source_supervisor: Arc<DomainSourceSupervisor>,
    pub linkages: Arc<dyn MarketLinkageRepository>,
    pub cursors: Arc<dyn DomainSourceCursorRepository>,
    pub projections: Arc<dyn DomainProjectionRepository>,
    pub writer: Arc<dyn FactWriter<CryptoPriceReportRow>>,
    pub source: Option<Arc<PolymarketRtdsSource>>,
}

impl CryptoRtdsIngestWorker {
    #[must_use]
    pub fn new(deps: CryptoRtdsIngestDeps) -> Self {
        Self {
            source_supervisor: deps.source_supervisor,
            linkages: deps.linkages,
            cursors: deps.cursors,
            projections: deps.projections,
            writer: deps.writer,
            source: deps.source,
        }
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        if let Err(error) = self.source_supervisor.ensure_boot_reconciled().await {
            tracing::error!(%error, "RTDS ingest blocked: expected-source reconciliation failed");
            return;
        }
        let binance = Arc::clone(&self)
            .run_instrument_supervisor(RtdsCryptoSource::Binance, shutdown.child_token());
        let chainlink = Arc::clone(&self)
            .run_instrument_supervisor(RtdsCryptoSource::Chainlink, shutdown.child_token());
        tokio::join!(binance, chainlink);
    }

    async fn run_instrument_supervisor(
        self: Arc<Self>,
        source_kind: RtdsCryptoSource,
        shutdown: CancellationToken,
    ) {
        let mut tasks = BTreeMap::<DomainInstrumentKey, SourceTask>::new();
        let mut interval = tokio::time::interval(DISCOVERY_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            let desired = match self.desired_instruments(source_kind).await {
                Ok(desired) => desired,
                Err(error) => {
                    tracing::warn!(?source_kind, %error, "RTDS binding discovery failed");
                    continue;
                }
            };
            stop_removed_tasks(source_kind, &mut tasks, &desired).await;
            let Some(source) = self.source.as_ref() else {
                self.mark_unavailable(source_kind, &desired).await;
                continue;
            };
            for instrument in desired {
                if tasks.contains_key(&instrument) {
                    continue;
                }
                let cancel = shutdown.child_token();
                let child_cancel = cancel.clone();
                let worker = Arc::clone(&self);
                let source = Arc::clone(source);
                let task_instruments = BTreeSet::from([instrument.clone()]);
                let handle = tokio::spawn(async move {
                    worker
                        .run_source(source, source_kind, task_instruments, child_cancel)
                        .await;
                });
                tasks.insert(instrument.clone(), SourceTask { cancel, handle });
            }
        }
        stop_tasks(source_kind, tasks).await;
    }

    async fn desired_instruments(
        &self,
        source_kind: RtdsCryptoSource,
    ) -> QuantResult<BTreeSet<DomainInstrumentKey>> {
        let mut desired = static_rtds_instruments(source_kind);
        for row in self.linkages.latest_for_active_markets().await? {
            let linkage = row.into_domain();
            let LinkageOutcome::Resolved(resolved) = linkage.outcome else {
                continue;
            };
            for binding in resolved
                .source_bindings
                .iter()
                .filter(|binding| binding.role == LinkageSourceRole::LiveEvent)
            {
                if binding.source_id == source_kind.source_id() {
                    desired.insert(binding.instrument_key.clone());
                }
            }
        }
        Ok(desired)
    }

    async fn run_source(
        &self,
        source: Arc<PolymarketRtdsSource>,
        source_kind: RtdsCryptoSource,
        instruments: BTreeSet<DomainInstrumentKey>,
        shutdown: CancellationToken,
    ) {
        let mut gap_generations = self.bump_gaps(source_kind, &instruments).await;
        loop {
            if shutdown.is_cancelled() {
                return;
            }
            match self
                .run_session(
                    source.as_ref(),
                    source_kind,
                    &instruments,
                    &gap_generations,
                    &shutdown,
                )
                .await
            {
                Ok(()) => return,
                Err(error) => {
                    tracing::warn!(?source_kind, %error, "RTDS source session failed");
                    for instrument in &instruments {
                        if let Err(mark_error) = self
                            .source_supervisor
                            .mark_source_failed(
                                &source_kind.source_id(),
                                instrument,
                                error.to_string(),
                            )
                            .await
                        {
                            tracing::error!(?source_kind, %instrument, %mark_error, "failed to record RTDS source failure");
                        }
                    }
                    gap_generations = self.bump_gaps(source_kind, &instruments).await;
                    tokio::select! {
                        () = shutdown.cancelled() => return,
                        () = tokio::time::sleep(RECONNECT_BACKOFF) => {}
                    }
                }
            }
        }
    }

    async fn run_session(
        &self,
        source: &PolymarketRtdsSource,
        source_kind: RtdsCryptoSource,
        instruments: &BTreeSet<DomainInstrumentKey>,
        gap_generations: &BTreeMap<DomainInstrumentKey, u64>,
        shutdown: &CancellationToken,
    ) -> QuantResult<()> {
        let mut last = BTreeMap::new();
        for instrument in instruments {
            let cursor = self
                .cursors
                .find(&source_kind.source_id(), instrument)
                .await?;
            let checkpoint = match cursor.map(|cursor| cursor.checkpoint_json) {
                Some(DomainSourceCheckpoint::PolymarketRtds {
                    source_timestamp,
                    report_hash,
                    ..
                }) => Some((source_timestamp, report_hash)),
                Some(_) => {
                    return Err(QuantError::config(format!(
                        "RTDS cursor `{instrument}` contains a different checkpoint type"
                    )));
                }
                None => None,
            };
            last.insert(instrument.clone(), checkpoint);
        }
        let instruments = instruments.iter().cloned().collect::<Vec<_>>();
        let mut stream = source.stream(source_kind, &instruments).await?;
        loop {
            let report = tokio::select! {
                () = shutdown.cancelled() => {
                    stream.close().await?;
                    return Ok(());
                }
                report = stream.next_report() => report?,
            };
            let previous = last.get(&report.instrument_key).and_then(Option::as_ref);
            if !should_process(&report, previous) {
                continue;
            }
            let gap_generation = gap_generations
                .get(&report.instrument_key)
                .copied()
                .ok_or_else(|| QuantError::config("RTDS report has no gap-generation binding"))?;
            self.persist(report.clone(), gap_generation).await?;
            last.insert(
                report.instrument_key.clone(),
                Some((report.event_time, report.report_hash)),
            );
        }
    }

    async fn persist(&self, report: CryptoPriceReport, gap_generation: u64) -> QuantResult<()> {
        let source_id = report.source_id.clone();
        let instrument_key = report.instrument_key.clone();
        self.writer
            .write_batch(vec![report.to_clickhouse_row()])
            .await?;
        let checkpoint = DomainSourceCheckpoint::PolymarketRtds {
            source_timestamp: report.event_time,
            envelope_timestamp: report.published_at,
            report_hash: report.report_hash,
        };
        self.projections
            .apply_crypto_report(report, checkpoint, gap_generation, true)
            .await?;
        self.source_supervisor
            .mark_source_recovered(&source_id, &instrument_key)
            .await?;
        Ok(())
    }

    async fn bump_gaps(
        &self,
        source_kind: RtdsCryptoSource,
        instruments: &BTreeSet<DomainInstrumentKey>,
    ) -> BTreeMap<DomainInstrumentKey, u64> {
        let mut generations = BTreeMap::new();
        for instrument in instruments {
            let generation = self
                .projections
                .mark_crypto_source_gap(&source_kind.source_id(), instrument, Utc::now())
                .await
                .unwrap_or_else(|error| {
                    tracing::error!(?source_kind, %instrument, %error, "failed to persist RTDS source gap");
                    0
                });
            generations.insert(instrument.clone(), generation);
        }
        generations
    }

    async fn mark_unavailable(
        &self,
        source_kind: RtdsCryptoSource,
        instruments: &BTreeSet<DomainInstrumentKey>,
    ) {
        let _ = self.bump_gaps(source_kind, instruments).await;
    }
}

struct SourceTask {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

async fn stop_removed_tasks(
    source_kind: RtdsCryptoSource,
    tasks: &mut BTreeMap<DomainInstrumentKey, SourceTask>,
    desired: &BTreeSet<DomainInstrumentKey>,
) {
    let removed = tasks
        .keys()
        .filter(|instrument| !desired.contains(*instrument))
        .cloned()
        .collect::<Vec<_>>();
    for instrument in removed {
        if let Some(task) = tasks.remove(&instrument) {
            task.cancel.cancel();
            if let Err(error) = task.handle.await {
                tracing::warn!(?source_kind, %instrument, %error, "RTDS source task join failed");
            }
        }
    }
}

async fn stop_tasks(
    source_kind: RtdsCryptoSource,
    tasks: BTreeMap<DomainInstrumentKey, SourceTask>,
) {
    for task in tasks.values() {
        task.cancel.cancel();
    }
    for (instrument, task) in tasks {
        if let Err(error) = task.handle.await {
            tracing::warn!(?source_kind, %instrument, %error, "RTDS source shutdown join failed");
        }
    }
}

fn static_rtds_instruments(source_kind: RtdsCryptoSource) -> BTreeSet<DomainInstrumentKey> {
    rules()
        .iter()
        .filter(|rule| rule.public_rtds_supported())
        .map(|rule| match source_kind {
            RtdsCryptoSource::Binance => rule.rtds_binance_instrument(),
            RtdsCryptoSource::Chainlink => rule.rtds_chainlink_instrument(),
        })
        .collect()
}

fn should_process(
    report: &CryptoPriceReport,
    previous: Option<&(DateTime<Utc>, ContentHash)>,
) -> bool {
    previous.is_none_or(|(timestamp, hash)| {
        report.event_time > *timestamp
            || (report.event_time == *timestamp && report.report_hash != *hash)
    })
}

#[cfg(test)]
mod tests {
    use quant_pivot_api::rtds::RtdsCryptoSource;

    use super::static_rtds_instruments;

    #[test]
    fn public_rtds_sources_start_without_market_linkages() {
        let binance = static_rtds_instruments(RtdsCryptoSource::Binance);
        let chainlink = static_rtds_instruments(RtdsCryptoSource::Chainlink);
        assert_eq!(binance.len(), 4);
        assert_eq!(chainlink.len(), 4);
        assert!(
            binance
                .iter()
                .any(|instrument| instrument.as_str() == "RTDS:BINANCE:BTCUSDT")
        );
        assert!(
            chainlink
                .iter()
                .any(|instrument| instrument.as_str() == "RTDS:CHAINLINK:BTC-USD")
        );
    }
}
