//! Public Polymarket RTDS Crypto ingestion.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use chrono::Utc;
use quant_pivot_api::rtds::{PolymarketRtdsSource, RtdsCryptoSource};
use quant_pivot_error::{QuantError, QuantResult, infra::InfraError};
use quant_pivot_models::{
    clickhouse::CryptoPriceReportRow,
    domain::{
        data_plane::{CryptoPriceReport, DomainSourceCheckpoint},
        quant::{LinkageOutcome, MarketLinkage},
    },
    enums::domain::LinkageSourceRole,
    types::DomainInstrumentKey,
};
use quant_pivot_repository::traits::{
    DomainProjectionRepository, DomainSourceCursorRepository, MarketLinkageRepository,
};
use quant_pivot_research::linkage::rules;
use quant_pivot_storage::write::{DurableWriteTimeouts, DurableWriter};
use tokio::{
    task::{JoinError, JoinSet},
    time::MissedTickBehavior,
};
use tokio_util::sync::CancellationToken;

use super::{
    crypto_fact_persistence::{CryptoFactPersistence, PendingCryptoFact},
    domain_source_supervisor::DomainSourceSupervisor,
};

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(30);
const RECONNECT_BACKOFF: Duration = Duration::from_secs(2);

/// Runs one exact-filter RTDS connection per governed instrument. The desired
/// set is capability-seeded and enriched by active linkages.
pub struct CryptoRtdsIngestWorker {
    source_supervisor: Arc<DomainSourceSupervisor>,
    linkages: Arc<dyn MarketLinkageRepository>,
    cursors: Arc<dyn DomainSourceCursorRepository>,
    projections: Arc<dyn DomainProjectionRepository>,
    persistence: CryptoFactPersistence,
    source: Option<Arc<PolymarketRtdsSource>>,
}

pub struct CryptoRtdsIngestDeps {
    pub source_supervisor: Arc<DomainSourceSupervisor>,
    pub linkages: Arc<dyn MarketLinkageRepository>,
    pub cursors: Arc<dyn DomainSourceCursorRepository>,
    pub projections: Arc<dyn DomainProjectionRepository>,
    pub writer: Arc<DurableWriter<CryptoPriceReportRow>>,
    pub write_timeouts: DurableWriteTimeouts,
    pub source: Option<Arc<PolymarketRtdsSource>>,
}

impl CryptoRtdsIngestWorker {
    #[must_use]
    pub fn new(deps: CryptoRtdsIngestDeps) -> Self {
        let persistence = CryptoFactPersistence::new(
            Arc::clone(&deps.source_supervisor),
            Arc::clone(&deps.projections),
            deps.writer,
            deps.write_timeouts,
        );
        Self {
            source_supervisor: deps.source_supervisor,
            linkages: deps.linkages,
            cursors: deps.cursors,
            projections: deps.projections,
            persistence,
            source: deps.source,
        }
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) -> QuantResult<()> {
        self.source_supervisor.ensure_boot_reconciled().await?;
        let binance = Arc::clone(&self)
            .run_instrument_supervisor(RtdsCryptoSource::Binance, shutdown.child_token());
        let chainlink = Arc::clone(&self)
            .run_instrument_supervisor(RtdsCryptoSource::Chainlink, shutdown.child_token());
        tokio::try_join!(binance, chainlink)?;
        Ok(())
    }

    async fn run_instrument_supervisor(
        self: Arc<Self>,
        source_kind: RtdsCryptoSource,
        shutdown: CancellationToken,
    ) -> QuantResult<()> {
        let mut tasks = BTreeMap::<DomainInstrumentKey, CancellationToken>::new();
        let mut joins = JoinSet::new();
        let mut interval = tokio::time::interval(DISCOVERY_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                joined = joins.join_next(), if !joins.is_empty() => {
                    observe_source_task(
                        source_kind,
                        joined.ok_or(InfraError::ChannelClosed {
                            name: "crypto_rtds_source_tasks",
                        })?,
                        &mut tasks,
                        false,
                    )?;
                }
                _ = interval.tick() => {}
            }
            let desired = self.desired_instruments(source_kind).await?;
            stop_removed_tasks(&mut tasks, &desired);
            let Some(source) = self.source.as_ref() else {
                self.mark_unavailable(source_kind, &desired).await?;
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
                let completed_instrument = instrument.clone();
                joins.spawn(async move {
                    let result = worker
                        .run_source(source, source_kind, task_instruments, child_cancel)
                        .await;
                    (completed_instrument, result)
                });
                tasks.insert(instrument, cancel);
            }
        }
        for cancel in tasks.values() {
            cancel.cancel();
        }
        while let Some(joined) = joins.join_next().await {
            observe_source_task(source_kind, joined, &mut tasks, true)?;
        }
        Ok(())
    }

    async fn desired_instruments(
        &self,
        source_kind: RtdsCryptoSource,
    ) -> QuantResult<BTreeSet<DomainInstrumentKey>> {
        let mut desired = static_rtds_instruments(source_kind);
        for row in self.linkages.latest_for_active_markets().await? {
            let linkage = MarketLinkage::from(row);
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
    ) -> QuantResult<()> {
        for instrument in &instruments {
            self.source_supervisor
                .mark_source_failed(
                    &source_kind.source_id(),
                    instrument,
                    "RTDS source session is establishing continuity".to_owned(),
                )
                .await?;
        }
        let mut gap_generations = self.bump_gaps(source_kind, &instruments).await?;
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
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
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::warn!(?source_kind, %error, "RTDS source session failed");
                    for instrument in &instruments {
                        self.source_supervisor
                            .mark_source_failed(
                                &source_kind.source_id(),
                                instrument,
                                error.to_string(),
                            )
                            .await?;
                    }
                    gap_generations = self.bump_gaps(source_kind, &instruments).await?;
                    tokio::select! {
                        () = shutdown.cancelled() => return Ok(()),
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
                Some(checkpoint @ DomainSourceCheckpoint::PolymarketRtds { .. }) => {
                    Some(checkpoint)
                }
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
        let mut pending = VecDeque::<PendingCryptoFact>::new();
        let result = async {
            loop {
            let result = tokio::select! {
                biased;
                acknowledgement = self.persistence.acknowledge_front(&mut pending), if !pending.is_empty() => {
                    acknowledgement?;
                    continue;
                }
                () = shutdown.cancelled() => {
                    let drain_result = self.persistence.shutdown(&mut pending).await;
                    let close_result = stream.close().await;
                    drain_result?;
                    close_result?;
                    return Ok(());
                }
                report = stream.next_report() => report,
            };
            let report = match result {
                Ok(report) => report,
                Err(error) => {
                    self.persistence.drain(&mut pending).await?;
                    return Err(error);
                }
            };
            let previous = last.get(&report.instrument_key).and_then(Option::as_ref);
            if !should_process(&report, previous)? {
                continue;
            }
            let gap_generation = gap_generations
                .get(&report.instrument_key)
                .copied()
                .ok_or_else(|| QuantError::config("RTDS report has no gap-generation binding"))?;
            self.persistence
                .enqueue_ordered(report.clone(), gap_generation, &mut pending)
                .await?;
            last.insert(
                report.instrument_key.clone(),
                Some(
                    report
                        .checkpoint()
                        .map_err(|error| QuantError::config(error.to_string()))?,
                ),
            );
            }
        }
        .await;
        drop(pending);
        result
    }

    async fn bump_gaps(
        &self,
        source_kind: RtdsCryptoSource,
        instruments: &BTreeSet<DomainInstrumentKey>,
    ) -> QuantResult<BTreeMap<DomainInstrumentKey, u64>> {
        let mut generations = BTreeMap::new();
        for instrument in instruments {
            let generation = self
                .projections
                .mark_crypto_source_gap(&source_kind.source_id(), instrument, Utc::now())
                .await?;
            generations.insert(instrument.clone(), generation);
        }
        Ok(generations)
    }

    async fn mark_unavailable(
        &self,
        source_kind: RtdsCryptoSource,
        instruments: &BTreeSet<DomainInstrumentKey>,
    ) -> QuantResult<()> {
        for instrument in instruments {
            self.source_supervisor
                .mark_source_failed(
                    &source_kind.source_id(),
                    instrument,
                    "Polymarket RTDS source is unavailable".to_owned(),
                )
                .await?;
        }
        self.bump_gaps(source_kind, instruments).await?;
        Ok(())
    }
}

fn stop_removed_tasks(
    tasks: &mut BTreeMap<DomainInstrumentKey, CancellationToken>,
    desired: &BTreeSet<DomainInstrumentKey>,
) {
    let removed = tasks
        .keys()
        .filter(|instrument| !desired.contains(*instrument))
        .cloned()
        .collect::<Vec<_>>();
    for instrument in removed {
        if let Some(cancel) = tasks.remove(&instrument) {
            cancel.cancel();
        }
    }
}

fn observe_source_task(
    source_kind: RtdsCryptoSource,
    joined: Result<(DomainInstrumentKey, QuantResult<()>), JoinError>,
    tasks: &mut BTreeMap<DomainInstrumentKey, CancellationToken>,
    stopping: bool,
) -> QuantResult<()> {
    let (instrument, result) = joined.map_err(|error| InfraError::BlockingTaskJoin {
        detail: format!("RTDS {source_kind:?} source task failed: {error}"),
    })?;
    let planned = tasks.remove(&instrument).is_none() || stopping;
    result?;
    if !planned {
        return Err(InfraError::ChannelClosed {
            name: "crypto_rtds_source_task",
        }
        .into());
    }
    Ok(())
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
    previous: Option<&DomainSourceCheckpoint>,
) -> QuantResult<bool> {
    let Some(previous) = previous else {
        return Ok(true);
    };
    let incoming = report
        .checkpoint()
        .map_err(|error| QuantError::config(error.to_string()))?;
    match previous
        .compare_crypto(&incoming)
        .map_err(|error| QuantError::config(error.to_string()))?
    {
        Ordering::Greater => Ok(true),
        Ordering::Less => Ok(false),
        Ordering::Equal if previous.crypto_report_hash() == Some(report.report_hash) => Ok(false),
        Ordering::Equal => Err(QuantError::config(
            "RTDS source equivocated at one source/envelope checkpoint",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Duration, Utc};
    use quant_pivot_api::rtds::RtdsCryptoSource;
    use quant_pivot_models::{
        domain::data_plane::CryptoPriceReport,
        types::{ContentHash, DomainInstrumentKey, DomainSourceId, Usd},
    };
    use rust_decimal_macros::dec;

    use tokio_util::sync::CancellationToken;

    use super::{observe_source_task, should_process, static_rtds_instruments};

    #[test]
    fn public_rtds_without_linkages() {
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

    #[test]
    fn checkpoint_uses_source_order() {
        let now = Utc::now();
        let current_hash = hash('a');
        let current = report(now, now, current_hash)
            .checkpoint()
            .expect("current checkpoint");
        assert!(
            !should_process(&report(now, now, current_hash), Some(&current)).expect("exact replay")
        );
        assert!(
            should_process(
                &report(now, now + Duration::milliseconds(1), hash('b')),
                Some(&current),
            )
            .expect("newer envelope")
        );
        assert!(
            !should_process(
                &report(now - Duration::milliseconds(1), now, hash('b')),
                Some(&current),
            )
            .expect("stale source timestamp")
        );
        assert!(should_process(&report(now, now, hash('b')), Some(&current)).is_err());
    }

    #[test]
    fn unexpected_task_fails() {
        let instrument = DomainInstrumentKey::new("RTDS:BINANCE:BTCUSDT");
        let mut tasks = BTreeMap::from([(instrument.clone(), CancellationToken::new())]);
        assert!(
            observe_source_task(
                RtdsCryptoSource::Binance,
                Ok((instrument, Ok(()))),
                &mut tasks,
                false,
            )
            .is_err()
        );
    }

    fn report(
        event_time: DateTime<Utc>,
        published_at: DateTime<Utc>,
        report_hash: ContentHash,
    ) -> CryptoPriceReport {
        CryptoPriceReport {
            source_id: DomainSourceId::polymarket_rtds_binance(),
            instrument_key: DomainInstrumentKey::new("RTDS:BINANCE:BTCUSDT"),
            source_sequence: u64::try_from(event_time.timestamp_millis()).expect("timestamp"),
            price: Usd::new(dec!(100)),
            quantity: None,
            event_time,
            published_at,
            available_at: published_at,
            valid_from: None,
            observations_timestamp: None,
            expires_at: None,
            report_hash,
            raw_report: "test".to_owned(),
        }
    }

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }
}
