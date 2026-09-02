//! Crypto source-native live ingestion and gap recovery.

#[cfg(feature = "domain-chainlink")]
use std::{cmp::Ordering, future::Future, slice};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration as StdDuration,
};

use chrono::{NaiveDate, Utc};
use quant_pivot_api::binance::BinanceAggTradeSource;
#[cfg(feature = "domain-chainlink")]
pub(crate) use quant_pivot_api::chainlink::ChainlinkDataStreamsSource;
use quant_pivot_error::{QuantError, QuantResult, infra::InfraError};
use quant_pivot_models::{
    clickhouse::CryptoPriceReportRow,
    domain::{
        data_plane::{CryptoPriceReport, DomainSourceCheckpoint},
        quant::{LinkageOutcome, MarketLinkage, MarketSubject},
    },
    enums::domain::{BinanceMarketSegment, LinkageSourceRole},
    types::{BinanceSymbol, ChainlinkFeedKey, DomainInstrumentKey, DomainSourceId},
};
use quant_pivot_repository::traits::{
    DomainProjectionRepository, DomainSourceCursorRepository, MarketLinkageRepository,
};
use quant_pivot_research::linkage::rules;
use quant_pivot_storage::write::{DurableWriteTimeouts, DurableWriter};
use tokio::{
    task::{JoinError, JoinSet},
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::app::{
    crypto_fact_persistence::{CryptoFactPersistence, PendingCryptoFact},
    domain_source_supervisor::DomainSourceSupervisor,
};

const DISCOVERY_INTERVAL: StdDuration = StdDuration::from_secs(30);
const RECONNECT_BACKOFF: StdDuration = StdDuration::from_secs(2);
const BINANCE_PAGE_SIZE: u16 = 1_000;

#[derive(Default)]
struct CryptoBindings {
    binance: BTreeMap<DomainInstrumentKey, BinanceSymbol>,
    binance_usdm_futures: BTreeMap<DomainInstrumentKey, BinanceSymbol>,
    chainlink: BTreeMap<DomainInstrumentKey, ChainlinkFeedKey>,
}

pub struct CryptoLiveIngestWorker {
    source_supervisor: Arc<DomainSourceSupervisor>,
    linkages: Arc<dyn MarketLinkageRepository>,
    cursors: Arc<dyn DomainSourceCursorRepository>,
    projections: Arc<dyn DomainProjectionRepository>,
    persistence: CryptoFactPersistence,
    binance: Option<Arc<BinanceAggTradeSource>>,
    binance_usdm_futures: Option<Arc<BinanceAggTradeSource>>,
    #[cfg(feature = "domain-chainlink")]
    chainlink: Option<Arc<ChainlinkDataStreamsSource>>,
}

pub struct CryptoLiveIngestDeps {
    pub source_supervisor: Arc<DomainSourceSupervisor>,
    pub linkages: Arc<dyn MarketLinkageRepository>,
    pub cursors: Arc<dyn DomainSourceCursorRepository>,
    pub projections: Arc<dyn DomainProjectionRepository>,
    pub crypto_writer: Arc<DurableWriter<CryptoPriceReportRow>>,
    pub write_timeouts: DurableWriteTimeouts,
    pub binance: Option<Arc<BinanceAggTradeSource>>,
    pub binance_usdm_futures: Option<Arc<BinanceAggTradeSource>>,
    #[cfg(feature = "domain-chainlink")]
    pub chainlink: Option<Arc<ChainlinkDataStreamsSource>>,
}

impl CryptoLiveIngestWorker {
    #[must_use]
    pub fn new(deps: CryptoLiveIngestDeps) -> Self {
        let persistence = CryptoFactPersistence::new(
            Arc::clone(&deps.source_supervisor),
            Arc::clone(&deps.projections),
            deps.crypto_writer,
            deps.write_timeouts,
        );
        Self {
            source_supervisor: deps.source_supervisor,
            linkages: deps.linkages,
            cursors: deps.cursors,
            projections: deps.projections,
            persistence,
            binance: deps.binance,
            binance_usdm_futures: deps.binance_usdm_futures,
            #[cfg(feature = "domain-chainlink")]
            chainlink: deps.chainlink,
        }
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) -> QuantResult<()> {
        self.source_supervisor.ensure_boot_reconciled().await?;
        let binance = Arc::clone(&self).run_binance_supervisor(
            BinanceMarketSegment::Spot,
            self.binance.clone(),
            shutdown.child_token(),
        );
        let binance_usdm_futures = Arc::clone(&self).run_binance_supervisor(
            BinanceMarketSegment::UsdmFutures,
            self.binance_usdm_futures.clone(),
            shutdown.child_token(),
        );
        #[cfg(feature = "domain-chainlink")]
        {
            let chainlink = Arc::clone(&self).run_chainlink_supervisor(shutdown.child_token());
            tokio::try_join!(binance, binance_usdm_futures, chainlink)?;
        }
        #[cfg(not(feature = "domain-chainlink"))]
        tokio::try_join!(binance, binance_usdm_futures)?;
        Ok(())
    }

    async fn discover(&self) -> QuantResult<CryptoBindings> {
        let rows = self.linkages.latest_for_active_markets().await?;
        let mut bindings = CryptoBindings::default();
        for rule in rules() {
            match rule.binance_market {
                BinanceMarketSegment::Spot => {
                    bindings
                        .binance
                        .insert(rule.binance_event_instrument(), rule.symbol());
                }
                BinanceMarketSegment::UsdmFutures => {
                    bindings
                        .binance_usdm_futures
                        .insert(rule.binance_event_instrument(), rule.symbol());
                }
            }
            if !rule.public_rtds_supported() {
                bindings
                    .chainlink
                    .insert(rule.chainlink_instrument(), rule.feed());
            }
        }
        for row in rows {
            let linkage = MarketLinkage::from(row);
            Self::add_discovered_linkage(&mut bindings, linkage)?;
        }
        Ok(bindings)
    }

    fn add_discovered_linkage(
        bindings: &mut CryptoBindings,
        linkage: MarketLinkage,
    ) -> QuantResult<()> {
        let LinkageOutcome::Resolved(resolved) = linkage.outcome else {
            return Ok(());
        };
        let MarketSubject::Crypto(_) = &resolved.subject else {
            return Ok(());
        };
        let live = resolved
            .source_bindings
            .iter()
            .find(|binding| binding.role == LinkageSourceRole::LiveEvent);
        match live {
            Some(source) if source.source_id == DomainSourceId::binance_agg_trade() => {
                let symbol = source.instrument_key.binance_agg_symbol().ok_or_else(|| {
                    QuantError::config("invalid Binance live-event linkage instrument")
                })?;
                bindings
                    .binance
                    .insert(source.instrument_key.clone(), symbol);
            }
            Some(source) if source.source_id == DomainSourceId::binance_futures_trade() => {
                let symbol = source
                    .instrument_key
                    .binance_futures_symbol()
                    .ok_or_else(|| {
                        QuantError::config(
                            "invalid Binance USD-M Futures live-event linkage instrument",
                        )
                    })?;
                bindings
                    .binance_usdm_futures
                    .insert(source.instrument_key.clone(), symbol);
            }
            Some(source) if source.source_id == DomainSourceId::chainlink_data_streams() => {
                let feed = source.instrument_key.as_chainlink_feed().ok_or_else(|| {
                    QuantError::config("invalid Chainlink live-event linkage instrument")
                })?;
                bindings
                    .chainlink
                    .insert(source.instrument_key.clone(), feed);
            }
            Some(source)
                if source.source_id == DomainSourceId::polymarket_rtds_binance()
                    || source.source_id == DomainSourceId::polymarket_rtds_chainlink() => {}
            None => {
                return Err(QuantError::config(format!(
                    "active Crypto linkage {} has no live-event source binding",
                    linkage.market_id
                )));
            }
            Some(_) => {
                return Err(QuantError::config(format!(
                    "active Crypto linkage {} has an unsupported live-event source",
                    linkage.market_id
                )));
            }
        }
        Ok(())
    }

    async fn run_binance_supervisor(
        self: Arc<Self>,
        market: BinanceMarketSegment,
        source: Option<Arc<BinanceAggTradeSource>>,
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
                        "Binance live source task",
                        joined.ok_or(InfraError::ChannelClosed {
                            name: "crypto_binance_source_tasks",
                        })?,
                        &mut tasks,
                        false,
                    )?;
                }
                _ = interval.tick() => {}
            }
            let bindings = self.discover().await?;
            let desired = match market {
                BinanceMarketSegment::Spot => bindings.binance,
                BinanceMarketSegment::UsdmFutures => bindings.binance_usdm_futures,
            };
            stop_removed(&mut tasks, &desired);
            let Some(source) = source.as_ref() else {
                for instrument in desired.keys() {
                    self.mark_crypto_unavailable(instrument).await?;
                }
                continue;
            };
            for (instrument, symbol) in desired {
                if tasks.contains_key(&instrument) {
                    continue;
                }
                let cancel = shutdown.child_token();
                let child_cancel = cancel.clone();
                let worker = Arc::clone(&self);
                let source = Arc::clone(source);
                let task_instrument = instrument.clone();
                let completed_instrument = instrument.clone();
                let source_id = market.trade_source();
                joins.spawn(async move {
                    let result = worker
                        .run_binance_symbol(
                            source,
                            source_id,
                            task_instrument,
                            symbol,
                            child_cancel,
                        )
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
            observe_source_task("Binance live source task", joined, &mut tasks, true)?;
        }
        Ok(())
    }

    async fn run_binance_symbol(
        &self,
        source: Arc<BinanceAggTradeSource>,
        source_id: DomainSourceId,
        instrument: DomainInstrumentKey,
        symbol: BinanceSymbol,
        shutdown: CancellationToken,
    ) -> QuantResult<()> {
        self.source_supervisor
            .mark_source_failed(
                &source_id,
                &instrument,
                "Binance source session is establishing continuity".to_owned(),
            )
            .await?;
        let mut gap_generation = self
            .projections
            .mark_crypto_source_gap(&source_id, &instrument, Utc::now())
            .await?;
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            match self
                .run_binance_session(
                    source.as_ref(),
                    &source_id,
                    &instrument,
                    &symbol,
                    gap_generation,
                    &shutdown,
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::warn!(%instrument, %error, "Binance aggTrade session failed");
                    self.source_supervisor
                        .mark_source_failed(&source_id, &instrument, error.to_string())
                        .await?;
                    gap_generation = self
                        .projections
                        .mark_crypto_source_gap(&source_id, &instrument, Utc::now())
                        .await?;
                    tokio::select! {
                        () = shutdown.cancelled() => return Ok(()),
                        () = tokio::time::sleep(RECONNECT_BACKOFF) => {}
                    }
                }
            }
        }
    }

    async fn run_binance_session(
        &self,
        source: &BinanceAggTradeSource,
        source_id: &DomainSourceId,
        instrument: &DomainInstrumentKey,
        symbol: &BinanceSymbol,
        mut gap_generation: u64,
        shutdown: &CancellationToken,
    ) -> QuantResult<()> {
        source.validate_system_clock().await?;
        let mut expected = self
            .resume_binance_sequence(source, source_id, instrument, symbol, gap_generation)
            .await?;
        let mut stream = source.stream(symbol).await?;
        let mut next_clock_check = Instant::now() + StdDuration::from_secs(30);
        let recovery_poll_interval = source.recovery_poll_interval();
        let mut next_recovery = Instant::now() + recovery_poll_interval;
        let mut pending = VecDeque::<PendingCryptoFact>::new();
        let result = async {
            loop {
            self.refresh_clock(source, &mut next_clock_check, &mut pending)
                .await?;
            let (report, planned_rotation) = if stream.rotation_due() {
                self.persistence.drain(&mut pending).await?;
                match source.stream(symbol).await {
                    Ok(mut replacement) => {
                        let result = tokio::select! {
                            biased;
                            acknowledgement = self.persistence.acknowledge_front(&mut pending), if !pending.is_empty() => {
                                acknowledgement?;
                                continue;
                            }
                            () = shutdown.cancelled() => {
                                self.persistence.shutdown(&mut pending).await?;
                                return Ok(());
                            }
                            result = replacement.next_report() => result,
                            () = tokio::time::sleep(StdDuration::from_secs(10)) => {
                                tracing::warn!(%instrument, "Binance overlap rotation produced no first report");
                                continue;
                            }
                        };
                        let report = self.require_stream_report(result, &mut pending).await?;
                        stream = replacement;
                        (report, true)
                    }
                    Err(error) => {
                        tracing::warn!(%instrument, %error, "Binance overlap rotation connection failed");
                        let result = tokio::select! {
                            biased;
                            acknowledgement = self.persistence.acknowledge_front(&mut pending), if !pending.is_empty() => {
                                acknowledgement?;
                                continue;
                            }
                            () = shutdown.cancelled() => {
                                self.persistence.shutdown(&mut pending).await?;
                                return Ok(());
                            }
                            result = stream.next_report() => result,
                            () = tokio::time::sleep(StdDuration::from_secs(1)) => continue,
                        };
                        let report = self.require_stream_report(result, &mut pending).await?;
                        (report, false)
                    }
                }
            } else {
                let result = tokio::select! {
                    biased;
                    acknowledgement = self.persistence.acknowledge_front(&mut pending), if !pending.is_empty() => {
                        acknowledgement?;
                        continue;
                    }
                    () = shutdown.cancelled() => {
                        self.persistence.shutdown(&mut pending).await?;
                        return Ok(());
                    }
                    result = stream.next_report() => result,
                    () = tokio::time::sleep_until(next_recovery) => {
                        self.persistence.drain(&mut pending).await?;
                        expected = self
                            .recover_binance_frontier(
                                source,
                                source_id,
                                instrument,
                                symbol,
                                expected,
                                gap_generation,
                            )
                            .await?;
                        next_recovery = tokio::time::Instant::now() + recovery_poll_interval;
                        continue;
                    },
                };
                let report = self.require_stream_report(result, &mut pending).await?;
                (report, false)
            };
            next_recovery = Instant::now() + recovery_poll_interval;
            if let Some(next_id) = expected {
                if report.source_sequence < next_id {
                    if next_id.checked_sub(1) == Some(report.source_sequence) {
                        self.persistence
                            .enqueue_ordered(report, gap_generation, &mut pending)
                            .await?;
                    }
                    continue;
                }
                if report.source_sequence > next_id {
                    self.persistence.drain(&mut pending).await?;
                    if !planned_rotation {
                        self.source_supervisor
                            .mark_source_failed(
                                source_id,
                                instrument,
                                format!(
                                    "Binance sequence gap: expected {next_id}, received {}",
                                    report.source_sequence
                                ),
                            )
                            .await?;
                        gap_generation = self
                            .projections
                            .mark_crypto_source_gap(source_id, instrument, Utc::now())
                            .await?;
                    }
                    expected = Some(
                        self.recover_binance(
                            source,
                            symbol,
                            next_id,
                            Some(report.source_sequence),
                            gap_generation,
                        )
                        .await?,
                    );
                }
            }
            if expected.is_none_or(|next_id| report.source_sequence >= next_id) {
                self.persistence
                    .enqueue_ordered(report.clone(), gap_generation, &mut pending)
                    .await?;
                expected = Some(next_sequence(report.source_sequence)?);
            }
            }
        }
        .await;
        drop(pending);
        result
    }

    async fn require_stream_report(
        &self,
        result: QuantResult<CryptoPriceReport>,
        pending: &mut VecDeque<PendingCryptoFact>,
    ) -> QuantResult<CryptoPriceReport> {
        match result {
            Ok(report) => Ok(report),
            Err(error) => {
                self.persistence.drain(pending).await?;
                Err(error)
            }
        }
    }

    async fn refresh_clock(
        &self,
        source: &BinanceAggTradeSource,
        next_clock_check: &mut Instant,
        pending: &mut VecDeque<PendingCryptoFact>,
    ) -> QuantResult<()> {
        if Instant::now() >= *next_clock_check {
            self.persistence.drain(pending).await?;
            source.validate_system_clock().await?;
            *next_clock_check = Instant::now() + StdDuration::from_secs(30);
        }
        Ok(())
    }

    async fn resume_binance_sequence(
        &self,
        source: &BinanceAggTradeSource,
        source_id: &DomainSourceId,
        instrument: &DomainInstrumentKey,
        symbol: &BinanceSymbol,
        gap_generation: u64,
    ) -> QuantResult<Option<u64>> {
        let cursor = self.cursors.find(source_id, instrument).await?;
        let (mut expected, archive_from) = match cursor.map(|cursor| cursor.checkpoint_json) {
            Some(DomainSourceCheckpoint::BinanceAggTrade {
                aggregate_trade_id,
                event_time,
            }) => (
                Some(next_sequence(aggregate_trade_id)?),
                Some(event_time.date_naive()),
            ),
            Some(_) => {
                return Err(QuantError::config(
                    "Binance aggTrade cursor contains a different source checkpoint type",
                ));
            }
            None => {
                return self
                    .bootstrap_binance_frontier(
                        source,
                        source_id,
                        instrument,
                        symbol,
                        gap_generation,
                    )
                    .await;
            }
        };
        if let Some(from_id) = expected {
            let archive_recovered = self
                .recover_binance_archive(
                    source,
                    symbol,
                    from_id,
                    archive_from.ok_or_else(|| {
                        QuantError::config("Binance cursor lacks archive start date")
                    })?,
                    gap_generation,
                )
                .await?;
            expected = Some(
                self.recover_binance(source, symbol, archive_recovered, None, gap_generation)
                    .await?,
            );
        }
        Ok(expected)
    }

    async fn recover_binance_frontier(
        &self,
        source: &BinanceAggTradeSource,
        source_id: &DomainSourceId,
        instrument: &DomainInstrumentKey,
        symbol: &BinanceSymbol,
        expected: Option<u64>,
        gap_generation: u64,
    ) -> QuantResult<Option<u64>> {
        match expected {
            Some(from_id) => Ok(Some(
                self.recover_binance(source, symbol, from_id, None, gap_generation)
                    .await?,
            )),
            None => {
                self.bootstrap_binance_frontier(
                    source,
                    source_id,
                    instrument,
                    symbol,
                    gap_generation,
                )
                .await
            }
        }
    }

    async fn bootstrap_binance_frontier(
        &self,
        source: &BinanceAggTradeSource,
        source_id: &DomainSourceId,
        instrument: &DomainInstrumentKey,
        symbol: &BinanceSymbol,
        gap_generation: u64,
    ) -> QuantResult<Option<u64>> {
        let Some(report) = source.latest(symbol, Utc::now()).await? else {
            return Ok(None);
        };
        if report.source_id != *source_id || report.instrument_key != *instrument {
            return Err(QuantError::config(format!(
                "Binance latest report binding {}/{} does not match {source_id}/{instrument}",
                report.source_id, report.instrument_key
            )));
        }
        let next = next_sequence(report.source_sequence)?;
        self.persistence
            .persist_batch(vec![report], gap_generation)
            .await?;
        Ok(Some(next))
    }

    async fn recover_binance_archive(
        &self,
        source: &BinanceAggTradeSource,
        symbol: &BinanceSymbol,
        mut from_id: u64,
        mut date: NaiveDate,
        gap_generation: u64,
    ) -> QuantResult<u64> {
        let today = Utc::now().date_naive();
        while date < today {
            let Some(mut reports) = source.recover_archive_day(symbol, date, Utc::now()).await?
            else {
                break;
            };
            while let Some(batch) = reports.next_batch().await? {
                let mut pending = Vec::with_capacity(batch.len());
                for report in batch {
                    if report.source_sequence < from_id {
                        continue;
                    }
                    if report.source_sequence != from_id {
                        return Err(QuantError::config(format!(
                            "Binance archive recovery gap: expected {from_id}, got {} on {date}",
                            report.source_sequence
                        )));
                    }
                    from_id = next_sequence(from_id)?;
                    pending.push(report);
                }
                self.persistence
                    .persist_batch(pending, gap_generation)
                    .await?;
            }
            date = date
                .succ_opt()
                .ok_or_else(|| QuantError::config("Binance archive date overflow"))?;
        }
        Ok(from_id)
    }

    async fn recover_binance(
        &self,
        source: &BinanceAggTradeSource,
        symbol: &BinanceSymbol,
        mut from_id: u64,
        stop_before: Option<u64>,
        gap_generation: u64,
    ) -> QuantResult<u64> {
        loop {
            let reports = source
                .recover_from(symbol, from_id, BINANCE_PAGE_SIZE, Utc::now())
                .await?;
            if reports.is_empty() {
                return Ok(from_id);
            }
            let page_len = reports.len();
            let mut pending = Vec::with_capacity(page_len);
            for report in reports {
                if stop_before.is_some_and(|limit| report.source_sequence >= limit) {
                    self.persistence
                        .persist_batch(pending, gap_generation)
                        .await?;
                    return Ok(from_id);
                }
                if report.source_sequence != from_id {
                    return Err(QuantError::config(format!(
                        "Binance aggTrade recovery gap: expected {from_id}, got {}",
                        report.source_sequence
                    )));
                }
                pending.push(report);
                from_id = next_sequence(from_id)?;
            }
            self.persistence
                .persist_batch(pending, gap_generation)
                .await?;
            if page_len < usize::from(BINANCE_PAGE_SIZE) {
                return Ok(from_id);
            }
        }
    }

    #[cfg(feature = "domain-chainlink")]
    async fn run_chainlink_supervisor(
        self: Arc<Self>,
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
                        "Chainlink live source task",
                        joined.ok_or(InfraError::ChannelClosed {
                            name: "crypto_chainlink_source_tasks",
                        })?,
                        &mut tasks,
                        false,
                    )?;
                }
                _ = interval.tick() => {}
            }
            let desired = self.discover().await?.chainlink;
            stop_removed(&mut tasks, &desired);
            let Some(source) = self.chainlink.as_ref() else {
                for instrument in desired.keys() {
                    self.mark_crypto_unavailable(instrument).await?;
                }
                continue;
            };
            for (instrument, feed) in desired {
                if tasks.contains_key(&instrument) {
                    continue;
                }
                if !source.instruments().contains(&instrument) {
                    self.mark_crypto_unavailable(&instrument).await?;
                    tracing::error!(%instrument, "active Chainlink feed is not configured");
                    continue;
                }
                let cancel = shutdown.child_token();
                let child_cancel = cancel.clone();
                let worker = Arc::clone(&self);
                let source = Arc::clone(source);
                let task_instrument = instrument.clone();
                let completed_instrument = instrument.clone();
                joins.spawn(async move {
                    let result = worker
                        .run_chainlink_feed(source, task_instrument, feed, child_cancel)
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
            observe_source_task("Chainlink live source task", joined, &mut tasks, true)?;
        }
        Ok(())
    }

    #[cfg(feature = "domain-chainlink")]
    async fn run_chainlink_feed(
        &self,
        source: Arc<ChainlinkDataStreamsSource>,
        instrument: DomainInstrumentKey,
        feed: ChainlinkFeedKey,
        shutdown: CancellationToken,
    ) -> QuantResult<()> {
        self.source_supervisor
            .mark_source_failed(
                &DomainSourceId::chainlink_data_streams(),
                &instrument,
                "Chainlink source session is establishing continuity".to_owned(),
            )
            .await?;
        let mut gap_generation = self
            .projections
            .mark_crypto_source_gap(
                &DomainSourceId::chainlink_data_streams(),
                &instrument,
                Utc::now(),
            )
            .await?;
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            match self
                .run_chainlink_session(
                    source.as_ref(),
                    &instrument,
                    &feed,
                    gap_generation,
                    &shutdown,
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::warn!(%instrument, %error, "Chainlink Data Streams session failed");
                    self.source_supervisor
                        .mark_source_failed(
                            &DomainSourceId::chainlink_data_streams(),
                            &instrument,
                            error.to_string(),
                        )
                        .await?;
                    gap_generation = self
                        .projections
                        .mark_crypto_source_gap(
                            &DomainSourceId::chainlink_data_streams(),
                            &instrument,
                            Utc::now(),
                        )
                        .await?;
                    tokio::select! {
                        () = shutdown.cancelled() => return Ok(()),
                        () = tokio::time::sleep(RECONNECT_BACKOFF) => {}
                    }
                }
            }
        }
    }

    #[cfg(feature = "domain-chainlink")]
    async fn run_chainlink_session(
        &self,
        source: &ChainlinkDataStreamsSource,
        instrument: &DomainInstrumentKey,
        feed: &ChainlinkFeedKey,
        gap_generation: u64,
        shutdown: &CancellationToken,
    ) -> QuantResult<()> {
        source.validate_system_clock().await?;
        let cursor = self
            .cursors
            .find(&DomainSourceId::chainlink_data_streams(), instrument)
            .await?;
        let mut last = match cursor.map(|cursor| cursor.checkpoint_json) {
            Some(checkpoint @ DomainSourceCheckpoint::ChainlinkDataStreams { .. }) => {
                Some(checkpoint)
            }
            Some(_) => {
                return Err(QuantError::config(
                    "Chainlink cursor contains a different source checkpoint type",
                ));
            }
            None => None,
        };
        if let Some(DomainSourceCheckpoint::ChainlinkDataStreams {
            observations_timestamp,
            ..
        }) = &last
        {
            let start = u128::try_from(observations_timestamp.timestamp()).map_err(|error| {
                QuantError::config(format!("negative Chainlink checkpoint timestamp: {error}"))
            })?;
            last = catch_up_chainlink_pages(
                start,
                source.rest_page_limit(),
                last,
                |page_start| source.reports_page(feed, page_start, Utc::now()),
                |reports| self.persistence.persist_batch(reports, gap_generation),
            )
            .await?;
        }
        let mut stream = source.stream(slice::from_ref(feed)).await?;
        stream.listen().await.map_err(|error| {
            QuantError::config(format!("Chainlink stream listen failed: {error}"))
        })?;
        let mut clock_check = tokio::time::interval(StdDuration::from_secs(30));
        clock_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
        clock_check.tick().await;
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
                    let close_result = stream.close().await.map_err(|error| {
                        QuantError::config(format!("Chainlink stream close failed: {error}"))
                    });
                    drain_result?;
                    close_result?;
                    return Ok(());
                }
                _ = clock_check.tick() => {
                    self.persistence.drain(&mut pending).await?;
                    source.validate_system_clock().await?;
                    continue;
                }
                report = source.next_report(&mut stream, Utc::now()) => report,
            };
            let report = match result {
                Ok(report) => report,
                Err(error) => {
                    self.persistence.drain(&mut pending).await?;
                    return Err(error);
                }
            };
            if should_process_crypto(&report, last.as_ref())? {
                self.persistence
                    .enqueue_ordered(report.clone(), gap_generation, &mut pending)
                    .await?;
                last = Some(
                    report
                        .checkpoint()
                        .map_err(|error| QuantError::config(error.to_string()))?,
                );
            }
            }
        }
        .await;
        drop(pending);
        result
    }

    async fn mark_crypto_unavailable(&self, instrument: &DomainInstrumentKey) -> QuantResult<()> {
        let Some(source_id) = instrument.source_id() else {
            return Err(QuantError::config(format!(
                "Crypto instrument `{instrument}` has no source identity"
            )));
        };
        self.source_supervisor
            .mark_source_failed(
                &source_id,
                instrument,
                "configured Crypto source is unavailable".to_owned(),
            )
            .await?;
        self.projections
            .mark_crypto_source_gap(&source_id, instrument, Utc::now())
            .await?;
        Ok(())
    }
}

#[cfg(feature = "domain-chainlink")]
fn should_process_crypto(
    report: &CryptoPriceReport,
    last: Option<&DomainSourceCheckpoint>,
) -> QuantResult<bool> {
    let Some(last) = last else {
        return Ok(true);
    };
    let incoming = report
        .checkpoint()
        .map_err(|error| QuantError::config(error.to_string()))?;
    match last
        .compare_crypto(&incoming)
        .map_err(|error| QuantError::config(error.to_string()))?
    {
        Ordering::Greater => Ok(true),
        Ordering::Less => Ok(false),
        Ordering::Equal if last.crypto_report_hash() == Some(report.report_hash) => Ok(false),
        Ordering::Equal => Err(QuantError::config(
            "Chainlink source equivocated at one observations timestamp",
        )),
    }
}

#[cfg(feature = "domain-chainlink")]
async fn catch_up_chainlink_pages<Fetch, FetchFuture, Persist, PersistFuture>(
    mut start: u128,
    page_limit: usize,
    mut last: Option<DomainSourceCheckpoint>,
    mut fetch: Fetch,
    mut persist: Persist,
) -> QuantResult<Option<DomainSourceCheckpoint>>
where
    Fetch: FnMut(u128) -> FetchFuture,
    FetchFuture: Future<Output = QuantResult<Vec<CryptoPriceReport>>>,
    Persist: FnMut(Vec<CryptoPriceReport>) -> PersistFuture,
    PersistFuture: Future<Output = QuantResult<()>>,
{
    if page_limit == 0 {
        return Err(QuantError::config(
            "Chainlink report page limit must be positive",
        ));
    }
    loop {
        let reports = fetch(start).await?;
        let page_len = reports.len();
        let last_sequence = reports.last().map(|report| report.source_sequence);
        let mut pending = Vec::with_capacity(page_len);
        for report in reports {
            if should_process_crypto(&report, last.as_ref())? {
                let checkpoint = report
                    .checkpoint()
                    .map_err(|error| QuantError::config(error.to_string()))?;
                pending.push(report);
                last = Some(checkpoint);
            }
        }
        persist(pending).await?;
        if page_len < page_limit {
            return Ok(last);
        }
        let last_sequence = last_sequence.ok_or_else(|| {
            QuantError::config("Chainlink returned an empty page at the configured page limit")
        })?;
        start = u128::from(last_sequence.checked_add(1).ok_or_else(|| {
            QuantError::config("Chainlink report sequence overflow during catch-up")
        })?);
    }
}

fn next_sequence(value: u64) -> QuantResult<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| QuantError::config("source sequence overflow"))
}

fn stop_removed<V>(
    tasks: &mut BTreeMap<DomainInstrumentKey, CancellationToken>,
    desired: &BTreeMap<DomainInstrumentKey, V>,
) {
    let removed = tasks
        .keys()
        .filter(|key| !desired.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    for key in removed {
        if let Some(cancel) = tasks.remove(&key) {
            cancel.cancel();
        }
    }
}

fn observe_source_task(
    name: &'static str,
    joined: Result<(DomainInstrumentKey, QuantResult<()>), JoinError>,
    tasks: &mut BTreeMap<DomainInstrumentKey, CancellationToken>,
    stopping: bool,
) -> QuantResult<()> {
    let (instrument, result) = joined.map_err(|error| InfraError::BlockingTaskJoin {
        detail: format!("{name} failed: {error}"),
    })?;
    let planned = tasks.remove(&instrument).is_none() || stopping;
    result?;
    if !planned {
        return Err(InfraError::ChannelClosed {
            name: "crypto_live_source_task",
        }
        .into());
    }
    Ok(())
}

#[cfg(all(test, feature = "domain-chainlink"))]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use chrono::DateTime;
    use quant_pivot_error::QuantError;
    use quant_pivot_models::{
        domain::data_plane::CryptoPriceReport,
        types::{ContentHash, DomainInstrumentKey, DomainSourceId, Usd},
    };
    use rust_decimal_macros::dec;

    use super::{catch_up_chainlink_pages, should_process_crypto};

    const CHAINLINK_BASE_SECONDS: u64 = 1_700_000_000;

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }

    fn report(offset_seconds: u64, seed: char) -> CryptoPriceReport {
        let source_sequence = CHAINLINK_BASE_SECONDS
            .checked_add(offset_seconds)
            .expect("source sequence");
        let event_time =
            DateTime::from_timestamp(i64::try_from(source_sequence).expect("timestamp"), 0)
                .expect("event time");
        CryptoPriceReport {
            source_id: DomainSourceId::chainlink_data_streams(),
            instrument_key: DomainInstrumentKey::new("CHAINLINK_DATA_STREAMS:BTC-USD"),
            source_sequence,
            price: Usd::new(dec!(100)),
            quantity: None,
            event_time,
            published_at: event_time,
            available_at: event_time,
            valid_from: None,
            observations_timestamp: Some(event_time),
            expires_at: None,
            report_hash: hash(seed),
            raw_report: seed.to_string(),
        }
    }

    #[test]
    fn same_timestamp_equivocates() {
        let first = report(0, 'a');
        let correction = CryptoPriceReport {
            report_hash: hash('b'),
            ..first.clone()
        };
        let checkpoint = first.checkpoint().expect("checkpoint");
        assert!(!should_process_crypto(&first, Some(&checkpoint)).expect("exact replay"));
        assert!(should_process_crypto(&correction, Some(&checkpoint)).is_err());
    }

    #[tokio::test]
    async fn chainlink_reads_before_stopping() {
        let page_start = u128::from(CHAINLINK_BASE_SECONDS);
        let pages = Arc::new(Mutex::new(BTreeMap::from([
            (page_start, vec![report(0, 'a'), report(1, 'b')]),
            (page_start + 2, vec![report(2, 'c')]),
        ])));
        let starts = Arc::new(Mutex::new(Vec::new()));
        let persisted = Arc::new(Mutex::new(Vec::new()));
        let result = catch_up_chainlink_pages(
            page_start,
            2,
            None,
            {
                let pages = Arc::clone(&pages);
                let starts = Arc::clone(&starts);
                move |start| {
                    let pages = Arc::clone(&pages);
                    let starts = Arc::clone(&starts);
                    async move {
                        starts.lock().expect("lock").push(start);
                        Ok::<_, QuantError>(
                            pages
                                .lock()
                                .expect("lock")
                                .remove(&start)
                                .unwrap_or_default(),
                        )
                    }
                }
            },
            {
                let persisted = Arc::clone(&persisted);
                move |reports| {
                    let persisted = Arc::clone(&persisted);
                    async move {
                        persisted
                            .lock()
                            .expect("lock")
                            .extend(reports.into_iter().map(|report| report.source_sequence));
                        Ok::<(), QuantError>(())
                    }
                }
            },
        )
        .await
        .expect("gap recovery");
        assert_eq!(
            *starts.lock().expect("lock"),
            vec![page_start, page_start + 2]
        );
        assert_eq!(
            *persisted.lock().expect("lock"),
            vec![
                CHAINLINK_BASE_SECONDS,
                CHAINLINK_BASE_SECONDS + 1,
                CHAINLINK_BASE_SECONDS + 2,
            ]
        );
        assert_eq!(
            result
                .expect("checkpoint")
                .crypto_report_hash()
                .expect("report hash"),
            hash('c')
        );
    }
}
