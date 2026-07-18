//! Crypto source-native live ingestion and gap recovery.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    slice,
    sync::Arc,
    time::Duration as StdDuration,
};

use chrono::{DateTime, NaiveDate, Utc};
use quant_pivot_api::{binance::BinanceAggTradeSource, chainlink::ChainlinkDataStreamsSource};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    clickhouse::CryptoPriceReportRow,
    domain::{
        CryptoPriceReport, DomainSourceCheckpoint, LinkageOutcome, MarketLinkage, MarketSubject,
    },
    enums::domain::{BinanceMarketSegment, LinkageSourceRole},
    types::{BinanceSymbol, ChainlinkFeedKey, ContentHash, DomainInstrumentKey, DomainSourceId},
};
use quant_pivot_repository::traits::{
    DomainProjectionRepository, DomainSourceCursorRepository, FactWriter, MarketLinkageRepository,
};
use quant_pivot_research::linkage::rules;
use tokio::{task::JoinHandle, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::app::domain_source_supervisor::DomainSourceSupervisor;

const DISCOVERY_INTERVAL: StdDuration = StdDuration::from_secs(30);
const RECONNECT_BACKOFF: StdDuration = StdDuration::from_secs(2);
const BINANCE_PAGE_SIZE: u16 = 1_000;

#[derive(Default)]
struct CryptoBindings {
    binance: BTreeMap<DomainInstrumentKey, BinanceSymbol>,
    binance_usdm_futures: BTreeMap<DomainInstrumentKey, BinanceSymbol>,
    chainlink: BTreeMap<DomainInstrumentKey, ChainlinkFeedKey>,
}

struct SourceTask {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

pub struct CryptoLiveIngestWorker {
    source_supervisor: Arc<DomainSourceSupervisor>,
    linkages: Arc<dyn MarketLinkageRepository>,
    cursors: Arc<dyn DomainSourceCursorRepository>,
    projections: Arc<dyn DomainProjectionRepository>,
    crypto_writer: Arc<dyn FactWriter<CryptoPriceReportRow>>,
    binance: Option<Arc<BinanceAggTradeSource>>,
    binance_usdm_futures: Option<Arc<BinanceAggTradeSource>>,
    chainlink: Option<Arc<ChainlinkDataStreamsSource>>,
}

pub struct CryptoLiveIngestDeps {
    pub source_supervisor: Arc<DomainSourceSupervisor>,
    pub linkages: Arc<dyn MarketLinkageRepository>,
    pub cursors: Arc<dyn DomainSourceCursorRepository>,
    pub projections: Arc<dyn DomainProjectionRepository>,
    pub crypto_writer: Arc<dyn FactWriter<CryptoPriceReportRow>>,
    pub binance: Option<Arc<BinanceAggTradeSource>>,
    pub binance_usdm_futures: Option<Arc<BinanceAggTradeSource>>,
    pub chainlink: Option<Arc<ChainlinkDataStreamsSource>>,
}

impl CryptoLiveIngestWorker {
    #[must_use]
    pub fn new(deps: CryptoLiveIngestDeps) -> Self {
        Self {
            source_supervisor: deps.source_supervisor,
            linkages: deps.linkages,
            cursors: deps.cursors,
            projections: deps.projections,
            crypto_writer: deps.crypto_writer,
            binance: deps.binance,
            binance_usdm_futures: deps.binance_usdm_futures,
            chainlink: deps.chainlink,
        }
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        if let Err(error) = self.source_supervisor.ensure_boot_reconciled().await {
            tracing::error!(%error, "Crypto ingest blocked: expected-source reconciliation failed");
            return;
        }
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
        let chainlink = Arc::clone(&self).run_chainlink_supervisor(shutdown.child_token());
        tokio::join!(binance, binance_usdm_futures, chainlink);
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
            let linkage = row
                .into_domain()
                .map_err(|error| QuantError::config(format!("invalid linkage payload: {error}")))?;
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
                let symbol = source
                    .instrument_key
                    .as_binance_agg_trade_symbol()
                    .ok_or_else(|| {
                        QuantError::config("invalid Binance live-event linkage instrument")
                    })?;
                bindings
                    .binance
                    .insert(source.instrument_key.clone(), symbol);
            }
            Some(source)
                if source.source_id == DomainSourceId::binance_usdm_futures_agg_trade() =>
            {
                let symbol = source
                    .instrument_key
                    .as_binance_usdm_futures_agg_trade_symbol()
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
                tracing::warn!(
                    market_id = %linkage.market_id,
                    "resolved active Crypto linkage has no live-event source binding"
                );
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
            let desired = match self.discover().await {
                Ok(bindings) => match market {
                    BinanceMarketSegment::Spot => bindings.binance,
                    BinanceMarketSegment::UsdmFutures => bindings.binance_usdm_futures,
                },
                Err(error) => {
                    tracing::warn!(%error, "Binance live binding discovery failed");
                    continue;
                }
            };
            stop_removed(&mut tasks, desired.keys().collect()).await;
            let Some(source) = source.as_ref() else {
                for instrument in desired.keys() {
                    self.mark_crypto_unavailable(instrument).await;
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
                let source_id = source_id_for_market(market);
                let handle = tokio::spawn(async move {
                    worker
                        .run_binance_symbol(
                            source,
                            source_id,
                            task_instrument,
                            symbol,
                            child_cancel,
                        )
                        .await;
                });
                tasks.insert(instrument, SourceTask { cancel, handle });
            }
        }
        stop_all(tasks).await;
    }

    async fn run_binance_symbol(
        &self,
        source: Arc<BinanceAggTradeSource>,
        source_id: DomainSourceId,
        instrument: DomainInstrumentKey,
        symbol: BinanceSymbol,
        shutdown: CancellationToken,
    ) {
        let mut gap_generation = self
            .projections
            .mark_crypto_source_gap(&source_id, &instrument, Utc::now())
            .await
            .unwrap_or(0);
        loop {
            if shutdown.is_cancelled() {
                return;
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
                Ok(()) => return,
                Err(error) => {
                    tracing::warn!(%instrument, %error, "Binance aggTrade session failed");
                    if let Err(mark_error) = self
                        .source_supervisor
                        .mark_source_failed(&source_id, &instrument, error.to_string())
                        .await
                    {
                        tracing::error!(%instrument, %mark_error, "failed to record Binance source failure");
                    }
                    gap_generation = self
                        .projections
                        .mark_crypto_source_gap(
                            &source_id,
                            &instrument,
                            Utc::now(),
                        )
                        .await
                        .unwrap_or_else(|mark_error| {
                            tracing::error!(%instrument, %mark_error, "failed to persist Binance source gap");
                            gap_generation.saturating_add(1)
                        });
                    tokio::select! {
                        () = shutdown.cancelled() => return,
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
        let mut next_clock_check = tokio::time::Instant::now() + StdDuration::from_secs(30);
        let recovery_poll_interval = source.recovery_poll_interval();
        let mut next_recovery = tokio::time::Instant::now() + recovery_poll_interval;
        loop {
            if tokio::time::Instant::now() >= next_clock_check {
                source.validate_system_clock().await?;
                next_clock_check = tokio::time::Instant::now() + StdDuration::from_secs(30);
            }
            let (report, planned_rotation) = if stream.rotation_due() {
                match source.stream(symbol).await {
                    Ok(mut replacement) => {
                        let report = tokio::select! {
                            () = shutdown.cancelled() => return Ok(()),
                            result = replacement.next_report() => result?,
                            () = tokio::time::sleep(StdDuration::from_secs(10)) => {
                                tracing::warn!(%instrument, "Binance overlap rotation produced no first report");
                                continue;
                            }
                        };
                        stream = replacement;
                        (report, true)
                    }
                    Err(error) => {
                        tracing::warn!(%instrument, %error, "Binance overlap rotation connection failed");
                        let report = tokio::select! {
                            () = shutdown.cancelled() => return Ok(()),
                            result = stream.next_report() => result?,
                            () = tokio::time::sleep(StdDuration::from_secs(1)) => continue,
                        };
                        (report, false)
                    }
                }
            } else {
                let report = tokio::select! {
                    () = shutdown.cancelled() => return Ok(()),
                    result = stream.next_report() => result?,
                    () = tokio::time::sleep_until(next_recovery) => {
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
                (report, false)
            };
            next_recovery = tokio::time::Instant::now() + recovery_poll_interval;
            if let Some(next_id) = expected {
                if report.source_sequence < next_id {
                    continue;
                }
                if report.source_sequence > next_id {
                    if !planned_rotation {
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
                self.persist_crypto(report.clone(), gap_generation).await?;
                expected = Some(next_sequence(report.source_sequence)?);
            }
        }
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
        self.persist_crypto(report, gap_generation).await?;
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
                self.persist_crypto_batch(pending, gap_generation).await?;
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
                    self.persist_crypto_batch(pending, gap_generation).await?;
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
            self.persist_crypto_batch(pending, gap_generation).await?;
            if page_len < usize::from(BINANCE_PAGE_SIZE) {
                return Ok(from_id);
            }
        }
    }

    async fn run_chainlink_supervisor(self: Arc<Self>, shutdown: CancellationToken) {
        let mut tasks = BTreeMap::<DomainInstrumentKey, SourceTask>::new();
        let mut interval = tokio::time::interval(DISCOVERY_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            let desired = match self.discover().await {
                Ok(bindings) => bindings.chainlink,
                Err(error) => {
                    tracing::warn!(%error, "Chainlink live binding discovery failed");
                    continue;
                }
            };
            stop_removed(&mut tasks, desired.keys().collect()).await;
            let Some(source) = self.chainlink.as_ref() else {
                for instrument in desired.keys() {
                    self.mark_crypto_unavailable(instrument).await;
                }
                continue;
            };
            for (instrument, feed) in desired {
                if tasks.contains_key(&instrument) {
                    continue;
                }
                if !source.instruments().contains(&instrument) {
                    self.mark_crypto_unavailable(&instrument).await;
                    tracing::error!(%instrument, "active Chainlink feed is not configured");
                    continue;
                }
                let cancel = shutdown.child_token();
                let child_cancel = cancel.clone();
                let worker = Arc::clone(&self);
                let source = Arc::clone(source);
                let task_instrument = instrument.clone();
                let handle = tokio::spawn(async move {
                    worker
                        .run_chainlink_feed(source, task_instrument, feed, child_cancel)
                        .await;
                });
                tasks.insert(instrument, SourceTask { cancel, handle });
            }
        }
        stop_all(tasks).await;
    }

    async fn run_chainlink_feed(
        &self,
        source: Arc<ChainlinkDataStreamsSource>,
        instrument: DomainInstrumentKey,
        feed: ChainlinkFeedKey,
        shutdown: CancellationToken,
    ) {
        let mut gap_generation = self
            .projections
            .mark_crypto_source_gap(
                &DomainSourceId::chainlink_data_streams(),
                &instrument,
                Utc::now(),
            )
            .await
            .unwrap_or(0);
        loop {
            if shutdown.is_cancelled() {
                return;
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
                Ok(()) => return,
                Err(error) => {
                    tracing::warn!(%instrument, %error, "Chainlink Data Streams session failed");
                    if let Err(mark_error) = self
                        .source_supervisor
                        .mark_source_failed(
                            &DomainSourceId::chainlink_data_streams(),
                            &instrument,
                            error.to_string(),
                        )
                        .await
                    {
                        tracing::error!(%instrument, %mark_error, "failed to record Chainlink source failure");
                    }
                    gap_generation = self
                        .projections
                        .mark_crypto_source_gap(
                            &DomainSourceId::chainlink_data_streams(),
                            &instrument,
                            Utc::now(),
                        )
                        .await
                        .unwrap_or_else(|_| gap_generation.saturating_add(1));
                    tokio::select! {
                        () = shutdown.cancelled() => return,
                        () = tokio::time::sleep(RECONNECT_BACKOFF) => {}
                    }
                }
            }
        }
    }

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
            Some(DomainSourceCheckpoint::ChainlinkDataStreams {
                observations_timestamp,
                report_hash,
            }) => Some((observations_timestamp, report_hash)),
            Some(_) => {
                return Err(QuantError::config(
                    "Chainlink cursor contains a different source checkpoint type",
                ));
            }
            None => None,
        };
        if let Some((timestamp, _)) = &last {
            let start = u128::try_from(timestamp.timestamp()).map_err(|error| {
                QuantError::config(format!("negative Chainlink checkpoint timestamp: {error}"))
            })?;
            last = catch_up_chainlink_pages(
                start,
                source.rest_page_limit(),
                last,
                |page_start| source.reports_page(feed, page_start, Utc::now()),
                |report| self.persist_crypto(report, gap_generation),
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
        loop {
            let report = tokio::select! {
                () = shutdown.cancelled() => {
                    stream.close().await.map_err(|error| {
                        QuantError::config(format!("Chainlink stream close failed: {error}"))
                    })?;
                    return Ok(());
                }
                _ = clock_check.tick() => {
                    source.validate_system_clock().await?;
                    continue;
                }
                report = source.next_report(&mut stream, Utc::now()) => report?,
            };
            if should_process_crypto(&report, last.as_ref()) {
                self.persist_crypto(report.clone(), gap_generation).await?;
                last = Some((report.event_time, report.report_hash.clone()));
            }
        }
    }

    async fn persist_crypto(
        &self,
        report: CryptoPriceReport,
        gap_generation: u64,
    ) -> QuantResult<()> {
        let source_id = report.source_id.clone();
        let instrument_key = report.instrument_key.clone();
        self.crypto_writer
            .write_batch(vec![report.to_clickhouse_row()])
            .await?;
        let checkpoint = if report.source_id == DomainSourceId::binance_agg_trade()
            || report.source_id == DomainSourceId::binance_usdm_futures_agg_trade()
        {
            DomainSourceCheckpoint::BinanceAggTrade {
                aggregate_trade_id: report.source_sequence,
                event_time: report.event_time,
            }
        } else if report.source_id == DomainSourceId::chainlink_data_streams() {
            DomainSourceCheckpoint::ChainlinkDataStreams {
                observations_timestamp: report.observations_timestamp.ok_or_else(|| {
                    QuantError::config("Chainlink report lacks observations timestamp")
                })?,
                report_hash: report.report_hash.clone(),
            }
        } else {
            return Err(QuantError::config(format!(
                "unsupported crypto report source {}",
                report.source_id
            )));
        };
        self.projections
            .apply_crypto_report(report, checkpoint, gap_generation, true)
            .await?;
        self.source_supervisor
            .mark_source_recovered(&source_id, &instrument_key)
            .await?;
        Ok(())
    }

    async fn persist_crypto_batch(
        &self,
        reports: Vec<CryptoPriceReport>,
        gap_generation: u64,
    ) -> QuantResult<()> {
        let Some(last) = reports.last().cloned() else {
            return Ok(());
        };
        let source_id = last.source_id.clone();
        let instrument_key = last.instrument_key.clone();
        self.crypto_writer
            .write_batch(
                reports
                    .into_iter()
                    .map(|report| report.to_clickhouse_row())
                    .collect(),
            )
            .await?;
        let checkpoint = DomainSourceCheckpoint::BinanceAggTrade {
            aggregate_trade_id: last.source_sequence,
            event_time: last.event_time,
        };
        self.projections
            .apply_crypto_report(last, checkpoint, gap_generation, true)
            .await?;
        self.source_supervisor
            .mark_source_recovered(&source_id, &instrument_key)
            .await?;
        Ok(())
    }

    async fn mark_crypto_unavailable(&self, instrument: &DomainInstrumentKey) {
        let Some(source_id) = instrument.source_id() else {
            return;
        };
        if let Err(error) = self
            .projections
            .mark_crypto_source_gap(&source_id, instrument, Utc::now())
            .await
        {
            tracing::error!(%instrument, %error, "failed to mark crypto source unavailable");
        }
    }
}

fn should_process_crypto(
    report: &CryptoPriceReport,
    last: Option<&(DateTime<Utc>, ContentHash)>,
) -> bool {
    last.is_none_or(|(time, hash)| {
        report.event_time > *time || (report.event_time == *time && report.report_hash != *hash)
    })
}

async fn catch_up_chainlink_pages<Fetch, FetchFuture, Persist, PersistFuture>(
    mut start: u128,
    page_limit: usize,
    mut last: Option<(DateTime<Utc>, ContentHash)>,
    mut fetch: Fetch,
    mut persist: Persist,
) -> QuantResult<Option<(DateTime<Utc>, ContentHash)>>
where
    Fetch: FnMut(u128) -> FetchFuture,
    FetchFuture: Future<Output = QuantResult<Vec<CryptoPriceReport>>>,
    Persist: FnMut(CryptoPriceReport) -> PersistFuture,
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
        for report in reports {
            if should_process_crypto(&report, last.as_ref()) {
                let checkpoint = (report.event_time, report.report_hash.clone());
                persist(report).await?;
                last = Some(checkpoint);
            }
        }
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

fn source_id_for_market(market: BinanceMarketSegment) -> DomainSourceId {
    match market {
        BinanceMarketSegment::Spot => DomainSourceId::binance_agg_trade(),
        BinanceMarketSegment::UsdmFutures => DomainSourceId::binance_usdm_futures_agg_trade(),
    }
}

async fn stop_removed(
    tasks: &mut BTreeMap<DomainInstrumentKey, SourceTask>,
    desired: BTreeSet<&DomainInstrumentKey>,
) {
    let removed = tasks
        .keys()
        .filter(|key| !desired.contains(key))
        .cloned()
        .collect::<Vec<_>>();
    for key in removed {
        if let Some(task) = tasks.remove(&key) {
            task.cancel.cancel();
            if let Err(error) = task.handle.await {
                tracing::warn!(%key, %error, "domain source task join failed");
            }
        }
    }
}

async fn stop_all(tasks: BTreeMap<DomainInstrumentKey, SourceTask>) {
    for (_, task) in tasks {
        task.cancel.cancel();
        if let Err(error) = task.handle.await {
            tracing::warn!(%error, "domain source task join failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use chrono::{Duration, Utc};
    use quant_pivot_error::QuantError;
    use quant_pivot_models::{
        domain::CryptoPriceReport,
        types::{ContentHash, DomainInstrumentKey, DomainSourceId, Usd},
    };
    use rust_decimal_macros::dec;

    use super::{catch_up_chainlink_pages, should_process_crypto};

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }

    fn report(sequence: u64, seconds: i64, seed: char) -> CryptoPriceReport {
        let event_time = Utc::now() + Duration::seconds(seconds);
        CryptoPriceReport {
            source_id: DomainSourceId::chainlink_data_streams(),
            instrument_key: DomainInstrumentKey::new("CHAINLINK_DATA_STREAMS:BTC-USD"),
            source_sequence: sequence,
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
    fn same_timestamp_correction_is_not_deduplicated_by_time_only() {
        let first = report(1, 0, 'a');
        let correction = CryptoPriceReport {
            report_hash: hash('b'),
            ..first.clone()
        };
        let checkpoint = (first.event_time, first.report_hash.clone());
        assert!(!should_process_crypto(&first, Some(&checkpoint)));
        assert!(should_process_crypto(&correction, Some(&checkpoint)));
    }

    #[tokio::test]
    async fn chainlink_gap_recovery_reads_every_full_page_before_stopping() {
        let pages = Arc::new(Mutex::new(BTreeMap::from([
            (0_u128, vec![report(1, 0, 'a'), report(2, 1, 'b')]),
            (3_u128, vec![report(3, 2, 'c')]),
        ])));
        let starts = Arc::new(Mutex::new(Vec::new()));
        let persisted = Arc::new(Mutex::new(Vec::new()));
        let result = catch_up_chainlink_pages(
            0,
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
                move |report| {
                    let persisted = Arc::clone(&persisted);
                    async move {
                        persisted.lock().expect("lock").push(report.source_sequence);
                        Ok::<(), QuantError>(())
                    }
                }
            },
        )
        .await
        .expect("gap recovery");
        assert_eq!(*starts.lock().expect("lock"), vec![0, 3]);
        assert_eq!(*persisted.lock().expect("lock"), vec![1, 2, 3]);
        assert_eq!(result.expect("checkpoint").1, hash('c'));
    }
}
