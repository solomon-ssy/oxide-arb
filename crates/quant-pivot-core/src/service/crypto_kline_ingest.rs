//! External domain-source ingestion (Phase 11.2.2).
//!
//! [`CryptoKlineIngestor`] polls enabled [`DomainDataSource`] clients, normalizes
//! observations into `quant_domain_observation`, and advances durable
//! `(source, instrument)` cursors only after a successful `ClickHouse` write.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_api::{
    binance::BinanceKlineSource,
    domain::{DomainDataSource, DomainFetchRequest},
};
use quant_pivot_error::{
    QuantError, QuantResult,
    storage::{StorageError, entity},
};
use quant_pivot_models::{
    clickhouse::DomainObservationRow,
    config::DomainSourcesConfig,
    domain::{
        DomainCursorStatus, DomainObservation, DomainSourceCheckpoint, DomainSourceCursorInfo,
        UpsertDomainSourceCursor,
    },
    enums::domain::{DomainFamily, KlineInterval},
    hashing::CanonicalDigest,
    types::{ContentHash, DomainInstrumentKey, DomainSourceId},
};
use quant_pivot_repository::traits::{DomainSourceCursorRepository, FactWriter};
use quant_pivot_research::linkage::rules;

use crate::runtime_config::DecisionPolicyStore;

/// One `(source, instrument)` scan tick — observations to persist plus the
/// cursor row to commit after `ClickHouse` acknowledges the batch.
#[derive(Debug, Clone, PartialEq, Eq)]
struct InstrumentScanOutcome {
    source_id: DomainSourceId,
    instrument_key: DomainInstrumentKey,
    observations: Vec<DomainObservation>,
    checkpoint: InstrumentCheckpoint,
}

/// Checkpoint advanced only after a successful `ClickHouse` write.
#[derive(Debug, Clone, PartialEq, Eq)]
struct InstrumentCheckpoint {
    source_id: DomainSourceId,
    instrument_key: DomainInstrumentKey,
    checkpoint: DomainSourceCheckpoint,
    checkpoint_hash: ContentHash,
    status: DomainCursorStatus,
}

/// Source-supervisor transition emitted only after the corresponding cursor
/// write has completed (or a scan failure has been durably recorded).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CryptoKlineBindingOutcome {
    Recovered {
        source_id: DomainSourceId,
        instrument_key: DomainInstrumentKey,
    },
    Failed {
        source_id: DomainSourceId,
        instrument_key: DomainInstrumentKey,
        reason: String,
    },
}

/// Polls external domain sources and writes normalized observations to
/// `quant_domain_observation`.
pub struct CryptoKlineIngestor {
    sources: Vec<Arc<dyn DomainDataSource>>,
    binance_archive_sources: HashMap<DomainSourceId, Arc<BinanceKlineSource>>,
    cursor_repo: Arc<dyn DomainSourceCursorRepository>,
    writer: Arc<dyn FactWriter<DomainObservationRow>>,
    runtime_config: Arc<DecisionPolicyStore>,
    domain_sources: DomainSourcesConfig,
    instruments_by_source: HashMap<DomainSourceId, Vec<DomainInstrumentKey>>,
}

impl CryptoKlineIngestor {
    #[must_use]
    pub fn new(
        sources: Vec<Arc<dyn DomainDataSource>>,
        binance_archive_sources: Vec<Arc<BinanceKlineSource>>,
        cursor_repo: Arc<dyn DomainSourceCursorRepository>,
        writer: Arc<dyn FactWriter<DomainObservationRow>>,
        runtime_config: Arc<DecisionPolicyStore>,
        domain_sources: DomainSourcesConfig,
    ) -> Self {
        Self {
            sources,
            binance_archive_sources: binance_archive_sources
                .into_iter()
                .map(|source| (source.source_id(), source))
                .collect(),
            cursor_repo,
            writer,
            runtime_config,
            instruments_by_source: discover_instruments(&domain_sources),
            domain_sources,
        }
    }

    /// One ingest tick across every enabled `(source, instrument)` stream.
    ///
    /// Each instrument is isolated (R10 ingest hardening): one symbol's fetch
    /// failure records that instrument's cursor as [`DomainCursorStatus::Failed`]
    /// with the failure detail and is skipped, but never aborts the whole
    /// tick — every other instrument still scans, and the batch still writes
    /// and commits for every instrument that succeeded.
    pub async fn run_once(&self) -> QuantResult<Vec<CryptoKlineBindingOutcome>> {
        let runtime = self.runtime_config.load();
        if !runtime
            .profile_artifacts
            .domain
            .definition
            .family_enabled(DomainFamily::Crypto)
        {
            return Ok(Vec::new());
        }

        let backfill_days = runtime
            .profile_artifacts
            .domain
            .definition
            .crypto
            .backfill_days;
        let now = Utc::now();
        let bootstrap_from = now - Duration::days(i64::from(backfill_days.max(1)));
        let batch_size = self.domain_sources.binance.batch_size.max(1);

        let mut outcomes = Vec::new();
        let mut binding_outcomes = Vec::new();
        for source in &self.sources {
            let source_id = source.source_id();
            let Some(instruments) = self.instruments_by_source.get(&source_id) else {
                continue;
            };
            for instrument_key in instruments {
                if let Some(archive_source) = self.binance_archive_sources.get(&source_id) {
                    match self
                        .backfill_archive_instrument(
                            archive_source,
                            instrument_key,
                            bootstrap_from,
                            now,
                            batch_size,
                        )
                        .await
                    {
                        Ok(true) => continue,
                        Ok(false) => {}
                        Err(error) => {
                            tracing::warn!(
                                %source_id, %instrument_key, %error,
                                "Binance archive backfill failed for this instrument; \
                                 other instruments continue this tick"
                            );
                            self.record_scan_failure(
                                &source_id,
                                instrument_key,
                                bootstrap_from,
                                &error,
                            )
                            .await;
                            binding_outcomes.push(CryptoKlineBindingOutcome::Failed {
                                source_id: source_id.clone(),
                                instrument_key: instrument_key.clone(),
                                reason: error.to_string(),
                            });
                            continue;
                        }
                    }
                }
                match self
                    .scan_instrument(source.as_ref(), instrument_key, bootstrap_from, now)
                    .await
                {
                    Ok(outcome) => outcomes.push(outcome),
                    Err(error) => {
                        tracing::warn!(
                            %source_id, %instrument_key, %error,
                            "domain ingest scan failed for this instrument; \
                             other instruments continue this tick"
                        );
                        self.record_scan_failure(
                            &source_id,
                            instrument_key,
                            bootstrap_from,
                            &error,
                        )
                        .await;
                        binding_outcomes.push(CryptoKlineBindingOutcome::Failed {
                            source_id: source_id.clone(),
                            instrument_key: instrument_key.clone(),
                            reason: error.to_string(),
                        });
                    }
                }
            }
        }

        let mut all_observations = Vec::new();
        for outcome in &outcomes {
            all_observations.extend(outcome.observations.iter().cloned());
        }
        let observations = dedup_observations(all_observations);

        if !observations.is_empty() {
            let ingestion_time = Utc::now();
            let rows = observations
                .into_iter()
                .map(|observation| observation.into_clickhouse_row(ingestion_time))
                .collect::<Vec<_>>();
            for batch in rows.chunks(batch_size) {
                let token = domain_observation_batch_token(batch)?;
                self.writer
                    .write_batch_idempotent(&token, batch.to_vec())
                    .await?;
            }
        }

        for checkpoint in outcomes.into_iter().map(|outcome| outcome.checkpoint) {
            let recovered = (checkpoint.status == DomainCursorStatus::Live).then(|| {
                CryptoKlineBindingOutcome::Recovered {
                    source_id: checkpoint.source_id.clone(),
                    instrument_key: checkpoint.instrument_key.clone(),
                }
            });
            self.commit_checkpoint(checkpoint).await?;
            binding_outcomes.extend(recovered);
        }
        Ok(binding_outcomes)
    }

    async fn backfill_archive_instrument(
        &self,
        source: &BinanceKlineSource,
        instrument_key: &DomainInstrumentKey,
        bootstrap_from: DateTime<Utc>,
        now: DateTime<Utc>,
        batch_size: usize,
    ) -> QuantResult<bool> {
        let source_id = source.source_id();
        let cursor = self.cursor_repo.find(&source_id, instrument_key).await?;
        let (from_exclusive, bootstrap, _) = resume_point(cursor.as_ref(), bootstrap_from, now)?;
        if !bootstrap {
            return Ok(false);
        }
        let (_, symbol, interval) = instrument_key.as_binance_market_kline().ok_or_else(|| {
            StorageError::invariant_violation(
                Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
                format!("non-canonical Binance kline instrument `{instrument_key}`"),
            )
        })?;
        let step = kline_step(interval)?;
        let archive_date = (from_exclusive + step).date_naive();
        if archive_date >= now.date_naive() {
            return Ok(false);
        }
        source.validate_system_clock().await?;
        let Some(mut archive) = source
            .recover_archive_day(&symbol, interval, archive_date, now)
            .await?
        else {
            return Ok(false);
        };

        let mut expected = cursor.as_ref().map(|_| from_exclusive + step);
        let mut wrote = false;
        while let Some(observations) = archive.next_batch().await? {
            let observations = observations
                .into_iter()
                .filter(|observation| {
                    observation.observed_at > from_exclusive && observation.observed_at <= now
                })
                .collect::<Vec<_>>();
            if observations.is_empty() {
                continue;
            }
            validate_archive_observations(&observations, expected, from_exclusive, step)?;
            let last_event_time = observations
                .last()
                .map(|observation| observation.observed_at)
                .ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
                        "non-empty Binance archive batch lost its terminal observation",
                    )
                })?;
            self.persist_archive_observations(
                &source_id,
                instrument_key,
                observations,
                last_event_time,
                batch_size,
            )
            .await?;
            expected = Some(last_event_time + step);
            wrote = true;
        }
        if !wrote {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
                format!(
                    "verified Binance archive {archive_date} contains no observation after {from_exclusive}"
                ),
            )
            .into());
        }
        Ok(true)
    }

    async fn persist_archive_observations(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
        observations: Vec<DomainObservation>,
        last_event_time: DateTime<Utc>,
        batch_size: usize,
    ) -> QuantResult<()> {
        let ingestion_time = Utc::now();
        let rows = observations
            .into_iter()
            .map(|observation| observation.into_clickhouse_row(ingestion_time))
            .collect::<Vec<_>>();
        for rows in rows.chunks(batch_size.max(1)) {
            let token = domain_observation_batch_token(rows)?;
            self.writer
                .write_batch_idempotent(&token, rows.to_vec())
                .await?;
        }
        let checkpoint = DomainSourceCheckpoint::BinanceKline {
            close_time: last_event_time,
        };
        self.commit_checkpoint(InstrumentCheckpoint {
            source_id: source_id.clone(),
            instrument_key: instrument_key.clone(),
            checkpoint_hash: CanonicalDigest::content_hash_json(&checkpoint)?,
            checkpoint,
            status: DomainCursorStatus::Backfilling,
        })
        .await
    }

    async fn scan_instrument(
        &self,
        source: &dyn DomainDataSource,
        instrument_key: &DomainInstrumentKey,
        bootstrap_from: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> QuantResult<InstrumentScanOutcome> {
        let source_id = source.source_id();
        let cursor = self.cursor_repo.find(&source_id, instrument_key).await?;
        let (from_exclusive, bootstrap, prior_last_event_time) =
            resume_point(cursor.as_ref(), bootstrap_from, now)?;

        let observations = source
            .fetch(DomainFetchRequest {
                instrument_key: instrument_key.clone(),
                from_exclusive,
                to_inclusive: now,
                bootstrap,
            })
            .await?;
        if let Some(future) = observations
            .iter()
            .map(|observation| observation.observed_at)
            .filter(|observed_at| *observed_at > now)
            .min()
        {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
                format!(
                    "source {source_id}/{instrument_key} returned future observation {future} after scan boundary {now}"
                ),
            )
            .into());
        }

        let last_event_time = observations
            .iter()
            .map(|observation| observation.observed_at)
            .max()
            .unwrap_or(prior_last_event_time);
        let (_, _, interval) = instrument_key.as_binance_market_kline().ok_or_else(|| {
            StorageError::invariant_violation(
                Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
                format!("non-canonical Binance kline instrument `{instrument_key}`"),
            )
        })?;
        let status = checkpoint_status(
            cursor.as_ref(),
            bootstrap,
            last_event_time,
            latest_closed_kline_time(interval, now)?,
        );
        let checkpoint = DomainSourceCheckpoint::BinanceKline {
            close_time: last_event_time,
        };
        let checkpoint_hash = CanonicalDigest::content_hash_json(&checkpoint)?;

        Ok(InstrumentScanOutcome {
            source_id: source_id.clone(),
            instrument_key: instrument_key.clone(),
            observations,
            checkpoint: InstrumentCheckpoint {
                source_id,
                instrument_key: instrument_key.clone(),
                checkpoint,
                checkpoint_hash,
                status,
            },
        })
    }

    async fn commit_checkpoint(&self, checkpoint: InstrumentCheckpoint) -> QuantResult<()> {
        self.cursor_repo
            .upsert(UpsertDomainSourceCursor {
                source_id: checkpoint.source_id,
                instrument_key: checkpoint.instrument_key,
                checkpoint_json: checkpoint.checkpoint,
                checkpoint_hash: checkpoint.checkpoint_hash,
                status: checkpoint.status,
                // A checkpoint is only ever constructed on this tick's
                // success path, so any error recorded on a prior failed tick
                // is now resolved — clear it rather than let it linger.
                last_error: None,
                updated_at: Utc::now(),
            })
            .await?;
        Ok(())
    }

    /// Record a failed scan against the instrument's durable cursor without
    /// advancing `last_event_time`, so the next tick simply retries
    /// incrementally from the same resume point (self-healing once the
    /// transient condition clears). Best-effort: a failure to persist the
    /// failure marker is logged, never escalated (the scan failure itself is
    /// already logged by the caller).
    async fn record_scan_failure(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
        bootstrap_from: DateTime<Utc>,
        error: &QuantError,
    ) {
        let existing = match self.cursor_repo.find(source_id, instrument_key).await {
            Ok(existing) => existing,
            Err(find_error) => {
                tracing::warn!(
                    %find_error, %source_id, %instrument_key,
                    "failed to read domain-ingest cursor while recording scan failure; \
                     refusing to invent a checkpoint"
                );
                return;
            }
        };
        // A failed initial fetch has not observed any source event. Persist the
        // configured coverage floor, never `now`, so the retry cannot skip the
        // entire historical bootstrap window.
        let (checkpoint_json, checkpoint_hash) = if let Some(existing) = existing {
            (existing.checkpoint_json, existing.checkpoint_hash)
        } else {
            let checkpoint = DomainSourceCheckpoint::BinanceKline {
                close_time: bootstrap_from,
            };
            let Ok(hash) = CanonicalDigest::content_hash_json(&checkpoint) else {
                tracing::error!(
                    %source_id, %instrument_key,
                    "failed to hash initial domain-ingest error checkpoint"
                );
                return;
            };
            (checkpoint, hash)
        };
        if let Err(persist_error) = self
            .cursor_repo
            .upsert(UpsertDomainSourceCursor {
                source_id: source_id.clone(),
                instrument_key: instrument_key.clone(),
                checkpoint_json,
                checkpoint_hash,
                status: DomainCursorStatus::Failed,
                last_error: Some(error.to_string()),
                updated_at: Utc::now(),
            })
            .await
        {
            tracing::warn!(
                %persist_error, %source_id, %instrument_key,
                "failed to persist domain-ingest error cursor"
            );
        }
    }
}

fn domain_observation_batch_token(rows: &[DomainObservationRow]) -> QuantResult<ContentHash> {
    let identities = rows
        .iter()
        .map(|row| {
            (
                &row.family,
                &row.source_id,
                &row.instrument_key,
                &row.metric,
                row.value,
                row.event_time,
                row.publish_time,
                row.schema_version,
            )
        })
        .collect::<Vec<_>>();
    CanonicalDigest::content_hash_json(&("domain_observation_batch_v1", identities))
        .map_err(Into::into)
}

/// Discover canonical instrument keys from the frozen linkage ruleset and
/// active legacy factor sources. Live event sources are discovered from
/// linkage role projections by their dedicated workers.
#[must_use]
pub fn discover_instruments(
    domain_sources: &DomainSourcesConfig,
) -> HashMap<DomainSourceId, Vec<DomainInstrumentKey>> {
    let mut map = HashMap::new();
    if domain_sources.binance.enabled {
        let keys = rules()
            .iter()
            .filter(|rule| rule.kline_source_id() == DomainSourceId::binance())
            .flat_map(|rule| {
                [
                    rule.instrument_key(),
                    rule.kline_instrument(KlineInterval::OneHour),
                ]
            })
            .collect();
        map.insert(DomainSourceId::binance(), keys);
    }
    if domain_sources.binance_usdm_futures.enabled {
        let keys = rules()
            .iter()
            .filter(|rule| rule.kline_source_id() == DomainSourceId::binance_usdm_futures())
            .flat_map(|rule| {
                [
                    rule.instrument_key(),
                    rule.kline_instrument(KlineInterval::OneHour),
                ]
            })
            .collect();
        map.insert(DomainSourceId::binance_usdm_futures(), keys);
    }
    map
}

fn kline_step(interval: KlineInterval) -> QuantResult<Duration> {
    let seconds = i64::try_from(interval.secs()).map_err(|error| {
        StorageError::invariant_violation(
            Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
            format!("Binance kline interval does not fit chrono duration: {error}"),
        )
    })?;
    Ok(Duration::seconds(seconds))
}

fn validate_archive_observations(
    observations: &[DomainObservation],
    expected: Option<DateTime<Utc>>,
    lower_bound: DateTime<Utc>,
    step: Duration,
) -> QuantResult<()> {
    let first = observations.first().ok_or_else(|| {
        StorageError::invariant_violation(
            Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
            "Binance archive continuity received an empty batch",
        )
    })?;
    if let Some(expected) = expected {
        if first.observed_at != expected {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
                format!(
                    "Binance archive first close {} does not continue cursor at {expected}",
                    first.observed_at
                ),
            )
            .into());
        }
    } else if first.observed_at <= lower_bound || first.observed_at > lower_bound + step {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
            format!(
                "Binance archive first close {} does not cover bootstrap boundary {lower_bound}",
                first.observed_at
            ),
        )
        .into());
    }
    for pair in observations.windows(2) {
        let expected = pair[0].observed_at + step;
        if pair[1].observed_at != expected {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
                format!(
                    "Binance archive close {} does not continue previous close at {expected}",
                    pair[1].observed_at
                ),
            )
            .into());
        }
    }
    Ok(())
}

/// Resume point: exclusive lower bound and whether this tick bootstraps history.
fn resume_point(
    cursor: Option<&DomainSourceCursorInfo>,
    bootstrap_from: DateTime<Utc>,
    now: DateTime<Utc>,
) -> QuantResult<(DateTime<Utc>, bool, DateTime<Utc>)> {
    let Some(row) = cursor else {
        return Ok((bootstrap_from, true, bootstrap_from));
    };
    let last_event_time = row.checkpoint_json.event_time();
    if last_event_time > now {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
            format!(
                "cursor {}/{} has future last_event_time {} after scan boundary {now}",
                row.source_id, row.instrument_key, last_event_time
            ),
        )
        .into());
    }
    Ok(match row.status {
        DomainCursorStatus::Bootstrap => (bootstrap_from, true, last_event_time),
        DomainCursorStatus::Backfilling | DomainCursorStatus::Failed => {
            (last_event_time, true, last_event_time)
        }
        DomainCursorStatus::Live => (last_event_time, false, last_event_time),
    })
}

/// Derive the post-write cursor status from fetch progress.
#[must_use]
fn checkpoint_status(
    cursor: Option<&DomainSourceCursorInfo>,
    bootstrap: bool,
    last_event_time: DateTime<Utc>,
    latest_closed_kline_time: DateTime<Utc>,
) -> DomainCursorStatus {
    if bootstrap && last_event_time < latest_closed_kline_time {
        DomainCursorStatus::Backfilling
    } else if cursor.is_none() && bootstrap {
        DomainCursorStatus::Bootstrap
    } else {
        DomainCursorStatus::Live
    }
}

fn latest_closed_kline_time(
    interval: KlineInterval,
    now: DateTime<Utc>,
) -> QuantResult<DateTime<Utc>> {
    let interval_millis = kline_step(interval)?.num_milliseconds();
    let open_time_millis = now
        .timestamp_millis()
        .div_euclid(interval_millis)
        .checked_mul(interval_millis)
        .ok_or_else(|| {
            StorageError::invariant_violation(
                Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
                "Binance kline frontier overflow",
            )
        })?;
    let close_time_millis = open_time_millis.checked_sub(1).ok_or_else(|| {
        StorageError::invariant_violation(
            Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
            "Binance kline frontier underflow",
        )
    })?;
    DateTime::from_timestamp_millis(close_time_millis).ok_or_else(|| {
        StorageError::invariant_violation(
            Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
            format!("Binance kline frontier `{close_time_millis}` is outside UTC range"),
        )
        .into()
    })
}

fn dedup_observations(observations: Vec<DomainObservation>) -> Vec<DomainObservation> {
    let mut seen = HashSet::<(String, String, i64)>::new();
    let mut deduped = Vec::with_capacity(observations.len());
    for observation in observations {
        let key = (
            observation.instrument_key.as_str().to_owned(),
            observation.metric.as_str().to_owned(),
            observation.observed_at.timestamp_millis(),
        );
        if seen.insert(key) {
            deduped.push(observation);
        }
    }
    deduped
}

#[cfg(test)]
mod tests {
    use super::{
        checkpoint_status, dedup_observations, discover_instruments, latest_closed_kline_time,
        resume_point, validate_archive_observations,
    };
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        config::DomainSourcesConfig,
        domain::{
            DomainCursorStatus, DomainObservation, DomainSourceCheckpoint, DomainSourceCursorInfo,
        },
        enums::domain::{DomainFamily, DomainMetric, KlineInterval},
        types::{BinanceSymbol, ContentHash, DomainInstrumentKey, DomainSourceId},
    };
    use quant_pivot_research::linkage::ruleset::BINANCE_SPOT_ASSETS;
    use rust_decimal_macros::dec;

    #[test]
    fn discover_instruments_respects_deploy_enablement() {
        let config = DomainSourcesConfig::default();
        let map = discover_instruments(&config);
        assert!(map.contains_key(&DomainSourceId::binance()));
        assert!(!map.contains_key(&DomainSourceId::chainlink_data_streams()));
        assert_eq!(
            map[&DomainSourceId::binance()].len(),
            BINANCE_SPOT_ASSETS.len() * 2
        );
    }

    fn cursor(last: chrono::DateTime<Utc>, status: DomainCursorStatus) -> DomainSourceCursorInfo {
        DomainSourceCursorInfo {
            source_id: DomainSourceId::binance(),
            instrument_key: DomainInstrumentKey::binance_kline(
                &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
                KlineInterval::OneMinute,
            ),
            checkpoint_json: DomainSourceCheckpoint::BinanceKline { close_time: last },
            checkpoint_hash: ContentHash::parse(format!("blake3:{}", "a".repeat(64)))
                .expect("checkpoint hash"),
            status,
            last_error: None,
            created_at: last,
            updated_at: last,
        }
    }

    #[test]
    fn resume_point_bootstraps_without_cursor() {
        let from = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let (start, bootstrap, _) = resume_point(None, from, from).expect("resume point");
        assert_eq!(start, from);
        assert!(bootstrap);
    }

    #[test]
    fn resume_point_is_incremental_after_live_cursor() {
        let last = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let cursor = cursor(last, DomainCursorStatus::Live);
        let (start, bootstrap, prior) =
            resume_point(Some(&cursor), last, last).expect("resume point");
        assert_eq!(start, last);
        assert!(!bootstrap);
        assert_eq!(prior, last);
    }

    #[test]
    fn resume_point_keeps_backfilling_and_error_cursors_in_bootstrap_mode() {
        let last = Utc.with_ymd_and_hms(2026, 7, 1, 10, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        for status in [DomainCursorStatus::Backfilling, DomainCursorStatus::Failed] {
            let cursor = cursor(last, status);
            let (start, bootstrap, prior) = resume_point(Some(&cursor), last, now)
                .expect("backfill/error cursor must resume conservatively");
            assert_eq!(start, last);
            assert!(bootstrap);
            assert_eq!(prior, last);
        }
    }

    #[test]
    fn resume_point_rejects_future_checkpoint() {
        let now = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let mut cursor = cursor(now, DomainCursorStatus::Live);
        cursor.checkpoint_json = DomainSourceCheckpoint::BinanceKline {
            close_time: now + chrono::Duration::seconds(1),
        };
        let error = resume_point(Some(&cursor), now, now)
            .expect_err("future persisted checkpoint must fail closed");
        assert!(error.to_string().contains("future last_event_time"));
    }

    #[test]
    fn dedup_observations_keeps_first_occurrence() {
        let key = DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            KlineInterval::OneMinute,
        );
        let at = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let observation = DomainObservation {
            family: DomainFamily::Crypto,
            source_id: DomainSourceId::binance(),
            instrument_key: key,
            metric: DomainMetric::Close,
            value: dec!(1),
            observed_at: at,
            publish_time: at,
            available_at: None,
        };
        let deduped = dedup_observations(vec![observation.clone(), observation]);
        assert_eq!(deduped.len(), 1);
    }

    #[test]
    fn archive_batches_must_cover_bootstrap_and_continue_the_durable_cursor() {
        let key = DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            KlineInterval::OneMinute,
        );
        let lower = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 10).unwrap();
        let first = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 59).unwrap()
            + chrono::Duration::milliseconds(999);
        let observation = |observed_at| DomainObservation {
            family: DomainFamily::Crypto,
            source_id: DomainSourceId::binance(),
            instrument_key: key.clone(),
            metric: DomainMetric::Close,
            value: dec!(1),
            observed_at,
            publish_time: observed_at,
            available_at: Some(observed_at),
        };
        let batch = vec![
            observation(first),
            observation(first + chrono::Duration::minutes(1)),
        ];
        validate_archive_observations(&batch, None, lower, chrono::Duration::minutes(1))
            .expect("initial archive covers bootstrap boundary");
        validate_archive_observations(
            &batch[1..],
            Some(first + chrono::Duration::minutes(1)),
            lower,
            chrono::Duration::minutes(1),
        )
        .expect("next batch continues cursor");
        assert!(
            validate_archive_observations(
                &batch[1..],
                Some(first + chrono::Duration::minutes(2)),
                lower,
                chrono::Duration::minutes(1),
            )
            .is_err()
        );
    }

    #[test]
    fn checkpoint_status_uses_the_interval_closed_bar_frontier() {
        let now = Utc.with_ymd_and_hms(2026, 7, 1, 12, 24, 48).unwrap();
        let one_minute_frontier =
            latest_closed_kline_time(KlineInterval::OneMinute, now).expect("1m frontier");
        let one_hour_frontier =
            latest_closed_kline_time(KlineInterval::OneHour, now).expect("1h frontier");
        assert_eq!(
            one_minute_frontier,
            Utc.with_ymd_and_hms(2026, 7, 1, 12, 23, 59).unwrap()
                + chrono::Duration::milliseconds(999)
        );
        assert_eq!(
            one_hour_frontier,
            Utc.with_ymd_and_hms(2026, 7, 1, 11, 59, 59).unwrap()
                + chrono::Duration::milliseconds(999)
        );
        assert_eq!(
            checkpoint_status(
                None,
                true,
                one_hour_frontier - chrono::Duration::hours(1),
                one_hour_frontier,
            ),
            DomainCursorStatus::Backfilling
        );
        assert_eq!(
            checkpoint_status(None, true, one_hour_frontier, one_hour_frontier),
            DomainCursorStatus::Bootstrap
        );
        assert_eq!(
            checkpoint_status(
                Some(&cursor(one_hour_frontier, DomainCursorStatus::Backfilling)),
                true,
                one_hour_frontier,
                one_hour_frontier,
            ),
            DomainCursorStatus::Live
        );
    }

    #[test]
    fn latest_closed_kline_time_is_stable_at_interval_boundaries() {
        let boundary = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        assert_eq!(
            latest_closed_kline_time(KlineInterval::OneHour, boundary).expect("boundary frontier"),
            boundary - chrono::Duration::milliseconds(1)
        );
        assert_eq!(
            latest_closed_kline_time(
                KlineInterval::OneHour,
                boundary - chrono::Duration::milliseconds(1),
            )
            .expect("pre-boundary frontier"),
            boundary - chrono::Duration::hours(1) - chrono::Duration::milliseconds(1)
        );
    }
}

#[cfg(test)]
mod isolation_tests {
    use super::{CryptoKlineIngestor, discover_instruments};
    use crate::runtime_config::DecisionPolicyStore;
    use async_trait::async_trait;
    use chrono::Utc;
    use quant_pivot_api::domain::{DomainDataSource, DomainFetchRequest};
    use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
    use quant_pivot_models::{
        clickhouse::DomainObservationRow,
        config::DomainSourcesConfig,
        domain::{
            DomainCursorStatus, DomainObservation, DomainSourceCursorInfo, UpsertDomainSourceCursor,
        },
        enums::domain::{DomainFamily, DomainMetric},
        runtime_config::DecisionPolicySnapshot,
        types::{DomainInstrumentKey, DomainSourceId},
    };
    use quant_pivot_repository::traits::{DomainSourceCursorRepository, FactWriter};
    use rust_decimal_macros::dec;
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    /// A source that fails `fetch` for exactly one instrument key, succeeding
    /// (with one synthetic observation) for every other instrument it serves.
    struct PartiallyFailingSource {
        source_id: DomainSourceId,
        fail_instrument: DomainInstrumentKey,
    }

    #[async_trait]
    impl DomainDataSource for PartiallyFailingSource {
        fn family(&self) -> DomainFamily {
            DomainFamily::Crypto
        }

        fn source_id(&self) -> DomainSourceId {
            self.source_id.clone()
        }

        async fn fetch(&self, request: DomainFetchRequest) -> QuantResult<Vec<DomainObservation>> {
            if request.instrument_key == self.fail_instrument {
                return Err(QuantError::config("synthetic fetch failure"));
            }
            Ok(vec![DomainObservation {
                family: DomainFamily::Crypto,
                source_id: self.source_id.clone(),
                instrument_key: request.instrument_key,
                metric: DomainMetric::Close,
                value: dec!(1),
                observed_at: request.to_inclusive,
                publish_time: request.to_inclusive,
                available_at: None,
            }])
        }
    }

    #[derive(Default)]
    struct FakeCursorRepo {
        rows: Mutex<HashMap<(DomainSourceId, DomainInstrumentKey), DomainSourceCursorInfo>>,
    }

    #[async_trait::async_trait]
    impl DomainSourceCursorRepository for FakeCursorRepo {
        async fn find(
            &self,
            source_id: &DomainSourceId,
            instrument_key: &DomainInstrumentKey,
        ) -> Result<Option<DomainSourceCursorInfo>, StorageError> {
            Ok(self
                .rows
                .lock()
                .expect("lock")
                .get(&(source_id.clone(), instrument_key.clone()))
                .cloned())
        }

        async fn upsert(
            &self,
            cursor: UpsertDomainSourceCursor,
        ) -> Result<DomainSourceCursorInfo, StorageError> {
            let now = Utc::now();
            let info = DomainSourceCursorInfo {
                source_id: cursor.source_id,
                instrument_key: cursor.instrument_key,
                checkpoint_json: cursor.checkpoint_json,
                checkpoint_hash: cursor.checkpoint_hash,
                status: cursor.status,
                last_error: cursor.last_error,
                created_at: now,
                updated_at: now,
            };
            self.rows.lock().expect("lock").insert(
                (info.source_id.clone(), info.instrument_key.clone()),
                info.clone(),
            );
            Ok(info)
        }

        async fn list_all(&self) -> Result<Vec<DomainSourceCursorInfo>, StorageError> {
            Ok(self.rows.lock().expect("lock").values().cloned().collect())
        }
    }

    #[derive(Default)]
    struct FakeWriter {
        written: Mutex<Vec<DomainObservationRow>>,
    }

    #[async_trait::async_trait]
    impl FactWriter<DomainObservationRow> for FakeWriter {
        async fn write_batch(&self, rows: Vec<DomainObservationRow>) -> Result<(), StorageError> {
            self.written.lock().expect("lock").extend(rows);
            Ok(())
        }
    }

    struct FailSecondWrite {
        calls: AtomicUsize,
        written: Mutex<Vec<DomainObservationRow>>,
    }

    #[async_trait::async_trait]
    impl FactWriter<DomainObservationRow> for FailSecondWrite {
        async fn write_batch(&self, rows: Vec<DomainObservationRow>) -> Result<(), StorageError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == 2 {
                return Err(StorageError::Connection(
                    "synthetic second archive batch failure".to_owned(),
                ));
            }
            self.written.lock().expect("lock").extend(rows);
            Ok(())
        }
    }

    #[tokio::test]
    async fn archive_cursor_advances_only_after_each_successful_fact_batch() {
        let domain_sources = DomainSourcesConfig::default();
        let cursor_repo = Arc::new(FakeCursorRepo::default());
        let writer = Arc::new(FailSecondWrite {
            calls: AtomicUsize::new(0),
            written: Mutex::new(Vec::new()),
        });
        let ingestor = CryptoKlineIngestor::new(
            Vec::new(),
            Vec::new(),
            Arc::clone(&cursor_repo) as Arc<dyn DomainSourceCursorRepository>,
            Arc::clone(&writer) as Arc<dyn FactWriter<DomainObservationRow>>,
            Arc::new(DecisionPolicyStore::new(DecisionPolicySnapshot::default())),
            domain_sources,
        );
        let instrument = DomainInstrumentKey::new("BINANCE:BTCUSDT:1m");
        let first = Utc::now() - chrono::Duration::minutes(3);
        let second = first + chrono::Duration::minutes(1);
        let third = second + chrono::Duration::minutes(1);
        let observation = |observed_at| DomainObservation {
            family: DomainFamily::Crypto,
            source_id: DomainSourceId::binance(),
            instrument_key: instrument.clone(),
            metric: DomainMetric::Close,
            value: dec!(1),
            observed_at,
            publish_time: observed_at,
            available_at: Some(observed_at),
        };

        ingestor
            .persist_archive_observations(
                &DomainSourceId::binance(),
                &instrument,
                vec![observation(first), observation(second)],
                second,
                2,
            )
            .await
            .expect("first batch persists and commits");
        ingestor
            .persist_archive_observations(
                &DomainSourceId::binance(),
                &instrument,
                vec![observation(third)],
                third,
                2,
            )
            .await
            .expect_err("second fact write fails before cursor commit");
        let after_failure = cursor_repo
            .find(&DomainSourceId::binance(), &instrument)
            .await
            .expect("find cursor")
            .expect("first checkpoint exists");
        assert_eq!(after_failure.checkpoint_json.event_time(), second);

        ingestor
            .persist_archive_observations(
                &DomainSourceId::binance(),
                &instrument,
                vec![observation(third)],
                third,
                2,
            )
            .await
            .expect("retry resumes from last committed batch");
        let after_retry = cursor_repo
            .find(&DomainSourceId::binance(), &instrument)
            .await
            .expect("find cursor")
            .expect("retry checkpoint exists");
        assert_eq!(after_retry.checkpoint_json.event_time(), third);
        assert_eq!(writer.written.lock().expect("lock").len(), 3);
    }

    #[tokio::test]
    async fn one_instrument_failure_does_not_abort_the_tick() {
        let domain_sources = DomainSourcesConfig::default();
        let instruments = discover_instruments(&domain_sources);
        let binance_instruments = instruments
            .get(&DomainSourceId::binance())
            .cloned()
            .expect("binance instruments discovered");
        assert!(
            binance_instruments.len() > 1,
            "test needs at least 2 instruments to prove isolation"
        );
        let fail_instrument = binance_instruments[0].clone();
        let healthy_instrument = binance_instruments[1].clone();

        let source: Arc<dyn DomainDataSource> = Arc::new(PartiallyFailingSource {
            source_id: DomainSourceId::binance(),
            fail_instrument: fail_instrument.clone(),
        });
        let cursor_repo = Arc::new(FakeCursorRepo::default());
        let writer = Arc::new(FakeWriter::default());
        let runtime_config = Arc::new(DecisionPolicyStore::new(DecisionPolicySnapshot::default()));

        let ingestor = CryptoKlineIngestor::new(
            vec![source],
            Vec::new(),
            Arc::clone(&cursor_repo) as Arc<dyn DomainSourceCursorRepository>,
            Arc::clone(&writer) as Arc<dyn FactWriter<DomainObservationRow>>,
            runtime_config,
            domain_sources,
        );

        ingestor.run_once().await.expect("tick must not abort");

        let failed_cursor = cursor_repo
            .find(&DomainSourceId::binance(), &fail_instrument)
            .await
            .expect("find")
            .expect("failed instrument gets a cursor row");
        assert_eq!(failed_cursor.status, DomainCursorStatus::Failed);
        assert!(
            failed_cursor
                .last_error
                .as_deref()
                .is_some_and(|detail| detail.contains("synthetic fetch failure")),
            "last_error must record the failure detail"
        );

        let healthy_cursor = cursor_repo
            .find(&DomainSourceId::binance(), &healthy_instrument)
            .await
            .expect("find")
            .expect("healthy instrument still commits a cursor");
        assert_ne!(healthy_cursor.status, DomainCursorStatus::Failed);
        assert!(
            healthy_cursor.last_error.is_none(),
            "a successful instrument must not carry a stale error"
        );

        assert!(
            !writer.written.lock().expect("lock").is_empty(),
            "the healthy instrument's observation must still be written"
        );
    }
}
