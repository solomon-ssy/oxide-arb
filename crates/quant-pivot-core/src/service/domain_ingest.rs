//! External domain-source ingestion (Phase 11.2.2).
//!
//! [`DomainIngestor`] polls enabled [`DomainDataSource`] clients, normalizes
//! observations into `quant_domain_observation`, and advances durable
//! `(source, instrument)` cursors only after a successful `ClickHouse` write.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_api::domain::{DomainDataSource, DomainFetchRequest};
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
    enums::domain::DomainFamily,
    hashing::CanonicalDigest,
    types::{ContentHash, DomainInstrumentKey, DomainSourceId},
};
use quant_pivot_repository::traits::{DomainSourceCursorRepository, FactWriter};
use quant_pivot_research::linkage::{AssetRule, rules};

use crate::runtime_config::RuntimeConfigStore;

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

/// Polls external domain sources and writes normalized observations to
/// `quant_domain_observation`.
pub struct DomainIngestor {
    sources: Vec<Arc<dyn DomainDataSource>>,
    cursor_repo: Arc<dyn DomainSourceCursorRepository>,
    writer: Arc<dyn FactWriter<DomainObservationRow>>,
    runtime_config: Arc<RuntimeConfigStore>,
    domain_sources: DomainSourcesConfig,
    instruments_by_source: HashMap<DomainSourceId, Vec<DomainInstrumentKey>>,
}

impl DomainIngestor {
    #[must_use]
    pub fn new(
        sources: Vec<Arc<dyn DomainDataSource>>,
        cursor_repo: Arc<dyn DomainSourceCursorRepository>,
        writer: Arc<dyn FactWriter<DomainObservationRow>>,
        runtime_config: Arc<RuntimeConfigStore>,
        domain_sources: DomainSourcesConfig,
    ) -> Self {
        Self {
            sources,
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
    /// failure records that instrument's cursor as [`DomainCursorStatus::Error`]
    /// with the failure detail and is skipped, but never aborts the whole
    /// tick — every other instrument still scans, and the batch still writes
    /// and commits for every instrument that succeeded.
    pub async fn run_once(&self) -> QuantResult<()> {
        let runtime = self.runtime_config.load();
        if !runtime.domain.family_enabled(DomainFamily::Crypto) {
            return Ok(());
        }

        let backfill_days = runtime.domain.crypto.backfill_days;
        let now = Utc::now();
        let bootstrap_from = now - Duration::days(i64::from(backfill_days.max(1)));
        let batch_size = self.domain_sources.binance.batch_size.max(1);

        let mut outcomes = Vec::new();
        for source in &self.sources {
            let source_id = source.source_id();
            let Some(instruments) = self.instruments_by_source.get(&source_id) else {
                continue;
            };
            for instrument_key in instruments {
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
                self.writer.write_batch(batch.to_vec()).await?;
            }
        }

        for checkpoint in outcomes.into_iter().map(|outcome| outcome.checkpoint) {
            self.commit_checkpoint(checkpoint).await?;
        }
        Ok(())
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

        let last_event_time = observations
            .iter()
            .map(|observation| observation.observed_at)
            .max()
            .unwrap_or(prior_last_event_time);
        let status = checkpoint_status(cursor.as_ref(), bootstrap, last_event_time, now);
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
                status: checkpoint.status.as_str().to_owned(),
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
        if let Some(row) = existing.as_ref()
            && DomainCursorStatus::parse(&row.status).is_none()
        {
            tracing::error!(
                status = %row.status, %source_id, %instrument_key,
                "domain-ingest cursor has an unknown persisted status; \
                 refusing to overwrite the invalid state"
            );
            return;
        }
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
                status: DomainCursorStatus::Error.as_str().to_owned(),
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

/// Discover canonical instrument keys from the frozen linkage ruleset and
/// active legacy factor sources. Live event sources are discovered from
/// linkage role projections by their dedicated workers.
#[must_use]
pub fn discover_instruments(
    domain_sources: &DomainSourcesConfig,
) -> HashMap<DomainSourceId, Vec<DomainInstrumentKey>> {
    let mut map = HashMap::new();
    if domain_sources.binance.enabled {
        let keys = rules().iter().map(AssetRule::instrument_key).collect();
        map.insert(DomainSourceId::binance(), keys);
    }
    map
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
    let status = DomainCursorStatus::parse(&row.status).ok_or_else(|| {
        StorageError::invariant_violation(
            Some(entity::QUANT_DOMAIN_SOURCE_CURSOR),
            format!(
                "cursor {}/{} has unknown status `{}`",
                row.source_id, row.instrument_key, row.status
            ),
        )
    })?;
    Ok(match status {
        DomainCursorStatus::Bootstrap => (bootstrap_from, true, last_event_time),
        DomainCursorStatus::Backfilling | DomainCursorStatus::Error => {
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
    now: DateTime<Utc>,
) -> DomainCursorStatus {
    let live_edge = now - Duration::minutes(2);
    if bootstrap && last_event_time < live_edge {
        DomainCursorStatus::Backfilling
    } else if cursor.is_none() && bootstrap {
        DomainCursorStatus::Bootstrap
    } else {
        DomainCursorStatus::Live
    }
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
    use super::{checkpoint_status, dedup_observations, discover_instruments, resume_point};
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        config::DomainSourcesConfig,
        domain::{
            DomainCursorStatus, DomainObservation, DomainSourceCheckpoint, DomainSourceCursorInfo,
        },
        enums::domain::{DomainFamily, DomainMetric, KlineInterval},
        types::{BinanceSymbol, ContentHash, DomainInstrumentKey, DomainSourceId},
    };
    use rust_decimal_macros::dec;

    #[test]
    fn discover_instruments_respects_deploy_enablement() {
        let config = DomainSourcesConfig::default();
        let map = discover_instruments(&config);
        assert!(map.contains_key(&DomainSourceId::binance()));
        assert!(!map.contains_key(&DomainSourceId::chainlink_data_streams()));
        assert_eq!(map[&DomainSourceId::binance()].len(), 5);
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
            status: status.as_str().to_owned(),
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
        for status in [DomainCursorStatus::Backfilling, DomainCursorStatus::Error] {
            let cursor = cursor(last, status);
            let (start, bootstrap, prior) = resume_point(Some(&cursor), last, now)
                .expect("backfill/error cursor must resume conservatively");
            assert_eq!(start, last);
            assert!(bootstrap);
            assert_eq!(prior, last);
        }
    }

    #[test]
    fn resume_point_rejects_unknown_status_and_future_checkpoint() {
        let now = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let mut cursor = cursor(now, DomainCursorStatus::Live);
        cursor.status = "legacy_unknown".to_owned();
        let error = resume_point(Some(&cursor), now, now)
            .expect_err("unknown persisted status must fail closed");
        assert!(error.to_string().contains("unknown status"));

        cursor.status = DomainCursorStatus::Live.as_str().to_owned();
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
    fn checkpoint_status_marks_backfill_until_live_edge() {
        let now = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let stale = now - chrono::Duration::hours(1);
        assert_eq!(
            checkpoint_status(None, true, stale, now),
            DomainCursorStatus::Backfilling
        );
        assert_eq!(
            checkpoint_status(None, false, now, now),
            DomainCursorStatus::Live
        );
    }
}

#[cfg(test)]
mod isolation_tests {
    use super::{DomainIngestor, discover_instruments};
    use crate::runtime_config::RuntimeConfigStore;
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
        runtime_config::RuntimeConfig,
        types::{DomainInstrumentKey, DomainSourceId},
    };
    use quant_pivot_repository::traits::{DomainSourceCursorRepository, FactWriter};
    use rust_decimal_macros::dec;
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
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
        let runtime_config = Arc::new(RuntimeConfigStore::new(RuntimeConfig::default()));

        let ingestor = DomainIngestor::new(
            vec![source],
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
        assert_eq!(
            DomainCursorStatus::parse(&failed_cursor.status),
            Some(DomainCursorStatus::Error)
        );
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
        assert_ne!(
            DomainCursorStatus::parse(&healthy_cursor.status),
            Some(DomainCursorStatus::Error)
        );
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
