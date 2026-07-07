//! External domain-source ingestion (Phase 11.2.2).
//!
//! [`DomainIngestor`] polls enabled [`DomainDataSource`] clients, normalizes
//! observations into `quant_domain_observation`, and advances durable
//! `(source, instrument)` cursors only after a successful `ClickHouse` write.

use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_api::domain::{DomainDataSource, DomainFetchRequest};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    clickhouse::DomainObservationRow,
    config::DomainSourcesConfig,
    domain::{
        DomainCursorStatus, DomainObservation, DomainSourceCursorInfo, UpsertDomainSourceCursor,
    },
    enums::domain::DomainFamily,
    types::{DomainInstrumentKey, DomainSourceId},
};
use quant_pivot_repository::{
    clickhouse::ChFactWriter,
    traits::{DomainSourceCursorRepository, FactWriter},
};
use quant_pivot_research::linkage::rules;

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
    last_event_time: DateTime<Utc>,
    status: DomainCursorStatus,
}

/// Polls external domain sources and writes normalized observations to
/// `quant_domain_observation`.
pub struct DomainIngestor {
    sources: Vec<Arc<dyn DomainDataSource>>,
    cursor_repo: Arc<dyn DomainSourceCursorRepository>,
    writer: Arc<ChFactWriter<DomainObservationRow>>,
    runtime_config: Arc<RuntimeConfigStore>,
    domain_sources: DomainSourcesConfig,
    instruments_by_source: HashMap<DomainSourceId, Vec<DomainInstrumentKey>>,
}

impl DomainIngestor {
    #[must_use]
    pub fn new(
        sources: Vec<Arc<dyn DomainDataSource>>,
        cursor_repo: Arc<dyn DomainSourceCursorRepository>,
        writer: Arc<ChFactWriter<DomainObservationRow>>,
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
    pub async fn run_once(&self) -> QuantResult<()> {
        let runtime = self.runtime_config.load();
        if !runtime.domain.family_enabled(DomainFamily::Crypto) {
            return Ok(());
        }

        let backfill_days = runtime.domain.crypto.backfill_days;
        let now = Utc::now();
        let batch_size = self.domain_sources.binance.batch_size.max(1);

        let mut outcomes = Vec::new();
        for source in &self.sources {
            let source_id = source.source_id();
            let Some(instruments) = self.instruments_by_source.get(&source_id) else {
                continue;
            };
            for instrument_key in instruments {
                outcomes.push(
                    self.scan_instrument(source.as_ref(), instrument_key, backfill_days, now)
                        .await?,
                );
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
        backfill_days: u32,
        now: DateTime<Utc>,
    ) -> QuantResult<InstrumentScanOutcome> {
        let source_id = source.source_id();
        let cursor = self.cursor_repo.find(&source_id, instrument_key).await?;
        let bootstrap_from = now - Duration::days(i64::from(backfill_days.max(1)));
        let (from_exclusive, bootstrap, prior_last_event_time) =
            resume_point(cursor.as_ref(), bootstrap_from);

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

        Ok(InstrumentScanOutcome {
            source_id: source_id.clone(),
            instrument_key: instrument_key.clone(),
            observations,
            checkpoint: InstrumentCheckpoint {
                source_id,
                instrument_key: instrument_key.clone(),
                last_event_time,
                status,
            },
        })
    }

    async fn commit_checkpoint(&self, checkpoint: InstrumentCheckpoint) -> QuantResult<()> {
        self.cursor_repo
            .upsert(UpsertDomainSourceCursor {
                source_id: checkpoint.source_id,
                instrument_key: checkpoint.instrument_key,
                last_event_time: checkpoint.last_event_time,
                status: checkpoint.status.as_str().to_owned(),
                updated_at: Utc::now(),
            })
            .await?;
        Ok(())
    }
}

/// Discover canonical instrument keys from the frozen linkage ruleset and
/// deploy-config Chainlink feed map.
#[must_use]
pub fn discover_instruments(
    domain_sources: &DomainSourcesConfig,
) -> HashMap<DomainSourceId, Vec<DomainInstrumentKey>> {
    let mut map = HashMap::new();
    if domain_sources.binance.enabled {
        let keys = rules()
            .iter()
            .map(quant_pivot_research::linkage::AssetRule::instrument_key)
            .collect();
        map.insert(DomainSourceId::binance(), keys);
    }
    if domain_sources.chainlink.enabled {
        let keys = rules()
            .iter()
            .filter(|rule| {
                domain_sources
                    .chainlink
                    .feeds
                    .contains_key(rule.chainlink_feed)
            })
            .map(|rule| DomainInstrumentKey::chainlink_feed(&rule.feed()))
            .collect();
        map.insert(DomainSourceId::chainlink(), keys);
    }
    map
}

/// Resume point: exclusive lower bound and whether this tick bootstraps history.
fn resume_point(
    cursor: Option<&DomainSourceCursorInfo>,
    bootstrap_from: DateTime<Utc>,
) -> (DateTime<Utc>, bool, DateTime<Utc>) {
    cursor.map_or((bootstrap_from, true, bootstrap_from), |row| {
        let status =
            DomainCursorStatus::parse(&row.status).unwrap_or(DomainCursorStatus::Bootstrap);
        if status == DomainCursorStatus::Bootstrap {
            (bootstrap_from, true, row.last_event_time)
        } else {
            (row.last_event_time, false, row.last_event_time)
        }
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
    let mut seen = std::collections::HashSet::<(String, String, i64)>::new();
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
        domain::{DomainCursorStatus, DomainObservation, DomainSourceCursorInfo},
        enums::domain::{DomainFamily, DomainMetric, KlineInterval},
        types::{BinanceSymbol, DomainInstrumentKey, DomainSourceId},
    };
    use rust_decimal_macros::dec;

    #[test]
    fn discover_instruments_respects_deploy_enablement() {
        let mut config = DomainSourcesConfig::default();
        config.chainlink.enabled = false;
        let map = discover_instruments(&config);
        assert!(map.contains_key(&DomainSourceId::binance()));
        assert!(!map.contains_key(&DomainSourceId::chainlink()));
        assert_eq!(map[&DomainSourceId::binance()].len(), 5);
    }

    #[test]
    fn chainlink_instruments_intersect_ruleset_and_deploy_feeds() {
        let mut config = DomainSourcesConfig::default();
        config.chainlink.feeds.clear();
        config
            .chainlink
            .feeds
            .insert("BTC-USD".to_owned(), "0xabc".to_owned());
        let map = discover_instruments(&config);
        let keys = &map[&DomainSourceId::chainlink()];
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].as_str(), "CHAINLINK:BTC-USD");
    }

    #[test]
    fn resume_point_bootstraps_without_cursor() {
        let from = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let (start, bootstrap, _) = resume_point(None, from);
        assert_eq!(start, from);
        assert!(bootstrap);
    }

    #[test]
    fn resume_point_is_incremental_after_live_cursor() {
        let last = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let cursor = DomainSourceCursorInfo {
            source_id: DomainSourceId::binance(),
            instrument_key: DomainInstrumentKey::binance_kline(
                &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
                KlineInterval::OneMinute,
            ),
            last_event_time: last,
            status: DomainCursorStatus::Live.as_str().to_owned(),
            created_at: last,
            updated_at: last,
        };
        let (start, bootstrap, prior) = resume_point(Some(&cursor), last);
        assert_eq!(start, last);
        assert!(!bootstrap);
        assert_eq!(prior, last);
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
