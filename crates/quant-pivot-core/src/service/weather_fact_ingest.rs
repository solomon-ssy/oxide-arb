//! Idempotent PIT persistence for long-form Weather facts.
//!
//! Source adapters never assign durable revisions. This service compares each
//! source-native report with already-visible `ClickHouse` facts, suppresses exact
//! retries, and assigns a strictly increasing revision plus supersession edge
//! for corrections of the same source/instrument/variable/event identity.

use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
    sync::Arc,
};

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    clickhouse::{WeatherForecastFactRow, WeatherObservationFactRow},
    domain::data_plane::{WeatherForecastPoint, WeatherObservationReport},
    hashing::CanonicalDigest,
    types::{ContentHash, DomainInstrumentKey, DomainSourceId, WeatherVariable},
};
use quant_pivot_repository::traits::{FactWriter, QuantFactReadRepository};

const WRITE_BATCH_SIZE: usize = 5_000;
const MAX_PARTITIONS_PER_WRITE: usize = 64;

#[derive(Debug, Clone)]
pub struct WeatherObservationCandidate {
    pub report: WeatherObservationReport,
    pub local_date: NaiveDate,
}

#[derive(Debug, Clone)]
pub struct WeatherObservationPersistence {
    pub report: WeatherObservationReport,
    pub local_date: NaiveDate,
    pub revision: u32,
    pub supersedes_report_hash: Option<ContentHash>,
    pub inserted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WeatherWriteSummary {
    pub candidates: usize,
    pub inserted: usize,
    pub duplicates: usize,
}

#[derive(Clone)]
pub struct WeatherFactIngestService {
    observation_writer: Arc<dyn FactWriter<WeatherObservationFactRow>>,
    forecast_writer: Arc<dyn FactWriter<WeatherForecastFactRow>>,
    fact_read: Arc<dyn QuantFactReadRepository>,
}

impl WeatherFactIngestService {
    #[must_use]
    pub fn new(
        observation_writer: Arc<dyn FactWriter<WeatherObservationFactRow>>,
        forecast_writer: Arc<dyn FactWriter<WeatherForecastFactRow>>,
        fact_read: Arc<dyn QuantFactReadRepository>,
    ) -> Self {
        Self {
            observation_writer,
            forecast_writer,
            fact_read,
        }
    }

    pub async fn persist_observations(
        &self,
        candidates: Vec<WeatherObservationCandidate>,
    ) -> QuantResult<Vec<WeatherObservationPersistence>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let subjects = candidates
            .iter()
            .map(|candidate| candidate.report.subject_key.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let from_ms = candidates
            .iter()
            .map(|candidate| candidate.report.observed_at.timestamp_millis())
            .min()
            .ok_or_else(|| QuantError::config("Weather observation candidate set is empty"))?;
        let to_ms = candidates
            .iter()
            .map(|candidate| candidate.report.observed_at.timestamp_millis())
            .max()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| QuantError::config("Weather observation query window overflow"))?;
        let decision_at_ms = candidates
            .iter()
            .map(|candidate| candidate.report.available_at.timestamp_millis())
            .max()
            .ok_or_else(|| QuantError::config("Weather observation availability is absent"))?;
        let existing = self
            .fact_read
            .weather_observation_facts_between(
                subjects,
                from_ms,
                to_ms,
                decision_at_ms,
                decision_at_ms,
            )
            .await?;
        let persisted = plan_observation_persistence(existing, candidates)?;
        let rows = persisted
            .iter()
            .filter(|item| item.inserted)
            .map(|item| {
                item.report.to_clickhouse_row(
                    item.local_date,
                    item.revision,
                    item.supersedes_report_hash,
                )
            })
            .collect::<Vec<_>>();
        for batch in partition_aware_batches(rows, |row| row.observed_at)? {
            let token = weather_observation_batch_token(&batch)?;
            self.observation_writer
                .write_batch_idempotent(&token, batch)
                .await?;
        }
        Ok(persisted)
    }

    pub async fn persist_forecasts(
        &self,
        candidates: Vec<WeatherForecastPoint>,
    ) -> QuantResult<WeatherWriteSummary> {
        if candidates.is_empty() {
            return Ok(WeatherWriteSummary {
                candidates: 0,
                inserted: 0,
                duplicates: 0,
            });
        }
        let subjects = candidates
            .iter()
            .map(|candidate| candidate.subject_key.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let valid_from_ms = candidates
            .iter()
            .map(|candidate| candidate.valid_time.timestamp_millis())
            .min()
            .ok_or_else(|| QuantError::config("Weather forecast candidate set is empty"))?;
        let valid_to_ms = candidates
            .iter()
            .map(|candidate| candidate.valid_time.timestamp_millis())
            .max()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| QuantError::config("Weather forecast query window overflow"))?;
        let reference_cutoff_ms = candidates
            .iter()
            .map(|candidate| candidate.reference_time.timestamp_millis())
            .max()
            .ok_or_else(|| QuantError::config("Weather forecast reference time is absent"))?;
        let decision_at_ms = candidates
            .iter()
            .map(|candidate| candidate.available_at.timestamp_millis())
            .max()
            .ok_or_else(|| QuantError::config("Weather forecast availability is absent"))?;
        let existing = self
            .fact_read
            .weather_forecast_facts_between(
                subjects,
                valid_from_ms,
                valid_to_ms,
                reference_cutoff_ms,
                decision_at_ms,
            )
            .await?;
        let rows = plan_forecast_rows(existing, candidates)?;
        let candidates = rows.len();
        let inserted = rows.iter().filter(|row| row.is_some()).count();
        let inserted_rows = rows.into_iter().flatten().collect::<Vec<_>>();
        for batch in partition_aware_batches(inserted_rows, |row| row.reference_time)? {
            let token = weather_forecast_batch_token(&batch)?;
            self.forecast_writer
                .write_batch_idempotent(&token, batch)
                .await?;
        }
        Ok(WeatherWriteSummary {
            candidates,
            inserted,
            duplicates: candidates.saturating_sub(inserted),
        })
    }
}

fn weather_observation_batch_token(rows: &[WeatherObservationFactRow]) -> QuantResult<ContentHash> {
    let identities = rows
        .iter()
        .map(|row| {
            (
                &row.source_id,
                &row.instrument_key,
                &row.variable,
                row.observed_at,
                row.revision,
                &row.report_hash,
            )
        })
        .collect::<Vec<_>>();
    CanonicalDigest::content_hash_json(&("weather_observation_batch_v1", identities))
        .map_err(Into::into)
}

fn weather_forecast_batch_token(rows: &[WeatherForecastFactRow]) -> QuantResult<ContentHash> {
    let identities = rows
        .iter()
        .map(|row| {
            (
                &row.source_id,
                &row.instrument_key,
                &row.variable,
                row.reference_time,
                row.valid_time,
                row.member,
                row.revision,
                &row.report_hash,
            )
        })
        .collect::<Vec<_>>();
    CanonicalDigest::content_hash_json(&("weather_forecast_batch_v1", identities))
        .map_err(Into::into)
}

fn partition_aware_batches<T>(
    mut rows: Vec<T>,
    partition_timestamp_ms: impl Fn(&T) -> i64,
) -> QuantResult<Vec<Vec<T>>> {
    rows.sort_by_key(|row| partition_timestamp_ms(row));
    let mut batches = Vec::new();
    let mut batch = Vec::with_capacity(WRITE_BATCH_SIZE);
    let mut partitions = BTreeSet::new();
    for row in rows {
        let timestamp_ms = partition_timestamp_ms(&row);
        let timestamp = DateTime::<Utc>::from_timestamp_millis(timestamp_ms).ok_or_else(|| {
            QuantError::config(format!(
                "Weather fact partition timestamp `{timestamp_ms}` is outside chrono range"
            ))
        })?;
        let partition = (timestamp.year(), timestamp.month());
        let introduces_partition = !partitions.contains(&partition);
        if !batch.is_empty()
            && (batch.len() == WRITE_BATCH_SIZE
                || (introduces_partition && partitions.len() == MAX_PARTITIONS_PER_WRITE))
        {
            batches.push(mem::replace(
                &mut batch,
                Vec::with_capacity(WRITE_BATCH_SIZE),
            ));
            partitions.clear();
        }
        partitions.insert(partition);
        batch.push(row);
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    Ok(batches)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ObservationIdentity {
    source_id: DomainSourceId,
    instrument_key: DomainInstrumentKey,
    variable: WeatherVariable,
    observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ForecastIdentity {
    source_id: DomainSourceId,
    instrument_key: DomainInstrumentKey,
    variable: WeatherVariable,
    reference_time_ms: i64,
    valid_time_ms: i64,
    member: Option<u16>,
}

#[derive(Default)]
struct RevisionState {
    by_hash: BTreeMap<ContentHash, u32>,
    latest: Option<(u32, i64, ContentHash)>,
}

fn plan_observation_persistence(
    existing: Vec<WeatherObservationFactRow>,
    mut candidates: Vec<WeatherObservationCandidate>,
) -> QuantResult<Vec<WeatherObservationPersistence>> {
    let mut states = BTreeMap::<ObservationIdentity, RevisionState>::new();
    for row in existing {
        let variable = WeatherVariable::parse(&row.variable).ok_or_else(|| {
            QuantError::config("persisted Weather observation variable is invalid")
        })?;
        let key = ObservationIdentity {
            source_id: row.source_id,
            instrument_key: row.instrument_key,
            variable,
            observed_at_ms: row.observed_at,
        };
        add_existing_revision(
            states.entry(key).or_default(),
            row.report_hash,
            row.revision,
            row.available_at,
        )?;
    }
    candidates.sort_by(|left, right| {
        observation_identity(&left.report)
            .cmp(&observation_identity(&right.report))
            .then_with(|| left.report.available_at.cmp(&right.report.available_at))
            .then_with(|| left.report.report_hash.cmp(&right.report.report_hash))
    });
    let mut persisted = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let state = states
            .entry(observation_identity(&candidate.report))
            .or_default();
        if let Some(revision) = state.by_hash.get(&candidate.report.report_hash).copied() {
            persisted.push(WeatherObservationPersistence {
                report: candidate.report,
                local_date: candidate.local_date,
                revision,
                supersedes_report_hash: None,
                inserted: false,
            });
            continue;
        }
        let revision = state.latest.as_ref().map_or(Ok(0), |(revision, _, _)| {
            revision
                .checked_add(1)
                .ok_or_else(|| QuantError::config("Weather observation revision overflow"))
        })?;
        let supersedes_report_hash = state
            .latest
            .as_ref()
            .map(|(_, _, report_hash)| *report_hash);
        add_existing_revision(
            state,
            candidate.report.report_hash,
            revision,
            candidate.report.available_at.timestamp_millis(),
        )?;
        persisted.push(WeatherObservationPersistence {
            report: candidate.report,
            local_date: candidate.local_date,
            revision,
            supersedes_report_hash,
            inserted: true,
        });
    }
    Ok(persisted)
}

fn plan_forecast_rows(
    existing: Vec<WeatherForecastFactRow>,
    mut candidates: Vec<WeatherForecastPoint>,
) -> QuantResult<Vec<Option<WeatherForecastFactRow>>> {
    let mut states = BTreeMap::<ForecastIdentity, RevisionState>::new();
    for row in existing {
        let variable = WeatherVariable::parse(&row.variable)
            .ok_or_else(|| QuantError::config("persisted Weather forecast variable is invalid"))?;
        let key = ForecastIdentity {
            source_id: row.source_id,
            instrument_key: row.instrument_key,
            variable,
            reference_time_ms: row.reference_time,
            valid_time_ms: row.valid_time,
            member: row.member,
        };
        add_existing_revision(
            states.entry(key).or_default(),
            row.report_hash,
            row.revision,
            row.available_at,
        )?;
    }
    candidates.sort_by(|left, right| {
        forecast_identity(left)
            .cmp(&forecast_identity(right))
            .then_with(|| left.available_at.cmp(&right.available_at))
            .then_with(|| left.report_hash.cmp(&right.report_hash))
    });
    let mut rows = Vec::with_capacity(candidates.len());
    for mut candidate in candidates {
        let state = states.entry(forecast_identity(&candidate)).or_default();
        if state.by_hash.contains_key(&candidate.report_hash) {
            rows.push(None);
            continue;
        }
        candidate.revision = state.latest.as_ref().map_or(Ok(0), |(revision, _, _)| {
            revision
                .checked_add(1)
                .ok_or_else(|| QuantError::config("Weather forecast revision overflow"))
        })?;
        add_existing_revision(
            state,
            candidate.report_hash,
            candidate.revision,
            candidate.available_at.timestamp_millis(),
        )?;
        rows.push(Some(WeatherForecastFactRow::from(&candidate)));
    }
    Ok(rows)
}

fn add_existing_revision(
    state: &mut RevisionState,
    report_hash: ContentHash,
    revision: u32,
    available_at_ms: i64,
) -> QuantResult<()> {
    if let Some(existing_revision) = state.by_hash.insert(report_hash, revision)
        && existing_revision != revision
    {
        return Err(QuantError::config(
            "one Weather report hash is persisted under multiple revisions",
        ));
    }
    let candidate = (revision, available_at_ms, report_hash);
    if state
        .latest
        .as_ref()
        .is_none_or(|latest| candidate > *latest)
    {
        state.latest = Some(candidate);
    }
    Ok(())
}

fn observation_identity(report: &WeatherObservationReport) -> ObservationIdentity {
    ObservationIdentity {
        source_id: report.source_id.clone(),
        instrument_key: report.instrument_key.clone(),
        variable: report.variable,
        observed_at_ms: report.observed_at.timestamp_millis(),
    }
}

fn forecast_identity(point: &WeatherForecastPoint) -> ForecastIdentity {
    ForecastIdentity {
        source_id: point.source_id.clone(),
        instrument_key: point.instrument_key.clone(),
        variable: point.variable,
        reference_time_ms: point.reference_time.timestamp_millis(),
        valid_time_ms: point.valid_time.timestamp_millis(),
        member: point.member,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
    use quant_pivot_models::{
        domain::data_plane::{WeatherObservationReport, WeatherObservationReportKind},
        types::{
            ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId,
            WeatherVariable,
        },
    };
    use rust_decimal_macros::dec;

    use super::{
        MAX_PARTITIONS_PER_WRITE, WeatherObservationCandidate, partition_aware_batches,
        plan_observation_persistence,
    };

    fn hash(fill: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", fill.to_string().repeat(64))).expect("hash")
    }

    fn report(fill: char, available_minute: u32) -> WeatherObservationReport {
        WeatherObservationReport {
            source_id: DomainSourceId::hko_open_data(),
            instrument_key: DomainInstrumentKey::hko_rainfall("North District"),
            subject_key: "North District".to_owned(),
            report_kind: WeatherObservationReportKind::HkoRainfall,
            variable: WeatherVariable::Precipitation,
            value: dec!(1.2),
            unit: DomainMeasurementUnit::Millimeter,
            precision: dec!(0.1),
            observed_at: Utc.with_ymd_and_hms(2026, 7, 18, 8, 0, 0).unwrap(),
            valid_from: None,
            valid_to: None,
            published_at: Utc
                .with_ymd_and_hms(2026, 7, 18, 8, available_minute, 0)
                .unwrap(),
            available_at: Utc
                .with_ymd_and_hms(2026, 7, 18, 8, available_minute, 1)
                .unwrap(),
            report_hash: hash(fill),
            raw_report: "{}".to_owned(),
        }
    }

    #[test]
    fn correction_chain_monotonic_suppressed() {
        let local_date = NaiveDate::from_ymd_opt(2026, 7, 18).expect("date");
        let first = report('a', 1);
        let correction = report('b', 2);
        let planned = plan_observation_persistence(
            Vec::new(),
            vec![
                WeatherObservationCandidate {
                    report: correction,
                    local_date,
                },
                WeatherObservationCandidate {
                    report: first.clone(),
                    local_date,
                },
                WeatherObservationCandidate {
                    report: first.clone(),
                    local_date,
                },
            ],
        )
        .expect("plan");
        assert_eq!(planned[0].revision, 0);
        assert!(planned[0].inserted);
        assert_eq!(planned[1].revision, 0);
        assert!(!planned[1].inserted);
        assert_eq!(planned[2].revision, 1);
        assert_eq!(planned[2].supersedes_report_hash, Some(first.report_hash));
        assert!(planned[2].inserted);
    }

    #[test]
    fn historical_rows_batched_limit() {
        let rows = (0_u32..=240)
            .map(|month| {
                let year_offset = i32::try_from(month / 12).expect("test year offset");
                Utc.with_ymd_and_hms(2000 + year_offset, month % 12 + 1, 1, 0, 0, 0)
                    .unwrap()
                    .timestamp_millis()
            })
            .rev()
            .collect::<Vec<_>>();
        let batches = partition_aware_batches(rows.clone(), |timestamp| *timestamp)
            .expect("partition-aware batches");
        assert!(batches.len() > 1);
        assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), rows.len());
        assert!(batches.iter().all(|batch| {
            batch
                .iter()
                .map(|timestamp| {
                    let value = DateTime::<Utc>::from_timestamp_millis(*timestamp)
                        .expect("valid timestamp");
                    (value.year(), value.month())
                })
                .collect::<BTreeSet<_>>()
                .len()
                <= MAX_PARTITIONS_PER_WRITE
        }));
        assert!(
            batches
                .windows(2)
                .all(|pair| pair[0].last() < pair[1].first())
        );
    }
}
