//! Seal-first `ClickHouse` partition retention with content-addressed Parquet archives.

use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
use quant_pivot_models::{
    domain::{ArchivePartitionManifestInfo, NewArchivePartitionManifest},
    hashing::CanonicalDigest,
    types::ContentHash,
};
use quant_pivot_repository::traits::ArchivePartitionRepository;
use quant_pivot_research::artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore};
use quant_pivot_storage::clickhouse::ClickHousePool;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::infra::periodic_task::PeriodicTask;

const LEASE_DURATION: Duration = Duration::from_mins(5);

#[derive(Clone, Copy)]
struct ArchiveTableSpec {
    table: &'static str,
    time_column: &'static str,
    order_by: &'static str,
    retention_days: i32,
    final_rows: bool,
}

const TABLES: [ArchiveTableSpec; 6] = [
    ArchiveTableSpec {
        table: "quant_crypto_price_report",
        time_column: "event_time",
        order_by: "source_id, instrument_key, source_sequence, event_time, report_hash",
        retention_days: 90,
        final_rows: true,
    },
    ArchiveTableSpec {
        table: "quant_entry_condition_evaluation_event",
        time_column: "evaluated_at",
        order_by: "condition_instance_id, evaluation_id",
        retention_days: 30,
        final_rows: true,
    },
    ArchiveTableSpec {
        table: "quant_weather_observation_report",
        time_column: "local_date",
        order_by: "station, local_date, observation_time, revision, report_hash",
        retention_days: 730,
        final_rows: true,
    },
    ArchiveTableSpec {
        table: "quant_weather_forecast_point",
        time_column: "reference_time",
        order_by: "station, reference_time, valid_time, member, run_manifest_hash",
        retention_days: 730,
        final_rows: true,
    },
    ArchiveTableSpec {
        table: "quant_domain_event",
        time_column: "event_time",
        order_by: "subject, event_type, event_time, revision, event_id",
        retention_days: 730,
        final_rows: true,
    },
    ArchiveTableSpec {
        table: "quant_domain_observation",
        time_column: "event_date",
        order_by: "instrument_key, metric, event_time",
        retention_days: 730,
        final_rows: false,
    },
];

/// Archives one eligible partition at a time and drops only a verified seal.
pub struct ArchivePartitionWorker {
    worker_id: Uuid,
    clickhouse: Arc<ClickHousePool>,
    manifests: Arc<dyn ArchivePartitionRepository>,
    artifacts: Arc<dyn ArtifactStore>,
}

impl ArchivePartitionWorker {
    #[must_use]
    pub fn new(
        clickhouse: Arc<ClickHousePool>,
        manifests: Arc<dyn ArchivePartitionRepository>,
        artifacts: Arc<dyn ArtifactStore>,
    ) -> Self {
        Self {
            worker_id: Uuid::now_v7(),
            clickhouse,
            manifests,
            artifacts,
        }
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) -> QuantResult<()> {
        let worker = Arc::clone(&self);
        PeriodicTask::run(
            "archive-partition-worker",
            || Duration::from_mins(1),
            0.0,
            false,
            shutdown,
            move || {
                let worker = Arc::clone(&worker);
                async move { worker.run_once(Utc::now()).await }
            },
        )
        .await
    }

    async fn run_once(&self, now: DateTime<Utc>) -> QuantResult<()> {
        let lease = chrono::Duration::from_std(LEASE_DURATION).map_err(|error| {
            QuantError::config(format!("invalid archive drop lease duration: {error}"))
        })?;
        if let Some(manifest) = self
            .manifests
            .claim_pending_drop(self.worker_id, now, now + lease)
            .await?
        {
            return self.process_claimed_drop(manifest, now).await;
        }
        for spec in TABLES {
            for partition in self.partitions(spec).await? {
                if !partition_is_eligible(&partition, spec.retention_days, now)?
                    || self
                        .manifests
                        .find_manifest(spec.table, &partition)
                        .await?
                        .is_some()
                {
                    continue;
                }
                self.export_and_seal(spec, partition, now).await?;
                return Ok(());
            }
        }
        Ok(())
    }

    async fn process_claimed_drop(
        &self,
        manifest: ArchivePartitionManifestInfo,
        now: DateTime<Utc>,
    ) -> QuantResult<()> {
        let result = self.verify_and_drop(&manifest, now).await;
        if let Err(error) = result {
            self.manifests
                .mark_drop_failed(manifest.manifest_id, self.worker_id, error.to_string())
                .await?;
            return Err(error);
        }
        self.manifests
            .complete_drop(manifest.manifest_id, self.worker_id, Utc::now())
            .await?;
        Ok(())
    }

    async fn verify_and_drop(
        &self,
        manifest: &ArchivePartitionManifestInfo,
        now: DateTime<Utc>,
    ) -> QuantResult<()> {
        let spec = TABLES
            .iter()
            .copied()
            .find(|spec| spec.table == manifest.table_name)
            .ok_or_else(|| {
                QuantError::config("archive manifest references an unsupported table")
            })?;
        if manifest.retention_days != spec.retention_days
            || !partition_is_eligible(&manifest.partition_key, spec.retention_days, now)?
        {
            return Err(QuantError::config(
                "archive manifest no longer satisfies the table retention contract",
            ));
        }
        let parquet = self.artifacts.get(&manifest.parquet_uri).await?;
        if bytes_hash(&parquet)? != manifest.byte_hash {
            return Err(QuantError::config(
                "archive Parquet read-back byte hash does not match sealed manifest",
            ));
        }
        if self.partition_exists(spec, &manifest.partition_key).await? {
            let (content_hash, row_count) = self
                .semantic_snapshot(spec, &manifest.partition_key)
                .await?;
            if content_hash != manifest.content_hash || row_count != manifest.row_count {
                return Err(QuantError::config(
                    "ClickHouse partition changed after archive seal; destructive drop blocked",
                ));
            }
            self.drop_partition(spec, &manifest.partition_key).await?;
        }
        Ok(())
    }

    async fn export_and_seal(
        &self,
        spec: ArchiveTableSpec,
        partition: String,
        sealed_at: DateTime<Utc>,
    ) -> QuantResult<()> {
        let before = self.semantic_snapshot(spec, &partition).await?;
        if before.1 == 0 {
            return Ok(());
        }
        let parquet = self.partition_bytes(spec, &partition, "Parquet").await?;
        let after = self.semantic_snapshot(spec, &partition).await?;
        if before != after {
            return Err(QuantError::config(
                "ClickHouse partition changed during archive export; seal blocked",
            ));
        }
        let byte_hash = bytes_hash(&parquet)?;
        let key = ArtifactKey::new(
            ArtifactNamespace::Archive,
            format!("{}-{}-{}", spec.table, partition, byte_hash.hex()),
            "parquet",
        )?;
        let parquet_uri = self.artifacts.put(key, &parquet).await?;
        let read_back = self.artifacts.get(&parquet_uri).await?;
        if read_back != parquet || bytes_hash(&read_back)? != byte_hash {
            return Err(QuantError::config(
                "archive Parquet read-back verification failed",
            ));
        }
        let manifest_hash = CanonicalDigest::content_hash_json(&(
            spec.table,
            &partition,
            spec.retention_days,
            before.1,
            &parquet_uri,
            &byte_hash,
            &before.0,
            sealed_at,
        ))?;
        self.manifests
            .seal_manifest(NewArchivePartitionManifest {
                manifest_id: Uuid::now_v7(),
                table_name: spec.table.to_owned(),
                partition_key: partition,
                retention_days: spec.retention_days,
                row_count: before.1,
                parquet_uri,
                byte_hash,
                content_hash: before.0,
                manifest_hash,
                sealed_at,
            })
            .await?;
        Ok(())
    }

    async fn partitions(&self, spec: ArchiveTableSpec) -> QuantResult<Vec<String>> {
        self.clickhouse
            .client()
            .query(
                "SELECT DISTINCT partition FROM system.parts \
                 WHERE active AND database = currentDatabase() AND table = ? \
                 ORDER BY partition",
            )
            .bind(spec.table)
            .fetch_all::<String>()
            .await
            .map_err(StorageError::from)
            .map_err(Into::into)
    }

    async fn partition_exists(&self, spec: ArchiveTableSpec, partition: &str) -> QuantResult<bool> {
        self.clickhouse
            .client()
            .query(
                "SELECT count() FROM system.parts \
                 WHERE active AND database = currentDatabase() \
                 AND table = ? AND partition = ?",
            )
            .bind(spec.table)
            .bind(partition)
            .fetch_one::<u64>()
            .await
            .map(|count| count > 0)
            .map_err(StorageError::from)
            .map_err(Into::into)
    }

    async fn semantic_snapshot(
        &self,
        spec: ArchiveTableSpec,
        partition: &str,
    ) -> QuantResult<(ContentHash, i64)> {
        let bytes = self.partition_bytes(spec, partition, "JSONEachRow").await?;
        let rows = bytes
            .split(|byte| *byte == b'\n')
            .filter(|row| !row.is_empty())
            .count();
        let row_count = i64::try_from(rows)
            .map_err(|error| QuantError::config(format!("archive row count overflow: {error}")))?;
        Ok((bytes_hash(&bytes)?, row_count))
    }

    async fn partition_bytes(
        &self,
        spec: ArchiveTableSpec,
        partition: &str,
        format: &str,
    ) -> QuantResult<Vec<u8>> {
        let partition = parse_partition(partition)?;
        let final_clause = if spec.final_rows { " FINAL" } else { "" };
        let sql = format!(
            "SELECT * FROM {}{} WHERE toYYYYMM({}) = ? ORDER BY {}",
            spec.table, final_clause, spec.time_column, spec.order_by
        );
        let mut cursor = self
            .clickhouse
            .client()
            .query(&sql)
            .bind(partition)
            .fetch_bytes(format)
            .map_err(StorageError::from)?;
        cursor
            .collect()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(StorageError::from)
            .map_err(Into::into)
    }

    async fn drop_partition(&self, spec: ArchiveTableSpec, partition: &str) -> QuantResult<()> {
        let partition = parse_partition(partition)?;
        let sql = format!("ALTER TABLE {} DROP PARTITION {partition}", spec.table);
        self.clickhouse
            .client()
            .query(&sql)
            .execute()
            .await
            .map_err(StorageError::from)
            .map_err(Into::into)
    }
}

fn bytes_hash(bytes: &[u8]) -> QuantResult<ContentHash> {
    ContentHash::parse(CanonicalDigest::prefixed_bytes(bytes)).map_err(Into::into)
}

fn parse_partition(partition: &str) -> QuantResult<u32> {
    if partition.len() != 6 || !partition.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(QuantError::config(format!(
            "unsupported ClickHouse monthly partition key: {partition}"
        )));
    }
    partition
        .parse::<u32>()
        .map_err(|error| QuantError::config(format!("invalid partition key: {error}")))
}

fn partition_is_eligible(
    partition: &str,
    retention_days: i32,
    now: DateTime<Utc>,
) -> QuantResult<bool> {
    let partition = parse_partition(partition)?;
    let year = i32::try_from(partition / 100)
        .map_err(|error| QuantError::config(format!("partition year overflow: {error}")))?;
    let month = partition % 100;
    let start = NaiveDate::from_ymd_opt(year, month, 1)
        .ok_or_else(|| QuantError::config("invalid monthly partition date"))?;
    let (next_year, next_month) = if start.month() == 12 {
        (start.year() + 1, 1)
    } else {
        (start.year(), start.month() + 1)
    };
    let end = NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|date| Utc.from_utc_datetime(&date))
        .ok_or_else(|| QuantError::config("partition end is outside chrono range"))?;
    Ok(end <= now - chrono::Duration::days(i64::from(retention_days)))
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{parse_partition, partition_is_eligible};

    #[test]
    fn monthly_partition_requires_complete_retention_window() {
        let now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
        assert!(partition_is_eligible("202601", 90, now).expect("eligible"));
        assert!(!partition_is_eligible("202605", 90, now).expect("not eligible"));
        assert!(parse_partition("2026-01").is_err());
    }
}
