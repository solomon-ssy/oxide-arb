//! Seal-first `ClickHouse` partition retention with content-addressed Parquet archives.

use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use futures_util::StreamExt;
use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
use quant_pivot_models::{
    domain::{ArchivePartitionManifestInfo, NewArchivePartitionManifest},
    hashing::CanonicalDigest,
    types::ContentHash,
};
use quant_pivot_repository::traits::ArchivePartitionRepository;
use quant_pivot_research::artifact::{
    ArtifactByteStream, ArtifactKey, ArtifactNamespace, ArtifactStore,
};
use quant_pivot_storage::clickhouse::ClickHousePool;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::infra::periodic_task::PeriodicTask;

const LEASE_DURATION: Duration = Duration::from_mins(5);

#[derive(Clone, Copy)]
struct ArchiveTableSpec {
    table: &'static str,
    time_column: &'static str,
    order_by: &'static str,
    granularity: PartitionGranularity,
    retention_days: i32,
    final_rows: bool,
}

#[derive(Clone, Copy)]
enum PartitionGranularity {
    Daily,
    Monthly,
}

struct StreamDigest {
    byte_hash: ContentHash,
    byte_count: u64,
}

const TABLES: [ArchiveTableSpec; 10] = [
    ArchiveTableSpec {
        table: "quant_book_l2_event",
        time_column: "event_date",
        order_by: "token_id, stream_session_id, token_sequence",
        granularity: PartitionGranularity::Daily,
        retention_days: 3,
        final_rows: false,
    },
    ArchiveTableSpec {
        table: "quant_book_l2_checkpoint",
        time_column: "checkpoint_date",
        order_by: "token_id, event_time, stream_session_id, token_sequence",
        granularity: PartitionGranularity::Daily,
        retention_days: 3,
        final_rows: false,
    },
    ArchiveTableSpec {
        table: "quant_book_stream_session",
        time_column: "session_date",
        order_by: "stream_session_id, ledger_sequence",
        granularity: PartitionGranularity::Daily,
        retention_days: 3,
        final_rows: false,
    },
    ArchiveTableSpec {
        table: "quant_trade_tape",
        time_column: "event_date",
        order_by: "market_id, token_id, event_time, source_event_id, participant_address",
        granularity: PartitionGranularity::Daily,
        retention_days: 3,
        final_rows: false,
    },
    ArchiveTableSpec {
        table: "quant_crypto_price_report",
        time_column: "event_time",
        order_by: "source_id, instrument_key, source_sequence, event_time, report_hash",
        granularity: PartitionGranularity::Monthly,
        retention_days: 90,
        final_rows: true,
    },
    ArchiveTableSpec {
        table: "quant_entry_condition_evaluation_event",
        time_column: "evaluated_at",
        order_by: "condition_instance_id, evaluation_id",
        granularity: PartitionGranularity::Monthly,
        retention_days: 30,
        final_rows: true,
    },
    ArchiveTableSpec {
        table: "quant_weather_observation_report",
        time_column: "local_date",
        order_by: "station, local_date, observation_time, revision, report_hash",
        granularity: PartitionGranularity::Monthly,
        retention_days: 730,
        final_rows: true,
    },
    ArchiveTableSpec {
        table: "quant_weather_forecast_point",
        time_column: "reference_time",
        order_by: "station, reference_time, valid_time, member, run_manifest_hash",
        granularity: PartitionGranularity::Monthly,
        retention_days: 730,
        final_rows: true,
    },
    ArchiveTableSpec {
        table: "quant_domain_event",
        time_column: "event_time",
        order_by: "subject, event_type, event_time, revision, event_id",
        granularity: PartitionGranularity::Monthly,
        retention_days: 730,
        final_rows: true,
    },
    ArchiveTableSpec {
        table: "quant_domain_observation",
        time_column: "event_date",
        order_by: "instrument_key, metric, event_time",
        granularity: PartitionGranularity::Monthly,
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
                if !partition_is_eligible(&partition, spec, now)?
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
            || !partition_is_eligible(&manifest.partition_key, spec, now)?
        {
            return Err(QuantError::config(
                "archive manifest no longer satisfies the table retention contract",
            ));
        }
        let (partition_start_at, partition_end_at) =
            partition_bounds(&manifest.partition_key, spec.granularity)?;
        if manifest.partition_start_at != partition_start_at
            || manifest.partition_end_at != partition_end_at
            || manifest.source_schema_hash != self.source_schema_hash(spec).await?
        {
            return Err(QuantError::config(
                "archive manifest partition range or source schema identity is invalid",
            ));
        }
        let digest = stream_digest(self.artifacts.get_stream(&manifest.parquet_uri).await?).await?;
        let metadata = self.artifacts.metadata(&manifest.parquet_uri).await?;
        let byte_count = i64::try_from(metadata.byte_size).map_err(|error| {
            QuantError::config(format!("archive object byte count overflow: {error}"))
        })?;
        let hash_matches = digest.byte_hash == manifest.byte_hash;
        let streamed_size_matches = digest.byte_count == metadata.byte_size;
        let manifest_size_matches = byte_count == manifest.parquet_byte_count;
        let etag_matches = metadata.etag == manifest.object_etag;
        let version_matches = metadata.version_id == manifest.object_version_id;
        if !(hash_matches
            && streamed_size_matches
            && manifest_size_matches
            && etag_matches
            && version_matches)
        {
            return Err(QuantError::config(
                "archive Parquet object identity does not match sealed manifest",
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
        let parquet_digest =
            stream_digest(self.partition_stream(spec, &partition, "Parquet")?).await?;
        let after_digest = self.semantic_snapshot(spec, &partition).await?;
        if before != after_digest {
            return Err(QuantError::config(
                "ClickHouse partition changed during archive export; seal blocked",
            ));
        }
        let byte_hash = parquet_digest.byte_hash;
        let key = ArtifactKey::new(
            ArtifactNamespace::Archive,
            format!("{}-{}-{}", spec.table, partition, byte_hash.hex()),
            "parquet",
        )?;
        let parquet_uri = self
            .artifacts
            .put_stream(key, self.partition_stream(spec, &partition, "Parquet")?)
            .await?;
        let read_back = stream_digest(self.artifacts.get_stream(&parquet_uri).await?).await?;
        if read_back.byte_hash != byte_hash || read_back.byte_count != parquet_digest.byte_count {
            return Err(QuantError::config(
                "archive Parquet read-back verification failed",
            ));
        }
        let metadata = self.artifacts.metadata(&parquet_uri).await?;
        let parquet_byte_count = i64::try_from(metadata.byte_size).map_err(|error| {
            QuantError::config(format!("archive object byte count overflow: {error}"))
        })?;
        if parquet_byte_count
            != i64::try_from(parquet_digest.byte_count).map_err(|error| {
                QuantError::config(format!("archive Parquet byte count overflow: {error}"))
            })?
        {
            return Err(QuantError::config(
                "archive object metadata byte count does not match exported Parquet",
            ));
        }
        let after_upload = self.semantic_snapshot(spec, &partition).await?;
        if before != after_upload {
            return Err(QuantError::config(
                "ClickHouse partition changed before archive seal; hot partition retained",
            ));
        }
        let (partition_start_at, partition_end_at) =
            partition_bounds(&partition, spec.granularity)?;
        let source_schema_hash = self.source_schema_hash(spec).await?;
        let manifest_hash = CanonicalDigest::content_hash_json(&(
            spec.table,
            &partition,
            partition_start_at,
            partition_end_at,
            spec.retention_days,
            before.1,
            &parquet_uri,
            parquet_byte_count,
            &metadata.etag,
            &metadata.version_id,
            &byte_hash,
            &before.0,
            &source_schema_hash,
            sealed_at,
        ))?;
        self.manifests
            .seal_manifest(NewArchivePartitionManifest {
                manifest_id: Uuid::now_v7(),
                table_name: spec.table.to_owned(),
                partition_key: partition,
                partition_start_at,
                partition_end_at,
                retention_days: spec.retention_days,
                row_count: before.1,
                parquet_uri,
                parquet_byte_count,
                object_etag: metadata.etag,
                object_version_id: metadata.version_id,
                byte_hash,
                content_hash: before.0,
                source_schema_hash,
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

    async fn source_schema_hash(&self, spec: ArchiveTableSpec) -> QuantResult<ContentHash> {
        let sql = format!("SHOW CREATE TABLE {}", spec.table);
        let schema = self
            .clickhouse
            .client()
            .query(&sql)
            .fetch_one::<String>()
            .await
            .map_err(StorageError::from)?;
        bytes_hash(schema.as_bytes())
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
        let mut stream = self.partition_stream(spec, partition, "JSONEachRow")?;
        let mut hasher = blake3::Hasher::new();
        let mut byte_count = 0_u64;
        let mut row_count = 0_u64;
        let mut has_unterminated_row = false;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            byte_count = byte_count
                .checked_add(u64::try_from(chunk.len()).map_err(|error| {
                    QuantError::config(format!("archive byte count overflow: {error}"))
                })?)
                .ok_or_else(|| QuantError::config("archive byte count overflow"))?;
            row_count =
                row_count
                    .checked_add(u64::try_from(bytecount::count(&chunk, b'\n')).map_err(
                        |error| QuantError::config(format!("archive row count overflow: {error}")),
                    )?)
                    .ok_or_else(|| QuantError::config("archive row count overflow"))?;
            if let Some(last) = chunk.last() {
                has_unterminated_row = *last != b'\n';
            }
            hasher.update(&chunk);
        }
        if byte_count > 0 && has_unterminated_row {
            row_count = row_count
                .checked_add(1)
                .ok_or_else(|| QuantError::config("archive row count overflow"))?;
        }
        let row_count = i64::try_from(row_count)
            .map_err(|error| QuantError::config(format!("archive row count overflow: {error}")))?;
        Ok((content_hash_from_hasher(&hasher)?, row_count))
    }

    fn partition_stream(
        &self,
        spec: ArchiveTableSpec,
        partition: &str,
        format: &str,
    ) -> QuantResult<ArtifactByteStream> {
        let partition = parse_partition(partition, spec.granularity)?;
        let final_clause = if spec.final_rows { " FINAL" } else { "" };
        let partition_function = match spec.granularity {
            PartitionGranularity::Daily => "toYYYYMMDD",
            PartitionGranularity::Monthly => "toYYYYMM",
        };
        let sql = format!(
            "SELECT * FROM {}{} WHERE {}({}) = ? ORDER BY {}",
            spec.table, final_clause, partition_function, spec.time_column, spec.order_by
        );
        let cursor = self
            .clickhouse
            .client()
            .query(&sql)
            .bind(partition)
            .fetch_bytes(format)
            .map_err(StorageError::from)?;
        let stream = ReaderStream::new(cursor).map(|result| {
            result.map_err(|error| {
                StorageError::Connection(format!("ClickHouse archive stream failed: {error}"))
                    .into()
            })
        });
        Ok(Box::pin(stream))
    }

    async fn drop_partition(&self, spec: ArchiveTableSpec, partition: &str) -> QuantResult<()> {
        let partition = parse_partition(partition, spec.granularity)?;
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

async fn stream_digest(mut stream: ArtifactByteStream) -> QuantResult<StreamDigest> {
    let mut hasher = blake3::Hasher::new();
    let mut byte_count = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        byte_count = byte_count
            .checked_add(u64::try_from(chunk.len()).map_err(|error| {
                QuantError::config(format!("artifact byte count overflow: {error}"))
            })?)
            .ok_or_else(|| QuantError::config("artifact byte count overflow"))?;
        hasher.update(&chunk);
    }
    Ok(StreamDigest {
        byte_hash: content_hash_from_hasher(&hasher)?,
        byte_count,
    })
}

fn content_hash_from_hasher(hasher: &blake3::Hasher) -> QuantResult<ContentHash> {
    ContentHash::parse(format!("blake3:{}", hasher.finalize().to_hex())).map_err(Into::into)
}

fn parse_partition(partition: &str, granularity: PartitionGranularity) -> QuantResult<u32> {
    let expected_len = match granularity {
        PartitionGranularity::Daily => 8,
        PartitionGranularity::Monthly => 6,
    };
    if partition.len() != expected_len || !partition.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(QuantError::config(format!(
            "unsupported ClickHouse partition key: {partition}"
        )));
    }
    partition
        .parse::<u32>()
        .map_err(|error| QuantError::config(format!("invalid partition key: {error}")))
}

fn partition_is_eligible(
    partition: &str,
    spec: ArchiveTableSpec,
    now: DateTime<Utc>,
) -> QuantResult<bool> {
    let (_, end) = partition_bounds(partition, spec.granularity)?;
    Ok(end <= now - chrono::Duration::days(i64::from(spec.retention_days)))
}

fn partition_bounds(
    partition: &str,
    granularity: PartitionGranularity,
) -> QuantResult<(DateTime<Utc>, DateTime<Utc>)> {
    let partition = parse_partition(partition, granularity)?;
    let (start_date, end_date) = match granularity {
        PartitionGranularity::Daily => {
            let year = i32::try_from(partition / 10_000)
                .map_err(|error| QuantError::config(format!("partition year overflow: {error}")))?;
            let month = partition / 100 % 100;
            let day = partition % 100;
            let start = NaiveDate::from_ymd_opt(year, month, day)
                .ok_or_else(|| QuantError::config("invalid daily partition date"))?;
            let end = start
                .succ_opt()
                .ok_or_else(|| QuantError::config("invalid daily partition end"))?;
            (start, end)
        }
        PartitionGranularity::Monthly => {
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
                .ok_or_else(|| QuantError::config("invalid monthly partition end"))?;
            (start, end)
        }
    };
    let to_utc = |date: NaiveDate| {
        date.and_hms_opt(0, 0, 0)
            .map(|value| Utc.from_utc_datetime(&value))
            .ok_or_else(|| QuantError::config("partition boundary is outside chrono range"))
    };
    Ok((to_utc(start_date)?, to_utc(end_date)?))
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use futures_util::stream;
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::hashing::CanonicalDigest;
    use quant_pivot_research::artifact::ArtifactByteStream;

    use super::{
        ArchiveTableSpec, PartitionGranularity, parse_partition, partition_is_eligible,
        stream_digest,
    };

    fn spec(granularity: PartitionGranularity, retention_days: i32) -> ArchiveTableSpec {
        ArchiveTableSpec {
            table: "test",
            time_column: "event_time",
            order_by: "event_time",
            granularity,
            retention_days,
            final_rows: false,
        }
    }

    #[test]
    fn monthly_partition_requires_complete_retention_window() {
        let now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
        let monthly = spec(PartitionGranularity::Monthly, 90);
        assert!(partition_is_eligible("202601", monthly, now).expect("eligible"));
        assert!(!partition_is_eligible("202605", monthly, now).expect("not eligible"));
        assert!(parse_partition("2026-01", monthly.granularity).is_err());
    }

    #[test]
    fn daily_partition_requires_complete_hot_retention_window() {
        let now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
        let daily = spec(PartitionGranularity::Daily, 3);
        assert!(partition_is_eligible("20260710", daily, now).expect("eligible"));
        assert!(!partition_is_eligible("20260711", daily, now).expect("not eligible"));
        assert!(parse_partition("202607", daily.granularity).is_err());
    }

    #[tokio::test]
    async fn stream_digest_is_chunk_boundary_independent() {
        let chunks: [QuantResult<_>; 3] = [
            Ok(Vec::from(&b"par"[..]).into()),
            Ok(Vec::new().into()),
            Ok(Vec::from(&b"quet"[..]).into()),
        ];
        let stream: ArtifactByteStream = Box::pin(stream::iter(chunks));
        let digest = stream_digest(stream).await.expect("digest");
        assert_eq!(digest.byte_count, 7);
        assert_eq!(
            digest.byte_hash.as_str(),
            CanonicalDigest::prefixed_bytes(b"parquet")
        );
    }
}
