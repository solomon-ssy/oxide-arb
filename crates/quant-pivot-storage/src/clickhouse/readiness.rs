//! Read-only `ClickHouse` observations used by operational-readiness evidence.

use chrono::{DateTime, Utc};
use clickhouse::Row;
use quant_pivot_error::storage::StorageError;
use serde::Deserialize;

use crate::clickhouse::{ClickHousePool, RawHistoryTable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawHistoryObservation {
    pub earliest_ms: Option<i64>,
    pub latest_ms: Option<i64>,
    pub row_count: u64,
    pub active_bytes: u64,
    pub active_partition_count: u64,
    pub partition_key: String,
    pub create_table_query: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookLatencyObservation {
    pub event_count: u64,
    pub age_p50_ms: u64,
    pub age_p95_ms: u64,
    pub age_p99_ms: u64,
}

impl ClickHousePool {
    pub async fn observe_raw_history_table(
        &self,
        spec: RawHistoryTable,
        as_of: DateTime<Utc>,
    ) -> Result<RawHistoryObservation, StorageError> {
        let range_sql = format!(
            "SELECT toUnixTimestamp64Milli(minOrNull({time})) AS earliest_ms, \
             toUnixTimestamp64Milli(maxOrNull({time})) AS latest_ms, count() AS row_count \
             FROM {table} WHERE {time} <= fromUnixTimestamp64Milli(?)",
            time = spec.time_column,
            table = spec.table,
        );
        let range = self
            .client()
            .query(&range_sql)
            .bind(as_of.timestamp_millis())
            .fetch_one::<TimeRangeRow>()
            .await?;
        let parts = self
            .client()
            .query(
                "SELECT sum(bytes_on_disk) AS active_bytes, \
                 uniqExact(partition) AS active_partition_count FROM system.parts \
                 WHERE active AND database = currentDatabase() AND table = ?",
            )
            .bind(spec.table)
            .fetch_one::<PartStatsRow>()
            .await?;
        let metadata = self
            .client()
            .query(
                "SELECT partition_key, create_table_query FROM system.tables \
                 WHERE database = currentDatabase() AND name = ?",
            )
            .bind(spec.table)
            .fetch_one::<TableMetadataRow>()
            .await?;
        Ok(RawHistoryObservation {
            earliest_ms: range.earliest_ms,
            latest_ms: range.latest_ms,
            row_count: range.row_count,
            active_bytes: parts.active_bytes,
            active_partition_count: parts.active_partition_count,
            partition_key: metadata.partition_key,
            create_table_query: metadata.create_table_query,
        })
    }

    pub async fn observe_book_latency(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Result<BookLatencyObservation, StorageError> {
        let row = self
            .client()
            .query(
                "SELECT count() AS event_count, \
                 toUInt64(quantileExact(0.50)(greatest(0, dateDiff('millisecond', venue_event_time, ingress_time)))) AS age_p50_ms, \
                 toUInt64(quantileExact(0.95)(greatest(0, dateDiff('millisecond', venue_event_time, ingress_time)))) AS age_p95_ms, \
                 toUInt64(quantileExact(0.99)(greatest(0, dateDiff('millisecond', venue_event_time, ingress_time)))) AS age_p99_ms \
                 FROM quant_book_l2_event WHERE persisted_time >= fromUnixTimestamp64Milli(?) \
                 AND persisted_time < fromUnixTimestamp64Milli(?)",
            )
            .bind(window_start.timestamp_millis())
            .bind(window_end.timestamp_millis())
            .fetch_one::<BookLatencyRow>()
            .await?;
        Ok(BookLatencyObservation {
            event_count: row.event_count,
            age_p50_ms: row.age_p50_ms,
            age_p95_ms: row.age_p95_ms,
            age_p99_ms: row.age_p99_ms,
        })
    }
}

#[derive(Row, Deserialize)]
struct TimeRangeRow {
    earliest_ms: Option<i64>,
    latest_ms: Option<i64>,
    row_count: u64,
}

#[derive(Row, Deserialize)]
struct PartStatsRow {
    active_bytes: u64,
    active_partition_count: u64,
}

#[derive(Row, Deserialize)]
struct TableMetadataRow {
    partition_key: String,
    create_table_query: String,
}

#[derive(Row, Deserialize)]
struct BookLatencyRow {
    event_count: u64,
    age_p50_ms: u64,
    age_p95_ms: u64,
    age_p99_ms: u64,
}
