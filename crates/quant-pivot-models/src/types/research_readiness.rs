//! Signed operational-readiness evidence payloads.

use chrono::{DateTime, Utc};
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::jsonb_active;

pub const RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION: u32 = 2;
pub const SHADOW_LATENCY_PROFILE_FORMAT_VERSION: u32 = 1;

/// Per-table observations read from active `ClickHouse` parts and table metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionTableObservationV2 {
    pub table: String,
    pub time_column: String,
    pub earliest_event_time: Option<DateTime<Utc>>,
    pub latest_event_time: Option<DateTime<Utc>>,
    pub row_count: u64,
    pub active_bytes: u64,
    pub active_partition_count: u64,
    pub partition_key: String,
    pub table_ttl_expression: Option<String>,
}

/// Measured raw-history coverage for `ClickHouse` Cloud.
///
/// Cloud storage capacity is intentionally absent: local disk free-space is not
/// a valid proxy for elastic shared object storage. Raw tables must also remain
/// free of destructive table TTL until governed retention is implemented.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionRunwayEvidenceV2 {
    pub format_version: u32,
    pub observed_at: DateTime<Utc>,
    pub required_days: u32,
    pub measured_history_days: Option<u32>,
    pub active_raw_bytes: u64,
    pub tables: Vec<RetentionTableObservationV2>,
}

impl RetentionRunwayEvidenceV2 {
    #[must_use]
    pub fn proven(&self) -> bool {
        self.format_version == RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION
            && !self.tables.is_empty()
            && self.tables.iter().all(|table| {
                table.earliest_event_time.is_some()
                    && table.latest_event_time.is_some()
                    && table.row_count > 0
                    && table.active_partition_count > 0
                    && table.partition_key.contains("toYYYYMM(")
                    && table.table_ttl_expression.is_none()
            })
            && self
                .measured_history_days
                .is_some_and(|days| days >= self.required_days)
    }
}

/// Complete `ReportOnly` latency profile. Venue submit/ack/match/chain timings are
/// intentionally excluded; they belong to the later real-canary profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowLatencyProfileV1 {
    pub format_version: u32,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub book_event_count: u64,
    pub book_age_p50_ms: u64,
    pub book_age_p95_ms: u64,
    pub book_age_p99_ms: u64,
    pub decision_prepared_count: u64,
    pub decision_prepared_p95_ms: Option<u64>,
    pub endpoint_rtt_count: u64,
    pub endpoint_rtt_p95_ms: Option<u64>,
    pub market_delay_count: u64,
    pub market_delay_p95_ms: Option<u64>,
}

impl ShadowLatencyProfileV1 {
    #[must_use]
    pub fn complete_for(&self, minimum_secs: u64) -> bool {
        let Ok(minimum_secs) = i64::try_from(minimum_secs) else {
            return false;
        };
        let observed_secs = self
            .window_end
            .signed_duration_since(self.window_start)
            .num_seconds();
        observed_secs >= minimum_secs
            && self.book_event_count > 0
            && self.decision_prepared_count > 0
            && self.endpoint_rtt_count > 0
            && self.market_delay_count > 0
            && self.decision_prepared_p95_ms.is_some()
            && self.endpoint_rtt_p95_ms.is_some()
            && self.market_delay_p95_ms.is_some()
    }
}

/// Typed payload stored in the append-only readiness index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(tag = "kind", content = "evidence", rename_all = "snake_case")]
pub enum ResearchReadinessEvidencePayload {
    RetentionRunway(RetentionRunwayEvidenceV2),
    ShadowLatencyProfile(ShadowLatencyProfileV1),
}

jsonb_active!(ResearchReadinessEvidencePayload);

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};

    use super::{
        RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION, RetentionRunwayEvidenceV2,
        RetentionTableObservationV2, SHADOW_LATENCY_PROFILE_FORMAT_VERSION, ShadowLatencyProfileV1,
    };

    fn observed_at() -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(1_720_000_000, 0)
            .single()
            .expect("valid timestamp")
    }

    fn retention() -> RetentionRunwayEvidenceV2 {
        let observed_at = observed_at();
        RetentionRunwayEvidenceV2 {
            format_version: RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION,
            observed_at,
            required_days: 200,
            measured_history_days: Some(220),
            active_raw_bytes: 5_000,
            tables: vec![RetentionTableObservationV2 {
                table: "quant_book_l2_event".to_owned(),
                time_column: "persisted_time".to_owned(),
                earliest_event_time: Some(observed_at - Duration::days(220)),
                latest_event_time: Some(observed_at),
                row_count: 100,
                active_bytes: 5_000,
                active_partition_count: 8,
                partition_key: "toYYYYMM(persisted_time)".to_owned(),
                table_ttl_expression: None,
            }],
        }
    }

    #[test]
    fn retention_requires_measured_history_monthly_parts_and_no_table_ttl() {
        let mut evidence = retention();
        assert!(evidence.proven());

        evidence.tables[0].partition_key = "toDate(persisted_time)".to_owned();
        assert!(!evidence.proven());

        evidence = retention();
        evidence.measured_history_days = Some(199);
        assert!(!evidence.proven());

        evidence = retention();
        evidence.tables[0].table_ttl_expression =
            Some("persisted_time + toIntervalDay(200) DELETE".to_owned());
        assert!(!evidence.proven());
    }

    #[test]
    fn shadow_profile_requires_all_dimensions_over_the_full_window() {
        let observed_at = observed_at();
        let mut evidence = ShadowLatencyProfileV1 {
            format_version: SHADOW_LATENCY_PROFILE_FORMAT_VERSION,
            window_start: observed_at - Duration::hours(24),
            window_end: observed_at,
            observed_at,
            book_event_count: 1,
            book_age_p50_ms: 10,
            book_age_p95_ms: 20,
            book_age_p99_ms: 30,
            decision_prepared_count: 1,
            decision_prepared_p95_ms: Some(40),
            endpoint_rtt_count: 1,
            endpoint_rtt_p95_ms: Some(50),
            market_delay_count: 1,
            market_delay_p95_ms: Some(60),
        };
        assert!(evidence.complete_for(24 * 60 * 60));

        evidence.endpoint_rtt_count = 0;
        evidence.endpoint_rtt_p95_ms = None;
        assert!(!evidence.complete_for(24 * 60 * 60));

        evidence.endpoint_rtt_count = 1;
        evidence.endpoint_rtt_p95_ms = Some(50);
        evidence.window_start += Duration::seconds(1);
        assert!(!evidence.complete_for(24 * 60 * 60));
    }
}
