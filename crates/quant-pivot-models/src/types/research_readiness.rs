//! Signed operational-readiness evidence payloads.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    hashing::CanonicalDigest,
    types::{ContentHash, ResearchProfileDataSource, builtin_research_profiles},
    wire_enum,
};

pub const RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION: u32 = 1;
pub const SHADOW_LATENCY_PROFILE_FORMAT_VERSION: u32 = 1;

wire_enum! {
    /// Stable logical source identifiers covered by the readiness registry.
    @derive(PartialOrd, Ord)
    pub enum ResearchReadinessSource {
        CatalogLedger => "catalog_ledger",
        ClobMarketInfo => "clob_market_info",
        ClobL2 => "clob_l2",
        TradeTape => "trade_tape",
        BinanceMarketData => "binance_market_data",
        PolymarketRtds => "polymarket_rtds",
        DomainObservation => "domain_observation",
        AviationWeather => "aviation_weather",
        GhcnhCalibration => "ghcnh_calibration",
        GefsEnsemble => "gefs_ensemble",
        PolymarketResolution => "polymarket_resolution",
    }
}

wire_enum! {
    /// Durable storage semantics of one research-source binding.
    @derive(PartialOrd, Ord)
    pub enum ResearchSourceStorageKind {
        PostgresLedger => "postgres_ledger",
        PostgresVersionedProjection => "postgres_versioned_projection",
        ClickHouseTable => "clickhouse_table",
    }
}

/// Generic `PostgreSQL` coverage returned by typed repository probes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryCoverage {
    pub object: String,
    pub time_column: String,
    pub earliest_event_time: Option<DateTime<Utc>>,
    pub latest_event_time: Option<DateTime<Utc>>,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResearchSourceFilter {
    pub column: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResearchSourceBinding {
    pub source: ResearchReadinessSource,
    pub storage: ResearchSourceStorageKind,
    pub object: String,
    pub time_column: String,
    pub partition_key: Option<String>,
    pub filter: Option<ResearchSourceFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResearchSourceRegistry {
    pub required_sources: Vec<ResearchReadinessSource>,
    pub bindings: Vec<ResearchSourceBinding>,
}

impl ResearchSourceRegistry {
    pub fn contract_hash(&self) -> Result<ContentHash, String> {
        CanonicalDigest::content_hash_json(&("quant-pivot/research-source-registry", 1_u32, self))
            .map_err(|error| format!("research source registry hash failed: {error}"))
    }
}

/// Resolve the one canonical source-to-storage contract from built-in profiles.
pub fn research_source_registry() -> Result<ResearchSourceRegistry, String> {
    let profile_sources = builtin_research_profiles()?
        .into_iter()
        .flat_map(|profile| profile.spec.required_sources)
        .collect::<BTreeSet<_>>();
    let mut required_sources = profile_sources
        .iter()
        .copied()
        .map(ResearchReadinessSource::from)
        .collect::<BTreeSet<_>>();
    if profile_sources.iter().any(|source| {
        matches!(
            source,
            ResearchProfileDataSource::AviationWeather
                | ResearchProfileDataSource::GhcnhCalibration
                | ResearchProfileDataSource::GefsEnsemble
        )
    }) {
        required_sources.insert(ResearchReadinessSource::DomainObservation);
    }

    let bindings = source_bindings()
        .into_iter()
        .filter(|binding| required_sources.contains(&binding.source))
        .collect::<Vec<_>>();
    for source in &required_sources {
        if !bindings.iter().any(|binding| binding.source == *source) {
            return Err(format!(
                "required research source `{source}` has no durable storage binding"
            ));
        }
    }
    Ok(ResearchSourceRegistry {
        required_sources: required_sources.into_iter().collect(),
        bindings,
    })
}

impl From<ResearchProfileDataSource> for ResearchReadinessSource {
    fn from(value: ResearchProfileDataSource) -> Self {
        match value {
            ResearchProfileDataSource::CatalogLedger => Self::CatalogLedger,
            ResearchProfileDataSource::ClobMarketInfo => Self::ClobMarketInfo,
            ResearchProfileDataSource::ClobL2 => Self::ClobL2,
            ResearchProfileDataSource::TradeTape => Self::TradeTape,
            ResearchProfileDataSource::BinanceMarketData => Self::BinanceMarketData,
            ResearchProfileDataSource::PolymarketRtds => Self::PolymarketRtds,
            ResearchProfileDataSource::AviationWeather => Self::AviationWeather,
            ResearchProfileDataSource::GhcnhCalibration => Self::GhcnhCalibration,
            ResearchProfileDataSource::GefsEnsemble => Self::GefsEnsemble,
            ResearchProfileDataSource::PolymarketResolution => Self::PolymarketResolution,
        }
    }
}

fn source_bindings() -> Vec<ResearchSourceBinding> {
    vec![
        pg_binding(
            Source::CatalogLedger,
            Storage::PostgresLedger,
            "catalog_event_change",
            "source_effective_at",
        ),
        pg_binding(
            Source::CatalogLedger,
            Storage::PostgresLedger,
            "catalog_market_change",
            "source_effective_at",
        ),
        pg_binding(
            Source::ClobMarketInfo,
            Storage::PostgresVersionedProjection,
            "clob_market_info_version",
            "effective_at",
        ),
        ch_binding(
            Source::ClobL2,
            "quant_book_stream_session",
            "opened_at",
            "toYYYYMM(session_date)",
            None,
        ),
        ch_binding(
            Source::ClobL2,
            "quant_book_l2_ledger",
            "venue_event_time",
            "toYYYYMM(event_date)",
            None,
        ),
        ch_binding(
            Source::ClobL2,
            "book_microstructure_1s",
            "bucket_time",
            "toYYYYMM(bucket_date)",
            None,
        ),
        ch_binding(
            Source::TradeTape,
            "quant_trade_tape",
            "event_time",
            "toYYYYMM(event_date)",
            None,
        ),
        ch_binding(
            Source::BinanceMarketData,
            "quant_crypto_price_report",
            "event_time",
            "toYYYYMM(event_time)",
            Some(("source_id", "binance")),
        ),
        ch_binding(
            Source::PolymarketRtds,
            "quant_crypto_price_report",
            "event_time",
            "toYYYYMM(event_time)",
            Some(("source_id", "polymarket_rtds_binance")),
        ),
        ch_binding(
            Source::DomainObservation,
            "quant_domain_observation",
            "event_time",
            "toYYYYMM(event_date)",
            None,
        ),
        ch_binding(
            Source::AviationWeather,
            "quant_weather_observation_fact",
            "observed_at",
            "intDiv(local_date, 3660)",
            Some(("source_id", "aviation_weather")),
        ),
        ch_binding(
            Source::GhcnhCalibration,
            "quant_weather_observation_fact",
            "observed_at",
            "intDiv(local_date, 3660)",
            Some(("source_id", "ghcnh")),
        ),
        ch_binding(
            Source::GefsEnsemble,
            "quant_weather_forecast_fact",
            "valid_time",
            "toYYYYMM(reference_time)",
            Some(("source_id", "gefs")),
        ),
        ch_binding(
            Source::PolymarketResolution,
            "market_resolution_event",
            "resolved_at",
            "toYYYYMM(resolved_date)",
            None,
        ),
    ]
}

type Source = ResearchReadinessSource;
type Storage = ResearchSourceStorageKind;

fn pg_binding(
    source: ResearchReadinessSource,
    storage: ResearchSourceStorageKind,
    object: &str,
    time_column: &str,
) -> ResearchSourceBinding {
    ResearchSourceBinding {
        source,
        storage,
        object: object.to_owned(),
        time_column: time_column.to_owned(),
        partition_key: None,
        filter: None,
    }
}

fn ch_binding(
    source: ResearchReadinessSource,
    object: &str,
    time_column: &str,
    partition_key: &str,
    filter: Option<(&str, &str)>,
) -> ResearchSourceBinding {
    ResearchSourceBinding {
        source,
        storage: ResearchSourceStorageKind::ClickHouseTable,
        object: object.to_owned(),
        time_column: time_column.to_owned(),
        partition_key: Some(partition_key.to_owned()),
        filter: filter.map(|(column, value)| ResearchSourceFilter {
            column: column.to_owned(),
            value: value.to_owned(),
        }),
    }
}

/// Per-object observation from the cross-storage research source registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionSourceObservationV1 {
    pub source: ResearchReadinessSource,
    pub storage: ResearchSourceStorageKind,
    pub object: String,
    pub time_column: String,
    pub earliest_event_time: Option<DateTime<Utc>>,
    pub latest_event_time: Option<DateTime<Utc>>,
    pub row_count: u64,
    pub active_bytes: Option<u64>,
    pub active_partition_count: Option<u64>,
    pub partition_key: Option<String>,
    pub table_ttl_expression: Option<String>,
}

/// Measured history coverage for the complete cross-storage source contract.
///
/// Cloud storage capacity is intentionally absent: local disk free-space is not
/// a valid proxy for elastic shared object storage. Raw tables must also remain
/// free of destructive table TTL until governed retention is implemented.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionRunwayEvidenceV1 {
    pub format_version: u32,
    pub registry_hash: ContentHash,
    pub required_sources: Vec<ResearchReadinessSource>,
    pub observed_at: DateTime<Utc>,
    pub required_days: u32,
    pub measured_history_days: Option<u32>,
    pub active_raw_bytes: u64,
    pub observations: Vec<RetentionSourceObservationV1>,
}

impl RetentionRunwayEvidenceV1 {
    #[must_use]
    pub fn proven(&self) -> bool {
        self.format_version == RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION
            && !self.required_sources.is_empty()
            && self
                .required_sources
                .windows(2)
                .all(|sources| sources[0] < sources[1])
            && !self.observations.is_empty()
            && self.required_sources.iter().all(|required| {
                self.observations
                    .iter()
                    .any(|observation| observation.source == *required)
            })
            && self.observations.iter().all(|observation| {
                observation.earliest_event_time.is_some()
                    && observation.latest_event_time.is_some()
                    && observation.row_count > 0
                    && observation.table_ttl_expression.is_none()
                    && match observation.storage {
                        ResearchSourceStorageKind::ClickHouseTable => {
                            observation.active_bytes.is_some()
                                && observation
                                    .active_partition_count
                                    .is_some_and(|count| count > 0)
                                && observation
                                    .partition_key
                                    .as_deref()
                                    .is_some_and(|key| key.contains("toYYYYMM("))
                        }
                        ResearchSourceStorageKind::PostgresLedger
                        | ResearchSourceStorageKind::PostgresVersionedProjection => {
                            observation.active_bytes.is_none()
                                && observation.active_partition_count.is_none()
                                && observation.partition_key.is_none()
                        }
                    }
            })
            && self
                .measured_history_days
                .is_some_and(|days| days >= self.required_days)
    }

    pub fn matches_registry(&self, registry: &ResearchSourceRegistry) -> bool {
        registry
            .contract_hash()
            .is_ok_and(|hash| hash == self.registry_hash)
            && self.required_sources == registry.required_sources
            && self.observations.len() == registry.bindings.len()
            && registry.bindings.iter().all(|binding| {
                self.observations
                    .iter()
                    .filter(|observation| {
                        observation.source == binding.source
                            && observation.storage == binding.storage
                            && observation.object == binding.object
                            && observation.time_column == binding.time_column
                            && observation.partition_key == binding.partition_key
                    })
                    .count()
                    == 1
            })
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
    RetentionRunway(RetentionRunwayEvidenceV1),
    ShadowLatencyProfile(ShadowLatencyProfileV1),
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};

    use super::{
        ContentHash, RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION, ResearchReadinessSource,
        ResearchSourceBinding, ResearchSourceRegistry, ResearchSourceStorageKind,
        RetentionRunwayEvidenceV1, RetentionSourceObservationV1,
        SHADOW_LATENCY_PROFILE_FORMAT_VERSION, ShadowLatencyProfileV1,
    };

    fn observed_at() -> DateTime<Utc> {
        Utc.timestamp_opt(1_720_000_000, 0)
            .single()
            .expect("valid timestamp")
    }

    impl RetentionRunwayEvidenceV1 {
        fn test_fixture() -> Self {
            let observed_at = observed_at();
            Self {
                format_version: RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION,
                registry_hash: hash(1),
                required_sources: vec![ResearchReadinessSource::ClobL2],
                observed_at,
                required_days: 200,
                measured_history_days: Some(220),
                active_raw_bytes: 5_000,
                observations: vec![RetentionSourceObservationV1 {
                    source: ResearchReadinessSource::ClobL2,
                    storage: ResearchSourceStorageKind::ClickHouseTable,
                    object: "quant_book_l2_ledger".to_owned(),
                    time_column: "persisted_time".to_owned(),
                    earliest_event_time: Some(observed_at - Duration::days(220)),
                    latest_event_time: Some(observed_at),
                    row_count: 100,
                    active_bytes: Some(5_000),
                    active_partition_count: Some(8),
                    partition_key: Some("toYYYYMM(persisted_time)".to_owned()),
                    table_ttl_expression: None,
                }],
            }
        }
    }

    fn hash(index: u8) -> ContentHash {
        ContentHash::parse(&format!("blake3:{index:064x}")).expect("hash")
    }

    #[test]
    fn retention_requires_no_ttl() {
        let mut evidence = RetentionRunwayEvidenceV1::test_fixture();
        assert!(evidence.proven());

        evidence.observations[0].partition_key = Some("toDate(persisted_time)".to_owned());
        assert!(!evidence.proven());

        evidence = RetentionRunwayEvidenceV1::test_fixture();
        evidence.measured_history_days = Some(199);
        assert!(!evidence.proven());

        evidence = RetentionRunwayEvidenceV1::test_fixture();
        evidence.observations[0].table_ttl_expression =
            Some("persisted_time + toIntervalDay(200) DELETE".to_owned());
        assert!(!evidence.proven());
    }

    #[test]
    fn retention_requires_exact_set() {
        let evidence = RetentionRunwayEvidenceV1::test_fixture();
        let registry = ResearchSourceRegistry {
            required_sources: evidence.required_sources.clone(),
            bindings: vec![ResearchSourceBinding {
                source: ResearchReadinessSource::ClobL2,
                storage: ResearchSourceStorageKind::ClickHouseTable,
                object: "quant_book_l2_ledger".to_owned(),
                time_column: "persisted_time".to_owned(),
                partition_key: Some("toYYYYMM(persisted_time)".to_owned()),
                filter: None,
            }],
        };
        let mut evidence = evidence;
        evidence.registry_hash = registry.contract_hash().expect("registry hash");
        assert!(evidence.matches_registry(&registry));

        evidence.observations.clear();
        assert!(!evidence.matches_registry(&registry));
    }

    #[test]
    fn canonical_registry_filtered_observations() {
        let registry = super::research_source_registry().expect("source registry");
        assert!(
            registry
                .required_sources
                .contains(&ResearchReadinessSource::DomainObservation)
        );
        for source in [
            ResearchReadinessSource::BinanceMarketData,
            ResearchReadinessSource::PolymarketRtds,
            ResearchReadinessSource::AviationWeather,
            ResearchReadinessSource::GhcnhCalibration,
            ResearchReadinessSource::GefsEnsemble,
        ] {
            assert!(
                registry
                    .bindings
                    .iter()
                    .filter(|binding| binding.source == source)
                    .all(|binding| binding.filter.is_some())
            );
        }
    }

    #[test]
    fn shadow_profile_requires_window() {
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
