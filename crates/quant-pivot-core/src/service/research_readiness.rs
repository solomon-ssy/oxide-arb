//! Production operational-evidence collection and verification for fit preflight.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    config::EvidenceAttestationConfig,
    domain::{NewResearchReadinessEvidence, ResearchReadinessEvidenceInfo},
    enums::quant::ResearchReadinessEvidenceKind,
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, ContentHash, RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION,
        ResearchReadinessEvidencePayload, RetentionRunwayEvidenceV2, RetentionTableObservationV2,
        SHADOW_LATENCY_PROFILE_FORMAT_VERSION, ShadowLatencyProfileV1,
    },
};
use quant_pivot_repository::traits::ResearchReadinessEvidenceRepository;
use quant_pivot_research::artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore};
use quant_pivot_storage::clickhouse::{
    ClickHousePool, RAW_HISTORY_TABLES, RawHistoryTable, extract_table_ttl,
};
use serde::Serialize;
use uuid::Uuid;

const EVIDENCE_VALID_FOR: Duration = Duration::hours(6);
const LATENCY_WINDOW: Duration = Duration::hours(24);
const LOCAL_ARTIFACT_VERSION: &str = "local-development";

#[derive(Clone)]
pub struct EvidenceAttestor {
    key_id: String,
    key: [u8; 32],
}

impl EvidenceAttestor {
    pub fn from_config(config: &EvidenceAttestationConfig) -> QuantResult<Option<Self>> {
        match (config.key_id.trim(), config.secret_hex.as_deref()) {
            ("", None) => Ok(None),
            ("", Some(_)) => Err(methodology(
                "research evidence attestation secret requires a non-empty key_id",
            )),
            (_, None) => Err(methodology(
                "research evidence attestation key_id requires secret_hex",
            )),
            (key_id, Some(secret)) => Ok(Some(Self {
                key_id: key_id.to_owned(),
                key: decode_key(secret)?,
            })),
        }
    }

    fn mac<T: Serialize + ?Sized>(&self, value: &T) -> QuantResult<ContentHash> {
        let bytes = serde_json::to_vec(value).map_err(|error| {
            QuantError::from(ResearchError::Serialization {
                detail: format!("readiness attestation serialization failed: {error}"),
            })
        })?;
        ContentHash::parse(format!(
            "blake3:{}",
            blake3::keyed_hash(&self.key, &bytes).to_hex()
        ))
        .map_err(Into::into)
    }
}

#[derive(Serialize)]
struct AttestationInput<'a> {
    kind: ResearchReadinessEvidenceKind,
    scope_hash: &'a ContentHash,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    payload_hash: &'a ContentHash,
    artifact_uri: &'a ArtifactUri,
    artifact_version: &'a str,
}

pub struct VerifiedOperationalEvidence {
    pub retention: Option<ResearchReadinessEvidenceInfo>,
    pub latency: Option<ResearchReadinessEvidenceInfo>,
    pub diagnostics: Vec<String>,
}

pub struct ResearchReadinessEvidenceService {
    repo: Arc<dyn ResearchReadinessEvidenceRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    attestor: Option<EvidenceAttestor>,
    retention_scope_hash: ContentHash,
    latency_scope_hash: ContentHash,
}

impl ResearchReadinessEvidenceService {
    pub fn new(
        repo: Arc<dyn ResearchReadinessEvidenceRepository>,
        artifacts: Arc<dyn ArtifactStore>,
        attestor: Option<EvidenceAttestor>,
    ) -> QuantResult<Self> {
        Ok(Self {
            repo,
            artifacts,
            attestor,
            retention_scope_hash: evidence_scope_hash(
                ResearchReadinessEvidenceKind::RetentionRunway,
            )?,
            latency_scope_hash: evidence_scope_hash(
                ResearchReadinessEvidenceKind::ShadowLatencyProfile,
            )?,
        })
    }

    /// Read-only preflight lookup. Evidence production never runs in this path.
    pub async fn latest_verified(
        &self,
        as_of: DateTime<Utc>,
    ) -> QuantResult<VerifiedOperationalEvidence> {
        let mut diagnostics = Vec::new();
        let retention = match self
            .verified_payload(ResearchReadinessEvidenceKind::RetentionRunway, as_of)
            .await
        {
            Ok(Some(evidence)) => Some(evidence),
            Ok(None) => {
                diagnostics
                    .push("no current signed raw-history retention evidence exists".to_owned());
                None
            }
            Err(error) => {
                diagnostics.push(format!("retention evidence rejected: {error}"));
                None
            }
        };
        let latency = match self
            .verified_payload(ResearchReadinessEvidenceKind::ShadowLatencyProfile, as_of)
            .await
        {
            Ok(Some(evidence)) => Some(evidence),
            Ok(None) => {
                diagnostics.push("no current signed shadow-latency evidence exists".to_owned());
                None
            }
            Err(error) => {
                diagnostics.push(format!("shadow latency evidence rejected: {error}"));
                None
            }
        };
        Ok(VerifiedOperationalEvidence {
            retention,
            latency,
            diagnostics,
        })
    }

    /// Verify the exact append-only readiness observation frozen by a research
    /// artifact. Historical validation checks integrity and attestation, not
    /// whether the evidence would still satisfy a new preflight freshness gate.
    pub async fn verified_by_id(
        &self,
        evidence_id: Uuid,
    ) -> QuantResult<ResearchReadinessEvidenceInfo> {
        let info = self.repo.find_by_id(evidence_id).await?.ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: format!("frozen readiness evidence {evidence_id} does not exist"),
            }
        })?;
        self.verify(info).await
    }

    async fn verified_payload(
        &self,
        kind: ResearchReadinessEvidenceKind,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<ResearchReadinessEvidenceInfo>> {
        if self.attestor.is_none() {
            return Ok(None);
        }
        let scope_hash = match kind {
            ResearchReadinessEvidenceKind::RetentionRunway => &self.retention_scope_hash,
            ResearchReadinessEvidenceKind::ShadowLatencyProfile => &self.latency_scope_hash,
        };
        let Some(info) = self.repo.latest_valid(kind, scope_hash, as_of).await? else {
            return Ok(None);
        };
        self.verify(info).await.map(Some)
    }

    async fn verify(
        &self,
        info: ResearchReadinessEvidenceInfo,
    ) -> QuantResult<ResearchReadinessEvidenceInfo> {
        let attestor =
            self.attestor
                .as_ref()
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "readiness evidence attestation key is not configured".to_owned(),
                })?;
        if info.attestation_key_id != attestor.key_id {
            return Err(methodology(
                "readiness evidence was signed by a non-current attestation key",
            ));
        }
        verify_kind_binding(info.kind, &info.payload_json)?;
        let bytes = self.artifacts.get(&info.artifact_uri).await?;
        let actual_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(&bytes))?;
        let metadata = self.artifacts.metadata(&info.artifact_uri).await?;
        if actual_hash != info.payload_hash
            || metadata.version_id.as_deref() != Some(info.artifact_version.as_str())
            || !metadata.durability.permits_production_publish()
        {
            return Err(methodology(
                "readiness evidence artifact hash/version/WORM durability check failed",
            ));
        }
        let artifact_payload: ResearchReadinessEvidencePayload = serde_json::from_slice(&bytes)
            .map_err(|error| {
                QuantError::from(ResearchError::Serialization {
                    detail: format!("readiness evidence artifact is invalid: {error}"),
                })
            })?;
        if artifact_payload != info.payload_json {
            return Err(methodology(
                "readiness evidence artifact differs from the append-only index payload",
            ));
        }
        let mac = attestor.mac(&attestation_input(&info))?;
        if mac != info.attestation_mac {
            return Err(methodology("readiness evidence attestation MAC is invalid"));
        }
        Ok(info)
    }
}

fn attestation_input(info: &ResearchReadinessEvidenceInfo) -> AttestationInput<'_> {
    AttestationInput {
        kind: info.kind,
        scope_hash: &info.scope_hash,
        window_start: info.window_start,
        window_end: info.window_end,
        observed_at: info.observed_at,
        expires_at: info.expires_at,
        payload_hash: &info.payload_hash,
        artifact_uri: &info.artifact_uri,
        artifact_version: &info.artifact_version,
    }
}

fn verify_kind_binding(
    kind: ResearchReadinessEvidenceKind,
    payload: &ResearchReadinessEvidencePayload,
) -> QuantResult<()> {
    let valid = matches!(
        (kind, payload),
        (
            ResearchReadinessEvidenceKind::RetentionRunway,
            ResearchReadinessEvidencePayload::RetentionRunway(_)
        ) | (
            ResearchReadinessEvidenceKind::ShadowLatencyProfile,
            ResearchReadinessEvidencePayload::ShadowLatencyProfile(_)
        )
    );
    if !valid {
        return Err(methodology(
            "readiness evidence kind does not match its typed payload",
        ));
    }
    Ok(())
}

fn evidence_scope_hash(kind: ResearchReadinessEvidenceKind) -> QuantResult<ContentHash> {
    let format_version = match kind {
        ResearchReadinessEvidenceKind::RetentionRunway => RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION,
        ResearchReadinessEvidenceKind::ShadowLatencyProfile => {
            SHADOW_LATENCY_PROFILE_FORMAT_VERSION
        }
    };
    CanonicalDigest::content_hash_json(&(
        "research_readiness_evidence_scope_v2",
        kind,
        format_version,
        "clickhouse_cloud",
    ))
    .map_err(Into::into)
}

/// Periodic producer. It measures `ClickHouse` directly, writes content-addressed
/// evidence, then appends the signed index row.
pub struct ResearchReadinessEvidenceProducer {
    repo: Arc<dyn ResearchReadinessEvidenceRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    clickhouse: Arc<ClickHousePool>,
    attestor: Option<EvidenceAttestor>,
}

impl ResearchReadinessEvidenceProducer {
    #[must_use]
    pub const fn new(
        repo: Arc<dyn ResearchReadinessEvidenceRepository>,
        artifacts: Arc<dyn ArtifactStore>,
        clickhouse: Arc<ClickHousePool>,
        attestor: Option<EvidenceAttestor>,
    ) -> Self {
        Self {
            repo,
            artifacts,
            clickhouse,
            attestor,
        }
    }

    pub async fn capture(&self, required_days: u32) -> QuantResult<bool> {
        let Some(attestor) = self.attestor.as_ref() else {
            tracing::warn!(
                "research readiness evidence producer disabled: attestation key is not configured"
            );
            return Ok(false);
        };
        let observed_at = Utc::now();
        let retention = self.capture_retention(required_days, observed_at).await?;
        self.persist(
            ResearchReadinessEvidenceKind::RetentionRunway,
            ResearchReadinessEvidencePayload::RetentionRunway(retention),
            observed_at - Duration::days(i64::from(required_days)),
            observed_at,
            observed_at,
            attestor,
        )
        .await?;
        let latency = self.capture_shadow_latency(observed_at).await?;
        self.persist(
            ResearchReadinessEvidenceKind::ShadowLatencyProfile,
            ResearchReadinessEvidencePayload::ShadowLatencyProfile(latency),
            observed_at - LATENCY_WINDOW,
            observed_at,
            observed_at,
            attestor,
        )
        .await?;
        Ok(true)
    }

    async fn persist(
        &self,
        kind: ResearchReadinessEvidenceKind,
        payload: ResearchReadinessEvidencePayload,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        observed_at: DateTime<Utc>,
        attestor: &EvidenceAttestor,
    ) -> QuantResult<()> {
        let expires_at = observed_at + EVIDENCE_VALID_FOR;
        let bytes = serde_json::to_vec(&payload).map_err(|error| {
            QuantError::from(ResearchError::Serialization {
                detail: format!("readiness evidence serialization failed: {error}"),
            })
        })?;
        let payload_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(&bytes))?;
        let scope_hash = evidence_scope_hash(kind)?;
        let artifact_uri = self
            .artifacts
            .put(
                ArtifactKey::new(
                    ArtifactNamespace::ReadinessEvidence,
                    payload_hash.hex(),
                    "json",
                )?,
                &bytes,
            )
            .await?;
        let metadata = self.artifacts.metadata(&artifact_uri).await?;
        let artifact_version = metadata
            .version_id
            .unwrap_or_else(|| LOCAL_ARTIFACT_VERSION.to_owned());
        let input = AttestationInput {
            kind,
            scope_hash: &scope_hash,
            window_start,
            window_end,
            observed_at,
            expires_at,
            payload_hash: &payload_hash,
            artifact_uri: &artifact_uri,
            artifact_version: &artifact_version,
        };
        let attestation_mac = attestor.mac(&input)?;
        self.repo
            .append(NewResearchReadinessEvidence {
                evidence_id: Uuid::now_v7(),
                kind,
                scope_hash,
                window_start,
                window_end,
                observed_at,
                expires_at,
                payload_json: payload,
                payload_hash,
                artifact_uri,
                artifact_version,
                attestation_key_id: attestor.key_id.clone(),
                attestation_mac,
            })
            .await?;
        Ok(())
    }

    async fn capture_retention(
        &self,
        required_days: u32,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<RetentionRunwayEvidenceV2> {
        let mut tables = Vec::with_capacity(RAW_HISTORY_TABLES.len());
        for spec in RAW_HISTORY_TABLES {
            tables.push(self.retention_table(spec, observed_at).await?);
        }
        let history_start = tables
            .iter()
            .filter_map(|table| table.earliest_event_time)
            .max();
        let all_tables_observed = tables
            .iter()
            .all(|table| table.earliest_event_time.is_some());
        let measured_history_days = if all_tables_observed {
            history_start.and_then(|start| duration_days(observed_at - start))
        } else {
            None
        };
        let active_raw_bytes = tables.iter().try_fold(0_u64, |total, table| {
            total.checked_add(table.active_bytes).ok_or_else(|| {
                QuantError::from(ResearchError::ValidationMethodology {
                    detail: "raw ClickHouse byte count overflow".to_owned(),
                })
            })
        })?;
        Ok(RetentionRunwayEvidenceV2 {
            format_version: RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION,
            observed_at,
            required_days,
            measured_history_days,
            active_raw_bytes,
            tables,
        })
    }

    async fn retention_table(
        &self,
        spec: RawHistoryTable,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<RetentionTableObservationV2> {
        let observation = self
            .clickhouse
            .observe_raw_history_table(spec, observed_at)
            .await?;
        Ok(RetentionTableObservationV2 {
            table: spec.table.to_owned(),
            time_column: spec.time_column.to_owned(),
            earliest_event_time: timestamp_millis(observation.earliest_ms)?,
            latest_event_time: timestamp_millis(observation.latest_ms)?,
            row_count: observation.row_count,
            active_bytes: observation.active_bytes,
            active_partition_count: observation.active_partition_count,
            partition_key: observation.partition_key,
            table_ttl_expression: extract_table_ttl(&observation.create_table_query),
        })
    }

    async fn capture_shadow_latency(
        &self,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<ShadowLatencyProfileV1> {
        let window_start = observed_at - LATENCY_WINDOW;
        let row = self
            .clickhouse
            .observe_book_latency(window_start, observed_at)
            .await?;
        let pg = self
            .repo
            .observe_shadow_latency(window_start, observed_at)
            .await?;
        Ok(ShadowLatencyProfileV1 {
            format_version: SHADOW_LATENCY_PROFILE_FORMAT_VERSION,
            window_start,
            window_end: observed_at,
            observed_at,
            book_event_count: row.event_count,
            book_age_p50_ms: row.age_p50_ms,
            book_age_p95_ms: row.age_p95_ms,
            book_age_p99_ms: row.age_p99_ms,
            decision_prepared_count: pg.decision_prepared_count,
            decision_prepared_p95_ms: pg.decision_prepared_p95_ms,
            endpoint_rtt_count: pg.endpoint_rtt_count,
            endpoint_rtt_p95_ms: pg.endpoint_rtt_p95_ms,
            market_delay_count: pg.market_delay_count,
            market_delay_p95_ms: pg.market_delay_p95_ms,
        })
    }
}

fn duration_days(duration: Duration) -> Option<u32> {
    let seconds = duration.num_seconds();
    (seconds >= 0)
        .then_some(seconds / 86_400)
        .and_then(|days| u32::try_from(days).ok())
}

fn timestamp_millis(value: Option<i64>) -> QuantResult<Option<DateTime<Utc>>> {
    value
        .map(|millis| {
            DateTime::from_timestamp_millis(millis).ok_or_else(|| {
                methodology("ClickHouse readiness timestamp is outside chrono range")
            })
        })
        .transpose()
}

fn decode_key(value: &str) -> QuantResult<[u8; 32]> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(methodology(
            "research evidence attestation secret_hex must contain exactly 64 hex characters",
        ));
    }
    let mut key = [0_u8; 32];
    for (index, slot) in key.iter_mut().enumerate() {
        let offset = index * 2;
        *slot = u8::from_str_radix(&value[offset..offset + 2], 16).map_err(|error| {
            methodology(format!(
                "research evidence attestation secret_hex is invalid: {error}"
            ))
        })?;
    }
    Ok(key)
}

fn methodology(detail: impl Into<String>) -> QuantError {
    ResearchError::ValidationMethodology {
        detail: detail.into(),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        config::EvidenceAttestationConfig,
        enums::quant::ResearchReadinessEvidenceKind,
        types::{ResearchReadinessEvidencePayload, ShadowLatencyProfileV1},
    };

    use super::{EvidenceAttestor, verify_kind_binding};

    #[test]
    fn attestation_config_is_all_or_nothing() {
        assert!(
            EvidenceAttestor::from_config(&EvidenceAttestationConfig::default())
                .expect("disabled attestor")
                .is_none()
        );
        assert!(
            EvidenceAttestor::from_config(&EvidenceAttestationConfig {
                key_id: "operator-2026-07".to_owned(),
                secret_hex: Some("ab".repeat(32)),
            })
            .expect("configured attestor")
            .is_some()
        );
    }

    #[test]
    fn keyed_attestation_changes_when_signed_content_changes() {
        let attestor = EvidenceAttestor::from_config(&EvidenceAttestationConfig {
            key_id: "operator-2026-07".to_owned(),
            secret_hex: Some("ab".repeat(32)),
        })
        .expect("valid attestor config")
        .expect("configured attestor");
        let original = attestor.mac(&("scope", 1_u32)).expect("original MAC");
        let tampered = attestor.mac(&("scope", 2_u32)).expect("tampered MAC");
        assert_ne!(original, tampered);
    }

    #[test]
    fn evidence_kind_cannot_be_rebound_to_another_payload() {
        let observed_at = Utc
            .timestamp_opt(1_720_000_000, 0)
            .single()
            .expect("valid timestamp");
        let payload =
            ResearchReadinessEvidencePayload::ShadowLatencyProfile(ShadowLatencyProfileV1 {
                format_version: 1,
                window_start: observed_at - Duration::hours(24),
                window_end: observed_at,
                observed_at,
                book_event_count: 1,
                book_age_p50_ms: 1,
                book_age_p95_ms: 2,
                book_age_p99_ms: 3,
                decision_prepared_count: 1,
                decision_prepared_p95_ms: Some(4),
                endpoint_rtt_count: 1,
                endpoint_rtt_p95_ms: Some(5),
                market_delay_count: 1,
                market_delay_p95_ms: Some(6),
            });
        assert!(
            verify_kind_binding(
                ResearchReadinessEvidenceKind::ShadowLatencyProfile,
                &payload,
            )
            .is_ok()
        );
        assert!(
            verify_kind_binding(ResearchReadinessEvidenceKind::RetentionRunway, &payload).is_err()
        );
    }
}
