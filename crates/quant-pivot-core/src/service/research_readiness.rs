//! Production operational-evidence collection and verification for fit preflight.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use blake3::Hasher;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    config::{
        ArtifactStoreDeployConfig, ArtifactStoreKind, ClickHouseConfig, EvidenceAttestationConfig,
    },
    domain::{
        ports::{ResearchReadinessPort, ResearchReadinessSnapshot},
        quant::{NewResearchReadinessEvidence, ResearchReadinessEvidenceInfo},
    },
    enums::quant::ResearchReadinessEvidenceKind,
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, ArtifactVersion, AttestationKeyId, ContentHash, HistoryCoverage,
        RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION, ResearchReadinessEvidenceId,
        ResearchReadinessEvidencePayload, ResearchSourceBinding, ResearchSourceRegistry,
        ResearchSourceStorageKind, RetentionRunwayEvidenceV1, RetentionSourceObservationV1,
        SHADOW_LATENCY_PROFILE_FORMAT_VERSION, ShadowLatencyProfileV1, research_source_registry,
    },
};
use quant_pivot_repository::traits::{
    CatalogLedgerRepository, ClobMarketInfoRepository, ResearchReadinessEvidenceRepository,
};
use quant_pivot_research::artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore};
use quant_pivot_storage::clickhouse::{ClickHousePool, extract_table_ttl, schema_contract_hash};
use serde::Serialize;
use zeroize::{Zeroize, ZeroizeOnDrop};

const EVIDENCE_VALID_FOR: Duration = Duration::hours(6);
const LATENCY_WINDOW: Duration = Duration::hours(24);
const LOCAL_ARTIFACT_VERSION: &str = "local-development";
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
struct AttestationKey([u8; 32]);

#[derive(Clone)]
pub struct EvidenceAttestor {
    active_key_id: AttestationKeyId,
    keys: BTreeMap<AttestationKeyId, AttestationKey>,
}

impl EvidenceAttestor {
    pub fn from_config(config: &EvidenceAttestationConfig) -> QuantResult<Option<Self>> {
        let active = config.signing_key.expose_secret();
        if active.is_empty() {
            if config.previous_signing_keys.is_empty() {
                return Ok(None);
            }
            return Err(methodology(
                "research evidence previous_signing_keys require an active signing_key",
            ));
        }
        let active_key = decode_key(active)?;
        let active_key_id = active_key.attestation_key_id()?;
        let mut keys = BTreeMap::new();
        keys.insert(active_key_id.clone(), active_key);
        for previous in &config.previous_signing_keys {
            let key = decode_key(previous.expose_secret())?;
            let key_id = key.attestation_key_id()?;
            if key_id == active_key_id {
                return Err(methodology(
                    "research evidence active signing_key must not appear in previous_signing_keys",
                ));
            }
            if keys.insert(key_id, key).is_some() {
                return Err(methodology(
                    "research evidence previous_signing_keys must not contain duplicates",
                ));
            }
        }
        Ok(Some(Self {
            active_key_id,
            keys,
        }))
    }

    fn mac<T: Serialize + ?Sized>(
        &self,
        key_id: &AttestationKeyId,
        value: &T,
    ) -> QuantResult<ContentHash> {
        let key = self.keys.get(key_id).ok_or_else(|| {
            methodology(format!(
                "research evidence attestation key `{key_id}` is not present in the configured active/previous key set"
            ))
        })?;
        let bytes = CanonicalDigest::canonical_json_bytes(value).map_err(|error| {
            QuantError::from(ResearchError::Serialization {
                detail: format!("readiness attestation serialization failed: {error}"),
            })
        })?;
        ContentHash::parse(&format!(
            "blake3:{}",
            blake3::keyed_hash(&key.0, &bytes).to_hex()
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
    artifact_version: &'a ArtifactVersion,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum EvidenceArtifactDurabilityClass {
    LocalEphemeral,
    RemoteUnverified,
    VersionedObjectLock,
}

macro_rules! evidence_identity {
    ($name:ident, $label:literal) => {
        #[derive(Debug, Clone, Serialize)]
        #[serde(transparent)]
        struct $name(String);

        impl $name {
            fn parse(value: &str) -> QuantResult<Self> {
                let value = value.trim();
                if value.is_empty() || value.len() > 128 {
                    return Err(methodology(concat!(
                        $label,
                        " must contain 1..=128 non-whitespace characters"
                    )));
                }
                Ok(Self(value.to_owned()))
            }
        }
    };
}

evidence_identity!(DeploymentIdentity, "deployment_id");
evidence_identity!(ClickHouseClusterIdentity, "clickhouse_cluster_id");
evidence_identity!(ClickHouseDatabaseIdentity, "clickhouse_database");

#[derive(Debug, Clone, Serialize)]
pub struct EvidenceScopeIdentity {
    deployment_id: DeploymentIdentity,
    clickhouse_cluster_id: ClickHouseClusterIdentity,
    clickhouse_database: ClickHouseDatabaseIdentity,
    clickhouse_schema_contract_hash: String,
    research_source_registry_hash: ContentHash,
    artifact_durability_class: EvidenceArtifactDurabilityClass,
}

impl EvidenceScopeIdentity {
    pub fn from_config(
        clickhouse: &ClickHouseConfig,
        artifacts: &ArtifactStoreDeployConfig,
    ) -> QuantResult<Self> {
        let artifact_durability_class = match artifacts.kind {
            ArtifactStoreKind::Local => EvidenceArtifactDurabilityClass::LocalEphemeral,
            ArtifactStoreKind::S3
                if artifacts.require_versioning && artifacts.require_object_lock =>
            {
                EvidenceArtifactDurabilityClass::VersionedObjectLock
            }
            ArtifactStoreKind::S3 => EvidenceArtifactDurabilityClass::RemoteUnverified,
        };
        let registry_hash = research_source_registry()
            .and_then(|registry| registry.contract_hash())
            .map_err(methodology)?;
        Ok(Self {
            deployment_id: DeploymentIdentity::parse(&clickhouse.deployment_id)?,
            clickhouse_cluster_id: ClickHouseClusterIdentity::parse(&clickhouse.cluster_id)?,
            clickhouse_database: ClickHouseDatabaseIdentity::parse(&clickhouse.database)?,
            clickhouse_schema_contract_hash: schema_contract_hash(),
            research_source_registry_hash: registry_hash,
            artifact_durability_class,
        })
    }
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
    research_source_registry: ResearchSourceRegistry,
}

impl ResearchReadinessEvidenceService {
    pub fn new(
        repo: Arc<dyn ResearchReadinessEvidenceRepository>,
        artifacts: Arc<dyn ArtifactStore>,
        attestor: Option<EvidenceAttestor>,
        scope: &EvidenceScopeIdentity,
    ) -> QuantResult<Self> {
        let research_source_registry = research_source_registry().map_err(methodology)?;
        Ok(Self {
            repo,
            artifacts,
            attestor,
            retention_scope_hash: evidence_scope_hash(
                ResearchReadinessEvidenceKind::RetentionRunway,
                scope,
            )?,
            latency_scope_hash: evidence_scope_hash(
                ResearchReadinessEvidenceKind::ShadowLatencyProfile,
                scope,
            )?,
            research_source_registry,
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
        evidence_id: &ResearchReadinessEvidenceId,
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
        verify_kind_binding(info.kind, &info.payload_json)?;
        if let ResearchReadinessEvidencePayload::RetentionRunway(retention) = &info.payload_json
            && !retention.matches_registry(&self.research_source_registry)
        {
            return Err(methodology(
                "retention evidence does not contain the exact current research source registry",
            ));
        }
        let bytes = self.artifacts.get(&info.artifact_uri).await?;
        let actual_hash = CanonicalDigest::content_hash_bytes(&bytes);
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
        let mac = attestor.mac(&info.attestation_key_id, &attestation_input(&info))?;
        if mac != info.attestation_mac {
            return Err(methodology("readiness evidence attestation MAC is invalid"));
        }
        Ok(info)
    }
}

#[async_trait]
impl ResearchReadinessPort for ResearchReadinessEvidenceService {
    async fn snapshot(&self) -> QuantResult<Option<ResearchReadinessSnapshot>> {
        let evidence = self.latest_verified(Utc::now()).await?;
        let retention = evidence.retention.as_ref().and_then(|item| {
            let ResearchReadinessEvidencePayload::RetentionRunway(retention) = &item.payload_json
            else {
                return None;
            };
            Some((item.observed_at, retention))
        });
        let latency = evidence.latency.as_ref().and_then(|item| {
            let ResearchReadinessEvidencePayload::ShadowLatencyProfile(latency) =
                &item.payload_json
            else {
                return None;
            };
            Some((item.observed_at, latency))
        });
        let Some((retention_observed_at, retention)) = retention else {
            return Ok(None);
        };
        let observed_at = latency
            .map(|(observed_at, _)| observed_at)
            .map_or(retention_observed_at, |latency_observed_at| {
                retention_observed_at.max(latency_observed_at)
            });
        Ok(Some(ResearchReadinessSnapshot {
            observed_at,
            required_history_days: retention.required_days,
            observed_history_days: retention.measured_history_days,
            retention_ready: retention.proven(),
            latency_ready: latency.is_some_and(|(_, profile)| profile.complete_for(1)),
        }))
    }
}

const fn attestation_input(info: &ResearchReadinessEvidenceInfo) -> AttestationInput<'_> {
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

fn evidence_scope_hash(
    kind: ResearchReadinessEvidenceKind,
    identity: &EvidenceScopeIdentity,
) -> QuantResult<ContentHash> {
    let format_version = match kind {
        ResearchReadinessEvidenceKind::RetentionRunway => RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION,
        ResearchReadinessEvidenceKind::ShadowLatencyProfile => {
            SHADOW_LATENCY_PROFILE_FORMAT_VERSION
        }
    };
    CanonicalDigest::content_hash_json(&(
        "research_readiness_evidence_scope_v4",
        kind,
        format_version,
        identity,
    ))
    .map_err(Into::into)
}

/// Atomic owner of content-addressed readiness bytes, attestation, and the
/// append-only `PostgreSQL` index row.
pub struct ResearchReadinessEvidenceWriter {
    repo: Arc<dyn ResearchReadinessEvidenceRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    attestor: Option<EvidenceAttestor>,
    scope: EvidenceScopeIdentity,
}

impl ResearchReadinessEvidenceWriter {
    #[must_use]
    pub const fn new(
        repo: Arc<dyn ResearchReadinessEvidenceRepository>,
        artifacts: Arc<dyn ArtifactStore>,
        attestor: Option<EvidenceAttestor>,
        scope: EvidenceScopeIdentity,
    ) -> Self {
        Self {
            repo,
            artifacts,
            attestor,
            scope,
        }
    }

    /// Persist one immutable typed observation and return its exact index row.
    pub async fn persist(
        &self,
        kind: ResearchReadinessEvidenceKind,
        payload: ResearchReadinessEvidencePayload,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<ResearchReadinessEvidenceInfo> {
        let attestor =
            self.attestor
                .as_ref()
                .ok_or_else(|| ResearchError::ValidationMethodology {
                    detail: "readiness evidence attestation key is not configured".to_owned(),
                })?;
        let expires_at = observed_at + EVIDENCE_VALID_FOR;
        let bytes = CanonicalDigest::canonical_json_bytes(&payload).map_err(|error| {
            QuantError::from(ResearchError::Serialization {
                detail: format!("readiness evidence serialization failed: {error}"),
            })
        })?;
        let payload_hash = CanonicalDigest::content_hash_bytes(&bytes);
        let scope_hash = evidence_scope_hash(kind, &self.scope)?;
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
        let artifact_version = ArtifactVersion::parse(artifact_version)
            .map_err(|error| methodology(error.to_string()))?;
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
        let attestation_mac = attestor.mac(&attestor.active_key_id, &input)?;
        self.repo
            .append(NewResearchReadinessEvidence {
                evidence_id: ResearchReadinessEvidenceId::from_v7(),
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
                attestation_key_id: attestor.active_key_id.clone(),
                attestation_mac,
            })
            .await
            .map_err(Into::into)
    }
}

/// Periodic producer. It measures `ClickHouse` directly, writes content-addressed
/// evidence, then appends the signed index row.
pub struct ResearchReadinessEvidenceProducer {
    writer: ResearchReadinessEvidenceWriter,
    clickhouse: Arc<ClickHousePool>,
    catalog: Arc<dyn CatalogLedgerRepository>,
    clob_market_info: Arc<dyn ClobMarketInfoRepository>,
    source_registry: ResearchSourceRegistry,
}

impl ResearchReadinessEvidenceProducer {
    pub fn new(
        repo: Arc<dyn ResearchReadinessEvidenceRepository>,
        artifacts: Arc<dyn ArtifactStore>,
        clickhouse: Arc<ClickHousePool>,
        catalog: Arc<dyn CatalogLedgerRepository>,
        clob_market_info: Arc<dyn ClobMarketInfoRepository>,
        attestor: Option<EvidenceAttestor>,
        scope: EvidenceScopeIdentity,
    ) -> QuantResult<Self> {
        let source_registry = research_source_registry().map_err(methodology)?;
        Ok(Self {
            writer: ResearchReadinessEvidenceWriter::new(repo, artifacts, attestor, scope),
            clickhouse,
            catalog,
            clob_market_info,
            source_registry,
        })
    }

    pub async fn capture(&self, required_days: u32) -> QuantResult<bool> {
        if self.writer.attestor.is_none() {
            tracing::warn!(
                "research readiness evidence producer disabled: attestation key is not configured"
            );
            return Ok(false);
        }
        let observed_at = Utc::now();
        let retention = self.capture_retention(required_days, observed_at).await?;
        self.writer
            .persist(
                ResearchReadinessEvidenceKind::RetentionRunway,
                ResearchReadinessEvidencePayload::RetentionRunway(retention),
                observed_at - Duration::days(i64::from(required_days)),
                observed_at,
                observed_at,
            )
            .await?;
        let latency = self.capture_shadow_latency(observed_at).await?;
        self.writer
            .persist(
                ResearchReadinessEvidenceKind::ShadowLatencyProfile,
                ResearchReadinessEvidencePayload::ShadowLatencyProfile(latency),
                observed_at - LATENCY_WINDOW,
                observed_at,
                observed_at,
            )
            .await?;
        Ok(true)
    }

    async fn capture_retention(
        &self,
        required_days: u32,
        observed_at: DateTime<Utc>,
    ) -> QuantResult<RetentionRunwayEvidenceV1> {
        let catalog_coverage = self.catalog.research_history_coverage(observed_at).await?;
        let clob_coverage = self
            .clob_market_info
            .research_history_coverage(observed_at)
            .await?;
        let mut observations = Vec::with_capacity(self.source_registry.bindings.len());
        for binding in &self.source_registry.bindings {
            observations.push(
                self.retention_source(binding, observed_at, &catalog_coverage, &clob_coverage)
                    .await?,
            );
        }
        let history_start = observations
            .iter()
            .filter_map(|observation| observation.earliest_event_time)
            .max();
        let all_sources_observed = observations
            .iter()
            .all(|observation| observation.earliest_event_time.is_some());
        let measured_history_days = if all_sources_observed {
            history_start.and_then(|start| duration_days(observed_at - start))
        } else {
            None
        };
        let active_raw_bytes = observations.iter().try_fold(0_u64, |total, observation| {
            total
                .checked_add(observation.active_bytes.unwrap_or(0))
                .ok_or_else(|| {
                    QuantError::from(ResearchError::ValidationMethodology {
                        detail: "raw ClickHouse byte count overflow".to_owned(),
                    })
                })
        })?;
        Ok(RetentionRunwayEvidenceV1 {
            format_version: RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION,
            registry_hash: self.source_registry.contract_hash().map_err(methodology)?,
            required_sources: self.source_registry.required_sources.clone(),
            observed_at,
            required_days,
            measured_history_days,
            active_raw_bytes,
            observations,
        })
    }

    async fn retention_source(
        &self,
        binding: &ResearchSourceBinding,
        observed_at: DateTime<Utc>,
        catalog_coverage: &[HistoryCoverage],
        clob_coverage: &[HistoryCoverage],
    ) -> QuantResult<RetentionSourceObservationV1> {
        match binding.storage {
            ResearchSourceStorageKind::ClickHouseTable => {
                let observation = self
                    .clickhouse
                    .observe_raw_history_table(binding, observed_at)
                    .await?;
                Ok(RetentionSourceObservationV1 {
                    source: binding.source,
                    storage: binding.storage,
                    object: binding.object.clone(),
                    time_column: binding.time_column.clone(),
                    earliest_event_time: timestamp_millis(observation.earliest_ms)?,
                    latest_event_time: timestamp_millis(observation.latest_ms)?,
                    row_count: observation.row_count,
                    active_bytes: Some(observation.active_bytes),
                    active_partition_count: Some(observation.active_partition_count),
                    partition_key: Some(observation.partition_key),
                    table_ttl_expression: extract_table_ttl(&observation.create_table_query),
                })
            }
            ResearchSourceStorageKind::PostgresLedger => {
                pg_retention_observation(binding, catalog_coverage)
            }
            ResearchSourceStorageKind::PostgresVersionedProjection => {
                pg_retention_observation(binding, clob_coverage)
            }
        }
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
            .writer
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

fn pg_retention_observation(
    binding: &ResearchSourceBinding,
    coverage: &[HistoryCoverage],
) -> QuantResult<RetentionSourceObservationV1> {
    let observed = coverage
        .iter()
        .find(|observed| observed.object == binding.object)
        .ok_or_else(|| {
            methodology(format!(
                "PostgreSQL research source `{}` returned no coverage observation",
                binding.object
            ))
        })?;
    if observed.time_column != binding.time_column {
        return Err(methodology(format!(
            "PostgreSQL research source `{}` reported time column `{}`, expected `{}`",
            binding.object, observed.time_column, binding.time_column
        )));
    }
    Ok(RetentionSourceObservationV1 {
        source: binding.source,
        storage: binding.storage,
        object: binding.object.clone(),
        time_column: binding.time_column.clone(),
        earliest_event_time: observed.earliest_event_time,
        latest_event_time: observed.latest_event_time,
        row_count: observed.row_count,
        active_bytes: None,
        active_partition_count: None,
        partition_key: None,
        table_ttl_expression: None,
    })
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

fn decode_key(value: &str) -> QuantResult<AttestationKey> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(methodology(
            "research evidence attestation key must contain exactly 64 lowercase hex characters",
        ));
    }
    let mut key = AttestationKey([0_u8; 32]);
    for (index, slot) in key.0.iter_mut().enumerate() {
        let offset = index * 2;
        *slot = u8::from_str_radix(&value[offset..offset + 2], 16).map_err(|error| {
            methodology(format!(
                "research evidence attestation key is invalid: {error}"
            ))
        })?;
    }
    Ok(key)
}

impl AttestationKey {
    fn attestation_key_id(&self) -> QuantResult<AttestationKeyId> {
        let mut hasher =
            Hasher::new_derive_key("quant-pivot/research-evidence-attestation-key-fingerprint/v1");
        hasher.update(&self.0);
        AttestationKeyId::parse(format!("b3k1:{}", hasher.finalize().to_hex()))
            .map_err(|error| methodology(error.to_string()))
    }
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
        types::{AttestationKeyId, ResearchReadinessEvidencePayload, ShadowLatencyProfileV1},
    };

    use super::{EvidenceAttestor, verify_kind_binding};

    #[test]
    fn attestation_config_all_nothing() {
        assert!(
            EvidenceAttestor::from_config(&EvidenceAttestationConfig::default())
                .expect("disabled attestor")
                .is_none()
        );
        assert!(
            EvidenceAttestor::from_config(&EvidenceAttestationConfig {
                signing_key: "ab".repeat(32).into(),
                previous_signing_keys: Vec::new(),
            })
            .expect("configured attestor")
            .is_some()
        );
        assert!(
            EvidenceAttestor::from_config(&EvidenceAttestationConfig {
                signing_key: "".into(),
                previous_signing_keys: vec!["cd".repeat(32).into()],
            })
            .is_err()
        );
    }

    #[test]
    fn keyed_attestation_changes_keys() {
        let attestor = EvidenceAttestor::from_config(&EvidenceAttestationConfig {
            signing_key: "ab".repeat(32).into(),
            previous_signing_keys: vec!["cd".repeat(32).into()],
        })
        .expect("valid attestor config")
        .expect("configured attestor");
        let active_key_id = attestor.active_key_id.clone();
        let historical_key_id = attestor
            .keys
            .keys()
            .find(|key_id| *key_id != &active_key_id)
            .expect("historical key id")
            .clone();
        let original = attestor
            .mac(&active_key_id, &("scope", 1_u32))
            .expect("original MAC");
        let tampered = attestor
            .mac(&active_key_id, &("scope", 2_u32))
            .expect("tampered MAC");
        assert_ne!(original, tampered);
        assert!(attestor.mac(&historical_key_id, &("scope", 1_u32)).is_ok());
        let unknown_key = AttestationKeyId::parse("b3k1:unknown").expect("attestation key id");
        assert!(attestor.mac(&unknown_key, &("scope", 1_u32)).is_err());
    }

    #[test]
    fn attestation_keys_reject_hex() {
        assert!(
            EvidenceAttestor::from_config(&EvidenceAttestationConfig {
                signing_key: "ab".repeat(32).into(),
                previous_signing_keys: vec!["ab".repeat(32).into()],
            })
            .is_err()
        );
        assert!(
            EvidenceAttestor::from_config(&EvidenceAttestationConfig {
                signing_key: "AB".repeat(32).into(),
                previous_signing_keys: Vec::new(),
            })
            .is_err()
        );
    }

    #[test]
    fn evidence_kind_cannot_payload() {
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
