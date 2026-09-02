//! Production operational-evidence collection and verification for fit preflight.

use std::{
    collections::BTreeMap,
    error::Error as StdError,
    fmt::{Display, Formatter, Result as FmtResult},
    sync::Arc,
};

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
        ResearchSourceStorageKind, ResearchSourceTimeEncoding, RetentionRunwayEvidenceV1,
        RetentionSourceObservationV1, SHADOW_LATENCY_PROFILE_FORMAT_VERSION,
        ShadowLatencyProfileV1, research_source_registry,
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

/// Capture clock normalized to the precision preserved by `PostgreSQL`
/// `TIMESTAMPTZ`. Flooring the sub-microsecond remainder keeps the canonical
/// evidence clock at or before the actual observation time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadinessCaptureClock(DateTime<Utc>);

impl From<DateTime<Utc>> for ReadinessCaptureClock {
    fn from(value: DateTime<Utc>) -> Self {
        let sub_microsecond_nanos = value.timestamp_subsec_nanos() % 1_000;
        Self(value - Duration::nanoseconds(i64::from(sub_microsecond_nanos)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadinessCapturePhase {
    Measure,
    Persist,
    Assemble,
}

impl Display for ReadinessCapturePhase {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str(match self {
            Self::Measure => "measure",
            Self::Persist => "persist",
            Self::Assemble => "assemble",
        })
    }
}

impl ReadinessCapturePhase {
    fn contextualize(
        self,
        kind: ResearchReadinessEvidenceKind,
        source: QuantError,
    ) -> ReadinessCaptureFailure {
        ReadinessCaptureFailure {
            phase: self,
            kind,
            source: Box::new(source),
        }
    }
}

#[derive(Debug)]
pub struct ReadinessCaptureFailure {
    pub phase: ReadinessCapturePhase,
    pub kind: ResearchReadinessEvidenceKind,
    pub source: Box<QuantError>,
}

impl Display for ReadinessCaptureFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        write!(
            formatter,
            "research readiness evidence capture failed: phase={} kind={}: {}",
            self.phase, self.kind, self.source,
        )
    }
}

impl StdError for ReadinessCaptureFailure {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref())
    }
}

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
        let result: QuantResult<ResearchReadinessEvidenceInfo> = async {
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
            let info = self
                .repo
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
                .await?;
            Ok(info)
        }
        .await;
        result
    }
}

/// Exact signed evidence rows and derived retention readiness from one capture.
#[derive(Debug, Clone)]
pub struct ResearchReadinessCapture {
    pub retention: ResearchReadinessEvidenceInfo,
    pub latency: ResearchReadinessEvidenceInfo,
    pub retention_proven: bool,
    pub measured_history_days: Option<u32>,
    pub missing_binding_count: usize,
    pub unready_binding_count: usize,
}

impl ResearchReadinessCapture {
    fn new(
        retention: ResearchReadinessEvidenceInfo,
        latency: ResearchReadinessEvidenceInfo,
    ) -> Result<Self, ReadinessCaptureFailure> {
        if retention.kind != ResearchReadinessEvidenceKind::RetentionRunway
            || latency.kind != ResearchReadinessEvidenceKind::ShadowLatencyProfile
        {
            return Err(ReadinessCapturePhase::Assemble.contextualize(
                ResearchReadinessEvidenceKind::RetentionRunway,
                methodology("readiness capture result contains a mismatched evidence kind"),
            ));
        }
        let ResearchReadinessEvidencePayload::RetentionRunway(payload) = &retention.payload_json
        else {
            return Err(ReadinessCapturePhase::Assemble.contextualize(
                ResearchReadinessEvidenceKind::RetentionRunway,
                methodology("readiness capture retention row contains a mismatched payload"),
            ));
        };
        if !matches!(
            &latency.payload_json,
            ResearchReadinessEvidencePayload::ShadowLatencyProfile(_)
        ) {
            return Err(ReadinessCapturePhase::Assemble.contextualize(
                ResearchReadinessEvidenceKind::ShadowLatencyProfile,
                methodology("readiness capture latency row contains a mismatched payload"),
            ));
        }
        let (missing_binding_count, unready_binding_count) = Self::binding_counts(payload);
        Ok(Self {
            retention_proven: payload.proven(),
            measured_history_days: payload.measured_history_days,
            missing_binding_count,
            unready_binding_count,
            retention,
            latency,
        })
    }

    fn binding_counts(payload: &RetentionRunwayEvidenceV1) -> (usize, usize) {
        let missing = |observation: &RetentionSourceObservationV1| {
            observation.earliest_event_time.is_none()
                || observation.latest_event_time.is_none()
                || observation.row_count == 0
        };
        let ready = |observation: &RetentionSourceObservationV1| {
            let history_ready = observation.earliest_event_time.is_some_and(|earliest| {
                duration_days(payload.observed_at - earliest)
                    .is_some_and(|days| days >= payload.required_days)
            });
            let storage_ready = match observation.storage {
                ResearchSourceStorageKind::ClickHouseTable => {
                    observation.active_bytes.is_some()
                        && observation
                            .active_partition_count
                            .is_some_and(|count| count > 0)
                        && observation
                            .partition_key
                            .as_deref()
                            .is_some_and(|key| !key.trim().is_empty())
                        && matches!(
                            observation.time_encoding,
                            ResearchSourceTimeEncoding::ClickHouseDateTime64Milliseconds
                                | ResearchSourceTimeEncoding::ClickHouseUnixMilliseconds
                        )
                }
                ResearchSourceStorageKind::PostgresLedger
                | ResearchSourceStorageKind::PostgresVersionedProjection => {
                    observation.active_bytes.is_none()
                        && observation.active_partition_count.is_none()
                        && observation.partition_key.is_none()
                        && observation.time_encoding
                            == ResearchSourceTimeEncoding::PostgresTimestampWithTimeZone
                }
            };
            history_ready && observation.table_ttl_expression.is_none() && storage_ready
        };
        let missing_binding_count = payload
            .observations
            .iter()
            .filter(|observation| missing(observation))
            .count();
        let unready_binding_count = payload
            .observations
            .iter()
            .filter(|observation| !missing(observation) && !ready(observation))
            .count();
        (missing_binding_count, unready_binding_count)
    }
}

/// Typed result of one periodic readiness capture attempt.
#[derive(Debug, Clone)]
pub enum ResearchReadinessCaptureOutcome {
    Disabled,
    Captured(Box<ResearchReadinessCapture>),
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

    pub async fn capture(
        &self,
        required_days: u32,
    ) -> Result<ResearchReadinessCaptureOutcome, ReadinessCaptureFailure> {
        if self.writer.attestor.is_none() {
            return Ok(ResearchReadinessCaptureOutcome::Disabled);
        }
        let ReadinessCaptureClock(observed_at) = ReadinessCaptureClock::from(Utc::now());
        let retention = self
            .capture_retention(required_days, observed_at)
            .await
            .map_err(|error| {
                ReadinessCapturePhase::Measure
                    .contextualize(ResearchReadinessEvidenceKind::RetentionRunway, error)
            })?;
        let retention = self
            .writer
            .persist(
                ResearchReadinessEvidenceKind::RetentionRunway,
                ResearchReadinessEvidencePayload::RetentionRunway(retention),
                observed_at - Duration::days(i64::from(required_days)),
                observed_at,
                observed_at,
            )
            .await
            .map_err(|error| {
                ReadinessCapturePhase::Persist
                    .contextualize(ResearchReadinessEvidenceKind::RetentionRunway, error)
            })?;
        let latency = self
            .capture_shadow_latency(observed_at)
            .await
            .map_err(|error| {
                ReadinessCapturePhase::Measure
                    .contextualize(ResearchReadinessEvidenceKind::ShadowLatencyProfile, error)
            })?;
        let latency = self
            .writer
            .persist(
                ResearchReadinessEvidenceKind::ShadowLatencyProfile,
                ResearchReadinessEvidencePayload::ShadowLatencyProfile(latency),
                observed_at - LATENCY_WINDOW,
                observed_at,
                observed_at,
            )
            .await
            .map_err(|error| {
                ReadinessCapturePhase::Persist
                    .contextualize(ResearchReadinessEvidenceKind::ShadowLatencyProfile, error)
            })?;
        ResearchReadinessCapture::new(retention, latency)
            .map(Box::new)
            .map(ResearchReadinessCaptureOutcome::Captured)
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
                    time_encoding: binding.time_encoding,
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
        time_encoding: binding.time_encoding,
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
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_error::storage::StorageError;
    use quant_pivot_models::{
        config::EvidenceAttestationConfig,
        domain::quant::ResearchReadinessEvidenceInfo,
        enums::quant::ResearchReadinessEvidenceKind,
        hashing::CanonicalDigest,
        types::{
            ArtifactUri, ArtifactVersion, AttestationKeyId, ContentHash, HistoryCoverage,
            RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION, ResearchReadinessEvidenceId,
            ResearchReadinessEvidencePayload, ResearchSourceStorageKind, RetentionRunwayEvidenceV1,
            RetentionSourceObservationV1, ShadowLatencyProfileV1, research_source_registry,
        },
    };

    use super::{
        EvidenceAttestor, QuantError, ReadinessCaptureClock, ReadinessCapturePhase, ResearchError,
        ResearchReadinessCapture, StdError, attestation_input, methodology,
        pg_retention_observation, verify_kind_binding,
    };

    #[test]
    fn postgres_bindings_resolve() {
        let observed_at = Utc
            .timestamp_opt(1_720_000_000, 0)
            .single()
            .expect("valid timestamp");
        let coverage = |object: &str, time_column: &str| HistoryCoverage {
            object: object.to_owned(),
            time_column: time_column.to_owned(),
            earliest_event_time: Some(observed_at - Duration::days(30)),
            latest_event_time: Some(observed_at),
            row_count: 1,
        };
        let catalog = vec![
            coverage("catalog_event_change", "source_effective_at"),
            coverage("catalog_market_change", "source_effective_at"),
        ];
        let clob = vec![coverage("clob_market_info_version", "effective_at")];
        let registry = research_source_registry().expect("canonical research source registry");

        for binding in registry
            .bindings
            .iter()
            .filter(|binding| binding.storage != ResearchSourceStorageKind::ClickHouseTable)
        {
            let source = match binding.storage {
                ResearchSourceStorageKind::PostgresLedger => &catalog,
                ResearchSourceStorageKind::PostgresVersionedProjection => &clob,
                ResearchSourceStorageKind::ClickHouseTable => continue,
            };
            assert!(
                pg_retention_observation(binding, source).is_ok(),
                "PostgreSQL readiness binding did not resolve: {binding:?}"
            );
        }
    }

    #[test]
    fn capture_counts_bindings() {
        let observed_at = Utc
            .timestamp_opt(1_720_000_000, 0)
            .single()
            .expect("valid timestamp");
        let registry = research_source_registry().expect("canonical research source registry");
        let observations = registry
            .bindings
            .iter()
            .map(|binding| {
                let clickhouse = binding.storage == ResearchSourceStorageKind::ClickHouseTable;
                RetentionSourceObservationV1 {
                    source: binding.source,
                    storage: binding.storage,
                    object: binding.object.clone(),
                    time_column: binding.time_column.clone(),
                    time_encoding: binding.time_encoding,
                    earliest_event_time: Some(observed_at - Duration::days(30)),
                    latest_event_time: Some(observed_at),
                    row_count: 1,
                    active_bytes: clickhouse.then_some(1),
                    active_partition_count: clickhouse.then_some(1),
                    partition_key: binding.partition_key.clone(),
                    table_ttl_expression: None,
                }
            })
            .collect();
        let payload = RetentionRunwayEvidenceV1 {
            format_version: RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION,
            registry_hash: registry.contract_hash().expect("registry hash"),
            required_sources: registry.required_sources,
            observed_at,
            required_days: 30,
            measured_history_days: Some(30),
            active_raw_bytes: 1,
            observations,
        };
        assert!(payload.proven());
        assert_eq!(ResearchReadinessCapture::binding_counts(&payload), (0, 0));

        let mut missing = payload.clone();
        missing.measured_history_days = None;
        missing.observations[0].row_count = 0;
        assert_eq!(ResearchReadinessCapture::binding_counts(&missing), (1, 0));

        let mut unready = payload;
        unready.measured_history_days = Some(1);
        unready.observations[0].earliest_event_time = Some(observed_at - Duration::days(1));
        assert_eq!(ResearchReadinessCapture::binding_counts(&unready), (0, 1));
    }

    #[test]
    fn capture_preserves_source_variants() {
        let storage = ReadinessCapturePhase::Measure.contextualize(
            ResearchReadinessEvidenceKind::RetentionRunway,
            StorageError::invariant_violation(
                Some("quant_research_readiness_evidence"),
                "coverage unavailable",
            )
            .into(),
        );
        assert_eq!(storage.phase, ReadinessCapturePhase::Measure);
        assert_eq!(storage.kind, ResearchReadinessEvidenceKind::RetentionRunway);
        assert_eq!(storage.source.code(), "storage");
        assert!(matches!(storage.source.as_ref(), QuantError::Storage(_)));

        let artifact = ReadinessCapturePhase::Persist.contextualize(
            ResearchReadinessEvidenceKind::ShadowLatencyProfile,
            ResearchError::ArtifactTransport {
                uri: "s3://readiness/evidence".to_owned(),
                detail: "transport unavailable".to_owned(),
            }
            .into(),
        );
        assert!(matches!(
            artifact.source.as_ref(),
            QuantError::Research(ResearchError::ArtifactTransport { .. })
        ));

        let research = ReadinessCapturePhase::Assemble.contextualize(
            ResearchReadinessEvidenceKind::RetentionRunway,
            methodology("payload mismatch"),
        );
        assert!(matches!(
            research.source.as_ref(),
            QuantError::Research(ResearchError::ValidationMethodology { .. })
        ));
        assert_eq!(
            StdError::source(&artifact).map(ToString::to_string),
            Some(artifact.source.to_string())
        );
        assert!(artifact.to_string().contains("phase=persist"));
        assert!(artifact.to_string().contains("kind=shadow_latency_profile"));
    }

    #[test]
    fn capture_clock_roundtrips() {
        let raw = Utc
            .timestamp_opt(1_720_000_000, 123_456_789)
            .single()
            .expect("valid sub-microsecond capture clock");
        let ReadinessCaptureClock(observed_at) = ReadinessCaptureClock::from(raw);
        assert_eq!(
            raw.signed_duration_since(observed_at).num_nanoseconds(),
            Some(789)
        );
        assert!(observed_at <= raw);
        assert_eq!(observed_at.timestamp_subsec_nanos() % 1_000, 0);

        let window_start = observed_at - Duration::hours(24);
        let expires_at = observed_at + Duration::hours(6);
        let payload_json =
            ResearchReadinessEvidencePayload::ShadowLatencyProfile(ShadowLatencyProfileV1 {
                format_version: 1,
                window_start,
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
        let payload_hash = CanonicalDigest::content_hash_json(&payload_json)
            .expect("canonical readiness payload hash");
        let attestor = EvidenceAttestor::from_config(&EvidenceAttestationConfig {
            signing_key: "ab".repeat(32).into(),
            previous_signing_keys: Vec::new(),
        })
        .expect("valid attestor config")
        .expect("configured attestor");
        let mut info = ResearchReadinessEvidenceInfo {
            evidence_id: ResearchReadinessEvidenceId::from_v7(),
            kind: ResearchReadinessEvidenceKind::ShadowLatencyProfile,
            scope_hash: ContentHash::from_bytes([1; 32]),
            window_start,
            window_end: observed_at,
            observed_at,
            expires_at,
            payload_json,
            payload_hash,
            artifact_uri: ArtifactUri::parse(
                "s3://readiness/evidence.json?versionId=fixture-version",
            )
            .expect("valid readiness artifact URI"),
            artifact_version: ArtifactVersion::parse("fixture-version")
                .expect("valid artifact version"),
            attestation_key_id: attestor.active_key_id.clone(),
            attestation_mac: ContentHash::from_bytes([0; 32]),
            created_at: observed_at,
        };
        info.attestation_mac = attestor
            .mac(&info.attestation_key_id, &attestation_input(&info))
            .expect("canonical readiness attestation");

        let bytes = serde_json::to_vec(&info).expect("serialize readiness evidence info");
        let roundtrip: ResearchReadinessEvidenceInfo =
            serde_json::from_slice(&bytes).expect("deserialize readiness evidence info");
        assert_eq!(
            serde_json::to_value(&info).expect("serialize original readiness info"),
            serde_json::to_value(&roundtrip).expect("serialize round-trip readiness info")
        );
        assert_eq!(
            CanonicalDigest::content_hash_json(&roundtrip.payload_json)
                .expect("round-trip readiness payload hash"),
            roundtrip.payload_hash
        );
        for persisted_at in [
            roundtrip.window_start,
            roundtrip.window_end,
            roundtrip.observed_at,
            roundtrip.expires_at,
            roundtrip.created_at,
        ] {
            assert_eq!(persisted_at.timestamp_subsec_nanos() % 1_000, 0);
            assert_eq!(
                DateTime::from_timestamp_micros(persisted_at.timestamp_micros()),
                Some(persisted_at)
            );
        }
        assert_eq!(
            attestor
                .mac(
                    &roundtrip.attestation_key_id,
                    &attestation_input(&roundtrip),
                )
                .expect("round-trip readiness attestation"),
            roundtrip.attestation_mac
        );
        let ResearchReadinessEvidencePayload::ShadowLatencyProfile(payload) =
            &roundtrip.payload_json
        else {
            panic!("round-trip readiness payload changed kind");
        };
        assert_eq!(payload.observed_at, roundtrip.observed_at);
    }

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
