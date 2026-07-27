//! Canonical verification of frozen trade-policy evidence bundles.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use blake3::Hasher;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    config::WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS,
    enums::quant::TradePolicyTrialStatus,
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, ContentHash, ModelVersionId, POLICY_EVIDENCE_OBJECT_FORMAT_VERSION,
        ResearchEvaluationTrack, ResearchProfileRef, ResearchReadinessEvidencePayload,
        ShadowLatencyProfileV1, StructuralVolatilityOosFoldRow, TradePolicyArtifactPayload,
        TradePolicyCandidateTrialRow, TradePolicyCohortTrialRow, TradePolicyCoverageGapRow,
        TradePolicyCpcvPathRow, TradePolicyEvidenceBundleManifest, TradePolicyEvidenceObjectKind,
        TradePolicyFillEvidenceRow, TradePolicyObservationEligibilityRow,
        TradePolicyStatisticalSummaryRow, VerticalGateEvidence,
    },
};
use quant_pivot_repository::traits::TradePolicyRepository;
use quant_pivot_research::{
    artifact::ArtifactStore,
    execution_semantics::EXECUTION_SEMANTICS_VERSION,
    hashing::ResearchHasher,
    policy_evidence::{PolicyEvidenceParquetCodec, PolicyEvidenceRecord},
    policy_replay::POLICY_REPLAY_KERNEL_VERSION,
};

use crate::service::{
    research_readiness::ResearchReadinessEvidenceService,
    trade_policy_replay::WEATHER_REPLAY_ORCHESTRATOR_VERSION,
};

/// Persistence dependencies required to verify one frozen evidence graph.
pub struct TradePolicyEvidenceVerifierDeps {
    pub artifacts: Arc<dyn ArtifactStore>,
    pub policies: Arc<dyn TradePolicyRepository>,
    pub readiness: Arc<ResearchReadinessEvidenceService>,
}

/// Durability strength required while resolving an evidence graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradePolicyEvidenceDurability {
    /// Verify content and semantics without requiring production Object Lock.
    ContentVerified,
    /// Require every manifest and Parquet object to be production-durable.
    Production,
}

impl TradePolicyEvidenceDurability {
    const fn requires_production(self) -> bool {
        matches!(self, Self::Production)
    }
}

/// A complete evidence graph whose manifest, trial ledger, latency evidence,
/// typed Parquet objects, and execution semantics have been verified.
pub struct VerifiedTradePolicyEvidence {
    manifest: TradePolicyEvidenceBundleManifest,
    records: BTreeMap<TradePolicyEvidenceObjectKind, Vec<PolicyEvidenceRecord>>,
    latency_profile: ShadowLatencyProfileV1,
}

impl VerifiedTradePolicyEvidence {
    #[must_use]
    pub const fn manifest(&self) -> &TradePolicyEvidenceBundleManifest {
        &self.manifest
    }

    #[must_use]
    pub const fn records(
        &self,
    ) -> &BTreeMap<TradePolicyEvidenceObjectKind, Vec<PolicyEvidenceRecord>> {
        &self.records
    }

    #[must_use]
    pub const fn records_mut(
        &mut self,
    ) -> &mut BTreeMap<TradePolicyEvidenceObjectKind, Vec<PolicyEvidenceRecord>> {
        &mut self.records
    }

    #[must_use]
    pub const fn latency_profile(&self) -> &ShadowLatencyProfileV1 {
        &self.latency_profile
    }
}

/// Side-effect-free resolver for every opaque preimage in a trade-policy
/// evidence bundle.
pub struct TradePolicyEvidenceVerifier {
    artifacts: Arc<dyn ArtifactStore>,
    policies: Arc<dyn TradePolicyRepository>,
    readiness: Arc<ResearchReadinessEvidenceService>,
}

impl TradePolicyEvidenceVerifier {
    #[must_use]
    pub fn new(deps: TradePolicyEvidenceVerifierDeps) -> Self {
        Self {
            artifacts: deps.artifacts,
            policies: deps.policies,
            readiness: deps.readiness,
        }
    }

    /// Hash the exact execution and replay implementation active in this
    /// process.
    pub fn active_simulator_hash() -> QuantResult<ContentHash> {
        CanonicalDigest::content_hash_json(&(
            EXECUTION_SEMANTICS_VERSION,
            POLICY_REPLAY_KERNEL_VERSION,
            WEATHER_REPLAY_ORCHESTRATOR_VERSION,
            WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS,
            POLICY_EVIDENCE_OBJECT_FORMAT_VERSION,
        ))
        .map_err(Into::into)
    }

    /// Hash the exact replay-kernel version bound by every evidence manifest.
    pub fn active_replay_hash() -> QuantResult<ContentHash> {
        CanonicalDigest::content_hash_json(&POLICY_REPLAY_KERNEL_VERSION).map_err(Into::into)
    }

    /// Recompute the frozen experiment family bound by a complete policy
    /// payload and its append-only trial ledger.
    pub fn experiment_family_hash(
        payload: &TradePolicyArtifactPayload,
    ) -> QuantResult<ContentHash> {
        weather_experiment_family_hash(WeatherExperimentFamilyInput {
            profile_ref: &payload.fit_contract.profile_ref,
            evaluation_track: payload.fit_contract.evaluation_track,
            research_program_hash: &payload.fit_contract.research_program_hash,
            model_version_id: &payload.fit_contract.model_version_id,
            methodology_hash: &payload.fit_contract.methodology_hash,
            latency_profile_hash: &payload.fit_contract.latency_profile_hash,
            candidate_set_hash: &payload.candidate_set_hash,
            fit_window_start: payload.fit_contract.fit_window_start,
            fit_window_end: payload.fit_contract.fit_window_end,
        })
    }

    /// Verify the complete immutable evidence graph bound by `payload`.
    pub async fn verify(
        &self,
        payload: &TradePolicyArtifactPayload,
        durability: TradePolicyEvidenceDurability,
    ) -> QuantResult<VerifiedTradePolicyEvidence> {
        let bundle = payload.evidence_bundle.as_ref().ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: "trade policy has no evidence bundle".to_owned(),
            }
        })?;
        let manifest_bytes = self
            .read_manifest(&bundle.manifest_uri, &bundle.manifest_hash)
            .await?;
        let manifest = serde_json::from_slice::<TradePolicyEvidenceBundleManifest>(&manifest_bytes)
            .map_err(|error| ResearchError::Serialization {
                detail: format!("invalid trade-policy evidence manifest: {error}"),
            })?;
        manifest
            .validate()
            .map_err(|detail| QuantError::from(ResearchError::ValidationMethodology { detail }))?;
        verify_evidence_identity(payload, &manifest)?;

        let expected_simulator_hash = Self::active_simulator_hash()?;
        let expected_replay_kernel_hash = Self::active_replay_hash()?;
        if payload.fill_simulator_version != EXECUTION_SEMANTICS_VERSION
            || manifest.simulator_hash != expected_simulator_hash
            || manifest.replay_kernel_hash != expected_replay_kernel_hash
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "policy evidence was not produced by the active execution/replay semantics"
                    .to_owned(),
            }
            .into());
        }

        let latency_evidence = self
            .readiness
            .verified_by_id(&manifest.latency_evidence_id)
            .await?;
        if latency_evidence.payload_hash != manifest.latency_profile_hash {
            return Err(ResearchError::ValidationMethodology {
                detail: "signed latency evidence hash differs from the frozen manifest".to_owned(),
            }
            .into());
        }
        let ResearchReadinessEvidencePayload::ShadowLatencyProfile(latency_profile) =
            latency_evidence.payload_json
        else {
            return Err(ResearchError::ValidationMethodology {
                detail: "frozen latency evidence has the wrong typed payload".to_owned(),
            }
            .into());
        };

        self.verify_trial_ledger(payload, &manifest).await?;
        let records = self.read_evidence_objects(&manifest, durability).await?;
        if durability.requires_production() {
            self.require_durable(&bundle.manifest_uri).await?;
        }
        Ok(VerifiedTradePolicyEvidence {
            manifest,
            records,
            latency_profile,
        })
    }

    async fn verify_trial_ledger(
        &self,
        payload: &TradePolicyArtifactPayload,
        manifest: &TradePolicyEvidenceBundleManifest,
    ) -> QuantResult<()> {
        let trial_ledger = self
            .policies
            .list_trial_attempts(&manifest.fit_job_id, Some(manifest.trial_ledger_cutoff))
            .await?;
        let profile = payload
            .fit_contract
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(|detail| ResearchError::ValidationMethodology { detail })?;
        let expected_experiment_family =
            weather_experiment_family_hash(WeatherExperimentFamilyInput {
                profile_ref: &profile.profile_ref,
                evaluation_track: payload.fit_contract.evaluation_track,
                research_program_hash: &payload.fit_contract.research_program_hash,
                model_version_id: &payload.fit_contract.model_version_id,
                methodology_hash: &payload.fit_contract.methodology_hash,
                latency_profile_hash: &payload.fit_contract.latency_profile_hash,
                candidate_set_hash: &payload.candidate_set_hash,
                fit_window_start: payload.fit_contract.fit_window_start,
                fit_window_end: payload.fit_contract.fit_window_end,
            })?;
        let candidate_hashes = payload
            .candidates
            .iter()
            .map(|candidate| {
                Ok((
                    candidate.candidate_id.as_str(),
                    CanonicalDigest::content_hash_json(candidate)?,
                ))
            })
            .collect::<QuantResult<HashMap<_, _>>>()?;
        if trial_ledger.is_empty()
            || trial_ledger.last().map(|attempt| attempt.created_at)
                != Some(manifest.trial_ledger_cutoff)
            || trial_ledger.iter().enumerate().any(|(ordinal, attempt)| {
                let ordinal_matches =
                    i64::try_from(ordinal).is_ok_and(|ordinal| ordinal == attempt.attempt_ordinal);
                let candidate_matches = candidate_hashes
                    .get(attempt.candidate_id.as_str())
                    .is_some_and(|hash| hash == &attempt.candidate_hash);
                let evidence_matches = match (
                    &attempt.evidence_uri,
                    &attempt.evidence_hash,
                    attempt.evidence_row_count,
                ) {
                    (Some(uri), Some(hash), Some(row_count)) => {
                        manifest.objects.iter().any(|object| {
                            &object.uri == uri
                                && &object.byte_hash == hash
                                && i64::try_from(object.row_count) == Ok(row_count)
                        })
                    }
                    _ => false,
                };
                !ordinal_matches
                    || !candidate_matches
                    || attempt.status != TradePolicyTrialStatus::Succeeded
                    || attempt.experiment_family_hash != expected_experiment_family
                    || attempt.research_program_hash != payload.fit_contract.research_program_hash
                    || !evidence_matches
                    || (attempt.expected_row_hash() != Ok(attempt.row_hash))
            })
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "frozen trade-policy trial ledger is empty, truncated, or has an invalid row hash"
                    .to_owned(),
            }
            .into());
        }
        let actual_trial_ledger_hash = ResearchHasher::canonical(&(
            "trade_policy_trial_ledger_v1",
            &manifest.fit_job_id,
            trial_ledger
                .iter()
                .map(|attempt| (attempt.attempt_ordinal, &attempt.row_hash))
                .collect::<Vec<_>>(),
        ))?;
        if actual_trial_ledger_hash != manifest.trial_ledger_hash {
            return Err(ResearchError::ValidationMethodology {
                detail: "frozen trade-policy trial ledger hash differs from the evidence manifest"
                    .to_owned(),
            }
            .into());
        }
        Ok(())
    }

    async fn read_evidence_objects(
        &self,
        manifest: &TradePolicyEvidenceBundleManifest,
        durability: TradePolicyEvidenceDurability,
    ) -> QuantResult<BTreeMap<TradePolicyEvidenceObjectKind, Vec<PolicyEvidenceRecord>>> {
        let mut records = BTreeMap::new();
        for object in &manifest.objects {
            let bytes = self.artifacts.get(&object.uri).await?;
            let actual_hash = CanonicalDigest::content_hash_bytes(&bytes);
            if actual_hash != object.byte_hash {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "policy evidence object {:?} byte hash mismatch",
                        object.kind
                    ),
                }
                .into());
            }
            let decoded = PolicyEvidenceParquetCodec::decode(&bytes)?;
            let row_count = u64::try_from(decoded.len()).map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!("policy evidence row count does not fit u64: {error}"),
                }
            })?;
            if row_count != object.row_count
                || PolicyEvidenceParquetCodec::row_chain_hash(&decoded)? != object.row_chain_hash
            {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "policy evidence object {:?} row count or semantic chain mismatch",
                        object.kind
                    ),
                }
                .into());
            }
            validate_typed_evidence(object.kind, &decoded)?;
            if records.insert(object.kind, decoded).is_some() {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!("policy evidence object {:?} is duplicated", object.kind),
                }
                .into());
            }
            if durability.requires_production() {
                self.require_durable(&object.uri).await?;
            }
        }
        Ok(records)
    }

    pub(super) async fn read_manifest(
        &self,
        uri: &ArtifactUri,
        expected_hash: &ContentHash,
    ) -> QuantResult<Vec<u8>> {
        const MAX_MANIFEST_BYTES: usize = 1_048_576;

        let mut stream = self.artifacts.get_stream(uri).await?;
        let mut hasher = Hasher::new();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            let next_len = bytes.len().checked_add(chunk.len()).ok_or_else(|| {
                ResearchError::ValidationMethodology {
                    detail: "artifact manifest size overflow".to_owned(),
                }
            })?;
            if next_len > MAX_MANIFEST_BYTES {
                return Err(ResearchError::ValidationMethodology {
                    detail: "artifact manifest exceeds 1 MiB".to_owned(),
                }
                .into());
            }
            hasher.update(&chunk);
            bytes.extend_from_slice(&chunk);
        }
        let actual_hash = content_hash_from_hasher(&hasher);
        if &actual_hash != expected_hash {
            return Err(ResearchError::ValidationMethodology {
                detail: "artifact manifest byte hash mismatch".to_owned(),
            }
            .into());
        }
        Ok(bytes)
    }

    pub(super) async fn require_durable(&self, uri: &ArtifactUri) -> QuantResult<()> {
        if !self
            .artifacts
            .durability(uri)
            .await?
            .permits_production_publish()
        {
            return Err(ResearchError::ValidationMethodology {
                detail: format!(
                    "artifact {} is not backed by versioned Object-Lock storage",
                    uri.as_str()
                ),
            }
            .into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) struct WeatherExperimentFamilyInput<'a> {
    pub profile_ref: &'a ResearchProfileRef,
    pub evaluation_track: ResearchEvaluationTrack,
    pub research_program_hash: &'a ContentHash,
    pub model_version_id: &'a ModelVersionId,
    pub methodology_hash: &'a ContentHash,
    pub latency_profile_hash: &'a ContentHash,
    pub candidate_set_hash: &'a ContentHash,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
}

pub(super) fn weather_experiment_family_hash(
    input: WeatherExperimentFamilyInput<'_>,
) -> QuantResult<ContentHash> {
    CanonicalDigest::content_hash_json(&(
        "weather_policy_experiment_family_v1",
        input.profile_ref,
        input.evaluation_track,
        input.research_program_hash,
        input.model_version_id,
        input.methodology_hash,
        input.latency_profile_hash,
        input.candidate_set_hash,
        input.fit_window_start,
        input.fit_window_end,
    ))
    .map_err(Into::into)
}

pub(super) fn content_hash_from_hasher(hasher: &Hasher) -> ContentHash {
    ContentHash::from_bytes(*hasher.finalize().as_bytes())
}

fn verify_evidence_identity(
    payload: &TradePolicyArtifactPayload,
    manifest: &TradePolicyEvidenceBundleManifest,
) -> QuantResult<()> {
    let bundle =
        payload
            .evidence_bundle
            .as_ref()
            .ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "trade policy has no evidence bundle".to_owned(),
            })?;
    let identity_matches = manifest.source_dataset_hash == payload.source_dataset_hash
        && manifest.candidate_set_hash == payload.candidate_set_hash
        && manifest.simulator_hash == bundle.simulator_hash
        && manifest.replay_kernel_hash == bundle.replay_kernel_hash
        && manifest.methodology_hash == bundle.methodology_hash
        && manifest.latency_evidence_id == bundle.latency_evidence_id
        && manifest.latency_evidence_id == payload.fit_contract.latency_evidence_id
        && manifest.latency_profile_hash == bundle.latency_profile_hash
        && manifest.catalog_ledger_hash == bundle.catalog_ledger_hash
        && manifest.source_slice_manifest_hash == bundle.source_slice_manifest_hash
        && manifest.fit_job_id == bundle.fit_job_id
        && manifest.trial_ledger_hash == bundle.trial_ledger_hash
        && payload.validation.trial_ledger_hash.as_ref() == Some(&manifest.trial_ledger_hash)
        && payload.validation.trial_ledger_cutoff == Some(manifest.trial_ledger_cutoff);
    if !identity_matches {
        return Err(ResearchError::ValidationMethodology {
            detail: "policy evidence manifest identity does not match the frozen artifact"
                .to_owned(),
        }
        .into());
    }
    Ok(())
}

fn validate_typed_evidence(
    kind: TradePolicyEvidenceObjectKind,
    records: &[PolicyEvidenceRecord],
) -> QuantResult<()> {
    for record in records {
        match kind {
            TradePolicyEvidenceObjectKind::ObservationEligibility => {
                let _: TradePolicyObservationEligibilityRow = record.decode_typed()?;
            }
            TradePolicyEvidenceObjectKind::Fills => {
                let _: TradePolicyFillEvidenceRow = record.decode_typed()?;
            }
            TradePolicyEvidenceObjectKind::CandidateTrials => {
                let _: TradePolicyCandidateTrialRow = record.decode_typed()?;
            }
            TradePolicyEvidenceObjectKind::CohortTrials => {
                let _: TradePolicyCohortTrialRow = record.decode_typed()?;
            }
            TradePolicyEvidenceObjectKind::CpcvPaths => {
                let _: TradePolicyCpcvPathRow = record.decode_typed()?;
            }
            TradePolicyEvidenceObjectKind::CoverageGaps => {
                let _: TradePolicyCoverageGapRow = record.decode_typed()?;
            }
            TradePolicyEvidenceObjectKind::StatisticalSummaries => {
                let _: TradePolicyStatisticalSummaryRow = record.decode_typed()?;
            }
            TradePolicyEvidenceObjectKind::VerticalGates => {
                let _: VerticalGateEvidence = record.decode_typed()?;
            }
            TradePolicyEvidenceObjectKind::StructuralVolatilityOos => {
                let _: StructuralVolatilityOosFoldRow = record.decode_typed()?;
            }
        }
    }
    Ok(())
}
