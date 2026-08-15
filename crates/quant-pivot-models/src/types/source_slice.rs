//! Immutable, content-addressed point-in-time source-slice contract.

use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, CapabilityRegistryHashes, CatalogSyncBatchId, ContentHash,
        DATASET_ARTIFACT_FORMAT_VERSION, DecisionPolicySnapshotId, ReaderContractVersion,
        ResearchEvaluationTrack, ResearchInformationRegime, ResearchProfileArtifact,
        ResearchProfileDataSource, ResearchProfileRef, SchemaContractVersion,
    },
};

pub const SOURCE_SLICE_MANIFEST_FORMAT_VERSION: u32 = 4;

/// Immutable artifact-store location and content identity of one source slice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSliceManifestRef {
    pub manifest_uri: ArtifactUri,
    pub manifest_hash: ContentHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceSliceObjectKind {
    GammaMarketIdentity,
    CatalogMarket,
    CatalogEvent,
    ClobMarketInfo,
    L2Ledger,
    L2Session,
    L2Gap,
    BookMicrostructure,
    MarketExecution,
    ExecutionParticipant,
    MarketLinkage,
    DomainObservation,
    CryptoPriceReport,
    WeatherObservation,
    WeatherForecast,
    CalibrationReference,
    Resolution,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSliceObjectRef {
    pub kind: SourceSliceObjectKind,
    pub uri: ArtifactUri,
    pub object_version: String,
    pub byte_hash: ContentHash,
    pub schema_hash: ContentHash,
    pub row_count: u64,
    pub min_event_at: Option<DateTime<Utc>>,
    pub max_event_at: Option<DateTime<Utc>>,
    pub min_available_at: Option<DateTime<Utc>>,
    pub max_available_at: Option<DateTime<Utc>>,
}

/// Catalog-ledger proof frozen into a source slice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSliceCatalogProof {
    pub base_complete_batch_id: CatalogSyncBatchId,
    pub terminal_batch_id: CatalogSyncBatchId,
    pub committed_through: DateTime<Utc>,
    pub ordered_batch_chain_hash: ContentHash,
    pub market_count: u64,
    pub event_count: u64,
    pub snapshot_hash: ContentHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceSliceSessionInvalidationReason {
    MissingSnapshot,
    SequenceGap,
    Reset,
    TickSizeChanged,
    NumericDecodeFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSliceInvalidSession {
    pub token_id: String,
    pub session_id: String,
    pub invalidated_at: DateTime<Utc>,
    pub first_failure_sequence: Option<u64>,
    pub reason: SourceSliceSessionInvalidationReason,
    pub diagnostic_hash: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSlicePitCutoff {
    pub source: ResearchProfileDataSource,
    pub available_at: DateTime<Utc>,
}

/// The only readable input to fitting and validation after materialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct SourceSliceManifest {
    pub format_version: u32,
    pub profile_ref: ResearchProfileRef,
    pub evaluation_track: ResearchEvaluationTrack,
    pub research_program_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub pit_cutoff: DateTime<Utc>,
    pub materialized_at: DateTime<Utc>,
    pub catalog_proof: SourceSliceCatalogProof,
    pub reader_contract_version: ReaderContractVersion,
    pub schema_contract_version: SchemaContractVersion,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub runtime_config_hash: ContentHash,
    pub dataset_format_version: u32,
    pub capability_registry_hashes: CapabilityRegistryHashes,
    pub pit_cutoffs: Vec<SourceSlicePitCutoff>,
    pub invalid_sessions: Vec<SourceSliceInvalidSession>,
    pub objects: Vec<SourceSliceObjectRef>,
}

impl SourceSliceManifest {
    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != SOURCE_SLICE_MANIFEST_FORMAT_VERSION {
            return Err(format!(
                "unsupported source-slice manifest format {}",
                self.format_version
            ));
        }
        if self.window_start >= self.window_end
            || self.pit_cutoff > self.materialized_at
            || self.catalog_proof.committed_through > self.pit_cutoff
        {
            return Err("source-slice time boundaries are invalid".to_owned());
        }
        if self.catalog_proof.market_count == 0 || self.catalog_proof.event_count == 0 {
            return Err("source-slice catalog proof must contain markets and events".to_owned());
        }
        if self
            .pit_cutoffs
            .iter()
            .any(|cutoff| cutoff.available_at > self.materialized_at)
        {
            return Err(
                "source-slice availability cutoffs cannot be after materialization".to_owned(),
            );
        }
        if self
            .invalid_sessions
            .windows(2)
            .any(|sessions| sessions[0].invalid_session_key() >= sessions[1].invalid_session_key())
        {
            return Err(
                "source-slice invalid sessions must be unique and canonically sorted".to_owned(),
            );
        }
        let mut uris = BTreeSet::new();
        let mut object_kinds = BTreeSet::new();
        let mut prior = None;
        for object in &self.objects {
            if !uris.insert(object.uri.as_str()) {
                return Err("source-slice object URI must be unique".to_owned());
            }
            if !object_kinds.insert(object.kind) {
                return Err(
                    "source-slice must contain exactly one object per object family".to_owned(),
                );
            }
            let key = (object.kind, object.uri.as_str());
            if prior.is_some_and(|value| value >= key) {
                return Err("source-slice objects must be canonically sorted".to_owned());
            }
            if object.object_version.trim().is_empty() {
                return Err(format!(
                    "source-slice object {:?} has no immutable object version",
                    object.kind
                ));
            }
            prior = Some(key);
            if object
                .min_event_at
                .zip(object.max_event_at)
                .is_some_and(|(min_at, max_at)| min_at > max_at)
            {
                return Err(format!(
                    "source-slice object {:?} has an invalid time range",
                    object.kind
                ));
            }
            if object
                .min_available_at
                .zip(object.max_available_at)
                .is_some_and(|(min_at, max_at)| min_at > max_at)
            {
                return Err(format!(
                    "source-slice object {:?} has an invalid availability range",
                    object.kind
                ));
            }
            if object
                .max_available_at
                .is_some_and(|available_at| available_at > self.pit_cutoff)
            {
                return Err(format!(
                    "source-slice object {:?} contains facts after the PIT cutoff",
                    object.kind
                ));
            }
        }
        if self.objects.is_empty() {
            return Err("source-slice contains no objects".to_owned());
        }
        if self
            .pit_cutoffs
            .windows(2)
            .any(|pair| pair[0].source >= pair[1].source)
        {
            return Err(
                "source-slice PIT cutoffs must be unique and canonically sorted".to_owned(),
            );
        }
        Ok(())
    }

    pub fn content_hash(&self) -> Result<ContentHash, String> {
        self.validate()?;
        CanonicalDigest::content_hash_json(self)
            .map_err(|error| format!("source-slice manifest hash failed: {error}"))
    }

    /// Exact object-family contract consumed by one profile's source reader.
    #[must_use]
    pub fn required_object_kinds(
        profile: &ResearchProfileArtifact,
    ) -> BTreeSet<SourceSliceObjectKind> {
        let mut required = BTreeSet::new();
        if profile.required_sources_contains(ResearchProfileDataSource::GammaMarketIdentity) {
            required.insert(SourceSliceObjectKind::GammaMarketIdentity);
        }
        if profile.required_sources_contains(ResearchProfileDataSource::CatalogLedger) {
            required.extend([
                SourceSliceObjectKind::CatalogMarket,
                SourceSliceObjectKind::CatalogEvent,
            ]);
        }
        if profile.required_sources_contains(ResearchProfileDataSource::ClobMarketInfo) {
            required.insert(SourceSliceObjectKind::ClobMarketInfo);
        }
        if profile.required_sources_contains(ResearchProfileDataSource::ClobL2) {
            required.extend([
                SourceSliceObjectKind::L2Ledger,
                SourceSliceObjectKind::L2Session,
                SourceSliceObjectKind::L2Gap,
                SourceSliceObjectKind::BookMicrostructure,
            ]);
        }
        if profile.required_sources_contains(ResearchProfileDataSource::MarketExecution) {
            required.insert(SourceSliceObjectKind::MarketExecution);
        }
        if profile.required_sources_contains(ResearchProfileDataSource::ExecutionParticipant) {
            required.insert(SourceSliceObjectKind::ExecutionParticipant);
        }
        if profile.required_sources_contains(ResearchProfileDataSource::PolymarketResolution) {
            required.insert(SourceSliceObjectKind::Resolution);
        }
        let crypto_required = profile
            .required_sources_contains(ResearchProfileDataSource::BinanceMarketData)
            || profile.required_sources_contains(ResearchProfileDataSource::PolymarketRtds);
        let weather_required = profile
            .required_sources_contains(ResearchProfileDataSource::AviationWeather)
            || profile.required_sources_contains(ResearchProfileDataSource::GefsEnsemble);
        if crypto_required || weather_required {
            required.insert(SourceSliceObjectKind::MarketLinkage);
            required.insert(SourceSliceObjectKind::DomainObservation);
        }
        if crypto_required {
            required.insert(SourceSliceObjectKind::CryptoPriceReport);
        }
        if weather_required {
            required.insert(SourceSliceObjectKind::WeatherObservation);
            required.insert(SourceSliceObjectKind::WeatherForecast);
        }
        if profile.required_sources_contains(ResearchProfileDataSource::GhcnhCalibration) {
            required.insert(SourceSliceObjectKind::CalibrationReference);
        }
        required
    }

    /// Validate the complete immutable binding before a dataset, fitter, or
    /// validator consumes any object from this slice.
    pub fn validate_for_profile(
        &self,
        profile: &ResearchProfileArtifact,
        research_program_hash: &ContentHash,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        pit_cutoff: DateTime<Utc>,
    ) -> Result<BTreeSet<SourceSliceObjectKind>, String> {
        self.validate()?;
        if self.profile_ref != profile.profile_ref
            || &self.research_program_hash != research_program_hash
        {
            return Err("source-slice profile or research-program binding mismatch".to_owned());
        }
        if !profile.spec.permits(self.evaluation_track) {
            return Err("source-slice evaluation track is not permitted by the profile".to_owned());
        }
        if self.dataset_format_version != DATASET_ARTIFACT_FORMAT_VERSION {
            return Err(format!(
                "source-slice requires dataset v{DATASET_ARTIFACT_FORMAT_VERSION}"
            ));
        }
        if profile.spec.information_regime != ResearchInformationRegime::PooledBinaryMarket
            && self.capability_registry_hashes.as_slice().is_empty()
        {
            return Err(
                "vertical source-slice must bind at least one capability-registry hash".to_owned(),
            );
        }
        if self.pit_cutoff != pit_cutoff {
            return Err("source-slice PIT-cutoff binding mismatch".to_owned());
        }
        let horizon = i64::try_from(profile.spec.target_horizon_secs)
            .map_err(|error| format!("profile horizon does not fit chrono seconds: {error}"))?;
        let required_end = window_end
            .checked_add_signed(Duration::seconds(horizon))
            .ok_or_else(|| "source-slice required horizon overflows chrono".to_owned())?;
        if self.window_start > window_start || self.window_end < required_end {
            return Err(format!(
                "source-slice [{}, {}) does not contain sample window [{window_start}, {window_end}) plus {}s target horizon",
                self.window_start, self.window_end, profile.spec.target_horizon_secs
            ));
        }
        if self
            .pit_cutoffs
            .iter()
            .any(|cutoff| cutoff.available_at > pit_cutoff)
        {
            return Err("source-slice contains facts unavailable at the PIT cutoff".to_owned());
        }
        let actual_sources = self
            .pit_cutoffs
            .iter()
            .map(|cutoff| cutoff.source)
            .collect::<BTreeSet<_>>();
        let required_sources = profile.spec.required_sources().into_iter().collect();
        if actual_sources != required_sources {
            return Err("source-slice PIT source contract does not match the profile".to_owned());
        }

        let kinds = self
            .objects
            .iter()
            .map(|object| object.kind)
            .collect::<BTreeSet<_>>();
        let required = Self::required_object_kinds(profile);
        let missing = required.difference(&kinds).copied().collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(format!(
                "source-slice is missing required object families: {missing:?}"
            ));
        }
        let unexpected = kinds.difference(&required).copied().collect::<Vec<_>>();
        if !unexpected.is_empty() {
            return Err(format!(
                "source-slice contains object families outside the profile contract: {unexpected:?}"
            ));
        }
        Ok(kinds)
    }
}

impl SourceSliceInvalidSession {
    const fn invalid_session_key(&self) -> (&str, &str, DateTime<Utc>) {
        (
            self.token_id.as_str(),
            self.session_id.as_str(),
            self.invalidated_at,
        )
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;
    use crate::types::{ResearchProfileId, builtin_research_profiles};

    fn hash(index: u8) -> ContentHash {
        ContentHash::parse(&format!("blake3:{index:064x}")).expect("hash")
    }

    fn weather_manifest(
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        materialized_at: DateTime<Utc>,
    ) -> (SourceSliceManifest, ResearchProfileArtifact, ContentHash) {
        let profile = builtin_research_profiles()
            .expect("profiles")
            .into_iter()
            .find(|profile| profile.profile_ref.id.as_str() == "weather_forecast_24h")
            .expect("weather profile");
        let program_hash = hash(200);
        let kinds = SourceSliceManifest::required_object_kinds(&profile);
        let objects = kinds
            .into_iter()
            .enumerate()
            .map(|(index, kind)| SourceSliceObjectRef {
                kind,
                uri: ArtifactUri::parse(format!(
                    "s3://worm/source-slice/{index:02}.parquet?versionId=1"
                ))
                .expect("URI"),
                object_version: "1".to_owned(),
                byte_hash: hash(u8::try_from(index + 1).expect("hash index")),
                schema_hash: hash(100 + u8::try_from(index).expect("schema index")),
                row_count: 1,
                min_event_at: Some(window_start),
                max_event_at: Some(window_end),
                min_available_at: Some(window_start),
                max_available_at: Some(materialized_at),
            })
            .collect();
        let manifest = SourceSliceManifest {
            format_version: SOURCE_SLICE_MANIFEST_FORMAT_VERSION,
            profile_ref: profile.profile_ref.clone(),
            evaluation_track: ResearchEvaluationTrack::SemiAutoCandidate,
            research_program_hash: program_hash,
            window_start,
            window_end,
            pit_cutoff: materialized_at,
            materialized_at,
            catalog_proof: SourceSliceCatalogProof {
                base_complete_batch_id: CatalogSyncBatchId::from_v7(),
                terminal_batch_id: CatalogSyncBatchId::from_v7(),
                committed_through: materialized_at,
                ordered_batch_chain_hash: hash(201),
                market_count: 1,
                event_count: 1,
                snapshot_hash: hash(202),
            },
            reader_contract_version: ReaderContractVersion::parse("reader@2")
                .expect("reader contract version"),
            schema_contract_version: SchemaContractVersion::parse("source-slice@2")
                .expect("schema contract version"),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            runtime_config_hash: hash(203),
            dataset_format_version: DATASET_ARTIFACT_FORMAT_VERSION,
            capability_registry_hashes: CapabilityRegistryHashes::try_new(vec![hash(204)])
                .expect("capability registry hashes"),
            pit_cutoffs: profile
                .spec
                .required_sources()
                .into_iter()
                .map(|source| SourceSlicePitCutoff {
                    source,
                    available_at: materialized_at,
                })
                .collect(),
            invalid_sessions: Vec::new(),
            objects,
        };
        (manifest, profile, program_hash)
    }

    #[test]
    fn duplicate_object_uri_rejected() {
        let now = Utc::now();
        let hash = ContentHash::parse(&format!("blake3:{}", "1".repeat(64))).expect("hash");
        let object = SourceSliceObjectRef {
            kind: SourceSliceObjectKind::CatalogMarket,
            uri: ArtifactUri::parse("s3://worm/slice/catalog.parquet").expect("URI"),
            object_version: "1".to_owned(),
            byte_hash: hash,
            schema_hash: hash,
            row_count: 1,
            min_event_at: Some(now),
            max_event_at: Some(now),
            min_available_at: Some(now),
            max_available_at: Some(now),
        };
        let manifest = SourceSliceManifest {
            format_version: SOURCE_SLICE_MANIFEST_FORMAT_VERSION,
            profile_ref: ResearchProfileRef {
                id: ResearchProfileId::new("test"),
                version: 1,
                content_hash: hash,
            },
            evaluation_track: ResearchEvaluationTrack::SemiAutoCandidate,
            research_program_hash: hash,
            window_start: now - Duration::days(1),
            window_end: now,
            pit_cutoff: now,
            materialized_at: now,
            catalog_proof: SourceSliceCatalogProof {
                base_complete_batch_id: CatalogSyncBatchId::from_v7(),
                terminal_batch_id: CatalogSyncBatchId::from_v7(),
                committed_through: now,
                ordered_batch_chain_hash: hash,
                market_count: 1,
                event_count: 1,
                snapshot_hash: hash,
            },
            reader_contract_version: ReaderContractVersion::parse("reader@1")
                .expect("reader contract version"),
            schema_contract_version: SchemaContractVersion::parse("schema@1")
                .expect("schema contract version"),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            runtime_config_hash: hash,
            dataset_format_version: DATASET_ARTIFACT_FORMAT_VERSION,
            capability_registry_hashes: CapabilityRegistryHashes::default(),
            pit_cutoffs: vec![SourceSlicePitCutoff {
                source: ResearchProfileDataSource::GammaMarketIdentity,
                available_at: now,
            }],
            invalid_sessions: Vec::new(),
            objects: vec![object.clone(), object],
        };
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn weather_slice_requires_runway() {
        let pit_cutoff = Utc::now();
        let fit_start = pit_cutoff - Duration::days(91);
        let fit_end = pit_cutoff - Duration::days(1);
        let (manifest, profile, program_hash) = weather_manifest(fit_start, pit_cutoff, pit_cutoff);
        assert!(
            manifest
                .validate_for_profile(&profile, &program_hash, fit_start, fit_end, pit_cutoff,)
                .is_ok()
        );

        let (short, profile, program_hash) = weather_manifest(fit_start, fit_end, pit_cutoff);
        assert!(
            short
                .validate_for_profile(&profile, &program_hash, fit_start, fit_end, pit_cutoff,)
                .is_err()
        );
    }

    #[test]
    fn profile_object_contracts() {
        let profiles = builtin_research_profiles().expect("profiles");
        let pooled = profiles
            .iter()
            .find(|profile| profile.profile_ref.id.as_str() == "pooled_1h_control")
            .expect("pooled profile");
        let crypto = profiles
            .iter()
            .find(|profile| profile.profile_ref.id.as_str() == "crypto_price_15m")
            .expect("crypto profile");
        let weather = profiles
            .iter()
            .find(|profile| profile.profile_ref.id.as_str() == "weather_forecast_24h")
            .expect("weather profile");

        let pooled_kinds = SourceSliceManifest::required_object_kinds(pooled);
        let crypto_kinds = SourceSliceManifest::required_object_kinds(crypto);
        let weather_kinds = SourceSliceManifest::required_object_kinds(weather);
        assert!(pooled_kinds.contains(&SourceSliceObjectKind::MarketExecution));
        assert!(pooled_kinds.contains(&SourceSliceObjectKind::ExecutionParticipant));
        assert!(crypto_kinds.contains(&SourceSliceObjectKind::CryptoPriceReport));
        assert!(!crypto_kinds.contains(&SourceSliceObjectKind::WeatherForecast));
        assert!(weather_kinds.contains(&SourceSliceObjectKind::WeatherForecast));
        assert!(!weather_kinds.contains(&SourceSliceObjectKind::CryptoPriceReport));
    }

    #[test]
    fn weather_rejects_crypto_object() {
        let pit_cutoff = Utc::now();
        let fit_start = pit_cutoff - Duration::days(91);
        let fit_end = pit_cutoff - Duration::days(1);
        let (mut manifest, profile, program_hash) =
            weather_manifest(fit_start, pit_cutoff, pit_cutoff);
        let mut unexpected = manifest.objects[0].clone();
        unexpected.kind = SourceSliceObjectKind::CryptoPriceReport;
        unexpected.uri = ArtifactUri::parse("s3://worm/source-slice/crypto.parquet?versionId=1")
            .expect("unexpected object URI");
        manifest.objects.push(unexpected);
        manifest.objects.sort_by(|left, right| {
            (left.kind, left.uri.as_str()).cmp(&(right.kind, right.uri.as_str()))
        });

        assert!(
            manifest
                .validate_for_profile(&profile, &program_hash, fit_start, fit_end, pit_cutoff)
                .is_err()
        );
    }

    #[test]
    fn availability_after_materialization_rejected() {
        let pit_cutoff = Utc::now();
        let fit_start = pit_cutoff - Duration::days(91);
        let (mut manifest, _, _) = weather_manifest(fit_start, pit_cutoff, pit_cutoff);
        let weather = manifest
            .pit_cutoffs
            .iter_mut()
            .find(|cutoff| cutoff.source == ResearchProfileDataSource::GefsEnsemble)
            .expect("weather cutoff");
        weather.available_at = pit_cutoff + Duration::seconds(1);
        assert!(manifest.validate().is_err());
    }
}
