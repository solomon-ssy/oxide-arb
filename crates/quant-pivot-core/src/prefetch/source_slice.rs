//! Immutable Source Slice materialization over the durable PIT ledgers.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::Hash,
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, BookStreamSessionRow, ExecutionParticipantFactRow,
        ExecutionParticipantRow, MarketExecutionRow, MarketResolutionRow,
    },
    domain::{
        data_plane::{
            CryptoPriceReport, DecisionBoundary, DecisionClock, DomainObservation,
            WeatherForecastPoint, WeatherObservationFact,
        },
        market::{
            CatalogEventChangeInfo, CatalogMarketChangeInfo, CatalogWindowInfo, MarketRegistryInfo,
        },
        quant::{
            CompleteSourceSlice, MarketLinkage, NewSourceSlice, SourceSliceIdentity,
            SourceSliceInfo,
        },
    },
    enums::{
        clickhouse::{ChCanonicalBookEventType, ChExecutionParticipantRole},
        common::MarketCategory,
        domain::DomainFamily,
        quant::SourceSliceStatus,
    },
    hashing::CanonicalDigest,
    runtime_config::DomainConfig,
    types::{
        ArtifactUri, CapabilityRegistryHashes, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION,
        MarketId, ResearchProfileArtifact, SOURCE_SLICE_MANIFEST_FORMAT_VERSION,
        SourceSliceCatalogProof, SourceSliceId, SourceSliceInvalidSession, SourceSliceManifest,
        SourceSliceManifestRef, SourceSliceObjectKind, SourceSliceObjectRef, SourceSlicePitCutoff,
        SourceSliceSessionInvalidationReason, Usd,
    },
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, CatalogLedgerRepository, ClobMarketInfoRepository,
    ExchangeHistoryRepository, MarketLinkageRepository, QuantFactReadRepository,
    SourceSliceRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    source_slice::{SourceSliceParquetCodec, SourceSliceRecord},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::historical_window::{HistoricalWindowLoader, Prefetched, ReplaySample, WindowSpec};

/// Dependencies used by the single server-owned source materializer.
pub struct SourceSliceMaterializerDeps {
    pub facts: Arc<dyn QuantFactReadRepository>,
    pub catalog: Arc<dyn CatalogLedgerRepository>,
    pub clob_market_info: Arc<dyn ClobMarketInfoRepository>,
    pub linkage: Arc<dyn MarketLinkageRepository>,
    pub calibration: Arc<dyn CalibrationArtifactRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub ledger: Arc<dyn SourceSliceRepository>,
    pub exchange_history: Arc<dyn ExchangeHistoryRepository>,
}

/// Materializes raw, immutable facts before Dataset/Fit/Validate may run.
pub struct SourceSliceMaterializer {
    deps: SourceSliceMaterializerDeps,
    domain: DomainConfig,
    max_book_staleness: Duration,
}

/// Fully verified immutable inputs. Consumers receive owned in-memory facts and
/// cannot reach a live repository while replaying rows.
pub struct FrozenSourceSlice {
    pub manifest: SourceSliceManifest,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub pit_cutoff: DateTime<Utc>,
    pub prefetched: Prefetched,
    pub l2_ledger: Vec<BookL2LedgerRow>,
    pub sessions: Vec<BookStreamSessionRow>,
    pub invalid_sessions: Vec<SourceSliceInvalidSession>,
}

struct SourceSliceInputs {
    prefetched: Prefetched,
    l2_ledger: Vec<BookL2LedgerRow>,
    sessions: Vec<BookStreamSessionRow>,
    gap_records: Vec<SourceSliceRecord>,
    invalid_sessions: Vec<SourceSliceInvalidSession>,
    executions: Vec<MarketExecutionRow>,
    participants: Vec<ExecutionParticipantRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "identity_kind", rename_all = "snake_case")]
enum GammaIdentityRecord {
    Market(CatalogMarketChangeInfo),
    Event(CatalogEventChangeInfo),
}

impl GammaIdentityRecord {
    const fn event_at(&self) -> DateTime<Utc> {
        match self {
            Self::Market(row) => row.source_effective_at,
            Self::Event(row) => row.source_effective_at,
        }
    }

    const fn available_at(&self) -> DateTime<Utc> {
        match self {
            Self::Market(row) => row.available_at,
            Self::Event(row) => row.available_at,
        }
    }
}

impl SourceSliceInputs {
    fn capability_hashes(&self) -> QuantResult<CapabilityRegistryHashes> {
        let hashes = self
            .prefetched
            .linkages
            .values()
            .flatten()
            .filter_map(|linkage| linkage.capability_registry_hash)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        CapabilityRegistryHashes::try_new(hashes)
            .map_err(|error| ResearchError::DatasetBuild {
                detail: error.to_string(),
            })
            .map_err(Into::into)
    }
}

/// Strict reader for the current Parquet envelope and immutable object bindings.
pub struct SourceSliceReader {
    artifacts: Arc<dyn ArtifactStore>,
}

impl SourceSliceReader {
    #[must_use]
    pub const fn new(artifacts: Arc<dyn ArtifactStore>) -> Self {
        Self { artifacts }
    }

    pub async fn read(&self, source_slice: &SourceSliceInfo) -> QuantResult<FrozenSourceSlice> {
        let manifest = self.read_manifest(source_slice).await?;
        let records = self.read_objects(&manifest).await?;
        decode_frozen_source_slice(&manifest, records)
    }

    /// Independently verify and read a Dataset-bound Source Slice reference.
    ///
    /// Backtest and CPCV artifacts bind the immutable manifest URI/hash rather
    /// than a mutable ledger id. This path validates the exact manifest bytes,
    /// object versions, hashes, schema, row counts, and PIT cutoffs before
    /// exposing any L2 or fee fact to the pure replay engine.
    pub async fn read_ref(
        &self,
        source_slice: &SourceSliceManifestRef,
    ) -> QuantResult<FrozenSourceSlice> {
        let manifest = self.read_manifest_artifact(source_slice).await?;
        let records = self.read_objects(&manifest).await?;
        decode_frozen_source_slice(&manifest, records)
    }

    /// Verify the immutable manifest binding without materializing its fact
    /// objects. Serving cold loads use this boundary because inference does not
    /// consume historical Source Slice rows; research replay still calls
    /// [`Self::read_ref`] and verifies every object byte and decoded row.
    pub async fn verify_manifest_ref(
        &self,
        source_slice: &SourceSliceManifestRef,
    ) -> QuantResult<SourceSliceManifest> {
        self.read_manifest_artifact(source_slice).await
    }

    async fn read_manifest(
        &self,
        source_slice: &SourceSliceInfo,
    ) -> QuantResult<SourceSliceManifest> {
        if source_slice.status != SourceSliceStatus::Ready {
            return Err(StorageError::state_conflict(
                "quant_source_slice",
                Some(&source_slice.source_slice_id),
                format!(
                    "Source Slice reader requires Ready, got {}",
                    source_slice.status
                ),
            )
            .into());
        }
        let manifest = source_slice.manifest.as_ref().ok_or_else(|| {
            StorageError::invariant_violation(
                Some("quant_source_slice"),
                "Ready Source Slice has no manifest JSON",
            )
        })?;
        manifest
            .validate()
            .map_err(|detail| ResearchError::DatasetBuild { detail })?;
        let manifest_uri = source_slice.manifest_uri.as_ref().ok_or_else(|| {
            StorageError::invariant_violation(
                Some("quant_source_slice"),
                "Ready Source Slice has no manifest URI",
            )
        })?;
        let manifest_hash = source_slice.manifest_hash.as_ref().ok_or_else(|| {
            StorageError::invariant_violation(
                Some("quant_source_slice"),
                "Ready Source Slice has no manifest hash",
            )
        })?;
        let manifest_ref = SourceSliceManifestRef {
            manifest_uri: manifest_uri.clone(),
            manifest_hash: *manifest_hash,
        };
        let decoded = self.read_manifest_artifact(&manifest_ref).await?;
        if decoded != *manifest {
            return Err(ResearchError::DatasetBuild {
                detail: "Source Slice manifest bytes do not match its frozen ledger binding"
                    .to_owned(),
            }
            .into());
        }

        Ok(decoded)
    }

    async fn read_manifest_artifact(
        &self,
        source_slice: &SourceSliceManifestRef,
    ) -> QuantResult<SourceSliceManifest> {
        let manifest_metadata = self.artifacts.metadata(&source_slice.manifest_uri).await?;
        if manifest_metadata.durability.remote
            && !manifest_metadata.durability.permits_production_publish()
        {
            return Err(ResearchError::DatasetBuild {
                detail: "Source Slice manifest is not backed by versioned Object-Lock storage"
                    .to_owned(),
            }
            .into());
        }
        let manifest_bytes = self.artifacts.get(&source_slice.manifest_uri).await?;
        let actual_manifest_hash = CanonicalDigest::content_hash_bytes(&manifest_bytes);
        if actual_manifest_hash != source_slice.manifest_hash {
            return Err(ResearchError::DatasetBuild {
                detail: "Source Slice manifest bytes do not match the Dataset binding".to_owned(),
            }
            .into());
        }
        let manifest =
            serde_json::from_slice::<SourceSliceManifest>(&manifest_bytes).map_err(|error| {
                ResearchError::DatasetBuild {
                    detail: format!("Source Slice manifest JSON is invalid: {error}"),
                }
            })?;
        manifest
            .validate()
            .map_err(|detail| ResearchError::DatasetBuild { detail })?;
        let semantic_hash = manifest
            .content_hash()
            .map_err(|detail| ResearchError::DatasetBuild { detail })?;
        if semantic_hash != source_slice.manifest_hash {
            return Err(ResearchError::DatasetBuild {
                detail: "Source Slice manifest semantic hash differs from its byte binding"
                    .to_owned(),
            }
            .into());
        }
        Ok(manifest)
    }

    async fn read_objects(
        &self,
        manifest: &SourceSliceManifest,
    ) -> QuantResult<SourceRecordsByKind> {
        let mut by_kind = SourceRecordsByKind::new();
        for object in &manifest.objects {
            let expected_schema = CanonicalDigest::content_hash_json(&(
                "source_slice_parquet_envelope_v2",
                object.kind,
            ))?;
            if object.schema_hash != expected_schema {
                return Err(ResearchError::DatasetBuild {
                    detail: format!("Source Slice object {:?} schema hash mismatch", object.kind),
                }
                .into());
            }
            let metadata = self.artifacts.metadata(&object.uri).await?;
            if metadata.durability.remote && !metadata.durability.permits_production_publish() {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "Source Slice object {} is not backed by versioned Object-Lock storage",
                        object.uri
                    ),
                }
                .into());
            }
            let observed_version = metadata
                .version_id
                .unwrap_or_else(|| format!("local-content:{}", object.byte_hash));
            if observed_version != object.object_version {
                return Err(ResearchError::DatasetBuild {
                    detail: format!("Source Slice object {} version changed", object.uri),
                }
                .into());
            }
            let bytes = self.artifacts.get(&object.uri).await?;
            let actual_hash = CanonicalDigest::content_hash_bytes(&bytes);
            if actual_hash != object.byte_hash {
                return Err(ResearchError::ArtifactHashMismatch {
                    expected: object.byte_hash.to_string(),
                    actual: actual_hash.to_string(),
                }
                .into());
            }
            let rows = SourceSliceParquetCodec::decode(&bytes)?;
            let actual_count =
                u64::try_from(rows.len()).map_err(|error| ResearchError::DatasetBuild {
                    detail: error.to_string(),
                })?;
            if actual_count != object.row_count
                || rows.iter().filter_map(|row| row.event_at).min() != object.min_event_at
                || rows.iter().filter_map(|row| row.event_at).max() != object.max_event_at
                || rows.iter().filter_map(|row| row.available_at).min() != object.min_available_at
                || rows.iter().filter_map(|row| row.available_at).max() != object.max_available_at
                || rows.iter().any(|row| {
                    row.available_at
                        .is_some_and(|available| available > manifest.pit_cutoff)
                })
            {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "Source Slice object {:?} row/time evidence does not verify",
                        object.kind
                    ),
                }
                .into());
            }
            by_kind.entry(object.kind).or_default().extend(rows);
        }
        Ok(by_kind)
    }
}

type SourceRecordsByKind = BTreeMap<SourceSliceObjectKind, Vec<SourceSliceRecord>>;

fn decode_frozen_source_slice(
    manifest: &SourceSliceManifest,
    mut by_kind: SourceRecordsByKind,
) -> QuantResult<FrozenSourceSlice> {
    let identities = decode_records::<GammaIdentityRecord>(take(
        &mut by_kind,
        SourceSliceObjectKind::GammaMarketIdentity,
    ))?;
    let mut catalog = CatalogWindowInfo {
        market_changes: decode_records(take(&mut by_kind, SourceSliceObjectKind::CatalogMarket))?,
        event_changes: decode_records(take(&mut by_kind, SourceSliceObjectKind::CatalogEvent))?,
    };
    for identity in identities {
        match identity {
            GammaIdentityRecord::Market(row) => catalog.market_changes.push(row),
            GammaIdentityRecord::Event(row) => catalog.event_changes.push(row),
        }
    }
    let l2_ledger =
        decode_records::<BookL2LedgerRow>(take(&mut by_kind, SourceSliceObjectKind::L2Ledger))?;
    let books = group_by(
        l2_ledger
            .iter()
            .filter(|row| row.event_type == ChCanonicalBookEventType::Snapshot)
            .cloned()
            .collect(),
        |row| row.token_id.clone(),
    );
    let micro = group_by(
        decode_records::<BookMicrostructureRow>(take(
            &mut by_kind,
            SourceSliceObjectKind::BookMicrostructure,
        ))?,
        |row| row.token_id.clone(),
    );
    let executions = decode_records::<MarketExecutionRow>(take(
        &mut by_kind,
        SourceSliceObjectKind::MarketExecution,
    ))?;
    let participants = decode_records::<ExecutionParticipantRow>(take(
        &mut by_kind,
        SourceSliceObjectKind::ExecutionParticipant,
    ))?;
    let finalized_executions = group_by(join_execution_facts(&executions, &participants)?, |row| {
        row.market_id.clone()
    });
    let resolutions = group_by(
        decode_records::<MarketResolutionRow>(take(
            &mut by_kind,
            SourceSliceObjectKind::Resolution,
        ))?,
        |row| row.market_id.clone(),
    );
    let linkages = group_by(
        decode_records::<MarketLinkage>(take(&mut by_kind, SourceSliceObjectKind::MarketLinkage))?,
        |row| row.market_id.clone(),
    );
    let domain_observations = group_by(
        decode_records::<DomainObservation>(take(
            &mut by_kind,
            SourceSliceObjectKind::DomainObservation,
        ))?,
        |row| row.instrument_key.clone(),
    );
    let crypto_reports = group_by(
        decode_records::<CryptoPriceReport>(take(
            &mut by_kind,
            SourceSliceObjectKind::CryptoPriceReport,
        ))?,
        |row| row.instrument_key.clone(),
    );
    let weather_observations =
        group_weather_observations(decode_records::<WeatherObservationFact>(take(
            &mut by_kind,
            SourceSliceObjectKind::WeatherObservation,
        ))?);
    let weather_forecasts = group_weather_forecasts(decode_records::<WeatherForecastPoint>(take(
        &mut by_kind,
        SourceSliceObjectKind::WeatherForecast,
    ))?);
    let weather_calibrations = decode_records(take(
        &mut by_kind,
        SourceSliceObjectKind::CalibrationReference,
    ))?;
    let clob_market_info =
        decode_records(take(&mut by_kind, SourceSliceObjectKind::ClobMarketInfo))?;
    let sessions = decode_records(take(&mut by_kind, SourceSliceObjectKind::L2Session))?;
    let _gap_rows = take(&mut by_kind, SourceSliceObjectKind::L2Gap);
    if by_kind.values().any(|rows| !rows.is_empty()) {
        return Err(ResearchError::DatasetBuild {
            detail: "Source Slice contains an unread object family".to_owned(),
        }
        .into());
    }
    Ok(FrozenSourceSlice {
        manifest: manifest.clone(),
        window_start: manifest.window_start,
        window_end: manifest.window_end,
        pit_cutoff: manifest.pit_cutoff,
        prefetched: Prefetched {
            books,
            micro,
            finalized_executions,
            resolutions,
            catalog,
            clob_market_info,
            domain_observations,
            crypto_reports,
            weather_observations,
            weather_forecasts,
            weather_calibrations,
            linkages,
        },
        l2_ledger,
        sessions,
        invalid_sessions: manifest.invalid_sessions.clone(),
    })
}

impl SourceSliceMaterializer {
    #[must_use]
    pub const fn new(
        deps: SourceSliceMaterializerDeps,
        domain: DomainConfig,
        max_book_staleness: Duration,
    ) -> Self {
        Self {
            deps,
            domain,
            max_book_staleness,
        }
    }

    /// Claim the deterministic identity, materialize every object, verify it by
    /// read-back, then write the manifest last and CAS the ledger to `Ready`.
    pub async fn materialize(
        &self,
        identity: SourceSliceIdentity,
        profile: &ResearchProfileArtifact,
        cancel: &CancellationToken,
    ) -> QuantResult<SourceSliceInfo> {
        let claim = self
            .deps
            .ledger
            .begin_or_get(NewSourceSlice::from_identity(
                SourceSliceId::from_v7(),
                identity.clone(),
            ))
            .await?;
        let claimed = claim.source_slice;
        match claimed.status {
            SourceSliceStatus::Ready => return Ok(claimed),
            SourceSliceStatus::Failed => {
                return Err(StorageError::state_conflict(
                    "quant_source_slice",
                    Some(&claimed.source_slice_id),
                    "the canonical source-slice attempt is failed and requires an explicit retry",
                )
                .into());
            }
            SourceSliceStatus::Materializing => {}
        }
        if !claim.acquired {
            return Err(StorageError::state_conflict(
                "quant_source_slice",
                Some(&claimed.source_slice_id),
                "the canonical Source Slice is already being materialized",
            )
            .into());
        }
        let result = self.materialize_claim(&identity, profile, cancel).await;
        match result {
            Ok(completion) => self
                .deps
                .ledger
                .complete(&claimed.source_slice_id, completion)
                .await
                .map_err(Into::into),
            Err(error) => {
                let detail = error.to_string().chars().take(2_048).collect::<String>();
                self.deps
                    .ledger
                    .fail(&claimed.source_slice_id, detail)
                    .await?;
                Err(error)
            }
        }
    }

    async fn materialize_claim(
        &self,
        identity: &SourceSliceIdentity,
        profile: &ResearchProfileArtifact,
        cancel: &CancellationToken,
    ) -> QuantResult<CompleteSourceSlice> {
        ensure_not_cancelled(cancel, "before Source Slice reads")?;
        if Utc::now() < identity.pit_cutoff {
            return Err(ResearchError::DatasetBuild {
                detail: "Source Slice PIT cutoff cannot be in the future".to_owned(),
            }
            .into());
        }
        let inputs = self.load_inputs(identity, profile, cancel).await?;
        let mut objects = self.write_platform_objects(&inputs, profile).await?;
        objects.extend(self.write_domain_objects(&inputs, profile).await?);
        objects.sort_by(|left, right| {
            (left.kind, left.uri.as_str()).cmp(&(right.kind, right.uri.as_str()))
        });
        self.seal_manifest(identity, profile, &inputs, objects, cancel)
            .await
    }

    async fn filter_cohort(
        &self,
        profile: &ResearchProfileArtifact,
        boundary: &DecisionBoundary,
        market_changes: Vec<CatalogMarketChangeInfo>,
    ) -> QuantResult<(Vec<MarketId>, Vec<CatalogMarketChangeInfo>)> {
        let required_category = profile.spec.cohort_contract.category();
        let mut market_ids = Vec::with_capacity(market_changes.len());
        let mut selected_changes = Vec::with_capacity(market_changes.len());
        for version in market_changes {
            let market =
                serde_json::from_value::<MarketRegistryInfo>(version.payload.clone().into_inner())
                    .map_err(|error| ResearchError::CohortMismatch {
                        profile: profile.profile_ref.id.to_string(),
                        detail: format!(
                            "catalog market {} cannot be decoded for cohort selection: {error}",
                            version.market_id
                        ),
                    })?;
            if required_category.is_none_or(|category| market.categories.contains(category)) {
                market_ids.push(version.market_id.clone());
                selected_changes.push(version);
            }
        }
        if let Some(category) = required_category {
            let expected_family = match category {
                MarketCategory::Crypto => DomainFamily::Crypto,
                MarketCategory::Weather => DomainFamily::Weather,
                _ => {
                    return Err(ResearchError::CohortMismatch {
                        profile: profile.profile_ref.id.to_string(),
                        detail: format!("category {category:?} has no vertical linkage contract"),
                    }
                    .into());
                }
            };
            let linked = self
                .deps
                .linkage
                .ledger_for_markets(&market_ids, boundary)
                .await?
                .into_iter()
                .filter_map(|info| {
                    let market_id = info.market_id.clone();
                    let linkage = MarketLinkage::from(info);
                    (linkage.domain_family == expected_family && linkage.binding().is_some())
                        .then_some(market_id)
                })
                .collect::<HashSet<_>>();
            market_ids.retain(|market_id| linked.contains(market_id));
            selected_changes.retain(|version| linked.contains(&version.market_id));
        }
        if market_ids.is_empty() {
            return Err(ResearchError::CohortMismatch {
                profile: profile.profile_ref.id.to_string(),
                detail: "no market satisfies the explicit category and PIT linkage contract"
                    .to_owned(),
            }
            .into());
        }
        Ok((market_ids, selected_changes))
    }

    async fn load_inputs(
        &self,
        identity: &SourceSliceIdentity,
        profile: &ResearchProfileArtifact,
        cancel: &CancellationToken,
    ) -> QuantResult<SourceSliceInputs> {
        let fit_seal = self
            .deps
            .exchange_history
            .find_fit_seal(identity.fit_seal_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found("quant_history_fit_seal", identity.fit_seal_id)
            })?;
        if fit_seal.seal.seal_hash != identity.fit_seal_hash {
            return Err(ResearchError::DatasetBuild {
                detail: "Source Slice fit-seal hash does not match the immutable ledger".to_owned(),
            }
            .into());
        }
        self.deps
            .exchange_history
            .validate_fit_seal(identity.fit_seal_id, identity.fit_seal_hash)
            .await?;
        let boundary = source_boundary(identity)?;
        let market_ids = self
            .deps
            .facts
            .observed_markets_between(
                identity.window_start.timestamp_millis(),
                identity.window_end.timestamp_millis(),
                identity.pit_cutoff.timestamp_millis(),
            )
            .await?;
        if market_ids.is_empty() {
            return Err(ResearchError::DatasetBuild {
                detail: "Source Slice contains no PIT-observed markets".to_owned(),
            }
            .into());
        }
        let market_changes = self.deps.catalog.markets_at(&market_ids, &boundary).await?;
        require_catalog_coverage(&market_ids, &market_changes)?;
        let (market_ids, market_changes) = self
            .filter_cohort(profile, &boundary, market_changes)
            .await?;
        let samples = replay_samples(&market_changes)?;
        if samples.is_empty() {
            return Err(ResearchError::DatasetBuild {
                detail: "Source Slice catalog has no decodable market/token pairs".to_owned(),
            }
            .into());
        }
        let loader = HistoricalWindowLoader::new(
            Arc::clone(&self.deps.facts),
            Arc::clone(&self.deps.catalog),
            Arc::clone(&self.deps.clob_market_info),
            Arc::clone(&self.deps.linkage),
            Arc::clone(&self.deps.calibration),
            self.max_book_staleness,
        );
        let prefetched = loader
            .prefetch(&WindowSpec {
                window_start: identity.window_start,
                window_end: identity.window_end,
                available_by: identity.pit_cutoff,
                samples,
                lookback: Duration::from_secs(profile.spec.max_feature_lookback_secs),
                knowledge_lag: Duration::ZERO,
                max_horizon_secs: profile.spec.target_horizon_secs,
                domain: self.domain.clone(),
                feature_contract: profile.spec.feature_contract,
                execution_history_chunks: fit_seal.chunks.clone(),
                requires_execution_history: true,
            })
            .await?;
        ensure_not_cancelled(cancel, "after Source Slice prefetch")?;

        let (l2_ledger, sessions, gap_records, invalid_sessions) =
            if profile.spec.feature_contract.requires_l2() {
                let tokens = prefetched.books.keys().cloned().collect::<Vec<_>>();
                let ledger_window = self
                    .deps
                    .facts
                    .book_l2_ledger_between(
                        tokens,
                        identity.window_start.timestamp_millis(),
                        identity.window_end.timestamp_millis(),
                        identity.pit_cutoff.timestamp_millis(),
                    )
                    .await?;
                let l2_ledger = merge_l2_ledger(&prefetched, ledger_window)?;
                let session_ids = stream_session_ids(&l2_ledger);
                let sessions = self
                    .deps
                    .facts
                    .book_stream_sessions(session_ids, identity.pit_cutoff.timestamp_millis())
                    .await?;
                let (gap_records, invalid_sessions) = prefetched.gap_evidence()?;
                (l2_ledger, sessions, gap_records, invalid_sessions)
            } else {
                (Vec::new(), Vec::new(), Vec::new(), Vec::new())
            };
        let lookback = ChronoDuration::seconds(
            i64::try_from(profile.spec.max_feature_lookback_secs).map_err(|error| {
                QuantError::config(format!("profile lookback does not fit i64: {error}"))
            })?,
        );
        let execution_from = identity
            .window_start
            .checked_sub_signed(lookback)
            .ok_or_else(|| QuantError::config("source-slice execution lookback overflow"))?;
        let executions = self
            .deps
            .facts
            .market_executions_between(
                market_ids.clone(),
                fit_seal.chunks.clone(),
                execution_from.timestamp_millis(),
                identity.window_end.timestamp_millis(),
                identity.pit_cutoff.timestamp_millis(),
            )
            .await?;
        let participants = self
            .deps
            .facts
            .execution_participants_between(
                market_ids,
                fit_seal.chunks,
                execution_from.timestamp_millis(),
                identity.window_end.timestamp_millis(),
                identity.pit_cutoff.timestamp_millis(),
            )
            .await?;
        Ok(SourceSliceInputs {
            prefetched,
            l2_ledger,
            sessions,
            gap_records,
            invalid_sessions,
            executions,
            participants,
        })
    }

    async fn write_platform_objects(
        &self,
        inputs: &SourceSliceInputs,
        profile: &ResearchProfileArtifact,
    ) -> QuantResult<Vec<SourceSliceObjectRef>> {
        let prefetched = &inputs.prefetched;
        let required = SourceSliceManifest::required_object_kinds(profile);
        let mut objects = Vec::new();
        if required.contains(&SourceSliceObjectKind::GammaMarketIdentity) {
            objects.push(self.write_gamma_identity(inputs).await?);
        }
        if required.contains(&SourceSliceObjectKind::CatalogMarket) {
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::CatalogMarket,
                    records(
                        &prefetched.catalog.market_changes,
                        |row| Some(row.source_effective_at),
                        |row| Some(row.available_at),
                    )?,
                )
                .await?,
            );
        }
        if required.contains(&SourceSliceObjectKind::CatalogEvent) {
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::CatalogEvent,
                    records(
                        &prefetched.catalog.event_changes,
                        |row| Some(row.source_effective_at),
                        |row| Some(row.available_at),
                    )?,
                )
                .await?,
            );
        }
        if required.contains(&SourceSliceObjectKind::ClobMarketInfo) {
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::ClobMarketInfo,
                    records(
                        &prefetched.clob_market_info,
                        |row| Some(row.effective_at),
                        |row| Some(row.available_at),
                    )?,
                )
                .await?,
            );
        }
        if required.contains(&SourceSliceObjectKind::L2Ledger) {
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::L2Ledger,
                    records(
                        &inputs.l2_ledger,
                        |row| DateTime::from_timestamp_millis(row.venue_event_time),
                        |row| DateTime::from_timestamp_millis(row.persisted_time),
                    )?,
                )
                .await?,
            );
        }
        if required.contains(&SourceSliceObjectKind::L2Session) {
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::L2Session,
                    records(
                        &inputs.sessions,
                        |row| DateTime::from_timestamp_millis(row.opened_at),
                        |row| DateTime::from_timestamp_millis(row.recorded_at),
                    )?,
                )
                .await?,
            );
        }
        if required.contains(&SourceSliceObjectKind::L2Gap) {
            objects.push(
                self.write_object(SourceSliceObjectKind::L2Gap, inputs.gap_records.clone())
                    .await?,
            );
        }
        if required.contains(&SourceSliceObjectKind::BookMicrostructure) {
            let micro = flatten(&prefetched.micro);
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::BookMicrostructure,
                    records(
                        &micro,
                        |row| DateTime::from_timestamp_millis(row.bucket_time),
                        |row| DateTime::from_timestamp_millis(row.available_at),
                    )?,
                )
                .await?,
            );
        }
        if required.contains(&SourceSliceObjectKind::MarketExecution) {
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::MarketExecution,
                    records(
                        &inputs.executions,
                        |row| DateTime::from_timestamp_millis(row.effective_at),
                        |row| DateTime::from_timestamp_millis(row.model_available_at),
                    )?,
                )
                .await?,
            );
        }
        if required.contains(&SourceSliceObjectKind::ExecutionParticipant) {
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::ExecutionParticipant,
                    records(
                        &inputs.participants,
                        |row| DateTime::from_timestamp_millis(row.effective_at),
                        |row| DateTime::from_timestamp_millis(row.model_available_at),
                    )?,
                )
                .await?,
            );
        }
        if required.contains(&SourceSliceObjectKind::Resolution) {
            let resolutions = flatten(&prefetched.resolutions);
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::Resolution,
                    records(
                        &resolutions,
                        |row| DateTime::from_timestamp_millis(row.resolved_at),
                        |row| DateTime::from_timestamp_millis(row.observed_at),
                    )?,
                )
                .await?,
            );
        }
        Ok(objects)
    }

    async fn write_gamma_identity(
        &self,
        inputs: &SourceSliceInputs,
    ) -> QuantResult<SourceSliceObjectRef> {
        let identities = inputs
            .prefetched
            .catalog
            .market_changes
            .iter()
            .cloned()
            .map(GammaIdentityRecord::Market)
            .chain(
                inputs
                    .prefetched
                    .catalog
                    .event_changes
                    .iter()
                    .cloned()
                    .map(GammaIdentityRecord::Event),
            )
            .collect::<Vec<_>>();
        self.write_object(
            SourceSliceObjectKind::GammaMarketIdentity,
            records(
                &identities,
                |row| Some(row.event_at()),
                |row| Some(row.available_at()),
            )?,
        )
        .await
    }

    async fn write_domain_objects(
        &self,
        inputs: &SourceSliceInputs,
        profile: &ResearchProfileArtifact,
    ) -> QuantResult<Vec<SourceSliceObjectRef>> {
        let prefetched = &inputs.prefetched;
        let required = SourceSliceManifest::required_object_kinds(profile);
        let mut objects = Vec::new();
        if required.contains(&SourceSliceObjectKind::MarketLinkage) {
            let linkages = flatten(&prefetched.linkages);
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::MarketLinkage,
                    records(
                        &linkages,
                        |row| Some(row.effective_at),
                        |row| Some(row.available_at),
                    )?,
                )
                .await?,
            );
        }
        if required.contains(&SourceSliceObjectKind::DomainObservation) {
            let domain = flatten(&prefetched.domain_observations);
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::DomainObservation,
                    records(&domain, |row| Some(row.observed_at), |row| row.available_at)?,
                )
                .await?,
            );
        }
        if required.contains(&SourceSliceObjectKind::CryptoPriceReport) {
            let crypto = flatten(&prefetched.crypto_reports);
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::CryptoPriceReport,
                    records(
                        &crypto,
                        |row| Some(row.event_time),
                        |row| Some(row.available_at),
                    )?,
                )
                .await?,
            );
        }
        if required.contains(&SourceSliceObjectKind::WeatherObservation) {
            let weather_observations = flatten(&prefetched.weather_observations);
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::WeatherObservation,
                    records(
                        &weather_observations,
                        |row| Some(row.observed_at),
                        |row| Some(row.available_at),
                    )?,
                )
                .await?,
            );
        }
        if required.contains(&SourceSliceObjectKind::WeatherForecast) {
            let weather_forecasts = flatten(&prefetched.weather_forecasts);
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::WeatherForecast,
                    records(
                        &weather_forecasts,
                        |row| Some(row.valid_time),
                        |row| Some(row.available_at),
                    )?,
                )
                .await?,
            );
        }
        if required.contains(&SourceSliceObjectKind::CalibrationReference) {
            objects.push(
                self.write_object(
                    SourceSliceObjectKind::CalibrationReference,
                    records(
                        &prefetched.weather_calibrations,
                        |row| Some(row.fit_window_end),
                        |row| Some(row.published_at),
                    )?,
                )
                .await?,
            );
        }
        Ok(objects)
    }

    async fn seal_manifest(
        &self,
        identity: &SourceSliceIdentity,
        profile: &ResearchProfileArtifact,
        inputs: &SourceSliceInputs,
        objects: Vec<SourceSliceObjectRef>,
        cancel: &CancellationToken,
    ) -> QuantResult<CompleteSourceSlice> {
        let catalog_proof =
            catalog_proof(&*self.deps.catalog, identity, &inputs.prefetched).await?;
        let materialized_at = Utc::now();
        if materialized_at < identity.pit_cutoff {
            return Err(ResearchError::DatasetBuild {
                detail: "Source Slice PIT cutoff cannot be in the future".to_owned(),
            }
            .into());
        }
        let manifest = SourceSliceManifest {
            format_version: SOURCE_SLICE_MANIFEST_FORMAT_VERSION,
            profile_ref: identity.profile_ref.clone(),
            evaluation_track: identity.evaluation_track,
            research_program_hash: identity.research_program_hash,
            window_start: identity.window_start,
            window_end: identity.window_end,
            pit_cutoff: identity.pit_cutoff,
            materialized_at,
            catalog_proof,
            reader_contract_version: identity.reader_contract_version.clone(),
            schema_contract_version: identity.schema_contract_version.clone(),
            decision_policy_snapshot_id: identity.decision_policy_snapshot_id,
            runtime_config_hash: identity.runtime_config_hash,
            fit_seal_id: identity.fit_seal_id,
            fit_seal_hash: identity.fit_seal_hash,
            dataset_format_version: DATASET_ARTIFACT_FORMAT_VERSION,
            capability_registry_hashes: inputs.capability_hashes()?,
            pit_cutoffs: profile
                .spec
                .required_sources()
                .into_iter()
                .map(|source| SourceSlicePitCutoff {
                    source,
                    available_at: identity.pit_cutoff,
                })
                .collect(),
            invalid_sessions: inputs.invalid_sessions.clone(),
            objects,
        };
        manifest
            .validate_for_profile(
                profile,
                &identity.research_program_hash,
                identity.window_start,
                identity.window_end
                    - ChronoDuration::seconds(
                        i64::try_from(profile.spec.target_horizon_secs).map_err(|error| {
                            ResearchError::DatasetBuild {
                                detail: error.to_string(),
                            }
                        })?,
                    ),
                identity.pit_cutoff,
            )
            .map_err(|detail| ResearchError::DatasetBuild { detail })?;
        ensure_not_cancelled(cancel, "before Source Slice manifest seal")?;
        let manifest_bytes = CanonicalDigest::canonical_json_bytes(&manifest)?;
        let manifest_hash = CanonicalDigest::content_hash_bytes(&manifest_bytes);
        let manifest_uri = self
            .put_verified(&manifest_hash, "json", &manifest_bytes)
            .await?
            .0;
        let manifest_ref = SourceSliceManifestRef {
            manifest_uri,
            manifest_hash,
        };
        Ok(CompleteSourceSlice {
            manifest_ref,
            manifest,
        })
    }

    async fn write_object(
        &self,
        kind: SourceSliceObjectKind,
        records: Vec<SourceSliceRecord>,
    ) -> QuantResult<SourceSliceObjectRef> {
        let bytes = SourceSliceParquetCodec::encode(&records)?;
        let byte_hash = CanonicalDigest::content_hash_bytes(&bytes);
        let (uri, object_version) = self.put_verified(&byte_hash, "parquet", &bytes).await?;
        let decoded = SourceSliceParquetCodec::decode(&self.deps.artifacts.get(&uri).await?)?;
        if decoded != records {
            return Err(ResearchError::DatasetBuild {
                detail: format!("Source Slice object {kind:?} changed during persistence"),
            }
            .into());
        }
        let schema_hash =
            CanonicalDigest::content_hash_json(&("source_slice_parquet_envelope_v2", kind))?;
        Ok(SourceSliceObjectRef {
            kind,
            uri,
            object_version,
            byte_hash,
            schema_hash,
            row_count: u64::try_from(records.len()).map_err(|error| {
                ResearchError::DatasetBuild {
                    detail: error.to_string(),
                }
            })?,
            min_event_at: records.iter().filter_map(|row| row.event_at).min(),
            max_event_at: records.iter().filter_map(|row| row.event_at).max(),
            min_available_at: records.iter().filter_map(|row| row.available_at).min(),
            max_available_at: records.iter().filter_map(|row| row.available_at).max(),
        })
    }

    async fn put_verified(
        &self,
        byte_hash: &ContentHash,
        extension: &str,
        bytes: &[u8],
    ) -> QuantResult<(ArtifactUri, String)> {
        let id = byte_hash.to_string().replace(':', "-");
        let uri = self
            .deps
            .artifacts
            .put(
                ArtifactKey::new(ArtifactNamespace::SourceSlice, id, extension)?,
                bytes,
            )
            .await?;
        let metadata = self.deps.artifacts.metadata(&uri).await?;
        if metadata.byte_size
            != u64::try_from(bytes.len()).map_err(|error| ResearchError::DatasetBuild {
                detail: error.to_string(),
            })?
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!("Source Slice object {uri} size changed after persistence"),
            }
            .into());
        }
        if metadata.durability.remote && !metadata.durability.permits_production_publish() {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "Source Slice object {} is not backed by versioned Object-Lock storage",
                    uri.as_str()
                ),
            }
            .into());
        }
        let persisted = self.deps.artifacts.get(&uri).await?;
        let actual = CanonicalDigest::content_hash_bytes(&persisted);
        if &actual != byte_hash {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: byte_hash.to_string(),
                actual: actual.to_string(),
            }
            .into());
        }
        let object_version = metadata
            .version_id
            .unwrap_or_else(|| format!("local-content:{byte_hash}"));
        Ok((uri, object_version))
    }
}

fn source_boundary(identity: &SourceSliceIdentity) -> QuantResult<DecisionBoundary> {
    let millis = identity
        .pit_cutoff
        .signed_duration_since(identity.window_end)
        .num_milliseconds();
    if millis < 0 {
        return Err(ResearchError::DatasetPlan {
            detail: "Source Slice PIT cutoff precedes its source window".to_owned(),
        }
        .into());
    }
    let lag_secs =
        u64::try_from((millis + 999) / 1_000).map_err(|error| ResearchError::DatasetPlan {
            detail: error.to_string(),
        })?;
    DecisionClock::new(lag_secs).serving_boundary(identity.pit_cutoff, lag_secs, lag_secs)
}

fn replay_samples(versions: &[CatalogMarketChangeInfo]) -> QuantResult<Vec<ReplaySample>> {
    let mut samples = Vec::with_capacity(versions.len().saturating_mul(2));
    for version in versions {
        let market =
            serde_json::from_value::<MarketRegistryInfo>(version.payload.clone().into_inner())
                .map_err(|error| ResearchError::DatasetBuild {
                    detail: format!("catalog market {} is invalid: {error}", version.market_id),
                })?;
        let market_id = market.market_id;
        for token_id in [market.token_yes, market.token_no] {
            samples.push(ReplaySample {
                market_id: market_id.clone(),
                token_id,
            });
        }
    }
    samples.sort_by(|left, right| {
        (&left.market_id, &left.token_id).cmp(&(&right.market_id, &right.token_id))
    });
    samples.dedup_by(|left, right| {
        left.market_id == right.market_id && left.token_id == right.token_id
    });
    Ok(samples)
}

fn require_catalog_coverage(
    observed: &[MarketId],
    resolved: &[CatalogMarketChangeInfo],
) -> QuantResult<()> {
    let resolved_ids = resolved
        .iter()
        .map(|version| &version.market_id)
        .collect::<HashSet<_>>();
    let missing = observed
        .iter()
        .filter(|market_id| !resolved_ids.contains(market_id))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    let sample = missing
        .iter()
        .take(5)
        .map(|market_id| market_id.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    Err(ResearchError::DatasetBuild {
        detail: format!(
            "catalog ledger omitted {} PIT-observed markets; first missing: {sample}",
            missing.len()
        ),
    }
    .into())
}

fn stream_session_ids(ledger: &[BookL2LedgerRow]) -> Vec<Uuid> {
    let mut ids = BTreeSet::new();
    for row in ledger {
        ids.insert(row.stream_session_id);
    }
    ids.into_iter().collect()
}

fn merge_l2_ledger(
    prefetched: &Prefetched,
    mut ledger_window: Vec<BookL2LedgerRow>,
) -> QuantResult<Vec<BookL2LedgerRow>> {
    let mut ledger = flatten(&prefetched.books);
    ledger.append(&mut ledger_window);
    ledger.sort_by(|left, right| {
        (
            &left.token_id,
            left.stream_session_id,
            left.token_sequence,
            left.event_hash,
        )
            .cmp(&(
                &right.token_id,
                right.stream_session_id,
                right.token_sequence,
                right.event_hash,
            ))
    });
    let mut canonical = Vec::<BookL2LedgerRow>::with_capacity(ledger.len());
    for row in ledger {
        if let Some(previous) = canonical.last()
            && previous.token_id == row.token_id
            && previous.stream_session_id == row.stream_session_id
            && previous.token_sequence == row.token_sequence
        {
            if previous.event_hash != row.event_hash {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "conflicting L2 ledger identity for token {}, session {}, sequence {}",
                        row.token_id, row.stream_session_id, row.token_sequence
                    ),
                }
                .into());
            }
            continue;
        }
        canonical.push(row);
    }
    Ok(canonical)
}

impl Prefetched {
    fn gap_evidence(
        &self,
    ) -> QuantResult<(Vec<SourceSliceRecord>, Vec<SourceSliceInvalidSession>)> {
        let mut gap_rows = Vec::new();
        let mut invalid = BTreeMap::new();
        for (token_id, series) in &self.micro {
            for row in series.iter().filter(|row| row.gap_count > 0) {
                let event_at = DateTime::from_timestamp_millis(row.bucket_time);
                let available_at = DateTime::from_timestamp_millis(row.available_at);
                gap_rows.push(source_record(row, event_at, available_at)?);
                let snapshot = self.books.get(token_id).and_then(|rows| {
                    rows.iter()
                        .rev()
                        .find(|snapshot| snapshot.venue_event_time <= row.bucket_time)
                });
                if let (Some(event_at), Some(snapshot)) = (event_at, snapshot) {
                    let diagnostic_hash = CanonicalDigest::content_hash_json(row)?;
                    invalid
                        .entry((token_id.to_string(), snapshot.stream_session_id.to_string()))
                        .or_insert_with(|| SourceSliceInvalidSession {
                            token_id: token_id.to_string(),
                            session_id: snapshot.stream_session_id.to_string(),
                            invalidated_at: event_at,
                            first_failure_sequence: None,
                            reason: SourceSliceSessionInvalidationReason::SequenceGap,
                            diagnostic_hash,
                        });
                }
            }
        }
        gap_rows.sort_by(|left, right| left.record_key.cmp(&right.record_key));
        Ok((gap_rows, invalid.into_values().collect()))
    }
}

async fn catalog_proof(
    repository: &dyn CatalogLedgerRepository,
    identity: &SourceSliceIdentity,
    prefetched: &Prefetched,
) -> QuantResult<SourceSliceCatalogProof> {
    let chain = repository
        .batch_chain(identity.window_start, identity.pit_cutoff)
        .await?
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: "catalog has no complete committed baseline for the Source Slice window"
                .to_owned(),
        })?;
    let first = chain
        .batches
        .first()
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: "catalog batch chain is empty".to_owned(),
        })?;
    let last = chain
        .batches
        .last()
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: "catalog batch chain is empty".to_owned(),
        })?;
    let committed_through = last
        .committed_at
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: "catalog terminal batch has no committed timestamp".to_owned(),
        })?;
    let chain_entries = chain
        .batches
        .iter()
        .map(|batch| {
            Ok((
                batch.catalog_sync_batch_id,
                batch.sync_kind,
                batch
                    .committed_at
                    .ok_or_else(|| ResearchError::DatasetBuild {
                        detail: format!(
                            "catalog batch {} has no commit time",
                            batch.catalog_sync_batch_id
                        ),
                    })?,
                batch
                    .batch_hash
                    .ok_or_else(|| ResearchError::DatasetBuild {
                        detail: format!(
                            "catalog batch {} has no content hash",
                            batch.catalog_sync_batch_id
                        ),
                    })?,
            ))
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let market_count = u64::try_from(prefetched.catalog.market_changes.len()).map_err(|error| {
        ResearchError::DatasetBuild {
            detail: error.to_string(),
        }
    })?;
    let event_count = u64::try_from(prefetched.catalog.event_changes.len()).map_err(|error| {
        ResearchError::DatasetBuild {
            detail: error.to_string(),
        }
    })?;
    Ok(SourceSliceCatalogProof {
        base_complete_batch_id: first.catalog_sync_batch_id,
        terminal_batch_id: last.catalog_sync_batch_id,
        committed_through,
        ordered_batch_chain_hash: CanonicalDigest::content_hash_json(&chain_entries)?,
        market_count,
        event_count,
        snapshot_hash: CanonicalDigest::content_hash_json(&(
            &prefetched.catalog.market_changes,
            &prefetched.catalog.event_changes,
        ))?,
    })
}

fn records<T, E, A>(
    values: &[T],
    event_at: E,
    available_at: A,
) -> QuantResult<Vec<SourceSliceRecord>>
where
    T: Serialize,
    E: Fn(&T) -> Option<DateTime<Utc>>,
    A: Fn(&T) -> Option<DateTime<Utc>>,
{
    let mut records = BTreeMap::new();
    for value in values {
        let record = source_record(value, event_at(value), available_at(value))?;
        records.insert(record.record_key.clone(), record);
    }
    Ok(records.into_values().collect())
}

fn source_record<T: Serialize>(
    value: &T,
    event_at: Option<DateTime<Utc>>,
    available_at: Option<DateTime<Utc>>,
) -> QuantResult<SourceSliceRecord> {
    let payload = serde_json::to_value(value).map_err(|error| ResearchError::DatasetBuild {
        detail: format!("Source Slice fact serialization failed: {error}"),
    })?;
    let hash = CanonicalDigest::content_hash_json(&payload)?;
    Ok(SourceSliceRecord {
        record_key: hash.to_string(),
        event_at,
        available_at,
        payload,
    })
}

fn flatten<K, T: Clone>(values: &HashMap<K, Vec<T>>) -> Vec<T> {
    values.values().flatten().cloned().collect()
}

fn take(
    by_kind: &mut BTreeMap<SourceSliceObjectKind, Vec<SourceSliceRecord>>,
    kind: SourceSliceObjectKind,
) -> Vec<SourceSliceRecord> {
    by_kind.remove(&kind).unwrap_or_default()
}

fn decode_records<T: DeserializeOwned>(records: Vec<SourceSliceRecord>) -> QuantResult<Vec<T>> {
    records
        .into_iter()
        .map(|record| {
            // `read_objects` already verifies the immutable Parquet byte hash,
            // schema, row count, ordering, and PIT bounds. Deserializing the
            // borrowed JSON value directly preserves adapters that require a
            // borrowed string without serializing and reparsing every row.
            T::deserialize(&record.payload).map_err(|error| {
                ResearchError::DatasetBuild {
                    detail: format!(
                        "Source Slice record {} payload cannot be decoded: {error}",
                        record.record_key
                    ),
                }
                .into()
            })
        })
        .collect()
}

fn group_by<K, T, F>(values: Vec<T>, key: F) -> HashMap<K, Vec<T>>
where
    K: Eq + Hash,
    F: Fn(&T) -> K,
{
    let mut grouped = HashMap::new();
    for value in values {
        grouped
            .entry(key(&value))
            .or_insert_with(Vec::new)
            .push(value);
    }
    grouped
}

fn join_execution_facts(
    executions: &[MarketExecutionRow],
    participants: &[ExecutionParticipantRow],
) -> QuantResult<Vec<ExecutionParticipantFactRow>> {
    let executions_by_id = executions
        .iter()
        .map(|execution| (execution.execution_id, execution))
        .collect::<BTreeMap<_, _>>();
    let mut participants_by_id = BTreeMap::new();
    for participant in participants {
        participants_by_id
            .entry(participant.execution_id)
            .or_insert_with(Vec::new)
            .push(participant);
    }
    if executions_by_id.len() != executions.len()
        || executions_by_id.len() != participants_by_id.len()
    {
        return Err(ResearchError::DatasetBuild {
            detail: "execution source slice has duplicate or orphan identities".to_owned(),
        }
        .into());
    }
    let mut rows = Vec::with_capacity(participants.len());
    for (execution_id, execution) in executions_by_id {
        let execution_participants =
            participants_by_id
                .get(&execution_id)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: "execution source slice is missing participant facts".to_owned(),
                })?;
        let maker_count = execution_participants
            .iter()
            .filter(|row| row.participant_role == ChExecutionParticipantRole::Maker)
            .count();
        let taker_count = execution_participants
            .iter()
            .filter(|row| row.participant_role == ChExecutionParticipantRole::Taker)
            .count();
        let participant_notional = execution_participants
            .iter()
            .map(|row| Usd::from(row.participant_notional).inner())
            .sum::<Decimal>();
        let expected_notional = Usd::from(execution.notional_usd).inner() * Decimal::TWO;
        if execution_participants.len() != 2
            || maker_count != 1
            || taker_count != 1
            || participant_notional != expected_notional
        {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "execution {} violates the two-sided participant conservation contract",
                    ContentHash::from(execution_id)
                ),
            }
            .into());
        }
        for participant in execution_participants {
            if participant.market_id != execution.market_id
                || participant.token_id != execution.token_id
                || participant.effective_at != execution.effective_at
                || participant.model_available_at != execution.model_available_at
                || participant.availability_policy_hash != execution.availability_policy_hash
            {
                return Err(ResearchError::DatasetBuild {
                    detail: "execution and participant source facts disagree".to_owned(),
                }
                .into());
            }
            rows.push(ExecutionParticipantFactRow {
                execution_id,
                market_id: execution.market_id.clone(),
                token_id: execution.token_id.clone(),
                participant_address: participant.participant_address.clone(),
                participant_role: participant.participant_role,
                side: execution.side,
                price: execution.price,
                size_shares: execution.size_shares,
                notional_usd: execution.notional_usd,
                transaction_hash: execution.transaction_hash.clone(),
                effective_at: execution.effective_at,
                observed_at: execution.observed_at,
                model_available_at: execution.model_available_at,
                availability_policy_hash: execution.availability_policy_hash,
            });
        }
    }
    Ok(rows)
}

fn group_weather_observations(
    values: Vec<WeatherObservationFact>,
) -> HashMap<String, Vec<WeatherObservationFact>> {
    let mut grouped = HashMap::new();
    for value in values {
        grouped
            .entry(value.subject_key.clone())
            .or_insert_with(Vec::new)
            .push(value);
    }
    grouped
}

fn group_weather_forecasts(
    values: Vec<WeatherForecastPoint>,
) -> HashMap<String, Vec<WeatherForecastPoint>> {
    let mut grouped = HashMap::new();
    for value in values {
        grouped
            .entry(value.subject_key.clone())
            .or_insert_with(Vec::new)
            .push(value);
    }
    grouped
}

fn ensure_not_cancelled(cancel: &CancellationToken, stage: &'static str) -> QuantResult<()> {
    if cancel.is_cancelled() {
        return Err(ResearchError::Cancelled {
            detail: format!("Source Slice materialization cancelled {stage}"),
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, env, path::Path, sync::Arc};

    use chrono::{TimeZone, Utc};
    use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
    use quant_pivot_models::{
        clickhouse::{BookL2LedgerRow, ChDigest},
        enums::clickhouse::ChCanonicalBookEventType,
        hashing::CanonicalDigest,
        types::{
            ArtifactUri, CapabilityRegistryHashes, CatalogSyncBatchId, ContentHash,
            DATASET_ARTIFACT_FORMAT_VERSION, DecisionPolicySnapshotId, MarketId,
            ReaderContractVersion, ResearchEvaluationTrack, SOURCE_SLICE_MANIFEST_FORMAT_VERSION,
            SchemaContractVersion, SourceSliceCatalogProof, SourceSliceManifest,
            SourceSliceManifestRef, SourceSliceObjectKind, SourceSliceObjectRef,
            SourceSlicePitCutoff, TokenId, builtin_research_profiles,
        },
    };
    use quant_pivot_research::{
        artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
        source_slice::SourceSliceRecord,
    };
    use rust_decimal::Decimal;
    use serde::Deserialize;
    use serde_json::{json, to_value};
    use uuid::Uuid;

    use super::{SourceSliceReader, decode_records, require_catalog_coverage};

    #[derive(Debug, PartialEq, Eq, Deserialize)]
    struct TypedSourceRecord {
        amount: Decimal,
        flags: BTreeMap<String, bool>,
    }

    fn hash(byte: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", byte.to_string().repeat(64))).expect("hash")
    }

    fn manifest(root: &Path) -> SourceSliceManifest {
        let window_start = Utc.timestamp_opt(100, 0).single().expect("source start");
        let window_end = Utc.timestamp_opt(120, 0).single().expect("source end");
        let pit_cutoff = Utc.timestamp_opt(130, 0).single().expect("source cutoff");
        let materialized_at = Utc.timestamp_opt(140, 0).single().expect("materialized at");
        let kind = SourceSliceObjectKind::CatalogMarket;
        let profile = builtin_research_profiles()
            .expect("built-in profiles")
            .remove(0);
        let pit_cutoffs = profile
            .spec
            .required_sources()
            .into_iter()
            .map(|source| SourceSlicePitCutoff {
                source,
                available_at: pit_cutoff,
            })
            .collect();
        let schema_hash =
            CanonicalDigest::content_hash_json(&("source_slice_parquet_envelope_v2", kind))
                .expect("source schema hash");
        SourceSliceManifest {
            format_version: SOURCE_SLICE_MANIFEST_FORMAT_VERSION,
            fit_seal_id: Uuid::from_u128(10).into(),
            fit_seal_hash: hash('0'),
            profile_ref: profile.profile_ref,
            evaluation_track: ResearchEvaluationTrack::ResearchOnly,
            research_program_hash: hash('1'),
            window_start,
            window_end,
            pit_cutoff,
            materialized_at,
            catalog_proof: SourceSliceCatalogProof {
                base_complete_batch_id: CatalogSyncBatchId::new(Uuid::from_u128(1)),
                terminal_batch_id: CatalogSyncBatchId::new(Uuid::from_u128(2)),
                committed_through: pit_cutoff,
                ordered_batch_chain_hash: hash('2'),
                market_count: 1,
                event_count: 1,
                snapshot_hash: hash('3'),
            },
            reader_contract_version: ReaderContractVersion::v1(),
            schema_contract_version: SchemaContractVersion::v1(),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            runtime_config_hash: hash('4'),
            dataset_format_version: DATASET_ARTIFACT_FORMAT_VERSION,
            capability_registry_hashes: CapabilityRegistryHashes::try_new(vec![hash('5')])
                .expect("capabilities"),
            pit_cutoffs,
            invalid_sessions: Vec::new(),
            objects: vec![SourceSliceObjectRef {
                kind,
                uri: ArtifactUri::parse(format!(
                    "file://{}",
                    root.join("missing.parquet").display()
                ))
                .expect("missing source object URI"),
                object_version: "missing-v1".to_owned(),
                byte_hash: hash('6'),
                schema_hash,
                row_count: 0,
                min_event_at: None,
                max_event_at: None,
                min_available_at: None,
                max_available_at: None,
            }],
        }
    }

    #[test]
    fn missing_catalog_fails() {
        let observed = [MarketId::new("missing-market")];
        let result = require_catalog_coverage(&observed, &[]);

        assert!(matches!(
            result,
            Err(QuantError::Research(ResearchError::DatasetBuild { detail }))
                if detail.contains("missing-market")
        ));
    }

    #[test]
    fn typed_records_decode_directly() -> QuantResult<()> {
        let records = vec![SourceSliceRecord {
            record_key: "fixture-record".to_owned(),
            event_at: None,
            available_at: None,
            payload: json!({
                "amount": "12.3400",
                "flags": {"zeta": false, "alpha": true},
            }),
        }];

        let decoded = decode_records::<TypedSourceRecord>(records)?;

        assert_eq!(
            decoded,
            vec![TypedSourceRecord {
                amount: Decimal::new(123_400, 4),
                flags: BTreeMap::from([("alpha".to_owned(), true), ("zeta".to_owned(), false),]),
            }]
        );
        Ok(())
    }

    #[test]
    fn book_row_uuid_decodes() -> QuantResult<()> {
        let stream_session_id = Uuid::from_u128(0x018f_1234_5678_7000_8000_0000_0000_0001);
        let row = BookL2LedgerRow {
            stream_session_id,
            shard_id: 0,
            token_id: TokenId::new("uuid-adapter-token"),
            market_id: None,
            token_sequence: 1,
            event_type: ChCanonicalBookEventType::Snapshot,
            bid_prices: Vec::new(),
            bid_sizes: Vec::new(),
            ask_prices: Vec::new(),
            ask_sizes: Vec::new(),
            old_tick_size: None,
            new_tick_size: None,
            trade_price: None,
            trade_side: None,
            trade_size: None,
            fee_rate_bps: None,
            venue_event_time: 1,
            ingress_time: 2,
            persisted_time: 3,
            event_hash: ChDigest::new([7; 32]),
            schema_version: BookL2LedgerRow::SCHEMA_VERSION,
        };
        let records = vec![SourceSliceRecord {
            record_key: "uuid-adapter-record".to_owned(),
            event_at: None,
            available_at: None,
            payload: to_value(row).expect("serialize ClickHouse ledger fixture"),
        }];

        let decoded = decode_records::<BookL2LedgerRow>(records)?;
        let decoded = decoded.into_iter().next().expect("one decoded ledger row");

        assert_eq!(decoded.stream_session_id, stream_session_id);
        Ok(())
    }

    #[tokio::test]
    async fn manifest_only_skips_objects() {
        let root = env::temp_dir().join(format!("quant-pivot-source-manifest-{}", Uuid::now_v7()));
        let store = LocalArtifactStore::new(root.clone());
        let manifest = manifest(&root);
        let bytes = CanonicalDigest::canonical_json_bytes(&manifest).expect("manifest bytes");
        let manifest_hash = CanonicalDigest::content_hash_bytes(&bytes);
        let uri = store
            .put(
                ArtifactKey::new(ArtifactNamespace::SourceSlice, "manifest-only", "json")
                    .expect("manifest key"),
                &bytes,
            )
            .await
            .expect("persist manifest");
        let reference = SourceSliceManifestRef {
            manifest_uri: uri,
            manifest_hash,
        };
        let reader = SourceSliceReader::new(Arc::new(store));
        let verified = reader
            .verify_manifest_ref(&reference)
            .await
            .expect("verify manifest without source objects");
        let full = reader.read_ref(&reference).await;
        tokio::fs::remove_dir_all(&root)
            .await
            .expect("remove manifest fixture");

        assert_eq!(verified, manifest);
        assert!(matches!(
            full,
            Err(QuantError::Research(ResearchError::ArtifactIo { uri, .. }))
                if uri.contains("missing.parquet")
        ));
    }
}
