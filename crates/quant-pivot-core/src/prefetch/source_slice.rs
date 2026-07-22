//! Immutable Source Slice materialization over the durable PIT ledgers.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    hash::Hash,
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, BookStreamSessionRow, MarketResolutionRow,
        TradeTapeRow,
    },
    domain::{
        data_plane::{
            CryptoPriceReport, DecisionBoundary, DecisionClock, DomainObservation,
            WeatherForecastPoint, WeatherObservationFact,
        },
        market::{CatalogMarketChangeInfo, CatalogWindowInfo, MarketRegistryInfo},
        quant::{
            CompleteSourceSlice, MarketLinkage, NewSourceSlice, SourceSliceIdentity,
            SourceSliceInfo,
        },
    },
    enums::{clickhouse::ChCanonicalBookEventType, quant::SourceSliceStatus},
    hashing::CanonicalDigest,
    runtime_config::DomainConfig,
    types::{
        ArtifactUri, ClobMarketInfoVersion, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION,
        IcaoStation, ResearchProfileArtifact, ResearchProfileDataSource,
        SOURCE_SLICE_MANIFEST_FORMAT_VERSION, SourceSliceCatalogProof, SourceSliceId,
        SourceSliceInvalidSession, SourceSliceManifest, SourceSliceManifestRef,
        SourceSliceObjectKind, SourceSliceObjectRef, SourceSlicePitCutoffs,
        SourceSliceSessionInvalidationReason,
    },
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, CatalogLedgerRepository, ClobMarketInfoRepository,
    MarketLinkageRepository, QuantFactReadRepository, SourceSliceRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    source_slice::{SourceSliceParquetCodec, SourceSliceRecord},
};
use serde::{Serialize, de::DeserializeOwned};
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
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub pit_cutoff: DateTime<Utc>,
    pub prefetched: Prefetched,
    pub clob_market_info: Vec<ClobMarketInfoVersion>,
    pub l2_ledger: Vec<BookL2LedgerRow>,
    pub sessions: Vec<BookStreamSessionRow>,
    pub invalid_sessions: Vec<SourceSliceInvalidSession>,
}

struct SourceSliceInputs {
    prefetched: Prefetched,
    clob_market_info: Vec<ClobMarketInfoVersion>,
    l2_ledger: Vec<BookL2LedgerRow>,
    sessions: Vec<BookStreamSessionRow>,
    gap_records: Vec<SourceSliceRecord>,
    invalid_sessions: Vec<SourceSliceInvalidSession>,
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
        let manifest = source_slice.manifest_json.as_ref().ok_or_else(|| {
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
    let catalog = CatalogWindowInfo {
        market_changes: decode_records(take(&mut by_kind, SourceSliceObjectKind::CatalogMarket))?,
        event_changes: decode_records(take(&mut by_kind, SourceSliceObjectKind::CatalogEvent))?,
    };
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
    let trade_tape = group_by(
        decode_records::<TradeTapeRow>(take(&mut by_kind, SourceSliceObjectKind::TradeTape))?,
        |row| row.market_id.clone(),
    );
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
        ))?)?;
    let weather_forecasts = group_weather_forecasts(decode_records::<WeatherForecastPoint>(
        take(&mut by_kind, SourceSliceObjectKind::WeatherForecast),
    )?)?;
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
        window_start: manifest.window_start,
        window_end: manifest.window_end,
        pit_cutoff: manifest.pit_cutoff,
        prefetched: Prefetched {
            books,
            micro,
            trade_tape,
            resolutions,
            catalog,
            domain_observations,
            crypto_reports,
            weather_observations,
            weather_forecasts,
            weather_calibrations,
            linkages,
        },
        clob_market_info,
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
        let inputs = self.load_inputs(identity, cancel).await?;
        let mut objects = self.write_platform_objects(&inputs).await?;
        objects.extend(self.write_domain_objects(&inputs).await?);
        objects.sort_by(|left, right| {
            (left.kind, left.uri.as_str()).cmp(&(right.kind, right.uri.as_str()))
        });
        self.seal_manifest(identity, profile, &inputs, objects, cancel)
            .await
    }

    async fn load_inputs(
        &self,
        identity: &SourceSliceIdentity,
        cancel: &CancellationToken,
    ) -> QuantResult<SourceSliceInputs> {
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
                lookback: Duration::ZERO,
                knowledge_lag: Duration::ZERO,
                max_horizon_secs: 0,
                domain: self.domain.clone(),
            })
            .await?;
        ensure_not_cancelled(cancel, "after Source Slice prefetch")?;

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
        let clob_market_info = self
            .deps
            .clob_market_info
            .window(
                &market_ids,
                identity.window_start,
                identity.window_end,
                identity.pit_cutoff,
            )
            .await?;
        let (gap_records, invalid_sessions) = gap_evidence(&prefetched)?;
        Ok(SourceSliceInputs {
            prefetched,
            clob_market_info,
            l2_ledger,
            sessions,
            gap_records,
            invalid_sessions,
        })
    }

    async fn write_platform_objects(
        &self,
        inputs: &SourceSliceInputs,
    ) -> QuantResult<Vec<SourceSliceObjectRef>> {
        let prefetched = &inputs.prefetched;
        let mut objects = Vec::new();
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
        objects.push(
            self.write_object(
                SourceSliceObjectKind::ClobMarketInfo,
                records(
                    &inputs.clob_market_info,
                    |row| Some(row.effective_at),
                    |row| Some(row.available_at),
                )?,
            )
            .await?,
        );
        objects.push(
            self.write_object(
                SourceSliceObjectKind::L2Ledger,
                records(
                    &inputs.l2_ledger,
                    |row| millis(row.venue_event_time),
                    |row| millis(row.persisted_time),
                )?,
            )
            .await?,
        );
        objects.push(
            self.write_object(
                SourceSliceObjectKind::L2Session,
                records(
                    &inputs.sessions,
                    |row| millis(row.opened_at),
                    |row| millis(row.recorded_at),
                )?,
            )
            .await?,
        );
        objects.push(
            self.write_object(SourceSliceObjectKind::L2Gap, inputs.gap_records.clone())
                .await?,
        );
        let micro = flatten(&prefetched.micro);
        objects.push(
            self.write_object(
                SourceSliceObjectKind::BookMicrostructure,
                records(
                    &micro,
                    |row| millis(row.bucket_time),
                    |row| millis(row.available_at),
                )?,
            )
            .await?,
        );
        let trades = flatten(&prefetched.trade_tape);
        objects.push(
            self.write_object(
                SourceSliceObjectKind::TradeTape,
                records(
                    &trades,
                    |row| millis(row.event_time),
                    |row| millis(row.ingestion_time),
                )?,
            )
            .await?,
        );
        let resolutions = flatten(&prefetched.resolutions);
        objects.push(
            self.write_object(
                SourceSliceObjectKind::Resolution,
                records(
                    &resolutions,
                    |row| millis(row.resolved_at),
                    |row| millis(row.observed_at),
                )?,
            )
            .await?,
        );
        Ok(objects)
    }

    async fn write_domain_objects(
        &self,
        inputs: &SourceSliceInputs,
    ) -> QuantResult<Vec<SourceSliceObjectRef>> {
        let prefetched = &inputs.prefetched;
        let linkages = flatten(&prefetched.linkages);
        let mut objects = vec![
            self.write_object(
                SourceSliceObjectKind::MarketLinkage,
                records(
                    &linkages,
                    |row| Some(row.effective_at),
                    |row| Some(row.available_at),
                )?,
            )
            .await?,
        ];
        let domain = flatten(&prefetched.domain_observations);
        objects.push(
            self.write_object(
                SourceSliceObjectKind::DomainObservation,
                records(&domain, |row| Some(row.observed_at), |row| row.available_at)?,
            )
            .await?,
        );
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
        let weather_required = profile
            .required_sources_contains(ResearchProfileDataSource::AviationWeather)
            || profile.required_sources_contains(ResearchProfileDataSource::GefsEnsemble);
        let calibration_required =
            profile.required_sources_contains(ResearchProfileDataSource::GhcnhCalibration);
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
            dataset_format_version: DATASET_ARTIFACT_FORMAT_VERSION,
            pit_cutoffs: SourceSlicePitCutoffs {
                catalog_available_at: identity.pit_cutoff,
                clob_market_info_available_at: identity.pit_cutoff,
                l2_available_at: identity.pit_cutoff,
                trade_tape_available_at: identity.pit_cutoff,
                weather_available_at: weather_required.then_some(identity.pit_cutoff),
                calibration_available_at: calibration_required.then_some(identity.pit_cutoff),
                resolution_available_at: identity.pit_cutoff,
            },
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

fn gap_evidence(
    prefetched: &Prefetched,
) -> QuantResult<(Vec<SourceSliceRecord>, Vec<SourceSliceInvalidSession>)> {
    let mut gap_rows = Vec::new();
    let mut invalid = BTreeMap::new();
    for (token_id, series) in &prefetched.micro {
        for row in series.iter().filter(|row| row.gap_count > 0) {
            let event_at = millis(row.bucket_time);
            let available_at = millis(row.available_at);
            gap_rows.push(source_record(row, event_at, available_at)?);
            let snapshot = prefetched.books.get(token_id).and_then(|rows| {
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
            let payload = CanonicalDigest::canonical_json_bytes(&record.payload)?;
            serde_json::from_slice(&payload).map_err(|error| {
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

fn group_weather_observations(
    values: Vec<WeatherObservationFact>,
) -> QuantResult<HashMap<IcaoStation, Vec<WeatherObservationFact>>> {
    let mut grouped = HashMap::new();
    for value in values {
        let station = value.station().ok_or_else(|| ResearchError::DatasetBuild {
            detail: format!(
                "Weather observation subject `{}` is not an ICAO station",
                value.subject_key
            ),
        })?;
        grouped.entry(station).or_insert_with(Vec::new).push(value);
    }
    Ok(grouped)
}

fn group_weather_forecasts(
    values: Vec<WeatherForecastPoint>,
) -> QuantResult<HashMap<IcaoStation, Vec<WeatherForecastPoint>>> {
    let mut grouped = HashMap::new();
    for value in values {
        let station = value.station().ok_or_else(|| ResearchError::DatasetBuild {
            detail: format!(
                "Weather forecast subject `{}` is not an ICAO station",
                value.subject_key
            ),
        })?;
        grouped.entry(station).or_insert_with(Vec::new).push(value);
    }
    Ok(grouped)
}

const fn millis(value: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value)
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
