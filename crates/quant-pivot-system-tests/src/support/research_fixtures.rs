//! Canonical research fixtures shared by integration tests.

use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookStreamSessionRow, ChDigest, ChPrice, ChSchemaVersion, ChShares,
    },
    domain::{
        data_plane::DecisionSource,
        quant::{
            CompleteSourceSlice, CompleteTrainingDatasetBuild, FeedbackCohortWindow,
            NewSourceSlice, NewTrainingDatasetPlan, SourceSliceIdentity, SourceSliceIdentityInput,
        },
    },
    enums::{
        catalog::CatalogTimestampQuality,
        clickhouse::{ChCanonicalBookEventType, ChStreamSessionEndReason, ChStreamSessionState},
        common::TickSize,
        market::MarketStatus,
        quant::{DatasetPurpose, FeedbackCohort, TrainingDatasetStatus},
    },
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, BookSnapshotRef, BookSnapshotSource, Bps, CapabilityRegistryHashes,
        CatalogDecisionRef, CatalogEventChangeId, CatalogMarketChangeId, CatalogSyncBatchId,
        ClobFeeDetails, ClobMarketInfoVersion, ClobMarketInfoVersionId, ClobTokenDescriptor,
        ContentHash, DATASET_ARTIFACT_FORMAT_VERSION, DATASET_COHORT_MANIFEST_FORMAT_VERSION,
        DATASET_SOURCE_LINEAGE_FORMAT_VERSION, DatasetCohortArtifactRef, DatasetCohortCounts,
        DatasetCohortManifest, DatasetCoverage, DatasetManifest, DatasetSourceLineage,
        DecisionCaptureEvidence, DecisionPolicySnapshotId, DecisionSnapshotEvidence, MarketContext,
        ModelSpecId, Price, Probability, ReaderContractVersion, RecommendationIdentity,
        ResearchEvaluationTrack, ResearchProfileRef, SOURCE_SLICE_MANIFEST_FORMAT_VERSION,
        SchemaContractVersion, SchemaVersion, Shares, SourceSliceCatalogProof, SourceSliceId,
        SourceSliceManifest, SourceSliceManifestRef, SourceSliceObjectKind, SourceSliceObjectRef,
        SourceSlicePitCutoffs, TrainingDatasetId, TrainingHorizonsSecs, TrainingSampleSources, Usd,
        default_sample_sources,
    },
};
use quant_pivot_repository::{
    postgres::{PgModelRegistryRepository, PgSourceSliceRepository, PgTrainingDatasetRepository},
    traits::{SourceSliceRepository, TrainingDatasetRepository},
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    source_slice::{SourceSliceParquetCodec, SourceSliceRecord},
    training::TrainingExample,
};
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;
use serde::Serialize;

use super::seeded_uuid;

fn source_hash(example: &TrainingExample, role: &str) -> ContentHash {
    CanonicalDigest::content_hash_typed(
        "quant-pivot:test:decision-capture-source",
        1,
        &(
            role,
            &example.market_id,
            &example.token_id,
            example.decision_at(),
        ),
    )
    .expect("canonical decision-capture source hash")
}

/// Bind a complete capture to an already frozen training example.
///
/// All source clocks are derived from the example's `DecisionBoundary`; the
/// fixture never subtracts knowledge lag independently or invents a second
/// decision time.
pub fn bind_fixture_decision_capture(example: &mut TrainingExample) {
    let boundary = example.decision_boundary.clone();
    let decision_at = boundary.decision_at();
    let catalog_effective_at = boundary.cutoff_for(DecisionSource::Catalog);
    let book_effective_at = boundary.cutoff_for(DecisionSource::Book);
    let book_age_ms = u64::try_from((decision_at - book_effective_at).num_milliseconds())
        .expect("fixture book cutoff is not after decision time");
    let scope = format!(
        "{}:{}:{}",
        example.market_id,
        example.token_id,
        decision_at.timestamp_micros()
    );
    let token_id = example.token_id.clone();
    example.decision_capture = Some(DecisionCaptureEvidence {
        snapshot: DecisionSnapshotEvidence {
            boundary,
            market_id: example.market_id.clone(),
            event_id: example.selected_market.event_id.clone(),
            token_id: token_id.clone(),
            catalog: CatalogDecisionRef {
                catalog_sync_batch_id: CatalogSyncBatchId::new(seeded_uuid(&format!(
                    "{scope}:catalog-batch"
                ))),
                market_change_id: CatalogMarketChangeId::new(seeded_uuid(&format!(
                    "{scope}:market-change"
                ))),
                event_change_id: CatalogEventChangeId::new(seeded_uuid(&format!(
                    "{scope}:event-change"
                ))),
                market_content_hash: source_hash(example, "market"),
                event_content_hash: source_hash(example, "event"),
                membership_hash: source_hash(example, "membership"),
                market_effective_at: catalog_effective_at,
                market_available_at: decision_at,
                event_effective_at: catalog_effective_at,
                event_available_at: decision_at,
                market_timestamp_quality: CatalogTimestampQuality::Source,
                event_timestamp_quality: CatalogTimestampQuality::Source,
            },
            book_snapshot_ref: BookSnapshotRef {
                token_id,
                source: BookSnapshotSource::CanonicalL2 {
                    stream_session_id: seeded_uuid(&format!("{scope}:book-session")),
                    token_sequence: 1,
                    source_event_hash: source_hash(example, "book-event"),
                    event_time_ms: book_effective_at.timestamp_millis(),
                    ingestion_time_ms: decision_at.timestamp_millis(),
                },
                content_hash: source_hash(example, "book-snapshot"),
            },
            book_effective_at,
            book_available_at: decision_at,
            selection: (&example.selected_market).into(),
        },
        identity: RecommendationIdentity {
            category: example.selected_market.category,
            question: "Fixture market?".to_owned(),
            outcome_name: "Yes".to_owned(),
        },
        market_context: MarketContext {
            best_bid: Some(Price::new(dec!(0.49))),
            best_ask: Some(Price::new(dec!(0.51))),
            mid_price: Some(Price::new(dec!(0.50))),
            spread_bps: Some(Bps::new(dec!(400))),
            depth_usd: example
                .selected_market
                .liquidity_usd
                .unwrap_or_else(|| Usd::new(dec!(100))),
            volume_24h_usd: example.selected_market.volume_24h_usd,
            book_age_ms,
            time_to_resolution_secs: None,
            market_status: MarketStatus::Active,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            fee_rate: None,
        },
        data_quality: example.feature_vector.data_quality,
        liquidity_score: Probability::ONE,
    });
}

fn source_record<T: Serialize>(
    record_key: String,
    event_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    payload: &T,
) -> QuantResult<SourceSliceRecord> {
    let payload = serde_json::to_value(payload).map_err(|error| ResearchError::DatasetBuild {
        detail: format!("fixture source record serialization failed: {error}"),
    })?;
    Ok(SourceSliceRecord {
        record_key,
        event_at: Some(event_at),
        available_at: Some(available_at),
        payload,
    })
}

const fn source_object_slug(kind: SourceSliceObjectKind) -> &'static str {
    match kind {
        SourceSliceObjectKind::ClobMarketInfo => "clob-market-info",
        SourceSliceObjectKind::L2Ledger => "l2-ledger",
        SourceSliceObjectKind::L2Session => "l2-session",
        _ => "source-object",
    }
}

async fn persist_source_object(
    store: &Arc<dyn ArtifactStore>,
    kind: SourceSliceObjectKind,
    mut records: Vec<SourceSliceRecord>,
) -> QuantResult<SourceSliceObjectRef> {
    records.sort_by(|left, right| {
        (&left.record_key, left.event_at, left.available_at).cmp(&(
            &right.record_key,
            right.event_at,
            right.available_at,
        ))
    });
    let bytes = SourceSliceParquetCodec::encode(&records)?;
    let byte_hash = CanonicalDigest::content_hash_bytes(&bytes);
    let key = ArtifactKey::new(
        ArtifactNamespace::SourceSlice,
        format!("{}-{}", source_object_slug(kind), byte_hash.hex()),
        "parquet",
    )?;
    let uri = store.put(key, &bytes).await?;
    let decoded = SourceSliceParquetCodec::decode(&store.get(&uri).await?)?;
    if decoded != records {
        return Err(ResearchError::DatasetBuild {
            detail: format!("fixture Source Slice object {kind:?} changed during persistence"),
        }
        .into());
    }
    let metadata = store.metadata(&uri).await?;
    let object_version = metadata
        .version_id
        .unwrap_or_else(|| format!("local-content:{byte_hash}"));
    Ok(SourceSliceObjectRef {
        kind,
        uri,
        object_version,
        byte_hash,
        schema_hash: CanonicalDigest::content_hash_json(&(
            "source_slice_parquet_envelope_v2",
            kind,
        ))?,
        row_count: u64::try_from(records.len()).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("fixture source row count overflow: {error}"),
        })?,
        min_event_at: records.iter().filter_map(|row| row.event_at).min(),
        max_event_at: records.iter().filter_map(|row| row.event_at).max(),
        min_available_at: records.iter().filter_map(|row| row.available_at).min(),
        max_available_at: records.iter().filter_map(|row| row.available_at).max(),
    })
}

/// Persist the immutable market-info and canonical-L2 inputs consumed by
/// backtest/CPCV integration tests, then seal a canonical manifest.
pub struct ReplayableSourceSliceFixture {
    pub profile_ref: ResearchProfileRef,
    pub research_program_hash: ContentHash,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub runtime_config_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
}

/// Stored source-slice manifest bytes and their immutable reference.
pub struct StoredSourceSlice {
    pub manifest_ref: SourceSliceManifestRef,
    pub manifest: SourceSliceManifest,
}

/// Inputs for a complete, repository-backed Source Slice test fixture.
pub struct DatasetSourceSeed {
    pub scope: String,
    pub profile_ref: ResearchProfileRef,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub pit_cutoff: DateTime<Utc>,
}

/// Inputs for a repository-backed reusable evaluation holdout fixture.
pub struct EvaluationDatasetSeed {
    pub scope: String,
    pub model_spec_id: ModelSpecId,
    pub model_spec_definition_hash: ContentHash,
    pub profile_ref: ResearchProfileRef,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub sample_count: u64,
}

/// Complete immutable inputs for one Dataset v2 plan/manifest fixture.
pub struct DatasetLedgerSeed {
    pub training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub model_spec_definition_hash: ContentHash,
    pub source_lineage: DatasetSourceLineage,
    pub cohort_manifest: Option<DatasetCohortManifest>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub purpose: DatasetPurpose,
    pub knowledge_lag_secs: u64,
    pub sample_interval_secs: u64,
    pub horizons_secs: Vec<u64>,
    pub feature_schema_version: Option<SchemaVersion>,
    pub sample_sources: Option<TrainingSampleSources>,
    pub feature_schema_hash: ContentHash,
    pub factor_schema_hash: ContentHash,
    pub label_schema_hash: ContentHash,
    pub semantic_dataset_hash: ContentHash,
    pub source_fingerprint: ContentHash,
    pub sample_count: u64,
}

/// Canonical matching Dataset v2 plan and manifest.
pub struct DatasetLedgerFixture {
    pub plan: NewTrainingDatasetPlan,
    pub manifest: DatasetManifest,
}

impl DatasetLedgerFixture {
    pub fn try_new(seed: DatasetLedgerSeed) -> QuantResult<Self> {
        let DatasetLedgerSeed {
            training_dataset_id,
            model_spec_id,
            model_spec_definition_hash,
            source_lineage,
            cohort_manifest,
            window_start,
            window_end,
            purpose,
            knowledge_lag_secs,
            sample_interval_secs,
            horizons_secs,
            feature_schema_version,
            sample_sources,
            feature_schema_hash,
            factor_schema_hash,
            label_schema_hash,
            semantic_dataset_hash,
            source_fingerprint,
            sample_count,
        } = seed;
        let feedback_cohort = cohort_manifest.as_ref().map(|manifest| manifest.cohort);
        let knowledge_lag_i64 =
            i64::try_from(knowledge_lag_secs).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("fixture knowledge lag overflow: {error}"),
            })?;
        let sample_interval_i64 =
            i64::try_from(sample_interval_secs).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("fixture sample interval overflow: {error}"),
            })?;
        let plan = NewTrainingDatasetPlan {
            training_dataset_id,
            model_spec_id,
            model_spec_definition_hash,
            research_profile_artifact_id: source_lineage.research_profile_artifact_id.clone(),
            source_slice_id: source_lineage.source_slice_id,
            pit_cutoff: source_lineage.pit_cutoff,
            source_lineage: source_lineage.clone(),
            feedback_cohort,
            cohort_manifest: cohort_manifest.clone(),
            window_start,
            window_end,
            purpose,
            knowledge_lag_secs: knowledge_lag_i64,
            sample_interval_secs: sample_interval_i64,
            horizons_secs: TrainingHorizonsSecs(horizons_secs.clone()),
            feature_schema_version,
            sample_sources,
            decision_policy_snapshot_id: source_lineage.decision_policy_snapshot_id,
        };
        plan.validate()
            .map_err(|error| ResearchError::DatasetBuild {
                detail: error.to_string(),
            })?;
        let manifest = DatasetManifest {
            format_version: DATASET_ARTIFACT_FORMAT_VERSION,
            training_dataset_id,
            source_lineage,
            cohort_manifest,
            model_spec_id,
            model_spec_definition_hash,
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            window_start,
            window_end,
            purpose,
            knowledge_lag_secs,
            sample_interval_secs,
            horizons_secs,
            feature_schema_hash,
            factor_schema_hash,
            label_schema_hash,
            semantic_dataset_hash,
            source_fingerprint,
            sample_count,
        };
        manifest
            .validate()
            .map_err(|error| ResearchError::DatasetBuild {
                detail: error.to_string(),
            })?;
        Ok(Self { plan, manifest })
    }

    #[must_use]
    pub fn coverage(&self) -> DatasetCoverage {
        DatasetCoverage {
            planned_samples: self.manifest.sample_count,
            built_examples: self.manifest.sample_count,
            ..DatasetCoverage::default()
        }
    }

    pub fn completion(
        &self,
        status: TrainingDatasetStatus,
        artifact_bytes_hash: ContentHash,
        parquet_uri: ArtifactUri,
        coverage: DatasetCoverage,
        failure_detail: Option<String>,
    ) -> QuantResult<CompleteTrainingDatasetBuild> {
        CompleteTrainingDatasetBuild::try_new(
            status,
            self.manifest.clone(),
            artifact_bytes_hash,
            parquet_uri,
            coverage,
            failure_detail,
        )
        .map_err(|error| ResearchError::DatasetBuild {
            detail: error.to_string(),
        })
        .map_err(Into::into)
    }
}

/// Build a fully balanced `ModelLearning` cohort for evaluation fixtures.
pub fn model_learning_cohort(
    scope: &str,
    source_lineage: &DatasetSourceLineage,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    sample_count: u64,
) -> QuantResult<DatasetCohortManifest> {
    let counts = DatasetCohortCounts::try_new(
        sample_count,
        sample_count,
        sample_count,
        Vec::new(),
        Vec::new(),
    )
    .map_err(|error| ResearchError::DatasetBuild {
        detail: error.to_string(),
    })?;
    Ok(DatasetCohortManifest {
        format_version: DATASET_COHORT_MANIFEST_FORMAT_VERSION,
        cohort: FeedbackCohort::ModelLearning,
        window: FeedbackCohortWindow::try_new(
            source_lineage.research_profile_artifact_id.profile_ref(),
            window_start,
            window_end,
        )
        .map_err(|error| ResearchError::DatasetBuild {
            detail: error.to_string(),
        })?,
        artifact: DatasetCohortArtifactRef {
            uri: ArtifactUri::parse(format!("s3://fixture/evaluation/{scope}/cohort.parquet"))?,
            bytes_hash: CanonicalDigest::content_hash_json(&("cohort-bytes", scope))?,
            schema_hash: CanonicalDigest::content_hash_json(&("cohort-schema", scope))?,
            source_hash: CanonicalDigest::content_hash_json(&("cohort-source", scope))?,
            row_count: sample_count,
        },
        counts,
        capability_registry_hashes: source_lineage.capability_registry_hashes.clone(),
    })
}

struct ReplayableSourceRecords {
    market_info: Vec<SourceSliceRecord>,
    ledger: Vec<SourceSliceRecord>,
    sessions: Vec<SourceSliceRecord>,
}

fn replayable_source_records(examples: &[TrainingExample]) -> QuantResult<ReplayableSourceRecords> {
    let mut unique = BTreeMap::new();
    for example in examples {
        unique
            .entry((example.market_id.clone(), example.token_id.clone()))
            .or_insert(example);
    }
    let mut market_info_records = Vec::with_capacity(unique.len());
    let mut ledger = Vec::with_capacity(unique.len());
    let mut sessions = Vec::with_capacity(unique.len());
    for ((market_id, token_id), example) in unique {
        let decision_at = example.decision_at();
        let event_at = example.decision_boundary.cutoff_for(DecisionSource::Book);
        let event_at_ms = event_at.timestamp_millis();
        let available_at_ms = decision_at.timestamp_millis();
        let scope = format!("{market_id}:{token_id}:{available_at_ms}");
        let stream_session_id = seeded_uuid(&format!("{scope}:stream-session"));
        let event = BookL2LedgerRow {
            stream_session_id,
            shard_id: 0,
            token_id: token_id.clone(),
            market_id: Some(market_id.clone()),
            token_sequence: 1,
            event_type: ChCanonicalBookEventType::Snapshot,
            bid_prices: vec![ChPrice::from(Price::new(dec!(0.49)))],
            bid_sizes: vec![ChShares::from(Shares::new(dec!(100)))],
            ask_prices: vec![ChPrice::from(Price::new(dec!(0.51)))],
            ask_sizes: vec![ChShares::from(Shares::new(dec!(100)))],
            old_tick_size: None,
            new_tick_size: None,
            trade_price: None,
            trade_side: None,
            trade_size: None,
            fee_rate_bps: None,
            venue_event_time: event_at_ms,
            ingress_time: available_at_ms,
            persisted_time: available_at_ms,
            event_hash: ChDigest::new([0; 32]),
            schema_version: BookL2LedgerRow::SCHEMA_VERSION,
        }
        .seal()?;
        ledger.push(source_record(
            format!("ledger:{token_id}:1"),
            event_at,
            decision_at,
            &event,
        )?);
        let session = BookStreamSessionRow {
            stream_session_id,
            shard_id: 0,
            ledger_sequence: 1,
            state: ChStreamSessionState::Open,
            end_reason: ChStreamSessionEndReason::None,
            subscription_token_hash: CanonicalDigest::content_hash_json(&token_id)?,
            subscription_token_count: 1,
            received_sequence_json: format!(r#"{{"{}":1}}"#, token_id.as_str()),
            persisted_sequence_json: format!(r#"{{"{}":1}}"#, token_id.as_str()),
            opened_at: event_at_ms,
            recorded_at: available_at_ms,
            schema_version: ChSchemaVersion(2),
        };
        sessions.push(source_record(
            format!("session:{stream_session_id}:1"),
            event_at,
            decision_at,
            &session,
        )?);
        let raw_payload = serde_json::json!({
            "market_id": market_id,
            "token_id": token_id,
            "effective_at": event_at,
            "available_at": decision_at,
        });
        let market_info = ClobMarketInfoVersion {
            version_id: ClobMarketInfoVersionId::new(seeded_uuid(&format!("{scope}:market-info"))),
            market_id: market_id.clone(),
            tokens: vec![ClobTokenDescriptor {
                token_id: token_id.clone(),
                outcome: "Yes".to_owned(),
            }],
            tick_size: TickSize::Hundredth,
            minimum_order_size: dec!(1),
            neg_risk: false,
            taker_order_delay_enabled: false,
            minimum_order_age_secs: None,
            blockaid_check_enabled: false,
            fee_details: ClobFeeDetails {
                rate: dec!(0),
                exponent: 1,
                taker_only: true,
            },
            builder_maker_fee_rate_bps: 0,
            builder_taker_fee_rate_bps: 0,
            effective_at: event_at,
            available_at: decision_at,
            payload_hash: CanonicalDigest::content_hash_json(&raw_payload)?,
            raw_payload,
        };
        market_info_records.push(source_record(
            format!("market-info:{}:{}", market_id, market_info.version_id),
            event_at,
            decision_at,
            &market_info,
        )?);
    }
    Ok(ReplayableSourceRecords {
        market_info: market_info_records,
        ledger,
        sessions,
    })
}

async fn persist_replayable_objects(
    store: &Arc<dyn ArtifactStore>,
    records: ReplayableSourceRecords,
) -> QuantResult<Vec<SourceSliceObjectRef>> {
    let mut objects = vec![
        persist_source_object(
            store,
            SourceSliceObjectKind::ClobMarketInfo,
            records.market_info,
        )
        .await?,
        persist_source_object(store, SourceSliceObjectKind::L2Ledger, records.ledger).await?,
        persist_source_object(store, SourceSliceObjectKind::L2Session, records.sessions).await?,
    ];
    objects.sort_by(|left, right| {
        (left.kind, left.uri.as_str()).cmp(&(right.kind, right.uri.as_str()))
    });
    Ok(objects)
}

fn replayable_manifest(
    fixture: &ReplayableSourceSliceFixture,
    market_count: usize,
    objects: Vec<SourceSliceObjectRef>,
) -> QuantResult<SourceSliceManifest> {
    let pit_cutoff = fixture.window_end;
    Ok(SourceSliceManifest {
        format_version: SOURCE_SLICE_MANIFEST_FORMAT_VERSION,
        profile_ref: fixture.profile_ref.clone(),
        evaluation_track: ResearchEvaluationTrack::ResearchOnly,
        research_program_hash: fixture.research_program_hash,
        window_start: fixture.window_start,
        window_end: fixture.window_end,
        pit_cutoff,
        materialized_at: pit_cutoff,
        catalog_proof: SourceSliceCatalogProof {
            base_complete_batch_id: CatalogSyncBatchId::new(seeded_uuid(
                "model-backtest-source-base",
            )),
            terminal_batch_id: CatalogSyncBatchId::new(seeded_uuid(
                "model-backtest-source-terminal",
            )),
            committed_through: fixture.window_start,
            ordered_batch_chain_hash: CanonicalDigest::content_hash_json(&(
                "model-backtest-catalog-chain",
                fixture.window_start,
                fixture.window_end,
            ))?,
            market_count: u64::try_from(market_count).map_err(|error| {
                ResearchError::DatasetBuild {
                    detail: format!("fixture market count overflow: {error}"),
                }
            })?,
            event_count: 1,
            snapshot_hash: CanonicalDigest::content_hash_json(&(
                "model-backtest-catalog-snapshot",
                fixture.window_start,
                fixture.window_end,
            ))?,
        },
        reader_contract_version: ReaderContractVersion::v1(),
        schema_contract_version: SchemaContractVersion::v1(),
        decision_policy_snapshot_id: fixture.decision_policy_snapshot_id,
        runtime_config_hash: fixture.runtime_config_hash,
        dataset_format_version: DATASET_ARTIFACT_FORMAT_VERSION,
        capability_registry_hashes: CapabilityRegistryHashes::try_new(vec![
            CanonicalDigest::content_hash_json(&(
                "fixture-capability-registry",
                &fixture.profile_ref,
            ))?,
        ])
        .map_err(|error| ResearchError::DatasetBuild {
            detail: error.to_string(),
        })?,
        pit_cutoffs: SourceSlicePitCutoffs {
            catalog_available_at: pit_cutoff,
            clob_market_info_available_at: pit_cutoff,
            l2_available_at: pit_cutoff,
            trade_tape_available_at: pit_cutoff,
            weather_available_at: None,
            calibration_available_at: None,
            resolution_available_at: pit_cutoff,
        },
        invalid_sessions: Vec::new(),
        objects,
    })
}

async fn store_source_manifest(
    store: &Arc<dyn ArtifactStore>,
    manifest: SourceSliceManifest,
) -> QuantResult<StoredSourceSlice> {
    manifest
        .validate()
        .map_err(|detail| ResearchError::DatasetBuild { detail })?;
    let manifest_bytes = CanonicalDigest::canonical_json_bytes(&manifest)?;
    let manifest_hash = CanonicalDigest::content_hash_bytes(&manifest_bytes);
    let manifest_uri = store
        .put(
            ArtifactKey::new(
                ArtifactNamespace::SourceSlice,
                format!("manifest-{}", manifest_hash.hex()),
                "json",
            )?,
            &manifest_bytes,
        )
        .await?;
    Ok(StoredSourceSlice {
        manifest_ref: SourceSliceManifestRef {
            manifest_uri,
            manifest_hash,
        },
        manifest,
    })
}

pub async fn persist_replayable_source_slice(
    store: &Arc<dyn ArtifactStore>,
    examples: &[TrainingExample],
    fixture: ReplayableSourceSliceFixture,
) -> QuantResult<StoredSourceSlice> {
    let records = replayable_source_records(examples)?;
    let objects = persist_replayable_objects(store, records).await?;
    let manifest = replayable_manifest(&fixture, examples.len(), objects)?;
    store_source_manifest(store, manifest).await
}

/// Register and complete one exact Source Slice, then derive the Dataset
/// lineage from the committed row and manifest.
pub async fn seed_source_manifest(
    db: &DatabaseConnection,
    stored: &StoredSourceSlice,
) -> QuantResult<DatasetSourceLineage> {
    PgModelRegistryRepository::new(db.clone())
        .ensure_builtin_research_profiles()
        .await?;
    let manifest = &stored.manifest;
    let identity = SourceSliceIdentity::derive(SourceSliceIdentityInput {
        profile_ref: manifest.profile_ref.clone(),
        evaluation_track: manifest.evaluation_track,
        research_program_hash: manifest.research_program_hash,
        decision_policy_snapshot_id: manifest.decision_policy_snapshot_id,
        runtime_config_hash: manifest.runtime_config_hash,
        window_start: manifest.window_start,
        window_end: manifest.window_end,
        pit_cutoff: manifest.pit_cutoff,
    })?;
    let identity_hash = identity.identity_hash;
    let repository = PgSourceSliceRepository::new(db.clone());
    let claimed = repository
        .begin_or_get(NewSourceSlice::from_identity(
            SourceSliceId::from_v7(),
            identity,
        ))
        .await?;
    let committed = repository
        .complete(
            &claimed.source_slice.source_slice_id,
            CompleteSourceSlice {
                manifest_ref: stored.manifest_ref.clone(),
                manifest: manifest.clone(),
            },
        )
        .await?;
    let source_schema_hash =
        DatasetSourceLineage::derive_schema_hash(manifest).map_err(|error| {
            ResearchError::DatasetBuild {
                detail: error.to_string(),
            }
        })?;
    Ok(DatasetSourceLineage {
        format_version: DATASET_SOURCE_LINEAGE_FORMAT_VERSION,
        source_slice_id: committed.source_slice_id,
        source_slice_identity_hash: identity_hash,
        research_profile_artifact_id: manifest.profile_ref.artifact_id(),
        research_program_hash: manifest.research_program_hash,
        source_slice: stored.manifest_ref.clone(),
        source_window_start: manifest.window_start,
        source_window_end: manifest.window_end,
        pit_cutoff: manifest.pit_cutoff,
        decision_policy_snapshot_id: manifest.decision_policy_snapshot_id,
        runtime_config_hash: manifest.runtime_config_hash,
        reader_contract_version: manifest.reader_contract_version.clone(),
        schema_contract_version: manifest.schema_contract_version.clone(),
        source_schema_hash,
        capability_registry_hashes: manifest.capability_registry_hashes.clone(),
    })
}

/// Seed a complete synthetic Source Slice for repository/core dataset tests
/// that do not consume the source object bytes.
pub async fn seed_dataset_source(
    db: &DatabaseConnection,
    input: DatasetSourceSeed,
) -> QuantResult<DatasetSourceLineage> {
    if input.window_start >= input.window_end || input.window_end > input.pit_cutoff {
        return Err(ResearchError::DatasetBuild {
            detail: "dataset source fixture requires start < end <= cutoff".to_owned(),
        }
        .into());
    }
    let research_program_hash =
        CanonicalDigest::content_hash_json(&("dataset-source-program", &input.scope))?;
    let runtime_config_hash =
        CanonicalDigest::content_hash_json(&("dataset-source-runtime", &input.scope))?;
    let capability_registry_hashes =
        CapabilityRegistryHashes::try_new(vec![CanonicalDigest::content_hash_json(&(
            "dataset-source-capability",
            &input.scope,
        ))?])
        .map_err(|error| ResearchError::DatasetBuild {
            detail: error.to_string(),
        })?;
    let kinds = [
        SourceSliceObjectKind::CatalogMarket,
        SourceSliceObjectKind::CatalogEvent,
        SourceSliceObjectKind::ClobMarketInfo,
        SourceSliceObjectKind::L2Ledger,
        SourceSliceObjectKind::L2Session,
        SourceSliceObjectKind::L2Gap,
        SourceSliceObjectKind::BookMicrostructure,
        SourceSliceObjectKind::TradeTape,
        SourceSliceObjectKind::MarketLinkage,
        SourceSliceObjectKind::DomainObservation,
        SourceSliceObjectKind::CryptoPriceReport,
        SourceSliceObjectKind::WeatherObservation,
        SourceSliceObjectKind::WeatherForecast,
        SourceSliceObjectKind::CalibrationReference,
        SourceSliceObjectKind::Resolution,
    ];
    let mut objects = kinds
        .into_iter()
        .enumerate()
        .map(|(index, kind)| {
            let identity = (&input.scope, index, kind);
            Ok(SourceSliceObjectRef {
                kind,
                uri: ArtifactUri::parse(format!(
                    "s3://fixture/source-slices/{}/{index:02}.parquet",
                    input.scope
                ))?,
                object_version: format!("fixture-{}", input.scope),
                byte_hash: CanonicalDigest::content_hash_json(&("bytes", identity))?,
                schema_hash: CanonicalDigest::content_hash_json(&("schema", identity))?,
                row_count: 1,
                min_event_at: Some(input.window_start),
                max_event_at: Some(input.window_end),
                min_available_at: Some(input.window_start),
                max_available_at: Some(input.pit_cutoff),
            })
        })
        .collect::<QuantResult<Vec<_>>>()?;
    objects.sort_by(|left, right| {
        (left.kind, left.uri.as_str()).cmp(&(right.kind, right.uri.as_str()))
    });
    let manifest = SourceSliceManifest {
        format_version: SOURCE_SLICE_MANIFEST_FORMAT_VERSION,
        profile_ref: input.profile_ref,
        evaluation_track: ResearchEvaluationTrack::ResearchOnly,
        research_program_hash,
        window_start: input.window_start,
        window_end: input.window_end,
        pit_cutoff: input.pit_cutoff,
        materialized_at: input.pit_cutoff,
        catalog_proof: SourceSliceCatalogProof {
            base_complete_batch_id: CatalogSyncBatchId::new(seeded_uuid(&format!(
                "{}:catalog-base",
                input.scope
            ))),
            terminal_batch_id: CatalogSyncBatchId::new(seeded_uuid(&format!(
                "{}:catalog-terminal",
                input.scope
            ))),
            committed_through: input.pit_cutoff,
            ordered_batch_chain_hash: CanonicalDigest::content_hash_json(&(
                "dataset-source-catalog-chain",
                &input.scope,
            ))?,
            market_count: 1,
            event_count: 1,
            snapshot_hash: CanonicalDigest::content_hash_json(&(
                "dataset-source-catalog-snapshot",
                &input.scope,
            ))?,
        },
        reader_contract_version: ReaderContractVersion::v1(),
        schema_contract_version: SchemaContractVersion::v1(),
        decision_policy_snapshot_id: input.decision_policy_snapshot_id,
        runtime_config_hash,
        dataset_format_version: DATASET_ARTIFACT_FORMAT_VERSION,
        capability_registry_hashes,
        pit_cutoffs: SourceSlicePitCutoffs {
            catalog_available_at: input.pit_cutoff,
            clob_market_info_available_at: input.pit_cutoff,
            l2_available_at: input.pit_cutoff,
            trade_tape_available_at: input.pit_cutoff,
            weather_available_at: Some(input.pit_cutoff),
            calibration_available_at: Some(input.pit_cutoff),
            resolution_available_at: input.pit_cutoff,
        },
        invalid_sessions: Vec::new(),
        objects,
    };
    manifest
        .validate()
        .map_err(|detail| ResearchError::DatasetBuild { detail })?;
    let manifest_hash = manifest
        .content_hash()
        .map_err(|detail| ResearchError::DatasetBuild { detail })?;
    let stored = StoredSourceSlice {
        manifest_ref: SourceSliceManifestRef {
            manifest_uri: ArtifactUri::parse(format!(
                "s3://fixture/source-slices/{}/manifest-{}.json",
                input.scope,
                manifest_hash.hex()
            ))?,
            manifest_hash,
        },
        manifest,
    };
    seed_source_manifest(db, &stored).await
}

/// Seed a Ready purpose-bound evaluation dataset for report-ledger tests.
pub async fn seed_evaluation_dataset(
    db: &DatabaseConnection,
    input: EvaluationDatasetSeed,
) -> QuantResult<TrainingDatasetId> {
    let dataset_id = TrainingDatasetId::from_v7();
    let source_lineage = seed_dataset_source(
        db,
        DatasetSourceSeed {
            scope: format!("{}:source", input.scope),
            profile_ref: input.profile_ref.clone(),
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            window_start: input.window_start,
            window_end: input.window_end,
            pit_cutoff: input.window_end + Duration::hours(1),
        },
    )
    .await?;
    let cohort_manifest = model_learning_cohort(
        &input.scope,
        &source_lineage,
        input.window_start,
        input.window_end,
        input.sample_count,
    )?;
    let feature_schema_hash =
        CanonicalDigest::content_hash_json(&("evaluation-feature-schema", &input.scope))?;
    let factor_schema_hash =
        CanonicalDigest::content_hash_json(&("evaluation-factor-schema", &input.scope))?;
    let label_schema_hash =
        CanonicalDigest::content_hash_json(&("evaluation-label-schema", &input.scope))?;
    let fixture = DatasetLedgerFixture::try_new(DatasetLedgerSeed {
        training_dataset_id: dataset_id,
        model_spec_id: input.model_spec_id,
        model_spec_definition_hash: input.model_spec_definition_hash,
        source_lineage,
        cohort_manifest: Some(cohort_manifest),
        window_start: input.window_start,
        window_end: input.window_end,
        purpose: DatasetPurpose::Evaluation,
        knowledge_lag_secs: 0,
        sample_interval_secs: 3_600,
        horizons_secs: vec![0],
        feature_schema_version: Some(SchemaVersion::FIRST),
        sample_sources: Some(TrainingSampleSources(default_sample_sources())),
        feature_schema_hash,
        factor_schema_hash,
        label_schema_hash,
        semantic_dataset_hash: CanonicalDigest::content_hash_json(&(
            "evaluation-dataset",
            &input.scope,
        ))?,
        source_fingerprint: CanonicalDigest::content_hash_json(&(
            "evaluation-source-fingerprint",
            &input.scope,
        ))?,
        sample_count: input.sample_count,
    })?;
    let repository = PgTrainingDatasetRepository::new(db.clone());
    repository.create_plan(fixture.plan.clone()).await?;
    repository.start_build(&dataset_id).await?;
    repository
        .complete_build(
            &dataset_id,
            fixture.completion(
                TrainingDatasetStatus::Ready,
                CanonicalDigest::content_hash_json(&("evaluation-artifact", &input.scope))?,
                ArtifactUri::parse(format!(
                    "s3://fixture/evaluation/{}/dataset.parquet",
                    input.scope
                ))?,
                fixture.coverage(),
                None,
            )?,
        )
        .await?;
    Ok(dataset_id)
}
