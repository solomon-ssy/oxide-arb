//! Canonical research fixtures shared by integration tests.

use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{
        BookL2CheckpointRow, BookL2EventRow, BookStreamSessionRow, ChPrice, ChSchemaVersion,
        ChShares,
    },
    domain::DecisionSource,
    enums::{
        catalog::CatalogTimestampQuality,
        clickhouse::{ChCanonicalBookEventType, ChStreamSessionEndReason, ChStreamSessionState},
        common::TickSize,
        market::MarketStatus,
    },
    hashing::CanonicalDigest,
    types::{
        BookSnapshotRef, BookSnapshotSource, Bps, CatalogEventChangeId, CatalogMarketChangeId,
        CatalogSyncBatchId, ClobFeeDetails, ClobMarketInfoVersion, ClobMarketInfoVersionId,
        ClobTokenDescriptor, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION,
        DecisionPolicySnapshotId, MarketContext, Price, Probability, ReaderContractVersion,
        RecommendationIdentity, ResearchEvaluationTrack, ResearchProfileRef,
        SOURCE_SLICE_MANIFEST_FORMAT_VERSION, SchemaContractVersion, Shares,
        SourceSliceCatalogProof, SourceSliceManifestRef, SourceSliceManifestV1,
        SourceSliceObjectKind, SourceSliceObjectRef, SourceSlicePitCutoffs, Usd,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    features::{CatalogDecisionRef, DecisionCaptureEvidence, DecisionSnapshotEvidence},
    source_slice::{SourceSliceParquetCodec, SourceSliceRecord},
    training::TrainingExample,
};
use rust_decimal_macros::dec;
use serde::Serialize;

use crate::seeded_uuid;

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

/// Bind a complete V1 capture to an already frozen training example.
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
        SourceSliceObjectKind::L2Event => "l2-event",
        SourceSliceObjectKind::L2Checkpoint => "l2-checkpoint",
        SourceSliceObjectKind::L2Session => "l2-session",
        _ => "source-object",
    }
}

async fn persist_source_object(
    store: &Arc<dyn ArtifactStore>,
    kind: SourceSliceObjectKind,
    records: Vec<SourceSliceRecord>,
) -> QuantResult<SourceSliceObjectRef> {
    let bytes = SourceSliceParquetCodec::encode(&records)?;
    let byte_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(&bytes))?;
    let key = ArtifactKey::new(
        ArtifactNamespace::SourceSlice,
        format!("{}-{}", source_object_slug(kind), byte_hash.hex()),
        "parquet",
    )?;
    let uri = store.put(key, &bytes).await?;
    let metadata = store.metadata(&uri).await?;
    let object_version = metadata
        .version_id
        .unwrap_or_else(|| format!("local-content:{}", byte_hash.as_str()));
    Ok(SourceSliceObjectRef {
        kind,
        uri,
        object_version,
        byte_hash,
        schema_hash: CanonicalDigest::content_hash_json(&(
            "source_slice_parquet_envelope_v1",
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
/// backtest/CPCV integration tests, then seal a canonical V1 manifest.
pub struct ReplayableSourceSliceFixture {
    pub profile_ref: ResearchProfileRef,
    pub research_program_hash: ContentHash,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub runtime_config_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
}

struct ReplayableSourceRecords {
    market_info: Vec<SourceSliceRecord>,
    checkpoints: Vec<SourceSliceRecord>,
    events: Vec<SourceSliceRecord>,
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
    let mut checkpoints = Vec::with_capacity(unique.len());
    let mut events = Vec::with_capacity(unique.len());
    let mut sessions = Vec::with_capacity(unique.len());
    for ((market_id, token_id), example) in unique {
        let decision_at = example.decision_at();
        let event_at = example.decision_boundary.cutoff_for(DecisionSource::Book);
        let event_at_ms = event_at.timestamp_millis();
        let available_at_ms = decision_at.timestamp_millis();
        let scope = format!("{market_id}:{token_id}:{available_at_ms}");
        let stream_session_id = seeded_uuid(&format!("{scope}:stream-session"));
        let source_event_hash = source_hash(example, "replay-book-event");
        let checkpoint = BookL2CheckpointRow {
            token_id: token_id.clone(),
            market_id: Some(market_id.clone()),
            stream_session_id,
            token_sequence: 1,
            bids_json: r#"[["0.49","100"]]"#.to_owned(),
            asks_json: r#"[["0.51","100"]]"#.to_owned(),
            book_version: 1,
            source_event_hash: source_event_hash.clone(),
            checkpoint_hash: source_hash(example, "replay-checkpoint"),
            event_time: event_at_ms,
            created_at: available_at_ms,
            schema_version: ChSchemaVersion(2),
        };
        checkpoints.push(source_record(
            format!("checkpoint:{token_id}:{event_at_ms}"),
            event_at,
            decision_at,
            &checkpoint,
        )?);
        let event = BookL2EventRow {
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
            book_version: 1,
            old_tick_size: None,
            new_tick_size: None,
            venue_event_time: event_at_ms,
            ingress_time: available_at_ms,
            persisted_time: available_at_ms,
            payload_hash: source_event_hash,
            schema_version: ChSchemaVersion(2),
        };
        events.push(source_record(
            format!("event:{token_id}:1"),
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
        checkpoints,
        events,
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
        persist_source_object(store, SourceSliceObjectKind::L2Event, records.events).await?,
        persist_source_object(
            store,
            SourceSliceObjectKind::L2Checkpoint,
            records.checkpoints,
        )
        .await?,
        persist_source_object(store, SourceSliceObjectKind::L2Session, records.sessions).await?,
    ];
    objects.sort_by(|left, right| {
        (left.kind, left.uri.as_str()).cmp(&(right.kind, right.uri.as_str()))
    });
    Ok(objects)
}

fn replayable_manifest(
    fixture: ReplayableSourceSliceFixture,
    market_count: usize,
    objects: Vec<SourceSliceObjectRef>,
) -> QuantResult<SourceSliceManifestV1> {
    let pit_cutoff = fixture.window_end;
    Ok(SourceSliceManifestV1 {
        format_version: SOURCE_SLICE_MANIFEST_FORMAT_VERSION,
        profile_ref: fixture.profile_ref,
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
        reader_contract_version: ReaderContractVersion::parse("source-slice-reader-v2")
            .expect("valid fixture reader contract version"),
        schema_contract_version: SchemaContractVersion::parse("source-slice-schema-v1")
            .expect("valid fixture schema contract version"),
        decision_policy_snapshot_id: fixture.decision_policy_snapshot_id,
        runtime_config_hash: fixture.runtime_config_hash,
        dataset_format_version: DATASET_ARTIFACT_FORMAT_VERSION,
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

async fn persist_source_manifest(
    store: &Arc<dyn ArtifactStore>,
    manifest: SourceSliceManifestV1,
) -> QuantResult<SourceSliceManifestRef> {
    manifest
        .validate()
        .map_err(|detail| ResearchError::DatasetBuild { detail })?;
    let manifest_bytes = CanonicalDigest::canonical_json_bytes(&manifest)?;
    let manifest_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(&manifest_bytes))?;
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
    Ok(SourceSliceManifestRef {
        manifest_uri,
        manifest_hash,
    })
}

pub async fn persist_replayable_source_slice(
    store: &Arc<dyn ArtifactStore>,
    examples: &[TrainingExample],
    fixture: ReplayableSourceSliceFixture,
) -> QuantResult<SourceSliceManifestRef> {
    let records = replayable_source_records(examples)?;
    let objects = persist_replayable_objects(store, records).await?;
    let manifest = replayable_manifest(fixture, examples.len(), objects)?;
    persist_source_manifest(store, manifest).await
}
