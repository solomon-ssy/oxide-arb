//! Canonical research fixtures shared by integration tests.

use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, BookStreamSessionRow, ChDigest, ChPrice,
        ChSchemaVersion, ChShares, ChUsd, MarketResolutionFactInput, MarketResolutionRow,
    },
    domain::{
        data_plane::{
            CreateHistoryFitSeal, DecisionSource, ExchangeHistoryChunkStatus,
            ExchangeHistoryContinuityBasis, ExchangeHistoryFrontier, HistorySealChunkRef,
            NewExchangeHistoryChunk, NewExchangeHistoryPlan, NewHistoryFitSeal,
        },
        market::{
            CATALOG_OBJECT_SCHEMA_VERSION, CatalogEventChangeInfo, CatalogMarketChangeInfo,
            EventRegistryInfo, MarketMakerRebateEvidence, MarketRegistryInfo,
        },
        quant::{
            CompleteSourceSlice, CompleteTrainingDatasetBuild, FeedbackCohortWindow,
            NewSourceSlice, NewTrainingDatasetPlan, SourceSliceIdentity, SourceSliceIdentityInput,
        },
    },
    enums::{
        catalog::{CatalogChangeType, CatalogFilterReasonSet, CatalogTimestampQuality},
        clickhouse::{ChCanonicalBookEventType, ChStreamSessionEndReason, ChStreamSessionState},
        common::{CategorySet, TickSize},
        factor::{FactorFamily, FactorNormalization},
        market::{EventStatus, MarketStatus},
        model::ModelFamily,
        quant::{DatasetPurpose, FeedbackCohort, TrainingDatasetStatus},
    },
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, BookSnapshotRef, BookSnapshotSource, Bps, CapabilityRegistryHashes,
        CatalogDecisionRef, CatalogEventChangeId, CatalogEventObjectId, CatalogMarketChangeId,
        CatalogMarketObjectId, CatalogSyncBatchId, ClobFeeDetails, ClobMarketInfoVersion,
        ClobMarketInfoVersionId, ClobTokenDescriptor, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION,
        DATASET_COHORT_MANIFEST_FORMAT_VERSION, DATASET_SOURCE_LINEAGE_FORMAT_VERSION,
        DatasetCohortArtifactRef, DatasetCohortCounts, DatasetCohortManifest, DatasetCoverage,
        DatasetManifest, DatasetSourceLineage, DecisionCaptureEvidence, DecisionPolicySnapshotId,
        DecisionSnapshotEvidence, EvmBlockHash, EvmTransactionHash, FeatureValue,
        FinalizedExecutionEvidence, MarketContext, ModelSpecId, PayoutRatio, Price, Probability,
        ReaderContractVersion, RecommendationIdentity, ResearchEvaluationTrack,
        ResearchProfileArtifact, ResearchProfileRef, SOURCE_SLICE_MANIFEST_FORMAT_VERSION,
        SchemaContractVersion, SchemaVersion, Shares, SourceSliceCatalogProof, SourceSliceId,
        SourceSliceManifest, SourceSliceManifestRef, SourceSliceObjectKind, SourceSliceObjectRef,
        SourceSlicePitCutoff, TokenId, TrainingDatasetId, TrainingHorizonsSecs,
        TrainingSampleSources, Usd,
        backtest::{
            BacktestPortfolioFunnel, CscvSelectionEvidence, CscvTrialDescriptor,
            CscvTrialGridBinding,
        },
        factor::{
            FactorAlphaOrientation, FactorComputationContract, FactorDefinitionDocument,
            FactorDefinitionRef, FactorOutputSemantics, FactorServingPlane,
        },
        stable_name::FactorName,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgExchangeHistoryRepository, PgModelRegistryRepository, PgPolicyRepository,
        PgSourceSliceRepository, PgTrainingDatasetRepository,
    },
    traits::{
        ExchangeHistoryRepository, ModelRegistryRepository, PolicyRepository,
        SourceSliceRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    features::names::book::{BEST_ASK, MID},
    source_slice::{SourceSliceParquetCodec, SourceSliceRecord},
    training::TrainingExample,
    validation::{TrialPerformanceMatrix, analyze_selection_bias},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;
use serde::Serialize;

use super::seeded_uuid;

const SOURCE_FIXTURE_BLOCK: i64 = 1;
const SOURCE_FIXTURE_CHAIN_ID: i64 = 137;

fn source_fit_plan(created_at: DateTime<Utc>) -> NewExchangeHistoryPlan {
    NewExchangeHistoryPlan {
        plan_id: seeded_uuid("source-slice-history-plan"),
        chain_id: SOURCE_FIXTURE_CHAIN_ID,
        policy_hash: ContentHash::from_bytes([0x41; 32]),
        bootstrap_profile_set_hash: ContentHash::from_bytes([0x42; 32]),
        finalized_anchor_block: 3,
        finalized_anchor_hash: EvmBlockHash::parse(format!("0x{}", "43".repeat(32)))
            .expect("fixture finalized anchor hash"),
        finalized_anchor_timestamp: 1,
        activation_from_block: 2,
        activation_through_block: 2,
        crypto_required_from_block: 2,
        weather_required_from_block: 1,
        retention_from_block: SOURCE_FIXTURE_BLOCK,
        retention_through_block: SOURCE_FIXTURE_BLOCK,
        created_at,
    }
}

fn source_fit_command(
    profile_ref: &ResearchProfileRef,
    research_program_hash: ContentHash,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    created_at: DateTime<Utc>,
) -> QuantResult<CreateHistoryFitSeal> {
    let fit_seal_id = seeded_uuid(&format!(
        "source-slice-fit-seal:{}:{}:{}:{}",
        profile_ref.artifact_id(),
        research_program_hash,
        window_start.timestamp_micros(),
        window_end.timestamp_micros(),
    ))
    .into();
    let chunk_id = seeded_uuid(&format!("source-slice-fit-chunk:{fit_seal_id}"));
    let chunks = vec![HistorySealChunkRef {
        chunk_id,
        frontier: ExchangeHistoryFrontier::Retention,
        state_revision: 1,
        from_block: SOURCE_FIXTURE_BLOCK,
        to_block: SOURCE_FIXTURE_BLOCK,
    }];
    let plan = source_fit_plan(created_at);
    let mut command = CreateHistoryFitSeal {
        seal: NewHistoryFitSeal {
            fit_seal_id,
            seal_hash: ContentHash::from_bytes([0; 32]),
            plan_id: plan.plan_id,
            window_from_block: SOURCE_FIXTURE_BLOCK,
            window_to_block: SOURCE_FIXTURE_BLOCK,
            policy_hash: plan.policy_hash,
            profile_hash: profile_ref.content_hash,
            cohort_hash: research_program_hash,
            created_at,
        },
        chunks,
    };
    command.seal.seal_hash = command.derive_hash()?;
    Ok(command)
}

async fn seed_source_fit_seal(
    db: &DatabaseConnection,
    manifest: &SourceSliceManifest,
) -> QuantResult<()> {
    let command = source_fit_command(
        &manifest.profile_ref,
        manifest.research_program_hash,
        manifest.window_start,
        manifest.window_end,
        manifest.materialized_at,
    )?;
    if command.seal.fit_seal_id != manifest.fit_seal_id
        || command.seal.seal_hash != manifest.fit_seal_hash
    {
        return Err(ResearchError::DatasetBuild {
            detail: "Source Slice fixture FitSeal binding does not match its window".to_owned(),
        }
        .into());
    }
    let repository = PgExchangeHistoryRepository::new(db.clone());
    repository
        .create_or_load_plan(source_fit_plan(manifest.materialized_at))
        .await?;
    let chunk = &command.chunks[0];
    let digest = ContentHash::from_bytes([0x44; 32]);
    let block_hash =
        EvmBlockHash::parse(format!("0x{}", "45".repeat(32))).expect("fixture accepted block hash");
    repository
        .save_chunk(NewExchangeHistoryChunk {
            chunk_id: chunk.chunk_id,
            frontier: chunk.frontier,
            from_block: chunk.from_block,
            to_block: chunk.to_block,
            status: ExchangeHistoryChunkStatus::Accepted,
            attempt_count: 1,
            hypersync_count: Some(0),
            attestor_count: Some(0),
            hypersync_digest: Some(digest),
            attestor_digest: Some(digest),
            first_block_hash: Some(block_hash.clone()),
            last_block_hash: Some(block_hash.clone()),
            archive_height: Some(3),
            continuity_basis: Some(ExchangeHistoryContinuityBasis::HyperSyncBoundaryHeaders),
            continuity_block: Some(0),
            continuity_hash: Some(block_hash),
            effective_through_at: Some(manifest.window_end),
            state_revision: Some(chunk.state_revision),
            accepted_at: Some(manifest.materialized_at),
            created_at: manifest.materialized_at,
            updated_at: manifest.materialized_at,
        })
        .await?;
    repository.create_fit_seal(command).await?;
    Ok(())
}

/// Build a complete deterministic CSCV fixture from synchronous OOS trial columns.
///
/// The returned methodology binding and evidence are produced by the same
/// production algorithm used by CPCV; tests never hand-author PBO or DSR
/// dispersion scalars.
pub fn cscv_selection_fixture(
    scope: &str,
    periods: &[DateTime<Utc>],
    trial_columns: &[Vec<Decimal>],
    block_count: u32,
) -> (CscvTrialGridBinding, CscvSelectionEvidence) {
    let descriptors = trial_columns
        .iter()
        .enumerate()
        .map(|(trial_id, _)| CscvTrialDescriptor {
            trial_id: u32::try_from(trial_id).expect("fixture CSCV trial id fits u32"),
            label: format!("{scope}-trial-{trial_id}"),
            config_hash: CanonicalDigest::content_hash_typed(
                "quant-pivot/test-cscv-trial-config",
                1,
                &(scope, trial_id),
            )
            .expect("fixture CSCV trial hash"),
        })
        .collect();
    let grid = CscvTrialGridBinding::try_new(block_count, descriptors)
        .expect("valid fixture CSCV trial grid");
    let matrix = TrialPerformanceMatrix::from_columns(periods.to_vec(), trial_columns)
        .expect("valid fixture CSCV performance matrix");
    let evidence =
        analyze_selection_bias(&matrix, &grid).expect("valid fixture CSCV selection evidence");
    (grid, evidence)
}

/// Build a count-conserving fixture where every emitted candidate yields one
/// admitted tier, is selected, executes, and resolves.
pub fn fully_resolved_backtest_funnel(
    decision_tick_count: u64,
    allocation_count: u64,
) -> BacktestPortfolioFunnel {
    assert!(
        allocation_count == 0 || decision_tick_count <= allocation_count,
        "a selected decision tick requires at least one allocation"
    );
    let no_candidate_tick_count = if allocation_count == 0 {
        decision_tick_count
    } else {
        0
    };
    let selected_tick_count = if allocation_count == 0 {
        0
    } else {
        decision_tick_count
    };
    let funnel = BacktestPortfolioFunnel {
        schema_version: 1,
        decision_tick_count,
        emitted_candidate_count: allocation_count,
        candidate_without_executable_tier_count: 0,
        executable_tier_count: allocation_count,
        admission_rejected_tier_count: 0,
        admitted_tier_count: allocation_count,
        selected_tier_count: allocation_count,
        executed_entry_count: allocation_count,
        resolved_allocation_count: allocation_count,
        no_candidate_tick_count,
        no_executable_tier_tick_count: 0,
        no_selection_tick_count: 0,
        selected_tick_count,
        tier_exclusion_reasons: Vec::new(),
    };
    funnel.validate().expect("valid backtest fixture funnel");
    funnel
}

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

#[derive(Clone, Copy)]
struct FixtureBookTop {
    bid: Price,
    ask: Price,
    mid: Price,
    depth_usd: Usd,
}

fn feature_probability(example: &TrainingExample, name: &str) -> Option<Decimal> {
    example
        .feature_vector
        .generic
        .iter()
        .find(|(feature, _)| feature.as_str() == name)
        .and_then(|(_, cell)| cell.value.as_ref())
        .and_then(|value| match value {
            FeatureValue::Probability(probability) => Some(probability.inner()),
            _ => None,
        })
}

fn fixture_book_top(example: &TrainingExample) -> FixtureBookTop {
    let mid = feature_probability(example, MID.as_str())
        .expect("replayable fixture requires an observed mid probability");
    let ask = feature_probability(example, BEST_ASK.as_str()).unwrap_or(mid + dec!(0.01));
    let half_spread = ask - mid;
    let bid = mid - half_spread;
    assert!(
        dec!(0.01) < bid && bid < ask && ask < Decimal::ONE,
        "replayable fixture book must satisfy 0.01 < bid < ask < 1"
    );
    FixtureBookTop {
        bid: Price::new(bid),
        ask: Price::new(ask),
        mid: Price::new(mid),
        depth_usd: example
            .selected_market
            .liquidity_usd
            .unwrap_or_else(|| Usd::new(dec!(100))),
    }
}

fn token_book_top(example: &TrainingExample, token_id: &TokenId) -> FixtureBookTop {
    let yes = fixture_book_top(example);
    if token_id == &example.selected_market.primary_token_id {
        return yes;
    }
    assert_eq!(
        example.selected_market.secondary_token_id.as_ref(),
        Some(token_id),
        "replayable fixture requested an unknown market token"
    );
    FixtureBookTop {
        bid: Price::new(Decimal::ONE - yes.ask.inner()),
        ask: Price::new(Decimal::ONE - yes.bid.inner()),
        mid: Price::new(Decimal::ONE - yes.mid.inner()),
        depth_usd: yes.depth_usd,
    }
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
    let top = fixture_book_top(example);
    let spread_bps = (top.ask.inner() - top.bid.inner()) * dec!(10000) / top.mid.inner();
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
        finalized_execution_evidence: FinalizedExecutionEvidence::not_required(),
        identity: RecommendationIdentity {
            category: example.selected_market.category,
            question: "Fixture market?".to_owned(),
            outcome_name: "Yes".to_owned(),
        },
        market_context: MarketContext {
            best_bid: Some(top.bid),
            best_ask: Some(top.ask),
            mid_price: Some(top.mid),
            spread_bps: Some(Bps::new(spread_bps)),
            depth_usd: top.depth_usd,
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
        SourceSliceObjectKind::CatalogMarket => "catalog-market",
        SourceSliceObjectKind::GammaMarketIdentity => "gamma-market-identity",
        SourceSliceObjectKind::CatalogEvent => "catalog-event",
        SourceSliceObjectKind::ClobMarketInfo => "clob-market-info",
        SourceSliceObjectKind::L2Ledger => "l2-ledger",
        SourceSliceObjectKind::L2Session => "l2-session",
        SourceSliceObjectKind::L2Gap => "l2-gap",
        SourceSliceObjectKind::BookMicrostructure => "book-microstructure",
        SourceSliceObjectKind::MarketExecution => "market-execution",
        SourceSliceObjectKind::ExecutionParticipant => "execution-participant",
        SourceSliceObjectKind::MarketLinkage => "market-linkage",
        SourceSliceObjectKind::DomainObservation => "domain-observation",
        SourceSliceObjectKind::CryptoPriceReport => "crypto-price-report",
        SourceSliceObjectKind::WeatherObservation => "weather-observation",
        SourceSliceObjectKind::WeatherForecast => "weather-forecast",
        SourceSliceObjectKind::CalibrationReference => "calibration-reference",
        SourceSliceObjectKind::Resolution => "resolution",
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
    pub evaluation_track: ResearchEvaluationTrack,
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
    pub source_training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub model_spec_definition_hash: ContentHash,
    pub profile_ref: ResearchProfileRef,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub sample_count: u64,
}

struct EvaluationDatasetSource {
    model_family: ModelFamily,
    factor_serving_plane: FactorServingPlane,
    source_manifest: SourceSliceManifest,
    knowledge_lag_secs: u64,
    sample_interval_secs: u64,
    horizons_secs: Vec<u64>,
    feature_schema_version: SchemaVersion,
    sample_sources: TrainingSampleSources,
    feature_schema_hash: ContentHash,
    label_schema_hash: ContentHash,
}

impl EvaluationDatasetSource {
    async fn load(db: &DatabaseConnection, input: &EvaluationDatasetSeed) -> QuantResult<Self> {
        let training_dataset = PgTrainingDatasetRepository::new(db.clone())
            .find_by_id(&input.source_training_dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_training_dataset",
                id: input.source_training_dataset_id.to_string(),
            })?;
        let materialization =
            training_dataset
                .materialization()
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: format!(
                        "source Training Dataset {} is not fully materialized",
                        input.source_training_dataset_id
                    ),
                })?;
        if training_dataset.status != TrainingDatasetStatus::Ready
            || training_dataset.purpose != DatasetPurpose::Training
            || training_dataset.model_spec_id != input.model_spec_id
            || training_dataset.model_spec_definition_hash != input.model_spec_definition_hash
            || training_dataset.research_profile_artifact_id != input.profile_ref.artifact_id()
            || training_dataset.decision_policy_snapshot_id != input.decision_policy_snapshot_id
        {
            return Err(ResearchError::DatasetBuild {
                detail: "evaluation Dataset source does not match the exact model/profile/policy"
                    .to_owned(),
            }
            .into());
        }
        if materialization.manifest.trade_policy_artifact_id.is_some() {
            return Err(ResearchError::DatasetBuild {
                detail: "evaluation fixture does not support a trade-policy-bound source Dataset"
                    .to_owned(),
            }
            .into());
        }
        let source_slice = PgSourceSliceRepository::new(db.clone())
            .find_by_id(&training_dataset.source_slice_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_source_slice",
                id: training_dataset.source_slice_id.to_string(),
            })?;
        let source_manifest = source_slice
            .manifest
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: "source Training Dataset has no complete Source Slice manifest".to_owned(),
            })?;
        let model_spec = PgModelRegistryRepository::new(db.clone())
            .find_model_spec(&input.model_spec_id)
            .await?
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: format!("evaluation model spec {} is missing", input.model_spec_id),
            })?;
        if model_spec.definition_hash != input.model_spec_definition_hash {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "evaluation model spec {} definition hash changed",
                    input.model_spec_id
                ),
            }
            .into());
        }
        let knowledge_lag_secs =
            u64::try_from(training_dataset.knowledge_lag_secs).map_err(|error| {
                ResearchError::DatasetBuild {
                    detail: format!("source Dataset knowledge lag is invalid: {error}"),
                }
            })?;
        let sample_interval_secs =
            u64::try_from(training_dataset.sample_interval_secs).map_err(|error| {
                ResearchError::DatasetBuild {
                    detail: format!("source Dataset sample interval is invalid: {error}"),
                }
            })?;
        let sample_sources =
            training_dataset
                .sample_sources
                .clone()
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: "source Training Dataset has no frozen sample-source contract"
                        .to_owned(),
                })?;
        Ok(Self {
            model_family: model_spec.model_family,
            factor_serving_plane: materialization.factor_serving_plane.clone(),
            source_manifest,
            knowledge_lag_secs,
            sample_interval_secs,
            horizons_secs: training_dataset.horizons_secs.0.clone(),
            feature_schema_version: training_dataset.feature_schema_version,
            sample_sources,
            feature_schema_hash: *materialization.feature_schema_hash,
            label_schema_hash: *materialization.label_schema_hash,
        })
    }

    fn ledger(
        self,
        input: &EvaluationDatasetSeed,
        dataset_id: TrainingDatasetId,
        source_lineage: DatasetSourceLineage,
        cohort_manifest: DatasetCohortManifest,
    ) -> QuantResult<DatasetLedgerFixture> {
        let semantic_dataset_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot:test:evaluation-dataset",
            1,
            &(
                dataset_id,
                input.model_spec_id,
                input.model_spec_definition_hash,
                source_lineage.source_slice_identity_hash,
                input.window_start,
                input.window_end,
                self.factor_serving_plane.factor_schema_hash(),
                self.feature_schema_hash,
                self.label_schema_hash,
                input.sample_count,
            ),
        )?;
        let source_fingerprint = CanonicalDigest::content_hash_typed(
            "quant-pivot:test:evaluation-source-fingerprint",
            1,
            &(
                source_lineage.source_slice_identity_hash,
                cohort_manifest.artifact.source_hash,
                input.sample_count,
            ),
        )?;
        DatasetLedgerFixture::try_new(DatasetLedgerSeed {
            training_dataset_id: dataset_id,
            model_spec_id: input.model_spec_id,
            model_family: self.model_family,
            model_spec_definition_hash: input.model_spec_definition_hash,
            factor_serving_plane: self.factor_serving_plane,
            source_lineage,
            cohort_manifest: Some(cohort_manifest),
            window_start: input.window_start,
            window_end: input.window_end,
            purpose: DatasetPurpose::Evaluation,
            knowledge_lag_secs: self.knowledge_lag_secs,
            sample_interval_secs: self.sample_interval_secs,
            horizons_secs: self.horizons_secs,
            feature_schema_version: self.feature_schema_version,
            sample_sources: Some(self.sample_sources),
            feature_schema_hash: self.feature_schema_hash,
            label_schema_hash: self.label_schema_hash,
            semantic_dataset_hash,
            source_fingerprint,
            sample_count: input.sample_count,
        })
    }
}

/// Build one valid factor-native serving plane for persistence fixtures.
pub fn fixture_factor_plane(
    feature_schema_hash: ContentHash,
    feature_schema_version: SchemaVersion,
) -> QuantResult<FactorServingPlane> {
    let definition = FactorDefinitionDocument {
        name: FactorName::new("liquidity_depth"),
        family: FactorFamily::Momentum,
        input_features: Vec::new(),
        output: FactorOutputSemantics::OutcomeAlpha {
            orientation: FactorAlphaOrientation::CanonicalYes,
        },
        normalization: FactorNormalization::Rank,
        owner: "system-tests".to_owned(),
        required: false,
        computation: FactorComputationContract {
            semantic_version: 1,
            semantic_key: format!(
                "quant-pivot/system-test-factor-{}@1",
                feature_schema_hash.hex()
            ),
        },
    };
    let revision = FactorDefinitionRef::try_seal(
        definition,
        feature_schema_hash,
        feature_schema_version,
        SchemaVersion::FIRST,
    )
    .map_err(|error| ResearchError::DatasetBuild {
        detail: format!("fixture factor revision is invalid: {error}"),
    })?;
    FactorServingPlane::try_seal(vec![revision])
        .map_err(|error| ResearchError::DatasetBuild {
            detail: format!("fixture factor plane is invalid: {error}"),
        })
        .map_err(Into::into)
}

/// Complete immutable inputs for one Dataset v3 plan/manifest fixture.
pub struct DatasetLedgerSeed {
    pub training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub model_family: ModelFamily,
    pub model_spec_definition_hash: ContentHash,
    pub factor_serving_plane: FactorServingPlane,
    pub source_lineage: DatasetSourceLineage,
    pub cohort_manifest: Option<DatasetCohortManifest>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub purpose: DatasetPurpose,
    pub knowledge_lag_secs: u64,
    pub sample_interval_secs: u64,
    pub horizons_secs: Vec<u64>,
    pub feature_schema_version: SchemaVersion,
    pub sample_sources: Option<TrainingSampleSources>,
    pub feature_schema_hash: ContentHash,
    pub label_schema_hash: ContentHash,
    pub semantic_dataset_hash: ContentHash,
    pub source_fingerprint: ContentHash,
    pub sample_count: u64,
}

/// Canonical matching Dataset v3 plan and manifest.
pub struct DatasetLedgerFixture {
    pub plan: NewTrainingDatasetPlan,
    pub manifest: DatasetManifest,
}

impl DatasetLedgerFixture {
    pub fn try_new(seed: DatasetLedgerSeed) -> QuantResult<Self> {
        let DatasetLedgerSeed {
            training_dataset_id,
            model_spec_id,
            model_family,
            model_spec_definition_hash,
            factor_serving_plane,
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
            model_family,
            model_spec_definition_hash,
            factor_schema_hash: factor_serving_plane.factor_schema_hash(),
            factor_serving_plane: factor_serving_plane.clone(),
            feature_schema_hash,
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
            model_family,
            model_spec_definition_hash,
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            window_start,
            window_end,
            purpose,
            knowledge_lag_secs,
            sample_interval_secs,
            horizons_secs,
            feature_schema_version,
            feature_schema_hash,
            factor_serving_plane,
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

#[derive(Default)]
struct ReplayableSourceRecords {
    catalog_markets: Vec<SourceSliceRecord>,
    catalog_events: Vec<SourceSliceRecord>,
    market_info: Vec<SourceSliceRecord>,
    ledger: Vec<SourceSliceRecord>,
    sessions: Vec<SourceSliceRecord>,
    gaps: Vec<SourceSliceRecord>,
    microstructure: Vec<SourceSliceRecord>,
    executions: Vec<SourceSliceRecord>,
    participants: Vec<SourceSliceRecord>,
    resolutions: Vec<SourceSliceRecord>,
}

struct ReplaySourceContext<'a> {
    index: usize,
    example: &'a TrainingExample,
    catalog: &'a CatalogDecisionRef,
    decision_at: DateTime<Utc>,
    event_at: DateTime<Utc>,
    event_at_ms: i64,
    available_at_ms: i64,
    downside_at: DateTime<Utc>,
    downside_at_ms: i64,
    scope: String,
    no_token_id: TokenId,
}

impl<'a> ReplaySourceContext<'a> {
    fn try_new(
        index: usize,
        example: &'a TrainingExample,
        downside_horizon_secs: u64,
        source_window_end: DateTime<Utc>,
    ) -> QuantResult<Self> {
        let decision_at = example.decision_at();
        let event_at = example.decision_boundary.cutoff_for(DecisionSource::Book);
        let available_at_ms = decision_at.timestamp_millis();
        let downside_horizon_secs =
            i64::try_from(downside_horizon_secs).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("replayable Source Slice downside horizon overflow: {error}"),
            })?;
        let downside_at = decision_at
            .checked_add_signed(Duration::seconds(downside_horizon_secs))
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: "replayable Source Slice downside timestamp overflow".to_owned(),
            })?;
        if downside_at > source_window_end {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "replayable Source Slice ends at {source_window_end} before the downside horizon {downside_at}"
                ),
            }
            .into());
        }
        let market_id = &example.market_id;
        let token_id = &example.token_id;
        let no_token_id = example
            .selected_market
            .secondary_token_id
            .clone()
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: format!(
                    "replayable Source Slice market {market_id} has no canonical secondary token"
                ),
            })?;
        let capture =
            example
                .decision_capture
                .as_ref()
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: format!(
                        "replayable Source Slice example {} has no decision capture",
                        example.example_id
                    ),
                })?;

        Ok(Self {
            index,
            example,
            catalog: &capture.snapshot.catalog,
            decision_at,
            event_at,
            event_at_ms: event_at.timestamp_millis(),
            available_at_ms,
            downside_at,
            downside_at_ms: downside_at.timestamp_millis(),
            scope: format!("{market_id}:{token_id}:{available_at_ms}"),
            no_token_id,
        })
    }
}

impl ReplayableSourceRecords {
    fn push(
        &mut self,
        index: usize,
        example: &TrainingExample,
        downside_horizon_secs: u64,
        source_window_end: DateTime<Utc>,
    ) -> QuantResult<()> {
        let context =
            ReplaySourceContext::try_new(index, example, downside_horizon_secs, source_window_end)?;
        self.push_catalog(&context)?;
        self.push_book(&context)?;
        self.push_market_info(&context)?;
        self.push_resolution(&context)
    }

    fn push_catalog(&mut self, context: &ReplaySourceContext<'_>) -> QuantResult<()> {
        let example = context.example;
        let market_id = &example.market_id;
        let token_id = &example.token_id;
        let catalog = context.catalog;
        let event = EventRegistryInfo {
            event_id: example.selected_market.event_id.clone(),
            title: "Train/backtest Source Slice fixture".to_owned(),
            slug: format!("fixture-event-{}", context.index),
            series_slug: None,
            status: EventStatus::Active,
            market_ids: vec![market_id.clone()],
            categories: CategorySet::from(example.selected_market.category),
            tags: Vec::new(),
            neg_risk: false,
            end_date: None,
            created_at: catalog.event_effective_at,
            updated_at: catalog.event_effective_at,
        };
        let event_change = CatalogEventChangeInfo {
            event_change_id: catalog.event_change_id,
            catalog_sync_batch_id: catalog.catalog_sync_batch_id,
            event_object_id: CatalogEventObjectId::from_content_hash(&catalog.event_content_hash),
            event_id: example.selected_market.event_id.clone(),
            source_effective_at: catalog.event_effective_at,
            source_timestamp_quality: catalog.event_timestamp_quality,
            available_at: catalog.event_available_at,
            change_type: CatalogChangeType::GammaScanUpsert,
            content_hash: catalog.event_content_hash,
            schema_version: CATALOG_OBJECT_SCHEMA_VERSION,
            payload: serde_json::to_value(event)
                .map_err(|error| ResearchError::DatasetBuild {
                    detail: format!("fixture catalog event serialization failed: {error}"),
                })?
                .into(),
            created_at: catalog.event_available_at,
        };
        self.catalog_events.push(source_record(
            format!("catalog-event:{}", event_change.event_change_id),
            event_change.source_effective_at,
            event_change.available_at,
            &event_change,
        )?);

        let top = fixture_book_top(example);
        let market = MarketRegistryInfo {
            market_id: market_id.clone(),
            event_id: example.selected_market.event_id.clone(),
            token_yes: token_id.clone(),
            token_no: context.no_token_id.clone(),
            question: "Train/backtest fixture market?".to_owned(),
            slug: format!("fixture-market-{}", context.index),
            description: None,
            categories: CategorySet::from(example.selected_market.category),
            status: MarketStatus::Active,
            filter_reasons: CatalogFilterReasonSet::default(),
            outcome: None,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: Vec::new(),
            best_bid: Some(top.bid.inner()),
            best_ask: Some(top.ask.inner()),
            depth_usd: example.selected_market.liquidity_usd,
            min_order_size: dec!(1),
            liquidity_usd: example.selected_market.liquidity_usd,
            volume_24h: example.selected_market.volume_24h_usd,
            maker_rebate_evidence: MarketMakerRebateEvidence::source_unavailable(),
            start_date: None,
            end_date: None,
            resolved_at: None,
            created_at: Some(catalog.market_effective_at),
            updated_at: catalog.market_effective_at,
        };
        let market_change = CatalogMarketChangeInfo {
            market_change_id: catalog.market_change_id,
            catalog_sync_batch_id: catalog.catalog_sync_batch_id,
            event_change_id: catalog.event_change_id,
            market_object_id: CatalogMarketObjectId::from_content_hash(
                &catalog.market_content_hash,
            ),
            market_id: market_id.clone(),
            event_id: example.selected_market.event_id.clone(),
            source_effective_at: catalog.market_effective_at,
            source_timestamp_quality: catalog.market_timestamp_quality,
            source_created_at: Some(catalog.market_effective_at),
            available_at: catalog.market_available_at,
            change_type: CatalogChangeType::GammaScanUpsert,
            content_hash: catalog.market_content_hash,
            schema_version: CATALOG_OBJECT_SCHEMA_VERSION,
            payload: serde_json::to_value(market)
                .map_err(|error| ResearchError::DatasetBuild {
                    detail: format!("fixture catalog market serialization failed: {error}"),
                })?
                .into(),
            created_at: catalog.market_available_at,
        };
        self.catalog_markets.push(source_record(
            format!("catalog-market:{}", market_change.market_change_id),
            market_change.source_effective_at,
            market_change.available_at,
            &market_change,
        )?);
        Ok(())
    }

    fn push_book(&mut self, context: &ReplaySourceContext<'_>) -> QuantResult<()> {
        self.push_token_book(context, &context.example.token_id)?;
        self.push_token_book(context, &context.no_token_id)
    }

    fn push_token_book(
        &mut self,
        context: &ReplaySourceContext<'_>,
        token_id: &TokenId,
    ) -> QuantResult<()> {
        let example = context.example;
        let market_id = &example.market_id;
        let top = token_book_top(example, token_id);
        let bid_size = Shares::new(top.depth_usd.inner() / top.bid.inner());
        let ask_size = Shares::new(top.depth_usd.inner() / top.ask.inner());
        let stream_session_id =
            seeded_uuid(&format!("{}:{token_id}:stream-session", context.scope));
        let event = BookL2LedgerRow {
            stream_session_id,
            shard_id: 0,
            token_id: token_id.clone(),
            market_id: Some(market_id.clone()),
            token_sequence: 1,
            event_type: ChCanonicalBookEventType::Snapshot,
            bid_prices: vec![ChPrice::from(top.bid)],
            bid_sizes: vec![ChShares::from(bid_size)],
            ask_prices: vec![ChPrice::from(top.ask)],
            ask_sizes: vec![ChShares::from(ask_size)],
            old_tick_size: None,
            new_tick_size: None,
            trade_price: None,
            trade_side: None,
            trade_size: None,
            fee_rate_bps: None,
            trade_transaction_hash: None,
            venue_event_time: context.event_at_ms,
            ingress_time: context.available_at_ms,
            persisted_time: context.available_at_ms,
            event_hash: ChDigest::new([0; 32]),
            schema_version: BookL2LedgerRow::SCHEMA_VERSION,
        }
        .seal()?;
        self.ledger.push(source_record(
            format!("ledger:{token_id}:1"),
            context.event_at,
            context.decision_at,
            &event,
        )?);
        let session = BookStreamSessionRow {
            stream_session_id,
            shard_id: 0,
            ledger_sequence: 1,
            state: ChStreamSessionState::Open,
            end_reason: ChStreamSessionEndReason::None,
            subscription_token_hash: CanonicalDigest::content_hash_json(token_id)?,
            subscription_token_count: 1,
            received_sequence_json: format!(r#"{{"{}":1}}"#, token_id.as_str()),
            persisted_sequence_json: format!(r#"{{"{}":1}}"#, token_id.as_str()),
            opened_at: context.event_at_ms,
            recorded_at: context.available_at_ms,
            schema_version: ChSchemaVersion(2),
        };
        self.sessions.push(source_record(
            format!("session:{stream_session_id}:1"),
            context.event_at,
            context.decision_at,
            &session,
        )?);
        let terminal_bid = Price::new(top.bid.inner() - dec!(0.01));
        let terminal_ask = Price::new(top.ask.inner() - dec!(0.01));
        let terminal_midpoint = Price::new(top.mid.inner() - dec!(0.01));
        let depth = top.depth_usd.inner();
        let microstructure = BookMicrostructureRow {
            token_id: token_id.clone(),
            market_id: Some(market_id.clone()),
            bucket_time: context.event_at_ms,
            best_bid_open: Some(ChPrice::from(top.bid)),
            best_bid_high: Some(ChPrice::from(top.bid)),
            best_bid_low: Some(ChPrice::from(top.bid)),
            best_bid_close: Some(ChPrice::from(top.bid)),
            best_ask_open: Some(ChPrice::from(top.ask)),
            best_ask_high: Some(ChPrice::from(top.ask)),
            best_ask_low: Some(ChPrice::from(top.ask)),
            best_ask_close: Some(ChPrice::from(top.ask)),
            spread_bps_min: None,
            spread_bps_avg: None,
            spread_bps_max: None,
            mid_price_open: Some(ChPrice::from(top.mid)),
            mid_price_close: Some(ChPrice::from(top.mid)),
            top1_depth_usd_avg: Some(ChUsd::from(depth)),
            top5_depth_usd_avg: Some(ChUsd::from(depth)),
            top20_depth_usd_avg: Some(ChUsd::from(depth)),
            imbalance_avg: None,
            update_count: 1,
            snapshot_count: 1,
            delta_count: 0,
            delete_count: 0,
            crossed_count: 0,
            invalid_level_count: 0,
            gap_count: 0,
            last_trade_count: 0,
            max_book_age_ms: 0,
            schema_version: ChSchemaVersion::FIRST,
            available_at: context.available_at_ms,
        };
        self.microstructure.push(source_record(
            format!("microstructure:{token_id}:{}", context.event_at_ms),
            context.event_at,
            context.decision_at,
            &microstructure,
        )?);
        let terminal = BookMicrostructureRow {
            bucket_time: context.downside_at_ms,
            best_bid_open: Some(ChPrice::from(terminal_bid)),
            best_bid_high: Some(ChPrice::from(top.bid)),
            best_bid_low: Some(ChPrice::from(terminal_bid)),
            best_bid_close: Some(ChPrice::from(terminal_bid)),
            best_ask_open: Some(ChPrice::from(terminal_ask)),
            best_ask_high: Some(ChPrice::from(top.ask)),
            best_ask_low: Some(ChPrice::from(terminal_ask)),
            best_ask_close: Some(ChPrice::from(terminal_ask)),
            mid_price_open: Some(ChPrice::from(terminal_midpoint)),
            mid_price_close: Some(ChPrice::from(terminal_midpoint)),
            available_at: context.downside_at_ms,
            ..microstructure
        };
        self.microstructure.push(source_record(
            format!("microstructure:{token_id}:{}", context.downside_at_ms),
            context.downside_at,
            context.downside_at,
            &terminal,
        )?);
        Ok(())
    }

    fn push_market_info(&mut self, context: &ReplaySourceContext<'_>) -> QuantResult<()> {
        let market_id = &context.example.market_id;
        let token_id = &context.example.token_id;
        let raw_payload = serde_json::json!({
            "market_id": market_id,
            "token_id": token_id,
            "effective_at": context.event_at,
            "available_at": context.decision_at,
        });
        let market_info = ClobMarketInfoVersion {
            version_id: ClobMarketInfoVersionId::new(seeded_uuid(&format!(
                "{}:market-info",
                context.scope
            ))),
            market_id: market_id.clone(),
            tokens: vec![
                ClobTokenDescriptor {
                    token_id: token_id.clone(),
                    outcome: "Yes".to_owned(),
                },
                ClobTokenDescriptor {
                    token_id: context.no_token_id.clone(),
                    outcome: "No".to_owned(),
                },
            ],
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
            effective_at: context.event_at,
            available_at: context.decision_at,
            payload_hash: CanonicalDigest::content_hash_json(&raw_payload)?,
            raw_payload,
        };
        self.market_info.push(source_record(
            format!("market-info:{}:{}", market_id, market_info.version_id),
            context.event_at,
            context.decision_at,
            &market_info,
        )?);
        Ok(())
    }

    fn push_resolution(&mut self, context: &ReplaySourceContext<'_>) -> QuantResult<()> {
        let example = context.example;
        let market_id = &example.market_id;
        let token_id = &example.token_id;
        if let Some(label) = example
            .labels
            .iter()
            .find(|label| label.label_name.as_str() == "token_payout_ratio")
            && let Ok(payout) = PayoutRatio::try_new(label.value)
        {
            let source_ordinal =
                u64::try_from(context.index + 1).map_err(|error| ResearchError::DatasetBuild {
                    detail: format!("fixture resolution source identity overflow: {error}"),
                })?;
            let block_hash = CanonicalDigest::content_hash_json(&(
                "fixture-resolution-block-v1",
                market_id,
                token_id,
                source_ordinal,
            ))?;
            let transaction_hash = CanonicalDigest::content_hash_json(&(
                "fixture-resolution-transaction-v1",
                market_id,
                token_id,
                source_ordinal,
            ))?;
            let checkpoint_hash = CanonicalDigest::content_hash_json(&(
                "fixture-resolution-checkpoint-v1",
                market_id,
                token_id,
                source_ordinal,
            ))?;
            let resolution = MarketResolutionRow::seal(MarketResolutionFactInput {
                market_id: market_id.clone(),
                token_ids: [token_id.clone(), context.no_token_id.clone()],
                payout_ratios: [payout, payout.complement()],
                resolved_at: label.matured_at.timestamp_millis(),
                observed_at: label.matured_at.timestamp_millis(),
                source_block_number: source_ordinal,
                source_block_hash: EvmBlockHash::parse(format!("0x{}", block_hash.hex())).map_err(
                    |error| ResearchError::DatasetBuild {
                        detail: format!("fixture resolution block hash is invalid: {error}"),
                    },
                )?,
                source_transaction_hash: EvmTransactionHash::parse(format!(
                    "0x{}",
                    transaction_hash.hex()
                ))
                .map_err(|error| ResearchError::DatasetBuild {
                    detail: format!("fixture resolution transaction hash is invalid: {error}"),
                })?,
                source_log_index: source_ordinal,
                source_checkpoint_hash: checkpoint_hash,
            })
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("fixture resolution contract failed: {error}"),
            })?;
            self.resolutions.push(source_record(
                format!("resolution:{market_id}:{source_ordinal}"),
                label.matured_at,
                label.matured_at,
                &resolution,
            )?);
        }
        Ok(())
    }
}

fn replayable_source_records(
    examples: &[TrainingExample],
    downside_horizon_secs: u64,
    source_window_end: DateTime<Utc>,
) -> QuantResult<ReplayableSourceRecords> {
    let mut unique = BTreeMap::new();
    for example in examples {
        unique
            .entry((example.market_id.clone(), example.token_id.clone()))
            .or_insert(example);
    }
    let mut records = ReplayableSourceRecords::default();
    for (index, example) in unique.into_values().enumerate() {
        records.push(index, example, downside_horizon_secs, source_window_end)?;
    }
    Ok(records)
}

async fn persist_replayable_objects(
    store: &Arc<dyn ArtifactStore>,
    records: ReplayableSourceRecords,
    profile: &ResearchProfileArtifact,
) -> QuantResult<Vec<SourceSliceObjectRef>> {
    let required = SourceSliceManifest::required_object_kinds(profile);
    let mut objects = Vec::with_capacity(required.len());
    if required.contains(&SourceSliceObjectKind::GammaMarketIdentity) {
        objects.push(
            persist_source_object(
                store,
                SourceSliceObjectKind::GammaMarketIdentity,
                Vec::new(),
            )
            .await?,
        );
    }
    if required.contains(&SourceSliceObjectKind::CatalogMarket) {
        objects.push(
            persist_source_object(
                store,
                SourceSliceObjectKind::CatalogMarket,
                records.catalog_markets,
            )
            .await?,
        );
    }
    if required.contains(&SourceSliceObjectKind::CatalogEvent) {
        objects.push(
            persist_source_object(
                store,
                SourceSliceObjectKind::CatalogEvent,
                records.catalog_events,
            )
            .await?,
        );
    }
    if required.contains(&SourceSliceObjectKind::ClobMarketInfo) {
        objects.push(
            persist_source_object(
                store,
                SourceSliceObjectKind::ClobMarketInfo,
                records.market_info,
            )
            .await?,
        );
    }
    if required.contains(&SourceSliceObjectKind::L2Ledger) {
        objects.push(
            persist_source_object(store, SourceSliceObjectKind::L2Ledger, records.ledger).await?,
        );
    }
    if required.contains(&SourceSliceObjectKind::L2Session) {
        objects.push(
            persist_source_object(store, SourceSliceObjectKind::L2Session, records.sessions)
                .await?,
        );
    }
    if required.contains(&SourceSliceObjectKind::L2Gap) {
        objects
            .push(persist_source_object(store, SourceSliceObjectKind::L2Gap, records.gaps).await?);
    }
    if required.contains(&SourceSliceObjectKind::BookMicrostructure) {
        objects.push(
            persist_source_object(
                store,
                SourceSliceObjectKind::BookMicrostructure,
                records.microstructure,
            )
            .await?,
        );
    }
    if required.contains(&SourceSliceObjectKind::MarketExecution) {
        objects.push(
            persist_source_object(
                store,
                SourceSliceObjectKind::MarketExecution,
                records.executions,
            )
            .await?,
        );
    }
    if required.contains(&SourceSliceObjectKind::ExecutionParticipant) {
        objects.push(
            persist_source_object(
                store,
                SourceSliceObjectKind::ExecutionParticipant,
                records.participants,
            )
            .await?,
        );
    }
    if required.contains(&SourceSliceObjectKind::Resolution) {
        objects.push(
            persist_source_object(
                store,
                SourceSliceObjectKind::Resolution,
                records.resolutions,
            )
            .await?,
        );
    }
    for kind in [
        SourceSliceObjectKind::MarketLinkage,
        SourceSliceObjectKind::DomainObservation,
        SourceSliceObjectKind::CryptoPriceReport,
        SourceSliceObjectKind::WeatherObservation,
        SourceSliceObjectKind::WeatherForecast,
        SourceSliceObjectKind::CalibrationReference,
    ] {
        if required.contains(&kind) {
            objects.push(persist_source_object(store, kind, Vec::new()).await?);
        }
    }
    objects.sort_by(|left, right| {
        (left.kind, left.uri.as_str()).cmp(&(right.kind, right.uri.as_str()))
    });
    Ok(objects)
}

fn profile_pit_cutoffs(
    profile: &ResearchProfileArtifact,
    available_at: DateTime<Utc>,
) -> Vec<SourceSlicePitCutoff> {
    let mut cutoffs = profile
        .spec
        .required_sources()
        .into_iter()
        .map(|source| SourceSlicePitCutoff {
            source,
            available_at,
        })
        .collect::<Vec<_>>();
    cutoffs.sort_by_key(|cutoff| cutoff.source);
    cutoffs
}

fn replayable_manifest(
    fixture: &ReplayableSourceSliceFixture,
    profile: &ResearchProfileArtifact,
    market_count: usize,
    objects: Vec<SourceSliceObjectRef>,
) -> QuantResult<SourceSliceManifest> {
    let pit_cutoff = fixture.window_end;
    let fit_seal = source_fit_command(
        &fixture.profile_ref,
        fixture.research_program_hash,
        fixture.window_start,
        fixture.window_end,
        pit_cutoff,
    )?;
    Ok(SourceSliceManifest {
        format_version: SOURCE_SLICE_MANIFEST_FORMAT_VERSION,
        profile_ref: fixture.profile_ref.clone(),
        evaluation_track: fixture.evaluation_track,
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
        fit_seal_id: fit_seal.seal.fit_seal_id,
        fit_seal_hash: fit_seal.seal.seal_hash,
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
        pit_cutoffs: profile_pit_cutoffs(profile, pit_cutoff),
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
    let profile = fixture
        .profile_ref
        .resolve_builtin_research_profile()
        .map_err(|detail| ResearchError::DatasetBuild {
            detail: format!("fixture ResearchProfile resolution failed: {detail}"),
        })?;
    let records = replayable_source_records(
        examples,
        profile.spec.target_horizon_secs,
        fixture.window_end,
    )?;
    let objects = persist_replayable_objects(store, records, &profile).await?;
    let manifest = replayable_manifest(&fixture, &profile, examples.len(), objects)?;
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
    seed_source_fit_seal(db, manifest).await?;
    let identity = SourceSliceIdentity::derive(SourceSliceIdentityInput {
        profile_ref: manifest.profile_ref.clone(),
        evaluation_track: manifest.evaluation_track,
        research_program_hash: manifest.research_program_hash,
        decision_policy_snapshot_id: manifest.decision_policy_snapshot_id,
        runtime_config_hash: manifest.runtime_config_hash,
        fit_seal_id: manifest.fit_seal_id,
        fit_seal_hash: manifest.fit_seal_hash,
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
        fit_seal_id: manifest.fit_seal_id,
        fit_seal_hash: manifest.fit_seal_hash,
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
    let runtime_config_hash = PgPolicyRepository::new(db.clone())
        .load_snapshot(&input.decision_policy_snapshot_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "decision_policy_snapshot",
            id: input.decision_policy_snapshot_id.to_string(),
        })?
        .snapshot_hash;
    let capability_registry_hashes =
        CapabilityRegistryHashes::try_new(vec![CanonicalDigest::content_hash_json(&(
            "dataset-source-capability",
            &input.scope,
        ))?])
        .map_err(|error| ResearchError::DatasetBuild {
            detail: error.to_string(),
        })?;
    let profile = input
        .profile_ref
        .resolve_builtin_research_profile()
        .map_err(|detail| ResearchError::DatasetBuild { detail })?;
    let mut objects = SourceSliceManifest::required_object_kinds(&profile)
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
    let fit_seal = source_fit_command(
        &input.profile_ref,
        research_program_hash,
        input.window_start,
        input.window_end,
        input.pit_cutoff,
    )?;
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
        fit_seal_id: fit_seal.seal.fit_seal_id,
        fit_seal_hash: fit_seal.seal.seal_hash,
        dataset_format_version: DATASET_ARTIFACT_FORMAT_VERSION,
        capability_registry_hashes,
        pit_cutoffs: profile_pit_cutoffs(&profile, input.pit_cutoff),
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

async fn seed_evaluation_source(
    db: &DatabaseConnection,
    input: DatasetSourceSeed,
    template: &SourceSliceManifest,
) -> QuantResult<DatasetSourceLineage> {
    if input.window_start >= input.window_end || input.window_end > input.pit_cutoff {
        return Err(ResearchError::DatasetBuild {
            detail: "evaluation source fixture requires start < end <= cutoff".to_owned(),
        }
        .into());
    }
    if template.profile_ref != input.profile_ref
        || template.decision_policy_snapshot_id != input.decision_policy_snapshot_id
    {
        return Err(ResearchError::DatasetBuild {
            detail: "evaluation Source Slice must preserve the training profile and policy"
                .to_owned(),
        }
        .into());
    }
    let runtime_config_hash = PgPolicyRepository::new(db.clone())
        .load_snapshot(&input.decision_policy_snapshot_id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "decision_policy_snapshot",
            id: input.decision_policy_snapshot_id.to_string(),
        })?
        .snapshot_hash;
    if runtime_config_hash != template.runtime_config_hash {
        return Err(ResearchError::DatasetBuild {
            detail: "evaluation Source Slice runtime config differs from training".to_owned(),
        }
        .into());
    }

    let mut manifest = template.clone();
    manifest.window_start = input.window_start;
    manifest.window_end = input.window_end;
    manifest.pit_cutoff = input.pit_cutoff;
    manifest.materialized_at = input.pit_cutoff;
    let fit_seal = source_fit_command(
        &input.profile_ref,
        manifest.research_program_hash,
        input.window_start,
        input.window_end,
        input.pit_cutoff,
    )?;
    manifest.fit_seal_id = fit_seal.seal.fit_seal_id;
    manifest.fit_seal_hash = fit_seal.seal.seal_hash;
    manifest.catalog_proof = SourceSliceCatalogProof {
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
            "evaluation-source-catalog-chain",
            &input.scope,
        ))?,
        market_count: template.catalog_proof.market_count,
        event_count: template.catalog_proof.event_count,
        snapshot_hash: CanonicalDigest::content_hash_json(&(
            "evaluation-source-catalog-snapshot",
            &input.scope,
        ))?,
    };
    for cutoff in &mut manifest.pit_cutoffs {
        cutoff.available_at = input.pit_cutoff;
    }
    manifest.invalid_sessions.clear();
    for (index, object) in manifest.objects.iter_mut().enumerate() {
        object.uri = ArtifactUri::parse(format!(
            "s3://fixture/source-slices/{}/{index:02}.parquet",
            input.scope
        ))?;
        object.object_version = format!("fixture-{}", input.scope);
        object.byte_hash = CanonicalDigest::content_hash_json(&(
            "evaluation-source-bytes",
            &input.scope,
            index,
            object.kind,
        ))?;
        object.min_event_at = object.min_event_at.map(|_| input.window_start);
        object.max_event_at = object.max_event_at.map(|_| input.window_end);
        object.min_available_at = object.min_available_at.map(|_| input.window_start);
        object.max_available_at = object.max_available_at.map(|_| input.pit_cutoff);
    }
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
    let repository = PgTrainingDatasetRepository::new(db.clone());
    let source = EvaluationDatasetSource::load(db, &input).await?;
    let source_lineage = seed_evaluation_source(
        db,
        DatasetSourceSeed {
            scope: format!("{}:source", input.scope),
            profile_ref: input.profile_ref.clone(),
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            window_start: input.window_start,
            window_end: input.window_end,
            pit_cutoff: input.window_end + Duration::hours(1),
        },
        &source.source_manifest,
    )
    .await?;
    let cohort_manifest = model_learning_cohort(
        &input.scope,
        &source_lineage,
        input.window_start,
        input.window_end,
        input.sample_count,
    )?;
    let fixture = source.ledger(&input, dataset_id, source_lineage, cohort_manifest)?;
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
