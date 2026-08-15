//! Deterministic historical truth for the production feedback-closure fixture.
//!
//! This module seeds only facts that must already exist before the current
//! cadence cutoff. Every immutable row is sealed with the production domain
//! contract. Feedback stage artifacts, candidates, route bindings, permits,
//! activations, and rollback receipts are deliberately absent: the real binary
//! must create them through the production coordinator.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    mem,
    sync::Arc,
    time::Duration as StdDuration,
};

use anyhow::{Context, Error as AnyhowError, Result, ensure};
use chrono::{DateTime, Duration, Utc};
use futures_util::{StreamExt, TryStreamExt, future::join_all, stream};
use quant_pivot_core::{
    app::ports::feedback_mutation::FeedbackCycleFreezePlan,
    governance::{CoreCalibrationArtifactLoader, resolve_return_model_calibration},
    observability::serving_evidence::{
        ModelInputEvidenceBatch, completion_marker, feature_commitment, verify_completion,
    },
    pit::platform::ch_historical::DurablePitSource,
    prefetch::{
        historical_window::{HistoricalWindowLoader, ReplaySample, WindowSpec},
        market_candidates::MarketCandidateProvider,
    },
    projection::inference_context::build_market_inference_context,
    service::{
        historical_replay::{
            CrossSectionRequest, ReplayCaptureKey, ReplayConfig, ReplayCrossSection,
            ReplayExecutionSource, ReplayFactorMode, ReplayFactorOutput, materialize_cross_section,
        },
        market_selection::map_snapshot_to_model,
        model_runner::{ActiveModelRequirementsRequest, ModelRunRequest, ModelRunner},
        model_serving_generation::ModelServingRouteSnapshot,
    },
};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, BookStreamSessionRow, ChAssetAmount, ChBps,
        ChDecimal64, ChDigest, ChPrice, ChSchemaVersion, ChShares, ChUsd, DomainObservationRow,
        ExchangeHistoryAcceptanceRow, ExecutionParticipantRow, MarketExecutionRow,
        MarketResolutionFactInput, MarketResolutionRow, QuantFeatureEventRow,
        QuantModelInputEventRow, QuantReportRecommendationFactRow,
        QuantServingEvidenceCompletionRow, ReportMarketFunnelRow,
    },
    config::{ClickHouseConfig, WeatherVerticalBindingsConfig},
    domain::{
        data_plane::{DecisionBoundary, DecisionClock, DecisionSource, DomainObservation},
        market::{
            BookLevel, CATALOG_OBJECT_SCHEMA_VERSION, EventRegistryInfo, EventTags,
            MarketRegistryInfo, TokenInfo, UpsertEvent, UpsertMarket,
        },
        ports::{
            FeedbackRecipeCalibrationSpec, FeedbackRecipeCpcvSpec, FeedbackRecipeDiagnosticSpec,
            FeedbackRecipeDownsideSpec, FeedbackRecipeResourceBudget, FeedbackRecipeTemplate,
            FeedbackRecipeTemplateInput, FeedbackRecipeTrainingSpec,
        },
        quant::{
            AttributionSubject, ExecutionAttemptDerivation, ExecutionAttemptOutcomeInfo,
            ExecutionAttemptSourceGraph, FeedbackCycleInfo, FeedbackCycleKey,
            FeedbackCycleKeyInput, FeedbackStageEventInfo, FeedbackStageEventInput, LinkageOutcome,
            LinkageSourceMetadata, LinkageUnresolvedReason, MarketLinkageDerivation,
            MarketSelectionModel, ModelSpecInfo, ModelVersionInfo, NewAttributionArtifact,
            NewFeedbackCycle, NewFeedbackStageEvent, NewMarketLinkage, NewPosition,
            NewRecommendation, NewRecommendationExecutionRollup,
            NewRecommendationExecutionRollupAttempt, NewRecommendationResolutionOutcome,
            NewReportTransaction, PortfolioScenarioModelArtifact, PortfolioScenarioVisibility,
            RepresentedRouteSet, ResearchJobInfo, ShadowObservationQuery,
            report_parity_evidence_hash, report_parity_generation_hash,
        },
    },
    entities::{
        catalog_event_change::{
            Entity as CatalogEventChangeEntity, Model as CatalogEventChangeModel,
        },
        catalog_event_object::{
            Entity as CatalogEventObjectEntity, Model as CatalogEventObjectModel,
        },
        catalog_market_change::{
            Entity as CatalogMarketChangeEntity, Model as CatalogMarketChangeModel,
        },
        catalog_market_object::{
            Entity as CatalogMarketObjectEntity, Model as CatalogMarketObjectModel,
        },
        catalog_sync_batch::{Entity as CatalogSyncBatchEntity, Model as CatalogSyncBatchModel},
        quant_attribution_artifact::Entity as AttributionArtifactEntity,
        quant_capital_allocation::Entity as CapitalAllocationEntity,
        quant_entry_condition_instance::{
            Column as EntryConditionColumn, Entity as EntryConditionEntity,
        },
        quant_execution_attempt_outcome::Entity as ExecutionAttemptOutcomeEntity,
        quant_execution_order::Entity as ExecutionOrderEntity,
        quant_factor_value::Entity as FactorValueEntity,
        quant_feature_parity_subject::{
            Column as FeatureParitySubjectColumn, Entity as FeatureParitySubjectEntity,
        },
        quant_feature_vector::Entity as FeatureVectorEntity,
        quant_market_linkage::{Column as MarketLinkageColumn, Entity as MarketLinkageEntity},
        quant_model_route_shadow_binding::{
            Column as ShadowBindingColumn, Entity as ShadowBindingEntity,
            Model as ShadowBindingModel,
        },
        quant_order_intent::Entity as OrderIntentEntity,
        quant_position::{Entity as PositionEntity, Model as PositionModel},
        quant_recommendation_execution_rollup::{
            ActiveModel as ExecutionRollupActiveModel, Entity as ExecutionRollupEntity,
        },
        quant_recommendation_execution_rollup_attempt::{
            ActiveModel as ExecutionRollupAttemptActiveModel,
            Entity as ExecutionRollupAttemptEntity,
        },
        quant_recommendation_report::Entity as RecommendationReportEntity,
        quant_recommendation_resolution_outcome::{
            ActiveModel as ResolutionOutcomeActiveModel, Entity as ResolutionOutcomeEntity,
        },
        quant_reconciliation::Entity as ReconciliationEntity,
        quant_shadow_comparison::{
            Column as ShadowComparisonColumn, Entity as ShadowComparisonEntity,
        },
        user::{Column as UserColumn, Entity as UserEntity},
    },
    enums::{
        catalog::{
            CatalogChangeType, CatalogFilterReasonSet, CatalogSyncKind, CatalogSyncStatus,
            CatalogTimestampQuality,
        },
        clickhouse::{
            ChAvailabilityBasis, ChCanonicalBookEventType, ChExchangeSide, ChExchangeVersion,
            ChExecutionParticipantRole, ChStreamSessionEndReason, ChStreamSessionState,
        },
        common::{CategorySet, MarketCategory, TickSize},
        domain::{DomainFamily, DomainMetric, LinkageStatus, ResolverTier},
        execution::{
            CapitalAllocationState, ExitReason, ExitState, PositionLedgerState, VenueOrderStatus,
        },
        market::{EventStatus, MarketStatus},
        model::ModelFamily,
        quant::{
            AccountSource, ApprovalStatus, AttributionArtifactKind, AttributionCohort,
            CalibrationMethod, DownsideSource, EmptyReportReason, ExecutionOrderState,
            FeedbackDecision, FeedbackDriftMetric, FeedbackEvaluationMode,
            FeedbackRecipeTemplateStatus, FeedbackStage, FeedbackStageEventKind,
            FeedbackTriggerFamily, OrderIntentStatus, OutcomeSide, QuantRuntimeMode,
            RecommendationStatus, ResearchJobKind, ResearchJobResultKind, ResearchJobStatus,
            ShadowBindingStatus,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::{
        ActivePolicyBundle, BuyModelRoute, PortfolioScenarioModelArtifactBinding,
        ResearchValidationConfig, SelectionConfig,
    },
    types::{
        ArtifactUri, BookSnapshotRef, BookSnapshotSource, Bps, CatalogDecisionRef,
        CatalogEventChangeId, CatalogEventObjectId, CatalogMarketChangeId, CatalogMarketIds,
        CatalogMarketObjectId, CatalogSyncBatchId, ClobFeeDetails, ClobMarketInfoVersion,
        ClobMarketInfoVersionId, ClobTokenDescriptor, ContentHash, DecisionCaptureEvidence,
        DecisionPolicySnapshotId, EligibilitySummary, EventId, EvmBlockHash, EvmTransactionHash,
        ExternalJsonDocument, FactorBreakdownEntry, FeatureVectorId, FeedbackCycleId,
        FeedbackRecipeTemplateId, FinalizedExecutionEvidence, MarketId, MarketLinkageId,
        ModelCandidateManifestId, ModelVersionId, OrderId, OrderIntentId, PayoutRatio, PositionId,
        Price, Probability, RecommendationFactorBreakdown, RecommendationId,
        RecommendationReportId, ReportFunnelDiagnostics, ReportFunnelReason, ReportFunnelStage,
        ResearchJobId, ResearchProfileArtifact, ResolverVersion, RoleCode, ScaleOutState,
        SchemaVersion, ShadowComparisonId, Shares, TokenId, Usd, UsdHours,
        stable_name::FeatureName,
    },
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChQuantFactReadRepository},
    postgres::{
        PgBacktestPathSetRepository, PgCalibrationArtifactRepository, PgCatalogLedgerRepository,
        PgClobMarketInfoRepository, PgEventRepository, PgFeatureRepository,
        PgFeedbackCycleRepository, PgFeedbackRecipeTemplateRepository, PgMarketLinkageRepository,
        PgMarketRepository, PgModelCandidateManifestRepository, PgModelRegistryRepository,
        PgModelRunRepository, PgPolicyRepository, PgResearchJobRepository,
        PgShadowComparisonRepository,
    },
    traits::{
        BacktestPathSetRepository, CalibrationArtifactRepository, CatalogLedgerRepository,
        ClobMarketInfoRepository, EventRepository, FactWriter, FeatureRepository,
        FeedbackCycleRepository, FeedbackCycleWriteOutcome, FeedbackRecipeTemplateRepository,
        FeedbackStageWriteOutcome, FeedbackTriggerCommit, FeedbackTriggerWriteOutcome,
        MarketLinkageRepository, MarketRepository, ModelCandidateManifestRepository,
        ModelRegistryRepository, ModelRunRepository, PolicyRepository, QuantFactReadRepository,
        ResearchJobRepository, ShadowComparisonRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    attribution::{
        AttributionArtifact, AttributionArtifactCodec, AttributionLineage,
        PredictionExplanationArtifact, PredictionOutputKind, WeightedExplanationInput,
        WeightedTerm,
    },
    factors::{FactorEligibility, FactorEngine, FactorValue, FactorValueInsertContext},
    features::{ConfiguredFeatureBuilder, ExecutableFeatureSchema, FeatureVector, feature_events},
    feedback_governance::FeedbackGovernanceCodec,
    hashing::ResearchHasher,
    linkage::{LayeredResolver, WeatherStationRegistry, rule_for_alias},
    model::{
        FactorInferenceRow, FactorInferenceTable, ModelArtifact, ModelRuntimeInput,
        QuantModelRuntime, SignalCandidate, WeightedFactorRuntime, WeightedInputAuditContract,
        canonical_business_prediction_hash, finalize_candidates, model_input_contract_hash,
    },
    portfolio::{PortfolioScenarioGenerator, PortfolioScenarioModelFitter},
    selection::{
        ConfiguredMarketSelector, MarketSelectionBuildRequest, MarketSelector,
        ModelFeatureRequirements,
    },
    stats,
};
use quant_pivot_storage::clickhouse::{ChWriteManager, ClickHousePool};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use rust_decimal_macros::dec;
use sea_orm::{
    ActiveValue::Set, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    DbBackend, EntityTrait, IntoActiveModel, QueryFilter, Statement, TransactionTrait,
};
use serde_json::{Value as JsonValue, json};
use tokio::time::{Instant, sleep};

use crate::postgres::PostgresClock;

use super::{
    execution_pg_seed::{
        ExecutionTxnIds, ReportBuildOptions, ReportSeedConfig, SharedDemoInfra,
        build_custom_report_transaction, demo_recommendation, exit_order, exit_reconciliation_row,
        fixture_profile_ref, new_capital_allocation, new_execution_order, new_order_intent,
        prepare_report_lineage_model, reconciliation_row,
    },
    report_lifecycle_seed::{persist_and_publish_report, seal_report_facts},
    report_pipeline_harness::build_model_runner,
    seeded_uuid,
};

// The governed closure needs 96 chronological decision groups so its active
// 24-hour scenario model observes every PIT-mature bucket in the 90-day fit
// span. Eight markets per group preserve the complete four-strength by
// two-regime factorial without padding any validation partition.
const TRAINING_OBSERVATION_COUNT: usize = 768;
const TRAINING_RESOLUTION_LAG_SECS: i64 = 86_400;
const TRAINING_TRUTH_BUFFER_SECS: i64 = 120;
const CALIBRATION_GROUP_COUNT: usize = 4;
// The calibration replay retains its complete pre-decision score population,
// so 1,024 independent binary samples clear the governed 1,000-sample
// isotonic floor even when the downstream decision deadband abstains.
const CALIBRATION_OBSERVATION_COUNT: usize = 1_024;
const EVALUATION_OBSERVATION_COUNT: usize = 500;
const EVALUATION_MARKETS_PER_TICK: usize = 5;
const EXECUTION_ASSOCIATION_SAMPLE_COUNT: usize = 3;
const FACTOR_VALUE_INSERT_BATCH_ROWS: usize = 1_000;
const EXECUTION_ROLLUP_INSERT_BATCH_ROWS: usize = 1_000;
const FACT_WRITE_BATCH_ROWS: usize = 2_000;
// Historical decisions are temporally ordered production boundaries. Seeding
// them concurrently would create artificial ClickHouse query pressure and can
// expose future fixture writes to earlier builders even though PIT filtering
// eventually rejects those rows. Keep decision materialization serial; the
// inner source writes remain independently bounded below.
const DECISION_SEED_CONCURRENCY: usize = 1;
// Each seed task exercises the real PostgreSQL PIT repositories in addition to
// ClickHouse writes. Keep this below the isolated stack's connection-pool
// capacity so fixture pressure cannot starve the production background workers.
const SOURCE_SEED_CONCURRENCY: usize = 4;
const SHADOW_OBSERVATION_COUNT: usize = 1_000;
const SHADOW_OBSERVATION_CONCURRENCY: usize = 16;
/// The mixed-Route report universe remains inside the production WebSocket
/// prewarm window while still exercising the multi-day capital bucket.
pub(crate) const CLOSURE_REPORT_HORIZON_HOURS: i64 = 48;
// This unoptimized fresh-stack path is a functional correctness gate, not a
// production latency benchmark. Keep a finite ceiling for cancellation and
// deadlock detection while leaving performance acceptance to the controlled
// release-profile full-compute benchmark.
const CLOSURE_COMPUTE_LIVENESS_SECS: u64 = 30 * 60;
const CYCLE_TO_BIND_TIMEOUT: StdDuration = StdDuration::from_hours(1);
const CYCLE_LIVENESS_TIMEOUT: StdDuration = StdDuration::from_mins(3);
const CANDIDATE_READY_TIMEOUT: StdDuration = StdDuration::from_mins(3);
const CLOSURE_POLL_INTERVAL: StdDuration = StdDuration::from_millis(100);
const CATALOG_BASELINE_DOMAIN: &str = "quant-pivot/system-test/feedback-closure-catalog-baseline";

#[derive(Debug, Clone, Copy)]
struct ScenarioBucketRequirement {
    bucket_secs: i64,
    complete_bucket_floor: usize,
    eligible_bucket_count: usize,
}

#[derive(Debug, Clone)]
struct ScenarioTrainingPlan {
    window_start: DateTime<Utc>,
    latest_decision_exclusive: DateTime<Utc>,
    requirements: Vec<ScenarioBucketRequirement>,
    group_floor: usize,
}

impl ScenarioTrainingPlan {
    async fn load(
        artifact_store: &Arc<dyn ArtifactStore>,
        policy: &ActivePolicyBundle,
        cycle: &FeedbackCycleFreezePlan,
        governance_frozen_at: DateTime<Utc>,
    ) -> Result<Self> {
        let bindings = policy
            .snapshot
            .model_routing
            .model
            .portfolio_scenario_model_bindings
            .iter()
            .filter(|binding| binding.ordered_routes.contains(&BuyModelRoute::Weather))
            .collect::<Vec<_>>();
        ensure!(
            !bindings.is_empty(),
            "closure Weather training has no active portfolio-scenario binding"
        );
        let truth_lag_secs = TRAINING_RESOLUTION_LAG_SECS
            .checked_add(TRAINING_TRUTH_BUFFER_SECS)
            .context("closure training truth lag overflowed")?;
        let latest_decision_exclusive = cycle
            .training()
            .cutoff()
            .checked_sub_signed(Duration::seconds(truth_lag_secs))
            .context("closure training maturity boundary underflowed")?;
        ensure!(
            latest_decision_exclusive > cycle.training().window_start(),
            "closure training window has no PIT-mature scenario observations"
        );

        let mut requirements = Vec::with_capacity(bindings.len());
        for binding in bindings {
            let key = ArtifactKey::new(
                ArtifactNamespace::PortfolioScenarioModel,
                binding.portfolio_scenario_model_artifact_id.to_string(),
                "json",
            )?;
            let bytes = artifact_store.get_by_key(&key).await?;
            let model = serde_json::from_slice::<PortfolioScenarioModelArtifact>(&bytes)
                .context("decode active closure scenario-model artifact")?;
            let represented = RepresentedRouteSet::from_routes(binding.ordered_routes.clone())?;
            PortfolioScenarioGenerator::verify_model(
                binding,
                &model,
                &represented,
                cycle.label_cutoff(),
                PortfolioScenarioVisibility::HistoricalReplay {
                    governance_frozen_at,
                },
            )?;
            let bucket_secs = i64::try_from(model.time_bucket_secs)
                .context("closure scenario bucket seconds exceed i64")?;
            ensure!(bucket_secs > 0, "closure scenario bucket must be positive");
            let complete_bucket_floor =
                PortfolioScenarioModelFitter::minimum_complete_buckets(model.resampling_method)?;
            let eligible_bucket_count = Self::bucket_count(
                cycle.training().window_start(),
                latest_decision_exclusive,
                bucket_secs,
            )?;
            ensure!(
                eligible_bucket_count >= complete_bucket_floor,
                "closure training has {eligible_bucket_count} PIT-mature canonical buckets but active scenario methodology requires {complete_bucket_floor}"
            );
            requirements.push(ScenarioBucketRequirement {
                bucket_secs,
                complete_bucket_floor,
                eligible_bucket_count,
            });
        }
        let group_floor = requirements
            .iter()
            .map(|requirement| requirement.eligible_bucket_count)
            .max()
            .context("closure scenario training requirement set is empty")?;
        Ok(Self {
            window_start: cycle.training().window_start(),
            latest_decision_exclusive,
            requirements,
            group_floor,
        })
    }

    fn points(&self, count: usize) -> Result<Vec<DateTime<Utc>>> {
        let points = interior_points(self.window_start, self.latest_decision_exclusive, count)?;
        for requirement in &self.requirements {
            let buckets = points
                .iter()
                .map(|point| point.timestamp().div_euclid(requirement.bucket_secs))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            ensure!(
                buckets.len() == requirement.eligible_bucket_count
                    && buckets.len() >= requirement.complete_bucket_floor,
                "closure training timeline covers {} canonical buckets, expected all {} PIT-mature buckets and at least the methodology floor {}",
                buckets.len(),
                requirement.eligible_bucket_count,
                requirement.complete_bucket_floor
            );
            ensure!(
                buckets.windows(2).all(|pair| pair[1] == pair[0] + 1),
                "closure training timeline contains a gap in the canonical scenario clock"
            );
        }
        Ok(points)
    }

    fn bucket_count(
        window_start: DateTime<Utc>,
        end_exclusive: DateTime<Utc>,
        bucket_secs: i64,
    ) -> Result<usize> {
        ensure!(
            bucket_secs > 0 && end_exclusive > window_start,
            "closure scenario bucket count requires a positive interval"
        );
        let end_millis = end_exclusive.timestamp_millis();
        let last_millis = end_millis
            .checked_sub(1)
            .context("closure scenario exclusive endpoint underflowed")?;
        let bucket_millis = bucket_secs
            .checked_mul(1_000)
            .context("closure scenario bucket milliseconds overflowed")?;
        let first_bucket = window_start.timestamp_millis().div_euclid(bucket_millis);
        let last_bucket = last_millis.div_euclid(bucket_millis);
        let bucket_count = last_bucket
            .checked_sub(first_bucket)
            .and_then(|distance| distance.checked_add(1))
            .context("closure scenario bucket range overflowed")?;
        usize::try_from(bucket_count).context("closure scenario bucket count exceeds usize")
    }
}

/// Stable production-cycle identity created by the historical closure fixture.
#[derive(Clone)]
pub struct FeedbackClosureFixture {
    pub feedback_cycle_id: FeedbackCycleId,
    catalogs: Arc<[Arc<ClosureCatalogFacts>]>,
    report_cohorts: Arc<[ShadowObservationCohort]>,
    shadow_cohorts: Arc<[ShadowObservationCohort]>,
    fact_writers: Arc<ClosureFactWriters>,
    replay: Arc<ClosureReplayContext>,
    runtime_finalized_execution_evidence: FinalizedExecutionEvidence,
}

/// Terminal proof for one node in the closed 15-stage production DAG.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FeedbackClosureStageEvidence {
    pub stage: FeedbackStage,
    pub started_event_sequence: Option<i64>,
    pub event_sequence: i64,
    pub research_job_id: Option<ResearchJobId>,
    pub attempt_ordinal: Option<i32>,
    pub max_attempts: Option<i32>,
    pub started_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub duration_millis: Option<i64>,
    pub evidence_uri: Option<ArtifactUri>,
    pub evidence_hash: Option<ContentHash>,
    pub event_hash: ContentHash,
    pub occurred_at: DateTime<Utc>,
}

/// Candidate and scenario evidence that may enter governed permit issuance.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FeedbackClosureOutcome {
    pub feedback_cycle_id: FeedbackCycleId,
    pub champion_model_version_id: ModelVersionId,
    pub candidate_model_version_id: ModelVersionId,
    pub candidate_manifest_id: ModelCandidateManifestId,
    pub candidate_manifest_hash: ContentHash,
    pub scenario_model_bindings_hash: ContentHash,
    pub portfolio_scenario_model_bindings: Vec<PortfolioScenarioModelArtifactBinding>,
    pub stage_evidence: Vec<FeedbackClosureStageEvidence>,
}

/// Fresh mixed-Route market plane used by the post-activation production report.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FeedbackReportUniverse {
    pub decision_at: DateTime<Utc>,
    pub market_ids: Vec<MarketId>,
    pub categories: Vec<MarketCategory>,
}

/// Exact current-time L2 snapshot required by the mixed-Route report fixture.
#[derive(Debug, Clone)]
pub struct FeedbackReportBookSnapshot {
    pub token_id: TokenId,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
}

/// Read-back proof for one canonical post-report resolution fact.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FeedbackResolutionFactEvidence {
    pub market_id: MarketId,
    pub resolved_outcome: String,
    pub resolved_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub source_checkpoint_hash: ContentHash,
    pub resolution_fact_hash: ContentHash,
}

/// Source-native truth written after the promoted mixed-Route report.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FeedbackReportResolutionEvidence {
    pub report_id: RecommendationReportId,
    pub report_decision_at: DateTime<Utc>,
    pub resolved_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub facts: Vec<FeedbackResolutionFactEvidence>,
}

#[derive(Clone)]
struct ShadowObservationCohort {
    markets: Arc<[ClosureMarketSource]>,
    book_price_shift: Decimal,
    catalog: Arc<ClosureCatalogFacts>,
}

#[derive(Clone)]
struct ClosureMarketSource {
    source_id: RecommendationId,
    market_id: MarketId,
}

impl From<&NewRecommendation> for ClosureMarketSource {
    fn from(recommendation: &NewRecommendation) -> Self {
        Self {
            source_id: recommendation.recommendation_id,
            market_id: recommendation.market_id.clone(),
        }
    }
}

#[derive(Clone)]
struct ShadowObservationResult {
    shadow_comparison_id: ShadowComparisonId,
    comparison_hash: ContentHash,
    emitted: u32,
    topn_decision_overlap: Probability,
    hard_divergence: bool,
}

struct ShadowObservationRequest<'a> {
    db: &'a DatabaseConnection,
    runner: &'a ModelRunner,
    serving: &'a ModelServingRouteSnapshot,
    schema: &'a ExecutableFeatureSchema,
    facts: &'a ClosureFactWriters,
    replay: &'a ClosureReplayContext,
    catalog: &'a ClosureCatalogFacts,
    policy_snapshot_id: DecisionPolicySnapshotId,
    sources: &'a [ClosureMarketSource],
    required_features: &'a [FeatureName],
    runtime_finalized_execution_evidence: &'a FinalizedExecutionEvidence,
    decision_at: DateTime<Utc>,
    book_price_shift: Decimal,
}

struct CohortSeed {
    catalog: Arc<ClosureCatalogFacts>,
    decision_at: DateTime<Utc>,
    book_price_shift: Decimal,
    resolution_by_market: BTreeMap<MarketId, DateTime<Utc>>,
    ids: ExecutionTxnIds,
    market_universe: Vec<NewRecommendation>,
    recommendations: Vec<NewRecommendation>,
}

struct PreparedCohort {
    cohort: CohortSeed,
    options: ReportBuildOptions,
    trigger_key: String,
}

impl PreparedCohort {
    async fn publish(
        self,
        db: &DatabaseConnection,
        artifact_store: &Arc<dyn ArtifactStore>,
        fact_writers: &ClosureFactWriters,
    ) -> Result<CohortSeed> {
        let Self {
            cohort,
            options,
            trigger_key,
        } = self;
        let mut transaction = build_custom_report_transaction(&cohort.ids, options);
        let recommendation_rows = closure_report_recommendations(&transaction)?;
        let funnel_rows = closure_report_funnel(&transaction, &cohort.market_universe)?;
        seal_report_facts(
            artifact_store,
            &mut transaction,
            recommendation_rows.clone(),
            funnel_rows.clone(),
        )
        .await?;
        fact_writers
            .commit_report(recommendation_rows, funnel_rows)
            .await?;
        persist_and_publish_report(db, transaction, &trigger_key, 10).await;
        Ok(cohort)
    }
}

impl ReportBuildOptions {
    fn align_closure_summary(&mut self, market_selection_count: usize) -> Result<()> {
        let published_count = u32::try_from(self.recommendations.len())?;
        let market_selection_count = u32::try_from(market_selection_count)?;
        let total_suggested_usd = self
            .recommendations
            .iter()
            .map(|recommendation| recommendation.trade_plan.sizing.suggested_usd)
            .sum();
        let max_single_recommendation_usd = self
            .recommendations
            .iter()
            .map(|recommendation| recommendation.trade_plan.sizing.suggested_usd)
            .max()
            .unwrap_or(Usd::ZERO);
        let mut category_allocation = BTreeMap::new();
        let mut event_allocation = BTreeMap::new();
        let mut route_allocation = BTreeMap::new();
        let mut eligibility = EligibilitySummary::default();
        for recommendation in &self.recommendations {
            let sizing = &recommendation.trade_plan.sizing;
            *category_allocation
                .entry(recommendation.identity.category)
                .or_default() += sizing.suggested_usd;
            *event_allocation
                .entry(recommendation.event_id.clone())
                .or_default() += sizing.suggested_usd;
            *route_allocation.entry(recommendation.route).or_default() += sizing.suggested_usd;
            if recommendation
                .execution_eligibility
                .is_eligible(QuantRuntimeMode::ReportOnly)
            {
                eligibility.eligible_report_only += 1;
            }
            if recommendation
                .execution_eligibility
                .is_eligible(QuantRuntimeMode::SemiAuto)
            {
                eligibility.eligible_semi_auto += 1;
            }
            if recommendation
                .execution_eligibility
                .is_eligible(QuantRuntimeMode::AutoExecution)
            {
                eligibility.eligible_auto_execution += 1;
            }
        }
        let is_empty = published_count == 0;
        self.summary.market_selection_count = market_selection_count;
        self.summary.candidate_count = published_count;
        self.summary.rejected_tier_count = market_selection_count.saturating_sub(published_count);
        self.summary.published_recommendation_count = published_count;
        self.summary.total_suggested_usd = total_suggested_usd;
        self.summary.max_single_recommendation_usd = max_single_recommendation_usd;
        self.summary.category_allocation = category_allocation;
        self.summary.event_allocation = event_allocation;
        self.summary.route_allocation = route_allocation;
        self.summary.robust_expected_net_usd = self
            .recommendations
            .iter()
            .map(|recommendation| recommendation.economics_json.robust_expected_net_usd)
            .sum();
        self.summary.nominal_expected_net_usd = self
            .recommendations
            .iter()
            .map(|recommendation| recommendation.economics_json.nominal_expected_net_usd)
            .sum();
        self.summary.cvar_usd = self
            .recommendations
            .iter()
            .map(|recommendation| recommendation.economics_json.cvar_contribution_usd)
            .sum();
        self.summary.maximum_scenario_loss_usd = self
            .recommendations
            .iter()
            .map(|recommendation| recommendation.economics_json.max_loss_usd)
            .sum();
        self.summary.capital_occupancy_usd_hours = UsdHours::new(
            self.recommendations
                .iter()
                .map(|recommendation| {
                    recommendation
                        .economics_json
                        .capital_occupancy_usd_hours
                        .inner()
                })
                .sum(),
        );
        self.summary.top_rejection_reasons.clear();
        self.summary.execution_eligibility_summary = eligibility;
        self.summary.empty_reason = is_empty.then_some(EmptyReportReason::NoPositiveSignal);
        self.summary.warnings = if is_empty {
            vec!["active model emitted no positive candidate".to_owned()]
        } else {
            Vec::new()
        };
        Ok(())
    }
}

fn closure_report_recommendations(
    transaction: &NewReportTransaction,
) -> Result<Vec<QuantReportRecommendationFactRow>> {
    transaction
        .recommendations
        .iter()
        .map(|recommendation| {
            let economics = recommendation.economics_json;
            Ok(QuantReportRecommendationFactRow {
                event_time: transaction.report.decision_at.timestamp_millis(),
                recommendation_report_id: transaction.report.recommendation_report_id,
                recommendation_id: recommendation.recommendation_id,
                report_route_run_id: recommendation.report_route_run_id,
                economic_tier_id: recommendation.economic_tier_id,
                route: recommendation.route.as_str().to_owned(),
                rank: u32::try_from(recommendation.rank)?,
                market_id: recommendation.market_id.clone(),
                token_id: recommendation.token_id.clone(),
                side: recommendation.outcome_side.into(),
                profit_probability_bps: economics
                    .profit_probability_bps
                    .inner()
                    .to_i64()
                    .context("closure profit probability bps must fit i64")?,
                nominal_expected_net_usd: ChUsd::from(economics.nominal_expected_net_usd),
                robust_expected_net_usd: ChUsd::from(economics.robust_expected_net_usd),
                max_loss_usd: ChUsd::from(economics.max_loss_usd),
                cvar_contribution_usd: ChUsd::from(economics.cvar_contribution_usd),
                capital_occupancy_usd_hours: ChUsd::from(Usd::new(
                    economics.capital_occupancy_usd_hours.inner(),
                )),
                marginal_portfolio_value_usd: ChUsd::from(economics.marginal_portfolio_value_usd),
                suggested_usd: ChUsd::from(recommendation.trade_plan.sizing.suggested_usd),
                valid_until: recommendation.valid_until.timestamp_millis(),
            })
        })
        .collect()
}

fn closure_report_funnel(
    transaction: &NewReportTransaction,
    market_universe: &[NewRecommendation],
) -> Result<Vec<ReportMarketFunnelRow>> {
    let published = transaction
        .recommendations
        .iter()
        .map(|recommendation| recommendation.recommendation_id)
        .collect::<HashSet<_>>();
    market_universe
        .iter()
        .map(|recommendation| {
            let is_published = published.contains(&recommendation.recommendation_id);
            let (terminal_stage, primary_reason, recommendation_id, signal_candidate_id) =
                if is_published {
                    (
                        ReportFunnelStage::Published,
                        ReportFunnelReason::Published,
                        Some(recommendation.recommendation_id),
                        Some(recommendation.evidence_refs.signal_candidate_id),
                    )
                } else {
                    // A factor-complete market can legitimately stop at the
                    // governed decision deadband. It remains in the conserved
                    // report universe, but it is not a SignalCandidate and no
                    // Recommendation identity may be fabricated for it.
                    (
                        ReportFunnelStage::ModelScored,
                        ReportFunnelReason::NoPositiveSignal,
                        None,
                        None,
                    )
                };
            let secondary_diagnostics = ReportFunnelDiagnostics::None {};
            secondary_diagnostics
                .validate_for(primary_reason)
                .map_err(AnyhowError::msg)?;
            let route_run = transaction
                .route_runs
                .iter()
                .find(|run| run.report_route_run_id == recommendation.report_route_run_id)
                .context("closure recommendation Route run must exist")?;
            let lineage = route_run.lineage_json.as_ref();
            let mut row = ReportMarketFunnelRow {
                event_time: transaction.report.decision_at.timestamp_millis(),
                recommendation_report_id: transaction.report.recommendation_report_id,
                market_selection_id: transaction.report.market_selection_id,
                decision_policy_snapshot_id: transaction.report.decision_policy_snapshot_id,
                report_route_run_id: Some(route_run.report_route_run_id),
                route: Some(route_run.route.as_str().to_owned()),
                model_version_id: lineage.map(|value| value.model_version_id),
                model_run_id: lineage.and_then(|value| value.model_run_id),
                market_id: recommendation.market_id.clone(),
                event_id: recommendation.event_id.clone(),
                primary_token_id: recommendation
                    .evidence_refs
                    .book_snapshot_ref
                    .token_id
                    .clone(),
                terminal_stage: terminal_stage.as_str().to_owned(),
                primary_reason: primary_reason.as_str().to_owned(),
                secondary_diagnostics_json: serde_json::to_string(&secondary_diagnostics)?,
                feature_vector_id: Some(recommendation.evidence_refs.feature_vector_id),
                signal_candidate_id,
                recommendation_id,
                row_hash: String::new(),
                ingestion_time: transaction.report.decision_at.timestamp_millis(),
            };
            row.seal_hash()?;
            Ok(row)
        })
        .collect()
}

struct ClosureCatalogMarket {
    info: MarketRegistryInfo,
    change_id: CatalogMarketChangeId,
    object_id: CatalogMarketObjectId,
    content_hash: ContentHash,
}

struct ClosureCatalogFacts {
    batch_id: CatalogSyncBatchId,
    event_change_id: CatalogEventChangeId,
    event_object_id: CatalogEventObjectId,
    event: EventRegistryInfo,
    event_content_hash: ContentHash,
    registry_event: EventRegistryInfo,
    registry_event_content_hash: ContentHash,
    effective_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    category: MarketCategory,
    markets: Vec<ClosureCatalogMarket>,
}

async fn seed_catalog_baseline(
    db: &DatabaseConnection,
    coverage_start: DateTime<Utc>,
) -> Result<()> {
    let batch_id = CatalogSyncBatchId::new(seeded_uuid("feedback-closure:catalog-baseline"));
    let batch_hash = CanonicalDigest::content_hash_typed(
        CATALOG_BASELINE_DOMAIN,
        1,
        &(batch_id, coverage_start, 0_u64, 0_u64),
    )?;
    CatalogSyncBatchEntity::insert(
        CatalogSyncBatchModel {
            catalog_sync_batch_id: batch_id,
            sync_kind: CatalogSyncKind::Baseline,
            status: CatalogSyncStatus::Committed,
            started_at: coverage_start - Duration::seconds(2),
            fetched_at: Some(coverage_start - Duration::seconds(1)),
            committed_at: Some(coverage_start),
            event_count: 0,
            market_count: 0,
            rejected_count: 0,
            batch_hash: Some(batch_hash),
            failure_stage: None,
            failure_detail: None,
            created_at: coverage_start,
            updated_at: coverage_start,
        }
        .into_active_model(),
    )
    .exec_without_returning(db)
    .await?;
    Ok(())
}

#[derive(Clone, Copy)]
struct ClosureCatalogBuild<'a> {
    scope: &'a str,
    event_id: &'a str,
    category: MarketCategory,
    decision_at: DateTime<Utc>,
    market_created_at: DateTime<Utc>,
    resolutions: &'a BTreeMap<usize, DateTime<Utc>>,
    first_ordinal: usize,
    last_ordinal: usize,
    price_shift: Decimal,
}

struct ClosureCatalogMarketBuild<'a> {
    scope: &'a str,
    event_id: &'a EventId,
    category: MarketCategory,
    decision_at: DateTime<Utc>,
    market_created_at: DateTime<Utc>,
    resolutions: &'a BTreeMap<usize, DateTime<Utc>>,
    decision_key: i64,
    price_shift: Decimal,
}

const CLOSURE_CRYPTO_CLOSE_PRICE: Decimal = dec!(100000);
const CLOSURE_CRYPTO_STRIKE_STEP: Decimal = dec!(1000);
const CLOSURE_BINANCE_RULES: &str = "This market resolves using the Binance BTCUSDT 1-minute candle close at its scheduled observation time.";

fn closure_crypto_strike(scope: &str, ordinal: usize) -> Result<Usd> {
    let strength = i64::try_from(closure_reversion_strength(scope, ordinal)?)
        .context("closure Crypto strength exceeds i64")?;
    let signed_strength = Decimal::from(closure_regime_sign(scope, ordinal)? * strength);
    let strike = CLOSURE_CRYPTO_CLOSE_PRICE - signed_strength * CLOSURE_CRYPTO_STRIKE_STEP;
    ensure!(
        strike > Decimal::ZERO,
        "closure Crypto strike must remain positive"
    );
    Ok(Usd::new(strike))
}

pub(crate) fn closure_market_text(scope: &str, ordinal: usize) -> Result<(String, Option<String>)> {
    if scope == "report-crypto" {
        let strike = closure_crypto_strike(scope, ordinal)?;
        return Ok((
            format!("Will Bitcoin reach ${}?", strike.inner().normalize()),
            Some(CLOSURE_BINANCE_RULES.to_owned()),
        ));
    }
    Ok((
        format!("Will feedback closure {scope} sample {ordinal} resolve Yes?"),
        Some("Deterministic historical source for the production closure test".to_owned()),
    ))
}

impl ClosureCatalogMarketBuild<'_> {
    fn build(&self, ordinal: usize, market_id: &MarketId) -> Result<ClosureCatalogMarket> {
        let resolution_at =
            self.resolutions.get(&ordinal).copied().with_context(|| {
                format!("closure catalog has no resolution for ordinal {ordinal}")
            })?;
        ensure!(
            resolution_at > self.decision_at,
            "closure market {market_id} must resolve after its decision"
        );
        let (bids, asks) = closure_levels(self.scope, true, self.price_shift, ordinal)?;
        let metrics = ClosureBookMetrics::from_levels(&bids, &asks)?;
        if self.scope == "report-crypto" {
            ensure!(
                self.category == MarketCategory::Crypto,
                "report-crypto catalog must retain the Crypto category"
            );
        }
        let (question, description) = closure_market_text(self.scope, ordinal)?;
        let info = MarketRegistryInfo {
            market_id: market_id.clone(),
            event_id: self.event_id.clone(),
            token_yes: TokenId::new(closure_token(self.scope, ordinal)),
            token_no: closure_no_token(self.scope, ordinal),
            question,
            slug: format!("feedback-closure-{}-market-{ordinal}", self.scope),
            description,
            categories: CategorySet::from(self.category),
            status: MarketStatus::Active,
            filter_reasons: CatalogFilterReasonSet::default(),
            outcome: None,
            neg_risk: false,
            tick_size: TickSize::QuarterCent,
            tokens: vec![
                TokenInfo {
                    token_id: TokenId::new(closure_token(self.scope, ordinal)),
                    outcome: "Yes".to_owned(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: closure_no_token(self.scope, ordinal),
                    outcome: "No".to_owned(),
                    neg_risk: false,
                },
            ],
            // Gamma catalog objects do not own live L2 top-of-book fields. The
            // exact book/depth plane is persisted independently below.
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: dec!(1),
            liquidity_usd: Some(metrics.visible_liquidity_usd),
            volume_24h: Some(Usd::new(dec!(10000))),
            start_date: Some(self.market_created_at),
            end_date: Some(resolution_at),
            resolved_at: None,
            created_at: Some(self.market_created_at),
            updated_at: self.decision_at - Duration::minutes(2),
        };
        let content_hash =
            CanonicalDigest::content_hash_typed("quant-pivot/catalog-market-object", 1, &info)?;
        Ok(ClosureCatalogMarket {
            change_id: CatalogMarketChangeId::new(seeded_uuid(&format!(
                "feedback-closure:{market_id}:{}:catalog-market-change",
                self.decision_key
            ))),
            object_id: CatalogMarketObjectId::from_content_hash(&content_hash),
            info,
            content_hash,
        })
    }
}

impl ClosureCatalogFacts {
    fn build(input: ClosureCatalogBuild<'_>) -> Result<Self> {
        let ClosureCatalogBuild {
            scope,
            event_id,
            category,
            decision_at,
            market_created_at,
            resolutions,
            first_ordinal,
            last_ordinal,
            price_shift,
        } = input;
        ensure!(
            first_ordinal <= last_ordinal,
            "closure catalog ordinal range is inverted"
        );
        ensure!(
            market_created_at < decision_at,
            "closure catalog market must exist before its decision boundary"
        );
        let effective_at = decision_at - Duration::minutes(2);
        let available_at = decision_at - Duration::minutes(1);
        let decision_key = decision_at.timestamp_micros();
        let market_ids = (first_ordinal..=last_ordinal)
            .map(|ordinal| MarketId::new(format!("feedback-closure-{scope}-market-{ordinal}")))
            .collect::<Vec<_>>();
        let event_end_date = (first_ordinal..=last_ordinal)
            .map(|ordinal| {
                resolutions.get(&ordinal).copied().with_context(|| {
                    format!("closure catalog has no resolution for ordinal {ordinal}")
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .max()
            .context("closure catalog has no market resolution")?;
        let event_id = EventId::new(event_id);
        let event = EventRegistryInfo {
            event_id: event_id.clone(),
            title: format!("Feedback closure {scope} historical cohort"),
            slug: format!("feedback-closure-{scope}-event"),
            series_slug: None,
            status: EventStatus::Active,
            market_ids: market_ids.clone(),
            categories: CategorySet::from(category),
            tags: vec![category_slug(category).to_owned()],
            neg_risk: false,
            end_date: Some(event_end_date),
            created_at: market_created_at,
            updated_at: effective_at,
        };
        let event_content_hash =
            CanonicalDigest::content_hash_typed("quant-pivot/catalog-event-object", 1, &event)?;
        let event_object_id = CatalogEventObjectId::from_content_hash(&event_content_hash);
        // Historical event objects must retain only the membership visible at
        // their decision boundary. The mutable registry projection, however,
        // is the bootstrap state observed by the real binary after every
        // cohort has been seeded, so it must cover the complete fixture scope.
        let registry_market_ids = resolutions
            .keys()
            .map(|ordinal| MarketId::new(format!("feedback-closure-{scope}-market-{ordinal}")))
            .collect::<Vec<_>>();
        let registry_market_id_set = registry_market_ids.iter().collect::<HashSet<_>>();
        ensure!(
            event
                .market_ids
                .iter()
                .all(|market_id| registry_market_id_set.contains(market_id)),
            "closure historical event membership is not contained in its registry projection"
        );
        let mut registry_event = event.clone();
        registry_event.market_ids = registry_market_ids;
        registry_event.end_date = resolutions.values().copied().max();
        let registry_event_content_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/catalog-event-object",
            1,
            &registry_event,
        )?;
        let batch_id = CatalogSyncBatchId::new(seeded_uuid(&format!(
            "feedback-closure:{scope}:{first_ordinal}:{decision_key}:catalog-batch"
        )));
        let event_change_id = CatalogEventChangeId::new(seeded_uuid(&format!(
            "feedback-closure:{scope}:{first_ordinal}:{decision_key}:catalog-event-change"
        )));
        let market_builder = ClosureCatalogMarketBuild {
            scope,
            event_id: &event_id,
            category,
            decision_at,
            market_created_at,
            resolutions,
            decision_key,
            price_shift,
        };
        let mut markets = market_ids
            .into_iter()
            .enumerate()
            .map(|(offset, market_id)| {
                let ordinal = first_ordinal
                    .checked_add(offset)
                    .context("closure catalog ordinal overflowed")?;
                market_builder.build(ordinal, &market_id)
            })
            .collect::<Result<Vec<_>>>()?;
        markets.sort_by(|left, right| left.info.market_id.cmp(&right.info.market_id));
        Ok(Self {
            batch_id,
            event_change_id,
            event_object_id,
            event,
            event_content_hash,
            registry_event,
            registry_event_content_hash,
            effective_at,
            available_at,
            category,
            markets,
        })
    }

    fn market(&self, market_id: &MarketId) -> Result<&ClosureCatalogMarket> {
        self.markets
            .binary_search_by(|market| market.info.market_id.cmp(market_id))
            .ok()
            .and_then(|index| self.markets.get(index))
            .with_context(|| format!("closure catalog is missing market {market_id}"))
    }

    fn linkage_metadata(&self, market: &ClosureCatalogMarket) -> LinkageSourceMetadata {
        LinkageSourceMetadata {
            market_id: market.info.market_id.clone(),
            slug: market.info.slug.clone(),
            question: market.info.question.clone(),
            description: market.info.description.clone(),
            series_slug: self.registry_event.series_slug.clone(),
            decision_group_market_ids: if self.category == MarketCategory::Weather {
                self.registry_event.market_ids.clone()
            } else {
                Vec::new()
            },
            end_date: market.info.end_date,
        }
    }

    fn verify_decision_ref(&self, market_id: &MarketId, actual: &CatalogDecisionRef) -> Result<()> {
        let market = self.market(market_id)?;
        ensure!(
            actual.market_content_hash == market.content_hash
                && actual.event_content_hash == self.registry_event_content_hash,
            "closure catalog re-observation changed canonical content for {market_id}"
        );
        ensure!(
            actual.market_effective_at == self.effective_at
                && actual.event_effective_at == self.effective_at
                && actual.market_timestamp_quality == CatalogTimestampQuality::Source
                && actual.event_timestamp_quality == CatalogTimestampQuality::Source,
            "closure catalog re-observation changed source-effective lineage for {market_id}"
        );
        ensure!(
            actual.market_available_at >= self.available_at
                && actual.event_available_at >= self.available_at,
            "closure catalog re-observation moved availability backwards for {market_id}"
        );
        Ok(())
    }

    fn gamma_response(
        &self,
        market: &ClosureCatalogMarket,
        event: &EventRegistryInfo,
    ) -> Result<JsonValue> {
        let info = &market.info;
        ensure!(
            info.status == MarketStatus::Active
                && info.outcome.is_none()
                && info.resolved_at.is_none(),
            "closure Gamma fixture only represents active unresolved markets"
        );
        ensure!(
            info.best_bid.is_none() && info.best_ask.is_none() && info.depth_usd.is_none(),
            "closure Gamma catalog must not embed live L2 state"
        );
        ensure!(
            info.event_id == event.event_id
                && event.market_ids.contains(&info.market_id)
                && info.primary_category() == self.category,
            "closure Gamma market {} is inconsistent with its event/category",
            info.market_id
        );
        let (token_yes, token_no) = info.resolve_token_pair()?;
        let outcomes = info
            .tokens
            .iter()
            .map(|token| token.outcome.clone())
            .collect::<Vec<_>>();
        ensure!(
            outcomes.len() == 2,
            "closure Gamma market {} is not binary",
            info.market_id
        );
        ensure!(
            event.status == EventStatus::Active,
            "closure Gamma fixture only represents active events"
        );
        let tags = event
            .tags
            .iter()
            .map(|slug| json!({"label": slug, "slug": slug}))
            .collect::<Vec<_>>();
        Ok(json!([{
            "id": info.market_id.as_str(),
            "conditionId": info.market_id.as_str(),
            "question": info.question,
            "slug": info.slug,
            "description": info.description,
            "clobTokenIds": [token_yes.as_str(), token_no.as_str()],
            "outcomes": outcomes,
            "negRisk": info.neg_risk,
            "active": true,
            "closed": false,
            "enableOrderBook": true,
            "acceptingOrders": true,
            "orderMinSize": info.min_order_size.to_string(),
            "orderPriceMinTickSize": info.tick_size.as_decimal().to_string(),
            "liquidityNum": info
                .liquidity_usd
                .map(|value| value.inner().to_string()),
            "volume24hr": info
                .volume_24h
                .map(|value| value.inner().to_string()),
            "startDate": info.start_date,
            "endDate": info.end_date,
            "createdAt": info.created_at,
            "updatedAt": info.updated_at,
            "events": [{
                "id": event.event_id.as_str(),
                "title": event.title,
                "slug": event.slug,
                "seriesSlug": event.series_slug,
                "active": true,
                "closed": false,
                "negRisk": event.neg_risk,
                "tags": tags,
                "endDate": event.end_date,
                "createdAt": event.created_at,
                "updatedAt": event.updated_at
            }]
        }]))
    }

    async fn persist(
        &self,
        db: &DatabaseConnection,
        capability_registry_hash: ContentHash,
    ) -> Result<()> {
        // Canonical projections own the FK parents used by the immutable
        // linkage ledger below. The isolated stack is not made observable
        // until fixture seeding succeeds, so establish those parents through
        // the production repositories before committing historical facts.
        self.persist_registry(db).await?;
        let market_count = i64::try_from(self.markets.len())?;
        let batch_hash = CanonicalDigest::content_hash_json(&(
            self.batch_id,
            self.event_content_hash,
            self.markets
                .iter()
                .map(|market| (market.change_id, market.content_hash))
                .collect::<Vec<_>>(),
        ))?;
        let transaction = db.begin().await?;
        self.persist_event(&transaction, market_count, batch_hash)
            .await?;
        self.persist_markets(&transaction, capability_registry_hash)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn persist_event(
        &self,
        transaction: &DatabaseTransaction,
        market_count: i64,
        batch_hash: ContentHash,
    ) -> Result<()> {
        CatalogSyncBatchEntity::insert(
            CatalogSyncBatchModel {
                catalog_sync_batch_id: self.batch_id,
                sync_kind: CatalogSyncKind::Reconcile,
                status: CatalogSyncStatus::Committed,
                started_at: self.effective_at,
                fetched_at: Some(self.available_at - Duration::seconds(1)),
                committed_at: Some(self.available_at),
                event_count: 1,
                market_count,
                rejected_count: 0,
                batch_hash: Some(batch_hash),
                failure_stage: None,
                failure_detail: None,
                created_at: self.available_at,
                updated_at: self.available_at,
            }
            .into_active_model(),
        )
        .exec_without_returning(transaction)
        .await?;
        CatalogEventObjectEntity::insert(
            CatalogEventObjectModel {
                event_object_id: self.event_object_id,
                content_hash: self.event_content_hash,
                schema_version: CATALOG_OBJECT_SCHEMA_VERSION,
                payload: ExternalJsonDocument::from(serde_json::to_value(&self.event)?),
                created_at: self.available_at,
            }
            .into_active_model(),
        )
        .exec_without_returning(transaction)
        .await?;
        CatalogEventChangeEntity::insert(
            CatalogEventChangeModel {
                event_change_id: self.event_change_id,
                catalog_sync_batch_id: self.batch_id,
                event_object_id: self.event_object_id,
                event_id: self.event.event_id.clone(),
                source_effective_at: self.effective_at,
                source_timestamp_quality: CatalogTimestampQuality::Source,
                change_type: CatalogChangeType::GammaScanUpsert,
                created_at: self.available_at,
            }
            .into_active_model(),
        )
        .exec_without_returning(transaction)
        .await?;
        Ok(())
    }

    async fn persist_markets(
        &self,
        transaction: &DatabaseTransaction,
        capability_registry_hash: ContentHash,
    ) -> Result<()> {
        let market_objects = self
            .markets
            .iter()
            .map(|market| {
                Ok(CatalogMarketObjectModel {
                    market_object_id: market.object_id,
                    content_hash: market.content_hash,
                    schema_version: CATALOG_OBJECT_SCHEMA_VERSION,
                    payload: ExternalJsonDocument::from(serde_json::to_value(&market.info)?),
                    created_at: self.available_at,
                }
                .into_active_model())
            })
            .collect::<Result<Vec<_>>>()?;
        CatalogMarketObjectEntity::insert_many(market_objects)
            .exec(transaction)
            .await?;
        CatalogMarketChangeEntity::insert_many(self.markets.iter().map(|market| {
            CatalogMarketChangeModel {
                market_change_id: market.change_id,
                catalog_sync_batch_id: self.batch_id,
                event_change_id: self.event_change_id,
                market_object_id: market.object_id,
                market_id: market.info.market_id.clone(),
                event_id: self.event.event_id.clone(),
                source_effective_at: self.effective_at,
                source_timestamp_quality: CatalogTimestampQuality::Source,
                source_created_at: market.info.created_at,
                change_type: CatalogChangeType::GammaScanUpsert,
                created_at: self.available_at,
            }
            .into_active_model()
        }))
        .exec(transaction)
        .await?;
        let domain_family = DomainFamily::for_category(self.category)
            .context("closure catalog category has no governed domain family")?;
        let linkages = self
            .markets
            .iter()
            .map(|market| {
                let metadata_hash = self.linkage_metadata(market).metadata_hash()?;
                let mut linkage = NewMarketLinkage::from_derivation(MarketLinkageDerivation {
                    market_id: market.info.market_id.clone(),
                    domain_family,
                    outcome: LinkageOutcome::Unresolved {
                        reason: LinkageUnresolvedReason::NoDeterministicTemplate,
                    },
                    confidence: Probability::ONE,
                    resolver_tier: ResolverTier::Tier1Template,
                    resolver_version: ResolverVersion::FIRST,
                    metadata_hash,
                    capability_registry_hash,
                    effective_at: self.effective_at,
                })?;
                linkage.linkage_id = MarketLinkageId::new(seeded_uuid(&format!(
                    "feedback-closure:{}:{}:market-linkage",
                    market.info.market_id,
                    self.effective_at.timestamp_micros()
                )));
                let mut active = linkage.into_active_model();
                active.created_at = Set(self.available_at);
                Ok(active)
            })
            .collect::<Result<Vec<_>>>()?;
        MarketLinkageEntity::insert_many(linkages)
            .on_conflict_do_nothing_on([MarketLinkageColumn::ContentHash])
            .exec(transaction)
            .await?;
        Ok(())
    }

    async fn persist_registry(&self, db: &DatabaseConnection) -> Result<()> {
        let event = PgEventRepository::new(db.clone())
            .upsert(UpsertEvent {
                event_id: self.registry_event.event_id.clone(),
                title: self.registry_event.title.clone(),
                slug: self.registry_event.slug.clone(),
                series_slug: self.registry_event.series_slug.clone(),
                status: self.registry_event.status,
                tags: EventTags::from(self.registry_event.tags.clone()),
                neg_risk: self.registry_event.neg_risk,
                catalog_market_ids: CatalogMarketIds::from(self.registry_event.market_ids.clone()),
                end_date: self.registry_event.end_date,
                content_hash: self.registry_event_content_hash,
            })
            .await?;
        ensure!(
            event.catalog_market_ids.as_slice() == self.registry_event.market_ids.as_slice()
                && event.content_hash == self.registry_event_content_hash,
            "closure event registry projection did not preserve complete membership"
        );
        let markets = self
            .markets
            .iter()
            .map(|market| UpsertMarket::try_from(&market.info).map_err(AnyhowError::from))
            .collect::<Result<Vec<_>>>()?;
        let expected_market_count = u64::try_from(markets.len())?;
        let persisted_market_count = PgMarketRepository::new(db.clone())
            .upsert_batch(markets)
            .await?;
        ensure!(
            persisted_market_count == expected_market_count,
            "closure registry persisted {persisted_market_count} of {expected_market_count} markets"
        );
        Ok(())
    }
}

impl FeedbackClosureFixture {
    fn new(
        feedback_cycle_id: FeedbackCycleId,
        cohorts: &[CohortSeed],
        report_cohorts: Arc<[ShadowObservationCohort]>,
        shadow_cohorts: Arc<[ShadowObservationCohort]>,
        fact_writers: Arc<ClosureFactWriters>,
        replay: Arc<ClosureReplayContext>,
        runtime_finalized_execution_evidence: FinalizedExecutionEvidence,
    ) -> Self {
        let catalogs = cohorts
            .iter()
            .map(|cohort| Arc::clone(&cohort.catalog))
            .chain(
                report_cohorts
                    .iter()
                    .map(|cohort| Arc::clone(&cohort.catalog)),
            )
            .chain(
                shadow_cohorts
                    .iter()
                    .map(|cohort| Arc::clone(&cohort.catalog)),
            )
            .collect::<Vec<_>>();
        Self {
            feedback_cycle_id,
            catalogs: Arc::from(catalogs),
            report_cohorts,
            shadow_cohorts,
            fact_writers,
            replay,
            runtime_finalized_execution_evidence,
        }
    }

    /// Complete Gamma condition-id responses for every catalog object seeded
    /// before the real binary starts. Reconciliation must observe the same
    /// normalized market content instead of replacing it with a sparse mock.
    pub(crate) fn gamma_market_responses(&self) -> Result<HashMap<String, JsonValue>> {
        Self::gamma_responses_for(&self.catalogs)
    }

    /// Exact per-token books whose cross-sectional signal the activated report
    /// must consume through the real WebSocket ingress.
    pub fn report_book_snapshots(&self) -> Result<Vec<FeedbackReportBookSnapshot>> {
        let mut snapshots = Vec::with_capacity(
            self.report_cohorts
                .iter()
                .map(|cohort| cohort.markets.len() * 2)
                .sum(),
        );
        for cohort in self.report_cohorts.iter() {
            for source in cohort.markets.iter() {
                let (scope, ordinal) = closure_market_identity(&source.market_id)?;
                for (primary, token_id) in [
                    (true, TokenId::new(closure_token(scope, ordinal))),
                    (false, closure_no_token(scope, ordinal)),
                ] {
                    let (bids, asks) =
                        closure_levels(scope, primary, cohort.book_price_shift, ordinal)?;
                    snapshots.push(FeedbackReportBookSnapshot {
                        token_id,
                        bids: bids.into_iter().collect(),
                        asks: asks.into_iter().collect(),
                    });
                }
            }
        }
        snapshots.sort_by(|left, right| left.token_id.as_str().cmp(right.token_id.as_str()));
        Ok(snapshots)
    }

    /// Wait until `ClickHouse` exposes every exact WS snapshot at the durable PIT
    /// boundary used by the imminent browser-triggered report.
    pub async fn await_report_book_snapshots(
        &self,
        expected: &[FeedbackReportBookSnapshot],
        sent_after: DateTime<Utc>,
    ) -> Result<DateTime<Utc>> {
        let deadline = Instant::now() + StdDuration::from_secs(30);
        let expected_by_token = expected
            .iter()
            .map(|snapshot| (snapshot.token_id.clone(), snapshot))
            .collect::<BTreeMap<_, _>>();
        let token_ids = expected_by_token.keys().cloned().collect::<Vec<_>>();
        loop {
            let observed_at = Utc::now();
            let rows = self
                .fact_writers
                .fact_read
                .book_ledger_snapshots_at(
                    token_ids.clone(),
                    observed_at.timestamp_millis(),
                    observed_at.timestamp_millis(),
                )
                .await?;
            let exact = rows.len() == expected_by_token.len()
                && rows.iter().all(|row| {
                    let Some(snapshot) = expected_by_token.get(&row.token_id) else {
                        return false;
                    };
                    row.venue_event_time >= sent_after.timestamp_millis()
                        && row.bid_prices
                            == snapshot
                                .bids
                                .iter()
                                .map(|level| ChPrice::from(level.price_decimal()))
                                .collect::<Vec<_>>()
                        && row.bid_sizes
                            == snapshot
                                .bids
                                .iter()
                                .map(|level| ChShares::from(level.size_decimal()))
                                .collect::<Vec<_>>()
                        && row.ask_prices
                            == snapshot
                                .asks
                                .iter()
                                .map(|level| ChPrice::from(level.price_decimal()))
                                .collect::<Vec<_>>()
                        && row.ask_sizes
                            == snapshot
                                .asks
                                .iter()
                                .map(|level| ChShares::from(level.size_decimal()))
                                .collect::<Vec<_>>()
                });
            if exact {
                return Ok(observed_at);
            }
            ensure!(
                Instant::now() < deadline,
                "exact mixed-Route WebSocket books did not become durable within 30s"
            );
            sleep(CLOSURE_POLL_INTERVAL).await;
        }
    }

    fn gamma_responses_for(
        catalogs: &[Arc<ClosureCatalogFacts>],
    ) -> Result<HashMap<String, JsonValue>> {
        let mut event_catalogs = BTreeMap::<EventId, &ClosureCatalogFacts>::new();
        let mut market_catalogs =
            BTreeMap::<MarketId, (&ClosureCatalogFacts, &ClosureCatalogMarket)>::new();
        for catalog in catalogs {
            let catalog = catalog.as_ref();
            let event_id = catalog.registry_event.event_id.clone();
            let replace_event = match event_catalogs.get(&event_id) {
                Some(existing) if existing.effective_at == catalog.effective_at => {
                    ensure!(
                        existing.registry_event_content_hash == catalog.registry_event_content_hash,
                        "closure Gamma fixture has conflicting event {event_id} versions at {}",
                        catalog.effective_at
                    );
                    false
                }
                Some(existing) => existing.effective_at < catalog.effective_at,
                None => true,
            };
            if replace_event {
                event_catalogs.insert(event_id, catalog);
            }
            for market in &catalog.markets {
                let market_id = market.info.market_id.clone();
                let replace_market = match market_catalogs.get(&market_id) {
                    Some((existing, existing_market))
                        if existing.effective_at == catalog.effective_at =>
                    {
                        ensure!(
                            existing_market.content_hash == market.content_hash,
                            "closure Gamma fixture has conflicting market {market_id} versions at {}",
                            catalog.effective_at
                        );
                        false
                    }
                    Some((existing, _)) => existing.effective_at < catalog.effective_at,
                    None => true,
                };
                if replace_market {
                    market_catalogs.insert(market_id, (catalog, market));
                }
            }
        }
        let mut responses = HashMap::new();
        for (market_id, (catalog, market)) in market_catalogs {
            let event_catalog = event_catalogs.get(&market.info.event_id).with_context(|| {
                format!(
                    "closure Gamma fixture is missing event {} for market {market_id}",
                    market.info.event_id
                )
            })?;
            let response = catalog.gamma_response(market, &event_catalog.registry_event)?;
            responses.insert(market_id.to_string(), response);
        }
        ensure!(
            !responses.is_empty(),
            "closure Gamma fixture has no catalog markets"
        );
        Ok(responses)
    }
}

struct CohortServingFacts {
    feature_rows: Vec<QuantFeatureEventRow>,
    input_rows: Vec<QuantModelInputEventRow>,
    completion: QuantServingEvidenceCompletionRow,
}

struct CohortSourceFacts {
    books: Vec<BookL2LedgerRow>,
    microstructure: Vec<BookMicrostructureRow>,
    sessions: Vec<BookStreamSessionRow>,
    executions: Vec<MarketExecutionRow>,
    participants: Vec<ExecutionParticipantRow>,
    acceptances: Vec<ExchangeHistoryAcceptanceRow>,
    domain_observations: Vec<DomainObservationRow>,
}

struct ClosureExecutionFacts {
    executions: Vec<MarketExecutionRow>,
    participants: Vec<ExecutionParticipantRow>,
    acceptance: ExchangeHistoryAcceptanceRow,
}

struct CohortInferenceResult {
    facts: CohortServingFacts,
    candidates: Vec<SignalCandidate>,
}

struct PersistedCohortPlane {
    vectors: Vec<FeatureVector>,
    captures: Vec<DecisionCaptureEvidence>,
    vector_ids: Vec<FeatureVectorId>,
    inference_rows: Vec<FactorInferenceRow>,
}

struct CohortInferenceContext<'a> {
    db: &'a DatabaseConnection,
    champion: &'a ModelVersionInfo,
    schema: &'a ExecutableFeatureSchema,
    runtime: &'a WeightedFactorRuntime,
    boundary: &'a DecisionBoundary,
    event_time: i64,
}

struct ClosureFactWriters {
    books: Arc<dyn FactWriter<BookL2LedgerRow>>,
    microstructure: Arc<dyn FactWriter<BookMicrostructureRow>>,
    sessions: Arc<dyn FactWriter<BookStreamSessionRow>>,
    executions: Arc<dyn FactWriter<MarketExecutionRow>>,
    participants: Arc<dyn FactWriter<ExecutionParticipantRow>>,
    acceptances: Arc<dyn FactWriter<ExchangeHistoryAcceptanceRow>>,
    domain_observations: Arc<dyn FactWriter<DomainObservationRow>>,
    features: Arc<dyn FactWriter<QuantFeatureEventRow>>,
    inputs: Arc<dyn FactWriter<QuantModelInputEventRow>>,
    completions: Arc<dyn FactWriter<QuantServingEvidenceCompletionRow>>,
    resolutions: Arc<dyn FactWriter<MarketResolutionRow>>,
    report_recommendations: Arc<dyn FactWriter<QuantReportRecommendationFactRow>>,
    report_funnel: Arc<dyn FactWriter<ReportMarketFunnelRow>>,
    fact_read: Arc<dyn QuantFactReadRepository>,
}

impl ClosureFactWriters {
    async fn connect(config: &ClickHouseConfig) -> Result<Self> {
        let pool = Arc::new(ClickHousePool::connect(config).await?);
        let manager = Arc::new(ChWriteManager::new(config.max_concurrent_inserts));
        let fact_read = Arc::new(ChQuantFactReadRepository::new(Arc::clone(&pool)))
            as Arc<dyn QuantFactReadRepository>;
        Ok(Self {
            books: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_book_l2_ledger",
            )),
            microstructure: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "book_microstructure_1s",
            )),
            sessions: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_book_stream_session",
            )),
            executions: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_market_execution",
            )),
            participants: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_execution_participant",
            )),
            acceptances: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_exchange_history_acceptance",
            )),
            domain_observations: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_domain_observation",
            )),
            features: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_feature_event",
            )),
            inputs: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_model_input_event",
            )),
            completions: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_serving_evidence_completion",
            )),
            resolutions: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "market_resolution_event",
            )),
            report_recommendations: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_report_recommendation_fact",
            )),
            report_funnel: Arc::new(ChFactWriter::new(
                pool,
                manager,
                "quant_report_market_funnel",
            )),
            fact_read,
        })
    }

    async fn commit_sources(&self, facts: CohortSourceFacts) -> Result<()> {
        Self::write_batches(self.books.as_ref(), facts.books)
            .await
            .context("write closure book ledger facts")?;
        Self::write_batches(self.microstructure.as_ref(), facts.microstructure)
            .await
            .context("write closure microstructure facts")?;
        Self::write_batches(self.sessions.as_ref(), facts.sessions)
            .await
            .context("write closure book session facts")?;
        Self::write_batches(self.executions.as_ref(), facts.executions)
            .await
            .context("write closure execution facts")?;
        Self::write_batches(self.participants.as_ref(), facts.participants)
            .await
            .context("write closure execution participant facts")?;
        Self::write_batches(self.acceptances.as_ref(), facts.acceptances)
            .await
            .context("write closure execution acceptance facts")?;
        Self::write_batches(self.domain_observations.as_ref(), facts.domain_observations)
            .await
            .context("write closure domain observation facts")?;
        Ok(())
    }

    async fn commit_shadow_features(&self, rows: Vec<QuantFeatureEventRow>) -> Result<()> {
        Self::write_batches(self.features.as_ref(), rows)
            .await
            .context("write closure shadow feature facts")
    }

    async fn commit_serving(&self, facts: CohortServingFacts) -> Result<()> {
        Self::write_batches(self.features.as_ref(), facts.feature_rows).await?;
        Self::write_batches(self.inputs.as_ref(), facts.input_rows).await?;
        Self::write_batches(self.completions.as_ref(), vec![facts.completion]).await?;
        Ok(())
    }

    async fn commit_report(
        &self,
        recommendations: Vec<QuantReportRecommendationFactRow>,
        funnel: Vec<ReportMarketFunnelRow>,
    ) -> Result<()> {
        Self::write_batches(self.report_recommendations.as_ref(), recommendations).await?;
        Self::write_batches(self.report_funnel.as_ref(), funnel).await?;
        Ok(())
    }

    async fn commit_resolutions(&self, rows: Vec<MarketResolutionRow>) -> Result<()> {
        Self::write_batches(self.resolutions.as_ref(), rows)
            .await
            .context("write closure market-resolution facts")
    }

    async fn write_batches<T>(writer: &dyn FactWriter<T>, rows: Vec<T>) -> Result<()>
    where
        T: Send + Sync + 'static,
    {
        let mut rows = rows.into_iter();
        loop {
            let batch = rows
                .by_ref()
                .take(FACT_WRITE_BATCH_ROWS)
                .collect::<Vec<_>>();
            if batch.is_empty() {
                return Ok(());
            }
            writer.write_batch(batch).await?;
        }
    }
}

struct ClosureReplayContext {
    builder: ConfiguredFeatureBuilder,
    factor_engine: FactorEngine,
    config: ReplayConfig,
    selection: SelectionConfig,
    loader: HistoricalWindowLoader,
    lookback: StdDuration,
    knowledge_lag: StdDuration,
}

impl ClosureReplayContext {
    fn build(
        db: &DatabaseConnection,
        fact_read: &Arc<dyn QuantFactReadRepository>,
        policy: &ActivePolicyBundle,
        champion: &ModelVersionInfo,
    ) -> Result<Self> {
        let profile = &policy.snapshot.profile_artifacts;
        let features = &profile.features.definition;
        let factors = &profile.scoring.definition;
        let domain = &profile.domain.definition;
        let research_profile = champion
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(AnyhowError::msg)?;
        let builder = ConfiguredFeatureBuilder::new_for_contract(
            features,
            domain,
            research_profile.spec.feature_contract,
        )?;
        let factor_engine = FactorEngine::for_model_scope(
            factors,
            features,
            domain,
            research_profile.spec.feature_contract,
            champion.category_scope,
            None,
        );
        let bindings = champion.serving_contract.bindings();
        ensure!(
            bindings.factors.bias_table.is_none(),
            "closure replay requires an explicit bias-table loader before seeding a biased champion"
        );
        ensure!(
            ResearchHasher::feature_schema(builder.schema())?
                == bindings.schemas.feature_schema_hash
                && factor_engine.serving_plane()? == &bindings.factors.plane,
            "closure replay contract differs from the exact champion serving plane"
        );
        let catalog = Arc::new(PgCatalogLedgerRepository::new(db.clone()))
            as Arc<dyn CatalogLedgerRepository>;
        let clob_market_info = Arc::new(PgClobMarketInfoRepository::new(db.clone()))
            as Arc<dyn ClobMarketInfoRepository>;
        let linkages = Arc::new(PgMarketLinkageRepository::new(db.clone()))
            as Arc<dyn MarketLinkageRepository>;
        let calibrations = Arc::new(PgCalibrationArtifactRepository::new(db.clone()))
            as Arc<dyn CalibrationArtifactRepository>;
        let max_book_staleness =
            StdDuration::from_millis(profile.research_method.training.max_book_staleness_ms);
        let knowledge_lag_secs = policy
            .snapshot
            .pit_knowledge_lag_secs()
            .context("closure policy has no unique enabled report knowledge lag")?;
        let feature_contract = bindings
            .model
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(AnyhowError::msg)?
            .spec
            .feature_contract;
        Ok(Self {
            builder,
            factor_engine,
            config: ReplayConfig {
                features: features.clone(),
                factors: factors.clone(),
                domain: domain.clone(),
                data_quality: policy.snapshot.recommendation.data_quality.clone(),
                feature_contract,
                liquidity_cap_usd: Usd::new(
                    policy
                        .snapshot
                        .execution_risk
                        .portfolio
                        .exposure_limits
                        .max_single_recommendation_usd
                        .value,
                ),
                bias_table: None,
            },
            selection: policy.snapshot.recommendation.selection.clone(),
            loader: HistoricalWindowLoader::new(
                Arc::clone(fact_read),
                Arc::clone(&catalog),
                clob_market_info,
                linkages,
                calibrations,
                max_book_staleness,
            ),
            lookback: StdDuration::from_secs(features.max_lookback_secs()),
            knowledge_lag: StdDuration::from_secs(knowledge_lag_secs),
        })
    }
}

struct SelectionModelBuild<'a> {
    db: &'a DatabaseConnection,
    facts: &'a ClosureFactWriters,
    replay: &'a ClosureReplayContext,
    infra: &'a SharedDemoInfra,
    champion: &'a ModelVersionInfo,
    runtime: &'a WeightedFactorRuntime,
    decision_at: DateTime<Utc>,
    expected_markets: &'a HashSet<MarketId>,
}

async fn build_selection_model(input: SelectionModelBuild<'_>) -> Result<MarketSelectionModel> {
    let SelectionModelBuild {
        db,
        facts,
        replay,
        infra,
        champion,
        runtime,
        decision_at,
        expected_markets,
    } = input;
    let boundary = DecisionClock::new(replay.knowledge_lag.as_secs()).serving_boundary(
        decision_at,
        replay.config.domain.crypto.availability_lag_secs,
        replay.config.domain.weather.availability_lag_secs,
    )?;
    let catalog =
        Arc::new(PgCatalogLedgerRepository::new(db.clone())) as Arc<dyn CatalogLedgerRepository>;
    let clob_market_info =
        Arc::new(PgClobMarketInfoRepository::new(db.clone())) as Arc<dyn ClobMarketInfoRepository>;
    let linkages =
        Arc::new(PgMarketLinkageRepository::new(db.clone())) as Arc<dyn MarketLinkageRepository>;
    let pit = Arc::new(DurablePitSource::new(
        Arc::clone(&facts.fact_read),
        catalog,
        clob_market_info,
    ));
    let provider = MarketCandidateProvider::new(pit, linkages, Arc::clone(&facts.fact_read));
    let candidate_batch = provider
        .candidates(&boundary, &replay.config.domain)
        .await?;
    let required_features = runtime.required_features();
    let model_requirements = match champion.category_scope {
        Some(category) => ModelFeatureRequirements {
            generic: Vec::new(),
            by_category: BTreeMap::from([(category, required_features)]),
        },
        None => ModelFeatureRequirements::generic_only(required_features),
    };
    let candidates = candidate_batch.candidates;
    let snapshot = ConfiguredMarketSelector::new()
        .build_snapshot(
            MarketSelectionBuildRequest {
                decision_at,
                decision_policy_snapshot_id: infra.decision_policy_snapshot_id,
                selection: replay.selection.clone(),
                data_quality: replay.config.data_quality.clone(),
                features: replay.config.features.clone(),
                model_requirements,
                knowledge_lag_secs: replay.knowledge_lag.as_secs(),
            },
            candidates.clone(),
        )
        .await?;
    let included = snapshot
        .included
        .iter()
        .map(|market| market.market_id.clone())
        .collect::<HashSet<_>>();
    ensure!(
        included == *expected_markets,
        "closure production selector included {} markets, expected {}; unexpected={:?}; missing={:?}",
        included.len(),
        expected_markets.len(),
        included
            .difference(expected_markets)
            .take(5)
            .collect::<Vec<_>>(),
        expected_markets
            .difference(&included)
            .take(5)
            .collect::<Vec<_>>()
    );
    Ok(map_snapshot_to_model(&snapshot, &candidates)?)
}

async fn seed_cohort_sources(
    db: &DatabaseConnection,
    sources: &[ClosureMarketSource],
    decision_at: DateTime<Utc>,
    book_price_shift: Decimal,
    fact_writers: &ClosureFactWriters,
    replay: &ClosureReplayContext,
) -> Result<()> {
    let observation_count = sources.len();
    let mut book_rows = Vec::with_capacity(observation_count * 2);
    let mut microstructure_rows = Vec::with_capacity(observation_count * 65);
    let mut session_rows = Vec::with_capacity(observation_count * 2);
    let mut execution_rows = Vec::with_capacity(observation_count * 20);
    let mut participant_rows = Vec::with_capacity(observation_count * 40);
    let mut acceptance_rows = Vec::with_capacity(observation_count);
    let mut market_infos = Vec::with_capacity(observation_count);
    for source in sources {
        let (scope, market_ordinal) = closure_market_identity(&source.market_id)?;
        let ClosureBookFacts {
            ledger_rows,
            session_rows: source_sessions,
            market_info,
            ..
        } = closure_book_facts(
            source,
            decision_at,
            replay.knowledge_lag.as_secs(),
            book_price_shift,
        )?;
        book_rows.extend(ledger_rows);
        microstructure_rows.extend(closure_microstructure_rows(
            source,
            decision_at,
            replay.knowledge_lag.as_secs(),
            closure_yes_wins(scope, market_ordinal)?,
            book_price_shift,
        )?);
        let execution_facts = closure_execution_history_rows(
            source,
            decision_at,
            replay.knowledge_lag.as_secs(),
            book_price_shift,
        )?;
        execution_rows.extend(execution_facts.executions);
        participant_rows.extend(execution_facts.participants);
        acceptance_rows.push(execution_facts.acceptance);
        session_rows.extend(source_sessions);
        market_infos.push(market_info);
    }
    let market_info_repository = Arc::new(PgClobMarketInfoRepository::new(db.clone()));
    let inserted_market_infos = stream::iter(market_infos)
        .map(|market_info| {
            let repository = Arc::clone(&market_info_repository);
            async move { repository.insert_observation(market_info).await }
        })
        .buffer_unordered(SOURCE_SEED_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    ensure!(
        inserted_market_infos.len() == observation_count,
        "closure CLOB market-info seed is incomplete"
    );
    fact_writers
        .commit_sources(CohortSourceFacts {
            books: book_rows,
            microstructure: microstructure_rows,
            sessions: session_rows,
            executions: execution_rows,
            participants: participant_rows,
            acceptances: acceptance_rows,
            domain_observations: Vec::new(),
        })
        .await
}

struct ClosedPositionSeed<'a> {
    ids: &'a ExecutionTxnIds,
    recommendation: &'a NewRecommendation,
    intent_id: OrderIntentId,
    market_id: &'a MarketId,
    token_id: &'a TokenId,
    entry_shares: Shares,
    entry_cost_usd: Usd,
    exit_price: Decimal,
    entry_at: DateTime<Utc>,
    exit_at: DateTime<Utc>,
}

impl ClosedPositionSeed<'_> {
    async fn insert(self, transaction: &DatabaseTransaction) -> Result<PositionModel> {
        let proceeds_usd = self.entry_shares * Price::new(self.exit_price);
        let realized_pnl_usd = proceeds_usd - self.entry_cost_usd;
        let mut active = NewPosition {
            position_id: PositionId::from_v7(),
            order_intent_id: self.intent_id,
            execution_account_id: self.ids.execution_account,
            token_id: self.token_id.clone(),
            market_id: self.market_id.clone(),
            event_id: Some(EventId::new(&self.recommendation.event_id)),
            category: MarketCategory::Weather,
            side: OutcomeSide::Yes,
            state: PositionLedgerState::Closed,
            shares: Shares::ZERO,
            avg_price: Price::new(self.entry_cost_usd.inner() / self.entry_shares.inner()),
            cost_usd: Usd::ZERO,
            realized_pnl_usd,
            source: AccountSource::Polymarket,
            opened_at: self.entry_at,
            closed_at: Some(self.exit_at),
        }
        .into_active_model();
        active.updated_at = Set(self.exit_at);
        Ok(PositionEntity::insert(active)
            .exec_with_returning(transaction)
            .await?)
    }
}

struct ClosureExecutionGraph(ExecutionAttemptSourceGraph);

impl ClosureExecutionGraph {
    async fn seal(self, transaction: &DatabaseTransaction) -> Result<ExecutionAttemptOutcomeInfo> {
        let source_observed_at = self.0.source_observed_at();
        let outcome = match self.0.derive()? {
            ExecutionAttemptDerivation::Ready(outcome) => *outcome,
            ExecutionAttemptDerivation::Deferred(reason) => {
                anyhow::bail!("closure execution source unexpectedly deferred: {reason:?}")
            }
        };
        let available_at = source_observed_at + Duration::minutes(1);
        let outcome_hash = outcome.expected_outcome_hash(source_observed_at, available_at)?;
        let mut active = outcome.into_active_model();
        active.source_observed_at = Set(source_observed_at);
        active.available_at = Set(available_at);
        active.outcome_hash = Set(outcome_hash);
        active.created_at = Set(available_at);
        let stored = ExecutionAttemptOutcomeEntity::insert(active)
            .exec_with_returning(transaction)
            .await?;
        let info = ExecutionAttemptOutcomeInfo::from(stored);
        info.validate()?;
        Ok(info)
    }
}

struct CohortSeedContext<'a> {
    db: &'a DatabaseConnection,
    artifacts: &'a Arc<dyn ArtifactStore>,
    infra: &'a SharedDemoInfra,
    champion: &'a ModelVersionInfo,
    schema: &'a ExecutableFeatureSchema,
    runtime: &'a WeightedFactorRuntime,
    facts: &'a ClosureFactWriters,
    replay: &'a ClosureReplayContext,
    capability_registry_hash: ContentHash,
    account_capital_usd: Usd,
    runtime_finalized_execution_evidence: &'a FinalizedExecutionEvidence,
}

struct ClosureSeedInputs {
    champion: ModelVersionInfo,
    model_spec: ModelSpecInfo,
    policy: ActivePolicyBundle,
    account_capital_usd: Usd,
}

impl ClosureSeedInputs {
    async fn load(
        db: &DatabaseConnection,
        champion_model_version_id: ModelVersionId,
    ) -> Result<Self> {
        let registry = PgModelRegistryRepository::new(db.clone());
        let champion = registry
            .find_model_version(&champion_model_version_id)
            .await?
            .with_context(|| format!("closure champion {champion_model_version_id} is missing"))?;
        let model_spec = registry
            .find_model_spec(&champion.model_spec_id)
            .await?
            .with_context(|| format!("closure model spec {} is missing", champion.model_spec_id))?;
        let policy = PgPolicyRepository::new(db.clone())
            .load_current_bundle()
            .await?
            .context("closure policy bundle is missing")?;
        let account_capital_usd = Usd::new(
            policy
                .snapshot
                .execution_risk
                .portfolio
                .budget
                .total_budget_usd
                .value,
        );
        Ok(Self {
            champion,
            model_spec,
            policy,
            account_capital_usd,
        })
    }
}

struct CohortSpecification<'a> {
    scope: &'static str,
    decision_at: DateTime<Utc>,
    market_created_at: DateTime<Utc>,
    resolutions: &'a BTreeMap<usize, DateTime<Utc>>,
    first_ordinal: usize,
    observation_count: usize,
    book_price_shift: Decimal,
}

impl CohortSpecification<'_> {
    fn last_ordinal(&self) -> Result<usize> {
        ensure!(self.observation_count > 0, "closure report cannot be empty");
        self.first_ordinal
            .checked_add(self.observation_count - 1)
            .context("closure report ordinal range overflowed")
    }

    fn report_config(&self) -> ReportSeedConfig {
        ReportSeedConfig {
            event_id: format!("feedback-closure-{}-event", self.scope),
            market_id: format!(
                "feedback-closure-{}-market-{}",
                self.scope, self.first_ordinal
            ),
            market_question: format!(
                "Will feedback closure {} sample {} resolve Yes?",
                self.scope, self.first_ordinal
            ),
            market_slug: format!(
                "feedback-closure-{}-market-{}",
                self.scope, self.first_ordinal
            ),
            token_id: closure_token(self.scope, self.first_ordinal),
            trigger_key: format!(
                "feedback-closure:{}:{}",
                self.scope,
                RecommendationReportId::from_v7()
            ),
        }
    }

    fn sources(&self, last_ordinal: usize) -> Vec<ClosureMarketSource> {
        (self.first_ordinal..=last_ordinal)
            .map(|ordinal| ClosureMarketSource {
                source_id: RecommendationId::from_v7(),
                market_id: MarketId::new(format!(
                    "feedback-closure-{}-market-{ordinal}",
                    self.scope
                )),
            })
            .collect()
    }

    fn resolution_map(&self, last_ordinal: usize) -> Result<BTreeMap<MarketId, DateTime<Utc>>> {
        (self.first_ordinal..=last_ordinal)
            .map(|ordinal| {
                let resolved_at = self.resolutions.get(&ordinal).copied().with_context(|| {
                    format!("closure market {}/{ordinal} has no resolution", self.scope)
                })?;
                ensure!(
                    self.decision_at < resolved_at,
                    "closure market {}/{ordinal} must resolve after its decision boundary",
                    self.scope
                );
                Ok((
                    MarketId::new(format!("feedback-closure-{}-market-{ordinal}", self.scope)),
                    resolved_at,
                ))
            })
            .collect()
    }
}

fn build_cohort_recommendations(
    sources: &[ClosureMarketSource],
    ids: &ExecutionTxnIds,
    options: &ReportBuildOptions,
    specification: &CohortSpecification<'_>,
    last_ordinal: usize,
    knowledge_lag_secs: u64,
) -> Result<Vec<NewRecommendation>> {
    let mut recommendations = Vec::with_capacity(specification.observation_count);
    recommendations.push(
        options
            .recommendations
            .first()
            .cloned()
            .context("closure report is missing its first recommendation")?,
    );
    for (source, ordinal) in sources
        .iter()
        .skip(1)
        .zip((specification.first_ordinal + 1)..=last_ordinal)
    {
        let rank = i32::try_from(ordinal).context("closure rank exceeds i32")?;
        recommendations.push(demo_recommendation(
            source.source_id,
            ids.report,
            ids,
            rank,
            &format!("feedback-closure-{}-market-{ordinal}", specification.scope),
            &ids.event,
            &closure_token(specification.scope, ordinal),
        ));
    }
    for recommendation in &mut recommendations {
        let (_, ordinal) = closure_market_identity(&recommendation.market_id)?;
        let (bids, asks) = closure_levels(
            specification.scope,
            true,
            specification.book_price_shift,
            ordinal,
        )?;
        let metrics = ClosureBookMetrics::from_levels(&bids, &asks)?;
        recommendation.market_context.best_bid = Some(metrics.best_bid);
        recommendation.market_context.best_ask = Some(metrics.best_ask);
        recommendation.market_context.mid_price = Some(metrics.mid_price);
        recommendation.market_context.spread_bps = Some(metrics.spread_bps);
        recommendation.market_context.depth_usd = metrics.visible_liquidity_usd;
        recommendation.market_context.book_age_ms = 0;
        recommendation.evidence_refs.book_snapshot_ref = closure_book_facts(
            &ClosureMarketSource::from(&*recommendation),
            specification.decision_at,
            knowledge_lag_secs,
            specification.book_price_shift,
        )?
        .primary_ref;
    }
    Ok(recommendations)
}

async fn seed_training_cohorts(
    context: &CohortSeedContext<'_>,
    plan: &FeedbackCycleFreezePlan,
    group_count: usize,
    scenario_training: &ScenarioTrainingPlan,
) -> Result<Vec<CohortSeed>> {
    let points = scenario_training.points(group_count)?;
    ensure!(
        TRAINING_OBSERVATION_COUNT.is_multiple_of(group_count),
        "closure training budget must divide evenly across validation groups"
    );
    let observation_count = TRAINING_OBSERVATION_COUNT / group_count;
    ensure!(
        observation_count.is_multiple_of(8),
        "closure training group size must preserve the complete regime/strength factorial"
    );
    let mut resolutions = BTreeMap::new();
    for (group_index, decision_at) in points.iter().copied().enumerate() {
        let (first_ordinal, last_ordinal) = training_market_range(group_index, observation_count)?;
        let resolved_at = decision_at + Duration::seconds(TRAINING_RESOLUTION_LAG_SECS);
        ensure!(
            resolved_at + Duration::seconds(TRAINING_TRUTH_BUFFER_SECS) <= plan.training().cutoff(),
            "closure rolling market resolution is not PIT-mature by training cutoff"
        );
        for ordinal in first_ordinal..=last_ordinal {
            resolutions.insert(ordinal, resolved_at);
        }
    }
    let mut cohorts = Vec::with_capacity(group_count);
    for (group_index, decision_at) in points.into_iter().enumerate() {
        let (first_ordinal, _) = training_market_range(group_index, observation_count)?;
        cohorts.push(
            Box::pin(seed_report(
                context,
                CohortSpecification {
                    scope: "training",
                    decision_at,
                    market_created_at: plan.training().window_start() - Duration::days(1),
                    resolutions: &resolutions,
                    first_ordinal,
                    observation_count,
                    book_price_shift: training_book_price_shift(group_index),
                },
            ))
            .await?,
        );
    }
    Ok(cohorts)
}

async fn seed_calibration_cohorts(
    context: &CohortSeedContext<'_>,
    plan: &FeedbackCycleFreezePlan,
) -> Result<Vec<CohortSeed>> {
    ensure!(
        CALIBRATION_OBSERVATION_COUNT.is_multiple_of(CALIBRATION_GROUP_COUNT),
        "closure calibration budget must divide evenly across decision groups"
    );
    let group_size = CALIBRATION_OBSERVATION_COUNT / CALIBRATION_GROUP_COUNT;
    let latest = plan
        .calibration()
        .cutoff()
        .checked_sub_signed(Duration::days(1) + Duration::minutes(2))
        .context("closure calibration maturity boundary overflowed")?;
    let points = interior_points(
        plan.calibration().window_start(),
        latest,
        CALIBRATION_GROUP_COUNT,
    )?;
    let mut resolutions = BTreeMap::new();
    for (group_index, decision_at) in points.iter().copied().enumerate() {
        let (first_ordinal, last_ordinal) = calibration_market_range(group_index, group_size)?;
        let resolved_at = decision_at + Duration::days(1);
        ensure!(
            resolved_at + Duration::minutes(2) <= plan.calibration().cutoff(),
            "closure calibration resolution is not PIT-mature by cutoff"
        );
        for ordinal in first_ordinal..=last_ordinal {
            resolutions.insert(ordinal, resolved_at);
        }
    }
    let mut cohorts = Vec::with_capacity(CALIBRATION_GROUP_COUNT);
    for (group_index, decision_at) in points.into_iter().enumerate() {
        let (first_ordinal, _) = calibration_market_range(group_index, group_size)?;
        cohorts.push(
            Box::pin(seed_report(
                context,
                CohortSpecification {
                    scope: "calibration",
                    decision_at,
                    market_created_at: plan.calibration().window_start() - Duration::days(1),
                    resolutions: &resolutions,
                    first_ordinal,
                    observation_count: group_size,
                    book_price_shift: Decimal::ZERO,
                },
            ))
            .await?,
        );
    }
    Ok(cohorts)
}

async fn seed_evaluation_cohorts(
    context: &CohortSeedContext<'_>,
    plan: &FeedbackCycleFreezePlan,
) -> Result<Vec<CohortSeed>> {
    // Comparison evidence is defined over independent decision ticks, not the
    // number of recommendations inside one cross-section.
    let points = evaluation_decision_points(
        plan.evaluation().window_start(),
        plan.evaluation().cutoff(),
        EVALUATION_OBSERVATION_COUNT,
    )?;
    let resolutions = points.iter().copied().enumerate().try_fold(
        BTreeMap::<usize, DateTime<Utc>>::new(),
        |mut resolutions, (index, decision_at)| {
            let (first_ordinal, last_ordinal) = evaluation_market_range(index)?;
            let resolved_at = decision_at + Duration::days(1);
            for ordinal in first_ordinal..=last_ordinal {
                resolutions
                    .entry(ordinal)
                    .and_modify(|existing| *existing = (*existing).max(resolved_at))
                    .or_insert(resolved_at);
            }
            Ok::<_, AnyhowError>(resolutions)
        },
    )?;
    let market_created_at = plan.evaluation().window_start() - Duration::days(1);
    let resolutions = &resolutions;
    let mut prepared = stream::iter(points.into_iter().enumerate())
        .map(|(index, decision_at)| {
            let market_range = evaluation_market_range(index);
            async move {
                let (first_ordinal, _) = market_range?;
                PreparedCohort::prepare(
                    context,
                    CohortSpecification {
                        scope: "evaluation",
                        decision_at,
                        market_created_at,
                        resolutions,
                        first_ordinal,
                        observation_count: EVALUATION_MARKETS_PER_TICK,
                        book_price_shift: evaluation_book_price_shift(index),
                    },
                )
                .await
            }
        })
        .buffer_unordered(DECISION_SEED_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    prepared.sort_by_key(|cohort| cohort.cohort.decision_at);
    let mut cohorts = Vec::with_capacity(prepared.len());
    for cohort in prepared {
        cohorts.push(Box::pin(cohort.publish(context.db, context.artifacts, context.facts)).await?);
    }
    ensure!(
        cohorts.len() == EVALUATION_OBSERVATION_COUNT,
        "closure fixture materialized {} of {} evaluation decision ticks",
        cohorts.len(),
        EVALUATION_OBSERVATION_COUNT
    );
    Ok(cohorts)
}

fn historical_feedback_seed(
    evaluation_seeds: &[CohortSeed],
) -> Result<(Arc<[Decimal]>, RecommendationId)> {
    let observation_price_shifts = Arc::<[Decimal]>::from(
        evaluation_seeds
            .iter()
            .map(|cohort| cohort.book_price_shift)
            .collect::<Vec<_>>(),
    );
    let historical_cohort = evaluation_seeds
        .iter()
        .rev()
        .find(|cohort| !cohort.recommendations.is_empty())
        .context("closure fixture has no historical emitted recommendation")?;
    ensure!(
        historical_cohort.market_universe.len() == EVALUATION_MARKETS_PER_TICK,
        "closure historical decision universe has {} markets, expected {}",
        historical_cohort.market_universe.len(),
        EVALUATION_MARKETS_PER_TICK
    );
    let historical_recommendation_id = historical_cohort
        .recommendations
        .first()
        .context("closure historical decision tick emitted no recommendation")?
        .recommendation_id;
    Ok((observation_price_shifts, historical_recommendation_id))
}

/// Inputs that jointly define one deterministic production-closure seed.
pub(crate) struct FeedbackClosureSeedRequest<'a> {
    pub(crate) db: &'a DatabaseConnection,
    pub(crate) clickhouse_config: &'a ClickHouseConfig,
    pub(crate) artifact_store: &'a Arc<dyn ArtifactStore>,
    pub(crate) infra: &'a SharedDemoInfra,
    pub(crate) champion_model_version_id: ModelVersionId,
    pub(crate) historical_feedback_cycle_id: FeedbackCycleId,
    pub(crate) report_resolves_at: DateTime<Utc>,
    pub(crate) runtime_finalized_execution_evidence: FinalizedExecutionEvidence,
}

impl FeedbackClosureSeedRequest<'_> {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.runtime_finalized_execution_evidence
                .runtime_parts()
                .is_some(),
            "closure live-inference fixture requires frozen finalized-execution evidence"
        );
        Ok(())
    }
}

/// Seed disjoint PIT cohorts, an approved recipe, and one queued cycle.
pub(crate) async fn seed_feedback_closure(
    request: FeedbackClosureSeedRequest<'_>,
) -> Result<FeedbackClosureFixture> {
    request.validate()?;
    let db = request.db;
    let ClosureSeedInputs {
        champion,
        model_spec,
        policy,
        account_capital_usd,
    } = ClosureSeedInputs::load(db, request.champion_model_version_id).await?;
    let validation = &policy
        .snapshot
        .profile_artifacts
        .research_method
        .research
        .validation;
    let serving_bindings = champion.serving_contract.bindings();
    ensure!(
        serving_bindings.model.model_family == ModelFamily::WeightedFactor,
        "closure serving-evidence fixture requires a weighted-factor champion"
    );
    let capability_registry_hashes = serving_bindings.capability_registry_hashes.as_slice();
    ensure!(
        capability_registry_hashes.len() == 1,
        "closure serving-evidence fixture requires exactly one capability-registry revision"
    );
    let capability_registry_hash = capability_registry_hashes[0];
    let artifact = ModelArtifact::load_verified(request.artifact_store.as_ref(), &champion).await?;
    let calibration_loader = CoreCalibrationArtifactLoader::new(Arc::new(
        PgCalibrationArtifactRepository::new(db.clone()),
    )
        as Arc<dyn CalibrationArtifactRepository>);
    let calibration = resolve_return_model_calibration(&calibration_loader, &artifact).await?;
    let runtime = WeightedFactorRuntime::new(artifact, calibration)?;
    let profile = fixture_profile_ref()
        .resolve_builtin_research_profile()
        .map_err(AnyhowError::msg)?;
    let schema = ExecutableFeatureSchema::build(
        &policy.snapshot.profile_artifacts.features.definition,
        profile.spec.feature_contract,
    )?;
    let fact_writers = Arc::new(ClosureFactWriters::connect(request.clickhouse_config).await?);
    let replay = Arc::new(ClosureReplayContext::build(
        db,
        &fact_writers.fact_read,
        &policy,
        &champion,
    )?);
    let database_now = db.statement_time().await;
    let plan = FeedbackCycleFreezePlan::derive(
        &profile,
        champion.model_spec_id,
        champion.model_spec_definition_hash,
        policy.decision_policy_snapshot_id,
        policy.snapshot_hash,
        database_now,
    )?;
    let scenario_training =
        ScenarioTrainingPlan::load(request.artifact_store, &policy, &plan, database_now).await?;
    let training_group_count = closure_training_groups(
        validation.cpcv.n_groups,
        validation.cpcv.nested_estimator_min_groups,
        validation.pbo.block_count,
        scenario_training.group_floor,
    )?;
    seed_catalog_baseline(db, plan.source_start()).await?;
    let calibration_artifact_id = serving_bindings
        .model
        .calibration
        .as_ref()
        .context("closure champion must bind calibration")?
        .artifact_id;

    let closure_infra = SharedDemoInfra {
        feature_parity_state_id: request.infra.feature_parity_state_id,
        decision_policy_snapshot_id: request.infra.decision_policy_snapshot_id,
        model_version_id: champion.model_version_id,
        calibration_artifact_id,
        model_run_id: request.infra.model_run_id,
        trade_policy: request.infra.trade_policy.clone(),
        factor_serving_plane: serving_bindings.factors.plane.clone(),
    };
    let seed_context = CohortSeedContext {
        db,
        artifacts: request.artifact_store,
        infra: &closure_infra,
        champion: &champion,
        schema: &schema,
        runtime: &runtime,
        facts: fact_writers.as_ref(),
        replay: replay.as_ref(),
        capability_registry_hash,
        account_capital_usd,
        runtime_finalized_execution_evidence: &request.runtime_finalized_execution_evidence,
    };
    let mut seeded = Box::pin(seed_training_cohorts(
        &seed_context,
        &plan,
        training_group_count,
        &scenario_training,
    ))
    .await?;
    seeded.extend(Box::pin(seed_calibration_cohorts(&seed_context, &plan)).await?);
    let evaluation_seeds = Box::pin(seed_evaluation_cohorts(&seed_context, &plan)).await?;
    let execution_attempts = seed_execution_attempts(db, &evaluation_seeds).await?;
    let (observation_price_shifts, historical_recommendation_id) =
        historical_feedback_seed(&evaluation_seeds)?;
    seeded.extend(evaluation_seeds);
    seeded.sort_by_key(|cohort| cohort.decision_at);
    align_report_history(db, &seeded).await?;
    let resolution_facts = closure_resolution_facts(&seeded, plan.label_cutoff())?;
    fact_writers
        .commit_resolutions(resolution_facts.values().cloned().collect())
        .await?;
    stream::iter(&seeded)
        .map(|cohort| cohort.persist_rows(db, &resolution_facts))
        .buffer_unordered(SOURCE_SEED_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    seal_execution_rollups(db, &seeded, execution_attempts).await?;
    seed_historical_diagnostic(
        db,
        request.artifact_store,
        request.historical_feedback_cycle_id,
        plan.label_cutoff(),
        &champion,
        &model_spec,
        historical_recommendation_id,
    )
    .await?;
    seed_recipe(db, &profile, &champion, &model_spec, validation).await?;
    let cycle = trigger_cycle(db, &profile, &champion, &policy, plan.label_cutoff()).await?;
    let report_cohorts =
        seed_report_catalogs(db, capability_registry_hash, request.report_resolves_at).await?;
    // The real binary must never observe a catalog row that is inserted later
    // with an earlier availability time. Seed the complete shadow universe
    // before startup so every online report and subsequent replay sees the
    // same append-only catalog history.
    let shadow_catalog_anchor = db.statement_time().await;
    let shadow_cohorts = seed_shadow_catalogs(
        db,
        observation_price_shifts.as_ref(),
        capability_registry_hash,
        shadow_catalog_anchor,
    )
    .await?;
    Ok(FeedbackClosureFixture::new(
        cycle.feedback_cycle_id,
        &seeded,
        report_cohorts,
        shadow_cohorts,
        fact_writers,
        replay,
        request.runtime_finalized_execution_evidence,
    ))
}

/// A CPCV partition is not one decision-time group. Every outer fold must
/// retain independent model, calibration, and scenario populations while the
/// PBO block floor remains satisfied.
fn closure_training_groups(
    cpcv_partitions: u32,
    nested_group_floor: u32,
    pbo_blocks: u32,
    scenario_bucket_floor: usize,
) -> Result<usize> {
    let cpcv_partitions =
        usize::try_from(cpcv_partitions).context("closure CPCV N exceeds usize")?;
    let nested_group_floor = usize::try_from(nested_group_floor)
        .context("closure nested estimator group floor exceeds usize")?;
    let cpcv_groups = cpcv_partitions
        .checked_mul(nested_group_floor)
        .context("closure CPCV training group count overflowed")?;
    let pbo_groups =
        usize::try_from(pbo_blocks).context("closure PBO block count exceeds usize")?;
    let minimum_groups = cpcv_groups.max(pbo_groups).max(scenario_bucket_floor);
    (minimum_groups..=TRAINING_OBSERVATION_COUNT)
        .find(|group_count| {
            TRAINING_OBSERVATION_COUNT.is_multiple_of(*group_count)
                && (TRAINING_OBSERVATION_COUNT / *group_count).is_multiple_of(8)
        })
        .with_context(|| {
            format!(
                "closure training budget cannot satisfy CPCV/PBO/scenario floors from {minimum_groups} groups while preserving the complete eight-cell factorial"
            )
        })
}

fn closure_linkage_resolver() -> Result<LayeredResolver> {
    LayeredResolver::try_deterministic(
        WeatherStationRegistry::default(),
        &WeatherVerticalBindingsConfig::default(),
    )
    .map_err(AnyhowError::from)
}

async fn persist_crypto_linkages(
    db: &DatabaseConnection,
    catalog: &ClosureCatalogFacts,
    capability_registry_hash: ContentHash,
) -> Result<()> {
    ensure!(
        catalog.category == MarketCategory::Crypto,
        "resolved report linkage fixture requires a Crypto catalog"
    );
    let resolver = closure_linkage_resolver()?;
    let effective_at = db.statement_time().await;
    let mut linkages = Vec::with_capacity(catalog.markets.len());
    for market in &catalog.markets {
        let (scope, _) = closure_market_identity(&market.info.market_id)?;
        ensure!(
            scope == "report-crypto",
            "resolved Crypto linkage fixture received non-report scope `{scope}`"
        );
        let metadata = catalog.linkage_metadata(market);
        let metadata_hash = metadata.metadata_hash()?;
        let resolution = resolver.resolve(&metadata, effective_at)?;
        ensure!(
            resolution.resolver_tier == ResolverTier::Tier1Template
                && matches!(&resolution.outcome, LinkageOutcome::Resolved(_)),
            "report Crypto market {} did not clear the production Tier-1 resolver: tier={:?} outcome={:?}",
            market.info.market_id,
            resolution.resolver_tier,
            resolution.outcome
        );
        linkages.push(NewMarketLinkage::from_derivation(
            MarketLinkageDerivation {
                market_id: market.info.market_id.clone(),
                domain_family: DomainFamily::Crypto,
                outcome: resolution.outcome,
                confidence: resolution.confidence,
                resolver_tier: resolution.resolver_tier,
                resolver_version: resolution.resolver_version,
                metadata_hash,
                capability_registry_hash,
                effective_at,
            },
        )?);
    }
    let expected = linkages.len();
    let rows = PgMarketLinkageRepository::new(db.clone())
        .append_batch(linkages)
        .await?;
    ensure!(
        rows.len() == expected && rows.iter().all(|row| row.status == LinkageStatus::Resolved),
        "report Crypto linkage append did not persist the complete resolved cohort"
    );
    Ok(())
}

async fn seed_report_catalogs(
    db: &DatabaseConnection,
    capability_registry_hash: ContentHash,
    resolves_at: DateTime<Utc>,
) -> Result<Arc<[ShadowObservationCohort]>> {
    let database_now = db.statement_time().await;
    let decision_at = DateTime::from_timestamp_millis(database_now.timestamp_millis())
        .context("mixed-Route report catalog clock is outside millisecond range")?;
    let market_created_at = decision_at - Duration::days(1);
    ensure!(
        resolves_at > decision_at,
        "mixed-Route report resolution must remain after its decision boundary"
    );
    let resolutions = (1..=EVALUATION_MARKETS_PER_TICK)
        .map(|ordinal| (ordinal, resolves_at))
        .collect::<BTreeMap<_, _>>();
    // Route/model lineage is the treatment under test. The two report scopes
    // share one latent domain below, and the explicit price shift is identical,
    // so structural signal, spread, and executable-price nuisance are paired.
    let specifications = [
        ("report-crypto", MarketCategory::Crypto, Decimal::ZERO),
        ("report-weather", MarketCategory::Weather, Decimal::ZERO),
    ];
    let mut cohorts = Vec::with_capacity(specifications.len());
    for (scope, category, price_shift) in specifications {
        let event_id = format!("feedback-closure-{scope}-event");
        let catalog = Arc::new(ClosureCatalogFacts::build(ClosureCatalogBuild {
            scope,
            event_id: &event_id,
            category,
            decision_at,
            market_created_at,
            resolutions: &resolutions,
            first_ordinal: 1,
            last_ordinal: EVALUATION_MARKETS_PER_TICK,
            price_shift,
        })?);
        catalog.persist(db, capability_registry_hash).await?;
        if category == MarketCategory::Crypto {
            persist_crypto_linkages(db, catalog.as_ref(), capability_registry_hash).await?;
        }
        let markets = (1..=EVALUATION_MARKETS_PER_TICK)
            .map(|ordinal| ClosureMarketSource {
                source_id: RecommendationId::new(seeded_uuid(&format!(
                    "feedback-closure:{scope}:{ordinal}:report-source"
                ))),
                market_id: MarketId::new(format!("feedback-closure-{scope}-market-{ordinal}")),
            })
            .collect::<Vec<_>>();
        cohorts.push(ShadowObservationCohort {
            markets: Arc::from(markets),
            book_price_shift: price_shift,
            catalog,
        });
    }
    Ok(Arc::from(cohorts))
}

async fn seed_report(
    context: &CohortSeedContext<'_>,
    specification: CohortSpecification<'_>,
) -> Result<CohortSeed> {
    let prepared = Box::pin(PreparedCohort::prepare(context, specification)).await?;
    Box::pin(prepared.publish(context.db, context.artifacts, context.facts)).await
}

impl PreparedCohort {
    async fn prepare(
        context: &CohortSeedContext<'_>,
        specification: CohortSpecification<'_>,
    ) -> Result<Self> {
        let db = context.db;
        let infra = context.infra;
        let champion = context.champion;
        let schema = context.schema;
        let runtime = context.runtime;
        let fact_writers = context.facts;
        let replay = context.replay;
        let capability_registry_hash = context.capability_registry_hash;
        let scope = specification.scope;
        let decision_at = specification.decision_at;
        let first_ordinal = specification.first_ordinal;
        let book_price_shift = specification.book_price_shift;
        let last_ordinal = specification.last_ordinal()?;
        let config = specification.report_config();
        let catalog = Arc::new(ClosureCatalogFacts::build(ClosureCatalogBuild {
            scope,
            event_id: &config.event_id,
            category: MarketCategory::Weather,
            decision_at,
            market_created_at: specification.market_created_at,
            resolutions: specification.resolutions,
            first_ordinal,
            last_ordinal,
            price_shift: book_price_shift,
        })?);
        catalog.persist(db, capability_registry_hash).await?;
        let sources = specification.sources(last_ordinal);
        seed_cohort_sources(
            db,
            &sources,
            decision_at,
            book_price_shift,
            fact_writers,
            replay,
        )
        .await?;
        let expected_markets = sources
            .iter()
            .map(|source| source.market_id.clone())
            .collect::<HashSet<_>>();
        let selection = build_selection_model(SelectionModelBuild {
            db,
            facts: fact_writers,
            replay,
            infra,
            champion,
            runtime,
            decision_at,
            expected_markets: &expected_markets,
        })
        .await?;
        let mut ids =
            prepare_report_lineage_model(db, infra, &config, decision_at, selection).await;
        ids.recommendation = sources
            .first()
            .context("closure source set is unexpectedly empty")?
            .source_id;
        let mut options = ReportBuildOptions::published_single(&ids);
        options.runtime_mode = QuantRuntimeMode::AutoExecution;
        options.account_capital_usd = Some(context.account_capital_usd);
        let recommendations = build_cohort_recommendations(
            &sources,
            &ids,
            &options,
            &specification,
            last_ordinal,
            replay.knowledge_lag.as_secs(),
        )?;
        let resolution_by_market = specification.resolution_map(last_ordinal)?;
        let mut cohort = CohortSeed {
            catalog,
            decision_at,
            book_price_shift,
            resolution_by_market,
            ids,
            market_universe: Vec::new(),
            recommendations,
        };
        let inference = cohort
            .persist_evidence(
                db,
                champion,
                schema,
                runtime,
                replay,
                context.runtime_finalized_execution_evidence,
            )
            .await
            .with_context(|| {
                format!(
                    "prepare closure {scope} cohort {first_ordinal}..={last_ordinal} at {decision_at}"
                )
            })?;
        let prediction_hash = canonical_business_prediction_hash(&inference.candidates)?;
        cohort.bind_predictions(&inference.candidates)?;
        options.recommendations = cohort.recommendations.clone();
        options.align_closure_summary(cohort.market_universe.len())?;
        fact_writers.commit_serving(inference.facts).await?;
        PgModelRunRepository::new(db.clone())
            .succeed(&cohort.ids.model_run, prediction_hash, None)
            .await?;
        Ok(Self {
            cohort,
            options,
            trigger_key: config.trigger_key,
        })
    }
}

async fn seed_execution_attempts(
    db: &DatabaseConnection,
    cohorts: &[CohortSeed],
) -> Result<Vec<ExecutionAttemptOutcomeInfo>> {
    let exit_prices = [dec!(0.45), dec!(0.55), dec!(0.75)];
    let mut subjects = cohorts
        .iter()
        .rev()
        .filter_map(|cohort| {
            cohort
                .recommendations
                .first()
                .map(|recommendation| (cohort, recommendation))
        })
        .take(EXECUTION_ASSOCIATION_SAMPLE_COUNT)
        .collect::<Vec<_>>();
    subjects.reverse();
    let mut attempts = Vec::with_capacity(EXECUTION_ASSOCIATION_SAMPLE_COUNT);
    for (ordinal, ((cohort, recommendation), exit_price)) in
        subjects.into_iter().zip(exit_prices).enumerate()
    {
        let terminal_at =
            cohort.decision_at + Duration::days(1) + Duration::minutes(i64::try_from(ordinal)?);
        attempts.push(
            seed_execution_attempt(db, &cohort.ids, recommendation, exit_price, terminal_at)
                .await?,
        );
    }
    ensure!(
        attempts.len() == EXECUTION_ASSOCIATION_SAMPLE_COUNT,
        "closure fixture did not produce the required execution association samples"
    );
    let realized = attempts
        .iter()
        .filter_map(|attempt| attempt.realized_pnl_usd)
        .map(Usd::inner)
        .collect::<HashSet<_>>();
    ensure!(
        realized.len() == EXECUTION_ASSOCIATION_SAMPLE_COUNT,
        "closure execution samples must carry distinct realized PnL"
    );
    Ok(attempts)
}

async fn seed_execution_attempt(
    db: &DatabaseConnection,
    ids: &ExecutionTxnIds,
    recommendation: &NewRecommendation,
    exit_price: Decimal,
    terminal_at: DateTime<Utc>,
) -> Result<ExecutionAttemptOutcomeInfo> {
    let transaction = db.begin().await?;
    let recommendation_id = recommendation.recommendation_id;
    let condition = EntryConditionEntity::find()
        .filter(EntryConditionColumn::RecommendationId.eq(recommendation_id))
        .one(&transaction)
        .await?
        .with_context(|| {
            format!("closure recommendation {recommendation_id} has no entry condition")
        })?;
    let approved_at = terminal_at - Duration::minutes(10);
    let entry_at = terminal_at - Duration::minutes(8);
    let exit_at = terminal_at;
    let intent_id = OrderIntentId::from_v7();
    let market_id = MarketId::new(&recommendation.market_id);
    let token_id = TokenId::new(&recommendation.token_id);
    let exit_reason = if exit_price >= dec!(0.6) {
        ExitReason::TakeProfit
    } else {
        ExitReason::StopLoss
    };

    let mut intent = new_order_intent(
        intent_id,
        ids,
        OrderIntentStatus::ApprovedByPolicy,
        ApprovalStatus::NotRequired,
        QuantRuntimeMode::AutoExecution,
        None,
    );
    intent.recommendation_id = recommendation_id;
    intent.condition_instance_id = condition.condition_instance_id;
    intent.approved_at = Some(approved_at);
    intent.expires_at = terminal_at + Duration::hours(1);
    intent.entry_order_json.token_id = token_id.clone();
    intent.entry_order_json.valid_until = terminal_at + Duration::hours(1);
    let mut intent_active = intent.into_active_model();
    intent_active.status = Set(OrderIntentStatus::Filled);
    intent_active.exit_state = Set(ExitState::Exited);
    intent_active.exit_reason = Set(Some(exit_reason));
    intent_active.scale_out_state = Set(ScaleOutState::default());
    intent_active.created_at = Set(approved_at);
    intent_active.updated_at = Set(exit_at);
    let intent_model = OrderIntentEntity::insert(intent_active)
        .exec_with_returning(&transaction)
        .await?;

    let mut allocation = new_capital_allocation(intent_id, ids);
    allocation.recommendation_id = recommendation_id;
    allocation.state = CapitalAllocationState::Released;
    allocation.spent_usd = allocation.allocated_usd;
    allocation.released_usd = Usd::ZERO;
    "historical closure execution exited".clone_into(&mut allocation.reason);
    let mut allocation_active = allocation.into_active_model();
    allocation_active.created_at = Set(approved_at);
    allocation_active.updated_at = Set(exit_at);
    CapitalAllocationEntity::insert(allocation_active)
        .exec_without_returning(&transaction)
        .await?;

    let mut entry = new_execution_order(&intent_id, ids);
    entry.market_id = market_id.clone();
    entry.token_id = token_id.clone();
    entry.prepared_order_json.token_id = token_id.clone();
    entry.prepared_order_json.fee_schedule.effective_at = approved_at;
    entry.prepared_order_json.fee_schedule.available_at = approved_at;
    entry.prepared_order_json.prepared_at = approved_at;
    entry.prepared_order_json.valid_until = terminal_at;
    entry.venue_order_id = Some(OrderId::new(format!("closure-entry-{recommendation_id}")));
    entry.venue_status = Some(VenueOrderStatus::Filled);
    entry.state = ExecutionOrderState::Filled;
    entry.submitted_at = Some(entry_at);
    entry.filled_at = Some(entry_at);
    let mut entry_active = entry.into_active_model();
    entry_active.created_at = Set(approved_at);
    entry_active.updated_at = Set(entry_at);
    let entry_model = ExecutionOrderEntity::insert(entry_active)
        .exec_with_returning(&transaction)
        .await?;

    let entry_reconciliation =
        reconciliation_row(&entry_model.execution_order_id, &intent_id, entry_at);
    let mut entry_reconciliation_active = entry_reconciliation.into_active_model();
    entry_reconciliation_active.created_at = Set(entry_at);
    entry_reconciliation_active.updated_at = Set(entry_at);
    let entry_reconciliation_model = ReconciliationEntity::insert(entry_reconciliation_active)
        .exec_with_returning(&transaction)
        .await?;

    let mut exit = exit_order(&intent_id, ids, entry_model.shares.inner(), exit_price);
    exit.market_id = market_id.clone();
    exit.token_id = token_id.clone();
    exit.prepared_order_json.token_id = token_id.clone();
    exit.prepared_order_json.fee_schedule.effective_at = entry_at;
    exit.prepared_order_json.fee_schedule.available_at = entry_at;
    exit.prepared_order_json.prepared_at = entry_at;
    exit.prepared_order_json.valid_until = terminal_at + Duration::minutes(1);
    exit.venue_order_id = Some(OrderId::new(format!("closure-exit-{recommendation_id}")));
    exit.venue_status = Some(VenueOrderStatus::Filled);
    exit.state = ExecutionOrderState::Filled;
    exit.submitted_at = Some(exit_at);
    exit.filled_at = Some(exit_at);
    let mut exit_active = exit.into_active_model();
    exit_active.created_at = Set(entry_at + Duration::minutes(1));
    exit_active.updated_at = Set(exit_at);
    let exit_model = ExecutionOrderEntity::insert(exit_active)
        .exec_with_returning(&transaction)
        .await?;

    let exit_reconciliation = exit_reconciliation_row(
        &exit_model.execution_order_id,
        &intent_id,
        entry_model.shares,
        Price::new(exit_price),
        exit_at,
    );
    let mut exit_reconciliation_active = exit_reconciliation.into_active_model();
    exit_reconciliation_active.created_at = Set(exit_at);
    exit_reconciliation_active.updated_at = Set(exit_at);
    let exit_reconciliation_model = ReconciliationEntity::insert(exit_reconciliation_active)
        .exec_with_returning(&transaction)
        .await?;

    let position_model = ClosedPositionSeed {
        ids,
        recommendation,
        intent_id,
        market_id: &market_id,
        token_id: &token_id,
        entry_shares: entry_model.shares,
        entry_cost_usd: entry_model.cost_usd,
        exit_price,
        entry_at,
        exit_at,
    }
    .insert(&transaction)
    .await?;

    let graph = ExecutionAttemptSourceGraph {
        recommendation_id,
        market_id,
        token_id,
        intent: intent_model.into(),
        orders: vec![entry_model.into(), exit_model.into()],
        reconciliations: vec![
            entry_reconciliation_model.into(),
            exit_reconciliation_model.into(),
        ],
        position: Some(position_model.into()),
        settlement_lot: None,
    };
    let info = ClosureExecutionGraph(graph).seal(&transaction).await?;
    transaction.commit().await?;
    Ok(info)
}

async fn align_report_history(db: &DatabaseConnection, cohorts: &[CohortSeed]) -> Result<()> {
    for (index, cohort) in cohorts.iter().enumerate() {
        let transaction = db.begin().await?;
        let created_at = cohort.decision_at + Duration::seconds(1);
        let published_at = cohort.decision_at + Duration::seconds(3);
        let superseded_at = cohorts
            .get(index + 1)
            .map(|next| next.decision_at + Duration::seconds(3));
        let recommendation_status = if superseded_at.is_some() {
            RecommendationStatus::Superseded
        } else {
            RecommendationStatus::Expired
        };
        let recommendation_terminal_at =
            superseded_at.unwrap_or(cohort.decision_at + Duration::days(2));
        transaction
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                r"
                UPDATE quant_recommendation_report
                SET published_at = $2,
                    superseded_at = $3
                WHERE recommendation_report_id = $1
            ",
                [
                    cohort.ids.report.into(),
                    published_at.into(),
                    superseded_at.into(),
                ],
            ))
            .await?;
        transaction
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                r"
                UPDATE quant_recommendation
                SET status = $2::qp_recommendation_status,
                    status_changed_at = $3
                WHERE recommendation_report_id = $1
            ",
                [
                    cohort.ids.report.into(),
                    recommendation_status.into(),
                    recommendation_terminal_at.into(),
                ],
            ))
            .await?;
        transaction
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                r"
                UPDATE quant_report_fact_delivery
                SET status = 'verified'::qp_report_fact_delivery_status,
                    claim_owner = NULL,
                    lease_expires_at = NULL,
                    next_attempt_at = NULL,
                    last_error = NULL,
                    verified_at = $2,
                    announced_at = $3,
                    created_at = $4,
                    updated_at = $3
                WHERE recommendation_report_id = $1
            ",
                [
                    cohort.ids.report.into(),
                    published_at.into(),
                    (published_at + Duration::seconds(1)).into(),
                    created_at.into(),
                ],
            ))
            .await?;

        let report = RecommendationReportEntity::find_by_id(cohort.ids.report)
            .one(&transaction)
            .await?
            .context("closure historical report disappeared during clock alignment")?;
        let subject = FeatureParitySubjectEntity::find()
            .filter(FeatureParitySubjectColumn::RecommendationReportId.eq(cohort.ids.report))
            .one(&transaction)
            .await?
            .context("closure report has no atomically frozen parity subject")?;
        let generation = report_parity_generation_hash(
            &report.recommendation_report_id,
            report.decision_at,
            report.created_at,
        )?;
        let evidence_hash = report_parity_evidence_hash(
            &generation,
            &report.represented_routes_json,
            &report.scenario_artifact_hash,
            &report.decision_policy_snapshot_id,
            &report.market_selection_id,
            &report.data_quality_snapshot_ref,
            &report.portfolio_plan_id,
        )?;
        ensure!(
            subject.subject_generation == generation && subject.evidence_hash == evidence_hash,
            "closure report {} availability clock differs from its WORM parity commitment",
            cohort.ids.report
        );
        transaction.commit().await?;
    }
    Ok(())
}

impl CohortSeed {
    fn bind_predictions(&mut self, candidates: &[SignalCandidate]) -> Result<()> {
        ensure!(
            self.market_universe.is_empty(),
            "closure cohort predictions were already bound"
        );
        let by_market = candidates
            .iter()
            .map(|candidate| (candidate.market_id.clone(), candidate))
            .collect::<HashMap<_, _>>();
        ensure!(
            by_market.len() == candidates.len(),
            "closure runtime emitted duplicate market candidates"
        );
        let mut universe = mem::take(&mut self.recommendations);
        let universe_index = universe
            .iter()
            .enumerate()
            .map(|(index, recommendation)| (recommendation.market_id.clone(), index))
            .collect::<HashMap<_, _>>();
        ensure!(
            universe_index.len() == universe.len(),
            "closure decision universe contains duplicate markets"
        );
        let mut emitted = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let index = universe_index.get(&candidate.market_id).with_context(|| {
                format!(
                    "closure runtime emitted unknown decision-universe market {}",
                    candidate.market_id
                )
            })?;
            let recommendation = &mut universe[*index];
            ensure!(
                candidate.model_run_id == self.ids.model_run,
                "closure candidate {} belongs to model run {}, expected {}",
                candidate.signal_candidate_id,
                candidate.model_run_id,
                self.ids.model_run
            );
            ensure!(
                candidate.decision_at == self.decision_at,
                "closure candidate {} has decision time {}, expected {}",
                candidate.signal_candidate_id,
                candidate.decision_at,
                self.decision_at
            );
            recommendation.token_id = candidate.token_id.clone();
            recommendation.outcome_side = candidate.outcome_side;
            recommendation.economic_tier_json.candidate_id = candidate.signal_candidate_id;
            recommendation.economic_tier_json.token_id = candidate.token_id.clone();
            recommendation.economic_tier_json.outcome_side = candidate.outcome_side;
            recommendation.factor_breakdown = RecommendationFactorBreakdown(
                candidate
                    .factor_breakdown
                    .iter()
                    .map(|factor| FactorBreakdownEntry {
                        factor_name: factor.name.to_string(),
                        family: factor.family,
                        value_state: factor.value_state,
                        raw_value: factor.raw_value,
                        normalized_score: factor.normalized_score,
                        normalization_source: factor.normalization_source,
                        indeterminate_reason: factor.indeterminate_reason,
                        weight: factor.weight,
                        contribution: factor.contribution,
                        confidence: factor.confidence,
                        direction: factor.direction,
                        explanation: factor.explanation.clone(),
                        source_refs: factor.source_refs.clone(),
                    })
                    .collect(),
            );
            recommendation.evidence_refs.signal_candidate_id = candidate.signal_candidate_id;
            recommendation.evidence_refs.factor_definition_versions = candidate
                .factor_breakdown
                .iter()
                .map(|factor| factor.definition_id)
                .collect();
            emitted.push(recommendation.clone());
        }
        self.market_universe = universe;
        self.recommendations = emitted;
        Ok(())
    }

    async fn persist_evidence(
        &mut self,
        db: &DatabaseConnection,
        champion: &ModelVersionInfo,
        schema: &ExecutableFeatureSchema,
        runtime: &WeightedFactorRuntime,
        replay: &ClosureReplayContext,
        runtime_finalized_execution_evidence: &FinalizedExecutionEvidence,
    ) -> Result<CohortInferenceResult> {
        let created_at = self.decision_at + Duration::seconds(2);
        let event_time = created_at.timestamp_millis();
        let boundary = DecisionClock::new(replay.knowledge_lag.as_secs()).serving_boundary(
            self.decision_at,
            replay.config.domain.crypto.availability_lag_secs,
            replay.config.domain.weather.availability_lag_secs,
        )?;
        let required_features = runtime.required_features();
        let cross = self
            .replay_cross_section(
                replay,
                &boundary,
                &required_features,
                runtime_finalized_execution_evidence,
            )
            .await?;
        let plane = self
            .persist_factor_plane(db, &boundary, created_at, cross)
            .await?;
        self.finish_inference(
            CohortInferenceContext {
                db,
                champion,
                schema,
                runtime,
                boundary: &boundary,
                event_time,
            },
            plane,
        )
        .await
    }

    async fn replay_cross_section(
        &self,
        replay: &ClosureReplayContext,
        boundary: &DecisionBoundary,
        required_features: &[FeatureName],
        runtime_finalized_execution_evidence: &FinalizedExecutionEvidence,
    ) -> Result<ReplayCrossSection> {
        let observation_count = self.recommendations.len();
        let samples = self
            .recommendations
            .iter()
            .map(|recommendation| {
                let (scope, ordinal) = closure_market_identity(&recommendation.market_id)?;
                let source = ClosureMarketSource::from(recommendation);
                let primary_ref = closure_book_facts(
                    &source,
                    self.decision_at,
                    replay.knowledge_lag.as_secs(),
                    self.book_price_shift,
                )?
                .primary_ref;
                ensure!(
                    recommendation.evidence_refs.book_snapshot_ref == primary_ref,
                    "closure recommendation {} changed its canonical book binding",
                    recommendation.recommendation_id
                );
                Ok(ReplaySample {
                    market_id: recommendation.market_id.clone(),
                    token_id: TokenId::new(closure_token(scope, ordinal)),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let window_end = self
            .decision_at
            .checked_add_signed(Duration::milliseconds(1))
            .context("closure replay window end overflowed")?;
        let window = replay
            .loader
            .load(&WindowSpec {
                window_start: self.decision_at,
                window_end,
                available_by: window_end,
                samples: samples.clone(),
                lookback: replay.lookback,
                knowledge_lag: replay.knowledge_lag,
                feature_contract: replay.config.feature_contract,
                max_horizon_secs: 0,
                domain: replay.config.domain.clone(),
            })
            .await?;
        let finalized_execution_evidences =
            frozen_finalized_execution_evidences(&samples, runtime_finalized_execution_evidence)?;
        let cross = materialize_cross_section(
            &replay.builder,
            ReplayFactorMode::FactorNative {
                engine: &replay.factor_engine,
            },
            &replay.config,
            &CrossSectionRequest {
                // These rows are persisted as live-inference evidence, so the
                // source plane must use the same frozen runtime semantics as
                // the real feature pipeline even though the underlying source
                // slice was prepared before the binary starts.
                pit: &window.pit,
                prefetched: &window.prefetched,
                finalized_execution_evidence: ReplayExecutionSource::FrozenRuntime(
                    &finalized_execution_evidences,
                ),
                decision_at: self.decision_at,
                group: &samples,
                required_features,
                category_scope: None,
                knowledge_lag: replay.knowledge_lag,
            },
        )
        .await?
        .context("closure exact replay resolved no catalog cross-section")?;
        ensure!(
            &cross.boundary == boundary,
            "closure exact replay changed the serving decision boundary"
        );
        ensure!(
            cross.rejected_vectors.is_empty() && cross.vectors.len() == observation_count,
            "closure exact replay retained {} of {} markets and rejected {}; first_rejected={:?}",
            cross.vectors.len(),
            observation_count,
            cross.rejected_vectors.len(),
            cross.rejected_vectors.first()
        );
        Ok(cross)
    }

    async fn persist_factor_plane(
        &mut self,
        db: &DatabaseConnection,
        boundary: &DecisionBoundary,
        created_at: DateTime<Utc>,
        cross: ReplayCrossSection,
    ) -> Result<PersistedCohortPlane> {
        let ReplayCrossSection {
            boundary: replay_boundary,
            vectors,
            captures,
            markets,
            factor_output,
            ..
        } = cross;
        let outcomes = match factor_output {
            ReplayFactorOutput::FactorNative { outcomes } => outcomes,
            ReplayFactorOutput::FeatureOnly => {
                anyhow::bail!("weighted closure replay omitted its governed factor plane")
            }
        };
        ensure!(
            outcomes.len() == vectors.len() && markets.len() == vectors.len(),
            "closure replay feature/factor/selection cardinality drifted"
        );
        ensure!(
            &replay_boundary == boundary,
            "closure factor persistence changed the replay boundary"
        );
        let observation_count = self.recommendations.len();
        let recommendation_index = self
            .recommendations
            .iter()
            .enumerate()
            .map(|(index, recommendation)| (recommendation.market_id.clone(), index))
            .collect::<HashMap<_, _>>();
        ensure!(
            recommendation_index.len() == observation_count,
            "closure recommendations contain duplicate markets"
        );
        let mut ordered_vectors = Vec::with_capacity(observation_count);
        let mut ordered_captures = Vec::with_capacity(observation_count);
        let mut vector_ids = Vec::with_capacity(observation_count);
        let mut feature_models = Vec::with_capacity(observation_count);
        let mut factor_models = Vec::new();
        let mut inference_rows = Vec::with_capacity(observation_count);
        for ((vector, selected), outcome) in vectors.into_iter().zip(markets).zip(outcomes) {
            ensure!(
                outcome.market_id == vector.market_id
                    && outcome.decision_at == self.decision_at
                    && matches!(outcome.eligibility, FactorEligibility::Eligible),
                "closure factor outcome for {} is not inference-eligible: {:?}",
                vector.market_id,
                outcome.eligibility
            );
            let index = recommendation_index
                .get(&vector.market_id)
                .copied()
                .with_context(|| {
                    format!("closure replay emitted unknown market {}", vector.market_id)
                })?;
            let capture = captures
                .get(&ReplayCaptureKey::new(
                    &vector.market_id,
                    &selected.primary_token_id,
                ))
                .with_context(|| {
                    format!("closure replay omitted capture for {}", vector.market_id)
                })?;
            let capture_evidence = capture.evidence();
            let recommendation = &mut self.recommendations[index];
            recommendation.identity = capture.identity.clone();
            recommendation.market_context = capture.market_context.clone();
            recommendation.evidence_refs.book_snapshot_ref = capture.book_snapshot_ref.clone();
            let vector_id = recommendation.evidence_refs.feature_vector_id;
            let mut new_feature = vector.try_to_new(&replay_boundary, &capture_evidence)?;
            new_feature.feature_vector_id = vector_id;
            let mut feature_active = new_feature.into_active_model();
            feature_active.created_at = Set(created_at);
            feature_models.push(feature_active);
            let values = outcome
                .factors
                .into_iter()
                .map(|factor| factor.value)
                .collect::<Vec<FactorValue>>();
            let factor_context = FactorValueInsertContext {
                model_run_id: &self.ids.model_run,
                feature_vector_id: &vector_id,
                market_id: &recommendation.market_id,
                decision_at: self.decision_at,
            };
            for value in &values {
                let mut active = value.try_to_new(&factor_context)?.into_active_model();
                active.created_at = Set(created_at);
                factor_models.push(active);
            }
            let inference_context = build_market_inference_context(&vector, &selected)
                .with_context(|| {
                    format!(
                        "closure market {} has no executable inference context",
                        recommendation.market_id
                    )
                })?;
            inference_rows.push(FactorInferenceRow {
                market_id: recommendation.market_id.clone(),
                token_id: selected.primary_token_id,
                factors: values,
                context: inference_context,
            });
            vector_ids.push(vector_id);
            ordered_vectors.push(vector);
            ordered_captures.push(capture_evidence);
        }
        FeatureVectorEntity::insert_many(feature_models)
            .exec(db)
            .await?;
        for batch in factor_models.chunks(FACTOR_VALUE_INSERT_BATCH_ROWS) {
            FactorValueEntity::insert_many(batch.iter().cloned())
                .exec(db)
                .await?;
        }
        Ok(PersistedCohortPlane {
            vectors: ordered_vectors,
            captures: ordered_captures,
            vector_ids,
            inference_rows,
        })
    }

    async fn finish_inference(
        &self,
        context: CohortInferenceContext<'_>,
        plane: PersistedCohortPlane,
    ) -> Result<CohortInferenceResult> {
        let CohortInferenceContext {
            db,
            champion,
            schema,
            runtime,
            boundary,
            event_time,
        } = context;
        let PersistedCohortPlane {
            vectors,
            captures,
            vector_ids,
            inference_rows,
        } = plane;
        let persisted = PgFeatureRepository::new(db.clone())
            .find_by_ids(&vector_ids)
            .await?;
        ensure!(
            persisted.len() == vector_ids.len(),
            "closure cohort persisted {} of {} feature vectors",
            persisted.len(),
            vector_ids.len()
        );
        let persisted_by_id = persisted
            .into_iter()
            .map(|info| (info.feature_vector_id, info))
            .collect::<HashMap<_, _>>();
        let mut feature_rows = Vec::new();
        for ((vector, capture), vector_id) in vectors.iter().zip(&captures).zip(&vector_ids) {
            let info = persisted_by_id.get(vector_id).with_context(|| {
                format!("closure feature vector {vector_id} disappeared after insert")
            })?;
            feature_rows.extend(feature_events(
                vector,
                info,
                &capture.snapshot.boundary,
                &self.ids.decision_policy_snapshot,
                schema,
                event_time,
            )?);
        }
        let evidence = feature_commitment(&feature_rows)?;
        let bindings = champion.serving_contract.bindings();
        let inference_table = FactorInferenceTable {
            model_run_id: self.ids.model_run,
            decision_at: self.decision_at,
            rows: inference_rows,
        };
        let expected_input_audit =
            inference_table.weighted_input_audit(WeightedInputAuditContract {
                model_version_id: champion.model_version_id,
                input_contract_hash: bindings.transform.input_contract_hash,
                transform_hash: bindings.transform.input_transform_hash,
                training_input_hash: bindings.transform.training_input_hash,
            })?;
        let inference_market_count = inference_table.rows.len();
        let mut output = runtime
            .infer_batch(ModelRuntimeInput::FactorTable(inference_table))
            .await?;
        ensure!(
            output.calibration_scores.len() == inference_market_count
                && output.rank_scores.len() == inference_market_count,
            "closure runtime censored the pre-decision score population: calibration={} rank={} expected={inference_market_count}",
            output.calibration_scores.len(),
            output.rank_scores.len()
        );
        finalize_candidates(&mut output.candidates)?;
        ensure!(
            output.input_audit == expected_input_audit,
            "closure runtime input audit differs from durable evidence projection"
        );
        let input_rows = ModelInputEvidenceBatch::try_new(&vectors, &vector_ids)?.project(
            &self.ids.model_run,
            boundary,
            &output.input_audit,
            event_time,
        )?;
        let completion = completion_marker(
            &self.ids.model_run,
            boundary,
            &evidence,
            &input_rows,
            event_time,
        )?;
        let verified_ids = verify_completion(&completion, &feature_rows, &input_rows)?;
        ensure!(
            verified_ids.into_iter().collect::<HashSet<_>>()
                == vector_ids.iter().copied().collect::<HashSet<_>>(),
            "closure serving completion changed the persisted feature-vector membership"
        );
        Ok(CohortInferenceResult {
            facts: CohortServingFacts {
                feature_rows,
                input_rows,
                completion,
            },
            candidates: output.candidates,
        })
    }

    async fn persist_rows(
        &self,
        db: &DatabaseConnection,
        resolution_facts: &BTreeMap<MarketId, MarketResolutionRow>,
    ) -> Result<()> {
        let outcome_rows = self
            .recommendations
            .iter()
            .map(|recommendation| {
                let expected_resolved_at = self
                    .resolution_by_market
                    .get(&recommendation.market_id)
                    .copied()
                    .with_context(|| {
                        format!(
                            "closure recommendation {} has no market resolution",
                            recommendation.recommendation_id
                        )
                    })?;
                let fact = resolution_facts
                    .get(&recommendation.market_id)
                    .with_context(|| {
                        format!(
                            "closure recommendation {} has no canonical market-resolution fact",
                            recommendation.recommendation_id
                        )
                    })?;
                ensure!(
                    fact.resolved_at == expected_resolved_at.timestamp_millis(),
                    "closure recommendation {} resolution time disagrees with canonical fact",
                    recommendation.recommendation_id
                );
                resolution_outcome(recommendation, self.decision_at, fact)
            })
            .collect::<Result<Vec<_>>>()?;
        ResolutionOutcomeEntity::insert_many(outcome_rows)
            .exec(db)
            .await?;
        Ok(())
    }
}

async fn seal_execution_rollups(
    db: &DatabaseConnection,
    cohorts: &[CohortSeed],
    attempts: Vec<ExecutionAttemptOutcomeInfo>,
) -> Result<()> {
    let mut attempts = attempts
        .into_iter()
        .map(|attempt| (attempt.recommendation_id, attempt))
        .collect::<HashMap<_, _>>();
    ensure!(
        attempts.len() == EXECUTION_ASSOCIATION_SAMPLE_COUNT,
        "closure execution attempt identities are not unique"
    );
    let expected_rollup_count = cohorts
        .iter()
        .map(|cohort| cohort.recommendations.len())
        .sum::<usize>();
    let mut rollup_rows = Vec::with_capacity(expected_rollup_count);
    let mut binding_rows = Vec::with_capacity(EXECUTION_ASSOCIATION_SAMPLE_COUNT);
    for (index, cohort) in cohorts.iter().enumerate() {
        let terminal_at = cohorts
            .get(index + 1)
            .map_or(cohort.decision_at + Duration::days(2), |next| {
                next.decision_at + Duration::seconds(3)
            });
        for recommendation in &cohort.recommendations {
            let attempt = attempts.remove(&recommendation.recommendation_id);
            let source_observed_at = attempt
                .as_ref()
                .map_or(terminal_at, |attempt| terminal_at.max(attempt.available_at));
            let seal = NewRecommendationExecutionRollup::aggregate(
                recommendation.recommendation_id,
                usize::from(attempt.is_some()),
                source_observed_at,
                source_observed_at,
                attempt.into_iter().collect(),
            )?;
            let available_at = source_observed_at + Duration::minutes(1);
            let rollup_hash = seal.rollup.expected_rollup_hash(available_at)?;
            rollup_rows.push(execution_rollup_active(
                &seal.rollup,
                rollup_hash,
                available_at,
            ));
            binding_rows.extend(
                seal.bindings
                    .iter()
                    .map(|binding| execution_binding_active(binding, available_at)),
            );
        }
    }
    ensure!(
        attempts.is_empty(),
        "closure execution attempts are outside the frozen cohort"
    );
    ensure!(
        rollup_rows.len() == expected_rollup_count,
        "closure execution rollup set is incomplete"
    );
    ensure!(
        binding_rows.len() == EXECUTION_ASSOCIATION_SAMPLE_COUNT,
        "closure terminal execution binding set is incomplete"
    );
    // The clean fixture can contain thousands of terminal rollups. Bound each
    // statement below PostgreSQL's 65,535-parameter protocol ceiling.
    for batch in rollup_rows.chunks(EXECUTION_ROLLUP_INSERT_BATCH_ROWS) {
        ExecutionRollupEntity::insert_many(batch.iter().cloned())
            .exec(db)
            .await?;
    }
    ExecutionRollupAttemptEntity::insert_many(binding_rows)
        .exec(db)
        .await?;
    Ok(())
}

const fn execution_rollup_active(
    rollup: &NewRecommendationExecutionRollup,
    rollup_hash: ContentHash,
    available_at: DateTime<Utc>,
) -> ExecutionRollupActiveModel {
    ExecutionRollupActiveModel {
        recommendation_id: Set(rollup.recommendation_id),
        intent_count: Set(rollup.intent_count),
        attempt_count: Set(rollup.attempt_count),
        unfilled_attempt_count: Set(rollup.unfilled_attempt_count),
        partially_filled_attempt_count: Set(rollup.partially_filled_attempt_count),
        fully_filled_attempt_count: Set(rollup.fully_filled_attempt_count),
        total_requested_shares: Set(rollup.total_requested_shares),
        total_filled_shares: Set(rollup.total_filled_shares),
        total_entry_fee_usd: Set(rollup.total_entry_fee_usd),
        total_exit_fee_usd: Set(rollup.total_exit_fee_usd),
        total_settlement_payout_usd: Set(rollup.total_settlement_payout_usd),
        total_realized_pnl_usd: Set(rollup.total_realized_pnl_usd),
        first_attempt_terminal_at: Set(rollup.first_attempt_terminal_at),
        last_attempt_terminal_at: Set(rollup.last_attempt_terminal_at),
        terminal_at: Set(rollup.terminal_at),
        source_observed_at: Set(rollup.source_observed_at),
        available_at: Set(available_at),
        attempt_set_hash: Set(rollup.attempt_set_hash),
        rollup_hash: Set(rollup_hash),
        created_at: Set(available_at),
    }
}

const fn execution_binding_active(
    binding: &NewRecommendationExecutionRollupAttempt,
    created_at: DateTime<Utc>,
) -> ExecutionRollupAttemptActiveModel {
    ExecutionRollupAttemptActiveModel {
        recommendation_id: Set(binding.recommendation_id),
        sequence: Set(binding.sequence),
        order_intent_id: Set(binding.order_intent_id),
        attempt_outcome_hash: Set(binding.attempt_outcome_hash),
        terminal_at: Set(binding.terminal_at),
        created_at: Set(created_at),
    }
}

struct ClosureBookFacts {
    primary_ref: BookSnapshotRef,
    ledger_rows: [BookL2LedgerRow; 2],
    session_rows: [BookStreamSessionRow; 2],
    market_info: ClobMarketInfoVersion,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ClosureBookMetrics {
    best_bid: Price,
    best_ask: Price,
    mid_price: Price,
    spread_bps: Bps,
    depth_imbalance: Decimal,
    visible_liquidity_usd: Usd,
}

impl ClosureBookMetrics {
    fn from_levels(bids: &[BookLevel], asks: &[BookLevel]) -> Result<Self> {
        let best_bid = bids
            .first()
            .map(|level| level.price_decimal())
            .context("closure book has no bid")?;
        let best_ask = asks
            .first()
            .map(|level| level.price_decimal())
            .context("closure book has no ask")?;
        ensure!(best_bid < best_ask, "closure book is crossed");
        let mid_price = Price::new((best_bid.inner() + best_ask.inner()) / Decimal::from(2));
        let spread_bps = Bps::relative(best_ask.inner() - best_bid.inner(), mid_price.inner())
            .context("closure book spread is undefined")?;
        let bid_shares = bids
            .iter()
            .map(|level| level.size_decimal().inner())
            .sum::<Decimal>();
        let ask_shares = asks
            .iter()
            .map(|level| level.size_decimal().inner())
            .sum::<Decimal>();
        let total_shares = bid_shares + ask_shares;
        ensure!(
            !total_shares.is_zero(),
            "closure book has no visible shares"
        );
        let depth_imbalance = (bid_shares - ask_shares) / total_shares;
        let visible_liquidity_usd = bids.iter().chain(asks).fold(Usd::ZERO, |total, level| {
            total + level.size_decimal() * level.price_decimal()
        });
        Ok(Self {
            best_bid,
            best_ask,
            mid_price,
            spread_bps,
            depth_imbalance,
            visible_liquidity_usd,
        })
    }
}

#[derive(serde::Serialize)]
struct ClosureBookLevels<'a> {
    bids: &'a [BookLevel],
    asks: &'a [BookLevel],
}

const fn training_book_price_shift(group_index: usize) -> Decimal {
    // One stationary zero-mean nuisance cycle. The old expanding staircase
    // made CSCV block identity a proxy for executable price regime.
    const SHIFTS: [Decimal; 8] = [
        dec!(0),
        dec!(0.01),
        dec!(-0.01),
        dec!(0.02),
        dec!(-0.02),
        dec!(0.01),
        dec!(-0.01),
        dec!(0),
    ];
    SHIFTS[group_index % SHIFTS.len()]
}

const fn evaluation_book_price_shift(index: usize) -> Decimal {
    const SHIFTS: [Decimal; 9] = [
        dec!(0),
        dec!(0.01),
        dec!(-0.01),
        dec!(0.02),
        dec!(-0.02),
        dec!(0.03),
        dec!(-0.03),
        dec!(0.04),
        dec!(-0.04),
    ];
    // The exact market universe is intentionally held for two consecutive
    // decision ticks. Hold its executable book state as well so portfolio
    // turnover measures the governed universe transition, rather than a
    // synthetic within-universe rank flip caused only by fixture movement.
    SHIFTS[index.div_euclid(2) % SHIFTS.len()]
}

fn closure_levels(
    scope: &str,
    primary: bool,
    price_shift: Decimal,
    market_ordinal: usize,
) -> Result<([BookLevel; 1], [BookLevel; 1])> {
    let market_offset = closure_market_offset(scope, market_ordinal)?;
    let spread = closure_spread_width(scope, market_ordinal)?;
    let half_spread = spread / Decimal::from(2);
    let midpoint = if primary {
        dec!(0.42) + price_shift + market_offset
    } else {
        dec!(0.58) - price_shift - market_offset
    };
    let bid = midpoint - half_spread;
    let ask = midpoint + half_spread;
    ensure!(
        Decimal::ZERO < bid && bid < ask && ask <= Decimal::ONE,
        "closure book levels must be ordered within (0, 1], got bid={bid}, ask={ask}"
    );
    // Depth carries a balanced nuisance signal that is independent from the
    // latent reversion regime, price stream, and terminal label-noise stream.
    // It prevents the closure cohort from being a one-feature toy population.
    let (bid_size, ask_size) = if market_ordinal.is_multiple_of(2) {
        (Shares::new(dec!(2_400)), Shares::new(dec!(21_600)))
    } else {
        (Shares::new(dec!(18_000)), Shares::new(dec!(2_000)))
    };
    Ok((
        [BookLevel::from_decimal(Price::new(bid), bid_size)
            .map_err(|_| AnyhowError::msg("closure bid level is not representable"))?],
        [BookLevel::from_decimal(Price::new(ask), ask_size)
            .map_err(|_| AnyhowError::msg("closure ask level is not representable"))?],
    ))
}

fn closure_microstructure_row(
    token_id: TokenId,
    market_id: MarketId,
    at: DateTime<Utc>,
    bid: Decimal,
    ask: Decimal,
) -> Result<BookMicrostructureRow> {
    ensure!(
        Decimal::ZERO < bid && bid < ask && ask <= Decimal::ONE,
        "closure microstructure must be ordered within (0, 1], got bid={bid}, ask={ask}"
    );
    let mid = (bid + ask) / Decimal::from(2);
    let spread_bps =
        Bps::relative(ask - bid, mid).context("closure microstructure spread is undefined")?;
    let phase = u64::try_from(at.timestamp().rem_euclid(11))?;
    let depth_step = Decimal::from(phase) * Decimal::from(50);
    let top1_depth = dec!(5_000) + depth_step;
    Ok(BookMicrostructureRow {
        token_id,
        market_id: Some(market_id),
        bucket_time: at.timestamp_millis(),
        best_bid_open: Some(ChPrice::from(Price::new(bid))),
        best_bid_high: Some(ChPrice::from(Price::new(bid))),
        best_bid_low: Some(ChPrice::from(Price::new(bid))),
        best_bid_close: Some(ChPrice::from(Price::new(bid))),
        best_ask_open: Some(ChPrice::from(Price::new(ask))),
        best_ask_high: Some(ChPrice::from(Price::new(ask))),
        best_ask_low: Some(ChPrice::from(Price::new(ask))),
        best_ask_close: Some(ChPrice::from(Price::new(ask))),
        spread_bps_min: Some(ChBps::from(spread_bps)),
        spread_bps_avg: Some(ChBps::from(spread_bps)),
        spread_bps_max: Some(ChBps::from(spread_bps)),
        mid_price_open: Some(ChPrice::from(Price::new(mid))),
        mid_price_close: Some(ChPrice::from(Price::new(mid))),
        top1_depth_usd_avg: Some(ChUsd::from(top1_depth)),
        top5_depth_usd_avg: Some(ChUsd::from(top1_depth * Decimal::from(2))),
        top20_depth_usd_avg: Some(ChUsd::from(top1_depth * Decimal::from(4))),
        imbalance_avg: Some(ChDecimal64::from(dec!(0.10))),
        update_count: 12 + phase,
        snapshot_count: 1,
        delta_count: 6 + phase / 2,
        delete_count: 1,
        crossed_count: 0,
        invalid_level_count: 0,
        gap_count: 0,
        last_trade_count: 0,
        max_book_age_ms: 0,
        schema_version: ChSchemaVersion::FIRST,
        available_at: (at + Duration::seconds(1)).timestamp_millis(),
    })
}

fn closure_microstructure_rows(
    source: &ClosureMarketSource,
    decision_at: DateTime<Utc>,
    knowledge_lag_secs: u64,
    canonical_yes_wins: bool,
    price_shift: Decimal,
) -> Result<Vec<BookMicrostructureRow>> {
    let mut rows =
        closure_serving_microstructure_rows(source, decision_at, knowledge_lag_secs, price_shift)?;
    let (scope, ordinal) = closure_market_identity(&source.market_id)?;
    let yes_token = TokenId::new(closure_token(scope, ordinal));
    let no_token = closure_no_token(scope, ordinal);
    let (yes_bids, _) = closure_levels(scope, true, price_shift, ordinal)?;
    let yes_entry_bid = yes_bids[0].price_decimal().inner();
    let (no_bids, _) = closure_levels(scope, false, price_shift, ordinal)?;
    let no_entry_bid = no_bids[0].price_decimal().inner();
    let midpoint_at = decision_at + Duration::hours(12);
    let horizon_at = decision_at + Duration::hours(24);
    let (yes_mid_bid, yes_horizon_bid, no_mid_bid, no_horizon_bid) = if canonical_yes_wins {
        (
            yes_entry_bid - dec!(0.03),
            dec!(0.72),
            no_entry_bid - dec!(0.05),
            dec!(0.20),
        )
    } else {
        (
            yes_entry_bid - dec!(0.04),
            dec!(0.18),
            no_entry_bid - dec!(0.03),
            dec!(0.75),
        )
    };
    rows.extend([
        closure_microstructure_row(
            yes_token.clone(),
            source.market_id.clone(),
            midpoint_at,
            yes_mid_bid,
            yes_mid_bid + dec!(0.02),
        )?,
        closure_microstructure_row(
            yes_token,
            source.market_id.clone(),
            horizon_at,
            yes_horizon_bid,
            yes_horizon_bid + dec!(0.02),
        )?,
        closure_microstructure_row(
            no_token.clone(),
            source.market_id.clone(),
            midpoint_at,
            no_mid_bid,
            no_mid_bid + dec!(0.02),
        )?,
        closure_microstructure_row(
            no_token,
            source.market_id.clone(),
            horizon_at,
            no_horizon_bid,
            no_horizon_bid + dec!(0.02),
        )?,
    ]);
    Ok(rows)
}

fn closure_serving_microstructure_rows(
    source: &ClosureMarketSource,
    decision_at: DateTime<Utc>,
    knowledge_lag_secs: u64,
    price_shift: Decimal,
) -> Result<Vec<BookMicrostructureRow>> {
    let (scope, ordinal) = closure_market_identity(&source.market_id)?;
    let yes_token = TokenId::new(closure_token(scope, ordinal));
    let (yes_bids, yes_asks) = closure_levels(scope, true, price_shift, ordinal)?;
    let yes_entry_bid = yes_bids[0].price_decimal().inner();
    let yes_entry_ask = yes_asks[0].price_decimal().inner();
    let cutoff = DecisionClock::new(knowledge_lag_secs)
        .boundary(decision_at)?
        .cutoff_for(DecisionSource::Microstructure);
    (0_i64..=60)
        .rev()
        .map(|minutes_ago| {
            let variation = closure_momentum_variation(scope, ordinal, minutes_ago)?;
            closure_microstructure_row(
                yes_token.clone(),
                source.market_id.clone(),
                closure_bucket_at(cutoff, minutes_ago),
                yes_entry_bid + variation,
                yes_entry_ask + variation,
            )
        })
        .collect()
}

/// Place the newest synthetic 1s bucket immediately before the frozen cutoff.
///
/// The production source slice is half-open (`bucket_time < cutoff`). Historical
/// minute anchors retain the full one-hour span, while the newest closed 1s
/// bucket provides the same freshness shape as the live materializer.
fn closure_bucket_at(cutoff: DateTime<Utc>, minutes_ago: i64) -> DateTime<Utc> {
    if minutes_ago == 0 {
        cutoff - Duration::seconds(1)
    } else {
        cutoff - Duration::minutes(minutes_ago)
    }
}

fn closure_execution_history_rows(
    source: &ClosureMarketSource,
    decision_at: DateTime<Utc>,
    knowledge_lag_secs: u64,
    price_shift: Decimal,
) -> Result<ClosureExecutionFacts> {
    const PARTICIPANTS_PER_ROLE: usize = 20;
    let (scope, ordinal) = closure_market_identity(&source.market_id)?;
    let token_id = TokenId::new(closure_token(scope, ordinal));
    let (bids, asks) = closure_levels(scope, true, price_shift, ordinal)?;
    let price = Price::new(
        (bids[0].price_decimal().inner() + asks[0].price_decimal().inner()) / Decimal::from(2),
    );
    let cutoff = DecisionClock::new(knowledge_lag_secs)
        .boundary(decision_at)?
        .cutoff_for(DecisionSource::FinalizedExecution);
    let chunk_id = seeded_uuid(&format!(
        "feedback-closure:history:{}:{}",
        source.market_id,
        decision_at.timestamp_millis()
    ));
    let policy_hash = ResearchHasher::canonical(&(
        "feedback-closure-availability-policy",
        source.market_id.as_str(),
        decision_at,
    ))?;
    let block_hash = format!("0x{}", policy_hash.hex());
    let block_number = u64::try_from(decision_at.timestamp())?;
    let mut executions = Vec::with_capacity(PARTICIPANTS_PER_ROLE);
    let mut participants = Vec::with_capacity(PARTICIPANTS_PER_ROLE * 2);
    for index in 0..PARTICIPANTS_PER_ROLE {
        let offset_secs = i64::try_from(PARTICIPANTS_PER_ROLE - index)?;
        let event_at = cutoff - Duration::seconds(offset_secs);
        let observed_at = event_at + Duration::seconds(1);
        let maker_seed = ordinal
            .checked_mul(100)
            .and_then(|value| value.checked_add(index))
            .context("closure trade participant identity overflowed")?;
        let taker_seed = maker_seed
            .checked_add(PARTICIPANTS_PER_ROLE)
            .context("closure taker identity overflowed")?;
        let shares = Shares::new(Decimal::from(24 + (index % 5)));
        let notional = shares * price;
        let source_event_id = format!(
            "feedback-closure:{scope}:{ordinal}:{}:{index}:on-chain-fill",
            decision_at.timestamp_millis()
        );
        let tx_seed = seeded_uuid(&source_event_id).as_u128();
        let execution_hash = ResearchHasher::canonical(&source_event_id)?;
        let side = if index.is_multiple_of(2) {
            ChExchangeSide::Buy
        } else {
            ChExchangeSide::Sell
        };
        executions.push(MarketExecutionRow {
            execution_id: ChDigest::from(execution_hash),
            match_id: None,
            maker_order_filled_event_id: ChDigest::from(execution_hash),
            market_id: source.market_id.clone(),
            token_id: token_id.clone(),
            contract_key: "ctf_exchange_v2".to_owned(),
            exchange_version: ChExchangeVersion::V2,
            transaction_hash: format!("0x{tx_seed:064x}"),
            block_number,
            transaction_index: 0,
            log_index: u64::try_from(index)?,
            maker_address: format!("0x{maker_seed:040x}"),
            taker_address: format!("0x{taker_seed:040x}"),
            side,
            price: ChPrice::from(price),
            size_shares: ChShares::from(shares),
            notional_usd: ChUsd::from(notional),
            fee_amount: ChAssetAmount::from(Decimal::ZERO),
            fee_asset_id: "0".to_owned(),
            effective_at: event_at.timestamp_millis(),
            observed_at: observed_at.timestamp_millis(),
            model_available_at: observed_at.timestamp_millis(),
            availability_basis: ChAvailabilityBasis::BlockConfirmation,
            availability_policy_hash: ChDigest::from(policy_hash),
            chunk_id,
            schema_version: MarketExecutionRow::SCHEMA_VERSION,
        });
        for (participant_role, participant_address) in [
            (
                ChExecutionParticipantRole::Maker,
                format!("0x{maker_seed:040x}"),
            ),
            (
                ChExecutionParticipantRole::Taker,
                format!("0x{taker_seed:040x}"),
            ),
        ] {
            participants.push(ExecutionParticipantRow {
                execution_id: ChDigest::from(execution_hash),
                market_id: source.market_id.clone(),
                token_id: token_id.clone(),
                participant_address,
                participant_role,
                participant_notional: ChUsd::from(notional),
                effective_at: event_at.timestamp_millis(),
                model_available_at: observed_at.timestamp_millis(),
                availability_policy_hash: ChDigest::from(policy_hash),
                chunk_id,
                schema_version: ExecutionParticipantRow::SCHEMA_VERSION,
            });
        }
    }
    let acceptance = ExchangeHistoryAcceptanceRow {
        chunk_id,
        frontier: "activation".to_owned(),
        from_block: 1,
        to_block: block_number,
        log_count: u64::try_from(executions.len())?,
        provider_digest: ChDigest::from(policy_hash),
        first_block_hash: block_hash.clone(),
        last_block_hash: block_hash,
        effective_through_at: cutoff.timestamp_millis(),
        accepted_at: decision_at.timestamp_millis(),
        active: 1,
        state_revision: u64::try_from(decision_at.timestamp_micros())?,
        schema_version: ExchangeHistoryAcceptanceRow::SCHEMA_VERSION,
    };
    Ok(ClosureExecutionFacts {
        executions,
        participants,
        acceptance,
    })
}

fn closure_book_row(
    source: &ClosureMarketSource,
    token_id: &TokenId,
    decision_at: DateTime<Utc>,
    knowledge_lag_secs: u64,
    primary: bool,
    price_shift: Decimal,
) -> Result<(BookL2LedgerRow, [BookLevel; 1], [BookLevel; 1])> {
    let boundary = DecisionClock::new(knowledge_lag_secs).boundary(decision_at)?;
    let effective_at = boundary.cutoff_for(DecisionSource::Book);
    let (scope, market_ordinal) = closure_market_identity(&source.market_id)?;
    let (bids, asks) = closure_levels(scope, primary, price_shift, market_ordinal)?;
    let stream_session_id = seeded_uuid(&format!(
        "feedback-closure:{}:{}:{}:book-session",
        source.source_id, token_id, decision_at
    ));
    let row = BookL2LedgerRow {
        stream_session_id,
        shard_id: 0,
        token_id: token_id.clone(),
        market_id: Some(source.market_id.clone()),
        token_sequence: 1,
        event_type: ChCanonicalBookEventType::Snapshot,
        bid_prices: bids
            .iter()
            .map(|level| ChPrice::from(level.price_decimal()))
            .collect(),
        bid_sizes: bids
            .iter()
            .map(|level| ChShares::from(level.size_decimal()))
            .collect(),
        ask_prices: asks
            .iter()
            .map(|level| ChPrice::from(level.price_decimal()))
            .collect(),
        ask_sizes: asks
            .iter()
            .map(|level| ChShares::from(level.size_decimal()))
            .collect(),
        old_tick_size: None,
        new_tick_size: None,
        trade_price: None,
        trade_side: None,
        trade_size: None,
        fee_rate_bps: None,
        venue_event_time: effective_at.timestamp_millis(),
        ingress_time: effective_at.timestamp_millis(),
        persisted_time: effective_at.timestamp_millis(),
        event_hash: ChDigest::new([0; 32]),
        schema_version: BookL2LedgerRow::SCHEMA_VERSION,
    }
    .seal()?;
    Ok((row, bids, asks))
}

fn closure_session(row: &BookL2LedgerRow) -> Result<BookStreamSessionRow> {
    let sequence_json = serde_json::to_string(&BTreeMap::from([(
        row.token_id.as_str(),
        row.token_sequence,
    )]))?;
    Ok(BookStreamSessionRow {
        stream_session_id: row.stream_session_id,
        shard_id: row.shard_id,
        ledger_sequence: 1,
        state: ChStreamSessionState::Open,
        end_reason: ChStreamSessionEndReason::None,
        subscription_token_hash: CanonicalDigest::content_hash_json(&row.token_id)?,
        subscription_token_count: 1,
        received_sequence_json: sequence_json.clone(),
        persisted_sequence_json: sequence_json,
        opened_at: row.venue_event_time,
        recorded_at: row.persisted_time,
        schema_version: ChSchemaVersion(2),
    })
}

fn closure_book_facts(
    source: &ClosureMarketSource,
    decision_at: DateTime<Utc>,
    knowledge_lag_secs: u64,
    price_shift: Decimal,
) -> Result<ClosureBookFacts> {
    let (scope, ordinal) = closure_market_identity(&source.market_id)?;
    let primary_token_id = TokenId::new(closure_token(scope, ordinal));
    let secondary_token_id = closure_no_token(scope, ordinal);
    let (primary_row, primary_bids, primary_asks) = closure_book_row(
        source,
        &primary_token_id,
        decision_at,
        knowledge_lag_secs,
        true,
        price_shift,
    )?;
    let (secondary_row, _, _) = closure_book_row(
        source,
        &secondary_token_id,
        decision_at,
        knowledge_lag_secs,
        false,
        price_shift,
    )?;
    let primary_ref = BookSnapshotRef {
        token_id: primary_token_id.clone(),
        source: BookSnapshotSource::CanonicalL2 {
            stream_session_id: primary_row.stream_session_id,
            token_sequence: primary_row.token_sequence,
            source_event_hash: ContentHash::from(primary_row.event_hash),
            event_time_ms: primary_row.venue_event_time,
            ingestion_time_ms: primary_row.ingress_time,
        },
        content_hash: CanonicalDigest::content_hash_json(&ClosureBookLevels {
            bids: &primary_bids,
            asks: &primary_asks,
        })?,
    };
    let raw_payload = serde_json::json!({
        "market_id": source.market_id,
        "primary_token_id": primary_token_id,
        "secondary_token_id": secondary_token_id,
        "effective_at": primary_row.venue_event_time,
        "available_at": primary_row.persisted_time,
    });
    let market_info = ClobMarketInfoVersion {
        version_id: ClobMarketInfoVersionId::new(seeded_uuid(&format!(
            "feedback-closure:{}:{}:clob-market-info",
            source.market_id,
            decision_at.timestamp_micros()
        ))),
        market_id: source.market_id.clone(),
        tokens: vec![
            ClobTokenDescriptor {
                token_id: primary_token_id,
                outcome: "Yes".to_owned(),
            },
            ClobTokenDescriptor {
                token_id: secondary_token_id,
                outcome: "No".to_owned(),
            },
        ],
        tick_size: TickSize::QuarterCent,
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
        effective_at: DateTime::from_timestamp_millis(primary_row.venue_event_time)
            .context("closure book effective time is invalid")?,
        available_at: DateTime::from_timestamp_millis(primary_row.persisted_time)
            .context("closure book availability time is invalid")?,
        payload_hash: CanonicalDigest::content_hash_json(&raw_payload)?,
        raw_payload,
    };
    Ok(ClosureBookFacts {
        primary_ref,
        session_rows: [
            closure_session(&primary_row)?,
            closure_session(&secondary_row)?,
        ],
        ledger_rows: [primary_row, secondary_row],
        market_info,
    })
}

fn closure_resolution_facts(
    cohorts: &[CohortSeed],
    truth_cutoff: DateTime<Utc>,
) -> Result<BTreeMap<MarketId, MarketResolutionRow>> {
    let scored_population = cohorts
        .iter()
        .map(|cohort| cohort.market_universe.len())
        .sum::<usize>();
    ensure!(
        scored_population > 0,
        "closure scored-serving population is empty"
    );
    let mut facts = BTreeMap::<MarketId, MarketResolutionRow>::new();
    for cohort in cohorts {
        for scored in &cohort.market_universe {
            let resolved_at = cohort
                .resolution_by_market
                .get(&scored.market_id)
                .copied()
                .with_context(|| {
                    format!(
                        "closure scored market {} has no resolution time",
                        scored.market_id
                    )
                })?;
            let fact = closure_resolution_fact(&scored.market_id, resolved_at)?;
            ensure!(
                fact.observed_at <= truth_cutoff.timestamp_millis(),
                "closure resolution for {} is not observable by the frozen truth cutoff",
                scored.market_id
            );
            if let Some(existing) = facts.insert(scored.market_id.clone(), fact.clone()) {
                ensure!(
                    existing == fact,
                    "closure market {} has contradictory terminal resolution facts",
                    scored.market_id
                );
            }
        }
    }
    ensure!(
        !facts.is_empty(),
        "closure scored-serving population produced no resolution facts"
    );
    Ok(facts)
}

fn closure_resolution_fact(
    market_id: &MarketId,
    resolved_at: DateTime<Utc>,
) -> Result<MarketResolutionRow> {
    closure_resolution_fact_at(market_id, resolved_at, resolved_at + Duration::minutes(1))
}

fn closure_resolution_fact_at(
    market_id: &MarketId,
    resolved_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
) -> Result<MarketResolutionRow> {
    ensure!(
        resolved_at <= observed_at,
        "closure resolution observation precedes economic resolution"
    );
    let (scope, ordinal) = closure_market_identity(market_id)?;
    let yes_token_id = TokenId::new(closure_token(scope, ordinal));
    let no_token_id = closure_no_token(scope, ordinal);
    let yes_wins = closure_yes_wins(scope, ordinal)?;
    let source_block_hash = CanonicalDigest::content_hash_json(&(
        "feedback-closure-resolution-block",
        market_id,
        resolved_at,
    ))?;
    let source_transaction_hash = CanonicalDigest::content_hash_json(&(
        "feedback-closure-resolution-transaction",
        market_id,
        resolved_at,
    ))?;
    let source_checkpoint_hash = CanonicalDigest::content_hash_json(&(
        "feedback-closure-resolution-checkpoint",
        market_id,
        resolved_at,
    ))?;
    MarketResolutionRow::seal(MarketResolutionFactInput {
        market_id: market_id.clone(),
        token_ids: [yes_token_id, no_token_id],
        payout_ratios: if yes_wins {
            [PayoutRatio::ONE, PayoutRatio::ZERO]
        } else {
            [PayoutRatio::ZERO, PayoutRatio::ONE]
        },
        resolved_at: resolved_at.timestamp_millis(),
        observed_at: observed_at.timestamp_millis(),
        source_block_number: u64::try_from(ordinal)?
            .checked_add(1)
            .context("closure resolution block identity overflowed")?,
        source_block_hash: EvmBlockHash::parse(format!("0x{}", source_block_hash.hex()))?,
        source_transaction_hash: EvmTransactionHash::parse(format!(
            "0x{}",
            source_transaction_hash.hex()
        ))?,
        source_log_index: 0,
        source_checkpoint_hash,
    })
    .map_err(AnyhowError::from)
}

fn resolution_outcome(
    recommendation: &NewRecommendation,
    decision_at: DateTime<Utc>,
    resolution: &MarketResolutionRow,
) -> Result<ResolutionOutcomeActiveModel> {
    resolution.validate()?;
    ensure!(
        resolution.market_id == recommendation.market_id,
        "closure recommendation and market-resolution identities differ"
    );
    let resolved_at = DateTime::from_timestamp_millis(resolution.resolved_at)
        .context("closure resolution time is outside the supported UTC range")?;
    let source_observed_at = DateTime::from_timestamp_millis(resolution.observed_at)
        .context("closure resolution observation is outside the supported UTC range")?;
    ensure!(
        decision_at < resolved_at,
        "closure resolution must be after its recommendation decision"
    );
    let available_at = source_observed_at + Duration::minutes(1);
    let token_payout_ratio = resolution.payout_for(&recommendation.token_id)?;
    let (scope, market_ordinal) = closure_market_identity(&recommendation.market_id)?;
    let expected_won = recommendation_won(
        recommendation.outcome_side,
        closure_yes_wins(scope, market_ordinal)?,
    );
    ensure!(
        token_payout_ratio
            == if expected_won {
                PayoutRatio::ONE
            } else {
                PayoutRatio::ZERO
            },
        "closure recommendation payout disagrees with canonical market resolution"
    );
    let outcome = NewRecommendationResolutionOutcome {
        recommendation_id: recommendation.recommendation_id,
        market_id: recommendation.market_id.clone(),
        token_id: recommendation.token_id.clone(),
        resolution_kind: resolution.resolution_kind()?,
        token_payout_ratio,
        resolved_at,
        source_observed_at,
        source_checkpoint_hash: resolution.source_checkpoint_hash,
        resolution_fact_hash: resolution.resolution_fact_hash,
        resolution_fact_log_index: i64::try_from(resolution.source_log_index)?,
        resolution_fact_schema_version: SchemaVersion::FIRST,
    };
    outcome.validate()?;
    let outcome_hash = outcome.expected_outcome_hash(available_at)?;
    let mut active = outcome.into_active_model();
    active.available_at = Set(available_at);
    active.outcome_hash = Set(outcome_hash);
    active.created_at = Set(available_at);
    Ok(active)
}

const fn recommendation_won(outcome_side: OutcomeSide, canonical_yes_wins: bool) -> bool {
    match outcome_side {
        OutcomeSide::Yes => canonical_yes_wins,
        OutcomeSide::No => !canonical_yes_wins,
    }
}

const CLOSURE_STRUCTURE_DOMAIN: u64 = 0xa5a5_d3c4_e5f6_0718;
const CLOSURE_COPRIME_MULTIPLIERS: [usize; 4] = [1, 3, 5, 7];
const CLOSURE_HALF_MULTIPLIERS: [usize; 2] = [1, 3];
const CLOSURE_LABEL_NOISE_DOMAIN: u64 = 0xd1b5_4a32_d192_ed03;
const CLOSURE_PRICE_DOMAIN: u64 = 0x94d0_49bb_1331_11eb;
const CLOSURE_SPREAD_DOMAIN: u64 = 0x3f84_6b17_c2d9_5ea1;

fn closure_scope_domain(scope: &str) -> Result<u64> {
    match scope {
        "training" => Ok(0x243f_6a88_85a3_08d3),
        "calibration" => Ok(0x1319_8a2e_0370_7344),
        // Shadow is a strict replay of the evaluation population, so these
        // two scopes intentionally share one ex-ante latent population.
        "evaluation" | "shadow" => Ok(0xa409_3822_299f_31d0),
        // The mixed-Route production report is one randomized-block fixture:
        // every Crypto/Weather ordinal is the same ex-ante market population,
        // with only Route/category and its governed model lineage changing.
        "report-crypto" | "report-weather" => Ok(0x082e_fa98_ec4e_6c89),
        other => anyhow::bail!("unsupported closure latent scope `{other}`"),
    }
}

fn closure_latent_word(scope: &str, market_ordinal: usize, domain: u64) -> Result<u64> {
    let ordinal = u64::try_from(market_ordinal)
        .context("closure market ordinal exceeds deterministic latent identity")?;
    ensure!(ordinal > 0, "closure market ordinal must be positive");
    // SplitMix64's finalizer is used only as a deterministic fixture PRF. The
    // immutable population scope and latent domain are both mixed before the
    // ordinal so train/calibration/evaluation never reuse terminal noise.
    let mut value =
        (ordinal ^ domain ^ closure_scope_domain(scope)?).wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    Ok(value ^ (value >> 31))
}

fn closure_structural_slot(scope: &str, market_ordinal: usize) -> Result<usize> {
    let zero_based = market_ordinal
        .checked_sub(1)
        .context("closure structural ordinal underflowed")?;
    if scope == "training" {
        // The training universe rolls by four of eight markets. Alternate two
        // complementary, internally shuffled half-blocks so every decision
        // cross-section contains each strength/regime cell exactly once while
        // an overlapping market retains one immutable latent identity.
        // Group equal-regime slots before rotation/reversal. Every half then
        // has one cell per strength and remains exactly orthogonal to the
        // odd/even nuisance side regardless of its latent shuffle.
        const HALF_SLOTS: [[usize; 4]; 2] = [[0, 4, 3, 7], [1, 5, 2, 6]];
        let half = zero_based.div_euclid(4);
        let position = zero_based % 4;
        let half_identity = half
            .checked_add(1)
            .context("closure structural half identity overflowed")?;
        let word = closure_latent_word(scope, half_identity, CLOSURE_STRUCTURE_DOMAIN)?;
        let multiplier = CLOSURE_HALF_MULTIPLIERS[usize::try_from(word & 1)?];
        let offset = usize::try_from((word >> 1) & 3)?;
        let shuffled = (position * multiplier + offset) % 4;
        return Ok(HALF_SLOTS[half % 2][shuffled]);
    }
    let block = zero_based.div_euclid(8);
    let position = zero_based % 8;
    let block_identity = block
        .checked_add(1)
        .context("closure structural block identity overflowed")?;
    let word = closure_latent_word(scope, block_identity, CLOSURE_STRUCTURE_DOMAIN)?;
    let multiplier = CLOSURE_COPRIME_MULTIPLIERS[usize::try_from(word & 3)?];
    let offset = usize::try_from((word >> 2) & 7)?;
    Ok((position * multiplier + offset) % 8)
}

fn closure_reversion_strength(scope: &str, market_ordinal: usize) -> Result<usize> {
    Ok(closure_structural_slot(scope, market_ordinal)?.div_euclid(2) + 1)
}

fn closure_price_tier(scope: &str, market_ordinal: usize) -> Result<(usize, bool)> {
    let word = closure_latent_word(scope, market_ordinal, CLOSURE_PRICE_DOMAIN)?;
    let tier = usize::try_from(word % 4 + 1).context("closure price tier exceeds usize")?;
    Ok((tier, word & 4 != 0))
}

fn closure_market_offset(scope: &str, market_ordinal: usize) -> Result<Decimal> {
    const MAGNITUDES: [Decimal; 4] = [dec!(0.01), dec!(0.015), dec!(0.02), dec!(0.025)];
    let price_identity = match scope {
        // Evaluation and production-shadow replay compare two models on one
        // exact cross-section. Keep executable price a cohort-level nuisance:
        // it still varies independently between cohorts, but cannot reorder
        // the five markets inside a cohort and masquerade as model instability.
        "evaluation" | "shadow" | "report-crypto" | "report-weather" => market_ordinal
            .checked_sub(1)
            .and_then(|offset| {
                offset
                    .div_euclid(EVALUATION_MARKETS_PER_TICK)
                    .checked_mul(EVALUATION_MARKETS_PER_TICK)
            })
            .and_then(|offset| offset.checked_add(1))
            .context("closure evaluation price identity underflowed")?,
        // Training and calibration retain independent per-market executable
        // prices so the model sees the governed price support without sharing
        // a random stream with the signal or terminal-label processes.
        "training" | "calibration" => market_ordinal,
        other => anyhow::bail!("unsupported closure market scope `{other}`"),
    };
    let (tier, positive) = closure_price_tier(scope, price_identity)?;
    let magnitude = MAGNITUDES[tier - 1];
    Ok(if positive { magnitude } else { -magnitude })
}

fn closure_spread_width(scope: &str, market_ordinal: usize) -> Result<Decimal> {
    const WIDTHS: [Decimal; EVALUATION_MARKETS_PER_TICK] = [
        dec!(0.010),
        dec!(0.015),
        dec!(0.020),
        dec!(0.025),
        dec!(0.030),
    ];
    match scope {
        "training" | "calibration" => Ok(dec!(0.020)),
        "evaluation" | "shadow" | "report-crypto" | "report-weather" => {
            let zero_based = market_ordinal
                .checked_sub(1)
                .context("closure spread ordinal underflowed")?;
            let cohort_index = zero_based.div_euclid(EVALUATION_MARKETS_PER_TICK);
            let slot = zero_based % EVALUATION_MARKETS_PER_TICK;
            let cohort_identity = cohort_index
                .checked_add(1)
                .context("closure spread cohort identity overflowed")?;
            let word = closure_latent_word(scope, cohort_identity, CLOSURE_SPREAD_DOMAIN)?;
            let rotation = usize::try_from(
                word % u64::try_from(EVALUATION_MARKETS_PER_TICK)
                    .context("closure spread cardinality exceeds u64")?,
            )
            .context("closure spread rotation exceeds usize")?;
            let balanced_slot = if word & 8 == 0 {
                slot
            } else {
                EVALUATION_MARKETS_PER_TICK - 1 - slot
            };
            Ok(WIDTHS[(balanced_slot + rotation) % EVALUATION_MARKETS_PER_TICK])
        }
        other => anyhow::bail!("unsupported closure spread scope `{other}`"),
    }
}

fn closure_momentum_variation(
    scope: &str,
    market_ordinal: usize,
    minutes_ago: i64,
) -> Result<Decimal> {
    ensure!(
        (0..=60).contains(&minutes_ago),
        "closure momentum minute must be within [0, 60]"
    );
    let strength = i64::try_from(closure_reversion_strength(scope, market_ordinal)?)
        .context("closure signal strength exceeds i64")?;
    let sign = closure_regime_sign(scope, market_ordinal)?;
    // Both regimes have the same 15-minute start and executable end price.
    // Only the interior excursion differs, so lag-skipped momentum and window
    // mean-reversion carry the challenger signal while the common endpoint
    // return remains a non-discriminating nuisance available to the champion.
    // The regime is frozen before independent terminal label noise is drawn;
    // production code only observes the resulting sealed PIT rows.
    let start = dec!(-0.02);
    let peak = Decimal::from(sign * (8 + strength * 2)) / Decimal::from(100);
    let variation = match minutes_ago {
        15..=60 => start,
        5..=14 => {
            let elapsed = Decimal::from(15 - minutes_ago) / Decimal::from(10);
            start + (peak - start) * elapsed
        }
        0..=4 => peak * Decimal::from(minutes_ago) / Decimal::from(5),
        _ => {
            return Err(AnyhowError::msg(
                "closure momentum minute escaped its validated range",
            ));
        }
    };
    Ok(variation)
}

fn closure_yes_wins(scope: &str, market_ordinal: usize) -> Result<bool> {
    // Signal-dependent irreducible noise makes stronger pre-decision excursions
    // more reliable without making any label deterministic. The independent
    // noise stream is absent from every feature and executable-price input.
    const ERROR_BPS: [u64; 4] = [1_800, 1_100, 650, 350];
    let regime_yes = closure_regime_sign(scope, market_ordinal)? > 0;
    let strength = closure_reversion_strength(scope, market_ordinal)?;
    let draw = closure_latent_word(scope, market_ordinal, CLOSURE_LABEL_NOISE_DOMAIN)? % 10_000;
    Ok(if draw < ERROR_BPS[strength - 1] {
        !regime_yes
    } else {
        regime_yes
    })
}

fn closure_regime_sign(scope: &str, market_ordinal: usize) -> Result<i64> {
    Ok(
        if closure_structural_slot(scope, market_ordinal)?.is_multiple_of(2) {
            -1
        } else {
            1
        },
    )
}

async fn seed_historical_diagnostic(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    source_feedback_cycle_id: FeedbackCycleId,
    current_cutoff: DateTime<Utc>,
    champion: &ModelVersionInfo,
    model_spec: &ModelSpecInfo,
    recommendation_id: RecommendationId,
) -> Result<()> {
    let source_cycle = PgFeedbackCycleRepository::new(db.clone())
        .find_cycle(&source_feedback_cycle_id)
        .await?
        .with_context(|| {
            format!("closure diagnostic source cycle {source_feedback_cycle_id} is missing")
        })?;
    ensure!(
        source_cycle.profile_ref == champion.profile_ref
            && source_cycle.route == BuyModelRoute::Weather
            && source_cycle.champion_model_family == champion.model_family
            && source_cycle.label_cutoff < current_cutoff,
        "closure diagnostic source violates profile/route/family/cutoff isolation"
    );
    let available_at = current_cutoff - Duration::seconds(1);
    ensure!(
        available_at >= source_cycle.label_cutoff,
        "closure diagnostic availability precedes its source cutoff"
    );
    let input_contract_hash = model_input_contract_hash(&model_spec.input_contract)?;
    let lineage = AttributionLineage::try_new(
        source_feedback_cycle_id,
        AttributionCohort::Evaluation,
        source_cycle.label_cutoff,
        available_at,
        vec![
            champion.artifact_hash,
            champion.serving_contract_hash,
            input_contract_hash,
        ],
    )?;
    let explanation = PredictionExplanationArtifact::weighted(
        lineage,
        WeightedExplanationInput {
            model_version_id: champion.model_version_id,
            recommendation_id,
            model_artifact_hash: champion.artifact_hash,
            input_contract_hash,
            output_kind: PredictionOutputKind::CanonicalYesAlpha,
            intercept: dec!(0.01),
            terms: vec![WeightedTerm {
                input_name: "feedback_closure_alpha".to_owned(),
                encoded_value: dec!(0.5),
                weight: dec!(0.2),
            }],
        },
    )?;
    let payload = AttributionArtifact::PredictionExplanation(Box::new(explanation));
    let bytes = AttributionArtifactCodec::encode(&payload)?;
    let artifact_hash = AttributionArtifactCodec::hash(&bytes);
    let key = ArtifactKey::new(ArtifactNamespace::Attribution, artifact_hash.hex(), "json")?;
    let artifact_uri = artifact_store.put(key, &bytes).await?;
    let persisted = artifact_store.get(&artifact_uri).await?;
    ensure!(
        AttributionArtifactCodec::hash(&persisted) == artifact_hash
            && AttributionArtifactCodec::decode(&persisted)? == payload,
        "closure historical diagnostic object failed exact read-back"
    );
    let artifact = NewAttributionArtifact::try_new(
        AttributionCohort::Evaluation,
        source_feedback_cycle_id,
        AttributionSubject::Prediction {
            model_version_id: champion.model_version_id,
            recommendation_id,
        },
        artifact_uri,
        artifact_hash,
        source_cycle.label_cutoff,
    )?;
    let mut active = artifact.into_active_model();
    active.available_at = Set(available_at);
    active.created_at = Set(available_at);
    AttributionArtifactEntity::insert(active)
        .exec_without_returning(db)
        .await?;
    Ok(())
}

async fn seed_recipe(
    db: &DatabaseConnection,
    profile: &ResearchProfileArtifact,
    champion: &ModelVersionInfo,
    model_spec: &ModelSpecInfo,
    validation: &ResearchValidationConfig,
) -> Result<()> {
    let admin = UserEntity::find()
        .filter(UserColumn::Username.eq("admin"))
        .one(db)
        .await?
        .context("closure recipe approver is missing")?;
    let approved_at = db.statement_time().await;
    let template = FeedbackRecipeTemplate::try_seal(FeedbackRecipeTemplateInput {
        recipe_template_id: FeedbackRecipeTemplateId::from_v7(),
        revision: 1,
        profile_ref: profile.profile_ref.clone(),
        route: BuyModelRoute::Weather,
        model_family: champion.model_family,
        training_spec: FeedbackRecipeTrainingSpec::try_new(
            champion.model_spec_id,
            champion.model_spec_definition_hash,
            model_spec.input_contract.clone(),
            model_spec.training_contract.clone(),
            profile.spec.fit_span_days,
        )?,
        calibration_spec: FeedbackRecipeCalibrationSpec::try_new(
            CalibrationMethod::Isotonic,
            profile.spec.feedback_policy.evaluation_window_days,
        )?,
        cpcv_spec: FeedbackRecipeCpcvSpec::try_new(
            validation.clone(),
            profile.spec.target_horizon_secs,
            profile.spec.purge_embargo_secs,
        )?,
        downside_spec: FeedbackRecipeDownsideSpec::try_new(DownsideSource::MfeMae)?,
        diagnostic_spec: FeedbackRecipeDiagnosticSpec {
            accepted_artifact_kinds: vec![AttributionArtifactKind::PredictionExplanation],
            responsive_feature_names: vec!["feedback_closure_alpha".to_owned()],
            minimum_evidence_count: 1,
            minimum_feature_matches: 1,
        },
        responsive_triggers: vec![
            FeedbackDriftMetric::PopulationStabilityIndex,
            FeedbackDriftMetric::KolmogorovSmirnovPValue,
            FeedbackDriftMetric::RankIcDrop,
            FeedbackDriftMetric::JensenShannonDivergence,
        ],
        catalog_priority: 100,
        resource_budget: FeedbackRecipeResourceBudget {
            max_concurrency: 1,
            max_working_set_bytes: 10 * 1024 * 1024 * 1024,
            max_resident_model_bytes: 128 * 1024 * 1024,
            deadline_secs: CLOSURE_COMPUTE_LIVENESS_SECS,
        },
        status: FeedbackRecipeTemplateStatus::Approved,
        approved_by_user_id: Some(admin.id),
        approved_by_role: Some(RoleCode::new("super_admin")),
        approved_at: Some(approved_at),
        governance_reason: "approve deterministic production closure challenger".to_owned(),
    })?;
    PgFeedbackRecipeTemplateRepository::new(db.clone())
        .insert(template)
        .await?;
    Ok(())
}

async fn trigger_cycle(
    db: &DatabaseConnection,
    profile: &ResearchProfileArtifact,
    champion: &ModelVersionInfo,
    policy: &ActivePolicyBundle,
    label_cutoff: DateTime<Utc>,
) -> Result<FeedbackCycleInfo> {
    let route = policy
        .snapshot
        .model_routing
        .model
        .route_binding(BuyModelRoute::Weather)?;
    ensure!(
        route.champion.model_version_id == champion.model_version_id,
        "closure route champion differs from the seeded model"
    );
    let feedback_policy_hash = profile.spec.feedback_policy.content_hash()?;
    let cycle = NewFeedbackCycle::try_seal(FeedbackCycleKey::try_new(FeedbackCycleKeyInput {
        profile_ref: profile.profile_ref.clone(),
        feedback_policy_hash,
        label_cutoff,
        champion_model_version_id: champion.model_version_id,
        champion_serving_contract_hash: champion.serving_contract_hash,
        champion_model_spec_id: champion.model_spec_id,
        champion_model_spec_definition_hash: champion.model_spec_definition_hash,
        champion_model_family: champion.model_family,
        route: BuyModelRoute::Weather,
        decision_policy_snapshot_id: policy.decision_policy_snapshot_id,
        decision_policy_snapshot_hash: policy.snapshot_hash,
        policy_bundle_generation: policy.generation,
        route_generation: route.champion.generation,
        evaluation_mode: FeedbackEvaluationMode::Conditional,
        parent_cycle_id: None,
        forced_idempotency_key: None,
    })?)?;
    let cycle_id = cycle.feedback_cycle_id();
    let trigger = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
        feedback_cycle_id: cycle_id,
        event_sequence: 1,
        stage: FeedbackStage::Trigger,
        event_kind: FeedbackStageEventKind::Triggered,
        trigger_family: Some(FeedbackTriggerFamily::Manual),
        research_job_id: None,
        actor: Some("feedback_closure_fixture".to_owned()),
        reason_code: Some("production_closure_e2e".to_owned()),
        evidence_uri: None,
        evidence_hash: None,
        occurred_at: db.statement_time().await,
    })?;
    let commit = PgFeedbackCycleRepository::new(db.clone())
        .record_trigger(cycle, trigger)
        .await?;
    match commit {
        FeedbackTriggerCommit {
            cycle: FeedbackCycleWriteOutcome::Inserted(cycle),
            stage: FeedbackStageWriteOutcome::Inserted(_),
            trigger: FeedbackTriggerWriteOutcome::Inserted(_),
        }
        | FeedbackTriggerCommit {
            cycle: FeedbackCycleWriteOutcome::AlreadyPresent(cycle),
            stage: FeedbackStageWriteOutcome::AlreadyPresent(_),
            trigger: FeedbackTriggerWriteOutcome::AlreadyPresent(_),
        } => Ok(cycle),
        _ => anyhow::bail!("closure trigger write outcomes are inconsistent"),
    }
}

/// Drive the route-owned production shadow with real [`ModelRunner`] rounds and
/// wait until the production coordinator seals `CandidateReady`.
pub async fn complete_feedback_closure(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    fixture: &FeedbackClosureFixture,
) -> Result<FeedbackClosureOutcome> {
    let binding = await_shadow_binding(db, artifact_store, fixture.feedback_cycle_id).await?;
    let policy = PgPolicyRepository::new(db.clone())
        .load_current()
        .await?
        .context("closure shadow binding has no current policy snapshot")?;
    let runner = build_model_runner(db, artifact_store).await;
    let research_profile = fixture_profile_ref()
        .resolve_builtin_research_profile()
        .map_err(AnyhowError::msg)?;
    let schema = Arc::new(ExecutableFeatureSchema::build(
        &policy.snapshot.profile_artifacts.features.definition,
        research_profile.spec.feature_contract,
    )?);
    let first_decision_at = binding.bound_at + Duration::milliseconds(1);
    ensure!(
        fixture
            .shadow_cohorts
            .iter()
            .all(|cohort| cohort.catalog.available_at <= binding.bound_at),
        "closure shadow catalog was not durably visible before route binding"
    );
    await_database_time(db, first_decision_at + Duration::seconds(1)).await?;
    let observation_cohorts = Arc::clone(&fixture.shadow_cohorts);
    let requirements = runner
        .active_requirements(ActiveModelRequirementsRequest {
            policy: &policy,
            decision_at: first_decision_at,
            route: binding.route,
        })
        .await?;
    let shadow_identity = requirements.serving.published_shadow_identity()?;
    ensure!(
        shadow_identity.candidate_model_version_id == binding.candidate_model_version_id,
        "closure ModelRunner resolved a different route-owned shadow"
    );
    let decision_policy_snapshot_id = policy.decision_policy_snapshot_id;
    ensure!(
        !observation_cohorts.is_empty(),
        "closure has no shadow observation cohorts"
    );
    let window_secs = i64::try_from(shadow_identity.required_shadow_window_secs)
        .context("closure shadow window exceeds i64")?;
    let window_start = binding.bound_at;
    let window_end = window_start
        .checked_add_signed(Duration::seconds(window_secs))
        .context("closure shadow window end overflowed")?;
    ensure!(
        db.statement_time().await < window_end,
        "closure shadow preparation exhausted the frozen observation window"
    );
    let observation_query = ShadowObservationQuery {
        champion_model_version_id: shadow_identity.champion_model_version_id,
        candidate_model_version_id: shadow_identity.candidate_model_version_id,
        champion_serving_contract_hash: shadow_identity.champion_serving_contract_hash,
        candidate_serving_contract_hash: shadow_identity.candidate_serving_contract_hash,
        research_profile_artifact_id: shadow_identity.research_profile_artifact_id.clone(),
        category_scope: shadow_identity.category_scope,
        decision_policy_snapshot_id: shadow_identity.decision_policy_snapshot_id,
        decision_policy_snapshot_hash: shadow_identity.decision_policy_snapshot_hash,
        policy_bundle_generation: shadow_identity.policy_bundle_generation,
        window_start,
        window_end,
    };
    let fact_writers = Arc::clone(&fixture.fact_writers);
    let replay = Arc::clone(&fixture.replay);
    let runtime_finalized_execution_evidence = fixture.runtime_finalized_execution_evidence.clone();
    let required_features: Arc<[FeatureName]> = requirements.model_requirements.union_all().into();
    let results = stream::iter(0..SHADOW_OBSERVATION_COUNT)
        .map(|ordinal| {
            let db = db.clone();
            let runner = Arc::clone(&runner);
            let serving = requirements.serving.clone();
            let schema = Arc::clone(&schema);
            let cohorts = Arc::clone(&observation_cohorts);
            let fact_writers = Arc::clone(&fact_writers);
            let replay = Arc::clone(&replay);
            let required_features = Arc::clone(&required_features);
            let runtime_finalized_execution_evidence = runtime_finalized_execution_evidence.clone();
            async move {
                let cohort_index = ordinal.div_euclid(2) % cohorts.len();
                let cohort = cohorts[cohort_index].clone();
                let decision_at = shadow_decision_at(first_decision_at, ordinal)?;
                ensure!(
                    window_start <= decision_at && decision_at < window_end,
                    "closure shadow observation {ordinal} fell outside the frozen serving window"
                );
                let result = run_shadow_observation(ShadowObservationRequest {
                    db: &db,
                    runner: &runner,
                    serving: &serving,
                    schema: &schema,
                    facts: fact_writers.as_ref(),
                    replay: replay.as_ref(),
                    catalog: &cohort.catalog,
                    policy_snapshot_id: decision_policy_snapshot_id,
                    sources: cohort.markets.as_ref(),
                    required_features: required_features.as_ref(),
                    runtime_finalized_execution_evidence: &runtime_finalized_execution_evidence,
                    decision_at,
                    book_price_shift: cohort.book_price_shift,
                })
                .await?;
                ensure!(
                    !result.hard_divergence,
                    "closure shadow observation {ordinal} produced a hard divergence"
                );
                Ok(result)
            }
        })
        .buffer_unordered(SHADOW_OBSERVATION_CONCURRENCY)
        .try_collect::<Vec<ShadowObservationResult>>()
        .await?;
    let finished_at = db.statement_time().await;
    let elapsed_millis = finished_at
        .signed_duration_since(window_start)
        .num_milliseconds();
    ensure!(
        finished_at < window_end,
        "closure shadow observations did not finish inside the frozen serving window: finished_at={finished_at} window_end={window_end} elapsed_ms={elapsed_millis} observations={} concurrency={SHADOW_OBSERVATION_CONCURRENCY}",
        results.len()
    );
    let executable_observations = results.iter().filter(|result| result.emitted > 0).count();
    let maximum_overlap = results
        .iter()
        .map(|result| result.topn_decision_overlap)
        .max()
        .unwrap_or(Probability::ZERO);
    ensure!(
        executable_observations > 0,
        "closure shadow produced no executable candidate; maximum overlap was {maximum_overlap}"
    );
    let observed = PgShadowComparisonRepository::new(db.clone())
        .observation_window(&observation_query)
        .await?;
    validate_shadow_observations(&results, SHADOW_OBSERVATION_COUNT, observed.sample_count)?;
    let mean_overlap = observed
        .mean_topn_decision_overlap
        .context("closure shadow aggregate has no signed decision overlap")?;
    ensure!(
        mean_overlap.inner() >= shadow_identity.minimum_topn_decision_overlap.inner()
            && !observed.any_hard_divergence,
        "closure shadow is not stable: executable={executable_observations} mean_overlap={mean_overlap} required_overlap={} maximum_overlap={maximum_overlap} hard_divergence={}",
        shadow_identity.minimum_topn_decision_overlap,
        observed.any_hard_divergence
    );
    await_candidate_ready(db, fixture.feedback_cycle_id).await
}

fn validate_shadow_observations(
    results: &[ShadowObservationResult],
    expected_generated: usize,
    aggregate_count: u64,
) -> Result<()> {
    ensure!(
        results.len() == expected_generated,
        "closure shadow completed {} owned observations, expected {expected_generated}",
        results.len()
    );
    let distinct_ids = results
        .iter()
        .map(|result| result.shadow_comparison_id)
        .collect::<HashSet<_>>()
        .len();
    let distinct_hashes = results
        .iter()
        .map(|result| result.comparison_hash)
        .collect::<HashSet<_>>()
        .len();
    ensure!(
        distinct_ids == expected_generated && distinct_hashes == expected_generated,
        "closure shadow owned observations are not unique: ids={distinct_ids} hashes={distinct_hashes} expected={expected_generated}"
    );
    let expected_count = u64::try_from(expected_generated)?;
    ensure!(
        aggregate_count >= expected_count,
        "closure shadow persisted {aggregate_count} in-window comparisons, below its owned {expected_generated} observations"
    );
    Ok(())
}

fn shadow_decision_at(first: DateTime<Utc>, ordinal: usize) -> Result<DateTime<Utc>> {
    let offset_millis = i64::try_from(ordinal).context("shadow ordinal exceeds i64")?;
    first
        .checked_add_signed(Duration::milliseconds(offset_millis))
        .context("shadow decision time overflowed")
}

async fn seed_shadow_catalogs(
    db: &DatabaseConnection,
    observation_price_shifts: &[Decimal],
    capability_registry_hash: ContentHash,
    decision_at: DateTime<Utc>,
) -> Result<Arc<[ShadowObservationCohort]>> {
    ensure!(
        observation_price_shifts.len() == EVALUATION_OBSERVATION_COUNT
            && observation_price_shifts.len().is_multiple_of(2),
        "closure shadow requires the complete paired evaluation price sequence"
    );
    let cohort_count = observation_price_shifts.len().div_euclid(2);
    ensure!(cohort_count > 0, "closure has no shadow price cohorts");
    let market_created_at = decision_at - Duration::days(1);
    let resolves_at = decision_at + Duration::days(30);
    let mut cohorts = Vec::with_capacity(cohort_count);
    for cohort_index in 0..cohort_count {
        let price_shift = shadow_price_shift(observation_price_shifts, cohort_index)?;
        let (first_ordinal, last_ordinal) = shadow_market_range(cohort_index)?;
        let event_id = format!("feedback-closure-shadow-event-{cohort_index}");
        let resolutions = (first_ordinal..=last_ordinal)
            .map(|ordinal| (ordinal, resolves_at))
            .collect::<BTreeMap<_, _>>();
        let catalog = Arc::new(ClosureCatalogFacts::build(ClosureCatalogBuild {
            scope: "shadow",
            event_id: &event_id,
            category: MarketCategory::Weather,
            decision_at,
            market_created_at,
            resolutions: &resolutions,
            first_ordinal,
            last_ordinal,
            price_shift,
        })?);
        catalog.persist(db, capability_registry_hash).await?;
        let markets = (first_ordinal..=last_ordinal)
            .map(|ordinal| ClosureMarketSource {
                source_id: RecommendationId::new(seeded_uuid(&format!(
                    "feedback-closure:shadow:{ordinal}:source"
                ))),
                market_id: MarketId::new(format!("feedback-closure-shadow-market-{ordinal}")),
            })
            .collect::<Vec<_>>();
        cohorts.push(ShadowObservationCohort {
            markets: Arc::from(markets),
            book_price_shift: price_shift,
            catalog,
        });
    }
    Ok(Arc::from(cohorts))
}

/// Materialize one fresh Crypto/Weather decision plane after governed promotion.
///
/// Catalog membership is seeded before binary startup; only source-native facts
/// are written here so the subsequent HTTP report run consumes real PIT inputs.
pub async fn prepare_feedback_report_universe(
    db: &DatabaseConnection,
    fixture: &FeedbackClosureFixture,
) -> Result<FeedbackReportUniverse> {
    let categories = fixture
        .report_cohorts
        .iter()
        .map(|cohort| cohort.catalog.category)
        .collect::<Vec<_>>();
    ensure!(
        categories == vec![MarketCategory::Crypto, MarketCategory::Weather],
        "post-activation universe is not the canonical Crypto/Weather Route set"
    );
    let database_now = db.statement_time().await;
    let decision_at = DateTime::from_timestamp_millis(database_now.timestamp_millis())
        .context("mixed-Route report decision clock is outside millisecond range")?;
    let mut market_ids = Vec::with_capacity(
        fixture
            .report_cohorts
            .iter()
            .map(|cohort| cohort.markets.len())
            .sum(),
    );
    for cohort in fixture.report_cohorts.iter() {
        persist_serving_sources(
            db,
            fixture.fact_writers.as_ref(),
            fixture.replay.as_ref(),
            cohort.markets.as_ref(),
            decision_at,
            cohort.book_price_shift,
        )
        .await?;
        market_ids.extend(cohort.markets.iter().map(|market| market.market_id.clone()));
    }
    market_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    ensure!(
        market_ids.len() == EVALUATION_MARKETS_PER_TICK * categories.len(),
        "mixed-Route report universe has an incomplete market cross-section"
    );
    Ok(FeedbackReportUniverse {
        decision_at,
        market_ids,
        categories,
    })
}

/// Resolve the complete post-promotion decision plane through the canonical
///
/// `ClickHouse` truth boundary is read back before the `PostgreSQL` catalog
/// projection moves to `Settled` for the production outcome
/// reconciliation worker.
pub async fn settle_feedback_report_universe(
    db: &DatabaseConnection,
    fixture: &FeedbackClosureFixture,
    universe: &FeedbackReportUniverse,
    report_id: RecommendationReportId,
) -> Result<FeedbackReportResolutionEvidence> {
    ensure!(
        !universe.market_ids.is_empty(),
        "post-activation report universe has no market to resolve"
    );
    let report = RecommendationReportEntity::find_by_id(report_id)
        .one(db)
        .await?
        .context("post-report resolution is not bound to a committed report")?;
    let (resolved_at, observed_at) =
        feedback_resolution_times(universe.decision_at, report.decision_at)?;
    await_database_time(db, observed_at).await?;

    let mut rows = Vec::with_capacity(universe.market_ids.len());
    for market_id in &universe.market_ids {
        let (scope, _) = closure_market_identity(market_id)?;
        ensure!(
            matches!(scope, "report-crypto" | "report-weather"),
            "post-report resolution contains a non-report market {market_id}"
        );
        rows.push(closure_resolution_fact_at(
            market_id,
            resolved_at,
            observed_at,
        )?);
    }
    fixture
        .fact_writers
        .commit_resolutions(rows.clone())
        .await?;

    let markets = PgMarketRepository::new(db.clone());
    let mut facts = Vec::with_capacity(rows.len());
    for expected in rows {
        let persisted = fixture
            .fact_writers
            .fact_read
            .resolution_by_checkpoint(&expected.source_checkpoint_hash)
            .await?
            .with_context(|| {
                format!(
                    "post-report resolution checkpoint {} was acknowledged but cannot be read back",
                    expected.source_checkpoint_hash
                )
            })?;
        ensure!(
            persisted == expected,
            "post-report resolution read-back changed canonical content for {}",
            expected.market_id
        );
        let (scope, ordinal) = closure_market_identity(&expected.market_id)?;
        let resolved_outcome = if closure_yes_wins(scope, ordinal)? {
            "Yes"
        } else {
            "No"
        };
        markets
            .update_status(
                &expected.market_id,
                MarketStatus::Settled,
                Some(resolved_outcome),
            )
            .await?;
        facts.push(FeedbackResolutionFactEvidence {
            market_id: expected.market_id,
            resolved_outcome: resolved_outcome.to_owned(),
            resolved_at,
            observed_at,
            source_checkpoint_hash: expected.source_checkpoint_hash,
            resolution_fact_hash: expected.resolution_fact_hash,
        });
    }
    facts.sort_by(|left, right| left.market_id.as_str().cmp(right.market_id.as_str()));
    Ok(FeedbackReportResolutionEvidence {
        report_id,
        report_decision_at: report.decision_at,
        resolved_at,
        observed_at,
        facts,
    })
}

fn feedback_resolution_times(
    universe_decision_at: DateTime<Utc>,
    report_decision_at: DateTime<Utc>,
) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    ensure!(
        report_decision_at >= universe_decision_at,
        "committed report decision precedes its source-universe availability boundary"
    );
    let resolved_at = report_decision_at
        .checked_add_signed(Duration::milliseconds(1))
        .context("post-report resolution time overflowed")?;
    let observed_at = resolved_at
        .checked_add_signed(Duration::milliseconds(1))
        .context("post-report resolution observation time overflowed")?;
    Ok((resolved_at, observed_at))
}

fn closure_crypto_observation(decision_at: DateTime<Utc>) -> Result<DomainObservationRow> {
    const KLINE_MILLIS: i64 = 60_000;

    let decision_millis = decision_at.timestamp_millis();
    let publish_millis = decision_millis
        .div_euclid(KLINE_MILLIS)
        .checked_mul(KLINE_MILLIS)
        .context("closure Binance minute boundary overflowed")?;
    let event_millis = publish_millis
        .checked_sub(1)
        .context("closure Binance candle close underflowed")?;
    let observed_at = DateTime::from_timestamp_millis(event_millis)
        .context("closure Binance candle close is outside chrono range")?;
    let publish_time = DateTime::from_timestamp_millis(publish_millis)
        .context("closure Binance publication time is outside chrono range")?;
    ensure!(
        observed_at < publish_time && publish_time <= decision_at,
        "closure Binance candle is not closed and PIT-visible at the decision boundary"
    );
    let rule = rule_for_alias("btc").context("closure fixture has no BTC source rule")?;
    Ok(DomainObservation {
        family: DomainFamily::Crypto,
        source_id: rule.kline_source_id(),
        instrument_key: rule.instrument_key(),
        metric: DomainMetric::Close,
        value: CLOSURE_CRYPTO_CLOSE_PRICE,
        observed_at,
        publish_time,
        available_at: Some(decision_at),
    }
    .into_clickhouse_row(decision_at))
}

async fn persist_serving_sources(
    db: &DatabaseConnection,
    facts: &ClosureFactWriters,
    replay: &ClosureReplayContext,
    sources: &[ClosureMarketSource],
    decision_at: DateTime<Utc>,
    book_price_shift: Decimal,
) -> Result<Vec<ReplaySample>> {
    let first_source = sources
        .first()
        .context("closure serving-source cohort is empty")?;
    let cohort_scope = closure_market_identity(&first_source.market_id)?
        .0
        .to_owned();
    for source in sources {
        let (scope, _) = closure_market_identity(&source.market_id)?;
        ensure!(
            scope == cohort_scope,
            "closure serving-source cohort mixes scopes `{cohort_scope}` and `{scope}`"
        );
    }
    let domain_observations = if cohort_scope == "report-crypto" {
        vec![closure_crypto_observation(decision_at)?]
    } else {
        Vec::new()
    };
    let knowledge_lag_secs = replay.knowledge_lag.as_secs();
    let mut book_rows = Vec::with_capacity(sources.len() * 2);
    let mut microstructure_rows = Vec::with_capacity(sources.len() * 61);
    let mut session_rows = Vec::with_capacity(sources.len() * 2);
    let mut execution_rows = Vec::with_capacity(sources.len() * 20);
    let mut participant_rows = Vec::with_capacity(sources.len() * 40);
    let mut acceptance_rows = Vec::with_capacity(sources.len());
    let mut market_infos = Vec::with_capacity(sources.len());
    for source in sources {
        let source_facts =
            closure_book_facts(source, decision_at, knowledge_lag_secs, book_price_shift)?;
        book_rows.extend(source_facts.ledger_rows);
        session_rows.extend(source_facts.session_rows);
        market_infos.push(source_facts.market_info);
        microstructure_rows.extend(closure_serving_microstructure_rows(
            source,
            decision_at,
            knowledge_lag_secs,
            book_price_shift,
        )?);
        let execution_facts = closure_execution_history_rows(
            source,
            decision_at,
            knowledge_lag_secs,
            book_price_shift,
        )?;
        execution_rows.extend(execution_facts.executions);
        participant_rows.extend(execution_facts.participants);
        acceptance_rows.push(execution_facts.acceptance);
    }
    let market_info_repository = PgClobMarketInfoRepository::new(db.clone());
    for market_info in market_infos {
        market_info_repository
            .insert_observation(market_info)
            .await?;
    }
    facts
        .commit_sources(CohortSourceFacts {
            books: book_rows,
            microstructure: microstructure_rows,
            sessions: session_rows,
            executions: execution_rows,
            participants: participant_rows,
            acceptances: acceptance_rows,
            domain_observations,
        })
        .await?;
    sources
        .iter()
        .map(|source| {
            let (scope, ordinal) = closure_market_identity(&source.market_id)?;
            Ok(ReplaySample {
                market_id: source.market_id.clone(),
                token_id: TokenId::new(closure_token(scope, ordinal)),
            })
        })
        .collect()
}

async fn await_shadow_binding(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    feedback_cycle_id: FeedbackCycleId,
) -> Result<ShadowBindingModel> {
    let started_at = Instant::now();
    let deadline = started_at + CYCLE_TO_BIND_TIMEOUT;
    let mut liveness_deadline = started_at + CYCLE_LIVENESS_TIMEOUT;
    let mut observed_generation = None;
    let cycles = PgFeedbackCycleRepository::new(db.clone());
    loop {
        if let Some(binding) = ShadowBindingEntity::find()
            .filter(ShadowBindingColumn::FeedbackCycleId.eq(feedback_cycle_id))
            .filter(ShadowBindingColumn::Status.eq(ShadowBindingStatus::Active))
            .one(db)
            .await?
        {
            return Ok(binding);
        }
        let cycle = cycles
            .find_cycle(&feedback_cycle_id)
            .await?
            .with_context(|| format!("closure cycle {feedback_cycle_id} disappeared"))?;
        if observed_generation != Some(cycle.generation) {
            observed_generation = Some(cycle.generation);
            liveness_deadline = Instant::now() + CYCLE_LIVENESS_TIMEOUT;
        }
        if cycle.status.is_terminal() {
            let coordinator_fault = cycles.find_coordinator_fault(&feedback_cycle_id).await?;
            let events = cycles.list_stage_events(&feedback_cycle_id).await?;
            let terminal_job = events
                .iter()
                .rev()
                .find_map(|event| event.research_job_id.map(|job_id| (event.stage, job_id)));
            let terminal_job = match terminal_job {
                Some((stage, job_id)) => {
                    let job = PgResearchJobRepository::new(db.clone())
                        .find_by_id(&job_id)
                        .await?;
                    terminal_job_diagnostics(db, artifact_store, stage, job_id, job.as_ref()).await
                }
                None => "none".to_owned(),
            };
            anyhow::bail!(
                "closure cycle terminated before ShadowBind: status={:?} decision={:?} reason={:?} terminal_job={terminal_job} coordinator_fault={coordinator_fault:?}",
                cycle.status,
                cycle.decision,
                cycle.terminal_reason_code
            );
        }
        let now = Instant::now();
        let timeout_kind = if now >= deadline {
            Some(format!(
                "did not bind a shadow within the bounded full-DAG budget {CYCLE_TO_BIND_TIMEOUT:?}"
            ))
        } else if now >= liveness_deadline {
            Some(format!(
                "made no durable cycle-generation progress within {CYCLE_LIVENESS_TIMEOUT:?}"
            ))
        } else {
            None
        };
        if let Some(timeout_kind) = timeout_kind {
            let events = cycles.list_stage_events(&feedback_cycle_id).await?;
            let latest = events.last().map_or_else(
                || "no stage event".to_owned(),
                |event| {
                    format!(
                        "sequence={} stage={} kind={:?} job_id={:?}",
                        event.event_sequence, event.stage, event.event_kind, event.research_job_id
                    )
                },
            );
            let job = match events.last().and_then(|event| event.research_job_id) {
                Some(job_id) => PgResearchJobRepository::new(db.clone())
                    .find_by_id(&job_id)
                    .await?
                    .map_or_else(
                        || format!("job {job_id} missing"),
                        |job| {
                            format!(
                                "kind={} status={:?} heartbeat={:?} progress={:?} error={:?}",
                                job.kind,
                                job.status,
                                job.heartbeat_at,
                                job.progress_json,
                                job.error_json
                            )
                        },
                    ),
                None => "no linked job".to_owned(),
            };
            anyhow::bail!(
                "closure cycle {feedback_cycle_id} {timeout_kind}: latest={latest}; job={job}"
            );
        }
        sleep(CLOSURE_POLL_INTERVAL).await;
    }
}

async fn terminal_job_diagnostics(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    stage: FeedbackStage,
    job_id: ResearchJobId,
    job: Option<&ResearchJobInfo>,
) -> String {
    let Some(job) = job else {
        return format!("stage={stage} job_id={job_id} missing");
    };
    let summary = format!(
        "stage={stage} job_id={job_id} kind={} status={:?} error={:?}",
        job.kind, job.status, job.error_json
    );
    if job.kind != ResearchJobKind::FeedbackValidation
        || job.result_kind != Some(ResearchJobResultKind::FeedbackValidationArtifact)
    {
        return summary;
    }
    let Some(uri) = job.result_artifact_uri.as_ref() else {
        return format!("{summary} validation_diagnostics=missing_artifact_uri");
    };
    let artifact = match artifact_store
        .get(uri)
        .await
        .and_then(|bytes| FeedbackGovernanceCodec::decode_validation(&bytes))
    {
        Ok(artifact) => artifact,
        Err(error) => {
            return format!("{summary} validation_diagnostics_error={error}");
        }
    };
    let failures = artifact
        .candidates
        .iter()
        .flat_map(|candidate| {
            candidate
                .quality_gate_report
                .hard_failures
                .iter()
                .map(move |failure| {
                    format!(
                        "candidate={} trial={:?} gate={} observed={} threshold={} detail={}",
                        candidate.model_version_id,
                        candidate.trial_outcome,
                        failure.gate.wire_name(),
                        failure.observed,
                        failure.threshold,
                        failure.detail
                    )
                })
        })
        .collect::<Vec<_>>();
    let path_sets = join_all(
        artifact
            .candidates
            .iter()
            .map(|candidate| path_set_diagnostics(db, candidate.model_version_id)),
    )
    .await;
    format!(
        "{summary} validation_hard_failures=[{}] path_set_diagnostics=[{}]",
        failures.join("; "),
        path_sets.join("; ")
    )
}

async fn path_set_diagnostics(db: &DatabaseConnection, model_version_id: ModelVersionId) -> String {
    let path_sets = match PgBacktestPathSetRepository::new(db.clone())
        .list_by_model_version(&model_version_id)
        .await
    {
        Ok(path_sets) => path_sets,
        Err(error) => {
            return format!("candidate={model_version_id} path_set_query_error={error}");
        }
    };
    let Some(path_set) = path_sets.first() else {
        return format!("candidate={model_version_id} path_set=missing");
    };
    let representative = path_set
        .paths
        .iter()
        .min_by_key(|path| (path.sharpe - path_set.sharpe_distribution.median).abs());
    let Some(representative) = representative else {
        return format!(
            "candidate={model_version_id} path_set={} representative_path=missing",
            path_set.path_set_id
        );
    };
    let path_sharpes = path_set
        .paths
        .iter()
        .map(|path| format!("{}:{}", path.path_index, path.sharpe))
        .collect::<Vec<_>>()
        .join(",");
    let trial_sharpes = path_set
        .cscv_selection_evidence
        .trial_performances
        .iter()
        .map(|trial| format!("{}:{}", trial.trial_id, trial.full_sample_sharpe))
        .collect::<Vec<_>>()
        .join(",");
    let behavioral_classes = path_set
        .cscv_selection_evidence
        .trial_dependence
        .equivalence_classes
        .iter()
        .map(|class| {
            format!(
                "{}:{}:{:?}",
                class.class_id, class.representative_trial_id, class.member_trial_ids
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "candidate={model_version_id} path_set={} observed_sharpe={} benchmark_sharpe={} dsr={} periods={} skewness={} kurtosis={} dsr_n={} raw_m={} behavioral_m={} behavioral_trial_sharpe_variance={} trial_count_method={:?} sharpe_distribution={:?} path_sharpes=[{}] trial_sharpes=[{}] behavioral_classes=[{}]",
        path_set.path_set_id,
        representative.sharpe,
        path_set.dsr_benchmark_sharpe,
        path_set.deflated_sharpe,
        representative.group_returns.len(),
        stats::skewness(&representative.group_returns),
        stats::kurtosis(&representative.group_returns),
        path_set.dsr_conservative_independent_trial_count,
        path_set.trial_grid_count,
        path_set
            .cscv_selection_evidence
            .trial_dependence
            .equivalence_classes
            .len(),
        path_set
            .cscv_selection_evidence
            .behavioral_trial_sharpe_variance,
        path_set
            .cscv_selection_evidence
            .trial_dependence
            .trial_count_estimation,
        path_set.sharpe_distribution,
        path_sharpes,
        trial_sharpes,
        behavioral_classes,
    )
}

impl ShadowObservationRequest<'_> {
    async fn persist_sources(&self) -> Result<Vec<ReplaySample>> {
        persist_serving_sources(
            self.db,
            self.facts,
            self.replay,
            self.sources,
            self.decision_at,
            self.book_price_shift,
        )
        .await
    }

    async fn load_result(&self, emitted: u32) -> Result<ShadowObservationResult> {
        let shadow_identity = self.serving.published_shadow_identity()?;
        let comparison = ShadowComparisonEntity::find()
            .filter(
                ShadowComparisonColumn::ChampionModelVersionId
                    .eq(shadow_identity.champion_model_version_id),
            )
            .filter(
                ShadowComparisonColumn::CandidateModelVersionId
                    .eq(shadow_identity.candidate_model_version_id),
            )
            .filter(ShadowComparisonColumn::DecisionPolicySnapshotId.eq(self.policy_snapshot_id))
            .filter(ShadowComparisonColumn::DecisionAt.eq(self.decision_at))
            .one(self.db)
            .await?
            .context("successful closure shadow run did not persist its comparison")?;
        Ok(ShadowObservationResult {
            shadow_comparison_id: comparison.shadow_comparison_id,
            comparison_hash: comparison.comparison_hash,
            emitted,
            topn_decision_overlap: comparison.topn_decision_overlap,
            hard_divergence: comparison.hard_divergence,
        })
    }
}

fn frozen_finalized_execution_evidences(
    samples: &[ReplaySample],
    evidence: &FinalizedExecutionEvidence,
) -> Result<HashMap<MarketId, FinalizedExecutionEvidence>> {
    ensure!(
        evidence.runtime_parts().is_some(),
        "live serving evidence requires frozen finalized-execution history"
    );
    let sources = samples
        .iter()
        .map(|sample| (sample.market_id.clone(), evidence.clone()))
        .collect::<HashMap<_, _>>();
    ensure!(
        sources.len() == samples.len(),
        "live serving sample set contains duplicate markets"
    );
    Ok(sources)
}

async fn run_shadow_observation(
    request: ShadowObservationRequest<'_>,
) -> Result<ShadowObservationResult> {
    ensure!(
        request.sources.len() >= EVALUATION_MARKETS_PER_TICK,
        "closure shadow requires a real cross-section of at least {EVALUATION_MARKETS_PER_TICK} markets"
    );
    let samples = request.persist_sources().await?;
    let replay = request.replay;
    let window_end = request
        .decision_at
        .checked_add_signed(Duration::milliseconds(1))
        .context("closure shadow replay window end overflowed")?;
    let window = replay
        .loader
        .load(&WindowSpec {
            window_start: request.decision_at,
            window_end,
            available_by: window_end,
            samples: samples.clone(),
            lookback: replay.lookback,
            knowledge_lag: replay.knowledge_lag,
            feature_contract: replay.config.feature_contract,
            max_horizon_secs: 0,
            domain: replay.config.domain.clone(),
        })
        .await?;
    let finalized_execution_evidences = frozen_finalized_execution_evidences(
        &samples,
        request.runtime_finalized_execution_evidence,
    )?;
    let cross = materialize_cross_section(
        &replay.builder,
        ReplayFactorMode::FeatureOnly,
        &replay.config,
        &CrossSectionRequest {
            pit: &window.pit,
            prefetched: &window.prefetched,
            finalized_execution_evidence: ReplayExecutionSource::FrozenRuntime(
                &finalized_execution_evidences,
            ),
            decision_at: request.decision_at,
            group: &samples,
            required_features: request.required_features,
            category_scope: None,
            knowledge_lag: replay.knowledge_lag,
        },
    )
    .await?
    .context("closure live shadow replay resolved no catalog cross-section")?;
    let ReplayCrossSection {
        boundary,
        vectors,
        rejected_vectors,
        captures,
        markets: selections,
        factor_output,
        ..
    } = cross;
    ensure!(
        rejected_vectors.is_empty()
            && vectors.len() == request.sources.len()
            && selections.len() == vectors.len(),
        "closure live shadow retained {} of {} markets and rejected {}; first_rejected={:?}",
        vectors.len(),
        request.sources.len(),
        rejected_vectors.len(),
        rejected_vectors.first()
    );
    ensure!(
        matches!(factor_output, ReplayFactorOutput::FeatureOnly),
        "closure live shadow unexpectedly materialized a second factor plane"
    );
    let feature_repository = PgFeatureRepository::new(request.db.clone());
    let mut vector_ids = Vec::with_capacity(vectors.len());
    let mut rows = Vec::new();
    let observed_at_ms = Utc::now().timestamp_millis();
    for (selection, vector) in selections.iter().zip(&vectors) {
        let capture = captures
            .get(&ReplayCaptureKey::new(
                &vector.market_id,
                &selection.primary_token_id,
            ))
            .with_context(|| {
                format!(
                    "closure live shadow omitted capture for {}",
                    vector.market_id
                )
            })?;
        let capture = capture.evidence();
        ensure!(
            capture.snapshot.boundary == boundary,
            "closure shadow cross-section produced inconsistent decision boundaries"
        );
        request
            .catalog
            .verify_decision_ref(&vector.market_id, &capture.snapshot.catalog)?;
        let expected_market = request.catalog.market(&vector.market_id)?;
        ensure!(
            selection.liquidity_usd == expected_market.info.liquidity_usd,
            "closure shadow selection changed catalog liquidity for {}",
            vector.market_id
        );
        let persisted = feature_repository
            .create(vector.try_to_new(&boundary, &capture)?)
            .await?;
        rows.extend(feature_events(
            vector,
            &persisted,
            &boundary,
            &request.policy_snapshot_id,
            request.schema,
            observed_at_ms,
        )?);
        vector_ids.push(persisted.feature_vector_id);
    }
    let evidence = feature_commitment(&rows)?;
    request.facts.commit_shadow_features(rows).await?;
    let outcome = request
        .runner
        .run_shadow_evaluation(ModelRunRequest {
            decision_policy_snapshot_id: request.policy_snapshot_id,
            market_selection_id: None,
            selection: &selections,
            feature_vectors: &vectors,
            feature_vector_ids: vector_ids.as_slice(),
            feature_evidence: &evidence,
            serving: request.serving,
            top_n: 1,
            boundary,
        })
        .await?;
    let shadow = outcome
        .shadow
        .context("closure route emitted no shadow run")?;
    ensure!(
        shadow.failure.is_none() && shadow.diff.is_some() && shadow.model_run_id.is_some(),
        "closure shadow inference degraded: failure={:?} emitted={}",
        shadow.failure,
        shadow.emitted
    );
    request.load_result(shadow.emitted).await
}

async fn await_database_time(db: &DatabaseConnection, target: DateTime<Utc>) -> Result<()> {
    let deadline = Instant::now() + StdDuration::from_secs(5);
    while db.statement_time().await < target {
        ensure!(
            Instant::now() < deadline,
            "database clock did not reach closure observation boundary {target}"
        );
        sleep(CLOSURE_POLL_INTERVAL).await;
    }
    Ok(())
}

async fn await_candidate_ready(
    db: &DatabaseConnection,
    feedback_cycle_id: FeedbackCycleId,
) -> Result<FeedbackClosureOutcome> {
    let binding = ShadowBindingEntity::find()
        .filter(ShadowBindingColumn::FeedbackCycleId.eq(feedback_cycle_id))
        .one(db)
        .await?
        .with_context(|| format!("closure cycle {feedback_cycle_id} has no shadow binding"))?;
    await_candidate_with_binding(db, feedback_cycle_id, &binding).await
}

async fn await_candidate_with_binding(
    db: &DatabaseConnection,
    feedback_cycle_id: FeedbackCycleId,
    binding: &ShadowBindingModel,
) -> Result<FeedbackClosureOutcome> {
    let deadline = Instant::now() + CANDIDATE_READY_TIMEOUT;
    let cycles = PgFeedbackCycleRepository::new(db.clone());
    loop {
        let cycle = cycles
            .find_cycle(&feedback_cycle_id)
            .await?
            .with_context(|| format!("closure cycle {feedback_cycle_id} disappeared"))?;
        if cycle.status.is_terminal() {
            ensure!(
                cycle.status == quant_pivot_models::enums::quant::FeedbackCycleStatus::Succeeded
                    && cycle.decision == Some(FeedbackDecision::CandidateReady),
                "closure cycle did not reach CandidateReady: status={:?} decision={:?} reason={:?}",
                cycle.status,
                cycle.decision,
                cycle.terminal_reason_code
            );
            return closure_outcome(db, &cycles, feedback_cycle_id, binding).await;
        }
        ensure!(
            Instant::now() < deadline,
            "closure cycle {feedback_cycle_id} did not reach CandidateReady within {CANDIDATE_READY_TIMEOUT:?}"
        );
        sleep(CLOSURE_POLL_INTERVAL).await;
    }
}

async fn closure_outcome(
    db: &DatabaseConnection,
    cycles: &PgFeedbackCycleRepository,
    feedback_cycle_id: FeedbackCycleId,
    binding: &ShadowBindingModel,
) -> Result<FeedbackClosureOutcome> {
    ensure!(
        binding.feedback_cycle_id == feedback_cycle_id
            && binding.status == ShadowBindingStatus::Active
            && binding.terminated_at.is_none(),
        "CandidateReady closure has no active route-owned shadow binding"
    );
    let events = cycles.list_stage_events(&feedback_cycle_id).await?;
    let stage_evidence = validate_stage_ledger(db, &events).await?;
    let manifest = PgModelCandidateManifestRepository::new(db.clone())
        .find_by_id(&binding.candidate_manifest_id)
        .await?
        .context("CandidateReady shadow binding lost its immutable candidate manifest")?;
    ensure!(
        manifest.feedback_cycle_id == feedback_cycle_id
            && manifest.manifest_id == binding.candidate_manifest_id
            && manifest.manifest_hash == binding.candidate_manifest_hash
            && manifest.model_version_id == binding.candidate_model_version_id
            && manifest.document.feedback_cycle_id == feedback_cycle_id
            && manifest.document.model_version_id == binding.candidate_model_version_id,
        "CandidateReady candidate manifest differs from the route-owned shadow binding"
    );
    ensure!(
        !manifest
            .document
            .portfolio_scenario_model_bindings
            .is_empty()
            && manifest
                .document
                .portfolio_scenario_model_bindings
                .iter()
                .all(|scenario| scenario.ordered_routes.contains(&binding.route)),
        "CandidateReady manifest has no refitted scenario model covering its Route"
    );
    Ok(FeedbackClosureOutcome {
        feedback_cycle_id,
        champion_model_version_id: binding.champion_model_version_id,
        candidate_model_version_id: binding.candidate_model_version_id,
        candidate_manifest_id: manifest.manifest_id,
        candidate_manifest_hash: manifest.manifest_hash,
        scenario_model_bindings_hash: manifest.document.scenario_model_bindings_hash,
        portfolio_scenario_model_bindings: manifest.document.portfolio_scenario_model_bindings,
        stage_evidence,
    })
}

const CLOSURE_STAGES: [FeedbackStage; 15] = [
    FeedbackStage::Trigger,
    FeedbackStage::TruthFreeze,
    FeedbackStage::Coverage,
    FeedbackStage::Attribution,
    FeedbackStage::Drift,
    FeedbackStage::RecipePlan,
    FeedbackStage::DatasetSeal,
    FeedbackStage::Training,
    FeedbackStage::Calibration,
    FeedbackStage::Cpcv,
    FeedbackStage::Validation,
    FeedbackStage::Comparison,
    FeedbackStage::ShadowBind,
    FeedbackStage::Shadow,
    FeedbackStage::Decision,
];

#[derive(Default)]
struct StageAttemptEvidence {
    started_event_sequence: Option<i64>,
    attempt_ordinal: Option<i32>,
    max_attempts: Option<i32>,
    started_at: Option<DateTime<Utc>>,
    last_heartbeat_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    duration_millis: Option<i64>,
}

struct StageLedgerValidator<'a> {
    events: &'a [FeedbackStageEventInfo],
    jobs: PgResearchJobRepository,
    job_ids: HashSet<ResearchJobId>,
    terminal_sequence: i64,
}

impl<'a> StageLedgerValidator<'a> {
    fn try_new(db: &DatabaseConnection, events: &'a [FeedbackStageEventInfo]) -> Result<Self> {
        ensure!(!events.is_empty(), "closure stage ledger is empty");
        for (index, event) in events.iter().enumerate() {
            event.validate()?;
            let expected_sequence = i64::try_from(index + 1)?;
            ensure!(
                event.event_sequence == expected_sequence,
                "closure stage ledger sequence gap: expected {expected_sequence}, found {}",
                event.event_sequence
            );
            ensure!(
                !matches!(
                    event.event_kind,
                    FeedbackStageEventKind::Failed
                        | FeedbackStageEventKind::Cancelled
                        | FeedbackStageEventKind::CancellationRequested
                ),
                "closure stage ledger contains terminal failure/cancellation at sequence {}",
                event.event_sequence
            );
        }
        Ok(Self {
            events,
            jobs: PgResearchJobRepository::new(db.clone()),
            job_ids: HashSet::new(),
            terminal_sequence: 0,
        })
    }

    async fn validate_stage(
        &mut self,
        stage: FeedbackStage,
    ) -> Result<FeedbackClosureStageEvidence> {
        let stage_events = self
            .events
            .iter()
            .filter(|event| event.stage == stage)
            .collect::<Vec<_>>();
        ensure!(
            !stage_events.is_empty(),
            "closure stage ledger omitted {stage}"
        );
        let terminal_kind = if stage == FeedbackStage::Trigger {
            FeedbackStageEventKind::Triggered
        } else {
            FeedbackStageEventKind::Succeeded
        };
        let terminals = stage_events
            .iter()
            .filter(|event| event.event_kind == terminal_kind)
            .copied()
            .collect::<Vec<_>>();
        ensure!(
            terminals.len() == 1,
            "closure stage {stage} has {} terminal events, expected one",
            terminals.len()
        );
        let terminal = terminals[0];
        ensure!(
            terminal.event_sequence > self.terminal_sequence,
            "closure terminal stage order is not the canonical 15-stage DAG"
        );
        self.terminal_sequence = terminal.event_sequence;

        let attempt = if stage == FeedbackStage::Trigger {
            ensure!(
                stage_events.len() == 1
                    && terminal.research_job_id.is_none()
                    && terminal.evidence_uri.is_none()
                    && terminal.evidence_hash.is_none(),
                "closure Trigger must be one pure trigger event"
            );
            StageAttemptEvidence::default()
        } else {
            Self::validate_attempt(
                &self.jobs,
                &mut self.job_ids,
                stage,
                &stage_events,
                terminal,
            )
            .await?
        };
        Ok(FeedbackClosureStageEvidence {
            stage,
            started_event_sequence: attempt.started_event_sequence,
            event_sequence: terminal.event_sequence,
            research_job_id: terminal.research_job_id,
            attempt_ordinal: attempt.attempt_ordinal,
            max_attempts: attempt.max_attempts,
            started_at: attempt.started_at,
            last_heartbeat_at: attempt.last_heartbeat_at,
            finished_at: attempt.finished_at,
            duration_millis: attempt.duration_millis,
            evidence_uri: terminal.evidence_uri.clone(),
            evidence_hash: terminal.evidence_hash,
            event_hash: terminal.event_hash,
            occurred_at: terminal.occurred_at,
        })
    }

    async fn validate_attempt(
        jobs: &PgResearchJobRepository,
        job_ids: &mut HashSet<ResearchJobId>,
        stage: FeedbackStage,
        stage_events: &[&FeedbackStageEventInfo],
        terminal: &FeedbackStageEventInfo,
    ) -> Result<StageAttemptEvidence> {
        let job_id = terminal
            .research_job_id
            .with_context(|| format!("closure stage {stage} terminal has no job lineage"))?;
        ensure!(
            job_ids.insert(job_id),
            "closure reused research job {job_id} across DAG stages"
        );
        let linked = stage_events
            .iter()
            .filter(|event| event.event_kind == FeedbackStageEventKind::JobLinked)
            .copied()
            .collect::<Vec<_>>();
        let started = stage_events
            .iter()
            .filter(|event| event.event_kind == FeedbackStageEventKind::Started)
            .copied()
            .collect::<Vec<_>>();
        ensure!(
            linked.len() == 1
                && started.len() == 1
                && linked[0].event_sequence < started[0].event_sequence
                && started[0].event_sequence < terminal.event_sequence
                && stage_events
                    .iter()
                    .all(|event| event.research_job_id == Some(job_id)),
            "closure stage {stage} has incomplete or divergent job lineage"
        );
        ensure!(
            terminal.evidence_uri.is_some() && terminal.evidence_hash.is_some(),
            "closure stage {stage} succeeded without immutable artifact evidence"
        );
        let job = jobs
            .find_by_id(&job_id)
            .await?
            .with_context(|| format!("closure stage {stage} lost research job {job_id}"))?;
        job.validate_identity()?;
        ensure!(
            job.feedback_cycle_id == Some(terminal.feedback_cycle_id)
                && job.feedback_stage == Some(stage)
                && job.status == ResearchJobStatus::Succeeded
                && job.job_id == job_id
                && job.parent_job_id.is_none()
                && job.next_attempt_at.is_none()
                && job.lease_owner.is_none()
                && job.lease_expires_at.is_none(),
            "closure stage {stage} has invalid terminal attempt or lease state"
        );
        let job_started_at = job
            .started_at
            .with_context(|| format!("closure stage {stage} job has no started_at"))?;
        let job_heartbeat_at = job
            .heartbeat_at
            .with_context(|| format!("closure stage {stage} job has no heartbeat_at"))?;
        let job_finished_at = job
            .finished_at
            .with_context(|| format!("closure stage {stage} job has no finished_at"))?;
        ensure!(
            started[0].occurred_at == job_started_at
                && terminal.occurred_at == job_finished_at
                && job_started_at <= job_heartbeat_at
                && job_heartbeat_at <= job_finished_at
                && job.recovery_attempt >= 0
                && job.recovery_attempt <= job.max_recovery_attempts,
            "closure stage {stage} has divergent attempt timing or heartbeat evidence"
        );
        let result_artifact = job.result_artifact().with_context(|| {
            format!("closure stage {stage} terminal job has no result artifact")
        })?;
        ensure!(
            terminal.evidence_uri.as_ref() == Some(&result_artifact.uri)
                && terminal.evidence_hash == Some(result_artifact.content_hash),
            "closure stage {stage} event artifact differs from its terminal job"
        );
        Ok(StageAttemptEvidence {
            started_event_sequence: Some(started[0].event_sequence),
            attempt_ordinal: Some(
                job.recovery_attempt
                    .checked_add(1)
                    .context("closure attempt ordinal overflowed")?,
            ),
            max_attempts: Some(
                job.max_recovery_attempts
                    .checked_add(1)
                    .context("closure maximum attempt count overflowed")?,
            ),
            started_at: Some(job_started_at),
            last_heartbeat_at: Some(job_heartbeat_at),
            finished_at: Some(job_finished_at),
            duration_millis: Some(
                job_finished_at
                    .signed_duration_since(job_started_at)
                    .num_milliseconds(),
            ),
        })
    }
}

async fn validate_stage_ledger(
    db: &DatabaseConnection,
    events: &[FeedbackStageEventInfo],
) -> Result<Vec<FeedbackClosureStageEvidence>> {
    let mut validator = StageLedgerValidator::try_new(db, events)?;
    let mut evidence = Vec::with_capacity(CLOSURE_STAGES.len());
    for stage in CLOSURE_STAGES {
        evidence.push(validator.validate_stage(stage).await?);
    }
    ensure!(
        evidence.len() == 15,
        "closure terminal evidence does not cover the exact 15-stage DAG"
    );
    Ok(evidence)
}

fn evaluation_decision_points(
    start: DateTime<Utc>,
    cutoff: DateTime<Utc>,
    count: usize,
) -> Result<Vec<DateTime<Utc>>> {
    // The last report remains active for two days in the historical lifecycle,
    // and its terminal execution rollup becomes available one minute later.
    // Keep one additional minute of deterministic safety margin before the
    // frozen evaluation cutoff.
    let latest = cutoff
        .checked_sub_signed(Duration::days(2) + Duration::minutes(2))
        .context("closure evaluation maturity boundary overflowed")?;
    let points = interior_points(start, latest, count)?;
    ensure!(
        points
            .iter()
            .all(|point| { *point + Duration::days(2) + Duration::minutes(1) <= cutoff }),
        "closure evaluation decision ticks are not PIT-mature by cutoff"
    );
    Ok(points)
}

fn training_market_range(group_index: usize, observation_count: usize) -> Result<(usize, usize)> {
    ensure!(
        observation_count > 0 && observation_count.is_multiple_of(8),
        "closure rolling market range requires a cross-section divisible by eight"
    );
    let stride = observation_count / 2;
    let first_ordinal = group_index
        .checked_mul(stride)
        .and_then(|offset| offset.checked_add(1))
        .context("closure rolling market ordinal overflowed")?;
    let last_ordinal = first_ordinal
        .checked_add(observation_count - 1)
        .context("closure rolling market range overflowed")?;
    Ok((first_ordinal, last_ordinal))
}

fn calibration_market_range(
    group_index: usize,
    observation_count: usize,
) -> Result<(usize, usize)> {
    ensure!(
        observation_count > 0,
        "closure calibration market range requires a non-empty cross-section"
    );
    let first_ordinal = group_index
        .checked_mul(observation_count)
        .and_then(|offset| offset.checked_add(1))
        .context("closure calibration market ordinal overflowed")?;
    let last_ordinal = first_ordinal
        .checked_add(observation_count - 1)
        .context("closure calibration market range overflowed")?;
    Ok((first_ordinal, last_ordinal))
}

fn evaluation_market_range(index: usize) -> Result<(usize, usize)> {
    // Keep each exact five-market universe for two consecutive decision ticks.
    // This yields a deterministic 50%-churn boundary instead of fabricating a
    // completely new market universe on every report, while every
    // recommendation/report identity remains independent.
    let first_ordinal = index
        .div_euclid(2)
        .checked_mul(EVALUATION_MARKETS_PER_TICK)
        .and_then(|offset| offset.checked_add(1))
        .context("closure evaluation market ordinal overflowed")?;
    let last_ordinal = first_ordinal
        .checked_add(EVALUATION_MARKETS_PER_TICK - 1)
        .context("closure evaluation market range overflowed")?;
    Ok((first_ordinal, last_ordinal))
}

fn shadow_market_range(index: usize) -> Result<(usize, usize)> {
    let evaluation_index = index
        .checked_mul(2)
        .context("closure shadow evaluation index overflowed")?;
    evaluation_market_range(evaluation_index)
}

fn shadow_price_shift(shifts: &[Decimal], index: usize) -> Result<Decimal> {
    let first_index = index
        .checked_mul(2)
        .context("closure shadow price index overflowed")?;
    let first = shifts
        .get(first_index)
        .copied()
        .context("closure shadow price pair is missing its first tick")?;
    let second = shifts
        .get(first_index + 1)
        .copied()
        .context("closure shadow price pair is missing its second tick")?;
    ensure!(
        first == second,
        "closure shadow price pair changed inside one frozen evaluation universe"
    );
    Ok(first)
}

fn interior_points(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    count: usize,
) -> Result<Vec<DateTime<Utc>>> {
    let duration = end.signed_duration_since(start);
    ensure!(
        duration > Duration::zero() && count > 0,
        "closure timeline requires a non-empty window and point count"
    );
    let divisor = i32::try_from(
        count
            .checked_add(1)
            .context("closure timeline divisor overflowed")?,
    )?;
    let step = duration / divisor;
    (1..=count)
        .map(|ordinal| {
            let multiplier = i32::try_from(ordinal)?;
            let unaligned = start
                .checked_add_signed(step * multiplier)
                .context("closure timeline point overflowed")?;
            // Serving/source-slice records use millisecond event time while
            // PostgreSQL timestamptz is more precise. Canonicalize to the
            // narrowest shared boundary before the timestamp enters any model,
            // catalog, or evidence hash.
            let point = DateTime::from_timestamp_millis(unaligned.timestamp_millis())
                .context("closure timeline point is outside chrono range")?;
            ensure!(
                point > start && point < end,
                "closure timeline point is outside its open interval"
            );
            Ok(point)
        })
        .collect()
}

fn closure_token(scope: &str, ordinal: usize) -> String {
    let base = match scope {
        "training" => 710_000,
        "calibration" => 720_000,
        "shadow" => 740_000,
        "report-crypto" => 750_000,
        "report-weather" => 760_000,
        _ => 730_000,
    };
    (base + ordinal).to_string()
}

fn closure_no_token(scope: &str, ordinal: usize) -> TokenId {
    let base = match scope {
        "training" => 810_000,
        "calibration" => 820_000,
        "shadow" => 840_000,
        "report-crypto" => 850_000,
        "report-weather" => 860_000,
        _ => 830_000,
    };
    TokenId::new((base + ordinal).to_string())
}

fn closure_market_identity(market_id: &MarketId) -> Result<(&str, usize)> {
    let identity = market_id
        .as_str()
        .strip_prefix("feedback-closure-")
        .context("closure market id has an invalid prefix")?;
    let (scope, ordinal) = identity
        .rsplit_once("-market-")
        .context("closure market id has no ordinal")?;
    Ok((
        scope,
        ordinal.parse().context("parse closure market ordinal")?,
    ))
}

const fn category_slug(category: MarketCategory) -> &'static str {
    match category {
        MarketCategory::Geopolitics => "geopolitics",
        MarketCategory::Sports => "sports",
        MarketCategory::Politics => "politics",
        MarketCategory::Finance => "finance",
        MarketCategory::Tech => "tech",
        MarketCategory::Culture => "culture",
        MarketCategory::Weather => "weather",
        MarketCategory::Economics => "economics",
        MarketCategory::Crypto => "crypto",
        MarketCategory::Other => "other",
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet, HashSet},
        sync::Arc,
    };

    use anyhow::Result;
    use chrono::{DateTime, Duration, Utc};
    use quant_pivot_api::gamma::GammaClient;
    use quant_pivot_core::prefetch::historical_window::ReplaySample;
    use quant_pivot_models::{
        config::GammaConfig,
        domain::quant::{
            GroundingKind, LinkageOutcome, MarketSubject, PriceComparator, ResolutionOracle,
        },
        enums::{
            common::MarketCategory,
            domain::{BinanceMarketSegment, KlineInterval, LinkageSourceRole},
            quant::OutcomeSide,
        },
        hashing::CanonicalDigest,
        types::{
            ContentHash, DomainSourceId, FinalizedExecutionEvidence, MarketId, PayoutRatio,
            Probability, ShadowComparisonId, TokenId,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::{
        CALIBRATION_GROUP_COUNT, CALIBRATION_OBSERVATION_COUNT, CLOSURE_CRYPTO_CLOSE_PRICE,
        CLOSURE_CRYPTO_STRIKE_STEP, ClosureBookMetrics, ClosureCatalogBuild, ClosureCatalogFacts,
        EVALUATION_MARKETS_PER_TICK, EVALUATION_OBSERVATION_COUNT, FeedbackClosureFixture, Price,
        SHADOW_OBSERVATION_COUNT, ScenarioBucketRequirement, ScenarioTrainingPlan,
        ShadowObservationResult, TRAINING_OBSERVATION_COUNT, TRAINING_RESOLUTION_LAG_SECS,
        TRAINING_TRUTH_BUFFER_SECS, calibration_market_range, closure_bucket_at,
        closure_crypto_observation, closure_crypto_strike, closure_levels,
        closure_linkage_resolver, closure_market_offset, closure_momentum_variation,
        closure_price_tier, closure_regime_sign, closure_resolution_fact,
        closure_reversion_strength, closure_spread_width, closure_training_groups,
        closure_yes_wins, evaluation_book_price_shift, evaluation_decision_points,
        evaluation_market_range, feedback_resolution_times, frozen_finalized_execution_evidences,
        recommendation_won, rule_for_alias, shadow_decision_at, shadow_market_range,
        shadow_price_shift, training_book_price_shift, training_market_range,
        validate_shadow_observations,
    };

    #[test]
    fn runtime_sources_are_frozen() -> Result<()> {
        let samples = [
            ReplaySample {
                market_id: MarketId::new("runtime-source-a"),
                token_id: TokenId::new("runtime-token-a"),
            },
            ReplaySample {
                market_id: MarketId::new("runtime-source-b"),
                token_id: TokenId::new("runtime-token-b"),
            },
        ];
        let evidence = FinalizedExecutionEvidence::runtime(false, None, None);
        let sources = frozen_finalized_execution_evidences(&samples, &evidence)?;

        assert_eq!(sources.len(), samples.len());
        assert!(sources.values().all(|source| source == &evidence));
        assert!(
            frozen_finalized_execution_evidences(
                &samples,
                &FinalizedExecutionEvidence::materialized(Utc::now())
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn runtime_sources_reject_duplicates() {
        let samples = [
            ReplaySample {
                market_id: MarketId::new("runtime-source-duplicate"),
                token_id: TokenId::new("runtime-token-a"),
            },
            ReplaySample {
                market_id: MarketId::new("runtime-source-duplicate"),
                token_id: TokenId::new("runtime-token-b"),
            },
        ];
        let evidence = FinalizedExecutionEvidence::runtime(false, None, None);

        assert!(frozen_finalized_execution_evidences(&samples, &evidence).is_err());
    }

    fn report_catalog(scope: &str, category: MarketCategory) -> Result<ClosureCatalogFacts> {
        let decision_at = "2026-07-01T12:34:56.789Z".parse::<DateTime<Utc>>()?;
        let resolutions = (1..=EVALUATION_MARKETS_PER_TICK)
            .map(|ordinal| (ordinal, decision_at + Duration::hours(6)))
            .collect::<BTreeMap<_, _>>();
        ClosureCatalogFacts::build(ClosureCatalogBuild {
            scope,
            event_id: "feedback-closure-report-contract-event",
            category,
            decision_at,
            market_created_at: decision_at - Duration::days(1),
            resolutions: &resolutions,
            first_ordinal: 1,
            last_ordinal: EVALUATION_MARKETS_PER_TICK,
            price_shift: Decimal::ZERO,
        })
    }

    #[test]
    fn registry_preserves_history() -> Result<()> {
        let decision_at = "2026-07-01T00:00:00Z".parse::<DateTime<Utc>>()?;
        let resolutions = (1..=10)
            .map(|ordinal| {
                Ok((
                    ordinal,
                    decision_at + Duration::days(i64::try_from(ordinal)?),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let facts = ClosureCatalogFacts::build(ClosureCatalogBuild {
            scope: "evaluation",
            event_id: "feedback-closure-evaluation-event",
            category: MarketCategory::Weather,
            decision_at,
            market_created_at: decision_at - Duration::days(1),
            resolutions: &resolutions,
            first_ordinal: 6,
            last_ordinal: 10,
            price_shift: Decimal::ZERO,
        })?;
        let expected_historical = (6..=10)
            .map(|ordinal| MarketId::new(format!("feedback-closure-evaluation-market-{ordinal}")))
            .collect::<Vec<_>>();
        let expected_registry = (1..=10)
            .map(|ordinal| MarketId::new(format!("feedback-closure-evaluation-market-{ordinal}")))
            .collect::<Vec<_>>();

        assert_eq!(facts.event.market_ids, expected_historical);
        assert_eq!(facts.registry_event.market_ids, expected_registry);
        assert_ne!(facts.event_content_hash, facts.registry_event_content_hash);
        Ok(())
    }

    #[tokio::test]
    async fn gamma_catalog_round_trips() -> Result<()> {
        let decision_at = "2026-07-01T00:00:00Z".parse::<DateTime<Utc>>()?;
        let resolutions = BTreeMap::from([(1, decision_at + Duration::days(2))]);
        let facts = ClosureCatalogFacts::build(ClosureCatalogBuild {
            scope: "shadow",
            event_id: "feedback-closure-shadow-event-0",
            category: MarketCategory::Weather,
            decision_at,
            market_created_at: decision_at - Duration::days(1),
            resolutions: &resolutions,
            first_ordinal: 1,
            last_ordinal: 1,
            price_shift: dec!(0.01),
        })?;
        let market_id = MarketId::new("feedback-closure-shadow-market-1");
        let expected = facts.market(&market_id)?;
        let response = facts.gamma_response(expected, &facts.registry_event)?;
        let upstream = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/markets"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response))
            .mount(&upstream)
            .await;
        let client = GammaClient::new(GammaConfig {
            base_url: upstream.uri(),
            ..GammaConfig::default()
        });

        let actual = client.get_market(&market_id).await?;
        let actual_hash =
            CanonicalDigest::content_hash_typed("quant-pivot/catalog-market-object", 1, &actual)?;

        assert_eq!(
            serde_json::to_value(&actual)?,
            serde_json::to_value(&expected.info)?
        );
        assert_eq!(actual_hash, expected.content_hash);
        assert_eq!(actual.liquidity_usd, expected.info.liquidity_usd);
        assert_eq!(actual.volume_24h, expected.info.volume_24h);
        assert_eq!(actual.primary_category(), MarketCategory::Weather);
        assert_eq!(response[0]["events"][0]["tags"][0]["slug"], "weather");
        Ok(())
    }

    #[test]
    fn gamma_snapshot_is_latest() -> Result<()> {
        let first_decision = "2026-07-01T00:00:00Z".parse::<DateTime<Utc>>()?;
        let second_decision = first_decision + Duration::days(1);
        let resolutions = (1..=12)
            .map(|ordinal| (ordinal, second_decision + Duration::days(2)))
            .collect::<BTreeMap<_, _>>();
        let first = Arc::new(ClosureCatalogFacts::build(ClosureCatalogBuild {
            scope: "training",
            event_id: "feedback-closure-training-event",
            category: MarketCategory::Weather,
            decision_at: first_decision,
            market_created_at: first_decision - Duration::days(1),
            resolutions: &resolutions,
            first_ordinal: 1,
            last_ordinal: 8,
            price_shift: Decimal::ZERO,
        })?);
        let second = Arc::new(ClosureCatalogFacts::build(ClosureCatalogBuild {
            scope: "training",
            event_id: "feedback-closure-training-event",
            category: MarketCategory::Weather,
            decision_at: second_decision,
            market_created_at: first_decision - Duration::days(1),
            resolutions: &resolutions,
            first_ordinal: 5,
            last_ordinal: 12,
            price_shift: dec!(0.01),
        })?);

        let responses = FeedbackClosureFixture::gamma_responses_for(&[
            Arc::clone(&second),
            Arc::clone(&first),
        ])?;
        let market_one = &responses["feedback-closure-training-market-1"][0];
        let market_five = &responses["feedback-closure-training-market-5"][0];
        let market_twelve = &responses["feedback-closure-training-market-12"][0];

        assert_eq!(responses.len(), 12);
        assert_eq!(
            market_one["updatedAt"],
            serde_json::to_value(first.effective_at)?
        );
        assert_eq!(
            market_five["updatedAt"],
            serde_json::to_value(second.effective_at)?
        );
        assert_eq!(
            market_five["liquidityNum"],
            serde_json::to_value(
                second
                    .market(&MarketId::new("feedback-closure-training-market-5"))?
                    .info
                    .liquidity_usd
                    .map(|value| value.inner().to_string())
            )?
        );
        for response in [market_one, market_five, market_twelve] {
            assert_eq!(
                response["events"][0]["updatedAt"],
                serde_json::to_value(second.effective_at)?
            );
        }
        Ok(())
    }

    #[test]
    fn shadow_ticks_are_unique() -> Result<()> {
        let first = "2026-08-03T00:00:00.001Z".parse::<DateTime<Utc>>()?;
        let ticks = (0..SHADOW_OBSERVATION_COUNT)
            .map(|ordinal| shadow_decision_at(first, ordinal))
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(ticks.first(), Some(&first));
        assert_eq!(ticks.last(), Some(&(first + Duration::milliseconds(999))));
        assert_eq!(ticks.iter().collect::<HashSet<_>>().len(), ticks.len());
        assert!(
            ticks
                .last()
                .is_some_and(|last| *last < first + Duration::minutes(5))
        );
        Ok(())
    }

    #[test]
    fn serving_grid_half_open() -> Result<()> {
        let cutoff = "2026-08-03T00:00:00Z".parse::<DateTime<Utc>>()?;
        let buckets = (0_i64..=60)
            .rev()
            .map(|minutes_ago| closure_bucket_at(cutoff, minutes_ago))
            .collect::<Vec<_>>();

        assert_eq!(buckets.len(), 61);
        assert_eq!(buckets.first(), Some(&(cutoff - Duration::hours(1))));
        assert_eq!(buckets.last(), Some(&(cutoff - Duration::seconds(1))));
        assert!(buckets.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(buckets.iter().all(|bucket| *bucket < cutoff));
        assert_eq!(
            buckets
                .last()
                .copied()
                .map(|bucket| bucket + Duration::seconds(1)),
            Some(cutoff)
        );
        Ok(())
    }

    #[test]
    fn shadow_aggregate_allows_concurrency() -> Result<()> {
        let observation = |byte: u8, shadow_comparison_id| ShadowObservationResult {
            shadow_comparison_id,
            comparison_hash: ContentHash::from_bytes([byte; 32]),
            emitted: 1,
            topn_decision_overlap: Probability::ZERO,
            hard_divergence: false,
        };
        let first_id = ShadowComparisonId::from_v7();
        let results = vec![
            observation(1, first_id),
            observation(2, ShadowComparisonId::from_v7()),
        ];

        validate_shadow_observations(&results, 2, 3)?;
        assert!(validate_shadow_observations(&results, 2, 1).is_err());
        assert!(
            validate_shadow_observations(&[results[0].clone(), observation(1, first_id)], 2, 2,)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn training_prices_vary() {
        let shifts = (0..8).map(training_book_price_shift).collect::<Vec<_>>();

        assert_eq!(
            shifts,
            vec![
                Decimal::ZERO,
                dec!(0.01),
                dec!(-0.01),
                dec!(0.02),
                dec!(-0.02),
                dec!(0.01),
                dec!(-0.01),
                Decimal::ZERO,
            ]
        );
        assert_eq!(shifts.iter().copied().sum::<Decimal>(), Decimal::ZERO);
        assert_eq!(shifts.iter().copied().collect::<HashSet<_>>().len(), 5);
        assert_eq!(
            (0..32)
                .map(training_book_price_shift)
                .collect::<Vec<_>>()
                .chunks_exact(8)
                .collect::<Vec<_>>(),
            vec![shifts.as_slice(); 4]
        );
    }

    #[test]
    fn training_turnover_bounded() -> Result<()> {
        let ranges = (0..8)
            .map(|group| training_market_range(group, 8))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(
            ranges.iter().map(|(first, _)| *first).collect::<Vec<_>>(),
            vec![1, 5, 9, 13, 17, 21, 25, 29]
        );
        for pair in ranges.windows(2) {
            let overlap = pair[0]
                .1
                .min(pair[1].1)
                .checked_sub(pair[0].0.max(pair[1].0))
                .and_then(|distance| distance.checked_add(1))
                .expect("training overlap must be non-empty");
            assert_eq!(overlap, 4);
            assert_eq!(Decimal::from(8 - overlap) / Decimal::from(8), dec!(0.5));
        }
        Ok(())
    }

    #[test]
    fn training_groups_are_factorial() -> Result<()> {
        let group_count = closure_training_groups(8, 4, 16, 89)?;
        let observation_count = TRAINING_OBSERVATION_COUNT / group_count;
        assert_eq!(group_count, 96);
        assert_eq!(observation_count, 8);
        for group_index in 0..group_count {
            let (first, last) = training_market_range(group_index, observation_count)?;
            let mut cells = [[0_usize; 2]; 4];
            for ordinal in first..=last {
                let strength = closure_reversion_strength("training", ordinal)?;
                let regime = usize::from(closure_regime_sign("training", ordinal)? > 0);
                cells[strength - 1][regime] += 1;
            }
            assert_eq!(cells, [[1, 1]; 4]);
        }
        Ok(())
    }

    #[test]
    fn training_clock_is_complete() -> Result<()> {
        let window_start = "2026-05-01T00:00:00Z".parse::<DateTime<Utc>>()?;
        let cutoff = window_start + Duration::days(90);
        let latest_decision_exclusive =
            cutoff - Duration::seconds(TRAINING_RESOLUTION_LAG_SECS + TRAINING_TRUTH_BUFFER_SECS);
        let eligible_bucket_count =
            ScenarioTrainingPlan::bucket_count(window_start, latest_decision_exclusive, 86_400)?;
        let plan = ScenarioTrainingPlan {
            window_start,
            latest_decision_exclusive,
            requirements: vec![ScenarioBucketRequirement {
                bucket_secs: 86_400,
                complete_bucket_floor: 60,
                eligible_bucket_count,
            }],
            group_floor: eligible_bucket_count,
        };
        let group_count = closure_training_groups(8, 4, 16, plan.group_floor)?;
        let points = plan.points(group_count)?;
        let buckets = points
            .iter()
            .map(|point| point.timestamp().div_euclid(86_400))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        assert_eq!(eligible_bucket_count, 89);
        assert_eq!(group_count, 96);
        assert_eq!(points.len(), group_count);
        assert_eq!(buckets.len(), eligible_bucket_count);
        assert!(buckets.windows(2).all(|pair| pair[1] == pair[0] + 1));
        assert!(points.last().is_some_and(|point| {
            *point
                + Duration::seconds(TRAINING_RESOLUTION_LAG_SECS)
                + Duration::seconds(TRAINING_TRUTH_BUFFER_SECS)
                < cutoff
        }));
        Ok(())
    }

    #[test]
    fn latent_populations_are_sealed() -> Result<()> {
        let outcomes = |scope| {
            (1..=128)
                .map(|ordinal| closure_yes_wins(scope, ordinal))
                .collect::<Result<Vec<_>>>()
        };
        let training = outcomes("training")?;
        let calibration = outcomes("calibration")?;
        let evaluation = outcomes("evaluation")?;

        assert_ne!(training, calibration);
        assert_ne!(training, evaluation);
        assert_ne!(calibration, evaluation);
        assert_eq!(evaluation, outcomes("shadow")?);
        Ok(())
    }

    #[test]
    fn report_routes_share_blocks() -> Result<()> {
        for ordinal in 1..=EVALUATION_MARKETS_PER_TICK {
            assert_eq!(
                closure_reversion_strength("report-crypto", ordinal)?,
                closure_reversion_strength("report-weather", ordinal)?
            );
            assert_eq!(
                closure_regime_sign("report-crypto", ordinal)?,
                closure_regime_sign("report-weather", ordinal)?
            );
            assert_eq!(
                closure_market_offset("report-crypto", ordinal)?,
                closure_market_offset("report-weather", ordinal)?
            );
            assert_eq!(
                closure_spread_width("report-crypto", ordinal)?,
                closure_spread_width("report-weather", ordinal)?
            );
            assert_eq!(
                closure_yes_wins("report-crypto", ordinal)?,
                closure_yes_wins("report-weather", ordinal)?
            );
            for minutes_ago in [0, 5, 15, 60] {
                assert_eq!(
                    closure_momentum_variation("report-crypto", ordinal, minutes_ago)?,
                    closure_momentum_variation("report-weather", ordinal, minutes_ago)?
                );
            }
            for primary in [true, false] {
                assert_eq!(
                    closure_levels("report-crypto", primary, Decimal::ZERO, ordinal)?,
                    closure_levels("report-weather", primary, Decimal::ZERO, ordinal)?
                );
            }
        }
        Ok(())
    }

    #[test]
    fn report_crypto_linkages_resolve() -> Result<()> {
        let catalog = report_catalog("report-crypto", MarketCategory::Crypto)?;
        let resolver = closure_linkage_resolver()?;
        let source_rule = rule_for_alias("btc").expect("built-in BTC source rule");
        let mut signed_distances = BTreeSet::new();

        for (index, market) in catalog.markets.iter().enumerate() {
            let ordinal = index + 1;
            let metadata = catalog.linkage_metadata(market);
            let result = resolver.resolve(&metadata, catalog.available_at)?;
            assert_eq!(result.resolver_tier, super::ResolverTier::Tier1Template);
            let LinkageOutcome::Resolved(binding) = result.outcome else {
                panic!("report Crypto contract remained unresolved");
            };
            let MarketSubject::Crypto(subject) = &binding.subject else {
                panic!("report Crypto contract resolved to a non-Crypto subject");
            };
            assert_eq!(subject.comparator, PriceComparator::GreaterThanOrEqual);
            assert_eq!(
                subject.strike,
                Some(closure_crypto_strike("report-crypto", ordinal)?)
            );
            assert_eq!(
                subject.observation_at,
                market.info.end_date.expect("end date")
            );
            assert!(matches!(
                &subject.resolution_oracle,
                ResolutionOracle::BinanceKline {
                    market: BinanceMarketSegment::Spot,
                    symbol,
                    interval: KlineInterval::OneMinute,
                } if symbol.as_str() == "BTCUSDT"
            ));
            for field in ["asset", "strike", "resolution_oracle"] {
                assert!(binding.grounding.spans.iter().any(|span| {
                    span.subject_field == field && span.kind == GroundingKind::LiteralSpan
                }));
            }
            assert!(binding.source_bindings.iter().any(|source| {
                source.role == LinkageSourceRole::Feature
                    && source.source_id == DomainSourceId::binance()
                    && source.instrument_key == source_rule.instrument_key()
            }));

            let strike = subject.strike.expect("resolved strike").inner();
            let signed_strength = Decimal::from(
                closure_regime_sign("report-crypto", ordinal)?
                    * i64::try_from(closure_reversion_strength("report-crypto", ordinal)?)?,
            );
            assert_eq!(
                CLOSURE_CRYPTO_CLOSE_PRICE - strike,
                signed_strength * CLOSURE_CRYPTO_STRIKE_STEP
            );
            let distance = (CLOSURE_CRYPTO_CLOSE_PRICE - strike) / strike;
            assert!(!distance.is_zero());
            assert_eq!(
                distance.is_sign_positive(),
                closure_regime_sign("report-crypto", ordinal)? > 0
            );
            signed_distances.insert(distance);
        }
        assert!(signed_distances.len() > 1);

        let weather = report_catalog("report-weather", MarketCategory::Weather)?;
        for market in &weather.markets {
            let result =
                resolver.resolve(&weather.linkage_metadata(market), weather.available_at)?;
            assert!(matches!(result.outcome, LinkageOutcome::Unresolved { .. }));
        }
        Ok(())
    }

    #[test]
    fn crypto_observation_is_closed() -> Result<()> {
        let decision_at = "2026-07-01T12:34:56.789Z".parse::<DateTime<Utc>>()?;
        let row = closure_crypto_observation(decision_at)?;

        assert_eq!(row.family, "crypto");
        assert_eq!(row.source_id, DomainSourceId::binance());
        assert_eq!(row.instrument_key.as_str(), "BINANCE:BTCUSDT:1m");
        assert_eq!(row.metric, "close");
        assert_eq!(Decimal::from(row.value), CLOSURE_CRYPTO_CLOSE_PRICE);
        assert_eq!(
            row.event_time,
            "2026-07-01T12:33:59.999Z"
                .parse::<DateTime<Utc>>()?
                .timestamp_millis()
        );
        assert_eq!(
            row.publish_time,
            "2026-07-01T12:34:00Z"
                .parse::<DateTime<Utc>>()?
                .timestamp_millis()
        );
        assert_eq!(row.ingestion_time, decision_at.timestamp_millis());
        assert!(row.event_time < row.publish_time && row.publish_time <= row.ingestion_time);
        Ok(())
    }

    #[test]
    fn resolution_clock_binds_report() -> Result<()> {
        let universe_decision = "2026-07-01T12:34:56.789Z".parse::<DateTime<Utc>>()?;
        let report_decision = universe_decision + Duration::milliseconds(17);
        let (resolved_at, observed_at) =
            feedback_resolution_times(universe_decision, report_decision)?;

        assert_eq!(resolved_at, report_decision + Duration::milliseconds(1));
        assert_eq!(observed_at, report_decision + Duration::milliseconds(2));
        assert!(feedback_resolution_times(report_decision, universe_decision).is_err());
        Ok(())
    }

    #[test]
    fn calibration_ranges_partition() -> Result<()> {
        let group_size = CALIBRATION_OBSERVATION_COUNT / CALIBRATION_GROUP_COUNT;
        let ranges = (0..CALIBRATION_GROUP_COUNT)
            .map(|group| calibration_market_range(group, group_size))
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(ranges.first(), Some(&(1, group_size)));
        assert_eq!(
            ranges.last(),
            Some(&(
                CALIBRATION_OBSERVATION_COUNT - group_size + 1,
                CALIBRATION_OBSERVATION_COUNT,
            ))
        );
        assert!(ranges.windows(2).all(|pair| pair[0].1 + 1 == pair[1].0));
        assert!(
            ranges
                .iter()
                .all(|(first, last)| last - first + 1 == group_size)
        );
        Ok(())
    }

    #[test]
    fn binary_levels_cohere() -> Result<()> {
        for group_index in 1..=8 {
            let shift = training_book_price_shift(group_index);
            let (yes_bids, yes_asks) = closure_levels("training", true, shift, group_index)?;
            let (no_bids, no_asks) = closure_levels("training", false, shift, group_index)?;

            assert_eq!(
                yes_bids[0].price_decimal().inner() + no_asks[0].price_decimal().inner(),
                Decimal::ONE
            );
            assert_eq!(
                yes_asks[0].price_decimal().inner() + no_bids[0].price_decimal().inner(),
                Decimal::ONE
            );
            assert_eq!(
                yes_asks[0].price_decimal().inner() - yes_bids[0].price_decimal().inner(),
                dec!(0.02)
            );
            assert_eq!(
                no_asks[0].price_decimal().inner() - no_bids[0].price_decimal().inner(),
                dec!(0.02)
            );
        }
        Ok(())
    }

    #[test]
    fn shadow_cross_section_signal() -> Result<()> {
        let metrics = (1..=EVALUATION_MARKETS_PER_TICK)
            .map(|ordinal| {
                let (bids, asks) = closure_levels("shadow", true, Decimal::ZERO, ordinal)?;
                ClosureBookMetrics::from_levels(&bids, &asks)
            })
            .collect::<Result<Vec<_>>>()?;
        let liquidity_values = metrics
            .iter()
            .map(|value| value.visible_liquidity_usd)
            .collect::<HashSet<_>>();
        let spread_values = metrics
            .iter()
            .map(|value| value.spread_bps)
            .collect::<HashSet<_>>();
        let ask_values = metrics
            .iter()
            .map(|value| value.best_ask)
            .collect::<HashSet<_>>();
        let midpoint_values = metrics
            .iter()
            .map(|value| value.mid_price)
            .collect::<HashSet<_>>();
        let signal_values = (1..=EVALUATION_MARKETS_PER_TICK)
            .map(|ordinal| closure_momentum_variation("shadow", ordinal, 5))
            .collect::<Result<HashSet<_>>>()?;

        assert_eq!(metrics.len(), EVALUATION_MARKETS_PER_TICK);
        assert!(liquidity_values.len() >= 2);
        assert_eq!(spread_values.len(), EVALUATION_MARKETS_PER_TICK);
        assert_eq!(ask_values.len(), EVALUATION_MARKETS_PER_TICK);
        assert_eq!(midpoint_values.len(), 1);
        assert!(signal_values.len() >= 2);
        assert!(metrics.iter().enumerate().all(|(index, value)| {
            let ordinal = index + 1;
            value.depth_imbalance.is_sign_positive() != ordinal.is_multiple_of(2)
        }));
        for ordinal in 1..=EVALUATION_MARKETS_PER_TICK {
            let (yes_bids, yes_asks) = closure_levels("shadow", true, Decimal::ZERO, ordinal)?;
            let (no_bids, no_asks) = closure_levels("shadow", false, Decimal::ZERO, ordinal)?;
            let spread = Price::new(closure_spread_width("shadow", ordinal)?);

            assert_eq!(
                yes_asks[0].price_decimal() - yes_bids[0].price_decimal(),
                spread
            );
            assert_eq!(
                no_asks[0].price_decimal() - no_bids[0].price_decimal(),
                spread
            );
            assert_eq!(
                yes_bids[0].price_decimal().inner() + no_asks[0].price_decimal().inner(),
                Decimal::ONE
            );
            assert_eq!(
                yes_asks[0].price_decimal().inner() + no_bids[0].price_decimal().inner(),
                Decimal::ONE
            );
            for level in [yes_bids[0], yes_asks[0], no_bids[0], no_asks[0]] {
                assert_eq!(level.price_decimal().inner() % dec!(0.0025), Decimal::ZERO);
            }
        }
        Ok(())
    }

    #[test]
    fn shadow_spread_noise_independent() -> Result<()> {
        let mut by_width = BTreeMap::<Decimal, [usize; 3]>::new();
        for ordinal in 1..=1_250 {
            let width = closure_spread_width("shadow", ordinal)?;
            let counts = by_width.entry(width).or_default();
            counts[0] += 1;
            counts[1] += usize::from(closure_regime_sign("shadow", ordinal)? > 0);
            counts[2] += usize::from(closure_yes_wins("shadow", ordinal)?);
        }

        assert_eq!(by_width.len(), EVALUATION_MARKETS_PER_TICK);
        for counts in by_width.values() {
            assert_eq!(counts[0], 250);
            let regime_rate = Decimal::from(counts[1]) / Decimal::from(counts[0]);
            let label_rate = Decimal::from(counts[2]) / Decimal::from(counts[0]);
            assert!((dec!(0.40)..=dec!(0.60)).contains(&regime_rate));
            assert!((dec!(0.40)..=dec!(0.60)).contains(&label_rate));
        }
        Ok(())
    }

    #[test]
    fn shadow_price_noise_cohort() -> Result<()> {
        let cohort_count = EVALUATION_OBSERVATION_COUNT.div_euclid(2);
        let mut cohort_offsets = HashSet::new();
        let mut regime_agreements = 0_usize;
        let mut sample_count = 0_usize;
        for cohort_index in 0..cohort_count {
            let (first, last) = shadow_market_range(cohort_index)?;
            let first_offset = closure_market_offset("shadow", first)?;
            for ordinal in first..=last {
                let offset = closure_market_offset("shadow", ordinal)?;
                assert_eq!(offset, first_offset);
                regime_agreements += usize::from(
                    offset.is_sign_positive() == (closure_regime_sign("shadow", ordinal)? > 0),
                );
                sample_count += 1;
            }
            cohort_offsets.insert(first_offset);
        }

        let agreement = Decimal::from(regime_agreements) / Decimal::from(sample_count);
        assert_eq!(cohort_offsets.len(), 8);
        assert!((dec!(0.45)..=dec!(0.55)).contains(&agreement));
        Ok(())
    }

    #[test]
    fn evaluation_ticks_mature() -> Result<()> {
        let start = "2026-07-01T00:00:00Z".parse::<DateTime<Utc>>()?;
        let cutoff = "2026-08-01T00:00:00Z".parse::<DateTime<Utc>>()?;
        let points = evaluation_decision_points(start, cutoff, 500)?;

        assert_eq!(points.len(), 500);
        assert!(points.windows(2).all(|window| window[0] < window[1]));
        assert!(points.iter().all(|point| {
            point
                .timestamp_nanos_opt()
                .is_some_and(|value| value % 1_000_000 == 0)
        }));
        assert!(
            points
                .last()
                .is_some_and(|point| *point + Duration::days(2) + Duration::minutes(1) <= cutoff)
        );
        Ok(())
    }

    #[test]
    fn evaluation_ranges_partition() -> Result<()> {
        let ranges = (0..500)
            .map(evaluation_market_range)
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(EVALUATION_MARKETS_PER_TICK, 5);
        assert_eq!(ranges.first(), Some(&(1, 5)));
        assert_eq!(ranges.get(1), Some(&(1, 5)));
        assert_eq!(ranges.last(), Some(&(1_246, 1_250)));
        assert!(ranges.chunks_exact(2).all(|pair| pair[0] == pair[1]));
        assert!(
            ranges
                .chunks_exact(2)
                .map(|pair| pair[0])
                .collect::<Vec<_>>()
                .windows(2)
                .all(|pair| pair[0].1 + 1 == pair[1].0)
        );
        assert!(ranges.iter().all(|(first, last)| {
            (first - 1) % EVALUATION_MARKETS_PER_TICK == 0
                && last % EVALUATION_MARKETS_PER_TICK == 0
                && last - first + 1 == EVALUATION_MARKETS_PER_TICK
        }));
        Ok(())
    }

    #[test]
    fn evaluation_prices_cycle() {
        let shifts = (0..36).map(evaluation_book_price_shift).collect::<Vec<_>>();

        assert!(shifts.chunks_exact(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(&shifts[..18], &shifts[18..]);
        assert_eq!(shifts.iter().copied().min(), Some(dec!(-0.04)));
        assert_eq!(shifts.iter().copied().max(), Some(dec!(0.04)));
        assert_eq!(
            evaluation_book_price_shift(EVALUATION_OBSERVATION_COUNT - 1),
            dec!(-0.03)
        );
    }

    #[test]
    fn shadow_replays_evaluation_universes() -> Result<()> {
        let shifts = (0..EVALUATION_OBSERVATION_COUNT)
            .map(evaluation_book_price_shift)
            .collect::<Vec<_>>();
        let cohort_count = EVALUATION_OBSERVATION_COUNT.div_euclid(2);
        let ranges = (0..cohort_count)
            .map(shadow_market_range)
            .collect::<Result<Vec<_>>>()?;
        let shadow_shifts = (0..cohort_count)
            .map(|index| shadow_price_shift(&shifts, index))
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(ranges.first(), Some(&(1, 5)));
        assert_eq!(ranges.last(), Some(&(1_246, 1_250)));
        assert_eq!(shadow_shifts.len(), cohort_count);
        assert!(ranges.windows(2).all(|pair| pair[0].1 + 1 == pair[1].0));
        assert!(
            shadow_shifts
                .iter()
                .copied()
                .enumerate()
                .all(|(index, shift)| shift == evaluation_book_price_shift(index * 2))
        );
        Ok(())
    }

    #[test]
    fn recommendation_payout_projects() {
        assert!(recommendation_won(OutcomeSide::Yes, true));
        assert!(!recommendation_won(OutcomeSide::Yes, false));
        assert!(!recommendation_won(OutcomeSide::No, true));
        assert!(recommendation_won(OutcomeSide::No, false));
    }

    #[test]
    fn resolution_fact_projects_population() -> Result<()> {
        let market_id = MarketId::new("feedback-closure-evaluation-market-7");
        let resolved_at = "2026-07-20T12:00:00Z".parse::<DateTime<Utc>>()?;
        let fact = closure_resolution_fact(&market_id, resolved_at)?;

        fact.validate()?;
        assert_eq!(fact.market_id, market_id);
        assert_eq!(fact.resolved_at, resolved_at.timestamp_millis());
        assert_eq!(
            fact.observed_at,
            (resolved_at + Duration::minutes(1)).timestamp_millis()
        );
        let yes_wins = closure_yes_wins("evaluation", 7)?;
        assert_eq!(
            fact.payout_for(&TokenId::new("730007"))?,
            if yes_wins {
                PayoutRatio::ONE
            } else {
                PayoutRatio::ZERO
            }
        );
        assert_eq!(
            fact.payout_for(&TokenId::new("830007"))?,
            if yes_wins {
                PayoutRatio::ZERO
            } else {
                PayoutRatio::ONE
            }
        );
        assert_eq!(
            closure_resolution_fact(&market_id, resolved_at)?,
            fact,
            "the canonical projection must be deterministic"
        );
        assert_ne!(
            closure_resolution_fact(&market_id, resolved_at + Duration::seconds(1))?
                .resolution_fact_hash,
            fact.resolution_fact_hash,
            "economic settlement time must be content-addressed"
        );
        Ok(())
    }

    #[test]
    fn latent_streams_are_decoupled() -> Result<()> {
        let mut regime_yes = 0;
        let mut stable_agreement = 0;
        let mut price_positive = 0;
        let mut regime_price_agreement = 0;
        let mut price_tiers = [0_usize; 4];
        let mut strengths = [0_usize; 4];
        for ordinal in 1..=4_096 {
            let regime = closure_regime_sign("training", ordinal)? > 0;
            let stable = !ordinal.is_multiple_of(2);
            let (price_tier, positive_price) = closure_price_tier("training", ordinal)?;
            let strength = closure_reversion_strength("training", ordinal)?;
            regime_yes += usize::from(regime);
            stable_agreement += usize::from(regime == stable);
            price_positive += usize::from(positive_price);
            regime_price_agreement += usize::from(regime == positive_price);
            price_tiers[price_tier - 1] += 1;
            strengths[strength - 1] += 1;
        }

        assert_eq!(regime_yes, 2_048);
        assert_eq!(strengths, [1_024; 4]);
        for count in [stable_agreement, price_positive, regime_price_agreement] {
            assert!((1_850..=2_250).contains(&count));
        }
        assert!(
            price_tiers
                .iter()
                .all(|count| (900..=1_150).contains(count))
        );
        Ok(())
    }

    #[test]
    fn history_encodes_noisy_reversion() -> Result<()> {
        let mut signal_errors = 0;
        for ordinal in 1..=32 {
            let yes_wins = closure_yes_wins("training", ordinal)?;
            let regime_yes = closure_regime_sign("training", ordinal)? > 0;
            let at_fifteen = closure_momentum_variation("training", ordinal, 15)?;
            let at_five = closure_momentum_variation("training", ordinal, 5)?;
            let at_entry = closure_momentum_variation("training", ordinal, 0)?;
            let lag_skipped_move = at_five - at_fifteen;
            let recent_reversal = at_entry - at_five;
            let window_mean = (0..=15)
                .map(|minutes_ago| closure_momentum_variation("training", ordinal, minutes_ago))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .sum::<Decimal>()
                / Decimal::from(16);

            assert_eq!(lag_skipped_move.is_sign_positive(), regime_yes);
            assert_eq!(at_fifteen, dec!(-0.02));
            assert_eq!(at_five.is_sign_positive(), regime_yes);
            assert_eq!(recent_reversal.is_sign_negative(), regime_yes);
            assert_eq!(window_mean.is_sign_positive(), regime_yes);
            signal_errors += usize::from(yes_wins != regime_yes);
            assert_eq!(at_entry - at_fifteen, dec!(0.02));
            assert_eq!(at_entry, Decimal::ZERO);
        }
        assert!((1..8).contains(&signal_errors));
        Ok(())
    }

    #[test]
    fn calibration_has_both_outcomes() -> Result<()> {
        let outcomes = (1..=CALIBRATION_OBSERVATION_COUNT)
            .map(|ordinal| closure_yes_wins("calibration", ordinal))
            .collect::<Result<Vec<_>>>()?;
        let mut strength_counts = [0_usize; 4];
        let mut loss_counts = [0_usize; 4];
        let mut selected_side_wins = Vec::with_capacity(CALIBRATION_OBSERVATION_COUNT);
        for ordinal in 1..=CALIBRATION_OBSERVATION_COUNT {
            let strength = closure_reversion_strength("calibration", ordinal)?;
            let won = closure_yes_wins("calibration", ordinal)?
                == (closure_regime_sign("calibration", ordinal)? > 0);
            strength_counts[strength - 1] += 1;
            loss_counts[strength - 1] += usize::from(!won);
            selected_side_wins.push(won);
        }

        let yes_count = outcomes.iter().filter(|&&yes| yes).count();
        let win_count = selected_side_wins.iter().filter(|&&won| won).count();
        assert!(yes_count > 0 && yes_count < outcomes.len());
        assert!(win_count > 0 && win_count < selected_side_wins.len());
        assert!((460..=564).contains(&yes_count));
        assert_eq!(strength_counts, [256; 4]);
        assert!((65..=130).contains(&(selected_side_wins.len() - win_count)));
        assert!(loss_counts.iter().all(|losses| *losses > 0));
        assert!(
            loss_counts
                .iter()
                .zip(strength_counts)
                .map(|(losses, count)| Decimal::from(*losses) / Decimal::from(count))
                .collect::<Vec<_>>()
                .windows(2)
                .all(|pair| pair[0] > pair[1])
        );
        Ok(())
    }

    #[test]
    fn signal_economics_are_monotone() -> Result<()> {
        let mut returns = [Decimal::ZERO; 4];
        let mut counts = [0_usize; 4];
        let mut nuisance_return = Decimal::ZERO;
        for ordinal in 1..=4_096 {
            let regime_yes = closure_regime_sign("training", ordinal)? > 0;
            let strength = closure_reversion_strength("training", ordinal)?;
            let yes_wins = closure_yes_wins("training", ordinal)?;
            let (yes_bids, yes_asks) = closure_levels("training", true, Decimal::ZERO, ordinal)?;
            let (_, no_asks) = closure_levels("training", false, Decimal::ZERO, ordinal)?;
            let signal_price = if regime_yes {
                yes_asks[0].price_decimal().inner()
            } else {
                no_asks[0].price_decimal().inner()
            };
            let signal_won = regime_yes == yes_wins;
            returns[strength - 1] += if signal_won {
                Decimal::ONE / signal_price - Decimal::ONE
            } else {
                -Decimal::ONE
            };
            counts[strength - 1] += 1;

            let nuisance_yes = !ordinal.is_multiple_of(2);
            let nuisance_price = if nuisance_yes {
                yes_asks[0].price_decimal().inner()
            } else {
                no_asks[0].price_decimal().inner()
            };
            nuisance_return += if nuisance_yes == yes_wins {
                Decimal::ONE / nuisance_price - Decimal::ONE
            } else {
                -Decimal::ONE
            };
            assert!(yes_bids[0].price_decimal() < yes_asks[0].price_decimal());
        }
        let mean_returns = returns
            .into_iter()
            .zip(counts)
            .map(|(total, count)| total / Decimal::from(count))
            .collect::<Vec<_>>();
        let signal_return = mean_returns
            .iter()
            .zip(counts)
            .map(|(mean, count)| *mean * Decimal::from(count))
            .sum::<Decimal>()
            / Decimal::from(4_096);
        let nuisance_return = nuisance_return / Decimal::from(4_096);

        assert!(mean_returns.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(signal_return > dec!(0.80));
        assert!(
            nuisance_return < dec!(0.02),
            "fixture nuisance strategy return {nuisance_return} is not economically neutral"
        );
        assert!(signal_return - nuisance_return > dec!(0.80));
        Ok(())
    }
}
