//! Deterministic historical truth for the production feedback-closure fixture.
//!
//! This module seeds only facts that must already exist before the current
//! cadence cutoff. Every immutable row is sealed with the production domain
//! contract. Feedback stage artifacts, candidates, route bindings, permits,
//! activations, and rollback receipts are deliberately absent: the real binary
//! must create them through the production coordinator.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    mem,
    sync::Arc,
    time::Duration as StdDuration,
};

use anyhow::{Context, Error as AnyhowError, Result, ensure};
use chrono::{DateTime, Duration, Utc};
use futures_util::{StreamExt, TryStreamExt, stream};
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
            ReplayFactorMode, ReplayFactorOutput, materialize_cross_section,
        },
        market_selection::map_snapshot_to_model,
        model_runner::{ActiveModelRequirementsRequest, ModelRunRequest, ModelRunner},
        model_serving_generation::ModelServingRouteSnapshot,
    },
};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, BookStreamSessionRow, ChBps, ChDecimal64, ChDigest,
        ChPrice, ChSchemaVersion, ChShares, ChUsd, MarketResolutionFactInput, MarketResolutionRow,
        QuantFeatureEventRow, QuantModelInputEventRow, QuantReportRecommendationFactRow,
        QuantServingEvidenceCompletionRow, ReportMarketFunnelRow, TradeTapeRow,
    },
    config::ClickHouseConfig,
    domain::{
        data_plane::{
            DecisionBoundary, DecisionClock, DecisionSource,
            trade_tape_coverage::{
                FEE_RATE as TRADE_COVERAGE_FEE_RATE, MARKET_ID as TRADE_COVERAGE_MARKET_ID,
                PARTICIPANT_ADDRESS as TRADE_COVERAGE_PARTICIPANT_ADDRESS,
                PARTICIPANT_ROLE as TRADE_COVERAGE_PARTICIPANT_ROLE, PRICE as TRADE_COVERAGE_PRICE,
                SIDE as TRADE_COVERAGE_SIDE, SIZE as TRADE_COVERAGE_SIZE,
                TOKEN_ID as TRADE_COVERAGE_TOKEN_ID, TRADE_ID as TRADE_COVERAGE_TRADE_ID,
                TX_HASH as TRADE_COVERAGE_TX_HASH,
            },
        },
        market::{
            BookLevel, CATALOG_OBJECT_SCHEMA_VERSION, EventRegistryInfo, MarketRegistryInfo,
            TokenInfo,
        },
        ports::{
            FeedbackRecipeCalibrationSpec, FeedbackRecipeCpcvSpec, FeedbackRecipeDiagnosticSpec,
            FeedbackRecipeDownsideSpec, FeedbackRecipeResourceBudget, FeedbackRecipeTemplate,
            FeedbackRecipeTemplateInput, FeedbackRecipeTrainingSpec,
        },
        quant::{
            AttributionSubject, ExecutionAttemptDerivation, ExecutionAttemptOutcomeInfo,
            ExecutionAttemptSourceGraph, FeedbackCycleInfo, FeedbackCycleKey,
            FeedbackCycleKeyInput, FeedbackStageEventInput, LinkageOutcome,
            LinkageUnresolvedReason, MarketLinkageDerivation, MarketSelectionModel, ModelSpecInfo,
            ModelVersionInfo, NewAttributionArtifact, NewFeedbackCycle, NewFeedbackStageEvent,
            NewMarketLinkage, NewPosition, NewRecommendation, NewRecommendationExecutionRollup,
            NewRecommendationExecutionRollupAttempt, NewRecommendationResolutionOutcome,
            NewReportTransaction, ShadowObservationQuery,
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
        quant_feature_vector::Entity as FeatureVectorEntity,
        quant_market_linkage::Entity as MarketLinkageEntity,
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
            ChCanonicalBookEventType, ChStreamSessionEndReason, ChStreamSessionState,
            ChTradeParticipantRole, ChTradeReconciliationStatus, ChTradeSide, ChTradeTapeSource,
        },
        common::{CategorySet, MarketCategory, TickSize},
        domain::{DomainFamily, ResolverTier},
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
            RecommendationStatus, ShadowBindingStatus,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::{
        ActivePolicyBundle, BuyModelRoute, ResearchValidationConfig, SelectionConfig,
    },
    types::{
        BookSnapshotRef, BookSnapshotSource, Bps, CatalogDecisionRef, CatalogEventChangeId,
        CatalogEventObjectId, CatalogMarketChangeId, CatalogMarketObjectId, CatalogSyncBatchId,
        ClobFeeDetails, ClobMarketInfoVersion, ClobMarketInfoVersionId, ClobTokenDescriptor,
        ConfidenceSummary, ContentHash, DecisionCaptureEvidence, DecisionPolicySnapshotId,
        EligibilitySummary, EventId, EvmBlockHash, EvmTransactionHash, ExternalJsonDocument,
        FactorBreakdownEntry, FeatureVectorId, FeedbackCycleId, FeedbackRecipeTemplateId, MarketId,
        MarketLinkageId, ModelVersionId, OrderId, OrderIntentId, PayoutRatio, PositionId, Price,
        Probability, RecommendationFactorBreakdown, RecommendationId, RecommendationReportId,
        ReportFunnelDiagnostics, ReportFunnelReason, ReportFunnelStage, ResearchProfileArtifact,
        ResolverVersion, RoleCode, ScaleOutState, SchemaVersion, Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChQuantFactReadRepository},
    postgres::{
        PgCalibrationArtifactRepository, PgCatalogLedgerRepository, PgClobMarketInfoRepository,
        PgFeatureRepository, PgFeedbackCycleRepository, PgFeedbackRecipeTemplateRepository,
        PgMarketLinkageRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgPolicyRepository, PgResearchJobRepository, PgShadowComparisonRepository,
    },
    traits::{
        CalibrationArtifactRepository, CatalogLedgerRepository, ClobMarketInfoRepository,
        FactWriter, FeatureRepository, FeedbackCycleRepository, FeedbackCycleWriteOutcome,
        FeedbackRecipeTemplateRepository, FeedbackStageWriteOutcome, FeedbackTriggerCommit,
        FeedbackTriggerWriteOutcome, MarketLinkageRepository, ModelRegistryRepository,
        ModelRunRepository, PolicyRepository, QuantFactReadRepository, ResearchJobRepository,
        ShadowComparisonRepository,
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
    features::{ConfiguredFeatureBuilder, FeatureSchema, FeatureVector, feature_events},
    hashing::ResearchHasher,
    model::{
        FactorInferenceRow, FactorInferenceTable, ModelArtifact, ModelRuntimeInput,
        QuantModelRuntime, SignalCandidate, WeightedFactorRuntime, WeightedInputAuditContract,
        canonical_business_prediction_hash, finalize_candidates, model_input_contract_hash,
    },
    selection::{
        ConfiguredMarketSelector, MarketSelectionBuildRequest, MarketSelector,
        ModelFeatureRequirements,
    },
};
use quant_pivot_storage::clickhouse::{ChWriteManager, ClickHousePool};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{
    ActiveValue::Set, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    DbBackend, EntityTrait, IntoActiveModel, QueryFilter, Statement, TransactionTrait,
};
use tokio::time::{Instant, sleep};

use crate::postgres::PostgresClock;

use super::{
    execution_pg_seed::{
        ExecutionTxnIds, ReportBuildOptions, ReportSeedConfig, SharedDemoInfra,
        build_custom_report_transaction, demo_recommendation, exit_order, exit_reconciliation_row,
        fixture_profile_ref, new_capital_allocation, new_execution_order, new_order_intent,
        prepare_report_lineage_model, reconciliation_row, seed_report_catalog,
    },
    report_lifecycle_seed::{persist_and_publish_report, seal_report_facts},
    report_pipeline_harness::build_model_runner,
    seeded_uuid,
};

const TRAINING_OBSERVATION_COUNT: usize = 512;
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
const CLOSURE_COMPUTE_DEADLINE_SECS: u64 = 15 * 60;
const CYCLE_TO_BIND_TIMEOUT: StdDuration = StdDuration::from_mins(45);
const CYCLE_LIVENESS_TIMEOUT: StdDuration = StdDuration::from_mins(3);
const CANDIDATE_READY_TIMEOUT: StdDuration = StdDuration::from_mins(3);
const CLOSURE_POLL_INTERVAL: StdDuration = StdDuration::from_millis(100);
const CATALOG_BASELINE_DOMAIN: &str = "quant-pivot/system-test/feedback-closure-catalog-baseline";

/// Stable production-cycle identity created by the historical closure fixture.
pub struct FeedbackClosureFixture {
    pub feedback_cycle_id: FeedbackCycleId,
    observation_price_shifts: Arc<[Decimal]>,
    fact_writers: Arc<ClosureFactWriters>,
    replay: Arc<ClosureReplayContext>,
    capability_registry_hash: ContentHash,
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

struct ShadowObservationResult {
    emitted: u32,
    topn_decision_overlap: Probability,
    hard_divergence: bool,
}

struct ShadowObservationRequest<'a> {
    db: &'a DatabaseConnection,
    runner: &'a ModelRunner,
    serving: &'a ModelServingRouteSnapshot,
    schema: &'a FeatureSchema,
    facts: &'a ClosureFactWriters,
    replay: &'a ClosureReplayContext,
    catalog: &'a ClosureCatalogFacts,
    policy_snapshot_id: DecisionPolicySnapshotId,
    sources: &'a [ClosureMarketSource],
    decision_at: DateTime<Utc>,
    book_price_shift: Decimal,
}

struct CohortSeed {
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
            .filter_map(|recommendation| {
                recommendation
                    .trade_plan
                    .sizing()
                    .map(|sizing| sizing.suggested_usd)
            })
            .sum();
        let max_single_recommendation_usd = self
            .recommendations
            .iter()
            .filter_map(|recommendation| {
                recommendation
                    .trade_plan
                    .sizing()
                    .map(|sizing| sizing.suggested_usd)
            })
            .max()
            .unwrap_or(Usd::ZERO);
        let mut category_allocation = BTreeMap::new();
        let mut event_allocation = BTreeMap::new();
        let mut eligibility = EligibilitySummary::default();
        for recommendation in &self.recommendations {
            if let Some(sizing) = recommendation.trade_plan.sizing() {
                *category_allocation
                    .entry(recommendation.identity.category)
                    .or_default() += sizing.suggested_usd;
                *event_allocation
                    .entry(recommendation.event_id.clone())
                    .or_default() += sizing.suggested_usd;
            }
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
        let average_score = if published_count == 0 {
            Probability::default()
        } else {
            let sum = self
                .recommendations
                .iter()
                .map(|recommendation| recommendation.composite_score.inner())
                .sum::<Decimal>();
            Probability::new(sum / Decimal::from(published_count))
        };
        let confidence_summary = if published_count == 0 {
            ConfidenceSummary::default()
        } else {
            let sum = self
                .recommendations
                .iter()
                .map(|recommendation| recommendation.confidence.inner())
                .sum::<Decimal>();
            ConfidenceSummary {
                mean_confidence: Probability::new(sum / Decimal::from(published_count)),
                min_confidence: self
                    .recommendations
                    .iter()
                    .map(|recommendation| recommendation.confidence)
                    .min()
                    .unwrap_or_default(),
                max_confidence: self
                    .recommendations
                    .iter()
                    .map(|recommendation| recommendation.confidence)
                    .max()
                    .unwrap_or_default(),
            }
        };
        let is_empty = published_count == 0;
        self.summary.market_selection_count = market_selection_count;
        self.summary.candidate_count = published_count;
        self.summary.rejected_count = 0;
        self.summary.published_recommendation_count = published_count;
        self.summary.total_suggested_usd = total_suggested_usd;
        self.summary.max_single_recommendation_usd = max_single_recommendation_usd;
        self.summary.category_allocation = category_allocation;
        self.summary.event_allocation = event_allocation;
        self.summary.average_score = average_score;
        self.summary.min_score = self
            .recommendations
            .iter()
            .map(|recommendation| recommendation.composite_score)
            .min()
            .unwrap_or_default();
        self.summary.model_confidence_summary = confidence_summary;
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
            Ok(QuantReportRecommendationFactRow {
                event_time: transaction.report.decision_at.timestamp_millis(),
                recommendation_report_id: transaction.report.recommendation_report_id,
                recommendation_id: recommendation.recommendation_id,
                rank: u32::try_from(recommendation.rank)?,
                market_id: recommendation.market_id.clone(),
                token_id: recommendation.token_id.clone(),
                side: recommendation.outcome_side.into(),
                score: recommendation.composite_score.into(),
                risk_adjusted_score: recommendation.risk_adjusted_score.into(),
                trade_plan_available: recommendation.trade_plan.is_available(),
                suggested_usd: recommendation
                    .trade_plan
                    .sizing()
                    .map(|sizing| ChUsd::from(sizing.suggested_usd)),
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
            let profile_ref = recommendation.research_profile_artifact_id.profile_ref();
            let mut row = ReportMarketFunnelRow {
                event_time: transaction.report.decision_at.timestamp_millis(),
                recommendation_report_id: transaction.report.recommendation_report_id,
                market_selection_id: transaction.report.market_selection_id,
                profile_id: profile_ref.id.to_string(),
                profile_version: profile_ref.version,
                profile_content_hash: profile_ref.content_hash.to_string(),
                decision_policy_snapshot_id: transaction.report.decision_policy_snapshot_id,
                model_version_id: transaction.report.model_version_id,
                model_run_id: transaction.report.model_run_id,
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
    effective_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    markets: Vec<ClosureCatalogMarket>,
    membership_hash: ContentHash,
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

#[derive(serde::Serialize)]
struct ClosureCatalogMember<'a> {
    market_id: &'a MarketId,
    market_change_id: &'a CatalogMarketChangeId,
    content_hash: &'a ContentHash,
}

#[derive(serde::Serialize)]
struct ClosureCatalogMembership<'a> {
    event_change_id: &'a CatalogEventChangeId,
    expected_market_ids: Vec<&'a MarketId>,
    materialized_members: Vec<ClosureCatalogMember<'a>>,
}

#[derive(Clone, Copy)]
struct ClosureCatalogBuild<'a> {
    scope: &'a str,
    event_id: &'a str,
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
    decision_at: DateTime<Utc>,
    market_created_at: DateTime<Utc>,
    resolutions: &'a BTreeMap<usize, DateTime<Utc>>,
    decision_key: i64,
    price_shift: Decimal,
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
        let info = MarketRegistryInfo {
            market_id: market_id.clone(),
            event_id: self.event_id.clone(),
            token_yes: TokenId::new(closure_token(self.scope, ordinal)),
            token_no: closure_no_token(self.scope, ordinal),
            question: format!(
                "Will feedback closure {} sample {ordinal} resolve Yes?",
                self.scope
            ),
            slug: format!("feedback-closure-{}-market-{ordinal}", self.scope),
            description: Some(
                "Deterministic historical source for the production closure test".to_owned(),
            ),
            categories: CategorySet::from(MarketCategory::Weather),
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
            best_bid: Some(metrics.best_bid.inner()),
            best_ask: Some(metrics.best_ask.inner()),
            depth_usd: Some(metrics.visible_liquidity_usd),
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
            categories: CategorySet::from(MarketCategory::Weather),
            tags: Vec::new(),
            neg_risk: false,
            end_date: Some(event_end_date),
            created_at: market_created_at,
            updated_at: effective_at,
        };
        let event_content_hash =
            CanonicalDigest::content_hash_typed("quant-pivot/catalog-event-object", 1, &event)?;
        let event_object_id = CatalogEventObjectId::from_content_hash(&event_content_hash);
        let batch_id = CatalogSyncBatchId::new(seeded_uuid(&format!(
            "feedback-closure:{scope}:{first_ordinal}:{decision_key}:catalog-batch"
        )));
        let event_change_id = CatalogEventChangeId::new(seeded_uuid(&format!(
            "feedback-closure:{scope}:{first_ordinal}:{decision_key}:catalog-event-change"
        )));
        let market_builder = ClosureCatalogMarketBuild {
            scope,
            event_id: &event_id,
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
        let mut expected_market_ids = event.market_ids.iter().collect::<Vec<_>>();
        expected_market_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let materialized_members = markets
            .iter()
            .map(|market| ClosureCatalogMember {
                market_id: &market.info.market_id,
                market_change_id: &market.change_id,
                content_hash: &market.content_hash,
            })
            .collect::<Vec<_>>();
        let membership_hash = CanonicalDigest::content_hash_json(&ClosureCatalogMembership {
            event_change_id: &event_change_id,
            expected_market_ids,
            materialized_members,
        })?;
        Ok(Self {
            batch_id,
            event_change_id,
            event_object_id,
            event,
            event_content_hash,
            effective_at,
            available_at,
            markets,
            membership_hash,
        })
    }

    fn market(&self, market_id: &MarketId) -> Result<&ClosureCatalogMarket> {
        self.markets
            .binary_search_by(|market| market.info.market_id.cmp(market_id))
            .ok()
            .and_then(|index| self.markets.get(index))
            .with_context(|| format!("closure catalog is missing market {market_id}"))
    }

    fn decision_ref(&self, market_id: &MarketId) -> Result<CatalogDecisionRef> {
        let market = self.market(market_id)?;
        Ok(CatalogDecisionRef {
            catalog_sync_batch_id: self.batch_id,
            market_change_id: market.change_id,
            event_change_id: self.event_change_id,
            market_content_hash: market.content_hash,
            event_content_hash: self.event_content_hash,
            membership_hash: self.membership_hash,
            market_effective_at: self.effective_at,
            market_available_at: self.available_at,
            event_effective_at: self.effective_at,
            event_available_at: self.available_at,
            market_timestamp_quality: CatalogTimestampQuality::Source,
            event_timestamp_quality: CatalogTimestampQuality::Source,
        })
    }

    async fn persist(
        &self,
        db: &DatabaseConnection,
        capability_registry_hash: ContentHash,
    ) -> Result<()> {
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
        .exec_without_returning(&transaction)
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
        .exec_without_returning(&transaction)
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
        .exec_without_returning(&transaction)
        .await?;
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
            .exec(&transaction)
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
        .exec(&transaction)
        .await?;
        let linkages = self
            .markets
            .iter()
            .map(|market| {
                let mut linkage = NewMarketLinkage::from_derivation(MarketLinkageDerivation {
                    market_id: market.info.market_id.clone(),
                    domain_family: DomainFamily::Weather,
                    outcome: LinkageOutcome::Unresolved {
                        reason: LinkageUnresolvedReason::NoDeterministicTemplate,
                    },
                    confidence: Probability::ONE,
                    resolver_tier: ResolverTier::Tier1Template,
                    resolver_version: ResolverVersion::FIRST,
                    metadata_hash: market.content_hash,
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
            .exec(&transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
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
    trade_tape: Vec<TradeTapeRow>,
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
    schema: &'a FeatureSchema,
    runtime: &'a WeightedFactorRuntime,
    boundary: &'a DecisionBoundary,
    event_time: i64,
}

struct ClosureFactWriters {
    books: Arc<dyn FactWriter<BookL2LedgerRow>>,
    microstructure: Arc<dyn FactWriter<BookMicrostructureRow>>,
    sessions: Arc<dyn FactWriter<BookStreamSessionRow>>,
    trade_tape: Arc<dyn FactWriter<TradeTapeRow>>,
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
            trade_tape: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_trade_tape",
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
        Self::write_batches(self.trade_tape.as_ref(), facts.trade_tape)
            .await
            .context("write closure trade tape facts")?;
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
        let builder = ConfiguredFeatureBuilder::new(features, domain)?;
        let factor_engine =
            FactorEngine::for_model_scope(factors, features, domain, champion.category_scope, None);
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
        Ok(Self {
            builder,
            factor_engine,
            config: ReplayConfig {
                features: features.clone(),
                factors: factors.clone(),
                domain: domain.clone(),
                data_quality: policy.snapshot.recommendation.data_quality.clone(),
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
    let mut trade_tape_rows = Vec::with_capacity(observation_count * 40);
    let mut market_infos = Vec::with_capacity(observation_count);
    for source in sources {
        let (_, market_ordinal) = closure_market_identity(&source.market_id)?;
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
            closure_yes_wins(market_ordinal)?,
            book_price_shift,
        )?);
        trade_tape_rows.extend(closure_trade_tape_rows(
            source,
            decision_at,
            replay.knowledge_lag.as_secs(),
            book_price_shift,
        )?);
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
            trade_tape: trade_tape_rows,
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
    schema: &'a FeatureSchema,
    runtime: &'a WeightedFactorRuntime,
    facts: &'a ClosureFactWriters,
    replay: &'a ClosureReplayContext,
    capability_registry_hash: ContentHash,
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
) -> Result<Vec<CohortSeed>> {
    let points = interior_points(
        plan.training().window_start(),
        plan.training().cutoff(),
        group_count,
    )?;
    ensure!(
        TRAINING_OBSERVATION_COUNT.is_multiple_of(group_count),
        "closure training budget must divide evenly across validation groups"
    );
    let observation_count = TRAINING_OBSERVATION_COUNT / group_count;
    ensure!(
        observation_count.is_multiple_of(2),
        "closure training group size must preserve balanced binary outcomes"
    );
    let mut resolutions = BTreeMap::new();
    for (group_index, decision_at) in points.iter().copied().enumerate() {
        let (first_ordinal, last_ordinal) = training_market_range(group_index, observation_count)?;
        let resolved_at = decision_at + Duration::days(1);
        ensure!(
            resolved_at + Duration::minutes(2) <= plan.training().cutoff(),
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
            seed_report(
                context,
                CohortSpecification {
                    scope: "training",
                    decision_at,
                    market_created_at: plan.training().window_start() - Duration::days(1),
                    resolutions: &resolutions,
                    first_ordinal,
                    observation_count,
                    book_price_shift: training_book_price_shift(group_index)?,
                },
            )
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
            seed_report(
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
            )
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
                let (first_ordinal, last_ordinal) = market_range?;
                let cohort_resolutions = (first_ordinal..=last_ordinal)
                    .map(|ordinal| {
                        resolutions
                            .get(&ordinal)
                            .copied()
                            .map(|resolved_at| (ordinal, resolved_at))
                            .with_context(|| {
                                format!(
                                    "closure evaluation market {ordinal} has no terminal resolution"
                                )
                            })
                    })
                    .collect::<Result<BTreeMap<_, _>>>()?;
                PreparedCohort::prepare(
                    context,
                    CohortSpecification {
                        scope: "evaluation",
                        decision_at,
                        market_created_at,
                        resolutions: &cohort_resolutions,
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
        cohorts.push(
            cohort
                .publish(context.db, context.artifacts, context.facts)
                .await?,
        );
    }
    ensure!(
        cohorts.len() == EVALUATION_OBSERVATION_COUNT,
        "closure fixture materialized {} of {} evaluation decision ticks",
        cohorts.len(),
        EVALUATION_OBSERVATION_COUNT
    );
    Ok(cohorts)
}

/// Seed disjoint PIT cohorts, an approved recipe, and one queued cycle.
pub async fn seed_feedback_closure(
    db: &DatabaseConnection,
    clickhouse_config: &ClickHouseConfig,
    artifact_store: &Arc<dyn ArtifactStore>,
    infra: &SharedDemoInfra,
    champion_model_version_id: ModelVersionId,
    historical_feedback_cycle_id: FeedbackCycleId,
) -> Result<FeedbackClosureFixture> {
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
    let validation = &policy
        .snapshot
        .profile_artifacts
        .research_method
        .research
        .validation;
    let training_group_count =
        usize::try_from(validation.cpcv.n_groups.max(validation.pbo.block_count))
            .context("closure validation group count exceeds usize")?;
    ensure!(
        training_group_count > 0 && TRAINING_OBSERVATION_COUNT >= training_group_count,
        "closure training budget cannot satisfy the frozen CPCV/PBO timeline"
    );
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
    let artifact = ModelArtifact::load_verified(artifact_store.as_ref(), &champion).await?;
    let calibration_loader = CoreCalibrationArtifactLoader::new(Arc::new(
        PgCalibrationArtifactRepository::new(db.clone()),
    )
        as Arc<dyn CalibrationArtifactRepository>);
    let calibration = resolve_return_model_calibration(&calibration_loader, &artifact).await?;
    let runtime = WeightedFactorRuntime::new(artifact, calibration)?;
    let schema = FeatureSchema::build(&policy.snapshot.profile_artifacts.features.definition)?;
    let fact_writers = Arc::new(ClosureFactWriters::connect(clickhouse_config).await?);
    let replay = Arc::new(ClosureReplayContext::build(
        db,
        &fact_writers.fact_read,
        &policy,
        &champion,
    )?);
    let profile = fixture_profile_ref()
        .resolve_builtin_research_profile()
        .map_err(AnyhowError::msg)?;
    let database_now = db.statement_time().await;
    let plan = FeedbackCycleFreezePlan::derive(
        &profile,
        champion.model_spec_id,
        champion.model_spec_definition_hash,
        policy.decision_policy_snapshot_id,
        policy.snapshot_hash,
        database_now,
    )?;
    seed_catalog_baseline(db, plan.source_start()).await?;

    let closure_infra = SharedDemoInfra {
        feature_parity_state_id: infra.feature_parity_state_id,
        decision_policy_snapshot_id: infra.decision_policy_snapshot_id,
        model_version_id: champion.model_version_id,
        model_run_id: infra.model_run_id,
        trade_policy: infra.trade_policy.clone(),
        factor_serving_plane: serving_bindings.factors.plane.clone(),
    };
    let seed_context = CohortSeedContext {
        db,
        artifacts: artifact_store,
        infra: &closure_infra,
        champion: &champion,
        schema: &schema,
        runtime: &runtime,
        facts: fact_writers.as_ref(),
        replay: replay.as_ref(),
        capability_registry_hash,
    };
    let mut seeded = seed_training_cohorts(&seed_context, &plan, training_group_count).await?;
    seeded.extend(seed_calibration_cohorts(&seed_context, &plan).await?);
    let evaluation_seeds = seed_evaluation_cohorts(&seed_context, &plan).await?;
    let execution_attempts = seed_execution_attempts(db, &evaluation_seeds).await?;
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
        artifact_store,
        historical_feedback_cycle_id,
        plan.label_cutoff(),
        &champion,
        &model_spec,
        historical_recommendation_id,
    )
    .await?;
    seed_recipe(db, &profile, &champion, &model_spec, validation).await?;
    let cycle = trigger_cycle(db, &profile, &champion, &policy, plan.label_cutoff()).await?;
    Ok(FeedbackClosureFixture {
        feedback_cycle_id: cycle.feedback_cycle_id,
        observation_price_shifts,
        fact_writers,
        replay,
        capability_registry_hash,
    })
}

async fn seed_report(
    context: &CohortSeedContext<'_>,
    specification: CohortSpecification<'_>,
) -> Result<CohortSeed> {
    PreparedCohort::prepare(context, specification)
        .await?
        .publish(context.db, context.artifacts, context.facts)
        .await
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
        let no_token_id = closure_no_token(scope, first_ordinal);
        seed_report_catalog(
            db,
            &config.event_id,
            &config.market_id,
            &config.market_question,
            &config.market_slug,
            &config.token_id,
            &no_token_id,
        )
        .await;
        seed_catalog_clones(db, scope, first_ordinal, last_ordinal, &config.market_id).await?;
        let catalog = ClosureCatalogFacts::build(ClosureCatalogBuild {
            scope,
            event_id: &config.event_id,
            decision_at,
            market_created_at: specification.market_created_at,
            resolutions: specification.resolutions,
            first_ordinal,
            last_ordinal,
            price_shift: book_price_shift,
        })?;
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
            decision_at,
            book_price_shift,
            resolution_by_market,
            ids,
            market_universe: Vec::new(),
            recommendations,
        };
        let inference = cohort
            .persist_evidence(db, champion, schema, runtime, replay)
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

async fn seed_catalog_clones(
    db: &DatabaseConnection,
    scope: &str,
    first_ordinal: usize,
    last_ordinal: usize,
    template_market_id: &str,
) -> Result<()> {
    let sql = r"
        INSERT INTO market (
            market_id, event_id, question, slug, description, categories, status,
            filter_reasons, outcome, yes_token_id, no_token_id, tick_size, neg_risk,
            start_date, end_date, resolved_at, content_hash, created_at, updated_at
        )
        SELECT
            'feedback-closure-' || $1 || '-market-' || ordinal,
            template.event_id,
            'Feedback closure ' || $1 || ' sample ' || ordinal,
            'feedback-closure-' || $1 || '-market-' || ordinal,
            template.description,
            template.categories,
            template.status,
            template.filter_reasons,
            template.outcome,
            CASE $1
                WHEN 'training' THEN (710000 + ordinal)::text
                WHEN 'calibration' THEN (720000 + ordinal)::text
                WHEN 'shadow' THEN (740000 + ordinal)::text
                ELSE (730000 + ordinal)::text
            END,
            CASE $1
                WHEN 'training' THEN (810000 + ordinal)::text
                WHEN 'calibration' THEN (820000 + ordinal)::text
                WHEN 'shadow' THEN (840000 + ordinal)::text
                ELSE (830000 + ordinal)::text
            END,
            template.tick_size,
            template.neg_risk,
            template.start_date,
            template.end_date,
            template.resolved_at,
            template.content_hash,
            template.created_at,
            template.updated_at
        FROM market AS template
        CROSS JOIN generate_series($2, $3) AS ordinal
        WHERE template.market_id = $4
        ON CONFLICT (market_id) DO UPDATE SET
            event_id = EXCLUDED.event_id,
            question = EXCLUDED.question,
            slug = EXCLUDED.slug,
            description = EXCLUDED.description,
            categories = EXCLUDED.categories,
            status = EXCLUDED.status,
            filter_reasons = EXCLUDED.filter_reasons,
            outcome = EXCLUDED.outcome,
            yes_token_id = EXCLUDED.yes_token_id,
            no_token_id = EXCLUDED.no_token_id,
            tick_size = EXCLUDED.tick_size,
            neg_risk = EXCLUDED.neg_risk,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            resolved_at = EXCLUDED.resolved_at,
            content_hash = EXCLUDED.content_hash,
            updated_at = EXCLUDED.updated_at
    ";
    let clone_start = first_ordinal
        .checked_add(1)
        .context("closure clone start overflowed")?;
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            [
                scope.to_owned().into(),
                i32::try_from(clone_start)?.into(),
                i32::try_from(last_ordinal)?.into(),
                template_market_id.to_owned().into(),
            ],
        ))
        .await?;
    ensure!(
        result.rows_affected() == u64::try_from(last_ordinal - first_ordinal)?,
        "closure catalog clone count drifted"
    );
    Ok(())
}

async fn align_report_history(db: &DatabaseConnection, cohorts: &[CohortSeed]) -> Result<()> {
    for (index, cohort) in cohorts.iter().enumerate() {
        let created_at = cohort.decision_at + Duration::seconds(1);
        let recommendation_at = cohort.decision_at + Duration::seconds(2);
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
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            r"
                UPDATE quant_recommendation_report
                SET decision_at = $2,
                    created_at = $3,
                    published_at = $4,
                    superseded_at = $5
                WHERE recommendation_report_id = $1
            ",
            [
                cohort.ids.report.into(),
                cohort.decision_at.into(),
                created_at.into(),
                published_at.into(),
                superseded_at.into(),
            ],
        ))
        .await?;
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            r"
                UPDATE quant_recommendation
                SET status = $2::qp_recommendation_status,
                    created_at = $3,
                    status_changed_at = $4
                WHERE recommendation_report_id = $1
            ",
            [
                cohort.ids.report.into(),
                recommendation_status.into(),
                recommendation_at.into(),
                recommendation_terminal_at.into(),
            ],
        ))
        .await?;
        db.execute_raw(Statement::from_sql_and_values(
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
            recommendation.composite_score = candidate.composite_score;
            recommendation.risk_adjusted_score = candidate.composite_score;
            recommendation.confidence = candidate.confidence;
            recommendation.expected_return_bps = Bps::new(candidate.expected_return_bps);
            recommendation.downside_bps = Bps::new(candidate.downside_bps);
            recommendation.rank_before_portfolio =
                i32::try_from(candidate.rank_before_portfolio)
                    .context("closure pre-portfolio rank exceeds i32")?;
            recommendation.liquidity_score = candidate.liquidity_score;
            recommendation.data_quality_score = candidate.data_quality_score;
            recommendation.model_score_percentile = candidate.model_score_percentile;
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
        schema: &FeatureSchema,
        runtime: &WeightedFactorRuntime,
        replay: &ClosureReplayContext,
    ) -> Result<CohortInferenceResult> {
        let created_at = self.decision_at + Duration::seconds(2);
        let event_time = created_at.timestamp_millis();
        let boundary = DecisionClock::new(replay.knowledge_lag.as_secs()).serving_boundary(
            self.decision_at,
            replay.config.domain.crypto.availability_lag_secs,
            replay.config.domain.weather.availability_lag_secs,
        )?;
        let cross = self.replay_cross_section(replay, &boundary).await?;
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
                max_horizon_secs: 0,
                domain: replay.config.domain.clone(),
            })
            .await?;
        let cross = materialize_cross_section(
            &replay.builder,
            ReplayFactorMode::FactorNative {
                engine: &replay.factor_engine,
            },
            &replay.config,
            &CrossSectionRequest {
                // Production DatasetSeal/Training materializes the prefetched
                // source slice into this zero-I/O PIT engine before replay. Use
                // that exact path so an observation batch cannot fan out one
                // PostgreSQL query graph per market.
                pit: &window.pit,
                prefetched: &window.prefetched,
                decision_at: self.decision_at,
                group: &samples,
                category_scope: None,
                knowledge_lag: replay.knowledge_lag,
                lookback: replay.lookback,
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
            recommendation.liquidity_score = capture.liquidity_score;
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
        let evidence = feature_commitment(&feature_rows)?.bind_model_vectors(&vector_ids)?;
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

fn training_book_price_shift(group_index: usize) -> Result<Decimal> {
    let pair_index = group_index.div_euclid(2);
    let magnitude = i64::try_from(pair_index.div_ceil(2))
        .context("closure training price-shift magnitude exceeds i64")?;
    let signed_ticks = if pair_index.is_multiple_of(2) {
        -magnitude
    } else {
        magnitude
    };
    let shift = Decimal::from(signed_ticks) / Decimal::from(100);
    ensure!(
        dec!(-0.10) <= shift && shift <= dec!(0.10),
        "closure training price shift {shift} exceeds the governed ten-cent fixture band"
    );
    Ok(shift)
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
    let (scope, ordinal) = closure_market_identity(&source.market_id)?;
    let yes_token = TokenId::new(closure_token(scope, ordinal));
    let no_token = closure_no_token(scope, ordinal);
    let (yes_bids, yes_asks) = closure_levels(scope, true, price_shift, ordinal)?;
    let yes_entry_bid = yes_bids[0].price_decimal().inner();
    let yes_entry_ask = yes_asks[0].price_decimal().inner();
    let (no_bids, _) = closure_levels(scope, false, price_shift, ordinal)?;
    let no_entry_bid = no_bids[0].price_decimal().inner();
    let cutoff = DecisionClock::new(knowledge_lag_secs)
        .boundary(decision_at)?
        .cutoff_for(DecisionSource::Microstructure);
    let mut rows = Vec::with_capacity(65);
    for minutes_ago in (0_i64..=60).rev() {
        let variation = closure_momentum_variation(ordinal, minutes_ago)?;
        rows.push(closure_microstructure_row(
            yes_token.clone(),
            source.market_id.clone(),
            cutoff - Duration::minutes(minutes_ago),
            yes_entry_bid + variation,
            yes_entry_ask + variation,
        )?);
    }
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

fn closure_trade_tape_rows(
    source: &ClosureMarketSource,
    decision_at: DateTime<Utc>,
    knowledge_lag_secs: u64,
    price_shift: Decimal,
) -> Result<Vec<TradeTapeRow>> {
    const PARTICIPANTS_PER_ROLE: usize = 20;
    const OBSERVED_FLAGS: u16 = TRADE_COVERAGE_TRADE_ID
        | TRADE_COVERAGE_MARKET_ID
        | TRADE_COVERAGE_TOKEN_ID
        | TRADE_COVERAGE_PARTICIPANT_ADDRESS
        | TRADE_COVERAGE_PARTICIPANT_ROLE
        | TRADE_COVERAGE_SIDE
        | TRADE_COVERAGE_TX_HASH
        | TRADE_COVERAGE_PRICE
        | TRADE_COVERAGE_SIZE
        | TRADE_COVERAGE_FEE_RATE;

    let (scope, ordinal) = closure_market_identity(&source.market_id)?;
    let token_id = TokenId::new(closure_token(scope, ordinal));
    let (bids, asks) = closure_levels(scope, true, price_shift, ordinal)?;
    let price = Price::new(
        (bids[0].price_decimal().inner() + asks[0].price_decimal().inner()) / Decimal::from(2),
    );
    let cutoff = DecisionClock::new(knowledge_lag_secs)
        .boundary(decision_at)?
        .cutoff_for(DecisionSource::TradeTape);
    let participant_count = PARTICIPANTS_PER_ROLE * 2;
    (0..participant_count)
        .map(|index| {
            let offset_secs = i64::try_from(participant_count - index)?;
            let event_at = cutoff - Duration::seconds(offset_secs);
            let ingestion_at = event_at + Duration::seconds(1);
            let role = if index < PARTICIPANTS_PER_ROLE {
                ChTradeParticipantRole::Maker
            } else {
                ChTradeParticipantRole::Taker
            };
            let participant_seed = ordinal
                .checked_mul(100)
                .and_then(|value| value.checked_add(index))
                .context("closure trade participant identity overflowed")?;
            let shares = Shares::new(Decimal::from(24 + (index % 5)));
            let notional = shares * price;
            let source_event_id = format!(
                "feedback-closure:{scope}:{ordinal}:{}:{index}:on-chain-fill",
                decision_at.timestamp_millis()
            );
            let tx_seed = seeded_uuid(&source_event_id).as_u128();
            Ok(TradeTapeRow {
                market_id: source.market_id.clone(),
                token_id: token_id.clone(),
                event_time: event_at.timestamp_millis(),
                ingestion_time: ingestion_at.timestamp_millis(),
                stream_session_id: None,
                token_sequence: None,
                participant_address: format!("0x{participant_seed:040x}"),
                participant_role: role,
                side: if index.is_multiple_of(2) {
                    ChTradeSide::Buy
                } else {
                    ChTradeSide::Sell
                },
                price: ChPrice::from(price),
                size_shares: ChShares::from(shares),
                notional_usd: ChUsd::from(notional),
                tx_hash: Some(format!("0x{tx_seed:064x}")),
                source_event_id: source_event_id.clone(),
                source: ChTradeTapeSource::OnChainOrderFilled,
                observed_field_flags: OBSERVED_FLAGS,
                fee_rate_bps: Some(ChBps::from(Bps::ZERO)),
                reconciliation_status: ChTradeReconciliationStatus::Matched,
                matched_source_event_id: Some(format!("market-ws:{source_event_id}")),
                revision: 1,
                reconciled_at: Some(ingestion_at.timestamp_millis()),
                raw_payload_json: Some(
                    serde_json::json!({
                        "market_id": source.market_id,
                        "token_id": token_id,
                        "participant": participant_seed,
                        "role": if role == ChTradeParticipantRole::Maker { "maker" } else { "taker" },
                    })
                    .to_string(),
                ),
                schema_version: TradeTapeRow::SCHEMA_VERSION,
            })
        })
        .collect()
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
    let (scope, ordinal) = closure_market_identity(market_id)?;
    let yes_token_id = TokenId::new(closure_token(scope, ordinal));
    let no_token_id = closure_no_token(scope, ordinal);
    let yes_wins = closure_yes_wins(ordinal)?;
    let observed_at = resolved_at + Duration::minutes(1);
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
    let (_, market_ordinal) = closure_market_identity(&recommendation.market_id)?;
    let expected_won = recommendation_won(
        recommendation.outcome_side,
        closure_yes_wins(market_ordinal)?,
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

const CLOSURE_REGIME_DOMAIN: u64 = 0xa5a5_d3c4_e5f6_0718;
const CLOSURE_STRENGTH_DOMAIN: u64 = 0x6c8e_9cf5_7093_2bd1;
const CLOSURE_LABEL_NOISE_DOMAIN: u64 = 0xd1b5_4a32_d192_ed03;
const CLOSURE_PRICE_DOMAIN: u64 = 0x94d0_49bb_1331_11eb;
const CLOSURE_SPREAD_DOMAIN: u64 = 0x3f84_6b17_c2d9_5ea1;

fn closure_latent_word(market_ordinal: usize, domain: u64) -> Result<u64> {
    let ordinal = u64::try_from(market_ordinal)
        .context("closure market ordinal exceeds deterministic latent identity")?;
    ensure!(ordinal > 0, "closure market ordinal must be positive");
    // SplitMix64's finalizer is used only as a deterministic fixture PRF. Each
    // latent variable has a separate domain so price, pre-decision signal,
    // signal strength, and terminal label noise do not share a shortcut.
    let mut value = (ordinal ^ domain).wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    Ok(value ^ (value >> 31))
}

fn closure_reversion_strength(market_ordinal: usize) -> Result<usize> {
    let bucket = closure_latent_word(market_ordinal, CLOSURE_STRENGTH_DOMAIN)? % 4;
    usize::try_from(bucket + 1).context("closure reversion strength exceeds usize")
}

fn closure_price_tier(market_ordinal: usize) -> Result<(usize, bool)> {
    let word = closure_latent_word(market_ordinal, CLOSURE_PRICE_DOMAIN)?;
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
        "evaluation" | "shadow" => market_ordinal
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
    let (tier, positive) = closure_price_tier(price_identity)?;
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
        "evaluation" | "shadow" => {
            let zero_based = market_ordinal
                .checked_sub(1)
                .context("closure spread ordinal underflowed")?;
            let cohort_index = zero_based.div_euclid(EVALUATION_MARKETS_PER_TICK);
            let slot = zero_based % EVALUATION_MARKETS_PER_TICK;
            let cohort_identity = cohort_index
                .checked_add(1)
                .context("closure spread cohort identity overflowed")?;
            let word = closure_latent_word(cohort_identity, CLOSURE_SPREAD_DOMAIN)?;
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

fn closure_momentum_variation(market_ordinal: usize, minutes_ago: i64) -> Result<Decimal> {
    ensure!(
        (0..=60).contains(&minutes_ago),
        "closure momentum minute must be within [0, 60]"
    );
    let strength = i64::try_from(closure_reversion_strength(market_ordinal)?)
        .context("closure signal strength exceeds i64")?;
    let sign = closure_regime_sign(market_ordinal)?;
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

fn closure_yes_wins(market_ordinal: usize) -> Result<bool> {
    // Signal-dependent irreducible noise makes stronger pre-decision excursions
    // more reliable without making any label deterministic. The independent
    // noise stream is absent from every feature and executable-price input.
    const ERROR_BPS: [u64; 4] = [1_800, 1_100, 650, 350];
    let regime_yes = closure_regime_sign(market_ordinal)? > 0;
    let strength = closure_reversion_strength(market_ordinal)?;
    let draw = closure_latent_word(market_ordinal, CLOSURE_LABEL_NOISE_DOMAIN)? % 10_000;
    Ok(if draw < ERROR_BPS[strength - 1] {
        !regime_yes
    } else {
        regime_yes
    })
}

fn closure_regime_sign(market_ordinal: usize) -> Result<i64> {
    Ok(
        if closure_latent_word(market_ordinal, CLOSURE_REGIME_DOMAIN)? & 1 == 1 {
            1
        } else {
            -1
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
            deadline_secs: CLOSURE_COMPUTE_DEADLINE_SECS,
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
) -> Result<()> {
    let binding = await_shadow_binding(db, fixture.feedback_cycle_id).await?;
    let policy = PgPolicyRepository::new(db.clone())
        .load_current()
        .await?
        .context("closure shadow binding has no current policy snapshot")?;
    let runner = build_model_runner(db, artifact_store).await;
    let schema = Arc::new(FeatureSchema::build(
        &policy.snapshot.profile_artifacts.features.definition,
    )?);
    let first_decision_at = binding.bound_at + Duration::milliseconds(1);
    await_database_time(db, first_decision_at + Duration::seconds(1)).await?;
    let observation_cohorts = prepare_shadow_cohorts(db, fixture, first_decision_at).await?;
    let requirements = runner
        .active_requirements(ActiveModelRequirementsRequest {
            policy: &policy,
            decision_at: first_decision_at,
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
    let observation_cohorts = Arc::new(observation_cohorts);
    let fact_writers = Arc::clone(&fixture.fact_writers);
    let replay = Arc::clone(&fixture.replay);
    let results = stream::iter(0..SHADOW_OBSERVATION_COUNT)
        .map(|ordinal| {
            let db = db.clone();
            let runner = Arc::clone(&runner);
            let serving = requirements.serving.clone();
            let schema = Arc::clone(&schema);
            let cohorts = Arc::clone(&observation_cohorts);
            let fact_writers = Arc::clone(&fact_writers);
            let replay = Arc::clone(&replay);
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
    ensure!(
        observed.sample_count == u64::try_from(SHADOW_OBSERVATION_COUNT)?,
        "closure shadow persisted {} in-window comparisons, expected {SHADOW_OBSERVATION_COUNT}",
        observed.sample_count
    );
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

fn shadow_decision_at(first: DateTime<Utc>, ordinal: usize) -> Result<DateTime<Utc>> {
    let offset_millis = i64::try_from(ordinal).context("shadow ordinal exceeds i64")?;
    first
        .checked_add_signed(Duration::milliseconds(offset_millis))
        .context("shadow decision time overflowed")
}

async fn prepare_shadow_cohorts(
    db: &DatabaseConnection,
    fixture: &FeedbackClosureFixture,
    decision_at: DateTime<Utc>,
) -> Result<Vec<ShadowObservationCohort>> {
    ensure!(
        fixture.observation_price_shifts.len() == EVALUATION_OBSERVATION_COUNT
            && fixture.observation_price_shifts.len().is_multiple_of(2),
        "closure shadow requires the complete paired evaluation price sequence"
    );
    let cohort_count = fixture.observation_price_shifts.len().div_euclid(2);
    ensure!(cohort_count > 0, "closure has no shadow price cohorts");
    let market_created_at = decision_at - Duration::days(1);
    let resolves_at = decision_at + Duration::days(30);
    let mut cohorts = Vec::with_capacity(cohort_count);
    for cohort_index in 0..cohort_count {
        let price_shift = shadow_price_shift(&fixture.observation_price_shifts, cohort_index)?;
        let (first_ordinal, last_ordinal) = shadow_market_range(cohort_index)?;
        let event_id = format!("feedback-closure-shadow-event-{cohort_index}");
        let first_market_id = format!("feedback-closure-shadow-market-{first_ordinal}");
        seed_report_catalog(
            db,
            &event_id,
            &first_market_id,
            &format!("Will feedback closure shadow sample {first_ordinal} resolve Yes?"),
            &first_market_id,
            &closure_token("shadow", first_ordinal),
            &closure_no_token("shadow", first_ordinal),
        )
        .await;
        seed_catalog_clones(db, "shadow", first_ordinal, last_ordinal, &first_market_id).await?;
        let resolutions = (first_ordinal..=last_ordinal)
            .map(|ordinal| (ordinal, resolves_at))
            .collect::<BTreeMap<_, _>>();
        let catalog = Arc::new(ClosureCatalogFacts::build(ClosureCatalogBuild {
            scope: "shadow",
            event_id: &event_id,
            decision_at,
            market_created_at,
            resolutions: &resolutions,
            first_ordinal,
            last_ordinal,
            price_shift,
        })?);
        catalog
            .persist(db, fixture.capability_registry_hash)
            .await?;
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
    Ok(cohorts)
}

async fn await_shadow_binding(
    db: &DatabaseConnection,
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
                Some((stage, job_id)) => PgResearchJobRepository::new(db.clone())
                    .find_by_id(&job_id)
                    .await?
                    .map_or_else(
                        || format!("stage={stage} job_id={job_id} missing"),
                        |job| {
                            format!(
                                "stage={stage} job_id={job_id} kind={} status={:?} error={:?}",
                                job.kind, job.status, job.error_json
                            )
                        },
                    ),
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

impl ShadowObservationRequest<'_> {
    async fn persist_sources(&self) -> Result<Vec<ReplaySample>> {
        let knowledge_lag_secs = self.replay.knowledge_lag.as_secs();
        let mut book_rows = Vec::with_capacity(self.sources.len() * 2);
        let mut microstructure_rows = Vec::with_capacity(self.sources.len() * 65);
        let mut session_rows = Vec::with_capacity(self.sources.len() * 2);
        let mut trade_tape_rows = Vec::with_capacity(self.sources.len() * 40);
        let mut market_infos = Vec::with_capacity(self.sources.len());
        for source in self.sources {
            let (_, ordinal) = closure_market_identity(&source.market_id)?;
            let facts = closure_book_facts(
                source,
                self.decision_at,
                knowledge_lag_secs,
                self.book_price_shift,
            )?;
            book_rows.extend(facts.ledger_rows);
            session_rows.extend(facts.session_rows);
            market_infos.push(facts.market_info);
            microstructure_rows.extend(closure_microstructure_rows(
                source,
                self.decision_at,
                knowledge_lag_secs,
                closure_yes_wins(ordinal)?,
                self.book_price_shift,
            )?);
            trade_tape_rows.extend(closure_trade_tape_rows(
                source,
                self.decision_at,
                knowledge_lag_secs,
                self.book_price_shift,
            )?);
        }
        let market_info_repository = PgClobMarketInfoRepository::new(self.db.clone());
        for market_info in market_infos {
            market_info_repository
                .insert_observation(market_info)
                .await?;
        }
        self.facts
            .commit_sources(CohortSourceFacts {
                books: book_rows,
                microstructure: microstructure_rows,
                sessions: session_rows,
                trade_tape: trade_tape_rows,
            })
            .await?;
        self.sources
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
            emitted,
            topn_decision_overlap: comparison.topn_decision_overlap,
            hard_divergence: comparison.hard_divergence,
        })
    }
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
            max_horizon_secs: 0,
            domain: replay.config.domain.clone(),
        })
        .await?;
    let cross = materialize_cross_section(
        &replay.builder,
        ReplayFactorMode::FeatureOnly,
        &replay.config,
        &CrossSectionRequest {
            pit: &window.pit,
            prefetched: &window.prefetched,
            decision_at: request.decision_at,
            group: &samples,
            category_scope: None,
            knowledge_lag: replay.knowledge_lag,
            lookback: replay.lookback,
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
        ensure!(
            capture.snapshot.catalog == request.catalog.decision_ref(&vector.market_id)?,
            "closure shadow capture changed the frozen catalog binding for {}",
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
    let evidence = feature_commitment(&rows)?.bind_model_vectors(&vector_ids)?;
    request.facts.commit_shadow_features(rows).await?;
    let outcome = request
        .runner
        .run(ModelRunRequest {
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
) -> Result<()> {
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
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "closure cycle {feedback_cycle_id} did not reach CandidateReady within {CANDIDATE_READY_TIMEOUT:?}"
        );
        sleep(CLOSURE_POLL_INTERVAL).await;
    }
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
        observation_count > 0 && observation_count.is_multiple_of(2),
        "closure paired market range requires an even cross-section"
    );
    let first_ordinal = group_index
        .div_euclid(2)
        .checked_mul(observation_count)
        .and_then(|offset| offset.checked_add(1))
        .context("closure paired market ordinal overflowed")?;
    let last_ordinal = first_ordinal
        .checked_add(observation_count - 1)
        .context("closure paired market range overflowed")?;
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
        _ => 730_000,
    };
    (base + ordinal).to_string()
}

fn closure_no_token(scope: &str, ordinal: usize) -> TokenId {
    let base = match scope {
        "training" => 810_000,
        "calibration" => 820_000,
        "shadow" => 840_000,
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet};

    use anyhow::Result;
    use chrono::{DateTime, Duration, Utc};
    use quant_pivot_models::{
        enums::quant::OutcomeSide,
        types::{MarketId, PayoutRatio, TokenId},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        CALIBRATION_GROUP_COUNT, CALIBRATION_OBSERVATION_COUNT, ClosureBookMetrics,
        EVALUATION_MARKETS_PER_TICK, EVALUATION_OBSERVATION_COUNT, Price, SHADOW_OBSERVATION_COUNT,
        calibration_market_range, closure_levels, closure_market_offset,
        closure_momentum_variation, closure_price_tier, closure_regime_sign,
        closure_resolution_fact, closure_reversion_strength, closure_spread_width,
        closure_yes_wins, evaluation_book_price_shift, evaluation_decision_points,
        evaluation_market_range, recommendation_won, shadow_decision_at, shadow_market_range,
        shadow_price_shift, training_book_price_shift, training_market_range,
    };

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
    fn training_prices_vary() -> Result<()> {
        let shifts = (0..8)
            .map(training_book_price_shift)
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            shifts,
            vec![
                Decimal::ZERO,
                Decimal::ZERO,
                dec!(0.01),
                dec!(0.01),
                dec!(-0.01),
                dec!(-0.01),
                dec!(0.02),
                dec!(0.02),
            ]
        );
        assert_eq!(shifts.iter().copied().collect::<HashSet<_>>().len(), 4);
        Ok(())
    }

    #[test]
    fn training_turnover_bounded() -> Result<()> {
        let ranges = (0..8)
            .map(|group| training_market_range(group, 64))
            .collect::<Result<Vec<_>>>()?;
        let anchors = ranges.iter().map(|(first, _)| *first).collect::<Vec<_>>();
        let transitions = anchors.windows(2).filter(|pair| pair[0] != pair[1]).count();

        assert_eq!(anchors, vec![1, 1, 65, 65, 129, 129, 193, 193]);
        assert!(ranges.chunks_exact(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(transitions, 3);
        assert!(Decimal::from(i64::try_from(transitions)?) / dec!(7) <= dec!(0.5));
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
            let shift = training_book_price_shift(group_index)?;
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
            .map(|ordinal| closure_momentum_variation(ordinal, 5))
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
            counts[1] += usize::from(closure_regime_sign(ordinal)? > 0);
            counts[2] += usize::from(closure_yes_wins(ordinal)?);
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
                regime_agreements +=
                    usize::from(offset.is_sign_positive() == (closure_regime_sign(ordinal)? > 0));
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
        assert_eq!(fact.payout_for(&TokenId::new("730007"))?, PayoutRatio::ONE);
        assert_eq!(fact.payout_for(&TokenId::new("830007"))?, PayoutRatio::ZERO);
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
            let regime = closure_regime_sign(ordinal)? > 0;
            let stable = !ordinal.is_multiple_of(2);
            let (price_tier, positive_price) = closure_price_tier(ordinal)?;
            let strength = closure_reversion_strength(ordinal)?;
            regime_yes += usize::from(regime);
            stable_agreement += usize::from(regime == stable);
            price_positive += usize::from(positive_price);
            regime_price_agreement += usize::from(regime == positive_price);
            price_tiers[price_tier - 1] += 1;
            strengths[strength - 1] += 1;
        }

        assert_eq!(regime_yes, 2_117);
        assert_eq!(stable_agreement, 2_043);
        assert_eq!(price_positive, 2_015);
        assert_eq!(regime_price_agreement, 2_066);
        assert_eq!(price_tiers, [1_033, 1_015, 1_001, 1_047]);
        assert_eq!(strengths, [974, 1_014, 1_006, 1_102]);
        Ok(())
    }

    #[test]
    fn history_encodes_noisy_reversion() -> Result<()> {
        let mut signal_errors = 0;
        for ordinal in 1..=32 {
            let yes_wins = closure_yes_wins(ordinal)?;
            let regime_yes = closure_regime_sign(ordinal)? > 0;
            let at_fifteen = closure_momentum_variation(ordinal, 15)?;
            let at_five = closure_momentum_variation(ordinal, 5)?;
            let at_entry = closure_momentum_variation(ordinal, 0)?;
            let lag_skipped_move = at_five - at_fifteen;
            let recent_reversal = at_entry - at_five;
            let window_mean = (0..=15)
                .map(|minutes_ago| closure_momentum_variation(ordinal, minutes_ago))
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
        assert_eq!(signal_errors, 1);
        Ok(())
    }

    #[test]
    fn calibration_has_both_outcomes() -> Result<()> {
        let outcomes = (1..=CALIBRATION_OBSERVATION_COUNT)
            .map(closure_yes_wins)
            .collect::<Result<Vec<_>>>()?;
        let mut strength_counts = [0_usize; 4];
        let mut loss_counts = [0_usize; 4];
        let mut selected_side_wins = Vec::with_capacity(CALIBRATION_OBSERVATION_COUNT);
        for ordinal in 1..=CALIBRATION_OBSERVATION_COUNT {
            let strength = closure_reversion_strength(ordinal)?;
            let won = closure_yes_wins(ordinal)? == (closure_regime_sign(ordinal)? > 0);
            strength_counts[strength - 1] += 1;
            loss_counts[strength - 1] += usize::from(!won);
            selected_side_wins.push(won);
        }

        let yes_count = outcomes.iter().filter(|&&yes| yes).count();
        let win_count = selected_side_wins.iter().filter(|&&won| won).count();
        assert!(yes_count > 0 && yes_count < outcomes.len());
        assert!(win_count > 0 && win_count < selected_side_wins.len());
        assert_eq!(yes_count, 513);
        assert_eq!(strength_counts, [239, 272, 246, 267]);
        assert_eq!(loss_counts, [40, 37, 11, 9]);
        assert_eq!(selected_side_wins.len() - win_count, 97);
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
            let regime_yes = closure_regime_sign(ordinal)? > 0;
            let strength = closure_reversion_strength(ordinal)?;
            let yes_wins = closure_yes_wins(ordinal)?;
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
        assert!(nuisance_return < dec!(0.02));
        assert!(signal_return - nuisance_return > dec!(0.80));
        Ok(())
    }
}
