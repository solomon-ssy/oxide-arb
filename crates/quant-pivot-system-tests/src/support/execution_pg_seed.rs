//! `PostgreSQL` seed helpers owned by execution-ledger system tests.
//!
//! Shared fixture chain extracted from `pg_execution_submission` so attribution,
//! submission, and capital tests can drive the same money-critical ledger paths.

use std::{collections::BTreeMap, str::FromStr, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::governance::model_score_content_hash;
use quant_pivot_models::{
    domain::{
        governance::{NewOperationLog, UpsertKillSwitchState},
        market::fee::BuilderFeeAttribution,
        quant::{
            ApproveOrderIntent, CalibrationArtifactPayload, CapitalSettlement, EntryConditionClaim,
            ExitLedgerWrite, NewAccountSnapshot, NewCalibrationArtifact, NewCapitalAllocation,
            NewEntryConditionArtifact, NewEntryConditionInstance, NewEquitySnapshot,
            NewExecutionOrder, NewFeatureParityState, NewMarketSelection, NewModelRun,
            NewModelVersion, NewOrderIntent, NewPortfolioPlan, NewRecommendation,
            NewRecommendationReport, NewReconciliation, NewReportDataQualitySnapshot,
            NewReportTransaction, NewTradePolicyArtifact, NewTradePolicyGovernanceAudit,
            PositionExit, PositionFill, SubmissionLedgerWrite,
        },
        query::TimeWindow,
    },
    entities::{
        quant_feature_parity_state::Entity as QuantFeatureParityStateEntity,
        quant_model_run::{Column as QuantModelRunColumn, Entity as QuantModelRunEntity},
        quant_model_spec::{Column, Entity},
        quant_model_version::{
            Column as QuantModelVersionColumn, Entity as QuantModelVersionEntity,
        },
    },
    enums::{
        common::{MarketCategory, OrderType, Side, TickSize::Hundredth},
        execution::{
            CapitalAllocationState, ExecutionOrderPhase, ExitReason, ExitState, KillSwitchState,
            OrderIntentKind, OrderTypeKind, ReconciliationEvidenceKind, ReconciliationResult,
            VenueOrderStatus,
        },
        factor::{FactorFamily, FactorValueState, NormalizationSource},
        market::MarketStatus,
        model::ModelFamily,
        operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
        quant::{
            AccountSource, ApprovalStatus, BindingConstraint, CalibrationKind, DownsideSource,
            EmptyReportReason, EntryConditionState, ExecutionOrderState, ExitSettlementMode,
            FactorDirection, FeatureParityLatchState, FeatureParityStateTransition,
            FillRequirement, ModelRunKind, ModelRunStatus, OrderIntentStatus, OutcomeSide,
            PriceComparison, PublicationStatus, QuantRuntimeMode, RecommendationReportStatus,
            RecommendationStatus, RedeemPolicy, ReportKind, SizingModelKind,
            TradePolicyGovernanceAction, TradePolicyStatus,
        },
        rbac::ResourceType,
    },
    hashing::CanonicalDigest,
    runtime_config::{DecisionPolicySnapshot, FactorCrossSectionConfig},
    types::{
        AccountPositions, AccountSnapshotId, ArtifactUri, BookSnapshotRef, Bps,
        CalibrationArtifactId, CapitalAllocationId, ConditionTruth, ConfidenceSummary,
        ConfirmationPolicy, ContentHash, DataQualitySummary, DecisionPolicySnapshotId,
        ENTRY_CONDITION_EVALUATOR_VERSION, ENTRY_CONDITION_SCHEMA_VERSION, EligibilitySummary,
        EntryConditionArtifactId, EntryConditionArtifactV1, EntryConditionBinding,
        EntryConditionFoldState, EntryConditionInstanceId, EntryConditionPlan,
        EntryConditionTemplate, EntryConditionV1, EntryOrderPolicy, EntryOrderSpec,
        EntryOrderTemplate, EntryPlan, EquitySnapshotId, EventId, EvidenceRefs,
        ExecutablePriceBasis, ExecutionEligibility, ExecutionOrderId, ExitExecutionTemplate,
        ExitPlan, ExitPolicySpec, ExposureBreakdown, FactorBreakdownEntry, FeatureParityStateId,
        FeatureVectorId, MarketContext, MarketId, MarketSelectionId, ModelInputContract,
        ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId, OperationDetailDocument,
        OperationLogId, OpportunisticExitPolicy, OrderAmount, OrderId, OrderIntentId,
        PortfolioConstraintsSnapshot, PortfolioOptimizerMeta, PortfolioPlanId,
        PortfolioRejectedSummary, PortfolioRiskBudget, PositionSnapshot, PreparedFeeSchedule,
        PreparedVenueOrder, Price, PriceCondition, Probability, RecommendationFactorBreakdown,
        RecommendationId, RecommendationIdentity, RecommendationReportId, RecommendationTradePlan,
        ReconciliationEvidence, ReconciliationEvidenceChain, ReconciliationId,
        ReportDataQualitySnapshotId, ReportDataQualityTokens, ReportSummary,
        ResearchEvaluationTrack, ResearchJobId, ResearchProfileRef, ResearchReadinessEvidenceId,
        ResidualSharePolicy, RiskEnvelope, RoleCode, SelectionExclusionSummary, Shares,
        SignalCandidateId, SizingPlan, SourceSliceManifestRef, StructuralVolatilityOosEvidence,
        TRADE_POLICY_ARTIFACT_FORMAT_VERSION, ThesisInvalidationPolicy, TokenId,
        TradePolicyArtifactId, TradePolicyArtifactPayload, TradePolicyCandidateSpec,
        TradePolicyCohort, TradePolicyCohortDimension, TradePolicyCohortKey,
        TradePolicyCohortProvenance, TradePolicyEvidenceBundleRef, TradePolicyExecutionEvidence,
        TradePolicyExitTemplate, TradePolicyFitContract, TradePolicyGovernanceAuditId,
        TradePolicyParameterSource, TradePolicyPitCutoffEvidence, TradePolicyValidationEvidence,
        TrainingDatasetId, Usd, UserId, VenueOrderAmount, VerticalActivationTarget,
        VerticalGateEvidence, VerticalGateKind, builtin_research_profiles,
        calibration::{
            IsotonicKnot, ModelScoreCalibrationPayload, MonotoneMapping, ReliabilityBin,
            ReliabilityReport,
        },
        model_metrics::ModelVersionMetrics,
        model_training::ModelTrainingObjective,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCalibrationArtifactRepository, PgEntryConditionRepository, PgEventRepository,
        PgExecutionSubmissionRepository, PgFeatureParityRepository, PgKillSwitchStateRepository,
        PgMarketRepository, PgMarketSelectionRepository, PgModelRegistryRepository,
        PgModelRunRepository, PgOrderIntentRepository, PgPolicyRepository, PgTradePolicyRepository,
    },
    traits::{
        CalibrationArtifactRepository, EntryConditionRepository, EventRepository,
        ExecutionSubmissionRepository, FeatureParityRepository, KillSwitchStateRepository,
        MarketRepository, MarketSelectionRepository, ModelRegistryRepository, ModelRunRepository,
        OrderIntentRepository, PolicyRepository, TradePolicyRepository,
    },
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    factors::{FrozenReferenceQuantiles, names::LIQUIDITY_DEPTH},
    hashing::ResearchHasher,
    model::{
        CalibratedReturnModel, FactorWeight, ModelArtifact, ModelArtifactHeader, ReturnModelSpec,
        ScoreMultiplierSpec, SubstitutionConfidenceRules, WeightedFactorModelArtifact,
        model_input_contract_hash,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
};
use uuid::Uuid;

use super::{
    catalog_fixtures::{make_event, make_market},
    model_spec_fixtures::new_model_spec_fixture,
    policy_fixtures::bootstrap_policy_bundle,
    report_fixtures,
    report_lifecycle_seed::persist_and_publish_report,
    seeded_uuid,
};

/// shares (100) * `limit_price` (0.6).
/// Weather `SemiAuto` fixtures use the profile's only governed total cash-budget tier.
pub const EXECUTION_NOTIONAL: Decimal = dec!(25);

/// Explicitly close the fail-closed bootstrap kill switch for integration tests
/// that exercise risk-increasing entry paths.
pub async fn enable_entry_admission_for_test(db: &DatabaseConnection, actor: &str) {
    let state = PgKillSwitchStateRepository::new(db.clone())
        .upsert(UpsertKillSwitchState {
            id: 1,
            state: KillSwitchState::Closed,
            changed_by: actor.to_owned(),
            reason: "explicitly enable risk-increasing integration test".to_owned(),
            requires_operator_ack: false,
            changed_at: Utc::now(),
        })
        .await
        .expect("explicitly close kill switch for risk-increasing test");
    assert_eq!(state.state, KillSwitchState::Closed);
}

/// Claim the fixture's current condition and intent through the same atomic
/// transaction used by the production dispatcher.
pub async fn claim_entry_for_test(
    db: &DatabaseConnection,
    submission: &PgExecutionSubmissionRepository,
    intent_id: &OrderIntentId,
) {
    let claim = entry_claim_for_test(db, intent_id).await;
    submission
        .claim_for_submission(claim)
        .await
        .expect("atomic intent/condition claim");
}

/// Build the exact claim payload for concurrency and conflict tests.
pub async fn entry_claim_for_test(
    db: &DatabaseConnection,
    intent_id: &OrderIntentId,
) -> EntryConditionClaim {
    let intent = PgOrderIntentRepository::new(db.clone())
        .find_by_id(intent_id)
        .await
        .expect("load intent for claim")
        .expect("seeded intent");
    let condition = PgEntryConditionRepository::new(db.clone())
        .find_instance(&intent.condition_instance_id)
        .await
        .expect("load condition for claim")
        .expect("seeded condition");
    let admission_state_version =
        ResearchHasher::canonical(&("test-admission-state-v1", intent_id, condition.revision))
            .expect("admission state hash");
    EntryConditionClaim {
        condition_instance_id: condition.condition_instance_id,
        order_intent_id: intent.order_intent_id,
        artifact_id: condition.artifact_id,
        artifact_hash: condition.artifact_hash,
        expected_revision: condition.revision,
        evaluation_hash: condition.evaluation_hash,
        input_fingerprint: condition.input_fingerprint,
        continuity_hash: condition.continuity_hash,
        admission_state_version,
        claimed_at: Utc::now(),
    }
}

/// Stable ids produced by [`seed_report_fixture`] / [`seed_report_on_infra`].
pub struct ExecutionTxnIds {
    pub decision_at: DateTime<Utc>,
    pub feature_parity_state_id: FeatureParityStateId,
    pub account_snapshot: AccountSnapshotId,
    pub data_quality_snapshot: ReportDataQualitySnapshotId,
    pub portfolio_plan: PortfolioPlanId,
    pub report: RecommendationReportId,
    pub recommendation: RecommendationId,
    pub condition_instance: EntryConditionInstanceId,
    pub model_version: ModelVersionId,
    pub model_run: ModelRunId,
    pub market_selection: MarketSelectionId,
    pub decision_policy_snapshot: DecisionPolicySnapshotId,
    pub trade_policy: TradePolicyCohortProvenance,
    pub market: String,
    pub event: String,
    pub token: String,
}

/// Shared model/runtime lineage for multiple demo reports (one model spec).
pub struct SharedDemoInfra {
    pub feature_parity_state_id: FeatureParityStateId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: ModelRunId,
    pub trade_policy: TradePolicyCohortProvenance,
}

/// Catalog + trigger identity for a single published report fixture.
pub struct ReportSeedConfig {
    pub event_id: String,
    pub market_id: String,
    pub market_question: String,
    pub market_slug: String,
    pub token_id: String,
    pub trigger_key: String,
}

/// Overrides when composing a [`NewReportTransaction`] for UI demo fixtures.
pub struct ReportBuildOptions {
    pub recommendations: Vec<NewRecommendation>,
    pub entry_condition_artifacts: Vec<NewEntryConditionArtifact>,
    pub summary: ReportSummary,
    pub as_of: DateTime<Utc>,
    pub runtime_mode: QuantRuntimeMode,
}

impl ReportBuildOptions {
    /// One published recommendation — the default execution-fixture shape.
    #[must_use]
    pub fn published_single(ids: &ExecutionTxnIds) -> Self {
        Self {
            recommendations: vec![demo_recommendation(
                ids.recommendation,
                ids.report,
                ids,
                1,
                &ids.market,
                &ids.event,
                &ids.token,
            )],
            entry_condition_artifacts: Vec::new(),
            summary: report_summary(),
            as_of: ids.decision_at,
            runtime_mode: QuantRuntimeMode::AutoExecution,
        }
    }

    /// Published report with zero recommendations and an explicit empty reason.
    #[must_use]
    pub fn empty_report() -> Self {
        Self {
            recommendations: Vec::new(),
            entry_condition_artifacts: Vec::new(),
            summary: empty_report_summary(),
            as_of: Utc::now(),
            runtime_mode: QuantRuntimeMode::AutoExecution,
        }
    }
}

/// Seed runtime config + model registry once; reuse for many reports.
pub async fn seed_shared_demo_infra(db: &DatabaseConnection) -> SharedDemoInfra {
    seed_shared_demo_infra_inner(db, None).await
}

/// Seed execution UI lineage with a loadable, calibrated model artifact.
pub async fn seed_shared_demo_infra_with_artifact_store(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
) -> SharedDemoInfra {
    seed_shared_demo_infra_inner(db, Some(artifact_store)).await
}

async fn seed_shared_demo_infra_inner(
    db: &DatabaseConnection,
    artifact_store: Option<&Arc<dyn ArtifactStore>>,
) -> SharedDemoInfra {
    let runtime_config_repo = PgPolicyRepository::new(db.clone());
    let decision_policy_snapshot_id = match runtime_config_repo
        .load_current()
        .await
        .expect("load active runtime config for demo seed")
    {
        Some(active) => active.decision_policy_snapshot_id,
        None => seed_runtime_config_named(db, "ui-demo-seed", "ui demo fixture").await,
    };

    if let Some(infra) =
        find_existing_demo_infra(db, &decision_policy_snapshot_id, artifact_store).await
    {
        return infra;
    }

    let (model_version_id, model_run_id, trade_policy) = seed_model_version_named(
        db,
        &decision_policy_snapshot_id,
        "ui-demo-seed-model",
        artifact_store,
    )
    .await;
    SharedDemoInfra {
        feature_parity_state_id: ensure_clear_feature_parity_state(db).await,
        decision_policy_snapshot_id,
        model_version_id,
        model_run_id,
        trade_policy,
    }
}

async fn find_existing_demo_infra(
    db: &DatabaseConnection,
    active_decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    artifact_store: Option<&Arc<dyn ArtifactStore>>,
) -> Option<SharedDemoInfra> {
    let spec = Entity::find()
        .filter(Column::Name.eq("ui-demo-seed-model"))
        .one(db)
        .await
        .ok()??;
    let version = QuantModelVersionEntity::find()
        .filter(QuantModelVersionColumn::ModelSpecId.eq(spec.model_spec_id))
        .order_by_desc(QuantModelVersionColumn::Version)
        .one(db)
        .await
        .ok()??;
    if let Some(store) = artifact_store {
        let key = ModelArtifact::artifact_key(&version.artifact_hash).ok()?;
        if !store.exists_by_key(&key).await.ok()? {
            return None;
        }
    }
    let run = QuantModelRunEntity::find()
        .filter(QuantModelRunColumn::ModelVersionId.eq(version.model_version_id))
        .filter(
            QuantModelRunColumn::DecisionPolicySnapshotId.eq(*active_decision_policy_snapshot_id),
        )
        .order_by_desc(QuantModelRunColumn::StartedAt)
        .one(db)
        .await
        .ok()??;
    let artifact_id = version.trade_policy_artifact_id?;
    let artifact_hash = version.trade_policy_hash?;
    let artifact = PgTradePolicyRepository::new(db.clone())
        .find(&artifact_id)
        .await
        .ok()??;
    let cohort_key = artifact.payload_json.cohorts.first()?.key.clone();
    Some(SharedDemoInfra {
        feature_parity_state_id: ensure_clear_feature_parity_state(db).await,
        decision_policy_snapshot_id: run.decision_policy_snapshot_id,
        model_version_id: version.model_version_id,
        model_run_id: run.model_run_id,
        trade_policy: TradePolicyCohortProvenance {
            artifact_id,
            artifact_hash,
            cohort_index: 0,
            cohort_key,
        },
    })
}

async fn ensure_clear_feature_parity_state(db: &DatabaseConnection) -> FeatureParityStateId {
    let repository = PgFeatureParityRepository::new(db.clone());
    if let Some(state) = repository
        .current_state()
        .await
        .expect("load feature parity state")
    {
        assert_eq!(
            state.state,
            FeatureParityLatchState::Clear,
            "execution fixture must not bypass an open parity latch"
        );
        return state.state_id;
    }

    let state_id = FeatureParityStateId::from_v7();
    QuantFeatureParityStateEntity::insert(
        NewFeatureParityState {
            state_id,
            state: FeatureParityLatchState::Clear,
            transition: FeatureParityStateTransition::GovernedAcknowledge,
            cause_run_id: None,
            recovery_run_id: None,
            previous_state_id: None,
            actor: Some("execution-test-fixture".to_owned()),
            acting_role: Some(RoleCode::new("risk_owner")),
            reason: "test fixture clear generation".to_owned(),
        }
        .into_active_model(),
    )
    .exec(db)
    .await
    .expect("seed feature parity clear generation");
    state_id
}

/// Seed catalog + published report on existing shared infra.
pub async fn seed_report_on_infra(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    config: ReportSeedConfig,
) -> ExecutionTxnIds {
    let ids = prepare_report_seed(db, infra, &config).await;
    persist_and_publish_report(db, build_report_transaction(&ids), &config.trigger_key, 10).await;
    ids
}

/// Seed a report whose recommendation waits for a continuously satisfied
/// executable-price condition. The artifact, recommendation reference, and
/// durable instance are committed by the report transaction.
pub async fn seed_conditional_price_report_on_infra(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    config: ReportSeedConfig,
) -> ExecutionTxnIds {
    seed_conditional_price_report_with_mode(db, infra, config, QuantRuntimeMode::AutoExecution)
        .await
}

/// Seed the same durable conditional evidence graph under `ReportOnly`, where
/// evaluation is active but intent creation and venue submission are forbidden.
pub async fn seed_report_only_conditional_price_report_on_infra(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    config: ReportSeedConfig,
) -> ExecutionTxnIds {
    seed_conditional_price_report_with_mode(db, infra, config, QuantRuntimeMode::ReportOnly).await
}

async fn seed_conditional_price_report_with_mode(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    config: ReportSeedConfig,
    runtime_mode: QuantRuntimeMode,
) -> ExecutionTxnIds {
    let ids = prepare_report_seed(db, infra, &config).await;
    let mut options = ReportBuildOptions::published_single(&ids);
    options.runtime_mode = runtime_mode;
    let artifact = price_condition_artifact(&ids);
    let recommendation = options
        .recommendations
        .first_mut()
        .expect("conditional report recommendation");
    let condition = EntryConditionPlan::Conditional {
        artifact_id: artifact.artifact_id,
        content_hash: artifact.content_hash,
    };
    match &mut recommendation.trade_plan {
        RecommendationTradePlan::Frozen { entry, .. } => {
            entry.condition = condition;
            "wait for executable ask at or below 0.62".clone_into(&mut entry.entry_reason);
        }
        RecommendationTradePlan::Unavailable { .. } => {
            panic!("conditional report recommendation must have a frozen trade plan")
        }
    }
    options.entry_condition_artifacts.push(artifact);
    persist_and_publish_report(
        db,
        build_report_transaction_inner(&ids, options),
        &config.trigger_key,
        10,
    )
    .await;
    ids
}

async fn prepare_report_seed(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    config: &ReportSeedConfig,
) -> ExecutionTxnIds {
    seed_market_catalog(
        db,
        &config.event_id,
        &config.market_id,
        &config.market_question,
        &config.market_slug,
    )
    .await;
    let market_selection_id =
        seed_market_selection(db, &infra.decision_policy_snapshot_id, &config.market_id).await;
    let decision_at = Utc::now();
    let model_run_id = seed_report_model_run(db, infra, &market_selection_id, decision_at).await;
    ExecutionTxnIds {
        decision_at,
        feature_parity_state_id: infra.feature_parity_state_id,
        account_snapshot: AccountSnapshotId::from_v7(),
        data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
        portfolio_plan: PortfolioPlanId::from_v7(),
        report: RecommendationReportId::from_v7(),
        recommendation: RecommendationId::from_v7(),
        condition_instance: EntryConditionInstanceId::from_v7(),
        model_version: infra.model_version_id,
        model_run: model_run_id,
        market_selection: market_selection_id,
        decision_policy_snapshot: infra.decision_policy_snapshot_id,
        trade_policy: infra.trade_policy.clone(),
        market: config.market_id.clone(),
        event: config.event_id.clone(),
        token: config.token_id.clone(),
    }
}

/// Seed the one inference run owned by a report fixture.
///
/// Production reports enforce a one-to-one `model_run_id` lineage. Shared demo
/// model metadata may be reused, but its inference run must never be reused by
/// another report.
pub async fn seed_report_model_run(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    market_selection_id: &MarketSelectionId,
    decision_at: DateTime<Utc>,
) -> ModelRunId {
    let model_run_id = ModelRunId::from_v7();
    let input_hash = ResearchHasher::canonical(&(
        "execution_report_fixture_model_input_v1",
        &model_run_id,
        &infra.model_version_id,
        market_selection_id,
    ))
    .expect("hash report fixture model input");
    let output_hash = ResearchHasher::canonical(&(
        "execution_report_fixture_model_output_v1",
        &model_run_id,
        market_selection_id,
    ))
    .expect("hash report fixture model output");
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::LiveInference,
            model_version_id: Some(infra.model_version_id),
            decision_policy_snapshot_id: infra.decision_policy_snapshot_id,
            market_selection_id: Some(*market_selection_id),
            window_start: decision_at,
            window_end: decision_at,
            status: ModelRunStatus::Succeeded,
            input_hash,
            output_hash: Some(output_hash),
            error_code: None,
            error_message: None,
            started_at: decision_at,
            finished_at: Some(decision_at),
        })
        .await
        .expect("report fixture model run");
    model_run_id
}

/// Compose a published report transaction with caller-controlled recommendations.
#[must_use]
pub fn build_custom_report_transaction(
    ids: &ExecutionTxnIds,
    options: ReportBuildOptions,
) -> NewReportTransaction {
    build_report_transaction_inner(ids, options)
}

/// Build one ranked recommendation row wired to shared demo infra refs.
#[must_use]
pub fn demo_recommendation(
    recommendation_id: RecommendationId,
    report_id: RecommendationReportId,
    ids: &ExecutionTxnIds,
    rank: i32,
    market_id: &str,
    event_id: &str,
    token_id: &str,
) -> NewRecommendation {
    NewRecommendation {
        recommendation_id,
        research_profile_artifact_id: fixture_profile_ref().artifact_id(),
        recommendation_report_id: report_id,
        rank,
        market_id: MarketId::new(market_id),
        event_id: EventId::new(event_id),
        token_id: TokenId::new(token_id),
        outcome_side: OutcomeSide::Yes,
        composite_score: Probability::new(dec!(0.7)),
        risk_adjusted_score: Probability::new(dec!(0.65)),
        confidence: Probability::new(dec!(0.72)),
        expected_return_bps: Bps::new(dec!(150)),
        downside_bps: Bps::new(dec!(80)),
        identity: recommendation_identity(),
        market_context: market_context(),
        rank_before_portfolio: rank,
        liquidity_score: Probability::new(dec!(0.8)),
        data_quality_score: Probability::new(dec!(0.9)),
        model_score_percentile: Probability::new(dec!(0.75)),
        trade_plan: trade_plan(&ids.trade_policy),
        factor_breakdown: factor_breakdown(),
        evidence_refs: evidence_refs(ids),
        execution_eligibility: execution_eligibility(),
        valid_from: Utc::now(),
        valid_until: Utc::now() + Duration::hours(1),
        status: RecommendationStatus::Prepared,
    }
}

/// Seed runtime config, catalog, model lineage, market selection, and a published report.
pub async fn seed_report_fixture(db: &DatabaseConnection) -> ExecutionTxnIds {
    let infra = seed_shared_demo_infra(db).await;
    seed_report_on_infra(
        db,
        &infra,
        ReportSeedConfig {
            event_id: "evt-1".to_owned(),
            market_id: "0xmarket".to_owned(),
            market_question: "Will it?".to_owned(),
            market_slug: "will-it".to_owned(),
            token_id: "token-1".to_owned(),
            trigger_key: format!("scheduled:test:{}", RecommendationReportId::from_v7()),
        },
    )
    .await
}

/// Create a semi-auto intent awaiting operator approval.
pub async fn seed_pending_intent(db: &DatabaseConnection, ids: &ExecutionTxnIds) -> OrderIntentId {
    let order_intent_id = OrderIntentId::from_v7();
    PgOrderIntentRepository::new(db.clone())
        .create_with_allocation(
            new_order_intent(
                order_intent_id,
                ids,
                OrderIntentStatus::PendingApproval,
                ApprovalStatus::Pending,
                QuantRuntimeMode::SemiAuto,
                None,
            ),
            new_capital_allocation(order_intent_id, ids),
            None,
        )
        .await
        .expect("create pending intent")
        .order_intent_id
}

/// Create an operator-approved intent (post-governance, pre-submission).
pub async fn seed_manual_approved_intent(
    db: &DatabaseConnection,
    ids: &ExecutionTxnIds,
) -> OrderIntentId {
    let intent_id = seed_pending_intent(db, ids).await;
    PgOrderIntentRepository::new(db.clone())
        .approve(
            &intent_id,
            ApproveOrderIntent {
                approved_by: seeded_uuid("ui-demo-operator").into(),
                approval_reason: "ui-demo-seed".to_owned(),
                approved_at: Utc::now(),
            },
            None,
            None,
            Utc::now(),
        )
        .await
        .expect("approve intent");
    intent_id
}

/// Create an auto-approved intent with capital allocation reserved.
pub async fn seed_approved_intent(db: &DatabaseConnection, ids: &ExecutionTxnIds) -> OrderIntentId {
    let order_intent_id = OrderIntentId::from_v7();
    PgOrderIntentRepository::new(db.clone())
        .create_with_allocation(
            new_order_intent(
                order_intent_id,
                ids,
                OrderIntentStatus::ApprovedByPolicy,
                ApprovalStatus::NotRequired,
                QuantRuntimeMode::AutoExecution,
                None,
            ),
            new_capital_allocation(order_intent_id, ids),
            None,
        )
        .await
        .expect("create approved intent")
        .order_intent_id
}

/// Drive an approved intent's entry to a confirmed full fill: capital `Spent`,
/// one open lot (100 @ 0.60), intent `Filled`.
pub async fn fill_entry_lot(
    db: &DatabaseConnection,
    submission: &PgExecutionSubmissionRepository,
    ids: &ExecutionTxnIds,
    intent_id: &OrderIntentId,
) {
    claim_entry_for_test(db, submission, intent_id).await;
    let order = submission
        .create_entry_order_and_lock_capital(
            new_execution_order(intent_id, ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create entry order");
    submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
                state: ExecutionOrderState::Filled,
                intent_status: OrderIntentStatus::Filled,
                venue_order_id: Some(OrderId::new("venue-entry")),
                venue_status: Some(VenueOrderStatus::Filled),
                submitted_at: Utc::now(),
                filled_at: Some(Utc::now()),
                cancelled_at: None,
                error_message: None,
                capital: CapitalSettlement::SettleFull {
                    spent_usd: Usd::new(EXECUTION_NOTIONAL),
                },
                fill: Some(position_fill(ids, intent_id)),
                reconciliation: Some(reconciliation_row(&order.execution_order_id, intent_id)),
            },
        )
        .await
        .expect("record entry fill");
}

/// Full exit flow: entry fill then exit fill at 0.55 (realized -5), position `Closed`.
/// When `peak_mark_price` is set, seeds it on the exit monitor after entry fill.
pub async fn close_position_full(
    db: &DatabaseConnection,
    submission: &PgExecutionSubmissionRepository,
    ids: &ExecutionTxnIds,
    intent_id: &OrderIntentId,
    peak_mark_price: Option<Price>,
) {
    fill_entry_lot(db, submission, ids, intent_id).await;

    if let Some(peak) = peak_mark_price {
        submission
            .touch_exit_monitor(intent_id, Utc::now(), Some(peak), None, None)
            .await
            .expect("seed peak mark price");
    }

    let exit = submission
        .create_exit_order_and_mark_closing(
            exit_order(intent_id, ids, dec!(100), dec!(0.55)),
            ExitReason::StopLoss,
            None,
        )
        .await
        .expect("exit order");

    submission
        .record_exit_result(
            &exit.execution_order_id,
            ExitLedgerWrite {
                order_state: ExecutionOrderState::Filled,
                venue_order_id: Some(OrderId::new("venue-exit")),
                venue_status: Some(VenueOrderStatus::Filled),
                filled_at: Some(Utc::now()),
                cancelled_at: None,
                error_message: None,
                exit_state: ExitState::Exited,
                exit_reason: ExitReason::StopLoss,
                position_exit: Some(PositionExit {
                    shares: Shares::new(dec!(100)),
                    avg_price: Price::new(dec!(0.55)),
                    proceeds_usd: Usd::new(dec!(55)),
                    realized_pnl_usd: Usd::new(dec!(-5)),
                    exited_at: Utc::now(),
                    reason: ExitReason::StopLoss,
                }),
                fully_exited: true,
                revert_to_open: false,
                reconciliation: None,
            },
        )
        .await
        .expect("record exit");
}

pub fn report_operation_log(ids: &ExecutionTxnIds) -> NewOperationLog {
    NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("scheduled:test:{}", ids.report).into(),
        actor_user_id: None,
        actor_username: Some("system".to_owned()),
        acting_role: Some("test".into()),
        category: OperationCategory::QuantReport,
        action: "publish".into(),
        resource_type: Some(ResourceType::QuantReport),
        resource_id: Some(ids.report.to_string()),
        http_method: OperationHttpMethod::System,
        http_path: "/test/quant/report".to_owned(),
        http_status: 201,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: OperationDetailDocument::try_from(serde_json::json!({ "test": true }))
            .expect("static operation detail"),
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    }
}

fn new_order_intent(
    order_intent_id: OrderIntentId,
    ids: &ExecutionTxnIds,
    status: OrderIntentStatus,
    approval_status: ApprovalStatus,
    runtime_mode: QuantRuntimeMode,
    approved_by: Option<UserId>,
) -> NewOrderIntent {
    let approved = matches!(
        status,
        OrderIntentStatus::Approved | OrderIntentStatus::ApprovedByPolicy
    );
    NewOrderIntent {
        order_intent_id,
        recommendation_id: ids.recommendation,
        runtime_mode,
        decision_policy_snapshot_id: ids.decision_policy_snapshot,
        model_version_id: ids.model_version,
        research_profile_artifact_id: fixture_profile_ref().artifact_id(),
        intent_kind: OrderIntentKind::Buy,
        status,
        approval_status,
        approved_by,
        approval_reason: if approved {
            Some("ui-demo-seed".to_owned())
        } else {
            None
        },
        approved_at: approved.then(Utc::now),
        policy_id: if runtime_mode == QuantRuntimeMode::SemiAuto {
            Some(ids.trade_policy.artifact_id.to_string())
        } else if status == OrderIntentStatus::ApprovedByPolicy {
            Some("auto".to_owned())
        } else {
            None
        },
        policy_hash: (runtime_mode == QuantRuntimeMode::SemiAuto)
            .then_some(ids.trade_policy.artifact_hash),
        status_reason: None,
        admission_trace_ref: None,
        condition_instance_id: ids.condition_instance,
        entry_order_json: EntryOrderSpec {
            token_id: TokenId::new(&ids.token),
            side: Side::Buy,
            order_type: OrderType::Fak,
            post_only: false,
            limit_price: Price::new(dec!(0.6)),
            amount: OrderAmount::CashBudget(Usd::new(EXECUTION_NOTIONAL)),
            max_slippage_bps: Bps::new(dec!(50)),
            valid_until: Utc::now() + Duration::hours(1),
        },
        exit_policy_json: ExitPolicySpec {
            take_profit_price: Some(Price::new(dec!(0.8))),
            take_profit_pct: None,
            stop_loss_price: Some(Price::new(dec!(0.5))),
            stop_loss_pct: None,
            time_exit_at: None,
            max_hold_secs: None,
            trailing_stop: None,
            thesis_invalidation: ThesisInvalidationPolicy {
                min_score_retention: dec!(0.6),
                min_expected_return_bps: Bps::ZERO,
                require_execution_eligibility: true,
            },
            opportunistic_exit: opportunistic_exit_policy(),
            scale_out_targets: Vec::new(),
            settlement_mode: ExitSettlementMode::ExitBeforeResolution,
            redeem_policy: RedeemPolicy::Manual,
            manual_review_at: None,
            entry_reference_price: Price::new(dec!(0.6)),
            entry_composite_score: Probability::new(dec!(0.8)),
        },
        risk_envelope_hash: content_hash('f'),
        expires_at: Utc::now() + Duration::hours(1),
    }
}

fn new_capital_allocation(
    order_intent_id: OrderIntentId,
    ids: &ExecutionTxnIds,
) -> NewCapitalAllocation {
    NewCapitalAllocation {
        capital_allocation_id: CapitalAllocationId::from_v7(),
        order_intent_id,
        recommendation_id: ids.recommendation,
        state: CapitalAllocationState::Allocated,
        planned_usd: Usd::new(EXECUTION_NOTIONAL),
        allocated_usd: Usd::new(EXECUTION_NOTIONAL),
        locked_usd: Usd::ZERO,
        spent_usd: Usd::ZERO,
        released_usd: Usd::ZERO,
        reason: "intent created".to_owned(),
    }
}

/// Entry execution order template for submission integration / demo seeds.
pub fn entry_execution_order(
    intent_id: &OrderIntentId,
    ids: &ExecutionTxnIds,
) -> NewExecutionOrder {
    new_execution_order(intent_id, ids)
}

pub fn fixture_profile_ref() -> ResearchProfileRef {
    builtin_research_profiles()
        .expect("research profiles")
        .into_iter()
        .find(|profile| {
            profile.spec.activation_eligibility == ResearchEvaluationTrack::SemiAutoCandidate
        })
        .expect("weather profile")
        .profile_ref
}

pub fn prepared_order(
    side: Side,
    order_type: OrderType,
    venue_amount: VenueOrderAmount,
    expected_fee: Usd,
    expected_filled_shares: Shares,
    worst_price: Price,
) -> PreparedVenueOrder {
    let now = Utc::now();
    let total_cash_delta = match (side, venue_amount) {
        (Side::Buy, VenueOrderAmount::GrossUsd(gross)) => -(gross.inner() + expected_fee.inner()),
        (Side::Sell, VenueOrderAmount::Shares(shares)) => {
            shares.inner() * worst_price.inner() - expected_fee.inner()
        }
        _ => Decimal::ZERO,
    };
    PreparedVenueOrder {
        profile_ref: fixture_profile_ref(),
        token_id: TokenId::new("1001"),
        side,
        order_type,
        post_only: false,
        worst_price,
        cash_budget: venue_amount.gross_usd().map(|gross| gross + expected_fee),
        venue_amount,
        expected_fee,
        total_cash_delta,
        expected_filled_shares,
        book_hash: content_hash('b'),
        clob_market_info_hash: content_hash('c'),
        fee_schedule: PreparedFeeSchedule {
            schedule_hash: content_hash('f'),
            effective_at: now,
            available_at: now,
            platform_rate: dec!(0.02),
            exponent: Decimal::ONE,
            taker_only: true,
            builder_maker_fee_bps: Bps::ZERO,
            builder_taker_fee_bps: Bps::ZERO,
            builder_attribution: BuilderFeeAttribution::NoBuilderCode,
        },
        prepared_at: now,
        valid_until: now + Duration::hours(1),
    }
}

fn new_execution_order(intent_id: &OrderIntentId, ids: &ExecutionTxnIds) -> NewExecutionOrder {
    NewExecutionOrder {
        execution_order_id: ExecutionOrderId::from_v7(),
        order_intent_id: *intent_id,
        order_phase: ExecutionOrderPhase::Entry,
        market_id: MarketId::new(&ids.market),
        token_id: TokenId::new(&ids.token),
        side: Side::Buy,
        order_type: OrderTypeKind::Fak,
        price: Price::new(dec!(0.6)),
        shares: Shares::new(dec!(100)),
        cost_usd: Usd::new(EXECUTION_NOTIONAL),
        prepared_order_json: prepared_order(
            Side::Buy,
            OrderType::Fak,
            VenueOrderAmount::GrossUsd(Usd::new(dec!(24))),
            Usd::new(dec!(1)),
            Shares::new(dec!(40)),
            Price::new(dec!(0.6)),
        ),
        venue_order_id: None,
        venue_status: None,
        state: ExecutionOrderState::Submitted,
        submitted_at: None,
        filled_at: None,
        cancelled_at: None,
        gtd_expiration_at: None,
        error_message: None,
    }
}

fn position_fill(ids: &ExecutionTxnIds, intent_id: &OrderIntentId) -> PositionFill {
    position_fill_public(
        ids,
        intent_id,
        Shares::new(dec!(100)),
        Usd::new(EXECUTION_NOTIONAL),
    )
}

/// Position fill helper for partial-fill demo scenarios.
pub fn position_fill_public(
    ids: &ExecutionTxnIds,
    intent_id: &OrderIntentId,
    shares: Shares,
    cost_usd: Usd,
) -> PositionFill {
    PositionFill {
        order_intent_id: *intent_id,
        token_id: TokenId::new(&ids.token),
        market_id: MarketId::new(&ids.market),
        event_id: Some(EventId::new(&ids.event)),
        category: MarketCategory::Politics,
        side: OutcomeSide::Yes,
        shares,
        price: Price::new(dec!(0.6)),
        cost_usd,
        filled_at: Utc::now(),
        source: AccountSource::Polymarket,
    }
}

fn reconciliation_row(
    execution_order_id: &ExecutionOrderId,
    intent_id: &OrderIntentId,
) -> NewReconciliation {
    NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: *execution_order_id,
        order_intent_id: *intent_id,
        result: ReconciliationResult::Unresolvable,
        evidence_json: ReconciliationEvidenceChain(vec![ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::ClobOrderStatus,
            observed_at: Utc::now(),
            detail: "submission result".to_owned(),
            venue_ref: None,
            shares: None,
            price: None,
            fee_evidence: None,
        }]),
        venue_filled_shares: None,
        venue_avg_price: None,
        expected_cash_delta_usd: None,
        venue_cash_delta_usd: None,
        realized_pnl_usd: None,
        expected_fee_usd: None,
        observed_fee_usd: None,
        fee_delta_usd: None,
        resolved_by: None,
        resolved_at: None,
    }
}

fn exit_order(
    intent_id: &OrderIntentId,
    ids: &ExecutionTxnIds,
    shares: Decimal,
    price: Decimal,
) -> NewExecutionOrder {
    NewExecutionOrder {
        execution_order_id: ExecutionOrderId::from_v7(),
        order_intent_id: *intent_id,
        order_phase: ExecutionOrderPhase::Exit,
        market_id: MarketId::new(&ids.market),
        token_id: TokenId::new(&ids.token),
        side: Side::Sell,
        order_type: OrderTypeKind::Gtc,
        price: Price::new(price),
        shares: Shares::new(shares),
        cost_usd: Shares::new(shares) * Price::new(price),
        prepared_order_json: prepared_order(
            Side::Sell,
            OrderType::Gtc,
            VenueOrderAmount::Shares(Shares::new(shares)),
            Usd::ZERO,
            Shares::new(shares),
            Price::new(price),
        ),
        venue_order_id: None,
        venue_status: None,
        state: ExecutionOrderState::Submitted,
        submitted_at: None,
        filled_at: None,
        cancelled_at: None,
        gtd_expiration_at: None,
        error_message: None,
    }
}

async fn seed_market_catalog(
    db: &DatabaseConnection,
    event_id: &str,
    market_id: &str,
    market_question: &str,
    market_slug: &str,
) {
    PgEventRepository::new(db.clone())
        .upsert(make_event(
            event_id,
            "Event",
            "event",
            MarketCategory::Politics,
        ))
        .await
        .expect("seed event");
    PgMarketRepository::new(db.clone())
        .upsert(make_market(
            market_id,
            event_id,
            market_question,
            market_slug,
            MarketCategory::Politics,
            None,
        ))
        .await
        .expect("seed market");
}

async fn seed_runtime_config_named(
    db: &DatabaseConnection,
    created_by: &str,
    reason: &str,
) -> DecisionPolicySnapshotId {
    bootstrap_policy_bundle(
        &PgPolicyRepository::new(db.clone()),
        &DecisionPolicySnapshot::default(),
        created_by,
        reason,
    )
    .await
}

/// Test-only Published policy whose evidence is internally coherent.
///
/// Production fitters intentionally cannot create `Published` cohorts directly;
/// this fixture exists solely to exercise guarded execution paths in isolated
/// database tests.
fn executable_policy_fixture_key(category: MarketCategory) -> TradePolicyCohortKey {
    let profile_ref = builtin_research_profiles()
        .expect("research profiles")
        .into_iter()
        .find(|profile| {
            profile.spec.activation_eligibility == ResearchEvaluationTrack::SemiAutoCandidate
        })
        .expect("weather profile")
        .profile_ref;
    let dimension = TradePolicyCohortDimension {
        methodology_id: "test-only-structural-volatility-v1".to_owned(),
        methodology_hash: content_hash('7'),
        bucket_id: "fixture".to_owned(),
    };
    TradePolicyCohortKey {
        profile_ref,
        category,
        horizon_secs: 86_400,
        entry_price_min: Price::new(dec!(0.01)),
        entry_price_max: Price::new(dec!(0.99)),
        cash_budget_tier: Usd::new(dec!(25)),
        liquidity: dimension.clone(),
        volatility: dimension,
    }
}

fn executable_policy_fixture_cohort(key: TradePolicyCohortKey) -> TradePolicyCohort {
    TradePolicyCohort {
        key,
        entry_condition: EntryConditionTemplate::Immediate,
        entry_order: EntryOrderTemplate::Aggressive {
            fill_requirement: FillRequirement::AllOrNothing,
            max_slippage_bps: Bps::new(dec!(50)),
            max_book_age_ms: 2_000,
        },
        max_slippage_bps: Bps::new(dec!(50)),
        max_book_age_ms: 2_000,
        upper_barrier_bps: Bps::new(dec!(1_000)),
        lower_barrier_bps: Bps::new(dec!(1_000)),
        vertical_barrier_secs: 3_600,
        scale_out_targets: Vec::new(),
        trailing_stop: None,
        min_score_retention: dec!(0.6),
        min_expected_return_bps: Bps::ZERO,
        require_execution_eligibility: true,
        opportunistic_exit: opportunistic_exit_policy(),
        settlement_mode: ExitSettlementMode::HoldToResolution,
        redeem_policy: RedeemPolicy::Manual,
        sample_count: 100,
        effective_sample_size: Decimal::from(100),
        executable_sample_count: 100,
        executable_coverage: Decimal::ONE,
        selected_candidate_id: "immediate".to_owned(),
        full_l2_coverage: Decimal::ONE,
        common_candidate_support: Decimal::ONE,
        passive_reconciled_trade_coverage: None,
        fee_catalog_coverage: Decimal::ONE,
        cpcv_path_count: 21,
        trial_count: 1,
        deflated_sharpe_ratio: Decimal::ONE,
        probability_of_backtest_overfitting: Decimal::ZERO,
        ambiguous_touch_rate: Decimal::ZERO,
        depth_failure_rate: Decimal::ZERO,
        lower_confidence_utility_bps: Some(Bps::new(dec!(2))),
        parameter_source: TradePolicyParameterSource {
            relaxed_dimensions: Vec::new(),
            source_sample_count: 100,
            source_effective_sample_size: Decimal::from(100),
            source_selector_hash: content_hash('8'),
        },
    }
}

fn executable_policy_fixture_payload(
    now: DateTime<Utc>,
    decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    cohort_key: &TradePolicyCohortKey,
) -> TradePolicyArtifactPayload {
    let exit = TradePolicyExitTemplate {
        upper_barrier_bps: Bps::new(dec!(1_000)),
        lower_barrier_bps: Bps::new(dec!(1_000)),
        vertical_barrier_secs: 3_600,
        scale_out_targets: Vec::new(),
        trailing_stop: None,
        min_score_retention: dec!(0.6),
        min_expected_return_bps: Bps::ZERO,
        require_execution_eligibility: true,
        opportunistic_exit: opportunistic_exit_policy(),
        settlement_mode: ExitSettlementMode::HoldToResolution,
        redeem_policy: RedeemPolicy::Manual,
        reason_execution: ExitReason::ALL
            .into_iter()
            .map(|reason| ExitExecutionTemplate {
                reason,
                fill_requirement: FillRequirement::AllowPartial,
                max_attempts: 3,
                retry_cadence_ms: 1_000,
                max_slippage_bps: Bps::new(dec!(50)),
                residual_share_policy: ResidualSharePolicy::HoldToSettlement,
            })
            .collect(),
    };
    let candidates = vec![TradePolicyCandidateSpec {
        candidate_id: "immediate".to_owned(),
        entry_condition: EntryConditionTemplate::Immediate,
        entry_execution: EntryOrderTemplate::Aggressive {
            fill_requirement: FillRequirement::AllOrNothing,
            max_slippage_bps: Bps::new(dec!(50)),
            max_book_age_ms: 2_000,
        },
        exit,
    }];
    let candidate_set_hash = ResearchHasher::canonical(&candidates).expect("candidate hash");
    let methodology_hash = content_hash('9');
    let latency_profile_hash = content_hash('a');
    let latency_evidence_id = ResearchReadinessEvidenceId::from_v7();
    let trial_ledger_hash = content_hash('6');
    let profile = builtin_research_profiles()
        .expect("research profiles")
        .into_iter()
        .find(|profile| profile.profile_ref == cohort_key.profile_ref)
        .expect("cohort profile");
    let vertical_gate_evidence = if cohort_key.category == MarketCategory::Weather {
        vec![VerticalGateEvidence {
            gate: VerticalGateKind::WeatherNoaaProxy,
            target: VerticalActivationTarget::SemiAuto,
            evidence_window_start: now - chrono::Duration::days(31),
            evidence_window_end: now,
            sample_count: 500,
            distinct_subject_count: 20,
            distinct_local_dates: 30,
            availability: dec!(0.99),
            agreement_wilson_lower_bound: dec!(0.95),
            target_subject_sample_count: Some(20),
            target_subject_wilson_lower_bound: Some(dec!(0.90)),
            unresolved_mismatch_count: 0,
            gaps_recovered: true,
            methodology_hash: content_hash('9'),
        }]
    } else {
        Vec::new()
    };
    TradePolicyArtifactPayload {
        format_version: TRADE_POLICY_ARTIFACT_FORMAT_VERSION,
        activation_target: VerticalActivationTarget::SemiAuto,
        fit_contract: TradePolicyFitContract {
            profile_ref: profile.profile_ref,
            evaluation_track: ResearchEvaluationTrack::SemiAutoCandidate,
            research_program_hash: content_hash('7'),
            source_dataset_id: TrainingDatasetId::from_v7(),
            model_version_id: ModelVersionId::from_v7(),
            decision_policy_snapshot_id: *decision_policy_snapshot_id,
            fit_window_start: now - Duration::days(92),
            fit_window_end: now - Duration::days(2),
            pit_cutoff: now - Duration::days(1),
            target_horizon_secs: profile.spec.target_horizon_secs,
            cash_budget_tiers: profile.spec.allowed_cash_budget_tiers,
            methodology_hash,
            latency_evidence_id,
            latency_profile_hash,
            quality_gate: profile.spec.quality_gate,
        },
        source_dataset_hash: content_hash('1'),
        feature_schema_hash: content_hash('2'),
        label_schema_hash: content_hash('3'),
        fill_simulator_version: "test-only-v1".to_owned(),
        embargo_secs: 86_400,
        pit_cutoff_evidence: Some(TradePolicyPitCutoffEvidence {
            filtered_sample_count: 1,
            labels_matured_by_cutoff: 1,
            labels_excluded_after_cutoff: 0,
            filtered_sample_hash: content_hash('4'),
        }),
        execution_evidence: TradePolicyExecutionEvidence {
            entry_basis: Some(ExecutablePriceBasis::FullL2Vwap),
            exit_basis: Some(ExecutablePriceBasis::FullL2Vwap),
            full_l2_sample_count: 1,
            full_l2_coverage: Some(Decimal::ONE),
            fee_model_hash: Some(content_hash('5')),
            gaps: Vec::new(),
        },
        candidate_set_hash,
        candidates,
        evidence_bundle: Some(TradePolicyEvidenceBundleRef {
            manifest_uri: ArtifactUri::parse("s3://fixture/policy-evidence/manifest.json")
                .expect("artifact uri"),
            manifest_hash: content_hash('b'),
            simulator_hash: content_hash('c'),
            replay_kernel_hash: content_hash('d'),
            methodology_hash,
            latency_evidence_id,
            latency_profile_hash,
            catalog_ledger_hash: content_hash('e'),
            source_slice_manifest_hash: content_hash('f'),
            fit_job_id: ResearchJobId::from_v7(),
            trial_ledger_hash,
        }),
        vertical_gate_evidence,
        structural_volatility_oos: StructuralVolatilityOosEvidence {
            methodology_hash: content_hash('8'),
            active_update_only: true,
            activity_proxy: "sqrt_reconciled_hourly_volume_usd".to_owned(),
            minimum_contract_observations: 48,
            fold_count: 2,
            forecast_count: 100,
            deadline_vw_interval_score: dec!(0.5),
            dr_as_vw_interval_score: dec!(0.4),
            deadline_volume_weighted_coverage: dec!(0.94),
            dr_as_volume_weighted_coverage: dec!(0.95),
            valid: true,
        },
        cohorts: vec![executable_policy_fixture_cohort(cohort_key.clone())],
        validation: TradePolicyValidationEvidence {
            trial_ledger_cutoff: Some(now),
            trial_ledger_hash: Some(trial_ledger_hash),
            attempted_candidate_count: Some(1),
            cpcv_path_count: Some(21),
            deflated_sharpe_ratio: Some(Decimal::ONE),
            probability_of_backtest_overfitting: Some(Decimal::ZERO),
            effective_sample_size: Some(Decimal::from(100)),
            ambiguous_touch_rate: Some(Decimal::ZERO),
            depth_failure_rate: Some(Decimal::ZERO),
            common_candidate_support: Some(Decimal::ONE),
            fee_catalog_coverage: Some(Decimal::ONE),
            eligible_market_coverage: Some(Decimal::ONE),
        },
    }
}

async fn seed_trade_policy_fixture(
    db: &DatabaseConnection,
    decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    category: MarketCategory,
) -> TradePolicyCohortProvenance {
    let now = Utc::now();
    let cohort_key = executable_policy_fixture_key(category);
    let payload = executable_policy_fixture_payload(now, decision_policy_snapshot_id, &cohort_key);
    let blockers = payload.publication_blockers();
    assert!(
        blockers.is_empty(),
        "execution fixture policy must pass its frozen gates: {blockers:?}"
    );
    let artifact_hash = ResearchHasher::canonical(&payload).expect("hash fixture policy");
    let artifact_id = TradePolicyArtifactId::from_content_hash(&artifact_hash);
    let policies = PgTradePolicyRepository::new(db.clone());
    policies
        .insert(NewTradePolicyArtifact {
            artifact_id,
            content_hash: artifact_hash,
            status: TradePolicyStatus::Validated,
            source_dataset_id: payload.fit_contract.source_dataset_id,
            payload_json: payload,
        })
        .await
        .expect("seed test-only executable trade policy");
    policies
        .transition(
            &artifact_id,
            TradePolicyStatus::Validated,
            TradePolicyStatus::Published,
            NewTradePolicyGovernanceAudit {
                audit_id: TradePolicyGovernanceAuditId::from_v7(),
                artifact_id,
                action: TradePolicyGovernanceAction::Publish,
                from_status: TradePolicyStatus::Validated,
                to_status: TradePolicyStatus::Published,
                content_hash: artifact_hash,
                actor_id: UserId::new(Uuid::nil()),
                reason: "test-only execution fixture publication".to_owned(),
            },
        )
        .await
        .expect("publish test-only executable trade policy with WORM audit");
    TradePolicyCohortProvenance {
        artifact_id,
        artifact_hash,
        cohort_index: 0,
        cohort_key,
    }
}

/// Seed a coherent Published policy for report-pipeline integration tests.
///
/// The production fitter remains the only non-test path that may construct a
/// policy artifact. This helper exists to bind report fixtures to the same
/// immutable policy contract enforced by serving.
pub async fn seed_report_trade_policy_fixture(
    db: &DatabaseConnection,
    decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    category: MarketCategory,
) -> TradePolicyCohortProvenance {
    seed_trade_policy_fixture(db, decision_policy_snapshot_id, category).await
}

async fn seed_executable_trade_policy_fixture(
    db: &DatabaseConnection,
    decision_policy_snapshot_id: &DecisionPolicySnapshotId,
) -> TradePolicyCohortProvenance {
    seed_trade_policy_fixture(db, decision_policy_snapshot_id, MarketCategory::Weather).await
}

/// Seed a coherent held-out score calibration for model/report integration tests.
pub async fn seed_model_score_calibration_fixture(
    db: &DatabaseConnection,
    model_version_id: &ModelVersionId,
) -> CalibrationArtifactId {
    let calibration_id = CalibrationArtifactId::from_v7();
    let fit_window_start = Utc::now() - Duration::days(90);
    let fit_window_end = Utc::now() - Duration::days(1);
    let fit_window = TimeWindow::new(fit_window_start, fit_window_end);
    let calibration_split_hash = content_hash('c');
    let calibration_payload = ModelScoreCalibrationPayload {
        model_version_id: *model_version_id,
        calibration_dataset_id: TrainingDatasetId::from_v7(),
        mapping: MonotoneMapping::Isotonic {
            knots: vec![
                IsotonicKnot {
                    score: Decimal::ZERO,
                    probability: dec!(0.55),
                },
                IsotonicKnot {
                    score: Decimal::ONE,
                    probability: dec!(0.65),
                },
            ],
        },
        reliability: ReliabilityReport {
            bins: vec![ReliabilityBin {
                predicted_lo: Decimal::ZERO,
                predicted_hi: Decimal::ONE,
                sample_count: 500,
                mean_predicted: Probability::new(dec!(0.60)),
                empirical_frequency: Probability::new(dec!(0.60)),
                wilson_ci: (Probability::new(dec!(0.55)), Probability::new(dec!(0.65))),
                mean_adverse_excursion_bps: Some(dec!(-500)),
            }],
            brier_score: dec!(0.24),
            log_loss: dec!(0.67),
            ece: dec!(0.02),
            n_samples: 500,
        },
    };
    let calibration_hash =
        model_score_content_hash(&fit_window, &calibration_split_hash, &calibration_payload)
            .expect("hash demo calibration artifact");
    PgCalibrationArtifactRepository::new(db.clone())
        .create(NewCalibrationArtifact {
            artifact_id: calibration_id,
            kind: CalibrationKind::ModelScore,
            content_hash: calibration_hash,
            fit_window_start,
            fit_window_end,
            calibration_split_hash,
            sample_count: 500,
            payload: CalibrationArtifactPayload::ModelScore(calibration_payload),
            active: true,
        })
        .await
        .expect("seed active demo calibration artifact");

    calibration_id
}

async fn seed_calibrated_execution_model_artifact(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    model_version_id: &ModelVersionId,
    model_spec_definition_hash: &ContentHash,
    trade_policy: &TradePolicyCohortProvenance,
) -> ContentHash {
    let calibration_id = seed_model_score_calibration_fixture(db, model_version_id).await;

    let input_contract = ModelInputContract::single_required("book.mid");
    let input_contract_hash =
        model_input_contract_hash(&input_contract).expect("hash demo model input contract");
    let artifact = ModelArtifact::WeightedFactor(Box::new(WeightedFactorModelArtifact {
        header: ModelArtifactHeader {
            model_version_id: *model_version_id,
            model_spec_definition_hash: *model_spec_definition_hash,
            profile_ref: fixture_profile_ref(),
            model_family: ModelFamily::WeightedFactor,
            feature_schema_hash: content_hash('f'),
            factor_schema_hash: content_hash('6'),
            trade_policy_artifact_id: Some(trade_policy.artifact_id),
            trade_policy_hash: Some(trade_policy.artifact_hash),
        },
        training_dataset_hash: content_hash('d'),
        training_input_hash: content_hash('7'),
        input_contract,
        input_contract_hash,
        weights: vec![FactorWeight {
            factor: LIQUIDITY_DEPTH,
            weight: Decimal::ONE,
        }],
        prediction_horizon_secs: 86_400,
        multipliers: ScoreMultiplierSpec::conservative(),
        substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
        return_model: ReturnModelSpec::Calibrated(CalibratedReturnModel {
            calibrator_ref: calibration_id,
            downside_source: DownsideSource::MfeMae,
        }),
        factor_cross_section: FactorCrossSectionConfig::default(),
        frozen_reference_quantiles: FrozenReferenceQuantiles::empty(),
        objective_report: None,
        category_scope: None,
    }));
    artifact.validate().expect("validate demo model artifact");
    let artifact_hash = artifact.content_hash().expect("hash demo model artifact");
    store
        .put(
            ModelArtifact::artifact_key(&artifact_hash).expect("key demo model artifact"),
            &artifact.to_bytes().expect("serialize demo model artifact"),
        )
        .await
        .expect("store demo model artifact");
    artifact_hash
}

async fn seed_model_version_named(
    db: &DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
    model_name: &str,
    artifact_store: Option<&Arc<dyn ArtifactStore>>,
) -> (ModelVersionId, ModelRunId, TradePolicyCohortProvenance) {
    let registry = PgModelRegistryRepository::new(db.clone());
    let (model_spec_id, model_spec_definition_hash) = if let Some(existing) = Entity::find()
        .filter(Column::Name.eq(model_name))
        .one(db)
        .await
        .expect("find demo model spec")
    {
        (existing.model_spec_id, existing.definition_hash)
    } else {
        let model_spec_id = ModelSpecId::from_v7();
        let spec = new_model_spec_fixture(
            model_spec_id,
            model_name,
            ModelFamily::WeightedFactor,
            86_400,
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        );
        let definition_hash = spec.definition_hash;
        registry.create_model_spec(spec).await.expect("model spec");
        (model_spec_id, definition_hash)
    };
    let trade_policy = seed_executable_trade_policy_fixture(db, rc_id).await;
    let version = registry
        .next_version_for_spec(&model_spec_id)
        .await
        .expect("next demo model version");
    let model_version_id = ModelVersionId::from_v7();
    let artifact_hash = match artifact_store {
        Some(store) => {
            seed_calibrated_execution_model_artifact(
                db,
                store,
                &model_version_id,
                &model_spec_definition_hash,
                &trade_policy,
            )
            .await
        }
        None => content_hash('a'),
    };
    registry
        .create_model_version(NewModelVersion {
            model_version_id,
            model_spec_id,
            version,
            artifact_hash,
            category_scope: None,
            profile_ref: fixture_profile_ref(),
            training_dataset_id: None,
            trade_policy_artifact_id: Some(trade_policy.artifact_id),
            trade_policy_hash: Some(trade_policy.artifact_hash),
            publish_path_set_id: None,
            derivation: NewModelVersion::training_derivation(),
            metrics: ModelVersionMetrics::not_measured("test fixture"),
            training_objective: ModelTrainingObjective::hand_authored("test fixture"),
            quality_gate_report: None,
            publication_status: PublicationStatus::Published,
            published_at: Some(Utc::now()),
            retired_at: None,
        })
        .await
        .expect("model version");
    let model_run_id = ModelRunId::from_v7();
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::LiveInference,
            model_version_id: Some(model_version_id),
            decision_policy_snapshot_id: *rc_id,
            market_selection_id: None,
            window_start: Utc::now(),
            window_end: Utc::now(),
            status: ModelRunStatus::Succeeded,
            input_hash: content_hash('d'),
            output_hash: None,
            error_code: None,
            error_message: None,
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
        })
        .await
        .expect("model run");
    (model_version_id, model_run_id, trade_policy)
}

async fn seed_market_selection(
    db: &DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
    _market_id: &str,
) -> MarketSelectionId {
    let id = MarketSelectionId::from_v7();
    PgMarketSelectionRepository::new(db.clone())
        .create_snapshot(
            NewMarketSelection {
                market_selection_id: id,
                decision_at: Utc::now(),
                decision_policy_snapshot_id: *rc_id,
                selector_hash: content_hash('b'),
                market_count: 1,
                exclusion_summary: SelectionExclusionSummary::default(),
            },
            Vec::new(),
        )
        .await
        .expect("market selection");
    id
}

fn build_report_transaction(ids: &ExecutionTxnIds) -> NewReportTransaction {
    build_report_transaction_inner(ids, ReportBuildOptions::published_single(ids))
}

fn build_report_transaction_inner(
    ids: &ExecutionTxnIds,
    options: ReportBuildOptions,
) -> NewReportTransaction {
    let equity_snapshot_id = EquitySnapshotId::from_v7();
    let (report, allocated_usd) = fixture_report(ids, &options, &equity_snapshot_id);
    let ReportBuildOptions {
        recommendations,
        entry_condition_artifacts,
        as_of,
        ..
    } = options;
    let sampled_feature_parity = report_fixtures::sampled_parity(&report);
    let entry_condition_instances = recommendations
        .iter()
        .map(|recommendation| fixture_condition_instance(recommendation, ids, as_of))
        .collect();
    NewReportTransaction {
        feature_parity_state_id: Some(ids.feature_parity_state_id),
        account_snapshot: NewAccountSnapshot {
            account_snapshot_id: ids.account_snapshot,
            ..new_account_snapshot(ids)
        },
        equity_snapshot: NewEquitySnapshot {
            equity_snapshot_id,
            as_of,
            source: AccountSource::Polymarket,
            venue_net_liquidation_usd: Usd::new(dec!(10000)),
            capital_base_usd: Usd::new(dec!(10000)),
            available_usd: Usd::new(dec!(9000)),
            reserved_usd: Usd::ZERO,
            realized_pnl_cumulative_usd: Usd::ZERO,
            unrealized_pnl_usd: Usd::ZERO,
            high_water_mark_usd: Usd::new(dec!(10000)),
            drawdown_pct: Decimal::ZERO,
            account_snapshot_ref: Some(ids.account_snapshot),
        },
        data_quality_snapshot: NewReportDataQualitySnapshot {
            report_data_quality_snapshot_id: ids.data_quality_snapshot,
            decision_at: as_of,
            decision_policy_snapshot_id: ids.decision_policy_snapshot,
            tokens_json: ReportDataQualityTokens(Vec::new()),
        },
        portfolio_plan: NewPortfolioPlan {
            portfolio_plan_id: ids.portfolio_plan,
            model_run_id: Some(ids.model_run),
            market_selection_id: ids.market_selection,
            decision_at: as_of,
            budget_usd: Usd::new(dec!(10000)),
            allocated_usd,
            risk_budget_json: PortfolioRiskBudget::default(),
            constraints_json: PortfolioConstraintsSnapshot::default(),
            rejected_summary: PortfolioRejectedSummary::default(),
            optimizer_meta_json: PortfolioOptimizerMeta::default(),
        },
        report,
        recommendations,
        entry_condition_artifacts,
        entry_condition_instances,
        sampled_feature_parity: Some(sampled_feature_parity),
        fact_delivery: Some(report_fixtures::pending_fact_delivery(&ids.report)),
        operation_log: report_operation_log(ids),
    }
}

fn fixture_condition_instance(
    recommendation: &NewRecommendation,
    ids: &ExecutionTxnIds,
    published_at: DateTime<Utc>,
) -> NewEntryConditionInstance {
    let (artifact_id, artifact_hash, state, truth, next_evaluation_at) =
        match &recommendation.trade_plan {
            RecommendationTradePlan::Frozen { entry, .. } => match &entry.condition {
                EntryConditionPlan::Immediate => (
                    None,
                    None,
                    EntryConditionState::NotRequired,
                    Some(ConditionTruth::Satisfied),
                    None,
                ),
                EntryConditionPlan::Conditional {
                    artifact_id,
                    content_hash,
                } => (
                    Some(*artifact_id),
                    Some(*content_hash),
                    EntryConditionState::Waiting,
                    None,
                    Some(published_at),
                ),
            },
            RecommendationTradePlan::Unavailable { .. } => {
                (None, None, EntryConditionState::Invalidated, None, None)
            }
        };
    NewEntryConditionInstance {
        condition_instance_id: if recommendation.recommendation_id == ids.recommendation {
            ids.condition_instance
        } else {
            EntryConditionInstanceId::from_v7()
        },
        recommendation_id: recommendation.recommendation_id,
        artifact_id,
        artifact_hash,
        state,
        truth_json: truth,
        revision: 0,
        evaluation_hash: None,
        input_fingerprint: None,
        continuity_hash: None,
        fold_state_json: EntryConditionFoldState::default(),
        confirmation_started_at: None,
        last_evaluated_at: None,
        next_evaluation_at,
        expires_at: recommendation.valid_until,
        lease_owner: None,
        lease_expires_at: None,
        lease_epoch: 0,
        claimed_by_intent_id: None,
        claim_admission_state_version: None,
        consumed_at: None,
    }
}

fn price_condition_artifact(ids: &ExecutionTxnIds) -> NewEntryConditionArtifact {
    let payload = EntryConditionArtifactV1 {
        schema_version: ENTRY_CONDITION_SCHEMA_VERSION,
        evaluator_version: ENTRY_CONDITION_EVALUATOR_VERSION,
        binding: EntryConditionBinding {
            recommendation_id: ids.recommendation,
            market_id: MarketId::new(&ids.market),
            token_id: TokenId::new(&ids.token),
            outcome_side: OutcomeSide::Yes,
            market_linkage_id: None,
            market_linkage_hash: None,
            catalog_snapshot_id: ids.market_selection,
            catalog_snapshot_hash: content_hash('b'),
            model_version_id: ids.model_version,
            decision_policy_snapshot_id: ids.decision_policy_snapshot,
            factor_bindings: Vec::new(),
            source_bindings: Vec::new(),
        },
        confirmation: ConfirmationPolicy {
            required_continuous_ms: 2_000,
            max_observation_gap_ms: 1_000,
        },
        root: EntryConditionV1::Price(PriceCondition {
            token_id: TokenId::new(&ids.token),
            comparison: PriceComparison::AtOrBelow,
            threshold: Price::new(dec!(0.62)),
            max_input_age_ms: 2_000,
        }),
    }
    .canonicalize()
    .expect("canonical conditional price fixture");
    let content_hash = payload
        .canonical_content_hash()
        .expect("conditional price fixture hash");
    NewEntryConditionArtifact {
        artifact_id: EntryConditionArtifactId::from_content_hash(&content_hash),
        content_hash,
        schema_version: i32::try_from(ENTRY_CONDITION_SCHEMA_VERSION)
            .expect("entry-condition schema version fits i32"),
        evaluator_version: i32::try_from(ENTRY_CONDITION_EVALUATOR_VERSION)
            .expect("entry-condition evaluator version fits i32"),
        payload_json: payload,
    }
}

fn fixture_report(
    ids: &ExecutionTxnIds,
    options: &ReportBuildOptions,
    equity_snapshot_id: &EquitySnapshotId,
) -> (NewRecommendationReport, Usd) {
    let allocated_usd = options
        .recommendations
        .iter()
        .filter_map(|rec| rec.trade_plan.sizing().map(|sizing| sizing.suggested_usd))
        .sum();
    let report = NewRecommendationReport {
        recommendation_report_id: ids.report,
        research_profile_artifact_id: fixture_profile_ref().artifact_id(),
        report_kind: ReportKind::TopN,
        decision_at: options.as_of,
        horizon_secs: 86_400,
        runtime_mode: options.runtime_mode,
        decision_policy_snapshot_id: ids.decision_policy_snapshot,
        model_run_id: Some(ids.model_run),
        model_version_id: ids.model_version,
        market_selection_id: ids.market_selection,
        portfolio_plan_id: ids.portfolio_plan,
        top_n: 20,
        status: RecommendationReportStatus::Prepared,
        account_source: AccountSource::Polymarket,
        capital_base_usd: Usd::new(dec!(10000)),
        account_snapshot_ref: ids.account_snapshot,
        equity_snapshot_ref: *equity_snapshot_id,
        data_quality_snapshot_ref: ids.data_quality_snapshot,
        summary_json: options.summary.clone(),
        published_at: None,
        successor_report_id: None,
        superseded_at: None,
        obsoleted_at: None,
        valid_until: Some(options.as_of + Duration::hours(1)),
        revoked_at: None,
        expired_at: None,
        status_reason: None,
    };
    (report, allocated_usd)
}

pub fn content_hash(seed: char) -> ContentHash {
    CanonicalDigest::content_hash_json(&seed).expect("canonical fixture content hash")
}

pub fn source_slice_ref(seed: char) -> SourceSliceManifestRef {
    SourceSliceManifestRef {
        manifest_uri: ArtifactUri::parse(format!("s3://fixture/source-slices/{seed}.json"))
            .expect("source-slice URI"),
        manifest_hash: content_hash(seed),
    }
}

fn new_account_snapshot(ids: &ExecutionTxnIds) -> NewAccountSnapshot {
    let positions = vec![PositionSnapshot {
        token_id: TokenId::new(&ids.token),
        market_id: MarketId::new(&ids.market),
        event_id: Some(EventId::new(&ids.event)),
        category: MarketCategory::Politics,
        outcome: "Yes".to_owned(),
        size: Shares::new(dec!(100)),
        avg_price: Price::new(dec!(0.5)),
        cur_price: Price::new(dec!(0.6)),
        current_value: Usd::new(dec!(60)),
        redeemable: false,
    }];
    NewAccountSnapshot {
        account_snapshot_id: AccountSnapshotId::from_v7(),
        as_of: Utc::now(),
        source: AccountSource::Polymarket,
        venue_net_liquidation_usd: Usd::new(dec!(10000)),
        capital_base_usd: Usd::new(dec!(10000)),
        available_usd: Usd::new(dec!(9000)),
        reserved_usd: Usd::new(dec!(0)),
        positions_json: AccountPositions(positions.clone()),
        exposures_json: ExposureBreakdown::from_positions(&positions),
    }
}

fn entry_plan() -> EntryPlan {
    EntryPlan {
        condition: EntryConditionPlan::Immediate,
        order_policy: EntryOrderPolicy::Aggressive {
            worst_price: Price::new(dec!(0.6)),
            fill_requirement: FillRequirement::AllOrNothing,
        },
        max_slippage_bps: Bps::new(dec!(50)),
        valid_from: Utc::now(),
        valid_until: Utc::now() + Duration::hours(1),
        min_depth_usd: Usd::new(dec!(100)),
        max_book_age_ms: 2_000,
        cancel_if_not_triggered: true,
        entry_reason: "immediate".to_owned(),
    }
}

fn sizing_plan() -> SizingPlan {
    SizingPlan {
        suggested_usd: Usd::new(EXECUTION_NOTIONAL),
        suggested_shares: Shares::new(dec!(100)),
        max_usd: Usd::new(dec!(500)),
        min_usd: Usd::new(dec!(10)),
        portfolio_weight_pct: dec!(0.025),
        market_exposure_after_usd: Usd::new(EXECUTION_NOTIONAL),
        event_exposure_after_usd: Usd::new(EXECUTION_NOTIONAL),
        category_exposure_after_usd: Usd::new(EXECUTION_NOTIONAL),
        binding_constraint: BindingConstraint::KellyCap,
        sizing_reason: "kelly".to_owned(),
        sizing_model: SizingModelKind::Kelly,
        edge_bps: Some(Bps::new(dec!(100))),
        kelly_fraction_applied: Some(dec!(0.5)),
        edge_uncertainty_shrink_applied: None,
        correlation_shrink_applied: None,
        f_star_applied: None,
        kelly_fraction_config_applied: None,
        confidence_shrink_applied: None,
        drawdown_shrink_applied: None,
        raw_fraction_applied: None,
        position_cap_fraction_applied: None,
    }
}

fn exit_plan() -> ExitPlan {
    ExitPlan {
        take_profit_price: Some(Price::new(dec!(0.8))),
        take_profit_pct: None,
        stop_loss_price: Some(Price::new(dec!(0.4))),
        stop_loss_pct: None,
        time_exit_at: None,
        max_hold_secs: Some(86_400),
        scale_out_targets: Vec::new(),
        trailing_stop: None,
        thesis_invalidation: ThesisInvalidationPolicy {
            min_score_retention: dec!(0.6),
            min_expected_return_bps: Bps::ZERO,
            require_execution_eligibility: true,
        },
        opportunistic_exit: opportunistic_exit_policy(),
        settlement_mode: ExitSettlementMode::HoldToResolution,
        redeem_policy: RedeemPolicy::Manual,
        manual_review_at: None,
        exit_reason: "tp/sl".to_owned(),
    }
}

const fn opportunistic_exit_policy() -> OpportunisticExitPolicy {
    OpportunisticExitPolicy {
        min_confidence: Probability::new(dec!(0.65)),
        min_expected_alpha_bps: Bps::new(dec!(50)),
        min_p_exit_better: Probability::new(dec!(0.5)),
        max_cumulative_exit_pct: Decimal::ONE,
        min_incremental_exit_pct: dec!(0.1),
    }
}

fn trade_plan(policy: &TradePolicyCohortProvenance) -> RecommendationTradePlan {
    RecommendationTradePlan::Frozen {
        policy: Box::new(policy.clone()),
        entry: entry_plan(),
        sizing: Box::new(sizing_plan()),
        exit: Box::new(exit_plan()),
        risk_envelope: Box::new(risk_envelope()),
    }
}

fn risk_envelope() -> RiskEnvelope {
    RiskEnvelope {
        max_loss_usd: Usd::new(dec!(120)),
        max_slippage_bps: Bps::new(dec!(50)),
        max_position_usd: Usd::new(dec!(500)),
        max_market_exposure_usd: Usd::new(dec!(500)),
        max_event_exposure_usd: Usd::new(dec!(750)),
        max_category_exposure_usd: Usd::new(dec!(1500)),
        requires_approval: true,
        auto_execution_allowed: true,
        risk_notes: Vec::new(),
        envelope_hash: content_hash('f'),
    }
}

fn factor_breakdown() -> RecommendationFactorBreakdown {
    RecommendationFactorBreakdown(vec![FactorBreakdownEntry {
        factor_name: "liquidity_depth".to_owned(),
        family: FactorFamily::Liquidity,
        value_state: FactorValueState::Scored,
        raw_value: Some(dec!(1234.5)),
        normalized_score: Some(Probability::new(dec!(0.8))),
        normalization_source: Some(NormalizationSource::CrossSection),
        indeterminate_reason: None,
        weight: dec!(0.4),
        contribution: dec!(0.32),
        confidence: Probability::new(dec!(0.75)),
        direction: FactorDirection::Positive,
        explanation: "deep".to_owned(),
        source_refs: Vec::new(),
    }])
}

fn recommendation_identity() -> RecommendationIdentity {
    RecommendationIdentity {
        category: MarketCategory::Politics,
        question: "Will the event resolve Yes?".to_owned(),
        outcome_name: "Yes".to_owned(),
    }
}

const fn market_context() -> MarketContext {
    MarketContext {
        best_bid: Some(Price::new(dec!(0.41))),
        best_ask: Some(Price::new(dec!(0.43))),
        mid_price: Some(Price::new(dec!(0.42))),
        spread_bps: Some(Bps::new(dec!(50))),
        depth_usd: Usd::new(dec!(5000)),
        volume_24h_usd: Some(Usd::new(dec!(10000))),
        book_age_ms: 500,
        time_to_resolution_secs: Some(86_400),
        market_status: MarketStatus::Active,
        neg_risk: false,
        tick_size: Hundredth,
        fee_rate: None,
    }
}

fn evidence_refs(ids: &ExecutionTxnIds) -> EvidenceRefs {
    EvidenceRefs {
        signal_candidate_id: SignalCandidateId::from_v7(),
        feature_vector_id: FeatureVectorId::from_v7(),
        model_run_id: ids.model_run,
        market_selection_id: ids.market_selection,
        book_snapshot_ref: BookSnapshotRef::from_str(&format!(
            "book:l2|{}|00000000-0000-0000-0000-000000000001|1|blake3:{}|1700000000|1700000000@blake3:{}",
            ids.token,
            "1".repeat(64),
            "0".repeat(64)
        ))
        .expect("book ref"),
        decision_policy_snapshot_id: ids.decision_policy_snapshot,
        model_version_id: ids.model_version,
        factor_definition_versions: Vec::new(),
        data_quality_snapshot_ref: ids.data_quality_snapshot,
    }
}

fn execution_eligibility() -> ExecutionEligibility {
    ExecutionEligibility {
        eligible_modes: vec![
            QuantRuntimeMode::ReportOnly,
            QuantRuntimeMode::SemiAuto,
            QuantRuntimeMode::AutoExecution,
        ],
        ineligibility_reasons: Vec::new(),
        approval_required: false,
        auto_policy_id: None,
        uncalibrated_watermark: false,
    }
}

fn report_summary() -> ReportSummary {
    ReportSummary {
        market_selection_count: 1,
        candidate_count: 1,
        rejected_count: 0,
        published_recommendation_count: 1,
        total_suggested_usd: Usd::new(EXECUTION_NOTIONAL),
        max_single_recommendation_usd: Usd::new(EXECUTION_NOTIONAL),
        aggregate_exposure_cap_usd: None,
        category_allocation: BTreeMap::new(),
        event_allocation: BTreeMap::new(),
        average_score: Probability::new(dec!(0.7)),
        min_score: Probability::new(dec!(0.7)),
        model_confidence_summary: ConfidenceSummary::default(),
        data_quality_summary: DataQualitySummary::default(),
        top_rejection_reasons: Vec::new(),
        execution_eligibility_summary: EligibilitySummary::default(),
        empty_reason: None,
        warnings: Vec::new(),
    }
}

/// Summary for a published-empty report fixture.
#[must_use]
pub fn empty_report_summary() -> ReportSummary {
    ReportSummary {
        market_selection_count: 1,
        candidate_count: 12,
        rejected_count: 12,
        published_recommendation_count: 0,
        total_suggested_usd: Usd::ZERO,
        max_single_recommendation_usd: Usd::ZERO,
        aggregate_exposure_cap_usd: None,
        category_allocation: BTreeMap::new(),
        event_allocation: BTreeMap::new(),
        average_score: Probability::new(dec!(0)),
        min_score: Probability::new(dec!(0)),
        model_confidence_summary: ConfidenceSummary::default(),
        data_quality_summary: DataQualitySummary::default(),
        top_rejection_reasons: Vec::new(),
        execution_eligibility_summary: EligibilitySummary::default(),
        empty_reason: Some(EmptyReportReason::NoPositiveSignal),
        warnings: vec!["ui-demo: no positive signal above threshold".to_owned()],
    }
}
