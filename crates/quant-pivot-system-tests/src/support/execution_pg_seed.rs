//! `PostgreSQL` seed helpers owned by execution-ledger system tests.
//!
//! Shared fixture chain extracted from `pg_execution_submission` so attribution,
//! submission, and capital tests can drive the same money-critical ledger paths.

use std::{collections::BTreeMap, str::FromStr, sync::Arc};

use anyhow::{Result, ensure};
use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::governance::model_score_content_hash;
use quant_pivot_models::{
    config::ClickHouseConfig,
    domain::{
        governance::{NewOperationLog, RuntimeControlUpdate},
        market::fee::{BuilderFeeAttribution, ImmediateExecutionCost},
        quant::{
            AggressiveEntryEconomics, ApproveOrderIntent, CalibrationArtifactPayload,
            CapitalOccupancyBucket, CapitalSettlement, DiscountCurvePoint, EntryConditionClaim,
            EntryExecutionEconomics, ExactVerificationEvidence, ExecutableEconomicTier,
            ExecutionIdentityRefs, ExistingPortfolioState, ExitLedgerWrite, GlobalPortfolioPlan,
            HardReservationBucket, MarketSelectionModel, ModelScoreCalibrationCommit,
            ModelVersionInfo, NewAccountSnapshot, NewCalibrationArtifact, NewCapitalAllocation,
            NewEntryConditionArtifact, NewEntryConditionInstance, NewEquitySnapshot,
            NewExecutionAccount, NewExecutionOrder, NewFactorDefinition, NewFeatureParityState,
            NewMarketSelection, NewMarketSelectionMember, NewModelRun, NewModelVersion,
            NewOrderIntent, NewPortfolioPlan, NewRecommendation, NewRecommendationReport,
            NewReconciliation, NewReportDataQualitySnapshot, NewReportRouteRun,
            NewReportTransaction, PortfolioConstraintEvidence, PortfolioDecisionResult,
            PortfolioObjectiveEvidence, PortfolioScenario, PortfolioScenarioArtifact,
            PortfolioScenarioEvidenceRegime, PortfolioScenarioKind, PortfolioScenarioVisibility,
            PositionExit, PositionFill, RecommendationEconomics, RepresentedRouteSet,
            RouteCandidateFunnel, RouteModelLineage, RouteRunOutcome,
            ScenarioCapitalOccupancySlice, ScenarioCashflow, ScenarioDistribution,
            ScenarioEntryExecution, ScenarioExecutionCashflow, ScenarioWeight, SolverEvidence,
            SubmissionLedgerWrite, TrainingDatasetInfo,
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
        factor::{FactorNormalization, NormalizationSource},
        market::MarketStatus,
        model::ModelFamily,
        operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
        quant::{
            AccountSource, ApprovalStatus, CalibrationKind, DatasetPurpose, DownsideSource,
            EmptyReportReason, EntryConditionState, ExecutionOrderState, ExecutionWalletKind,
            ExitSettlementMode, FeatureParityLatchState, FeatureParityStateTransition,
            FillRequirement, ModelRunKind, OrderIntentStatus, OutcomeSide, PriceComparison,
            QuantRuntimeMode, RecommendationReportStatus, RecommendationStatus, RedeemPolicy,
            ReportKind,
        },
        rbac::ResourceType,
    },
    hashing::CanonicalDigest,
    runtime_config::{
        BuyModelRoute, BuyRouteBinding, DecimalValue, DecisionPolicySnapshot, FactorHeadConfig,
        ModelBinding, ModelBindingSource, PortfolioConfig,
    },
    types::{
        AccountPositions, AccountSnapshotId, ArtifactUri, BookSnapshotRef, Bps,
        CalibrationArtifactId, CapitalAllocationId, ConditionTruth, ConfirmationPolicy,
        ContentHash, DataQualitySummary, DecisionPolicySnapshotId,
        ENTRY_CONDITION_EVALUATOR_VERSION, ENTRY_CONDITION_SCHEMA_VERSION, EconomicTierId,
        EligibilitySummary, EntryConditionArtifactId, EntryConditionArtifactV1,
        EntryConditionBinding, EntryConditionFoldState, EntryConditionInstanceId,
        EntryConditionPlan, EntryConditionV1, EntryOrderPolicy, EntryOrderSpec, EntryPlan,
        EquitySnapshotId, EventId, EvidenceRefs, EvmAddress, ExecutionAccountId,
        ExecutionEligibility, ExecutionOrderId, ExitPlan, ExitPolicySpec, ExposureBreakdown,
        FactorBreakdownEntry, FeatureParityStateId, FeatureVectorId, HistoryServingHeadSealId,
        MarketContext, MarketId, MarketSelectionId, ModelInputContract, ModelRunId, ModelSpecId,
        ModelVersionId, OperationDetailDocument, OperationLogId, OpportunisticExitPolicy,
        OrderAmount, OrderId, OrderIntentId, PayoutRatio, PendingScaleOut, PolicyBundleGeneration,
        PortfolioPlanId, PortfolioScenarioArtifactId, PortfolioScenarioModelArtifactId,
        PositionSnapshot, PreparedFeeSchedule, PreparedVenueOrder, Price, PriceCondition,
        Probability, RecommendationFactorBreakdown, RecommendationId, RecommendationIdentity,
        RecommendationReportId, RecommendationTradePlan, ReconciliationEvidence,
        ReconciliationEvidenceChain, ReconciliationId, ReportDataQualitySnapshotId,
        ReportDataQualityTokens, ReportRouteRunId, ReportRunId, ReportSummary, ResearchProfileRef,
        ResearchProfileSpec, RiskEnvelope, RoleCode, SchemaVersion, SelectionExclusionSummary,
        ServingAuthority, Shares, SignalCandidateId, SizingPlan, SourceSliceManifestRef,
        ThesisInvalidationPolicy, TokenId, TradePolicyCohortProvenance, TrainingDatasetId, Usd,
        UsdHours, UserId, VenueOrderAmount, builtin_research_profiles,
        calibration::{
            IsotonicKnot, MODEL_SCORE_CALIBRATION_FORMAT_VERSION,
            ModelScoreCalibrationDatasetBinding, ModelScoreCalibrationFitContract,
            ModelScoreCalibrationModelBinding, ModelScoreCalibrationPayload,
            ModelScoreCalibrationPolicyBinding, MonotoneMapping, ReliabilityBin, ReliabilityReport,
            SplitPayoutRateEvidence,
        },
        factor::{FactorDefinitionRef, FactorExplanation, FactorServingPlane},
        model_lineage::ModelVersionDerivation,
        model_metrics::ModelVersionMetrics,
        model_serving::ModelServingTradePolicyBinding,
        model_training::ModelTrainingObjective,
        stable_name::FactorName,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCalibrationArtifactRepository, PgEntryConditionRepository, PgEventRepository,
        PgExecutionAccountRepository, PgExecutionSubmissionRepository, PgFactorRepository,
        PgFeatureParityRepository, PgMarketRepository, PgMarketSelectionRepository,
        PgModelRegistryRepository, PgModelRunRepository, PgOrderIntentRepository,
        PgPolicyRepository, PgPositionRepository, PgRuntimeControlRepository,
        PgTradePolicyRepository, PgTrainingDatasetRepository,
    },
    traits::{
        CalibrationArtifactRepository, EntryConditionRepository, EventRepository,
        ExecutionAccountRepository, ExecutionSubmissionRepository, FactorRepository,
        FeatureParityRepository, MarketRepository, MarketSelectionRepository,
        ModelRegistryRepository, ModelRunRepository, OrderIntentRepository, PolicyRepository,
        PositionRepository, RuntimeControlRepository, TradePolicyRepository,
        TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    factors::{FactorEngine, FactorValue, NormalizedFactor},
    features::ExecutableFeatureSchema,
    hashing::ResearchHasher,
    model::{CalibratedReturnModel, ModelArtifact, ReturnModelSpec},
    portfolio::CapitalTimeBucketContract,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, Statement,
};

use crate::postgres::PostgresClock;

use super::{
    SelectorFixture,
    catalog_fixtures::{make_event, make_market},
    model_serving_fixtures::{
        ModelArtifactFixtureSeed, ModelBindingFixture, ModelDatasetLedgerFixture,
        ModelDatasetLedgerSeed, ModelPayloadFixture, ModelVersionFixture, SealedModelFixture,
    },
    model_spec_fixtures::new_model_spec_fixture,
    policy_fixtures::bootstrap_policy_bundle,
    report_fixtures,
    report_lifecycle_seed::{materialize_report_facts, persist_and_publish_report},
    seeded_uuid,
    trade_policy_fixtures::{PublishedTradePolicyFixture, PublishedTradePolicyFixtureInput},
};

/// Total cash budget: 40 shares * 0.60 gross price + 1.00 observed fee.
/// Weather `SemiAuto` fixtures use the profile's only governed cash-budget tier.
pub const EXECUTION_NOTIONAL: Decimal = dec!(25);
/// Venue-confirmed shares derived from the prepared order's gross cash amount.
pub const ENTRY_FILLED_SHARES: Decimal = dec!(40);
/// Reports used by the feedback keyset scale fixture.
pub const FEEDBACK_SCALE_REPORT_COUNT: usize = 11;
/// Recommendations retained in each scaled report.
pub const FEEDBACK_SCALE_PER_REPORT: usize = 1_000;
/// Total recommendations admitted by the scaled feedback fixture.
pub const FEEDBACK_SCALE_TOTAL: usize = FEEDBACK_SCALE_REPORT_COUNT * FEEDBACK_SCALE_PER_REPORT;
const ENTRY_GROSS_USD: Decimal = dec!(24);
const ENTRY_FEE_USD: Decimal = dec!(1);
const ENTRY_PRICE: Decimal = dec!(0.6);

/// Derive the catalog's opposite token as a deterministic canonical decimal U256.
#[must_use]
pub fn fixture_no_token_id(market_id: &str, token_id: &str) -> TokenId {
    let identity = format!("catalog-token:no:{market_id}:{token_id}");
    TokenId::new(seeded_uuid(&identity).as_u128().to_string())
}

#[must_use]
pub fn fixture_execution_account() -> NewExecutionAccount {
    let funder = EvmAddress::parse("0x1111111111111111111111111111111111111111")
        .expect("fixture execution account address");
    NewExecutionAccount::build(
        137,
        funder.clone(),
        ExecutionWalletKind::Eoa,
        funder.clone(),
        funder,
        None,
        None,
    )
    .expect("fixture execution account identity")
}

pub async fn ensure_fixture_execution_account(db: &DatabaseConnection) -> ExecutionAccountId {
    PgExecutionAccountRepository::new(db.clone())
        .ensure(fixture_execution_account())
        .await
        .expect("persist fixture execution account")
        .execution_account_id
}

/// Explicitly close the fail-closed kill switch for integration tests
/// that exercise risk-increasing entry paths.
pub async fn enable_test_admission(db: &DatabaseConnection, actor: &str) {
    let repository = PgRuntimeControlRepository::new(db.clone());
    let current = repository
        .load()
        .await
        .expect("load runtime control for risk-increasing test");
    let state = repository
        .compare_and_set(RuntimeControlUpdate {
            expected_revision: current.revision,
            quant_runtime_mode: None,
            settlement_write_policy: None,
            kill_switch_state: Some(KillSwitchState::Closed),
            kill_switch_requires_ack: Some(false),
            actor: actor.to_owned(),
            reason: "explicitly enable risk-increasing integration test".to_owned(),
        })
        .await
        .expect("explicitly close kill switch for risk-increasing test");
    assert_eq!(state.kill_switch_state, KillSwitchState::Closed);
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
    pub execution_account: ExecutionAccountId,
    pub data_quality_snapshot: ReportDataQualitySnapshotId,
    pub portfolio_plan: PortfolioPlanId,
    pub report: RecommendationReportId,
    pub recommendation: RecommendationId,
    pub condition_instance: EntryConditionInstanceId,
    pub model_version: ModelVersionId,
    pub calibration_artifact: CalibrationArtifactId,
    pub model_run: ModelRunId,
    pub market_selection: MarketSelectionId,
    pub decision_policy_snapshot: DecisionPolicySnapshotId,
    pub trade_policy: TradePolicyCohortProvenance,
    pub factor_serving_plane: FactorServingPlane,
    pub market: String,
    pub event: String,
    pub token: String,
}

/// Shared model/runtime lineage for multiple demo reports (one model spec).
pub struct SharedDemoInfra {
    pub feature_parity_state_id: FeatureParityStateId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_version_id: ModelVersionId,
    pub calibration_artifact_id: CalibrationArtifactId,
    pub model_run_id: ModelRunId,
    pub trade_policy: TradePolicyCohortProvenance,
    pub factor_serving_plane: FactorServingPlane,
}

/// Template lineage plus the preselected model identity owned by the
/// governed-feedback production fixture.
pub struct GovernedFeedbackInfra {
    pub template: SharedDemoInfra,
    pub champion_model_version_id: ModelVersionId,
}

/// Explicit production-stack workset and financial limits for one governed
/// feedback serving fixture.
pub struct FeedbackServingFixtureConfig {
    pub required_shadow_window_secs: u64,
    pub shadow_diff_threshold: Decimal,
    pub feedback_budget_usd: Decimal,
    pub outcome_reconciliation_enabled: bool,
    pub ad_hoc_report_enabled: bool,
}

impl FeedbackServingFixtureConfig {
    fn apply_runtime_controls(&self, snapshot: &mut DecisionPolicySnapshot) {
        snapshot.operations_policy.outcome_reconciliation.enabled =
            self.outcome_reconciliation_enabled;
        if self.outcome_reconciliation_enabled {
            snapshot.operations_policy.outcome_reconciliation.sweep_secs = 1;
        }
        snapshot
            .profile_artifacts
            .research_method
            .model_promotion
            .required_shadow_window_secs = self.required_shadow_window_secs;
        snapshot.execution_risk.portfolio = self.portfolio_policy();
        snapshot.recommendation.selection.enabled_categories = Vec::new();
        snapshot.recommendation.reports.ad_hoc_report_enabled = self.ad_hoc_report_enabled;
        for schedule in &mut snapshot.report_schedule.schedules {
            schedule.enabled = false;
        }
        snapshot.model_routing.model.shadow_diff_threshold =
            DecimalValue::new(self.shadow_diff_threshold);
    }

    fn portfolio_policy(&self) -> PortfolioConfig {
        let profile = fixture_profile_ref()
            .resolve_builtin_research_profile()
            .expect("resolve governed-feedback ResearchProfile");
        let cash_budget_tier = profile
            .spec
            .allowed_cash_budget_tiers
            .iter()
            .copied()
            .max()
            .expect("governed-feedback ResearchProfile cash-budget tier");
        let cash_reserve = self.feedback_budget_usd * dec!(0.10);
        let max_open_capital = self.feedback_budget_usd - cash_reserve;
        let total_budget = self.feedback_budget_usd;
        let mut portfolio = PortfolioConfig::default();
        let tier_capacity = cash_budget_tier
            .inner()
            .checked_mul(Decimal::from(
                portfolio.exposure_limits.max_open_recommendations,
            ))
            .expect("governed-feedback tier capacity must fit Decimal");
        assert!(
            max_open_capital >= tier_capacity,
            "governed-feedback budget must fund every concurrently open ResearchProfile cash tier after reserve"
        );
        portfolio.budget.total_budget_usd = DecimalValue::new(total_budget);
        portfolio.budget.cash_reserve_usd = DecimalValue::new(cash_reserve);
        portfolio.budget.max_open_capital_usd = DecimalValue::new(max_open_capital);

        // Use the exact immutable cash tier fitted into the serving Trade
        // Policy. A fixture-only dollar cap would make every venue-executable
        // tier ineligible before optimization and would no longer exercise the
        // production report path it claims to verify.
        portfolio.exposure_limits.max_single_recommendation_usd =
            DecimalValue::new(cash_budget_tier.inner());
        let governed_capacity = tier_capacity;
        portfolio.exposure_limits.max_market_exposure_usd = DecimalValue::new(governed_capacity);
        portfolio.exposure_limits.max_event_exposure_usd = DecimalValue::new(governed_capacity);
        portfolio.exposure_limits.max_category_exposure_usd = DecimalValue::new(governed_capacity);
        portfolio.exposure_limits.max_route_exposure_usd = DecimalValue::new(governed_capacity);

        // Every fixture position is a fully funded long outcome token. Its
        // worst one-horizon loss, including executable fees, is bounded by
        // entry cash, so the concurrent tier capacity is the exact conservative
        // aggregate cap for scenario loss, CVaR, and every disjoint lock bucket.
        // Drawdown is different: it is a peak-to-current wealth-path loss and
        // can accumulate across sequentially settled/reopened positions. In the
        // self-financing, no-leverage replay its exact absolute ceiling is the
        // funded total budget, not the concurrent open-position capacity.
        portfolio.tail_risk.max_cvar_usd = DecimalValue::new(governed_capacity);
        portfolio.tail_risk.max_scenario_loss_usd = DecimalValue::new(governed_capacity);
        portfolio.tail_risk.max_drawdown_usd = DecimalValue::new(total_budget);
        for (bucket, cap) in portfolio
            .tail_risk
            .capital_time_buckets
            .iter_mut()
            .zip([governed_capacity; 3])
        {
            bucket.max_capital_usd = DecimalValue::new(cap);
        }
        portfolio
    }
}

/// Complete persisted lineage for one Route-owned calibrated serving model.
pub(super) struct SeededRouteModel {
    pub model_version_id: ModelVersionId,
    pub calibration_artifact_id: CalibrationArtifactId,
    pub model_run_id: ModelRunId,
    pub trade_policy: TradePolicyCohortProvenance,
    pub factor_serving_plane: FactorServingPlane,
}

/// Immutable inputs for one calibrated system-test model chain.
pub struct CalibratedModelSeed {
    pub model_version_id: ModelVersionId,
    pub training_dataset_id: TrainingDatasetId,
    pub training_input_hash: ContentHash,
    pub head: CalibratedModelHead,
}

/// Frozen alpha-head source for a calibrated system-test model.
#[derive(Clone)]
pub enum CalibratedModelHead {
    /// Use the exact head frozen in the governing policy snapshot.
    Policy,
    /// Build an explicit alpha simplex; every unlisted alpha factor receives zero weight.
    AlphaSimplex(BTreeMap<FactorName, Decimal>),
}

impl CalibratedModelHead {
    /// Canonical governed control head for feedback-closure fixtures.
    pub(super) fn feedback_control() -> Self {
        Self::alpha_simplex([
            (FactorName::new("momentum_ema_slope"), dec!(0.03)),
            (FactorName::new("momentum_macd"), dec!(0.15)),
            (FactorName::new("momentum_roc"), dec!(0.03)),
            (FactorName::new("momentum_vol_adjusted"), dec!(0.18)),
            (
                FactorName::new("struct.resolution_proximity_regime"),
                dec!(0.17),
            ),
            (FactorName::new("struct.reversal_after_shock"), dec!(0.44)),
        ])
        .expect("governed-feedback control head")
    }

    /// Validate and freeze an explicit alpha-factor simplex.
    pub fn alpha_simplex(weights: impl IntoIterator<Item = (FactorName, Decimal)>) -> Result<Self> {
        let mut simplex = BTreeMap::new();
        for (factor, weight) in weights {
            ensure!(
                Decimal::ZERO <= weight && weight <= Decimal::ONE,
                "fixture alpha weight for `{factor}` must be within [0, 1], got {weight}"
            );
            ensure!(
                simplex.insert(factor.clone(), weight).is_none(),
                "fixture alpha simplex duplicates factor `{factor}`"
            );
        }
        ensure!(!simplex.is_empty(), "fixture alpha simplex is empty");
        let total = simplex.values().copied().sum::<Decimal>();
        ensure!(
            total == Decimal::ONE,
            "fixture alpha simplex must sum exactly to 1, got {total}"
        );
        Ok(Self::AlphaSimplex(simplex))
    }

    fn resolve(
        &self,
        plane: &FactorServingPlane,
        policy: &FactorHeadConfig,
    ) -> Result<FactorHeadConfig> {
        let Self::AlphaSimplex(simplex) = self else {
            return Ok(policy.clone());
        };
        let mut matched = 0_usize;
        let mut config = policy.clone();
        config.alpha_seed_weights = plane
            .definitions()
            .iter()
            .filter(|revision| revision.definition().is_outcome_alpha())
            .map(|revision| {
                let name = &revision.definition().name;
                let weight = simplex.get(name).copied().unwrap_or(Decimal::ZERO);
                matched += usize::from(simplex.contains_key(name));
                (name.to_string(), DecimalValue::new(weight))
            })
            .collect();
        ensure!(
            matched == simplex.len(),
            "fixture alpha simplex references {} factor(s) outside the sealed plane",
            simplex.len() - matched
        );
        Ok(config)
    }
}

/// Exact parent → calibrator → derived-child fixture.
pub struct CalibratedModelFixture {
    sealed: SealedModelFixture,
    parent_model_version_id: ModelVersionId,
    calibration_artifact_id: CalibrationArtifactId,
    metrics: ModelVersionMetrics,
    training_objective: ModelTrainingObjective,
}

impl CalibratedModelFixture {
    /// Project the sealed derived child into its exact Candidate row.
    pub fn version(&self, model_spec_id: ModelSpecId, version: i32) -> NewModelVersion {
        let serving_contract = self.sealed.serving_contract().clone();
        let bindings = serving_contract.bindings();
        let category_scope = bindings.model.category_scope;
        let model_version_id = bindings.model.model_version_id;
        let profile_ref = bindings.model.profile_ref.clone();
        let training_dataset_id = bindings.dataset.manifest.training_dataset_id;
        let trade_policy = bindings
            .trade_policy
            .as_ref()
            .map(|binding| (binding.artifact_id, binding.content_hash));
        NewModelVersion {
            model_version_id,
            model_spec_id,
            version,
            artifact_hash: self.sealed.artifact_hash(),
            serving_contract,
            category_scope,
            profile_ref,
            training_dataset_id: Some(training_dataset_id),
            trade_policy_artifact_id: trade_policy.map(|binding| binding.0),
            trade_policy_hash: trade_policy.map(|binding| binding.1),
            derivation: ModelVersionDerivation::ReturnCalibration {
                parent_model_version_id: self.parent_model_version_id,
                calibration_artifact_id: self.calibration_artifact_id,
            },
            metrics: self.metrics.clone(),
            training_objective: self.training_objective.clone(),
        }
    }
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

/// Immutable member payload used to create one atomic market-selection snapshot.
pub struct ReportSelectionMemberSeed {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub category: MarketCategory,
    pub status: MarketStatus,
    pub primary_token_id: TokenId,
    pub secondary_token_id: Option<TokenId>,
    pub liquidity_usd: Option<Usd>,
    pub volume_24h_usd: Option<Usd>,
}

impl ReportSelectionMemberSeed {
    fn bind(self, market_selection_id: MarketSelectionId) -> NewMarketSelectionMember {
        NewMarketSelectionMember {
            market_selection_id,
            market_id: self.market_id,
            event_id: self.event_id,
            category: self.category,
            status: self.status,
            primary_token_id: self.primary_token_id,
            secondary_token_id: self.secondary_token_id,
            liquidity_usd: self.liquidity_usd,
            volume_24h_usd: self.volume_24h_usd,
        }
    }
}

/// Overrides when composing a [`NewReportTransaction`] for UI demo fixtures.
pub struct ReportBuildOptions {
    pub recommendations: Vec<NewRecommendation>,
    pub entry_condition_artifacts: Vec<NewEntryConditionArtifact>,
    pub summary: ReportSummary,
    pub as_of: DateTime<Utc>,
    pub runtime_mode: QuantRuntimeMode,
    /// Optional governed capital scale for historical fixtures that share the
    /// live account's equity high-water ledger.
    pub account_capital_usd: Option<Usd>,
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
            account_capital_usd: None,
        }
    }

    /// Published report with zero recommendations and an explicit empty reason.
    #[must_use]
    pub fn empty_report(ids: &ExecutionTxnIds) -> Self {
        Self {
            recommendations: Vec::new(),
            entry_condition_artifacts: Vec::new(),
            summary: empty_report_summary(),
            as_of: ids.decision_at,
            runtime_mode: QuantRuntimeMode::AutoExecution,
            account_capital_usd: None,
        }
    }
}

/// Seed runtime config + model registry once; reuse for many reports.
pub async fn seed_shared_demo_infra(db: &DatabaseConnection) -> SharedDemoInfra {
    Box::pin(seed_demo_inner(db, None)).await
}

/// Seed execution UI lineage with a loadable, calibrated model artifact.
pub async fn seed_demo_with_store(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
) -> SharedDemoInfra {
    Box::pin(seed_demo_inner(db, Some(artifact_store))).await
}

/// Reserve one exact Weather route and seed the model-template lineage used to
/// build its snapshot-bound research model before the production process starts.
pub async fn seed_feedback_serving_infra(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    config: FeedbackServingFixtureConfig,
) -> GovernedFeedbackInfra {
    let policy = PgPolicyRepository::new(db.clone());
    assert!(
        policy
            .load_current()
            .await
            .expect("load governed-feedback policy")
            .is_none(),
        "governed-feedback serving fixture requires a fresh policy database"
    );
    let champion_model_version_id = ModelVersionId::from_v7();
    let mut snapshot = DecisionPolicySnapshot::default();
    config.apply_runtime_controls(&mut snapshot);
    snapshot.model_routing.model.buy_routes.insert(
        BuyModelRoute::Weather,
        BuyRouteBinding {
            champion: ModelBinding::new(
                champion_model_version_id,
                ModelBindingSource::Bootstrap,
                Utc::now(),
                PolicyBundleGeneration::FIRST,
                1,
            ),
            shadow: None,
        },
    );
    let decision_policy_snapshot_id = bootstrap_policy_bundle(
        &policy,
        &snapshot,
        "governed-feedback-fixture",
        "publish an exact Weather serving route for governed feedback",
    )
    .await;
    let seeded = Box::pin(seed_model_version_named(
        db,
        SeedModelVersionInput {
            decision_policy_snapshot_id,
            model_version_id: champion_model_version_id,
            model_name: "governed-feedback-model",
            profile_ref: fixture_profile_ref(),
            artifact_store: Some(artifact_store),
            head: CalibratedModelHead::feedback_control(),
        },
    ))
    .await;
    GovernedFeedbackInfra {
        template: SharedDemoInfra {
            feature_parity_state_id: clear_parity_state(db).await,
            decision_policy_snapshot_id,
            model_version_id: seeded.model_version_id,
            calibration_artifact_id: seeded.calibration_artifact_id,
            model_run_id: seeded.model_run_id,
            trade_policy: seeded.trade_policy,
            factor_serving_plane: seeded.factor_serving_plane,
        },
        champion_model_version_id,
    }
}

async fn seed_demo_inner(
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

    let seeded = Box::pin(seed_model_version_named(
        db,
        SeedModelVersionInput {
            decision_policy_snapshot_id,
            model_version_id: ModelVersionId::from_v7(),
            model_name: "ui-demo-seed-model",
            profile_ref: fixture_profile_ref(),
            artifact_store,
            head: CalibratedModelHead::Policy,
        },
    ))
    .await;
    SharedDemoInfra {
        feature_parity_state_id: clear_parity_state(db).await,
        decision_policy_snapshot_id,
        model_version_id: seeded.model_version_id,
        calibration_artifact_id: seeded.calibration_artifact_id,
        model_run_id: seeded.model_run_id,
        trade_policy: seeded.trade_policy,
        factor_serving_plane: seeded.factor_serving_plane,
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
    let calibration_artifact_id = version
        .serving_contract
        .bindings()
        .model
        .calibration
        .as_ref()?
        .artifact_id;
    let artifact = PgTradePolicyRepository::new(db.clone())
        .find(&artifact_id)
        .await
        .ok()??;
    let cohort_key = artifact.payload_json.cohorts.first()?.key.clone();
    Some(SharedDemoInfra {
        feature_parity_state_id: clear_parity_state(db).await,
        decision_policy_snapshot_id: run.decision_policy_snapshot_id,
        model_version_id: version.model_version_id,
        calibration_artifact_id,
        model_run_id: run.model_run_id,
        trade_policy: TradePolicyCohortProvenance {
            artifact_id,
            artifact_hash,
            cohort_index: 0,
            cohort_key,
        },
        factor_serving_plane: version.serving_contract.bindings().factors.plane.clone(),
    })
}

async fn clear_parity_state(db: &DatabaseConnection) -> FeatureParityStateId {
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
    let ids = prepare_report_on_infra(db, infra, &config, db.statement_time().await).await;
    ids.complete_model_run(db).await;
    persist_and_publish_report(db, ids.build_report_transaction(), &config.trigger_key, 10).await;
    ids
}

/// Seed a report with a production-format S3 bundle and exact `ClickHouse` facts.
pub async fn seed_production_report(
    db: &DatabaseConnection,
    clickhouse: &ClickHouseConfig,
    artifacts: &Arc<dyn ArtifactStore>,
    infra: &SharedDemoInfra,
    config: ReportSeedConfig,
) -> Result<ExecutionTxnIds> {
    let ids = prepare_report_on_infra(db, infra, &config, db.statement_time().await).await;
    let mut transaction = ids.build_report_transaction();
    materialize_report_facts(artifacts, clickhouse, &mut transaction).await?;
    ids.complete_model_run(db).await;
    persist_and_publish_report(db, transaction, &config.trigger_key, 10).await;
    Ok(ids)
}

/// Seed a report whose recommendation waits for a continuously satisfied
/// executable-price condition. The artifact, recommendation reference, and
/// durable instance are committed by the report transaction.
pub async fn seed_price_report(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    config: ReportSeedConfig,
) -> ExecutionTxnIds {
    seed_price_by_mode(db, infra, config, QuantRuntimeMode::AutoExecution).await
}

/// Seed the same durable conditional evidence graph under `ReportOnly`, where
/// evaluation is active but intent creation and venue submission are forbidden.
pub async fn seed_report_price(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    config: ReportSeedConfig,
) -> ExecutionTxnIds {
    seed_price_by_mode(db, infra, config, QuantRuntimeMode::ReportOnly).await
}

async fn seed_price_by_mode(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    config: ReportSeedConfig,
    runtime_mode: QuantRuntimeMode,
) -> ExecutionTxnIds {
    let ids = prepare_report_on_infra(db, infra, &config, db.statement_time().await).await;
    let mut options = ReportBuildOptions::published_single(&ids);
    options.runtime_mode = runtime_mode;
    let artifact = ids.price_condition_artifact();
    let recommendation = options
        .recommendations
        .first_mut()
        .expect("conditional report recommendation");
    let condition = EntryConditionPlan::Conditional {
        artifact_id: artifact.artifact_id,
        content_hash: artifact.content_hash,
    };
    recommendation.trade_plan.entry.condition = condition;
    "wait for executable ask at or below 0.62"
        .clone_into(&mut recommendation.trade_plan.entry.entry_reason);
    options.entry_condition_artifacts.push(artifact);
    ids.complete_model_run(db).await;
    persist_and_publish_report(
        db,
        build_report_transaction_inner(&ids, options),
        &config.trigger_key,
        10,
    )
    .await;
    ids
}

/// Prepare catalog and immutable report lineage without committing the report.
///
/// Feedback system tests use this boundary to persist the exact serving feature
/// evidence before the recommendation transaction makes that evidence visible.
pub async fn prepare_report_on_infra(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    config: &ReportSeedConfig,
    decision_at: DateTime<Utc>,
) -> ExecutionTxnIds {
    let no_token_id = fixture_no_token_id(&config.market_id, &config.token_id);
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
    prepare_report_lineage(
        db,
        infra,
        config,
        decision_at,
        vec![ReportSelectionMemberSeed {
            market_id: MarketId::new(&config.market_id),
            event_id: EventId::new(&config.event_id),
            category: MarketCategory::Weather,
            status: MarketStatus::Active,
            primary_token_id: TokenId::new(&config.token_id),
            secondary_token_id: Some(no_token_id),
            liquidity_usd: Some(Usd::new(dec!(5000))),
            volume_24h_usd: Some(Usd::new(dec!(10000))),
        }],
    )
    .await
}

/// Prepare immutable report lineage after the complete catalog membership is present.
pub async fn prepare_report_lineage(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    config: &ReportSeedConfig,
    decision_at: DateTime<Utc>,
    selection_members: Vec<ReportSelectionMemberSeed>,
) -> ExecutionTxnIds {
    let market_selection_id = seed_market_selection(
        db,
        &infra.decision_policy_snapshot_id,
        decision_at,
        selection_members,
    )
    .await;
    finish_report_lineage(db, infra, config, decision_at, market_selection_id).await
}

/// Prepare immutable report lineage from an already materialized production
/// selection model.
pub async fn prepare_report_lineage_model(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    config: &ReportSeedConfig,
    decision_at: DateTime<Utc>,
    selection: MarketSelectionModel,
) -> ExecutionTxnIds {
    let market_selection_id = selection.snapshot.market_selection_id;
    assert_eq!(
        selection.snapshot.decision_at, decision_at,
        "report selection decision time must match its report lineage"
    );
    assert_eq!(
        selection.snapshot.decision_policy_snapshot_id, infra.decision_policy_snapshot_id,
        "report selection policy must match its report lineage"
    );
    PgMarketSelectionRepository::new(db.clone())
        .create_snapshot(selection.snapshot, selection.members)
        .await
        .expect("persist exact market selection");
    finish_report_lineage(db, infra, config, decision_at, market_selection_id).await
}

async fn finish_report_lineage(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
    config: &ReportSeedConfig,
    decision_at: DateTime<Utc>,
    market_selection_id: MarketSelectionId,
) -> ExecutionTxnIds {
    let execution_account = ensure_fixture_execution_account(db).await;
    let model_run_id = seed_report_model_run(db, infra, &market_selection_id, decision_at).await;
    ExecutionTxnIds {
        decision_at,
        feature_parity_state_id: infra.feature_parity_state_id,
        account_snapshot: AccountSnapshotId::from_v7(),
        execution_account,
        data_quality_snapshot: ReportDataQualitySnapshotId::from_v7(),
        portfolio_plan: PortfolioPlanId::from_v7(),
        report: RecommendationReportId::from_v7(),
        recommendation: RecommendationId::from_v7(),
        condition_instance: EntryConditionInstanceId::from_v7(),
        model_version: infra.model_version_id,
        calibration_artifact: infra.calibration_artifact_id,
        model_run: model_run_id,
        market_selection: market_selection_id,
        decision_policy_snapshot: infra.decision_policy_snapshot_id,
        trade_policy: infra.trade_policy.clone(),
        factor_serving_plane: infra.factor_serving_plane.clone(),
        market: config.market_id.clone(),
        event: config.event_id.clone(),
        token: config.token_id.clone(),
    }
}

/// Expand the prepared report set to more than ten thousand recommendations.
///
/// The inserted rows intentionally retain the production recommendation schema
/// while referring to unresolved catalog markets, so the feedback cohort must
/// page and classify every row without attempting to materialize censored rows.
pub async fn seed_feedback_scale(
    db: &DatabaseConnection,
    reports: &[ExecutionTxnIds],
    window_start: DateTime<Utc>,
    report_cutoff: DateTime<Utc>,
) {
    assert_eq!(
        reports.len(),
        FEEDBACK_SCALE_REPORT_COUNT,
        "feedback scale fixture requires its canonical report count"
    );
    let profile_artifact_id = fixture_profile_ref().artifact_id();
    let shared_event_id = EventId::new(&reports[0].event);
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        r"
        INSERT INTO market (
            market_id, event_id, question, slug, description, categories, status,
            filter_reasons, outcome, yes_token_id, no_token_id, tick_size, neg_risk,
            start_date, end_date, resolved_at, content_hash, created_at, updated_at
        )
        SELECT
            'feedback-scale-market-' || ordinal,
            template.event_id,
            'Feedback scale market ' || ordinal,
            'feedback-scale-market-' || ordinal,
            template.description,
            template.categories,
            template.status,
            template.filter_reasons,
            template.outcome,
            'feedback-scale-yes-' || ordinal,
            'feedback-scale-no-' || ordinal,
            template.tick_size,
            template.neg_risk,
            template.start_date,
            template.end_date,
            template.resolved_at,
            template.content_hash,
            template.created_at,
            template.updated_at
        FROM market AS template
        CROSS JOIN generate_series(2, 1000) AS ordinal
        WHERE template.market_id = $1
        ",
        [reports[1].market.clone().into()],
    ))
    .await
    .expect("seed scale catalog FK rows");
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        r"
        UPDATE quant_recommendation_report
        SET top_n = 1000
        WHERE decision_at >= $1
          AND decision_at <= $2
        ",
        [window_start.into(), report_cutoff.into()],
    ))
    .await
    .expect("align scale report cardinality");
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            r"
            WITH templates AS (
                SELECT recommendation.*
                FROM quant_recommendation AS recommendation
                INNER JOIN quant_recommendation_report AS report
                    ON report.recommendation_report_id =
                       recommendation.recommendation_report_id
                INNER JOIN quant_report_route_run AS route_run
                    ON route_run.report_route_run_id =
                       recommendation.report_route_run_id
                WHERE route_run.research_profile_artifact_id = $1
                  AND recommendation.rank = 1
                  AND report.decision_at >= $2
                  AND report.decision_at <= $3
            )
            INSERT INTO quant_recommendation (
                recommendation_id, recommendation_report_id,
                report_route_run_id, portfolio_plan_id, economic_tier_id,
                rank, route, market_id, event_id, token_id, outcome_side,
                economics_json, economic_tier_json, identity, market_context,
                trade_plan, factor_breakdown, evidence_refs, execution_eligibility,
                valid_from, valid_until, status, status_changed_at, created_at
            )
            SELECT
                md5(template.recommendation_report_id::text || ':' || ordinal)::uuid,
                template.recommendation_report_id,
                template.report_route_run_id,
                template.portfolio_plan_id,
                md5(template.recommendation_report_id::text || ':' || ordinal || ':tier')::uuid,
                ordinal,
                template.route,
                'feedback-scale-market-' || ordinal,
                $4,
                'feedback-scale-yes-' || ordinal,
                template.outcome_side,
                template.economics_json,
                jsonb_set(
                    jsonb_set(
                        jsonb_set(
                            jsonb_set(
                                template.economic_tier_json,
                                '{economic_tier_id}',
                                to_jsonb((md5(template.recommendation_report_id::text || ':' || ordinal || ':tier')::uuid)::text)
                            ),
                            '{market_id}',
                            to_jsonb('feedback-scale-market-' || ordinal)
                        ),
                        '{event_id}',
                        to_jsonb($4::text)
                    ),
                    '{token_id}',
                    to_jsonb('feedback-scale-yes-' || ordinal)
                ),
                template.identity,
                template.market_context,
                template.trade_plan,
                template.factor_breakdown,
                template.evidence_refs,
                template.execution_eligibility,
                template.valid_from,
                template.valid_until,
                template.status,
                template.status_changed_at,
                template.created_at
            FROM templates AS template
            CROSS JOIN generate_series(2, 1000) AS ordinal
            ",
            [
                profile_artifact_id.into(),
                window_start.into(),
                report_cutoff.into(),
                shared_event_id.into(),
            ],
        ))
        .await
        .expect("seed scale recommendations");
    assert_eq!(
        usize::try_from(result.rows_affected()).expect("bounded affected rows"),
        FEEDBACK_SCALE_REPORT_COUNT * (FEEDBACK_SCALE_PER_REPORT - 1)
    );
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
    let runs = PgModelRunRepository::new(db.clone());
    runs.create(NewModelRun {
        model_run_id,
        run_kind: ModelRunKind::LiveInference,
        model_version_id: Some(infra.model_version_id),
        decision_policy_snapshot_id: infra.decision_policy_snapshot_id,
        market_selection_id: Some(*market_selection_id),
        window_start: decision_at,
        window_end: decision_at,
        input_hash,
    })
    .await
    .expect("create report fixture model run");
    model_run_id
}

impl ExecutionTxnIds {
    /// Complete the prepared inference run after all feature/factor evidence is durable.
    pub async fn complete_model_run(&self, db: &DatabaseConnection) {
        let output_hash = ResearchHasher::canonical(&(
            "execution_report_fixture_model_output_v1",
            &self.model_run,
            &self.market_selection,
        ))
        .expect("hash report fixture model output");
        PgModelRunRepository::new(db.clone())
            .succeed(&self.model_run, output_hash, None)
            .await
            .expect("finish report fixture model run");
    }

    /// Exact governed factor values referenced by this fixture recommendation.
    #[must_use]
    pub fn factor_values(&self) -> Vec<FactorValue> {
        self.factor_serving_plane
            .definitions()
            .iter()
            .map(|revision| {
                let definition = revision.definition();
                let raw_value = dec!(0.5);
                let value = FactorValue {
                    definition_id: revision.factor_definition_id(),
                    name: definition.name.clone(),
                    family: definition.family,
                    raw_value: Some(raw_value),
                    normalization: NormalizedFactor::Scored {
                        score: Probability::new(dec!(0.5)),
                        source: match definition.normalization {
                            FactorNormalization::MinMax => NormalizationSource::PerMarket,
                            FactorNormalization::WinsorizedZScore | FactorNormalization::Rank => {
                                NormalizationSource::CrossSection
                            }
                        },
                        clamp: None,
                    },
                    direction: definition
                        .contribution_direction(raw_value)
                        .expect("fixture factor raw value must match output semantics"),
                    confidence: Probability::new(dec!(0.75)),
                    explanation: FactorExplanation {
                        headline: format!("fixture {}", definition.name),
                        drivers: Vec::new(),
                    },
                    input_feature_refs: definition.input_features.clone(),
                };
                value
                    .validate_against(revision)
                    .expect("fixture factor value must match its exact revision");
                value
            })
            .collect()
    }
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
    let created_at = Utc::now();
    let token_id = TokenId::new(token_id);
    let economic_tier_id = EconomicTierId::new(recommendation_id.as_uuid());
    let report_route_run_id = ReportRouteRunId::new(report_id.as_uuid());
    let economics = fixture_recommendation_economics();
    let evidence_refs = ids.evidence_refs(&token_id);
    let economic_tier = fixture_economic_tier(
        economic_tier_id,
        report_route_run_id,
        evidence_refs.signal_candidate_id,
        market_id,
        event_id,
        token_id.clone(),
        economics,
    );
    NewRecommendation {
        recommendation_id,
        recommendation_report_id: report_id,
        report_route_run_id,
        portfolio_plan_id: ids.portfolio_plan,
        economic_tier_id,
        rank,
        route: BuyModelRoute::Weather,
        market_id: MarketId::new(market_id),
        event_id: EventId::new(event_id),
        token_id,
        outcome_side: OutcomeSide::Yes,
        economics_json: economics,
        economic_tier_json: economic_tier,
        identity: recommendation_identity(),
        market_context: market_context(),
        trade_plan: trade_plan(&ids.trade_policy, economic_tier_id),
        factor_breakdown: ids.factor_breakdown(),
        evidence_refs,
        execution_eligibility: execution_eligibility(),
        valid_from: created_at,
        valid_until: created_at + Duration::hours(1),
        status: RecommendationStatus::Prepared,
        created_at,
    }
}

const fn fixture_recommendation_economics() -> RecommendationEconomics {
    RecommendationEconomics {
        profit_probability_bps: Bps::new(dec!(7000)),
        nominal_expected_net_usd: Usd::new(dec!(19.5)),
        robust_expected_net_usd: Usd::new(dec!(11.5)),
        max_loss_usd: Usd::new(dec!(25)),
        cvar_contribution_usd: Usd::new(dec!(25)),
        capital_occupancy_usd_hours: UsdHours::new(dec!(25)),
        marginal_portfolio_value_usd: Usd::new(dec!(11.5)),
    }
}

fn fixture_economic_tier(
    economic_tier_id: EconomicTierId,
    report_route_run_id: ReportRouteRunId,
    candidate_id: SignalCandidateId,
    market_id: &str,
    event_id: &str,
    token_id: TokenId,
    economics: RecommendationEconomics,
) -> ExecutableEconomicTier {
    let shares = Shares::new(ENTRY_FILLED_SHARES);
    let immediate_cost = ImmediateExecutionCost::new(
        Usd::new(ENTRY_FILLED_SHARES * ENTRY_PRICE),
        Usd::new(ENTRY_FEE_USD),
        Usd::ZERO,
    )
    .expect("valid execution fixture cost");
    ExecutableEconomicTier {
        economic_tier_id,
        report_route_run_id,
        candidate_id,
        tier_ordinal: 1,
        route: BuyModelRoute::Weather,
        market_id: MarketId::new(market_id),
        event_id: EventId::new(event_id),
        category: MarketCategory::Weather,
        token_id,
        outcome_side: OutcomeSide::Yes,
        entry_execution: EntryExecutionEconomics::Aggressive(AggressiveEntryEconomics {
            requested_shares: shares,
            filled_shares: shares,
            limit_price: Price::new(ENTRY_PRICE),
            entry_vwap: Price::new(ENTRY_PRICE),
            immediate_cost,
            slippage_usd: Usd::ZERO,
            visible_liquidity_usd: Usd::new(dec!(5000)),
        }),
        profit_probability_lower_bps: 6_500,
        probability_interval_width_bps: 1_000,
        scenario_cashflows: vec![
            ScenarioExecutionCashflow {
                scenario_index: 0,
                entry_execution: ScenarioEntryExecution::AggressiveFill,
                filled_shares: shares,
                immediate_cash_outlay_usd: immediate_cost.cash_outlay_usd,
                discounted_exit_cash_usd: immediate_cost.cash_outlay_usd + Usd::new(dec!(50)),
                delayed_maker_rebate_usd: Usd::ZERO,
                discounted_maker_rebate_usd: Usd::ZERO,
                capital_cost_usd: Usd::ZERO,
                capital_occupancy: vec![ScenarioCapitalOccupancySlice {
                    locked_cash_usd: immediate_cost.cash_outlay_usd,
                    duration_secs: 3_600,
                }],
                discounted_net_usd: Usd::new(dec!(50)),
                risk_net_usd: Usd::new(dec!(50)),
            },
            ScenarioExecutionCashflow {
                scenario_index: 1,
                entry_execution: ScenarioEntryExecution::AggressiveFill,
                filled_shares: shares,
                immediate_cash_outlay_usd: immediate_cost.cash_outlay_usd,
                discounted_exit_cash_usd: Usd::ZERO,
                delayed_maker_rebate_usd: Usd::ZERO,
                discounted_maker_rebate_usd: Usd::ZERO,
                capital_cost_usd: Usd::ZERO,
                capital_occupancy: vec![ScenarioCapitalOccupancySlice {
                    locked_cash_usd: immediate_cost.cash_outlay_usd,
                    duration_secs: 3_600,
                }],
                discounted_net_usd: Usd::new(dec!(-25)),
                risk_net_usd: Usd::new(dec!(-25)),
            },
            ScenarioExecutionCashflow {
                scenario_index: 2,
                entry_execution: ScenarioEntryExecution::AggressiveFill,
                filled_shares: shares,
                immediate_cash_outlay_usd: immediate_cost.cash_outlay_usd,
                discounted_exit_cash_usd: immediate_cost.cash_outlay_usd + Usd::new(dec!(10)),
                delayed_maker_rebate_usd: Usd::ZERO,
                discounted_maker_rebate_usd: Usd::ZERO,
                capital_cost_usd: Usd::ZERO,
                capital_occupancy: vec![ScenarioCapitalOccupancySlice {
                    locked_cash_usd: immediate_cost.cash_outlay_usd,
                    duration_secs: 3_600,
                }],
                discounted_net_usd: Usd::new(dec!(10)),
                risk_net_usd: Usd::new(dec!(10)),
            },
        ],
        hard_reservation_envelope: vec![
            HardReservationBucket {
                end_secs: 3_600,
                reserved_cash_usd: immediate_cost.cash_outlay_usd,
            },
            HardReservationBucket {
                end_secs: 86_400,
                reserved_cash_usd: Usd::ZERO,
            },
            HardReservationBucket {
                end_secs: 604_800,
                reserved_cash_usd: Usd::ZERO,
            },
        ],
        economics,
        lineage_hash: content_hash('l'),
    }
}

/// Seed runtime config, catalog, model lineage, market selection, and a published report.
pub async fn seed_report_fixture(db: &DatabaseConnection) -> ExecutionTxnIds {
    let infra = Box::pin(seed_shared_demo_infra(db)).await;
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

/// Seed a report whose token identity is a canonical Polymarket `uint256`.
/// Settlement contract tests require the catalog, intent, fill, and CTF token
/// lineage to remain identical end to end.
pub async fn seed_settlement_report_fixture(db: &DatabaseConnection) -> ExecutionTxnIds {
    let infra = Box::pin(seed_shared_demo_infra(db)).await;
    seed_report_on_infra(
        db,
        &infra,
        ReportSeedConfig {
            event_id: "settlement-evt-1".to_owned(),
            market_id: "0xsettlement-market".to_owned(),
            market_question: "Will settlement complete?".to_owned(),
            market_slug: "will-settlement-complete".to_owned(),
            token_id: "12345".to_owned(),
            trigger_key: format!("scheduled:settlement:{}", RecommendationReportId::from_v7()),
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
        )
        .await
        .expect("create approved intent")
        .order_intent_id
}

/// Drive an approved intent's entry to a confirmed full fill: capital `Spent`,
/// one open lot (40 @ 0.60 gross + 1.00 fee), intent `Filled`.
pub async fn fill_entry_lot(
    db: &DatabaseConnection,
    submission: &PgExecutionSubmissionRepository,
    ids: &ExecutionTxnIds,
    intent_id: &OrderIntentId,
) {
    claim_entry_for_test(db, submission, intent_id).await;
    let order = submission
        .create_entry_order(
            new_execution_order(intent_id, ids),
            &ids.feature_parity_state_id,
        )
        .await
        .expect("create entry order");
    let filled_at = db.statement_time().await;
    submission
        .record_submission_result(
            &order.execution_order_id,
            SubmissionLedgerWrite {
                identity_refs: empty_identity_refs(),
                state: ExecutionOrderState::Filled,
                intent_status: OrderIntentStatus::Filled,
                venue_order_id: Some(OrderId::new("venue-entry")),
                venue_status: Some(VenueOrderStatus::Filled),
                submitted_at: filled_at,
                filled_at: Some(filled_at),
                cancelled_at: None,
                error_message: None,
                capital: CapitalSettlement::SettleFull {
                    spent_usd: Usd::new(EXECUTION_NOTIONAL),
                },
                fill: Some(position_fill(ids, intent_id, filled_at)),
                reconciliation: Some(reconciliation_row(
                    &order.execution_order_id,
                    intent_id,
                    filled_at,
                )),
            },
        )
        .await
        .expect("record entry fill");
}

/// Settle one real partial exchange exit against the current per-intent lot.
pub async fn partial_exit_lot(
    db: &DatabaseConnection,
    submission: &PgExecutionSubmissionRepository,
    ids: &ExecutionTxnIds,
    intent_id: &OrderIntentId,
    shares: Shares,
    average_price: Price,
) {
    let position = PgPositionRepository::new(db.clone())
        .find_by_intent(intent_id)
        .await
        .expect("load position before partial exit")
        .expect("partial exit position exists");
    assert!(
        shares.is_positive() && shares < position.shares,
        "partial exit shares must be strictly inside the open lot"
    );
    let proportional_cost =
        Usd::new(position.cost_usd.inner() * shares.inner() / position.shares.inner());
    let proceeds_usd = shares * average_price;
    let realized_pnl_usd = proceeds_usd - proportional_cost;
    let exit = submission
        .create_exit_order(
            exit_order(intent_id, ids, shares.inner(), average_price.inner()),
            ExitReason::PartialExit,
            Some(PendingScaleOut {
                target_id: None,
                target_cumulative_exit_pct: shares.inner() / position.shares.inner(),
            }),
        )
        .await
        .expect("create partial exit order");
    let exited_at = db.statement_time().await;
    submission
        .record_exit_result(
            &exit.execution_order_id,
            ExitLedgerWrite {
                identity_refs: empty_identity_refs(),
                order_state: ExecutionOrderState::Filled,
                venue_order_id: Some(OrderId::new("venue-exit-partial")),
                venue_status: Some(VenueOrderStatus::Filled),
                filled_at: Some(exited_at),
                cancelled_at: None,
                error_message: None,
                exit_state: ExitState::PartiallyExited,
                exit_reason: ExitReason::PartialExit,
                position_exit: Some(PositionExit {
                    shares,
                    avg_price: average_price,
                    proceeds_usd,
                    realized_pnl_usd,
                    exited_at,
                    reason: ExitReason::PartialExit,
                }),
                fully_exited: false,
                revert_to_open: false,
                reconciliation: Some(exit_reconciliation_row(
                    &exit.execution_order_id,
                    intent_id,
                    shares,
                    average_price,
                    exited_at,
                )),
            },
        )
        .await
        .expect("record partial exit");
}

/// Full exit flow: entry fill then exit fill at 0.55 (realized -3), position `Closed`.
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
            .touch_exit_monitor(intent_id, db.statement_time().await, Some(peak), None, None)
            .await
            .expect("seed peak mark price");
    }

    let exit = submission
        .create_exit_order(
            exit_order(intent_id, ids, ENTRY_FILLED_SHARES, dec!(0.55)),
            ExitReason::StopLoss,
            None,
        )
        .await
        .expect("exit order");

    let exited_at = db.statement_time().await;
    submission
        .record_exit_result(
            &exit.execution_order_id,
            ExitLedgerWrite {
                identity_refs: empty_identity_refs(),
                order_state: ExecutionOrderState::Filled,
                venue_order_id: Some(OrderId::new("venue-exit")),
                venue_status: Some(VenueOrderStatus::Filled),
                filled_at: Some(exited_at),
                cancelled_at: None,
                error_message: None,
                exit_state: ExitState::Exited,
                exit_reason: ExitReason::StopLoss,
                position_exit: Some(PositionExit {
                    shares: Shares::new(ENTRY_FILLED_SHARES),
                    avg_price: Price::new(dec!(0.55)),
                    proceeds_usd: Usd::new(dec!(22)),
                    realized_pnl_usd: Usd::new(dec!(-3)),
                    exited_at,
                    reason: ExitReason::StopLoss,
                }),
                fully_exited: true,
                revert_to_open: false,
                reconciliation: Some(exit_reconciliation_row(
                    &exit.execution_order_id,
                    intent_id,
                    Shares::new(ENTRY_FILLED_SHARES),
                    Price::new(dec!(0.55)),
                    exited_at,
                )),
            },
        )
        .await
        .expect("record exit");
}

/// Empty placement identity set for fixtures that exercise accounting only.
pub fn empty_identity_refs() -> ExecutionIdentityRefs {
    ExecutionIdentityRefs {
        trade_ids: Vec::new(),
        transaction_hashes: Vec::new(),
        observed_at: Utc::now(),
    }
}

impl ExecutionTxnIds {
    pub fn report_operation_log(&self) -> NewOperationLog {
        NewOperationLog {
            id: OperationLogId::from_v7(),
            request_id: format!("scheduled:test:{}", self.report).into(),
            actor_user_id: None,
            actor_username: Some("system".to_owned()),
            acting_role: Some("test".into()),
            category: OperationCategory::QuantReport,
            action: "publish".into(),
            resource_type: Some(ResourceType::QuantReport),
            resource_id: Some(self.report.to_string()),
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
}

pub(super) fn new_order_intent(
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
        execution_account_id: ids.execution_account,
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
            maker_rebate_schedule: None,
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
                require_route_gate_eligibility: true,
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

pub(super) fn new_capital_allocation(
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
            profile.spec.category == Some(MarketCategory::Weather)
                && profile.spec.feature_contract.requires_l2()
        })
        .expect("Weather ResearchProfile")
        .profile_ref
}

pub fn prepared_order(
    token_id: TokenId,
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
        token_id,
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
        maker_rebate_schedule: None,
        prepared_at: now,
        valid_until: now + Duration::hours(1),
    }
}

pub(super) fn new_execution_order(
    intent_id: &OrderIntentId,
    ids: &ExecutionTxnIds,
) -> NewExecutionOrder {
    NewExecutionOrder {
        execution_order_id: ExecutionOrderId::from_v7(),
        order_intent_id: *intent_id,
        order_phase: ExecutionOrderPhase::Entry,
        market_id: MarketId::new(&ids.market),
        token_id: TokenId::new(&ids.token),
        side: Side::Buy,
        order_type: OrderTypeKind::Fak,
        price: Price::new(ENTRY_PRICE),
        shares: Shares::new(ENTRY_FILLED_SHARES),
        cost_usd: Usd::new(EXECUTION_NOTIONAL),
        prepared_order_json: prepared_order(
            TokenId::new(&ids.token),
            Side::Buy,
            OrderType::Fak,
            VenueOrderAmount::GrossUsd(Usd::new(ENTRY_GROSS_USD)),
            Usd::new(ENTRY_FEE_USD),
            Shares::new(ENTRY_FILLED_SHARES),
            Price::new(ENTRY_PRICE),
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

fn position_fill(
    ids: &ExecutionTxnIds,
    intent_id: &OrderIntentId,
    filled_at: DateTime<Utc>,
) -> PositionFill {
    position_fill_public(
        ids,
        intent_id,
        Shares::new(ENTRY_FILLED_SHARES),
        Usd::new(EXECUTION_NOTIONAL),
        filled_at,
    )
}

/// Position fill helper for partial-fill demo scenarios.
pub fn position_fill_public(
    ids: &ExecutionTxnIds,
    intent_id: &OrderIntentId,
    shares: Shares,
    cost_usd: Usd,
    filled_at: DateTime<Utc>,
) -> PositionFill {
    PositionFill {
        order_intent_id: *intent_id,
        execution_account_id: ids.execution_account,
        token_id: TokenId::new(&ids.token),
        market_id: MarketId::new(&ids.market),
        event_id: Some(EventId::new(&ids.event)),
        category: MarketCategory::Weather,
        side: OutcomeSide::Yes,
        shares,
        price: Price::new(ENTRY_PRICE),
        cost_usd,
        filled_at,
        source: AccountSource::Polymarket,
    }
}

pub(super) fn reconciliation_row(
    execution_order_id: &ExecutionOrderId,
    intent_id: &OrderIntentId,
    resolved_at: DateTime<Utc>,
) -> NewReconciliation {
    NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: *execution_order_id,
        order_intent_id: *intent_id,
        result: ReconciliationResult::Filled,
        evidence_json: ReconciliationEvidenceChain(vec![ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::ClobOrderStatus,
            observed_at: resolved_at,
            detail: "submission result".to_owned(),
            venue_ref: None,
            shares: Some(Shares::new(ENTRY_FILLED_SHARES)),
            price: Some(Price::new(ENTRY_PRICE)),
            fee_evidence: None,
        }]),
        venue_filled_shares: Some(Shares::new(ENTRY_FILLED_SHARES)),
        venue_avg_price: Some(Price::new(ENTRY_PRICE)),
        expected_cash_delta_usd: None,
        venue_cash_delta_usd: None,
        realized_pnl_usd: None,
        expected_fee_usd: Some(Usd::new(ENTRY_FEE_USD)),
        derived_fee_usd: None,
        settled_fee_usd: Some(Usd::new(ENTRY_FEE_USD)),
        fee_delta_usd: Some(Usd::ZERO),
        resolved_by: Some("venue_submit_response".to_owned()),
        resolved_at: Some(resolved_at),
    }
}

pub(super) fn exit_reconciliation_row(
    execution_order_id: &ExecutionOrderId,
    intent_id: &OrderIntentId,
    filled_shares: Shares,
    average_price: Price,
    resolved_at: DateTime<Utc>,
) -> NewReconciliation {
    NewReconciliation {
        reconciliation_id: ReconciliationId::from_v7(),
        execution_order_id: *execution_order_id,
        order_intent_id: *intent_id,
        result: ReconciliationResult::Filled,
        evidence_json: ReconciliationEvidenceChain(vec![ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::ClobOrderStatus,
            observed_at: resolved_at,
            detail: "confirmed exit submission result".to_owned(),
            venue_ref: Some("venue-exit".to_owned()),
            shares: Some(filled_shares),
            price: Some(average_price),
            fee_evidence: None,
        }]),
        venue_filled_shares: Some(filled_shares),
        venue_avg_price: Some(average_price),
        expected_cash_delta_usd: None,
        venue_cash_delta_usd: None,
        realized_pnl_usd: None,
        expected_fee_usd: Some(Usd::ZERO),
        derived_fee_usd: None,
        settled_fee_usd: Some(Usd::ZERO),
        fee_delta_usd: Some(Usd::ZERO),
        resolved_by: Some("venue_submit_response".to_owned()),
        resolved_at: Some(resolved_at),
    }
}

pub(super) fn exit_order(
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
            TokenId::new(&ids.token),
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

pub async fn seed_report_catalog(
    db: &DatabaseConnection,
    event_id: &str,
    market_id: &str,
    market_question: &str,
    market_slug: &str,
    token_id: &str,
    no_token_id: &TokenId,
) {
    let mut event = make_event(event_id, "Event", "event", MarketCategory::Weather);
    event.catalog_market_ids = vec![MarketId::new(market_id)].into();
    PgEventRepository::new(db.clone())
        .upsert(event)
        .await
        .expect("seed event");
    let mut market = make_market(
        market_id,
        event_id,
        market_question,
        market_slug,
        MarketCategory::Weather,
        None,
    );
    market.yes_token_id = TokenId::new(token_id);
    market.no_token_id = no_token_id.clone();
    PgMarketRepository::new(db.clone())
        .upsert(market)
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

struct ScoreCalibrationFixture {
    source: ModelVersionInfo,
    training: TrainingDatasetInfo,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    snapshot_hash: ContentHash,
    prediction_horizon_secs: u64,
    trade_policy: Option<ModelServingTradePolicyBinding>,
    fit_window_start: DateTime<Utc>,
    fit_window_end: DateTime<Utc>,
}

struct PreparedScoreCalibration {
    fit_window: TimeWindow,
    split_hash: ContentHash,
    payload: ModelScoreCalibrationPayload,
    dataset_hash: ContentHash,
}

impl ScoreCalibrationFixture {
    async fn load(
        db: &DatabaseConnection,
        store: &Arc<dyn ArtifactStore>,
        model_version_id: &ModelVersionId,
    ) -> Self {
        let source = PgModelRegistryRepository::new(db.clone())
            .find_model_version(model_version_id)
            .await
            .expect("load score-calibration source model")
            .expect("persisted score-calibration source model");
        let artifact = ModelArtifact::load_verified(store.as_ref(), &source)
            .await
            .expect("load exact score-calibration source artifact");
        assert_eq!(
            artifact.content_hash().expect("source artifact hash"),
            source.artifact_hash,
            "score-calibration source artifact must match the registry"
        );
        let contract = source
            .verified_serving_contract()
            .expect("verified score-calibration source contract");
        let bindings = contract.bindings();
        let training = PgTrainingDatasetRepository::new(db.clone())
            .find_by_id(&bindings.dataset.manifest.training_dataset_id)
            .await
            .expect("load score-calibration training Dataset")
            .expect("score-calibration training Dataset");
        let policy = PgPolicyRepository::new(db.clone())
            .load_snapshot(&bindings.policy_snapshot.decision_policy_snapshot_id)
            .await
            .expect("load score-calibration policy snapshot")
            .expect("score-calibration policy snapshot");
        assert_eq!(
            policy.snapshot_hash, bindings.policy_snapshot.snapshot_hash,
            "score-calibration policy preimage must match the source contract"
        );
        let embargo = Duration::seconds(
            i64::try_from(policy.snapshot.model_routing.model.calibration.embargo_secs)
                .expect("calibration embargo fits chrono"),
        );
        let fit_window_start = training
            .window_end
            .checked_add_signed(embargo)
            .expect("calibration fit start");
        let decision_policy_snapshot_id = bindings.policy_snapshot.decision_policy_snapshot_id;
        let snapshot_hash = bindings.policy_snapshot.snapshot_hash;
        let prediction_horizon_secs = bindings.model.prediction_horizon_secs;
        let trade_policy = bindings.trade_policy.clone();
        Self {
            source,
            training,
            decision_policy_snapshot_id,
            snapshot_hash,
            prediction_horizon_secs,
            trade_policy,
            fit_window_start,
            fit_window_end: fit_window_start + Duration::days(1),
        }
    }

    async fn persist_dataset(
        &self,
        db: &DatabaseConnection,
        store: &Arc<dyn ArtifactStore>,
    ) -> TrainingDatasetInfo {
        let training = self
            .training
            .materialization()
            .expect("score-calibration training Dataset materialization");
        ModelDatasetLedgerFixture::persist(
            db,
            store,
            ModelDatasetLedgerSeed {
                scope: format!("score-calibration-{}", self.source.model_version_id),
                model_spec_id: self.source.model_spec_id,
                model_family: self.source.model_family,
                model_spec_definition_hash: self.source.model_spec_definition_hash,
                factor_serving_plane: training.factor_serving_plane.clone(),
                feature_schema_version: self.training.feature_schema_version,
                feature_schema_hash: *training.feature_schema_hash,
                decision_policy_snapshot_id: self.decision_policy_snapshot_id,
                profile_ref: self.source.profile_ref.clone(),
                prediction_horizon_secs: self.prediction_horizon_secs,
                purpose: DatasetPurpose::Calibration,
                window_start: self.fit_window_start,
                window_end: self.fit_window_end,
                research_program_hash: self.training.source_lineage.research_program_hash,
                sample_count: 500,
                decision_interval_secs: 1,
                trade_policy: self.trade_policy.clone(),
            },
        )
        .await
        .expect("persist held-out Calibration Dataset")
    }

    fn prepare(&self, dataset: &TrainingDatasetInfo) -> PreparedScoreCalibration {
        let training = self
            .training
            .materialization()
            .expect("score-calibration training Dataset materialization");
        let calibration = dataset
            .materialization()
            .expect("Calibration Dataset materialization");
        let split_hash = CanonicalDigest::content_hash_json(&(
            "model-score-calibration-split-v1",
            self.source.model_version_id,
            dataset.training_dataset_id,
            calibration.dataset_hash,
            calibration.manifest_hash,
        ))
        .expect("score-calibration split hash");
        PreparedScoreCalibration {
            fit_window: TimeWindow::new(self.fit_window_start, self.fit_window_end),
            split_hash,
            dataset_hash: *calibration.dataset_hash,
            payload: ModelScoreCalibrationPayload {
                format_version: MODEL_SCORE_CALIBRATION_FORMAT_VERSION,
                fit_contract: ModelScoreCalibrationFitContract {
                    model: ModelScoreCalibrationModelBinding {
                        model_version_id: self.source.model_version_id,
                        artifact_hash: self.source.artifact_hash,
                        serving_contract_hash: self.source.serving_contract_hash,
                        model_spec_id: self.source.model_spec_id,
                        model_spec_definition_hash: self.source.model_spec_definition_hash,
                        model_family: self.source.model_family,
                        profile_ref: self.source.profile_ref.clone(),
                        category_scope: self.source.category_scope,
                        prediction_horizon_secs: self.prediction_horizon_secs,
                        training_dataset_id: self.training.training_dataset_id,
                        training_dataset_hash: *training.dataset_hash,
                    },
                    calibration_dataset: ModelScoreCalibrationDatasetBinding {
                        calibration_dataset_id: dataset.training_dataset_id,
                        dataset_hash: *calibration.dataset_hash,
                        manifest_hash: *calibration.manifest_hash,
                        artifact_bytes_hash: *calibration.artifact_bytes_hash,
                        source_slice_manifest_hash: dataset
                            .source_lineage
                            .source_slice
                            .manifest_hash,
                        feature_schema_hash: *calibration.feature_schema_hash,
                        factor_schema_hash: calibration.factor_schema_hash(),
                        label_schema_hash: *calibration.label_schema_hash,
                    },
                    policy_snapshot: ModelScoreCalibrationPolicyBinding {
                        decision_policy_snapshot_id: self.decision_policy_snapshot_id,
                        snapshot_hash: self.snapshot_hash,
                    },
                },
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
                split_payout_rate: SplitPayoutRateEvidence {
                    total_sample_count: 500,
                    split_sample_count: 0,
                    empirical_probability: Probability::ZERO,
                    wilson_ci: (Probability::ZERO, Probability::new(dec!(0.007624))),
                    split_payout_ratio: PayoutRatio::try_new(dec!(0.5))
                        .expect("split payout ratio"),
                },
            },
        }
    }

    async fn commit(
        &self,
        db: &DatabaseConnection,
        prepared: PreparedScoreCalibration,
    ) -> CalibrationArtifactId {
        let calibration_id = CalibrationArtifactId::from_v7();
        let content_hash = model_score_content_hash(
            &prepared.fit_window,
            &prepared.split_hash,
            &prepared.payload,
        )
        .expect("hash demo calibration artifact");
        let model_run_id = ModelRunId::from_v7();
        PgModelRunRepository::new(db.clone())
            .create(NewModelRun {
                model_run_id,
                run_kind: ModelRunKind::Calibration,
                model_version_id: Some(self.source.model_version_id),
                decision_policy_snapshot_id: self.decision_policy_snapshot_id,
                market_selection_id: None,
                window_start: self.fit_window_start,
                window_end: self.fit_window_end,
                input_hash: prepared.dataset_hash,
            })
            .await
            .expect("seed running score-calibration model run");
        let outcome = PgCalibrationArtifactRepository::new(db.clone())
            .commit_model_score(ModelScoreCalibrationCommit {
                model_run_id,
                artifact: NewCalibrationArtifact {
                    artifact_id: calibration_id,
                    kind: CalibrationKind::ModelScore,
                    content_hash,
                    fit_window_start: self.fit_window_start,
                    fit_window_end: self.fit_window_end,
                    calibration_split_hash: prepared.split_hash,
                    sample_count: 500,
                    payload: CalibrationArtifactPayload::ModelScore(Box::new(prepared.payload)),
                    active: false,
                },
            })
            .await
            .expect("atomically seed inactive score-calibration fit artifact");
        outcome.artifact().artifact_id
    }
}

/// Seed a held-out score calibration from one exact persisted source model.
pub async fn seed_score_calibration(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    model_version_id: &ModelVersionId,
) -> CalibrationArtifactId {
    let fixture = ScoreCalibrationFixture::load(db, store, model_version_id).await;
    let dataset = fixture.persist_dataset(db, store).await;
    let prepared = fixture.prepare(&dataset);
    fixture.commit(db, prepared).await
}

/// Persist an uncalibrated parent, held-out calibrator, and sealed derived child.
pub async fn seed_calibrated_model(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    seed: CalibratedModelSeed,
) -> CalibratedModelFixture {
    let dataset = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&seed.training_dataset_id)
        .await
        .expect("load execution model dataset")
        .expect("execution model dataset");
    let policy = PgPolicyRepository::new(db.clone())
        .load_snapshot(&dataset.decision_policy_snapshot_id)
        .await
        .expect("load execution model policy")
        .expect("execution model policy");
    let profile = dataset
        .source_lineage
        .research_profile_artifact_id
        .profile_ref()
        .resolve_builtin_research_profile()
        .expect("resolve execution model ResearchProfile");
    let model_spec = PgModelRegistryRepository::new(db.clone())
        .find_model_spec(&dataset.model_spec_id)
        .await
        .expect("load execution model spec")
        .expect("execution model spec");
    let input_contract = model_spec.input_contract.clone();
    let plane = dataset.factor_serving_plane.clone();
    let scoring = &policy.snapshot.profile_artifacts.scoring.definition;
    let factor_head = seed
        .head
        .resolve(&plane, &scoring.factor_head)
        .expect("resolve calibrated model factor head");
    let metrics = ModelVersionMetrics::not_measured("calibrated model fixture");
    let training_objective = ModelTrainingObjective::hand_authored("calibrated model fixture");
    let parent_model_version_id = ModelVersionId::from_v7();
    let parent_payload = ModelPayloadFixture::weighted(
        &plane,
        &factor_head,
        input_contract.clone(),
        ReturnModelSpec::heuristic_default(),
        scoring.cross_section.clone(),
    )
    .expect("execution parent model payload");
    let parent = SealedModelFixture::seal(
        db,
        ModelArtifactFixtureSeed {
            model_version_id: parent_model_version_id,
            training_dataset_id: seed.training_dataset_id,
            payload: parent_payload,
            training_input_hash: seed.training_input_hash,
            category_scope: profile.spec.category,
            calibration: None,
            bias_table: None,
        },
    )
    .await
    .expect("seal execution parent model artifact");
    parent
        .store(store)
        .await
        .expect("store execution parent model artifact");
    let parent_contract = parent.serving_contract().clone();
    let parent_bindings = parent_contract.bindings();
    let parent_category_scope = parent_bindings.model.category_scope;
    let parent_profile_ref = parent_bindings.model.profile_ref.clone();
    let parent_training_dataset_id = parent_bindings.dataset.manifest.training_dataset_id;
    let parent_trade_policy = parent_bindings
        .trade_policy
        .as_ref()
        .map(|binding| (binding.artifact_id, binding.content_hash));
    let registry = PgModelRegistryRepository::new(db.clone());
    let parent_version = registry
        .next_version_for_spec(&model_spec.model_spec_id)
        .await
        .expect("next execution parent version");
    registry
        .create_model_version(NewModelVersion {
            model_version_id: parent_model_version_id,
            model_spec_id: model_spec.model_spec_id,
            version: parent_version,
            artifact_hash: parent.artifact_hash(),
            serving_contract: parent_contract,
            category_scope: parent_category_scope,
            profile_ref: parent_profile_ref,
            training_dataset_id: Some(parent_training_dataset_id),
            trade_policy_artifact_id: parent_trade_policy.map(|binding| binding.0),
            trade_policy_hash: parent_trade_policy.map(|binding| binding.1),
            derivation: NewModelVersion::training_derivation(),
            metrics: metrics.clone(),
            training_objective: training_objective.clone(),
        })
        .await
        .expect("persist execution parent model");

    let calibration_id =
        Box::pin(seed_score_calibration(db, store, &parent_model_version_id)).await;
    let calibration_repo = PgCalibrationArtifactRepository::new(db.clone());
    calibration_repo
        .mark_active(&calibration_id)
        .await
        .expect("activate execution calibration");
    let payload = ModelPayloadFixture::weighted(
        &plane,
        &factor_head,
        input_contract,
        ReturnModelSpec::Calibrated(CalibratedReturnModel {
            calibrator_ref: calibration_id,
            downside_source: DownsideSource::MfeMae,
        }),
        scoring.cross_section.clone(),
    )
    .expect("execution calibrated model payload");
    let calibration = calibration_repo
        .find_by_id(&calibration_id)
        .await
        .expect("load execution calibration")
        .expect("execution calibration row");
    let fixture = SealedModelFixture::seal(
        db,
        ModelArtifactFixtureSeed {
            model_version_id: seed.model_version_id,
            training_dataset_id: seed.training_dataset_id,
            payload,
            training_input_hash: seed.training_input_hash,
            category_scope: profile.spec.category,
            calibration: Some(ModelBindingFixture::score_calibration(
                calibration_id,
                calibration.content_hash,
            )),
            bias_table: None,
        },
    )
    .await
    .expect("seal execution model artifact");
    fixture
        .store(store)
        .await
        .expect("store execution model artifact");
    CalibratedModelFixture {
        sealed: fixture,
        parent_model_version_id,
        calibration_artifact_id: calibration_id,
        metrics,
        training_objective,
    }
}

pub(super) struct SeedModelVersionInput<'a> {
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_version_id: ModelVersionId,
    pub model_name: &'a str,
    pub profile_ref: ResearchProfileRef,
    pub artifact_store: Option<&'a Arc<dyn ArtifactStore>>,
    pub head: CalibratedModelHead,
}

fn execution_model_window(spec: &ResearchProfileSpec) -> (DateTime<Utc>, DateTime<Utc>) {
    let evaluation_days = i64::from(spec.feedback_policy.evaluation_window_days);
    let fit_days = i64::from(spec.fit_span_days);
    let horizon_secs = i64::try_from(spec.target_horizon_secs)
        .expect("execution ResearchProfile horizon fits i64");
    let purge_embargo_secs =
        i64::try_from(spec.purge_embargo_secs).expect("execution ResearchProfile embargo fits i64");
    let cadence_secs = i64::try_from(spec.feedback_policy.feedback_cadence_secs)
        .expect("execution ResearchProfile cadence fits i64");
    let governed_history = Duration::days(
        fit_days
            .checked_add(
                evaluation_days
                    .checked_mul(2)
                    .expect("execution feedback evaluation span fits i64"),
            )
            .and_then(|days| days.checked_add(2))
            .expect("execution feedback day horizon fits i64"),
    ) + Duration::seconds(
        horizon_secs
            .checked_add(
                purge_embargo_secs
                    .checked_mul(2)
                    .expect("execution feedback embargo span fits i64"),
            )
            .and_then(|seconds| seconds.checked_add(cadence_secs))
            .expect("execution feedback second horizon fits i64"),
    );
    let window_start_raw = Utc::now() - governed_history;
    let window_start = DateTime::from_timestamp_millis(window_start_raw.timestamp_millis())
        .expect("execution model window must fit millisecond precision");
    (window_start, window_start + Duration::days(1))
}

async fn seed_fixture_evaluation_run(
    db: &DatabaseConnection,
    model_version_id: ModelVersionId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
) -> ModelRunId {
    let model_run_id = ModelRunId::from_v7();
    let runs = PgModelRunRepository::new(db.clone());
    let window_end = Utc::now();
    let window_start = window_end - Duration::milliseconds(1);
    // This handle exists only for repository fixtures that need completed
    // model lineage before a report-specific decision exists. Classifying it
    // as LiveInference would fabricate a serving decision without a frozen
    // market selection and poison the automatic 24-hour parity window. Every
    // real fixture report creates its own exact LiveInference run later.
    runs.create(NewModelRun {
        model_run_id,
        run_kind: ModelRunKind::Backtest,
        model_version_id: Some(model_version_id),
        decision_policy_snapshot_id,
        market_selection_id: None,
        window_start,
        window_end,
        input_hash: content_hash('d'),
    })
    .await
    .expect("create model run");
    runs.succeed(&model_run_id, content_hash('e'), None)
        .await
        .expect("finish model run");
    model_run_id
}

pub(super) async fn seed_model_version_named(
    db: &DatabaseConnection,
    input: SeedModelVersionInput<'_>,
) -> SeededRouteModel {
    let SeedModelVersionInput {
        decision_policy_snapshot_id,
        model_version_id,
        model_name,
        profile_ref,
        artifact_store,
        head,
    } = input;
    let profile = profile_ref
        .resolve_builtin_research_profile()
        .expect("resolve execution ResearchProfile");
    let prediction_horizon_secs = i64::try_from(profile.spec.target_horizon_secs)
        .expect("execution ResearchProfile horizon fits i64");
    let registry = PgModelRegistryRepository::new(db.clone());
    let dataset_store_fallback = ModelDatasetLedgerFixture::local_store();
    let dataset_store = artifact_store.unwrap_or(&dataset_store_fallback);
    // Every serving preimage must predate the complete future feedback program.
    let (window_start, window_end) = execution_model_window(&profile.spec);
    let policy_scope = format!("execution-{model_name}");
    let trade_policy_fixture = Box::pin(PublishedTradePolicyFixture::persist(
        db,
        dataset_store,
        PublishedTradePolicyFixtureInput {
            decision_policy_snapshot_id,
            profile_ref: profile_ref.clone(),
            scope: &policy_scope,
            training_window_start: window_start,
        },
    ))
    .await
    .expect("persist complete execution TradePolicy preimage");
    let trade_policy = trade_policy_fixture.provenance().clone();
    let input_contract = ModelInputContract::single_required("book.mid");
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
            prediction_horizon_secs,
            input_contract,
            trade_policy_fixture.outcome_training_contract(),
        );
        let definition_hash = spec.definition_hash;
        registry.create_model_spec(spec).await.expect("model spec");
        (model_spec_id, definition_hash)
    };
    let policy = PgPolicyRepository::new(db.clone())
        .load_snapshot(&decision_policy_snapshot_id)
        .await
        .expect("load demo policy snapshot")
        .expect("demo policy snapshot");
    let features = &policy.snapshot.profile_artifacts.features.definition;
    let factors = &policy.snapshot.profile_artifacts.scoring.definition;
    let domain = &policy.snapshot.profile_artifacts.domain.definition;
    let factor_engine = FactorEngine::for_model_scope(
        factors,
        features,
        domain,
        profile.spec.feature_contract,
        profile.spec.category,
        None,
    );
    let factor_plane = factor_engine.serving_plane().expect("demo factor plane");
    PgFactorRepository::new(db.clone())
        .register_definitions(
            factor_plane
                .definitions()
                .iter()
                .cloned()
                .map(NewFactorDefinition::from)
                .collect(),
        )
        .await
        .expect("register execution model factor definitions");
    let feature_schema_hash = ResearchHasher::feature_schema(
        &ExecutableFeatureSchema::build(features, profile.spec.feature_contract)
            .expect("feature schema"),
    )
    .expect("feature schema hash");
    let dataset = ModelDatasetLedgerFixture::persist(
        db,
        dataset_store,
        ModelDatasetLedgerSeed {
            scope: format!("execution-model-{model_version_id}"),
            model_spec_id,
            model_family: ModelFamily::WeightedFactor,
            model_spec_definition_hash,
            factor_serving_plane: factor_plane.clone(),
            feature_schema_version: SchemaVersion::FIRST,
            feature_schema_hash,
            decision_policy_snapshot_id,
            profile_ref: profile_ref.clone(),
            prediction_horizon_secs: profile.spec.target_horizon_secs,
            purpose: DatasetPurpose::Training,
            window_start,
            window_end,
            research_program_hash: ResearchHasher::canonical(&(
                "execution-model-program-v1",
                model_spec_id,
                model_spec_definition_hash,
            ))
            .expect("execution research program hash"),
            sample_count: 500,
            decision_interval_secs: 1,
            trade_policy: Some(ModelBindingFixture::trade_policy(
                trade_policy.artifact_id,
                trade_policy.artifact_hash,
            )),
        },
    )
    .await
    .expect("persist execution model dataset");
    let fixture = Box::pin(seed_calibrated_model(
        db,
        dataset_store,
        CalibratedModelSeed {
            model_version_id,
            training_dataset_id: dataset.training_dataset_id,
            training_input_hash: content_hash('7'),
            head,
        },
    ))
    .await;
    let version = registry
        .next_version_for_spec(&model_spec_id)
        .await
        .expect("next calibrated model version");
    ModelVersionFixture::persist_route_candidate(db, fixture.version(model_spec_id, version))
        .await
        .expect("publish model version through exact parity proof");
    let model_run_id =
        seed_fixture_evaluation_run(db, model_version_id, decision_policy_snapshot_id).await;
    SeededRouteModel {
        model_version_id,
        calibration_artifact_id: fixture.calibration_artifact_id,
        model_run_id,
        trade_policy,
        factor_serving_plane: factor_plane.clone(),
    }
}

async fn seed_market_selection(
    db: &DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
    decision_at: DateTime<Utc>,
    members: Vec<ReportSelectionMemberSeed>,
) -> MarketSelectionId {
    let id = MarketSelectionId::from_v7();
    let members = members
        .into_iter()
        .map(|member| member.bind(id))
        .collect::<Vec<_>>();
    let market_count = i32::try_from(members.len()).expect("market selection exceeds i32");
    PgMarketSelectionRepository::new(db.clone())
        .create_snapshot(
            NewMarketSelection {
                market_selection_id: id,
                decision_at,
                decision_policy_snapshot_id: *rc_id,
                selector_hash: content_hash('b'),
                selector_evidence: SelectorFixture::evidence(content_hash('b')),
                market_count,
                exclusion_summary: SelectionExclusionSummary::default(),
            },
            members,
        )
        .await
        .expect("market selection");
    id
}

impl ExecutionTxnIds {
    fn build_report_transaction(&self) -> NewReportTransaction {
        build_report_transaction_inner(self, ReportBuildOptions::published_single(self))
    }
}

fn build_report_transaction_inner(
    ids: &ExecutionTxnIds,
    options: ReportBuildOptions,
) -> NewReportTransaction {
    let equity_snapshot_id = EquitySnapshotId::from_v7();
    let ReportBuildOptions {
        mut recommendations,
        entry_condition_artifacts,
        summary,
        as_of,
        runtime_mode,
        account_capital_usd,
    } = options;
    let recommendation_created_at = as_of + Duration::seconds(2);
    for recommendation in &mut recommendations {
        recommendation.created_at = recommendation_created_at;
    }
    let report_run_id = ReportRunId::new(ids.report.as_uuid());
    let represented_routes =
        RepresentedRouteSet::from_routes([BuyModelRoute::Weather]).expect("fixture Route set");
    let portfolio_policy = PortfolioConfig::default();
    let scenario_artifact =
        fixture_scenario_artifact(&represented_routes, &portfolio_policy, as_of);
    let mut account_snapshot = ids.new_account_snapshot();
    account_snapshot.account_snapshot_id = ids.account_snapshot;
    if let Some(capital) = account_capital_usd {
        let invested = account_snapshot
            .positions_json
            .0
            .iter()
            .map(|position| position.current_value)
            .sum::<Usd>();
        assert!(
            capital >= invested,
            "historical fixture capital must cover its marked positions"
        );
        account_snapshot.venue_net_liquidation_usd = capital;
        account_snapshot.capital_base_usd = capital;
        account_snapshot.available_usd = capital - invested;
    }
    let capital_base_usd = account_snapshot.capital_base_usd;
    let venue_net_liquidation_usd = account_snapshot.venue_net_liquidation_usd;
    let available_usd = account_snapshot.available_usd;
    let report = fixture_report(
        ids,
        FixtureReportInput {
            equity_snapshot_id: &equity_snapshot_id,
            report_run_id,
            represented_routes: represented_routes.clone(),
            scenario_artifact: &scenario_artifact,
            summary,
            as_of,
            runtime_mode,
            capital_base_usd,
        },
    );
    let route_runs = fixture_route_runs(ids, &report, &recommendations);
    let sampled_feature_parity = report_fixtures::sampled_parity(&report);
    let entry_condition_instances = recommendations
        .iter()
        .map(|recommendation| fixture_condition_instance(recommendation, ids, as_of))
        .collect();
    NewReportTransaction {
        feature_parity_state_id: Some(ids.feature_parity_state_id),
        account_snapshot,
        equity_snapshot: NewEquitySnapshot {
            equity_snapshot_id,
            as_of,
            source: AccountSource::Polymarket,
            venue_net_liquidation_usd,
            capital_base_usd,
            available_usd,
            reserved_usd: Usd::ZERO,
            realized_pnl_cumulative_usd: Usd::ZERO,
            unrealized_pnl_usd: Usd::ZERO,
            incentive_credit_cumulative_usd: Usd::ZERO,
            high_water_mark_usd: capital_base_usd,
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
            account_snapshot_id: ids.account_snapshot,
            decision_policy_snapshot_id: ids.decision_policy_snapshot,
            market_selection_id: ids.market_selection,
            decision_at: as_of,
            represented_routes_json: represented_routes,
            scenario_artifact_id: Some(scenario_artifact.portfolio_scenario_artifact_id),
            scenario_artifact_hash: Some(scenario_artifact.content_hash),
            scenario_artifact_json: Some(scenario_artifact),
            portfolio_policy_json: portfolio_policy,
            existing_state_json: fixture_existing_state(),
            decision_json: fixture_portfolio_decision(ids.portfolio_plan, &recommendations),
        },
        report,
        route_runs,
        recommendations,
        entry_condition_artifacts,
        entry_condition_instances,
        sampled_feature_parity: Some(sampled_feature_parity),
        fact_delivery: Some(report_fixtures::pending_fact_delivery(&ids.report)),
        operation_log: (ids).report_operation_log(),
    }
}

fn fixture_condition_instance(
    recommendation: &NewRecommendation,
    ids: &ExecutionTxnIds,
    published_at: DateTime<Utc>,
) -> NewEntryConditionInstance {
    let (artifact_id, artifact_hash, state, truth, next_evaluation_at) =
        match &recommendation.trade_plan.entry.condition {
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

impl ExecutionTxnIds {
    fn price_condition_artifact(&self) -> NewEntryConditionArtifact {
        let payload = EntryConditionArtifactV1 {
            schema_version: ENTRY_CONDITION_SCHEMA_VERSION,
            evaluator_version: ENTRY_CONDITION_EVALUATOR_VERSION,
            binding: EntryConditionBinding {
                recommendation_id: self.recommendation,
                market_id: MarketId::new(&self.market),
                token_id: TokenId::new(&self.token),
                outcome_side: OutcomeSide::Yes,
                market_linkage_id: None,
                market_linkage_hash: None,
                catalog_snapshot_id: self.market_selection,
                catalog_snapshot_hash: content_hash('b'),
                model_version_id: self.model_version,
                decision_policy_snapshot_id: self.decision_policy_snapshot,
                factor_bindings: Vec::new(),
                source_bindings: Vec::new(),
            },
            confirmation: ConfirmationPolicy {
                required_continuous_ms: 2_000,
                max_observation_gap_ms: 1_000,
            },
            root: EntryConditionV1::Price(PriceCondition {
                token_id: TokenId::new(&self.token),
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
}

struct FixtureReportInput<'a> {
    equity_snapshot_id: &'a EquitySnapshotId,
    report_run_id: ReportRunId,
    represented_routes: RepresentedRouteSet,
    scenario_artifact: &'a PortfolioScenarioArtifact,
    summary: ReportSummary,
    as_of: DateTime<Utc>,
    runtime_mode: QuantRuntimeMode,
    capital_base_usd: Usd,
}

fn fixture_report(ids: &ExecutionTxnIds, input: FixtureReportInput<'_>) -> NewRecommendationReport {
    let FixtureReportInput {
        equity_snapshot_id,
        report_run_id,
        represented_routes,
        scenario_artifact,
        summary,
        as_of,
        runtime_mode,
        capital_base_usd,
    } = input;
    NewRecommendationReport {
        recommendation_report_id: ids.report,
        report_run_id,
        report_kind: ReportKind::TopN,
        decision_at: as_of,
        runtime_mode,
        decision_policy_snapshot_id: ids.decision_policy_snapshot,
        market_selection_id: ids.market_selection,
        portfolio_plan_id: ids.portfolio_plan,
        represented_routes_json: represented_routes,
        scenario_artifact_id: Some(scenario_artifact.portfolio_scenario_artifact_id),
        scenario_artifact_hash: Some(scenario_artifact.content_hash),
        top_n: 20,
        status: RecommendationReportStatus::Prepared,
        account_source: AccountSource::Polymarket,
        capital_base_usd,
        account_snapshot_ref: ids.account_snapshot,
        equity_snapshot_ref: *equity_snapshot_id,
        data_quality_snapshot_ref: ids.data_quality_snapshot,
        summary_json: summary,
        published_at: None,
        successor_report_id: None,
        superseded_at: None,
        obsoleted_at: None,
        valid_until: Some(as_of + Duration::hours(1)),
        revoked_at: None,
        expired_at: None,
        status_reason: None,
        created_at: as_of + Duration::seconds(1),
    }
}

fn fixture_route_runs(
    ids: &ExecutionTxnIds,
    report: &NewRecommendationReport,
    recommendations: &[NewRecommendation],
) -> Vec<NewReportRouteRun> {
    let profile_ref = fixture_profile_ref();
    let lineage = RouteModelLineage {
        model_version_id: ids.model_version,
        model_run_id: Some(ids.model_run),
        calibration_artifact_id: ids.calibration_artifact,
        trade_policy_artifact_id: Some(ids.trade_policy.artifact_id),
        research_profile_artifact_id: profile_ref.artifact_id(),
        research_profile_ref: profile_ref,
        prediction_horizon_secs: 86_400,
        feature_contract_digest: content_hash('h'),
        pit_lineage_digest: content_hash('i'),
        serving_contract_digest: content_hash('j'),
        recommendation_contract_hash: ids.trade_policy.artifact_hash,
        report_universe_plan_hash: content_hash('k'),
        history_serving_head_seal_id: HistoryServingHeadSealId::new(ids.report.as_uuid()),
        history_serving_head_seal_hash: content_hash('l'),
        serving_authority: ServingAuthority::ExecutionEligible,
    };
    let selected_recommendations =
        u32::try_from(recommendations.len()).expect("fixture recommendation count fits u32");
    vec![NewReportRouteRun {
        report_route_run_id: ReportRouteRunId::new(ids.report.as_uuid()),
        report_run_id: report.report_run_id,
        route: BuyModelRoute::Weather,
        outcome: if recommendations.is_empty() {
            RouteRunOutcome::ZeroCandidates
        } else {
            RouteRunOutcome::Ready
        },
        model_version_id: Some(lineage.model_version_id),
        model_run_id: lineage.model_run_id,
        calibration_artifact_id: Some(lineage.calibration_artifact_id),
        trade_policy_artifact_id: lineage.trade_policy_artifact_id,
        research_profile_artifact_id: Some(lineage.research_profile_artifact_id.clone()),
        lineage_json: Some(lineage),
        funnel_json: RouteCandidateFunnel {
            eligible_markets: 1,
            feature_complete_markets: 1,
            calibrated_candidates: selected_recommendations,
            admitted_economic_tiers: selected_recommendations,
            selected_recommendations,
        },
        diagnostic_code: None,
        finished_at: report.decision_at,
    }]
}

fn fixture_scenario_artifact(
    represented_routes: &RepresentedRouteSet,
    policy: &PortfolioConfig,
    as_of: DateTime<Utc>,
) -> PortfolioScenarioArtifact {
    let mut scenarios = vec![
        PortfolioScenario {
            scenario_index: 0,
            kind: PortfolioScenarioKind::PitBootstrap,
            label: "fixture_pit".to_owned(),
            scenario_model_state_hash: content_hash('3'),
            scenario_state_hash: content_hash('0'),
            market_outcomes: Vec::new(),
        },
        PortfolioScenario {
            scenario_index: 1,
            kind: PortfolioScenarioKind::CalibrationUncertainty,
            label: "fixture_calibration".to_owned(),
            scenario_model_state_hash: content_hash('4'),
            scenario_state_hash: content_hash('1'),
            market_outcomes: Vec::new(),
        },
        PortfolioScenario {
            scenario_index: 2,
            kind: PortfolioScenarioKind::StructuralStress,
            label: "fixture_stress".to_owned(),
            scenario_model_state_hash: content_hash('5'),
            scenario_state_hash: content_hash('2'),
            market_outcomes: Vec::new(),
        },
    ];
    for scenario in &mut scenarios {
        scenario.scenario_state_hash = scenario
            .recomputed_state_hash()
            .expect("fixture scenario state hash");
    }
    let capital_time_bucket_contract_digest =
        CapitalTimeBucketContract::try_from(policy.tail_risk.capital_time_buckets.as_slice())
            .expect("fixture capital-time grid")
            .content_hash()
            .expect("fixture capital-time contract hash");
    let scenario_model_content_hash = content_hash('n');
    let scenario_model_artifact_id =
        PortfolioScenarioModelArtifactId::from_content_hash(&scenario_model_content_hash);
    let mut artifact = PortfolioScenarioArtifact {
        portfolio_scenario_artifact_id: PortfolioScenarioArtifactId::from_v7(),
        portfolio_scenario_model_artifact_id: scenario_model_artifact_id,
        scenario_model_content_hash,
        schema_version: SchemaVersion::FIRST,
        decision_at: as_of,
        visibility: PortfolioScenarioVisibility::PointInTime,
        input_universe_hash: content_hash('o'),
        ordered_routes: represented_routes.routes.clone(),
        route_set_digest: represented_routes.digest,
        serving_contract_digest: content_hash('j'),
        calibration_contract_digest: content_hash('k'),
        recommendation_contract_digest: content_hash('m'),
        evidence_regime: PortfolioScenarioEvidenceRegime::FullL2ExecutionEconomics,
        capital_time_bucket_contract_digest,
        scenarios,
        distributions: vec![
            ScenarioDistribution {
                distribution_id: "nominal".to_owned(),
                nominal: true,
                weights: vec![
                    ScenarioWeight {
                        scenario_index: 0,
                        probability_bps: 5_000,
                    },
                    ScenarioWeight {
                        scenario_index: 1,
                        probability_bps: 3_000,
                    },
                    ScenarioWeight {
                        scenario_index: 2,
                        probability_bps: 2_000,
                    },
                ],
            },
            ScenarioDistribution {
                distribution_id: "robust".to_owned(),
                nominal: false,
                weights: vec![
                    ScenarioWeight {
                        scenario_index: 0,
                        probability_bps: 3_000,
                    },
                    ScenarioWeight {
                        scenario_index: 1,
                        probability_bps: 3_000,
                    },
                    ScenarioWeight {
                        scenario_index: 2,
                        probability_bps: 4_000,
                    },
                ],
            },
        ],
        structural_exclusivity: Vec::new(),
        discount_curve: policy
            .tail_risk
            .capital_time_buckets
            .iter()
            .map(|bucket| DiscountCurvePoint {
                end_secs: bucket.end_secs,
                annualized_cost_bps: 500,
            })
            .collect(),
        content_hash: content_hash('q'),
    };
    artifact.content_hash = artifact
        .recomputed_hash()
        .expect("fixture scenario artifact hash");
    artifact.portfolio_scenario_artifact_id =
        PortfolioScenarioArtifactId::from_content_hash(&artifact.content_hash);
    artifact
}

fn fixture_existing_state() -> ExistingPortfolioState {
    ExistingPortfolioState {
        existing_open_capital_usd: Usd::ZERO,
        existing_open_recommendations: 0,
        current_drawdown_usd: Usd::ZERO,
        scenario_cashflows: (0_u32..3)
            .map(|scenario_index| ScenarioCashflow {
                scenario_index,
                discounted_net_usd: Usd::ZERO,
            })
            .collect(),
        capital_occupancy: [3_600_u64, 86_400, 604_800]
            .into_iter()
            .map(|end_secs| CapitalOccupancyBucket {
                end_secs,
                locked_usd: Usd::ZERO,
            })
            .collect(),
    }
}

fn fixture_portfolio_decision(
    portfolio_plan_id: PortfolioPlanId,
    recommendations: &[NewRecommendation],
) -> PortfolioDecisionResult {
    if recommendations.is_empty() {
        return PortfolioDecisionResult::ZeroCandidates {
            rejected_tier_count: 0,
            evidence_hash: content_hash('r'),
        };
    }
    let selected_tier_ids = recommendations
        .iter()
        .map(|recommendation| recommendation.economic_tier_id)
        .collect::<Vec<_>>();
    let economics = recommendations
        .iter()
        .map(|recommendation| recommendation.economics_json)
        .collect::<Vec<_>>();
    let objectives = PortfolioObjectiveEvidence {
        robust_expected_net_usd: economics
            .iter()
            .map(|value| value.robust_expected_net_usd)
            .sum(),
        nominal_expected_net_usd: economics
            .iter()
            .map(|value| value.nominal_expected_net_usd)
            .sum(),
        cvar_usd: economics
            .iter()
            .map(|value| value.cvar_contribution_usd)
            .sum(),
        capital_occupancy_usd_hours: UsdHours::new(
            economics
                .iter()
                .map(|value| value.capital_occupancy_usd_hours.inner())
                .sum(),
        ),
        stable_tie_break_stages: 1,
    };
    let allocated_usd = recommendations
        .iter()
        .map(|recommendation| {
            recommendation
                .economic_tier_json
                .entry_execution
                .hard_reserved_cash_usd()
        })
        .sum();
    let selected_recommendation_count =
        u32::try_from(recommendations.len()).expect("fixture recommendation count fits u32");
    let constraints = PortfolioConstraintEvidence {
        available_cash_used_usd: allocated_usd,
        open_capital_usd: allocated_usd,
        selected_recommendation_count,
        maximum_scenario_loss_usd: economics.iter().map(|value| value.max_loss_usd).sum(),
        checked_constraint_count: 1,
        evidence_hash: content_hash('s'),
    };
    let solver = SolverEvidence {
        backend: "highs".to_owned(),
        lexicographic_model_build_count: 1,
        lexicographic_solve_count: 6,
        tie_break_proof_count: 1,
        lexicographic_warm_start_count: 5,
        marginal_model_build_count: 0,
        marginal_solve_count: selected_recommendation_count,
        marginal_model_reuse_count: selected_recommendation_count,
        configured_deadline_secs: 30,
        deterministic_threads: 1,
        coefficient_scale: 1_000_000,
        bound_scale_exponent: 0,
        optimal: true,
    };
    let exact_verification = ExactVerificationEvidence {
        passed: true,
        selected_tier_digest: CanonicalDigest::content_hash_typed(
            "quant-pivot/selected-economic-tiers",
            1,
            &selected_tier_ids,
        )
        .expect("fixture tier digest"),
        recomputed_economics_hash: CanonicalDigest::content_hash_typed(
            "quant-pivot/recomputed-recommendation-economics",
            1,
            &economics,
        )
        .expect("fixture economics digest"),
    };
    let content_hash = CanonicalDigest::content_hash_typed(
        "quant-pivot/global-portfolio-plan",
        1,
        &(
            portfolio_plan_id,
            &selected_tier_ids,
            &objectives,
            &constraints,
            &solver,
            &exact_verification,
        ),
    )
    .expect("fixture global plan digest");
    PortfolioDecisionResult::Optimized {
        plan: Box::new(GlobalPortfolioPlan {
            portfolio_plan_id,
            selected_tier_ids,
            objectives,
            constraints,
            solver,
            exact_verification,
            content_hash,
        }),
    }
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

impl ExecutionTxnIds {
    fn new_account_snapshot(&self) -> NewAccountSnapshot {
        let positions = vec![PositionSnapshot {
            token_id: TokenId::new(&self.token),
            market_id: MarketId::new(&self.market),
            event_id: Some(EventId::new(&self.event)),
            category: MarketCategory::Weather,
            outcome: "Yes".to_owned(),
            size: Shares::new(dec!(100)),
            avg_price: Price::new(dec!(0.5)),
            cur_price: Price::new(dec!(0.6)),
            current_value: Usd::new(dec!(60)),
            redeemable: false,
        }];
        NewAccountSnapshot {
            account_snapshot_id: AccountSnapshotId::from_v7(),
            execution_account_id: self.execution_account,
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

fn sizing_plan(economic_tier_id: EconomicTierId) -> SizingPlan {
    SizingPlan {
        economic_tier_id,
        requested_shares: Shares::new(ENTRY_FILLED_SHARES),
        expected_filled_shares: Shares::new(ENTRY_FILLED_SHARES),
        hard_reserved_cash_usd: Usd::new(EXECUTION_NOTIONAL),
        immediate_fee_usd: Usd::new(ENTRY_FEE_USD),
        expected_maker_rebate_usd: Usd::ZERO,
        maker_rebate_schedule: None,
        reference_entry_price: Price::new(ENTRY_PRICE),
        portfolio_weight_pct: dec!(0.025),
        market_exposure_after_usd: Usd::new(EXECUTION_NOTIONAL),
        event_exposure_after_usd: Usd::new(EXECUTION_NOTIONAL),
        category_exposure_after_usd: Usd::new(EXECUTION_NOTIONAL),
        route_exposure_after_usd: Usd::new(EXECUTION_NOTIONAL),
        capital_occupancy_usd_hours: UsdHours::new(EXECUTION_NOTIONAL),
        sizing_reason: "selected executable tier from the exact global MILP".to_owned(),
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
            require_route_gate_eligibility: true,
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

fn trade_plan(
    policy: &TradePolicyCohortProvenance,
    economic_tier_id: EconomicTierId,
) -> RecommendationTradePlan {
    RecommendationTradePlan {
        policy: Box::new(policy.clone().into()),
        entry: entry_plan(),
        sizing: Box::new(sizing_plan(economic_tier_id)),
        exit: Box::new(exit_plan().into()),
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
        max_route_exposure_usd: Usd::new(dec!(1500)),
        cvar_contribution_usd: Usd::new(dec!(25)),
        portfolio_cvar_cap_usd: Usd::new(dec!(1500)),
        maximum_scenario_loss_cap_usd: Usd::new(dec!(2500)),
        requires_approval: true,
        auto_execution_allowed: true,
        risk_notes: Vec::new(),
        envelope_hash: content_hash('f'),
    }
}

fn recommendation_identity() -> RecommendationIdentity {
    RecommendationIdentity {
        category: MarketCategory::Weather,
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

impl ExecutionTxnIds {
    fn factor_breakdown(&self) -> RecommendationFactorBreakdown {
        RecommendationFactorBreakdown(
            self.factor_values()
                .into_iter()
                .map(|factor| FactorBreakdownEntry {
                    factor_name: factor.name.to_string(),
                    family: factor.family,
                    value_state: factor.value_state(),
                    raw_value: factor.raw_value,
                    normalized_score: factor.normalized_score(),
                    normalization_source: factor.normalization_source(),
                    indeterminate_reason: factor.indeterminate_reason(),
                    weight: Decimal::ZERO,
                    contribution: Decimal::ZERO,
                    confidence: factor.confidence,
                    direction: factor.direction,
                    explanation: factor.explanation.headline,
                    source_refs: Vec::new(),
                })
                .collect(),
        )
    }

    fn evidence_refs(&self, token_id: &TokenId) -> EvidenceRefs {
        EvidenceRefs {
        signal_candidate_id: SignalCandidateId::from_v7(),
        feature_vector_id: FeatureVectorId::from_v7(),
        model_run_id: self.model_run,
        market_selection_id: self.market_selection,
        book_snapshot_ref: BookSnapshotRef::from_str(&format!(
            "book:l2|{}|00000000-0000-0000-0000-000000000001|1|blake3:{}|1700000000|1700000000@blake3:{}",
            token_id,
            "1".repeat(64),
            "0".repeat(64)
        ))
        .expect("book ref"),
        decision_policy_snapshot_id: self.decision_policy_snapshot,
        model_version_id: self.model_version,
            factor_definition_versions: self
                .factor_serving_plane
                .definitions()
                .iter()
                .map(FactorDefinitionRef::factor_definition_id)
                .collect(),
        data_quality_snapshot_ref: self.data_quality_snapshot,
    }
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
    }
}

fn report_summary() -> ReportSummary {
    ReportSummary {
        market_selection_count: 1,
        represented_route_count: 1,
        candidate_count: 1,
        rejected_tier_count: 0,
        published_recommendation_count: 1,
        total_hard_reserved_cash_usd: Usd::new(EXECUTION_NOTIONAL),
        max_single_recommendation_usd: Usd::new(EXECUTION_NOTIONAL),
        robust_expected_net_usd: Usd::new(dec!(11.5)),
        nominal_expected_net_usd: Usd::new(dec!(19.5)),
        cvar_usd: Usd::new(dec!(25)),
        maximum_scenario_loss_usd: Usd::new(dec!(25)),
        capital_occupancy_usd_hours: UsdHours::new(dec!(25)),
        category_allocation: BTreeMap::new(),
        event_allocation: BTreeMap::new(),
        route_allocation: BTreeMap::from([(BuyModelRoute::Weather, Usd::new(EXECUTION_NOTIONAL))]),
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
        represented_route_count: 1,
        candidate_count: 12,
        rejected_tier_count: 12,
        published_recommendation_count: 0,
        total_hard_reserved_cash_usd: Usd::ZERO,
        max_single_recommendation_usd: Usd::ZERO,
        robust_expected_net_usd: Usd::ZERO,
        nominal_expected_net_usd: Usd::ZERO,
        cvar_usd: Usd::ZERO,
        maximum_scenario_loss_usd: Usd::ZERO,
        capital_occupancy_usd_hours: UsdHours::ZERO,
        category_allocation: BTreeMap::new(),
        event_allocation: BTreeMap::new(),
        route_allocation: BTreeMap::new(),
        data_quality_summary: DataQualitySummary::default(),
        top_rejection_reasons: Vec::new(),
        execution_eligibility_summary: EligibilitySummary::default(),
        empty_reason: Some(EmptyReportReason::NoPositiveSignal),
        warnings: vec!["ui-demo: no positive signal above threshold".to_owned()],
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::runtime_config::DecisionPolicySnapshot;
    use rust_decimal_macros::dec;

    use super::FeedbackServingFixtureConfig;

    #[test]
    fn feedback_policy_is_valid() {
        for (budget, reserve, max_open, capacity, ad_hoc_enabled) in [
            (dec!(555.56), dec!(55.556), dec!(500.004), dec!(500), true),
            (dec!(5000), dec!(500), dec!(4500), dec!(500), false),
        ] {
            let config = FeedbackServingFixtureConfig {
                required_shadow_window_secs: 60,
                shadow_diff_threshold: dec!(1),
                feedback_budget_usd: budget,
                outcome_reconciliation_enabled: true,
                ad_hoc_report_enabled: ad_hoc_enabled,
            };
            let mut snapshot = DecisionPolicySnapshot::default();
            config.apply_runtime_controls(&mut snapshot);
            let portfolio = &snapshot.execution_risk.portfolio;
            assert_eq!(portfolio.budget.cash_reserve_usd.value, reserve);
            assert_eq!(portfolio.budget.max_open_capital_usd.value, max_open);
            assert_eq!(
                portfolio
                    .exposure_limits
                    .max_single_recommendation_usd
                    .value,
                dec!(25)
            );
            for cap in [
                portfolio.exposure_limits.max_market_exposure_usd.value,
                portfolio.exposure_limits.max_event_exposure_usd.value,
                portfolio.exposure_limits.max_category_exposure_usd.value,
                portfolio.exposure_limits.max_route_exposure_usd.value,
                portfolio.tail_risk.max_cvar_usd.value,
                portfolio.tail_risk.max_scenario_loss_usd.value,
            ] {
                assert_eq!(cap, capacity);
            }
            assert_eq!(portfolio.tail_risk.max_drawdown_usd.value, budget);
            assert_eq!(
                portfolio
                    .tail_risk
                    .capital_time_buckets
                    .iter()
                    .map(|bucket| bucket.max_capital_usd.value)
                    .collect::<Vec<_>>(),
                [capacity; 3]
            );
            assert_eq!(
                snapshot.recommendation.reports.ad_hoc_report_enabled,
                ad_hoc_enabled
            );
            assert!(
                snapshot
                    .report_schedule
                    .schedules
                    .iter()
                    .all(|schedule| !schedule.enabled)
            );
            let validation = snapshot.validate_runtime_config();
            assert!(
                !validation.has_errors(),
                "governed feedback policy must be valid: {validation}"
            );
        }
    }

    #[test]
    #[should_panic(expected = "fund every concurrently open ResearchProfile cash tier")]
    fn underfunded_policy_rejected() {
        let config = FeedbackServingFixtureConfig {
            required_shadow_window_secs: 60,
            shadow_diff_threshold: dec!(1),
            feedback_budget_usd: dec!(100),
            outcome_reconciliation_enabled: true,
            ad_hoc_report_enabled: true,
        };
        config.apply_runtime_controls(&mut DecisionPolicySnapshot::default());
    }
}
