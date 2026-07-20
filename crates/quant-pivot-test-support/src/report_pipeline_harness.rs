//! Shared harness for Phase 04 report pipeline integration tests.

use std::{
    env, process,
    sync::{Arc, Mutex},
};

use crate::{
    catalog_fixtures::{make_event, make_market},
    execution_pg_seed::{
        fixture_profile_ref, seed_model_score_calibration_fixture, seed_report_trade_policy_fixture,
    },
    fact_sink::DiscardFactWriter,
    factor_governance::publish_all_factor_definitions,
    model_spec_fixtures::new_model_spec_fixture,
    pit::InMemoryDecisionSnapshotSource,
    policy_fixtures::bootstrap_policy_bundle,
    report_fixtures,
    report_lifecycle_seed::{persist_and_publish_report, persist_prepared_report},
    trade_tape_fixtures::live_trade_tape_block_cursor_repo,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_api::data_api::VenuePosition;
use quant_pivot_core::{
    governance::{
        BiasTableApplicator, CoreCalibrationArtifactLoader, RuntimeModeHandle,
        WeightOverlayApplicator,
    },
    ingest::{book_store::BookStore, market_registry::MarketRegistry},
    observability::{
        alert_dispatcher::AlertDispatcher, factor_fact_writer::FactorEventWriter,
        feature_fact_writer::FeatureEventWriter, metrics_hub::MetricsHub,
        model_input_fact_writer::ModelInputEventWriter,
        signal_candidate_fact_writer::SignalCandidateEventWriter,
    },
    prefetch::{feature_window::FeatureWindowProvider, market_candidates::MarketCandidateProvider},
    report::{
        AdHocReportRequest, DefaultRecommendationComposer, DefaultReportBuilder, ReportBuilderDeps,
        ReportLifecycleDeps, ReportLifecycleService, ReportPublisher, ReportPublisherDeps,
        ReportReadinessGate,
    },
    service::{
        account::{
            AccountProviderFactory, PolymarketAccountClient, RepoReservedCapitalReader,
            ReservedCapitalReader,
        },
        equity::EquitySnapshotService,
        factor_pipeline::FactorPipelineService,
        feature_integrity::{FeatureParityGatePort, FeatureParityRunCoordinator},
        feature_pipeline::{FeaturePipelineDeps, FeaturePipelineService},
        model_runner::{DispatcherAlertSink, ModelRunner, ModelRunnerDeps},
    },
};
use quant_pivot_error::{QuantResult, storage::StorageError};
use quant_pivot_models::{
    clickhouse::{
        BookL2CheckpointRow, BookMicrostructureRow, DomainObservationRow, MarketResolutionRow,
        MidPriceBucketRow, TradeTapeRow,
    },
    config::TradeTapeOnChainConfig,
    domain::{
        BasisAlertInfo, BasisAlertListQuery, ClaimReportSchedule, CoreEventPublisher,
        DecisionBoundary, MarketLinkageInfo, MarketLinkageListQuery, NewAccountSnapshot,
        NewBasisAlert, NewEntryConditionInstance, NewEquitySnapshot, NewFeatureParityState,
        NewMarketLinkage, NewMarketSelection, NewModelRun, NewModelVersion, NewOperationLog,
        NewPortfolioPlan, NewRecommendation, NewRecommendationReport, NewReportDataQualitySnapshot,
        NewReportTransaction, Paginated, RecommendationInfo, RecommendationReportInfo,
        ReportRunClaimConfig,
        governance::lifecycle::OperationalPhase,
        market::{EventRegistryInfo, MarketRegistryInfo, TokenInfo, book::BookLevel},
    },
    entities::quant_feature_parity_state,
    enums::{
        catalog::CatalogFilterReasonSet,
        common::{CategorySet, MarketCategory, TickSize},
        factor::FactorFamily,
        market::{EventStatus, MarketStatus},
        model::ModelFamily,
        operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
        quant::{
            DownsideSource, EntryConditionState, FeatureParityLatchState,
            FeatureParityStateTransition, ModelRunKind, ModelRunStatus, OutcomeSide,
            PublicationStatus, RecommendationReportStatus, ReportKind,
        },
        rbac::ResourceType,
    },
    runtime_config::{
        DecimalValue, DecisionPolicySnapshot, DomainConfig, FactorCrossSectionConfig,
        FactorsConfig, FeaturesConfig, ModelConfig, ModelVersionRef, PortfolioBudget,
        PortfolioConfig, PortfolioConstraints, ReportsConfig, SelectionConfig,
    },
    types::{
        AccountPositions, BasisAlertId, ConditionTruth, ContentHash, DecisionPolicySnapshotId,
        DomainInstrumentKey, EntryConditionFoldState, EntryConditionInstanceId, EntryConditionPlan,
        EventId, ExposureBreakdown, FeatureParityStateId, MarketId, MarketLinkageId,
        MarketSelectionId, ModelInputContract, ModelRunId, ModelSpecId, ModelTrainingContract,
        ModelVersionId, OperationDetailDocument, OperationLogId, PortfolioConstraintsSnapshot,
        PortfolioOptimizerMeta, PortfolioRejectedSummary, PortfolioRiskBudget, Price,
        RecommendationId, RecommendationReportId, RecommendationTradePlan, ReportDataQualityTokens,
        ResearchProfileRef, SelectionExclusionSummary, Shares, TokenId, Usd, WorkerId,
        model_metrics::ModelVersionMetrics, model_training::ModelTrainingObjective,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCalibrationArtifactRepository, PgEquitySnapshotRepository, PgEventRepository,
        PgFactorRepository, PgFeatureParityRepository, PgFeatureRepository, PgMarketRepository,
        PgMarketSelectionRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgPolicyRepository, PgPositionRepository, PgRecommendationReportRepository,
        PgRecommendationRepository, PgReportRunRepository, PgReservedCapitalRepository,
        PgShadowComparisonRepository, PgTradePolicyRepository,
    },
    traits::{
        BasisAlertRepository, CalibrationArtifactRepository, EquitySnapshotRepository,
        EventRepository, FactorRepository, FeatureParityRepository, MarketLinkageRepository,
        MarketRepository, MarketSelectionRepository, ModelRegistryRepository, ModelRunRepository,
        PolicyRepository, PositionRepository, QuantFactReadRepository,
        RecommendationReportRepository, RecommendationRepository, ReportRunRepository,
        ReservedCapitalRepository, TradePolicyRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    factors::{FactorEngine, FrozenReferenceQuantiles},
    features::FeatureSchema,
    hashing::ResearchHasher,
    model::{
        CalibratedReturnModel, CalibrationArtifactLoader, DefaultModelRuntimeFactoryBuilder,
        FactorWeight, ModelArtifact, ModelArtifactHeader, ReturnModelSpec, ScoreMultiplierSpec,
        SubstitutionConfidenceRules, WeightedFactorModelArtifact, model_input_contract_hash,
    },
    pit::PointInTimeSnapshotSource,
    portfolio::HistoricalCorrelationEstimator,
    selection::ConfiguredMarketSelector,
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{DatabaseConnection, EntityTrait, IntoActiveModel};

/// Seeded catalog ids shared across report pipeline E2E tests.
pub const EVENT_ID: &str = "evt-report-pipeline-e2e";
pub const MARKET_ID: &str = "0xreportpipelinee2e";
pub const MARKET_ID_2: &str = "0xreportpipelinee2e2";
pub const YES_TOKEN: &str = "55555";
pub const NO_TOKEN: &str = "66666";
pub const YES_TOKEN_2: &str = "55556";
pub const NO_TOKEN_2: &str = "66667";
pub const STUB_FUNDER: &str = "0xfunder";

/// How market selection is configured for a harness bootstrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionPreset {
    /// Sports category matching the seeded catalog markets (two markets for rank factors).
    Standard,
    /// Politics-only selection so the Sports market is excluded.
    Empty,
}

/// Account wiring for the harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountFixture {
    /// Stub venue client returning `collateral`.
    Stub,
    /// Missing private key — factory fails closed on `create`.
    Unavailable,
}

/// Bootstrap knobs for [`ReportPipelineHarness::bootstrap`].
#[derive(Debug, Clone)]
pub struct HarnessOptions {
    pub selection: SelectionPreset,
    pub account: AccountFixture,
    pub collateral: Usd,
    pub bind_trade_policy: bool,
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self {
            selection: SelectionPreset::Standard,
            account: AccountFixture::Stub,
            collateral: Usd::new(dec!(10_000)),
            bind_trade_policy: true,
        }
    }
}

impl HarnessOptions {
    /// Harness with empty market selection (wrong category filter).
    #[must_use]
    pub fn empty_selection() -> Self {
        Self {
            selection: SelectionPreset::Empty,
            ..Self::default()
        }
    }

    /// Harness whose account provider factory has no venue client.
    #[must_use]
    pub fn unavailable_account() -> Self {
        Self {
            account: AccountFixture::Unavailable,
            ..Self::default()
        }
    }

    /// Harness whose active model deliberately has no trade-policy binding.
    #[must_use]
    pub fn missing_trade_policy() -> Self {
        Self {
            bind_trade_policy: false,
            ..Self::default()
        }
    }
}

/// Wired report lifecycle + repositories for integration tests.
pub struct ReportPipelineHarness {
    pub db: DatabaseConnection,
    pub lifecycle: ReportLifecycleService,
    pub report_repo: Arc<PgRecommendationReportRepository>,
    pub report_run_repo: Arc<PgReportRunRepository>,
    pub recommendation_repo: Arc<PgRecommendationRepository>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_version_id: ModelVersionId,
}

impl ReportPipelineHarness {
    /// Drive the durable queue, claim, build, delivery verification, and
    /// publication contract for one ad-hoc integration-test request.
    pub async fn execute_ad_hoc(
        &self,
        request: AdHocReportRequest,
    ) -> QuantResult<RecommendationReportInfo> {
        let outcome = self.lifecycle.run_ad_hoc(request).await?;
        if let Some(report_id) = outcome.run().output_report_id.as_ref() {
            return self
                .report_repo
                .find_by_id(report_id)
                .await?
                .ok_or_else(|| {
                    StorageError::not_found("quant_recommendation_report", report_id).into()
                });
        }
        let worker_id = WorkerId::from_v7();
        let run = self
            .report_run_repo
            .claim_next_run(
                worker_id,
                120,
                300,
                ReportRunClaimConfig {
                    decision_policy_snapshot_id: self.decision_policy_snapshot_id.clone(),
                    ad_hoc_default_top_n: 20,
                    ad_hoc_default_knowledge_lag_secs: 10,
                    schedules: Vec::<ClaimReportSchedule>::new(),
                },
            )
            .await?
            .ok_or_else(|| {
                StorageError::state_conflict(
                    "quant_report_run",
                    Option::<&str>::None,
                    "queued test run was not claimable",
                )
            })?;
        let prepared = self.lifecycle.execute_claimed(run).await?;
        let delivery_worker = WorkerId::from_v7();
        let now = Utc::now();
        let claimed = self
            .report_repo
            .claim_fact_delivery(delivery_worker.clone(), 60)
            .await?
            .ok_or_else(|| {
                StorageError::state_conflict(
                    "quant_report_fact_delivery",
                    Some(&prepared.recommendation_report_id),
                    "prepared test report delivery was not claimable",
                )
            })?;
        if claimed.recommendation_report_id != prepared.recommendation_report_id {
            return Err(StorageError::state_conflict(
                "quant_report_fact_delivery",
                Some(&prepared.recommendation_report_id),
                "claimed a different report delivery",
            )
            .into());
        }
        let settlement = self
            .report_repo
            .verify_and_publish_report(&prepared.recommendation_report_id, delivery_worker, now)
            .await?;
        settlement
            .into_applied()
            .map(|outcome| outcome.report)
            .map_err(|lost| {
                StorageError::state_conflict(
                    "quant_report_fact_delivery",
                    Some(&lost.recommendation_report_id),
                    "prepared test report lost its delivery claim",
                )
                .into()
            })
    }
}

struct AlwaysOperationalGate;

struct ClearFeatureParityGate {
    state_id: FeatureParityStateId,
}

#[async_trait]
impl FeatureParityGatePort for ClearFeatureParityGate {
    async fn ensure_clear(&self, _action: &'static str) -> QuantResult<()> {
        Ok(())
    }

    async fn commit_state_id(&self, _action: &'static str) -> QuantResult<FeatureParityStateId> {
        Ok(self.state_id.clone())
    }
}

impl ReportReadinessGate for AlwaysOperationalGate {
    fn operational_phase(&self) -> OperationalPhase {
        OperationalPhase::Operational
    }
}

struct StubAccountClient {
    collateral: Usd,
}

#[async_trait]
impl PolymarketAccountClient for StubAccountClient {
    async fn available_collateral(&self) -> QuantResult<Usd> {
        Ok(self.collateral)
    }

    async fn positions(&self, _funder: &str) -> QuantResult<Vec<VenuePosition>> {
        Ok(Vec::new())
    }
}

/// Empty linkage ledger: every market is fail-closed unresolved (no domain
/// slice, `DomainAvailability::Unresolved` for mapped categories).
pub struct EmptyLinkageRepo;

#[async_trait]
impl MarketLinkageRepository for EmptyLinkageRepo {
    async fn append(&self, _linkage: NewMarketLinkage) -> Result<MarketLinkageInfo, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("quant_market_linkage"),
            detail: "EmptyLinkageRepo is read-only".to_owned(),
        })
    }

    async fn append_batch(
        &self,
        _linkages: Vec<NewMarketLinkage>,
    ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("quant_market_linkage"),
            detail: "EmptyLinkageRepo is read-only".to_owned(),
        })
    }

    async fn valid_at(
        &self,
        _market_id: &MarketId,
        _boundary: &DecisionBoundary,
    ) -> Result<Option<MarketLinkageInfo>, StorageError> {
        Ok(None)
    }

    async fn valid_at_for_markets(
        &self,
        _market_ids: &[MarketId],
        _boundary: &DecisionBoundary,
    ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn latest_for_markets(
        &self,
        _market_ids: &[MarketId],
    ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn latest_for_active_markets(&self) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn ledger_for_markets(
        &self,
        _market_ids: &[MarketId],
        _end_boundary: &DecisionBoundary,
    ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn find_by_id(
        &self,
        _linkage_id: &MarketLinkageId,
    ) -> Result<Option<MarketLinkageInfo>, StorageError> {
        Ok(None)
    }

    async fn page(
        &self,
        query: MarketLinkageListQuery,
    ) -> Result<Paginated<MarketLinkageInfo>, StorageError> {
        Ok(Paginated::empty_for(&query))
    }
}

/// Empty basis-alert feed: every basis check reports no history (no cooldown
/// suppression), and recording is a read-only no-op error for this harness.
pub struct EmptyBasisAlertRepo;

#[async_trait]
impl BasisAlertRepository for EmptyBasisAlertRepo {
    async fn record(&self, _alert: NewBasisAlert) -> Result<BasisAlertInfo, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("quant_basis_alert"),
            detail: "EmptyBasisAlertRepo is read-only".to_owned(),
        })
    }

    async fn latest_for_market(
        &self,
        _market_id: &MarketId,
    ) -> Result<Option<BasisAlertInfo>, StorageError> {
        Ok(None)
    }

    async fn page(
        &self,
        query: BasisAlertListQuery,
    ) -> Result<Paginated<BasisAlertInfo>, StorageError> {
        Ok(Paginated::empty_for(&query))
    }

    async fn acknowledge(
        &self,
        alert_id: &BasisAlertId,
        _actor: String,
    ) -> Result<BasisAlertInfo, StorageError> {
        Err(StorageError::NotFound {
            entity: "quant_basis_alert",
            id: alert_id.to_string(),
        })
    }
}

struct EmptyFactRead;

#[async_trait]
impl QuantFactReadRepository for EmptyFactRead {
    async fn microstructure_window(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn microstructure_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _available_by_ms: i64,
        _minute: bool,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn trade_tape_window_by_market(
        &self,
        _market_ids: Vec<MarketId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn last_trades(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _limit: u64,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn mid_price_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
        _bucket_secs: u32,
    ) -> Result<Vec<MidPriceBucketRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn book_checkpoint_at(
        &self,
        _token_id: &TokenId,
        _as_of_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<BookL2CheckpointRow>, StorageError> {
        Ok(None)
    }

    async fn book_checkpoints_between(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _available_by_ms: i64,
    ) -> Result<Vec<BookL2CheckpointRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn resolution_at(
        &self,
        _market_id: &MarketId,
        _source_cutoff_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        Ok(None)
    }

    async fn resolutions_between(
        &self,
        _market_ids: Vec<MarketId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn domain_observations_between(
        &self,
        _instrument_keys: Vec<DomainInstrumentKey>,
        _from_ms: i64,
        _to_ms: i64,
        _publish_cutoff_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<DomainObservationRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn domain_observation_at(
        &self,
        _instrument_key: &DomainInstrumentKey,
        _metric: &str,
        _as_of_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<DomainObservationRow>, StorageError> {
        Ok(None)
    }

    async fn observed_markets_between(
        &self,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<MarketId>, StorageError> {
        Ok(Vec::new())
    }
}

impl ReportPipelineHarness {
    /// Bootstrap catalog, model, runtime config, and the full report pipeline.
    pub async fn bootstrap(db: &DatabaseConnection, options: HarnessOptions) -> Self {
        seed_catalog(db).await;

        let registry = Arc::new(MarketRegistry::new());
        let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
        wire_live_book(&registry, &book_store);

        let factors = factors_config();
        let features = FeaturesConfig::default();
        let store = artifact_store();
        let calibration_loader = calibration_artifact_loader(db);
        let factor_repo =
            Arc::new(PgFactorRepository::new(db.clone())) as Arc<dyn FactorRepository>;
        let domain = DomainConfig::default();
        publish_all_factor_definitions(factor_repo.as_ref(), &factors, &features, &domain)
            .await
            .expect("publish factor definitions");
        let model_version_id = ModelVersionId::from_v7();

        let runtime_config = runtime_config_for_pipeline(
            &model_version_id,
            selection_config(options.selection),
            &factors,
            &features,
        );
        let runtime_config_repo =
            Arc::new(PgPolicyRepository::new(db.clone())) as Arc<dyn PolicyRepository>;
        let decision_policy_snapshot_id =
            bootstrap_policy_activation(runtime_config_repo.as_ref(), &runtime_config).await;
        publish_weighted_model(&WeightedModelFixture {
            db,
            store: &store,
            factors: &factors,
            features: &features,
            domain: &domain,
            model_version_id: &model_version_id,
            decision_policy_snapshot_id: &decision_policy_snapshot_id,
            bind_trade_policy: options.bind_trade_policy,
        })
        .await;

        let version = runtime_config_repo
            .load_current()
            .await
            .expect("active runtime config")
            .expect("active runtime config row");

        let account_factory = account_factory(db, Arc::clone(&registry), &options);
        let model_runner =
            build_model_runner(db, Arc::clone(&store), Arc::clone(&calibration_loader));
        let builder = build_report_builder(ReportBuilderHarnessInput {
            db,
            runtime_config_repo: Arc::clone(&runtime_config_repo),
            registry: &registry,
            book_store: &book_store,
            model_runner,
            account_factory,
            artifact_store: Arc::clone(&store),
            calibration_loader,
        });
        let lifecycle = build_lifecycle_service(db, runtime_config_repo, builder, store).await;

        Self {
            db: db.clone(),
            lifecycle,
            report_repo: Arc::new(PgRecommendationReportRepository::new(db.clone())),
            report_run_repo: Arc::new(PgReportRunRepository::new(db.clone())),
            recommendation_repo: Arc::new(PgRecommendationRepository::new(db.clone())),
            decision_policy_snapshot_id: version.decision_policy_snapshot_id,
            model_version_id,
        }
    }
}

/// Activate a runtime config version when the store is empty.
pub async fn bootstrap_policy_activation(
    repo: &dyn PolicyRepository,
    config: &DecisionPolicySnapshot,
) -> DecisionPolicySnapshotId {
    bootstrap_policy_bundle(
        repo,
        config,
        "report-pipeline-it",
        "report pipeline integration test bootstrap",
    )
    .await
}

/// Persist a pre-built report transaction via the production repository.
pub async fn seed_published_report(
    db: &DatabaseConnection,
    txn: NewReportTransaction,
) -> RecommendationReportInfo {
    let trigger_key = format!("fixture:{}", txn.report.recommendation_report_id);
    persist_and_publish_report(db, txn, &trigger_key, 10).await
}

/// Context for seeding fixture reports against a bootstrapped harness database.
#[derive(Debug, Clone)]
pub struct FixtureReportSeedContext {
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_version_id: ModelVersionId,
}

const FIXTURE_MARKET_A: &str = "0xmarketA";
const FIXTURE_MARKET_B: &str = "0xmarketB";
const FIXTURE_EVENT: &str = "evt-fixture-web";

/// Seed a published `TopN` report with two shared [`report_fixtures`] recommendations.
pub async fn seed_fixture_published_report(
    db: &DatabaseConnection,
    report_id: RecommendationReportId,
    ctx: &FixtureReportSeedContext,
) -> RecommendationReportInfo {
    seed_fixture_published_report_with_profile(db, report_id, ctx, fixture_profile_ref()).await
}

/// Seed a published fixture report in an explicit authority scope.
///
/// All report-owned recommendations receive the same immutable profile
/// reference so same-scope API validation is exercised without corrupting
/// lineage.
pub async fn seed_fixture_published_report_with_profile(
    db: &DatabaseConnection,
    report_id: RecommendationReportId,
    ctx: &FixtureReportSeedContext,
    profile_ref: ResearchProfileRef,
) -> RecommendationReportInfo {
    seed_fixture_report(db, report_id, ctx, profile_ref, true).await
}

/// Seed a complete Prepared fixture whose fact delivery is still Pending.
pub async fn seed_fixture_prepared_report(
    db: &DatabaseConnection,
    report_id: RecommendationReportId,
    ctx: &FixtureReportSeedContext,
) -> RecommendationReportInfo {
    seed_fixture_report(db, report_id, ctx, fixture_profile_ref(), false).await
}

async fn seed_fixture_report(
    db: &DatabaseConnection,
    report_id: RecommendationReportId,
    ctx: &FixtureReportSeedContext,
    profile_ref: ResearchProfileRef,
    publish: bool,
) -> RecommendationReportInfo {
    seed_fixture_market_catalog(db).await;
    let market_selection_id =
        seed_minimal_market_selection(db, &ctx.decision_policy_snapshot_id).await;

    let mut report = report_fixtures::report(
        report_id.clone(),
        ReportKind::TopN,
        RecommendationReportStatus::Published,
    );
    report.decision_policy_snapshot_id = ctx.decision_policy_snapshot_id.clone();
    report.model_version_id = ctx.model_version_id.clone();
    report.market_selection_id = market_selection_id.clone();
    let model_run_id =
        seed_fixture_model_run(db, ctx, &market_selection_id, report.decision_at).await;
    report.model_run_id = Some(model_run_id.clone());
    report.profile_id = profile_ref.id.clone();
    report.profile_ref = profile_ref.clone();

    let mut recommendations = vec![
        report_fixtures::recommendation(
            report_id.clone(),
            RecommendationId::from_v7(),
            1,
            FIXTURE_MARKET_A,
            OutcomeSide::Yes,
            Usd::new(dec!(300)),
        ),
        report_fixtures::recommendation(
            report_id,
            RecommendationId::from_v7(),
            2,
            FIXTURE_MARKET_B,
            OutcomeSide::No,
            Usd::new(dec!(200)),
        ),
    ];
    for rec in &mut recommendations {
        rec.profile_ref = profile_ref.clone();
        rec.event_id = EventId::new(FIXTURE_EVENT);
        rec.evidence_refs.decision_policy_snapshot_id = ctx.decision_policy_snapshot_id.clone();
        rec.evidence_refs.model_version_id = ctx.model_version_id.clone();
        rec.evidence_refs.model_run_id = model_run_id.clone();
        rec.evidence_refs.market_selection_id = market_selection_id.clone();
    }

    let feature_parity_state_id = seed_clear_feature_parity_state(db).await;
    let txn = fixture_report_transaction(&report, recommendations, feature_parity_state_id);
    if publish {
        seed_published_report(db, txn).await
    } else {
        let trigger_key = format!("fixture:{}", txn.report.recommendation_report_id);
        persist_prepared_report(db, txn, &trigger_key, 10).await
    }
}

async fn seed_fixture_model_run(
    db: &DatabaseConnection,
    ctx: &FixtureReportSeedContext,
    market_selection_id: &MarketSelectionId,
    decision_at: DateTime<Utc>,
) -> ModelRunId {
    let model_run_id = ModelRunId::from_v7();
    let input_hash = ResearchHasher::canonical(&(
        "web_report_fixture_model_input_v1",
        &model_run_id,
        &ctx.model_version_id,
        market_selection_id,
    ))
    .expect("hash web report fixture model input");
    let output_hash = ResearchHasher::canonical(&(
        "web_report_fixture_model_output_v1",
        &model_run_id,
        market_selection_id,
    ))
    .expect("hash web report fixture model output");
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::LiveInference,
            model_version_id: Some(ctx.model_version_id.clone()),
            decision_policy_snapshot_id: ctx.decision_policy_snapshot_id.clone(),
            market_selection_id: Some(market_selection_id.clone()),
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
        .expect("seed web report fixture model run");
    model_run_id
}

async fn seed_fixture_market_catalog(db: &DatabaseConnection) {
    PgEventRepository::new(db.clone())
        .upsert(make_event(
            FIXTURE_EVENT,
            "Fixture web report event",
            "fixture-web-report",
            MarketCategory::Politics,
        ))
        .await
        .expect("seed fixture event");
    for (market_id, slug) in [
        (FIXTURE_MARKET_A, "fixture-market-a"),
        (FIXTURE_MARKET_B, "fixture-market-b"),
    ] {
        PgMarketRepository::new(db.clone())
            .upsert(make_market(
                market_id,
                FIXTURE_EVENT,
                "Fixture market?",
                slug,
                MarketCategory::Politics,
                Some(Utc::now() + ChronoDuration::days(7)),
            ))
            .await
            .expect("seed fixture market");
    }
}

async fn seed_minimal_market_selection(
    db: &DatabaseConnection,
    decision_policy_snapshot_id: &DecisionPolicySnapshotId,
) -> MarketSelectionId {
    let market_selection_id = MarketSelectionId::from_v7();
    PgMarketSelectionRepository::new(db.clone())
        .create_snapshot(
            NewMarketSelection {
                market_selection_id: market_selection_id.clone(),
                decision_at: Utc::now(),
                decision_policy_snapshot_id: decision_policy_snapshot_id.clone(),
                selector_hash: ContentHash::parse(format!("blake3:{}", "b".repeat(64)))
                    .expect("selector hash"),
                market_count: 2,
                exclusion_summary: SelectionExclusionSummary::default(),
            },
            Vec::new(),
        )
        .await
        .expect("seed market selection");
    market_selection_id
}

fn fixture_report_transaction(
    report: &RecommendationReportInfo,
    recommendations: Vec<RecommendationInfo>,
    feature_parity_state_id: FeatureParityStateId,
) -> NewReportTransaction {
    let report_row = fixture_new_report(report);
    let sampled_feature_parity = report_fixtures::sampled_parity(&report_row);
    let recommendations = recommendations
        .into_iter()
        .map(fixture_new_recommendation)
        .collect::<Vec<_>>();
    let entry_condition_instances = recommendations
        .iter()
        .map(|recommendation| {
            let (state, truth_json, artifact_id, artifact_hash, next_evaluation_at) =
                match &recommendation.trade_plan {
                    RecommendationTradePlan::Frozen { entry, .. } => match &entry.condition {
                        EntryConditionPlan::Immediate => (
                            EntryConditionState::NotRequired,
                            Some(ConditionTruth::Satisfied),
                            None,
                            None,
                            None,
                        ),
                        EntryConditionPlan::Conditional {
                            artifact_id,
                            content_hash,
                        } => (
                            EntryConditionState::Waiting,
                            None,
                            Some(artifact_id.clone()),
                            Some(content_hash.clone()),
                            Some(report.decision_at),
                        ),
                    },
                    RecommendationTradePlan::Unavailable { .. } => {
                        (EntryConditionState::Invalidated, None, None, None, None)
                    }
                };
            NewEntryConditionInstance {
                condition_instance_id: EntryConditionInstanceId::from_v7(),
                recommendation_id: recommendation.recommendation_id.clone(),
                artifact_id,
                artifact_hash,
                state,
                truth_json,
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
        })
        .collect();
    NewReportTransaction {
        feature_parity_state_id: Some(feature_parity_state_id),
        account_snapshot: fixture_account_snapshot(report),
        equity_snapshot: fixture_equity_snapshot(report),
        data_quality_snapshot: fixture_data_quality_snapshot(report),
        portfolio_plan: fixture_portfolio_plan(report),
        report: report_row,
        recommendations,
        entry_condition_artifacts: Vec::new(),
        entry_condition_instances,
        sampled_feature_parity: Some(sampled_feature_parity),
        fact_delivery: Some(report_fixtures::pending_fact_delivery(
            &report.recommendation_report_id,
        )),
        operation_log: fixture_publish_operation_log(report),
    }
}

fn fixture_account_snapshot(report: &RecommendationReportInfo) -> NewAccountSnapshot {
    NewAccountSnapshot {
        account_snapshot_id: report.account_snapshot_ref.clone(),
        as_of: report.decision_at,
        source: report.account_source,
        venue_net_liquidation_usd: report.capital_base_usd,
        capital_base_usd: report.capital_base_usd,
        available_usd: report.capital_base_usd,
        reserved_usd: Usd::ZERO,
        positions_json: AccountPositions(Vec::new()),
        exposures_json: ExposureBreakdown::default(),
    }
}

fn fixture_equity_snapshot(report: &RecommendationReportInfo) -> NewEquitySnapshot {
    NewEquitySnapshot {
        equity_snapshot_id: report.equity_snapshot_ref.clone(),
        as_of: report.decision_at,
        source: report.account_source,
        venue_net_liquidation_usd: report.capital_base_usd,
        capital_base_usd: report.capital_base_usd,
        available_usd: report.capital_base_usd,
        reserved_usd: Usd::ZERO,
        realized_pnl_cumulative_usd: Usd::ZERO,
        unrealized_pnl_usd: Usd::ZERO,
        high_water_mark_usd: report.capital_base_usd,
        drawdown_pct: rust_decimal::Decimal::ZERO,
        account_snapshot_ref: Some(report.account_snapshot_ref.clone()),
    }
}

fn fixture_data_quality_snapshot(
    report: &RecommendationReportInfo,
) -> NewReportDataQualitySnapshot {
    NewReportDataQualitySnapshot {
        report_data_quality_snapshot_id: report.data_quality_snapshot_ref.clone(),
        decision_at: report.decision_at,
        decision_policy_snapshot_id: report.decision_policy_snapshot_id.clone(),
        tokens_json: ReportDataQualityTokens(Vec::new()),
    }
}

fn fixture_portfolio_plan(report: &RecommendationReportInfo) -> NewPortfolioPlan {
    NewPortfolioPlan {
        portfolio_plan_id: report.portfolio_plan_id.clone(),
        model_run_id: report.model_run_id.clone(),
        market_selection_id: report.market_selection_id.clone(),
        decision_at: report.decision_at,
        budget_usd: report.capital_base_usd,
        allocated_usd: report.summary_json.total_suggested_usd,
        risk_budget_json: PortfolioRiskBudget::default(),
        constraints_json: PortfolioConstraintsSnapshot::default(),
        rejected_summary: PortfolioRejectedSummary::default(),
        optimizer_meta_json: PortfolioOptimizerMeta::default(),
    }
}

fn fixture_new_report(report: &RecommendationReportInfo) -> NewRecommendationReport {
    NewRecommendationReport {
        recommendation_report_id: report.recommendation_report_id.clone(),
        research_profile_artifact_id: report.profile_ref.artifact_id(),
        report_kind: report.report_kind,
        decision_at: report.decision_at,
        horizon_secs: report.horizon_secs,
        runtime_mode: report.runtime_mode,
        decision_policy_snapshot_id: report.decision_policy_snapshot_id.clone(),
        model_run_id: report.model_run_id.clone(),
        model_version_id: report.model_version_id.clone(),
        market_selection_id: report.market_selection_id.clone(),
        portfolio_plan_id: report.portfolio_plan_id.clone(),
        top_n: report.top_n,
        status: report.status,
        account_source: report.account_source,
        capital_base_usd: report.capital_base_usd,
        account_snapshot_ref: report.account_snapshot_ref.clone(),
        equity_snapshot_ref: report.equity_snapshot_ref.clone(),
        data_quality_snapshot_ref: report.data_quality_snapshot_ref.clone(),
        summary_json: report.summary_json.clone(),
        published_at: report.published_at,
        successor_report_id: report.successor_report_id.clone(),
        superseded_at: report.superseded_at,
        obsoleted_at: report.obsoleted_at,
        valid_until: report.valid_until,
        revoked_at: report.revoked_at,
        expired_at: report.expired_at,
        status_reason: report.status_reason.clone(),
    }
}

fn fixture_new_recommendation(rec: RecommendationInfo) -> NewRecommendation {
    NewRecommendation {
        recommendation_id: rec.recommendation_id,
        research_profile_artifact_id: rec.profile_ref.artifact_id(),
        recommendation_report_id: rec.recommendation_report_id,
        rank: rec.rank,
        market_id: rec.market_id,
        event_id: rec.event_id,
        token_id: rec.token_id,
        outcome_side: rec.outcome_side,
        composite_score: rec.composite_score,
        risk_adjusted_score: rec.risk_adjusted_score,
        confidence: rec.confidence,
        expected_return_bps: rec.expected_return_bps,
        downside_bps: rec.downside_bps,
        identity: rec.identity,
        market_context: rec.market_context,
        rank_before_portfolio: rec.rank_before_portfolio,
        liquidity_score: rec.liquidity_score,
        data_quality_score: rec.data_quality_score,
        model_score_percentile: rec.model_score_percentile,
        trade_plan: rec.trade_plan,
        factor_breakdown: rec.factor_breakdown,
        evidence_refs: rec.evidence_refs,
        execution_eligibility: rec.execution_eligibility,
        valid_from: rec.valid_from,
        valid_until: rec.valid_until,
        status: rec.status,
    }
}

fn fixture_publish_operation_log(report: &RecommendationReportInfo) -> NewOperationLog {
    NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("fixture:{}", report.recommendation_report_id).into(),
        actor_user_id: None,
        actor_username: Some("fixture".to_owned()),
        acting_role: Some("test".into()),
        category: OperationCategory::QuantReport,
        action: "publish".into(),
        resource_type: Some(ResourceType::QuantReport),
        resource_id: Some(report.recommendation_report_id.to_string()),
        http_method: OperationHttpMethod::System,
        http_path: "/test/quant/report".to_owned(),
        http_status: 201,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: OperationDetailDocument::try_from(serde_json::json!({ "fixture": true }))
            .expect("static operation detail"),
        before_hash: None,
        after_hash: None,
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    }
}

fn recording_alerts() -> Arc<AlertDispatcher> {
    Arc::new(AlertDispatcher::with_recordings(Arc::new(Mutex::new(
        Vec::new(),
    ))))
}

fn calibration_artifact_loader(db: &DatabaseConnection) -> Arc<dyn CalibrationArtifactLoader> {
    Arc::new(CoreCalibrationArtifactLoader::new(
        Arc::new(PgCalibrationArtifactRepository::new(db.clone()))
            as Arc<dyn CalibrationArtifactRepository>,
    ))
}

fn build_model_runner(
    db: &DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
    calibration_loader: Arc<dyn CalibrationArtifactLoader>,
) -> Arc<ModelRunner> {
    let factor_repo = Arc::new(PgFactorRepository::new(db.clone())) as Arc<dyn FactorRepository>;
    let bias_table_repo = Arc::new(PgCalibrationArtifactRepository::new(db.clone()))
        as Arc<dyn CalibrationArtifactRepository>;
    let bias_table = Arc::new(BiasTableApplicator::new(bias_table_repo));
    let factor_pipeline = Arc::new(FactorPipelineService::new(
        Arc::clone(&factor_repo),
        noop_factor_writer(),
        Arc::clone(&bias_table),
    ));
    Arc::new(ModelRunner::new(ModelRunnerDeps {
        model_run_repo: Arc::new(PgModelRunRepository::new(db.clone())),
        model_registry_repo: Arc::new(PgModelRegistryRepository::new(db.clone())),
        shadow_comparison_repo: Arc::new(PgShadowComparisonRepository::new(db.clone())),
        factory_builder: Arc::new(DefaultModelRuntimeFactoryBuilder::new(
            store,
            calibration_loader,
        )),
        factor_pipeline,
        signal_writer: noop_signal_writer(),
        model_input_writer: noop_model_input_writer(),
        alerts: Arc::new(DispatcherAlertSink::new(recording_alerts())),
        weight_overlay: Arc::new(WeightOverlayApplicator::new()),
        bias_table,
    }))
}

struct ReportBuilderHarnessInput<'a> {
    db: &'a DatabaseConnection,
    runtime_config_repo: Arc<dyn PolicyRepository>,
    registry: &'a Arc<MarketRegistry>,
    book_store: &'a Arc<BookStore>,
    model_runner: Arc<ModelRunner>,
    account_factory: Arc<AccountProviderFactory>,
    artifact_store: Arc<dyn ArtifactStore>,
    calibration_loader: Arc<dyn CalibrationArtifactLoader>,
}

fn build_report_builder(input: ReportBuilderHarnessInput<'_>) -> Arc<DefaultReportBuilder> {
    let ReportBuilderHarnessInput {
        db,
        runtime_config_repo,
        registry,
        book_store,
        model_runner,
        account_factory,
        artifact_store,
        calibration_loader,
    } = input;
    let pit_source: Arc<dyn PointInTimeSnapshotSource> = Arc::new(
        InMemoryDecisionSnapshotSource::freeze_with_zero_fee_schedule(
            registry.as_ref(),
            book_store.as_ref(),
        ),
    );
    Arc::new(DefaultReportBuilder::new(ReportBuilderDeps {
        runtime_config_repo,
        artifact_store,
        calibration_loader,
        trade_policy_repo: Arc::new(PgTradePolicyRepository::new(db.clone()))
            as Arc<dyn TradePolicyRepository>,
        market_selector: Arc::new(ConfiguredMarketSelector::new()),
        market_selection_repo: Arc::new(PgMarketSelectionRepository::new(db.clone())),
        candidate_provider: Arc::new(MarketCandidateProvider::new(
            Arc::clone(&pit_source),
            Arc::new(EmptyLinkageRepo),
            Arc::new(EmptyFactRead),
        )),
        feature_pipeline: Arc::new(FeaturePipelineService::new(FeaturePipelineDeps {
            window_provider: FeatureWindowProvider::new(Arc::new(EmptyFactRead)),
            feature_repo: Arc::new(PgFeatureRepository::new(db.clone())),
            event_writer: noop_feature_writer(),
            market_registry: Arc::clone(registry),
            block_cursor_repo: live_trade_tape_block_cursor_repo(),
            linkage_repo: Arc::new(EmptyLinkageRepo),
            basis_alert_repo: Arc::new(EmptyBasisAlertRepo),
            calibration_repo: Arc::new(PgCalibrationArtifactRepository::new(db.clone())),
            trade_tape_on_chain: TradeTapeOnChainConfig::default(),
        })),
        model_runner,
        account_provider_factory: account_factory,
        drawdown_provider: Arc::new(EquitySnapshotService::new(
            Arc::new(PgEquitySnapshotRepository::new(db.clone()))
                as Arc<dyn EquitySnapshotRepository>,
            Arc::new(PgPositionRepository::new(db.clone())) as Arc<dyn PositionRepository>,
        )),
        composer: Arc::new(DefaultRecommendationComposer::new()),
        quant_fact_read_repo: Arc::new(EmptyFactRead),
        correlation_estimator: Arc::new(HistoricalCorrelationEstimator::new()),
        runtime_mode: RuntimeModeHandle::default(),
        readiness_gate: Arc::new(AlwaysOperationalGate),
    }))
}

async fn build_lifecycle_service(
    db: &DatabaseConnection,
    runtime_config_repo: Arc<dyn PolicyRepository>,
    builder: Arc<DefaultReportBuilder>,
    artifact_store: Arc<dyn ArtifactStore>,
) -> ReportLifecycleService {
    let metrics = Arc::new(MetricsHub::new());
    let (events, _rx) = CoreEventPublisher::bounded(64);
    let report_repo = Arc::new(PgRecommendationReportRepository::new(db.clone()));
    let feature_parity_runs = Arc::new(FeatureParityRunCoordinator::new(
        Arc::new(PgFeatureParityRepository::new(db.clone())) as Arc<dyn FeatureParityRepository>,
        Arc::clone(&runtime_config_repo),
        3,
    ));
    ReportLifecycleService::new(ReportLifecycleDeps {
        report_repo: Arc::clone(&report_repo) as Arc<dyn RecommendationReportRepository>,
        run_repo: Arc::new(PgReportRunRepository::new(db.clone())) as Arc<dyn ReportRunRepository>,
        recommendation_repo: Arc::new(PgRecommendationRepository::new(db.clone()))
            as Arc<dyn RecommendationRepository>,
        builder,
        publisher: Arc::new(ReportPublisher::new(ReportPublisherDeps {
            events,
            alerts: recording_alerts(),
            metrics: Arc::clone(&metrics),
        })),
        feature_parity_gate: Arc::new(ClearFeatureParityGate {
            state_id: seed_clear_feature_parity_state(db).await,
        }),
        feature_parity_runs,
        artifact_store,
        ad_hoc_queue_capacity: 64,
        ad_hoc_queue_ttl_secs: 300,
    })
}

async fn seed_clear_feature_parity_state(db: &DatabaseConnection) -> FeatureParityStateId {
    if let Some(state) = PgFeatureParityRepository::new(db.clone())
        .current_state()
        .await
        .expect("load feature parity state")
    {
        assert_eq!(
            state.state,
            FeatureParityLatchState::Clear,
            "report fixture must not bypass an open parity latch"
        );
        return state.state_id;
    }
    let state_id = FeatureParityStateId::from_v7();
    quant_feature_parity_state::Entity::insert(
        NewFeatureParityState {
            state_id: state_id.clone(),
            state: FeatureParityLatchState::Clear,
            transition: FeatureParityStateTransition::GovernedAcknowledge,
            cause_run_id: None,
            recovery_run_id: None,
            previous_state_id: None,
            actor: Some("report-pipeline-test".to_owned()),
            acting_role: Some("risk_owner".to_owned()),
            reason: "test fixture clear generation".to_owned(),
        }
        .into_active_model(),
    )
    .exec(db)
    .await
    .expect("seed feature parity clear generation");
    state_id
}

fn registry_market(
    market_id: &str,
    yes_token: &str,
    no_token: &str,
    question: &str,
    slug: &str,
    liquidity_usd: Usd,
    volume_24h_usd: Usd,
) -> MarketRegistryInfo {
    MarketRegistryInfo {
        market_id: MarketId::new(market_id),
        event_id: EventId::new(EVENT_ID),
        token_yes: TokenId::new(yes_token),
        token_no: TokenId::new(no_token),
        question: question.into(),
        slug: slug.into(),
        description: None,
        categories: CategorySet::from(MarketCategory::Sports),
        status: MarketStatus::Active,
        filter_reasons: CatalogFilterReasonSet::default(),
        outcome: None,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![
            TokenInfo {
                token_id: TokenId::new(yes_token),
                outcome: "Yes".into(),
                neg_risk: false,
            },
            TokenInfo {
                token_id: TokenId::new(no_token),
                outcome: "No".into(),
                neg_risk: false,
            },
        ],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: Decimal::ONE,
        liquidity_usd: Some(liquidity_usd),
        volume_24h: Some(volume_24h_usd),
        start_date: Some(Utc::now() - ChronoDuration::hours(1)),
        end_date: Some(Utc::now() + ChronoDuration::days(2)),
        resolved_at: None,
        created_at: Some(Utc::now() - ChronoDuration::days(2)),
        updated_at: Utc::now(),
    }
}

fn apply_book_snapshot(book_store: &BookStore, yes_token: &str, bid_shares: i64, ask_shares: i64) {
    book_store.apply_snapshot(
        &TokenId::new(yes_token),
        Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(Decimal::new(47, 2)),
            Shares::new(Decimal::from(bid_shares)),
        )]),
        Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(Decimal::new(53, 2)),
            Shares::new(Decimal::from(ask_shares)),
        )]),
        u64::try_from(Utc::now().timestamp_millis())
            .expect("test book timestamp must be non-negative"),
        None,
    );
}

async fn seed_catalog(db: &DatabaseConnection) {
    PgEventRepository::new(db.clone())
        .upsert(make_event(
            EVENT_ID,
            "Report Pipeline E2E",
            "report-pipeline-e2e",
            MarketCategory::Sports,
        ))
        .await
        .expect("seed event");
    PgMarketRepository::new(db.clone())
        .upsert(make_market(
            MARKET_ID,
            EVENT_ID,
            "Report pipeline E2E?",
            "report-pipeline-e2e",
            MarketCategory::Sports,
            Some(Utc::now() + ChronoDuration::days(2)),
        ))
        .await
        .expect("seed market");
    PgMarketRepository::new(db.clone())
        .upsert(make_market(
            MARKET_ID_2,
            EVENT_ID,
            "Report pipeline E2E second?",
            "report-pipeline-e2e-2",
            MarketCategory::Sports,
            Some(Utc::now() + ChronoDuration::days(2)),
        ))
        .await
        .expect("seed second market");
}

fn wire_live_book(registry: &MarketRegistry, book_store: &BookStore) {
    let primary = registry_market(
        MARKET_ID,
        YES_TOKEN,
        NO_TOKEN,
        "Report pipeline E2E?",
        "report-pipeline-e2e",
        Usd::new(Decimal::from(60_000)),
        Usd::new(Decimal::from(9_000)),
    );
    let secondary = registry_market(
        MARKET_ID_2,
        YES_TOKEN_2,
        NO_TOKEN_2,
        "Report pipeline E2E second?",
        "report-pipeline-e2e-2",
        Usd::new(Decimal::from(25_000)),
        Usd::new(Decimal::from(4_500)),
    );
    registry.register_event(EventRegistryInfo {
        event_id: EventId::new(EVENT_ID),
        title: "Report Pipeline E2E".to_owned(),
        slug: "report-pipeline-e2e".to_owned(),
        series_slug: None,
        status: EventStatus::Active,
        market_ids: vec![primary.market_id.clone(), secondary.market_id.clone()],
        categories: CategorySet::from(MarketCategory::Sports),
        tags: vec![MarketCategory::Sports.as_str().to_owned()],
        neg_risk: false,
        end_date: primary.end_date,
        created_at: Utc::now() - ChronoDuration::days(2),
        updated_at: primary.updated_at.max(secondary.updated_at),
    });
    registry.register_market(primary);
    registry.register_market(secondary);
    // Distinct visible depth so rank-normalized liquidity_depth differs across the cross-section.
    apply_book_snapshot(book_store, YES_TOKEN, 150, 140);
    apply_book_snapshot(book_store, YES_TOKEN_2, 80, 60);
}

fn factors_config() -> FactorsConfig {
    FactorsConfig {
        enabled_factor_families: vec![
            FactorFamily::Liquidity,
            FactorFamily::Microstructure,
            FactorFamily::Resolution,
            FactorFamily::DataQuality,
        ],
        cross_section: FactorCrossSectionConfig {
            min_size: 2,
            ..FactorCrossSectionConfig::default()
        },
        ..FactorsConfig::default()
    }
}

fn selection_config(preset: SelectionPreset) -> SelectionConfig {
    match preset {
        SelectionPreset::Standard => SelectionConfig {
            enabled_categories: vec![MarketCategory::Sports],
            ..SelectionConfig::default()
        },
        SelectionPreset::Empty => SelectionConfig {
            enabled_categories: vec![MarketCategory::Politics],
            ..SelectionConfig::default()
        },
    }
}

fn runtime_config_for_pipeline(
    model_version_id: &ModelVersionId,
    selection: SelectionConfig,
    factors: &FactorsConfig,
    features: &FeaturesConfig,
) -> DecisionPolicySnapshot {
    let mut config = DecisionPolicySnapshot::default();
    config.recommendation.selection = selection;
    config.profile_artifacts.scoring.definition = factors.clone();
    config.profile_artifacts.features.definition = features.clone();
    config.model_routing.model = ModelConfig {
        active_model_version_id: Some(ModelVersionRef::new(model_version_id.clone())),
        min_model_confidence: DecimalValue::new(rust_decimal_macros::dec!(0.00)),
        candidate_score_floor: DecimalValue::new(rust_decimal_macros::dec!(0.00)),
        ..ModelConfig::default()
    };
    config.execution_risk.portfolio = PortfolioConfig {
        budget: PortfolioBudget {
            total_budget_usd: DecimalValue::new(rust_decimal_macros::dec!(50000)),
            min_recommendation_usd: DecimalValue::new(rust_decimal_macros::dec!(10)),
            max_single_recommendation_usd: DecimalValue::new(rust_decimal_macros::dec!(5000)),
        },
        constraints: PortfolioConstraints {
            max_market_exposure_usd: DecimalValue::new(rust_decimal_macros::dec!(10000)),
            max_event_exposure_usd: DecimalValue::new(rust_decimal_macros::dec!(10000)),
            max_category_exposure_usd: DecimalValue::new(rust_decimal_macros::dec!(20000)),
            ..PortfolioConstraints::default()
        },
        ..PortfolioConfig::default()
    };
    config.recommendation.reports = ReportsConfig {
        ad_hoc_report_enabled: true,
        ..ReportsConfig::default()
    };
    config
}

fn account_factory(
    db: &DatabaseConnection,
    registry: Arc<MarketRegistry>,
    options: &HarnessOptions,
) -> Arc<AccountProviderFactory> {
    let reserved_repo: Arc<dyn ReservedCapitalRepository> =
        Arc::new(PgReservedCapitalRepository::new(db.clone()));
    let reserved_reader: Arc<dyn ReservedCapitalReader> =
        Arc::new(RepoReservedCapitalReader::new(reserved_repo));
    let client: Option<Arc<dyn PolymarketAccountClient>> = match options.account {
        AccountFixture::Stub => Some(Arc::new(StubAccountClient {
            collateral: options.collateral,
        })),
        AccountFixture::Unavailable => None,
    };
    Arc::new(AccountProviderFactory::new(
        client,
        registry,
        reserved_reader,
        Some(STUB_FUNDER.to_owned()),
    ))
}

struct WeightedModelFixture<'a> {
    db: &'a DatabaseConnection,
    store: &'a Arc<dyn ArtifactStore>,
    factors: &'a FactorsConfig,
    features: &'a FeaturesConfig,
    domain: &'a DomainConfig,
    model_version_id: &'a ModelVersionId,
    decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    bind_trade_policy: bool,
}

async fn publish_weighted_model(input: &WeightedModelFixture<'_>) {
    let engine = FactorEngine::new(input.factors, input.features, input.domain, None);
    let factor_set = engine.factor_set();
    let count = factor_set.definitions.len();
    assert!(count > 0, "the factor set must be non-empty");
    let each = Decimal::ONE / Decimal::from(u64::try_from(count).expect("count"));
    let mut weights: Vec<FactorWeight> = factor_set
        .definitions
        .iter()
        .map(|spec| FactorWeight {
            factor: spec.name.clone(),
            weight: each,
        })
        .collect();
    let tail: Decimal = weights.iter().skip(1).map(|w| w.weight).sum();
    weights[0].weight = Decimal::ONE - tail;

    let trade_policy = if input.bind_trade_policy {
        Some(
            seed_report_trade_policy_fixture(
                input.db,
                input.decision_policy_snapshot_id,
                MarketCategory::Sports,
            )
            .await,
        )
    } else {
        None
    };
    let calibration_id =
        seed_model_score_calibration_fixture(input.db, input.model_version_id).await;
    let feature_schema_hash = ResearchHasher::feature_schema(
        &FeatureSchema::build(input.features).expect("feature schema"),
    )
    .expect("feature hash");
    let factor_schema_hash = engine.factor_schema_hash().expect("factor hash");
    let input_contract = ModelInputContract::single_required("book.mid");
    let input_contract_hash =
        model_input_contract_hash(&input_contract).expect("input contract hash");
    let model_spec_id = ModelSpecId::from_v7();
    let spec = new_model_spec_fixture(
        model_spec_id.clone(),
        "report-pipeline-e2e",
        ModelFamily::WeightedFactor,
        86_400,
        input_contract.clone(),
        ModelTrainingContract::settlement_default(),
    );
    let model_spec_definition_hash = spec.definition_hash.clone();

    let artifact = ModelArtifact::WeightedFactor(Box::new(WeightedFactorModelArtifact {
        header: ModelArtifactHeader {
            model_version_id: input.model_version_id.clone(),
            model_spec_definition_hash,
            profile_ref: fixture_profile_ref(),
            model_family: ModelFamily::WeightedFactor,
            feature_schema_hash,
            factor_schema_hash: factor_schema_hash.clone(),
            trade_policy_artifact_id: trade_policy
                .as_ref()
                .map(|policy| policy.artifact_id.clone()),
            trade_policy_hash: trade_policy
                .as_ref()
                .map(|policy| policy.artifact_hash.clone()),
        },
        training_dataset_hash: factor_schema_hash.clone(),
        training_input_hash: factor_schema_hash,
        input_contract: input_contract.clone(),
        input_contract_hash,
        weights,
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
    artifact.validate().expect("artifact valid");
    let artifact_hash = artifact.content_hash().expect("hash");
    let key = ModelArtifact::artifact_key(&artifact_hash).expect("key");
    input
        .store
        .put(key, &artifact.to_bytes().expect("bytes"))
        .await
        .expect("store artifact");

    let registry = PgModelRegistryRepository::new(input.db.clone());
    registry.create_model_spec(spec).await.expect("create spec");
    registry
        .create_model_version(NewModelVersion {
            model_version_id: input.model_version_id.clone(),
            model_spec_id,
            version: 1,
            artifact_hash,
            category_scope: None,
            profile_ref: fixture_profile_ref(),
            training_dataset_id: None,
            trade_policy_artifact_id: trade_policy
                .as_ref()
                .map(|policy| policy.artifact_id.clone()),
            trade_policy_hash: trade_policy.map(|policy| policy.artifact_hash),
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
        .expect("create version");
}

fn artifact_store() -> Arc<dyn ArtifactStore> {
    let root = env::temp_dir().join(format!(
        "qp_report_pipeline_e2e_{}_{}",
        process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    Arc::new(LocalArtifactStore::new(root))
}

fn noop_feature_writer() -> Arc<FeatureEventWriter> {
    Arc::new(FeatureEventWriter::new(Arc::new(DiscardFactWriter::new())))
}

fn noop_factor_writer() -> Arc<FactorEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("report-pipeline-factor").capacity(64),
        |_| Box::pin(async { Ok(()) }),
        prometheus::IntCounter::new("report_pipeline_fac_drops", "d").expect("counter"),
        AsyncWriterObservability::default(),
    );
    Arc::new(FactorEventWriter::new(Arc::new(writer)))
}

fn noop_signal_writer() -> Arc<SignalCandidateEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("report-pipeline-signal").capacity(64),
        |_| Box::pin(async { Ok(()) }),
        prometheus::IntCounter::new("report_pipeline_sig_drops", "d").expect("counter"),
        AsyncWriterObservability::default(),
    );
    Arc::new(SignalCandidateEventWriter::new(Arc::new(writer)))
}

fn noop_model_input_writer() -> Arc<ModelInputEventWriter> {
    Arc::new(ModelInputEventWriter::new(
        Arc::new(DiscardFactWriter::new()),
        Arc::new(DiscardFactWriter::new()),
    ))
}
