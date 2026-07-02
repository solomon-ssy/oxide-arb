//! Shared harness for Phase 04 report pipeline integration tests.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_api::data_api::VenuePosition;
use quant_pivot_core::{
    governance::{RuntimeModeHandle, WeightOverlayApplicator},
    observability::{
        alert_dispatcher::AlertDispatcher, fact_lag::FactLagTracker,
        factor_fact_writer::FactorEventWriter, feature_fact_writer::FeatureEventWriter,
        metrics_hub::MetricsHub, recommendation_fact_writer::RecommendationEventWriter,
        signal_candidate_fact_writer::SignalCandidateEventWriter,
    },
    pipeline::{
        book_store::BookStore, feature_window_provider::FeatureWindowProvider,
        market_candidate_provider::MarketCandidateProvider, market_registry::MarketRegistry,
        point_in_time::LiveBookDataSource,
    },
    report::{
        DefaultRecommendationComposer, DefaultReportBuilder, ReportBuilderDeps,
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
        feature_pipeline::FeaturePipelineService,
        model_runner::{DispatcherAlertSink, ModelRunner, ModelRunnerDeps},
    },
};
use quant_pivot_error::{QuantResult, storage::StorageError};
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, MarketResolutionRow, MidPriceBucketRow,
        TickEventRow,
    },
    domain::{
        CoreEventPublisher, NewAccountSnapshot, NewEquitySnapshot, NewMarketSelection,
        NewModelSpec, NewModelVersion, NewOperationLog, NewPortfolioPlan, NewRecommendation,
        NewRecommendationReport, NewReportDataQualitySnapshot, NewReportTransaction,
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, PointInTimeDataSource,
        RecommendationInfo, RecommendationReportInfo,
        governance::lifecycle::OperationalPhase,
        market::{MarketRegistryInfo, TokenInfo, book::BookLevel},
    },
    enums::{
        common::{CategorySet, MarketCategory, TickSize},
        factor::FactorFamily,
        market::MarketStatus,
        model::ModelFamily,
        operation_log::{OperationCategory, OperationOutcome},
        quant::{OutcomeSide, PublicationStatus, RecommendationReportStatus, ReportKind},
        rbac::ResourceType,
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    hashing::CanonicalDigest,
    runtime_config::{
        DecimalString, FactorsConfig, FeaturesConfig, ModelConfig, ModelVersionRef,
        PortfolioBudget, PortfolioConfig, PortfolioConstraints, RUNTIME_CONFIG_SCHEMA_VERSION,
        ReportsConfig, RuntimeConfig, SelectionConfig,
    },
    types::{
        AccountPositions, ContentHash, EventId, ExposureBreakdown, MarketId, MarketSelectionId,
        ModelSpecId, ModelVersionId, OperationLogId, PortfolioConstraintsSnapshot,
        PortfolioOptimizerMeta, PortfolioRejectedSummary, PortfolioRiskBudget, Price,
        RecommendationId, RecommendationReportId, ReportDataQualityTokens,
        RuntimeConfigActivationId, RuntimeConfigVersionId, SchemaVersion,
        SelectionExclusionSummary, Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgEquitySnapshotRepository, PgEventRepository, PgFactorRepository, PgFeatureRepository,
        PgMarketRepository, PgMarketSelectionRepository, PgModelRegistryRepository,
        PgModelRunRepository, PgPositionRepository, PgRecommendationReportRepository,
        PgRecommendationRepository, PgReservedCapitalRepository, PgRuntimeConfigVersionRepository,
        PgShadowComparisonRepository,
    },
    traits::{
        EquitySnapshotRepository, EventRepository, FactorRepository, MarketRepository,
        MarketSelectionRepository, ModelRegistryRepository, PositionRepository,
        QuantFactReadRepository, RecommendationReportRepository, RecommendationRepository,
        ReservedCapitalRepository, RuntimeConfigVersionRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    factors::FactorEngine,
    features::FeatureSchema,
    hashing::ResearchHasher,
    model::{
        DefaultModelRuntimeFactoryBuilder, FactorWeight, ModelArtifact, ModelArtifactHeader,
        ReturnModelSpec, ScoreMultiplierSpec, SubstitutionConfidenceRules,
        WeightedFactorModelArtifact,
    },
    portfolio::HistoricalCorrelationEstimator,
    selection::ConfiguredMarketSelector,
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

use crate::factor_governance::publish_all_factor_definitions;
use crate::{
    catalog_fixtures::{make_event, make_market},
    report_fixtures,
};

/// Seeded catalog ids shared across report pipeline E2E tests.
pub const EVENT_ID: &str = "evt-report-pipeline-e2e";
pub const MARKET_ID: &str = "0xreportpipelinee2e";
pub const YES_TOKEN: &str = "55555";
pub const NO_TOKEN: &str = "66666";
pub const STUB_FUNDER: &str = "0xfunder";

/// How market selection is configured for a harness bootstrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionPreset {
    /// Sports category matching the seeded catalog market.
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
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self {
            selection: SelectionPreset::Standard,
            account: AccountFixture::Stub,
            collateral: Usd::new(dec!(10_000)),
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
}

/// Wired report lifecycle + repositories for integration tests.
pub struct ReportPipelineHarness {
    pub db: DatabaseConnection,
    pub lifecycle: ReportLifecycleService,
    pub report_repo: Arc<PgRecommendationReportRepository>,
    pub recommendation_repo: Arc<PgRecommendationRepository>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub model_version_id: ModelVersionId,
}

struct AlwaysOperationalGate;

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

struct EmptyFactRead;

#[async_trait]
impl QuantFactReadRepository for EmptyFactRead {
    async fn microstructure_window(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn microstructure_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _minute: bool,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn last_trades(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _limit: u64,
    ) -> Result<Vec<TickEventRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn mid_price_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _bucket_secs: u32,
    ) -> Result<Vec<MidPriceBucketRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn book_snapshot_at(
        &self,
        _token_id: &TokenId,
        _as_of_ms: i64,
    ) -> Result<Option<BookSnapshotRow>, StorageError> {
        Ok(None)
    }

    async fn book_snapshots_between(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
    ) -> Result<Vec<BookSnapshotRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn resolution_at(
        &self,
        _market_id: &MarketId,
        _as_of_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        Ok(None)
    }

    async fn resolutions_between(
        &self,
        _market_ids: Vec<MarketId>,
        _from_ms: i64,
        _to_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError> {
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
        let factor_repo =
            Arc::new(PgFactorRepository::new(db.clone())) as Arc<dyn FactorRepository>;
        publish_all_factor_definitions(factor_repo.as_ref(), &factors, &features)
            .await
            .expect("publish factor definitions");
        let model_version_id = publish_weighted_model(db, &store, &factors, &features).await;

        let runtime_config = runtime_config_for_pipeline(
            &model_version_id,
            selection_config(options.selection),
            &factors,
            &features,
        );
        let runtime_config_repo = Arc::new(PgRuntimeConfigVersionRepository::new(db.clone()))
            as Arc<dyn RuntimeConfigVersionRepository>;
        bootstrap_runtime_config_activation(runtime_config_repo.as_ref(), &runtime_config).await;

        let version = runtime_config_repo
            .load_active_at(Utc::now())
            .await
            .expect("active runtime config")
            .expect("active runtime config row");

        let account_factory = account_factory(db, Arc::clone(&registry), &options);
        let model_runner = build_model_runner(db, store);
        let builder = build_report_builder(
            db,
            Arc::clone(&runtime_config_repo),
            &registry,
            &book_store,
            model_runner,
            account_factory,
        );
        let lifecycle = build_lifecycle_service(db, runtime_config_repo, builder);

        Self {
            db: db.clone(),
            lifecycle,
            report_repo: Arc::new(PgRecommendationReportRepository::new(db.clone())),
            recommendation_repo: Arc::new(PgRecommendationRepository::new(db.clone())),
            runtime_config_version_id: version.runtime_config_version_id,
            model_version_id,
        }
    }
}

/// Activate a runtime config version when the store is empty.
pub async fn bootstrap_runtime_config_activation(
    repo: &dyn RuntimeConfigVersionRepository,
    config: &RuntimeConfig,
) {
    if repo
        .load_current_activation()
        .await
        .expect("activation")
        .is_some()
    {
        return;
    }
    let config_json = config.to_json();
    let config_hash = CanonicalDigest::content_hash_json(&config_json).expect("hash");
    let version = repo
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            config_hash,
            schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
            config_json,
            source: RuntimeConfigVersionSource::Bootstrap,
            created_by: "report-pipeline-it".to_owned(),
            reason: "report pipeline integration test bootstrap".to_owned(),
        })
        .await
        .expect("runtime config version");
    repo.activate_version(NewRuntimeConfigActivation {
        runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
        runtime_config_version_id: version.runtime_config_version_id,
        activated_at: Utc::now(),
        activated_by: "report-pipeline-it".to_owned(),
        reason: "report pipeline integration test bootstrap".to_owned(),
        activation_kind: RuntimeConfigActivationKind::Initial,
        previous_runtime_config_version_id: None,
        rollback_target_version_id: None,
        audit_event_id: None,
    })
    .await
    .expect("runtime config activation");
}

/// Persist a pre-built report transaction via the production repository.
pub async fn seed_published_report(
    db: &DatabaseConnection,
    txn: NewReportTransaction,
) -> RecommendationReportInfo {
    PgRecommendationReportRepository::new(db.clone())
        .create_report(txn)
        .await
        .expect("seed published report")
}

/// Context for seeding fixture reports against a bootstrapped harness database.
#[derive(Debug, Clone)]
pub struct FixtureReportSeedContext {
    pub runtime_config_version_id: RuntimeConfigVersionId,
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
    seed_fixture_market_catalog(db).await;
    let market_selection_id =
        seed_minimal_market_selection(db, &ctx.runtime_config_version_id).await;

    let mut report = report_fixtures::report(
        report_id.clone(),
        ReportKind::TopN,
        RecommendationReportStatus::Published,
    );
    report.runtime_config_version_id = ctx.runtime_config_version_id.clone();
    report.model_version_id = ctx.model_version_id.clone();
    report.market_selection_id = market_selection_id.clone();
    report.trigger_key = format!("fixture:published:{}", report.recommendation_report_id);

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
        rec.event_id = EventId::new(FIXTURE_EVENT);
        rec.evidence_refs.runtime_config_version_id = ctx.runtime_config_version_id.clone();
        rec.evidence_refs.model_version_id = ctx.model_version_id.clone();
        rec.evidence_refs.market_selection_id = market_selection_id.clone();
    }

    let txn = fixture_report_transaction(&report, recommendations);
    seed_published_report(db, txn).await
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
    runtime_config_version_id: &RuntimeConfigVersionId,
) -> MarketSelectionId {
    let market_selection_id = MarketSelectionId::from_v7();
    PgMarketSelectionRepository::new(db.clone())
        .create_snapshot(
            NewMarketSelection {
                market_selection_id: market_selection_id.clone(),
                as_of: Utc::now(),
                runtime_config_version_id: runtime_config_version_id.clone(),
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
) -> NewReportTransaction {
    NewReportTransaction {
        account_snapshot: fixture_account_snapshot(report),
        equity_snapshot: fixture_equity_snapshot(report),
        data_quality_snapshot: fixture_data_quality_snapshot(report),
        portfolio_plan: fixture_portfolio_plan(report, &recommendations),
        report: fixture_new_report(report),
        recommendations: recommendations
            .into_iter()
            .map(fixture_new_recommendation)
            .collect(),
        operation_log: fixture_publish_operation_log(report),
    }
}

fn fixture_account_snapshot(report: &RecommendationReportInfo) -> NewAccountSnapshot {
    NewAccountSnapshot {
        account_snapshot_id: report.account_snapshot_ref.clone(),
        as_of: report.as_of,
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
        as_of: report.as_of,
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
        as_of: report.as_of,
        runtime_config_version_id: report.runtime_config_version_id.clone(),
        tokens_json: ReportDataQualityTokens(Vec::new()),
    }
}

fn fixture_portfolio_plan(
    report: &RecommendationReportInfo,
    _recommendations: &[RecommendationInfo],
) -> NewPortfolioPlan {
    NewPortfolioPlan {
        portfolio_plan_id: report.portfolio_plan_id.clone(),
        model_run_id: None,
        market_selection_id: report.market_selection_id.clone(),
        as_of: report.as_of,
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
        report_kind: report.report_kind,
        trigger_kind: report.trigger_kind,
        trigger_key: report.trigger_key.clone(),
        trigger_time: report.trigger_time,
        source_delay_secs: report.source_delay_secs,
        as_of: report.as_of,
        horizon_secs: report.horizon_secs,
        runtime_mode: report.runtime_mode,
        runtime_config_version_id: report.runtime_config_version_id.clone(),
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
        valid_until: report.valid_until,
        revoked_at: report.revoked_at,
        expired_at: report.expired_at,
        status_reason: report.status_reason.clone(),
    }
}

fn fixture_new_recommendation(rec: RecommendationInfo) -> NewRecommendation {
    NewRecommendation {
        recommendation_id: rec.recommendation_id,
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
        entry_plan: rec.entry_plan,
        sizing_plan: rec.sizing_plan,
        exit_plan: rec.exit_plan,
        risk_envelope: rec.risk_envelope,
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
        request_id: report.trigger_key.clone(),
        actor_user_id: None,
        actor_username: Some("fixture".to_owned()),
        acting_role: Some("test".to_owned()),
        category: OperationCategory::QuantReport,
        action: "publish".to_owned(),
        resource_type: Some(ResourceType::QuantReport),
        resource_id: Some(report.recommendation_report_id.to_string()),
        http_method: "SYSTEM".to_owned(),
        http_path: "/test/quant/report".to_owned(),
        http_status: 201,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: serde_json::json!({ "fixture": true }),
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

fn build_model_runner(db: &DatabaseConnection, store: Arc<dyn ArtifactStore>) -> Arc<ModelRunner> {
    let factor_repo = Arc::new(PgFactorRepository::new(db.clone())) as Arc<dyn FactorRepository>;
    let factor_pipeline = Arc::new(FactorPipelineService::new(
        Arc::clone(&factor_repo),
        noop_factor_writer(),
    ));
    Arc::new(ModelRunner::new(ModelRunnerDeps {
        model_run_repo: Arc::new(PgModelRunRepository::new(db.clone())),
        model_registry_repo: Arc::new(PgModelRegistryRepository::new(db.clone())),
        shadow_comparison_repo: Arc::new(PgShadowComparisonRepository::new(db.clone())),
        factory_builder: Arc::new(DefaultModelRuntimeFactoryBuilder::new(store)),
        factor_pipeline,
        signal_writer: noop_signal_writer(),
        alerts: Arc::new(DispatcherAlertSink::new(recording_alerts())),
        weight_overlay: Arc::new(WeightOverlayApplicator::new()),
    }))
}

fn build_report_builder(
    db: &DatabaseConnection,
    runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository>,
    registry: &Arc<MarketRegistry>,
    book_store: &Arc<BookStore>,
    model_runner: Arc<ModelRunner>,
    account_factory: Arc<AccountProviderFactory>,
) -> Arc<DefaultReportBuilder> {
    let pit_source = Arc::new(LiveBookDataSource::new(
        Arc::clone(book_store),
        Arc::clone(registry),
    )) as Arc<dyn PointInTimeDataSource>;
    Arc::new(DefaultReportBuilder::new(ReportBuilderDeps {
        runtime_config_repo,
        market_selector: Arc::new(ConfiguredMarketSelector::new()),
        market_selection_repo: Arc::new(PgMarketSelectionRepository::new(db.clone())),
        candidate_provider: Arc::new(MarketCandidateProvider::new(
            Arc::clone(registry),
            Arc::clone(book_store),
            Arc::new(FactLagTracker::new()),
        )),
        feature_pipeline: Arc::new(FeaturePipelineService::new(
            FeatureWindowProvider::new(Arc::new(EmptyFactRead)),
            Arc::new(PgFeatureRepository::new(db.clone())),
            noop_feature_writer(),
        )),
        model_runner,
        account_provider_factory: account_factory,
        drawdown_provider: Arc::new(EquitySnapshotService::new(
            Arc::new(PgEquitySnapshotRepository::new(db.clone()))
                as Arc<dyn EquitySnapshotRepository>,
            Arc::new(PgPositionRepository::new(db.clone())) as Arc<dyn PositionRepository>,
        )),
        composer: Arc::new(DefaultRecommendationComposer::new()),
        pit_source,
        quant_fact_read_repo: Arc::new(EmptyFactRead),
        correlation_estimator: Arc::new(HistoricalCorrelationEstimator::new()),
        runtime_mode: RuntimeModeHandle::default(),
        readiness_gate: Arc::new(AlwaysOperationalGate),
    }))
}

fn build_lifecycle_service(
    db: &DatabaseConnection,
    runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository>,
    builder: Arc<DefaultReportBuilder>,
) -> ReportLifecycleService {
    let metrics = Arc::new(MetricsHub::new());
    let (events, _rx) = CoreEventPublisher::bounded(64);
    let report_repo = Arc::new(PgRecommendationReportRepository::new(db.clone()));
    ReportLifecycleService::new(ReportLifecycleDeps {
        report_repo: Arc::clone(&report_repo) as Arc<dyn RecommendationReportRepository>,
        recommendation_repo: Arc::new(PgRecommendationRepository::new(db.clone()))
            as Arc<dyn RecommendationRepository>,
        runtime_config_repo,
        builder,
        publisher: Arc::new(ReportPublisher::new(ReportPublisherDeps {
            events,
            recommendation_writer: noop_recommendation_writer(),
            alerts: recording_alerts(),
            metrics: Arc::clone(&metrics),
        })),
        runtime_mode: RuntimeModeHandle::default(),
        metrics,
    })
}

fn registry_market() -> MarketRegistryInfo {
    MarketRegistryInfo {
        market_id: MarketId::new(MARKET_ID),
        event_id: EventId::new(EVENT_ID),
        token_yes: TokenId::new(YES_TOKEN),
        token_no: TokenId::new(NO_TOKEN),
        question: "Report pipeline E2E?".into(),
        slug: "report-pipeline-e2e".into(),
        categories: CategorySet::from(MarketCategory::Sports),
        status: MarketStatus::Active,
        outcome: None,
        neg_risk: false,
        tick_size: TickSize::Hundredth,
        tokens: vec![
            TokenInfo {
                token_id: TokenId::new(YES_TOKEN),
                outcome: "Yes".into(),
                neg_risk: false,
            },
            TokenInfo {
                token_id: TokenId::new(NO_TOKEN),
                outcome: "No".into(),
                neg_risk: false,
            },
        ],
        best_bid: None,
        best_ask: None,
        depth_usd: None,
        min_order_size: Decimal::ONE,
        liquidity_usd: Some(Usd::new(Decimal::from(60_000))),
        volume_24h: Some(Usd::new(Decimal::from(9_000))),
        fee_schedule: None,
        end_date: Some(Utc::now() + ChronoDuration::days(2)),
        resolved_at: None,
        created_at: Utc::now() - ChronoDuration::days(2),
        updated_at: Utc::now(),
    }
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
}

fn wire_live_book(registry: &MarketRegistry, book_store: &BookStore) {
    registry.register_market(registry_market());
    book_store.apply_snapshot(
        &TokenId::new(YES_TOKEN),
        Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(Decimal::new(47, 2)),
            Shares::new(Decimal::from(150)),
        )]),
        Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(Decimal::new(53, 2)),
            Shares::new(Decimal::from(140)),
        )]),
        u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0),
        None,
    );
}

fn factors_config() -> FactorsConfig {
    FactorsConfig {
        enabled_factor_families: vec![
            FactorFamily::Liquidity,
            FactorFamily::Microstructure,
            FactorFamily::Resolution,
            FactorFamily::DataQuality,
        ],
        ..FactorsConfig::default()
    }
}

fn selection_config(preset: SelectionPreset) -> SelectionConfig {
    match preset {
        SelectionPreset::Standard => SelectionConfig {
            enabled_categories: vec![MarketCategory::Sports],
            max_selection_size: 10,
            ..SelectionConfig::default()
        },
        SelectionPreset::Empty => SelectionConfig {
            enabled_categories: vec![MarketCategory::Politics],
            max_selection_size: 10,
            ..SelectionConfig::default()
        },
    }
}

fn runtime_config_for_pipeline(
    model_version_id: &ModelVersionId,
    selection: SelectionConfig,
    _factors: &FactorsConfig,
    _features: &FeaturesConfig,
) -> RuntimeConfig {
    RuntimeConfig {
        selection,
        factors: factors_config(),
        features: FeaturesConfig::default(),
        model: ModelConfig {
            active_model_version_id: Some(ModelVersionRef {
                id: model_version_id.to_string(),
            }),
            min_model_confidence: DecimalString::new("0.00"),
            candidate_score_floor: DecimalString::new("0.00"),
            ..ModelConfig::default()
        },
        portfolio: PortfolioConfig {
            budget: PortfolioBudget {
                total_budget_usd: DecimalString::new("50000"),
                min_recommendation_usd: DecimalString::new("10"),
                max_single_recommendation_usd: DecimalString::new("5000"),
            },
            constraints: PortfolioConstraints {
                max_market_exposure_usd: DecimalString::new("10000"),
                max_event_exposure_usd: DecimalString::new("10000"),
                max_category_exposure_usd: DecimalString::new("20000"),
                ..PortfolioConstraints::default()
            },
            ..PortfolioConfig::default()
        },
        reports: ReportsConfig {
            publish_empty_reports: true,
            ad_hoc_report_enabled: true,
            ..ReportsConfig::default()
        },
        ..RuntimeConfig::default()
    }
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

async fn publish_weighted_model(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    factors: &FactorsConfig,
    features: &FeaturesConfig,
) -> ModelVersionId {
    let engine = FactorEngine::new(factors, features);
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

    let model_version_id = ModelVersionId::from_v7();
    let feature_schema_hash =
        ResearchHasher::feature_schema(&FeatureSchema::build(features)).expect("feature hash");
    let factor_schema_hash = engine.factor_schema_hash().expect("factor hash");

    let artifact = ModelArtifact::WeightedFactor(Box::new(WeightedFactorModelArtifact {
        header: ModelArtifactHeader {
            model_version_id: model_version_id.clone(),
            model_family: ModelFamily::WeightedFactor,
            feature_schema_hash,
            factor_schema_hash,
        },
        weights,
        prediction_horizon_secs: 86_400,
        multipliers: ScoreMultiplierSpec::conservative(),
        substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
        return_model: ReturnModelSpec::heuristic_default(),
        required_features: Vec::new(),
        objective_report: None,
    }));
    artifact.validate().expect("artifact valid");
    let artifact_hash = artifact.content_hash().expect("hash");
    let key = ModelArtifact::artifact_key(&artifact_hash).expect("key");
    store
        .put(key, &artifact.to_bytes().expect("bytes"))
        .await
        .expect("store artifact");

    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(NewModelSpec {
            model_spec_id: model_spec_id.clone(),
            name: "report-pipeline-e2e".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            prediction_horizon_secs: 86_400,
            feature_schema_version: SchemaVersion::FIRST,
            label_schema_version: SchemaVersion::FIRST,
            spec_json: serde_json::json!({}),
            status: PublicationStatus::Published,
        })
        .await
        .expect("create spec");
    registry
        .create_model_version(NewModelVersion {
            model_version_id: model_version_id.clone(),
            model_spec_id,
            version: 1,
            artifact_hash,
            training_dataset_id: None,
            metrics_json: serde_json::json!({}),
            quality_gate_report: serde_json::json!({}),
            publication_status: PublicationStatus::Published,
            published_at: Some(Utc::now()),
            retired_at: None,
        })
        .await
        .expect("create version");

    model_version_id
}

fn artifact_store() -> Arc<dyn ArtifactStore> {
    let root = std::env::temp_dir().join(format!(
        "qp_report_pipeline_e2e_{}_{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    Arc::new(LocalArtifactStore::new(root))
}

fn noop_feature_writer() -> Arc<FeatureEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("report-pipeline-feature").capacity(64),
        |_| Box::pin(async { Ok(()) }),
        prometheus::IntCounter::new("report_pipeline_feat_drops", "d").expect("counter"),
        AsyncWriterObservability::default(),
    );
    Arc::new(FeatureEventWriter::new(Arc::new(writer)))
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

fn noop_recommendation_writer() -> Arc<RecommendationEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("report-pipeline-rec").capacity(64),
        |_| Box::pin(async { Ok(()) }),
        prometheus::IntCounter::new("report_pipeline_rec_drops", "d").expect("counter"),
        AsyncWriterObservability::default(),
    );
    Arc::new(RecommendationEventWriter::new(Arc::new(writer)))
}
