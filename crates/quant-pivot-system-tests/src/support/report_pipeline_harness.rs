//! Shared harness for report-pipeline system scenarios.

use std::{
    collections::BTreeMap,
    env, process,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use prometheus::IntCounter;
use quant_pivot_api::data_api::VenuePosition;
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_core::{
    governance::{CoreCalibrationArtifactLoader, RuntimeControlsHandle},
    ingest::{book_store::BookStore, data_plane_index::DataPlane, market_registry::MarketRegistry},
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
        BookL2LedgerRow, BookMicrostructureRow, ChDecimal64, ChSchemaVersion, DomainObservationRow,
        MarketResolutionRow, MidPriceBucketRow, TradeTapeRow, WeatherForecastFactRow,
    },
    config::TradeTapeOnChainConfig,
    domain::{
        api::{BasisAlertListQuery, MarketLinkageListQuery},
        data_plane::DecisionBoundary,
        governance::{NewOperationLog, lifecycle::OperationalPhase},
        market::{
            EventRegistryInfo, MarketRegistryInfo, TokenInfo,
            book::{BookLevel, BookSnapshot},
        },
        pagination::Paginated,
        quant::{
            BasisAlertInfo, ClaimReportSchedule, GroundingProof, LinkageOutcome,
            MarketLinkageDerivation, MarketLinkageInfo, MarketSubject, NewAccountSnapshot,
            NewBasisAlert, NewEntryConditionInstance, NewEquitySnapshot, NewExecutionAccount,
            NewFeatureParityState, NewMarketLinkage, NewMarketSelection, NewModelRun,
            NewModelVersion, NewPortfolioPlan, NewRecommendation, NewRecommendationReport,
            NewReportDataQualitySnapshot, NewReportTransaction, OverrideContext,
            RecommendationInfo, RecommendationReportInfo, ReportRunClaimConfig, ResolvedBinding,
            ResolvedSourceBinding, WeatherDecisionGroupKey, WeatherSubject,
        },
        runtime::CoreEventPublisher,
    },
    entities::quant_feature_parity_state::Entity,
    enums::{
        catalog::CatalogFilterReasonSet,
        common::{CategorySet, MarketCategory, TickSize},
        domain::{DomainFamily, LinkageSourceRole, ResolverTier},
        factor::FactorFamily,
        market::{EventStatus, MarketStatus},
        model::ModelFamily,
        operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
        quant::{
            DatasetPurpose, DownsideSource, EntryConditionState, ExecutionWalletKind,
            FeatureParityLatchState, FeatureParityStateTransition, ModelRunKind, OutcomeSide,
            RecommendationReportStatus, ReportKind,
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
        DomainInstrumentKey, DomainSourceId, EntryConditionFoldState, EntryConditionInstanceId,
        EntryConditionPlan, EventId, EvmAddress, ExecutionAccountId, ExposureBreakdown,
        FeatureParityStateId, IcaoStation, MarketId, MarketLinkageId, MarketSelectionId,
        ModelInputContract, ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId,
        OperationDetailDocument, OperationLogId, PortfolioConstraintsSnapshot,
        PortfolioOptimizerMeta, PortfolioRejectedSummary, PortfolioRiskBudget, Price, Probability,
        RecommendationId, RecommendationReportId, RecommendationTradePlan, ReportDataQualityTokens,
        ResearchProfileRef, ResolverVersion, RoleCode, SchemaVersion, SelectionExclusionSummary,
        Shares, TemperatureBand, TemperatureUnit, TokenId, TrainingDatasetId, Usd,
        WeatherContractFinalizationPolicy, WeatherTemperatureStatistic, WorkerId,
        domain_capability::{DomainMeasurementUnit, WeatherVariable},
        factor::FactorServingPlane,
        model_lineage::ModelVersionDerivation,
        model_metrics::ModelVersionMetrics,
        model_training::ModelTrainingObjective,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCalibrationArtifactRepository, PgEquitySnapshotRepository, PgEventRepository,
        PgExecutionAccountRepository, PgFactorRepository, PgFeatureParityRepository,
        PgFeatureRepository, PgMarketLinkageRepository, PgMarketRepository,
        PgMarketSelectionRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgPolicyRepository, PgPositionRepository, PgRecommendationReportRepository,
        PgRecommendationRepository, PgReportRunRepository, PgReservedCapitalRepository,
        PgShadowComparisonRepository, PgTradePolicyRepository,
    },
    traits::{
        BasisAlertRepository, CalibrationArtifactRepository, EquitySnapshotRepository,
        EventRepository, ExecutionAccountRepository, FactorRepository, FeatureParityRepository,
        MarketLinkageRepository, MarketRepository, MarketSelectionRepository,
        ModelRegistryRepository, ModelRunRepository, PolicyRepository, PositionRepository,
        QuantFactReadRepository, RecommendationReportRepository, RecommendationRepository,
        ReportRunRepository, ReservedCapitalRepository, TradePolicyRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    factors::FactorEngine,
    features::FeatureSchema,
    hashing::ResearchHasher,
    model::{CalibratedReturnModel, CalibrationArtifactLoader, ReturnModelSpec},
    pit::PointInTimeSnapshotSource,
    portfolio::HistoricalCorrelationEstimator,
    selection::ConfiguredMarketSelector,
};
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{DatabaseConnection, EntityTrait, IntoActiveModel};

use super::{
    artifact_store::VersionedArtifactStoreFixture,
    catalog_fixtures::{make_event, make_market},
    execution_pg_seed::{fixture_profile_ref, seed_score_calibration},
    fact_sink::DiscardFactWriter,
    factor_definitions::register_all_factor_definitions,
    model_serving_fixtures::{
        ModelArtifactFixtureSeed, ModelBindingFixture, ModelDatasetLedgerFixture,
        ModelDatasetLedgerSeed, ModelPayloadFixture, ModelVersionFixture, SealedModelFixture,
    },
    model_serving_runtime::ModelServingRegistryFixture,
    model_spec_fixtures::{
        new_model_spec_fixture, pooled_horizon_secs, pooled_profile_ref, weather_horizon_secs,
    },
    pit::InMemoryDecisionSnapshotSource,
    policy_fixtures::bootstrap_policy_bundle,
    report_fixtures,
    report_lifecycle_seed::{persist_and_publish_report, persist_prepared_report},
    trade_policy_fixtures::PublishedTradePolicyFixture,
    trade_tape_fixtures::live_tape_cursor_repo,
};

/// Seeded catalog ids shared across report pipeline E2E tests.
pub const EVENT_ID: &str = "evt-report-pipeline-e2e";
pub const MARKET_ID: &str = "0xreportpipelinee2e";
pub const MARKET_ID_2: &str = "0xreportpipelinee2e2";
pub const YES_TOKEN: &str = "55555";
pub const NO_TOKEN: &str = "66666";
pub const YES_TOKEN_2: &str = "55556";
pub const NO_TOKEN_2: &str = "66667";
pub const STUB_FUNDER: &str = "0xfunder";

fn harness_execution_account() -> NewExecutionAccount {
    let funder = EvmAddress::parse("0x2222222222222222222222222222222222222222")
        .expect("harness execution account address");
    NewExecutionAccount::build(
        137,
        funder.clone(),
        ExecutionWalletKind::Eoa,
        funder.clone(),
        funder,
        None,
        None,
    )
    .expect("harness execution account identity")
}

async fn ensure_harness_execution_account(db: &DatabaseConnection) -> ExecutionAccountId {
    PgExecutionAccountRepository::new(db.clone())
        .ensure(harness_execution_account())
        .await
        .expect("persist harness execution account")
        .execution_account_id
}

/// How market selection is configured for a harness bootstrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionPreset {
    /// Weather category matching the seeded catalog markets (two markets for rank factors).
    Standard,
    /// Politics-only selection so the Weather markets are excluded.
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
    decision_at: DateTime<Utc>,
}

impl ReportPipelineHarness {
    /// Build an ad-hoc request against the immutable fixture decision slice.
    #[must_use]
    pub fn ad_hoc_request(&self, request_id: impl Into<String>) -> AdHocReportRequest {
        AdHocReportRequest {
            request_id: request_id.into(),
            trigger_time: self.decision_at,
            top_n: Some(5),
            knowledge_lag_secs: Some(0),
        }
    }

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
                    decision_policy_snapshot_id: self.decision_policy_snapshot_id,
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
            .claim_fact_delivery(delivery_worker, 60)
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
        Ok(self.state_id)
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

    async fn latest_for_markets(
        &self,
        _market_ids: &[MarketId],
    ) -> Result<Vec<BasisAlertInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn record_many(&self, alerts: Vec<NewBasisAlert>) -> Result<(), StorageError> {
        if alerts.is_empty() {
            Ok(())
        } else {
            Err(StorageError::InvariantViolation {
                entity: Some("quant_basis_alert"),
                detail: "EmptyBasisAlertRepo is read-only".to_owned(),
            })
        }
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

struct ReportFactRead;

#[async_trait]
impl QuantFactReadRepository for ReportFactRead {
    async fn weather_forecast_facts_between(
        &self,
        stations: Vec<String>,
        valid_from_ms: i64,
        valid_to_ms: i64,
        reference_cutoff_ms: i64,
        decision_at_ms: i64,
    ) -> Result<Vec<WeatherForecastFactRow>, StorageError> {
        if !stations.iter().any(|station| station == "KLGA") || valid_from_ms >= valid_to_ms {
            return Ok(Vec::new());
        }
        let valid_time = valid_to_ms
            .checked_sub(ChronoDuration::hours(1).num_milliseconds())
            .expect("report GEFS target time");
        let visible_at = reference_cutoff_ms.min(decision_at_ms);
        let station = IcaoStation::parse("KLGA").expect("report fixture station");
        let grid_binding_hash =
            ResearchHasher::canonical(&"report-fixture-grid").expect("report grid hash");
        let run_manifest_hash =
            ResearchHasher::canonical(&"report-fixture-gefs-run").expect("report GEFS run hash");
        Ok((0_u16..31)
            .map(|member| WeatherForecastFactRow {
                source_id: DomainSourceId::gefs(),
                instrument_key: DomainInstrumentKey::gefs(&station),
                subject_key: station.to_string(),
                variable: WeatherVariable::TemperatureMaximum.as_str().to_owned(),
                value: ChDecimal64::from(Decimal::from(23 + i64::from(member % 3))),
                unit: DomainMeasurementUnit::Celsius.as_str().to_owned(),
                precision: ChDecimal64::from(dec!(0.1)),
                reference_time: visible_at,
                valid_time,
                published_at: visible_at,
                available_at: visible_at,
                lead_hours: 24,
                member: Some(member),
                revision: 1,
                grid_binding_hash,
                run_manifest_hash,
                report_hash: ResearchHasher::canonical(&(
                    "report-fixture-gefs-point",
                    valid_time,
                    member,
                ))
                .expect("report GEFS point hash"),
                schema_version: ChSchemaVersion::FIRST,
            })
            .collect())
    }

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

    async fn market_tape_window(
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

    async fn book_ledger_snapshot_at(
        &self,
        _token_id: &TokenId,
        _as_of_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<BookL2LedgerRow>, StorageError> {
        Ok(None)
    }

    async fn book_ledger_snapshots_between(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _available_by_ms: i64,
    ) -> Result<Vec<BookL2LedgerRow>, StorageError> {
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
        seed_weather_linkages(db).await;

        let data_plane = Arc::new(DataPlane::new());
        let registry = Arc::new(MarketRegistry::new(Arc::clone(&data_plane)));
        let book_store = Arc::new(BookStore::new(data_plane, Arc::new(MetricsHub::new())));

        let factors = factors_config();
        let features = FeaturesConfig::default();
        let store = artifact_store();
        let calibration_loader = calibration_artifact_loader(db);
        let factor_repo =
            Arc::new(PgFactorRepository::new(db.clone())) as Arc<dyn FactorRepository>;
        let domain = DomainConfig::default();
        register_all_factor_definitions(factor_repo.as_ref(), &factors, &features, &domain)
            .await
            .expect("register immutable factor definitions");
        let model_version_id = ModelVersionId::from_v7();
        let generic_model_version_id = ModelVersionId::from_v7();

        let runtime_config = runtime_config_for_pipeline(
            &generic_model_version_id,
            &model_version_id,
            (options.selection).selection_config(),
            &factors,
            &features,
        );
        let runtime_config_repo =
            Arc::new(PgPolicyRepository::new(db.clone())) as Arc<dyn PolicyRepository>;
        let decision_policy_snapshot_id =
            bootstrap_policy_activation(runtime_config_repo.as_ref(), &runtime_config).await;
        Box::pin(publish_weighted_model(&WeightedModelFixture {
            db,
            store: &store,
            factors: &factors,
            features: &features,
            domain: &domain,
            model_version_id: &model_version_id,
            decision_policy_snapshot_id: &decision_policy_snapshot_id,
            bind_trade_policy: options.bind_trade_policy,
        }))
        .await;
        Box::pin(publish_pooled_model(&PooledModelFixture {
            db,
            store: &store,
            factors: &factors,
            features: &features,
            domain: &domain,
            model_version_id: generic_model_version_id,
            decision_policy_snapshot_id,
        }))
        .await;

        let version = runtime_config_repo
            .load_current()
            .await
            .expect("active runtime config")
            .expect("active runtime config row");

        ensure_harness_execution_account(db).await;
        let account_factory = account_factory(db, Arc::clone(&registry), &options);
        let model_runner = build_model_runner(db, &store).await;
        let feature_parity_state_id = clear_feature_parity(db).await;

        // Freeze the venue snapshot only after every asynchronous bootstrap
        // step. The report claim owns its decision time through the PostgreSQL
        // statement clock; slow model/preimage setup before claim must never
        // age a nominal fixture beyond the governed five-second ceiling.
        let decision_at = wire_live_book(&registry, &book_store);
        let pit_source: Arc<dyn PointInTimeSnapshotSource> =
            Arc::new(InMemoryDecisionSnapshotSource::freeze_zero_fee_at(
                registry.as_ref(),
                book_store.as_ref(),
                decision_at,
            ));
        let candidate_provider = Arc::new(MarketCandidateProvider::new(
            pit_source,
            Arc::new(PgMarketLinkageRepository::new(db.clone())),
            Arc::new(ReportFactRead),
        ));
        let builder = build_report_builder(ReportBuilderHarnessInput {
            db,
            runtime_config_repo: Arc::clone(&runtime_config_repo),
            registry: &registry,
            candidate_provider,
            model_runner,
            account_factory,
            artifact_store: Arc::clone(&store),
            calibration_loader,
        });
        let lifecycle = build_lifecycle_service(
            db,
            runtime_config_repo,
            builder,
            store,
            feature_parity_state_id,
        );

        Self {
            db: db.clone(),
            lifecycle,
            report_repo: Arc::new(PgRecommendationReportRepository::new(db.clone())),
            report_run_repo: Arc::new(PgReportRunRepository::new(db.clone())),
            recommendation_repo: Arc::new(PgRecommendationRepository::new(db.clone())),
            decision_policy_snapshot_id: version.decision_policy_snapshot_id,
            model_version_id,
            decision_at,
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
    seed_scoped_report(db, report_id, ctx, fixture_profile_ref()).await
}

/// Seed a published fixture report in an explicit authority scope.
///
/// All report-owned recommendations receive the same immutable profile
/// reference so same-scope API validation is exercised without corrupting
/// lineage.
pub async fn seed_scoped_report(
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
        report_id,
        ReportKind::TopN,
        RecommendationReportStatus::Published,
    );
    report.decision_policy_snapshot_id = ctx.decision_policy_snapshot_id;
    report.model_version_id = ctx.model_version_id;
    report.market_selection_id = market_selection_id;
    let model_run_id =
        seed_fixture_model_run(db, ctx, &market_selection_id, report.decision_at).await;
    report.model_run_id = Some(model_run_id);
    report.profile_id = profile_ref.id.clone();
    report.profile_ref = profile_ref.clone();

    let mut recommendations = vec![
        report_fixtures::recommendation(
            report_id,
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
        rec.evidence_refs.decision_policy_snapshot_id = ctx.decision_policy_snapshot_id;
        rec.evidence_refs.model_version_id = ctx.model_version_id;
        rec.evidence_refs.model_run_id = model_run_id;
        rec.evidence_refs.market_selection_id = market_selection_id;
    }

    let feature_parity_state_id = clear_feature_parity(db).await;
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
    let runs = PgModelRunRepository::new(db.clone());
    runs.create(NewModelRun {
        model_run_id,
        run_kind: ModelRunKind::LiveInference,
        model_version_id: Some(ctx.model_version_id),
        decision_policy_snapshot_id: ctx.decision_policy_snapshot_id,
        market_selection_id: Some(*market_selection_id),
        window_start: decision_at,
        window_end: decision_at,
        input_hash,
    })
    .await
    .expect("create web report fixture model run");
    runs.succeed(&model_run_id, output_hash, None)
        .await
        .expect("finish web report fixture model run");
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
                market_selection_id,
                decision_at: Utc::now(),
                decision_policy_snapshot_id: *decision_policy_snapshot_id,
                selector_hash: ContentHash::parse(&format!("blake3:{}", "b".repeat(64)))
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
                            Some(*artifact_id),
                            Some(*content_hash),
                            Some(report.decision_at),
                        ),
                    },
                    RecommendationTradePlan::Unavailable { .. } => {
                        (EntryConditionState::Invalidated, None, None, None, None)
                    }
                };
            NewEntryConditionInstance {
                condition_instance_id: EntryConditionInstanceId::from_v7(),
                recommendation_id: recommendation.recommendation_id,
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
        account_snapshot_id: report.account_snapshot_ref,
        execution_account_id: harness_execution_account().execution_account_id,
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

const fn fixture_equity_snapshot(report: &RecommendationReportInfo) -> NewEquitySnapshot {
    NewEquitySnapshot {
        equity_snapshot_id: report.equity_snapshot_ref,
        as_of: report.decision_at,
        source: report.account_source,
        venue_net_liquidation_usd: report.capital_base_usd,
        capital_base_usd: report.capital_base_usd,
        available_usd: report.capital_base_usd,
        reserved_usd: Usd::ZERO,
        realized_pnl_cumulative_usd: Usd::ZERO,
        unrealized_pnl_usd: Usd::ZERO,
        high_water_mark_usd: report.capital_base_usd,
        drawdown_pct: Decimal::ZERO,
        account_snapshot_ref: Some(report.account_snapshot_ref),
    }
}

const fn fixture_data_quality_snapshot(
    report: &RecommendationReportInfo,
) -> NewReportDataQualitySnapshot {
    NewReportDataQualitySnapshot {
        report_data_quality_snapshot_id: report.data_quality_snapshot_ref,
        decision_at: report.decision_at,
        decision_policy_snapshot_id: report.decision_policy_snapshot_id,
        tokens_json: ReportDataQualityTokens(Vec::new()),
    }
}

fn fixture_portfolio_plan(report: &RecommendationReportInfo) -> NewPortfolioPlan {
    NewPortfolioPlan {
        portfolio_plan_id: report.portfolio_plan_id,
        model_run_id: report.model_run_id,
        market_selection_id: report.market_selection_id,
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
        recommendation_report_id: report.recommendation_report_id,
        research_profile_artifact_id: report.profile_ref.artifact_id(),
        report_kind: report.report_kind,
        decision_at: report.decision_at,
        horizon_secs: report.horizon_secs,
        runtime_mode: report.runtime_mode,
        decision_policy_snapshot_id: report.decision_policy_snapshot_id,
        model_run_id: report.model_run_id,
        model_version_id: report.model_version_id,
        market_selection_id: report.market_selection_id,
        portfolio_plan_id: report.portfolio_plan_id,
        top_n: report.top_n,
        status: report.status,
        account_source: report.account_source,
        capital_base_usd: report.capital_base_usd,
        account_snapshot_ref: report.account_snapshot_ref,
        equity_snapshot_ref: report.equity_snapshot_ref,
        data_quality_snapshot_ref: report.data_quality_snapshot_ref,
        summary_json: report.summary_json.clone(),
        published_at: report.published_at,
        successor_report_id: report.successor_report_id,
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

async fn build_model_runner(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
) -> Arc<ModelRunner> {
    let factor_repo = Arc::new(PgFactorRepository::new(db.clone())) as Arc<dyn FactorRepository>;
    let factor_pipeline = Arc::new(FactorPipelineService::new(
        Arc::clone(&factor_repo),
        noop_factor_writer(),
        Arc::new(ComputeExecutor::new().expect("test compute executor")),
    ));
    let evidence_scope =
        PublishedTradePolicyFixture::evidence_scope().expect("report serving evidence scope");
    let serving_generations = ModelServingRegistryFixture {
        db: db.clone(),
        artifact_store: Arc::clone(store),
        evidence_scope,
        evidence_attestor: Some(
            PublishedTradePolicyFixture::evidence_attestor()
                .expect("report serving evidence attestor"),
        ),
    }
    .build_generation()
    .await
    .expect("report serving generation");
    Arc::new(ModelRunner::new(ModelRunnerDeps {
        model_run_repo: Arc::new(PgModelRunRepository::new(db.clone())),
        shadow_comparison_repo: Arc::new(PgShadowComparisonRepository::new(db.clone())),
        serving_generations,
        factor_pipeline,
        signal_writer: noop_signal_writer(),
        model_input_writer: noop_model_input_writer(),
        alerts: Arc::new(DispatcherAlertSink::new(recording_alerts())),
    }))
}

struct ReportBuilderHarnessInput<'a> {
    db: &'a DatabaseConnection,
    runtime_config_repo: Arc<dyn PolicyRepository>,
    registry: &'a Arc<MarketRegistry>,
    candidate_provider: Arc<MarketCandidateProvider>,
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
        candidate_provider,
        model_runner,
        account_factory,
        artifact_store,
        calibration_loader,
    } = input;
    Arc::new(DefaultReportBuilder::new(ReportBuilderDeps {
        runtime_config_repo,
        artifact_store,
        calibration_loader,
        trade_policy_repo: Arc::new(PgTradePolicyRepository::new(db.clone()))
            as Arc<dyn TradePolicyRepository>,
        market_selector: Arc::new(ConfiguredMarketSelector::new()),
        market_selection_repo: Arc::new(PgMarketSelectionRepository::new(db.clone())),
        candidate_provider,
        feature_pipeline: Arc::new(FeaturePipelineService::new(FeaturePipelineDeps {
            compute: Arc::new(ComputeExecutor::new().expect("test compute executor")),
            window_provider: FeatureWindowProvider::new(Arc::new(ReportFactRead)),
            feature_repo: Arc::new(PgFeatureRepository::new(db.clone())),
            event_writer: noop_feature_writer(),
            market_registry: Arc::clone(registry),
            block_cursor_repo: live_tape_cursor_repo(),
            linkage_repo: Arc::new(PgMarketLinkageRepository::new(db.clone())),
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
            harness_execution_account().execution_account_id,
        )),
        composer: Arc::new(DefaultRecommendationComposer::new()),
        quant_fact_read_repo: Arc::new(ReportFactRead),
        correlation_estimator: Arc::new(HistoricalCorrelationEstimator::new()),
        runtime_controls: RuntimeControlsHandle::default(),
        readiness_gate: Arc::new(AlwaysOperationalGate),
    }))
}

fn build_lifecycle_service(
    db: &DatabaseConnection,
    runtime_config_repo: Arc<dyn PolicyRepository>,
    builder: Arc<DefaultReportBuilder>,
    artifact_store: Arc<dyn ArtifactStore>,
    feature_parity_state_id: FeatureParityStateId,
) -> ReportLifecycleService {
    let metrics = Arc::new(MetricsHub::new());
    let (events, _rx) = CoreEventPublisher::bounded(64);
    let report_repo = Arc::new(PgRecommendationReportRepository::new(db.clone()));
    let feature_parity_runs = Arc::new(FeatureParityRunCoordinator::new(
        Arc::new(PgFeatureParityRepository::new(db.clone())) as Arc<dyn FeatureParityRepository>,
        runtime_config_repo,
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
            state_id: feature_parity_state_id,
        }),
        feature_parity_runs,
        artifact_store,
        ad_hoc_queue_capacity: 64,
        ad_hoc_queue_ttl_secs: 300,
    })
}

async fn clear_feature_parity(db: &DatabaseConnection) -> FeatureParityStateId {
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
    Entity::insert(
        NewFeatureParityState {
            state_id,
            state: FeatureParityLatchState::Clear,
            transition: FeatureParityStateTransition::GovernedAcknowledge,
            cause_run_id: None,
            recovery_run_id: None,
            previous_state_id: None,
            actor: Some("report-pipeline-test".to_owned()),
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

struct RegistryMarketFixture<'a> {
    market_id: &'a str,
    yes_token: &'a str,
    no_token: &'a str,
    question: &'a str,
    slug: &'a str,
    liquidity_usd: Usd,
    volume_24h_usd: Usd,
    decision_at: DateTime<Utc>,
}

impl From<RegistryMarketFixture<'_>> for MarketRegistryInfo {
    fn from(fixture: RegistryMarketFixture<'_>) -> Self {
        let observed_at = fixture.decision_at - ChronoDuration::seconds(1);
        Self {
            market_id: MarketId::new(fixture.market_id),
            event_id: EventId::new(EVENT_ID),
            token_yes: TokenId::new(fixture.yes_token),
            token_no: TokenId::new(fixture.no_token),
            question: fixture.question.into(),
            slug: fixture.slug.into(),
            description: None,
            categories: CategorySet::from(MarketCategory::Weather),
            status: MarketStatus::Active,
            filter_reasons: CatalogFilterReasonSet::default(),
            outcome: None,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: TokenId::new(fixture.yes_token),
                    outcome: "Yes".into(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: TokenId::new(fixture.no_token),
                    outcome: "No".into(),
                    neg_risk: false,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: Decimal::ONE,
            liquidity_usd: Some(fixture.liquidity_usd),
            volume_24h: Some(fixture.volume_24h_usd),
            start_date: Some(fixture.decision_at - ChronoDuration::hours(1)),
            end_date: Some(fixture.decision_at + ChronoDuration::days(2)),
            resolved_at: None,
            created_at: Some(fixture.decision_at - ChronoDuration::days(2)),
            updated_at: observed_at,
        }
    }
}

fn apply_book_snapshot(
    book_store: &BookStore,
    yes_token: &str,
    best_bid: Decimal,
    best_ask: Decimal,
    bid_shares: i64,
    ask_shares: i64,
    published_at: DateTime<Utc>,
) {
    let token_id = TokenId::new(yes_token);
    let timestamp_ms = u64::try_from(published_at.timestamp_millis())
        .expect("test book timestamp must be non-negative");
    super::publish_fresh_book(
        book_store,
        &token_id,
        BookSnapshot::new(
            Arc::from([BookLevel::from_decimal_unchecked(
                Price::new(best_bid),
                Shares::new(Decimal::from(bid_shares)),
            )]),
            Arc::from([BookLevel::from_decimal_unchecked(
                Price::new(best_ask),
                Shares::new(Decimal::from(ask_shares)),
            )]),
            timestamp_ms,
            1,
        ),
        1,
    );
}

async fn seed_catalog(db: &DatabaseConnection) {
    PgEventRepository::new(db.clone())
        .upsert(make_event(
            EVENT_ID,
            "Report Pipeline E2E",
            "report-pipeline-e2e",
            MarketCategory::Weather,
        ))
        .await
        .expect("seed event");
    PgMarketRepository::new(db.clone())
        .upsert(make_market(
            MARKET_ID,
            EVENT_ID,
            "Report pipeline E2E?",
            "report-pipeline-e2e",
            MarketCategory::Weather,
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
            MarketCategory::Weather,
            Some(Utc::now() + ChronoDuration::days(2)),
        ))
        .await
        .expect("seed second market");
}

async fn seed_weather_linkages(db: &DatabaseConnection) {
    let effective_at = Utc::now() - ChronoDuration::hours(1);
    let station = IcaoStation::parse("KLGA").expect("report fixture station");
    let group = WeatherDecisionGroupKey {
        temperature_statistic: WeatherTemperatureStatistic::Maximum,
        station: station.clone(),
        timezone: "America/New_York".to_owned(),
        local_date: (Utc::now() + ChronoDuration::days(1)).date_naive(),
        market_unit: TemperatureUnit::Celsius,
        settlement_rule_url: "https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA"
            .to_owned(),
        finalization_policy: WeatherContractFinalizationPolicy::SourceFinalized,
        station_registry_hash: ResearchHasher::canonical(&"report-fixture-station-registry")
            .expect("station registry hash"),
        station_profile_hash: ResearchHasher::canonical(&(
            "KLGA",
            "America/New_York",
            dec!(40.7769),
            dec!(-73.8740),
        ))
        .expect("station profile hash"),
        proxy_methodology_hash: ResearchHasher::canonical(&"report-fixture-weather-proxy-v1")
            .expect("weather proxy hash"),
    };
    let decision_group_id = group
        .decision_group_id()
        .expect("weather decision group id");
    let source_bindings = vec![
        ResolvedSourceBinding {
            role: LinkageSourceRole::LiveEvent,
            source_id: DomainSourceId::aviation_weather(),
            instrument_key: DomainInstrumentKey::aviation_weather(&station),
            available_at: effective_at,
            binding_hash: ResearchHasher::canonical(&"report-fixture-live-binding")
                .expect("live binding hash"),
        },
        ResolvedSourceBinding {
            role: LinkageSourceRole::Forecast,
            source_id: DomainSourceId::gefs(),
            instrument_key: DomainInstrumentKey::gefs(&station),
            available_at: effective_at,
            binding_hash: ResearchHasher::canonical(&"report-fixture-forecast-binding")
                .expect("forecast binding hash"),
        },
        ResolvedSourceBinding {
            role: LinkageSourceRole::HistoricalCalibration,
            source_id: DomainSourceId::ghcnh(),
            instrument_key: DomainInstrumentKey::ghcnh(&station),
            available_at: effective_at,
            binding_hash: ResearchHasher::canonical(&"report-fixture-history-binding")
                .expect("history binding hash"),
        },
    ];
    let bands = [
        (
            MARKET_ID,
            TemperatureBand {
                lower_inclusive: None,
                upper_inclusive: Some(dec!(24)),
            },
        ),
        (
            MARKET_ID_2,
            TemperatureBand {
                lower_inclusive: Some(dec!(25)),
                upper_inclusive: None,
            },
        ),
    ];
    let repository = PgMarketLinkageRepository::new(db.clone());
    for (market_id, outcome_band) in bands {
        let outcome = LinkageOutcome::Resolved(Box::new(ResolvedBinding {
            subject: MarketSubject::Weather(WeatherSubject {
                decision_group_id,
                decision_group: group.clone(),
                outcome_band,
            }),
            source_bindings: source_bindings.clone(),
            grounding: GroundingProof { spans: Vec::new() },
            override_context: Some(OverrideContext {
                reason: "bind deterministic report-pipeline Weather fixture".to_owned(),
                actor: "quant-pivot-system-tests".to_owned(),
            }),
        }));
        let metadata_hash =
            ResearchHasher::canonical(&("report-fixture-market-metadata", market_id))
                .expect("market metadata hash");
        repository
            .append(
                NewMarketLinkage::from_derivation(MarketLinkageDerivation {
                    market_id: MarketId::new(market_id),
                    domain_family: DomainFamily::Weather,
                    outcome,
                    confidence: Probability::ONE,
                    resolver_tier: ResolverTier::Override,
                    resolver_version: ResolverVersion::FIRST,
                    metadata_hash,
                    capability_registry_hash: ResearchHasher::canonical(
                        &"report-fixture-capability-registry",
                    )
                    .expect("capability registry hash"),
                    effective_at,
                })
                .expect("build report Weather linkage"),
            )
            .await
            .expect("persist report Weather linkage");
    }
}

fn wire_live_book(registry: &MarketRegistry, book_store: &BookStore) -> DateTime<Utc> {
    let decision_at = Utc::now();
    let observed_at = decision_at - ChronoDuration::seconds(1);
    let primary: MarketRegistryInfo = RegistryMarketFixture {
        market_id: MARKET_ID,
        yes_token: YES_TOKEN,
        no_token: NO_TOKEN,
        question: "Report pipeline E2E?",
        slug: "report-pipeline-e2e",
        liquidity_usd: Usd::new(Decimal::from(60_000)),
        volume_24h_usd: Usd::new(Decimal::from(9_000)),
        decision_at,
    }
    .into();
    let secondary: MarketRegistryInfo = RegistryMarketFixture {
        market_id: MARKET_ID_2,
        yes_token: YES_TOKEN_2,
        no_token: NO_TOKEN_2,
        question: "Report pipeline E2E second?",
        slug: "report-pipeline-e2e-2",
        liquidity_usd: Usd::new(Decimal::from(25_000)),
        volume_24h_usd: Usd::new(Decimal::from(4_500)),
        decision_at,
    }
    .into();
    registry.register_event(EventRegistryInfo {
        event_id: EventId::new(EVENT_ID),
        title: "Report Pipeline E2E".to_owned(),
        slug: "report-pipeline-e2e".to_owned(),
        series_slug: None,
        status: EventStatus::Active,
        market_ids: vec![primary.market_id.clone(), secondary.market_id.clone()],
        categories: CategorySet::from(MarketCategory::Weather),
        tags: vec![MarketCategory::Weather.to_string()],
        neg_risk: false,
        end_date: primary.end_date,
        created_at: decision_at - ChronoDuration::days(2),
        updated_at: primary.updated_at.max(secondary.updated_at),
    });
    registry.register_market(primary);
    registry.register_market(secondary);
    // Distinct depth and spread make both required cross-sectional factors
    // statistically identifiable rather than relying on a zero-variance tie.
    apply_book_snapshot(
        book_store,
        YES_TOKEN,
        dec!(0.48),
        dec!(0.52),
        200,
        50,
        observed_at,
    );
    apply_book_snapshot(
        book_store,
        NO_TOKEN,
        dec!(0.48),
        dec!(0.52),
        50,
        200,
        observed_at,
    );
    apply_book_snapshot(
        book_store,
        YES_TOKEN_2,
        dec!(0.46),
        dec!(0.54),
        70,
        130,
        observed_at,
    );
    apply_book_snapshot(
        book_store,
        NO_TOKEN_2,
        dec!(0.46),
        dec!(0.54),
        130,
        70,
        observed_at,
    );
    decision_at
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

impl SelectionPreset {
    fn selection_config(self) -> SelectionConfig {
        match self {
            Self::Standard => SelectionConfig {
                enabled_categories: vec![MarketCategory::Weather],
                ..SelectionConfig::default()
            },
            Self::Empty => SelectionConfig {
                enabled_categories: vec![MarketCategory::Politics],
                ..SelectionConfig::default()
            },
        }
    }
}

fn runtime_config_for_pipeline(
    generic_model_version_id: &ModelVersionId,
    weather_model_version_id: &ModelVersionId,
    selection: SelectionConfig,
    factors: &FactorsConfig,
    features: &FeaturesConfig,
) -> DecisionPolicySnapshot {
    let pooled_route = !matches!(
        selection.enabled_categories.as_slice(),
        [MarketCategory::Crypto | MarketCategory::Weather]
    );
    let mut config = DecisionPolicySnapshot::default();
    config.recommendation.selection = selection;
    config.profile_artifacts.scoring.definition = factors.clone();
    config.profile_artifacts.features.definition = features.clone();
    config.model_routing.model = ModelConfig {
        active_model_version_id: pooled_route
            .then(|| ModelVersionRef::new(*generic_model_version_id)),
        category_model_pointers: BTreeMap::from([(
            MarketCategory::Weather,
            ModelVersionRef::new(*weather_model_version_id),
        )]),
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

struct PooledModelFixture<'a> {
    db: &'a DatabaseConnection,
    store: &'a Arc<dyn ArtifactStore>,
    factors: &'a FactorsConfig,
    features: &'a FeaturesConfig,
    domain: &'a DomainConfig,
    model_version_id: ModelVersionId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
}

struct PreparedWeightedModel {
    factor_plane: FactorServingPlane,
    input_contract: ModelInputContract,
    model_spec_id: ModelSpecId,
    training_dataset_id: TrainingDatasetId,
    training_input_hash: ContentHash,
}

async fn publish_weighted_model(input: &WeightedModelFixture<'_>) {
    let prepared = prepare_weighted_model(input).await;
    let source_model_version_id = persist_source_model(input, &prepared).await;
    persist_calibrated_model(input, &prepared, source_model_version_id).await;
}

async fn publish_pooled_model(input: &PooledModelFixture<'_>) {
    let profile_ref = pooled_profile_ref();
    let profile = profile_ref
        .resolve_builtin_research_profile()
        .expect("resolve pooled ResearchProfile");
    let factor_engine =
        FactorEngine::for_model_scope(input.factors, input.features, input.domain, None, None);
    let factor_plane = factor_engine.serving_plane().expect("pooled factor plane");
    let feature_schema_hash = ResearchHasher::feature_schema(
        &FeatureSchema::build(input.features).expect("pooled feature schema"),
    )
    .expect("pooled feature schema hash");
    let model_spec_id = ModelSpecId::from_v7();
    let input_contract = ModelInputContract::single_required("book.mid");
    let spec = new_model_spec_fixture(
        model_spec_id,
        "report-pipeline-pooled-control",
        ModelFamily::WeightedFactor,
        pooled_horizon_secs(),
        input_contract.clone(),
        ModelTrainingContract::settlement_default(),
    );
    let model_spec_definition_hash = spec.definition_hash;
    PgModelRegistryRepository::new(input.db.clone())
        .create_model_spec(spec)
        .await
        .expect("create pooled model spec");
    let window_end = Utc::now() - ChronoDuration::days(60);
    let window_start = window_end - ChronoDuration::days(1);
    let dataset = ModelDatasetLedgerFixture::persist(
        input.db,
        input.store,
        ModelDatasetLedgerSeed {
            scope: format!("report-pipeline-pooled-{}", input.model_version_id),
            model_spec_id,
            model_family: ModelFamily::WeightedFactor,
            model_spec_definition_hash,
            factor_serving_plane: factor_plane.clone(),
            feature_schema_version: SchemaVersion::FIRST,
            feature_schema_hash,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            profile_ref,
            prediction_horizon_secs: profile.spec.target_horizon_secs,
            purpose: DatasetPurpose::Training,
            window_start,
            window_end,
            research_program_hash: ResearchHasher::canonical(&(
                "report-pipeline-pooled-program-v1",
                model_spec_id,
                model_spec_definition_hash,
            ))
            .expect("pooled research program hash"),
            sample_count: 500,
            decision_interval_secs: 1,
            trade_policy: None,
        },
    )
    .await
    .expect("persist pooled model dataset");
    let training_input_hash = ResearchHasher::canonical(&(
        "report-pipeline-pooled-training-input-v1",
        input.model_version_id,
        dataset.training_dataset_id,
    ))
    .expect("pooled training input hash");
    let payload = ModelPayloadFixture::weighted(
        factor_plane,
        &input.factors.factor_head,
        input_contract,
        ReturnModelSpec::heuristic_default(),
        input.factors.cross_section.clone(),
    )
    .expect("pooled weighted model payload");
    let fixture = SealedModelFixture::seal(
        input.db,
        ModelArtifactFixtureSeed {
            model_version_id: input.model_version_id,
            training_dataset_id: dataset.training_dataset_id,
            payload,
            training_input_hash,
            category_scope: None,
            calibration: None,
            bias_table: None,
        },
    )
    .await
    .expect("seal pooled control model");
    fixture
        .store(input.store)
        .await
        .expect("store pooled control artifact");
    let serving_contract = fixture.serving_contract().clone();
    let bindings = serving_contract.bindings();
    let category_scope = bindings.model.category_scope;
    let bound_profile_ref = bindings.model.profile_ref.clone();
    let training_dataset_id = bindings.dataset.manifest.training_dataset_id;
    ModelVersionFixture::persist_route_candidate(
        input.db,
        NewModelVersion {
            model_version_id: input.model_version_id,
            model_spec_id,
            version: 1,
            artifact_hash: fixture.artifact_hash(),
            serving_contract,
            category_scope,
            profile_ref: bound_profile_ref,
            training_dataset_id: Some(training_dataset_id),
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            derivation: NewModelVersion::training_derivation(),
            metrics: ModelVersionMetrics::not_measured("pooled control fixture"),
            training_objective: ModelTrainingObjective::hand_authored("pooled control fixture"),
        },
    )
    .await
    .expect("publish pooled control model");
}

async fn prepare_weighted_model(input: &WeightedModelFixture<'_>) -> PreparedWeightedModel {
    let engine = FactorEngine::for_model_scope(
        input.factors,
        input.features,
        input.domain,
        Some(MarketCategory::Weather),
        None,
    );
    let factor_plane = engine.serving_plane().expect("factor plane");

    let window_end_raw = Utc::now() - ChronoDuration::days(30);
    let window_end = DateTime::from_timestamp_millis(window_end_raw.timestamp_millis())
        .expect("report model window must fit millisecond precision");
    let window_start = window_end - ChronoDuration::days(1);
    let trade_policy = if input.bind_trade_policy {
        Some(
            Box::pin(PublishedTradePolicyFixture::persist(
                input.db,
                input.store,
                *input.decision_policy_snapshot_id,
                "report-pipeline",
                window_start,
            ))
            .await
            .expect("persist complete report TradePolicy preimage"),
        )
    } else {
        None
    };
    let feature_schema_hash = ResearchHasher::feature_schema(
        &FeatureSchema::build(input.features).expect("feature schema"),
    )
    .expect("feature hash");
    let input_contract = ModelInputContract::single_required("book.mid");
    let model_spec_id = ModelSpecId::from_v7();
    let training_contract = trade_policy
        .as_ref()
        .map_or_else(ModelTrainingContract::settlement_default, |policy| {
            policy.target_training_contract()
        });
    let spec = new_model_spec_fixture(
        model_spec_id,
        "report-pipeline-e2e",
        ModelFamily::WeightedFactor,
        weather_horizon_secs(),
        input_contract.clone(),
        training_contract,
    );
    let model_spec_definition_hash = spec.definition_hash;
    let registry = PgModelRegistryRepository::new(input.db.clone());
    registry.create_model_spec(spec).await.expect("create spec");
    let trade_policy_binding = trade_policy.as_ref().map(|policy| {
        let provenance = policy.provenance();
        ModelBindingFixture::trade_policy(provenance.artifact_id, provenance.artifact_hash)
    });
    let dataset = ModelDatasetLedgerFixture::persist(
        input.db,
        input.store,
        ModelDatasetLedgerSeed {
            scope: format!("report-pipeline-{}", input.model_version_id),
            model_spec_id,
            model_family: ModelFamily::WeightedFactor,
            model_spec_definition_hash,
            factor_serving_plane: factor_plane.clone(),
            feature_schema_version: SchemaVersion::FIRST,
            feature_schema_hash,
            decision_policy_snapshot_id: *input.decision_policy_snapshot_id,
            profile_ref: fixture_profile_ref(),
            prediction_horizon_secs: 86_400,
            purpose: DatasetPurpose::Training,
            window_start,
            window_end,
            research_program_hash: ResearchHasher::canonical(&(
                "report-pipeline-program-v1",
                model_spec_id,
                model_spec_definition_hash,
            ))
            .expect("report research program hash"),
            sample_count: 500,
            decision_interval_secs: 1,
            trade_policy: trade_policy_binding,
        },
    )
    .await
    .expect("persist model training dataset");
    let training_input_hash =
        ResearchHasher::canonical(&"report-pipeline-training-input").expect("training input hash");
    PreparedWeightedModel {
        factor_plane: factor_plane.clone(),
        input_contract,
        model_spec_id,
        training_dataset_id: dataset.training_dataset_id,
        training_input_hash,
    }
}

async fn persist_source_model(
    input: &WeightedModelFixture<'_>,
    prepared: &PreparedWeightedModel,
) -> ModelVersionId {
    let registry = PgModelRegistryRepository::new(input.db.clone());
    let factor_plane = &prepared.factor_plane;
    let model_spec_id = prepared.model_spec_id;
    let source_model_version_id = ModelVersionId::from_v7();
    let source_payload = ModelPayloadFixture::weighted(
        factor_plane,
        &input.factors.factor_head,
        prepared.input_contract.clone(),
        ReturnModelSpec::heuristic_default(),
        input.factors.cross_section.clone(),
    )
    .expect("source weighted model payload");
    let source_fixture = SealedModelFixture::seal(
        input.db,
        ModelArtifactFixtureSeed {
            model_version_id: source_model_version_id,
            training_dataset_id: prepared.training_dataset_id,
            payload: source_payload,
            training_input_hash: prepared.training_input_hash,
            category_scope: Some(MarketCategory::Weather),
            calibration: None,
            bias_table: None,
        },
    )
    .await
    .expect("seal source weighted model fixture");
    source_fixture
        .store(input.store)
        .await
        .expect("store source artifact");
    let source_contract = source_fixture.serving_contract().clone();
    let source_bindings = source_contract.bindings();
    let source_trade_policy = source_bindings
        .trade_policy
        .as_ref()
        .map(|binding| (binding.artifact_id, binding.content_hash));
    let source_category_scope = source_bindings.model.category_scope;
    let source_profile_ref = source_bindings.model.profile_ref.clone();
    let source_training_dataset_id = source_bindings.dataset.manifest.training_dataset_id;
    let metrics = ModelVersionMetrics::not_measured("test fixture");
    let objective = ModelTrainingObjective::hand_authored("test fixture");
    registry
        .create_model_version(NewModelVersion {
            model_version_id: source_model_version_id,
            model_spec_id,
            version: 1,
            artifact_hash: source_fixture.artifact_hash(),
            serving_contract: source_contract,
            category_scope: source_category_scope,
            profile_ref: source_profile_ref,
            training_dataset_id: Some(source_training_dataset_id),
            trade_policy_artifact_id: source_trade_policy.map(|binding| binding.0),
            trade_policy_hash: source_trade_policy.map(|binding| binding.1),
            derivation: NewModelVersion::training_derivation(),
            metrics: metrics.clone(),
            training_objective: objective.clone(),
        })
        .await
        .expect("persist source model version");
    source_model_version_id
}

async fn persist_calibrated_model(
    input: &WeightedModelFixture<'_>,
    prepared: &PreparedWeightedModel,
    source_model_version_id: ModelVersionId,
) {
    let calibration_id = Box::pin(seed_score_calibration(
        input.db,
        input.store,
        &source_model_version_id,
    ))
    .await;
    let calibration_repo = PgCalibrationArtifactRepository::new(input.db.clone());
    calibration_repo
        .mark_active(&calibration_id)
        .await
        .expect("activate report calibration");
    let calibration = calibration_repo
        .find_by_id(&calibration_id)
        .await
        .expect("load calibration")
        .expect("calibration row");
    let payload = ModelPayloadFixture::weighted(
        &prepared.factor_plane,
        &input.factors.factor_head,
        prepared.input_contract.clone(),
        ReturnModelSpec::Calibrated(CalibratedReturnModel {
            calibrator_ref: calibration_id,
            downside_source: DownsideSource::MfeMae,
        }),
        input.factors.cross_section.clone(),
    )
    .expect("weighted model payload");
    let fixture = SealedModelFixture::seal(
        input.db,
        ModelArtifactFixtureSeed {
            model_version_id: *input.model_version_id,
            training_dataset_id: prepared.training_dataset_id,
            payload,
            training_input_hash: prepared.training_input_hash,
            category_scope: Some(MarketCategory::Weather),
            calibration: Some(ModelBindingFixture::score_calibration(
                calibration_id,
                calibration.content_hash,
            )),
            bias_table: None,
        },
    )
    .await
    .expect("seal weighted model fixture");
    fixture.store(input.store).await.expect("store artifact");
    let serving_contract = fixture.serving_contract().clone();
    let bindings = serving_contract.bindings();
    let category_scope = bindings.model.category_scope;
    let profile_ref = bindings.model.profile_ref.clone();
    let training_dataset_id = bindings.dataset.manifest.training_dataset_id;
    let trade_policy_binding = bindings
        .trade_policy
        .as_ref()
        .map(|binding| (binding.artifact_id, binding.content_hash));
    let metrics = ModelVersionMetrics::not_measured("test fixture");
    let objective = ModelTrainingObjective::hand_authored("test fixture");
    ModelVersionFixture::persist_route_candidate(
        input.db,
        NewModelVersion {
            model_version_id: *input.model_version_id,
            model_spec_id: prepared.model_spec_id,
            version: 2,
            artifact_hash: fixture.artifact_hash(),
            serving_contract,
            category_scope,
            profile_ref,
            training_dataset_id: Some(training_dataset_id),
            trade_policy_artifact_id: trade_policy_binding.map(|binding| binding.0),
            trade_policy_hash: trade_policy_binding.map(|binding| binding.1),
            derivation: ModelVersionDerivation::ReturnCalibration {
                parent_model_version_id: source_model_version_id,
                calibration_artifact_id: calibration_id,
            },
            metrics,
            training_objective: objective,
        },
    )
    .await
    .expect("publish version through exact parity proof");
}

fn artifact_store() -> Arc<dyn ArtifactStore> {
    static STORE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    let root = env::temp_dir().join(format!(
        "qp_report_pipeline_e2e_{}_{}_{}",
        process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default(),
        STORE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let inner: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(root));
    Arc::new(VersionedArtifactStoreFixture::new(inner))
}

fn noop_feature_writer() -> Arc<FeatureEventWriter> {
    Arc::new(FeatureEventWriter::new(Arc::new(DiscardFactWriter::new())))
}

fn noop_factor_writer() -> Arc<FactorEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("report-pipeline-factor").capacity(64),
        |_| Box::pin(async { Ok(()) }),
        IntCounter::new("report_pipeline_fac_drops", "d").expect("counter"),
        AsyncWriterObservability::default(),
    );
    Arc::new(FactorEventWriter::new(Arc::new(writer)))
}

fn noop_signal_writer() -> Arc<SignalCandidateEventWriter> {
    let (writer, _worker) = AsyncWriter::new(
        AsyncWriterConfig::new("report-pipeline-signal").capacity(64),
        |_| Box::pin(async { Ok(()) }),
        IntCounter::new("report_pipeline_sig_drops", "d").expect("counter"),
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
