//! Real-binary system fixture backed by disposable infrastructure.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    env,
    ffi::OsStr,
    fs::{self, File, OpenOptions, Permissions},
    future::{Future, pending},
    io::{Read, Seek, SeekFrom, Write},
    net::{Ipv4Addr, SocketAddrV4, TcpListener},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    slice,
    string::ToString,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Error as AnyhowError, Result, anyhow, bail, ensure};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    Client as S3ControlClient,
    config::{Builder as S3ConfigBuilder, Credentials, Region},
    types::{
        BucketVersioningStatus, DefaultRetention, ObjectLockConfiguration, ObjectLockEnabled,
        ObjectLockRetentionMode, ObjectLockRule, VersioningConfiguration,
    },
};
use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use clap::ValueEnum;
use quant_pivot_api::{
    data_api::VenuePosition,
    exchange::constants::{CTF_EXCHANGE_V2, EXCHANGE_CONTRACTS, NEG_RISK_EXCHANGE_V2},
};
use quant_pivot_core::{
    app::exchange_history_worker::{
        ExchangeHistoryProgressHandle, ExchangeHistoryWorker, ExchangeHistoryWriters,
    },
    observability::metrics_hub::MetricsHub,
    service::{
        equity::{DrawdownProvider, EquitySnapshotService, ReportEquitySnapshot},
        feedback_cohort::evaluate_feedback_cohort,
        research_readiness::{
            EvidenceAttestor, EvidenceScopeIdentity, ResearchReadinessEvidenceService,
        },
    },
};
use quant_pivot_models::{
    clickhouse::ExchangeHistoryAcceptanceRow,
    config::{
        ArtifactStoreDeployConfig, ArtifactStoreKind, ClickHouseConfig, DeployConfig,
        DeployConfigLoadRequest, FinalizedExchangeHistoryConfig, PolygonRpcEndpoint,
    },
    domain::{
        api::ModelVersionListQuery,
        data_plane::{ExchangeHistoryChunkStatus, ExchangeHistoryFrontier, HistoryServingHeadSeal},
        governance::runtime_control::RuntimeControlSnapshot,
        order::PolymarketOrderRules,
        pagination::PageRequest,
        quant::{
            EconomicOutcomeReplayContext, FEEDBACK_COHORT_PAGE_LIMIT, FeedbackCohortCandidate,
            FeedbackCohortDecision, FeedbackCohortEvidence, FeedbackCohortPageQuery,
            FeedbackCohortSnapshot, FeedbackCohortWindow, FeedbackSchedulerControl,
            NewExecutionAccount, NewFeedbackSchedulerState, PortfolioDecisionResult,
            RecommendationEconomicOutcomeInfo, RecommendationEconomicStateDetail,
            RecommendationResolutionOutcomeInfo, ResearchReadinessEvidenceInfo,
        },
    },
    entities::{
        market::Entity as MarketEntity,
        quant_account_chain_execution::Entity as AccountChainExecutionEntity,
        quant_economic_outcome_reconciliation_task::{
            Column as EconomicTaskColumn, Entity as EconomicTaskEntity,
        },
        quant_feature_parity_run::{
            Column as FeatureParityRunColumn, Entity as FeatureParityRunEntity,
            Model as FeatureParityRunModel,
        },
        quant_feature_vector::Entity as FeatureVectorEntity,
        quant_model_route_shadow_binding::{
            Column as ShadowBindingColumn, Entity as ShadowBindingEntity,
        },
        quant_portfolio_plan::Entity as PortfolioPlanEntity,
        quant_recommendation::{
            Column as RecommendationColumn, Entity as RecommendationEntity,
            Model as RecommendationModel,
        },
        quant_recommendation_economic_outcome::{
            Column as EconomicOutcomeColumn, Entity as EconomicOutcomeEntity,
        },
        quant_recommendation_report::{
            Column as RecommendationReportColumn, Entity as RecommendationReportEntity,
        },
        quant_report_data_quality_snapshot::Entity as ReportDataQualitySnapshotEntity,
        quant_report_route_run::Entity as ReportRouteRunEntity,
        quant_settlement_redeem::Entity as QuantSettlementRedeemEntity,
    },
    enums::{
        common::{MarketCategory, TickSize},
        execution::{AccountChainExecutionRole, ExitReason},
        market::MarketStatus,
        quant::{
            AccountSource, CohortCensorReason, EntryAuthorizationPolicy, ExecutionWalletKind,
            FeatureParityLatchState, FeatureParityRunKind, FeatureParityRunStatus, FeedbackCohort,
            FeedbackDecision, FeedbackStage, FeedbackStageEventKind,
            OutcomeReconciliationTaskStatus, RecommendationReportStatus,
            ResearchReadinessEvidenceKind, ShadowBindingStatus,
        },
        settlement::{SettlementEffectivePolicy, SettlementWritePolicy},
    },
    hashing::CanonicalDigest,
    runtime_config::{ActivePolicyBundle, BuyModelRoute},
    types::{
        ContentHash, DecisionPolicySnapshotId, DeploymentEnvironment, EconomicTierId,
        EntryMakerRebateTerms, EventId, EvmAddress, FeatureCellState, FeatureParityRunId,
        FeatureParityStateId, FeedbackCycleId, FinalizedExecutionEvidence, MarketId,
        ModelVersionId, Price, RecommendationId, RecommendationReportId, ReportFunnelReason,
        ReportRouteRunId, ReportRunId, ResearchJobId, ResearchProfileArtifactId,
        ResearchProfileRef, ResearchReadinessEvidencePayload, ResearchReadinessSource, Shares,
        TokenId, TradePolicyArtifactId, Usd, VenuePositionSnapshot, WorkerId,
        builtin_research_profiles, minimum_raw_retention_days, research_source_registry,
    },
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChQuantFactReadRepository},
    postgres::{
        PgEquitySnapshotRepository, PgExchangeHistoryRepository, PgExecutionAccountRepository,
        PgExecutionSubmissionRepository, PgFeatureParityRepository, PgFeedbackCohortRepository,
        PgFeedbackCycleRepository, PgFeedbackSchedulerRepository, PgMarketRepository,
        PgModelRegistryRepository, PgPolicyRepository, PgRecommendationEconomicOutcomeRepository,
        PgRecommendationResolutionOutcomeRepository, PgResearchReadinessEvidenceRepository,
        PgReservedCapitalRepository, PgRuntimeControlRepository, PgStrategyPositionLotRepository,
        PgVenueIncentiveRepository, policy_bootstrap::ensure_default_policy_bundle,
    },
    traits::{
        EquitySnapshotRepository, ExchangeHistoryRepository, ExecutionAccountRepository,
        FactWriter, FeatureParityRepository, FeedbackCohortRepository, FeedbackCycleClaim,
        FeedbackCycleRepository, FeedbackSchedulerRepository, ModelRegistryRepository,
        PolicyRepository, QuantFactReadRepository, RecommendationEconomicOutcomeRepository,
        RecommendationResolutionOutcomeRepository, ReservedCapitalRepository,
        RuntimeControlRepository, StrategyPositionLotRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore, S3ArtifactStore, S3StaticCredentials},
    model::ModelArtifact,
    policy_replay::POLICY_REPLAY_KERNEL_VERSION,
    portfolio::AccountSnapshot,
};
use quant_pivot_storage::clickhouse::{ChWriteManager, ClickHousePool, ClickHouseQueryLimits};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{
    AccessMode, ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection,
    DbBackend, EntityTrait, FromQueryResult, IntoActiveModel, IsolationLevel, QueryFilter,
    QueryOrder, QuerySelect, Statement, TransactionTrait,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{Mount, WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener as TokioTcpListener, TcpStream},
    process::{Child, Command as TokioCommand},
    signal::{unix, unix::SignalKind},
    sync::{Mutex as TokioMutex, MutexGuard as TokioMutexGuard},
    task::JoinHandle,
    time::{Instant, sleep, timeout, timeout_at},
};
use toml::{Table, Value};
use uuid::Uuid;
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{method, path, path_regex, query_param},
};

use self::cancellation_owner::FixtureCancellationOwner;
use crate::{
    cargo_env::CargoCommandExt,
    performance::upstream::{DeterministicClobRefreshHandle, DeterministicClobServer},
    postgres::PostgresClock,
    stack::{BOOTSTRAP_ADMIN_PASSWORD, SystemStack},
    support::execution_pg_seed::{
        CalibrationEvidencePreset, ENTRY_FILLED_SHARES, ENTRY_PRICE, EXECUTION_NOTIONAL,
        FeedbackServingFixtureConfig, ProductionReportSeed, ReportSeedConfig, SharedDemoInfra,
        enable_test_admission, fill_entry_lot, fixture_no_token_id, fixture_profile_ref,
        seed_approved_intent, seed_browser_serving_infra, seed_demo_with_store,
        seed_feedback_serving_infra, seed_pending_intent, seed_production_report,
    },
    support::feedback_closure_seed::{
        CLOSURE_NEG_RISK, CLOSURE_ORDER_RULES, CLOSURE_REPORT_HORIZON_HOURS,
        FeedbackClosureFixture, FeedbackClosureOutcome, FeedbackClosureSeedRequest,
        FeedbackGammaCatalogGate, FeedbackReportBookSnapshot, FeedbackReportResolutionEvidence,
        FeedbackReportUniverse, closure_market_text, complete_feedback_closure,
        prepare_feedback_report_universe, seed_feedback_closure, settle_feedback_report_universe,
    },
    support::portfolio_scenario_fixtures::finalize_feedback_portfolio,
    support::production_history::{
        DeterministicPolygonChain, DeterministicPolygonHead, HYPERSYNC_TOKEN,
        MODEL_CONFIRMATION_BLOCKS, V2_PRODUCTION_BLOCK, V2_PRODUCTION_BLOCK_HASH,
        polygon_block_hash, start_hypersync,
    },
    support::report_pipeline_harness::publish_pooled_control_model,
    support::research_browser_seed::{
        BrowserResearchFixture, seed_browser_research, seed_closure_feedback_research,
        seed_governed_feedback_research,
    },
    support::research_fixtures::source_fit_acceptance_row,
    support::trade_policy_fixtures::FixtureBookTiming,
    support::trade_policy_fixtures::SYSTEM_EVIDENCE_SIGNING_KEY,
};

#[cfg(test)]
mod browser_report_seed_tests;
mod cancellation_owner;
#[cfg(test)]
mod history_cutpoint_tests;
#[cfg(test)]
mod successor_outcome_tests;

const PRIVATE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const CENT_ORDER_RULES: PolymarketOrderRules = PolymarketOrderRules {
    tick_size: TickSize::Hundredth,
    minimum_order_size: Shares::ONE,
};
const FUNDER: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";
const API_KEY: &str = "00000000-0000-0000-0000-000000000000";
const API_PASSPHRASE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const API_SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const JWT_SIGNING_KEY: &str = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc";
const BROWSER_MARKET_ID: &str =
    "0x8888888888888888888888888888888888888888888888888888888888888888";
const BROWSER_SETTLEMENT_MARKET_ID: &str =
    "0x7777777777777777777777777777777777777777777777777777777777777777";
const BROWSER_SETTLEMENT_TOKEN_ID: &str = "12345";
const BROWSER_TOKEN_ID: &str = "700001";
const ERC1967_IMPLEMENTATION_SLOT: &str =
    "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc";
const HYPERSYNC_ENDPOINT: &str = "http://polygon.hypersync.xyz/hypersync";
const STANDARD_ADAPTER: &str = "0xada100db00ca00073811820692005400218fce1f";
const NEG_RISK_ADAPTER: &str = "0xada2005600dec949baf300f4c6120000bdb6eaab";
const CONTRACT_OWNER: &str = "0x47ebfac3353314c788b96cdcbf41daadfe03629c";
const CONDITIONAL_TOKENS: &str = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045";
const COLLATERAL_TOKEN: &str = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb";
const USDC: &str = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359";
const USDCE: &str = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174";
const COLLATERAL_VAULT: &str = "0xc417fd8e9661c0d2120b64a04bb3278c17e99db1";
const LEGACY_NEG_RISK_ADAPTER: &str = "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296";
const WRAPPED_COLLATERAL: &str = "0x3a3bd7bb9528e159577f7c2e685cc81a765002e2";
const COLLATERAL_IMPLEMENTATION_WORD: &str =
    "0x0000000000000000000000006bbcef9f7ef3b6c592c99e0f206a0de94ad0925f";
const COLLATERAL_IMPLEMENTATION: &str = "0x6bbcef9f7ef3b6c592c99e0f206a0de94ad0925f";
const STARTUP_TIMEOUT: Duration = Duration::from_mins(1);
const OPERATIONAL_READINESS_TIMEOUT: Duration = Duration::from_mins(1);
const HISTORY_READINESS_TIMEOUT: Duration = Duration::from_mins(2);
const BROWSER_SERIES_MIN_POINTS: usize = 8;
const BROWSER_SERIES_READINESS_TIMEOUT: Duration = Duration::from_secs(90);
// A parity attempt waiting on append-only evidence must release its worker slot
// promptly so another queued parity run can establish its own pending clock.
const SAMPLED_PARITY_START_TIMEOUT: Duration = Duration::from_mins(1);
// The production parity executor deliberately enforces a ten-minute minimum
// materialization grace. Start this budget only after the target run has
// durably entered pending_materialization.
const SAMPLED_PARITY_CONTAINMENT_TIMEOUT: Duration = Duration::from_mins(12);
// The deployed replay attempt budget is 30 minutes and evidence may spend up
// to ten minutes in governed materialization before a retry. Two additional
// minutes cover the single-worker lease hand-off without treating 900 seconds
// as a correctness constant.
const RUNTIME_PARITY_COMPLETION_TIMEOUT: Duration = Duration::from_mins(42);
const BROWSER_ACTIVITY_STABILITY_TIMEOUT: Duration = Duration::from_mins(2);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const RECOVERY_POINT_TIMEOUT: Duration = Duration::from_mins(10);
const LEASE_RECOVERY_TIMEOUT: Duration = Duration::from_mins(3);
const BROWSER_CLOSURE_TIMEOUT: Duration = Duration::from_mins(10);
// The debug-profile fresh-stack verifies report correctness and liveness. A
// controlled release-profile full-compute benchmark owns the latency SLO.
const REPORT_COMPLETION_TIMEOUT: Duration = Duration::from_mins(15);
const READINESS_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const BACKEND_LOG_TAIL_BYTES: u64 = 16 * 1024;
// Startup ingestion and ledger reconciliation share the debug-profile runtime
// with the single Actix worker. Keep control-plane requests bounded while
// allowing that finite startup pressure to drain without ambiguous retries.
const GOVERNED_REQUEST_TIMEOUT: Duration = Duration::from_mins(2);
const SIGNAL_PROPAGATION_TIMEOUT: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const REPORT_BOOK_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const REPORT_INGEST_QUIESCE: Duration = Duration::from_secs(3);
const REPORT_EXECUTION_MAX_AGE: Duration = Duration::from_mins(3);
// Evidence reads scan the immutable report funnel, never an unbounded catalog.
const REPORT_FUNNEL_MAX_PAGES: u64 = 100;
const REPORT_FUNNEL_READ_TIMEOUT: Duration = Duration::from_mins(1);
const CLOSURE_OUTCOME_SWEEP_SECS: u64 = 10;
const SUCCESSOR_OUTCOME_TIMEOUT: Duration = Duration::from_secs(45);
// Failure-only observation cannot extend or retry the forward acceptance gate.
const SUCCESSOR_DIAGNOSTIC_TIMEOUT: Duration = Duration::from_secs(2);
// Historical fixture replay is warmup, not the new report's 45-second feedback
// contract. After parity drains, the real worker gets a separate bounded
// debug-compute window. The three-minute liveness budget accumulates only while
// work is eligible/in flight; future-only retries pause, never reset, that budget.
const HISTORICAL_ECONOMIC_TIMEOUT: Duration = Duration::from_mins(30);
const HISTORICAL_ECONOMIC_IDLE_TIMEOUT: Duration = Duration::from_mins(3);
const HISTORICAL_ECONOMIC_READ_TIMEOUT: Duration = Duration::from_secs(10);
const HISTORICAL_ECONOMIC_POLL: Duration = Duration::from_secs(1);
const HISTORICAL_ECONOMIC_READ_BATCH: usize = 512;
// The fixed seed emits at most 768 + 1024 + 500 * 5 recommendations.
const HISTORICAL_ECONOMIC_MAX_TARGETS: u64 = 4_292;
const CLOSURE_GAMMA_RECONCILE_SECS: i64 = 30;
const CATALOG_SETTLE_TIMEOUT: Duration = Duration::from_mins(2);
const GOVERNED_CANCELLATION_LEASE_SECS: u64 = 3_600;
// One thousand full-pipeline shadow observations took 605.846 seconds under a
// concurrent CPCV/parity load at the fixed concurrency of sixteen. Fifteen
// minutes provides a bounded 1.48x liveness ceiling without reducing the
// governed sample, weakening comparison gates, or raising concurrency.
const CLOSURE_SHADOW_WINDOW_SECS: u64 = 15 * 60;
// Historical feedback/replay uses a governed 90-second PIT lag: N+12 is about
// 24 seconds on Polygon, leaving one 30-second poll plus bounded jitter.
const CLOSURE_HISTORY_LAG_SECS: u64 = 90;
// The post-commit report consumes current live books and source facts. Its
// source generator and HTTP request freeze the same two-second boundary: one
// deterministic Polygon block interval is the minimum lag that preserves
// cutoff <= provider_observed_at without making live features stale. Reusing
// the historical 90-second lag rejects every required live feature.
const CLOSURE_REPORT_KNOWLEDGE_LAG_SECS: u64 = FixtureBookTiming::REPORT_LAG_SECS;
// The production-composed closure starts 59 bounded tasks and runs report,
// feedback, parity, and reconciliation transactions concurrently. Ten
// connections starve unrelated durable workers during CPCV peaks, so this
// owned stack keeps a measured burst ceiling of 80. The canonical warm floor
// remains 2: prewarming the ceiling makes SQLx rebuild 80 idle sessions as one
// max-lifetime wave and leaves almost no PostgreSQL headroom for the 15-slot
// harness pool, two dedicated listeners, and read-only evidence observers.
const CLOSURE_POSTGRES_MAX_CONNECTIONS: i64 = 80;
// Exercise replacement connections on HTTP workers during every real-binary
// gate instead of relying on startup prewarming for the default 30 minutes.
const CLOSURE_POSTGRES_MAX_LIFETIME_SECS: u64 = 30;
// The production-composed closure invalidates catalog caches while the full
// feedback and reconciliation workers are active. Keep the operation bounded,
// but provision enough Redis concurrency that invalidation never degrades to a
// stale-TTL fallback during acceptance evidence.
const CLOSURE_CACHE_OPERATION_TIMEOUT_MS: i64 = 2_000;
const CLOSURE_REDIS_POOL_SIZE: i64 = 32;
const CLOSURE_REDIS_TIMEOUT_MS: i64 = CLOSURE_CACHE_OPERATION_TIMEOUT_MS;
const MINIO_ACCESS_KEY: &str = "quantpivot-system-test";
const MINIO_SECRET_KEY: &str = "quantpivot-system-test-object-lock-secret";
const MINIO_BUCKET: &str = "quant-pivot-production-stack";
const MINIO_REGION: &str = "us-east-1";
const MINIO_API_PORT: u16 = 9_000;
const ARTIFACT_KEY_PREFIX: &str = "artifacts/";
const MINIO_STALE_UPLOADS_EXPIRY: &str = "24h";
const MINIO_STALE_UPLOADS_CLEANUP_INTERVAL: &str = "1h";
const MINIO_RETENTION_DAYS: i32 = 30;
const MINIO_SERVER_IMAGE_TAG: &str = "RELEASE.2025-06-13T11-33-47Z";
const FORBIDDEN_RUNTIME_LOG_PATTERNS: &[&str] = &[
    "canonical ledger persistence failed",
    "canonical ledger reconciliation failed",
    "gap ledger persistence failed",
    "L2 ledger batch persistence failed",
    "microstructure feature-fact persistence failed",
    "research readiness evidence capture failed",
    "stream-session ledger persistence failed",
    "session barrier mailbox reservation timed out",
    "session drain barrier timed out",
    "failed to enqueue stream-session close ledger; gap fan-out suppressed",
    "Partition queue rejected a batch; continuity invalidated",
    "report fact worker poll failed; retrying",
    "durable report coordinator pass failed",
    "periodic task iteration failed task=\"outcome-reconciliation-worker\"",
    "time to acquire exceeded slow threshold",
];
const BROWSER_PARITY_TARGET: &str = concat!(
    " WARN quant_pivot_core::service::feature_parity_executor: ",
    "feature parity detected a deterministic online/replay mismatch"
);
const AUTH_OUTAGE_TARGET: &str = " ERROR HTTP request{";
const AUTH_OUTAGE_SUFFIX: &str = concat!(
    "}: quant_pivot_web::request_tracing: HTTP request failed ",
    "error=ServiceUnavailable(\"authentication temporarily unavailable\")"
);
const CLOSURE_MERGE_TABLES: &[&str] = &[
    "quant_book_l2_ledger",
    "book_microstructure_1s",
    "quant_book_stream_session",
];
const TRIGGER_IDENTITY_FIELDS: &[&str] = &[
    "feedback_cycle_id",
    "idempotency_hash",
    "profile_ref",
    "research_profile_artifact_id",
    "feedback_policy_hash",
    "label_cutoff",
    "champion_model_version_id",
    "champion_serving_contract_hash",
    "champion_model_spec_id",
    "champion_model_spec_definition_hash",
    "champion_model_family",
    "route",
    "decision_policy_snapshot_id",
    "decision_policy_snapshot_hash",
    "policy_bundle_generation",
    "route_generation",
    "evaluation_mode",
    "parent_cycle_id",
    "forced_idempotency_key",
];

const DISABLED_DOMAIN_SOURCES: &[&str] = &[
    "binance",
    "binance_usdm_futures",
    "polymarket_rtds",
    "chainlink_data_streams",
    "aviation_weather",
    "ghcnh",
    "ghcnd",
    "gefs",
    "hko_open_data",
    "airnow",
    "tornado",
    "nhc",
    "nasa_gistemp",
    "nsidc_sea_ice",
    "nws_observation",
];

#[derive(Deserialize)]
struct CargoMetadata {
    workspace_root: PathBuf,
    target_directory: PathBuf,
}

struct Workspace {
    root: PathBuf,
    target_directory: PathBuf,
    binary: PathBuf,
}

struct ProductionLaunch {
    workspace_root: PathBuf,
    binary: PathBuf,
    run_dir: PathBuf,
    base_url: String,
    uses_fixture_s3: bool,
    hypersync_proxy: Option<String>,
    catalog_runtime_started_at: DateTime<Utc>,
}

struct HistoryUpstreams {
    attestor: MockServer,
    hypersync: MockServer,
    minimum_serving_block: i64,
}

struct BrowserFixtureEvidence {
    closure: Option<FeedbackClosureFixture>,
    cancellation_claim: Option<FeedbackCycleClaim>,
    sampled_parity_report_id: Option<RecommendationReportId>,
    await_settlement_discovery: bool,
}

#[derive(Serialize)]
struct FeedbackReportEvidence {
    recommendations: JsonValue,
    diagnostics: JsonValue,
    funnel: JsonValue,
    funnel_markets: JsonValue,
    funnel_market_pages: Vec<JsonValue>,
    feature_nulls: JsonValue,
}

#[derive(Serialize)]
struct FeedbackReportDiagnosticArchive<'a> {
    recommendation_report_id: RecommendationReportId,
    report_universe: &'a FeedbackReportUniverse,
    report_run: &'a JsonValue,
    report_detail: &'a JsonValue,
    evidence: &'a FeedbackReportEvidence,
}

impl FeedbackReportDiagnosticArchive<'_> {
    fn persist(&self, path: &Path) -> Result<()> {
        ProductionStack::persist_json_manifest(
            path,
            &serde_json::to_vec_pretty(self)?,
            "report diagnostics",
        )
    }
}

struct FeedbackMarketFunnelEvidence {
    response: JsonValue,
    pages: Vec<JsonValue>,
}

impl FeedbackMarketFunnelEvidence {
    fn feature_routes(&self) -> Result<HashMap<String, JsonValue>> {
        let items = self.response["data"]["items"]
            .as_array()
            .context("mixed-Route market funnel omitted items")?;
        items
            .iter()
            .filter(|item| {
                item["primary_reason"] == ReportFunnelReason::FeatureDataQualityRejected.as_str()
            })
            .map(|item| {
                Ok((
                    item["market_id"]
                        .as_str()
                        .context("feature-rejected market omitted identity")?
                        .to_owned(),
                    item["route"].clone(),
                ))
            })
            .collect()
    }

    async fn read(
        http: &Client,
        endpoint: &str,
        access_token: &str,
        market_ids: &[MarketId],
    ) -> Result<Self> {
        let deadline = Instant::now() + REPORT_FUNNEL_READ_TIMEOUT;
        let requested = market_ids
            .iter()
            .map(MarketId::as_str)
            .collect::<HashSet<_>>();
        let mut pages = Vec::new();
        let mut selected = BTreeMap::new();
        let mut total = None;
        for page in 1..=REPORT_FUNNEL_MAX_PAGES {
            let response = tokio::time::timeout_at(deadline, async {
                decode_http_json(
                    http.get(endpoint)
                        .query(&PageRequest::new(page, PageRequest::MAX_SIZE))
                        .header("accept-api-version", "v1")
                        .bearer_auth(access_token)
                        .send()
                        .await
                        .context("read complete mixed-Route market funnel")?,
                    StatusCode::OK,
                    "read complete mixed-Route market funnel",
                )
                .await
            })
            .await
            .context("complete market funnel exceeded its bounded read deadline")??;
            let data = &response["data"];
            let page_total = data["total"]
                .as_u64()
                .context("market funnel omitted total")?;
            let has_next = data["has_next"]
                .as_bool()
                .context("market funnel omitted has_next")?;
            let items = data["items"]
                .as_array()
                .context("market funnel omitted items")?;
            ensure!(
                data["page"].as_u64() == Some(page)
                    && data["size"].as_u64() == Some(PageRequest::MAX_SIZE)
                    && total.is_none_or(|expected| expected == page_total),
                "immutable market funnel pagination changed"
            );
            total = Some(page_total);
            let consumed = page * PageRequest::MAX_SIZE;
            let expected_count = page_total
                .saturating_sub((page - 1) * PageRequest::MAX_SIZE)
                .min(PageRequest::MAX_SIZE);
            ensure!(
                u64::try_from(items.len())? == expected_count
                    && has_next == (consumed < page_total),
                "market funnel returned an incomplete or inconsistent page {page}"
            );
            for item in items {
                let market = item["market_id"]
                    .as_str()
                    .context("market funnel row omitted market_id")?;
                if requested.contains(market) {
                    ensure!(
                        selected.insert(market.to_owned(), item.clone()).is_none(),
                        "market funnel repeated terminal evidence for {market}"
                    );
                }
            }
            pages.push(response);
            if !has_next {
                let items = selected.into_values().collect::<Vec<_>>();
                return Ok(Self {
                    response: json!({
                        "scope": "exact_report_universe",
                        "source_total": page_total,
                        "source_pages": pages.len(),
                        "data": { "total": items.len(), "items": items, "page": 1, "size": market_ids.len(), "has_next": false }
                    }),
                    pages,
                });
            }
        }
        bail!("market funnel exceeded its {REPORT_FUNNEL_MAX_PAGES}-page evidence budget")
    }
}

#[derive(Serialize)]
struct CandidateReadyClosureManifest<'a> {
    closure: &'a FeedbackClosureOutcome,
    report_universe: &'a FeedbackReportUniverse,
    historical_economic_backfill: &'a HistoricalEconomicBackfill,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GovernedClosureManifest {
    format_version: u32,
    evidence_boundary: DisposableEvidenceBoundary,
    closure: FeedbackClosureOutcome,
    data_plane_stability: DataPlaneStabilityEvidence,
    readiness_capture: ReadinessCaptureEvidence,
    pre_activation_parity: Vec<RuntimeParityEvidence>,
    historical_economic_backfill: HistoricalEconomicBackfill,
    permit: JsonValue,
    disposable_model_route_commit: JsonValue,
    report_universe: FeedbackReportUniverse,
    report: JsonValue,
    report_parity: RuntimeParityEvidence,
    resolution_plane: FeedbackReportResolutionEvidence,
    successor_feedback: SuccessorFeedbackEvidence,
}

struct PendingClosureManifest {
    preimage: DisposableBoundaryPreimage,
    runtime_control_before_drain: RuntimeControlSnapshot,
    closure: FeedbackClosureOutcome,
    data_plane_stability: DataPlaneStabilityEvidence,
    readiness_capture: ReadinessCaptureEvidence,
    pre_activation_parity: Vec<RuntimeParityEvidence>,
    historical_economic_backfill: HistoricalEconomicBackfill,
    permit: JsonValue,
    disposable_model_route_commit: JsonValue,
    report_universe: FeedbackReportUniverse,
    report: JsonValue,
    report_parity: RuntimeParityEvidence,
    resolution_plane: FeedbackReportResolutionEvidence,
    successor_feedback: SuccessorFeedbackEvidence,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct HistoricalEconomicCounts {
    pending: u64,
    delivering: u64,
    retrying: u64,
    completed: u64,
    visible_completed: u64,
    outcomes: u64,
    visible_outcomes: u64,
    live_claims: u64,
}

impl HistoricalEconomicCounts {
    fn validate(&self, expected: u64) -> Result<()> {
        let tasks = [self.pending, self.delivering, self.retrying, self.completed]
            .into_iter()
            .try_fold(0_u64, u64::checked_add)
            .context("historical economic task count overflow")?;
        ensure!(
            tasks == expected
                && self.visible_completed <= self.completed
                && self.completed <= self.outcomes
                && self.outcomes <= expected
                && self.visible_outcomes <= self.outcomes
                && self.live_claims <= expected,
            "historical economic frozen membership/counts differ: expected={expected} counts={self:?}"
        );
        Ok(())
    }

    const fn drained(&self, expected: u64) -> bool {
        expected > 0
            && self.pending == 0
            && self.delivering == 0
            && self.retrying == 0
            && self.completed == expected
            && self.visible_completed == expected
            && self.outcomes == expected
            && self.visible_outcomes == expected
            && self.live_claims == 0
    }

    fn observe(&mut self, task: &HistoricalEconomicTaskRead, at: DateTime<Utc>) {
        match task.status {
            OutcomeReconciliationTaskStatus::Pending => self.pending += 1,
            OutcomeReconciliationTaskStatus::Delivering => self.delivering += 1,
            OutcomeReconciliationTaskStatus::Retrying => self.retrying += 1,
            OutcomeReconciliationTaskStatus::Completed => {
                self.completed += 1;
                self.visible_completed +=
                    u64::from(task.completed_at.is_some_and(|time| time <= at));
            }
        }
        self.live_claims +=
            u64::from(task.claim_owner.is_some() || task.lease_expires_at.is_some());
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct HistoricalEconomicProgress {
    observed_at: DateTime<Utc>,
    counts: HistoricalEconomicCounts,
    schedule: HistoricalEconomicSchedule,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct HistoricalEconomicSchedule {
    eligible: u64,
    waiting_retry: u64,
    next_eligible_at: Option<DateTime<Utc>>,
}

impl HistoricalEconomicSchedule {
    fn observe(&mut self, task: &HistoricalEconomicTaskRead, at: DateTime<Utc>) -> Result<()> {
        let claimed = task.claim_owner.is_some() && task.lease_expires_at.is_some();
        let unclaimed = task.claim_owner.is_none() && task.lease_expires_at.is_none();
        match task.status {
            OutcomeReconciliationTaskStatus::Retrying => {
                ensure!(unclaimed, "historical economic retry retains a claim");
                let next = task
                    .next_attempt_at
                    .context("historical economic retry has no eligibility time")?;
                if next <= at {
                    self.eligible += 1;
                } else {
                    self.waiting_retry += 1;
                    self.next_eligible_at = Some(
                        self.next_eligible_at
                            .map_or(next, |existing| existing.min(next)),
                    );
                }
            }
            OutcomeReconciliationTaskStatus::Delivering => {
                ensure!(
                    claimed && task.next_attempt_at.is_none(),
                    "historical economic delivery claim is incomplete"
                );
                // A live lease is work in flight; an expired lease is reclaimable.
                // Neither lease renewal nor ownership changes pause liveness.
                self.eligible += 1;
            }
            OutcomeReconciliationTaskStatus::Pending => {
                ensure!(
                    unclaimed && task.next_attempt_at.is_none(),
                    "historical economic pending task retains a retry or claim"
                );
                self.eligible += 1;
            }
            OutcomeReconciliationTaskStatus::Completed => {
                ensure!(
                    unclaimed && task.next_attempt_at.is_none() && task.completed_at.is_some(),
                    "historical economic completed task has unfinished scheduling state"
                );
            }
        }
        Ok(())
    }

    fn validate(&self, progress: &HistoricalEconomicProgress) -> Result<()> {
        let unfinished =
            progress.counts.pending + progress.counts.delivering + progress.counts.retrying;
        ensure!(
            self.eligible.checked_add(self.waiting_retry) == Some(unfinished)
                && self.waiting_retry <= progress.counts.retrying
                && (self.waiting_retry == 0) == self.next_eligible_at.is_none()
                && self
                    .next_eligible_at
                    .is_none_or(|next| next > progress.observed_at),
            "historical economic scheduling evidence differs from its task population"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct HistoricalEconomicObservation {
    progress: HistoricalEconomicProgress,
    clock_at: Instant,
}

impl HistoricalEconomicObservation {
    fn eligible_from(&self) -> Result<Instant> {
        let schedule = self.progress.schedule;
        let counts = self.progress.counts;
        let settled_evidence_complete = counts.completed == counts.visible_completed
            && counts.completed == counts.outcomes
            && counts.outcomes == counts.visible_outcomes;
        if schedule.eligible == 0 && schedule.waiting_retry > 0 && settled_evidence_complete {
            let next = schedule
                .next_eligible_at
                .context("future retries lack eligibility time")?;
            let wait = (next - self.progress.observed_at).to_std()?;
            self.clock_at
                .checked_add(wait)
                .context("historical retry eligibility exceeds monotonic clock")
        } else {
            Ok(self.clock_at)
        }
    }
}

struct HistoricalEconomicLiveness {
    total_deadline: Instant,
    previous: HistoricalEconomicObservation,
    eligible_from: Instant,
    eligible_elapsed: Duration,
    peak: (u64, u64),
}

impl HistoricalEconomicLiveness {
    fn new(started: Instant, initial: HistoricalEconomicObservation) -> Result<Self> {
        initial.progress.schedule.validate(&initial.progress)?;
        Ok(Self {
            total_deadline: started + HISTORICAL_ECONOMIC_TIMEOUT,
            previous: initial,
            eligible_from: initial.eligible_from()?,
            eligible_elapsed: Duration::ZERO,
            peak: (
                initial.progress.counts.visible_completed,
                initial.progress.counts.visible_outcomes,
            ),
        })
    }

    fn observe(&mut self, current: HistoricalEconomicObservation) -> Result<bool> {
        current.progress.schedule.validate(&current.progress)?;
        ensure!(
            current.clock_at >= self.previous.clock_at
                && current.progress.observed_at >= self.previous.progress.observed_at,
            "historical economic observation clock moved backwards"
        );
        self.eligible_elapsed += current
            .clock_at
            .saturating_duration_since(self.previous.clock_at.max(self.eligible_from));
        let counts = current.progress.counts;
        let advanced =
            counts.visible_completed > self.peak.0 || counts.visible_outcomes > self.peak.1;
        if advanced {
            self.peak.0 = self.peak.0.max(counts.visible_completed);
            self.peak.1 = self.peak.1.max(counts.visible_outcomes);
            self.eligible_elapsed = Duration::ZERO;
        }
        self.eligible_from = current.eligible_from()?;
        self.previous = current;
        Ok(advanced)
    }

    fn deadline(&self) -> Instant {
        if self.eligible_elapsed >= HISTORICAL_ECONOMIC_IDLE_TIMEOUT {
            return self.total_deadline.min(self.previous.clock_at);
        }
        let remaining = HISTORICAL_ECONOMIC_IDLE_TIMEOUT.saturating_sub(self.eligible_elapsed);
        self.total_deadline
            .min(self.previous.clock_at.max(self.eligible_from) + remaining)
    }

    fn check(&self, now: Instant) -> Result<()> {
        ensure!(
            now < self.total_deadline,
            "historical economic warmup exhausted its {HISTORICAL_ECONOMIC_TIMEOUT:?} total budget"
        );
        ensure!(
            now < self.deadline(),
            "historical economic warmup exhausted its {HISTORICAL_ECONOMIC_IDLE_TIMEOUT:?} cumulative eligible-no-progress budget"
        );
        Ok(())
    }

    fn read_deadline(&self, now: Instant) -> Result<Instant> {
        self.check(now)?;
        Ok(self.deadline().min(now + HISTORICAL_ECONOMIC_READ_TIMEOUT))
    }

    fn read_timeout(&self, now: Instant) -> AnyhowError {
        match self.check(now) {
            Err(error) => error,
            Ok(()) => anyhow!(
                "historical economic progress read exceeded its {HISTORICAL_ECONOMIC_READ_TIMEOUT:?} read budget"
            ),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct HistoricalEconomicReceipt {
    recommendation_id: RecommendationId,
    evidence_hash: ContentHash,
    available_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct HistoricalEconomicBackfill {
    target_cutoff: DateTime<Utc>,
    recommendation_ids: Vec<RecommendationId>,
    target_hash: ContentHash,
    initial: HistoricalEconomicProgress,
    terminal: HistoricalEconomicProgress,
    elapsed_ms: u64,
    outcomes: Vec<HistoricalEconomicReceipt>,
    outcome_set_hash: ContentHash,
}

impl HistoricalEconomicBackfill {
    fn validate(&self) -> Result<()> {
        let expected = u64::try_from(self.recommendation_ids.len())?;
        ensure!(
            expected > 0
                && expected <= HISTORICAL_ECONOMIC_MAX_TARGETS
                && self
                    .recommendation_ids
                    .windows(2)
                    .all(|pair| pair[0].as_uuid() < pair[1].as_uuid()),
            "historical economic target set is empty, duplicated, or exceeds its fixed fixture bound"
        );
        ensure!(
            self.target_hash
                == HistoricalEconomicTarget::hash(self.target_cutoff, &self.recommendation_ids)?,
            "historical economic frozen target hash differs"
        );
        self.initial.counts.validate(expected)?;
        self.terminal.counts.validate(expected)?;
        self.initial.schedule.validate(&self.initial)?;
        self.terminal.schedule.validate(&self.terminal)?;
        let initial_is_visible = self.target_cutoff <= self.initial.observed_at;
        let observations_are_forward = self.initial.observed_at <= self.terminal.observed_at;
        ensure!(
            initial_is_visible
                && observations_are_forward
                && u128::from(self.elapsed_ms) <= HISTORICAL_ECONOMIC_TIMEOUT.as_millis(),
            "historical economic backfill clocks or elapsed budget are invalid"
        );
        let database_elapsed = (self.terminal.observed_at - self.initial.observed_at).to_std()?;
        ensure!(
            database_elapsed <= HISTORICAL_ECONOMIC_TIMEOUT,
            "historical economic database observations exceed the warmup budget"
        );
        ensure!(
            self.terminal.counts.drained(expected),
            "historical economic backfill lacks completed tasks and visible WORM outcomes"
        );
        ensure!(
            self.outcomes.len() == self.recommendation_ids.len()
                && self
                    .outcomes
                    .iter()
                    .zip(&self.recommendation_ids)
                    .all(|(receipt, id)| receipt.recommendation_id == *id
                        && receipt.available_at <= self.terminal.observed_at),
            "historical economic outcome receipts are missing, misbound, or future-visible"
        );
        ensure!(
            self.outcome_set_hash == Self::receipt_hash(&self.outcomes)?,
            "historical economic verified outcome-set hash differs"
        );
        Ok(())
    }

    fn receipt_hash(receipts: &[HistoricalEconomicReceipt]) -> Result<ContentHash> {
        Ok(CanonicalDigest::content_hash_typed(
            "quant-pivot/fixture-historical-economic-outcomes",
            1,
            &receipts,
        )?)
    }
}

#[derive(FromQueryResult)]
struct HistoricalEconomicTargetRead {
    recommendation_id: RecommendationId,
    horizon_at: DateTime<Utc>,
}

#[derive(FromQueryResult)]
struct HistoricalEconomicTaskRead {
    status: OutcomeReconciliationTaskStatus,
    completed_at: Option<DateTime<Utc>>,
    claim_owner: Option<WorkerId>,
    lease_expires_at: Option<DateTime<Utc>>,
    next_attempt_at: Option<DateTime<Utc>>,
}

#[derive(FromQueryResult)]
struct HistoricalEconomicOutcomeRead {
    available_at: DateTime<Utc>,
}

struct HistoricalEconomicTarget {
    cutoff: DateTime<Utc>,
    ids: Vec<RecommendationId>,
    hash: ContentHash,
}

impl HistoricalEconomicTarget {
    fn hash(cutoff: DateTime<Utc>, ids: &[RecommendationId]) -> Result<ContentHash> {
        Ok(CanonicalDigest::content_hash_typed(
            "quant-pivot/fixture-historical-economic-targets",
            1,
            &(cutoff, ids),
        )?)
    }

    async fn freeze(db: &DatabaseConnection) -> Result<Self> {
        let cutoff = db.statement_time().await;
        let rows = EconomicTaskEntity::find()
            .select_only()
            .columns([
                EconomicTaskColumn::RecommendationId,
                EconomicTaskColumn::HorizonAt,
            ])
            .filter(EconomicTaskColumn::CreatedAt.lte(cutoff))
            .order_by_asc(EconomicTaskColumn::RecommendationId)
            .limit(HISTORICAL_ECONOMIC_MAX_TARGETS + 1)
            .into_model::<HistoricalEconomicTargetRead>()
            .all(db)
            .await?;
        ensure!(
            !rows.is_empty() && u64::try_from(rows.len())? <= HISTORICAL_ECONOMIC_MAX_TARGETS,
            "historical economic fixture membership is empty or exceeds its fixed population"
        );
        ensure!(
            rows.iter().all(|row| row.horizon_at <= cutoff),
            "historical economic warmup encountered an unmatured nonhistorical task"
        );
        let ids = rows
            .into_iter()
            .map(|row| row.recommendation_id)
            .collect::<Vec<_>>();
        let hash = Self::hash(cutoff, &ids)?;
        Ok(Self { cutoff, ids, hash })
    }

    async fn progress(&self, db: &DatabaseConnection) -> Result<HistoricalEconomicObservation> {
        let observed_at = db.statement_time().await;
        let clock_at = Instant::now();
        let mut counts = HistoricalEconomicCounts::default();
        let mut schedule = HistoricalEconomicSchedule::default();
        for ids in self.ids.chunks(HISTORICAL_ECONOMIC_READ_BATCH) {
            let tasks = EconomicTaskEntity::find()
                .select_only()
                .columns([
                    EconomicTaskColumn::Status,
                    EconomicTaskColumn::CompletedAt,
                    EconomicTaskColumn::ClaimOwner,
                    EconomicTaskColumn::LeaseExpiresAt,
                    EconomicTaskColumn::NextAttemptAt,
                ])
                .filter(EconomicTaskColumn::RecommendationId.is_in(ids.iter().copied()))
                .limit(u64::try_from(ids.len())?)
                .into_model::<HistoricalEconomicTaskRead>()
                .all(db)
                .await?;
            for task in &tasks {
                counts.observe(task, observed_at);
                schedule.observe(task, observed_at)?;
            }
            let outcomes = EconomicOutcomeEntity::find()
                .select_only()
                .column(EconomicOutcomeColumn::AvailableAt)
                .filter(EconomicOutcomeColumn::RecommendationId.is_in(ids.iter().copied()))
                .limit(u64::try_from(ids.len())?)
                .into_model::<HistoricalEconomicOutcomeRead>()
                .all(db)
                .await?;
            counts.outcomes += u64::try_from(outcomes.len())?;
            counts.visible_outcomes += u64::try_from(
                outcomes
                    .iter()
                    .filter(|outcome| outcome.available_at <= observed_at)
                    .count(),
            )?;
        }
        counts.validate(u64::try_from(self.ids.len())?)?;
        let progress = HistoricalEconomicProgress {
            observed_at,
            counts,
            schedule,
        };
        schedule.validate(&progress)?;
        Ok(HistoricalEconomicObservation { progress, clock_at })
    }

    async fn verify_outcomes(
        &self,
        db: &DatabaseConnection,
        visible_at: DateTime<Utc>,
    ) -> Result<Vec<HistoricalEconomicReceipt>> {
        let mut receipts = Vec::with_capacity(self.ids.len());
        for ids in self.ids.chunks(HISTORICAL_ECONOMIC_READ_BATCH) {
            let rows = EconomicOutcomeEntity::find()
                .filter(EconomicOutcomeColumn::RecommendationId.is_in(ids.iter().copied()))
                .limit(u64::try_from(ids.len())?)
                .all(db)
                .await?;
            ensure!(
                rows.len() == ids.len(),
                "historical economic WORM disappeared during final readback"
            );
            for row in rows {
                let outcome = RecommendationEconomicOutcomeInfo::from(row);
                outcome.verify()?;
                ensure!(
                    outcome.available_at <= visible_at,
                    "historical economic WORM is not visible at its terminal observation"
                );
                receipts.push(HistoricalEconomicReceipt {
                    recommendation_id: outcome.recommendation_id,
                    evidence_hash: outcome.evidence_hash,
                    available_at: outcome.available_at,
                });
            }
        }
        receipts.sort_by_key(|receipt| receipt.recommendation_id.as_uuid());
        Ok(receipts)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MoneyPathCounts {
    order_intents: i64,
    capital_allocations: i64,
    execution_accounts: i64,
    execution_orders: i64,
    execution_attempt_outcomes: i64,
    execution_reconciliation_tasks: i64,
    execution_rollup_tasks: i64,
    execution_trade_refs: i64,
    clob_trade_observations: i64,
    execution_transaction_refs: i64,
    strategy_position_lots: i64,
    settlement_authorizations: i64,
    settlement_chain_submissions: i64,
    settlement_external_cursors: i64,
    settlement_governed_actions: i64,
    settlement_inventory_lots: i64,
    settlement_redeems: i64,
    settlement_redeem_lots: i64,
    account_chain_executions: i64,
    account_execution_associations: i64,
    account_clean_funder_blockers: i64,
    account_pause_operations: i64,
    account_recovery_incidents: i64,
    account_recovery_manifests: i64,
}

impl MoneyPathCounts {
    fn total(&self) -> Result<u64> {
        [
            ("order_intents", self.order_intents),
            ("capital_allocations", self.capital_allocations),
            ("execution_accounts", self.execution_accounts),
            ("execution_orders", self.execution_orders),
            (
                "execution_attempt_outcomes",
                self.execution_attempt_outcomes,
            ),
            (
                "execution_reconciliation_tasks",
                self.execution_reconciliation_tasks,
            ),
            ("execution_rollup_tasks", self.execution_rollup_tasks),
            ("execution_trade_refs", self.execution_trade_refs),
            ("clob_trade_observations", self.clob_trade_observations),
            (
                "execution_transaction_refs",
                self.execution_transaction_refs,
            ),
            ("strategy_position_lots", self.strategy_position_lots),
            ("settlement_authorizations", self.settlement_authorizations),
            (
                "settlement_chain_submissions",
                self.settlement_chain_submissions,
            ),
            (
                "settlement_external_cursors",
                self.settlement_external_cursors,
            ),
            (
                "settlement_governed_actions",
                self.settlement_governed_actions,
            ),
            ("settlement_inventory_lots", self.settlement_inventory_lots),
            ("settlement_redeems", self.settlement_redeems),
            ("settlement_redeem_lots", self.settlement_redeem_lots),
            ("account_chain_executions", self.account_chain_executions),
            (
                "account_execution_associations",
                self.account_execution_associations,
            ),
            (
                "account_clean_funder_blockers",
                self.account_clean_funder_blockers,
            ),
            ("account_pause_operations", self.account_pause_operations),
            (
                "account_recovery_incidents",
                self.account_recovery_incidents,
            ),
            (
                "account_recovery_manifests",
                self.account_recovery_manifests,
            ),
        ]
        .into_iter()
        .try_fold(0_u64, |total, (field, count)| {
            let count = u64::try_from(count)
                .with_context(|| format!("money-path count `{field}` is negative"))?;
            total
                .checked_add(count)
                .context("money-path count sum overflowed")
        })
    }
}

struct DisposableBoundaryPreimage {
    runtime_control: RuntimeControlSnapshot,
    money_path_counts: MoneyPathCounts,
}

impl DisposableBoundaryPreimage {
    fn verify_runtime(
        &self,
        before_drain: &RuntimeControlSnapshot,
        after_drain: &RuntimeControlSnapshot,
    ) -> Result<()> {
        ensure!(
            before_drain == after_drain,
            "disposable runtime control changed while the production binary drained"
        );
        ensure!(
            self.runtime_control == *after_drain
                && after_drain.entry_authorization_policy
                    == EntryAuthorizationPolicy::OperatorApprovalRequired
                && after_drain.settlement_write_policy == SettlementWritePolicy::Disabled,
            "disposable model-route commit changed execution or settlement authority"
        );
        Ok(())
    }
}

/// Audited proof that the route commit and report closure remained inside
/// owned disposable infrastructure and never expanded execution authority.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DisposableEvidenceBoundary {
    evidence_scope: String,
    production_composed_binary: bool,
    operational_activation_claimed: bool,
    model_route_commit_scope: String,
    outbound_write_endpoints: String,
    runtime_control_before: RuntimeControlSnapshot,
    runtime_control_after: RuntimeControlSnapshot,
    execution_authority_unchanged: bool,
    money_path_before: MoneyPathCounts,
    money_path_after: MoneyPathCounts,
    real_venue_order_write_count: u64,
    real_chain_write_count: u64,
    real_capital_write_count: u64,
    relayer_request_count: u64,
}

/// Exact production-verified retention observation frozen into the manifest.
///
/// Derived readiness booleans are deliberately absent. Consumers recompute
/// registry coverage and the proven runway from the typed payload, while the
/// capture path verifies the immutable artifact and attestation before this
/// value can be constructed.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReadinessCaptureEvidence {
    verified_at: DateTime<Utc>,
    evidence: ResearchReadinessEvidenceInfo,
}

impl ReadinessCaptureEvidence {
    fn validate(&self) -> Result<()> {
        let info = &self.evidence;
        let verified_at = self.verified_at;
        ensure!(
            info.kind == ResearchReadinessEvidenceKind::RetentionRunway,
            "closure readiness capture is not retention-runway evidence"
        );
        ensure!(
            info.window_start < info.window_end
                && info.window_end == info.observed_at
                && info.observed_at <= verified_at
                && info.observed_at <= info.created_at
                && info.created_at <= verified_at
                && verified_at < info.expires_at,
            "closure readiness evidence was not current at verification time"
        );
        let ResearchReadinessEvidencePayload::RetentionRunway(payload) = &info.payload_json else {
            bail!("closure readiness capture has the wrong typed payload")
        };
        let payload_hash = CanonicalDigest::content_hash_json(&info.payload_json)
            .context("hash closure readiness typed payload")?;
        ensure!(
            payload_hash == info.payload_hash,
            "closure readiness payload hash does not match the embedded typed payload"
        );
        ensure!(
            payload.observed_at == info.observed_at,
            "closure readiness payload and index observation clocks differ"
        );
        let history_start = payload
            .observations
            .iter()
            .filter_map(|observation| observation.earliest_event_time)
            .max();
        let timestamps_valid = payload.observations.iter().all(|observation| {
            observation
                .earliest_event_time
                .zip(observation.latest_event_time)
                .is_some_and(|(earliest, latest)| {
                    earliest <= latest && latest <= payload.observed_at
                })
        });
        let measured_history_days = timestamps_valid
            .then_some(history_start)
            .flatten()
            .and_then(|start| {
                u32::try_from(
                    payload
                        .observed_at
                        .signed_duration_since(start)
                        .num_seconds()
                        / 86_400,
                )
                .ok()
            });
        ensure!(
            timestamps_valid && payload.measured_history_days == measured_history_days,
            "closure readiness measured history does not match the physical source observations"
        );
        let active_raw_bytes = payload
            .observations
            .iter()
            .try_fold(0_u64, |total, observation| {
                total.checked_add(observation.active_bytes.unwrap_or(0))
            });
        ensure!(
            active_raw_bytes == Some(payload.active_raw_bytes),
            "closure readiness active raw bytes do not match the physical source observations"
        );
        let required_days = minimum_raw_retention_days()
            .map_err(|detail| anyhow!("resolve canonical raw-history retention: {detail}"))?;
        ensure!(
            payload.required_days == required_days
                && info.window_start
                    == info.observed_at - ChronoDuration::days(i64::from(required_days)),
            "closure readiness capture does not bind the canonical {required_days}-day retention window"
        );
        let registry = research_source_registry()
            .map_err(|detail| anyhow!("resolve canonical research source registry: {detail}"))?;
        let mut gamma_objects = payload
            .observations
            .iter()
            .filter(|observation| {
                observation.source == ResearchReadinessSource::GammaMarketIdentity
            })
            .map(|observation| observation.object.as_str())
            .collect::<Vec<_>>();
        gamma_objects.sort_unstable();
        ensure!(
            gamma_objects == ["catalog_event_change", "catalog_market_change"],
            "closure readiness Gamma identity evidence does not bind both immutable catalog ledgers: {gamma_objects:?}"
        );
        ensure!(
            payload.matches_registry(&registry),
            "closure readiness retention payload does not match the canonical research source registry"
        );
        ensure!(
            payload.proven(),
            "closure readiness retention runway is not proven"
        );
        Ok(())
    }
}

/// Bounded CLOB ownership and zero durable-ingest failures observed across the
/// exact governed closure window.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DataPlaneStabilityEvidence {
    expected_shards: u64,
    active_connections: u64,
    connection_high_water: u64,
    concurrency_bound: u64,
    baseline_accepted_connections: u64,
    final_accepted_connections: u64,
    accepted_connection_delta: u64,
    allowed_turnover: u64,
    forbidden_runtime_failures: u64,
}

#[derive(Serialize)]
struct BrowserClosureManifest<'a> {
    closure: &'a FeedbackClosureOutcome,
    report_universe: &'a FeedbackReportUniverse,
    report_id: RecommendationReportId,
    report_parity: &'a RuntimeParityEvidence,
    resolution_plane: &'a FeedbackReportResolutionEvidence,
    successor_feedback: &'a SuccessorFeedbackEvidence,
}

#[derive(Serialize)]
struct BrowserClosureFailureManifest<'a> {
    feedback_cycle_id: FeedbackCycleId,
    error: &'a str,
}

/// Exact successful runtime replay that gates the deterministic closure.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RuntimeParityEvidence {
    run_id: FeatureParityRunId,
    kind: FeatureParityRunKind,
    report_id: Option<RecommendationReportId>,
    total_count: i64,
    compared_count: i64,
    matched_count: i64,
    finished_at: DateTime<Utc>,
    latch_state_id: FeatureParityStateId,
}

/// Frozen N+1 feedback eligibility proven from the promoted report's own
/// forward outcomes. This is prospective label availability, not causal OPE.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SuccessorFeedbackEvidence {
    parent_cycle_id: FeedbackCycleId,
    decision_window_start: DateTime<Utc>,
    decision_cutoff: DateTime<Utc>,
    truth_cutoff: DateTime<Utc>,
    route_cohorts: Vec<SuccessorRouteFeedbackEvidence>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SuccessorRouteFeedbackEvidence {
    route: BuyModelRoute,
    report_route_run_id: ReportRouteRunId,
    profile_ref: ResearchProfileRef,
    model_version_id: ModelVersionId,
    recommendation_ids: Vec<RecommendationId>,
    resolution_outcome_hashes: Vec<ContentHash>,
    economic_outcomes: Vec<RecommendationEconomicOutcomeInfo>,
    economic_outcome_count: u32,
    model_learning_eligible_count: u32,
    policy_evaluation_eligible_count: u32,
    execution_learning_censored_count: u32,
    execution_censor_reason: CohortCensorReason,
}

struct ActivationPolicyPreimage {
    bundle: ActivePolicyBundle,
    pooled_champion_model_version_id: ModelVersionId,
    crypto_champion_model_version_id: ModelVersionId,
}

struct SuccessorRouteVerifier<'a> {
    db: &'a DatabaseConnection,
    outcome: &'a FeedbackClosureOutcome,
    report_id: RecommendationReportId,
    decision_at: DateTime<Utc>,
    truth_cutoff: DateTime<Utc>,
    outcomes: &'a HashMap<RecommendationId, RecommendationResolutionOutcomeInfo>,
    economic_outcomes: &'a HashMap<RecommendationId, RecommendationEconomicOutcomeInfo>,
}

struct SuccessorOutcomeEvidence {
    outcomes: HashMap<RecommendationId, RecommendationResolutionOutcomeInfo>,
    economic_outcomes: HashMap<RecommendationId, RecommendationEconomicOutcomeInfo>,
    truth_cutoff: DateTime<Utc>,
}

struct SuccessorOutcomeVerifier<'a> {
    db: &'a DatabaseConnection,
    recommendations: &'a [RecommendationModel],
    resolution_plane: &'a FeedbackReportResolutionEvidence,
    decision_at: DateTime<Utc>,
}

impl SuccessorOutcomeVerifier<'_> {
    async fn verify(&self) -> Result<SuccessorOutcomeEvidence> {
        let economic_repository = PgRecommendationEconomicOutcomeRepository::new(self.db.clone());
        let mut economic_identities = HashMap::with_capacity(self.recommendations.len());
        for recommendation in self.recommendations {
            let context = economic_repository
                .replay_context(&recommendation.recommendation_id)
                .await?;
            economic_identities.insert(
                recommendation.recommendation_id,
                SuccessorEconomicIdentity::try_from(&context)?,
            );
        }
        let (outcomes, economic_outcomes) = self
            .observe(Instant::now() + SUCCESSOR_OUTCOME_TIMEOUT)
            .await?;
        let strictly_forward = outcomes.values().all(|resolution| {
            let observed_after_decision = resolution.source_observed_at > self.decision_at;
            let available_after_source = resolution.available_at >= resolution.source_observed_at;
            observed_after_decision && available_after_source
        });
        ensure!(
            strictly_forward,
            "successor feedback outcomes are not strictly forward-looking"
        );

        let truth_cutoff = self.db.statement_time().await;
        for recommendation in self.recommendations {
            let identity = economic_identities
                .get(&recommendation.recommendation_id)
                .context("economic expected identity disappeared")?;
            let resolution = outcomes
                .get(&recommendation.recommendation_id)
                .context("successor resolution disappeared")?;
            identity.verify(
                economic_outcomes.get(&recommendation.recommendation_id),
                resolution,
                truth_cutoff,
            )?;
        }
        ensure!(
            outcomes
                .values()
                .all(|resolution| resolution.available_at <= truth_cutoff),
            "successor feedback truth cutoff precedes a reconciled outcome"
        );
        Ok(SuccessorOutcomeEvidence {
            outcomes,
            economic_outcomes,
            truth_cutoff,
        })
    }

    async fn observe(
        &self,
        deadline: Instant,
    ) -> Result<(
        HashMap<RecommendationId, RecommendationResolutionOutcomeInfo>,
        HashMap<RecommendationId, RecommendationEconomicOutcomeInfo>,
    )> {
        let outcome_repository = PgRecommendationResolutionOutcomeRepository::new(self.db.clone());
        let economic_repository = PgRecommendationEconomicOutcomeRepository::new(self.db.clone());
        let mut outcomes = HashMap::new();
        let mut economic_outcomes = HashMap::new();
        // Every query and poll shares the original absolute acceptance deadline.
        // Keep observations outside the future so cancellation preserves diagnostics.
        let observation = timeout_at(deadline, async {
            loop {
                outcomes.clear();
                economic_outcomes.clear();
                for recommendation in self.recommendations {
                    if let Some(resolution) = outcome_repository
                        .find_by_recommendation(&recommendation.recommendation_id)
                        .await?
                    {
                        resolution.validate()?;
                        let expected_fact = self
                            .resolution_plane
                            .facts
                            .iter()
                            .find(|fact| fact.market_id == recommendation.market_id)
                            .context(
                                "successor recommendation lost its source-native resolution fact",
                            )?;
                        let observed_time_matches =
                            resolution.source_observed_at == expected_fact.observed_at;
                        let resolved_time_matches =
                            resolution.resolved_at == expected_fact.resolved_at;
                        ensure!(
                            resolution.recommendation_id == recommendation.recommendation_id
                                && resolution.market_id == recommendation.market_id
                                && resolution.token_id == recommendation.token_id
                                && resolved_time_matches
                                && observed_time_matches
                                && resolution.source_checkpoint_hash
                                    == expected_fact.source_checkpoint_hash
                                && resolution.resolution_fact_hash
                                    == expected_fact.resolution_fact_hash,
                            "successor resolution differs from its frozen recommendation"
                        );
                        outcomes.insert(recommendation.recommendation_id, resolution);
                    }
                    if let Some(economic) = economic_repository
                        .find_by_id(&recommendation.recommendation_id)
                        .await?
                    {
                        economic.verify()?;
                        economic_outcomes.insert(recommendation.recommendation_id, economic);
                    }
                }
                if outcomes.len() == self.recommendations.len()
                    && economic_outcomes.len() == self.recommendations.len()
                {
                    return Ok::<(), AnyhowError>(());
                }
                sleep(POLL_INTERVAL).await;
            }
        })
        .await;
        let expired = match observation {
            // A ready future can win timeout polling at the deadline. Recheck
            // the clock before accepting even a complete last batch.
            Ok(result) => {
                result?;
                Instant::now() >= deadline
            }
            Err(_) => true,
        };
        if expired {
            let diagnostics = match timeout(
                SUCCESSOR_DIAGNOSTIC_TIMEOUT,
                self.failure_state(&outcomes, &economic_outcomes),
            )
            .await
            {
                Ok(Ok(state)) => state,
                Ok(Err(error)) => json!({"capture_error": format!("{error:#}")}),
                Err(error) => json!({"capture_timeout": error.to_string()}),
            };
            bail!(
                "production outcome reconciliation did not project all post-report resolution/economic facts within {SUCCESSOR_OUTCOME_TIMEOUT:?}: resolutions={} economics={} expected={} diagnostics={diagnostics}",
                outcomes.len(),
                economic_outcomes.len(),
                self.recommendations.len()
            );
        }
        Ok((outcomes, economic_outcomes))
    }

    async fn failure_state(
        &self,
        resolutions: &HashMap<RecommendationId, RecommendationResolutionOutcomeInfo>,
        economics: &HashMap<RecommendationId, RecommendationEconomicOutcomeInfo>,
    ) -> Result<JsonValue> {
        let ids = self
            .recommendations
            .iter()
            .map(|recommendation| recommendation.recommendation_id)
            .collect::<Vec<_>>();
        let tasks = EconomicTaskEntity::find()
            .filter(EconomicTaskColumn::RecommendationId.is_in(ids))
            .all(self.db)
            .await
            .context("capture exact forward economic tasks before fixture cleanup")?;
        let tasks = tasks
            .into_iter()
            .map(|task| (task.recommendation_id, task))
            .collect::<HashMap<_, _>>();
        let recommendations = self
            .recommendations
            .iter()
            .map(|recommendation| {
                let id = recommendation.recommendation_id;
                let task = tasks.get(&id).map(|task| {
                    json!({
                        "status": task.status,
                        "attempt_count": task.attempt_count,
                        "horizon_at": task.horizon_at,
                        "replay_until": task.replay_until,
                        "resolution_outcome_hash": task.resolution_outcome_hash,
                        "source_cutoff_at": task.source_cutoff_at,
                        "claim_owner": task.claim_owner,
                        "lease_expires_at": task.lease_expires_at,
                        "next_attempt_at": task.next_attempt_at,
                        "last_error": task.last_error,
                        "completed_at": task.completed_at,
                        "updated_at": task.updated_at,
                    })
                });
                json!({
                    "recommendation_id": id,
                    "market_id": recommendation.market_id,
                    "token_id": recommendation.token_id,
                    "resolution_observed": resolutions.contains_key(&id),
                    "economic_observed": economics.contains_key(&id),
                    "task": task,
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "decision_at": self.decision_at,
            "recommendations": recommendations,
        }))
    }
}

struct SuccessorEconomicIdentity {
    recommendation_id: RecommendationId,
    recommendation_report_id: RecommendationReportId,
    report_route_run_id: ReportRouteRunId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    economic_tier_id: EconomicTierId,
    model_version_id: ModelVersionId,
    trade_policy_artifact_id: TradePolicyArtifactId,
    research_profile_artifact_id: ResearchProfileArtifactId,
    decision_at: DateTime<Utc>,
    horizon_at: DateTime<Utc>,
    passive_entry: bool,
    resolution_knowledge_lag: ChronoDuration,
}

impl TryFrom<&EconomicOutcomeReplayContext> for SuccessorEconomicIdentity {
    type Error = AnyhowError;

    fn try_from(context: &EconomicOutcomeReplayContext) -> Result<Self> {
        let horizon =
            ChronoDuration::seconds(i64::try_from(context.profile_spec.target_horizon_secs)?);
        Ok(Self {
            recommendation_id: context.recommendation.recommendation_id,
            recommendation_report_id: context.report.recommendation_report_id,
            report_route_run_id: context.route_run.report_route_run_id,
            decision_policy_snapshot_id: context.report.decision_policy_snapshot_id,
            economic_tier_id: context.recommendation.economic_tier_id,
            model_version_id: context
                .route_run
                .model_version_id
                .context("economic Route model is absent")?,
            trade_policy_artifact_id: context
                .route_run
                .trade_policy_artifact_id
                .context("economic Route policy is absent")?,
            research_profile_artifact_id: context
                .route_run
                .research_profile_artifact_id
                .clone()
                .context("economic Route profile is absent")?,
            decision_at: context.report.decision_at,
            horizon_at: context
                .report
                .decision_at
                .checked_add_signed(horizon)
                .context("economic horizon overflow")?,
            passive_entry: !matches!(
                context.recommendation.trade_plan.sizing.maker_rebate_terms,
                EntryMakerRebateTerms::AggressiveNotApplicable
            ),
            resolution_knowledge_lag: context.decision_boundary.decision_at()
                - context.decision_boundary.knowledge_cutoff(),
        })
    }
}

impl SuccessorEconomicIdentity {
    fn verify(
        &self,
        economic: Option<&RecommendationEconomicOutcomeInfo>,
        resolution: &RecommendationResolutionOutcomeInfo,
        truth_cutoff: DateTime<Utc>,
    ) -> Result<()> {
        let economic = economic.context("successor economic outcome is missing")?;
        economic.verify()?;
        resolution.validate()?;
        let recommendation_matches = economic.recommendation_id == self.recommendation_id
            && resolution.recommendation_id == self.recommendation_id;
        ensure!(
            recommendation_matches
                && economic.recommendation_report_id == self.recommendation_report_id
                && economic.report_route_run_id == self.report_route_run_id
                && economic.decision_policy_snapshot_id == self.decision_policy_snapshot_id
                && economic.economic_tier_id == self.economic_tier_id
                && economic.model_version_id == self.model_version_id
                && economic.trade_policy_artifact_id == self.trade_policy_artifact_id
                && economic.research_profile_artifact_id == self.research_profile_artifact_id
                && economic.decision_at == self.decision_at
                && economic.horizon_at == self.horizon_at,
            "successor economic outcome differs from frozen identity"
        );
        let terminal = economic
            .payload_json
            .detail
            .terminal_at()
            .context("economic terminal is absent")?;
        let resolution_forward = resolution.source_observed_at > self.decision_at;
        let resolution_visible = resolution.available_at <= truth_cutoff;
        ensure!(
            resolution_forward
                && resolution_visible
                && terminal > self.decision_at
                && terminal < self.horizon_at
                && terminal <= economic.source_available_until
                && economic.source_available_until <= economic.available_at
                && economic.available_at <= truth_cutoff,
            "successor economic outcome is not strictly forward and PIT-visible"
        );
        match &economic.payload_json.detail {
            RecommendationEconomicStateDetail::ResolvedBeforeHorizon {
                entered_at: Some(entered_at),
                resolved_at,
                payout_ratio,
            } => {
                let applied_at = resolution
                    .resolved_at
                    .checked_add_signed(self.resolution_knowledge_lag)
                    .context("successor resolution application time overflow")?
                    .max(resolution.source_observed_at);
                ensure!(
                    *entered_at >= self.decision_at
                        && *entered_at < *resolved_at
                        && *resolved_at == applied_at
                        && *payout_ratio == resolution.token_payout_ratio,
                    "successor economic resolution differs from canonical payout"
                );
            }
            RecommendationEconomicStateDetail::PolicyExited {
                entered_at,
                exited_at,
                exit_reason,
            } => {
                ensure!(
                    *entered_at >= self.decision_at
                        && entered_at <= exited_at
                        && *exit_reason != ExitReason::ResolutionRedeem,
                    "successor policy exit predates its entry"
                );
            }
            _ => bail!(
                "successor economic replay has no verified early executable terminal: {:?}",
                economic.state
            ),
        }
        let amounts = &economic.payload_json.amounts;
        let evidence = &economic.payload_json.evidence;
        ensure!(
            amounts.entry_filled_shares > Shares::ZERO
                && amounts.exited_shares == amounts.entry_filled_shares
                && amounts.entry_cost_usd > Usd::ZERO
                && amounts.net_pnl_usd.is_some()
                && amounts.net_return_bps.is_some()
                && evidence.full_l2_covered
                && evidence.fee_covered
                && evidence.passive_trade_covered != Some(false)
                && (!self.passive_entry || evidence.passive_trade_covered == Some(true))
                && evidence.replay_input_hash != ContentHash::from_bytes([0; 32])
                && evidence.replay_output_hash != ContentHash::from_bytes([0; 32])
                && economic.replay_kernel_version == POLICY_REPLAY_KERNEL_VERSION,
            "successor economic replay lacks complete executable economics or canonical evidence"
        );
        Ok(())
    }
}

#[cfg(test)]
mod successor_economic_tests {
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use quant_pivot_models::{
        domain::quant::{
            EconomicExitEvidenceKind, EconomicOutcomeCensorReason,
            NewRecommendationEconomicOutcome, RecommendationEconomicAmounts,
            RecommendationEconomicEvidence, RecommendationEconomicOutcomeInfo,
            RecommendationEconomicOutcomeInput, RecommendationEconomicOutcomePayload,
            RecommendationEconomicStateDetail, RecommendationResolutionOutcomeInfo,
        },
        enums::{
            execution::ExitReason,
            quant::{RecommendationEconomicOutcomeState, RecommendationResolutionKind},
        },
        types::{
            ContentHash, DecisionPolicySnapshotId, EconomicTierId, MarketId, ModelVersionId,
            PayoutRatio, RecommendationId, RecommendationReportId, ReportRouteRunId,
            ResearchProfileId, ResearchProfileRef, SchemaVersion, Shares, TokenId,
            TradePolicyArtifactId, Usd,
        },
    };
    use quant_pivot_research::policy_replay::POLICY_REPLAY_KERNEL_VERSION;
    use rust_decimal_macros::dec;

    use super::SuccessorEconomicIdentity;

    struct EconomicFixture {
        identity: SuccessorEconomicIdentity,
        resolution: RecommendationResolutionOutcomeInfo,
        input: RecommendationEconomicOutcomeInput,
    }

    impl EconomicFixture {
        fn new() -> Result<Self> {
            let decision_at = Utc::now();
            let recommendation_id = RecommendationId::from_v7();
            let mut resolution = RecommendationResolutionOutcomeInfo {
                recommendation_id,
                market_id: MarketId::new("successor-market"),
                token_id: TokenId::new("123"),
                resolution_kind: RecommendationResolutionKind::SplitPayout,
                token_payout_ratio: PayoutRatio::try_new(dec!(0.5))?,
                resolved_at: decision_at + Duration::seconds(10),
                source_observed_at: decision_at + Duration::seconds(11),
                available_at: decision_at + Duration::seconds(12),
                source_checkpoint_hash: ContentHash::from_bytes([1; 32]),
                resolution_fact_hash: ContentHash::from_bytes([2; 32]),
                resolution_fact_log_index: 1,
                resolution_fact_schema_version: SchemaVersion::FIRST,
                outcome_hash: ContentHash::from_bytes([0; 32]),
                created_at: decision_at + Duration::seconds(12),
            };
            resolution.outcome_hash = resolution.expected_outcome_hash()?;
            let identity = SuccessorEconomicIdentity {
                recommendation_id,
                recommendation_report_id: RecommendationReportId::from_v7(),
                report_route_run_id: ReportRouteRunId::from_v7(),
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                economic_tier_id: EconomicTierId::from_v7(),
                model_version_id: ModelVersionId::from_v7(),
                trade_policy_artifact_id: TradePolicyArtifactId::from_v7(),
                research_profile_artifact_id: ResearchProfileRef {
                    id: ResearchProfileId::new("successor"),
                    version: 1,
                    content_hash: ContentHash::from_bytes([9; 32]),
                }
                .artifact_id(),
                decision_at,
                horizon_at: decision_at + Duration::minutes(15),
                passive_entry: false,
                resolution_knowledge_lag: Duration::zero(),
            };
            let input = RecommendationEconomicOutcomeInput {
                recommendation_id,
                recommendation_report_id: identity.recommendation_report_id,
                report_route_run_id: identity.report_route_run_id,
                decision_policy_snapshot_id: identity.decision_policy_snapshot_id,
                economic_tier_id: identity.economic_tier_id,
                model_version_id: identity.model_version_id,
                trade_policy_artifact_id: identity.trade_policy_artifact_id,
                research_profile_artifact_id: identity.research_profile_artifact_id.clone(),
                state: RecommendationEconomicOutcomeState::ResolvedBeforeHorizon,
                decision_at,
                horizon_at: identity.horizon_at,
                source_available_until: decision_at + Duration::seconds(12),
                replay_kernel_version: POLICY_REPLAY_KERNEL_VERSION.to_owned(),
                payload: RecommendationEconomicOutcomePayload {
                    detail: RecommendationEconomicStateDetail::ResolvedBeforeHorizon {
                        entered_at: Some(decision_at + Duration::seconds(1)),
                        resolved_at: resolution.source_observed_at,
                        payout_ratio: resolution.token_payout_ratio,
                    },
                    amounts: RecommendationEconomicAmounts {
                        entry_filled_shares: Shares::new(dec!(10)),
                        exited_shares: Shares::new(dec!(10)),
                        entry_cost_usd: Usd::new(dec!(4)),
                        exit_proceeds_usd: Usd::ZERO,
                        resolution_payout_usd: Usd::new(dec!(5)),
                        execution_fee_usd: Usd::ZERO,
                        expected_maker_rebate_usd: Usd::ZERO,
                        net_pnl_usd: Some(Usd::new(dec!(1))),
                        net_return_bps: Some(dec!(2500)),
                    },
                    evidence: RecommendationEconomicEvidence {
                        exit_evidence_kind: EconomicExitEvidenceKind::ResolutionPayout,
                        full_l2_covered: true,
                        fee_covered: true,
                        passive_trade_covered: None,
                        replay_input_hash: ContentHash::from_bytes([3; 32]),
                        replay_output_hash: ContentHash::from_bytes([4; 32]),
                    },
                },
                available_at: decision_at + Duration::seconds(13),
            };
            Ok(Self {
                identity,
                resolution,
                input,
            })
        }

        fn outcome(&self) -> Result<RecommendationEconomicOutcomeInfo> {
            let sealed = NewRecommendationEconomicOutcome::try_seal(self.input.clone())?;
            Ok(RecommendationEconomicOutcomeInfo {
                recommendation_id: sealed.recommendation_id,
                recommendation_report_id: sealed.recommendation_report_id,
                report_route_run_id: sealed.report_route_run_id,
                decision_policy_snapshot_id: sealed.decision_policy_snapshot_id,
                economic_tier_id: sealed.economic_tier_id,
                model_version_id: sealed.model_version_id,
                trade_policy_artifact_id: sealed.trade_policy_artifact_id,
                research_profile_artifact_id: sealed.research_profile_artifact_id,
                state: sealed.state,
                decision_at: sealed.decision_at,
                horizon_at: sealed.horizon_at,
                source_available_until: sealed.source_available_until,
                replay_kernel_version: sealed.replay_kernel_version,
                payload_json: sealed.payload_json,
                evidence_hash: sealed.evidence_hash,
                available_at: sealed.available_at,
                created_at: sealed.available_at,
            })
        }

        fn verify(&self) -> Result<()> {
            self.identity.verify(
                Some(&self.outcome()?),
                &self.resolution,
                self.identity.decision_at + Duration::seconds(20),
            )
        }
    }

    impl SuccessorEconomicIdentity {
        pub(super) fn manifest_outcome(
            self,
            terminal: DateTime<Utc>,
        ) -> RecommendationEconomicOutcomeInfo {
            let mut fixture = EconomicFixture::new().expect("valid economic manifest fixture");
            fixture.input.recommendation_id = self.recommendation_id;
            fixture.input.recommendation_report_id = self.recommendation_report_id;
            fixture.input.report_route_run_id = self.report_route_run_id;
            fixture.input.decision_policy_snapshot_id = self.decision_policy_snapshot_id;
            fixture.input.economic_tier_id = self.economic_tier_id;
            fixture.input.model_version_id = self.model_version_id;
            fixture.input.trade_policy_artifact_id = self.trade_policy_artifact_id;
            fixture.input.research_profile_artifact_id = self.research_profile_artifact_id;
            fixture.input.decision_at = self.decision_at;
            fixture.input.horizon_at = self.horizon_at;
            fixture.input.source_available_until = terminal;
            fixture.input.available_at = terminal;
            fixture.input.payload.detail =
                RecommendationEconomicStateDetail::ResolvedBeforeHorizon {
                    entered_at: Some(self.decision_at),
                    resolved_at: terminal,
                    payout_ratio: fixture.resolution.token_payout_ratio,
                };
            fixture.outcome().expect("sealed economic manifest fixture")
        }
    }

    #[test]
    fn valid_early_terminal() -> Result<()> {
        let mut fixture = EconomicFixture::new()?;
        fixture.verify()?;
        fixture.input.state = RecommendationEconomicOutcomeState::PolicyExited;
        fixture.input.payload.detail = RecommendationEconomicStateDetail::PolicyExited {
            entered_at: fixture.identity.decision_at + Duration::seconds(1),
            exited_at: fixture.identity.decision_at + Duration::seconds(5),
            exit_reason: ExitReason::TimeExit,
        };
        fixture.input.payload.evidence.exit_evidence_kind = EconomicExitEvidenceKind::PolicyFill;
        fixture.input.payload.amounts.exit_proceeds_usd = Usd::new(dec!(5));
        fixture.input.payload.amounts.resolution_payout_usd = Usd::ZERO;
        fixture.verify()
    }

    #[test]
    fn missing_or_censored_rejected() -> Result<()> {
        let mut fixture = EconomicFixture::new()?;
        assert!(
            fixture
                .identity
                .verify(None, &fixture.resolution, fixture.input.available_at)
                .is_err()
        );
        fixture.input.state = RecommendationEconomicOutcomeState::Censored;
        fixture.input.payload.detail = RecommendationEconomicStateDetail::Censored {
            censored_at: fixture.resolution.source_observed_at,
            reason: EconomicOutcomeCensorReason::SourceUnavailable,
        };
        fixture.input.payload.evidence.exit_evidence_kind = EconomicExitEvidenceKind::None;
        let error = fixture
            .verify()
            .expect_err("censor must not substitute for executable economics");
        assert!(
            error
                .to_string()
                .contains("no verified early executable terminal")
        );
        Ok(())
    }

    #[test]
    fn future_or_mismatch_rejected() -> Result<()> {
        let mut fixture = EconomicFixture::new()?;
        fixture.input.available_at = fixture.identity.decision_at + Duration::seconds(21);
        assert!(
            fixture
                .verify()
                .expect_err("future economic row must be invisible")
                .to_string()
                .contains("PIT-visible")
        );
        fixture.input.available_at = fixture.identity.decision_at + Duration::seconds(13);
        fixture.input.model_version_id = ModelVersionId::from_v7();
        assert!(
            fixture
                .verify()
                .expect_err("resealed wrong model must be rejected")
                .to_string()
                .contains("frozen identity")
        );
        fixture.input.model_version_id = fixture.identity.model_version_id;
        let mut corrupted = fixture.outcome()?;
        corrupted.evidence_hash = ContentHash::from_bytes([8; 32]);
        assert!(
            fixture
                .identity
                .verify(
                    Some(&corrupted),
                    &fixture.resolution,
                    fixture.input.available_at
                )
                .is_err()
        );
        fixture.input.payload.amounts.net_return_bps = None;
        assert!(fixture.verify().context("missing economic return").is_err());
        Ok(())
    }

    #[test]
    fn passive_requires_trade_coverage() -> Result<()> {
        let mut fixture = EconomicFixture::new()?;
        fixture.identity.passive_entry = true;
        assert!(fixture.verify().is_err());
        fixture.input.payload.evidence.passive_trade_covered = Some(true);
        fixture.verify()?;
        fixture.input.payload.evidence.fee_covered = false;
        assert!(fixture.verify().is_err());
        Ok(())
    }

    #[test]
    fn resolution_requires_visible_application() -> Result<()> {
        let mut fixture = EconomicFixture::new()?;
        fixture.identity.resolution_knowledge_lag = Duration::seconds(10);
        assert!(fixture.verify().is_err());
        let applied_at = fixture.identity.decision_at + Duration::seconds(20);
        fixture.input.payload.detail = RecommendationEconomicStateDetail::ResolvedBeforeHorizon {
            entered_at: Some(fixture.identity.decision_at + Duration::seconds(15)),
            resolved_at: applied_at,
            payout_ratio: fixture.resolution.token_payout_ratio,
        };
        fixture.input.source_available_until = applied_at;
        fixture.input.available_at = applied_at;
        fixture.verify()
    }

    #[test]
    fn late_observed_resolution_valid() -> Result<()> {
        let mut fixture = EconomicFixture::new()?;
        fixture.resolution.resolved_at = fixture.identity.decision_at - Duration::seconds(2);
        fixture.resolution.outcome_hash = fixture.resolution.expected_outcome_hash()?;
        fixture.verify()
    }
}

impl SuccessorRouteVerifier<'_> {
    async fn verify(
        &self,
        route_run_id: ReportRouteRunId,
        mut recommendation_ids: Vec<RecommendationId>,
    ) -> Result<SuccessorRouteFeedbackEvidence> {
        recommendation_ids.sort_by_key(|recommendation_id| recommendation_id.as_uuid());
        let expected_ids = recommendation_ids.iter().copied().collect::<HashSet<_>>();
        let route_run = ReportRouteRunEntity::find_by_id(route_run_id)
            .one(self.db)
            .await?
            .context("successor feedback Route run is missing")?;
        let lineage = route_run
            .lineage_json
            .as_ref()
            .context("successor feedback Route run omitted immutable lineage")?;
        ensure!(
            route_run.model_version_id == Some(lineage.model_version_id),
            "successor feedback Route model identity is inconsistent"
        );
        if route_run.route == BuyModelRoute::Weather {
            ensure!(
                lineage.model_version_id == self.outcome.candidate_model_version_id,
                "successor Weather cohort is not bound to the promoted candidate"
            );
        }
        let window = FeedbackCohortWindow::try_new(
            lineage.research_profile_ref.clone(),
            self.decision_at,
            self.truth_cutoff,
        )?;
        let snapshot = FeedbackCohortSnapshot::try_new(window, self.truth_cutoff)?;
        let model_candidates = self
            .candidates(FeedbackCohort::ModelLearning, &snapshot, &expected_ids)
            .await?;
        let mut resolution_outcome_hashes = self.model_hashes(&model_candidates, &snapshot)?;
        let policy_candidates = self
            .candidates(FeedbackCohort::PolicyEvaluation, &snapshot, &expected_ids)
            .await?;
        let economic_outcomes = self.verify_policy(&policy_candidates, &snapshot)?;
        let execution_candidates = self
            .candidates(FeedbackCohort::ExecutionLearning, &snapshot, &expected_ids)
            .await?;
        Self::verify_execution(&execution_candidates, &snapshot)?;
        resolution_outcome_hashes.sort();
        Ok(SuccessorRouteFeedbackEvidence {
            route: route_run.route,
            report_route_run_id: route_run_id,
            profile_ref: lineage.research_profile_ref.clone(),
            model_version_id: lineage.model_version_id,
            recommendation_ids,
            resolution_outcome_hashes,
            economic_outcome_count: u32::try_from(economic_outcomes.len())?,
            economic_outcomes,
            model_learning_eligible_count: u32::try_from(model_candidates.len())?,
            policy_evaluation_eligible_count: u32::try_from(policy_candidates.len())?,
            execution_learning_censored_count: u32::try_from(execution_candidates.len())?,
            execution_censor_reason: CohortCensorReason::ExecutionOutcomeUnavailableAtCutoff,
        })
    }

    async fn candidates(
        &self,
        cohort: FeedbackCohort,
        snapshot: &FeedbackCohortSnapshot,
        expected_ids: &HashSet<RecommendationId>,
    ) -> Result<Vec<FeedbackCohortCandidate>> {
        let repository = PgFeedbackCohortRepository::new(self.db.clone());
        let candidates = read_feedback_cohort(&repository, cohort, snapshot.clone()).await?;
        current_report_candidates(candidates, self.report_id, expected_ids, cohort)
    }

    fn model_hashes(
        &self,
        candidates: &[FeedbackCohortCandidate],
        snapshot: &FeedbackCohortSnapshot,
    ) -> Result<Vec<ContentHash>> {
        candidates
            .iter()
            .map(|candidate| {
                let decision = evaluate_feedback_cohort(
                    FeedbackCohort::ModelLearning,
                    snapshot,
                    candidate.context(),
                    candidate.resolution_outcome(),
                    candidate.execution_rollup(),
                    candidate.economic_outcome(),
                )?;
                let FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::ModelLearning(
                    evidence,
                )) = decision
                else {
                    bail!(
                        "post-report recommendation {} is not ModelLearning eligible: {decision:?}",
                        candidate.context().recommendation_id()
                    )
                };
                let expected = self
                    .outcomes
                    .get(&candidate.context().recommendation_id())
                    .context("ModelLearning candidate lost its reconciled outcome")?;
                ensure!(
                    evidence.outcome_hash == expected.outcome_hash,
                    "ModelLearning cohort changed the immutable outcome hash"
                );
                Ok(evidence.outcome_hash)
            })
            .collect()
    }

    fn verify_policy(
        &self,
        candidates: &[FeedbackCohortCandidate],
        snapshot: &FeedbackCohortSnapshot,
    ) -> Result<Vec<RecommendationEconomicOutcomeInfo>> {
        let mut economic_outcomes = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let id = candidate.context().recommendation_id();
            let expected = self
                .economic_outcomes
                .get(&id)
                .context("PolicyEvaluation lost verified economic outcome")?;
            let expected_resolution = self
                .outcomes
                .get(&id)
                .context("PolicyEvaluation lost verified resolution")?;
            ensure!(
                candidate.economic_outcome() == Some(expected),
                "PolicyEvaluation changed immutable economic content"
            );
            let decision = evaluate_feedback_cohort(
                FeedbackCohort::PolicyEvaluation,
                snapshot,
                candidate.context(),
                candidate.resolution_outcome(),
                candidate.execution_rollup(),
                candidate.economic_outcome(),
            )?;
            ensure!(
                matches!(
                    decision,
                    FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::PolicyEvaluation {
                        ref economic,
                        execution_state: None,
                        resolution_outcome_hash: Some(hash),
                        execution_rollup_hash: None,
                    }) if hash == expected_resolution.outcome_hash
                        && economic.evidence_hash == expected.evidence_hash
                        && economic.state == expected.state
                        && economic.net_return_bps == expected.payload_json.amounts.net_return_bps
                        && economic.available_at == expected.available_at
                        && economic.horizon_at == expected.horizon_at
                ),
                "post-report recommendation {} has invalid PolicyEvaluation evidence: {decision:?}",
                candidate.context().recommendation_id()
            );
            economic_outcomes.push(expected.clone());
        }
        economic_outcomes.sort_by_key(|economic| economic.recommendation_id.as_uuid());
        Ok(economic_outcomes)
    }

    fn verify_execution(
        candidates: &[FeedbackCohortCandidate],
        snapshot: &FeedbackCohortSnapshot,
    ) -> Result<()> {
        for candidate in candidates {
            let decision = evaluate_feedback_cohort(
                FeedbackCohort::ExecutionLearning,
                snapshot,
                candidate.context(),
                candidate.resolution_outcome(),
                candidate.execution_rollup(),
                candidate.economic_outcome(),
            )?;
            ensure!(
                candidate.execution_rollup().is_none()
                    && decision
                        == FeedbackCohortDecision::Censored(
                            CohortCensorReason::ExecutionOutcomeUnavailableAtCutoff,
                        ),
                "published recommendation {} did not preserve execution uncertainty at the frozen cutoff: {decision:?}",
                candidate.context().recommendation_id()
            );
        }
        Ok(())
    }
}

struct StartedProduction {
    child: Child,
    launch: ProductionLaunch,
    browser_evidence: Option<BrowserFixtureEvidence>,
}

#[derive(Clone)]
struct BackendChild {
    child: Arc<TokioMutex<Child>>,
    log_path: PathBuf,
}

impl BackendChild {
    fn new(child: Child, log_path: PathBuf) -> Self {
        Self {
            child: Arc::new(TokioMutex::new(child)),
            log_path,
        }
    }

    async fn supervise<T, F>(&self, phase: &'static str, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        tokio::pin!(future);
        tokio::select! {
            biased;
            error = self.unexpected_exit(phase) => Err(error),
            result = &mut future => {
                self.ensure_running(phase).await?;
                result
            },
        }
    }

    async fn ensure_running(&self, phase: &'static str) -> Result<()> {
        let status = {
            let mut child = self.child.lock().await;
            child.try_wait()
        }
        .with_context(|| format!("inspect production binary after {phase}"))?;
        status.map_or_else(
            || Ok(()),
            |status| Err(backend_exit_error(status, phase, &self.log_path)),
        )
    }

    async fn begin_shutdown(&self) -> Result<TokioMutexGuard<'_, Child>> {
        let mut child = self.child.lock().await;
        let status = child
            .try_wait()
            .context("inspect production binary before planned shutdown")?;
        if let Some(status) = status {
            return Err(backend_exit_error(
                status,
                "before planned closure shutdown",
                &self.log_path,
            ));
        }
        Ok(child)
    }

    fn verify_exit(&self, status: ExitStatus) -> Result<()> {
        if !status.success() {
            bail!(
                "production binary shutdown failed with {status}; logs={}; tail={}",
                self.log_path.display(),
                backend_log_tail(&self.log_path),
            );
        }
        Ok(())
    }

    async fn unexpected_exit(&self, phase: &'static str) -> AnyhowError {
        loop {
            let status = {
                let mut child = self.child.lock().await;
                child.try_wait()
            };
            match status {
                Ok(Some(status)) => return backend_exit_error(status, phase, &self.log_path),
                Ok(None) => sleep(POLL_INTERVAL).await,
                Err(error) => {
                    return AnyhowError::new(error).context(format!(
                        "inspect production binary during {phase}; logs={}; tail={}",
                        self.log_path.display(),
                        backend_log_tail(&self.log_path),
                    ));
                }
            }
        }
    }
}

struct ProductionLaunchInput<'a> {
    workspace: &'a Workspace,
    listen_port: u16,
    fixture: ProductionStackFixture,
    report_resolves_at: DateTime<Utc>,
    history_evidence: FinalizedExecutionEvidence,
    polygon: &'a Arc<DeterministicPolygonChain>,
    upstream: &'a MockServer,
    history_upstreams: Option<&'a HistoryUpstreams>,
    clob_upstream: &'a DeterministicClobServer,
}

struct ProductionArtifactRuntime {
    config: ArtifactStoreDeployConfig,
    store: Arc<dyn ArtifactStore>,
}

struct ProductionConfigRender<'a> {
    workspace_root: &'a Path,
    run_dir: &'a Path,
    listen_port: u16,
    upstream: &'a MockServer,
    clob_upstream: &'a DeterministicClobServer,
    history: Option<&'a HistoryUpstreams>,
    stack: &'a SystemStack,
    artifact_store: &'a ArtifactStoreDeployConfig,
}

struct StackReadinessServer {
    listener: TokioTcpListener,
}

struct ProductionArtifactStack {
    config: ArtifactStoreDeployConfig,
    container: ContainerAsync<GenericImage>,
}

#[derive(Clone, Copy)]
struct MinioStaleUploadPolicy {
    expiry: &'static str,
    cleanup_interval: &'static str,
}

struct ProductionStartup {
    artifact_infrastructure: Option<ProductionArtifactStack>,
    infrastructure: Option<SystemStack>,
}

#[derive(Clone, Copy)]
enum PrestartHistoryEvidence {
    NotRequired,
    Installed {
        chunk_id: Uuid,
        frontier: ExchangeHistoryFrontier,
        from_block: i64,
        to_block: i64,
        state_revision: i64,
    },
}

impl PrestartHistoryEvidence {
    async fn verify(self, fixture: ProductionStackFixture, db: &DatabaseConnection) -> Result<()> {
        match (fixture.seeds_browser(), self) {
            (false, Self::NotRequired) => Ok(()),
            (
                true,
                Self::Installed {
                    chunk_id,
                    frontier,
                    from_block,
                    to_block,
                    state_revision,
                },
            ) => {
                let persisted = PgExchangeHistoryRepository::new(db.clone())
                    .find_range(frontier, from_block, to_block)
                    .await?
                    .context("pre-start source-fit PG chunk is missing")?;
                ensure!(
                    persisted.chunk_id == chunk_id
                        && persisted.status == ExchangeHistoryChunkStatus::Accepted
                        && persisted.state_revision == Some(state_revision),
                    "pre-start source-fit CH marker differs from its accepted PG cursor"
                );
                Ok(())
            }
            (true, Self::NotRequired) => {
                bail!("browser fixture omitted its pre-start source-fit CH marker")
            }
            (false, Self::Installed { .. }) => {
                bail!("non-browser fixture installed unexpected source-fit history")
            }
        }
    }
}

async fn install_source_history(
    config: &ClickHouseConfig,
    fixture: ProductionStackFixture,
) -> Result<PrestartHistoryEvidence> {
    if !fixture.seeds_browser() {
        return Ok(PrestartHistoryEvidence::NotRequired);
    }
    let row = source_fit_acceptance_row().context("build pre-start source-fit CH marker")?;
    let evidence = PrestartHistoryEvidence::Installed {
        chunk_id: row.chunk_id,
        frontier: ExchangeHistoryFrontier::Retention,
        from_block: i64::try_from(row.from_block).context("source-fit start exceeds i64")?,
        to_block: i64::try_from(row.to_block).context("source-fit end exceeds i64")?,
        state_revision: i64::try_from(row.state_revision)
            .context("source-fit state revision exceeds i64")?,
    };
    let pool = Arc::new(ClickHousePool::connect(config).await?);
    let manager = Arc::new(ChWriteManager::new(
        config.max_concurrent_inserts,
        &config.io,
    ));
    let writer = ChFactWriter::<ExchangeHistoryAcceptanceRow>::new(
        pool,
        manager,
        "quant_exchange_history_acceptance",
    );
    writer
        .write_batch_idempotent(
            &ExchangeHistoryWorker::acceptance_token(row.chunk_id),
            vec![row],
        )
        .await
        .context("durably install pre-start source-fit CH marker")?;
    Ok(evidence)
}

impl ProductionArtifactStack {
    async fn start(run_dir: &Path) -> Result<Self> {
        Self::start_with_stale_policy(
            run_dir,
            MinioStaleUploadPolicy {
                expiry: MINIO_STALE_UPLOADS_EXPIRY,
                cleanup_interval: MINIO_STALE_UPLOADS_CLEANUP_INTERVAL,
            },
        )
        .await
    }

    async fn start_with_stale_policy(
        run_dir: &Path,
        stale_uploads: MinioStaleUploadPolicy,
    ) -> Result<Self> {
        let identity = Uuid::now_v7();
        let server_name = format!("quant-pivot-minio-{identity}");
        let data_dir = run_dir.join("minio-data");
        fs::create_dir_all(&data_dir)
            .with_context(|| format!("create MinIO data root {}", data_dir.display()))?;
        let data_root = data_dir
            .to_str()
            .context("MinIO data root is not valid UTF-8")?;

        let container = GenericImage::new("minio/minio", MINIO_SERVER_IMAGE_TAG)
            .with_exposed_port(MINIO_API_PORT.into())
            .with_wait_for(WaitFor::http(
                // Bucket bootstrap requires initialized object, metadata, and IAM
                // owners plus write quorum; /ready can return 200 while offline.
                HttpWaitStrategy::new("/minio/health/cluster")
                    .with_port(MINIO_API_PORT.into())
                    .with_expected_status_code(200u16),
            ))
            .with_cmd(["server", "/data", "--console-address", ":9001"])
            .with_env_var("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
            .with_env_var("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
            // This pinned MinIO fixture does not persist the AWS
            // AbortIncompleteMultipartUpload lifecycle action. Its native
            // stale-upload sweep is the crash/forced-cancellation backstop;
            // the standard S3 prefix-scoped protocol is verified separately.
            .with_env_var("MINIO_API_STALE_UPLOADS_EXPIRY", stale_uploads.expiry)
            .with_env_var(
                "MINIO_API_STALE_UPLOADS_CLEANUP_INTERVAL",
                stale_uploads.cleanup_interval,
            )
            .with_container_name(server_name)
            .with_mount(Mount::bind_mount(data_root, "/data"))
            .with_startup_timeout(Duration::from_mins(2))
            .start()
            .await
            .context("start production-stack MinIO")?;
        let host_port = container
            .get_host_port_ipv4(MINIO_API_PORT)
            .await
            .context("resolve production-stack MinIO port")?;
        let endpoint = format!("http://127.0.0.1:{host_port}");
        if let Err(error) = Self::configure_bucket(&endpoint).await {
            return match container.rm().await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "remove MinIO after bucket bootstrap failure: {cleanup}"
                ))),
            };
        }

        Ok(Self {
            config: ArtifactStoreDeployConfig {
                kind: ArtifactStoreKind::S3,
                bucket: MINIO_BUCKET.to_owned(),
                prefix: ARTIFACT_KEY_PREFIX.trim_end_matches('/').to_owned(),
                region: MINIO_REGION.to_owned(),
                endpoint: Some(endpoint),
                path_style: true,
                require_object_lock: true,
                require_versioning: true,
            },
            container,
        })
    }

    async fn configure_bucket(endpoint: &str) -> Result<()> {
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(MINIO_REGION))
            .credentials_provider(Credentials::new(
                MINIO_ACCESS_KEY,
                MINIO_SECRET_KEY,
                None,
                None,
                "quant-pivot-production-stack",
            ))
            .load()
            .await;
        let client = S3ControlClient::from_conf(
            S3ConfigBuilder::from(&shared)
                .force_path_style(true)
                .endpoint_url(endpoint)
                .build(),
        );

        client
            .create_bucket()
            .bucket(MINIO_BUCKET)
            .object_lock_enabled_for_bucket(true)
            .send()
            .await
            .context("create Object-Lock-enabled MinIO bucket")?;
        client
            .put_bucket_versioning()
            .bucket(MINIO_BUCKET)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .context("enable MinIO bucket versioning")?;
        client
            .put_object_lock_configuration()
            .bucket(MINIO_BUCKET)
            .object_lock_configuration(
                ObjectLockConfiguration::builder()
                    .object_lock_enabled(ObjectLockEnabled::Enabled)
                    .rule(
                        ObjectLockRule::builder()
                            .default_retention(
                                DefaultRetention::builder()
                                    .mode(ObjectLockRetentionMode::Governance)
                                    .days(MINIO_RETENTION_DAYS)
                                    .build(),
                            )
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .context("configure MinIO default Object Lock retention")?;

        let versioning = client
            .get_bucket_versioning()
            .bucket(MINIO_BUCKET)
            .send()
            .await
            .context("read back MinIO bucket versioning")?;
        ensure!(
            versioning.status() == Some(&BucketVersioningStatus::Enabled),
            "MinIO bucket versioning is not enabled: {:?}",
            versioning.status()
        );
        let object_lock = client
            .get_object_lock_configuration()
            .bucket(MINIO_BUCKET)
            .send()
            .await
            .context("read back MinIO Object Lock configuration")?;
        let configuration = object_lock
            .object_lock_configuration()
            .context("MinIO did not return an Object Lock configuration")?;
        ensure!(
            configuration.object_lock_enabled() == Some(&ObjectLockEnabled::Enabled),
            "MinIO Object Lock is not enabled: {:?}",
            configuration.object_lock_enabled()
        );
        let retention = configuration
            .rule()
            .and_then(ObjectLockRule::default_retention)
            .context("MinIO did not return a default Object Lock retention rule")?;
        ensure!(
            retention.mode() == Some(&ObjectLockRetentionMode::Governance)
                && retention.days() == Some(MINIO_RETENTION_DAYS)
                && retention.years().is_none(),
            "MinIO Object Lock retention does not match the governed fixture contract: {retention:?}"
        );
        Ok(())
    }

    fn store(&self) -> Result<Arc<dyn ArtifactStore>> {
        let credentials = S3StaticCredentials::new(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
            .context("build production-stack MinIO credentials")?;
        let store = S3ArtifactStore::new_with_credentials(&self.config, credentials)
            .context("build production-stack S3 artifact store")?;
        Ok(Arc::new(store))
    }

    async fn shutdown(self) -> Result<()> {
        self.container
            .rm()
            .await
            .context("remove production-stack MinIO")
    }
}

impl StackReadinessServer {
    async fn bind(port: u16) -> Result<Self> {
        let listener = TokioTcpListener::bind((Ipv4Addr::LOCALHOST, port))
            .await
            .with_context(|| format!("bind production-stack readiness port {port}"))?;
        Ok(Self { listener })
    }

    async fn serve(self) -> Result<()> {
        loop {
            let (mut stream, peer) = self
                .listener
                .accept()
                .await
                .context("accept production-stack readiness probe")?;
            if let Err(error) = self.respond(&mut stream).await {
                tracing::warn!(
                    peer = %peer,
                    error = %error,
                    "production-stack readiness probe failed"
                );
            }
        }
    }

    async fn respond(&self, stream: &mut TcpStream) -> Result<()> {
        let mut request = [0_u8; 1_024];
        let read = timeout(READINESS_REQUEST_TIMEOUT, stream.read(&mut request))
            .await
            .context("time out reading production-stack readiness request")?
            .context("read production-stack readiness request")?;
        ensure!(read > 0, "production-stack readiness request was empty");
        let response = if request[..read].starts_with(b"GET /ready HTTP/1.1\r\n") {
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n".as_slice()
        } else {
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n".as_slice()
        };
        stream
            .write_all(response)
            .await
            .context("write production-stack readiness response")?;
        stream
            .shutdown()
            .await
            .context("close production-stack readiness response")
    }
}

impl ProductionStartup {
    const fn new(infrastructure: SystemStack) -> Self {
        Self {
            artifact_infrastructure: None,
            infrastructure: Some(infrastructure),
        }
    }

    fn infrastructure(&self) -> Result<&SystemStack> {
        self.infrastructure
            .as_ref()
            .context("production-stack infrastructure ownership is missing")
    }

    async fn prepare_artifacts(
        &mut self,
        run_dir: &Path,
        fixture: ProductionStackFixture,
    ) -> Result<ProductionArtifactRuntime> {
        if fixture.seeds_browser() {
            self.artifact_infrastructure = Some(
                ProductionArtifactStack::start(run_dir)
                    .await
                    .with_context(|| {
                        format!(
                            "start production artifact infrastructure; retained artifacts={}",
                            run_dir.display()
                        )
                    })?,
            );
        }
        let config = self.artifact_infrastructure.as_ref().map_or_else(
            || ArtifactStoreDeployConfig {
                prefix: run_dir.join("artifacts").to_string_lossy().into_owned(),
                ..ArtifactStoreDeployConfig::default()
            },
            |infrastructure| infrastructure.config.clone(),
        );
        let store: Arc<dyn ArtifactStore> = match self.artifact_infrastructure.as_ref() {
            Some(infrastructure) => infrastructure.store()?,
            None => Arc::new(LocalArtifactStore::new(&config.prefix)),
        };
        Ok(ProductionArtifactRuntime { config, store })
    }

    async fn launch(&mut self, input: ProductionLaunchInput<'_>) -> Result<StartedProduction> {
        {
            let infrastructure = self.infrastructure()?;
            PgModelRegistryRepository::new(infrastructure.postgres.connection().clone())
                .ensure_builtin_research_profiles()
                .await
                .context("bootstrap immutable fresh-deployment research profiles")?;
            if input.fixture.requires_default_policy() {
                ensure_default_policy_bundle(
                    &PgPolicyRepository::new(infrastructure.postgres.connection().clone()),
                    "production-stack-fixture",
                    "canonical fresh-boot policy for the real-binary system fixture",
                )
                .await
                .context("bootstrap canonical fresh-boot policy bundle")?;
            }
        }

        let run_dir = input
            .workspace
            .target_directory
            .join("production-stack")
            .join(Uuid::now_v7().to_string());
        fs::create_dir_all(&run_dir)
            .with_context(|| format!("create production-stack run {}", run_dir.display()))?;
        let artifacts = self.prepare_artifacts(&run_dir, input.fixture).await?;
        let infrastructure = self.infrastructure()?;
        // Install the durable CH marker before any fixture can expose the
        // matching accepted PG cursor. The binary is not spawned until both
        // sides have been verified below.
        let prestart_history =
            install_source_history(&infrastructure.clickhouse_config, input.fixture).await?;
        let browser_evidence = if input.fixture.seeds_browser() {
            Some(
                Box::pin(seed_browser_fixture(
                    infrastructure.postgres.connection(),
                    &infrastructure.clickhouse_config,
                    &artifacts.store,
                    input.fixture,
                    input.report_resolves_at,
                    input.history_evidence,
                    input.polygon,
                ))
                .await
                .with_context(|| {
                    format!(
                        "seed browser production fixture; retained artifacts={}",
                        run_dir.display()
                    )
                })?,
            )
        } else {
            None
        };
        input
            .fixture
            .verify_account_positions(infrastructure.postgres.connection())
            .await?;
        if matches!(
            input.fixture,
            ProductionStackFixture::FeedbackClosure
                | ProductionStackFixture::FeedbackClosureRecovery
        ) {
            settle_closure_clickhouse(&infrastructure.clickhouse_config).await?;
        }
        prestart_history
            .verify(input.fixture, infrastructure.postgres.connection())
            .await?;
        if let Some(closure) = browser_evidence
            .as_ref()
            .and_then(|evidence| evidence.closure.as_ref())
        {
            mount_closure_catalog(input.upstream, closure)
                .await
                .context("mount complete closure Gamma condition responses")?;
        }
        input.polygon.freeze();

        ProductionConfigRender {
            workspace_root: &input.workspace.root,
            run_dir: &run_dir,
            listen_port: input.listen_port,
            upstream: input.upstream,
            clob_upstream: input.clob_upstream,
            history: input.history_upstreams,
            stack: infrastructure,
            artifact_store: &artifacts.config,
        }
        .render()
        .with_context(|| {
            format!(
                "render production config; retained artifacts={}",
                run_dir.display()
            )
        })?;

        let catalog_runtime_started_at =
            infrastructure.postgres.connection().statement_time().await;
        let launch = ProductionLaunch {
            workspace_root: input.workspace.root.clone(),
            binary: input.workspace.binary.clone(),
            run_dir,
            base_url: format!("http://127.0.0.1:{}", input.listen_port),
            uses_fixture_s3: self.artifact_infrastructure.is_some(),
            hypersync_proxy: input
                .history_upstreams
                .map(|history| history.hypersync.uri()),
            catalog_runtime_started_at,
        };
        let child = launch.spawn().await?;
        Ok(StartedProduction {
            child,
            launch,
            browser_evidence,
        })
    }

    fn finish(mut self) -> Result<(Option<ProductionArtifactStack>, SystemStack)> {
        let infrastructure = self
            .infrastructure
            .take()
            .context("production-stack infrastructure ownership is missing")?;
        Ok((self.artifact_infrastructure.take(), infrastructure))
    }

    async fn abort<T>(mut self, error: AnyhowError) -> Result<T> {
        let artifact_result = match self.artifact_infrastructure.take() {
            Some(infrastructure) => infrastructure.shutdown().await,
            None => Ok(()),
        };
        let infrastructure_result = match self.infrastructure.take() {
            Some(infrastructure) => Box::pin(infrastructure.shutdown())
                .await
                .context("remove production-stack infrastructure after startup failure"),
            None => Ok(()),
        };
        let cleanup_detail = match (artifact_result, infrastructure_result) {
            (Ok(()), Ok(())) => return Err(error),
            (Err(artifact), Ok(())) => {
                format!("artifact infrastructure cleanup also failed: {artifact:#}")
            }
            (Ok(()), Err(infrastructure)) => {
                format!("base infrastructure cleanup also failed: {infrastructure:#}")
            }
            (Err(artifact), Err(infrastructure)) => format!(
                "artifact infrastructure cleanup also failed: {artifact:#}; base infrastructure cleanup also failed: {infrastructure:#}"
            ),
        };
        Err(error.context(cleanup_detail))
    }
}

impl ProductionLaunch {
    fn log_path(&self) -> PathBuf {
        self.run_dir.join("backend.log")
    }

    async fn spawn(&self) -> Result<Child> {
        let log_path = self.log_path();
        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("open backend log {}", log_path.display()))?;
        let stderr = stdout
            .try_clone()
            .with_context(|| format!("clone backend log handle {}", log_path.display()))?;
        let mut command = TokioCommand::new(&self.binary);
        let config_file = self.run_dir.join("quant-pivot.toml");
        command
            .arg("--config-file")
            .arg(&config_file)
            .arg("--expected-environment")
            .arg("local-development")
            .current_dir(&self.workspace_root)
            .env("RUST_LOG", "info,polymarket_client_sdk_v2=error")
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true);
        if self.uses_fixture_s3 {
            command
                .env("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
                .env("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
                .env("AWS_REGION", MINIO_REGION)
                .env("AWS_DEFAULT_REGION", MINIO_REGION)
                .env("AWS_EC2_METADATA_DISABLED", "true")
                .env_remove("AWS_PROFILE")
                .env_remove("AWS_SESSION_TOKEN")
                .env_remove("AWS_WEB_IDENTITY_TOKEN_FILE")
                .env_remove("AWS_ROLE_ARN");
        }
        if let Some(proxy) = &self.hypersync_proxy {
            command
                .env("HTTP_PROXY", proxy)
                .env("http_proxy", proxy)
                .env("NO_PROXY", "127.0.0.1,localhost")
                .env("no_proxy", "127.0.0.1,localhost")
                .env_remove("ALL_PROXY")
                .env_remove("all_proxy")
                .env_remove("HTTPS_PROXY")
                .env_remove("https_proxy");
        }
        let mut child = command
            .spawn()
            .with_context(|| format!("spawn production binary {}", self.binary.display()))?;
        if let Err(error) = await_startup(&mut child, &self.base_url).await {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(error.context(format!(
                "production binary did not become ready; logs={}; tail={}",
                log_path.display(),
                backend_log_tail(&log_path),
            )));
        }
        Ok(child)
    }
}

fn backend_exit_error(status: ExitStatus, phase: &str, log_path: &Path) -> AnyhowError {
    anyhow!(
        "production binary exited during {phase} with {status}; logs={}; tail={}",
        log_path.display(),
        backend_log_tail(log_path),
    )
}

fn backend_log_tail(log_path: &Path) -> String {
    let mut file = match File::open(log_path) {
        Ok(file) => file,
        Err(error) => return format!("<unavailable: {error}>"),
    };
    let length = match file.metadata() {
        Ok(metadata) => metadata.len(),
        Err(error) => return format!("<metadata unavailable: {error}>"),
    };
    let start = length.saturating_sub(BACKEND_LOG_TAIL_BYTES);
    if let Err(error) = file.seek(SeekFrom::Start(start)) {
        return format!("<seek unavailable: {error}>");
    }
    let mut bytes = Vec::with_capacity(usize::try_from(length - start).unwrap_or_default());
    if let Err(error) = file.take(BACKEND_LOG_TAIL_BYTES).read_to_end(&mut bytes) {
        return format!("<read unavailable: {error}>");
    }
    let tail = String::from_utf8_lossy(&bytes);
    let tail = tail.trim();
    if tail.is_empty() {
        "<empty>".to_owned()
    } else {
        tail.to_owned()
    }
}

pub struct ProductionStack {
    browser_closure_monitor: Option<JoinHandle<Result<()>>>,
    cancellation_owner: Option<FixtureCancellationOwner>,
    child: BackendChild,
    clob_accepted_baseline: u64,
    closure_cycle_id: Option<FeedbackCycleId>,
    fixture: ProductionStackFixture,
    launch: ProductionLaunch,
    listen_port: u16,
    upstream: MockServer,
    history_upstreams: Option<HistoryUpstreams>,
    clob_upstream: DeterministicClobServer,
    artifact_infrastructure: Option<ProductionArtifactStack>,
    infrastructure: SystemStack,
    pending_closure_manifest: Option<Box<PendingClosureManifest>>,
}

/// Coherent seed surface installed before the real production binary starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ProductionStackFixture {
    /// Canonical empty fresh deployment.
    Empty,
    /// Coherent research, report, intent, and settlement browser evidence.
    Browser,
    /// Browser evidence plus one exact active Weather serving route.
    GovernedFeedback,
    /// Historical PIT facts plus one real production feedback closure cycle.
    FeedbackClosure,
    /// Feedback closure with a real binary crash and durable lease recovery.
    FeedbackClosureRecovery,
}

#[derive(Clone, Copy)]
enum BrowserAccountStage {
    BeforeEntry,
    SettledHolding,
}

struct AccountExecutionOwner<'a> {
    funder: &'a EvmAddress,
    maker: &'a EvmAddress,
    taker: &'a EvmAddress,
    role: AccountChainExecutionRole,
}

impl AccountExecutionOwner<'_> {
    fn matches(&self) -> bool {
        // V2 liquidity role does not change the account-owned order's maker.
        self.maker == self.funder
            && match self.role {
                AccountChainExecutionRole::Maker | AccountChainExecutionRole::Taker => true,
                AccountChainExecutionRole::SelfMatch => self.taker == self.funder,
            }
    }
}

impl ProductionStackFixture {
    const fn calibration_preset(self) -> Option<CalibrationEvidencePreset> {
        match self {
            Self::GovernedFeedback => Some(CalibrationEvidencePreset::Baseline),
            Self::FeedbackClosure | Self::FeedbackClosureRecovery => {
                Some(CalibrationEvidencePreset::StrongBinarySignal)
            }
            Self::Empty | Self::Browser => None,
        }
    }

    fn book_timing(self) -> Result<FixtureBookTiming> {
        match self {
            Self::FeedbackClosure | Self::FeedbackClosureRecovery => {
                Ok(FixtureBookTiming::closure()?)
            }
            _ => Ok(FixtureBookTiming::standard()),
        }
    }
    const fn history_enabled(self) -> bool {
        matches!(self, Self::FeedbackClosure)
    }

    const fn account_collateral_usd(self) -> Decimal {
        match self {
            Self::Empty | Self::Browser => dec!(100),
            Self::GovernedFeedback => dec!(5000),
            Self::FeedbackClosure | Self::FeedbackClosureRecovery => dec!(555.56),
        }
    }

    fn account_positions(self) -> JsonValue {
        if !matches!(self, Self::Browser | Self::GovernedFeedback) {
            return json!([]);
        }
        // The browser seed owns one filled, not-yet-redeemed Yes lot in a
        // resolved market. The venue explicitly marks that winning token at
        // its one-dollar payout; settlement writes remain disabled. Existing
        // collateral is cash, not NLV: this holding adds $40 to NLV while the
        // governed budget cap and available cash stay unchanged.
        let current_price = Decimal::ONE;
        let initial_value = ENTRY_FILLED_SHARES * ENTRY_PRICE;
        let current_value = ENTRY_FILLED_SHARES * current_price;
        let cash_pnl = current_value - initial_value;
        json!([{
            "proxyWallet": FUNDER,
            "asset": BROWSER_SETTLEMENT_TOKEN_ID,
            "conditionId": BROWSER_SETTLEMENT_MARKET_ID,
            "size": ENTRY_FILLED_SHARES.to_string(),
            "avgPrice": ENTRY_PRICE.to_string(),
            "initialValue": initial_value.to_string(),
            "curPrice": current_price.to_string(),
            "currentValue": current_value.to_string(),
            "cashPnl": cash_pnl.to_string(),
            "percentPnl": (cash_pnl / initial_value * dec!(100)).to_string(),
            "totalBought": ENTRY_FILLED_SHARES.to_string(),
            "realizedPnl": "0",
            "percentRealizedPnl": "0",
            "redeemable": true,
            "mergeable": false,
            "negativeRisk": false,
            "outcome": "Yes",
            "outcomeIndex": 0,
        }])
    }

    async fn browser_equity(
        self,
        db: &DatabaseConnection,
        stage: BrowserAccountStage,
    ) -> Result<ReportEquitySnapshot> {
        ensure!(
            matches!(self, Self::Browser | Self::GovernedFeedback),
            "browser account stage requires a browser fixture"
        );
        let funder = EvmAddress::parse(FUNDER)?;
        let account = PgExecutionAccountRepository::new(db.clone())
            .ensure(NewExecutionAccount::build(
                137,
                funder.clone(),
                ExecutionWalletKind::Eoa,
                funder.clone(),
                funder,
                None,
                None,
            )?)
            .await?;
        let policy = PgPolicyRepository::new(db.clone())
            .load_current()
            .await?
            .context("browser account has no active policy")?;
        let budget = Usd::new(
            policy
                .snapshot
                .execution_risk
                .portfolio
                .budget
                .total_budget_usd
                .value,
        );
        let (collateral, venue_positions) = match stage {
            BrowserAccountStage::BeforeEntry => (
                self.account_collateral_usd() + EXECUTION_NOTIONAL,
                Vec::new(),
            ),
            BrowserAccountStage::SettledHolding => (
                self.account_collateral_usd(),
                serde_json::from_value::<Vec<VenuePosition>>(self.account_positions())?,
            ),
        };
        let positions = venue_positions
            .into_iter()
            .map(|position| VenuePositionSnapshot {
                token_id: TokenId::new(position.asset),
                market_id: MarketId::new(position.condition_id),
                event_id: Some(EventId::new("browser-settlement-event")),
                category: MarketCategory::Weather,
                outcome: position.outcome,
                size: Shares::new(position.size),
                avg_price: Price::new(position.avg_price),
                cur_price: Price::new(position.cur_price),
                current_value: Usd::new(position.current_value),
                redeemable: position.redeemable,
            })
            .collect::<Vec<_>>();
        let collateral = Usd::new(collateral);
        let nlv = collateral
            + positions
                .iter()
                .map(|position| position.current_value)
                .sum::<Usd>();
        let reserved = PgReservedCapitalRepository::new(db.clone())
            .sum_reserved_usd()
            .await?;
        let snapshot = AccountSnapshot::new(
            db.statement_time().await,
            AccountSource::Polymarket,
            nlv,
            nlv.min(budget),
            (collateral - reserved).max(Usd::ZERO),
            reserved,
            positions,
        );
        EquitySnapshotService::new(
            Arc::new(PgEquitySnapshotRepository::new(db.clone())),
            Arc::new(PgStrategyPositionLotRepository::new(db.clone())),
            Arc::new(PgVenueIncentiveRepository::new(db.clone())),
            account.execution_account_id,
        )
        .snapshot_for_report(&snapshot)
        .await
        .map_err(Into::into)
    }

    async fn verify_account_positions(self, db: &DatabaseConnection) -> Result<()> {
        let funder = EvmAddress::parse(FUNDER)?;
        let account = NewExecutionAccount::build(
            137,
            funder.clone(),
            ExecutionWalletKind::Eoa,
            funder.clone(),
            funder,
            None,
            None,
        )?;
        if matches!(self, Self::Browser | Self::GovernedFeedback) {
            let identity = db
                .query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "SELECT EXISTS (SELECT 1 FROM quant_account_snapshot WHERE execution_account_id <> $1) \
                         OR EXISTS (SELECT 1 FROM quant_order_intent WHERE execution_account_id <> $1) \
                         OR EXISTS (SELECT 1 FROM quant_strategy_position_lot WHERE execution_account_id <> $1) \
                         AS mismatched",
                    [account.execution_account_id.into()],
                ))
                .await?
                .context("browser account identity verification returned no row")?;
            ensure!(
                !identity.try_get::<bool>("", "mismatched")?,
                "browser report, intent, or lot has inconsistent account identity",
            );
            for execution in AccountChainExecutionEntity::find().all(db).await? {
                let owner = AccountExecutionOwner {
                    funder: &account.funder_address,
                    maker: &execution.maker_address,
                    taker: &execution.taker_address,
                    role: execution.role,
                };
                ensure!(
                    execution.execution_account_id == account.execution_account_id
                        && execution.chain_id == account.chain_id
                        && owner.matches(),
                    "browser chain execution has inconsistent account identity",
                );
            }
        }
        let positions: Vec<VenuePosition> = serde_json::from_value(self.account_positions())
            .context("decode deterministic venue account positions")?;
        let lots = PgStrategyPositionLotRepository::new(db.clone())
            .find_open_lots()
            .await
            .context("read open lots for venue fixture consistency")?;
        ensure!(
            lots.len() == positions.len(),
            "deterministic venue account has {} positions but the fixture owns {} open lots",
            positions.len(),
            lots.len(),
        );
        for lot in lots {
            let position = positions
                .iter()
                .find(|position| position.asset == lot.token_id.as_str())
                .with_context(|| {
                    format!(
                        "fixture lot {} has no venue position",
                        lot.strategy_position_lot_id
                    )
                })?;
            ensure!(
                position.condition_id == lot.market_id.as_str()
                    && lot.execution_account_id == account.execution_account_id
                    && position.size == lot.shares.inner()
                    && position.avg_price == ENTRY_PRICE
                    && position.initial_value == position.size * position.avg_price
                    && lot.cost_usd.inner() == EXECUTION_NOTIONAL
                    && lot.cost_usd == lot.shares * lot.avg_price
                    && position.current_value == position.size * position.cur_price,
                "fixture lot {} differs from its venue account holding",
                lot.strategy_position_lot_id,
            );
            let market = MarketEntity::find_by_id(lot.market_id)
                .one(db)
                .await?
                .context("venue account fixture market is missing")?;
            ensure!(
                market.status == MarketStatus::Settled
                    && market.outcome.as_deref() == Some(position.outcome.as_str())
                    && position.cur_price == Decimal::ONE
                    && position.redeemable,
                "venue account fixture mark does not match the seeded winning settlement outcome",
            );
        }
        Ok(())
    }

    async fn deterministic_upstream(
        self,
        report_resolves_at: DateTime<Utc>,
        polygon: Arc<DeterministicPolygonChain>,
    ) -> Result<MockServer> {
        let server = MockServer::start().await;
        let collateral = (self.account_collateral_usd() * dec!(1_000_000))
            .normalize()
            .to_string();
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(move |request: &Request| {
                deterministic_polygon_rpc(request, polygon.as_ref())
            })
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/order"))
            .respond_with(
                ResponseTemplate::new(405)
                    .set_body_string("fixture_forbids_real_venue_order_write"),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path_regex(r"^/__fixture__/relayer-denied(?:/.*)?$"))
            .respond_with(
                ResponseTemplate::new(405).set_body_string("fixture_forbids_relayer_write"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/time"))
            .respond_with(ResponseTemplate::new(200).set_body_string("1000000"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/version"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "version": 2 })),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/auth/derive-api-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "apiKey": API_KEY,
                "passphrase": API_PASSPHRASE,
                "secret": API_SECRET,
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/balance-allowance"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "balance": collateral,
                "allowances": {},
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/positions"))
            .and(query_param("user", FUNDER))
            .and(query_param("offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(self.account_positions()))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/positions"))
            .and(query_param("user", FUNDER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
            .with_priority(6)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/rebates/current"))
            .and(query_param("maker_address", FUNDER))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/activity"))
            .and(query_param("user", FUNDER))
            .and(query_param("type", "MAKER_REBATE,TAKER_REBATE"))
            .and(query_param("sortBy", "TIMESTAMP"))
            .and(query_param("sortDirection", "ASC"))
            .and(query_param("limit", "500"))
            .and(query_param("offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/heartbeats"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "heartbeat_id": "00000000-0000-0000-0000-000000000001",
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/events/keyset"))
            .and(query_param("active", "true"))
            .and(query_param("closed", "false"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(self.deterministic_gamma(report_resolves_at)?),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/events/keyset"))
            .and(query_param("active", "false"))
            .and(query_param("closed", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [],
                "next_cursor": null,
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/markets"))
            .respond_with(deterministic_market_by_condition)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/clob-markets/{}", synthetic_condition_id())))
            .respond_with(synthetic_clob_market_info)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/clob-markets/{BROWSER_MARKET_ID}")))
            .respond_with(|_: &Request| {
                let no_token_id = fixture_no_token_id(BROWSER_MARKET_ID, BROWSER_TOKEN_ID);
                clob_market_info_response(
                    BROWSER_MARKET_ID,
                    BROWSER_TOKEN_ID,
                    no_token_id.as_str(),
                    CENT_ORDER_RULES,
                    false,
                )
            })
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex(
                r"^/clob-markets/feedback-closure-(training|calibration|evaluation|shadow|report-crypto|report-weather)-market-[0-9]+$",
            ))
            .respond_with(deterministic_closure_market_info)
            .mount(&server)
            .await;
        Ok(server)
    }

    fn deterministic_gamma(self, report_resolves_at: DateTime<Utc>) -> Result<JsonValue> {
        if matches!(self, Self::FeedbackClosure | Self::FeedbackClosureRecovery) {
            let report_resolves_at =
                report_resolves_at.to_rfc3339_opts(SecondsFormat::Millis, true);
            let events = [
                ("report-crypto", "Crypto", 750_000_usize, 850_000_usize),
                ("report-weather", "Weather", 760_000_usize, 860_000_usize),
            ]
            .into_iter()
            .map(
                |(scope, category, yes_base, no_base)| -> Result<JsonValue> {
                    let markets = (1_usize..=5)
                        .map(|ordinal| -> Result<JsonValue> {
                            let market_id = format!("feedback-closure-{scope}-market-{ordinal}");
                            let (question, description) = closure_market_text(scope, ordinal)?;
                            Ok(serde_json::json!({
                                "id": market_id,
                                "conditionId": market_id,
                                "question": question,
                                "slug": format!("feedback-closure-{scope}-market-{ordinal}"),
                                "description": description,
                                "clobTokenIds": [
                                    (yes_base + ordinal).to_string(),
                                    (no_base + ordinal).to_string()
                                ],
                                "outcomes": ["Yes", "No"],
                                "active": true,
                                "closed": false,
                                "enableOrderBook": true,
                                "acceptingOrders": true,
                                "orderMinSize": CLOSURE_ORDER_RULES.minimum_order_size.inner().to_string(),
                                "orderPriceMinTickSize": CLOSURE_ORDER_RULES.tick_size.as_decimal().to_string(),
                                "negRisk": CLOSURE_NEG_RISK,
                                "liquidityNum": "100000",
                                "volume24hr": "10000",
                                "createdAt": "2026-01-01T00:00:00Z",
                                "updatedAt": "2026-08-05T00:00:00Z",
                                "endDate": report_resolves_at
                            }))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(serde_json::json!({
                        "id": format!("feedback-closure-{scope}-event"),
                        "title": format!("Deterministic {scope} report event"),
                        "slug": format!("feedback-closure-{scope}-event"),
                        "active": true,
                        "closed": false,
                        "negRisk": CLOSURE_NEG_RISK,
                        "tags": [{"label": category, "slug": category.to_ascii_lowercase()}],
                        "markets": markets,
                        "createdAt": "2026-01-01T00:00:00Z",
                        "updatedAt": "2026-08-05T00:00:00Z",
                        "endDate": report_resolves_at
                    }))
                },
            )
            .collect::<Result<Vec<_>>>()?;
            return Ok(serde_json::json!({"events": events, "next_cursor": null}));
        }
        let condition_id = synthetic_condition_id();
        let report_resolves_at = report_resolves_at.to_rfc3339_opts(SecondsFormat::Millis, true);
        Ok(serde_json::json!({
            "events": [{
                "id": "production-stack-external-event",
                "title": "Deterministic production-stack external event",
                "slug": "production-stack-external-event",
                "active": true,
                "closed": false,
                "negRisk": false,
                "tags": [{"label": "Crypto", "slug": "crypto"}],
                "markets": [{
                    "id": "production-stack-external-market",
                    "conditionId": condition_id,
                    "question": "Will the deterministic production-stack catalog remain healthy?",
                    "slug": "production-stack-external-market",
                    "clobTokenIds": ["900001", "900002"],
                    "outcomes": ["Yes", "No"],
                    "active": true,
                    "closed": false,
                    "enableOrderBook": true,
                    "acceptingOrders": true,
                    "orderMinSize": "1",
                    "orderPriceMinTickSize": "0.01",
                    "liquidityNum": "100000",
                    "volume24hr": "10000",
                    "createdAt": "2026-01-01T00:00:00Z",
                    "updatedAt": "2026-08-05T00:00:00Z",
                    "endDate": report_resolves_at
                }],
                "createdAt": "2026-01-01T00:00:00Z",
                "updatedAt": "2026-08-05T00:00:00Z",
                "endDate": report_resolves_at
            }],
            "next_cursor": null
        }))
    }

    const fn seeds_browser(self) -> bool {
        matches!(
            self,
            Self::Browser
                | Self::GovernedFeedback
                | Self::FeedbackClosure
                | Self::FeedbackClosureRecovery
        )
    }

    const fn requires_default_policy(self) -> bool {
        matches!(self, Self::Empty)
    }

    async fn seed_research_fixture(
        self,
        db: &DatabaseConnection,
        store: &Arc<dyn ArtifactStore>,
    ) -> Result<(SharedDemoInfra, BrowserResearchFixture)> {
        match self {
            Self::Browser => {
                let serving = Box::pin(seed_browser_serving_infra(db, store)).await?;
                Box::pin(publish_pooled_control_model(
                    db,
                    store,
                    serving.pooled_model_version_id,
                    serving.template.decision_policy_snapshot_id,
                ))
                .await;
                let infra = serving.template;
                let research = Box::pin(seed_browser_research(db, store, &infra)).await?;
                Ok((infra, research))
            }
            Self::GovernedFeedback => {
                let governed = Box::pin(seed_feedback_serving_infra(
                    db,
                    store,
                    FeedbackServingFixtureConfig {
                        book_timing: self.book_timing()?,
                        required_shadow_window_secs: 86_400,
                        shadow_diff_threshold: dec!(0.10),
                        feedback_budget_usd: self.account_collateral_usd(),
                        outcome_reconciliation_enabled: true,
                        outcome_reconciliation_sweep_secs: CLOSURE_OUTCOME_SWEEP_SECS,
                        ad_hoc_report_enabled: false,
                        knowledge_lag_secs: 10,
                    },
                ))
                .await;
                Box::pin(publish_pooled_control_model(
                    db,
                    store,
                    governed.pooled_model_version_id,
                    governed.template.decision_policy_snapshot_id,
                ))
                .await;
                let research = Box::pin(seed_governed_feedback_research(
                    db,
                    store,
                    &governed.template,
                    governed.champion_model_version_id,
                ))
                .await?;
                Ok((governed.template, research))
            }
            Self::FeedbackClosure | Self::FeedbackClosureRecovery => {
                let governed = Box::pin(seed_feedback_serving_infra(
                    db,
                    store,
                    FeedbackServingFixtureConfig {
                        book_timing: self.book_timing()?,
                        required_shadow_window_secs: CLOSURE_SHADOW_WINDOW_SECS,
                        shadow_diff_threshold: dec!(1),
                        feedback_budget_usd: self.account_collateral_usd(),
                        outcome_reconciliation_enabled: true,
                        outcome_reconciliation_sweep_secs: CLOSURE_OUTCOME_SWEEP_SECS,
                        ad_hoc_report_enabled: true,
                        knowledge_lag_secs: CLOSURE_HISTORY_LAG_SECS,
                    },
                ))
                .await;
                Box::pin(publish_pooled_control_model(
                    db,
                    store,
                    governed.pooled_model_version_id,
                    governed.template.decision_policy_snapshot_id,
                ))
                .await;
                let research = Box::pin(seed_closure_feedback_research(
                    db,
                    store,
                    &governed.template,
                    governed.champion_model_version_id,
                ))
                .await?;
                Ok((governed.template, research))
            }
            Self::Empty => bail!("empty production fixture cannot seed browser evidence"),
        }
    }

    async fn claim_cancellation_cycle(
        self,
        db: &DatabaseConnection,
        cycle_id: Option<FeedbackCycleId>,
    ) -> Result<Option<FeedbackCycleClaim>> {
        let Some(cycle_id) = cycle_id else {
            ensure!(
                self != Self::GovernedFeedback,
                "governed feedback fixture is missing its queued cancellation cycle"
            );
            return Ok(None);
        };
        ensure!(
            self == Self::GovernedFeedback,
            "only a governed feedback fixture may seed a cancellation cycle"
        );
        // Startup transfers this exact claim to a live fixture-owned worker.
        // Cancellation releases it for the production coordinator to finish.
        let claim = PgFeedbackCycleRepository::new(db.clone())
            .claim_cycle(WorkerId::from_v7(), GOVERNED_CANCELLATION_LEASE_SECS)
            .await?
            .context("claim governed cancellation fixture cycle")?;
        ensure!(
            claim.cycle.feedback_cycle_id == cycle_id,
            "governed cancellation fixture claimed {}, expected {}",
            claim.cycle.feedback_cycle_id,
            cycle_id,
        );
        Ok(Some(claim))
    }
}

impl HistoryUpstreams {
    /// Materialize a bounded, fully attested Activation prefix before Browser
    /// artifacts are published; no runtime control or persistent config changes.
    async fn serving_head(
        db: &DatabaseConnection,
        clickhouse: &ClickHouseConfig,
        polygon: &Arc<DeterministicPolygonChain>,
    ) -> Result<HistoryServingHeadSeal> {
        let controls = PgRuntimeControlRepository::new(db.clone());
        let control_before = controls.load().await?;
        let policy = PgPolicyRepository::new(db.clone());
        let policy_before = policy
            .load_current_bundle()
            .await?
            .context("Browser policy is missing")?;
        let repository = Arc::new(PgExchangeHistoryRepository::new(db.clone()));
        let plan = repository
            .load_plan(137)
            .await?
            .context("Browser history plan is missing")?;
        let model_head = polygon
            .head()
            .block_number
            .checked_sub(MODEL_CONFIRMATION_BLOCKS)
            .context("Browser source head is below N+12")?;
        let window_start = u64::try_from(plan.activation_from_block)?;
        let block_budget = model_head
            .checked_sub(window_start)
            .and_then(|span| span.checked_add(1))
            .context("Browser Activation history window is invalid")?;
        let upstreams = Self::start(Arc::clone(polygon)).await?;
        let mut config = FinalizedExchangeHistoryConfig {
            enabled: true,
            max_blocks_per_chunk: 50_000,
            hot_window_blocks_per_tick: block_budget,
            ..FinalizedExchangeHistoryConfig::default()
        };
        config.attestor.rpc_endpoint = PolygonRpcEndpoint::Public {
            url: upstreams.attestor.uri(),
        };
        config.attestor.max_blocks_per_log_request = 50_000;
        config.hypersync.endpoint = upstreams.hypersync.uri();
        config.hypersync.api_token = HYPERSYNC_TOKEN.into();
        ensure!(
            ExchangeHistoryWorker::availability_policy_hash(&config)? == plan.policy_hash,
            "Browser source worker differs from the immutable history policy"
        );
        let pool = Arc::new(ClickHousePool::connect(clickhouse).await?);
        let manager = Arc::new(ChWriteManager::new(
            clickhouse.max_concurrent_inserts,
            &clickhouse.io,
        ));
        let writers = ExchangeHistoryWriters {
            raw_logs: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_exchange_log_raw",
            )),
            events: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_exchange_event",
            )),
            fee_charges: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_exchange_fee_charge",
            )),
            matches: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                Arc::clone(&manager),
                "quant_exchange_match",
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
            acceptance: Arc::new(ChFactWriter::new(
                Arc::clone(&pool),
                manager,
                "quant_exchange_history_acceptance",
            )),
        };
        let worker = ExchangeHistoryWorker::connect(
            Arc::clone(&repository) as Arc<dyn ExchangeHistoryRepository>,
            Arc::new(PgMarketRepository::new(db.clone())),
            writers,
            config,
            ExchangeHistoryProgressHandle::fresh_boot(),
            Arc::new(MetricsHub::new()),
        )?;
        Box::pin(timeout(Duration::from_mins(3), async {
            worker.probe().await?;
            worker.run_once().await
        }))
        .await
        .context("Browser Activation history exceeded its bounded pre-start budget")??;
        let head = repository
            .latest_serving_head(ExchangeHistoryFrontier::Activation)
            .await?
            .context("Browser Activation worker did not publish a serving head")?;
        let head = repository
            .validate_serving_head(head.seal.serving_head_seal_id, head.seal.seal_hash)
            .await?;
        ensure!(
            head.seal.window_from_block == plan.activation_from_block
                && u64::try_from(head.seal.accepted_through_block)? >= model_head
                && head.seal.effective_through_at <= head.seal.created_at,
            "Browser head omitted its frozen Activation window or violates source time"
        );
        ChQuantFactReadRepository::new(pool)
            .validate_execution_history_chunks(head.chunks.clone())
            .await?;
        let policy_after = policy
            .load_current_bundle()
            .await?
            .context("Browser policy disappeared")?;
        ensure!(
            controls.load().await? == control_before
                && policy_after.decision_policy_snapshot_id
                    == policy_before.decision_policy_snapshot_id
                && policy_after.snapshot_hash == policy_before.snapshot_hash,
            "Browser history materialization changed a runtime policy or execution authority"
        );
        Ok(head)
    }

    async fn start(polygon: Arc<DeterministicPolygonChain>) -> Result<Self> {
        let minimum_serving_block = i64::try_from(
            polygon
                .head()
                .block_number
                .saturating_sub(MODEL_CONFIRMATION_BLOCKS),
        )?;
        let attestor = MockServer::start().await;
        let attestor_chain = Arc::clone(&polygon);
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(move |request: &Request| {
                deterministic_polygon_rpc(request, attestor_chain.as_ref())
            })
            .mount(&attestor)
            .await;
        let hypersync = start_hypersync(polygon).await?;
        Ok(Self {
            attestor,
            hypersync,
            minimum_serving_block,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShutdownOrigin {
    Harness(ProductionShutdownSignal),
    ProcessTreeSignal,
}

/// OS signal exercised by an owned production-binary shutdown contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProductionShutdownSignal {
    Terminate,
    Interrupt,
}

impl ProductionShutdownSignal {
    const fn kill_argument(self) -> &'static str {
        match self {
            Self::Terminate => "-TERM",
            Self::Interrupt => "-INT",
        }
    }
}

/// Defines who owns the governed actions after the production DAG reaches
/// `CandidateReady`. Verification runs exercise those actions through the
/// harness HTTP client; browser evidence runs deliberately leave them to the
/// operator-facing UI so the same immutable candidate is never activated
/// twice by competing test owners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProductionStackPurpose {
    BrowserEvidence,
    Verification,
}

#[derive(Debug)]
enum SignalObservation {
    ChildExited(ExitStatus),
    IngressClosed,
    Unobserved,
}

/// Unique completion proof owned by one external verification invocation.
pub struct ProductionServeCompletion {
    output_path: PathBuf,
    verification_nonce: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ServeCompletionOutcome {
    Pending,
    Succeeded,
    Failed { error: String },
}

#[derive(Debug, Serialize, Deserialize)]
struct ServeCompletionReport {
    verification_nonce: Uuid,
    #[serde(flatten)]
    outcome: ServeCompletionOutcome,
}

impl ProductionServeCompletion {
    /// Bind the proof to an explicit external path and fresh invocation nonce.
    pub const fn new(output_path: PathBuf, verification_nonce: Uuid) -> Self {
        Self {
            output_path,
            verification_nonce,
        }
    }

    fn begin(self) -> Result<Self> {
        ensure!(
            self.output_path.is_absolute(),
            "backend completion path must be absolute"
        );
        ensure!(
            !self.verification_nonce.is_nil(),
            "backend verification nonce must not be nil"
        );
        // Refuse an existing proof instead of ever accepting a stale success.
        let reservation = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.output_path)
            .context("reserve unique backend completion path")?;
        drop(reservation);
        self.persist(ServeCompletionOutcome::Pending)?;
        Ok(self)
    }

    fn finish(&self, result: &Result<()>) -> Result<()> {
        let outcome = match result {
            Ok(()) => ServeCompletionOutcome::Succeeded,
            Err(error) => ServeCompletionOutcome::Failed {
                error: format!("{error:#}"),
            },
        };
        self.persist(outcome)
    }

    fn persist(&self, outcome: ServeCompletionOutcome) -> Result<()> {
        let report = ServeCompletionReport {
            verification_nonce: self.verification_nonce,
            outcome,
        };
        ProductionStack::persist_json_manifest(
            &self.output_path,
            &serde_json::to_vec(&report)?,
            "backend completion",
        )
    }
}

#[cfg(test)]
mod completion_tests {
    use std::{env, fs};

    use anyhow::{Result, anyhow};
    use uuid::Uuid;

    use super::{ProductionServeCompletion, ServeCompletionOutcome, ServeCompletionReport};

    #[test]
    fn completion_preserves_outcomes() -> Result<()> {
        let directory = env::temp_dir().join(format!("backend-completion-test-{}", Uuid::now_v7()));
        fs::create_dir(&directory)?;
        let path = directory.join("failed.json");
        let nonce = Uuid::now_v7();
        let target = ProductionServeCompletion::new(path.clone(), nonce).begin()?;
        let pending: ServeCompletionReport = serde_json::from_slice(&fs::read(&path)?)?;
        assert_eq!(pending.verification_nonce, nonce);
        assert!(matches!(pending.outcome, ServeCompletionOutcome::Pending));
        target.finish(&Err(anyhow!("owned cleanup failed")))?;
        let failed: ServeCompletionReport = serde_json::from_slice(&fs::read(&path)?)?;
        assert!(
            matches!(failed.outcome, ServeCompletionOutcome::Failed { error } if error == "owned cleanup failed")
        );
        assert!(
            ProductionServeCompletion::new(path, Uuid::now_v7())
                .begin()
                .is_err()
        );
        let success_path = directory.join("success.json");
        ProductionServeCompletion::new(success_path.clone(), nonce)
            .begin()?
            .finish(&Ok(()))?;
        let succeeded: ServeCompletionReport = serde_json::from_slice(&fs::read(success_path)?)?;
        assert_eq!(succeeded.verification_nonce, nonce);
        assert!(matches!(
            succeeded.outcome,
            ServeCompletionOutcome::Succeeded
        ));
        fs::remove_dir_all(directory)?;
        Ok(())
    }
}

pub async fn serve(
    listen_port: u16,
    readiness_port: Option<u16>,
    fixture: ProductionStackFixture,
    retain_artifacts: bool,
    completion: Option<ProductionServeCompletion>,
) -> Result<()> {
    let completion = completion
        .map(ProductionServeCompletion::begin)
        .transpose()?;
    let result = async {
    if listen_port == 0 {
        bail!("production-stack serve requires a non-zero --listen-port");
    }
    ensure_port_available(listen_port)?;
    if let Some(readiness_port) = readiness_port {
        ensure!(
            readiness_port != listen_port,
            "production-stack readiness port must differ from its API port"
        );
        ensure_port_available(readiness_port)?;
    }
    let workspace = Workspace::build()?;
    let running = Box::pin(ProductionStack::start_at(
        &workspace,
        listen_port,
        fixture,
        ProductionStackPurpose::BrowserEvidence,
    ))
    .await?;
    let readiness = match readiness_port {
        Some(port) => match StackReadinessServer::bind(port).await {
            Ok(readiness) => Some(readiness),
            Err(error) => {
                let cleanup =
                    Box::pin(running.stop(!retain_artifacts, ProductionShutdownSignal::Terminate))
                        .await;
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(error.context(format!(
                        "production-stack readiness bind cleanup also failed: {cleanup:#}"
                    ))),
                };
            }
        },
        None => None,
    };
    println!(
        "production stack ready: base_url={} readiness_port={readiness_port:?} artifacts={} (terminate to stop)",
        running.base_url(),
        running.run_dir().display(),
    );
    let termination = running.await_termination(readiness).await;
    let cleanup = Box::pin(running.stop_after_signal(!retain_artifacts)).await;
    match (termination, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(error), Err(cleanup)) => Err(error.context(format!(
            "production-stack serve cleanup also failed: {cleanup:#}"
        ))),
    }
    }.await;
    let proof_result = completion.map_or(Ok(()), |target| target.finish(&result));
    match (result, proof_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(proof_error)) => Err(error.context(format!(
            "backend completion proof also failed: {proof_error:#}"
        ))),
    }
}

pub async fn verify(runs: u16, retain_artifacts: bool) -> Result<()> {
    verify_fixture(runs, ProductionStackFixture::Empty, retain_artifacts).await
}

/// Run the complete governed 15-stage closure, disposable model-route commit,
/// and mixed-Route report against independently bootstrapped production stacks.
pub async fn verify_feedback_closure(runs: u16, retain_artifacts: bool) -> Result<()> {
    verify_fixture(
        runs,
        ProductionStackFixture::FeedbackClosure,
        retain_artifacts,
    )
    .await
}

async fn verify_fixture(
    runs: u16,
    fixture: ProductionStackFixture,
    retain_artifacts: bool,
) -> Result<()> {
    if runs == 0 {
        bail!("production-stack verify requires --runs greater than zero");
    }
    let workspace = Workspace::build()?;
    for run_number in 1..=runs {
        let listen_port = reserve_port()?;
        let mut running = Box::pin(ProductionStack::start_at(
            &workspace,
            listen_port,
            fixture,
            ProductionStackPurpose::Verification,
        ))
        .await
        .with_context(|| format!("start production-stack verification run {run_number}"))?;
        let run_dir = running.run_dir().to_path_buf();
        let manifest_result = running.finalize_closure_manifest().await;
        let cleanup_result =
            Box::pin(running.stop(!retain_artifacts, ProductionShutdownSignal::Terminate))
                .await
                .with_context(|| format!("stop production-stack verification run {run_number}"));
        let manifest_hash = match (manifest_result, cleanup_result) {
            (Ok(hash), Ok(())) => hash,
            (Err(error), Ok(())) => return Err(error),
            (Ok(_), Err(cleanup)) => return Err(cleanup),
            (Err(error), Err(cleanup)) => {
                return Err(error.context(format!(
                    "production-stack evidence read cleanup also failed: {cleanup:#}"
                )));
            }
        };
        println!(
            "production-stack {fixture:?} verification run {run_number}/{runs} passed artifacts={} governed_manifest_hash={}",
            if retain_artifacts {
                run_dir.display().to_string()
            } else {
                "removed".to_owned()
            },
            manifest_hash.map_or_else(|| "not-applicable".to_owned(), |hash| hash.to_string()),
        );
    }
    Ok(())
}

fn validate_closure_manifest(bytes: &[u8]) -> Result<ContentHash> {
    let manifest: GovernedClosureManifest =
        serde_json::from_slice(bytes).context("decode governed closure manifest")?;
    manifest.validate()?;
    Ok(ContentHash::from_bytes(*blake3::hash(bytes).as_bytes()))
}

impl GovernedClosureManifest {
    fn validate(&self) -> Result<()> {
        ensure!(self.format_version == 1, "unknown closure manifest format");
        self.evidence_boundary.validate()?;
        self.closure.validate_manifest()?;
        self.historical_economic_backfill.validate()?;
        ensure!(
            self.historical_economic_backfill.terminal.observed_at
                <= self.report_universe.decision_at,
            "historical economic warmup did not finish before the new report universe"
        );
        self.validate_runtime_evidence()?;
        self.validate_report_evidence()?;
        Ok(())
    }

    fn validate_runtime_evidence(&self) -> Result<()> {
        self.readiness_capture.validate()?;
        ensure!(
            self.data_plane_stability.expected_shards > 0
                && self.data_plane_stability.active_connections
                    == self.data_plane_stability.expected_shards
                && self.data_plane_stability.connection_high_water
                    <= self.data_plane_stability.concurrency_bound
                && self.data_plane_stability.accepted_connection_delta
                    <= self.data_plane_stability.allowed_turnover
                && self.data_plane_stability.forbidden_runtime_failures == 0
                && !self.pre_activation_parity.is_empty()
                && self
                    .pre_activation_parity
                    .iter()
                    .all(RuntimeParityEvidence::matched)
                && self.report_parity.matched()
                && self.disposable_model_route_commit["data"]["receipt"]["execution_authority_unchanged"]
                    == true
                && self.disposable_model_route_commit["data"]["replayed"] == false
                && self.permit.is_object(),
            "closure manifest runtime/readiness/parity evidence is incomplete"
        );
        Ok(())
    }
}

impl DisposableEvidenceBoundary {
    fn validate(&self) -> Result<()> {
        let before_total = self.money_path_before.total()?;
        let after_total = self.money_path_after.total()?;
        ensure!(
            self.evidence_scope == "owned_disposable_only"
                && self.production_composed_binary
                && !self.operational_activation_claimed
                && self.model_route_commit_scope == "disposable_fixture_only"
                && self.outbound_write_endpoints == "owned_loopback_rejectors"
                && self.runtime_control_before == self.runtime_control_after
                && self.runtime_control_after.entry_authorization_policy
                    == EntryAuthorizationPolicy::OperatorApprovalRequired
                && self.runtime_control_after.settlement_write_policy
                    == SettlementWritePolicy::Disabled
                && self.execution_authority_unchanged
                && self.money_path_before == self.money_path_after
                && before_total == after_total
                && self.real_venue_order_write_count == 0
                && self.real_chain_write_count == 0
                && self.real_capital_write_count == 0
                && self.relayer_request_count == 0,
            "closure manifest does not prove its drained disposable safety boundary"
        );
        Ok(())
    }
}

impl FeedbackClosureOutcome {
    fn validate_manifest(&self) -> Result<()> {
        ensure!(
            self.stage_evidence.len() == 15 && !self.portfolio_scenario_model_bindings.is_empty(),
            "closure manifest does not contain the complete governed DAG"
        );
        ensure!(
            self.stage_evidence.first().map(|row| row.stage) == Some(FeedbackStage::Trigger)
                && self.stage_evidence.last().map(|row| row.stage) == Some(FeedbackStage::Decision)
                && self.stage_evidence.windows(2).all(|pair| {
                    pair[0].stage.next() == Some(pair[1].stage)
                        && pair[0].event_sequence < pair[1].event_sequence
                }),
            "closure manifest stage ledger is incomplete or out of order"
        );
        Ok(())
    }
}

impl RuntimeParityEvidence {
    const fn matched(&self) -> bool {
        self.total_count > 0
            && self.total_count == self.compared_count
            && self.total_count == self.matched_count
    }
}

impl GovernedClosureManifest {
    fn validate_report_evidence(&self) -> Result<()> {
        let manifest = self;
        let universe = &manifest.report_universe;
        let expected_markets = universe.market_ids.iter().cloned().collect::<HashSet<_>>();
        let route_counts =
            universe
                .routes_by_market
                .values()
                .fold(BTreeMap::new(), |mut counts, route| {
                    *counts.entry(*route).or_insert(0_usize) += 1;
                    counts
                });
        ensure!(
            universe.market_ids.len() == 10
                && expected_markets.len() == 10
                && universe.routes_by_market.len() == 10
                && universe
                    .routes_by_market
                    .keys()
                    .all(|market_id| expected_markets.contains(market_id))
                && route_counts.get(&BuyModelRoute::Crypto) == Some(&5)
                && route_counts.get(&BuyModelRoute::Weather) == Some(&5)
                && route_counts.len() == 2,
            "closure report universe is not the exact 5+5 mixed-Route set"
        );
        let report_id = manifest.report["run"]["data"]["output_report_id"]
            .as_str()
            .context("closure manifest report omitted output_report_id")?
            .parse::<RecommendationReportId>()?;
        let recommendations = manifest.report["recommendations"]["data"]
            .as_array()
            .context("closure manifest recommendations.data is not an array")?;
        let mut recommendation_ids = HashSet::new();
        let mut recommendation_markets = HashSet::new();
        for recommendation in recommendations {
            let recommendation_id = recommendation["recommendation_id"]
                .as_str()
                .context("closure recommendation omitted recommendation_id")?
                .parse::<RecommendationId>()?;
            let market_id = MarketId::new(
                recommendation["market_id"]
                    .as_str()
                    .context("closure recommendation omitted market_id")?,
            );
            let route = recommendation["route"]
                .as_str()
                .context("closure recommendation omitted Route")?;
            ensure!(
                recommendation_ids.insert(recommendation_id)
                    && recommendation_markets.insert(market_id.clone())
                    && universe
                        .routes_by_market
                        .get(&market_id)
                        .is_some_and(|expected| expected.as_str() == route),
                "closure recommendation identity or Route mapping drifted"
            );
        }
        ensure!(
            recommendations.len() == 10
                && recommendation_markets == expected_markets
                && manifest.report["funnel"]["data"]["conserved"] == true
                && manifest.report["feature_nulls"]
                    .as_array()
                    .is_some_and(Vec::is_empty)
                && manifest.report_parity.report_id == Some(report_id),
            "closure report is incomplete or failed its exact funnel/parity contract"
        );
        validate_forward_evidence(manifest, report_id, &recommendation_ids, &expected_markets)
    }
}

fn validate_forward_evidence(
    manifest: &GovernedClosureManifest,
    report_id: RecommendationReportId,
    recommendation_ids: &HashSet<RecommendationId>,
    expected_markets: &HashSet<MarketId>,
) -> Result<()> {
    let resolution = &manifest.resolution_plane;
    let resolved_markets = resolution
        .facts
        .iter()
        .map(|fact| fact.market_id.clone())
        .collect::<HashSet<_>>();
    let successor_ids = manifest
        .successor_feedback
        .route_cohorts
        .iter()
        .flat_map(|cohort| cohort.recommendation_ids.iter().copied())
        .collect::<HashSet<_>>();
    let resolution_forward = resolution.report_decision_at < resolution.observed_at;
    let resolution_ordered = resolution.resolved_at <= resolution.observed_at;
    ensure!(
        resolution.report_id == report_id
            && resolution.facts.len() == 10
            && &resolved_markets == expected_markets
            && resolution.report_decision_at >= manifest.report_universe.decision_at
            && resolution_forward
            && resolution_ordered
            && manifest.successor_feedback.parent_cycle_id == manifest.closure.feedback_cycle_id
            && manifest.successor_feedback.decision_window_start
                <= manifest.successor_feedback.decision_cutoff
            && manifest.successor_feedback.decision_cutoff
                <= manifest.successor_feedback.truth_cutoff
            && manifest.successor_feedback.route_cohorts.len() == 2
            && &successor_ids == recommendation_ids
            && manifest
                .successor_feedback
                .route_cohorts
                .iter()
                .all(|cohort| {
                    let count = u32::try_from(cohort.recommendation_ids.len()).ok();
                    count == Some(cohort.model_learning_eligible_count)
                        && count == Some(cohort.economic_outcome_count)
                        && cohort.economic_outcomes.len() == cohort.recommendation_ids.len()
                        && cohort
                            .economic_outcomes
                            .iter()
                            .map(|economic| economic.recommendation_id)
                            .collect::<HashSet<_>>()
                            == cohort
                                .recommendation_ids
                                .iter()
                                .copied()
                                .collect::<HashSet<_>>()
                        && cohort.economic_outcomes.iter().all(|economic| {
                            let evidence = &economic.payload_json.evidence;
                            let amounts = &economic.payload_json.amounts;
                            economic.verify().is_ok()
                                && economic.recommendation_report_id == report_id
                                && economic.report_route_run_id == cohort.report_route_run_id
                                && economic.model_version_id == cohort.model_version_id
                                && economic.research_profile_artifact_id
                                    == cohort.profile_ref.artifact_id()
                                && economic.decision_at == resolution.report_decision_at
                                && economic.available_at <= manifest.successor_feedback.truth_cutoff
                                && economic.source_available_until <= economic.available_at
                                && economic.payload_json.detail.terminal_at().is_some_and(
                                    |terminal| {
                                        terminal > economic.decision_at
                                            && terminal < economic.horizon_at
                                            && terminal <= economic.source_available_until
                                    },
                                )
                                && matches!(
                                    economic.payload_json.detail,
                                    RecommendationEconomicStateDetail::ResolvedBeforeHorizon {
                                        entered_at: Some(_),
                                        ..
                                    } | RecommendationEconomicStateDetail::PolicyExited { .. }
                                )
                                && amounts.entry_filled_shares > Shares::ZERO
                                && amounts.entry_filled_shares == amounts.exited_shares
                                && amounts.entry_cost_usd > Usd::ZERO
                                && amounts.net_pnl_usd.is_some()
                                && amounts.net_return_bps.is_some()
                                && evidence.full_l2_covered
                                && evidence.fee_covered
                                && evidence.passive_trade_covered != Some(false)
                                && economic.replay_kernel_version == POLICY_REPLAY_KERNEL_VERSION
                        })
                        && count == Some(cohort.policy_evaluation_eligible_count)
                        && count == Some(cohort.execution_learning_censored_count)
                        && cohort.resolution_outcome_hashes.len() == cohort.recommendation_ids.len()
                        && cohort.execution_censor_reason
                            == CohortCensorReason::ExecutionOutcomeUnavailableAtCutoff
                }),
        "closure resolution or N-to-N+1 feedback evidence is incomplete"
    );
    Ok(())
}

impl ProductionStack {
    pub async fn start(fixture: ProductionStackFixture) -> Result<Self> {
        let workspace = Workspace::build()?;
        let listen_port = reserve_port()?;
        Box::pin(Self::start_at(
            &workspace,
            listen_port,
            fixture,
            ProductionStackPurpose::Verification,
        ))
        .await
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.launch.base_url
    }

    #[must_use]
    pub fn run_dir(&self) -> &Path {
        &self.launch.run_dir
    }

    /// Cycle held by a distinct live worker for deterministic governed
    /// cancellation evidence in the `GovernedFeedback` fixture.
    #[must_use]
    pub fn governed_cancellation_cycle_id(&self) -> Option<FeedbackCycleId> {
        self.cancellation_owner
            .as_ref()
            .map(FixtureCancellationOwner::cycle_id)
    }

    /// Cycle driven only by production coordinator stages in the closure fixture.
    #[must_use]
    pub const fn closure_cycle_id(&self) -> Option<FeedbackCycleId> {
        self.closure_cycle_id
    }

    /// Gracefully restart only the real binary while preserving every owned
    /// persistence service, the rendered config, port, and artifact directory.
    pub async fn restart(&self) -> Result<()> {
        let child = Arc::clone(&self.child.child);
        // Hold the sole child lock across the intentional exit and replacement;
        // liveness observers must never mistake that transition for a fail-stop.
        let mut child = child.lock().await;
        self.shutdown_child(
            &mut child,
            ShutdownOrigin::Harness(ProductionShutdownSignal::Terminate),
        )
        .await?;
        *child = self.launch.spawn().await?;
        drop(child);
        Ok(())
    }

    /// Abruptly terminate the real binary without releasing its durable
    /// coordinator lease, then start the same binary against the same stores.
    async fn crash_restart(&self) -> Result<()> {
        let child = Arc::clone(&self.child.child);
        // The crash is deliberate evidence, so publish the replacement under
        // the same lock that owns kill/wait and expose no dead-child window.
        let mut child = child.lock().await;
        child
            .start_kill()
            .context("kill production binary at lease-recovery fault point")?;
        let status = timeout(SHUTDOWN_TIMEOUT, child.wait())
            .await
            .context("time out waiting for crashed production binary")?
            .context("wait for crashed production binary")?;
        ensure!(
            !status.success(),
            "production binary unexpectedly exited successfully at crash fault point"
        );
        *child = self.launch.spawn().await?;
        drop(child);
        Ok(())
    }

    /// Stop the owned Redis service while `verify_outage` exercises the real
    /// binary, then restore the same fixed endpoint and wait for the original
    /// shared pool to recover. Redis restoration is attempted even when the
    /// outage assertion fails.
    pub async fn with_redis_outage<F, Fut>(&self, verify_outage: F) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        self.infrastructure
            .redis_container
            .stop_with_timeout(Some(0))
            .await
            .context("stop production-stack Redis")?;
        let outage_result = verify_outage().await;
        let recovery_result = self.restore_redis().await;
        match (outage_result, recovery_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(outage), Ok(())) => Err(outage.context("verify production Redis outage")),
            (Ok(()), Err(recovery)) => Err(recovery),
            (Err(outage), Err(recovery)) => bail!(
                "production Redis outage assertion failed: {outage:#}; Redis recovery also failed: {recovery:#}"
            ),
        }
    }

    pub async fn stop(
        self,
        remove_artifacts: bool,
        signal: ProductionShutdownSignal,
    ) -> Result<()> {
        Box::pin(self.shutdown(remove_artifacts, ShutdownOrigin::Harness(signal))).await
    }

    /// Prove that the production pool replaces live connections while HTTP
    /// requests continue, before its worker runtime is torn down by shutdown.
    pub async fn verify_pool_recycling(&self) -> Result<()> {
        let initial = self.runtime_connection_ids().await?;
        ensure!(
            !initial.is_empty(),
            "production pool has no initial sessions"
        );
        let config_path = self.launch.run_dir.join("quant-pivot.toml");
        let request =
            DeployConfigLoadRequest::new(config_path, DeploymentEnvironment::local_development());
        let deploy = DeployConfig::load(&request)?;
        let postgres = &deploy.db.postgres;
        ensure!(
            postgres.max_connections == u32::try_from(CLOSURE_POSTGRES_MAX_CONNECTIONS)?
                && postgres.max_lifetime_secs == CLOSURE_POSTGRES_MAX_LIFETIME_SECS
                && postgres.min_connections < postgres.max_connections
                && u32::try_from(initial.len())? < postgres.max_connections,
            "production pool must retain its burst ceiling and recycling contract without prewarming every connection"
        );
        let deadline = Instant::now()
            + Duration::from_secs(CLOSURE_POSTGRES_MAX_LIFETIME_SECS.saturating_mul(3));
        let client = Client::builder()
            .timeout(READINESS_REQUEST_TIMEOUT)
            .build()?;
        loop {
            let response = client
                .get(format!("{}/ready", self.base_url()))
                .send()
                .await?;
            ensure!(
                response.status().is_success(),
                "production readiness failed during pool recycling: {}",
                response.status()
            );
            let current = self.runtime_connection_ids().await?;
            if !initial.is_subset(&current) && !current.is_subset(&initial) {
                println!(
                    "production-stack pool recycling passed: initial={} current={} replaced={}",
                    initial.len(),
                    current.len(),
                    initial.difference(&current).count(),
                );
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "production pool did not recycle connections within its bounded lifetime"
            );
            sleep(POLL_INTERVAL).await;
        }
    }

    async fn runtime_connection_ids(&self) -> Result<HashSet<i32>> {
        self.infrastructure
            .postgres
            .connection()
            .query_all_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT pid FROM pg_stat_activity WHERE application_name = 'quant-pivot-production-stack' AND datname = current_database()",
            ))
            .await?
            .into_iter()
            .map(|row| row.try_get("", "pid").map_err(Into::into))
            .collect()
    }

    async fn restore_redis(&self) -> Result<()> {
        self.infrastructure
            .redis_container
            .start()
            .await
            .context("restart production-stack Redis")?;
        let restarted_port = self
            .infrastructure
            .redis_container
            .get_host_port_ipv4(6379)
            .await
            .context("resolve production-stack Redis port after restart")?;
        ensure!(
            restarted_port == self.infrastructure.redis_config.port,
            "production-stack Redis endpoint changed across restart: expected {}, got {restarted_port}",
            self.infrastructure.redis_config.port,
        );
        ensure!(
            self.infrastructure
                .redis_container
                .is_running()
                .await
                .context("inspect production-stack Redis after restart")?,
            "production-stack Redis exited immediately after restart"
        );

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match self.infrastructure.redis.health_check().await {
                Ok(()) => return Ok(()),
                Err(error) if Instant::now() >= deadline => {
                    bail!("production-stack Redis pool did not recover: {error}")
                }
                Err(_) => sleep(POLL_INTERVAL).await,
            }
        }
    }

    async fn stop_after_signal(self, remove_artifacts: bool) -> Result<()> {
        Box::pin(self.shutdown(remove_artifacts, ShutdownOrigin::ProcessTreeSignal)).await
    }

    async fn shutdown(mut self, remove_artifacts: bool, origin: ShutdownOrigin) -> Result<()> {
        let monitor_result = self.finish_browser_closure_monitor().await;
        let shutdown_result = self.shutdown_binary(origin).await;
        let cancellation_owner_result = match self.cancellation_owner.take() {
            Some(owner) => owner.shutdown().await,
            None => Ok(()),
        };
        let runtime_health_result = self.verify_runtime_log_health();
        let clob_upstream_result = self.clob_upstream.shutdown().await;
        let artifact_infrastructure_result = match self.artifact_infrastructure {
            Some(infrastructure) => infrastructure.shutdown().await,
            None => Ok(()),
        };
        let infrastructure_result = Box::pin(self.infrastructure.shutdown())
            .await
            .context("remove disposable production-stack infrastructure");
        let run_dir_result = if remove_artifacts
            && monitor_result.is_ok()
            && shutdown_result.is_ok()
            && cancellation_owner_result.is_ok()
            && runtime_health_result.is_ok()
            && clob_upstream_result.is_ok()
            && artifact_infrastructure_result.is_ok()
            && infrastructure_result.is_ok()
        {
            fs::remove_dir_all(&self.launch.run_dir)
                .with_context(|| format!("remove successful run {}", self.launch.run_dir.display()))
        } else {
            Ok(())
        };

        monitor_result?;
        shutdown_result?;
        cancellation_owner_result?;
        runtime_health_result?;
        clob_upstream_result?;
        artifact_infrastructure_result?;
        infrastructure_result?;
        run_dir_result
    }

    async fn finish_browser_closure_monitor(&mut self) -> Result<()> {
        let Some(handle) = self.browser_closure_monitor.take() else {
            return Ok(());
        };
        if !handle.is_finished() {
            handle.abort();
        }
        match handle.await {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(error).context("join browser N-to-N+1 closure monitor"),
        }
    }

    async fn shutdown_binary(&self, origin: ShutdownOrigin) -> Result<()> {
        let child = Arc::clone(&self.child.child);
        let mut child = child.lock().await;
        self.shutdown_child(&mut child, origin).await
    }

    async fn shutdown_child(&self, child: &mut Child, origin: ShutdownOrigin) -> Result<()> {
        if let Some(status) = child.try_wait().context("inspect production binary")? {
            return self.child.verify_exit(status);
        }

        if origin == ShutdownOrigin::ProcessTreeSignal {
            match self.observe_signal(child).await? {
                SignalObservation::ChildExited(status) => return self.child.verify_exit(status),
                SignalObservation::IngressClosed => return self.wait_for_exit(child).await,
                SignalObservation::Unobserved => {}
            }
        }

        let signal = match origin {
            ShutdownOrigin::Harness(signal) => signal,
            ShutdownOrigin::ProcessTreeSignal => ProductionShutdownSignal::Terminate,
        };
        self.signal_binary(child, signal)?;
        self.wait_for_exit(child).await?;
        if signal == ProductionShutdownSignal::Interrupt {
            let log = fs::read_to_string(self.launch.log_path())?;
            ensure!(
                log.contains("Received SIGINT — initiating graceful shutdown")
                    && !log.contains("actix_server::server: SIGINT received")
                    && log.contains("shared PostgreSQL pool closed"),
                "SIGINT must be owned by core and drain shared PostgreSQL before exit"
            );
        }
        Ok(())
    }

    async fn observe_signal(&self, child: &mut Child) -> Result<SignalObservation> {
        let deadline = Instant::now() + SIGNAL_PROPAGATION_TIMEOUT;
        let address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.listen_port);
        loop {
            if let Some(status) = child
                .try_wait()
                .context("inspect externally signalled production binary")?
            {
                return Ok(SignalObservation::ChildExited(status));
            }
            if matches!(
                timeout(POLL_INTERVAL, TcpStream::connect(address)).await,
                Ok(Err(_))
            ) {
                return Ok(SignalObservation::IngressClosed);
            }
            if Instant::now() >= deadline {
                return Ok(SignalObservation::Unobserved);
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    fn signal_binary(&self, child: &Child, signal: ProductionShutdownSignal) -> Result<()> {
        let process_id = child.id().context("production binary has no process id")?;
        let terminate = Command::new("kill")
            .args([signal.kill_argument(), &process_id.to_string()])
            .status()
            .with_context(|| format!("send {signal:?} to production binary"))?;
        if !terminate.success() {
            bail!(
                "could not signal production binary {process_id}; logs={}",
                self.launch.log_path().display(),
            );
        }
        Ok(())
    }

    async fn wait_for_exit(&self, child: &mut Child) -> Result<()> {
        if let Ok(status) = timeout(SHUTDOWN_TIMEOUT, child.wait()).await {
            let status = status.context("wait for graceful production shutdown")?;
            self.child.verify_exit(status)
        } else {
            child
                .start_kill()
                .context("force-stop unresponsive production binary")?;
            let _ = child.wait().await;
            bail!(
                "production binary exceeded the {SHUTDOWN_TIMEOUT:?} shutdown budget; logs={}; tail={}",
                self.launch.log_path().display(),
                backend_log_tail(&self.launch.log_path()),
            );
        }
    }
}

impl ProductionStack {
    async fn await_termination(&self, readiness: Option<StackReadinessServer>) -> Result<()> {
        let child = self.child.clone();
        let readiness = async move {
            match readiness {
                Some(server) => server.serve().await,
                None => pending::<Result<()>>().await,
            }
        };
        tokio::pin!(readiness);
        tokio::select! {
            biased;
            error = child.unexpected_exit("fixture termination wait") => Err(error),
            signal = termination_signal() => signal,
            result = &mut readiness => result,
        }
    }

    async fn start_at(
        workspace: &Workspace,
        listen_port: u16,
        fixture: ProductionStackFixture,
        purpose: ProductionStackPurpose,
    ) -> Result<Self> {
        let report_resolves_at = Utc::now()
            .checked_add_signed(ChronoDuration::hours(CLOSURE_REPORT_HORIZON_HOURS))
            .context("production-stack report horizon exceeds the UTC clock")?;
        let report_resolves_at =
            DateTime::from_timestamp_millis(report_resolves_at.timestamp_millis())
                .context("production-stack report horizon is outside the millisecond wire range")?;
        let polygon = Arc::new(DeterministicPolygonChain::new());
        let history_evidence = fixture_history_evidence(fixture, polygon.as_ref())?;
        let upstream = fixture
            .deterministic_upstream(report_resolves_at, Arc::clone(&polygon))
            .await?;
        let history_upstreams = if fixture.history_enabled() {
            Some(HistoryUpstreams::start(Arc::clone(&polygon)).await?)
        } else {
            None
        };
        // Every subscribed token is refreshed on each ten-second pulse, which
        // stays below the production 15-second book-age ceiling. Real
        // performance load uses its own high-rate upstream and is unchanged.
        let clob_upstream = DeterministicClobServer::start_keepalive(Duration::from_secs(
            FixtureBookTiming::FEED_PERIOD_SECS,
        ))
        .await
        .context("start deterministic production-stack CLOB transport")?;
        let infrastructure = Box::pin(SystemStack::start())
            .await
            .context("start disposable production-stack infrastructure")?;
        let mut startup = ProductionStartup::new(infrastructure);
        let started = startup
            .launch(ProductionLaunchInput {
                workspace,
                listen_port,
                fixture,
                report_resolves_at,
                history_evidence,
                polygon: &polygon,
                upstream: &upstream,
                history_upstreams: history_upstreams.as_ref(),
                clob_upstream: &clob_upstream,
            })
            .await;
        let mut started = match started {
            Ok(started) => started,
            Err(error) => return Box::pin(startup.abort(error)).await,
        };
        let (artifact_infrastructure, infrastructure) = startup.finish()?;
        let cancellation_owner = started
            .browser_evidence
            .as_mut()
            .and_then(|evidence| evidence.cancellation_claim.take())
            .map(|claim| {
                FixtureCancellationOwner::start(
                    infrastructure.postgres.connection().clone(),
                    claim,
                    GOVERNED_CANCELLATION_LEASE_SECS,
                )
            });
        let closure_cycle_id = started.browser_evidence.as_ref().and_then(|evidence| {
            evidence
                .closure
                .as_ref()
                .map(|closure| closure.feedback_cycle_id)
        });
        let child = BackendChild::new(started.child, started.launch.log_path());
        let running = Self {
            browser_closure_monitor: None,
            cancellation_owner,
            child,
            clob_accepted_baseline: 0,
            closure_cycle_id,
            fixture,
            launch: started.launch,
            listen_port,
            upstream,
            history_upstreams,
            clob_upstream,
            artifact_infrastructure,
            infrastructure,
            pending_closure_manifest: None,
        };
        Box::pin(running.into_ready_or_shutdown(started.browser_evidence.as_ref(), purpose)).await
    }

    async fn into_ready_or_shutdown(
        mut self,
        evidence: Option<&BrowserFixtureEvidence>,
        purpose: ProductionStackPurpose,
    ) -> Result<Self> {
        let child = self.child.clone();
        let readiness = child
            .supervise(
                "production-stack readiness",
                Box::pin(self.await_readiness(evidence, purpose)),
            )
            .await;
        if let Err(error) = readiness {
            let cleanup = Box::pin(self.shutdown(
                false,
                ShutdownOrigin::Harness(ProductionShutdownSignal::Terminate),
            ))
            .await
            .context("clean production stack after readiness failure");
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "production-stack readiness cleanup also failed: {cleanup:#}"
                ))),
            };
        }
        Ok(self)
    }

    async fn await_readiness(
        &mut self,
        evidence: Option<&BrowserFixtureEvidence>,
        purpose: ProductionStackPurpose,
    ) -> Result<()> {
        self.await_operational_readiness().await?;
        self.clob_accepted_baseline = self.clob_upstream.accepted_connection_count();
        if self.fixture.history_enabled() {
            self.await_history_head().await?;
            self.await_catalog_settle().await?;
        }
        let Some(evidence) = evidence else {
            return Ok(());
        };
        let mut browser_closure = None;
        if let Some(closure) = evidence.closure.as_ref() {
            let boundary_preimage = if purpose == ProductionStackPurpose::Verification {
                Some(self.boundary_preimage().await?)
            } else {
                None
            };
            self.verify_governed_trigger(closure).await?;
            if self.fixture == ProductionStackFixture::FeedbackClosureRecovery {
                let job_id = await_recovery_point(
                    self.infrastructure.postgres.connection(),
                    closure.feedback_cycle_id,
                )
                .await?;
                self.crash_restart().await?;
                await_lease_recovery(
                    self.infrastructure.postgres.connection(),
                    closure.feedback_cycle_id,
                    job_id,
                )
                .await?;
                println!(
                    "feedback closure lease recovery: cycle_id={} stage={} job_id={}",
                    closure.feedback_cycle_id,
                    FeedbackStage::Attribution,
                    job_id,
                );
            }
            let (outcome, pre_activation_parity, historical_economic_backfill) =
                self.prepare_feedback_closure(closure).await?;
            match purpose {
                ProductionStackPurpose::BrowserEvidence => {
                    self.quiesce_report_ingest().await?;
                    let report_universe = self.prepare_report_ingress(closure).await?;
                    self.persist_candidate_manifest(
                        &outcome,
                        &report_universe,
                        &historical_economic_backfill,
                    )?;
                    if self.fixture == ProductionStackFixture::FeedbackClosure {
                        browser_closure = Some((closure.clone(), outcome, report_universe));
                    }
                }
                ProductionStackPurpose::Verification => {
                    self.quiesce_report_ingest().await?;
                    let (permit, route_commit) = self.commit_disposable_candidate(&outcome).await?;
                    let report_universe = self.prepare_report_ingress(closure).await?;
                    let refresh = self.clob_upstream.refresh_handle();
                    Self::install_report_books(
                        self.infrastructure.postgres.connection(),
                        closure,
                        &refresh,
                        report_universe.knowledge_lag_secs,
                    )
                    .await?;
                    closure
                        .refresh_report_microstructure(
                            self.infrastructure.postgres.connection(),
                            report_universe.knowledge_lag_secs,
                        )
                        .await?;
                    let report = self.run_feedback_report(&outcome, &report_universe).await?;
                    let report_id = report["run"]["data"]["output_report_id"]
                        .as_str()
                        .context("feedback closure report omitted output_report_id")?
                        .parse::<RecommendationReportId>()?;
                    let report_parity =
                        await_report_parity(self.infrastructure.postgres.connection(), &report_id)
                            .await?;
                    let resolution_plane = settle_feedback_report_universe(
                        self.infrastructure.postgres.connection(),
                        closure,
                        &report_universe,
                        report_id,
                    )
                    .await?;
                    let successor_feedback = Self::verify_successor_feedback(
                        self.infrastructure.postgres.connection(),
                        &outcome,
                        &report,
                        &resolution_plane,
                    )
                    .await?;
                    let data_plane_stability = self.verify_data_plane_stability().await?;
                    let readiness_capture = self.verify_readiness_capture().await?;
                    let (http, access_token) = self.governed_http_session().await?;
                    let runtime_control_before_drain =
                        self.read_runtime_control(&http, &access_token).await?;
                    self.pending_closure_manifest = Some(Box::new(PendingClosureManifest {
                        preimage: boundary_preimage
                            .context("verification closure has no safety preimage")?,
                        runtime_control_before_drain,
                        closure: outcome,
                        data_plane_stability,
                        readiness_capture,
                        pre_activation_parity,
                        historical_economic_backfill,
                        permit,
                        disposable_model_route_commit: route_commit,
                        report_universe,
                        report,
                        report_parity,
                        resolution_plane,
                        successor_feedback,
                    }));
                }
            }
        }
        // The real research worker consumes the report's mandatory sampled
        // parity job. The deterministic browser profile has no serving facts,
        // so wait for fail-closed containment before exposing stable evidence.
        if let Some(report_id) = evidence.sampled_parity_report_id.as_ref() {
            await_sampled_parity_containment(self.infrastructure.postgres.connection(), report_id)
                .await?;
        }
        if evidence.await_settlement_discovery {
            await_browser_settlement_discovery(self.infrastructure.postgres.connection()).await?;
        }
        self.stabilize_browser_baseline(purpose).await?;
        if let Some((fixture, outcome, report_universe)) = browser_closure {
            self.start_browser_closure_monitor(fixture, outcome, report_universe)?;
        }
        let _stability = self.verify_data_plane_stability().await?;
        Ok(())
    }

    async fn prepare_feedback_closure(
        &self,
        closure: &FeedbackClosureFixture,
    ) -> Result<(
        FeedbackClosureOutcome,
        Vec<RuntimeParityEvidence>,
        HistoricalEconomicBackfill,
    )> {
        let parity_target =
            RuntimeParityTarget::freeze(self.infrastructure.postgres.connection()).await?;
        let artifact_store = self
            .artifact_infrastructure
            .as_ref()
            .context("feedback closure fixture has no artifact infrastructure")?
            .store()?;
        let outcome = Box::pin(complete_feedback_closure(
            self.infrastructure.postgres.connection(),
            &artifact_store,
            closure,
        ))
        .await?;
        let parity = parity_target
            .await_completion(self.infrastructure.postgres.connection())
            .await?;
        let historical = self.await_historical_economics().await?;
        Ok((outcome, parity, historical))
    }

    async fn await_historical_economics(&self) -> Result<HistoricalEconomicBackfill> {
        let started = Instant::now();
        let deadline = started + HISTORICAL_ECONOMIC_TIMEOUT;
        let db = self.infrastructure.postgres.connection();
        let target = timeout(
            HISTORICAL_ECONOMIC_READ_TIMEOUT,
            HistoricalEconomicTarget::freeze(db),
        )
        .await
        .context("historical economic target read exceeded its bounded deadline")??;
        let observation = timeout(HISTORICAL_ECONOMIC_READ_TIMEOUT, target.progress(db))
            .await
            .context("historical economic initial read exceeded its bounded deadline")??;
        let initial = observation.progress;
        let expected = u64::try_from(target.ids.len())?;
        let mut current = initial;
        let mut liveness = HistoricalEconomicLiveness::new(started, observation)?;
        println!(
            "historical economic warmup: target_count={expected} target_cutoff={} target_hash={} initial={:?}",
            target.cutoff, target.hash, initial
        );
        loop {
            ensure!(
                Instant::now() < deadline,
                "historical economic warmup exceeded {HISTORICAL_ECONOMIC_TIMEOUT:?}: target_count={expected} target_cutoff={} target_hash={} elapsed={:?} initial={:?} latest={:?}",
                target.cutoff,
                target.hash,
                started.elapsed(),
                initial.counts,
                current.counts
            );
            if current.counts.drained(expected) {
                let read_deadline = deadline.min(Instant::now() + HISTORICAL_ECONOMIC_READ_TIMEOUT);
                let outcomes = match tokio::time::timeout_at(
                    read_deadline,
                    target.verify_outcomes(db, current.observed_at),
                )
                .await
                {
                    Ok(result) => result?,
                    Err(_) => {
                        return Err(liveness.read_timeout(Instant::now()))
                            .context("historical economic final WORM verification failed");
                    }
                };
                let outcome_set_hash = HistoricalEconomicBackfill::receipt_hash(&outcomes)?;
                let evidence = HistoricalEconomicBackfill {
                    target_cutoff: target.cutoff,
                    recommendation_ids: target.ids,
                    target_hash: target.hash,
                    initial,
                    terminal: current,
                    elapsed_ms: u64::try_from(started.elapsed().as_millis())?,
                    outcomes,
                    outcome_set_hash,
                };
                evidence.validate()?;
                println!(
                    "historical economic warmup completed: target_count={expected} elapsed_ms={} outcome_set_hash={} terminal={:?}",
                    evidence.elapsed_ms, evidence.outcome_set_hash, evidence.terminal.counts
                );
                return Ok(evidence);
            }
            liveness.check(Instant::now()).with_context(|| {
                format!(
                    "historical economic liveness failed: target_hash={} latest={current:?}",
                    target.hash
                )
            })?;
            tokio::time::sleep_until(
                (Instant::now() + HISTORICAL_ECONOMIC_POLL).min(liveness.deadline()),
            )
            .await;
            let read_deadline = liveness.read_deadline(Instant::now()).with_context(|| {
                format!("historical economic liveness failed before read: target_hash={} latest={current:?}", target.hash)
            })?;
            let observation = match tokio::time::timeout_at(read_deadline, target.progress(db))
                .await
            {
                Ok(result) => result?,
                Err(_) => return Err(liveness.read_timeout(Instant::now())).with_context(|| {
                    format!(
                        "historical economic observation failed: target_hash={} latest={current:?}",
                        target.hash
                    )
                }),
            };
            current = observation.progress;
            if liveness.observe(observation)? {
                println!(
                    "historical economic warmup progress: target_count={expected} elapsed_ms={} latest={:?}",
                    started.elapsed().as_millis(),
                    current
                );
            }
        }
    }

    async fn stabilize_browser_baseline(&self, purpose: ProductionStackPurpose) -> Result<()> {
        if matches!(
            self.fixture,
            ProductionStackFixture::Browser | ProductionStackFixture::GovernedFeedback
        ) {
            if purpose == ProductionStackPurpose::BrowserEvidence {
                self.stabilize_browser_activity().await?;
            }
            // Containment releases the pending intent's reservation. Persist
            // that current state before the UI reads the equity ledger; the
            // unchanged periodic worker will reproduce the same financials.
            let db = self.infrastructure.postgres.connection();
            let mut current = self
                .fixture
                .browser_equity(db, BrowserAccountStage::SettledHolding)
                .await?;
            ensure!(
                current.equity_snapshot.reserved_usd == Usd::ZERO,
                "browser readiness left capital reserved"
            );
            ensure!(
                current.equity_snapshot.high_water_mark_usd
                    == current.equity_snapshot.capital_base_usd
                    && current.equity_snapshot.drawdown_pct == Decimal::ZERO,
                "browser historical equity polluted the current account high-water mark"
            );
            current.equity_snapshot.account_snapshot_ref = None;
            PgEquitySnapshotRepository::new(db.clone())
                .create(current.equity_snapshot)
                .await?;
        }
        Ok(())
    }

    async fn await_history_head(&self) -> Result<()> {
        let repository =
            PgExchangeHistoryRepository::new(self.infrastructure.postgres.connection().clone());
        let minimum_block = self
            .history_upstreams
            .as_ref()
            .context("history-enabled fixture lost its local providers")?
            .minimum_serving_block;
        let deadline = Instant::now() + HISTORY_READINESS_TIMEOUT;
        loop {
            if let Some(head) = repository
                .latest_serving_head(ExchangeHistoryFrontier::Activation)
                .await?
                && head.seal.accepted_through_block >= minimum_block
            {
                repository
                    .validate_serving_head(head.seal.serving_head_seal_id, head.seal.seal_hash)
                    .await?;
                ensure!(
                    !head.chunks.is_empty(),
                    "production history serving head has no accepted chunks"
                );
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "production history serving head did not reach block {minimum_block} within {HISTORY_READINESS_TIMEOUT:?}"
            );
            sleep(POLL_INTERVAL).await;
        }
    }

    async fn await_catalog_settle(&self) -> Result<()> {
        let deadline = Instant::now() + CATALOG_SETTLE_TIMEOUT;
        loop {
            let row = self
                .infrastructure
                .postgres
                .connection()
                .query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "SELECT (SELECT COUNT(*) FROM catalog_sync_batch \
                             WHERE status = 'committed'::qp_catalog_sync_status \
                             AND committed_at >= $1)::bigint AS batches, \
                            (SELECT COUNT(*) FROM quant_market_linkage \
                             WHERE created_at >= $1)::bigint AS linkages",
                    [self.launch.catalog_runtime_started_at.into()],
                ))
                .await
                .context("read closure catalog-settle evidence")?
                .context("closure catalog-settle query returned no row")?;
            let batches = row.try_get::<i64>("", "batches")?;
            let linkages = row.try_get::<i64>("", "linkages")?;
            if batches >= 1 && linkages > 0 {
                self.verify_runtime_log_health()
                    .context("validate runtime log after catalog settle")?;
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "production catalog did not complete a runtime reconciliation and linkage projection within {CATALOG_SETTLE_TIMEOUT:?}: batches={batches} linkages={linkages}"
            );
            sleep(POLL_INTERVAL).await;
        }
    }

    async fn await_report_history(&self, universe: &FeedbackReportUniverse) -> Result<()> {
        let repository =
            PgExchangeHistoryRepository::new(self.infrastructure.postgres.connection().clone());
        let pool = Arc::new(
            ClickHousePool::connect(&self.infrastructure.clickhouse_config)
                .await
                .context("connect report history verifier")?,
        );
        let facts = ChQuantFactReadRepository::new(pool);
        let deadline = Instant::now() + HISTORY_READINESS_TIMEOUT;
        loop {
            let decision_at = self
                .infrastructure
                .postgres
                .connection()
                .statement_time()
                .await;
            if let Some(head) = repository
                .serving_head_at(ExchangeHistoryFrontier::Activation, decision_at)
                .await?
            {
                let head = repository
                    .validate_serving_head(head.seal.serving_head_seal_id, head.seal.seal_hash)
                    .await?;
                let rows = facts
                    .market_execution_window(
                        universe.market_ids.clone(),
                        head.chunks.clone(),
                        (decision_at - ChronoDuration::hours(24)).timestamp_millis(),
                        head.seal.effective_through_at.timestamp_millis(),
                        decision_at.timestamp_millis(),
                    )
                    .await?;
                let mut executions = HashMap::<MarketId, HashSet<ContentHash>>::new();
                let mut freshest = HashMap::<MarketId, i64>::new();
                for row in rows {
                    executions
                        .entry(row.market_id.clone())
                        .or_default()
                        .insert(ContentHash::from_bytes(row.execution_id.into_bytes()));
                    freshest
                        .entry(row.market_id)
                        .and_modify(|current| *current = (*current).max(row.effective_at))
                        .or_insert(row.effective_at);
                }
                let ready = universe.market_ids.iter().all(|market_id| {
                    let fresh_enough = freshest.get(market_id).is_some_and(|effective_at| {
                        decision_at.timestamp_millis().saturating_sub(*effective_at)
                            <= i64::try_from(REPORT_EXECUTION_MAX_AGE.as_millis())
                                .unwrap_or(i64::MAX)
                    });
                    executions.get(market_id).is_some_and(|ids| ids.len() >= 3) && fresh_enough
                });
                if ready {
                    return Ok(());
                }
            }
            ensure!(
                Instant::now() < deadline,
                "production history did not expose three fresh finalized executions for every report market within {HISTORY_READINESS_TIMEOUT:?}"
            );
            sleep(POLL_INTERVAL).await;
        }
    }

    async fn stabilize_browser_activity(&self) -> Result<()> {
        let repository =
            PgFeatureParityRepository::new(self.infrastructure.postgres.connection().clone());
        let now = Utc::now();
        let recent_cutoff = now - ChronoDuration::hours(24);
        let mut latest = repository.latest_unbound_full().await?;
        if !latest
            .as_ref()
            .is_some_and(|run| run.created_at > recent_cutoff && run.window_end >= recent_cutoff)
        {
            let (http, access_token) = self.governed_http_session().await?;
            decode_http_json(
                http.post(format!(
                    "{}/api/research/feature-integrity/runs/full",
                    self.base_url()
                ))
                .header("accept-api-version", "v1")
                .header("x-acting-role", "super_admin")
                .bearer_auth(&access_token)
                .json(&json!({
                    "window_start": null,
                    "window_end": null,
                    "reason": "freeze browser evidence after one real current 24-hour parity replay",
                }))
                .send()
                .await
                .context("enqueue browser-evidence full parity replay")?,
                StatusCode::ACCEPTED,
                "enqueue browser-evidence full parity replay",
            )
            .await?;
            latest = repository.latest_unbound_full().await?;
        }

        let run_id = latest
            .as_ref()
            .context("browser-evidence full parity replay was not persisted")?
            .run_id;
        let deadline = Instant::now() + BROWSER_ACTIVITY_STABILITY_TIMEOUT;
        loop {
            let run = repository
                .find_run(&run_id)
                .await?
                .context("browser-evidence full parity replay disappeared")?;
            if run.status.is_terminal() {
                println!(
                    "browser activity state stabilized: parity_run_id={} status={}",
                    run.run_id, run.status
                );
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "browser-evidence full parity replay {run_id} did not settle within {BROWSER_ACTIVITY_STABILITY_TIMEOUT:?}: status={}",
                run.status
            );
            sleep(POLL_INTERVAL).await;
        }
    }

    async fn verify_data_plane_stability(&self) -> Result<DataPlaneStabilityEvidence> {
        let (http, access_token) = self.governed_http_session().await?;
        let status = decode_http_json(
            http.get(format!("{}/api/system/status", self.base_url()))
                .header("accept-api-version", "v1")
                .bearer_auth(&access_token)
                .send()
                .await
                .context("read connection-bound operational status")?,
            StatusCode::OK,
            "connection-bound operational status",
        )
        .await?;
        let expected = status["data"]["market_data"]["ws_shards"]["total"]
            .as_u64()
            .context("operational status omitted total WebSocket shards")?;
        ensure!(expected > 0, "production fixture owns no WebSocket shards");
        self.clob_upstream
            .wait_for_active_connections(expected, Duration::from_secs(10))
            .await?;
        let active = self.clob_upstream.active_connection_count();
        let high_water = self.clob_upstream.connection_high_water();
        let accepted = self.clob_upstream.accepted_connection_count();
        let accepted_delta = accepted.checked_sub(self.clob_accepted_baseline).context(
            "deterministic CLOB accepted-connection counter moved behind its readiness baseline",
        )?;
        let concurrency_bound = expected.saturating_mul(2);
        let allowed_turnover = if self.fixture == ProductionStackFixture::FeedbackClosureRecovery {
            expected
        } else {
            0
        };
        ensure!(
            active == expected
                && high_water <= concurrency_bound
                && accepted_delta <= allowed_turnover,
            "CLOB connection ownership escaped its bounds: active={active}, expected={expected}, high_water={high_water}, concurrency_bound={concurrency_bound}, baseline_accepted={}, accepted={accepted}, accepted_delta={accepted_delta}, allowed_turnover={allowed_turnover}",
            self.clob_accepted_baseline,
        );
        self.verify_runtime_log_health()?;
        println!(
            "production-stack data-plane stability passed: active={active} high_water={high_water} accepted={accepted} accepted_delta={accepted_delta} shard_bound={expected} allowed_turnover={allowed_turnover} forbidden_runtime_failures=0"
        );
        Ok(DataPlaneStabilityEvidence {
            expected_shards: expected,
            active_connections: active,
            connection_high_water: high_water,
            concurrency_bound,
            baseline_accepted_connections: self.clob_accepted_baseline,
            final_accepted_connections: accepted,
            accepted_connection_delta: accepted_delta,
            allowed_turnover,
            forbidden_runtime_failures: 0,
        })
    }

    async fn boundary_preimage(&self) -> Result<DisposableBoundaryPreimage> {
        let (http, access_token) = self.governed_http_session().await?;
        Ok(DisposableBoundaryPreimage {
            runtime_control: self.read_runtime_control(&http, &access_token).await?,
            money_path_counts: self.money_path_counts().await?,
        })
    }

    async fn read_runtime_control(
        &self,
        http: &Client,
        access_token: &str,
    ) -> Result<RuntimeControlSnapshot> {
        let response = decode_http_json(
            http.get(format!("{}/api/system/runtime-controls", self.base_url()))
                .header("accept-api-version", "v1")
                .bearer_auth(access_token)
                .send()
                .await
                .context("read disposable runtime-control boundary")?,
            StatusCode::OK,
            "read disposable runtime-control boundary",
        )
        .await?;
        serde_json::from_value(response["data"].clone())
            .context("decode disposable runtime-control boundary")
    }

    async fn money_path_counts(&self) -> Result<MoneyPathCounts> {
        let row = self
            .infrastructure
            .postgres
            .connection()
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT \
                 (SELECT COUNT(*) FROM quant_order_intent)::bigint AS order_intents, \
                 (SELECT COUNT(*) FROM quant_capital_allocation)::bigint AS capital_allocations, \
                 (SELECT COUNT(*) FROM quant_execution_account)::bigint AS execution_accounts, \
                 (SELECT COUNT(*) FROM quant_execution_order)::bigint AS execution_orders, \
                 (SELECT COUNT(*) FROM quant_execution_attempt_outcome)::bigint AS execution_attempt_outcomes, \
                 (SELECT COUNT(*) FROM quant_execution_attempt_reconciliation_task)::bigint AS execution_reconciliation_tasks, \
                 (SELECT COUNT(*) FROM quant_execution_rollup_reconciliation_task)::bigint AS execution_rollup_tasks, \
                 (SELECT COUNT(*) FROM quant_execution_trade_ref)::bigint AS execution_trade_refs, \
                 (SELECT COUNT(*) FROM quant_clob_trade_observation)::bigint AS clob_trade_observations, \
                 (SELECT COUNT(*) FROM quant_execution_transaction_ref)::bigint AS execution_transaction_refs, \
                 (SELECT COUNT(*) FROM quant_strategy_position_lot)::bigint AS strategy_position_lots, \
                 (SELECT COUNT(*) FROM quant_settlement_authorization)::bigint AS settlement_authorizations, \
                 (SELECT COUNT(*) FROM quant_settlement_chain_submission)::bigint AS settlement_chain_submissions, \
                 (SELECT COUNT(*) FROM quant_settlement_external_cursor)::bigint AS settlement_external_cursors, \
                 (SELECT COUNT(*) FROM quant_settlement_governed_action)::bigint AS settlement_governed_actions, \
                 (SELECT COUNT(*) FROM quant_settlement_inventory_lot)::bigint AS settlement_inventory_lots, \
                 (SELECT COUNT(*) FROM quant_settlement_redeem)::bigint AS settlement_redeems, \
                 (SELECT COUNT(*) FROM quant_settlement_redeem_lot)::bigint AS settlement_redeem_lots, \
                 (SELECT COUNT(*) FROM quant_account_chain_execution)::bigint AS account_chain_executions, \
                 (SELECT COUNT(*) FROM quant_account_execution_association)::bigint AS account_execution_associations, \
                 (SELECT COUNT(*) FROM quant_account_clean_funder_blocker)::bigint AS account_clean_funder_blockers, \
                 (SELECT COUNT(*) FROM quant_account_pause_operation)::bigint AS account_pause_operations, \
                 (SELECT COUNT(*) FROM quant_account_recovery_incident)::bigint AS account_recovery_incidents, \
                 (SELECT COUNT(*) FROM quant_account_recovery_manifest)::bigint AS account_recovery_manifests",
            ))
            .await
            .context("read disposable money-path counts")?
            .context("disposable money-path count query returned no row")?;
        Ok(MoneyPathCounts {
            order_intents: row.try_get("", "order_intents")?,
            capital_allocations: row.try_get("", "capital_allocations")?,
            execution_accounts: row.try_get("", "execution_accounts")?,
            execution_orders: row.try_get("", "execution_orders")?,
            execution_attempt_outcomes: row.try_get("", "execution_attempt_outcomes")?,
            execution_reconciliation_tasks: row.try_get("", "execution_reconciliation_tasks")?,
            execution_rollup_tasks: row.try_get("", "execution_rollup_tasks")?,
            execution_trade_refs: row.try_get("", "execution_trade_refs")?,
            clob_trade_observations: row.try_get("", "clob_trade_observations")?,
            execution_transaction_refs: row.try_get("", "execution_transaction_refs")?,
            strategy_position_lots: row.try_get("", "strategy_position_lots")?,
            settlement_authorizations: row.try_get("", "settlement_authorizations")?,
            settlement_chain_submissions: row.try_get("", "settlement_chain_submissions")?,
            settlement_external_cursors: row.try_get("", "settlement_external_cursors")?,
            settlement_governed_actions: row.try_get("", "settlement_governed_actions")?,
            settlement_inventory_lots: row.try_get("", "settlement_inventory_lots")?,
            settlement_redeems: row.try_get("", "settlement_redeems")?,
            settlement_redeem_lots: row.try_get("", "settlement_redeem_lots")?,
            account_chain_executions: row.try_get("", "account_chain_executions")?,
            account_execution_associations: row.try_get("", "account_execution_associations")?,
            account_clean_funder_blockers: row.try_get("", "account_clean_funder_blockers")?,
            account_pause_operations: row.try_get("", "account_pause_operations")?,
            account_recovery_incidents: row.try_get("", "account_recovery_incidents")?,
            account_recovery_manifests: row.try_get("", "account_recovery_manifests")?,
        })
    }

    async fn verify_disposable_boundary(
        &self,
        preimage: &DisposableBoundaryPreimage,
        runtime_control_before_drain: RuntimeControlSnapshot,
        route_commit: &JsonValue,
    ) -> Result<DisposableEvidenceBoundary> {
        let runtime_control_after = RuntimeControlSnapshot::from(
            PgRuntimeControlRepository::new(self.infrastructure.postgres.connection().clone())
                .load()
                .await
                .context("read drained durable runtime-control boundary")?,
        );
        let money_path_after = self.money_path_counts().await?;
        preimage.verify_runtime(&runtime_control_before_drain, &runtime_control_after)?;
        ensure!(
            preimage.money_path_counts == money_path_after,
            "disposable feedback closure wrote a capital-moving lifecycle: before={:?} after={money_path_after:?}",
            preimage.money_path_counts,
        );
        let execution_authority_unchanged =
            route_commit["data"]["receipt"]["execution_authority_unchanged"] == true;
        ensure!(
            execution_authority_unchanged,
            "disposable model-route receipt expanded execution authority"
        );

        let requests = self
            .upstream
            .received_requests()
            .await
            .context("read owned upstream request ledger")?;
        let real_venue_order_write_count = requests
            .iter()
            .filter(|request| {
                matches!(request.method.as_str(), "POST" | "DELETE")
                    && matches!(
                        request.url.path(),
                        "/order" | "/orders" | "/cancel-all" | "/cancel-market-orders"
                    )
            })
            .count();
        let relayer_request_count = requests
            .iter()
            .filter(|request| {
                request
                    .url
                    .path()
                    .starts_with("/__fixture__/relayer-denied")
            })
            .count();
        let real_chain_write_count = requests
            .iter()
            .filter_map(|request| serde_json::from_slice::<JsonRpcRequest>(&request.body).ok())
            .filter(|request| {
                matches!(
                    request.method.as_str(),
                    "eth_sendRawTransaction" | "eth_sendTransaction"
                )
            })
            .count();
        ensure!(
            real_venue_order_write_count == 0
                && real_chain_write_count == 0
                && relayer_request_count == 0,
            "disposable feedback closure attempted an outbound write: venue={real_venue_order_write_count} chain={real_chain_write_count} relayer={relayer_request_count}"
        );
        let real_capital_write_count = money_path_after
            .total()?
            .checked_sub(preimage.money_path_counts.total()?)
            .context("disposable money-path count regressed")?;
        ensure!(
            real_capital_write_count == 0,
            "disposable feedback closure changed money-path row counts"
        );

        Ok(DisposableEvidenceBoundary {
            evidence_scope: "owned_disposable_only".to_owned(),
            production_composed_binary: true,
            operational_activation_claimed: false,
            model_route_commit_scope: "disposable_fixture_only".to_owned(),
            outbound_write_endpoints: "owned_loopback_rejectors".to_owned(),
            runtime_control_before: preimage.runtime_control.clone(),
            runtime_control_after,
            execution_authority_unchanged,
            money_path_before: preimage.money_path_counts.clone(),
            money_path_after,
            real_venue_order_write_count: u64::try_from(real_venue_order_write_count)?,
            real_chain_write_count: u64::try_from(real_chain_write_count)?,
            real_capital_write_count,
            relayer_request_count: u64::try_from(relayer_request_count)?,
        })
    }

    fn verify_runtime_log_health(&self) -> Result<()> {
        let path = self.launch.log_path();
        let log = fs::read_to_string(&path)
            .with_context(|| format!("read production runtime health log {}", path.display()))?;
        Self::validate_fixture_log(&log, self.fixture)?;
        println!(
            "production-stack runtime log health passed: forbidden_failures=0 log={}",
            path.display()
        );
        Ok(())
    }

    #[cfg(test)]
    fn validate_runtime_log(log: &str) -> Result<()> {
        Self::validate_log_patterns(log, false)
    }

    fn validate_fixture_log(log: &str, fixture: ProductionStackFixture) -> Result<()> {
        let browser_fixture = matches!(
            fixture,
            ProductionStackFixture::Browser | ProductionStackFixture::GovernedFeedback
        );
        Self::validate_log_patterns(log, browser_fixture)
    }

    fn validate_log_patterns(log: &str, allow_browser_fixture_events: bool) -> Result<()> {
        for pattern in FORBIDDEN_RUNTIME_LOG_PATTERNS {
            ensure!(
                !log.contains(pattern),
                "production runtime emitted forbidden failure `{pattern}`"
            );
        }
        for line in log.lines() {
            ensure!(
                !line.contains(" ERROR ")
                    || (allow_browser_fixture_events && Self::is_expected_auth_outage(line)),
                "production runtime emitted an error log: {line}"
            );
            if line.contains(" WARN ") {
                ensure!(
                    allow_browser_fixture_events && Self::is_expected_parity_mismatch(line),
                    "production runtime emitted an unapproved warning log: {line}"
                );
            }
        }
        Ok(())
    }

    fn event_suffix<'a>(line: &'a str, target: &str) -> Option<&'a str> {
        let (timestamp, suffix) = line.split_once(target)?;
        timestamp
            .trim()
            .parse::<DateTime<Utc>>()
            .ok()
            .map(|_| suffix)
    }

    fn is_expected_parity_mismatch(line: &str) -> bool {
        let Some(mut suffix) = Self::event_suffix(line, BROWSER_PARITY_TARGET)
            .and_then(|suffix| suffix.strip_prefix(" parity_run_id="))
        else {
            return false;
        };
        for field in [
            " sampling_key=",
            " stage=",
            " report_id=",
            " model_run_id=",
            " model_version_id=",
            " market_id=",
            " feature_name=",
            " projected_evidence_matched=",
            " online_state=",
            " replay_state=",
            " online_value=",
            " replay_value=",
            " online_effective_at=",
            " replay_effective_at=",
            " online_available_at=",
            " replay_available_at=",
            " online_cutoff=",
            " replay_cutoff=",
            " online_fingerprint=",
            " replay_fingerprint=",
            " detail=",
        ] {
            let Some((value, remainder)) = suffix.split_once(field) else {
                return false;
            };
            if value.is_empty() {
                return false;
            }
            suffix = remainder;
        }
        !suffix.is_empty() && suffix.ends_with('}')
    }

    fn is_expected_auth_outage(line: &str) -> bool {
        let Some(fields) = Self::event_suffix(line, AUTH_OUTAGE_TARGET)
            .and_then(|suffix| suffix.strip_suffix(AUTH_OUTAGE_SUFFIX))
            .and_then(|fields| fields.strip_prefix("http.method=GET http.route=/api/auth/me "))
        else {
            return false;
        };
        [
            "exception.message=service unavailable: authentication temporarily unavailable",
            "exception.details=ServiceUnavailable(\"authentication temporarily unavailable\")",
            "http.status_code=503",
            "otel.status_code=\"ERROR\"",
        ]
        .iter()
        .all(|field| fields.contains(field))
    }

    async fn verify_readiness_capture(&self) -> Result<ReadinessCaptureEvidence> {
        let config_path = self.launch.run_dir.join("quant-pivot.toml");
        let request =
            DeployConfigLoadRequest::new(config_path, DeploymentEnvironment::local_development());
        let deploy = DeployConfig::load(&request).context("load production readiness config")?;
        let scope = EvidenceScopeIdentity::from_config(
            &deploy.db.clickhouse,
            &deploy.research.artifact_store,
        )?;
        let attestor = EvidenceAttestor::from_config(&deploy.research.evidence_attestation)?;
        let artifacts = self
            .artifact_infrastructure
            .as_ref()
            .context("feedback closure fixture has no readiness artifact infrastructure")?
            .store()?;
        let readiness = ResearchReadinessEvidenceService::new(
            Arc::new(PgResearchReadinessEvidenceRepository::new(
                self.infrastructure.postgres.connection().clone(),
            )),
            artifacts,
            attestor,
            &scope,
        )?;
        let verified_at = self
            .infrastructure
            .postgres
            .connection()
            .statement_time()
            .await;
        let verified = readiness.latest_verified(verified_at).await?;
        let evidence = verified.retention.with_context(|| {
            format!(
                "production closure has no current verified retention-runway evidence for the exact deployment scope: {}",
                verified.diagnostics.join("; ")
            )
        })?;
        let capture = ReadinessCaptureEvidence {
            verified_at,
            evidence,
        };
        capture.validate()?;
        Ok(capture)
    }

    async fn await_operational_readiness(&self) -> Result<()> {
        let (http, access_token) = self.governed_http_session().await?;
        let deadline = Instant::now() + OPERATIONAL_READINESS_TIMEOUT;
        let expected_markets = if matches!(
            self.fixture,
            ProductionStackFixture::FeedbackClosure
                | ProductionStackFixture::FeedbackClosureRecovery
        ) {
            10_u64
        } else {
            1
        };
        loop {
            let status = decode_bounded_http_json(
                http.get(format!("{}/api/system/status", self.base_url()))
                    .header("accept-api-version", "v1")
                    .bearer_auth(&access_token),
                StatusCode::OK,
                "production-stack operational status",
                READINESS_REQUEST_TIMEOUT,
            )
            .await?;
            let data = &status["data"];
            let operational = data["operational_phase"]["phase"] == "operational";
            let market_data_ready = data["market_data"]["ready"] == true;
            let connected_shards = data["market_data"]["ws_shards"]["total"]
                .as_u64()
                .is_some_and(|total| total > 0)
                && data["market_data"]["ws_shards"]["disconnected"].as_u64() == Some(0);
            let subscribed_markets = data["active_markets"]
                .as_u64()
                .is_some_and(|markets| markets >= expected_markets);
            let base_ready =
                operational && market_data_ready && connected_shards && subscribed_markets;
            if base_ready {
                self.await_browser_series(&http, &access_token).await?;
                println!(
                    "production-stack operational readiness passed: fixture={:?} active_markets={} ws_shards={} last_message_age_ms={}",
                    self.fixture,
                    data["active_markets"],
                    data["market_data"]["ws_shards"]["total"],
                    data["market_data"]["last_message_age_ms"],
                );
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!(
                    "production stack did not reach real market-data operational readiness within {OPERATIONAL_READINESS_TIMEOUT:?}: {status}"
                );
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    async fn await_browser_series(&self, http: &Client, access_token: &str) -> Result<()> {
        if !matches!(
            self.fixture,
            ProductionStackFixture::Browser | ProductionStackFixture::GovernedFeedback
        ) {
            return Ok(());
        }
        let deadline = Instant::now() + BROWSER_SERIES_READINESS_TIMEOUT;
        loop {
            let (yes_points, no_points) = self.browser_series_counts(http, access_token).await?;
            if yes_points >= BROWSER_SERIES_MIN_POINTS && no_points >= BROWSER_SERIES_MIN_POINTS {
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "browser microstructure did not reach {BROWSER_SERIES_MIN_POINTS} points per side within {BROWSER_SERIES_READINESS_TIMEOUT:?}: yes={yes_points} no={no_points}"
            );
            sleep(REPORT_BOOK_REFRESH_INTERVAL).await;
        }
    }

    async fn browser_series_counts(
        &self,
        http: &Client,
        access_token: &str,
    ) -> Result<(usize, usize)> {
        let to = Utc::now();
        let from = to - ChronoDuration::hours(1);
        let response = decode_bounded_http_json(
            http.get(format!(
                "{}/api/markets/{}/microstructure",
                self.base_url(),
                synthetic_condition_id()
            ))
            .header("accept-api-version", "v1")
            .bearer_auth(access_token)
            .query(&[("from", from.to_rfc3339()), ("to", to.to_rfc3339())]),
            StatusCode::OK,
            "browser microstructure readiness",
            READINESS_REQUEST_TIMEOUT,
        )
        .await?;
        let yes_points = response["data"]["yes"]
            .as_array()
            .context("browser microstructure readiness omitted yes series")?
            .len();
        let no_points = response["data"]["no"]
            .as_array()
            .context("browser microstructure readiness omitted no series")?
            .len();
        Ok((yes_points, no_points))
    }

    async fn governed_http_session(&self) -> Result<(Client, String)> {
        let http = Client::builder()
            .timeout(GOVERNED_REQUEST_TIMEOUT)
            .build()
            .context("build governed closure HTTP client")?;
        let login = decode_bounded_http_json(
            http.post(format!("{}/api/auth/login", self.base_url()))
                .header("accept-api-version", "v1")
                .json(&json!({
                    "username": "admin",
                    "password": BOOTSTRAP_ADMIN_PASSWORD,
                })),
            StatusCode::OK,
            "governed closure login",
            READINESS_REQUEST_TIMEOUT,
        )
        .await?;
        let access_token = login["data"]["access_token"]
            .as_str()
            .context("governed closure login omitted access_token")?
            .to_owned();
        Ok((http, access_token))
    }

    async fn verify_governed_trigger(&self, closure: &FeedbackClosureFixture) -> Result<()> {
        let (http, access_token) = self.governed_http_session().await?;
        let request = json!({
            "profile_id": fixture_profile_ref().id,
            "evaluation_mode": "conditional",
            "idempotency_key": format!("closure-trigger-{}", closure.feedback_cycle_id),
            "parent_cycle_id": null,
            "reason": "production_feedback_closure_trigger_verification",
        });
        let mut responses = Vec::with_capacity(2);
        for invocation in 1..=2 {
            let response = decode_http_json(
                http.post(format!("{}/api/research/feedback-cycles", self.base_url()))
                    .header("accept-api-version", "v1")
                    .header("x-acting-role", "super_admin")
                    .bearer_auth(&access_token)
                    .json(&request)
                    .send()
                    .await
                    .with_context(|| {
                        format!("invoke governed closure trigger attempt {invocation}")
                    })?,
                StatusCode::ACCEPTED,
                "verify governed closure trigger",
            )
            .await?;
            responses.push(response);
        }
        verify_trigger_replay(&responses[0], &responses[1], closure.feedback_cycle_id)
    }

    async fn commit_disposable_candidate(
        &self,
        outcome: &FeedbackClosureOutcome,
    ) -> Result<(JsonValue, JsonValue)> {
        let preimage = self.activation_preimage(outcome).await?;
        let (http, access_token) = self.governed_http_session().await?;
        let permit = decode_http_json(
            http.post(format!(
                "{}/api/research/model-route-activation-permits",
                self.base_url()
            ))
            .header("accept-api-version", "v1")
            .header("x-acting-role", "super_admin")
            .bearer_auth(&access_token)
            .json(&json!({
                "feedback_cycle_id": outcome.feedback_cycle_id,
                "ttl_secs": 3_600,
                "idempotency_key": format!("closure-permit-{}", outcome.feedback_cycle_id),
                "reason_code": "production_e2e_feedback_closure",
                "note": "Issue one exact permit after verifying all fifteen production feedback stages and the refitted mixed-Route scenario evidence.",
            }))
            .send()
            .await
            .context("issue governed closure activation permit")?,
            StatusCode::CREATED,
            "issue governed closure activation permit",
        )
        .await?;
        let permit_view = &permit["data"]["permit"];
        ensure!(
            permit_view["feedback_cycle_id"] == outcome.feedback_cycle_id.to_string()
                && permit_view["candidate_model_version_id"]
                    == outcome.candidate_model_version_id.to_string()
                && permit_view["candidate_manifest_id"]
                    == outcome.candidate_manifest_id.to_string()
                && permit_view["candidate_manifest_hash"]
                    == outcome.candidate_manifest_hash.to_string()
                && permit_view["status"] == "active",
            "server-derived promotion permit diverged from the verified closure outcome: {permit}"
        );
        let promotion_permit_id = permit_view["promotion_permit_id"]
            .as_str()
            .context("promotion permit omitted promotion_permit_id")?;
        let expected_policy_generation = permit_view["expected_policy_generation"]
            .as_u64()
            .context("promotion permit omitted expected_policy_generation")?;
        let expected_runtime_control_revision = permit_view["expected_runtime_control_revision"]
            .as_i64()
            .context("promotion permit omitted expected_runtime_control_revision")?;
        let route_commit = decode_http_json(
            http.post(format!(
                "{}/api/research/model-route-activations",
                self.base_url()
            ))
            .header("accept-api-version", "v1")
            .header("x-acting-role", "super_admin")
            .bearer_auth(&access_token)
            .json(&json!({
                "promotion_permit_id": promotion_permit_id,
                "feedback_cycle_id": outcome.feedback_cycle_id,
                "expected_policy_generation": expected_policy_generation,
                "expected_runtime_control_revision": expected_runtime_control_revision,
                "idempotency_key": format!("closure-activate-{}", outcome.feedback_cycle_id),
                "reason_code": "disposable_e2e_model_route_commit",
                "note": "Inside the owned disposable fixture, consume the verified permit and atomically commit the Weather candidate with its exact mixed-Route scenario-model bindings.",
            }))
            .send()
            .await
            .context("commit disposable governed model route")?,
            StatusCode::CREATED,
            "commit disposable governed model route",
        )
        .await?;
        let receipt = &route_commit["data"]["receipt"];
        ensure!(
            receipt["feedback_cycle_id"] == outcome.feedback_cycle_id.to_string()
                && receipt["route"] == "weather"
                && receipt["previous_model_version_id"]
                    == outcome.champion_model_version_id.to_string()
                && receipt["activated_model_version_id"]
                    == outcome.candidate_model_version_id.to_string()
                && receipt["execution_authority_unchanged"] == true
                && route_commit["data"]["replayed"] == false,
            "disposable model-route commit receipt diverged from the verified closure outcome: {route_commit}"
        );
        self.verify_activation_commit(outcome, &preimage).await?;
        Ok((permit, route_commit))
    }

    async fn activation_preimage(
        &self,
        outcome: &FeedbackClosureOutcome,
    ) -> Result<ActivationPolicyPreimage> {
        let policies = PgPolicyRepository::new(self.infrastructure.postgres.connection().clone());
        let bundle = policies
            .load_current_bundle()
            .await?
            .context("feedback activation has no current policy bundle")?;
        let pooled_champion_model_version_id = bundle
            .snapshot
            .model_routing
            .model
            .route_binding(BuyModelRoute::Pooled)?
            .champion
            .model_version_id;
        let crypto_champion_model_version_id = bundle
            .snapshot
            .model_routing
            .model
            .route_binding(BuyModelRoute::Crypto)?
            .champion
            .model_version_id;
        let weather_before = bundle
            .snapshot
            .model_routing
            .model
            .route_binding(BuyModelRoute::Weather)?;
        ensure!(
            weather_before.champion.model_version_id == outcome.champion_model_version_id
                && weather_before.shadow.as_ref().is_some_and(|shadow| {
                    shadow.model_version_id == outcome.candidate_model_version_id
                }),
            "CandidateReady outcome differs from the current Weather champion/shadow pair"
        );
        Ok(ActivationPolicyPreimage {
            bundle,
            pooled_champion_model_version_id,
            crypto_champion_model_version_id,
        })
    }

    async fn verify_activation_commit(
        &self,
        outcome: &FeedbackClosureOutcome,
        preimage: &ActivationPolicyPreimage,
    ) -> Result<()> {
        let policies = PgPolicyRepository::new(self.infrastructure.postgres.connection().clone());
        let after = policies
            .load_current_bundle()
            .await?
            .context("feedback activation produced no current policy bundle")?;
        let pooled_after = after
            .snapshot
            .model_routing
            .model
            .route_binding(BuyModelRoute::Pooled)?;
        let crypto_after = after
            .snapshot
            .model_routing
            .model
            .route_binding(BuyModelRoute::Crypto)?;
        let weather_after = after
            .snapshot
            .model_routing
            .model
            .route_binding(BuyModelRoute::Weather)?;
        let mut expected_scenario_bindings = preimage
            .bundle
            .snapshot
            .model_routing
            .model
            .portfolio_scenario_model_bindings
            .iter()
            .filter(|binding| !binding.ordered_routes.contains(&BuyModelRoute::Weather))
            .cloned()
            .collect::<Vec<_>>();
        expected_scenario_bindings.extend(outcome.portfolio_scenario_model_bindings.clone());
        expected_scenario_bindings.sort_by_key(|binding| {
            (
                binding.route_set_digest,
                binding.model_content_hash,
                binding.portfolio_scenario_model_artifact_id.as_uuid(),
            )
        });
        ensure!(
            pooled_after.champion.model_version_id == preimage.pooled_champion_model_version_id
                && pooled_after.shadow.is_none()
                && crypto_after.champion.model_version_id
                    == preimage.crypto_champion_model_version_id
                && crypto_after.shadow.is_none()
                && weather_after.champion.model_version_id == outcome.candidate_model_version_id
                && weather_after.shadow.is_none()
                && after
                    .snapshot
                    .model_routing
                    .model
                    .portfolio_scenario_model_bindings
                    == expected_scenario_bindings,
            "governed activation did not atomically preserve Crypto and install the candidate/scenario bindings"
        );
        let cycle =
            PgFeedbackCycleRepository::new(self.infrastructure.postgres.connection().clone())
                .find_cycle(&outcome.feedback_cycle_id)
                .await?
                .context("promoted feedback cycle disappeared")?;
        let shadow = ShadowBindingEntity::find()
            .filter(ShadowBindingColumn::FeedbackCycleId.eq(outcome.feedback_cycle_id))
            .one(self.infrastructure.postgres.connection())
            .await?
            .context("promoted feedback cycle lost its shadow-binding ledger")?;
        ensure!(
            cycle.decision == Some(FeedbackDecision::Promoted)
                && shadow.status == ShadowBindingStatus::Promoted
                && shadow.terminated_at.is_some(),
            "governed activation did not close the feedback and shadow lifecycles"
        );
        Ok(())
    }

    async fn run_feedback_report(
        &self,
        outcome: &FeedbackClosureOutcome,
        universe: &FeedbackReportUniverse,
    ) -> Result<JsonValue> {
        let (http, access_token) = self.governed_http_session().await?;
        let enqueue = decode_http_json(
            http.post(format!("{}/api/quant/reports/run", self.base_url()))
                .header("accept-api-version", "v1")
                .header("x-acting-role", "super_admin")
                .bearer_auth(&access_token)
                .json(&json!({
                    "request_id": format!("feedback-closure-report-{}", outcome.feedback_cycle_id),
                    "reason": "Verify the governed candidate through one real mixed-Route global portfolio report after the complete production feedback closure.",
                    "top_n": 10,
                    "knowledge_lag_secs": universe.knowledge_lag_secs,
                }))
                .send()
                .await
                .context("enqueue post-activation mixed-Route report")?,
            StatusCode::ACCEPTED,
            "enqueue post-activation mixed-Route report",
        )
        .await?;
        let report_run_id = enqueue["data"]["report_run_id"]
            .as_str()
            .context("mixed-Route report enqueue omitted report_run_id")?;
        let deadline = Instant::now() + REPORT_COMPLETION_TIMEOUT;
        let terminal_run = loop {
            let run = decode_http_json(
                http.get(format!(
                    "{}/api/quant/report-runs/{report_run_id}",
                    self.base_url()
                ))
                .header("accept-api-version", "v1")
                .bearer_auth(&access_token)
                .send()
                .await
                .context("poll mixed-Route report run")?,
                StatusCode::OK,
                "poll mixed-Route report run",
            )
            .await?;
            match run["data"]["status"].as_str() {
                Some("succeeded") => break run,
                Some("failed" | "skipped" | "abandoned") => {
                    bail!("mixed-Route report run failed closed: {run}")
                }
                Some("queued" | "running") => {}
                status => bail!("mixed-Route report run returned unknown status {status:?}: {run}"),
            }
            ensure!(
                Instant::now() < deadline,
                "mixed-Route report run exceeded the functional liveness ceiling {REPORT_COMPLETION_TIMEOUT:?}: {run}"
            );
            sleep(POLL_INTERVAL).await;
        };
        ensure!(
            terminal_run["data"]["knowledge_lag_secs"] == json!(universe.knowledge_lag_secs),
            "mixed-Route report changed its frozen knowledge lag: {terminal_run}"
        );
        let report_id = terminal_run["data"]["output_report_id"]
            .as_str()
            .context("successful mixed-Route run omitted output_report_id")?
            .parse::<RecommendationReportId>()?;
        let detail = self
            .wait_report_publication(&http, &access_token, &report_id.to_string(), deadline)
            .await?;
        let evidence = self
            .load_report_evidence(&http, &access_token, report_id, universe)
            .await?;
        let diagnostic_path = self
            .run_dir()
            .join(format!("feedback-report-{report_id}-diagnostics.json"));
        FeedbackReportDiagnosticArchive {
            recommendation_report_id: report_id,
            report_universe: universe,
            report_run: &terminal_run,
            report_detail: &detail,
            evidence: &evidence,
        }
        .persist(&diagnostic_path)?;
        ensure!(
            detail["data"]["represented_routes"]["routes"]
                == json!(["pooled", "crypto", "weather"])
                && detail["data"]["scenario_artifact_id"].is_string()
                && detail["data"]["scenario_artifact_hash"].is_string(),
            "published report lost its mixed-Route/scenario identity: report_id={report_id} diagnostics_artifact={}",
            diagnostic_path.display(),
        );
        validate_mixed_recommendations(
            &evidence.recommendations,
            universe,
            &evidence.diagnostics,
            &evidence.funnel,
            &evidence.funnel_markets,
            &diagnostic_path,
        )
        .with_context(|| format!("mixed-Route report evidence: {}", diagnostic_path.display()))?;
        self.verify_portfolio_plan(&report_id.to_string(), outcome, &evidence.recommendations)
            .await
            .with_context(|| {
                format!(
                    "mixed-Route portfolio evidence: {}",
                    diagnostic_path.display()
                )
            })?;
        Ok(json!({
            "run": terminal_run,
            "detail": detail,
            "recommendations": evidence.recommendations,
            "diagnostics": evidence.diagnostics,
            "funnel": evidence.funnel,
            "funnel_markets": evidence.funnel_markets,
            "diagnostics_artifact": diagnostic_path,
            "feature_nulls": evidence.feature_nulls,
        }))
    }

    async fn load_report_evidence(
        &self,
        http: &Client,
        access_token: &str,
        report_id: RecommendationReportId,
        universe: &FeedbackReportUniverse,
    ) -> Result<FeedbackReportEvidence> {
        let endpoint =
            |suffix: &str| format!("{}/api/quant/reports/{report_id}/{suffix}", self.base_url());
        let recommendations = decode_http_json(
            http.get(endpoint("recommendations"))
                .header("accept-api-version", "v1")
                .bearer_auth(access_token)
                .send()
                .await
                .context("read mixed-Route recommendations")?,
            StatusCode::OK,
            "read mixed-Route recommendations",
        )
        .await?;
        let diagnostics = decode_http_json(
            http.get(endpoint("diagnostics"))
                .header("accept-api-version", "v1")
                .bearer_auth(access_token)
                .send()
                .await
                .context("read mixed-Route report diagnostics")?,
            StatusCode::OK,
            "read mixed-Route report diagnostics",
        )
        .await?;
        let funnel = decode_http_json(
            http.get(endpoint("funnel"))
                .header("accept-api-version", "v1")
                .bearer_auth(access_token)
                .send()
                .await
                .context("read mixed-Route report funnel")?,
            StatusCode::OK,
            "read mixed-Route report funnel",
        )
        .await?;
        let market_evidence = FeedbackMarketFunnelEvidence::read(
            http,
            &endpoint("funnel/markets"),
            access_token,
            &universe.market_ids,
        )
        .await?;
        let feature_routes = market_evidence.feature_routes()?;
        let feature_nulls = self
            .feature_null_diagnostics(report_id, &feature_routes)
            .await?;
        Ok(FeedbackReportEvidence {
            recommendations,
            diagnostics,
            funnel,
            funnel_markets: market_evidence.response,
            funnel_market_pages: market_evidence.pages,
            feature_nulls,
        })
    }

    async fn feature_null_diagnostics(
        &self,
        report_id: RecommendationReportId,
        routes: &HashMap<String, JsonValue>,
    ) -> Result<JsonValue> {
        let report = RecommendationReportEntity::find_by_id(report_id)
            .one(self.infrastructure.postgres.connection())
            .await?
            .context("mixed-Route report disappeared before DQ diagnostics")?;
        let snapshot =
            ReportDataQualitySnapshotEntity::find_by_id(report.data_quality_snapshot_ref)
                .one(self.infrastructure.postgres.connection())
                .await?
                .context("mixed-Route report DQ snapshot disappeared")?;
        let mut diagnostics = Vec::new();
        for record in snapshot.tokens_json.0 {
            let Some(route) = routes.get(record.market_id.as_str()) else {
                continue;
            };
            let feature_vector_id = record.feature_vector_id;
            let vector = FeatureVectorEntity::find_by_id(feature_vector_id)
                .one(self.infrastructure.postgres.connection())
                .await?
                .with_context(|| {
                    format!("funnel feature vector {feature_vector_id} disappeared")
                })?;
            let cells = vector
                .payload
                .generic
                .iter()
                .chain(
                    vector
                        .payload
                        .domain
                        .iter()
                        .flat_map(|slice| slice.values.iter()),
                )
                .filter(|(_, cell)| {
                    matches!(
                        cell.state,
                        FeatureCellState::Missing | FeatureCellState::NotApplicable
                    )
                })
                .map(|(name, cell)| {
                    json!({
                        "name": name,
                        "state": cell.state,
                        "reason": cell.reason,
                        "staleness": cell.staleness,
                    })
                })
                .collect::<Vec<_>>();
            diagnostics.push(json!({
                "market_id": vector.market_id,
                "route": route,
                "feature_vector_id": feature_vector_id,
                "data_quality": vector.data_quality,
                "cells": cells,
            }));
        }
        Ok(JsonValue::Array(diagnostics))
    }

    async fn prepare_report_ingress(
        &self,
        fixture: &FeedbackClosureFixture,
    ) -> Result<FeedbackReportUniverse> {
        let universe = prepare_feedback_report_universe(
            self.infrastructure.postgres.connection(),
            fixture,
            CLOSURE_REPORT_KNOWLEDGE_LAG_SECS,
        )
        .await?;
        let tokens = fixture
            .report_book_snapshots()?
            .into_iter()
            .map(|snapshot| snapshot.token_id)
            .collect::<Vec<_>>();
        self.clob_upstream
            .refresh_handle()
            .wait_for_token_owners(&tokens, CATALOG_SETTLE_TIMEOUT)
            .await?;
        // Catalog convergence may consume its control-plane window. Refresh
        // history readiness afterwards, before starting any event deadline.
        self.await_report_history(&universe).await?;
        Ok(universe)
    }

    /// Shared real-ingress boundary for closure and focused readiness proofs.
    pub(crate) async fn install_report_books(
        db: &DatabaseConnection,
        fixture: &FeedbackClosureFixture,
        refresh: &DeterministicClobRefreshHandle,
        knowledge_lag_secs: u64,
    ) -> Result<DateTime<Utc>> {
        FixtureBookTiming::closure()?.validate_closure()?;
        ensure!(
            knowledge_lag_secs == FixtureBookTiming::REPORT_LAG_SECS,
            "report readiness lag differs from the closure freshness contract"
        );
        refresh.pause_keepalive();
        let snapshots = fixture.report_book_snapshots()?;
        let delivery_budget = Duration::from_millis(FixtureBookTiming::DELIVERY_BUDGET_MS);
        for (index, snapshot) in snapshots.iter().enumerate() {
            let deadline = Instant::now() + delivery_budget;
            let sent_after = refresh
                .send_snapshot(
                    &snapshot.token_id,
                    &snapshot.bids,
                    &snapshot.asks,
                    u64::try_from(index)?,
                    deadline,
                )
                .await?;
            fixture
                .await_report_book_snapshots(slice::from_ref(snapshot), sent_after, deadline)
                .await?;
            sleep(POLL_INTERVAL).await;
        }
        let resumed_at = Utc::now();
        refresh.resume_keepalive();
        let warmup_budget = Duration::from_secs(FixtureBookTiming::FEED_PERIOD_SECS)
            .checked_add(delivery_budget)
            .context("exact pulse warmup budget overflow")?;
        // Installation can span many token-delivery budgets. Do not admit a
        // report until a fresh, complete periodic cohort has reached durability.
        fixture
            .await_report_book_snapshots(&snapshots, resumed_at, Instant::now() + warmup_budget)
            .await
            .context("complete exact periodic pulse did not become durable within cadence plus delivery budget")?;
        fixture
            .await_report_pit_books(
                db,
                resumed_at,
                knowledge_lag_secs,
                Instant::now() + CATALOG_SETTLE_TIMEOUT,
            )
            .await
            .context("complete exact periodic cohort was not visible at the report PIT boundary")
    }

    async fn quiesce_report_ingest(&self) -> Result<()> {
        self.clob_upstream.refresh_handle().pause_keepalive();
        sleep(REPORT_INGEST_QUIESCE).await;
        self.verify_runtime_log_health()
            .context("quiesce canonical ingest before report source commit")
    }

    async fn wait_report_publication(
        &self,
        http: &Client,
        access_token: &str,
        report_id: &str,
        deadline: Instant,
    ) -> Result<JsonValue> {
        loop {
            let detail = decode_http_json(
                http.get(format!("{}/api/quant/reports/{report_id}", self.base_url()))
                    .header("accept-api-version", "v1")
                    .bearer_auth(access_token)
                    .send()
                    .await
                    .context("poll mixed-Route report publication")?,
                StatusCode::OK,
                "poll mixed-Route report publication",
            )
            .await?;
            let report_status = detail["data"]["status"].as_str();
            let delivery_status = detail["data"]["fact_delivery"]["status"].as_str();
            match (report_status, delivery_status) {
                (Some("published"), Some("verified")) => return Ok(detail),
                (Some("prepared"), Some("pending" | "delivering" | "retrying" | "verified")) => {}
                (Some("prepared"), Some("failed" | "cancelled")) => {
                    bail!("mixed-Route report fact delivery failed closed: {detail}")
                }
                (Some("superseded" | "obsolete" | "revoked" | "expired"), _) => {
                    bail!("mixed-Route report terminated before publication: {detail}")
                }
                _ => bail!(
                    "mixed-Route report returned an invalid publication/delivery state: {detail}"
                ),
            }
            ensure!(
                Instant::now() < deadline,
                "mixed-Route report publication exceeded the end-to-end functional liveness ceiling {REPORT_COMPLETION_TIMEOUT:?}: {detail}"
            );
            sleep(POLL_INTERVAL).await;
        }
    }

    async fn verify_portfolio_plan(
        &self,
        report_id: &str,
        outcome: &FeedbackClosureOutcome,
        recommendations: &JsonValue,
    ) -> Result<()> {
        let report_id = report_id.parse::<RecommendationReportId>()?;
        let report = RecommendationReportEntity::find_by_id(report_id)
            .one(self.infrastructure.postgres.connection())
            .await?
            .context("published mixed-Route report row is missing")?;
        let portfolio = PortfolioPlanEntity::find_by_id(report.portfolio_plan_id)
            .one(self.infrastructure.postgres.connection())
            .await?
            .context("published mixed-Route portfolio plan is missing")?;
        let scenario = portfolio
            .scenario_artifact_json
            .as_ref()
            .context("optimized mixed-Route plan has no concrete scenario artifact")?;
        let expected = outcome
            .portfolio_scenario_model_bindings
            .iter()
            .find(|binding| {
                binding.ordered_routes
                    == vec![
                        BuyModelRoute::Pooled,
                        BuyModelRoute::Crypto,
                        BuyModelRoute::Weather,
                    ]
            })
            .context("CandidateReady manifest omitted the three-Route scenario binding")?;
        let scenario_model_matches =
            scenario.scenario_model_content_hash == expected.model_content_hash;
        ensure!(
            (scenario.portfolio_scenario_model_artifact_id
                == expected.portfolio_scenario_model_artifact_id)
                && scenario_model_matches
                && (scenario.ordered_routes == expected.ordered_routes)
                && (scenario.route_set_digest == expected.route_set_digest),
            "report scenario artifact differs from the candidate-fitted governed binding"
        );
        let PortfolioDecisionResult::Optimized { plan } = &portfolio.decision_json else {
            bail!("mixed-Route report published without an optimized global portfolio plan")
        };
        let recommendation_count = recommendations["data"]
            .as_array()
            .context("mixed-Route recommendation response is not an array")?
            .len();
        ensure!(
            plan.solver.backend == "highs"
                && plan.solver.optimal
                && plan.solver.deterministic_threads == 1
                && plan.exact_verification.passed
                && plan.selected_tier_ids.len() == recommendation_count
                && plan.constraints.selected_recommendation_count
                    == u32::try_from(recommendation_count)?,
            "mixed-Route plan lacks optimal HiGHS and exact post-solve evidence: {plan:?}"
        );
        Ok(())
    }

    async fn verify_successor_feedback(
        db: &DatabaseConnection,
        outcome: &FeedbackClosureOutcome,
        report: &JsonValue,
        resolution_plane: &FeedbackReportResolutionEvidence,
    ) -> Result<SuccessorFeedbackEvidence> {
        let report_id = report["run"]["data"]["output_report_id"]
            .as_str()
            .context("feedback closure report omitted output_report_id")?
            .parse::<RecommendationReportId>()?;
        let report_row = RecommendationReportEntity::find_by_id(report_id)
            .one(db)
            .await?
            .context("successor feedback report row is missing")?;
        let plane_identity_matches = resolution_plane.report_id == report_id
            && resolution_plane.report_decision_at == report_row.decision_at;
        let plane_forward = resolution_plane.observed_at > report_row.decision_at;
        let plane_ordered = resolution_plane.observed_at >= resolution_plane.resolved_at;
        ensure!(
            plane_identity_matches && plane_forward && plane_ordered,
            "source-native resolution plane is not bound after the exact committed report decision"
        );
        let recommendations = RecommendationEntity::find()
            .filter(RecommendationColumn::RecommendationReportId.eq(report_id))
            .order_by_asc(RecommendationColumn::Rank)
            .all(db)
            .await?;
        ensure!(
            !recommendations.is_empty(),
            "successor feedback proof has no published recommendation"
        );
        let resolved_markets = resolution_plane
            .facts
            .iter()
            .map(|fact| fact.market_id.clone())
            .collect::<HashSet<_>>();
        ensure!(
            recommendations
                .iter()
                .all(|recommendation| resolved_markets.contains(&recommendation.market_id)),
            "source-native resolution plane does not cover every published recommendation"
        );

        let SuccessorOutcomeEvidence {
            outcomes,
            economic_outcomes,
            truth_cutoff,
        } = SuccessorOutcomeVerifier {
            db,
            recommendations: &recommendations,
            resolution_plane,
            decision_at: report_row.decision_at,
        }
        .verify()
        .await?;
        let mut recommendations_by_run = HashMap::<ReportRouteRunId, Vec<RecommendationId>>::new();
        for recommendation in &recommendations {
            recommendations_by_run
                .entry(recommendation.report_route_run_id)
                .or_default()
                .push(recommendation.recommendation_id);
        }

        let verifier = SuccessorRouteVerifier {
            db,
            outcome,
            report_id,
            decision_at: report_row.decision_at,
            truth_cutoff,
            outcomes: &outcomes,
            economic_outcomes: &economic_outcomes,
        };
        let mut route_cohorts = Vec::with_capacity(recommendations_by_run.len());
        for (route_run_id, recommendation_ids) in recommendations_by_run {
            route_cohorts.push(verifier.verify(route_run_id, recommendation_ids).await?);
        }
        route_cohorts.sort_by_key(|route| route.route.as_str());
        ensure!(
            route_cohorts.len() == 2
                && route_cohorts
                    .iter()
                    .any(|route| route.route == BuyModelRoute::Crypto)
                && route_cohorts
                    .iter()
                    .any(|route| route.route == BuyModelRoute::Weather)
                && route_cohorts.iter().all(|route| {
                    let expected = route.recommendation_ids.len();
                    usize::try_from(route.model_learning_eligible_count).ok() == Some(expected)
                        && usize::try_from(route.economic_outcome_count).ok() == Some(expected)
                        && route.economic_outcomes.len() == expected
                        && usize::try_from(route.policy_evaluation_eligible_count).ok()
                            == Some(expected)
                        && usize::try_from(route.execution_learning_censored_count).ok()
                            == Some(expected)
                }),
            "successor feedback evidence is not complete for both represented Routes"
        );
        Ok(SuccessorFeedbackEvidence {
            parent_cycle_id: outcome.feedback_cycle_id,
            decision_window_start: report_row.decision_at,
            decision_cutoff: truth_cutoff,
            truth_cutoff,
            route_cohorts,
        })
    }

    async fn finalize_closure_manifest(&mut self) -> Result<Option<ContentHash>> {
        let pending = self.pending_closure_manifest.take();
        // Alive-through-readiness supervision ends here. Claim the still-running
        // child before signaling, then explicitly accept its verified planned
        // exit while the disposable stores remain available for final reads.
        let mut child = self.child.begin_shutdown().await?;
        self.signal_binary(&child, ProductionShutdownSignal::Terminate)?;
        self.wait_for_exit(&mut child)
            .await
            .context("drain production binary before sealing closure evidence")?;
        drop(child);
        self.verify_runtime_log_health()
            .context("validate drained production runtime log")?;
        let Some(pending) = pending else {
            return Ok(None);
        };
        let pending = *pending;
        let evidence_boundary = self
            .verify_disposable_boundary(
                &pending.preimage,
                pending.runtime_control_before_drain,
                &pending.disposable_model_route_commit,
            )
            .await?;
        let manifest = GovernedClosureManifest {
            format_version: 1,
            evidence_boundary,
            closure: pending.closure,
            data_plane_stability: pending.data_plane_stability,
            readiness_capture: pending.readiness_capture,
            pre_activation_parity: pending.pre_activation_parity,
            historical_economic_backfill: pending.historical_economic_backfill,
            permit: pending.permit,
            disposable_model_route_commit: pending.disposable_model_route_commit,
            report_universe: pending.report_universe,
            report: pending.report,
            report_parity: pending.report_parity,
            resolution_plane: pending.resolution_plane,
            successor_feedback: pending.successor_feedback,
        };
        self.persist_closure_manifest(&manifest)?;
        let path = self.run_dir().join("feedback-closure-manifest.json");
        let bytes = fs::read(&path).with_context(|| {
            format!("read governed feedback-closure evidence {}", path.display())
        })?;
        validate_closure_manifest(&bytes).map(Some)
    }

    fn persist_closure_manifest(&self, manifest: &GovernedClosureManifest) -> Result<()> {
        let path = self.run_dir().join("feedback-closure-manifest.json");
        let payload = serde_json::to_vec_pretty(manifest)?;
        Self::persist_json_manifest(&path, &payload, "governed closure")
    }

    fn persist_candidate_manifest(
        &self,
        outcome: &FeedbackClosureOutcome,
        report_universe: &FeedbackReportUniverse,
        historical_economic_backfill: &HistoricalEconomicBackfill,
    ) -> Result<()> {
        historical_economic_backfill.validate()?;
        ensure!(
            historical_economic_backfill.terminal.observed_at <= report_universe.decision_at,
            "CandidateReady universe predates historical economic warmup completion"
        );
        let path = self
            .run_dir()
            .join("feedback-candidate-ready-manifest.json");
        let payload = serde_json::to_vec_pretty(&CandidateReadyClosureManifest {
            closure: outcome,
            report_universe,
            historical_economic_backfill,
        })?;
        Self::persist_json_manifest(&path, &payload, "CandidateReady")
    }

    fn start_browser_closure_monitor(
        &mut self,
        fixture: FeedbackClosureFixture,
        outcome: FeedbackClosureOutcome,
        report_universe: FeedbackReportUniverse,
    ) -> Result<()> {
        ensure!(
            self.browser_closure_monitor.is_none(),
            "browser closure monitor was started more than once"
        );
        let db = self.infrastructure.postgres.connection().clone();
        let run_dir = self.run_dir().to_path_buf();
        let feedback_cycle_id = outcome.feedback_cycle_id;
        let clob_refresh = self.clob_upstream.refresh_handle();
        self.browser_closure_monitor = Some(tokio::spawn(async move {
            let result = Self::monitor_browser_feedback_closure(
                &db,
                &run_dir,
                &clob_refresh,
                &fixture,
                &outcome,
                &report_universe,
            )
            .await;
            if let Err(error) = result {
                let detail = format!("{error:#}");
                let failure_path = run_dir.join("feedback-browser-closure-error.json");
                let payload = serde_json::to_vec_pretty(&BrowserClosureFailureManifest {
                    feedback_cycle_id,
                    error: &detail,
                })?;
                Self::persist_json_manifest(&failure_path, &payload, "browser closure failure")
                    .with_context(|| format!("persist closure failure after {detail}"))?;
                return Err(error);
            }
            Ok(())
        }));
        Ok(())
    }

    async fn monitor_browser_feedback_closure(
        db: &DatabaseConnection,
        run_dir: &Path,
        clob_refresh: &DeterministicClobRefreshHandle,
        fixture: &FeedbackClosureFixture,
        outcome: &FeedbackClosureOutcome,
        report_universe: &FeedbackReportUniverse,
    ) -> Result<()> {
        Self::await_browser_candidate_activation(db, outcome).await?;
        let refreshed_at = Self::install_report_books(
            db,
            fixture,
            clob_refresh,
            report_universe.knowledge_lag_secs,
        )
        .await?;
        let snapshots = fixture.report_book_snapshots()?;
        fixture
            .refresh_report_microstructure(db, report_universe.knowledge_lag_secs)
            .await?;
        let readiness_path = run_dir.join("feedback-browser-report-ready.json");
        let readiness = serde_json::to_vec_pretty(&json!({
            "feedback_cycle_id": outcome.feedback_cycle_id,
            "refreshed_at": refreshed_at,
            "snapshot_count": snapshots.len(),
        }))?;
        Self::persist_json_manifest(&readiness_path, &readiness, "browser report readiness")?;
        let report_id = Self::await_browser_feedback_report(
            db,
            clob_refresh,
            &snapshots,
            fixture,
            outcome,
            report_universe,
        )
        .await?;
        let report_parity = await_report_parity(db, &report_id).await?;
        let resolution_plane =
            settle_feedback_report_universe(db, fixture, report_universe, report_id).await?;
        let report = json!({
            "run": {
                "data": {
                    "output_report_id": report_id,
                },
            },
        });
        let successor_feedback =
            Self::verify_successor_feedback(db, outcome, &report, &resolution_plane).await?;
        let manifest_path = run_dir.join("feedback-browser-closure-manifest.json");
        let payload = serde_json::to_vec_pretty(&BrowserClosureManifest {
            closure: outcome,
            report_universe,
            report_id,
            report_parity: &report_parity,
            resolution_plane: &resolution_plane,
            successor_feedback: &successor_feedback,
        })?;
        Self::persist_json_manifest(&manifest_path, &payload, "browser N-to-N+1 closure")
    }

    async fn await_browser_candidate_activation(
        db: &DatabaseConnection,
        outcome: &FeedbackClosureOutcome,
    ) -> Result<()> {
        let deadline = Instant::now() + BROWSER_CLOSURE_TIMEOUT;
        let cycles = PgFeedbackCycleRepository::new(db.clone());
        loop {
            let cycle = cycles
                .find_cycle(&outcome.feedback_cycle_id)
                .await?
                .with_context(|| {
                    format!(
                        "browser closure cycle {} disappeared before activation",
                        outcome.feedback_cycle_id
                    )
                })?;
            let shadow = ShadowBindingEntity::find()
                .filter(ShadowBindingColumn::FeedbackCycleId.eq(outcome.feedback_cycle_id))
                .one(db)
                .await?
                .context("browser closure lost its Route-owned shadow binding")?;
            if cycle.decision == Some(FeedbackDecision::Promoted)
                && shadow.status == ShadowBindingStatus::Promoted
                && shadow.terminated_at.is_some()
            {
                return Ok(());
            }
            ensure!(
                shadow.status != ShadowBindingStatus::Rejected,
                "browser closure candidate was rejected before N-to-N+1 verification"
            );
            ensure!(
                Instant::now() < deadline,
                "browser closure did not observe exact candidate activation within {BROWSER_CLOSURE_TIMEOUT:?}"
            );
            sleep(POLL_INTERVAL).await;
        }
    }

    async fn await_browser_feedback_report(
        db: &DatabaseConnection,
        clob_refresh: &DeterministicClobRefreshHandle,
        snapshots: &[FeedbackReportBookSnapshot],
        fixture: &FeedbackClosureFixture,
        outcome: &FeedbackClosureOutcome,
        universe: &FeedbackReportUniverse,
    ) -> Result<RecommendationReportId> {
        let deadline = Instant::now() + BROWSER_CLOSURE_TIMEOUT;
        let expected_markets = universe.market_ids.iter().cloned().collect::<HashSet<_>>();
        let expected_routes = vec![
            BuyModelRoute::Pooled,
            BuyModelRoute::Crypto,
            BuyModelRoute::Weather,
        ];
        let expected_recommendation_routes =
            HashSet::from([BuyModelRoute::Crypto, BuyModelRoute::Weather]);
        ensure!(
            !snapshots.is_empty(),
            "browser report readiness has no exact book snapshot"
        );
        let mut refresh_index = 0_usize;
        let mut next_microstructure_refresh = Instant::now() + Duration::from_secs(10);
        loop {
            if Instant::now() >= next_microstructure_refresh {
                fixture
                    .refresh_report_microstructure(db, universe.knowledge_lag_secs)
                    .await?;
                next_microstructure_refresh = Instant::now() + Duration::from_secs(10);
            }
            for snapshot in snapshots {
                clob_refresh
                    .send_snapshot(
                        &snapshot.token_id,
                        &snapshot.bids,
                        &snapshot.asks,
                        u64::try_from(refresh_index)?,
                        Instant::now()
                            + Duration::from_millis(FixtureBookTiming::DELIVERY_BUDGET_MS),
                    )
                    .await?;
                refresh_index = refresh_index.saturating_add(1);
            }
            let reports = RecommendationReportEntity::find()
                .filter(
                    RecommendationReportColumn::Status.eq(RecommendationReportStatus::Published),
                )
                .filter(RecommendationReportColumn::DecisionAt.gte(universe.decision_at))
                .order_by_asc(RecommendationReportColumn::DecisionAt)
                .all(db)
                .await?;
            for report in reports {
                if report.represented_routes_json.routes != expected_routes {
                    continue;
                }
                let recommendations = RecommendationEntity::find()
                    .filter(
                        RecommendationColumn::RecommendationReportId
                            .eq(report.recommendation_report_id),
                    )
                    .order_by_asc(RecommendationColumn::Rank)
                    .all(db)
                    .await?;
                let routes = recommendations
                    .iter()
                    .map(|recommendation| recommendation.route)
                    .collect::<HashSet<_>>();
                let recommendation_markets = recommendations
                    .iter()
                    .map(|recommendation| recommendation.market_id.clone())
                    .collect::<HashSet<_>>();
                if recommendations.len() == expected_markets.len()
                    && routes == expected_recommendation_routes
                    && recommendation_markets == expected_markets
                    && recommendations.iter().all(|recommendation| {
                        universe.routes_by_market.get(&recommendation.market_id)
                            == Some(&recommendation.route)
                    })
                {
                    let weather_route = recommendations
                        .iter()
                        .find(|recommendation| recommendation.route == BuyModelRoute::Weather)
                        .context("mixed-Route browser report omitted its Weather recommendation")?;
                    let route_run =
                        ReportRouteRunEntity::find_by_id(weather_route.report_route_run_id)
                            .one(db)
                            .await?
                            .context("mixed-Route browser report lost its Weather Route run")?;
                    ensure!(
                        route_run.model_version_id == Some(outcome.candidate_model_version_id),
                        "browser report did not consume the activated Weather candidate"
                    );
                    return Ok(report.recommendation_report_id);
                }
            }
            ensure!(
                Instant::now() < deadline,
                "browser closure did not observe a candidate-backed mixed-Route report within {BROWSER_CLOSURE_TIMEOUT:?}"
            );
            sleep(REPORT_BOOK_REFRESH_INTERVAL).await;
        }
    }

    fn persist_json_manifest(path: &Path, payload: &[u8], label: &str) -> Result<()> {
        let temporary_path = path.with_extension("json.tmp");
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary_path)
            .with_context(|| {
                format!(
                    "create temporary {label} manifest {}",
                    temporary_path.display()
                )
            })?;
        file.write_all(payload).with_context(|| {
            format!(
                "write temporary {label} manifest {}",
                temporary_path.display()
            )
        })?;
        file.sync_all().with_context(|| {
            format!(
                "sync temporary {label} manifest {}",
                temporary_path.display()
            )
        })?;
        drop(file);
        fs::rename(&temporary_path, path)
            .with_context(|| format!("publish {label} manifest {}", path.display()))?;
        Ok(())
    }
}

fn verify_trigger_replay(
    first: &JsonValue,
    replay: &JsonValue,
    expected_cycle_id: FeedbackCycleId,
) -> Result<()> {
    let first = &first["data"];
    let replay = &replay["data"];
    let expected_cycle_id = expected_cycle_id.to_string();
    ensure!(
        first["cycle"]["feedback_cycle_id"] == expected_cycle_id
            && replay["cycle"]["feedback_cycle_id"] == expected_cycle_id,
        "governed Trigger did not converge on the expected cycle"
    );
    for field in TRIGGER_IDENTITY_FIELDS {
        if !matches!(*field, "parent_cycle_id" | "forced_idempotency_key") {
            ensure!(
                !first["cycle"][field].is_null(),
                "governed Trigger omitted immutable cycle field `{field}`"
            );
        }
        ensure!(
            first["cycle"][field] == replay["cycle"][field],
            "governed Trigger replay changed immutable cycle field `{field}`: first={} replay={}",
            first["cycle"][field],
            replay["cycle"][field]
        );
    }
    ensure!(
        first["cycle_reused"].is_boolean()
            && first["trigger_replayed"] == false
            && replay["cycle_reused"] == true
            && replay["trigger_replayed"] == true,
        "governed Trigger replay flags are inconsistent: first={first} replay={replay}"
    );
    let first_generation = first["cycle"]["generation"]
        .as_i64()
        .context("first Trigger response omitted lifecycle generation")?;
    let replay_generation = replay["cycle"]["generation"]
        .as_i64()
        .context("replayed Trigger response omitted lifecycle generation")?;
    ensure!(
        replay_generation >= first_generation,
        "governed Trigger lifecycle generation regressed: first={first_generation} replay={replay_generation}"
    );
    let first_status = first["cycle"]["status"]
        .as_str()
        .context("first Trigger response omitted lifecycle status")?;
    let replay_status = replay["cycle"]["status"]
        .as_str()
        .context("replayed Trigger response omitted lifecycle status")?;
    ensure!(
        trigger_status_rank(replay_status)? >= trigger_status_rank(first_status)?,
        "governed Trigger lifecycle status regressed: first={first_status} replay={replay_status}"
    );
    Ok(())
}

fn trigger_status_rank(status: &str) -> Result<u8> {
    match status {
        "queued" => Ok(0),
        "running" => Ok(1),
        "succeeded" | "failed" | "cancelled" => Ok(2),
        _ => bail!("governed Trigger returned unknown lifecycle status `{status}`"),
    }
}

async fn read_feedback_cohort(
    repository: &PgFeedbackCohortRepository,
    cohort: FeedbackCohort,
    snapshot: FeedbackCohortSnapshot,
) -> Result<Vec<FeedbackCohortCandidate>> {
    let mut candidates = Vec::new();
    let mut after = None;
    loop {
        let query = FeedbackCohortPageQuery::try_new(
            cohort,
            snapshot.clone(),
            after,
            FEEDBACK_COHORT_PAGE_LIMIT,
        )?;
        let page = repository.list_page(query).await?;
        candidates.extend_from_slice(page.candidates());
        let Some(cursor) = page.next_cursor() else {
            break;
        };
        after = Some(cursor);
    }
    Ok(candidates)
}

fn current_report_candidates(
    candidates: Vec<FeedbackCohortCandidate>,
    report_id: RecommendationReportId,
    expected_ids: &HashSet<RecommendationId>,
    cohort: FeedbackCohort,
) -> Result<Vec<FeedbackCohortCandidate>> {
    let current = candidates
        .into_iter()
        .filter(|candidate| {
            candidate.context().recommendation_report_id() == report_id
                && expected_ids.contains(&candidate.context().recommendation_id())
        })
        .collect::<Vec<_>>();
    let actual_ids = current
        .iter()
        .map(|candidate| candidate.context().recommendation_id())
        .collect::<HashSet<_>>();
    ensure!(
        &actual_ids == expected_ids,
        "{cohort} successor cohort does not contain the exact report Route population: actual={actual_ids:?} expected={expected_ids:?}"
    );
    Ok(current)
}

fn validate_mixed_recommendations(
    response: &JsonValue,
    universe: &FeedbackReportUniverse,
    diagnostics: &JsonValue,
    funnel: &JsonValue,
    funnel_markets: &JsonValue,
    artifact_path: &Path,
) -> Result<()> {
    let recommendations = response["data"]
        .as_array()
        .context("mixed-Route recommendation response is not an array")?;
    let mut route_counts = BTreeMap::new();
    for recommendation in recommendations {
        let route = recommendation["route"]
            .as_str()
            .context("global recommendation omitted Route")?;
        *route_counts.entry(route).or_insert(0_usize) += 1;
    }
    let selected_market_ids = recommendations
        .iter()
        .map(|row| {
            row["market_id"]
                .as_str()
                .context("global recommendation omitted market_id")
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let expected_market_ids = universe
        .market_ids
        .iter()
        .map(MarketId::as_str)
        .collect::<BTreeSet<_>>();
    let terminal_rows = funnel_markets["data"]["items"]
        .as_array()
        .context("market terminal evidence omitted items")?;
    let terminal_market_ids = terminal_rows
        .iter()
        .map(|row| {
            row["market_id"]
                .as_str()
                .context("market terminal evidence omitted market_id")
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let missing_market_ids = expected_market_ids
        .difference(&selected_market_ids)
        .collect::<Vec<_>>();
    let unexpected_market_ids = selected_market_ids
        .difference(&expected_market_ids)
        .collect::<Vec<_>>();
    let missing_terminal_ids = expected_market_ids
        .difference(&terminal_market_ids)
        .collect::<Vec<_>>();
    ensure!(
        recommendations.len() == universe.market_ids.len()
            && selected_market_ids == expected_market_ids
            && terminal_rows.len() == universe.market_ids.len()
            && terminal_market_ids == expected_market_ids,
        "mixed-Route report did not publish the exact terminal market universe: recommendations={} universe={} missing_market_ids={missing_market_ids:?} unexpected_market_ids={unexpected_market_ids:?} missing_terminal_ids={missing_terminal_ids:?} route_counts={route_counts:?} route_funnels={} market_terminals={} diagnostics_artifact={}",
        recommendations.len(),
        universe.market_ids.len(),
        compact_route_funnels(diagnostics),
        compact_market_terminals(funnel_markets),
        artifact_path.display()
    );
    let mut expected_route_counts = BTreeMap::new();
    for route in universe.routes_by_market.values() {
        *expected_route_counts
            .entry(route.as_str())
            .or_insert(0_usize) += 1;
    }
    for (index, recommendation) in recommendations.iter().enumerate() {
        let expected_rank = i64::try_from(index + 1)?;
        let route = recommendation["route"]
            .as_str()
            .context("global recommendation omitted Route")?;
        let market_id = recommendation["market_id"]
            .as_str()
            .context("global recommendation omitted market_id")?;
        let expected_route = universe
            .routes_by_market
            .iter()
            .find_map(|(expected_market, expected_route)| {
                (expected_market.as_str() == market_id).then_some(*expected_route)
            })
            .with_context(|| format!("unexpected recommendation market {market_id}"))?;
        ensure!(
            recommendation["rank"].as_i64() == Some(expected_rank)
                && universe
                    .market_ids
                    .iter()
                    .any(|candidate| candidate.as_str() == market_id)
                && route == expected_route.as_str()
                && recommendation["economic_tier"]["route"] == route,
            "global recommendation rank/Route/market lineage is inconsistent: market_id={market_id} route={route} expected_rank={expected_rank} diagnostics_artifact={}",
            artifact_path.display()
        );
        ensure!(
            matches!(route, "crypto" | "weather"),
            "mixed-Route report published unexpected Route {route}: diagnostics_artifact={}",
            artifact_path.display()
        );
        let economics = &recommendation["economics"];
        for field in [
            "profit_probability_bps",
            "nominal_expected_net_usd",
            "robust_expected_net_usd",
            "max_loss_usd",
            "cvar_contribution_usd",
            "capital_occupancy_usd_hours",
            "marginal_portfolio_value_usd",
        ] {
            json_decimal(&economics[field]).with_context(|| {
                format!("global recommendation has invalid economics field {field}")
            })?;
        }
        ensure!(
            json_decimal(&economics["robust_expected_net_usd"])? > Decimal::ZERO
                && json_decimal(&economics["marginal_portfolio_value_usd"])? > Decimal::ZERO,
            "selected recommendation has no positive robust/marginal portfolio value: market_id={market_id} route={route} diagnostics_artifact={}",
            artifact_path.display()
        );
    }
    ensure!(
        route_counts == expected_route_counts,
        "global recommendations do not span both represented Routes: route_counts={route_counts:?} expected_route_counts={expected_route_counts:?} route_funnels={} market_terminals={} diagnostics_artifact={}",
        compact_route_funnels(diagnostics),
        compact_market_terminals(funnel_markets),
        artifact_path.display()
    );
    let routes = diagnostics["data"]["routes"]
        .as_array()
        .context("mixed-Route diagnostics omitted routes")?;
    ensure!(
        routes.len() == 3
            && routes.iter().any(|route| {
                route["route"] == "pooled" && route["outcome"] == "zero_candidates"
            })
            && routes
                .iter()
                .any(|route| { route["route"] == "crypto" && route["outcome"] == "ready" })
            && routes
                .iter()
                .any(|route| { route["route"] == "weather" && route["outcome"] == "ready" })
            && funnel["data"]["conserved"] == true,
        "mixed-Route report diagnostics/funnel are incomplete: route_funnels={} conserved={} diagnostics_artifact={}",
        compact_route_funnels(diagnostics),
        funnel["data"]["conserved"],
        artifact_path.display()
    );
    Ok(())
}

fn compact_route_funnels(diagnostics: &JsonValue) -> JsonValue {
    JsonValue::Array(
        diagnostics["data"]["routes"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|route| {
                json!({
                    "route": route["route"],
                    "outcome": route["outcome"],
                    "funnel": route["funnel"],
                })
            })
            .collect(),
    )
}

fn compact_market_terminals(funnel_markets: &JsonValue) -> JsonValue {
    JsonValue::Array(
        funnel_markets["data"]["items"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|market| {
                json!({
                    "market_id": market["market_id"],
                    "route": market["route"],
                    "terminal_stage": market["terminal_stage"],
                    "primary_reason": market["primary_reason"],
                    "diagnostic_kind": market["secondary_diagnostics"]["kind"],
                })
            })
            .collect(),
    )
}

fn json_decimal(value: &JsonValue) -> Result<Decimal> {
    let text = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_number().map(ToString::to_string))
        .context("decimal JSON value is neither a string nor a number")?;
    text.parse::<Decimal>()
        .with_context(|| format!("parse Decimal value `{text}`"))
}

async fn decode_http_json(
    response: Response,
    expected: StatusCode,
    operation: &str,
) -> Result<JsonValue> {
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("read {operation} response"))?;
    let payload = serde_json::from_str::<JsonValue>(&body)
        .with_context(|| format!("decode {operation} response: {body}"))?;
    ensure!(
        status == expected,
        "{operation} returned {status}, expected {expected}: {payload}"
    );
    Ok(payload)
}

async fn decode_bounded_http_json(
    request: RequestBuilder,
    expected: StatusCode,
    operation: &'static str,
    budget: Duration,
) -> Result<JsonValue> {
    timeout(budget, async {
        let response = request
            .send()
            .await
            .with_context(|| format!("send {operation} request"))?;
        decode_http_json(response, expected, operation).await
    })
    .await
    .with_context(|| format!("{operation} exceeded HTTP budget {budget:?}"))?
}

async fn send_bounded(
    request: RequestBuilder,
    operation: &'static str,
    budget: Duration,
) -> Result<Response> {
    timeout(budget, request.send())
        .await
        .with_context(|| format!("{operation} exceeded HTTP budget {budget:?}"))?
        .with_context(|| format!("send {operation} request"))
}

async fn await_recovery_point(
    db: &DatabaseConnection,
    cycle_id: FeedbackCycleId,
) -> Result<ResearchJobId> {
    let deadline = Instant::now() + RECOVERY_POINT_TIMEOUT;
    let cycles = PgFeedbackCycleRepository::new(db.clone());
    loop {
        let events = cycles.list_stage_events(&cycle_id).await?;
        if let Some(job_id) = events.iter().rev().find_map(|event| {
            (event.stage == FeedbackStage::Attribution
                && event.event_kind == FeedbackStageEventKind::Started)
                .then_some(event.research_job_id)
                .flatten()
        }) {
            return Ok(job_id);
        }
        let cycle = cycles
            .find_cycle(&cycle_id)
            .await?
            .with_context(|| format!("recovery closure cycle {cycle_id} disappeared"))?;
        ensure!(
            !cycle.status.is_terminal(),
            "recovery closure cycle terminated before the Attribution crash point: status={:?} decision={:?} reason={:?}",
            cycle.status,
            cycle.decision,
            cycle.terminal_reason_code
        );
        ensure!(
            Instant::now() < deadline,
            "recovery closure cycle {cycle_id} did not start Attribution within {RECOVERY_POINT_TIMEOUT:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn await_lease_recovery(
    db: &DatabaseConnection,
    cycle_id: FeedbackCycleId,
    job_id: ResearchJobId,
) -> Result<()> {
    let deadline = Instant::now() + LEASE_RECOVERY_TIMEOUT;
    let cycles = PgFeedbackCycleRepository::new(db.clone());
    loop {
        let events = cycles.list_stage_events(&cycle_id).await?;
        if events.iter().any(|event| {
            event.stage == FeedbackStage::Attribution
                && event.research_job_id == Some(job_id)
                && event.event_kind == FeedbackStageEventKind::LeaseRecovered
        }) {
            ensure!(
                cycles.find_coordinator_fault(&cycle_id).await?.is_none(),
                "recovered closure cycle persisted a coordinator fault"
            );
            return Ok(());
        }
        let cycle = cycles
            .find_cycle(&cycle_id)
            .await?
            .with_context(|| format!("recovery closure cycle {cycle_id} disappeared"))?;
        ensure!(
            !cycle.status.is_terminal(),
            "recovery closure cycle terminated before lease recovery: status={:?} decision={:?} reason={:?}",
            cycle.status,
            cycle.decision,
            cycle.terminal_reason_code
        );
        ensure!(
            Instant::now() < deadline,
            "recovery closure cycle {cycle_id} did not recover Attribution job {job_id} within {LEASE_RECOVERY_TIMEOUT:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

fn fixture_history_evidence(
    fixture: ProductionStackFixture,
    polygon: &DeterministicPolygonChain,
) -> Result<FinalizedExecutionEvidence> {
    if !fixture.history_enabled() {
        return Ok(FinalizedExecutionEvidence::runtime(false, None, None));
    }
    let head = polygon.head();
    let accepted_block = head
        .block_number
        .checked_sub(MODEL_CONFIRMATION_BLOCKS)
        .context("deterministic Polygon head is below the confirmation policy")?;
    let accepted = DeterministicPolygonChain::block(accepted_block, head)
        .context("deterministic accepted block is unavailable")?;
    let accepted_at = DateTime::from_timestamp(accepted.timestamp, 0)
        .context("deterministic accepted timestamp is outside UTC")?;
    ensure!(
        accepted_at <= Utc::now(),
        "deterministic history evidence cannot claim a future serving head"
    );
    Ok(FinalizedExecutionEvidence::runtime(
        true,
        Some(accepted_block),
        Some(accepted_at),
    ))
}

async fn seed_browser_fixture(
    db: &DatabaseConnection,
    clickhouse_config: &ClickHouseConfig,
    runtime_artifact_store: &Arc<dyn ArtifactStore>,
    fixture: ProductionStackFixture,
    report_resolves_at: DateTime<Utc>,
    runtime_finalized_execution_evidence: FinalizedExecutionEvidence,
    polygon: &Arc<DeterministicPolygonChain>,
) -> Result<BrowserFixtureEvidence> {
    let (mut infra, research) =
        Box::pin(fixture.seed_research_fixture(db, runtime_artifact_store)).await?;
    if let Some(calibration_preset) = fixture.calibration_preset() {
        infra = Box::pin(finalize_feedback_portfolio(
            db,
            runtime_artifact_store,
            infra,
            research.model_version_id,
            research.evaluation_dataset_id,
            fixture.book_timing()?,
            calibration_preset,
        ))
        .await?;
    }
    println!(
        "browser research fixture: model_version_id={} evaluation_dataset_id={} backtest_report_id={} feedback_cycle_id={} governed_cancellation_cycle_id={:?} cancellable_research_job_id={}",
        research.model_version_id,
        research.evaluation_dataset_id,
        research.backtest_report_id,
        research.feedback_cycle_id,
        research.governed_cancellation_cycle_id,
        research.cancellable_research_job_id,
    );
    let closure = Box::pin(seed_optional_closure(OptionalClosureSeed {
        db,
        clickhouse_config,
        runtime_artifact_store,
        infra: &infra,
        model_version_id: research.model_version_id,
        historical_feedback_cycle_id: research.feedback_cycle_id,
        fixture,
        report_resolves_at,
        runtime_finalized_execution_evidence,
        polygon,
    }))
    .await?;
    if closure.is_some() || fixture == ProductionStackFixture::GovernedFeedback {
        pause_feedback_schedulers(db).await?;
    }
    let cancellation_claim = fixture
        .claim_cancellation_cycle(db, research.governed_cancellation_cycle_id)
        .await?;
    if matches!(
        fixture,
        ProductionStackFixture::FeedbackClosure | ProductionStackFixture::FeedbackClosureRecovery
    ) {
        verify_browser_artifacts(db, runtime_artifact_store).await?;
        return Ok(BrowserFixtureEvidence {
            closure,
            cancellation_claim,
            sampled_parity_report_id: None,
            await_settlement_discovery: false,
        });
    }
    enable_test_admission(db, "browser-e2e-fixture").await;
    if matches!(
        fixture,
        ProductionStackFixture::Browser | ProductionStackFixture::GovernedFeedback
    ) {
        // These fixtures have no later execution-log registrations. Freeze the
        // empty log set before its first attestation; the chain head still advances.
        polygon.freeze();
    }
    let history_head = Box::pin(HistoryUpstreams::serving_head(
        db,
        clickhouse_config,
        polygon,
    ))
    .await?;
    let settlement_report = Box::pin(seed_production_report(
        db,
        clickhouse_config,
        runtime_artifact_store,
        &infra,
        ProductionReportSeed {
            history_head: history_head.clone(),
            account: fixture
                .browser_equity(db, BrowserAccountStage::BeforeEntry)
                .await?,
            catalog: ReportSeedConfig {
                event_id: "browser-settlement-event".to_owned(),
                market_id: BROWSER_SETTLEMENT_MARKET_ID.to_owned(),
                market_question: "Will the browser settlement fixture resolve?".to_owned(),
                market_slug: "browser-settlement-fixture".to_owned(),
                token_id: BROWSER_SETTLEMENT_TOKEN_ID.to_owned(),
                trigger_key: format!(
                    "scheduled:browser-settlement:{}",
                    RecommendationReportId::from_v7()
                ),
            },
        },
    ))
    .await?;
    let settlement_intent = seed_approved_intent(db, &settlement_report).await;
    fill_entry_lot(
        db,
        &PgExecutionSubmissionRepository::new(db.clone()),
        &settlement_report,
        &settlement_intent,
    )
    .await;
    let market = MarketEntity::find_by_id(MarketId::new(&settlement_report.market))
        .one(db)
        .await
        .context("load browser settlement market")?
        .context("browser settlement market is missing")?;
    let mut active_market = market.into_active_model();
    active_market.status = ActiveValue::Set(MarketStatus::Settled);
    active_market.outcome = ActiveValue::Set(Some("Yes".to_owned()));
    active_market.resolved_at = ActiveValue::Set(Some(Utc::now()));
    active_market.content_hash = ActiveValue::Set(ContentHash::from_bytes([0x96; 32]));
    active_market
        .update(db)
        .await
        .context("mark browser settlement market resolved")?;

    // Publish the parity-containment fixture last so the settlement report
    // cannot supersede its still-pending intent before the real worker
    // atomically revokes it.
    let parity_infra = Box::pin(seed_demo_with_store(db, runtime_artifact_store)).await;
    // Keep the demo research object, but bind the report to the actual champion
    // already frozen in the active policy. Missing serving evidence remains the
    // deliberate containment fault, never a fictitious model/source identity.
    infra.feature_parity_state_id = parity_infra.feature_parity_state_id;
    let report = Box::pin(seed_production_report(
        db,
        clickhouse_config,
        runtime_artifact_store,
        &infra,
        ProductionReportSeed {
            history_head,
            account: fixture
                .browser_equity(db, BrowserAccountStage::SettledHolding)
                .await?,
            catalog: ReportSeedConfig {
                event_id: "evt-1".to_owned(),
                market_id: BROWSER_MARKET_ID.to_owned(),
                market_question: "Will it?".to_owned(),
                market_slug: "will-it".to_owned(),
                token_id: BROWSER_TOKEN_ID.to_owned(),
                trigger_key: format!("scheduled:test:{}", RecommendationReportId::from_v7()),
            },
        },
    ))
    .await?;
    seed_pending_intent(db, &report).await;
    verify_browser_artifacts(db, runtime_artifact_store).await?;
    Ok(BrowserFixtureEvidence {
        closure,
        cancellation_claim,
        sampled_parity_report_id: Some(report.report),
        await_settlement_discovery: true,
    })
}

/// Compact deterministic seed parts before the production binary starts so
/// runtime merges begin from a bounded, stable part set.
async fn settle_closure_clickhouse(config: &ClickHouseConfig) -> Result<()> {
    let pool = ClickHousePool::connect(config)
        .await
        .context("connect ClickHouse for closure part settlement")?;
    for table in CLOSURE_MERGE_TABLES {
        let statement =
            format!("OPTIMIZE TABLE `{table}` FINAL SETTINGS optimize_throw_if_noop = 0");
        timeout(
            Duration::from_millis(config.io.maintenance_timeout_ms),
            pool.client().query(&statement).execute(),
        )
        .await
        .with_context(|| format!("compact closure fixture table {table} deadline"))?
        .with_context(|| format!("compact closure fixture table {table}"))?;
        let active_parts =
            ClickHouseQueryLimits::new("ch.system_test.closure_active_parts.v1", 1, 64)
                .query(
                    &pool,
                    "SELECT count() FROM system.parts \
                 WHERE active AND database = currentDatabase() AND table = ?",
                )
                .bind(*table)
                .fetch_one::<u64>()
                .await
                .with_context(|| format!("count settled closure parts for {table}"))?;
        let active_merges =
            ClickHouseQueryLimits::new("ch.system_test.closure_active_merges.v1", 1, 64)
                .query(
                    &pool,
                    "SELECT count() FROM system.merges \
                 WHERE database = currentDatabase() AND table = ?",
                )
                .bind(*table)
                .fetch_one::<u64>()
                .await
                .with_context(|| format!("count active closure merges for {table}"))?;
        ensure!(
            active_merges == 0,
            "closure fixture table {table} still has {active_merges} active merges after OPTIMIZE FINAL"
        );
        println!(
            "production-stack ClickHouse seed settled: table={table} active_parts={active_parts} active_merges=0"
        );
    }
    Ok(())
}

async fn pause_feedback_schedulers(db: &DatabaseConnection) -> Result<()> {
    let repository = PgFeedbackSchedulerRepository::new(db.clone());
    let database_now = Utc::now();
    for profile in builtin_research_profiles().map_err(AnyhowError::msg)? {
        let state = repository
            .sync_state(NewFeedbackSchedulerState::try_new(&profile, database_now)?)
            .await?;
        let paused = repository
            .apply_control(FeedbackSchedulerControl {
                research_profile_id: profile.profile_ref.id,
                expected_pause_revision: state.pause_revision,
                pause: true,
                reason_code: "production_stack_closure_scope".to_owned(),
                note: "The deterministic feedback-closure fixture owns one manually frozen cycle; autonomous profile scheduling is outside this production DAG run.".to_owned(),
            })
            .await?;
        ensure!(
            paused.paused,
            "feedback scheduler {} did not enter the closure fixture pause state",
            paused.research_profile_id
        );
    }
    Ok(())
}

struct OptionalClosureSeed<'a> {
    db: &'a DatabaseConnection,
    clickhouse_config: &'a ClickHouseConfig,
    runtime_artifact_store: &'a Arc<dyn ArtifactStore>,
    infra: &'a SharedDemoInfra,
    model_version_id: ModelVersionId,
    historical_feedback_cycle_id: FeedbackCycleId,
    fixture: ProductionStackFixture,
    report_resolves_at: DateTime<Utc>,
    runtime_finalized_execution_evidence: FinalizedExecutionEvidence,
    polygon: &'a Arc<DeterministicPolygonChain>,
}

async fn seed_optional_closure(
    input: OptionalClosureSeed<'_>,
) -> Result<Option<FeedbackClosureFixture>> {
    let OptionalClosureSeed {
        db,
        clickhouse_config,
        runtime_artifact_store,
        infra,
        model_version_id,
        historical_feedback_cycle_id,
        fixture,
        report_resolves_at,
        runtime_finalized_execution_evidence,
        polygon,
    } = input;
    if !matches!(
        fixture,
        ProductionStackFixture::FeedbackClosure | ProductionStackFixture::FeedbackClosureRecovery
    ) {
        return Ok(None);
    }
    Box::pin(seed_feedback_closure(FeedbackClosureSeedRequest {
        db,
        clickhouse_config,
        artifact_store: runtime_artifact_store,
        infra,
        champion_model_version_id: model_version_id,
        historical_feedback_cycle_id,
        report_resolves_at,
        runtime_finalized_execution_evidence,
        polygon,
    }))
    .await
    .map(Some)
}

async fn verify_browser_artifacts(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
) -> Result<()> {
    let versions = PgModelRegistryRepository::new(db.clone())
        .page_versions(ModelVersionListQuery {
            page: PageRequest::new(1, PageRequest::MAX_SIZE),
            ..ModelVersionListQuery::default()
        })
        .await
        .context("list browser fixture model versions")?;
    ensure!(
        !versions.has_next,
        "browser fixture exceeds the bounded model-artifact verification window"
    );
    ensure!(
        !versions.items.is_empty(),
        "browser fixture must expose at least one model version"
    );
    for version in &versions.items {
        ModelArtifact::load_verified(artifact_store.as_ref(), version)
            .await
            .with_context(|| {
                format!(
                    "verify browser fixture model artifact {}",
                    version.model_version_id
                )
            })?;
    }
    Ok(())
}

async fn await_browser_settlement_discovery(db: &DatabaseConnection) -> Result<()> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        if let Some(redeem) = QuantSettlementRedeemEntity::find()
            .one(db)
            .await
            .context("read browser settlement case")?
        {
            if redeem.effective_policy != SettlementEffectivePolicy::ManualOnly {
                bail!(
                    "browser settlement fixture must remain ManualOnly, got {}",
                    redeem.effective_policy
                );
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("production settlement discovery did not create the browser fixture case");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn await_sampled_parity_containment(
    db: &DatabaseConnection,
    report_id: &RecommendationReportId,
) -> Result<()> {
    let start_deadline = Instant::now() + SAMPLED_PARITY_START_TIMEOUT;
    let mut containment_deadline = None;
    let parity = PgFeatureParityRepository::new(db.clone());
    loop {
        let run = FeatureParityRunEntity::find()
            .filter(FeatureParityRunColumn::Kind.eq(FeatureParityRunKind::Sampled))
            .filter(FeatureParityRunColumn::ReportId.eq(*report_id))
            .order_by_desc(FeatureParityRunColumn::CreatedAt)
            .one(db)
            .await
            .context("read initial automatic feature-parity run")?;
        if let Some(run) = run.as_ref() {
            if run.containment_completed_at.is_some() {
                return Ok(());
            }
            if run.status == FeatureParityRunStatus::Passed {
                bail!(
                    "sampled feature-parity run {} passed although this no-serving-evidence fixture requires fail-closed containment",
                    run.run_id,
                );
            }
            if matches!(
                run.status,
                FeatureParityRunStatus::Mismatched | FeatureParityRunStatus::Failed
            ) {
                let latch = parity
                    .current_state()
                    .await?
                    .context("terminal unsafe parity run has no fail-closed latch generation")?;
                ensure!(
                    latch.state == FeatureParityLatchState::Open
                        && latch.cause_run_id == Some(run.run_id),
                    "terminal unsafe parity run {} became visible without its atomic latch generation; latch_state={} latch_cause={:?}",
                    run.run_id,
                    latch.state.as_str(),
                    latch.cause_run_id,
                );
                containment_deadline
                    .get_or_insert_with(|| Instant::now() + SAMPLED_PARITY_CONTAINMENT_TIMEOUT);
            }
        }
        if run.as_ref().is_some_and(|run| run.pending_since.is_some())
            && containment_deadline.is_none()
        {
            containment_deadline = Some(Instant::now() + SAMPLED_PARITY_CONTAINMENT_TIMEOUT);
        }
        let now = Instant::now();
        if containment_deadline.is_none() && now >= start_deadline {
            let state = run.as_ref().map_or_else(
                || "run_missing".to_owned(),
                |run| {
                    format!(
                        "run_id={} status={} pending_since={:?}",
                        run.run_id,
                        run.status.as_str(),
                        run.pending_since,
                    )
                },
            );
            bail!(
                "sampled feature-parity run did not establish a durable evidence wait within \
                 {SAMPLED_PARITY_START_TIMEOUT:?}; another pending run may be blocking the \
                 serialized worker: {state}"
            );
        }
        if containment_deadline.is_some_and(|deadline| now >= deadline) {
            let state = run.as_ref().map_or_else(
                || "run_missing".to_owned(),
                |run| {
                    format!(
                        "run_id={} status={} pending_since={:?} failure_code={:?} failure_detail={:?}",
                        run.run_id,
                        run.status.as_str(),
                        run.pending_since,
                        run.failure_code,
                        run.failure_detail,
                    )
                },
            );
            bail!(
                "sampled feature-parity containment did not settle within \
                 {SAMPLED_PARITY_CONTAINMENT_TIMEOUT:?} after pending materialization: {state}"
            );
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

const RUNTIME_PARITY_REPORT_LIMIT: usize = 4_096;
const RUNTIME_PARITY_RUN_LIMIT: usize = 8_192;

#[derive(Debug, Clone, PartialEq, Eq, FromQueryResult)]
struct RuntimeParityReportScope {
    recommendation_report_id: RecommendationReportId,
    report_run_id: ReportRunId,
    decision_at: DateTime<Utc>,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeParityRunScope {
    run_id: FeatureParityRunId,
    kind: FeatureParityRunKind,
    report_id: Option<RecommendationReportId>,
    model_version_id: Option<ModelVersionId>,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    created_at: DateTime<Utc>,
}

impl From<&FeatureParityRunModel> for RuntimeParityRunScope {
    fn from(run: &FeatureParityRunModel) -> Self {
        Self {
            run_id: run.run_id,
            kind: run.kind,
            report_id: run.report_id,
            model_version_id: run.model_version_id,
            window_start: run.window_start,
            window_end: run.window_end,
            created_at: run.created_at,
        }
    }
}

/// The closure owns a fresh database. Freeze its complete pre-DAG report and
/// runtime-parity populations; lifecycle status never determines membership.
struct RuntimeParityTarget {
    reports: Vec<RuntimeParityReportScope>,
    runs: Vec<RuntimeParityRunScope>,
}

impl RuntimeParityTarget {
    async fn freeze(db: &DatabaseConnection) -> Result<Self> {
        let (reports, runs) = Self::read_population(db).await?;
        Self::from_population(reports, &runs)
    }

    fn from_population(
        reports: Vec<RuntimeParityReportScope>,
        runs: &[FeatureParityRunModel],
    ) -> Result<Self> {
        ensure!(
            !reports.is_empty()
                && reports.len() <= RUNTIME_PARITY_REPORT_LIMIT
                && runs.len() <= RUNTIME_PARITY_RUN_LIMIT,
            "runtime parity fixture population is empty or exceeds its bounded report/run limits"
        );
        let reports_by_id = reports
            .iter()
            .map(|report| (report.recommendation_report_id, report))
            .collect::<HashMap<_, _>>();
        ensure!(
            reports_by_id.len() == reports.len(),
            "runtime parity fixture repeats a report identity"
        );
        let mut sampled = HashMap::new();
        let mut run_ids = HashSet::new();
        for run in runs {
            ensure!(
                run_ids.insert(run.run_id) && run.training_dataset_id.is_none(),
                "runtime parity fixture has a duplicate or dataset-scoped run {}",
                run.run_id
            );
            if let Some(report_id) = run.report_id {
                ensure!(
                    reports_by_id.contains_key(&report_id),
                    "runtime parity run {} references a report outside the frozen fixture",
                    run.run_id
                );
            }
            if run.kind == FeatureParityRunKind::Sampled {
                let report_id = run.report_id.with_context(|| {
                    format!("mandatory sampled parity {} has no report", run.run_id)
                })?;
                let report = reports_by_id
                    .get(&report_id)
                    .context("sampled parity report is absent")?;
                ensure!(
                    run.model_version_id.is_none()
                        && run.window_start == report.decision_at
                        && run.window_end > run.window_start,
                    "mandatory sampled parity {} differs from its exact global report scope",
                    run.run_id
                );
                ensure!(
                    sampled.insert(report_id, run.run_id).is_none(),
                    "report {report_id} has duplicate mandatory sampled parity runs"
                );
            }
            ensure!(
                !matches!(
                    run.status,
                    FeatureParityRunStatus::Failed | FeatureParityRunStatus::Mismatched
                ),
                "runtime parity run {} failed closed before activation: status={} mismatch_count={} code={:?} detail={:?}",
                run.run_id,
                run.status.as_str(),
                run.mismatched_count,
                run.failure_code,
                run.failure_detail
            );
        }
        ensure!(
            sampled.len() == reports.len(),
            "runtime parity fixture omitted mandatory sampled evidence: reports={} sampled={}",
            reports.len(),
            sampled.len()
        );
        Ok(Self {
            reports,
            runs: runs.iter().map(RuntimeParityRunScope::from).collect(),
        })
    }

    async fn read_population(
        db: &DatabaseConnection,
    ) -> Result<(Vec<RuntimeParityReportScope>, Vec<FeatureParityRunModel>)> {
        let snapshot = db
            .begin_with_config(
                Some(IsolationLevel::RepeatableRead),
                Some(AccessMode::ReadOnly),
            )
            .await?;
        let reports = RecommendationReportEntity::find()
            .select_only()
            .columns([
                RecommendationReportColumn::RecommendationReportId,
                RecommendationReportColumn::ReportRunId,
                RecommendationReportColumn::DecisionAt,
                RecommendationReportColumn::DecisionPolicySnapshotId,
                RecommendationReportColumn::CreatedAt,
            ])
            .order_by_asc(RecommendationReportColumn::RecommendationReportId)
            .limit(u64::try_from(RUNTIME_PARITY_REPORT_LIMIT + 1)?)
            .into_model::<RuntimeParityReportScope>()
            .all(&snapshot)
            .await
            .context("read complete owned fixture report population")?;
        let runs = FeatureParityRunEntity::find()
            .filter(FeatureParityRunColumn::TrainingDatasetId.is_null())
            .order_by_asc(FeatureParityRunColumn::RunId)
            .limit(u64::try_from(RUNTIME_PARITY_RUN_LIMIT + 1)?)
            .all(&snapshot)
            .await
            .context("read complete owned runtime parity population")?;
        snapshot.commit().await?;
        Ok((reports, runs))
    }

    async fn current_runs(&self, db: &DatabaseConnection) -> Result<Vec<FeatureParityRunModel>> {
        let (reports, runs) = Self::read_population(db).await?;
        self.validate_population(reports, &runs)?;
        Ok(runs)
    }

    fn validate_population(
        &self,
        reports: Vec<RuntimeParityReportScope>,
        runs: &[FeatureParityRunModel],
    ) -> Result<()> {
        let observed = Self::from_population(reports, runs)?;
        ensure!(
            observed.reports == self.reports && observed.runs == self.runs,
            "runtime parity fixture report/run population changed after its pre-DAG freeze"
        );
        Ok(())
    }

    async fn await_completion(
        &self,
        db: &DatabaseConnection,
    ) -> Result<Vec<RuntimeParityEvidence>> {
        let deadline = Instant::now() + RUNTIME_PARITY_COMPLETION_TIMEOUT;
        let parity = PgFeatureParityRepository::new(db.clone());
        loop {
            let runs = self.current_runs(db).await?;
            if runs
                .iter()
                .all(|run| run.status == FeatureParityRunStatus::Passed)
            {
                let latch = parity
                    .current_state()
                    .await?
                    .context("passed runtime population has no durable latch generation")?;
                ensure!(
                    latch.state == FeatureParityLatchState::Clear,
                    "complete runtime parity population passed but the global latch is {}",
                    latch.state.as_str()
                );
                return runs
                    .iter()
                    .map(|run| RuntimeParityEvidence::from_passed(run, latch.state_id))
                    .collect();
            }
            ensure!(
                Instant::now() < deadline,
                "runtime parity fixture population did not settle within {RUNTIME_PARITY_COMPLETION_TIMEOUT:?}; pending={} total={}",
                runs.iter()
                    .filter(|run| run.status != FeatureParityRunStatus::Passed)
                    .count(),
                runs.len()
            );
            sleep(POLL_INTERVAL).await;
        }
    }
}

impl RuntimeParityEvidence {
    fn from_passed(
        run: &FeatureParityRunModel,
        latch_state_id: FeatureParityStateId,
    ) -> Result<Self> {
        ensure!(
            run.status == FeatureParityRunStatus::Passed
                && run.total_count > 0
                && run.compared_count == run.total_count
                && run.matched_count == run.total_count
                && run.mismatched_count == 0
                && run.pending_materialization_count == 0
                && run.failure_code.is_none()
                && run.failure_detail.is_none(),
            "runtime parity run {} passed without complete exact-match evidence",
            run.run_id
        );
        Ok(Self {
            run_id: run.run_id,
            kind: run.kind,
            report_id: run.report_id,
            total_count: run.total_count,
            compared_count: run.compared_count,
            matched_count: run.matched_count,
            finished_at: run
                .finished_at
                .context("passed runtime parity has no finished_at")?,
            latch_state_id,
        })
    }
}

async fn await_report_parity(
    db: &DatabaseConnection,
    report_id: &RecommendationReportId,
) -> Result<RuntimeParityEvidence> {
    let run = FeatureParityRunEntity::find()
        .filter(FeatureParityRunColumn::Kind.eq(FeatureParityRunKind::Sampled))
        .filter(FeatureParityRunColumn::ReportId.eq(*report_id))
        .one(db)
        .await
        .context("read report-bound sampled parity")?
        .context("committed report has no atomic sampled parity run")?;
    await_runtime_parity(db, run.run_id, Some(*report_id)).await
}

async fn await_runtime_parity(
    db: &DatabaseConnection,
    run_id: FeatureParityRunId,
    expected_report_id: Option<RecommendationReportId>,
) -> Result<RuntimeParityEvidence> {
    let deadline = Instant::now() + RUNTIME_PARITY_COMPLETION_TIMEOUT;
    let parity = PgFeatureParityRepository::new(db.clone());
    loop {
        let run = FeatureParityRunEntity::find_by_id(run_id)
            .one(db)
            .await
            .context("read runtime parity barrier run")?
            .with_context(|| format!("runtime parity run {run_id} disappeared"))?;
        ensure!(
            run.training_dataset_id.is_none() && run.report_id == expected_report_id,
            "runtime parity run {run_id} changed scope while awaiting its barrier"
        );
        match run.status {
            FeatureParityRunStatus::Passed => {
                let finished_at = run
                    .finished_at
                    .context("passed runtime parity has no finished_at")?;
                ensure!(
                    run.total_count > 0
                        && run.compared_count == run.total_count
                        && run.matched_count == run.total_count
                        && run.mismatched_count == 0
                        && run.pending_materialization_count == 0
                        && run.failure_code.is_none()
                        && run.failure_detail.is_none(),
                    "runtime parity run {run_id} passed without complete exact-match evidence"
                );
                let latch = parity
                    .current_state()
                    .await?
                    .context("passed runtime parity has no durable latch generation")?;
                ensure!(
                    latch.state == FeatureParityLatchState::Clear,
                    "runtime parity run {run_id} passed but the global latch is {}",
                    latch.state.as_str()
                );
                return Ok(RuntimeParityEvidence {
                    run_id,
                    kind: run.kind,
                    report_id: run.report_id,
                    total_count: run.total_count,
                    compared_count: run.compared_count,
                    matched_count: run.matched_count,
                    finished_at,
                    latch_state_id: latch.state_id,
                });
            }
            FeatureParityRunStatus::Mismatched | FeatureParityRunStatus::Failed => {
                bail!(
                    "runtime parity run {run_id} failed closed: status={} mismatch_count={} code={:?} detail={:?}",
                    run.status.as_str(),
                    run.mismatched_count,
                    run.failure_code,
                    run.failure_detail,
                );
            }
            FeatureParityRunStatus::Queued
            | FeatureParityRunStatus::Running
            | FeatureParityRunStatus::PendingMaterialization => {}
        }
        ensure!(
            Instant::now() < deadline,
            "runtime parity run {run_id} did not settle within the configured attempt/materialization envelope {RUNTIME_PARITY_COMPLETION_TIMEOUT:?}: status={} pending={}",
            run.status.as_str(),
            run.pending_materialization_count,
        );
        sleep(POLL_INTERVAL).await;
    }
}

impl Workspace {
    fn build() -> Result<Self> {
        let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let metadata_output = Command::new(&cargo)
            .clear_caller_metadata()
            .args(["metadata", "--format-version", "1", "--no-deps"])
            .output()
            .context("resolve workspace and target directories")?;
        if !metadata_output.status.success() {
            bail!(
                "cargo metadata failed: {}",
                String::from_utf8_lossy(&metadata_output.stderr).trim()
            );
        }
        let metadata: CargoMetadata =
            serde_json::from_slice(&metadata_output.stdout).context("decode cargo metadata")?;
        let status = Self::production_build_command(&cargo, &metadata.workspace_root)
            .status()
            .context("build real quant-pivot production binary")?;
        if !status.success() {
            bail!("production binary build failed with {status}");
        }
        let binary_name = if cfg!(windows) {
            "quant-pivot.exe"
        } else {
            "quant-pivot"
        };
        let binary = metadata.target_directory.join("debug").join(binary_name);
        if !binary.is_file() {
            bail!("built production binary is missing: {}", binary.display());
        }
        Ok(Self {
            root: metadata.workspace_root,
            target_directory: metadata.target_directory,
            binary,
        })
    }

    /// Build only the child process. Rebuilding this running launcher would
    /// replace its path without changing the executable bytes already mapped.
    fn production_build_command(cargo: &OsStr, workspace_root: &Path) -> Command {
        let mut command = Command::new(cargo);
        command
            .clear_caller_metadata()
            .args(["build", "-p", "quant-pivot-bin", "--bin", "quant-pivot"])
            .current_dir(workspace_root);
        command
    }
}

fn deterministic_market_by_condition(request: &Request) -> ResponseTemplate {
    let Some(condition_id) = requested_condition(request) else {
        return ResponseTemplate::new(400);
    };
    if condition_id == synthetic_condition_id() {
        return gamma_market_response(
            &condition_id,
            "production-stack-external-event",
            "Crypto",
            &900_001.to_string(),
            &900_002.to_string(),
        );
    }
    if condition_id == BROWSER_MARKET_ID {
        let no_token_id = fixture_no_token_id(&condition_id, BROWSER_TOKEN_ID);
        return gamma_market_response(
            &condition_id,
            "evt-1",
            "Weather",
            BROWSER_TOKEN_ID,
            no_token_id.as_str(),
        );
    }
    let Some(identity) = condition_id.strip_prefix("feedback-closure-") else {
        return ResponseTemplate::new(200).set_body_json(serde_json::json!([]));
    };
    let Some((scope, ordinal)) = identity.rsplit_once("-market-") else {
        return ResponseTemplate::new(200).set_body_json(serde_json::json!([]));
    };
    let Ok(ordinal) = ordinal.parse::<usize>() else {
        return ResponseTemplate::new(200).set_body_json(serde_json::json!([]));
    };
    let Some((yes_base, no_base, category)) = (match scope {
        "training" => Some((710_000_usize, 810_000_usize, "Weather")),
        "calibration" => Some((720_000, 820_000, "Weather")),
        "evaluation" => Some((730_000, 830_000, "Weather")),
        "shadow" => Some((740_000, 840_000, "Weather")),
        "report-crypto" => Some((750_000, 850_000, "Crypto")),
        "report-weather" => Some((760_000, 860_000, "Weather")),
        _ => None,
    }) else {
        return ResponseTemplate::new(200).set_body_json(serde_json::json!([]));
    };
    let event_id = if scope == "shadow" {
        format!(
            "feedback-closure-shadow-event-{}",
            ordinal.saturating_sub(1).div_euclid(5)
        )
    } else {
        format!("feedback-closure-{scope}-event")
    };
    gamma_market_response(
        &condition_id,
        &event_id,
        category,
        &(yes_base + ordinal).to_string(),
        &(no_base + ordinal).to_string(),
    )
}

async fn mount_closure_catalog(
    upstream: &MockServer,
    closure: &FeedbackClosureFixture,
) -> Result<()> {
    let responses = Arc::new(closure.gamma_market_responses()?);
    let gate = closure.gamma_catalog_gate();
    let response_count = responses.len();
    Mock::given(method("GET"))
        .and(path("/markets"))
        .respond_with(move |request: &Request| {
            closure_market_response(request, responses.as_ref(), &gate)
        })
        .with_priority(1)
        .mount(upstream)
        .await;
    ensure!(
        response_count > 0,
        "closure Gamma responder has no condition-id payloads"
    );
    Ok(())
}

fn closure_market_response(
    request: &Request,
    responses: &HashMap<String, JsonValue>,
    gate: &FeedbackGammaCatalogGate,
) -> ResponseTemplate {
    let Some(condition_id) = requested_condition(request) else {
        return ResponseTemplate::new(400);
    };
    if gate.blocks(&condition_id) {
        return ResponseTemplate::new(200).set_body_json(serde_json::json!([]));
    }
    responses.get(&condition_id).map_or_else(
        || deterministic_market_by_condition(request),
        |response| ResponseTemplate::new(200).set_body_json(response),
    )
}

fn requested_condition(request: &Request) -> Option<String> {
    request
        .url
        .query_pairs()
        .find_map(|(name, value)| (name == "condition_ids").then(|| value.into_owned()))
}

fn gamma_market_response(
    condition_id: &str,
    event_id: &str,
    category: &str,
    yes_token_id: &str,
    no_token_id: &str,
) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(serde_json::json!([{
        "conditionId": condition_id,
        "question": format!("Will deterministic market {condition_id} resolve Yes?"),
        "active": true,
        "closed": false,
        "feesEnabled": true,
        "clobTokenIds": [yes_token_id, no_token_id],
        "outcomes": ["Yes", "No"],
        "events": [{
            "id": event_id,
            "tags": [{"label": category, "slug": category.to_ascii_lowercase()}]
        }]
    }]))
}

fn synthetic_condition_id() -> String {
    format!("0x{}", "9".repeat(64))
}

fn synthetic_clob_market_info(_: &Request) -> ResponseTemplate {
    clob_market_info_response(
        &synthetic_condition_id(),
        "900001",
        "900002",
        CENT_ORDER_RULES,
        false,
    )
}

fn deterministic_closure_market_info(request: &Request) -> ResponseTemplate {
    let Some(identity) = request
        .url
        .path()
        .strip_prefix("/clob-markets/feedback-closure-")
    else {
        return ResponseTemplate::new(404);
    };
    let Some((scope, ordinal)) = identity.rsplit_once("-market-") else {
        return ResponseTemplate::new(404);
    };
    let Ok(ordinal) = ordinal.parse::<usize>() else {
        return ResponseTemplate::new(404);
    };
    let Some((yes_base, no_base)) = (match scope {
        "training" => Some((710_000_usize, 810_000_usize)),
        "calibration" => Some((720_000, 820_000)),
        "evaluation" => Some((730_000, 830_000)),
        "shadow" => Some((740_000, 840_000)),
        "report-crypto" => Some((750_000, 850_000)),
        "report-weather" => Some((760_000, 860_000)),
        _ => None,
    }) else {
        return ResponseTemplate::new(404);
    };
    let condition_id = format!("0x{}", blake3::hash(request.url.path().as_bytes()).to_hex());
    let yes_token_id = (yes_base + ordinal).to_string();
    let no_token_id = (no_base + ordinal).to_string();
    clob_market_info_response(
        &condition_id,
        &yes_token_id,
        &no_token_id,
        CLOSURE_ORDER_RULES,
        CLOSURE_NEG_RISK,
    )
}

fn clob_market_info_response(
    condition_id: &str,
    yes_token_id: &str,
    no_token_id: &str,
    rules: PolymarketOrderRules,
    neg_risk: bool,
) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(serde_json::json!({
        "c": condition_id,
        "t": [
            { "t": yes_token_id.to_string(), "o": "Yes" },
            { "t": no_token_id.to_string(), "o": "No" }
        ],
        "mts": rules.tick_size.as_decimal().to_string(),
        "mos": rules.minimum_order_size.inner().to_string(),
        "nr": neg_risk,
        "itode": false,
        "ibce": false,
        "oas": 0,
        "fd": { "r": "0", "e": 1, "to": true },
        "mbf": 0,
        "tbf": 0,
        "rfqe": false
    }))
}

fn deterministic_polygon_rpc(
    request: &Request,
    polygon: &DeterministicPolygonChain,
) -> ResponseTemplate {
    let request = serde_json::from_slice::<JsonRpcRequest>(&request.body);
    let Ok(request) = request else {
        return ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": { "code": -32700, "message": "invalid JSON-RPC request" },
        }));
    };
    let head = polygon.head();
    let result = match request.method.as_str() {
        "eth_chainId" => Ok(serde_json::json!("0x89")),
        "eth_blockNumber" => Ok(serde_json::json!(format!("0x{:x}", head.block_number))),
        "eth_getBlockByNumber" => deterministic_polygon_block(&request.params, head),
        "eth_getBlockByHash" => deterministic_polygon_hash(&request.params, head),
        "eth_getLogs" => deterministic_polygon_logs(&request.params, polygon),
        "eth_getCode" => deterministic_polygon_code(&request.params),
        "eth_getStorageAt" => deterministic_polygon_storage(&request.params),
        "eth_call" => deterministic_polygon_call(&request.params),
        "eth_sendRawTransaction" | "eth_sendTransaction" => {
            Err("fixture_forbids_chain_write".to_owned())
        }
        method => Err(format!("unsupported deterministic method: {method}")),
    };
    let result = match result {
        Ok(result) => result,
        Err(message) => {
            return ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": request.id,
                "error": { "code": -32602, "message": message },
            }));
        }
    };
    ResponseTemplate::new(200).set_body_json(JsonRpcResponse {
        jsonrpc: "2.0",
        id: request.id,
        result,
    })
}

fn deterministic_polygon_code(params: &JsonValue) -> Result<JsonValue, String> {
    let address = params
        .as_array()
        .and_then(|params| params.first())
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "eth_getCode requires an address".to_owned())?
        .to_ascii_lowercase();
    let code = match address.as_str() {
        FUNDER => "0x".to_owned(),
        STANDARD_ADAPTER => format!(
            "0x{}",
            include_str!("../fixtures/polygon-v2/standard-adapter.hex").trim()
        ),
        NEG_RISK_ADAPTER => format!(
            "0x{}",
            include_str!("../fixtures/polygon-v2/neg-risk-adapter.hex").trim()
        ),
        address if address == format!("{:#x}", CTF_EXCHANGE_V2.address) => format!(
            "0x{}",
            include_str!("../fixtures/polygon-v2/ctf-exchange-v2.hex").trim()
        ),
        address if address == format!("{:#x}", NEG_RISK_EXCHANGE_V2.address) => format!(
            "0x{}",
            include_str!("../fixtures/polygon-v2/neg-risk-exchange-v2.hex").trim()
        ),
        COLLATERAL_TOKEN => format!(
            "0x{}",
            include_str!("../fixtures/polygon-v2/collateral-token-proxy.hex").trim()
        ),
        COLLATERAL_IMPLEMENTATION => format!(
            "0x{}",
            include_str!("../fixtures/polygon-v2/collateral-token-implementation.hex").trim()
        ),
        CONDITIONAL_TOKENS | USDC | USDCE | COLLATERAL_VAULT | LEGACY_NEG_RISK_ADAPTER => {
            "0x01".to_owned()
        }
        _ => {
            return Err(format!(
                "unsupported deterministic eth_getCode address: {address}"
            ));
        }
    };
    Ok(serde_json::json!(code))
}

fn deterministic_polygon_call(params: &JsonValue) -> Result<JsonValue, String> {
    let call = params
        .as_array()
        .and_then(|params| params.first())
        .and_then(JsonValue::as_object)
        .ok_or_else(|| "eth_call requires a call object".to_owned())?;
    let to = call
        .get("to")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "eth_call is missing its target".to_owned())?
        .to_ascii_lowercase();
    let input = call
        .get("input")
        .or_else(|| call.get("data"))
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "eth_call is missing calldata".to_owned())?
        .to_ascii_lowercase();
    let selector = input
        .get(..10)
        .ok_or_else(|| "eth_call calldata has no four-byte selector".to_owned())?;
    let adapter = matches!(to.as_str(), STANDARD_ADAPTER | NEG_RISK_ADAPTER);
    let result = match (to.as_str(), selector) {
        (STANDARD_ADAPTER | NEG_RISK_ADAPTER | COLLATERAL_TOKEN, "0x8da5cb5b") => {
            abi_address(CONTRACT_OWNER)
        }
        (STANDARD_ADAPTER | NEG_RISK_ADAPTER, "0x165d1f36") => abi_address(CONDITIONAL_TOKENS),
        (STANDARD_ADAPTER | NEG_RISK_ADAPTER, "0xf5f1f1a7") => abi_address(COLLATERAL_TOKEN),
        (STANDARD_ADAPTER | NEG_RISK_ADAPTER | COLLATERAL_TOKEN, "0x195187e1") => {
            abi_address(USDCE)
        }
        (STANDARD_ADAPTER | NEG_RISK_ADAPTER, "0x2e48152c") => abi_bool(false),
        (COLLATERAL_TOKEN, "0x89a30271") => abi_address(USDC),
        (COLLATERAL_TOKEN, "0x411557d1") => abi_address(COLLATERAL_VAULT),
        (COLLATERAL_TOKEN, "0x514e62fc") | (CONDITIONAL_TOKENS, "0xe985e9c5") => abi_bool(true),
        (NEG_RISK_ADAPTER, "0xf6f88a8d") => abi_address(LEGACY_NEG_RISK_ADAPTER),
        (NEG_RISK_ADAPTER, "0x2d277260") | (LEGACY_NEG_RISK_ADAPTER, "0x7e3b74c3") => {
            abi_address(WRAPPED_COLLATERAL)
        }
        (COLLATERAL_TOKEN | USDCE, "0x313ce567") => abi_u8(6),
        _ => {
            return Err(format!(
                "unsupported deterministic eth_call target={to} selector={selector} adapter={adapter}"
            ));
        }
    };
    Ok(serde_json::json!(result))
}

fn abi_address(address: &str) -> String {
    format!("0x{}{}", "0".repeat(24), address.trim_start_matches("0x"))
}

fn abi_bool(value: bool) -> String {
    abi_u8(u8::from(value))
}

fn abi_u8(value: u8) -> String {
    format!("0x{value:064x}")
}

fn deterministic_polygon_storage(params: &JsonValue) -> Result<JsonValue, String> {
    let address = params
        .as_array()
        .and_then(|params| params.first())
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "eth_getStorageAt requires a target".to_owned())?;
    if !address.eq_ignore_ascii_case(COLLATERAL_TOKEN) {
        return Err(format!(
            "unsupported deterministic eth_getStorageAt target: {address}"
        ));
    }
    let slot = params
        .as_array()
        .and_then(|params| params.get(1))
        .and_then(JsonValue::as_str);
    if slot.is_some_and(|slot| slot.eq_ignore_ascii_case(ERC1967_IMPLEMENTATION_SLOT)) {
        Ok(serde_json::json!(COLLATERAL_IMPLEMENTATION_WORD))
    } else {
        Ok(serde_json::json!(format!("0x{}", "0".repeat(64))))
    }
}

fn deterministic_polygon_block(
    params: &JsonValue,
    head: DeterministicPolygonHead,
) -> Result<JsonValue, String> {
    let requested = params
        .as_array()
        .and_then(|params| params.first())
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "eth_getBlockByNumber requires a block selector".to_owned())?;
    let block_number = if matches!(requested, "finalized" | "latest" | "safe") {
        head.block_number
    } else {
        requested
            .strip_prefix("0x")
            .filter(|value| !value.is_empty())
            .and_then(|value| u64::from_str_radix(value, 16).ok())
            .ok_or_else(|| "eth_getBlockByNumber has a malformed selector".to_owned())?
    };
    let Some(block) = DeterministicPolygonChain::block(block_number, head) else {
        return Ok(JsonValue::Null);
    };
    Ok(serde_json::json!({
        "number": format!("0x{:x}", block.number),
        "hash": block.hash,
        "parentHash": block.parent_hash,
        "sha3Uncles": format!("0x{}", "1d".repeat(32)),
        "miner": format!("0x{}", "22".repeat(20)),
        "stateRoot": format!("0x{}", "33".repeat(32)),
        "transactionsRoot": format!("0x{}", "44".repeat(32)),
        "receiptsRoot": format!("0x{}", "55".repeat(32)),
        "logsBloom": format!("0x{}", "00".repeat(256)),
        "difficulty": "0x0",
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x0",
        "timestamp": format!("0x{:x}", block.timestamp),
        "extraData": "0x",
        "mixHash": format!("0x{}", "66".repeat(32)),
        "nonce": "0x0000000000000000",
        "baseFeePerGas": "0x0",
        "totalDifficulty": "0x0",
        "size": "0x200",
        "transactions": [],
        "uncles": [],
    }))
}

fn deterministic_polygon_hash(
    params: &JsonValue,
    head: DeterministicPolygonHead,
) -> Result<JsonValue, String> {
    let Some(requested) = params
        .as_array()
        .and_then(|params| params.first())
        .and_then(JsonValue::as_str)
    else {
        return Err("eth_getBlockByHash requires a block hash".to_owned());
    };
    let recent_start = head.block_number.saturating_sub(4_096);
    let block_number = if requested.eq_ignore_ascii_case(V2_PRODUCTION_BLOCK_HASH) {
        Some(V2_PRODUCTION_BLOCK)
    } else {
        (recent_start..=head.block_number)
            .find(|number| requested.eq_ignore_ascii_case(&polygon_block_hash(*number)))
    };
    block_number.map_or(Ok(JsonValue::Null), |number| {
        deterministic_polygon_block(&serde_json::json!([format!("0x{number:x}")]), head)
    })
}

fn deterministic_polygon_logs(
    params: &JsonValue,
    polygon: &DeterministicPolygonChain,
) -> Result<JsonValue, String> {
    let filter = params
        .as_array()
        .and_then(|params| params.first())
        .and_then(JsonValue::as_object)
        .ok_or_else(|| "eth_getLogs requires one filter object".to_owned())?;
    if !requests_exchange_history(filter) {
        return Ok(serde_json::json!([]));
    }
    if filter.len() != 4 {
        return Err("eth_getLogs changed its exact filter fields".to_owned());
    }
    let from_block = parse_rpc_block(filter.get("fromBlock"), "fromBlock")?;
    let to_block = parse_rpc_block(filter.get("toBlock"), "toBlock")?;
    let head = polygon.head();
    if from_block > to_block || to_block > head.block_number || to_block - from_block >= 50_000 {
        return Err("eth_getLogs range exceeds the deterministic archive budget".to_owned());
    }
    let addresses = filter
        .get("address")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| "eth_getLogs omitted V2 addresses".to_owned())?
        .iter()
        .map(|value| value.as_str().map(str::to_ascii_lowercase))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| "eth_getLogs contains a malformed address".to_owned())?;
    let expected_addresses =
        [CTF_EXCHANGE_V2, NEG_RISK_EXCHANGE_V2].map(|contract| format!("{:#x}", contract.address));
    if addresses != expected_addresses {
        return Err("eth_getLogs changed V2 contract addresses".to_owned());
    }
    let topics = filter
        .get("topics")
        .and_then(JsonValue::as_array)
        .filter(|topics| topics.len() == 1)
        .and_then(|topics| topics.first())
        .and_then(JsonValue::as_array)
        .ok_or_else(|| "eth_getLogs changed its topic0 shape".to_owned())?
        .iter()
        .map(|value| value.as_str().map(str::to_ascii_lowercase))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| "eth_getLogs contains a malformed topic".to_owned())?;
    let expected_topics = [CTF_EXCHANGE_V2, NEG_RISK_EXCHANGE_V2]
        .into_iter()
        .flat_map(|contract| {
            [
                contract.order_filled_topic,
                contract.orders_matched_topic,
                contract.fee_charged_topic,
            ]
        })
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    if topics != expected_topics {
        return Err("eth_getLogs changed V2 event topics".to_owned());
    }
    let rows = polygon
        .logs_between(from_block, to_block.saturating_add(1))
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "address": format!("{:#x}", row.address),
                "blockNumber": format!("0x{:x}", row.block_number),
                "blockHash": polygon_block_hash(row.block_number),
                "transactionHash": format!("{:#x}", row.transaction_hash),
                "transactionIndex": format!("0x{:x}", row.transaction_index),
                "logIndex": format!("0x{:x}", row.log_index),
                "topics": row.topics
                    .iter()
                    .map(|topic| format!("{topic:#x}"))
                    .collect::<Vec<_>>(),
                "data": format!("0x{}", hex::encode(row.data)),
                "removed": false,
            })
        })
        .collect::<Vec<_>>();
    Ok(serde_json::json!(rows))
}

fn requests_exchange_history(filter: &JsonMap<String, JsonValue>) -> bool {
    let encoded = filter
        .values()
        .map(JsonValue::to_string)
        .collect::<String>()
        .to_ascii_lowercase();
    EXCHANGE_CONTRACTS.iter().any(|contract| {
        encoded.contains(&format!("{:#x}", contract.address))
            || [
                contract.order_filled_topic,
                contract.orders_matched_topic,
                contract.fee_charged_topic,
            ]
            .iter()
            .any(|topic| encoded.contains(&format!("{topic:#x}")))
    })
}

fn parse_rpc_block(value: Option<&JsonValue>, field: &str) -> Result<u64, String> {
    value
        .and_then(JsonValue::as_str)
        .and_then(|value| value.strip_prefix("0x"))
        .filter(|value| !value.is_empty())
        .and_then(|value| u64::from_str_radix(value, 16).ok())
        .ok_or_else(|| format!("eth_getLogs has malformed {field}"))
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        ffi::OsStr,
        fs::{self, File, OpenOptions},
        future::pending,
        path::Path,
        process::Stdio,
        sync::Arc,
        time::{Duration, Instant as StdInstant},
    };

    use super::{
        ARTIFACT_KEY_PREFIX, AccountChainExecutionRole, AccountExecutionOwner, BROWSER_MARKET_ID,
        BROWSER_SETTLEMENT_MARKET_ID, BROWSER_SETTLEMENT_TOKEN_ID, BackendChild, CENT_ORDER_RULES,
        CLOSURE_NEG_RISK, CLOSURE_ORDER_RULES, CLOSURE_REPORT_HORIZON_HOURS, CTF_EXCHANGE_V2,
        Child, Client, ContentHash, DecisionPolicySnapshotId, DeterministicPolygonChain,
        DisposableBoundaryPreimage, DisposableEvidenceBoundary, ENTRY_FILLED_SHARES, ENTRY_PRICE,
        EXECUTION_NOTIONAL, EconomicTierId, EntryAuthorizationPolicy, EvmAddress, FUNDER,
        FeedbackCycleId, GovernedClosureManifest, HISTORICAL_ECONOMIC_TIMEOUT, HYPERSYNC_TOKEN,
        HistoricalEconomicBackfill, HistoricalEconomicCounts, HistoricalEconomicProgress,
        HistoricalEconomicReceipt, HistoricalEconomicSchedule, HistoricalEconomicTarget,
        HistoryUpstreams, JsonMap, MINIO_ACCESS_KEY, MINIO_BUCKET, MINIO_REGION, MINIO_SECRET_KEY,
        MarketId, MinioStaleUploadPolicy, ModelVersionId, NEG_RISK_EXCHANGE_V2, POLL_INTERVAL,
        PRIVATE_KEY, PolymarketOrderRules, ProductionArtifactStack, ProductionStack,
        ProductionStackFixture, ReadinessCaptureEvidence, RecommendationId, RecommendationReportId,
        ReportRouteRunId, ResearchProfileRef, Result, Shares, StatusCode,
        SuccessorEconomicIdentity, TRIGGER_IDENTITY_FIELDS, TokioCommand, TokioTcpListener,
        TradePolicyArtifactId, Uuid, Workspace, closure_market_text, decode_bounded_http_json,
        deterministic_polygon_block, synthetic_condition_id, validate_closure_manifest,
        verify_trigger_replay,
    };
    use anyhow::{Context, bail, ensure};
    use aws_config::BehaviorVersion;
    use aws_sdk_s3::{
        Client as S3ControlClient,
        config::{Builder as S3ConfigBuilder, Credentials, Region},
        error::ProvideErrorMetadata,
        primitives::ByteStream,
        types::{
            AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, ExpirationStatus,
            LifecycleRule, LifecycleRuleFilter,
        },
    };
    use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
    use quant_pivot_api::{
        clob::ClobClient,
        data_api::DataApiClient,
        exchange::{
            execution_projector::project_history,
            history_client::{ExchangeHistoryAttestor, ExchangeHistoryExtractor, chunks_agree},
        },
        keystore::OrderSigner,
        wallet::WalletTopology,
    };
    use quant_pivot_models::{
        config::{
            DataApiConfig, FinalizedExchangeHistoryConfig, PolygonRpcEndpoint, PolymarketConfig,
        },
        domain::quant::ResearchReadinessEvidenceInfo,
        enums::{
            common::Side,
            quant::{ExecutionWalletKind, ResearchReadinessEvidenceKind},
        },
        hashing::CanonicalDigest,
        types::{
            ArtifactUri, ArtifactVersion, AttestationKeyId, Price,
            RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION, ResearchReadinessEvidenceId,
            ResearchReadinessEvidencePayload, ResearchSourceStorageKind, RetentionRunwayEvidenceV1,
            RetentionSourceObservationV1, VenueOrderAmount, minimum_raw_retention_days,
            research_source_registry,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use serde_json::{Value as JsonValue, json};
    use tokio::io::AsyncWriteExt;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_string, method, path, query_param},
    };

    use crate::support::production_history::DETERMINISTIC_POLYGON_BLOCK_SECS;

    mod runtime_parity_tests {
        use std::time::Duration;

        use anyhow::{Context, Error as AnyhowError, Result};
        use chrono::{DateTime, Duration as ChronoDuration, Utc};
        use quant_pivot_models::{
            domain::quant::{CompleteFeatureParityRun, NewFeatureParityRun},
            entities::quant_recommendation_report::Entity as ReportEntity,
            enums::quant::{
                FeatureParityRunKind, FeatureParityRunStatus, RecommendationReportStatus,
            },
            types::{
                ContentHash, DiagnosticCode, FeatureParityRunId, ModelVersionId,
                RecommendationReportId, RoleCode,
            },
        };
        use quant_pivot_repository::{
            postgres::{PgFeatureParityRepository, PgModelRegistryRepository},
            traits::{FeatureParityRepository, ModelRegistryRepository},
        };
        use sea_orm::{DatabaseConnection, EntityTrait};

        use super::super::{RuntimeParityEvidence, RuntimeParityTarget};
        use crate::{
            postgres::{PostgresClock, setup_pg, with_postgres_suite},
            support::{
                economic_outcome_fixtures::seed_report_at,
                execution_pg_seed::seed_shared_demo_infra,
            },
        };

        struct ParityFixture {
            repository: PgFeatureParityRepository,
        }

        impl ParityFixture {
            async fn finish(
                &self,
                run_id: FeatureParityRunId,
                status: FeatureParityRunStatus,
            ) -> Result<()> {
                let run = self
                    .repository
                    .find_run(&run_id)
                    .await?
                    .context("fixture parity run")?;
                self.repository.mark_running(&run_id).await?;
                let failed = status == FeatureParityRunStatus::Failed;
                self.repository
                    .complete_run(
                        &run_id,
                        CompleteFeatureParityRun {
                            status,
                            total_count: i64::from(!failed),
                            compared_count: i64::from(!failed),
                            matched_count: i64::from(status == FeatureParityRunStatus::Passed),
                            mismatched_count: i64::from(
                                status == FeatureParityRunStatus::Mismatched,
                            ),
                            pending_materialization_count: 0,
                            feature_contract_hash: run.feature_contract_hash,
                            transform_hash: Some(ContentHash::from_bytes([7; 32])),
                            failure_code: failed
                                .then(|| DiagnosticCode::new("fixture_capture_hash_mismatch")),
                            failure_detail: failed
                                .then(|| "frozen capture hash differs from replay".to_owned()),
                        },
                    )
                    .await?;
                Ok(())
            }

            async fn dataset_run(
                &self,
                db: &DatabaseConnection,
                model_id: ModelVersionId,
                decision: DateTime<Utc>,
            ) -> Result<FeatureParityRunId> {
                let model = PgModelRegistryRepository::new(db.clone())
                    .find_model_version(&model_id)
                    .await?
                    .context("fixture model")?;
                let dataset = model
                    .training_dataset_id
                    .context("fixture training dataset")?;
                let run = self
                    .repository
                    .create_run(NewFeatureParityRun {
                        run_id: FeatureParityRunId::from_v7(),
                        kind: FeatureParityRunKind::Full,
                        status: FeatureParityRunStatus::Queued,
                        window_start: decision,
                        window_end: decision + ChronoDuration::seconds(10),
                        report_id: None,
                        model_version_id: Some(model_id),
                        training_dataset_id: Some(dataset),
                        triggered_by: "runtime-barrier-test".to_owned(),
                        requested_by: None,
                        acting_role: RoleCode::new("system"),
                        reason: "independent Dataset parity must not alter runtime membership"
                            .to_owned(),
                        total_count: 0,
                        compared_count: 0,
                        matched_count: 0,
                        mismatched_count: 0,
                        pending_materialization_count: 0,
                        feature_contract_hash: ContentHash::from_bytes([7; 32]),
                        transform_hash: None,
                        failure_code: None,
                        failure_detail: None,
                        started_at: None,
                        pending_since: None,
                        containment_completed_at: None,
                        finished_at: None,
                    })
                    .await?;
                Ok(run.run_id)
            }
        }

        impl RuntimeParityTarget {
            async fn assert_invalid_scopes(
                &self,
                db: &DatabaseConnection,
                evidence: &[RuntimeParityEvidence],
            ) -> Result<()> {
                let (reports, runs) = Self::read_population(db).await?;
                let mut missing = runs.clone();
                missing.pop();
                assert!(Self::from_population(reports.clone(), &missing).is_err());
                let mut duplicate = runs.clone();
                duplicate.push(runs[0].clone());
                assert!(Self::from_population(reports.clone(), &duplicate).is_err());
                duplicate.last_mut().context("duplicate run")?.run_id =
                    FeatureParityRunId::from_v7();
                assert!(
                    Self::from_population(reports.clone(), &duplicate).is_err(),
                    "two distinct sampled runs cannot substitute one required report binding"
                );
                let mut foreign = runs.clone();
                foreign[0].report_id = Some(RecommendationReportId::from_v7());
                assert!(Self::from_population(reports.clone(), &foreign).is_err());
                let mut drifted = runs.clone();
                drifted[0].window_end += ChronoDuration::microseconds(1);
                assert!(self.validate_population(reports.clone(), &drifted).is_err());
                let mut incomplete = runs[0].clone();
                incomplete.matched_count = 0;
                assert!(
                    RuntimeParityEvidence::from_passed(&incomplete, evidence[0].latch_state_id)
                        .is_err()
                );
                let mut added_reports = reports.clone();
                let mut added_runs = runs.clone();
                let mut added_report = reports[0].clone();
                added_report.recommendation_report_id = RecommendationReportId::from_v7();
                let mut added_run = runs[0].clone();
                added_run.run_id = FeatureParityRunId::from_v7();
                added_run.report_id = Some(added_report.recommendation_report_id);
                added_reports.push(added_report);
                added_runs.push(added_run);
                assert!(
                    self.validate_population(added_reports, &added_runs)
                        .is_err(),
                    "a new complete report/run pair cannot widen the frozen scope"
                );
                Ok(())
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn completed_population_is_required() -> Result<()> {
            Box::pin(with_postgres_suite(async {
                tokio::time::timeout(
                    Duration::from_mins(5),
                    Box::pin(async {
                        let (pool, _scenario) = setup_pg().await;
                        let db = pool.connection();
                        let infra = Box::pin(seed_shared_demo_infra(db)).await;
                        let decision = db.statement_time().await - ChronoDuration::hours(1);
                        let first = seed_report_at(db, &infra, decision).await?;
                        seed_report_at(db, &infra, decision + ChronoDuration::seconds(1)).await?;
                        let fixture = ParityFixture {
                            repository: PgFeatureParityRepository::new(db.clone()),
                        };
                        let (_, queued) = RuntimeParityTarget::read_population(db).await?;
                        assert_eq!(queued.len(), 2);
                        for run in &queued {
                            fixture
                                .finish(run.run_id, FeatureParityRunStatus::Passed)
                                .await?;
                        }
                        let first_report = ReportEntity::find_by_id(first.report)
                            .one(db)
                            .await?
                            .context("historical report")?;
                        assert_ne!(
                            first_report.status,
                            RecommendationReportStatus::Published,
                            "historical terminal reports must remain in the barrier"
                        );
                        let target = RuntimeParityTarget::freeze(db).await?;
                        let evidence = target.await_completion(db).await?;
                        assert_eq!(
                            evidence.len(),
                            2,
                            "runs completed before freeze cannot disappear"
                        );
                        assert!(evidence.iter().all(RuntimeParityEvidence::matched));
                        target.assert_invalid_scopes(db, &evidence).await?;
                        let dataset_run = fixture
                            .dataset_run(db, infra.model_version_id, decision)
                            .await?;
                        assert_eq!(
                            target.await_completion(db).await?.len(),
                            2,
                            "late Dataset parity is a distinct scope"
                        );
                        fixture
                            .finish(dataset_run, FeatureParityRunStatus::Failed)
                            .await?;
                        let latch_error = target
                            .await_completion(db)
                            .await
                            .err()
                            .context("runtime success cannot override an open global latch")?;
                        assert!(latch_error.to_string().contains("global latch"));
                        Ok::<_, AnyhowError>(())
                    }),
                )
                .await
                .context("bounded pre-activation parity population regression")?
            }))
            .await?
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn prior_failure_blocks_activation() -> Result<()> {
            Box::pin(with_postgres_suite(async {
                tokio::time::timeout(
                    Duration::from_mins(5),
                    Box::pin(async {
                        let (pool, _scenario) = setup_pg().await;
                        let db = pool.connection();
                        let infra = Box::pin(seed_shared_demo_infra(db)).await;
                        let decision = db.statement_time().await - ChronoDuration::hours(1);
                        for offset in 0..3 {
                            seed_report_at(db, &infra, decision + ChronoDuration::seconds(offset))
                                .await?;
                        }
                        let target = RuntimeParityTarget::freeze(db).await?;
                        let (_, runs) = RuntimeParityTarget::read_population(db).await?;
                        assert_eq!(runs.len(), 3);
                        let fixture = ParityFixture {
                            repository: PgFeatureParityRepository::new(db.clone()),
                        };
                        for (run, status) in runs.iter().zip([
                            FeatureParityRunStatus::Passed,
                            FeatureParityRunStatus::Failed,
                            FeatureParityRunStatus::Mismatched,
                        ]) {
                            fixture.finish(run.run_id, status).await?;
                        }
                        let freeze_error = RuntimeParityTarget::freeze(db)
                            .await
                            .err()
                            .context("pre-existing terminal failure must reject freeze")?;
                        assert!(
                            freeze_error
                                .to_string()
                                .contains("failed closed before activation")
                        );
                        let barrier_error =
                            target.await_completion(db).await.err().context(
                                "terminal failure must reject the full frozen population",
                            )?;
                        assert!(
                            barrier_error
                                .to_string()
                                .contains("failed closed before activation")
                        );
                        Ok::<_, AnyhowError>(())
                    }),
                )
                .await
                .context("bounded terminal runtime parity regression")?
            }))
            .await?
        }
    }

    fn spawn_shell_process(log_path: &Path, script: &str) -> Result<Child> {
        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .with_context(|| format!("open child-supervision log {}", log_path.display()))?;
        let stderr = stdout
            .try_clone()
            .with_context(|| format!("clone child-supervision log {}", log_path.display()))?;
        let mut command = TokioCommand::new("/bin/sh");
        command
            .args(["-c", script])
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true);
        command.spawn().context("spawn supervised shell child")
    }

    const S3_LIFECYCLE_RULE_ID: &str = "abort-incomplete-artifact-multipart";
    const S3_MULTIPART_ABORT_DAYS: i32 = 1;
    const S3_LIFECYCLE_XML: &str = concat!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
        "<Rule><ID>abort-incomplete-artifact-multipart</ID>",
        "<Filter><Prefix>artifacts/</Prefix></Filter><Status>Enabled</Status>",
        "<AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation>",
        "</AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>"
    );
    const S3_LIFECYCLE_READBACK_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Rule>
    <ID>abort-incomplete-artifact-multipart</ID>
    <Filter><Prefix>artifacts/</Prefix></Filter>
    <Status>Enabled</Status>
    <AbortIncompleteMultipartUpload>
      <DaysAfterInitiation>1</DaysAfterInitiation>
    </AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>"#;

    #[derive(Clone, Copy)]
    struct S3LifecycleContract {
        rule_id: &'static str,
        prefix: &'static str,
        abort_days: i32,
    }

    impl S3LifecycleContract {
        fn configuration(self) -> Result<BucketLifecycleConfiguration> {
            let rule = LifecycleRule::builder()
                .id(self.rule_id)
                .filter(LifecycleRuleFilter::builder().prefix(self.prefix).build())
                .status(ExpirationStatus::Enabled)
                .abort_incomplete_multipart_upload(
                    AbortIncompleteMultipartUpload::builder()
                        .days_after_initiation(self.abort_days)
                        .build(),
                )
                .build()
                .context("build standard S3 incomplete-multipart lifecycle rule")?;
            BucketLifecycleConfiguration::builder()
                .rules(rule)
                .build()
                .context("build standard S3 bucket lifecycle configuration")
        }

        fn validate(self, rules: &[LifecycleRule]) -> Result<()> {
            ensure!(
                rules.len() == 1,
                "S3 must expose exactly one artifact lifecycle rule, observed {}",
                rules.len()
            );
            let rule = &rules[0];
            let filter = rule
                .filter()
                .context("S3 multipart lifecycle rule has no filter")?;
            let abort = rule
                .abort_incomplete_multipart_upload()
                .context("S3 lifecycle rule does not abort incomplete multipart uploads")?;
            ensure!(
                rule.id() == Some(self.rule_id)
                    && rule.status() == &ExpirationStatus::Enabled
                    && filter.prefix() == Some(self.prefix)
                    && filter.tag().is_none()
                    && filter.object_size_greater_than().is_none()
                    && filter.object_size_less_than().is_none()
                    && filter.and().is_none()
                    && abort.days_after_initiation() == Some(self.abort_days)
                    && rule.expiration().is_none()
                    && rule.transitions.is_none()
                    && rule.noncurrent_version_transitions.is_none()
                    && rule.noncurrent_version_expiration().is_none(),
                "S3 incomplete-multipart lifecycle rule drifted: {rule:?}"
            );
            Ok(())
        }
    }

    const S3_MULTIPART_LIFECYCLE: S3LifecycleContract = S3LifecycleContract {
        rule_id: S3_LIFECYCLE_RULE_ID,
        prefix: ARTIFACT_KEY_PREFIX,
        abort_days: S3_MULTIPART_ABORT_DAYS,
    };

    #[test]
    fn workspace_build_excludes_launcher() {
        let workspace_root = Path::new("/tmp/quant-pivot-workspace");
        let command =
            Workspace::production_build_command(OsStr::new("fixture-cargo"), workspace_root);
        let args = command
            .get_args()
            .map(OsStr::to_string_lossy)
            .collect::<Vec<_>>();

        assert_eq!(command.get_program(), OsStr::new("fixture-cargo"));
        assert_eq!(command.get_current_dir(), Some(workspace_root));
        assert_eq!(
            args,
            ["build", "-p", "quant-pivot-bin", "--bin", "quant-pivot"]
        );
        assert!(!args.iter().any(|arg| arg.contains("quant-pivot-xtask")));
    }

    #[test]
    fn book_timing_is_scoped() -> Result<()> {
        for fixture in [
            ProductionStackFixture::Browser,
            ProductionStackFixture::GovernedFeedback,
        ] {
            assert_eq!(fixture.book_timing()?.max_book_age_ms, 2_000);
        }
        for fixture in [
            ProductionStackFixture::FeedbackClosure,
            ProductionStackFixture::FeedbackClosureRecovery,
        ] {
            assert_eq!(fixture.book_timing()?.max_book_age_ms, 14_110);
        }
        Ok(())
    }

    #[tokio::test]
    async fn child_exit_interrupts_readiness() -> Result<()> {
        let run_dir = env::temp_dir().join(format!("qp-child-supervision-{}", Uuid::now_v7()));
        fs::create_dir_all(&run_dir)
            .with_context(|| format!("create child-supervision run dir {}", run_dir.display()))?;
        let result = async {
            let log_path = run_dir.join("backend.log");
            let stdout = File::create(&log_path)
                .with_context(|| format!("create child-supervision log {}", log_path.display()))?;
            let stderr = stdout
                .try_clone()
                .with_context(|| format!("clone child-supervision log {}", log_path.display()))?;
            let mut command = TokioCommand::new("/bin/sh");
            command
                .args(["-c", "echo deterministic-child-exit; exit 42"])
                .stdin(Stdio::null())
                .stdout(Stdio::from(stdout))
                .stderr(Stdio::from(stderr))
                .kill_on_drop(true);
            let child = command.spawn().context("spawn deterministic exit child")?;
            let child = BackendChild::new(child, log_path);
            let started = StdInstant::now();
            let error = child
                .supervise("deterministic readiness", pending::<Result<()>>())
                .await
                .expect_err("child exit must interrupt a pending readiness future");
            ensure!(
                started.elapsed() < Duration::from_secs(2),
                "child exit supervision was not fail-fast: {:?}",
                started.elapsed()
            );
            let detail = format!("{error:#}");
            ensure!(
                detail.contains("exit status: 42") && detail.contains("deterministic-child-exit"),
                "child-exit diagnostic omitted status or log tail: {detail}"
            );
            Ok(())
        }
        .await;
        let cleanup = fs::remove_dir_all(&run_dir)
            .with_context(|| format!("remove child-supervision run dir {}", run_dir.display()));
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(cleanup)) => Err(cleanup),
            (Err(error), Err(cleanup)) => Err(error.context(format!(
                "child-supervision directory cleanup also failed: {cleanup:#}"
            ))),
        }
    }

    #[tokio::test]
    async fn child_exit_wins_error() -> Result<()> {
        let run_dir = env::temp_dir().join(format!("qp-child-race-{}", Uuid::now_v7()));
        fs::create_dir_all(&run_dir)?;
        let result = async {
            let log_path = run_dir.join("backend.log");
            let child = BackendChild::new(
                spawn_shell_process(&log_path, "echo deterministic-child-race; exit 42")?,
                log_path,
            );
            let shared = Arc::clone(&child.child);
            let mut guard = shared.lock().await;
            let supervised = tokio::spawn(async move {
                child
                    .supervise("phase-error race", async {
                        Err::<(), _>(anyhow::anyhow!(
                            "ordinary phase error must not hide child exit"
                        ))
                    })
                    .await
            });
            let status = guard.wait().await.context("wait for race child")?;
            ensure!(!status.success(), "race child unexpectedly succeeded");
            drop(guard);
            let error = supervised
                .await
                .context("join phase-error race supervisor")?
                .expect_err("child exit must override the concurrent phase error");
            let detail = format!("{error:#}");
            ensure!(
                detail.contains("exit status: 42")
                    && detail.contains("deterministic-child-race")
                    && !detail.contains("ordinary phase error"),
                "child-exit race returned the wrong diagnostic: {detail}"
            );
            Ok(())
        }
        .await;
        let cleanup = fs::remove_dir_all(&run_dir);
        result.and_then(|()| cleanup.context("remove child-race run directory"))
    }

    #[tokio::test]
    async fn planned_exit_observes_status() -> Result<()> {
        let run_dir = env::temp_dir().join(format!("qp-child-planned-{}", Uuid::now_v7()));
        fs::create_dir_all(&run_dir)?;
        let result = async {
            for exit_code in [0, 42] {
                let log_path = run_dir.join(format!("backend-{exit_code}.log"));
                let stdout = File::create(&log_path)?;
                let stderr = stdout.try_clone()?;
                let process = TokioCommand::new("/bin/sh")
                    .arg("-c")
                    .arg(format!(
                        "read signal; echo planned-child-exit; exit {exit_code}"
                    ))
                    .stdin(Stdio::piped())
                    .stdout(Stdio::from(stdout))
                    .stderr(Stdio::from(stderr))
                    .kill_on_drop(true)
                    .spawn()
                    .context("spawn input-controlled planned-exit child")?;
                let child = BackendChild::new(process, log_path);
                let mut running = child.begin_shutdown().await?;
                let mut control = running.stdin.take().context("planned child has no input")?;
                control.write_all(b"finish\n").await?;
                drop(control);
                let status = tokio::time::timeout(Duration::from_secs(2), running.wait())
                    .await
                    .context("planned child did not finish")??;
                drop(running);
                let terminal = child.verify_exit(status);
                ensure!(
                    terminal.is_ok() == (exit_code == 0),
                    "planned exit verification lost the child status: {terminal:?}"
                );
            }
            Ok(())
        }
        .await;
        let cleanup = fs::remove_dir_all(&run_dir).context("remove planned-child run directory");
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(cleanup)) => Err(cleanup),
            (Err(error), Err(cleanup)) => Err(error.context(format!(
                "planned-child directory cleanup also failed: {cleanup:#}"
            ))),
        }
    }

    #[tokio::test]
    async fn premature_exit_rejects_shutdown() -> Result<()> {
        let run_dir = env::temp_dir().join(format!("qp-child-premature-{}", Uuid::now_v7()));
        fs::create_dir_all(&run_dir)?;
        let result = async {
            let log_path = run_dir.join("backend.log");
            let child = BackendChild::new(
                spawn_shell_process(&log_path, "echo premature-success; exit 0")?,
                log_path,
            );
            let status = child.child.lock().await.wait().await?;
            ensure!(status.success(), "fixture child did not exit successfully");
            let Err(error) = child.begin_shutdown().await else {
                bail!("an already-exited child cannot enter planned shutdown");
            };
            let detail = format!("{error:#}");
            ensure!(
                detail.contains("before planned closure shutdown")
                    && detail.contains("exit status: 0")
                    && detail.contains("premature-success"),
                "premature successful exit lost its failure evidence: {detail}"
            );
            Ok(())
        }
        .await;
        let cleanup = fs::remove_dir_all(&run_dir).context("remove premature-child directory");
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(cleanup)) => Err(cleanup),
            (Err(error), Err(cleanup)) => Err(error.context(format!(
                "premature-child directory cleanup also failed: {cleanup:#}"
            ))),
        }
    }

    #[tokio::test]
    async fn replacement_hides_dead_window() -> Result<()> {
        let run_dir = env::temp_dir().join(format!("qp-child-replace-{}", Uuid::now_v7()));
        fs::create_dir_all(&run_dir)?;
        let result = async {
            let log_path = run_dir.join("backend.log");
            let child =
                BackendChild::new(spawn_shell_process(&log_path, "sleep 5")?, log_path.clone());
            let observer_child = child.clone();
            let observer = tokio::spawn(async move {
                observer_child
                    .unexpected_exit("intentional replacement")
                    .await
            });
            tokio::task::yield_now().await;
            let shared = Arc::clone(&child.child);
            let mut guard = shared.lock().await;
            guard.start_kill().context("kill first replacement child")?;
            let _ = guard
                .wait()
                .await
                .context("wait for first replacement child")?;
            *guard = spawn_shell_process(&log_path, "sleep 5")?;
            drop(guard);

            tokio::time::sleep(POLL_INTERVAL.saturating_mul(2)).await;
            ensure!(
                !observer.is_finished(),
                "supervisor observed the lock-protected intentional dead-child window"
            );
            observer.abort();
            let join = observer
                .await
                .expect_err("replacement observer must be aborted");
            ensure!(join.is_cancelled(), "replacement observer did not cancel");
            let mut guard = shared.lock().await;
            guard.start_kill().context("kill replacement child")?;
            let _ = guard.wait().await.context("wait for replacement child")?;
            drop(guard);
            Ok(())
        }
        .await;
        let cleanup = fs::remove_dir_all(&run_dir);
        result.and_then(|()| cleanup.context("remove child-replacement run directory"))
    }

    #[tokio::test]
    async fn readiness_http_is_bounded() -> Result<()> {
        let listener = TokioTcpListener::bind(("127.0.0.1", 0))
            .await
            .context("bind never-response readiness server")?;
        let address = listener
            .local_addr()
            .context("read never-response readiness address")?;
        let server = tokio::spawn(async move {
            let (socket, _) = listener
                .accept()
                .await
                .expect("accept never-response readiness request");
            let _socket = socket;
            pending::<()>().await;
        });
        let http = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .context("build never-response readiness client")?;
        let budget = Duration::from_millis(50);
        let started = StdInstant::now();
        let result = async {
            let error = decode_bounded_http_json(
                http.get(format!("http://{address}/readiness")),
                StatusCode::OK,
                "never-response readiness probe",
                budget,
            )
            .await
            .expect_err("never-response readiness request must time out");
            ensure!(
                started.elapsed() < Duration::from_secs(2),
                "readiness HTTP timeout exceeded its bounded envelope: {:?}",
                started.elapsed()
            );
            ensure!(
                format!("{error:#}").contains("exceeded HTTP budget"),
                "never-response timeout lost its bounded diagnostic: {error:#}"
            );
            Ok(())
        }
        .await;
        server.abort();
        let join = server.await.expect_err("aborted never-response server");
        ensure!(join.is_cancelled(), "never-response server did not cancel");
        result
    }

    #[test]
    fn s3_lifecycle_rejects_drift() -> Result<()> {
        let configuration = S3_MULTIPART_LIFECYCLE.configuration()?;
        S3_MULTIPART_LIFECYCLE.validate(configuration.rules())?;
        let valid = configuration.rules()[0].clone();
        assert!(S3_MULTIPART_LIFECYCLE.validate(&[]).is_err());
        assert!(
            S3_MULTIPART_LIFECYCLE
                .validate(&[valid.clone(), valid.clone()])
                .is_err()
        );

        let mut disabled = valid.clone();
        disabled.status = ExpirationStatus::Disabled;
        assert!(S3_MULTIPART_LIFECYCLE.validate(&[disabled]).is_err());

        let mut wrong_prefix = valid.clone();
        wrong_prefix.filter = Some(LifecycleRuleFilter::builder().prefix("wrong/").build());
        assert!(S3_MULTIPART_LIFECYCLE.validate(&[wrong_prefix]).is_err());

        let mut missing_abort = valid.clone();
        missing_abort.abort_incomplete_multipart_upload = None;
        assert!(S3_MULTIPART_LIFECYCLE.validate(&[missing_abort]).is_err());

        let mut missing_days = valid.clone();
        missing_days.abort_incomplete_multipart_upload = Some(
            AbortIncompleteMultipartUpload::builder()
                .set_days_after_initiation(None)
                .build(),
        );
        assert!(S3_MULTIPART_LIFECYCLE.validate(&[missing_days]).is_err());

        let mut wrong_days = valid;
        wrong_days.abort_incomplete_multipart_upload = Some(
            AbortIncompleteMultipartUpload::builder()
                .days_after_initiation(S3_MULTIPART_ABORT_DAYS + 1)
                .build(),
        );
        assert!(S3_MULTIPART_LIFECYCLE.validate(&[wrong_days]).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn s3_lifecycle_roundtrips() -> Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path(format!("/{MINIO_BUCKET}/")))
            .and(query_param("lifecycle", ""))
            .and(body_string(S3_LIFECYCLE_XML))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/{MINIO_BUCKET}/")))
            .and(query_param("lifecycle", ""))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/xml")
                    .set_body_string(S3_LIFECYCLE_READBACK_XML),
            )
            .expect(1)
            .mount(&server)
            .await;

        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(MINIO_REGION))
            .credentials_provider(Credentials::new(
                MINIO_ACCESS_KEY,
                MINIO_SECRET_KEY,
                None,
                None,
                "quant-pivot-s3-lifecycle-contract",
            ))
            .load()
            .await;
        let client = S3ControlClient::from_conf(
            S3ConfigBuilder::from(&shared)
                .force_path_style(true)
                .endpoint_url(server.uri())
                .build(),
        );
        client
            .put_bucket_lifecycle_configuration()
            .bucket(MINIO_BUCKET)
            .lifecycle_configuration(S3_MULTIPART_LIFECYCLE.configuration()?)
            .send()
            .await
            .context("write standard S3 multipart lifecycle contract")?;
        let lifecycle = client
            .get_bucket_lifecycle_configuration()
            .bucket(MINIO_BUCKET)
            .send()
            .await
            .context("read back standard S3 multipart lifecycle contract")?;
        S3_MULTIPART_LIFECYCLE.validate(lifecycle.rules())?;
        server.verify().await;
        Ok(())
    }

    #[tokio::test]
    async fn minio_sweeps_stale_upload() -> Result<()> {
        let run_dir = env::temp_dir().join(format!("quant-pivot-minio-stale-{}", Uuid::now_v7()));
        fs::create_dir_all(&run_dir)
            .with_context(|| format!("create MinIO stale-upload run {}", run_dir.display()))?;
        let stack = match ProductionArtifactStack::start_with_stale_policy(
            &run_dir,
            MinioStaleUploadPolicy {
                expiry: "1s",
                cleanup_interval: "1s",
            },
        )
        .await
        {
            Ok(stack) => stack,
            Err(error) => {
                let cleanup = fs::remove_dir_all(&run_dir);
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(error.context(format!(
                        "remove MinIO stale-upload run after startup failure: {cleanup}"
                    ))),
                };
            }
        };

        let result = async {
            let endpoint = stack
                .config
                .endpoint
                .as_deref()
                .context("MinIO stale-upload fixture omitted its endpoint")?;
            let shared = aws_config::defaults(BehaviorVersion::latest())
                .region(Region::new(MINIO_REGION))
                .credentials_provider(Credentials::new(
                    MINIO_ACCESS_KEY,
                    MINIO_SECRET_KEY,
                    None,
                    None,
                    "quant-pivot-minio-stale-upload",
                ))
                .load()
                .await;
            let client = S3ControlClient::from_conf(
                S3ConfigBuilder::from(&shared)
                    .force_path_style(true)
                    .endpoint_url(endpoint)
                    .build(),
            );
            let key = format!("{ARTIFACT_KEY_PREFIX}stale-upload-probe");
            let created = client
                .create_multipart_upload()
                .bucket(MINIO_BUCKET)
                .key(&key)
                .send()
                .await
                .context("create MinIO stale multipart upload")?;
            let upload_id = created
                .upload_id()
                .context("MinIO create-multipart response omitted upload id")?;
            client
                .upload_part()
                .bucket(MINIO_BUCKET)
                .key(&key)
                .upload_id(upload_id)
                .part_number(1)
                .body(ByteStream::from_static(b"stale-upload-probe"))
                .send()
                .await
                .context("write MinIO stale multipart probe part")?;
            let initial = client
                .list_parts()
                .bucket(MINIO_BUCKET)
                .key(&key)
                .upload_id(upload_id)
                .send()
                .await
                .context("list initial MinIO multipart probe parts")?;
            ensure!(
                initial.parts().len() == 1,
                "MinIO multipart probe did not expose its uploaded part"
            );

            let deadline = StdInstant::now() + Duration::from_secs(10);
            loop {
                match client
                    .list_parts()
                    .bucket(MINIO_BUCKET)
                    .key(&key)
                    .upload_id(upload_id)
                    .send()
                    .await
                {
                    Err(error)
                        if error
                            .as_service_error()
                            .and_then(ProvideErrorMetadata::code)
                            == Some("NoSuchUpload") =>
                    {
                        return Ok(());
                    }
                    Err(error) => {
                        return Err(error).context(
                            "MinIO stale-upload probe failed with an unexpected ListParts error",
                        );
                    }
                    Ok(parts) if StdInstant::now() >= deadline => {
                        bail!(
                            "MinIO retained stale multipart upload after the cleanup deadline: parts={}",
                            parts.parts().len()
                        );
                    }
                    Ok(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        }
        .await;
        let shutdown = stack.shutdown().await;
        let directory_cleanup = if shutdown.is_ok() {
            fs::remove_dir_all(&run_dir)
                .with_context(|| format!("remove MinIO stale-upload run {}", run_dir.display()))
        } else {
            Ok(())
        };
        match (result, shutdown, directory_cleanup) {
            (Ok(()), Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(()), Ok(())) => Err(error),
            (Ok(()), Err(cleanup), _) | (Ok(()), Ok(()), Err(cleanup)) => Err(cleanup),
            (Err(error), Err(cleanup), _) | (Err(error), Ok(()), Err(cleanup)) => Err(error
                .context(format!(
                    "MinIO stale-upload fixture cleanup also failed: {cleanup:#}"
                ))),
        }
    }

    #[test]
    fn trigger_replay_allows_progress() -> Result<()> {
        let cycle_id = FeedbackCycleId::from_v7();
        let mut cycle = JsonMap::new();
        for field in TRIGGER_IDENTITY_FIELDS {
            cycle.insert(
                (*field).to_owned(),
                if matches!(*field, "parent_cycle_id" | "forced_idempotency_key") {
                    JsonValue::Null
                } else {
                    json!("frozen")
                },
            );
        }
        cycle.insert("feedback_cycle_id".to_owned(), json!(cycle_id));
        cycle.insert("generation".to_owned(), json!(0));
        cycle.insert("status".to_owned(), json!("queued"));
        let first = json!({
            "data": {
                "cycle": cycle,
                "cycle_reused": false,
                "trigger_replayed": false,
            }
        });
        let mut replay = first.clone();
        replay["data"]["cycle"]["generation"] = json!(1);
        replay["data"]["cycle"]["status"] = json!("running");
        replay["data"]["cycle_reused"] = json!(true);
        replay["data"]["trigger_replayed"] = json!(true);

        verify_trigger_replay(&first, &replay, cycle_id)
    }

    mod report_evidence_tests {
        use std::{collections::BTreeMap, fs};

        use anyhow::{Context, Result};
        use chrono::Utc;
        use quant_pivot_models::{
            enums::quant::DataQualityStatus,
            runtime_config::BuyModelRoute,
            types::{
                MarketId, RecommendationReportId, ReportFunnelDiagnostics, ReportFunnelReason,
                ReportFunnelStage,
            },
        };
        use reqwest::Client;
        use serde_json::{Value as JsonValue, json};
        use tempfile::TempDir;
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path, query_param},
        };

        use super::super::{
            FeedbackMarketFunnelEvidence, FeedbackReportDiagnosticArchive, FeedbackReportEvidence,
            FeedbackReportUniverse, validate_mixed_recommendations,
        };

        struct ReportEvidenceFixture {
            universe: FeedbackReportUniverse,
            diagnostics: JsonValue,
            funnel: JsonValue,
        }

        #[derive(Clone, Copy)]
        enum TerminalFixture {
            Published,
            FeatureRejected,
            NominalFloor,
            RobustFloor,
        }

        impl Default for ReportEvidenceFixture {
            fn default() -> Self {
                let market_ids = (0..10)
                    .map(|index| MarketId::new(format!("market-{index:02}")))
                    .collect::<Vec<_>>();
                let routes_by_market = market_ids
                    .iter()
                    .enumerate()
                    .map(|(index, market)| {
                        (
                            market.clone(),
                            if index < 5 {
                                BuyModelRoute::Crypto
                            } else {
                                BuyModelRoute::Weather
                            },
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                Self {
                    universe: FeedbackReportUniverse {
                        decision_at: Utc::now(),
                        knowledge_lag_secs: 90,
                        market_ids,
                        routes_by_market,
                    },
                    diagnostics: json!({"data":{"routes":[
                        {"route":"pooled","outcome":"zero_candidates","funnel":{"eligible_markets":0}},
                        {"route":"crypto","outcome":"ready","funnel":{"eligible_markets":5}},
                        {"route":"weather","outcome":"ready","funnel":{"eligible_markets":5}}
                    ]}}),
                    funnel: json!({"data":{"conserved":true}}),
                }
            }
        }

        impl ReportEvidenceFixture {
            fn terminal(&self, index: usize, terminal: TerminalFixture) -> JsonValue {
                let market = &self.universe.market_ids[index];
                let (stage, reason, secondary) = match terminal {
                    TerminalFixture::Published => (
                        ReportFunnelStage::Published,
                        ReportFunnelReason::Published,
                        ReportFunnelDiagnostics::None {},
                    ),
                    TerminalFixture::FeatureRejected => (
                        ReportFunnelStage::FeatureReady,
                        ReportFunnelReason::FeatureDataQualityRejected,
                        ReportFunnelDiagnostics::FeatureDataQuality {
                            status: DataQualityStatus::Insufficient,
                            missing_required: Vec::new(),
                        },
                    ),
                    TerminalFixture::NominalFloor => (
                        ReportFunnelStage::SizingEligible,
                        ReportFunnelReason::NominalExpectedNetBelowFloor,
                        ReportFunnelDiagnostics::PlannerRejection {
                            detail: "nominal expected net below the governed floor".to_owned(),
                        },
                    ),
                    TerminalFixture::RobustFloor => (
                        ReportFunnelStage::SizingEligible,
                        ReportFunnelReason::RobustExpectedNetBelowFloor,
                        ReportFunnelDiagnostics::PlannerRejection {
                            detail: "robust expected net below the governed floor".to_owned(),
                        },
                    ),
                };
                secondary
                    .validate_for(reason)
                    .expect("canonical terminal fixture diagnostics");
                json!({
                    "market_id": market,
                    "route": self.universe.routes_by_market[market],
                    "terminal_stage": stage,
                    "primary_reason": reason,
                    "secondary_diagnostics": secondary,
                    "row_hash":"complete-original-row-hash",
                    "primary_token_id": format!("token-{index}")
                })
            }

            fn recommendations(&self, omitted: &[usize], large: bool) -> JsonValue {
                let mut rows = self.universe.market_ids.iter().enumerate().filter(|(index, _)| !omitted.contains(index))
                    .enumerate().map(|(rank, (_, market))| {
                        let route = self.universe.routes_by_market[market];
                        json!({"market_id":market,"route":route,"rank":rank+1,"economic_tier":{"route":route},
                            "economics":{"profit_probability_bps":"1","nominal_expected_net_usd":"1","robust_expected_net_usd":"1",
                                "max_loss_usd":"1","cvar_contribution_usd":"1","capital_occupancy_usd_hours":"1","marginal_portfolio_value_usd":"1"}})
                    }).collect::<Vec<_>>();
                if large {
                    rows[0]["scenario_cashflows"] =
                        json!(["huge_scenario_payload".repeat(100_000)]);
                }
                json!({"data":rows})
            }

            fn page(items: &[JsonValue], total: u64, page: u64) -> JsonValue {
                json!({"data":{"items":items,"total":total,"page":page,"size":100,"has_next":page*100 < total}})
            }
        }

        #[tokio::test]
        async fn paginated_terminals_keep_sizing() -> Result<()> {
            let fixture = ReportEvidenceFixture::default();
            let server = MockServer::start().await;
            let mut first = (0..99).map(|index| json!({"market_id":format!("unrelated-{index}"),"primary_reason":"feature_data_quality_rejected"})).collect::<Vec<_>>();
            first.push(fixture.terminal(0, TerminalFixture::NominalFloor));
            let second = (1..10)
                .map(|index| {
                    fixture.terminal(
                        index,
                        if index == 1 {
                            TerminalFixture::FeatureRejected
                        } else {
                            TerminalFixture::Published
                        },
                    )
                })
                .collect::<Vec<_>>();
            for (page, items) in [(1, first), (2, second)] {
                Mock::given(method("GET"))
                    .and(path("/funnel/markets"))
                    .and(query_param("page", page.to_string()))
                    .and(query_param("size", "100"))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .set_body_json(ReportEvidenceFixture::page(&items, 109, page)),
                    )
                    .expect(1)
                    .mount(&server)
                    .await;
            }
            let capture = FeedbackMarketFunnelEvidence::read(
                &Client::new(),
                &format!("{}/funnel/markets", server.uri()),
                "fixture-token",
                &fixture.universe.market_ids,
            )
            .await?;
            assert_eq!(capture.pages.len(), 2);
            let rows = capture.response["data"]["items"]
                .as_array()
                .context("exact terminal rows")?;
            assert_eq!(rows.len(), 10);
            assert_eq!(
                rows[0]["primary_reason"],
                ReportFunnelReason::NominalExpectedNetBelowFloor.as_str()
            );
            assert_eq!(
                rows[0]["terminal_stage"],
                ReportFunnelStage::SizingEligible.as_str()
            );
            assert_eq!(
                rows[0]["secondary_diagnostics"]["kind"],
                "planner_rejection"
            );
            assert_eq!(rows[0]["row_hash"], "complete-original-row-hash");
            assert_eq!(capture.feature_routes()?.len(), 1);
            assert!(capture.feature_routes()?.contains_key("market-01"));
            for request in server
                .received_requests()
                .await
                .context("captured HTTP requests")?
            {
                assert!(
                    request
                        .url
                        .query_pairs()
                        .all(|(key, _)| key == "page" || key == "size"),
                    "terminal evidence must not filter away sizing or portfolio reasons"
                );
            }
            Ok(())
        }

        #[tokio::test]
        async fn pagination_drift_is_rejected() -> Result<()> {
            let fixture = ReportEvidenceFixture::default();
            for (changed_total, expected) in [
                (true, "pagination changed"),
                (false, "repeated terminal evidence"),
            ] {
                let server = MockServer::start().await;
                let mut first = (0..99)
                    .map(|index| json!({"market_id":format!("unrelated-{index}")}))
                    .collect::<Vec<_>>();
                first.push(fixture.terminal(0, TerminalFixture::NominalFloor));
                let (second, total) = if changed_total {
                    (
                        vec![
                            fixture.terminal(1, TerminalFixture::Published),
                            fixture.terminal(2, TerminalFixture::Published),
                        ],
                        102,
                    )
                } else {
                    (
                        vec![fixture.terminal(0, TerminalFixture::NominalFloor)],
                        101,
                    )
                };
                for (page, items, count) in [(1, first, 101), (2, second, total)] {
                    Mock::given(method("GET"))
                        .and(path("/funnel/markets"))
                        .and(query_param("page", page.to_string()))
                        .and(query_param("size", "100"))
                        .respond_with(
                            ResponseTemplate::new(200)
                                .set_body_json(ReportEvidenceFixture::page(&items, count, page)),
                        )
                        .expect(1)
                        .mount(&server)
                        .await;
                }
                let error = FeedbackMarketFunnelEvidence::read(
                    &Client::new(),
                    &format!("{}/funnel/markets", server.uri()),
                    "fixture-token",
                    &fixture.universe.market_ids,
                )
                .await
                .err()
                .context("invalid immutable pagination must fail")?;
                assert!(
                    error.to_string().contains(expected),
                    "wrong pagination failure: {error:#}"
                );
            }
            Ok(())
        }

        #[test]
        fn failure_archives_large_responses() -> Result<()> {
            let fixture = ReportEvidenceFixture::default();
            let terminals = (0..10)
                .map(|index| {
                    fixture.terminal(
                        index,
                        match index {
                            1 => TerminalFixture::NominalFloor,
                            3 => TerminalFixture::RobustFloor,
                            _ => TerminalFixture::Published,
                        },
                    )
                })
                .collect::<Vec<_>>();
            let market_response = ReportEvidenceFixture::page(&terminals, 10, 1);
            let evidence = FeedbackReportEvidence {
                recommendations: fixture.recommendations(&[1, 3], true),
                diagnostics: fixture.diagnostics.clone(),
                funnel: fixture.funnel.clone(),
                funnel_markets: market_response.clone(),
                funnel_market_pages: vec![market_response],
                feature_nulls: json!([]),
            };
            let directory = TempDir::new()?;
            let archive_path = directory.path().join("feedback-report-diagnostics.json");
            FeedbackReportDiagnosticArchive {
                recommendation_report_id: RecommendationReportId::from_v7(),
                report_universe: &fixture.universe,
                report_run: &json!({"data":{"status":"succeeded"}}),
                report_detail: &json!({"data":{"status":"published"}}),
                evidence: &evidence,
            }
            .persist(&archive_path)?;
            let error = validate_mixed_recommendations(
                &evidence.recommendations,
                &fixture.universe,
                &evidence.diagnostics,
                &evidence.funnel,
                &evidence.funnel_markets,
                &archive_path,
            )
            .err()
            .context("8/10 must fail exact closure")?
            .to_string();
            assert!(
                error.contains("market-01")
                    && error.contains("market-03")
                    && error.contains(ReportFunnelReason::NominalExpectedNetBelowFloor.as_str())
                    && error.contains(ReportFunnelReason::RobustExpectedNetBelowFloor.as_str())
            );
            assert!(
                error.contains("route_counts")
                    && error.contains(&archive_path.display().to_string())
            );
            assert!(
                error.len() < 8_000
                    && !error.contains("huge_scenario_payload")
                    && !error.contains("scenario_cashflows")
            );
            let retained: JsonValue = serde_json::from_slice(&fs::read(&archive_path)?)?;
            assert_eq!(
                retained["evidence"]["recommendations"],
                evidence.recommendations
            );
            assert_eq!(
                retained["evidence"]["funnel_market_pages"],
                json!(evidence.funnel_market_pages)
            );
            directory.close()?;
            Ok(())
        }

        #[test]
        fn complete_universe_still_required() -> Result<()> {
            let fixture = ReportEvidenceFixture::default();
            let directory = TempDir::new()?;
            let path = directory.path().join("diagnostics.json");
            let complete = ReportEvidenceFixture::page(
                &(0..10)
                    .map(|index| fixture.terminal(index, TerminalFixture::Published))
                    .collect::<Vec<_>>(),
                10,
                1,
            );
            validate_mixed_recommendations(
                &fixture.recommendations(&[], false),
                &fixture.universe,
                &fixture.diagnostics,
                &fixture.funnel,
                &complete,
                &path,
            )?;
            let incomplete = ReportEvidenceFixture::page(
                &(0..9)
                    .map(|index| fixture.terminal(index, TerminalFixture::Published))
                    .collect::<Vec<_>>(),
                9,
                1,
            );
            let error = validate_mixed_recommendations(
                &fixture.recommendations(&[], false),
                &fixture.universe,
                &fixture.diagnostics,
                &fixture.funnel,
                &incomplete,
                &path,
            )
            .err()
            .context("terminal evidence must cover all ten markets")?;
            assert!(
                error
                    .to_string()
                    .contains("missing_terminal_ids=[\"market-09\"]")
            );
            directory.close()?;
            Ok(())
        }
    }

    mod historical_economic_liveness {
        use std::time::Duration;

        use anyhow::Result;
        use chrono::{DateTime, Duration as ChronoDuration, Utc};
        use quant_pivot_models::{enums::quant::OutcomeReconciliationTaskStatus, types::WorkerId};
        use tokio::time::Instant;

        use super::super::{
            HISTORICAL_ECONOMIC_IDLE_TIMEOUT, HISTORICAL_ECONOMIC_TIMEOUT,
            HistoricalEconomicCounts, HistoricalEconomicLiveness, HistoricalEconomicObservation,
            HistoricalEconomicProgress, HistoricalEconomicSchedule, HistoricalEconomicTaskRead,
        };

        struct ClockFixture {
            started: Instant,
            observed_at: DateTime<Utc>,
        }

        impl Default for ClockFixture {
            fn default() -> Self {
                Self {
                    started: Instant::now(),
                    observed_at: "2026-08-30T16:27:56Z"
                        .parse()
                        .expect("virtual database clock"),
                }
            }
        }

        impl ClockFixture {
            fn retry(&self, due_secs: i64) -> HistoricalEconomicTaskRead {
                HistoricalEconomicTaskRead {
                    status: OutcomeReconciliationTaskStatus::Retrying,
                    completed_at: None,
                    claim_owner: None,
                    lease_expires_at: None,
                    next_attempt_at: Some(self.observed_at + ChronoDuration::seconds(due_secs)),
                }
            }

            fn snapshot(
                &self,
                elapsed: u64,
                tasks: &[HistoricalEconomicTaskRead],
            ) -> Result<HistoricalEconomicObservation> {
                let observed_at =
                    self.observed_at + ChronoDuration::seconds(i64::try_from(elapsed)?);
                let mut counts = HistoricalEconomicCounts::default();
                let mut schedule = HistoricalEconomicSchedule::default();
                for task in tasks {
                    counts.observe(task, observed_at);
                    schedule.observe(task, observed_at)?;
                }
                counts.outcomes = counts.completed;
                counts.visible_outcomes = counts.visible_completed;
                counts.validate(u64::try_from(tasks.len())?)?;
                let progress = HistoricalEconomicProgress {
                    observed_at,
                    counts,
                    schedule,
                };
                schedule.validate(&progress)?;
                Ok(HistoricalEconomicObservation {
                    progress,
                    clock_at: self.started + Duration::from_secs(elapsed),
                })
            }
        }

        impl HistoricalEconomicTaskRead {
            fn pending() -> Self {
                Self {
                    status: OutcomeReconciliationTaskStatus::Pending,
                    completed_at: None,
                    claim_owner: None,
                    lease_expires_at: None,
                    next_attempt_at: None,
                }
            }
        }

        #[test]
        fn future_retry_pauses_budget() -> Result<()> {
            let clock = ClockFixture::default();
            let mut liveness = HistoricalEconomicLiveness::new(
                clock.started,
                clock.snapshot(0, &[clock.retry(840)])?,
            )?;
            assert!(!liveness.observe(clock.snapshot(780, &[clock.retry(840)])?)?);
            assert_eq!(liveness.eligible_elapsed, Duration::ZERO);
            liveness.check(clock.started + Duration::from_secs(1_019))?;
            assert!(
                liveness
                    .check(clock.started + Duration::from_mins(17))
                    .is_err()
            );
            Ok(())
        }

        #[test]
        fn future_requires_complete_evidence() -> Result<()> {
            let clock = ClockFixture::default();
            let mut visible = clock.snapshot(0, &[clock.retry(840)])?;
            visible.progress.counts = HistoricalEconomicCounts {
                retrying: 1,
                completed: 4_198,
                visible_completed: 4_198,
                outcomes: 4_198,
                visible_outcomes: 4_198,
                ..HistoricalEconomicCounts::default()
            };
            visible.progress.counts.validate(4_199)?;
            let permitted = HistoricalEconomicLiveness::new(clock.started, visible)?;
            permitted.check(clock.started + Duration::from_mins(13))?;

            for counts in [
                HistoricalEconomicCounts {
                    visible_completed: 4_197,
                    ..visible.progress.counts
                },
                HistoricalEconomicCounts {
                    outcomes: 4_197,
                    visible_outcomes: 4_197,
                    ..visible.progress.counts
                },
                HistoricalEconomicCounts {
                    visible_outcomes: 4_197,
                    ..visible.progress.counts
                },
                HistoricalEconomicCounts {
                    outcomes: 4_199,
                    visible_outcomes: 4_199,
                    ..visible.progress.counts
                },
            ] {
                let mut incomplete = visible;
                incomplete.progress.counts = counts;
                let liveness = HistoricalEconomicLiveness::new(clock.started, incomplete)?;
                assert!(
                    liveness
                        .check(clock.started + HISTORICAL_ECONOMIC_IDLE_TIMEOUT)
                        .is_err()
                );
            }
            Ok(())
        }

        #[test]
        fn retries_preserve_spent_budget() -> Result<()> {
            let clock = ClockFixture::default();
            let mut liveness = HistoricalEconomicLiveness::new(
                clock.started,
                clock.snapshot(0, &[HistoricalEconomicTaskRead::pending()])?,
            )?;
            assert!(!liveness.observe(clock.snapshot(170, &[clock.retry(770)])?)?);
            assert!(!liveness.observe(clock.snapshot(600, &[clock.retry(770)])?)?);
            assert_eq!(liveness.eligible_elapsed, Duration::from_secs(170));
            liveness.check(clock.started + Duration::from_secs(779))?;
            assert!(
                liveness
                    .check(clock.started + Duration::from_mins(13))
                    .is_err()
            );
            let mut exhausted = HistoricalEconomicLiveness::new(
                clock.started,
                clock.snapshot(0, &[HistoricalEconomicTaskRead::pending()])?,
            )?;
            exhausted.observe(clock.snapshot(180, &[clock.retry(770)])?)?;
            assert!(
                exhausted
                    .check(clock.started + Duration::from_mins(3))
                    .is_err()
            );
            Ok(())
        }

        #[test]
        fn active_work_stays_timed() -> Result<()> {
            let clock = ClockFixture::default();
            let mut delivering = HistoricalEconomicTaskRead::pending();
            delivering.status = OutcomeReconciliationTaskStatus::Delivering;
            delivering.claim_owner = Some(WorkerId::from_v7());
            delivering.lease_expires_at = Some(clock.observed_at + ChronoDuration::hours(1));
            for active in [HistoricalEconomicTaskRead::pending(), delivering] {
                let observation = clock.snapshot(0, &[active, clock.retry(600)])?;
                assert_eq!(observation.progress.schedule.eligible, 1);
                assert_eq!(observation.progress.schedule.waiting_retry, 1);
                let liveness = HistoricalEconomicLiveness::new(clock.started, observation)?;
                assert!(
                    liveness
                        .check(clock.started + HISTORICAL_ECONOMIC_IDLE_TIMEOUT)
                        .is_err()
                );
            }
            Ok(())
        }

        #[test]
        fn completion_alone_resets_budget() -> Result<()> {
            let clock = ClockFixture::default();
            let mut liveness = HistoricalEconomicLiveness::new(
                clock.started,
                clock.snapshot(
                    0,
                    &[
                        HistoricalEconomicTaskRead::pending(),
                        HistoricalEconomicTaskRead::pending(),
                    ],
                )?,
            )?;
            let completed = HistoricalEconomicTaskRead {
                status: OutcomeReconciliationTaskStatus::Completed,
                completed_at: Some(clock.observed_at + ChronoDuration::seconds(169)),
                ..HistoricalEconomicTaskRead::pending()
            };
            assert!(liveness.observe(
                clock.snapshot(170, &[completed, HistoricalEconomicTaskRead::pending()])?
            )?);
            assert_eq!(liveness.eligible_elapsed, Duration::ZERO);
            liveness.check(clock.started + Duration::from_secs(349))?;
            assert!(
                liveness
                    .check(clock.started + Duration::from_secs(350))
                    .is_err()
            );
            Ok(())
        }

        #[test]
        fn timeouts_preserve_their_cause() -> Result<()> {
            let clock = ClockFixture::default();
            let pending = HistoricalEconomicLiveness::new(
                clock.started,
                clock.snapshot(0, &[HistoricalEconomicTaskRead::pending()])?,
            )?;
            assert!(
                pending
                    .read_timeout(clock.started + HISTORICAL_ECONOMIC_IDLE_TIMEOUT)
                    .to_string()
                    .contains("eligible-no-progress")
            );
            let future = HistoricalEconomicLiveness::new(
                clock.started,
                clock.snapshot(0, &[clock.retry(3_600)])?,
            )?;
            assert_eq!(
                future.read_deadline(clock.started)?,
                clock.started + Duration::from_secs(10)
            );
            assert!(
                future
                    .read_timeout(clock.started + Duration::from_secs(10))
                    .to_string()
                    .contains("read budget")
            );
            assert!(
                future
                    .read_timeout(clock.started + HISTORICAL_ECONOMIC_TIMEOUT)
                    .to_string()
                    .contains("total budget")
            );
            assert!(
                future
                    .check(clock.started + HISTORICAL_ECONOMIC_TIMEOUT)
                    .is_err()
            );
            Ok(())
        }

        #[test]
        fn eligibility_uses_observed_clock() -> Result<()> {
            let clock = ClockFixture::default();
            assert_eq!(
                clock
                    .snapshot(9, &[clock.retry(10)])?
                    .progress
                    .schedule
                    .waiting_retry,
                1
            );
            let due = clock.snapshot(10, &[clock.retry(10)])?;
            assert_eq!(due.progress.schedule.eligible, 1);
            assert_eq!(due.progress.schedule.waiting_retry, 0);
            let mut liveness = HistoricalEconomicLiveness::new(clock.started, due)?;
            let mut backwards = clock.snapshot(11, &[clock.retry(10)])?;
            backwards.progress.observed_at = clock.observed_at + ChronoDuration::seconds(9);
            assert!(liveness.observe(backwards).is_err());
            let malformed = HistoricalEconomicTaskRead {
                next_attempt_at: None,
                ..clock.retry(10)
            };
            assert!(clock.snapshot(0, &[malformed]).is_err());
            Ok(())
        }
    }

    mod historical_economic_postgres {
        use std::time::{Duration, Instant};

        use anyhow::{Context, Error as AnyhowError, Result, bail};
        use chrono::Duration as ChronoDuration;
        use quant_pivot_core::service::recommendation_economic_outcome::{
            RecommendationEconomicReplayAdapter, RecommendationEconomicReplayBinding,
        };
        use quant_pivot_models::{
            domain::quant::{
                EconomicOutcomeReconciliationResult, EconomicOutcomeTaskClaim,
                EconomicOutcomeTaskSettlement, RecommendationEconomicOutcomeInfo,
            },
            hashing::CanonicalDigest,
            types::WorkerId,
        };
        use quant_pivot_repository::{
            postgres::PgRecommendationEconomicOutcomeRepository,
            traits::RecommendationEconomicOutcomeRepository,
        };
        use sea_orm::DatabaseConnection;

        use super::{
            HistoricalEconomicBackfill, HistoricalEconomicCounts, HistoricalEconomicTarget,
        };
        use crate::{
            postgres::{PostgresClock, setup_pg, with_postgres_suite},
            support::{
                economic_outcome_fixtures::seed_report_at,
                execution_pg_seed::{fixture_profile_ref, seed_shared_demo_infra},
            },
        };

        struct MissingSourceFixture {
            repository: PgRecommendationEconomicOutcomeRepository,
            worker: WorkerId,
        }

        impl MissingSourceFixture {
            async fn retry_then_claim(
                &self,
                target: &HistoricalEconomicTarget,
                db: &DatabaseConnection,
                claim: EconomicOutcomeTaskClaim,
            ) -> Result<EconomicOutcomeTaskClaim> {
                assert!(matches!(
                    self.repository
                        .retry_task(
                            claim,
                            self.worker,
                            2,
                            "ComputeCapacityUnavailable".to_owned()
                        )
                        .await?,
                    EconomicOutcomeTaskSettlement::Retried
                ));
                let waiting = target.progress(db).await?.progress;
                assert_eq!(waiting.schedule.eligible, 0);
                assert_eq!(waiting.schedule.waiting_retry, 1);
                let next = waiting
                    .schedule
                    .next_eligible_at
                    .context("future retry time")?;
                assert!(next > waiting.observed_at);
                assert!(!waiting.counts.drained(u64::try_from(target.ids.len())?));
                // The later-created report is outside the frozen maturity slice.
                assert!(
                    self.repository
                        .claim_due(claim.horizon_at, self.worker, 60, 300, 1)
                        .await?
                        .is_empty()
                );
                tokio::time::timeout(Duration::from_secs(5), async {
                    while db.statement_time().await < next {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                })
                .await
                .context("canonical retry must become eligible within its bounded wait")?;
                let eligible = target.progress(db).await?.progress;
                assert_eq!(eligible.schedule.eligible, 1);
                assert_eq!(eligible.schedule.waiting_retry, 0);
                assert_eq!(eligible.counts.completed, waiting.counts.completed);
                assert_eq!(
                    eligible.counts.visible_outcomes,
                    waiting.counts.visible_outcomes
                );
                let claims = self
                    .repository
                    .claim_due(db.statement_time().await, self.worker, 60, 300, 1)
                    .await?;
                assert_eq!(claims.len(), 1);
                let reclaimed = claims[0];
                assert_eq!(reclaimed.recommendation_id, claim.recommendation_id);
                assert_eq!(reclaimed.attempt_count, claim.attempt_count + 1);
                assert_eq!(reclaimed.horizon_at, claim.horizon_at);
                assert_eq!(reclaimed.replay_until, claim.replay_until);
                assert_eq!(reclaimed.source_cutoff_at, claim.source_cutoff_at);
                assert_eq!(
                    reclaimed.resolution_outcome_hash,
                    claim.resolution_outcome_hash
                );
                let delivering = target.progress(db).await?.progress;
                assert_eq!(delivering.schedule.eligible, 1);
                assert_eq!(delivering.counts.live_claims, 1);
                Ok(reclaimed)
            }

            async fn complete(
                &self,
                claim: EconomicOutcomeTaskClaim,
            ) -> Result<RecommendationEconomicOutcomeInfo> {
                assert_eq!(claim.source_available_until, claim.source_cutoff_at);
                let context = self
                    .repository
                    .replay_context(&claim.recommendation_id)
                    .await?;
                // This fixture has no replay source objects. Exercise the canonical
                // source-unavailable seal, never a fabricated executable return.
                let outcome = RecommendationEconomicReplayAdapter::censor_unavailable(
                    RecommendationEconomicReplayBinding {
                        recommendation_id: claim.recommendation_id,
                        recommendation_report_id: context.report.recommendation_report_id,
                        report_route_run_id: context.route_run.report_route_run_id,
                        decision_policy_snapshot_id: context.report.decision_policy_snapshot_id,
                        economic_tier_id: context.recommendation.economic_tier_id,
                        model_version_id: context
                            .route_run
                            .model_version_id
                            .context("frozen model")?,
                        trade_policy_artifact_id: context
                            .route_run
                            .trade_policy_artifact_id
                            .context("frozen policy")?,
                        research_profile_artifact_id: context
                            .route_run
                            .research_profile_artifact_id
                            .context("frozen profile")?,
                        decision_at: context.report.decision_at,
                        horizon_at: claim.horizon_at,
                        replay_until: claim.replay_until,
                        resolution_outcome_hash: claim.resolution_outcome_hash,
                        source_cutoff_at: claim.source_cutoff_at,
                        source_available_until: claim.source_available_until,
                        replay_input_hash: CanonicalDigest::content_hash_json(
                            &context.recommendation.evidence_refs,
                        )?,
                        available_at: claim.source_available_until,
                    },
                )?;
                let EconomicOutcomeReconciliationResult::Inserted(stored) = self
                    .repository
                    .complete_task(claim, self.worker, outcome)
                    .await?
                else {
                    bail!("historical fixture claim must atomically insert its WORM outcome");
                };
                stored.verify()?;
                Ok(stored)
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn freezes_worm_readback() -> Result<()> {
            Box::pin(with_postgres_suite(async {
                tokio::time::timeout(
                    Duration::from_mins(1),
                    Box::pin(async {
                        let (pool, _scenario) = setup_pg().await;
                        let db = pool.connection();
                        let infra = Box::pin(seed_shared_demo_infra(db)).await;
                        let profile = fixture_profile_ref()
                            .resolve_builtin_research_profile()
                            .map_err(AnyhowError::msg)?;
                        let horizon = ChronoDuration::seconds(i64::try_from(
                            profile.spec.target_horizon_secs,
                        )?);
                        let decision_at =
                            db.statement_time().await - horizon - ChronoDuration::hours(2);
                        let mut expected_ids = Vec::new();
                        for offset in 0..2 {
                            expected_ids.push(
                                seed_report_at(
                                    db,
                                    &infra,
                                    decision_at + ChronoDuration::seconds(offset),
                                )
                                .await?
                                .recommendation,
                            );
                        }
                        expected_ids.sort_by_key(|id| id.as_uuid());
                        let started = Instant::now();
                        let target = HistoricalEconomicTarget::freeze(db).await?;
                        assert_eq!(target.ids, expected_ids);
                        assert_eq!(
                            target.hash,
                            HistoricalEconomicTarget::hash(target.cutoff, &expected_ids)?
                        );
                        let initial = target.progress(db).await?.progress;
                        assert_eq!(
                            initial.counts,
                            HistoricalEconomicCounts {
                                pending: 2,
                                ..HistoricalEconomicCounts::default()
                            }
                        );
                        assert!(
                            target
                                .verify_outcomes(db, initial.observed_at)
                                .await
                                .is_err()
                        );

                        let later =
                            seed_report_at(db, &infra, decision_at + ChronoDuration::hours(1))
                                .await?;
                        assert!(!target.ids.contains(&later.recommendation));
                        assert_eq!(target.progress(db).await?.progress.counts, initial.counts);
                        let expanded = HistoricalEconomicTarget::freeze(db).await?;
                        assert_eq!(expanded.ids.len(), 3);
                        assert!(expanded.ids.contains(&later.recommendation));
                        let fixture = MissingSourceFixture {
                            repository: PgRecommendationEconomicOutcomeRepository::new(db.clone()),
                            worker: WorkerId::from_v7(),
                        };
                        let mut expected_receipts = Vec::new();
                        for completed in 0..2 {
                            let claims = fixture
                                .repository
                                .claim_due(db.statement_time().await, fixture.worker, 60, 300, 1)
                                .await?;
                            assert_eq!(claims.len(), 1);
                            let mut claim = claims[0];
                            assert_eq!(
                                claim.recommendation_id,
                                expected_ids[usize::try_from(completed)?]
                            );
                            assert_eq!(
                                target.progress(db).await?.progress.counts,
                                HistoricalEconomicCounts {
                                    pending: 1 - completed,
                                    delivering: 1,
                                    retrying: 0,
                                    completed,
                                    visible_completed: completed,
                                    outcomes: completed,
                                    visible_outcomes: completed,
                                    live_claims: 1,
                                }
                            );
                            if completed == 1 {
                                claim = fixture.retry_then_claim(&target, db, claim).await?;
                            }
                            let stored = fixture.complete(claim).await?;
                            expected_receipts.push((
                                stored.recommendation_id,
                                stored.evidence_hash,
                                stored.available_at,
                            ));
                            if completed == 0 {
                                assert!(!target.progress(db).await?.progress.counts.drained(2));
                                assert!(
                                    target
                                        .verify_outcomes(db, db.statement_time().await)
                                        .await
                                        .is_err()
                                );
                            }
                        }
                        let terminal = target.progress(db).await?.progress;
                        assert!(terminal.counts.drained(2));
                        assert!(target.verify_outcomes(db, target.cutoff).await.is_err());
                        let outcomes = target.verify_outcomes(db, terminal.observed_at).await?;
                        assert_eq!(
                            outcomes
                                .iter()
                                .map(|receipt| (
                                    receipt.recommendation_id,
                                    receipt.evidence_hash,
                                    receipt.available_at
                                ))
                                .collect::<Vec<_>>(),
                            expected_receipts
                        );
                        let proof = HistoricalEconomicBackfill {
                            target_cutoff: target.cutoff,
                            recommendation_ids: target.ids.clone(),
                            target_hash: target.hash,
                            initial,
                            terminal,
                            elapsed_ms: u64::try_from(started.elapsed().as_millis())?,
                            outcome_set_hash: HistoricalEconomicBackfill::receipt_hash(&outcomes)?,
                            outcomes,
                        };
                        proof.validate()?;

                        let future =
                            seed_report_at(db, &infra, db.statement_time().await - horizon / 2)
                                .await?;
                        assert!(!target.ids.contains(&future.recommendation));
                        assert_eq!(target.progress(db).await?.progress.counts, terminal.counts);
                        let error = HistoricalEconomicTarget::freeze(db)
                            .await
                            .err()
                            .context("future horizon must reject a new freeze")?;
                        assert!(error.to_string().contains("unmatured nonhistorical task"));
                        Ok::<(), AnyhowError>(())
                    }),
                )
                .await
                .context(
                    "historical economic PostgreSQL readback exceeded its bounded scenario budget",
                )?
            }))
            .await?
        }
    }

    struct ClosureManifestFixture(JsonValue);

    fn readiness_capture_fixture(verified_at: &str) -> ReadinessCaptureEvidence {
        let verified_at = verified_at
            .parse::<DateTime<Utc>>()
            .expect("manifest readiness verification time");
        let observed_at = verified_at - ChronoDuration::seconds(1);
        let required_days = minimum_raw_retention_days().expect("canonical raw-history retention");
        let registry = research_source_registry().expect("canonical research source registry");
        let observations = registry
            .bindings
            .iter()
            .map(|binding| {
                let clickhouse = binding.storage == ResearchSourceStorageKind::ClickHouseTable;
                RetentionSourceObservationV1 {
                    source: binding.source,
                    storage: binding.storage,
                    object: binding.object.clone(),
                    time_column: binding.time_column.clone(),
                    time_encoding: binding.time_encoding,
                    earliest_event_time: Some(observed_at - ChronoDuration::days(201)),
                    latest_event_time: Some(observed_at),
                    row_count: 2,
                    active_bytes: clickhouse.then_some(1),
                    active_partition_count: clickhouse.then_some(1),
                    partition_key: binding.partition_key.clone(),
                    table_ttl_expression: None,
                }
            })
            .collect::<Vec<_>>();
        let payload_json =
            ResearchReadinessEvidencePayload::RetentionRunway(RetentionRunwayEvidenceV1 {
                format_version: RETENTION_RUNWAY_EVIDENCE_FORMAT_VERSION,
                registry_hash: registry.contract_hash().expect("readiness registry hash"),
                required_sources: registry.required_sources,
                observed_at,
                required_days,
                measured_history_days: Some(201),
                active_raw_bytes: observations
                    .iter()
                    .filter_map(|observation| observation.active_bytes)
                    .sum(),
                observations,
            });
        let payload_hash = CanonicalDigest::content_hash_json(&payload_json)
            .expect("manifest readiness payload hash");
        ReadinessCaptureEvidence {
            verified_at,
            evidence: ResearchReadinessEvidenceInfo {
                evidence_id: ResearchReadinessEvidenceId::from_v7(),
                kind: ResearchReadinessEvidenceKind::RetentionRunway,
                scope_hash: ContentHash::from_bytes([6; 32]),
                window_start: observed_at - ChronoDuration::days(i64::from(required_days)),
                window_end: observed_at,
                observed_at,
                expires_at: observed_at + ChronoDuration::hours(6),
                payload_json,
                payload_hash,
                artifact_uri: ArtifactUri::parse("s3://fixture/readiness/retention.json")
                    .expect("manifest readiness artifact URI"),
                artifact_version: ArtifactVersion::parse("fixture-version")
                    .expect("manifest readiness artifact version"),
                attestation_key_id: AttestationKeyId::parse("fixture-attestor")
                    .expect("manifest readiness attestation key"),
                attestation_mac: ContentHash::from_bytes([5; 32]),
                created_at: observed_at,
            },
        }
    }

    fn reseal_readiness_payload(manifest: &mut JsonValue) -> Result<()> {
        let payload: ResearchReadinessEvidencePayload = serde_json::from_value(
            manifest["readiness_capture"]["evidence"]["payload_json"].clone(),
        )?;
        manifest["readiness_capture"]["evidence"]["payload_hash"] =
            json!(CanonicalDigest::content_hash_json(&payload)?);
        Ok(())
    }

    impl HistoricalEconomicBackfill {
        fn fixture() -> Self {
            let cutoff = "2026-08-26T23:59:58Z"
                .parse::<DateTime<Utc>>()
                .expect("historical target cutoff");
            let visible_at = cutoff + ChronoDuration::seconds(1);
            let recommendation_ids = vec![RecommendationId::from_v7()];
            let outcomes = vec![HistoricalEconomicReceipt {
                recommendation_id: recommendation_ids[0],
                evidence_hash: ContentHash::from_bytes([8; 32]),
                available_at: visible_at,
            }];
            Self {
                target_cutoff: cutoff,
                target_hash: HistoricalEconomicTarget::hash(cutoff, &recommendation_ids)
                    .expect("target hash"),
                recommendation_ids,
                initial: HistoricalEconomicProgress {
                    observed_at: cutoff,
                    counts: HistoricalEconomicCounts {
                        pending: 1,
                        ..HistoricalEconomicCounts::default()
                    },
                    schedule: HistoricalEconomicSchedule {
                        eligible: 1,
                        ..HistoricalEconomicSchedule::default()
                    },
                },
                terminal: HistoricalEconomicProgress {
                    observed_at: visible_at,
                    counts: HistoricalEconomicCounts {
                        completed: 1,
                        visible_completed: 1,
                        outcomes: 1,
                        visible_outcomes: 1,
                        ..HistoricalEconomicCounts::default()
                    },
                    schedule: HistoricalEconomicSchedule::default(),
                },
                elapsed_ms: 1_000,
                outcome_set_hash: Self::receipt_hash(&outcomes).expect("outcome-set hash"),
                outcomes,
            }
        }
    }

    #[test]
    fn historical_backfill_requires_worm() -> Result<()> {
        HistoricalEconomicBackfill::fixture().validate()?;
        let mut pending = HistoricalEconomicBackfill::fixture();
        pending.terminal.counts = pending.initial.counts;
        assert!(!pending.terminal.counts.drained(1));
        assert!(pending.validate().is_err());
        for missing in [
            "outcomes",
            "visible_outcomes",
            "visible_completed",
            "live_claims",
        ] {
            let mut value = serde_json::to_value(HistoricalEconomicBackfill::fixture())?;
            value["terminal"]["counts"][missing] = json!(u64::from(missing == "live_claims"));
            let proof: HistoricalEconomicBackfill = serde_json::from_value(value)?;
            assert!(!proof.terminal.counts.drained(1));
            assert!(
                proof.validate().is_err(),
                "incomplete {missing} must not satisfy the barrier"
            );
        }
        let mut missing_receipt = HistoricalEconomicBackfill::fixture();
        missing_receipt.outcomes.clear();
        missing_receipt.outcome_set_hash =
            HistoricalEconomicBackfill::receipt_hash(&missing_receipt.outcomes)?;
        assert!(missing_receipt.validate().is_err());
        Ok(())
    }

    #[test]
    fn historical_backfill_clocks_enforced() -> Result<()> {
        let mut future = HistoricalEconomicBackfill::fixture();
        future.outcomes[0].available_at = future.terminal.observed_at + ChronoDuration::seconds(1);
        future.outcome_set_hash = HistoricalEconomicBackfill::receipt_hash(&future.outcomes)?;
        assert!(future.validate().is_err());
        let mut before_cutoff = HistoricalEconomicBackfill::fixture();
        before_cutoff.initial.observed_at =
            before_cutoff.target_cutoff - ChronoDuration::seconds(1);
        assert!(before_cutoff.validate().is_err());
        let mut reversed = HistoricalEconomicBackfill::fixture();
        reversed.terminal.observed_at = reversed.initial.observed_at - ChronoDuration::seconds(1);
        assert!(reversed.validate().is_err());
        let mut over_budget = HistoricalEconomicBackfill::fixture();
        over_budget.elapsed_ms = u64::try_from(HISTORICAL_ECONOMIC_TIMEOUT.as_millis())? + 1;
        assert!(over_budget.validate().is_err());
        let mut forged_membership = HistoricalEconomicBackfill::fixture();
        forged_membership.recommendation_ids[0] = RecommendationId::from_v7();
        assert!(forged_membership.validate().is_err());
        let mut forged_initial = HistoricalEconomicBackfill::fixture();
        forged_initial.initial.counts.completed = 1;
        assert!(forged_initial.validate().is_err());
        Ok(())
    }

    #[test]
    fn closure_rejects_incomplete_backfill() -> Result<()> {
        let baseline = ClosureManifestFixture::default().0;
        validate_closure_manifest(&serde_json::to_vec(&baseline)?)?;
        for field in [
            "completed",
            "visible_completed",
            "outcomes",
            "visible_outcomes",
        ] {
            let mut manifest = baseline.clone();
            manifest["historical_economic_backfill"]["terminal"]["counts"][field] = json!(0);
            assert!(validate_closure_manifest(&serde_json::to_vec(&manifest)?).is_err());
        }
        let mut after_report = HistoricalEconomicBackfill::fixture();
        after_report.terminal.observed_at += ChronoDuration::seconds(2);
        after_report.outcomes[0].available_at = after_report.terminal.observed_at;
        after_report.elapsed_ms = 3_000;
        after_report.outcome_set_hash =
            HistoricalEconomicBackfill::receipt_hash(&after_report.outcomes)?;
        after_report.validate()?;
        let mut manifest = baseline;
        manifest["historical_economic_backfill"] = serde_json::to_value(after_report)?;
        assert!(validate_closure_manifest(&serde_json::to_vec(&manifest)?).is_err());
        Ok(())
    }

    struct ClosureManifestParts {
        timestamp: &'static str,
        resolved_at: &'static str,
        observed_at: &'static str,
        digest: String,
        report_id: String,
        cycle_id: String,
        runtime_control: JsonValue,
        money_path: JsonValue,
        stage_evidence: Vec<JsonValue>,
        market_ids: Vec<String>,
        recommendation_ids: Vec<String>,
        routes_by_market: JsonMap<String, JsonValue>,
        recommendations: Vec<JsonValue>,
        resolution_facts: Vec<JsonValue>,
    }

    impl Default for ClosureManifestFixture {
        fn default() -> Self {
            Self(ClosureManifestParts::fixture().into_value())
        }
    }

    impl ClosureManifestParts {
        fn fixture() -> Self {
            let timestamp = "2026-08-27T00:00:00Z";
            let resolved_at = "2026-08-27T00:00:01Z";
            let observed_at = "2026-08-27T00:00:02Z";
            let digest = ContentHash::from_bytes([7; 32]).to_string();
            let report_id = Uuid::now_v7().to_string();
            let cycle_id = Uuid::now_v7().to_string();
            let runtime_control = json!({
                "entry_authorization_policy": "operator_approval_required",
                "settlement_write_policy": "disabled",
                "kill_switch_state": "closed",
                "kill_switch_requires_ack": false,
                "revision": 1,
                "changed_by": "fixture",
                "reason": "fixture",
                "changed_at": timestamp,
            });
            let money_path = json!({
                "order_intents": 2,
                "capital_allocations": 2,
                "execution_accounts": 1,
                "execution_orders": 1,
                "execution_attempt_outcomes": 0,
                "execution_reconciliation_tasks": 0,
                "execution_rollup_tasks": 0,
                "execution_trade_refs": 0,
                "clob_trade_observations": 0,
                "execution_transaction_refs": 0,
                "strategy_position_lots": 0,
                "settlement_authorizations": 0,
                "settlement_chain_submissions": 0,
                "settlement_external_cursors": 0,
                "settlement_governed_actions": 0,
                "settlement_inventory_lots": 0,
                "settlement_redeems": 0,
                "settlement_redeem_lots": 0,
                "account_chain_executions": 0,
                "account_execution_associations": 0,
                "account_clean_funder_blockers": 0,
                "account_pause_operations": 0,
                "account_recovery_incidents": 0,
                "account_recovery_manifests": 0,
            });
            let stages = [
                "trigger",
                "truth_freeze",
                "coverage",
                "attribution",
                "drift",
                "recipe_plan",
                "dataset_seal",
                "training",
                "calibration",
                "cpcv",
                "validation",
                "comparison",
                "shadow_bind",
                "shadow",
                "decision",
            ];
            let stage_evidence = stages
                .into_iter()
                .enumerate()
                .map(|(index, stage)| {
                    json!({
                        "stage": stage,
                        "started_event_sequence": index,
                        "event_sequence": index + 1,
                        "research_job_id": Uuid::now_v7(),
                        "attempt_ordinal": 1,
                        "max_attempts": 1,
                        "started_at": timestamp,
                        "last_heartbeat_at": timestamp,
                        "finished_at": timestamp,
                        "duration_millis": 1,
                        "evidence_uri": null,
                        "evidence_hash": digest,
                        "event_hash": digest,
                        "occurred_at": timestamp,
                    })
                })
                .collect::<Vec<_>>();
            let market_ids = (0..10)
                .map(|index| {
                    format!(
                        "feedback-closure-report-{}-market-{}",
                        if index < 5 { "crypto" } else { "weather" },
                        index % 5 + 1
                    )
                })
                .collect::<Vec<_>>();
            let recommendation_ids = (0..10)
                .map(|_| Uuid::now_v7().to_string())
                .collect::<Vec<_>>();
            let routes_by_market = market_ids
                .iter()
                .enumerate()
                .map(|(index, market_id)| {
                    (
                        market_id.clone(),
                        json!(if index < 5 { "crypto" } else { "weather" }),
                    )
                })
                .collect::<JsonMap<_, _>>();
            let recommendations = market_ids
                .iter()
                .zip(&recommendation_ids)
                .enumerate()
                .map(|(index, (market_id, recommendation_id))| {
                    json!({
                        "recommendation_id": recommendation_id,
                        "market_id": market_id,
                        "route": if index < 5 { "crypto" } else { "weather" },
                    })
                })
                .collect::<Vec<_>>();
            let resolution_facts = market_ids
                .iter()
                .map(|market_id| {
                    json!({
                        "market_id": market_id,
                        "resolved_outcome": "Yes",
                        "resolved_at": resolved_at,
                        "observed_at": observed_at,
                        "source_checkpoint_hash": digest,
                        "resolution_fact_hash": digest,
                    })
                })
                .collect::<Vec<_>>();
            Self {
                timestamp,
                resolved_at,
                observed_at,
                digest,
                report_id,
                cycle_id,
                runtime_control,
                money_path,
                stage_evidence,
                market_ids,
                recommendation_ids,
                routes_by_market,
                recommendations,
                resolution_facts,
            }
        }
    }

    impl ClosureManifestParts {
        fn successor(&self, route: &str, ids: &[String]) -> JsonValue {
            let digest = &self.digest;
            let timestamp = self.timestamp;
            let observed_at = self.observed_at;
            let report_id = &self.report_id;
            let route_id = ReportRouteRunId::from_v7();
            let model_id = ModelVersionId::from_v7();
            let profile_ref: ResearchProfileRef = serde_json::from_value(json!({
                "id": if route == "crypto" { "crypto_price_15m_bootstrap_trade" } else { "weather_forecast_24h_bootstrap_trade" },
                "version": 1, "content_hash": digest,
            })).expect("valid manifest profile identity");
            let decision_at = timestamp
                .parse::<DateTime<Utc>>()
                .expect("manifest decision time");
            let terminal = observed_at
                .parse::<DateTime<Utc>>()
                .expect("manifest terminal time");
            let economic_outcomes = ids
                .iter()
                .map(|id| {
                    SuccessorEconomicIdentity {
                        recommendation_id: id
                            .parse::<RecommendationId>()
                            .expect("manifest recommendation id"),
                        recommendation_report_id: report_id
                            .parse::<RecommendationReportId>()
                            .expect("manifest report id"),
                        report_route_run_id: route_id,
                        decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                        economic_tier_id: EconomicTierId::from_v7(),
                        model_version_id: model_id,
                        trade_policy_artifact_id: TradePolicyArtifactId::from_v7(),
                        research_profile_artifact_id: profile_ref.artifact_id(),
                        decision_at,
                        horizon_at: decision_at
                            + ChronoDuration::seconds(if route == "crypto" { 900 } else { 86400 }),
                        passive_entry: false,
                        resolution_knowledge_lag: ChronoDuration::zero(),
                    }
                    .manifest_outcome(terminal)
                })
                .collect::<Vec<_>>();
            json!({
                "route": route,
                "report_route_run_id": route_id,
                "profile_ref": profile_ref,
                "model_version_id": model_id,
                "recommendation_ids": ids,
                "resolution_outcome_hashes": ids.iter().map(|_| digest.clone()).collect::<Vec<_>>(),
                "economic_outcome_count": economic_outcomes.len(),
                "economic_outcomes": economic_outcomes,
                "model_learning_eligible_count": ids.len(),
                "policy_evaluation_eligible_count": ids.len(),
                "execution_learning_censored_count": ids.len(),
                "execution_censor_reason": "execution_outcome_unavailable_at_cutoff",
            })
        }

        fn into_value(self) -> JsonValue {
            let route_cohorts = [
                self.successor("crypto", &self.recommendation_ids[..5]),
                self.successor("weather", &self.recommendation_ids[5..]),
            ];
            let readiness_capture = readiness_capture_fixture(self.timestamp);
            let Self {
                timestamp,
                resolved_at,
                observed_at,
                digest,
                report_id,
                cycle_id,
                runtime_control,
                money_path,
                stage_evidence,
                market_ids,
                recommendation_ids: _,
                routes_by_market,
                recommendations,
                resolution_facts,
            } = self;
            json!({
                "format_version": 1,
                "evidence_boundary": {
                    "evidence_scope": "owned_disposable_only",
                    "production_composed_binary": true,
                    "operational_activation_claimed": false,
                    "model_route_commit_scope": "disposable_fixture_only",
                    "outbound_write_endpoints": "owned_loopback_rejectors",
                    "runtime_control_before": runtime_control,
                    "runtime_control_after": runtime_control,
                    "execution_authority_unchanged": true,
                    "money_path_before": money_path,
                    "money_path_after": money_path,
                    "real_venue_order_write_count": 0,
                    "real_chain_write_count": 0,
                    "real_capital_write_count": 0,
                    "relayer_request_count": 0,
                },
                "disposable_model_route_commit": {
                    "data": {
                        "receipt": {"execution_authority_unchanged": true},
                        "replayed": false,
                    }
                },
                "closure": {
                    "feedback_cycle_id": cycle_id,
                    "champion_model_version_id": Uuid::now_v7(),
                    "candidate_model_version_id": Uuid::now_v7(),
                    "candidate_manifest_id": Uuid::now_v7(),
                    "candidate_manifest_hash": digest,
                    "scenario_model_bindings_hash": digest,
                    "portfolio_scenario_model_bindings": [{
                        "portfolio_scenario_model_artifact_id": Uuid::now_v7(),
                        "ordered_routes": ["pooled", "crypto", "weather"],
                        "route_set_digest": digest,
                        "serving_contract_digest": digest,
                        "calibration_contract_digest": digest,
                        "recommendation_contract_digest": digest,
                        "scenario_model_schema_version": 1,
                        "capital_time_bucket_contract_digest": digest,
                        "model_content_hash": digest,
                        "bound_at": timestamp,
                    }],
                    "stage_evidence": stage_evidence,
                },
                "data_plane_stability": {
                    "expected_shards": 8,
                    "active_connections": 8,
                    "connection_high_water": 8,
                    "concurrency_bound": 16,
                    "baseline_accepted_connections": 8,
                    "final_accepted_connections": 8,
                    "accepted_connection_delta": 0,
                    "allowed_turnover": 0,
                    "forbidden_runtime_failures": 0,
                },
                "readiness_capture": readiness_capture,
                "pre_activation_parity": [{
                    "run_id": Uuid::now_v7(),
                    "kind": "full",
                    "report_id": null,
                    "total_count": 10,
                    "compared_count": 10,
                    "matched_count": 10,
                    "finished_at": timestamp,
                    "latch_state_id": Uuid::now_v7(),
                }],
                "historical_economic_backfill": HistoricalEconomicBackfill::fixture(),
                "permit": {"data": {"permit_id": Uuid::now_v7()}},
                "report_universe": {
                    "decision_at": timestamp,
                    "knowledge_lag_secs": 2,
                    "market_ids": market_ids,
                    "routes_by_market": routes_by_market,
                },
                "report": {
                    "run": {"data": {"output_report_id": report_id}},
                    "recommendations": {"code": 200, "message": "ok", "data": recommendations},
                    "funnel": {"data": {"conserved": true}},
                    "feature_nulls": [],
                },
                "report_parity": {
                    "run_id": Uuid::now_v7(),
                    "kind": "sampled",
                    "report_id": report_id,
                    "total_count": 10,
                    "compared_count": 10,
                    "matched_count": 10,
                    "finished_at": timestamp,
                    "latch_state_id": Uuid::now_v7(),
                },
                "resolution_plane": {
                    "report_id": report_id,
                    "report_decision_at": timestamp,
                    "resolved_at": resolved_at,
                    "observed_at": observed_at,
                    "facts": resolution_facts,
                },
                "successor_feedback": {
                    "parent_cycle_id": cycle_id,
                    "decision_window_start": timestamp,
                    "decision_cutoff": resolved_at,
                    "truth_cutoff": observed_at,
                    "route_cohorts": route_cohorts,
                },
            })
        }
    }

    #[test]
    fn closure_requires_http_envelope() -> Result<()> {
        let baseline = ClosureManifestFixture::default().0;
        validate_closure_manifest(&serde_json::to_vec(&baseline)?)?;
        for (case, envelope) in [
            (
                "bare_array",
                baseline["report"]["recommendations"]["data"].clone(),
            ),
            ("null_envelope", JsonValue::Null),
            ("missing_data", json!({"code": 200, "message": "ok"})),
            ("null_data", json!({"data": null})),
            ("object_data", json!({"data": {}})),
            ("string_data", json!({"data": "recommendations"})),
            ("boolean_data", json!({"data": true})),
            ("number_data", json!({"data": 10})),
        ] {
            let mut manifest = baseline.clone();
            manifest["report"]["recommendations"] = envelope;
            let error = validate_closure_manifest(&serde_json::to_vec(&manifest)?)
                .expect_err("noncanonical recommendation envelope must fail closed");
            assert!(
                error.to_string().contains("recommendations.data"),
                "{case}: {error}"
            );
        }
        let mut missing = baseline;
        missing["report"]
            .as_object_mut()
            .context("report fixture is not an object")?
            .remove("recommendations");
        let error = validate_closure_manifest(&serde_json::to_vec(&missing)?)
            .expect_err("missing recommendations must fail closed");
        assert!(error.to_string().contains("recommendations.data"));
        Ok(())
    }

    #[test]
    fn closure_rejects_report_drift() -> Result<()> {
        let baseline = ClosureManifestFixture::default().0;
        validate_closure_manifest(&serde_json::to_vec(&baseline)?)?;
        let recommendations = baseline["report"]["recommendations"]["data"]
            .as_array()
            .context("canonical recommendation envelope")?;
        assert_eq!(recommendations.len(), 10);
        for (path, value) in [
            ("/report/recommendations/data", json!(recommendations[..9])),
            ("/report/recommendations/data", json!([])),
            (
                "/report/recommendations/data/1/recommendation_id",
                recommendations[0]["recommendation_id"].clone(),
            ),
            (
                "/report/recommendations/data/1/market_id",
                recommendations[0]["market_id"].clone(),
            ),
            ("/report/recommendations/data/0/route", json!("weather")),
            (
                "/report/recommendations/data/0/recommendation_id",
                json!(Uuid::now_v7()),
            ),
            ("/report/funnel/data/conserved", json!(false)),
            ("/report/feature_nulls", json!([{}])),
            ("/report_parity/report_id", json!(Uuid::now_v7())),
            ("/report_parity/matched_count", json!(9)),
            ("/report_parity/total_count", json!(0)),
            (
                "/successor_feedback/route_cohorts/0/model_learning_eligible_count",
                json!(4),
            ),
        ] {
            let mut manifest = baseline.clone();
            *manifest
                .pointer_mut(path)
                .with_context(|| format!("fixture omitted {path}"))? = value;
            assert!(
                validate_closure_manifest(&serde_json::to_vec(&manifest)?).is_err(),
                "canonical HTTP envelope must retain the {path} invariant"
            );
        }
        Ok(())
    }

    #[test]
    fn closure_rejects_readiness_tamper() -> Result<()> {
        let mut manifest = ClosureManifestFixture::default().0;
        manifest["readiness_capture"]["evidence"]["payload_hash"] =
            json!(ContentHash::from_bytes([9; 32]));
        let manifest: GovernedClosureManifest = serde_json::from_value(manifest)?;
        let error = manifest
            .validate()
            .err()
            .context("tampered readiness payload hash must be rejected")?;
        assert_eq!(
            error.to_string(),
            "closure readiness payload hash does not match the embedded typed payload"
        );
        Ok(())
    }

    #[test]
    fn closure_rejects_readiness_registry() -> Result<()> {
        let mut manifest = ClosureManifestFixture::default().0;
        manifest["readiness_capture"]["evidence"]["payload_json"]["evidence"]["registry_hash"] =
            json!(ContentHash::from_bytes([9; 32]));
        reseal_readiness_payload(&mut manifest)?;
        let manifest: GovernedClosureManifest = serde_json::from_value(manifest)?;
        let error = manifest
            .validate()
            .err()
            .context("tampered readiness registry must be rejected")?;
        assert_eq!(
            error.to_string(),
            "closure readiness retention payload does not match the canonical research source registry"
        );
        Ok(())
    }

    #[test]
    fn closure_rejects_unproven_readiness() -> Result<()> {
        let mut manifest = ClosureManifestFixture::default().0;
        let observed_at = manifest["readiness_capture"]["evidence"]["observed_at"]
            .as_str()
            .context("readiness fixture omitted observed_at")?
            .parse::<DateTime<Utc>>()?;
        let earliest =
            (observed_at - ChronoDuration::days(199)).to_rfc3339_opts(SecondsFormat::Secs, true);
        let observations =
            manifest["readiness_capture"]["evidence"]["payload_json"]["evidence"]["observations"]
                .as_array_mut()
                .context("readiness fixture omitted observations")?;
        for observation in observations {
            observation["earliest_event_time"] = json!(earliest);
        }
        manifest["readiness_capture"]["evidence"]["payload_json"]["evidence"]["measured_history_days"] =
            json!(199);
        reseal_readiness_payload(&mut manifest)?;
        let manifest: GovernedClosureManifest = serde_json::from_value(manifest)?;
        let error = manifest
            .validate()
            .err()
            .context("unproven readiness runway must be rejected")?;
        assert_eq!(
            error.to_string(),
            "closure readiness retention runway is not proven"
        );
        Ok(())
    }

    #[test]
    fn closure_rejects_readiness_derivations() -> Result<()> {
        for field in ["matches_registry", "proven"] {
            let mut manifest = ClosureManifestFixture::default().0;
            manifest["readiness_capture"][field] = json!(true);
            let error = serde_json::from_value::<GovernedClosureManifest>(manifest)
                .err()
                .context("derived readiness field must be rejected")?;
            let detail = error.to_string();
            assert!(
                detail.contains("unknown field") && detail.contains(field),
                "unexpected derived-field error for {field}: {detail}"
            );
        }
        Ok(())
    }

    #[test]
    fn closure_rejects_expired_readiness() -> Result<()> {
        let mut manifest = ClosureManifestFixture::default().0;
        manifest["readiness_capture"]["verified_at"] =
            manifest["readiness_capture"]["evidence"]["expires_at"].clone();
        let manifest: GovernedClosureManifest = serde_json::from_value(manifest)?;
        let error = manifest
            .validate()
            .err()
            .context("expired readiness evidence must be rejected")?;
        assert_eq!(
            error.to_string(),
            "closure readiness evidence was not current at verification time"
        );
        Ok(())
    }

    #[test]
    fn closure_boundary_rejects_writes() -> Result<()> {
        let mut manifest = ClosureManifestFixture::default().0;
        let valid = serde_json::to_vec(&manifest)?;
        validate_closure_manifest(&valid)?;

        manifest["evidence_boundary"]["real_venue_order_write_count"] = json!(1);
        let attempted_write = serde_json::to_vec(&manifest)?;
        assert!(validate_closure_manifest(&attempted_write).is_err());
        Ok(())
    }

    #[test]
    fn closure_requires_count_snapshots() -> Result<()> {
        for field in ["money_path_before", "money_path_after"] {
            let mut manifest = ClosureManifestFixture::default().0;
            manifest["evidence_boundary"]
                .as_object_mut()
                .context("closure evidence boundary is not an object")?
                .remove(field);
            assert!(validate_closure_manifest(&serde_json::to_vec(&manifest)?).is_err());
        }
        let mut manifest = ClosureManifestFixture::default().0;
        manifest["evidence_boundary"]["money_path_after"]
            .as_object_mut()
            .context("closure after snapshot is not an object")?
            .remove("capital_allocations");
        assert!(validate_closure_manifest(&serde_json::to_vec(&manifest)?).is_err());
        Ok(())
    }

    #[test]
    fn closure_requires_economic_truth() -> Result<()> {
        let baseline = ClosureManifestFixture::default().0;
        validate_closure_manifest(&serde_json::to_vec(&baseline)?)?;
        for (field, value) in [
            ("economic_outcome_count", json!(0)),
            ("economic_outcomes", json!([])),
        ] {
            let mut manifest = baseline.clone();
            manifest["successor_feedback"]["route_cohorts"][0][field] = value;
            assert!(validate_closure_manifest(&serde_json::to_vec(&manifest)?).is_err());
        }
        for (field, value) in [
            ("state", json!("censored")),
            ("available_at", json!("2099-01-01T00:00:00Z")),
            ("recommendation_id", json!(Uuid::now_v7())),
            ("evidence_hash", json!(ContentHash::from_bytes([0; 32]))),
        ] {
            let mut manifest = baseline.clone();
            manifest["successor_feedback"]["route_cohorts"][0]["economic_outcomes"][0][field] =
                value;
            assert!(validate_closure_manifest(&serde_json::to_vec(&manifest)?).is_err());
        }
        Ok(())
    }

    #[test]
    fn closure_requires_execution_censor() -> Result<()> {
        let baseline = ClosureManifestFixture::default().0;
        let cohort = &baseline["successor_feedback"]["route_cohorts"][0];
        assert_eq!(cohort["execution_learning_censored_count"], json!(5));
        assert_eq!(
            cohort["execution_censor_reason"],
            json!("execution_outcome_unavailable_at_cutoff")
        );
        assert!(cohort.get("execution_learning_excluded_count").is_none());
        assert!(cohort.get("execution_exclusion_reason").is_none());
        validate_closure_manifest(&serde_json::to_vec(&baseline)?)?;

        for (field, value) in [
            ("execution_learning_censored_count", json!(0)),
            ("execution_censor_reason", json!("execution_not_attempted")),
        ] {
            let mut manifest = baseline.clone();
            manifest["successor_feedback"]["route_cohorts"][0][field] = value;
            assert!(validate_closure_manifest(&serde_json::to_vec(&manifest)?).is_err());
        }

        let mut legacy = baseline;
        legacy["successor_feedback"]["route_cohorts"][0]["execution_learning_excluded_count"] =
            json!(5);
        assert!(validate_closure_manifest(&serde_json::to_vec(&legacy)?).is_err());
        Ok(())
    }

    #[test]
    fn closure_rejects_count_tamper() -> Result<()> {
        let fields = ClosureManifestFixture::default().0["evidence_boundary"]["money_path_after"]
            .as_object()
            .context("closure money-path fixture is not an object")?
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for field in fields {
            let mut manifest = ClosureManifestFixture::default().0;
            let count = manifest["evidence_boundary"]["money_path_after"][&field]
                .as_i64()
                .with_context(|| format!("closure money-path fixture omitted `{field}`"))?;
            manifest["evidence_boundary"]["money_path_after"][&field] = json!(count + 1);
            assert!(validate_closure_manifest(&serde_json::to_vec(&manifest)?).is_err());
        }
        Ok(())
    }

    #[test]
    fn closure_requires_every_section() -> Result<()> {
        for field in [
            "closure",
            "data_plane_stability",
            "readiness_capture",
            "pre_activation_parity",
            "historical_economic_backfill",
            "report_universe",
            "report",
            "report_parity",
            "resolution_plane",
            "successor_feedback",
        ] {
            let mut manifest = ClosureManifestFixture::default().0;
            manifest
                .as_object_mut()
                .context("closure manifest fixture is not an object")?
                .remove(field);
            assert!(validate_closure_manifest(&serde_json::to_vec(&manifest)?).is_err());
        }
        Ok(())
    }

    #[test]
    fn drained_authority_is_authoritative() -> Result<()> {
        let fixture = ClosureManifestFixture::default().0;
        let boundary: DisposableEvidenceBoundary =
            serde_json::from_value(fixture["evidence_boundary"].clone())?;
        let preimage = DisposableBoundaryPreimage {
            runtime_control: boundary.runtime_control_before.clone(),
            money_path_counts: boundary.money_path_before,
        };
        let before_drain = boundary.runtime_control_after;
        preimage.verify_runtime(&before_drain, &before_drain)?;

        let mut changed = before_drain.clone();
        changed.revision += 1;
        assert!(preimage.verify_runtime(&before_drain, &changed).is_err());

        changed = before_drain.clone();
        changed.entry_authorization_policy = EntryAuthorizationPolicy::PolicyAutomatic;
        assert!(preimage.verify_runtime(&before_drain, &changed).is_err());
        assert!(preimage.verify_runtime(&changed, &before_drain).is_err());
        Ok(())
    }

    #[test]
    fn runtime_log_rejects_unknown() {
        assert!(
            ProductionStack::validate_runtime_log(
                "2026-08-27T00:00:00Z ERROR quant_pivot_core::worker: unexpected failure"
            )
            .is_err()
        );
        assert!(
            ProductionStack::validate_runtime_log(
                "2026-08-27T00:00:00Z WARN quant_pivot_core::worker: unexpected warning"
            )
            .is_err()
        );
    }

    #[test]
    fn report_warning_is_rejected() {
        let target = "2026-08-27T00:00:00Z WARN quant_pivot_core::report::coordinator: runtime config changed during report coordinator pass; retrying";
        for line in [
            target.to_owned(),
            format!(
                "{target} error=state conflict for decision_policy_snapshot policy-id: runtime config changed during report schedule operation"
            ),
            format!("{target}; hidden failure"),
        ] {
            assert!(ProductionStack::validate_runtime_log(&line).is_err());
            for fixture in [
                ProductionStackFixture::Browser,
                ProductionStackFixture::GovernedFeedback,
                ProductionStackFixture::FeedbackClosure,
            ] {
                assert!(ProductionStack::validate_fixture_log(&line, fixture).is_err());
            }
        }
    }

    #[test]
    fn runtime_log_allows_containment() -> Result<()> {
        let line = concat!(
            "2026-08-27T00:00:00Z WARN ",
            "quant_pivot_core::service::feature_parity_executor: ",
            "feature parity detected a deterministic online/replay mismatch ",
            "parity_run_id=run-1 sampling_key=report/market stage=\"selection\" ",
            "report_id=None model_run_id=None model_version_id=None market_id=None ",
            "feature_name=None projected_evidence_matched=false online_state=None ",
            "replay_state=None online_value=None replay_value=None online_effective_at=None ",
            "replay_effective_at=None online_available_at=None replay_available_at=None ",
            "online_cutoff=None replay_cutoff=None online_fingerprint=blake3:01 ",
            "replay_fingerprint=blake3:02 detail=Compared { sampling_key: \"fixture\" }"
        );
        ProductionStack::validate_fixture_log(line, ProductionStackFixture::GovernedFeedback)?;
        assert!(
            ProductionStack::validate_fixture_log(line, ProductionStackFixture::FeedbackClosure)
                .is_err()
        );
        assert!(
            ProductionStack::validate_fixture_log(
                &format!("{line}; hidden failure"),
                ProductionStackFixture::GovernedFeedback,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn runtime_log_allows_outage() -> Result<()> {
        let line = concat!(
            "2026-08-27T00:00:00Z ERROR HTTP request{",
            "http.method=GET http.route=/api/auth/me ",
            "exception.message=service unavailable: authentication temporarily unavailable ",
            "exception.details=ServiceUnavailable(\"authentication temporarily unavailable\") ",
            "http.status_code=503 otel.status_code=\"ERROR\"",
            "}: quant_pivot_web::request_tracing: HTTP request failed ",
            "error=ServiceUnavailable(\"authentication temporarily unavailable\")"
        );
        ProductionStack::validate_fixture_log(line, ProductionStackFixture::GovernedFeedback)?;
        assert!(
            ProductionStack::validate_fixture_log(line, ProductionStackFixture::FeedbackClosure,)
                .is_err()
        );
        assert!(
            ProductionStack::validate_fixture_log(
                &line.replace("http.method=GET", "http.method=POST"),
                ProductionStackFixture::GovernedFeedback,
            )
            .is_err()
        );
        assert!(
            ProductionStack::validate_fixture_log(
                &format!("{line}; hidden failure"),
                ProductionStackFixture::GovernedFeedback,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn feedback_drift_is_rejected() {
        let line = concat!(
            "2026-08-27T00:00:00Z WARN ",
            "quant_pivot_core::app::research_job_worker: ",
            "research job execution failed ",
            "job_id=01a04fc8-2a63-77f0-a3a4-347accb88df6 ",
            "kind=feedback_coverage ",
            "error=leakage-aware validation methodology failed: ",
            "decision-time policy or Route generation differs from the frozen feedback cycle"
        );
        for fixture in [
            ProductionStackFixture::Browser,
            ProductionStackFixture::GovernedFeedback,
            ProductionStackFixture::FeedbackClosure,
            ProductionStackFixture::FeedbackClosureRecovery,
        ] {
            assert!(ProductionStack::validate_fixture_log(line, fixture).is_err());
        }
    }

    #[test]
    fn liquidity_role_preserves_owner() -> Result<()> {
        let funder = EvmAddress::parse(FUNDER)?;
        let counterparty = EvmAddress::parse("0x2222222222222222222222222222222222222222")?;
        for (role, maker, taker, expected) in [
            (
                AccountChainExecutionRole::Maker,
                &funder,
                &counterparty,
                true,
            ),
            (
                AccountChainExecutionRole::Maker,
                &counterparty,
                &funder,
                false,
            ),
            (
                AccountChainExecutionRole::Taker,
                &funder,
                &counterparty,
                true,
            ),
            (
                AccountChainExecutionRole::Taker,
                &counterparty,
                &funder,
                false,
            ),
            (AccountChainExecutionRole::SelfMatch, &funder, &funder, true),
            (
                AccountChainExecutionRole::SelfMatch,
                &funder,
                &counterparty,
                false,
            ),
            (
                AccountChainExecutionRole::SelfMatch,
                &counterparty,
                &funder,
                false,
            ),
        ] {
            assert_eq!(
                AccountExecutionOwner {
                    funder: &funder,
                    maker,
                    taker,
                    role
                }
                .matches(),
                expected,
                "liquidity role {role:?} must not reinterpret the order owner",
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn browser_account_holding_matches() -> Result<()> {
        for (fixture, expected_nlv) in [
            (ProductionStackFixture::Browser, dec!(140)),
            (ProductionStackFixture::GovernedFeedback, dec!(5040)),
        ] {
            let upstream = fixture
                .deterministic_upstream(Utc::now(), Arc::new(DeterministicPolygonChain::new()))
                .await?;
            let client = DataApiClient::new(DataApiConfig {
                base_url: upstream.uri(),
                page_size: 1,
                size_threshold: 0,
            })
            .with_http_client(Client::builder().no_proxy().build()?);
            let positions = client.positions(FUNDER).await?;
            assert_eq!(positions.len(), 1);
            let position = &positions[0];
            assert_eq!(position.proxy_wallet.as_deref(), Some(FUNDER));
            assert_eq!(position.asset, BROWSER_SETTLEMENT_TOKEN_ID);
            assert_eq!(position.condition_id, BROWSER_SETTLEMENT_MARKET_ID);
            assert_eq!(position.size, ENTRY_FILLED_SHARES);
            assert_eq!(position.avg_price, ENTRY_PRICE);
            assert_eq!(position.initial_value, ENTRY_FILLED_SHARES * ENTRY_PRICE);
            assert_eq!(position.cur_price, Decimal::ONE);
            assert_eq!(position.current_value, dec!(40));
            assert_eq!(position.current_value - EXECUTION_NOTIONAL, dec!(15));
            assert_eq!(position.cash_pnl, dec!(16));
            assert_eq!(position.realized_pnl, Decimal::ZERO);
            assert_eq!(position.outcome, "Yes");
            assert_eq!(position.outcome_index, 0);
            assert!(position.redeemable);
            assert!(!position.mergeable && !position.negative_risk);
            assert_eq!(
                fixture.account_collateral_usd() + position.current_value,
                expected_nlv
            );
            let requests = upstream
                .received_requests()
                .await
                .context("recorded requests")?;
            assert_eq!(
                requests.len(),
                2,
                "the second page must terminate pagination"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn unfilled_fixtures_are_empty() -> Result<()> {
        for fixture in [
            ProductionStackFixture::Empty,
            ProductionStackFixture::FeedbackClosure,
            ProductionStackFixture::FeedbackClosureRecovery,
        ] {
            let upstream = fixture
                .deterministic_upstream(Utc::now(), Arc::new(DeterministicPolygonChain::new()))
                .await?;
            let client = DataApiClient::new(DataApiConfig {
                base_url: upstream.uri(),
                page_size: 1,
                size_threshold: 0,
            })
            .with_http_client(Client::builder().no_proxy().build()?);
            assert!(client.positions(FUNDER).await?.is_empty());
        }
        Ok(())
    }

    #[test]
    fn head_advances_in_slots() {
        let clock = DeterministicPolygonChain::at(1_777_403_341, StdInstant::now());
        let anchor = clock.head_after(Duration::ZERO);
        let later = clock.head_after(Duration::from_secs(121));

        assert_eq!(anchor.timestamp, 1_777_403_340);
        assert_eq!(later.block_number, anchor.block_number + 60);
        assert_eq!(later.timestamp, anchor.timestamp + 120);
    }

    #[test]
    fn block_history_is_immutable() {
        let clock = DeterministicPolygonChain::at(1_777_403_341, StdInstant::now());
        let anchor = clock.head_after(Duration::ZERO);
        let later = clock.head_after(Duration::from_secs(121));
        let original_number = format!("0x{:x}", anchor.block_number);
        let original =
            deterministic_polygon_block(&json!([original_number]), later).expect("original block");
        let future =
            deterministic_polygon_block(&json!([format!("0x{:x}", later.block_number + 1)]), later)
                .expect("future block response");
        let expected_timestamp = format!("0x{:x}", anchor.timestamp);

        assert_eq!(
            original.get("timestamp").and_then(JsonValue::as_str),
            Some(expected_timestamp.as_str())
        );
        assert_eq!(future, JsonValue::Null);
        assert_eq!(DETERMINISTIC_POLYGON_BLOCK_SECS, 2);
    }

    #[test]
    fn exchange_code_is_pinned() {
        for (contract, fixture) in [
            (
                CTF_EXCHANGE_V2,
                include_str!("../fixtures/polygon-v2/ctf-exchange-v2.hex"),
            ),
            (
                NEG_RISK_EXCHANGE_V2,
                include_str!("../fixtures/polygon-v2/neg-risk-exchange-v2.hex"),
            ),
        ] {
            let bytes = hex::decode(fixture.trim()).expect("decode V2 bytecode fixture");
            assert_eq!(
                blake3::hash(&bytes).to_hex().as_str(),
                contract.bytecode_blake3
            );
        }
    }

    #[tokio::test]
    async fn upstream_serves_clob_v2() -> Result<()> {
        let report_resolves_at = Utc::now() + ChronoDuration::hours(48);
        let polygon = Arc::new(DeterministicPolygonChain::new());
        let upstream = ProductionStackFixture::Empty
            .deterministic_upstream(report_resolves_at, polygon)
            .await?;
        let response = Client::new()
            .get(format!("{}/version", upstream.uri()))
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.json::<JsonValue>().await?, json!({ "version": 2 }));
        Ok(())
    }

    #[tokio::test]
    async fn closure_upstream_preserves_rules() -> Result<()> {
        let upstream = ProductionStackFixture::FeedbackClosure
            .deterministic_upstream(
                Utc::now() + ChronoDuration::hours(CLOSURE_REPORT_HORIZON_HOURS),
                Arc::new(DeterministicPolygonChain::new()),
            )
            .await?;
        let config = PolymarketConfig {
            clob_base_url: upstream.uri(),
            ..PolymarketConfig::default()
        };
        let signer = Arc::new(OrderSigner::from_bytes(&hex::decode(
            PRIVATE_KEY.trim_start_matches("0x"),
        )?)?);
        let topology = WalletTopology::resolve(
            ExecutionWalletKind::Eoa,
            signer.address(),
            FUNDER,
            config.chain_id,
        )?;
        let clob = ClobClient::connect(signer, &config, &topology).await?;
        for (scope, yes_base, no_base) in [
            ("training", 710_000, 810_000),
            ("calibration", 720_000, 820_000),
            ("evaluation", 730_000, 830_000),
            ("shadow", 740_000, 840_000),
            ("report-crypto", 750_000, 850_000),
            ("report-weather", 760_000, 860_000),
        ] {
            let market_id = MarketId::new(format!("feedback-closure-{scope}-market-1"));
            let info = clob.clob_market_info_version(&market_id).await?;
            let rules = PolymarketOrderRules::new(info.tick_size, info.minimum_order_size)?;
            assert_eq!(info.market_id, market_id);
            assert_eq!(rules, CLOSURE_ORDER_RULES);
            assert_eq!(info.neg_risk, CLOSURE_NEG_RISK);
            assert_eq!(info.tokens.len(), 2);
            assert_eq!(info.tokens[0].token_id.as_str(), (yes_base + 1).to_string());
            assert_eq!(info.tokens[1].token_id.as_str(), (no_base + 1).to_string());
            assert_eq!(info.raw_payload["mts"], "0.0025");
            assert_eq!(info.raw_payload["mos"], "1");
            assert_eq!(info.raw_payload["nr"], CLOSURE_NEG_RISK);
            rules.validate_order(
                Side::Buy,
                VenueOrderAmount::Shares(Shares::new(dec!(10))),
                Price::new(dec!(0.5125)),
            )?;
        }
        for market_id in [
            MarketId::new(BROWSER_MARKET_ID),
            MarketId::new(synthetic_condition_id()),
        ] {
            let info = clob.clob_market_info_version(&market_id).await?;
            let rules = PolymarketOrderRules::new(info.tick_size, info.minimum_order_size)?;
            assert_eq!(rules, CENT_ORDER_RULES);
            assert!(!info.neg_risk);
            assert!(
                rules
                    .validate_order(
                        Side::Buy,
                        VenueOrderAmount::Shares(Shares::new(dec!(10))),
                        Price::new(dec!(0.5125)),
                    )
                    .is_err()
            );
        }
        let gamma = Client::new()
            .get(format!("{}/events/keyset", upstream.uri()))
            .query(&[("active", "true"), ("closed", "false")])
            .send()
            .await?
            .error_for_status()?
            .json::<JsonValue>()
            .await?;
        let events = gamma["events"].as_array().context("Gamma events")?;
        assert_eq!(events.len(), 2);
        for event in events {
            assert_eq!(event["negRisk"], CLOSURE_NEG_RISK);
            let markets = event["markets"].as_array().context("Gamma markets")?;
            assert_eq!(markets.len(), 5);
            for market in markets {
                assert_eq!(market["orderPriceMinTickSize"], "0.0025");
                assert_eq!(market["orderMinSize"], "1");
                assert_eq!(market["negRisk"], CLOSURE_NEG_RISK);
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn attestor_proves_archive() -> Result<()> {
        let polygon = Arc::new(DeterministicPolygonChain::new());
        let registered = polygon.register_tokens(&[750_001], polygon.head())?;
        let history = HistoryUpstreams::start(Arc::clone(&polygon)).await?;
        polygon.freeze();
        let mut config = FinalizedExchangeHistoryConfig::default();
        config.attestor.rpc_endpoint = PolygonRpcEndpoint::Public {
            url: history.attestor.uri(),
        };
        config.attestor.max_blocks_per_log_request = 50_000;
        config.hypersync.endpoint = history.hypersync.uri();
        config.hypersync.api_token = HYPERSYNC_TOKEN.into();
        let attestor = ExchangeHistoryAttestor::connect(&config)?;
        let extractor = ExchangeHistoryExtractor::connect(&config)?;
        let probe = attestor.probe_archive().await?;
        let (extracted, attested) = tokio::try_join!(
            extractor.fetch_chunk(registered.from_block, registered.to_block),
            attestor.fetch_chunk(registered.from_block, registered.to_block),
        )?;

        assert!(probe.finalized_head.number > CTF_EXCHANGE_V2.first_valid_block);
        assert_eq!(probe.contract_code_hashes.len(), 2);
        assert!(chunks_agree(&extracted, &attested));
        let projection = project_history(
            &extracted.logs,
            extracted.observed_at_millis,
            attested.observed_at_millis,
            ContentHash::from_bytes([7; 32]),
            Uuid::now_v7(),
            |_| Some(MarketId::new("feedback-closure-report-crypto-market-1")),
        )?;
        assert_eq!(projection.executions.len(), 20);
        Ok(())
    }

    #[test]
    fn gamma_horizon_is_subscribable() -> Result<()> {
        let now = Utc::now();
        let report_resolves_at = now + ChronoDuration::hours(CLOSURE_REPORT_HORIZON_HOURS);
        let gamma =
            ProductionStackFixture::FeedbackClosure.deterministic_gamma(report_resolves_at)?;
        let events = gamma["events"].as_array().expect("Gamma events");
        let expected_end = report_resolves_at.to_rfc3339_opts(SecondsFormat::Millis, true);

        assert_eq!(events.len(), 2);
        for (scope, event) in ["report-crypto", "report-weather"].into_iter().zip(events) {
            assert_eq!(event["endDate"].as_str(), Some(expected_end.as_str()));
            let markets = event["markets"].as_array().expect("Gamma markets");
            assert_eq!(markets.len(), 5);
            for (index, market) in markets.iter().enumerate() {
                let ordinal = index + 1;
                let (question, description) = closure_market_text(scope, ordinal)?;
                assert_eq!(market["endDate"].as_str(), Some(expected_end.as_str()));
                assert_eq!(market["question"].as_str(), Some(question.as_str()));
                assert_eq!(market["description"].as_str(), description.as_deref());
            }
        }
        assert!(
            report_resolves_at - now < ChronoDuration::hours(72),
            "closure report markets must remain inside the production subscription window"
        );
        Ok(())
    }
}

#[derive(Deserialize)]
struct JsonRpcRequest {
    id: JsonValue,
    method: String,
    #[serde(default)]
    params: JsonValue,
}

#[derive(Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: JsonValue,
    result: JsonValue,
}

impl ProductionConfigRender<'_> {
    fn render(self) -> Result<()> {
        let source_path = self.workspace_root.join("config/quant-pivot.toml");
        let source = fs::read_to_string(&source_path)
            .with_context(|| format!("read canonical deploy config {}", source_path.display()))?;
        let mut config: Value = toml::from_str(&source)
            .with_context(|| format!("parse canonical deploy config {}", source_path.display()))?;
        configure_upstreams(&mut config, self.upstream, self.clob_upstream, self.history)?;
        configure_test_identity(&mut config, self.artifact_store)?;
        configure_infrastructure(&mut config, self.stack)?;
        configure_web(&mut config, self.listen_port)?;

        let rendered =
            toml::to_string_pretty(&config).context("serialize production-stack config")?;
        let config_path = self.run_dir.join("quant-pivot.toml");
        fs::write(&config_path, rendered)
            .with_context(|| format!("write production-stack config {}", config_path.display()))?;
        fs::set_permissions(&config_path, Permissions::from_mode(0o600)).with_context(|| {
            format!(
                "restrict production-stack config permissions {}",
                config_path.display()
            )
        })?;
        let request =
            DeployConfigLoadRequest::new(config_path, DeploymentEnvironment::local_development());
        DeployConfig::load(&request).context("validate generated production-stack config")?;
        Ok(())
    }
}

fn configure_upstreams(
    config: &mut Value,
    upstream: &MockServer,
    clob_upstream: &DeterministicClobServer,
    history: Option<&HistoryUpstreams>,
) -> Result<()> {
    let upstream_url = upstream.uri();
    set(
        config,
        &["polymarket", "clob_base_url"],
        upstream_url.clone(),
    )?;
    set(
        config,
        &["polymarket", "clob_ws_url"],
        clob_upstream.base_url(),
    )?;
    set(
        config,
        &["polymarket", "onchain", "rpc_endpoint"],
        Value::Table(Table::from_iter([
            ("source".to_owned(), Value::String("public".to_owned())),
            ("url".to_owned(), Value::String(upstream_url.clone())),
        ])),
    )?;
    set(
        config,
        &["market_data", "gamma", "base_url"],
        upstream_url.clone(),
    )?;
    set(
        config,
        &["market_data", "data_api", "base_url"],
        upstream_url.clone(),
    )?;
    set(
        config,
        &["polymarket", "relayer", "base_url"],
        format!("{upstream_url}/__fixture__/relayer-denied"),
    )?;
    set(
        config,
        &["polymarket", "relayer", "request_timeout_ms"],
        250_i64,
    )?;
    remove(config, &["polymarket", "relayer", "api_key"])?;
    remove(config, &["polymarket", "relayer", "api_key_address"])?;
    set(
        config,
        &["market_data", "finalized_exchange_history", "enabled"],
        history.is_some(),
    )?;
    if let Some(history) = history {
        set(
            config,
            &["market_data", "gamma", "reconcile_interval_secs"],
            CLOSURE_GAMMA_RECONCILE_SECS,
        )?;
        set(
            config,
            &[
                "market_data",
                "finalized_exchange_history",
                "hypersync",
                "endpoint",
            ],
            HYPERSYNC_ENDPOINT,
        )?;
        set(
            config,
            &[
                "market_data",
                "finalized_exchange_history",
                "hypersync",
                "api_token",
            ],
            HYPERSYNC_TOKEN,
        )?;
        set(
            config,
            &[
                "market_data",
                "finalized_exchange_history",
                "attestor",
                "max_blocks_per_log_request",
            ],
            50_000,
        )?;
        set(
            config,
            &[
                "market_data",
                "finalized_exchange_history",
                "min_blocks_per_chunk",
            ],
            50_000,
        )?;
        set(
            config,
            &[
                "market_data",
                "finalized_exchange_history",
                "max_blocks_per_chunk",
            ],
            50_000,
        )?;
        set(
            config,
            &[
                "market_data",
                "finalized_exchange_history",
                "hot_window_blocks_per_tick",
            ],
            1_500_000,
        )?;
        set(
            config,
            &[
                "market_data",
                "finalized_exchange_history",
                "attestor",
                "rpc_endpoint",
            ],
            Value::Table(Table::from_iter([
                ("source".to_owned(), Value::String("public".to_owned())),
                ("url".to_owned(), Value::String(history.attestor.uri())),
            ])),
        )?;
    }
    for source in DISABLED_DOMAIN_SOURCES {
        set(config, &["domain_sources", source, "enabled"], false)?;
    }
    for binding in [
        "hko_rainfall",
        "hko_daily_temperature",
        "airnow_pm25_reporting_areas",
        "airnow_pm25_sites",
        "tornado_regions",
        "nhc_historical_storms",
        "nws_wind_stations",
    ] {
        set(
            config,
            &["domain_sources", "weather_vertical_bindings", binding],
            Value::Array(Vec::new()),
        )?;
    }
    set(
        config,
        &["domain_sources", "weather_stations"],
        Value::Table(Table::new()),
    )?;
    Ok(())
}

fn configure_test_identity(
    config: &mut Value,
    artifact_store: &ArtifactStoreDeployConfig,
) -> Result<()> {
    set(config, &["keys", "private_key"], PRIVATE_KEY)?;
    set(config, &["quant", "account", "funder"], FUNDER)?;
    set(
        config,
        &["research", "artifact_store", "kind"],
        match artifact_store.kind {
            ArtifactStoreKind::Local => "local",
            ArtifactStoreKind::S3 => "s3",
        },
    )?;
    set(
        config,
        &["research", "artifact_store", "bucket"],
        artifact_store.bucket.clone(),
    )?;
    set(
        config,
        &["research", "artifact_store", "prefix"],
        artifact_store.prefix.clone(),
    )?;
    set(
        config,
        &["research", "artifact_store", "region"],
        artifact_store.region.clone(),
    )?;
    set(
        config,
        &["research", "artifact_store", "path_style"],
        artifact_store.path_style,
    )?;
    set(
        config,
        &["research", "artifact_store", "require_object_lock"],
        artifact_store.require_object_lock,
    )?;
    set(
        config,
        &["research", "artifact_store", "require_versioning"],
        artifact_store.require_versioning,
    )?;
    match &artifact_store.endpoint {
        Some(endpoint) => set(
            config,
            &["research", "artifact_store", "endpoint"],
            endpoint.clone(),
        )?,
        None => remove(config, &["research", "artifact_store", "endpoint"])?,
    }
    set(
        config,
        &["research", "evidence_attestation", "signing_key"],
        SYSTEM_EVIDENCE_SIGNING_KEY,
    )?;
    Ok(())
}

fn configure_infrastructure(config: &mut Value, stack: &SystemStack) -> Result<()> {
    set(
        config,
        &["db", "postgres", "host"],
        stack.postgres_config.host.clone(),
    )?;
    set(
        config,
        &["db", "postgres", "port"],
        i64::from(stack.postgres_config.port),
    )?;
    set(
        config,
        &["db", "postgres", "user"],
        stack.postgres_config.user.clone(),
    )?;
    set(
        config,
        &["db", "postgres", "password"],
        stack.postgres_config.password.expose_secret(),
    )?;
    set(
        config,
        &["db", "postgres", "database"],
        stack.postgres_config.database.clone(),
    )?;
    set(
        config,
        &["db", "postgres", "schema"],
        stack.postgres_config.schema.clone(),
    )?;
    set(
        config,
        &["db", "postgres", "application_name"],
        "quant-pivot-production-stack",
    )?;
    set(
        config,
        &["db", "postgres", "max_connections"],
        CLOSURE_POSTGRES_MAX_CONNECTIONS,
    )?;
    set(
        config,
        &["db", "postgres", "max_lifetime_secs"],
        i64::try_from(CLOSURE_POSTGRES_MAX_LIFETIME_SECS)?,
    )?;
    set(
        config,
        &["db", "clickhouse", "deployment_id"],
        stack.clickhouse_config.deployment_id.clone(),
    )?;
    set(
        config,
        &["db", "clickhouse", "cluster_id"],
        stack.clickhouse_config.cluster_id.clone(),
    )?;
    set(
        config,
        &["db", "clickhouse", "url"],
        stack.clickhouse_config.url.clone(),
    )?;
    set(
        config,
        &["db", "clickhouse", "database"],
        stack.clickhouse_config.database.clone(),
    )?;
    set(
        config,
        &["db", "clickhouse", "user"],
        stack.clickhouse_config.user.clone(),
    )?;
    set(
        config,
        &["db", "clickhouse", "password"],
        stack.clickhouse_config.password.expose_secret(),
    )?;
    set(
        config,
        &["cache", "redis", "host"],
        stack.redis_config.host.clone(),
    )?;
    set(
        config,
        &["cache", "redis", "port"],
        i64::from(stack.redis_config.port),
    )?;
    set(
        config,
        &["cache", "redis", "key_prefix"],
        stack.redis_config.key_prefix.clone(),
    )?;
    set(
        config,
        &["cache", "operation_timeout_ms"],
        CLOSURE_CACHE_OPERATION_TIMEOUT_MS,
    )?;
    set(
        config,
        &["cache", "domains", "market", "timeout_ms"],
        CLOSURE_CACHE_OPERATION_TIMEOUT_MS,
    )?;
    set(
        config,
        &["cache", "redis", "pool_size"],
        CLOSURE_REDIS_POOL_SIZE,
    )?;
    set(
        config,
        &["cache", "redis", "timeout_ms"],
        CLOSURE_REDIS_TIMEOUT_MS,
    )?;
    Ok(())
}

fn configure_web(config: &mut Value, listen_port: u16) -> Result<()> {
    set(config, &["web", "listen_host"], "127.0.0.1")?;
    set(config, &["web", "listen_port"], i64::from(listen_port))?;
    set(config, &["web", "serve_static_ui"], false)?;
    set(
        config,
        &["web", "cors_allowed_origins"],
        Value::Array(vec![Value::String("http://127.0.0.1:6099".to_owned())]),
    )?;
    set(config, &["web", "jwt", "signing_key"], JWT_SIGNING_KEY)?;
    Ok(())
}

fn set<T>(root: &mut Value, path: &[&str], value: T) -> Result<()>
where
    T: Into<Value>,
{
    let (key, parents) = path
        .split_last()
        .context("configuration mutation path cannot be empty")?;
    let mut cursor = root;
    for parent in parents {
        cursor = cursor
            .get_mut(*parent)
            .with_context(|| format!("configuration section is missing: {parent}"))?;
    }
    let table = cursor
        .as_table_mut()
        .with_context(|| format!("configuration parent is not a table: {}", parents.join(".")))?;
    table.insert((*key).to_owned(), value.into());
    Ok(())
}

fn remove(root: &mut Value, path: &[&str]) -> Result<()> {
    let (key, parents) = path
        .split_last()
        .context("configuration mutation path cannot be empty")?;
    let mut cursor = root;
    for parent in parents {
        cursor = cursor
            .get_mut(*parent)
            .with_context(|| format!("configuration section is missing: {parent}"))?;
    }
    let table = cursor
        .as_table_mut()
        .with_context(|| format!("configuration parent is not a table: {}", parents.join(".")))?;
    table.remove(*key);
    Ok(())
}

async fn await_startup(child: &mut Child, base_url: &str) -> Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("build startup probe client")?;
    let startup_url = format!("{base_url}/startup");
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().context("inspect production binary")? {
            bail!("production binary exited during startup with {status}");
        }
        let response = tokio::select! {
            biased;
            status = child.wait() => {
                let status = status.context("wait for production binary during startup")?;
                bail!("production binary exited during startup with {status}");
            }
            response = send_bounded(
                client.get(&startup_url),
                "production startup probe",
                READINESS_REQUEST_TIMEOUT,
            ) => response,
        };
        if let Ok(response) = response
            && response.status().is_success()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("startup probe exceeded {STARTUP_TIMEOUT:?}: {startup_url}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn reserve_port() -> Result<u16> {
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .context("reserve local production-stack port")?;
    let port = listener
        .local_addr()
        .context("read reserved production-stack port")?
        .port();
    drop(listener);
    Ok(port)
}

fn ensure_port_available(port: u16) -> Result<()> {
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
        .with_context(|| format!("production-stack port {port} is already occupied"))?;
    drop(listener);
    Ok(())
}

async fn termination_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            unix::signal(SignalKind::terminate()).context("install SIGTERM listener")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("listen for Ctrl-C")?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.context("listen for Ctrl-C")?;
    Ok(())
}
