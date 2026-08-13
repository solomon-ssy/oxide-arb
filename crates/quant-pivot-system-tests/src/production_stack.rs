//! Real-binary system fixture backed by disposable infrastructure.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    fs::{self, OpenOptions, Permissions},
    future::{Future, pending},
    io::Write,
    net::{Ipv4Addr, SocketAddrV4, TcpListener},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::Arc,
    time::{Duration, Instant as StdInstant},
};

use anyhow::{Context, Error as AnyhowError, Result, bail, ensure};
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
use quant_pivot_core::service::feedback_cohort::evaluate_feedback_cohort;
use quant_pivot_models::{
    config::{
        ArtifactStoreDeployConfig, ArtifactStoreKind, ClickHouseConfig, DeployConfig,
        DeployConfigLoadRequest,
    },
    domain::{
        api::ModelVersionListQuery,
        pagination::PageRequest,
        quant::{
            FEEDBACK_COHORT_PAGE_LIMIT, FeedbackCohortCandidate, FeedbackCohortDecision,
            FeedbackCohortEvidence, FeedbackCohortPageQuery, FeedbackCohortSnapshot,
            FeedbackCohortWindow, FeedbackSchedulerControl, NewFeedbackSchedulerState,
            PortfolioDecisionResult, RecommendationResolutionOutcomeInfo,
        },
    },
    entities::{
        market::Entity as MarketEntity,
        quant_feature_parity_run::{
            Column as FeatureParityRunColumn, Entity as FeatureParityRunEntity,
        },
        quant_model_route_shadow_binding::{
            Column as ShadowBindingColumn, Entity as ShadowBindingEntity,
        },
        quant_portfolio_plan::Entity as PortfolioPlanEntity,
        quant_recommendation::{Column as RecommendationColumn, Entity as RecommendationEntity},
        quant_recommendation_report::{
            Column as RecommendationReportColumn, Entity as RecommendationReportEntity,
        },
        quant_report_route_run::Entity as ReportRouteRunEntity,
        quant_settlement_redeem::Entity as QuantSettlementRedeemEntity,
    },
    enums::{
        market::MarketStatus,
        quant::{
            CohortExclusionReason, FeatureParityLatchState, FeatureParityRunKind,
            FeatureParityRunStatus, FeedbackCohort, FeedbackDecision, FeedbackStage,
            FeedbackStageEventKind, RecommendationReportStatus, ShadowBindingStatus,
        },
        settlement::SettlementEffectivePolicy,
    },
    runtime_config::{ActivePolicyBundle, BuyModelRoute},
    types::{
        ContentHash, DeploymentEnvironment, FeatureParityRunId, FeatureParityStateId,
        FeedbackCycleId, MarketId, ModelVersionId, RecommendationId, RecommendationReportId,
        ReportRouteRunId, ResearchJobId, ResearchProfileRef, TradeTapeSourceEvidence, WorkerId,
        builtin_research_profiles,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgExecutionSubmissionRepository, PgFeatureParityRepository, PgFeedbackCohortRepository,
        PgFeedbackCycleRepository, PgFeedbackSchedulerRepository, PgModelRegistryRepository,
        PgPolicyRepository, PgRecommendationResolutionOutcomeRepository,
        policy_bootstrap::ensure_default_policy_bundle,
    },
    traits::{
        FeatureParityRepository, FeedbackCohortRepository, FeedbackCycleRepository,
        FeedbackSchedulerRepository, ModelRegistryRepository, PolicyRepository,
        RecommendationResolutionOutcomeRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore, S3ArtifactStore, S3StaticCredentials},
    model::ModelArtifact,
};
use reqwest::{Client, Response, StatusCode};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
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
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use toml::{Table, Value};
use uuid::Uuid;
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{method, path, path_regex},
};

use crate::{
    performance::upstream::{DeterministicClobRefreshHandle, DeterministicClobServer},
    postgres::PostgresClock,
    stack::{BOOTSTRAP_ADMIN_PASSWORD, SystemStack},
    support::execution_pg_seed::{
        FeedbackServingFixtureConfig, ReportSeedConfig, SharedDemoInfra, enable_test_admission,
        fill_entry_lot, fixture_profile_ref, seed_approved_intent, seed_demo_with_store,
        seed_feedback_serving_infra, seed_pending_intent, seed_production_report,
    },
    support::feedback_closure_seed::{
        CLOSURE_REPORT_HORIZON_HOURS, FeedbackClosureFixture, FeedbackClosureOutcome,
        FeedbackClosureSeedRequest, FeedbackReportBookSnapshot, FeedbackReportResolutionEvidence,
        FeedbackReportUniverse, closure_market_text, complete_feedback_closure,
        prepare_feedback_report_universe, seed_feedback_closure, settle_feedback_report_universe,
    },
    support::portfolio_scenario_fixtures::finalize_feedback_portfolio,
    support::research_browser_seed::{
        BrowserResearchFixture, seed_browser_research, seed_closure_feedback_research,
        seed_governed_feedback_research,
    },
    support::trade_policy_fixtures::SYSTEM_EVIDENCE_SIGNING_KEY,
};

const PRIVATE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const FUNDER: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";
const API_KEY: &str = "00000000-0000-0000-0000-000000000000";
const API_PASSPHRASE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const API_SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const JWT_SIGNING_KEY: &str = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc";
const ERC1967_IMPLEMENTATION_SLOT: &str =
    "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc";
const DETERMINISTIC_POLYGON_HEAD_BLOCK: u64 = 0x0400_0000;
const DETERMINISTIC_POLYGON_BLOCK_SECS: i64 = 2;
// The full-stack fixture deliberately exercises the explicit source-unavailable
// serving path. Keep this single constant shared by generated deploy config and
// the frozen evidence attached to pre-startup live-inference fixtures.
const FIXTURE_TRADE_TAPE_ON_CHAIN_ENABLED: bool = false;
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
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const RECOVERY_POINT_TIMEOUT: Duration = Duration::from_mins(10);
const LEASE_RECOVERY_TIMEOUT: Duration = Duration::from_mins(3);
const BROWSER_CLOSURE_TIMEOUT: Duration = Duration::from_mins(10);
// The debug-profile fresh-stack verifies report correctness and liveness. A
// controlled release-profile full-compute benchmark owns the latency SLO.
const REPORT_COMPLETION_TIMEOUT: Duration = Duration::from_mins(15);
const READINESS_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const SIGNAL_PROPAGATION_TIMEOUT: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const REPORT_BOOK_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const GOVERNED_CANCELLATION_LEASE_SECS: u64 = 3_600;
// The production gate remains one full day. The closure fixture compresses
// wall-clock time while preserving 1,000 real ModelRunner observations; five
// minutes leaves deterministic headroom for slower validation hosts.
const CLOSURE_SHADOW_WINDOW_SECS: u64 = 5 * 60;
const MINIO_ACCESS_KEY: &str = "quantpivot-system-test";
const MINIO_SECRET_KEY: &str = "quantpivot-system-test-object-lock-secret";
const MINIO_BUCKET: &str = "quant-pivot-production-stack";
const MINIO_REGION: &str = "us-east-1";
const MINIO_API_PORT: u16 = 9_000;
const MINIO_RETENTION_DAYS: i32 = 30;
const MINIO_SERVER_IMAGE_TAG: &str = "RELEASE.2025-06-13T11-33-47Z";

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
}

struct BrowserFixtureEvidence {
    closure: Option<FeedbackClosureFixture>,
    governed_cancellation_cycle_id: Option<FeedbackCycleId>,
    sampled_parity_report_id: Option<RecommendationReportId>,
    await_settlement_discovery: bool,
}

#[derive(Serialize)]
struct CandidateReadyClosureManifest<'a> {
    closure: &'a FeedbackClosureOutcome,
    report_universe: &'a FeedbackReportUniverse,
}

#[derive(Serialize)]
struct GovernedClosureManifest<'a> {
    closure: &'a FeedbackClosureOutcome,
    pre_activation_parity: &'a [RuntimeParityEvidence],
    permit: &'a JsonValue,
    activation: &'a JsonValue,
    report_universe: &'a FeedbackReportUniverse,
    report: &'a JsonValue,
    report_parity: &'a RuntimeParityEvidence,
    resolution_plane: &'a FeedbackReportResolutionEvidence,
    successor_feedback: &'a SuccessorFeedbackEvidence,
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
#[derive(Debug, Serialize)]
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
#[derive(Debug, Serialize)]
struct SuccessorFeedbackEvidence {
    parent_cycle_id: FeedbackCycleId,
    decision_window_start: DateTime<Utc>,
    decision_cutoff: DateTime<Utc>,
    truth_cutoff: DateTime<Utc>,
    route_cohorts: Vec<SuccessorRouteFeedbackEvidence>,
}

#[derive(Debug, Serialize)]
struct SuccessorRouteFeedbackEvidence {
    route: BuyModelRoute,
    report_route_run_id: ReportRouteRunId,
    profile_ref: ResearchProfileRef,
    model_version_id: ModelVersionId,
    recommendation_ids: Vec<RecommendationId>,
    resolution_outcome_hashes: Vec<ContentHash>,
    model_learning_eligible_count: u32,
    policy_evaluation_eligible_count: u32,
    execution_learning_excluded_count: u32,
    execution_exclusion_reason: CohortExclusionReason,
}

struct ActivationPolicyPreimage {
    bundle: ActivePolicyBundle,
    crypto_champion_model_version_id: ModelVersionId,
}

struct SuccessorRouteVerifier<'a> {
    db: &'a DatabaseConnection,
    outcome: &'a FeedbackClosureOutcome,
    report_id: RecommendationReportId,
    decision_at: DateTime<Utc>,
    truth_cutoff: DateTime<Utc>,
    outcomes: &'a HashMap<RecommendationId, RecommendationResolutionOutcomeInfo>,
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
        Self::verify_policy(&policy_candidates, &snapshot)?;
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
            model_learning_eligible_count: u32::try_from(model_candidates.len())?,
            policy_evaluation_eligible_count: u32::try_from(policy_candidates.len())?,
            execution_learning_excluded_count: u32::try_from(execution_candidates.len())?,
            execution_exclusion_reason: CohortExclusionReason::ReportOnlyNoExecutionAuthority,
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
        candidates: &[FeedbackCohortCandidate],
        snapshot: &FeedbackCohortSnapshot,
    ) -> Result<()> {
        for candidate in candidates {
            let decision = evaluate_feedback_cohort(
                FeedbackCohort::PolicyEvaluation,
                snapshot,
                candidate.context(),
                candidate.resolution_outcome(),
                candidate.execution_rollup(),
            )?;
            ensure!(
                matches!(
                    decision,
                    FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::PolicyEvaluation {
                        execution_state: None,
                        resolution_outcome_hash: Some(_),
                        execution_rollup_hash: None,
                    })
                ),
                "post-report recommendation {} has invalid PolicyEvaluation evidence: {decision:?}",
                candidate.context().recommendation_id()
            );
        }
        Ok(())
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
            )?;
            ensure!(
                decision
                    == FeedbackCohortDecision::Excluded(
                        CohortExclusionReason::ReportOnlyNoExecutionAuthority,
                    ),
                "ReportOnly recommendation {} fabricated execution feedback: {decision:?}",
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

struct StackReadinessServer {
    listener: TokioTcpListener,
}

struct ProductionArtifactStack {
    config: ArtifactStoreDeployConfig,
    container: ContainerAsync<GenericImage>,
}

struct ProductionStartup {
    artifact_infrastructure: Option<ProductionArtifactStack>,
    infrastructure: Option<SystemStack>,
}

impl ProductionArtifactStack {
    async fn start(run_dir: &Path) -> Result<Self> {
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
                HttpWaitStrategy::new("/minio/health/ready")
                    .with_port(MINIO_API_PORT.into())
                    .with_expected_status_code(200u16),
            ))
            .with_cmd(["server", "/data", "--console-address", ":9001"])
            .with_env_var("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
            .with_env_var("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
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
                prefix: "artifacts".to_owned(),
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
        let mut child = command
            .spawn()
            .with_context(|| format!("spawn production binary {}", self.binary.display()))?;
        if let Err(error) = await_startup(&mut child, &self.base_url).await {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(error.context(format!(
                "production binary did not become ready; logs={}",
                log_path.display()
            )));
        }
        Ok(child)
    }
}

pub struct ProductionStack {
    browser_closure_monitor: Option<JoinHandle<Result<()>>>,
    child: Child,
    closure_cycle_id: Option<FeedbackCycleId>,
    fixture: ProductionStackFixture,
    governed_cancellation_cycle_id: Option<FeedbackCycleId>,
    launch: ProductionLaunch,
    listen_port: u16,
    _upstream: MockServer,
    clob_upstream: DeterministicClobServer,
    artifact_infrastructure: Option<ProductionArtifactStack>,
    infrastructure: SystemStack,
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

impl ProductionStackFixture {
    const fn account_collateral_usd(self) -> Decimal {
        match self {
            Self::Empty | Self::Browser => dec!(100),
            Self::GovernedFeedback => dec!(5000),
            Self::FeedbackClosure | Self::FeedbackClosureRecovery => dec!(555.56),
        }
    }

    async fn deterministic_upstream(self, report_resolves_at: DateTime<Utc>) -> Result<MockServer> {
        let server = MockServer::start().await;
        let polygon_clock = DeterministicPolygonClock::new();
        let collateral = format!("{:.2}", self.account_collateral_usd());
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(move |request: &Request| {
                deterministic_polygon_rpc(request, &polygon_clock)
            })
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
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(self.deterministic_gamma(report_resolves_at)?),
            )
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
                                "orderMinSize": "1",
                                "orderPriceMinTickSize": "0.01",
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
                        "negRisk": false,
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
        !matches!(
            self,
            Self::GovernedFeedback | Self::FeedbackClosure | Self::FeedbackClosureRecovery
        )
    }

    async fn seed_research_fixture(
        self,
        db: &DatabaseConnection,
        store: &Arc<dyn ArtifactStore>,
    ) -> Result<(SharedDemoInfra, BrowserResearchFixture)> {
        match self {
            Self::Browser => {
                let infra = Box::pin(seed_demo_with_store(db, store)).await;
                let research = Box::pin(seed_browser_research(db, store, &infra)).await?;
                Ok((infra, research))
            }
            Self::GovernedFeedback => {
                let governed = Box::pin(seed_feedback_serving_infra(
                    db,
                    store,
                    FeedbackServingFixtureConfig {
                        required_shadow_window_secs: 86_400,
                        shadow_diff_threshold: dec!(0.10),
                        feedback_budget_usd: self.account_collateral_usd(),
                        outcome_reconciliation_enabled: true,
                        ad_hoc_report_enabled: false,
                    },
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
                        required_shadow_window_secs: CLOSURE_SHADOW_WINDOW_SECS,
                        shadow_diff_threshold: dec!(1),
                        feedback_budget_usd: self.account_collateral_usd(),
                        outcome_reconciliation_enabled: true,
                        ad_hoc_report_enabled: true,
                    },
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShutdownOrigin {
    Harness,
    ProcessTreeSignal,
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

pub async fn serve(
    listen_port: u16,
    readiness_port: Option<u16>,
    fixture: ProductionStackFixture,
    retain_artifacts: bool,
) -> Result<()> {
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
    let mut running = Box::pin(ProductionStack::start_at(
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
                let cleanup = Box::pin(running.stop(!retain_artifacts)).await;
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
}

pub async fn verify(runs: u16) -> Result<()> {
    verify_fixture(runs, ProductionStackFixture::Empty).await
}

/// Run the complete governed 15-stage closure, activation, and mixed-Route
/// report against independently bootstrapped production stacks.
pub async fn verify_feedback_closure(runs: u16) -> Result<()> {
    verify_fixture(runs, ProductionStackFixture::FeedbackClosure).await
}

async fn verify_fixture(runs: u16, fixture: ProductionStackFixture) -> Result<()> {
    if runs == 0 {
        bail!("production-stack verify requires --runs greater than zero");
    }
    let workspace = Workspace::build()?;
    for run_number in 1..=runs {
        let listen_port = reserve_port()?;
        let running = Box::pin(ProductionStack::start_at(
            &workspace,
            listen_port,
            fixture,
            ProductionStackPurpose::Verification,
        ))
        .await
        .with_context(|| format!("start production-stack verification run {run_number}"))?;
        Box::pin(running.stop(true))
            .await
            .with_context(|| format!("stop production-stack verification run {run_number}"))?;
        println!("production-stack {fixture:?} verification run {run_number}/{runs} passed");
    }
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
    pub const fn governed_cancellation_cycle_id(&self) -> Option<FeedbackCycleId> {
        self.governed_cancellation_cycle_id
    }

    /// Cycle driven only by production coordinator stages in the closure fixture.
    #[must_use]
    pub const fn closure_cycle_id(&self) -> Option<FeedbackCycleId> {
        self.closure_cycle_id
    }

    /// Gracefully restart only the real binary while preserving every owned
    /// persistence service, the rendered config, port, and artifact directory.
    pub async fn restart(&mut self) -> Result<()> {
        self.shutdown_binary(ShutdownOrigin::Harness).await?;
        self.child = self.launch.spawn().await?;
        Ok(())
    }

    /// Abruptly terminate the real binary without releasing its durable
    /// coordinator lease, then start the same binary against the same stores.
    async fn crash_restart(&mut self) -> Result<()> {
        self.child
            .start_kill()
            .context("kill production binary at lease-recovery fault point")?;
        let status = timeout(SHUTDOWN_TIMEOUT, self.child.wait())
            .await
            .context("time out waiting for crashed production binary")?
            .context("wait for crashed production binary")?;
        ensure!(
            !status.success(),
            "production binary unexpectedly exited successfully at crash fault point"
        );
        self.child = self.launch.spawn().await?;
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

    pub async fn stop(self, remove_artifacts: bool) -> Result<()> {
        self.shutdown(remove_artifacts, ShutdownOrigin::Harness)
            .await
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
        self.shutdown(remove_artifacts, ShutdownOrigin::ProcessTreeSignal)
            .await
    }

    async fn shutdown(mut self, remove_artifacts: bool, origin: ShutdownOrigin) -> Result<()> {
        let monitor_result = self.finish_browser_closure_monitor().await;
        let shutdown_result = self.shutdown_binary(origin).await;
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

    async fn shutdown_binary(&mut self, origin: ShutdownOrigin) -> Result<()> {
        if let Some(status) = self.child.try_wait().context("inspect production binary")? {
            return self.verify_exit(status);
        }

        if origin == ShutdownOrigin::ProcessTreeSignal {
            match self.observe_signal().await? {
                SignalObservation::ChildExited(status) => return self.verify_exit(status),
                SignalObservation::IngressClosed => return self.wait_for_exit().await,
                SignalObservation::Unobserved => {}
            }
        }

        self.signal_binary()?;
        self.wait_for_exit().await
    }

    async fn observe_signal(&mut self) -> Result<SignalObservation> {
        let deadline = Instant::now() + SIGNAL_PROPAGATION_TIMEOUT;
        let address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.listen_port);
        loop {
            if let Some(status) = self
                .child
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

    fn signal_binary(&self) -> Result<()> {
        let process_id = self
            .child
            .id()
            .context("production binary has no process id")?;
        let terminate = Command::new("kill")
            .args(["-TERM", &process_id.to_string()])
            .status()
            .context("send SIGTERM to production binary")?;
        if !terminate.success() {
            bail!(
                "could not signal production binary {process_id}; logs={}",
                self.launch.log_path().display(),
            );
        }
        Ok(())
    }

    async fn wait_for_exit(&mut self) -> Result<()> {
        if let Ok(status) = timeout(SHUTDOWN_TIMEOUT, self.child.wait()).await {
            let status = status.context("wait for graceful production shutdown")?;
            self.verify_exit(status)
        } else {
            self.child
                .start_kill()
                .context("force-stop unresponsive production binary")?;
            let _ = self.child.wait().await;
            bail!(
                "production binary exceeded the {SHUTDOWN_TIMEOUT:?} shutdown budget; logs={}",
                self.launch.log_path().display(),
            );
        }
    }

    fn verify_exit(&self, status: ExitStatus) -> Result<()> {
        if !status.success() {
            bail!(
                "production binary shutdown failed with {status}; logs={}",
                self.launch.log_path().display(),
            );
        }
        Ok(())
    }
}

impl ProductionStack {
    async fn await_termination(&mut self, readiness: Option<StackReadinessServer>) -> Result<()> {
        let readiness = async move {
            match readiness {
                Some(server) => server.serve().await,
                None => pending::<Result<()>>().await,
            }
        };
        tokio::pin!(readiness);
        tokio::select! {
            signal = termination_signal() => signal,
            status = self.child.wait() => {
                let status = status.context("wait for production binary")?;
                bail!(
                    "production binary exited before the fixture was terminated: {status}; logs={}",
                    self.launch.log_path().display(),
                );
            }
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
        let upstream = fixture.deterministic_upstream(report_resolves_at).await?;
        let clob_upstream = DeterministicClobServer::start_keepalive(Duration::from_secs(5))
            .await
            .context("start deterministic production-stack CLOB transport")?;
        let infrastructure = Box::pin(SystemStack::start())
            .await
            .context("start disposable production-stack infrastructure")?;
        let mut startup = ProductionStartup::new(infrastructure);
        let started = async {
            let infrastructure = startup.infrastructure()?;
            PgModelRegistryRepository::new(infrastructure.postgres.connection().clone())
                .ensure_builtin_research_profiles()
                .await
                .context("bootstrap immutable fresh-deployment research profiles")?;
            if fixture.requires_default_policy() {
                ensure_default_policy_bundle(
                    &PgPolicyRepository::new(infrastructure.postgres.connection().clone()),
                    "production-stack-fixture",
                    "canonical fresh-boot policy for the real-binary system fixture",
                )
                .await
                .context("bootstrap canonical fresh-boot policy bundle")?;
            }

            let run_dir = workspace
                .target_directory
                .join("production-stack")
                .join(Uuid::now_v7().to_string());
            fs::create_dir_all(&run_dir)
                .with_context(|| format!("create production-stack run {}", run_dir.display()))?;
            if fixture.seeds_browser() {
                startup.artifact_infrastructure = Some(
                    ProductionArtifactStack::start(&run_dir)
                        .await
                        .with_context(|| {
                            format!(
                                "start production artifact infrastructure; retained artifacts={}",
                                run_dir.display()
                            )
                        })?,
                );
            }
            let artifact_config = startup.artifact_infrastructure.as_ref().map_or_else(
                || ArtifactStoreDeployConfig {
                    prefix: run_dir.join("artifacts").to_string_lossy().into_owned(),
                    ..ArtifactStoreDeployConfig::default()
                },
                |infrastructure| infrastructure.config.clone(),
            );
            let runtime_artifact_store: Arc<dyn ArtifactStore> =
                match startup.artifact_infrastructure.as_ref() {
                    Some(infrastructure) => infrastructure.store()?,
                    None => Arc::new(LocalArtifactStore::new(&artifact_config.prefix)),
                };
            let infrastructure = startup.infrastructure()?;
            let browser_evidence = if fixture.seeds_browser() {
                Some(
                    Box::pin(seed_browser_fixture(
                        infrastructure.postgres.connection(),
                        &infrastructure.clickhouse_config,
                        &runtime_artifact_store,
                        fixture,
                        report_resolves_at,
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
            if let Some(closure) = browser_evidence
                .as_ref()
                .and_then(|evidence| evidence.closure.as_ref())
            {
                mount_closure_catalog(&upstream, closure)
                    .await
                    .context("mount complete closure Gamma condition responses")?;
            }

            render_config(
                &workspace.root,
                &run_dir,
                listen_port,
                &upstream,
                &clob_upstream,
                infrastructure,
                &artifact_config,
            )
            .with_context(|| {
                format!(
                    "render production config; retained artifacts={}",
                    run_dir.display()
                )
            })?;

            let launch = ProductionLaunch {
                workspace_root: workspace.root.clone(),
                binary: workspace.binary.clone(),
                run_dir,
                base_url: format!("http://127.0.0.1:{listen_port}"),
                uses_fixture_s3: startup.artifact_infrastructure.is_some(),
            };
            let child = launch.spawn().await?;
            Ok::<_, AnyhowError>(StartedProduction {
                child,
                launch,
                browser_evidence,
            })
        }
        .await;
        let started = match started {
            Ok(started) => started,
            Err(error) => return startup.abort(error).await,
        };
        let (artifact_infrastructure, infrastructure) = startup.finish()?;
        let governed_cancellation_cycle_id = started
            .browser_evidence
            .as_ref()
            .and_then(|evidence| evidence.governed_cancellation_cycle_id);
        let closure_cycle_id = started.browser_evidence.as_ref().and_then(|evidence| {
            evidence
                .closure
                .as_ref()
                .map(|closure| closure.feedback_cycle_id)
        });
        let running = Self {
            browser_closure_monitor: None,
            child: started.child,
            closure_cycle_id,
            fixture,
            governed_cancellation_cycle_id,
            launch: started.launch,
            listen_port,
            _upstream: upstream,
            clob_upstream,
            artifact_infrastructure,
            infrastructure,
        };
        Box::pin(running.into_ready_or_shutdown(started.browser_evidence.as_ref(), purpose)).await
    }

    async fn into_ready_or_shutdown(
        mut self,
        evidence: Option<&BrowserFixtureEvidence>,
        purpose: ProductionStackPurpose,
    ) -> Result<Self> {
        let readiness = Box::pin(self.await_readiness(evidence, purpose)).await;
        if let Err(error) = readiness {
            let cleanup = self
                .shutdown(false, ShutdownOrigin::Harness)
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
        let Some(evidence) = evidence else {
            return Ok(());
        };
        let mut browser_closure = None;
        if let Some(closure) = evidence.closure.as_ref() {
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
            let pre_activation_parity =
                await_existing_parity(self.infrastructure.postgres.connection()).await?;
            match purpose {
                ProductionStackPurpose::BrowserEvidence => {
                    let report_universe = prepare_feedback_report_universe(
                        self.infrastructure.postgres.connection(),
                        closure,
                    )
                    .await?;
                    self.persist_candidate_manifest(&outcome, &report_universe)?;
                    if self.fixture == ProductionStackFixture::FeedbackClosure {
                        browser_closure = Some((closure.clone(), outcome, report_universe));
                    }
                }
                ProductionStackPurpose::Verification => {
                    let (permit, activation) = self.activate_feedback_candidate(&outcome).await?;
                    self.clob_upstream.refresh_handle().pause_keepalive();
                    let report_universe = prepare_feedback_report_universe(
                        self.infrastructure.postgres.connection(),
                        closure,
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
                    self.persist_closure_manifest(&GovernedClosureManifest {
                        closure: &outcome,
                        pre_activation_parity: &pre_activation_parity,
                        permit: &permit,
                        activation: &activation,
                        report_universe: &report_universe,
                        report: &report,
                        report_parity: &report_parity,
                        resolution_plane: &resolution_plane,
                        successor_feedback: &successor_feedback,
                    })?;
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
        if let Some((fixture, outcome, report_universe)) = browser_closure {
            self.start_browser_closure_monitor(fixture, outcome, report_universe)?;
        }
        self.verify_clob_connection_bound().await?;
        Ok(())
    }

    async fn verify_clob_connection_bound(&self) -> Result<()> {
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
        let turnover_bound = expected.saturating_mul(2);
        ensure!(
            active == expected && high_water <= turnover_bound,
            "CLOB connection ownership escaped its shard bound: active={active}, expected={expected}, high_water={high_water}, turnover_bound={turnover_bound}",
        );
        println!(
            "production-stack CLOB connection bound passed: active={active} high_water={high_water} shard_bound={expected} turnover_bound={turnover_bound}"
        );
        Ok(())
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
            let status = decode_http_json(
                http.get(format!("{}/api/system/status", self.base_url()))
                    .header("accept-api-version", "v1")
                    .bearer_auth(&access_token)
                    .send()
                    .await
                    .context("read production-stack operational status")?,
                StatusCode::OK,
                "production-stack operational status",
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
            if operational && market_data_ready && connected_shards && subscribed_markets {
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

    async fn governed_http_session(&self) -> Result<(Client, String)> {
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("build governed closure HTTP client")?;
        let login = decode_http_json(
            http.post(format!("{}/api/auth/login", self.base_url()))
                .header("accept-api-version", "v1")
                .json(&json!({
                    "username": "admin",
                    "password": BOOTSTRAP_ADMIN_PASSWORD,
                }))
                .send()
                .await
                .context("login for governed closure")?,
            StatusCode::OK,
            "governed closure login",
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
        let first = &responses[0]["data"];
        let replay = &responses[1]["data"];
        ensure!(
            first["cycle"]["feedback_cycle_id"] == closure.feedback_cycle_id.to_string()
                && first["cycle_reused"] == true
                && first["trigger_replayed"] == false
                && replay["cycle"]["feedback_cycle_id"] == closure.feedback_cycle_id.to_string()
                && replay["cycle_reused"] == true
                && replay["trigger_replayed"] == true,
            "governed Trigger did not converge and replay against the frozen production cycle: first={} replay={}",
            responses[0],
            responses[1]
        );
        Ok(())
    }

    async fn activate_feedback_candidate(
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
        let activation = decode_http_json(
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
                "reason_code": "production_e2e_feedback_activation",
                "note": "Consume the verified permit and atomically promote the Weather candidate with its exact mixed-Route scenario-model bindings.",
            }))
            .send()
            .await
            .context("activate governed closure candidate")?,
            StatusCode::CREATED,
            "activate governed closure candidate",
        )
        .await?;
        let receipt = &activation["data"]["receipt"];
        ensure!(
            receipt["feedback_cycle_id"] == outcome.feedback_cycle_id.to_string()
                && receipt["route"] == "weather"
                && receipt["previous_model_version_id"]
                    == outcome.champion_model_version_id.to_string()
                && receipt["activated_model_version_id"]
                    == outcome.candidate_model_version_id.to_string()
                && receipt["execution_authority_unchanged"] == true
                && activation["data"]["replayed"] == false,
            "governed activation receipt diverged from the verified closure outcome: {activation}"
        );
        self.verify_activation_commit(outcome, &preimage).await?;
        Ok((permit, activation))
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
            crypto_after.champion.model_version_id == preimage.crypto_champion_model_version_id
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
                    "knowledge_lag_secs": 0,
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
        let report_id = terminal_run["data"]["output_report_id"]
            .as_str()
            .context("successful mixed-Route run omitted output_report_id")?;
        let detail = self
            .wait_report_publication(&http, &access_token, report_id, deadline)
            .await?;
        let recommendations = decode_http_json(
            http.get(format!(
                "{}/api/quant/reports/{report_id}/recommendations",
                self.base_url()
            ))
            .header("accept-api-version", "v1")
            .bearer_auth(&access_token)
            .send()
            .await
            .context("read mixed-Route recommendations")?,
            StatusCode::OK,
            "read mixed-Route recommendations",
        )
        .await?;
        let diagnostics = decode_http_json(
            http.get(format!(
                "{}/api/quant/reports/{report_id}/diagnostics",
                self.base_url()
            ))
            .header("accept-api-version", "v1")
            .bearer_auth(&access_token)
            .send()
            .await
            .context("read mixed-Route report diagnostics")?,
            StatusCode::OK,
            "read mixed-Route report diagnostics",
        )
        .await?;
        let funnel = decode_http_json(
            http.get(format!(
                "{}/api/quant/reports/{report_id}/funnel",
                self.base_url()
            ))
            .header("accept-api-version", "v1")
            .bearer_auth(&access_token)
            .send()
            .await
            .context("read mixed-Route report funnel")?,
            StatusCode::OK,
            "read mixed-Route report funnel",
        )
        .await?;
        let funnel_markets = decode_http_json(
            http.get(format!(
                "{}/api/quant/reports/{report_id}/funnel/markets",
                self.base_url()
            ))
            .header("accept-api-version", "v1")
            .bearer_auth(&access_token)
            .send()
            .await
            .context("read mixed-Route market funnel")?,
            StatusCode::OK,
            "read mixed-Route market funnel",
        )
        .await?;
        ensure!(
            detail["data"]["represented_routes"]["routes"] == json!(["crypto", "weather"])
                && detail["data"]["scenario_artifact_id"].is_string()
                && detail["data"]["scenario_artifact_hash"].is_string(),
            "published report lost its mixed-Route/scenario identity: {detail}"
        );
        validate_mixed_recommendations(&recommendations, universe, &diagnostics, &funnel_markets)?;
        let diagnostic_routes = diagnostics["data"]["routes"]
            .as_array()
            .context("mixed-Route diagnostics omitted Route runs")?;
        ensure!(
            diagnostic_routes.len() == 2
                && diagnostic_routes
                    .iter()
                    .any(|route| { route["route"] == "crypto" && route["outcome"] == "ready" })
                && diagnostic_routes
                    .iter()
                    .any(|route| { route["route"] == "weather" && route["outcome"] == "ready" })
                && funnel["data"]["conserved"] == true,
            "mixed-Route report diagnostics/funnel are incomplete: diagnostics={diagnostics} funnel={funnel}"
        );
        self.verify_portfolio_plan(report_id, outcome, &recommendations)
            .await?;
        Ok(json!({
            "run": terminal_run,
            "detail": detail,
            "recommendations": recommendations,
            "diagnostics": diagnostics,
            "funnel": funnel,
            "funnel_markets": funnel_markets,
        }))
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
                binding.ordered_routes == vec![BuyModelRoute::Crypto, BuyModelRoute::Weather]
            })
            .context("CandidateReady manifest omitted the mixed-Route scenario binding")?;
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
        ensure!(
            resolution_plane.report_id == report_id
                && resolution_plane.report_decision_at == report_row.decision_at
                && resolution_plane.resolved_at > report_row.decision_at
                && resolution_plane.observed_at >= resolution_plane.resolved_at,
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

        let outcome_repository = PgRecommendationResolutionOutcomeRepository::new(db.clone());
        let deadline = Instant::now() + Duration::from_secs(30);
        let outcomes = loop {
            let mut observed = HashMap::new();
            for recommendation in &recommendations {
                if let Some(resolution) = outcome_repository
                    .find_by_recommendation(&recommendation.recommendation_id)
                    .await?
                {
                    observed.insert(recommendation.recommendation_id, resolution);
                }
            }
            if observed.len() == recommendations.len() {
                break observed;
            }
            ensure!(
                Instant::now() < deadline,
                "production outcome reconciliation did not project all post-report resolutions: projected={} expected={}",
                observed.len(),
                recommendations.len()
            );
            sleep(POLL_INTERVAL).await;
        };
        let strictly_forward = outcomes.values().all(|resolution| {
            let observed_after_decision = resolution.source_observed_at > report_row.decision_at;
            let available_after_source = resolution.available_at >= resolution.source_observed_at;
            observed_after_decision && available_after_source
        });
        ensure!(
            strictly_forward,
            "successor feedback outcomes are not strictly forward-looking"
        );

        let truth_cutoff = db.statement_time().await;
        ensure!(
            outcomes
                .values()
                .all(|resolution| resolution.available_at <= truth_cutoff),
            "successor feedback truth cutoff precedes a reconciled outcome"
        );
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
        };
        let mut route_cohorts = Vec::with_capacity(recommendations_by_run.len());
        for (route_run_id, recommendation_ids) in recommendations_by_run {
            route_cohorts.push(verifier.verify(route_run_id, recommendation_ids).await?);
        }
        route_cohorts.sort_by_key(|route| route.route.as_str());
        ensure!(
            route_cohorts.len() == 2
                && route_cohorts.iter().all(|route| {
                    let expected = route.recommendation_ids.len();
                    usize::try_from(route.model_learning_eligible_count).ok() == Some(expected)
                        && usize::try_from(route.policy_evaluation_eligible_count).ok()
                            == Some(expected)
                        && usize::try_from(route.execution_learning_excluded_count).ok()
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

    fn persist_closure_manifest(&self, manifest: &GovernedClosureManifest<'_>) -> Result<()> {
        let path = self.run_dir().join("feedback-closure-manifest.json");
        let payload = serde_json::to_vec_pretty(manifest)?;
        Self::persist_json_manifest(&path, &payload, "governed closure")
    }

    fn persist_candidate_manifest(
        &self,
        outcome: &FeedbackClosureOutcome,
        report_universe: &FeedbackReportUniverse,
    ) -> Result<()> {
        let path = self
            .run_dir()
            .join("feedback-candidate-ready-manifest.json");
        let payload = serde_json::to_vec_pretty(&CandidateReadyClosureManifest {
            closure: outcome,
            report_universe,
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
        clob_refresh.pause_keepalive();
        let snapshots = fixture.report_book_snapshots()?;
        let sent_after = Utc::now();
        for (index, snapshot) in snapshots.iter().enumerate() {
            clob_refresh
                .send_snapshot(
                    &snapshot.token_id,
                    &snapshot.bids,
                    &snapshot.asks,
                    u64::try_from(index)?,
                )
                .await?;
            sleep(POLL_INTERVAL).await;
        }
        let refreshed_at = fixture
            .await_report_book_snapshots(&snapshots, sent_after)
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
        outcome: &FeedbackClosureOutcome,
        universe: &FeedbackReportUniverse,
    ) -> Result<RecommendationReportId> {
        let deadline = Instant::now() + BROWSER_CLOSURE_TIMEOUT;
        let expected_markets = universe.market_ids.iter().cloned().collect::<HashSet<_>>();
        let expected_routes = vec![BuyModelRoute::Crypto, BuyModelRoute::Weather];
        ensure!(
            !snapshots.is_empty(),
            "browser report readiness has no exact book snapshot"
        );
        let mut refresh_index = 0_usize;
        loop {
            let snapshot = snapshots
                .get(refresh_index % snapshots.len())
                .context("browser report refresh index escaped its snapshot set")?;
            clob_refresh
                .send_snapshot(
                    &snapshot.token_id,
                    &snapshot.bids,
                    &snapshot.asks,
                    u64::try_from(refresh_index)?,
                )
                .await?;
            refresh_index = refresh_index.saturating_add(1);
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
                if recommendations.len() >= expected_routes.len()
                    && routes == expected_routes.iter().copied().collect::<HashSet<_>>()
                    && recommendations
                        .iter()
                        .all(|recommendation| expected_markets.contains(&recommendation.market_id))
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
    funnel_markets: &JsonValue,
) -> Result<()> {
    let recommendations = response["data"]
        .as_array()
        .context("mixed-Route recommendation response is not an array")?;
    ensure!(
        recommendations.len() >= 2,
        "mixed-Route report did not publish at least one recommendation per Route: {response}"
    );
    let mut route_counts = BTreeMap::new();
    let mut selected_markets = Vec::with_capacity(recommendations.len());
    for (index, recommendation) in recommendations.iter().enumerate() {
        let expected_rank = i64::try_from(index + 1)?;
        let route = recommendation["route"]
            .as_str()
            .context("global recommendation omitted Route")?;
        let market_id = recommendation["market_id"]
            .as_str()
            .context("global recommendation omitted market_id")?;
        ensure!(
            recommendation["rank"].as_i64() == Some(expected_rank)
                && universe
                    .market_ids
                    .iter()
                    .any(|candidate| candidate.as_str() == market_id)
                && recommendation["economic_tier"]["route"] == route,
            "global recommendation rank/Route/market lineage is inconsistent: {recommendation}"
        );
        match route {
            "crypto" | "weather" => {
                *route_counts.entry(route).or_insert(0_usize) += 1;
                selected_markets.push(market_id.to_owned());
            }
            other => bail!("mixed-Route report published unexpected Route {other}"),
        }
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
            "selected recommendation has no positive robust/marginal portfolio value: {recommendation}"
        );
    }
    ensure!(
        route_counts.contains_key("crypto") && route_counts.contains_key("weather"),
        "global recommendations do not span both represented Routes: route_counts={route_counts:?} selected_markets={selected_markets:?} route_funnels={} market_terminals={}",
        compact_route_funnels(diagnostics),
        compact_market_terminals(funnel_markets)
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

async fn seed_browser_fixture(
    db: &DatabaseConnection,
    clickhouse_config: &ClickHouseConfig,
    runtime_artifact_store: &Arc<dyn ArtifactStore>,
    fixture: ProductionStackFixture,
    report_resolves_at: DateTime<Utc>,
) -> Result<BrowserFixtureEvidence> {
    let (mut infra, research) =
        Box::pin(fixture.seed_research_fixture(db, runtime_artifact_store)).await?;
    if matches!(
        fixture,
        ProductionStackFixture::GovernedFeedback
            | ProductionStackFixture::FeedbackClosure
            | ProductionStackFixture::FeedbackClosureRecovery
    ) {
        infra = Box::pin(finalize_feedback_portfolio(
            db,
            runtime_artifact_store,
            infra,
            research.model_version_id,
            research.evaluation_dataset_id,
        ))
        .await?;
    }
    println!(
        "browser research fixture: model_version_id={} evaluation_dataset_id={} backtest_report_id={} feedback_cycle_id={} governed_cancellation_cycle_id={:?}",
        research.model_version_id,
        research.evaluation_dataset_id,
        research.backtest_report_id,
        research.feedback_cycle_id,
        research.governed_cancellation_cycle_id,
    );
    ensure!(
        !FIXTURE_TRADE_TAPE_ON_CHAIN_ENABLED,
        "pre-startup closure serving evidence requires the deterministic trade-tape source to remain disabled"
    );
    let runtime_trade_tape_source =
        TradeTapeSourceEvidence::runtime(FIXTURE_TRADE_TAPE_ON_CHAIN_ENABLED, Vec::new())
            .map_err(AnyhowError::msg)?;
    let closure = Box::pin(seed_optional_closure(OptionalClosureSeed {
        db,
        clickhouse_config,
        runtime_artifact_store,
        infra: &infra,
        model_version_id: research.model_version_id,
        historical_feedback_cycle_id: research.feedback_cycle_id,
        fixture,
        report_resolves_at,
        runtime_trade_tape_source,
    }))
    .await?;
    if closure.is_some() {
        pause_feedback_schedulers(db).await?;
    }
    let governed_cancellation_cycle_id =
        if let Some(cycle_id) = research.governed_cancellation_cycle_id {
            ensure!(
                fixture == ProductionStackFixture::GovernedFeedback,
                "only a governed feedback fixture may seed a cancellation cycle"
            );
            // Model a cycle already owned by another healthy worker. Its live lease
            // makes API cancellation deterministic without weakening the terminal
            // state machine or depending on a request-vs-worker race.
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
            Some(cycle_id)
        } else {
            ensure!(
                fixture != ProductionStackFixture::GovernedFeedback,
                "governed feedback fixture is missing its queued cancellation cycle"
            );
            None
        };
    if matches!(
        fixture,
        ProductionStackFixture::FeedbackClosure | ProductionStackFixture::FeedbackClosureRecovery
    ) {
        verify_browser_artifacts(db, runtime_artifact_store).await?;
        return Ok(BrowserFixtureEvidence {
            closure,
            governed_cancellation_cycle_id,
            sampled_parity_report_id: None,
            await_settlement_discovery: false,
        });
    }
    enable_test_admission(db, "browser-e2e-fixture").await;
    let settlement_report = Box::pin(seed_production_report(
        db,
        clickhouse_config,
        runtime_artifact_store,
        &infra,
        ReportSeedConfig {
            event_id: "browser-settlement-event".to_owned(),
            market_id: "0xbrowser-settlement-market".to_owned(),
            market_question: "Will the browser settlement fixture resolve?".to_owned(),
            market_slug: "browser-settlement-fixture".to_owned(),
            token_id: "12345".to_owned(),
            trigger_key: format!(
                "scheduled:browser-settlement:{}",
                RecommendationReportId::from_v7()
            ),
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
    let report = Box::pin(seed_production_report(
        db,
        clickhouse_config,
        runtime_artifact_store,
        &parity_infra,
        ReportSeedConfig {
            event_id: "evt-1".to_owned(),
            market_id: "0xmarket".to_owned(),
            market_question: "Will it?".to_owned(),
            market_slug: "will-it".to_owned(),
            token_id: "token-1".to_owned(),
            trigger_key: format!("scheduled:test:{}", RecommendationReportId::from_v7()),
        },
    ))
    .await?;
    seed_pending_intent(db, &report).await;
    verify_browser_artifacts(db, runtime_artifact_store).await?;
    Ok(BrowserFixtureEvidence {
        closure,
        governed_cancellation_cycle_id,
        sampled_parity_report_id: Some(report.report),
        await_settlement_discovery: true,
    })
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
    runtime_trade_tape_source: TradeTapeSourceEvidence,
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
        runtime_trade_tape_source,
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
        runtime_trade_tape_source,
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

async fn await_existing_parity(db: &DatabaseConnection) -> Result<Vec<RuntimeParityEvidence>> {
    let runs = FeatureParityRunEntity::find()
        .filter(FeatureParityRunColumn::TrainingDatasetId.is_null())
        .filter(FeatureParityRunColumn::Status.is_in([
            FeatureParityRunStatus::Queued,
            FeatureParityRunStatus::Running,
            FeatureParityRunStatus::PendingMaterialization,
        ]))
        .order_by_asc(FeatureParityRunColumn::CreatedAt)
        .order_by_asc(FeatureParityRunColumn::RunId)
        .all(db)
        .await
        .context("read pre-activation runtime parity barrier")?;
    let mut evidence = Vec::with_capacity(runs.len());
    for run in runs {
        evidence.push(await_runtime_parity(db, run.run_id, run.report_id).await?);
    }
    Ok(evidence)
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
        let status = Command::new(cargo)
            .args(["build", "-p", "quant-pivot-bin", "-p", "quant-pivot-xtask"])
            .current_dir(&metadata.workspace_root)
            .status()
            .context("build real quant-pivot production binary with the launcher feature set")?;
        if !status.success() {
            bail!("production binary and launcher build failed with {status}");
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
            900_001,
            900_002,
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
        yes_base + ordinal,
        no_base + ordinal,
    )
}

async fn mount_closure_catalog(
    upstream: &MockServer,
    closure: &FeedbackClosureFixture,
) -> Result<()> {
    let responses = Arc::new(closure.gamma_market_responses()?);
    let response_count = responses.len();
    Mock::given(method("GET"))
        .and(path("/markets"))
        .respond_with(move |request: &Request| closure_market_response(request, responses.as_ref()))
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
) -> ResponseTemplate {
    let Some(condition_id) = requested_condition(request) else {
        return ResponseTemplate::new(400);
    };
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
    yes_token_id: usize,
    no_token_id: usize,
) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(serde_json::json!([{
        "conditionId": condition_id,
        "question": format!("Will deterministic market {condition_id} resolve Yes?"),
        "active": true,
        "closed": false,
        "feesEnabled": true,
        "clobTokenIds": [yes_token_id.to_string(), no_token_id.to_string()],
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
    clob_market_info_response(&synthetic_condition_id(), 900_001, 900_002)
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
    clob_market_info_response(&condition_id, yes_base + ordinal, no_base + ordinal)
}

fn clob_market_info_response(
    condition_id: &str,
    yes_token_id: usize,
    no_token_id: usize,
) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(serde_json::json!({
        "c": condition_id,
        "t": [
            { "t": yes_token_id.to_string(), "o": "Yes" },
            { "t": no_token_id.to_string(), "o": "No" }
        ],
        "mts": "0.01",
        "mos": "1",
        "nr": false,
        "itode": false,
        "ibce": false,
        "oas": 0,
        "fd": { "r": "0", "e": 1, "to": true },
        "mbf": 0,
        "tbf": 0,
        "rfqe": false
    }))
}

struct DeterministicPolygonClock {
    anchor_block: u64,
    anchor_timestamp: i64,
    started_at: StdInstant,
}

#[derive(Clone, Copy)]
struct DeterministicPolygonHead {
    block_number: u64,
    timestamp: i64,
}

impl DeterministicPolygonClock {
    fn new() -> Self {
        Self::at(Utc::now().timestamp(), StdInstant::now())
    }

    const fn at(timestamp: i64, started_at: StdInstant) -> Self {
        let anchor_timestamp = timestamp.div_euclid(DETERMINISTIC_POLYGON_BLOCK_SECS)
            * DETERMINISTIC_POLYGON_BLOCK_SECS;
        Self {
            anchor_block: DETERMINISTIC_POLYGON_HEAD_BLOCK,
            anchor_timestamp,
            started_at,
        }
    }

    fn head(&self) -> DeterministicPolygonHead {
        self.head_after(self.started_at.elapsed())
    }

    fn head_after(&self, elapsed: Duration) -> DeterministicPolygonHead {
        let block_seconds = u64::try_from(DETERMINISTIC_POLYGON_BLOCK_SECS).unwrap_or(1);
        let elapsed_blocks = elapsed
            .as_secs()
            .checked_div(block_seconds)
            .unwrap_or_default();
        let elapsed_seconds = i64::try_from(elapsed_blocks)
            .unwrap_or(i64::MAX)
            .saturating_mul(DETERMINISTIC_POLYGON_BLOCK_SECS);
        DeterministicPolygonHead {
            block_number: self.anchor_block.saturating_add(elapsed_blocks),
            timestamp: self.anchor_timestamp.saturating_add(elapsed_seconds),
        }
    }
}

fn deterministic_polygon_rpc(
    request: &Request,
    clock: &DeterministicPolygonClock,
) -> ResponseTemplate {
    let request = serde_json::from_slice::<JsonRpcRequest>(&request.body);
    let Ok(request) = request else {
        return ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": { "code": -32700, "message": "invalid JSON-RPC request" },
        }));
    };
    let head = clock.head();
    let result = match request.method.as_str() {
        "eth_chainId" => Ok(serde_json::json!("0x89")),
        "eth_blockNumber" => Ok(serde_json::json!(format!("0x{:x}", head.block_number))),
        "eth_getBlockByNumber" => Ok(deterministic_polygon_block(&request.params, head)),
        "eth_getLogs" => Ok(serde_json::json!([])),
        "eth_getCode" => deterministic_polygon_code(&request.params),
        "eth_getStorageAt" => Ok(deterministic_polygon_storage(&request.params)),
        "eth_call" => deterministic_polygon_call(&request.params),
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

fn deterministic_polygon_storage(params: &JsonValue) -> JsonValue {
    let slot = params
        .as_array()
        .and_then(|params| params.get(1))
        .and_then(JsonValue::as_str);
    if slot.is_some_and(|slot| slot.eq_ignore_ascii_case(ERC1967_IMPLEMENTATION_SLOT)) {
        serde_json::json!(COLLATERAL_IMPLEMENTATION_WORD)
    } else {
        serde_json::json!(format!("0x{}", "0".repeat(64)))
    }
}

fn deterministic_polygon_block(params: &JsonValue, head: DeterministicPolygonHead) -> JsonValue {
    let requested = params
        .as_array()
        .and_then(|params| params.first())
        .and_then(JsonValue::as_str)
        .unwrap_or("finalized");
    let block_number = if matches!(requested, "finalized" | "latest" | "safe") {
        head.block_number
    } else {
        requested
            .strip_prefix("0x")
            .and_then(|value| u64::from_str_radix(value, 16).ok())
            .unwrap_or(head.block_number)
    };
    if block_number > head.block_number {
        return JsonValue::Null;
    }
    let block_age_secs = head
        .block_number
        .saturating_sub(block_number)
        .saturating_mul(u64::try_from(DETERMINISTIC_POLYGON_BLOCK_SECS).unwrap_or(2));
    let timestamp = head
        .timestamp
        .saturating_sub(i64::try_from(block_age_secs).unwrap_or(i64::MAX));
    let parent_number = block_number.saturating_sub(1);
    serde_json::json!({
        "number": format!("0x{block_number:x}"),
        "hash": polygon_block_hash(block_number),
        "parentHash": polygon_block_hash(parent_number),
        "sha3Uncles": format!("0x{}", "1d".repeat(32)),
        "miner": format!("0x{}", "22".repeat(20)),
        "stateRoot": format!("0x{}", "33".repeat(32)),
        "transactionsRoot": format!("0x{}", "44".repeat(32)),
        "receiptsRoot": format!("0x{}", "55".repeat(32)),
        "logsBloom": format!("0x{}", "00".repeat(256)),
        "difficulty": "0x0",
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x0",
        "timestamp": format!("0x{timestamp:x}"),
        "extraData": "0x",
        "mixHash": format!("0x{}", "66".repeat(32)),
        "nonce": "0x0000000000000000",
        "baseFeePerGas": "0x0",
        "totalDifficulty": "0x0",
        "size": "0x200",
        "transactions": [],
        "uncles": [],
    })
}

fn polygon_block_hash(block_number: u64) -> String {
    format!("0x{}", blake3::hash(&block_number.to_be_bytes()).to_hex())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant as StdInstant};

    use super::{
        CLOSURE_REPORT_HORIZON_HOURS, Client, DETERMINISTIC_POLYGON_BLOCK_SECS,
        DETERMINISTIC_POLYGON_HEAD_BLOCK, DeterministicPolygonClock, ProductionStackFixture,
        Result, StatusCode, closure_market_text, deterministic_polygon_block,
    };
    use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
    use serde_json::{Value as JsonValue, json};

    #[test]
    fn head_advances_in_slots() {
        let clock = DeterministicPolygonClock::at(1_700_000_001, StdInstant::now());
        let anchor = clock.head_after(Duration::ZERO);
        let later = clock.head_after(Duration::from_secs(121));

        assert_eq!(anchor.block_number, DETERMINISTIC_POLYGON_HEAD_BLOCK);
        assert_eq!(anchor.timestamp, 1_700_000_000);
        assert_eq!(later.block_number, DETERMINISTIC_POLYGON_HEAD_BLOCK + 60);
        assert_eq!(later.timestamp, 1_700_000_120);
    }

    #[test]
    fn block_history_is_immutable() {
        let clock = DeterministicPolygonClock::at(1_700_000_001, StdInstant::now());
        let later = clock.head_after(Duration::from_secs(121));
        let original_number = format!("0x{DETERMINISTIC_POLYGON_HEAD_BLOCK:x}");
        let original = deterministic_polygon_block(&json!([original_number]), later);
        let future =
            deterministic_polygon_block(&json!([format!("0x{:x}", later.block_number + 1)]), later);

        assert_eq!(
            original.get("timestamp").and_then(JsonValue::as_str),
            Some("0x6553f100")
        );
        assert_eq!(future, JsonValue::Null);
        assert_eq!(DETERMINISTIC_POLYGON_BLOCK_SECS, 2);
    }

    #[tokio::test]
    async fn upstream_serves_clob_v2() -> Result<()> {
        let report_resolves_at = Utc::now() + ChronoDuration::hours(48);
        let upstream = ProductionStackFixture::Empty
            .deterministic_upstream(report_resolves_at)
            .await?;
        let response = Client::new()
            .get(format!("{}/version", upstream.uri()))
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.json::<JsonValue>().await?, json!({ "version": 2 }));
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

fn render_config(
    workspace_root: &Path,
    run_dir: &Path,
    listen_port: u16,
    upstream: &MockServer,
    clob_upstream: &DeterministicClobServer,
    stack: &SystemStack,
    artifact_store: &ArtifactStoreDeployConfig,
) -> Result<()> {
    let source_path = workspace_root.join("config/quant-pivot.toml");
    let source = fs::read_to_string(&source_path)
        .with_context(|| format!("read canonical deploy config {}", source_path.display()))?;
    let mut config: Value = toml::from_str(&source)
        .with_context(|| format!("parse canonical deploy config {}", source_path.display()))?;
    configure_upstreams(&mut config, upstream, clob_upstream)?;
    configure_test_identity(&mut config, artifact_store)?;
    configure_infrastructure(&mut config, stack)?;
    configure_web(&mut config, listen_port)?;

    let rendered = toml::to_string_pretty(&config).context("serialize production-stack config")?;
    let config_path = run_dir.join("quant-pivot.toml");
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

fn configure_upstreams(
    config: &mut Value,
    upstream: &MockServer,
    clob_upstream: &DeterministicClobServer,
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
        upstream_url,
    )?;
    set(
        config,
        &["market_data", "trade_tape_on_chain", "enabled"],
        FIXTURE_TRADE_TAPE_ON_CHAIN_ENABLED,
    )?;
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
        if let Ok(response) = client.get(&startup_url).send().await
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
