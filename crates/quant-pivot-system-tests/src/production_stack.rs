//! Real-binary system fixture backed by disposable infrastructure.

use std::{
    env, fs,
    fs::OpenOptions,
    future::Future,
    net::{Ipv4Addr, SocketAddrV4, TcpListener},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Error as AnyhowError, Result, bail, ensure};
use chrono::Utc;
use clap::ValueEnum;
use quant_pivot_models::{
    config::{ArtifactStoreDeployConfig, ArtifactStoreKind, ClickHouseConfig, DeployConfig},
    domain::{api::ModelVersionListQuery, pagination::PageRequest},
    entities::{
        market::Entity as MarketEntity,
        quant_feature_parity_run::{Column, Entity},
        quant_settlement_redeem::Entity as QuantSettlementRedeemEntity,
    },
    enums::{
        market::MarketStatus,
        quant::{
            FeatureParityLatchState, FeatureParityRunKind, FeatureParityRunStatus, FeedbackStage,
            FeedbackStageEventKind,
        },
        settlement::SettlementEffectivePolicy,
    },
    types::{
        ContentHash, FeedbackCycleId, MarketId, ModelVersionId, RecommendationReportId,
        ResearchJobId, WorkerId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgExecutionSubmissionRepository, PgFeatureParityRepository, PgFeedbackCycleRepository,
        PgModelRegistryRepository, PgPolicyRepository,
        policy_bootstrap::ensure_default_policy_bundle,
    },
    traits::{FeatureParityRepository, FeedbackCycleRepository, ModelRegistryRepository},
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore, S3ArtifactStore, S3StaticCredentials},
    model::ModelArtifact,
};
use reqwest::Client;
use rust_decimal_macros::dec;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{Mount, WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};
use tokio::{
    net::TcpStream,
    process::{Child, Command as TokioCommand},
    signal::{unix, unix::SignalKind},
    time::{Instant, sleep, timeout},
};
use toml::{Table, Value};
use uuid::Uuid;
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{method, path, path_regex},
};

use crate::{
    stack::SystemStack,
    support::execution_pg_seed::{
        ReportSeedConfig, SharedDemoInfra, enable_test_admission, fill_entry_lot,
        seed_approved_intent, seed_demo_with_store, seed_feedback_serving_infra,
        seed_pending_intent, seed_production_report,
    },
    support::feedback_closure_seed::{
        FeedbackClosureFixture, complete_feedback_closure, seed_feedback_closure,
    },
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
const COLLATERAL_IMPLEMENTATION_WORD: &str =
    "0x0000000000000000000000006bbcef9f7ef3b6c592c99e0f206a0de94ad0925f";
const STARTUP_TIMEOUT: Duration = Duration::from_mins(1);
// A parity attempt waiting on append-only evidence must release its worker slot
// promptly so another queued parity run can establish its own pending clock.
const SAMPLED_PARITY_START_TIMEOUT: Duration = Duration::from_mins(1);
// The production parity executor deliberately enforces a ten-minute minimum
// materialization grace. Start this budget only after the target run has
// durably entered pending_materialization.
const SAMPLED_PARITY_CONTAINMENT_TIMEOUT: Duration = Duration::from_mins(12);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const RECOVERY_POINT_TIMEOUT: Duration = Duration::from_mins(10);
const LEASE_RECOVERY_TIMEOUT: Duration = Duration::from_mins(3);
const SIGNAL_PROPAGATION_TIMEOUT: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
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
const MINIO_SERVER_IMAGE_TAG: &str = "RELEASE.2025-06-13T11-33-47Z";
const MINIO_CLIENT_IMAGE_TAG: &str = "RELEASE.2025-07-16T15-35-03Z";
const MINIO_BOOTSTRAP_READY: &str = "quant-pivot-minio-bootstrap-ready";

const DISABLED_DOMAIN_SOURCES: &[&str] = &[
    "binance",
    "binance_usdm_futures",
    "polymarket_rtds",
    "chainlink_data_streams",
    "aviation_weather",
    "ghcnh",
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

struct StartedProduction {
    child: Child,
    launch: ProductionLaunch,
    browser_evidence: Option<BrowserFixtureEvidence>,
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
        let network = format!("quant-pivot-artifacts-{identity}");
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
            .with_network(network.clone())
            .with_container_name(server_name.clone())
            .with_mount(Mount::bind_mount(data_root, "/data"))
            .with_startup_timeout(Duration::from_mins(2))
            .start()
            .await
            .context("start production-stack MinIO")?;
        let host_port = container
            .get_host_port_ipv4(MINIO_API_PORT)
            .await
            .context("resolve production-stack MinIO port")?;

        let bootstrap = r#"
/usr/bin/mc alias set system "http://${QP_MINIO_SERVER}:9000" "${QP_MINIO_ACCESS_KEY}" "${QP_MINIO_SECRET_KEY}" &&
/usr/bin/mc mb --with-lock "system/${QP_MINIO_BUCKET}" &&
/usr/bin/mc retention set --default GOVERNANCE "30d" "system/${QP_MINIO_BUCKET}" &&
echo "quant-pivot-minio-bootstrap-ready" &&
while :; do sleep 3600; done
"#;
        let helper = GenericImage::new("minio/mc", MINIO_CLIENT_IMAGE_TAG)
            .with_entrypoint("/bin/sh")
            .with_wait_for(WaitFor::message_on_stdout(MINIO_BOOTSTRAP_READY))
            .with_cmd(["-c", bootstrap])
            .with_env_var("QP_MINIO_SERVER", server_name)
            .with_env_var("QP_MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
            .with_env_var("QP_MINIO_SECRET_KEY", MINIO_SECRET_KEY)
            .with_env_var("QP_MINIO_BUCKET", MINIO_BUCKET)
            .with_network(network)
            .with_startup_timeout(Duration::from_mins(2))
            .start()
            .await;
        let helper = match helper {
            Ok(helper) => helper,
            Err(error) => {
                let cleanup = container
                    .rm()
                    .await
                    .context("remove MinIO after bootstrap failure");
                cleanup?;
                return Err(error).context("configure versioned Object-Lock MinIO bucket");
            }
        };
        if let Err(error) = helper.rm().await.context("remove MinIO bootstrap helper") {
            let cleanup = container
                .rm()
                .await
                .context("remove MinIO after helper cleanup failure");
            cleanup?;
            return Err(error);
        }

        Ok(Self {
            config: ArtifactStoreDeployConfig {
                kind: ArtifactStoreKind::S3,
                bucket: MINIO_BUCKET.to_owned(),
                prefix: "artifacts".to_owned(),
                region: MINIO_REGION.to_owned(),
                endpoint: Some(format!("http://127.0.0.1:{host_port}")),
                path_style: true,
                require_object_lock: true,
                require_versioning: true,
            },
            container,
        })
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
        command
            .arg("--config-dir")
            .arg(&self.run_dir)
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
    child: Child,
    closure_cycle_id: Option<FeedbackCycleId>,
    fixture: ProductionStackFixture,
    governed_cancellation_cycle_id: Option<FeedbackCycleId>,
    launch: ProductionLaunch,
    listen_port: u16,
    _upstream: MockServer,
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
                    86_400,
                    dec!(0.10),
                    dec!(5000),
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
                    CLOSURE_SHADOW_WINDOW_SECS,
                    dec!(1),
                    dec!(1),
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

#[derive(Debug)]
enum SignalObservation {
    ChildExited(ExitStatus),
    IngressClosed,
    Unobserved,
}

pub async fn serve(
    listen_port: u16,
    fixture: ProductionStackFixture,
    retain_artifacts: bool,
) -> Result<()> {
    if listen_port == 0 {
        bail!("production-stack serve requires a non-zero --listen-port");
    }
    ensure_port_available(listen_port)?;
    let workspace = Workspace::build()?;
    let mut running = Box::pin(ProductionStack::start_at(&workspace, listen_port, fixture)).await?;
    println!(
        "production stack ready: base_url={} artifacts={} (terminate to stop)",
        running.base_url(),
        running.run_dir().display(),
    );

    tokio::select! {
        signal = termination_signal() => signal?,
        status = running.child.wait() => {
            let status = status.context("wait for production binary")?;
            bail!(
                "production binary exited before the fixture was terminated: {status}; logs={}",
                running.launch.log_path().display(),
            );
        }
    }

    Box::pin(running.stop_after_signal(!retain_artifacts)).await
}

pub async fn verify(runs: u16) -> Result<()> {
    if runs == 0 {
        bail!("production-stack verify requires --runs greater than zero");
    }
    let workspace = Workspace::build()?;
    for run_number in 1..=runs {
        let listen_port = reserve_port()?;
        let running = Box::pin(ProductionStack::start_at(
            &workspace,
            listen_port,
            ProductionStackFixture::Empty,
        ))
        .await
        .with_context(|| format!("start production-stack verification run {run_number}"))?;
        Box::pin(running.stop(true))
            .await
            .with_context(|| format!("stop production-stack verification run {run_number}"))?;
        println!("production-stack verification run {run_number}/{runs} passed");
    }
    Ok(())
}

impl ProductionStack {
    pub async fn start(fixture: ProductionStackFixture) -> Result<Self> {
        let workspace = Workspace::build()?;
        let listen_port = reserve_port()?;
        Box::pin(Self::start_at(&workspace, listen_port, fixture)).await
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
        let shutdown_result = self.shutdown_binary(origin).await;
        let artifact_infrastructure_result = match self.artifact_infrastructure {
            Some(infrastructure) => infrastructure.shutdown().await,
            None => Ok(()),
        };
        let infrastructure_result = Box::pin(self.infrastructure.shutdown())
            .await
            .context("remove disposable production-stack infrastructure");
        let run_dir_result = if remove_artifacts
            && shutdown_result.is_ok()
            && artifact_infrastructure_result.is_ok()
            && infrastructure_result.is_ok()
        {
            fs::remove_dir_all(&self.launch.run_dir)
                .with_context(|| format!("remove successful run {}", self.launch.run_dir.display()))
        } else {
            Ok(())
        };

        shutdown_result?;
        artifact_infrastructure_result?;
        infrastructure_result?;
        run_dir_result
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
    async fn start_at(
        workspace: &Workspace,
        listen_port: u16,
        fixture: ProductionStackFixture,
    ) -> Result<Self> {
        let upstream = deterministic_upstream().await;
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

            render_config(
                &workspace.root,
                &run_dir,
                listen_port,
                &upstream,
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
        let mut running = Self {
            child: started.child,
            closure_cycle_id,
            fixture,
            governed_cancellation_cycle_id,
            launch: started.launch,
            listen_port,
            _upstream: upstream,
            artifact_infrastructure,
            infrastructure,
        };
        let readiness = Box::pin(running.await_readiness(started.browser_evidence.as_ref())).await;
        if let Err(error) = readiness {
            let cleanup = running
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
        Ok(running)
    }

    async fn await_readiness(&mut self, evidence: Option<&BrowserFixtureEvidence>) -> Result<()> {
        let Some(evidence) = evidence else {
            return Ok(());
        };
        if let Some(closure) = evidence.closure.as_ref() {
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
            Box::pin(complete_feedback_closure(
                self.infrastructure.postgres.connection(),
                &artifact_store,
                closure,
            ))
            .await?;
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
        Ok(())
    }
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
) -> Result<BrowserFixtureEvidence> {
    let (infra, research) =
        Box::pin(fixture.seed_research_fixture(db, runtime_artifact_store)).await?;
    println!(
        "browser research fixture: model_version_id={} evaluation_dataset_id={} backtest_report_id={} feedback_cycle_id={} governed_cancellation_cycle_id={:?}",
        research.model_version_id,
        research.evaluation_dataset_id,
        research.backtest_report_id,
        research.feedback_cycle_id,
        research.governed_cancellation_cycle_id,
    );
    let closure = Box::pin(seed_optional_closure(
        db,
        clickhouse_config,
        runtime_artifact_store,
        &infra,
        research.model_version_id,
        research.feedback_cycle_id,
        fixture,
    ))
    .await?;
    let governed_cancellation_cycle_id =
        if let Some(cycle_id) = research.governed_cancellation_cycle_id {
            ensure!(
                matches!(
                    fixture,
                    ProductionStackFixture::GovernedFeedback
                        | ProductionStackFixture::FeedbackClosure
                        | ProductionStackFixture::FeedbackClosureRecovery
                ),
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
                !matches!(
                    fixture,
                    ProductionStackFixture::GovernedFeedback
                        | ProductionStackFixture::FeedbackClosure
                        | ProductionStackFixture::FeedbackClosureRecovery
                ),
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
    let settlement_report = seed_production_report(
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
    )
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

async fn seed_optional_closure(
    db: &DatabaseConnection,
    clickhouse_config: &ClickHouseConfig,
    runtime_artifact_store: &Arc<dyn ArtifactStore>,
    infra: &SharedDemoInfra,
    model_version_id: ModelVersionId,
    historical_feedback_cycle_id: FeedbackCycleId,
    fixture: ProductionStackFixture,
) -> Result<Option<FeedbackClosureFixture>> {
    if !matches!(
        fixture,
        ProductionStackFixture::FeedbackClosure | ProductionStackFixture::FeedbackClosureRecovery
    ) {
        return Ok(None);
    }
    Box::pin(seed_feedback_closure(
        db,
        clickhouse_config,
        runtime_artifact_store,
        infra,
        model_version_id,
        historical_feedback_cycle_id,
    ))
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
        let run = Entity::find()
            .filter(Column::Kind.eq(FeatureParityRunKind::Sampled))
            .filter(Column::ReportId.eq(*report_id))
            .order_by_desc(Column::CreatedAt)
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

async fn deterministic_upstream() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(deterministic_polygon_rpc)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/time"))
        .respond_with(ResponseTemplate::new(200).set_body_string("1000000"))
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
    Mock::given(method("POST"))
        .and(path("/v1/heartbeats"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "heartbeat_id": "00000000-0000-0000-0000-000000000001",
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/events/keyset"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": [],
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(
            r"^/clob-markets/feedback-closure-(training|calibration|evaluation)-market-[0-9]+$",
        ))
        .respond_with(deterministic_closure_market_info)
        .mount(&server)
        .await;
    server
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
        _ => None,
    }) else {
        return ResponseTemplate::new(404);
    };
    let condition_id = request.url.path().trim_start_matches("/clob-markets/");
    ResponseTemplate::new(200).set_body_json(serde_json::json!({
        "c": condition_id,
        "t": [
            { "t": (yes_base + ordinal).to_string(), "o": "Yes" },
            { "t": (no_base + ordinal).to_string(), "o": "No" }
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

fn deterministic_polygon_rpc(request: &Request) -> ResponseTemplate {
    let request = serde_json::from_slice::<JsonRpcRequest>(&request.body);
    let Ok(request) = request else {
        return ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": { "code": -32700, "message": "invalid JSON-RPC request" },
        }));
    };
    let result = match request.method.as_str() {
        "eth_chainId" => serde_json::json!("0x89"),
        "eth_blockNumber" => serde_json::json!("0x4000000"),
        "eth_getBlockByNumber" => deterministic_polygon_block(&request.params),
        "eth_getLogs" => serde_json::json!([]),
        "eth_getCode" => serde_json::json!("0x01"),
        "eth_getStorageAt" => deterministic_polygon_storage(&request.params),
        "eth_call" => serde_json::json!(format!("0x{}", "0".repeat(64))),
        method => {
            return ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": request.id,
                "error": { "code": -32601, "message": format!("unsupported deterministic method: {method}") },
            }));
        }
    };
    ResponseTemplate::new(200).set_body_json(JsonRpcResponse {
        jsonrpc: "2.0",
        id: request.id,
        result,
    })
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

fn deterministic_polygon_block(params: &JsonValue) -> JsonValue {
    let requested = params
        .as_array()
        .and_then(|params| params.first())
        .and_then(JsonValue::as_str)
        .unwrap_or("finalized");
    let number = if matches!(requested, "finalized" | "latest" | "safe") {
        "0x4000000"
    } else {
        requested
    };
    serde_json::json!({
        "number": number,
        "hash": format!("0x{}", "11".repeat(32)),
        "parentHash": format!("0x{}", "10".repeat(32)),
        "sha3Uncles": format!("0x{}", "1d".repeat(32)),
        "miner": format!("0x{}", "22".repeat(20)),
        "stateRoot": format!("0x{}", "33".repeat(32)),
        "transactionsRoot": format!("0x{}", "44".repeat(32)),
        "receiptsRoot": format!("0x{}", "55".repeat(32)),
        "logsBloom": format!("0x{}", "00".repeat(256)),
        "difficulty": "0x0",
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x0",
        "timestamp": format!("0x{:x}", Utc::now().timestamp()),
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
    stack: &SystemStack,
    artifact_store: &ArtifactStoreDeployConfig,
) -> Result<()> {
    let source_path = workspace_root.join("config/quant-pivot.toml");
    let source = fs::read_to_string(&source_path)
        .with_context(|| format!("read canonical deploy config {}", source_path.display()))?;
    let mut config: Value = toml::from_str(&source)
        .with_context(|| format!("parse canonical deploy config {}", source_path.display()))?;
    configure_upstreams(&mut config, upstream)?;
    configure_test_identity(&mut config, artifact_store)?;
    configure_infrastructure(&mut config, stack)?;
    configure_web(&mut config, listen_port)?;

    let rendered = toml::to_string_pretty(&config).context("serialize production-stack config")?;
    let config_path = run_dir.join("quant-pivot.toml");
    fs::write(&config_path, rendered)
        .with_context(|| format!("write production-stack config {}", config_path.display()))?;
    let config_dir = run_dir
        .to_str()
        .context("production-stack config path is not valid UTF-8")?;
    DeployConfig::load(config_dir).context("validate generated production-stack config")?;
    Ok(())
}

fn configure_upstreams(config: &mut Value, upstream: &MockServer) -> Result<()> {
    let upstream_url = upstream.uri();
    set(
        config,
        &["polymarket", "clob_base_url"],
        upstream_url.clone(),
    )?;
    set(
        config,
        &["polymarket", "clob_ws_url"],
        "ws://127.0.0.1:1/ws",
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
        false,
    )?;
    for source in DISABLED_DOMAIN_SOURCES {
        set(config, &["domain_sources", source, "enabled"], false)?;
    }
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
